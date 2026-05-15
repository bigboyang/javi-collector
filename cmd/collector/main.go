package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kkc/javi-collector/internal/config"
	"github.com/kkc/javi-collector/internal/forecast"
	"github.com/kkc/javi-collector/internal/ingester"
	jkafka "github.com/kkc/javi-collector/internal/kafka"
	"github.com/kkc/javi-collector/internal/processor"
	"github.com/kkc/javi-collector/internal/sampling"
	"github.com/kkc/javi-collector/internal/selftracing"
	"github.com/kkc/javi-collector/internal/server"
	"github.com/kkc/javi-collector/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}

	// DisableClickHouse=true 이면 인메모리 링버퍼로 fallback한다.
	// 프로덕션에서는 DISABLE_CLICKHOUSE=false (기본값)로 ClickHouse를 사용한다.
	var (
		traceStore      store.TraceStore
		metricStore     store.MetricStore
		logStore        store.LogStore
		chConn          driver.Conn                  // ClickHouse 공유 커넥션 (RCA Engine 등에서 재사용)
		catalogStore         *store.ServiceCatalogStore    // Gap 4: 서비스 카탈로그
		errorGroupStore      *store.ErrorGroupStore        // Gap 2: 에러 그룹 집계
		sloStore             *store.SLOStore               // Gap 3: SLO/SLI + Burn-Rate
		rcaStore             *store.RCAStore               // P1: RCA 결과 조회
		deploymentEventStore *store.DeploymentEventStore   // GAP-04: 배포 이벤트 상관 분석
		logAnalyticsStore    *store.LogAnalyticsStore      // GAP-06: Log Analytics
		profilingStore       *store.ProfilingStore         // GAP-07: Continuous Profiling
		k8sPodMetricsStore   *store.K8sPodMetricsStore    // GAP-08 확장: K8s Pod 리소스 메트릭
	)

	if cfg.DisableClickHouse {
		slog.Info("using in-memory store (ClickHouse disabled)")
		memTrace := store.NewMemoryTraceStore(cfg.MemoryBufferSize)
		memMetric := store.NewMemoryMetricStore(cfg.MemoryBufferSize)
		memLog := store.NewMemoryLogStore(cfg.MemoryBufferSize)

		if cfg.WALEnabled {
			walCfg := store.WALConfig{Dir: cfg.WALDir, MaxBytes: cfg.WALMaxBytes}
			slog.Info("wal enabled", "dir", cfg.WALDir, "max_bytes", cfg.WALMaxBytes)

			if ws, werr := store.NewWALTraceStore(memTrace, walCfg); werr != nil {
				slog.Warn("wal trace store init failed, falling back to memory-only", "err", werr)
				traceStore = memTrace
			} else {
				traceStore = ws
			}
			if ws, werr := store.NewWALMetricStore(memMetric, walCfg); werr != nil {
				slog.Warn("wal metric store init failed, falling back to memory-only", "err", werr)
				metricStore = memMetric
			} else {
				metricStore = ws
			}
			if ws, werr := store.NewWALLogStore(memLog, walCfg); werr != nil {
				slog.Warn("wal log store init failed, falling back to memory-only", "err", werr)
				logStore = memLog
			} else {
				logStore = ws
			}
		} else {
			traceStore = memTrace
			metricStore = memMetric
			logStore = memLog
		}
	} else {
		chCfg := store.ClickHouseConfig{
			Addr:               cfg.ClickHouseAddr,
			Database:           cfg.ClickHouseDB,
			Username:           cfg.ClickHouseUser,
			Password:           cfg.ClickHousePassword,
			BatchSize:          cfg.BatchSize,
			FlushInterval:      cfg.FlushInterval,
			ChanBuffer:         cfg.ChannelBufferSize,
			RetentionDays:      cfg.RetentionDays,
			DLQDir:             cfg.DLQDir,
			CBFailureThreshold: cfg.CBFailureThreshold,
			CBCooldown:         cfg.CBCooldown,
			FlushWorkers:       cfg.FlushWorkers,
		}

		// 상용 APM 패턴: 하나의 공유 커넥션 풀을 세 Store가 공유한다.
		// 이전에는 Store마다 openConn을 호출해 최대 3×MaxOpenConns 커넥션이 생성됐다.
		var err error
		chConn, err = store.OpenConn(chCfg)
		if err != nil {
			slog.Error("clickhouse connection failed", "err", err)
			os.Exit(1)
		}

		ts, err := store.NewClickHouseTraceStore(chConn, chCfg)
		if err != nil {
			slog.Error("clickhouse trace store init failed", "err", err)
			_ = chConn.Close()
			os.Exit(1)
		}
		ms, err := store.NewClickHouseMetricStore(chConn, chCfg)
		if err != nil {
			slog.Error("clickhouse metric store init failed", "err", err)
			_ = ts.Close()
			_ = chConn.Close()
			os.Exit(1)
		}
		ls, err := store.NewClickHouseLogStore(chConn, chCfg)
		if err != nil {
			slog.Error("clickhouse log store init failed", "err", err)
			_ = ts.Close()
			_ = ms.Close()
			_ = chConn.Close()
			os.Exit(1)
		}

		traceStore = ts
		metricStore = ms
		logStore = ls

		// Gap 4: 서비스 카탈로그 — 팀 소유권 · 운영 메타데이터 CRUD
		if cs, cerr := store.NewServiceCatalogStore(chConn, cfg.ClickHouseDB); cerr != nil {
			slog.Warn("service catalog init failed (continuing without catalog)", "err", cerr)
		} else {
			catalogStore = cs
		}

		// Gap 2: 에러 그룹 — fingerprint 기반 중복 에러 집계
		if eg, egerr := store.NewErrorGroupStore(chConn, cfg.ClickHouseDB); egerr != nil {
			slog.Warn("error groups init failed (continuing without error groups)", "err", egerr)
		} else {
			errorGroupStore = eg
		}

		// Gap 3: SLO/SLI + Burn-Rate Alerting
		if ss, slerr := store.NewSLOStore(chConn, cfg.ClickHouseDB); slerr != nil {
			slog.Warn("slo store init failed (continuing without SLO)", "err", slerr)
		} else {
			sloStore = ss
			burnCalc := store.NewSLOBurnCalculator(ss, chConn, cfg.ClickHouseDB, time.Minute)
			burnCalc.Start()
			defer burnCalc.Stop()
			slog.Info("slo burn calculator started")
		}

		// P1: RCA 결과 조회 스토어
		rcaStore = store.NewRCAStore(chConn, cfg.ClickHouseDB)

		// GAP-04: 배포 이벤트 상관 분석 스토어
		if ds, derr := store.NewDeploymentEventStore(chConn, cfg.ClickHouseDB); derr != nil {
			slog.Warn("deployment event store init failed (continuing without deployment correlation)", "err", derr)
		} else {
			deploymentEventStore = ds
		}

		// GAP-07: Continuous Profiling 스토어
		if ps, perr := store.NewProfilingStore(chConn, cfg.ClickHouseDB); perr != nil {
			slog.Warn("profiling store init failed (continuing without profiling)", "err", perr)
		} else {
			profilingStore = ps
			slog.Info("profiling store initialized")
		}

		// GAP-08 확장: K8s Pod 리소스 메트릭 스토어
		if km, kmerr := store.NewK8sPodMetricsStore(chConn, cfg.ClickHouseDB); kmerr != nil {
			slog.Warn("k8s pod metrics store init failed (continuing without k8s metrics)", "err", kmerr)
		} else {
			k8sPodMetricsStore = km
			slog.Info("k8s pod metrics store initialized")
		}

		// 공유 커넥션은 모든 store가 drain된 후 닫아야 한다.
		// defer 실행 순서(LIFO)를 이용: store Close() → conn Close()
		defer func() {
			if err := chConn.Close(); err != nil {
				slog.Warn("clickhouse conn close error", "err", err)
			}
		}()

		slog.Info("ClickHouse store initialized",
			"addr", cfg.ClickHouseAddr,
			"db", cfg.ClickHouseDB,
			"batch_size", cfg.BatchSize,
			"flush_interval", cfg.FlushInterval,
			"flush_workers", cfg.FlushWorkers,
		)

	}

	// Signal context: SIGINT/SIGTERM 수신 시 취소된다.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Tail Sampling + Adaptive Sampling 설정
	// SAMPLING_ENABLED=false(기본)이면 TailSamplingStore는 전량 통과(no-op) 모드로 동작한다.
	// SAMPLING_CONFIG_JSON이 설정되면 해당 JSON을 초기 config로 사용한다.
	// REMOTE_CONFIG_URL이 설정되면 주기적으로 SamplingConfig를 폴링해 동적으로 반영한다.
	initSamplingCfg := sampling.NewDefaultConfig(cfg.SamplingEnabled)
	if cfg.SamplingConfigJSON != "" {
		if parsed, err := sampling.ParseConfig([]byte(cfg.SamplingConfigJSON)); err != nil {
			slog.Warn("SAMPLING_CONFIG_JSON parse failed, using default", "err", err)
		} else {
			initSamplingCfg = parsed
			slog.Info("sampling: inline config loaded from SAMPLING_CONFIG_JSON",
				"error_sampling", parsed.ErrorSampling.Enabled,
				"latency_ms", parsed.LatencySampling.ThresholdMs,
				"probabilistic_rate", parsed.ProbabilisticSampling.Rate,
			)
		}
	}
	poller := sampling.NewRemoteConfigPoller(
		cfg.RemoteConfigURL,
		cfg.RemoteConfigPollInterval,
		initSamplingCfg,
	)
	poller.Start(ctx)

	tailStore := sampling.NewTailSamplingStore(traceStore, poller)
	tailStore.Start(ctx)
	tailStore.WatchConfig(poller.OnChange())

	slog.Info("sampling initialized",
		"enabled", cfg.SamplingEnabled,
		"remote_config_url", cfg.RemoteConfigURL,
		"poll_interval", cfg.RemoteConfigPollInterval,
	)

	// DLQ 자동 재적재 & 보관 기간 정리
	// ClickHouse가 활성화돼 있고 DLQDir이 설정된 경우에만 실행한다.
	// 재적재는 전날 이전 파일만 대상으로 하며, Sampling을 우회해 traceStore에 직접 쓴다.
	if !cfg.DisableClickHouse && cfg.DLQDir != "" {
		store.CleanupDLQFiles(cfg.DLQDir, cfg.DLQRetentionDays)
		replayer := store.NewDLQReplayer(cfg.DLQDir, traceStore, metricStore, logStore, cfg.DLQReplayInterval)
		replayer.Start(ctx)
		slog.Info("DLQ replayer started",
			"dir", cfg.DLQDir,
			"replay_interval", cfg.DLQReplayInterval,
			"retention_days", cfg.DLQRetentionDays,
		)
	}

	// GAP-05: Alert Routing & Escalation
	// alert_routes / alert_events 테이블을 관리하는 저장소를 초기화한다.
	var alertRouteStore *store.AlertRouteStore
	if !cfg.DisableClickHouse {
		if ars, err := store.NewAlertRouteStore(chConn, cfg.ClickHouseDB); err != nil {
			slog.Warn("alert route store init failed (continuing without alert routing)", "err", err)
		} else {
			alertRouteStore = ars
			slog.Info("alert route store initialized")
		}
	}

	// GAP-06: Log Analytics
	// mv_log_volume_1m_state MV를 생성하고 5가지 로그 분석 쿼리를 제공한다.
	if !cfg.DisableClickHouse {
		if las, err := store.NewLogAnalyticsStore(chConn, cfg.ClickHouseDB); err != nil {
			slog.Warn("log analytics store init failed (continuing without log analytics)", "err", err)
		} else {
			logAnalyticsStore = las
			slog.Info("log analytics store initialized")
		}
	}

	// 파일 백업: BACKUP_ENABLED=true이면 ingester에 전달되는 store를
	// Backup 데코레이터로 감싼다. tailStore 자체는 *TailSamplingStore 타입을
	// 유지해야 하므로(Start/Close 메서드 사용) 인터페이스 변수를 별도로 선언한다.
	// 백업 쓰기 실패가 수신 파이프라인을 막지 않도록 내부에서 warn-only 처리한다.
	var (
		ingestTraceStore  store.TraceStore  = tailStore
		ingestMetricStore store.MetricStore = metricStore
		ingestLogStore    store.LogStore    = logStore
	)

	if cfg.BackupEnabled {
		backupWriter, err := store.NewFileBackupWriter(cfg.BackupDir)
		if err != nil {
			slog.Error("backup writer init failed", "err", err)
			os.Exit(1)
		}
		defer func() {
			if err := backupWriter.Close(); err != nil {
				slog.Warn("backup writer close error", "err", err)
			}
		}()
		ingestTraceStore = store.NewBackupTraceStore(tailStore, backupWriter)
		ingestMetricStore = store.NewBackupMetricStore(metricStore, backupWriter)
		ingestLogStore = store.NewBackupLogStore(logStore, backupWriter)
		slog.Info("file backup enabled", "dir", cfg.BackupDir)
	}

	// 설정 핫 리로드: HOT_RELOAD_FILE이 설정된 경우 BatchSize/FlushInterval을 동적으로 반영한다.
	// ClickHouse가 활성화된 경우에만 의미가 있다.
	if !cfg.DisableClickHouse && cfg.HotReloadFile != "" {
		hr := config.NewHotReloader(cfg.HotReloadFile, cfg.HotReloadInterval)
		hr.OnChange(func(dc config.DynamicConfig) {
			if ts, ok := traceStore.(store.DynamicConfigSetter); ok {
				ts.SetDynamicConfig(dc.BatchSize, dc.FlushInterval)
			}
			if ms, ok := metricStore.(store.DynamicConfigSetter); ok {
				ms.SetDynamicConfig(dc.BatchSize, dc.FlushInterval)
			}
			if ls, ok := logStore.(store.DynamicConfigSetter); ok {
				ls.SetDynamicConfig(dc.BatchSize, dc.FlushInterval)
			}
		})
		hr.Start(ctx)
		slog.Info("hot reload enabled",
			"file", cfg.HotReloadFile,
			"interval", cfg.HotReloadInterval,
		)
	}

	// Publisher 결정: Kafka 활성화 여부에 따라 팬아웃 방식이 달라진다.
	//
	//   KAFKA_ENABLED=false (기본):
	//     spanPub   → DirectSpanPublisher (RAG)  [+ ForecastForwarder if FORECAST_ENDPOINT 설정]
	//     metricPub → ForecastForwarder (if FORECAST_ENDPOINT 설정)
	//
	//   KAFKA_ENABLED=true:
	//     SpanProducer   → Kafka "spans.error" → [RAG + Forecast consumers]
	//     MetricProducer → Kafka "metrics"      → [MetricForecast consumer]
	//     LogProducer    → Kafka "logs"         → [LogRAG consumer]
	var spanPub ingester.SpanPublisher
	var metricPub ingester.MetricPublisher
	var logPub ingester.LogPublisher
	var deployPub *jkafka.DeploymentProducer

	// Direct Forecast Forwarder: KAFKA_ENABLED=false이고 FORECAST_ENDPOINT가 설정된 경우 활성화.
	// span/metric을 배치로 묶어 javi-forecast HTTP 엔드포인트에 직접 전송한다.
	// jvm.* OTel 메트릭은 자동으로 JvmMetricBatch로 변환해 /v1/metrics/jvm 으로 전송한다.
	var forecaster *forecast.ForecastForwarder
	if !cfg.KafkaEnabled && cfg.ForecastEndpoint != "" {
		forecaster = forecast.New(forecast.Config{
			Endpoint:      cfg.ForecastEndpoint,
			BatchSize:     cfg.ForecastBatchSize,
			FlushInterval: cfg.ForecastFlushInterval,
		})
		forecaster.Start(ctx)
		metricPub = forecaster
		slog.Info("forecast forwarder started",
			"endpoint", cfg.ForecastEndpoint,
			"batch_size", cfg.ForecastBatchSize,
			"flush_interval", cfg.ForecastFlushInterval,
		)
	}

	if cfg.KafkaEnabled {
		// Span producer
		spanProducer := jkafka.NewSpanProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
		spanPub = spanProducer
		slog.Info("kafka span producer started",
			"brokers", cfg.KafkaBrokers,
			"topic", cfg.KafkaTopic,
		)

		// Metric producer
		metricProducer := jkafka.NewMetricProducer(cfg.KafkaBrokers, cfg.KafkaMetricsTopic)
		metricPub = metricProducer
		slog.Info("kafka metric producer started", "topic", cfg.KafkaMetricsTopic)

		// Log producer
		logProducer := jkafka.NewLogProducer(cfg.KafkaBrokers, cfg.KafkaLogsTopic)
		logPub = logProducer
		slog.Info("kafka log producer started", "topic", cfg.KafkaLogsTopic)

		// Deployment producer
		deployPub = jkafka.NewDeploymentProducer(cfg.KafkaBrokers, cfg.KafkaDeployTopic)
		defer deployPub.Close() //nolint:errcheck
		slog.Info("kafka deployment producer started", "topic", cfg.KafkaDeployTopic)

		defer spanProducer.Close()   //nolint:errcheck
		defer metricProducer.Close() //nolint:errcheck
		defer logProducer.Close()    //nolint:errcheck
	} else {
		// 직접 모드 (Kafka 미사용): Forecast 팬아웃
		if forecaster != nil {
			spanPub = forecaster
		}
	}

	ing := ingester.New(ingestTraceStore, ingestMetricStore, ingestLogStore, spanPub)
	if metricPub != nil {
		ing.SetMetricPublisher(metricPub)
	}
	if logPub != nil {
		ing.SetLogPublisher(logPub)
	}

	// ── Processor Pipeline ────────────────────────────────────────────────
	// CARDINALITY_ENABLED=true이면 CardinalityProcessor를 파이프라인에 추가한다.
	// 파이프라인은 이후 추가 프로세서를 여기에 append해 확장한다.
	{
		var procs []processor.Processor
		if cfg.CardinalityEnabled {
			limits := processor.CardinalityLimits{
				PerServiceAttrLimit: cfg.CardinalityLimit,
				BloomFilterBits:     uint(cfg.CardinalityBloomBits),
				BloomHashFunctions:  uint(cfg.CardinalityBloomK),
			}
			procs = append(procs, processor.NewCardinalityProcessor(limits))
			slog.Info("cardinality processor enabled",
				"limit_per_service_attr", cfg.CardinalityLimit,
				"bloom_bits", cfg.CardinalityBloomBits,
				"bloom_k", cfg.CardinalityBloomK,
			)
		}
		if len(procs) > 0 {
			ing.SetPipeline(processor.NewPipeline(procs...))
		}
	}

	// ── Collector Self-Tracing ─────────────────────────────────────────────
	// SELF_TRACING_ENABLED=true이면 컬렉터 내부 파이프라인 스팬을 traceStore에 기록한다.
	// 셀프 트레이스는 일반 스팬과 함께 저장되며 "javi.internal"=true 속성으로 구분 가능하다.
	if cfg.SelfTracingEnabled {
		st := selftracing.New(traceStore)
		st.Start()
		defer st.Stop()
		ing.SetSelfTracer(st)
		slog.Info("collector self-tracing enabled")
	}

	// TraceRouter: SAMPLING_ENABLED=true이고 SELF_URL이 설정된 경우 활성화.
	// 멀티 인스턴스 배포 시 traceID 기반 일관 해시로 동일 trace의 spans를
	// 항상 같은 인스턴스로 모아 TailSampling이 완전한 trace 데이터로 결정하도록 한다.
	var traceRouter *sampling.TraceRouter
	if cfg.SamplingEnabled && cfg.SelfURL != "" {
		traceRouter = sampling.NewTraceRouter(cfg.SelfURL, cfg.PeerURLs,
			cfg.PeerCBFailureThreshold, cfg.PeerCBCooldown)
		if traceRouter.Enabled() {
			slog.Info("trace routing enabled",
				"self", cfg.SelfURL,
				"peers", cfg.PeerURLs,
				"total_nodes", len(cfg.PeerURLs)+1,
				"cb_threshold", cfg.PeerCBFailureThreshold,
				"cb_cooldown", cfg.PeerCBCooldown,
			)
		}
	}

	defer func() {
		poller.Stop()
		// tailStore.Close()가 내부적으로 downstream(traceStore)을 닫는다.
		if err := tailStore.Close(); err != nil {
			slog.Warn("tail store close error", "err", err)
		}
		if err := metricStore.Close(); err != nil {
			slog.Warn("metric store close error", "err", err)
		}
		if err := logStore.Close(); err != nil {
			slog.Warn("log store close error", "err", err)
		}
	}()

	// HTTP 서버 (OTLP/HTTP + REST API + /healthz + /readyz + /metrics)
	httpAddr := ":" + strconv.Itoa(cfg.HTTPPort)
	httpSrv := server.NewHTTPServer(httpAddr, ing, traceStore, metricStore, logStore)

	// gRPC 서버 (OTLP/gRPC + Health + Reflection)
	grpcAddr := ":" + strconv.Itoa(cfg.GRPCPort)
	grpcSrv := server.NewGRPCServer(grpcAddr, ing)

	// TraceRouter를 HTTP/gRPC 서버 양쪽에 주입
	if traceRouter != nil && traceRouter.Enabled() {
		httpSrv.SetTraceRouter(traceRouter)
		grpcSrv.SetTraceRouter(traceRouter)
	}

	// Gap 4: 서비스 카탈로그 주입
	if catalogStore != nil {
		httpSrv.SetServiceCatalog(catalogStore)
	}

	// Gap 2: 에러 그룹 주입
	if errorGroupStore != nil {
		httpSrv.SetErrorGroups(errorGroupStore)
	}

	// Gap 1: Correlated Signal Navigation — ClickHouseTraceStore가 구현체를 제공한다.
	if tc, ok := traceStore.(server.CorrelatedSignalQuerier); ok {
		httpSrv.SetTraceContext(tc)
	}

	// GAP-01: Trace Waterfall / Critical Path — ClickHouseTraceStore가 구현체를 제공한다.
	if tw, ok := traceStore.(server.TraceWaterfallQuerier); ok {
		httpSrv.SetTraceWaterfall(tw)
	}

	// Gap 3: SLO/SLI + Burn-Rate Alerting
	if sloStore != nil {
		httpSrv.SetSLOManager(sloStore)
	}

	// P1: RCA 결과 조회
	if rcaStore != nil {
		httpSrv.SetRCAReports(rcaStore)
	}

	// GAP-04: 배포 이벤트 ClickHouse 저장소 주입
	if deploymentEventStore != nil {
		httpSrv.SetDeploymentStore(deploymentEventStore)
	}

	// GAP-05: Alert Routing & Escalation
	if alertRouteStore != nil {
		httpSrv.SetAlertRoutes(alertRouteStore)
	}

	// GAP-06: Log Analytics
	if logAnalyticsStore != nil {
		httpSrv.SetLogAnalytics(logAnalyticsStore)
	}

	// DB Slow Query MV 조회기 주입
	if sq, ok := traceStore.(server.SlowQueryQuerier); ok {
		httpSrv.SetSlowQueryQuerier(sq)
	}

	// GAP-08: Infra Metrics Correlation — ClickHouseTraceStore가 구현체를 제공한다.
	if ic, ok := traceStore.(server.InfraCorrelationQuerier); ok {
		httpSrv.SetInfraCorrelation(ic)
	}

	// GAP-07: Continuous Profiling 스토어 주입
	if profilingStore != nil {
		httpSrv.SetProfilingStore(profilingStore)
	}

	// GAP-08 확장: K8s Pod 리소스 메트릭 스토어 주입
	if k8sPodMetricsStore != nil {
		httpSrv.SetK8sMetrics(k8sPodMetricsStore)
	}

	// 배포 이벤트 프로듀서 주입
	if deployPub != nil {
		httpSrv.SetDeployProducer(deployPub)
	}

	// API Key 인증 주입 (API_KEY 설정 시 /api/* 엔드포인트에 적용)
	if cfg.APIKey != "" {
		httpSrv.SetAPIKey(cfg.APIKey)
		slog.Info("API key authentication enabled for /api/* endpoints")
	}

	// HTTP 서버 시작
	go func() {
		if err := httpSrv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", "err", err)
			stop()
		}
	}()

	// gRPC 서버 시작
	go func() {
		if err := grpcSrv.Start(); err != nil {
			slog.Error("gRPC server error", "err", err)
			stop()
		}
	}()

	// 모든 초기화 완료 → readyz 활성화
	// 이 시점부터 쿠버네티스 로드밸런서가 트래픽을 보내기 시작한다.
	httpSrv.MarkReady()

	slog.Info("javi-collector started",
		"http_port", cfg.HTTPPort,
		"grpc_port", cfg.GRPCPort,
		"clickhouse_disabled", cfg.DisableClickHouse,
	)

	<-ctx.Done()
	slog.Info("shutting down...")

	// readyz를 먼저 비활성화해 새 연결을 차단한다.
	// UnmarkReady() 호출 → readyz가 즉시 503 반환 →
	// 로드밸런서가 이 인스턴스를 제거할 시간(terminationGracePeriodSeconds)을 확보한다.
	httpSrv.UnmarkReady()
	slog.Info("readyz deactivated, waiting for load balancer drain")

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// gRPC GracefulStop: 진행 중인 RPC가 완료될 때까지 대기
	grpcSrv.Stop()

	if err := httpSrv.Shutdown(shutCtx); err != nil {
		slog.Error("HTTP shutdown error", "err", err)
	}

	slog.Info("shutdown complete")
}
