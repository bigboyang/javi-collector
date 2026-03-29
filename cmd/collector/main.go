package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kkc/javi-collector/internal/alerter"
	"github.com/kkc/javi-collector/internal/anomaly"
	"github.com/kkc/javi-collector/internal/config"
	"github.com/kkc/javi-collector/internal/ingester"
	jkafka "github.com/kkc/javi-collector/internal/kafka"
	"github.com/kkc/javi-collector/internal/processor"
	"github.com/kkc/javi-collector/internal/rag"
	"github.com/kkc/javi-collector/internal/rca"
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
		traceStore  store.TraceStore
		metricStore store.MetricStore
		logStore    store.LogStore
		chConn      driver.Conn // ClickHouse 공유 커넥션 (RCA Engine 등에서 재사용)
	)

	if cfg.DisableClickHouse {
		slog.Info("using in-memory store (ClickHouse disabled)")
		traceStore = store.NewMemoryTraceStore(cfg.MemoryBufferSize)
		metricStore = store.NewMemoryMetricStore(cfg.MemoryBufferSize)
		logStore = store.NewMemoryLogStore(cfg.MemoryBufferSize)
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

		// AIOps Phase 1: RED Baseline 자동 집계
		// BaselineComputer는 공유 커넥션으로 매 BaselineInterval마다
		// red_baseline 테이블을 갱신한다.
		if cfg.BaselineEnabled {
			bc := store.NewBaselineComputer(chConn, cfg.ClickHouseDB, cfg.BaselineInterval)
			bc.Start()
			defer bc.Stop()
			slog.Info("baseline computer started",
				"interval", cfg.BaselineInterval,
				"db", cfg.ClickHouseDB,
			)
		}

		// AIOps Phase 2: Z-score + IsolationForest 이상 탐지
		// Detector는 AnomalyInterval마다 mv_red_1m_state와 red_baseline을 비교해
		// latency_p95_spike / error_rate_spike / traffic_drop / multivariate_anomaly를
		// anomalies 테이블에 기록한다.
		if cfg.AnomalyEnabled {
			anomalyCfg := anomaly.Config{
				Interval:      cfg.AnomalyInterval,
				TrainInterval: cfg.AnomalyTrainInterval,
				NTrees:        cfg.AnomalyNTrees,
				MaxSamples:    cfg.AnomalyMaxSamples,
				ZWarn:         cfg.AnomalyZWarn,
				ZCritical:     cfg.AnomalyZCritical,
				IFThreshold:   cfg.AnomalyIFThreshold,
			}
			det := anomaly.NewDetector(chConn, cfg.ClickHouseDB, anomalyCfg)
			det.Start()
			defer det.Stop()
			slog.Info("anomaly detector started",
				"interval", anomalyCfg.Interval,
				"train_interval", anomalyCfg.TrainInterval,
				"z_warn", anomalyCfg.ZWarn,
				"z_critical", anomalyCfg.ZCritical,
				"if_threshold", anomalyCfg.IFThreshold,
			)
		}
	}

	// Signal context: SIGINT/SIGTERM 수신 시 취소된다.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Tail Sampling + Adaptive Sampling 설정
	// SAMPLING_ENABLED=false(기본)이면 TailSamplingStore는 전량 통과(no-op) 모드로 동작한다.
	// REMOTE_CONFIG_URL이 설정되면 주기적으로 SamplingConfig를 폴링해 동적으로 반영한다.
	initSamplingCfg := sampling.NewDefaultConfig(cfg.SamplingEnabled)
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

	// RAG 파이프라인 초기화 (EMBED_ENABLED=true 시)
	// Ollama(nomic-embed-text) → Qdrant 벡터 저장 → 자연어 장애 검색
	var embedPipeline *rag.EmbedPipeline
	var ragSearcher *rag.RAGSearcher
	var ragGenerator *rag.RAGGenerator
	if cfg.EmbedEnabled {
		embedClient := rag.NewOllamaEmbedClient(cfg.EmbedEndpoint, cfg.EmbedModel)
		qdrantClient := rag.NewQdrantClient(cfg.QdrantEndpoint, cfg.QdrantCollection)
		if err := qdrantClient.EnsureCollection(ctx, 768); err != nil {
			slog.Warn("qdrant collection init failed (RAG disabled)", "err", err)
		} else {
			embedPipeline = rag.NewEmbedPipeline(embedClient, qdrantClient, 1024, 32, 10*time.Second)
			embedPipeline.Start(ctx)
			ragSearcher = rag.NewRAGSearcher(embedClient, qdrantClient, cfg.RAGScoreThreshold)

			// RAG Generation: LLM 기반 RCA 분석 (LLM_ENABLED=true 시)
			if cfg.LLMEnabled {
				llmClient := rag.NewOllamaLLMClient(cfg.EmbedEndpoint, cfg.LLMModel)
				ragGenerator = rag.NewRAGGenerator(ragSearcher, llmClient)
				slog.Info("RAG LLM generation enabled",
					"model", cfg.LLMModel,
					"endpoint", cfg.EmbedEndpoint,
				)
			}

			slog.Info("RAG embed pipeline started",
				"endpoint", cfg.EmbedEndpoint,
				"model", cfg.EmbedModel,
				"qdrant", cfg.QdrantEndpoint,
			)

			// RAG Janitor: 오래된 Qdrant 포인트를 주기적으로 삭제해 컬렉션 크기를 제한한다.
			// RAG_RETENTION_DAYS=0 이면 비활성화.
			if cfg.RAGRetentionDays > 0 {
				janitor := rag.NewQdrantJanitor(qdrantClient, cfg.RAGRetentionDays, cfg.RAGJanitorInterval)
				janitor.Start(ctx)
				slog.Info("qdrant janitor started",
					"retention_days", cfg.RAGRetentionDays,
					"interval", cfg.RAGJanitorInterval,
				)
			}

			// RAG Historical Backfill: ClickHouse 과거 ERROR spans → Qdrant 적재
			// 기동 직후 Qdrant가 비어 있어 유사 사례 검색이 안 되는 문제를 해결한다.
			if cfg.RAGBackfillEnabled {
				checkpointFile := cfg.RAGBackfillCheckpointFile
				if checkpointFile == "" {
					checkpointFile = filepath.Join(cfg.BackupDir, "rag_backfill_checkpoint.json")
				}
				backfiller := rag.NewHistoricalBackfiller(
					traceStore, embedPipeline,
					cfg.RAGBackfillDays, cfg.RAGBackfillBatchSize,
					checkpointFile, int64(cfg.RAGSlowThresholdMs),
				)
				backfiller.Run(ctx)
				slog.Info("RAG historical backfill scheduled",
					"days", cfg.RAGBackfillDays,
					"batch_size", cfg.RAGBackfillBatchSize,
				)
			}
		}
	}

	// AIOps Alert: Webhook / Slack Push Alerter
	// anomalies 테이블을 폴링해 신규 이상 이벤트를 외부로 Push한다.
	// ALERT_WEBHOOK_URL 또는 ALERT_SLACK_WEBHOOK_URL 중 하나라도 설정하면 활성화된다.
	if !cfg.DisableClickHouse {
		alertCfg := alerter.Config{
			WebhookURL:      cfg.AlertWebhookURL,
			SlackWebhookURL: cfg.AlertSlackWebhookURL,
			Interval:        cfg.AlertInterval,
			MinSeverity:     cfg.AlertMinSeverity,
		}
		al := alerter.New(chConn, cfg.ClickHouseDB, alertCfg)
		if al.Enabled() {
			al.Start()
			defer al.Stop()
			slog.Info("alerter started",
				"interval", cfg.AlertInterval,
				"min_severity", cfg.AlertMinSeverity,
				"slack", cfg.AlertSlackWebhookURL != "",
				"webhook", cfg.AlertWebhookURL != "",
			)
		}
	}

	// AIOps Phase 3: RCA Engine
	// anomalies 테이블을 폴링해 연관 spans + RAG 유사 사례를 결합한 rca_reports를 생성한다.
	if !cfg.DisableClickHouse && cfg.RCAEnabled {
		rcaCfg := rca.Config{Interval: cfg.RCAInterval}
		rcaEngine := rca.NewEngine(chConn, cfg.ClickHouseDB, rcaCfg, ragSearcher)
		if ragGenerator != nil {
			rcaEngine.SetGenerator(ragGenerator)
		}
		rcaEngine.Start()
		defer rcaEngine.Stop()
		slog.Info("rca engine started",
			"interval", cfg.RCAInterval,
			"rag_enabled", ragSearcher != nil,
			"llm_enabled", ragGenerator != nil,
		)
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

	// SpanPublisher 결정: Kafka 활성화 여부에 따라 팬아웃 방식이 달라진다.
	//   KAFKA_ENABLED=false (기본): DirectSpanPublisher → EmbedPipeline (채널)
	//   KAFKA_ENABLED=true:         SpanProducer → Kafka "spans.error" → [RAG + Forecast consumers]
	var spanPub ingester.SpanPublisher
	if cfg.KafkaEnabled {
		// Kafka 모드: span 이벤트를 Kafka에 발행
		producer := jkafka.NewSpanProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
		spanPub = producer
		slog.Info("kafka span producer started",
			"brokers", cfg.KafkaBrokers,
			"topic", cfg.KafkaTopic,
		)

		// RAG Consumer: Kafka → EmbedPipeline → Qdrant
		if embedPipeline != nil {
			ragConsumer := jkafka.NewRAGConsumer(
				cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaRAGGroup,
				embedPipeline, int64(cfg.RAGSlowThresholdMs),
			)
			ragConsumer.Start(ctx)
			defer ragConsumer.Close() //nolint:errcheck
		}

		// Forecast Consumer: Kafka → Forecast Server (endpoint 미설정 시 stub)
		forecastConsumer := jkafka.NewForecastConsumer(
			cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaForecastGroup,
			cfg.KafkaForecastEndpoint,
		)
		forecastConsumer.Start(ctx)
		defer forecastConsumer.Close() //nolint:errcheck

		defer producer.Close() //nolint:errcheck
	} else if embedPipeline != nil {
		// 직접 모드: EmbedPipeline에 직접 제출 (Kafka 미사용)
		spanPub = &ingester.DirectSpanPublisher{
			Pipeline: embedPipeline,
			Builder:  rag.DocumentBuilder{SlowMs: int64(cfg.RAGSlowThresholdMs)},
		}
	}

	ing := ingester.New(ingestTraceStore, ingestMetricStore, ingestLogStore, spanPub)

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
		if embedPipeline != nil {
			embedPipeline.Close()
		}
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
	// (HTTPServer.Shutdown 전에 readyz가 503을 반환하도록 하면
	//  로드밸런서가 이 인스턴스로의 라우팅을 중단할 시간을 확보할 수 있다.)
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// gRPC GracefulStop: 진행 중인 RPC가 완료될 때까지 대기
	grpcSrv.Stop()

	if err := httpSrv.Shutdown(shutCtx); err != nil {
		slog.Error("HTTP shutdown error", "err", err)
	}

	slog.Info("shutdown complete")
}
