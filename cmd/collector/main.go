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

	"github.com/kkc/javi-collector/internal/config"
	"github.com/kkc/javi-collector/internal/ingester"
	"github.com/kkc/javi-collector/internal/rag"
	"github.com/kkc/javi-collector/internal/sampling"
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
	)

	if cfg.DisableClickHouse {
		slog.Info("using in-memory store (ClickHouse disabled)")
		traceStore = store.NewMemoryTraceStore(cfg.MemoryBufferSize)
		metricStore = store.NewMemoryMetricStore(cfg.MemoryBufferSize)
		logStore = store.NewMemoryLogStore(cfg.MemoryBufferSize)
	} else {
		chCfg := store.ClickHouseConfig{
			Addr:          cfg.ClickHouseAddr,
			Database:      cfg.ClickHouseDB,
			Username:      cfg.ClickHouseUser,
			Password:      cfg.ClickHousePassword,
			BatchSize:     cfg.BatchSize,
			FlushInterval: cfg.FlushInterval,
			ChanBuffer:    cfg.ChannelBufferSize,
			RetentionDays: cfg.RetentionDays,
			DLQDir:        cfg.DLQDir,
		}

		// 상용 APM 패턴: 하나의 공유 커넥션 풀을 세 Store가 공유한다.
		// 이전에는 Store마다 openConn을 호출해 최대 3×MaxOpenConns 커넥션이 생성됐다.
		chConn, err := store.OpenConn(chCfg)
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
		)
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

	// RAG 파이프라인 초기화 (EMBED_ENABLED=true 시)
	// Ollama(nomic-embed-text) → Qdrant 벡터 저장 → 자연어 장애 검색
	var embedPipeline *rag.EmbedPipeline
	if cfg.EmbedEnabled {
		embedClient := rag.NewOllamaEmbedClient(cfg.EmbedEndpoint, cfg.EmbedModel)
		qdrantClient := rag.NewQdrantClient(cfg.QdrantEndpoint, cfg.QdrantCollection)
		if err := qdrantClient.EnsureCollection(ctx, 768); err != nil {
			slog.Warn("qdrant collection init failed (RAG disabled)", "err", err)
		} else {
			embedPipeline = rag.NewEmbedPipeline(embedClient, qdrantClient, 1024, 32, 10*time.Second)
			embedPipeline.Start(ctx)
			slog.Info("RAG embed pipeline started",
				"endpoint", cfg.EmbedEndpoint,
				"model", cfg.EmbedModel,
				"qdrant", cfg.QdrantEndpoint,
			)
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

	ing := ingester.New(ingestTraceStore, ingestMetricStore, ingestLogStore, embedPipeline)

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
