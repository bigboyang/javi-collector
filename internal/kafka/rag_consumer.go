package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/kkc/javi-collector/internal/model"
	"github.com/kkc/javi-collector/internal/rag"
	"github.com/kkc/javi-collector/internal/store"
)

// logLookbackWindow는 span traceId로 연관 로그를 조회할 때 사용하는 시간 범위다.
// span 발생 전후 이 범위의 로그를 조회한다.
const logLookbackWindow = 5 * time.Minute

// RAGConsumer는 Kafka "spans.error" 토픽에서 span 이벤트를 읽어
// EmbedPipeline을 통해 Qdrant에 벡터로 적재한다.
//
// consumer group 분리로 ForecastConsumer와 독립적으로 오프셋을 관리한다.
type RAGConsumer struct {
	reader   *kafka.Reader
	pipeline *rag.EmbedPipeline
	builder  rag.DocumentBuilder
	logStore store.LogStore // traceId 기반 연관 로그 조회용 (nil이면 로그 없이 적재)
}

// NewRAGConsumer는 RAGConsumer를 생성한다.
//   - brokers: Kafka 브로커 목록
//   - topic: 구독할 토픽 (e.g. "spans.error")
//   - group: consumer group ID (e.g. "rag-embedder")
//   - pipeline: EmbedPipeline — Qdrant 적재 담당
//   - logStore: LogStore — traceId 기반 연관 로그 조회용 (nil 허용)
//   - slowMs: SLOW span 인덱싱 임계값(ms). 0이면 비활성화.
func NewRAGConsumer(brokers []string, topic, group string, pipeline *rag.EmbedPipeline, logStore store.LogStore, slowMs int64) *RAGConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024, // 10 MB
		Logger:   kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			slog.Warn("kafka rag consumer error", "msg", msg)
		}),
	})
	return &RAGConsumer{
		reader:   r,
		pipeline: pipeline,
		builder:  rag.DocumentBuilder{SlowMs: slowMs},
		logStore: logStore,
	}
}

// Start는 백그라운드 goroutine으로 Kafka 메시지를 소비한다.
// ctx가 취소되면 reader를 닫고 종료한다.
func (c *RAGConsumer) Start(ctx context.Context) {
	go func() {
		slog.Info("kafka rag consumer started",
			"topic", c.reader.Config().Topic,
			"group", c.reader.Config().GroupID,
		)
		for {
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					slog.Info("kafka rag consumer stopped")
					return
				}
				slog.Error("kafka rag consumer read failed", "err", err)
				continue
			}

			var sp model.SpanData
			if err := json.Unmarshal(msg.Value, &sp); err != nil {
				slog.Warn("kafka rag consumer decode failed",
					"err", err, "offset", msg.Offset)
				continue
			}

			var correlatedLogs []*model.LogData
			if c.logStore != nil && sp.TraceID != "" {
				spanMs := sp.StartTimeNano / 1_000_000
				logs, err := c.logStore.QueryLogs(ctx, store.LogQuery{
					TraceID: sp.TraceID,
					FromMs:  spanMs - logLookbackWindow.Milliseconds(),
					ToMs:    spanMs + logLookbackWindow.Milliseconds(),
					Limit:   50,
				})
				if err != nil {
					slog.Warn("kafka rag consumer log query failed",
						"err", err, "traceId", sp.TraceID)
				} else {
					correlatedLogs = logs
				}
			}

			if doc := c.builder.BuildFromSpan(&sp, correlatedLogs); doc != nil {
				c.pipeline.Submit(doc)
			}
		}
	}()
}

// Close는 Kafka reader를 닫는다.
func (c *RAGConsumer) Close() error {
	return c.reader.Close()
}
