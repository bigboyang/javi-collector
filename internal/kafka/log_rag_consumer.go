package kafka

import (
	"context"
	"encoding/json"
	"log/slog"

	kafka "github.com/segmentio/kafka-go"

	"github.com/kkc/javi-collector/internal/model"
	"github.com/kkc/javi-collector/internal/rag"
)

// LogRAGConsumer는 Kafka logs 토픽에서 LogEvent를 읽어
// ERROR 이상 심각도 로그를 EmbedPipeline을 통해 Qdrant에 벡터로 적재한다.
//
// severityNumber < 17 (ERROR 미만) 로그는 DocumentBuilder.BuildFromLog에서 nil을 반환해
// 자동으로 필터링된다.
type LogRAGConsumer struct {
	reader   *kafka.Reader
	pipeline *rag.EmbedPipeline
	builder  rag.DocumentBuilder
}

// NewLogRAGConsumer는 LogRAGConsumer를 생성한다.
//   - brokers: Kafka 브로커 목록
//   - topic: 구독할 토픽 (e.g. "logs")
//   - group: consumer group ID (e.g. "log-rag-embedder")
//   - pipeline: EmbedPipeline — Qdrant 적재 담당
func NewLogRAGConsumer(brokers []string, topic, group string, pipeline *rag.EmbedPipeline) *LogRAGConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
		Logger:   kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			slog.Warn("kafka log rag consumer error", "msg", msg)
		}),
	})
	return &LogRAGConsumer{
		reader:   r,
		pipeline: pipeline,
	}
}

// Start는 백그라운드 goroutine으로 Kafka 메시지를 소비한다.
// ctx가 취소되면 reader를 닫고 종료한다.
func (c *LogRAGConsumer) Start(ctx context.Context) {
	go func() {
		slog.Info("kafka log rag consumer started",
			"topic", c.reader.Config().Topic,
			"group", c.reader.Config().GroupID,
		)
		for {
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					slog.Info("kafka log rag consumer stopped")
					return
				}
				slog.Error("kafka log rag consumer read failed", "err", err)
				continue
			}

			var l model.LogData
			if err := json.Unmarshal(msg.Value, &l); err != nil {
				slog.Warn("kafka log rag consumer decode failed",
					"err", err, "offset", msg.Offset)
				continue
			}

			// BuildFromLog가 ERROR 미만(severityNumber < 17)은 nil을 반환해 자동 필터링
			if doc := c.builder.BuildFromLog(&l); doc != nil {
				c.pipeline.Submit(doc)
			}
		}
	}()
}

// Close는 Kafka reader를 닫는다.
func (c *LogRAGConsumer) Close() error {
	return c.reader.Close()
}
