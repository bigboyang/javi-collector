// Package kafka는 span 이벤트 기반 팬아웃 파이프라인을 제공한다.
//
// 아키텍처:
//
//	Ingester → SpanProducer → Kafka "spans.error"
//	                              ├─ RAGConsumer  → EmbedPipeline → Qdrant
//	                              └─ ForecastConsumer → Forecast Server (예정)
//
// 운영 설정:
//
//	KAFKA_ENABLED=true             (기본: false)
//	KAFKA_BROKERS=localhost:9092   (콤마 구분 브로커 목록)
//	KAFKA_TOPIC=spans.error        (기본값)
//	KAFKA_RAG_GROUP=rag-embedder   (기본값)
//	KAFKA_FORECAST_GROUP=forecast-feeder (기본값)
package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/kkc/javi-collector/internal/model"
)

// SpanProducer는 span 이벤트를 Kafka 토픽에 비동기로 발행한다.
// APM의 ingestion 핫패스를 블로킹하지 않도록 Async=true로 동작한다.
// 채널이 꽉 차면 즉시 드롭한다 (완전성보다 속도 우선).
type SpanProducer struct {
	writer *kafka.Writer
}

// NewSpanProducer는 SpanProducer를 생성한다.
// ServiceName을 파티션 키로 사용해 같은 서비스의 span이 같은 파티션에 모인다.
func NewSpanProducer(brokers []string, topic string) *SpanProducer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // ServiceName 키 기반 파티셔닝
		Async:        true,          // non-blocking: 쓰기 실패는 로그만 남기고 드롭
		MaxAttempts:  3,
		WriteTimeout: 1 * time.Second,
		Logger:       kafka.LoggerFunc(func(msg string, args ...interface{}) {}), // 노이즈 억제
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			slog.Warn("kafka producer error", "msg", msg)
		}),
	}
	return &SpanProducer{writer: w}
}

// Publish는 span 이벤트를 Kafka에 비동기로 발행한다.
// JSON 직렬화 실패 시 즉시 드롭 (발생하지 않는 케이스).
func (p *SpanProducer) Publish(sp *model.SpanData) {
	data, err := json.Marshal(sp)
	if err != nil {
		slog.Warn("kafka producer marshal failed", "span_id", sp.SpanID, "err", err)
		return
	}
	// Async=true이므로 WriteMessages는 즉시 반환한다.
	_ = p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(sp.ServiceName),
		Value: data,
	})
}

// Close는 Kafka writer를 닫고 미발송 메시지를 flush한다.
func (p *SpanProducer) Close() error {
	return p.writer.Close()
}
