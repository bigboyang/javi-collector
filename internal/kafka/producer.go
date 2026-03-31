// Package kafka는 signal(span/metric/log) 기반 팬아웃 파이프라인을 제공한다.
//
// 아키텍처:
//
//	Ingester → SpanProducer   → Kafka "spans.error"
//	                                ├─ RAGConsumer             → EmbedPipeline → Qdrant
//	                                └─ ForecastConsumer        → Forecast Server
//	         → MetricProducer → Kafka "metrics"
//	                                └─ MetricForecastConsumer  → Forecast Server
//	         → LogProducer    → Kafka "logs"
//	                                └─ LogRAGConsumer          → EmbedPipeline → Qdrant (ERROR+)
//
// 운영 설정:
//
//	KAFKA_ENABLED=true                        (기본: false)
//	KAFKA_BROKERS=localhost:9092              (콤마 구분 브로커 목록)
//	KAFKA_TOPIC=spans.error                   (기본값)
//	KAFKA_METRICS_TOPIC=metrics               (기본값)
//	KAFKA_LOGS_TOPIC=logs                     (기본값)
//	KAFKA_RAG_GROUP=rag-embedder              (기본값)
//	KAFKA_FORECAST_GROUP=forecast-feeder      (기본값)
//	KAFKA_METRIC_FORECAST_GROUP=metric-forecast-feeder (기본값)
//	KAFKA_LOG_RAG_GROUP=log-rag-embedder      (기본값)
package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/kkc/javi-collector/internal/model"
)

// SpanEvent는 Kafka 메시지로 발행되는 경량 span 이벤트다.
// RAGConsumer와 ForecastConsumer에 필요한 필드만 포함한다.
//
// 제외 필드:
//   - ReceivedAtMs: 수신 타임스탬프는 ingestion 내부 계측용이며 하류 컨슈머에 불필요.
//   - TraceState: W3C Trace Context 전파 메타데이터; 벡터 임베딩·예측 불필요.
//   - Links: 크로스-트레이스 연결; RAG/Forecast 로직에서 미사용.
//
// JSON 태그는 model.SpanData와 동일하므로 기존 컨슈머(SpanData 역직렬화)와 호환된다.
type SpanEvent struct {
	TraceID       string         `json:"traceId"`
	SpanID        string         `json:"spanId"`
	ParentSpanID  string         `json:"parentSpanId,omitempty"`
	Name          string         `json:"name"`
	Kind          int32          `json:"kind"`
	StartTimeNano int64          `json:"startTimeNanos"`
	EndTimeNano   int64          `json:"endTimeNanos"`
	Attributes    map[string]any `json:"attributes,omitempty"`
	StatusCode    int32          `json:"statusCode"`
	StatusMessage string         `json:"statusMessage,omitempty"`
	ServiceName   string         `json:"serviceName"`
	ScopeName     string         `json:"scopeName,omitempty"`
}

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
// SpanData에서 SpanEvent로 projection해 ClickHouse 전용 필드(ReceivedAtMs, TraceState, Links)를 제외한다.
// JSON 직렬화 실패 시 즉시 드롭 (발생하지 않는 케이스).
func (p *SpanProducer) Publish(sp *model.SpanData) {
	ev := SpanEvent{
		TraceID:       sp.TraceID,
		SpanID:        sp.SpanID,
		ParentSpanID:  sp.ParentSpanID,
		Name:          sp.Name,
		Kind:          sp.Kind,
		StartTimeNano: sp.StartTimeNano,
		EndTimeNano:   sp.EndTimeNano,
		Attributes:    sp.Attributes,
		StatusCode:    sp.StatusCode,
		StatusMessage: sp.StatusMessage,
		ServiceName:   sp.ServiceName,
		ScopeName:     sp.ScopeName,
	}
	data, err := json.Marshal(ev)
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

// ---- MetricProducer ----

// MetricEvent는 Kafka metrics 토픽에 발행되는 단일 metric 측정값이다.
// ForecastConsumer가 시계열 예측에 사용한다.
//
// HISTOGRAM은 Count > 0이면 Sum/Count 평균값을, 아니면 Sum을 Value로 사용한다.
type MetricEvent struct {
	ServiceName string         `json:"service_name"`
	MetricName  string         `json:"metric_name"`
	MetricType  string         `json:"metric_type"`
	Unit        string         `json:"unit,omitempty"`
	Value       float64        `json:"value"`
	TimestampMs int64          `json:"timestamp_ms"`
	Attributes  map[string]any `json:"attributes,omitempty"`
}

// MetricProducer는 metric 이벤트를 Kafka metrics 토픽에 비동기로 발행한다.
type MetricProducer struct {
	writer *kafka.Writer
}

// NewMetricProducer는 MetricProducer를 생성한다.
// service_name + metric_name을 파티션 키로 사용해 동일 시계열이 같은 파티션에 모인다.
func NewMetricProducer(brokers []string, topic string) *MetricProducer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		Async:        true,
		MaxAttempts:  3,
		WriteTimeout: 1 * time.Second,
		Logger:       kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			slog.Warn("kafka metric producer error", "msg", msg)
		}),
	}
	return &MetricProducer{writer: w}
}

// PublishMetric은 MetricData의 각 DataPoint를 개별 MetricEvent로 Kafka에 발행한다.
func (p *MetricProducer) PublishMetric(m *model.MetricData) {
	for _, dp := range m.DataPoints {
		value := dp.Value
		if string(m.Type) == "HISTOGRAM" && dp.Count > 0 {
			value = dp.Sum / float64(dp.Count) // 평균값
		} else if string(m.Type) == "HISTOGRAM" {
			value = dp.Sum
		}

		ev := MetricEvent{
			ServiceName: m.ServiceName,
			MetricName:  m.Name,
			MetricType:  string(m.Type),
			Unit:        m.Unit,
			Value:       value,
			TimestampMs: dp.TimeNanos / 1_000_000,
			Attributes:  dp.Attributes,
		}
		data, err := json.Marshal(ev)
		if err != nil {
			slog.Warn("kafka metric producer marshal failed", "metric", m.Name, "err", err)
			continue
		}
		key := []byte(m.ServiceName + "." + m.Name)
		_ = p.writer.WriteMessages(context.Background(), kafka.Message{
			Key:   key,
			Value: data,
		})
	}
}

// Close는 Kafka writer를 닫는다.
func (p *MetricProducer) Close() error {
	return p.writer.Close()
}

// ---- LogProducer ----

// LogEvent는 Kafka logs 토픽에 발행되는 log record다.
// JSON 태그는 model.LogData와 동일해 컨슈머가 LogData로 역직렬화할 수 있다.
type LogEvent struct {
	TimestampNanos int64          `json:"timestampNanos"`
	SeverityNumber int32          `json:"severityNumber"`
	SeverityText   string         `json:"severityText"`
	Body           string         `json:"body"`
	Attributes     map[string]any `json:"attributes,omitempty"`
	TraceID        string         `json:"traceId,omitempty"`
	SpanID         string         `json:"spanId,omitempty"`
	ServiceName    string         `json:"serviceName"`
	ScopeName      string         `json:"scopeName,omitempty"`
	ReceivedAtMs   int64          `json:"receivedAtMs"`
}

// LogProducer는 log 이벤트를 Kafka logs 토픽에 비동기로 발행한다.
type LogProducer struct {
	writer *kafka.Writer
}

// NewLogProducer는 LogProducer를 생성한다.
// ServiceName을 파티션 키로 사용한다.
func NewLogProducer(brokers []string, topic string) *LogProducer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		Async:        true,
		MaxAttempts:  3,
		WriteTimeout: 1 * time.Second,
		Logger:       kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			slog.Warn("kafka log producer error", "msg", msg)
		}),
	}
	return &LogProducer{writer: w}
}

// PublishLog는 log 이벤트를 Kafka에 비동기로 발행한다.
func (p *LogProducer) PublishLog(l *model.LogData) {
	ev := LogEvent{
		TimestampNanos: l.TimestampNanos,
		SeverityNumber: l.SeverityNumber,
		SeverityText:   l.SeverityText,
		Body:           l.Body,
		Attributes:     l.Attributes,
		TraceID:        l.TraceID,
		SpanID:         l.SpanID,
		ServiceName:    l.ServiceName,
		ScopeName:      l.ScopeName,
		ReceivedAtMs:   l.ReceivedAtMs,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		slog.Warn("kafka log producer marshal failed", "service", l.ServiceName, "err", err)
		return
	}
	_ = p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(l.ServiceName),
		Value: data,
	})
}

// Close는 Kafka writer를 닫는다.
func (p *LogProducer) Close() error {
	return p.writer.Close()
}
