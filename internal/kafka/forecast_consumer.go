package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/kkc/javi-collector/internal/model"
)

var kafkaConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "javi",
	Subsystem: "kafka",
	Name:      "consumer_lag",
	Help:      "Current Kafka consumer lag (messages behind latest offset).",
}, []string{"topic", "group"})

// maxConcurrentSends는 Forecast 서버로의 최대 동시 전송 goroutine 수다.
// 서버 지연 시 goroutine 누적을 방지하기 위한 semaphore 크기다.
const maxConcurrentSends = 32

// ForecastConsumer는 Kafka "spans.error" 토픽에서 span 이벤트를 읽어
// Forecast 서버로 전달한다.
//
// 현재는 Forecast 서버 미구현 상태이므로 endpoint가 비어 있으면 span을 수신만 하고 버린다.
// Forecast 서버 구현 후 KAFKA_FORECAST_ENDPOINT를 설정하면 자동 활성화된다.
//
// Forecast 서버가 구현되면 이 consumer는 span 스트림을 실시간으로 집계해
// RED 메트릭(RPS·ErrorRate·P99)을 주기적으로 Forecast 서버에 전송한다.
type ForecastConsumer struct {
	reader   *kafka.Reader
	endpoint string      // 빈 문자열이면 stub 모드 (수신 후 드롭)
	client   *http.Client
	sem      chan struct{} // semaphore: 최대 동시 전송 수 제한
}

// NewForecastConsumer는 ForecastConsumer를 생성한다.
//   - brokers: Kafka 브로커 목록
//   - topic: 구독할 토픽 (e.g. "spans.error")
//   - group: consumer group ID (e.g. "forecast-feeder")
//   - endpoint: Forecast 서버 URL (빈 문자열이면 stub 모드)
func NewForecastConsumer(brokers []string, topic, group, endpoint string) *ForecastConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
		Logger:   kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			slog.Warn("kafka forecast consumer error", "msg", msg)
		}),
	})
	return &ForecastConsumer{
		reader:   r,
		endpoint: endpoint,
		client:   &http.Client{Timeout: 5 * time.Second},
		sem:      make(chan struct{}, maxConcurrentSends),
	}
}

// spanEvent는 Forecast 서버로 전송할 span 이벤트다.
// javi-forecast의 SpanEvent Pydantic 모델과 필드명·타입이 일치한다 (snake_case).
type spanEvent struct {
	TraceID       string         `json:"trace_id"`
	SpanID        string         `json:"span_id"`
	ParentSpanID  string         `json:"parent_span_id,omitempty"`
	Name          string         `json:"name"`
	Kind          int32          `json:"kind"`
	StartTimeNano int64          `json:"start_time_nano"`
	EndTimeNano   int64          `json:"end_time_nano"`
	Attributes    map[string]any `json:"attributes,omitempty"`
	StatusCode    int32          `json:"status_code"`
	StatusMessage string         `json:"status_message,omitempty"`
	ServiceName   string         `json:"service_name"`
	ScopeName     string         `json:"scope_name,omitempty"`
}

// Start는 백그라운드 goroutine으로 Kafka 메시지를 소비한다.
// ctx가 취소되면 reader를 닫고 종료한다.
func (c *ForecastConsumer) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		topic := c.reader.Config().Topic
		group := c.reader.Config().GroupID
		for {
			select {
			case <-ticker.C:
				stats := c.reader.Stats()
				kafkaConsumerLag.WithLabelValues(topic, group).Set(float64(stats.Lag))
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		slog.Info("kafka forecast consumer started",
			"topic", c.reader.Config().Topic,
			"group", c.reader.Config().GroupID,
			"endpoint", func() string {
				if c.endpoint == "" {
					return "(stub)"
				}
				return c.endpoint
			}(),
		)
		for {
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					slog.Info("kafka forecast consumer stopped")
					return
				}
				slog.Error("kafka forecast consumer read failed", "err", err)
				continue
			}

			// Forecast 서버 미구현: endpoint가 비어 있으면 수신 후 드롭
			if c.endpoint == "" {
				continue
			}

			var sp model.SpanData
			if err := json.Unmarshal(msg.Value, &sp); err != nil {
				slog.Warn("kafka forecast consumer decode failed",
					"err", err, "offset", msg.Offset)
				continue
			}

			evt := spanEvent{
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
			// semaphore로 최대 동시 goroutine 수를 제한한다.
			// 전송 슬롯이 모두 사용 중이면 span을 드롭하고 경고를 남긴다.
			select {
			case c.sem <- struct{}{}:
				go func() {
					defer func() { <-c.sem }()
					c.send(ctx, evt)
				}()
			default:
				slog.Warn("forecast consumer: send queue full, dropping span",
					"service", evt.ServiceName)
			}
		}
	}()
}

// send는 span 이벤트를 Forecast 서버에 HTTP POST로 전송한다.
func (c *ForecastConsumer) send(ctx context.Context, evt spanEvent) {
	data, _ := json.Marshal(evt)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.endpoint+"/v1/span", bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		slog.Debug("forecast consumer send failed", "err", err)
		return
	}
	resp.Body.Close()
}

// Close는 Kafka reader를 닫는다.
func (c *ForecastConsumer) Close() error {
	return c.reader.Close()
}
