package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// MetricForecastConsumer는 Kafka metrics 토픽에서 MetricEvent를 읽어
// Forecast 서버로 전달한다.
//
// endpoint가 비어 있으면 stub 모드로 동작한다 (수신 후 드롭).
// Forecast 서버 구현 후 KAFKA_FORECAST_ENDPOINT를 설정하면 자동 활성화된다.
type MetricForecastConsumer struct {
	reader   *kafka.Reader
	endpoint string
	client   *http.Client
	sem      chan struct{}
}

// NewMetricForecastConsumer는 MetricForecastConsumer를 생성한다.
//   - brokers: Kafka 브로커 목록
//   - topic: 구독할 토픽 (e.g. "metrics")
//   - group: consumer group ID (e.g. "metric-forecast-feeder")
//   - endpoint: Forecast 서버 URL (빈 문자열이면 stub 모드)
func NewMetricForecastConsumer(brokers []string, topic, group, endpoint string) *MetricForecastConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
		Logger:   kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			slog.Warn("kafka metric forecast consumer error", "msg", msg)
		}),
	})
	return &MetricForecastConsumer{
		reader:   r,
		endpoint: endpoint,
		client:   &http.Client{Timeout: 5 * time.Second},
		sem:      make(chan struct{}, maxConcurrentSends),
	}
}

// Start는 백그라운드 goroutine으로 Kafka 메시지를 소비한다.
func (c *MetricForecastConsumer) Start(ctx context.Context) {
	go func() {
		slog.Info("kafka metric forecast consumer started",
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
					slog.Info("kafka metric forecast consumer stopped")
					return
				}
				slog.Error("kafka metric forecast consumer read failed", "err", err)
				continue
			}

			if c.endpoint == "" {
				continue
			}

			var ev MetricEvent
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				slog.Warn("kafka metric forecast consumer decode failed",
					"err", err, "offset", msg.Offset)
				continue
			}

			select {
			case c.sem <- struct{}{}:
				go func() {
					defer func() { <-c.sem }()
					c.send(ctx, ev)
				}()
			default:
				slog.Warn("metric forecast consumer: send queue full, dropping metric",
					"service", ev.ServiceName, "metric", ev.MetricName)
			}
		}
	}()
}

// send는 MetricEvent를 Forecast 서버에 HTTP POST로 전송한다.
func (c *MetricForecastConsumer) send(ctx context.Context, ev MetricEvent) {
	data, _ := json.Marshal(ev)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.endpoint+"/ingest/metric", bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		slog.Debug("metric forecast consumer send failed", "err", err)
		return
	}
	resp.Body.Close()
}

// Close는 Kafka reader를 닫는다.
func (c *MetricForecastConsumer) Close() error {
	return c.reader.Close()
}
