// Package alerter는 AIOps 이상 감지 결과를 외부 시스템(Slack / 일반 Webhook)으로
// Push하는 백그라운드 고루틴을 제공한다.
//
// 동작 방식 (매 Interval):
//  1. anomalies 테이블에서 직전 interval 이후에 탐지된 신규 이상 이벤트를 조회
//  2. MinSeverity 필터 적용 (warning | critical)
//  3. SlackWebhookURL이 설정된 경우 Slack 포맷으로 POST
//  4. WebhookURL이 설정된 경우 JSON 포맷으로 POST
package alerter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Config holds Alerter configuration.
type Config struct {
	// WebhookURL: 일반 JSON webhook URL. 비어 있으면 비활성화.
	WebhookURL string
	// SlackWebhookURL: Slack Incoming Webhook URL. 비어 있으면 비활성화.
	SlackWebhookURL string
	// Interval: anomalies 테이블 폴링 주기. (기본 1m)
	Interval time.Duration
	// MinSeverity: 알림을 보낼 최소 severity. "warning" 또는 "critical". (기본 "warning")
	MinSeverity string
}

// AnomalyEvent는 알림 대상 이상 이벤트 하나.
type AnomalyEvent struct {
	ID            string
	ServiceName   string
	SpanName      string
	AnomalyType   string
	Minute        time.Time
	CurrentValue  float64
	BaselineValue float64
	ZScore        float64
	Severity      string
	DetectedAt    time.Time
}

// Alerter는 이상 감지 알림 백그라운드 고루틴을 관리한다.
type Alerter struct {
	conn        driver.Conn
	db          string
	cfg         Config
	httpClient  *http.Client
	lastChecked time.Time

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New creates a new Alerter.
func New(conn driver.Conn, db string, cfg Config) *Alerter {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}
	if cfg.MinSeverity == "" {
		cfg.MinSeverity = "warning"
	}
	return &Alerter{
		conn:       conn,
		db:         db,
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		stopCh:     make(chan struct{}),
	}
}

// Enabled는 WebhookURL 또는 SlackWebhookURL 중 하나라도 설정되어 있으면 true를 반환한다.
func (a *Alerter) Enabled() bool {
	return a.cfg.WebhookURL != "" || a.cfg.SlackWebhookURL != ""
}

// Start launches the background goroutine.
func (a *Alerter) Start() {
	a.lastChecked = time.Now()
	a.wg.Add(1)
	go a.run()
}

// Stop signals the goroutine to stop and waits for completion.
func (a *Alerter) Stop() {
	close(a.stopCh)
	a.wg.Wait()
}

func (a *Alerter) run() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.process()
		case <-a.stopCh:
			return
		}
	}
}

func (a *Alerter) process() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	since := a.lastChecked
	now := time.Now()

	events, err := a.fetchNewAnomalies(ctx, since, now)
	if err != nil {
		slog.Error("alerter: fetch anomalies failed", "err", err)
		return
	}

	a.lastChecked = now

	if len(events) == 0 {
		return
	}

	// severity 필터
	filtered := filterBySeverity(events, a.cfg.MinSeverity)
	if len(filtered) == 0 {
		return
	}

	slog.Info("alerter: sending alerts", "count", len(filtered))

	if a.cfg.SlackWebhookURL != "" {
		if err := a.sendSlack(ctx, filtered); err != nil {
			slog.Warn("alerter: slack send failed", "err", err)
		}
	}
	if a.cfg.WebhookURL != "" {
		if err := a.sendWebhook(ctx, filtered); err != nil {
			slog.Warn("alerter: webhook send failed", "err", err)
		}
	}
}

// fetchNewAnomalies는 since ~ until 사이에 detected_at이 있는 이상 이벤트를 반환한다.
func (a *Alerter) fetchNewAnomalies(ctx context.Context, since, until time.Time) ([]AnomalyEvent, error) {
	q := fmt.Sprintf(`
SELECT id, service_name, span_name, anomaly_type, minute,
       current_value, baseline_value, z_score, severity, detected_at
FROM %s.anomalies
WHERE detected_at > ? AND detected_at <= ?
ORDER BY detected_at ASC`, a.db)

	rows, err := a.conn.Query(ctx, q, since, until)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AnomalyEvent
	for rows.Next() {
		var e AnomalyEvent
		if err := rows.Scan(
			&e.ID, &e.ServiceName, &e.SpanName, &e.AnomalyType, &e.Minute,
			&e.CurrentValue, &e.BaselineValue, &e.ZScore, &e.Severity, &e.DetectedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// sendWebhook은 이벤트 목록을 JSON 배열로 POST한다.
func (a *Alerter) sendWebhook(ctx context.Context, events []AnomalyEvent) error {
	payload, err := json.Marshal(map[string]any{
		"anomalies":  events,
		"count":      len(events),
		"alerted_at": time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return a.post(ctx, a.cfg.WebhookURL, payload, "application/json")
}

// sendSlack은 Slack Incoming Webhook 포맷으로 메시지를 전송한다.
func (a *Alerter) sendSlack(ctx context.Context, events []AnomalyEvent) error {
	// 최대 5개까지만 인라인으로 표시 (너무 긴 메시지 방지)
	limit := len(events)
	if limit > 5 {
		limit = 5
	}

	var sb strings.Builder
	for _, e := range events[:limit] {
		icon := ":warning:"
		if e.Severity == "critical" {
			icon = ":rotating_light:"
		}
		fmt.Fprintf(&sb, "%s *[%s]* `%s/%s`\n  type=%s  z=%.1f  value=%.2f  baseline=%.2f\n",
			icon, strings.ToUpper(e.Severity),
			e.ServiceName, e.SpanName,
			e.AnomalyType, e.ZScore, e.CurrentValue, e.BaselineValue,
		)
	}
	if len(events) > 5 {
		fmt.Fprintf(&sb, "_...and %d more anomalies_\n", len(events)-5)
	}

	payload, err := json.Marshal(map[string]any{
		"text": fmt.Sprintf(":bar_chart: *javi-collector — %d anomaly alert(s)*\n%s",
			len(events), sb.String()),
	})
	if err != nil {
		return err
	}
	return a.post(ctx, a.cfg.SlackWebhookURL, payload, "application/json")
}

func (a *Alerter) post(ctx context.Context, url string, body []byte, contentType string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// filterBySeverity는 minSeverity("warning"|"critical") 이상의 이벤트만 반환한다.
func filterBySeverity(events []AnomalyEvent, minSeverity string) []AnomalyEvent {
	if minSeverity == "warning" {
		return events // warning은 모두 통과
	}
	var out []AnomalyEvent
	for _, e := range events {
		if e.Severity == "critical" {
			out = append(out, e)
		}
	}
	return out
}
