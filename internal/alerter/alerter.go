// Package alerter는 AIOps 이상 감지 결과를 외부 시스템(Slack / 일반 Webhook)으로
// Push하는 백그라운드 고루틴을 제공한다.
//
// GAP-05: Alert Routing & Escalation
//
// 동작 방식 (매 Interval):
//  1. anomalies 테이블에서 직전 interval 이후에 탐지된 신규 이상 이벤트를 조회
//  2. MinSeverity 필터 적용 (warning | critical)
//  3. Alert Routes (alert_routes 테이블) 로드 → 이벤트별 매칭 라우트 탐색
//     - 매칭 라우트가 없으면 cfg.SlackWebhookURL / cfg.WebhookURL 레거시 목적지로 폴백
//     - 매칭 라우트가 있으면 각 라우트의 slack_url / webhook_url 로 전송
//  4. 전송된 이벤트를 alert_events 감사 로그에 기록
//  5. 에스컬레이션 체크 (EscalationAfterMin 초과 & ack 없음 → escalation 목적지로 재전송)
package alerter

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kkc/javi-collector/internal/store"
)

// Config holds Alerter configuration.
type Config struct {
	// WebhookURL: 일반 JSON webhook URL. 라우트가 없을 때 폴백으로 사용한다.
	WebhookURL string
	// SlackWebhookURL: Slack Incoming Webhook URL. 라우트가 없을 때 폴백으로 사용한다.
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

// firedEvent는 라우팅되어 전송된 알림 이벤트의 런타임 상태다.
// 에스컬레이션 체크에 사용하며, 재시작 시 초기화된다.
type firedEvent struct {
	eventID   string
	routeID   string         // 빈 문자열 = 레거시 폴백
	route     store.AlertRoute
	anomalyID string
	service   string
	severity  string
	firedAt   time.Time
}

// Alerter는 이상 감지 알림 백그라운드 고루틴을 관리한다.
type Alerter struct {
	conn        driver.Conn
	db          string
	cfg         Config
	httpClient  *http.Client
	lastChecked time.Time

	// routeStore: alert_routes / alert_events ClickHouse 관리.
	// nil이면 레거시 단일 목적지 모드로 동작한다.
	routeStore *store.AlertRouteStore

	// firedEvents: 이번 프로세스 생애 동안 발송된 이벤트 (에스컬레이션 추적)
	mu          sync.Mutex
	firedEvents map[string]*firedEvent // eventID → *firedEvent

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
		conn:        conn,
		db:          db,
		cfg:         cfg,
		httpClient:  &http.Client{Timeout: 10 * time.Second},
		firedEvents: make(map[string]*firedEvent),
		stopCh:      make(chan struct{}),
	}
}

// SetRouteStore는 Alert Routing & Escalation 저장소를 등록한다.
// Start() 전에 호출해야 한다.
func (a *Alerter) SetRouteStore(rs *store.AlertRouteStore) {
	a.routeStore = rs
}

// Enabled는 WebhookURL 또는 SlackWebhookURL 중 하나라도 설정되어 있거나
// routeStore가 주입되어 있으면 true를 반환한다.
func (a *Alerter) Enabled() bool {
	return a.cfg.WebhookURL != "" || a.cfg.SlackWebhookURL != "" || a.routeStore != nil
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

	if len(events) > 0 {
		filtered := filterBySeverity(events, a.cfg.MinSeverity)
		if len(filtered) > 0 {
			slog.Info("alerter: sending alerts", "count", len(filtered))
			a.routeAndSend(ctx, filtered)
		}
	}

	// 에스컬레이션 체크 (routeStore가 있을 때만)
	if a.routeStore != nil {
		a.checkEscalations(ctx)
	}

	// 24시간 이상 된 firedEvents 정리
	a.cleanupFiredEvents(24 * time.Hour)
}

// routeAndSend는 이벤트 목록을 라우팅 규칙에 따라 각 목적지로 전송한다.
// routeStore가 nil이면 레거시 단일 목적지로 폴백한다.
func (a *Alerter) routeAndSend(ctx context.Context, events []AnomalyEvent) {
	if a.routeStore == nil {
		// 레거시 단일 목적지 모드
		a.legacySend(ctx, events)
		return
	}

	routes, err := a.routeStore.ListRoutes(ctx)
	if err != nil {
		slog.Warn("alerter: load routes failed, falling back to legacy destinations", "err", err)
		a.legacySend(ctx, events)
		return
	}

	now := time.Now()

	for _, event := range events {
		matched := false
		for _, route := range routes {
			if !route.HasDestination() {
				continue
			}
			if !route.MatchesEvent(event.ServiceName, event.Severity, event.AnomalyType) {
				continue
			}
			matched = true

			// 이 라우트로 전송
			if route.SlackURL != "" {
				if err := a.postSlackToURL(ctx, route.SlackURL, []AnomalyEvent{event}); err != nil {
					slog.Warn("alerter: route slack send failed", "route", route.Name, "err", err)
				}
			}
			if route.WebhookURL != "" {
				if err := a.postWebhookToURL(ctx, route.WebhookURL, []AnomalyEvent{event}); err != nil {
					slog.Warn("alerter: route webhook send failed", "route", route.Name, "err", err)
				}
			}

			// 런타임 발송 이벤트 기록 (에스컬레이션 추적)
			eventID := newEventID()
			fe := &firedEvent{
				eventID:   eventID,
				routeID:   route.ID,
				route:     route,
				anomalyID: event.ID,
				service:   event.ServiceName,
				severity:  event.Severity,
				firedAt:   now,
			}
			a.mu.Lock()
			a.firedEvents[eventID] = fe
			a.mu.Unlock()

			// ClickHouse 감사 로그 (비동기 — 실패해도 전송에 영향 없음)
			go func(id, routeID string, ev AnomalyEvent, firedAt time.Time) {
				logCtx, logCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer logCancel()
				_ = a.routeStore.InsertAlertEvent(logCtx, store.AlertEvent{
					ID:          id,
					AnomalyID:   ev.ID,
					RouteID:     routeID,
					ServiceName: ev.ServiceName,
					Severity:    ev.Severity,
					AnomalyType: ev.AnomalyType,
					FiredAt:     firedAt,
				})
			}(eventID, route.ID, event, now)
		}

		// 매칭 라우트 없음 → 레거시 폴백
		if !matched {
			a.legacySend(ctx, []AnomalyEvent{event})
		}
	}
}

// checkEscalations는 에스컬레이션 대상 이벤트를 찾아 escalation 목적지로 재전송한다.
func (a *Alerter) checkEscalations(ctx context.Context) {
	now := time.Now()

	a.mu.Lock()
	pending := make([]*firedEvent, 0)
	for _, fe := range a.firedEvents {
		if !fe.route.HasEscalation() {
			continue
		}
		// 이미 에스컬레이션됐거나 ack된 것은 제외
		if a.routeStore.IsEscalated(fe.eventID) || a.routeStore.IsAcked(fe.eventID) {
			continue
		}
		escalateAt := fe.firedAt.Add(time.Duration(fe.route.EscalationAfterMin) * time.Minute)
		if now.After(escalateAt) {
			pending = append(pending, fe)
		}
	}
	a.mu.Unlock()

	for _, fe := range pending {
		slog.Warn("alerter: escalating alert",
			"event_id", fe.eventID,
			"route", fe.route.Name,
			"service", fe.service,
			"severity", fe.severity,
			"fired_at", fe.firedAt,
		)

		escalationPayload := fmt.Sprintf("[ESCALATION] Route=%s Service=%s Severity=%s — unacknowledged for %d min",
			fe.route.Name, fe.service, fe.severity, fe.route.EscalationAfterMin)

		if fe.route.EscalateSlackURL != "" {
			if err := a.postSlackEscalation(ctx, fe.route.EscalateSlackURL, fe, escalationPayload); err != nil {
				slog.Warn("alerter: escalation slack failed", "err", err)
			}
		}
		if fe.route.EscalateWebhookURL != "" {
			if err := a.postWebhookEscalation(ctx, fe.route.EscalateWebhookURL, fe, escalationPayload); err != nil {
				slog.Warn("alerter: escalation webhook failed", "err", err)
			}
		}

		a.routeStore.MarkEscalated(fe.eventID)
	}
}

// cleanupFiredEvents는 maxAge보다 오래된 firedEvents를 정리한다.
func (a *Alerter) cleanupFiredEvents(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, fe := range a.firedEvents {
		if fe.firedAt.Before(cutoff) {
			delete(a.firedEvents, id)
		}
	}
}

// legacySend는 cfg의 단일 목적지로 이벤트 배치를 전송한다 (하위 호환).
func (a *Alerter) legacySend(ctx context.Context, events []AnomalyEvent) {
	if a.cfg.SlackWebhookURL != "" {
		if err := a.postSlackToURL(ctx, a.cfg.SlackWebhookURL, events); err != nil {
			slog.Warn("alerter: slack send failed", "err", err)
		}
	}
	if a.cfg.WebhookURL != "" {
		if err := a.postWebhookToURL(ctx, a.cfg.WebhookURL, events); err != nil {
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

// postWebhookToURL은 이벤트 목록을 JSON 배열로 지정 URL에 POST한다.
func (a *Alerter) postWebhookToURL(ctx context.Context, url string, events []AnomalyEvent) error {
	payload, err := json.Marshal(map[string]any{
		"anomalies":  events,
		"count":      len(events),
		"alerted_at": time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return a.post(ctx, url, payload, "application/json")
}

// postSlackToURL은 Slack Incoming Webhook 포맷으로 메시지를 지정 URL에 POST한다.
func (a *Alerter) postSlackToURL(ctx context.Context, url string, events []AnomalyEvent) error {
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
	return a.post(ctx, url, payload, "application/json")
}

// postSlackEscalation은 에스컬레이션 알림을 Slack으로 전송한다.
func (a *Alerter) postSlackEscalation(ctx context.Context, url string, fe *firedEvent, msg string) error {
	payload, err := json.Marshal(map[string]any{
		"text": fmt.Sprintf(":sos: *[ESCALATION]* %s\nevent_id=%s  service=%s  severity=%s  fired=%s",
			msg, fe.eventID, fe.service, fe.severity, fe.firedAt.UTC().Format(time.RFC3339)),
	})
	if err != nil {
		return err
	}
	return a.post(ctx, url, payload, "application/json")
}

// postWebhookEscalation은 에스컬레이션 알림을 JSON webhook으로 전송한다.
func (a *Alerter) postWebhookEscalation(ctx context.Context, url string, fe *firedEvent, msg string) error {
	payload, err := json.Marshal(map[string]any{
		"type":       "escalation",
		"event_id":   fe.eventID,
		"route_id":   fe.routeID,
		"service":    fe.service,
		"severity":   fe.severity,
		"fired_at":   fe.firedAt.UTC(),
		"message":    msg,
		"alerted_at": time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return a.post(ctx, url, payload, "application/json")
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

// newEventID는 발송 이벤트 추적용 랜덤 ID를 생성한다.
func newEventID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
