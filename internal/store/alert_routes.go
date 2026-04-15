// Package store: alert_routes.go — Alert Routing & Escalation 저장소
//
// GAP-05: Alert Routing & Escalation
//
// 두 개의 ClickHouse 테이블을 관리한다:
//   - alert_routes: 라우팅 규칙 (영속, ReplacingMergeTree 소프트 삭제)
//   - alert_events: 발사된 알림 감사 로그 (append-only, 30일 TTL)
//
// 런타임 ack/escalation 상태는 sync.Map 인메모리 캐시로 빠르게 처리한다.
// (재시작 시 초기화 — 에스컬레이션 윈도우가 짧으므로 실용적으로 허용)
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// AlertRoute는 알림 라우팅 규칙이다.
// MatchService / MatchSeverity / MatchAnomalyType 중 빈 문자열은 "모두 일치"를 의미한다.
type AlertRoute struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	MatchService       string    `json:"match_service"`        // glob 패턴 ("payment-*"), 빈 = 전체
	MatchSeverity      string    `json:"match_severity"`       // "", "warning", "critical"
	MatchAnomalyType   string    `json:"match_anomaly_type"`   // "", "latency", "error_rate"
	SlackURL           string    `json:"slack_url"`
	WebhookURL         string    `json:"webhook_url"`
	Priority           int32     `json:"priority"`              // 낮을수록 높은 우선순위
	EscalationAfterMin int32     `json:"escalation_after_min"`  // 0 = 에스컬레이션 없음
	EscalateSlackURL   string    `json:"escalate_slack_url"`
	EscalateWebhookURL string    `json:"escalate_webhook_url"`
	Enabled            bool      `json:"enabled"`
	CreatedAt          time.Time `json:"created_at"`
}

// MatchesEvent는 이 라우트가 주어진 이벤트에 적용되는지 판단한다.
// glob 패턴은 path.Match 로 처리한다 (*, ? 사용 가능).
func (r AlertRoute) MatchesEvent(serviceName, severity, anomalyType string) bool {
	if r.MatchSeverity != "" && r.MatchSeverity != severity {
		return false
	}
	if r.MatchAnomalyType != "" && r.MatchAnomalyType != anomalyType {
		return false
	}
	if r.MatchService != "" {
		matched, _ := path.Match(r.MatchService, serviceName)
		if !matched {
			return false
		}
	}
	return true
}

// HasDestination은 목적지 URL이 하나라도 설정되어 있으면 true를 반환한다.
func (r AlertRoute) HasDestination() bool {
	return r.SlackURL != "" || r.WebhookURL != ""
}

// HasEscalation은 에스컬레이션이 설정되어 있으면 true를 반환한다.
func (r AlertRoute) HasEscalation() bool {
	return r.EscalationAfterMin > 0 && (r.EscalateSlackURL != "" || r.EscalateWebhookURL != "")
}

// AlertEvent는 발사된 알림 이벤트 감사 로그다.
type AlertEvent struct {
	ID          string    `json:"id"`
	AnomalyID   string    `json:"anomaly_id"`
	RouteID     string    `json:"route_id"`    // 빈 문자열 = 레거시 기본 발송
	ServiceName string    `json:"service_name"`
	Severity    string    `json:"severity"`
	AnomalyType string    `json:"anomaly_type"`
	FiredAt     time.Time `json:"fired_at"`
	AckedAt     time.Time `json:"acked_at"`    // zero = ack 안 됨
	Escalated   bool      `json:"escalated"`
}

// AlertRouteStore는 alert_routes + alert_events 테이블을 관리한다.
type AlertRouteStore struct {
	conn driver.Conn
	db   string

	// 런타임 인메모리 상태 — 재시작 시 초기화
	ackedEvents     sync.Map // eventID(string) → struct{}
	escalatedEvents sync.Map // eventID(string) → struct{}
}

// NewAlertRouteStore는 AlertRouteStore를 초기화하고 테이블을 생성한다.
func NewAlertRouteStore(conn driver.Conn, db string) (*AlertRouteStore, error) {
	s := &AlertRouteStore{conn: conn, db: db}
	if err := s.init(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *AlertRouteStore) init(ctx context.Context) error {
	if err := s.conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.alert_routes (
    id                   String,
    name                 String                 DEFAULT '',
    match_service        String                 DEFAULT '',
    match_severity       LowCardinality(String) DEFAULT '',
    match_anomaly_type   String                 DEFAULT '',
    slack_url            String                 DEFAULT '',
    webhook_url          String                 DEFAULT '',
    priority             Int32                  DEFAULT 0,
    escalation_after_min Int32                  DEFAULT 0,
    escalate_slack_url   String                 DEFAULT '',
    escalate_webhook_url String                 DEFAULT '',
    enabled              UInt8                  DEFAULT 1,
    created_at           DateTime               DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY id;`, s.db)); err != nil {
		return fmt.Errorf("create alert_routes: %w", err)
	}

	if err := s.conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.alert_events (
    id           String,
    anomaly_id   String                 DEFAULT '',
    route_id     String                 DEFAULT '',
    service_name LowCardinality(String),
    severity     LowCardinality(String),
    anomaly_type LowCardinality(String) DEFAULT '',
    fired_at     DateTime,
    acked_at     DateTime               DEFAULT toDateTime(0),
    escalated    UInt8                  DEFAULT 0,
    dt           Date                   DEFAULT toDate(fired_at)
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, fired_at)
TTL dt + INTERVAL 30 DAY;`, s.db)); err != nil {
		return fmt.Errorf("create alert_events: %w", err)
	}
	return nil
}

// alertRouteID는 16진수 8바이트 랜덤 ID를 생성한다.
func alertRouteID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// UpsertRoute는 라우팅 규칙을 삽입하거나 업데이트한다.
// r.ID가 비어 있으면 자동 생성한다.
func (s *AlertRouteStore) UpsertRoute(ctx context.Context, r *AlertRoute) error {
	if r.ID == "" {
		r.ID = alertRouteID()
	}
	r.CreatedAt = time.Now()
	enabled := uint8(0)
	if r.Enabled {
		enabled = 1
	}
	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(`
INSERT INTO %s.alert_routes
(id, name, match_service, match_severity, match_anomaly_type,
 slack_url, webhook_url, priority,
 escalation_after_min, escalate_slack_url, escalate_webhook_url,
 enabled, created_at) VALUES`, s.db))
	if err != nil {
		return err
	}
	if err := batch.Append(
		r.ID, r.Name, r.MatchService, r.MatchSeverity, r.MatchAnomalyType,
		r.SlackURL, r.WebhookURL, r.Priority,
		r.EscalationAfterMin, r.EscalateSlackURL, r.EscalateWebhookURL,
		enabled, r.CreatedAt,
	); err != nil {
		return err
	}
	return batch.Send()
}

// DeleteRoute는 라우팅 규칙을 비활성화(enabled=0)로 소프트 삭제한다.
// ReplacingMergeTree이므로 동일 id에 더 최신 created_at으로 새 버전을 삽입한다.
func (s *AlertRouteStore) DeleteRoute(ctx context.Context, id string) error {
	return s.conn.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.alert_routes
    (id, name, match_service, match_severity, match_anomaly_type,
     slack_url, webhook_url, priority, escalation_after_min,
     escalate_slack_url, escalate_webhook_url, enabled, created_at)
SELECT id, name, match_service, match_severity, match_anomaly_type,
       slack_url, webhook_url, priority, escalation_after_min,
       escalate_slack_url, escalate_webhook_url, 0, now()
FROM %s.alert_routes FINAL
WHERE id = ?`, s.db, s.db), id)
}

// ListRoutes는 활성화된 라우팅 규칙 목록을 반환한다 (priority 오름차순).
func (s *AlertRouteStore) ListRoutes(ctx context.Context) ([]AlertRoute, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT id, name, match_service, match_severity, match_anomaly_type,
       slack_url, webhook_url, priority,
       escalation_after_min, escalate_slack_url, escalate_webhook_url,
       enabled, created_at
FROM %s.alert_routes FINAL
WHERE enabled = 1
ORDER BY priority ASC, created_at ASC`, s.db))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AlertRoute
	for rows.Next() {
		var r AlertRoute
		var enabled uint8
		if err := rows.Scan(
			&r.ID, &r.Name, &r.MatchService, &r.MatchSeverity, &r.MatchAnomalyType,
			&r.SlackURL, &r.WebhookURL, &r.Priority,
			&r.EscalationAfterMin, &r.EscalateSlackURL, &r.EscalateWebhookURL,
			&enabled, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		r.Enabled = enabled == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

// InsertAlertEvent는 발사된 알림 이벤트를 감사 로그에 기록한다.
func (s *AlertRouteStore) InsertAlertEvent(ctx context.Context, e AlertEvent) error {
	if e.ID == "" {
		e.ID = alertRouteID()
	}
	escalated := uint8(0)
	if e.Escalated {
		escalated = 1
	}
	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(`
INSERT INTO %s.alert_events
(id, anomaly_id, route_id, service_name, severity, anomaly_type, fired_at, acked_at, escalated)
VALUES`, s.db))
	if err != nil {
		return err
	}
	if err := batch.Append(
		e.ID, e.AnomalyID, e.RouteID, e.ServiceName, e.Severity, e.AnomalyType,
		e.FiredAt, e.AckedAt, escalated,
	); err != nil {
		return err
	}
	return batch.Send()
}

// AckEvent는 이벤트를 인메모리에서 ack 처리한다.
// 에스컬레이션 억제에 사용된다.
func (s *AlertRouteStore) AckEvent(eventID string) {
	s.ackedEvents.Store(eventID, struct{}{})
}

// IsAcked는 이벤트가 ack 되었는지 확인한다.
func (s *AlertRouteStore) IsAcked(eventID string) bool {
	_, ok := s.ackedEvents.Load(eventID)
	return ok
}

// IsEscalated는 이벤트가 이미 에스컬레이션되었는지 확인한다.
func (s *AlertRouteStore) IsEscalated(eventID string) bool {
	_, ok := s.escalatedEvents.Load(eventID)
	return ok
}

// MarkEscalated는 이벤트를 에스컬레이션 완료로 표시한다.
func (s *AlertRouteStore) MarkEscalated(eventID string) {
	s.escalatedEvents.Store(eventID, struct{}{})
}

// ListAlertHistory는 최근 알림 이벤트 목록을 반환한다.
func (s *AlertRouteStore) ListAlertHistory(ctx context.Context, service string, limit int) ([]AlertEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	var (
		q    string
		args []any
	)
	if service != "" {
		q = fmt.Sprintf(`
SELECT id, anomaly_id, route_id, service_name, severity, anomaly_type, fired_at, acked_at, escalated
FROM %s.alert_events
WHERE service_name = ?
ORDER BY fired_at DESC
LIMIT %d`, s.db, limit)
		args = []any{service}
	} else {
		q = fmt.Sprintf(`
SELECT id, anomaly_id, route_id, service_name, severity, anomaly_type, fired_at, acked_at, escalated
FROM %s.alert_events
ORDER BY fired_at DESC
LIMIT %d`, s.db, limit)
	}
	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AlertEvent
	for rows.Next() {
		var e AlertEvent
		var escalated uint8
		if err := rows.Scan(
			&e.ID, &e.AnomalyID, &e.RouteID, &e.ServiceName, &e.Severity, &e.AnomalyType,
			&e.FiredAt, &e.AckedAt, &escalated,
		); err != nil {
			return nil, err
		}
		e.Escalated = escalated == 1
		out = append(out, e)
	}
	return out, rows.Err()
}
