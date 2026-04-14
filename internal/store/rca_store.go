// Package store - rca_store.go
//
// RCAStore는 rca_reports 테이블 조회와 피드백 업데이트를 제공한다.
//
// 상용 APM의 "Incident Insights / RCA Dashboard" 기능에 해당한다.
// (Datadog Watchdog, New Relic Applied Intelligence)
package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// RCAReport는 rca_reports 테이블의 단일 레코드다.
type RCAReport struct {
	ID               string    `json:"id"`
	AnomalyID        string    `json:"anomaly_id"`
	ServiceName      string    `json:"service_name"`
	SpanName         string    `json:"span_name"`
	AnomalyType      string    `json:"anomaly_type"`
	Minute           time.Time `json:"minute"`
	Severity         string    `json:"severity"`
	ZScore           float64   `json:"z_score"`
	CorrelatedSpans  string    `json:"correlated_spans"`  // JSON array
	SimilarIncidents string    `json:"similar_incidents"` // JSON array
	Hypothesis       string    `json:"hypothesis"`
	LLMAnalysis      string    `json:"llm_analysis"`
	Resolved         uint8     `json:"resolved"`
	Feedback         string    `json:"feedback"`
	CreatedAt        time.Time `json:"created_at"`
}

// RCAStore는 rca_reports 테이블 조회/피드백 업데이트를 담당한다.
type RCAStore struct {
	conn driver.Conn
	db   string
}

// NewRCAStore는 RCAStore를 생성한다.
func NewRCAStore(conn driver.Conn, db string) *RCAStore {
	return &RCAStore{conn: conn, db: db}
}

// QueryRCAReports는 rca_reports를 필터 조건에 맞게 반환한다.
func (s *RCAStore) QueryRCAReports(ctx context.Context, service, severity string, fromMs, toMs int64, limit int) ([]RCAReport, error) {
	if limit <= 0 {
		limit = 100
	}
	var conds []string
	var args []any

	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}
	if severity != "" {
		conds = append(conds, "severity = ?")
		args = append(args, severity)
	}
	if fromMs > 0 {
		conds = append(conds, "created_at >= ?")
		args = append(args, time.UnixMilli(fromMs))
	}
	if toMs > 0 {
		conds = append(conds, "created_at <= ?")
		args = append(args, time.UnixMilli(toMs))
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT id, anomaly_id, service_name, span_name, anomaly_type, minute,
       severity, z_score, correlated_spans, similar_incidents,
       hypothesis, llm_analysis, resolved, feedback, created_at
FROM %s.rca_reports
%s
ORDER BY created_at DESC
LIMIT %d`, s.db, where, limit), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []RCAReport
	for rows.Next() {
		var r RCAReport
		if err := rows.Scan(
			&r.ID, &r.AnomalyID, &r.ServiceName, &r.SpanName, &r.AnomalyType, &r.Minute,
			&r.Severity, &r.ZScore, &r.CorrelatedSpans, &r.SimilarIncidents,
			&r.Hypothesis, &r.LLMAnalysis, &r.Resolved, &r.Feedback, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// UpdateRCAFeedback는 특정 rca_report의 resolved 상태와 feedback 텍스트를 업데이트한다.
// ClickHouse ALTER TABLE ... UPDATE mutation을 사용한다.
func (s *RCAStore) UpdateRCAFeedback(ctx context.Context, id string, resolved uint8, feedback string) error {
	return s.conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.rca_reports UPDATE resolved = ?, feedback = ? WHERE id = ?`,
		s.db,
	), resolved, feedback, id)
}
