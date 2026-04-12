// Package store - error_groups.go
//
// ErrorGroupStore는 에러 그룹 집계를 제공한다.
//
// 동일한 exception_type + exception_message(최대 200자)를 가진 error span을
// fingerprint(cityHash64)로 그룹화해 중복 알람을 방지하고
// 에러의 first_seen / last_seen / total_count를 추적한다.
//
// 상용 APM의 "Error Tracking" 기능 (Datadog Error Tracking, Sentry)에 해당하며
// alert fatigue를 줄이는 핵심 컴포넌트다.
//
// 구현: ClickHouse Materialized View (mv_error_groups) →
//
//	AggregatingMergeTree(mv_error_groups_state)
package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ErrorGroupStore는 error_groups MV 기반 집계 조회를 담당한다.
type ErrorGroupStore struct {
	conn driver.Conn
	db   string
}

// NewErrorGroupStore는 ErrorGroupStore를 생성하고 materialized view DDL을 적용한다.
func NewErrorGroupStore(conn driver.Conn, db string) (*ErrorGroupStore, error) {
	s := &ErrorGroupStore{conn: conn, db: db}
	if err := s.ensureTables(); err != nil {
		return nil, fmt.Errorf("error_groups DDL: %w", err)
	}
	return s, nil
}

func (s *ErrorGroupStore) ensureTables() error {
	ctx := context.Background()
	// AggregatingMergeTree state 테이블: MV의 집계 결과를 저장한다.
	if err := s.conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_error_groups_state (
    fingerprint       UInt64,
    service_name      LowCardinality(String),
    exception_type    String,
    exception_message String,
    total_count       SimpleAggregateFunction(sum, UInt64),
    first_seen        SimpleAggregateFunction(min, DateTime),
    last_seen         SimpleAggregateFunction(max, DateTime),
    dt                Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, fingerprint)
TTL dt + INTERVAL 90 DAY;
`, s.db)); err != nil {
		return fmt.Errorf("create mv_error_groups_state: %w", err)
	}
	// Materialized View: status_code=2이고 exception_type이 있는 span을 자동 집계한다.
	if err := s.conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_error_groups
TO %s.mv_error_groups_state
AS SELECT
    cityHash64(service_name, exception_type, left(exception_message, 200)) AS fingerprint,
    service_name,
    exception_type,
    left(exception_message, 200)                                            AS exception_message,
    toUInt64(1)                                                             AS total_count,
    toDateTime(intDiv(start_time_nano, 1000000000))                         AS first_seen,
    toDateTime(intDiv(start_time_nano, 1000000000))                         AS last_seen,
    dt
FROM %s.spans
WHERE status_code = 2 AND exception_type != '';
`, s.db, s.db, s.db)); err != nil {
		return fmt.Errorf("create mv_error_groups: %w", err)
	}
	return nil
}

// QueryErrorGroups는 집계된 에러 그룹을 반환한다.
// 동일 fingerprint의 에러를 하나의 그룹으로 묶어 total_count, first_seen, last_seen을 제공한다.
func (s *ErrorGroupStore) QueryErrorGroups(ctx context.Context, service string, fromMs, toMs int64, limit int) ([]map[string]any, error) {
	var conds []string
	var args []any

	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}
	if fromMs > 0 {
		conds = append(conds, "last_seen >= ?")
		args = append(args, time.UnixMilli(fromMs))
	}
	if toMs > 0 {
		conds = append(conds, "first_seen <= ?")
		args = append(args, time.UnixMilli(toMs))
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	if limit <= 0 {
		limit = 100
	}

	q := fmt.Sprintf(`
SELECT
    fingerprint,
    service_name,
    exception_type,
    exception_message,
    sum(total_count)  AS total_count,
    min(first_seen)   AS first_seen,
    max(last_seen)    AS last_seen
FROM %s.mv_error_groups_state
%s
GROUP BY fingerprint, service_name, exception_type, exception_message
ORDER BY total_count DESC
LIMIT %d`, s.db, where, limit)

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			fingerprint          uint64
			serviceName, excType string
			excMessage           string
			totalCount           uint64
			firstSeen, lastSeen  time.Time
		)
		if err := rows.Scan(&fingerprint, &serviceName, &excType, &excMessage,
			&totalCount, &firstSeen, &lastSeen); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"fingerprint":       fingerprint,
			"service_name":      serviceName,
			"exception_type":    excType,
			"exception_message": excMessage,
			"total_count":       totalCount,
			"first_seen_ms":     firstSeen.UnixMilli(),
			"last_seen_ms":      lastSeen.UnixMilli(),
		})
	}
	return result, rows.Err()
}
