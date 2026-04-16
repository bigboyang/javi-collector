// Package store: log_analytics.go — Log Analytics 저장소
//
// GAP-06: Log Analytics
//
// ClickHouse logs 테이블을 기반으로 5가지 분석 쿼리를 제공한다:
//   - QueryLogVolume   : 시간대별 로그 볼륨 (severity 분류) — MV 활용(60s) or raw table
//   - QueryLogSearch   : 본문 키워드 검색 (positionCaseInsensitive)
//   - QueryLogPatterns : 반복 로그 패턴 탐지 (top-N, 숫자열 정규화)
//   - QueryLogContext  : 특정 타임스탬프 전후 N초 컨텍스트 윈도우
//   - QueryLogFields   : severity 분포 · 상위 logger · 상위 exception 통계
//
// init() 시 mv_log_volume_1m_state / mv_log_volume_1m 을 DDL로 생성한다.
// 이미 존재하면 IF NOT EXISTS로 무시된다.
package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// LogAnalyticsStore는 logs 테이블에 대한 분석 쿼리를 제공한다.
type LogAnalyticsStore struct {
	conn driver.Conn
	db   string
}

// NewLogAnalyticsStore는 LogAnalyticsStore를 초기화하고 MV DDL을 적용한다.
func NewLogAnalyticsStore(conn driver.Conn, db string) (*LogAnalyticsStore, error) {
	s := &LogAnalyticsStore{conn: conn, db: db}
	if err := s.init(context.Background()); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *LogAnalyticsStore) init(ctx context.Context) error {
	// mv_log_volume_1m_state: (service_name, severity_text, minute) 별 로그 수 집계 테이블.
	// QueryLogVolume(intervalSec=60)의 빠른 경로로 활용된다.
	if err := s.conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_log_volume_1m_state (
    service_name  LowCardinality(String),
    severity_text LowCardinality(String),
    minute        DateTime,
    log_count     SimpleAggregateFunction(sum, UInt64),
    dt            Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, minute, severity_text)
TTL dt + INTERVAL 30 DAY;`, s.db)); err != nil {
		return fmt.Errorf("create mv_log_volume_1m_state: %w", err)
	}

	if err := s.conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_log_volume_1m
TO %s.mv_log_volume_1m_state AS
SELECT
    service_name,
    severity_text,
    toStartOfMinute(fromUnixTimestamp64Nano(timestamp_nano)) AS minute,
    toUInt64(count())                                         AS log_count,
    dt
FROM %s.logs
GROUP BY service_name, severity_text, minute, dt;`, s.db, s.db, s.db)); err != nil {
		return fmt.Errorf("create mv_log_volume_1m: %w", err)
	}

	return nil
}

// ---- 볼륨 집계 ----

// LogVolumePoint는 특정 시간 버킷의 severity별 로그 수다.
type LogVolumePoint struct {
	BucketMs     int64  `json:"bucket_ms"`
	SeverityText string `json:"severity_text"`
	Count        uint64 `json:"count"`
}

// QueryLogVolume은 시간대별 로그 볼륨을 severity별로 집계해 반환한다.
// intervalSec=60(기본)이면 mv_log_volume_1m_state MV를 활용한다.
// 다른 interval이면 raw logs 테이블에서 직접 집계한다.
func (s *LogAnalyticsStore) QueryLogVolume(ctx context.Context, service string, fromMs, toMs int64, intervalSec int) ([]LogVolumePoint, error) {
	if intervalSec <= 0 {
		intervalSec = 60
	}
	if intervalSec == 60 {
		return s.queryLogVolumeMV(ctx, service, fromMs, toMs)
	}
	return s.queryLogVolumeRaw(ctx, service, fromMs, toMs, intervalSec)
}

func (s *LogAnalyticsStore) queryLogVolumeMV(ctx context.Context, service string, fromMs, toMs int64) ([]LogVolumePoint, error) {
	conds, args := logBaseConds(service, fromMs, toMs, "minute")
	where := buildWhereClause(conds)

	sql := fmt.Sprintf(`
SELECT
    toInt64(toUnixTimestamp(minute)) * 1000 AS bucket_ms,
    severity_text,
    sum(log_count)                           AS cnt
FROM %s.mv_log_volume_1m_state
%s
GROUP BY bucket_ms, severity_text
ORDER BY bucket_ms ASC
LIMIT 2000`, s.db, where)

	return s.scanVolumeRows(ctx, sql, args)
}

func (s *LogAnalyticsStore) queryLogVolumeRaw(ctx context.Context, service string, fromMs, toMs int64, intervalSec int) ([]LogVolumePoint, error) {
	conds, args := logBaseConds(service, fromMs, toMs, "timestamp_nano")
	where := buildWhereClause(conds)

	sql := fmt.Sprintf(`
SELECT
    toUnixTimestamp64Milli(
        toStartOfInterval(fromUnixTimestamp64Nano(timestamp_nano), INTERVAL %d SECOND)
    ) AS bucket_ms,
    severity_text,
    count() AS cnt
FROM %s.logs
%s
GROUP BY bucket_ms, severity_text
ORDER BY bucket_ms ASC
LIMIT 2000`, intervalSec, s.db, where)

	return s.scanVolumeRows(ctx, sql, args)
}

func (s *LogAnalyticsStore) scanVolumeRows(ctx context.Context, sql string, args []any) ([]LogVolumePoint, error) {
	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LogVolumePoint
	for rows.Next() {
		var p LogVolumePoint
		if err := rows.Scan(&p.BucketMs, &p.SeverityText, &p.Count); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ---- 키워드 검색 ----

// LogSearchQuery는 로그 키워드 검색 파라미터다.
type LogSearchQuery struct {
	Service  string
	Severity string // "" = 전체
	Keyword  string // 본문 검색어; 비어 있으면 전체 반환
	FromMs   int64
	ToMs     int64
	Limit    int
	Offset   int
}

// LogSearchResult는 키워드 검색 결과 로그 항목이다.
type LogSearchResult struct {
	TimestampNano int64  `json:"timestamp_nano"`
	ServiceName   string `json:"service_name"`
	SeverityText  string `json:"severity_text"`
	Body          string `json:"body"`
	TraceID       string `json:"trace_id"`
	SpanID        string `json:"span_id"`
	ExceptionType string `json:"exception_type"`
	LoggerName    string `json:"logger_name"`
}

// QueryLogSearch는 키워드·서비스·심각도·시간 범위로 로그를 검색한다.
// 키워드 매칭은 positionCaseInsensitive(body, keyword)를 사용해 대소문자 무관 탐색한다.
// LIKE '%x%' 대비 열 스캔 최적화가 가능하며 별도 스키마 변경이 불필요하다.
func (s *LogAnalyticsStore) QueryLogSearch(ctx context.Context, q LogSearchQuery) ([]LogSearchResult, error) {
	if q.Limit <= 0 {
		q.Limit = 100
	}

	var conds []string
	var args []any

	if q.Service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, q.Service)
	}
	if q.Severity != "" {
		conds = append(conds, "severity_text = ?")
		args = append(args, strings.ToUpper(q.Severity))
	}
	if q.FromMs > 0 {
		conds = append(conds, "timestamp_nano >= ?")
		args = append(args, q.FromMs*1_000_000)
	}
	if q.ToMs > 0 {
		conds = append(conds, "timestamp_nano <= ?")
		args = append(args, q.ToMs*1_000_000)
	}
	if q.Keyword != "" {
		conds = append(conds, "positionCaseInsensitive(body, ?) > 0")
		args = append(args, q.Keyword)
	}

	where := buildWhereClause(conds)
	sql := fmt.Sprintf(`
SELECT timestamp_nano, service_name, severity_text, body, trace_id, span_id, exception_type, logger_name
FROM %s.logs
%s
ORDER BY timestamp_nano DESC
LIMIT %d OFFSET %d`, s.db, where, q.Limit, q.Offset)

	return s.scanSearchRows(ctx, sql, args)
}

func (s *LogAnalyticsStore) scanSearchRows(ctx context.Context, sql string, args []any) ([]LogSearchResult, error) {
	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LogSearchResult
	for rows.Next() {
		var r LogSearchResult
		if err := rows.Scan(
			&r.TimestampNano, &r.ServiceName, &r.SeverityText, &r.Body,
			&r.TraceID, &r.SpanID, &r.ExceptionType, &r.LoggerName,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---- 패턴 탐지 ----

// LogPattern은 반복 로그 패턴 그룹이다.
type LogPattern struct {
	Pattern       string `json:"pattern"`         // 정규화된 body 앞 120자
	SeverityText  string `json:"severity_text"`
	ServiceName   string `json:"service_name"`
	Count         uint64 `json:"count"`
	FirstSeenNano int64  `json:"first_seen_nano"`
	LastSeenNano  int64  `json:"last_seen_nano"`
}

// QueryLogPatterns는 반복 로그 패턴을 발생 빈도 내림차순으로 반환한다.
// body 앞 120자에서 4자리 이상 숫자열(타임스탬프·PID·요청ID)을 'N'으로 치환해 그룹화한다.
// 이렇게 하면 동일 템플릿의 로그가 서로 다른 패턴으로 분산되는 문제를 줄인다.
func (s *LogAnalyticsStore) QueryLogPatterns(ctx context.Context, service string, fromMs, toMs int64, limit int) ([]LogPattern, error) {
	if limit <= 0 {
		limit = 20
	}

	conds, args := logBaseConds(service, fromMs, toMs, "timestamp_nano")
	where := buildWhereClause(conds)

	sql := fmt.Sprintf(`
SELECT
    replaceRegexpAll(left(body, 120), '[0-9]{4,}', 'N') AS pattern,
    severity_text,
    service_name,
    count()             AS cnt,
    min(timestamp_nano) AS first_seen,
    max(timestamp_nano) AS last_seen
FROM %s.logs
%s
GROUP BY pattern, severity_text, service_name
ORDER BY cnt DESC
LIMIT %d`, s.db, where, limit)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LogPattern
	for rows.Next() {
		var p LogPattern
		if err := rows.Scan(
			&p.Pattern, &p.SeverityText, &p.ServiceName,
			&p.Count, &p.FirstSeenNano, &p.LastSeenNano,
		); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ---- 컨텍스트 윈도우 ----

// QueryLogContext는 특정 타임스탬프 전후 windowSec초 범위의 로그를 반환한다.
// 장애 발생 시점 전후 컨텍스트를 빠르게 파악하는 데 사용한다.
func (s *LogAnalyticsStore) QueryLogContext(ctx context.Context, service string, timestampNano int64, windowSec int, limit int) ([]LogSearchResult, error) {
	if windowSec <= 0 {
		windowSec = 30
	}
	if limit <= 0 {
		limit = 50
	}

	windowNano := int64(windowSec) * 1_000_000_000
	fromNano := timestampNano - windowNano
	toNano := timestampNano + windowNano

	var conds []string
	var args []any

	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}
	conds = append(conds, "timestamp_nano >= ?")
	args = append(args, fromNano)
	conds = append(conds, "timestamp_nano <= ?")
	args = append(args, toNano)

	where := buildWhereClause(conds)
	sql := fmt.Sprintf(`
SELECT timestamp_nano, service_name, severity_text, body, trace_id, span_id, exception_type, logger_name
FROM %s.logs
%s
ORDER BY timestamp_nano ASC
LIMIT %d`, s.db, where, limit)

	return s.scanSearchRows(ctx, sql, args)
}

// ---- 필드 통계 ----

// SeverityCount는 severity별 로그 수다.
type SeverityCount struct {
	SeverityText string `json:"severity_text"`
	Count        uint64 `json:"count"`
}

// NamedCount는 이름-카운트 쌍이다.
type NamedCount struct {
	Name  string `json:"name"`
	Count uint64 `json:"count"`
}

// LogFieldStats는 로그 필드 분포 통계다.
type LogFieldStats struct {
	SeverityDist  []SeverityCount `json:"severity_dist"`
	TopLoggers    []NamedCount    `json:"top_loggers"`
	TopExceptions []NamedCount    `json:"top_exceptions"`
	TotalCount    uint64          `json:"total_count"`
	Service       string          `json:"service,omitempty"`
	FromMs        int64           `json:"from_ms,omitempty"`
	ToMs          int64           `json:"to_ms,omitempty"`
}

// QueryLogFields는 서비스 로그의 필드 분포 통계를 반환한다.
// severity 분포, 상위 10개 logger, 상위 10개 exception type을 한 번에 반환한다.
func (s *LogAnalyticsStore) QueryLogFields(ctx context.Context, service string, fromMs, toMs int64) (*LogFieldStats, error) {
	conds, args := logBaseConds(service, fromMs, toMs, "timestamp_nano")
	where := buildWhereClause(conds)

	stats := &LogFieldStats{
		SeverityDist:  []SeverityCount{},
		TopLoggers:    []NamedCount{},
		TopExceptions: []NamedCount{},
		Service:       service,
		FromMs:        fromMs,
		ToMs:          toMs,
	}

	// 1) Severity 분포
	sevRows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT severity_text, count() AS cnt
FROM %s.logs
%s
GROUP BY severity_text
ORDER BY cnt DESC`, s.db, where), args...)
	if err != nil {
		return nil, fmt.Errorf("severity dist: %w", err)
	}
	defer sevRows.Close()
	for sevRows.Next() {
		var sc SeverityCount
		if err := sevRows.Scan(&sc.SeverityText, &sc.Count); err != nil {
			return nil, err
		}
		stats.SeverityDist = append(stats.SeverityDist, sc)
		stats.TotalCount += sc.Count
	}
	if err := sevRows.Err(); err != nil {
		return nil, err
	}

	// 2) 상위 logger_name (비어 있는 항목 제외)
	logConds := append(append([]string(nil), conds...), "logger_name != ''")
	logWhere := buildWhereClause(logConds)
	logRows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT logger_name, count() AS cnt
FROM %s.logs
%s
GROUP BY logger_name
ORDER BY cnt DESC
LIMIT 10`, s.db, logWhere), args...)
	if err != nil {
		return nil, fmt.Errorf("top loggers: %w", err)
	}
	defer logRows.Close()
	for logRows.Next() {
		var nc NamedCount
		if err := logRows.Scan(&nc.Name, &nc.Count); err != nil {
			return nil, err
		}
		stats.TopLoggers = append(stats.TopLoggers, nc)
	}
	if err := logRows.Err(); err != nil {
		return nil, err
	}

	// 3) 상위 exception_type (비어 있는 항목 제외)
	excConds := append(append([]string(nil), conds...), "exception_type != ''")
	excWhere := buildWhereClause(excConds)
	excRows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT exception_type, count() AS cnt
FROM %s.logs
%s
GROUP BY exception_type
ORDER BY cnt DESC
LIMIT 10`, s.db, excWhere), args...)
	if err != nil {
		return nil, fmt.Errorf("top exceptions: %w", err)
	}
	defer excRows.Close()
	for excRows.Next() {
		var nc NamedCount
		if err := excRows.Scan(&nc.Name, &nc.Count); err != nil {
			return nil, err
		}
		stats.TopExceptions = append(stats.TopExceptions, nc)
	}
	if err := excRows.Err(); err != nil {
		return nil, err
	}

	return stats, nil
}

// ---- 내부 헬퍼 ----

// logBaseConds는 logs 테이블의 공통 조건 (service, 시간 범위)을 만든다.
// timeCol은 조건을 적용할 시간 컬럼명이다: "timestamp_nano" 또는 "minute".
func logBaseConds(service string, fromMs, toMs int64, timeCol string) ([]string, []any) {
	var conds []string
	var args []any

	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}

	switch timeCol {
	case "minute":
		// DateTime 컬럼 — Unix 초 단위로 비교
		if fromMs > 0 {
			conds = append(conds, "minute >= toDateTime(?)")
			args = append(args, fromMs/1000)
		}
		if toMs > 0 {
			conds = append(conds, "minute <= toDateTime(?)")
			args = append(args, toMs/1000)
		}
	default:
		// Int64 나노초 컬럼
		if fromMs > 0 {
			conds = append(conds, "timestamp_nano >= ?")
			args = append(args, fromMs*1_000_000)
		}
		if toMs > 0 {
			conds = append(conds, "timestamp_nano <= ?")
			args = append(args, toMs*1_000_000)
		}
	}

	return conds, args
}

// buildWhereClause는 조건 슬라이스를 WHERE 절 문자열로 변환한다.
// 빈 슬라이스이면 빈 문자열을 반환한다.
func buildWhereClause(conds []string) string {
	if len(conds) == 0 {
		return ""
	}
	return "WHERE " + strings.Join(conds, " AND ")
}

