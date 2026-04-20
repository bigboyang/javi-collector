// Package store - migration.go
//
// Migrator는 ClickHouse 스키마 마이그레이션을 버전 기반으로 관리한다.
//
// 동작 방식:
//  1. schema_migrations 테이블이 없으면 자동 생성한다.
//  2. 이미 적용된 마이그레이션(version)은 건너뛴다.
//  3. 미적용 마이그레이션을 version 순으로 실행한다.
//  4. 실행 성공 시 schema_migrations에 기록한다.
//
// 기존 ensureXxxTable 함수는 "version 0" (CREATE TABLE IF NOT EXISTS)에 해당하므로
// Migrator는 그 이후의 변경 사항을 관리한다.
//
// 마이그레이션 등록 예시:
//
//	var SpansMigrations = []store.Migration{
//	    {Version: 1, Description: "add trace_state column", SQL: "ALTER TABLE ..."},
//	}
//
//	migrator := store.NewMigrator(conn, "apm")
//	if err := migrator.Run(ctx, store.SpansMigrations); err != nil {
//	    log.Fatal(err)
//	}
package store

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Migration은 단일 스키마 마이그레이션을 나타낸다.
type Migration struct {
	// Version은 단조 증가하는 양의 정수다. 중복 불가.
	Version int
	// Description은 마이그레이션 목적을 설명하는 짧은 문자열이다.
	Description string
	// SQL은 실행할 DDL 문장이다. 단일 문장만 지원한다.
	SQL string
}

// Migrator는 schema_migrations 테이블을 통해 마이그레이션 상태를 추적한다.
type Migrator struct {
	conn driver.Conn
	db   string
}

// NewMigrator는 Migrator를 생성한다.
func NewMigrator(conn driver.Conn, db string) *Migrator {
	return &Migrator{conn: conn, db: db}
}

// Run은 migrations 슬라이스에서 미적용 마이그레이션을 순서대로 실행한다.
// 이미 적용된 버전은 건너뛴다. 실패하면 즉시 에러를 반환한다.
func (m *Migrator) Run(ctx context.Context, migrations []Migration) error {
	if err := m.ensureMigrationsTable(ctx); err != nil {
		return fmt.Errorf("migration: ensure table: %w", err)
	}

	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return fmt.Errorf("migration: list applied: %w", err)
	}

	for _, mg := range migrations {
		if applied[mg.Version] {
			slog.Debug("migration already applied, skipping",
				"version", mg.Version,
				"description", mg.Description,
			)
			continue
		}

		slog.Info("applying migration",
			"version", mg.Version,
			"description", mg.Description,
		)

		if err := m.conn.Exec(ctx, mg.SQL); err != nil {
			return fmt.Errorf("migration v%d (%s): %w", mg.Version, mg.Description, err)
		}

		if err := m.recordApplied(ctx, mg); err != nil {
			// SQL은 실행됐지만 기록 실패 — 경고 후 계속 (다음 시작 시 재실행 가능성 있으나 SQL이 idempotent여야 함)
			slog.Warn("migration applied but record failed",
				"version", mg.Version,
				"err", err,
			)
		}

		slog.Info("migration applied",
			"version", mg.Version,
			"description", mg.Description,
		)
	}
	return nil
}

// AppliedVersions는 현재 적용된 마이그레이션 버전 목록을 반환한다.
func (m *Migrator) AppliedVersions(ctx context.Context) ([]int, error) {
	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return nil, err
	}
	versions := make([]int, 0, len(applied))
	for v := range applied {
		versions = append(versions, v)
	}
	return versions, nil
}

// ensureMigrationsTable은 schema_migrations 추적 테이블을 생성한다.
func (m *Migrator) ensureMigrationsTable(ctx context.Context) error {
	return m.conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.schema_migrations (
    version      Int32,
    description  String,
    applied_at   DateTime DEFAULT now(),
    dt           Date DEFAULT today()
) ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version;
`, m.db))
}

// appliedVersions는 schema_migrations에서 적용된 버전 set을 반환한다.
func (m *Migrator) appliedVersions(ctx context.Context) (map[int]bool, error) {
	rows, err := m.conn.Query(ctx,
		fmt.Sprintf(`SELECT version FROM %s.schema_migrations FINAL`, m.db))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int32
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[int(v)] = true
	}
	return applied, rows.Err()
}

// recordApplied는 마이그레이션 적용 기록을 schema_migrations에 삽입한다.
func (m *Migrator) recordApplied(ctx context.Context, mg Migration) error {
	batch, err := m.conn.PrepareBatch(ctx,
		fmt.Sprintf(`INSERT INTO %s.schema_migrations (version, description, applied_at) VALUES`, m.db))
	if err != nil {
		return err
	}
	if err := batch.Append(int32(mg.Version), mg.Description, time.Now()); err != nil {
		return err
	}
	return batch.Send()
}

// ---- 등록된 마이그레이션 ----

// SpansMigrations는 spans 테이블에 대한 버전 관리 마이그레이션 목록이다.
// 새 컬럼/인덱스 추가 시 이 슬라이스에 항목을 추가한다.
// SQL은 반드시 멱등적(idempotent)이어야 한다 (IF NOT EXISTS / IF EXISTS 사용).
//
// 버전 범위:
//   - 1–5:  spans 테이블 컬럼 추가 (%SPANS_TABLE% 플레이스홀더)
//   - 6–99: RED 롤업 테이블 추가 (%DB% 플레이스홀더)
var SpansMigrations = []Migration{
	{
		Version:     1,
		Description: "add trace_state column for W3C Trace Context propagation",
		SQL:         `ALTER TABLE %SPANS_TABLE% ADD COLUMN IF NOT EXISTS trace_state String DEFAULT ''`,
	},
	{
		Version:     2,
		Description: "add span_links column for cross-trace link metadata",
		SQL:         `ALTER TABLE %SPANS_TABLE% ADD COLUMN IF NOT EXISTS span_links String DEFAULT ''`,
	},
	{
		Version:     3,
		Description: "add duration_ms materialized column for millisecond-granularity latency queries",
		SQL:         `ALTER TABLE %SPANS_TABLE% ADD COLUMN IF NOT EXISTS duration_ms Int64 MATERIALIZED toInt64((end_time_nano - start_time_nano) / 1000000)`,
	},
	{
		Version:     4,
		Description: "add is_error materialized column for fast error-rate aggregation",
		SQL:         `ALTER TABLE %SPANS_TABLE% ADD COLUMN IF NOT EXISTS is_error UInt8 MATERIALIZED if(status_code = 2, 1, 0)`,
	},
	{
		Version:     5,
		Description: "add span_kind_str materialized column for human-readable span kind filtering",
		SQL:         `ALTER TABLE %SPANS_TABLE% ADD COLUMN IF NOT EXISTS span_kind_str LowCardinality(String) MATERIALIZED multiIf(kind=1,'INTERNAL',kind=2,'SERVER',kind=3,'CLIENT',kind=4,'PRODUCER',kind=5,'CONSUMER','UNSPECIFIED')`,
	},
	// M-7: 데이터 티어링 — 5분/1시간 RED 롤업 테이블
	{
		Version:     6,
		Description: "create mv_red_5m_state for 5-minute RED rollup (180-day retention)",
		SQL: `CREATE TABLE IF NOT EXISTS %DB%.mv_red_5m_state (
    service_name       LowCardinality(String),
    span_name          LowCardinality(String),
    http_route         LowCardinality(String),
    minute5            DateTime,
    total_count        SimpleAggregateFunction(sum, UInt64),
    error_count        SimpleAggregateFunction(sum, UInt64),
    duration_quantiles AggregateFunction(quantiles(0.5, 0.95, 0.99), Float64),
    duration_sum       SimpleAggregateFunction(sum, Float64),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, minute5, span_name)
TTL dt + INTERVAL 180 DAY`,
	},
	{
		Version:     7,
		Description: "create mv_red_5m materialized view aggregating spans into 5-minute buckets",
		SQL: `CREATE MATERIALIZED VIEW IF NOT EXISTS %DB%.mv_red_5m
TO %DB%.mv_red_5m_state
AS SELECT
    service_name,
    name                                                             AS span_name,
    http_route,
    toStartOfFiveMinute(fromUnixTimestamp64Nano(start_time_nano))  AS minute5,
    toUInt64(count())                                               AS total_count,
    toUInt64(countIf(status_code = 2))                             AS error_count,
    quantilesState(0.5, 0.95, 0.99)(toFloat64(duration_nano))     AS duration_quantiles,
    toFloat64(sum(duration_nano))                                   AS duration_sum,
    dt
FROM %DB%.spans
WHERE kind IN (2, 5)
GROUP BY service_name, span_name, http_route, minute5, dt`,
	},
	{
		Version:     8,
		Description: "create mv_red_1h_state for 1-hour RED rollup (365-day retention)",
		SQL: `CREATE TABLE IF NOT EXISTS %DB%.mv_red_1h_state (
    service_name       LowCardinality(String),
    span_name          LowCardinality(String),
    http_route         LowCardinality(String),
    hour               DateTime,
    total_count        SimpleAggregateFunction(sum, UInt64),
    error_count        SimpleAggregateFunction(sum, UInt64),
    duration_quantiles AggregateFunction(quantiles(0.5, 0.95, 0.99), Float64),
    duration_sum       SimpleAggregateFunction(sum, Float64),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, hour, span_name)
TTL dt + INTERVAL 365 DAY`,
	},
	{
		Version:     9,
		Description: "create mv_red_1h materialized view aggregating spans into 1-hour buckets",
		SQL: `CREATE MATERIALIZED VIEW IF NOT EXISTS %DB%.mv_red_1h
TO %DB%.mv_red_1h_state
AS SELECT
    service_name,
    name                                                         AS span_name,
    http_route,
    toStartOfHour(fromUnixTimestamp64Nano(start_time_nano))     AS hour,
    toUInt64(count())                                            AS total_count,
    toUInt64(countIf(status_code = 2))                          AS error_count,
    quantilesState(0.5, 0.95, 0.99)(toFloat64(duration_nano))  AS duration_quantiles,
    toFloat64(sum(duration_nano))                               AS duration_sum,
    dt
FROM %DB%.spans
WHERE kind IN (2, 5)
GROUP BY service_name, span_name, http_route, hour, dt`,
	},
}

// MetricsMigrations는 metrics 테이블에 대한 버전 관리 마이그레이션 목록이다.
//
// 버전 범위:
//   - 100–109: mv_histogram_1m 버킷 집계 재구성 (M-2)
//   - 110–199: 히스토그램 5분/1시간 롤업 테이블 추가 (M-7)
var MetricsMigrations = []Migration{
	{
		Version:     100,
		Description: "drop old mv_histogram_1m to prepare for bucket-aware rebuild",
		SQL:         `DROP TABLE IF EXISTS %DB%.mv_histogram_1m`,
	},
	{
		Version:     101,
		Description: "add bucket_counts_state column to mv_histogram_1m_state for percentile support",
		SQL:         `ALTER TABLE %DB%.mv_histogram_1m_state ADD COLUMN IF NOT EXISTS bucket_counts_state AggregateFunction(sumForEach, Array(UInt64))`,
	},
	{
		Version:     102,
		Description: "add bounds_state column to mv_histogram_1m_state for bucket boundary storage",
		SQL:         `ALTER TABLE %DB%.mv_histogram_1m_state ADD COLUMN IF NOT EXISTS bounds_state AggregateFunction(any, Array(Float64))`,
	},
	{
		Version:     103,
		Description: "recreate mv_histogram_1m with bucket_counts and bounds aggregation for accurate P50/P95/P99",
		SQL: `CREATE MATERIALIZED VIEW IF NOT EXISTS %DB%.mv_histogram_1m
TO %DB%.mv_histogram_1m_state
AS SELECT
    service_name,
    metric_name,
    toStartOfMinute(fromUnixTimestamp64Nano(timestamp_nano)) AS minute,
    sumState(total_count)          AS count_state,
    sumState(total_sum)            AS sum_state,
    sumForEachState(bucket_counts) AS bucket_counts_state,
    anyState(bounds)               AS bounds_state,
    dt
FROM %DB%.metric_histograms
GROUP BY service_name, metric_name, minute, dt`,
	},
	// M-7: 데이터 티어링 — 5분/1시간 히스토그램 롤업 테이블
	{
		Version:     110,
		Description: "create mv_histogram_5m_state for 5-minute histogram rollup (180-day retention)",
		SQL: `CREATE TABLE IF NOT EXISTS %DB%.mv_histogram_5m_state (
    service_name         LowCardinality(String),
    metric_name          LowCardinality(String),
    minute5              DateTime,
    count_state          AggregateFunction(sum, UInt64),
    sum_state            AggregateFunction(sum, Float64),
    bucket_counts_state  AggregateFunction(sumForEach, Array(UInt64)),
    bounds_state         AggregateFunction(any, Array(Float64)),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, metric_name, minute5)
TTL dt + INTERVAL 180 DAY`,
	},
	{
		Version:     111,
		Description: "create mv_histogram_5m materialized view aggregating metric_histograms into 5-minute buckets",
		SQL: `CREATE MATERIALIZED VIEW IF NOT EXISTS %DB%.mv_histogram_5m
TO %DB%.mv_histogram_5m_state
AS SELECT
    service_name,
    metric_name,
    toStartOfFiveMinute(fromUnixTimestamp64Nano(timestamp_nano)) AS minute5,
    sumState(total_count)          AS count_state,
    sumState(total_sum)            AS sum_state,
    sumForEachState(bucket_counts) AS bucket_counts_state,
    anyState(bounds)               AS bounds_state,
    dt
FROM %DB%.metric_histograms
GROUP BY service_name, metric_name, minute5, dt`,
	},
	{
		Version:     112,
		Description: "create mv_histogram_1h_state for 1-hour histogram rollup (365-day retention)",
		SQL: `CREATE TABLE IF NOT EXISTS %DB%.mv_histogram_1h_state (
    service_name         LowCardinality(String),
    metric_name          LowCardinality(String),
    hour                 DateTime,
    count_state          AggregateFunction(sum, UInt64),
    sum_state            AggregateFunction(sum, Float64),
    bucket_counts_state  AggregateFunction(sumForEach, Array(UInt64)),
    bounds_state         AggregateFunction(any, Array(Float64)),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, metric_name, hour)
TTL dt + INTERVAL 365 DAY`,
	},
	{
		Version:     113,
		Description: "create mv_histogram_1h materialized view aggregating metric_histograms into 1-hour buckets",
		SQL: `CREATE MATERIALIZED VIEW IF NOT EXISTS %DB%.mv_histogram_1h
TO %DB%.mv_histogram_1h_state
AS SELECT
    service_name,
    metric_name,
    toStartOfHour(fromUnixTimestamp64Nano(timestamp_nano)) AS hour,
    sumState(total_count)          AS count_state,
    sumState(total_sum)            AS sum_state,
    sumForEachState(bucket_counts) AS bucket_counts_state,
    anyState(bounds)               AS bounds_state,
    dt
FROM %DB%.metric_histograms
GROUP BY service_name, metric_name, hour, dt`,
	},
}

// LogsMigrations는 logs 테이블에 대한 버전 관리 마이그레이션 목록이다.
//
// 버전 범위: 200–299 (SpansMigrations 1–99, MetricsMigrations 100–199와 충돌 방지)
var LogsMigrations = []Migration{
	// M-7: 데이터 티어링 — 1시간 에러 로그 롤업 테이블
	{
		Version:     200,
		Description: "create mv_error_logs_1h_state for 1-hour error log rollup (365-day retention)",
		SQL: `CREATE TABLE IF NOT EXISTS %DB%.mv_error_logs_1h_state (
    service_name   LowCardinality(String),
    exception_type LowCardinality(String),
    hour           DateTime,
    error_count    SimpleAggregateFunction(sum, UInt64),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, hour, exception_type)
TTL dt + INTERVAL 365 DAY`,
	},
	{
		Version:     201,
		Description: "create mv_error_logs_1h materialized view aggregating error logs into 1-hour buckets",
		SQL: `CREATE MATERIALIZED VIEW IF NOT EXISTS %DB%.mv_error_logs_1h
TO %DB%.mv_error_logs_1h_state
AS SELECT
    service_name,
    exception_type,
    toStartOfHour(fromUnixTimestamp64Nano(timestamp_nano)) AS hour,
    toUInt64(count()) AS error_count,
    dt
FROM %DB%.logs
WHERE severity_number >= 17
GROUP BY service_name, exception_type, hour, dt`,
	},
}

// BuildSpansMigrations는 %SPANS_TABLE%을 db.spans으로, %DB%를 db로 치환한 SpansMigrations를 반환한다.
// v1–5는 %SPANS_TABLE% 플레이스홀더를 사용하고, v6+는 %DB% 플레이스홀더를 사용한다.
func BuildSpansMigrations(db string) []Migration {
	spansTable := db + ".spans"
	result := make([]Migration, len(SpansMigrations))
	for i, m := range SpansMigrations {
		sql := strings.ReplaceAll(m.SQL, "%SPANS_TABLE%", spansTable)
		sql = strings.ReplaceAll(sql, "%DB%", db)
		result[i] = Migration{
			Version:     m.Version,
			Description: m.Description,
			SQL:         sql,
		}
	}
	return result
}

// BuildMetricsMigrations는 %DB% 플레이스홀더를 db로 치환한 MetricsMigrations를 반환한다.
func BuildMetricsMigrations(db string) []Migration {
	result := make([]Migration, len(MetricsMigrations))
	for i, m := range MetricsMigrations {
		result[i] = Migration{
			Version:     m.Version,
			Description: m.Description,
			SQL:         strings.ReplaceAll(m.SQL, "%DB%", db),
		}
	}
	return result
}

// BuildLogsMigrations는 %DB% 플레이스홀더를 db로 치환한 LogsMigrations를 반환한다.
func BuildLogsMigrations(db string) []Migration {
	result := make([]Migration, len(LogsMigrations))
	for i, m := range LogsMigrations {
		result[i] = Migration{
			Version:     m.Version,
			Description: m.Description,
			SQL:         strings.ReplaceAll(m.SQL, "%DB%", db),
		}
	}
	return result
}

// buildMigrations는 마이그레이션 SQL에서 %SPANS_TABLE% 플레이스홀더를 table로 치환한다.
// Deprecated: BuildSpansMigrations를 사용하세요 (%DB% 치환도 지원).
func buildMigrations(migrations []Migration, table string) []Migration {
	result := make([]Migration, len(migrations))
	for i, m := range migrations {
		result[i] = Migration{
			Version:     m.Version,
			Description: m.Description,
			SQL:         strings.ReplaceAll(m.SQL, "%SPANS_TABLE%", table),
		}
	}
	return result
}
