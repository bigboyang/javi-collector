package store

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func ensureSpansTable(conn driver.Conn, db string, retentionDays int) error {
	ctx := context.Background()

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.spans (
    trace_id         String,
    span_id          String,
    parent_span_id   String,
    name             String,
    kind             Int32,
    start_time_nano  Int64,
    end_time_nano    Int64,
    duration_nano    Int64,
    attributes       Map(String, String),
    status_code      Int32,
    status_message   String,
    service_name     LowCardinality(String),
    scope_name       LowCardinality(String),
    received_at_ms   Int64,
    http_method      LowCardinality(String) DEFAULT '',
    http_route       LowCardinality(String) DEFAULT '',
    http_status_code UInt16                 DEFAULT 0,
    db_system        LowCardinality(String) DEFAULT '',
    db_name          LowCardinality(String) DEFAULT '',
    db_operation     LowCardinality(String) DEFAULT '',
    rpc_system       LowCardinality(String) DEFAULT '',
    rpc_service      LowCardinality(String) DEFAULT '',
    rpc_method       LowCardinality(String) DEFAULT '',
    peer_service     LowCardinality(String) DEFAULT '',
    exception_type        String DEFAULT '',
    exception_message     String DEFAULT '',
    exception_stacktrace  String DEFAULT '',
    trace_state           String DEFAULT '',
    span_links            String DEFAULT '',
    dt Date DEFAULT toDate(fromUnixTimestamp64Milli(received_at_ms))
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, trace_id, start_time_nano)
TTL dt + INTERVAL %d DAY;
`, db, retentionDays)); err != nil {
		return err
	}

	alterCols := []string{
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS http_method LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS http_route LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS http_status_code UInt16 DEFAULT 0", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS db_system LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS db_name LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS db_operation LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS rpc_system LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS rpc_service LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS rpc_method LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS peer_service LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS exception_type String DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS exception_message String DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS exception_stacktrace String DEFAULT ''", db),
		// #6: attributes String → Map(String,String) 마이그레이션 (기존 테이블 대상)
		fmt.Sprintf("ALTER TABLE %s.spans MODIFY COLUMN IF EXISTS attributes Map(String, String)", db),
		// #7: W3C Trace State 및 Span Links 컬럼 추가
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS trace_state String DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.spans ADD COLUMN IF NOT EXISTS span_links String DEFAULT ''", db),
	}
	for _, q := range alterCols {
		if err := conn.Exec(ctx, q); err != nil {
			slog.Warn("alter spans column skipped", "query", q, "err", err)
		}
	}

	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.spans MODIFY TTL dt + INTERVAL %d DAY`, db, retentionDays)); err != nil {
		return err
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_red_1m_state (
    service_name       LowCardinality(String),
    span_name          LowCardinality(String),
    http_route         LowCardinality(String),
    minute             DateTime,
    total_count        SimpleAggregateFunction(sum, UInt64),
    error_count        SimpleAggregateFunction(sum, UInt64),
    duration_quantiles AggregateFunction(quantiles(0.5, 0.95, 0.99), Float64),
    duration_sum       SimpleAggregateFunction(sum, Float64),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, minute, span_name)
TTL dt + INTERVAL 90 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_red_1m_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_red_1m
TO %s.mv_red_1m_state
AS SELECT
    service_name,
    name                                                              AS span_name,
    http_route,
    toStartOfMinute(fromUnixTimestamp64Nano(start_time_nano))        AS minute,
    toUInt64(count())                                                 AS total_count,
    toUInt64(countIf(status_code = 2))                               AS error_count,
    quantilesState(0.5, 0.95, 0.99)(toFloat64(duration_nano))       AS duration_quantiles,
    toFloat64(sum(duration_nano))                                     AS duration_sum,
    dt
FROM %s.spans
WHERE kind IN (2, 5)
GROUP BY service_name, span_name, http_route, minute, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_red_1m: %w", err)
	}

	// M-7: 데이터 티어링 — 5분 RED 롤업 (180일 보관)
	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_red_5m_state (
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
TTL dt + INTERVAL 180 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_red_5m_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_red_5m
TO %s.mv_red_5m_state
AS SELECT
    service_name,
    name                                                            AS span_name,
    http_route,
    toStartOfFiveMinute(fromUnixTimestamp64Nano(start_time_nano)) AS minute5,
    toUInt64(count())                                              AS total_count,
    toUInt64(countIf(status_code = 2))                            AS error_count,
    quantilesState(0.5, 0.95, 0.99)(toFloat64(duration_nano))    AS duration_quantiles,
    toFloat64(sum(duration_nano))                                  AS duration_sum,
    dt
FROM %s.spans
WHERE kind IN (2, 5)
GROUP BY service_name, span_name, http_route, minute5, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_red_5m: %w", err)
	}

	// M-7: 데이터 티어링 — 1시간 RED 롤업 (365일 보관)
	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_red_1h_state (
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
TTL dt + INTERVAL 365 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_red_1h_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_red_1h
TO %s.mv_red_1h_state
AS SELECT
    service_name,
    name                                                        AS span_name,
    http_route,
    toStartOfHour(fromUnixTimestamp64Nano(start_time_nano))    AS hour,
    toUInt64(count())                                          AS total_count,
    toUInt64(countIf(status_code = 2))                        AS error_count,
    quantilesState(0.5, 0.95, 0.99)(toFloat64(duration_nano)) AS duration_quantiles,
    toFloat64(sum(duration_nano))                              AS duration_sum,
    dt
FROM %s.spans
WHERE kind IN (2, 5)
GROUP BY service_name, span_name, http_route, hour, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_red_1h: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_service_topology_state (
    caller_service LowCardinality(String),
    callee_service LowCardinality(String),
    minute5        DateTime,
    call_count     SimpleAggregateFunction(sum, UInt64),
    error_count    SimpleAggregateFunction(sum, UInt64),
    duration_sum   SimpleAggregateFunction(sum, Float64),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (caller_service, callee_service, minute5)
TTL dt + INTERVAL 30 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_service_topology_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_service_topology
TO %s.mv_service_topology_state
AS SELECT
    service_name                                                       AS caller_service,
    peer_service                                                       AS callee_service,
    toStartOfFiveMinute(fromUnixTimestamp64Nano(start_time_nano))     AS minute5,
    toUInt64(count())                                                  AS call_count,
    toUInt64(countIf(status_code = 2))                                AS error_count,
    toFloat64(sum(duration_nano))                                      AS duration_sum,
    dt
FROM %s.spans
WHERE kind = 3 AND peer_service != ''
GROUP BY caller_service, callee_service, minute5, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_service_topology: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.rag_error_context (
    doc_id         String,
    source_type    LowCardinality(String),
    source_id      String,
    service_name   LowCardinality(String),
    trace_id       String,
    span_id        String,
    occurred_at    DateTime64(3),
    content_text   String,
    embedded_at    Nullable(DateTime),
    embedding_model LowCardinality(String) DEFAULT '',
    dt Date DEFAULT toDate(occurred_at)
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, dt, occurred_at)
TTL dt + INTERVAL 30 DAY;
`, db)); err != nil {
		return fmt.Errorf("create rag_error_context: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_rag_from_spans
TO %s.rag_error_context
AS SELECT
    generateUUIDv4()                                                AS doc_id,
    'span_error'                                                    AS source_type,
    span_id                                                         AS source_id,
    service_name,
    trace_id,
    span_id,
    fromUnixTimestamp64Nano(start_time_nano)                        AS occurred_at,
    concat(
        '[', service_name, '] ', name, '\n',
        'ERROR: ', status_message, '\n',
        if(exception_type != '', concat('Exception: ', exception_type, '\n'), ''),
        if(exception_message != '', concat('Message: ', exception_message, '\n'), ''),
        if(exception_stacktrace != '', substring(exception_stacktrace, 1, 2000), '')
    )                                                               AS content_text,
    CAST(NULL, 'Nullable(DateTime)')                                AS embedded_at,
    ''                                                              AS embedding_model,
    dt
FROM %s.spans
WHERE status_code = 2 AND status_message != '';
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_rag_from_spans: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_slow_queries_state (
    trace_id      String,
    span_id       String,
    service_name  LowCardinality(String),
    db_system     LowCardinality(String),
    db_name       LowCardinality(String),
    db_operation  LowCardinality(String),
    db_statement  String,
    duration_ms   Float64,
    start_time    DateTime64(3),
    status_code   Int32,
    dt            Date
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, dt, start_time)
TTL dt + INTERVAL 7 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_slow_queries_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_slow_queries
TO %s.mv_slow_queries_state
AS SELECT
    trace_id,
    span_id,
    service_name,
    db_system,
    db_name,
    db_operation,
    attributes['db.statement']                          AS db_statement,
    duration_nano / 1e6                                 AS duration_ms,
    fromUnixTimestamp64Nano(start_time_nano)             AS start_time,
    status_code,
    dt
FROM %s.spans
WHERE db_system != '';
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_slow_queries: %w", err)
	}

	if err := ensureAIopsSchema(conn, db); err != nil {
		return fmt.Errorf("aiops schema: %w", err)
	}

	return nil
}

// ensureAIopsSchema는 AIOps Phase 1에 필요한 두 테이블을 생성한다.
//
//   - red_baseline: 서비스/오퍼레이션별 요일+시간대 정상 성능 기준선.
//     BaselineComputer가 매시간 spans 테이블 28일치를 집계해 upsert한다.
//     ReplacingMergeTree(computed_at) → 동일 키에 대해 최신 computed_at만 유지.
//
//   - anomalies: 이상 감지 결과 기록 테이블.
//     Phase 2(Python Z-score)나 Phase 3(Go RCAEngine)이 INSERT한다.
func ensureAIopsSchema(conn driver.Conn, db string) error {
	ctx := context.Background()

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.red_baseline (
    service_name   LowCardinality(String),
    span_name      LowCardinality(String),
    http_route     LowCardinality(String),
    day_of_week    UInt8,       -- 1=Mon .. 7=Sun  (toDayOfWeek 기준)
    hour_of_day    UInt8,       -- 0–23
    p50_ms         Float64,
    p95_ms         Float64,
    p99_ms         Float64,
    error_rate     Float64,     -- 0.0–1.0
    avg_rps        Float64,     -- requests per second
    sample_count   UInt64,      -- 기준 샘플 수 (신뢰도 지표)
    computed_at    DateTime,
    dt             Date DEFAULT today()
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (service_name, span_name, http_route, day_of_week, hour_of_day)
TTL dt + INTERVAL 90 DAY;
`, db)); err != nil {
		return fmt.Errorf("create red_baseline: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.anomalies (
    id             String,      -- generateUUIDv4()
    service_name   LowCardinality(String),
    span_name      LowCardinality(String),
    anomaly_type   LowCardinality(String), -- 'latency_p95_spike' | 'error_rate_spike' | 'traffic_drop'
    minute         DateTime,
    current_value  Float64,     -- 감지 시점 실측값
    baseline_value Float64,     -- 해당 요일+시간대 기준값
    z_score        Float64,     -- 편차 (표준편차 배수)
    severity       LowCardinality(String), -- 'warning' | 'critical'
    detected_at    DateTime DEFAULT now(),
    dt             Date DEFAULT toDate(minute)
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, detected_at)
TTL dt + INTERVAL 90 DAY;
`, db)); err != nil {
		return fmt.Errorf("create anomalies: %w", err)
	}

	// AIOps Phase 3: RCA 결과 저장 테이블
	// rca_reports 마이그레이션: llm_analysis 컬럼 추가 (기존 테이블 대상)
	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.rca_reports ADD COLUMN IF NOT EXISTS llm_analysis String DEFAULT ''`, db,
	)); err != nil {
		slog.Warn("alter rca_reports llm_analysis skipped", "err", err)
	}
	// rca_reports 마이그레이션: resolved + feedback 컬럼 추가 (RAG 피드백 루프)
	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.rca_reports ADD COLUMN IF NOT EXISTS resolved UInt8 DEFAULT 0`, db,
	)); err != nil {
		slog.Warn("alter rca_reports resolved skipped", "err", err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.rca_reports ADD COLUMN IF NOT EXISTS feedback String DEFAULT ''`, db,
	)); err != nil {
		slog.Warn("alter rca_reports feedback skipped", "err", err)
	}
	// GAP-04: 배포 이벤트 상관관계 컬럼 추가 (기존 테이블 대상 마이그레이션)
	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.rca_reports ADD COLUMN IF NOT EXISTS nearby_deployments String DEFAULT ''`, db,
	)); err != nil {
		slog.Warn("alter rca_reports nearby_deployments skipped", "err", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.rca_reports (
    id                String,
    anomaly_id        String,
    service_name      LowCardinality(String),
    span_name         LowCardinality(String),
    anomaly_type      LowCardinality(String),
    minute            DateTime,
    severity          LowCardinality(String),
    z_score           Float64,
    correlated_spans    String,            -- JSON array of CorrelatedSpan
    similar_incidents   String,            -- JSON array of SimilarIncident
    nearby_deployments  String DEFAULT '', -- JSON array of NearbyDeployment (GAP-04)
    hypothesis          String,
    llm_analysis        String DEFAULT '', -- LLM 기반 RCA 분석 텍스트
    resolved            UInt8  DEFAULT 0,  -- 0: open, 1: resolved
    feedback            String DEFAULT '', -- 운영자 피드백 텍스트
    created_at        DateTime DEFAULT now(),
    dt                Date DEFAULT toDate(minute)
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, created_at)
TTL dt + INTERVAL 90 DAY;
`, db)); err != nil {
		return fmt.Errorf("create rca_reports: %w", err)
	}

	return nil
}

func ensureMetricsTable(conn driver.Conn, db string, retentionDays int) error {
	ctx := context.Background()

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.metrics (
    name           LowCardinality(String),
    type           LowCardinality(String),
    value          Float64,
    attributes     Map(String, String),
    service_name   LowCardinality(String),
    timestamp_nano Int64,
    received_at_ms Int64,
    dt Date DEFAULT toDate(fromUnixTimestamp64Milli(received_at_ms))
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, name, timestamp_nano)
TTL dt + INTERVAL %d DAY;
`, db, retentionDays)); err != nil {
		return err
	}
	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.metrics MODIFY TTL dt + INTERVAL %d DAY`, db, retentionDays)); err != nil {
		return err
	}
	// #6: attributes String → Map(String,String) 마이그레이션
	for _, q := range []string{
		fmt.Sprintf("ALTER TABLE %s.metrics MODIFY COLUMN IF EXISTS attributes Map(String, String)", db),
		fmt.Sprintf("ALTER TABLE %s.metric_histograms MODIFY COLUMN IF EXISTS attributes Map(String, String)", db),
	} {
		if err := conn.Exec(ctx, q); err != nil {
			slog.Warn("alter metrics attributes column skipped", "query", q, "err", err)
		}
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.metric_histograms (
    service_name    LowCardinality(String),
    metric_name     LowCardinality(String),
    timestamp_nano  Int64,
    bounds          Array(Float64),
    bucket_counts   Array(UInt64),
    total_count     UInt64,
    total_sum       Float64,
    attributes      Map(String, String),
    exemplar_trace_ids Array(String),
    exemplar_span_ids  Array(String),
    exemplar_values    Array(Float64),
    exemplar_times     Array(Int64),
    exemplar_attributes Array(String),
    received_at_ms  Int64,
    dt Date DEFAULT toDate(fromUnixTimestamp64Milli(received_at_ms))
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, metric_name, timestamp_nano)
TTL dt + INTERVAL %d DAY;
`, db, retentionDays)); err != nil {
		return fmt.Errorf("create metric_histograms: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_histogram_1m_state (
    service_name         LowCardinality(String),
    metric_name          LowCardinality(String),
    minute               DateTime,
    count_state          AggregateFunction(sum, UInt64),
    sum_state            AggregateFunction(sum, Float64),
    bucket_counts_state  AggregateFunction(sumForEach, Array(UInt64)),
    bounds_state         AggregateFunction(any, Array(Float64)),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, metric_name, minute)
TTL dt + INTERVAL 90 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_histogram_1m_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_histogram_1m
TO %s.mv_histogram_1m_state
AS SELECT
    service_name,
    metric_name,
    toStartOfMinute(fromUnixTimestamp64Nano(timestamp_nano)) AS minute,
    sumState(total_count)          AS count_state,
    sumState(total_sum)            AS sum_state,
    sumForEachState(bucket_counts) AS bucket_counts_state,
    anyState(bounds)               AS bounds_state,
    dt
FROM %s.metric_histograms
GROUP BY service_name, metric_name, minute, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_histogram_1m: %w", err)
	}

	// M-7: 데이터 티어링 — 5분 히스토그램 롤업 (180일 보관)
	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_histogram_5m_state (
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
TTL dt + INTERVAL 180 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_histogram_5m_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_histogram_5m
TO %s.mv_histogram_5m_state
AS SELECT
    service_name,
    metric_name,
    toStartOfFiveMinute(fromUnixTimestamp64Nano(timestamp_nano)) AS minute5,
    sumState(total_count)          AS count_state,
    sumState(total_sum)            AS sum_state,
    sumForEachState(bucket_counts) AS bucket_counts_state,
    anyState(bounds)               AS bounds_state,
    dt
FROM %s.metric_histograms
GROUP BY service_name, metric_name, minute5, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_histogram_5m: %w", err)
	}

	// M-7: 데이터 티어링 — 1시간 히스토그램 롤업 (365일 보관)
	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_histogram_1h_state (
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
TTL dt + INTERVAL 365 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_histogram_1h_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_histogram_1h
TO %s.mv_histogram_1h_state
AS SELECT
    service_name,
    metric_name,
    toStartOfHour(fromUnixTimestamp64Nano(timestamp_nano)) AS hour,
    sumState(total_count)          AS count_state,
    sumState(total_sum)            AS sum_state,
    sumForEachState(bucket_counts) AS bucket_counts_state,
    anyState(bounds)               AS bounds_state,
    dt
FROM %s.metric_histograms
GROUP BY service_name, metric_name, hour, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_histogram_1h: %w", err)
	}

	return nil
}

func ensureLogsTable(conn driver.Conn, db string, retentionDays int) error {
	ctx := context.Background()

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.logs (
    severity_text   LowCardinality(String),
    severity_number Int32,
    body            String,
    attributes      Map(String, String),
    service_name    LowCardinality(String),
    trace_id        String,
    span_id         String,
    timestamp_nano  Int64,
    received_at_ms  Int64,
    exception_type  LowCardinality(String) DEFAULT '',
    logger_name     LowCardinality(String) DEFAULT '',
    dt Date DEFAULT toDate(fromUnixTimestamp64Milli(received_at_ms))
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, severity_number, timestamp_nano)
TTL dt + INTERVAL %d DAY;
`, db, retentionDays)); err != nil {
		return err
	}

	alterCols := []string{
		fmt.Sprintf("ALTER TABLE %s.logs ADD COLUMN IF NOT EXISTS exception_type LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.logs ADD COLUMN IF NOT EXISTS logger_name LowCardinality(String) DEFAULT ''", db),
		// #6: attributes String → Map(String,String) 마이그레이션
		fmt.Sprintf("ALTER TABLE %s.logs MODIFY COLUMN IF EXISTS attributes Map(String, String)", db),
	}
	for _, q := range alterCols {
		if err := conn.Exec(ctx, q); err != nil {
			slog.Warn("alter logs column skipped", "query", q, "err", err)
		}
	}

	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.logs MODIFY TTL dt + INTERVAL %d DAY`, db, retentionDays)); err != nil {
		return err
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_error_logs_1m_state (
    service_name   LowCardinality(String),
    exception_type LowCardinality(String),
    minute         DateTime,
    error_count    SimpleAggregateFunction(sum, UInt64),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, minute, exception_type)
TTL dt + INTERVAL 30 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_error_logs_1m_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_error_logs_1m
TO %s.mv_error_logs_1m_state
AS SELECT
    service_name,
    exception_type,
    toStartOfMinute(fromUnixTimestamp64Nano(timestamp_nano)) AS minute,
    toUInt64(count()) AS error_count,
    dt
FROM %s.logs
WHERE severity_number >= 17
GROUP BY service_name, exception_type, minute, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_error_logs_1m: %w", err)
	}

	// M-7: 데이터 티어링 — 1시간 에러 로그 롤업 (365일 보관)
	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_error_logs_1h_state (
    service_name   LowCardinality(String),
    exception_type LowCardinality(String),
    hour           DateTime,
    error_count    SimpleAggregateFunction(sum, UInt64),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, hour, exception_type)
TTL dt + INTERVAL 365 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_error_logs_1h_state: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_error_logs_1h
TO %s.mv_error_logs_1h_state
AS SELECT
    service_name,
    exception_type,
    toStartOfHour(fromUnixTimestamp64Nano(timestamp_nano)) AS hour,
    toUInt64(count()) AS error_count,
    dt
FROM %s.logs
WHERE severity_number >= 17
GROUP BY service_name, exception_type, hour, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_error_logs_1h: %w", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_rag_from_logs
TO %s.rag_error_context
AS SELECT
    generateUUIDv4()                                        AS doc_id,
    'log_error'                                             AS source_type,
    toString(timestamp_nano)                                AS source_id,
    service_name,
    trace_id,
    span_id,
    fromUnixTimestamp64Nano(timestamp_nano)                 AS occurred_at,
    concat(
        '[', service_name, '] ',
        if(exception_type != '', concat('[', exception_type, '] '), ''),
        body
    )                                                       AS content_text,
    CAST(NULL, 'Nullable(DateTime)')                        AS embedded_at,
    ''                                                      AS embedding_model,
    dt
FROM %s.logs
WHERE severity_number >= 17;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_rag_from_logs: %w", err)
	}

	return nil
}
