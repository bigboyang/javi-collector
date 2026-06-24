package store

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kkc/javi-collector/internal/model"
)

// ClickHouseTraceStore는 SpanData를 apm.spans 테이블에 배치 insert한다.
type ClickHouseTraceStore struct {
	conn    driver.Conn            // 공유 커넥션 풀 (소유권 없음 — Close()에서 닫지 않는다)
	ch      chan *model.SpanData   // 수신 데이터 채널
	flushCh chan []*model.SpanData // 조립된 배치를 flush worker에 전달하는 큐
	cfg     ClickHouseConfig
	dynCfg  atomic.Pointer[storeDynCfg] // 핫 리로드 가능한 배치 설정 (nil이면 cfg 사용)
	done    chan struct{}               // 모든 flushWorker 종료 시 닫힘
	dlq     *FileBackupWriter           // flush 실패 시 배치 보존 (nil이면 비활성화)
	cb      *circuitBreaker             // 연속 실패 시 flush 차단 (nil이면 비활성화)
}

// NewClickHouseTraceStore는 공유 커넥션을 받아 테이블 DDL을 적용하고
// batchWriter 및 FlushWorkers개의 flushWorker goroutine을 시작한다.
// conn 소유권은 호출자가 가진다 — Close()를 호출해도 conn은 닫히지 않는다.
func NewClickHouseTraceStore(conn driver.Conn, cfg ClickHouseConfig) (*ClickHouseTraceStore, error) {
	if err := ensureSpansTable(conn, cfg.Database, cfg.RetentionDays); err != nil {
		return nil, fmt.Errorf("clickhouse trace DDL: %w", err)
	}

	// 버전 관리 마이그레이션: ensureSpansTable 이후의 스키마 변경을 추적 적용한다.
	migrator := NewMigrator(conn, cfg.Database)
	if err := migrator.Run(context.Background(), BuildSpansMigrations(cfg.Database)); err != nil {
		// 마이그레이션 실패는 경고로만 처리 — 대부분의 SQL이 IF NOT EXISTS로 idempotent함
		slog.Warn("spans schema migration failed", "err", err)
	}

	var dlq *FileBackupWriter
	if cfg.DLQDir != "" {
		var err error
		dlq, err = NewFileBackupWriter(cfg.DLQDir)
		if err != nil {
			return nil, fmt.Errorf("clickhouse trace DLQ init: %w", err)
		}
	}

	var cb *circuitBreaker
	if cfg.CBFailureThreshold > 0 {
		cb = newCircuitBreaker("spans", cfg.CBFailureThreshold, cfg.CBCooldown)
	}

	workers := cfg.FlushWorkers
	if workers < 1 {
		workers = 1
	}

	s := &ClickHouseTraceStore{
		conn:    conn,
		ch:      make(chan *model.SpanData, cfg.ChanBuffer),
		flushCh: make(chan []*model.SpanData, workers*2),
		cfg:     cfg,
		done:    make(chan struct{}),
		dlq:     dlq,
		cb:      cb,
	}

	chFlushWorkerPoolSize.WithLabelValues("spans").Set(float64(workers))

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.flushWorker()
		}()
	}
	go func() {
		wg.Wait()
		close(s.done)
	}()
	go s.batchWriter()
	return s, nil
}

// SetDynamicConfig는 배치 설정을 런타임에 변경한다.
// batchSize 또는 flushInterval이 0이면 기존 cfg 값을 유지한다.
func (s *ClickHouseTraceStore) SetDynamicConfig(batchSize int, flushInterval time.Duration) {
	cur := s.dynCfg.Load()
	next := storeDynCfg{BatchSize: s.cfg.BatchSize, FlushInterval: s.cfg.FlushInterval}
	if cur != nil {
		next = *cur
	}
	if batchSize > 0 {
		next.BatchSize = batchSize
	}
	if flushInterval > 0 {
		next.FlushInterval = flushInterval
	}
	s.dynCfg.Store(&next)
	slog.Info("trace store dynamic config updated",
		"batch_size", next.BatchSize,
		"flush_interval", next.FlushInterval,
	)
}

// loadDynCfg는 현재 유효한 배치 설정을 반환한다.
func (s *ClickHouseTraceStore) loadDynCfg() storeDynCfg {
	if p := s.dynCfg.Load(); p != nil {
		return *p
	}
	return storeDynCfg{BatchSize: s.cfg.BatchSize, FlushInterval: s.cfg.FlushInterval}
}

// AppendSpans는 spans를 채널에 enqueue한다.
// TOCTOU 경쟁 방지: pre-check 없이 non-blocking send만 사용한다.
// 일부 항목만 삽입되면 나머지는 chDroppedTotal에 계상하고 nil을 반환한다.
// 채널이 완전히 꽉 차서 0개도 삽입되지 않은 경우에만 error를 반환한다 (→ 503).
func (s *ClickHouseTraceStore) AppendSpans(_ context.Context, spans []*model.SpanData) error {
	inserted := 0
	for _, sp := range spans {
		select {
		case s.ch <- sp:
			inserted++
		default:
			chDroppedTotal.WithLabelValues("spans").Inc()
		}
	}
	chChannelDepth.WithLabelValues("spans").Set(float64(len(s.ch)))
	if inserted == 0 && len(spans) > 0 {
		return fmt.Errorf("trace channel full (capacity=%d): backpressure", cap(s.ch))
	}
	return nil
}

// QuerySpans는 필터에 맞는 span을 반환한다.
func (s *ClickHouseTraceStore) QuerySpans(ctx context.Context, q SpanQuery) ([]*model.SpanData, error) {
	if q.Limit <= 0 {
		q.Limit = 100
	}

	var conds []string
	var args []any

	if q.FromMs > 0 {
		conds = append(conds, "received_at_ms >= ?")
		args = append(args, q.FromMs)
	}
	if q.ToMs > 0 {
		conds = append(conds, "received_at_ms <= ?")
		args = append(args, q.ToMs)
	}
	if q.ServiceName != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, q.ServiceName)
	}
	if q.TraceID != "" {
		conds = append(conds, "trace_id = ?")
		args = append(args, q.TraceID)
	}
	if q.StatusCode >= 0 {
		conds = append(conds, "status_code = ?")
		args = append(args, q.StatusCode)
	}
	if q.MinDurationMs > 0 {
		conds = append(conds, "duration_nano >= ?")
		args = append(args, q.MinDurationMs*1_000_000)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	sql := fmt.Sprintf(
		`SELECT trace_id, span_id, parent_span_id, name, kind,
		        start_time_nano, end_time_nano, duration_nano,
		        attributes, status_code, status_message,
		        service_name, scope_name, received_at_ms
		 FROM %s.spans
		 %s
		 ORDER BY received_at_ms DESC
		 LIMIT %d OFFSET %d`,
		s.cfg.Database, where, q.Limit, q.Offset,
	)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*model.SpanData
	for rows.Next() {
		var (
			sp           model.SpanData
			attrsMap     map[string]string
			durationNano int64
		)
		if err := rows.Scan(
			&sp.TraceID, &sp.SpanID, &sp.ParentSpanID, &sp.Name, &sp.Kind,
			&sp.StartTimeNano, &sp.EndTimeNano, &durationNano,
			&attrsMap, &sp.StatusCode, &sp.StatusMessage,
			&sp.ServiceName, &sp.ScopeName, &sp.ReceivedAtMs,
		); err != nil {
			return nil, err
		}
		sp.Attributes = fromStringMap(attrsMap)
		result = append(result, &sp)
	}
	return result, rows.Err()
}

// redTableForRange는 쿼리 시간 범위에 따라 적절한 RED 롤업 테이블과 타임스탬프 컬럼명을 반환한다.
//
// 티어링 기준:
//   - < 3h:   1분 집계 (mv_red_1m_state, "minute")
//   - 3h–24h: 5분 집계 (mv_red_5m_state, "minute5")
//   - > 24h:  1시간 집계 (mv_red_1h_state, "hour")
func redTableForRange(fromMs, toMs int64) (table, tsCol string) {
	if fromMs <= 0 || toMs <= 0 {
		return "mv_red_1m_state", "minute"
	}
	const (
		threeHoursMs    = int64(3 * 60 * 60 * 1000)
		twentyFourHrsMs = int64(24 * 60 * 60 * 1000)
	)
	rangeMs := toMs - fromMs
	switch {
	case rangeMs > twentyFourHrsMs:
		return "mv_red_1h_state", "hour"
	case rangeMs > threeHoursMs:
		return "mv_red_5m_state", "minute5"
	default:
		return "mv_red_1m_state", "minute"
	}
}

// QueryRED는 서비스별 RED 메트릭을 반환한다.
// 쿼리 시간 범위에 따라 1분/5분/1시간 롤업 테이블 중 하나를 자동 선택한다 (데이터 티어링).
func (s *ClickHouseTraceStore) QueryRED(ctx context.Context, service string, fromMs, toMs int64) ([]map[string]any, error) {
	table, tsCol := redTableForRange(fromMs, toMs)

	var conds []string
	var args []any

	if fromMs > 0 {
		conds = append(conds, tsCol+" >= ?")
		args = append(args, fromMs/1000)
	}
	if toMs > 0 {
		conds = append(conds, tsCol+" <= ?")
		args = append(args, toMs/1000)
	}
	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	sql := fmt.Sprintf(`
SELECT
    service_name,
    span_name,
    http_route,
    %s AS minute,
    sum(total_count)                                                   AS rps,
    sum(error_count)                                                   AS errors,
    sum(error_count) / sum(total_count) * 100                         AS error_rate_pct,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[1] / 1e6    AS p50_ms,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[2] / 1e6    AS p95_ms,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[3] / 1e6    AS p99_ms
FROM %s.%s
%s
GROUP BY service_name, span_name, http_route, %s
ORDER BY minute DESC
LIMIT 1000`, tsCol, s.cfg.Database, table, where, tsCol)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			serviceName, spanName, httpRoute string
			minute                           time.Time
			rps, errors                      uint64
			errorRatePct, p50, p95, p99      float64
		)
		if err := rows.Scan(&serviceName, &spanName, &httpRoute, &minute,
			&rps, &errors, &errorRatePct, &p50, &p95, &p99); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"service_name":   serviceName,
			"span_name":      spanName,
			"http_route":     httpRoute,
			"minute":         minute.UnixMilli(),
			"rps":            rps,
			"errors":         errors,
			"error_rate_pct": errorRatePct,
			"p50_ms":         p50,
			"p95_ms":         p95,
			"p99_ms":         p99,
		})
	}
	return result, rows.Err()
}

// QueryTopology는 mv_service_topology_state에서 서비스 간 호출 관계를 반환한다.
func (s *ClickHouseTraceStore) QueryTopology(ctx context.Context, fromMs, toMs int64) ([]map[string]any, error) {
	var conds []string
	var args []any

	if fromMs > 0 {
		conds = append(conds, "minute5 >= ?")
		args = append(args, fromMs/1000)
	}
	if toMs > 0 {
		conds = append(conds, "minute5 <= ?")
		args = append(args, toMs/1000)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	sql := fmt.Sprintf(`
SELECT
    caller_service,
    callee_service,
    sum(call_count)                                  AS total_calls,
    sum(error_count)                                 AS error_calls,
    sum(error_count) / sum(call_count) * 100         AS error_rate_pct,
    sum(duration_sum) / sum(call_count) / 1e6        AS avg_latency_ms
FROM %s.mv_service_topology_state
%s
GROUP BY caller_service, callee_service
ORDER BY total_calls DESC`, s.cfg.Database, where)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			caller, callee         string
			totalCalls, errorCalls uint64
			errorRate, avgLatency  float64
		)
		if err := rows.Scan(&caller, &callee, &totalCalls, &errorCalls, &errorRate, &avgLatency); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"caller_service": caller,
			"callee_service": callee,
			"total_calls":    totalCalls,
			"error_calls":    errorCalls,
			"error_rate_pct": errorRate,
			"avg_latency_ms": avgLatency,
		})
	}
	return result, rows.Err()
}

// QueryAnomalies는 anomalies 테이블에서 이상 이벤트 목록을 반환한다.
// GET /api/collector/anomalies?service=&severity=&from=<ms>&to=<ms>&limit=
func (s *ClickHouseTraceStore) QueryAnomalies(ctx context.Context, service, severity string, fromMs, toMs int64, limit int) ([]map[string]any, error) {
	var conds []string
	var args []any

	if fromMs > 0 {
		conds = append(conds, "detected_at >= ?")
		args = append(args, time.UnixMilli(fromMs))
	}
	if toMs > 0 {
		conds = append(conds, "detected_at <= ?")
		args = append(args, time.UnixMilli(toMs))
	}
	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}
	if severity != "" {
		conds = append(conds, "severity = ?")
		args = append(args, severity)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	if limit <= 0 {
		limit = 100
	}

	q := fmt.Sprintf(`
SELECT id, service_name, span_name, anomaly_type, minute,
       current_value, baseline_value, z_score, severity, detected_at
FROM %s.anomalies
%s
ORDER BY detected_at DESC
LIMIT %d`, s.cfg.Database, where, limit)

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			id, serviceName, spanName, anomalyType, sev string
			minute, detectedAt                          time.Time
			currentValue, baselineValue, zScore         float64
		)
		if err := rows.Scan(&id, &serviceName, &spanName, &anomalyType, &minute,
			&currentValue, &baselineValue, &zScore, &sev, &detectedAt); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"id":             id,
			"service_name":   serviceName,
			"span_name":      spanName,
			"anomaly_type":   anomalyType,
			"minute":         minute.UnixMilli(),
			"current_value":  currentValue,
			"baseline_value": baselineValue,
			"z_score":        zScore,
			"severity":       sev,
			"detected_at":    detectedAt.UnixMilli(),
		})
	}
	return result, rows.Err()
}

// QueryBrokenTraces는 root span이 없는 트레이스를 감지한다.
// traceID 기준으로 집계: parent_span_id가 빈 문자열(empty string)인 span이 없는 trace = 브로큰 트레이스.
// 계측 미설정, 샘플링 불일치, 또는 네트워크 손실이 원인일 수 있다.
func (s *ClickHouseTraceStore) QueryBrokenTraces(ctx context.Context, service string, fromMs, toMs int64, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}

	var conds []string
	var args []any

	if fromMs > 0 {
		conds = append(conds, "start_time_nano >= ?")
		args = append(args, fromMs*1_000_000)
	}
	if toMs > 0 {
		conds = append(conds, "start_time_nano <= ?")
		args = append(args, toMs*1_000_000)
	}
	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	q := fmt.Sprintf(`
SELECT
    trace_id,
    count()                                               AS span_count,
    anyIf(service_name, service_name != '')               AS service_name,
    min(intDiv(start_time_nano, 1000000))                 AS start_ms,
    countIf(status_code = 2)                              AS error_count,
    max(intDiv(end_time_nano - start_time_nano, 1000000)) AS max_duration_ms
FROM %s.spans
%s
GROUP BY trace_id
HAVING countIf(parent_span_id = '') = 0
ORDER BY start_ms DESC
LIMIT %d`, s.cfg.Database, where, limit)

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			traceID, serviceName   string
			spanCount, errorCount  uint64
			startMs, maxDurationMs int64
		)
		if err := rows.Scan(&traceID, &spanCount, &serviceName, &startMs, &errorCount, &maxDurationMs); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"trace_id":        traceID,
			"span_count":      spanCount,
			"service_name":    serviceName,
			"start_ms":        startMs,
			"error_count":     errorCount,
			"max_duration_ms": maxDurationMs,
		})
	}
	return result, rows.Err()
}

// QuerySlowQueries는 mv_slow_queries_state에서 thresholdMs 이상 소요된 DB 쿼리를 반환한다.
// GET /api/collector/slow-queries?service=svc&from=<ms>&to=<ms>&threshold_ms=500&limit=100
func (s *ClickHouseTraceStore) QuerySlowQueries(ctx context.Context, service string, fromMs, toMs, thresholdMs int64, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	if thresholdMs <= 0 {
		thresholdMs = 500
	}

	var conds []string
	var args []any

	conds = append(conds, "duration_ms >= ?")
	args = append(args, float64(thresholdMs))

	if fromMs > 0 {
		conds = append(conds, "start_time >= ?")
		args = append(args, fromMs/1000)
	}
	if toMs > 0 {
		conds = append(conds, "start_time <= ?")
		args = append(args, toMs/1000)
	}
	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}

	where := "WHERE " + strings.Join(conds, " AND ")

	q := fmt.Sprintf(`
SELECT
    trace_id,
    span_id,
    service_name,
    db_system,
    db_name,
    db_operation,
    db_statement,
    round(duration_ms, 2) AS duration_ms,
    start_time,
    status_code
FROM %s.mv_slow_queries_state
%s
ORDER BY duration_ms DESC
LIMIT %d`, s.cfg.Database, where, limit)

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			traceID, spanID, serviceName   string
			dbSystem, dbName, dbOp, dbStmt string
			durationMs                     float64
			startTime                      time.Time
			statusCode                     int32
		)
		if err := rows.Scan(&traceID, &spanID, &serviceName,
			&dbSystem, &dbName, &dbOp, &dbStmt,
			&durationMs, &startTime, &statusCode); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"trace_id":     traceID,
			"span_id":      spanID,
			"service_name": serviceName,
			"db_system":    dbSystem,
			"db_name":      dbName,
			"db_operation": dbOp,
			"db_statement": dbStmt,
			"duration_ms":  durationMs,
			"start_time":   startTime.UnixMilli(),
			"status_code":  statusCode,
		})
	}
	return result, rows.Err()
}

// QueryTraceContext는 trace_id로 연관된 spans·logs·RED 메트릭을 한 번에 반환한다.
// Gap 1: Correlated Signal Navigation — Datadog의 "Trace to Logs/Metrics" 피벗에 해당.
func (s *ClickHouseTraceStore) QueryTraceContext(ctx context.Context, traceID string) (map[string]any, error) {
	// 1) Spans
	spanRows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT
    span_id, parent_span_id, service_name, span_name,
    intDiv(start_time_nano, 1000000) AS start_ms,
    intDiv(end_time_nano - start_time_nano, 1000000) AS duration_ms,
    status_code, exception_type, exception_message, http_status_code
FROM %s.spans
WHERE trace_id = ?
ORDER BY start_ms ASC
LIMIT 500`, s.cfg.Database), traceID)
	if err != nil {
		return nil, fmt.Errorf("query spans: %w", err)
	}
	defer spanRows.Close()

	var spans []map[string]any
	var serviceName string
	var fromMs, toMs int64
	for spanRows.Next() {
		var (
			spanID, parentSpanID, svc, spanName string
			startMs, durationMs                 int64
			statusCode, httpStatus              int32
			excType, excMsg                     string
		)
		if err := spanRows.Scan(&spanID, &parentSpanID, &svc, &spanName,
			&startMs, &durationMs, &statusCode, &excType, &excMsg, &httpStatus); err != nil {
			return nil, fmt.Errorf("scan span: %w", err)
		}
		if serviceName == "" {
			serviceName = svc
		}
		if fromMs == 0 || startMs < fromMs {
			fromMs = startMs
		}
		if endMs := startMs + durationMs; endMs > toMs {
			toMs = endMs
		}
		spans = append(spans, map[string]any{
			"span_id":           spanID,
			"parent_span_id":    parentSpanID,
			"service_name":      svc,
			"span_name":         spanName,
			"start_ms":          startMs,
			"duration_ms":       durationMs,
			"status_code":       statusCode,
			"exception_type":    excType,
			"exception_message": excMsg,
			"http_status_code":  httpStatus,
		})
	}
	if err := spanRows.Err(); err != nil {
		return nil, err
	}

	// 2) Logs: trace_id 또는 service_name + 시간 범위로 조회
	logArgs := []any{traceID}
	logWhere := "trace_id = ?"
	if serviceName != "" && fromMs > 0 {
		// trace_id가 비어 있는 로그도 서비스+시간으로 포함
		logWhere = "(trace_id = ? OR (service_name = ? AND timestamp_nano BETWEEN ? AND ?))"
		logArgs = []any{traceID, serviceName, fromMs * 1_000_000, toMs * 1_000_000}
	}
	logRows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT
    severity_text, body, service_name,
    intDiv(timestamp_nano, 1000000) AS ts_ms,
    trace_id, span_id, exception_type
FROM %s.logs
WHERE %s
ORDER BY ts_ms ASC
LIMIT 200`, s.cfg.Database, logWhere), logArgs...)
	if err != nil {
		return nil, fmt.Errorf("query logs: %w", err)
	}
	defer logRows.Close()

	var logs []map[string]any
	for logRows.Next() {
		var (
			severity, body, svc, trID, spID, excType string
			tsMs                                     int64
		)
		if err := logRows.Scan(&severity, &body, &svc, &tsMs, &trID, &spID, &excType); err != nil {
			return nil, fmt.Errorf("scan log: %w", err)
		}
		logs = append(logs, map[string]any{
			"severity":       severity,
			"body":           body,
			"service_name":   svc,
			"ts_ms":          tsMs,
			"trace_id":       trID,
			"span_id":        spID,
			"exception_type": excType,
		})
	}
	if err := logRows.Err(); err != nil {
		return nil, err
	}

	// 3) RED 메트릭: 서비스 + 트레이스 시간 범위의 1분 집계
	var redMetrics []map[string]any
	if serviceName != "" && fromMs > 0 {
		redRows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT
    minute,
    sum(total_count) AS rps,
    sum(error_count) AS errors,
    quantilesMerge(0.95)(duration_quantiles)[1] / 1e6 AS p95_ms
FROM %s.mv_red_1m_state
WHERE service_name = ?
  AND minute BETWEEN toDateTime(?) AND toDateTime(?)
GROUP BY minute
ORDER BY minute ASC`, s.cfg.Database),
			serviceName,
			fromMs/1000-60, // -1분 여유
			toMs/1000+60,
		)
		if err == nil {
			defer redRows.Close()
			for redRows.Next() {
				var minute time.Time
				var rps, errors uint64
				var p95 float64
				if err := redRows.Scan(&minute, &rps, &errors, &p95); err == nil {
					redMetrics = append(redMetrics, map[string]any{
						"minute_ms": minute.UnixMilli(),
						"rps":       rps,
						"errors":    errors,
						"p95_ms":    p95,
					})
				}
			}
		}
	}

	return map[string]any{
		"trace_id":     traceID,
		"service_name": serviceName,
		"from_ms":      fromMs,
		"to_ms":        toMs,
		"spans":        spans,
		"logs":         logs,
		"red_metrics":  redMetrics,
	}, nil
}

// ---- GAP-01: Trace Waterfall / Critical Path ----

// waterfallNode는 Waterfall 트리 빌드에 사용되는 내부 타입이다.
type waterfallNode struct {
	spanID         string
	parentSpanID   string
	serviceName    string
	spanName       string
	kind           int32
	startMs        int64
	durationMs     int64
	statusCode     int32
	exceptionType  string
	httpStatusCode int32
	children       []*waterfallNode
	// 계산 필드
	onCriticalPath bool
	maxLeafEndMs   int64 // 서브트리의 최대 end 시간 (criticalPath 계산용)
	visited        bool  // 사이클 감지용
}

// QueryTraceWaterfall은 trace_id의 스팬을 폭포수 뷰 + 임계 경로 정보로 반환한다.
// GAP-01: Trace Waterfall / Critical Path — Datadog의 Flame Graph / Waterfall에 해당.
//
// 반환 구조:
//
//	{
//	  "summary": { trace_id, total_duration_ms, critical_path_ms, span_count, service_count, root_span_count },
//	  "spans":   [ { span_id, parent_span_id, service_name, span_name, kind, start_ms, duration_ms,
//	                 offset_ms, depth, on_critical_path, status_code, exception_type, http_status_code } ]
//	}
func (s *ClickHouseTraceStore) QueryTraceWaterfall(ctx context.Context, traceID string) (map[string]any, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT
    span_id, parent_span_id, service_name, span_name, kind,
    intDiv(start_time_nano, 1000000)                    AS start_ms,
    intDiv(end_time_nano - start_time_nano, 1000000)    AS duration_ms,
    status_code, exception_type, exception_message, http_status_code
FROM %s.spans
WHERE trace_id = ?
ORDER BY start_ms ASC
LIMIT 500`, s.cfg.Database), traceID)
	if err != nil {
		return nil, fmt.Errorf("query waterfall spans: %w", err)
	}
	defer rows.Close()

	var nodes []*waterfallNode
	for rows.Next() {
		var n waterfallNode
		var excMsg string
		if err := rows.Scan(
			&n.spanID, &n.parentSpanID, &n.serviceName, &n.spanName, &n.kind,
			&n.startMs, &n.durationMs,
			&n.statusCode, &n.exceptionType, &excMsg, &n.httpStatusCode,
		); err != nil {
			return nil, fmt.Errorf("scan waterfall span: %w", err)
		}
		nodes = append(nodes, &n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, nil // handler에서 404 처리
	}

	// Phase 1: 인덱스 + 트레이스 시작 시간 탐색
	byID := make(map[string]*waterfallNode, len(nodes))
	traceStartMs := nodes[0].startMs
	for _, n := range nodes {
		byID[n.spanID] = n
		if n.startMs < traceStartMs {
			traceStartMs = n.startMs
		}
	}

	// Phase 2: 부모-자식 연결 + 루트(orphan 포함) 탐색
	services := make(map[string]struct{})
	var roots []*waterfallNode
	for _, n := range nodes {
		services[n.serviceName] = struct{}{}
		if n.parentSpanID == "" || byID[n.parentSpanID] == nil {
			roots = append(roots, n)
		} else {
			byID[n.parentSpanID].children = append(byID[n.parentSpanID].children, n)
		}
	}

	// Phase 3: maxLeafEndMs 계산 (포스트-오더 DFS, 사이클 방어)
	var computeMax func(n *waterfallNode) int64
	computeMax = func(n *waterfallNode) int64 {
		if n.visited {
			return n.startMs + n.durationMs // 사이클 감지: 현재 end 반환
		}
		n.visited = true
		maxEnd := n.startMs + n.durationMs
		for _, child := range n.children {
			if childMax := computeMax(child); childMax > maxEnd {
				maxEnd = childMax
			}
		}
		n.maxLeafEndMs = maxEnd
		return maxEnd
	}
	for _, r := range roots {
		computeMax(r)
	}

	// Phase 4: 임계 경로 마킹 (글로벌 최대 end를 가진 루트부터 하향)
	var globalMax int64
	for _, r := range roots {
		if r.maxLeafEndMs > globalMax {
			globalMax = r.maxLeafEndMs
		}
	}
	var markCritical func(n *waterfallNode, targetEnd int64)
	markCritical = func(n *waterfallNode, targetEnd int64) {
		n.onCriticalPath = true
		for _, child := range n.children {
			if child.maxLeafEndMs == targetEnd {
				markCritical(child, targetEnd)
				break
			}
		}
	}
	for _, r := range roots {
		if r.maxLeafEndMs == globalMax {
			markCritical(r, globalMax)
			break
		}
	}

	// Phase 5: DFS 평탄화 + depth/offset 할당 (자식은 startMs 오름차순)
	var result []map[string]any
	var flatten func(n *waterfallNode, depth int)
	flatten = func(n *waterfallNode, depth int) {
		// 자식을 startMs 기준으로 삽입 정렬 (일반적으로 span 수가 적어 충분)
		for i := 1; i < len(n.children); i++ {
			for j := i; j > 0 && n.children[j].startMs < n.children[j-1].startMs; j-- {
				n.children[j], n.children[j-1] = n.children[j-1], n.children[j]
			}
		}
		offsetMs := n.startMs - traceStartMs
		if offsetMs < 0 {
			offsetMs = 0 // 클럭 스큐 방어
		}
		result = append(result, map[string]any{
			"span_id":          n.spanID,
			"parent_span_id":   n.parentSpanID,
			"service_name":     n.serviceName,
			"span_name":        n.spanName,
			"kind":             n.kind,
			"start_ms":         n.startMs,
			"duration_ms":      n.durationMs,
			"offset_ms":        offsetMs,
			"depth":            depth,
			"on_critical_path": n.onCriticalPath,
			"status_code":      n.statusCode,
			"exception_type":   n.exceptionType,
			"http_status_code": n.httpStatusCode,
		})
		for _, child := range n.children {
			flatten(child, depth+1)
		}
	}
	// 루트도 startMs 기준으로 정렬
	for i := 1; i < len(roots); i++ {
		for j := i; j > 0 && roots[j].startMs < roots[j-1].startMs; j-- {
			roots[j], roots[j-1] = roots[j-1], roots[j]
		}
	}
	for _, r := range roots {
		flatten(r, 0)
	}

	return map[string]any{
		"summary": map[string]any{
			"trace_id":          traceID,
			"total_duration_ms": globalMax - traceStartMs,
			"critical_path_ms":  globalMax - traceStartMs,
			"span_count":        len(nodes),
			"service_count":     len(services),
			"root_span_count":   len(roots),
		},
		"spans": result,
	}, nil
}

// QueryRaw는 화이트리스트를 통과한 SELECT 쿼리를 ClickHouse에 직접 실행하고
// []map[string]any 형태로 결과를 반환한다.
// 컬럼 이름과 값은 ClickHouse driver의 타입을 그대로 사용한다.
func (s *ClickHouseTraceStore) QueryRaw(ctx context.Context, sql string) ([]map[string]any, error) {
	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := rows.Columns()
	var result []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// ChannelStatus는 readyz 상세 응답에 포함할 채널 포화도와 서킷 브레이커 상태를 반환한다.
func (s *ClickHouseTraceStore) ChannelStatus() map[string]any {
	cbState := "disabled"
	if s.cb != nil {
		s.cb.mu.Lock()
		switch s.cb.state {
		case cbStateClosed:
			cbState = "closed"
		case cbStateOpen:
			cbState = "open"
		case cbStateHalfOpen:
			cbState = "half_open"
		}
		s.cb.mu.Unlock()
	}
	return map[string]any{
		"ch_len":   len(s.ch),
		"ch_cap":   cap(s.ch),
		"cb_state": cbState,
	}
}

// Ping은 ClickHouse 연결 상태를 확인한다.
func (s *ClickHouseTraceStore) Ping(ctx context.Context) error {
	return s.conn.Ping(ctx)
}

// Close는 채널을 닫고 batchWriter가 남은 항목을 flush할 때까지 대기한다.
// conn은 공유 자원이므로 닫지 않는다 — 호출자가 직접 conn.Close()를 호출해야 한다.
func (s *ClickHouseTraceStore) Close() error {
	close(s.ch)
	select {
	case <-s.done:
	case <-time.After(closeTimeout):
		slog.Warn("clickhouse trace store close timeout: drain incomplete")
	}
	if s.dlq != nil {
		if err := s.dlq.Close(); err != nil {
			slog.Warn("clickhouse trace DLQ close error", "err", err)
		}
	}
	return nil
}

// batchWriter는 채널에서 span을 읽어 배치를 조립하고 flushCh로 전달한다.
// 실제 ClickHouse I/O는 flushWorker goroutine 풀이 담당한다.
// batchWriter 종료 시 flushCh를 닫아 worker들이 drain 후 종료하도록 신호한다.
func (s *ClickHouseTraceStore) batchWriter() {
	defer close(s.flushCh) // flushWorker들에게 종료 신호
	defer func() {
		if r := recover(); r != nil {
			slog.Error("clickhouse trace batchWriter panic recovered", "panic", r)
		}
	}()

	dynCfg := s.loadDynCfg()
	ticker := time.NewTicker(dynCfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.SpanData, 0, dynCfg.BatchSize)

	doFlush := func(b []*model.SpanData) {
		if len(b) == 0 {
			return
		}
		chFlushQueueDepth.WithLabelValues("spans").Set(float64(len(s.flushCh) + 1))
		s.flushCh <- b
	}

	for {
		select {
		case sp, ok := <-s.ch:
			if !ok {
				doFlush(batch)
				return
			}
			batch = append(batch, sp)
			dc := s.loadDynCfg()
			if len(batch) >= dc.BatchSize {
				doFlush(batch)
				batch = make([]*model.SpanData, 0, dc.BatchSize)
			}

		case <-ticker.C:
			dc := s.loadDynCfg()
			// FlushInterval이 변경됐으면 ticker 리셋
			if dc.FlushInterval != dynCfg.FlushInterval {
				ticker.Reset(dc.FlushInterval)
				dynCfg = dc
			}
			if len(batch) > 0 {
				doFlush(batch)
				batch = make([]*model.SpanData, 0, dc.BatchSize)
			}
		}
	}
}

// flushWorker는 flushCh에서 배치를 수신해 Circuit Breaker → retry → DLQ 순으로 처리한다.
// FlushWorkers개의 goroutine이 병렬로 실행되어 고부하 쓰기 병목을 해소한다.
func (s *ClickHouseTraceStore) flushWorker() {
	for data := range s.flushCh {
		chFlushQueueDepth.WithLabelValues("spans").Set(float64(len(s.flushCh)))

		// Circuit breaker: Open 상태이면 DLQ로 직행해 ClickHouse 과부하 방지
		if s.cb != nil && !s.cb.Allow() {
			slog.Warn("clickhouse span flush blocked by circuit breaker — routing to DLQ", "count", len(data))
			chFlushErrorsTotal.WithLabelValues("spans").Inc()
			if s.dlq != nil {
				if dlqErr := s.dlq.WriteDLQSpans(data, "circuit breaker open"); dlqErr != nil {
					slog.Error("clickhouse span DLQ write failed — data lost", "err", dlqErr, "count", len(data))
				} else {
					chDLQWrittenTotal.WithLabelValues("spans").Add(float64(len(data)))
				}
			}
			continue
		}

		if err := retryFlush("spans", func() error { return s.flushSpans(data) }); err != nil {
			chFlushErrorsTotal.WithLabelValues("spans").Inc()
			if s.cb != nil {
				s.cb.RecordFailure()
			}
			slog.Error("clickhouse span flush failed (all retries exhausted)", "err", err, "count", len(data))
			if s.dlq != nil {
				if dlqErr := s.dlq.WriteDLQSpans(data, err.Error()); dlqErr != nil {
					slog.Error("clickhouse span DLQ write failed — data lost", "err", dlqErr, "count", len(data))
				} else {
					chDLQWrittenTotal.WithLabelValues("spans").Add(float64(len(data)))
					slog.Warn("clickhouse span flush failed; batch saved to DLQ", "count", len(data), "dlq_dir", s.cfg.DLQDir)
				}
			}
		} else if s.cb != nil {
			s.cb.RecordSuccess()
		}
	}
}

func (s *ClickHouseTraceStore) flushSpans(spans []*model.SpanData) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := s.conn.PrepareBatch(ctx,
		fmt.Sprintf(`INSERT INTO %s.spans
		(trace_id, span_id, parent_span_id, name, kind,
		 start_time_nano, end_time_nano, duration_nano,
		 attributes, status_code, status_message,
		 service_name, scope_name, received_at_ms,
		 http_method, http_route, http_status_code,
		 db_system, db_name, db_operation,
		 rpc_system, rpc_service, rpc_method,
		 peer_service, exception_type, exception_message, exception_stacktrace,
		 trace_state, span_links)`, s.cfg.Database),
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, sp := range spans {
		attrs := sp.Attributes
		if attrs == nil {
			attrs = map[string]any{}
		}
		if err := batch.Append(
			sp.TraceID, sp.SpanID, sp.ParentSpanID, sp.Name, sp.Kind,
			sp.StartTimeNano, sp.EndTimeNano, sp.DurationNano(),
			toStringMap(attrs), sp.StatusCode, sp.StatusMessage,
			sp.ServiceName, sp.ScopeName, sp.ReceivedAtMs,
			strAttr(attrs, "http.request.method"),
			strAttr(attrs, "http.route"),
			uint16Attr(attrs, "http.response.status_code"),
			strAttr(attrs, "db.system"),
			strAttr(attrs, "db.name"),
			strAttr(attrs, "db.operation"),
			strAttr(attrs, "rpc.system"),
			strAttr(attrs, "rpc.service"),
			strAttr(attrs, "rpc.method"),
			strAttr(attrs, "peer.service"),
			strAttr(attrs, "exception.type"),
			strAttr(attrs, "exception.message"),
			strAttr(attrs, "exception.stacktrace"),
			sp.TraceState,
			encodeSpanLinks(sp.Links),
		); err != nil {
			return fmt.Errorf("batch append span: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("batch send spans: %w", err)
	}

	elapsed := time.Since(start).Seconds()
	chFlushDuration.WithLabelValues("spans").Observe(elapsed)
	chFlushRowsTotal.WithLabelValues("spans").Add(float64(len(spans)))
	slog.Debug("clickhouse spans flushed", "count", len(spans), "elapsed_ms", elapsed*1000)
	return nil
}

// ---- ClickHouseMetricStore ----

// ---- Infra Metrics Correlation ----

// QueryInfraCorrelation은 서비스의 k8s 배포 컨텍스트와 JVM/인프라 메트릭을 상관 분석한다.
//
// 동작 방식:
//  1. spans 테이블에서 서비스의 k8s.node.name / k8s.pod.name / host.name 추출
//  2. metrics 테이블에서 동일 pod/host의 jvm.*/process.* 메트릭 조회
//
// 반환:
//   - k8s_context: 서비스가 실행 중인 pod/node/namespace 목록
//   - jvm_metrics: 해당 인스턴스들의 JVM 주요 메트릭 시계열
//   - infra_metrics: process.cpu, host.memory 등 인프라 지표
func (s *ClickHouseTraceStore) QueryInfraCorrelation(ctx context.Context, service string, fromMs, toMs int64) (map[string]any, error) {
	// Step 1: spans 테이블에서 k8s 컨텍스트 추출
	k8sRows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT
    attributes['k8s.pod.name']       AS pod_name,
    attributes['k8s.node.name']      AS node_name,
    attributes['k8s.namespace.name'] AS namespace,
    attributes['host.name']          AS host_name,
    count()                          AS span_count
FROM %s.spans
WHERE service_name = ?
  AND start_time_nano BETWEEN ? AND ?
  AND (notEmpty(attributes['k8s.pod.name']) OR notEmpty(attributes['host.name']))
GROUP BY pod_name, node_name, namespace, host_name
ORDER BY span_count DESC
LIMIT 20`, s.cfg.Database),
		service,
		fromMs*1_000_000,
		toMs*1_000_000,
	)
	if err != nil {
		return nil, fmt.Errorf("infra correlation k8s query: %w", err)
	}
	defer k8sRows.Close()

	type k8sContext struct {
		PodName   string `json:"pod_name"`
		NodeName  string `json:"node_name"`
		Namespace string `json:"namespace"`
		HostName  string `json:"host_name"`
		SpanCount uint64 `json:"span_count"`
	}
	var k8sContexts []k8sContext
	var hostNames []string
	hostSet := make(map[string]bool)

	for k8sRows.Next() {
		var kc k8sContext
		if err := k8sRows.Scan(&kc.PodName, &kc.NodeName, &kc.Namespace, &kc.HostName, &kc.SpanCount); err != nil {
			return nil, err
		}
		k8sContexts = append(k8sContexts, kc)
		// host.name 기반으로 메트릭 조회 (pod.name과 host.name 둘 다 수집)
		for _, h := range []string{kc.PodName, kc.HostName} {
			if h != "" && !hostSet[h] {
				hostSet[h] = true
				hostNames = append(hostNames, h)
			}
		}
	}
	if err := k8sRows.Err(); err != nil {
		return nil, err
	}

	if len(k8sContexts) == 0 {
		return map[string]any{
			"service_name": service,
			"k8s_context":  []any{},
			"jvm_metrics":  []any{},
			"message":      "no k8s/host context found in spans — ensure Java agent has k8s resource attributes",
		}, nil
	}

	// Step 2: metrics 테이블에서 JVM / 인프라 메트릭 조회
	// attributes['host.name'] 또는 attributes['service.instance.id']로 매칭
	// jvm.*, process.*, system.* 메트릭만 대상으로 한다
	metricRows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT
    name,
    toStartOfMinute(fromUnixTimestamp64Nano(timestamp_nano)) AS minute,
    avg(value) AS avg_val,
    max(value) AS max_val,
    min(value) AS min_val
FROM %s.metrics
WHERE service_name = ?
  AND timestamp_nano BETWEEN ? AND ?
  AND (
      name LIKE 'jvm.%%'
      OR name LIKE 'process.%%'
      OR name LIKE 'system.%%'
      OR name IN ('runtime.jvm.gc.duration', 'runtime.jvm.memory.used',
                  'runtime.jvm.memory.limit', 'runtime.jvm.thread.count')
  )
GROUP BY name, minute
ORDER BY name, minute`, s.cfg.Database),
		service,
		fromMs*1_000_000,
		toMs*1_000_000,
	)
	if err != nil {
		return nil, fmt.Errorf("infra correlation metrics query: %w", err)
	}
	defer metricRows.Close()

	type metricPoint struct {
		Name   string  `json:"name"`
		Minute string  `json:"minute"`
		Avg    float64 `json:"avg"`
		Max    float64 `json:"max"`
		Min    float64 `json:"min"`
	}
	var jvmMetrics []metricPoint
	for metricRows.Next() {
		var mp metricPoint
		var minute time.Time
		if err := metricRows.Scan(&mp.Name, &minute, &mp.Avg, &mp.Max, &mp.Min); err != nil {
			return nil, err
		}
		mp.Minute = minute.UTC().Format(time.RFC3339)
		jvmMetrics = append(jvmMetrics, mp)
	}
	if err := metricRows.Err(); err != nil {
		return nil, err
	}

	return map[string]any{
		"service_name": service,
		"k8s_context":  k8sContexts,
		"jvm_metrics":  jvmMetrics,
		"time_range": map[string]int64{
			"from_ms": fromMs,
			"to_ms":   toMs,
		},
	}, nil
}

// ---- DDL ----
