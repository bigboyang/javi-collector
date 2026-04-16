// Package store - ClickHouse 배치 저장소 구현체.
//
// 파이프라인:
//
//	AppendSpans/Metrics/Logs
//	  → 채널에 enqueue (backpressure: ResourceExhausted 반환)
//	  → batchWriter goroutine: size-trigger 또는 time-trigger 로 flushCh에 배치 전송
//	  → N개 flushWorker goroutine: flushCh에서 배치를 수신해 ClickHouse에 병렬 insert
//
// 상용 APM best-practice 적용:
//   - 공유 커넥션 풀: OpenConn으로 1개 pool 생성 후 3개 store가 공유
//   - Flush worker pool: FlushWorkers개 goroutine이 배치를 병렬 처리 (병목 해소)
//   - Retry with backoff: flush 실패 시 최대 3회 재시도 (1s→2s→4s)
//   - Drop counter: backpressure로 드롭된 항목 Prometheus 계측
//   - Panic recovery: batchWriter goroutine 비정상 종료 방지
//   - Drain timeout: Close() 30초 이내 강제 종료
//   - Sequential histogram flush: 커넥션을 동시에 2개 점유하지 않도록 순차 처리
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/kkc/javi-collector/internal/model"
)

// ---- 상수 ----

const (
	// maxFlushRetries: flush 실패 시 최대 재시도 횟수
	maxFlushRetries = 3
	// retryBaseDelay: 첫 번째 재시도 대기 시간 (지수 backoff: 1s, 2s, 4s)
	retryBaseDelay = time.Second
	// closeTimeout: Close() 시 batchWriter drain 대기 최대 시간
	closeTimeout = 30 * time.Second
)

// ---- Prometheus 지표 ----

var (
	chFlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "flush_duration_seconds",
		Help:      "Duration of ClickHouse batch flush operations.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
	}, []string{"table"})

	chFlushRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "flush_rows_total",
		Help:      "Total number of rows flushed to ClickHouse.",
	}, []string{"table"})

	chFlushErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "flush_errors_total",
		Help:      "Total number of ClickHouse flush errors (after all retries).",
	}, []string{"table"})

	chChannelDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "channel_depth",
		Help:      "Current depth of the ClickHouse write channel.",
	}, []string{"table"})

	// chDroppedTotal: backpressure로 인해 드롭된 항목 수.
	// rate()로 드롭율을 계산해 알람 임계값으로 활용한다.
	chDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "dropped_total",
		Help:      "Total number of items dropped due to channel backpressure.",
	}, []string{"table"})

	// chFlushRetriesTotal: flush 재시도 횟수 (성공 여부 무관).
	// 높은 retry rate는 ClickHouse 부하 또는 네트워크 불안정을 시사한다.
	chFlushRetriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "flush_retries_total",
		Help:      "Total number of flush retry attempts.",
	}, []string{"table"})

	// chDLQWrittenTotal: flush 실패 후 DLQ 파일에 보존된 항목 수.
	// DLQ 파일은 ClickHouse 복구 후 수동 재적재(replay)에 사용된다.
	chDLQWrittenTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "dlq_written_total",
		Help:      "Total number of items written to DLQ after flush failure.",
	}, []string{"table"})

	// chFlushWorkerPoolSize: 테이블별 flush worker goroutine 수.
	// FlushWorkers 설정값을 반영하며 운영 중에는 고정된다.
	chFlushWorkerPoolSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "flush_worker_pool_size",
		Help:      "Number of flush worker goroutines configured per table.",
	}, []string{"table"})

	// chFlushQueueDepth: flushCh에 대기 중인 배치 수.
	// 높은 값은 flush worker가 포화 상태임을 시사한다.
	chFlushQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "flush_queue_depth",
		Help:      "Number of batches waiting in the flush queue (flushCh).",
	}, []string{"table"})
)

// ClickHouseConfig는 ClickHouse 연결 및 배치 설정이다.
type ClickHouseConfig struct {
	Addr          string
	Database      string
	Username      string
	Password      string
	BatchSize     int
	FlushInterval time.Duration
	ChanBuffer    int
	RetentionDays int // 데이터 보관 기간 (일), TTL로 적용

	// DLQDir: flush 실패 배치를 보존할 Dead Letter Queue 디렉터리.
	// 비어 있으면 DLQ 비활성화 (데이터 유실 허용).
	// DLQ 파일(traces/metrics/logs-YYYY-MM-DD.jsonl)은 ClickHouse 복구 후 자동/수동 재적재 가능.
	DLQDir string

	// Circuit Breaker 설정: 연속 실패 시 flush를 일시 차단해 ClickHouse 과부하 방지.
	// CBFailureThreshold=0 이면 비활성화 (기본 동작 유지).
	CBFailureThreshold int           // Open으로 전환하는 연속 실패 횟수 (기본 5)
	CBCooldown         time.Duration // Open → HalfOpen 전환 대기 시간 (기본 60s)

	// FlushWorkers: 테이블별 flush worker goroutine 수.
	// 1이면 단일 직렬 flush, N이면 N개 goroutine이 배치를 병렬로 처리한다.
	// 0 또는 미설정 시 기본값 1이 적용된다.
	// 고부하 환경: 4–8 권장. ClickHouse MaxOpenConns(10)보다 작게 유지할 것.
	FlushWorkers int
}

// OpenConn은 ClickHouse native protocol 공유 연결 풀을 연다.
//
// 상용 APM 패턴: 하나의 커넥션 풀을 TraceStore, MetricStore, LogStore가 공유한다.
// 세 Store가 각각 openConn을 호출하면 최대 3×MaxOpenConns 커넥션이 생성된다.
// 공유 풀은 커넥션 수를 1/3로 줄이고 pool exhaustion 가능성을 낮춘다.
func OpenConn(cfg ClickHouseConfig) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"async_insert":          0,
			"max_insert_block_size": 1_000_000,
		},
		MaxOpenConns:    10,
		MaxIdleConns:    3,
		ConnMaxLifetime: time.Hour,
		DialTimeout:     10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return conn, nil
}

// retryFlush는 fn을 최대 maxFlushRetries 회 실행한다.
// 각 실패 후 지수 backoff(1s, 2s, 4s)로 대기한다.
// 모든 재시도 후에도 실패하면 마지막 에러를 반환한다.
func retryFlush(table string, fn func() error) error {
	var err error
	for attempt := 0; attempt < maxFlushRetries; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		chFlushRetriesTotal.WithLabelValues(table).Inc()
		delay := retryBaseDelay << attempt // 1s, 2s, 4s
		slog.Warn("clickhouse flush retry",
			"table", table,
			"attempt", attempt+1,
			"max", maxFlushRetries,
			"delay", delay,
			"err", err,
		)
		time.Sleep(delay)
	}
	return err
}

// ---- 동적 설정 ----

// storeDynCfg는 핫 리로드로 변경 가능한 배치 설정이다.
// SetDynamicConfig가 atomic.Pointer로 교체한다.
type storeDynCfg struct {
	BatchSize     int
	FlushInterval time.Duration
}

// DynamicConfigSetter는 런타임 배치 설정을 변경할 수 있는 인터페이스다.
// 핫 리로드 콜백에서 사용한다.
type DynamicConfigSetter interface {
	SetDynamicConfig(batchSize int, flushInterval time.Duration)
}

// ---- ClickHouseTraceStore ----

// ClickHouseTraceStore는 SpanData를 apm.spans 테이블에 배치 insert한다.
type ClickHouseTraceStore struct {
	conn    driver.Conn            // 공유 커넥션 풀 (소유권 없음 — Close()에서 닫지 않는다)
	ch      chan *model.SpanData   // 수신 데이터 채널
	flushCh chan []*model.SpanData // 조립된 배치를 flush worker에 전달하는 큐
	cfg     ClickHouseConfig
	dynCfg  atomic.Pointer[storeDynCfg] // 핫 리로드 가능한 배치 설정 (nil이면 cfg 사용)
	done    chan struct{}                // 모든 flushWorker 종료 시 닫힘
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
			attrsStr     string
			durationNano int64
		)
		if err := rows.Scan(
			&sp.TraceID, &sp.SpanID, &sp.ParentSpanID, &sp.Name, &sp.Kind,
			&sp.StartTimeNano, &sp.EndTimeNano, &durationNano,
			&attrsStr, &sp.StatusCode, &sp.StatusMessage,
			&sp.ServiceName, &sp.ScopeName, &sp.ReceivedAtMs,
		); err != nil {
			return nil, err
		}
		sp.Attributes = fromJSONString(attrsStr)
		result = append(result, &sp)
	}
	return result, rows.Err()
}

// QueryRED는 mv_red_1m_state에서 서비스별 RED 메트릭을 반환한다.
func (s *ClickHouseTraceStore) QueryRED(ctx context.Context, service string, fromMs, toMs int64) ([]map[string]any, error) {
	var conds []string
	var args []any

	if fromMs > 0 {
		conds = append(conds, "minute >= ?")
		args = append(args, fromMs/1000)
	}
	if toMs > 0 {
		conds = append(conds, "minute <= ?")
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
    minute,
    sum(total_count)                                                   AS rps,
    sum(error_count)                                                   AS errors,
    sum(error_count) / sum(total_count) * 100                         AS error_rate_pct,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[1] / 1e6    AS p50_ms,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[2] / 1e6    AS p95_ms,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[3] / 1e6    AS p99_ms
FROM %s.mv_red_1m_state
%s
GROUP BY service_name, span_name, http_route, minute
ORDER BY minute DESC
LIMIT 1000`, s.cfg.Database, where)

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
// traceID 기준으로 집계: parent_span_id=''인 span이 없는 trace = 브로큰 트레이스.
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
			traceID, spanID, serviceName string
			dbSystem, dbName, dbOp, dbStmt string
			durationMs                   float64
			startTime                    time.Time
			statusCode                   int32
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
			"span_id":          spanID,
			"parent_span_id":   parentSpanID,
			"service_name":     svc,
			"span_name":        spanName,
			"start_ms":         startMs,
			"duration_ms":      durationMs,
			"status_code":      statusCode,
			"exception_type":   excType,
			"exception_message": excMsg,
			"http_status_code": httpStatus,
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
			toJSONString(toStringMap(attrs)), sp.StatusCode, sp.StatusMessage,
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

// ClickHouseMetricStore는 MetricData를 apm.metrics 테이블에 배치 insert한다.
type ClickHouseMetricStore struct {
	conn    driver.Conn              // 공유 커넥션 풀 (소유권 없음)
	ch      chan *model.MetricData   // 수신 데이터 채널
	flushCh chan []*model.MetricData // 조립된 배치를 flush worker에 전달하는 큐
	cfg     ClickHouseConfig
	dynCfg  atomic.Pointer[storeDynCfg] // 핫 리로드 가능한 배치 설정
	done    chan struct{}                // 모든 flushWorker 종료 시 닫힘
	dlq     *FileBackupWriter           // flush 실패 시 배치 보존 (nil이면 비활성화)
	cb      *circuitBreaker             // 연속 실패 시 flush 차단 (nil이면 비활성화)
}

// SetDynamicConfig는 metric store의 배치 설정을 런타임에 변경한다.
func (s *ClickHouseMetricStore) SetDynamicConfig(batchSize int, flushInterval time.Duration) {
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
	slog.Info("metric store dynamic config updated",
		"batch_size", next.BatchSize,
		"flush_interval", next.FlushInterval,
	)
}

func (s *ClickHouseMetricStore) loadDynCfg() storeDynCfg {
	if p := s.dynCfg.Load(); p != nil {
		return *p
	}
	return storeDynCfg{BatchSize: s.cfg.BatchSize, FlushInterval: s.cfg.FlushInterval}
}

func NewClickHouseMetricStore(conn driver.Conn, cfg ClickHouseConfig) (*ClickHouseMetricStore, error) {
	if err := ensureMetricsTable(conn, cfg.Database, cfg.RetentionDays); err != nil {
		return nil, fmt.Errorf("clickhouse metric DDL: %w", err)
	}

	var dlq *FileBackupWriter
	if cfg.DLQDir != "" {
		var err error
		dlq, err = NewFileBackupWriter(cfg.DLQDir)
		if err != nil {
			return nil, fmt.Errorf("clickhouse metric DLQ init: %w", err)
		}
	}

	var cbM *circuitBreaker
	if cfg.CBFailureThreshold > 0 {
		cbM = newCircuitBreaker("metrics", cfg.CBFailureThreshold, cfg.CBCooldown)
	}

	workers := cfg.FlushWorkers
	if workers < 1 {
		workers = 1
	}

	s := &ClickHouseMetricStore{
		conn:    conn,
		ch:      make(chan *model.MetricData, cfg.ChanBuffer),
		flushCh: make(chan []*model.MetricData, workers*2),
		cfg:     cfg,
		done:    make(chan struct{}),
		dlq:     dlq,
		cb:      cbM,
	}

	chFlushWorkerPoolSize.WithLabelValues("metrics").Set(float64(workers))

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

func (s *ClickHouseMetricStore) AppendMetrics(_ context.Context, metrics []*model.MetricData) error {
	inserted := 0
	for _, m := range metrics {
		select {
		case s.ch <- m:
			inserted++
		default:
			chDroppedTotal.WithLabelValues("metrics").Inc()
		}
	}
	chChannelDepth.WithLabelValues("metrics").Set(float64(len(s.ch)))
	if inserted == 0 && len(metrics) > 0 {
		return fmt.Errorf("metric channel full (capacity=%d): backpressure", cap(s.ch))
	}
	return nil
}

func (s *ClickHouseMetricStore) QueryMetrics(ctx context.Context, q MetricQuery) ([]*model.MetricData, error) {
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
	if q.Name != "" {
		conds = append(conds, "name = ?")
		args = append(args, q.Name)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	// metrics 테이블 (GAUGE / SUM / SUMMARY)
	sql := fmt.Sprintf(
		`SELECT name, type, value, attributes, service_name, timestamp_nano, received_at_ms
		 FROM %s.metrics
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

	var result []*model.MetricData
	for rows.Next() {
		var (
			name, mtype, serviceName string
			attrsStr                 string
			value                    float64
			timestampNano, receivedAtMs int64
		)
		if err := rows.Scan(&name, &mtype, &value, &attrsStr, &serviceName, &timestampNano, &receivedAtMs); err != nil {
			return nil, err
		}
		result = append(result, &model.MetricData{
			Name:         name,
			Type:         model.MetricType(mtype),
			ServiceName:  serviceName,
			ReceivedAtMs: receivedAtMs,
			DataPoints: []model.DataPoint{
				{Attributes: fromJSONString(attrsStr), TimeNanos: timestampNano, Value: value},
			},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// metric_histograms 테이블 (HISTOGRAM)
	// metric_name 컬럼으로 필터한다 (metrics 테이블의 name과 동일 역할).
	var histConds []string
	var histArgs []any
	if q.FromMs > 0 {
		histConds = append(histConds, "received_at_ms >= ?")
		histArgs = append(histArgs, q.FromMs)
	}
	if q.ToMs > 0 {
		histConds = append(histConds, "received_at_ms <= ?")
		histArgs = append(histArgs, q.ToMs)
	}
	if q.ServiceName != "" {
		histConds = append(histConds, "service_name = ?")
		histArgs = append(histArgs, q.ServiceName)
	}
	if q.Name != "" {
		histConds = append(histConds, "metric_name = ?")
		histArgs = append(histArgs, q.Name)
	}
	histWhere := ""
	if len(histConds) > 0 {
		histWhere = "WHERE " + strings.Join(histConds, " AND ")
	}

	histSQL := fmt.Sprintf(
		`SELECT metric_name, bounds, bucket_counts, total_count, total_sum, attributes, service_name, timestamp_nano, received_at_ms
		 FROM %s.metric_histograms
		 %s
		 ORDER BY received_at_ms DESC
		 LIMIT %d`,
		s.cfg.Database, histWhere, q.Limit,
	)

	hrows, err := s.conn.Query(ctx, histSQL, histArgs...)
	if err != nil {
		return nil, err
	}
	defer hrows.Close()

	for hrows.Next() {
		var (
			metricName, serviceName string
			bounds                  []float64
			bucketCounts            []uint64
			totalCount              uint64
			totalSum                float64
			attrsStr                string
			timestampNano, receivedAtMs int64
		)
		if err := hrows.Scan(&metricName, &bounds, &bucketCounts, &totalCount, &totalSum, &attrsStr, &serviceName, &timestampNano, &receivedAtMs); err != nil {
			return nil, err
		}
		result = append(result, &model.MetricData{
			Name:         metricName,
			Type:         model.MetricTypeHistogram,
			ServiceName:  serviceName,
			ReceivedAtMs: receivedAtMs,
			DataPoints: []model.DataPoint{
				{
					Attributes:     fromJSONString(attrsStr),
					TimeNanos:      timestampNano,
					Count:          int64(totalCount),
					Sum:            totalSum,
					BucketCounts:   bucketCounts,
					ExplicitBounds: bounds,
				},
			},
		})
	}
	return result, hrows.Err()
}

// QueryHistogramMV는 mv_histogram_1m_state에서 1분 집계 히스토그램 메트릭을 반환한다.
// GET /api/collector/histogram?service=svc&name=http.server.duration&from=<ms>&to=<ms>&limit=100
func (s *ClickHouseMetricStore) QueryHistogramMV(ctx context.Context, service, name string, fromMs, toMs int64, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	var conds []string
	var args []any

	if fromMs > 0 {
		conds = append(conds, "minute >= ?")
		args = append(args, time.UnixMilli(fromMs).UTC())
	}
	if toMs > 0 {
		conds = append(conds, "minute <= ?")
		args = append(args, time.UnixMilli(toMs).UTC())
	}
	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}
	if name != "" {
		conds = append(conds, "metric_name = ?")
		args = append(args, name)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	q := fmt.Sprintf(`
SELECT service_name, metric_name, minute,
       sumMerge(count_state) AS count,
       sumMerge(sum_state)   AS sum
FROM %s.mv_histogram_1m_state
%s
GROUP BY service_name, metric_name, minute
ORDER BY minute DESC
LIMIT %d`, s.cfg.Database, where, limit)

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			serviceName, metricName string
			minute                  time.Time
			count                   uint64
			sum                     float64
		)
		if err := rows.Scan(&serviceName, &metricName, &minute, &count, &sum); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"service_name": serviceName,
			"metric_name":  metricName,
			"minute":       minute.UnixMilli(),
			"count":        count,
			"sum":          sum,
			"avg":          func() float64 {
				if count == 0 {
					return 0
				}
				return sum / float64(count)
			}(),
		})
	}
	return result, rows.Err()
}

func (s *ClickHouseMetricStore) Close() error {
	close(s.ch)
	select {
	case <-s.done:
	case <-time.After(closeTimeout):
		slog.Warn("clickhouse metric store close timeout: drain incomplete")
	}
	if s.dlq != nil {
		if err := s.dlq.Close(); err != nil {
			slog.Warn("clickhouse metric DLQ close error", "err", err)
		}
	}
	return nil
}

func (s *ClickHouseMetricStore) batchWriter() {
	defer close(s.flushCh)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("clickhouse metric batchWriter panic recovered", "panic", r)
		}
	}()

	dynCfg := s.loadDynCfg()
	ticker := time.NewTicker(dynCfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.MetricData, 0, dynCfg.BatchSize)

	doFlush := func(b []*model.MetricData) {
		if len(b) == 0 {
			return
		}
		chFlushQueueDepth.WithLabelValues("metrics").Set(float64(len(s.flushCh) + 1))
		s.flushCh <- b
	}

	for {
		select {
		case m, ok := <-s.ch:
			if !ok {
				doFlush(batch)
				return
			}
			batch = append(batch, m)
			dc := s.loadDynCfg()
			if len(batch) >= dc.BatchSize {
				doFlush(batch)
				batch = make([]*model.MetricData, 0, dc.BatchSize)
			}
		case <-ticker.C:
			dc := s.loadDynCfg()
			if dc.FlushInterval != dynCfg.FlushInterval {
				ticker.Reset(dc.FlushInterval)
				dynCfg = dc
			}
			if len(batch) > 0 {
				doFlush(batch)
				batch = make([]*model.MetricData, 0, s.cfg.BatchSize)
			}
		}
	}
}

func (s *ClickHouseMetricStore) flushWorker() {
	for data := range s.flushCh {
		chFlushQueueDepth.WithLabelValues("metrics").Set(float64(len(s.flushCh)))

		if s.cb != nil && !s.cb.Allow() {
			slog.Warn("clickhouse metric flush blocked by circuit breaker — routing to DLQ", "count", len(data))
			chFlushErrorsTotal.WithLabelValues("metrics").Inc()
			if s.dlq != nil {
				if dlqErr := s.dlq.WriteDLQMetrics(data, "circuit breaker open"); dlqErr != nil {
					slog.Error("clickhouse metric DLQ write failed — data lost", "err", dlqErr, "count", len(data))
				} else {
					chDLQWrittenTotal.WithLabelValues("metrics").Add(float64(len(data)))
				}
			}
			continue
		}

		if err := retryFlush("metrics", func() error { return s.flushMetrics(data) }); err != nil {
			chFlushErrorsTotal.WithLabelValues("metrics").Inc()
			if s.cb != nil {
				s.cb.RecordFailure()
			}
			slog.Error("clickhouse metric flush failed (all retries exhausted)", "err", err, "count", len(data))
			if s.dlq != nil {
				if dlqErr := s.dlq.WriteDLQMetrics(data, err.Error()); dlqErr != nil {
					slog.Error("clickhouse metric DLQ write failed — data lost", "err", dlqErr, "count", len(data))
				} else {
					chDLQWrittenTotal.WithLabelValues("metrics").Add(float64(len(data)))
					slog.Warn("clickhouse metric flush failed; batch saved to DLQ", "count", len(data), "dlq_dir", s.cfg.DLQDir)
				}
			}
		} else if s.cb != nil {
			s.cb.RecordSuccess()
		}
	}
}

// flushMetrics는 scalar(Gauge/Sum)과 Histogram을 순차 처리한다.
//
// 이전 구현은 두 배치를 동시에 PrepareBatch해 커넥션 풀에서 2개를 동시 점유했다.
// 순차 처리로 커넥션을 하나씩만 점유 → pool exhaustion 위험 제거.
func (s *ClickHouseMetricStore) flushMetrics(metrics []*model.MetricData) error {
	start := time.Now()

	scalarRows, err := s.flushScalarMetrics(metrics)
	if err != nil {
		return err
	}

	histRows, err := s.flushHistogramMetrics(metrics)
	if err != nil {
		return err
	}

	elapsed := time.Since(start).Seconds()
	totalRows := scalarRows + histRows
	if totalRows > 0 {
		chFlushDuration.WithLabelValues("metrics").Observe(elapsed)
		chFlushRowsTotal.WithLabelValues("metrics").Add(float64(totalRows))
		slog.Debug("clickhouse metrics flushed",
			"scalar_rows", scalarRows,
			"hist_rows", histRows,
			"elapsed_ms", elapsed*1000,
		)
	}
	return nil
}

// flushScalarMetrics는 Gauge/Sum/Histogram(평균용) 데이터를 metrics 테이블에 insert한다.
// 커넥션을 단독 점유 후 Send() 완료 시 반납한다.
func (s *ClickHouseMetricStore) flushScalarMetrics(metrics []*model.MetricData) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := s.conn.PrepareBatch(ctx,
		fmt.Sprintf(`INSERT INTO %s.metrics
		(name, type, value, attributes, service_name, timestamp_nano, received_at_ms)`, s.cfg.Database),
	)
	if err != nil {
		return 0, fmt.Errorf("prepare scalar batch: %w", err)
	}

	rowCount := 0
	for _, m := range metrics {
		for _, dp := range m.DataPoints {
			var value float64
			if m.Type == model.MetricTypeHistogram {
				value = dp.Sum
			} else {
				value = dp.Value
			}
			if err := batch.Append(
				m.Name, string(m.Type), value,
				toJSONString(toStringMap(dp.Attributes)), m.ServiceName,
				dp.TimeNanos, m.ReceivedAtMs,
			); err != nil {
				return 0, fmt.Errorf("batch append scalar metric: %w", err)
			}
			rowCount++
		}
	}

	if rowCount == 0 {
		// 빈 배치는 Send 없이 반환 (커넥션 즉시 반납)
		return 0, nil
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("batch send scalar metrics: %w", err)
	}
	return rowCount, nil
}

// flushHistogramMetrics는 Histogram BucketCounts를 metric_histograms 테이블에 insert한다.
// flushScalarMetrics 완료 후 호출되어 커넥션을 순차 점유한다.
func (s *ClickHouseMetricStore) flushHistogramMetrics(metrics []*model.MetricData) (int, error) {
	// Histogram이 없으면 커넥션 획득 없이 즉시 반환
	hasHistogram := false
	for _, m := range metrics {
		if m.Type == model.MetricTypeHistogram {
			hasHistogram = true
			break
		}
	}
	if !hasHistogram {
		return 0, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := s.conn.PrepareBatch(ctx,
		fmt.Sprintf(`INSERT INTO %s.metric_histograms
		(service_name, metric_name, timestamp_nano, bounds, bucket_counts,
		 total_count, total_sum, attributes, received_at_ms)`, s.cfg.Database),
	)
	if err != nil {
		return 0, fmt.Errorf("prepare histogram batch: %w", err)
	}

	rowCount := 0
	for _, m := range metrics {
		if m.Type != model.MetricTypeHistogram {
			continue
		}
		for _, dp := range m.DataPoints {
			buckets := make([]uint64, len(dp.BucketCounts))
			copy(buckets, dp.BucketCounts)
			if err := batch.Append(
				m.ServiceName, m.Name, dp.TimeNanos,
				dp.ExplicitBounds, buckets,
				uint64(dp.Count), dp.Sum,
				toJSONString(toStringMap(dp.Attributes)), m.ReceivedAtMs,
			); err != nil {
				return 0, fmt.Errorf("batch append histogram: %w", err)
			}
			rowCount++
		}
	}

	if rowCount == 0 {
		return 0, nil
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("batch send histograms: %w", err)
	}
	return rowCount, nil
}

// ---- ClickHouseLogStore ----

// ClickHouseLogStore는 LogData를 apm.logs 테이블에 배치 insert한다.
type ClickHouseLogStore struct {
	conn    driver.Conn            // 공유 커넥션 풀 (소유권 없음)
	ch      chan *model.LogData    // 수신 데이터 채널
	flushCh chan []*model.LogData  // 조립된 배치를 flush worker에 전달하는 큐
	cfg     ClickHouseConfig
	dynCfg  atomic.Pointer[storeDynCfg] // 핫 리로드 가능한 배치 설정
	done    chan struct{}                // 모든 flushWorker 종료 시 닫힘
	dlq     *FileBackupWriter           // flush 실패 시 배치 보존 (nil이면 비활성화)
	cb      *circuitBreaker             // 연속 실패 시 flush 차단 (nil이면 비활성화)
}

// SetDynamicConfig는 log store의 배치 설정을 런타임에 변경한다.
func (s *ClickHouseLogStore) SetDynamicConfig(batchSize int, flushInterval time.Duration) {
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
	slog.Info("log store dynamic config updated",
		"batch_size", next.BatchSize,
		"flush_interval", next.FlushInterval,
	)
}

func (s *ClickHouseLogStore) loadDynCfg() storeDynCfg {
	if p := s.dynCfg.Load(); p != nil {
		return *p
	}
	return storeDynCfg{BatchSize: s.cfg.BatchSize, FlushInterval: s.cfg.FlushInterval}
}

func NewClickHouseLogStore(conn driver.Conn, cfg ClickHouseConfig) (*ClickHouseLogStore, error) {
	if err := ensureLogsTable(conn, cfg.Database, cfg.RetentionDays); err != nil {
		return nil, fmt.Errorf("clickhouse log DDL: %w", err)
	}

	var dlq *FileBackupWriter
	if cfg.DLQDir != "" {
		var err error
		dlq, err = NewFileBackupWriter(cfg.DLQDir)
		if err != nil {
			return nil, fmt.Errorf("clickhouse log DLQ init: %w", err)
		}
	}

	var cbL *circuitBreaker
	if cfg.CBFailureThreshold > 0 {
		cbL = newCircuitBreaker("logs", cfg.CBFailureThreshold, cfg.CBCooldown)
	}

	workers := cfg.FlushWorkers
	if workers < 1 {
		workers = 1
	}

	s := &ClickHouseLogStore{
		conn:    conn,
		ch:      make(chan *model.LogData, cfg.ChanBuffer),
		flushCh: make(chan []*model.LogData, workers*2),
		cfg:     cfg,
		done:    make(chan struct{}),
		dlq:     dlq,
		cb:      cbL,
	}

	chFlushWorkerPoolSize.WithLabelValues("logs").Set(float64(workers))

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

func (s *ClickHouseLogStore) AppendLogs(_ context.Context, logs []*model.LogData) error {
	inserted := 0
	for _, l := range logs {
		select {
		case s.ch <- l:
			inserted++
		default:
			chDroppedTotal.WithLabelValues("logs").Inc()
		}
	}
	chChannelDepth.WithLabelValues("logs").Set(float64(len(s.ch)))
	if inserted == 0 && len(logs) > 0 {
		return fmt.Errorf("log channel full (capacity=%d): backpressure", cap(s.ch))
	}
	return nil
}

func (s *ClickHouseLogStore) QueryLogs(ctx context.Context, q LogQuery) ([]*model.LogData, error) {
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
	if q.SeverityText != "" {
		conds = append(conds, "severity_text = ?")
		args = append(args, q.SeverityText)
	}
	if q.TraceID != "" {
		conds = append(conds, "trace_id = ?")
		args = append(args, q.TraceID)
	}

	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}

	sql := fmt.Sprintf(
		`SELECT severity_text, severity_number, body, attributes,
		        service_name, trace_id, span_id, timestamp_nano, received_at_ms
		 FROM %s.logs
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

	var result []*model.LogData
	for rows.Next() {
		var (
			l        model.LogData
			attrsStr string
		)
		if err := rows.Scan(
			&l.SeverityText, &l.SeverityNumber, &l.Body, &attrsStr,
			&l.ServiceName, &l.TraceID, &l.SpanID,
			&l.TimestampNanos, &l.ReceivedAtMs,
		); err != nil {
			return nil, err
		}
		l.Attributes = fromJSONString(attrsStr)
		result = append(result, &l)
	}
	return result, rows.Err()
}

// QueryErrorLogs는 mv_error_logs_1m_state에서 서비스별 에러 집계를 반환한다.
func (s *ClickHouseLogStore) QueryErrorLogs(ctx context.Context, service string, fromMs, toMs int64) ([]map[string]any, error) {
	var conds []string
	var args []any

	if fromMs > 0 {
		conds = append(conds, "minute >= ?")
		args = append(args, fromMs/1000)
	}
	if toMs > 0 {
		conds = append(conds, "minute <= ?")
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
    exception_type,
    minute,
    sum(error_count) AS error_count
FROM %s.mv_error_logs_1m_state
%s
GROUP BY service_name, exception_type, minute
ORDER BY minute DESC, error_count DESC
LIMIT 500`, s.cfg.Database, where)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			serviceName, exceptionType string
			minute                     time.Time
			errorCount                 uint64
		)
		if err := rows.Scan(&serviceName, &exceptionType, &minute, &errorCount); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"service_name":   serviceName,
			"exception_type": exceptionType,
			"minute":         minute.UnixMilli(),
			"error_count":    errorCount,
		})
	}
	return result, rows.Err()
}

func (s *ClickHouseLogStore) Close() error {
	close(s.ch)
	select {
	case <-s.done:
	case <-time.After(closeTimeout):
		slog.Warn("clickhouse log store close timeout: drain incomplete")
	}
	if s.dlq != nil {
		if err := s.dlq.Close(); err != nil {
			slog.Warn("clickhouse log DLQ close error", "err", err)
		}
	}
	return nil
}

func (s *ClickHouseLogStore) batchWriter() {
	defer close(s.flushCh)
	defer func() {
		if r := recover(); r != nil {
			slog.Error("clickhouse log batchWriter panic recovered", "panic", r)
		}
	}()

	dynCfg := s.loadDynCfg()
	ticker := time.NewTicker(dynCfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.LogData, 0, dynCfg.BatchSize)

	doFlush := func(b []*model.LogData) {
		if len(b) == 0 {
			return
		}
		chFlushQueueDepth.WithLabelValues("logs").Set(float64(len(s.flushCh) + 1))
		s.flushCh <- b
	}

	for {
		select {
		case l, ok := <-s.ch:
			if !ok {
				doFlush(batch)
				return
			}
			batch = append(batch, l)
			dc := s.loadDynCfg()
			if len(batch) >= dc.BatchSize {
				doFlush(batch)
				batch = make([]*model.LogData, 0, dc.BatchSize)
			}
		case <-ticker.C:
			dc := s.loadDynCfg()
			if dc.FlushInterval != dynCfg.FlushInterval {
				ticker.Reset(dc.FlushInterval)
				dynCfg = dc
			}
			if len(batch) > 0 {
				doFlush(batch)
				batch = make([]*model.LogData, 0, dc.BatchSize)
			}
		}
	}
}

func (s *ClickHouseLogStore) flushWorker() {
	for data := range s.flushCh {
		chFlushQueueDepth.WithLabelValues("logs").Set(float64(len(s.flushCh)))

		if s.cb != nil && !s.cb.Allow() {
			slog.Warn("clickhouse log flush blocked by circuit breaker — routing to DLQ", "count", len(data))
			chFlushErrorsTotal.WithLabelValues("logs").Inc()
			if s.dlq != nil {
				if dlqErr := s.dlq.WriteDLQLogs(data, "circuit breaker open"); dlqErr != nil {
					slog.Error("clickhouse log DLQ write failed — data lost", "err", dlqErr, "count", len(data))
				} else {
					chDLQWrittenTotal.WithLabelValues("logs").Add(float64(len(data)))
				}
			}
			continue
		}

		if err := retryFlush("logs", func() error { return s.flushLogs(data) }); err != nil {
			chFlushErrorsTotal.WithLabelValues("logs").Inc()
			if s.cb != nil {
				s.cb.RecordFailure()
			}
			slog.Error("clickhouse log flush failed (all retries exhausted)", "err", err, "count", len(data))
			if s.dlq != nil {
				if dlqErr := s.dlq.WriteDLQLogs(data, err.Error()); dlqErr != nil {
					slog.Error("clickhouse log DLQ write failed — data lost", "err", dlqErr, "count", len(data))
				} else {
					chDLQWrittenTotal.WithLabelValues("logs").Add(float64(len(data)))
					slog.Warn("clickhouse log flush failed; batch saved to DLQ", "count", len(data), "dlq_dir", s.cfg.DLQDir)
				}
			}
		} else if s.cb != nil {
			s.cb.RecordSuccess()
		}
	}
}

func (s *ClickHouseLogStore) flushLogs(logs []*model.LogData) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := s.conn.PrepareBatch(ctx,
		fmt.Sprintf(`INSERT INTO %s.logs
		(severity_text, severity_number, body, attributes,
		 service_name, trace_id, span_id, timestamp_nano, received_at_ms,
		 exception_type, logger_name)`, s.cfg.Database),
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, l := range logs {
		attrs := l.Attributes
		if attrs == nil {
			attrs = map[string]any{}
		}
		if err := batch.Append(
			l.SeverityText, l.SeverityNumber, l.Body,
			toJSONString(toStringMap(attrs)), l.ServiceName,
			l.TraceID, l.SpanID, l.TimestampNanos, l.ReceivedAtMs,
			strAttr(attrs, "exception.type"),
			strAttr(attrs, "logger.name"),
		); err != nil {
			return fmt.Errorf("batch append log: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("batch send logs: %w", err)
	}

	elapsed := time.Since(start).Seconds()
	chFlushDuration.WithLabelValues("logs").Observe(elapsed)
	chFlushRowsTotal.WithLabelValues("logs").Add(float64(len(logs)))
	slog.Debug("clickhouse logs flushed", "count", len(logs), "elapsed_ms", elapsed*1000)
	return nil
}

// ---- OTel 속성 추출 헬퍼 ----

// encodeSpanLinks는 SpanLink 슬라이스를 JSON 문자열로 직렬화한다.
// ClickHouse의 String 컬럼에 저장하며, 빈 슬라이스는 빈 문자열을 반환한다.
func encodeSpanLinks(links []model.SpanLink) string {
	if len(links) == 0 {
		return ""
	}
	b, _ := json.Marshal(links)
	return string(b)
}

// toStringMap은 map[string]any를 ClickHouse Map(String,String) 타입에 맞게 변환한다.
// 비문자열 값은 fmt.Sprintf("%v")로 직렬화한다.
func toStringMap(attrs map[string]any) map[string]string {
	if len(attrs) == 0 {
		return map[string]string{}
	}
	m := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if s, ok := v.(string); ok {
			m[k] = s
		} else {
			m[k] = fmt.Sprintf("%v", v)
		}
	}
	return m
}

// fromStringMap은 ClickHouse Map(String,String)을 map[string]any로 변환한다.
func fromStringMap(m map[string]string) map[string]any {
	if len(m) == 0 {
		return nil
	}
	attrs := make(map[string]any, len(m))
	for k, v := range m {
		attrs[k] = v
	}
	return attrs
}

// toJSONString은 map[string]string을 JSON 문자열로 직렬화한다.
// ClickHouse String 컬럼에 attributes를 저장할 때 사용한다.
func toJSONString(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// fromJSONString은 ClickHouse String 컬럼의 JSON 문자열을 map[string]any로 역직렬화한다.
func fromJSONString(s string) map[string]any {
	if s == "" || s == "{}" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil
	}
	return fromStringMap(m)
}

func strAttr(attrs map[string]any, key string) string {
	if v, ok := attrs[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func uint16Attr(attrs map[string]any, key string) uint16 {
	if v, ok := attrs[key]; ok {
		switch val := v.(type) {
		case float64:
			return uint16(val)
		case int64:
			return uint16(val)
		case int:
			return uint16(val)
		case string:
			var n int
			_, _ = fmt.Sscanf(val, "%d", &n)
			return uint16(n)
		}
	}
	return 0
}

// ---- DDL ----

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
    toStartOfFiveMinutes(fromUnixTimestamp64Nano(start_time_nano))    AS minute5,
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
    service_name LowCardinality(String),
    metric_name  LowCardinality(String),
    minute       DateTime,
    count_state  AggregateFunction(sum, UInt64),
    sum_state    AggregateFunction(sum, Float64),
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
    sumState(total_count) AS count_state,
    sumState(total_sum)   AS sum_state,
    dt
FROM %s.metric_histograms
GROUP BY service_name, metric_name, minute, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_histogram_1m: %w", err)
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
