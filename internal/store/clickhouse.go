// Package store - ClickHouse 배치 저장소 구현체.
//
// 파이프라인:
//
//	AppendSpans/Metrics/Logs
//	  → 채널에 enqueue (backpressure: ResourceExhausted 반환)
//	  → batchWriter goroutine: size-trigger 또는 time-trigger 로 flush
//	  → ClickHouse native protocol batch insert
//
// 상용 APM best-practice 적용:
//   - 공유 커넥션 풀: OpenConn으로 1개 pool 생성 후 3개 store가 공유
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

// ---- ClickHouseTraceStore ----

// ClickHouseTraceStore는 SpanData를 apm.spans 테이블에 배치 insert한다.
type ClickHouseTraceStore struct {
	conn    driver.Conn // 공유 커넥션 풀 (소유권 없음 — Close()에서 닫지 않는다)
	ch      chan *model.SpanData
	cfg     ClickHouseConfig
	done    chan struct{}
	flushWg sync.WaitGroup // 비동기 flush 완료 대기용
}

// NewClickHouseTraceStore는 공유 커넥션을 받아 테이블 DDL을 적용하고 batchWriter를 시작한다.
// conn 소유권은 호출자가 가진다 — Close()를 호출해도 conn은 닫히지 않는다.
func NewClickHouseTraceStore(conn driver.Conn, cfg ClickHouseConfig) (*ClickHouseTraceStore, error) {
	if err := ensureSpansTable(conn, cfg.Database, cfg.RetentionDays); err != nil {
		return nil, fmt.Errorf("clickhouse trace DDL: %w", err)
	}

	s := &ClickHouseTraceStore{
		conn: conn,
		ch:   make(chan *model.SpanData, cfg.ChanBuffer),
		cfg:  cfg,
		done: make(chan struct{}),
	}
	go s.batchWriter()
	return s, nil
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
		 LIMIT %d`,
		s.cfg.Database, where, q.Limit,
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
			attrsJSON    string
			durationNano int64
		)
		if err := rows.Scan(
			&sp.TraceID, &sp.SpanID, &sp.ParentSpanID, &sp.Name, &sp.Kind,
			&sp.StartTimeNano, &sp.EndTimeNano, &durationNano,
			&attrsJSON, &sp.StatusCode, &sp.StatusMessage,
			&sp.ServiceName, &sp.ScopeName, &sp.ReceivedAtMs,
		); err != nil {
			return nil, err
		}
		if attrsJSON != "" && attrsJSON != "{}" {
			_ = json.Unmarshal([]byte(attrsJSON), &sp.Attributes)
		}
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
    sumMerge(total_count)                                AS rps,
    sumMerge(error_count)                                AS errors,
    sumMerge(error_count) / sumMerge(total_count) * 100 AS error_rate_pct,
    quantileTDigestMerge(0.50)(duration_p50) / 1e6      AS p50_ms,
    quantileTDigestMerge(0.95)(duration_p95) / 1e6      AS p95_ms,
    quantileTDigestMerge(0.99)(duration_p99) / 1e6      AS p99_ms
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
    sumMerge(call_count)                                  AS total_calls,
    sumMerge(error_count)                                 AS error_calls,
    sumMerge(error_count) / sumMerge(call_count) * 100   AS error_rate_pct,
    sumMerge(duration_sum) / sumMerge(call_count) / 1e6  AS avg_latency_ms
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

// Close는 채널을 닫고 batchWriter가 남은 항목을 flush할 때까지 대기한다.
// conn은 공유 자원이므로 닫지 않는다 — 호출자가 직접 conn.Close()를 호출해야 한다.
func (s *ClickHouseTraceStore) Close() error {
	close(s.ch)
	select {
	case <-s.done:
	case <-time.After(closeTimeout):
		slog.Warn("clickhouse trace store close timeout: drain incomplete")
	}
	return nil
}

// batchWriter는 채널에서 span을 읽어 배치를 구성하고 ClickHouse에 비동기 insert한다.
//
// 핵심 개선: flush를 별도 goroutine에서 실행하여 batchWriter가 ClickHouse I/O와
// retry sleep(1s→2s→4s) 동안 블록되지 않는다. 채널은 flush 중에도 계속 소비된다.
// flushMu로 동시 flush를 직렬화해 ClickHouse 커넥션 pool 고갈을 방지한다.
func (s *ClickHouseTraceStore) batchWriter() {
	defer func() {
		s.flushWg.Wait() // 비동기 flush 완료 대기 후 done 시그널
		close(s.done)
	}()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("clickhouse trace batchWriter panic recovered", "panic", r)
		}
	}()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.SpanData, 0, s.cfg.BatchSize)
	var flushMu sync.Mutex // flush를 직렬화: ClickHouse에 동시에 1개만 전송

	doFlush := func(b []*model.SpanData) {
		if len(b) == 0 {
			return
		}
		s.flushWg.Add(1)
		go func(data []*model.SpanData) {
			defer s.flushWg.Done()
			flushMu.Lock()
			defer flushMu.Unlock()
			if err := retryFlush("spans", func() error { return s.flushSpans(data) }); err != nil {
				chFlushErrorsTotal.WithLabelValues("spans").Inc()
				slog.Error("clickhouse span flush failed (all retries exhausted)", "err", err, "count", len(data))
			}
		}(b)
	}

	for {
		select {
		case sp, ok := <-s.ch:
			if !ok {
				doFlush(batch)
				return
			}
			batch = append(batch, sp)
			if len(batch) >= s.cfg.BatchSize {
				doFlush(batch)
				batch = make([]*model.SpanData, 0, s.cfg.BatchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				doFlush(batch)
				batch = make([]*model.SpanData, 0, s.cfg.BatchSize)
			}
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
		 peer_service, exception_type, exception_message, exception_stacktrace)`, s.cfg.Database),
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, sp := range spans {
		attrsJSON, _ := json.Marshal(sp.Attributes)
		attrs := sp.Attributes
		if attrs == nil {
			attrs = map[string]any{}
		}
		if err := batch.Append(
			sp.TraceID, sp.SpanID, sp.ParentSpanID, sp.Name, sp.Kind,
			sp.StartTimeNano, sp.EndTimeNano, sp.DurationNano(),
			string(attrsJSON), sp.StatusCode, sp.StatusMessage,
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
	conn    driver.Conn // 공유 커넥션 풀 (소유권 없음)
	ch      chan *model.MetricData
	cfg     ClickHouseConfig
	done    chan struct{}
	flushWg sync.WaitGroup // 비동기 flush 완료 대기용
}

func NewClickHouseMetricStore(conn driver.Conn, cfg ClickHouseConfig) (*ClickHouseMetricStore, error) {
	if err := ensureMetricsTable(conn, cfg.Database, cfg.RetentionDays); err != nil {
		return nil, fmt.Errorf("clickhouse metric DDL: %w", err)
	}

	s := &ClickHouseMetricStore{
		conn: conn,
		ch:   make(chan *model.MetricData, cfg.ChanBuffer),
		cfg:  cfg,
		done: make(chan struct{}),
	}
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

	sql := fmt.Sprintf(
		`SELECT name, type, value, attributes, service_name, timestamp_nano, received_at_ms
		 FROM %s.metrics
		 %s
		 ORDER BY received_at_ms DESC
		 LIMIT %d`,
		s.cfg.Database, where, q.Limit,
	)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*model.MetricData
	for rows.Next() {
		var (
			name, mtype, attrsJSON, serviceName string
			value                               float64
			timestampNano, receivedAtMs         int64
		)
		if err := rows.Scan(&name, &mtype, &value, &attrsJSON, &serviceName, &timestampNano, &receivedAtMs); err != nil {
			return nil, err
		}
		var attrs map[string]any
		if attrsJSON != "" {
			_ = json.Unmarshal([]byte(attrsJSON), &attrs)
		}
		result = append(result, &model.MetricData{
			Name:         name,
			Type:         model.MetricType(mtype),
			ServiceName:  serviceName,
			ReceivedAtMs: receivedAtMs,
			DataPoints: []model.DataPoint{
				{Attributes: attrs, TimeNanos: timestampNano, Value: value},
			},
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
	return nil
}

func (s *ClickHouseMetricStore) batchWriter() {
	defer func() {
		s.flushWg.Wait()
		close(s.done)
	}()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("clickhouse metric batchWriter panic recovered", "panic", r)
		}
	}()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.MetricData, 0, s.cfg.BatchSize)
	var flushMu sync.Mutex

	doFlush := func(b []*model.MetricData) {
		if len(b) == 0 {
			return
		}
		s.flushWg.Add(1)
		go func(data []*model.MetricData) {
			defer s.flushWg.Done()
			flushMu.Lock()
			defer flushMu.Unlock()
			if err := retryFlush("metrics", func() error { return s.flushMetrics(data) }); err != nil {
				chFlushErrorsTotal.WithLabelValues("metrics").Inc()
				slog.Error("clickhouse metric flush failed (all retries exhausted)", "err", err, "count", len(data))
			}
		}(b)
	}

	for {
		select {
		case m, ok := <-s.ch:
			if !ok {
				doFlush(batch)
				return
			}
			batch = append(batch, m)
			if len(batch) >= s.cfg.BatchSize {
				doFlush(batch)
				batch = make([]*model.MetricData, 0, s.cfg.BatchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				doFlush(batch)
				batch = make([]*model.MetricData, 0, s.cfg.BatchSize)
			}
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
			attrsJSON, _ := json.Marshal(dp.Attributes)
			var value float64
			if m.Type == model.MetricTypeHistogram {
				value = dp.Sum
			} else {
				value = dp.Value
			}
			if err := batch.Append(
				m.Name, string(m.Type), value,
				string(attrsJSON), m.ServiceName,
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
			attrsJSON, _ := json.Marshal(dp.Attributes)
			buckets := make([]uint64, len(dp.BucketCounts))
			copy(buckets, dp.BucketCounts)
			if err := batch.Append(
				m.ServiceName, m.Name, dp.TimeNanos,
				dp.ExplicitBounds, buckets,
				uint64(dp.Count), dp.Sum,
				string(attrsJSON), m.ReceivedAtMs,
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
	conn    driver.Conn // 공유 커넥션 풀 (소유권 없음)
	ch      chan *model.LogData
	cfg     ClickHouseConfig
	done    chan struct{}
	flushWg sync.WaitGroup // 비동기 flush 완료 대기용
}

func NewClickHouseLogStore(conn driver.Conn, cfg ClickHouseConfig) (*ClickHouseLogStore, error) {
	if err := ensureLogsTable(conn, cfg.Database, cfg.RetentionDays); err != nil {
		return nil, fmt.Errorf("clickhouse log DDL: %w", err)
	}

	s := &ClickHouseLogStore{
		conn: conn,
		ch:   make(chan *model.LogData, cfg.ChanBuffer),
		cfg:  cfg,
		done: make(chan struct{}),
	}
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
		 LIMIT %d`,
		s.cfg.Database, where, q.Limit,
	)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*model.LogData
	for rows.Next() {
		var (
			l         model.LogData
			attrsJSON string
		)
		if err := rows.Scan(
			&l.SeverityText, &l.SeverityNumber, &l.Body, &attrsJSON,
			&l.ServiceName, &l.TraceID, &l.SpanID,
			&l.TimestampNanos, &l.ReceivedAtMs,
		); err != nil {
			return nil, err
		}
		if attrsJSON != "" {
			_ = json.Unmarshal([]byte(attrsJSON), &l.Attributes)
		}
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
    sumMerge(error_count) AS error_count
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
	return nil
}

func (s *ClickHouseLogStore) batchWriter() {
	defer func() {
		s.flushWg.Wait()
		close(s.done)
	}()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("clickhouse log batchWriter panic recovered", "panic", r)
		}
	}()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.LogData, 0, s.cfg.BatchSize)
	var flushMu sync.Mutex

	doFlush := func(b []*model.LogData) {
		if len(b) == 0 {
			return
		}
		s.flushWg.Add(1)
		go func(data []*model.LogData) {
			defer s.flushWg.Done()
			flushMu.Lock()
			defer flushMu.Unlock()
			if err := retryFlush("logs", func() error { return s.flushLogs(data) }); err != nil {
				chFlushErrorsTotal.WithLabelValues("logs").Inc()
				slog.Error("clickhouse log flush failed (all retries exhausted)", "err", err, "count", len(data))
			}
		}(b)
	}

	for {
		select {
		case l, ok := <-s.ch:
			if !ok {
				doFlush(batch)
				return
			}
			batch = append(batch, l)
			if len(batch) >= s.cfg.BatchSize {
				doFlush(batch)
				batch = make([]*model.LogData, 0, s.cfg.BatchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				doFlush(batch)
				batch = make([]*model.LogData, 0, s.cfg.BatchSize)
			}
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
		attrsJSON, _ := json.Marshal(l.Attributes)
		attrs := l.Attributes
		if attrs == nil {
			attrs = map[string]any{}
		}
		if err := batch.Append(
			l.SeverityText, l.SeverityNumber, l.Body,
			string(attrsJSON), l.ServiceName,
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
    attributes       String,
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
	}
	for _, q := range alterCols {
		if err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("alter spans column: %w", err)
		}
	}

	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.spans MODIFY TTL dt + INTERVAL %d DAY`, db, retentionDays)); err != nil {
		return err
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_red_1m_state (
    service_name LowCardinality(String),
    span_name    LowCardinality(String),
    http_route   LowCardinality(String),
    minute       DateTime,
    total_count  SimpleAggregateFunction(sum, UInt64),
    error_count  SimpleAggregateFunction(sum, UInt64),
    duration_p50 AggregateFunction(quantileTDigest(0.5), Float64),
    duration_p95 AggregateFunction(quantileTDigest(0.95), Float64),
    duration_p99 AggregateFunction(quantileTDigest(0.99), Float64),
    duration_sum SimpleAggregateFunction(sum, Float64),
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
    quantileTDigestState(0.5)(toFloat64(duration_nano))              AS duration_p50,
    quantileTDigestState(0.95)(toFloat64(duration_nano))             AS duration_p95,
    quantileTDigestState(0.99)(toFloat64(duration_nano))             AS duration_p99,
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

	return nil
}

func ensureMetricsTable(conn driver.Conn, db string, retentionDays int) error {
	ctx := context.Background()

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.metrics (
    name           LowCardinality(String),
    type           LowCardinality(String),
    value          Float64,
    attributes     String,
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

	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.metric_histograms (
    service_name    LowCardinality(String),
    metric_name     LowCardinality(String),
    timestamp_nano  Int64,
    bounds          Array(Float64),
    bucket_counts   Array(UInt64),
    total_count     UInt64,
    total_sum       Float64,
    attributes      String,
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
    attributes      String,
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
	}
	for _, q := range alterCols {
		if err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("alter logs column: %w", err)
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
