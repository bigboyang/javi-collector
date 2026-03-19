// Package store - ClickHouse 배치 저장소 구현체.
//
// 파이프라인:
//
//	AppendSpans/Metrics/Logs
//	  → 채널에 enqueue (backpressure: ResourceExhausted 반환)
//	  → batchWriter goroutine: size-trigger 또는 time-trigger 로 flush
//	  → ClickHouse native protocol batch insert
//
// 성능 포인트:
//   - native protocol(9000) 사용: HTTP보다 약 3-5x 처리량 우수
//   - async_insert=0: 서버 측 버퍼링 없이 클라이언트가 배치를 직접 제어
//   - 채널 버퍼가 꽉 차면 즉시 에러를 반환해 gRPC 레이어에서 backpressure 전달
//
// backpressure 원자성:
//
//	AppendSpans 루프 중간에 채널이 꽉 차면 일부만 enqueue되는 partial insert 문제가 있다.
//	이를 방지하기 위해 먼저 여유 공간을 확인하고, 공간이 충분할 때만 전체를 enqueue한다.
//	⚠️ 경쟁 조건: 확인과 삽입 사이에 다른 goroutine이 채널을 채울 수 있다.
//	하지만 Append의 각 항목이 독립적이므로 중복 저장보다 partial drop이 허용되는 APM 특성상
//	이 방식이 더 안전하다. 실제 원자성이 필요하다면 별도 mutex로 enqueue 구간을 보호해야 한다.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/kkc/javi-collector/internal/model"
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
		Help:      "Total number of ClickHouse flush errors.",
	}, []string{"table"})

	chChannelDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "channel_depth",
		Help:      "Current depth of the ClickHouse write channel.",
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

// ---- ClickHouseTraceStore ----

// ClickHouseTraceStore는 SpanData를 apm.spans 테이블에 배치 insert한다.
type ClickHouseTraceStore struct {
	conn driver.Conn
	ch   chan *model.SpanData
	cfg  ClickHouseConfig
	done chan struct{}
}

// NewClickHouseTraceStore는 연결을 열고 테이블 DDL을 적용한 뒤 batchWriter를 시작한다.
func NewClickHouseTraceStore(cfg ClickHouseConfig) (*ClickHouseTraceStore, error) {
	conn, err := openConn(cfg)
	if err != nil {
		return nil, fmt.Errorf("clickhouse trace store: %w", err)
	}
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
//
// backpressure 원자성 개선:
// 루프 중간에 채널이 꽉 차면 일부만 enqueue되는 partial insert를 방지하기 위해
// 먼저 채널 여유 공간을 확인한 뒤 전체를 enqueue한다.
// APM 특성상 일부 span의 drop은 허용되므로 공간 부족 시 전체를 drop하고 에러를 반환한다.
func (s *ClickHouseTraceStore) AppendSpans(_ context.Context, spans []*model.SpanData) error {
	// 채널 여유 공간 선제 확인 (approximate — 경쟁 조건 가능하지만 APM에서 허용 가능)
	available := cap(s.ch) - len(s.ch)
	if available < len(spans) {
		chChannelDepth.WithLabelValues("spans").Set(float64(len(s.ch)))
		return fmt.Errorf("trace channel full (capacity=%d, available=%d, need=%d): backpressure",
			cap(s.ch), available, len(spans))
	}
	for _, sp := range spans {
		select {
		case s.ch <- sp:
		default:
			// 사전 확인 후에도 race로 가득 찬 경우 — 나머지는 drop
			chChannelDepth.WithLabelValues("spans").Set(float64(len(s.ch)))
			return fmt.Errorf("trace channel full during append: backpressure")
		}
	}
	chChannelDepth.WithLabelValues("spans").Set(float64(len(s.ch)))
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
// service가 빈 문자열이면 전체 서비스를 조회한다.
// 대시보드의 Service Overview 패널에서 사용한다.
func (s *ClickHouseTraceStore) QueryRED(ctx context.Context, service string, fromMs, toMs int64) ([]map[string]any, error) {
	var conds []string
	var args []any

	if fromMs > 0 {
		conds = append(conds, "minute >= ?")
		args = append(args, fromMs/1000) // ms → unix seconds
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
// 대시보드의 Service Topology Map 패널에서 사용한다.
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
			"caller_service":  caller,
			"callee_service":  callee,
			"total_calls":     totalCalls,
			"error_calls":     errorCalls,
			"error_rate_pct":  errorRate,
			"avg_latency_ms":  avgLatency,
		})
	}
	return result, rows.Err()
}

func (s *ClickHouseTraceStore) Close() error {
	close(s.ch)
	<-s.done
	return s.conn.Close()
}

// batchWriter는 채널에서 span을 읽어 배치를 구성하고 ClickHouse에 insert한다.
func (s *ClickHouseTraceStore) batchWriter() {
	defer close(s.done)

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.SpanData, 0, s.cfg.BatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.flushSpans(batch); err != nil {
			chFlushErrorsTotal.WithLabelValues("spans").Inc()
			slog.Error("clickhouse span flush failed", "err", err, "count", len(batch))
		}
		batch = batch[:0]
	}

	for {
		select {
		case sp, ok := <-s.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, sp)
			if len(batch) >= s.cfg.BatchSize {
				flush()
			}

		case <-ticker.C:
			flush()
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
			// HTTP semantic conventions
			strAttr(attrs, "http.request.method"),
			strAttr(attrs, "http.route"),
			uint16Attr(attrs, "http.response.status_code"),
			// DB semantic conventions
			strAttr(attrs, "db.system"),
			strAttr(attrs, "db.name"),
			strAttr(attrs, "db.operation"),
			// RPC semantic conventions
			strAttr(attrs, "rpc.system"),
			strAttr(attrs, "rpc.service"),
			strAttr(attrs, "rpc.method"),
			// 서비스 토폴로지 (CLIENT span의 peer.service)
			strAttr(attrs, "peer.service"),
			// 에러/예외 정보 (RAG 컨텍스트)
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
	conn driver.Conn
	ch   chan *model.MetricData
	cfg  ClickHouseConfig
	done chan struct{}
}

func NewClickHouseMetricStore(cfg ClickHouseConfig) (*ClickHouseMetricStore, error) {
	conn, err := openConn(cfg)
	if err != nil {
		return nil, fmt.Errorf("clickhouse metric store: %w", err)
	}
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
	available := cap(s.ch) - len(s.ch)
	if available < len(metrics) {
		chChannelDepth.WithLabelValues("metrics").Set(float64(len(s.ch)))
		return fmt.Errorf("metric channel full (capacity=%d, available=%d, need=%d): backpressure",
			cap(s.ch), available, len(metrics))
	}
	for _, m := range metrics {
		select {
		case s.ch <- m:
		default:
			chChannelDepth.WithLabelValues("metrics").Set(float64(len(s.ch)))
			return fmt.Errorf("metric channel full during append: backpressure")
		}
	}
	chChannelDepth.WithLabelValues("metrics").Set(float64(len(s.ch)))
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
	<-s.done
	return s.conn.Close()
}

func (s *ClickHouseMetricStore) batchWriter() {
	defer close(s.done)

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.MetricData, 0, s.cfg.BatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.flushMetrics(batch); err != nil {
			chFlushErrorsTotal.WithLabelValues("metrics").Inc()
			slog.Error("clickhouse metric flush failed", "err", err, "count", len(batch))
		}
		batch = batch[:0]
	}

	for {
		select {
		case m, ok := <-s.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, m)
			if len(batch) >= s.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (s *ClickHouseMetricStore) flushMetrics(metrics []*model.MetricData) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// GAUGE/SUM 메트릭 배치
	batch, err := s.conn.PrepareBatch(ctx,
		fmt.Sprintf(`INSERT INTO %s.metrics
		(name, type, value, attributes, service_name, timestamp_nano, received_at_ms)`, s.cfg.Database),
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	// Histogram 배치 (별도 테이블에 BucketCounts 전체 보존)
	histBatch, err := s.conn.PrepareBatch(ctx,
		fmt.Sprintf(`INSERT INTO %s.metric_histograms
		(service_name, metric_name, timestamp_nano, bounds, bucket_counts,
		 total_count, total_sum, attributes, received_at_ms)`, s.cfg.Database),
	)
	if err != nil {
		return fmt.Errorf("prepare histogram batch: %w", err)
	}

	rowCount := 0
	histRowCount := 0

	for _, m := range metrics {
		for _, dp := range m.DataPoints {
			attrsJSON, _ := json.Marshal(dp.Attributes)

			if m.Type == model.MetricTypeHistogram {
				// Histogram → metric_histograms 테이블 (P95/P99 계산 가능)
				if err := histBatch.Append(
					m.ServiceName, m.Name, dp.TimeNanos,
					dp.ExplicitBounds, dp.BucketCounts,
					uint64(dp.Count), dp.Sum,
					string(attrsJSON), m.ReceivedAtMs,
				); err != nil {
					return fmt.Errorf("batch append histogram: %w", err)
				}
				histRowCount++
				// metrics 테이블에도 sum/count 기록 (평균 계산용)
				if err := batch.Append(
					m.Name, string(m.Type), dp.Sum,
					string(attrsJSON), m.ServiceName,
					dp.TimeNanos, m.ReceivedAtMs,
				); err != nil {
					return fmt.Errorf("batch append metric (hist): %w", err)
				}
			} else {
				if err := batch.Append(
					m.Name, string(m.Type), dp.Value,
					string(attrsJSON), m.ServiceName,
					dp.TimeNanos, m.ReceivedAtMs,
				); err != nil {
					return fmt.Errorf("batch append metric: %w", err)
				}
			}
			rowCount++
		}
	}

	if rowCount == 0 {
		return nil
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("batch send metrics: %w", err)
	}
	if histRowCount > 0 {
		if err := histBatch.Send(); err != nil {
			return fmt.Errorf("batch send histograms: %w", err)
		}
	}

	elapsed := time.Since(start).Seconds()
	chFlushDuration.WithLabelValues("metrics").Observe(elapsed)
	chFlushRowsTotal.WithLabelValues("metrics").Add(float64(rowCount))
	slog.Debug("clickhouse metrics flushed", "rows", rowCount, "hist_rows", histRowCount, "elapsed_ms", elapsed*1000)
	return nil
}

// ---- ClickHouseLogStore ----

// ClickHouseLogStore는 LogData를 apm.logs 테이블에 배치 insert한다.
type ClickHouseLogStore struct {
	conn driver.Conn
	ch   chan *model.LogData
	cfg  ClickHouseConfig
	done chan struct{}
}

func NewClickHouseLogStore(cfg ClickHouseConfig) (*ClickHouseLogStore, error) {
	conn, err := openConn(cfg)
	if err != nil {
		return nil, fmt.Errorf("clickhouse log store: %w", err)
	}
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
	available := cap(s.ch) - len(s.ch)
	if available < len(logs) {
		chChannelDepth.WithLabelValues("logs").Set(float64(len(s.ch)))
		return fmt.Errorf("log channel full (capacity=%d, available=%d, need=%d): backpressure",
			cap(s.ch), available, len(logs))
	}
	for _, l := range logs {
		select {
		case s.ch <- l:
		default:
			chChannelDepth.WithLabelValues("logs").Set(float64(len(s.ch)))
			return fmt.Errorf("log channel full during append: backpressure")
		}
	}
	chChannelDepth.WithLabelValues("logs").Set(float64(len(s.ch)))
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
// 대시보드의 Error Stream 패널에서 사용한다.
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
	<-s.done
	return s.conn.Close()
}

func (s *ClickHouseLogStore) batchWriter() {
	defer close(s.done)

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]*model.LogData, 0, s.cfg.BatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.flushLogs(batch); err != nil {
			chFlushErrorsTotal.WithLabelValues("logs").Inc()
			slog.Error("clickhouse log flush failed", "err", err, "count", len(batch))
		}
		batch = batch[:0]
	}

	for {
		select {
		case l, ok := <-s.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, l)
			if len(batch) >= s.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
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

// strAttr은 attributes map에서 string 값을 추출한다.
func strAttr(attrs map[string]any, key string) string {
	if v, ok := attrs[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
}

// uint16Attr은 attributes map에서 uint16 값을 추출한다 (HTTP status code 등).
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

// ---- 공통 헬퍼 ----

// openConn은 ClickHouse native protocol 연결을 연다.
func openConn(cfg ClickHouseConfig) (driver.Conn, error) {
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
		MaxOpenConns:    5,
		MaxIdleConns:    2,
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

// ensureSpansTable은 apm.spans 테이블이 없으면 생성하고,
// 기존 테이블에 신규 컬럼 추가 + TTL 갱신 + Materialized View를 생성한다.
func ensureSpansTable(conn driver.Conn, db string, retentionDays int) error {
	ctx := context.Background()

	// 1. 기본 테이블 생성 (신규 설치)
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
    -- [OTel HTTP semantic conventions] 대시보드 필터/집계 핵심
    http_method      LowCardinality(String) DEFAULT '',
    http_route       LowCardinality(String) DEFAULT '',
    http_status_code UInt16                 DEFAULT 0,
    -- [OTel DB semantic conventions] 슬로우쿼리 분석
    db_system        LowCardinality(String) DEFAULT '',
    db_name          LowCardinality(String) DEFAULT '',
    db_operation     LowCardinality(String) DEFAULT '',
    -- [OTel RPC semantic conventions] gRPC 추적
    rpc_system       LowCardinality(String) DEFAULT '',
    rpc_service      LowCardinality(String) DEFAULT '',
    rpc_method       LowCardinality(String) DEFAULT '',
    -- [서비스 토폴로지] CLIENT span의 peer.service → 호출 관계 추출
    peer_service     LowCardinality(String) DEFAULT '',
    -- [에러 분석 / RAG] exception 정보 추출
    exception_type        String DEFAULT '',
    exception_message     String DEFAULT '',
    exception_stacktrace  String DEFAULT '',  -- 스택트레이스 (RAG 임베딩 핵심)
    dt Date DEFAULT toDate(fromUnixTimestamp64Milli(received_at_ms))
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, trace_id, start_time_nano)
TTL dt + INTERVAL %d DAY;
`, db, retentionDays)); err != nil {
		return err
	}

	// 2. 기존 테이블에 신규 컬럼 추가 (마이그레이션 안전)
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

	// 3. TTL 갱신
	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.spans MODIFY TTL dt + INTERVAL %d DAY`, db, retentionDays)); err != nil {
		return err
	}

	// 4. RED 메트릭 집계 테이블 (1분 단위: RPS, 에러율, 레이턴시)
	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mv_red_1m_state (
    service_name LowCardinality(String),
    span_name    LowCardinality(String),
    http_route   LowCardinality(String),
    minute       DateTime,
    total_count  SimpleAggregateFunction(sum, UInt64),
    error_count  SimpleAggregateFunction(sum, UInt64),
    duration_p50 AggregateFunction(quantileTDigest(0.001), Float64),
    duration_p95 AggregateFunction(quantileTDigest(0.001), Float64),
    duration_p99 AggregateFunction(quantileTDigest(0.001), Float64),
    duration_sum SimpleAggregateFunction(sum, Float64),
    dt Date
) ENGINE = AggregatingMergeTree()
PARTITION BY dt
ORDER BY (service_name, minute, span_name)
TTL dt + INTERVAL 90 DAY;
`, db)); err != nil {
		return fmt.Errorf("create mv_red_1m_state: %w", err)
	}

	// 5. RED 메트릭 MV (SERVER/CONSUMER span → 인바운드 요청만 집계)
	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE MATERIALIZED VIEW IF NOT EXISTS %s.mv_red_1m
TO %s.mv_red_1m_state
AS SELECT
    service_name,
    name                                                              AS span_name,
    http_route,
    toStartOfMinute(fromUnixTimestamp64Nano(start_time_nano))        AS minute,
    toUInt64(1)                                                       AS total_count,
    toUInt64(if(status_code = 2, 1, 0))                              AS error_count,
    quantileTDigestState(0.001)(toFloat64(duration_nano))             AS duration_p50,
    quantileTDigestState(0.001)(toFloat64(duration_nano))             AS duration_p95,
    quantileTDigestState(0.001)(toFloat64(duration_nano))             AS duration_p99,
    toFloat64(duration_nano)                                          AS duration_sum,
    dt
FROM %s.spans
WHERE kind IN (2, 5)
GROUP BY service_name, span_name, http_route, minute, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_red_1m: %w", err)
	}

	// 6. 서비스 토폴로지 집계 테이블 (5분 단위: caller→callee 호출 관계)
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
    toUInt64(1)                                                        AS call_count,
    toUInt64(if(status_code = 2, 1, 0))                               AS error_count,
    toFloat64(duration_nano)                                           AS duration_sum,
    dt
FROM %s.spans
WHERE kind = 3 AND peer_service != ''
GROUP BY caller_service, callee_service, minute5, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_service_topology: %w", err)
	}

	// 7. RAG 에러 컨텍스트 테이블 (임베딩 파이프라인 소스)
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
    NULL                                                            AS embedded_at,
    ''                                                              AS embedding_model,
    dt
FROM %s.spans
WHERE status_code = 2 AND status_message != '';
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_rag_from_spans: %w", err)
	}

	return nil
}

// ensureMetricsTable은 apm.metrics 테이블과 histogram 전용 테이블을 생성/갱신한다.
func ensureMetricsTable(conn driver.Conn, db string, retentionDays int) error {
	ctx := context.Background()

	// 1. GAUGE/SUM 집계용 기본 메트릭 테이블
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

	// 2. Histogram 전용 테이블: BucketCounts를 Array로 보존해 P95/P99 계산 가능
	if err := conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.metric_histograms (
    service_name    LowCardinality(String),
    metric_name     LowCardinality(String),
    timestamp_nano  Int64,
    -- OTLP explicit_bounds: 구간 경계값 배열
    bounds          Array(Float64),
    -- OTLP bucket_counts: 각 구간의 카운트
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

	// 3. Histogram 1분 집계 MV (P50/P95/P99 레이턴시 집계)
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

// ensureLogsTable은 apm.logs 테이블이 없으면 생성하고,
// 기존 테이블에 신규 컬럼 추가 + TTL 갱신 + RAG MV를 생성한다.
func ensureLogsTable(conn driver.Conn, db string, retentionDays int) error {
	ctx := context.Background()

	// 1. 테이블 생성
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
    -- 에러 분석용 추출 필드
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

	// 2. 기존 테이블 마이그레이션
	alterCols := []string{
		fmt.Sprintf("ALTER TABLE %s.logs ADD COLUMN IF NOT EXISTS exception_type LowCardinality(String) DEFAULT ''", db),
		fmt.Sprintf("ALTER TABLE %s.logs ADD COLUMN IF NOT EXISTS logger_name LowCardinality(String) DEFAULT ''", db),
	}
	for _, q := range alterCols {
		if err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("alter logs column: %w", err)
		}
	}

	// 3. TTL 갱신
	if err := conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s.logs MODIFY TTL dt + INTERVAL %d DAY`, db, retentionDays)); err != nil {
		return err
	}

	// 4. 에러 로그 1분 집계 MV (대시보드 에러 스트림 패널)
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
    toUInt64(1) AS error_count,
    dt
FROM %s.logs
WHERE severity_number >= 17
GROUP BY service_name, exception_type, minute, dt;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_error_logs_1m: %w", err)
	}

	// 5. RAG 에러 로그 컨텍스트 MV
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
    NULL                                                    AS embedded_at,
    ''                                                      AS embedding_model,
    dt
FROM %s.logs
WHERE severity_number >= 17;
`, db, db, db)); err != nil {
		return fmt.Errorf("create mv_rag_from_logs: %w", err)
	}

	return nil
}
