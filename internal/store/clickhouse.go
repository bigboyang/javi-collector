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
	if err := ensureSpansTable(conn, cfg.Database); err != nil {
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

// QuerySpans는 최근 limit개 span을 service_name, start_time_nano DESC 순으로 반환한다.
func (s *ClickHouseTraceStore) QuerySpans(ctx context.Context, limit int) ([]*model.SpanData, error) {
	q := fmt.Sprintf(
		`SELECT trace_id, span_id, parent_span_id, name, kind,
		        start_time_nano, end_time_nano, duration_nano,
		        attributes, status_code, status_message,
		        service_name, scope_name, received_at_ms
		 FROM %s.spans
		 ORDER BY received_at_ms DESC
		 LIMIT %d`,
		s.cfg.Database, limit,
	)
	rows, err := s.conn.Query(ctx, q)
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
		 service_name, scope_name, received_at_ms)`, s.cfg.Database),
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, sp := range spans {
		attrsJSON, _ := json.Marshal(sp.Attributes)
		if err := batch.Append(
			sp.TraceID, sp.SpanID, sp.ParentSpanID, sp.Name, sp.Kind,
			sp.StartTimeNano, sp.EndTimeNano, sp.DurationNano(),
			string(attrsJSON), sp.StatusCode, sp.StatusMessage,
			sp.ServiceName, sp.ScopeName, sp.ReceivedAtMs,
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
	if err := ensureMetricsTable(conn, cfg.Database); err != nil {
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

func (s *ClickHouseMetricStore) QueryMetrics(ctx context.Context, limit int) ([]*model.MetricData, error) {
	q := fmt.Sprintf(
		`SELECT name, type, value, attributes, service_name, timestamp_nano, received_at_ms
		 FROM %s.metrics
		 ORDER BY received_at_ms DESC
		 LIMIT %d`,
		s.cfg.Database, limit,
	)
	rows, err := s.conn.Query(ctx, q)
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

	batch, err := s.conn.PrepareBatch(ctx,
		fmt.Sprintf(`INSERT INTO %s.metrics
		(name, type, value, attributes, service_name, timestamp_nano, received_at_ms)`, s.cfg.Database),
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	rowCount := 0
	for _, m := range metrics {
		for _, dp := range m.DataPoints {
			attrsJSON, _ := json.Marshal(dp.Attributes)
			val := dp.Value
			if m.Type == model.MetricTypeHistogram || m.Type == model.MetricTypeSummary {
				val = dp.Sum
			}
			if err := batch.Append(
				m.Name, string(m.Type), val,
				string(attrsJSON), m.ServiceName,
				dp.TimeNanos, m.ReceivedAtMs,
			); err != nil {
				return fmt.Errorf("batch append metric: %w", err)
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

	elapsed := time.Since(start).Seconds()
	chFlushDuration.WithLabelValues("metrics").Observe(elapsed)
	chFlushRowsTotal.WithLabelValues("metrics").Add(float64(rowCount))
	slog.Debug("clickhouse metrics flushed", "rows", rowCount, "elapsed_ms", elapsed*1000)
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
	if err := ensureLogsTable(conn, cfg.Database); err != nil {
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

func (s *ClickHouseLogStore) QueryLogs(ctx context.Context, limit int) ([]*model.LogData, error) {
	q := fmt.Sprintf(
		`SELECT severity_text, severity_number, body, attributes,
		        service_name, trace_id, span_id, timestamp_nano, received_at_ms
		 FROM %s.logs
		 ORDER BY received_at_ms DESC
		 LIMIT %d`,
		s.cfg.Database, limit,
	)
	rows, err := s.conn.Query(ctx, q)
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
		 service_name, trace_id, span_id, timestamp_nano, received_at_ms)`, s.cfg.Database),
	)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, l := range logs {
		attrsJSON, _ := json.Marshal(l.Attributes)
		if err := batch.Append(
			l.SeverityText, l.SeverityNumber, l.Body,
			string(attrsJSON), l.ServiceName,
			l.TraceID, l.SpanID, l.TimestampNanos, l.ReceivedAtMs,
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

// ensureSpansTable은 apm.spans 테이블이 없으면 생성한다.
func ensureSpansTable(conn driver.Conn, db string) error {
	return conn.Exec(context.Background(), fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.spans (
    trace_id        String,
    span_id         String,
    parent_span_id  String,
    name            String,
    kind            Int32,
    start_time_nano Int64,
    end_time_nano   Int64,
    duration_nano   Int64,
    attributes      String,
    status_code     Int32,
    status_message  String,
    service_name    LowCardinality(String),
    scope_name      LowCardinality(String),
    received_at_ms  Int64,
    dt Date DEFAULT toDate(fromUnixTimestamp64Milli(received_at_ms))
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, trace_id, start_time_nano)
TTL dt + INTERVAL 30 DAY;
`, db))
}

// ensureMetricsTable은 apm.metrics 테이블이 없으면 생성한다.
func ensureMetricsTable(conn driver.Conn, db string) error {
	return conn.Exec(context.Background(), fmt.Sprintf(`
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
TTL dt + INTERVAL 30 DAY;
`, db))
}

// ensureLogsTable은 apm.logs 테이블이 없으면 생성한다.
func ensureLogsTable(conn driver.Conn, db string) error {
	return conn.Exec(context.Background(), fmt.Sprintf(`
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
    dt Date DEFAULT toDate(fromUnixTimestamp64Milli(received_at_ms))
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, timestamp_nano)
TTL dt + INTERVAL 30 DAY;
`, db))
}
