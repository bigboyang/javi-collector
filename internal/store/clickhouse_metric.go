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

// ClickHouseMetricStore는 MetricData를 apm.metrics 테이블에 배치 insert한다.
type ClickHouseMetricStore struct {
	conn    driver.Conn              // 공유 커넥션 풀 (소유권 없음)
	ch      chan *model.MetricData   // 수신 데이터 채널
	flushCh chan []*model.MetricData // 조립된 배치를 flush worker에 전달하는 큐
	cfg     ClickHouseConfig
	dynCfg  atomic.Pointer[storeDynCfg] // 핫 리로드 가능한 배치 설정
	done    chan struct{}               // 모든 flushWorker 종료 시 닫힘
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

	// 히스토그램 MV 스키마 마이그레이션 (bucket_counts_state / bounds_state 컬럼 추가 및 MV 재생성)
	migrator := NewMigrator(conn, cfg.Database)
	if err := migrator.Run(context.Background(), BuildMetricsMigrations(cfg.Database)); err != nil {
		// 마이그레이션 실패는 경고로만 처리 — IF NOT EXISTS / IF EXISTS로 idempotent
		slog.Warn("metrics schema migration failed", "err", err)
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
			name, mtype, serviceName    string
			attrsMap                    map[string]string
			value                       float64
			timestampNano, receivedAtMs int64
		)
		if err := rows.Scan(&name, &mtype, &value, &attrsMap, &serviceName, &timestampNano, &receivedAtMs); err != nil {
			return nil, err
		}
		result = append(result, &model.MetricData{
			Name:         name,
			Type:         model.MetricType(mtype),
			ServiceName:  serviceName,
			ReceivedAtMs: receivedAtMs,
			DataPoints: []model.DataPoint{
				{Attributes: fromStringMap(attrsMap), TimeNanos: timestampNano, Value: value},
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
			metricName, serviceName     string
			bounds                      []float64
			bucketCounts                []uint64
			totalCount                  uint64
			totalSum                    float64
			attrsMap                    map[string]string
			timestampNano, receivedAtMs int64
		)
		if err := hrows.Scan(&metricName, &bounds, &bucketCounts, &totalCount, &totalSum, &attrsMap, &serviceName, &timestampNano, &receivedAtMs); err != nil {
			return nil, err
		}
		result = append(result, &model.MetricData{
			Name:         metricName,
			Type:         model.MetricTypeHistogram,
			ServiceName:  serviceName,
			ReceivedAtMs: receivedAtMs,
			DataPoints: []model.DataPoint{
				{
					Attributes:     fromStringMap(attrsMap),
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

// histogramTableForRange는 쿼리 시간 범위에 따라 적절한 히스토그램 롤업 테이블과 타임스탬프 컬럼명을 반환한다.
//
// 티어링 기준:
//   - < 3h:   1분 집계 (mv_histogram_1m_state, "minute")
//   - 3h–24h: 5분 집계 (mv_histogram_5m_state, "minute5")
//   - > 24h:  1시간 집계 (mv_histogram_1h_state, "hour")
func histogramTableForRange(fromMs, toMs int64) (table, tsCol string) {
	if fromMs <= 0 || toMs <= 0 {
		return "mv_histogram_1m_state", "minute"
	}
	const (
		threeHoursMs    = int64(3 * 60 * 60 * 1000)
		twentyFourHrsMs = int64(24 * 60 * 60 * 1000)
	)
	rangeMs := toMs - fromMs
	switch {
	case rangeMs > twentyFourHrsMs:
		return "mv_histogram_1h_state", "hour"
	case rangeMs > threeHoursMs:
		return "mv_histogram_5m_state", "minute5"
	default:
		return "mv_histogram_1m_state", "minute"
	}
}

// QueryHistogramMV는 히스토그램 메트릭을 반환한다.
// 쿼리 시간 범위에 따라 1분/5분/1시간 롤업 테이블 중 하나를 자동 선택한다 (데이터 티어링).
// 버킷 분포로부터 P50/P95/P99를 선형 보간으로 계산한다.
// GET /api/collector/histogram?service=svc&name=http.server.duration&from=<ms>&to=<ms>&limit=100
func (s *ClickHouseMetricStore) QueryHistogramMV(ctx context.Context, service, name string, fromMs, toMs int64, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	table, tsCol := histogramTableForRange(fromMs, toMs)

	var conds []string
	var args []any

	if fromMs > 0 {
		conds = append(conds, tsCol+" >= ?")
		args = append(args, time.UnixMilli(fromMs).UTC())
	}
	if toMs > 0 {
		conds = append(conds, tsCol+" <= ?")
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
SELECT service_name, metric_name, %s AS minute,
       sumMerge(count_state)               AS count,
       sumMerge(sum_state)                 AS sum,
       sumForEachMerge(bucket_counts_state) AS bucket_counts,
       anyMerge(bounds_state)              AS bounds
FROM %s.%s
%s
GROUP BY service_name, metric_name, %s
ORDER BY minute DESC
LIMIT %d`, tsCol, s.cfg.Database, table, where, tsCol, limit)

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
			bucketCounts            []uint64
			bounds                  []float64
		)
		if err := rows.Scan(&serviceName, &metricName, &minute, &count, &sum, &bucketCounts, &bounds); err != nil {
			return nil, err
		}
		avg := float64(0)
		if count > 0 {
			avg = sum / float64(count)
		}
		result = append(result, map[string]any{
			"service_name": serviceName,
			"metric_name":  metricName,
			"minute":       minute.UnixMilli(),
			"count":        count,
			"sum":          sum,
			"avg":          avg,
			"p50_ms":       histogramPercentile(50.0, bucketCounts, bounds),
			"p95_ms":       histogramPercentile(95.0, bucketCounts, bounds),
			"p99_ms":       histogramPercentile(99.0, bucketCounts, bounds),
		})
	}
	return result, rows.Err()
}

// histogramPercentile는 명시적 버킷 히스토그램에서 퍼센타일을 선형 보간으로 추정한다.
// Prometheus histogram_quantile()과 동일한 알고리즘을 사용한다.
//
// bucketCounts: 버킷별 카운트 (길이 = len(bounds)+1, 마지막이 overflow)
// bounds: 버킷 상한 경계 (OTel ExplicitBucketHistogram explicit_bounds)
func histogramPercentile(percentile float64, bucketCounts []uint64, bounds []float64) float64 {
	if len(bucketCounts) == 0 || len(bounds) == 0 {
		return 0
	}
	var total uint64
	for _, c := range bucketCounts {
		total += c
	}
	if total == 0 {
		return 0
	}

	target := float64(total) * percentile / 100.0
	cumulative := float64(0)

	for i, bc := range bucketCounts {
		cumulative += float64(bc)
		if cumulative >= target {
			// overflow 버킷 (i >= len(bounds)): 마지막 경계값 반환
			if i >= len(bounds) {
				return bounds[len(bounds)-1]
			}
			upperBound := bounds[i]
			lowerBound := float64(0)
			if i > 0 {
				lowerBound = bounds[i-1]
			}
			if bc == 0 {
				return upperBound
			}
			// 선형 보간: lowerBound + (upperBound - lowerBound) * (target - prevCumul) / bc
			prevCumul := cumulative - float64(bc)
			return lowerBound + (upperBound-lowerBound)*(target-prevCumul)/float64(bc)
		}
	}
	return bounds[len(bounds)-1]
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
				toStringMap(dp.Attributes), m.ServiceName,
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
		 total_count, total_sum, attributes, 
		 exemplar_trace_ids, exemplar_span_ids, exemplar_values, exemplar_times, exemplar_attributes,
		 received_at_ms)`, s.cfg.Database),
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

			var exTraceIDs, exSpanIDs []string
			var exValues []float64
			var exTimes []int64
			var exAttrs []string

			for _, ex := range dp.Exemplars {
				exTraceIDs = append(exTraceIDs, ex.TraceID)
				exSpanIDs = append(exSpanIDs, ex.SpanID)
				exValues = append(exValues, ex.Value)
				exTimes = append(exTimes, ex.TimeNanos)
				exAttrs = append(exAttrs, toJSONString(toStringMap(ex.Attributes)))
			}

			if err := batch.Append(
				m.ServiceName, m.Name, dp.TimeNanos,
				dp.ExplicitBounds, buckets,
				uint64(dp.Count), dp.Sum,
				toStringMap(dp.Attributes),
				exTraceIDs, exSpanIDs, exValues, exTimes, exAttrs,
				m.ReceivedAtMs,
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
