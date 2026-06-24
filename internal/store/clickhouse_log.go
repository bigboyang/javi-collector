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

// ClickHouseLogStore는 LogData를 apm.logs 테이블에 배치 insert한다.
type ClickHouseLogStore struct {
	conn    driver.Conn           // 공유 커넥션 풀 (소유권 없음)
	ch      chan *model.LogData   // 수신 데이터 채널
	flushCh chan []*model.LogData // 조립된 배치를 flush worker에 전달하는 큐
	cfg     ClickHouseConfig
	dynCfg  atomic.Pointer[storeDynCfg] // 핫 리로드 가능한 배치 설정
	done    chan struct{}               // 모든 flushWorker 종료 시 닫힘
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

	// 로그 MV 스키마 마이그레이션 (1시간 에러 로그 롤업 테이블 추가)
	migrator := NewMigrator(conn, cfg.Database)
	if err := migrator.Run(context.Background(), BuildLogsMigrations(cfg.Database)); err != nil {
		slog.Warn("logs schema migration failed", "err", err)
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
			attrsMap map[string]string
		)
		if err := rows.Scan(
			&l.SeverityText, &l.SeverityNumber, &l.Body, &attrsMap,
			&l.ServiceName, &l.TraceID, &l.SpanID,
			&l.TimestampNanos, &l.ReceivedAtMs,
		); err != nil {
			return nil, err
		}
		l.Attributes = fromStringMap(attrsMap)
		result = append(result, &l)
	}
	return result, rows.Err()
}

// errorLogTableForRange는 쿼리 시간 범위에 따라 적절한 에러 로그 롤업 테이블과 타임스탬프 컬럼명을 반환한다.
//
// 티어링 기준:
//   - < 24h:  1분 집계 (mv_error_logs_1m_state, "minute")
//   - >= 24h: 1시간 집계 (mv_error_logs_1h_state, "hour")
func errorLogTableForRange(fromMs, toMs int64) (table, tsCol string) {
	if fromMs <= 0 || toMs <= 0 {
		return "mv_error_logs_1m_state", "minute"
	}
	const twentyFourHrsMs = int64(24 * 60 * 60 * 1000)
	if toMs-fromMs >= twentyFourHrsMs {
		return "mv_error_logs_1h_state", "hour"
	}
	return "mv_error_logs_1m_state", "minute"
}

// QueryErrorLogs는 서비스별 에러 로그 집계를 반환한다.
// 쿼리 시간 범위에 따라 1분/1시간 롤업 테이블 중 하나를 자동 선택한다 (데이터 티어링).
func (s *ClickHouseLogStore) QueryErrorLogs(ctx context.Context, service string, fromMs, toMs int64) ([]map[string]any, error) {
	table, tsCol := errorLogTableForRange(fromMs, toMs)

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
    exception_type,
    %s AS minute,
    sum(error_count) AS error_count
FROM %s.%s
%s
GROUP BY service_name, exception_type, %s
ORDER BY minute DESC, error_count DESC
LIMIT 500`, tsCol, s.cfg.Database, table, where, tsCol)

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
			toStringMap(attrs), l.ServiceName,
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
