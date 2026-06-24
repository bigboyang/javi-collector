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
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
