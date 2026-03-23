package store

// BaselineComputer는 매 interval마다 spans 테이블 28일치를 집계해
// red_baseline 테이블에 upsert(INSERT INTO ReplacingMergeTree)한다.
//
// 집계 기준: (service_name, span_name, http_route, day_of_week, hour_of_day)
// → 요일+시간대별 정상 성능 기준선 (p50/p95/p99 ms, error_rate, avg_rps)
//
// Phase 2(Python Z-score sidecar)는 이 테이블을 읽어 현재 수치와 비교한다.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// BaselineComputer는 RED 기준선 갱신 고루틴을 관리한다.
type BaselineComputer struct {
	conn     driver.Conn
	db       string
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewBaselineComputer는 BaselineComputer를 생성한다.
// interval은 기준선 갱신 주기 (권장: 1h).
func NewBaselineComputer(conn driver.Conn, db string, interval time.Duration) *BaselineComputer {
	return &BaselineComputer{
		conn:     conn,
		db:       db,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start는 백그라운드 고루틴을 시작한다.
// 시작 즉시 첫 번째 집계를 실행하고, 이후 interval마다 반복한다.
func (bc *BaselineComputer) Start() {
	bc.wg.Add(1)
	go bc.run()
}

// Stop은 백그라운드 고루틴을 정지하고 완료를 기다린다.
func (bc *BaselineComputer) Stop() {
	close(bc.stopCh)
	bc.wg.Wait()
}

func (bc *BaselineComputer) run() {
	defer bc.wg.Done()

	// 기동 시 즉시 한 번 실행 (재시작 직후 기준선을 최신화)
	bc.compute()

	ticker := time.NewTicker(bc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bc.compute()
		case <-bc.stopCh:
			return
		}
	}
}

// compute는 spans 테이블 28일치를 집계해 red_baseline에 upsert한다.
//
// INSERT INTO ... SELECT 패턴:
//   - spans에서 오늘 이전 28일치를 읽어 (서비스, 오퍼레이션, 요일, 시간대)로 그룹핑
//   - quantile(0.5/0.95/0.99) → 레이턴시 기준선
//   - error_rate = countIf(status_code=2) / count()
//   - avg_rps = 총 스팬 수 / (관측된 고유 날짜 수 × 3600초)
//   - HAVING sample_count >= 10 → 표본 부족한 오퍼레이션 제외
//
// ReplacingMergeTree(computed_at) 덕분에 동일 키에 대해
// 가장 최근 computed_at을 가진 행만 최종적으로 남는다.
func (bc *BaselineComputer) compute() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	q := fmt.Sprintf(`
INSERT INTO %s.red_baseline
    (service_name, span_name, http_route,
     day_of_week, hour_of_day,
     p50_ms, p95_ms, p99_ms,
     error_rate, avg_rps, sample_count,
     computed_at, dt)
SELECT
    service_name,
    name                                                                    AS span_name,
    http_route,
    toDayOfWeek(fromUnixTimestamp64Nano(start_time_nano))                  AS day_of_week,
    toHour(fromUnixTimestamp64Nano(start_time_nano))                        AS hour_of_day,
    quantile(0.50)(duration_nano) / 1e6                                     AS p50_ms,
    quantile(0.95)(duration_nano) / 1e6                                     AS p95_ms,
    quantile(0.99)(duration_nano) / 1e6                                     AS p99_ms,
    toFloat64(countIf(status_code = 2)) / count()                           AS error_rate,
    toFloat64(count()) /
        (toFloat64(countDistinct(toDate(fromUnixTimestamp64Nano(start_time_nano)))) * 3600.0)
                                                                            AS avg_rps,
    toUInt64(count())                                                        AS sample_count,
    now()                                                                    AS computed_at,
    today()                                                                  AS dt
FROM %s.spans
WHERE kind IN (2, 5)
  AND dt >= today() - 28
  AND dt <  today()
GROUP BY service_name, span_name, http_route, day_of_week, hour_of_day
HAVING sample_count >= 10
`, bc.db, bc.db)

	if err := bc.conn.Exec(ctx, q); err != nil {
		slog.Error("baseline compute failed", "db", bc.db, "err", err)
		return
	}

	slog.Info("red_baseline updated", "db", bc.db, "next_run", time.Now().Add(bc.interval).Format(time.RFC3339))
}
