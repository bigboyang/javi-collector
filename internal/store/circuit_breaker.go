// Package store - circuit_breaker.go
//
// circuitBreaker는 연속적인 ClickHouse flush 실패 시 flush를 일시 차단해
// ClickHouse 과부하를 방지하는 간단한 상태 머신이다.
//
// 상태 전이:
//
//	Closed → Open : 연속 실패 횟수가 threshold 이상
//	Open → HalfOpen : cooldown 경과 후
//	HalfOpen → Closed : 다음 flush 성공
//	HalfOpen → Open : 다음 flush 실패
package store

import (
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var chCircuitBreakerOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "javi",
	Subsystem: "clickhouse",
	Name:      "circuit_breaker_open",
	Help:      "1 if the circuit breaker is open (flush blocked), 0 if closed.",
}, []string{"table"})

type cbState int

const (
	cbStateClosed   cbState = iota // 정상: flush 허용
	cbStateOpen                    // 차단: flush 거부 → DLQ로 직행
	cbStateHalfOpen                // 탐색: 한 번만 허용해 복구 여부 확인
)

// circuitBreaker는 ClickHouse flush의 연속 실패 횟수를 추적해 과부하를 방지한다.
// threshold=0 이면 비활성화(항상 허용)된다.
type circuitBreaker struct {
	mu              sync.Mutex
	state           cbState
	failures        int
	lastFailureTime time.Time

	table     string
	threshold int
	cooldown  time.Duration
}

func newCircuitBreaker(table string, threshold int, cooldown time.Duration) *circuitBreaker {
	return &circuitBreaker{
		table:     table,
		threshold: threshold,
		cooldown:  cooldown,
	}
}

// Allow는 flush를 허용할지 반환한다.
// Open 상태에서 cooldown이 지나면 HalfOpen으로 전환하여 한 번의 시도를 허용한다.
func (cb *circuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbStateClosed:
		return true
	case cbStateOpen:
		if time.Since(cb.lastFailureTime) >= cb.cooldown {
			cb.state = cbStateHalfOpen
			slog.Info("clickhouse circuit breaker half-open: probing ClickHouse", "table", cb.table)
			return true
		}
		return false
	case cbStateHalfOpen:
		return true
	}
	return true
}

// RecordSuccess는 flush 성공을 기록하고 Closed 상태로 복원한다.
func (cb *circuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state != cbStateClosed {
		slog.Info("clickhouse circuit breaker closed: ClickHouse recovered", "table", cb.table)
	}
	cb.failures = 0
	cb.state = cbStateClosed
	chCircuitBreakerOpen.WithLabelValues(cb.table).Set(0)
}

// RecordFailure는 flush 실패를 기록한다.
// threshold 초과 또는 HalfOpen 상태이면 Open으로 전환한다.
func (cb *circuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailureTime = time.Now()
	if cb.state == cbStateHalfOpen || cb.failures >= cb.threshold {
		if cb.state != cbStateOpen {
			slog.Warn("clickhouse circuit breaker opened: blocking flush",
				"table", cb.table,
				"consecutive_failures", cb.failures,
				"cooldown", cb.cooldown,
			)
		}
		cb.state = cbStateOpen
		chCircuitBreakerOpen.WithLabelValues(cb.table).Set(1)
	}
}
