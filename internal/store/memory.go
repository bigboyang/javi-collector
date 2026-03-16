package store

import (
	"context"
	"sync"

	"github.com/kkc/javi-collector/internal/model"
)

// ringBuffer는 용량이 고정된 스레드-안전 링버퍼다.
// 버퍼가 꽉 차면 가장 오래된 항목을 덮어쓴다(자동 드롭).
// head: 다음 쓰기 위치, count: 현재 저장된 항목 수
type ringBuffer[T any] struct {
	mu   sync.RWMutex
	buf  []T
	head int
	size int
}

func newRingBuffer[T any](cap int) *ringBuffer[T] {
	return &ringBuffer[T]{buf: make([]T, cap)}
}

func (r *ringBuffer[T]) push(item T) {
	r.mu.Lock()
	r.buf[r.head] = item
	r.head = (r.head + 1) % len(r.buf)
	if r.size < len(r.buf) {
		r.size++
	}
	r.mu.Unlock()
}

// latest는 최신 n개 항목을 반환한다. n > size인 경우 전체를 반환한다.
func (r *ringBuffer[T]) latest(n int) []T {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cap := len(r.buf)
	if n > r.size {
		n = r.size
	}
	if n == 0 {
		return nil
	}

	result := make([]T, n)
	// tail: 가장 오래된 항목의 인덱스
	// head는 다음에 쓸 위치이므로 head-1이 가장 최근 항목
	tail := (r.head - r.size + cap) % cap

	// 최신 n개는 tail+size-n 위치부터 시작
	start := (tail + r.size - n + cap) % cap
	for i := 0; i < n; i++ {
		result[i] = r.buf[(start+i)%cap]
	}
	return result
}

func (r *ringBuffer[T]) len() int {
	r.mu.RLock()
	n := r.size
	r.mu.RUnlock()
	return n
}

// MemoryTraceStore는 인메모리 링버퍼 기반 TraceStore 구현체다.
// ClickHouse가 없는 개발/테스트 환경에서 fallback으로 사용한다.
type MemoryTraceStore struct {
	buf *ringBuffer[*model.SpanData]
}

func NewMemoryTraceStore(capacity int) *MemoryTraceStore {
	return &MemoryTraceStore{buf: newRingBuffer[*model.SpanData](capacity)}
}

func (s *MemoryTraceStore) AppendSpans(_ context.Context, spans []*model.SpanData) error {
	for _, sp := range spans {
		s.buf.push(sp)
	}
	return nil
}

func (s *MemoryTraceStore) QuerySpans(_ context.Context, limit int) ([]*model.SpanData, error) {
	return s.buf.latest(limit), nil
}

func (s *MemoryTraceStore) Close() error { return nil }

func (s *MemoryTraceStore) Size() int { return s.buf.len() }

// MemoryMetricStore는 인메모리 링버퍼 기반 MetricStore 구현체다.
type MemoryMetricStore struct {
	buf *ringBuffer[*model.MetricData]
}

func NewMemoryMetricStore(capacity int) *MemoryMetricStore {
	return &MemoryMetricStore{buf: newRingBuffer[*model.MetricData](capacity)}
}

func (s *MemoryMetricStore) AppendMetrics(_ context.Context, metrics []*model.MetricData) error {
	for _, m := range metrics {
		s.buf.push(m)
	}
	return nil
}

func (s *MemoryMetricStore) QueryMetrics(_ context.Context, limit int) ([]*model.MetricData, error) {
	return s.buf.latest(limit), nil
}

func (s *MemoryMetricStore) Close() error { return nil }

func (s *MemoryMetricStore) Size() int { return s.buf.len() }

// MemoryLogStore는 인메모리 링버퍼 기반 LogStore 구현체다.
type MemoryLogStore struct {
	buf *ringBuffer[*model.LogData]
}

func NewMemoryLogStore(capacity int) *MemoryLogStore {
	return &MemoryLogStore{buf: newRingBuffer[*model.LogData](capacity)}
}

func (s *MemoryLogStore) AppendLogs(_ context.Context, logs []*model.LogData) error {
	for _, l := range logs {
		s.buf.push(l)
	}
	return nil
}

func (s *MemoryLogStore) QueryLogs(_ context.Context, limit int) ([]*model.LogData, error) {
	return s.buf.latest(limit), nil
}

func (s *MemoryLogStore) Close() error { return nil }

func (s *MemoryLogStore) Size() int { return s.buf.len() }
