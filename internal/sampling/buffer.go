package sampling

import (
	"container/heap"
	"sync"
	"time"

	"github.com/kkc/javi-collector/internal/model"
)

// traceEntry는 하나의 trace에 속하는 spans와 만료 시각을 보관한다.
type traceEntry struct {
	traceID   string
	spans     []*model.SpanData
	expiresAt time.Time
	hasRoot   bool // root span(ParentSpanID=="") 수신 여부
	heapIdx   int  // expiryHeap 내 위치 (heap.Remove용)
}

// expiryHeap은 만료 시각 기준 min-heap이다.
// 가장 빨리 만료되는 trace를 O(log n)으로 꺼낼 수 있다.
type expiryHeap []*traceEntry

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].expiresAt.Before(h[j].expiresAt) }
func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}
func (h *expiryHeap) Push(x any) {
	e := x.(*traceEntry)
	e.heapIdx = len(*h)
	*h = append(*h, e)
}
func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil // GC 대상 처리
	*h = old[:n-1]
	return e
}

// traceBuffer는 진행 중인 trace를 traceId별로 버퍼링한다.
//
// 만료/완성된 trace는 onFlush 콜백으로 방출된다:
//   - root span 수신 후 grace period 경과 시 즉시 flush (tryFlushComplete)
//   - timeout 경과 시 expiryLoop에서 FlushExpired 호출
//   - 버퍼 용량 초과 시 가장 오래된 trace 강제 flush
//   - 프로세스 종료 시 FlushAll → Wait 호출로 잔여 trace 보장 전송
type traceBuffer struct {
	mu      sync.Mutex
	entries map[string]*traceEntry
	expHeap expiryHeap

	maxSize int           // 최대 동시 버퍼 trace 수
	timeout time.Duration // trace 완성 대기 timeout

	// onFlush는 결정이 완료된 spans를 받는다.
	// traceBuffer는 keep/drop 결정을 모른다 — 호출자(TailSamplingStore)가 담당.
	onFlush func(spans []*model.SpanData)

	// wg는 in-flight onFlush goroutine을 추적한다.
	// Wait()로 모든 goroutine 완료를 보장할 수 있다.
	wg sync.WaitGroup
}

func newTraceBuffer(maxSize int, timeout time.Duration, onFlush func([]*model.SpanData)) *traceBuffer {
	return &traceBuffer{
		entries: make(map[string]*traceEntry, min(maxSize, 1024)),
		expHeap: make(expiryHeap, 0, min(maxSize, 1024)),
		maxSize: maxSize,
		timeout: timeout,
		onFlush: onFlush,
	}
}

// append는 spans를 traceID 버킷에 추가한다.
// 버퍼 용량 초과 시 가장 오래된 trace를 강제 flush한다.
func (b *traceBuffer) append(spans []*model.SpanData) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, sp := range spans {
		entry, exists := b.entries[sp.TraceID]
		if !exists {
			entry = &traceEntry{
				traceID:   sp.TraceID,
				spans:     make([]*model.SpanData, 0, 8),
				expiresAt: time.Now().Add(b.timeout),
			}
			b.entries[sp.TraceID] = entry
			heap.Push(&b.expHeap, entry)

			// 버퍼 용량 초과 시 oldest 강제 flush
			if len(b.entries) > b.maxSize {
				b.flushOldest()
			}
		}
		entry.spans = append(entry.spans, sp)
		if sp.ParentSpanID == "" {
			entry.hasRoot = true
		}
	}
}

// tryFlushComplete는 root span을 수신한 trace를 즉시 flush한다.
// Ingester가 AppendSpans 직후 호출해 timeout 대기를 단축한다.
//
// grace period(100ms)를 적용해 같은 배치의 child span이 아직 도착 중인 경우를 흡수한다.
func (b *traceBuffer) tryFlushComplete() {
	b.mu.Lock()
	defer b.mu.Unlock()

	gracePeriod := 100 * time.Millisecond
	now := time.Now()

	for id, entry := range b.entries {
		if !entry.hasRoot {
			continue
		}
		// entry 생성 후 grace period가 지났으면 flush
		entryAge := b.timeout - entry.expiresAt.Sub(now)
		if entryAge >= gracePeriod {
			b.flushEntry(id, entry)
		}
	}
}

// FlushExpired는 만료된 모든 trace를 flush한다.
// expiryLoop에서 1초 주기로 호출한다.
func (b *traceBuffer) FlushExpired() {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()

	for len(b.expHeap) > 0 && b.expHeap[0].expiresAt.Before(now) {
		entry := heap.Pop(&b.expHeap).(*traceEntry)
		if _, exists := b.entries[entry.traceID]; exists {
			b.flushEntryNoHeapRemove(entry.traceID, entry)
		}
	}
}

// flushEntry는 mu를 보유한 상태에서 호출해야 한다.
// heap에서 항목을 제거하고 onFlush를 goroutine으로 실행한다.
func (b *traceBuffer) flushEntry(traceID string, entry *traceEntry) {
	delete(b.entries, traceID)
	if entry.heapIdx < len(b.expHeap) {
		heap.Remove(&b.expHeap, entry.heapIdx)
	}
	spans := entry.spans
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.onFlush(spans)
	}()
}

// flushEntryNoHeapRemove는 heap.Pop으로 이미 heap에서 제거된 entry를 flush할 때 사용한다.
func (b *traceBuffer) flushEntryNoHeapRemove(traceID string, entry *traceEntry) {
	delete(b.entries, traceID)
	spans := entry.spans
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.onFlush(spans)
	}()
}

// flushOldest는 heap의 root(가장 오래된 trace)를 강제 flush한다.
// mu 보유 상태에서 호출한다.
func (b *traceBuffer) flushOldest() {
	if len(b.expHeap) == 0 {
		return
	}
	oldest := heap.Pop(&b.expHeap).(*traceEntry)
	if _, exists := b.entries[oldest.traceID]; exists {
		b.flushEntryNoHeapRemove(oldest.traceID, oldest)
	}
}

// FlushAll은 만료 여부와 관계없이 버퍼에 남은 모든 trace를 즉시 flush한다.
// 프로세스 종료 시 expiryLoop 종료 후 호출해 데이터 유실을 방지한다.
// Wait()를 함께 호출해 모든 in-flight goroutine 완료를 보장해야 한다.
func (b *traceBuffer) FlushAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// heap 초기화 — 이후 entry 접근 시 heap 인덱스 참조를 방지한다.
	b.expHeap = b.expHeap[:0]

	for id, entry := range b.entries {
		delete(b.entries, id)
		spans := entry.spans
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			b.onFlush(spans)
		}()
	}
}

// Wait는 모든 in-flight onFlush goroutine이 완료될 때까지 블록한다.
// FlushAll 호출 후 downstream.Close() 전에 반드시 호출해야 한다.
func (b *traceBuffer) Wait() {
	b.wg.Wait()
}

// Size는 현재 버퍼에 보관 중인 trace 수를 반환한다.
func (b *traceBuffer) Size() int {
	b.mu.Lock()
	n := len(b.entries)
	b.mu.Unlock()
	return n
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
