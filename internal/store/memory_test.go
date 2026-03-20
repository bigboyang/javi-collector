package store

import (
	"context"
	"testing"
	"time"

	"github.com/kkc/javi-collector/internal/model"
)

// ---- ringBuffer 테스트 ----

func TestRingBuffer_PushAndLatest(t *testing.T) {
	rb := newRingBuffer[int](5)

	for i := 1; i <= 3; i++ {
		rb.push(i)
	}

	got := rb.latest(3)
	if len(got) != 3 {
		t.Fatalf("want 3, got %d", len(got))
	}
	for i, v := range []int{1, 2, 3} {
		if got[i] != v {
			t.Errorf("latest[%d]: want %d, got %d", i, v, got[i])
		}
	}
}

func TestRingBuffer_Overflow(t *testing.T) {
	rb := newRingBuffer[int](3)
	// 5개를 넣으면 마지막 3개만 남아야 한다
	for i := 1; i <= 5; i++ {
		rb.push(i)
	}
	got := rb.latest(3)
	if len(got) != 3 {
		t.Fatalf("want 3, got %d", len(got))
	}
	for i, v := range []int{3, 4, 5} {
		if got[i] != v {
			t.Errorf("latest[%d]: want %d, got %d", i, v, got[i])
		}
	}
}

func TestRingBuffer_LatestLessThanSize(t *testing.T) {
	rb := newRingBuffer[int](10)
	for i := 1; i <= 5; i++ {
		rb.push(i)
	}
	got := rb.latest(2)
	if len(got) != 2 {
		t.Fatalf("want 2, got %d", len(got))
	}
	// 최신 2개: 4, 5
	if got[0] != 4 || got[1] != 5 {
		t.Errorf("want [4, 5], got %v", got)
	}
}

func TestRingBuffer_Empty(t *testing.T) {
	rb := newRingBuffer[int](5)
	got := rb.latest(3)
	if len(got) != 0 {
		t.Errorf("empty buffer: want 0, got %d", len(got))
	}
}

func TestRingBuffer_Len(t *testing.T) {
	rb := newRingBuffer[int](5)
	if rb.len() != 0 {
		t.Errorf("initial len: want 0, got %d", rb.len())
	}
	rb.push(1)
	rb.push(2)
	if rb.len() != 2 {
		t.Errorf("after 2 pushes: want 2, got %d", rb.len())
	}
	// 넘치면 cap을 초과하지 않아야 한다
	for i := 0; i < 10; i++ {
		rb.push(i)
	}
	if rb.len() != 5 {
		t.Errorf("overflow: want 5 (cap), got %d", rb.len())
	}
}

// ---- MemoryTraceStore 테스트 ----

func TestMemoryTraceStore_AppendAndQuery(t *testing.T) {
	s := NewMemoryTraceStore(100)
	ctx := context.Background()
	now := time.Now().UnixMilli()

	spans := []*model.SpanData{
		{TraceID: "trace1", SpanID: "span1", ServiceName: "svc-a", StatusCode: 1, StartTimeNano: 1000, EndTimeNano: 2000, ReceivedAtMs: now},
		{TraceID: "trace1", SpanID: "span2", ServiceName: "svc-a", StatusCode: 2, StartTimeNano: 1000, EndTimeNano: 3000, ReceivedAtMs: now},
		{TraceID: "trace2", SpanID: "span3", ServiceName: "svc-b", StatusCode: 1, StartTimeNano: 1000, EndTimeNano: 1500, ReceivedAtMs: now},
	}

	if err := s.AppendSpans(ctx, spans); err != nil {
		t.Fatal(err)
	}

	// 전체 조회
	all, err := s.QuerySpans(ctx, SpanQuery{Limit: 10, StatusCode: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Errorf("want 3 spans, got %d", len(all))
	}

	// 서비스명 필터
	svcA, err := s.QuerySpans(ctx, SpanQuery{Limit: 10, ServiceName: "svc-a", StatusCode: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(svcA) != 2 {
		t.Errorf("svc-a: want 2 spans, got %d", len(svcA))
	}

	// TraceID 필터
	trace1, err := s.QuerySpans(ctx, SpanQuery{Limit: 10, TraceID: "trace1", StatusCode: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(trace1) != 2 {
		t.Errorf("trace1: want 2 spans, got %d", len(trace1))
	}

	// StatusCode 필터 (ERROR=2)
	errSpans, err := s.QuerySpans(ctx, SpanQuery{Limit: 10, StatusCode: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(errSpans) != 1 {
		t.Errorf("error spans: want 1, got %d", len(errSpans))
	}
}

func TestMemoryTraceStore_MinDurationFilter(t *testing.T) {
	s := NewMemoryTraceStore(100)
	ctx := context.Background()
	now := time.Now().UnixMilli()

	spans := []*model.SpanData{
		// duration: 1ms = 1_000_000 ns
		{TraceID: "t1", SpanID: "s1", ServiceName: "svc", StatusCode: -1,
			StartTimeNano: 0, EndTimeNano: 1_000_000, ReceivedAtMs: now},
		// duration: 100ms = 100_000_000 ns
		{TraceID: "t2", SpanID: "s2", ServiceName: "svc", StatusCode: -1,
			StartTimeNano: 0, EndTimeNano: 100_000_000, ReceivedAtMs: now},
		// duration: 500ms = 500_000_000 ns
		{TraceID: "t3", SpanID: "s3", ServiceName: "svc", StatusCode: -1,
			StartTimeNano: 0, EndTimeNano: 500_000_000, ReceivedAtMs: now},
	}
	_ = s.AppendSpans(ctx, spans)

	// MinDurationMs=50 → 100ms, 500ms 두 개만 반환
	result, err := s.QuerySpans(ctx, SpanQuery{Limit: 10, StatusCode: -1, MinDurationMs: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Errorf("MinDurationMs=50: want 2 spans, got %d", len(result))
	}
}

func TestMemoryTraceStore_TimeRangeFilter(t *testing.T) {
	s := NewMemoryTraceStore(100)
	ctx := context.Background()
	now := time.Now().UnixMilli()

	spans := []*model.SpanData{
		{TraceID: "t1", SpanID: "s1", ServiceName: "svc", StatusCode: -1,
			StartTimeNano: 0, EndTimeNano: 0, ReceivedAtMs: now - 1000},
		{TraceID: "t2", SpanID: "s2", ServiceName: "svc", StatusCode: -1,
			StartTimeNano: 0, EndTimeNano: 0, ReceivedAtMs: now},
		{TraceID: "t3", SpanID: "s3", ServiceName: "svc", StatusCode: -1,
			StartTimeNano: 0, EndTimeNano: 0, ReceivedAtMs: now + 1000},
	}
	_ = s.AppendSpans(ctx, spans)

	// now-500ms ~ now+500ms 범위만
	result, err := s.QuerySpans(ctx, SpanQuery{
		Limit:      10,
		StatusCode: -1,
		FromMs:     now - 500,
		ToMs:       now + 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Errorf("time range: want 1 span, got %d", len(result))
	}
}

func TestMemoryTraceStore_Size(t *testing.T) {
	s := NewMemoryTraceStore(100)
	ctx := context.Background()

	if s.Size() != 0 {
		t.Errorf("initial size: want 0, got %d", s.Size())
	}

	_ = s.AppendSpans(ctx, []*model.SpanData{
		{TraceID: "t1", SpanID: "s1"},
		{TraceID: "t2", SpanID: "s2"},
	})

	if s.Size() != 2 {
		t.Errorf("after 2 spans: want 2, got %d", s.Size())
	}
}

func TestMemoryTraceStore_LimitRespected(t *testing.T) {
	s := NewMemoryTraceStore(1000)
	ctx := context.Background()
	now := time.Now().UnixMilli()

	spans := make([]*model.SpanData, 50)
	for i := range spans {
		spans[i] = &model.SpanData{TraceID: "t", SpanID: "s", StatusCode: -1, ReceivedAtMs: now}
	}
	_ = s.AppendSpans(ctx, spans)

	result, err := s.QuerySpans(ctx, SpanQuery{Limit: 10, StatusCode: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) > 10 {
		t.Errorf("limit not respected: want <=10, got %d", len(result))
	}
}

// ---- MemoryMetricStore 테스트 ----

func TestMemoryMetricStore_AppendAndQuery(t *testing.T) {
	s := NewMemoryMetricStore(100)
	ctx := context.Background()
	now := time.Now().UnixMilli()

	metrics := []*model.MetricData{
		{Name: "jvm.heap", Type: model.MetricTypeGauge, ServiceName: "svc-a", ReceivedAtMs: now,
			DataPoints: []model.DataPoint{{Value: 100}}},
		{Name: "http.requests", Type: model.MetricTypeSum, ServiceName: "svc-a", ReceivedAtMs: now,
			DataPoints: []model.DataPoint{{Value: 200}}},
		{Name: "jvm.heap", Type: model.MetricTypeGauge, ServiceName: "svc-b", ReceivedAtMs: now,
			DataPoints: []model.DataPoint{{Value: 300}}},
	}

	if err := s.AppendMetrics(ctx, metrics); err != nil {
		t.Fatal(err)
	}

	// 전체 조회
	all, err := s.QueryMetrics(ctx, MetricQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Errorf("want 3 metrics, got %d", len(all))
	}

	// 서비스명 필터
	svcA, err := s.QueryMetrics(ctx, MetricQuery{Limit: 10, ServiceName: "svc-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(svcA) != 2 {
		t.Errorf("svc-a: want 2 metrics, got %d", len(svcA))
	}

	// 메트릭 이름 필터
	heap, err := s.QueryMetrics(ctx, MetricQuery{Limit: 10, Name: "jvm.heap"})
	if err != nil {
		t.Fatal(err)
	}
	if len(heap) != 2 {
		t.Errorf("jvm.heap: want 2 metrics, got %d", len(heap))
	}

	// 서비스 + 이름 복합 필터
	svcBHeap, err := s.QueryMetrics(ctx, MetricQuery{Limit: 10, ServiceName: "svc-b", Name: "jvm.heap"})
	if err != nil {
		t.Fatal(err)
	}
	if len(svcBHeap) != 1 {
		t.Errorf("svc-b jvm.heap: want 1 metric, got %d", len(svcBHeap))
	}
}

func TestMemoryMetricStore_Size(t *testing.T) {
	s := NewMemoryMetricStore(100)
	ctx := context.Background()

	if s.Size() != 0 {
		t.Errorf("initial size: want 0, got %d", s.Size())
	}

	_ = s.AppendMetrics(ctx, []*model.MetricData{{Name: "m1"}, {Name: "m2"}})

	if s.Size() != 2 {
		t.Errorf("after 2 metrics: want 2, got %d", s.Size())
	}
}

// ---- MemoryLogStore 테스트 ----

func TestMemoryLogStore_AppendAndQuery(t *testing.T) {
	s := NewMemoryLogStore(100)
	ctx := context.Background()
	now := time.Now().UnixMilli()

	logs := []*model.LogData{
		{Body: "app started", SeverityText: "INFO", ServiceName: "svc-a", ReceivedAtMs: now},
		{Body: "null pointer", SeverityText: "ERROR", ServiceName: "svc-a", ReceivedAtMs: now,
			TraceID: "trace-abc"},
		{Body: "disk full", SeverityText: "ERROR", ServiceName: "svc-b", ReceivedAtMs: now},
	}

	if err := s.AppendLogs(ctx, logs); err != nil {
		t.Fatal(err)
	}

	// 전체 조회
	all, err := s.QueryLogs(ctx, LogQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Errorf("want 3 logs, got %d", len(all))
	}

	// Severity 필터
	errLogs, err := s.QueryLogs(ctx, LogQuery{Limit: 10, SeverityText: "ERROR"})
	if err != nil {
		t.Fatal(err)
	}
	if len(errLogs) != 2 {
		t.Errorf("ERROR logs: want 2, got %d", len(errLogs))
	}

	// 서비스명 필터
	svcA, err := s.QueryLogs(ctx, LogQuery{Limit: 10, ServiceName: "svc-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(svcA) != 2 {
		t.Errorf("svc-a logs: want 2, got %d", len(svcA))
	}

	// TraceID 필터
	byTrace, err := s.QueryLogs(ctx, LogQuery{Limit: 10, TraceID: "trace-abc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byTrace) != 1 {
		t.Errorf("trace-abc logs: want 1, got %d", len(byTrace))
	}
}

func TestMemoryLogStore_Size(t *testing.T) {
	s := NewMemoryLogStore(100)
	ctx := context.Background()

	if s.Size() != 0 {
		t.Errorf("initial size: want 0, got %d", s.Size())
	}

	_ = s.AppendLogs(ctx, []*model.LogData{{Body: "l1"}, {Body: "l2"}, {Body: "l3"}})

	if s.Size() != 3 {
		t.Errorf("after 3 logs: want 3, got %d", s.Size())
	}
}

func TestMemoryLogStore_TimeRangeFilter(t *testing.T) {
	s := NewMemoryLogStore(100)
	ctx := context.Background()
	now := time.Now().UnixMilli()

	_ = s.AppendLogs(ctx, []*model.LogData{
		{Body: "old", ReceivedAtMs: now - 2000},
		{Body: "now", ReceivedAtMs: now},
		{Body: "future", ReceivedAtMs: now + 2000},
	})

	result, err := s.QueryLogs(ctx, LogQuery{Limit: 10, FromMs: now - 500, ToMs: now + 500})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Errorf("time range: want 1 log, got %d", len(result))
	}
	if result[0].Body != "now" {
		t.Errorf("time range result: want 'now', got %s", result[0].Body)
	}
}

func TestMemoryLogStore_Close(t *testing.T) {
	s := NewMemoryLogStore(100)
	if err := s.Close(); err != nil {
		t.Errorf("Close should return nil, got %v", err)
	}
}

func TestMemoryTraceStore_Close(t *testing.T) {
	s := NewMemoryTraceStore(100)
	if err := s.Close(); err != nil {
		t.Errorf("Close should return nil, got %v", err)
	}
}

func TestMemoryMetricStore_Close(t *testing.T) {
	s := NewMemoryMetricStore(100)
	if err := s.Close(); err != nil {
		t.Errorf("Close should return nil, got %v", err)
	}
}
