package ingester

import (
	"context"
	"testing"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/kkc/javi-collector/internal/store"
)

// ---- 헬퍼 ----

func strKV(key, val string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}},
	}
}

func serviceResource(name string) *resourcev1.Resource {
	return &resourcev1.Resource{Attributes: []*commonv1.KeyValue{strKV("service.name", name)}}
}

func newTestIngester() (*Ingester, *store.MemoryTraceStore, *store.MemoryMetricStore, *store.MemoryLogStore) {
	ts := store.NewMemoryTraceStore(1000)
	ms := store.NewMemoryMetricStore(1000)
	ls := store.NewMemoryLogStore(1000)
	ing := New(ts, ms, ls, nil, 0)
	return ing, ts, ms, ls
}

// ---- Trace 인제스트 테스트 ----

func TestIngester_IngestTracesFromProto(t *testing.T) {
	ing, ts, _, _ := newTestIngester()
	ctx := context.Background()

	req := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: serviceResource("checkout"),
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId:           make([]byte, 16),
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "placeOrder",
								StartTimeUnixNano: 1000,
								EndTimeUnixNano:   2000,
								Status:            &tracev1.Status{Code: tracev1.Status_STATUS_CODE_OK},
							},
							{
								TraceId:           make([]byte, 16),
								SpanId:            []byte{2, 3, 4, 5, 6, 7, 8, 9},
								Name:              "validateCart",
								StartTimeUnixNano: 1000,
								EndTimeUnixNano:   1500,
								Status:            &tracev1.Status{Code: tracev1.Status_STATUS_CODE_ERROR},
							},
						},
					},
				},
			},
		},
	}

	count, err := ing.IngestTracesFromProto(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("count: want 2, got %d", count)
	}
	if ing.TraceReceived() != 2 {
		t.Errorf("TraceReceived: want 2, got %d", ing.TraceReceived())
	}

	// 스토어에서 조회
	spans, err := ts.QuerySpans(ctx, store.SpanQuery{Limit: 10, StatusCode: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 2 {
		t.Fatalf("store has %d spans, want 2", len(spans))
	}
}

func TestIngester_IngestTraces_HTTP_Proto(t *testing.T) {
	ing, ts, _, _ := newTestIngester()
	ctx := context.Background()

	req := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: serviceResource("inventory"),
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId:           make([]byte, 16),
								SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 0},
								Name:              "getStock",
								StartTimeUnixNano: 100,
								EndTimeUnixNano:   200,
								Status:            &tracev1.Status{Code: tracev1.Status_STATUS_CODE_OK},
							},
						},
					},
				},
			},
		},
	}

	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	count, err := ing.IngestTraces(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("count: want 1, got %d", count)
	}

	spans, err := ts.QuerySpans(ctx, store.SpanQuery{Limit: 10, StatusCode: -1})
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 {
		t.Fatalf("store has %d spans, want 1", len(spans))
	}
	if spans[0].ServiceName != "inventory" {
		t.Errorf("ServiceName: want inventory, got %s", spans[0].ServiceName)
	}
	if spans[0].Name != "getStock" {
		t.Errorf("Name: want getStock, got %s", spans[0].Name)
	}
}

func TestIngester_IngestTraces_InvalidProto(t *testing.T) {
	ing, _, _, _ := newTestIngester()
	ctx := context.Background()

	_, err := ing.IngestTraces(ctx, []byte{0xFF, 0xFE, 0xFD})
	if err == nil {
		t.Error("expected error for invalid protobuf")
	}
}

func TestIngester_IngestTraces_Empty(t *testing.T) {
	ing, _, _, _ := newTestIngester()
	ctx := context.Background()

	// 빈 요청 (ResourceSpans 없음)
	req := &collectortracev1.ExportTraceServiceRequest{}
	b, _ := proto.Marshal(req)

	count, err := ing.IngestTraces(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("empty request: want 0, got %d", count)
	}
	if ing.TraceReceived() != 0 {
		t.Errorf("TraceReceived should be 0, got %d", ing.TraceReceived())
	}
}

// ---- Metric 인제스트 테스트 ----

func TestIngester_IngestMetricsFromProto(t *testing.T) {
	ing, _, ms, _ := newTestIngester()
	ctx := context.Background()

	req := &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: serviceResource("api"),
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name: "jvm.heap.used",
								Data: &metricsv1.Metric_Gauge{
									Gauge: &metricsv1.Gauge{
										DataPoints: []*metricsv1.NumberDataPoint{
											{TimeUnixNano: 1000, Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 52428800}},
										},
									},
								},
							},
							{
								Name: "http.server.requests",
								Data: &metricsv1.Metric_Sum{
									Sum: &metricsv1.Sum{
										DataPoints: []*metricsv1.NumberDataPoint{
											{TimeUnixNano: 1000, Value: &metricsv1.NumberDataPoint_AsInt{AsInt: 1234}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	count, err := ing.IngestMetricsFromProto(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("count: want 2, got %d", count)
	}
	if ing.MetricReceived() != 2 {
		t.Errorf("MetricReceived: want 2, got %d", ing.MetricReceived())
	}

	metrics, err := ms.QueryMetrics(ctx, store.MetricQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 2 {
		t.Fatalf("store has %d metrics, want 2", len(metrics))
	}
}

func TestIngester_IngestMetrics_HTTP_Proto(t *testing.T) {
	ing, _, ms, _ := newTestIngester()
	ctx := context.Background()

	req := &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: serviceResource("worker"),
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name: "cpu.usage",
								Data: &metricsv1.Metric_Gauge{
									Gauge: &metricsv1.Gauge{
										DataPoints: []*metricsv1.NumberDataPoint{
											{TimeUnixNano: 5000, Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 0.75}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	b, _ := proto.Marshal(req)
	count, err := ing.IngestMetrics(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("count: want 1, got %d", count)
	}

	metrics, err := ms.QueryMetrics(ctx, store.MetricQuery{Limit: 10, Name: "cpu.usage"})
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 {
		t.Fatalf("store has %d metrics, want 1", len(metrics))
	}
	if metrics[0].DataPoints[0].Value != 0.75 {
		t.Errorf("Value: want 0.75, got %f", metrics[0].DataPoints[0].Value)
	}
}

func TestIngester_IngestMetrics_Histogram(t *testing.T) {
	ing, _, ms, _ := newTestIngester()
	ctx := context.Background()

	bounds := []float64{5, 10, 25, 50}
	counts := []uint64{1, 2, 5, 10, 3}

	req := &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: serviceResource("frontend"),
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name: "http.server.duration",
								Data: &metricsv1.Metric_Histogram{
									Histogram: &metricsv1.Histogram{
										DataPoints: []*metricsv1.HistogramDataPoint{
											{
												TimeUnixNano:   1000,
												Count:          21,
												Sum:            func() *float64 { v := 525.0; return &v }(),
												ExplicitBounds: bounds,
												BucketCounts:   counts,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	count, err := ing.IngestMetricsFromProto(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("count: want 1, got %d", count)
	}

	metrics, err := ms.QueryMetrics(ctx, store.MetricQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 {
		t.Fatalf("store has %d metrics, want 1", len(metrics))
	}

	m := metrics[0]
	if len(m.DataPoints) != 1 {
		t.Fatalf("want 1 data point, got %d", len(m.DataPoints))
	}
	dp := m.DataPoints[0]
	if dp.Count != 21 {
		t.Errorf("Count: want 21, got %d", dp.Count)
	}
	if dp.Sum != 525.0 {
		t.Errorf("Sum: want 525, got %f", dp.Sum)
	}
	if len(dp.BucketCounts) != len(counts) {
		t.Errorf("BucketCounts len: want %d, got %d", len(counts), len(dp.BucketCounts))
	}
}

// ---- Log 인제스트 테스트 ----

func TestIngester_IngestLogsFromProto(t *testing.T) {
	ing, _, _, ls := newTestIngester()
	ctx := context.Background()

	req := &collectorlogsv1.ExportLogsServiceRequest{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				Resource: serviceResource("auth"),
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							{
								SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR,
								SeverityText:   "ERROR",
								Body:           &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "login failed"}},
							},
							{
								SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_INFO,
								SeverityText:   "INFO",
								Body:           &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "user logged in"}},
							},
						},
					},
				},
			},
		},
	}

	count, err := ing.IngestLogsFromProto(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("count: want 2, got %d", count)
	}
	if ing.LogReceived() != 2 {
		t.Errorf("LogReceived: want 2, got %d", ing.LogReceived())
	}

	logs, err := ls.QueryLogs(ctx, store.LogQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 {
		t.Fatalf("store has %d logs, want 2", len(logs))
	}
}

func TestIngester_IngestLogs_HTTP_Proto(t *testing.T) {
	ing, _, _, ls := newTestIngester()
	ctx := context.Background()

	req := &collectorlogsv1.ExportLogsServiceRequest{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				Resource: serviceResource("backend"),
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							{
								SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_WARN,
								SeverityText:   "WARN",
								Body:           &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "disk 80% full"}},
							},
						},
					},
				},
			},
		},
	}

	b, _ := proto.Marshal(req)
	count, err := ing.IngestLogs(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("count: want 1, got %d", count)
	}

	logs, err := ls.QueryLogs(ctx, store.LogQuery{Limit: 10, SeverityText: "WARN"})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("store has %d WARN logs, want 1", len(logs))
	}
	if logs[0].Body != "disk 80% full" {
		t.Errorf("Body: want 'disk 80%% full', got %s", logs[0].Body)
	}
}

func TestIngester_IngestLogs_InvalidProto(t *testing.T) {
	ing, _, _, _ := newTestIngester()
	ctx := context.Background()

	_, err := ing.IngestLogs(ctx, []byte{0xFF, 0xFE})
	if err == nil {
		t.Error("expected error for invalid protobuf")
	}
}

// ---- 통계 카운터 테스트 ----

func TestIngester_Counters_CumulativeIncrements(t *testing.T) {
	ing, _, _, _ := newTestIngester()
	ctx := context.Background()

	// 초기값
	if ing.TraceReceived() != 0 || ing.MetricReceived() != 0 || ing.LogReceived() != 0 {
		t.Error("initial counters should be 0")
	}

	// 트레이스 인제스트
	traceReq := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: serviceResource("svc"),
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{TraceId: make([]byte, 16), SpanId: []byte{1, 0, 0, 0, 0, 0, 0, 0}},
							{TraceId: make([]byte, 16), SpanId: []byte{2, 0, 0, 0, 0, 0, 0, 0}},
							{TraceId: make([]byte, 16), SpanId: []byte{3, 0, 0, 0, 0, 0, 0, 0}},
						},
					},
				},
			},
		},
	}
	_, _ = ing.IngestTracesFromProto(ctx, traceReq)

	if ing.TraceReceived() != 3 {
		t.Errorf("TraceReceived: want 3, got %d", ing.TraceReceived())
	}

	// 메트릭 인제스트
	metricReq := &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: serviceResource("svc"),
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{Name: "m1", Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{
								DataPoints: []*metricsv1.NumberDataPoint{{Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 1}}},
							}}},
						},
					},
				},
			},
		},
	}
	_, _ = ing.IngestMetricsFromProto(ctx, metricReq)

	if ing.MetricReceived() != 1 {
		t.Errorf("MetricReceived: want 1, got %d", ing.MetricReceived())
	}

	// 로그 인제스트
	logReq := &collectorlogsv1.ExportLogsServiceRequest{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				Resource: serviceResource("svc"),
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							{Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "l1"}}},
							{Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "l2"}}},
						},
					},
				},
			},
		},
	}
	_, _ = ing.IngestLogsFromProto(ctx, logReq)

	if ing.LogReceived() != 2 {
		t.Errorf("LogReceived: want 2, got %d", ing.LogReceived())
	}
}

// ---- 파이프라인 통합 테스트: gRPC 경로 vs HTTP 경로 ----

func TestIngester_gRPCPath_vs_HTTPPath_ProduceEquivalentData(t *testing.T) {
	ingHTTP, tsHTTP, _, _ := newTestIngester()
	ingGRPC, tsGRPC, _, _ := newTestIngester()
	ctx := context.Background()

	req := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: serviceResource("payment"),
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Scope: &commonv1.InstrumentationScope{Name: "payment-lib"},
						Spans: []*tracev1.Span{
							{
								TraceId:           []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
								SpanId:            []byte{2, 2, 2, 2, 2, 2, 2, 2},
								Name:              "charge",
								Kind:              tracev1.Span_SPAN_KIND_CLIENT,
								StartTimeUnixNano: 1_000_000_000,
								EndTimeUnixNano:   2_000_000_000,
								Status:            &tracev1.Status{Code: tracev1.Status_STATUS_CODE_OK, Message: "charged"},
								Attributes: []*commonv1.KeyValue{
									strKV("currency", "USD"),
								},
							},
						},
					},
				},
			},
		},
	}

	// HTTP 경로
	b, _ := proto.Marshal(req)
	_, _ = ingHTTP.IngestTraces(ctx, b)

	// gRPC 경로
	_, _ = ingGRPC.IngestTracesFromProto(ctx, req)

	// 두 경로 모두 동일한 수의 span을 저장해야 한다
	httpSpans, _ := tsHTTP.QuerySpans(ctx, store.SpanQuery{Limit: 10, StatusCode: -1})
	grpcSpans, _ := tsGRPC.QuerySpans(ctx, store.SpanQuery{Limit: 10, StatusCode: -1})

	if len(httpSpans) != len(grpcSpans) {
		t.Errorf("span count: http=%d, grpc=%d", len(httpSpans), len(grpcSpans))
	}
	if len(httpSpans) == 0 {
		t.Fatal("no spans stored")
	}

	hs, gs := httpSpans[0], grpcSpans[0]
	if hs.TraceID != gs.TraceID {
		t.Errorf("TraceID: http=%s, grpc=%s", hs.TraceID, gs.TraceID)
	}
	if hs.Name != gs.Name {
		t.Errorf("Name: http=%s, grpc=%s", hs.Name, gs.Name)
	}
	if hs.ServiceName != gs.ServiceName {
		t.Errorf("ServiceName: http=%s, grpc=%s", hs.ServiceName, gs.ServiceName)
	}
	if hs.StatusCode != gs.StatusCode {
		t.Errorf("StatusCode: http=%d, grpc=%d", hs.StatusCode, gs.StatusCode)
	}
	if hs.Kind != gs.Kind {
		t.Errorf("Kind: http=%d, grpc=%d", hs.Kind, gs.Kind)
	}
}
