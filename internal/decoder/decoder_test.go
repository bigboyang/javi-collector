package decoder

import (
	"encoding/hex"
	"testing"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/kkc/javi-collector/internal/model"
)

// ---- 헬퍼 ----

func strKV(key, val string) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}},
	}
}

func intKV(key string, val int64) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: val}},
	}
}

func serviceResource(name string) *resourcev1.Resource {
	return &resourcev1.Resource{Attributes: []*commonv1.KeyValue{strKV("service.name", name)}}
}

// ---- Trace 디코딩 테스트 ----

func TestDecodeTraces_Basic(t *testing.T) {
	traceIDBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanIDBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	parentSpanIDBytes := []byte{8, 7, 6, 5, 4, 3, 2, 1}

	req := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: serviceResource("order-service"),
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Scope: &commonv1.InstrumentationScope{Name: "io.opentelemetry"},
						Spans: []*tracev1.Span{
							{
								TraceId:           traceIDBytes,
								SpanId:            spanIDBytes,
								ParentSpanId:      parentSpanIDBytes,
								Name:              "GET /orders",
								Kind:              tracev1.Span_SPAN_KIND_SERVER,
								StartTimeUnixNano: 1_000_000_000,
								EndTimeUnixNano:   1_500_000_000,
								Status: &tracev1.Status{
									Code:    tracev1.Status_STATUS_CODE_OK,
									Message: "success",
								},
								Attributes: []*commonv1.KeyValue{
									strKV("http.route", "/orders"),
									intKV("http.response.status_code", 200),
								},
							},
						},
					},
				},
			},
		},
	}

	b, err := proto.Marshal(req)
	if err != nil {
		t.Fatal("proto marshal:", err)
	}

	spans, err := DecodeTraces(b)
	if err != nil {
		t.Fatal("DecodeTraces:", err)
	}

	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}

	sp := spans[0]

	if want := hex.EncodeToString(traceIDBytes); sp.TraceID != want {
		t.Errorf("TraceID: want %s, got %s", want, sp.TraceID)
	}
	if want := hex.EncodeToString(spanIDBytes); sp.SpanID != want {
		t.Errorf("SpanID: want %s, got %s", want, sp.SpanID)
	}
	if want := hex.EncodeToString(parentSpanIDBytes); sp.ParentSpanID != want {
		t.Errorf("ParentSpanID: want %s, got %s", want, sp.ParentSpanID)
	}
	if sp.Name != "GET /orders" {
		t.Errorf("Name: want GET /orders, got %s", sp.Name)
	}
	if sp.Kind != int32(tracev1.Span_SPAN_KIND_SERVER) {
		t.Errorf("Kind: want %d, got %d", tracev1.Span_SPAN_KIND_SERVER, sp.Kind)
	}
	if sp.StartTimeNano != 1_000_000_000 {
		t.Errorf("StartTimeNano: want 1000000000, got %d", sp.StartTimeNano)
	}
	if sp.EndTimeNano != 1_500_000_000 {
		t.Errorf("EndTimeNano: want 1500000000, got %d", sp.EndTimeNano)
	}
	if sp.DurationNano() != 500_000_000 {
		t.Errorf("DurationNano: want 500000000, got %d", sp.DurationNano())
	}
	if sp.StatusCode != int32(tracev1.Status_STATUS_CODE_OK) {
		t.Errorf("StatusCode: want 1(OK), got %d", sp.StatusCode)
	}
	if sp.StatusMessage != "success" {
		t.Errorf("StatusMessage: want success, got %s", sp.StatusMessage)
	}
	if sp.ServiceName != "order-service" {
		t.Errorf("ServiceName: want order-service, got %s", sp.ServiceName)
	}
	if sp.ScopeName != "io.opentelemetry" {
		t.Errorf("ScopeName: want io.opentelemetry, got %s", sp.ScopeName)
	}
	if sp.ReceivedAtMs <= 0 {
		t.Error("ReceivedAtMs should be set")
	}
	if sp.Attributes["http.route"] != "/orders" {
		t.Errorf("Attributes[http.route]: want /orders, got %v", sp.Attributes["http.route"])
	}
}

func TestDecodeTraces_ErrorStatus(t *testing.T) {
	req := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: serviceResource("payment-service"),
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId: make([]byte, 16),
								SpanId:  make([]byte, 8),
								Name:    "processPayment",
								Status: &tracev1.Status{
									Code:    tracev1.Status_STATUS_CODE_ERROR,
									Message: "NullPointerException",
								},
							},
						},
					},
				},
			},
		},
	}

	b, _ := proto.Marshal(req)
	spans, err := DecodeTraces(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(spans))
	}
	if spans[0].StatusCode != 2 {
		t.Errorf("StatusCode: want 2(ERROR), got %d", spans[0].StatusCode)
	}
	if spans[0].StatusMessage != "NullPointerException" {
		t.Errorf("StatusMessage: want NullPointerException, got %s", spans[0].StatusMessage)
	}
}

func TestDecodeTraces_MultipleServicesAndSpans(t *testing.T) {
	req := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: serviceResource("svc-a"),
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "span-a1"},
							{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "span-a2"},
						},
					},
				},
			},
			{
				Resource: serviceResource("svc-b"),
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "span-b1"},
						},
					},
				},
			},
		},
	}

	b, _ := proto.Marshal(req)
	spans, err := DecodeTraces(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 3 {
		t.Fatalf("want 3 spans, got %d", len(spans))
	}

	svcCount := map[string]int{}
	for _, sp := range spans {
		svcCount[sp.ServiceName]++
	}
	if svcCount["svc-a"] != 2 {
		t.Errorf("svc-a: want 2 spans, got %d", svcCount["svc-a"])
	}
	if svcCount["svc-b"] != 1 {
		t.Errorf("svc-b: want 1 span, got %d", svcCount["svc-b"])
	}
}

func TestDecodeTraces_NoResourceServiceName(t *testing.T) {
	req := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{strKV("host.name", "myhost")}},
				ScopeSpans: []*tracev1.ScopeSpans{
					{Spans: []*tracev1.Span{{TraceId: make([]byte, 16), SpanId: make([]byte, 8), Name: "op"}}},
				},
			},
		},
	}

	b, _ := proto.Marshal(req)
	spans, err := DecodeTraces(b)
	if err != nil {
		t.Fatal(err)
	}
	if spans[0].ServiceName != "" {
		t.Errorf("ServiceName should be empty, got %q", spans[0].ServiceName)
	}
}

func TestDecodeTraces_InvalidProto(t *testing.T) {
	_, err := DecodeTraces([]byte{0xFF, 0xFE, 0xFD})
	if err == nil {
		t.Error("expected error for invalid protobuf bytes")
	}
}

func TestDecodeTracesFromRequest_SameAsDecodeTraces(t *testing.T) {
	req := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: serviceResource("svc"),
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Scope: &commonv1.InstrumentationScope{Name: "scope"},
						Spans: []*tracev1.Span{
							{
								TraceId:           make([]byte, 16),
								SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
								Name:              "test-span",
								StartTimeUnixNano: 100,
								EndTimeUnixNano:   200,
							},
						},
					},
				},
			},
		},
	}

	// HTTP 경로
	b, _ := proto.Marshal(req)
	fromBytes, err := DecodeTraces(b)
	if err != nil {
		t.Fatal(err)
	}

	// gRPC 경로
	fromProto := DecodeTracesFromRequest(req)

	if len(fromBytes) != len(fromProto) {
		t.Fatalf("span count mismatch: bytes=%d, proto=%d", len(fromBytes), len(fromProto))
	}

	sp1, sp2 := fromBytes[0], fromProto[0]
	if sp1.TraceID != sp2.TraceID {
		t.Errorf("TraceID mismatch: %s vs %s", sp1.TraceID, sp2.TraceID)
	}
	if sp1.SpanID != sp2.SpanID {
		t.Errorf("SpanID mismatch: %s vs %s", sp1.SpanID, sp2.SpanID)
	}
	if sp1.Name != sp2.Name {
		t.Errorf("Name mismatch: %s vs %s", sp1.Name, sp2.Name)
	}
	if sp1.ServiceName != sp2.ServiceName {
		t.Errorf("ServiceName mismatch: %s vs %s", sp1.ServiceName, sp2.ServiceName)
	}
}

// ---- Metric 디코딩 테스트 ----

func TestDecodeMetrics_Gauge(t *testing.T) {
	req := &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: serviceResource("api-server"),
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name:        "jvm.memory.used",
								Description: "JVM heap used bytes",
								Unit:        "By",
								Data: &metricsv1.Metric_Gauge{
									Gauge: &metricsv1.Gauge{
										DataPoints: []*metricsv1.NumberDataPoint{
											{
												Attributes:  []*commonv1.KeyValue{strKV("area", "heap")},
												TimeUnixNano: 1_000_000_000,
												Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 52428800},
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

	b, _ := proto.Marshal(req)
	metrics, err := DecodeMetrics(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 {
		t.Fatalf("want 1 metric, got %d", len(metrics))
	}

	m := metrics[0]
	if m.Name != "jvm.memory.used" {
		t.Errorf("Name: want jvm.memory.used, got %s", m.Name)
	}
	if m.Type != model.MetricTypeGauge {
		t.Errorf("Type: want GAUGE, got %s", m.Type)
	}
	if m.ServiceName != "api-server" {
		t.Errorf("ServiceName: want api-server, got %s", m.ServiceName)
	}
	if len(m.DataPoints) != 1 {
		t.Fatalf("want 1 data point, got %d", len(m.DataPoints))
	}
	if m.DataPoints[0].Value != 52428800 {
		t.Errorf("Value: want 52428800, got %f", m.DataPoints[0].Value)
	}
	if m.DataPoints[0].Attributes["area"] != "heap" {
		t.Errorf("Attributes[area]: want heap, got %v", m.DataPoints[0].Attributes["area"])
	}
}

func TestDecodeMetrics_Sum(t *testing.T) {
	req := &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: serviceResource("worker"),
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name: "http.server.requests",
								Data: &metricsv1.Metric_Sum{
									Sum: &metricsv1.Sum{
										DataPoints: []*metricsv1.NumberDataPoint{
											{
												TimeUnixNano: 2_000_000_000,
												Value:        &metricsv1.NumberDataPoint_AsInt{AsInt: 1234},
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

	b, _ := proto.Marshal(req)
	metrics, err := DecodeMetrics(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 {
		t.Fatalf("want 1 metric, got %d", len(metrics))
	}
	if metrics[0].Type != model.MetricTypeSum {
		t.Errorf("Type: want SUM, got %s", metrics[0].Type)
	}
	if metrics[0].DataPoints[0].Value != 1234 {
		t.Errorf("Value: want 1234, got %f", metrics[0].DataPoints[0].Value)
	}
}

func TestDecodeMetrics_Histogram(t *testing.T) {
	bounds := []float64{5, 10, 25, 50, 100}
	counts := []uint64{1, 2, 5, 10, 20, 5}

	req := &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: serviceResource("frontend"),
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name: "http.server.request.duration",
								Data: &metricsv1.Metric_Histogram{
									Histogram: &metricsv1.Histogram{
										DataPoints: []*metricsv1.HistogramDataPoint{
											{
												TimeUnixNano:   3_000_000_000,
												Count:          43,
												Sum:            func() *float64 { v := 2150.0; return &v }(),
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

	b, _ := proto.Marshal(req)
	metrics, err := DecodeMetrics(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 {
		t.Fatalf("want 1 metric, got %d", len(metrics))
	}
	m := metrics[0]
	if m.Type != model.MetricTypeHistogram {
		t.Errorf("Type: want HISTOGRAM, got %s", m.Type)
	}
	dp := m.DataPoints[0]
	if dp.Count != 43 {
		t.Errorf("Count: want 43, got %d", dp.Count)
	}
	if dp.Sum != 2150.0 {
		t.Errorf("Sum: want 2150, got %f", dp.Sum)
	}
	if len(dp.BucketCounts) != len(counts) {
		t.Errorf("BucketCounts len: want %d, got %d", len(counts), len(dp.BucketCounts))
	}
	for i, c := range counts {
		if dp.BucketCounts[i] != c {
			t.Errorf("BucketCounts[%d]: want %d, got %d", i, c, dp.BucketCounts[i])
		}
	}
	if len(dp.ExplicitBounds) != len(bounds) {
		t.Errorf("ExplicitBounds len: want %d, got %d", len(bounds), len(dp.ExplicitBounds))
	}
}

func TestDecodeMetrics_MultipleDataPoints(t *testing.T) {
	req := &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: serviceResource("svc"),
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name: "cpu.usage",
								Data: &metricsv1.Metric_Gauge{
									Gauge: &metricsv1.Gauge{
										DataPoints: []*metricsv1.NumberDataPoint{
											{TimeUnixNano: 1000, Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 0.1}},
											{TimeUnixNano: 2000, Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 0.2}},
											{TimeUnixNano: 3000, Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 0.3}},
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
	metrics, err := DecodeMetrics(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 {
		t.Fatalf("want 1 metric, got %d", len(metrics))
	}
	if len(metrics[0].DataPoints) != 3 {
		t.Errorf("want 3 data points, got %d", len(metrics[0].DataPoints))
	}
}

func TestDecodeMetricsFromRequest_SameAsDecodeMetrics(t *testing.T) {
	req := &collectormetricsv1.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricsv1.ResourceMetrics{
			{
				Resource: serviceResource("svc"),
				ScopeMetrics: []*metricsv1.ScopeMetrics{
					{
						Metrics: []*metricsv1.Metric{
							{
								Name: "gauge.test",
								Data: &metricsv1.Metric_Gauge{
									Gauge: &metricsv1.Gauge{
										DataPoints: []*metricsv1.NumberDataPoint{
											{TimeUnixNano: 1000, Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 42.0}},
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
	fromBytes, err := DecodeMetrics(b)
	if err != nil {
		t.Fatal(err)
	}
	fromProto := DecodeMetricsFromRequest(req)

	if len(fromBytes) != len(fromProto) {
		t.Fatalf("metric count mismatch: bytes=%d, proto=%d", len(fromBytes), len(fromProto))
	}
	if fromBytes[0].Name != fromProto[0].Name {
		t.Errorf("Name mismatch: %s vs %s", fromBytes[0].Name, fromProto[0].Name)
	}
	if fromBytes[0].ServiceName != fromProto[0].ServiceName {
		t.Errorf("ServiceName mismatch: %s vs %s", fromBytes[0].ServiceName, fromProto[0].ServiceName)
	}
	if fromBytes[0].DataPoints[0].Value != fromProto[0].DataPoints[0].Value {
		t.Errorf("Value mismatch: %f vs %f", fromBytes[0].DataPoints[0].Value, fromProto[0].DataPoints[0].Value)
	}
}

// ---- Log 디코딩 테스트 ----

func TestDecodeLogs_Basic(t *testing.T) {
	traceIDBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanIDBytes := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18}

	req := &collectorlogsv1.ExportLogsServiceRequest{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				Resource: serviceResource("auth-service"),
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						Scope: &commonv1.InstrumentationScope{Name: "com.example"},
						LogRecords: []*logsv1.LogRecord{
							{
								TimeUnixNano:   5_000_000_000,
								SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR,
								SeverityText:   "ERROR",
								Body: &commonv1.AnyValue{
									Value: &commonv1.AnyValue_StringValue{StringValue: "Login failed"},
								},
								TraceId: traceIDBytes,
								SpanId:  spanIDBytes,
								Attributes: []*commonv1.KeyValue{
									strKV("exception.type", "AuthenticationException"),
								},
							},
						},
					},
				},
			},
		},
	}

	b, _ := proto.Marshal(req)
	logs, err := DecodeLogs(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("want 1 log, got %d", len(logs))
	}

	l := logs[0]
	if l.Body != "Login failed" {
		t.Errorf("Body: want 'Login failed', got %s", l.Body)
	}
	if l.SeverityNumber != int32(logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR) {
		t.Errorf("SeverityNumber: want %d, got %d", logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR, l.SeverityNumber)
	}
	if l.SeverityText != "ERROR" {
		t.Errorf("SeverityText: want ERROR, got %s", l.SeverityText)
	}
	if l.ServiceName != "auth-service" {
		t.Errorf("ServiceName: want auth-service, got %s", l.ServiceName)
	}
	if l.ScopeName != "com.example" {
		t.Errorf("ScopeName: want com.example, got %s", l.ScopeName)
	}
	if want := hex.EncodeToString(traceIDBytes); l.TraceID != want {
		t.Errorf("TraceID: want %s, got %s", want, l.TraceID)
	}
	if want := hex.EncodeToString(spanIDBytes); l.SpanID != want {
		t.Errorf("SpanID: want %s, got %s", want, l.SpanID)
	}
	if l.TimestampNanos != 5_000_000_000 {
		t.Errorf("TimestampNanos: want 5000000000, got %d", l.TimestampNanos)
	}
	if l.ReceivedAtMs <= 0 {
		t.Error("ReceivedAtMs should be set")
	}
	if l.Attributes["exception.type"] != "AuthenticationException" {
		t.Errorf("Attributes[exception.type]: want AuthenticationException, got %v", l.Attributes["exception.type"])
	}
}

func TestDecodeLogs_SeverityLabel(t *testing.T) {
	cases := []struct {
		severityNumber int32
		wantLabel      string
	}{
		{0, "UNSPECIFIED"},
		{1, "TRACE"},
		{5, "DEBUG"},
		{9, "INFO"},
		{13, "WARN"},
		{17, "ERROR"},
		{21, "FATAL"},
	}

	for _, tc := range cases {
		req := &collectorlogsv1.ExportLogsServiceRequest{
			ResourceLogs: []*logsv1.ResourceLogs{
				{
					Resource: serviceResource("svc"),
					ScopeLogs: []*logsv1.ScopeLogs{
						{
							LogRecords: []*logsv1.LogRecord{
								{
									SeverityNumber: logsv1.SeverityNumber(tc.severityNumber),
									Body:           &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "msg"}},
								},
							},
						},
					},
				},
			},
		}
		b, _ := proto.Marshal(req)
		logs, err := DecodeLogs(b)
		if err != nil {
			t.Fatal(err)
		}
		if got := logs[0].SeverityLabel(); got != tc.wantLabel {
			t.Errorf("SeverityNumber=%d: want label %s, got %s", tc.severityNumber, tc.wantLabel, got)
		}
	}
}

func TestDecodeLogs_MultipleLogs(t *testing.T) {
	makeRecord := func(body string) *logsv1.LogRecord {
		return &logsv1.LogRecord{
			Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: body}},
		}
	}

	req := &collectorlogsv1.ExportLogsServiceRequest{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				Resource: serviceResource("svc"),
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							makeRecord("msg1"),
							makeRecord("msg2"),
							makeRecord("msg3"),
						},
					},
				},
			},
		},
	}

	b, _ := proto.Marshal(req)
	logs, err := DecodeLogs(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 3 {
		t.Fatalf("want 3 logs, got %d", len(logs))
	}
	bodies := []string{logs[0].Body, logs[1].Body, logs[2].Body}
	for i, want := range []string{"msg1", "msg2", "msg3"} {
		if bodies[i] != want {
			t.Errorf("logs[%d].Body: want %s, got %s", i, want, bodies[i])
		}
	}
}

func TestDecodeLogsFromRequest_SameAsDecodeLogs(t *testing.T) {
	req := &collectorlogsv1.ExportLogsServiceRequest{
		ResourceLogs: []*logsv1.ResourceLogs{
			{
				Resource: serviceResource("svc"),
				ScopeLogs: []*logsv1.ScopeLogs{
					{
						LogRecords: []*logsv1.LogRecord{
							{
								SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_WARN,
								SeverityText:   "WARN",
								Body:           &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "warning msg"}},
							},
						},
					},
				},
			},
		},
	}

	b, _ := proto.Marshal(req)
	fromBytes, err := DecodeLogs(b)
	if err != nil {
		t.Fatal(err)
	}
	fromProto := DecodeLogsFromRequest(req)

	if len(fromBytes) != len(fromProto) {
		t.Fatalf("log count mismatch: bytes=%d, proto=%d", len(fromBytes), len(fromProto))
	}
	l1, l2 := fromBytes[0], fromProto[0]
	if l1.Body != l2.Body {
		t.Errorf("Body mismatch: %q vs %q", l1.Body, l2.Body)
	}
	if l1.SeverityNumber != l2.SeverityNumber {
		t.Errorf("SeverityNumber mismatch: %d vs %d", l1.SeverityNumber, l2.SeverityNumber)
	}
	if l1.ServiceName != l2.ServiceName {
		t.Errorf("ServiceName mismatch: %q vs %q", l1.ServiceName, l2.ServiceName)
	}
}

// ---- convertAttrs 테스트 ----

func TestConvertAttrs_Types(t *testing.T) {
	attrs := []*commonv1.KeyValue{
		strKV("str", "hello"),
		intKV("int", 42),
		{Key: "float", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 3.14}}},
		{Key: "bool", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: true}}},
	}

	m := convertAttrs(attrs)
	if m["str"] != "hello" {
		t.Errorf("str: want hello, got %v", m["str"])
	}
	if m["int"] != int64(42) {
		t.Errorf("int: want 42, got %v (%T)", m["int"], m["int"])
	}
	if m["float"] != 3.14 {
		t.Errorf("float: want 3.14, got %v", m["float"])
	}
	if m["bool"] != true {
		t.Errorf("bool: want true, got %v", m["bool"])
	}
}

func TestConvertAttrs_Nil(t *testing.T) {
	m := convertAttrs(nil)
	if m != nil {
		t.Errorf("nil attrs should return nil map, got %v", m)
	}
}

func TestConvertAttrs_Empty(t *testing.T) {
	m := convertAttrs([]*commonv1.KeyValue{})
	if m != nil {
		t.Errorf("empty attrs should return nil map, got %v", m)
	}
}
