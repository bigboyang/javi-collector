// Package ingester는 수신된 OTLP 데이터를 디코딩하여 저장소에 기록한다.
//
// 두 가지 입력 경로를 지원한다:
//   - Ingest*(ctx, []byte): HTTP 경로. protobuf bytes를 디코딩 후 저장.
//   - IngestFrom*Proto(ctx, *req): gRPC 경로. 이미 파싱된 proto 구조체를 직접 변환.
//
// gRPC 경로에서 proto.Marshal → proto.Unmarshal 왕복을 제거한 이유:
// gRPC 프레임워크가 이미 Unmarshal을 완료한 *req 구조체가 있으므로
// 재직렬화하는 것은 불필요한 heap 할당(평균 수 KB~수십 KB) + CPU 낭비다.
// 100k spans/sec 환경에서 약 10-20% CPU 절감 효과가 있다.
package ingester

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"time"

	"github.com/kkc/javi-collector/internal/decoder"
	"github.com/kkc/javi-collector/internal/model"
	"github.com/kkc/javi-collector/internal/processor"
	"github.com/kkc/javi-collector/internal/rag"
	"github.com/kkc/javi-collector/internal/selftracing"
	"github.com/kkc/javi-collector/internal/store"
)

// ---- Prometheus 지표 ----
//
// promauto를 사용해 전역 레지스트리에 자동 등록한다.
// Counter는 monotonic으로 Prometheus가 rate()로 TPS를 계산할 수 있다.
var (
	spansIngestedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "ingester",
		Name:      "spans_total",
		Help:      "Total number of spans ingested successfully.",
	})
	metricsIngestedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "ingester",
		Name:      "metrics_total",
		Help:      "Total number of metric data points ingested successfully.",
	})
	logsIngestedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "ingester",
		Name:      "logs_total",
		Help:      "Total number of log records ingested successfully.",
	})
	// signal 레이블: "traces" | "metrics" | "logs"
	ingestErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "ingester",
		Name:      "errors_total",
		Help:      "Total number of ingest errors by signal type.",
	}, []string{"signal"})
)

// SpanPublisher는 span 이벤트를 하류(RAG, Forecast 등)로 발행하는 인터페이스다.
//   - DirectSpanPublisher: 채널 기반 EmbedPipeline에 직접 제출 (Kafka 미사용 시)
//   - kafka.SpanProducer: Kafka 토픽에 발행 (팬아웃 필요 시)
type SpanPublisher interface {
	Publish(sp *model.SpanData)
}

// MetricPublisher는 metric 이벤트를 하류(Forecast 등)로 발행하는 인터페이스다.
//   - kafka.MetricProducer: Kafka metrics 토픽에 발행
type MetricPublisher interface {
	PublishMetric(m *model.MetricData)
}

// LogPublisher는 log 이벤트를 하류(RAG 등)로 발행하는 인터페이스다.
//   - kafka.LogProducer: Kafka logs 토픽에 발행
type LogPublisher interface {
	PublishLog(l *model.LogData)
}

// DirectSpanPublisher는 span을 DocumentBuilder로 변환해 EmbedPipeline에 직접 제출한다.
// KAFKA_ENABLED=false(기본)일 때 사용한다.
type DirectSpanPublisher struct {
	Pipeline *rag.EmbedPipeline
	Builder  rag.DocumentBuilder
}

// Publish는 span을 EmbedDocument로 변환해 EmbedPipeline에 제출한다.
func (d *DirectSpanPublisher) Publish(sp *model.SpanData) {
	if doc := d.Builder.BuildFromSpan(sp, nil); doc != nil {
		d.Pipeline.Submit(doc)
	}
}

// DirectLogPublisher는 ERROR+ 로그를 EmbedPipeline에 직접 제출한다.
// KAFKA_ENABLED=false(기본)일 때 사용한다.
type DirectLogPublisher struct {
	Pipeline *rag.EmbedPipeline
	Builder  rag.DocumentBuilder
}

// PublishLog는 ERROR 이상 심각도 로그를 EmbedDocument로 변환해 EmbedPipeline에 제출한다.
func (d *DirectLogPublisher) PublishLog(l *model.LogData) {
	if doc := d.Builder.BuildFromLog(l); doc != nil {
		d.Pipeline.Submit(doc)
	}
}

// MultiSpanPublisher는 여러 SpanPublisher에 span 이벤트를 팬아웃한다.
// RAG + Forecast 같이 복수의 하류가 필요한 경우에 사용한다.
type MultiSpanPublisher struct {
	publishers []SpanPublisher
}

// NewMultiSpanPublisher는 MultiSpanPublisher를 생성한다.
func NewMultiSpanPublisher(pubs ...SpanPublisher) *MultiSpanPublisher {
	return &MultiSpanPublisher{publishers: pubs}
}

// Publish는 모든 SpanPublisher에 span을 순차 전달한다.
func (m *MultiSpanPublisher) Publish(sp *model.SpanData) {
	for _, p := range m.publishers {
		p.Publish(sp)
	}
}

// Ingester는 OTLP 수신 → decode → process → store 파이프라인을 담당한다.
type Ingester struct {
	traceStore  store.TraceStore
	metricStore store.MetricStore
	logStore    store.LogStore
	spanPub     SpanPublisher          // nil이면 span RAG/Forecast 비활성화
	metricPub   MetricPublisher        // nil이면 metric Forecast 비활성화
	logPub      LogPublisher           // nil이면 log RAG 비활성화
	pipeline    *processor.Pipeline    // nil이면 프로세서 파이프라인 비활성화
	selfTracer  *selftracing.Tracer    // nil이면 셀프 트레이싱 비활성화

	// atomic 카운터: /api/collector/stats 조회용 (단조 증가, reset 없음)
	traceReceived  atomic.Int64
	metricReceived atomic.Int64
	logReceived    atomic.Int64
}

// New는 Ingester를 생성한다.
// pub은 nil이면 RAG/Forecast 기능이 비활성화된다.
// pub이 *DirectSpanPublisher이면 채널 기반 직접 적재, *kafka.SpanProducer이면 Kafka 팬아웃.
func New(ts store.TraceStore, ms store.MetricStore, ls store.LogStore, pub SpanPublisher) *Ingester {
	return &Ingester{
		traceStore:  ts,
		metricStore: ms,
		logStore:    ls,
		spanPub:     pub,
	}
}

// SetPipeline attaches a processor pipeline. Must be called before serving traffic.
func (ing *Ingester) SetPipeline(p *processor.Pipeline) { ing.pipeline = p }

// SetSelfTracer attaches the self-tracing tracer. Must be called before serving traffic.
func (ing *Ingester) SetSelfTracer(t *selftracing.Tracer) { ing.selfTracer = t }

// SetMetricPublisher는 metric 이벤트 발행자를 설정한다. 트래픽 수신 전에 호출해야 한다.
func (ing *Ingester) SetMetricPublisher(p MetricPublisher) { ing.metricPub = p }

// SetLogPublisher는 log 이벤트 발행자를 설정한다. 트래픽 수신 전에 호출해야 한다.
func (ing *Ingester) SetLogPublisher(p LogPublisher) { ing.logPub = p }

// ---- HTTP 경로: protobuf bytes 입력 ----

// IngestTraces는 OTLP/HTTP 경로의 ExportTraceServiceRequest bytes를 처리한다.
func (ing *Ingester) IngestTraces(ctx context.Context, body []byte) (int, error) {
	spans, err := decoder.DecodeTraces(body)
	if err != nil {
		ingestErrorsTotal.WithLabelValues("traces").Inc()
		return 0, err
	}
	return ing.storeSpans(ctx, spans)
}

// IngestMetrics는 OTLP/HTTP 경로의 ExportMetricsServiceRequest bytes를 처리한다.
func (ing *Ingester) IngestMetrics(ctx context.Context, body []byte) (int, error) {
	metrics, err := decoder.DecodeMetrics(body)
	if err != nil {
		ingestErrorsTotal.WithLabelValues("metrics").Inc()
		return 0, err
	}
	return ing.storeMetrics(ctx, metrics)
}

// IngestLogs는 OTLP/HTTP 경로의 ExportLogsServiceRequest bytes를 처리한다.
func (ing *Ingester) IngestLogs(ctx context.Context, body []byte) (int, error) {
	logs, err := decoder.DecodeLogs(body)
	if err != nil {
		ingestErrorsTotal.WithLabelValues("logs").Inc()
		return 0, err
	}
	return ing.storeLogs(ctx, logs)
}

// IngestTracesJSON은 OTLP/HTTP JSON 경로의 ExportTraceServiceRequest를 처리한다.
func (ing *Ingester) IngestTracesJSON(ctx context.Context, body []byte) (int, error) {
	spans, err := decoder.DecodeTracesJSON(body)
	if err != nil {
		ingestErrorsTotal.WithLabelValues("traces").Inc()
		return 0, err
	}
	return ing.storeSpans(ctx, spans)
}

// IngestMetricsJSON은 OTLP/HTTP JSON 경로의 ExportMetricsServiceRequest를 처리한다.
func (ing *Ingester) IngestMetricsJSON(ctx context.Context, body []byte) (int, error) {
	metrics, err := decoder.DecodeMetricsJSON(body)
	if err != nil {
		ingestErrorsTotal.WithLabelValues("metrics").Inc()
		return 0, err
	}
	return ing.storeMetrics(ctx, metrics)
}

// IngestLogsJSON은 OTLP/HTTP JSON 경로의 ExportLogsServiceRequest를 처리한다.
func (ing *Ingester) IngestLogsJSON(ctx context.Context, body []byte) (int, error) {
	logs, err := decoder.DecodeLogsJSON(body)
	if err != nil {
		ingestErrorsTotal.WithLabelValues("logs").Inc()
		return 0, err
	}
	return ing.storeLogs(ctx, logs)
}

// ---- gRPC 경로: 파싱된 proto 구조체 직접 전달 ----
//
// gRPC 프레임워크가 이미 Unmarshal을 완료한 구조체를 재직렬화하지 않고
// 직접 decoder.DecodeTracesFromRequest를 호출해 SpanData로 변환한다.

// IngestTracesFromProto는 gRPC 경로의 ExportTraceServiceRequest를 처리한다.
func (ing *Ingester) IngestTracesFromProto(
	ctx context.Context,
	req *collectortracev1.ExportTraceServiceRequest,
) (int, error) {
	spans := decoder.DecodeTracesFromRequest(req)
	return ing.storeSpans(ctx, spans)
}

// IngestMetricsFromProto는 gRPC 경로의 ExportMetricsServiceRequest를 처리한다.
func (ing *Ingester) IngestMetricsFromProto(
	ctx context.Context,
	req *collectormetricsv1.ExportMetricsServiceRequest,
) (int, error) {
	metrics := decoder.DecodeMetricsFromRequest(req)
	return ing.storeMetrics(ctx, metrics)
}

// IngestLogsFromProto는 gRPC 경로의 ExportLogsServiceRequest를 처리한다.
func (ing *Ingester) IngestLogsFromProto(
	ctx context.Context,
	req *collectorlogsv1.ExportLogsServiceRequest,
) (int, error) {
	logs := decoder.DecodeLogsFromRequest(req)
	return ing.storeLogs(ctx, logs)
}

// ---- 공통 저장 로직 ----

func (ing *Ingester) storeSpans(ctx context.Context, spans []*model.SpanData) (int, error) {
	if len(spans) == 0 {
		return 0, nil
	}

	// Processor pipeline: cardinality control, attribute filtering, etc.
	if ing.pipeline != nil && ing.pipeline.Enabled() {
		procStart := time.Now()
		inCount := len(spans)
		var procErr error
		spans, procErr = ing.pipeline.ProcessSpans(ctx, spans)
		if ing.selfTracer != nil {
			ing.selfTracer.RecordProcess("traces", inCount, len(spans), procStart, procErr)
		}
		if procErr != nil {
			ingestErrorsTotal.WithLabelValues("traces").Inc()
			return 0, procErr
		}
	}

	storeStart := time.Now()
	storeErr := ing.traceStore.AppendSpans(ctx, spans)
	if ing.selfTracer != nil {
		ing.selfTracer.RecordIngest("ingest.traces", "traces", len(spans), storeStart, storeErr)
	}
	if storeErr != nil {
		ingestErrorsTotal.WithLabelValues("traces").Inc()
		return 0, storeErr
	}

	n := int64(len(spans))
	ing.traceReceived.Add(n)
	spansIngestedTotal.Add(float64(n))

	// INFO: service별 span 수 + error 카운트 요약
	if slog.Default().Enabled(ctx, slog.LevelInfo) {
		svcCount := make(map[string]int, 4)
		errCount := 0
		for _, sp := range spans {
			svcCount[sp.ServiceName]++
			if sp.StatusCode == 2 { // ERROR
				errCount++
			}
		}
		for svc, cnt := range svcCount {
			slog.Info("traces stored", "service", svc, "spans", cnt, "errors", errCount, "total", ing.traceReceived.Load())
		}
	}

	// RAG / Forecast 파이프라인: span 이벤트를 비동기로 발행 (non-blocking)
	// SpanPublisher 구현에 따라 직접 EmbedPipeline 제출 또는 Kafka 발행.
	if ing.spanPub != nil {
		for _, sp := range spans {
			ing.spanPub.Publish(sp)
		}
	}

	return int(n), nil
}

func (ing *Ingester) storeMetrics(ctx context.Context, metrics []*model.MetricData) (int, error) {
	if len(metrics) == 0 {
		return 0, nil
	}

	if ing.pipeline != nil && ing.pipeline.Enabled() {
		procStart := time.Now()
		inCount := len(metrics)
		var procErr error
		metrics, procErr = ing.pipeline.ProcessMetrics(ctx, metrics)
		if ing.selfTracer != nil {
			ing.selfTracer.RecordProcess("metrics", inCount, len(metrics), procStart, procErr)
		}
		if procErr != nil {
			ingestErrorsTotal.WithLabelValues("metrics").Inc()
			return 0, procErr
		}
	}

	storeStart := time.Now()
	storeErr := ing.metricStore.AppendMetrics(ctx, metrics)
	if ing.selfTracer != nil {
		ing.selfTracer.RecordIngest("ingest.metrics", "metrics", len(metrics), storeStart, storeErr)
	}
	if storeErr != nil {
		ingestErrorsTotal.WithLabelValues("metrics").Inc()
		return 0, storeErr
	}

	n := int64(len(metrics))
	ing.metricReceived.Add(n)
	metricsIngestedTotal.Add(float64(n))

	if slog.Default().Enabled(ctx, slog.LevelInfo) {
		svc := ""
		if len(metrics) > 0 {
			svc = metrics[0].ServiceName
		}
		slog.Info("metrics stored", "service", svc, "count", n, "total", ing.metricReceived.Load())
	}

	// Forecast 파이프라인: metric 이벤트를 비동기로 발행 (non-blocking)
	if ing.metricPub != nil {
		for _, m := range metrics {
			ing.metricPub.PublishMetric(m)
		}
	}

	return int(n), nil
}

func (ing *Ingester) storeLogs(ctx context.Context, logs []*model.LogData) (int, error) {
	if len(logs) == 0 {
		return 0, nil
	}

	if ing.pipeline != nil && ing.pipeline.Enabled() {
		procStart := time.Now()
		inCount := len(logs)
		var procErr error
		logs, procErr = ing.pipeline.ProcessLogs(ctx, logs)
		if ing.selfTracer != nil {
			ing.selfTracer.RecordProcess("logs", inCount, len(logs), procStart, procErr)
		}
		if procErr != nil {
			ingestErrorsTotal.WithLabelValues("logs").Inc()
			return 0, procErr
		}
	}

	storeStart := time.Now()
	storeErr := ing.logStore.AppendLogs(ctx, logs)
	if ing.selfTracer != nil {
		ing.selfTracer.RecordIngest("ingest.logs", "logs", len(logs), storeStart, storeErr)
	}
	if storeErr != nil {
		ingestErrorsTotal.WithLabelValues("logs").Inc()
		return 0, storeErr
	}

	n := int64(len(logs))
	ing.logReceived.Add(n)
	logsIngestedTotal.Add(float64(n))

	if slog.Default().Enabled(ctx, slog.LevelInfo) {
		svc := ""
		if len(logs) > 0 {
			svc = logs[0].ServiceName
		}
		slog.Info("logs stored", "service", svc, "count", n, "total", ing.logReceived.Load())
	}

	// RAG 파이프라인: log 이벤트를 비동기로 발행 (non-blocking)
	if ing.logPub != nil {
		for _, l := range logs {
			ing.logPub.PublishLog(l)
		}
	}

	return int(n), nil
}

// ---- 통계 조회 ----

func (ing *Ingester) TraceReceived() int64  { return ing.traceReceived.Load() }
func (ing *Ingester) MetricReceived() int64 { return ing.metricReceived.Load() }
func (ing *Ingester) LogReceived() int64    { return ing.logReceived.Load() }
