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

	"github.com/kkc/javi-collector/internal/decoder"
	"github.com/kkc/javi-collector/internal/model"
	"github.com/kkc/javi-collector/internal/rag"
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

// Ingester는 OTLP 수신 → decode → store 파이프라인을 담당한다.
type Ingester struct {
	traceStore  store.TraceStore
	metricStore store.MetricStore
	logStore    store.LogStore
	embedPipe   *rag.EmbedPipeline    // nil이면 RAG 비활성화
	docBuilder  rag.DocumentBuilder

	// atomic 카운터: /api/collector/stats 조회용 (단조 증가, reset 없음)
	traceReceived  atomic.Int64
	metricReceived atomic.Int64
	logReceived    atomic.Int64
}

// New는 Ingester를 생성한다.
// embedPipeline은 nil이면 RAG 기능이 비활성화된다.
func New(ts store.TraceStore, ms store.MetricStore, ls store.LogStore, embedPipeline *rag.EmbedPipeline) *Ingester {
	return &Ingester{
		traceStore:  ts,
		metricStore: ms,
		logStore:    ls,
		embedPipe:   embedPipeline,
	}
}

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
	if err := ing.traceStore.AppendSpans(ctx, spans); err != nil {
		ingestErrorsTotal.WithLabelValues("traces").Inc()
		return 0, err
	}
	n := int64(len(spans))
	ing.traceReceived.Add(n)
	spansIngestedTotal.Add(float64(n))
	slog.Debug("traces ingested", "spans", n)

	// RAG 파이프라인: ERROR span을 비동기로 임베딩 큐에 제출 (non-blocking)
	if ing.embedPipe != nil {
		for _, sp := range spans {
			if sp.StatusCode == 2 { // 2=ERROR
				if doc := ing.docBuilder.BuildFromSpan(sp, nil); doc != nil {
					ing.embedPipe.Submit(doc)
				}
			}
		}
	}

	return int(n), nil
}

func (ing *Ingester) storeMetrics(ctx context.Context, metrics []*model.MetricData) (int, error) {
	if len(metrics) == 0 {
		return 0, nil
	}
	if err := ing.metricStore.AppendMetrics(ctx, metrics); err != nil {
		ingestErrorsTotal.WithLabelValues("metrics").Inc()
		return 0, err
	}
	n := int64(len(metrics))
	ing.metricReceived.Add(n)
	metricsIngestedTotal.Add(float64(n))
	slog.Debug("metrics ingested", "count", n)
	return int(n), nil
}

func (ing *Ingester) storeLogs(ctx context.Context, logs []*model.LogData) (int, error) {
	if len(logs) == 0 {
		return 0, nil
	}
	if err := ing.logStore.AppendLogs(ctx, logs); err != nil {
		ingestErrorsTotal.WithLabelValues("logs").Inc()
		return 0, err
	}
	n := int64(len(logs))
	ing.logReceived.Add(n)
	logsIngestedTotal.Add(float64(n))
	slog.Debug("logs ingested", "count", n)
	return int(n), nil
}

// ---- 통계 조회 ----

func (ing *Ingester) TraceReceived() int64  { return ing.traceReceived.Load() }
func (ing *Ingester) MetricReceived() int64 { return ing.metricReceived.Load() }
func (ing *Ingester) LogReceived() int64    { return ing.logReceived.Load() }
