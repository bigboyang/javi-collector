// Package forecast는 javi-collector에서 javi-forecast로 직접 HTTP 전송하는 포워더를 제공한다.
//
// Kafka 없이 동작하는 직접 전송 경로다. SpanPublisher, MetricPublisher 인터페이스를
// 구현하므로 Kafka 활성화 여부와 무관하게 Ingester에 plug-in 가능하다.
//
// 지원 엔드포인트:
//
//	POST /v1/spans         — span 배치 (SpanBatch)
//	POST /v1/metrics       — metric 배치 (MetricEventBatch)
//	POST /v1/metrics/jvm   — JVM 스냅샷 배치 (JvmMetricBatch)
//
// JVM 메트릭 변환:
//
//	OTel 'jvm.*' 및 'process.runtime.jvm.*' 메트릭을 수신하면
//	서비스별 스냅샷에 누적하고, flushInterval마다 /v1/metrics/jvm 으로 전송한다.
package forecast

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kkc/javi-collector/internal/model"
)

// ── javi-forecast 페이로드 구조체 ─────────────────────────────────────────────

// spanPayload는 /v1/spans 배치 페이로드다 (javi-forecast SpanBatch).
type spanPayload struct {
	Spans []spanEvent `json:"spans"`
}

// spanEvent는 javi-forecast SpanEvent 모델과 1:1 매핑된다.
// 모든 필드명은 snake_case (Python FastAPI Pydantic 모델 기준).
type spanEvent struct {
	TraceID            string         `json:"trace_id"`
	SpanID             string         `json:"span_id"`
	ParentSpanID       string         `json:"parent_span_id,omitempty"`
	Name               string         `json:"name"`
	Kind               int32          `json:"kind"`
	StartTimeNano      int64          `json:"start_time_nano"`
	EndTimeNano        int64          `json:"end_time_nano"`
	Attributes         map[string]any `json:"attributes,omitempty"`
	ResourceAttributes map[string]any `json:"resource_attributes,omitempty"`
	StatusCode         int32          `json:"status_code"`
	StatusMessage      string         `json:"status_message,omitempty"`
	ServiceName        string         `json:"service_name"`
	ScopeName          string         `json:"scope_name,omitempty"`
}

// metricPayload는 /v1/metrics 배치 페이로드다 (javi-forecast MetricEventBatch).
type metricPayload struct {
	Metrics []metricEvent `json:"metrics"`
}

// metricEvent는 javi-forecast MetricEvent 모델과 1:1 매핑된다.
type metricEvent struct {
	ServiceName string         `json:"service_name"`
	MetricName  string         `json:"metric_name"`
	MetricType  string         `json:"metric_type"`
	Unit        string         `json:"unit,omitempty"`
	Value       float64        `json:"value"`
	TimestampMs int64          `json:"timestamp_ms"`
	Attributes  map[string]any `json:"attributes,omitempty"`
}

// jvmBatch는 /v1/metrics/jvm 배치 페이로드다 (javi-forecast JvmMetricBatch).
type jvmBatch struct {
	Metrics []jvmEvent `json:"metrics"`
}

// jvmEvent는 javi-forecast JvmMetricEvent 모델과 1:1 매핑된다.
type jvmEvent struct {
	ServiceName           string  `json:"service_name"`
	TimestampNano         int64   `json:"timestamp_nano"`
	HeapUsedBytes         float64 `json:"heap_used_bytes,omitempty"`
	HeapCommittedBytes    float64 `json:"heap_committed_bytes,omitempty"`
	HeapMaxBytes          float64 `json:"heap_max_bytes,omitempty"`
	GCCountDelta          float64 `json:"gc_count_delta,omitempty"`
	GCPauseMsTotalDelta   float64 `json:"gc_pause_ms_total_delta,omitempty"`
	GCCollectionName      string  `json:"gc_collection_name,omitempty"`
	ThreadCount           float64 `json:"thread_count,omitempty"`
	ThreadPeak            float64 `json:"thread_peak,omitempty"`
	ThreadDaemon          float64 `json:"thread_daemon,omitempty"`
	ProcessCPUUtilization float64 `json:"process_cpu_utilization,omitempty"`
	SystemCPUUtilization  float64 `json:"system_cpu_utilization,omitempty"`
}

// jvmSnapshot은 서비스별 JVM 메트릭 누적 상태다.
type jvmSnapshot struct {
	ev        jvmEvent
	updatedAt time.Time
}

// ── ForecastForwarder ─────────────────────────────────────────────────────────

// Config는 ForecastForwarder 설정이다.
type Config struct {
	Endpoint      string        // javi-forecast base URL (e.g. http://localhost:8080)
	BatchSize     int           // 배치 최대 크기 (기본 100)
	FlushInterval time.Duration // 배치 flush 주기 (기본 5s)
}

// ForecastForwarder는 javi-forecast 서버로 span/metric 데이터를 직접 HTTP 전송한다.
//
// Publish / PublishMetric은 각각 ingester.SpanPublisher / ingester.MetricPublisher를 구현해
// Kafka 없이도 Ingester 파이프라인에 플러그인할 수 있다.
type ForecastForwarder struct {
	endpoint      string
	client        *http.Client
	batchSize     int
	flushInterval time.Duration

	spanCh   chan spanEvent
	metricCh chan metricEvent

	mu       sync.Mutex
	jvmSnaps map[string]*jvmSnapshot // key: service_name
}

// New는 ForecastForwarder를 생성한다.
func New(cfg Config) *ForecastForwarder {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	return &ForecastForwarder{
		endpoint:      cfg.Endpoint,
		client:        &http.Client{Timeout: 10 * time.Second},
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		spanCh:        make(chan spanEvent, cfg.BatchSize*8),
		metricCh:      make(chan metricEvent, cfg.BatchSize*8),
		jvmSnaps:      make(map[string]*jvmSnapshot),
	}
}

// Start는 백그라운드 flush 고루틴을 시작한다.
// ctx 취소 시 남은 버퍼를 드레인 후 종료한다.
func (f *ForecastForwarder) Start(ctx context.Context) {
	go f.runSpanFlusher(ctx)
	go f.runMetricFlusher(ctx)
	go f.runJVMFlusher(ctx)
}

// Publish는 span을 내부 채널에 넣는다 (ingester.SpanPublisher 구현).
func (f *ForecastForwarder) Publish(sp *model.SpanData) {
	if f.endpoint == "" {
		return
	}
	ev := spanEvent{
		TraceID:       sp.TraceID,
		SpanID:        sp.SpanID,
		ParentSpanID:  sp.ParentSpanID,
		Name:          sp.Name,
		Kind:          sp.Kind,
		StartTimeNano: sp.StartTimeNano,
		EndTimeNano:   sp.EndTimeNano,
		Attributes:    sp.Attributes,
		StatusCode:    sp.StatusCode,
		StatusMessage: sp.StatusMessage,
		ServiceName:   sp.ServiceName,
		ScopeName:     sp.ScopeName,
	}
	select {
	case f.spanCh <- ev:
	default:
		slog.Warn("forecast forwarder: span channel full, dropping span", "service", sp.ServiceName)
	}
}

// PublishMetric은 metric을 처리한다 (ingester.MetricPublisher 구현).
// jvm.* / process.runtime.jvm.* 메트릭은 JVM 스냅샷에 누적하고,
// 나머지는 metric 배치 채널로 전달한다.
func (f *ForecastForwarder) PublishMetric(m *model.MetricData) {
	if f.endpoint == "" {
		return
	}
	if strings.HasPrefix(m.Name, "jvm.") || strings.HasPrefix(m.Name, "process.runtime.jvm.") {
		f.updateJVMSnap(m)
		return
	}
	for _, dp := range m.DataPoints {
		value := dp.Value
		if m.Type == model.MetricTypeHistogram {
			if dp.Count > 0 {
				value = dp.Sum / float64(dp.Count)
			} else {
				value = dp.Sum
			}
		}
		ev := metricEvent{
			ServiceName: m.ServiceName,
			MetricName:  m.Name,
			MetricType:  string(m.Type),
			Unit:        m.Unit,
			Value:       value,
			TimestampMs: dp.TimeNanos / 1_000_000,
			Attributes:  dp.Attributes,
		}
		select {
		case f.metricCh <- ev:
		default:
			slog.Warn("forecast forwarder: metric channel full, dropping metric",
				"service", m.ServiceName, "metric", m.Name)
		}
	}
}

// updateJVMSnap은 OTel JVM 메트릭을 서비스별 jvmSnapshot에 반영한다.
//
// 지원 메트릭명 (OTel Java Agent 기준):
//
//	jvm.memory.heap.used / committed / max
//	jvm.gc.duration (histogram)
//	jvm.thread.count / peak / daemon.count
//	jvm.process.cpu.utilization, process.runtime.jvm.cpu.utilization
//	system.cpu.utilization
func (f *ForecastForwarder) updateJVMSnap(m *model.MetricData) {
	f.mu.Lock()
	defer f.mu.Unlock()

	snap, ok := f.jvmSnaps[m.ServiceName]
	if !ok {
		snap = &jvmSnapshot{ev: jvmEvent{ServiceName: m.ServiceName}}
		f.jvmSnaps[m.ServiceName] = snap
	}

	snap.updatedAt = time.Now()
	for _, dp := range m.DataPoints {
		if snap.ev.TimestampNano < dp.TimeNanos {
			snap.ev.TimestampNano = dp.TimeNanos
		}
		switch m.Name {
		case "jvm.memory.heap.used":
			snap.ev.HeapUsedBytes = dp.Value
		case "jvm.memory.heap.committed":
			snap.ev.HeapCommittedBytes = dp.Value
		case "jvm.memory.heap.max":
			snap.ev.HeapMaxBytes = dp.Value
		case "jvm.gc.duration":
			// HISTOGRAM: sum=총 GC 시간(초), count=GC 횟수
			snap.ev.GCPauseMsTotalDelta = dp.Sum * 1000 // 초 → ms
			snap.ev.GCCountDelta = float64(dp.Count)
			if v, ok := dp.Attributes["jvm.gc.name"]; ok {
				if name, ok := v.(string); ok && name != "" {
					snap.ev.GCCollectionName = name
				}
			}
		case "jvm.thread.count", "jvm.threads.count",
			"process.runtime.jvm.threads.count":
			snap.ev.ThreadCount = dp.Value
		case "jvm.thread.peak":
			snap.ev.ThreadPeak = dp.Value
		case "jvm.thread.daemon.count":
			snap.ev.ThreadDaemon = dp.Value
		case "jvm.process.cpu.utilization",
			"process.runtime.jvm.cpu.utilization",
			"jvm.cpu.utilization":
			snap.ev.ProcessCPUUtilization = dp.Value
		case "system.cpu.utilization":
			snap.ev.SystemCPUUtilization = dp.Value
		}
	}
}

// ── 백그라운드 flush 루프 ─────────────────────────────────────────────────────

func (f *ForecastForwarder) runSpanFlusher(ctx context.Context) {
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()
	batch := make([]spanEvent, 0, f.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		f.sendSpans(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case ev := <-f.spanCh:
			batch = append(batch, ev)
			if len(batch) >= f.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			for len(f.spanCh) > 0 {
				batch = append(batch, <-f.spanCh)
			}
			flush()
			return
		}
	}
}

func (f *ForecastForwarder) runMetricFlusher(ctx context.Context) {
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()
	batch := make([]metricEvent, 0, f.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		f.sendMetrics(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case ev := <-f.metricCh:
			batch = append(batch, ev)
			if len(batch) >= f.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			for len(f.metricCh) > 0 {
				batch = append(batch, <-f.metricCh)
			}
			flush()
			return
		}
	}
}

func (f *ForecastForwarder) runJVMFlusher(ctx context.Context) {
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			f.flushJVM(ctx)
		case <-ctx.Done():
			f.flushJVM(ctx)
			return
		}
	}
}

func (f *ForecastForwarder) flushJVM(ctx context.Context) {
	f.mu.Lock()
	if len(f.jvmSnaps) == 0 {
		f.mu.Unlock()
		return
	}
	events := make([]jvmEvent, 0, len(f.jvmSnaps))
	for _, snap := range f.jvmSnaps {
		events = append(events, snap.ev)
	}
	f.mu.Unlock()

	f.sendJVM(ctx, events)
}

// ── HTTP 전송 ─────────────────────────────────────────────────────────────────

func (f *ForecastForwarder) sendSpans(ctx context.Context, spans []spanEvent) {
	f.post(ctx, "/v1/spans", spanPayload{Spans: spans})
}

func (f *ForecastForwarder) sendMetrics(ctx context.Context, metrics []metricEvent) {
	f.post(ctx, "/v1/metrics", metricPayload{Metrics: metrics})
}

func (f *ForecastForwarder) sendJVM(ctx context.Context, events []jvmEvent) {
	f.post(ctx, "/v1/metrics/jvm", jvmBatch{Metrics: events})
}

func (f *ForecastForwarder) post(ctx context.Context, path string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		slog.Warn("forecast forwarder: marshal failed", "path", path, "err", err)
		return
	}

	const maxRetries = 3
	delay := 200 * time.Millisecond
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
				delay *= 2
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			f.endpoint+path, bytes.NewReader(data))
		if err != nil {
			slog.Debug("forecast forwarder: request build failed", "path", path, "err", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := f.client.Do(req)
		if err != nil {
			slog.Debug("forecast forwarder: send failed", "path", path, "attempt", attempt+1, "err", err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 500 {
			slog.Warn("forecast forwarder: server error", "path", path, "status", resp.StatusCode, "attempt", attempt+1)
			continue // 5xx만 재시도
		}
		if resp.StatusCode >= 400 {
			slog.Warn("forecast forwarder: client error", "path", path, "status", resp.StatusCode)
		}
		return
	}
	slog.Warn("forecast forwarder: send failed after retries", "path", path)
}
