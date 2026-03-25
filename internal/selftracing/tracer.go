// Package selftracing provides collector-internal pipeline observability.
//
// The Tracer records timing and metadata for each ingestion operation
// (decode, process, store) and stores them as ordinary SpanData records
// so they appear alongside application traces in ClickHouse and the UI.
//
// Architecture:
//
//   - A small ring-buffer (channel) decouples the hot ingest path from
//     the storage write: callers call RecordSpan which is non-blocking.
//   - A background goroutine drains the buffer and calls store.AppendSpans
//     in batches to minimise overhead.
//   - Self-traces have serviceName = "javi-collector" and carry attributes
//     such as component, signal, span_count, duration_us, and error.
//
// Internal spans are tagged with "javi.internal"=true so dashboards and
// queries can filter them out if desired.
package selftracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/kkc/javi-collector/internal/model"
	"github.com/kkc/javi-collector/internal/store"
)

const (
	selfServiceName = "javi-collector"
	bufferSize      = 4096
	flushInterval   = 2 * time.Second
	flushBatchSize  = 256
)

// Prometheus counters for self-tracing overhead visibility.
var (
	selfSpansEmitted = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "selftracing",
		Name:      "spans_emitted_total",
		Help:      "Total internal pipeline spans emitted by the self-tracer.",
	})
	selfSpansDropped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "selftracing",
		Name:      "spans_dropped_total",
		Help:      "Total internal spans dropped because the ring-buffer was full.",
	})
)

// Tracer records internal pipeline spans without blocking the ingest path.
type Tracer struct {
	traceStore store.TraceStore
	buf        chan *model.SpanData
	done       chan struct{}
	stopped    atomic.Bool
}

// New creates a Tracer backed by the given store.
// Call Start to begin the background flush goroutine.
func New(ts store.TraceStore) *Tracer {
	return &Tracer{
		traceStore: ts,
		buf:        make(chan *model.SpanData, bufferSize),
		done:       make(chan struct{}),
	}
}

// Start launches the background flush goroutine.
func (t *Tracer) Start() {
	go t.run()
}

// Stop drains the buffer and shuts down the flush goroutine.
func (t *Tracer) Stop() {
	if t.stopped.CompareAndSwap(false, true) {
		close(t.done)
	}
}

// run is the background goroutine.
func (t *Tracer) run() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]*model.SpanData, 0, flushBatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := t.traceStore.AppendSpans(context.Background(), batch); err != nil {
			slog.Warn("selftracing: flush error", "err", err, "count", len(batch))
		}
		batch = batch[:0]
	}

	for {
		select {
		case sp := <-t.buf:
			batch = append(batch, sp)
			if len(batch) >= flushBatchSize {
				flush()
			}

		case <-ticker.C:
			flush()

		case <-t.done:
			// Drain remaining buffered spans.
			for {
				select {
				case sp := <-t.buf:
					batch = append(batch, sp)
				default:
					flush()
					return
				}
			}
		}
	}
}

// emit submits a span to the ring-buffer (non-blocking).
func (t *Tracer) emit(sp *model.SpanData) {
	if t.stopped.Load() {
		return
	}
	select {
	case t.buf <- sp:
		selfSpansEmitted.Inc()
	default:
		selfSpansDropped.Inc()
	}
}

// ---------- Span creation helpers ----------

// SpanBuilder accumulates span fields and emits on End.
type SpanBuilder struct {
	tracer    *Tracer
	traceID   string
	spanID    string
	name      string
	start     time.Time
	attrs     map[string]any
}

// StartSpan begins timing a pipeline operation and returns a SpanBuilder.
// The caller must call End() to emit the span.
func (t *Tracer) StartSpan(name string) *SpanBuilder {
	return &SpanBuilder{
		tracer:  t,
		traceID: newID(16),
		spanID:  newID(8),
		name:    name,
		start:   time.Now(),
		attrs:   map[string]any{"javi.internal": true},
	}
}

// SetAttr adds an attribute to the span.
func (sb *SpanBuilder) SetAttr(k string, v any) *SpanBuilder {
	sb.attrs[k] = v
	return sb
}

// End finalises the span and submits it to the ring-buffer.
// statusCode: 0=UNSET, 1=OK, 2=ERROR.
func (sb *SpanBuilder) End(statusCode int32, statusMsg string) {
	now := time.Now()
	durationUs := now.Sub(sb.start).Microseconds()
	sb.attrs["duration_us"] = durationUs

	sp := &model.SpanData{
		TraceID:       sb.traceID,
		SpanID:        sb.spanID,
		Name:          sb.name,
		Kind:          1, // INTERNAL
		StartTimeNano: sb.start.UnixNano(),
		EndTimeNano:   now.UnixNano(),
		Attributes:    sb.attrs,
		StatusCode:    statusCode,
		StatusMessage: statusMsg,
		ServiceName:   selfServiceName,
		ScopeName:     "javi.selftracing",
		ReceivedAtMs:  now.UnixMilli(),
	}
	sb.tracer.emit(sp)
}

// ---------- Convenience wrappers ----------

// RecordIngest is a helper that records a complete ingest operation.
// It is called after the operation completes.
//
//	start := time.Now()
//	n, err := doIngest(...)
//	tracer.RecordIngest("ingest.traces", "traces", n, start, err)
func (t *Tracer) RecordIngest(spanName, signal string, count int, start time.Time, err error) {
	code := int32(1) // OK
	msg := ""
	if err != nil {
		code = 2
		msg = err.Error()
	}
	t.StartSpan(spanName).
		SetAttr("component", "ingester").
		SetAttr("signal", signal).
		SetAttr("item_count", count).
		End(code, msg)
}

// RecordProcess records a processor pipeline execution.
func (t *Tracer) RecordProcess(signal string, inCount, outCount int, start time.Time, err error) {
	code := int32(1)
	msg := ""
	if err != nil {
		code = 2
		msg = err.Error()
	}
	t.StartSpan("processor.pipeline").
		SetAttr("component", "processor").
		SetAttr("signal", signal).
		SetAttr("in_count", inCount).
		SetAttr("out_count", outCount).
		End(code, msg)
}

// ---------- ID generation ----------

func newID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Fallback: use time-based value (extremely rare).
		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> uint(i))
		}
	}
	return hex.EncodeToString(b)
}
