package processor

// Cardinality-limiting processor with Bloom-filter–based uniqueness tracking.
//
// Problem: high-cardinality attributes (e.g. user IDs, request IDs) cause
// ClickHouse dictionary-encoding overhead and can exhaust memory. This processor
// replaces attribute values that exceed the configured per-service / per-key
// cardinality limit with the placeholder string "__high_cardinality__".
//
// Design:
//   - One bloom filter per (service_name, attribute_key) tuple.
//   - A Bloom filter answers "has this value been seen before?" in O(k) time
//     with configurable false-positive rate (≈ 0.003% at default settings).
//   - When the estimated unique-value count exceeds the limit, new distinct
//     values are replaced rather than inserted into the filter.
//   - Prometheus counters expose per-(service,key) drops for dashboarding.

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/kkc/javi-collector/internal/model"
)

const cardinalityPlaceholder = "__high_cardinality__"

// CardinalityLimits configures the cardinality processor.
type CardinalityLimits struct {
	// PerServiceAttrLimit is the maximum number of unique string values
	// allowed per (service_name, attribute_key) pair. Values exceeding the
	// limit are replaced with the placeholder.
	// Default: 200
	PerServiceAttrLimit int

	// BloomFilterBits is the size of each bloom filter in bits.
	// Larger values reduce false-positive rate at the cost of memory.
	// Default: 100_000  (≈12.5 KB per tracker)
	BloomFilterBits uint

	// BloomHashFunctions is the number of independent hash functions used.
	// Optimal value for FP rate is k = (m/n)*ln2 ≈ 4 at default settings.
	// Default: 4
	BloomHashFunctions uint
}

// DefaultCardinalityLimits returns production-ready defaults.
func DefaultCardinalityLimits() CardinalityLimits {
	return CardinalityLimits{
		PerServiceAttrLimit: 200,
		BloomFilterBits:     100_000,
		BloomHashFunctions:  4,
	}
}

// ---------- bloom filter ----------

// bloomFilter is a minimal, fixed-size Bloom filter backed by a uint64 slice.
// It is NOT goroutine-safe; callers must hold attrTracker.mu.
type bloomFilter struct {
	bits []uint64
	m    uint   // total bit capacity
	k    uint   // number of hash functions
	n    uint64 // estimated unique insertions (may over-count on false positives)
}

func newBloomFilter(m, k uint) *bloomFilter {
	return &bloomFilter{
		bits: make([]uint64, (m+63)/64),
		m:    m,
		k:    k,
	}
}

// add inserts value and returns true if it appears to be a new entry.
// Uses double-hashing: hash_i = h1 + i*h2.
func (bf *bloomFilter) add(value string) (isNew bool) {
	h1, h2 := bloomHash(value)
	isNew = false
	for i := uint(0); i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(bf.m)
		word, bit := idx/64, idx%64
		if bf.bits[word]&(1<<bit) == 0 {
			isNew = true
			bf.bits[word] |= 1 << bit
		}
	}
	if isNew {
		bf.n++
	}
	return
}

// test returns true if value MAY have been inserted (possible false positive).
func (bf *bloomFilter) test(value string) bool {
	h1, h2 := bloomHash(value)
	for i := uint(0); i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % uint64(bf.m)
		word, bit := idx/64, idx%64
		if bf.bits[word]&(1<<bit) == 0 {
			return false
		}
	}
	return true
}

// bloomHash returns two independent 64-bit hashes via FNV-1a + Kirsch-Mitzenmacher.
func bloomHash(s string) (h1, h2 uint64) {
	f := fnv.New64a()
	_, _ = f.Write([]byte(s))
	h1 = f.Sum64()
	// derive h2 by hashing the bytes of h1
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], h1)
	f.Reset()
	_, _ = f.Write(b[:])
	h2 = f.Sum64()
	return
}

// ---------- per-(service,attr) tracker ----------

type attrTracker struct {
	mu    sync.Mutex
	bloom *bloomFilter
	uniq  uint64 // estimated distinct values accepted
	drops uint64 // values replaced due to limit
	limit int
}

func newAttrTracker(bits, k uint, limit int) *attrTracker {
	return &attrTracker{
		bloom: newBloomFilter(bits, k),
		limit: limit,
	}
}

// allow returns true when the value should pass through.
// If the value appears new AND we are at the limit, it returns false.
func (at *attrTracker) allow(value string) bool {
	at.mu.Lock()
	defer at.mu.Unlock()

	if at.bloom.test(value) {
		// Bloom positive → already seen (or false positive); always allow.
		return true
	}
	// Definitely a new value.
	if int(atomic.LoadUint64(&at.uniq)) >= at.limit {
		atomic.AddUint64(&at.drops, 1)
		return false
	}
	at.bloom.add(value)
	atomic.AddUint64(&at.uniq, 1)
	return true
}

// ---------- Prometheus metrics ----------

var (
	cardinalityDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "cardinality",
		Name:      "dropped_total",
		Help:      "Total span/metric/log attribute values replaced due to cardinality limit.",
	}, []string{"service", "attr_key"})

	cardinalityTrackersActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "javi",
		Subsystem: "cardinality",
		Name:      "trackers_active",
		Help:      "Number of active (service, attr_key) bloom filter trackers.",
	})
)

// ---------- CardinalityProcessor ----------

// CardinalityProcessor is a Processor that limits attribute value cardinality
// per (service_name, attribute_key) using bloom filters.
type CardinalityProcessor struct {
	limits   CardinalityLimits
	mu       sync.RWMutex
	trackers map[string]*attrTracker // key: "service::attr_key"
}

// NewCardinalityProcessor creates a ready-to-use cardinality processor.
func NewCardinalityProcessor(limits CardinalityLimits) *CardinalityProcessor {
	return &CardinalityProcessor{
		limits:   limits,
		trackers: make(map[string]*attrTracker),
	}
}

func (cp *CardinalityProcessor) tracker(service, attrKey string) *attrTracker {
	key := service + "::" + attrKey
	cp.mu.RLock()
	t, ok := cp.trackers[key]
	cp.mu.RUnlock()
	if ok {
		return t
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if t, ok = cp.trackers[key]; ok {
		return t
	}
	t = newAttrTracker(cp.limits.BloomFilterBits, cp.limits.BloomHashFunctions, cp.limits.PerServiceAttrLimit)
	cp.trackers[key] = t
	cardinalityTrackersActive.Set(float64(len(cp.trackers)))
	return t
}

// sanitizeAttrs replaces high-cardinality string values in-place.
func (cp *CardinalityProcessor) sanitizeAttrs(service string, attrs map[string]any) {
	for k, v := range attrs {
		strVal, ok := v.(string)
		if !ok {
			continue // only string values are subject to cardinality control
		}
		if !cp.tracker(service, k).allow(strVal) {
			attrs[k] = cardinalityPlaceholder
			cardinalityDroppedTotal.WithLabelValues(service, k).Inc()
			slog.Debug("cardinality limit: value replaced",
				"service", service, "attr", k, "limit", cp.limits.PerServiceAttrLimit)
		}
	}
}

// ProcessSpans implements Processor.
func (cp *CardinalityProcessor) ProcessSpans(_ context.Context, spans []*model.SpanData) ([]*model.SpanData, error) {
	for _, sp := range spans {
		cp.sanitizeAttrs(sp.ServiceName, sp.Attributes)
	}
	return spans, nil
}

// ProcessMetrics implements Processor.
func (cp *CardinalityProcessor) ProcessMetrics(_ context.Context, metrics []*model.MetricData) ([]*model.MetricData, error) {
	for _, m := range metrics {
		for i := range m.DataPoints {
			cp.sanitizeAttrs(m.ServiceName, m.DataPoints[i].Attributes)
		}
	}
	return metrics, nil
}

// ProcessLogs implements Processor.
func (cp *CardinalityProcessor) ProcessLogs(_ context.Context, logs []*model.LogData) ([]*model.LogData, error) {
	for _, l := range logs {
		cp.sanitizeAttrs(l.ServiceName, l.Attributes)
	}
	return logs, nil
}
