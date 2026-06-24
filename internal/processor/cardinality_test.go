package processor

import (
	"context"
	"testing"

	"github.com/kkc/javi-collector/internal/model"
)

func TestBloomFilterAddTest(t *testing.T) {
	bf := newBloomFilter(10_000, 4)

	if bf.test("alpha") {
		t.Fatal("test on empty filter should return false")
	}
	if isNew := bf.add("alpha"); !isNew {
		t.Fatal("first add should report isNew=true")
	}
	if !bf.test("alpha") {
		t.Fatal("test after add should return true")
	}
	if isNew := bf.add("alpha"); isNew {
		t.Fatal("re-adding the same value should report isNew=false")
	}
	if bf.n != 1 {
		t.Fatalf("estimated unique count = %d, want 1", bf.n)
	}
}

func TestAttrTrackerRespectsLimit(t *testing.T) {
	at := newAttrTracker(10_000, 4, 2)

	if !at.allow("v1") {
		t.Fatal("first distinct value should be allowed")
	}
	if !at.allow("v2") {
		t.Fatal("second distinct value should be allowed (at limit boundary)")
	}
	if at.allow("v3") {
		t.Fatal("third distinct value should be blocked (over limit)")
	}
	// Previously seen values must still pass through after the limit is hit.
	if !at.allow("v1") {
		t.Fatal("previously seen value should still be allowed")
	}
	if at.drops != 1 {
		t.Fatalf("drops = %d, want 1", at.drops)
	}
}

func TestCardinalityProcessorReplacesHighCardinality(t *testing.T) {
	limits := CardinalityLimits{PerServiceAttrLimit: 2, BloomFilterBits: 10_000, BloomHashFunctions: 4}
	cp := NewCardinalityProcessor(limits)

	spans := make([]*model.SpanData, 0, 4)
	for _, id := range []string{"u1", "u2", "u3", "u4"} {
		spans = append(spans, &model.SpanData{
			ServiceName: "svc",
			Attributes:  map[string]any{"user_id": id},
		})
	}

	out, err := cp.ProcessSpans(context.Background(), spans)
	if err != nil {
		t.Fatalf("ProcessSpans error: %v", err)
	}

	// First two distinct values pass; the rest are replaced with the placeholder.
	want := []string{"u1", "u2", cardinalityPlaceholder, cardinalityPlaceholder}
	for i, sp := range out {
		if got := sp.Attributes["user_id"]; got != want[i] {
			t.Errorf("span[%d] user_id = %q, want %q", i, got, want[i])
		}
	}
}

func TestCardinalityProcessorIgnoresNonString(t *testing.T) {
	cp := NewCardinalityProcessor(CardinalityLimits{PerServiceAttrLimit: 1, BloomFilterBits: 1000, BloomHashFunctions: 4})

	spans := []*model.SpanData{
		{ServiceName: "svc", Attributes: map[string]any{"count": 1}},
		{ServiceName: "svc", Attributes: map[string]any{"count": 2}},
		{ServiceName: "svc", Attributes: map[string]any{"count": 3}},
	}

	out, _ := cp.ProcessSpans(context.Background(), spans)
	for i, sp := range out {
		if _, replaced := sp.Attributes["count"].(string); replaced {
			t.Errorf("span[%d]: non-string value must never be replaced, got %v", i, sp.Attributes["count"])
		}
	}
}

func TestCardinalityProcessorIsolatesServiceAndKey(t *testing.T) {
	cp := NewCardinalityProcessor(CardinalityLimits{PerServiceAttrLimit: 1, BloomFilterBits: 10_000, BloomHashFunctions: 4})

	// Same value "x" under different (service, key) tuples must each get its own
	// budget — none should be replaced on first sight.
	spans := []*model.SpanData{
		{ServiceName: "a", Attributes: map[string]any{"k1": "x"}},
		{ServiceName: "b", Attributes: map[string]any{"k1": "x"}},
		{ServiceName: "a", Attributes: map[string]any{"k2": "x"}},
	}

	out, _ := cp.ProcessSpans(context.Background(), spans)
	for i, sp := range out {
		for k, v := range sp.Attributes {
			if v == cardinalityPlaceholder {
				t.Errorf("span[%d] key %q wrongly replaced; trackers must be isolated per (service,key)", i, k)
			}
		}
	}
}

func TestCardinalityProcessorMetricsAndLogs(t *testing.T) {
	cp := NewCardinalityProcessor(CardinalityLimits{PerServiceAttrLimit: 1, BloomFilterBits: 10_000, BloomHashFunctions: 4})

	metrics := []*model.MetricData{
		{ServiceName: "svc", DataPoints: []model.DataPoint{
			{Attributes: map[string]any{"region": "r1"}},
			{Attributes: map[string]any{"region": "r2"}},
		}},
	}
	mOut, err := cp.ProcessMetrics(context.Background(), metrics)
	if err != nil {
		t.Fatalf("ProcessMetrics error: %v", err)
	}
	if mOut[0].DataPoints[1].Attributes["region"] != cardinalityPlaceholder {
		t.Errorf("second metric region should be replaced, got %v", mOut[0].DataPoints[1].Attributes["region"])
	}

	logs := []*model.LogData{
		{ServiceName: "svc2", Attributes: map[string]any{"trace": "t1"}},
		{ServiceName: "svc2", Attributes: map[string]any{"trace": "t2"}},
	}
	lOut, err := cp.ProcessLogs(context.Background(), logs)
	if err != nil {
		t.Fatalf("ProcessLogs error: %v", err)
	}
	if lOut[1].Attributes["trace"] != cardinalityPlaceholder {
		t.Errorf("second log trace should be replaced, got %v", lOut[1].Attributes["trace"])
	}
}
