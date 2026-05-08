package sampling

import (
	"testing"

	"github.com/kkc/javi-collector/internal/model"
)

func TestEvaluate_ProbabilisticSampling_Deterministic(t *testing.T) {
	eval := PolicyEvaluator{}
	cfg := &SamplingConfig{
		Enabled: true,
		ProbabilisticSampling: ProbabilisticSamplingConfig{
			Enabled: true,
			Rate:    0.5,
		},
	}

	// Case 1: TraceID that should be kept (hash < threshold)
	// threshold = 0.5 * MaxUint64 = 9223372036854775807
	// last 16 hex chars "0000000000000001" → BigEndian uint64 = 1 < threshold
	traceKeep := "4bf92f3577b34da60000000000000001"
	spansKeep := []*model.SpanData{{TraceID: traceKeep}}

	if eval.Evaluate(spansKeep, cfg) != DecisionKeep {
		t.Errorf("expected DecisionKeep for trace %s", traceKeep)
	}

	// Case 2: TraceID that should be dropped (hash > threshold)
	// TraceID ending in "ffffffffffffffff" -> hash = MaxUint64
	traceDrop := "4bf92f3577b34da6a3ce929dffffffff"
	spansDrop := []*model.SpanData{{TraceID: traceDrop}}

	if eval.Evaluate(spansDrop, cfg) != DecisionDrop {
		t.Errorf("expected DecisionDrop for trace %s", traceDrop)
	}

	// Case 3: Same TraceID should always yield same decision
	for i := 0; i < 100; i++ {
		if eval.Evaluate(spansKeep, cfg) != DecisionKeep {
			t.Errorf("non-deterministic decision for keep trace at iteration %d", i)
		}
		if eval.Evaluate(spansDrop, cfg) != DecisionDrop {
			t.Errorf("non-deterministic decision for drop trace at iteration %d", i)
		}
	}
}

func TestTraceIDHash(t *testing.T) {
	tests := []struct {
		traceID  string
		expected uint64
	}{
		{"00000000000000000000000000000001", 1},
		{"0000000000000000ffffffffffffffff", 18446744073709551615},
		// len < 32: 비표준 TraceID → 0 반환 (guard)
		{"invalid", 0},
		{"short", 0},
		{"0000000000000001", 0}, // 16자: 표준 32자 미만 → 0
	}

	for _, tt := range tests {
		got := traceIDHash(tt.traceID)
		if got != tt.expected {
			t.Errorf("traceIDHash(%s) = %d; want %d", tt.traceID, got, tt.expected)
		}
	}
}

func TestEvaluate_ProbabilisticSampling_BoundaryRates(t *testing.T) {
	eval := PolicyEvaluator{}
	traceID := "4bf92f3577b34da6a3ce929dffffffff" // hash = MaxUint64 (항상 드롭 경계)

	// Rate=1.0: float64 overflow 없이 반드시 KEEP
	cfg1 := &SamplingConfig{
		Enabled: true,
		ProbabilisticSampling: ProbabilisticSamplingConfig{Enabled: true, Rate: 1.0},
	}
	if eval.Evaluate([]*model.SpanData{{TraceID: traceID}}, cfg1) != DecisionKeep {
		t.Error("Rate=1.0 must always return DecisionKeep (overflow bug regression)")
	}

	// Rate=0.0: 반드시 DROP
	cfg0 := &SamplingConfig{
		Enabled: true,
		ProbabilisticSampling: ProbabilisticSamplingConfig{Enabled: true, Rate: 0.0},
	}
	if eval.Evaluate([]*model.SpanData{{TraceID: "4bf92f3577b34da60000000000000001"}}, cfg0) != DecisionDrop {
		t.Error("Rate=0.0 must always return DecisionDrop")
	}
}
