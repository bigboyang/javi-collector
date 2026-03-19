package sampling

import (
	"math/rand/v2"

	"github.com/kkc/javi-collector/internal/model"
)

// Decision은 trace 보관/폐기 결정이다.
type Decision int8

const (
	DecisionDrop Decision = iota
	DecisionKeep
)

// PolicyEvaluator는 spans 슬라이스를 받아 keep/drop을 결정한다.
// 상태를 갖지 않는 순수 함수 집합이므로 goroutine-safe하다.
type PolicyEvaluator struct{}

// Evaluate는 cfg 기준으로 spans를 평가해 결정을 반환한다.
//
// 결정 우선순위: error > latency > probabilistic
// 하나라도 keep 조건을 만족하면 즉시 DecisionKeep을 반환한다.
// sampling이 비활성화(Enabled=false)이면 항상 DecisionKeep을 반환한다.
func (e PolicyEvaluator) Evaluate(spans []*model.SpanData, cfg *SamplingConfig) Decision {
	if !cfg.Enabled {
		return DecisionKeep
	}

	if cfg.ErrorSampling.Enabled && hasError(spans) {
		return DecisionKeep
	}

	if cfg.LatencySampling.Enabled && exceedsLatency(spans, cfg.LatencySampling.ThresholdMs) {
		return DecisionKeep
	}

	if cfg.ProbabilisticSampling.Enabled {
		if rand.Float64() < cfg.ProbabilisticSampling.Rate {
			return DecisionKeep
		}
		return DecisionDrop
	}

	// 모든 정책이 비활성화된 경우 drop (sampling은 enabled이나 정책 없음)
	return DecisionDrop
}

// hasError는 StatusCode == 2 (ERROR) span이 하나라도 있는지 검사한다.
func hasError(spans []*model.SpanData) bool {
	for _, s := range spans {
		if s.StatusCode == 2 {
			return true
		}
	}
	return false
}

// exceedsLatency는 trace duration이 임계값을 초과하는지 검사한다.
// root span(ParentSpanID=="")의 duration을 우선 사용하고,
// root span이 없으면 spans 중 최대 duration으로 대체한다.
func exceedsLatency(spans []*model.SpanData, thresholdMs int64) bool {
	thresholdNano := thresholdMs * 1_000_000
	for _, s := range spans {
		if s.ParentSpanID == "" {
			return s.DurationNano() > thresholdNano
		}
	}
	// root span 없음 → 가장 긴 span으로 대체
	for _, s := range spans {
		if s.DurationNano() > thresholdNano {
			return true
		}
	}
	return false
}

// isCritical은 trace가 error 또는 latency 정책으로 keep 조건을 만족하는지 확인한다.
// critical trace는 AdaptiveController를 우회해 항상 보존한다.
func isCritical(spans []*model.SpanData, cfg *SamplingConfig) bool {
	if cfg.ErrorSampling.Enabled && hasError(spans) {
		return true
	}
	if cfg.LatencySampling.Enabled && exceedsLatency(spans, cfg.LatencySampling.ThresholdMs) {
		return true
	}
	return false
}
