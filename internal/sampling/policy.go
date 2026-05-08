package sampling

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"path"

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
		if len(spans) > 0 {
			rate := cfg.ProbabilisticSampling.Rate
			// 경계값 처리: Rate=1.0 시 float64(math.MaxUint64) > MaxUint64이므로
			// uint64 변환이 0으로 오버플로 → threshold=0 → hash<0 불가 → 모든 trace DROP.
			// OTel TraceIdRatioBasedSampler 스펙과 동일하게 경계값을 명시 처리한다.
			if rate >= 1.0 {
				return DecisionKeep
			}
			if rate <= 0.0 {
				return DecisionDrop
			}
			// TraceID 기반 결정론적 샘플링.
			// 동일 TraceID를 가진 Span들은 항상 같은 샘플링 결정을 받는다.
			// SDK의 TraceIdRatioBasedSampler와 호환되는 방식.
			hash := traceIDHash(spans[0].TraceID)
			threshold := uint64(rate * math.MaxUint64)
			if hash < threshold {
				return DecisionKeep
			}
			return DecisionDrop
		}
	}

	// 모든 정책이 비활성화된 경우 drop (sampling은 enabled이나 정책 없음)
	return DecisionDrop
}

// traceIDHash는 16진수 TraceID 스트링(128bit)의 하위 64bit를 uint64 해시로 변환한다.
// SDK의 TraceIdRatioBasedSampler 알고리즘(하위 8바이트 빅엔디안 정수화)과 일치한다.
// 표준 OTel TraceID는 32 hex chars(128bit)이며, 그 미만이면 0을 반환한다(→ 항상 KEEP 편향).
func traceIDHash(traceID string) uint64 {
	if len(traceID) < 32 {
		return 0
	}
	// 하위 16문자 (8바이트) 사용
	b, err := hex.DecodeString(traceID[len(traceID)-16:])
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
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

// resolveEffective는 spans의 서비스명에 매칭되는 첫 번째 ServiceSamplingRule을
// 전역 cfg에 오버라이드한 effective config를 반환한다.
// 매칭 규칙이 없으면 cfg를 그대로 반환한다 (복사 없음).
// ServiceRules는 순서대로 평가하며 첫 번째 매칭 규칙이 우선한다.
func resolveEffective(serviceName string, cfg *SamplingConfig) *SamplingConfig {
	for _, rule := range cfg.ServiceRules {
		matched, _ := path.Match(rule.ServicePattern, serviceName)
		if !matched {
			continue
		}
		merged := *cfg // shallow copy — ServiceRules/ExcludeURLPatterns는 포인터 공유
		merged.ErrorSampling = rule.Override.ErrorSampling
		merged.LatencySampling = rule.Override.LatencySampling
		merged.ProbabilisticSampling = rule.Override.ProbabilisticSampling
		return &merged
	}
	return cfg
}

// spanServiceName은 spans에서 서비스명을 추출한다.
// root span(ParentSpanID=="") 우선, 없으면 첫 번째 span을 사용한다.
func spanServiceName(spans []*model.SpanData) string {
	for _, s := range spans {
		if s.ParentSpanID == "" {
			return s.ServiceName
		}
	}
	if len(spans) > 0 {
		return spans[0].ServiceName
	}
	return ""
}

// spanMatchesExcludePattern은 span name 또는 URL 어트리뷰트가 패턴과 일치하면 true를 반환한다.
// 패턴 문법은 path.Match (glob: *, ?)이며 슬래시도 *로 매칭된다.
// 검사 순서: span.Name → http.url → url.path → http.target → url.full
func spanMatchesExcludePattern(sp *model.SpanData, patterns []string) bool {
	// span name 검사 (e.g. "GET /health", "grpc.health.v1.Health/Check")
	for _, p := range patterns {
		if m, _ := path.Match(p, sp.Name); m {
			return true
		}
	}
	// HTTP/URL 어트리뷰트 검사
	for _, attr := range []string{"http.url", "url.path", "http.target", "url.full"} {
		v, ok := sp.Attributes[attr]
		if !ok {
			continue
		}
		s, ok := v.(string)
		if !ok || s == "" {
			continue
		}
		for _, p := range patterns {
			if m, _ := path.Match(p, s); m {
				return true
			}
		}
	}
	return false
}
