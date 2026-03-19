// Package sampling은 Tail Sampling과 Adaptive Sampling을 구현한다.
//
// 파이프라인:
//
//	Ingester → TailSamplingStore.AppendSpans
//	  → traceBuffer (traceId별 버퍼링)
//	  → (root span 수신 or timeout 만료)
//	  → PolicyEvaluator.Evaluate → keep/drop
//	  → AdaptiveController.Allow (TPS gate)
//	  → downstream TraceStore
//
// 동적 설정은 RemoteConfigPoller가 HTTP GET으로 주기적으로 폴링한다.
package sampling

import "encoding/json"

// SamplingConfig는 remote config 서버에서 폴링하는 샘플링 설정 JSON 스키마다.
//
// JSON 예시:
//
//	{
//	  "enabled": true,
//	  "error_sampling": { "enabled": true },
//	  "latency_sampling": { "enabled": true, "threshold_ms": 500 },
//	  "probabilistic_sampling": { "enabled": true, "rate": 0.1 },
//	  "adaptive": {
//	    "enabled": true,
//	    "target_tps": 1000,
//	    "min_rate": 0.01,
//	    "max_rate": 1.0,
//	    "ewma_alpha": 0.3
//	  },
//	  "trace_timeout_sec": 30,
//	  "max_buffer_traces": 50000
//	}
type SamplingConfig struct {
	// Enabled=false이면 전량 통과(no-op). remote config 장애 시 fail-open 전략.
	Enabled bool `json:"enabled"`

	ErrorSampling         ErrorSamplingConfig         `json:"error_sampling"`
	LatencySampling       LatencySamplingConfig       `json:"latency_sampling"`
	ProbabilisticSampling ProbabilisticSamplingConfig `json:"probabilistic_sampling"`
	Adaptive              AdaptiveConfig              `json:"adaptive"`

	// trace 완성 대기 timeout. 초과 시 수집된 spans로 즉시 결정. (기본 30s)
	TraceTimeoutSec int `json:"trace_timeout_sec"`

	// 동시에 버퍼에 보관할 최대 trace 수. 초과 시 가장 오래된 trace 강제 flush. (기본 50000)
	MaxBufferTraces int `json:"max_buffer_traces"`
}

// ErrorSamplingConfig는 error span 포함 trace를 항상 보존하는 설정이다.
type ErrorSamplingConfig struct {
	Enabled bool `json:"enabled"`
}

// LatencySamplingConfig는 임계값 초과 지연 trace를 항상 보존하는 설정이다.
type LatencySamplingConfig struct {
	Enabled     bool  `json:"enabled"`
	ThresholdMs int64 `json:"threshold_ms"`
}

// ProbabilisticSamplingConfig는 확률적 샘플링 설정이다.
type ProbabilisticSamplingConfig struct {
	Enabled bool    `json:"enabled"`
	Rate    float64 `json:"rate"` // [0.0, 1.0]
}

// AdaptiveConfig는 EWMA 기반 TPS 추적으로 샘플링 레이트를 자동 조정하는 설정이다.
type AdaptiveConfig struct {
	Enabled   bool    `json:"enabled"`
	TargetTPS float64 `json:"target_tps"` // 목표 초당 trace 통과 수
	MinRate   float64 `json:"min_rate"`   // 최소 샘플링 비율 (기본 0.01)
	MaxRate   float64 `json:"max_rate"`   // 최대 샘플링 비율 (기본 1.0)
	EWMAAlpha float64 `json:"ewma_alpha"` // EWMA 평활 계수 (기본 0.3)
}

// defaults는 zero value 필드를 기본값으로 채운다.
// json.Unmarshal에서 생략된 숫자 필드는 0이 되므로 명시적으로 보정한다.
func (c *SamplingConfig) defaults() {
	if c.TraceTimeoutSec <= 0 {
		c.TraceTimeoutSec = 30
	}
	if c.MaxBufferTraces <= 0 {
		c.MaxBufferTraces = 50_000
	}
	if c.Adaptive.MinRate <= 0 {
		c.Adaptive.MinRate = 0.01
	}
	if c.Adaptive.MaxRate <= 0 {
		c.Adaptive.MaxRate = 1.0
	}
	if c.Adaptive.EWMAAlpha <= 0 {
		c.Adaptive.EWMAAlpha = 0.3
	}
}

// ParseConfig는 JSON bytes를 SamplingConfig로 파싱하고 기본값을 적용한다.
func ParseConfig(data []byte) (*SamplingConfig, error) {
	var cfg SamplingConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	cfg.defaults()
	return &cfg, nil
}

// NewDefaultConfig는 enabled 플래그만 설정된 기본 SamplingConfig를 반환한다.
// remote config 폴링 전 초기값으로 사용한다.
func NewDefaultConfig(enabled bool) *SamplingConfig {
	cfg := &SamplingConfig{Enabled: enabled}
	cfg.defaults()
	return cfg
}

// ConfigProvider는 현재 유효한 SamplingConfig를 반환하는 인터페이스다.
// TailSamplingStore는 이 인터페이스만 의존해 RemoteConfigPoller와 결합도를 낮춘다.
type ConfigProvider interface {
	Current() *SamplingConfig
}

// staticConfigProvider는 불변 config를 반환하는 테스트/기본값용 provider다.
type staticConfigProvider struct {
	cfg *SamplingConfig
}

func (s staticConfigProvider) Current() *SamplingConfig { return s.cfg }

// defaultDisabledConfig는 sampling이 비활성화된 기본 config를 반환한다.
func defaultDisabledConfig() *SamplingConfig {
	cfg := &SamplingConfig{Enabled: false}
	cfg.defaults()
	return cfg
}
