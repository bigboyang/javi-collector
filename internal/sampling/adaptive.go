package sampling

import (
	"sync"
	"time"
)

// AdaptiveController는 EWMA 기반으로 실시간 TPS를 추적하고
// 목표 TPS를 유지하도록 샘플링 레이트를 자동 조정한다.
//
// 알고리즘:
//  1. 매 tick(1초)마다 직전 1초간 Allow()가 true를 반환한 횟수 집계
//  2. EWMA로 평활화: ewma = α * currentTPS + (1-α) * ewma
//  3. 비율 조정: newRate = rate * (targetTPS / ewmaTPS)
//  4. [minRate, maxRate] 범위로 클램프
//
// EWMA α 선택:
//   - α=0.1: 느린 반응, 안정적 (burst 환경에 적합)
//   - α=0.3: 중간 (기본값)
//   - α=0.5: 빠른 반응, 진동 위험
type AdaptiveController struct {
	mu sync.Mutex

	cfg AdaptiveConfig

	currentRate float64 // 현재 샘플링 비율 [minRate, maxRate]
	ewmaTPS     float64 // EWMA 평활화된 TPS
	passCount   int64   // 현재 tick에서 통과(Allow=true)한 횟수
	lastTick    time.Time

	// Prometheus/Stats용 스냅샷
	lastRate float64
	lastTPS  float64
}

// NewAdaptiveController는 AdaptiveController를 생성한다.
// 비활성화(cfg.Enabled=false) 시에도 Allow()는 항상 true를 반환한다.
func NewAdaptiveController(cfg AdaptiveConfig) *AdaptiveController {
	initialRate := initialAdaptiveRate(cfg)
	return &AdaptiveController{
		cfg:         cfg,
		currentRate: initialRate,
		lastRate:    initialRate,
		lastTick:    time.Now(),
	}
}

// Allow는 현재 샘플링 레이트 기준으로 trace 통과 여부를 결정한다.
//
// Adaptive가 비활성화된 경우 항상 true를 반환한다.
// PolicyEvaluator.Evaluate로 이미 keep이 결정된 trace에 대해 호출해야 하며,
// critical trace(error/latency)는 호출자가 우회해야 한다.
func (a *AdaptiveController) Allow() bool {
	if !a.cfg.Enabled {
		return true
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// tick 경계: 1초가 지났으면 EWMA 업데이트 및 rate 재조정
	now := time.Now()
	if now.Sub(a.lastTick) >= time.Second {
		a.adjust(now)
	}

	// 결정적 counter 기반 샘플링:
	// rate=0.1이면 10개마다 1개 통과 → 균일 분포 보장
	// rate가 정수 역수가 아닐 때는 반올림 오차가 있지만 충분히 작다.
	threshold := int64(1.0/a.currentRate + 0.5) // round
	if threshold < 1 {
		threshold = 1
	}
	a.passCount++
	pass := (a.passCount % threshold) == 0
	return pass
}

// adjust는 EWMA TPS를 업데이트하고 샘플링 레이트를 재계산한다.
// mu 보유 상태에서 호출한다.
func (a *AdaptiveController) adjust(now time.Time) {
	elapsed := now.Sub(a.lastTick).Seconds()
	if elapsed <= 0 {
		return
	}

	// 직전 tick의 실제 TPS
	currentTPS := float64(a.passCount) / elapsed

	// EWMA 업데이트
	α := a.cfg.EWMAAlpha
	if a.ewmaTPS == 0 {
		a.ewmaTPS = currentTPS // cold start: 첫 관측값으로 초기화
	} else {
		a.ewmaTPS = α*currentTPS + (1-α)*a.ewmaTPS
	}

	// rate 재조정: target/actual 비율로 스케일
	// ewmaTPS가 0에 가까우면 rate를 max로 올려 수집 증가
	if a.ewmaTPS > 0 {
		ratio := a.cfg.TargetTPS / a.ewmaTPS
		a.currentRate = clamp(a.currentRate*ratio, a.cfg.MinRate, a.cfg.MaxRate)
	} else {
		a.currentRate = a.cfg.MaxRate
	}

	a.lastRate = a.currentRate
	a.lastTPS = a.ewmaTPS

	a.passCount = 0
	a.lastTick = now
}

// Stats는 현재 AdaptiveController 상태를 반환한다. (Prometheus 노출용)
func (a *AdaptiveController) Stats() (rate float64, tps float64) {
	a.mu.Lock()
	rate = a.lastRate
	tps = a.lastTPS
	a.mu.Unlock()
	return
}

// UpdateConfig는 remote config 변경 시 AdaptiveConfig를 원자적으로 교체한다.
// currentRate는 유지해 급격한 변화를 방지한다.
func (a *AdaptiveController) UpdateConfig(cfg AdaptiveConfig) {
	a.mu.Lock()
	defer a.mu.Unlock()

	wasEnabled := a.cfg.Enabled
	a.cfg = cfg
	if !wasEnabled && cfg.Enabled {
		// Adaptive가 런타임에 켜질 때 1.0에서 시작하면 첫 tick 동안 전량 통과한다.
		// cold start burst를 피하기 위해 새 설정의 MinRate에서 보수적으로 시작한다.
		a.currentRate = initialAdaptiveRate(cfg)
		a.ewmaTPS = 0
		a.passCount = 0
		a.lastTick = time.Now()
	}
	a.currentRate = clamp(a.currentRate, cfg.MinRate, cfg.MaxRate)
	a.lastRate = a.currentRate
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func initialAdaptiveRate(cfg AdaptiveConfig) float64 {
	if !cfg.Enabled {
		if cfg.MaxRate > 0 {
			return cfg.MaxRate
		}
		return 1.0
	}
	if cfg.MinRate > 0 {
		return cfg.MinRate
	}
	return 0.01
}
