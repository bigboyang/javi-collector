package sampling

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// RemoteConfigPoller는 HTTP GET으로 SamplingConfig를 주기적으로 폴링한다.
//
// 생명주기:
//  1. NewRemoteConfigPoller: 초기 config 설정
//  2. Start(ctx): 백그라운드 goroutine 시작, 시작 즉시 1회 폴링
//  3. ctx 취소 또는 Stop() 호출 → goroutine 정리 후 종료
//
// 보장:
//   - Current()는 항상 nil이 아닌 포인터를 반환한다 (초기값 보장)
//   - 폴링 실패 시 이전 config를 유지한다 (fail-safe)
//   - Stop()은 goroutine이 완전히 종료될 때까지 블로킹한다 (goroutine leak 방지)
//   - url이 빈 문자열이면 폴링하지 않고 initial config를 유지한다
//
// ConfigProvider 인터페이스를 구현하므로 TailSamplingStore에 직접 주입 가능하다.
type RemoteConfigPoller struct {
	url          string
	pollInterval time.Duration
	httpClient   *http.Client

	// atomic.Pointer: 읽기 경로(hot path)에서 lock-free O(1) 접근
	// 쓰기는 poller goroutine 하나만 수행 → race-free
	current atomic.Pointer[SamplingConfig]

	// onChange: config 변경 시 TailSamplingStore가 AdaptiveController를 업데이트하도록 알림
	// 버퍼 1: 빠른 연속 변경 시 poller 블로킹 방지
	onChange chan struct{}

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewRemoteConfigPoller는 poller를 생성한다.
// url이 빈 문자열이면 폴링을 수행하지 않고 initial config를 유지한다.
func NewRemoteConfigPoller(url string, interval time.Duration, initial *SamplingConfig) *RemoteConfigPoller {
	p := &RemoteConfigPoller{
		url:          url,
		pollInterval: interval,
		httpClient: &http.Client{
			Timeout: 10 * time.Second, // 개별 폴링 요청 타임아웃
		},
		onChange: make(chan struct{}, 1),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	p.current.Store(initial)
	return p
}

// Current는 현재 유효한 SamplingConfig를 반환한다.
// atomic load이므로 뮤텍스 없이 any goroutine에서 호출 가능하다.
// ConfigProvider 인터페이스를 구현한다.
func (p *RemoteConfigPoller) Current() *SamplingConfig {
	return p.current.Load()
}

// OnChange는 config가 변경될 때 신호를 보내는 읽기 전용 채널을 반환한다.
func (p *RemoteConfigPoller) OnChange() <-chan struct{} {
	return p.onChange
}

// Start는 백그라운드 폴링 goroutine을 시작한다.
// url이 빈 문자열이면 즉시 반환한다.
func (p *RemoteConfigPoller) Start(ctx context.Context) {
	if p.url == "" {
		close(p.doneCh)
		return
	}

	go func() {
		defer close(p.doneCh)

		// 시작 즉시 1회 폴링 (ticker의 첫 이벤트는 interval 후에 발생하므로)
		p.poll()

		ticker := time.NewTicker(p.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case <-ticker.C:
				p.poll()
			}
		}
	}()
}

// Stop은 폴링을 중단하고 goroutine이 완전히 종료될 때까지 대기한다.
func (p *RemoteConfigPoller) Stop() {
	select {
	case <-p.stopCh: // 이미 닫혀 있으면 무시 (중복 호출 방어)
	default:
		close(p.stopCh)
	}
	<-p.doneCh
}

// poll은 HTTP GET으로 config를 가져와 atomic 교체한다.
// 실패 시 경고 로그만 남기고 이전 config를 유지한다.
func (p *RemoteConfigPoller) poll() {
	resp, err := p.httpClient.Get(p.url)
	if err != nil {
		slog.Warn("remote config poll failed", "url", p.url, "err", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Warn("remote config non-200", "url", p.url, "status", resp.StatusCode)
		return
	}

	// 응답 크기를 64KB로 제한: 비정상 응답으로부터 OOM 방어
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		slog.Warn("remote config read body failed", "err", err)
		return
	}

	newCfg, err := ParseConfig(body)
	if err != nil {
		slog.Warn("remote config parse failed", "err", err, "body_len", len(body))
		return
	}

	old := p.current.Load()
	p.current.Store(newCfg)

	if old == nil || configChanged(old, newCfg) {
		select {
		case p.onChange <- struct{}{}:
		default: // 이미 신호 대기 중이면 중복 전송 생략
		}
		slog.Info("remote sampling config updated",
			"enabled", newCfg.Enabled,
			"prob_rate", newCfg.ProbabilisticSampling.Rate,
			"target_tps", newCfg.Adaptive.TargetTPS,
			"latency_ms", newCfg.LatencySampling.ThresholdMs,
		)
	}
}

// configChanged는 두 config 간에 의미 있는 변경이 있는지 확인한다.
// deep equal 대신 주요 필드만 비교해 불필요한 업데이트를 줄인다.
func configChanged(old, new *SamplingConfig) bool {
	return old.Enabled != new.Enabled ||
		old.Adaptive.Enabled != new.Adaptive.Enabled ||
		old.Adaptive.TargetTPS != new.Adaptive.TargetTPS ||
		old.Adaptive.EWMAAlpha != new.Adaptive.EWMAAlpha ||
		old.ProbabilisticSampling.Enabled != new.ProbabilisticSampling.Enabled ||
		old.ProbabilisticSampling.Rate != new.ProbabilisticSampling.Rate ||
		old.LatencySampling.Enabled != new.LatencySampling.Enabled ||
		old.LatencySampling.ThresholdMs != new.LatencySampling.ThresholdMs ||
		old.ErrorSampling.Enabled != new.ErrorSampling.Enabled ||
		old.TraceTimeoutSec != new.TraceTimeoutSec ||
		old.MaxBufferTraces != new.MaxBufferTraces
}
