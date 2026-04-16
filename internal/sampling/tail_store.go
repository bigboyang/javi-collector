package sampling

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/kkc/javi-collector/internal/model"
	"github.com/kkc/javi-collector/internal/store"
)

// ---- Prometheus 지표 ----
var (
	samplingDecisionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "sampling",
		Name:      "decisions_total",
		Help:      "Total tail sampling decisions by result (keep|drop|adaptive_drop).",
	}, []string{"result"})

	samplingBufferSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "javi",
		Subsystem: "sampling",
		Name:      "buffer_traces",
		Help:      "Current number of traces buffered in tail sampler.",
	})

	samplingAdaptiveRate = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "javi",
		Subsystem: "sampling",
		Name:      "adaptive_rate",
		Help:      "Current adaptive sampling rate [0,1].",
	})

	samplingAdaptiveTPS = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "javi",
		Subsystem: "sampling",
		Name:      "adaptive_observed_tps",
		Help:      "EWMA-smoothed observed TPS in the adaptive sampler.",
	})

	// URL 패턴 제외로 버퍼링 전에 드롭된 span 수
	samplingURLExcludedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "sampling",
		Name:      "url_excluded_spans_total",
		Help:      "Total spans dropped before buffering due to exclude_url_patterns match.",
	})
)

// TailSamplingStore는 store.TraceStore를 래핑하는 Decorator다.
//
// Ingester는 TailSamplingStore를 일반 TraceStore처럼 사용하므로
// 기존 코드를 변경하지 않고 sampling 레이어를 투명하게 삽입할 수 있다.
//
// 내부 파이프라인:
//
//	AppendSpans → traceBuffer (traceId별 버퍼링)
//	  → (root span 수신 or timeout 만료)
//	  → PolicyEvaluator.Evaluate → DecisionKeep
//	  → AdaptiveController.Allow (critical trace 우회)
//	  → downstream.AppendSpans
type TailSamplingStore struct {
	downstream store.TraceStore
	buffer     *traceBuffer
	policy     PolicyEvaluator
	adaptive   *AdaptiveController
	cfgProv    ConfigProvider

	// goroutine 생명주기
	stopCh chan struct{}
	doneCh chan struct{}

	// 통계 카운터 (atomic)
	keptTotal    atomic.Int64
	droppedTotal atomic.Int64
}

// NewTailSamplingStore는 TailSamplingStore를 생성한다.
//
// cfgProv가 nil이면 sampling이 비활성화된 기본 config를 사용한다.
// Start(ctx)를 호출해야 만료 처리 goroutine이 시작된다.
func NewTailSamplingStore(downstream store.TraceStore, cfgProv ConfigProvider) *TailSamplingStore {
	if cfgProv == nil {
		cfgProv = staticConfigProvider{cfg: defaultDisabledConfig()}
	}

	cfg := cfgProv.Current()
	timeout := time.Duration(cfg.TraceTimeoutSec) * time.Second
	maxBuf := cfg.MaxBufferTraces

	ts := &TailSamplingStore{
		downstream: downstream,
		cfgProv:    cfgProv,
		adaptive:   NewAdaptiveController(cfg.Adaptive),
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
	ts.buffer = newTraceBuffer(maxBuf, timeout, ts.onFlush)
	return ts
}

// AppendSpans는 store.TraceStore 인터페이스를 구현한다.
//
// ExcludeURLPatterns에 매칭되는 spans를 버퍼링 전에 제거한 뒤 나머지를 버퍼에 추가한다.
// sampling 결정은 비동기로 발생하므로 항상 nil을 반환한다.
// (에러 반환 시 OTLP 클라이언트가 재전송을 시도해 중복 수집이 발생한다.)
func (ts *TailSamplingStore) AppendSpans(ctx context.Context, spans []*model.SpanData) error {
	cfg := ts.cfgProv.Current()
	if len(cfg.ExcludeURLPatterns) > 0 {
		filtered := make([]*model.SpanData, 0, len(spans))
		for _, sp := range spans {
			if spanMatchesExcludePattern(sp, cfg.ExcludeURLPatterns) {
				samplingURLExcludedTotal.Inc()
				continue
			}
			filtered = append(filtered, sp)
		}
		spans = filtered
	}
	if len(spans) == 0 {
		return nil
	}

	ts.buffer.append(spans)

	// root span이 포함된 경우 즉시 flush 시도 (timeout 대기 단축)
	for _, sp := range spans {
		if sp.ParentSpanID == "" {
			ts.buffer.tryFlushComplete()
			break
		}
	}
	return nil
}

// QuerySpans는 downstream에 위임한다.
// sampling으로 drop된 spans는 downstream에 없으므로 쿼리에서도 제외된다.
func (ts *TailSamplingStore) QuerySpans(ctx context.Context, q store.SpanQuery) ([]*model.SpanData, error) {
	return ts.downstream.QuerySpans(ctx, q)
}

// Close는 goroutine을 중단하고, 버퍼 잔여 trace를 force-flush한 뒤 downstream을 닫는다.
//
// 종료 순서:
//  1. stopCh 닫기 → expiryLoop 종료 신호
//  2. doneCh 대기 → expiryLoop 완전 종료 확인
//  3. FlushAll() → timeout 미만 잔여 trace 즉시 flush
//  4. Wait() → 모든 in-flight onFlush goroutine 완료 대기
//  5. downstream.Close()
func (ts *TailSamplingStore) Close() error {
	close(ts.stopCh)
	<-ts.doneCh // expiryLoop 종료 보장 — 이후 FlushExpired 호출 없음

	remaining := ts.buffer.Size()
	if remaining > 0 {
		slog.Info("tail sampler force-flushing remaining traces on shutdown", "traces", remaining)
	}
	ts.buffer.FlushAll() // 잔여 trace 방출 (만료 여부 무관)
	ts.buffer.Wait()     // in-flight onFlush goroutine 완료 대기

	return ts.downstream.Close()
}

// Start는 만료 처리 goroutine을 시작한다.
// NewTailSamplingStore 직후 호출해야 한다.
func (ts *TailSamplingStore) Start(ctx context.Context) {
	go ts.expiryLoop(ctx)
}

// WatchConfig는 ConfigProvider의 onChange 채널을 감시해 AdaptiveController를 업데이트한다.
// RemoteConfigPoller 사용 시 Start 이후에 호출한다.
func (ts *TailSamplingStore) WatchConfig(onChange <-chan struct{}) {
	go func() {
		for {
			select {
			case <-ts.stopCh:
				return
			case _, ok := <-onChange:
				if !ok {
					return
				}
				cfg := ts.cfgProv.Current()
				ts.adaptive.UpdateConfig(cfg.Adaptive)
				slog.Info("sampling config applied",
					"enabled", cfg.Enabled,
					"target_tps", cfg.Adaptive.TargetTPS,
					"prob_rate", cfg.ProbabilisticSampling.Rate,
					"latency_ms", cfg.LatencySampling.ThresholdMs,
				)
			}
		}
	}()
}

// Stats는 현재 sampling 통계를 반환한다.
func (ts *TailSamplingStore) Stats() (kept, dropped int64) {
	return ts.keptTotal.Load(), ts.droppedTotal.Load()
}

// expiryLoop는 주기적으로 만료 trace flush와 Prometheus 지표를 업데이트한다.
func (ts *TailSamplingStore) expiryLoop(ctx context.Context) {
	defer close(ts.doneCh)

	expiryTicker := time.NewTicker(time.Second)
	defer expiryTicker.Stop()

	metricsTicker := time.NewTicker(5 * time.Second)
	defer metricsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ts.stopCh:
			return
		case <-expiryTicker.C:
			ts.buffer.FlushExpired()
		case <-metricsTicker.C:
			ts.updateMetrics()
		}
	}
}

// onFlush는 traceBuffer의 콜백으로 호출된다.
// 서비스별 오버라이드를 적용한 뒤 PolicyEvaluator와 AdaptiveController를 통과한 spans만 downstream으로 전달한다.
func (ts *TailSamplingStore) onFlush(spans []*model.SpanData) {
	if len(spans) == 0 {
		return
	}

	cfg := ts.cfgProv.Current()

	// 서비스별 오버라이드 적용: 첫 번째 매칭 ServiceSamplingRule의 정책을 사용한다.
	// Adaptive TPS 제어는 항상 전역 cfg 기준으로 동작한다.
	svcName := spanServiceName(spans)
	effectiveCfg := resolveEffective(svcName, cfg)

	decision := ts.policy.Evaluate(spans, effectiveCfg)

	if decision == DecisionDrop {
		ts.droppedTotal.Add(1)
		samplingDecisionsTotal.WithLabelValues("drop").Inc()
		return
	}

	// critical trace(error/latency)는 AdaptiveController를 우회한다.
	// SLA 위반 신호가 rate 조정에 의해 drop되면 alert 누락이 발생한다.
	if cfg.Adaptive.Enabled && !isCritical(spans, effectiveCfg) {
		if !ts.adaptive.Allow() {
			ts.droppedTotal.Add(1)
			samplingDecisionsTotal.WithLabelValues("adaptive_drop").Inc()
			return
		}
	}

	ts.keptTotal.Add(1)
	samplingDecisionsTotal.WithLabelValues("keep").Inc()

	ctx := context.Background()
	if err := ts.downstream.AppendSpans(ctx, spans); err != nil {
		slog.Warn("tail sampler downstream write failed", "err", err, "spans", len(spans))
	}
}

func (ts *TailSamplingStore) updateMetrics() {
	samplingBufferSize.Set(float64(ts.buffer.Size()))
	rate, tps := ts.adaptive.Stats()
	samplingAdaptiveRate.Set(rate)
	samplingAdaptiveTPS.Set(tps)
}
