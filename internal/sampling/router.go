package sampling

import (
	"bytes"
	"context"
	"hash/fnv"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// RoutedHeader는 이미 라우팅된 요청에 설정하는 헤더다.
// 피어가 이 헤더를 확인해 무한 전달 루프를 방지한다.
const RoutedHeader = "X-Javi-Routed"

var (
	routingForwardedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "sampling",
		Name:      "routing_forwarded_total",
		Help:      "Total number of OTLP trace requests forwarded to peer collectors.",
	})
	routingForwardErrorTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "sampling",
		Name:      "routing_forward_error_total",
		Help:      "Total number of forwarding errors to peer collectors.",
	})
	routingCBDropTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "sampling",
		Name:      "routing_cb_drop_total",
		Help:      "Total forwarding attempts dropped due to open peer circuit breaker.",
	})
)

// peerCircuitBreaker는 피어별 Circuit Breaker 상태를 관리한다.
//
// 상태 전환:
//
//	Closed  → (연속 failCount ≥ threshold) → Open  (openUntil 설정)
//	Open    → (cooldown 경과)             → Half-Open (openUntil 초과 여부로 판단)
//	Half-Open → (다음 시도 성공)           → Closed
//	Half-Open → (다음 시도 실패)           → Open  (cooldown 재시작)
type peerCircuitBreaker struct {
	mu        sync.Mutex
	failCount int
	openUntil time.Time // zero → closed; 미래 시각 → open
}

// allow는 현재 전달을 허용할지 반환한다.
// Closed 또는 cooldown이 경과한 Half-Open 상태이면 true를 반환한다.
func (cb *peerCircuitBreaker) allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.openUntil.IsZero() || time.Now().After(cb.openUntil)
}

// onSuccess는 전달 성공 시 circuit을 닫고 failCount를 초기화한다.
func (cb *peerCircuitBreaker) onSuccess() {
	cb.mu.Lock()
	cb.failCount = 0
	cb.openUntil = time.Time{}
	cb.mu.Unlock()
}

// onFailure는 전달 실패 시 failCount를 증가시키고 임계값 도달 시 circuit을 연다.
// 열린 경우 peerURL 로그를 남기기 위해 인자로 받는다.
func (cb *peerCircuitBreaker) onFailure(peerURL string, threshold int, cooldown time.Duration) {
	cb.mu.Lock()
	cb.failCount++
	if cb.failCount >= threshold {
		cb.openUntil = time.Now().Add(cooldown)
		cb.failCount = 0
		cb.mu.Unlock()
		slog.Warn("trace router: peer circuit breaker opened",
			"peer", peerURL, "cooldown", cooldown)
		return
	}
	cb.mu.Unlock()
}

// TraceRouter는 traceID 기반 일관 해시(FNV-1a)로 spans를 피어 인스턴스에 라우팅한다.
//
// 멀티 인스턴스 배포에서 로드밸런서가 동일 trace의 spans를 여러 인스턴스에 분산하면
// TailSampling 결정이 불완전한 데이터에 기반하게 된다.
// TraceRouter는 동일 traceID의 모든 spans를 항상 같은 인스턴스로 모은다.
//
// 일관 해시 알고리즘:
//   - 모든 피어 URL(self 포함)을 사전순 정렬 → 모든 인스턴스에서 동일한 해시 링 구성
//   - FNV-1a(traceID) % len(peers) → 담당 인스턴스 결정
//   - 동일 traceID는 항상 동일 인스턴스로 라우팅
//
// 피어 Circuit Breaker:
//   - 연속 N회 전달 실패 시 해당 피어로의 전달을 일시 차단 (cooldown 동안)
//   - cooldown 경과 후 자동 재시도 (half-open)
//   - 피어 장애 시 해당 spans는 드롭 (fail-open: 불완전 trace로 처리)
type TraceRouter struct {
	selfURL    string
	peers      []string // selfURL 포함한 정렬된 피어 URL 목록
	httpClient *http.Client

	// per-peer circuit breaker
	cbThreshold int
	cbCooldown  time.Duration
	peerBreakers map[string]*peerCircuitBreaker
}

// NewTraceRouter는 TraceRouter를 생성한다.
//
// selfURL은 현재 인스턴스의 HTTP base URL (e.g., "http://collector-0:4318")
// peerURLs는 다른 인스턴스들의 URL 목록 (자신과 중복 시 자동 제거)
// cbThreshold: 연속 실패 횟수 임계값 (0이면 CB 비활성화)
// cbCooldown: circuit open 유지 시간
// selfURL이 비어 있거나 피어가 1개 이하면 라우팅이 비활성화된다.
func NewTraceRouter(selfURL string, peerURLs []string, cbThreshold int, cbCooldown time.Duration) *TraceRouter {
	seen := make(map[string]struct{})
	all := make([]string, 0, len(peerURLs)+1)

	if selfURL != "" {
		all = append(all, selfURL)
		seen[selfURL] = struct{}{}
	}
	for _, p := range peerURLs {
		p = strings.TrimRight(p, "/")
		if p != "" {
			if _, dup := seen[p]; !dup {
				all = append(all, p)
				seen[p] = struct{}{}
			}
		}
	}
	sort.Strings(all) // 모든 인스턴스에서 동일한 순서 보장

	// 피어별 circuit breaker 초기화 (self 제외)
	breakers := make(map[string]*peerCircuitBreaker, len(all))
	for _, p := range all {
		if p != selfURL {
			breakers[p] = &peerCircuitBreaker{}
		}
	}

	return &TraceRouter{
		selfURL: selfURL,
		peers:   all,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		cbThreshold:  cbThreshold,
		cbCooldown:   cbCooldown,
		peerBreakers: breakers,
	}
}

// Enabled는 라우팅이 활성화되어 있는지 반환한다.
// selfURL이 설정되고 피어가 2개 이상(self 포함)일 때 활성화된다.
func (r *TraceRouter) Enabled() bool {
	return r.selfURL != "" && len(r.peers) > 1
}

// ownerFor는 traceID를 FNV-1a 해시로 매핑해 담당 피어 URL을 반환한다.
func (r *TraceRouter) ownerFor(traceID string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(traceID))
	idx := int(h.Sum32()) % len(r.peers)
	return r.peers[idx]
}

// Route는 OTLP 요청을 traceID별 owner 인스턴스로 분리한다.
//
// 반환값:
//   - localReq: 현재 인스턴스(self)가 처리할 spans
//   - remote: peerURL → 해당 피어로 전달할 요청 (비어 있으면 전달 불필요)
//
// Resource / Scope 메타데이터는 deep-copy 없이 포인터를 공유한다.
// Route 반환 후 원본 req를 수정하지 않는 한 안전하다.
func (r *TraceRouter) Route(
	ctx context.Context,
	req *collectortracev1.ExportTraceServiceRequest,
) (localReq *collectortracev1.ExportTraceServiceRequest, remote map[string]*collectortracev1.ExportTraceServiceRequest) {
	localReq = &collectortracev1.ExportTraceServiceRequest{}
	remote = make(map[string]*collectortracev1.ExportTraceServiceRequest)

	for _, rs := range req.ResourceSpans {
		localScopes := make([]*tracev1.ScopeSpans, 0, len(rs.ScopeSpans))
		remoteScopes := make(map[string][]*tracev1.ScopeSpans)

		for _, ss := range rs.ScopeSpans {
			localSpans := make([]*tracev1.Span, 0, len(ss.Spans))
			remoteSpans := make(map[string][]*tracev1.Span)

			for _, span := range ss.Spans {
				owner := r.ownerFor(traceIDStr(span.TraceId))
				if owner == r.selfURL {
					localSpans = append(localSpans, span)
				} else {
					remoteSpans[owner] = append(remoteSpans[owner], span)
				}
			}

			if len(localSpans) > 0 {
				localScopes = append(localScopes, &tracev1.ScopeSpans{
					Scope:     ss.Scope,
					Spans:     localSpans,
					SchemaUrl: ss.SchemaUrl,
				})
			}
			for owner, spans := range remoteSpans {
				remoteScopes[owner] = append(remoteScopes[owner], &tracev1.ScopeSpans{
					Scope:     ss.Scope,
					Spans:     spans,
					SchemaUrl: ss.SchemaUrl,
				})
			}
		}

		if len(localScopes) > 0 {
			localReq.ResourceSpans = append(localReq.ResourceSpans, &tracev1.ResourceSpans{
				Resource:   rs.Resource,
				ScopeSpans: localScopes,
				SchemaUrl:  rs.SchemaUrl,
			})
		}
		for owner, scopes := range remoteScopes {
			if remote[owner] == nil {
				remote[owner] = &collectortracev1.ExportTraceServiceRequest{}
			}
			remote[owner].ResourceSpans = append(remote[owner].ResourceSpans, &tracev1.ResourceSpans{
				Resource:   rs.Resource,
				ScopeSpans: scopes,
				SchemaUrl:  rs.SchemaUrl,
			})
		}
	}

	return localReq, remote
}

// Forward는 proto 요청을 직렬화해 피어의 /v1/traces 엔드포인트로 POST한다.
//
// X-Javi-Routed 헤더를 설정해 피어가 재라우팅하지 않도록 한다.
// 피어 Circuit Breaker가 open 상태이면 전달을 건너뛴다 (fail-open).
// 실패 시 warn 로그와 메트릭만 기록하고 반환한다.
func (r *TraceRouter) Forward(ctx context.Context, peerURL string, req *collectortracev1.ExportTraceServiceRequest) {
	// Circuit Breaker 확인: open 상태이면 전달 스킵
	if cb := r.peerBreakers[peerURL]; cb != nil && r.cbThreshold > 0 {
		if !cb.allow() {
			slog.Debug("trace router: circuit open, skipping forward", "peer", peerURL)
			routingCBDropTotal.Inc()
			return
		}
	}

	b, err := proto.Marshal(req)
	if err != nil {
		slog.Warn("trace router: marshal failed", "peer", peerURL, "err", err)
		routingForwardErrorTotal.Inc()
		r.recordFailure(peerURL)
		return
	}

	url := strings.TrimRight(peerURL, "/") + "/v1/traces"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		slog.Warn("trace router: request create failed", "peer", peerURL, "err", err)
		routingForwardErrorTotal.Inc()
		r.recordFailure(peerURL)
		return
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set(RoutedHeader, "1")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		slog.Warn("trace router: forward failed", "peer", peerURL, "err", err)
		routingForwardErrorTotal.Inc()
		r.recordFailure(peerURL)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Warn("trace router: peer returned error", "peer", peerURL, "status", resp.StatusCode)
		routingForwardErrorTotal.Inc()
		r.recordFailure(peerURL)
		return
	}

	r.recordSuccess(peerURL)
	routingForwardedTotal.Inc()
}

// recordSuccess는 피어 CB에 성공을 기록한다.
func (r *TraceRouter) recordSuccess(peerURL string) {
	if cb := r.peerBreakers[peerURL]; cb != nil && r.cbThreshold > 0 {
		cb.onSuccess()
	}
}

// recordFailure는 피어 CB에 실패를 기록한다.
func (r *TraceRouter) recordFailure(peerURL string) {
	if cb := r.peerBreakers[peerURL]; cb != nil && r.cbThreshold > 0 {
		cb.onFailure(peerURL, r.cbThreshold, r.cbCooldown)
	}
}

// traceIDStr은 16바이트 traceID를 hex 문자열로 변환한다.
func traceIDStr(b []byte) string {
	const hextable = "0123456789abcdef"
	buf := make([]byte, len(b)*2)
	for i, v := range b {
		buf[i*2] = hextable[v>>4]
		buf[i*2+1] = hextable[v&0x0f]
	}
	return string(buf)
}
