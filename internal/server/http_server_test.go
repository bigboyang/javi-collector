package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kkc/javi-collector/internal/ingester"
	"github.com/kkc/javi-collector/internal/store"
)

// http.go 분할 검증: NewHTTPServer의 라우팅 등록, 인증 미들웨어, 운영 엔드포인트,
// 그리고 querier가 nil일 때의 501 경로가 분할 후에도 동일하게 동작하는지 고정한다.
// ClickHouse 없이 메모리 스토어만으로 검증한다.

func newTestServer(t *testing.T) *HTTPServer {
	t.Helper()
	ts := store.NewMemoryTraceStore(100)
	ms := store.NewMemoryMetricStore(100)
	ls := store.NewMemoryLogStore(100)
	ing := ingester.New(ts, ms, ls, nil)
	return NewHTTPServer(":0", ing, ts, ms, ls)
}

// TestRoutesRegistered는 NewHTTPServer가 모든 엔드포인트를 mux에 등록했는지
// 확인한다. 등록되지 않은 경로만 ServeMux가 404를 반환하므로, 404가 아니면
// 라우트가 살아 있다는 의미다 (501/405/400/200 등은 모두 등록됨).
func TestRoutesRegistered(t *testing.T) {
	s := newTestServer(t)
	handler := s.srv.Handler

	routes := []string{
		// OTLP 수신
		"/v1/traces", "/v1/metrics", "/v1/logs", "/v1/events/deploy",
		// REST 조회
		"/api/collector/traces", "/api/collector/metrics", "/api/collector/logs",
		"/api/collector/stats", "/api/collector/red", "/api/collector/topology",
		"/api/collector/error-logs", "/api/collector/anomalies", "/api/collector/histogram",
		"/api/collector/broken-traces", "/api/collector/error-groups",
		"/api/collector/trace-context", "/api/collector/trace-waterfall",
		"/api/collector/slow-queries", "/api/collector/infra-correlation",
		"/api/collector/profiling", "/api/collector/profiling/payload",
		"/api/collector/profiling/summary", "/api/collector/k8s-metrics",
		"/api/collector/k8s-metrics/summary",
		// SSE
		"/api/stream/logs", "/api/stream/alerts",
		// raw query
		"/api/query",
		// catalog
		"/api/catalog/services", "/api/catalog/service",
		// SLO
		"/api/slo/definitions", "/api/slo/burn-alerts",
		// RCA
		"/api/rca/reports", "/api/rca/feedback",
		// alerts
		"/api/alerts/routes", "/api/alerts/history", "/api/alerts/ack",
		// log analytics
		"/api/logs/volume", "/api/logs/search", "/api/logs/patterns",
		"/api/logs/context", "/api/logs/fields",
		// 운영
		"/healthz", "/readyz", "/metrics",
	}

	// 이미 취소된 context를 사용한다. mux 라우팅은 경로 기반이라 영향이 없고,
	// /api/stream/* SSE 핸들러가 폴링 루프에서 블로킹되지 않고 즉시 반환한다.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, p := range routes {
		req := httptest.NewRequest(http.MethodGet, p, nil).WithContext(ctx)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Errorf("route %s not registered (got 404)", p)
		}
	}
}

func TestHealthz(t *testing.T) {
	s := newTestServer(t)
	rec := httptest.NewRecorder()
	s.healthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("healthz code = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("healthz body = %q, want %q", rec.Body.String(), "ok")
	}
}

// TestReadyzGate는 readyz의 3단계 게이트 중 초기화·드레인 게이트를 검증한다.
// 메모리 스토어는 ReadinessChecker가 아니므로 백엔드 ping 단계는 통과한다.
func TestReadyzGate(t *testing.T) {
	s := newTestServer(t)

	// 1. MarkReady 전 -> 503
	rec := httptest.NewRecorder()
	s.readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz before MarkReady = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	// 2. MarkReady 후 -> 200
	s.MarkReady()
	rec = httptest.NewRecorder()
	s.readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("readyz after MarkReady = %d, want %d", rec.Code, http.StatusOK)
	}

	// 3. UnmarkReady(드레인) 후 -> 503
	s.UnmarkReady()
	rec = httptest.NewRecorder()
	s.readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz after UnmarkReady = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestRequireAPIKey(t *testing.T) {
	s := newTestServer(t)

	ok := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }

	// 1. API Key 미설정 -> 인증 없이 통과
	rec := httptest.NewRecorder()
	s.requireAPIKey(ok)(rec, httptest.NewRequest(http.MethodGet, "/api/x", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("no apiKey set: code = %d, want %d", rec.Code, http.StatusOK)
	}

	// API Key 설정
	s.SetAPIKey("secret")

	// 2. 헤더 없음 -> 401
	rec = httptest.NewRecorder()
	s.requireAPIKey(ok)(rec, httptest.NewRequest(http.MethodGet, "/api/x", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("missing key: code = %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	// 3. X-Api-Key 헤더 -> 통과
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/x", nil)
	req.Header.Set("X-Api-Key", "secret")
	s.requireAPIKey(ok)(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("X-Api-Key: code = %d, want %d", rec.Code, http.StatusOK)
	}

	// 4. Authorization: Bearer 헤더 -> 통과
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/x", nil)
	req.Header.Set("Authorization", "Bearer secret")
	s.requireAPIKey(ok)(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Bearer: code = %d, want %d", rec.Code, http.StatusOK)
	}

	// 5. 잘못된 키 -> 401
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/x", nil)
	req.Header.Set("X-Api-Key", "wrong")
	s.requireAPIKey(ok)(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong key: code = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// TestNilQueriersReturn501은 ClickHouse 의존 querier가 nil일 때 각 핸들러가
// 501을 반환하는지 확인한다. 분할로 흩어진 핸들러 파일들이 동일 동작을 유지하는지 검증한다.
func TestNilQueriersReturn501(t *testing.T) {
	s := newTestServer(t)

	cases := map[string]http.HandlerFunc{
		"sloBurnAlerts (http_slo.go)":                s.sloBurnAlerts,
		"queryRCAReports (http_rca.go)":              s.queryRCAReports,
		"queryLogVolume (http_logs.go)":              s.queryLogVolume,
		"alertRoutes_ (http_alerts.go)":              s.alertRoutes_,
		"queryInfraCorrelation (http_profiling.go)":  s.queryInfraCorrelation,
		"handleProfilingSummary (http_profiling.go)": s.handleProfilingSummary,
	}

	for name, h := range cases {
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusNotImplemented {
			t.Errorf("%s with nil querier: code = %d, want %d", name, rec.Code, http.StatusNotImplemented)
		}
	}
}
