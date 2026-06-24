package server

import (
	"encoding/json"
	"net/http"
)

func (s *HTTPServer) stats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := map[string]any{
		"traces": map[string]any{
			"received": s.ingester.TraceReceived(),
			"buffered": storeSize(s.traceStore),
		},
		"metrics": map[string]any{
			"received": s.ingester.MetricReceived(),
			"buffered": storeSize(s.metricStore),
		},
		"logs": map[string]any{
			"received": s.ingester.LogReceived(),
			"buffered": storeSize(s.logStore),
		},
	}
	writeJSON(w, resp)
}

// ---- 운영 엔드포인트 ----

// healthz는 liveness probe 엔드포인트다.
// 프로세스가 실행 중이면 항상 200 OK를 반환한다.
// 재시작이 필요한 상태(데드락, OOM 등)를 감지하는 데 사용한다.
func (s *HTTPServer) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// readyz는 readiness probe 엔드포인트다.
// 다음 세 가지 조건을 순서대로 확인한다:
//  1. MarkReady()가 호출되어 초기화가 완료되었는지 (미완료 → 503)
//  2. UnmarkReady()로 드레인 중인지 (드레인 중 → 503)
//  3. ClickHouse 생존 여부 (실패 → 503 + JSON 상세)
//
// Kubernetes readiness probe는 HTTP 상태코드만 확인하므로
// 초기화 이후 ClickHouse 장애 시에도 503을 반환해 로드밸런서에서 제외된다.
//
// 응답 예시 (정상):
//
//	{"ready":true,"clickhouse":"ok","channel":{"ch_len":12,"ch_cap":1000,"cb_state":"closed"}}
//
// 응답 예시 (ClickHouse 장애):
//
//	{"ready":false,"clickhouse":"error: dial tcp: connection refused","channel":{...}}
func (s *HTTPServer) readyz(w http.ResponseWriter, r *http.Request) {
	// 1. 초기화 게이트: MarkReady() 호출 전이면 503
	select {
	case <-s.ready:
		// 초기화 완료
	default:
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}

	// 2. Graceful shutdown 드레인: UnmarkReady() 호출 후 503
	if s.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}

	// 3. 백엔드 생존 여부: ClickHouse ping 실패 시 503
	status := map[string]any{"ready": true}
	healthy := true
	if checker, ok := s.traceStore.(ReadinessChecker); ok {
		pingCtx := r.Context()
		if err := checker.Ping(pingCtx); err != nil {
			status["clickhouse"] = "error: " + err.Error()
			status["ready"] = false
			healthy = false
		} else {
			status["clickhouse"] = "ok"
		}
		status["channel"] = checker.ChannelStatus()
	}
	w.Header().Set("Content-Type", "application/json")
	if healthy {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(status)
}
