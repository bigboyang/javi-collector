package server

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/kkc/javi-collector/internal/store"
)

// queryInfraCorrelation은 /api/collector/infra-correlation 엔드포인트를 처리한다.
//
//	GET /api/collector/infra-correlation?service=<svc>&from=<ms>&to=<ms>
//
// 서비스의 k8s 배포 컨텍스트(pod/node/namespace)와 JVM/인프라 메트릭을 상관 분석해 반환한다.
// Datadog Infrastructure Correlation, Dynatrace Smartscape와 동일한 기능을 제공한다.
func (s *HTTPServer) queryInfraCorrelation(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", jsonContentType)
	if s.infraCorrelation == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "infra correlation unavailable (ClickHouse disabled)"})
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	service := q.Get("service")
	if service == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service parameter is required"})
		return
	}
	fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
	toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)
	if toMs == 0 {
		toMs = time.Now().UnixMilli()
	}
	if fromMs == 0 {
		fromMs = toMs - 30*60*1000 // 기본 30분
	}

	result, err := s.infraCorrelation.QueryInfraCorrelation(r.Context(), service, fromMs, toMs)
	if err != nil {
		slog.Warn("infra correlation query failed", "service", service, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}

// handleProfiling은 /api/collector/profiling 엔드포인트를 처리한다.
//
//	POST /api/collector/profiling         — 스냅샷 저장 (Java Agent → Collector)
//	GET  /api/collector/profiling?service=<svc>&type=<type>&from=<ms>&to=<ms>&limit=<n>
//	                                      — 스냅샷 목록 조회 (payload 제외)
func (s *HTTPServer) handleProfiling(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", jsonContentType)
	if s.profilingStore == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "profiling unavailable (ClickHouse disabled)"})
		return
	}

	switch r.Method {
	case http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		var snap store.ProfilingSnapshot
		if err := json.Unmarshal(body, &snap); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}
		if snap.ServiceName == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "service_name is required"})
			return
		}
		if snap.ID == "" {
			// 클라이언트가 ID를 제공하지 않은 경우 생성
			idBytes := make([]byte, 16)
			_, _ = rand.Read(idBytes)
			snap.ID = hex.EncodeToString(idBytes)
		}
		if err := s.profilingStore.InsertSnapshot(r.Context(), snap); err != nil {
			slog.Warn("profiling snapshot insert failed", "service", snap.ServiceName, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"id": snap.ID, "status": "accepted"})

	case http.MethodGet:
		q := r.URL.Query()
		service := q.Get("service")
		if service == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "service parameter is required"})
			return
		}
		fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))

		snaps, err := s.profilingStore.QuerySnapshots(r.Context(), store.QuerySnapshotsParams{
			ServiceName: service,
			ProfileType: q.Get("type"),
			FromMs:      fromMs,
			ToMs:        toMs,
			Limit:       limit,
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		if snaps == nil {
			snaps = []store.ProfilingSnapshot{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"snapshots": snaps, "count": len(snaps)})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleProfilingPayload는 /api/collector/profiling/payload?id=<id> 엔드포인트를 처리한다.
// 특정 스냅샷의 payload(Flame Graph 원본 데이터)를 반환한다.
func (s *HTTPServer) handleProfilingPayload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", jsonContentType)
	if s.profilingStore == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "profiling unavailable"})
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "id parameter is required"})
		return
	}
	snap, err := s.profilingStore.GetSnapshotPayload(r.Context(), id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if snap == nil {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "snapshot not found"})
		return
	}
	_ = json.NewEncoder(w).Encode(snap)
}

// handleProfilingSummary는 /api/collector/profiling/summary 엔드포인트를 처리한다.
//
//	GET /api/collector/profiling/summary?from=<ms>&to=<ms>
func (s *HTTPServer) handleProfilingSummary(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", jsonContentType)
	if s.profilingStore == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "profiling unavailable"})
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)
	fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
	if toMs == 0 {
		toMs = time.Now().UnixMilli()
	}
	if fromMs == 0 {
		fromMs = toMs - 24*60*60*1000 // 기본 24시간
	}
	summary, err := s.profilingStore.QueryProfileSummary(r.Context(), fromMs, toMs)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if summary == nil {
		summary = []map[string]any{}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"summary": summary})
}

// handleK8sMetrics는 /api/collector/k8s-metrics 엔드포인트를 처리한다.
//
//	POST /api/collector/k8s-metrics       — Pod 리소스 메트릭 저장 (Java Agent → Collector)
//	GET  /api/collector/k8s-metrics?service=<svc>&pod=<pod>&from=<ms>&to=<ms>&limit=<n>
//	                                      — 시계열 조회
func (s *HTTPServer) handleK8sMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", jsonContentType)
	if s.k8sMetrics == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "k8s metrics unavailable (ClickHouse disabled)"})
		return
	}

	switch r.Method {
	case http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		var m store.K8sPodMetric
		if err := json.Unmarshal(body, &m); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}
		if m.ServiceName == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "service_name is required"})
			return
		}
		if err := s.k8sMetrics.InsertMetric(r.Context(), m); err != nil {
			slog.Warn("k8s metric insert failed", "service", m.ServiceName, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})

	case http.MethodGet:
		q := r.URL.Query()
		service := q.Get("service")
		if service == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "service parameter is required"})
			return
		}
		fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		limit, _ := strconv.Atoi(q.Get("limit"))

		metrics, err := s.k8sMetrics.QueryMetrics(r.Context(), store.QueryK8sMetricsParams{
			ServiceName: service,
			PodName:     q.Get("pod"),
			FromMs:      fromMs,
			ToMs:        toMs,
			Limit:       limit,
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		if metrics == nil {
			metrics = []store.K8sPodMetric{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"metrics": metrics, "count": len(metrics)})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handleK8sMetricsSummary는 /api/collector/k8s-metrics/summary 엔드포인트를 처리한다.
//
//	GET /api/collector/k8s-metrics/summary?service=<svc>&from=<ms>&to=<ms>
func (s *HTTPServer) handleK8sMetricsSummary(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", jsonContentType)
	if s.k8sMetrics == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "k8s metrics unavailable"})
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	service := q.Get("service")
	if service == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "service parameter is required"})
		return
	}
	toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)
	fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
	if toMs == 0 {
		toMs = time.Now().UnixMilli()
	}
	if fromMs == 0 {
		fromMs = toMs - 60*60*1000 // 기본 1시간
	}
	summary, err := s.k8sMetrics.QueryPodSummary(r.Context(), service, fromMs, toMs)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if summary == nil {
		summary = []map[string]any{}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"pods": summary})
}
