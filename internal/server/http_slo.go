package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/kkc/javi-collector/internal/store"
)

// sloDefinitions는 SLO 정의를 조회(GET)하거나 등록/수정(PUT)한다.
// Gap 3: SLO/SLI + Burn-Rate Alerting
//
//	GET /api/slo/definitions?service=<svc>
//	PUT /api/slo/definitions  body: SLODefinition JSON
func (s *HTTPServer) sloDefinitions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if s.sloManager == nil {
		http.Error(w, "SLO manager not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	switch r.Method {
	case http.MethodGet:
		defs, err := s.sloManager.ListSLOs(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if defs == nil {
			defs = []store.SLODefinition{}
		}
		writeJSON(w, defs)

	case http.MethodPut:
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read body failed", http.StatusBadRequest)
			return
		}
		var def store.SLODefinition
		if err := json.Unmarshal(body, &def); err != nil || def.ServiceName == "" || def.SLOName == "" {
			http.Error(w, "invalid body: service_name and slo_name required", http.StatusBadRequest)
			return
		}
		if err := s.sloManager.UpsertSLO(r.Context(), def); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// sloBurnAlerts는 번-레이트 초과 알람을 반환한다.
//
//	GET /api/slo/burn-alerts?service=<svc>&limit=100
func (s *HTTPServer) sloBurnAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	if s.sloManager == nil {
		http.Error(w, "SLO manager not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	service := r.URL.Query().Get("service")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	alerts, err := s.sloManager.GetBurnAlerts(r.Context(), service, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if alerts == nil {
		alerts = []store.SLOBurnAlert{}
	}
	writeJSON(w, alerts)
}
