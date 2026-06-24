package server

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/kkc/javi-collector/internal/store"
)

// listCatalogServices는 등록된 모든 서비스 카탈로그 항목을 반환한다.
// GET /api/catalog/services
func (s *HTTPServer) listCatalogServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	if s.catalog == nil {
		http.Error(w, "service catalog not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	result, err := s.catalog.ListServices(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if result == nil {
		result = []store.ServiceCatalogEntry{}
	}
	writeJSON(w, result)
}

// catalogService는 서비스 카탈로그 단일 항목을 조회(GET)하거나 등록/수정(PUT)한다.
//
//	GET /api/catalog/service?name=<service>
//	PUT /api/catalog/service  body: ServiceCatalogEntry JSON
func (s *HTTPServer) catalogService(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if s.catalog == nil {
		http.Error(w, "service catalog not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	switch r.Method {
	case http.MethodGet:
		name := r.URL.Query().Get("name")
		if name == "" {
			http.Error(w, "name parameter required", http.StatusBadRequest)
			return
		}
		entry, err := s.catalog.GetService(r.Context(), name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if entry == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		writeJSON(w, entry)

	case http.MethodPut:
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read body failed", http.StatusBadRequest)
			return
		}
		var entry store.ServiceCatalogEntry
		if err := json.Unmarshal(body, &entry); err != nil || entry.ServiceName == "" {
			http.Error(w, "invalid body: service_name required", http.StatusBadRequest)
			return
		}
		if err := s.catalog.UpsertService(r.Context(), entry); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
