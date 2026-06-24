package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/kkc/javi-collector/internal/store"
)

// alertRoutes_ 는 /api/alerts/routes 엔드포인트를 처리한다.
//
//	GET  /api/alerts/routes              — 활성 라우팅 규칙 목록
//	POST /api/alerts/routes              — 라우팅 규칙 생성/업데이트
//	DELETE /api/alerts/routes?id=<id>    — 라우팅 규칙 삭제 (소프트)
func (s *HTTPServer) alertRoutes_(w http.ResponseWriter, r *http.Request) {
	if s.alertRoutes == nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "alert routing unavailable (ClickHouse disabled)"})
		return
	}

	ctx := r.Context()
	w.Header().Set("Content-Type", jsonContentType)

	switch r.Method {
	case http.MethodGet:
		routes, err := s.alertRoutes.ListRoutes(ctx)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		if routes == nil {
			routes = []store.AlertRoute{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"routes": routes, "count": len(routes)})

	case http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		var route store.AlertRoute
		if err := json.Unmarshal(body, &route); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()})
			return
		}
		if route.Name == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "name is required"})
			return
		}
		if !route.HasDestination() {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "at least one of slack_url or webhook_url is required"})
			return
		}
		if err := s.alertRoutes.UpsertRoute(ctx, &route); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": route.ID})

	case http.MethodDelete:
		id := r.URL.Query().Get("id")
		if id == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "id query parameter is required"})
			return
		}
		if err := s.alertRoutes.DeleteRoute(ctx, id); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// alertHistory는 최근 알림 이벤트 이력을 반환한다.
//
//	GET /api/alerts/history?service=<svc>&limit=<n>
func (s *HTTPServer) alertHistory(w http.ResponseWriter, r *http.Request) {
	if s.alertRoutes == nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "alert routing unavailable"})
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	service := q.Get("service")
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}

	events, err := s.alertRoutes.ListAlertHistory(r.Context(), service, limit)
	if err != nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if events == nil {
		events = []store.AlertEvent{}
	}

	w.Header().Set("Content-Type", jsonContentType)
	_ = json.NewEncoder(w).Encode(map[string]any{"events": events, "count": len(events)})
}

// alertAck는 발송된 알림 이벤트를 ack 처리해 에스컬레이션을 억제한다.
//
//	POST /api/alerts/ack
//	body: {"id": "<event_id>"}
func (s *HTTPServer) alertAck(w http.ResponseWriter, r *http.Request) {
	if s.alertRoutes == nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "alert routing unavailable"})
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.ID == "" {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "id is required"})
		return
	}

	s.alertRoutes.AckEvent(req.ID)

	w.Header().Set("Content-Type", jsonContentType)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "acked": req.ID})
}
