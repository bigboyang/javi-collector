package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/kkc/javi-collector/internal/store"
)

// ---- GAP-06: Log Analytics 핸들러 ----

// logAnalyticsNotReady는 LogAnalyticsQuerier가 nil일 때 501을 반환하는 공통 처리다.
func (s *HTTPServer) logAnalyticsNotReady(w http.ResponseWriter) bool {
	if s.logAnalytics == nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "log analytics unavailable (ClickHouse disabled)"})
		return true
	}
	return false
}

// queryLogVolume는 시간대별 severity별 로그 볼륨을 반환한다.
//
//	GET /api/logs/volume?service=<svc>&from=<ms>&to=<ms>&interval=<sec>
//
// interval 기본값: 60 (분). 60이면 MV를 사용해 빠르게 응답한다.
func (s *HTTPServer) queryLogVolume(w http.ResponseWriter, r *http.Request) {
	if s.logAnalyticsNotReady(w) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	service := q.Get("service")
	fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
	toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)
	intervalSec, _ := strconv.Atoi(q.Get("interval"))
	if intervalSec <= 0 {
		intervalSec = 60
	}

	points, err := s.logAnalytics.QueryLogVolume(r.Context(), service, fromMs, toMs, intervalSec)
	if err != nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if points == nil {
		points = []store.LogVolumePoint{}
	}

	writeJSON(w, map[string]any{
		"points":       points,
		"count":        len(points),
		"interval_sec": intervalSec,
	})
}

// queryLogSearch는 키워드·서비스·심각도 조건으로 로그를 검색한다.
//
//	GET /api/logs/search?service=<svc>&severity=<lvl>&keyword=<kw>&from=<ms>&to=<ms>&limit=<n>&offset=<n>
func (s *HTTPServer) queryLogSearch(w http.ResponseWriter, r *http.Request) {
	if s.logAnalyticsNotReady(w) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	offset, _ := strconv.Atoi(q.Get("offset"))
	fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
	toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)

	sq := store.LogSearchQuery{
		Service:  q.Get("service"),
		Severity: q.Get("severity"),
		Keyword:  q.Get("keyword"),
		FromMs:   fromMs,
		ToMs:     toMs,
		Limit:    limit,
		Offset:   offset,
	}

	results, err := s.logAnalytics.QueryLogSearch(r.Context(), sq)
	if err != nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if results == nil {
		results = []store.LogSearchResult{}
	}

	writeJSON(w, map[string]any{"results": results, "count": len(results)})
}

// queryLogPatterns는 반복 로그 패턴을 발생 빈도 내림차순으로 반환한다.
//
//	GET /api/logs/patterns?service=<svc>&from=<ms>&to=<ms>&limit=<n>
func (s *HTTPServer) queryLogPatterns(w http.ResponseWriter, r *http.Request) {
	if s.logAnalyticsNotReady(w) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	service := q.Get("service")
	fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
	toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 20
	}

	patterns, err := s.logAnalytics.QueryLogPatterns(r.Context(), service, fromMs, toMs, limit)
	if err != nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if patterns == nil {
		patterns = []store.LogPattern{}
	}

	writeJSON(w, map[string]any{"patterns": patterns, "count": len(patterns)})
}

// queryLogContext는 특정 타임스탬프 전후 N초 범위의 로그를 반환한다.
//
//	GET /api/logs/context?service=<svc>&ts=<nano>&window=<sec>&limit=<n>
//
// ts: 로그 timestamp_nano (나노초 Unix 타임스탬프)
// window: 전후 탐색 범위 (초, 기본 30)
func (s *HTTPServer) queryLogContext(w http.ResponseWriter, r *http.Request) {
	if s.logAnalyticsNotReady(w) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	service := q.Get("service")
	tsNano, _ := strconv.ParseInt(q.Get("ts"), 10, 64)
	windowSec, _ := strconv.Atoi(q.Get("window"))
	limit, _ := strconv.Atoi(q.Get("limit"))

	if tsNano <= 0 {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "ts (timestamp_nano) is required"})
		return
	}

	logs, err := s.logAnalytics.QueryLogContext(r.Context(), service, tsNano, windowSec, limit)
	if err != nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if logs == nil {
		logs = []store.LogSearchResult{}
	}

	writeJSON(w, map[string]any{
		"logs":       logs,
		"count":      len(logs),
		"ts":         tsNano,
		"window_sec": windowSec,
	})
}

// queryLogFields는 서비스 로그의 필드 분포 통계를 반환한다.
//
//	GET /api/logs/fields?service=<svc>&from=<ms>&to=<ms>
//
// severity 분포, 상위 10개 logger, 상위 10개 exception_type을 반환한다.
func (s *HTTPServer) queryLogFields(w http.ResponseWriter, r *http.Request) {
	if s.logAnalyticsNotReady(w) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	service := q.Get("service")
	fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
	toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)

	stats, err := s.logAnalytics.QueryLogFields(r.Context(), service, fromMs, toMs)
	if err != nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, stats)
}
