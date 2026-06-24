package server

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/kkc/javi-collector/internal/store"
)

// ---- REST 조회 핸들러 ----

// topologyCacheKey는 토폴로지 캐시의 버킷 키다.
// 5분 단위로 버킷팅하여 같은 시간 창의 요청이 동일한 캐시 엔트리를 공유한다.
type topologyCacheKey struct{ fromBucket, toBucket int64 }

// topologyCacheEntry는 캐시된 토폴로지 결과와 만료 시각을 저장한다.
type topologyCacheEntry struct {
	data      []map[string]any
	expiresAt time.Time
}

// topoBucketMs는 캐시 버킷 크기다 (5분).
// mv_service_topology_state도 5분 단위로 집계하므로 동일 창으로 맞춘다.
const topoBucketMs = 5 * 60 * 1000

func roundTopoBucket(ms int64) int64 {
	if ms <= 0 {
		return 0
	}
	return (ms / topoBucketMs) * topoBucketMs
}

func (s *HTTPServer) queryTraces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)
	q := store.SpanQuery{
		Limit:         queryLimit(r),
		Offset:        queryOffset(r),
		FromMs:        queryInt64(r, "from"),
		ToMs:          queryInt64(r, "to"),
		ServiceName:   r.URL.Query().Get("service"),
		TraceID:       r.URL.Query().Get("trace_id"),
		StatusCode:    queryStatusCode(r),
		MinDurationMs: queryInt64(r, "min_duration_ms"),
	}
	spans, err := s.traceStore.QuerySpans(r.Context(), q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, spans)
}

func (s *HTTPServer) queryMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)
	q := store.MetricQuery{
		Limit:       queryLimit(r),
		Offset:      queryOffset(r),
		FromMs:      queryInt64(r, "from"),
		ToMs:        queryInt64(r, "to"),
		ServiceName: r.URL.Query().Get("service"),
		Name:        r.URL.Query().Get("name"),
	}
	metrics, err := s.metricStore.QueryMetrics(r.Context(), q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, metrics)
}

func (s *HTTPServer) queryLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)
	q := store.LogQuery{
		Limit:        queryLimit(r),
		Offset:       queryOffset(r),
		FromMs:       queryInt64(r, "from"),
		ToMs:         queryInt64(r, "to"),
		ServiceName:  r.URL.Query().Get("service"),
		SeverityText: r.URL.Query().Get("severity"),
		TraceID:      r.URL.Query().Get("trace_id"),
	}
	logs, err := s.logStore.QueryLogs(r.Context(), q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, logs)
}

// queryRED는 서비스별 RED 메트릭 (요청률/에러율/레이턴시)을 반환한다.
// GET /api/collector/red?service=my-svc&from=<ms>&to=<ms>
func (s *HTTPServer) queryRED(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	querier, ok := s.traceStore.(REDQuerier)
	if !ok {
		http.Error(w, "RED metrics not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	result, err := querier.QueryRED(r.Context(),
		r.URL.Query().Get("service"),
		queryInt64(r, "from"),
		queryInt64(r, "to"),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// queryTopology는 서비스 간 호출 관계 (토폴로지 맵)를 반환한다.
// GET /api/collector/topology?from=<ms>&to=<ms>
//
// 캐시 전략:
//   - 쿼리 파라미터를 5분 버킷으로 정규화하여 같은 창의 요청이 캐시를 공유한다.
//   - TTL(기본 60초) 이내 요청은 ClickHouse를 거치지 않고 즉시 반환한다.
//   - 캐시 미스 또는 만료 시에만 DB를 조회하고 결과를 갱신한다.
func (s *HTTPServer) queryTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	querier, ok := s.traceStore.(TopologyQuerier)
	if !ok {
		http.Error(w, "topology not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	fromMs := queryInt64(r, "from")
	toMs := queryInt64(r, "to")

	// 캐시 조회: 5분 버킷으로 정규화하여 키 생성
	cacheKey := topologyCacheKey{
		fromBucket: roundTopoBucket(fromMs),
		toBucket:   roundTopoBucket(toMs),
	}
	if v, ok := s.topoCache.Load(cacheKey); ok {
		entry := v.(topologyCacheEntry)
		if time.Now().Before(entry.expiresAt) {
			writeJSON(w, entry.data)
			return
		}
	}

	// 캐시 미스 또는 만료: DB 조회
	result, err := querier.QueryTopology(r.Context(), fromMs, toMs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 결과를 캐시에 저장
	s.topoCache.Store(cacheKey, topologyCacheEntry{
		data:      result,
		expiresAt: time.Now().Add(s.topoCacheTTL),
	})

	writeJSON(w, result)
}

// queryErrorLogs는 서비스별 에러 로그 집계를 반환한다.
// GET /api/collector/error-logs?service=my-svc&from=<ms>&to=<ms>
func (s *HTTPServer) queryErrorLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	querier, ok := s.logStore.(ErrorLogQuerier)
	if !ok {
		http.Error(w, "error log aggregation not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	result, err := querier.QueryErrorLogs(r.Context(),
		r.URL.Query().Get("service"),
		queryInt64(r, "from"),
		queryInt64(r, "to"),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// queryAnomalies는 AIOps Phase 2 이상 감지 결과를 반환한다.
// GET /api/collector/anomalies?service=my-svc&severity=critical&from=<ms>&to=<ms>&limit=100
func (s *HTTPServer) queryAnomalies(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	querier, ok := s.traceStore.(AnomalyQuerier)
	if !ok {
		http.Error(w, "anomaly queries not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	result, err := querier.QueryAnomalies(r.Context(),
		r.URL.Query().Get("service"),
		r.URL.Query().Get("severity"),
		queryInt64(r, "from"),
		queryInt64(r, "to"),
		queryLimit(r),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// queryHistogram은 mv_histogram_1m_state 집계 뷰에서 히스토그램 메트릭을 반환한다.
// GET /api/collector/histogram?service=svc&name=http.server.duration&from=<ms>&to=<ms>&limit=100
func (s *HTTPServer) queryHistogram(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	querier, ok := s.metricStore.(HistogramMVQuerier)
	if !ok {
		http.Error(w, "histogram MV not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	result, err := querier.QueryHistogramMV(r.Context(),
		r.URL.Query().Get("service"),
		r.URL.Query().Get("name"),
		queryInt64(r, "from"),
		queryInt64(r, "to"),
		queryLimit(r),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// streamLogs는 SSE(Server-Sent Events) 방식으로 신규 로그를 실시간 스트리밍한다.
// GET /api/stream/logs?service=svc&severity=ERROR
//
// 동작: 3초마다 logStore를 폴링해 새로 수신된 로그를 "data: <json>\n\n" 형식으로 전송한다.
// 클라이언트가 연결을 끊으면 자동으로 종료된다.
func (s *HTTPServer) streamLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// keepalive 코멘트: 프록시/방화벽의 유휴 연결 종료를 방지한다.
	fmt.Fprintf(w, ": keepalive\n\n")
	flusher.Flush()

	service := r.URL.Query().Get("service")
	severity := r.URL.Query().Get("severity")
	lastMs := time.Now().UnixMilli()

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			q := store.LogQuery{
				Limit:        50,
				FromMs:       lastMs,
				ServiceName:  service,
				SeverityText: severity,
			}
			logs, err := s.logStore.QueryLogs(r.Context(), q)
			if err != nil {
				slog.Warn("SSE log poll error", "err", err)
				lastMs = now
				continue
			}
			for _, log := range logs {
				data, err := json.Marshal(log)
				if err != nil {
					continue
				}
				fmt.Fprintf(w, "data: %s\n\n", data)
			}
			if len(logs) > 0 {
				flusher.Flush()
			}
			lastMs = now
		}
	}
}

// streamAlerts는 SSE 방식으로 신규 이상 감지 알림을 실시간 스트리밍한다.
// GET /api/stream/alerts?service=svc&severity=critical
//
// 동작: 10초마다 anomalies 테이블을 폴링해 새로 탐지된 이벤트를 전송한다.
// ClickHouse가 없으면 501을 반환한다.
func (s *HTTPServer) streamAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	querier, ok := s.traceStore.(AnomalyQuerier)
	if !ok {
		http.Error(w, "anomaly streaming not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	fmt.Fprintf(w, ": keepalive\n\n")
	flusher.Flush()

	service := r.URL.Query().Get("service")
	severity := r.URL.Query().Get("severity")
	lastMs := time.Now().UnixMilli()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			anomalies, err := querier.QueryAnomalies(r.Context(), service, severity, lastMs, now, 50)
			if err != nil {
				slog.Warn("SSE alert poll error", "err", err)
				lastMs = now
				continue
			}
			for _, a := range anomalies {
				data, err := json.Marshal(a)
				if err != nil {
					continue
				}
				fmt.Fprintf(w, "data: %s\n\n", data)
			}
			if len(anomalies) > 0 {
				flusher.Flush()
			}
			lastMs = now
		}
	}
}

// queryBrokenTraces는 root span이 없는 브로큰 트레이스를 반환한다.
// GET /api/collector/broken-traces?service=svc&from=<ms>&to=<ms>&limit=100
//
// 브로큰 트레이스는 계측 미설정, 샘플링 불일치, 또는 네트워크 손실의 신호다.
func (s *HTTPServer) queryBrokenTraces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	querier, ok := s.traceStore.(BrokenTraceQuerier)
	if !ok {
		http.Error(w, "broken trace detection not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	result, err := querier.QueryBrokenTraces(r.Context(),
		r.URL.Query().Get("service"),
		queryInt64(r, "from"),
		queryInt64(r, "to"),
		queryLimit(r),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// queryErrorGroups는 fingerprint 기반으로 집계된 에러 그룹 목록을 반환한다.
// GET /api/collector/error-groups?service=svc&from=<ms>&to=<ms>&limit=100
//
// 동일한 exception_type + exception_message를 가진 에러를 하나의 그룹으로 집계해
// alert fatigue를 줄이고 재발 패턴을 추적한다.
func (s *HTTPServer) queryErrorGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	if s.errorGroups == nil {
		http.Error(w, "error groups not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	result, err := s.errorGroups.QueryErrorGroups(r.Context(),
		r.URL.Query().Get("service"),
		queryInt64(r, "from"),
		queryInt64(r, "to"),
		queryLimit(r),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// querySlowQueries는 DB 슬로우 쿼리 MV에서 임계값 이상 소요된 DB 쿼리를 반환한다.
// GET /api/collector/slow-queries?service=svc&from=<ms>&to=<ms>&threshold_ms=500&limit=100
func (s *HTTPServer) querySlowQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	if s.slowQueryQuerier == nil {
		http.Error(w, "slow query not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	thresholdMs := queryInt64(r, "threshold_ms")
	result, err := s.slowQueryQuerier.QuerySlowQueries(r.Context(),
		r.URL.Query().Get("service"),
		queryInt64(r, "from"),
		queryInt64(r, "to"),
		thresholdMs,
		queryLimit(r),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// queryTraceContext는 trace_id에 연관된 spans·logs·RED 메트릭을 한 번에 반환한다.
// Gap 1: Correlated Signal Navigation
//
//	GET /api/collector/trace-context?trace_id=<id>
func (s *HTTPServer) queryTraceContext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	if s.traceContext == nil {
		http.Error(w, "trace context not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	traceID := strings.TrimSpace(r.URL.Query().Get("trace_id"))
	if traceID == "" {
		http.Error(w, "trace_id parameter required", http.StatusBadRequest)
		return
	}

	result, err := s.traceContext.QueryTraceContext(r.Context(), traceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// queryTraceWaterfall은 trace_id에 해당하는 스팬을 폭포수 뷰 + 임계 경로 정보로 반환한다.
// GAP-01: Trace Waterfall / Critical Path
//
//	GET /api/collector/trace-waterfall?trace_id=<id>
func (s *HTTPServer) queryTraceWaterfall(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	if s.traceWaterfall == nil {
		http.Error(w, "trace waterfall not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	traceID := strings.TrimSpace(r.URL.Query().Get("trace_id"))
	if traceID == "" {
		http.Error(w, "trace_id parameter required", http.StatusBadRequest)
		return
	}

	result, err := s.traceWaterfall.QueryTraceWaterfall(r.Context(), traceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if result == nil {
		http.Error(w, "trace not found", http.StatusNotFound)
		return
	}
	writeJSON(w, result)
}

// queryRaw는 화이트리스트를 통과한 SELECT SQL을 ClickHouse에 직접 실행한다.
// GET /api/query?sql=SELECT+service_name,+count()+FROM+apm.spans+GROUP+BY+service_name
//
// 보안: SELECT로 시작하지 않거나 위험 키워드가 포함된 쿼리는 400을 반환한다.
func (s *HTTPServer) queryRaw(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)

	querier, ok := s.traceStore.(RawQuerier)
	if !ok {
		http.Error(w, "raw query not available (requires ClickHouse)", http.StatusNotImplemented)
		return
	}

	sql := strings.TrimSpace(r.URL.Query().Get("sql"))
	if err := validateRawSQL(sql); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := querier.QueryRaw(r.Context(), sql)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

// validateRawSQL은 SELECT 이외의 구문 및 위험 키워드를 차단한다.
func validateRawSQL(sql string) error {
	if sql == "" {
		return fmt.Errorf("sql parameter required")
	}
	upper := strings.ToUpper(sql)
	// SELECT로 시작하지 않으면 거부
	if !strings.HasPrefix(upper, "SELECT") {
		return fmt.Errorf("only SELECT queries are allowed")
	}
	// 위험 구문 차단
	for _, kw := range []string{"DROP", "DELETE", "ALTER", "INSERT", "UPDATE", "CREATE",
		"TRUNCATE", "SYSTEM", "KILL", "ATTACH", "DETACH", "RENAME"} {
		if strings.Contains(upper, kw) {
			return fmt.Errorf("keyword %q not allowed", kw)
		}
	}
	return nil
}
