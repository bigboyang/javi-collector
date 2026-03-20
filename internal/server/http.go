// Package server는 OTLP/HTTP 수신 엔드포인트와 REST 조회 API를 제공한다.
//
// 수신 경로:
//
//	POST /v1/traces   — ExportTraceServiceRequest (application/x-protobuf)
//	POST /v1/metrics  — ExportMetricsServiceRequest
//	POST /v1/logs     — ExportLogsServiceRequest
//
// 조회 경로:
//
//	GET /api/collector/traces?limit=100
//	GET /api/collector/metrics?limit=100
//	GET /api/collector/logs?limit=100
//	GET /api/collector/stats
//
// 운영 경로:
//
//	GET /healthz  — liveness probe (항상 200 반환)
//	GET /readyz   — readiness probe (store 초기화 완료 시 200)
//	GET /metrics  — Prometheus exposition format
package server

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kkc/javi-collector/internal/ingester"
	"github.com/kkc/javi-collector/internal/store"
)

const (
	jsonContentType = "application/json"
	defaultLimit    = 100
	maxBodyBytes    = 16 << 20 // 16 MiB
)

// gzipReaderPool은 요청마다 gzip.Reader를 새로 할당하지 않고 재사용한다.
// OTel Java Agent / OTel Collector 등 대부분의 APM exporter가 기본적으로
// Content-Encoding: gzip으로 전송하므로, 고TPS 환경에서 GC pressure 절감 효과가 크다.
var gzipReaderPool = sync.Pool{
	New: func() any { return new(gzip.Reader) },
}

// sizer는 버퍼 크기를 조회할 수 있는 저장소 구현체를 위한 선택적 인터페이스다.
type sizer interface {
	Size() int
}

// REDQuerier는 RED 메트릭 집계를 지원하는 저장소 인터페이스다.
// ClickHouseTraceStore가 구현하며, 메모리 스토어는 구현하지 않는다.
type REDQuerier interface {
	QueryRED(ctx context.Context, service string, fromMs, toMs int64) ([]map[string]any, error)
}

// TopologyQuerier는 서비스 토폴로지 조회를 지원하는 저장소 인터페이스다.
type TopologyQuerier interface {
	QueryTopology(ctx context.Context, fromMs, toMs int64) ([]map[string]any, error)
}

// ErrorLogQuerier는 에러 로그 집계를 지원하는 저장소 인터페이스다.
type ErrorLogQuerier interface {
	QueryErrorLogs(ctx context.Context, service string, fromMs, toMs int64) ([]map[string]any, error)
}

// HTTPServer는 OTLP/HTTP 수신 + REST 조회 API + 운영 엔드포인트 서버다.
type HTTPServer struct {
	ingester    *ingester.Ingester
	traceStore  store.TraceStore
	metricStore store.MetricStore
	logStore    store.LogStore
	srv         *http.Server
	ready       chan struct{} // close되면 readyz가 200을 반환한다
}

func NewHTTPServer(addr string, ing *ingester.Ingester,
	ts store.TraceStore, ms store.MetricStore, ls store.LogStore) *HTTPServer {

	s := &HTTPServer{
		ingester:    ing,
		traceStore:  ts,
		metricStore: ms,
		logStore:    ls,
		ready:       make(chan struct{}),
	}

	mux := http.NewServeMux()

	// OTLP 수신 엔드포인트
	mux.HandleFunc("/v1/traces", s.handleTraces)
	mux.HandleFunc("/v1/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/logs", s.handleLogs)

	// REST 조회 엔드포인트
	mux.HandleFunc("/api/collector/traces", s.queryTraces)
	mux.HandleFunc("/api/collector/metrics", s.queryMetrics)
	mux.HandleFunc("/api/collector/logs", s.queryLogs)
	mux.HandleFunc("/api/collector/stats", s.stats)
	// 대시보드용 집계 엔드포인트 (ClickHouse MV 기반)
	mux.HandleFunc("/api/collector/red", s.queryRED)
	mux.HandleFunc("/api/collector/topology", s.queryTopology)
	mux.HandleFunc("/api/collector/error-logs", s.queryErrorLogs)

	// 운영 엔드포인트
	// /healthz: liveness probe — 프로세스가 살아있으면 200
	// /readyz:  readiness probe — MarkReady() 호출 후 200 (로드밸런서 트래픽 수신 여부 제어)
	// /metrics: Prometheus scrape 엔드포인트
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/readyz", s.readyz)
	mux.Handle("/metrics", promhttp.Handler())

	s.srv = &http.Server{
		Addr:    addr,
		Handler: mux,
		// ReadHeaderTimeout: DoS 방어 (slowloris 공격 대응)
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// MarkReady는 서버가 트래픽을 받을 준비가 되었음을 신호한다.
// main에서 모든 초기화(store 연결 등)가 완료된 후 호출해야 한다.
// 쿠버네티스 readiness probe가 이 상태를 확인한다.
func (s *HTTPServer) MarkReady() {
	select {
	case <-s.ready:
		// 이미 닫힌 경우 패닉 방지
	default:
		close(s.ready)
	}
}

func (s *HTTPServer) Start() error {
	slog.Info("HTTP server starting", "addr", s.srv.Addr)
	return s.srv.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// ---- OTLP 수신 핸들러 ----

func (s *HTTPServer) handleTraces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readProtoBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var count int
	if isJSON(r) {
		count, err = s.ingester.IngestTracesJSON(r.Context(), body)
	} else {
		count, err = s.ingester.IngestTraces(r.Context(), body)
	}
	if err != nil {
		slog.Warn("trace ingest error", "err", err)
		// backpressure: 503으로 클라이언트가 재시도하도록 유도
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	slog.Debug("POST /v1/traces", "spans", count, "bytes", len(body))
	w.WriteHeader(http.StatusOK)
}

func (s *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readProtoBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var count int
	if isJSON(r) {
		count, err = s.ingester.IngestMetricsJSON(r.Context(), body)
	} else {
		count, err = s.ingester.IngestMetrics(r.Context(), body)
	}
	if err != nil {
		slog.Warn("metric ingest error", "err", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	slog.Debug("POST /v1/metrics", "count", count, "bytes", len(body))
	w.WriteHeader(http.StatusOK)
}

func (s *HTTPServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := readProtoBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var count int
	if isJSON(r) {
		count, err = s.ingester.IngestLogsJSON(r.Context(), body)
	} else {
		count, err = s.ingester.IngestLogs(r.Context(), body)
	}
	if err != nil {
		slog.Warn("log ingest error", "err", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	slog.Debug("POST /v1/logs", "count", count, "bytes", len(body))
	w.WriteHeader(http.StatusOK)
}

// ---- REST 조회 핸들러 ----

func (s *HTTPServer) queryTraces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	setCORSHeaders(w)
	q := store.SpanQuery{
		Limit:         queryLimit(r),
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

	result, err := querier.QueryTopology(r.Context(),
		queryInt64(r, "from"),
		queryInt64(r, "to"),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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
// MarkReady()가 호출된 이후에만 200을 반환한다.
// 쿠버네티스가 이 엔드포인트를 통해 트래픽 수신 가능 여부를 확인한다.
func (s *HTTPServer) readyz(w http.ResponseWriter, r *http.Request) {
	select {
	case <-s.ready:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	default:
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}
}

// ---- 헬퍼 ----

// readProtoBody는 HTTP 요청 body를 읽어 반환한다.
// Content-Encoding: gzip인 경우 압축을 해제한다 (OTel exporter 기본값).
// gzip.Reader는 sync.Pool에서 재사용해 GC 부담을 줄인다.
// zip-bomb 방어: 압축 전/후 모두 maxBodyBytes로 제한한다.
func readProtoBody(r *http.Request) ([]byte, error) {
	reader := io.LimitReader(r.Body, maxBodyBytes)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz := gzipReaderPool.Get().(*gzip.Reader)
		if err := gz.Reset(reader); err != nil {
			gzipReaderPool.Put(gz)
			return nil, err
		}
		defer func() {
			gz.Close()
			gzipReaderPool.Put(gz)
		}()
		reader = io.LimitReader(gz, maxBodyBytes)
	}
	return io.ReadAll(reader)
}

func isJSON(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	return len(ct) >= len(jsonContentType) && ct[:len(jsonContentType)] == jsonContentType
}

func queryLimit(r *http.Request) int {
	s := r.URL.Query().Get("limit")
	if s == "" {
		return defaultLimit
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return defaultLimit
	}
	return n
}

func queryInt64(r *http.Request, key string) int64 {
	s := r.URL.Query().Get(key)
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// queryStatusCode는 ?status=ok|error|unset 을 int32로 변환한다.
// 지정되지 않으면 -1(필터 없음)을 반환한다.
func queryStatusCode(r *http.Request) int32 {
	switch r.URL.Query().Get("status") {
	case "unset":
		return 0
	case "ok":
		return 1
	case "error":
		return 2
	default:
		return -1
	}
}

// setCORSHeaders는 대시보드(브라우저)에서 직접 쿼리할 수 있도록 CORS 헤더를 설정한다.
func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func storeSize(s any) int {
	if sz, ok := s.(sizer); ok {
		return sz.Size()
	}
	return -1
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("json encode error", "err", err)
	}
}
