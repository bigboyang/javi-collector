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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/kkc/javi-collector/internal/ingester"
	"github.com/kkc/javi-collector/internal/sampling"
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

// AnomalyQuerier는 이상 감지 결과 조회를 지원하는 저장소 인터페이스다.
type AnomalyQuerier interface {
	QueryAnomalies(ctx context.Context, service, severity string, fromMs, toMs int64, limit int) ([]map[string]any, error)
}

// RawQuerier는 화이트리스트를 통과한 SELECT SQL을 직접 실행하는 인터페이스다.
// ClickHouseTraceStore가 구현하며, 메모리 스토어는 구현하지 않는다.
type RawQuerier interface {
	QueryRaw(ctx context.Context, sql string) ([]map[string]any, error)
}

// ReadinessChecker는 /readyz 상세 상태 조회를 위한 인터페이스다.
type ReadinessChecker interface {
	Ping(ctx context.Context) error
	ChannelStatus() map[string]any
}

// HTTPServer는 OTLP/HTTP 수신 + REST 조회 API + 운영 엔드포인트 서버다.
type HTTPServer struct {
	ingester    *ingester.Ingester
	traceStore  store.TraceStore
	metricStore store.MetricStore
	logStore    store.LogStore
	srv         *http.Server
	ready       chan struct{} // close되면 readyz가 200을 반환한다

	// traceRouter는 멀티 인스턴스 Tail Sampling 시 traceID 기반 라우팅을 담당한다.
	// nil이면 라우팅 비활성화 (단일 인스턴스 또는 Sampling 미사용 배포).
	traceRouter *sampling.TraceRouter
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
	mux.HandleFunc("/api/collector/anomalies", s.queryAnomalies)
	// ClickHouse 직접 쿼리 (화이트리스트 SELECT)
	mux.HandleFunc("/api/query", s.queryRaw)

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

// SetTraceRouter는 멀티 인스턴스 Tail Sampling용 TraceRouter를 설정한다.
// Start() 전에 호출해야 한다.
func (s *HTTPServer) SetTraceRouter(r *sampling.TraceRouter) {
	s.traceRouter = r
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
	switch {
	case isJSON(r):
		// JSON 경로: 라우팅 미지원 (JSON exporter는 일반적으로 테스트/개발용)
		count, err = s.ingester.IngestTracesJSON(r.Context(), body)

	case s.traceRouter != nil && s.traceRouter.Enabled() && r.Header.Get(sampling.RoutedHeader) == "":
		// Protobuf + 라우팅 활성화 + 직접 수신(forwarded 아님):
		// traceID 기반 일관 해시로 spans를 owner 인스턴스별로 분리.
		// 비담당 spans는 해당 피어로 비동기 전달하고, 담당 spans만 로컬에서 처리.
		count, err = s.routeAndIngestTraces(r.Context(), body)

	default:
		// 단일 인스턴스 또는 이미 라우팅된 요청: 직접 처리
		count, err = s.ingester.IngestTraces(r.Context(), body)
	}

	if err != nil {
		slog.Warn("trace ingest error", "err", err)
		// backpressure: Retry-After로 클라이언트가 적절한 간격 후 재시도하도록 유도
		w.Header().Set("Retry-After", "1")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	slog.Debug("POST /v1/traces", "spans", count, "bytes", len(body))
	w.WriteHeader(http.StatusOK)
}

// routeAndIngestTraces는 OTLP protobuf 요청을 traceID 기반으로 라우팅한다.
//
// 1. proto.Unmarshal로 요청을 파싱
// 2. TraceRouter.Route로 spans를 owner별로 분리
// 3. 비담당 spans를 각 피어로 비동기 전달 (context.Background 사용 — HTTP 응답 후에도 전달 완료)
// 4. 담당 spans만 ingester로 처리
func (s *HTTPServer) routeAndIngestTraces(ctx context.Context, body []byte) (int, error) {
	req := &collectortracev1.ExportTraceServiceRequest{}
	if err := proto.Unmarshal(body, req); err != nil {
		return 0, err
	}

	localReq, remoteMap := s.traceRouter.Route(ctx, req)

	// 비담당 spans 비동기 전달: HTTP 응답 전송 후 context가 취소될 수 있으므로
	// context.Background()를 사용해 전달이 완전히 완료되도록 한다.
	for peerURL, remoteReq := range remoteMap {
		go s.traceRouter.Forward(context.Background(), peerURL, remoteReq)
	}

	if len(localReq.GetResourceSpans()) == 0 {
		return 0, nil
	}
	return s.ingester.IngestTracesFromProto(ctx, localReq)
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
		w.Header().Set("Retry-After", "1")
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
		w.Header().Set("Retry-After", "1")
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
// ReadinessChecker 인터페이스를 구현한 traceStore가 있으면 상세 JSON을 함께 반환한다.
//
// 응답 예시:
//
//	{"ready":true,"clickhouse":"ok","channel":{"ch_len":12,"ch_cap":1000,"cb_state":"closed"}}
func (s *HTTPServer) readyz(w http.ResponseWriter, r *http.Request) {
	select {
	case <-s.ready:
		// ready 상태: 상세 진단 정보 포함
		status := map[string]any{"ready": true}
		if checker, ok := s.traceStore.(ReadinessChecker); ok {
			pingCtx := r.Context()
			if err := checker.Ping(pingCtx); err != nil {
				status["clickhouse"] = "error: " + err.Error()
			} else {
				status["clickhouse"] = "ok"
			}
			status["channel"] = checker.ChannelStatus()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(status)
	default:
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}
}

// ---- 헬퍼 ----

// readProtoBody는 HTTP 요청 body를 읽어 반환한다.
// Content-Encoding: gzip이거나 gzip 매직 바이트(0x1f 0x8b)로 시작하면 압축을 해제한다.
// 일부 OTel exporter가 gzip 압축은 하지만 Content-Encoding 헤더를 누락하는 경우를 방어한다.
// gzip.Reader는 sync.Pool에서 재사용해 GC 부담을 줄인다.
// zip-bomb 방어: 압축 전/후 모두 maxBodyBytes로 제한한다.
func readProtoBody(r *http.Request) ([]byte, error) {
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		return decompressGzip(io.LimitReader(r.Body, maxBodyBytes))
	}

	b, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}

	// Fallback: gzip 매직 바이트(0x1f 0x8b) 스니핑.
	// Content-Encoding 헤더 없이 gzip 전송하는 클라이언트 대응.
	// 해당 바이트를 proto.Unmarshal에 그대로 전달하면 wire type 7 오류 발생.
	if len(b) >= 2 && b[0] == 0x1f && b[1] == 0x8b {
		return decompressGzip(bytes.NewReader(b))
	}

	return b, nil
}

// decompressGzip은 gzip 스트림을 해제해 원본 bytes를 반환한다.
func decompressGzip(r io.Reader) ([]byte, error) {
	gz := gzipReaderPool.Get().(*gzip.Reader)
	if err := gz.Reset(r); err != nil {
		gzipReaderPool.Put(gz)
		return nil, err
	}
	defer func() {
		gz.Close()
		gzipReaderPool.Put(gz)
	}()
	return io.ReadAll(io.LimitReader(gz, maxBodyBytes))
}

func isJSON(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	return len(ct) >= len(jsonContentType) && ct[:len(jsonContentType)] == jsonContentType
}

func queryOffset(r *http.Request) int {
	s := r.URL.Query().Get("offset")
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0
	}
	return n
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
