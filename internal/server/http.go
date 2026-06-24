// Package server는 OTLP/HTTP 수신 엔드포인트와 REST 조회 API를 제공한다.
//
// 수신 경로:
//
//	POST /v1/traces   — ExportTraceServiceRequest (application/x-protobuf)
//	POST /v1/metrics  — ExportMetricsServiceRequest
//	POST /v1/logs     — ExportLogsServiceRequest
//
// 조회 경로 (Polling):
//
//	GET /api/collector/traces?limit=100
//	GET /api/collector/metrics?limit=100
//	GET /api/collector/logs?limit=100
//	GET /api/collector/stats
//	GET /api/collector/red?service=svc&from=<ms>&to=<ms>
//	GET /api/collector/topology?from=<ms>&to=<ms>
//	GET /api/collector/error-logs?service=svc&from=<ms>&to=<ms>
//	GET /api/collector/anomalies?service=svc&severity=critical&from=<ms>&to=<ms>&limit=100
//	GET /api/collector/histogram?service=svc&name=<metric>&from=<ms>&to=<ms>&limit=100
//
// SSE 실시간 스트리밍:
//
//	GET /api/stream/logs?service=svc&severity=ERROR   — 신규 로그 (3초 폴링)
//	GET /api/stream/alerts?service=svc&severity=warn  — 신규 이상 감지 알림 (10초 폴링)
//
// On-Demand:
//
//	GET /api/query?sql=SELECT+...  — SELECT 전용 raw SQL (화이트리스트 검증)
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
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

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

// HTTPServer는 OTLP/HTTP 수신 + REST 조회 API + 운영 엔드포인트 서버다.
type HTTPServer struct {
	ingester    *ingester.Ingester
	traceStore  store.TraceStore
	metricStore store.MetricStore
	logStore    store.LogStore
	srv         *http.Server
	ready       chan struct{} // close되면 readyz가 200을 반환한다
	draining    atomic.Bool   // true이면 readyz가 503을 반환한다 (graceful shutdown 드레인)

	// traceRouter는 멀티 인스턴스 Tail Sampling 시 traceID 기반 라우팅을 담당한다.
	// nil이면 라우팅 비활성화 (단일 인스턴스 또는 Sampling 미사용 배포).
	traceRouter *sampling.TraceRouter

	// catalog는 서비스 카탈로그 CRUD를 담당한다.
	// nil이면 서비스 카탈로그 비활성화 (ClickHouse 미사용 배포).
	catalog ServiceCatalogManager

	// errorGroups는 에러 그룹 집계를 담당한다.
	// nil이면 에러 그룹 비활성화.
	errorGroups ErrorGroupQuerier

	// traceContext는 trace_id 기반 통합 시그널 조회를 담당한다.
	// nil이면 /api/collector/trace-context 가 501을 반환한다.
	traceContext CorrelatedSignalQuerier

	// sloManager는 SLO 정의·번-레이트 알람 관리를 담당한다.
	// nil이면 /api/slo/* 가 501을 반환한다.
	sloManager SLOManager

	// rcaReports는 RCA 결과 조회와 피드백 업데이트를 담당한다.
	// nil이면 /api/rca/* 가 501을 반환한다.
	rcaReports RCAReportQuerier

	// deployProducer는 CI/CD 배포 이벤트를 Kafka deploys 토픽에 발행한다.
	// nil이면 Kafka 발행을 건너뛴다.
	deployProducer DeploymentPublisher

	// deploymentStore는 배포 이벤트를 ClickHouse에 직접 기록한다.
	// GAP-04: nil이면 ClickHouse 저장을 건너뛴다.
	deploymentStore DeploymentEventWriter

	// traceWaterfall은 trace_id 기반 폭포수 뷰 + 임계 경로 분석을 담당한다.
	// GAP-01: nil이면 /api/collector/trace-waterfall 가 501을 반환한다.
	traceWaterfall TraceWaterfallQuerier

	// alertRoutes는 Alert Routing & Escalation 규칙 관리를 담당한다.
	// GAP-05: nil이면 /api/alerts/* 가 501을 반환한다.
	alertRoutes AlertRouteManager

	// logAnalytics는 Log Analytics 쿼리를 담당한다.
	// GAP-06: nil이면 /api/logs/* 가 501을 반환한다.
	logAnalytics LogAnalyticsQuerier

	// slowQueryQuerier는 DB 슬로우 쿼리 MV 조회를 담당한다.
	// nil이면 /api/collector/slow-queries 가 501을 반환한다.
	slowQueryQuerier SlowQueryQuerier

	// infraCorrelation은 서비스의 k8s 컨텍스트와 JVM/인프라 메트릭 상관 분석을 담당한다.
	// GAP-08: nil이면 /api/collector/infra-correlation 가 501을 반환한다.
	infraCorrelation InfraCorrelationQuerier

	// profilingStore는 프로파일링 스냅샷 쓰기/조회를 담당한다.
	// GAP-07: nil이면 /api/collector/profiling 가 501을 반환한다.
	profilingStore ProfilingWriter

	// k8sMetrics는 Pod 리소스 메트릭(CPU/메모리) 쓰기/조회를 담당한다.
	// GAP-08 확장: nil이면 /api/collector/k8s-metrics 가 501을 반환한다.
	k8sMetrics K8sMetricsWriter

	// topoCache는 서비스 토폴로지 조회 결과를 TTL 기반으로 캐싱한다.
	// 키: topologyCacheKey (5분 버킷), 값: topologyCacheEntry
	// 매 요청마다 ClickHouse MV를 재스캔하는 오버헤드를 제거한다.
	topoCache    sync.Map
	topoCacheTTL time.Duration // 기본 60초

	// apiKey가 비어 있지 않으면 /api/* 요청에 X-Api-Key 헤더 인증을 적용한다.
	// OTLP 수신 경로(/v1/*), 운영 경로(/healthz, /readyz, /metrics)는 제외.
	apiKey string
}

func NewHTTPServer(addr string, ing *ingester.Ingester,
	ts store.TraceStore, ms store.MetricStore, ls store.LogStore) *HTTPServer {

	s := &HTTPServer{
		ingester:     ing,
		traceStore:   ts,
		metricStore:  ms,
		logStore:     ls,
		ready:        make(chan struct{}),
		topoCacheTTL: 60 * time.Second,
	}

	mux := http.NewServeMux()

	// apiHandle은 /api/* 경로 등록 시 API Key 인증 미들웨어를 자동으로 감싼다.
	apiHandle := func(pattern string, handler http.HandlerFunc) {
		mux.HandleFunc(pattern, s.requireAPIKey(handler))
	}

	// OTLP 수신 엔드포인트 — 인증 제외 (Agent 측 헤더 설정 없음)
	mux.HandleFunc("/v1/traces", s.handleTraces)
	mux.HandleFunc("/v1/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/logs", s.handleLogs)
	// CI/CD 배포 이벤트 수신 엔드포인트
	mux.HandleFunc("/v1/events/deploy", s.handleDeployEvent)

	// REST 조회 엔드포인트 — API Key 인증 적용 (API_KEY 설정 시)
	apiHandle("/api/collector/traces", s.queryTraces)
	apiHandle("/api/collector/metrics", s.queryMetrics)
	apiHandle("/api/collector/logs", s.queryLogs)
	apiHandle("/api/collector/stats", s.stats)
	// 대시보드용 집계 엔드포인트 (ClickHouse MV 기반)
	apiHandle("/api/collector/red", s.queryRED)
	apiHandle("/api/collector/topology", s.queryTopology)
	apiHandle("/api/collector/error-logs", s.queryErrorLogs)
	apiHandle("/api/collector/anomalies", s.queryAnomalies)
	apiHandle("/api/collector/histogram", s.queryHistogram)
	// SSE 실시간 스트리밍 엔드포인트
	apiHandle("/api/stream/logs", s.streamLogs)
	apiHandle("/api/stream/alerts", s.streamAlerts)
	// ClickHouse 직접 쿼리 (화이트리스트 SELECT)
	apiHandle("/api/query", s.queryRaw)
	// RAG 벡터 검색 (EMBED_ENABLED=true 시 활성)
	// 브로큰 트레이스 탐지 (root span 없는 트레이스)
	apiHandle("/api/collector/broken-traces", s.queryBrokenTraces)
	// 에러 그룹 집계 (Error Tracking)
	apiHandle("/api/collector/error-groups", s.queryErrorGroups)
	// Gap 1: Correlated Signal Navigation — trace_id 기반 spans·logs·메트릭 통합 조회
	apiHandle("/api/collector/trace-context", s.queryTraceContext)
	// GAP-01: Trace Waterfall / Critical Path — 폭포수 뷰 + 임계 경로 분석
	apiHandle("/api/collector/trace-waterfall", s.queryTraceWaterfall)
	// 서비스 카탈로그 (팀 소유권, 운영 메타데이터)
	apiHandle("/api/catalog/services", s.listCatalogServices)
	apiHandle("/api/catalog/service", s.catalogService)
	// Gap 3: SLO/SLI + Burn-Rate Alerting
	apiHandle("/api/slo/definitions", s.sloDefinitions)
	apiHandle("/api/slo/burn-alerts", s.sloBurnAlerts)
	// P1: RCA 결과 조회 + 피드백
	apiHandle("/api/rca/reports", s.queryRCAReports)
	apiHandle("/api/rca/feedback", s.updateRCAFeedback)

	// GAP-05: Alert Routing & Escalation
	apiHandle("/api/alerts/routes", s.alertRoutes_)
	apiHandle("/api/alerts/history", s.alertHistory)
	apiHandle("/api/alerts/ack", s.alertAck)

	// GAP-06: Log Analytics
	apiHandle("/api/logs/volume", s.queryLogVolume)
	apiHandle("/api/logs/search", s.queryLogSearch)
	apiHandle("/api/logs/patterns", s.queryLogPatterns)
	apiHandle("/api/logs/context", s.queryLogContext)
	apiHandle("/api/logs/fields", s.queryLogFields)
	// DB Slow Query MV — db_system != '' 스팬 중 임계값 초과 쿼리 조회
	apiHandle("/api/collector/slow-queries", s.querySlowQueries)
	// GAP-08: Infra Metrics Correlation — k8s 컨텍스트 + JVM/인프라 메트릭 상관
	apiHandle("/api/collector/infra-correlation", s.queryInfraCorrelation)
	// GAP-07: Continuous Profiling — 프로파일링 스냅샷 수신/조회
	apiHandle("/api/collector/profiling", s.handleProfiling)
	apiHandle("/api/collector/profiling/payload", s.handleProfilingPayload)
	apiHandle("/api/collector/profiling/summary", s.handleProfilingSummary)
	// GAP-08 확장: K8s Pod 리소스 메트릭 수신/조회 — Agent cgroup 수집값
	apiHandle("/api/collector/k8s-metrics", s.handleK8sMetrics)
	apiHandle("/api/collector/k8s-metrics/summary", s.handleK8sMetricsSummary)

	// 운영 엔드포인트 — 인증 제외 (로드밸런서/프로메테우스 헬스체크)
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

// SetServiceCatalog는 서비스 카탈로그 관리자를 등록한다.
// nil이면 /api/catalog/* 가 501을 반환한다.
func (s *HTTPServer) SetServiceCatalog(c ServiceCatalogManager) {
	s.catalog = c
}

// SetErrorGroups는 에러 그룹 집계기를 등록한다.
// nil이면 /api/collector/error-groups 가 501을 반환한다.
func (s *HTTPServer) SetErrorGroups(eg ErrorGroupQuerier) {
	s.errorGroups = eg
}

// SetTraceContext는 trace_id 기반 통합 시그널 조회기를 등록한다.
// nil이면 /api/collector/trace-context 가 501을 반환한다.
func (s *HTTPServer) SetTraceContext(tc CorrelatedSignalQuerier) {
	s.traceContext = tc
}

// SetSLOManager는 SLO 관리자를 등록한다.
// nil이면 /api/slo/* 가 501을 반환한다.
func (s *HTTPServer) SetSLOManager(sm SLOManager) {
	s.sloManager = sm
}

// SetRCAReports는 RCA 보고서 조회기를 등록한다.
// nil이면 /api/rca/* 가 501을 반환한다.
func (s *HTTPServer) SetRCAReports(rq RCAReportQuerier) {
	s.rcaReports = rq
}

// SetDeployProducer는 배포 이벤트 Kafka 프로듀서를 등록한다.
func (s *HTTPServer) SetDeployProducer(p DeploymentPublisher) {
	s.deployProducer = p
}

// SetDeploymentStore는 배포 이벤트 ClickHouse 저장소를 등록한다.
// GAP-04: RCA Engine이 이상 발생 시간대 ±5분 배포 이벤트를 가설에 포함시킨다.
func (s *HTTPServer) SetDeploymentStore(ds DeploymentEventWriter) {
	s.deploymentStore = ds
}

// SetTraceWaterfall은 Trace Waterfall / Critical Path 조회기를 등록한다.
// GAP-01: nil이면 /api/collector/trace-waterfall 가 501을 반환한다.
func (s *HTTPServer) SetTraceWaterfall(tw TraceWaterfallQuerier) {
	s.traceWaterfall = tw
}

// SetAlertRoutes는 Alert Routing & Escalation 관리자를 등록한다.
// GAP-05: nil이면 /api/alerts/* 가 501을 반환한다.
func (s *HTTPServer) SetAlertRoutes(arm AlertRouteManager) {
	s.alertRoutes = arm
}

// SetLogAnalytics는 Log Analytics 쿼리기를 등록한다.
// GAP-06: nil이면 /api/logs/* 가 501을 반환한다.
func (s *HTTPServer) SetLogAnalytics(laq LogAnalyticsQuerier) {
	s.logAnalytics = laq
}

// SetSlowQueryQuerier는 DB 슬로우 쿼리 MV 조회기를 등록한다.
// nil이면 /api/collector/slow-queries 가 501을 반환한다.
func (s *HTTPServer) SetSlowQueryQuerier(sq SlowQueryQuerier) {
	s.slowQueryQuerier = sq
}

// SetInfraCorrelation은 Infra Metrics Correlation 조회기를 등록한다.
// GAP-08: nil이면 /api/collector/infra-correlation 가 501을 반환한다.
func (s *HTTPServer) SetInfraCorrelation(ic InfraCorrelationQuerier) {
	s.infraCorrelation = ic
}

// SetProfilingStore는 Continuous Profiling 저장소를 등록한다.
// GAP-07: nil이면 /api/collector/profiling 가 501을 반환한다.
func (s *HTTPServer) SetProfilingStore(ps ProfilingWriter) {
	s.profilingStore = ps
}

// SetK8sMetrics는 K8s Pod 메트릭 저장소를 등록한다.
// GAP-08 확장: nil이면 /api/collector/k8s-metrics 가 501을 반환한다.
func (s *HTTPServer) SetK8sMetrics(km K8sMetricsWriter) {
	s.k8sMetrics = km
}

// SetAPIKey는 /api/* 엔드포인트에 적용할 API Key를 설정한다.
// 빈 문자열이면 인증 비활성화. Start() 전에 호출해야 한다.
func (s *HTTPServer) SetAPIKey(key string) {
	s.apiKey = key
}

// requireAPIKey는 API Key 인증 미들웨어다.
// s.apiKey가 설정된 경우 X-Api-Key 헤더 또는 Authorization: Bearer <key>를 검증한다.
// 빈 apiKey이면 인증 없이 통과시킨다.
func (s *HTTPServer) requireAPIKey(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey == "" {
			next(w, r)
			return
		}
		provided := r.Header.Get("X-Api-Key")
		if provided == "" {
			if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
				provided = strings.TrimPrefix(auth, "Bearer ")
			}
		}
		if provided != s.apiKey {
			w.Header().Set("Content-Type", jsonContentType)
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
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

// UnmarkReady는 graceful shutdown 드레인 단계에서 /readyz가 503을 반환하도록 한다.
// Shutdown() 전에 호출하면 로드밸런서가 이 인스턴스로의 라우팅을 중단할 시간을 확보한다.
// 예: UnmarkReady() → sleep(5s) → Shutdown()
func (s *HTTPServer) UnmarkReady() {
	s.draining.Store(true)
}

func (s *HTTPServer) Start() error {
	slog.Info("HTTP server starting", "addr", s.srv.Addr)
	return s.srv.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
