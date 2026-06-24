package server

import (
	"context"

	jkafka "github.com/kkc/javi-collector/internal/kafka"
	"github.com/kkc/javi-collector/internal/store"
)

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

// BrokenTraceQuerier는 root span이 없는 브로큰 트레이스를 탐지하는 인터페이스다.
type BrokenTraceQuerier interface {
	QueryBrokenTraces(ctx context.Context, service string, fromMs, toMs int64, limit int) ([]map[string]any, error)
}

// ErrorGroupQuerier는 에러 그룹 집계를 지원하는 인터페이스다.
type ErrorGroupQuerier interface {
	QueryErrorGroups(ctx context.Context, service string, fromMs, toMs int64, limit int) ([]map[string]any, error)
}

// ServiceCatalogManager는 서비스 카탈로그 CRUD를 지원하는 인터페이스다.
type ServiceCatalogManager interface {
	UpsertService(ctx context.Context, e store.ServiceCatalogEntry) error
	GetService(ctx context.Context, name string) (*store.ServiceCatalogEntry, error)
	ListServices(ctx context.Context) ([]store.ServiceCatalogEntry, error)
}

// CorrelatedSignalQuerier는 trace_id 기반 통합 시그널 조회를 지원하는 인터페이스다.
// Gap 1: Correlated Signal Navigation — spans·logs·RED 메트릭을 한 번에 반환한다.
type CorrelatedSignalQuerier interface {
	QueryTraceContext(ctx context.Context, traceID string) (map[string]any, error)
}

// SLOManager는 SLO 정의 관리와 번-레이트 알람 조회를 지원하는 인터페이스다.
// Gap 3: SLO/SLI + Burn-Rate Alerting
type SLOManager interface {
	UpsertSLO(ctx context.Context, def store.SLODefinition) error
	ListSLOs(ctx context.Context) ([]store.SLODefinition, error)
	GetBurnAlerts(ctx context.Context, service string, limit int) ([]store.SLOBurnAlert, error)
}

// RCAReportQuerier는 RCA 분석 결과 조회와 피드백 업데이트를 지원하는 인터페이스다.
// P1: RCA 결과 소비 경로 — rca_reports 테이블을 조회한다.
type RCAReportQuerier interface {
	QueryRCAReports(ctx context.Context, service, severity string, fromMs, toMs int64, limit int) ([]store.RCAReport, error)
	UpdateRCAFeedback(ctx context.Context, id string, resolved uint8, feedback string) error
}

// DeploymentEventWriter는 배포 이벤트 기록을 지원하는 인터페이스다.
// GAP-04: Deployment Event Correlation — CI/CD 파이프라인에서 POST /api/events/deployment 호출.
type DeploymentEventWriter interface {
	InsertEvent(ctx context.Context, e store.DeploymentEvent) error
}

// TraceWaterfallQuerier는 trace_id 기반 폭포수 뷰 + 임계 경로 분석을 지원하는 인터페이스다.
// GAP-01: Trace Waterfall / Critical Path — Datadog Flame Graph에 해당.
type TraceWaterfallQuerier interface {
	QueryTraceWaterfall(ctx context.Context, traceID string) (map[string]any, error)
}

// AlertRouteManager는 Alert Routing & Escalation 규칙 관리와 이벤트 ack를 지원한다.
// GAP-05: Alert Routing & Escalation — *store.AlertRouteStore 가 구현한다.
type AlertRouteManager interface {
	UpsertRoute(ctx context.Context, r *store.AlertRoute) error
	DeleteRoute(ctx context.Context, id string) error
	ListRoutes(ctx context.Context) ([]store.AlertRoute, error)
	ListAlertHistory(ctx context.Context, service string, limit int) ([]store.AlertEvent, error)
	AckEvent(eventID string)
}

// LogAnalyticsQuerier는 GAP-06 Log Analytics 쿼리를 지원하는 인터페이스다.
// *store.LogAnalyticsStore 가 구현한다.
type LogAnalyticsQuerier interface {
	QueryLogVolume(ctx context.Context, service string, fromMs, toMs int64, intervalSec int) ([]store.LogVolumePoint, error)
	QueryLogSearch(ctx context.Context, q store.LogSearchQuery) ([]store.LogSearchResult, error)
	QueryLogPatterns(ctx context.Context, service string, fromMs, toMs int64, limit int) ([]store.LogPattern, error)
	QueryLogContext(ctx context.Context, service string, timestampNano int64, windowSec int, limit int) ([]store.LogSearchResult, error)
	QueryLogFields(ctx context.Context, service string, fromMs, toMs int64) (*store.LogFieldStats, error)
}

// HistogramMVQuerier는 mv_histogram_1m_state 집계 뷰를 조회하는 인터페이스다.
// ClickHouseMetricStore가 구현한다.
type HistogramMVQuerier interface {
	QueryHistogramMV(ctx context.Context, service, name string, fromMs, toMs int64, limit int) ([]map[string]any, error)
}

// SlowQueryQuerier는 DB 슬로우 쿼리 MV 조회를 지원하는 인터페이스다.
// ClickHouseTraceStore가 구현하며, nil이면 /api/collector/slow-queries가 501을 반환한다.
type SlowQueryQuerier interface {
	QuerySlowQueries(ctx context.Context, service string, fromMs, toMs, thresholdMs int64, limit int) ([]map[string]any, error)
}

// InfraCorrelationQuerier는 서비스의 k8s 컨텍스트와 JVM/인프라 메트릭 상관 분석을 지원한다.
// GAP-08: Infra Metrics Correlation — ClickHouseTraceStore가 구현한다.
type InfraCorrelationQuerier interface {
	QueryInfraCorrelation(ctx context.Context, service string, fromMs, toMs int64) (map[string]any, error)
}

// ProfilingWriter는 프로파일링 스냅샷 쓰기/조회를 지원한다.
// GAP-07: Continuous Profiling — *store.ProfilingStore가 구현한다.
type ProfilingWriter interface {
	InsertSnapshot(ctx context.Context, snap store.ProfilingSnapshot) error
	QuerySnapshots(ctx context.Context, p store.QuerySnapshotsParams) ([]store.ProfilingSnapshot, error)
	GetSnapshotPayload(ctx context.Context, id string) (*store.ProfilingSnapshot, error)
	QueryProfileSummary(ctx context.Context, fromMs, toMs int64) ([]map[string]any, error)
}

// K8sMetricsWriter는 Pod 리소스 메트릭 쓰기/조회를 지원한다.
// GAP-08 확장: Infra Metrics Correlation — *store.K8sPodMetricsStore가 구현한다.
type K8sMetricsWriter interface {
	InsertMetric(ctx context.Context, m store.K8sPodMetric) error
	QueryMetrics(ctx context.Context, p store.QueryK8sMetricsParams) ([]store.K8sPodMetric, error)
	QueryPodSummary(ctx context.Context, service string, fromMs, toMs int64) ([]map[string]any, error)
}

// ReadinessChecker는 /readyz 상세 상태 조회를 위한 인터페이스다.
type ReadinessChecker interface {
	Ping(ctx context.Context) error
	ChannelStatus() map[string]any
}

// DeploymentPublisher는 배포 이벤트 발행 인터페이스다.
type DeploymentPublisher interface {
	Publish(ev jkafka.DeploymentEvent)
}
