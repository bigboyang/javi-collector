// Package server - gRPC OTLP 수신 서버.
//
// TraceService, MetricsService, LogsService를 구현하며,
// 수신된 *req 구조체를 직접 ingester.IngestFrom*Proto 경로로 처리한다.
//
// gRPC → 재직렬화 제거:
// 기존 구현은 gRPC가 Unmarshal한 *req를 proto.Marshal로 다시 직렬화한 뒤
// ingester가 proto.Unmarshal을 한 번 더 수행했다 (Marshal→Unmarshal 왕복).
// 개선된 구현은 *req를 직접 전달해 이 왕복을 제거한다.
//
// 추가 기능:
//   - gRPC Health Check (grpc.health.v1.Health)
//   - gRPC Server Reflection (proto 목록 조회)
//   - Prometheus 지표 (수신 RPC 수, 오류 수)
package server

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/kkc/javi-collector/internal/ingester"
)

// ---- Prometheus 지표 ----

var (
	grpcRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "grpc",
		Name:      "requests_total",
		Help:      "Total number of gRPC OTLP requests received.",
	}, []string{"service"}) // service: TraceService | MetricsService | LogsService

	grpcErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "grpc",
		Name:      "errors_total",
		Help:      "Total number of gRPC OTLP request errors.",
	}, []string{"service"})
)

// GRPCServer는 OTLP/gRPC 수신 서버다.
// TraceService, MetricsService, LogsService, Health, Reflection을 단일 grpc.Server에 등록한다.
type GRPCServer struct {
	srv      *grpc.Server
	health   *health.Server
	ingester *ingester.Ingester
	addr     string
}

// NewGRPCServer는 OTLP gRPC 서버를 생성하고 서비스를 등록한다.
//
// 성능 설정:
//   - MaxRecvMsgSize 64 MiB: Java 에이전트가 대규모 배치를 보낼 때 거부되지 않도록.
//   - Keepalive: 클라이언트 연결이 idle 상태에서도 끊기지 않게 유지.
//     Java 에이전트는 기본 10분 keepalive를 사용하므로 서버를 더 관대하게 설정.
func NewGRPCServer(addr string, ing *ingester.Ingester) *GRPCServer {
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(64*1024*1024), // 64 MiB
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 60 * time.Second,
			Time:              30 * time.Second,
			Timeout:           5 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	// gRPC Health Check 서버
	// 쿠버네티스 liveness/readiness probe가 grpc.health.v1.Health/Check를 호출한다.
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus("otlp.collector", grpc_health_v1.HealthCheckResponse_SERVING)

	s := &GRPCServer{srv: srv, health: healthSrv, ingester: ing, addr: addr}

	// OTLP 서비스 등록
	collectortracev1.RegisterTraceServiceServer(srv, &traceServiceServer{ing: ing})
	collectormetricsv1.RegisterMetricsServiceServer(srv, &metricsServiceServer{ing: ing})
	collectorlogsv1.RegisterLogsServiceServer(srv, &logsServiceServer{ing: ing})

	// Health Check 서비스 등록 (grpc.health.v1.Health)
	grpc_health_v1.RegisterHealthServer(srv, healthSrv)

	// Server Reflection 등록: grpcurl, grpc-health-probe 등의 도구가 서비스 목록을 조회 가능
	reflection.Register(srv)

	return s
}

// Start는 TCP 리스너를 열고 gRPC 서버를 블로킹 실행한다.
func (g *GRPCServer) Start() error {
	ln, err := net.Listen("tcp", g.addr)
	if err != nil {
		return err
	}
	slog.Info("gRPC server starting", "addr", g.addr)
	return g.srv.Serve(ln)
}

// Stop은 진행 중인 RPC가 완료될 때까지 기다린 후 서버를 종료한다.
func (g *GRPCServer) Stop() {
	// Health 상태를 NOT_SERVING으로 변경해 새 요청이 들어오지 않도록 한다.
	g.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	g.srv.GracefulStop()
}

// ---- TraceService 구현 ----

type traceServiceServer struct {
	collectortracev1.UnimplementedTraceServiceServer
	ing *ingester.Ingester
}

// Export는 Java APM 에이전트가 보낸 ExportTraceServiceRequest를 처리한다.
// gRPC 프레임워크가 이미 Unmarshal한 *req를 직접 ingester에 전달해
// proto.Marshal → proto.Unmarshal 왕복을 제거한다.
func (s *traceServiceServer) Export(
	ctx context.Context,
	req *collectortracev1.ExportTraceServiceRequest,
) (*collectortracev1.ExportTraceServiceResponse, error) {
	grpcRequestsTotal.WithLabelValues("TraceService").Inc()

	count, err := s.ing.IngestTracesFromProto(ctx, req)
	if err != nil {
		grpcErrorsTotal.WithLabelValues("TraceService").Inc()
		slog.Warn("gRPC trace ingest error", "err", err)
		// 채널 backpressure: ResourceExhausted를 반환하면
		// Java OTel SDK가 재시도 또는 로컬 버퍼링을 수행한다.
		return nil, status.Errorf(codes.ResourceExhausted, "ingest failed: %v", err)
	}
	slog.Debug("gRPC /TraceService/Export", "spans", count)
	return &collectortracev1.ExportTraceServiceResponse{}, nil
}

// ---- MetricsService 구현 ----

type metricsServiceServer struct {
	collectormetricsv1.UnimplementedMetricsServiceServer
	ing *ingester.Ingester
}

func (s *metricsServiceServer) Export(
	ctx context.Context,
	req *collectormetricsv1.ExportMetricsServiceRequest,
) (*collectormetricsv1.ExportMetricsServiceResponse, error) {
	grpcRequestsTotal.WithLabelValues("MetricsService").Inc()

	count, err := s.ing.IngestMetricsFromProto(ctx, req)
	if err != nil {
		grpcErrorsTotal.WithLabelValues("MetricsService").Inc()
		slog.Warn("gRPC metric ingest error", "err", err)
		return nil, status.Errorf(codes.ResourceExhausted, "ingest failed: %v", err)
	}
	slog.Debug("gRPC /MetricsService/Export", "count", count)
	return &collectormetricsv1.ExportMetricsServiceResponse{}, nil
}

// ---- LogsService 구현 ----

type logsServiceServer struct {
	collectorlogsv1.UnimplementedLogsServiceServer
	ing *ingester.Ingester
}

func (s *logsServiceServer) Export(
	ctx context.Context,
	req *collectorlogsv1.ExportLogsServiceRequest,
) (*collectorlogsv1.ExportLogsServiceResponse, error) {
	grpcRequestsTotal.WithLabelValues("LogsService").Inc()

	count, err := s.ing.IngestLogsFromProto(ctx, req)
	if err != nil {
		grpcErrorsTotal.WithLabelValues("LogsService").Inc()
		slog.Warn("gRPC log ingest error", "err", err)
		return nil, status.Errorf(codes.ResourceExhausted, "ingest failed: %v", err)
	}
	slog.Debug("gRPC /LogsService/Export", "count", count)
	return &collectorlogsv1.ExportLogsServiceResponse{}, nil
}
