package server

import (
	"context"
	"log/slog"
	"net/http"

	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/kkc/javi-collector/internal/sampling"
)

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
