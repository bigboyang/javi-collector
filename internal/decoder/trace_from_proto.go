package decoder

// DecodeTracesFromProto는 이미 파싱된 OTLP proto 구조체에서 직접 SpanData를 추출한다.
//
// gRPC 경로: gRPC 프레임워크가 이미 Unmarshal을 완료했으므로 재직렬화(Marshal → Unmarshal)없이
// 구조체를 직접 변환한다. HTTP 경로는 기존 DecodeTraces(bytes)를 그대로 사용한다.
//
// 이중 직렬화 제거 효과:
//   - 기존: gRPC Unmarshal → proto.Marshal → proto.Unmarshal (3회 protobuf 처리)
//   - 개선: gRPC Unmarshal → 직접 변환 (1회 protobuf 처리)
//   - 100k spans/sec 기준 약 15-20% CPU 절감 예상 (marshal 비용 제거)

import (
	"encoding/hex"
	"time"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/kkc/javi-collector/internal/model"
)

// DecodeTracesFromRequest는 파싱된 ExportTraceServiceRequest에서 SpanData를 추출한다.
func DecodeTracesFromRequest(req *collectortracev1.ExportTraceServiceRequest) []*model.SpanData {
	now := time.Now().UnixMilli()
	var spans []*model.SpanData

	for _, rs := range req.ResourceSpans {
		serviceName := ""
		if rs.Resource != nil {
			serviceName = extractServiceName(rs.Resource.Attributes)
		}
		for _, ss := range rs.ScopeSpans {
			scopeName := ""
			if ss.Scope != nil {
				scopeName = ss.Scope.Name
			}
			for _, s := range ss.Spans {
				spans = append(spans, &model.SpanData{
					TraceID:       hex.EncodeToString(s.TraceId),
					SpanID:        hex.EncodeToString(s.SpanId),
					ParentSpanID:  encodeID(s.ParentSpanId),
					Name:          s.Name,
					Kind:          int32(s.Kind),
					StartTimeNano: int64(s.StartTimeUnixNano),
					EndTimeNano:   int64(s.EndTimeUnixNano),
					Attributes:    convertAttrs(s.Attributes),
					StatusCode:    int32(s.Status.GetCode()),
					StatusMessage: s.Status.GetMessage(),
					ServiceName:   serviceName,
					ScopeName:     scopeName,
					ReceivedAtMs:  now,
				})
			}
		}
	}
	return spans
}

// DecodeMetricsFromRequest는 파싱된 ExportMetricsServiceRequest에서 MetricData를 추출한다.
func DecodeMetricsFromRequest(req *collectormetricsv1.ExportMetricsServiceRequest) []*model.MetricData {
	now := time.Now().UnixMilli()
	var metrics []*model.MetricData

	for _, rm := range req.ResourceMetrics {
		serviceName := ""
		if rm.Resource != nil {
			serviceName = extractServiceName(rm.Resource.Attributes)
		}
		for _, sm := range rm.ScopeMetrics {
			scopeName := ""
			if sm.Scope != nil {
				scopeName = sm.Scope.Name
			}
			for _, m := range sm.Metrics {
				md := convertMetric(m, serviceName, scopeName, now)
				metrics = append(metrics, md)
			}
		}
	}
	return metrics
}

// DecodeLogsFromRequest는 파싱된 ExportLogsServiceRequest에서 LogData를 추출한다.
func DecodeLogsFromRequest(req *collectorlogsv1.ExportLogsServiceRequest) []*model.LogData {
	now := time.Now().UnixMilli()
	var logs []*model.LogData

	for _, rl := range req.ResourceLogs {
		serviceName := ""
		if rl.Resource != nil {
			serviceName = extractServiceName(rl.Resource.Attributes)
		}
		for _, sl := range rl.ScopeLogs {
			scopeName := ""
			if sl.Scope != nil {
				scopeName = sl.Scope.Name
			}
			for _, lr := range sl.LogRecords {
				logs = append(logs, &model.LogData{
					TimestampNanos: int64(lr.TimeUnixNano),
					SeverityNumber: int32(lr.SeverityNumber),
					SeverityText:   lr.SeverityText,
					Body:           anyValueToString(lr.Body),
					Attributes:     convertAttrs(lr.Attributes),
					TraceID:        hex.EncodeToString(lr.TraceId),
					SpanID:         hex.EncodeToString(lr.SpanId),
					ServiceName:    serviceName,
					ScopeName:      scopeName,
					ReceivedAtMs:   now,
				})
			}
		}
	}
	return logs
}
