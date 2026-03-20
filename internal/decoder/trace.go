package decoder

import (
	"encoding/hex"
	"time"

	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/kkc/javi-collector/internal/model"
)

// DecodeTraces parses OTLP ExportTraceServiceRequest protobuf bytes into SpanData slice.
func DecodeTraces(b []byte) ([]*model.SpanData, error) {
	req := &collectortracev1.ExportTraceServiceRequest{}
	if err := proto.Unmarshal(b, req); err != nil {
		return nil, err
	}

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
					ParentSpanID:  hex.EncodeToString(s.ParentSpanId),
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
	return spans, nil
}

// DecodeTracesJSON parses OTLP ExportTraceServiceRequest JSON bytes into SpanData slice.
func DecodeTracesJSON(b []byte) ([]*model.SpanData, error) {
	req := &collectortracev1.ExportTraceServiceRequest{}
	if err := protojson.Unmarshal(b, req); err != nil {
		return nil, err
	}
	pb, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return DecodeTraces(pb)
}
