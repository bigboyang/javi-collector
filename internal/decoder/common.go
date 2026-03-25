package decoder

import (
	"encoding/hex"
	"fmt"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/kkc/javi-collector/internal/model"
)

// encodeID converts a byte slice to a hex string.
// Root span의 ParentSpanId처럼 모든 바이트가 0인 경우 빈 문자열을 반환한다.
// hex.EncodeToString([]byte{0,0,...})은 "0000..."을 반환해 root span 감지를 방해하기 때문이다.
func encodeID(b []byte) string {
	for _, v := range b {
		if v != 0 {
			return hex.EncodeToString(b)
		}
	}
	return ""
}

func extractServiceName(attrs []*commonv1.KeyValue) string {
	for _, kv := range attrs {
		if kv.Key == "service.name" {
			if sv, ok := kv.Value.GetValue().(*commonv1.AnyValue_StringValue); ok {
				return sv.StringValue
			}
		}
	}
	return ""
}

func convertAttrs(attrs []*commonv1.KeyValue) map[string]any {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		m[kv.Key] = convertAnyValue(kv.Value)
	}
	return m
}

func convertAnyValue(v *commonv1.AnyValue) any {
	if v == nil {
		return nil
	}
	switch val := v.GetValue().(type) {
	case *commonv1.AnyValue_StringValue:
		return val.StringValue
	case *commonv1.AnyValue_BoolValue:
		return val.BoolValue
	case *commonv1.AnyValue_IntValue:
		return val.IntValue
	case *commonv1.AnyValue_DoubleValue:
		return val.DoubleValue
	default:
		return fmt.Sprintf("%v", v)
	}
}

func anyValueToString(v *commonv1.AnyValue) string {
	if v == nil {
		return ""
	}
	if sv, ok := v.GetValue().(*commonv1.AnyValue_StringValue); ok {
		return sv.StringValue
	}
	return fmt.Sprintf("%v", convertAnyValue(v))
}

// convertLinks는 OTLP Span.Links를 model.SpanLink 슬라이스로 변환한다.
func convertLinks(links []*tracev1.Span_Link) []model.SpanLink {
	if len(links) == 0 {
		return nil
	}
	result := make([]model.SpanLink, 0, len(links))
	for _, l := range links {
		result = append(result, model.SpanLink{
			TraceID:    hex.EncodeToString(l.TraceId),
			SpanID:     hex.EncodeToString(l.SpanId),
			TraceState: l.TraceState,
			Attributes: convertAttrs(l.Attributes),
		})
	}
	return result
}
