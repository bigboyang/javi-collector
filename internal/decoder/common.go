package decoder

import (
	"fmt"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

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
