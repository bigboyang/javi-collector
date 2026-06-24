package store

import (
	"encoding/json"
	"fmt"

	"github.com/kkc/javi-collector/internal/model"
)

// ---- OTel 속성 추출 헬퍼 ----

// encodeSpanLinks는 SpanLink 슬라이스를 JSON 문자열로 직렬화한다.
// ClickHouse의 String 컬럼에 저장하며, 빈 슬라이스는 빈 문자열을 반환한다.
func encodeSpanLinks(links []model.SpanLink) string {
	if len(links) == 0 {
		return ""
	}
	b, _ := json.Marshal(links)
	return string(b)
}

// toStringMap은 map[string]any를 ClickHouse Map(String,String) 타입에 맞게 변환한다.
// 비문자열 값은 fmt.Sprintf("%v")로 직렬화한다.
func toStringMap(attrs map[string]any) map[string]string {
	if len(attrs) == 0 {
		return map[string]string{}
	}
	m := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if s, ok := v.(string); ok {
			m[k] = s
		} else {
			m[k] = fmt.Sprintf("%v", v)
		}
	}
	return m
}

// fromStringMap은 ClickHouse Map(String,String)을 map[string]any로 변환한다.
func fromStringMap(m map[string]string) map[string]any {
	if len(m) == 0 {
		return nil
	}
	attrs := make(map[string]any, len(m))
	for k, v := range m {
		attrs[k] = v
	}
	return attrs
}

// toJSONString은 map[string]string을 JSON 문자열로 직렬화한다.
// ClickHouse String 컬럼에 attributes를 저장할 때 사용한다.
func toJSONString(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// fromJSONString은 ClickHouse String 컬럼의 JSON 문자열을 map[string]any로 역직렬화한다.
func fromJSONString(s string) map[string]any {
	if s == "" || s == "{}" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil
	}
	return fromStringMap(m)
}

func strAttr(attrs map[string]any, key string) string {
	if v, ok := attrs[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func uint16Attr(attrs map[string]any, key string) uint16 {
	if v, ok := attrs[key]; ok {
		switch val := v.(type) {
		case float64:
			return uint16(val)
		case int64:
			return uint16(val)
		case int:
			return uint16(val)
		case string:
			var n int
			_, _ = fmt.Sscanf(val, "%d", &n)
			return uint16(n)
		}
	}
	return 0
}
