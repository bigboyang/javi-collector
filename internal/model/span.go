package model

// SpanLink는 OTLP Span.Links 필드의 단일 항목을 나타낸다.
// 다른 Trace/Span에 대한 크로스-트레이스 연결 메타데이터를 보존한다.
type SpanLink struct {
	TraceID    string         `json:"traceId"`
	SpanID     string         `json:"spanId"`
	TraceState string         `json:"traceState,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// SpanData는 OTLP trace 프로토콜에서 추출한 단일 span을 나타낸다.
// Java SpanData와 필드 1:1 대응.
type SpanData struct {
	TraceID       string         `json:"traceId"`
	SpanID        string         `json:"spanId"`
	ParentSpanID  string         `json:"parentSpanId"`
	Name          string         `json:"name"`
	Kind          int32          `json:"kind"`
	StartTimeNano int64          `json:"startTimeNanos"`
	EndTimeNano   int64          `json:"endTimeNanos"`
	Attributes    map[string]any `json:"attributes"`
	StatusCode    int32          `json:"statusCode"` // 0=UNSET, 1=OK, 2=ERROR
	StatusMessage string         `json:"statusMessage"`
	ServiceName   string         `json:"serviceName"`
	ScopeName     string         `json:"scopeName"`
	ReceivedAtMs  int64          `json:"receivedAtMs"`
	// W3C Trace State: span 컨텍스트에 포함된 벤더별 트레이싱 메타데이터.
	// OTLP Span.trace_state 필드에서 추출된다.
	TraceState string     `json:"traceState,omitempty"`
	// Links: 다른 Trace/Span에 대한 크로스-트레이스 연결 목록.
	// OTLP Span.links 필드에서 추출된다.
	Links      []SpanLink `json:"links,omitempty"`
}

// DurationNano는 span의 소요 시간을 나노초로 반환한다.
func (s *SpanData) DurationNano() int64 {
	return s.EndTimeNano - s.StartTimeNano
}
