package model

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
}

// DurationNano는 span의 소요 시간을 나노초로 반환한다.
func (s *SpanData) DurationNano() int64 {
	return s.EndTimeNano - s.StartTimeNano
}
