package model

// LogData는 OTLP logs에서 추출한 단일 log record를 나타낸다.
type LogData struct {
	TimestampNanos int64          `json:"timestampNanos"`
	SeverityNumber int32          `json:"severityNumber"` // 1=TRACE … 24=FATAL4
	SeverityText   string         `json:"severityText"`
	Body           string         `json:"body"`
	Attributes     map[string]any `json:"attributes"`
	TraceID        string         `json:"traceId"`
	SpanID         string         `json:"spanId"`
	ServiceName    string         `json:"serviceName"`
	ScopeName      string         `json:"scopeName"`
	ReceivedAtMs   int64          `json:"receivedAtMs"`
}

// SeverityLabel은 severityNumber를 사람이 읽기 쉬운 레이블로 변환한다.
// Java LogData.severityLabel()과 동일한 로직.
func (l *LogData) SeverityLabel() string {
	switch {
	case l.SeverityNumber <= 0:
		return "UNSPECIFIED"
	case l.SeverityNumber <= 4:
		return "TRACE"
	case l.SeverityNumber <= 8:
		return "DEBUG"
	case l.SeverityNumber <= 12:
		return "INFO"
	case l.SeverityNumber <= 16:
		return "WARN"
	case l.SeverityNumber <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}
