package rca

import "time"

// AnomalyRow는 anomalies 테이블에서 읽어온 이상 이벤트.
type AnomalyRow struct {
	ID            string
	ServiceName   string
	SpanName      string
	AnomalyType   string
	Minute        time.Time
	CurrentValue  float64
	BaselineValue float64
	ZScore        float64
	Severity      string
	DetectedAt    time.Time
}

// CorrelatedSpan은 이상 발생 시간대의 관련 ERROR/WARN 스팬 요약.
type CorrelatedSpan struct {
	SpanID        string  `json:"span_id"`
	TraceID       string  `json:"trace_id"`
	Name          string  `json:"name"`
	StatusCode    int32   `json:"status_code"`
	StatusMessage string  `json:"status_message,omitempty"`
	DurationMs    float64 `json:"duration_ms"`
	ExceptionType string  `json:"exception_type,omitempty"`
}

// SimilarIncident는 RAG 검색으로 찾은 과거 유사 장애 요약.
type SimilarIncident struct {
	TraceID     string  `json:"trace_id"`
	ServiceName string  `json:"service_name"`
	Score       float32 `json:"score"`
	Summary     string  `json:"summary"` // 텍스트 앞 300자
}

// RCAReport는 하나의 anomaly에 대한 근본 원인 분석 결과.
type RCAReport struct {
	ID               string
	AnomalyID        string
	ServiceName      string
	SpanName         string
	AnomalyType      string
	Minute           time.Time
	Severity         string
	ZScore           float64
	CorrelatedSpans  []CorrelatedSpan  // 최대 5개
	SimilarIncidents []SimilarIncident // 최대 3개
	Hypothesis       string            // 규칙 기반 가설 문자열
	CreatedAt        time.Time
}
