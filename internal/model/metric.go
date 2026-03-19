package model

// MetricType은 OTLP metric 종류를 나타낸다.
type MetricType string

const (
	MetricTypeGauge       MetricType = "GAUGE"
	MetricTypeSum         MetricType = "SUM"
	MetricTypeHistogram   MetricType = "HISTOGRAM"
	MetricTypeSummary     MetricType = "SUMMARY"
	MetricTypeUnspecified MetricType = "UNSPECIFIED"
)

// DataPoint는 단일 metric 측정값이다.
// GAUGE/SUM은 Value를 사용하고, HISTOGRAM은 Count/Sum/BucketCounts/ExplicitBounds를 사용한다.
type DataPoint struct {
	Attributes     map[string]any `json:"attributes"`
	StartTimeNanos int64          `json:"startTimeNanos"`
	TimeNanos      int64          `json:"timeNanos"`
	Value          float64        `json:"value"`
	Count          int64          `json:"count"`
	Sum            float64        `json:"sum"`
	BucketCounts   []uint64       `json:"bucketCounts,omitempty"`
	// ExplicitBounds는 Histogram의 bucket 경계값 (len = len(BucketCounts) - 1)
	// e.g., [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
	ExplicitBounds []float64 `json:"explicitBounds,omitempty"`
}

// MetricData는 OTLP metrics에서 추출한 단일 metric을 나타낸다.
type MetricData struct {
	Name         string     `json:"name"`
	Description  string     `json:"description"`
	Unit         string     `json:"unit"`
	Type         MetricType `json:"metricType"`
	DataPoints   []DataPoint `json:"dataPoints"`
	ServiceName  string     `json:"serviceName"`
	ScopeName    string     `json:"scopeName"`
	ReceivedAtMs int64      `json:"receivedAtMs"`
}
