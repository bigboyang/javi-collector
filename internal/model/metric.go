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
// GAUGE/SUM은 Value를 사용하고, HISTOGRAM은 Count/Sum/BucketCounts를 사용한다.
type DataPoint struct {
	Attributes     map[string]any `json:"attributes"`
	StartTimeNanos int64          `json:"startTimeNanos"`
	TimeNanos      int64          `json:"timeNanos"`
	Value          float64        `json:"value"`
	Count          int64          `json:"count"`
	Sum            float64        `json:"sum"`
	BucketCounts   []float64      `json:"bucketCounts,omitempty"`
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
