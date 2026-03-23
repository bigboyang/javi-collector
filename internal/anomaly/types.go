package anomaly

import "time"

// REDPoint는 mv_red_1m_state에서 집계한 1분 단위 RED 메트릭 스냅샷.
type REDPoint struct {
	ServiceName string
	SpanName    string
	HTTPRoute   string
	Minute      time.Time
	P50Ms       float64
	P95Ms       float64
	P99Ms       float64
	ErrorRate   float64 // 0.0–1.0
	RPS         float64 // 분당 요청 수 / 60
	SampleCount uint64
}

// BaselineRow는 red_baseline 테이블의 한 행.
type BaselineRow struct {
	ServiceName string
	SpanName    string
	HTTPRoute   string
	DayOfWeek   uint8
	HourOfDay   uint8
	P50Ms       float64
	P95Ms       float64
	P99Ms       float64
	ErrorRate   float64
	AvgRPS      float64
	SampleCount uint64
}

// AnomalyRecord는 탐지된 이상 이벤트.
type AnomalyRecord struct {
	ID            string
	ServiceName   string
	SpanName      string
	AnomalyType   string
	Minute        time.Time
	CurrentValue  float64
	BaselineValue float64
	ZScore        float64
	Severity      string
}

// AnomalyType constants — anomalies 테이블 anomaly_type 컬럼 값과 일치해야 한다.
const (
	TypeLatencySpike   = "latency_p95_spike"
	TypeErrorRateSpike = "error_rate_spike"
	TypeTrafficDrop    = "traffic_drop"
	TypeMultivariate   = "multivariate_anomaly"
)

// Severity constants
const (
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)
