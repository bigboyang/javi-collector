package decoder

import (
	"encoding/hex"
	"time"

	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/kkc/javi-collector/internal/model"
)

// DecodeMetrics parses OTLP ExportMetricsServiceRequest protobuf bytes into MetricData slice.
func DecodeMetrics(b []byte) ([]*model.MetricData, error) {
	req := &collectormetricsv1.ExportMetricsServiceRequest{}
	if err := proto.Unmarshal(b, req); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()
	var metrics []*model.MetricData

	for _, rm := range req.ResourceMetrics {
		serviceName := ""
		if rm.Resource != nil {
			serviceName = extractServiceName(rm.Resource.Attributes)
		}
		for _, sm := range rm.ScopeMetrics {
			scopeName := ""
			if sm.Scope != nil {
				scopeName = sm.Scope.Name
			}
			for _, m := range sm.Metrics {
				md := convertMetric(m, serviceName, scopeName, now)
				metrics = append(metrics, md)
			}
		}
	}
	return metrics, nil
}

func convertMetric(m *metricsv1.Metric, serviceName, scopeName string, now int64) *model.MetricData {
	md := &model.MetricData{
		Name:         m.Name,
		Description:  m.Description,
		Unit:         m.Unit,
		ServiceName:  serviceName,
		ScopeName:    scopeName,
		ReceivedAtMs: now,
	}

	switch d := m.Data.(type) {
	case *metricsv1.Metric_Gauge:
		md.Type = model.MetricTypeGauge
		md.DataPoints = convertNumberDataPoints(d.Gauge.DataPoints)
	case *metricsv1.Metric_Sum:
		md.Type = model.MetricTypeSum
		md.DataPoints = convertNumberDataPoints(d.Sum.DataPoints)
	case *metricsv1.Metric_Histogram:
		md.Type = model.MetricTypeHistogram
		md.DataPoints = convertHistogramDataPoints(d.Histogram.DataPoints)
	default:
		md.Type = model.MetricTypeUnspecified
	}
	return md
}

func convertNumberDataPoints(dps []*metricsv1.NumberDataPoint) []model.DataPoint {
	points := make([]model.DataPoint, 0, len(dps))
	for _, dp := range dps {
		pt := model.DataPoint{
			Attributes:     convertAttrs(dp.Attributes),
			StartTimeNanos: int64(dp.StartTimeUnixNano),
			TimeNanos:      int64(dp.TimeUnixNano),
			Exemplars:      convertExemplars(dp.Exemplars),
		}
		switch v := dp.Value.(type) {
		case *metricsv1.NumberDataPoint_AsDouble:
			pt.Value = v.AsDouble
		case *metricsv1.NumberDataPoint_AsInt:
			pt.Value = float64(v.AsInt)
		}
		points = append(points, pt)
	}
	return points
}

func convertHistogramDataPoints(dps []*metricsv1.HistogramDataPoint) []model.DataPoint {
	points := make([]model.DataPoint, 0, len(dps))
	for _, dp := range dps {
		// BucketCounts: P95/P99 계산에 필수. 기존 코드에서 유실되던 데이터.
		// OTLP spec: bucket_counts[i] = 해당 bucket의 누적 카운트 (not cumulative)
		buckets := make([]uint64, len(dp.BucketCounts))
		for i, c := range dp.BucketCounts {
			buckets[i] = c
		}
		points = append(points, model.DataPoint{
			Attributes:     convertAttrs(dp.Attributes),
			StartTimeNanos: int64(dp.StartTimeUnixNano),
			TimeNanos:      int64(dp.TimeUnixNano),
			Count:          int64(dp.Count),
			Sum:            dp.GetSum(),
			BucketCounts:   buckets,
			ExplicitBounds: dp.ExplicitBounds,
			Exemplars:      convertExemplars(dp.Exemplars),
		})
	}
	return points
}

func convertExemplars(exs []*metricsv1.Exemplar) []model.Exemplar {
	if len(exs) == 0 {
		return nil
	}
	res := make([]model.Exemplar, 0, len(exs))
	for _, ex := range exs {
		e := model.Exemplar{
			TraceID:    hex.EncodeToString(ex.TraceId),
			SpanID:     hex.EncodeToString(ex.SpanId),
			TimeNanos:  int64(ex.TimeUnixNano),
			Attributes: convertAttrs(ex.FilteredAttributes),
		}
		switch v := ex.Value.(type) {
		case *metricsv1.Exemplar_AsDouble:
			e.Value = v.AsDouble
		case *metricsv1.Exemplar_AsInt:
			e.Value = float64(v.AsInt)
		}
		res = append(res, e)
	}
	return res
}

// DecodeMetricsJSON parses OTLP ExportMetricsServiceRequest JSON bytes into MetricData slice.
func DecodeMetricsJSON(b []byte) ([]*model.MetricData, error) {
	req := &collectormetricsv1.ExportMetricsServiceRequest{}
	if err := protojson.Unmarshal(b, req); err != nil {
		return nil, err
	}
	return DecodeMetricsFromRequest(req), nil
}
