package decoder

import (
	"encoding/hex"
	"time"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/kkc/javi-collector/internal/model"
)

// DecodeLogs parses OTLP ExportLogsServiceRequest protobuf bytes into LogData slice.
func DecodeLogs(b []byte) ([]*model.LogData, error) {
	req := &collectorlogsv1.ExportLogsServiceRequest{}
	if err := proto.Unmarshal(b, req); err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()
	var logs []*model.LogData

	for _, rl := range req.ResourceLogs {
		serviceName := ""
		if rl.Resource != nil {
			serviceName = extractServiceName(rl.Resource.Attributes)
		}
		for _, sl := range rl.ScopeLogs {
			scopeName := ""
			if sl.Scope != nil {
				scopeName = sl.Scope.Name
			}
			for _, lr := range sl.LogRecords {
				logs = append(logs, &model.LogData{
					TimestampNanos: int64(lr.TimeUnixNano),
					SeverityNumber: int32(lr.SeverityNumber),
					SeverityText:   lr.SeverityText,
					Body:           anyValueToString(lr.Body),
					Attributes:     convertAttrs(lr.Attributes),
					TraceID:        hex.EncodeToString(lr.TraceId),
					SpanID:         hex.EncodeToString(lr.SpanId),
					ServiceName:    serviceName,
					ScopeName:      scopeName,
					ReceivedAtMs:   now,
				})
			}
		}
	}
	return logs, nil
}

// DecodeLogsJSON parses OTLP ExportLogsServiceRequest JSON bytes into LogData slice.
func DecodeLogsJSON(b []byte) ([]*model.LogData, error) {
	req := &collectorlogsv1.ExportLogsServiceRequest{}
	if err := protojson.Unmarshal(b, req); err != nil {
		return nil, err
	}
	pb, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return DecodeLogs(pb)
}
