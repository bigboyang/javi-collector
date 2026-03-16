// Package store는 APM 데이터 저장소 인터페이스와 구현체를 제공한다.
//
// 저장소 계층 구조:
//   - Store: 공통 인터페이스 (Append + Query)
//   - ClickHouseStore: 영구 저장소 (배치 insert)
//   - MemoryStore: 인메모리 링버퍼 (fallback / 개발용)
//
// 파이프라인:
//
//	gRPC/HTTP handler → ingester channel → BatchWriter → Store.Append
package store

import (
	"context"

	"github.com/kkc/javi-collector/internal/model"
)

// TraceStore는 span 데이터 저장소 인터페이스다.
type TraceStore interface {
	// AppendSpans는 spans를 저장소에 기록한다.
	// 구현체는 배치 처리를 담당하며, 호출자는 슬라이스 소유권을 넘긴다.
	AppendSpans(ctx context.Context, spans []*model.SpanData) error

	// QuerySpans는 최근 limit개의 span을 반환한다.
	QuerySpans(ctx context.Context, limit int) ([]*model.SpanData, error)

	// Close는 저장소 자원을 해제한다.
	Close() error
}

// MetricStore는 metric 데이터 저장소 인터페이스다.
type MetricStore interface {
	AppendMetrics(ctx context.Context, metrics []*model.MetricData) error
	QueryMetrics(ctx context.Context, limit int) ([]*model.MetricData, error)
	Close() error
}

// LogStore는 log 데이터 저장소 인터페이스다.
type LogStore interface {
	AppendLogs(ctx context.Context, logs []*model.LogData) error
	QueryLogs(ctx context.Context, limit int) ([]*model.LogData, error)
	Close() error
}
