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

// SpanQuery는 스팬 조회 필터다.
// 빈 값(0 또는 빈 문자열)인 필드는 필터링에서 제외된다.
type SpanQuery struct {
	Limit         int    // 최대 행 수 (기본 100)
	Offset        int    // 건너뛸 행 수 (커서 페이지네이션용, 0: 처음부터)
	FromMs        int64  // 시작 시간 Unix ms (0: 필터 없음)
	ToMs          int64  // 종료 시간 Unix ms (0: 필터 없음)
	ServiceName   string // 서비스명
	TraceID       string // 트레이스 ID
	StatusCode    int32  // -1: 필터 없음, 0=UNSET, 1=OK, 2=ERROR
	MinDurationMs int64  // 최소 duration ms (0: 필터 없음)
}

// MetricQuery는 메트릭 조회 필터다.
type MetricQuery struct {
	Limit       int
	Offset      int // 건너뛸 행 수 (페이지네이션용)
	FromMs      int64
	ToMs        int64
	ServiceName string
	Name        string // 메트릭 이름
}

// LogQuery는 로그 조회 필터다.
type LogQuery struct {
	Limit        int
	Offset       int // 건너뛸 행 수 (페이지네이션용)
	FromMs       int64
	ToMs         int64
	ServiceName  string
	SeverityText string // 심각도 (ERROR, WARN, INFO 등)
	TraceID      string
}

// TraceStore는 span 데이터 저장소 인터페이스다.
type TraceStore interface {
	// AppendSpans는 spans를 저장소에 기록한다.
	// 구현체는 배치 처리를 담당하며, 호출자는 슬라이스 소유권을 넘긴다.
	AppendSpans(ctx context.Context, spans []*model.SpanData) error

	// QuerySpans는 필터에 맞는 span을 반환한다.
	QuerySpans(ctx context.Context, q SpanQuery) ([]*model.SpanData, error)

	// Close는 저장소 자원을 해제한다.
	Close() error
}

// MetricStore는 metric 데이터 저장소 인터페이스다.
type MetricStore interface {
	AppendMetrics(ctx context.Context, metrics []*model.MetricData) error
	QueryMetrics(ctx context.Context, q MetricQuery) ([]*model.MetricData, error)
	Close() error
}

// LogStore는 log 데이터 저장소 인터페이스다.
type LogStore interface {
	AppendLogs(ctx context.Context, logs []*model.LogData) error
	QueryLogs(ctx context.Context, q LogQuery) ([]*model.LogData, error)
	Close() error
}
