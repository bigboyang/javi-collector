// Package store - profiling.go
//
// ProfilingStore는 profiling_snapshots 테이블에 대한 쓰기/조회를 제공한다.
//
// GAP-07: Continuous Profiling
// Java Agent (async-profiler 기반)가 CPU/Heap Flame Graph 스냅샷을
// POST /api/collector/profiling 으로 전송하면 여기에 기록된다.
// Grafana Pyroscope / Datadog Continuous Profiler 동일 방식.
//
// 데이터 구조:
//   - profile_type: "cpu" | "heap" | "wall" | "alloc"
//   - format: "jfr" | "collapsed" | "pprof"
//   - payload: 실제 스냅샷 바이너리/텍스트 (압축 후 저장)
//   - k8s_pod / k8s_node / host: Infra Metrics Correlation 조인 키
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ProfilingSnapshot은 단일 프로파일링 스냅샷을 나타낸다.
type ProfilingSnapshot struct {
	ID          string    `json:"id"`
	ServiceName string    `json:"service_name"`
	ProfileType string    `json:"profile_type"` // "cpu" | "heap" | "wall" | "alloc"
	Format      string    `json:"format"`       // "jfr" | "collapsed" | "pprof"
	Payload     string    `json:"payload"`      // Base64 인코딩 또는 텍스트 (collapsed)
	Host        string    `json:"host"`
	K8sPod      string    `json:"k8s_pod"`
	K8sNode     string    `json:"k8s_node"`
	K8sNamespace string   `json:"k8s_namespace"`
	DurationMs  int64     `json:"duration_ms"`
	SampledAt   time.Time `json:"sampled_at"`
}

// ProfilingStore는 profiling_snapshots 테이블 CRUD를 담당한다.
type ProfilingStore struct {
	conn driver.Conn
	db   string
}

// NewProfilingStore는 ProfilingStore를 생성하고 테이블 DDL을 적용한다.
func NewProfilingStore(conn driver.Conn, db string) (*ProfilingStore, error) {
	s := &ProfilingStore{conn: conn, db: db}
	if err := s.ensureTable(); err != nil {
		return nil, fmt.Errorf("profiling_snapshots DDL: %w", err)
	}
	return s, nil
}

func (s *ProfilingStore) ensureTable() error {
	return s.conn.Exec(context.Background(), fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.profiling_snapshots (
    id            String,
    service_name  LowCardinality(String),
    profile_type  LowCardinality(String)  DEFAULT 'cpu',
    format        LowCardinality(String)  DEFAULT 'collapsed',
    payload       String                  DEFAULT '',
    host          LowCardinality(String)  DEFAULT '',
    k8s_pod       LowCardinality(String)  DEFAULT '',
    k8s_node      LowCardinality(String)  DEFAULT '',
    k8s_namespace LowCardinality(String)  DEFAULT '',
    duration_ms   Int64                   DEFAULT 0,
    sampled_at    DateTime64(3)           DEFAULT now64(),
    dt            Date                    DEFAULT toDate(sampled_at)
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, profile_type, sampled_at)
TTL dt + INTERVAL 30 DAY;
`, s.db))
}

// InsertSnapshot은 프로파일링 스냅샷을 기록한다.
func (s *ProfilingStore) InsertSnapshot(ctx context.Context, snap ProfilingSnapshot) error {
	if snap.SampledAt.IsZero() {
		snap.SampledAt = time.Now()
	}
	if snap.ProfileType == "" {
		snap.ProfileType = "cpu"
	}
	if snap.Format == "" {
		snap.Format = "collapsed"
	}
	return s.conn.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.profiling_snapshots
    (id, service_name, profile_type, format, payload,
     host, k8s_pod, k8s_node, k8s_namespace, duration_ms, sampled_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.db,
	), snap.ID, snap.ServiceName, snap.ProfileType, snap.Format, snap.Payload,
		snap.Host, snap.K8sPod, snap.K8sNode, snap.K8sNamespace, snap.DurationMs, snap.SampledAt)
}

// QuerySnapshotsParams는 프로파일링 스냅샷 조회 파라미터다.
type QuerySnapshotsParams struct {
	ServiceName string
	ProfileType string // 빈 문자열이면 모든 타입
	FromMs      int64
	ToMs        int64
	Limit       int
}

// QuerySnapshots는 서비스+타입+시간 범위로 스냅샷 목록을 반환한다.
// Payload는 대용량이므로 목록 조회에서는 제외하고 ID로 개별 조회하도록 설계했다.
func (s *ProfilingStore) QuerySnapshots(ctx context.Context, p QuerySnapshotsParams) ([]ProfilingSnapshot, error) {
	if p.Limit <= 0 || p.Limit > 200 {
		p.Limit = 50
	}

	where := fmt.Sprintf("service_name = '%s'", p.ServiceName)
	if p.ProfileType != "" {
		where += fmt.Sprintf(" AND profile_type = '%s'", p.ProfileType)
	}
	if p.FromMs > 0 {
		where += fmt.Sprintf(" AND sampled_at >= fromUnixTimestamp64Milli(%d)", p.FromMs)
	}
	if p.ToMs > 0 {
		where += fmt.Sprintf(" AND sampled_at <= fromUnixTimestamp64Milli(%d)", p.ToMs)
	}

	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT id, service_name, profile_type, format,
       host, k8s_pod, k8s_node, k8s_namespace, duration_ms, sampled_at
FROM %s.profiling_snapshots
WHERE %s
ORDER BY sampled_at DESC
LIMIT %d`, s.db, where, p.Limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ProfilingSnapshot
	for rows.Next() {
		var snap ProfilingSnapshot
		if err := rows.Scan(
			&snap.ID, &snap.ServiceName, &snap.ProfileType, &snap.Format,
			&snap.Host, &snap.K8sPod, &snap.K8sNode, &snap.K8sNamespace,
			&snap.DurationMs, &snap.SampledAt,
		); err != nil {
			return nil, err
		}
		result = append(result, snap)
	}
	return result, rows.Err()
}

// GetSnapshotPayload는 특정 스냅샷의 payload를 반환한다.
// Flame Graph 렌더링 시 호출한다.
func (s *ProfilingStore) GetSnapshotPayload(ctx context.Context, id string) (*ProfilingSnapshot, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT id, service_name, profile_type, format, payload,
       host, k8s_pod, k8s_node, k8s_namespace, duration_ms, sampled_at
FROM %s.profiling_snapshots
WHERE id = ?
LIMIT 1`, s.db), id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}
	var snap ProfilingSnapshot
	if err := rows.Scan(
		&snap.ID, &snap.ServiceName, &snap.ProfileType, &snap.Format, &snap.Payload,
		&snap.Host, &snap.K8sPod, &snap.K8sNode, &snap.K8sNamespace,
		&snap.DurationMs, &snap.SampledAt,
	); err != nil {
		return nil, err
	}
	return &snap, rows.Err()
}

// QueryProfileSummary는 서비스별 최근 프로파일링 활동 요약을 반환한다.
// 대시보드의 "Profiling Coverage" 위젯에서 사용한다.
func (s *ProfilingStore) QueryProfileSummary(ctx context.Context, fromMs, toMs int64) ([]map[string]any, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT
    service_name,
    profile_type,
    count()         AS snapshot_count,
    max(sampled_at) AS last_seen,
    avg(duration_ms) AS avg_duration_ms
FROM %s.profiling_snapshots
WHERE sampled_at >= fromUnixTimestamp64Milli(?) AND sampled_at <= fromUnixTimestamp64Milli(?)
GROUP BY service_name, profile_type
ORDER BY service_name, profile_type`, s.db), fromMs, toMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			svcName     string
			profType    string
			snapCount   uint64
			lastSeen    time.Time
			avgDuration float64
		)
		if err := rows.Scan(&svcName, &profType, &snapCount, &lastSeen, &avgDuration); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"service_name":    svcName,
			"profile_type":    profType,
			"snapshot_count":  snapCount,
			"last_seen":       lastSeen,
			"avg_duration_ms": avgDuration,
		})
	}
	return result, rows.Err()
}
