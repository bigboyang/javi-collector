// Package store - k8s_metrics.go
//
// K8sPodMetricsStore는 k8s_pod_metrics 테이블에 대한 쓰기/조회를 제공한다.
//
// GAP-08 확장: Infra Metrics Correlation
// Java Agent가 Pod 내부 cgroup(v1/v2) 파일을 읽어 수집한 Pod 수준 리소스 메트릭
// (CPU 사용량·한도, 메모리 사용량·한도·RSS)을 POST /api/collector/k8s-metrics 로 전송하면
// 여기에 기록된다.
//
// 데이터 구조:
//   - cpu_usage_millicore:  현재 CPU 사용량 (millicores, 1 core = 1000m)
//   - cpu_limit_millicore:  cgroup CPU 한도 (0 = unlimited)
//   - memory_usage_bytes:   컨테이너 메모리 사용량 (bytes)
//   - memory_limit_bytes:   cgroup 메모리 한도 (0 = unlimited)
//   - memory_rss_bytes:     실제 RSS (페이지 캐시 제외)
//
// 보존 정책: 7일 (고빈도 시계열 데이터로 단기 보존)
package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// K8sPodMetric은 단일 Pod 리소스 메트릭 스냅샷을 나타낸다.
type K8sPodMetric struct {
	ServiceName        string    `json:"service_name"`
	PodName            string    `json:"pod_name"`
	NodeName           string    `json:"node_name"`
	Namespace          string    `json:"namespace"`
	Host               string    `json:"host"`
	ContainerID        string    `json:"container_id"`
	CPUUsageMillicore  float64   `json:"cpu_usage_millicore"`
	CPULimitMillicore  float64   `json:"cpu_limit_millicore"`
	MemoryUsageBytes   int64     `json:"memory_usage_bytes"`
	MemoryLimitBytes   int64     `json:"memory_limit_bytes"`
	MemoryRSSBytes     int64     `json:"memory_rss_bytes"`
	Timestamp          time.Time `json:"timestamp"`
}

// K8sPodMetricsStore는 k8s_pod_metrics 테이블 CRUD를 담당한다.
type K8sPodMetricsStore struct {
	conn driver.Conn
	db   string
}

// NewK8sPodMetricsStore는 K8sPodMetricsStore를 생성하고 테이블 DDL을 적용한다.
func NewK8sPodMetricsStore(conn driver.Conn, db string) (*K8sPodMetricsStore, error) {
	s := &K8sPodMetricsStore{conn: conn, db: db}
	if err := s.ensureTable(); err != nil {
		return nil, fmt.Errorf("k8s_pod_metrics DDL: %w", err)
	}
	return s, nil
}

func (s *K8sPodMetricsStore) ensureTable() error {
	return s.conn.Exec(context.Background(), fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.k8s_pod_metrics (
    service_name          LowCardinality(String),
    pod_name              LowCardinality(String)  DEFAULT '',
    node_name             LowCardinality(String)  DEFAULT '',
    namespace             LowCardinality(String)  DEFAULT '',
    host                  LowCardinality(String)  DEFAULT '',
    container_id          String                  DEFAULT '',
    cpu_usage_millicore   Float64                 DEFAULT 0,
    cpu_limit_millicore   Float64                 DEFAULT 0,
    memory_usage_bytes    Int64                   DEFAULT 0,
    memory_limit_bytes    Int64                   DEFAULT 0,
    memory_rss_bytes      Int64                   DEFAULT 0,
    timestamp             DateTime64(3)           DEFAULT now64(),
    dt                    Date                    DEFAULT toDate(timestamp)
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, pod_name, timestamp)
TTL dt + INTERVAL 7 DAY;
`, s.db))
}

// InsertMetric은 Pod 리소스 메트릭 1건을 기록한다.
func (s *K8sPodMetricsStore) InsertMetric(ctx context.Context, m K8sPodMetric) error {
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}
	return s.conn.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.k8s_pod_metrics
    (service_name, pod_name, node_name, namespace, host, container_id,
     cpu_usage_millicore, cpu_limit_millicore,
     memory_usage_bytes, memory_limit_bytes, memory_rss_bytes, timestamp)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.db,
	), m.ServiceName, m.PodName, m.NodeName, m.Namespace, m.Host, m.ContainerID,
		m.CPUUsageMillicore, m.CPULimitMillicore,
		m.MemoryUsageBytes, m.MemoryLimitBytes, m.MemoryRSSBytes, m.Timestamp)
}

// QueryK8sMetricsParams는 Pod 메트릭 조회 파라미터다.
type QueryK8sMetricsParams struct {
	ServiceName string
	PodName     string // 빈 문자열이면 서비스 전체 Pod
	FromMs      int64
	ToMs        int64
	Limit       int
}

// QueryMetrics는 서비스+시간 범위로 Pod 메트릭 시계열을 반환한다.
func (s *K8sPodMetricsStore) QueryMetrics(ctx context.Context, p QueryK8sMetricsParams) ([]K8sPodMetric, error) {
	if p.Limit <= 0 || p.Limit > 1000 {
		p.Limit = 200
	}

	var conditions []string
	var args []any

	conditions = append(conditions, "service_name = ?")
	args = append(args, p.ServiceName)

	if p.PodName != "" {
		conditions = append(conditions, "pod_name = ?")
		args = append(args, p.PodName)
	}
	if p.FromMs > 0 {
		conditions = append(conditions, fmt.Sprintf("timestamp >= fromUnixTimestamp64Milli(%d)", p.FromMs))
	}
	if p.ToMs > 0 {
		conditions = append(conditions, fmt.Sprintf("timestamp <= fromUnixTimestamp64Milli(%d)", p.ToMs))
	}

	query := fmt.Sprintf(`
SELECT service_name, pod_name, node_name, namespace, host, container_id,
       cpu_usage_millicore, cpu_limit_millicore,
       memory_usage_bytes, memory_limit_bytes, memory_rss_bytes, timestamp
FROM %s.k8s_pod_metrics
WHERE %s
ORDER BY timestamp DESC
LIMIT %d`, s.db, strings.Join(conditions, " AND "), p.Limit)

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []K8sPodMetric
	for rows.Next() {
		var m K8sPodMetric
		if err := rows.Scan(
			&m.ServiceName, &m.PodName, &m.NodeName, &m.Namespace, &m.Host, &m.ContainerID,
			&m.CPUUsageMillicore, &m.CPULimitMillicore,
			&m.MemoryUsageBytes, &m.MemoryLimitBytes, &m.MemoryRSSBytes, &m.Timestamp,
		); err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// QueryPodSummary는 서비스별 최근 Pod 리소스 사용량 요약을 반환한다.
// 대시보드의 "Infrastructure" 위젯에서 사용한다.
func (s *K8sPodMetricsStore) QueryPodSummary(ctx context.Context, service string, fromMs, toMs int64) ([]map[string]any, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT
    pod_name,
    node_name,
    namespace,
    avg(cpu_usage_millicore)   AS avg_cpu_m,
    max(cpu_usage_millicore)   AS max_cpu_m,
    any(cpu_limit_millicore)   AS cpu_limit_m,
    avg(memory_usage_bytes)    AS avg_mem_bytes,
    max(memory_usage_bytes)    AS max_mem_bytes,
    any(memory_limit_bytes)    AS mem_limit_bytes,
    max(timestamp)             AS last_seen
FROM %s.k8s_pod_metrics
WHERE service_name = ?
  AND timestamp >= fromUnixTimestamp64Milli(?)
  AND timestamp <= fromUnixTimestamp64Milli(?)
GROUP BY pod_name, node_name, namespace
ORDER BY pod_name`, s.db), service, fromMs, toMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		var (
			podName      string
			nodeName     string
			namespace    string
			avgCPU       float64
			maxCPU       float64
			limitCPU     float64
			avgMem       float64
			maxMem       int64
			limitMem     int64
			lastSeen     time.Time
		)
		if err := rows.Scan(&podName, &nodeName, &namespace,
			&avgCPU, &maxCPU, &limitCPU,
			&avgMem, &maxMem, &limitMem,
			&lastSeen); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"pod_name":       podName,
			"node_name":      nodeName,
			"namespace":      namespace,
			"avg_cpu_m":      avgCPU,
			"max_cpu_m":      maxCPU,
			"cpu_limit_m":    limitCPU,
			"avg_mem_bytes":  avgMem,
			"max_mem_bytes":  maxMem,
			"mem_limit_bytes": limitMem,
			"last_seen":      lastSeen,
		})
	}
	return result, rows.Err()
}
