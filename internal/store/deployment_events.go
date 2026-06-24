// Package store - deployment_events.go
//
// DeploymentEventStore는 deployment_events 테이블에 대한 쓰기/조회를 제공한다.
//
// GAP-04: Deployment Event Correlation
// CI/CD 파이프라인이 POST /api/events/deployment을 호출하면 배포 이벤트가 기록되고,
// RCA Engine이 이상 발생 시간 ±5분 이내 배포 이벤트를 조회해 가설에 포함시킨다.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DeploymentEvent는 단일 배포 이벤트를 나타낸다.
type DeploymentEvent struct {
	ID          string    `json:"id"`
	ServiceName string    `json:"service_name"`
	Version     string    `json:"version"`
	Environment string    `json:"environment"` // "production" | "staging" | "dev"
	DeployedBy  string    `json:"deployed_by"`
	Description string    `json:"description"`
	DeployedAt  time.Time `json:"deployed_at"`
}

// DeploymentEventStore는 deployment_events 테이블 CRUD를 담당한다.
type DeploymentEventStore struct {
	conn driver.Conn
	db   string
}

// NewDeploymentEventStore는 DeploymentEventStore를 생성하고 테이블 DDL을 적용한다.
func NewDeploymentEventStore(conn driver.Conn, db string) (*DeploymentEventStore, error) {
	s := &DeploymentEventStore{conn: conn, db: db}
	if err := s.ensureTable(); err != nil {
		return nil, fmt.Errorf("deployment_events DDL: %w", err)
	}
	return s, nil
}

func (s *DeploymentEventStore) ensureTable() error {
	return s.conn.Exec(context.Background(), fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.deployment_events (
    id           String,
    service_name LowCardinality(String),
    version      String                 DEFAULT '',
    environment  LowCardinality(String) DEFAULT 'production',
    deployed_by  String                 DEFAULT '',
    description  String                 DEFAULT '',
    deployed_at  DateTime               DEFAULT now(),
    dt           Date                   DEFAULT toDate(deployed_at)
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, deployed_at)
TTL dt + INTERVAL 365 DAY;
`, s.db))
}

// InsertEvent는 배포 이벤트를 기록한다.
func (s *DeploymentEventStore) InsertEvent(ctx context.Context, e DeploymentEvent) error {
	if e.DeployedAt.IsZero() {
		e.DeployedAt = time.Now()
	}
	if e.Environment == "" {
		e.Environment = "production"
	}
	return s.conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.deployment_events
		 (id, service_name, version, environment, deployed_by, description, deployed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		s.db,
	), e.ID, e.ServiceName, e.Version, e.Environment, e.DeployedBy, e.Description, e.DeployedAt)
}

// QueryNearby는 service_name 기준으로 t ± window 범위 내 배포 이벤트를 반환한다.
// RCA Engine이 이상 발생 시간대 ±5분 배포 이벤트를 가설 생성에 활용한다.
func (s *DeploymentEventStore) QueryNearby(ctx context.Context, serviceName string, t time.Time, window time.Duration) ([]DeploymentEvent, error) {
	from := t.Add(-window)
	to := t.Add(window)
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT id, service_name, version, environment, deployed_by, description, deployed_at
FROM %s.deployment_events
WHERE service_name = ?
  AND deployed_at >= ? AND deployed_at <= ?
ORDER BY deployed_at DESC
LIMIT 10`, s.db), serviceName, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []DeploymentEvent
	for rows.Next() {
		var e DeploymentEvent
		if err := rows.Scan(&e.ID, &e.ServiceName, &e.Version, &e.Environment,
			&e.DeployedBy, &e.Description, &e.DeployedAt); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}
