// Package store - service_catalog.go
//
// ServiceCatalogStore는 service_catalog 테이블에 대한 CRUD를 제공한다.
//
// 서비스 카탈로그는 자동 발견된 서비스(spans의 service_name)에
// 팀 소유권, 연락처, 운영 메타데이터를 덧붙인다.
// 이를 통해 알람 발생 시 "이 서비스의 담당팀이 누구인가?"를 즉시 파악할 수 있다.
//
// ClickHouse ReplacingMergeTree(updated_at)를 사용하므로
// 같은 service_name에 대해 INSERT하면 updated_at이 가장 큰 행이 최종 상태가 된다.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ServiceCatalogEntry는 서비스 카탈로그의 단일 항목이다.
type ServiceCatalogEntry struct {
	ServiceName    string    `json:"service_name"`
	Team           string    `json:"team"`
	SlackChannel   string    `json:"slack_channel"`
	RunbookURL     string    `json:"runbook_url"`
	Tier           string    `json:"tier"` // "critical" | "standard" | "low"
	OnCallRotation string    `json:"on_call_rotation"`
	Description    string    `json:"description"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// ServiceCatalogStore는 service_catalog 테이블 CRUD를 담당한다.
type ServiceCatalogStore struct {
	conn driver.Conn
	db   string
}

// NewServiceCatalogStore는 ServiceCatalogStore를 생성하고 테이블 DDL을 적용한다.
func NewServiceCatalogStore(conn driver.Conn, db string) (*ServiceCatalogStore, error) {
	s := &ServiceCatalogStore{conn: conn, db: db}
	if err := s.ensureTable(); err != nil {
		return nil, fmt.Errorf("service_catalog DDL: %w", err)
	}
	return s, nil
}

func (s *ServiceCatalogStore) ensureTable() error {
	return s.conn.Exec(context.Background(), fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.service_catalog (
    service_name      String,
    team              String                 DEFAULT '',
    slack_channel     String                 DEFAULT '',
    runbook_url       String                 DEFAULT '',
    tier              LowCardinality(String) DEFAULT 'standard',
    on_call_rotation  String                 DEFAULT '',
    description       String                 DEFAULT '',
    updated_at        DateTime               DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY service_name;
`, s.db))
}

// UpsertService는 서비스 카탈로그 항목을 삽입하거나 업데이트한다.
func (s *ServiceCatalogStore) UpsertService(ctx context.Context, e ServiceCatalogEntry) error {
	if e.Tier == "" {
		e.Tier = "standard"
	}
	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(
		`INSERT INTO %s.service_catalog
		 (service_name, team, slack_channel, runbook_url, tier, on_call_rotation, description, updated_at)
		 VALUES`, s.db))
	if err != nil {
		return err
	}
	if err := batch.Append(
		e.ServiceName, e.Team, e.SlackChannel, e.RunbookURL,
		e.Tier, e.OnCallRotation, e.Description, time.Now(),
	); err != nil {
		return err
	}
	return batch.Send()
}

// GetService는 service_name으로 카탈로그 항목을 반환한다. 없으면 nil, nil을 반환한다.
func (s *ServiceCatalogStore) GetService(ctx context.Context, name string) (*ServiceCatalogEntry, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT service_name, team, slack_channel, runbook_url, tier, on_call_rotation, description, updated_at
FROM %s.service_catalog FINAL
WHERE service_name = ?
LIMIT 1`, s.db), name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var e ServiceCatalogEntry
	if err := rows.Scan(&e.ServiceName, &e.Team, &e.SlackChannel, &e.RunbookURL,
		&e.Tier, &e.OnCallRotation, &e.Description, &e.UpdatedAt); err != nil {
		return nil, err
	}
	return &e, rows.Err()
}

// ListServices는 등록된 모든 서비스를 반환한다.
func (s *ServiceCatalogStore) ListServices(ctx context.Context) ([]ServiceCatalogEntry, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT service_name, team, slack_channel, runbook_url, tier, on_call_rotation, description, updated_at
FROM %s.service_catalog FINAL
ORDER BY service_name`, s.db))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ServiceCatalogEntry
	for rows.Next() {
		var e ServiceCatalogEntry
		if err := rows.Scan(&e.ServiceName, &e.Team, &e.SlackChannel, &e.RunbookURL,
			&e.Tier, &e.OnCallRotation, &e.Description, &e.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}
