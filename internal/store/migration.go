// Package store - migration.go
//
// Migrator는 ClickHouse 스키마 마이그레이션을 버전 기반으로 관리한다.
//
// 동작 방식:
//  1. schema_migrations 테이블이 없으면 자동 생성한다.
//  2. 이미 적용된 마이그레이션(version)은 건너뛴다.
//  3. 미적용 마이그레이션을 version 순으로 실행한다.
//  4. 실행 성공 시 schema_migrations에 기록한다.
//
// 기존 ensureXxxTable 함수는 "version 0" (CREATE TABLE IF NOT EXISTS)에 해당하므로
// Migrator는 그 이후의 변경 사항을 관리한다.
//
// 마이그레이션 등록 예시:
//
//	var SpansMigrations = []store.Migration{
//	    {Version: 1, Description: "add trace_state column", SQL: "ALTER TABLE ..."},
//	}
//
//	migrator := store.NewMigrator(conn, "apm")
//	if err := migrator.Run(ctx, store.SpansMigrations); err != nil {
//	    log.Fatal(err)
//	}
package store

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Migration은 단일 스키마 마이그레이션을 나타낸다.
type Migration struct {
	// Version은 단조 증가하는 양의 정수다. 중복 불가.
	Version int
	// Description은 마이그레이션 목적을 설명하는 짧은 문자열이다.
	Description string
	// SQL은 실행할 DDL 문장이다. 단일 문장만 지원한다.
	SQL string
}

// Migrator는 schema_migrations 테이블을 통해 마이그레이션 상태를 추적한다.
type Migrator struct {
	conn driver.Conn
	db   string
}

// NewMigrator는 Migrator를 생성한다.
func NewMigrator(conn driver.Conn, db string) *Migrator {
	return &Migrator{conn: conn, db: db}
}

// Run은 migrations 슬라이스에서 미적용 마이그레이션을 순서대로 실행한다.
// 이미 적용된 버전은 건너뛴다. 실패하면 즉시 에러를 반환한다.
func (m *Migrator) Run(ctx context.Context, migrations []Migration) error {
	if err := m.ensureMigrationsTable(ctx); err != nil {
		return fmt.Errorf("migration: ensure table: %w", err)
	}

	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return fmt.Errorf("migration: list applied: %w", err)
	}

	for _, mg := range migrations {
		if applied[mg.Version] {
			slog.Debug("migration already applied, skipping",
				"version", mg.Version,
				"description", mg.Description,
			)
			continue
		}

		slog.Info("applying migration",
			"version", mg.Version,
			"description", mg.Description,
		)

		if err := m.conn.Exec(ctx, mg.SQL); err != nil {
			return fmt.Errorf("migration v%d (%s): %w", mg.Version, mg.Description, err)
		}

		if err := m.recordApplied(ctx, mg); err != nil {
			// SQL은 실행됐지만 기록 실패 — 경고 후 계속 (다음 시작 시 재실행 가능성 있으나 SQL이 idempotent여야 함)
			slog.Warn("migration applied but record failed",
				"version", mg.Version,
				"err", err,
			)
		}

		slog.Info("migration applied",
			"version", mg.Version,
			"description", mg.Description,
		)
	}
	return nil
}

// AppliedVersions는 현재 적용된 마이그레이션 버전 목록을 반환한다.
func (m *Migrator) AppliedVersions(ctx context.Context) ([]int, error) {
	applied, err := m.appliedVersions(ctx)
	if err != nil {
		return nil, err
	}
	versions := make([]int, 0, len(applied))
	for v := range applied {
		versions = append(versions, v)
	}
	return versions, nil
}

// ensureMigrationsTable은 schema_migrations 추적 테이블을 생성한다.
func (m *Migrator) ensureMigrationsTable(ctx context.Context) error {
	return m.conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.schema_migrations (
    version      Int32,
    description  String,
    applied_at   DateTime DEFAULT now(),
    dt           Date DEFAULT today()
) ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version;
`, m.db))
}

// appliedVersions는 schema_migrations에서 적용된 버전 set을 반환한다.
func (m *Migrator) appliedVersions(ctx context.Context) (map[int]bool, error) {
	rows, err := m.conn.Query(ctx,
		fmt.Sprintf(`SELECT version FROM %s.schema_migrations FINAL`, m.db))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int32
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[int(v)] = true
	}
	return applied, rows.Err()
}

// recordApplied는 마이그레이션 적용 기록을 schema_migrations에 삽입한다.
func (m *Migrator) recordApplied(ctx context.Context, mg Migration) error {
	batch, err := m.conn.PrepareBatch(ctx,
		fmt.Sprintf(`INSERT INTO %s.schema_migrations (version, description, applied_at) VALUES`, m.db))
	if err != nil {
		return err
	}
	if err := batch.Append(int32(mg.Version), mg.Description, time.Now()); err != nil {
		return err
	}
	return batch.Send()
}

// ---- 등록된 마이그레이션 ----

// SpansMigrations는 spans 테이블에 대한 버전 관리 마이그레이션 목록이다.
// 새 컬럼/인덱스 추가 시 이 슬라이스에 항목을 추가한다.
// SQL은 반드시 멱등적(idempotent)이어야 한다 (IF NOT EXISTS / IF EXISTS 사용).
var SpansMigrations = []Migration{
	{
		Version:     1,
		Description: "add trace_state column for W3C Trace Context propagation",
		SQL:         `ALTER TABLE %SPANS_TABLE% ADD COLUMN IF NOT EXISTS trace_state String DEFAULT ''`,
	},
	{
		Version:     2,
		Description: "add span_links column for cross-trace link metadata",
		SQL:         `ALTER TABLE %SPANS_TABLE% ADD COLUMN IF NOT EXISTS span_links String DEFAULT ''`,
	},
}

// MetricsMigrations는 metrics 테이블에 대한 버전 관리 마이그레이션 목록이다.
var MetricsMigrations = []Migration{}

// LogsMigrations는 logs 테이블에 대한 버전 관리 마이그레이션 목록이다.
var LogsMigrations = []Migration{}

// BuildSpansMigrations는 데이터베이스 이름을 치환한 SpansMigrations를 반환한다.
func BuildSpansMigrations(db string) []Migration {
	return buildMigrations(SpansMigrations, db+".spans")
}

// buildMigrations는 마이그레이션 SQL에서 %TABLE% 플레이스홀더를 table로 치환한다.
func buildMigrations(migrations []Migration, table string) []Migration {
	result := make([]Migration, len(migrations))
	for i, m := range migrations {
		result[i] = Migration{
			Version:     m.Version,
			Description: m.Description,
			SQL:         strings.ReplaceAll(m.SQL, "%SPANS_TABLE%", table),
		}
	}
	return result
}
