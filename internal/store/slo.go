// Package store - slo.go
//
// SLOStore는 SLO(Service Level Objective) 정의 관리와 번-레이트 알람을 제공한다.
//
// 상용 APM의 "SLO/SLI Management" 기능 (Datadog SLO, New Relic SLIs)에 해당한다.
//
// 동작 원리:
//  1. 운영자가 PUT /api/slo/definition 으로 SLO를 등록한다.
//     예: 결제 서비스, 에러율 < 0.1%, 30일 윈도우
//  2. SLOBurnCalculator가 매분 mv_red_1m_state를 읽어 번-레이트를 계산한다.
//     번-레이트 = 실제 불량률 / 에러 예산 (1 - target_pct/100)
//  3. 고속 번 (> 14.4×): 1시간 안에 30일 예산 소진 → "page" 알람
//     저속 번 (> 6×, 6h 지속): 5일 안에 30일 예산 소진 → "ticket" 알람
//  4. 알람은 slo_burn_alerts 테이블에 기록되고 GET /api/slo/burn-alerts 로 조회한다.
package store

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// SLODefinition은 단일 SLO 정의다.
type SLODefinition struct {
	ServiceName string    `json:"service_name"`
	SLOName     string    `json:"slo_name"`
	WindowHours int       `json:"window_hours"` // 평가 윈도우: 720 = 30일
	TargetPct   float64   `json:"target_pct"`   // 목표: 예) 99.9
	MetricType  string    `json:"metric_type"`  // "error_rate" | "latency_p95"
	ThresholdMs float64   `json:"threshold_ms"` // latency_p95 용: 허용 최대 p95(ms)
	UpdatedAt   time.Time `json:"updated_at"`
}

// SLOBurnAlert는 번-레이트 초과 알람 기록이다.
type SLOBurnAlert struct {
	ServiceName string    `json:"service_name"`
	SLOName     string    `json:"slo_name"`
	BurnRate    float64   `json:"burn_rate"`  // 실제 불량률 / 에러 예산
	Window      string    `json:"window"`     // "1h" | "6h"
	Severity    string    `json:"severity"`   // "page" | "ticket"
	AlertedAt   time.Time `json:"alerted_at"`
}

const (
	// fastBurnMultiplier: 1h 윈도우에서 30일 예산을 다 태우는 번-레이트
	fastBurnMultiplier = 14.4
	// slowBurnMultiplier: 6h 윈도우에서 5일 안에 30일 예산을 다 태우는 번-레이트
	slowBurnMultiplier = 6.0
)

// SLOStore는 slo_definitions + slo_burn_alerts 테이블 CRUD를 담당한다.
type SLOStore struct {
	conn driver.Conn
	db   string
}

// NewSLOStore는 SLOStore를 생성하고 테이블 DDL을 적용한다.
func NewSLOStore(conn driver.Conn, db string) (*SLOStore, error) {
	s := &SLOStore{conn: conn, db: db}
	if err := s.ensureTables(); err != nil {
		return nil, fmt.Errorf("slo DDL: %w", err)
	}
	return s, nil
}

func (s *SLOStore) ensureTables() error {
	ctx := context.Background()

	if err := s.conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.slo_definitions (
    service_name  String,
    slo_name      String,
    window_hours  Int32   DEFAULT 720,
    target_pct    Float64 DEFAULT 99.9,
    metric_type   LowCardinality(String) DEFAULT 'error_rate',
    threshold_ms  Float64 DEFAULT 0,
    updated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (service_name, slo_name);
`, s.db)); err != nil {
		return fmt.Errorf("create slo_definitions: %w", err)
	}

	if err := s.conn.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.slo_burn_alerts (
    service_name  LowCardinality(String),
    slo_name      String,
    burn_rate     Float64,
    window        LowCardinality(String),
    severity      LowCardinality(String),
    alerted_at    DateTime,
    dt            Date DEFAULT toDate(alerted_at)
) ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (service_name, alerted_at)
TTL dt + INTERVAL 90 DAY;
`, s.db)); err != nil {
		return fmt.Errorf("create slo_burn_alerts: %w", err)
	}

	return nil
}

// UpsertSLO는 SLO 정의를 삽입하거나 갱신한다.
func (s *SLOStore) UpsertSLO(ctx context.Context, def SLODefinition) error {
	if def.MetricType == "" {
		def.MetricType = "error_rate"
	}
	if def.WindowHours <= 0 {
		def.WindowHours = 720
	}
	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf(
		`INSERT INTO %s.slo_definitions
		 (service_name, slo_name, window_hours, target_pct, metric_type, threshold_ms, updated_at)
		 VALUES`, s.db))
	if err != nil {
		return err
	}
	if err := batch.Append(
		def.ServiceName, def.SLOName, int32(def.WindowHours),
		def.TargetPct, def.MetricType, def.ThresholdMs, time.Now(),
	); err != nil {
		return err
	}
	return batch.Send()
}

// ListSLOs는 등록된 모든 SLO 정의를 반환한다.
func (s *SLOStore) ListSLOs(ctx context.Context) ([]SLODefinition, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT service_name, slo_name, window_hours, target_pct, metric_type, threshold_ms, updated_at
FROM %s.slo_definitions FINAL
ORDER BY service_name, slo_name`, s.db))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []SLODefinition
	for rows.Next() {
		var d SLODefinition
		var windowHours int32
		if err := rows.Scan(&d.ServiceName, &d.SLOName, &windowHours,
			&d.TargetPct, &d.MetricType, &d.ThresholdMs, &d.UpdatedAt); err != nil {
			return nil, err
		}
		d.WindowHours = int(windowHours)
		result = append(result, d)
	}
	return result, rows.Err()
}

// GetBurnAlerts는 최근 번-레이트 알람을 반환한다.
func (s *SLOStore) GetBurnAlerts(ctx context.Context, service string, limit int) ([]SLOBurnAlert, error) {
	if limit <= 0 {
		limit = 100
	}
	var conds []string
	var args []any
	if service != "" {
		conds = append(conds, "service_name = ?")
		args = append(args, service)
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
SELECT service_name, slo_name, burn_rate, window, severity, alerted_at
FROM %s.slo_burn_alerts
%s
ORDER BY alerted_at DESC
LIMIT %d`, s.db, where, limit), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []SLOBurnAlert
	for rows.Next() {
		var a SLOBurnAlert
		if err := rows.Scan(&a.ServiceName, &a.SLOName, &a.BurnRate,
			&a.Window, &a.Severity, &a.AlertedAt); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}

// SLOBurnCalculator는 주기적으로 SLO 번-레이트를 계산해 알람을 기록하는 goroutine이다.
type SLOBurnCalculator struct {
	store    *SLOStore
	conn     driver.Conn
	db       string
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewSLOBurnCalculator는 SLOBurnCalculator를 생성한다.
func NewSLOBurnCalculator(store *SLOStore, conn driver.Conn, db string, interval time.Duration) *SLOBurnCalculator {
	if interval <= 0 {
		interval = time.Minute
	}
	return &SLOBurnCalculator{
		store:    store,
		conn:     conn,
		db:       db,
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start는 번-레이트 계산 goroutine을 시작한다.
func (c *SLOBurnCalculator) Start() {
	go c.run()
}

// Stop은 번-레이트 계산 goroutine을 중단하고 완료를 기다린다.
func (c *SLOBurnCalculator) Stop() {
	close(c.stopCh)
	<-c.doneCh
}

func (c *SLOBurnCalculator) run() {
	defer close(c.doneCh)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if err := c.evaluate(); err != nil {
				slog.Warn("slo burn calculator error", "err", err)
			}
		}
	}
}

// evaluate는 등록된 모든 SLO의 번-레이트를 계산하고 임계치 초과 시 알람을 기록한다.
func (c *SLOBurnCalculator) evaluate() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	defs, err := c.store.ListSLOs(ctx)
	if err != nil {
		return fmt.Errorf("list slos: %w", err)
	}
	if len(defs) == 0 {
		return nil
	}

	now := time.Now()
	// 1h와 6h 윈도우를 함께 평가한다.
	type window struct {
		name      string
		hours     int
		threshold float64 // 번-레이트 임계치
		severity  string
	}
	windows := []window{
		{"1h", 1, fastBurnMultiplier, "page"},
		{"6h", 6, slowBurnMultiplier, "ticket"},
	}

	var alerts []SLOBurnAlert
	for _, def := range defs {
		errorBudget := 1.0 - def.TargetPct/100.0
		if errorBudget <= 0 {
			continue
		}
		for _, w := range windows {
			badRate, err := c.queryBadRate(ctx, def, now, w.hours)
			if err != nil {
				slog.Warn("slo bad rate query failed",
					"service", def.ServiceName, "slo", def.SLOName, "window", w.name, "err", err)
				continue
			}
			burnRate := badRate / errorBudget
			if burnRate > w.threshold {
				alerts = append(alerts, SLOBurnAlert{
					ServiceName: def.ServiceName,
					SLOName:     def.SLOName,
					BurnRate:    burnRate,
					Window:      w.name,
					Severity:    w.severity,
					AlertedAt:   now,
				})
				slog.Warn("slo burn rate alert",
					"service", def.ServiceName, "slo", def.SLOName,
					"burn_rate", burnRate, "window", w.name, "severity", w.severity)
			}
		}
	}

	if len(alerts) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, fmt.Sprintf(
		`INSERT INTO %s.slo_burn_alerts
		 (service_name, slo_name, burn_rate, window, severity, alerted_at)
		 VALUES`, c.db))
	if err != nil {
		return fmt.Errorf("prepare burn_alerts batch: %w", err)
	}
	for _, a := range alerts {
		if err := batch.Append(a.ServiceName, a.SLOName, a.BurnRate, a.Window, a.Severity, a.AlertedAt); err != nil {
			return fmt.Errorf("append burn alert: %w", err)
		}
	}
	return batch.Send()
}

// queryBadRate는 주어진 SLO 정의와 시간 윈도우에 대해 실제 불량 비율을 계산한다.
func (c *SLOBurnCalculator) queryBadRate(ctx context.Context, def SLODefinition, now time.Time, windowHours int) (float64, error) {
	fromTime := now.Add(-time.Duration(windowHours) * time.Hour)

	switch def.MetricType {
	case "error_rate":
		row := c.conn.QueryRow(ctx, fmt.Sprintf(`
SELECT
    sum(error_count) / nullIf(sum(total_count), 0) AS bad_rate
FROM %s.mv_red_1m_state
WHERE service_name = ?
  AND minute >= ?`, c.db), def.ServiceName, fromTime)
		var badRate *float64
		if err := row.Scan(&badRate); err != nil {
			return 0, err
		}
		if badRate == nil {
			return 0, nil
		}
		return *badRate, nil

	case "latency_p95":
		if def.ThresholdMs <= 0 {
			return 0, nil
		}
		// p95 > threshold인 분(minute)의 비율 = 불량률
		row := c.conn.QueryRow(ctx, fmt.Sprintf(`
SELECT
    countIf(quantilesMerge(0.95)(duration_quantiles)[1] / 1e6 > ?) / nullIf(count(), 0) AS bad_rate
FROM %s.mv_red_1m_state
WHERE service_name = ?
  AND minute >= ?
GROUP BY service_name`, c.db), def.ThresholdMs, def.ServiceName, fromTime)
		var badRate *float64
		if err := row.Scan(&badRate); err != nil {
			return 0, err
		}
		if badRate == nil {
			return 0, nil
		}
		return *badRate, nil

	default:
		return 0, fmt.Errorf("unsupported metric_type: %s", def.MetricType)
	}
}
