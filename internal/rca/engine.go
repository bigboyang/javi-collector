// Package rca는 AIOps Phase 3 — Root Cause Analysis 엔진을 제공한다.
//
// 처리 파이프라인 (매 interval):
//  1. anomalies 테이블에서 직전 interval 동안 새로 탐지된 이상 이벤트를 읽는다.
//  2. 각 anomaly에 대해 해당 minute ± 1분 범위의 ERROR/WARN spans를 수집한다.
//  3. RAGSearcher(선택)로 과거 유사 장애 케이스를 검색한다.
//  4. 이상 유형 + 연관 데이터를 기반으로 가설(Hypothesis)을 생성한다.
//  5. rca_reports 테이블에 배치 INSERT한다.
package rca

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kkc/javi-collector/internal/rag"
)

// Searcher는 RAGSearcher의 추상 인터페이스 (테스트 교체 가능).
type Searcher interface {
	Search(ctx context.Context, req rag.SearchRequest) ([]rag.SearchResult, error)
}

// Generator는 RAGGenerator의 추상 인터페이스 (테스트 교체 가능).
// anomalyDesc와 검색 파라미터를 받아 LLM 기반 RCA 분석 텍스트를 반환한다.
type Generator interface {
	Generate(ctx context.Context, anomalyDesc string, searchReq *rag.SearchRequest) (string, error)
}

// Config는 RCA Engine 설정.
type Config struct {
	Interval time.Duration // 폴링 주기 (기본 2m)
}

// DefaultConfig returns production-ready defaults.
func DefaultConfig() Config {
	return Config{
		Interval: 2 * time.Minute,
	}
}

// Engine은 RCA 백그라운드 고루틴을 관리한다.
type Engine struct {
	conn      driver.Conn
	db        string
	interval  time.Duration
	searcher  Searcher  // nil이면 RAG 검색 생략
	generator Generator // nil이면 LLM Generation 생략

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewEngine creates a new RCA Engine.
// searcher가 nil이면 RAG 유사 사례 검색을 건너뛴다.
func NewEngine(conn driver.Conn, db string, cfg Config, searcher Searcher) *Engine {
	return &Engine{
		conn:     conn,
		db:       db,
		interval: cfg.Interval,
		searcher: searcher,
		stopCh:   make(chan struct{}),
	}
}

// SetGenerator attaches an LLM generator. Must be called before Start.
func (e *Engine) SetGenerator(g Generator) { e.generator = g }

// Start launches the background goroutine.
func (e *Engine) Start() {
	e.wg.Add(1)
	go e.run()
}

// Stop signals the goroutine to stop and waits for completion.
func (e *Engine) Stop() {
	close(e.stopCh)
	e.wg.Wait()
}

func (e *Engine) run() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.process()
		case <-e.stopCh:
			return
		}
	}
}

func (e *Engine) process() {
	ctx, cancel := context.WithTimeout(context.Background(), e.interval)
	defer cancel()

	// 직전 interval*2 기간의 anomalies를 읽는다 (처리 지연 여유 포함)
	anomalies, err := e.fetchRecentAnomalies(ctx, e.interval*2)
	if err != nil {
		slog.Error("rca: fetch anomalies failed", "err", err)
		return
	}
	if len(anomalies) == 0 {
		return
	}

	var reports []RCAReport
	for _, a := range anomalies {
		r, err := e.analyze(ctx, a)
		if err != nil {
			slog.Warn("rca: analyze failed", "anomaly_id", a.ID, "err", err)
			continue
		}
		reports = append(reports, r)
	}

	if len(reports) == 0 {
		return
	}
	if err := e.insertReports(ctx, reports); err != nil {
		slog.Error("rca: insert failed", "err", err, "count", len(reports))
		return
	}
	slog.Info("rca: reports generated", "count", len(reports))
}

// analyze는 단일 AnomalyRow에 대한 RCAReport를 생성한다.
func (e *Engine) analyze(ctx context.Context, a AnomalyRow) (RCAReport, error) {
	// 1. 연관 spans 수집
	spans, err := e.fetchCorrelatedSpans(ctx, a)
	if err != nil {
		slog.Warn("rca: correlated spans fetch failed", "err", err)
	}

	// 2. RAG 유사 사례 검색
	var similar []SimilarIncident
	if e.searcher != nil {
		similar, err = e.searchSimilarIncidents(ctx, a)
		if err != nil {
			slog.Warn("rca: rag search failed", "err", err)
		}
	}

	// 3. Causal Chain: 토폴로지 인접 서비스 조회
	neighbors, err := e.fetchTopologyNeighbors(ctx, a)
	if err != nil {
		slog.Warn("rca: topology fetch failed", "err", err)
	}

	// 4. 규칙 기반 가설 생성 (토폴로지 컨텍스트 포함)
	hypo := buildHypothesis(a, spans, similar, neighbors)

	// 4. LLM 기반 RCA 분석 (Generator가 설정된 경우)
	var llmAnalysis string
	if e.generator != nil {
		anomalyDesc := hypo // 규칙 기반 요약을 LLM 입력으로 재활용
		searchReq := &rag.SearchRequest{
			Query:       fmt.Sprintf("%s anomaly in %s operation %s z_score %.1f", a.AnomalyType, a.ServiceName, a.SpanName, a.ZScore),
			ServiceName: a.ServiceName,
			Limit:       3,
		}
		llmAnalysis, err = e.generator.Generate(ctx, anomalyDesc, searchReq)
		if err != nil {
			slog.Warn("rca: llm generation failed", "anomaly_id", a.ID, "err", err)
		}
	}

	return RCAReport{
		ID:                newID(),
		AnomalyID:         a.ID,
		ServiceName:       a.ServiceName,
		SpanName:          a.SpanName,
		AnomalyType:       a.AnomalyType,
		Minute:            a.Minute,
		Severity:          a.Severity,
		ZScore:            a.ZScore,
		CorrelatedSpans:   spans,
		SimilarIncidents:  similar,
		TopologyNeighbors: neighbors,
		Hypothesis:        hypo,
		LLMAnalysis:       llmAnalysis,
		CreatedAt:         time.Now(),
	}, nil
}

// fetchRecentAnomalies는 최근 window 기간의 anomalies를 조회한다.
func (e *Engine) fetchRecentAnomalies(ctx context.Context, window time.Duration) ([]AnomalyRow, error) {
	q := fmt.Sprintf(`
SELECT id, service_name, span_name, anomaly_type, minute,
       current_value, baseline_value, z_score, severity, detected_at
FROM %s.anomalies
WHERE detected_at >= now() - INTERVAL %d SECOND
ORDER BY detected_at ASC`, e.db, int(window.Seconds()))

	rows, err := e.conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AnomalyRow
	for rows.Next() {
		var a AnomalyRow
		if err := rows.Scan(
			&a.ID, &a.ServiceName, &a.SpanName, &a.AnomalyType, &a.Minute,
			&a.CurrentValue, &a.BaselineValue, &a.ZScore, &a.Severity, &a.DetectedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// fetchCorrelatedSpans는 이상 시간대의 ERROR/WARN spans를 최대 5개 반환한다.
func (e *Engine) fetchCorrelatedSpans(ctx context.Context, a AnomalyRow) ([]CorrelatedSpan, error) {
	q := fmt.Sprintf(`
SELECT
    span_id, trace_id, name, status_code, status_message,
    (end_time_nano - start_time_nano) / 1e6 AS duration_ms,
    ifNull(attributes['exception.type'], '')
FROM %s.spans
WHERE service_name = '%s'
  AND start_time_nano >= toUnixTimestamp64Nano(toDateTime64('%s', 9))
  AND start_time_nano <  toUnixTimestamp64Nano(toDateTime64('%s', 9)) + 120000000000
  AND status_code >= 2
ORDER BY duration_ms DESC
LIMIT 5`,
		e.db,
		escapeStr(a.ServiceName),
		a.Minute.UTC().Format("2006-01-02 15:04:05"),
		a.Minute.UTC().Format("2006-01-02 15:04:05"),
	)

	rows, err := e.conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CorrelatedSpan
	for rows.Next() {
		var s CorrelatedSpan
		if err := rows.Scan(
			&s.SpanID, &s.TraceID, &s.Name, &s.StatusCode,
			&s.StatusMessage, &s.DurationMs, &s.ExceptionType,
		); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// searchSimilarIncidents는 RAGSearcher로 과거 유사 장애를 검색한다.
func (e *Engine) searchSimilarIncidents(ctx context.Context, a AnomalyRow) ([]SimilarIncident, error) {
	query := fmt.Sprintf("%s anomaly in %s operation %s z_score %.1f",
		a.AnomalyType, a.ServiceName, a.SpanName, a.ZScore)

	results, err := e.searcher.Search(ctx, rag.SearchRequest{
		Query:       query,
		ServiceName: a.ServiceName,
		Limit:       3,
	})
	if err != nil {
		return nil, err
	}

	out := make([]SimilarIncident, 0, len(results))
	for _, r := range results {
		summary := r.Text
		if len(summary) > 300 {
			summary = summary[:300] + "..."
		}
		out = append(out, SimilarIncident{
			TraceID:     r.TraceID,
			ServiceName: r.ServiceName,
			Score:       r.Score,
			Summary:     summary,
		})
	}
	return out, nil
}

// fetchTopologyNeighbors는 mv_service_topology_state에서 이상 발생 서비스의
// 직접 업스트림/다운스트림 서비스를 조회한다.
func (e *Engine) fetchTopologyNeighbors(ctx context.Context, a AnomalyRow) ([]TopologyNeighbor, error) {
	// 이상 발생 시간 ±30분 윈도우에서 토폴로지 조회
	fromTime := a.Minute.Add(-30 * time.Minute)
	toTime := a.Minute.Add(30 * time.Minute)

	// downstream: 이 서비스가 호출한 서비스 (client spans)
	// upstream: 이 서비스를 호출한 서비스 (this service appears as peer_service)
	q := fmt.Sprintf(`
SELECT
    peer_service,
    'downstream' AS direction,
    sum(call_count) AS total_calls,
    if(sum(call_count) > 0, toFloat64(sum(error_count)) / toFloat64(sum(call_count)), 0) AS error_rate
FROM %s.mv_service_topology_state
WHERE caller_service = '%s'
  AND minute >= ? AND minute <= ?
GROUP BY peer_service
UNION ALL
SELECT
    caller_service,
    'upstream' AS direction,
    sum(call_count) AS total_calls,
    if(sum(call_count) > 0, toFloat64(sum(error_count)) / toFloat64(sum(call_count)), 0) AS error_rate
FROM %s.mv_service_topology_state
WHERE peer_service = '%s'
  AND minute >= ? AND minute <= ?
GROUP BY caller_service
ORDER BY total_calls DESC
LIMIT 10`,
		e.db, escapeStr(a.ServiceName),
		e.db, escapeStr(a.ServiceName),
	)

	rows, err := e.conn.Query(ctx, q, fromTime, toTime, fromTime, toTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TopologyNeighbor
	for rows.Next() {
		var n TopologyNeighbor
		if err := rows.Scan(&n.ServiceName, &n.Direction, &n.CallCount, &n.ErrorRate); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// buildHypothesis는 anomaly 유형과 연관 데이터를 기반으로 가설 문자열을 생성한다.
func buildHypothesis(a AnomalyRow, spans []CorrelatedSpan, similar []SimilarIncident, neighbors []TopologyNeighbor) string {
	var sb strings.Builder

	switch a.AnomalyType {
	case "latency_p95_spike":
		fmt.Fprintf(&sb, "[Latency Spike] %s/%s P95 응답 시간이 기준값(%.1fms) 대비 %.1fms(Z=%.1f)으로 급증.",
			a.ServiceName, a.SpanName, a.BaselineValue, a.CurrentValue, a.ZScore)
	case "error_rate_spike":
		fmt.Fprintf(&sb, "[Error Rate Spike] %s/%s 에러율이 기준값(%.1f%%) 대비 %.1f%%(Z=%.1f)으로 급증.",
			a.ServiceName, a.SpanName, a.BaselineValue*100, a.CurrentValue*100, a.ZScore)
	case "traffic_drop":
		ratio := 0.0
		if a.BaselineValue > 0 {
			ratio = a.CurrentValue / a.BaselineValue * 100
		}
		fmt.Fprintf(&sb, "[Traffic Drop] %s/%s 요청량이 기준값(%.1f RPS) 대비 %.0f%%로 감소.",
			a.ServiceName, a.SpanName, a.BaselineValue, ratio)
	case "multivariate_anomaly":
		fmt.Fprintf(&sb, "[Multivariate Anomaly] %s/%s 다변량 IsolationForest 점수=%.2f (임계값=%.2f).",
			a.ServiceName, a.SpanName, a.CurrentValue, a.BaselineValue)
	default:
		fmt.Fprintf(&sb, "[%s] %s/%s 이상 감지.", a.AnomalyType, a.ServiceName, a.SpanName)
	}

	// 연관 스팬 중 가장 긴 에러 스팬 요약
	if len(spans) > 0 {
		top := spans[0]
		sb.WriteString(" 가장 긴 에러 스팬: ")
		fmt.Fprintf(&sb, "%s (%.0fms", top.Name, top.DurationMs)
		if top.ExceptionType != "" {
			fmt.Fprintf(&sb, ", %s", top.ExceptionType)
		}
		sb.WriteString(").")
	}

	// 유사 과거 사례
	if len(similar) > 0 {
		fmt.Fprintf(&sb, " 유사 과거 사례 %d건 발견 (최고 유사도: %.2f).", len(similar), similar[0].Score)
	}

	// Causal Chain: 에러율이 높은 업/다운스트림 서비스 강조
	for _, n := range neighbors {
		if n.ErrorRate >= 0.05 { // 5% 이상 에러율인 인접 서비스만 언급
			fmt.Fprintf(&sb, " %s 서비스(%s) 에러율 %.1f%% 감지.",
				n.ServiceName, n.Direction, n.ErrorRate*100)
		}
	}

	return sb.String()
}

// insertReports는 rca_reports 테이블에 배치 INSERT한다.
func (e *Engine) insertReports(ctx context.Context, reports []RCAReport) error {
	batch, err := e.conn.PrepareBatch(ctx, fmt.Sprintf(
		`INSERT INTO %s.rca_reports
		 (id, anomaly_id, service_name, span_name, anomaly_type, minute,
		  severity, z_score, correlated_spans, similar_incidents, hypothesis, llm_analysis, created_at)`,
		e.db))
	if err != nil {
		return err
	}

	for _, r := range reports {
		spansJSON, _ := json.Marshal(r.CorrelatedSpans)
		similarJSON, _ := json.Marshal(r.SimilarIncidents)
		if err := batch.Append(
			r.ID, r.AnomalyID, r.ServiceName, r.SpanName, r.AnomalyType, r.Minute,
			r.Severity, r.ZScore,
			string(spansJSON), string(similarJSON),
			r.Hypothesis, r.LLMAnalysis, r.CreatedAt,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// escapeStr은 SQL 인젝션 방지를 위해 단순 문자열을 이스케이프한다.
func escapeStr(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// newID generates a cryptographically random 32-character hex ID.
func newID() string {
	var b [16]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
