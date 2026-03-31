package anomaly

// Detector는 AIOps Phase 2 이상 감지 고루틴을 관리한다.
//
// 탐지 파이프라인 (매 interval):
//  1. mv_red_1m_state에서 직전 1분 RED 메트릭 조회
//  2. red_baseline(FINAL)에서 해당 요일+시간대 기준선 조회
//  3. Z-score 분석: latency_p95_spike / error_rate_spike / traffic_drop
//  4. IsolationForest 다변량 점수: multivariate_anomaly
//  5. anomalies 테이블에 배치 INSERT
//
// IsolationForest 재학습 (매 trainInterval, 기동 시 즉시 1회):
//   - red_baseline 전체 행을 feature matrix로 변환해 Fit()
//   - 정규화 기준(maxP95, maxRPS)도 함께 갱신

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Config holds Detector configuration.
type Config struct {
	Interval      time.Duration // 탐지 주기 (기본 1m)
	TrainInterval time.Duration // IForest 재학습 주기 (기본 6h)
	NTrees        int           // IForest 트리 수 (기본 100)
	MaxSamples    int           // IForest 부분집합 크기 (기본 256)
	ZWarn         float64       // Z-score 경고 임계값 (기본 2.0)
	ZCritical     float64       // Z-score 위험 임계값 (기본 3.0)
	IFThreshold   float64       // IForest 이상 점수 임계값 (기본 0.65)
}

// DefaultConfig returns production-ready defaults.
func DefaultConfig() Config {
	return Config{
		Interval:      time.Minute,
		TrainInterval: 6 * time.Hour,
		NTrees:        100,
		MaxSamples:    256,
		ZWarn:         2.0,
		ZCritical:     3.0,
		IFThreshold:   0.65,
	}
}

// Detector manages the anomaly detection background goroutine.
type Detector struct {
	conn          driver.Conn
	db            string
	interval      time.Duration
	trainInterval time.Duration
	zWarn         float64
	zCrit         float64
	ifThresh      float64

	// IsolationForest 학습 상태 (mu로 보호)
	forest      *IsolationForest
	normMaxP95  float64
	normMaxRPS  float64
	mu          sync.RWMutex
	lastTrained time.Time

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewDetector creates a new Detector.
func NewDetector(conn driver.Conn, db string, cfg Config) *Detector {
	return &Detector{
		conn:          conn,
		db:            db,
		interval:      cfg.Interval,
		trainInterval: cfg.TrainInterval,
		zWarn:         cfg.ZWarn,
		zCrit:         cfg.ZCritical,
		ifThresh:      cfg.IFThreshold,
		forest:        NewIsolationForest(cfg.NTrees, cfg.MaxSamples),
		normMaxP95:    1.0,
		normMaxRPS:    1.0,
		stopCh:        make(chan struct{}),
	}
}

// Start launches the background goroutine.
func (d *Detector) Start() {
	d.wg.Add(1)
	go d.run()
}

// Stop signals the goroutine to stop and waits for completion.
func (d *Detector) Stop() {
	close(d.stopCh)
	d.wg.Wait()
}

func (d *Detector) run() {
	defer d.wg.Done()

	// 기동 즉시 IForest 학습 (재시작 후 최신 baseline으로 초기화)
	d.trainForest()

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if time.Since(d.lastTrained) >= d.trainInterval {
				d.trainForest()
			}
			d.detect()
		case <-d.stopCh:
			return
		}
	}
}

// trainForest loads red_baseline and re-trains the IsolationForest.
func (d *Detector) trainForest() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	baselines, err := d.queryBaseline(ctx)
	if err != nil {
		slog.Error("anomaly: baseline load failed", "err", err)
		return
	}
	if len(baselines) < 10 {
		slog.Warn("anomaly: insufficient baseline rows for IForest", "rows", len(baselines))
		return
	}

	// Feature normalization: maxP95 기준으로 레이턴시, maxRPS 기준으로 처리량 정규화
	maxP95, maxRPS := 1.0, 1.0
	for _, b := range baselines {
		if b.P95Ms > maxP95 {
			maxP95 = b.P95Ms
		}
		if b.AvgRPS > maxRPS {
			maxRPS = b.AvgRPS
		}
	}

	// Feature matrix: [p50/maxP95, p95/maxP95, p99/maxP95, error_rate, rps/maxRPS]
	matrix := make([][]float64, len(baselines))
	for i, b := range baselines {
		matrix[i] = []float64{
			b.P50Ms / maxP95,
			b.P95Ms / maxP95,
			b.P99Ms / maxP95,
			b.ErrorRate,
			b.AvgRPS / maxRPS,
		}
	}

	d.mu.Lock()
	d.forest.Fit(matrix)
	d.normMaxP95 = maxP95
	d.normMaxRPS = maxRPS
	d.lastTrained = time.Now()
	d.mu.Unlock()

	slog.Info("anomaly: IsolationForest trained",
		"samples", len(baselines),
		"max_p95_ms", maxP95,
		"max_rps", maxRPS,
	)
}

// detect runs one detection cycle for the last completed minute.
func (d *Detector) detect() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	points, err := d.queryCurrentRED(ctx)
	if err != nil {
		slog.Error("anomaly: RED query failed", "err", err)
		return
	}
	if len(points) == 0 {
		return
	}

	baselines, err := d.queryBaseline(ctx)
	if err != nil {
		slog.Error("anomaly: baseline query failed", "err", err)
		return
	}

	// baseline lookup: "service|span|route|dow|hour" → BaselineRow
	baselineMap := make(map[string]BaselineRow, len(baselines))
	for _, b := range baselines {
		k := baselineKey(b.ServiceName, b.SpanName, b.HTTPRoute, b.DayOfWeek, b.HourOfDay)
		baselineMap[k] = b
	}

	d.mu.RLock()
	maxP95 := d.normMaxP95
	maxRPS := d.normMaxRPS
	forest := d.forest
	d.mu.RUnlock()

	var detected []AnomalyRecord

	for _, pt := range points {
		// ClickHouse toDayOfWeek: 1=Mon .. 7=Sun
		dow := uint8(pt.Minute.Weekday())
		if dow == 0 {
			dow = 7 // Sunday
		}
		hour := uint8(pt.Minute.Hour())

		b, ok := baselineMap[baselineKey(pt.ServiceName, pt.SpanName, pt.HTTPRoute, dow, hour)]
		if !ok {
			continue // 신규 서비스/오퍼레이션은 기준선 충분히 쌓인 후 탐지
		}

		// ── Z-score: Latency P95 ──────────────────────────────────────────
		// stdEst = IQR-기반 정규분포 σ 추정 (p99 ≈ μ+2.326σ, p50 ≈ μ)
		// 최소 std를 baseline의 5%로 보정해 0-division 및 과민 탐지 방지
		if b.P95Ms > 0 {
			stdEst := math.Max((b.P99Ms-b.P50Ms)/2.326, b.P95Ms*0.05)
			z := (pt.P95Ms - b.P95Ms) / stdEst
			if z >= d.zWarn {
				sev := SeverityWarning
				if z >= d.zCrit {
					sev = SeverityCritical
				}
				detected = append(detected, AnomalyRecord{
					ID:            newID(),
					ServiceName:   pt.ServiceName,
					SpanName:      pt.SpanName,
					AnomalyType:   TypeLatencySpike,
					Minute:        pt.Minute,
					CurrentValue:  pt.P95Ms,
					BaselineValue: b.P95Ms,
					ZScore:        z,
					Severity:      sev,
				})
			}
		}

		// ── Z-score: Error Rate ───────────────────────────────────────────
		// 이항분포 표준오차: σ = sqrt(p*(1-p)/n)
		// 최소 0.1% 보장 (표본 100개이고 error_rate=0인 경우 분모=0 방지)
		if b.SampleCount >= 10 {
			baselineStdErr := math.Sqrt(b.ErrorRate*(1-b.ErrorRate)/float64(b.SampleCount))
			if baselineStdErr < 0.001 {
				baselineStdErr = 0.001
			}
			z := (pt.ErrorRate - b.ErrorRate) / baselineStdErr
			if z >= d.zWarn {
				sev := SeverityWarning
				if z >= d.zCrit {
					sev = SeverityCritical
				}
				detected = append(detected, AnomalyRecord{
					ID:            newID(),
					ServiceName:   pt.ServiceName,
					SpanName:      pt.SpanName,
					AnomalyType:   TypeErrorRateSpike,
					Minute:        pt.Minute,
					CurrentValue:  pt.ErrorRate,
					BaselineValue: b.ErrorRate,
					ZScore:        z,
					Severity:      sev,
				})
			}
		}

		// ── Traffic Drop ─────────────────────────────────────────────────
		// baseline avg_rps의 50% 미만이면 critical 트래픽 감소로 판단
		if b.AvgRPS > 0 && pt.RPS < b.AvgRPS*0.5 {
			ratio := pt.RPS / b.AvgRPS
			detected = append(detected, AnomalyRecord{
				ID:            newID(),
				ServiceName:   pt.ServiceName,
				SpanName:      pt.SpanName,
				AnomalyType:   TypeTrafficDrop,
				Minute:        pt.Minute,
				CurrentValue:  pt.RPS,
				BaselineValue: b.AvgRPS,
				ZScore:        (1 - ratio) * 10, // proxy z-score (1.0 drop → z=10)
				Severity:      SeverityCritical,
			})
		}

		// ── IsolationForest: 다변량 이상 ─────────────────────────────────
		// 5차원 feature vector를 baseline 최대값으로 정규화 후 점수 계산
		// score >= ifThresh(기본 0.65) 이면 정상 범위를 벗어난 다변량 이상
		if forest.Trained() {
			feat := []float64{
				pt.P50Ms / maxP95,
				pt.P95Ms / maxP95,
				pt.P99Ms / maxP95,
				pt.ErrorRate,
				pt.RPS / maxRPS,
			}
			score := forest.Score(feat)
			if score >= d.ifThresh {
				sev := SeverityWarning
				if score >= 0.75 {
					sev = SeverityCritical
				}
				detected = append(detected, AnomalyRecord{
					ID:            newID(),
					ServiceName:   pt.ServiceName,
					SpanName:      pt.SpanName,
					AnomalyType:   TypeMultivariate,
					Minute:        pt.Minute,
					CurrentValue:  score,
					BaselineValue: d.ifThresh,
					ZScore:        score,
					Severity:      sev,
				})
			}
		}
	}

	if len(detected) == 0 {
		return
	}

	if err := d.insertAnomalies(ctx, detected); err != nil {
		slog.Error("anomaly: insert failed", "err", err, "count", len(detected))
		return
	}
	slog.Info("anomaly: recorded", "count", len(detected))
}

// queryCurrentRED queries mv_red_1m_state for the last completed minute.
func (d *Detector) queryCurrentRED(ctx context.Context) ([]REDPoint, error) {
	q := fmt.Sprintf(`
SELECT
    service_name,
    span_name,
    http_route,
    minute,
    sumMerge(total_count)                                                   AS total_count,
    sumMerge(error_count) / greatest(sumMerge(total_count), 1)             AS error_rate,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[1] / 1e6          AS p50_ms,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[2] / 1e6          AS p95_ms,
    quantilesMerge(0.5, 0.95, 0.99)(duration_quantiles)[3] / 1e6          AS p99_ms
FROM %s.mv_red_1m_state
WHERE minute = toStartOfMinute(now()) - INTERVAL 1 MINUTE
GROUP BY service_name, span_name, http_route, minute
HAVING total_count >= 5`, d.db)

	rows, err := d.conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pts []REDPoint
	for rows.Next() {
		var pt REDPoint
		var totalCount uint64
		if err := rows.Scan(
			&pt.ServiceName, &pt.SpanName, &pt.HTTPRoute, &pt.Minute,
			&totalCount, &pt.ErrorRate,
			&pt.P50Ms, &pt.P95Ms, &pt.P99Ms,
		); err != nil {
			return nil, err
		}
		pt.SampleCount = totalCount
		pt.RPS = float64(totalCount) / 60.0
		pts = append(pts, pt)
	}
	return pts, rows.Err()
}

// queryBaseline loads all red_baseline rows with FINAL deduplication.
func (d *Detector) queryBaseline(ctx context.Context) ([]BaselineRow, error) {
	q := fmt.Sprintf(`
SELECT
    service_name, span_name, http_route,
    day_of_week, hour_of_day,
    p50_ms, p95_ms, p99_ms,
    error_rate, avg_rps, sample_count
FROM %s.red_baseline FINAL`, d.db)

	rows, err := d.conn.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []BaselineRow
	for rows.Next() {
		var b BaselineRow
		if err := rows.Scan(
			&b.ServiceName, &b.SpanName, &b.HTTPRoute,
			&b.DayOfWeek, &b.HourOfDay,
			&b.P50Ms, &b.P95Ms, &b.P99Ms,
			&b.ErrorRate, &b.AvgRPS, &b.SampleCount,
		); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// insertAnomalies batch-inserts anomaly records into the anomalies table.
func (d *Detector) insertAnomalies(ctx context.Context, anomalies []AnomalyRecord) error {
	batch, err := d.conn.PrepareBatch(ctx, fmt.Sprintf(
		`INSERT INTO %s.anomalies
		 (id, service_name, span_name, anomaly_type, minute,
		  current_value, baseline_value, z_score, severity)`, d.db))
	if err != nil {
		return err
	}
	for _, a := range anomalies {
		if err := batch.Append(
			a.ID, a.ServiceName, a.SpanName, a.AnomalyType, a.Minute,
			a.CurrentValue, a.BaselineValue, a.ZScore, a.Severity,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// builderPool reuses strings.Builder instances to eliminate per-call heap
// allocations in the hot detection loop.
var builderPool = sync.Pool{
	New: func() any {
		b := new(strings.Builder)
		b.Grow(128)
		return b
	},
}

// baselineKey returns a lookup key for the baseline map.
//
// Hot-path notes:
//   - sync.Pool reuses the Builder's internal []byte buffer across calls.
//   - dow (1–7) and hour (0–23) are stored as raw bytes offset by +1
//     (→ 2–8 and 1–24) so they are never confused with the '\x00' separator.
//   - strconv.Itoa is no longer needed.
func baselineKey(svc, span, route string, dow, hour uint8) string {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	b.WriteString(svc)
	b.WriteByte('\x00')
	b.WriteString(span)
	b.WriteByte('\x00')
	b.WriteString(route)
	b.WriteByte('\x00')
	b.WriteByte(dow + 1)  // 1–7  → 2–8,  never '\x00'
	b.WriteByte('\x00')
	b.WriteByte(hour + 1) // 0–23 → 1–24, never '\x00'
	key := b.String()
	builderPool.Put(b)
	return key
}

// newID generates a cryptographically random 32-character hex ID.
func newID() string {
	var b [16]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
