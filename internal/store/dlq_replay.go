// Package store - dlq_replay.go
//
// DLQReplayer는 DLQ 디렉터리의 JSONL 파일을 읽어 ClickHouse에 자동 재적재한다.
//
// 동작:
//   - Start() 호출 시 즉시 한 번 재적재 (기동 시 이전 DLQ 소비)
//   - interval > 0 이면 주기적으로 재적재
//   - 오늘 날짜 파일은 건너뜀 (현재 프로세스가 기록 중일 수 있음)
//   - 재적재 성공 시 파일 삭제
//
// CleanupDLQFiles는 retentionDays보다 오래된 JSONL 파일을 삭제한다.
package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/kkc/javi-collector/internal/model"
)

var (
	chDLQReplayedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "dlq_replayed_total",
		Help:      "Total number of items successfully replayed from DLQ to ClickHouse.",
	}, []string{"table"})

	chDLQCleanedFilesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "javi",
		Subsystem: "clickhouse",
		Name:      "dlq_cleaned_files_total",
		Help:      "Total number of DLQ files deleted by retention policy.",
	})
)

// DLQReplayer는 DLQ 파일을 읽어 ClickHouse에 자동 재적재한다.
type DLQReplayer struct {
	dir         string
	traceStore  TraceStore
	metricStore MetricStore
	logStore    LogStore
	interval    time.Duration
}

// NewDLQReplayer는 DLQ 재적재기를 생성한다.
// interval=0 이면 기동 시 한 번만 실행한다.
func NewDLQReplayer(dir string, ts TraceStore, ms MetricStore, ls LogStore, interval time.Duration) *DLQReplayer {
	return &DLQReplayer{
		dir:         dir,
		traceStore:  ts,
		metricStore: ms,
		logStore:    ls,
		interval:    interval,
	}
}

// Start는 백그라운드에서 DLQ 재적재를 시작한다.
// 즉시 한 번 실행 후, interval > 0 이면 주기적으로 반복한다.
func (r *DLQReplayer) Start(ctx context.Context) {
	go func() {
		r.ReplayAll(ctx)
		if r.interval <= 0 {
			return
		}
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.ReplayAll(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// ReplayAll은 DLQ 디렉터리의 이전 날짜 JSONL 파일을 모두 재적재한다.
func (r *DLQReplayer) ReplayAll(ctx context.Context) {
	today := time.Now().UTC().Format("2006-01-02")

	entries, err := os.ReadDir(r.dir)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("dlq replay: read dir failed", "dir", r.dir, "err", err)
		}
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		// 오늘 날짜 파일은 현재 프로세스가 기록 중일 수 있으므로 건너뜀
		if strings.Contains(entry.Name(), today) {
			continue
		}
		path := filepath.Join(r.dir, entry.Name())
		if err := r.replayFile(ctx, path, entry.Name()); err != nil {
			slog.Warn("dlq replay: file failed", "file", entry.Name(), "err", err)
		}
	}
}

// dlqLineHeader는 DLQ 라인의 포맷을 판별하기 위한 최소 구조체다.
// error_reason 필드가 있으면 배치 포맷(새 형식), 없으면 단일 항목 포맷(구 형식)이다.
type dlqLineHeader struct {
	ErrorReason string          `json:"error_reason"`
	Batch       json.RawMessage `json:"batch"`
}

// replayFile은 단일 JSONL 파일을 읽어 재적재하고 성공 시 삭제한다.
// 각 라인은 구 형식(단일 항목 JSON) 또는 신 형식({"error_reason":"...","batch":[...]}) 중 하나다.
func (r *DLQReplayer) replayFile(ctx context.Context, path, name string) error {
	var signal string
	switch {
	case strings.HasPrefix(name, "traces-"):
		signal = "spans"
	case strings.HasPrefix(name, "metrics-"):
		signal = "metrics"
	case strings.HasPrefix(name, "logs-"):
		signal = "logs"
	default:
		return nil // 알 수 없는 파일 형식은 건너뜀
	}

	// 파일 읽기를 클로저로 격리해 defer f.Close()가 스캔 직후 실행되도록 한다.
	// os.Remove 시점에는 파일 핸들이 반드시 닫혀 있어야 한다 (Windows 호환 및 명시적 자원 해제).
	var (
		spans   []*model.SpanData
		metrics []*model.MetricData
		logs    []*model.LogData
	)
	if err := func() error {
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open: %w", err)
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024) // 최대 4 MiB 라인 허용

		switch signal {
		case "spans":
			for scanner.Scan() {
				line := scanner.Bytes()
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}
				var hdr dlqLineHeader
				if err := json.Unmarshal(line, &hdr); err == nil && hdr.ErrorReason != "" {
					// 신 형식: {"error_reason":"...","batch":[...]}
					var batch []*model.SpanData
					if err := json.Unmarshal(hdr.Batch, &batch); err != nil {
						slog.Warn("dlq replay: batch decode failed", "err", err)
						continue
					}
					spans = append(spans, batch...)
				} else {
					// 구 형식: 단일 SpanData JSON
					var sp model.SpanData
					if err := json.Unmarshal(line, &sp); err != nil {
						slog.Warn("dlq replay: span decode failed", "err", err)
						continue
					}
					spans = append(spans, &sp)
				}
			}
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("scan spans: %w", err)
			}

		case "metrics":
			for scanner.Scan() {
				line := scanner.Bytes()
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}
				var hdr dlqLineHeader
				if err := json.Unmarshal(line, &hdr); err == nil && hdr.ErrorReason != "" {
					var batch []*model.MetricData
					if err := json.Unmarshal(hdr.Batch, &batch); err != nil {
						slog.Warn("dlq replay: batch decode failed", "err", err)
						continue
					}
					metrics = append(metrics, batch...)
				} else {
					var m model.MetricData
					if err := json.Unmarshal(line, &m); err != nil {
						slog.Warn("dlq replay: metric decode failed", "err", err)
						continue
					}
					metrics = append(metrics, &m)
				}
			}
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("scan metrics: %w", err)
			}

		case "logs":
			for scanner.Scan() {
				line := scanner.Bytes()
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}
				var hdr dlqLineHeader
				if err := json.Unmarshal(line, &hdr); err == nil && hdr.ErrorReason != "" {
					var batch []*model.LogData
					if err := json.Unmarshal(hdr.Batch, &batch); err != nil {
						slog.Warn("dlq replay: batch decode failed", "err", err)
						continue
					}
					logs = append(logs, batch...)
				} else {
					var l model.LogData
					if err := json.Unmarshal(line, &l); err != nil {
						slog.Warn("dlq replay: log decode failed", "err", err)
						continue
					}
					logs = append(logs, &l)
				}
			}
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("scan logs: %w", err)
			}
		}
		return nil
	}(); err != nil {
		return err
	}

	// 이 시점에서 f는 이미 닫혀 있다 (클로저 종료 시 defer f.Close() 실행됨).
	var count int
	switch signal {
	case "spans":
		if len(spans) == 0 {
			return nil
		}
		if err := r.traceStore.AppendSpans(ctx, spans); err != nil {
			return fmt.Errorf("append spans: %w", err)
		}
		count = len(spans)
	case "metrics":
		if len(metrics) == 0 {
			return nil
		}
		if err := r.metricStore.AppendMetrics(ctx, metrics); err != nil {
			return fmt.Errorf("append metrics: %w", err)
		}
		count = len(metrics)
	case "logs":
		if len(logs) == 0 {
			return nil
		}
		if err := r.logStore.AppendLogs(ctx, logs); err != nil {
			return fmt.Errorf("append logs: %w", err)
		}
		count = len(logs)
	}

	if err := os.Remove(path); err != nil {
		slog.Warn("dlq replay: remove failed after replay", "path", path, "err", err)
		return nil
	}
	chDLQReplayedTotal.WithLabelValues(signal).Add(float64(count))
	slog.Info("dlq replay: file replayed and removed", "file", name, "count", count)
	return nil
}

// CleanupDLQFiles는 DLQ 디렉터리에서 retentionDays일보다 오래된 JSONL 파일을 삭제한다.
// retentionDays <= 0 이면 아무것도 하지 않는다.
//
// 파일명 형식: {signal}-YYYY-MM-DD.jsonl (signal: traces, metrics, logs)
func CleanupDLQFiles(dir string, retentionDays int) {
	if retentionDays <= 0 {
		return
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)

	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("dlq cleanup: read dir failed", "dir", dir, "err", err)
		}
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		// 파일명에서 날짜 파싱: {signal}-YYYY-MM-DD.jsonl
		base := strings.TrimSuffix(entry.Name(), ".jsonl") // e.g. "traces-2024-01-10"
		parts := strings.SplitN(base, "-", 2)              // ["traces", "2024-01-10"]
		if len(parts) != 2 {
			continue
		}
		fileDate, err := time.Parse("2006-01-02", parts[1])
		if err != nil {
			continue
		}
		if fileDate.Before(cutoff) {
			path := filepath.Join(dir, entry.Name())
			if err := os.Remove(path); err != nil {
				slog.Warn("dlq cleanup: remove failed", "path", path, "err", err)
			} else {
				chDLQCleanedFilesTotal.Inc()
				slog.Info("dlq cleanup: removed old file", "file", entry.Name())
			}
		}
	}
}
