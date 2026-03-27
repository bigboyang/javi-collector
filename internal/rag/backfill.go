// Package rag - Historical Backfill: ClickHouse 과거 ERROR spans를 Qdrant에 적재한다.
//
// 목적:
//   - 기동 직후 Qdrant가 비어 있어 RAG 유사 사례 검색이 불가한 문제를 해결한다.
//   - ClickHouse의 retention 범위 내 ERROR spans를 backfill해 Knowledge Base를 초기화한다.
//
// 동작:
//   - 백그라운드 goroutine으로 비동기 실행 (기동 블로킹 없음)
//   - Qdrant upsert 방식이므로 중복 적재는 자동 overwrite (idempotent)
//   - ClickHouse에서 batchSize 단위로 페이지네이션해 대용량 처리
//   - ctx 취소 시 즉시 중단 (graceful shutdown 보장)
//
// 체크포인트:
//   - 완료 시 {checkpointFile}에 완료 시각(Unix ms)을 기록한다.
//   - 재기동 시 체크포인트가 설정 윈도우 내에 있으면 체크포인트 이후 span만 처리한다.
//   - 중간 중단(ctx 취소, 오류) 시 체크포인트를 갱신하지 않으므로 다음 재기동에서 재처리된다.
package rag

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/kkc/javi-collector/internal/store"
)

// backfillCheckpoint는 체크포인트 파일의 JSON 구조다.
type backfillCheckpoint struct {
	CompletedAtMs int64  `json:"completed_at_ms"` // 마지막 완료 시각 (Unix ms)
	CompletedAt   string `json:"completed_at"`    // 사람이 읽기 쉬운 RFC3339 시각
}

// HistoricalBackfiller는 ClickHouse의 과거 ERROR spans를 Qdrant에 적재한다.
// RAG Knowledge Base 초기화 용도로 기동 시 1회 실행한다.
type HistoricalBackfiller struct {
	traceStore     store.TraceStore
	pipeline       *EmbedPipeline
	builder        *DocumentBuilder
	days           int
	batchSize      int
	checkpointFile string // 체크포인트 파일 경로 (빈 문자열이면 비활성화)
}

// NewHistoricalBackfiller는 HistoricalBackfiller를 생성한다.
//
//   - ts: ClickHouse TraceStore (QuerySpans로 과거 ERROR spans를 페이지네이션 조회)
//   - pipeline: EmbedPipeline (임베딩 후 Qdrant에 적재)
//   - days: 과거 몇 일치를 적재할지 (예: 7이면 최근 7일)
//   - batchSize: 한 번에 ClickHouse에서 읽을 span 수 (예: 50)
//   - checkpointFile: 체크포인트 파일 경로 (빈 문자열이면 비활성화)
func NewHistoricalBackfiller(ts store.TraceStore, pipeline *EmbedPipeline, days, batchSize int, checkpointFile string) *HistoricalBackfiller {
	if days <= 0 {
		days = 7
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	return &HistoricalBackfiller{
		traceStore:     ts,
		pipeline:       pipeline,
		builder:        &DocumentBuilder{},
		days:           days,
		batchSize:      batchSize,
		checkpointFile: checkpointFile,
	}
}

// Run은 백그라운드 goroutine으로 과거 ERROR spans를 Qdrant에 적재한다.
// ctx가 취소되면 현재 배치 완료 후 종료한다.
// 완료 시 체크포인트를 저장해 다음 재기동에서 중복 처리를 방지한다.
func (b *HistoricalBackfiller) Run(ctx context.Context) {
	go func() {
		configuredFromMs := time.Now().AddDate(0, 0, -b.days).UnixMilli()
		fromMs := b.resolveFromMs(configuredFromMs)
		offset := 0
		processed := 0
		skipped := 0

		slog.Info("RAG historical backfill started",
			"from", time.UnixMilli(fromMs).UTC().Format(time.RFC3339),
			"checkpoint_active", fromMs > configuredFromMs,
			"batch_size", b.batchSize,
		)

		for {
			select {
			case <-ctx.Done():
				slog.Info("RAG backfill cancelled",
					"processed", processed, "skipped", skipped, "offset", offset)
				return
			default:
			}

			spans, err := b.traceStore.QuerySpans(ctx, store.SpanQuery{
				Limit:      b.batchSize,
				Offset:     offset,
				FromMs:     fromMs,
				StatusCode: 2, // ERROR spans only
			})
			if err != nil {
				slog.Error("RAG backfill query failed",
					"err", err, "offset", offset, "processed_so_far", processed)
				return
			}
			if len(spans) == 0 {
				break
			}

			for _, sp := range spans {
				doc := b.builder.BuildFromSpan(sp, nil)
				if doc == nil {
					skipped++
					continue
				}
				b.pipeline.Submit(doc)
				processed++
			}

			offset += len(spans)

			// 마지막 배치이면 종료
			if len(spans) < b.batchSize {
				break
			}
		}

		slog.Info("RAG historical backfill completed",
			"processed", processed, "skipped", skipped)

		// 정상 완료 시에만 체크포인트 저장
		b.saveCheckpoint(time.Now().UnixMilli())
	}()
}

// resolveFromMs는 체크포인트와 설정 윈도우를 비교해 실제 시작 시각(Unix ms)을 반환한다.
// 체크포인트가 설정 윈도우 내에 있으면 체크포인트 이후 span만 처리하도록 시작점을 조정한다.
func (b *HistoricalBackfiller) resolveFromMs(configuredFromMs int64) int64 {
	cp := b.loadCheckpoint()
	if cp <= configuredFromMs {
		// 체크포인트가 없거나 설정 윈도우보다 오래됐으면 전체 윈도우 처리
		return configuredFromMs
	}
	slog.Info("RAG backfill resuming from checkpoint",
		"checkpoint", time.UnixMilli(cp).UTC().Format(time.RFC3339),
		"skipping_days", float64(cp-configuredFromMs)/float64(24*time.Hour/time.Millisecond),
	)
	return cp
}

// loadCheckpoint는 체크포인트 파일에서 마지막 완료 시각(Unix ms)을 읽는다.
// 파일이 없거나 읽기 실패 시 0을 반환한다.
func (b *HistoricalBackfiller) loadCheckpoint() int64 {
	if b.checkpointFile == "" {
		return 0
	}
	data, err := os.ReadFile(b.checkpointFile)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("RAG backfill checkpoint read failed", "file", b.checkpointFile, "err", err)
		}
		return 0
	}
	var cp backfillCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		slog.Warn("RAG backfill checkpoint parse failed", "file", b.checkpointFile, "err", err)
		return 0
	}
	return cp.CompletedAtMs
}

// saveCheckpoint는 완료 시각을 체크포인트 파일에 저장한다.
// 저장 실패는 경고 로그만 남기고 무시한다 (다음 재기동에서 재처리됨).
func (b *HistoricalBackfiller) saveCheckpoint(completedAtMs int64) {
	if b.checkpointFile == "" {
		return
	}
	cp := backfillCheckpoint{
		CompletedAtMs: completedAtMs,
		CompletedAt:   time.UnixMilli(completedAtMs).UTC().Format(time.RFC3339),
	}
	data, err := json.Marshal(cp)
	if err != nil {
		slog.Warn("RAG backfill checkpoint marshal failed", "err", err)
		return
	}
	// 디렉터리가 없으면 생성
	if dir := filepath.Dir(b.checkpointFile); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			slog.Warn("RAG backfill checkpoint dir create failed", "dir", dir, "err", err)
			return
		}
	}
	if err := os.WriteFile(b.checkpointFile, data, 0o644); err != nil {
		slog.Warn("RAG backfill checkpoint write failed", "file", b.checkpointFile, "err", err)
		return
	}
	slog.Info("RAG backfill checkpoint saved", "file", b.checkpointFile,
		"completed_at", cp.CompletedAt)
}
