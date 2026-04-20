// Package store - wal.go
//
// WAL (Write-Ahead Log)은 MemoryStore 크래시 복구를 위한 JSONL 기반 퍼시스턴스 레이어다.
//
// 동작 방식:
//  1. AppendSpans/AppendMetrics/AppendLogs 호출 시 WAL 파일에 먼저 기록(write-ahead)
//  2. 프로세스 재시작 시 WAL 파일을 읽어 링버퍼에 재생(recovery)
//  3. WAL 파일이 MaxBytes를 초과하면 현재 링버퍼 스냅샷으로 교체(compact)
//     — .tmp 파일로 쓰고 atomic rename해 데이터 손실 없이 교체
//
// 파일 레이아웃:
//   {WALDir}/wal-traces.jsonl
//   {WALDir}/wal-metrics.jsonl
//   {WALDir}/wal-logs.jsonl
//
// 장애 모드:
//   - WAL 쓰기 실패 → 경고 로그만 남기고 메모리 저장은 계속 (graceful degradation)
//   - recovery 파싱 오류 → 해당 줄만 건너뛰고 계속 (best-effort)
//   - compact 실패 → 경고 로그, 다음 주기에 재시도
package store

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/kkc/javi-collector/internal/model"
)

const walDefaultMaxBytes = 64 << 20 // 64 MiB

// WALConfig는 WAL 설정을 담는다.
type WALConfig struct {
	Dir      string // WAL 파일 저장 디렉터리
	MaxBytes int64  // 이 크기 초과 시 compact 수행 (기본 64 MiB)
}

// walFile은 단일 signal의 WAL 파일을 관리한다.
// 모든 메서드는 mu를 통해 스레드 안전하다.
type walFile struct {
	mu       sync.Mutex
	path     string
	maxBytes int64
	f        *os.File
	enc      *json.Encoder
}

func openWALFile(path string, maxBytes int64) (*walFile, error) {
	if maxBytes <= 0 {
		maxBytes = walDefaultMaxBytes
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open WAL %s: %w", path, err)
	}
	return &walFile{
		path:     path,
		maxBytes: maxBytes,
		f:        f,
		enc:      json.NewEncoder(f),
	}, nil
}

// write는 v를 JSON 한 줄로 WAL에 기록한다.
func (w *walFile) write(v any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(v)
}

// needsCompact는 WAL 파일이 maxBytes를 초과했는지 확인한다.
func (w *walFile) needsCompact() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.f.Stat()
	if err != nil {
		return false
	}
	return info.Size() >= w.maxBytes
}

// compact는 items를 새 WAL 파일로 atomic rename하여 교체한다.
// 기존 WAL의 과거 항목을 버리고 현재 링버퍼 스냅샷만 유지한다.
func (w *walFile) compact(items []any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tmp := w.path + ".tmp"
	tf, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("wal compact create tmp: %w", err)
	}
	enc := json.NewEncoder(tf)
	for _, item := range items {
		if err := enc.Encode(item); err != nil {
			_ = tf.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("wal compact encode: %w", err)
		}
	}
	if err := tf.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("wal compact close tmp: %w", err)
	}
	if err := os.Rename(tmp, w.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("wal compact rename: %w", err)
	}

	// rename 후 기존 fd는 무효 → 새로 열기
	_ = w.f.Close()
	f2, err := os.OpenFile(w.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal reopen after compact: %w", err)
	}
	w.f = f2
	w.enc = json.NewEncoder(f2)
	return nil
}

func (w *walFile) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		err := w.f.Close()
		w.f = nil
		return err
	}
	return nil
}

// ── recovery helpers ──────────────────────────────────────────────────────────

func recoverSpans(path string) ([]*model.SpanData, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var result []*model.SpanData
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4<<20), 4<<20) // 4 MiB per line
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var sp model.SpanData
		if err := json.Unmarshal(line, &sp); err != nil {
			slog.Warn("wal trace recovery: skip bad line", "err", err)
			continue
		}
		result = append(result, &sp)
	}
	return result, sc.Err()
}

func recoverMetrics(path string) ([]*model.MetricData, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var result []*model.MetricData
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4<<20), 4<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var m model.MetricData
		if err := json.Unmarshal(line, &m); err != nil {
			slog.Warn("wal metric recovery: skip bad line", "err", err)
			continue
		}
		result = append(result, &m)
	}
	return result, sc.Err()
}

func recoverLogs(path string) ([]*model.LogData, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var result []*model.LogData
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4<<20), 4<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var l model.LogData
		if err := json.Unmarshal(line, &l); err != nil {
			slog.Warn("wal log recovery: skip bad line", "err", err)
			continue
		}
		result = append(result, &l)
	}
	return result, sc.Err()
}

// ── WALTraceStore ─────────────────────────────────────────────────────────────

// WALTraceStore는 MemoryTraceStore를 감싸 WAL 쓰기와 시작 시 복구를 제공한다.
type WALTraceStore struct {
	inner *MemoryTraceStore
	wal   *walFile
}

// NewWALTraceStore는 WAL을 열고, 기존 데이터를 inner에 복구한 뒤 데코레이터를 반환한다.
func NewWALTraceStore(inner *MemoryTraceStore, cfg WALConfig) (*WALTraceStore, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal dir create: %w", err)
	}
	path := filepath.Join(cfg.Dir, "wal-traces.jsonl")

	// 1. 복구
	spans, err := recoverSpans(path)
	if err != nil {
		slog.Warn("wal trace recovery failed", "err", err)
	}
	if len(spans) > 0 {
		ctx := context.Background()
		if err := inner.AppendSpans(ctx, spans); err != nil {
			slog.Warn("wal trace replay failed", "err", err)
		} else {
			slog.Info("wal trace recovered", "count", len(spans))
		}
	}

	// 2. WAL 파일 열기
	wf, err := openWALFile(path, cfg.MaxBytes)
	if err != nil {
		return nil, err
	}

	// 3. 복구 직후 compact: 링버퍼 초과분은 이미 evict됐으므로 현재 상태로 교체
	if len(spans) > 0 {
		snapshot := inner.Snapshot()
		items := make([]any, len(snapshot))
		for i, s := range snapshot {
			items[i] = s
		}
		if err := wf.compact(items); err != nil {
			slog.Warn("wal trace initial compact failed", "err", err)
		}
	}

	return &WALTraceStore{inner: inner, wal: wf}, nil
}

func (s *WALTraceStore) AppendSpans(ctx context.Context, spans []*model.SpanData) error {
	// WAL 먼저 기록 (write-ahead): 크래시 후 복구 가능성 확보
	for _, sp := range spans {
		if err := s.wal.write(sp); err != nil {
			slog.Warn("wal trace write failed", "err", err)
			break // partial write는 recovery에서 처리됨
		}
	}

	if err := s.inner.AppendSpans(ctx, spans); err != nil {
		return err
	}

	// 크기 초과 시 비동기 compact
	if s.wal.needsCompact() {
		go s.compactAsync()
	}
	return nil
}

func (s *WALTraceStore) compactAsync() {
	snapshot := s.inner.Snapshot()
	items := make([]any, len(snapshot))
	for i, sp := range snapshot {
		items[i] = sp
	}
	if err := s.wal.compact(items); err != nil {
		slog.Warn("wal trace compact failed", "err", err)
	} else {
		slog.Debug("wal trace compacted", "items", len(items))
	}
}

func (s *WALTraceStore) QuerySpans(ctx context.Context, q SpanQuery) ([]*model.SpanData, error) {
	return s.inner.QuerySpans(ctx, q)
}

func (s *WALTraceStore) Close() error {
	return s.wal.close()
}

func (s *WALTraceStore) Size() int { return s.inner.Size() }

// ── WALMetricStore ────────────────────────────────────────────────────────────

// WALMetricStore는 MemoryMetricStore를 감싸 WAL 쓰기와 시작 시 복구를 제공한다.
type WALMetricStore struct {
	inner *MemoryMetricStore
	wal   *walFile
}

// NewWALMetricStore는 WAL을 열고, 기존 데이터를 inner에 복구한 뒤 데코레이터를 반환한다.
func NewWALMetricStore(inner *MemoryMetricStore, cfg WALConfig) (*WALMetricStore, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal dir create: %w", err)
	}
	path := filepath.Join(cfg.Dir, "wal-metrics.jsonl")

	metrics, err := recoverMetrics(path)
	if err != nil {
		slog.Warn("wal metric recovery failed", "err", err)
	}
	if len(metrics) > 0 {
		ctx := context.Background()
		if err := inner.AppendMetrics(ctx, metrics); err != nil {
			slog.Warn("wal metric replay failed", "err", err)
		} else {
			slog.Info("wal metric recovered", "count", len(metrics))
		}
	}

	wf, err := openWALFile(path, cfg.MaxBytes)
	if err != nil {
		return nil, err
	}

	if len(metrics) > 0 {
		snapshot := inner.Snapshot()
		items := make([]any, len(snapshot))
		for i, m := range snapshot {
			items[i] = m
		}
		if err := wf.compact(items); err != nil {
			slog.Warn("wal metric initial compact failed", "err", err)
		}
	}

	return &WALMetricStore{inner: inner, wal: wf}, nil
}

func (s *WALMetricStore) AppendMetrics(ctx context.Context, metrics []*model.MetricData) error {
	for _, m := range metrics {
		if err := s.wal.write(m); err != nil {
			slog.Warn("wal metric write failed", "err", err)
			break
		}
	}

	if err := s.inner.AppendMetrics(ctx, metrics); err != nil {
		return err
	}

	if s.wal.needsCompact() {
		go s.compactAsync()
	}
	return nil
}

func (s *WALMetricStore) compactAsync() {
	snapshot := s.inner.Snapshot()
	items := make([]any, len(snapshot))
	for i, m := range snapshot {
		items[i] = m
	}
	if err := s.wal.compact(items); err != nil {
		slog.Warn("wal metric compact failed", "err", err)
	} else {
		slog.Debug("wal metric compacted", "items", len(items))
	}
}

func (s *WALMetricStore) QueryMetrics(ctx context.Context, q MetricQuery) ([]*model.MetricData, error) {
	return s.inner.QueryMetrics(ctx, q)
}

func (s *WALMetricStore) Close() error {
	return s.wal.close()
}

func (s *WALMetricStore) Size() int { return s.inner.Size() }

// ── WALLogStore ───────────────────────────────────────────────────────────────

// WALLogStore는 MemoryLogStore를 감싸 WAL 쓰기와 시작 시 복구를 제공한다.
type WALLogStore struct {
	inner *MemoryLogStore
	wal   *walFile
}

// NewWALLogStore는 WAL을 열고, 기존 데이터를 inner에 복구한 뒤 데코레이터를 반환한다.
func NewWALLogStore(inner *MemoryLogStore, cfg WALConfig) (*WALLogStore, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal dir create: %w", err)
	}
	path := filepath.Join(cfg.Dir, "wal-logs.jsonl")

	logs, err := recoverLogs(path)
	if err != nil {
		slog.Warn("wal log recovery failed", "err", err)
	}
	if len(logs) > 0 {
		ctx := context.Background()
		if err := inner.AppendLogs(ctx, logs); err != nil {
			slog.Warn("wal log replay failed", "err", err)
		} else {
			slog.Info("wal log recovered", "count", len(logs))
		}
	}

	wf, err := openWALFile(path, cfg.MaxBytes)
	if err != nil {
		return nil, err
	}

	if len(logs) > 0 {
		snapshot := inner.Snapshot()
		items := make([]any, len(snapshot))
		for i, l := range snapshot {
			items[i] = l
		}
		if err := wf.compact(items); err != nil {
			slog.Warn("wal log initial compact failed", "err", err)
		}
	}

	return &WALLogStore{inner: inner, wal: wf}, nil
}

func (s *WALLogStore) AppendLogs(ctx context.Context, logs []*model.LogData) error {
	for _, l := range logs {
		if err := s.wal.write(l); err != nil {
			slog.Warn("wal log write failed", "err", err)
			break
		}
	}

	if err := s.inner.AppendLogs(ctx, logs); err != nil {
		return err
	}

	if s.wal.needsCompact() {
		go s.compactAsync()
	}
	return nil
}

func (s *WALLogStore) compactAsync() {
	snapshot := s.inner.Snapshot()
	items := make([]any, len(snapshot))
	for i, l := range snapshot {
		items[i] = l
	}
	if err := s.wal.compact(items); err != nil {
		slog.Warn("wal log compact failed", "err", err)
	} else {
		slog.Debug("wal log compacted", "items", len(items))
	}
}

func (s *WALLogStore) QueryLogs(ctx context.Context, q LogQuery) ([]*model.LogData, error) {
	return s.inner.QueryLogs(ctx, q)
}

func (s *WALLogStore) Close() error {
	return s.wal.close()
}

func (s *WALLogStore) Size() int { return s.inner.Size() }
