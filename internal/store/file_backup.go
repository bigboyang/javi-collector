// Package store - file_backup.go
//
// FileBackupWriter는 OTLP 데이터를 수신 즉시 JSONL 파일로 백업한다.
//
// 파일 구조:
//
//	{BackupDir}/traces-2006-01-02.jsonl
//	{BackupDir}/metrics-2006-01-02.jsonl
//	{BackupDir}/logs-2006-01-02.jsonl
//
// 특징:
//   - 날짜(UTC)가 바뀌면 자동으로 새 파일로 전환(rotation)
//   - 파일은 O_APPEND|O_CREATE 모드로 열어 프로세스 재시작 후에도 이어쓰기
//   - 기록 실패는 경고 로그만 남기고 upstream store 호출을 막지 않는다
//   - BackupTraceStore / BackupMetricStore / BackupLogStore 는 기존 Store를
//     감싸는 데코레이터이므로 main.go의 변경이 최소화된다
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kkc/javi-collector/internal/model"
)

// FileBackupWriter는 signal별 JSONL 파일을 관리한다.
// 동시 접근에 안전하다.
type FileBackupWriter struct {
	dir string
	mu  sync.Mutex

	traceFile  *os.File
	metricFile *os.File
	logFile    *os.File

	traceEnc  *json.Encoder
	metricEnc *json.Encoder
	logEnc    *json.Encoder

	traceDate  string
	metricDate string
	logDate    string
}

// NewFileBackupWriter는 dir에 백업 디렉터리를 생성하고 Writer를 반환한다.
func NewFileBackupWriter(dir string) (*FileBackupWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("backup dir create: %w", err)
	}
	return &FileBackupWriter{dir: dir}, nil
}

// WriteSpans는 spans를 traces-YYYY-MM-DD.jsonl에 한 줄씩 기록한다.
func (w *FileBackupWriter) WriteSpans(spans []*model.SpanData) error {
	today := w.today()
	w.mu.Lock()
	defer w.mu.Unlock()

	enc, err := w.openFile("traces", today, &w.traceFile, &w.traceEnc, &w.traceDate)
	if err != nil {
		return err
	}
	return writeJSONL(enc, spans, func(enc *json.Encoder, v *model.SpanData) error {
		return enc.Encode(v)
	})
}

// WriteMetrics는 metrics를 metrics-YYYY-MM-DD.jsonl에 한 줄씩 기록한다.
func (w *FileBackupWriter) WriteMetrics(metrics []*model.MetricData) error {
	today := w.today()
	w.mu.Lock()
	defer w.mu.Unlock()

	enc, err := w.openFile("metrics", today, &w.metricFile, &w.metricEnc, &w.metricDate)
	if err != nil {
		return err
	}
	return writeJSONL(enc, metrics, func(enc *json.Encoder, v *model.MetricData) error {
		return enc.Encode(v)
	})
}

// WriteLogs는 logs를 logs-YYYY-MM-DD.jsonl에 한 줄씩 기록한다.
func (w *FileBackupWriter) WriteLogs(logs []*model.LogData) error {
	today := w.today()
	w.mu.Lock()
	defer w.mu.Unlock()

	enc, err := w.openFile("logs", today, &w.logFile, &w.logEnc, &w.logDate)
	if err != nil {
		return err
	}
	return writeJSONL(enc, logs, func(enc *json.Encoder, v *model.LogData) error {
		return enc.Encode(v)
	})
}

// dlqBatchEntry는 에러 사유와 배치 데이터를 포함하는 DLQ 항목이다.
// {"error_reason": "...", "batch": [...]} 형식으로 JSONL에 기록된다.
type dlqBatchEntry[T any] struct {
	ErrorReason string `json:"error_reason"`
	Batch       []T    `json:"batch"`
}

// WriteDLQSpans는 spans를 에러 사유와 함께 traces-YYYY-MM-DD.jsonl에 기록한다.
// 각 배치는 {"error_reason":"...","batch":[...]} 형식의 단일 JSON 라인으로 저장된다.
func (w *FileBackupWriter) WriteDLQSpans(spans []*model.SpanData, reason string) error {
	today := w.today()
	w.mu.Lock()
	defer w.mu.Unlock()

	enc, err := w.openFile("traces", today, &w.traceFile, &w.traceEnc, &w.traceDate)
	if err != nil {
		return err
	}
	entry := dlqBatchEntry[*model.SpanData]{ErrorReason: reason, Batch: spans}
	return enc.Encode(entry)
}

// WriteDLQMetrics는 metrics를 에러 사유와 함께 metrics-YYYY-MM-DD.jsonl에 기록한다.
func (w *FileBackupWriter) WriteDLQMetrics(metrics []*model.MetricData, reason string) error {
	today := w.today()
	w.mu.Lock()
	defer w.mu.Unlock()

	enc, err := w.openFile("metrics", today, &w.metricFile, &w.metricEnc, &w.metricDate)
	if err != nil {
		return err
	}
	entry := dlqBatchEntry[*model.MetricData]{ErrorReason: reason, Batch: metrics}
	return enc.Encode(entry)
}

// WriteDLQLogs는 logs를 에러 사유와 함께 logs-YYYY-MM-DD.jsonl에 기록한다.
func (w *FileBackupWriter) WriteDLQLogs(logs []*model.LogData, reason string) error {
	today := w.today()
	w.mu.Lock()
	defer w.mu.Unlock()

	enc, err := w.openFile("logs", today, &w.logFile, &w.logEnc, &w.logDate)
	if err != nil {
		return err
	}
	entry := dlqBatchEntry[*model.LogData]{ErrorReason: reason, Batch: logs}
	return enc.Encode(entry)
}

// Close는 열려있는 모든 백업 파일을 닫는다.
func (w *FileBackupWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var errs []error
	for _, fp := range []*os.File{w.traceFile, w.metricFile, w.logFile} {
		if fp != nil {
			if err := fp.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (w *FileBackupWriter) today() string {
	return time.Now().UTC().Format("2006-01-02")
}

// openFile은 signal-YYYY-MM-DD.jsonl 파일을 열거나 날짜 변경 시 새 파일로 교체한다.
// 파일과 함께 json.Encoder를 캐시하여 매 쓰기마다 Encoder를 재생성하는 비용을 제거한다.
// 호출자가 w.mu를 보유해야 한다.
func (w *FileBackupWriter) openFile(signal, today string, fp **os.File, ep **json.Encoder, date *string) (*json.Encoder, error) {
	if *fp != nil && *date == today {
		return *ep, nil
	}
	// 날짜가 변경됐으면 이전 파일 닫기
	if *fp != nil {
		_ = (*fp).Close()
		*fp = nil
		*ep = nil
	}
	path := filepath.Join(w.dir, fmt.Sprintf("%s-%s.jsonl", signal, today))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open backup file %s: %w", path, err)
	}
	*fp = f
	*ep = json.NewEncoder(f)
	*date = today
	slog.Debug("backup file opened", "path", path)
	return *ep, nil
}

// writeJSONL은 슬라이스의 각 원소를 JSON 한 줄로 enc에 기록한다.
// Encoder는 호출자가 캐시해서 전달하므로 매 호출마다 생성하지 않는다.
func writeJSONL[T any](enc *json.Encoder, items []T, encode func(*json.Encoder, T) error) error {
	for _, item := range items {
		if err := encode(enc, item); err != nil {
			return err
		}
	}
	return nil
}

// ---- 데코레이터 Store ----

// BackupTraceStore는 TraceStore를 감싸서 Append 시 JSONL 백업을 남긴다.
// 백업 쓰기 실패는 경고 로그만 남기고 upstream에는 영향을 주지 않는다.
type BackupTraceStore struct {
	inner  TraceStore
	writer *FileBackupWriter
}

// NewBackupTraceStore는 inner TraceStore를 감싸는 BackupTraceStore를 반환한다.
func NewBackupTraceStore(inner TraceStore, writer *FileBackupWriter) *BackupTraceStore {
	return &BackupTraceStore{inner: inner, writer: writer}
}

func (s *BackupTraceStore) AppendSpans(ctx context.Context, spans []*model.SpanData) error {
	if err := s.writer.WriteSpans(spans); err != nil {
		slog.Warn("trace backup write failed", "err", err)
	}
	return s.inner.AppendSpans(ctx, spans)
}

func (s *BackupTraceStore) QuerySpans(ctx context.Context, q SpanQuery) ([]*model.SpanData, error) {
	return s.inner.QuerySpans(ctx, q)
}

func (s *BackupTraceStore) Close() error {
	return s.inner.Close()
}

// BackupMetricStore는 MetricStore를 감싸서 Append 시 JSONL 백업을 남긴다.
type BackupMetricStore struct {
	inner  MetricStore
	writer *FileBackupWriter
}

// NewBackupMetricStore는 inner MetricStore를 감싸는 BackupMetricStore를 반환한다.
func NewBackupMetricStore(inner MetricStore, writer *FileBackupWriter) *BackupMetricStore {
	return &BackupMetricStore{inner: inner, writer: writer}
}

func (s *BackupMetricStore) AppendMetrics(ctx context.Context, metrics []*model.MetricData) error {
	if err := s.writer.WriteMetrics(metrics); err != nil {
		slog.Warn("metric backup write failed", "err", err)
	}
	return s.inner.AppendMetrics(ctx, metrics)
}

func (s *BackupMetricStore) QueryMetrics(ctx context.Context, q MetricQuery) ([]*model.MetricData, error) {
	return s.inner.QueryMetrics(ctx, q)
}

func (s *BackupMetricStore) Close() error {
	return s.inner.Close()
}

// BackupLogStore는 LogStore를 감싸서 Append 시 JSONL 백업을 남긴다.
type BackupLogStore struct {
	inner  LogStore
	writer *FileBackupWriter
}

// NewBackupLogStore는 inner LogStore를 감싸는 BackupLogStore를 반환한다.
func NewBackupLogStore(inner LogStore, writer *FileBackupWriter) *BackupLogStore {
	return &BackupLogStore{inner: inner, writer: writer}
}

func (s *BackupLogStore) AppendLogs(ctx context.Context, logs []*model.LogData) error {
	if err := s.writer.WriteLogs(logs); err != nil {
		slog.Warn("log backup write failed", "err", err)
	}
	return s.inner.AppendLogs(ctx, logs)
}

func (s *BackupLogStore) QueryLogs(ctx context.Context, q LogQuery) ([]*model.LogData, error) {
	return s.inner.QueryLogs(ctx, q)
}

func (s *BackupLogStore) Close() error {
	return s.inner.Close()
}
