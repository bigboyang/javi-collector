// Package config - hotreload.go
//
// HotReloader는 JSON 설정 파일을 주기적으로 폴링해 런타임 설정을 동적으로 변경한다.
//
// 지원 설정 항목:
//   - batch_size: 배치 최대 레코드 수
//   - flush_interval: 타이머 기반 flush 주기 (e.g. "2s", "500ms")
//
// 사용 예:
//
//	hr := config.NewHotReloader("/etc/javi/dynamic.json", 30*time.Second)
//	hr.OnChange(func(dc config.DynamicConfig) {
//	    traceStore.SetDynamicConfig(dc.BatchSize, dc.FlushInterval)
//	})
//	hr.Start(ctx)
//
// 설정 파일 형식 (JSON):
//
//	{"batch_size": 2000, "flush_interval": "5s"}
package config

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"time"
)

// DynamicConfig는 핫 리로드 가능한 런타임 설정 항목이다.
// 값이 0이면 해당 항목은 무시(변경 없음)된다.
type DynamicConfig struct {
	BatchSize     int           `json:"batch_size"`
	FlushInterval time.Duration `json:"-"` // 파싱 후 FlushIntervalStr에서 변환
	// FlushIntervalStr은 JSON 파싱용 문자열 (e.g. "2s", "500ms")
	FlushIntervalStr string `json:"flush_interval"`
}

// HotReloader는 파일 기반 폴링으로 설정을 동적으로 갱신한다.
type HotReloader struct {
	path     string
	interval time.Duration

	mu        sync.RWMutex
	current   DynamicConfig
	modTime   time.Time
	callbacks []func(DynamicConfig)
}

// NewHotReloader는 path 파일을 interval마다 폴링하는 HotReloader를 반환한다.
// interval이 0이면 기본값 30초가 적용된다.
func NewHotReloader(path string, interval time.Duration) *HotReloader {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &HotReloader{
		path:     path,
		interval: interval,
	}
}

// OnChange는 설정이 변경될 때 호출될 콜백을 등록한다.
// Start() 호출 전에 등록해야 초기 로드 시에도 콜백이 실행된다.
func (h *HotReloader) OnChange(fn func(DynamicConfig)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.callbacks = append(h.callbacks, fn)
}

// Current는 현재 로드된 DynamicConfig를 반환한다.
func (h *HotReloader) Current() DynamicConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.current
}

// Start는 백그라운드 폴링 고루틴을 시작한다.
// 즉시 파일을 한 번 로드한 뒤 interval마다 반복한다.
// ctx가 취소되면 폴링을 멈춘다.
func (h *HotReloader) Start(ctx context.Context) {
	go func() {
		h.reload() // 즉시 1회 로드
		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				h.reload()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// reload는 파일의 수정 시각을 확인하고 변경된 경우에만 파싱 후 콜백을 호출한다.
func (h *HotReloader) reload() {
	info, err := os.Stat(h.path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("hotreload: stat failed", "path", h.path, "err", err)
		}
		return
	}

	h.mu.RLock()
	unchanged := info.ModTime().Equal(h.modTime)
	h.mu.RUnlock()
	if unchanged {
		return
	}

	data, err := os.ReadFile(h.path)
	if err != nil {
		slog.Warn("hotreload: read failed", "path", h.path, "err", err)
		return
	}

	var dc DynamicConfig
	if err := json.Unmarshal(data, &dc); err != nil {
		slog.Warn("hotreload: parse failed", "path", h.path, "err", err)
		return
	}

	// FlushIntervalStr → FlushInterval 변환
	if dc.FlushIntervalStr != "" {
		d, err := time.ParseDuration(dc.FlushIntervalStr)
		if err != nil {
			slog.Warn("hotreload: invalid flush_interval", "value", dc.FlushIntervalStr, "err", err)
		} else {
			dc.FlushInterval = d
		}
	}

	h.mu.Lock()
	h.current = dc
	h.modTime = info.ModTime()
	cbs := make([]func(DynamicConfig), len(h.callbacks))
	copy(cbs, h.callbacks)
	h.mu.Unlock()

	slog.Info("hotreload: config reloaded",
		"path", h.path,
		"batch_size", dc.BatchSize,
		"flush_interval", dc.FlushInterval,
	)

	for _, fn := range cbs {
		fn(dc)
	}
}
