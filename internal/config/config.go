package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config는 컬렉터 전체 설정을 담는다.
// 환경변수로 오버라이드 가능하며, 지정하지 않으면 기본값을 사용한다.
type Config struct {
	// gRPC OTLP 수신 포트 (OTLP/gRPC)
	GRPCPort int

	// HTTP OTLP + REST API 포트 (OTLP/HTTP + /api/collector/*)
	HTTPPort int

	// ClickHouse 연결 주소 (host:port)
	ClickHouseAddr string

	// ClickHouse 인증 정보
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string

	// 배치 설정
	BatchSize     int           // 한 번에 flush할 최대 레코드 수
	FlushInterval time.Duration // 타이머 기반 flush 주기

	// 채널 버퍼 깊이 (backpressure 조절)
	ChannelBufferSize int

	// 인메모리 링버퍼 fallback 용량
	MemoryBufferSize int

	// ClickHouse 비활성화 플래그 (테스트/개발용)
	DisableClickHouse bool

	// Sampling 설정
	// SamplingEnabled=false이면 TailSamplingStore가 전량 통과(no-op) 모드로 동작한다.
	SamplingEnabled bool

	// RemoteConfigURL: SamplingConfig를 폴링할 HTTP 엔드포인트 URL.
	// 빈 문자열이면 폴링하지 않고 SamplingEnabled 기반 정적 config를 유지한다.
	RemoteConfigURL string

	// RemoteConfigPollInterval: remote config 폴링 주기 (기본 30s)
	RemoteConfigPollInterval time.Duration

	// RetentionDays: ClickHouse 데이터 보관 기간 (일). TTL로 적용된다.
	RetentionDays int

	// RAG 파이프라인 설정 (EMBED_ENABLED=true 시 활성화)
	EmbedEnabled     bool
	EmbedEndpoint    string // Ollama: http://localhost:11434
	EmbedModel       string // e.g. nomic-embed-text
	QdrantEndpoint   string // e.g. http://localhost:6333
	QdrantCollection string

	// 파일 백업 설정 (BACKUP_ENABLED=true 시 활성화)
	// 수신된 trace/metric/log를 JSONL 파일로 백업한다.
	BackupEnabled bool
	BackupDir     string // 백업 파일 저장 디렉터리 (기본: ./backup)

	// DLQDir: ClickHouse flush 실패 시 배치를 보존할 Dead Letter Queue 디렉터리.
	// 비어 있으면 DLQ 비활성화 (flush 실패 데이터 유실 허용).
	DLQDir string

	// DLQ 보관 기간 (일). 이 기간보다 오래된 JSONL 파일은 자동 삭제된다.
	// 0 이면 자동 삭제 비활성화.
	DLQRetentionDays int

	// DLQ 자동 재적재 주기. 기동 시 한 번 실행 후 이 간격으로 반복한다.
	// 0 이면 기동 시 한 번만 실행.
	DLQReplayInterval time.Duration

	// Circuit Breaker: 연속 flush 실패 시 ClickHouse flush를 일시 차단한다.
	// CBFailureThreshold=0 이면 비활성화.
	CBFailureThreshold int
	CBCooldown         time.Duration // Open 상태 유지 시간 (기본 60s)

	// FlushWorkers: 테이블별 ClickHouse flush 병렬 worker 수.
	// 1이면 단일 직렬 flush (이전 동작), N이면 N개 goroutine이 동시에 flush한다.
	// 기본값 2. 고부하 환경에서 4-8로 늘리면 쓰기 병목을 해소할 수 있다.
	FlushWorkers int

	// SelfURL: 현재 인스턴스의 HTTP base URL (멀티 인스턴스 Tail Sampling 라우팅용).
	// e.g., "http://collector-0:4318"
	// 비어 있으면 TraceRouter가 비활성화된다.
	SelfURL string

	// PeerURLs: 다른 컬렉터 인스턴스들의 HTTP base URL 목록 (콤마 구분).
	// SAMPLING_ENABLED=true이고 SelfURL이 설정된 경우에만 라우팅이 활성화된다.
	// e.g., "http://collector-1:4318,http://collector-2:4318"
	PeerURLs []string

	// PeerCBFailureThreshold: 피어 전달 연속 실패 횟수 임계값.
	// 이 횟수만큼 연속 실패하면 해당 피어로의 전달을 일시 차단한다.
	// 0이면 Circuit Breaker 비활성화. (기본 5)
	PeerCBFailureThreshold int

	// PeerCBCooldown: 피어 Circuit Breaker open 유지 시간. (기본 30s)
	PeerCBCooldown time.Duration

	// AIOps Phase 1: RED Baseline 자동 집계
	// BaselineEnabled=true이면 BaselineComputer 고루틴이 BaselineInterval마다
	// red_baseline 테이블을 갱신한다. DisableClickHouse=true이면 무시된다.
	BaselineEnabled  bool
	BaselineInterval time.Duration

	// AIOps Phase 2: Z-score + IsolationForest 이상 탐지
	// AnomalyEnabled=true이면 Detector 고루틴이 AnomalyInterval마다
	// mv_red_1m_state와 red_baseline을 비교해 anomalies 테이블에 기록한다.
	AnomalyEnabled       bool
	AnomalyInterval      time.Duration // 탐지 주기 (기본 1m)
	AnomalyTrainInterval time.Duration // IForest 재학습 주기 (기본 6h)
	AnomalyNTrees        int           // IForest 트리 수 (기본 100)
	AnomalyMaxSamples    int           // IForest 부분집합 크기 (기본 256)
	AnomalyZWarn         float64       // Z-score 경고 임계값 (기본 2.0)
	AnomalyZCritical     float64       // Z-score 위험 임계값 (기본 3.0)
	AnomalyIFThreshold   float64       // IForest 이상 점수 임계값 (기본 0.65)
}

// Load는 환경변수에서 설정을 읽어 Config를 반환한다.
// 필수 항목이 없으면 기본값을 사용하므로 항상 유효한 설정이 반환된다.
func Load() (*Config, error) {
	cfg := &Config{
		GRPCPort:          envInt("GRPC_PORT", 4317),
		HTTPPort:          envInt("HTTP_PORT", 4318),
		ClickHouseAddr:    envStr("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickHouseDB:      envStr("CLICKHOUSE_DB", "apm"),
		ClickHouseUser:    envStr("CLICKHOUSE_USER", "default"),
		ClickHousePassword: envStr("CLICKHOUSE_PASSWORD", ""),
		BatchSize:         envInt("BATCH_SIZE", 1000),
		FlushInterval:     envDuration("FLUSH_INTERVAL", 2*time.Second),
		ChannelBufferSize: envInt("CHANNEL_BUFFER_SIZE", 8192),
		MemoryBufferSize:  envInt("MEMORY_BUFFER_SIZE", 10000),
		DisableClickHouse:        envBool("DISABLE_CLICKHOUSE", false),
		SamplingEnabled:          envBool("SAMPLING_ENABLED", false),
		RemoteConfigURL:          envStr("REMOTE_CONFIG_URL", ""),
		RemoteConfigPollInterval: envDuration("REMOTE_CONFIG_POLL_INTERVAL", 30*time.Second),
		RetentionDays:            envInt("RETENTION_DAYS", 30),
		EmbedEnabled:             envBool("EMBED_ENABLED", false),
		EmbedEndpoint:            envStr("EMBED_ENDPOINT", "http://localhost:11434"),
		EmbedModel:               envStr("EMBED_MODEL", "nomic-embed-text"),
		QdrantEndpoint:           envStr("QDRANT_ENDPOINT", "http://localhost:6333"),
		QdrantCollection:         envStr("QDRANT_COLLECTION", "apm_errors"),
		BackupEnabled:            envBool("BACKUP_ENABLED", true),
		BackupDir:                envStr("BACKUP_DIR", "./backup"),
		DLQDir:                   envStr("DLQ_DIR", "./dlq"),
		DLQRetentionDays:         envInt("DLQ_RETENTION_DAYS", 7),
		DLQReplayInterval:        envDuration("DLQ_REPLAY_INTERVAL", 5*time.Minute),
		CBFailureThreshold:       envInt("CB_FAILURE_THRESHOLD", 5),
		CBCooldown:               envDuration("CB_COOLDOWN", 60*time.Second),
		FlushWorkers:             envInt("FLUSH_WORKERS", 2),
		SelfURL:                  envStr("SELF_URL", ""),
		PeerURLs:                 envStringSlice("PEER_URLS", nil),
		PeerCBFailureThreshold:   envInt("PEER_CB_FAILURE_THRESHOLD", 5),
		PeerCBCooldown:           envDuration("PEER_CB_COOLDOWN", 30*time.Second),
		BaselineEnabled:          envBool("BASELINE_ENABLED", true),
		BaselineInterval:         envDuration("BASELINE_INTERVAL", time.Hour),
		AnomalyEnabled:           envBool("ANOMALY_ENABLED", true),
		AnomalyInterval:          envDuration("ANOMALY_INTERVAL", time.Minute),
		AnomalyTrainInterval:     envDuration("ANOMALY_TRAIN_INTERVAL", 6*time.Hour),
		AnomalyNTrees:            envInt("ANOMALY_N_TREES", 100),
		AnomalyMaxSamples:        envInt("ANOMALY_MAX_SAMPLES", 256),
		AnomalyZWarn:             envFloat64("ANOMALY_Z_WARN", 2.0),
		AnomalyZCritical:         envFloat64("ANOMALY_Z_CRITICAL", 3.0),
		AnomalyIFThreshold:       envFloat64("ANOMALY_IF_THRESHOLD", 0.65),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.GRPCPort < 1 || c.GRPCPort > 65535 {
		return fmt.Errorf("invalid GRPC_PORT: %d", c.GRPCPort)
	}
	if c.HTTPPort < 1 || c.HTTPPort > 65535 {
		return fmt.Errorf("invalid HTTP_PORT: %d", c.HTTPPort)
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("BATCH_SIZE must be >= 1")
	}
	if c.FlushInterval < 100*time.Millisecond {
		return fmt.Errorf("FLUSH_INTERVAL must be >= 100ms")
	}
	if c.ChannelBufferSize < 1 {
		return fmt.Errorf("CHANNEL_BUFFER_SIZE must be >= 1")
	}
	if c.MemoryBufferSize < 1 {
		return fmt.Errorf("MEMORY_BUFFER_SIZE must be >= 1")
	}
	if c.RetentionDays < 1 {
		return fmt.Errorf("RETENTION_DAYS must be >= 1")
	}
	return nil
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envFloat64(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func envStringSlice(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var result []string
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
