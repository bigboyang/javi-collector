package config

import (
	"fmt"
	"os"
	"strconv"
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
		BackupEnabled:            envBool("BACKUP_ENABLED", false),
		BackupDir:                envStr("BACKUP_DIR", "./backup"),
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
