---
name: javi-collector architecture
description: javi-collector Go APM 수집기 프로젝트 구조, 구현 결정 및 스키마 정보
type: project
---

## 프로젝트 위치
`/Users/kkc/javi-collector`
Module: `github.com/kkc/javi-collector`

## 구현 완료된 파일
- `internal/server/grpc.go` — OTLP/gRPC 서버 (TraceService, MetricsService, LogsService)
- `internal/store/clickhouse.go` — ClickHouse 배치 저장소 (TraceStore, MetricStore, LogStore)
- `cmd/collector/main.go` — DisableClickHouse 분기, HTTP+gRPC 서버 동시 시작, graceful shutdown

## 주요 아키텍처 결정

### gRPC 서버 설계 (internal/server/grpc.go)
- `proto.Marshal(req)` 후 `ingester.Ingest*` 호출: HTTP 경로와 decoder 코드를 공유하기 위해 재직렬화 방식 채택
  - 재직렬화 비용은 미미, decoder 재사용성이 훨씬 중요
- Keepalive: MaxConnectionIdle=60s, Time=30s, Timeout=5s
- MaxRecvMsgSize=64MiB: Java 에이전트 대규모 배치 수용
- EnforcementPolicy.PermitWithoutStream=true: 스트림 없이도 클라이언트 ping 허용

### ClickHouse 스토어 설계 (internal/store/clickhouse.go)
- TraceStore / MetricStore / LogStore 각각 독립적인 `driver.Conn` 보유 (연결 경쟁 방지)
- 채널 기반 backpressure: 채널 꽉 차면 `error` 반환 → gRPC 레이어에서 `ResourceExhausted` 전달
- dual-trigger flush: size-trigger(BatchSize) + time-trigger(FlushInterval)
- `Close()`: 채널 close → batchWriter goroutine이 남은 데이터 flush 후 `done` 채널 close → 호출자 대기
- native protocol(9000) 사용, async_insert=0 (클라이언트가 배치 직접 제어)
- MetricData.DataPoints를 row 단위로 풀어서 insert (DataPoint 1개 = 1 row)
- HISTOGRAM/SUMMARY는 dp.Sum을 대표 value로 사용

### ClickHouse 테이블 스키마
```sql
-- apm.spans
service_name, scope_name → LowCardinality(String)
ORDER BY (service_name, trace_id, start_time_nano)
TTL dt + INTERVAL 30 DAY

-- apm.metrics
name, type, service_name → LowCardinality(String)
ORDER BY (service_name, name, timestamp_nano)
TTL dt + INTERVAL 30 DAY

-- apm.logs
severity_text, service_name → LowCardinality(String)
ORDER BY (service_name, timestamp_nano)
TTL dt + INTERVAL 30 DAY
```

### go.mod 의존성
- `github.com/ClickHouse/clickhouse-go/v2 v2.43.0` (direct)
- `google.golang.org/grpc v1.70.0` (direct으로 승격)
- Go 버전: 1.24.1로 업그레이드됨 (clickhouse-go 요구)

## 환경변수 설정
| 변수 | 기본값 | 설명 |
|------|--------|------|
| GRPC_PORT | 4317 | OTLP/gRPC 포트 |
| HTTP_PORT | 4318 | OTLP/HTTP + REST API 포트 |
| CLICKHOUSE_ADDR | localhost:9000 | ClickHouse native protocol 주소 |
| CLICKHOUSE_DB | apm | 데이터베이스 이름 |
| DISABLE_CLICKHOUSE | false | true이면 인메모리 fallback |
| BATCH_SIZE | 1000 | 배치 flush 크기 |
| FLUSH_INTERVAL | 2s | 타이머 flush 주기 |
| CHANNEL_BUFFER_SIZE | 8192 | 채널 버퍼 깊이 |
