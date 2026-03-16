# javi-collector

Java APM 에이전트(javi-agent)로부터 OTLP 데이터를 수신하여 ClickHouse에 저장하는 Go 기반 텔레메트리 컬렉터.

## 아키텍처

```
Java Agent (OTLP/gRPC or HTTP)
        │
        ▼
  javi-collector
  ├── gRPC :4317  (OTLP/gRPC)
  ├── HTTP :4318  (OTLP/HTTP + REST API)
  │
  ├── ingester  — decode → store 파이프라인
  ├── store     — 채널 기반 배치 insert
  │
  └── ClickHouse (apm.spans / apm.metrics / apm.logs)
```

## 빠른 시작

```bash
# ClickHouse + collector 실행
make docker-up

# 개발 모드 (ClickHouse 없이 인메모리)
make dev
```

## 엔드포인트

| 경로 | 설명 |
|------|------|
| `POST /v1/traces` | OTLP/HTTP trace 수신 |
| `POST /v1/metrics` | OTLP/HTTP metric 수신 |
| `POST /v1/logs` | OTLP/HTTP log 수신 |
| `GET /api/collector/traces?limit=100` | trace 조회 |
| `GET /api/collector/metrics?limit=100` | metric 조회 |
| `GET /api/collector/logs?limit=100` | log 조회 |
| `GET /api/collector/stats` | 수신 카운터 |
| `GET /healthz` | liveness probe |
| `GET /readyz` | readiness probe |
| `GET /metrics` | Prometheus 지표 |

gRPC 포트 4317에서 OTLP TraceService / MetricsService / LogsService 지원.

## 환경변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `GRPC_PORT` | `4317` | OTLP gRPC 포트 |
| `HTTP_PORT` | `4318` | OTLP HTTP 포트 |
| `CLICKHOUSE_ADDR` | `localhost:9000` | ClickHouse native 주소 |
| `CLICKHOUSE_DB` | `apm` | 데이터베이스 |
| `CLICKHOUSE_USER` | `default` | 사용자 |
| `CLICKHOUSE_PASSWORD` | `` | 패스워드 |
| `BATCH_SIZE` | `1000` | 배치당 최대 레코드 수 |
| `FLUSH_INTERVAL` | `2s` | 타이머 flush 주기 |
| `CHANNEL_BUFFER_SIZE` | `8192` | 채널 버퍼 깊이 |
| `DISABLE_CLICKHOUSE` | `false` | 인메모리 fallback |
