# javi-collector 아키텍처

> 이 문서는 PR 마다 자동 생성/갱신됩니다.

이 문서는 처음 이 레포에 합류한 사람 개발자가 코드를 빠르게 파악할 수 있도록, 풀어서 친절하게 설명한 온보딩 문서입니다. AI 에이전트용 간결 문서는 루트의 `CLAUDE.md`를 참고하세요.

## 1. 이 프로젝트는 무엇이고 왜 존재하는가

`javi-collector`는 Java APM 에이전트(`javi-agent`로 추정되는 별도 프로젝트)가 보내는 OTLP(OpenTelemetry Protocol) 텔레메트리 — trace(span), metric, log — 를 수신해 **ClickHouse**에 저장하는 Go 기반 컬렉터입니다. OpenTelemetry Collector의 "Receiver → Processor → Exporter" 패턴을 자체 구현한 단일 바이너리라고 볼 수 있습니다.

단순 수집·저장에 머물지 않고, 수신한 데이터를 ClickHouse에 적재하는 동시에 운영에 필요한 부가 기능들을 함께 제공합니다.

- Tail-based sampling(에러/지연 우선 보존), Adaptive sampling(목표 TPS 자동 조절)
- RED 베이스라인 집계, Z-score/IsolationForest 기반 이상 탐지, RCA(근본 원인 분석) 엔진
- 서비스 카탈로그, 에러 그룹 집계, SLO/Burn-Rate 알림, 배포 이벤트 상관 분석, 로그 분석, 프로파일링, K8s Pod 메트릭 등 다양한 AIOps성 조회 API
- ClickHouse를 끄고 인메모리로 동작하는 개발 모드, DLQ(Dead Letter Queue) 기반 장애 복구, 파일 백업, WAL(Write-Ahead Log) 등 신뢰성 장치
- Kafka 또는 직접 HTTP 호출을 통해 `javi-forecast`라는 다운스트림 서비스로 span/metric/log를 팬아웃 — `javi-forecast`가 RAG(Qdrant + Ollama) 파이프라인과 예측(forecast) 기능을 담당하는 것으로 보입니다(이 레포 안에는 RAG/Qdrant/Ollama 코드가 직접 존재하지 않으며, 컬렉터는 이벤트를 발행만 합니다)

요컨대 javi-collector는 "데이터를 안전하게 받아서 ClickHouse에 쌓고, 똑똑하게 걸러내고, 필요한 곳에 흘려보내는" 텔레메트리 파이프라인의 입구 역할을 합니다.

## 2. 전체 아키텍처

```
                         Java APM Agent (javi-agent)
                                   │
                 OTLP/gRPC :4317   │   OTLP/HTTP :4318 (+REST API)
                                   ▼
                  ┌────────────────────────────────────┐
                  │           javi-collector            │
                  │                                      │
                  │  server (grpc.go / http*.go)         │
                  │        │ decode (protobuf/JSON)      │
                  │        ▼                             │
                  │  ingester ──▶ processor.Pipeline      │
                  │        │      (cardinality 등)        │
                  │        ▼                             │
                  │  sampling.TailSamplingStore           │
                  │   (error/latency/probabilistic +      │
                  │    Adaptive 자동 레이트 조절)          │
                  │        │                              │
                  │        ▼                              │
                  │  store.{ClickHouse,Memory}Store       │
                  │   (배치 채널 → flush worker pool)      │
                  │        │                              │
                  │        ├──▶ ClickHouse (apm.spans/     │
                  │        │     metrics/logs 등)          │
                  │        ├──▶ FileBackup (옵션)          │
                  │        ├──▶ WAL / DLQ (장애 복구)       │
                  │        │                              │
                  │  spanPub/metricPub/logPub 팬아웃        │
                  │   ├─ ForecastForwarder (직접 HTTP)     │
                  │   └─ Kafka Producer (spans.all 등)     │
                  └──────────────┬───────────────┬────────┘
                                 │               │
                  REST/SSE 조회   │               │ Kafka topics
              (/api/collector/*) │               ▼
                                 │      다운스트림 javi-forecast
                                 ▼      (RAG: Qdrant + Ollama, 예측 등 — 별도 서비스로 추정)
                          UI / 운영 도구
```

추가로 멀티 인스턴스 배포 시 `sampling.TraceRouter`가 trace ID 기반 consistent hashing으로 같은 trace의 span을 항상 같은 인스턴스로 모아주는 라우팅 계층이 gRPC/HTTP 양쪽에 들어갑니다(같은 trace가 여러 인스턴스에 흩어지면 Tail Sampling이 불완전한 정보로 결정을 내리기 때문입니다).

## 3. 데이터/요청 흐름

### 3.1 수집 경로 (Ingest)

1. **수신**: Java 에이전트가 `POST /v1/traces|/v1/metrics|/v1/logs` (HTTP, protobuf 또는 JSON, gzip 가능) 또는 gRPC `TraceService/MetricsService/LogsService.Export`로 데이터를 보냅니다.
   - gRPC 경로는 이미 파싱된 proto 구조체를 그대로 `ingester`에 넘겨 `Marshal→Unmarshal` 왕복을 생략합니다(`internal/server/grpc.go`, `internal/ingester/ingester.go` 주석 참고. 고TPS 환경에서 CPU 10~20% 절감 효과를 노린 설계).
2. **디코드**: `internal/decoder`가 OTLP protobuf/JSON을 내부 모델(`model.SpanData`, `model.MetricData`, `model.LogData`)로 변환합니다.
3. **가공(Processor Pipeline)**: `internal/processor`가 OTel Collector 스타일의 체이닝 가능한 처리 단계를 제공합니다. 현재 내장된 것은 `CardinalityProcessor`(서비스+속성키별 고유값 수를 Bloom filter로 추정해 일정 개수 이상이면 `"__high_cardinality__"`로 치환 — ClickHouse 카디널리티 폭증 방지)입니다. `CARDINALITY_ENABLED=true`일 때만 파이프라인에 들어갑니다.
4. **Tail Sampling**: `internal/sampling.TailSamplingStore`가 `store.TraceStore`를 감싸는 데코레이터로 끼어듭니다. trace 단위로 버퍼링한 뒤 정책(`PolicyEvaluator`: error > latency > probabilistic 우선순위)을 평가해 keep/drop을 결정합니다. error나 latency로 이미 keep이 확정된 "critical trace"는 `AdaptiveController`(EWMA 기반 목표 TPS 자동 조절)를 우회해 항상 보존됩니다. `SAMPLING_ENABLED=false`(기본)면 전량 통과(no-op)로 동작합니다.
5. **저장(Store)**: `internal/store`의 `TraceStore`/`MetricStore`/`LogStore` 인터페이스 구현체가 실제 적재를 담당합니다.
   - `ClickHouseTraceStore` 등: `Append*` 호출 → 채널에 enqueue(가득 차면 backpressure로 거부) → `batchWriter` 고루틴이 size 또는 time 트리거로 flush 배치 생성 → `FlushWorkers`개의 flush worker 고루틴이 ClickHouse에 병렬 insert. 실패 시 지수 백오프 재시도(1s→2s→4s, 최대 3회), 그래도 실패하면 Circuit Breaker가 열리고 DLQ(JSONL 파일)로 직행합니다.
   - `MemoryTraceStore` 등: `DISABLE_CLICKHOUSE=true`일 때 쓰는 인메모리 링버퍼. 선택적으로 WAL(Write-Ahead Log)을 붙여 프로세스 재시작 후 복구할 수 있습니다.
   - `FileBackupWriter`로 감싸면(`BACKUP_ENABLED=true`, 기본값) 수신된 모든 데이터를 JSONL로 추가 백업합니다(쓰기 실패는 warn-only, 수집 파이프라인을 막지 않음).
6. **하류 팬아웃(Publish)**: 저장이 끝나면 span/metric/log를 비동기로 추가 발행합니다.
   - `KAFKA_ENABLED=false`(기본) + `FORECAST_ENDPOINT` 설정 시: `forecast.ForecastForwarder`가 배치로 모아 `javi-forecast`에 직접 HTTP로 보냅니다(`/v1/spans`, `/v1/metrics`, JVM 메트릭은 자동 변환해 `/v1/metrics/jvm`).
   - `KAFKA_ENABLED=true` 시: `internal/kafka`의 `SpanProducer`/`MetricProducer`/`LogProducer`가 각각 `spans.all`/`metrics`/`logs` 토픽에 비동기(Async, 실패 시 드롭)로 발행합니다. 컬렉터는 Producer 역할만 하고, 컨슈머(RAG 임베더, Forecast 등)는 `javi-forecast` 쪽 소관입니다.
7. **셀프 트레이싱(옵션)**: `SELF_TRACING_ENABLED=true`면 `internal/selftracing`이 컬렉터 내부 decode/process/store 단계 자체를 span으로 기록해 일반 trace와 함께 ClickHouse에 저장합니다(`serviceName="javi-collector"`, `javi.internal=true` 속성으로 구분).

### 3.2 조회 경로 (Query)

`internal/server/http.go` 및 `http_*.go` 파일들이 REST API를 제공합니다. 대표적으로:

- `GET /api/collector/traces|metrics|logs` — 단순 폴링 조회
- `GET /api/collector/red`, `/topology`, `/error-logs`, `/anomalies`, `/histogram` — AIOps 집계 조회
- `GET /api/stream/logs`, `/api/stream/alerts` — SSE(Server-Sent Events) 기반 준실시간 스트리밍(짧은 주기 폴링을 SSE로 감싼 형태)
- `GET /api/query?sql=...` — 화이트리스트 검증된 SELECT 전용 raw SQL
- `GET /healthz`, `/readyz`, `GET /metrics`(Prometheus) — 운영용

각 부가 기능(서비스 카탈로그, 에러 그룹, SLO, RCA, 배포 이벤트, 로그 분석, 프로파일링, K8s 메트릭 등)은 `cmd/collector/main.go`에서 ClickHouse가 활성화된 경우에만 개별 Store를 초기화하고, 성공하면 `HTTPServer.Set*` 메서드로 주입합니다. 초기화에 실패하거나 ClickHouse가 꺼져 있으면 해당 기능은 조용히 비활성화되고 관련 엔드포인트는 `501`을 반환합니다 — 이 방식 덕분에 `main.go`가 다소 길지만, 각 기능이 서로 독립적으로 켜지고 꺼질 수 있습니다.

## 4. 핵심 디렉터리·모듈

| 경로 | 책임 | 비고 |
|---|---|---|
| `cmd/collector/main.go` | 전체 와이어링(설정 로드 → store/sampling/kafka/forecast 초기화 → HTTP/gRPC 서버 기동 → graceful shutdown) | 기능 토글이 많아 파일이 길지만, 각 블록이 "이 기능 켜졌으면 초기화하고 주입"의 반복 구조 |
| `internal/config` | 환경변수 → `Config` 구조체. 모든 기본값과 검증 로직이 여기 모입니다 | `hotreload.go`는 일부 값(BatchSize/FlushInterval)을 파일 폴링으로 런타임에 갱신 |
| `internal/decoder` | OTLP protobuf/JSON → 내부 `model` 구조체 변환 | HTTP(바이트) 경로와 gRPC(이미 파싱된 proto) 경로 둘 다 지원 |
| `internal/model` | `SpanData`/`MetricData`/`LogData` 등 내부 공통 데이터 모델 | Java 에이전트의 SpanData와 1:1 대응되도록 설계 |
| `internal/processor` | 파이프라인 형태의 변환/필터 단계(현재 cardinality 제어) | OTel Collector의 Processor 개념을 모사 |
| `internal/sampling` | Tail sampling 정책, Adaptive rate controller, 멀티 인스턴스 TraceRouter, 원격 config 폴링 | trace 단위 결정이라 buffer(`tail_store.go`)가 필요 |
| `internal/ingester` | decode 이후 단계를 오케스트레이션(processor → store → publish), 통계 카운터 보관 | `SpanPublisher`/`MetricPublisher`/`LogPublisher` 인터페이스로 Kafka/Forecast를 디커플링 |
| `internal/store` | 저장소 인터페이스(`TraceStore`/`MetricStore`/`LogStore`) + ClickHouse/Memory 구현 + Circuit Breaker, WAL, DLQ replay, 파일 백업 + AIOps용 보조 Store(서비스 카탈로그, 에러 그룹, SLO, RCA, 배포 이벤트, 로그 분석, 프로파일링, K8s 메트릭 등) | 가장 큰 패키지. "저장"과 "AIOps 조회용 부가 Store"가 함께 있습니다 |
| `internal/server` | HTTP(OTLP 수신 + REST + SSE + 운영 엔드포인트) 및 gRPC(OTLP 수신 + Health + Reflection) 서버 | `http_*.go`로 기능별 파일 분리(alerts/catalog/ingest/logs/ops/profiling/query/rca/slo 등) |
| `internal/kafka` | Kafka Producer(span/metric/log/deploy). 컬렉터는 Producer만, 컨슈머는 다운스트림 소관 | `segmentio/kafka-go` 사용, Async 모드(실패 시 드롭, 핫패스 비차단 우선) |
| `internal/forecast` | Kafka 없이 `javi-forecast`로 직접 HTTP 전송하는 대안 경로. JVM 메트릭은 누적 후 스냅샷으로 변환 | `KAFKA_ENABLED=false` + `FORECAST_ENDPOINT` 설정 시에만 활성 |
| `internal/selftracing` | 컬렉터 내부 파이프라인 자체를 관측하기 위한 셀프 트레이싱 | 별도 작은 ring-buffer + flush 고루틴 |
| `docker/` | ClickHouse 설정 오버라이드 등 컨테이너 구성 | |
| `scripts/` | 로컬 실행(`run.sh`), k8s RAG 통합 테스트, 데이터 검증 스크립트 | `k8s-rag-test.sh`는 Qdrant/Ollama 연동을 검증하는 것으로 보이나 해당 k8s 매니페스트(`k8s/`)는 이 레포에 존재하지 않습니다(다른 레포 또는 미커밋 상태로 추정 — Makefile/스크립트가 `k8s/` 경로를 참조하지만 디렉터리 자체는 확인되지 않았습니다) |

## 5. 로컬 개발 시작법

필요 도구: Go 1.24+, Docker(또는 Docker Compose), 그리고 ClickHouse를 쓰려면 ClickHouse 인스턴스(로컬 docker-compose 또는 k8s).

```bash
# 가장 빠른 길: ClickHouse 없이 인메모리로 실행
make dev
# 내부적으로 DISABLE_CLICKHOUSE=true go run ./cmd/collector

# 빌드 / 테스트 / 린트
make build   # go build
make test    # go test -race -count=1 ./...
make lint    # go vet ./...

# ClickHouse + collector 풀스택 (docker compose)
make docker-up
make docker-down

# 환경별 실행 스크립트 (scripts/run.sh 래퍼)
make run-dev      # 개발
make run-sampling # 샘플링 활성화
make run-prod     # 운영 유사 설정
```

쿠버네티스 운영 명령(`k8s-apply`, `k8s-rollout`, `k8s-status`, `k8s-logs`, `ch-port-forward`, `ollama-port-forward`, `qdrant-port-forward`, `k8s-rag-test*`)도 Makefile에 정의돼 있습니다. 다만 위에서 언급했듯 이 명령들이 참조하는 `k8s/` 매니페스트 디렉터리는 현재 체크아웃에는 없으므로, 실제 사용 전에 해당 리소스가 어디서 관리되는지 먼저 확인이 필요합니다(추측이 아니라 이 레포에서 직접 확인되지 않는 부분임을 밝힙니다).

주요 환경변수는 `README.md`에 정리돼 있고, `internal/config/config.go`에 전체 목록과 기본값/설명이 한글 주석으로 매우 상세히 달려 있으니 새로운 토글을 찾을 때는 그 파일을 먼저 보는 게 가장 빠릅니다.

## 6. 알아두면 좋은 함정 · 주의사항 · 트레이드오프

- **ClickHouse 커넥션은 공유 풀 하나**입니다(`store.OpenConn` 한 번 호출 → trace/metric/log 세 Store가 공유). 예전에는 Store마다 별도로 커넥션을 열어 최대 3×MaxOpenConns가 생성됐던 문제를 고친 것이므로, 새 Store를 추가할 때도 이 공유 커넥션을 재사용해야 합니다.
- **종료 순서가 중요합니다**: `chConn.Close()`는 `defer`로 가장 먼저 등록되지만 Go의 LIFO 특성상 가장 마지막에 실행되어, "각 Store가 먼저 drain → 마지막에 커넥션 종료"가 보장됩니다. 새로운 defer를 추가할 때 이 순서를 깨지 않도록 주의해야 합니다.
- **Tail Sampling은 trace 전체가 한 인스턴스에 모여야 정확히 동작**합니다. 멀티 인스턴스로 배포하면서 `SAMPLING_ENABLED=true`를 쓰려면 `SELF_URL`/`PEER_URLS`를 함께 설정해 `TraceRouter`를 활성화해야 합니다. 안 그러면 같은 trace의 span이 인스턴스마다 흩어져 샘플링 결정이 불완전한 정보로 내려질 수 있습니다.
- **Probabilistic sampling 경계값 처리**: `rate=1.0`일 때 `float64(math.MaxUint64)` 변환 이슈로 threshold가 0이 돼 전부 DROP되는 버그가 있었고, 지금은 `rate>=1.0`/`rate<=0.0`을 명시적으로 분기해 처리합니다(`internal/sampling/policy.go` 참고) — 비슷한 부동소수점 경계 코드를 만질 때 주의할 사례입니다.
- **Async Kafka producer는 "속도 > 완전성"을 택합니다.** 채널이 차면 메시지를 그냥 드롭합니다. 데이터 무손실이 중요한 경로라면 Kafka보다는 ClickHouse 직접 적재 + DLQ 경로를 신뢰해야 합니다.
- **DLQ/WAL/파일 백업은 서로 다른 장애를 막는 장치**입니다: WAL은 인메모리 모드에서 프로세스 재시작 복구용, DLQ는 ClickHouse flush가 재시도 후에도 실패했을 때의 보존용(나중에 자동/수동 재적재), 파일 백업(`BACKUP_ENABLED`, 기본 true)은 수신된 모든 원본 데이터의 별도 JSONL 백업입니다. 세 가지를 혼동하지 않아야 합니다.
- **Circuit Breaker가 열리면 flush가 막히고 DLQ로 직행**합니다(`internal/store/circuit_breaker.go`). ClickHouse 장애 복구 후 DLQ 재적재(`DLQReplayer`)가 자동으로 돌지만, `DLQDir`이 비어 있으면 DLQ 자체가 비활성화되어 flush 실패 데이터가 유실될 수 있습니다.
- **gRPC와 HTTP는 별도의 디코드 경로**를 가집니다(`decoder.DecodeTraces` vs `decoder.DecodeTracesFromRequest`). gRPC 경로는 재직렬화를 피하려고 일부러 분리돼 있으므로, decoder 로직을 고칠 때는 두 경로 모두 동일하게 반영해야 합니다(테스트로 커버되는지 함께 확인할 것).
- **부가 기능(SLO/RCA/에러그룹/배포 이벤트 등)은 ClickHouse 비활성화 시 전부 꺼집니다.** `make dev`(인메모리 모드)로 개발할 때는 이 기능들의 엔드포인트가 501을 반환하는 게 정상이며 버그가 아닙니다.
- **카디널리티 제어는 옵트인**(`CARDINALITY_ENABLED=false` 기본)입니다. 끄고 운영하면 고카디널리티 속성(예: 사용자ID를 attribute로 넣는 경우)이 ClickHouse 비용/성능에 직접 영향을 줄 수 있습니다.
- **README에 명시된 RAG(Qdrant + Ollama) 파이프라인은 이 레포 코드에는 보이지 않습니다.** 컬렉터는 Kafka 토픽 발행 또는 `FORECAST_ENDPOINT`로의 직접 HTTP 호출까지만 담당하고, 실제 RAG/임베딩/벡터 검색은 별도 다운스트림 서비스(`javi-forecast`로 추정)의 책임으로 보입니다. RAG 동작 자체를 디버깅해야 한다면 이 레포가 아니라 해당 다운스트림 레포를 봐야 합니다.
