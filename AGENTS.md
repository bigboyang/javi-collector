<!-- AUTO-MIRROR of CLAUDE.md — 직접 수정 금지 (codex/AGENTS.md) -->

# javi-collector

javi 관측 데이터 수집기. 들어온 텔레메트리를 처리해 **ClickHouse**에 저장하고, **RAG**(Qdrant + Ollama) 파이프라인과 연동되는 Go 서비스.

## 아키텍처

- `cmd/` — 실행 진입점
- `internal/` — 수집·처리·저장 내부 패키지(HTTP 서버 등)
- `docker/` — 컨테이너 구성
- `scripts/` — 보조 스크립트
- module: `github.com/kkc/javi-collector`
- 저장소: ClickHouse / 벡터: Qdrant / 임베딩·LLM: Ollama

## 개발 명령 (Makefile)

- 실행: `make run` / `run-dev` / `run-prod` / `run-sampling`, `make dev`
- 빌드/테스트/린트: `make build` / `make test` / `make lint`
- 도커: `make docker-up` / `docker-down` / `docker-build`
- 쿠버네티스: `make k8s-apply` / `k8s-rollout` / `k8s-status` / `k8s-logs` / `k8s-delete`
- 포트포워딩: `make ch-port-forward` / `qdrant-port-forward` / `ollama-port-forward`
- RAG 통합 테스트: `make k8s-rag-test*`

## 규칙·관례

> 코딩 컨벤션·주의사항을 여기에 적어두세요. PR로 코드가 바뀌면 이 영역은 GitHub Actions(Claude)가 자동 보강합니다.

<!-- AUTO-GENERATED:start (스크립트가 관리. 직접 수정 금지) -->

_아래 구간은 스크립트가 자동 생성합니다. 직접 수정하지 마세요._

### 기술 스택
- Go (`go.mod`)
- Docker (`Dockerfile`)

### 명령어
**Make 타깃**:
```
build
ch-port-forward
clean
dev
docker-build
docker-down
docker-up
k8s-apply
k8s-apply-ch
k8s-delete
k8s-logs
k8s-rag-test
k8s-rag-test-all
k8s-rag-test-integration
k8s-rollout
k8s-status
lint
ollama-port-forward
qdrant-port-forward
run
run-dev
run-prod
run-sampling
test
```

### 최상위 디렉터리 구조
```
.github
cmd
docker
internal
scripts
```

<!-- AUTO-GENERATED:end -->
