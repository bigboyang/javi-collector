.PHONY: build run test lint docker-build docker-up docker-down dev run-dev run-sampling run-prod \
        k8s-apply k8s-apply-ch k8s-delete k8s-status k8s-logs k8s-rollout ch-port-forward \
        k8s-rag-test k8s-rag-test-integration k8s-rag-test-all ollama-port-forward qdrant-port-forward

BINARY := javi-collector
BUILD_FLAGS := -trimpath -ldflags="-s -w"

build:
	go build $(BUILD_FLAGS) -o $(BINARY) ./cmd/collector

run: build
	./$(BINARY)

# 개발 모드: ClickHouse 없이 인메모리 스토어로 실행
dev:
	DISABLE_CLICKHOUSE=true go run ./cmd/collector

test:
	go test -race -count=1 ./...

lint:
	go vet ./...

docker-build:
	docker build -t javi-collector:latest .

# ClickHouse + collector 전체 스택 실행
docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

# k8s ClickHouse NodePort를 로컬 9000으로 포트 포워드 (백그라운드)
# 사용: make ch-port-forward &
ch-port-forward:
	kubectl -n $(K8S_NS) port-forward svc/clickhouse-svc 9000:9000 8123:8123

# k8s ClickHouse만 배포 (collector 제외)
k8s-apply-ch:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/clickhouse/configmap.yaml
	kubectl apply -f k8s/clickhouse/secret.yaml
	kubectl apply -f k8s/clickhouse/service.yaml
	kubectl apply -f k8s/clickhouse/statefulset.yaml

run-dev:
	./scripts/run.sh dev

run-sampling:
	./scripts/run.sh sampling

run-prod:
	./scripts/run.sh prod

clean:
	rm -f $(BINARY)

# ── Kubernetes ────────────────────────────────────────────────────────────────
K8S_IMAGE ?= javi-collector:latest
K8S_NS    := apm

# 이미지를 빌드 후 전체 스택 적용 (kustomize 사용)
k8s-apply: docker-build
	kubectl apply -k k8s/

# 전체 리소스 삭제 (namespace 포함 → PVC도 삭제됨. 주의)
k8s-delete:
	kubectl delete -k k8s/ --ignore-not-found

# 현재 상태 확인
k8s-status:
	kubectl -n $(K8S_NS) get pods,svc,hpa,pdb

# collector 로그 스트리밍
k8s-logs:
	kubectl -n $(K8S_NS) logs -l app=javi-collector -f --max-log-requests=4

# 이미지 재빌드 후 롤링 업데이트
k8s-rollout: docker-build
	kubectl -n $(K8S_NS) rollout restart deployment/javi-collector
	kubectl -n $(K8S_NS) rollout status deployment/javi-collector

# k8s Ollama 포트 포워드 (백그라운드)
ollama-port-forward:
	kubectl -n $(K8S_NS) port-forward svc/ollama 11434:11434

# k8s Qdrant 포트 포워드 (백그라운드)
qdrant-port-forward:
	kubectl -n $(K8S_NS) port-forward svc/qdrant 6333:6333 6334:6334

# k8s RAG 통합 테스트 — Ollama E2E (port-forward 자동 설정)
k8s-rag-test:
	./scripts/k8s-rag-test.sh ollama

# k8s RAG 통합 테스트 — Qdrant only
k8s-rag-test-integration:
	./scripts/k8s-rag-test.sh integration

# k8s RAG 통합 테스트 — 전체
k8s-rag-test-all:
	./scripts/k8s-rag-test.sh all
