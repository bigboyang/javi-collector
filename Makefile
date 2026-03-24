.PHONY: build run test lint docker-build docker-up docker-down dev run-dev run-sampling run-prod \
        k8s-apply k8s-delete k8s-status k8s-logs k8s-rollout

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

# ClickHouse만 실행 (로컬 개발용)
ch-up:
	docker compose up -d clickhouse

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
