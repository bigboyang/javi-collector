.PHONY: build run test lint docker-build docker-up docker-down dev

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

clean:
	rm -f $(BINARY)
