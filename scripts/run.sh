#!/usr/bin/env bash
# javi-collector 실행 스크립트
#
# 사용법:
#   ./scripts/run.sh [mode]
#
# 모드:
#   dev      - 인메모리 스토어, 샘플링 비활성화 (기본값)
#   sampling - 인메모리 스토어 + Tail/Adaptive Sampling 활성화
#   prod     - ClickHouse를 Docker로 자동 시작 후 연결
#   help     - 사용법 출력
#
# 예시:
#   ./scripts/run.sh
#   ./scripts/run.sh dev
#   ./scripts/run.sh sampling
#   REMOTE_CONFIG_URL=http://localhost:8080/sampling-config ./scripts/run.sh sampling
#   ./scripts/run.sh prod

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MODE="${1:-dev}"

# .env 파일이 있으면 로드
if [[ -f "${ROOT_DIR}/.env" ]]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' "${ROOT_DIR}/.env" | xargs)
  echo "[info] .env 파일 로드됨"
fi

# ─── 색상 출력 ───────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[info]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[warn]${NC}  $*"; }
log_title() { echo -e "${CYAN}[javi-collector]${NC} $*"; }

# ─── 빌드 ────────────────────────────────────────────────────
build() {
  log_info "빌드 중..."
  cd "${ROOT_DIR}"
  go build -trimpath -ldflags="-s -w" -o javi-collector ./cmd/collector
  log_info "빌드 완료 → ${ROOT_DIR}/javi-collector"
}

# ─── 모드별 실행 ──────────────────────────────────────────────
run_dev() {
  log_title "개발 모드 (인메모리 스토어, 샘플링 비활성화)"
  export DISABLE_CLICKHOUSE="${DISABLE_CLICKHOUSE:-true}"
  export GRPC_PORT="${GRPC_PORT:-4317}"
  export HTTP_PORT="${HTTP_PORT:-4318}"
  export SAMPLING_ENABLED="${SAMPLING_ENABLED:-false}"
  export BATCH_SIZE="${BATCH_SIZE:-100}"
  export FLUSH_INTERVAL="${FLUSH_INTERVAL:-500ms}"
  export MEMORY_BUFFER_SIZE="${MEMORY_BUFFER_SIZE:-5000}"

  print_env
  exec "${ROOT_DIR}/javi-collector"
}

run_sampling() {
  log_title "샘플링 모드 (인메모리 스토어 + Tail/Adaptive Sampling)"
  export DISABLE_CLICKHOUSE="${DISABLE_CLICKHOUSE:-true}"
  export GRPC_PORT="${GRPC_PORT:-4317}"
  export HTTP_PORT="${HTTP_PORT:-4318}"
  export SAMPLING_ENABLED="${SAMPLING_ENABLED:-true}"
  export BATCH_SIZE="${BATCH_SIZE:-100}"
  export FLUSH_INTERVAL="${FLUSH_INTERVAL:-500ms}"
  export MEMORY_BUFFER_SIZE="${MEMORY_BUFFER_SIZE:-5000}"

  # Remote Config: URL이 지정되지 않으면 정적 설정으로 동작
  export REMOTE_CONFIG_URL="${REMOTE_CONFIG_URL:-}"
  export REMOTE_CONFIG_POLL_INTERVAL="${REMOTE_CONFIG_POLL_INTERVAL:-30s}"

  if [[ -z "${REMOTE_CONFIG_URL}" ]]; then
    log_warn "REMOTE_CONFIG_URL 미설정 → 정적 샘플링 설정으로 실행"
    log_warn "  예: REMOTE_CONFIG_URL=http://localhost:8080/config ./scripts/run.sh sampling"
  fi

  print_env
  exec "${ROOT_DIR}/javi-collector"
}

start_clickhouse() {
  log_info "ClickHouse 컨테이너 시작 중..."
  cd "${ROOT_DIR}"

  if ! command -v docker &>/dev/null; then
    log_warn "docker 명령을 찾을 수 없습니다. ClickHouse를 수동으로 실행해주세요."
    return 1
  fi

  docker compose up -d clickhouse

  log_info "ClickHouse 헬스체크 대기 중 (최대 60초)..."
  local elapsed=0
  until docker compose exec -T clickhouse clickhouse-client --query "SELECT 1" &>/dev/null; do
    if (( elapsed >= 60 )); then
      log_warn "ClickHouse 헬스체크 타임아웃 (60s). 계속 진행합니다."
      return 1
    fi
    sleep 2
    (( elapsed += 2 ))
    echo -n "."
  done
  echo ""
  log_info "ClickHouse 준비 완료 (${elapsed}s)"
}

run_prod() {
  log_title "프로덕션 모드 (ClickHouse Docker 자동 시작 + 연결)"
  export DISABLE_CLICKHOUSE="${DISABLE_CLICKHOUSE:-false}"
  export GRPC_PORT="${GRPC_PORT:-4317}"
  export HTTP_PORT="${HTTP_PORT:-4318}"
  export CLICKHOUSE_ADDR="${CLICKHOUSE_ADDR:-localhost:9000}"
  export CLICKHOUSE_DB="${CLICKHOUSE_DB:-apm}"
  export CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
  export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
  export BATCH_SIZE="${BATCH_SIZE:-1000}"
  export FLUSH_INTERVAL="${FLUSH_INTERVAL:-2s}"
  export CHANNEL_BUFFER_SIZE="${CHANNEL_BUFFER_SIZE:-8192}"
  export SAMPLING_ENABLED="${SAMPLING_ENABLED:-false}"
  export REMOTE_CONFIG_URL="${REMOTE_CONFIG_URL:-}"
  export REMOTE_CONFIG_POLL_INTERVAL="${REMOTE_CONFIG_POLL_INTERVAL:-30s}"
  export RETENTION_DAYS="${RETENTION_DAYS:-30}"

  start_clickhouse

  print_env
  exec "${ROOT_DIR}/javi-collector"
}

# ─── 환경변수 출력 ────────────────────────────────────────────
print_env() {
  echo ""
  echo "  GRPC_PORT                  = ${GRPC_PORT:-}"
  echo "  HTTP_PORT                  = ${HTTP_PORT:-}"
  echo "  DISABLE_CLICKHOUSE         = ${DISABLE_CLICKHOUSE:-}"
  echo "  SAMPLING_ENABLED           = ${SAMPLING_ENABLED:-}"
  echo "  REMOTE_CONFIG_URL          = ${REMOTE_CONFIG_URL:-(없음)}"
  echo "  REMOTE_CONFIG_POLL_INTERVAL= ${REMOTE_CONFIG_POLL_INTERVAL:-}"
  if [[ "${DISABLE_CLICKHOUSE:-true}" == "false" ]]; then
    echo "  CLICKHOUSE_ADDR            = ${CLICKHOUSE_ADDR:-}"
    echo "  CLICKHOUSE_DB              = ${CLICKHOUSE_DB:-}"
    echo "  CLICKHOUSE_USER            = ${CLICKHOUSE_USER:-}"
  fi
  echo ""
}

print_help() {
  cat <<EOF
사용법: $(basename "$0") [mode]

모드:
  dev      인메모리 스토어, 샘플링 비활성화 (기본값)
  sampling 인메모리 스토어 + Tail/Adaptive Sampling 활성화
  prod     ClickHouse Docker 자동 시작 후 연결
  help     이 메시지 출력

환경변수 (모든 모드 공통):
  GRPC_PORT                   gRPC OTLP 수신 포트 (기본: 4317)
  HTTP_PORT                   HTTP OTLP + API 포트 (기본: 4318)

환경변수 (sampling/prod 모드):
  SAMPLING_ENABLED            샘플링 활성화 여부 (기본: false)
  REMOTE_CONFIG_URL           원격 SamplingConfig 폴링 URL
  REMOTE_CONFIG_POLL_INTERVAL 폴링 주기 (기본: 30s)

환경변수 (prod 모드):
  CLICKHOUSE_ADDR             ClickHouse 주소 (기본: localhost:9000)
  CLICKHOUSE_DB               데이터베이스명 (기본: apm)
  CLICKHOUSE_USER             사용자명 (기본: default)
  CLICKHOUSE_PASSWORD         비밀번호 (기본: 없음)
  BATCH_SIZE                  배치 크기 (기본: 1000)
  FLUSH_INTERVAL              flush 주기 (기본: 2s)
  CHANNEL_BUFFER_SIZE         채널 버퍼 크기 (기본: 8192)
  RETENTION_DAYS              데이터 보관 기간 일수 (기본: 30)

예시:
  ./scripts/run.sh
  ./scripts/run.sh sampling
  REMOTE_CONFIG_URL=http://cfg-server/sampling ./scripts/run.sh sampling
  CLICKHOUSE_ADDR=ch-host:9000 ./scripts/run.sh prod
EOF
}

# ─── 메인 ────────────────────────────────────────────────────
case "${MODE}" in
  dev)
    build
    run_dev
    ;;
  sampling)
    build
    run_sampling
    ;;
  prod)
    build
    run_prod
    ;;
  help|--help|-h)
    print_help
    ;;
  *)
    log_warn "알 수 없는 모드: '${MODE}'"
    print_help
    exit 1
    ;;
esac
