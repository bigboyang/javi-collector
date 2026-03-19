#!/usr/bin/env bash
# OTLP/HTTP로 테스트 데이터를 전송해 collector 동작을 확인하는 스크립트
#
# 사용법:
#   ./scripts/send-test-data.sh [command]
#
# 커맨드:
#   trace    - 트레이스 1건 전송 (기본값)
#   metric   - 메트릭 1건 전송
#   log      - 로그 1건 전송
#   all      - trace + metric + log 전체 전송
#   query    - 저장된 데이터 조회 (REST API)
#   health   - healthz / readyz 확인
#   help     - 사용법 출력

set -euo pipefail

HOST="${COLLECTOR_HOST:-localhost}"
HTTP_PORT="${HTTP_PORT:-4318}"
BASE="http://${HOST}:${HTTP_PORT}"

CMD="${1:-trace}"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "${GREEN}[ok]${NC}    $*"; }
info() { echo -e "${CYAN}[info]${NC}  $*"; }
warn() { echo -e "${YELLOW}[warn]${NC}  $*"; }

# ─── 공통: curl 래퍼 ──────────────────────────────────────────
post_json() {
  local url="$1"
  local body="$2"
  local desc="${3:-}"
  info "POST ${url}"
  local status
  status=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${url}" \
    -H "Content-Type: application/json" \
    -d "${body}")
  if [[ "${status}" =~ ^2 ]]; then
    ok "HTTP ${status} ${desc}"
  else
    warn "HTTP ${status} ${desc} — 실패"
  fi
}

get_json() {
  local url="$1"
  local desc="${2:-}"
  info "GET  ${url}"
  curl -s "${url}" | python3 -m json.tool 2>/dev/null || curl -s "${url}"
  echo ""
}

# ─── 현재 시각 (nanosec) ──────────────────────────────────────
now_ns() {
  # macOS / Linux 호환
  if date -r 0 &>/dev/null 2>&1; then
    # macOS: python3로 nanosec 구하기
    python3 -c "import time; print(int(time.time() * 1e9))"
  else
    date +%s%N
  fi
}

TRACE_ID="5b8efff798038103d269b633813fc60c"
SPAN_ID="eee19b7ec3c1b174"
NOW=$(now_ns)

# ─── trace ────────────────────────────────────────────────────
send_trace() {
  info "트레이스 전송 중..."
  local body
  body=$(cat <<EOF
{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "test-service"}},
          {"key": "service.version", "value": {"stringValue": "1.0.0"}},
          {"key": "deployment.environment", "value": {"stringValue": "dev"}}
        ]
      },
      "scopeSpans": [
        {
          "scope": {"name": "test-instrumentation", "version": "0.1.0"},
          "spans": [
            {
              "traceId": "${TRACE_ID}",
              "spanId": "${SPAN_ID}",
              "name": "GET /api/users",
              "kind": 2,
              "startTimeUnixNano": "${NOW}",
              "endTimeUnixNano": "${NOW}",
              "status": {"code": 1},
              "attributes": [
                {"key": "http.method", "value": {"stringValue": "GET"}},
                {"key": "http.url", "value": {"stringValue": "/api/users"}},
                {"key": "http.status_code", "value": {"intValue": 200}}
              ]
            }
          ]
        }
      ]
    }
  ]
}
EOF
)
  post_json "${BASE}/v1/traces" "${body}" "(trace)"
}

# ─── metric ───────────────────────────────────────────────────
send_metric() {
  info "메트릭 전송 중..."
  local body
  body=$(cat <<EOF
{
  "resourceMetrics": [
    {
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "test-service"}}
        ]
      },
      "scopeMetrics": [
        {
          "scope": {"name": "test-instrumentation"},
          "metrics": [
            {
              "name": "http.server.request_count",
              "description": "Total HTTP requests",
              "unit": "1",
              "sum": {
                "dataPoints": [
                  {
                    "startTimeUnixNano": "${NOW}",
                    "timeUnixNano": "${NOW}",
                    "asInt": 42,
                    "attributes": [
                      {"key": "http.method", "value": {"stringValue": "GET"}},
                      {"key": "http.status_code", "value": {"intValue": 200}}
                    ]
                  }
                ],
                "aggregationTemporality": 2,
                "isMonotonic": true
              }
            }
          ]
        }
      ]
    }
  ]
}
EOF
)
  post_json "${BASE}/v1/metrics" "${body}" "(metric)"
}

# ─── log ──────────────────────────────────────────────────────
send_log() {
  info "로그 전송 중..."
  local body
  body=$(cat <<EOF
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "test-service"}}
        ]
      },
      "scopeLogs": [
        {
          "scope": {"name": "test-instrumentation"},
          "logRecords": [
            {
              "timeUnixNano": "${NOW}",
              "severityNumber": 9,
              "severityText": "INFO",
              "body": {"stringValue": "Hello from javi-collector test script"},
              "traceId": "${TRACE_ID}",
              "spanId": "${SPAN_ID}",
              "attributes": [
                {"key": "app", "value": {"stringValue": "test"}}
              ]
            }
          ]
        }
      ]
    }
  ]
}
EOF
)
  post_json "${BASE}/v1/logs" "${body}" "(log)"
}

# ─── query ────────────────────────────────────────────────────
query_data() {
  echo ""
  info "=== 저장된 트레이스 (최신 5건) ==="
  get_json "${BASE}/api/collector/traces?limit=5"

  info "=== 저장된 메트릭 (최신 5건) ==="
  get_json "${BASE}/api/collector/metrics?limit=5"

  info "=== 저장된 로그 (최신 5건) ==="
  get_json "${BASE}/api/collector/logs?limit=5"

  info "=== 통계 ==="
  get_json "${BASE}/api/collector/stats"
}

# ─── health ───────────────────────────────────────────────────
check_health() {
  echo ""
  info "=== 헬스 체크 ==="
  local hz
  hz=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/healthz")
  [[ "${hz}" == "200" ]] && ok "healthz: ${hz}" || warn "healthz: ${hz}"

  local rz
  rz=$(curl -s -o /dev/null -w "%{http_code}" "${BASE}/readyz")
  [[ "${rz}" == "200" ]] && ok "readyz:  ${rz}" || warn "readyz:  ${rz} (서버 초기화 중일 수 있음)"

  info "Prometheus metrics:"
  curl -s "${BASE}/metrics" | grep -E "^(javi_|go_goroutines)" | head -10 || true
}

print_help() {
  cat <<EOF
사용법: $(basename "$0") [command]

커맨드:
  trace    트레이스 1건 전송 (기본값)
  metric   메트릭 1건 전송
  log      로그 1건 전송
  all      trace + metric + log 전체 전송
  query    저장된 데이터 조회
  health   healthz / readyz 확인
  help     이 메시지 출력

환경변수:
  COLLECTOR_HOST  collector 호스트 (기본: localhost)
  HTTP_PORT       HTTP 포트 (기본: 4318)

예시:
  ./scripts/send-test-data.sh
  ./scripts/send-test-data.sh all
  ./scripts/send-test-data.sh query
  COLLECTOR_HOST=my-server ./scripts/send-test-data.sh health
EOF
}

# ─── 메인 ────────────────────────────────────────────────────
case "${CMD}" in
  trace)   send_trace ;;
  metric)  send_metric ;;
  log)     send_log ;;
  all)
    send_trace
    send_metric
    send_log
    echo ""
    ok "전송 완료. 데이터 확인: ./scripts/send-test-data.sh query"
    ;;
  query)   query_data ;;
  health)  check_health ;;
  help|--help|-h) print_help ;;
  *)
    warn "알 수 없는 커맨드: '${CMD}'"
    print_help
    exit 1
    ;;
esac
