#!/usr/bin/env bash
# k8s ClickHouse & Qdrant 적재 데이터 검증 스크립트
#
# 사용법: ./scripts/verify-k8s-data.sh [section]
#
# section:
#   all       전체 확인 (기본값)
#   ch        ClickHouse 만 확인
#   qdrant    Qdrant 만 확인
#   send      테스트 데이터 전송 후 확인
#   clean     포트포워드 프로세스 정리

set -euo pipefail

NS="apm"
CH_PASS="changeme-use-sealed-secrets"
CH_USER="default"
CH_DB="apm"
CH_LOCAL_PORT="18123"
QDRANT_LOCAL_PORT="16333"
COLLECTOR_LOCAL_PORT="14318"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()    { echo -e "${GREEN}[ok]${NC}    $*"; }
info()  { echo -e "${CYAN}[info]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC}  $*"; }
err()   { echo -e "${RED}[err]${NC}   $*"; }

CH_PF_PID=""
QDRANT_PF_PID=""
COLLECTOR_PF_PID=""

cleanup() {
  [[ -n "$CH_PF_PID" ]]        && kill "$CH_PF_PID"        2>/dev/null || true
  [[ -n "$QDRANT_PF_PID" ]]    && kill "$QDRANT_PF_PID"    2>/dev/null || true
  [[ -n "$COLLECTOR_PF_PID" ]] && kill "$COLLECTOR_PF_PID" 2>/dev/null || true
}
trap cleanup EXIT

start_port_forwards() {
  info "포트포워드 시작..."
  kubectl -n "$NS" port-forward svc/clickhouse-svc "$CH_LOCAL_PORT":8123 >/dev/null 2>&1 &
  CH_PF_PID=$!
  kubectl -n "$NS" port-forward svc/qdrant "$QDRANT_LOCAL_PORT":6333 >/dev/null 2>&1 &
  QDRANT_PF_PID=$!
  kubectl -n "$NS" port-forward deployment/javi-collector "$COLLECTOR_LOCAL_PORT":4318 >/dev/null 2>&1 &
  COLLECTOR_PF_PID=$!
  sleep 4
  ok "포트포워드 준비 (CH=:$CH_LOCAL_PORT  Qdrant=:$QDRANT_LOCAL_PORT  Collector=:$COLLECTOR_LOCAL_PORT)"
}

ch_query() {
  local sql="$1"
  curl -s --max-time 10 \
    "http://localhost:${CH_LOCAL_PORT}/?user=${CH_USER}&password=${CH_PASS}&query=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$sql")" 2>/dev/null
}

# ─── ClickHouse 확인 ─────────────────────────────────────────────────────────
check_clickhouse() {
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  info "ClickHouse 연결 확인..."
  local ping
  ping=$(curl -s --max-time 5 "http://localhost:${CH_LOCAL_PORT}/ping" 2>/dev/null)
  if [[ "$ping" == "Ok." ]]; then
    ok "ClickHouse 응답 정상"
  else
    err "ClickHouse 응답 실패: ${ping}"
    return 1
  fi

  echo ""
  info "=== Spans (최근 10건) ==="
  printf "%-50s %-20s %-10s %-30s\n" "SPAN_NAME" "SERVICE" "STATUS" "RECEIVED_AT"
  echo "────────────────────────────────────────────────────────────────────────────────────────────────────────────"
  ch_query "SELECT name, service_name, status_code, formatDateTime(toDateTime(received_at_ms/1000), '%Y-%m-%d %H:%M:%S') AS received_at FROM ${CH_DB}.spans ORDER BY received_at_ms DESC LIMIT 10 FORMAT TSV" | \
    while IFS=$'\t' read -r name svc status ts; do
      printf "%-50s %-20s %-10s %-30s\n" "${name:0:48}" "${svc:0:18}" "${status}" "${ts}"
    done

  echo ""
  info "=== Spans 서비스별 집계 ==="
  ch_query "SELECT service_name, count(*) AS cnt, countIf(status_code = 2) AS errors FROM ${CH_DB}.spans GROUP BY service_name ORDER BY cnt DESC FORMAT TSV" | \
    while IFS=$'\t' read -r svc cnt errs; do
      printf "  %-25s  총 %-6s  에러 %-6s\n" "$svc" "$cnt" "$errs"
    done

  echo ""
  info "=== Metrics (서비스별 지표 종류) ==="
  ch_query "SELECT service_name, count(DISTINCT name) AS metric_types, count(*) AS total_rows FROM ${CH_DB}.metrics GROUP BY service_name ORDER BY total_rows DESC FORMAT TSV" | \
    while IFS=$'\t' read -r svc types total; do
      printf "  %-25s  지표종류 %-6s  총 rows %-6s\n" "$svc" "$types" "$total"
    done

  echo ""
  info "=== Metrics 상위 10개 지표 ==="
  ch_query "SELECT name, service_name, count(*) AS cnt FROM ${CH_DB}.metrics GROUP BY name, service_name ORDER BY cnt DESC LIMIT 10 FORMAT TSV" | \
    while IFS=$'\t' read -r name svc cnt; do
      printf "  %-45s  %-20s  %-6s rows\n" "${name:0:43}" "${svc:0:18}" "$cnt"
    done

  echo ""
  info "=== Logs (서비스별 심각도) ==="
  ch_query "SELECT service_name, severity_text, count(*) AS cnt FROM ${CH_DB}.logs GROUP BY service_name, severity_text ORDER BY service_name, cnt DESC FORMAT TSV" | \
    while IFS=$'\t' read -r svc sev cnt; do
      printf "  %-25s  %-10s  %-6s 건\n" "$svc" "$sev" "$cnt"
    done

  echo ""
  info "=== Logs 최신 5건 ==="
  ch_query "SELECT formatDateTime(toDateTime(timestamp_nano/1000000000), '%H:%M:%S') AS ts, severity_text, service_name, substring(body, 1, 80) AS body FROM ${CH_DB}.logs ORDER BY timestamp_nano DESC LIMIT 5 FORMAT TSV" | \
    while IFS=$'\t' read -r ts sev svc body; do
      printf "  [%s] %-8s %-20s %s\n" "$ts" "$sev" "${svc:0:18}" "${body:0:60}"
    done

  echo ""
  info "=== ClickHouse 전체 테이블 row 수 ==="
  ch_query "SELECT table, sum(rows) AS total_rows FROM system.parts WHERE database='${CH_DB}' AND active=1 AND table IN ('spans','metrics','logs','metric_histograms','anomalies','rca_reports','red_baseline') GROUP BY table ORDER BY total_rows DESC FORMAT TSV" | \
    while IFS=$'\t' read -r table rows; do
      printf "  %-30s  %s rows\n" "$table" "$rows"
    done
  echo ""
}

# ─── Qdrant 확인 ─────────────────────────────────────────────────────────────
check_qdrant() {
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  info "Qdrant 컬렉션 확인..."
  local collections
  collections=$(curl -s --max-time 5 "http://localhost:${QDRANT_LOCAL_PORT}/collections" 2>/dev/null)
  if [[ -z "$collections" ]]; then
    err "Qdrant 응답 없음"
    return 1
  fi

  echo "$collections" | python3 -c "
import sys, json
data = json.load(sys.stdin)
cols = data.get('result',{}).get('collections',[])
print(f'  총 {len(cols)}개 컬렉션: {[c[\"name\"] for c in cols]}')
"

  echo ""
  info "=== 컬렉션별 벡터 수 ==="
  echo "$collections" | python3 -c "
import sys, json
data = json.load(sys.stdin)
cols = [c['name'] for c in data.get('result',{}).get('collections',[])]
for c in cols: print(c)
" | while read -r col; do
    local detail
    detail=$(curl -s --max-time 5 "http://localhost:${QDRANT_LOCAL_PORT}/collections/${col}" 2>/dev/null)
    local count status
    count=$(echo "$detail" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('result',{}).get('points_count',0))" 2>/dev/null || echo "?")
    status=$(echo "$detail" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('result',{}).get('status','?'))" 2>/dev/null || echo "?")
    printf "  %-30s  %s 포인트  [%s]\n" "$col" "$count" "$status"
  done

  echo ""
  info "=== apm_errors 컬렉션 최신 포인트 (있는 경우) ==="
  curl -s --max-time 10 -X POST \
    "http://localhost:${QDRANT_LOCAL_PORT}/collections/apm_errors/points/scroll" \
    -H "Content-Type: application/json" \
    -d '{"limit": 3, "with_payload": true, "with_vector": false}' 2>/dev/null | \
    python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    pts = data.get('result',{}).get('points',[])
    if not pts:
        print('  (포인트 없음 - ERROR 레벨 데이터 필요)')
    for p in pts:
        pl = p.get('payload',{})
        print(f'  id={p[\"id\"]}  svc={pl.get(\"service_name\",\"?\")}  err_type={pl.get(\"error_type\",\"?\")}')
        print(f'    body: {str(pl.get(\"body\",\"\"))[:80]}')
except Exception as e:
    print(f'  파싱 실패: {e}')
"
  echo ""
}

# ─── 테스트 데이터 전송 ────────────────────────────────────────────────────────
send_test_data() {
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  info "테스트 데이터 전송 (trace + metric + log + ERROR log)..."

  local NOW
  NOW=$(python3 -c "import time; print(int(time.time() * 1e9))")
  local TRACE_ID="$(python3 -c "import secrets; print(secrets.token_hex(16))")"
  local SPAN_ID="$(python3 -c "import secrets; print(secrets.token_hex(8))")"
  local BASE="http://localhost:${COLLECTOR_LOCAL_PORT}"

  # trace
  curl -sf -o /dev/null -w "" -X POST "${BASE}/v1/traces" -H "Content-Type: application/json" -d "{
    \"resourceSpans\": [{
      \"resource\": {\"attributes\": [{\"key\": \"service.name\", \"value\": {\"stringValue\": \"verify-test\"}}]},
      \"scopeSpans\": [{\"scope\": {\"name\": \"verify\"},
        \"spans\": [{
          \"traceId\": \"${TRACE_ID}\",
          \"spanId\": \"${SPAN_ID}\",
          \"name\": \"verify /test\",
          \"kind\": 2,
          \"startTimeUnixNano\": \"${NOW}\",
          \"endTimeUnixNano\": \"${NOW}\",
          \"status\": {\"code\": 1},
          \"attributes\": [{\"key\": \"http.method\", \"value\": {\"stringValue\": \"GET\"}}]
        }]
      }]
    }]
  }" && ok "trace 전송 완료"

  # metric
  curl -sf -o /dev/null -w "" -X POST "${BASE}/v1/metrics" -H "Content-Type: application/json" -d "{
    \"resourceMetrics\": [{
      \"resource\": {\"attributes\": [{\"key\": \"service.name\", \"value\": {\"stringValue\": \"verify-test\"}}]},
      \"scopeMetrics\": [{\"scope\": {\"name\": \"verify\"},
        \"metrics\": [{
          \"name\": \"verify.request.count\",
          \"sum\": {
            \"dataPoints\": [{\"startTimeUnixNano\": \"${NOW}\", \"timeUnixNano\": \"${NOW}\", \"asInt\": \"1\"}],
            \"aggregationTemporality\": 2,
            \"isMonotonic\": true
          }
        }]
      }]
    }]
  }" && ok "metric 전송 완료"

  # ERROR log (Qdrant RAG 적재 트리거용)
  curl -sf -o /dev/null -w "" -X POST "${BASE}/v1/logs" -H "Content-Type: application/json" -d "{
    \"resourceLogs\": [{
      \"resource\": {\"attributes\": [{\"key\": \"service.name\", \"value\": {\"stringValue\": \"verify-test\"}}]},
      \"scopeLogs\": [{\"scope\": {\"name\": \"verify\"},
        \"logRecords\": [
          {
            \"timeUnixNano\": \"${NOW}\",
            \"severityNumber\": 17,
            \"severityText\": \"ERROR\",
            \"body\": {\"stringValue\": \"NullPointerException in UserController.getUser: user not found\"},
            \"traceId\": \"${TRACE_ID}\",
            \"spanId\": \"${SPAN_ID}\",
            \"attributes\": [
              {\"key\": \"exception.type\", \"value\": {\"stringValue\": \"NullPointerException\"}},
              {\"key\": \"exception.message\", \"value\": {\"stringValue\": \"user not found\"}}
            ]
          },
          {
            \"timeUnixNano\": \"${NOW}\",
            \"severityNumber\": 9,
            \"severityText\": \"INFO\",
            \"body\": {\"stringValue\": \"verify test log from verify-k8s-data.sh\"},
            \"traceId\": \"${TRACE_ID}\",
            \"spanId\": \"${SPAN_ID}\"
          }
        ]
      }]
    }]
  }" && ok "log (ERROR + INFO) 전송 완료"

  info "ClickHouse flush 대기 (3초)..."
  sleep 3
}

# ─── pod 상태 확인 ────────────────────────────────────────────────────────────
check_pods() {
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  info "k8s Pod 상태 (namespace: ${NS})"
  kubectl -n "$NS" get pods -o wide 2>/dev/null
  echo ""
  info "javi-collector 최신 로그 (10줄):"
  kubectl -n "$NS" logs deployment/javi-collector --tail=10 2>/dev/null | grep -v "^$" || true
}

# ─── 메인 ─────────────────────────────────────────────────────────────────────
CMD="${1:-all}"

case "$CMD" in
  clean)
    pkill -f "kubectl.*port-forward.*apm" 2>/dev/null || true
    ok "포트포워드 프로세스 정리 완료"
    exit 0
    ;;
  ch)
    start_port_forwards
    check_clickhouse
    ;;
  qdrant)
    start_port_forwards
    check_qdrant
    ;;
  send)
    start_port_forwards
    send_test_data
    check_clickhouse
    check_qdrant
    ;;
  all|*)
    check_pods
    start_port_forwards
    check_clickhouse
    check_qdrant
    ;;
esac

echo ""
ok "검증 완료"
