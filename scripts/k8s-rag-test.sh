#!/usr/bin/env bash
# k8s에 배포된 Ollama + Qdrant를 포트포워딩 후 RAG 통합 테스트 실행
#
# 사용법:
#   ./scripts/k8s-rag-test.sh             # Ollama E2E 테스트 (기본)
#   ./scripts/k8s-rag-test.sh integration # Qdrant 통합 테스트
#   ./scripts/k8s-rag-test.sh all         # 전체 integration 태그 테스트
#
# 사전 조건:
#   - kubectl 설정 완료 (kubectl -n apm get pods 로 확인)
#   - apm namespace에 ollama, qdrant 파드가 Running 상태

set -euo pipefail

NS="apm"
OLLAMA_LOCAL_PORT=11434
QDRANT_LOCAL_PORT=6333
TEST_TARGET="${1:-ollama}"
TIMEOUT="${TIMEOUT:-300s}"

cleanup() {
  echo ""
  echo "🧹 포트포워드 정리 중..."
  [[ -n "${OLLAMA_PF_PID:-}" ]] && kill "$OLLAMA_PF_PID" 2>/dev/null && echo "  ollama port-forward 종료"
  [[ -n "${QDRANT_PF_PID:-}" ]] && kill "$QDRANT_PF_PID" 2>/dev/null && echo "  qdrant port-forward 종료"
}
trap cleanup EXIT INT TERM

# ── 1. 클러스터 접근 확인 ──────────────────────────────────────────
echo "🔍 k8s 클러스터 상태 확인 중..."
if ! kubectl -n "$NS" get pods --request-timeout=10s &>/dev/null; then
  echo "❌ k8s 클러스터 접근 실패. 다음을 확인하세요:"
  echo "   kubectl config current-context"
  echo "   kubectl -n $NS get pods"
  exit 1
fi

# ── 2. Ollama/Qdrant 파드 상태 확인 ───────────────────────────────
echo "🔍 ollama/qdrant 파드 상태 확인..."
kubectl -n "$NS" get pods -l 'app in (ollama,qdrant)' --no-headers

OLLAMA_READY=$(kubectl -n "$NS" get pods -l app=ollama --no-headers 2>/dev/null | grep -c "Running" || true)
QDRANT_READY=$(kubectl -n "$NS" get pods -l app=qdrant --no-headers 2>/dev/null | grep -c "Running" || true)

if [[ "$OLLAMA_READY" -eq 0 ]]; then
  echo "❌ ollama 파드가 Running 상태가 아닙니다"
  echo "   kubectl -n $NS get pods -l app=ollama"
  exit 1
fi
if [[ "$QDRANT_READY" -eq 0 ]]; then
  echo "❌ qdrant 파드가 Running 상태가 아닙니다"
  echo "   kubectl -n $NS get pods -l app=qdrant"
  exit 1
fi
echo "  ✓ ollama: Running, qdrant: Running"

# ── 3. 기존 포트 점유 확인 후 포트포워딩 ──────────────────────────
echo ""
echo "🔌 포트포워드 설정 중..."

if lsof -ti:${OLLAMA_LOCAL_PORT} &>/dev/null; then
  echo "  ⚠️  ${OLLAMA_LOCAL_PORT} 포트가 이미 사용 중입니다. 기존 프로세스를 사용합니다."
  OLLAMA_PF_PID=""
else
  kubectl -n "$NS" port-forward svc/ollama ${OLLAMA_LOCAL_PORT}:11434 &>/tmp/ollama-pf.log &
  OLLAMA_PF_PID=$!
  echo "  ollama port-forward PID: $OLLAMA_PF_PID (localhost:${OLLAMA_LOCAL_PORT} → svc/ollama:11434)"
fi

if lsof -ti:${QDRANT_LOCAL_PORT} &>/dev/null; then
  echo "  ⚠️  ${QDRANT_LOCAL_PORT} 포트가 이미 사용 중입니다. 기존 프로세스를 사용합니다."
  QDRANT_PF_PID=""
else
  kubectl -n "$NS" port-forward svc/qdrant ${QDRANT_LOCAL_PORT}:6333 &>/tmp/qdrant-pf.log &
  QDRANT_PF_PID=$!
  echo "  qdrant port-forward PID: $QDRANT_PF_PID (localhost:${QDRANT_LOCAL_PORT} → svc/qdrant:6333)"
fi

# ── 4. 서비스 준비 대기 ────────────────────────────────────────────
echo ""
echo "⏳ 서비스 응답 대기 중 (최대 30초)..."

wait_for_service() {
  local name=$1
  local url=$2
  local max=30
  for i in $(seq 1 $max); do
    if curl -sf "$url" -o /dev/null 2>/dev/null; then
      echo "  ✓ $name 준비 완료 (${i}초)"
      return 0
    fi
    sleep 1
  done
  echo "  ❌ $name 응답 없음 (${max}초 타임아웃)"
  return 1
}

wait_for_service "ollama" "http://localhost:${OLLAMA_LOCAL_PORT}/api/tags"
wait_for_service "qdrant" "http://localhost:${QDRANT_LOCAL_PORT}/healthz"

# ── 5. Ollama 모델 확인 ────────────────────────────────────────────
echo ""
echo "🤖 Ollama 모델 확인..."
MODELS=$(curl -sf "http://localhost:${OLLAMA_LOCAL_PORT}/api/tags" | python3 -c "import sys,json; models=json.load(sys.stdin).get('models',[]); [print('  -', m['name']) for m in models]" 2>/dev/null || echo "  (파이썬 없음 — 모델 목록 생략)")
echo "$MODELS"

# ── 6. 테스트 실행 ─────────────────────────────────────────────────
echo ""
echo "🧪 RAG 통합 테스트 실행 중..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

cd "$(dirname "$0")/.."

case "$TEST_TARGET" in
  ollama)
    echo "  대상: TestOllamaRAGPipeline (E2E Ollama + Qdrant)"
    go test -tags integration ./internal/rag/... \
      -v -run TestOllamaRAGPipeline \
      -timeout "$TIMEOUT"
    ;;
  integration)
    echo "  대상: TestQdrantIntegration_UpsertAndSearch"
    go test -tags integration ./internal/rag/... \
      -v -run TestQdrantIntegration_UpsertAndSearch \
      -timeout 60s
    ;;
  all)
    echo "  대상: 모든 integration 테스트"
    go test -tags integration ./internal/rag/... \
      -v -timeout "$TIMEOUT"
    ;;
  *)
    echo "  알 수 없는 대상: $TEST_TARGET"
    echo "  사용법: $0 [ollama|integration|all]"
    exit 1
    ;;
esac

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ 테스트 완료"
