#!/usr/bin/env bash
# CLAUDE.md 의 "기계적" 구간(기술스택/명령어/디렉터리 구조)을 재생성한다.
# - 마커가 있으면 그 사이를 교체, 없으면 파일 끝에 추가.
# - 이 부분은 결정론적이라 Claude(AI) 없이 항상 같은 결과를 낸다.
set -uo pipefail

FILE="CLAUDE.md"
START="<!-- AUTO-GENERATED:start (스크립트가 관리. 직접 수정 금지) -->"
END="<!-- AUTO-GENERATED:end -->"

[ -f "$FILE" ] || { echo "CLAUDE.md 없음 → 종료"; exit 0; }

tmp="$(mktemp)"
{
  echo "$START"
  echo
  echo "_아래 구간은 스크립트가 자동 생성합니다. 직접 수정하지 마세요._"
  echo

  # ---- 기술 스택 (매니페스트 파일로 감지) ----
  echo "### 기술 스택"
  detected=0
  [ -f package.json ]      && { echo "- Node.js (\`package.json\`)"; detected=1; }
  [ -f pnpm-lock.yaml ]    && echo "- 패키지 매니저: pnpm"
  [ -f yarn.lock ]         && echo "- 패키지 매니저: yarn"
  [ -f package-lock.json ] && echo "- 패키지 매니저: npm"
  [ -f pyproject.toml ]    && { echo "- Python (\`pyproject.toml\`)"; detected=1; }
  [ -f requirements.txt ]  && { echo "- Python (\`requirements.txt\`)"; detected=1; }
  [ -f go.mod ]            && { echo "- Go (\`go.mod\`)"; detected=1; }
  [ -f Cargo.toml ]        && { echo "- Rust (\`Cargo.toml\`)"; detected=1; }
  [ -f Gemfile ]           && { echo "- Ruby (\`Gemfile\`)"; detected=1; }
  [ -f pom.xml ]           && { echo "- Java/Maven (\`pom.xml\`)"; detected=1; }
  { [ -f build.gradle ] || [ -f build.gradle.kts ]; } && { echo "- Java/Gradle"; detected=1; }
  [ -f Dockerfile ]        && echo "- Docker (\`Dockerfile\`)"
  [ "$detected" = 0 ]      && echo "- (자동 감지된 매니페스트 없음)"
  echo

  # ---- 명령어 ----
  echo "### 명령어"
  if [ -f package.json ] && command -v jq >/dev/null 2>&1; then
    echo "**package.json scripts**:"
    echo '```'
    jq -r '.scripts // {} | to_entries[] | "\(.key) → \(.value)"' package.json
    echo '```'
  fi
  if [ -f Makefile ]; then
    echo "**Make 타깃**:"
    echo '```'
    grep -E '^[a-zA-Z0-9_.-]+:' Makefile | sed 's/:.*//' | grep -v '^\.' | sort -u
    echo '```'
  fi
  echo

  # ---- 최상위 디렉터리 구조 (스테이징/추적 파일 기준 → 첫 커밋·CI 모두 동작) ----
  echo "### 최상위 디렉터리 구조"
  echo '```'
  git ls-files 2>/dev/null | awk -F/ 'NF>1{print $1}' | sort -u | head -60
  echo '```'
  echo
  echo "$END"
} > "$tmp"

if grep -qF "$START" "$FILE"; then
  # 기존 마커 구간을 새 블록으로 교체
  awk -v s="$START" -v e="$END" -v bf="$tmp" '
    index($0,s){ while((getline l < bf)>0) print l; skip=1; next }
    index($0,e){ skip=0; next }
    !skip{ print }
  ' "$FILE" > "$FILE.new" && mv "$FILE.new" "$FILE"
else
  # 마커가 없으면 파일 끝에 새로 추가
  { echo; cat "$tmp"; } >> "$FILE"
fi
rm -f "$tmp"
echo "기계적 구간 갱신 완료"
