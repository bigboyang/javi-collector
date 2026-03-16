# /dev - Feature Implementation Command

사용자의 기능 요청을 분석하고, 전문 에이전트들을 활용하여 구현합니다.
이 프로젝트의 목표는 자체 Java Agent APM (AIOps)를 만드는 것 입니다.

## 참조
먼저 '.claude/skills/' 폴더의 모든 '.md' 파일을 읽어 규칙과 컨벤션을 참조하세요

## 요청 내용
$ARGUMENTS

## 실행 지침

다음 단계를 순서대로 수행하세요:

### 1단계: 요청 분석 및 계획 수립

요청을 분석하여 다음을 파악하세요:
- 구현할 기능의 범위와 목적
- 관련된 모듈 및 컴포넌트 (agent-core, agent-instrumentation, agent-transport 등)
- 어떤 전문 에이전트가 필요한지
- 구현 순서 및 의존성

### 2단계: 스킬 참조

구현 전 `/Users/kkc/APM/.claude/skills/code.rules.md`를 읽어 프로젝트 코드 규칙을 확인하세요.

### 3단계: 에이전트 라우팅

요청 유형에 따라 아래 전문 에이전트를 적절히 사용하세요:

| 요청 유형 | 사용할 에이전트 |
|-----------|----------------|
| 프로젝트 구조, WBS, 일정, 산추출 | `java-apm-project-architect` |
| ByteBuddy, 바이트코드 인스트루멘테이션, Java Agent | `java-agent-bytebuddy-expert` |
| 트레이싱, 메트릭, 로깅, OpenTelemetry | `apm-observability-expert` |
| 데이터 파이프라인, gRPC, 전송, 직렬화 | `apm-pipeline-expert` |
| 성능 측정, GC 영향, 오버헤드 분석 | `perf-overhead-inspector` |
| ML 기반 이상 감지, AIOps | `aiops-mlops-expert` |

복합적인 기능이라면 여러 에이전트를 병렬 또는 순차적으로 사용하세요.

### 4단계: 구현

에이전트가 반환한 결과를 바탕으로 실제 코드를 작성하고 파일에 반영하세요.
- 기존 코드를 먼저 읽고 패턴을 파악한 후 수정하세요
- 불필요한 파일 생성을 피하고 기존 파일을 수정하세요

### 5단계: 결과 보고

구현 완료 후 다음을 보고하세요:
- 변경된 파일 목록
- 구현 내용 요약
- 테스트 방법 (필요시)
- 추가 고려 사항 또는 후속 작업