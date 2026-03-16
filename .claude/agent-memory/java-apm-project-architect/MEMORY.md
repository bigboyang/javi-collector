# Java APM Project Architect - Memory

## 프로젝트 기본 정보

- **프로젝트명**: javi-agent (Javi)
- **목적**: Java 기반 자체 APM 에이전트
- **빌드 도구**: Maven (단일 모듈, 멀티모듈 아님)
- **Java 버전**: Agent=11, TestApp=17
- **바이트코드 라이브러리**: ByteBuddy 1.14.12 (ASM/Javassist 미사용)
- **패키징**: maven-shade-plugin (fat jar), Premain-Class=com.agent.SimpleAgent

## 현재 구조 (2026-03-05 기준)

### 모듈 구성
- `agent/` : APM 에이전트 본체 (com.agent)
- `test-app/` : 계측 대상 Spring Boot 3.2 앱 (Spring Web + JPA + H2)

### 에이전트 패키지 구성
```
com.agent
  SimpleAgent            - premain 엔트리포인트 (ByteBuddy 등록)
  instrumentation/
    AgentRuntime         - 전역 TracerProvider/Tracer 싱글톤
    ControllerMethodAdvice - ByteBuddy Advice (UserController 대상)
  span/
    Span (interface), SdkSpan, NoopSpan, PropagatedSpan
    SpanBuilder, SdkSpanBuilder, NoopSpanBuilder
    SpanContext, SpanKind, SpanStatus, SpanEvent, SpanLink, SpanLimits
    TraceFlags, TraceState, AttributeKey
    Context (ThreadLocal), Scope, SpanLogger
  trace/
    Tracer, SdkTracer, NoopTracer
    TracerProvider, SdkTracerProvider
    TracerBuilder, SdkTracerBuilder
    TracerConfig, TracerSharedState, TraceId, InstrumentationScopeInfo
    exporter/ : SpanExporter, LoggingSpanExporter, NoopSpanExporter, CompositeSpanExporter
    processor/ : SpanProcessor, SimpleSpanProcessor, BatchSpanProcessor,
                 NoopSpanProcessor, CompositeSpanProcessor
  propagation/
    TextMapPropagator, TextMapGetter, TextMapSetter
    TraceContextPropagator (W3C traceparent/tracestate)
    ContextPropagators (global singleton)
    Propagation (inject/extract helpers)
    MultiTextMapPropagator, NoopTextMapPropagator
    MapTextMapGetter, MapTextMapSetter
  common/utils/
    concurrent/ : CompletableResultCode
    time/ : Clock, SystemClock, AnchoredClock
    generator/ : IdGenerator, RandomIdGenerator, RandomSupplier, AndroidFriendlyRandomHolder
    core/ : TemporaryBuffers
    encode/ : EncodingUtils
  logs/
    sdkLog/ : ApiUsageLogger
    tmp/ : (비어있음)
```

### 비어있는 패키지 (미구현)
- `com.agent.config/` : 설정 시스템 없음
- `com.agent.metrics/` : 메트릭 수집 없음
- `com.agent.sampler/` : 샘플러 없음

## 핵심 아키텍처 결정사항

1. **ByteBuddy 선택**: ASM 대비 유지보수성 우수
2. **ThreadLocal 컨텍스트**: Context 클래스로 현재 Span 관리 (비동기 미지원)
3. **W3C traceparent 표준**: TraceContextPropagator로 분산 추적 헤더 처리
4. **BatchSpanProcessor**: 별도 데몬 스레드로 비동기 배치 export
5. **단일 모듈**: 멀티모듈 분리 없이 단일 Maven 모듈로 구성 (현재 단계)

## 미구현 핵심 컴포넌트 (우선순위순)

1. Config 시스템 (service.name, sampling rate, exporter URL 등)
2. Sampler (AlwaysOn/AlwaysOff/TraceIdRatioBased)
3. OTLP/HTTP Exporter (현재 콘솔 출력만)
4. JDBC 계측 플러그인
5. HTTP 서블릿/Spring MVC 계측 (현재 UserController만 하드코딩)
6. 비동기 컨텍스트 전파 (CompletableFuture, @Async)
7. 메트릭 수집 (Meter/Counter/Timer/Gauge)
8. MDC 연동 (traceId/spanId 로그 삽입)
9. 단위/통합 테스트 (TestApp 스텁 수준)
10. 멀티모듈 분리

## 전체 완성도 추정

- 핵심 Trace/Span 모델: ~85%
- 컨텍스트 전파: ~70%
- Processor/Exporter 파이프라인: ~65%
- ByteBuddy 계측: ~15% (UserController 하드코딩)
- Config/Sampler/Metrics: 0%
- 테스트: ~5%
- **전체 완성도: 약 25~30%**

## 링크
- 상세 분석: `analysis-2026-03-05.md`
