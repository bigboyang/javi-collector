# APM Pipeline Expert - Project Memory

## Project: Javi APM Agent
- Location: /Users/kkc/APM
- Language: Java 11, Maven
- Build: maven-shade-plugin produces fat JAR with Premain-Class=com.agent.SimpleAgent

## Current Implementation State (as of 2026-03-05)
- ByteBuddy 1.14.12: instruments `UserController` (nameContains match) via ControllerMethodAdvice
- Trace pipeline: SdkSpan -> SdkSpanBuilder -> SpanProcessor -> SpanExporter
- Processors: BatchSpanProcessor (ArrayBlockingQueue), SimpleSpanProcessor, CompositeSpanProcessor
- Exporter: LoggingSpanExporter only (console stdout via System.out.println)
- Context propagation: ThreadLocal-based (Context.java), W3C TraceContextPropagator
- SpanLimits: 128 attrs/events/links, dropped counters tracked with AtomicInteger
- AgentRuntime: static singleton holding SdkTracerProvider + LoggingSpanExporter

## Empty Packages (not yet implemented)
- com.agent.metrics/ - no Meter, Counter, Gauge, Histogram
- com.agent.sampler/ - no Sampler interface or implementations
- com.agent.config/ - no AgentConfig, no property loading

## Critical Gaps for Production APM
1. No OTLP exporter (HTTP/gRPC) - data stays local
2. No Sampler - all spans are always sampled (hardcoded `sampled=true` in SdkSpanBuilder)
3. No Metrics pipeline (MeterProvider, instruments)
4. No async context propagation (ThreadLocal breaks in CompletableFuture/reactive)
5. No JDBC instrumentation
6. No HTTP client instrumentation (outbound)
7. No shutdown hook in AgentRuntime (SdkTracerProvider.shutdown never called)
8. SimpleSpanProcessor blocks caller thread during export

## Known Code Issues
- SdkSpanBuilder line 90: `SpanContext.create(traceId, spanId, parentSpanId, true)` - sampled is hardcoded `true`
- AgentRuntime: no JVM shutdown hook, BatchSpanProcessor daemon thread will die without flush
- SimpleAgent: only instruments `nameContains("UserController")` - not generic
- LoggingSpanExporter: uses System.out.println, not JUL/SLF4J structured output
- Context.java: pure ThreadLocal - breaks with async/reactive code patterns

## Test App
- Spring Boot + Spring Data JPA + H2
- UserController (REST CRUD) -> UserService -> UserRepository (JPA)
- H2 DB files ignored in gitignore

## Key File Paths
- Agent entry: /Users/kkc/APM/agent/src/main/java/com/agent/SimpleAgent.java
- Advice: /Users/kkc/APM/agent/src/main/java/com/agent/instrumentation/ControllerMethodAdvice.java
- Provider: /Users/kkc/APM/agent/src/main/java/com/agent/trace/SdkTracerProvider.java
- BatchProcessor: /Users/kkc/APM/agent/src/main/java/com/agent/trace/processor/BatchSpanProcessor.java
- Context: /Users/kkc/APM/agent/src/main/java/com/agent/span/Context.java

## Config Server OTLP Collector Design (as of 2026-03-11)
- Package root: com.javi.configserver.collector/
- Sub-packages: controller/, decoder/, model/, service/, store/
- Entry point: OtlpController handles /v1/traces, /v1/metrics, /v1/logs
- Content-Type: application/x-protobuf; Spring uses ByteArrayHttpMessageConverter (byte[] param)
- ProtoDecoder: hand-written varint/wire reader (no protobuf lib dependency)
  - sliceMessage() copies sub-message bytes; allows GC of parent buffer after decoding
  - skipField() handles all 4 wire types for forward compatibility
- SignalStore<T>: ArrayBlockingQueue<T> with fixed capacity; offer() never blocks (drop-on-full)
  - AtomicLong totalAccepted + totalDropped; log every 1000 drops (no flooding)
- Store capacities (application.properties): trace=10000, metric=5000, log=5000 (~5MB max for traces)
- Response: byte[0] (valid empty proto) for 200 OK; agent checks HTTP 200 only
- Stats endpoint: GET /v1/collector/stats -> JSON with queue depth, drop rate per signal
- Agent OTLP endpoint: http://localhost:18888/v1/traces (same port as config server)

## Critical Wire Format Notes (decoder traps)
- Span field 7/8 (start/end time): wire type 1 (fixed64), NOT varint — tag bytes are 0x39/0x41
- trace_id/span_id: wire type 2 (LEN), raw bytes -> lowercase 32/16 hex chars
- repeated fields (attributes, spans): same field number appears multiple times; must accumulate not overwrite

## See Also
- patterns.md: detailed implementation patterns for this project
