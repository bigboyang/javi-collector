# Javi APM Agent - Implementation Patterns & Architecture Notes

## Sampler Integration Point
SdkSpanBuilder.startSpan() line 90 hardcodes `sampled=true`:
  SpanContext context = SpanContext.create(traceId, spanId, parentSpanId, true);
Sampler interface must be injected into TracerSharedState and called here before SpanContext creation.

## Context Propagation Gap
Context.java uses pure ThreadLocal<Span>. This breaks when:
- CompletableFuture.supplyAsync() / thenApplyAsync()
- Spring @Async methods
- JDBC async queries
Fix: wrap Runnable/Callable in ContextSnapshot before submitting to thread pool.

## OTLP Exporter Requirements (Java 11)
- Use java.net.http.HttpClient (built-in, no extra deps)
- Protobuf serialization: either google protobuf 3.x or manual JSON encoding
- Endpoint: http://localhost:4318/v1/traces (OTLP/HTTP)
- Headers: Content-Type: application/x-protobuf (or application/json for JSON)
- Retry: exponential backoff with jitter, max 3 retries
- Timeout: connect 5s, request 10s
- No new external deps needed if using JSON encoding (Jackson already common)

## Metrics Pipeline Design
MeterProvider -> Meter -> Instrument (Counter/Gauge/Histogram/UpDownCounter)
- MetricReader: PeriodicMetricReader (collect every 60s by default)
- MetricExporter: OtlpHttpMetricExporter or PrometheusExporter
- Exemplars: link active trace context to histogram measurements

Key instruments for HTTP APM:
- http.server.request.duration: Histogram (ms buckets: 5,10,25,50,100,250,500,1000)
- http.server.active_requests: UpDownCounter
- http.server.request.body.size: Histogram
- jvm.memory.used: Gauge (via MemoryMXBean)
- jvm.gc.duration: Histogram (via GarbageCollectorMXBean)

## AIOps Data Requirements
For anomaly detection / root cause analysis:
1. Trace data with full attribute set (http.method, http.route, http.status_code, db.statement)
2. Latency percentiles (p50/p95/p99) per endpoint - needs Histogram
3. Error rate per endpoint per time window
4. JVM metrics correlated with trace degradation events
5. Service dependency map (derived from span parent/child relationships)
6. Span events with timestamps for slow query annotation

## Shutdown Hook Pattern
In AgentRuntime or SimpleAgent.premain():
  Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      PROVIDER.forceFlush().join(5, TimeUnit.SECONDS);
      PROVIDER.shutdown().join(5, TimeUnit.SECONDS);
  }, "agent-shutdown"));

## ByteBuddy Instrumentation Expansion
Current: ElementMatchers.nameContains("UserController")
For production: use annotation-based matching
  ElementMatchers.isAnnotatedWith(named("org.springframework.web.bind.annotation.RestController"))
  .or(isAnnotatedWith(named("org.springframework.stereotype.Controller")))
