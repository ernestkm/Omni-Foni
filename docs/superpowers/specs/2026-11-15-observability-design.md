# Observability slice — design

Date: 2026-11-15
Status: approved design, pre-plan
Scope: P0 foundations debt — end-to-end tracing and structured, correlated, redacted logs across all ten services.

## 1. Goal

Close the P0 acceptance items "every service emits traces" and the §9 standard "structured JSON logging with tenant, request, and session correlation IDs, PII redacted at the log layer". After this slice, one call produces a single trace spanning switching → SBC → routing → catalogue → charging → Kafka → mediation → billing, and every log line carries `service`, `trace_id`, `span_id` and `request_id` with PII masked.

## 2. Decisions taken in brainstorming

- **Scope**: OpenTelemetry traces *and* JSON logs with correlation IDs and redaction. Metrics, log backends, Grafana and DB spans are out.
- **Backend**: Jaeger all-in-one (Apache-2.0) in the dev stack, OTLP-native on 4317, UI on 16686. No collector in this slice; services export directly. A collector can be inserted later without code changes.
- **Depth**: gRPC/HTTP interceptors on every service plus manual spans on the cross-process hops (Kafka publish/consume, reconciliation sweep) so traces stay connected end to end.
- **Redaction**: key-based masking in the log handler. Keys matching `phone|msisdn|caller|callee|destination|email|token|password|authorization|secret` are masked (e.g. `+2782***1234`); `tenant_id`, `account_id`, `call_id` and `trunk_id` are kept intact. Destination numbers are masked in span attributes too.
- **Approach**: one shared Go module `libs/observability`, imported via `replace` by each service (the existing sibling-`replace` pattern), not ten copies.

## 3. Architecture

```
service main.go -- observability.Init(ctx, name) --> tracer provider --OTLP/gRPC--> jaeger :4317
       |                                                    |
       +-- grpc server/client interceptors -----------------+--> spans (auto)
       +-- http middleware (identity, payments) ------------+--> spans (auto)
       +-- kafka inject/extract traceparent ----------------+--> spans (manual)
       +-- slog JSON handler + correlation + redaction -----+--> stdout logs
```

Observability is **fail-open**: an unreachable exporter, a bad endpoint or an `Init` error logs once and leaves a no-op tracer in place. The money path never fails because telemetry is unavailable.

## 4. Components

### 4.1 `services/observability` (new module)

| File | Responsibility |
|---|---|
| `obs.go` | `Init(ctx, service string) (shutdown func(context.Context) error, err error)`; builds a `TracerProvider` with a batcher and OTLP gRPC exporter. Reads `OTEL_EXPORTER_OTLP_ENDPOINT` (default `jaeger:4317`; `off` = no-op exporter) and `OBS_SAMPLE_RATIO` (default `1.0`, parent-based trace-ID-ratio sampler). Sets the global provider and returns a shutdown that flushes with a bounded timeout. |
| `log.go` | `SetupLogging(service string)`: installs a `slog.JSONHandler` on stdout behind a handler that adds `service`, `trace_id`, `span_id` (from `trace.SpanContextFromContext`) and `request_id` (from context), then applies redaction. `NewHandler(inner, service)` adds correlation and redacts the denylisted keys; `Mask(v)` keeps a five-character prefix and the last four characters (nine characters or fewer are fully masked). |
| `grpc.go` | `ServerOptions() []grpc.ServerOption` (otelgrpc stats handler + unary interceptor for request-ID extraction/generation); `ClientOptions() []grpc.DialOption` (otelgrpc stats handler + unary interceptor that propagates `x-request-id`). Span names are otelgrpc's defaults. |
| `http.go` | `Middleware(next http.Handler) http.Handler`: starts a server span, reads `x-request-id` or generates a UUID, stores both IDs in the context, echoes the request ID back. |
| `kafka.go` | `InjectKafka(ctx, *kgo.Record)` / `ExtractKafka(ctx, *kgo.Record) context.Context`: W3C `traceparent` in record headers. `ConsumerSpan(ctx, name, callID)` starts a span with the extracted parent. |
| `context.go` | `WithRequestID` / `RequestIDFrom` and `NewRequestID` (crypto-random UUIDv4). |

### 4.2 Manual spans

| Location | Span | Attributes |
|---|---|---|
| switching `StartCall` / `StopCall` | `switching.start_call`, `switching.stop_call` | `call_id`, `account`, `trunk_id` |
| SBC `CheckCall` | `sbc.check_call` | `trunk_id`, `deny_reason` when denied |
| routing `ResolveLcr` | `routing.resolve_lcr` | `destination` (masked), `trunk_id`, `cost_minor` |
| charging `Authorize` / `Charge` | `charging.authorize`, `charging.charge` | `account`, `destination` (masked); `session_id`, `total_seconds` on charge |
| catalogue `GetRate` | `catalogue.get_rate` | `destination` (masked), `matched_prefix`, `rate_per_sec` |
| mediation pipeline | `mediation.process` per message | `call_id` |
| billing `Handle` | `billing.handle` | `call_id`, `account` |
| reconciliation sweep | `reconciliation.sweep` per month | `period`, `accounts` |

Automatic spans from the interceptors cover every other gRPC/HTTP call; no span is started for Postgres queries in this slice.

### 4.3 Wiring per service

- `main.go`: `observability.SetupLogging(name)` then `shutdown, _ := observability.Init(ctx, name)`; defer shutdown.
- gRPC servers: `grpc.NewServer(observability.ServerOptions()...)`; all client dials add `observability.ClientOptions()...`.
- identity and payments HTTP: handlers wrapped in `observability.Middleware`.
- Kafka: switching injects on publish; mediation extracts on consume and injects on republish; billing extracts on consume. Consumer spans wrap message handling.
- Identity/payments never log tokens, passwords or bearer headers; the existing 401 semantics are unchanged.

## 5. Log schema

One JSON object per line:

```json
{"time":"2026-11-15T10:00:00Z","level":"INFO","msg":"call ended","service":"switching","trace_id":"…","span_id":"…","request_id":"…","call_id":"…","account":"…","destination":"+2782***1234"}
```

Absent correlation fields are omitted rather than emitted empty. Redaction runs after correlation injection, so the handler cannot leak a masked key through an attribute.

## 6. Error handling

- `Init` failure or empty endpoint: log once at `WARN`, continue with the global no-op provider.
- Export failure: batcher drops spans; no retry loop blocks request handling.
- Shutdown: flush with a 5 s bound, then return; a failed flush is logged, not fatal.
- Log handler never panics on unexpected attribute types; it falls back to `slog.AnyValue`.

## 7. Testing

- Unit: `log_test.go` (redaction of every denylisted key, masking shape, correlation fields present/absent, non-string values); `context_test.go` (request-ID round-trip, UUID shape); `kafka_test.go` (inject/extract round-trip, missing/invalid header); `grpc_test.go` (server interceptor generates and echoes a request ID, client propagates an existing one, span name shape).
- Integration: `tracetest` in-memory exporter against a real gRPC server/client pair (bufconn with `passthrough:///bufnet`): assert one parent→child tree and expected attributes.
- End to end: run the stack, place a call via the switching smoke path, then assert Jaeger `/api/services` contains `switching`, `mediation` and `billing`; assert a trace for the call spans at least switching → charging.
- `make smoke` asserts Jaeger is healthy and that `reconciliation` appears in `/api/services` after the live probe.

## 8. Ops

- `docker-compose.yml`: `jaeger` service (`jaegertracing/all-in-one`, UI 16686, OTLP 4317 exposed to the host for debugging), healthcheck; every service gets `OTEL_EXPORTER_OTLP_ENDPOINT=jaeger:4317` (reconciliation and billing also inherit `OBS_SAMPLE_RATIO`).
- Each service `go.mod` gains `replace omnifoni/observability => ../libs/observability`; each Dockerfile gains `COPY libs/observability` alongside the existing sibling copies.
- `AGENTS.md`: document the shared module, the Jaeger UI/OTLP ports, the env vars, and that traces are fail-open.
- `ops/smoke.sh`: Jaeger health + `/api/services` check.

## 9. Out of scope

Metrics and dashboards, Loki/Grafana/collector, DB query spans, sampling UI, alerting rules, SLO documents, and log shipping beyond stdout. These belong to later P0 hardening or P5 reporting work.

## 10. Acceptance criteria

1. Every service exports OTLP spans and shows up in Jaeger after handling traffic.
2. A single call yields one trace connecting switching, SBC, routing, catalogue, charging, mediation and billing.
3. Every service log line is JSON with `service`; lines emitted inside a request carry `trace_id`, `span_id` and `request_id`.
4. Denylisted PII keys are masked in logs and destination numbers are masked in span attributes.
5. An unreachable Jaeger does not fail any RPC, test or smoke run.
6. `make test` and `make smoke` pass with Jaeger up and with the exporter endpoint set to `off`.
