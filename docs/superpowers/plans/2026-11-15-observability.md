# Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every Omni-Foní service end-to-end OpenTelemetry traces (Jaeger backend) and structured JSON logs with correlation IDs and PII masking.

**Architecture:** A new shared Go module `services/observability` provides tracer initialization, a correlated/redacting slog handler, gRPC/HTTP propagation helpers, and Kafka `traceparent` injection. Each service imports it, wraps its servers/clients, and adds manual spans on cross-process hops. Jaeger all-in-one receives OTLP on 4317.

**Tech Stack:** Go 1.27, OpenTelemetry SDK + OTLP gRPC exporter, `otelgrpc`, `otelhttp`, `log/slog`, franz-go, Jaeger all-in-one.

## Global Constraints

- Go modules live under `services/`; shared module is `omnifoni/observability` at `services/observability`, imported with `replace omnifoni/observability => ../observability`.
- Docker build context for every service is `./services`; Dockerfiles copy siblings explicitly (`COPY <dir>/ ./<dir>/`). A missing sibling COPY fails the build.
- Services are their own modules; `go.mod`/`go.sum` changes are part of the same commit as the import that needs them.
- Telemetry is fail-open: an exporter or Jaeger outage must never fail an RPC, test, or smoke run.
- PII keys `phone|msisdn|caller|callee|email|token|password|authorization|secret` are masked in logs; destination numbers are masked in span attributes via `observability.Mask`.
- Do not cache tracers at package init (`var tracer = otel.Tracer(...)`); call `otel.Tracer(name)` inside the function so tests can swap the global provider.
- `make test` runs `go vet ./... && go test ./...` per module and needs the dev stack up; `make smoke` runs `ops/smoke.sh`.
- Regenerate stubs only with `make protoc`; this slice adds no protos.

---

### Task 1: Observability module core

**Files:**
- Create: `services/observability/go.mod`, `services/observability/obs.go`, `services/observability/context.go`, `services/observability/log.go`
- Test: `services/observability/obs_test.go`, `services/observability/context_test.go`, `services/observability/log_test.go`
- Modify: `Makefile:18-28` (add the module to `test`)

**Interfaces:**
- Produces: `Init(ctx context.Context, service string) (func(context.Context) error, error)`, `SetupLogging(service string)`, `NewHandler(inner slog.Handler, service string) slog.Handler`, `Mask(v string) string`, `NewRequestID() string`, `WithRequestID(ctx, id) context.Context`, `RequestIDFrom(ctx) string`.

- [ ] **Step 1: Create the module**

Run:
```bash
mkdir -p services/observability
cd services/observability
go mod init omnifoni/observability
go get go.opentelemetry.io/otel@latest go.opentelemetry.io/otel/sdk@latest go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@latest github.com/google/uuid@latest
```

- [ ] **Step 2: Write the failing tests**

Create `services/observability/context_test.go`:
```go
package observability

import (
	"context"
	"testing"
)

func TestRequestIDRoundTrip(t *testing.T) {
	ctx := WithRequestID(context.Background(), "abc")
	if got := RequestIDFrom(ctx); got != "abc" {
		t.Fatalf("RequestIDFrom = %q, want abc", got)
	}
	if got := RequestIDFrom(context.Background()); got != "" {
		t.Fatalf("RequestIDFrom(empty) = %q, want empty", got)
	}
}

func TestNewRequestIDIsUnique(t *testing.T) {
	a, b := NewRequestID(), NewRequestID()
	if a == "" || a == b {
		t.Fatalf("ids not unique: %q %q", a, b)
	}
}
```

Create `services/observability/log_test.go`:
```go
package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func decodeLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode %q: %v", buf.String(), err)
	}
	return got
}

func TestHandlerMasksSensitiveKeys(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil), "switching"))
	log.Info("call", "destination", "+27821234567", "password", "hunter2", "account_id", "acc-1")

	got := decodeLine(t, &buf)
	if got["destination"] != "+2782***4567" {
		t.Errorf("destination = %v, want +2782***4567", got["destination"])
	}
	if got["password"] != "***" {
		t.Errorf("password = %v, want ***", got["password"])
	}
	if got["account_id"] != "acc-1" {
		t.Errorf("account_id = %v, want acc-1", got["account_id"])
	}
	if got["service"] != "switching" {
		t.Errorf("service = %v, want switching", got["service"])
	}
}

func TestHandlerAddsCorrelation(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil), "billing"))
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	ctx = WithRequestID(ctx, "req-1")
	log.InfoContext(ctx, "batch")

	got := decodeLine(t, &buf)
	if got["trace_id"] != sc.TraceID().String() {
		t.Errorf("trace_id = %v, want %s", got["trace_id"], sc.TraceID())
	}
	if got["span_id"] != sc.SpanID().String() {
		t.Errorf("span_id = %v, want %s", got["span_id"], sc.SpanID())
	}
	if got["request_id"] != "req-1" {
		t.Errorf("request_id = %v, want req-1", got["request_id"])
	}
}

func TestMask(t *testing.T) {
	if got := Mask("short"); got != "***" {
		t.Errorf("Mask(short) = %q, want ***", got)
	}
	if got := Mask("+27821234567"); got != "+2782***4567" {
		t.Errorf("Mask(phone) = %q, want +2782***4567", got)
	}
}
```

Create `services/observability/obs_test.go`:
```go
package observability

import (
	"context"
	"testing"
	"time"
)

func TestInitOffIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "off")
	shutdown, err := Init(context.Background(), "test")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd services/observability && go test ./...`
Expected: FAIL/undefined: `Init`, `NewHandler`, `Mask`, `WithRequestID`, `RequestIDFrom`, `NewRequestID`.

- [ ] **Step 4: Implement `context.go`**

```go
package observability

import (
	"context"

	"github.com/google/uuid"
)

type requestIDKey struct{}

// NewRequestID returns a random UUID used to correlate one logical request.
func NewRequestID() string { return uuid.NewString() }

// WithRequestID stores the request id in ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFrom returns the request id in ctx, or "" when absent.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
```

- [ ] **Step 5: Implement `log.go`**

```go
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

var sensitiveKeys = map[string]bool{
	"phone": true, "msisdn": true, "caller": true, "callee": true,
	"email": true, "token": true, "password": true, "authorization": true,
	"secret": true,
}

// SetupLogging installs the JSON handler on stdout with correlation and redaction.
func SetupLogging(service string) {
	slog.SetDefault(slog.New(NewHandler(slog.NewJSONHandler(os.Stdout, nil), service)))
}

// NewHandler wraps inner so every record carries the service name and any
// trace/request correlation in ctx, with sensitive values masked.
func NewHandler(inner slog.Handler, service string) slog.Handler {
	return &handler{inner: inner, service: service}
}

type handler struct {
	inner   slog.Handler
	service string
}

func (h *handler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(redactAttr(a))
		return true
	})
	out.AddAttrs(slog.String("service", h.service))
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		out.AddAttrs(slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()))
	}
	if id := RequestIDFrom(ctx); id != "" {
		out.AddAttrs(slog.String("request_id", id))
	}
	return h.inner.Handle(ctx, out)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	red := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		red = append(red, redactAttr(a))
	}
	return &handler{inner: h.inner.WithAttrs(red), service: h.service}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{inner: h.inner.WithGroup(name), service: h.service}
}

func redactAttr(a slog.Attr) slog.Attr {
	if sensitiveKeys[strings.ToLower(a.Key)] {
		return slog.String(a.Key, Mask(a.Value.String()))
	}
	if a.Value.Kind() == slog.KindGroup {
		attrs := a.Value.Group()
		out := make([]slog.Attr, 0, len(attrs))
		for _, ga := range attrs {
			out = append(out, redactAttr(ga))
		}
		any := make([]any, len(out))
		for i := range out {
			any[i] = out[i]
		}
		return slog.Group(a.Key, any...)
	}
	return a
}

// Mask keeps a short prefix and suffix so logs stay correlatable without
// exposing the value. Values of nine characters or fewer are fully masked.
func Mask(v string) string {
	if len(v) <= 9 {
		return "***"
	}
	return v[:5] + "***" + v[len(v)-4:]
}
```

- [ ] **Step 6: Implement `obs.go`**

```go
package observability

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Init installs the global tracer provider and propagator. An endpoint of
// "off" installs a no-op provider (tests, exporter-less runs). Init is
// fail-open: callers log the error and keep serving.
func Init(ctx context.Context, service string) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "jaeger:4317"
	}
	if endpoint == "off" {
		return func(context.Context) error { return nil }, nil
	}
	ratio := 1.0
	if v := os.Getenv("OBS_SAMPLE_RATIO"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			ratio = f
		}
	}
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return func(context.Context) error { return nil }, fmt.Errorf("otlp exporter: %w", err)
	}
	res := resource.NewSchemaless(attribute.String("service.name", service))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown, nil
}
```

- [ ] **Step 7: Add the module to `make test`**

Modify `Makefile` in the `test` target, after the mediation line:
```make
	cd services/observability && go vet ./... && go test ./...
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `cd services/observability && go mod tidy && go vet ./... && go test ./...`
Expected: `ok omnifoni/observability`.

- [ ] **Step 9: Commit**

```bash
git add services/observability Makefile
git commit -m "feat(observability): shared tracer, correlated JSON logs, redaction"
```

---

### Task 2: Propagation helpers (gRPC, HTTP, Kafka)

**Files:**
- Create: `services/observability/grpc.go`, `services/observability/http.go`, `services/observability/kafka.go`
- Test: `services/observability/grpc_test.go`, `services/observability/http_test.go`, `services/observability/kafka_test.go`

**Interfaces:**
- Consumes: `RequestIDFrom`, `WithRequestID`, `NewRequestID` (Task 1).
- Produces: `ServerOptions() []grpc.ServerOption`, `ClientOptions() []grpc.DialOption`, `Middleware(http.Handler) http.Handler`, `InjectKafka(ctx, *kgo.Record)`, `ExtractKafka(ctx, *kgo.Record) context.Context`, `ConsumerSpan(ctx, name, callID string) (context.Context, trace.Span)`.

- [ ] **Step 1: Write the failing tests**

Create `services/observability/grpc_test.go`:
```go
package observability

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestServerRequestIDGenerates(t *testing.T) {
	var got string
	_, err := serverRequestID(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, req any) (any, error) {
			got = RequestIDFrom(ctx)
			return nil, nil
		})
	if err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if got == "" {
		t.Fatal("no request id in context")
	}
}

func TestServerRequestIDEchoesIncoming(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "in-1"))
	var got string
	_, _ = serverRequestID(ctx, nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, req any) (any, error) {
			got = RequestIDFrom(ctx)
			return nil, nil
		})
	if got != "in-1" {
		t.Fatalf("request id = %q, want in-1", got)
	}
}

func TestClientRequestIDPropagates(t *testing.T) {
	ctx := WithRequestID(context.Background(), "out-1")
	var got string
	err := clientRequestID(ctx, "/x/Y", nil, nil, nil,
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			md, _ := metadata.FromOutgoingContext(ctx)
			if v := md.Get("x-request-id"); len(v) > 0 {
				got = v[0]
			}
			return nil
		})
	if err != nil {
		t.Fatalf("invoker: %v", err)
	}
	if got != "out-1" {
		t.Fatalf("outgoing request id = %q, want out-1", got)
	}
}
```

Create `services/observability/http_test.go`:
```go
package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddlewareRequestID(t *testing.T) {
	var got string
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = RequestIDFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodGet, "/me", nil)
	req.Header.Set("x-request-id", "http-1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got != "http-1" {
		t.Fatalf("context request id = %q, want http-1", got)
	}
	if rec.Header().Get("x-request-id") != "http-1" {
		t.Fatalf("response header = %q", rec.Header().Get("x-request-id"))
	}
}
```

Create `services/observability/kafka_test.go`:
```go
package observability

import (
	"context"
	"testing"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestKafkaTraceparentRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6},
		SpanID:     trace.SpanID{1, 1, 2, 2, 3, 3, 4, 4},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	rec := &kgo.Record{}
	InjectKafka(ctx, rec)
	if len(rec.Headers) == 0 {
		t.Fatal("no headers injected")
	}
	got := trace.SpanContextFromContext(ExtractKafka(context.Background(), rec))
	if got.TraceID() != sc.TraceID() {
		t.Fatalf("trace id = %s, want %s", got.TraceID(), sc.TraceID())
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd services/observability && go test ./...`
Expected: FAIL/undefined: `serverRequestID`, `clientRequestID`, `Middleware`, `InjectKafka`, `ExtractKafka`.

- [ ] **Step 3: Implement `grpc.go`**

```go
package observability

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const requestIDHeader = "x-request-id"

// ServerOptions returns the otelgrpc stats handler plus request-id propagation.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(serverRequestID),
	}
}

// ClientOptions returns the otelgrpc stats handler plus request-id propagation.
func ClientOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(clientRequestID),
	}
}

func serverRequestID(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if id := incomingRequestID(ctx); id != "" {
		ctx = WithRequestID(ctx, id)
	} else {
		ctx = WithRequestID(ctx, NewRequestID())
	}
	return handler(ctx, req)
}

func clientRequestID(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if id := RequestIDFrom(ctx); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, requestIDHeader, id)
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func incomingRequestID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if v := md.Get(requestIDHeader); len(v) > 0 {
		return v[0]
	}
	return ""
}
```

- [ ] **Step 4: Implement `http.go`**

```go
package observability

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// Middleware adds an HTTP server span and request-id propagation/echo.
func Middleware(next http.Handler) http.Handler {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = NewRequestID()
		}
		w.Header().Set(requestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(WithRequestID(r.Context(), id)))
	})
	return otelhttp.NewHandler(h, "http")
}
```

- [ ] **Step 5: Implement `kafka.go`**

```go
package observability

import (
	"context"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// InjectKafka writes the W3C traceparent into record headers.
func InjectKafka(ctx context.Context, r *kgo.Record) {
	otel.GetTextMapPropagator().Inject(ctx, kafkaCarrier{headers: &r.Headers})
}

// ExtractKafka returns a context carrying the record's producer trace.
func ExtractKafka(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, kafkaCarrier{headers: &r.Headers})
}

// ConsumerSpan starts a span parented by the record's producer trace.
func ConsumerSpan(ctx context.Context, name, callID string) (context.Context, trace.Span) {
	return otel.Tracer("omnifoni/kafka").Start(ctx, name,
		trace.WithAttributes(attribute.String("call_id", callID)))
}

type kafkaCarrier struct{ headers *[]kgo.RecordHeader }

func (c kafkaCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaCarrier) Set(key, value string) {
	*c.headers = append(*c.headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c kafkaCarrier) Keys() []string {
	out := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		out = append(out, h.Key)
	}
	return out
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd services/observability && go mod tidy && go vet ./... && go test ./...`
Expected: `ok omnifoni/observability`.

- [ ] **Step 7: Commit**

```bash
git add services/observability
git commit -m "feat(observability): gRPC, HTTP and Kafka propagation helpers"
```

---

### Task 3: Jaeger backend and stack prep

**Files:**
- Modify: `docker-compose.yml` (add `jaeger`; 6 build contexts; `OTEL_*` env on all 10 services)
- Modify: `services/catalogue/Dockerfile`, `services/identity/Dockerfile`, `services/mediation/Dockerfile`, `services/payments/Dockerfile`, `services/routing/Dockerfile`, `services/sbc/Dockerfile`
- Modify: `services/charging/Dockerfile`, `services/billing/Dockerfile`, `services/switching/Dockerfile`, `services/reconciliation/Dockerfile` (add `COPY observability/`)
- Modify: all 10 `services/*/go.mod` (add `replace omnifoni/observability => ../observability`)
- Modify: `ops/smoke.sh:7` (add jaeger to the health loop)

**Interfaces:**
- Consumes: nothing from earlier tasks at runtime.
- Produces: a running Jaeger at `http://localhost:16686` (UI) and `jaeger:4317` (OTLP) for later tasks.

- [ ] **Step 1: Add the Jaeger service**

Insert at the top of the `services:` map in `docker-compose.yml`:
```yaml
  jaeger:
    image: jaegertracing/all-in-one:1.62
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
    ports:
      - "16686:16686"
      - "4317:4317"
    healthcheck:
      test: ["CMD-SHELL", "wget -q --spider http://localhost:16686/ || exit 1"]
      interval: 5s
      retries: 10
```

- [ ] **Step 2: Add telemetry env to every service**

Add these two lines to each of `catalogue`, `charging`, `switching`, `mediation`, `billing`, `payments`, `identity`, `routing`, `sbc`, `reconciliation` under `environment:`:
```yaml
      OTEL_EXPORTER_OTLP_ENDPOINT: jaeger:4317
      OBS_SAMPLE_RATIO: "1.0"
```

- [ ] **Step 3: Switch the six plain-context services to `./services`**

Replace the build block of each service with the block shown. Keep each service's existing `environment`, `ports`, and `depends_on` untouched.

`catalogue`:
```yaml
  catalogue:
    build:
      context: ./services
      dockerfile: catalogue/Dockerfile
```
`mediation`:
```yaml
  mediation:
    build:
      context: ./services
      dockerfile: mediation/Dockerfile
```
`payments`:
```yaml
  payments:
    build:
      context: ./services
      dockerfile: payments/Dockerfile
```
`identity`:
```yaml
  identity:
    build:
      context: ./services
      dockerfile: identity/Dockerfile
```
`routing`:
```yaml
  routing:
    build:
      context: ./services
      dockerfile: routing/Dockerfile
```
`sbc`:
```yaml
  sbc:
    build:
      context: ./services
      dockerfile: sbc/Dockerfile
```

- [ ] **Step 4: Rewrite the six Dockerfiles in sibling-copy style**

`services/catalogue/Dockerfile`:
```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY catalogue/ ./catalogue/
RUN cd catalogue && CGO_ENABLED=0 go build -o /catalogue .

FROM gcr.io/distroless/static
COPY --from=build /catalogue /catalogue
ENTRYPOINT ["/catalogue"]
```

`services/mediation/Dockerfile`:
```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY mediation/ ./mediation/
RUN cd mediation && CGO_ENABLED=0 go build -o /mediation .

FROM gcr.io/distroless/static
COPY --from=build /mediation /mediation
ENTRYPOINT ["/mediation"]
```

`services/payments/Dockerfile`:
```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY payments/ ./payments/
RUN cd payments && CGO_ENABLED=0 go build -o /payments .

FROM gcr.io/distroless/static
COPY --from=build /payments /payments
ENTRYPOINT ["/payments"]
```

`services/identity/Dockerfile` (identity imports billing's generated stubs):
```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY billing/ ./billing/
COPY identity/ ./identity/
RUN cd identity && CGO_ENABLED=0 go build -o /identity .

FROM gcr.io/distroless/static
COPY --from=build /identity /identity
ENTRYPOINT ["/identity"]
```

`services/routing/Dockerfile`:
```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY routing/ ./routing/
RUN cd routing && CGO_ENABLED=0 go build -o /routing .

FROM gcr.io/distroless/static
COPY --from=build /routing /routing
ENTRYPOINT ["/routing"]
```

`services/sbc/Dockerfile`:
```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ ./observability/
COPY sbc/ ./sbc/
RUN cd sbc && CGO_ENABLED=0 go build -o /sbc .

FROM gcr.io/distroless/static
COPY --from=build /sbc /sbc
ENTRYPOINT ["/sbc"]
```

- [ ] **Step 5: Add `COPY observability/` to the four sibling Dockerfiles**

In `services/charging/Dockerfile` insert `COPY observability/ ./observability/` before `COPY catalogue/`.
In `services/billing/Dockerfile` insert it before `COPY catalogue/`.
In `services/switching/Dockerfile` insert it before `COPY charging/`.
In `services/reconciliation/Dockerfile` insert it before `COPY billing/`.

- [ ] **Step 6: Add the replace directive to every service module**

Run from the repo root:
```bash
for s in catalogue charging switching mediation billing payments identity routing sbc reconciliation; do
  (cd "services/$s" && go mod edit -replace omnifoni/observability=../observability)
done
```

- [ ] **Step 7: Add Jaeger to the smoke health loop**

In `ops/smoke.sh` change line 7 to:
```bash
for c in postgres valkey kafka clickhouse jaeger; do
```

- [ ] **Step 8: Rebuild and verify the stack**

Run:
```bash
docker compose config -q
docker compose build
docker compose up -d
make test
make smoke
```
Expected: all builds succeed; `make test` all `ok`; smoke prints `ok: jaeger healthy` and `smoke: PASS`.

- [ ] **Step 9: Commit**

```bash
git add docker-compose.yml ops/smoke.sh services/*/Dockerfile services/*/go.mod
git commit -m "feat(ops): jaeger backend and telemetry-ready service images"
```

---

### Task 4: Instrument catalogue, charging, routing and SBC

**Files:**
- Modify: `services/catalogue/main.go:43`, `services/catalogue/server.go:33-52`
- Modify: `services/charging/main.go:34-61`, `services/charging/server.go:47-76`
- Modify: `services/routing/main.go:162`, `services/routing/main.go:94-114`
- Modify: `services/sbc/main.go:48`, `services/sbc/sbc.go:43`
- Test: `services/catalogue/span_test.go`, `services/charging/span_test.go`

**Interfaces:**
- Consumes: `observability.SetupLogging`, `observability.Init`, `observability.ServerOptions`, `observability.ClientOptions`, `observability.Mask` (Tasks 1–2).
- Produces: spans `catalogue.get_rate`, `charging.authorize`, `charging.charge`, `routing.resolve_lcr`, `sbc.check_call`.

- [ ] **Step 1: Write the failing span tests**

Create `services/catalogue/span_test.go`:
```go
package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestGetRateEmitsSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	s := NewServer(nil)
	// Empty destination returns before touching the pool, so nil pool is safe.
	_, _ = s.GetRate(context.Background(), &pb.GetRateRequest{})

	found := false
	for _, sp := range sr.Ended() {
		if sp.Name() == "catalogue.get_rate" {
			found = true
		}
	}
	if !found {
		t.Fatal("catalogue.get_rate span not recorded")
	}
}
```
with `pb "omnifoni/catalogue/gen/cataloguepb"` in the imports.

Create `services/charging/span_test.go`:
```go
package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	pb "omnifoni/charging/gen/chargingpb"
)

func TestAuthorizeEmitsSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	s := NewServer(nil, nil)
	_, _ = s.Authorize(context.Background(), &pb.AuthorizeRequest{})

	found := false
	for _, sp := range sr.Ended() {
		if sp.Name() == "charging.authorize" {
			found = true
		}
	}
	if !found {
		t.Fatal("charging.authorize span not recorded")
	}
}
```
The empty request returns `deny(reasonInternalError)` before touching the pool, so no DB is needed.

- [ ] **Step 2: Run the tests to verify they fail**

Run:
```bash
cd services/catalogue && go test -run TestGetRateEmitsSpan ./...
cd ../charging && go test -run TestAuthorizeEmitsSpan ./...
```
Expected: FAIL with `span not recorded`.

- [ ] **Step 3: Wire catalogue**

In `services/catalogue/main.go`, after the imports add:
```go
	"omnifoni/observability"
```
At the top of `main()` add:
```go
	observability.SetupLogging("catalogue")
	shutdown, err := observability.Init(context.Background(), "catalogue")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```
Change `srv := grpc.NewServer()` to:
```go
	srv := grpc.NewServer(observability.ServerOptions()...)
```

In `services/catalogue/server.go`, add imports:
```go
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"omnifoni/observability"
```
Replace `GetRate` with:
```go
func (s *Server) GetRate(ctx context.Context, req *pb.GetRateRequest) (resp *pb.GetRateResponse, err error) {
	ctx, span := otel.Tracer("omnifoni/catalogue").Start(ctx, "catalogue.get_rate",
		trace.WithAttributes(attribute.String("destination", observability.Mask(req.GetDestination()))))
	defer func() {
		if resp != nil {
			span.SetAttributes(
				attribute.String("matched_prefix", resp.GetMatchedPrefix()),
				attribute.String("rate_per_sec", resp.GetRatePerSec()))
		}
		span.End()
	}()
	if req.GetDestination() == "" {
		return &pb.GetRateResponse{Found: false}, nil
	}
	var prefix, rate string
	err = s.pool.QueryRow(ctx,
		`SELECT prefix, rate_per_sec::text FROM rates
		 WHERE LEFT($1, LENGTH(prefix)) = prefix
		 ORDER BY LENGTH(prefix) DESC LIMIT 1`,
		req.GetDestination()).Scan(&prefix, &rate)
	if errors.Is(err, pgx.ErrNoRows) {
		return &pb.GetRateResponse{Found: false}, nil
	}
	if err != nil {
		return nil, err
	}
	return &pb.GetRateResponse{Found: true, MatchedPrefix: prefix, RatePerSec: rate}, nil
}
```

- [ ] **Step 4: Wire charging**

In `services/charging/main.go`, add `"omnifoni/observability"`, then at the top of `main()`:
```go
	observability.SetupLogging("charging")
	shutdown, err := observability.Init(context.Background(), "charging")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```
Change the catalogue dial to:
```go
	catConn, err := grpc.NewClient(catAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
```
Change `srv := grpc.NewServer()` to `srv := grpc.NewServer(observability.ServerOptions()...)`.

In `services/charging/server.go`, add imports `"go.opentelemetry.io/otel"`, `"go.opentelemetry.io/otel/attribute"`, `"go.opentelemetry.io/otel/trace"`, `"omnifoni/observability"`, then replace `Authorize` and `Charge` with:
```go
func (s *Server) Authorize(ctx context.Context, req *pb.AuthorizeRequest) (resp *pb.AuthorizeResponse, err error) {
	ctx, span := otel.Tracer("omnifoni/charging").Start(ctx, "charging.authorize",
		trace.WithAttributes(
			attribute.String("account", req.GetAccountId()),
			attribute.String("destination", observability.Mask(req.GetDestination()))))
	defer func() {
		if resp != nil && !resp.GetAllow() {
			span.SetAttributes(attribute.String("deny_reason", resp.GetReason()))
		}
		span.End()
	}()
	if req.GetAccountId() == "" || req.GetDestination() == "" {
		return deny(reasonInternalError), nil
	}
	resp, err = s.authorizeTx(ctx, req)
	if err != nil {
		if errors.Is(err, errDeny) {
			return resp, nil
		}
		slog.Error("authorize failed", "account", req.GetAccountId(), "err", err)
		return deny(reasonInternalError), nil
	}
	return resp, nil
}

func (s *Server) Charge(ctx context.Context, req *pb.ChargeRequest) (resp *pb.ChargeResponse, err error) {
	ctx, span := otel.Tracer("omnifoni/charging").Start(ctx, "charging.charge",
		trace.WithAttributes(
			attribute.String("session_id", req.GetSessionId()),
			attribute.Int64("total_seconds", req.GetTotalSeconds())))
	defer span.End()
	if req.GetSessionId() == "" || req.GetTotalSeconds() < 0 {
		return chargeErr(), nil
	}
	resp, err = s.chargeTx(ctx, req)
	if err != nil {
		slog.Error("charge failed", "session", req.GetSessionId(), "err", err)
		return chargeErr(), nil
	}
	return resp, nil
}
```

- [ ] **Step 5: Wire routing**

In `services/routing/main.go`, add `"omnifoni/observability"`; at the top of `main()`:
```go
	observability.SetupLogging("routing")
	shutdown, err := observability.Init(context.Background(), "routing")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```
Change `g := grpc.NewServer()` to `g := grpc.NewServer(observability.ServerOptions()...)`.

Replace `ResolveLcr` with:
```go
func (s *RoutingServer) ResolveLcr(ctx context.Context, req *routingpb.ResolveRequest) (resp *routingpb.ResolveLcrResponse, err error) {
	ctx, span := otel.Tracer("omnifoni/routing").Start(ctx, "routing.resolve_lcr",
		trace.WithAttributes(attribute.String("destination", observability.Mask(req.GetNumber()))))
	defer func() {
		if resp != nil {
			span.SetAttributes(
				attribute.String("trunk_id", resp.GetTrunkId()),
				attribute.Int64("cost_per_sec", resp.GetCostPerSec()))
		}
		span.End()
	}()
	if req.Number == "" {
		return &routingpb.ResolveLcrResponse{}, nil
	}
	var trunk string
	var cost int64
	err = s.pool.QueryRow(ctx,
		`SELECT r.trunk_id, r.cost_per_sec
		 FROM lcr_routes r JOIN trunks t USING (trunk_id)
		 WHERE r.prefix <= $1 AND $1 LIKE r.prefix || '%' AND t.enabled
		 ORDER BY r.cost_per_sec ASC, length(r.prefix) DESC, r.trunk_id ASC
		 LIMIT 1`,
		req.Number).Scan(&trunk, &cost)
	if errors.Is(err, pgx.ErrNoRows) {
		return &routingpb.ResolveLcrResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	return &routingpb.ResolveLcrResponse{TrunkId: trunk, CostPerSec: cost}, nil
}
```
Add imports `"go.opentelemetry.io/otel"`, `"go.opentelemetry.io/otel/attribute"`, `"go.opentelemetry.io/otel/trace"`, `"omnifoni/observability"`.

- [ ] **Step 6: Wire SBC**

In `services/sbc/main.go`, add `"omnifoni/observability"`; at the top of `main()`:
```go
	observability.SetupLogging("sbc")
	shutdown, err := observability.Init(context.Background(), "sbc")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```
Change `g := grpc.NewServer()` to `g := grpc.NewServer(observability.ServerOptions()...)`.

In `services/sbc/sbc.go`, add imports `"go.opentelemetry.io/otel"`, `"go.opentelemetry.io/otel/attribute"`, `"go.opentelemetry.io/otel/trace"`, and change the `CheckCall` signature line to a named return plus span setup:
```go
func (s *SBCServer) CheckCall(ctx context.Context, req *sbcpb.CheckCallRequest) (resp *sbcpb.CheckCallResponse, err error) {
	ctx, span := otel.Tracer("omnifoni/sbc").Start(ctx, "sbc.check_call",
		trace.WithAttributes(attribute.String("trunk_id", req.GetTrunkId())))
	defer func() {
		if resp != nil && !resp.GetAllowed() {
			span.SetAttributes(attribute.String("deny_reason", resp.GetReason()))
		}
		span.End()
	}()
```
The rest of the function body is unchanged.

- [ ] **Step 7: Run the tests to verify they pass**

Run:
```bash
cd services/catalogue && go mod tidy && go test ./...
cd ../charging && go mod tidy && go test ./...
cd ../routing && go mod tidy && go test ./...
cd ../sbc && go mod tidy && go test ./...
```
Expected: all `ok`; the two span tests pass.

- [ ] **Step 8: Rebuild and confirm spans in Jaeger**

Run:
```bash
docker compose build catalogue charging routing sbc && docker compose up -d catalogue charging routing sbc
cd services/switching && go test ./...
curl -s http://localhost:16686/api/services | grep -o '"catalogue"\|"charging"\|"routing"\|"sbc"'
```
Expected: switching tests pass and at least `catalogue` and `charging` appear (they are called by the tests).

- [ ] **Step 9: Commit**

```bash
git add services/catalogue services/charging services/routing services/sbc
git commit -m "feat(telemetry): trace catalogue, charging, routing and sbc"
```

---

### Task 5: Instrument switching (call path and CDR publish)

**Files:**
- Modify: `services/switching/main.go:19-81`, `services/switching/grpc.go:19-25`, `services/switching/calls.go:74,119-190,291-300`, `services/switching/cdrs.go`
- Test: `services/switching/span_test.go` (new), `services/switching/*_test.go` fake publisher signature updates

**Interfaces:**
- Consumes: `observability.SetupLogging/Init/ServerOptions/ClientOptions/InjectKafka`, `observability.Mask`.
- Produces: spans `switching.start_call`, `switching.stop_call`; `cdrPublisher` becomes `PublishCDR(ctx context.Context, c *mediationpb.CallCdr) error`.

- [ ] **Step 1: Write the failing test**

There is no existing fake publisher in the switching tests; the new test defines one. Create `services/switching/span_test.go`:
```go
package main

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	mediationpb "omnifoni/mediation/gen/mediationpb"
	pb "omnifoni/switching/gen/switchingpb"
)

// spanCDRs records the trace context the registry hands to PublishCDR.
type spanCDRs struct{ got chan trace.SpanContext }

func (s *spanCDRs) PublishCDR(ctx context.Context, _ *mediationpb.CallCdr) error {
	select {
	case s.got <- trace.SpanContextFromContext(ctx):
	default:
	}
	return nil
}

// TestPublishCDRCarriesTraceContext drives a real call through the bufconn
// harness and asserts that the CDR publish happens under the start_call
// span's trace (needs the dev stack: charging :50051, catalogue :50053).
func TestPublishCDRCarriesTraceContext(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	pub := &spanCDRs{got: make(chan trace.SpanContext, 4)}
	client := startSwitchingWithCDR(t, chargingAddr(), pub)
	seed(t, "trace-acc", 1000, "+2782", "1.0")

	sw := pb.NewSwitchingClient(client)
	res, err := sw.StartCall(context.Background(), &pb.StartCallRequest{
		AccountId: "trace-acc", Destination: "+27821234567"})
	if err != nil || !res.GetAllowed() {
		t.Fatalf("start: %v %v", err, res)
	}
	if _, err := sw.StopCall(context.Background(), &pb.StopCallRequest{CallId: res.GetCallId()}); err != nil {
		t.Fatalf("stop: %v", err)
	}

	select {
	case pubSC := <-pub.got:
		if !pubSC.IsValid() {
			t.Fatal("published CDR context has no trace span")
		}
		found := false
		for _, sp := range sr.Ended() {
			if sp.Name() == "switching.start_call" {
				found = true
				if sp.SpanContext().TraceID() != pubSC.TraceID() {
					t.Fatalf("publish trace %s != start_call trace %s",
						pubSC.TraceID(), sp.SpanContext().TraceID())
				}
			}
		}
		if !found {
			t.Fatal("switching.start_call span not recorded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CDR publish never observed")
	}
}
```
This uses the existing `startSwitchingWithCDR` (main_test.go:65) and `seed` (main_test.go:113) helpers, so it runs against the dev stack like the other switching tests.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/switching && go test -run TestPublishCDRCarriesTraceContext ./...`
Expected: FAIL — `published CDR context has no trace span` (publish uses `context.Background()`), or a compile error for the new `PublishCDR(ctx, ...)` signature.

- [ ] **Step 3: Wire `main.go`**

Add `"omnifoni/observability"`; at the top of `main()`:
```go
	observability.SetupLogging("switching")
	shutdown, err := observability.Init(context.Background(), "switching")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```
Change each of the three `grpc.NewClient(...)` calls to include client options, e.g.:
```go
	cc, err := grpc.NewClient(chargingAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
```
Apply the same pattern to `rcc` (routing) and `scc` (SBC). Change `srv := grpc.NewServer()` to `srv := grpc.NewServer(observability.ServerOptions()...)`.

- [ ] **Step 4: Add the call trace context and spans**

In `services/switching/calls.go`:
- Add fields to `type call struct`:
```go
	traceCtx context.Context
```
- At the top of `Registry.StartCall`, add:
```go
	ctx, span := otel.Tracer("omnifoni/switching").Start(ctx, "switching.start_call",
		trace.WithAttributes(
			attribute.String("account", req.GetAccountId()),
			attribute.String("destination", observability.Mask(req.GetDestination()))))
	defer span.End()
```
- When constructing `c := &call{...}`, add:
```go
		traceCtx: trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)),
```
- After a successful start, set:
```go
	span.SetAttributes(attribute.String("call_id", callID), attribute.String("trunk_id", trunk))
```
- Change `publishCDR` to pass the stored context (fall back to `context.Background()` for calls constructed without one, e.g. in older tests):
```go
	ctx := c.traceCtx
	if ctx == nil {
		ctx = context.Background()
	}
	err := r.cdrPub.PublishCDR(ctx, buildCDR(c, endReason, total, time.Now()))
```
- Change the interface:
```go
	PublishCDR(ctx context.Context, c *mediationpb.CallCdr) error
```
Add imports `"go.opentelemetry.io/otel"`, `"go.opentelemetry.io/otel/attribute"`, `"go.opentelemetry.io/otel/trace"`, `"omnifoni/observability"` to `calls.go`.

In `services/switching/cdrs.go`, replace `PublishCDR` with:
```go
func (k *kafkaCDRs) PublishCDR(ctx context.Context, c *mediationpb.CallCdr) error {
	v, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: k.topic, Key: []byte(c.GetCallId()), Value: v}
	observability.InjectKafka(ctx, rec)
	return k.prod.ProduceSync(ctx, rec).FirstErr()
}
```
Add `"omnifoni/observability"` to `cdrs.go`.

In `services/switching/grpc.go`, wrap `StopCall`:
```go
func (s *switchingServer) StopCall(ctx context.Context, req *pb.StopCallRequest) (*pb.StopCallResponse, error) {
	ctx, span := otel.Tracer("omnifoni/switching").Start(ctx, "switching.stop_call",
		trace.WithAttributes(attribute.String("call_id", req.GetCallId())))
	defer span.End()
	_ = ctx
	return &pb.StopCallResponse{Stopped: s.reg.StopCall(req.GetCallId())}, nil
}
```
Add imports `"go.opentelemetry.io/otel"` and `"go.opentelemetry.io/otel/attribute"`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/switching && go mod tidy && go vet ./... && go test ./...`
Expected: all `ok`, including `TestPublishCDRCarriesTraceContext`.

- [ ] **Step 6: Rebuild and confirm**

Run:
```bash
docker compose build switching && docker compose up -d switching
curl -s http://localhost:16686/api/services | grep -o '"switching"'
```
Expected: `switching` appears after the switching tests or a smoke call.

- [ ] **Step 7: Commit**

```bash
git add services/switching
git commit -m "feat(telemetry): trace switching calls and propagate CDR traceparent"
```

---

### Task 6: Instrument mediation and billing

**Files:**
- Modify: `services/mediation/main.go:21`, `services/mediation/pipeline.go:31-79`
- Modify: `services/billing/main.go:28-82`, `services/billing/biller.go:44-120`
- Test: `services/mediation/span_test.go` (new), `services/billing/span_test.go` (new)

**Interfaces:**
- Consumes: `observability.SetupLogging/Init/ServerOptions/ClientOptions/ExtractKafka/InjectKafka/ConsumerSpan`.
- Produces: spans `mediation.process`, `billing.handle`; traceparent preserved from `cdrs.raw` to `cdrs.normalized` and `cdrs.billing.rejected`.

- [ ] **Step 1: Write the failing tests**

Create `services/mediation/span_test.go`:
```go
package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestHandleEmitsConsumerSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	// Pins the span-name contract the pipeline uses; imports the shared module.
	_, span := observability.ConsumerSpan(context.Background(), "mediation.process", "call-1")
	span.End()

	if len(sr.Ended()) != 1 || sr.Ended()[0].Name() != "mediation.process" {
		t.Fatalf("unexpected spans: %v", sr.Ended())
	}
}
```
Import `"omnifoni/observability"` in the test file. (A full `Handle` test needs both a DB and a broker; the span-name contract and the wiring in Step 3 are what this slice verifies here.)

Create `services/billing/span_test.go`:
```go
package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"omnifoni/observability"
)

func TestBillingSpanName(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	ctx, span := observability.ConsumerSpan(context.Background(), "billing.handle", "call-1")
	_ = ctx
	span.End()

	if len(sr.Ended()) != 1 || sr.Ended()[0].Name() != "billing.handle" {
		t.Fatalf("unexpected spans: %v", sr.Ended())
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:
```bash
cd services/mediation && go test -run TestHandleEmitsConsumerSpan ./...
cd ../billing && go test -run TestBillingSpanName ./...
```
Expected: FAIL — `observability` not imported / undefined helper.

- [ ] **Step 3: Wire mediation**

In `services/mediation/main.go`, add `"omnifoni/observability"`; after the signal context is created add:
```go
	observability.SetupLogging("mediation")
	shutdown, err := observability.Init(ctx, "mediation")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```

In `services/mediation/pipeline.go`, add `"omnifoni/observability"`. Replace `Handle` with:
```go
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) error {
	c := &pb.CallCdr{}
	if err := proto.Unmarshal(rec.Value, c); err != nil {
		return p.reject(ctx, rec, "decode_error", err)
	}
	ctx = observability.ExtractKafka(ctx, rec)
	ctx, span := observability.ConsumerSpan(ctx, "mediation.process", c.GetCallId())
	defer span.End()
	if reason := Validate(c); reason != "" {
		return p.reject(ctx, rec, reason, nil)
	}
	return p.publish(ctx, rec, c)
}
```
Replace `produce` with:
```go
func (p *Processor) produce(ctx context.Context, topic string, key, value []byte, reason string) error {
	r := &kgo.Record{Topic: topic, Key: key, Value: value}
	if reason != "" {
		r.Headers = []kgo.RecordHeader{{Key: "reason", Value: []byte(reason)}}
	}
	observability.InjectKafka(ctx, r)
	return p.prod.ProduceSync(ctx, r).FirstErr()
}
```

- [ ] **Step 4: Wire billing**

In `services/billing/main.go`, add `"omnifoni/observability"`; after the signal context add:
```go
	observability.SetupLogging("billing")
	shutdown, err := observability.Init(ctx, "billing")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```
Change the catalogue dial to include `observability.ClientOptions()` using the `append` pattern from Task 4, and `srv := grpc.NewServer()` to `grpc.NewServer(observability.ServerOptions()...)`.

In `services/billing/biller.go`, add `"omnifoni/observability"`, `"go.opentelemetry.io/otel"`, `"go.opentelemetry.io/otel/attribute"`. At the top of `Handle` after unmarshal/validation:
```go
	ctx = observability.ExtractKafka(ctx, rec)
	ctx, span := observability.ConsumerSpan(ctx, "billing.handle", c.GetCallId())
	defer span.End()
	span.SetAttributes(attribute.String("account", c.GetAccountId()))
```
In `Biller.reject` (the helper that publishes dead letters), inject the trace before producing:
```go
	observability.InjectKafka(ctx, r)
```
Apply it where `Biller` builds its rejected record (the `ProduceSync` call in `biller.go:111`); construct the record first, inject, then produce.

- [ ] **Step 5: Run the tests to verify they pass**

Run:
```bash
cd services/mediation && go mod tidy && go vet ./... && go test ./...
cd ../billing && go mod tidy && go vet ./... && go test ./...
```
Expected: all `ok`.

- [ ] **Step 6: Rebuild and confirm**

Run:
```bash
docker compose build mediation billing && docker compose up -d mediation billing
make test
curl -s http://localhost:16686/api/services | grep -o '"mediation"\|"billing"'
```
Expected: tests pass; both services appear after CDR traffic.

- [ ] **Step 7: Commit**

```bash
git add services/mediation services/billing
git commit -m "feat(telemetry): trace mediation and billing across kafka"
```

---

### Task 7: Instrument identity, payments and reconciliation

**Files:**
- Modify: `services/identity/main.go:48-63`, `services/identity/commission.go:106`
- Modify: `services/payments/main.go:26-76`
- Modify: `services/reconciliation/main.go:28-93`, `services/reconciliation/grpc.go`, `services/reconciliation/sweep.go:41-100`, `services/reconciliation/cmd/smokerecon/main.go:21`
- Test: `services/reconciliation/span_test.go` (new)

**Interfaces:**
- Consumes: `observability.SetupLogging/Init/ServerOptions/ClientOptions/Middleware/InjectKafka`.
- Produces: HTTP spans for identity/payments, spans `reconciliation.sweep` per period, traceparent on `reconciliation.drift`.

- [ ] **Step 1: Write the failing test**

Create `services/reconciliation/span_test.go`:
```go
package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestSweepEmitsSpanPerPeriod(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	s := NewServer(&fakeBilling{}, &fakePayments{}, noopPublisher{})
	if err := s.Sweep(context.Background(), mustTime(t, "2026-11-15T10:00:00Z")); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	got := 0
	for _, sp := range sr.Ended() {
		if sp.Name() == "reconciliation.sweep" {
			got++
		}
	}
	if got != 2 {
		t.Fatalf("sweep spans = %d, want 2 (current + previous month)", got)
	}
}
```
Use the existing test doubles in `reconcile_test.go` (`fakeBilling`, `fakePayments`, `noopPublisher` at reconcile_test.go:12-62; zero-valued fakes mean ok/no-activity periods, so nothing publishes). Add a small helper if needed:
```go
func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/reconciliation && go test -run TestSweepEmitsSpanPerPeriod ./...`
Expected: FAIL — `sweep spans = 0, want 2`.

- [ ] **Step 3: Wire identity**

In `services/identity/main.go`, add `"omnifoni/observability"`; after the signal context add:
```go
	observability.SetupLogging("identity")
	shutdown, err := observability.Init(ctx, "identity")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```
Change the HTTP server line to wrap the mux:
```go
	srv := &http.Server{Addr: addr, Handler: observability.Middleware(mux), ReadHeaderTimeout: 5 * time.Second}
```
In `services/identity/commission.go`, change `DialBilling` to:
```go
func DialBilling(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
}
```
Add `"omnifoni/observability"` there.

- [ ] **Step 4: Wire payments**

In `services/payments/main.go`, add `"omnifoni/observability"`; after the signal context add:
```go
	observability.SetupLogging("payments")
	shutdown, err := observability.Init(ctx, "payments")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```
Change `gsrv := grpc.NewServer()` to `gsrv := grpc.NewServer(observability.ServerOptions()...)` and the HTTP server to:
```go
	srv := &http.Server{Addr: addr, Handler: observability.Middleware(mux), ReadHeaderTimeout: 5 * time.Second}
```

- [ ] **Step 5: Wire reconciliation**

In `services/reconciliation/main.go`, add `"omnifoni/observability"`; after the signal context add:
```go
	observability.SetupLogging("reconciliation")
	shutdown, err := observability.Init(ctx, "reconciliation")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()
```
Change both dials (billing, payments) to include `observability.ClientOptions()` with the `append` pattern, and `g := grpc.NewServer()` to `grpc.NewServer(observability.ServerOptions()...)`.

In `services/reconciliation/sweep.go`, add `"go.opentelemetry.io/otel"` and `"go.opentelemetry.io/otel/attribute"`. At the top of the period loop body add:
```go
		ctx, span := otel.Tracer("omnifoni/reconciliation").Start(ctx, "reconciliation.sweep",
			trace.WithAttributes(attribute.String("period", period)))
```
and end it after the payment-delta loop:
```go
		span.SetAttributes(attribute.Int("accounts", len(resp.GetAccounts())))
		span.End()
```
Add `"go.opentelemetry.io/otel/trace"` for the `trace.WithAttributes` call.

In `services/reconciliation/sweep.go`, make `PublishDrift` inject the trace before producing:
```go
	r := &kgo.Record{Topic: p.topic, Key: []byte(key), Value: b}
	observability.InjectKafka(ctx, r)
	return p.prod.ProduceSync(ctx, r).FirstErr()
```
Add `"omnifoni/observability"`.

In `services/reconciliation/cmd/smokerecon/main.go`, add `"omnifoni/observability"` and change the dial to the `append` pattern.

- [ ] **Step 6: Run the tests to verify they pass**

Run:
```bash
cd services/identity && go mod tidy && go vet ./... && go test ./...
cd ../payments && go mod tidy && go vet ./... && go test ./...
cd ../reconciliation && go mod tidy && go vet ./... && go test ./...
```
Expected: all `ok`, including `TestSweepEmitsSpanPerPeriod`.

- [ ] **Step 7: Rebuild and confirm**

Run:
```bash
docker compose build identity payments reconciliation && docker compose up -d identity payments reconciliation
make smoke
curl -s http://localhost:16686/api/services | grep -o '"identity"\|"payments"\|"reconciliation"'
```
Expected: smoke PASS; `reconciliation`, `billing` and `payments` appear (the probe calls them).

- [ ] **Step 8: Commit**

```bash
git add services/identity services/payments services/reconciliation
git commit -m "feat(telemetry): trace identity, payments and reconciliation"
```

---

### Task 8: Trace smoke assertion, docs and final gate

**Files:**
- Modify: `ops/smoke.sh` (after the probe, assert the trace backend saw the probe)
- Modify: `AGENTS.md` (observability section)
- Test: manual + `make test` + `make smoke`

**Interfaces:**
- Consumes: everything from Tasks 1–7.
- Produces: a smoke run that fails if tracing is broken.

- [ ] **Step 1: Add the trace assertion**

In `ops/smoke.sh`, after the reconciliation probe line, add:
```bash
services=$(curl -fsS http://localhost:16686/api/services) || fail "jaeger api"
for s in reconciliation billing payments; do
  echo "$services" | grep -q "\"$s\"" || fail "no traces for $s"
  echo "ok: traces for $s"
done
```
The probe issues billing and payments RPCs through reconciliation, so all three report spans.

- [ ] **Step 2: Document in `AGENTS.md`**

Add a bullet under the Status section:
```markdown
- Observability (P0 debt): `services/observability` is a shared module (`omnifoni/observability`) imported via `replace ../observability`. It installs OTLP/gRPC tracing (env `OTEL_EXPORTER_OTLP_ENDPOINT`, default `jaeger:4317`, `off` disables; `OBS_SAMPLE_RATIO` default 1.0), a JSON slog handler with `service`/`trace_id`/`span_id`/`request_id`, and masks PII keys (`phone|msisdn|caller|callee|email|token|password|authorization|secret`). Telemetry is fail-open. Jaeger all-in-one runs in compose: UI http://localhost:16686, OTLP 4317. Every service must be built with `context: ./services` so the Dockerfile can `COPY observability/`.
```

- [ ] **Step 3: Run the full gate**

Run:
```bash
make test
make smoke
```
Expected: all modules `ok`; smoke ends with `ok: traces for reconciliation`, `ok: traces for billing`, `ok: traces for payments`, `smoke: PASS`.

- [ ] **Step 4: Verify logs are JSON and correlated**

Run:
```bash
docker compose logs --tail=20 switching | head -5
docker compose logs --tail=20 reconciliation | head -5
```
Expected: each line is a JSON object with `"service":"..."`; request-handling lines include `trace_id`/`span_id` (and `request_id` where a request context exists).

- [ ] **Step 5: Verify fail-open**

Run:
```bash
docker compose stop jaeger
make smoke
docker compose start jaeger
```
Expected: smoke still PASS (Jaeger health is checked before the stop; the trace assertion is the only step that would fail, so run `bash -c 'cd services/reconciliation && go run ./cmd/smokerecon'` instead to confirm RPCs succeed with Jaeger down). Restart Jaeger afterwards.

- [ ] **Step 6: Commit**

```bash
git add ops/smoke.sh AGENTS.md
git commit -m "test(ops): assert traces in smoke; document observability"
```

---

## Self-Review Notes

- Spec coverage: shared module (§4.1) → Tasks 1–2; Jaeger + env + Dockerfiles (§8) → Task 3; per-service wiring and manual spans (§4.2–4.3) → Tasks 4–7; JSON schema and redaction (§5) → Tasks 1 and 8; fail-open (§6) → Tasks 1 and 8; testing (§7) → span tests in Tasks 1–2, 4–7 plus smoke in Task 8; acceptance criteria (§10) → Task 8 gate.
- Deliberate deviations from the spec text: `OTEL_EXPORTER_OTLP_ENDPOINT=off` (not empty) disables export, because `os.Getenv` cannot distinguish empty from unset; the spec was updated in commit `494c5f1`.
- `duration_seconds` on `switching.stop_call` was dropped because `StopCallRequest` carries only `call_id`; the spec was updated in the same commit.
- The identity image previously built from a context that could not resolve its `../billing` replace; Task 3 fixes this latent Dockerfile bug.
- Self-review pass (verified every snippet against the executable source): Task 4's catalogue test now calls `GetRate` with an empty `pb.GetRateRequest{}` directly (span starts before the empty check; nil pool never touched); Task 5's test was rewritten — the original called `StartCall` with a nil `*grpc.ClientConn` (panics) and a `call` without `traceCtx` — it now drives a real call through the existing `startSwitchingWithCDR`/`seed` bufconn harness (needs the dev stack, like all switching tests) with a channel-based publisher, and `publishCDR` gains a nil-`traceCtx` fallback; Task 6's mediation test drops the `ConsumerSpanForTest` placeholder and pins the span-name contract via `observability.ConsumerSpan` directly (a full `Handle` test needs DB+broker); Task 7's test uses the real doubles `fakeBilling`/`fakePayments`/`noopPublisher` via `NewServer` (reconcile_test.go:12-62), not `stub*`/struct literals. All `main.go` wiring targets, `NewServer` signatures, `DialBilling`, `ResolveLcr` SQL, `produce`/`reject` shapes, and Dockerfile styles were verified against the code.
