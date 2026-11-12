# Mediation Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `services/mediation` (Kafka CDR consumer/producer with Postgres dedup ledger) and the Switching → Kafka CDR emit, per `docs/superpowers/specs/2026-11-12-mediation-design.md`.

**Architecture:** Mediation consumes `cdrs.raw`, validates, dedups via a Postgres ledger (`cdrs` table, call_id PK), produces `cdrs.normalized` (and `cdrs.rejected` dead letters), then commits Kafka offsets. Switching publishes one `CallCdr` protobuf per finished call. Mediation has no gRPC port.

**Tech Stack:** Go 1.27, franz-go (`github.com/twmb/franz-go/pkg/kgo`, BSD-3), pgx/v5, protobuf via existing `make protoc` flow.

## Global Constraints

- Module paths: `omnifoni/mediation`, sibling import in switching via `replace omnifoni/mediation => ../mediation` (same pattern as `omnifoni/charging`).
- Mediation has **no gRPC port**; its contract is Kafka topics.
- Topics: `cdrs.raw` (key = call_id, value = protobuf `mediation.v1.CallCdr`), `cdrs.normalized`, `cdrs.rejected` (dead letter, header `reason`).
- Kafka messages are keyed by `call_id` so per-call processing is serial within a partition.
- Mediation is at-least-once with a tiny crash-window duplicate; never loses a record. Order inside publish: insert row → produce → mark published.
- Money/units untouched this slice; `charged_seconds` is a plain int64 count.
- Distroless Docker images, `golang:1.27` build stage (match existing Dockerfiles).
- Postgres dev password is `dev-only`; new DB `mediation` goes in `ops/postgres-init.sql` (requires deleting the `pgdata` volume to apply — AGENTS.md line).
- Dev password defaults everywhere are dev-only conveniences (`// ponytail: dev default` comments, like the other services).
- Run stack first: `make up` (needs dockerd; AGENTS.md notes `nohup dockerd >/var/log/dockerd.log 2>&1 &` after host restart).
- After adding `CREATE DATABASE mediation;`, reset data: `make down && docker volume rm omni-fon_pgdata && make up` (init.sql runs on first init only).
- `make protoc` uses `PATH=/root/go/bin:$(PATH)`; protoc-gen-go is installed there. Mediation has no service, so only `--go_out` (no `--go-grpc_out`).
- Comments only where they carry intent (existing style: brief `// ponytail:` notes); no other comments.

---

### Task 1: Mediation proto and module skeleton

**Files:**
- Create: `proto/mediation/v1/mediation.proto`
- Create: `services/mediation/go.mod`
- Modify: `Makefile` (protoc target)
- Generated: `services/mediation/gen/mediationpb/mediation.pb.go`

**Interfaces:**
- Produces: protobuf package `omnifoni/mediation/gen/mediationpb`, message `mediationpb.CallCdr` with fields `CallId, AccountId, Destination, Cli, StartedAtMs, EndedAtMs, ChargedSeconds, EndReason string/int64` as declared below. All later tasks import this.

- [ ] **Step 1: Write the proto**

`proto/mediation/v1/mediation.proto`:

```proto
syntax = "proto3";

package mediation.v1;

option go_package = "omnifoni/mediation/gen/mediationpb";

// CallCdr is the call detail record flowing from Switching to Mediation on
// cdrs.raw and from Mediation to Billing on cdrs.normalized
// (docs/master-plan.md section 3.2; protobuf per the mediation slice spec).
message CallCdr {
  string call_id = 1;
  string account_id = 2;
  string destination = 3;
  string cli = 4;
  int64 started_at_ms = 5;
  int64 ended_at_ms = 6;
  int64 charged_seconds = 7;
  string end_reason = 8; // normal | balance_exhausted | ocs_unavailable
}
```

- [ ] **Step 2: Generate the stubs**

Add the Makefile protoc line first (in `Makefile`, append to the `protoc` target, keep the tab indent):

```make
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/mediation --go_opt=module=omnifoni/mediation \
		proto/mediation/v1/mediation.proto
```

Run: `make protoc`
Expected: `services/mediation/gen/mediationpb/mediation.pb.go` created.

- [ ] **Step 3: Create the module**

`services/mediation/go.mod`:

```
module omnifoni/mediation

go 1.27.4

require (
	google.golang.org/protobuf v1.36.12
)
```

Run: `cd services/mediation && go mod tidy`
Expected: `go.sum` created with the protobuf dependency kept (the generated code imports it).

- [ ] **Step 4: Verify**

Run: `cd services/mediation && go build ./...`
Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add proto/mediation services/mediation Makefile
git commit -m "feat(mediation): CallCdr proto and module skeleton"
```

---

### Task 2: Mediation migration and validation (TDD)

**Files:**
- Create: `services/mediation/mediation.go`
- Create: `services/mediation/validate_test.go`

**Interfaces:**
- Consumes: `mediationpb.CallCdr` (Task 1).
- Produces: `Migrate(ctx context.Context, pool *pgxpool.Pool) error`; `Validate(c *pb.CallCdr) string` (returns `""` when valid, else a rejection reason string). Task 3 uses both.

- [ ] **Step 1: Write the failing test**

`services/mediation/validate_test.go`:

```go
package main

import (
	"testing"

	pb "omnifoni/mediation/gen/mediationpb"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		c    *pb.CallCdr
		want string
	}{
		{"valid zero-second call", &pb.CallCdr{CallId: "c1", AccountId: "a1", EndReason: "normal"}, ""},
		{"missing call id", &pb.CallCdr{AccountId: "a1", EndReason: "normal"}, "missing_call_id"},
		{"missing account id", &pb.CallCdr{CallId: "c1", EndReason: "normal"}, "missing_account_id"},
		{"negative seconds", &pb.CallCdr{CallId: "c1", AccountId: "a1", ChargedSeconds: -1, EndReason: "normal"}, "negative_charged_seconds"},
		{"unknown reason", &pb.CallCdr{CallId: "c1", AccountId: "a1", EndReason: "weird"}, "unknown_end_reason"},
	}
	for _, tc := range cases {
		if got := Validate(tc.c); got != tc.want {
			t.Errorf("%s: Validate = %q, want %q", tc.name, got, tc.want)
		}
	}
}
```

- [ ] **Step 2: Run to see it fail**

Run: `cd services/mediation && go test ./...`
Expected: FAIL — `undefined: Validate`.

- [ ] **Step 3: Implement**

`services/mediation/mediation.go`:

```go
package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	pb "omnifoni/mediation/gen/mediationpb"
)

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

const schemaSQL = `CREATE TABLE IF NOT EXISTS cdrs (
    call_id    TEXT PRIMARY KEY,
    published  BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Validate returns "" for a valid CDR, else its rejection reason.
func Validate(c *pb.CallCdr) string {
	switch {
	case c.GetCallId() == "":
		return "missing_call_id"
	case c.GetAccountId() == "":
		return "missing_account_id"
	case c.GetChargedSeconds() < 0:
		return "negative_charged_seconds"
	case !validReason(c.GetEndReason()):
		return "unknown_end_reason"
	}
	return ""
}

func validReason(r string) bool {
	return r == "normal" || r == "balance_exhausted" || r == "ocs_unavailable"
}
```

- [ ] **Step 4: Run to see it pass**

Run: `cd services/mediation && go test ./...`
Expected: PASS (TestValidate only; no DB needed).

- [ ] **Step 5: Commit**

```bash
git add services/mediation/mediation.go services/mediation/validate_test.go
git commit -m "feat(mediation): dedup-ledger migration and CDR validation"
```

---

### Task 3: Handle pipeline (decode → validate → ledger → produce) with integration test

**Files:**
- Create: `services/mediation/pipeline.go`
- Create: `services/mediation/pipeline_test.go`
- Modify: `services/mediation/go.mod` (franz-go dependency)

**Interfaces:**
- Consumes: `Validate`, `Migrate` (Task 2); `mediationpb.CallCdr`; franz-go `kgo.Client`.
- Produces: `type Processor struct` with `NewProcessor(pool *pgxpool.Pool, cl *kgo.Client, normalized, rejected string) *Processor` and `(p *Processor) Handle(ctx context.Context, rec *kgo.Record) error`. Task 4's `Run` loop calls `Handle` per record. Tests construct `&kgo.Record{Topic: ..., Key: ..., Value: ...}` directly.

- [ ] **Step 1: Add franz-go and uuid**

Run: `cd services/mediation && go get github.com/twmb/franz-go@latest github.com/google/uuid@v1.6.0 && go mod tidy`
Expected: `go.mod` gains `github.com/twmb/franz-go` and `github.com/google/uuid`.

- [ ] **Step 2: Write the failing integration test**

`services/mediation/pipeline_test.go`:

```go
package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	pb "omnifoni/mediation/gen/mediationpb"
)

func kafkaBrokers() []string {
	if s := os.Getenv("MEDIATION_KAFKA"); s != "" {
		return []string{s}
	}
	return []string{"localhost:9092"}
}

func dsn() string {
	if s := os.Getenv("MEDIATION_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/mediation"
}

func newKafka(t *testing.T) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers()...))
	if err != nil {
		t.Skip("kafka unavailable:", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn())
	if err != nil {
		t.Skip("postgres unavailable:", err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `TRUNCATE cdrs`); err != nil {
		t.Fatal(err)
	}
	return pool
}

// cdrRecord builds a raw record like Switching would publish.
func cdrRecord(t *testing.T, topic string, c *pb.CallCdr) *kgo.Record {
	t.Helper()
	v, err := proto.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(c.GetCallId()), Value: v}
}

// readRecords consumes a topic from the start with a unique group until it
// has want records or the timeout hits.
func readRecords(t *testing.T, topic string, want int) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers()...),
		kgo.ConsumerGroup("mediation-test-"+uuid.New().String()),
		kgo.ConsumeTopics(topic),
		kgo.StartOffsetEarliest(),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	var recs []*kgo.Record
	for len(recs) < want && ctx.Err() == nil {
		f := cl.PollRecords(ctx, 100)
		for r := f.RecordIter().Next(); r != nil; r = f.RecordIter().Next() {
			recs = append(recs, r)
		}
	}
	return recs
}

// TestHandlePipeline: valid, duplicate, malformed, and invalid records produce
// exactly one normalized record and two rejected records.
func TestHandlePipeline(t *testing.T) {
	pool := newPool(t)
	cl := newKafka(t)
	p := NewProcessor(pool, cl, "cdrs.normalized", "cdrs.rejected")
	ctx := context.Background()

	valid := &pb.CallCdr{CallId: "c-1", AccountId: "a-1", Destination: "35551234567", EndReason: "normal", ChargedSeconds: 5}
	valid2 := &pb.CallCdr{CallId: "c-2", AccountId: "a-1", Destination: "35551234567", EndReason: "balance_exhausted", ChargedSeconds: 3}
	recs := []*kgo.Record{
		cdrRecord(t, "cdrs.raw", valid),
		cdrRecord(t, "cdrs.raw", valid), // duplicate of the same call
		cdrRecord(t, "cdrs.raw", valid2),
		{kgo.Record{Topic: "cdrs.raw", Key: []byte("bad"), Value: []byte("not-proto")}}, // malformed bytes: pointer composite inside a slice literal needs no &
		cdrRecord(t, "cdrs.raw", &pb.CallCdr{AccountId: "a-2", EndReason: "normal"}),   // no call_id
	}
	for _, rec := range recs {
		if err := p.Handle(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}

	norm := readRecords(t, "cdrs.normalized", 2)
	if len(norm) != 2 {
		t.Fatalf("normalized count %d, want 2", len(norm))
	}
	for _, r := range norm {
		if len(r.Headers) != 0 {
			t.Fatalf("normalized record carries headers: %+v", r.Headers)
		}
		var c pb.CallCdr
		if err := proto.Unmarshal(r.Value, &c); err != nil {
			t.Fatal(err)
		}
		if c.CallId != "c-1" && c.CallId != "c-2" {
			t.Fatalf("unexpected call in normalized: %q", c.CallId)
		}
	}
	if c1 := norm[0]; string(c1.Key) != "c-1" || string(norm[1].Key) != "c-2" {
		t.Fatalf("normalized order/keys wrong: %q, %q", norm[0].Key, norm[1].Key)
	}

	rej := readRecords(t, "cdrs.rejected", 2)
	if len(rej) != 2 {
		t.Fatalf("rejected count %d, want 2", len(rej))
	}
	reasons := map[string]bool{}
	for _, r := range rej {
		for _, h := range r.Headers {
			if h.Key == "reason" {
				reasons[string(h.Value)] = true
			}
		}
	}
	if !reasons["decode_error"] || !reasons["missing_call_id"] {
		t.Fatalf("rejected reasons %v, want decode_error and missing_call_id", reasons)
	}

	// Ledger state: both valid calls marked published.
	for _, id := range []string{"c-1", "c-2"} {
		var published bool
		if err := pool.QueryRow(ctx, `SELECT published FROM cdrs WHERE call_id = $1`, id).Scan(&published); err != nil {
			t.Fatal(err)
		}
		if !published {
			t.Fatalf("call %s not marked published", id)
		}
	}
}

// TestHandleReplaySkips: re-handling an already-published record emits nothing.
func TestHandleReplaySkips(t *testing.T) {
	pool := newPool(t)
	cl := newKafka(t)
	p := NewProcessor(pool, cl, "cdrs.normalized", "cdrs.rejected")
	ctx := context.Background()
	c := &pb.CallCdr{CallId: "c-replay", AccountId: "a-1", EndReason: "normal"}
	rec := cdrRecord(t, "cdrs.raw", c)
	if err := p.Handle(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(ctx, rec); err != nil {
		t.Fatal(err)
	}
	norm := readRecords(t, "cdrs.normalized", 1)
	count := 0
	for _, r := range norm {
		if string(r.Key) == "c-replay" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("c-replay appears %d times, want 1", count)
	}
}
```

- [ ] **Step 3: Run to see it fail**

Run: `cd services/mediation && go vet ./... && go test -run TestHandle ./...`
Expected: FAIL — `undefined: NewProcessor` (and the stack must be up: `make up` first; the test skips if Postgres/Kafka are unreachable).

- [ ] **Step 4: Implement**

`services/mediation/pipeline.go`:

```go
package main

import (
	"context"
	"log/slog"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	pb "omnifoni/mediation/gen/mediationpb"
)

// Processor turns raw CDR records into normalized ones: decode, validate,
// dedup via the ledger, produce. Rejected records go to the dead-letter topic
// with a reason header; nothing is ever dropped silently.
type Processor struct {
	pool       *pgxpool.Pool
	prod       *kgo.Client
	normalized string
	rejected   string
}

func NewProcessor(pool *pgxpool.Pool, cl *kgo.Client, normalized, rejected string) *Processor {
	return &Processor{pool: pool, prod: cl, normalized: normalized, rejected: rejected}
}

// Handle processes one raw record. Errors mean "don't commit past me"; the
// caller retries. At-least-once: a crash between produce and the published
// flag can re-produce once; downstream dedup by call_id handles it.
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) error {
	c := &pb.CallCdr{}
	if err := proto.Unmarshal(rec.Value, c); err != nil {
		return p.reject(ctx, rec, "decode_error", err)
	}
	if reason := Validate(c); reason != "" {
		return p.reject(ctx, rec, reason, nil)
	}
	return p.publish(ctx, rec, c)
}

func (p *Processor) reject(ctx context.Context, rec *kgo.Record, reason string, cause error) error {
	if cause != nil {
		slog.Error("cdr rejected", "reason", reason, "err", cause)
	} else {
		slog.Error("cdr rejected", "reason", reason)
	}
	return p.produce(ctx, p.rejected, rec.Key, rec.Value, reason)
}

func (p *Processor) publish(ctx context.Context, rec *kgo.Record, c *pb.CallCdr) error {
	tag, err := p.pool.Exec(ctx,
		`INSERT INTO cdrs (call_id) VALUES ($1) ON CONFLICT (call_id) DO NOTHING`, c.GetCallId())
	if err != nil {
		return err
	}
	published := false
	if tag.RowsAffected() == 0 {
		if err := p.pool.QueryRow(ctx,
			`SELECT published FROM cdrs WHERE call_id = $1`, c.GetCallId()).Scan(&published); err != nil {
			return err
		}
	}
	if published {
		return nil
	}
	if err := p.produce(ctx, p.normalized, rec.Key, rec.Value, ""); err != nil {
		return err
	}
	_, err = p.pool.Exec(ctx, `UPDATE cdrs SET published = true WHERE call_id = $1`, c.GetCallId())
	return err
}

func (p *Processor) produce(ctx context.Context, topic string, key, value []byte, reason string) error {
	r := &kgo.Record{Topic: topic, Key: key, Value: value}
	if reason != "" {
		r.Headers = []kgo.RecordHeader{{Key: "reason", Value: []byte(reason)}}
	}
	return p.prod.ProduceSync(ctx, r).FirstErr()
}
```

Also add `github.com/google/uuid` to mediation's go.mod (test uses it):

Run: `cd services/mediation && go get github.com/google/uuid@v1.6.0 && go mod tidy`

- [ ] **Step 5: Run to see it pass**

Run (stack up): `cd services/mediation && go test ./...`
Expected: PASS (TestValidate, TestHandlePipeline, TestHandleReplaySkips).

- [ ] **Step 6: Commit**

```bash
git add services/mediation/pipeline.go services/mediation/pipeline_test.go services/mediation/go.mod services/mediation/go.sum
git commit -m "feat(mediation): dedup pipeline with normalized/rejected topics"
```

---

### Task 4: Mediation service wiring (main, compose, Dockerfile, init.sql, smoke, Makefile)

**Files:**
- Create: `services/mediation/main.go`
- Create: `services/mediation/Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `ops/postgres-init.sql`
- Modify: `ops/smoke.sh`
- Modify: `Makefile` (test target)
- Create: `services/mediation/run_test.go`

**Interfaces:**
- Consumes: `NewProcessor`, `Handle`, `Run` (Tasks 2–3), env vars as below.
- Produces: running `mediation` compose service; `Run(ctx context.Context, cl *kgo.Client, p *Processor)` signature for run_test.go.

- [ ] **Step 1: Write the failing run-loop test**

The loop is tested at unit level with a fake-ish path: feed records through a Kafka client against the real stack, run `Run` briefly in a goroutine, assert output. `services/mediation/run_test.go`:

```go
package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	pb "omnifoni/mediation/gen/mediationpb"
)

// TestRunLoop: records fed to the raw topic reach the normalized topic
// through the full consume-produce-commit loop. The consumer client that Run
// uses must be subscribed to the per-test topic.
func TestRunLoop(t *testing.T) {
	pool := newPool(t)
	prod := newKafka(t)
	raw := "cdrs.raw-loop-" + uuid.New().String()
	norm := raw + "-normalized"
	p := NewProcessor(pool, prod, norm, "cdrs.rejected")

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers()...),
		kgo.ConsumerGroup("mediation-loop-"+uuid.New().String()),
		kgo.ConsumeTopics(raw),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, consumer, p)

	c := &pb.CallCdr{CallId: "c-loop-" + uuid.New().String(), AccountId: "a-1", EndReason: "normal"}
	rec := cdrRecord(t, raw, c)
	if err := prod.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
		t.Fatal(err)
	}

	want, err := proto.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range readRecords(t, norm, 1) {
			if string(r.Key) == c.CallId && proto.Equal(r.Value, want) {
				cancel()
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("normalized record never appeared")
}
```

Note: `readRecords` opens a new client per call; with per-test unique topics this is cheap and reliable. The first read before the record arrives returns zero records; the loop retries.

- [ ] **Step 2: Run to see it fail**

Run: `cd services/mediation && go test -run TestRunLoop ./...`
Expected: FAIL — `undefined: Run`.

- [ ] **Step 3: Implement Run and main**

Append to `services/mediation/mediation.go` (keeps the loop beside its Processor):

```go
// Run consumes cdrs.raw in small batches, handling records sequentially. A
// failed record is retried in-process with backoff; offsets are committed
// only after every record of the batch was handled. Keys pin a call to one
// partition, so this is serial per call.
func Run(ctx context.Context, cl *kgo.Client, p *Processor) {
	for {
		fetches := cl.PollRecords(ctx, 100)
		lasts := map[string]*kgo.Record{}
		for rec := fetches.RecordIter().Next(); rec != nil; rec = fetches.RecordIter().Next() {
			for {
				err := p.Handle(ctx, rec)
				if err == nil {
					break
				}
				slog.Error("handle failed, retrying", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			lasts[rec.Topic+"/"+strconv.Itoa(int(rec.Partition))] = rec
		}
		if ctx.Err() != nil {
			return
		}
		var toCommit []*kgo.Record
		for _, rec := range lasts {
			toCommit = append(toCommit, rec)
		}
		if len(toCommit) > 0 {
			if err := cl.CommitRecords(ctx, toCommit...); err != nil {
				slog.Error("commit failed", "err", err)
			}
		}
	}
}
```

Extend the imports in `mediation.go` to:

```go
import (
	"context"
	"log/slog"
	"strconv"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/jackc/pgx/v5/pgxpool"

	pb "omnifoni/mediation/gen/mediationpb"
)
```

`services/mediation/main.go`:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	kgo "github.com/twmb/franz-go/pkg/kgo"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := env("MEDIATION_DSN", "postgres://postgres:dev-only@localhost:5432/mediation")
	brokers := []string{env("MEDIATION_KAFKA", "localhost:9092")}
	raw := env("MEDIATION_TOPIC_RAW", "cdrs.raw")
	normalized := env("MEDIATION_TOPIC_NORMALIZED", "cdrs.normalized")
	rejected := env("MEDIATION_TOPIC_REJECTED", "cdrs.rejected")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	// ponytail: dev default; production resolves the mesh identity
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("mediation"),
		kgo.ConsumeTopics(raw),
		kgo.DisableAutoCommit(), // commit only after handling
	)
	if err != nil {
		slog.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	slog.Info("mediation up", "brokers", brokers, "raw", raw)
	Run(ctx, cl, NewProcessor(pool, cl, normalized, rejected))
}
```

- [ ] **Step 4: Dockerfile, compose, init.sql, smoke, Makefile**

`services/mediation/Dockerfile` (same shape as catalogue's):

```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/mediation .

FROM gcr.io/distroless/static
COPY --from=build /bin/mediation /mediation
ENTRYPOINT ["/mediation"]
```

In `docker-compose.yml`, add after the `switching` service:

```yaml
  mediation:
    build: ./services/mediation
    environment:
      MEDIATION_DSN: postgres://postgres:dev-only@postgres:5432/mediation
      MEDIATION_KAFKA: kafka:9092
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
```

`ops/postgres-init.sql`, append one line:

```sql
CREATE DATABASE mediation;
```

`ops/smoke.sh`, replace the two context-DB lines:

```bash
dbs=$(docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT count(*) FROM pg_database WHERE datistemplate=false AND datname IN ('identity','catalogue','charging','billing','payments','routing','sbc','mediation')")
[ "$dbs" = "8" ] || fail "expected 8 context databases, got $dbs"
```

`Makefile` `test` target, add the line (alphabetical, before switching):

```make
	cd services/mediation && go vet ./... && go test ./...
```

- [ ] **Step 5: Reset data volume and verify**

Run:
```bash
make down && docker volume rm omni-fon_pgdata && make up
```
Expected: all services healthy, mediation container running.

Run: `bash ops/smoke.sh`
Expected: ends with `smoke: PASS` (8 databases).

Run: `docker logs omni-fon-mediation-1 2>&1 | tail -1`
Expected: `mediation up` line.

- [ ] **Step 6: Run tests**

Run: `cd services/mediation && go vet ./... && go test ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add services/mediation/main.go services/mediation/mediation.go services/mediation/Dockerfile services/mediation/run_test.go docker-compose.yml ops/postgres-init.sql ops/smoke.sh Makefile
git commit -m "feat(mediation): service wiring, compose, init.sql, smoke, tests"
```

---

### Task 5: Switching builds a CDR per finished call (TDD, pure part)

**Files:**
- Modify: `services/switching/calls.go`
- Create: `services/switching/cdrs.go` (buildCDR only in this task)
- Create: `services/switching/cdrs_test.go`
- Modify: `services/switching/go.mod`

**Interfaces:**
- Consumes: `mediationpb.CallCdr` via `replace omnifoni/mediation => ../mediation`.
- Produces: `call` struct gains `accountID, destination, cli string` and `start time.Time` fields (set in `StartCall`); `buildCDR(c *call, endReason string, chargedSeconds int64, endedAt time.Time) *mediationpb.CallCdr`; `CDRPublisher` interface with `PublishCDR(c *mediationpb.CallCdr) error`; `NewRegistry()` keeps a nil publisher (nil-safe); `NewRegistryWithCDRs(pub CDRPublisher) *Registry`; `Registry.publishCDR(c *call, endReason string, total int64)`. Task 6 implements the Kafka publisher.

- [ ] **Step 1: Add the mediation dependency**

In `services/switching/go.mod`, add `replace omnifoni/mediation => ../mediation` above the existing `replace omnifoni/charging` line, and `omnifoni/mediation v0.0.0-00010101000000-000000000000` to the require block (same placeholder version as charging).

Run: `cd services/switching && go mod tidy`
Expected: go.sum updated; exits 0.

- [ ] **Step 2: Write the failing test**

`services/switching/cdrs_test.go`:

```go
package main

import (
	"testing"
	"time"
)

func TestBuildCDR(t *testing.T) {
	start := time.UnixMilli(1000)
	c := &call{id: "c1", accountID: "a1", destination: "35551234567", cli: "35551234568", start: start}
	got := buildCDR(c, "balance_exhausted", 42, time.UnixMilli(43000))
	if got.GetCallId() != "c1" || got.GetAccountId() != "a1" ||
		got.GetDestination() != "35551234567" || got.GetCli() != "35551234568" ||
		got.GetStartedAtMs() != 1000 || got.GetEndedAtMs() != 43000 ||
		got.GetChargedSeconds() != 42 || got.GetEndReason() != "balance_exhausted" {
		t.Fatalf("wrong CDR: %+v", got)
	}
}
```

- [ ] **Step 3: Run to see it fail**

Run: `cd services/switching && go test -run TestBuildCDR ./...`
Expected: FAIL — `undefined: buildCDR` (and unknown fields on `call`).

- [ ] **Step 4: Implement**

`services/switching/calls.go` — change the `call` struct to:

```go
type call struct {
	id, sessionID string
	stop          chan struct{}
	stopOnce      sync.Once

	accountID, destination, cli string
	start                       time.Time

	mu     sync.Mutex
	cond   *sync.Cond
	events []*pb.CallEvent
	done   bool
}
```

Replace the `Registry` struct definition with the version below, and add the interface and constructor after `NewRegistry`:

```go
// CDRPublisher emits the final call record for a finished call.
type CDRPublisher interface {
	PublishCDR(c *mediationpb.CallCdr) error
}

type Registry struct {
	mu     sync.Mutex
	calls  map[string]*call
	cdrPub CDRPublisher // nil in tests without Kafka
}

func NewRegistryWithCDRs(pub CDRPublisher) *Registry {
	r := NewRegistry()
	r.cdrPub = pub
	return r
}
```

In `StartCall`, build the call with the new fields (replace the `c := &call{...}` block):

```go
	c := &call{
		id: uuid.New().String(), sessionID: cauth.GetSessionId(),
		stop:        make(chan struct{}),
		accountID:   req.GetAccountId(),
		destination: req.GetDestination(),
		cli:         req.GetCli(),
		start:       time.Now(),
	}
```

Add the import `mediationpb "omnifoni/mediation/gen/mediationpb"` to `calls.go`.

Add the publisher call sites in `runCall` (each right after its `emit`):

```go
		case <-c.stop:
			_, _ = charger.Release(context.Background(),
				&chargingpb.ReleaseRequest{SessionId: c.sessionID})
			c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_RELEASED,
				ChargedSeconds: total})
			r.publishCDR(c, "normal", total)
			return
```

```go
			case err != nil:
				slog.Error("ocs charge failed, terminating", "call", c.id, "err", err)
				c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_TERMINATED,
					Reason: "ocs_unavailable", ChargedSeconds: total})
				r.publishCDR(c, "ocs_unavailable", total)
				return
			case resp.GetDenied():
				c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_TERMINATED,
					Reason: "balance_exhausted", ChargedSeconds: total})
				r.publishCDR(c, "balance_exhausted", total)
				return
```

Append to `calls.go`:

```go
func (r *Registry) publishCDR(c *call, endReason string, total int64) {
	if r.cdrPub == nil {
		return // ponytail: nil in tests without Kafka; production always wires one
	}
	err := r.cdrPub.PublishCDR(buildCDR(c, endReason, total, time.Now()))
	if err != nil {
		slog.Error("cdr publish failed", "call", c.id, "err", err)
	}
}
```

`services/switching/cdrs.go` (this task: only buildCDR):

```go
package main

import (
	"time"

	mediationpb "omnifoni/mediation/gen/mediationpb"
)

// buildCDR derives the final call detail record from a finished call.
func buildCDR(c *call, endReason string, chargedSeconds int64, endedAt time.Time) *mediationpb.CallCdr {
	return &mediationpb.CallCdr{
		CallId:         c.id,
		AccountId:      c.accountID,
		Destination:    c.destination,
		Cli:            c.cli,
		StartedAtMs:    c.start.UnixMilli(),
		EndedAtMs:      endedAt.UnixMilli(),
		ChargedSeconds: chargedSeconds,
		EndReason:      endReason,
	}
}
```

- [ ] **Step 5: Run to see it pass**

Run: `cd services/switching && go vet ./... && go test -run 'TestBuildCDR' ./...`
Expected: PASS. (Full suite runs in Task 7; tests needing the stack still pass with nil publisher since `publishCDR` no-ops.)

- [ ] **Step 6: Commit**

```bash
git add services/switching/calls.go services/switching/cdrs.go services/switching/cdrs_test.go services/switching/go.mod services/switching/go.sum
git commit -m "feat(switching): build and publish CDRs on call end (publisher optional)"
```

---

### Task 6: Switching Kafka publisher and integration test

**Files:**
- Modify: `services/switching/cdrs.go` (add the Kafka publisher)
- Modify: `services/switching/main.go`
- Modify: `services/switching/main_test.go`
- Modify: `docker-compose.yml` (switching env)
- Create: `services/switching/cdr_kafka_test.go`

**Interfaces:**
- Consumes: `CDRPublisher`, `NewRegistryWithCDRs` (Task 5), franz-go.
- Produces: `kafkaCDRs` struct with `PublishCDR` implementing `CDRPublisher`; compose env `SWITCHING_KAFKA: kafka:9092`.

- [ ] **Step 1: Add franz-go**

Run: `cd services/switching && go get github.com/twmb/franz-go@latest && go mod tidy`

- [ ] **Step 2: Write the failing integration test**

`services/switching/cdr_kafka_test.go`:

```go
package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	mediationpb "omnifoni/mediation/gen/mediationpb"
	pb "omnifoni/switching/gen/switchingpb"
)

func kafkaAddr() string {
	if s := os.Getenv("SWITCHING_KAFKA"); s != "" {
		return s
	}
	return "localhost:9092"
}

// TestCDREmittedOnStop: stopping a call lands exactly one CDR on cdrs.raw
// with the call's fields.
func TestCDREmittedOnStop(t *testing.T) {
	seed(t, "acc-cdr", 1000, "3555", "1.000000")

	prod, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr()))
	if err != nil {
		t.Skip("kafka unavailable:", err)
	}
	t.Cleanup(prod.Close)

	client := startSwitchingWithCDR(t, chargingAddr(), &kafkaCDRs{topic: "cdrs.raw", prod: prod})

	resp := startCall(t, client, "acc-cdr", "35551234567")
	if !resp.Allowed {
		t.Fatalf("denied: %+v", resp)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, err := pb.NewSwitchingClient(client).StopCall(context.Background(),
		&pb.StopCallRequest{CallId: resp.CallId}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr()),
		kgo.ConsumerGroup("switching-cdr-test-"+uuid.New().String()),
		kgo.ConsumeTopics("cdrs.raw"),
		kgo.StartOffsetEarliest(),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	for ctx.Err() == nil {
		f := cl.PollRecords(ctx, 100)
		for r := f.RecordIter().Next(); r != nil; r = f.RecordIter().Next() {
			if string(r.Key) != resp.CallId {
				continue // history from other test runs
			}
			var c mediationpb.CallCdr
			if err := proto.Unmarshal(r.Value, &c); err != nil {
				t.Fatal(err)
			}
			if c.AccountId != "acc-cdr" || c.Destination != "35551234567" ||
				c.ChargedSeconds < 1 || c.EndReason != "normal" {
				t.Fatalf("wrong CDR fields: %+v", &c)
			}
			cancel()
			return
		}
	}
	t.Fatal("CDR for call never appeared on cdrs.raw")
}
```

- [ ] **Step 3: Add the test helper**

In `services/switching/main_test.go`, generalize the harness: rename `startSwitching` to take a publisher and keep the old name as a wrapper so existing call sites don't change:

Replace the existing `startSwitching` function with:

```go
// startSwitching serves an in-process switching server backed by the given
// charging endpoint and returns a client connection to it. pub may be nil.
func startSwitchingWithCDR(t *testing.T, chargingEndpoint string, pub CDRPublisher) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient(chargingEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterSwitchingServer(srv, &switchingServer{reg: NewRegistryWithCDRs(pub), cc: cc})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	client, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func startSwitching(t *testing.T, chargingEndpoint string) *grpc.ClientConn {
	return startSwitchingWithCDR(t, chargingEndpoint, nil)
}
```

- [ ] **Step 4: Run to see it fail**

Run: `cd services/switching && go test -run TestCDREmittedOnStop ./...`
Expected: FAIL — `undefined: kafkaCDRs`.

- [ ] **Step 5: Implement the publisher and main wiring**

`services/switching/cdrs.go`, add:

```go
import (
	"context"

	"google.golang.org/protobuf/proto"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

// kafkaCDRs publishes finished-call CDRs to the raw topic.
type kafkaCDRs struct {
	topic string
	prod  *kgo.Client
}

func (k *kafkaCDRs) PublishCDR(c *mediationpb.CallCdr) error {
	v, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	return k.prod.ProduceSync(context.Background(),
		&kgo.Record{Topic: k.topic, Key: []byte(c.GetCallId()), Value: v}).FirstErr()
}
```

(Merge the imports into the file's single import block; the file then has `time`, `context`, `google.golang.org/protobuf/proto`, `kgo`, `mediationpb`.)

`services/switching/main.go` — read the broker and wire the publisher:

```go
import (
	"log/slog"
	"net"
	"os"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	pb "omnifoni/switching/gen/switchingpb"
)
```

In `main`, after the `chargingAddr` setup and before creating the registry:

```go
	brokers := []string{os.Getenv("SWITCHING_KAFKA")}
	if len(brokers) == 1 && brokers[0] == "" {
		// ponytail: dev default; production composes from secrets
		brokers[0] = "localhost:9092"
	}
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		slog.Error("cdr producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()
```

and change the server registration line to:

```go
	pb.RegisterSwitchingServer(srv, &switchingServer{reg: NewRegistryWithCDRs(&kafkaCDRs{topic: "cdrs.raw", prod: prod}), cc: cc})
```

and extend the startup log:

```go
	slog.Info("switching up", "addr", addr, "charging", chargingAddr, "kafka", brokers[0])
```

`docker-compose.yml` — add to the `switching` service environment:

```yaml
      SWITCHING_KAFKA: kafka:9092
```

- [ ] **Step 6: Run to see it pass**

Run: `cd services/switching && go vet ./... && go test ./...`
Expected: PASS (stack up; the new test skips only if Kafka is unreachable).

- [ ] **Step 7: Commit**

```bash
git add services/switching/cdrs.go services/switching/main.go services/switching/main_test.go services/switching/cdr_kafka_test.go services/switching/go.mod services/switching/go.sum docker-compose.yml
git commit -m "feat(switching): publish CDRs to Kafka cdrs.raw"
```

---

### Task 7: End-to-end verification and AGENTS.md

**Files:**
- Modify: `AGENTS.md` (status section)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Full verification from scratch**

Run:
```bash
make down && docker volume rm omni-fon_pgdata && make up && bash ops/smoke.sh
```
Expected: `smoke: PASS`.

Run:
```bash
make test
```
Expected: all four services pass `go vet` and `go test`.

- [ ] **Step 2: Manual end-to-end CDR check**

Run:
```bash
docker exec omni-fon-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep cdrs
```
Expected: `cdrs.raw` exists (created by switching's producer).

Run a call through the live services (catalogue rate, charging account, then switching), then check the normalized topic:
```bash
docker exec omni-fon-postgres-1 psql -U postgres -d catalogue -c \
  "INSERT INTO rates (prefix, rate_per_sec) VALUES ('3555', 1.0) ON CONFLICT (prefix) DO UPDATE SET rate_per_sec = 1.0"
docker exec omni-fon-postgres-1 psql -U postgres -d charging -c \
  "INSERT INTO accounts (account_id, kind, balance) VALUES ('e2e-acc', 'prepaid', 1000) ON CONFLICT (account_id) DO UPDATE SET balance = 1000"
grpcurl -plaintext -d '{"account_id":"e2e-acc","destination":"35551234567"}' \
  localhost:50052 switching.v1.Switching/StartCall
```
Take the `callId` from the response, then:
```bash
grpcurl -plaintext -d '{"callId":"<callId>"}' localhost:50052 switching.v1.Switching/StopCall
sleep 1
docker exec omni-fon-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cdrs.normalized --from-beginning --timeout-ms 5000 | grep -c call_id
```
Expected: at least one message containing the call_id. If `grpcurl`/reflections are missing, verify via the mediation DB instead:
```bash
docker exec omni-fon-postgres-1 psql -U postgres -d mediation -c "SELECT call_id, published FROM cdrs ORDER BY created_at DESC LIMIT 5"
```
Expected: the e2e call row with `published = true`.

- [ ] **Step 3: Update AGENTS.md status**

Add to the Status section of `AGENTS.md` (after the catalogue line):

```markdown
- P2 slice one: `services/mediation` consumes `cdrs.raw`, validates, dedups via the mediation DB `cdrs` ledger (call_id PK), publishes `cdrs.normalized` (and `cdrs.rejected` dead letters with a `reason` header), then commits Kafka offsets. No gRPC port. `services/switching` publishes one `mediation.v1.CallCdr` (proto in `proto/mediation/v1/mediation.proto`) to `cdrs.raw` on call end via franz-go; Kafka addr envs are `SWITCHING_KAFKA` and `MEDIATION_KAFKA` (default localhost:9092). Mediation is at-least-once with a tiny crash-window duplicate; Billing dedups by call_id. Design: `docs/superpowers/specs/2026-11-12-mediation-design.md`.
```

And under the "Dev data layer" line, no changes needed; but add to the init/quirk area:

```markdown
- The `mediation` database (like all context DBs) is created by `ops/postgres-init.sql` on first init only; a fresh volume (`docker volume rm omni-fon_pgdata`) is required if the volume predates it.
```

- [ ] **Step 4: Commit**

```bash
git add AGENTS.md
git commit -m "docs: P2 slice one (mediation) status"
```

---

## Self-review notes

- Spec coverage: proto (T1), ledger+validate (T2), pipeline+rejected (T3), service+compose+smoke (T4), switching CDR build (T5), Kafka emit (T6), e2e + docs (T7). All spec sections map to a task.
- Type consistency: `Processor.Handle(ctx, *kgo.Record) error` used identically in T3 tests and T4's `Run`; `CDRPublisher.PublishCDR` used identically in T5 and T6; `NewRegistryWithCDRs` in T5/T6.
- Known limitation preserved from spec: crash between produce and publish-flag can duplicate once; Billing dedups later.
