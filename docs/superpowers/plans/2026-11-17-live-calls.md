# Live Call Monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose in-flight calls from Switching through Reports as `GET /reports/live-calls` (JSON and CSV), covering the "advance live call" and "live switch monitoring" parity items.

**Architecture:** Switching snapshots its registry (live = `!done`, with OCS `max_seconds` and per-tick `charged_seconds`) behind a new gRPC method. Reports re-exposes that method under its tenant/account scoping and serves it from the existing HTTP facade with the shared CSV helpers.

**Tech Stack:** Go 1.27.4, protobuf/gRPC, stdlib HTTP, ClickHouse is untouched, Postgres/psql only for smoke seeding.

## Global Constraints

- Stubs are regenerated only with `make protoc` from the repo root (protoc + plugins in `/root/go/bin`); never hand-edit `services/*/gen/`.
- Money is integer minor units; CSV writes raw integers, no floats or formatting.
- Tenant scoping: role `user` → 403; reseller forced to own tenant; admin requires `tenant_id`; empty account scope returns an empty 200 (never all accounts).
- Source errors: `InvalidArgument` → 400, everything else → 503 (`sourceError`/`grpcStatus`).
- No new service/module; ports and envs follow the existing naming (`REPORTS_SWITCHING_ADDR`).
- Commit messages are lowercase conventional commits exactly as written in each task.
- `make up` never rebuilds: after code changes run `docker compose build <svc> && docker compose up -d <svc>`.
- Full-suite gate: `make test` exits 0 with 12 `ok` modules; `make smoke` prints `smoke: PASS`.
- Tests truncate dev data — run live/smoke verification before `make test`.

---

### Task 1: Switching live-call snapshot

**Files:**
- Modify: `proto/switching/v1/switching.proto`
- Modify: `services/switching/calls.go`
- Modify: `services/switching/grpc.go`
- Create: `services/switching/live.go`
- Create: `services/switching/live_test.go`
- Regenerated: `services/switching/gen/switchingpb/*` (via `make protoc`)

**Interfaces:**
- Consumes: existing `Registry`, `call`, `runCall`, `switchingServer`.
- Produces: `type LiveCallSnapshot struct { CallID, SessionID, AccountID, Destination, CLI, TrunkID string; StartedAt time.Time; MaxSeconds, ChargedSeconds int64 }`; `func (r *Registry) LiveCalls(accountIDs []string) []LiveCallSnapshot`; gRPC `switching.v1.Switching/ListLiveCalls` with `ListLiveCallsRequest{repeated string account_ids}`, `LiveCall{call_id, session_id, account_id, destination, cli, trunk_id, started_at_ms, elapsed_seconds, max_seconds, charged_seconds}`, `ListLiveCallsResponse{repeated LiveCall calls}`.

- [ ] **Step 1: Add the proto messages and RPC**

In `proto/switching/v1/switching.proto`, add the RPC inside `service Switching` after `CallEvents`:

```proto
  // Snapshots in-flight calls (finished calls kept for replay are excluded).
  rpc ListLiveCalls(ListLiveCallsRequest) returns (ListLiveCallsResponse);
```

Append after the `CallEvent` message:

```proto
message ListLiveCallsRequest {
  repeated string account_ids = 1; // empty = all live calls (internal/trusted)
}

message LiveCall {
  string call_id = 1;
  string session_id = 2;
  string account_id = 3;
  string destination = 4;
  string cli = 5;
  string trunk_id = 6;
  int64 started_at_ms = 7;
  int64 elapsed_seconds = 8;
  int64 max_seconds = 9;
  int64 charged_seconds = 10;
}

message ListLiveCallsResponse {
  repeated LiveCall calls = 1;
}
```

- [ ] **Step 2: Regenerate stubs**

Run: `make protoc`
Expected: exits 0; `git status --short` shows changes only under `services/switching/gen/switchingpb/` (and `services/reports/gen/reportspb/` stays unchanged at this point).

- [ ] **Step 3: Write the failing unit test**

Create `services/switching/live_test.go`:

```go
package main

import (
	"testing"
	"time"
)

func TestLiveCallsSnapshot(t *testing.T) {
	reg := NewRegistry()
	t0 := time.Now().Add(-10 * time.Second)
	mk := func(id, acct string, start time.Time, charged int64) *call {
		c := &call{
			id: id, sessionID: "s-" + id, accountID: acct, destination: "2782",
			cli: "cli", trunkID: "trunk-1", start: start,
			maxSeconds: 600, chargedSeconds: charged,
		}
		reg.calls[id] = c
		return c
	}
	mk("c2", "a1", t0.Add(time.Second), 2)
	mk("c1", "a1", t0, 5)
	mk("c3", "a2", t0, 0)
	done := mk("c4", "a1", t0, 0)
	done.mu.Lock()
	done.done = true
	done.mu.Unlock()

	all := reg.LiveCalls(nil)
	if len(all) != 3 {
		t.Fatalf("all = %d, want 3", len(all))
	}
	if all[0].CallID != "c1" || all[1].CallID != "c2" || all[2].CallID != "c3" {
		t.Fatalf("order = %q %q %q", all[0].CallID, all[1].CallID, all[2].CallID)
	}
	if all[0].ChargedSeconds != 5 || all[0].MaxSeconds != 600 || all[0].SessionID != "s-c1" {
		t.Fatalf("snapshot = %+v", all[0])
	}
	if got := reg.LiveCalls([]string{"a2"}); len(got) != 1 || got[0].CallID != "c3" {
		t.Fatalf("filter a2 = %+v", got)
	}
	if got := reg.LiveCalls([]string{"zz"}); len(got) != 0 {
		t.Fatalf("filter zz = %+v", got)
	}
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd services/switching && go test -run TestLiveCallsSnapshot ./...`
Expected: compile failure — `undefined: LiveCallSnapshot`, `c.maxSeconds undefined`, `reg.LiveCalls undefined`.

- [ ] **Step 5: Add the call fields and per-tick counter**

In `services/switching/calls.go`, extend the `call` struct:

```go
	accountID, destination, cli string
	trunkID                     string
	start                       time.Time
	maxSeconds                  int64 // OCS authorize cap, immutable
	chargedSeconds              int64 // updated under mu on every tick
	traceCtx                    context.Context
```

In `StartCall`, set `maxSeconds` when constructing the call:

```go
	c := &call{
		id: callID, sessionID: cauth.GetSessionId(),
		stop:        make(chan struct{}),
		accountID:   req.GetAccountId(),
		destination: req.GetDestination(),
		cli:         req.GetCli(),
		trunkID:     trunk,
		start:       time.Now(),
		maxSeconds:  cauth.GetMaxSeconds(),
		traceCtx:    trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)),
	}
```

In `runCall`'s ticker case, record the attempt before the OCS call:

```go
		case <-ticker.C:
			total++
			c.mu.Lock()
			c.chargedSeconds = total
			c.mu.Unlock()
			resp, err := charger.Charge(context.Background(), &chargingpb.ChargeRequest{
				SessionId: c.sessionID, TotalSeconds: total})
```

- [ ] **Step 6: Implement the registry snapshot**

Create `services/switching/live.go`:

```go
package main

import (
	"sort"
	"time"
)

// LiveCallSnapshot is one in-flight call as seen by ListLiveCalls.
type LiveCallSnapshot struct {
	CallID         string
	SessionID      string
	AccountID      string
	Destination    string
	CLI            string
	TrunkID        string
	StartedAt      time.Time
	MaxSeconds     int64
	ChargedSeconds int64
}

// LiveCalls returns a snapshot of calls that have not finished, oldest first.
// accountIDs empty means every account. Lock order: r.mu, then each call's mu.
func (r *Registry) LiveCalls(accountIDs []string) []LiveCallSnapshot {
	filter := make(map[string]bool, len(accountIDs))
	for _, id := range accountIDs {
		filter[id] = true
	}

	r.mu.Lock()
	calls := make([]*call, 0, len(r.calls))
	for _, c := range r.calls {
		if c != nil && (len(filter) == 0 || filter[c.accountID]) {
			calls = append(calls, c)
		}
	}
	r.mu.Unlock()

	out := make([]LiveCallSnapshot, 0, len(calls))
	for _, c := range calls {
		c.mu.Lock()
		if !c.done {
			out = append(out, LiveCallSnapshot{
				CallID: c.id, SessionID: c.sessionID, AccountID: c.accountID,
				Destination: c.destination, CLI: c.cli, TrunkID: c.trunkID,
				StartedAt: c.start, MaxSeconds: c.maxSeconds,
				ChargedSeconds: c.chargedSeconds,
			})
		}
		c.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].StartedAt.Before(out[j].StartedAt)
		}
		return out[i].CallID < out[j].CallID
	})
	return out
}
```

- [ ] **Step 7: Add the gRPC adapter method**

In `services/switching/grpc.go`, add `"time"` to the imports and append:

```go
func (s *switchingServer) ListLiveCalls(ctx context.Context, req *pb.ListLiveCallsRequest) (*pb.ListLiveCallsResponse, error) {
	ctx, span := otel.Tracer("omnifoni/switching").Start(ctx, "switching.list_live_calls",
		trace.WithAttributes(attribute.Int("account_ids", len(req.GetAccountIds()))))
	defer span.End()
	_ = ctx

	snaps := s.reg.LiveCalls(req.GetAccountIds())
	resp := &pb.ListLiveCallsResponse{Calls: make([]*pb.LiveCall, 0, len(snaps))}
	now := time.Now()
	for _, c := range snaps {
		elapsed := int64(now.Sub(c.StartedAt).Seconds())
		if elapsed < 0 {
			elapsed = 0
		}
		resp.Calls = append(resp.Calls, &pb.LiveCall{
			CallId: c.CallID, SessionId: c.SessionID, AccountId: c.AccountID,
			Destination: c.Destination, Cli: c.CLI, TrunkId: c.TrunkID,
			StartedAtMs: c.StartedAt.UnixMilli(), ElapsedSeconds: elapsed,
			MaxSeconds: c.MaxSeconds, ChargedSeconds: c.ChargedSeconds,
		})
	}
	return resp, nil
}
```

- [ ] **Step 8: Run the unit test to verify it passes**

Run: `cd services/switching && go test -run TestLiveCallsSnapshot ./...`
Expected: `ok  omnifoni/switching` (needs the dev stack only for the package's `TestMain`, which dials charging; `make up` first if not running).

- [ ] **Step 9: Write the integration test**

Append to `services/switching/live_test.go`:

```go
func TestListLiveCalls(t *testing.T) {
	seed(t, "acc-live", 1000, "3555", "1.000000")
	client := startSwitching(t, chargingAddr())

	resp := startCall(t, client, "acc-live", "35551234567")
	if !resp.Allowed {
		t.Fatalf("denied: %+v", resp)
	}
	t.Cleanup(func() { stopCall(t, client, resp.CallId) })

	list, err := pb.NewSwitchingClient(client).ListLiveCalls(context.Background(),
		&pb.ListLiveCallsRequest{AccountIds: []string{"acc-live"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetCalls()) != 1 {
		t.Fatalf("calls = %d, want 1", len(list.GetCalls()))
	}
	got := list.GetCalls()[0]
	if got.GetCallId() != resp.CallId || got.GetSessionId() != resp.SessionId {
		t.Fatalf("got %+v, start %+v", got, resp)
	}
	if got.GetMaxSeconds() != resp.GetMaxSeconds() || got.GetMaxSeconds() <= 0 {
		t.Fatalf("max_seconds = %d, start %d", got.GetMaxSeconds(), resp.GetMaxSeconds())
	}
	if got.GetStartedAtMs() == 0 || got.GetElapsedSeconds() < 0 {
		t.Fatalf("started_at_ms = %d elapsed = %d", got.GetStartedAtMs(), got.GetElapsedSeconds())
	}

	stopCall(t, client, resp.CallId)
	waitReleased(t, client, resp.CallId)
	list, err = pb.NewSwitchingClient(client).ListLiveCalls(context.Background(),
		&pb.ListLiveCallsRequest{AccountIds: []string{"acc-live"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetCalls()) != 0 {
		t.Fatalf("finished call still listed: %+v", list.GetCalls())
	}
}
```

Add `"context"` to that file's imports.

- [ ] **Step 10: Run the integration test**

Run: `cd services/switching && go test -run 'TestLiveCallsSnapshot|TestListLiveCalls' ./...`
Expected: PASS. If it fails with `Unimplemented`, the proto regen or the adapter method is missing.

- [ ] **Step 11: Vet and commit**

```bash
cd services/switching && go vet ./...
cd /root/Omni-Foní && git add proto/switching/v1/switching.proto services/switching/gen services/switching/calls.go services/switching/grpc.go services/switching/live.go services/switching/live_test.go
git commit -m "feat(switching): list live calls"
```

---

### Task 2: Reports gRPC live-call surface and wiring

**Files:**
- Modify: `proto/reports/v1/reports.proto`
- Modify: `services/reports/grpc.go`
- Create: `services/reports/grpc_live.go`
- Create: `services/reports/grpc_live_test.go`
- Modify: `services/reports/main.go`
- Modify: `services/reports/go.mod`
- Modify: `services/reports/Dockerfile`
- Modify: `docker-compose.yml`
- Regenerated: `services/reports/gen/reportspb/*` (via `make protoc`)

**Interfaces:**
- Consumes: Task 1's `switching.v1.Switching/ListLiveCalls` and `switchingpb` types.
- Produces: `type LiveCallSource interface { ListLiveCalls(context.Context, *switchingpb.ListLiveCallsRequest, ...grpc.CallOption) (*switchingpb.ListLiveCallsResponse, error) }`; `reportsServer.live LiveCallSource`; gRPC `reports.v1.Reports/ListLiveCalls` with `LiveCallsRequest{repeated string account_ids}`, `LiveCall` (same ten fields as switching), `LiveCallsResponse{repeated LiveCall calls}`; env `REPORTS_SWITCHING_ADDR` (default `switching:50052`).

- [ ] **Step 1: Add the proto messages and RPC**

In `proto/reports/v1/reports.proto`, add the RPC inside `service Reports` after `ListLowBalance`:

```proto
  rpc ListLiveCalls(LiveCallsRequest) returns (LiveCallsResponse);
```

Append at the end of the file:

```proto
message LiveCallsRequest {
  repeated string account_ids = 1;
}

message LiveCall {
  string call_id = 1;
  string session_id = 2;
  string account_id = 3;
  string destination = 4;
  string cli = 5;
  string trunk_id = 6;
  int64 started_at_ms = 7;
  int64 elapsed_seconds = 8;
  int64 max_seconds = 9;
  int64 charged_seconds = 10;
}

message LiveCallsResponse {
  repeated LiveCall calls = 1;
}
```

- [ ] **Step 2: Regenerate stubs**

Run: `make protoc`
Expected: exits 0; only `services/reports/gen/reportspb/` changes this time.

- [ ] **Step 3: Write the failing gRPC test**

Create `services/reports/grpc_live_test.go`:

```go
package main

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/reports/gen/reportspb"
	switchingpb "omnifoni/switching/gen/switchingpb"
)

type fakeLive struct {
	req  *switchingpb.ListLiveCallsRequest
	resp *switchingpb.ListLiveCallsResponse
	err  error
}

func (f *fakeLive) ListLiveCalls(_ context.Context, req *switchingpb.ListLiveCallsRequest, _ ...grpc.CallOption) (*switchingpb.ListLiveCallsResponse, error) {
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func TestReportsListLiveCalls(t *testing.T) {
	fl := &fakeLive{resp: &switchingpb.ListLiveCallsResponse{Calls: []*switchingpb.LiveCall{{
		CallId: "c1", SessionId: "s1", AccountId: "a1", Destination: "2782", Cli: "cli",
		TrunkId: "trunk-1", StartedAtMs: 1000, ElapsedSeconds: 5, MaxSeconds: 600, ChargedSeconds: 4,
	}}}}
	s := &reportsServer{live: fl}

	resp, err := s.ListLiveCalls(context.Background(), &pb.LiveCallsRequest{AccountIds: []string{"a1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(fl.req.GetAccountIds()) != 1 || fl.req.GetAccountIds()[0] != "a1" {
		t.Fatalf("source request = %+v", fl.req)
	}
	if len(resp.GetCalls()) != 1 {
		t.Fatalf("calls = %d", len(resp.GetCalls()))
	}
	got := resp.GetCalls()[0]
	if got.GetCallId() != "c1" || got.GetSessionId() != "s1" || got.GetAccountId() != "a1" ||
		got.GetDestination() != "2782" || got.GetCli() != "cli" || got.GetTrunkId() != "trunk-1" ||
		got.GetStartedAtMs() != 1000 || got.GetElapsedSeconds() != 5 ||
		got.GetMaxSeconds() != 600 || got.GetChargedSeconds() != 4 {
		t.Fatalf("mapped = %+v", got)
	}

	fl.err = status.Error(codes.Unavailable, "down")
	if _, err := s.ListLiveCalls(context.Background(), &pb.LiveCallsRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd services/reports && go test -run TestReportsListLiveCalls ./...`
Expected: compile failure — `unknown field live in struct literal`, `s.ListLiveCalls undefined`.

- [ ] **Step 5: Implement the source and method**

Create `services/reports/grpc_live.go`:

```go
package main

import (
	"context"

	"google.golang.org/grpc"

	pb "omnifoni/reports/gen/reportspb"
	switchingpb "omnifoni/switching/gen/switchingpb"
)

// LiveCallSource is the switching read surface used by the live report.
type LiveCallSource interface {
	ListLiveCalls(context.Context, *switchingpb.ListLiveCallsRequest, ...grpc.CallOption) (*switchingpb.ListLiveCallsResponse, error)
}

func (s *reportsServer) ListLiveCalls(ctx context.Context, req *pb.LiveCallsRequest) (*pb.LiveCallsResponse, error) {
	resp, err := s.live.ListLiveCalls(ctx, &switchingpb.ListLiveCallsRequest{AccountIds: req.GetAccountIds()})
	if err != nil {
		return nil, sourceError(err)
	}
	out := &pb.LiveCallsResponse{Calls: make([]*pb.LiveCall, 0, len(resp.GetCalls()))}
	for _, c := range resp.GetCalls() {
		out.Calls = append(out.Calls, &pb.LiveCall{
			CallId: c.GetCallId(), SessionId: c.GetSessionId(), AccountId: c.GetAccountId(),
			Destination: c.GetDestination(), Cli: c.GetCli(), TrunkId: c.GetTrunkId(),
			StartedAtMs: c.GetStartedAtMs(), ElapsedSeconds: c.GetElapsedSeconds(),
			MaxSeconds: c.GetMaxSeconds(), ChargedSeconds: c.GetChargedSeconds(),
		})
	}
	return out, nil
}
```

In `services/reports/grpc.go`, add the field:

```go
type reportsServer struct {
	pb.UnimplementedReportsServer
	store    *Store
	payments PaymentSource
	charges  ChargeSource
	live     LiveCallSource
}
```

- [ ] **Step 6: Add the module dependency and build**

```bash
cd services/reports
go mod edit -require=omnifoni/switching@v0.0.0-00010101000000-000000000000 \
  -replace=omnifoni/switching=../switching \
  -replace=omnifoni/routing=../routing \
  -replace=omnifoni/sbc=../sbc
go mod tidy
go build ./...
```

Expected: `go mod tidy` succeeds and `go build ./...` exits 0. If it complains about `omnifoni/routing`/`omnifoni/sbc` versions, the replaces above are missing.

- [ ] **Step 7: Run the gRPC test to verify it passes**

Run: `cd services/reports && go test -run TestReportsListLiveCalls ./...`
Expected: PASS (this test uses a fake, so the stack is only needed for the package's ClickHouse `TestMain`; `make up` first if needed).

- [ ] **Step 8: Wire the client in main**

In `services/reports/main.go`:

Add to the env block:

```go
	swAddr := env("REPORTS_SWITCHING_ADDR", "switching:50052")
```

Add the import:

```go
	switchingpb "omnifoni/switching/gen/switchingpb"
```

After the charging client block, dial switching:

```go
	swConn, err := grpc.NewClient(swAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("switching client", "err", err)
		os.Exit(1)
	}
```

Add `live: switchingpb.NewSwitchingClient(swConn),` to the `api := &reportsServer{...}` literal and `"switching", swAddr` to the `reports HTTP up` log line.

- [ ] **Step 9: Update the image build and compose**

In `services/reports/Dockerfile`, add after `COPY charging/ ./charging/`:

```dockerfile
COPY switching/ ./switching/
COPY routing/ ./routing/
COPY sbc/ ./sbc/
```

In `docker-compose.yml`, add to the `reports` environment map:

```yaml
      REPORTS_SWITCHING_ADDR: switching:50052
```

and to its `depends_on` map:

```yaml
      switching:
        condition: service_started
```

- [ ] **Step 10: Build the image and verify the wiring**

```bash
cd /root/Omni-Foní
docker compose build reports
docker compose up -d reports
docker inspect --format '{{.State.Status}}' omni-fon-reports-1
```

Expected: build succeeds and the container reports `running`. (A missing sibling `COPY` fails the build — that is the failure this step catches.)

- [ ] **Step 11: Vet and commit**

```bash
cd services/reports && go vet ./...
cd /root/Omni-Foní && git add proto/reports/v1/reports.proto services/reports/gen services/reports/grpc.go services/reports/grpc_live.go services/reports/grpc_live_test.go services/reports/main.go services/reports/go.mod services/reports/go.sum services/reports/Dockerfile docker-compose.yml
git commit -m "feat(reports): list live calls over grpc"
```

---

### Task 3: HTTP live-call endpoint

**Files:**
- Modify: `services/reports/http.go`
- Modify: `services/reports/main.go`
- Modify: `services/reports/http_test.go`

**Interfaces:**
- Consumes: `reportsServer.ListLiveCalls` from Task 2; `bearer`, `Accounts`, `csvRequested`, `streamCSV`, `writeCSVHeaderOnly`, `intersect`, `grpcStatus`.
- Produces: `GET /reports/live-calls` returning `{"calls":[...]}` or CSV with header `call_id,session_id,account_id,destination,cli,trunk_id,started_at_ms,elapsed_seconds,max_seconds,charged_seconds`.

- [ ] **Step 1: Write the failing HTTP tests**

In `services/reports/http_test.go`, add `live` to the `facade` server literal:

```go
		live: &fakeLive{resp: &switchingpb.ListLiveCallsResponse{Calls: []*switchingpb.LiveCall{{
			CallId: "c1", SessionId: "s1", AccountId: "a1", Destination: "2782", Cli: "cli",
			TrunkId: "trunk-1", StartedAtMs: 1000, ElapsedSeconds: 5, MaxSeconds: 600, ChargedSeconds: 4,
		}}}},
```

and register the route in `facade`'s mux:

```go
	mux.HandleFunc("GET /reports/live-calls", h.LiveCalls)
```

Add the import `switchingpb "omnifoni/switching/gen/switchingpb"`. Append the test at the end of the file:

```go
func TestHTTPLiveCalls(t *testing.T) {
	srv := facade(t)

	code, body := get(t, srv.URL+"/reports/live-calls", "reseller")
	if code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	calls, ok := body["calls"].([]any)
	if !ok || len(calls) != 1 {
		t.Fatalf("calls = %v", body["calls"])
	}
	row := calls[0].(map[string]any)
	if row["call_id"] != "c1" || row["account_id"] != "a1" || row["max_seconds"] != float64(600) ||
		row["charged_seconds"] != float64(4) || row["elapsed_seconds"] != float64(5) {
		t.Fatalf("row = %v", row)
	}

	if code, _ := get(t, srv.URL+"/reports/live-calls", "user"); code != http.StatusForbidden {
		t.Fatalf("user = %d, want 403", code)
	}
	if code, _ := get(t, srv.URL+"/reports/live-calls", "admin"); code != http.StatusBadRequest {
		t.Fatalf("admin without tenant = %d, want 400", code)
	}
	if code, _ := get(t, srv.URL+"/reports/live-calls?tenant_id=t1", "admin"); code != http.StatusOK {
		t.Fatalf("admin t1 = %d", code)
	}
	code, body = get(t, srv.URL+"/reports/live-calls", "empty")
	if code != http.StatusOK {
		t.Fatalf("empty tenant = %d", code)
	}
	if calls, ok := body["calls"].([]any); !ok || len(calls) != 0 {
		t.Fatalf("empty tenant calls = %v", body["calls"])
	}

	code, hdr, body := getRaw(t, srv.URL+"/reports/live-calls?format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("csv = %d (%s)", code, body)
	}
	if ct := hdr.Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 2 ||
		lines[0] != "call_id,session_id,account_id,destination,cli,trunk_id,started_at_ms,elapsed_seconds,max_seconds,charged_seconds" {
		t.Fatalf("csv lines = %q", lines)
	}
	if lines[1] != "c1,s1,a1,2782,cli,trunk-1,1000,5,600,4" {
		t.Fatalf("csv row = %q", lines[1])
	}
	if code, _, _ := getRaw(t, srv.URL+"/reports/live-calls?format=bogus", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bogus format = %d, want 400", code)
	}

	down := &httpServer{api: &reportsServer{
		live: &fakeLive{err: status.Error(codes.Unavailable, "down")},
	}, auth: NewAuthenticator(fakeIdentity(t).URL)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/live-calls", down.LiveCalls)
	downSrv := httptest.NewServer(mux)
	t.Cleanup(downSrv.Close)
	if code, _ := get(t, downSrv.URL+"/reports/live-calls", "reseller"); code != http.StatusServiceUnavailable {
		t.Fatalf("outage = %d, want 503", code)
	}
}
```

Add `"google.golang.org/grpc/codes"` and `"google.golang.org/grpc/status"` to the imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd services/reports && go test -run TestHTTPLiveCalls ./...`
Expected: compile failure — `h.LiveCalls undefined`, `unknown field live` (if Task 2's field is missing).

- [ ] **Step 3: Extend the API interface and add the handler**

In `services/reports/http.go`, add to `type API interface`:

```go
	ListLiveCalls(context.Context, *pb.LiveCallsRequest) (*pb.LiveCallsResponse, error)
```

Append at the end of the file:

```go
var liveCallCSVHeader = []string{
	"call_id", "session_id", "account_id", "destination", "cli", "trunk_id",
	"started_at_ms", "elapsed_seconds", "max_seconds", "charged_seconds",
}

type liveCallJSON struct {
	CallID         string `json:"call_id"`
	SessionID      string `json:"session_id"`
	AccountID      string `json:"account_id"`
	Destination    string `json:"destination"`
	CLI            string `json:"cli"`
	TrunkID        string `json:"trunk_id"`
	StartedAtMS    int64  `json:"started_at_ms"`
	ElapsedSeconds int64  `json:"elapsed_seconds"`
	MaxSeconds     int64  `json:"max_seconds"`
	ChargedSeconds int64  `json:"charged_seconds"`
}

type liveCallsJSON struct {
	Calls []liveCallJSON `json:"calls"`
}

func (h *httpServer) LiveCalls(w http.ResponseWriter, r *http.Request) {
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	q := r.URL.Query()
	ids, code, err := h.auth.Accounts(r.Context(), token, q.Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "live-calls", liveCallCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, liveCallsJSON{Calls: []liveCallJSON{}})
		return
	}
	req := &pb.LiveCallsRequest{AccountIds: ids}
	if requested := q["account_id"]; len(requested) > 0 {
		req.AccountIds = intersect(ids, requested)
		if len(req.AccountIds) == 0 {
			if csvOut {
				writeCSVHeaderOnly(w, "live-calls", liveCallCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, liveCallsJSON{Calls: []liveCallJSON{}})
			return
		}
	}
	if csvOut {
		streamCSV(w, r, "live-calls", liveCallCSVHeader, func(string) ([][]string, string, error) {
			resp, err := h.api.ListLiveCalls(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			rows := make([][]string, 0, len(resp.GetCalls()))
			for _, c := range resp.GetCalls() {
				rows = append(rows, []string{
					c.GetCallId(), c.GetSessionId(), c.GetAccountId(), c.GetDestination(),
					c.GetCli(), c.GetTrunkId(), strconv.FormatInt(c.GetStartedAtMs(), 10),
					strconv.FormatInt(c.GetElapsedSeconds(), 10), strconv.FormatInt(c.GetMaxSeconds(), 10),
					strconv.FormatInt(c.GetChargedSeconds(), 10),
				})
			}
			return rows, "", nil
		}, nil)
		return
	}

	resp, err := h.api.ListLiveCalls(r.Context(), req)
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	out := liveCallsJSON{Calls: []liveCallJSON{}}
	for _, c := range resp.GetCalls() {
		out.Calls = append(out.Calls, liveCallJSON{
			CallID: c.GetCallId(), SessionID: c.GetSessionId(), AccountID: c.GetAccountId(),
			Destination: c.GetDestination(), CLI: c.GetCli(), TrunkID: c.GetTrunkId(),
			StartedAtMS: c.GetStartedAtMs(), ElapsedSeconds: c.GetElapsedSeconds(),
			MaxSeconds: c.GetMaxSeconds(), ChargedSeconds: c.GetChargedSeconds(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}
```

- [ ] **Step 4: Register the route**

In `services/reports/main.go`, after the low-balance route:

```go
	mux.HandleFunc("GET /reports/live-calls", h.LiveCalls)
```

- [ ] **Step 5: Run the HTTP test to verify it passes**

Run: `cd services/reports && go test -run TestHTTPLiveCalls ./...`
Expected: PASS.

- [ ] **Step 6: Run the whole reports package**

Run: `cd services/reports && go vet ./... && go test ./...`
Expected: PASS (no regressions in the existing CSV/money tests).

- [ ] **Step 7: Commit**

```bash
cd /root/Omni-Foní && git add services/reports/http.go services/reports/main.go services/reports/http_test.go
git commit -m "feat(reports): serve live calls over http"
```

---

### Task 4: Smoke probe and slice close-out

**Files:**
- Create: `services/reports/cmd/smokelive/main.go`
- Modify: `ops/smoke.sh`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: Task 1's switching `ListLiveCalls`; Task 3's `/reports/live-calls`; seeded dev rows.
- Produces: `go run ./cmd/smokelive` printing `ok: live calls`; smoke seeds `acc-smoke-live` under the `smoke` tenant.

- [ ] **Step 1: Create the smoke probe**

Create `services/reports/cmd/smokelive/main.go`:

```go
// Command smokelive starts one call through switching, proves the reports
// live-call surface lists it, then stops it.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "omnifoni/reports/gen/reportspb"
	switchingpb "omnifoni/switching/gen/switchingpb"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	swAddr := env("SWITCHING_ADDR", "localhost:50052")
	rpAddr := env("REPORTS_ADDR", "localhost:50061")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	swConn, err := grpc.NewClient(swAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial switching:", err)
		os.Exit(1)
	}
	defer swConn.Close()
	rpConn, err := grpc.NewClient(rpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial reports:", err)
		os.Exit(1)
	}
	defer rpConn.Close()

	sw := switchingpb.NewSwitchingClient(swConn)
	start, err := sw.StartCall(ctx, &switchingpb.StartCallRequest{
		AccountId: "acc-smoke-live", Destination: "27821110000", Cli: "27820000000",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "start call:", err)
		os.Exit(1)
	}
	if !start.GetAllowed() {
		fmt.Fprintf(os.Stderr, "start call denied: %s\n", start.GetReason())
		os.Exit(1)
	}
	defer func() {
		_, _ = sw.StopCall(context.Background(), &switchingpb.StopCallRequest{CallId: start.GetCallId()})
	}()

	list, err := pb.NewReportsClient(rpConn).ListLiveCalls(ctx,
		&pb.LiveCallsRequest{AccountIds: []string{"acc-smoke-live"}})
	if err != nil {
		fmt.Fprintln(os.Stderr, "list live calls:", err)
		os.Exit(1)
	}
	found := false
	for _, c := range list.GetCalls() {
		if c.GetCallId() == start.GetCallId() {
			found = true
		}
	}
	if !found {
		fmt.Fprintf(os.Stderr, "call %s missing from live report (%d rows)\n",
			start.GetCallId(), len(list.GetCalls()))
		os.Exit(1)
	}
	if _, err := sw.StopCall(ctx, &switchingpb.StopCallRequest{CallId: start.GetCallId()}); err != nil {
		fmt.Fprintln(os.Stderr, "stop call:", err)
		os.Exit(1)
	}
	fmt.Println("ok: live calls")
}
```

- [ ] **Step 2: Seed the live-call fixtures in smoke**

In `ops/smoke.sh`, after the `sessions` insert line and before the reports CSV check, add:

```bash
docker exec omni-fon-postgres-1 psql -U postgres -d catalogue -qAt -c "INSERT INTO rates(prefix,rate_per_sec) VALUES ('2782',0.010000) ON CONFLICT (prefix) DO UPDATE SET rate_per_sec=EXCLUDED.rate_per_sec" >/dev/null
docker exec omni-fon-postgres-1 psql -U postgres -d charging -qAt -c "INSERT INTO accounts(account_id,kind,balance,credit_limit) VALUES ('acc-smoke-live','prepaid',100000,0) ON CONFLICT (account_id) DO UPDATE SET balance=EXCLUDED.balance, kind='prepaid', credit_limit=0" >/dev/null
docker exec omni-fon-postgres-1 psql -U postgres -d routing -qAt -c "INSERT INTO trunks(trunk_id,enabled) VALUES ('trunk-smoke',true) ON CONFLICT (trunk_id) DO UPDATE SET enabled=true" >/dev/null
docker exec omni-fon-postgres-1 psql -U postgres -d routing -qAt -c "INSERT INTO lcr_routes(prefix,trunk_id,cost_per_sec) VALUES ('2782','trunk-smoke',1) ON CONFLICT (prefix,trunk_id) DO UPDATE SET cost_per_sec=EXCLUDED.cost_per_sec" >/dev/null
docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "INSERT INTO accounts(account_id,tenant_id,created_by) VALUES ('acc-smoke-live','$tid','$uid') ON CONFLICT (account_id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, created_by=EXCLUDED.created_by" >/dev/null
```

Then, next to the existing probes:

```bash
(cd services/reports && go run ./cmd/smokelive) || fail "live calls probe"
```

- [ ] **Step 3: Build the changed images and run smoke**

```bash
cd /root/Omni-Foní
docker compose build switching reports
docker compose up -d switching reports
make smoke
```

Expected: `ok: live calls` and `smoke: PASS`. If switching denies the call, check the seeded rate/route/account (`insufficient_balance` means the account row; `no_route` means the LCR row).

- [ ] **Step 4: Run the full suite**

Run: `make test > /tmp/live-test.log 2>&1; echo "exit=$?"; grep -cE '^ok' /tmp/live-test.log`
Expected: `exit=0` and `12`.

- [ ] **Step 5: Commit the smoke probe**

```bash
cd /root/Omni-Foní && git add services/reports/cmd/smokelive/main.go ops/smoke.sh
git commit -m "feat(reports): add live calls smoke"
```

- [ ] **Step 6: Record the slice in AGENTS.md and close it**

Add one sentence to the P5 block (after the slice-three sentence):

```markdown
- P5 slice four: switching serves `ListLiveCalls` (registry snapshot of in-flight calls: `max_seconds` from the OCS authorize, `charged_seconds` per tick, ended calls excluded, ordered `started_at ASC, call_id ASC`, empty `account_ids` = all). Reports re-exposes it as gRPC `ListLiveCalls` and HTTP `GET /reports/live-calls` (same tenant/auth scoping as the other reports; `format=csv` uses the shared helpers; empty scope returns an empty 200; source errors are 503). Env `REPORTS_SWITCHING_ADDR` (default switching:50052); the reports image now also copies switching/routing/sbc. Smoke seeds a `smoke`-tenant account + rate + LCR route and runs `cmd/smokelive` (start, list, stop).
```

Then commit:

```bash
cd /root/Omni-Foní && git add AGENTS.md
git commit -m "docs: close the live calls slice"
```

---

## Self-Review

- Spec coverage: switching contract (§3) → Task 1; reports chain (§4) → Task 2; HTTP facade incl. CSV/errors (§5) → Task 3; testing/smoke (§6) → Tasks 1–4; decisions/risks (§7) → enforced by Task 1's `!done` filter and per-tick counter, Task 3's empty-scope behavior, Task 2's Dockerfile sibling copies.
- Placeholder scan: every step carries exact code, paths and commands; no TBDs.
- Type consistency: `LiveCallSnapshot` fields match the proto field names in both services; `LiveCallSource` matches the generated `switchingpb.SwitchingClient` method; `liveCallJSON` matches `liveCallCSVHeader`; the smoke probe imports only stubs available in the reports module after Task 2.
