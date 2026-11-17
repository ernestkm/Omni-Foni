# Money Reports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add payment, charges-history and low-balance reports: read RPCs in payments/charging, mirrored reports RPCs, and authenticated HTTP endpoints.

**Architecture:** The owning services expose keyset-paginated read RPCs over their existing Postgres tables (epoch-ms filters, no new stores). `services/reports` parses report date semantics once, forwards epoch ms through narrow Go interfaces, and serves both gRPC and the bearer/tenant-scoped HTTP facade.

**Tech Stack:** Go 1.27.4, protobuf/gRPC, pgx/v5, ClickHouse (untouched), Postgres 17.

## Global Constraints

- Money is integer minor units (1/10000 currency); no floats in responses.
- Dates are half-open `[from, to)` UTC; date-only `to` includes that whole day (`parseBound(to, true)` adds 24h).
- `from` and `to` are required for dated reports; reversed range → `InvalidArgument`.
- Limits: default 100, clamped 1..1000 in both reports and source services.
- Cursors are opaque base64url strings; malformed cursor → `InvalidArgument`.
- Stubs are generated only with `make protoc` (protoc and plugins in `/root/go/bin`); never hand-edit `gen/`.
- Tests hit the dev stack (`make up`); `make test` runs `go vet ./... && go test ./...` per module, no `-race`.
- After code changes: `docker compose build <svc> && docker compose up -d <svc>` (`make up` never rebuilds).
- Compose build contexts for services with sibling modules: `context: ./services`, `dockerfile: <svc>/Dockerfile`.
- Commit messages are exactly the ones in each task.

## File Structure

- `proto/payments/v1/payments.proto` — adds `ListPayments` (owner: payments).
- `services/payments/list.go` — cursor/limit helpers and `ListPayments` handler.
- `services/payments/list_test.go` — Postgres integration tests.
- `proto/charging/v1/charging.proto` — adds `ListCharges`, `ListLowBalance`.
- `services/charging/reports.go` — both read handlers.
- `services/charging/reports_test.go` — ledger aggregation and threshold tests.
- `proto/reports/v1/reports.proto` — `MoneyFilter` + three report RPCs.
- `services/reports/grpc_money.go` — source interfaces, date parsing, forwarding, mapping.
- `services/reports/grpc_money_test.go` — fake-source gRPC tests.
- `services/reports/http.go` — three handlers (extend existing file).
- `services/reports/http_test.go` — facade tests (extend existing file).
- `services/reports/main.go`, `Dockerfile`, `go.mod` — wiring.
- `docker-compose.yml`, `ops/smoke.sh`, `services/reports/cmd/smokereports/main.go`, `AGENTS.md` — deployment/smoke/docs.

---

### Task 1: payments ListPayments RPC

**Files:**
- Modify: `proto/payments/v1/payments.proto`
- Modify: `services/payments/gen/paymentspb/*` (regenerated)
- Create: `services/payments/list.go`
- Create: `services/payments/list_test.go`

**Interfaces:**
- Consumes: existing `paymentsServer{pool *pgxpool.Pool}` and `pool` test global.
- Produces: `payments.v1.Payments/ListPayments`; Go handler `func (s *paymentsServer) ListPayments(ctx, *pb.ListPaymentsRequest) (*pb.ListPaymentsResponse, error)`; cursor format `base64url("<created_at_ms>|<payment_id>")`.

- [ ] **Step 1: Add the proto messages and RPC**

In `proto/payments/v1/payments.proto`, add to `service Payments` after `ListAccountsForPeriod`:

```proto
  // ListPayments returns recorded and void payments in a time window.
  // Money is integer minor units; status is surfaced, not filtered by default.
  rpc ListPayments(ListPaymentsRequest) returns (ListPaymentsResponse);
```

Append at the end of the file:

```proto
message ListPaymentsRequest {
  repeated string account_ids = 1; // empty = all accounts
  int64 from_ms = 2;               // inclusive
  int64 to_ms = 3;                 // exclusive
  string status = 4;               // "" = all, else recorded|void
  int32 limit = 5;                 // default 100, clamped 1..1000
  string cursor = 6;               // base64url of "<created_at_ms>|<payment_id>"
}

message Payment {
  int64 payment_id = 1;
  string event_id = 2;
  string account_id = 3;
  string gateway = 4;
  int64 amount_minor = 5;
  string status = 6;
  int64 created_at_ms = 7;
}

message ListPaymentsResponse {
  repeated Payment payments = 1;
  string next_cursor = 2; // "" when the page is the last one
}
```

- [ ] **Step 2: Regenerate stubs**

Run: `make protoc`
Expected: no output, `services/payments/gen/paymentspb/payments_grpc.pb.go` now contains `ListPayments`.

- [ ] **Step 3: Write the failing test**

Create `services/payments/list_test.go`:

```go
package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/payments/gen/paymentspb"
)

func TestListPayments(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE payments`); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC)
	seed := []struct {
		event, account, status string
		amount                 int64
		at                     time.Time
	}{
		{"evt-1", "acc-a", "recorded", 100, base},
		{"evt-2", "acc-a", "recorded", 200, base.Add(time.Minute)},
		{"evt-3", "acc-a", "recorded", 300, base.Add(2 * time.Minute)},
		{"evt-void", "acc-b", "void", 999, base.Add(time.Minute)},
		{"evt-out", "acc-a", "recorded", 400, base.Add(3 * time.Hour)},
	}
	for _, s := range seed {
		if _, err := pool.Exec(ctx,
			`INSERT INTO payments (event_id, account_id, gateway, amount_minor, status, created_at)
			 VALUES ($1, $2, 'stripe', $3, $4, $5)`,
			s.event, s.account, s.amount, s.status, s.at); err != nil {
			t.Fatal(err)
		}
	}

	srv := &paymentsServer{pool: pool}
	from := base.UnixMilli()
	to := base.Add(10 * time.Minute).UnixMilli()

	page1, err := srv.ListPayments(ctx, &pb.ListPaymentsRequest{
		AccountIds: []string{"acc-a"}, FromMs: from, ToMs: to, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.GetPayments()) != 2 {
		t.Fatalf("page1 rows = %d, want 2", len(page1.GetPayments()))
	}
	if got := page1.GetPayments()[0].GetEventId(); got != "evt-3" {
		t.Fatalf("page1[0] = %q, want evt-3 (newest first)", got)
	}
	if got := page1.GetPayments()[1].GetEventId(); got != "evt-2" {
		t.Fatalf("page1[1] = %q, want evt-2", got)
	}
	if page1.GetNextCursor() == "" {
		t.Fatal("page1 next_cursor empty, want a cursor")
	}
	if got := page1.GetPayments()[0].GetCreatedAtMs(); got != base.Add(2*time.Minute).UnixMilli() {
		t.Fatalf("created_at_ms = %d, want %d", got, base.Add(2*time.Minute).UnixMilli())
	}

	page2, err := srv.ListPayments(ctx, &pb.ListPaymentsRequest{
		AccountIds: []string{"acc-a"}, FromMs: from, ToMs: to, Limit: 2,
		Cursor: page1.GetNextCursor()})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.GetPayments()) != 1 || page2.GetPayments()[0].GetEventId() != "evt-1" {
		t.Fatalf("page2 = %v, want only evt-1", page2.GetPayments())
	}
	if page2.GetNextCursor() != "" {
		t.Fatalf("page2 next_cursor = %q, want empty", page2.GetNextCursor())
	}

	void, err := srv.ListPayments(ctx, &pb.ListPaymentsRequest{
		FromMs: from, ToMs: to, Status: "void"})
	if err != nil {
		t.Fatal(err)
	}
	if len(void.GetPayments()) != 1 || void.GetPayments()[0].GetStatus() != "void" {
		t.Fatalf("void filter = %v, want the single void payment", void.GetPayments())
	}

	if _, err := srv.ListPayments(ctx, &pb.ListPaymentsRequest{
		FromMs: from, ToMs: to, Status: "bogus"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad status err = %v, want InvalidArgument", err)
	}
	if _, err := srv.ListPayments(ctx, &pb.ListPaymentsRequest{
		FromMs: from, ToMs: to, Cursor: "!!!"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad cursor err = %v, want InvalidArgument", err)
	}
	if _, err := srv.ListPayments(ctx, &pb.ListPaymentsRequest{
		FromMs: to, ToMs: from}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reversed window err = %v, want InvalidArgument", err)
	}

	empty, err := srv.ListPayments(ctx, &pb.ListPaymentsRequest{FromMs: from, ToMs: from})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.GetPayments()) != 0 {
		t.Fatalf("empty window rows = %d, want 0", len(empty.GetPayments()))
	}
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd services/payments && go test -run TestListPayments ./...`
Expected: build failure, `srv.ListPayments undefined` (or `pb.ListPaymentsRequest undefined` if stubs were not regenerated).

- [ ] **Step 5: Implement `list.go`**

Create `services/payments/list.go`:

```go
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/payments/gen/paymentspb"
)

const (
	defaultPageLimit = 100
	maxPageLimit     = 1000
)

type paymentCursor struct {
	ms int64
	id int64
}

func encodePaymentCursor(ms, id int64) string {
	raw := strconv.FormatInt(ms, 10) + "|" + strconv.FormatInt(id, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodePaymentCursor(s string) (paymentCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return paymentCursor{}, err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return paymentCursor{}, errors.New("malformed cursor")
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return paymentCursor{}, err
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return paymentCursor{}, err
	}
	return paymentCursor{ms: ms, id: id}, nil
}

func pageLimit(n int32) int {
	if n <= 0 {
		return defaultPageLimit
	}
	if int(n) > maxPageLimit {
		return maxPageLimit
	}
	return int(n)
}

// ListPayments returns payments in a half-open epoch-ms window, newest first.
func (s *paymentsServer) ListPayments(ctx context.Context, req *pb.ListPaymentsRequest) (*pb.ListPaymentsResponse, error) {
	switch req.GetStatus() {
	case "", "recorded", "void":
	default:
		return nil, status.Error(codes.InvalidArgument, "status must be recorded or void")
	}
	if req.GetToMs() < req.GetFromMs() {
		return nil, status.Error(codes.InvalidArgument, "to_ms must be >= from_ms")
	}
	limit := pageLimit(req.GetLimit())
	var cur paymentCursor
	hasCursor := false
	if c := req.GetCursor(); c != "" {
		cc, err := decodePaymentCursor(c)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		cur, hasCursor = cc, true
	}

	rows, err := s.pool.Query(ctx,
		`SELECT payment_id, event_id, account_id, gateway, amount_minor, status,
		        floor(extract(epoch FROM created_at) * 1000)::bigint
		 FROM payments
		 WHERE created_at >= to_timestamp($1::double precision / 1000.0)
		   AND created_at <  to_timestamp($2::double precision / 1000.0)
		   AND ($3 = '' OR status = $3)
		   AND (coalesce(cardinality($4::text[]), 0) = 0 OR account_id = ANY($4))
		   AND (NOT $5 OR (created_at, payment_id) <
		        (to_timestamp($6::double precision / 1000.0), $7))
		 ORDER BY created_at DESC, payment_id DESC
		 LIMIT $8`,
		req.GetFromMs(), req.GetToMs(), req.GetStatus(), req.GetAccountIds(),
		hasCursor, cur.ms, cur.id, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type row struct {
		id        int64
		event     string
		account   string
		gateway   string
		amount    int64
		status    string
		createdMs int64
	}
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.event, &r.account, &r.gateway, &r.amount, &r.status, &r.createdMs); err != nil {
			return nil, err
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hasMore := len(found) > limit
	if hasMore {
		found = found[:limit]
	}
	resp := &pb.ListPaymentsResponse{Payments: []*pb.Payment{}}
	for _, r := range found {
		resp.Payments = append(resp.Payments, &pb.Payment{
			PaymentId: r.id, EventId: r.event, AccountId: r.account, Gateway: r.gateway,
			AmountMinor: r.amount, Status: r.status, CreatedAtMs: r.createdMs,
		})
	}
	if hasMore && len(found) > 0 {
		last := found[len(found)-1]
		resp.NextCursor = encodePaymentCursor(last.createdMs, last.id)
	}
	return resp, nil
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd services/payments && go vet ./... && go test -run TestListPayments ./...`
Expected: `ok omnifoni/payments`.

- [ ] **Step 7: Run the whole module**

Run: `cd services/payments && go test ./...`
Expected: `ok omnifoni/payments` (webhook and totals tests still pass).

- [ ] **Step 8: Commit**

```bash
git add proto/payments/v1/payments.proto services/payments
git commit -m "feat(payments): add payment list rpc"
```

---

### Task 2: charging ListCharges and ListLowBalance RPCs

**Files:**
- Modify: `proto/charging/v1/charging.proto`
- Modify: `services/charging/gen/chargingpb/*` (regenerated)
- Create: `services/charging/reports.go`
- Create: `services/charging/reports_test.go`

**Interfaces:**
- Consumes: `Server{pool *pgxpool.Pool}`; test helpers `truncate(t)`, `pool`; tables `accounts`, `sessions`, `ledger`.
- Produces: `charging.v1.Charging/ListCharges` and `ListLowBalance`; cursors `base64url("<ms>|<session_id>")` and `base64url("<balance>|<account_id>")`.

- [ ] **Step 1: Add the proto messages and RPCs**

In `proto/charging/v1/charging.proto`, add to `service Charging` after `Release`:

```proto
  // ListCharges returns one row per OCS session (call) with the summed ledger
  // amount, the latest balance and the last charge time. Read-only report path.
  rpc ListCharges(ListChargesRequest) returns (ListChargesResponse);
  // ListLowBalance returns accounts with balance <= threshold_minor.
  rpc ListLowBalance(ListLowBalanceRequest) returns (ListLowBalanceResponse);
```

Append at the end of the file:

```proto
message ListChargesRequest {
  repeated string account_ids = 1; // empty = all accounts
  int64 from_ms = 2;               // inclusive, on the session's last charge
  int64 to_ms = 3;                 // exclusive
  int32 limit = 4;                 // default 100, clamped 1..1000
  string cursor = 5;               // base64url of "<last_charged_at_ms>|<session_id>"
}

message Charge {
  string session_id = 1;
  string account_id = 2;
  int64 charged_seconds = 3;
  int64 amount_minor = 4;    // sum of ledger deltas, negative
  int64 balance_after = 5;   // balance after the latest ledger row
  int64 last_charged_at_ms = 6;
}

message ListChargesResponse {
  repeated Charge charges = 1;
  string next_cursor = 2;
}

message ListLowBalanceRequest {
  repeated string account_ids = 1; // empty = all accounts
  int64 threshold_minor = 2;       // balance <= threshold
  int32 limit = 3;                 // default 100, clamped 1..1000
  string cursor = 4;               // base64url of "<balance>|<account_id>"
}

message BalanceRow {
  string account_id = 1;
  string kind = 2;
  int64 balance = 3;
  int64 credit_limit = 4;
  string currency = 5;
}

message ListLowBalanceResponse {
  repeated BalanceRow accounts = 1;
  string next_cursor = 2;
}
```

- [ ] **Step 2: Regenerate stubs**

Run: `make protoc`
Expected: no output; `services/charging/gen/chargingpb/charging_grpc.pb.go` contains `ListCharges` and `ListLowBalance`.

- [ ] **Step 3: Write the failing tests**

Create `services/charging/reports_test.go`:

```go
package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/charging/gen/chargingpb"
)

func TestListChargesAggregatesLedger(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx,
		`INSERT INTO accounts (account_id, kind, balance) VALUES
		 ('acc-c', 'prepaid', 996), ('acc-d', 'prepaid', 1000)`); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	s1, s2 := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO sessions (session_id, account_id, rate_per_sec, reserved_seconds, charged_seconds, released, created_at)
		 VALUES ($1, 'acc-c', 1, 10, 3, true, $2), ($3, 'acc-d', 1, 10, 1, true, $4)`,
		s1, base, s2, base.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	ledger := []struct {
		session uuid.UUID
		amount  int64
		balance int64
		at      time.Time
	}{
		{s1, -1, 999, base},
		{s1, -1, 998, base.Add(time.Second)},
		{s1, -2, 996, base.Add(2 * time.Second)},
		{s2, -1, 999, base.Add(time.Hour)},
	}
	for _, l := range ledger {
		if _, err := pool.Exec(ctx,
			`INSERT INTO ledger (session_id, account_id, amount_minor, balance_after, created_at)
			 VALUES ($1, CASE WHEN $1 = $2 THEN 'acc-c' ELSE 'acc-d' END, $3, $4, $5)`,
			l.session, s1, l.amount, l.balance, l.at); err != nil {
			t.Fatal(err)
		}
	}

	srv := NewServer(pool, catClient)
	window, err := srv.ListCharges(ctx, &pb.ListChargesRequest{
		FromMs: base.UnixMilli(), ToMs: base.Add(time.Minute).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	if len(window.GetCharges()) != 1 {
		t.Fatalf("window rows = %d, want 1", len(window.GetCharges()))
	}
	c := window.GetCharges()[0]
	if c.GetSessionId() != s1.String() || c.GetAccountId() != "acc-c" ||
		c.GetChargedSeconds() != 3 || c.GetAmountMinor() != -4 ||
		c.GetBalanceAfter() != 996 || c.GetLastChargedAtMs() != base.Add(2*time.Second).UnixMilli() {
		t.Fatalf("aggregate = %+v", c)
	}

	all, err := srv.ListCharges(ctx, &pb.ListChargesRequest{
		FromMs: base.UnixMilli(), ToMs: base.Add(2 * time.Hour).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	if len(all.GetCharges()) != 2 || all.GetCharges()[0].GetSessionId() != s2.String() {
		t.Fatalf("all = %+v, want 2 rows newest first", all.GetCharges())
	}

	page1, err := srv.ListCharges(ctx, &pb.ListChargesRequest{
		FromMs: base.UnixMilli(), ToMs: base.Add(2 * time.Hour).UnixMilli(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.GetCharges()) != 1 || page1.GetNextCursor() == "" {
		t.Fatalf("page1 = %+v, want one row and a cursor", page1)
	}
	page2, err := srv.ListCharges(ctx, &pb.ListChargesRequest{
		FromMs: base.UnixMilli(), ToMs: base.Add(2 * time.Hour).UnixMilli(), Limit: 1,
		Cursor: page1.GetNextCursor()})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.GetCharges()) != 1 || page2.GetCharges()[0].GetSessionId() != s1.String() {
		t.Fatalf("page2 = %+v, want s1", page2.GetCharges())
	}

	filtered, err := srv.ListCharges(ctx, &pb.ListChargesRequest{
		AccountIds: []string{"acc-d"},
		FromMs:     base.UnixMilli(), ToMs: base.Add(2 * time.Hour).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.GetCharges()) != 1 || filtered.GetCharges()[0].GetAccountId() != "acc-d" {
		t.Fatalf("account filter = %+v", filtered.GetCharges())
	}

	if _, err := srv.ListCharges(ctx, &pb.ListChargesRequest{
		FromMs: base.UnixMilli(), ToMs: base.Add(time.Minute).UnixMilli(),
		Cursor: "!!!"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad cursor err = %v, want InvalidArgument", err)
	}
	if _, err := srv.ListCharges(ctx, &pb.ListChargesRequest{
		FromMs: base.Add(time.Minute).UnixMilli(), ToMs: base.UnixMilli()}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reversed window err = %v, want InvalidArgument", err)
	}
}

func TestListLowBalance(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx,
		`INSERT INTO accounts (account_id, kind, balance, credit_limit) VALUES
		 ('a1', 'prepaid', 0, 0), ('a2', 'prepaid', 5, 0), ('a3', 'prepaid', 10, 0),
		 ('a4', 'postpaid', -20, 100), ('a5', 'postpaid', 50, 0)`); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(pool, catClient)

	zero, err := srv.ListLowBalance(ctx, &pb.ListLowBalanceRequest{ThresholdMinor: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(zero.GetAccounts()) != 2 ||
		zero.GetAccounts()[0].GetAccountId() != "a4" ||
		zero.GetAccounts()[1].GetAccountId() != "a1" {
		t.Fatalf("threshold 0 = %+v, want a4 then a1", zero.GetAccounts())
	}
	if zero.GetAccounts()[0].GetKind() != "postpaid" || zero.GetAccounts()[0].GetCreditLimit() != 100 {
		t.Fatalf("postpaid row = %+v", zero.GetAccounts()[0])
	}

	ten, err := srv.ListLowBalance(ctx, &pb.ListLowBalanceRequest{ThresholdMinor: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(ten.GetAccounts()) != 4 {
		t.Fatalf("threshold 10 rows = %d, want 4 (boundary included)", len(ten.GetAccounts()))
	}

	scoped, err := srv.ListLowBalance(ctx, &pb.ListLowBalanceRequest{
		AccountIds: []string{"a2", "a3"}, ThresholdMinor: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(scoped.GetAccounts()) != 2 || scoped.GetAccounts()[0].GetAccountId() != "a2" {
		t.Fatalf("account filter = %+v, want a2,a3", scoped.GetAccounts())
	}

	page1, err := srv.ListLowBalance(ctx, &pb.ListLowBalanceRequest{ThresholdMinor: 10, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.GetAccounts()) != 2 || page1.GetNextCursor() == "" {
		t.Fatalf("page1 = %+v, want 2 rows and a cursor", page1)
	}
	page2, err := srv.ListLowBalance(ctx, &pb.ListLowBalanceRequest{
		ThresholdMinor: 10, Limit: 2, Cursor: page1.GetNextCursor()})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.GetAccounts()) != 2 || page2.GetNextCursor() != "" {
		t.Fatalf("page2 = %+v, want the last 2 rows", page2)
	}
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cd services/charging && go test -run 'TestListCharges|TestListLowBalance' ./...`
Expected: build failure, `srv.ListCharges undefined`.

- [ ] **Step 5: Implement `reports.go`**

Create `services/charging/reports.go`:

```go
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/charging/gen/chargingpb"
)

const (
	defaultPageLimit = 100
	maxPageLimit     = 1000
)

type chargeCursor struct {
	ms        int64
	sessionID string
}

func encodeChargeCursor(ms int64, sessionID string) string {
	raw := strconv.FormatInt(ms, 10) + "|" + sessionID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeChargeCursor(s string) (chargeCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return chargeCursor{}, err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return chargeCursor{}, errors.New("malformed cursor")
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return chargeCursor{}, err
	}
	return chargeCursor{ms: ms, sessionID: parts[1]}, nil
}

type balanceCursor struct {
	balance   int64
	accountID string
}

func encodeBalanceCursor(balance int64, accountID string) string {
	raw := strconv.FormatInt(balance, 10) + "|" + accountID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeBalanceCursor(s string) (balanceCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return balanceCursor{}, err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return balanceCursor{}, errors.New("malformed cursor")
	}
	balance, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return balanceCursor{}, err
	}
	return balanceCursor{balance: balance, accountID: parts[1]}, nil
}

func pageLimit(n int32) int {
	if n <= 0 {
		return defaultPageLimit
	}
	if int(n) > maxPageLimit {
		return maxPageLimit
	}
	return int(n)
}

// ListCharges returns one row per session, aggregating its ledger rows.
func (s *Server) ListCharges(ctx context.Context, req *pb.ListChargesRequest) (*pb.ListChargesResponse, error) {
	if req.GetToMs() < req.GetFromMs() {
		return nil, status.Error(codes.InvalidArgument, "to_ms must be >= from_ms")
	}
	limit := pageLimit(req.GetLimit())
	var cur chargeCursor
	hasCursor := false
	if c := req.GetCursor(); c != "" {
		cc, err := decodeChargeCursor(c)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		cur, hasCursor = cc, true
	}

	rows, err := s.pool.Query(ctx,
		`SELECT l.session_id::text, l.account_id,
		        coalesce(s.charged_seconds, 0),
		        sum(l.amount_minor)::bigint,
		        (array_agg(l.balance_after ORDER BY l.ledger_id DESC))[1],
		        floor(extract(epoch FROM max(l.created_at)) * 1000)::bigint
		 FROM ledger l
		 LEFT JOIN sessions s ON s.session_id = l.session_id
		 WHERE (coalesce(cardinality($1::text[]), 0) = 0 OR l.account_id = ANY($1))
		 GROUP BY l.session_id, l.account_id, s.charged_seconds
		 HAVING max(l.created_at) >= to_timestamp($2::double precision / 1000.0)
		    AND max(l.created_at) <  to_timestamp($3::double precision / 1000.0)
		    AND (NOT $4 OR (max(l.created_at), l.session_id) <
		         (to_timestamp($5::double precision / 1000.0), $6::uuid))
		 ORDER BY max(l.created_at) DESC, l.session_id DESC
		 LIMIT $7`,
		req.GetAccountIds(), req.GetFromMs(), req.GetToMs(),
		hasCursor, cur.ms, cur.sessionID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type row struct {
		sessionID  string
		accountID  string
		seconds    int64
		amount     int64
		balance    int64
		lastCharge int64
	}
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.sessionID, &r.accountID, &r.seconds, &r.amount, &r.balance, &r.lastCharge); err != nil {
			return nil, err
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hasMore := len(found) > limit
	if hasMore {
		found = found[:limit]
	}
	resp := &pb.ListChargesResponse{Charges: []*pb.Charge{}}
	for _, r := range found {
		resp.Charges = append(resp.Charges, &pb.Charge{
			SessionId: r.sessionID, AccountId: r.accountID, ChargedSeconds: r.seconds,
			AmountMinor: r.amount, BalanceAfter: r.balance, LastChargedAtMs: r.lastCharge,
		})
	}
	if hasMore && len(found) > 0 {
		last := found[len(found)-1]
		resp.NextCursor = encodeChargeCursor(last.lastCharge, last.sessionID)
	}
	return resp, nil
}

// ListLowBalance returns accounts at or below the threshold, lowest first.
func (s *Server) ListLowBalance(ctx context.Context, req *pb.ListLowBalanceRequest) (*pb.ListLowBalanceResponse, error) {
	limit := pageLimit(req.GetLimit())
	var cur balanceCursor
	hasCursor := false
	if c := req.GetCursor(); c != "" {
		cc, err := decodeBalanceCursor(c)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		cur, hasCursor = cc, true
	}

	rows, err := s.pool.Query(ctx,
		`SELECT account_id, kind, balance, credit_limit, currency
		 FROM accounts
		 WHERE balance <= $1
		   AND (coalesce(cardinality($2::text[]), 0) = 0 OR account_id = ANY($2))
		   AND (NOT $3 OR (balance, account_id) > ($4, $5))
		 ORDER BY balance ASC, account_id ASC
		 LIMIT $6`,
		req.GetThresholdMinor(), req.GetAccountIds(),
		hasCursor, cur.balance, cur.accountID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type row struct {
		accountID   string
		kind        string
		balance     int64
		creditLimit int64
		currency    string
	}
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.accountID, &r.kind, &r.balance, &r.creditLimit, &r.currency); err != nil {
			return nil, err
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hasMore := len(found) > limit
	if hasMore {
		found = found[:limit]
	}
	resp := &pb.ListLowBalanceResponse{Accounts: []*pb.BalanceRow{}}
	for _, r := range found {
		resp.Accounts = append(resp.Accounts, &pb.BalanceRow{
			AccountId: r.accountID, Kind: r.kind, Balance: r.balance,
			CreditLimit: r.creditLimit, Currency: r.currency,
		})
	}
	if hasMore && len(found) > 0 {
		last := found[len(found)-1]
		resp.NextCursor = encodeBalanceCursor(last.balance, last.accountID)
	}
	return resp, nil
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd services/charging && go vet ./... && go test -run 'TestListCharges|TestListLowBalance' ./...`
Expected: `ok omnifoni/charging`.

- [ ] **Step 7: Run the whole module**

Run: `cd services/charging && go test ./...`
Expected: `ok omnifoni/charging` (authorize/charge tests still pass).

- [ ] **Step 8: Commit**

```bash
git add proto/charging/v1/charging.proto services/charging
git commit -m "feat(charging): add charge history and low balance rpcs"
```

---

### Task 3: reports money RPCs

**Files:**
- Modify: `proto/reports/v1/reports.proto`
- Modify: `services/reports/gen/reportspb/*` (regenerated)
- Modify: `services/reports/go.mod`
- Modify: `services/reports/grpc.go` (limit helper refactor)
- Create: `services/reports/grpc_money.go`
- Create: `services/reports/grpc_money_test.go`

**Interfaces:**
- Consumes: `paymentspb.ListPayments`, `chargingpb.ListCharges`, `chargingpb.ListLowBalance` from Tasks 1–2; existing `parseBound`, `reportsServer`.
- Produces: `PaymentSource` / `ChargeSource` interfaces; `reportsServer` fields `payments`, `charges`; `clampLimit(int32) int`; report RPCs `ListPayments`, `ListCharges`, `ListLowBalance`.

- [ ] **Step 1: Add the proto messages and RPCs**

In `proto/reports/v1/reports.proto`, add to `service Reports` after `Summarize`:

```proto
  rpc ListPayments(ListPaymentsRequest) returns (ListPaymentsResponse);
  rpc ListCharges(ListChargesRequest) returns (ListChargesResponse);
  rpc ListLowBalance(LowBalanceRequest) returns (LowBalanceResponse);
```

Append at the end of the file:

```proto
message MoneyFilter {
  repeated string account_ids = 1;
  string from = 2; // required; YYYY-MM-DD or RFC3339, inclusive
  string to = 3;   // required; YYYY-MM-DD (whole day) or RFC3339, exclusive
}

message ListPaymentsRequest {
  MoneyFilter filter = 1;
  string status = 2; // "" = all, else recorded|void
  int32 limit = 3;
  string cursor = 4;
}

message Payment {
  int64 payment_id = 1;
  string event_id = 2;
  string account_id = 3;
  string gateway = 4;
  int64 amount_minor = 5;
  string status = 6;
  int64 created_at_ms = 7;
}

message ListPaymentsResponse {
  repeated Payment payments = 1;
  string next_cursor = 2;
}

message ListChargesRequest {
  MoneyFilter filter = 1;
  int32 limit = 2;
  string cursor = 3;
}

message Charge {
  string session_id = 1;
  string account_id = 2;
  int64 charged_seconds = 3;
  int64 amount_minor = 4;
  int64 balance_after = 5;
  int64 last_charged_at_ms = 6;
}

message ListChargesResponse {
  repeated Charge charges = 1;
  string next_cursor = 2;
}

message LowBalanceRequest {
  repeated string account_ids = 1;
  int64 threshold_minor = 2;
  int32 limit = 3;
  string cursor = 4;
}

message BalanceRow {
  string account_id = 1;
  string kind = 2;
  int64 balance = 3;
  int64 credit_limit = 4;
  string currency = 5;
}

message LowBalanceResponse {
  repeated BalanceRow accounts = 1;
  string next_cursor = 2;
}
```

- [ ] **Step 2: Regenerate stubs**

Run: `make protoc`
Expected: no output; `services/reports/gen/reportspb/reports_grpc.pb.go` contains the three new methods.

- [ ] **Step 3: Add module dependencies**

In `services/reports/go.mod`, add replace directives after the existing ones:

```go
replace omnifoni/charging => ../charging

replace omnifoni/payments => ../payments
```

Add to the first `require` block:

```go
	omnifoni/charging v0.0.0-00010101000000-000000000000
	omnifoni/payments v0.0.0-00010101000000-000000000000
```

Then run: `cd services/reports && go mod tidy`
Expected: go.mod/go.sum update without errors.

- [ ] **Step 4: Refactor the limit clamp and add source interfaces**

In `services/reports/grpc.go`, replace the struct with:

```go
type reportsServer struct {
	pb.UnimplementedReportsServer
	store    *Store
	payments PaymentSource
	charges  ChargeSource
}
```

Add after the `defaultLimit`/`maxLimit` constants:

```go
func clampLimit(n int32) int {
	if n <= 0 {
		return defaultLimit
	}
	if int(n) > maxLimit {
		return maxLimit
	}
	return int(n)
}
```

In `ListCdrs`, replace:

```go
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
```

with:

```go
	limit := clampLimit(req.GetLimit())
```

- [ ] **Step 5: Write the failing test**

Create `services/reports/grpc_money_test.go`:

```go
package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chargingpb "omnifoni/charging/gen/chargingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
	pb "omnifoni/reports/gen/reportspb"
)

type fakePayments struct {
	req  *paymentspb.ListPaymentsRequest
	resp *paymentspb.ListPaymentsResponse
	err  error
}

func (f *fakePayments) ListPayments(_ context.Context, req *paymentspb.ListPaymentsRequest) (*paymentspb.ListPaymentsResponse, error) {
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

type fakeCharges struct {
	chargesReq *chargingpb.ListChargesRequest
	balanceReq *chargingpb.ListLowBalanceRequest
	resp       *chargingpb.ListChargesResponse
	balance    *chargingpb.ListLowBalanceResponse
	err        error
}

func (f *fakeCharges) ListCharges(_ context.Context, req *chargingpb.ListChargesRequest) (*chargingpb.ListChargesResponse, error) {
	f.chargesReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *fakeCharges) ListLowBalance(_ context.Context, req *chargingpb.ListLowBalanceRequest) (*chargingpb.ListLowBalanceResponse, error) {
	f.balanceReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.balance, nil
}

func TestReportsListPayments(t *testing.T) {
	fp := &fakePayments{resp: &paymentspb.ListPaymentsResponse{
		Payments: []*paymentspb.Payment{{
			PaymentId: 7, EventId: "evt", AccountId: "a1", Gateway: "stripe",
			AmountMinor: 500, Status: "recorded", CreatedAtMs: 1000,
		}},
		NextCursor: "next",
	}}
	srv := &reportsServer{payments: fp}

	resp, err := srv.ListPayments(context.Background(), &pb.ListPaymentsRequest{
		Filter: &pb.MoneyFilter{AccountIds: []string{"a1"}, From: "2026-11-17", To: "2026-11-17"},
		Limit:  5000, Cursor: "cur", Status: "recorded",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := fp.req.GetLimit(); got != 1000 {
		t.Fatalf("forwarded limit = %d, want 1000 (clamped)", got)
	}
	if got := fp.req.GetFromMs(); got != 1794873600000 {
		t.Fatalf("from_ms = %d, want 1794873600000", got)
	}
	if got := fp.req.GetToMs(); got != 1794960000000 {
		t.Fatalf("to_ms = %d, want 1794960000000 (whole day)", got)
	}
	if fp.req.GetStatus() != "recorded" || fp.req.GetCursor() != "cur" {
		t.Fatalf("forwarded req = %+v", fp.req)
	}
	if len(resp.GetPayments()) != 1 || resp.GetPayments()[0].GetAmountMinor() != 500 ||
		resp.GetNextCursor() != "next" {
		t.Fatalf("resp = %+v", resp)
	}

	if _, err := srv.ListPayments(context.Background(), &pb.ListPaymentsRequest{
		Filter: &pb.MoneyFilter{From: "2026-11-17"}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing to err = %v, want InvalidArgument", err)
	}

	fp.err = status.Error(codes.Unavailable, "down")
	if _, err := srv.ListPayments(context.Background(), &pb.ListPaymentsRequest{
		Filter: &pb.MoneyFilter{From: "2026-11-17", To: "2026-11-18"}}); status.Code(err) != codes.Unavailable {
		t.Fatalf("source error = %v, want Unavailable", err)
	}
}

func TestReportsListChargesAndLowBalance(t *testing.T) {
	fc := &fakeCharges{
		resp: &chargingpb.ListChargesResponse{Charges: []*chargingpb.Charge{{
			SessionId: "s1", AccountId: "a1", ChargedSeconds: 3,
			AmountMinor: -4, BalanceAfter: 996, LastChargedAtMs: 2000,
		}}},
		balance: &chargingpb.ListLowBalanceResponse{Accounts: []*chargingpb.BalanceRow{{
			AccountId: "a1", Kind: "prepaid", Balance: 0, CreditLimit: 0, Currency: "ZAR",
		}}},
	}
	srv := &reportsServer{charges: fc}

	resp, err := srv.ListCharges(context.Background(), &pb.ListChargesRequest{
		Filter: &pb.MoneyFilter{AccountIds: []string{"a1"}, From: "2026-11-17", To: "2026-11-18"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetCharges()) != 1 || resp.GetCharges()[0].GetAmountMinor() != -4 {
		t.Fatalf("charges resp = %+v", resp)
	}
	if fc.chargesReq.GetFromMs() == 0 || fc.chargesReq.GetToMs() == 0 {
		t.Fatalf("charges window not forwarded: %+v", fc.chargesReq)
	}

	low, err := srv.ListLowBalance(context.Background(), &pb.LowBalanceRequest{
		AccountIds: []string{"a1"}, ThresholdMinor: -5, Limit: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(low.GetAccounts()) != 1 || low.GetAccounts()[0].GetCurrency() != "ZAR" {
		t.Fatalf("low balance resp = %+v", low)
	}
	if fc.balanceReq.GetThresholdMinor() != -5 || fc.balanceReq.GetLimit() != 100 {
		t.Fatalf("low balance req = %+v", fc.balanceReq)
	}
}
```

- [ ] **Step 6: Run the test to verify it fails**

Run: `cd services/reports && go test -run 'TestReportsList' ./...`
Expected: build failure, `srv.ListPayments undefined` / `PaymentSource undefined`.

- [ ] **Step 7: Implement `grpc_money.go`**

Create `services/reports/grpc_money.go`:

```go
package main

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chargingpb "omnifoni/charging/gen/chargingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
	pb "omnifoni/reports/gen/reportspb"
)

// PaymentSource is the payments read surface used by the report handlers.
type PaymentSource interface {
	ListPayments(context.Context, *paymentspb.ListPaymentsRequest) (*paymentspb.ListPaymentsResponse, error)
}

// ChargeSource is the charging read surface used by the report handlers.
type ChargeSource interface {
	ListCharges(context.Context, *chargingpb.ListChargesRequest) (*chargingpb.ListChargesResponse, error)
	ListLowBalance(context.Context, *chargingpb.ListLowBalanceRequest) (*chargingpb.ListLowBalanceResponse, error)
}

func parseMoneyFilter(f *pb.MoneyFilter) (from, to time.Time, err error) {
	if f.GetFrom() == "" || f.GetTo() == "" {
		return time.Time{}, time.Time{}, errors.New("from and to are required")
	}
	from, err = parseBound(f.GetFrom(), false)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	to, err = parseBound(f.GetTo(), true)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if !to.After(from) {
		return time.Time{}, time.Time{}, errors.New("to must be after from")
	}
	return from, to, nil
}

// sourceError keeps InvalidArgument and hides everything else as Unavailable.
func sourceError(err error) error {
	if status.Code(err) == codes.InvalidArgument {
		return err
	}
	return status.Error(codes.Unavailable, "source unavailable")
}

func (s *reportsServer) ListPayments(ctx context.Context, req *pb.ListPaymentsRequest) (*pb.ListPaymentsResponse, error) {
	from, to, err := parseMoneyFilter(req.GetFilter())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, span := otel.Tracer("reports").Start(ctx, "reports.list_payments")
	span.SetAttributes(attribute.String("from", from.Format(time.RFC3339)),
		attribute.String("to", to.Format(time.RFC3339)))
	defer span.End()

	resp, err := s.payments.ListPayments(ctx, &paymentspb.ListPaymentsRequest{
		AccountIds: req.GetFilter().GetAccountIds(),
		FromMs:     from.UnixMilli(),
		ToMs:       to.UnixMilli(),
		Status:     req.GetStatus(),
		Limit:      int32(clampLimit(req.GetLimit())),
		Cursor:     req.GetCursor(),
	})
	if err != nil {
		return nil, sourceError(err)
	}
	out := &pb.ListPaymentsResponse{Payments: []*pb.Payment{}, NextCursor: resp.GetNextCursor()}
	for _, p := range resp.GetPayments() {
		out.Payments = append(out.Payments, &pb.Payment{
			PaymentId: p.GetPaymentId(), EventId: p.GetEventId(), AccountId: p.GetAccountId(),
			Gateway: p.GetGateway(), AmountMinor: p.GetAmountMinor(), Status: p.GetStatus(),
			CreatedAtMs: p.GetCreatedAtMs(),
		})
	}
	return out, nil
}

func (s *reportsServer) ListCharges(ctx context.Context, req *pb.ListChargesRequest) (*pb.ListChargesResponse, error) {
	from, to, err := parseMoneyFilter(req.GetFilter())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, span := otel.Tracer("reports").Start(ctx, "reports.list_charges")
	span.SetAttributes(attribute.String("from", from.Format(time.RFC3339)),
		attribute.String("to", to.Format(time.RFC3339)))
	defer span.End()

	resp, err := s.charges.ListCharges(ctx, &chargingpb.ListChargesRequest{
		AccountIds: req.GetFilter().GetAccountIds(),
		FromMs:     from.UnixMilli(),
		ToMs:       to.UnixMilli(),
		Limit:      int32(clampLimit(req.GetLimit())),
		Cursor:     req.GetCursor(),
	})
	if err != nil {
		return nil, sourceError(err)
	}
	out := &pb.ListChargesResponse{Charges: []*pb.Charge{}, NextCursor: resp.GetNextCursor()}
	for _, c := range resp.GetCharges() {
		out.Charges = append(out.Charges, &pb.Charge{
			SessionId: c.GetSessionId(), AccountId: c.GetAccountId(),
			ChargedSeconds: c.GetChargedSeconds(), AmountMinor: c.GetAmountMinor(),
			BalanceAfter: c.GetBalanceAfter(), LastChargedAtMs: c.GetLastChargedAtMs(),
		})
	}
	return out, nil
}

func (s *reportsServer) ListLowBalance(ctx context.Context, req *pb.LowBalanceRequest) (*pb.LowBalanceResponse, error) {
	ctx, span := otel.Tracer("reports").Start(ctx, "reports.list_low_balance")
	defer span.End()

	resp, err := s.charges.ListLowBalance(ctx, &chargingpb.ListLowBalanceRequest{
		AccountIds:     req.GetAccountIds(),
		ThresholdMinor: req.GetThresholdMinor(),
		Limit:          int32(clampLimit(req.GetLimit())),
		Cursor:         req.GetCursor(),
	})
	if err != nil {
		return nil, sourceError(err)
	}
	out := &pb.LowBalanceResponse{Accounts: []*pb.BalanceRow{}, NextCursor: resp.GetNextCursor()}
	for _, a := range resp.GetAccounts() {
		out.Accounts = append(out.Accounts, &pb.BalanceRow{
			AccountId: a.GetAccountId(), Kind: a.GetKind(), Balance: a.GetBalance(),
			CreditLimit: a.GetCreditLimit(), Currency: a.GetCurrency(),
		})
	}
	return out, nil
}
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cd services/reports && go vet ./... && go test -run 'TestReportsList' ./...`
Expected: `ok omnifoni/reports`.

- [ ] **Step 9: Run the whole module**

Run: `cd services/reports && go test ./...`
Expected: `ok omnifoni/reports` (money/store/ingest/grpc/http tests all pass; the store tests need ClickHouse up).

- [ ] **Step 10: Commit**

```bash
git add proto/reports/v1/reports.proto services/reports
git commit -m "feat(reports): add money report rpcs"
```

---

### Task 4: HTTP money report endpoints

**Files:**
- Modify: `services/reports/http.go`
- Modify: `services/reports/http_test.go`
- Modify: `services/reports/main.go` (route registration only; full wiring is Task 5)

**Interfaces:**
- Consumes: `reportsServer.ListPayments/ListCharges/ListLowBalance`, `Authenticator.Accounts`, `intersect`, `writeJSON`, `writeErr`, `grpcStatus`, `bearer`.
- Produces: handlers `Payments`, `Charges`, `LowBalance`; routes `GET /reports/payments`, `/reports/charges`, `/reports/low-balance`.

- [ ] **Step 1: Write the failing test**

In `services/reports/http_test.go`, change `facade` so the reports server carries fakes:

```go
func facade(t *testing.T) *httptest.Server {
	t.Helper()
	h := &httpServer{api: &reportsServer{
		store:    store,
		payments: &fakePayments{resp: &paymentspb.ListPaymentsResponse{Payments: []*paymentspb.Payment{{
			PaymentId: 7, EventId: "evt", AccountId: "a1", Gateway: "stripe",
			AmountMinor: 500, Status: "recorded", CreatedAtMs: 1000,
		}}}},
		charges: &fakeCharges{
			resp: &chargingpb.ListChargesResponse{Charges: []*chargingpb.Charge{{
				SessionId: "s1", AccountId: "a1", ChargedSeconds: 3,
				AmountMinor: -4, BalanceAfter: 996, LastChargedAtMs: 2000,
			}}},
			balance: &chargingpb.ListLowBalanceResponse{Accounts: []*chargingpb.BalanceRow{{
				AccountId: "a1", Kind: "prepaid", Balance: 0, Currency: "ZAR",
			}}},
		},
	}, auth: NewAuthenticator(fakeIdentity(t).URL)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/cdrs", h.ListCdrs)
	mux.HandleFunc("GET /reports/summary", h.Summarize)
	mux.HandleFunc("GET /reports/payments", h.Payments)
	mux.HandleFunc("GET /reports/charges", h.Charges)
	mux.HandleFunc("GET /reports/low-balance", h.LowBalance)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}
```

Add the imports `chargingpb "omnifoni/charging/gen/chargingpb"` and `paymentspb "omnifoni/payments/gen/paymentspb"` to the import block.

Append this test to `services/reports/http_test.go`:

```go
func TestHTTPMoneyReports(t *testing.T) {
	srv := facade(t)
	q := "from=2026-11-17&to=2026-11-18"

	code, body := get(t, srv.URL+"/reports/payments?"+q, "reseller")
	if code != http.StatusOK {
		t.Fatalf("payments = %d (%v)", code, body)
	}
	payments := body["payments"].([]any)
	if len(payments) != 1 || payments[0].(map[string]any)["amount_minor"].(float64) != 500 {
		t.Fatalf("payments = %v", payments)
	}

	code, body = get(t, srv.URL+"/reports/charges?"+q, "reseller")
	if code != http.StatusOK {
		t.Fatalf("charges = %d (%v)", code, body)
	}
	charges := body["charges"].([]any)
	if len(charges) != 1 || charges[0].(map[string]any)["amount_minor"].(float64) != -4 {
		t.Fatalf("charges = %v", charges)
	}

	code, body = get(t, srv.URL+"/reports/low-balance?threshold=-5", "reseller")
	if code != http.StatusOK {
		t.Fatalf("low balance = %d (%v)", code, body)
	}
	accounts := body["accounts"].([]any)
	if len(accounts) != 1 || accounts[0].(map[string]any)["account_id"] != "a1" {
		t.Fatalf("accounts = %v", accounts)
	}

	if code, _ := get(t, srv.URL+"/reports/low-balance?threshold=abc", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bad threshold = %d, want 400", code)
	}
	if code, _ := get(t, srv.URL+"/reports/payments?to=2026-11-18", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("missing from = %d, want 400", code)
	}
	if code, _ := get(t, srv.URL+"/reports/payments?"+q, ""); code != http.StatusUnauthorized {
		t.Fatalf("no token = %d, want 401", code)
	}
	if code, _ := get(t, srv.URL+"/reports/charges?"+q, "user"); code != http.StatusForbidden {
		t.Fatalf("user role = %d, want 403", code)
	}
	if code, _ := get(t, srv.URL+"/reports/payments?"+q+"&account_id=a2", "reseller"); code != http.StatusOK {
		t.Fatalf("cross-tenant account filter = %d, want 200", code)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/reports && go test -run TestHTTPMoneyReports ./...`
Expected: build failure, `h.Payments undefined`.

- [ ] **Step 3: Implement the handlers**

Append to `services/reports/http.go`:

```go
type paymentJSON struct {
	PaymentID   int64  `json:"payment_id"`
	EventID     string `json:"event_id"`
	AccountID   string `json:"account_id"`
	Gateway     string `json:"gateway"`
	AmountMinor int64  `json:"amount_minor"`
	Status      string `json:"status"`
	CreatedAtMS int64  `json:"created_at_ms"`
}

type paymentsJSON struct {
	Payments   []paymentJSON `json:"payments"`
	NextCursor string        `json:"next_cursor"`
}

type chargeJSON struct {
	SessionID       string `json:"session_id"`
	AccountID       string `json:"account_id"`
	ChargedSeconds  int64  `json:"charged_seconds"`
	AmountMinor     int64  `json:"amount_minor"`
	BalanceAfter    int64  `json:"balance_after"`
	LastChargedAtMS int64  `json:"last_charged_at_ms"`
}

type chargesJSON struct {
	Charges    []chargeJSON `json:"charges"`
	NextCursor string       `json:"next_cursor"`
}

type balanceJSON struct {
	AccountID   string `json:"account_id"`
	Kind        string `json:"kind"`
	Balance     int64  `json:"balance"`
	CreditLimit int64  `json:"credit_limit"`
	Currency    string `json:"currency"`
}

type lowBalanceJSON struct {
	Accounts   []balanceJSON `json:"accounts"`
	NextCursor string        `json:"next_cursor"`
}

func (h *httpServer) Payments(w http.ResponseWriter, r *http.Request) {
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
	if len(ids) == 0 {
		writeJSON(w, http.StatusOK, paymentsJSON{Payments: []paymentJSON{}})
		return
	}
	req := &pb.ListPaymentsRequest{
		Filter: &pb.MoneyFilter{AccountIds: ids, From: q.Get("from"), To: q.Get("to")},
		Status: q.Get("status"),
		Cursor: q.Get("cursor"),
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid limit")
			return
		}
		req.Limit = int32(n)
	}
	if requested := q["account_id"]; len(requested) > 0 {
		req.Filter.AccountIds = intersect(ids, requested)
		if len(req.Filter.AccountIds) == 0 {
			writeJSON(w, http.StatusOK, paymentsJSON{Payments: []paymentJSON{}})
			return
		}
	}

	resp, err := h.api.ListPayments(r.Context(), req)
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	out := paymentsJSON{Payments: []paymentJSON{}, NextCursor: resp.GetNextCursor()}
	for _, p := range resp.GetPayments() {
		out.Payments = append(out.Payments, paymentJSON{
			PaymentID: p.GetPaymentId(), EventID: p.GetEventId(), AccountID: p.GetAccountId(),
			Gateway: p.GetGateway(), AmountMinor: p.GetAmountMinor(), Status: p.GetStatus(),
			CreatedAtMS: p.GetCreatedAtMs(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *httpServer) Charges(w http.ResponseWriter, r *http.Request) {
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
	if len(ids) == 0 {
		writeJSON(w, http.StatusOK, chargesJSON{Charges: []chargeJSON{}})
		return
	}
	req := &pb.ListChargesRequest{
		Filter: &pb.MoneyFilter{AccountIds: ids, From: q.Get("from"), To: q.Get("to")},
		Cursor: q.Get("cursor"),
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid limit")
			return
		}
		req.Limit = int32(n)
	}
	if requested := q["account_id"]; len(requested) > 0 {
		req.Filter.AccountIds = intersect(ids, requested)
		if len(req.Filter.AccountIds) == 0 {
			writeJSON(w, http.StatusOK, chargesJSON{Charges: []chargeJSON{}})
			return
		}
	}

	resp, err := h.api.ListCharges(r.Context(), req)
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	out := chargesJSON{Charges: []chargeJSON{}, NextCursor: resp.GetNextCursor()}
	for _, c := range resp.GetCharges() {
		out.Charges = append(out.Charges, chargeJSON{
			SessionID: c.GetSessionId(), AccountID: c.GetAccountId(),
			ChargedSeconds: c.GetChargedSeconds(), AmountMinor: c.GetAmountMinor(),
			BalanceAfter: c.GetBalanceAfter(), LastChargedAtMS: c.GetLastChargedAtMs(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *httpServer) LowBalance(w http.ResponseWriter, r *http.Request) {
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
	if len(ids) == 0 {
		writeJSON(w, http.StatusOK, lowBalanceJSON{Accounts: []balanceJSON{}})
		return
	}
	req := &pb.LowBalanceRequest{AccountIds: ids, Cursor: q.Get("cursor")}
	if v := q.Get("threshold"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid threshold")
			return
		}
		req.ThresholdMinor = n
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid limit")
			return
		}
		req.Limit = int32(n)
	}
	if requested := q["account_id"]; len(requested) > 0 {
		req.AccountIds = intersect(ids, requested)
		if len(req.AccountIds) == 0 {
			writeJSON(w, http.StatusOK, lowBalanceJSON{Accounts: []balanceJSON{}})
			return
		}
	}

	resp, err := h.api.ListLowBalance(r.Context(), req)
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	out := lowBalanceJSON{Accounts: []balanceJSON{}, NextCursor: resp.GetNextCursor()}
	for _, a := range resp.GetAccounts() {
		out.Accounts = append(out.Accounts, balanceJSON{
			AccountID: a.GetAccountId(), Kind: a.GetKind(), Balance: a.GetBalance(),
			CreditLimit: a.GetCreditLimit(), Currency: a.GetCurrency(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}
```

In `services/reports/main.go`, register the routes next to the existing ones:

```go
	mux.HandleFunc("GET /reports/payments", h.Payments)
	mux.HandleFunc("GET /reports/charges", h.Charges)
	mux.HandleFunc("GET /reports/low-balance", h.LowBalance)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd services/reports && go vet ./... && go test -run 'TestHTTP' ./...`
Expected: `ok omnifoni/reports`.

- [ ] **Step 5: Run the whole module**

Run: `cd services/reports && go test ./...`
Expected: `ok omnifoni/reports`.

- [ ] **Step 6: Commit**

```bash
git add services/reports/http.go services/reports/http_test.go services/reports/main.go
git commit -m "feat(reports): serve money reports over http"
```

---

### Task 5: Wire sources, compose and smoke

**Files:**
- Modify: `services/reports/main.go`
- Modify: `services/reports/Dockerfile`
- Modify: `services/reports/cmd/smokereports/main.go`
- Modify: `docker-compose.yml`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: report RPCs from Task 3; handlers/routes from Task 4.
- Produces: deployed reports service dialing `payments:50060` and `charging:50051`; smoke probe covering the three money reports.

- [ ] **Step 1: Wire the clients in `main.go`**

Add imports:

```go
	chargingpb "omnifoni/charging/gen/chargingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
```

Add env reads after `httpAddr`:

```go
	payAddr := env("REPORTS_PAYMENTS_ADDR", "payments:50060")
	chgAddr := env("REPORTS_CHARGING_ADDR", "charging:50051")
```

After the catalogue client block, add:

```go
	payConn, err := grpc.NewClient(payAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("payments client", "err", err)
		os.Exit(1)
	}
	chgConn, err := grpc.NewClient(chgAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("charging client", "err", err)
		os.Exit(1)
	}
```

Replace:

```go
	api := &reportsServer{store: store}
```

with:

```go
	api := &reportsServer{
		store:    store,
		payments: paymentspb.NewPaymentsClient(payConn),
		charges:  chargingpb.NewChargingClient(chgConn),
	}
```

Update the HTTP-up log line to include the new addresses:

```go
	slog.Info("reports HTTP up", "addr", httpAddr, "identity", identity,
		"payments", payAddr, "charging", chgAddr)
```

- [ ] **Step 2: Extend the Dockerfile COPY list**

In `services/reports/Dockerfile`, after `COPY mediation/ ./mediation/` add:

```dockerfile
COPY payments/ ./payments/
COPY charging/ ./charging/
```

- [ ] **Step 3: Extend the smoke probe**

In `services/reports/cmd/smokereports/main.go`, replace the final block:

```go
	resp, err := pb.NewReportsClient(conn).Summarize(ctx, &pb.SummarizeRequest{
		Filter:  &pb.CdrFilter{From: from.Format("2006-01-02"), To: to.Format("2006-01-02")},
		GroupBy: pb.GroupBy_DAY,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "summarize:", err)
		os.Exit(1)
	}
	fmt.Printf("ok: reports summary rows=%d\n", len(resp.GetRows()))
```

with:

```go
	client := pb.NewReportsClient(conn)
	resp, err := client.Summarize(ctx, &pb.SummarizeRequest{
		Filter:  &pb.CdrFilter{From: from.Format("2006-01-02"), To: to.Format("2006-01-02")},
		GroupBy: pb.GroupBy_DAY,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "summarize:", err)
		os.Exit(1)
	}
	money := &pb.MoneyFilter{From: from.Format("2006-01-02"), To: to.Format("2006-01-02")}
	if _, err := client.ListPayments(ctx, &pb.ListPaymentsRequest{Filter: money}); err != nil {
		fmt.Fprintln(os.Stderr, "list payments:", err)
		os.Exit(1)
	}
	if _, err := client.ListCharges(ctx, &pb.ListChargesRequest{Filter: money}); err != nil {
		fmt.Fprintln(os.Stderr, "list charges:", err)
		os.Exit(1)
	}
	if _, err := client.ListLowBalance(ctx, &pb.LowBalanceRequest{}); err != nil {
		fmt.Fprintln(os.Stderr, "list low balance:", err)
		os.Exit(1)
	}
	fmt.Printf("ok: reports summary rows=%d money=ok\n", len(resp.GetRows()))
```

- [ ] **Step 4: Add the compose environment and dependencies**

In the `reports` service in `docker-compose.yml`, add under the existing environment keys:

```yaml
      REPORTS_PAYMENTS_ADDR: payments:50060
      REPORTS_CHARGING_ADDR: charging:50051
```

Add to `depends_on`:

```yaml
      payments:
        condition: service_started
      charging:
        condition: service_started
```

- [ ] **Step 5: Update `AGENTS.md`**

Append to the P5 slice-one bullet (same bullet, new sentences):

```markdown
 P5 slice two adds money reports: payments `ListPayments` (recorded+void with `status`, epoch-ms window), charging `ListCharges` (one row per session, summed negative ledger deltas, latest balance) and `ListLowBalance` (`balance <= threshold_minor`, both kinds), all mirrored on the reports gRPC service and served by the HTTP facade at `/reports/payments`, `/reports/charges`, `/reports/low-balance` with the same bearer/tenant scoping. Envs: `REPORTS_PAYMENTS_ADDR` (payments:50060), `REPORTS_CHARGING_ADDR` (charging:50051).
```

- [ ] **Step 6: Build and run**

Run:

```bash
cd /root/Omni-Foní
docker compose build payments charging reports
docker compose up -d payments charging reports
sleep 3
docker inspect --format '{{.State.Status}}' omni-fon-reports-1
```

Expected: `running`; `docker logs omni-fon-reports-1` shows `reports HTTP up` with the payments/charging addresses.

- [ ] **Step 7: Verify the smoke probe**

Run: `make smoke`
Expected: `smoke: PASS` including `ok: reports summary rows=N money=ok` and `ok: traces for reports`.

- [ ] **Step 8: Commit**

```bash
git add services/reports docker-compose.yml AGENTS.md
git commit -m "feat(reports): wire money report sources and smoke"
```

---

### Task 6: Live verification and slice close

**Files:**
- Modify: `AGENTS.md` only if verification corrects a claim (otherwise no file change).

**Interfaces:**
- Consumes: everything above; deployed services; dev seeds.
- Produces: verified acceptance evidence for the slice.

- [ ] **Step 1: Seed live data**

Run:

```bash
cd /root/Omni-Foní
docker compose exec -T postgres psql -U postgres -d charging -c "INSERT INTO accounts (account_id, kind, balance, credit_limit) VALUES ('acc-money-live','prepaid',100000,0) ON CONFLICT (account_id) DO UPDATE SET balance=100000"
docker compose exec -T postgres psql -U postgres -d payments -c "INSERT INTO payments (event_id, account_id, gateway, amount_minor, status, created_at) VALUES ('evt-money-live','acc-money-live','stripe',2500,'recorded',now()) ON CONFLICT (event_id) DO NOTHING"
docker compose exec -T postgres psql -U postgres -d charging -c "INSERT INTO sessions (session_id, account_id, rate_per_sec, reserved_seconds, charged_seconds, released, created_at) VALUES ('00000000-0000-0000-0000-0000000000aa','acc-money-live',0.5,10,3,true,now()-interval '5 minutes') ON CONFLICT (session_id) DO NOTHING"
docker compose exec -T postgres psql -U postgres -d charging -c "INSERT INTO ledger (session_id, account_id, amount_minor, balance_after, created_at) VALUES ('00000000-0000-0000-0000-0000000000aa','acc-money-live',-1,99999,now()-interval '4 minutes'), ('00000000-0000-0000-0000-0000000000aa','acc-money-live',-1,99998,now()-interval '3 minutes'), ('00000000-0000-0000-0000-0000000000aa','acc-money-live',-2,99996,now()-interval '2 minutes')"
```

- [ ] **Step 2: Query the three reports over gRPC**

Run:

```bash
from=$(date -u +%Y-%m-01); to=$(date -u -d '+1 month' +%Y-%m-01)
/root/go/bin/grpcurl -plaintext -d "{\"filter\":{\"from\":\"$from\",\"to\":\"$to\"}}" localhost:50061 reports.v1.Reports/ListPayments
/root/go/bin/grpcurl -plaintext -d "{\"filter\":{\"from\":\"$from\",\"to\":\"$to\"}}" localhost:50061 reports.v1.Reports/ListCharges
/root/go/bin/grpcurl -plaintext -d '{"threshold_minor":0}' localhost:50061 reports.v1.Reports/ListLowBalance
```

Expected: payments contains `evt-money-live` with `amountMinor 2500`; charges contains session `...00aa` with `chargedSeconds 3`, `amountMinor -4`, `balanceAfter 99996`; low balance contains `acc-money-live` with `balance 99996`.

- [ ] **Step 3: Query the HTTP facade**

Reuse the live identity session if present, else create it:

```bash
cd /root/Omni-Foní
tid=$(docker compose exec -T postgres psql -U postgres -d identity -qAt -c "SELECT tenant_id FROM tenants WHERE name='reports-live'")
uid=$(docker compose exec -T postgres psql -U postgres -d identity -qAt -c "INSERT INTO users(tenant_id,email,password_hash,role) VALUES ('$tid','money-admin@live.test','x','admin') ON CONFLICT (email) DO UPDATE SET role='admin' RETURNING user_id")
docker compose exec -T postgres psql -U postgres -d identity -qAt -c "INSERT INTO sessions(token,user_id,expires_at) VALUES ('tok-money-admin-live','$uid',now()+interval '2 hours') ON CONFLICT (token) DO UPDATE SET expires_at=now()+interval '2 hours'"
docker compose exec -T postgres psql -U postgres -d identity -qAt -c "INSERT INTO accounts(account_id,tenant_id,created_by) VALUES ('acc-money-live','$tid','$uid') ON CONFLICT (account_id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id"
from=$(date -u +%Y-%m-01); to=$(date -u -d '+1 month' +%Y-%m-01)
curl -s -w ' [%{http_code}]\n' -H 'Authorization: Bearer tok-money-admin-live' "http://localhost:50062/reports/payments?from=$from&to=$to&tenant_id=$tid"
curl -s -w ' [%{http_code}]\n' -H 'Authorization: Bearer tok-money-admin-live' "http://localhost:50062/reports/charges?from=$from&to=$to&tenant_id=$tid"
curl -s -w ' [%{http_code}]\n' -H 'Authorization: Bearer tok-money-admin-live' "http://localhost:50062/reports/low-balance?threshold=100000&tenant_id=$tid"
curl -s -w ' [%{http_code}]\n' -H 'Authorization: Bearer tok-money-admin-live' "http://localhost:50062/reports/payments?from=$from&to=$to"
```

Expected: three `200` bodies with the live rows, then `400` for the admin request without `tenant_id`.

- [ ] **Step 4: Run the full gate**

Run:

```bash
cd /root/Omni-Foní
make test > /tmp/make-test-money.log 2>&1; echo "exit=$?"
grep -cE '^ok' /tmp/make-test-money.log
grep -E '^FAIL' /tmp/make-test-money.log | head
```

Expected: `exit=0`, `13` modules `ok` (reports, payments, charging included), no `FAIL` lines.

Then run: `make smoke`
Expected: `smoke: PASS`.

- [ ] **Step 5: Commit any doc corrections**

If Step 2–4 contradicted an `AGENTS.md` claim, fix it and commit:

```bash
git add AGENTS.md
git commit -m "docs(reports): close the money reports slice"
```

If no correction was needed, skip the commit; the slice is closed by the gate output.
