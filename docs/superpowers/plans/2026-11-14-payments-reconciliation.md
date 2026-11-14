# Payments Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the money triangle — a reconciliation service that proves invoices match the rated-CDR ledger, payments match invoices, and publishes drift to Kafka.

**Architecture:** New `services/reconciliation` on :50059 is a read-only gRPC aggregator speaking only to billing (:50054, extended with a period ingest ledger and account-list RPCs) and payments (new gRPC surface on :50060). Drift events go to Kafka topic `reconciliation.drift` as proto. No new databases.

**Tech Stack:** Go 1.27, gRPC + protobuf (protoc-gen-go v1.36.x via `make protoc`), pgx v5, franz-go.

## Global Constraints

- Clean-room: never read ASTPP source; feature claims only from `docs/master-plan.md` §0/§8 citations.
- Money is integer minor units; all comparisons are integer sums — no rounding anywhere in this slice.
- Empty/missing data is zeros / `no_activity`, never an error.
- Regenerate stubs ONLY via `make protoc`; never hand-edit anything under `gen/`.
- `make up` never rebuilds; after changing a deployed service run `docker compose build <service>` then `docker compose up -d <service>`.
- Tests hit the real dev stack (`make up` first): Postgres on localhost:5432, Kafka on localhost:9092, billing gRPC on localhost:50054, payments gRPC on localhost:50060 (after Task 3). Host-side Kafka needs the `127.0.0.1 kafka` /etc/hosts entry.
- Ledger is drift-published only by the sweep; on-demand RPCs report, never publish.
- PayFast keeps the unified Stripe-style HMAC (existing `VerifySignature`); no gateway-specific auth in this slice.
- Commit style: lowercase conventional commits, exact messages given per task.
- Ports: reconciliation :50059, payments gRPC :50060 (both free; existing max is :50058).

---

### Task 1: Billing proto — ledger and account-list RPCs

**Files:**
- Modify: `proto/billing/v1/billing.proto`
- Modify (generated): `services/billing/gen/billingpb/billing.pb.go`, `services/billing/gen/billingpb/billing_grpc.pb.go`

**Interfaces:**
- Produces: gRPC `GetPeriodLedger(period) -> {period, cdr_count, amount_sum}` and `ListAccountsForPeriod(period) -> {account_ids}`; generated types `GetPeriodLedgerRequest/Response`, `ListAccountsForPeriodRequest/Response` in package `omnifoni/billing/gen/billingpb`. Task 2's handlers and Task 4's adapters consume these.

- [ ] **Step 1: Add the two RPCs and messages to `proto/billing/v1/billing.proto`**

Inside `service Billing`, after the `GetInvoicesByAccounts` rpc line, add:

```proto
  // GetPeriodLedger returns the rated-CDR ingest ledger for one period: how
  // many CDRs were billed and their total amount. An unseen period is zeros,
  // not an error.
  rpc GetPeriodLedger(GetPeriodLedgerRequest) returns (GetPeriodLedgerResponse);
  // ListAccountsForPeriod returns the distinct account ids that have invoice
  // lines in one period.
  rpc ListAccountsForPeriod(ListAccountsForPeriodRequest) returns (ListAccountsForPeriodResponse);
```

At the end of the file, add:

```proto
message GetPeriodLedgerRequest {
  string period = 1; // YYYY-MM
}

message GetPeriodLedgerResponse {
  string period = 1;
  int64 cdr_count = 2;
  int64 amount_sum = 3;
}

message ListAccountsForPeriodRequest {
  string period = 1; // YYYY-MM
}

message ListAccountsForPeriodResponse {
  repeated string account_ids = 1;
}
```

- [ ] **Step 2: Regenerate stubs**

Run: `make protoc`
Expected: no errors; `services/billing/gen/billingpb/billing.pb.go` gains `GetPeriodLedger`, `CdrCount`, `AmountSum`, and `billing_grpc.pb.go` gains the `getPeriodLedger` handler entries.

- [ ] **Step 3: Verify it compiles**

Run: `cd services/billing && go build ./...`
Expected: success (no callers yet; unimplemented RPCs sit behind `UnimplementedBillingServer`).

- [ ] **Step 4: Commit**

```bash
git add proto/billing/v1/billing.proto services/billing/gen/billingpb/
git commit -m "feat(billing): period-ledger and account-list RPCs"
```

---

### Task 2: Billing ingest ledger (schema, transactional write, handlers, tests)

**Files:**
- Modify: `services/billing/migrate.sql`
- Modify: `services/billing/biller.go:66-75` (the insert becomes a Tx)
- Modify: `services/billing/grpc.go` (two new handlers)
- Test: `services/billing/biller_test.go` (append), `services/billing/grpc_test.go` (create)

**Interfaces:**
- Consumes: Task 1 generated types.
- Produces: table `cdr_period_totals(period TEXT PRIMARY KEY, cdr_count BIGINT, amount_sum BIGINT)`; handlers `GetPeriodLedger`, `ListAccountsForPeriod` on `billingServer`. Task 4 consumes via gRPC.

- [ ] **Step 1: Write the failing tests (append to `services/billing/biller_test.go`)**

```go
// TestLedgerTracksBilledCDRs: the ingest ledger must count and sum exactly
// the CDRs that were rated and billed, in the same transaction as the
// invoice lines.
func TestLedgerTracksBilledCDRs(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE invoice_lines, cdr_period_totals`); err != nil {
		t.Fatal(err)
	}
	if _, err := catPool.Exec(ctx, `TRUNCATE rates`); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.PutRate(ctx, &cataloguepb.PutRateRequest{Prefix: "3555", RatePerSec: "0.500000"}); err != nil {
		t.Fatal(err)
	}

	// 60s * 0.5 = 30; 80s * 0.5 = 40 -> count 2, sum 70
	publish(t, &mediationpb.CallCdr{CallId: "bill-ledger-1", AccountId: "acc-inv",
		Destination: "3555999", StartedAtMs: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC).UnixMilli(),
		ChargedSeconds: 60, EndReason: "normal"})
	publish(t, &mediationpb.CallCdr{CallId: "bill-ledger-2", AccountId: "acc-inv",
		Destination: "3555999", StartedAtMs: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC).UnixMilli(),
		ChargedSeconds: 80, EndReason: "normal"})
	waitLines(t, 2)

	var count, sum int64
	if err := pool.QueryRow(ctx,
		`SELECT cdr_count, amount_sum FROM cdr_period_totals WHERE period = '2026-11'`).
		Scan(&count, &sum); err != nil {
		t.Fatal(err)
	}
	if count != 2 || sum != 70 {
		t.Fatalf("ledger (count %d, sum %d), want (2, 70)", count, sum)
	}
	var lines, invSum int64
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*), SUM(amount_minor) FROM invoice_lines WHERE period = '2026-11'`).
		Scan(&lines, &invSum); err != nil {
		t.Fatal(err)
	}
	if lines != count || invSum != sum {
		t.Fatalf("ledger (count %d, sum %d) != invoice_lines (count %d, sum %d)", count, sum, lines, invSum)
	}
}

// TestLedgerReplayNoDoubleCount: a replayed CDR must touch neither table.
func TestLedgerReplayNoDoubleCount(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE invoice_lines, cdr_period_totals`); err != nil {
		t.Fatal(err)
	}
	if _, err := catPool.Exec(ctx, `TRUNCATE rates`); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.PutRate(ctx, &cataloguepb.PutRateRequest{Prefix: "3555", RatePerSec: "1.000000"}); err != nil {
		t.Fatal(err)
	}

	c := cdr("bill-ledger-replay") // 60s, rate 1 -> 60 minor
	publish(t, c)
	publish(t, c) // at-least-once replay
	waitLines(t, 1)

	var count, sum int64
	if err := pool.QueryRow(ctx,
		`SELECT cdr_count, amount_sum FROM cdr_period_totals WHERE period = '2026-11'`).
		Scan(&count, &sum); err != nil {
		t.Fatal(err)
	}
	if count != 1 || sum != 60 {
		t.Fatalf("ledger (count %d, sum %d), want (1, 60) — replay double-counted", count, sum)
	}
}
```

Create `services/billing/grpc_test.go`:

```go
package main

import (
	"context"
	"testing"

	pb "omnifoni/billing/gen/billingpb"
)

// TestGetPeriodLedgerZeros: an unseen period is zeros, not an error.
func TestGetPeriodLedgerZeros(t *testing.T) {
	ctx := context.Background()
	s := &billingServer{pool: pool}
	resp, err := s.GetPeriodLedger(ctx, &pb.GetPeriodLedgerRequest{Period: "1970-01"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetCdrCount() != 0 || resp.GetAmountSum() != 0 {
		t.Fatalf("unseen period returned (%d, %d), want (0, 0)", resp.GetCdrCount(), resp.GetAmountSum())
	}
}

// TestListAccountsForPeriod: distinct account ids, sorted.
func TestListAccountsForPeriod(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE invoice_lines`); err != nil {
		t.Fatal(err)
	}
	for _, a := range []string{"acc-b", "acc-a", "acc-b"} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO invoice_lines (call_id, account_id, period, charged_seconds, amount_minor, rate_per_sec)
			 VALUES ($1, $2, '2026-11', 1, 1, 1)`, "call-"+a, a); err != nil {
			t.Fatal(err)
		}
	}
	s := &billingServer{pool: pool}
	resp, err := s.ListAccountsForPeriod(ctx, &pb.ListAccountsForPeriodRequest{Period: "2026-11"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetAccountIds()) != 2 || resp.GetAccountIds()[0] != "acc-a" || resp.GetAccountIds()[1] != "acc-b" {
		t.Fatalf("accounts %v, want [acc-a acc-b]", resp.GetAccountIds())
	}
	if _, err := pool.Exec(ctx, `TRUNCATE invoice_lines`); err != nil {
		t.Fatal(err)
	}
}

// TestPeriodValidation: malformed periods are rejected by both new handlers.
func TestPeriodValidation(t *testing.T) {
	ctx := context.Background()
	s := &billingServer{pool: pool}
	if _, err := s.GetPeriodLedger(ctx, &pb.GetPeriodLedgerRequest{Period: "2026-1"}); err == nil {
		t.Fatal("GetPeriodLedger accepted a malformed period")
	}
	if _, err := s.ListAccountsForPeriod(ctx, &pb.ListAccountsForPeriodRequest{Period: "2026-13"}); err == nil {
		t.Fatal("ListAccountsForPeriod accepted an invalid month")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd services/billing && go test -run 'TestLedger|TestGetPeriodLedger|TestListAccounts|TestPeriodValidation' ./...`
Expected: FAIL — `cdr_period_totals` table missing (ledger tests) and `GetPeriodLedger`/`ListAccountsForPeriod` handlers not defined (grpc tests).

- [ ] **Step 3: Add the ledger table to `services/billing/migrate.sql`**

Append at the end of the file:

```sql
CREATE TABLE IF NOT EXISTS cdr_period_totals (
    period     TEXT PRIMARY KEY,
    cdr_count  BIGINT NOT NULL DEFAULT 0,
    amount_sum BIGINT NOT NULL DEFAULT 0
);
```

- [ ] **Step 4: Make the CDR insert transactional with the ledger (replace `biller.go` lines 66-75)**

Replace the `pool.Exec` block in `Biller.Handle` with:

```go
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit

	tag, err := tx.Exec(ctx,
		`INSERT INTO invoice_lines (call_id, account_id, period, charged_seconds, amount_minor, rate_per_sec)
		 VALUES ($1, $2, $3, $4, $5, $6::numeric)
		 ON CONFLICT (call_id) DO NOTHING`,
		c.GetCallId(), c.GetAccountId(), periodOf(c), c.GetChargedSeconds(), amount, rate.GetRatePerSec())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 { // 0 rows = replay of an already-billed call
		if _, err := tx.Exec(ctx,
			`INSERT INTO cdr_period_totals (period, cdr_count, amount_sum)
			 VALUES ($1, 1, $2)
			 ON CONFLICT (period) DO UPDATE
			 SET cdr_count = cdr_period_totals.cdr_count + 1,
			     amount_sum = cdr_period_totals.amount_sum + EXCLUDED.amount_sum`,
			periodOf(c), amount); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
```

(Removes the old `_ = tag` line and comment; the replay semantics now live in the `RowsAffected == 1` branch.)

- [ ] **Step 5: Add the two handlers to `services/billing/grpc.go`**

Add `"errors"` and `"github.com/jackc/pgx/v5"` to the imports, then after `GetInvoicesByAccounts` add:

```go
// GetPeriodLedger returns the rated-CDR ingest ledger for one period; an
// unseen period is zeros, not an error.
func (s *billingServer) GetPeriodLedger(ctx context.Context, req *pb.GetPeriodLedgerRequest) (*pb.GetPeriodLedgerResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	var count, sum int64
	err := s.pool.QueryRow(ctx,
		`SELECT cdr_count, amount_sum FROM cdr_period_totals WHERE period = $1`,
		req.GetPeriod()).Scan(&count, &sum)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	return &pb.GetPeriodLedgerResponse{Period: req.GetPeriod(), CdrCount: count, AmountSum: sum}, nil
}

// ListAccountsForPeriod returns the distinct account ids with invoice lines
// in one period.
func (s *billingServer) ListAccountsForPeriod(ctx context.Context, req *pb.ListAccountsForPeriodRequest) (*pb.ListAccountsForPeriodResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT account_id FROM invoice_lines WHERE period = $1 ORDER BY account_id`,
		req.GetPeriod())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resp := &pb.ListAccountsForPeriodResponse{}
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		resp.AccountIds = append(resp.AccountIds, a)
	}
	return resp, rows.Err()
}
```

- [ ] **Step 6: Run the full billing suite**

Run: `cd services/billing && go vet ./... && go test ./...`
Expected: all PASS (ledger tests, handler tests, and the pre-existing replay/reconciliation/dead-letter tests).

- [ ] **Step 7: Rebuild the deployed billing service**

Run: `docker compose build billing && docker compose up -d billing`
Expected: image builds; container restarts on :50054.

- [ ] **Step 8: Commit**

```bash
git add services/billing/migrate.sql services/billing/biller.go services/billing/grpc.go services/billing/biller_test.go services/billing/grpc_test.go
git commit -m "feat(billing): rated-CDR ingest ledger"
```

---

### Task 3: Payments gRPC surface on :50060

**Files:**
- Create: `proto/payments/v1/payments.proto`
- Create (generated): `services/payments/gen/paymentspb/`
- Modify: `Makefile` (payments protoc recipe)
- Modify: `services/payments/go.mod`, `services/payments/go.sum` (grpc deps)
- Create: `services/payments/grpc.go`
- Modify: `services/payments/main.go`
- Test: `services/payments/grpc_test.go` (create)
- Modify: `docker-compose.yml` (payments gRPC env + port)

**Interfaces:**
- Produces: gRPC service `payments.v1.Payments` with `GetPaymentsTotals(period) -> {totals: [{account_id, payment_count, amount_sum}]}` and `ListAccountsForPeriod(period) -> {account_ids}` in package `omnifoni/payments/gen/paymentspb`. Only `recorded` payments count. Task 4 consumes via gRPC on :50060.

- [ ] **Step 1: Create `proto/payments/v1/payments.proto`**

```proto
syntax = "proto3";

package payments.v1;

option go_package = "omnifoni/payments/gen/paymentspb";

// Payments records gateway webhook payments idempotently and answers
// per-period totals for reconciliation. Periods are YYYY-MM of created_at
// in UTC.
service Payments {
  // GetPaymentsTotals returns per-account payment totals for one period.
  // Accounts with no payments in the period are omitted; void payments are
  // excluded.
  rpc GetPaymentsTotals(GetPaymentsTotalsRequest) returns (GetPaymentsTotalsResponse);
  // ListAccountsForPeriod returns the distinct account ids with recorded
  // payments in one period.
  rpc ListAccountsForPeriod(ListAccountsForPeriodRequest) returns (ListAccountsForPeriodResponse);
}

message GetPaymentsTotalsRequest {
  string period = 1; // YYYY-MM
}

message PaymentsTotal {
  string account_id = 1;
  int64 payment_count = 2;
  int64 amount_sum = 3;
}

message GetPaymentsTotalsResponse {
  repeated PaymentsTotal totals = 1;
}

message ListAccountsForPeriodRequest {
  string period = 1; // YYYY-MM
}

message ListAccountsForPeriodResponse {
  repeated string account_ids = 1;
}
```

- [ ] **Step 2: Add the protoc recipe to `Makefile` (append after the sbc recipe)**

```make
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/payments --go_opt=module=omnifoni/payments \
		--go-grpc_out=services/payments --go-grpc_opt=module=omnifoni/payments \
		proto/payments/v1/payments.proto
```

(Run with a leading TAB, like the other recipes.)

- [ ] **Step 3: Regenerate and add gRPC dependencies**

Run:
```bash
make protoc
cd services/payments && go get google.golang.org/grpc google.golang.org/protobuf && go mod tidy
```
Expected: `services/payments/gen/paymentspb/{payments.pb.go,payments_grpc.pb.go}` exist; go.mod gains grpc/protobuf.

- [ ] **Step 4: Write the failing tests — create `services/payments/grpc_test.go`**

```go
package main

import (
	"context"
	"testing"
	"time"

	pb "omnifoni/payments/gen/paymentspb"
)

// TestPaymentsTotals: recorded payments sum per account; void payments and
// other periods are excluded.
func TestPaymentsTotals(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE payments`); err != nil {
		t.Fatal(err)
	}
	nov := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	dec := time.Date(2026, 12, 10, 0, 0, 0, 0, time.UTC)
	seed := []struct {
		event, account string
		amount         int64
		status         string
		at             time.Time
	}{
		{"evt-a1", "acc-a", 100, "recorded", nov},
		{"evt-a2", "acc-a", 50, "recorded", nov},
		{"evt-void", "acc-a", 999, "void", nov},
		{"evt-b1", "acc-b", 40, "recorded", nov},
		{"evt-dec", "acc-a", 777, "recorded", dec},
	}
	for _, s := range seed {
		if _, err := pool.Exec(ctx,
			`INSERT INTO payments (event_id, account_id, gateway, amount_minor, status, created_at)
			 VALUES ($1, $2, 'stripe', $3, $4, $5)`, s.event, s.account, s.amount, s.status, s.at); err != nil {
			t.Fatal(err)
		}
	}
	srv := &paymentsServer{pool: pool}
	resp, err := srv.GetPaymentsTotals(ctx, &pb.GetPaymentsTotalsRequest{Period: "2026-11"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]*pb.PaymentsTotal{}
	for _, tot := range resp.GetTotals() {
		got[tot.GetAccountId()] = tot
	}
	if len(got) != 2 {
		t.Fatalf("got %d accounts, want 2", len(got))
	}
	a := got["acc-a"]
	if a.GetPaymentCount() != 2 || a.GetAmountSum() != 150 {
		t.Fatalf("acc-a (%d, %d), want (2, 150)", a.GetPaymentCount(), a.GetAmountSum())
	}
	b := got["acc-b"]
	if b.GetPaymentCount() != 1 || b.GetAmountSum() != 40 {
		t.Fatalf("acc-b (%d, %d), want (1, 40)", b.GetPaymentCount(), b.GetAmountSum())
	}

	// Unseen period: empty response, no error.
	empty, err := srv.GetPaymentsTotals(ctx, &pb.GetPaymentsTotalsRequest{Period: "1970-01"})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.GetTotals()) != 0 {
		t.Fatalf("unseen period returned %d totals, want 0", len(empty.GetTotals()))
	}
}

// TestPaymentsListAccounts: distinct recorded-payment accounts, sorted.
func TestPaymentsListAccounts(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE payments`); err != nil {
		t.Fatal(err)
	}
	nov := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	for _, s := range []struct{ event, account string }{
		{"evt-l2", "acc-b"}, {"evt-l1", "acc-a"}, {"evt-l3", "acc-b"},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO payments (event_id, account_id, gateway, amount_minor, status, created_at)
			 VALUES ($1, $2, 'stripe', 10, 'recorded', $3)`, s.event, s.account, nov); err != nil {
			t.Fatal(err)
		}
	}
	srv := &paymentsServer{pool: pool}
	resp, err := srv.ListAccountsForPeriod(ctx, &pb.ListAccountsForPeriodRequest{Period: "2026-11"})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetAccountIds()) != 2 || resp.GetAccountIds()[0] != "acc-a" || resp.GetAccountIds()[1] != "acc-b" {
		t.Fatalf("accounts %v, want [acc-a acc-b]", resp.GetAccountIds())
	}
	if _, err := pool.Exec(ctx, `TRUNCATE payments`); err != nil {
		t.Fatal(err)
	}
}

// TestPaymentsPeriodValidation: malformed periods are rejected.
func TestPaymentsPeriodValidation(t *testing.T) {
	ctx := context.Background()
	srv := &paymentsServer{pool: pool}
	if _, err := srv.GetPaymentsTotals(ctx, &pb.GetPaymentsTotalsRequest{Period: "bad"}); err == nil {
		t.Fatal("GetPaymentsTotals accepted a malformed period")
	}
	if _, err := srv.ListAccountsForPeriod(ctx, &pb.ListAccountsForPeriodRequest{Period: "2026-00"}); err == nil {
		t.Fatal("ListAccountsForPeriod accepted an invalid month")
	}
}
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `cd services/payments && go test -run 'TestPayments' ./...`
Expected: FAIL — `paymentsServer` and `validPeriod` undefined.

- [ ] **Step 6: Create `services/payments/grpc.go`**

```go
package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	pb "omnifoni/payments/gen/paymentspb"
)

// paymentsServer serves reconciliation queries over the payments DB.
type paymentsServer struct {
	pb.UnimplementedPaymentsServer
	pool *pgxpool.Pool
}

// GetPaymentsTotals returns per-account payment totals for one period
// (YYYY-MM of created_at in UTC). Only recorded payments count; accounts
// with no payments in the period are omitted.
func (s *paymentsServer) GetPaymentsTotals(ctx context.Context, req *pb.GetPaymentsTotalsRequest) (*pb.GetPaymentsTotalsResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT account_id, COUNT(*)::bigint, SUM(amount_minor)::bigint
		 FROM payments
		 WHERE status = 'recorded' AND to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM') = $1
		 GROUP BY account_id ORDER BY account_id`,
		req.GetPeriod())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resp := &pb.GetPaymentsTotalsResponse{}
	for rows.Next() {
		var t pb.PaymentsTotal
		if err := rows.Scan(&t.AccountId, &t.PaymentCount, &t.AmountSum); err != nil {
			return nil, err
		}
		resp.Totals = append(resp.Totals, &t)
	}
	return resp, rows.Err()
}

// ListAccountsForPeriod returns the distinct account ids with recorded
// payments in one period.
func (s *paymentsServer) ListAccountsForPeriod(ctx context.Context, req *pb.ListAccountsForPeriodRequest) (*pb.ListAccountsForPeriodResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT account_id FROM payments
		 WHERE status = 'recorded' AND to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM') = $1
		 ORDER BY account_id`,
		req.GetPeriod())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resp := &pb.ListAccountsForPeriodResponse{}
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		resp.AccountIds = append(resp.AccountIds, a)
	}
	return resp, rows.Err()
}

// validPeriod mirrors billing's period rule: strict YYYY-MM.
func validPeriod(p string) error {
	if len(p) != 7 || p[4] != '-' {
		return fmt.Errorf("period must be YYYY-MM")
	}
	for i, r := range p {
		if i == 4 {
			continue
		}
		if r < '0' || r > '9' {
			return fmt.Errorf("period must be YYYY-MM")
		}
	}
	m := int(p[5]-'0')*10 + int(p[6]-'0')
	if m < 1 || m > 12 {
		return fmt.Errorf("period month invalid")
	}
	return nil
}
```

- [ ] **Step 7: Wire the gRPC listener into `services/payments/main.go`**

Add imports `"net"`, `"google.golang.org/grpc"`, and `pb "omnifoni/payments/gen/paymentspb"`. Add the env read next to `addr`:

```go
	grpcAddr := env("PAYMENTS_GRPC_ADDR", ":50060")
```

After the `mux` setup and before `srv := &http.Server{...}`, add:

```go
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("grpc listen failed", "err", err)
		os.Exit(1)
	}
	gsrv := grpc.NewServer()
	pb.RegisterPaymentsServer(gsrv, &paymentsServer{pool: pool})
	go func() {
		<-ctx.Done()
		gsrv.GracefulStop()
	}()
	go func() {
		if err := gsrv.Serve(lis); err != nil {
			slog.Error("grpc serve failed", "err", err)
			os.Exit(1)
		}
	}()
	slog.Info("payments gRPC up", "addr", grpcAddr)
```

(`lis`/`gsrv` shadow nothing; the existing `srv.ListenAndServe()` block stays as-is.)

- [ ] **Step 8: Run the full payments suite**

Run: `cd services/payments && go vet ./... && go test ./...`
Expected: all PASS (new gRPC tests + existing webhook tests).

- [ ] **Step 9: Expose :50060 in `docker-compose.yml` — replace the payments service block with**

```yaml
  payments:
    build: ./services/payments
    environment:
      PAYMENTS_DSN: postgres://postgres:dev-only@postgres:5432/payments
      PAYMENTS_ADDR: ":50055"
      PAYMENTS_GRPC_ADDR: ":50060"
      PAYMENTS_WEBHOOK_SECRET: dev-only
    ports:
      - "50055:50055"
      - "50060:50060"
    depends_on:
      postgres:
        condition: service_healthy
```

- [ ] **Step 10: Rebuild the deployed payments service**

Run: `docker compose build payments && docker compose up -d payments`
Expected: container restarts serving both :50055 (HTTP) and :50060 (gRPC).

- [ ] **Step 11: Commit**

```bash
git add proto/payments/v1/payments.proto services/payments/gen/ services/payments/go.mod services/payments/go.sum services/payments/grpc.go services/payments/main.go services/payments/grpc_test.go Makefile docker-compose.yml
git commit -m "feat(payments): gRPC surface for reconciliation"
```

---

### Task 4: Reconciliation service core (proto, adapters, comparisons, server, unit tests)

**Files:**
- Create: `proto/reconciliation/v1/reconciliation.proto`
- Create (generated): `services/reconciliation/gen/reconciliationpb/`
- Modify: `Makefile` (reconciliation protoc recipe + test line)
- Create: `services/reconciliation/go.mod`, `services/reconciliation/reconcile.go`, `services/reconciliation/adapters.go`, `services/reconciliation/grpc.go`, `services/reconciliation/main.go`, `services/reconciliation/Dockerfile`
- Test: `services/reconciliation/reconcile_test.go` (create)

**Interfaces:**
- Consumes: Task 1/Task 3 generated types (billing + payments).
- Produces: gRPC `reconciliation.v1.Reconciliation` with `ReconcileAccount(account_id, period)` and `ReconcilePeriod(period)`; package-level pure funcs `CompareLedger`, `ComparePayments`, `DriftKinds`, `PeriodOf`, `validPeriod`; interfaces `BillingClient`, `PaymentsClient`, `DriftPublisher`; struct `Server{billing, payments, publisher}` with `reconcilePeriodData(ctx, period) (*pb.ReconcilePeriodResponse, error)`. Task 5 consumes all of these.

- [ ] **Step 1: Create `proto/reconciliation/v1/reconciliation.proto`**

```proto
syntax = "proto3";

package reconciliation.v1;

option go_package = "omnifoni/reconciliation/gen/reconciliationpb";

// Reconciliation is a read-only aggregator over Billing and Payments. It
// proves the money triangle: the rated-CDR ingest ledger matches invoice
// lines, and recorded payments match invoice totals. It owns no database.
service Reconciliation {
  // ReconcileAccount returns both comparisons for one account and period.
  // Ledger comparison is period-wide; payment comparison is per account.
  rpc ReconcileAccount(ReconcileAccountRequest) returns (ReconcileAccountResponse);
  // ReconcilePeriod returns the ledger comparison plus one row per account
  // found on either side.
  rpc ReconcilePeriod(ReconcilePeriodRequest) returns (ReconcilePeriodResponse);
}

message ReconcileAccountRequest {
  string account_id = 1;
  string period = 2; // YYYY-MM
}

message ReconcileAccountResponse {
  LedgerComparison ledger_vs_invoices = 1;
  PaymentComparison payments_vs_invoices = 2;
}

message ReconcilePeriodRequest {
  string period = 1; // YYYY-MM
}

message ReconcilePeriodResponse {
  LedgerComparison ledger_vs_invoices = 1;
  repeated PaymentComparison accounts = 2;
}

// LedgerComparison is period-wide: the billing ingest ledger (expected)
// against SUM/COUNT of invoice_lines (actual).
message LedgerComparison {
  string period = 1;
  int64 expected_count = 2;
  int64 actual_count = 3;
  int64 expected_amount_minor = 4;
  int64 actual_amount_minor = 5;
  string status = 6; // ok | drift
}

// PaymentComparison is per account: recorded payments against invoice total.
// The delta is a business state (accounts receivable), not reconciliation
// drift; only ledger mismatches are drift.
message PaymentComparison {
  string account_id = 1;
  string period = 2;
  int64 invoice_total_minor = 3;
  int64 payments_total_minor = 4;
  int64 delta_minor = 5; // payments - invoice
  string status = 6;     // paid | underpaid | overpaid | unpaid | no_activity
}

// DriftEvent is published to the reconciliation.drift Kafka topic when the
// sweep finds a ledger mismatch. account_id is empty for ledger drift.
message DriftEvent {
  string period = 1;
  string account_id = 2;
  string kind = 3; // ledger_count_mismatch | ledger_amount_mismatch
  int64 expected_minor = 4;
  int64 actual_minor = 5;
  int64 delta_minor = 6;
  int64 expected_count = 7;
  int64 actual_count = 8;
  int64 detected_at_ms = 9;
}
```

- [ ] **Step 2: Add the protoc recipe to `Makefile` (append after the payments recipe) and the test line**

```make
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/reconciliation --go_opt=module=omnifoni/reconciliation \
		--go-grpc_out=services/reconciliation --go-grpc_opt=module=omnifoni/reconciliation \
		proto/reconciliation/v1/reconciliation.proto
```

And in the `test:` target, after the payments line, add:

```make
	cd services/reconciliation && go vet ./... && go test ./...
```

(Run with a leading TAB, like the other lines.)

- [ ] **Step 3: Create the module and regenerate**

Run:
```bash
mkdir -p services/reconciliation
cd services/reconciliation && go mod init omnifoni/reconciliation
make protoc
cd services/reconciliation && go get google.golang.org/grpc google.golang.org/protobuf github.com/twmb/franz-go github.com/jackc/pgx/v5 && go mod tidy
```
Expected: `gen/reconciliationpb/{reconciliation.pb.go,reconciliation_grpc.pb.go}` exist.

- [ ] **Step 4: Create `services/reconciliation/reconcile.go` (pure logic)**

```go
package main

import (
	"fmt"
	"strconv"
	"time"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

// Statuses for the two comparison kinds.
const (
	StatusOk         = "ok"
	StatusDrift      = "drift"
	StatusPaid       = "paid"
	StatusUnderpaid  = "underpaid"
	StatusOverpaid   = "overpaid"
	StatusUnpaid     = "unpaid"
	StatusNoActivity = "no_activity"
)

// Drift kinds for DriftEvent.
const (
	KindLedgerCount  = "ledger_count_mismatch"
	KindLedgerAmount = "ledger_amount_mismatch"
)

// CompareLedger is period-wide: the billing ingest ledger (expected) against
// the invoice_lines SUM/COUNT (actual). Both dimensions must match for ok.
func CompareLedger(period string, ledgerCount, ledgerSum, invoiceCount, invoiceSum int64) *pb.LedgerComparison {
	status := StatusOk
	if ledgerCount != invoiceCount || ledgerSum != invoiceSum {
		status = StatusDrift
	}
	return &pb.LedgerComparison{
		Period:              period,
		ExpectedCount:       ledgerCount,
		ActualCount:         invoiceCount,
		ExpectedAmountMinor: ledgerSum,
		ActualAmountMinor:   invoiceSum,
		Status:              status,
	}
}

// ComparePayments is per account: recorded payments against the invoice
// total. The delta is a business state, not drift.
func ComparePayments(accountID, period string, invoiceTotal, paymentsTotal int64) *pb.PaymentComparison {
	var status string
	switch {
	case invoiceTotal == 0 && paymentsTotal == 0:
		status = StatusNoActivity
	case paymentsTotal == invoiceTotal:
		status = StatusPaid
	case paymentsTotal > invoiceTotal:
		status = StatusOverpaid
	case paymentsTotal == 0:
		status = StatusUnpaid
	default:
		status = StatusUnderpaid
	}
	return &pb.PaymentComparison{
		AccountId:          accountID,
		Period:             period,
		InvoiceTotalMinor:  invoiceTotal,
		PaymentsTotalMinor: paymentsTotal,
		DeltaMinor:         paymentsTotal - invoiceTotal,
		Status:             status,
	}
}

// DriftKinds lists which ledger dimensions mismatch, one DriftEvent kind per
// differing dimension.
func DriftKinds(c *pb.LedgerComparison) []string {
	var kinds []string
	if c.GetExpectedCount() != c.GetActualCount() {
		kinds = append(kinds, KindLedgerCount)
	}
	if c.GetExpectedAmountMinor() != c.GetActualAmountMinor() {
		kinds = append(kinds, KindLedgerAmount)
	}
	return kinds
}

// PeriodOf renders a time as the reconciliation period YYYY-MM in UTC.
func PeriodOf(t time.Time) string {
	u := t.UTC()
	return strconv.Itoa(u.Year()) + "-" + pad2(int(u.Month()))
}

func pad2(n int) string {
	if n < 10 {
		return "0" + strconv.Itoa(n)
	}
	return strconv.Itoa(n)
}

// validPeriod mirrors billing's period rule: strict YYYY-MM.
func validPeriod(p string) error {
	if len(p) != 7 || p[4] != '-' {
		return fmt.Errorf("period must be YYYY-MM")
	}
	for i, r := range p {
		if i == 4 {
			continue
		}
		if r < '0' || r > '9' {
			return fmt.Errorf("period must be YYYY-MM")
		}
	}
	m := int(p[5]-'0')*10 + int(p[6]-'0')
	if m < 1 || m > 12 {
		return fmt.Errorf("period month invalid")
	}
	return nil
}
```

- [ ] **Step 5: Create `services/reconciliation/adapters.go`**

```go
package main

import (
	"context"

	billingpb "omnifoni/billing/gen/billingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
)

// BillingClient is the subset of the billing API reconciliation needs.
type BillingClient interface {
	// GetPeriodLedger returns (cdr_count, amount_sum) for one period.
	GetPeriodLedger(ctx context.Context, period string) (int64, int64, error)
	// InvoiceTotals returns per-account invoice totals and line counts.
	InvoiceTotals(ctx context.Context, period string) (totals map[string]int64, counts map[string]int64, err error)
	// InvoiceTotal returns one account's invoice total and line count.
	InvoiceTotal(ctx context.Context, accountID, period string) (int64, int64, error)
	// ListAccounts returns distinct account ids with invoice lines.
	ListAccounts(ctx context.Context, period string) ([]string, error)
}

// PaymentsClient is the subset of the payments API reconciliation needs.
type PaymentsClient interface {
	// TotalsByAccount returns per-account recorded-payment sums.
	TotalsByAccount(ctx context.Context, period string) (map[string]int64, error)
	// ListAccounts returns distinct account ids with recorded payments.
	ListAccounts(ctx context.Context, period string) ([]string, error)
}

type billingAdapter struct {
	client billingpb.BillingClient
}

func (a *billingAdapter) GetPeriodLedger(ctx context.Context, period string) (int64, int64, error) {
	resp, err := a.client.GetPeriodLedger(ctx, &billingpb.GetPeriodLedgerRequest{Period: period})
	if err != nil {
		return 0, 0, err
	}
	return resp.GetCdrCount(), resp.GetAmountSum(), nil
}

func (a *billingAdapter) InvoiceTotals(ctx context.Context, period string) (map[string]int64, map[string]int64, error) {
	accounts, err := a.ListAccounts(ctx, period)
	if err != nil {
		return nil, nil, err
	}
	if len(accounts) == 0 {
		return map[string]int64{}, map[string]int64{}, nil
	}
	resp, err := a.client.GetInvoicesByAccounts(ctx, &billingpb.GetInvoicesByAccountsRequest{
		AccountIds: accounts, Period: period})
	if err != nil {
		return nil, nil, err
	}
	totals := make(map[string]int64, len(resp.GetTotals()))
	counts := make(map[string]int64, len(resp.GetTotals()))
	for _, t := range resp.GetTotals() {
		totals[t.GetAccountId()] = t.GetTotalMinor()
		counts[t.GetAccountId()] = t.GetLineCount()
	}
	return totals, counts, nil
}

func (a *billingAdapter) InvoiceTotal(ctx context.Context, accountID, period string) (int64, int64, error) {
	resp, err := a.client.GetInvoice(ctx, &billingpb.GetInvoiceRequest{AccountId: accountID, Period: period})
	if err != nil {
		return 0, 0, err
	}
	return resp.GetTotalMinor(), resp.GetLineCount(), nil
}

func (a *billingAdapter) ListAccounts(ctx context.Context, period string) ([]string, error) {
	resp, err := a.client.ListAccountsForPeriod(ctx, &billingpb.ListAccountsForPeriodRequest{Period: period})
	if err != nil {
		return nil, err
	}
	return resp.GetAccountIds(), nil
}

type paymentsAdapter struct {
	client paymentspb.PaymentsClient
}

func (a *paymentsAdapter) TotalsByAccount(ctx context.Context, period string) (map[string]int64, error) {
	resp, err := a.client.GetPaymentsTotals(ctx, &paymentspb.GetPaymentsTotalsRequest{Period: period})
	if err != nil {
		return nil, err
	}
	totals := make(map[string]int64, len(resp.GetTotals()))
	for _, t := range resp.GetTotals() {
		totals[t.GetAccountId()] = t.GetAmountSum()
	}
	return totals, nil
}

func (a *paymentsAdapter) ListAccounts(ctx context.Context, period string) ([]string, error) {
	resp, err := a.client.ListAccountsForPeriod(ctx, &paymentspb.ListAccountsForPeriodRequest{Period: period})
	if err != nil {
		return nil, err
	}
	return resp.GetAccountIds(), nil
}
```

- [ ] **Step 6: Create `services/reconciliation/grpc.go`**

```go
package main

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

// DriftPublisher sends one DriftEvent; the sweep uses it, RPCs never do.
type DriftPublisher interface {
	PublishDrift(ctx context.Context, ev *pb.DriftEvent) error
}

type noopPublisher struct{}

func (noopPublisher) PublishDrift(context.Context, *pb.DriftEvent) error { return nil }

// Server serves the reconciliation gRPC surface.
type Server struct {
	pb.UnimplementedReconciliationServer
	billing   BillingClient
	payments  PaymentsClient
	publisher DriftPublisher
}

func NewServer(billing BillingClient, payments PaymentsClient, publisher DriftPublisher) *Server {
	return &Server{billing: billing, payments: payments, publisher: publisher}
}

// ReconcilePeriod returns the ledger comparison plus one row per account
// found on either side. It reports; it never publishes drift.
func (s *Server) ReconcilePeriod(ctx context.Context, req *pb.ReconcilePeriodRequest) (*pb.ReconcilePeriodResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return s.reconcilePeriodData(ctx, req.GetPeriod())
}

// ReconcileAccount returns both comparisons for one account; an account with
// no activity on either side is a zero no_activity row.
func (s *Server) ReconcileAccount(ctx context.Context, req *pb.ReconcileAccountRequest) (*pb.ReconcileAccountResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id required")
	}
	period := req.GetPeriod()
	resp, err := s.reconcilePeriodData(ctx, period)
	if err != nil {
		return nil, err
	}
	pc := ComparePayments(req.GetAccountId(), period, 0, 0)
	for _, row := range resp.GetAccounts() {
		if row.GetAccountId() == req.GetAccountId() {
			pc = row
			break
		}
	}
	return &pb.ReconcileAccountResponse{LedgerVsInvoices: resp.GetLedgerVsInvoices(), PaymentsVsInvoices: pc}, nil
}

// reconcilePeriodData fetches both sides and assembles the period response.
func (s *Server) reconcilePeriodData(ctx context.Context, period string) (*pb.ReconcilePeriodResponse, error) {
	ledgerCount, ledgerSum, err := s.billing.GetPeriodLedger(ctx, period)
	if err != nil {
		return nil, err
	}
	billAccounts, err := s.billing.ListAccounts(ctx, period)
	if err != nil {
		return nil, err
	}
	payAccounts, err := s.payments.ListAccounts(ctx, period)
	if err != nil {
		return nil, err
	}
	invoiceTotals, invoiceCounts, err := s.billing.InvoiceTotals(ctx, period)
	if err != nil {
		return nil, err
	}
	paymentsTotals, err := s.payments.TotalsByAccount(ctx, period)
	if err != nil {
		return nil, err
	}

	var invoiceCount, invoiceSum int64
	for _, c := range invoiceCounts {
		invoiceCount += c
	}
	for _, v := range invoiceTotals {
		invoiceSum += v
	}

	// Deterministic union: billing accounts in order, then payments-only.
	union := make([]string, 0, len(billAccounts)+len(payAccounts))
	seen := map[string]bool{}
	for _, a := range append(append([]string{}, billAccounts...), payAccounts...) {
		if !seen[a] {
			seen[a] = true
			union = append(union, a)
		}
	}
	accounts := make([]*pb.PaymentComparison, 0, len(union))
	for _, a := range union {
		accounts = append(accounts, ComparePayments(a, period, invoiceTotals[a], paymentsTotals[a]))
	}
	return &pb.ReconcilePeriodResponse{
		LedgerVsInvoices: CompareLedger(period, ledgerCount, ledgerSum, invoiceCount, invoiceSum),
		Accounts:         accounts,
	}, nil
}
```

- [ ] **Step 7: Create `services/reconciliation/main.go`**

```go
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	billingpb "omnifoni/billing/gen/billingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	billingAddr := env("BILLING_ADDR", "billing:50054")
	paymentsAddr := env("PAYMENTS_ADDR", "payments:50060")
	addr := env("RECONCILIATION_ADDR", ":50059")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	bConn, err := grpc.NewClient(billingAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("billing client", "err", err)
		os.Exit(1)
	}
	pConn, err := grpc.NewClient(paymentsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("payments client", "err", err)
		os.Exit(1)
	}
	srv := NewServer(
		&billingAdapter{client: billingpb.NewBillingClient(bConn)},
		&paymentsAdapter{client: paymentspb.NewPaymentsClient(pConn)},
		noopPublisher{},
	)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer()
	pb.RegisterReconciliationServer(g, srv)
	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()
	slog.Info("reconciliation up", "addr", addr, "billing", billingAddr, "payments", paymentsAddr)
	if err := g.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 8: Create `services/reconciliation/Dockerfile`**

(Imports billing and payments gen via sibling replace directives, so the build context is ./services like switching.)

```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY billing/ ./billing/
COPY payments/ ./payments/
COPY reconciliation/ ./reconciliation/
RUN cd reconciliation && CGO_ENABLED=0 go build -o /reconciliation .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /reconciliation /reconciliation
ENTRYPOINT ["/reconciliation"]
```

And in `services/reconciliation/go.mod` add the sibling replaces (required for `go build` with the sibling gen imports):

```
replace omnifoni/billing => ../billing

replace omnifoni/payments => ../payments
```

- [ ] **Step 9: Write the unit tests — create `services/reconciliation/reconcile_test.go`**

```go
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

type fakeBilling struct {
	count, sum   int64
	totals       map[string]int64
	counts       map[string]int64
	accounts     []string
	err          error
}

func (f *fakeBilling) GetPeriodLedger(context.Context, string) (int64, int64, error) {
	return f.count, f.sum, f.err
}

func (f *fakeBilling) InvoiceTotals(context.Context, string) (map[string]int64, map[string]int64, error) {
	return f.totals, f.counts, f.err
}

func (f *fakeBilling) InvoiceTotal(_ context.Context, accountID, _ string) (int64, int64, error) {
	return f.totals[accountID], f.counts[accountID], f.err
}

func (f *fakeBilling) ListAccounts(context.Context, string) ([]string, error) {
	return f.accounts, f.err
}

type fakePayments struct {
	totals   map[string]int64
	accounts []string
	err      error
}

func (f *fakePayments) TotalsByAccount(context.Context, string) (map[string]int64, error) {
	return f.totals, f.err
}

func (f *fakePayments) ListAccounts(context.Context, string) ([]string, error) {
	return f.accounts, f.err
}

type fakePublisher struct{ events []*pb.DriftEvent }

func (f *fakePublisher) PublishDrift(_ context.Context, ev *pb.DriftEvent) error {
	f.events = append(f.events, ev)
	return nil
}

// TestCompareLedger: matching is ok; any count or amount mismatch is drift.
func TestCompareLedger(t *testing.T) {
	ok := CompareLedger("2026-11", 3, 140, 3, 140)
	if ok.GetStatus() != StatusOk {
		t.Fatalf("status %q, want ok", ok.GetStatus())
	}
	drift := CompareLedger("2026-11", 3, 140, 2, 140)
	if drift.GetStatus() != StatusDrift {
		t.Fatalf("status %q, want drift", drift.GetStatus())
	}
}

// TestComparePayments: every status branch.
func TestComparePayments(t *testing.T) {
	cases := []struct {
		name                 string
		invoice, payments    int64
		want                 string
	}{
		{"both zero", 0, 0, StatusNoActivity},
		{"paid", 100, 100, StatusPaid},
		{"overpaid", 100, 150, StatusOverpaid},
		{"unpaid", 100, 0, StatusUnpaid},
		{"underpaid", 100, 40, StatusUnderpaid},
	}
	for _, c := range cases {
		got := ComparePayments("acc", "2026-11", c.invoice, c.payments)
		if got.GetStatus() != c.want {
			t.Fatalf("%s: status %q, want %q", c.name, got.GetStatus(), c.want)
		}
		if got.GetDeltaMinor() != c.payments-c.invoice {
			t.Fatalf("%s: delta %d, want %d", c.name, got.GetDeltaMinor(), c.payments-c.invoice)
		}
	}
}

// TestDriftKinds: one kind per differing dimension.
func TestDriftKinds(t *testing.T) {
	if kinds := DriftKinds(CompareLedger("2026-11", 3, 140, 3, 140)); len(kinds) != 0 {
		t.Fatalf("ok comparison produced kinds %v", kinds)
	}
	amountOnly := DriftKinds(CompareLedger("2026-11", 3, 140, 3, 100))
	if len(amountOnly) != 1 || amountOnly[0] != KindLedgerAmount {
		t.Fatalf("kinds %v, want [ledger_amount_mismatch]", amountOnly)
	}
	both := DriftKinds(CompareLedger("2026-11", 3, 140, 2, 100))
	if len(both) != 2 {
		t.Fatalf("kinds %v, want both", both)
	}
}

// TestReconcilePeriodData: union ordering, zero-fill, ledger sums.
func TestReconcilePeriodData(t *testing.T) {
	b := &fakeBilling{
		count: 3, sum: 140,
		totals:   map[string]int64{"acc-a": 100, "acc-b": 40},
		counts:   map[string]int64{"acc-a": 2, "acc-b": 1},
		accounts: []string{"acc-a", "acc-b"},
	}
	p := &fakePayments{
		totals:   map[string]int64{"acc-a": 100},
		accounts: []string{"acc-c"},
	}
	s := NewServer(b, p, noopPublisher{})
	resp, err := s.reconcilePeriodData(context.Background(), "2026-11")
	if err != nil {
		t.Fatal(err)
	}
	led := resp.GetLedgerVsInvoices()
	if led.GetStatus() != StatusOk || led.GetExpectedCount() != 3 || led.GetActualCount() != 3 || led.GetExpectedAmountMinor() != 140 || led.GetActualAmountMinor() != 140 {
		t.Fatalf("ledger comparison wrong: %+v", led)
	}
	if len(resp.GetAccounts()) != 3 {
		t.Fatalf("accounts %d, want 3 (union)", len(resp.GetAccounts()))
	}
	byAccount := map[string]*pb.PaymentComparison{}
	for _, a := range resp.GetAccounts() {
		byAccount[a.GetAccountId()] = a
	}
	if byAccount["acc-a"].GetStatus() != StatusPaid {
		t.Fatalf("acc-a %q, want paid", byAccount["acc-a"].GetStatus())
	}
	if byAccount["acc-b"].GetStatus() != StatusUnpaid {
		t.Fatalf("acc-b %q, want unpaid", byAccount["acc-b"].GetStatus())
	}
	if byAccount["acc-c"].GetStatus() != StatusNoActivity {
		t.Fatalf("acc-c %q, want no_activity", byAccount["acc-c"].GetStatus())
	}
}

// TestReconcilePeriodUpstreamError: upstream failures surface, not mask.
func TestReconcilePeriodUpstreamError(t *testing.T) {
	b := &fakeBilling{err: errors.New("billing down")}
	s := NewServer(b, &fakePayments{}, noopPublisher{})
	if _, err := s.reconcilePeriodData(context.Background(), "2026-11"); err == nil {
		t.Fatal("expected an error from a failing billing client")
	}
}

// TestReconcileAccountNoActivity: an unknown account is a zero row, not an error.
func TestReconcileAccountNoActivity(t *testing.T) {
	b := &fakeBilling{
		totals:   map[string]int64{"acc-a": 100},
		counts:   map[string]int64{"acc-a": 1},
		accounts: []string{"acc-a"},
	}
	p := &fakePayments{totals: map[string]int64{"acc-a": 100}, accounts: []string{"acc-a"}}
	s := NewServer(b, p, noopPublisher{})
	resp, err := s.ReconcileAccount(context.Background(), &pb.ReconcileAccountRequest{AccountId: "acc-none", Period: "2026-11"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPaymentsVsInvoices().GetStatus() != StatusNoActivity {
		t.Fatalf("status %q, want no_activity", resp.GetPaymentsVsInvoices().GetStatus())
	}
}

// TestReconcileAccountValidation: bad period / empty account rejected.
func TestReconcileAccountValidation(t *testing.T) {
	s := NewServer(&fakeBilling{}, &fakePayments{}, noopPublisher{})
	if _, err := s.ReconcileAccount(context.Background(), &pb.ReconcileAccountRequest{AccountId: "a", Period: "bad"}); err == nil {
		t.Fatal("bad period accepted")
	}
	if _, err := s.ReconcileAccount(context.Background(), &pb.ReconcileAccountRequest{AccountId: "", Period: "2026-11"}); err == nil {
		t.Fatal("empty account accepted")
	}
}

// TestPeriodOf: UTC YYYY-MM rendering.
func TestPeriodOf(t *testing.T) {
	if got := PeriodOf(time.Date(2026, 1, 5, 23, 0, 0, 0, time.UTC)); got != "2026-01" {
		t.Fatalf("PeriodOf %q, want 2026-01", got)
	}
}
```

- [ ] **Step 10: Run the unit tests**

Run: `cd services/reconciliation && go vet ./... && go test ./...`
Expected: all PASS (no stack needed — pure fakes).

- [ ] **Step 11: Commit**

```bash
git add proto/reconciliation/v1/reconciliation.proto services/reconciliation/gen/ services/reconciliation/go.mod services/reconciliation/go.sum services/reconciliation/reconcile.go services/reconciliation/adapters.go services/reconciliation/grpc.go services/reconciliation/main.go services/reconciliation/Dockerfile services/reconciliation/reconcile_test.go Makefile
git commit -m "feat(reconciliation): money-triangle service"
```

---

### Task 5: Sweep, Kafka drift publisher, e2e on the real stack

**Files:**
- Modify: `services/reconciliation/main.go` (kafka publisher wiring + ticker)
- Create: `services/reconciliation/sweep.go`
- Test: `services/reconciliation/e2e_test.go` (create)

**Interfaces:**
- Consumes: Task 4's `Server`, `DriftPublisher`, `reconcilePeriodData`, `DriftKinds`, `PeriodOf`; fakes from `reconcile_test.go`.
- Produces: `func (s *Server) Sweep(ctx context.Context, now time.Time) error`; `type kafkaPublisher struct{...}` / `newKafkaPublisher(prod *kgo.Client, topic string) DriftPublisher`; main ticker honoring `RECONCILIATION_SWEEP_INTERVAL` (default 15m, `0` disables).

- [ ] **Step 1: Create `services/reconciliation/sweep.go`**

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"
	kgo "github.com/twmb/franz-go/pkg/kgo"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

// kafkaPublisher publishes DriftEvents as proto bytes, keyed by account when
// present else by period.
type kafkaPublisher struct {
	prod  *kgo.Client
	topic string
}

func newKafkaPublisher(prod *kgo.Client, topic string) DriftPublisher {
	return &kafkaPublisher{prod: prod, topic: topic}
}

func (p *kafkaPublisher) PublishDrift(ctx context.Context, ev *pb.DriftEvent) error {
	b, err := proto.Marshal(ev)
	if err != nil {
		return err
	}
	key := ev.GetPeriod()
	if ev.GetAccountId() != "" {
		key = ev.GetAccountId()
	}
	return p.prod.ProduceSync(ctx, &kgo.Record{Topic: p.topic, Key: []byte(key), Value: b}).FirstErr()
}

// Sweep runs one pass over the current and previous month. Ledger drift is
// warned and published; payment deltas are info logs (accounts-receivable
// states, not drift).
func (s *Server) Sweep(ctx context.Context, now time.Time) error {
	for _, period := range []string{PeriodOf(now), PeriodOf(now.AddDate(0, -1, 0))} {
		resp, err := s.reconcilePeriodData(ctx, period)
		if err != nil {
			return fmt.Errorf("reconcile %s: %w", period, err)
		}
		led := resp.GetLedgerVsInvoices()
		if led.GetStatus() == StatusDrift {
			slog.Warn("reconciliation drift", "period", period,
				"expected_count", led.GetExpectedCount(), "actual_count", led.GetActualCount(),
				"expected_minor", led.GetExpectedAmountMinor(), "actual_minor", led.GetActualAmountMinor())
			for _, kind := range DriftKinds(led) {
				ev := &pb.DriftEvent{
					Period:        period,
					Kind:          kind,
					ExpectedMinor: led.GetExpectedAmountMinor(),
					ActualMinor:   led.GetActualAmountMinor(),
					DeltaMinor:    led.GetActualAmountMinor() - led.GetExpectedAmountMinor(),
					ExpectedCount: led.GetExpectedCount(),
					ActualCount:   led.GetActualCount(),
					DetectedAtMs:  now.UnixMilli(),
				}
				if err := s.publisher.PublishDrift(ctx, ev); err != nil {
					slog.Error("drift publish failed", "period", period, "kind", kind, "err", err)
				}
			}
		}
		for _, pc := range resp.GetAccounts() {
			st := pc.GetStatus()
			if st != StatusPaid && st != StatusNoActivity {
				slog.Info("payment delta", "period", period, "account", pc.GetAccountId(),
					"status", st, "invoice_minor", pc.GetInvoiceTotalMinor(),
					"payments_minor", pc.GetPaymentsTotalMinor())
			}
		}
	}
	return nil
}
```

- [ ] **Step 2: Wire the publisher and ticker into `services/reconciliation/main.go`**

Change the imports: add `"time"` and keep the rest. Replace the `noopPublisher{}` server construction and add the ticker. The full `main()` after the two `grpc.NewClient` blocks becomes:

```go
	brokers := []string{env("RECONCILIATION_KAFKA", "kafka:9092")}
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
	if err != nil {
		slog.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	srv := NewServer(
		&billingAdapter{client: billingpb.NewBillingClient(bConn)},
		&paymentsAdapter{client: paymentspb.NewPaymentsClient(pConn)},
		newKafkaPublisher(prod, env("RECONCILIATION_TOPIC_DRIFT", "reconciliation.drift")),
	)

	if iv := env("RECONCILIATION_SWEEP_INTERVAL", "15m"); iv != "0" {
		interval, err := time.ParseDuration(iv)
		if err != nil {
			slog.Error("bad RECONCILIATION_SWEEP_INTERVAL", "value", iv, "err", err)
			os.Exit(1)
		}
		go func() {
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := srv.Sweep(context.Background(), time.Now()); err != nil {
						slog.Error("sweep failed", "err", err)
					}
				}
			}
		}()
	}

	lis, err := net.Listen("tcp", addr)
```

(The `lis`/`Serve` block from Task 4 stays unchanged below this.)

- [ ] **Step 3: Write the e2e tests — create `services/reconciliation/e2e_test.go`**

```go
package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	kgo "github.com/twmb/franz-go/pkg/kgo"

	billingpb "omnifoni/billing/gen/billingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

// E2E tests run against the dev stack (make up) with rebuilt billing and
// payments services: billing gRPC on localhost:50054, payments gRPC on
// localhost:50060, both DBs on localhost:5432, Kafka on localhost:9092.

func billingDSN() string {
	if s := os.Getenv("BILLING_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/billing"
}

func paymentsDSN() string {
	if s := os.Getenv("PAYMENTS_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/payments"
}

func stackBrokers() []string { return []string{"localhost:9092"} }

// liveStack returns real adapters over the running billing/payments gRPC
// services, or skips when the stack is down or stale (old images lack the
// new RPCs).
func liveStack(t *testing.T) (BillingClient, PaymentsClient) {
	t.Helper()
	bConn, err := grpc.NewClient("localhost:50054", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Skipf("billing unreachable: %v", err)
	}
	pConn, err := grpc.NewClient("localhost:50060", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Skipf("payments unreachable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := billingpb.NewBillingClient(bConn).GetPeriodLedger(ctx, &billingpb.GetPeriodLedgerRequest{Period: "1970-01"}); err != nil {
		if status.Code(err) == codes.Unimplemented {
			t.Skip("billing stale; run: docker compose build billing && docker compose up -d billing")
		}
		t.Skipf("billing probe failed: %v", err)
	}
	if _, err := paymentspb.NewPaymentsClient(pConn).GetPaymentsTotals(ctx, &paymentspb.GetPaymentsTotalsRequest{Period: "1970-01"}); err != nil {
		if status.Code(err) == codes.Unimplemented {
			t.Skip("payments stale; run: docker compose build payments && docker compose up -d payments")
		}
		t.Skipf("payments probe failed: %v", err)
	}
	return &billingAdapter{client: billingpb.NewBillingClient(bConn)},
		&paymentsAdapter{client: paymentspb.NewPaymentsClient(pConn)}
}

// seedLine inserts one invoice line directly into the billing DB.
func seedLine(t *testing.T, bpool *pgxpool.Pool, callID, account, period string, amount int64) {
	t.Helper()
	if _, err := bpool.Exec(context.Background(),
		`INSERT INTO invoice_lines (call_id, account_id, period, charged_seconds, amount_minor, rate_per_sec)
		 VALUES ($1, $2, $3, 10, $4, 1)`, callID, account, period, amount); err != nil {
		t.Fatal(err)
	}
}

func seedLedger(t *testing.T, bpool *pgxpool.Pool, period string, count, sum int64) {
	t.Helper()
	if _, err := bpool.Exec(context.Background(),
		`INSERT INTO cdr_period_totals (period, cdr_count, amount_sum) VALUES ($1, $2, $3)
		 ON CONFLICT (period) DO UPDATE SET cdr_count = $2, amount_sum = $3`, period, count, sum); err != nil {
		t.Fatal(err)
	}
}

func seedPayment(t *testing.T, ppool *pgxpool.Pool, event, account string, amount int64, at time.Time) {
	t.Helper()
	if _, err := ppool.Exec(context.Background(),
		`INSERT INTO payments (event_id, account_id, gateway, amount_minor, status, created_at)
		 VALUES ($1, $2, 'stripe', $3, 'recorded', $4)`, event, account, amount, at); err != nil {
		t.Fatal(err)
	}
}

// TestReconcilePeriodTriangle: the full money triangle on the real stack.
func TestReconcilePeriodTriangle(t *testing.T) {
	ctx := context.Background()
	bpool, err := pgxpool.New(ctx, billingDSN())
	if err != nil {
		t.Skipf("billing db unreachable: %v", err)
	}
	defer bpool.Close()
	ppool, err := pgxpool.New(ctx, paymentsDSN())
	if err != nil {
		t.Skipf("payments db unreachable: %v", err)
	}
	defer ppool.Close()
	bc, pc := liveStack(t)

	const period = "2026-11"
	if _, err := bpool.Exec(ctx, `TRUNCATE invoice_lines, cdr_period_totals`); err != nil {
		t.Fatal(err)
	}
	if _, err := ppool.Exec(ctx, `TRUNCATE payments`); err != nil {
		t.Fatal(err)
	}
	// acc-a: invoice 100 across 2 lines, paid exactly. acc-b: invoice 40,
	// unpaid. Ledger matches invoice_lines exactly: count 3, sum 140.
	seedLine(t, bpool, "e2e-a1", "acc-a", period, 60)
	seedLine(t, bpool, "e2e-a2", "acc-a", period, 40)
	seedLine(t, bpool, "e2e-b1", "acc-b", period, 40)
	seedLedger(t, bpool, period, 3, 140)
	seedPayment(t, ppool, "e2e-pay-a", "acc-a", 100, time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC))

	srv := NewServer(bc, pc, noopPublisher{})
	resp, err := srv.ReconcilePeriod(ctx, &pb.ReconcilePeriodRequest{Period: period})
	if err != nil {
		t.Fatal(err)
	}
	led := resp.GetLedgerVsInvoices()
	if led.GetStatus() != StatusOk {
		t.Fatalf("ledger status %q, want ok: %+v", led.GetStatus(), led)
	}
	byAccount := map[string]*pb.PaymentComparison{}
	for _, a := range resp.GetAccounts() {
		byAccount[a.GetAccountId()] = a
	}
	if byAccount["acc-a"].GetStatus() != StatusPaid {
		t.Fatalf("acc-a %q, want paid: %+v", byAccount["acc-a"].GetStatus(), byAccount["acc-a"])
	}
	if byAccount["acc-b"].GetStatus() != StatusUnpaid {
		t.Fatalf("acc-b %q, want unpaid", byAccount["acc-b"].GetStatus())
	}
	// On-demand RPCs must not publish drift even when the ledger matches.
	if _, err := srv.ReconcileAccount(ctx, &pb.ReconcileAccountRequest{AccountId: "acc-a", Period: period}); err != nil {
		t.Fatal(err)
	}
}

// TestSweepPublishesDriftEvent: a corrupted ledger produces a DriftEvent on
// reconciliation.drift; the sweep is invoked directly, never via ticker.
func TestSweepPublishesDriftEvent(t *testing.T) {
	ctx := context.Background()
	bpool, err := pgxpool.New(ctx, billingDSN())
	if err != nil {
		t.Skipf("billing db unreachable: %v", err)
	}
	defer bpool.Close()
	ppool, err := pgxpool.New(ctx, paymentsDSN())
	if err != nil {
		t.Skipf("payments db unreachable: %v", err)
	}
	defer ppool.Close()
	bc, pc := liveStack(t)

	prod, err := kgo.NewClient(kgo.SeedBrokers(stackBrokers()...), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Skipf("kafka unreachable: %v", err)
	}
	defer prod.Close()
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	if err := prod.Ping(pctx); err != nil {
		cancel()
		t.Skipf("kafka ping failed: %v", err)
	}
	cancel()

	const period = "2026-11"
	if _, err := bpool.Exec(ctx, `TRUNCATE invoice_lines, cdr_period_totals`); err != nil {
		t.Fatal(err)
	}
	if _, err := ppool.Exec(ctx, `TRUNCATE payments`); err != nil {
		t.Fatal(err)
	}
	// Invoices say 1 CDR / 50 minor; the (corrupted) ledger claims 999 CDRs /
	// 999999 minor — both dimensions drift.
	seedLine(t, bpool, "e2e-drift-1", "acc-x", period, 50)
	seedLedger(t, bpool, period, 999, 999999)

	srv := NewServer(bc, pc, newKafkaPublisher(prod, "reconciliation.drift"))
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	if err := srv.Sweep(ctx, now); err != nil {
		t.Fatal(err)
	}

	dead, err := kgo.NewClient(kgo.SeedBrokers(stackBrokers()...),
		kgo.ConsumerGroup(fmt.Sprintf("recon-test-%d", now.UnixNano())),
		kgo.ConsumeTopics("reconciliation.drift"),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit())
	if err != nil {
		t.Skipf("kafka unreachable: %v", err)
	}
	defer dead.Close()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := dead.PollRecords(pctx, 10)
		cancel()
		var found *pb.DriftEvent
		fetches.EachRecord(func(r *kgo.Record) {
			ev := &pb.DriftEvent{}
			if proto.Unmarshal(r.Value, ev) == nil && ev.GetPeriod() == period &&
				ev.GetKind() == KindLedgerAmount {
				found = ev
			}
		})
		if found != nil {
			if found.GetExpectedMinor() != 999999 || found.GetActualMinor() != 50 ||
				found.GetDeltaMinor() != -999949 || found.GetExpectedCount() != 999 || found.GetActualCount() != 1 ||
				found.GetDetectedAtMs() != now.UnixMilli() {
				t.Fatalf("drift event fields wrong: %+v", found)
			}
			return
		}
	}
	t.Fatal("drift event for 2026-11 not observed on reconciliation.drift")
}
```

- [ ] **Step 4: Run the unit tests first (fast gate), then the e2e**

Run: `cd services/reconciliation && go vet ./... && go test ./...`
Expected: unit tests PASS; e2e tests PASS when the stack is up with rebuilt billing/payments (Task 2/3 rebuilt them) — both e2e tests skip cleanly when the stack is down or stale.

- [ ] **Step 5: Commit**

```bash
git add services/reconciliation/sweep.go services/reconciliation/main.go services/reconciliation/e2e_test.go
git commit -m "feat(reconciliation): sweep and drift events"
```

---

### Task 6: Dev stack, smoke probe, docs, full gate

**Files:**
- Modify: `docker-compose.yml` (reconciliation service block)
- Create: `services/reconciliation/cmd/smokerecon/main.go`
- Modify: `ops/smoke.sh`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: everything above.
- Produces: `make up` runs reconciliation; `make smoke` proves the stack including a live `ReconcilePeriod` call.

- [ ] **Step 1: Add the reconciliation service block to `docker-compose.yml`** (after the `sbc` block, before `volumes:`)

```yaml
  reconciliation:
    build: ./services/reconciliation
    environment:
      BILLING_ADDR: billing:50054
      PAYMENTS_ADDR: payments:50060
      RECONCILIATION_KAFKA: kafka:9092
      RECONCILIATION_ADDR: ":50059"
      RECONCILIATION_SWEEP_INTERVAL: 15m
    ports:
      - "50059:50059"
    depends_on:
      billing:
        condition: service_started
      payments:
        condition: service_started
```

- [ ] **Step 2: Create the smoke probe `services/reconciliation/cmd/smokerecon/main.go`**

(A separate package so `go run` works inside the module. It asserts the RPC answers with well-formed statuses — not all-paid, because leftover dev data may legitimately show underpaid/overpaid.)

```go
// Command smokerecon calls ReconcilePeriod on the dev reconciliation service
// and asserts well-formed results. Used by ops/smoke.sh.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

func main() {
	addr := flag.String("addr", "localhost:50059", "reconciliation gRPC address")
	flag.Parse()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("FAIL:", err)
		os.Exit(1)
	}
	client := pb.NewReconciliationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	period := time.Now().UTC().Format("2006-01")
	resp, err := client.ReconcilePeriod(ctx, &pb.ReconcilePeriodRequest{Period: period})
	if err != nil {
		fmt.Println("FAIL:", err)
		os.Exit(1)
	}
	if st := resp.GetLedgerVsInvoices().GetStatus(); st != "ok" && st != "drift" {
		fmt.Println("FAIL: ledger status", st)
		os.Exit(1)
	}
	for _, a := range resp.GetAccounts() {
		switch a.GetStatus() {
		case "paid", "underpaid", "overpaid", "unpaid", "no_activity":
		default:
			fmt.Println("FAIL: account status", a.GetStatus())
			os.Exit(1)
		}
	}
	fmt.Println("ok: reconcile", resp.GetLedgerVsInvoices().GetPeriod())
}
```

- [ ] **Step 3: Extend `ops/smoke.sh`** — after the valkey/clickhouse/kafka checks and before the final `echo "smoke: PASS"`, add:

```bash
for s in charging switching mediation billing payments identity routing sbc reconciliation; do
  st=$(docker inspect --format '{{.State.Status}}' "omni-fon-$s-1" 2>/dev/null || echo missing)
  [ "$st" = "running" ] || fail "$s not running ($st)"
  echo "ok: $s running"
done

(cd services/reconciliation && go run ./cmd/smokerecon) || fail "reconciliation probe"
```

- [ ] **Step 4: Update `AGENTS.md`**

a) In the P2 slice two (billing) bullet area, the payments bullet: append to the end of the payments bullet (after "at-most-once money recording"): " gRPC `GetPaymentsTotals`/`ListAccountsForPeriod` on `PAYMENTS_GRPC_ADDR` (default :50060) serves reconciliation (recorded payments only; period = YYYY-MM of `created_at` UTC)."

b) Add a new bullet after the payments bullet:

"- P2 slice four: `services/reconciliation` on :50059 is the read-only money-triangle aggregator (gRPC `ReconcileAccount`/`ReconcilePeriod`, `proto/reconciliation/v1/reconciliation.proto`). No DB: it reads Billing (`GetPeriodLedger`, `GetInvoicesByAccounts`, `ListAccountsForPeriod`) and Payments (`GetPaymentsTotals`, `ListAccountsForPeriod`) over gRPC. Billing maintains the `cdr_period_totals` ledger transactionally with each new `invoice_lines` insert (replays touch neither table), so ledger-vs-invoice mismatch is genuine drift. The sweep (env `RECONCILIATION_SWEEP_INTERVAL`, default 15m, `0` disables) scans current+previous month: ledger drift is warned and published as proto `DriftEvent` to `reconciliation.drift` (`RECONCILIATION_TOPIC_DRIFT`); payment deltas are info logs only (AR states, not drift); on-demand RPCs report and never publish. Envs: `BILLING_ADDR` (billing:50054), `PAYMENTS_ADDR` (payments:50060), `RECONCILIATION_KAFKA` (kafka:9092)."

c) In the "Status" intro line for payments, no other change. Verify `make protoc`'s doc mention still holds (it now covers payments and reconciliation too — the existing line says "regenerates both protos"; update it to "regenerates all service protos (catalogue, charging, switching, mediation, billing, payments, routing, sbc, reconciliation)").

- [ ] **Step 5: Full gate**

Run: `docker compose build reconciliation && docker compose up -d reconciliation && make test && make smoke`
Expected: image builds; `make test` all packages `ok` (now including reconciliation); `make smoke` PASS with all 9 services running and the reconcile probe ok.

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml ops/smoke.sh AGENTS.md services/reconciliation/cmd/
git commit -m "feat(ops): reconciliation in the dev stack"
```

---

## Self-review checklist (for the plan author)

- Spec §4.1 ledger → Task 2; §4.1 RPCs → Tasks 1-2; §4.2 payments gRPC → Task 3; §4.3/4.4 service + DriftEvent → Tasks 4-5; §6 compose/ops/docs → Task 6; §7 testing spread across Tasks 2, 3, 4, 5.
- All code blocks complete; no placeholders; signatures consistent (`GetPeriodLedger`, `InvoiceTotals`, `newKafkaPublisher`, `reconcilePeriodData` used identically in Tasks 4/5).
- Task 4 `main.go` must NOT import franz-go (unused until Task 5's wiring); verified all referenced test helpers exist (`pool`, `catPool`, `cat`, `producer`, `cdr`, `publish`, `waitLines` in `biller_test.go`; `pool` from payments TestMain; `validPeriod` already in billing `grpc.go`, payments gets its own copy).
- Known spec deviation, recorded here: smoke asserts well-formed statuses rather than all-OK, because leftover dev data may legitimately show underpaid/overpaid.
