package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	pb "omnifoni/charging/gen/chargingpb"
)

// Integration tests run against the dev data layer (make up).
func dsn() string {
	if s := os.Getenv("CHARGING_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/charging"
}

func catDSN() string {
	if s := os.Getenv("CHARGING_TEST_CATALOGUE_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/catalogue"
}

var (
	pool      *pgxpool.Pool
	catClient cataloguepb.CatalogueClient
	catPool   *pgxpool.Pool
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	pool, err = pgxpool.New(ctx, dsn())
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	// Rates live in the Catalogue DB; tests gRPC to the catalogue service
	// (compose on :50053) for seeding, mirroring the runtime path.
	cc, err := grpc.NewClient("localhost:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "catalogue client:", err)
		os.Exit(1)
	}
	catClient = cataloguepb.NewCatalogueClient(cc)
	catPool, err = pgxpool.New(ctx, catDSN())
	if err != nil {
		fmt.Fprintln(os.Stderr, "catalogue pool:", err)
		os.Exit(1)
	}
	// ponytail: mirrors catalogue/migrate.sql so tests can seed before the
	// catalogue container first boots; delete when a migration tool exists
	if _, err := catPool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS rates (prefix text PRIMARY KEY, rate_per_sec numeric(14,6) NOT NULL CHECK (rate_per_sec > 0))`); err != nil {
		fmt.Fprintln(os.Stderr, "catalogue migrate:", err)
		os.Exit(1)
	}
	code := m.Run()
	os.Exit(code)
}

// truncate resets all state between tests.
func truncate(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`TRUNCATE ledger, sessions, accounts`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catPool.Exec(context.Background(), `TRUNCATE rates`); err != nil {
		t.Fatal(err)
	}
}

// seed inserts the golden rate set (via the Catalogue service, like runtime)
// and one prepaid account.
func seed(t *testing.T, balanceMinor int64) string {
	t.Helper()
	ctx := context.Background()
	rates := [][2]string{
		{"1", "0.250000"},
		{"27", "0.100000"},
		{"2782", "0.050000"},
	}
	for _, r := range rates {
		_, err := catClient.PutRate(ctx, &cataloguepb.PutRateRequest{Prefix: r[0], RatePerSec: r[1]})
		if err != nil {
			t.Fatal(err)
		}
	}
	const acc = "acc-test"
	_, err := pool.Exec(ctx,
		`INSERT INTO accounts (account_id, kind, balance) VALUES ($1, 'prepaid', $2)`,
		acc, balanceMinor)
	if err != nil {
		t.Fatal(err)
	}
	return acc
}

func authorize(t *testing.T, acc, dest string) *pb.AuthorizeResponse {
	t.Helper()
	s := NewServer(pool, catClient)
	resp, err := s.Authorize(context.Background(), &pb.AuthorizeRequest{AccountId: acc, Destination: dest})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func charge(t *testing.T, session string, total int64) *pb.ChargeResponse {
	t.Helper()
	s := NewServer(pool, catClient)
	resp, err := s.Charge(context.Background(), &pb.ChargeRequest{SessionId: session, TotalSeconds: total})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func balance(t *testing.T, acc string) int64 {
	t.Helper()
	var b int64
	if err := pool.QueryRow(context.Background(),
		`SELECT balance FROM accounts WHERE account_id = $1`, acc).Scan(&b); err != nil {
		t.Fatal(err)
	}
	return b
}

// ledgerCharged returns the total charged amount for an account (positive).
func ledgerCharged(t *testing.T, acc string) int64 {
	t.Helper()
	var sum int64
	if err := pool.QueryRow(context.Background(),
		`SELECT COALESCE(-SUM(amount_minor),0) FROM ledger WHERE account_id = $1`, acc).Scan(&sum); err != nil {
		t.Fatal(err)
	}
	return sum
}

// TestGoldenRating asserts exact money on the golden dataset. Zero discrepancy
// between rated cost, balance movement, and the ledger is the P1 money gate.
func TestGoldenRating(t *testing.T) {
	truncate(t)
	acc := seed(t, 100000) // 10.00 currency

	cases := []struct {
		dest       string
		maxSeconds int64 // expected from the running balance at that point
		seconds    int64
		costMinor  int64
	}{
		// longest prefix wins: 2782 at 0.05/sec, full balance 100000
		{"27821234567", 2000000, 60, 3},
		// 27 at 0.10/sec, balance now 99997: floor(99997/0.1)
		{"27311234567", 999970, 60, 6},
		// 1 at 0.25/sec, balance now 99991: floor(99991/0.25)
		{"12125550123", 399964, 60, 15},
	}
	expectedBal := int64(100000)
	for i, c := range cases {
		resp := authorize(t, acc, c.dest)
		if !resp.Allow {
			t.Fatalf("case %d: denied %q", i, resp.Reason)
		}
		if resp.MaxSeconds != c.maxSeconds {
			t.Fatalf("case %d: max_seconds %d, want %d", i, resp.MaxSeconds, c.maxSeconds)
		}
		ch := charge(t, resp.SessionId, c.seconds)
		if !ch.Applied || ch.Denied {
			t.Fatalf("case %d: charge not applied: %+v", i, ch)
		}
		expectedBal -= c.costMinor
		if ch.BalanceMinor != expectedBal {
			t.Fatalf("case %d: balance %d, want %d", i, ch.BalanceMinor, expectedBal)
		}
	}
	if got, want := ledgerCharged(t, acc), int64(24); got != want {
		t.Fatalf("ledger sum %d, want %d", got, want)
	}
	if got, want := balance(t, acc), int64(99976); got != want {
		t.Fatalf("balance %d, want %d", got, want)
	}
}

// TestDenyPaths covers the fail-closed deny reasons.
func TestDenyPaths(t *testing.T) {
	truncate(t)
	seed(t, 0)
	if r := authorize(t, "acc-test", "27821234567"); r.Allow || r.Reason != reasonInsufficientBalance {
		t.Fatalf("want insufficient_balance, got %+v", r)
	}
	if r := authorize(t, "acc-test", "447900000000"); r.Allow || r.Reason != reasonNoRate {
		t.Fatalf("want no_rate, got %+v", r)
	}
	if r := authorize(t, "ghost", "27821234567"); r.Allow || r.Reason != reasonNoAccount {
		t.Fatalf("want no_account, got %+v", r)
	}
	if r := authorize(t, "", "27821234567"); r.Allow || r.Reason != reasonInternalError {
		t.Fatalf("want internal_error on empty input, got %+v", r)
	}
}

// TestChargeIdempotent: replaying the same total must not double-charge and
// decreasing totals must not roll back charges.
func TestChargeIdempotent(t *testing.T) {
	truncate(t)
	acc := seed(t, 100000)
	sess := authorize(t, acc, "27821234567")
	if !sess.Allow {
		t.Fatal("expected allow")
	}

	first := charge(t, sess.SessionId, 60)
	if !first.Applied {
		t.Fatalf("first charge not applied: %+v", first)
	}
	replay := charge(t, sess.SessionId, 60)
	if replay.Applied {
		t.Fatal("replay charged again")
	}
	if replay.BalanceMinor != first.BalanceMinor {
		t.Fatalf("balance moved on replay: %d -> %d", first.BalanceMinor, replay.BalanceMinor)
	}
	older := charge(t, sess.SessionId, 30)
	if older.Applied {
		t.Fatal("older total applied")
	}
	if got, want := ledgerCharged(t, acc), int64(3); got != want {
		t.Fatalf("ledger sum %d, want %d (double charge)", got, want)
	}

	// monotonic advance still works after replays (delta 30s -> ROUND(1.5)=2)
	next := charge(t, sess.SessionId, 90)
	if !next.Applied || next.ChargedSeconds != 90 {
		t.Fatalf("advance failed: %+v", next)
	}
	if got, want := ledgerCharged(t, acc), int64(5); got != want {
		t.Fatalf("ledger sum %d, want %d", got, want)
	}
}

// TestPostpaidCreditLimit: postpaid can charge into the credit limit but no
// further.
func TestPostpaidCreditLimit(t *testing.T) {
	truncate(t)
	seed(t, 0)
	ctx := context.Background()
	if _, err := pool.Exec(ctx,
		`UPDATE accounts SET kind='postpaid', credit_limit=2 WHERE account_id='acc-test'`); err != nil {
		t.Fatal(err)
	}
	sess := authorize(t, "acc-test", "27821234567") // 0.05/sec
	if !sess.Allow || sess.MaxSeconds != 40 {
		t.Fatalf("want allow 40s (2.0 minor / 0.05), got %+v", sess)
	}
	ch := charge(t, sess.SessionId, 40)
	if !ch.Applied || ch.BalanceMinor != -2 {
		t.Fatalf("want applied to -2, got %+v", ch)
	}
	// past credit limit: deny
	sess2 := authorize(t, "acc-test", "27821234567")
	if sess2.Allow {
		t.Fatal("expected deny past credit limit")
	}
}

// TestExhaustionTermination: when a charge exceeds funds the response denies,
// which is the switch's signal to terminate within 1s (plan P1 gate).
// Balance 2 minor affords 40 reserved seconds; with per-event rounding to the
// nearest minor unit the denial lands at total_seconds=51 (cost 2.55 -> 3).
func TestExhaustionTermination(t *testing.T) {
	truncate(t)
	acc := seed(t, 2)
	sess := authorize(t, acc, "27821234567")
	if !sess.Allow || sess.MaxSeconds != 40 {
		t.Fatalf("want allow 40s, got %+v", sess)
	}
	ch := charge(t, sess.SessionId, 51)
	if !ch.Denied || ch.Applied {
		t.Fatalf("want denied, got %+v", ch)
	}
	if got, want := balance(t, acc), int64(2); got != want {
		t.Fatalf("balance %d, want unchanged %d", got, want)
	}
}

// TestFailClosedOnCatalogueOutage: an unreachable Catalogue denies
// authorization (internal_error), never allows unrated calls.
func TestFailClosedOnCatalogueOutage(t *testing.T) {
	truncate(t)
	seed(t, 100000)
	deadConn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(pool, cataloguepb.NewCatalogueClient(deadConn))
	resp, err := s.Authorize(context.Background(),
		&pb.AuthorizeRequest{AccountId: "acc-test", Destination: "27821234567"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Allow || resp.Reason != reasonInternalError {
		t.Fatalf("want internal_error deny, got %+v", resp)
	}
}
