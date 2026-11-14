package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	mediationpb "omnifoni/mediation/gen/mediationpb"
)

// Integration tests need the dev data layer: Kafka on localhost:9092, the
// Catalogue on :50053, and the billing Postgres DB (make up).
func dsn() string {
	if s := os.Getenv("BILLING_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/billing"
}

func catDSN() string {
	if s := os.Getenv("BILLING_TEST_CATALOGUE_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/catalogue"
}

var (
	pool     *pgxpool.Pool
	catPool  *pgxpool.Pool
	cat      cataloguepb.CatalogueClient
	producer *kgo.Client
)

func brokers() []string { return []string{"localhost:9092"} }

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
	cc, err := grpc.NewClient("localhost:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "catalogue client:", err)
		os.Exit(1)
	}
	cat = cataloguepb.NewCatalogueClient(cc)
	producer, err = kgo.NewClient(kgo.SeedBrokers(brokers()...), kgo.AllowAutoTopicCreation())
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func cdr(id string) *mediationpb.CallCdr {
	return &mediationpb.CallCdr{
		CallId:        id,
		AccountId:     "acc-inv",
		Destination:   "3555999",
		Cli:           "2721222",
		StartedAtMs:   time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC).UnixMilli(),
		EndedAtMs:     time.Date(2026, 11, 5, 12, 1, 0, 0, time.UTC).UnixMilli(),
		ChargedSeconds: 60,
		EndReason:     "normal",
	}
}

func publish(t *testing.T, cdr *mediationpb.CallCdr) {
	t.Helper()
	b, err := proto.Marshal(cdr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := producer.ProduceSync(ctx, &kgo.Record{
		Topic: "cdrs.normalized", Key: []byte(cdr.CallId), Value: b}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// waitLines polls the billing DB until the account has n lines or times out.
func waitLines(t *testing.T, n int64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var got int64
		if err := pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM invoice_lines WHERE account_id='acc-inv'`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got >= n {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d lines", n)
}

// TestReplayOneLine: the same CDR published twice must produce exactly one
// invoice line with the rated amount (P2 money gate: duplicates never bill).
func TestReplayOneLine(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE invoice_lines`); err != nil {
		t.Fatal(err)
	}
	if _, err := catPool.Exec(ctx, `TRUNCATE rates`); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.PutRate(ctx, &cataloguepb.PutRateRequest{Prefix: "3555", RatePerSec: "0.500000"}); err != nil {
		t.Fatal(err)
	}

	c := cdr("bill-replay-1")
	publish(t, c)
	publish(t, c) // at-least-once replay
	waitLines(t, 1)

	var lines, seconds, amount int64
	var rate string
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*), MAX(charged_seconds), MAX(amount_minor), MAX(rate_per_sec::text)
		 FROM invoice_lines WHERE call_id = $1`, c.CallId).
		Scan(&lines, &seconds, &amount, &rate); err != nil {
		t.Fatal(err)
	}
	if lines != 1 {
		t.Fatalf("line count %d, want 1", lines)
	}
	// ROUND(60 * 0.5) = 30 minor
	if amount != 30 {
		t.Fatalf("amount %d, want 30", amount)
	}
}

// TestInvoiceReconciliation: invoice total must equal the sum of its rated
// lines, to the cent, through the gRPC-facing query path.
func TestInvoiceReconciliation(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE invoice_lines`); err != nil {
		t.Fatal(err)
	}
	if _, err := catPool.Exec(ctx, `TRUNCATE rates`); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.PutRate(ctx, &cataloguepb.PutRateRequest{Prefix: "3555", RatePerSec: "0.333333"}); err != nil {
		t.Fatal(err)
	}

	// 37s * 0.333333 = 12.333321 -> 12; 90s * 0.333333 = 29.99997 -> 30
	publish(t, &mediationpb.CallCdr{CallId: "bill-rec-1", AccountId: "acc-inv",
		Destination: "3555999", StartedAtMs: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC).UnixMilli(),
		ChargedSeconds: 37, EndReason: "normal"})
	publish(t, &mediationpb.CallCdr{CallId: "bill-rec-2", AccountId: "acc-inv",
		Destination: "3555999", StartedAtMs: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC).UnixMilli(),
		ChargedSeconds: 90, EndReason: "balance_exhausted"})
	waitLines(t, 2)

	sum, err := Invoice(ctx, pool, "acc-inv", "2026-11")
	if err != nil {
		t.Fatal(err)
	}
	if sum.LineCount != 2 {
		t.Fatalf("line count %d, want 2", sum.LineCount)
	}
	var want int64
	for _, l := range sum.Lines {
		want += l.AmountMinor
	}
	if sum.TotalMinor != want {
		t.Fatalf("invoice total %d != lines sum %d", sum.TotalMinor, want)
	}
	if sum.TotalMinor != 42 {
		t.Fatalf("total %d, want 42 (12+30)", sum.TotalMinor)
	}
}

// TestNoRateDeadLetter: a CDR with an unmatched destination must reach the
// billing dead-letter topic with a reason header, not vanish or stall.
func TestNoRateDeadLetter(t *testing.T) {
	ctx := context.Background()
	if _, err := catPool.Exec(ctx, `TRUNCATE rates`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`DELETE FROM invoice_lines WHERE call_id = 'bill-norate-1'`); err != nil {
		t.Fatal(err)
	}
	publish(t, &mediationpb.CallCdr{CallId: "bill-norate-1", AccountId: "acc-inv",
		Destination: "9999999", StartedAtMs: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC).UnixMilli(),
		ChargedSeconds: 10, EndReason: "normal"})

	// Poll the dead-letter topic for the record.
	dead, err := kgo.NewClient(kgo.SeedBrokers(brokers()...),
		kgo.ConsumerGroup("billing-test-dead"),
		kgo.ConsumeTopics("cdrs.billing.rejected"),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer dead.Close()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := dead.PollRecords(pctx, 10)
		cancel()
		var found bool
		fetches.EachRecord(func(r *kgo.Record) {
			var reason string
			for _, h := range r.Headers {
				if h.Key == "reason" {
					reason = string(h.Value)
				}
			}
			c := &mediationpb.CallCdr{}
			if proto.Unmarshal(r.Value, c) == nil && c.CallId == "bill-norate-1" && reason == "no_rate" {
				found = true
			}
		})
		if found {
			return
		}
	}
	t.Fatal("dead letter for bill-norate-1 not observed")
}

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
