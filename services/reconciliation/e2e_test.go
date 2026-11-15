package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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

func stackBrokers() []string {
	if s := os.Getenv("RECONCILIATION_KAFKA"); s != "" {
		return []string{s}
	}
	return []string{"localhost:9092"}
}

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
	t.Cleanup(func() { bConn.Close(); pConn.Close() })
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

	pub := &fakePublisher{}
	srv := NewServer(bc, pc, pub)
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
	if len(pub.events) != 0 {
		t.Fatalf("RPCs published %d drift events, want 0", len(pub.events))
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
	// a run-unique amount — both dimensions drift. The unique amount means a
	// stale record from an earlier run cannot satisfy the assertions below.
	expectedMinor := int64(1_000_000 + time.Now().UnixNano()%1_000_000)
	seedLine(t, bpool, "e2e-drift-1", "acc-x", period, 50)
	seedLedger(t, bpool, period, 999, expectedMinor)

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
				ev.GetKind() == KindLedgerAmount && ev.GetExpectedMinor() == expectedMinor {
				found = ev
			}
		})
		if found != nil {
			if found.GetActualMinor() != 50 ||
				found.GetDeltaMinor() != 50-expectedMinor || found.GetExpectedCount() != 999 || found.GetActualCount() != 1 ||
				found.GetDetectedAtMs() != now.UnixMilli() {
				t.Fatalf("drift event fields wrong: %+v", found)
			}
			return
		}
	}
	t.Fatal("drift event for 2026-11 not observed on reconciliation.drift")
}
