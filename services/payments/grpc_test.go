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
