package main

import (
	"context"
	"fmt"
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
	for i, a := range []string{"acc-b", "acc-a", "acc-b"} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO invoice_lines (call_id, account_id, period, charged_seconds, amount_minor, rate_per_sec)
			 VALUES ($1, $2, '2026-11', 1, 1, 1)`, fmt.Sprintf("call-%s-%d", a, i), a); err != nil {
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
