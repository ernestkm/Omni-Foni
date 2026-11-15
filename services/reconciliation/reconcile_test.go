package main

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

type fakeBilling struct {
	count, sum int64
	totals     map[string]int64
	counts     map[string]int64
	accounts   []string
	err        error
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
		name              string
		invoice, payments int64
		want              string
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
