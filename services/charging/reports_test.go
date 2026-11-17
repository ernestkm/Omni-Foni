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
		account string
		amount  int64
		balance int64
		at      time.Time
	}{
		{s1, "acc-c", -1, 999, base},
		{s1, "acc-c", -1, 998, base.Add(time.Second)},
		{s1, "acc-c", -2, 996, base.Add(2 * time.Second)},
		{s2, "acc-d", -1, 999, base.Add(time.Hour)},
	}
	for _, l := range ledger {
		if _, err := pool.Exec(ctx,
			`INSERT INTO ledger (session_id, account_id, amount_minor, balance_after, created_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			l.session, l.account, l.amount, l.balance, l.at); err != nil {
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
