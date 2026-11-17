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
		{"list-evt-1", "acc-a", "recorded", 100, base},
		{"list-evt-2", "acc-a", "recorded", 200, base.Add(time.Minute)},
		{"list-evt-3", "acc-a", "recorded", 300, base.Add(2 * time.Minute)},
		{"list-evt-void", "acc-b", "void", 999, base.Add(time.Minute)},
		{"list-evt-out", "acc-a", "recorded", 400, base.Add(3 * time.Hour)},
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
	if got := page1.GetPayments()[0].GetEventId(); got != "list-evt-3" {
		t.Fatalf("page1[0] = %q, want list-evt-3 (newest first)", got)
	}
	if got := page1.GetPayments()[1].GetEventId(); got != "list-evt-2" {
		t.Fatalf("page1[1] = %q, want list-evt-2", got)
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
	if len(page2.GetPayments()) != 1 || page2.GetPayments()[0].GetEventId() != "list-evt-1" {
		t.Fatalf("page2 = %v, want only list-evt-1", page2.GetPayments())
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
