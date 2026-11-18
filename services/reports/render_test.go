package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "omnifoni/reports/gen/reportspb"
)

type renderAPI struct {
	API
	cdrs     []*pb.Cdr
	summary  *pb.SummarizeResponse
	payments []*pb.Payment
	charges  []*pb.Charge
	balances []*pb.BalanceRow
}

func (a *renderAPI) ListCdrs(context.Context, *pb.ListCdrsRequest) (*pb.ListCdrsResponse, error) {
	return &pb.ListCdrsResponse{Cdrs: a.cdrs}, nil
}

func (a *renderAPI) Summarize(context.Context, *pb.SummarizeRequest) (*pb.SummarizeResponse, error) {
	return a.summary, nil
}

func (a *renderAPI) ListPayments(context.Context, *pb.ListPaymentsRequest) (*pb.ListPaymentsResponse, error) {
	return &pb.ListPaymentsResponse{Payments: a.payments}, nil
}

func (a *renderAPI) ListCharges(context.Context, *pb.ListChargesRequest) (*pb.ListChargesResponse, error) {
	return &pb.ListChargesResponse{Charges: a.charges}, nil
}

func (a *renderAPI) ListLowBalance(context.Context, *pb.LowBalanceRequest) (*pb.LowBalanceResponse, error) {
	return &pb.LowBalanceResponse{Accounts: a.balances}, nil
}

// countingFailAPI records any attempt to reach the internal report APIs. A
// schedule without an account scope must fail closed before paging, never call
// them (an empty filter means "all accounts").
type countingFailAPI struct {
	API
	calls int
}

func (a *countingFailAPI) fail(name string) error {
	a.calls++
	return errors.New("unexpected " + name)
}

func (a *countingFailAPI) ListCdrs(context.Context, *pb.ListCdrsRequest) (*pb.ListCdrsResponse, error) {
	return nil, a.fail("ListCdrs")
}

func (a *countingFailAPI) Summarize(context.Context, *pb.SummarizeRequest) (*pb.SummarizeResponse, error) {
	return nil, a.fail("Summarize")
}

func (a *countingFailAPI) ListPayments(context.Context, *pb.ListPaymentsRequest) (*pb.ListPaymentsResponse, error) {
	return nil, a.fail("ListPayments")
}

func (a *countingFailAPI) ListCharges(context.Context, *pb.ListChargesRequest) (*pb.ListChargesResponse, error) {
	return nil, a.fail("ListCharges")
}

func (a *countingFailAPI) ListLowBalance(context.Context, *pb.LowBalanceRequest) (*pb.LowBalanceResponse, error) {
	return nil, a.fail("ListLowBalance")
}

func TestRenderReportCSVEmptyAccountScopeFailsClosed(t *testing.T) {
	cases := []struct {
		kind   string
		header []string
	}{
		{"cdrs", cdrCSVHeader},
		{"summary", summaryCSVHeader},
		{"payments", paymentCSVHeader},
		{"charges", chargeCSVHeader},
		{"low_balance", balanceCSVHeader},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			api := &countingFailAPI{}
			body, rows, err := renderReportCSV(context.Background(), api,
				Schedule{ReportKind: tc.kind}, time.Time{}, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if api.calls != 0 {
				t.Fatalf("api calls = %d, want 0", api.calls)
			}
			if rows != 0 {
				t.Fatalf("rows = %d, want 0", rows)
			}
			if want := strings.Join(tc.header, ",") + "\n"; body != want {
				t.Fatalf("body = %q, want header-only %q", body, want)
			}
		})
	}
}

func TestRenderReportCSVSummaryAppendsTotal(t *testing.T) {
	api := &renderAPI{summary: &pb.SummarizeResponse{
		Rows: []*pb.SummaryRow{
			{Key: "a", Calls: 1, Seconds: 60, AmountMinor: 10},
			{Key: "b", Calls: 1, Seconds: 30, AmountMinor: 5},
		},
		Totals: &pb.SummaryRow{Calls: 2, Seconds: 90, AmountMinor: 15},
	}}
	body, rows, err := renderReportCSV(context.Background(), api,
		Schedule{ReportKind: "summary", GroupBy: "prefix", PrefixLen: 3, AccountIDs: []string{"a1"}}, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("rows = %d, want 2", rows)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 4 {
		t.Fatalf("lines = %q", lines)
	}
	if lines[0] != strings.Join(summaryCSVHeader, ",") {
		t.Fatalf("header = %q", lines[0])
	}
	if lines[len(lines)-1] != "TOTAL,2,90,15" {
		t.Fatalf("total row = %q", lines[len(lines)-1])
	}
}

func TestRenderReportCSVSummaryTotalSurvivesRowCap(t *testing.T) {
	const groups = csvMaxRows + 1
	rows := make([]*pb.SummaryRow, 0, groups)
	var calls, seconds, amount int64
	for i := 0; i < groups; i++ {
		rows = append(rows, &pb.SummaryRow{
			Key: fmt.Sprintf("k%05d", i), Calls: 1, Seconds: int64(i), AmountMinor: int64(i),
		})
		calls++
		seconds += int64(i)
		amount += int64(i)
	}
	api := &renderAPI{summary: &pb.SummarizeResponse{
		Rows:   rows,
		Totals: &pb.SummaryRow{Calls: calls, Seconds: seconds, AmountMinor: amount},
	}}
	body, n, err := renderReportCSV(context.Background(), api,
		Schedule{ReportKind: "summary", GroupBy: "prefix", PrefixLen: 3, AccountIDs: []string{"a1"}}, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if n != csvMaxRows {
		t.Fatalf("rows = %d, want %d", n, csvMaxRows)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != csvMaxRows+2 {
		t.Fatalf("lines = %d, want %d", len(lines), csvMaxRows+2)
	}
	want := "TOTAL," + strconv.FormatInt(calls, 10) + "," +
		strconv.FormatInt(seconds, 10) + "," + strconv.FormatInt(amount, 10)
	if lines[len(lines)-1] != want {
		t.Fatalf("total row = %q, want %q", lines[len(lines)-1], want)
	}
}

func TestRenderReportCSVHeadersAndEmptyResults(t *testing.T) {
	cases := []struct {
		name string
		sch  Schedule
		api  *renderAPI
		want []string
		rows int64
	}{
		{"cdrs empty", Schedule{ReportKind: "cdrs", AccountIDs: []string{"a1"}}, &renderAPI{}, cdrCSVHeader, 0},
		{"payments", Schedule{ReportKind: "payments", AccountIDs: []string{"a1"}}, &renderAPI{payments: []*pb.Payment{{
			PaymentId: 1, EventId: "e1", AccountId: "a1", Gateway: "stripe",
			AmountMinor: 10, Status: "recorded", CreatedAtMs: 1000,
		}}}, paymentCSVHeader, 1},
		{"charges", Schedule{ReportKind: "charges", AccountIDs: []string{"a1"}}, &renderAPI{charges: []*pb.Charge{{
			SessionId: "s1", AccountId: "a1", ChargedSeconds: 60,
			AmountMinor: 10, BalanceAfter: 90, LastChargedAtMs: 1000,
		}}}, chargeCSVHeader, 1},
		{"low balance", Schedule{ReportKind: "low_balance", AccountIDs: []string{"a1"}}, &renderAPI{balances: []*pb.BalanceRow{{
			AccountId: "a1", Kind: "prepaid", Balance: -5, CreditLimit: 0, Currency: "USD",
		}}}, balanceCSVHeader, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, rows, err := renderReportCSV(context.Background(), tc.api, tc.sch, time.Time{}, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if rows != tc.rows {
				t.Fatalf("rows = %d, want %d", rows, tc.rows)
			}
			lines := strings.Split(strings.TrimSuffix(body, "\n"), "\n")
			if lines[0] != strings.Join(tc.want, ",") {
				t.Fatalf("header = %q", lines[0])
			}
			if tc.rows == 0 && len(lines) != 1 {
				t.Fatalf("empty result lines = %q", lines)
			}
		})
	}
}
