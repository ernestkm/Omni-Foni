package main

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chargingpb "omnifoni/charging/gen/chargingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
	pb "omnifoni/reports/gen/reportspb"
)

type fakePayments struct {
	req  *paymentspb.ListPaymentsRequest
	resp *paymentspb.ListPaymentsResponse
	err  error
}

func (f *fakePayments) ListPayments(_ context.Context, req *paymentspb.ListPaymentsRequest) (*paymentspb.ListPaymentsResponse, error) {
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

type fakeCharges struct {
	chargesReq *chargingpb.ListChargesRequest
	balanceReq *chargingpb.ListLowBalanceRequest
	resp       *chargingpb.ListChargesResponse
	balance    *chargingpb.ListLowBalanceResponse
	err        error
}

func (f *fakeCharges) ListCharges(_ context.Context, req *chargingpb.ListChargesRequest) (*chargingpb.ListChargesResponse, error) {
	f.chargesReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func (f *fakeCharges) ListLowBalance(_ context.Context, req *chargingpb.ListLowBalanceRequest) (*chargingpb.ListLowBalanceResponse, error) {
	f.balanceReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.balance, nil
}

func TestReportsListPayments(t *testing.T) {
	fp := &fakePayments{resp: &paymentspb.ListPaymentsResponse{
		Payments: []*paymentspb.Payment{{
			PaymentId: 7, EventId: "evt", AccountId: "a1", Gateway: "stripe",
			AmountMinor: 500, Status: "recorded", CreatedAtMs: 1000,
		}},
		NextCursor: "next",
	}}
	srv := &reportsServer{payments: fp}

	resp, err := srv.ListPayments(context.Background(), &pb.ListPaymentsRequest{
		Filter: &pb.MoneyFilter{AccountIds: []string{"a1"}, From: "2026-11-17", To: "2026-11-17"},
		Limit:  5000, Cursor: "cur", Status: "recorded",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := fp.req.GetLimit(); got != 1000 {
		t.Fatalf("forwarded limit = %d, want 1000 (clamped)", got)
	}
	if got := fp.req.GetFromMs(); got != 1794873600000 {
		t.Fatalf("from_ms = %d, want 1794873600000", got)
	}
	if got := fp.req.GetToMs(); got != 1794960000000 {
		t.Fatalf("to_ms = %d, want 1794960000000 (whole day)", got)
	}
	if fp.req.GetStatus() != "recorded" || fp.req.GetCursor() != "cur" {
		t.Fatalf("forwarded req = %+v", fp.req)
	}
	if len(resp.GetPayments()) != 1 || resp.GetPayments()[0].GetAmountMinor() != 500 ||
		resp.GetNextCursor() != "next" {
		t.Fatalf("resp = %+v", resp)
	}

	if _, err := srv.ListPayments(context.Background(), &pb.ListPaymentsRequest{
		Filter: &pb.MoneyFilter{From: "2026-11-17"}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing to err = %v, want InvalidArgument", err)
	}

	fp.err = status.Error(codes.Unavailable, "down")
	if _, err := srv.ListPayments(context.Background(), &pb.ListPaymentsRequest{
		Filter: &pb.MoneyFilter{From: "2026-11-17", To: "2026-11-18"}}); status.Code(err) != codes.Unavailable {
		t.Fatalf("source error = %v, want Unavailable", err)
	}
}

func TestReportsListChargesAndLowBalance(t *testing.T) {
	fc := &fakeCharges{
		resp: &chargingpb.ListChargesResponse{Charges: []*chargingpb.Charge{{
			SessionId: "s1", AccountId: "a1", ChargedSeconds: 3,
			AmountMinor: -4, BalanceAfter: 996, LastChargedAtMs: 2000,
		}}},
		balance: &chargingpb.ListLowBalanceResponse{Accounts: []*chargingpb.BalanceRow{{
			AccountId: "a1", Kind: "prepaid", Balance: 0, CreditLimit: 0, Currency: "ZAR",
		}}},
	}
	srv := &reportsServer{charges: fc}

	resp, err := srv.ListCharges(context.Background(), &pb.ListChargesRequest{
		Filter: &pb.MoneyFilter{AccountIds: []string{"a1"}, From: "2026-11-17", To: "2026-11-18"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetCharges()) != 1 || resp.GetCharges()[0].GetAmountMinor() != -4 {
		t.Fatalf("charges resp = %+v", resp)
	}
	if fc.chargesReq.GetFromMs() == 0 || fc.chargesReq.GetToMs() == 0 {
		t.Fatalf("charges window not forwarded: %+v", fc.chargesReq)
	}

	low, err := srv.ListLowBalance(context.Background(), &pb.LowBalanceRequest{
		AccountIds: []string{"a1"}, ThresholdMinor: -5, Limit: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(low.GetAccounts()) != 1 || low.GetAccounts()[0].GetCurrency() != "ZAR" {
		t.Fatalf("low balance resp = %+v", low)
	}
	if fc.balanceReq.GetThresholdMinor() != -5 || fc.balanceReq.GetLimit() != 100 {
		t.Fatalf("low balance req = %+v", fc.balanceReq)
	}
}
