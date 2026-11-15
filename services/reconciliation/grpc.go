package main

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

// DriftPublisher sends one DriftEvent; the sweep uses it, RPCs never do.
type DriftPublisher interface {
	PublishDrift(ctx context.Context, ev *pb.DriftEvent) error
}

// Server serves the reconciliation gRPC surface.
type Server struct {
	pb.UnimplementedReconciliationServer
	billing   BillingClient
	payments  PaymentsClient
	publisher DriftPublisher
}

func NewServer(billing BillingClient, payments PaymentsClient, publisher DriftPublisher) *Server {
	return &Server{billing: billing, payments: payments, publisher: publisher}
}

// ReconcilePeriod returns the ledger comparison plus one row per account
// found on either side. It reports; it never publishes drift.
func (s *Server) ReconcilePeriod(ctx context.Context, req *pb.ReconcilePeriodRequest) (*pb.ReconcilePeriodResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return s.reconcilePeriodData(ctx, req.GetPeriod())
}

// ReconcileAccount returns both comparisons for one account; an account with
// no activity on either side is a zero no_activity row.
func (s *Server) ReconcileAccount(ctx context.Context, req *pb.ReconcileAccountRequest) (*pb.ReconcileAccountResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id required")
	}
	period := req.GetPeriod()
	resp, err := s.reconcilePeriodData(ctx, period)
	if err != nil {
		return nil, err
	}
	pc := ComparePayments(req.GetAccountId(), period, 0, 0)
	for _, row := range resp.GetAccounts() {
		if row.GetAccountId() == req.GetAccountId() {
			pc = row
			break
		}
	}
	return &pb.ReconcileAccountResponse{LedgerVsInvoices: resp.GetLedgerVsInvoices(), PaymentsVsInvoices: pc}, nil
}

// reconcilePeriodData fetches both sides and assembles the period response.
func (s *Server) reconcilePeriodData(ctx context.Context, period string) (*pb.ReconcilePeriodResponse, error) {
	ledgerCount, ledgerSum, err := s.billing.GetPeriodLedger(ctx, period)
	if err != nil {
		return nil, err
	}
	billAccounts, err := s.billing.ListAccounts(ctx, period)
	if err != nil {
		return nil, err
	}
	payAccounts, err := s.payments.ListAccounts(ctx, period)
	if err != nil {
		return nil, err
	}
	invoiceTotals, invoiceCounts, err := s.billing.InvoiceTotals(ctx, period)
	if err != nil {
		return nil, err
	}
	paymentsTotals, err := s.payments.TotalsByAccount(ctx, period)
	if err != nil {
		return nil, err
	}

	var invoiceCount, invoiceSum int64
	for _, c := range invoiceCounts {
		invoiceCount += c
	}
	for _, v := range invoiceTotals {
		invoiceSum += v
	}

	// Deterministic union: billing accounts in order, then payments-only.
	union := make([]string, 0, len(billAccounts)+len(payAccounts))
	seen := map[string]bool{}
	for _, a := range append(append([]string{}, billAccounts...), payAccounts...) {
		if !seen[a] {
			seen[a] = true
			union = append(union, a)
		}
	}
	accounts := make([]*pb.PaymentComparison, 0, len(union))
	for _, a := range union {
		accounts = append(accounts, ComparePayments(a, period, invoiceTotals[a], paymentsTotals[a]))
	}
	return &pb.ReconcilePeriodResponse{
		LedgerVsInvoices: CompareLedger(period, ledgerCount, ledgerSum, invoiceCount, invoiceSum),
		Accounts:         accounts,
	}, nil
}
