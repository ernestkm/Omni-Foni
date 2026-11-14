package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pb "omnifoni/billing/gen/billingpb"
)

// billingServer serves invoice queries over the billing DB.
type billingServer struct {
	pb.UnimplementedBillingServer
	pool *pgxpool.Pool
}

// GetInvoice returns one account's invoice for a period.
func (s *billingServer) GetInvoice(ctx context.Context, req *pb.GetInvoiceRequest) (*pb.GetInvoiceResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	sum, err := Invoice(ctx, s.pool, req.GetAccountId(), req.GetPeriod())
	if err != nil {
		return nil, err
	}
	resp := &pb.GetInvoiceResponse{AccountId: sum.AccountID, Period: sum.Period,
		TotalMinor: sum.TotalMinor, LineCount: sum.LineCount}
	for _, l := range sum.Lines {
		resp.Lines = append(resp.Lines, &pb.InvoiceLine{
			CallId: l.CallID, ChargedSeconds: l.ChargedSeconds,
			AmountMinor: l.AmountMinor, RatePerSec: l.RatePerSec})
	}
	return resp, nil
}

// GetInvoicesByAccounts returns per-account totals for a batch of accounts.
// Accounts with no lines in the period are omitted, not errors.
func (s *billingServer) GetInvoicesByAccounts(ctx context.Context, req *pb.GetInvoicesByAccountsRequest) (*pb.GetInvoicesByAccountsResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	if len(req.GetAccountIds()) == 0 {
		return &pb.GetInvoicesByAccountsResponse{}, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT account_id, SUM(amount_minor)::bigint, COUNT(*)
		 FROM invoice_lines
		 WHERE account_id = ANY($1) AND period = $2
		 GROUP BY account_id ORDER BY account_id`,
		req.GetAccountIds(), req.GetPeriod())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resp := &pb.GetInvoicesByAccountsResponse{}
	for rows.Next() {
		var t pb.AccountTotal
		if err := rows.Scan(&t.AccountId, &t.TotalMinor, &t.LineCount); err != nil {
			return nil, err
		}
		resp.Totals = append(resp.Totals, &t)
	}
	return resp, rows.Err()
}

// GetPeriodLedger returns the rated-CDR ingest ledger for one period; an
// unseen period is zeros, not an error.
func (s *billingServer) GetPeriodLedger(ctx context.Context, req *pb.GetPeriodLedgerRequest) (*pb.GetPeriodLedgerResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	var count, sum int64
	err := s.pool.QueryRow(ctx,
		`SELECT cdr_count, amount_sum FROM cdr_period_totals WHERE period = $1`,
		req.GetPeriod()).Scan(&count, &sum)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	return &pb.GetPeriodLedgerResponse{Period: req.GetPeriod(), CdrCount: count, AmountSum: sum}, nil
}

// ListAccountsForPeriod returns the distinct account ids with invoice lines
// in one period.
func (s *billingServer) ListAccountsForPeriod(ctx context.Context, req *pb.ListAccountsForPeriodRequest) (*pb.ListAccountsForPeriodResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT account_id FROM invoice_lines WHERE period = $1 ORDER BY account_id`,
		req.GetPeriod())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resp := &pb.ListAccountsForPeriodResponse{}
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		resp.AccountIds = append(resp.AccountIds, a)
	}
	return resp, rows.Err()
}

func validPeriod(p string) error {
	if len(p) != 7 || p[4] != '-' {
		return fmt.Errorf("period must be YYYY-MM")
	}
	for i, r := range p {
		if i == 4 {
			continue
		}
		if r < '0' || r > '9' {
			return fmt.Errorf("period must be YYYY-MM")
		}
	}
	m := int(p[5]-'0')*10 + int(p[6]-'0')
	if m < 1 || m > 12 {
		return fmt.Errorf("period month invalid")
	}
	return nil
}
