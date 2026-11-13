package main

import (
	"context"
	"fmt"

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
	if p[5] < '0' || p[5] > '1' || p[6] < '0' || p[6] > '9' {
		return fmt.Errorf("period month invalid")
	}
	return nil
}
