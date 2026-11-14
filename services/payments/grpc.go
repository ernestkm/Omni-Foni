package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	pb "omnifoni/payments/gen/paymentspb"
)

// paymentsServer serves reconciliation queries over the payments DB.
type paymentsServer struct {
	pb.UnimplementedPaymentsServer
	pool *pgxpool.Pool
}

// GetPaymentsTotals returns per-account payment totals for one period
// (YYYY-MM of created_at in UTC). Only recorded payments count; accounts
// with no payments in the period are omitted.
func (s *paymentsServer) GetPaymentsTotals(ctx context.Context, req *pb.GetPaymentsTotalsRequest) (*pb.GetPaymentsTotalsResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT account_id, COUNT(*)::bigint, SUM(amount_minor)::bigint
		 FROM payments
		 WHERE status = 'recorded' AND to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM') = $1
		 GROUP BY account_id ORDER BY account_id`,
		req.GetPeriod())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resp := &pb.GetPaymentsTotalsResponse{}
	for rows.Next() {
		var t pb.PaymentsTotal
		if err := rows.Scan(&t.AccountId, &t.PaymentCount, &t.AmountSum); err != nil {
			return nil, err
		}
		resp.Totals = append(resp.Totals, &t)
	}
	return resp, rows.Err()
}

// ListAccountsForPeriod returns the distinct account ids with recorded
// payments in one period.
func (s *paymentsServer) ListAccountsForPeriod(ctx context.Context, req *pb.ListAccountsForPeriodRequest) (*pb.ListAccountsForPeriodResponse, error) {
	if err := validPeriod(req.GetPeriod()); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT account_id FROM payments
		 WHERE status = 'recorded' AND to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM') = $1
		 ORDER BY account_id`,
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

// validPeriod mirrors billing's period rule: strict YYYY-MM.
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
