package main

import (
	"context"

	billingpb "omnifoni/billing/gen/billingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
)

// BillingClient is the subset of the billing API reconciliation needs.
type BillingClient interface {
	// GetPeriodLedger returns (cdr_count, amount_sum) for one period.
	GetPeriodLedger(ctx context.Context, period string) (int64, int64, error)
	// InvoiceTotals returns per-account invoice totals and line counts.
	InvoiceTotals(ctx context.Context, period string) (totals map[string]int64, counts map[string]int64, err error)
	// InvoiceTotal returns one account's invoice total and line count.
	InvoiceTotal(ctx context.Context, accountID, period string) (int64, int64, error)
	// ListAccounts returns distinct account ids with invoice lines.
	ListAccounts(ctx context.Context, period string) ([]string, error)
}

// PaymentsClient is the subset of the payments API reconciliation needs.
type PaymentsClient interface {
	// TotalsByAccount returns per-account recorded-payment sums.
	TotalsByAccount(ctx context.Context, period string) (map[string]int64, error)
	// ListAccounts returns distinct account ids with recorded payments.
	ListAccounts(ctx context.Context, period string) ([]string, error)
}

type billingAdapter struct {
	client billingpb.BillingClient
}

func (a *billingAdapter) GetPeriodLedger(ctx context.Context, period string) (int64, int64, error) {
	resp, err := a.client.GetPeriodLedger(ctx, &billingpb.GetPeriodLedgerRequest{Period: period})
	if err != nil {
		return 0, 0, err
	}
	return resp.GetCdrCount(), resp.GetAmountSum(), nil
}

func (a *billingAdapter) InvoiceTotals(ctx context.Context, period string) (map[string]int64, map[string]int64, error) {
	accounts, err := a.ListAccounts(ctx, period)
	if err != nil {
		return nil, nil, err
	}
	if len(accounts) == 0 {
		return map[string]int64{}, map[string]int64{}, nil
	}
	resp, err := a.client.GetInvoicesByAccounts(ctx, &billingpb.GetInvoicesByAccountsRequest{
		AccountIds: accounts, Period: period})
	if err != nil {
		return nil, nil, err
	}
	totals := make(map[string]int64, len(resp.GetTotals()))
	counts := make(map[string]int64, len(resp.GetTotals()))
	for _, t := range resp.GetTotals() {
		totals[t.GetAccountId()] = t.GetTotalMinor()
		counts[t.GetAccountId()] = t.GetLineCount()
	}
	return totals, counts, nil
}

func (a *billingAdapter) InvoiceTotal(ctx context.Context, accountID, period string) (int64, int64, error) {
	resp, err := a.client.GetInvoice(ctx, &billingpb.GetInvoiceRequest{AccountId: accountID, Period: period})
	if err != nil {
		return 0, 0, err
	}
	return resp.GetTotalMinor(), resp.GetLineCount(), nil
}

func (a *billingAdapter) ListAccounts(ctx context.Context, period string) ([]string, error) {
	resp, err := a.client.ListAccountsForPeriod(ctx, &billingpb.ListAccountsForPeriodRequest{Period: period})
	if err != nil {
		return nil, err
	}
	return resp.GetAccountIds(), nil
}

type paymentsAdapter struct {
	client paymentspb.PaymentsClient
}

func (a *paymentsAdapter) TotalsByAccount(ctx context.Context, period string) (map[string]int64, error) {
	resp, err := a.client.GetPaymentsTotals(ctx, &paymentspb.GetPaymentsTotalsRequest{Period: period})
	if err != nil {
		return nil, err
	}
	totals := make(map[string]int64, len(resp.GetTotals()))
	for _, t := range resp.GetTotals() {
		totals[t.GetAccountId()] = t.GetAmountSum()
	}
	return totals, nil
}

func (a *paymentsAdapter) ListAccounts(ctx context.Context, period string) ([]string, error) {
	resp, err := a.client.ListAccountsForPeriod(ctx, &paymentspb.ListAccountsForPeriodRequest{Period: period})
	if err != nil {
		return nil, err
	}
	return resp.GetAccountIds(), nil
}
