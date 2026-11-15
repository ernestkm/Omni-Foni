package main

import (
	"fmt"
	"strconv"
	"time"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

// Statuses for the two comparison kinds.
const (
	StatusOk         = "ok"
	StatusDrift      = "drift"
	StatusPaid       = "paid"
	StatusUnderpaid  = "underpaid"
	StatusOverpaid   = "overpaid"
	StatusUnpaid     = "unpaid"
	StatusNoActivity = "no_activity"
)

// Drift kinds for DriftEvent.
const (
	KindLedgerCount  = "ledger_count_mismatch"
	KindLedgerAmount = "ledger_amount_mismatch"
)

// CompareLedger is period-wide: the billing ingest ledger (expected) against
// the invoice_lines SUM/COUNT (actual). Both dimensions must match for ok.
func CompareLedger(period string, ledgerCount, ledgerSum, invoiceCount, invoiceSum int64) *pb.LedgerComparison {
	status := StatusOk
	if ledgerCount != invoiceCount || ledgerSum != invoiceSum {
		status = StatusDrift
	}
	return &pb.LedgerComparison{
		Period:              period,
		ExpectedCount:       ledgerCount,
		ActualCount:         invoiceCount,
		ExpectedAmountMinor: ledgerSum,
		ActualAmountMinor:   invoiceSum,
		Status:              status,
	}
}

// ComparePayments is per account: recorded payments against the invoice
// total. The delta is a business state, not drift.
func ComparePayments(accountID, period string, invoiceTotal, paymentsTotal int64) *pb.PaymentComparison {
	var status string
	switch {
	case invoiceTotal == 0 && paymentsTotal == 0:
		status = StatusNoActivity
	case paymentsTotal == invoiceTotal:
		status = StatusPaid
	case paymentsTotal > invoiceTotal:
		status = StatusOverpaid
	case paymentsTotal == 0:
		status = StatusUnpaid
	default:
		status = StatusUnderpaid
	}
	return &pb.PaymentComparison{
		AccountId:          accountID,
		Period:             period,
		InvoiceTotalMinor:  invoiceTotal,
		PaymentsTotalMinor: paymentsTotal,
		DeltaMinor:         paymentsTotal - invoiceTotal,
		Status:             status,
	}
}

// DriftKinds lists which ledger dimensions mismatch, one DriftEvent kind per
// differing dimension.
func DriftKinds(c *pb.LedgerComparison) []string {
	var kinds []string
	if c.GetExpectedCount() != c.GetActualCount() {
		kinds = append(kinds, KindLedgerCount)
	}
	if c.GetExpectedAmountMinor() != c.GetActualAmountMinor() {
		kinds = append(kinds, KindLedgerAmount)
	}
	return kinds
}

// PeriodOf renders a time as the reconciliation period YYYY-MM in UTC.
func PeriodOf(t time.Time) string {
	u := t.UTC()
	return strconv.Itoa(u.Year()) + "-" + pad2(int(u.Month()))
}

func pad2(n int) string {
	if n < 10 {
		return "0" + strconv.Itoa(n)
	}
	return strconv.Itoa(n)
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
