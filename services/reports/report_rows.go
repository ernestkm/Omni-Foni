package main

import (
	"strconv"

	pb "omnifoni/reports/gen/reportspb"
)

func cdrCSVRow(c *pb.Cdr) []string {
	return []string{
		c.GetCallId(), c.GetAccountId(), c.GetDestination(), c.GetCli(),
		strconv.FormatInt(c.GetStartedAtMs(), 10), strconv.FormatInt(c.GetEndedAtMs(), 10),
		strconv.FormatInt(c.GetChargedSeconds(), 10), c.GetEndReason(), c.GetTrunkId(),
		c.GetRatePerSec(), strconv.FormatInt(c.GetAmountMinor(), 10),
		strconv.FormatBool(c.GetRateMissing()),
	}
}

func summaryCSVRow(r *pb.SummaryRow) []string {
	return []string{r.GetKey(), strconv.FormatInt(r.GetCalls(), 10),
		strconv.FormatInt(r.GetSeconds(), 10), strconv.FormatInt(r.GetAmountMinor(), 10)}
}

func summaryTotalRow(r *pb.SummaryRow) []string {
	return []string{"TOTAL", strconv.FormatInt(r.GetCalls(), 10),
		strconv.FormatInt(r.GetSeconds(), 10), strconv.FormatInt(r.GetAmountMinor(), 10)}
}

func paymentCSVRow(p *pb.Payment) []string {
	return []string{
		strconv.FormatInt(p.GetPaymentId(), 10), p.GetEventId(), p.GetAccountId(),
		p.GetGateway(), strconv.FormatInt(p.GetAmountMinor(), 10), p.GetStatus(),
		strconv.FormatInt(p.GetCreatedAtMs(), 10),
	}
}

func chargeCSVRow(c *pb.Charge) []string {
	return []string{
		c.GetSessionId(), c.GetAccountId(),
		strconv.FormatInt(c.GetChargedSeconds(), 10), strconv.FormatInt(c.GetAmountMinor(), 10),
		strconv.FormatInt(c.GetBalanceAfter(), 10), strconv.FormatInt(c.GetLastChargedAtMs(), 10),
	}
}

func balanceCSVRow(b *pb.BalanceRow) []string {
	return []string{
		b.GetAccountId(), b.GetKind(), strconv.FormatInt(b.GetBalance(), 10),
		strconv.FormatInt(b.GetCreditLimit(), 10), b.GetCurrency(),
	}
}
