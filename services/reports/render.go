package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"time"

	pb "omnifoni/reports/gen/reportspb"
)

func reportCSVHeader(kind string) ([]string, error) {
	switch kind {
	case "cdrs":
		return cdrCSVHeader, nil
	case "summary":
		return summaryCSVHeader, nil
	case "payments":
		return paymentCSVHeader, nil
	case "charges":
		return chargeCSVHeader, nil
	case "low_balance":
		return balanceCSVHeader, nil
	}
	return nil, fmt.Errorf("unknown report_kind %q", kind)
}

// collectReportRows pages the API exactly like the HTTP CSV handlers do and
// stops at csvMaxRows data rows.
func collectReportRows(ctx context.Context, api API, sch Schedule, from, to time.Time) ([][]string, func() []string, error) {
	var rows [][]string
	var tail func() []string
	switch sch.ReportKind {
	case "cdrs":
		var cursor string
		for len(rows) < csvMaxRows {
			resp, err := api.ListCdrs(ctx, &pb.ListCdrsRequest{
				Filter: &pb.CdrFilter{
					AccountIds: sch.AccountIDs,
					From:       from.Format(time.RFC3339),
					To:         to.Format(time.RFC3339),
				},
				Limit: csvPageSize, Cursor: cursor,
			})
			if err != nil {
				return nil, nil, err
			}
			for _, c := range resp.GetCdrs() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, cdrCSVRow(c))
			}
			cursor = resp.GetNextCursor()
			if cursor == "" {
				return rows, tail, nil
			}
		}
	case "summary":
		group, _ := groupFromString(sch.GroupBy)
		prefixLen := int32(sch.PrefixLen)
		var totals *pb.SummaryRow
		for {
			resp, err := api.Summarize(ctx, &pb.SummarizeRequest{
				Filter: &pb.CdrFilter{
					AccountIds: sch.AccountIDs,
					From:       from.Format(time.RFC3339),
					To:         to.Format(time.RFC3339),
				},
				GroupBy: group, PrefixLen: prefixLen,
			})
			if err != nil {
				return nil, nil, err
			}
			totals = resp.GetTotals()
			if totals != nil {
				tr := totals
				tail = func() []string {
					return summaryTotalRow(tr)
				}
			}
			for _, row := range resp.GetRows() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, summaryCSVRow(row))
			}
			break // Summarize returns everything in one response
		}
	case "payments":
		var cursor string
		for len(rows) < csvMaxRows {
			resp, err := api.ListPayments(ctx, &pb.ListPaymentsRequest{
				Filter: &pb.MoneyFilter{
					AccountIds: sch.AccountIDs,
					From:       from.Format(time.RFC3339),
					To:         to.Format(time.RFC3339),
				},
				Limit: csvPageSize, Cursor: cursor,
			})
			if err != nil {
				return nil, nil, err
			}
			for _, p := range resp.GetPayments() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, paymentCSVRow(p))
			}
			cursor = resp.GetNextCursor()
			if cursor == "" {
				return rows, tail, nil
			}
		}
	case "charges":
		var cursor string
		for len(rows) < csvMaxRows {
			resp, err := api.ListCharges(ctx, &pb.ListChargesRequest{
				Filter: &pb.MoneyFilter{
					AccountIds: sch.AccountIDs,
					From:       from.Format(time.RFC3339),
					To:         to.Format(time.RFC3339),
				},
				Limit: csvPageSize, Cursor: cursor,
			})
			if err != nil {
				return nil, nil, err
			}
			for _, c := range resp.GetCharges() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, chargeCSVRow(c))
			}
			cursor = resp.GetNextCursor()
			if cursor == "" {
				return rows, tail, nil
			}
		}
	case "low_balance":
		var cursor string
		for len(rows) < csvMaxRows {
			resp, err := api.ListLowBalance(ctx, &pb.LowBalanceRequest{
				AccountIds: sch.AccountIDs, Limit: csvPageSize, Cursor: cursor,
			})
			if err != nil {
				return nil, nil, err
			}
			for _, b := range resp.GetAccounts() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, balanceCSVRow(b))
			}
			cursor = resp.GetNextCursor()
			if cursor == "" {
				return rows, tail, nil
			}
		}
	}
	return rows, tail, nil
}

// renderReportCSV returns the full CSV text and the data-row count.
func renderReportCSV(ctx context.Context, api API, sch Schedule, from, to time.Time) (string, int64, error) {
	header, err := reportCSVHeader(sch.ReportKind)
	if err != nil {
		return "", 0, err
	}
	rows, tail, err := collectReportRows(ctx, api, sch, from, to)
	if err != nil {
		return "", 0, err
	}
	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	if err := cw.Write(header); err != nil {
		return "", 0, err
	}
	for _, r := range rows {
		if err := cw.Write(r); err != nil {
			return "", 0, err
		}
	}
	if tail != nil {
		if err := cw.Write(tail()); err != nil {
			return "", 0, err
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return "", 0, err
	}
	return buf.String(), int64(len(rows)), nil
}
