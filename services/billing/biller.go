package main

import (
	"context"
	_ "embed"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	kgo "github.com/twmb/franz-go/pkg/kgo"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	mediationpb "omnifoni/mediation/gen/mediationpb"
)

//go:embed migrate.sql
var schemaSQL string

// Migrate applies the schema. Idempotent, safe to run at boot.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

// Biller turns normalized CDRs into deduped, rated invoice lines.
type Biller struct {
	pool          *pgxpool.Pool
	cat           cataloguepb.CatalogueClient
	prod          *kgo.Client
	topicRejected string
}

func NewBiller(pool *pgxpool.Pool, cat cataloguepb.CatalogueClient, prod *kgo.Client, rejected string) *Biller {
	return &Biller{pool: pool, cat: cat, prod: prod, topicRejected: rejected}
}

// Handle processes one normalized CDR. The dedup insert is the last step and
// carries the final values, so every crash window replays into an idempotent
// single insert. A missing rate is unrecoverable by retry, so it dead-letters
// and the consumer moves on; every other failure returns an error and is
// retried in-process (at-least-once).
func (b *Biller) Handle(ctx context.Context, rec *kgo.Record) error {
	c := &mediationpb.CallCdr{}
	if err := proto.Unmarshal(rec.Value, c); err != nil {
		return b.reject(ctx, rec, "decode_error", err)
	}
	if reason := Validate(c); reason != "" {
		return b.reject(ctx, rec, reason, nil)
	}

	rate, err := b.cat.GetRate(ctx, &cataloguepb.GetRateRequest{Destination: c.GetDestination()})
	if err != nil {
		return err
	}
	if !rate.GetFound() {
		return b.reject(ctx, rec, "no_rate", nil)
	}

	amount, err := b.rateCost(ctx, c.GetChargedSeconds(), rate.GetRatePerSec())
	if err != nil {
		return err
	}

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit

	tag, err := tx.Exec(ctx,
		`INSERT INTO invoice_lines (call_id, account_id, period, charged_seconds, amount_minor, rate_per_sec)
		 VALUES ($1, $2, $3, $4, $5, $6::numeric)
		 ON CONFLICT (call_id) DO NOTHING`,
		c.GetCallId(), c.GetAccountId(), periodOf(c), c.GetChargedSeconds(), amount, rate.GetRatePerSec())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 { // 0 rows = replay of an already-billed call
		if _, err := tx.Exec(ctx,
			`INSERT INTO cdr_period_totals (period, cdr_count, amount_sum)
			 VALUES ($1, 1, $2)
			 ON CONFLICT (period) DO UPDATE
			 SET cdr_count = cdr_period_totals.cdr_count + 1,
			     amount_sum = cdr_period_totals.amount_sum + EXCLUDED.amount_sum`,
			periodOf(c), amount); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// rateCost mirrors the OCS money rule: ROUND(delta * rate_per_sec) in integer
// minor units, half away from zero, computed by Postgres numeric math.
func (b *Biller) rateCost(ctx context.Context, seconds int64, rate string) (int64, error) {
	var cost int64
	err := b.pool.QueryRow(ctx, `SELECT ROUND($1::numeric * $2::numeric)::bigint`,
		seconds, rate).Scan(&cost)
	return cost, err
}

func (b *Biller) reject(ctx context.Context, rec *kgo.Record, reason string, cause error) error {
	if cause != nil {
		slog.Error("cdr rejected", "reason", reason, "err", cause)
	} else {
		slog.Error("cdr rejected", "reason", reason)
	}
	r := &kgo.Record{Topic: b.topicRejected, Key: rec.Key, Value: rec.Value,
		Headers: []kgo.RecordHeader{{Key: "reason", Value: []byte(reason)}}}
	return b.prod.ProduceSync(ctx, r).FirstErr()
}

// Validate mirrors Mediation's checks plus billing-side requirements.
func Validate(c *mediationpb.CallCdr) string {
	switch {
	case c.GetCallId() == "":
		return "missing_call_id"
	case c.GetAccountId() == "":
		return "missing_account_id"
	case c.GetStartedAtMs() == 0:
		return "missing_started_at"
	case c.GetChargedSeconds() < 0:
		return "negative_charged_seconds"
	case !validReason(c.GetEndReason()):
		return "unknown_end_reason"
	}
	return ""
}

func validReason(r string) bool {
	return r == "normal" || r == "balance_exhausted" || r == "ocs_unavailable"
}

// periodOf renders a CDR's start time as the invoice period YYYY-MM in UTC.
func periodOf(c *mediationpb.CallCdr) string {
	t := time.UnixMilli(c.GetStartedAtMs()).UTC()
	return strconv.Itoa(t.Year()) + "-" + pad2(int(t.Month()))
}

func pad2(n int) string {
	if n < 10 {
		return "0" + strconv.Itoa(n)
	}
	return strconv.Itoa(n)
}

// Invoice reads one account's invoice for a period; missing is empty.
func Invoice(ctx context.Context, pool *pgxpool.Pool, accountID, period string) (*InvoiceSummary, error) {
	rows, err := pool.Query(ctx,
		`SELECT call_id, charged_seconds, amount_minor, rate_per_sec::text
		 FROM invoice_lines WHERE account_id = $1 AND period = $2 ORDER BY created_at`,
		accountID, period)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sum := &InvoiceSummary{AccountID: accountID, Period: period}
	for rows.Next() {
		var callID, rate string
		var seconds, amount int64
		if err := rows.Scan(&callID, &seconds, &amount, &rate); err != nil {
			return nil, err
		}
		sum.Lines = append(sum.Lines, InvoiceLineDetail{
			CallID: callID, ChargedSeconds: seconds, AmountMinor: amount, RatePerSec: rate})
		sum.TotalMinor += amount
		sum.LineCount++
	}
	return sum, nil
}

// InvoiceSummary is the storage-neutral invoice view for the gRPC layer.
type InvoiceSummary struct {
	AccountID  string
	Period     string
	TotalMinor int64
	LineCount  int64
	Lines      []InvoiceLineDetail
}

type InvoiceLineDetail struct {
	CallID         string
	ChargedSeconds int64
	AmountMinor    int64
	RatePerSec     string
}
