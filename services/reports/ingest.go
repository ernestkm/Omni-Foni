package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	mediationpb "omnifoni/mediation/gen/mediationpb"
	"omnifoni/observability"
)

// Ingester turns normalized CDRs into rated ClickHouse rows. Catalogue
// failures return an error so the consumer retries; they never zero money.
type Ingester struct {
	store *Store
	cat   cataloguepb.CatalogueClient
}

func NewIngester(store *Store, cat cataloguepb.CatalogueClient) *Ingester {
	return &Ingester{store: store, cat: cat}
}

// Validate mirrors billing's CDR requirements.
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
	case c.GetEndedAtMs() < c.GetStartedAtMs():
		return "ended_before_started"
	}
	return ""
}

func (i *Ingester) Handle(ctx context.Context, rec *kgo.Record) error {
	c := &mediationpb.CallCdr{}
	if err := proto.Unmarshal(rec.Value, c); err != nil {
		slog.ErrorContext(ctx, "cdr rejected", "reason", "decode_error", "err", err)
		return nil // poison pill: log and move on
	}
	ctx = observability.ExtractKafka(ctx, rec)
	ctx, span := observability.ConsumerSpan(ctx, "reports.ingest", c.GetCallId())
	defer span.End()

	if reason := Validate(c); reason != "" {
		slog.ErrorContext(ctx, "cdr rejected", "reason", reason)
		return nil
	}
	row, err := i.rate(ctx, c)
	if err != nil {
		return err
	}
	return i.store.Insert(ctx, []CdrRow{row})
}

func (i *Ingester) rate(ctx context.Context, c *mediationpb.CallCdr) (CdrRow, error) {
	start := time.UnixMilli(c.GetStartedAtMs()).UTC()
	row := CdrRow{
		CallID:         c.GetCallId(),
		AccountID:      c.GetAccountId(),
		Destination:    c.GetDestination(),
		CLI:            c.GetCli(),
		StartedAt:      start,
		EndedAt:        time.UnixMilli(c.GetEndedAtMs()).UTC(),
		ChargedSeconds: c.GetChargedSeconds(),
		EndReason:      c.GetEndReason(),
		TrunkID:        c.GetTrunkId(),
	}
	resp, err := i.cat.GetRate(ctx, &cataloguepb.GetRateRequest{Destination: c.GetDestination()})
	if err != nil {
		return CdrRow{}, fmt.Errorf("catalogue get rate: %w", err)
	}
	if !resp.GetFound() {
		row.RatePerSec = "0"
		row.RateMissing = true
		return row, nil
	}
	amount, err := RateCost(c.GetChargedSeconds(), resp.GetRatePerSec())
	if err != nil {
		return CdrRow{}, err
	}
	row.RatePerSec = resp.GetRatePerSec()
	row.AmountMinor = amount
	return row, nil
}
