package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
	"omnifoni/observability"
)

// kafkaPublisher publishes DriftEvents as proto bytes, keyed by account when
// present else by period.
type kafkaPublisher struct {
	prod  *kgo.Client
	topic string
}

func newKafkaPublisher(prod *kgo.Client, topic string) DriftPublisher {
	return &kafkaPublisher{prod: prod, topic: topic}
}

func (p *kafkaPublisher) PublishDrift(ctx context.Context, ev *pb.DriftEvent) error {
	b, err := proto.Marshal(ev)
	if err != nil {
		return err
	}
	key := ev.GetPeriod()
	if ev.GetAccountId() != "" {
		key = ev.GetAccountId()
	}
	r := &kgo.Record{Topic: p.topic, Key: []byte(key), Value: b}
	observability.InjectKafka(ctx, r)
	return p.prod.ProduceSync(ctx, r).FirstErr()
}

// Sweep runs one pass over the current and previous month. Ledger drift is
// warned and published; payment deltas are info logs (accounts-receivable
// states, not drift).
func (s *Server) Sweep(ctx context.Context, now time.Time) error {
	u := now.UTC()
	// Build the previous month from year/month with day 1: AddDate(0,-1,0)
	// normalizes short months (2026-03-31 -> 2026-03-03) and would sweep the
	// current month twice.
	prev := time.Date(u.Year(), u.Month()-1, 1, 0, 0, 0, 0, time.UTC)
	for _, period := range []string{PeriodOf(now), PeriodOf(prev)} {
		ctx, span := otel.Tracer("omnifoni/reconciliation").Start(ctx, "reconciliation.sweep",
			trace.WithAttributes(attribute.String("period", period)))
		resp, err := s.reconcilePeriodData(ctx, period)
		if err != nil {
			return fmt.Errorf("reconcile %s: %w", period, err)
		}
		led := resp.GetLedgerVsInvoices()
		if led.GetStatus() == StatusDrift {
			// Guard against read skew: reconcilePeriodData reads the ledger and
			// the invoice aggregate over separate RPCs. While billing is
			// actively ingesting, a CDR committing between those reads looks
			// like drift. Re-read the ledger; if it moved, defer to the next
			// pass instead of publishing a spurious event.
			lc2, ls2, err := s.billing.GetPeriodLedger(ctx, period)
			if err != nil {
				slog.Error("drift recheck failed", "period", period, "err", err)
				continue
			}
			if lc2 != led.GetExpectedCount() || ls2 != led.GetExpectedAmountMinor() {
				slog.Info("ledger moved during sweep; deferring", "period", period)
				continue
			}
			slog.Warn("reconciliation drift", "period", period,
				"expected_count", led.GetExpectedCount(), "actual_count", led.GetActualCount(),
				"expected_minor", led.GetExpectedAmountMinor(), "actual_minor", led.GetActualAmountMinor())
			for _, kind := range DriftKinds(led) {
				ev := &pb.DriftEvent{
					Period:        period,
					Kind:          kind,
					ExpectedMinor: led.GetExpectedAmountMinor(),
					ActualMinor:   led.GetActualAmountMinor(),
					DeltaMinor:    led.GetActualAmountMinor() - led.GetExpectedAmountMinor(),
					ExpectedCount: led.GetExpectedCount(),
					ActualCount:   led.GetActualCount(),
					DetectedAtMs:  now.UnixMilli(),
				}
				if err := s.publisher.PublishDrift(ctx, ev); err != nil {
					slog.Error("drift publish failed", "period", period, "kind", kind, "err", err)
				}
			}
		}
		for _, pc := range resp.GetAccounts() {
			st := pc.GetStatus()
			if st != StatusPaid && st != StatusNoActivity {
				slog.Info("payment delta", "period", period, "account", pc.GetAccountId(),
					"status", st, "invoice_minor", pc.GetInvoiceTotalMinor(),
					"payments_minor", pc.GetPaymentsTotalMinor())
			}
		}
		span.SetAttributes(attribute.Int("accounts", len(resp.GetAccounts())))
		span.End()
	}
	return nil
}
