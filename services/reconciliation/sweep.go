package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
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
	return p.prod.ProduceSync(ctx, &kgo.Record{Topic: p.topic, Key: []byte(key), Value: b}).FirstErr()
}

// Sweep runs one pass over the current and previous month. Ledger drift is
// warned and published; payment deltas are info logs (accounts-receivable
// states, not drift).
func (s *Server) Sweep(ctx context.Context, now time.Time) error {
	for _, period := range []string{PeriodOf(now), PeriodOf(now.AddDate(0, -1, 0))} {
		resp, err := s.reconcilePeriodData(ctx, period)
		if err != nil {
			return fmt.Errorf("reconcile %s: %w", period, err)
		}
		led := resp.GetLedgerVsInvoices()
		if led.GetStatus() == StatusDrift {
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
	}
	return nil
}
