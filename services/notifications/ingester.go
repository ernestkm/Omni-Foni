package main

import (
	"context"
	"log/slog"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	schedulingpb "omnifoni/notifications/gen/schedulingpb"
	"omnifoni/observability"
)

type Ingester struct {
	store *Store
}

func NewIngester(store *Store) *Ingester {
	return &Ingester{store: store}
}

func (i *Ingester) Handle(ctx context.Context, rec *kgo.Record) error {
	var ev schedulingpb.ReportReadyEvent
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		slog.ErrorContext(ctx, "report.ready decode failed", "err", err)
		return nil
	}
	ctx = observability.ExtractKafka(ctx, rec)
	ctx, span := observability.ConsumerSpan(ctx, "notifications.consume_report_ready", ev.GetArtifactId())
	defer span.End()
	n := notificationFromEvent(&ev, time.Now().UTC())
	if _, _, err := i.store.Enqueue(ctx, n); err != nil {
		span.RecordError(err)
		return err
	}
	if n.Status == "failed" {
		slog.WarnContext(ctx, "report.ready terminal validation failure",
			"artifact_id", ev.GetArtifactId(), "recipient", ev.GetRecipientEmail(), "reason", n.LastError)
	}
	return nil
}
