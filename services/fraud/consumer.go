package main

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	mediationpb "omnifoni/mediation/gen/mediationpb"
	"omnifoni/observability"
)

type Handler interface {
	Handle(context.Context, *kgo.Record) error
}

func Run(ctx context.Context, cl *kgo.Client, h Handler) {
	for {
		fetches := cl.PollRecords(ctx, 100)
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.ErrorContext(ctx, "kafka fetch", "topic", topic, "partition", partition, "err", err)
		})
		lasts := map[string]*kgo.Record{}
		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()
			for {
				err := h.Handle(ctx, rec)
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return
				}
				slog.ErrorContext(ctx, "fraud handle failed, retrying", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			lasts[rec.Topic+"/"+strconv.Itoa(int(rec.Partition))] = rec
		}
		if ctx.Err() != nil {
			return
		}
		toCommit := make([]*kgo.Record, 0, len(lasts))
		for _, rec := range lasts {
			toCommit = append(toCommit, rec)
		}
		if len(toCommit) > 0 {
			if err := cl.CommitRecords(ctx, toCommit...); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "fraud offset commit failed", "err", err)
			}
		}
	}
}

type Ingester struct {
	store *Store
}

func NewIngester(store *Store) *Ingester {
	return &Ingester{store: store}
}

func (i *Ingester) Handle(ctx context.Context, rec *kgo.Record) error {
	c := &mediationpb.CallCdr{}
	if err := proto.Unmarshal(rec.Value, c); err != nil {
		slog.ErrorContext(ctx, "fraud cdr decode failed", "err", err)
		return nil
	}
	if c.GetCallId() == "" || c.GetAccountId() == "" {
		slog.ErrorContext(ctx, "fraud cdr validation failed", "call_id", c.GetCallId())
		return nil
	}
	ctx = observability.ExtractKafka(ctx, rec)
	ctx, span := observability.ConsumerSpan(ctx, "fraud.consume_cdr", c.GetCallId())
	defer span.End()
	if err := i.store.IngestCDR(ctx, c); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}
