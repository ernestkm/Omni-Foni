package main

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

type Handler interface {
	Handle(context.Context, *kgo.Record) error
}

type kafkaClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
}

func Run(ctx context.Context, cl kafkaClient, h Handler, retry time.Duration) {
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
				slog.ErrorContext(ctx, "notification handle failed, retrying", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(retry):
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
				slog.ErrorContext(ctx, "notification offset commit failed", "err", err)
			}
		}
	}
}
