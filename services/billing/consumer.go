// Run consumes cdrs.normalized in small batches, handling records
// sequentially. A failed record is retried in-process with backoff; offsets
// are committed only after every record of the batch was handled. Keys pin a
// call to one partition, so this is serial per call.
package main

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

type Handler interface {
	Handle(ctx context.Context, rec *kgo.Record) error
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
				slog.ErrorContext(ctx, "handle failed, retrying", "err", err)
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
		var toCommit []*kgo.Record
		for _, rec := range lasts {
			toCommit = append(toCommit, rec)
		}
		if len(toCommit) > 0 {
			if err := cl.CommitRecords(ctx, toCommit...); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "commit failed", "err", err)
			}
		}
	}
}
