package main

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/jackc/pgx/v5/pgxpool"

	pb "omnifoni/mediation/gen/mediationpb"
)

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

const schemaSQL = `CREATE TABLE IF NOT EXISTS cdrs (
    call_id    TEXT PRIMARY KEY,
    published  BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Validate returns "" for a valid CDR, else its rejection reason.
func Validate(c *pb.CallCdr) string {
	switch {
	case c.GetCallId() == "":
		return "missing_call_id"
	case c.GetAccountId() == "":
		return "missing_account_id"
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

// Run consumes cdrs.raw in small batches, handling records sequentially. A
// failed record is retried in-process with backoff; offsets are committed
// only after every record of the batch was handled. Keys pin a call to one
// partition, so this is serial per call.
func Run(ctx context.Context, cl *kgo.Client, p *Processor) {
	for {
		fetches := cl.PollRecords(ctx, 100)
		lasts := map[string]*kgo.Record{}
		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()
			for {
				err := p.Handle(ctx, rec)
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return
				}
				slog.Error("handle failed, retrying", "err", err)
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
				slog.Error("commit failed", "err", err)
			}
		}
	}
}
