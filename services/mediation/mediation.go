package main

import (
	"context"

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
