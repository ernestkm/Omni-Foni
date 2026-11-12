package main

import (
	"time"

	mediationpb "omnifoni/mediation/gen/mediationpb"
)

// buildCDR derives the final call detail record from a finished call.
func buildCDR(c *call, endReason string, chargedSeconds int64, endedAt time.Time) *mediationpb.CallCdr {
	return &mediationpb.CallCdr{
		CallId:         c.id,
		AccountId:      c.accountID,
		Destination:    c.destination,
		Cli:            c.cli,
		StartedAtMs:    c.start.UnixMilli(),
		EndedAtMs:      endedAt.UnixMilli(),
		ChargedSeconds: chargedSeconds,
		EndReason:      endReason,
	}
}
