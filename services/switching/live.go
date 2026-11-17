package main

import (
	"sort"
	"time"
)

// LiveCallSnapshot is one in-flight call as seen by ListLiveCalls.
type LiveCallSnapshot struct {
	CallID         string
	SessionID      string
	AccountID      string
	Destination    string
	CLI            string
	TrunkID        string
	StartedAt      time.Time
	MaxSeconds     int64
	ChargedSeconds int64
}

// LiveCalls returns a snapshot of calls that have not finished, oldest first.
// accountIDs empty means every account. Lock order: r.mu, then each call's mu.
func (r *Registry) LiveCalls(accountIDs []string) []LiveCallSnapshot {
	filter := make(map[string]bool, len(accountIDs))
	for _, id := range accountIDs {
		filter[id] = true
	}

	r.mu.Lock()
	calls := make([]*call, 0, len(r.calls))
	for _, c := range r.calls {
		if c != nil && (len(filter) == 0 || filter[c.accountID]) {
			calls = append(calls, c)
		}
	}
	r.mu.Unlock()

	out := make([]LiveCallSnapshot, 0, len(calls))
	for _, c := range calls {
		c.mu.Lock()
		if !c.done {
			out = append(out, LiveCallSnapshot{
				CallID: c.id, SessionID: c.sessionID, AccountID: c.accountID,
				Destination: c.destination, CLI: c.cli, TrunkID: c.trunkID,
				StartedAt: c.start, MaxSeconds: c.maxSeconds,
				ChargedSeconds: c.chargedSeconds,
			})
		}
		c.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].StartedAt.Before(out[j].StartedAt)
		}
		return out[i].CallID < out[j].CallID
	})
	return out
}
