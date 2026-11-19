package main

import (
	"context"
	"log/slog"
	"time"
)

// dialogKiller ends a Kamailio dialog by hash coordinates.
type dialogKiller interface {
	endDialog(ctx context.Context, hEntry, hID string) error
}

type reaper struct {
	store *store
	kam   dialogKiller
	live  func(ctx context.Context) (map[string]bool, error)
}

func newReaper(f *facadeClient, kam *kamRPC, st *store) *reaper {
	return &reaper{store: st, kam: kam, live: f.live}
}

func (r *reaper) run(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := r.tick(ctx); err != nil {
			slog.Warn("reaper tick", "err", err)
		}
		cancel()
	}
}

// tick ends every tracked dialog whose switching call is no longer live.
func (r *reaper) tick(ctx context.Context) error {
	live, err := r.live(ctx)
	if err != nil {
		return err
	}
	for id, e := range r.store.all() {
		if live[e.switchingID] {
			continue
		}
		if err := r.kam.endDialog(ctx, e.hEntry, e.hID); err != nil {
			slog.Warn("end dialog", "call", id, "err", err)
			continue
		}
		r.store.take(id)
	}
	return nil
}
