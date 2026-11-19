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
	stop  func(ctx context.Context, id string) (bool, error)
}

func newReaper(f *facadeClient, kam *kamRPC, st *store) *reaper {
	return &reaper{store: st, kam: kam, live: f.live, stop: f.stop}
}

func (r *reaper) retryStops(ctx context.Context) {
	for id, snap := range r.store.all() {
		if !snap.stopFailed {
			continue
		}
		if _, err := r.stop(ctx, snap.switchingID); err != nil {
			slog.Warn("retry stop", "call", id, "err", err)
			continue
		}
		r.store.takePendingStop(id, snap.switchingID)
	}
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

// tick retries failed stops and ends every tracked dialog whose switching call is no longer live.
func (r *reaper) tick(ctx context.Context) error {
	r.retryStops(ctx)
	tracked := r.store.all()
	live, err := r.live(ctx)
	if err != nil {
		return err
	}
	for id, snap := range tracked {
		if snap.stopFailed || live[snap.switchingID] {
			continue
		}
		e, ok := r.store.get(id)
		if !ok || e.switchingID != snap.switchingID {
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
