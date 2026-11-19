package main

import (
	"context"
	"log/slog"
	"time"
)

type Sender interface {
	Send(context.Context, Notification) error
}

type Worker struct {
	store       *Store
	sender      Sender
	interval    time.Duration
	batch       int
	maxAttempts int
	backoff     time.Duration
}

func NewWorker(store *Store, sender Sender, interval time.Duration, batch, maxAttempts int, backoff time.Duration) *Worker {
	return &Worker{store: store, sender: sender, interval: interval, batch: batch, maxAttempts: maxAttempts, backoff: backoff}
}

func (w *Worker) Run(ctx context.Context) {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "notification worker", "err", err)
			}
		}
	}
}

func (w *Worker) RunOnce(ctx context.Context) error {
	items, err := w.store.ClaimDue(ctx, w.batch)
	if err != nil {
		return err
	}
	for _, n := range items {
		if err := w.sender.Send(ctx, n); err == nil {
			if err := w.store.MarkSent(ctx, n.NotificationID); err != nil {
				return err
			}
			continue
		} else {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			last := truncate(err.Error(), 1000)
			if n.Attempts >= w.maxAttempts {
				if err := w.store.MarkTerminal(ctx, n.NotificationID, last); err != nil {
					return err
				}
				continue
			}
			if err := w.store.MarkRetry(ctx, n.NotificationID, last,
				time.Now().Add(retryDelay(n.Attempts, w.backoff))); err != nil {
				return err
			}
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
