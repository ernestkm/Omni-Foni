package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeSender struct {
	errs []error
	sent []Notification
}

func (f *fakeSender) Send(ctx context.Context, n Notification) error {
	f.sent = append(f.sent, n)
	if len(f.errs) == 0 {
		return nil
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	return err
}

func TestWorkerMarksSent(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, _, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{}
	w := NewWorker(testStore, sender, time.Millisecond, 10, 5, time.Minute)
	if err := w.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 1 || sender.sent[0].NotificationID != n.NotificationID {
		t.Fatalf("sent = %+v", sender.sent)
	}
	due, err := testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("reclaimed = %+v %v", due, err)
	}
}

func TestWorkerRetriesThenFails(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, _, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{errs: []error{errors.New("smtp down")}}
	w := NewWorker(testStore, sender, time.Millisecond, 10, 1, time.Minute)
	if err := w.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	due, err := testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("failed row reclaimed = %+v %v", due, err)
	}
	if sender.sent[0].NotificationID != n.NotificationID {
		t.Fatalf("sent = %+v", sender.sent)
	}
}
