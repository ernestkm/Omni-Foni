package main

import (
	"context"
	"os"
	"testing"
	"time"
)

var testStore *Store

func testDSN() string {
	if s := os.Getenv("NOTIFICATIONS_TEST_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/notifications"
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	testStore, err = Open(ctx, testDSN())
	if err != nil {
		panic(err)
	}
	if err := testStore.Migrate(ctx); err != nil {
		panic(err)
	}
	code := m.Run()
	testStore.Close()
	os.Exit(code)
}

func resetOutbox(t *testing.T) {
	t.Helper()
	if _, err := testStore.pool.Exec(context.Background(), "TRUNCATE TABLE outbox"); err != nil {
		t.Fatal(err)
	}
}

func testNotification(key string) Notification {
	return Notification{
		DedupeKey: key, Channel: "email", Recipient: "ops@example.test",
		Subject: "report ready", Body: "body", Status: "pending",
		NextAttemptAt: time.Now().UTC(),
	}
}

func TestEnqueueDedupes(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, inserted, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil || !inserted || n.NotificationID == "" {
		t.Fatalf("first enqueue = %+v %v %v", n, inserted, err)
	}
	dup, inserted, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil || inserted || dup.NotificationID != "" {
		t.Fatalf("duplicate = %+v %v %v", dup, inserted, err)
	}
}

func TestClaimAndTransitions(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, _, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil {
		t.Fatal(err)
	}
	due, err := testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 1 || due[0].NotificationID != n.NotificationID {
		t.Fatalf("claim = %+v %v", due, err)
	}
	if due[0].Attempts != 1 {
		t.Fatalf("attempts = %d", due[0].Attempts)
	}
	if err := testStore.MarkRetry(ctx, n.NotificationID, "smtp down", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := testStore.MarkSent(ctx, n.NotificationID); err != nil {
		t.Fatal(err)
	}
	due, err = testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("sent row reclaimed = %+v %v", due, err)
	}
}

func TestMarkTerminal(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, _, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := testStore.MarkTerminal(ctx, n.NotificationID, "missing recipient"); err != nil {
		t.Fatal(err)
	}
	due, err := testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("failed row reclaimed = %+v %v", due, err)
	}
}
