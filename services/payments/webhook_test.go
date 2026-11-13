package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Integration tests run against the dev payments DB (make up).
func dsn() string {
	if s := os.Getenv("PAYMENTS_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/payments"
}

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	pool, err = pgxpool.New(ctx, dsn())
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

const secret = "test-secret"

func sign(t *testing.T, payload []byte, at time.Time) string {
	t.Helper()
	ts := strconv.FormatInt(at.Unix(), 10)
	h := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(h, "%s.", ts)
	h.Write(payload)
	return "t=" + ts + ",v1=" + hex.EncodeToString(h.Sum(nil))
}

func post(t *testing.T, h http.Handler, body []byte, sig string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/webhook/stripe/payment", bytes.NewReader(body))
	req.Header.Set("Stripe-Signature", sig)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func count(t *testing.T, event string) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM payments WHERE event_id = $1`, event).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestValidWebhookRecordsPayment: a properly signed event is recorded once.
func TestValidWebhookRecordsPayment(t *testing.T) {
	if _, err := pool.Exec(context.Background(),
		`DELETE FROM payments WHERE event_id = 'evt-1'`); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"evt-1","account_id":"acc-web","gateway":"stripe","amount_minor":1000}`)
	rec := post(t, wrap(NewWebhookServer(pool, secret)), body, sign(t, body, time.Now()))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if got := count(t, "evt-1"); got != 1 {
		t.Fatalf("payments %d, want 1", got)
	}
}

// TestReplayIdempotent: the same event twice yields exactly one row.
func TestReplayIdempotent(t *testing.T) {
	if _, err := pool.Exec(context.Background(),
		`DELETE FROM payments WHERE event_id = 'evt-2'`); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"evt-2","account_id":"acc-web","gateway":"stripe","amount_minor":500}`)
	h := NewWebhookServer(pool, secret)
	if rec := post(t, wrap(h), body, sign(t, body, time.Now())); rec.Code != http.StatusOK {
		t.Fatalf("first: %d", rec.Code)
	}
	if rec := post(t, wrap(h), body, sign(t, body, time.Now())); rec.Code != http.StatusOK {
		t.Fatalf("replay: %d", rec.Code)
	}
	if got := count(t, "evt-2"); got != 1 {
		t.Fatalf("payments %d, want 1 (replay double-recorded)", got)
	}
}

// TestForgedSignature: a bad MAC is rejected and records nothing.
func TestForgedSignature(t *testing.T) {
	body := []byte(`{"id":"evt-3","account_id":"acc-web","gateway":"stripe","amount_minor":999}`)
	rec := post(t, wrap(NewWebhookServer(pool, secret)), body,
		"t="+strconv.FormatInt(time.Now().Unix(), 10)+",v1=0000000000000000000000000000000000000000000000000000000000000000")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", rec.Code)
	}
	if got := count(t, "evt-3"); got != 0 {
		t.Fatal("forged event recorded a payment")
	}
}

// TestStaleTimestamp: signatures older than the replay window are rejected.
func TestStaleTimestamp(t *testing.T) {
	body := []byte(`{"id":"evt-4","account_id":"acc-web","gateway":"stripe","amount_minor":100}`)
	old := time.Now().Add(-10 * time.Minute)
	rec := post(t, wrap(NewWebhookServer(pool, secret)), body, sign(t, body, old))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", rec.Code)
	}
	if got := count(t, "evt-4"); got != 0 {
		t.Fatal("stale event recorded a payment")
	}
}

func wrap(h *WebhookServer) http.Handler { return http.HandlerFunc(h.RecordPayment) }
