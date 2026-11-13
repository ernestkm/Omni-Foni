package main

import (
	"context"
	_ "embed"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrate.sql
var schemaSQL string

// Migrate applies the schema. Idempotent, safe to run at boot.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

// WebhookRequest is the gateway event body both Stripe and PayFast adapters
// normalize into.
type WebhookRequest struct {
	ID         string `json:"id"`          // gateway event id, used for idempotency
	AccountID  string `json:"account_id"`  // Omni-Foní account
	Gateway    string `json:"gateway"`     // stripe | payfast | paypal
	AmountMinor int64 `json:"amount_minor"` // integer minor units (1/10000 currency)
}

// staleWindow bounds how old a signature timestamp may be; older requests are
// rejected even before idempotency, bounding replay risk.
const staleWindow = 5 * time.Minute

// VerifySignature implements the Stripe-Signature scheme: HMAC-SHA256 over
// "<timestamp>.<payload>" with the webhook secret, hex-encoded in v1=.
func VerifySignature(secret, header string, payload []byte, now time.Time) error {
	var ts, mac string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "t":
			ts = v
		case "v1":
			mac = v
		}
	}
	if ts == "" || mac == "" {
		return errors.New("signature header missing fields")
	}
	tUnix, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return errors.New("signature timestamp invalid")
	}
	if now.Sub(time.Unix(tUnix, 0)) > staleWindow || time.Unix(tUnix, 0).Sub(now) > time.Minute {
		return errors.New("signature timestamp stale")
	}
	h := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(h, "%s.", ts)
	h.Write(payload)
	if !hmac.Equal([]byte(hex.EncodeToString(h.Sum(nil))), []byte(mac)) {
		return errors.New("signature mismatch")
	}
	return nil
}

// WebhookServer records gateway payments idempotently by event id.
type WebhookServer struct {
	pool   *pgxpool.Pool
	secret string
}

func NewWebhookServer(pool *pgxpool.Pool, secret string) *WebhookServer {
	return &WebhookServer{pool: pool, secret: secret}
}

// RecordPayment verifies the signature, then inserts the payment keyed by the
// gateway event id; a replay returns 200 with no second row (at-most-once
// money recording, unlike the CDR paths).
func (s *WebhookServer) RecordPayment(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if err := VerifySignature(s.secret, r.Header.Get("Stripe-Signature"), body, time.Now()); err != nil {
		slog.Warn("webhook rejected", "err", err)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	var req WebhookRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.AccountID == "" || req.AmountMinor <= 0 {
		http.Error(w, "bad event", http.StatusBadRequest)
		return
	}
	if req.Gateway == "" {
		req.Gateway = "stripe"
	}
	tag, err := s.pool.Exec(r.Context(),
		`INSERT INTO payments (event_id, account_id, gateway, amount_minor)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		req.ID, req.AccountID, req.Gateway, req.AmountMinor)
	if err != nil {
		slog.Error("payment insert", "err", err)
		http.Error(w, "internal", http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 0 {
		slog.Info("webhook replay absorbed", "event", req.ID)
	}
	w.WriteHeader(http.StatusOK)
}

// maxBody bounds webhook payloads; anything larger is refused unread.
const maxBody = 1 << 16

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, maxBody))
}
