package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := env("PAYMENTS_DSN", "postgres://postgres:dev-only@postgres:5432/payments")
	addr := env("PAYMENTS_ADDR", ":50055")
	// ponytail: dev secret; production injects per-gateway secrets from the
	// secret store, keyed by gateway
	secret := env("PAYMENTS_WEBHOOK_SECRET", "dev-only")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook/{gateway}/payment", func(w http.ResponseWriter, r *http.Request) {
		// Gateway path segment is advisory (logs/audit); the body carries the
		// authoritative gateway value.
		NewWebhookServer(pool, secret).RecordPayment(w, r)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	slog.Info("payments up", "addr", addr)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
