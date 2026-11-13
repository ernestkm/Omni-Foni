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

	billingpb "omnifoni/billing/gen/billingpb"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := env("IDENTITY_DSN", "postgres://postgres:dev-only@postgres:5432/identity")
	addr := env("IDENTITY_ADDR", ":50056")

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

	billingAddr := env("IDENTITY_BILLING_ADDR", "localhost:50054")
	bc, err := DialBilling(billingAddr)
	if err != nil {
		slog.Error("billing client", "err", err)
		os.Exit(1)
	}

	id := NewIdentityServer(pool)
	commission := NewCommissionServer(id, billingpb.NewBillingClient(bc))
	mux := http.NewServeMux()
	mux.HandleFunc("POST /bootstrap", id.Bootstrap)
	mux.HandleFunc("POST /users", id.CreateUser)
	mux.HandleFunc("POST /tenants", id.CreateTenant)
	mux.HandleFunc("POST /sessions", id.Login)
	mux.HandleFunc("GET /me", id.Me)
	mux.HandleFunc("POST /accounts", id.CreateAccount)
	mux.HandleFunc("GET /accounts", id.ListAccounts)
	mux.HandleFunc("GET /commission", commission.ServeHTTP)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	slog.Info("identity up", "addr", addr)
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
