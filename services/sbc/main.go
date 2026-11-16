package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	sbcpb "omnifoni/sbc/gen/sbcpb"
	"omnifoni/observability"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	observability.SetupLogging("sbc")
	shutdown, err := observability.Init(context.Background(), "sbc")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	dsn := env("SBC_DSN", "postgres://postgres:dev-only@postgres:5432/sbc")
	addr := env("SBC_ADDR", ":50058")
	reflectFlag := env("SBC_REFLECTION", "") == "1"

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

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer(observability.ServerOptions()...)
	sbcpb.RegisterSbcServer(g, NewSBCServer(pool))
	if reflectFlag {
		reflection.Register(g)
	}
	slog.Info("sbc up", "addr", addr)
	if err := g.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
