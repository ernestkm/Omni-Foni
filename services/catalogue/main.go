package main

import (
	"context"
	"log/slog"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "omnifoni/catalogue/gen/cataloguepb"
)

func main() {
	dsn := os.Getenv("CATALOGUE_DSN")
	if dsn == "" {
		// ponytail: dev default; production composes the DSN from secrets
		dsn = "postgres://postgres:dev-only@localhost:5432/catalogue"
	}
	addr := os.Getenv("CATALOGUE_ADDR")
	if addr == "" {
		addr = ":50053"
	}

	ctx := context.Background()
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
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}
	srv := grpc.NewServer()
	pb.RegisterCatalogueServer(srv, NewServer(pool))
	if os.Getenv("CATALOGUE_REFLECTION") == "1" {
		reflection.Register(srv)
	}
	slog.Info("catalogue up", "addr", addr)
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
