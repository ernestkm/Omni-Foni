package main

import (
	"context"
	"log/slog"
	"net"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	pb "omnifoni/charging/gen/chargingpb"
	"omnifoni/observability"
)

func main() {
	observability.SetupLogging("charging")
	shutdown, err := observability.Init(context.Background(), "charging")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	dsn := os.Getenv("CHARGING_DSN")
	if dsn == "" {
		// ponytail: dev default, production injects the real DSN
		dsn = "postgres://postgres:dev-only@localhost:5432/charging"
	}
	addr := os.Getenv("CHARGING_ADDR")
	if addr == "" {
		addr = ":50051"
	}
	catAddr := os.Getenv("CHARGING_CATALOGUE_ADDR")
	if catAddr == "" {
		// ponytail: dev default; production resolves the mesh identity
		catAddr = "localhost:50053"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("dsn invalid", "err", err)
		os.Exit(1)
	}
	if err := pool.Ping(ctx); err != nil {
		slog.Error("postgres unreachable, fail-closed", "err", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
		slog.Error("migrate failed", "err", err)
		os.Exit(1)
	}

	catConn, err := grpc.NewClient(catAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("catalogue client", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}
	srv := grpc.NewServer(observability.ServerOptions()...)
	pb.RegisterChargingServer(srv, NewServer(pool, cataloguepb.NewCatalogueClient(catConn)))
	// ponytail: reflection gated by env for dev tooling (grpcurl); disable in prod
	if os.Getenv("CHARGING_REFLECTION") == "1" {
		reflection.Register(srv)
	}
	slog.Info("charging up", "addr", addr)
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
