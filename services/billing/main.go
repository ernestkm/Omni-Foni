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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	kgo "github.com/twmb/franz-go/pkg/kgo"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	pb "omnifoni/billing/gen/billingpb"
	"omnifoni/observability"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := env("BILLING_DSN", "postgres://postgres:dev-only@postgres:5432/billing")
	brokers := []string{env("BILLING_KAFKA", "kafka:9092")}
	normalized := env("BILLING_TOPIC_NORMALIZED", "cdrs.normalized")
	rejected := env("BILLING_TOPIC_REJECTED", "cdrs.billing.rejected")
	catAddr := env("BILLING_CATALOGUE_ADDR", "catalogue:50053")
	addr := env("BILLING_ADDR", ":50054")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	observability.SetupLogging("billing")
	shutdown, err := observability.Init(ctx, "billing")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	catConn, err := grpc.NewClient(catAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("catalogue client", "err", err)
		os.Exit(1)
	}
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
	if err != nil {
		slog.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer prod.Close()
	cons, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("billing"),
		kgo.ConsumeTopics(normalized),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(), // commit only after handling
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	defer cons.Close()

	go Run(ctx, cons, NewBiller(pool, cataloguepb.NewCatalogueClient(catConn), prod, rejected))

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}
	srv := grpc.NewServer(observability.ServerOptions()...)
	pb.RegisterBillingServer(srv, &billingServer{pool: pool})
	if os.Getenv("BILLING_REFLECTION") == "1" {
		reflection.Register(srv)
	}
	slog.Info("billing up", "addr", addr, "brokers", brokers)
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
