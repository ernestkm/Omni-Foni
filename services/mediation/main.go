package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	kgo "github.com/twmb/franz-go/pkg/kgo"

	"omnifoni/observability"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := env("MEDIATION_DSN", "postgres://postgres:dev-only@postgres:5432/mediation")
	brokers := []string{env("MEDIATION_KAFKA", "kafka:9092")}
	raw := env("MEDIATION_TOPIC_RAW", "cdrs.raw")
	normalized := env("MEDIATION_TOPIC_NORMALIZED", "cdrs.normalized")
	rejected := env("MEDIATION_TOPIC_REJECTED", "cdrs.rejected")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	observability.SetupLogging("mediation")
	shutdown, err := observability.Init(ctx, "mediation")
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

	// ponytail: dev default; production resolves the mesh identity
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("mediation"),
		kgo.ConsumeTopics(raw),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(), // commit only after handling
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		slog.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	slog.Info("mediation up", "brokers", brokers, "raw", raw)
	Run(ctx, cl, NewProcessor(pool, cl, normalized, rejected))
}
