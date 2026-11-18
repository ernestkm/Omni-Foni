package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"

	fraudpb "omnifoni/fraud/gen/fraudpb"
	"omnifoni/observability"
)

type kafkaPublisher struct {
	prod  *kgo.Client
	topic string
}

func (p *kafkaPublisher) Publish(ctx context.Context, event *fraudpb.FraudAlert) error {
	raw, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	rec := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(event.GetAlert().GetAccountId()),
		Value: raw,
	}
	observability.InjectKafka(ctx, rec)
	return p.prod.ProduceSync(ctx, rec).FirstErr()
}

func env(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func envInt(key string, def int) int {
	value := os.Getenv(key)
	if value == "" {
		return def
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		slog.Error("invalid integer env", "key", key, "value", value)
		os.Exit(1)
	}
	return n
}

func main() {
	observability.SetupLogging("fraud")
	shutdown, err := observability.Init(context.Background(), "fraud")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	addr := env("FRAUD_ADDR", ":50063")
	dsn := env("FRAUD_DSN", "postgres://postgres:dev-only@postgres:5432/fraud")
	brokers := env("FRAUD_KAFKA", "kafka:9092")
	alertsTopic := env("FRAUD_TOPIC_ALERTS", "fraud.alerts")
	cfg := Config{
		AlertThreshold:   envInt("FRAUD_ALERT_THRESHOLD", 60),
		SoftAccountCalls: envInt("FRAUD_SOFT_ACCOUNT_CALLS", 5),
		SoftPrefixCalls:  envInt("FRAUD_SOFT_PREFIX_CALLS", 2),
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := Open(ctx, dsn)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup("fraud"),
		kgo.ConsumeTopics("cdrs.normalized"),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	defer consumer.Close()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		slog.Error("kafka producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	publisher := &kafkaPublisher{prod: producer, topic: alertsTopic}
	server := NewFraudServer(store, cfg, publisher)
	ingester := NewIngester(store)
	go Run(ctx, consumer, ingester)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer(observability.ServerOptions()...)
	fraudpb.RegisterFraudServer(g, server)
	if env("FRAUD_REFLECTION", "") == "1" {
		reflection.Register(g)
	}
	slog.Info("fraud up", "addr", addr)
	if err := g.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
