package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"

	"omnifoni/observability"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		slog.Error("bad "+key, "value", v)
		os.Exit(1)
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 1 {
		slog.Error("bad "+key, "value", v)
		os.Exit(1)
	}
	return d
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	observability.SetupLogging("notifications")
	shutdown, err := observability.Init(ctx, "notifications")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	store, err := Open(ctx, env("NOTIFICATIONS_DSN", "postgres://postgres:dev-only@postgres:5432/notifications"))
	if err != nil {
		slog.Error("notifications db", "err", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		slog.Error("notifications migrate", "err", err)
		os.Exit(1)
	}

	brokers := strings.Split(env("NOTIFICATIONS_KAFKA", "kafka:9092"), ",")
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(env("NOTIFICATIONS_GROUP", "notifications")),
		kgo.ConsumeTopics(env("NOTIFICATIONS_TOPIC_REPORT_READY", "report.ready")),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	defer cl.Close()
	go Run(ctx, cl, NewIngester(store), time.Second)

	sender := NewSMTPSender(
		env("NOTIFICATIONS_SMTP_ADDR", "mailpit:1025"),
		env("NOTIFICATIONS_SMTP_FROM", "omnifoni@example.test"),
		10*time.Second,
	)
	worker := NewWorker(store, sender,
		envDuration("NOTIFICATIONS_POLL_INTERVAL", time.Second),
		envInt("NOTIFICATIONS_BATCH_SIZE", 32),
		envInt("NOTIFICATIONS_MAX_ATTEMPTS", 5),
		30*time.Second,
	)
	go worker.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Addr:              env("NOTIFICATIONS_ADDR", ":50064"),
		Handler:           observability.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("notifications up", "addr", srv.Addr, "brokers", brokers,
		"smtp", env("NOTIFICATIONS_SMTP_ADDR", "mailpit:1025"))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http serve failed", "err", err)
		os.Exit(1)
	}
}
