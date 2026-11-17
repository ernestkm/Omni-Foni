package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	chargingpb "omnifoni/charging/gen/chargingpb"
	"omnifoni/observability"
	paymentspb "omnifoni/payments/gen/paymentspb"
	pb "omnifoni/reports/gen/reportspb"
	switchingpb "omnifoni/switching/gen/switchingpb"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	dsn := env("REPORTS_CLICKHOUSE", "clickhouse://omnifoni:dev-only@clickhouse:9000")
	brokers := []string{env("REPORTS_KAFKA", "kafka:9092")}
	topic := env("REPORTS_TOPIC", "cdrs.normalized")
	group := env("REPORTS_GROUP", "reports")
	catAddr := env("REPORTS_CATALOGUE_ADDR", "catalogue:50053")
	identity := env("REPORTS_IDENTITY_ADDR", "http://identity:50056")
	addr := env("REPORTS_ADDR", ":50061")
	httpAddr := env("REPORTS_HTTP_ADDR", ":50062")
	payAddr := env("REPORTS_PAYMENTS_ADDR", "payments:50060")
	chgAddr := env("REPORTS_CHARGING_ADDR", "charging:50051")
	swAddr := env("REPORTS_SWITCHING_ADDR", "switching:50052")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	observability.SetupLogging("reports")
	shutdown, err := observability.Init(ctx, "reports")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	store, err := Open(ctx, dsn)
	if err != nil {
		slog.Error("clickhouse", "err", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
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
	payConn, err := grpc.NewClient(payAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("payments client", "err", err)
		os.Exit(1)
	}
	chgConn, err := grpc.NewClient(chgAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("charging client", "err", err)
		os.Exit(1)
	}
	swConn, err := grpc.NewClient(swAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("switching client", "err", err)
		os.Exit(1)
	}
	cons, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	defer cons.Close()
	go Run(ctx, cons, NewIngester(store, cataloguepb.NewCatalogueClient(catConn)))

	api := &reportsServer{
		store:    store,
		payments: paymentspb.NewPaymentsClient(payConn),
		charges:  chargingpb.NewChargingClient(chgConn),
		live:     switchingpb.NewSwitchingClient(swConn),
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}
	gsrv := grpc.NewServer(observability.ServerOptions()...)
	pb.RegisterReportsServer(gsrv, api)
	if os.Getenv("REPORTS_REFLECTION") == "1" {
		reflection.Register(gsrv)
	}
	go func() {
		<-ctx.Done()
		gsrv.GracefulStop()
	}()
	go func() {
		if err := gsrv.Serve(lis); err != nil {
			slog.Error("grpc serve failed", "err", err)
			os.Exit(1)
		}
	}()
	slog.Info("reports gRPC up", "addr", addr, "brokers", brokers, "topic", topic, "group", group)

	mux := http.NewServeMux()
	h := &httpServer{api: api, auth: NewAuthenticator(identity)}
	mux.HandleFunc("GET /reports/cdrs", h.ListCdrs)
	mux.HandleFunc("GET /reports/summary", h.Summarize)
	mux.HandleFunc("GET /reports/payments", h.Payments)
	mux.HandleFunc("GET /reports/charges", h.Charges)
	mux.HandleFunc("GET /reports/low-balance", h.LowBalance)
	mux.HandleFunc("GET /reports/live-calls", h.LiveCalls)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: httpAddr, Handler: observability.Middleware(mux), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("reports HTTP up", "addr", httpAddr, "identity", identity,
		"payments", payAddr, "charging", chgAddr, "switching", swAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http serve failed", "err", err)
		os.Exit(1)
	}
}
