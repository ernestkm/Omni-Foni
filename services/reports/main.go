package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
	schedDSN := os.Getenv("REPORTS_SCHED_DSN")
	retentionDays := 90
	if v := os.Getenv("REPORTS_SCHED_RETENTION_DAYS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			slog.Error("bad REPORTS_SCHED_RETENTION_DAYS", "value", v)
			os.Exit(1)
		}
		retentionDays = n
	}

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
	var sched *Scheduler
	var schedStore *SchedStore
	var signer *ArtifactSigner
	if schedDSN != "" {
		schedStore, err = OpenSched(ctx, schedDSN)
		if err != nil {
			slog.Error("sched db", "err", err)
			os.Exit(1)
		}
		defer schedStore.Close()
		if err := schedStore.Migrate(ctx); err != nil {
			slog.Error("sched migrate", "err", err)
			os.Exit(1)
		}
		prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
		if err != nil {
			slog.Error("kafka producer", "err", err)
			os.Exit(1)
		}
		defer prod.Close()
		signedTTL := int64(86400000)
		if v := os.Getenv("REPORTS_SIGNED_URL_TTL"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 1 {
				slog.Error("bad REPORTS_SIGNED_URL_TTL", "value", v)
				os.Exit(1)
			}
			signedTTL = n
		}
		signer = NewArtifactSigner(
			env("REPORTS_ARTIFACT_SIGNING_SECRET", "dev-only"),
			env("REPORTS_PUBLIC_BASE_URL", "http://localhost:50062"),
			time.Duration(signedTTL)*time.Millisecond,
		)
		sched = NewScheduler(schedStore, api, newKafkaReportPublisher(prod, env("REPORTS_SCHED_TOPIC", "report.ready")), signer)
		go func() {
			t := time.NewTicker(time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-t.C:
					if err := sched.RunDueSchedules(context.Background(), now); err != nil {
						slog.Error("scheduled reports", "err", err)
					}
					if now.Minute() == 0 {
						before := time.Now().AddDate(0, 0, -retentionDays)
						if n, err := schedStore.PruneArtifacts(context.Background(), before); err != nil {
							slog.Error("sched prune", "err", err)
						} else if n > 0 {
							slog.Info("pruned report artifacts", "count", n)
						}
					}
				}
			}
		}()
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
	h := &httpServer{api: api, auth: NewAuthenticator(identity), sched: sched, signer: signer}
	mux.HandleFunc("GET /reports/cdrs", h.ListCdrs)
	mux.HandleFunc("GET /reports/summary", h.Summarize)
	mux.HandleFunc("GET /reports/payments", h.Payments)
	mux.HandleFunc("GET /reports/charges", h.Charges)
	mux.HandleFunc("GET /reports/low-balance", h.LowBalance)
	mux.HandleFunc("GET /reports/live-calls", h.LiveCalls)
	mux.HandleFunc("POST /scheduled-reports", h.CreateSchedule)
	mux.HandleFunc("GET /scheduled-reports", h.ListSchedules)
	mux.HandleFunc("GET /scheduled-reports/{id}", h.GetSchedule)
	mux.HandleFunc("PATCH /scheduled-reports/{id}", h.PatchSchedule)
	mux.HandleFunc("DELETE /scheduled-reports/{id}", h.DeleteSchedule)
	mux.HandleFunc("GET /scheduled-reports/{id}/artifacts", h.ListArtifacts)
	mux.HandleFunc("GET /scheduled-reports/{id}/artifacts/{aid}/download", h.DownloadArtifact)
	mux.HandleFunc("POST /scheduled-reports/{id}/run", h.RunSchedule)
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
