package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"omnifoni/observability"
	routingpb "omnifoni/routing/gen/routingpb"
	sbcpb "omnifoni/sbc/gen/sbcpb"
	pb "omnifoni/switching/gen/switchingpb"
)

func main() {
	observability.SetupLogging("switching")
	shutdown, err := observability.Init(context.Background(), "switching")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	chargingAddr := os.Getenv("SWITCHING_CHARGING_ADDR")
	if chargingAddr == "" {
		// ponytail: dev default; production resolves the mesh identity
		chargingAddr = "localhost:50051"
	}
	addr := os.Getenv("SWITCHING_ADDR")
	if addr == "" {
		addr = ":50052"
	}

	brokers := []string{os.Getenv("SWITCHING_KAFKA")}
	if len(brokers) == 1 && brokers[0] == "" {
		// ponytail: dev default; production composes from secrets
		brokers[0] = "localhost:9092"
	}
	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordDeliveryTimeout(10*time.Second))
	if err != nil {
		slog.Error("cdr producer", "err", err)
		os.Exit(1)
	}
	defer prod.Close()

	cc, err := grpc.NewClient(chargingAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("charging client", "err", err)
		os.Exit(1)
	}

	routingAddr := os.Getenv("SWITCHING_ROUTING_ADDR")
	if routingAddr == "" {
		// ponytail: dev default; production resolves the mesh identity
		routingAddr = "localhost:50057"
	}
	rcc, err := grpc.NewClient(routingAddr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		slog.Error("routing client", "err", err)
		os.Exit(1)
	}

	// SBC is opt-in by env: unset skips the playbook (dev host runs);
	// compose always sets it, so production is fail-closed.
	reg := NewRegistryWithCDRs(&kafkaCDRs{topic: "cdrs.raw", prod: prod}).
		WithRouting(routingClient{routingpb.NewRoutingClient(rcc)})
	if sbcAddr := os.Getenv("SWITCHING_SBC_ADDR"); sbcAddr != "" {
		scc, err := grpc.NewClient(sbcAddr, append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, observability.ClientOptions()...)...)
		if err != nil {
			slog.Error("sbc client", "err", err)
			os.Exit(1)
		}
		reg = reg.WithSBC(sbcClient{sbcpb.NewSbcClient(scc)})
		slog.Info("sbc wired", "addr", sbcAddr)
	}

	if httpAddr := os.Getenv("SWITCHING_HTTP_ADDR"); httpAddr != "" {
		hs := &http.Server{Addr: httpAddr, Handler: newFacade(reg, cc), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("http facade", "err", err)
			}
		}()
		slog.Info("switching http facade", "addr", httpAddr)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}
	srv := grpc.NewServer(observability.ServerOptions()...)
	pb.RegisterSwitchingServer(srv, &switchingServer{reg: reg, cc: cc})
	if os.Getenv("SWITCHING_REFLECTION") == "1" {
		reflection.Register(srv)
	}
	slog.Info("switching up", "addr", addr, "charging", chargingAddr, "routing", routingAddr, "kafka", brokers[0])
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
