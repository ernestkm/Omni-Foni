package main

import (
	"log/slog"
	"net"
	"os"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	pb "omnifoni/switching/gen/switchingpb"
)

func main() {
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

	cc, err := grpc.NewClient(chargingAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("charging client", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}
	srv := grpc.NewServer()
	pb.RegisterSwitchingServer(srv, &switchingServer{reg: NewRegistryWithCDRs(&kafkaCDRs{topic: "cdrs.raw", prod: prod}), cc: cc})
	if os.Getenv("SWITCHING_REFLECTION") == "1" {
		reflection.Register(srv)
	}
	slog.Info("switching up", "addr", addr, "charging", chargingAddr, "kafka", brokers[0])
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
