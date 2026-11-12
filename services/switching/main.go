package main

import (
	"log/slog"
	"net"
	"os"

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
	pb.RegisterSwitchingServer(srv, &switchingServer{reg: NewRegistry(), cc: cc})
	if os.Getenv("SWITCHING_REFLECTION") == "1" {
		reflection.Register(srv)
	}
	slog.Info("switching up", "addr", addr, "charging", chargingAddr)
	if err := srv.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
