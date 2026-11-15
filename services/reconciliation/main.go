package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	billingpb "omnifoni/billing/gen/billingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	billingAddr := env("BILLING_ADDR", "billing:50054")
	paymentsAddr := env("PAYMENTS_ADDR", "payments:50060")
	addr := env("RECONCILIATION_ADDR", ":50059")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	bConn, err := grpc.NewClient(billingAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("billing client", "err", err)
		os.Exit(1)
	}
	pConn, err := grpc.NewClient(paymentsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("payments client", "err", err)
		os.Exit(1)
	}
	srv := NewServer(
		&billingAdapter{client: billingpb.NewBillingClient(bConn)},
		&paymentsAdapter{client: paymentspb.NewPaymentsClient(pConn)},
		noopPublisher{},
	)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer()
	pb.RegisterReconciliationServer(g, srv)
	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()
	slog.Info("reconciliation up", "addr", addr, "billing", billingAddr, "payments", paymentsAddr)
	if err := g.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
