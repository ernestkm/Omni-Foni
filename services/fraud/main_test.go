package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestEnvInt(t *testing.T) {
	t.Setenv("FRAUD_TEST_INT", "7")
	if got := envInt("FRAUD_TEST_INT", 3); got != 7 {
		t.Fatalf("envInt = %d", got)
	}
	t.Setenv("FRAUD_TEST_INT", "")
	if got := envInt("FRAUD_TEST_INT", 3); got != 3 {
		t.Fatalf("default = %d", got)
	}
}

func TestServeGRPCStopsOnContextCancel(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- serveGRPC(ctx, grpc.NewServer(), lis) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveGRPC = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveGRPC did not stop on context cancel")
	}
}
