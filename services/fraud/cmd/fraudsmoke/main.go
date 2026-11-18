package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	fraudpb "omnifoni/fraud/gen/fraudpb"
)

func env(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func main() {
	addr := env("FRAUD_ADDR", "localhost:50063")
	account := env("FRAUD_SMOKE_ACCOUNT", "acc-smoke-fraud")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail("dial fraud: %v", err)
	}
	defer conn.Close()
	client := fraudpb.NewFraudClient(conn)

	firstID := "smoke-fraud-" + uuid.NewString()
	first, err := client.CheckCall(ctx, &fraudpb.CheckCallRequest{
		CallId: firstID, AccountId: account,
		Destination: "9001234", Cli: "27820000000",
	})
	if err != nil {
		fail("first check: %v", err)
	}
	if !first.GetAllowed() {
		fail("first check denied: %s", first.GetReason())
	}

	secondID := "smoke-fraud-" + uuid.NewString()
	second, err := client.CheckCall(ctx, &fraudpb.CheckCallRequest{
		CallId: secondID, AccountId: account,
		Destination: "9001234", Cli: "27820000000",
	})
	if err != nil {
		fail("second check: %v", err)
	}
	if second.GetAllowed() || second.GetReason() != "fraud_velocity_account" {
		fail("want fraud_velocity_account, got %+v", second)
	}

	alerts, err := client.ListAlerts(ctx, &fraudpb.ListAlertsRequest{AccountId: account, Limit: 10})
	if err != nil {
		fail("list alerts: %v", err)
	}
	if len(alerts.GetAlerts()) == 0 {
		fail("no fraud alerts for %s", account)
	}

	if _, err := client.EndCall(ctx, &fraudpb.EndCallRequest{CallId: firstID}); err != nil {
		fail("end first call: %v", err)
	}
	fmt.Println("ok: fraud")
}
