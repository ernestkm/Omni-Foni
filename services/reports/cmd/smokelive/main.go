// Command smokelive starts one call through switching, proves the reports
// live-call surface lists it, then stops it.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
	swAddr := env("SWITCHING_ADDR", "localhost:50052")
	rpAddr := env("REPORTS_ADDR", "localhost:50061")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	swConn, err := grpc.NewClient(swAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial switching:", err)
		os.Exit(1)
	}
	defer swConn.Close()
	rpConn, err := grpc.NewClient(rpAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial reports:", err)
		os.Exit(1)
	}
	defer rpConn.Close()

	sw := switchingpb.NewSwitchingClient(swConn)
	start, err := sw.StartCall(ctx, &switchingpb.StartCallRequest{
		AccountId: "acc-smoke-live", Destination: "27821110000", Cli: "27820000000",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "start call:", err)
		os.Exit(1)
	}
	if !start.GetAllowed() {
		fmt.Fprintf(os.Stderr, "start call denied: %s\n", start.GetReason())
		os.Exit(1)
	}
	defer func() {
		_, _ = sw.StopCall(context.Background(), &switchingpb.StopCallRequest{CallId: start.GetCallId()})
	}()

	list, err := pb.NewReportsClient(rpConn).ListLiveCalls(ctx,
		&pb.LiveCallsRequest{AccountIds: []string{"acc-smoke-live"}})
	if err != nil {
		fmt.Fprintln(os.Stderr, "list live calls:", err)
		os.Exit(1)
	}
	found := false
	for _, c := range list.GetCalls() {
		if c.GetCallId() == start.GetCallId() {
			found = true
		}
	}
	if !found {
		fmt.Fprintf(os.Stderr, "call %s missing from live report (%d rows)\n",
			start.GetCallId(), len(list.GetCalls()))
		os.Exit(1)
	}
	if _, err := sw.StopCall(ctx, &switchingpb.StopCallRequest{CallId: start.GetCallId()}); err != nil {
		fmt.Fprintln(os.Stderr, "stop call:", err)
		os.Exit(1)
	}
	fmt.Println("ok: live calls")
}
