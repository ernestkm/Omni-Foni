// Command smokerecon calls ReconcilePeriod on the dev reconciliation service
// and asserts well-formed results. Used by ops/smoke.sh.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"omnifoni/observability"

	pb "omnifoni/reconciliation/gen/reconciliationpb"
)

func main() {
	addr := flag.String("addr", "localhost:50059", "reconciliation gRPC address")
	flag.Parse()
	conn, err := grpc.NewClient(*addr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
	if err != nil {
		fmt.Println("FAIL:", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewReconciliationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	period := time.Now().UTC().Format("2006-01")
	resp, err := client.ReconcilePeriod(ctx, &pb.ReconcilePeriodRequest{Period: period})
	if err != nil {
		fmt.Println("FAIL:", err)
		os.Exit(1)
	}
	if st := resp.GetLedgerVsInvoices().GetStatus(); st != "ok" && st != "drift" {
		fmt.Println("FAIL: ledger status", st)
		os.Exit(1)
	}
	for _, a := range resp.GetAccounts() {
		switch a.GetStatus() {
		case "paid", "underpaid", "overpaid", "unpaid", "no_activity":
		default:
			fmt.Println("FAIL: account status", a.GetStatus())
			os.Exit(1)
		}
	}
	fmt.Println("ok: reconcile", resp.GetLedgerVsInvoices().GetPeriod())
}
