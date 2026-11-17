// Command smokereports queries the current month's day summary so the smoke
// test proves the reports API and its trace span work end to end.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "omnifoni/reports/gen/reportspb"
)

func main() {
	addr := "localhost:50061"
	if v := os.Getenv("REPORTS_ADDR"); v != "" {
		addr = v
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer conn.Close()

	now := time.Now().UTC()
	from := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(0, 1, 0)
	resp, err := pb.NewReportsClient(conn).Summarize(ctx, &pb.SummarizeRequest{
		Filter:  &pb.CdrFilter{From: from.Format("2006-01-02"), To: to.Format("2006-01-02")},
		GroupBy: pb.GroupBy_DAY,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "summarize:", err)
		os.Exit(1)
	}
	fmt.Printf("ok: reports summary rows=%d\n", len(resp.GetRows()))
}
