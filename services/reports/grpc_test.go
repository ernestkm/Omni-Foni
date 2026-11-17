package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/reports/gen/reportspb"
)

func TestListCdrsValidation(t *testing.T) {
	srv := &reportsServer{store: store}
	ctx := context.Background()

	if _, err := srv.ListCdrs(ctx, &pb.ListCdrsRequest{Filter: &pb.CdrFilter{}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing bounds err = %v, want InvalidArgument", err)
	}
	if _, err := srv.ListCdrs(ctx, &pb.ListCdrsRequest{Filter: &pb.CdrFilter{From: "nope", To: "2026-11-12"}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad from err = %v, want InvalidArgument", err)
	}
	if _, err := srv.ListCdrs(ctx, &pb.ListCdrsRequest{Filter: &pb.CdrFilter{From: "2026-11-13", To: "2026-11-12"}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty range err = %v, want InvalidArgument", err)
	}
	good := &pb.CdrFilter{From: "2026-11-12", To: "2026-11-13"}
	if _, err := srv.ListCdrs(ctx, &pb.ListCdrsRequest{Filter: good, Cursor: "!!!"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad cursor err = %v, want InvalidArgument", err)
	}
}

func TestListCdrsPaginationAndFields(t *testing.T) {
	reset(t)
	start := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	seed(t,
		testRow("p1", "a1", "27820001", "trunk-1", "normal", start.Add(1*time.Minute), 60, 10),
		testRow("p2", "a1", "27820002", "trunk-1", "normal", start.Add(2*time.Minute), 60, 20),
		testRow("p3", "a1", "27820003", "trunk-1", "normal", start.Add(3*time.Minute), 60, 30),
	)
	srv := &reportsServer{store: store}
	filter := &pb.CdrFilter{AccountIds: []string{"a1"}, From: "2026-11-12", To: "2026-11-13"}

	page1, err := srv.ListCdrs(context.Background(), &pb.ListCdrsRequest{Filter: filter, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.GetCdrs()) != 2 || page1.GetCdrs()[0].GetCallId() != "p3" || page1.GetNextCursor() == "" {
		t.Fatalf("page1 = %+v", page1)
	}
	if page1.GetCdrs()[0].GetStartedAtMs() != start.Add(3*time.Minute).UnixMilli() ||
		page1.GetCdrs()[0].GetRatePerSec() != "0.012345" {
		t.Fatalf("page1 row = %+v", page1.GetCdrs()[0])
	}

	page2, err := srv.ListCdrs(context.Background(), &pb.ListCdrsRequest{Filter: filter, Limit: 2, Cursor: page1.GetNextCursor()})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.GetCdrs()) != 1 || page2.GetCdrs()[0].GetCallId() != "p1" || page2.GetNextCursor() != "" {
		t.Fatalf("page2 = %+v", page2)
	}
}

func TestListCdrsClampsLimit(t *testing.T) {
	reset(t)
	srv := &reportsServer{store: store}
	resp, err := srv.ListCdrs(context.Background(), &pb.ListCdrsRequest{
		Filter: &pb.CdrFilter{From: "2026-11-12", To: "2026-11-13"}, Limit: 5000})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetCdrs()) != 0 {
		t.Fatalf("rows = %d, want 0", len(resp.GetCdrs()))
	}
}

func TestSummarize(t *testing.T) {
	reset(t)
	day1 := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	seed(t,
		testRow("s1", "a1", "27820001", "trunk-1", "normal", day1, 60, 10),
		testRow("s2", "a1", "3555999", "trunk-2", "normal", day1, 30, 5),
		testRow("s3", "a2", "27820001", "trunk-1", "normal", day2, 60, 7),
	)
	srv := &reportsServer{store: store}
	resp, err := srv.Summarize(context.Background(), &pb.SummarizeRequest{
		Filter:  &pb.CdrFilter{From: "2026-11-12", To: "2026-11-14"},
		GroupBy: pb.GroupBy_DAY,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetRows()) != 2 || resp.GetRows()[0].GetKey() != "2026-11-12" {
		t.Fatalf("rows = %+v", resp.GetRows())
	}
	if got := resp.GetTotals(); got.GetCalls() != 3 || got.GetSeconds() != 150 || got.GetAmountMinor() != 22 {
		t.Fatalf("totals = %+v", got)
	}
	if _, err := srv.Summarize(context.Background(), &pb.SummarizeRequest{
		Filter: &pb.CdrFilter{From: "2026-11-12", To: "2026-11-14"}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unspecified group err = %v, want InvalidArgument", err)
	}
}

func TestSummarizePrefixLen(t *testing.T) {
	reset(t)
	day := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	seed(t,
		testRow("s1", "a1", "27820001", "trunk-1", "normal", day, 60, 10),
		testRow("s2", "a1", "3555999", "trunk-1", "normal", day, 30, 5),
	)
	srv := &reportsServer{store: store}
	filter := &pb.CdrFilter{From: "2026-11-12", To: "2026-11-13"}

	def, err := srv.Summarize(context.Background(), &pb.SummarizeRequest{
		Filter: filter, GroupBy: pb.GroupBy_PREFIX})
	if err != nil {
		t.Fatal(err)
	}
	if len(def.GetRows()) != 2 || def.GetRows()[0].GetKey() != "278" {
		t.Fatalf("default prefix = %+v", def.GetRows())
	}

	two, err := srv.Summarize(context.Background(), &pb.SummarizeRequest{
		Filter: filter, GroupBy: pb.GroupBy_PREFIX, PrefixLen: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(two.GetRows()) != 2 || two.GetRows()[0].GetKey() != "27" {
		t.Fatalf("prefix_len 2 = %+v", two.GetRows())
	}

	for _, n := range []int32{-1, 16} {
		if _, err := srv.Summarize(context.Background(), &pb.SummarizeRequest{
			Filter: filter, GroupBy: pb.GroupBy_PREFIX, PrefixLen: n}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("prefix_len %d err = %v, want InvalidArgument", n, err)
		}
	}
}
