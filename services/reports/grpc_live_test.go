package main

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/reports/gen/reportspb"
	switchingpb "omnifoni/switching/gen/switchingpb"
)

type fakeLive struct {
	req  *switchingpb.ListLiveCallsRequest
	resp *switchingpb.ListLiveCallsResponse
	err  error
}

func (f *fakeLive) ListLiveCalls(_ context.Context, req *switchingpb.ListLiveCallsRequest, _ ...grpc.CallOption) (*switchingpb.ListLiveCallsResponse, error) {
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func TestReportsListLiveCalls(t *testing.T) {
	fl := &fakeLive{resp: &switchingpb.ListLiveCallsResponse{Calls: []*switchingpb.LiveCall{{
		CallId: "c1", SessionId: "s1", AccountId: "a1", Destination: "2782", Cli: "cli",
		TrunkId: "trunk-1", StartedAtMs: 1000, ElapsedSeconds: 5, MaxSeconds: 600, ChargedSeconds: 4,
	}}}}
	s := &reportsServer{live: fl}

	resp, err := s.ListLiveCalls(context.Background(), &pb.LiveCallsRequest{AccountIds: []string{"a1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(fl.req.GetAccountIds()) != 1 || fl.req.GetAccountIds()[0] != "a1" {
		t.Fatalf("source request = %+v", fl.req)
	}
	if len(resp.GetCalls()) != 1 {
		t.Fatalf("calls = %d", len(resp.GetCalls()))
	}
	got := resp.GetCalls()[0]
	if got.GetCallId() != "c1" || got.GetSessionId() != "s1" || got.GetAccountId() != "a1" ||
		got.GetDestination() != "2782" || got.GetCli() != "cli" || got.GetTrunkId() != "trunk-1" ||
		got.GetStartedAtMs() != 1000 || got.GetElapsedSeconds() != 5 ||
		got.GetMaxSeconds() != 600 || got.GetChargedSeconds() != 4 {
		t.Fatalf("mapped = %+v", got)
	}

	fl.err = status.Error(codes.Unavailable, "down")
	if _, err := s.ListLiveCalls(context.Background(), &pb.LiveCallsRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable", err)
	}
}
