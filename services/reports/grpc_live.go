package main

import (
	"context"

	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"

	pb "omnifoni/reports/gen/reportspb"
	switchingpb "omnifoni/switching/gen/switchingpb"
)

// LiveCallSource is the switching read surface used by the live report.
type LiveCallSource interface {
	ListLiveCalls(context.Context, *switchingpb.ListLiveCallsRequest, ...grpc.CallOption) (*switchingpb.ListLiveCallsResponse, error)
}

func (s *reportsServer) ListLiveCalls(ctx context.Context, req *pb.LiveCallsRequest) (*pb.LiveCallsResponse, error) {
	ctx, span := otel.Tracer("reports").Start(ctx, "reports.list_live_calls")
	defer span.End()

	resp, err := s.live.ListLiveCalls(ctx, &switchingpb.ListLiveCallsRequest{AccountIds: req.GetAccountIds()})
	if err != nil {
		return nil, sourceError(err)
	}
	out := &pb.LiveCallsResponse{Calls: make([]*pb.LiveCall, 0, len(resp.GetCalls()))}
	for _, c := range resp.GetCalls() {
		out.Calls = append(out.Calls, &pb.LiveCall{
			CallId: c.GetCallId(), SessionId: c.GetSessionId(), AccountId: c.GetAccountId(),
			Destination: c.GetDestination(), Cli: c.GetCli(), TrunkId: c.GetTrunkId(),
			StartedAtMs: c.GetStartedAtMs(), ElapsedSeconds: c.GetElapsedSeconds(),
			MaxSeconds: c.GetMaxSeconds(), ChargedSeconds: c.GetChargedSeconds(),
		})
	}
	return out, nil
}
