package main

import (
	"context"
	"io"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	pb "omnifoni/switching/gen/switchingpb"
)

// switchingServer adapts the Registry to the gRPC service.
type switchingServer struct {
	pb.UnimplementedSwitchingServer
	reg *Registry
	cc  *grpc.ClientConn
}

func (s *switchingServer) StartCall(ctx context.Context, req *pb.StartCallRequest) (*pb.StartCallResponse, error) {
	return s.reg.StartCall(ctx, s.cc, req)
}

func (s *switchingServer) StopCall(ctx context.Context, req *pb.StopCallRequest) (*pb.StopCallResponse, error) {
	ctx, span := otel.Tracer("omnifoni/switching").Start(ctx, "switching.stop_call",
		trace.WithAttributes(attribute.String("call_id", req.GetCallId())))
	defer span.End()
	_ = ctx
	return &pb.StopCallResponse{Stopped: s.reg.StopCall(req.GetCallId())}, nil
}

func (s *switchingServer) CallEvents(req *pb.CallEventsRequest, stream pb.Switching_CallEventsServer) error {
	s.reg.mu.Lock()
	c := s.reg.calls[req.GetCallId()]
	s.reg.mu.Unlock()
	if c == nil {
		return io.EOF // unknown or already-finished call
	}
	return c.Stream(func(ev *pb.CallEvent) error {
		return stream.Send(ev)
	})
}

func (s *switchingServer) ListLiveCalls(ctx context.Context, req *pb.ListLiveCallsRequest) (*pb.ListLiveCallsResponse, error) {
	ctx, span := otel.Tracer("omnifoni/switching").Start(ctx, "switching.list_live_calls",
		trace.WithAttributes(attribute.Int("account_ids", len(req.GetAccountIds()))))
	defer span.End()
	_ = ctx

	snaps := s.reg.LiveCalls(req.GetAccountIds())
	resp := &pb.ListLiveCallsResponse{Calls: make([]*pb.LiveCall, 0, len(snaps))}
	now := time.Now()
	for _, c := range snaps {
		elapsed := int64(now.Sub(c.StartedAt).Seconds())
		if elapsed < 0 {
			elapsed = 0
		}
		resp.Calls = append(resp.Calls, &pb.LiveCall{
			CallId: c.CallID, SessionId: c.SessionID, AccountId: c.AccountID,
			Destination: c.Destination, Cli: c.CLI, TrunkId: c.TrunkID,
			StartedAtMs: c.StartedAt.UnixMilli(), ElapsedSeconds: elapsed,
			MaxSeconds: c.MaxSeconds, ChargedSeconds: c.ChargedSeconds,
		})
	}
	return resp, nil
}
