package main

import (
	"context"
	"io"

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
