package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	fraudpb "omnifoni/fraud/gen/fraudpb"
	"omnifoni/observability"
)

type Publisher interface {
	Publish(context.Context, *fraudpb.FraudAlert) error
}

type FraudServer struct {
	fraudpb.UnimplementedFraudServer
	store     *Store
	cfg       Config
	publisher Publisher
}

func NewFraudServer(store *Store, cfg Config, publisher Publisher) *FraudServer {
	return &FraudServer{store: store, cfg: cfg, publisher: publisher}
}

func (s *FraudServer) CheckCall(ctx context.Context, req *fraudpb.CheckCallRequest) (*fraudpb.CheckCallResponse, error) {
	ctx, span := otel.Tracer("omnifoni/fraud").Start(ctx, "fraud.check_call",
		trace.WithAttributes(
			attribute.String("account_id", req.GetAccountId()),
			attribute.String("destination", observability.Mask(req.GetDestination()))))
	defer span.End()

	if req.GetCallId() == "" || req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "call_id and account_id are required")
	}
	resp, alert, err := s.store.Reserve(ctx, req, s.cfg)
	if err != nil {
		slog.ErrorContext(ctx, "fraud reserve failed", "err", err)
		span.RecordError(err)
		return nil, status.Error(codes.Internal, "fraud store failure")
	}
	if alert != nil && s.publisher != nil {
		event := &fraudpb.FraudAlert{EventId: uuid.NewString(), Alert: alert}
		if err := s.publisher.Publish(ctx, event); err != nil {
			slog.ErrorContext(ctx, "fraud alert publish failed", "err", err)
			span.RecordError(err)
		}
	}
	return resp, nil
}

func (s *FraudServer) EndCall(ctx context.Context, req *fraudpb.EndCallRequest) (*fraudpb.EndCallResponse, error) {
	ctx, span := otel.Tracer("omnifoni/fraud").Start(ctx, "fraud.end_call",
		trace.WithAttributes(attribute.String("call_id", req.GetCallId())))
	defer span.End()

	if req.GetCallId() == "" {
		return nil, status.Error(codes.InvalidArgument, "call_id is required")
	}
	if err := s.store.EndCall(ctx, req.GetCallId()); err != nil {
		slog.ErrorContext(ctx, "fraud end call failed", "err", err)
		span.RecordError(err)
		return nil, status.Error(codes.Internal, "fraud store failure")
	}
	return &fraudpb.EndCallResponse{}, nil
}

func (s *FraudServer) ListAlerts(ctx context.Context, req *fraudpb.ListAlertsRequest) (*fraudpb.ListAlertsResponse, error) {
	ctx, span := otel.Tracer("omnifoni/fraud").Start(ctx, "fraud.list_alerts",
		trace.WithAttributes(attribute.String("account_id", req.GetAccountId())))
	defer span.End()

	alerts, next, err := s.store.ListAlerts(ctx, req.GetAccountId(), int(req.GetLimit()), req.GetCursor())
	if err != nil {
		if errors.Is(err, errInvalidAlertCursor) {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		slog.ErrorContext(ctx, "fraud list alerts failed", "err", err)
		span.RecordError(err)
		return nil, status.Error(codes.Internal, "fraud store failure")
	}
	return &fraudpb.ListAlertsResponse{Alerts: alerts, NextCursor: next}, nil
}
