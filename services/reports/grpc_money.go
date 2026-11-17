package main

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chargingpb "omnifoni/charging/gen/chargingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
	pb "omnifoni/reports/gen/reportspb"
)

// PaymentSource is the payments read surface used by the report handlers.
type PaymentSource interface {
	ListPayments(context.Context, *paymentspb.ListPaymentsRequest) (*paymentspb.ListPaymentsResponse, error)
}

// ChargeSource is the charging read surface used by the report handlers.
type ChargeSource interface {
	ListCharges(context.Context, *chargingpb.ListChargesRequest) (*chargingpb.ListChargesResponse, error)
	ListLowBalance(context.Context, *chargingpb.ListLowBalanceRequest) (*chargingpb.ListLowBalanceResponse, error)
}

func parseMoneyFilter(f *pb.MoneyFilter) (from, to time.Time, err error) {
	if f.GetFrom() == "" || f.GetTo() == "" {
		return time.Time{}, time.Time{}, errors.New("from and to are required")
	}
	from, err = parseBound(f.GetFrom(), false)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	to, err = parseBound(f.GetTo(), true)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if !to.After(from) {
		return time.Time{}, time.Time{}, errors.New("to must be after from")
	}
	return from, to, nil
}

// sourceError keeps InvalidArgument and hides everything else as Unavailable.
func sourceError(err error) error {
	if status.Code(err) == codes.InvalidArgument {
		return err
	}
	return status.Error(codes.Unavailable, "source unavailable")
}

func (s *reportsServer) ListPayments(ctx context.Context, req *pb.ListPaymentsRequest) (*pb.ListPaymentsResponse, error) {
	from, to, err := parseMoneyFilter(req.GetFilter())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, span := otel.Tracer("reports").Start(ctx, "reports.list_payments")
	span.SetAttributes(attribute.String("from", from.Format(time.RFC3339)),
		attribute.String("to", to.Format(time.RFC3339)))
	defer span.End()

	resp, err := s.payments.ListPayments(ctx, &paymentspb.ListPaymentsRequest{
		AccountIds: req.GetFilter().GetAccountIds(),
		FromMs:     from.UnixMilli(),
		ToMs:       to.UnixMilli(),
		Status:     req.GetStatus(),
		Limit:      int32(clampLimit(req.GetLimit())),
		Cursor:     req.GetCursor(),
	})
	if err != nil {
		return nil, sourceError(err)
	}
	out := &pb.ListPaymentsResponse{Payments: []*pb.Payment{}, NextCursor: resp.GetNextCursor()}
	for _, p := range resp.GetPayments() {
		out.Payments = append(out.Payments, &pb.Payment{
			PaymentId: p.GetPaymentId(), EventId: p.GetEventId(), AccountId: p.GetAccountId(),
			Gateway: p.GetGateway(), AmountMinor: p.GetAmountMinor(), Status: p.GetStatus(),
			CreatedAtMs: p.GetCreatedAtMs(),
		})
	}
	return out, nil
}

func (s *reportsServer) ListCharges(ctx context.Context, req *pb.ListChargesRequest) (*pb.ListChargesResponse, error) {
	from, to, err := parseMoneyFilter(req.GetFilter())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, span := otel.Tracer("reports").Start(ctx, "reports.list_charges")
	span.SetAttributes(attribute.String("from", from.Format(time.RFC3339)),
		attribute.String("to", to.Format(time.RFC3339)))
	defer span.End()

	resp, err := s.charges.ListCharges(ctx, &chargingpb.ListChargesRequest{
		AccountIds: req.GetFilter().GetAccountIds(),
		FromMs:     from.UnixMilli(),
		ToMs:       to.UnixMilli(),
		Limit:      int32(clampLimit(req.GetLimit())),
		Cursor:     req.GetCursor(),
	})
	if err != nil {
		return nil, sourceError(err)
	}
	out := &pb.ListChargesResponse{Charges: []*pb.Charge{}, NextCursor: resp.GetNextCursor()}
	for _, c := range resp.GetCharges() {
		out.Charges = append(out.Charges, &pb.Charge{
			SessionId: c.GetSessionId(), AccountId: c.GetAccountId(),
			ChargedSeconds: c.GetChargedSeconds(), AmountMinor: c.GetAmountMinor(),
			BalanceAfter: c.GetBalanceAfter(), LastChargedAtMs: c.GetLastChargedAtMs(),
		})
	}
	return out, nil
}

func (s *reportsServer) ListLowBalance(ctx context.Context, req *pb.LowBalanceRequest) (*pb.LowBalanceResponse, error) {
	ctx, span := otel.Tracer("reports").Start(ctx, "reports.list_low_balance")
	defer span.End()

	resp, err := s.charges.ListLowBalance(ctx, &chargingpb.ListLowBalanceRequest{
		AccountIds:     req.GetAccountIds(),
		ThresholdMinor: req.GetThresholdMinor(),
		Limit:          int32(clampLimit(req.GetLimit())),
		Cursor:         req.GetCursor(),
	})
	if err != nil {
		return nil, sourceError(err)
	}
	out := &pb.LowBalanceResponse{Accounts: []*pb.BalanceRow{}, NextCursor: resp.GetNextCursor()}
	for _, a := range resp.GetAccounts() {
		out.Accounts = append(out.Accounts, &pb.BalanceRow{
			AccountId: a.GetAccountId(), Kind: a.GetKind(), Balance: a.GetBalance(),
			CreditLimit: a.GetCreditLimit(), Currency: a.GetCurrency(),
		})
	}
	return out, nil
}
