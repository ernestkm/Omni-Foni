package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/reports/gen/reportspb"
)

type reportsServer struct {
	pb.UnimplementedReportsServer
	store    *Store
	payments PaymentSource
	charges  ChargeSource
}

const (
	defaultLimit = 100
	maxLimit     = 1000
)

func clampLimit(n int32) int {
	if n <= 0 {
		return defaultLimit
	}
	if int(n) > maxLimit {
		return maxLimit
	}
	return int(n)
}

func parseBound(s string, endOfDay bool) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q", s)
	}
	if endOfDay {
		return t.Add(24 * time.Hour), nil
	}
	return t, nil
}

func parseFilter(f *pb.CdrFilter) (Filter, error) {
	if f.GetFrom() == "" || f.GetTo() == "" {
		return Filter{}, errors.New("from and to are required")
	}
	from, err := parseBound(f.GetFrom(), false)
	if err != nil {
		return Filter{}, err
	}
	to, err := parseBound(f.GetTo(), true)
	if err != nil {
		return Filter{}, err
	}
	if !to.After(from) {
		return Filter{}, errors.New("to must be after from")
	}
	return Filter{
		AccountIDs:        f.GetAccountIds(),
		From:              from,
		To:                to,
		DestinationPrefix: f.GetDestinationPrefix(),
		TrunkID:           f.GetTrunkId(),
		EndReason:         f.GetEndReason(),
	}, nil
}

func encodeCursor(t time.Time, callID string) string {
	raw := strconv.FormatInt(t.UnixMilli(), 10) + "|" + callID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (*Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return nil, errors.New("malformed cursor")
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	return &Cursor{StartedAt: time.UnixMilli(ms).UTC(), CallID: parts[1]}, nil
}

func (s *reportsServer) ListCdrs(ctx context.Context, req *pb.ListCdrsRequest) (*pb.ListCdrsResponse, error) {
	f, err := parseFilter(req.GetFilter())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	limit := clampLimit(req.GetLimit())
	var cur *Cursor
	if c := req.GetCursor(); c != "" {
		cur, err = decodeCursor(c)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
	}

	ctx, span := otel.Tracer("reports").Start(ctx, "reports.list_cdrs")
	span.SetAttributes(attribute.String("from", f.From.Format(time.RFC3339)),
		attribute.String("to", f.To.Format(time.RFC3339)))
	defer span.End()

	rows, err := s.store.ListCdrs(ctx, f, limit+1, cur)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "store unavailable")
	}
	resp := &pb.ListCdrsResponse{Cdrs: []*pb.Cdr{}}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	for _, r := range rows {
		resp.Cdrs = append(resp.Cdrs, &pb.Cdr{
			CallId:         r.CallID,
			AccountId:      r.AccountID,
			Destination:    r.Destination,
			Cli:            r.CLI,
			StartedAtMs:    r.StartedAt.UnixMilli(),
			EndedAtMs:      r.EndedAt.UnixMilli(),
			ChargedSeconds: r.ChargedSeconds,
			EndReason:      r.EndReason,
			TrunkId:        r.TrunkID,
			RatePerSec:     r.RatePerSec,
			AmountMinor:    r.AmountMinor,
			RateMissing:    r.RateMissing,
		})
	}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		resp.NextCursor = encodeCursor(last.StartedAt, last.CallID)
	}
	return resp, nil
}

func parseGroup(g pb.GroupBy) (GroupBy, error) {
	switch g {
	case pb.GroupBy_DAY:
		return GroupDay, nil
	case pb.GroupBy_DESTINATION:
		return GroupDestination, nil
	case pb.GroupBy_ACCOUNT:
		return GroupAccount, nil
	case pb.GroupBy_TRUNK:
		return GroupTrunk, nil
	}
	return "", errors.New("group_by is required")
}

func (s *reportsServer) Summarize(ctx context.Context, req *pb.SummarizeRequest) (*pb.SummarizeResponse, error) {
	f, err := parseFilter(req.GetFilter())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	group, err := parseGroup(req.GetGroupBy())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ctx, span := otel.Tracer("reports").Start(ctx, "reports.summarize")
	span.SetAttributes(attribute.String("group_by", string(group)),
		attribute.String("from", f.From.Format(time.RFC3339)),
		attribute.String("to", f.To.Format(time.RFC3339)))
	defer span.End()

	rows, err := s.store.Summarize(ctx, f, group)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "store unavailable")
	}
	resp := &pb.SummarizeResponse{Rows: []*pb.SummaryRow{}, Totals: &pb.SummaryRow{}}
	for _, r := range rows {
		resp.Rows = append(resp.Rows, &pb.SummaryRow{
			Key: r.Key, Calls: r.Calls, Seconds: r.Seconds, AmountMinor: r.AmountMinor})
		resp.Totals.Calls += r.Calls
		resp.Totals.Seconds += r.Seconds
		resp.Totals.AmountMinor += r.AmountMinor
	}
	span.SetAttributes(attribute.Int("rows", len(resp.Rows)))
	return resp, nil
}
