package main

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/payments/gen/paymentspb"
)

const (
	defaultPageLimit = 100
	maxPageLimit     = 1000
)

type paymentCursor struct {
	ms int64
	id int64
}

func encodePaymentCursor(ms, id int64) string {
	raw := strconv.FormatInt(ms, 10) + "|" + strconv.FormatInt(id, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodePaymentCursor(s string) (paymentCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return paymentCursor{}, err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return paymentCursor{}, errors.New("malformed cursor")
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return paymentCursor{}, err
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return paymentCursor{}, err
	}
	return paymentCursor{ms: ms, id: id}, nil
}

func pageLimit(n int32) int {
	if n <= 0 {
		return defaultPageLimit
	}
	if int(n) > maxPageLimit {
		return maxPageLimit
	}
	return int(n)
}

// ListPayments returns payments in a half-open epoch-ms window, newest first.
func (s *paymentsServer) ListPayments(ctx context.Context, req *pb.ListPaymentsRequest) (*pb.ListPaymentsResponse, error) {
	switch req.GetStatus() {
	case "", "recorded", "void":
	default:
		return nil, status.Error(codes.InvalidArgument, "status must be recorded or void")
	}
	if req.GetToMs() < req.GetFromMs() {
		return nil, status.Error(codes.InvalidArgument, "to_ms must be >= from_ms")
	}
	limit := pageLimit(req.GetLimit())
	var cur paymentCursor
	hasCursor := false
	if c := req.GetCursor(); c != "" {
		cc, err := decodePaymentCursor(c)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		cur, hasCursor = cc, true
	}

	rows, err := s.pool.Query(ctx,
		`SELECT payment_id, event_id, account_id, gateway, amount_minor, status,
		        floor(extract(epoch FROM created_at) * 1000)::bigint
		 FROM payments
		 WHERE created_at >= to_timestamp($1::double precision / 1000.0)
		   AND created_at <  to_timestamp($2::double precision / 1000.0)
		   AND ($3 = '' OR status = $3)
		   AND (coalesce(cardinality($4::text[]), 0) = 0 OR account_id = ANY($4))
		   AND (NOT $5 OR (created_at, payment_id) <
		        (to_timestamp($6::double precision / 1000.0), $7))
		 ORDER BY created_at DESC, payment_id DESC
		 LIMIT $8`,
		req.GetFromMs(), req.GetToMs(), req.GetStatus(), req.GetAccountIds(),
		hasCursor, cur.ms, cur.id, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type row struct {
		id        int64
		event     string
		account   string
		gateway   string
		amount    int64
		status    string
		createdMs int64
	}
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.event, &r.account, &r.gateway, &r.amount, &r.status, &r.createdMs); err != nil {
			return nil, err
		}
		found = append(found, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hasMore := len(found) > limit
	if hasMore {
		found = found[:limit]
	}
	resp := &pb.ListPaymentsResponse{Payments: []*pb.Payment{}}
	for _, r := range found {
		resp.Payments = append(resp.Payments, &pb.Payment{
			PaymentId: r.id, EventId: r.event, AccountId: r.account, Gateway: r.gateway,
			AmountMinor: r.amount, Status: r.status, CreatedAtMs: r.createdMs,
		})
	}
	if hasMore && len(found) > 0 {
		last := found[len(found)-1]
		resp.NextCursor = encodePaymentCursor(last.createdMs, last.id)
	}
	return resp, nil
}
