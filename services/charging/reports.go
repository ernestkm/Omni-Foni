package main

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/charging/gen/chargingpb"
)

const (
	defaultPageLimit = 100
	maxPageLimit     = 1000
)

const zeroUUID = "00000000-0000-0000-0000-000000000000"

type chargeCursor struct {
	ms        int64
	sessionID string
}

func encodeChargeCursor(ms int64, sessionID string) string {
	raw := strconv.FormatInt(ms, 10) + "|" + sessionID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeChargeCursor(s string) (chargeCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return chargeCursor{}, err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return chargeCursor{}, errors.New("malformed cursor")
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return chargeCursor{}, err
	}
	return chargeCursor{ms: ms, sessionID: parts[1]}, nil
}

type balanceCursor struct {
	balance   int64
	accountID string
}

func encodeBalanceCursor(balance int64, accountID string) string {
	raw := strconv.FormatInt(balance, 10) + "|" + accountID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeBalanceCursor(s string) (balanceCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return balanceCursor{}, err
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return balanceCursor{}, errors.New("malformed cursor")
	}
	balance, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return balanceCursor{}, err
	}
	return balanceCursor{balance: balance, accountID: parts[1]}, nil
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

// ListCharges returns one row per session, aggregating its ledger rows.
func (s *Server) ListCharges(ctx context.Context, req *pb.ListChargesRequest) (*pb.ListChargesResponse, error) {
	if req.GetToMs() < req.GetFromMs() {
		return nil, status.Error(codes.InvalidArgument, "to_ms must be >= from_ms")
	}
	limit := pageLimit(req.GetLimit())
	cur := chargeCursor{sessionID: zeroUUID}
	hasCursor := false
	if c := req.GetCursor(); c != "" {
		cc, err := decodeChargeCursor(c)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		cur, hasCursor = cc, true
	}

	rows, err := s.pool.Query(ctx,
		`SELECT l.session_id::text, l.account_id,
		        coalesce(s.charged_seconds, 0),
		        sum(l.amount_minor)::bigint,
		        (array_agg(l.balance_after ORDER BY l.ledger_id DESC))[1],
		        floor(extract(epoch FROM max(l.created_at)) * 1000)::bigint
		 FROM ledger l
		 LEFT JOIN sessions s ON s.session_id = l.session_id
		 WHERE (coalesce(cardinality($1::text[]), 0) = 0 OR l.account_id = ANY($1))
		 GROUP BY l.session_id, l.account_id, s.charged_seconds
		 HAVING max(l.created_at) >= to_timestamp($2::double precision / 1000.0)
		    AND max(l.created_at) <  to_timestamp($3::double precision / 1000.0)
		    AND (NOT $4 OR (max(l.created_at), l.session_id) <
		         (to_timestamp($5::double precision / 1000.0), $6::uuid))
		 ORDER BY max(l.created_at) DESC, l.session_id DESC
		 LIMIT $7`,
		req.GetAccountIds(), req.GetFromMs(), req.GetToMs(),
		hasCursor, cur.ms, cur.sessionID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type row struct {
		sessionID  string
		accountID  string
		seconds    int64
		amount     int64
		balance    int64
		lastCharge int64
	}
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.sessionID, &r.accountID, &r.seconds, &r.amount, &r.balance, &r.lastCharge); err != nil {
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
	resp := &pb.ListChargesResponse{Charges: []*pb.Charge{}}
	for _, r := range found {
		resp.Charges = append(resp.Charges, &pb.Charge{
			SessionId: r.sessionID, AccountId: r.accountID, ChargedSeconds: r.seconds,
			AmountMinor: r.amount, BalanceAfter: r.balance, LastChargedAtMs: r.lastCharge,
		})
	}
	if hasMore && len(found) > 0 {
		last := found[len(found)-1]
		resp.NextCursor = encodeChargeCursor(last.lastCharge, last.sessionID)
	}
	return resp, nil
}

// ListLowBalance returns accounts at or below the threshold, lowest first.
func (s *Server) ListLowBalance(ctx context.Context, req *pb.ListLowBalanceRequest) (*pb.ListLowBalanceResponse, error) {
	limit := pageLimit(req.GetLimit())
	var cur balanceCursor
	hasCursor := false
	if c := req.GetCursor(); c != "" {
		cc, err := decodeBalanceCursor(c)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		cur, hasCursor = cc, true
	}

	rows, err := s.pool.Query(ctx,
		`SELECT account_id, kind, balance, credit_limit, currency
		 FROM accounts
		 WHERE balance <= $1
		   AND (coalesce(cardinality($2::text[]), 0) = 0 OR account_id = ANY($2))
		   AND (NOT $3 OR (balance, account_id) > ($4, $5))
		 ORDER BY balance ASC, account_id ASC
		 LIMIT $6`,
		req.GetThresholdMinor(), req.GetAccountIds(),
		hasCursor, cur.balance, cur.accountID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type row struct {
		accountID   string
		kind        string
		balance     int64
		creditLimit int64
		currency    string
	}
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.accountID, &r.kind, &r.balance, &r.creditLimit, &r.currency); err != nil {
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
	resp := &pb.ListLowBalanceResponse{Accounts: []*pb.BalanceRow{}}
	for _, r := range found {
		resp.Accounts = append(resp.Accounts, &pb.BalanceRow{
			AccountId: r.accountID, Kind: r.kind, Balance: r.balance,
			CreditLimit: r.creditLimit, Currency: r.currency,
		})
	}
	if hasMore && len(found) > 0 {
		last := found[len(found)-1]
		resp.NextCursor = encodeBalanceCursor(last.balance, last.accountID)
	}
	return resp, nil
}
