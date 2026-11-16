package main

import (
	"context"
	_ "embed"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	pb "omnifoni/charging/gen/chargingpb"
	"omnifoni/observability"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

//go:embed migrate.sql
var schemaSQL string

const (
	reasonNoAccount           = "no_account"
	reasonNoRate              = "no_rate"
	reasonInsufficientBalance = "insufficient_balance"
	reasonInternalError       = "internal_error"
)

var errDeny = errors.New("deny")

type Server struct {
	pb.UnimplementedChargingServer
	pool *pgxpool.Pool
	cat  cataloguepb.CatalogueClient
}

func NewServer(pool *pgxpool.Pool, cat cataloguepb.CatalogueClient) *Server {
	return &Server{pool: pool, cat: cat}
}

// Migrate applies the schema. Idempotent, safe to run at boot.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

// Authorize resolves the rate and reserves the maximum affordable call
// duration. Fail-closed: every internal error denies.
func (s *Server) Authorize(ctx context.Context, req *pb.AuthorizeRequest) (resp *pb.AuthorizeResponse, err error) {
	ctx, span := otel.Tracer("omnifoni/charging").Start(ctx, "charging.authorize",
		trace.WithAttributes(
			attribute.String("account", req.GetAccountId()),
			attribute.String("destination", observability.Mask(req.GetDestination()))))
	defer func() {
		if resp != nil && !resp.GetAllow() {
			span.SetAttributes(attribute.String("deny_reason", resp.GetReason()))
		}
		span.End()
	}()
	if req.GetAccountId() == "" || req.GetDestination() == "" {
		return deny(reasonInternalError), nil
	}
	resp, err = s.authorizeTx(ctx, req)
	if err != nil {
		if errors.Is(err, errDeny) {
			return resp, nil
		}
		slog.Error("authorize failed", "account", req.GetAccountId(), "err", err)
		return deny(reasonInternalError), nil
	}
	return resp, nil
}

// Charge applies the cumulative total. Retries with the same or smaller total
// are no-ops (applied=false), so at-least-once callers cannot double-charge.
// denied=true means insufficient funds: the switch must terminate the call.
func (s *Server) Charge(ctx context.Context, req *pb.ChargeRequest) (resp *pb.ChargeResponse, err error) {
	ctx, span := otel.Tracer("omnifoni/charging").Start(ctx, "charging.charge",
		trace.WithAttributes(
			attribute.String("session_id", req.GetSessionId()),
			attribute.Int64("total_seconds", req.GetTotalSeconds())))
	defer span.End()
	if req.GetSessionId() == "" || req.GetTotalSeconds() < 0 {
		return chargeErr(), nil
	}
	resp, err = s.chargeTx(ctx, req)
	if err != nil {
		slog.Error("charge failed", "session", req.GetSessionId(), "err", err)
		return chargeErr(), nil
	}
	return resp, nil
}

func (s *Server) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.ReleaseResponse, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE sessions SET released = TRUE WHERE session_id = $1 AND NOT released`,
		req.GetSessionId())
	if err != nil {
		slog.Error("release failed", "session", req.GetSessionId(), "err", err)
		return &pb.ReleaseResponse{Released: false}, nil
	}
	return &pb.ReleaseResponse{Released: tag.RowsAffected() == 1}, nil
}

func (s *Server) authorizeTx(ctx context.Context, req *pb.AuthorizeRequest) (*pb.AuthorizeResponse, error) {
	// Rate resolution happens outside the account tx: the Catalogue owns rate
	// truth, and a rate snapshot is stored on the session.
	if s.cat == nil {
		return nil, errors.New("no catalogue client")
	}
	crate, err := s.cat.GetRate(ctx, &cataloguepb.GetRateRequest{Destination: req.GetDestination()})
	if err != nil {
		return nil, err
	}
	if !crate.GetFound() {
		return deny(reasonNoRate), errDeny
	}
	rate := crate.GetRatePerSec()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var kind string
	var balance, creditLimit int64
	err = tx.QueryRow(ctx,
		`SELECT kind, balance, credit_limit FROM accounts WHERE account_id = $1`,
		req.GetAccountId()).Scan(&kind, &balance, &creditLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return deny(reasonNoAccount), errDeny
	}
	if err != nil {
		return nil, err
	}

	var maxSeconds int64
	err = tx.QueryRow(ctx, `SELECT FLOOR($1::numeric / $2::numeric)::bigint`,
		availableFor(kind, balance, creditLimit), rate).Scan(&maxSeconds)
	if err != nil {
		return nil, err
	}
	if maxSeconds < 1 {
		return deny(reasonInsufficientBalance), errDeny
	}

	sessionID := uuid.New().String()
	_, err = tx.Exec(ctx,
		`INSERT INTO sessions (session_id, account_id, rate_per_sec, reserved_seconds)
		 VALUES ($1, $2, $3::numeric, $4)`,
		sessionID, req.GetAccountId(), rate, maxSeconds)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	slog.Info("authorized", "session", sessionID, "account", req.GetAccountId(),
		"max_seconds", maxSeconds)
	return &pb.AuthorizeResponse{Allow: true, MaxSeconds: maxSeconds, SessionId: sessionID}, nil
}

func (s *Server) chargeTx(ctx context.Context, req *pb.ChargeRequest) (*pb.ChargeResponse, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var accountID, rate string
	var charged int64
	var released bool
	err = tx.QueryRow(ctx,
		`SELECT account_id, rate_per_sec::text, charged_seconds, released
		 FROM sessions WHERE session_id = $1 FOR UPDATE`,
		req.GetSessionId()).Scan(&accountID, &rate, &charged, &released)
	if errors.Is(err, pgx.ErrNoRows) {
		return chargeNoopUnknown(), nil
	}
	if err != nil {
		return nil, err
	}
	if released || req.GetTotalSeconds() <= charged {
		return chargeNoop(ctx, tx, accountID), nil
	}
	delta := req.GetTotalSeconds() - charged

	var cost int64
	err = tx.QueryRow(ctx, `SELECT ROUND($1::numeric * $2::numeric)::bigint`,
		delta, rate).Scan(&cost)
	if err != nil {
		return nil, err
	}

	var balance int64
	err = tx.QueryRow(ctx,
		`UPDATE accounts SET balance = balance - $2
		 WHERE account_id = $1 AND (kind = 'postpaid' OR balance >= $2)
		 RETURNING balance`,
		accountID, cost).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		// Insufficient funds: no state change, switch terminates the call.
		return &pb.ChargeResponse{Applied: false, Denied: true,
			ChargedSeconds: charged}, nil
	}
	if err != nil {
		return nil, err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE sessions SET charged_seconds = $2
		 WHERE session_id = $1 AND charged_seconds < $2`,
		req.GetSessionId(), req.GetTotalSeconds())
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return chargeNoop(ctx, tx, accountID), nil
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO ledger (session_id, account_id, amount_minor, balance_after)
		 VALUES ($1, $2, $3, $4)`,
		req.GetSessionId(), accountID, -cost, balance)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &pb.ChargeResponse{Applied: true, Denied: false,
		ChargedSeconds: req.GetTotalSeconds(), BalanceMinor: balance}, nil
}

// availableFor returns the spendable amount in minor units. Postpaid balances
// start at 0 and go negative as they are charged, so spendable is balance plus
// credit limit.
func availableFor(kind string, balance, creditLimit int64) int64 {
	if kind == "postpaid" {
		return balance + creditLimit
	}
	return balance
}

func deny(reason string) *pb.AuthorizeResponse {
	return &pb.AuthorizeResponse{Allow: false, Reason: reason}
}

func chargeErr() *pb.ChargeResponse {
	return &pb.ChargeResponse{Applied: false, Denied: false}
}

// chargeNoopUnknown handles charges for sessions we never authorized (e.g.
// after state loss): apply nothing, surface nothing. The caller may release.
func chargeNoopUnknown() *pb.ChargeResponse {
	return &pb.ChargeResponse{Applied: false, Denied: false}
}

// chargeNoop echoes the current balance so callers can stay in sync on
// idempotent replays.
func chargeNoop(ctx context.Context, tx pgx.Tx, accountID string) *pb.ChargeResponse {
	var balance int64
	if err := tx.QueryRow(ctx,
		`SELECT balance FROM accounts WHERE account_id = $1`, accountID).Scan(&balance); err != nil {
		return chargeErr()
	}
	return &pb.ChargeResponse{Applied: false, Denied: false, BalanceMinor: balance}
}
