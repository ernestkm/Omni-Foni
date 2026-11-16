package main

import (
	"context"
	_ "embed"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	sbcpb "omnifoni/sbc/gen/sbcpb"
)

//go:embed migrate.sql
var schemaSQL string

// Migrate applies the schema. Idempotent, safe to run at boot.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

const (
	// failThreshold is the automatic blocking threshold: this many auth
	// failures from one source inside failWindow make the SBC deny it.
	failThreshold = 5
	failWindow    = time.Minute
)

// SBCServer implements sbc.v1.Sbc.
type SBCServer struct {
	sbcpb.UnimplementedSbcServer
	pool *pgxpool.Pool
}

func NewSBCServer(pool *pgxpool.Pool) *SBCServer {
	return &SBCServer{pool: pool}
}

// CheckCall runs the playbook: intrusion block, destination barring, caller
// id barring, trunk concurrent-call limit. The trunk slot is reserved in the
// same transaction that enforces the limit, so concurrent starts race safely.
func (s *SBCServer) CheckCall(ctx context.Context, req *sbcpb.CheckCallRequest) (resp *sbcpb.CheckCallResponse, err error) {
	ctx, span := otel.Tracer("omnifoni/sbc").Start(ctx, "sbc.check_call",
		trace.WithAttributes(attribute.String("trunk_id", req.GetTrunkId())))
	defer func() {
		if resp != nil && !resp.GetAllowed() {
			span.SetAttributes(attribute.String("deny_reason", resp.GetReason()))
		}
		span.End()
	}()
	if req.GetSourceIp() != "" {
		var fails int
		if err := s.pool.QueryRow(ctx,
			`SELECT count(*) FROM auth_failures
			 WHERE source_ip = $1 AND failed_at > now() - interval '1 second' * $2`,
			req.GetSourceIp(), int64(failWindow/time.Second)).Scan(&fails); err != nil {
			return nil, err
		}
		if fails >= failThreshold {
			return deny("ip_blocked"), nil
		}
	}
	if r, ok, err := s.barring(ctx, "destination", req.GetDestination()); err != nil {
		return nil, err
	} else if ok {
		return r, nil
	}
	if r, ok, err := s.barring(ctx, "cli", req.GetCli()); err != nil {
		return nil, err
	} else if ok {
		return r, nil
	}
	return s.reserve(ctx, req)
}

// barring applies the longest-prefix rule matching the value.
func (s *SBCServer) barring(ctx context.Context, matchType, value string) (*sbcpb.CheckCallResponse, bool, error) {
	if value == "" {
		return nil, false, nil
	}
	var action string
	err := s.pool.QueryRow(ctx,
		`SELECT action FROM barring_rules
		 WHERE match_type = $1 AND $2 LIKE prefix || '%'
		 ORDER BY length(prefix) DESC LIMIT 1`,
		matchType, value).Scan(&action)
	if err == pgx.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if action == "deny" {
		return deny("barring_" + matchType), true, nil
	}
	return nil, false, nil
}

// reserve inserts the call slot and denies when the trunk is over its limit.
// The advisory lock serializes reservations per trunk: with plain READ
// COMMITTED, concurrent starts can each see only committed rows plus their
// own insert and both sneak under the limit.
func (s *SBCServer) reserve(ctx context.Context, req *sbcpb.CheckCallRequest) (*sbcpb.CheckCallResponse, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtext('trunk:' || $1))`, req.GetTrunkId()); err != nil {
		return nil, err
	}

	var maxCalls int
	hasLimit := true
	err = tx.QueryRow(ctx,
		`SELECT max_calls FROM trunk_limits WHERE trunk_id = $1`,
		req.GetTrunkId()).Scan(&maxCalls)
	if err == pgx.ErrNoRows {
		hasLimit = false
	} else if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO active_calls (call_id, trunk_id) VALUES ($1, $2)
		 ON CONFLICT (call_id) DO NOTHING`,
		req.GetCallId(), req.GetTrunkId()); err != nil {
		return nil, err
	}
	if hasLimit {
		var n int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM active_calls WHERE trunk_id = $1`,
			req.GetTrunkId()).Scan(&n); err != nil {
			return nil, err
		}
		if n > maxCalls {
			return deny("trunk_limit_exceeded"), nil
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &sbcpb.CheckCallResponse{Allowed: true}, nil
}

// EndCall releases the slot.
func (s *SBCServer) EndCall(ctx context.Context, req *sbcpb.EndCallRequest) (*sbcpb.EndCallResponse, error) {
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM active_calls WHERE call_id = $1`, req.GetCallId()); err != nil {
		return nil, err
	}
	return &sbcpb.EndCallResponse{}, nil
}

// ReportAuthFailure records one failed authentication attempt.
func (s *SBCServer) ReportAuthFailure(ctx context.Context, req *sbcpb.ReportAuthFailureRequest) (*sbcpb.ReportAuthFailureResponse, error) {
	if req.GetSourceIp() == "" {
		return &sbcpb.ReportAuthFailureResponse{}, nil
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO auth_failures (source_ip) VALUES ($1)`, req.GetSourceIp()); err != nil {
		return nil, err
	}
	return &sbcpb.ReportAuthFailureResponse{}, nil
}

func deny(reason string) *sbcpb.CheckCallResponse {
	return &sbcpb.CheckCallResponse{Allowed: false, Reason: reason}
}
