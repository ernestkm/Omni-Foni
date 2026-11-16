package main

import (
	"context"
	_ "embed"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	routingpb "omnifoni/routing/gen/routingpb"
	"omnifoni/observability"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

//go:embed migrate.sql
var schemaSQL string

// Migrate applies the schema. Idempotent, safe to run at boot.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

// RoutingServer implements routing.v1.Routing.
type RoutingServer struct {
	routingpb.UnimplementedRoutingServer
	pool *pgxpool.Pool
}

func NewRoutingServer(pool *pgxpool.Pool) *RoutingServer {
	return &RoutingServer{pool: pool}
}

// Resolve longest-prefix matches the number against the rules table.
// The empty trunk_id response means "no rule matched", not an error: callers
// decide whether unrouted numbers fail closed.
func (s *RoutingServer) Resolve(ctx context.Context, req *routingpb.ResolveRequest) (*routingpb.ResolveResponse, error) {
	if req.Number == "" {
		return &routingpb.ResolveResponse{}, nil
	}
	var trunk string
	// Length-descending prefix ladder, one match then stop.
	for n := len(req.Number); n > 0; n-- {
		err := s.pool.QueryRow(ctx,
			`SELECT trunk_id FROM rules WHERE prefix = $1`, req.Number[:n]).Scan(&trunk)
		if err == nil {
			return &routingpb.ResolveResponse{TrunkId: trunk}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
	}
	return &routingpb.ResolveResponse{}, nil
}

// PutRule upserts one prefix rule.
func (s *RoutingServer) PutRule(ctx context.Context, req *routingpb.PutRuleRequest) (*routingpb.PutRuleResponse, error) {
	if req.Prefix == "" || req.TrunkId == "" {
		return nil, status.Error(codes.InvalidArgument, "prefix and trunk_id required")
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO rules (prefix, trunk_id) VALUES ($1, $2)
		 ON CONFLICT (prefix) DO UPDATE SET trunk_id = EXCLUDED.trunk_id`,
		req.Prefix, req.TrunkId); err != nil {
		return nil, err
	}
	return &routingpb.PutRuleResponse{}, nil
}

// PutLcrRoute upserts one LCR candidate.
func (s *RoutingServer) PutLcrRoute(ctx context.Context, req *routingpb.PutLcrRouteRequest) (*routingpb.PutLcrRouteResponse, error) {
	if req.Prefix == "" || req.TrunkId == "" || req.CostPerSec <= 0 {
		return nil, status.Error(codes.InvalidArgument, "prefix, trunk_id and positive cost required")
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO lcr_routes (prefix, trunk_id, cost_per_sec) VALUES ($1, $2, $3)
		 ON CONFLICT (prefix, trunk_id) DO UPDATE SET cost_per_sec = EXCLUDED.cost_per_sec`,
		req.Prefix, req.TrunkId, req.CostPerSec); err != nil {
		return nil, err
	}
	return &routingpb.PutLcrRouteResponse{}, nil
}

// ResolveLcr picks the cheapest enabled candidate trunk for the number,
// breaking ties on longest prefix then trunk id.
func (s *RoutingServer) ResolveLcr(ctx context.Context, req *routingpb.ResolveRequest) (resp *routingpb.ResolveLcrResponse, err error) {
	ctx, span := otel.Tracer("omnifoni/routing").Start(ctx, "routing.resolve_lcr",
		trace.WithAttributes(attribute.String("destination", observability.Mask(req.GetNumber()))))
	defer func() {
		if resp != nil {
			span.SetAttributes(
				attribute.String("trunk_id", resp.GetTrunkId()),
				attribute.Int64("cost_per_sec", resp.GetCostPerSec()))
		}
		span.End()
	}()
	if req.Number == "" {
		return &routingpb.ResolveLcrResponse{}, nil
	}
	// Candidate match is left-anchored; the text_pattern_ops index keeps the
	// 1000-route dataset a single index scan.
	var trunk string
	var cost int64
	err = s.pool.QueryRow(ctx,
		`SELECT r.trunk_id, r.cost_per_sec
		 FROM lcr_routes r JOIN trunks t USING (trunk_id)
		 WHERE r.prefix <= $1 AND $1 LIKE r.prefix || '%' AND t.enabled
		 ORDER BY r.cost_per_sec ASC, length(r.prefix) DESC, r.trunk_id ASC
		 LIMIT 1`,
		req.Number).Scan(&trunk, &cost)
	if errors.Is(err, pgx.ErrNoRows) {
		return &routingpb.ResolveLcrResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	return &routingpb.ResolveLcrResponse{TrunkId: trunk, CostPerSec: cost}, nil
}

// PutTrunk registers a trunk's enabled state (LCR validity).
func (s *RoutingServer) PutTrunk(ctx context.Context, req *routingpb.PutTrunkRequest) (*routingpb.PutTrunkResponse, error) {
	if req.TrunkId == "" {
		return nil, status.Error(codes.InvalidArgument, "trunk_id required")
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO trunks (trunk_id, enabled) VALUES ($1, $2)
		 ON CONFLICT (trunk_id) DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = now()`,
		req.TrunkId, req.Enabled); err != nil {
		return nil, err
	}
	return &routingpb.PutTrunkResponse{}, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	observability.SetupLogging("routing")
	shutdown, err := observability.Init(context.Background(), "routing")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	dsn := env("ROUTING_DSN", "postgres://postgres:dev-only@postgres:5432/routing")
	addr := env("ROUTING_ADDR", ":50057")
	reflectFlag := env("ROUTING_REFLECTION", "") == "1"

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		slog.Error("db pool", "err", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer(observability.ServerOptions()...)
	routingpb.RegisterRoutingServer(g, NewRoutingServer(pool))
	if reflectFlag {
		reflection.Register(g)
	}
	slog.Info("routing up", "addr", addr)
	if err := g.Serve(lis); err != nil {
		slog.Error("serve failed", "err", err)
		os.Exit(1)
	}
}
