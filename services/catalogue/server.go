package main

import (
	"context"
	_ "embed"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pb "omnifoni/catalogue/gen/cataloguepb"
)

//go:embed migrate.sql
var schemaSQL string

// Server serves rate lookups against the catalogue DB.
type Server struct {
	pb.UnimplementedCatalogueServer
	pool *pgxpool.Pool
}

func NewServer(pool *pgxpool.Pool) *Server { return &Server{pool: pool} }

// Migrate applies the schema. Idempotent, safe to run at boot.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

// GetRate resolves the longest-prefix rate for a destination. The empty prefix
// is a legal catch-all matching every destination.
func (s *Server) GetRate(ctx context.Context, req *pb.GetRateRequest) (*pb.GetRateResponse, error) {
	if req.GetDestination() == "" {
		return &pb.GetRateResponse{Found: false}, nil
	}
	var prefix, rate string
	err := s.pool.QueryRow(ctx,
		`SELECT prefix, rate_per_sec::text FROM rates
		 WHERE LEFT($1, LENGTH(prefix)) = prefix
		 ORDER BY LENGTH(prefix) DESC LIMIT 1`,
		req.GetDestination()).Scan(&prefix, &rate)
	if errors.Is(err, pgx.ErrNoRows) {
		return &pb.GetRateResponse{Found: false}, nil
	}
	if err != nil {
		return nil, err
	}
	return &pb.GetRateResponse{Found: true, MatchedPrefix: prefix, RatePerSec: rate}, nil
}

// PutRate upserts one rate row.
func (s *Server) PutRate(ctx context.Context, req *pb.PutRateRequest) (*pb.PutRateResponse, error) {
	if req.GetPrefix() == "" || req.GetRatePerSec() == "" {
		return nil, errors.New("prefix and rate_per_sec required")
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO rates (prefix, rate_per_sec) VALUES ($1, $2::numeric)
		 ON CONFLICT (prefix) DO UPDATE SET rate_per_sec = $2::numeric`,
		req.GetPrefix(), req.GetRatePerSec())
	if err != nil {
		return nil, err
	}
	return &pb.PutRateResponse{}, nil
}
