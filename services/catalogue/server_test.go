package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	pb "omnifoni/catalogue/gen/cataloguepb"
)

func dsn() string {
	if s := os.Getenv("CATALOGUE_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/catalogue"
}

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	pool, err = pgxpool.New(ctx, dsn())
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func seed(t *testing.T, prefix, rate string) {
	t.Helper()
	_, err := NewServer(pool).PutRate(context.Background(),
		&pb.PutRateRequest{Prefix: prefix, RatePerSec: rate})
	if err != nil {
		t.Fatal(err)
	}
}

// TestLongestPrefixMatch: the golden prefix set resolves longest-prefix wins
// and clean misses, identical to the contract Charging consumes.
func TestLongestPrefixMatch(t *testing.T) {
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE rates`); err != nil {
		t.Fatal(err)
	}
	seed(t, "1", "0.250000")
	seed(t, "27", "0.100000")
	seed(t, "2782", "0.050000")
	s := NewServer(pool)

	for _, c := range []struct {
		dest, wantPrefix, wantRate string
	}{
		{"27821234567", "2782", "0.050000"},
		{"27311234567", "27", "0.100000"},
		{"12125550123", "1", "0.250000"},
	} {
		r, err := s.GetRate(ctx, &pb.GetRateRequest{Destination: c.dest})
		if err != nil {
			t.Fatal(err)
		}
		if !r.Found || r.MatchedPrefix != c.wantPrefix || r.RatePerSec != c.wantRate {
			t.Fatalf("%s: got %+v", c.dest, r)
		}
	}

	r, err := s.GetRate(ctx, &pb.GetRateRequest{Destination: "447900000000"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Found {
		t.Fatalf("want miss, got %+v", r)
	}

	// PutRate upserts: same prefix gets the new value.
	seed(t, "2782", "0.060000")
	r, err = s.GetRate(ctx, &pb.GetRateRequest{Destination: "27821234567"})
	if err != nil {
		t.Fatal(err)
	}
	if r.RatePerSec != "0.060000" {
		t.Fatalf("upsert failed: %+v", r)
	}
}
