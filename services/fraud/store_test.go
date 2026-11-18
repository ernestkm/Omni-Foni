package main

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func fraudDSN() string {
	if dsn := os.Getenv("FRAUD_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:dev-only@localhost:5432/fraud"
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), fraudDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := &Store{pool: pool}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(context.Background(),
		`TRUNCATE fraud_controls, prefix_blocks, cli_blocks, risk_prefixes,
		 daily_usage, reservations, processed_cdrs, alerts CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestMigrateCreatesFraudSchema(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO fraud_controls(account_id, max_calls_per_day)
		 VALUES ('acc-schema', 3)`); err != nil {
		t.Fatal(err)
	}
	var cap int
	if err := s.pool.QueryRow(ctx,
		`SELECT max_calls_per_day FROM fraud_controls WHERE account_id = 'acc-schema'`).Scan(&cap); err != nil {
		t.Fatal(err)
	}
	if cap != 3 {
		t.Fatalf("cap = %d, want 3", cap)
	}
}
