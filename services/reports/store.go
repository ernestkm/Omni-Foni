package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type CdrRow struct {
	CallID, AccountID, Destination, CLI string
	StartedAt, EndedAt                  time.Time
	ChargedSeconds                      int64
	EndReason, TrunkID                  string
	RatePerSec                          string
	AmountMinor                         int64
	RateMissing                         bool
}

type Filter struct {
	AccountIDs         []string
	From, To           time.Time
	DestinationPrefix  string
	TrunkID, EndReason string
}

type Cursor struct {
	StartedAt time.Time
	CallID    string
}

type GroupBy string

const (
	GroupDay         GroupBy = "day"
	GroupDestination GroupBy = "destination"
	GroupAccount     GroupBy = "account"
	GroupTrunk       GroupBy = "trunk"
)

type SummaryRow struct {
	Key         string
	Calls       int64
	Seconds     int64
	AmountMinor int64
}

type Store struct {
	db clickhouse.Conn
}

const schemaDDL = `CREATE DATABASE IF NOT EXISTS reports`

const tableDDL = `
CREATE TABLE IF NOT EXISTS reports.cdrs (
  call_id         String,
  account_id      String,
  destination     String,
  cli             String,
  started_at      DateTime64(3, 'UTC'),
  ended_at        DateTime64(3, 'UTC'),
  charged_seconds Int64,
  end_reason      LowCardinality(String),
  trunk_id        LowCardinality(String),
  rate_per_sec    Decimal(14, 6),
  amount_minor    Int64,
  rate_missing    Bool,
  ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY (account_id, started_at, call_id)`

func Open(ctx context.Context, dsn string) (*Store, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Migrate(ctx context.Context) error {
	if err := s.db.Exec(ctx, schemaDDL); err != nil {
		return err
	}
	return s.db.Exec(ctx, tableDDL)
}

func (s *Store) Insert(ctx context.Context, rows []CdrRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := s.db.PrepareBatch(ctx, `INSERT INTO reports.cdrs
		(call_id, account_id, destination, cli, started_at, ended_at, charged_seconds,
		 end_reason, trunk_id, rate_per_sec, amount_minor, rate_missing)`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(r.CallID, r.AccountID, r.Destination, r.CLI, r.StartedAt, r.EndedAt,
			r.ChargedSeconds, r.EndReason, r.TrunkID, r.RatePerSec, r.AmountMinor, r.RateMissing); err != nil {
			_ = b.Abort()
			return err
		}
	}
	return b.Send()
}

func (f Filter) where() (string, []any) {
	clauses := []string{
		"started_at >= toDateTime64(?, 3, 'UTC')",
		"started_at < toDateTime64(?, 3, 'UTC')",
	}
	args := []any{f.From.UTC(), f.To.UTC()}
	if len(f.AccountIDs) > 0 {
		clauses = append(clauses, "account_id IN (?)")
		args = append(args, f.AccountIDs)
	}
	if f.DestinationPrefix != "" {
		clauses = append(clauses, "startsWith(destination, ?)")
		args = append(args, f.DestinationPrefix)
	}
	if f.TrunkID != "" {
		clauses = append(clauses, "trunk_id = ?")
		args = append(args, f.TrunkID)
	}
	if f.EndReason != "" {
		clauses = append(clauses, "end_reason = ?")
		args = append(args, f.EndReason)
	}
	return strings.Join(clauses, " AND "), args
}

func (s *Store) ListCdrs(ctx context.Context, f Filter, limit int, cur *Cursor) ([]CdrRow, error) {
	where, args := f.where()
	if cur != nil {
		where += " AND (started_at, call_id) < (toDateTime64(?, 3, 'UTC'), ?)"
		args = append(args, cur.StartedAt.UTC(), cur.CallID)
	}
	q := `SELECT call_id, account_id, destination, cli,
			toUnixTimestamp64Milli(started_at), toUnixTimestamp64Milli(ended_at),
			charged_seconds, end_reason, trunk_id, toString(rate_per_sec),
			amount_minor, rate_missing
		FROM reports.cdrs FINAL
		WHERE ` + where + `
		ORDER BY started_at DESC, call_id DESC
		LIMIT ?`
	args = append(args, limit)

	rs, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	var out []CdrRow
	for rs.Next() {
		var r CdrRow
		var startMS, endMS int64
		if err := rs.Scan(&r.CallID, &r.AccountID, &r.Destination, &r.CLI,
			&startMS, &endMS, &r.ChargedSeconds, &r.EndReason, &r.TrunkID,
			&r.RatePerSec, &r.AmountMinor, &r.RateMissing); err != nil {
			return nil, err
		}
		r.StartedAt = time.UnixMilli(startMS).UTC()
		r.EndedAt = time.UnixMilli(endMS).UTC()
		out = append(out, r)
	}
	return out, rs.Err()
}

func keyExpr(g GroupBy) (string, error) {
	switch g {
	case GroupDay:
		return "toString(toDate(started_at))", nil
	case GroupDestination:
		return "destination", nil
	case GroupAccount:
		return "account_id", nil
	case GroupTrunk:
		return "trunk_id", nil
	}
	return "", fmt.Errorf("unknown group_by %q", g)
}

func (s *Store) Summarize(ctx context.Context, f Filter, g GroupBy) ([]SummaryRow, error) {
	key, err := keyExpr(g)
	if err != nil {
		return nil, err
	}
	where, args := f.where()
	q := `SELECT ` + key + ` AS k, count(), sum(charged_seconds), sum(amount_minor)
		FROM reports.cdrs FINAL
		WHERE ` + where + `
		GROUP BY k
		ORDER BY sum(amount_minor) DESC, k ASC`

	rs, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	var out []SummaryRow
	for rs.Next() {
		var row SummaryRow
		var calls uint64
		if err := rs.Scan(&row.Key, &calls, &row.Seconds, &row.AmountMinor); err != nil {
			return nil, err
		}
		row.Calls = int64(calls)
		out = append(out, row)
	}
	return out, rs.Err()
}
