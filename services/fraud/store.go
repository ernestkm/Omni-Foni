package main

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	fraudpb "omnifoni/fraud/gen/fraudpb"
)

//go:embed migrate.sql
var schemaSQL string

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

func (s *Store) Reserve(ctx context.Context, req *fraudpb.CheckCallRequest, cfg Config) (*fraudpb.CheckCallResponse, *fraudpb.Alert, error) {
	account := req.GetAccountId()
	destination := req.GetDestination()
	prefix := DestinationPrefix(destination)
	now := time.Now().UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtext('fraud:' || $1))`, account); err != nil {
		return nil, nil, err
	}

	enabled := true
	var maxDay, maxPrefix pgtype.Int4
	err = tx.QueryRow(ctx,
		`SELECT enabled, max_calls_per_day, max_calls_per_prefix_day
		 FROM fraud_controls WHERE account_id = $1`, account).
		Scan(&enabled, &maxDay, &maxPrefix)
	if errors.Is(err, pgx.ErrNoRows) {
		enabled = true
	} else if err != nil {
		return nil, nil, err
	}
	if !enabled {
		maxDay = pgtype.Int4{}
		maxPrefix = pgtype.Int4{}
	}

	blockedDestination, err := prefixBlocked(ctx, tx, "prefix_blocks", account, destination)
	if err != nil {
		return nil, nil, err
	}
	blockedCLI, err := prefixBlocked(ctx, tx, "cli_blocks", account, req.GetCli())
	if err != nil {
		return nil, nil, err
	}
	signals, err := readSignals(ctx, tx, account, destination, prefix, req.GetCli(), cfg, now, day)
	if err != nil {
		return nil, nil, err
	}
	score, reasons := Score(cfg, signals)
	resp := &fraudpb.CheckCallResponse{
		Allowed:     true,
		RiskScore:   int32(score),
		RiskLevel:   Level(score),
		Reasons:     reasons,
		RuleVersion: RuleVersion,
	}

	var alreadyReserved bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM reservations WHERE call_id = $1)`, req.GetCallId()).
		Scan(&alreadyReserved); err != nil {
		return nil, nil, err
	}
	if alreadyReserved {
		resp.Allowed = true
		resp.Reason = ""
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, err
		}
		return resp, nil, nil
	}

	reason := hardRule(blockedDestination, blockedCLI, maxDay, maxPrefix,
		signals.AccountCalls, signals.PrefixCalls)
	if reason != "" {
		resp.Allowed = false
		resp.Reason = reason
	}

	var alert *fraudpb.Alert
	if score >= cfg.AlertThreshold {
		var alreadyAlerted bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM alerts WHERE call_id = $1)`, req.GetCallId()).
			Scan(&alreadyAlerted); err != nil {
			return nil, nil, err
		}
		if !alreadyAlerted {
			alert, err = insertAlert(ctx, tx, req, score, resp.GetRiskLevel(), reasons)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if reason == "" {
		if _, err := tx.Exec(ctx,
			`INSERT INTO reservations(call_id, account_id, day, prefix)
			 VALUES ($1, $2, $3, $4)`, req.GetCallId(), account, day, prefix); err != nil {
			return nil, nil, err
		}
		for _, p := range usagePrefixes(prefix) {
			if _, err := tx.Exec(ctx,
				`INSERT INTO daily_usage(account_id, day, prefix, active)
				 VALUES ($1, $2, $3, 1)
				 ON CONFLICT (account_id, day, prefix)
				 DO UPDATE SET active = daily_usage.active + 1`, account, day, p); err != nil {
				return nil, nil, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	return resp, alert, nil
}

func (s *Store) EndCall(ctx context.Context, callID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var account, prefix string
	var day time.Time
	err = tx.QueryRow(ctx,
		`DELETE FROM reservations WHERE call_id = $1
		 RETURNING account_id, day, prefix`, callID).Scan(&account, &day, &prefix)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	if err := decrementActive(ctx, tx, account, day, prefix); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func decrementActive(ctx context.Context, tx pgx.Tx, account string, day time.Time, prefix string) error {
	for _, p := range usagePrefixes(prefix) {
		if _, err := tx.Exec(ctx,
			`UPDATE daily_usage SET active = GREATEST(active - 1, 0)
			 WHERE account_id = $1 AND day = $2::date AND prefix = $3`, account, day, p); err != nil {
			return err
		}
	}
	return nil
}

func usagePrefixes(prefix string) []string {
	if prefix == "" {
		return []string{""}
	}
	return []string{"", prefix}
}

func hardRule(blockedDestination, blockedCLI bool, maxDay, maxPrefix pgtype.Int4, accountCalls, prefixCalls int) string {
	switch {
	case blockedDestination:
		return "fraud_destination"
	case blockedCLI:
		return "fraud_cli"
	case maxDay.Valid && accountCalls >= int(maxDay.Int32):
		return "fraud_velocity_account"
	case maxPrefix.Valid && prefixCalls >= int(maxPrefix.Int32):
		return "fraud_velocity_prefix"
	default:
		return ""
	}
}

func prefixBlocked(ctx context.Context, tx pgx.Tx, table, account, value string) (bool, error) {
	if value == "" {
		return false, nil
	}
	query := `SELECT prefix FROM prefix_blocks
	          WHERE account_id = $1 AND $2 LIKE prefix || '%'
	          ORDER BY length(prefix) DESC LIMIT 1`
	if table == "cli_blocks" {
		query = `SELECT prefix FROM cli_blocks
		         WHERE account_id = $1 AND $2 LIKE prefix || '%'
		         ORDER BY length(prefix) DESC LIMIT 1`
	}
	var prefix string
	err := tx.QueryRow(ctx, query, account, value).Scan(&prefix)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func readSignals(ctx context.Context, tx pgx.Tx, account, destination, prefix, cli string, cfg Config, now, day time.Time) (Signals, error) {
	s := Signals{
		Destination:      destination,
		CLI:              cli,
		Start:            now,
		RiskPrefixWeight: riskPrefixWeight(ctx, tx, destination),
	}

	rows, err := tx.Query(ctx,
		`SELECT destination FROM processed_cdrs WHERE account_id = $1`, account)
	if err != nil {
		return Signals{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var seenDestination string
		if err := rows.Scan(&seenDestination); err != nil {
			return Signals{}, err
		}
		if DestinationPrefix(seenDestination) == prefix {
			s.FirstSeenPrefix = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		return Signals{}, err
	}
	s.FirstSeenPrefix = !s.FirstSeenPrefix

	err = tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_cdrs WHERE account_id = $1 AND cli = $2)`,
		account, cli).Scan(&s.FirstSeenCLI)
	if err != nil {
		return Signals{}, err
	}
	s.FirstSeenCLI = !s.FirstSeenCLI

	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(SUM(active + completed), 0)
		 FROM daily_usage WHERE account_id = $1 AND day = $2::date AND prefix = ''`,
		account, day).
		Scan(&s.AccountCalls); err != nil {
		return Signals{}, err
	}
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(SUM(active + completed), 0)
		 FROM daily_usage WHERE account_id = $1 AND day = $3::date AND prefix = $2`,
		account, prefix, day).
		Scan(&s.PrefixCalls); err != nil {
		return Signals{}, err
	}
	_ = cfg
	return s, nil
}

func riskPrefixWeight(ctx context.Context, tx pgx.Tx, destination string) int {
	var weight int
	err := tx.QueryRow(ctx,
		`SELECT weight FROM risk_prefixes
		 WHERE $1 LIKE prefix || '%'
		 ORDER BY length(prefix) DESC
		 LIMIT 1`, destination).Scan(&weight)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0
	}
	if err != nil {
		return 0
	}
	return weight
}

func insertAlert(ctx context.Context, tx pgx.Tx, req *fraudpb.CheckCallRequest, score int, level string, reasons []string) (*fraudpb.Alert, error) {
	raw, err := json.Marshal(reasons)
	if err != nil {
		return nil, err
	}
	var alertID string
	var createdAt time.Time
	err = tx.QueryRow(ctx,
		`INSERT INTO alerts(call_id, account_id, risk_score, risk_level, reasons, rule_version)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (call_id) DO NOTHING
		 RETURNING alert_id::text, created_at`,
		req.GetCallId(), req.GetAccountId(), score, level, raw, RuleVersion).
		Scan(&alertID, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &fraudpb.Alert{
		AlertId:     alertID,
		CallId:      req.GetCallId(),
		AccountId:   req.GetAccountId(),
		RiskScore:   int32(score),
		RiskLevel:   level,
		Reasons:     reasons,
		RuleVersion: RuleVersion,
		CreatedAtMs: createdAt.UnixMilli(),
		Status:      "new",
	}, nil
}

func (s *Store) ListAlerts(ctx context.Context, accountID string, limit int, cursor string) ([]*fraudpb.Alert, string, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	query := `SELECT alert_id::text, call_id, account_id, risk_score, risk_level,
	                 reasons, rule_version, created_at, status
	          FROM alerts
	          WHERE ($1 = '' OR account_id = $1)
	          ORDER BY created_at DESC, alert_id DESC
	          LIMIT $2`
	args := []any{accountID, limit}
	if cursor != "" {
		createdAt, alertID, err := decodeAlertCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		query = `SELECT alert_id::text, call_id, account_id, risk_score, risk_level,
		                reasons, rule_version, created_at, status
		         FROM alerts
		         WHERE ($1 = '' OR account_id = $1)
		           AND (created_at, alert_id) < ($2::timestamptz, $3::uuid)
		         ORDER BY created_at DESC, alert_id DESC
		         LIMIT $4`
		args = []any{accountID, createdAt, alertID, limit}
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	alerts := []*fraudpb.Alert{}
	for rows.Next() {
		var alertID, callID, account, level, version, status string
		var score int
		var rawReasons []byte
		var createdAt time.Time
		if err := rows.Scan(&alertID, &callID, &account, &score, &level,
			&rawReasons, &version, &createdAt, &status); err != nil {
			return nil, "", err
		}
		reasons := []string{}
		if err := json.Unmarshal(rawReasons, &reasons); err != nil {
			return nil, "", err
		}
		alerts = append(alerts, &fraudpb.Alert{
			AlertId:     alertID,
			CallId:      callID,
			AccountId:   account,
			RiskScore:   int32(score),
			RiskLevel:   level,
			Reasons:     reasons,
			RuleVersion: version,
			CreatedAtMs: createdAt.UnixMilli(),
			Status:      status,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(alerts) == limit {
		last := alerts[len(alerts)-1]
		next = encodeAlertCursor(last)
	}
	return alerts, next, nil
}

var errInvalidAlertCursor = errors.New("invalid alert cursor")

func decodeAlertCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %v", errInvalidAlertCursor, err)
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 || parts[1] == "" {
		return time.Time{}, "", errInvalidAlertCursor
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %v", errInvalidAlertCursor, err)
	}
	alertID, err := uuid.Parse(parts[1])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("%w: %v", errInvalidAlertCursor, err)
	}
	return time.UnixMilli(ms).UTC(), alertID.String(), nil
}

func encodeAlertCursor(alert *fraudpb.Alert) string {
	raw := fmt.Sprintf("%d|%s", alert.GetCreatedAtMs(), alert.GetAlertId())
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}
