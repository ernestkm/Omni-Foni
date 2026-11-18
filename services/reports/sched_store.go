package main

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sched_migrate.sql
var schedSchemaSQL string

// Schedule is one recurring report definition. Weekday (weekly) and
// DayOfMonth (monthly) are nil for the other frequencies.
type Schedule struct {
	ScheduleID string
	TenantID   string
	AccountIDs []string
	ReportKind string
	WindowKind string
	GroupBy    string
	PrefixLen  int
	Frequency  string
	AtHour     int
	AtMinute   int
	Weekday    *int
	DayOfMonth *int
	Enabled    bool
	CreatedBy  string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Artifact is one rendered CSV body plus its slot identity.
type Artifact struct {
	ArtifactID string
	ScheduleID string
	SlotKey    string
	PeriodFrom time.Time
	PeriodTo   time.Time
	RowCount   int64
	CSVBody    string
	CreatedAt  time.Time
}

// SchedStore persists schedules and artifacts in Postgres.
type SchedStore struct {
	pool *pgxpool.Pool
}

func OpenSched(ctx context.Context, dsn string) (*SchedStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &SchedStore{pool: pool}, nil
}

func (s *SchedStore) Close() { s.pool.Close() }

// Migrate applies the schema. Idempotent, safe at boot.
func (s *SchedStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schedSchemaSQL)
	return err
}

const scheduleColumns = `schedule_id, tenant_id, account_ids, report_kind, window_kind,
	group_by, prefix_len, frequency, at_hour, at_minute, weekday, day_of_month,
	enabled, created_by, created_at, updated_at`

func scanSchedule(row pgx.Row) (Schedule, error) {
	var s Schedule
	err := row.Scan(&s.ScheduleID, &s.TenantID, &s.AccountIDs, &s.ReportKind, &s.WindowKind,
		&s.GroupBy, &s.PrefixLen, &s.Frequency, &s.AtHour, &s.AtMinute, &s.Weekday, &s.DayOfMonth,
		&s.Enabled, &s.CreatedBy, &s.CreatedAt, &s.UpdatedAt)
	return s, err
}

func (s *SchedStore) CreateSchedule(ctx context.Context, in Schedule) (Schedule, error) {
	return scanSchedule(s.pool.QueryRow(ctx, `INSERT INTO schedules
		(tenant_id, account_ids, report_kind, window_kind, group_by, prefix_len,
		 frequency, at_hour, at_minute, weekday, day_of_month, enabled, created_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		RETURNING `+scheduleColumns,
		in.TenantID, in.AccountIDs, in.ReportKind, in.WindowKind, in.GroupBy, in.PrefixLen,
		in.Frequency, in.AtHour, in.AtMinute, in.Weekday, in.DayOfMonth, in.Enabled, in.CreatedBy))
}

func (s *SchedStore) GetSchedule(ctx context.Context, id string) (Schedule, error) {
	return scanSchedule(s.pool.QueryRow(ctx,
		`SELECT `+scheduleColumns+` FROM schedules WHERE schedule_id = $1`, id))
}

func (s *SchedStore) ListSchedules(ctx context.Context, tenantID string) ([]Schedule, error) {
	return s.querySchedules(ctx, `SELECT `+scheduleColumns+` FROM schedules
		WHERE ($1 = '' OR tenant_id = $1) ORDER BY created_at DESC`, tenantID)
}

func (s *SchedStore) ListEnabledSchedules(ctx context.Context) ([]Schedule, error) {
	return s.querySchedules(ctx, `SELECT `+scheduleColumns+` FROM schedules
		WHERE enabled ORDER BY created_at ASC`)
}

func (s *SchedStore) querySchedules(ctx context.Context, query string, args ...any) ([]Schedule, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (s *SchedStore) UpdateSchedule(ctx context.Context, in Schedule) error {
	_, err := s.pool.Exec(ctx, `UPDATE schedules SET
		account_ids = $2, report_kind = $3, window_kind = $4, group_by = $5, prefix_len = $6,
		frequency = $7, at_hour = $8, at_minute = $9, weekday = $10, day_of_month = $11,
		enabled = $12, updated_at = now()
		WHERE schedule_id = $1`,
		in.ScheduleID, in.AccountIDs, in.ReportKind, in.WindowKind, in.GroupBy, in.PrefixLen,
		in.Frequency, in.AtHour, in.AtMinute, in.Weekday, in.DayOfMonth, in.Enabled)
	return err
}

func (s *SchedStore) DeleteSchedule(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM schedules WHERE schedule_id = $1`, id)
	return err
}

// InsertArtifact stores one artifact; inserted is false when the slot already
// ran (UNIQUE (schedule_id, slot_key)).
func (s *SchedStore) InsertArtifact(ctx context.Context, a Artifact) (Artifact, bool, error) {
	var out Artifact
	err := s.pool.QueryRow(ctx, `INSERT INTO report_artifacts
		(schedule_id, slot_key, period_from, period_to, row_count, csv_body)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (schedule_id, slot_key) DO NOTHING
		RETURNING artifact_id, schedule_id, slot_key, period_from, period_to, row_count, created_at`,
		a.ScheduleID, a.SlotKey, a.PeriodFrom, a.PeriodTo, a.RowCount, a.CSVBody).
		Scan(&out.ArtifactID, &out.ScheduleID, &out.SlotKey, &out.PeriodFrom, &out.PeriodTo,
			&out.RowCount, &out.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Artifact{}, false, nil
	}
	if err != nil {
		return Artifact{}, false, err
	}
	return out, true, nil
}

// ListArtifacts returns artifact metadata without CSV bodies; callers that
// need the body use GetArtifact.
func (s *SchedStore) ListArtifacts(ctx context.Context, scheduleID string) ([]Artifact, error) {
	rows, err := s.pool.Query(ctx, `SELECT artifact_id, schedule_id, slot_key, period_from,
		period_to, row_count, created_at FROM report_artifacts
		WHERE schedule_id = $1 ORDER BY created_at DESC`, scheduleID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Artifact
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.ArtifactID, &a.ScheduleID, &a.SlotKey, &a.PeriodFrom,
			&a.PeriodTo, &a.RowCount, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *SchedStore) GetArtifact(ctx context.Context, scheduleID, artifactID string) (Artifact, string, error) {
	var a Artifact
	var body string
	err := s.pool.QueryRow(ctx, `SELECT artifact_id, schedule_id, slot_key, period_from,
		period_to, row_count, csv_body, created_at FROM report_artifacts
		WHERE schedule_id = $1 AND artifact_id = $2`, scheduleID, artifactID).
		Scan(&a.ArtifactID, &a.ScheduleID, &a.SlotKey, &a.PeriodFrom, &a.PeriodTo,
			&a.RowCount, &body, &a.CreatedAt)
	return a, body, err
}

func (s *SchedStore) PruneArtifacts(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM report_artifacts WHERE created_at < $1`, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
