package main

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrate.sql
var schemaSQL string

type Notification struct {
	NotificationID string
	DedupeKey      string
	Channel        string
	Recipient      string
	Subject        string
	Body           string
	Status         string
	Attempts       int
	NextAttemptAt  time.Time
	LastError      string
	CreatedAt      time.Time
	SentAt         *time.Time
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

const notificationColumns = `notification_id, dedupe_key, channel, recipient, subject,
	body, status, attempts, next_attempt_at, last_error, created_at, sent_at`

const notificationColumnsQualified = `o.notification_id, o.dedupe_key, o.channel, o.recipient, o.subject,
	o.body, o.status, o.attempts, o.next_attempt_at, o.last_error, o.created_at, o.sent_at`

func scanNotification(row pgx.Row) (Notification, error) {
	var n Notification
	err := row.Scan(&n.NotificationID, &n.DedupeKey, &n.Channel, &n.Recipient, &n.Subject,
		&n.Body, &n.Status, &n.Attempts, &n.NextAttemptAt, &n.LastError, &n.CreatedAt, &n.SentAt)
	return n, err
}

func (s *Store) Enqueue(ctx context.Context, n Notification) (Notification, bool, error) {
	if n.Status == "" {
		n.Status = "pending"
	}
	if n.NextAttemptAt.IsZero() {
		n.NextAttemptAt = time.Now().UTC()
	}
	out, err := scanNotification(s.pool.QueryRow(ctx, `INSERT INTO outbox
		(dedupe_key, channel, recipient, subject, body, status, attempts, next_attempt_at, last_error)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (dedupe_key) DO NOTHING
		RETURNING `+notificationColumns,
		n.DedupeKey, n.Channel, n.Recipient, n.Subject, n.Body, n.Status, n.Attempts,
		n.NextAttemptAt, n.LastError))
	if errors.Is(err, pgx.ErrNoRows) {
		return Notification{}, false, nil
	}
	if err != nil {
		return Notification{}, false, err
	}
	return out, true, nil
}

func (s *Store) ClaimDue(ctx context.Context, limit int) ([]Notification, error) {
	rows, err := s.pool.Query(ctx, `WITH due AS (
		SELECT notification_id FROM outbox
		WHERE status = 'pending' AND next_attempt_at <= now()
		ORDER BY next_attempt_at, created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE outbox o SET attempts = o.attempts + 1, next_attempt_at = now()
	FROM due WHERE o.notification_id = due.notification_id
	RETURNING `+notificationColumnsQualified, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *Store) MarkSent(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox SET status = 'sent', sent_at = now(), last_error = '' WHERE notification_id = $1`, id)
	return err
}

func (s *Store) MarkRetry(ctx context.Context, id, lastError string, next time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox SET status = 'pending', last_error = $2, next_attempt_at = $3 WHERE notification_id = $1`,
		id, lastError, next)
	return err
}

func (s *Store) MarkTerminal(ctx context.Context, id, lastError string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox SET status = 'failed', last_error = $2, next_attempt_at = now() WHERE notification_id = $1`,
		id, lastError)
	return err
}
