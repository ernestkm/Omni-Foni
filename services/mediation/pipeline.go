package main

import (
	"context"
	"log/slog"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	pb "omnifoni/mediation/gen/mediationpb"
)

// Processor turns raw CDR records into normalized ones: decode, validate,
// dedup via the ledger, produce. Rejected records go to the dead-letter topic
// with a reason header; nothing is ever dropped silently.
type Processor struct {
	pool       *pgxpool.Pool
	prod       *kgo.Client
	normalized string
	rejected   string
}

func NewProcessor(pool *pgxpool.Pool, cl *kgo.Client, normalized, rejected string) *Processor {
	return &Processor{pool: pool, prod: cl, normalized: normalized, rejected: rejected}
}

// Handle processes one raw record. Errors mean "don't commit past me"; the
// caller retries. At-least-once: a crash between produce and the published
// flag can re-produce once; downstream dedup by call_id handles it.
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) error {
	c := &pb.CallCdr{}
	if err := proto.Unmarshal(rec.Value, c); err != nil {
		return p.reject(ctx, rec, "decode_error", err)
	}
	if reason := Validate(c); reason != "" {
		return p.reject(ctx, rec, reason, nil)
	}
	return p.publish(ctx, rec, c)
}

func (p *Processor) reject(ctx context.Context, rec *kgo.Record, reason string, cause error) error {
	if cause != nil {
		slog.Error("cdr rejected", "reason", reason, "err", cause)
	} else {
		slog.Error("cdr rejected", "reason", reason)
	}
	return p.produce(ctx, p.rejected, rec.Key, rec.Value, reason)
}

func (p *Processor) publish(ctx context.Context, rec *kgo.Record, c *pb.CallCdr) error {
	tag, err := p.pool.Exec(ctx,
		`INSERT INTO cdrs (call_id) VALUES ($1) ON CONFLICT (call_id) DO NOTHING`, c.GetCallId())
	if err != nil {
		return err
	}
	published := false
	if tag.RowsAffected() == 0 {
		if err := p.pool.QueryRow(ctx,
			`SELECT published FROM cdrs WHERE call_id = $1`, c.GetCallId()).Scan(&published); err != nil {
			return err
		}
	}
	if published {
		return nil
	}
	if err := p.produce(ctx, p.normalized, rec.Key, rec.Value, ""); err != nil {
		return err
	}
	_, err = p.pool.Exec(ctx, `UPDATE cdrs SET published = true WHERE call_id = $1`, c.GetCallId())
	return err
}

func (p *Processor) produce(ctx context.Context, topic string, key, value []byte, reason string) error {
	r := &kgo.Record{Topic: topic, Key: key, Value: value}
	if reason != "" {
		r.Headers = []kgo.RecordHeader{{Key: "reason", Value: []byte(reason)}}
	}
	return p.prod.ProduceSync(ctx, r).FirstErr()
}
