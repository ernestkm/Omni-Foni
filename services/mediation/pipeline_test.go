package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	pb "omnifoni/mediation/gen/mediationpb"
)

func kafkaBrokers() []string {
	if s := os.Getenv("MEDIATION_KAFKA"); s != "" {
		return []string{s}
	}
	return []string{"localhost:9092"}
}

func dsn() string {
	if s := os.Getenv("MEDIATION_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/mediation"
}

func newKafka(t *testing.T) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers()...))
	if err != nil {
		t.Skip("kafka unavailable:", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn())
	if err != nil {
		t.Skip("postgres unavailable:", err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `TRUNCATE cdrs`); err != nil {
		t.Fatal(err)
	}
	return pool
}

// resetTopics deletes and recreates the test output topics so repeated runs
// start from empty logs.
func resetTopics(t *testing.T, cl *kgo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := kadm.NewClient(cl)
	deleted, err := admin.DeleteTopics(ctx, "cdrs.normalized", "cdrs.rejected")
	if err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Fatal(err)
	}
	for _, d := range deleted.Sorted() {
		if d.Err != nil && !errors.Is(d.Err, kerr.UnknownTopicOrPartition) {
			t.Fatal(d.Err)
		}
	}
	created, err := admin.CreateTopics(ctx, 1, 1, nil, "cdrs.normalized", "cdrs.rejected")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range created.Sorted() {
		if c.Err != nil && !errors.Is(c.Err, kerr.TopicAlreadyExists) {
			t.Fatal(c.Err)
		}
	}
	for ctx.Err() == nil {
		details, err := admin.ListTopics(ctx, "cdrs.normalized", "cdrs.rejected")
		if err != nil {
			t.Fatal(err)
		}
		if len(details) == 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("test topics not ready:", ctx.Err())
}

// cdrRecord builds a raw record like Switching would publish.
func cdrRecord(t *testing.T, topic string, c *pb.CallCdr) *kgo.Record {
	t.Helper()
	v, err := proto.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(c.GetCallId()), Value: v}
}

// readRecords consumes a topic from the start with a unique group until it
// has want records or the timeout hits.
func readRecords(t *testing.T, topic string, want int) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers()...),
		kgo.ConsumerGroup("mediation-test-"+uuid.New().String()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	var recs []*kgo.Record
	for len(recs) < want && ctx.Err() == nil {
		f := cl.PollRecords(ctx, 100)
		for iter := f.RecordIter(); !iter.Done(); {
			recs = append(recs, iter.Next())
		}
	}
	return recs
}

// TestHandlePipeline: valid, duplicate, malformed, and invalid records produce
// exactly one normalized record and two rejected records.
func TestHandlePipeline(t *testing.T) {
	pool := newPool(t)
	cl := newKafka(t)
	resetTopics(t, cl)
	p := NewProcessor(pool, cl, "cdrs.normalized", "cdrs.rejected")
	ctx := context.Background()

	valid := &pb.CallCdr{CallId: "c-1", AccountId: "a-1", Destination: "35551234567", EndReason: "normal", ChargedSeconds: 5}
	valid2 := &pb.CallCdr{CallId: "c-2", AccountId: "a-1", Destination: "35551234567", EndReason: "balance_exhausted", ChargedSeconds: 3}
	recs := []*kgo.Record{
		cdrRecord(t, "cdrs.raw", valid),
		cdrRecord(t, "cdrs.raw", valid), // duplicate of the same call
		cdrRecord(t, "cdrs.raw", valid2),
		&kgo.Record{Topic: "cdrs.raw", Key: []byte("bad"), Value: []byte("not-proto")}, // malformed bytes: elements of []*kgo.Record need &
		cdrRecord(t, "cdrs.raw", &pb.CallCdr{AccountId: "a-2", EndReason: "normal"}),   // no call_id
	}
	for _, rec := range recs {
		if err := p.Handle(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}

	norm := readRecords(t, "cdrs.normalized", 2)
	if len(norm) != 2 {
		t.Fatalf("normalized count %d, want 2", len(norm))
	}
	for _, r := range norm {
		if len(r.Headers) != 0 {
			t.Fatalf("normalized record carries headers: %+v", r.Headers)
		}
		var c pb.CallCdr
		if err := proto.Unmarshal(r.Value, &c); err != nil {
			t.Fatal(err)
		}
		if c.CallId != "c-1" && c.CallId != "c-2" {
			t.Fatalf("unexpected call in normalized: %q", c.CallId)
		}
	}
	if c1 := norm[0]; string(c1.Key) != "c-1" || string(norm[1].Key) != "c-2" {
		t.Fatalf("normalized order/keys wrong: %q, %q", norm[0].Key, norm[1].Key)
	}

	rej := readRecords(t, "cdrs.rejected", 2)
	if len(rej) != 2 {
		t.Fatalf("rejected count %d, want 2", len(rej))
	}
	reasons := map[string]bool{}
	for _, r := range rej {
		for _, h := range r.Headers {
			if h.Key == "reason" {
				reasons[string(h.Value)] = true
			}
		}
	}
	if !reasons["decode_error"] || !reasons["missing_call_id"] {
		t.Fatalf("rejected reasons %v, want decode_error and missing_call_id", reasons)
	}

	// Ledger state: both valid calls marked published.
	for _, id := range []string{"c-1", "c-2"} {
		var published bool
		if err := pool.QueryRow(ctx, `SELECT published FROM cdrs WHERE call_id = $1`, id).Scan(&published); err != nil {
			t.Fatal(err)
		}
		if !published {
			t.Fatalf("call %s not marked published", id)
		}
	}
}

// TestHandleReplaySkips: re-handling an already-published record emits nothing.
func TestHandleReplaySkips(t *testing.T) {
	pool := newPool(t)
	cl := newKafka(t)
	resetTopics(t, cl)
	p := NewProcessor(pool, cl, "cdrs.normalized", "cdrs.rejected")
	ctx := context.Background()
	c := &pb.CallCdr{CallId: "c-replay", AccountId: "a-1", EndReason: "normal"}
	rec := cdrRecord(t, "cdrs.raw", c)
	if err := p.Handle(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(ctx, rec); err != nil {
		t.Fatal(err)
	}
	norm := readRecords(t, "cdrs.normalized", 1)
	count := 0
	for _, r := range norm {
		if string(r.Key) == "c-replay" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("c-replay appears %d times, want 1", count)
	}
}
