package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	kadm "github.com/twmb/franz-go/pkg/kadm"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fraudpb "omnifoni/fraud/gen/fraudpb"
	mediationpb "omnifoni/mediation/gen/mediationpb"
)

func kafkaBrokers() []string {
	if brokers := os.Getenv("FRAUD_KAFKA"); brokers != "" {
		return []string{brokers}
	}
	return []string{"127.0.0.1:9092"}
}

func newFraudKafka(t *testing.T, topics ...string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	admin := kadm.NewClient(cl)
	created, err := admin.CreateTopics(context.Background(), 1, 1, nil, topics...)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range created.Sorted() {
		if c.Err != nil {
			t.Fatal(c.Err)
		}
	}
	return cl
}

func cdrRecord(t *testing.T, topic string, c *mediationpb.CallCdr) *kgo.Record {
	t.Helper()
	raw, err := proto.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(c.GetCallId()), Value: raw}
}

func TestIngestCDRDedupAndActiveConversion(t *testing.T) {
	s := newTestStore(t)
	cfg := Config{AlertThreshold: 101, SoftAccountCalls: 5, SoftPrefixCalls: 2}
	if _, _, err := s.Reserve(context.Background(), &fraudpb.CheckCallRequest{
		CallId: "c-cdr", AccountId: "acc-cdr", Destination: "2782123456", Cli: "cli-a",
	}, cfg); err != nil {
		t.Fatal(err)
	}
	ing := NewIngester(s)
	c := &mediationpb.CallCdr{
		CallId: "c-cdr", AccountId: "acc-cdr", Destination: "2782123456",
		Cli: "cli-a", ChargedSeconds: 7,
	}
	raw, err := proto.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := ing.Handle(ctx, &kgo.Record{Topic: "cdrs.normalized", Value: raw}); err != nil {
			t.Fatal(err)
		}
	}
	var completed, active, seconds, amount int
	err = s.pool.QueryRow(ctx,
		`SELECT completed, active, seconds, amount_minor FROM daily_usage
		 WHERE account_id = 'acc-cdr' AND day = current_date AND prefix = ''`).
		Scan(&completed, &active, &seconds, &amount)
	if err != nil {
		t.Fatal(err)
	}
	if completed != 1 || active != 0 || seconds != 7 || amount != 0 {
		t.Fatalf("usage = %d %d %d %d", completed, active, seconds, amount)
	}
	var processed int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM processed_cdrs WHERE call_id = 'c-cdr'`).Scan(&processed); err != nil {
		t.Fatal(err)
	}
	if processed != 1 {
		t.Fatalf("processed rows = %d", processed)
	}
}

func TestIngestCDRPoisonRecordIgnored(t *testing.T) {
	s := newTestStore(t)
	ing := NewIngester(s)
	if err := ing.Handle(context.Background(), &kgo.Record{Value: []byte("not a cdr")}); err != nil {
		t.Fatalf("poison record must be dropped: %v", err)
	}
}

type storeHandler struct {
	store *Store
	mu    sync.Mutex
	order []string
	fail  map[string]int
}

func (h *storeHandler) Handle(ctx context.Context, rec *kgo.Record) error {
	c := &mediationpb.CallCdr{}
	if err := proto.Unmarshal(rec.Value, c); err != nil {
		return err
	}
	h.mu.Lock()
	h.order = append(h.order, c.GetCallId())
	shouldFail := h.fail[c.GetCallId()] == 0
	if shouldFail {
		h.fail[c.GetCallId()]++
	}
	h.mu.Unlock()
	if shouldFail {
		return errors.New("transient store failure")
	}
	return h.store.IngestCDR(ctx, c)
}

func TestRunCommitsOffsetsAfterDatabaseWrites(t *testing.T) {
	s := newTestStore(t)
	topic := "fraud-cdrs-" + uuid.New().String()
	prod := newFraudKafka(t, topic)
	group := "fraud-consumer-" + uuid.New().String()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Close)

	records := []*kgo.Record{
		cdrRecord(t, topic, &mediationpb.CallCdr{
			CallId: "stream-1", AccountId: "acc-stream", Destination: "2782001", Cli: "cli-a",
			ChargedSeconds: 1,
		}),
		cdrRecord(t, topic, &mediationpb.CallCdr{
			CallId: "stream-2", AccountId: "acc-stream", Destination: "2782002", Cli: "cli-b",
			ChargedSeconds: 2,
		}),
	}
	if err := prod.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	handler := &storeHandler{store: s, fail: map[string]int{"stream-1": 0, "stream-2": 0}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, consumer, handler)

	admin := kadm.NewClient(prod)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var processed int
		if err := s.pool.QueryRow(ctx,
			`SELECT count(*) FROM processed_cdrs WHERE account_id = 'acc-stream'`).Scan(&processed); err == nil && processed == 2 {
			offsets, err := admin.FetchOffsets(ctx, group)
			if err == nil {
				if o, ok := offsets.Lookup(topic, 0); ok && o.At >= 2 {
					cancel()
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("two CDRs were not processed and committed")
}
