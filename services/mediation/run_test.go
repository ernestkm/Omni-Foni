package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	kadm "github.com/twmb/franz-go/pkg/kadm"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	pb "omnifoni/mediation/gen/mediationpb"
)

// TestRunLoop: records fed to the raw topic reach the normalized topic
// through the full consume-produce-commit loop. The consumer client that Run
// uses must be subscribed to the per-test topic.
func TestRunLoop(t *testing.T) {
	pool := newPool(t)
	prod := newKafka(t)
	raw := "cdrs.raw-loop-" + uuid.New().String()
	norm := raw + "-normalized"
	rejected := raw + "-rejected"
	createTestTopics(t, prod, norm, rejected)
	p := NewProcessor(pool, prod, norm, rejected)

	admin := kadm.NewClient(prod)
	created, err := admin.CreateTopics(context.Background(), 1, 1, nil, raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range created.Sorted() {
		if c.Err != nil {
			t.Fatal(c.Err)
		}
	}
	t.Cleanup(func() {
		if _, err := admin.DeleteTopics(context.Background(), raw); err != nil {
			t.Logf("delete raw test topic: %v", err)
		}
	})

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaBrokers()...),
		kgo.ConsumerGroup("mediation-loop-"+uuid.New().String()),
		kgo.ConsumeTopics(raw),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Run(ctx, consumer, p)

	c := &pb.CallCdr{CallId: "c-loop-" + uuid.New().String(), AccountId: "a-1", EndReason: "normal"}
	rec := cdrRecord(t, raw, c)
	if err := prod.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
		t.Fatal(err)
	}

	want, err := proto.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range readRecords(t, norm, 1) {
			if string(r.Key) == c.CallId && bytes.Equal(r.Value, want) {
				cancel()
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("normalized record never appeared")
}
