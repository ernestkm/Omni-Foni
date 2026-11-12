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
	"google.golang.org/protobuf/proto"

	mediationpb "omnifoni/mediation/gen/mediationpb"
	pb "omnifoni/switching/gen/switchingpb"
)

func kafkaAddr() string {
	if s := os.Getenv("SWITCHING_KAFKA"); s != "" {
		return s
	}
	return "localhost:9092"
}

// ensureTopic pre-creates cdrs.raw so ProduceSync never races auto-create;
// existing topics are tolerated.
func ensureTopic(t *testing.T, cl *kgo.Client, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin := kadm.NewClient(cl)
	created, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range created.Sorted() {
		if c.Err != nil && !errors.Is(c.Err, kerr.TopicAlreadyExists) {
			t.Fatal(c.Err)
		}
	}
	for ctx.Err() == nil {
		details, err := admin.ListTopics(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		if len(details) == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("topic not ready:", ctx.Err())
}

// TestCDREmittedOnStop: stopping a call lands exactly one CDR on cdrs.raw
// with the call's fields.
func TestCDREmittedOnStop(t *testing.T) {
	seed(t, "acc-cdr", 1000, "3555", "1.000000")

	prod, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr()))
	if err != nil {
		t.Skip("kafka unavailable:", err)
	}
	t.Cleanup(prod.Close)
	ensureTopic(t, prod, "cdrs.raw")

	client := startSwitchingWithCDR(t, chargingAddr(), &kafkaCDRs{topic: "cdrs.raw", prod: prod})

	resp := startCall(t, client, "acc-cdr", "35551234567")
	if !resp.Allowed {
		t.Fatalf("denied: %+v", resp)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, err := pb.NewSwitchingClient(client).StopCall(context.Background(),
		&pb.StopCallRequest{CallId: resp.CallId}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkaAddr()),
		kgo.ConsumerGroup("switching-cdr-test-"+uuid.New().String()),
		kgo.ConsumeTopics("cdrs.raw"),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	for ctx.Err() == nil {
		f := cl.PollRecords(ctx, 100)
		for iter := f.RecordIter(); !iter.Done(); {
			r := iter.Next()
			if string(r.Key) != resp.CallId {
				continue // history from other test runs
			}
			var c mediationpb.CallCdr
			if err := proto.Unmarshal(r.Value, &c); err != nil {
				t.Fatal(err)
			}
			if c.AccountId != "acc-cdr" || c.Destination != "35551234567" ||
				c.ChargedSeconds < 1 || c.EndReason != "normal" {
				t.Fatalf("wrong CDR fields: %+v", &c)
			}
			cancel()
			return
		}
	}
	t.Fatal("CDR for call never appeared on cdrs.raw")
}
