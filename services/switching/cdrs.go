package main

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	kgo "github.com/twmb/franz-go/pkg/kgo"

	mediationpb "omnifoni/mediation/gen/mediationpb"
	"omnifoni/observability"
)

// buildCDR derives the final call detail record from a finished call.
func buildCDR(c *call, endReason string, chargedSeconds int64, endedAt time.Time) *mediationpb.CallCdr {
	return &mediationpb.CallCdr{
		CallId:         c.id,
		AccountId:      c.accountID,
		Destination:    c.destination,
		Cli:            c.cli,
		StartedAtMs:    c.start.UnixMilli(),
		EndedAtMs:      endedAt.UnixMilli(),
		ChargedSeconds: chargedSeconds,
		EndReason:      endReason,
		TrunkId:        c.trunkID,
	}
}

// kafkaCDRs publishes finished-call CDRs to the raw topic.
type kafkaCDRs struct {
	topic string
	prod  *kgo.Client
}

func (k *kafkaCDRs) PublishCDR(ctx context.Context, c *mediationpb.CallCdr) error {
	v, err := proto.Marshal(c)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: k.topic, Key: []byte(c.GetCallId()), Value: v}
	observability.InjectKafka(ctx, rec)
	return k.prod.ProduceSync(ctx, rec).FirstErr()
}
