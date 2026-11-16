package observability

import (
	"context"
	"testing"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestKafkaTraceparentRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6},
		SpanID:     trace.SpanID{1, 1, 2, 2, 3, 3, 4, 4},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	rec := &kgo.Record{}
	InjectKafka(ctx, rec)
	if len(rec.Headers) == 0 {
		t.Fatal("no headers injected")
	}
	got := trace.SpanContextFromContext(ExtractKafka(context.Background(), rec))
	if got.TraceID() != sc.TraceID() {
		t.Fatalf("trace id = %s, want %s", got.TraceID(), sc.TraceID())
	}
}
