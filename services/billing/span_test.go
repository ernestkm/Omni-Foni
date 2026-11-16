package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"omnifoni/observability"
)

func TestBillingSpanName(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	ctx, span := observability.ConsumerSpan(context.Background(), "billing.handle", "call-1")
	_ = ctx
	span.End()

	if len(sr.Ended()) != 1 || sr.Ended()[0].Name() != "billing.handle" {
		t.Fatalf("unexpected spans: %v", sr.Ended())
	}
}
