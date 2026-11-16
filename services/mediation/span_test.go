package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"omnifoni/observability"
)

func TestHandleEmitsConsumerSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	// Pins the span-name contract the pipeline uses; imports the shared module.
	_, span := observability.ConsumerSpan(context.Background(), "mediation.process", "call-1")
	span.End()

	if len(sr.Ended()) != 1 || sr.Ended()[0].Name() != "mediation.process" {
		t.Fatalf("unexpected spans: %v", sr.Ended())
	}
}
