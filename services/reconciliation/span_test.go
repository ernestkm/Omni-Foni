package main

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSweepEmitsSpanPerPeriod(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	s := NewServer(&fakeBilling{}, &fakePayments{}, noopPublisher{})
	if err := s.Sweep(context.Background(), mustTime(t, "2026-11-15T10:00:00Z")); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	got := 0
	for _, sp := range sr.Ended() {
		if sp.Name() == "reconciliation.sweep" {
			got++
		}
	}
	if got != 2 {
		t.Fatalf("sweep spans = %d, want 2 (current + previous month)", got)
	}
}
