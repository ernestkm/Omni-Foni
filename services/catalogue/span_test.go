package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	pb "omnifoni/catalogue/gen/cataloguepb"
)

func TestGetRateEmitsSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	s := NewServer(nil)
	_, _ = s.GetRate(context.Background(), &pb.GetRateRequest{})

	found := false
	for _, sp := range sr.Ended() {
		if sp.Name() == "catalogue.get_rate" {
			found = true
		}
	}
	if !found {
		t.Fatal("catalogue.get_rate span not recorded")
	}
}
