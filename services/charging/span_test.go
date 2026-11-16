package main

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	pb "omnifoni/charging/gen/chargingpb"
)

func TestAuthorizeEmitsSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	s := NewServer(nil, nil)
	_, _ = s.Authorize(context.Background(), &pb.AuthorizeRequest{})

	found := false
	for _, sp := range sr.Ended() {
		if sp.Name() == "charging.authorize" {
			found = true
		}
	}
	if !found {
		t.Fatal("charging.authorize span not recorded")
	}
}
