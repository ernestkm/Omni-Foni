package main

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	mediationpb "omnifoni/mediation/gen/mediationpb"
	pb "omnifoni/switching/gen/switchingpb"
)

// spanCDRs records the trace context the registry hands to PublishCDR.
type spanCDRs struct{ got chan trace.SpanContext }

func (s *spanCDRs) PublishCDR(ctx context.Context, _ *mediationpb.CallCdr) error {
	select {
	case s.got <- trace.SpanContextFromContext(ctx):
	default:
	}
	return nil
}

// TestPublishCDRCarriesTraceContext drives a real call through the bufconn
// harness and asserts that the CDR publish happens under the start_call
// span's trace (needs the dev stack: charging :50051, catalogue :50053).
func TestPublishCDRCarriesTraceContext(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	pub := &spanCDRs{got: make(chan trace.SpanContext, 4)}
	client := startSwitchingWithCDR(t, chargingAddr(), pub)
	seed(t, "trace-acc", 1000, "+2782", "1.0")

	sw := pb.NewSwitchingClient(client)
	res, err := sw.StartCall(context.Background(), &pb.StartCallRequest{
		AccountId: "trace-acc", Destination: "+27821234567"})
	if err != nil || !res.GetAllowed() {
		t.Fatalf("start: %v %v", err, res)
	}
	if _, err := sw.StopCall(context.Background(), &pb.StopCallRequest{CallId: res.GetCallId()}); err != nil {
		t.Fatalf("stop: %v", err)
	}

	select {
	case pubSC := <-pub.got:
		if !pubSC.IsValid() {
			t.Fatal("published CDR context has no trace span")
		}
		found := false
		for _, sp := range sr.Ended() {
			if sp.Name() == "switching.start_call" {
				found = true
				if sp.SpanContext().TraceID() != pubSC.TraceID() {
					t.Fatalf("publish trace %s != start_call trace %s",
						pubSC.TraceID(), sp.SpanContext().TraceID())
				}
			}
		}
		if !found {
			t.Fatal("switching.start_call span not recorded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CDR publish never observed")
	}
}
