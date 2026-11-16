package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func decodeLine(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode %q: %v", buf.String(), err)
	}
	return got
}

func TestHandlerMasksSensitiveKeys(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil), "switching"))
	log.Info("call", "destination", "+27821234567", "password", "hunter2", "account_id", "acc-1")

	got := decodeLine(t, &buf)
	if got["destination"] != "+2782***4567" {
		t.Errorf("destination = %v, want +2782***4567", got["destination"])
	}
	if got["password"] != "***" {
		t.Errorf("password = %v, want ***", got["password"])
	}
	if got["account_id"] != "acc-1" {
		t.Errorf("account_id = %v, want acc-1", got["account_id"])
	}
	if got["service"] != "switching" {
		t.Errorf("service = %v, want switching", got["service"])
	}
}

func TestHandlerAddsCorrelation(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil), "billing"))
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	ctx = WithRequestID(ctx, "req-1")
	log.InfoContext(ctx, "batch")

	got := decodeLine(t, &buf)
	if got["trace_id"] != sc.TraceID().String() {
		t.Errorf("trace_id = %v, want %s", got["trace_id"], sc.TraceID())
	}
	if got["span_id"] != sc.SpanID().String() {
		t.Errorf("span_id = %v, want %s", got["span_id"], sc.SpanID())
	}
	if got["request_id"] != "req-1" {
		t.Errorf("request_id = %v, want req-1", got["request_id"])
	}
}

func TestMask(t *testing.T) {
	if got := Mask("short"); got != "***" {
		t.Errorf("Mask(short) = %q, want ***", got)
	}
	if got := Mask("+27821234567"); got != "+2782***4567" {
		t.Errorf("Mask(phone) = %q, want +2782***4567", got)
	}
}
