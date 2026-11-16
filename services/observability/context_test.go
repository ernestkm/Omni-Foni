package observability

import (
	"context"
	"testing"
)

func TestRequestIDRoundTrip(t *testing.T) {
	ctx := WithRequestID(context.Background(), "abc")
	if got := RequestIDFrom(ctx); got != "abc" {
		t.Fatalf("RequestIDFrom = %q, want abc", got)
	}
	if got := RequestIDFrom(context.Background()); got != "" {
		t.Fatalf("RequestIDFrom(empty) = %q, want empty", got)
	}
}

func TestNewRequestIDIsUnique(t *testing.T) {
	a, b := NewRequestID(), NewRequestID()
	if a == "" || a == b {
		t.Fatalf("ids not unique: %q %q", a, b)
	}
}
