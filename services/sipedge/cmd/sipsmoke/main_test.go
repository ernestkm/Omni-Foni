package main

import (
	"os"
	"testing"
)

func TestCallIDUsesEnvironmentOverride(t *testing.T) {
	t.Setenv("SIP_SMOKE_CALL_ID", "fixed-call-id")
	if got := callID(); got != "fixed-call-id" {
		t.Fatalf("want fixed-call-id, got %q", got)
	}
}

func TestFromURIIncludesHost(t *testing.T) {
	if got := fromURI("27820000000"); got != "sip:27820000000@edge" {
		t.Fatalf("want sip:27820000000@edge, got %q", got)
	}
}

func TestReasonHeaderParsesToken(t *testing.T) {
	msg := "SIP/2.0 403 denied: no_route\r\nReason: SIP;cause=403;text=\"no_route\"\r\nContent-Length: 0\r\n\r\n"
	if got := reasonHeader(msg); got != "no_route" {
		t.Fatalf("want no_route, got %q", got)
	}
}

func TestReasonHeaderMissing(t *testing.T) {
	if got := reasonHeader("SIP/2.0 503 unavailable\r\n\r\n"); got != "" {
		t.Fatalf("want empty reason, got %q", got)
	}
}

func TestCallIDDefaultsWhenUnset(t *testing.T) {
	if err := os.Unsetenv("SIP_SMOKE_CALL_ID"); err != nil {
		t.Fatal(err)
	}
	if got := callID(); got == "" {
		t.Fatal("empty generated call id")
	}
}
