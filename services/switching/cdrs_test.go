package main

import (
	"testing"
	"time"
)

func TestBuildCDR(t *testing.T) {
	start := time.UnixMilli(1000)
	c := &call{id: "c1", accountID: "a1", destination: "35551234567", cli: "35551234568", trunkID: "trunk-a", start: start}
	got := buildCDR(c, "balance_exhausted", 42, time.UnixMilli(43000))
	if got.GetCallId() != "c1" || got.GetAccountId() != "a1" ||
		got.GetDestination() != "35551234567" || got.GetCli() != "35551234568" ||
		got.GetStartedAtMs() != 1000 || got.GetEndedAtMs() != 43000 ||
		got.GetChargedSeconds() != 42 || got.GetEndReason() != "balance_exhausted" ||
		got.GetTrunkId() != "trunk-a" {
		t.Fatalf("wrong CDR: %+v", got)
	}
}
