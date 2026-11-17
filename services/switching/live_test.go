package main

import (
	"context"
	"testing"
	"time"

	pb "omnifoni/switching/gen/switchingpb"
)

func TestLiveCallsSnapshot(t *testing.T) {
	reg := NewRegistry()
	t0 := time.Now().Add(-10 * time.Second)
	mk := func(id, acct string, start time.Time, charged int64) *call {
		c := &call{
			id: id, sessionID: "s-" + id, accountID: acct, destination: "2782",
			cli: "cli", trunkID: "trunk-1", start: start,
			maxSeconds: 600, chargedSeconds: charged,
		}
		reg.calls[id] = c
		return c
	}
	mk("c2", "a1", t0.Add(time.Second), 2)
	mk("c1", "a1", t0, 5)
	mk("c3", "a2", t0, 0)
	done := mk("c4", "a1", t0, 0)
	done.mu.Lock()
	done.done = true
	done.mu.Unlock()

	all := reg.LiveCalls(nil)
	if len(all) != 3 {
		t.Fatalf("all = %d, want 3", len(all))
	}
	if all[0].CallID != "c1" || all[1].CallID != "c3" || all[2].CallID != "c2" {
		t.Fatalf("order = %q %q %q", all[0].CallID, all[1].CallID, all[2].CallID)
	}
	if all[0].ChargedSeconds != 5 || all[0].MaxSeconds != 600 || all[0].SessionID != "s-c1" {
		t.Fatalf("snapshot = %+v", all[0])
	}
	if got := reg.LiveCalls([]string{"a2"}); len(got) != 1 || got[0].CallID != "c3" {
		t.Fatalf("filter a2 = %+v", got)
	}
	if got := reg.LiveCalls([]string{"zz"}); len(got) != 0 {
		t.Fatalf("filter zz = %+v", got)
	}
}

func TestListLiveCalls(t *testing.T) {
	seed(t, "acc-live", 1000, "3555", "1.000000")
	client := startSwitching(t, chargingAddr())

	resp := startCall(t, client, "acc-live", "35551234567")
	if !resp.Allowed {
		t.Fatalf("denied: %+v", resp)
	}
	t.Cleanup(func() { stopCall(t, client, resp.CallId) })

	list, err := pb.NewSwitchingClient(client).ListLiveCalls(context.Background(),
		&pb.ListLiveCallsRequest{AccountIds: []string{"acc-live"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetCalls()) != 1 {
		t.Fatalf("calls = %d, want 1", len(list.GetCalls()))
	}
	got := list.GetCalls()[0]
	if got.GetCallId() != resp.CallId || got.GetSessionId() != resp.SessionId {
		t.Fatalf("got %+v, start %+v", got, resp)
	}
	if got.GetMaxSeconds() != resp.GetMaxSeconds() || got.GetMaxSeconds() <= 0 {
		t.Fatalf("max_seconds = %d, start %d", got.GetMaxSeconds(), resp.GetMaxSeconds())
	}
	if got.GetStartedAtMs() == 0 || got.GetElapsedSeconds() < 0 {
		t.Fatalf("started_at_ms = %d elapsed = %d", got.GetStartedAtMs(), got.GetElapsedSeconds())
	}

	stopCall(t, client, resp.CallId)
	waitReleased(t, client, resp.CallId)
	list, err = pb.NewSwitchingClient(client).ListLiveCalls(context.Background(),
		&pb.ListLiveCallsRequest{AccountIds: []string{"acc-live"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetCalls()) != 0 {
		t.Fatalf("finished call still listed: %+v", list.GetCalls())
	}
}
