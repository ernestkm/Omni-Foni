package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseAccounts(t *testing.T) {
	m := parseAccounts("172.28.0.1=acc-a, 10.0.0.1 = acc-b ,bad")
	if m["172.28.0.1"] != "acc-a" || m["10.0.0.1"] != "acc-b" || len(m) != 2 {
		t.Fatalf("unexpected accounts: %+v", m)
	}
}

func TestSipCode(t *testing.T) {
	cases := map[string]int{
		"ocs_unavailable":     http.StatusServiceUnavailable,
		"routing_unavailable": http.StatusServiceUnavailable,
		"sbc_unavailable":     http.StatusServiceUnavailable,
		"fraud_unavailable":   http.StatusServiceUnavailable,
		"no_rate":             http.StatusServiceUnavailable,
		"internal_error":      http.StatusServiceUnavailable,
		"fraud_destination":   http.StatusForbidden,
		"no_route":            http.StatusForbidden,
		"ip_blocked":          http.StatusForbidden,
		"whatever":            http.StatusServiceUnavailable,
	}
	for reason, want := range cases {
		if got := sipCode(reason); got != want {
			t.Errorf("%s: want %d, got %d", reason, want, got)
		}
	}
}

type fakeFacade struct {
	started  startReq
	stopped  []string
	startRes startResp
	startErr error
	starts   int
}

func (f *fakeFacade) start(ctx context.Context, req startReq) (startResp, error) {
	f.starts++
	f.started = req
	return f.startRes, f.startErr
}
func (f *fakeFacade) stop(ctx context.Context, id string) (bool, error) {
	f.stopped = append(f.stopped, id)
	return true, nil
}

func newTestSipedge(t *testing.T, facade *fakeFacade) (*httptest.Server, *store) {
	t.Helper()
	st := newStore()
	srv := &sipedgeServer{accounts: map[string]string{"172.28.0.1": "acc-a"}, facade: facade, store: st, decisions: newDecisionCache()}
	hs := httptest.NewServer(srv.handler())
	t.Cleanup(hs.Close)
	return hs, st
}

func TestInviteAllow(t *testing.T) {
	f := &fakeFacade{startRes: startResp{Allowed: true, CallID: "sw-1", TargetURI: "sip:gw:5060", MaxSeconds: 300}}
	hs, st := newTestSipedge(t, f)

	resp, err := http.Post(hs.URL+"/invite", "text/plain",
		strings.NewReader("ci-1|172.28.0.1|27820000000|27821110000|27821110000|7|9"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	if f.started.AccountID != "acc-a" || f.started.Destination != "27821110000" {
		t.Fatalf("unexpected start req: %+v", f.started)
	}
	if _, ok := st.get("ci-1"); !ok {
		t.Fatal("call id not tracked")
	}
}

func TestInviteUnknownIP(t *testing.T) {
	f := &fakeFacade{}
	hs, _ := newTestSipedge(t, f)

	resp, err := http.Post(hs.URL+"/invite", "text/plain",
		strings.NewReader("ci-2|10.9.9.9|27820000000|27821110000|27821110000|1|2"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %d", resp.StatusCode)
	}
}

func TestInviteDenyMapping(t *testing.T) {
	for _, tc := range []struct {
		reason string
		code   int
	}{{"fraud_destination", http.StatusForbidden}, {"ocs_unavailable", http.StatusServiceUnavailable}} {
		f := &fakeFacade{startRes: startResp{Allowed: false, Reason: tc.reason}}
		hs, _ := newTestSipedge(t, f)
		resp, err := http.Post(hs.URL+"/invite", "text/plain",
			strings.NewReader("ci-3|172.28.0.1|27820000000|27821110000|27821110000|1|2"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.code {
			t.Fatalf("%s: want %d, got %d", tc.reason, tc.code, resp.StatusCode)
		}
	}
}

func TestByeStopsCall(t *testing.T) {
	f := &fakeFacade{startRes: startResp{Allowed: true, CallID: "sw-2", TargetURI: "sip:gw:5060", MaxSeconds: 300}}
	hs, st := newTestSipedge(t, f)

	inv, _ := http.Post(hs.URL+"/invite", "text/plain",
		strings.NewReader("ci-4|172.28.0.1|27820000000|27821110000|27821110000|3|4"))
	inv.Body.Close()

	resp, err := http.Post(hs.URL+"/bye", "text/plain", strings.NewReader("BYE ci-4"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(f.stopped) != 1 || f.stopped[0] != "sw-2" {
		t.Fatalf("want stop sw-2, got %+v", f.stopped)
	}
	if _, ok := st.get("ci-4"); ok {
		t.Fatal("call id still tracked after bye")
	}
}

func TestInviteAllowReturnsLifetimeAndTarget(t *testing.T) {
	f := &fakeFacade{startRes: startResp{
		Allowed: true, CallID: "sw-lifetime", TargetURI: "sip:gw:5060", MaxSeconds: 305,
	}}
	hs, _ := newTestSipedge(t, f)

	resp, err := http.Post(hs.URL+"/invite", "text/plain",
		strings.NewReader("ci-lifetime|172.28.0.1|27820000000|27821110000|27821110000|1|2"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(body), "305|sip:gw:5060"; got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestInviteReplaysDecisionWithoutRestartingCall(t *testing.T) {
	f := &fakeFacade{startRes: startResp{
		Allowed: true, CallID: "sw-replay", TargetURI: "sip:gw:5060", MaxSeconds: 120,
	}}
	hs, _ := newTestSipedge(t, f)
	invite := "ci-replay|172.28.0.1|27820000000|27821110000|27821110000|1|2"

	bodies := make([]string, 0, 2)
	for range 2 {
		resp, err := http.Post(hs.URL+"/invite", "text/plain", strings.NewReader(invite))
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(b))
	}
	if f.starts != 1 {
		t.Fatalf("want one facade start, got %d", f.starts)
	}
	if bodies[0] != bodies[1] {
		t.Fatalf("replay body differs: %q != %q", bodies[0], bodies[1])
	}
}

func TestInviteDenyBodyIsSanitized(t *testing.T) {
	f := &fakeFacade{startRes: startResp{Allowed: false, Reason: "no_route\r\nX-Evil: 1"}}
	hs, _ := newTestSipedge(t, f)

	resp, err := http.Post(hs.URL+"/invite", "text/plain",
		strings.NewReader("ci-sanitize|172.28.0.1|27820000000|27821110000|27821110000|1|2"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "\r") || strings.Contains(string(body), "X-Evil") {
		t.Fatalf("unsafe deny body: %q", body)
	}
	if got, want := string(body), "policy_denied"; got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestInviteResponseHasRequestID(t *testing.T) {
	f := &fakeFacade{startRes: startResp{Allowed: true, CallID: "sw-rid", TargetURI: "sip:gw:5060", MaxSeconds: 60}}
	hs, _ := newTestSipedge(t, f)

	resp, err := http.Post(hs.URL+"/invite", "text/plain",
		strings.NewReader("ci-rid|172.28.0.1|27820000000|27821110000|27821110000|1|2"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("x-request-id") == "" {
		t.Fatal("missing request id header")
	}
}

func TestDecisionCacheEvictsOldestWhenFull(t *testing.T) {
	c := newDecisionCache()
	d := decision{status: http.StatusOK, body: "ok"}
	for i := range maxDecisions {
		c.put(fmt.Sprintf("ci-%04d", i), d)
	}
	c.put("ci-new", d)

	if len(c.items) != maxDecisions {
		t.Fatalf("want %d cached decisions, got %d", maxDecisions, len(c.items))
	}
	if _, ok := c.items["ci-0000"]; ok {
		t.Fatal("oldest decision was not evicted")
	}
	if _, ok := c.items["ci-new"]; !ok {
		t.Fatal("new decision is missing")
	}
}
