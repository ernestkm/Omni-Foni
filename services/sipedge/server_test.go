package main

import (
	"context"
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
}

func (f *fakeFacade) start(ctx context.Context, req startReq) (startResp, error) {
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
	srv := &sipedgeServer{accounts: map[string]string{"172.28.0.1": "acc-a"}, facade: facade, store: st}
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return hs, st
}

func TestInviteAllow(t *testing.T) {
	f := &fakeFacade{startRes: startResp{Allowed: true, CallID: "sw-1", TargetURI: "sip:gw:5060"}}
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
	f := &fakeFacade{startRes: startResp{Allowed: true, CallID: "sw-2", TargetURI: "sip:gw:5060"}}
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
