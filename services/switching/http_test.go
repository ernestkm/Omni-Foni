package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func facadeTestServer(t *testing.T, reg *Registry) (*httptest.Server, *grpc.ClientConn) {
	t.Helper()
	cc, err := grpc.NewClient(chargingAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	srv := httptest.NewServer(newFacade(reg, cc))
	t.Cleanup(srv.Close)
	return srv, cc
}

func TestFacadeStartAllowsAndReturnsTarget(t *testing.T) {
	seed(t, "acc-facade", 100000, "2782", "0.010000")
	reg := NewRegistry().WithRouting(fixedResolver{"trunk-a", "sip:gw:5060"})
	srv, _ := facadeTestServer(t, reg)

	resp, err := http.Post(srv.URL+"/start", "application/json",
		strings.NewReader(`{"account_id":"acc-facade","destination":"27821110000","cli":"27820000000"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out startResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.Allowed || out.TargetURI != "sip:gw:5060" || out.CallID == "" {
		t.Fatalf("unexpected start response: %+v", out)
	}

	// stop it
	stopResp, err := http.Post(srv.URL+"/stop", "application/json",
		strings.NewReader(`{"call_id":"`+out.CallID+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	stopResp.Body.Close()
}

func TestFacadeStartNoRouteDenies(t *testing.T) {
	reg := NewRegistry().WithRouting(fixedResolver{"", ""})
	srv, _ := facadeTestServer(t, reg)

	resp, err := http.Post(srv.URL+"/start", "application/json",
		strings.NewReader(`{"account_id":"acc-x","destination":"27821110000"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out startResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Allowed || out.Reason != "no_route" {
		t.Fatalf("want no_route denial, got %+v", out)
	}
}

func TestFacadeStartBadInput(t *testing.T) {
	srv, _ := facadeTestServer(t, NewRegistry())
	resp, err := http.Post(srv.URL+"/start", "application/json", strings.NewReader(`{"cli":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

func TestFacadeLiveListsCalls(t *testing.T) {
	seed(t, "acc-live-facade", 100000, "2782", "0.010000")
	reg := NewRegistry().WithRouting(fixedResolver{"trunk-a", "sip:gw:5060"})
	srv, _ := facadeTestServer(t, reg)

	startHTTPResp, err := http.Post(srv.URL+"/start", "application/json",
		strings.NewReader(`{"account_id":"acc-live-facade","destination":"27821110000"}`))
	if err != nil {
		t.Fatal(err)
	}
	var sr startResp
	_ = json.NewDecoder(startHTTPResp.Body).Decode(&sr)
	startHTTPResp.Body.Close()

	live, err := http.Get(srv.URL + "/live")
	if err != nil {
		t.Fatal(err)
	}
	defer live.Body.Close()
	var lr struct {
		Calls []struct {
			CallID string `json:"call_id"`
		} `json:"calls"`
	}
	if err := json.NewDecoder(live.Body).Decode(&lr); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range lr.Calls {
		if c.CallID == sr.CallID {
			found = true
		}
	}
	if !found {
		t.Fatalf("live call %s not listed", sr.CallID)
	}

	// cleanup
	_, _ = http.Post(srv.URL+"/stop", "application/json",
		strings.NewReader(`{"call_id":"`+sr.CallID+`"}`))
}

var _ = context.Background
