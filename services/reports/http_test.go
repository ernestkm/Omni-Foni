package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func fakeIdentity(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/me", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Header.Get("Authorization") {
		case "Bearer admin":
			fmt.Fprint(w, `{"user_id":"u1","email":"a@x","role":"admin","tenant_id":"t0"}`)
		case "Bearer reseller":
			fmt.Fprint(w, `{"user_id":"u2","email":"r@x","role":"reseller","tenant_id":"t1"}`)
		case "Bearer user":
			fmt.Fprint(w, `{"user_id":"u3","email":"c@x","role":"user","tenant_id":"t1"}`)
		default:
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}
	})
	mux.HandleFunc("/accounts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("tenant_id") {
		case "t1":
			fmt.Fprint(w, `[{"account_id":"a1","tenant_id":"t1"}]`)
		case "t2":
			fmt.Fprint(w, `[{"account_id":"a2","tenant_id":"t2"}]`)
		default:
			http.Error(w, "boom", http.StatusInternalServerError)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func facade(t *testing.T) *httptest.Server {
	t.Helper()
	h := &httpServer{api: &reportsServer{store: store}, auth: NewAuthenticator(fakeIdentity(t).URL)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/cdrs", h.ListCdrs)
	mux.HandleFunc("GET /reports/summary", h.Summarize)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url, token string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

func TestHTTPAuthAndTenantScope(t *testing.T) {
	reset(t)
	start := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	seed(t,
		testRow("h1", "a1", "27820001", "trunk-1", "normal", start, 60, 10),
		testRow("h2", "a2", "27820002", "trunk-1", "normal", start.Add(time.Minute), 60, 20),
	)
	srv := facade(t)
	q := "from=2026-11-12&to=2026-11-13"

	if code, _ := get(t, srv.URL+"/reports/cdrs?"+q, ""); code != http.StatusUnauthorized {
		t.Fatalf("no token = %d, want 401", code)
	}
	if code, _ := get(t, srv.URL+"/reports/cdrs?"+q, "user"); code != http.StatusForbidden {
		t.Fatalf("user role = %d, want 403", code)
	}

	code, body := get(t, srv.URL+"/reports/cdrs?"+q, "reseller")
	if code != http.StatusOK {
		t.Fatalf("reseller = %d, want 200 (%v)", code, body)
	}
	if cdrs := body["cdrs"].([]any); len(cdrs) != 1 || cdrs[0].(map[string]any)["account_id"] != "a1" {
		t.Fatalf("reseller cdrs = %v, want only a1", cdrs)
	}

	code, body = get(t, srv.URL+"/reports/cdrs?"+q+"&account_id=a2", "reseller")
	if code != http.StatusOK || len(body["cdrs"].([]any)) != 0 {
		t.Fatalf("cross-tenant filter = %d %v, want empty 200", code, body)
	}

	code, body = get(t, srv.URL+"/reports/cdrs?"+q+"&tenant_id=t2", "admin")
	if code != http.StatusOK {
		t.Fatalf("admin t2 = %d (%v)", code, body)
	}
	if cdrs := body["cdrs"].([]any); len(cdrs) != 1 || cdrs[0].(map[string]any)["account_id"] != "a2" {
		t.Fatalf("admin t2 cdrs = %v, want a2", cdrs)
	}

	if code, _ := get(t, srv.URL+"/reports/cdrs?"+q, "admin"); code != http.StatusBadRequest {
		t.Fatalf("admin without tenant = %d, want 400", code)
	}
	if code, _ := get(t, srv.URL+"/reports/cdrs?"+q+"&tenant_id=t9", "admin"); code != http.StatusBadGateway {
		t.Fatalf("identity failure = %d, want 502", code)
	}
	if code, _ := get(t, srv.URL+"/reports/cdrs?to=2026-11-13", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("missing from = %d, want 400", code)
	}
}

func TestHTTPSummary(t *testing.T) {
	reset(t)
	start := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	seed(t,
		testRow("hs1", "a1", "27820001", "trunk-1", "normal", start, 60, 10),
		testRow("hs2", "a1", "27820001", "trunk-1", "normal", start.Add(time.Minute), 60, 20),
	)
	srv := facade(t)
	code, body := get(t, srv.URL+"/reports/summary?from=2026-11-12&to=2026-11-13&group_by=destination", "reseller")
	if code != http.StatusOK {
		t.Fatalf("summary = %d (%v)", code, body)
	}
	rows := body["rows"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["key"] != "27820001" {
		t.Fatalf("rows = %v", rows)
	}
	totals := body["totals"].(map[string]any)
	if totals["amount_minor"].(float64) != 30 || totals["calls"].(float64) != 2 {
		t.Fatalf("totals = %v", totals)
	}
	if code, _ := get(t, srv.URL+"/reports/summary?from=2026-11-12&to=2026-11-13", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("missing group_by = %d, want 400", code)
	}
}
