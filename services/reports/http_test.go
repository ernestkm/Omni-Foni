package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	chargingpb "omnifoni/charging/gen/chargingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
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
		case "Bearer empty":
			fmt.Fprint(w, `{"user_id":"u4","email":"e@x","role":"reseller","tenant_id":"t3"}`)
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
		case "t3":
			fmt.Fprint(w, `[]`)
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
	h := &httpServer{api: &reportsServer{
		store: store,
		payments: &fakePayments{resp: &paymentspb.ListPaymentsResponse{Payments: []*paymentspb.Payment{{
			PaymentId: 7, EventId: "evt", AccountId: "a1", Gateway: "stripe",
			AmountMinor: 500, Status: "recorded", CreatedAtMs: 1000,
		}}}},
		charges: &fakeCharges{
			resp: &chargingpb.ListChargesResponse{Charges: []*chargingpb.Charge{{
				SessionId: "s1", AccountId: "a1", ChargedSeconds: 3,
				AmountMinor: -4, BalanceAfter: 996, LastChargedAtMs: 2000,
			}}},
			balance: &chargingpb.ListLowBalanceResponse{Accounts: []*chargingpb.BalanceRow{{
				AccountId: "a1", Kind: "prepaid", Balance: 0, Currency: "ZAR",
			}}},
		},
	}, auth: NewAuthenticator(fakeIdentity(t).URL)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/cdrs", h.ListCdrs)
	mux.HandleFunc("GET /reports/summary", h.Summarize)
	mux.HandleFunc("GET /reports/payments", h.Payments)
	mux.HandleFunc("GET /reports/charges", h.Charges)
	mux.HandleFunc("GET /reports/low-balance", h.LowBalance)
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

func TestHTTPEmptyTenantSeesNothing(t *testing.T) {
	reset(t)
	start := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	seed(t,
		testRow("et1", "a1", "27820001", "trunk-1", "normal", start, 60, 10),
		testRow("et2", "a2", "27820002", "trunk-1", "normal", start.Add(time.Minute), 60, 20),
	)
	srv := facade(t)
	q := "from=2026-11-12&to=2026-11-13"

	code, body := get(t, srv.URL+"/reports/cdrs?"+q, "empty")
	if code != http.StatusOK || len(body["cdrs"].([]any)) != 0 {
		t.Fatalf("empty tenant cdrs = %d %v, want empty 200", code, body)
	}

	code, body = get(t, srv.URL+"/reports/summary?"+q+"&group_by=account", "empty")
	if code != http.StatusOK || len(body["rows"].([]any)) != 0 {
		t.Fatalf("empty tenant summary = %d %v, want empty rows", code, body)
	}
	totals := body["totals"].(map[string]any)
	if totals["calls"].(float64) != 0 || totals["amount_minor"].(float64) != 0 {
		t.Fatalf("empty tenant totals = %v, want zero", totals)
	}
}

func TestHTTPMoneyReports(t *testing.T) {
	srv := facade(t)
	q := "from=2026-11-17&to=2026-11-18"

	code, body := get(t, srv.URL+"/reports/payments?"+q, "reseller")
	if code != http.StatusOK {
		t.Fatalf("payments = %d (%v)", code, body)
	}
	payments := body["payments"].([]any)
	if len(payments) != 1 || payments[0].(map[string]any)["amount_minor"].(float64) != 500 {
		t.Fatalf("payments = %v", payments)
	}

	code, body = get(t, srv.URL+"/reports/charges?"+q, "reseller")
	if code != http.StatusOK {
		t.Fatalf("charges = %d (%v)", code, body)
	}
	charges := body["charges"].([]any)
	if len(charges) != 1 || charges[0].(map[string]any)["amount_minor"].(float64) != -4 {
		t.Fatalf("charges = %v", charges)
	}

	code, body = get(t, srv.URL+"/reports/low-balance?threshold=-5", "reseller")
	if code != http.StatusOK {
		t.Fatalf("low balance = %d (%v)", code, body)
	}
	accounts := body["accounts"].([]any)
	if len(accounts) != 1 || accounts[0].(map[string]any)["account_id"] != "a1" {
		t.Fatalf("accounts = %v", accounts)
	}

	if code, _ := get(t, srv.URL+"/reports/low-balance?threshold=abc", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bad threshold = %d, want 400", code)
	}
	if code, _ := get(t, srv.URL+"/reports/payments?to=2026-11-18", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("missing from = %d, want 400", code)
	}
	if code, _ := get(t, srv.URL+"/reports/payments?"+q, ""); code != http.StatusUnauthorized {
		t.Fatalf("no token = %d, want 401", code)
	}
	if code, _ := get(t, srv.URL+"/reports/charges?"+q, "user"); code != http.StatusForbidden {
		t.Fatalf("user role = %d, want 403", code)
	}
	if code, _ := get(t, srv.URL+"/reports/payments?"+q+"&account_id=a2", "reseller"); code != http.StatusOK {
		t.Fatalf("cross-tenant account filter = %d, want 200", code)
	}
}

func TestHTTPPrefixGrouping(t *testing.T) {
	reset(t)
	day := time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)
	seed(t, testRow("h1", "a1", "27820001", "trunk-1", "normal", day, 60, 10))
	srv := facade(t)

	code, body := get(t, srv.URL+"/reports/summary?group_by=prefix&prefix_len=2&from=2026-11-17&to=2026-11-18", "reseller")
	if code != http.StatusOK {
		t.Fatalf("code = %d (%v)", code, body)
	}
	rows := body["rows"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["key"] != "27" {
		t.Fatalf("rows = %v", rows)
	}

	if code, _ := get(t, srv.URL+"/reports/summary?group_by=prefix&prefix_len=abc&from=2026-11-17&to=2026-11-18", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bad prefix_len = %d, want 400", code)
	}
}
