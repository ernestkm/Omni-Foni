package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chargingpb "omnifoni/charging/gen/chargingpb"
	paymentspb "omnifoni/payments/gen/paymentspb"
	pb "omnifoni/reports/gen/reportspb"
	switchingpb "omnifoni/switching/gen/switchingpb"
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
		live: &fakeLive{resp: &switchingpb.ListLiveCallsResponse{Calls: []*switchingpb.LiveCall{{
			CallId: "c1", SessionId: "s1", AccountId: "a1", Destination: "2782", Cli: "cli",
			TrunkId: "trunk-1", StartedAtMs: 1000, ElapsedSeconds: 5, MaxSeconds: 600, ChargedSeconds: 4,
		}}}},
	}, auth: NewAuthenticator(fakeIdentity(t).URL)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/cdrs", h.ListCdrs)
	mux.HandleFunc("GET /reports/summary", h.Summarize)
	mux.HandleFunc("GET /reports/payments", h.Payments)
	mux.HandleFunc("GET /reports/charges", h.Charges)
	mux.HandleFunc("GET /reports/low-balance", h.LowBalance)
	mux.HandleFunc("GET /reports/live-calls", h.LiveCalls)
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

func getRaw(t *testing.T, url, token string) (int, http.Header, string) {
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
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, string(b)
}

func TestHTTPCSVCDRsAndSummary(t *testing.T) {
	reset(t)
	day := time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)
	seed(t,
		testRow("csv1", "a1", "27820001", "trunk-1", "normal", day, 60, 10),
		testRow("csv2", "a1", "27820002", "trunk-1", "normal", day.Add(time.Hour), 30, 5),
	)
	srv := facade(t)
	window := "from=2026-11-17&to=2026-11-18"

	code, hdr, body := getRaw(t, srv.URL+"/reports/cdrs?"+window+"&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("cdrs = %d (%s)", code, body)
	}
	if ct := hdr.Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}
	if cd := hdr.Get("Content-Disposition"); !strings.Contains(cd, `filename="reports-cdrs-`) {
		t.Fatalf("content-disposition = %q", cd)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines = %d (%q)", len(lines), body)
	}
	wantHeader := "call_id,account_id,destination,cli,started_at_ms,ended_at_ms,charged_seconds,end_reason,trunk_id,rate_per_sec,amount_minor,rate_missing"
	if lines[0] != wantHeader {
		t.Fatalf("header = %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "csv2,a1,27820002,") || !strings.HasSuffix(lines[1], ",5,false") {
		t.Fatalf("row = %q", lines[1])
	}

	code, _, body = getRaw(t, srv.URL+"/reports/summary?group_by=day&"+window+"&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("summary = %d (%s)", code, body)
	}
	lines = strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 3 || lines[0] != "key,calls,seconds,amount_minor" {
		t.Fatalf("summary lines = %q", lines)
	}
	if lines[2] != "TOTAL,2,90,15" {
		t.Fatalf("totals row = %q", lines[2])
	}

	code, hdr, body = getRaw(t, srv.URL+"/reports/cdrs?"+window+"&format=json", "reseller")
	if code != http.StatusOK || !strings.HasPrefix(hdr.Get("Content-Type"), "application/json") ||
		!strings.Contains(body, `"cdrs"`) {
		t.Fatalf("json = %d %q %q", code, hdr.Get("Content-Type"), body)
	}

	if code, _, _ := getRaw(t, srv.URL+"/reports/cdrs?"+window+"&format=bogus", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bogus format = %d, want 400", code)
	}
}

type pagingAPI struct {
	API
	fetched []string
}

func (p *pagingAPI) ListCdrs(_ context.Context, req *pb.ListCdrsRequest) (*pb.ListCdrsResponse, error) {
	p.fetched = append(p.fetched, req.GetCursor())
	rows := make([]*pb.Cdr, 0, 1000)
	for i := 0; i < 1000; i++ {
		rows = append(rows, &pb.Cdr{CallId: fmt.Sprintf("%s-%04d", req.GetCursor(), i), AccountId: "a1"})
	}
	next := ""
	if req.GetCursor() != "p9" {
		next = "p" + strconv.Itoa(len(p.fetched))
	}
	return &pb.ListCdrsResponse{Cdrs: rows, NextCursor: next}, nil
}

func TestHTTPCSVCap(t *testing.T) {
	api := &pagingAPI{}
	mux := http.NewServeMux()
	h := &httpServer{api: api, auth: NewAuthenticator(fakeIdentity(t).URL)}
	mux.HandleFunc("GET /reports/cdrs", h.ListCdrs)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	code, _, body := getRaw(t, srv.URL+"/reports/cdrs?from=2026-11-17&to=2026-11-18&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 10001 {
		t.Fatalf("lines = %d, want 10001 (header + 10000 rows)", len(lines))
	}
	if len(api.fetched) != 10 {
		t.Fatalf("fetched %d pages, want 10", len(api.fetched))
	}
}

func TestHTTPCSVMoneyReports(t *testing.T) {
	srv := facade(t)
	q := "from=2026-11-17&to=2026-11-18"

	code, _, body := getRaw(t, srv.URL+"/reports/payments?"+q+"&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("payments = %d (%s)", code, body)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 2 || lines[0] != "payment_id,event_id,account_id,gateway,amount_minor,status,created_at_ms" {
		t.Fatalf("payments lines = %q", lines)
	}
	if lines[1] != "7,evt,a1,stripe,500,recorded,1000" {
		t.Fatalf("payments row = %q", lines[1])
	}

	code, _, body = getRaw(t, srv.URL+"/reports/charges?"+q+"&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("charges = %d (%s)", code, body)
	}
	lines = strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 2 || lines[0] != "session_id,account_id,charged_seconds,amount_minor,balance_after,last_charged_at_ms" {
		t.Fatalf("charges lines = %q", lines)
	}
	if lines[1] != "s1,a1,3,-4,996,2000" {
		t.Fatalf("charges row = %q", lines[1])
	}

	code, _, body = getRaw(t, srv.URL+"/reports/low-balance?threshold=-5&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("low balance = %d (%s)", code, body)
	}
	lines = strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 2 || lines[0] != "account_id,kind,balance,credit_limit,currency" {
		t.Fatalf("low balance lines = %q", lines)
	}
	if lines[1] != "a1,prepaid,0,0,ZAR" {
		t.Fatalf("low balance row = %q", lines[1])
	}

	if code, _, _ := getRaw(t, srv.URL+"/reports/payments?"+q+"&format=bogus", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bogus format = %d, want 400", code)
	}
	if code, _, _ := getRaw(t, srv.URL+"/reports/charges?"+q+"&format=csv", "user"); code != http.StatusForbidden {
		t.Fatalf("user csv = %d, want 403", code)
	}
}

func TestHTTPLiveCalls(t *testing.T) {
	srv := facade(t)

	code, body := get(t, srv.URL+"/reports/live-calls", "reseller")
	if code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	calls, ok := body["calls"].([]any)
	if !ok || len(calls) != 1 {
		t.Fatalf("calls = %v", body["calls"])
	}
	row := calls[0].(map[string]any)
	if row["call_id"] != "c1" || row["account_id"] != "a1" || row["max_seconds"] != float64(600) ||
		row["charged_seconds"] != float64(4) || row["elapsed_seconds"] != float64(5) {
		t.Fatalf("row = %v", row)
	}

	if code, _ := get(t, srv.URL+"/reports/live-calls", "user"); code != http.StatusForbidden {
		t.Fatalf("user = %d, want 403", code)
	}
	if code, _ := get(t, srv.URL+"/reports/live-calls", "admin"); code != http.StatusBadRequest {
		t.Fatalf("admin without tenant = %d, want 400", code)
	}
	if code, _ := get(t, srv.URL+"/reports/live-calls?tenant_id=t1", "admin"); code != http.StatusOK {
		t.Fatalf("admin t1 = %d", code)
	}
	code, body = get(t, srv.URL+"/reports/live-calls", "empty")
	if code != http.StatusOK {
		t.Fatalf("empty tenant = %d", code)
	}
	if calls, ok := body["calls"].([]any); !ok || len(calls) != 0 {
		t.Fatalf("empty tenant calls = %v", body["calls"])
	}

	code, hdr, raw := getRaw(t, srv.URL+"/reports/live-calls?format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("csv = %d (%s)", code, raw)
	}
	if ct := hdr.Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	if len(lines) != 2 ||
		lines[0] != "call_id,session_id,account_id,destination,cli,trunk_id,started_at_ms,elapsed_seconds,max_seconds,charged_seconds" {
		t.Fatalf("csv lines = %q", lines)
	}
	if lines[1] != "c1,s1,a1,2782,cli,trunk-1,1000,5,600,4" {
		t.Fatalf("csv row = %q", lines[1])
	}
	if code, _, _ := getRaw(t, srv.URL+"/reports/live-calls?format=bogus", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bogus format = %d, want 400", code)
	}

	down := &httpServer{api: &reportsServer{
		live: &fakeLive{err: status.Error(codes.Unavailable, "down")},
	}, auth: NewAuthenticator(fakeIdentity(t).URL)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/live-calls", down.LiveCalls)
	downSrv := httptest.NewServer(mux)
	t.Cleanup(downSrv.Close)
	if code, _ := get(t, downSrv.URL+"/reports/live-calls", "reseller"); code != http.StatusServiceUnavailable {
		t.Fatalf("outage = %d, want 503", code)
	}
}
