package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	billingpb "omnifoni/billing/gen/billingpb"
)

// commissionDSN reads the dev billing DB for invoice seeding.
func commissionDSN() string {
	if s := os.Getenv("BILLING_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/billing"
}

func billingAddr() string {
	if s := os.Getenv("IDENTITY_BILLING_ADDR"); s != "" {
		return s
	}
	return "localhost:50054"
}

// TestCommissionReconciles: a tenant's billed invoice lines (seeded directly
// in the billing DB) reconcile exactly through the /commission join, and
// cross-tenant commission stays scoped.
func TestCommissionReconciles(t *testing.T) {
	h, _ := newServer(t)
	adminTok, resTok, tenantA := bootstrapPair(t, h, "ta")

	// tenant B via POST /tenants, with one account
	rec := call(t, h, "POST", "/tenants", adminTok, map[string]any{"name": "tb"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("tenant b %d: %s", rec.Code, rec.Body)
	}
	var tb struct {
		TenantID string `json:"tenant_id"`
	}
	decode(t, rec, &tb)

	for _, a := range []struct{ id, tenant string }{
		{"acc-a1", tenantA}, {"acc-a2", tenantA}, {"acc-b1", tb.TenantID},
	} {
		rec = call(t, h, "POST", "/accounts", adminTok, map[string]any{
			"account_id": a.id, "tenant_id": a.tenant})
		if rec.Code != http.StatusCreated {
			t.Fatalf("account %s: %d", a.id, rec.Code)
		}
	}

	// Seed invoice lines in the billing DB: acc-a1 12+30, acc-a2 100, acc-b1 999
	bdb, err := sql.Open("pgx", commissionDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()
	if _, err := bdb.Exec(`DELETE FROM invoice_lines WHERE account_id IN ('acc-a1','acc-a2','acc-b1')`); err != nil {
		t.Fatal(err)
	}
	seed := `INSERT INTO invoice_lines (call_id, account_id, period, charged_seconds, amount_minor, rate_per_sec)
		VALUES ($1, $2, '2026-11', 60, $3, 0.333333)`
	for _, l := range []struct {
		call, acct string
		amt        int64
	}{
		{"c-a1-1", "acc-a1", 12}, {"c-a1-2", "acc-a1", 30}, {"c-a2-1", "acc-a2", 100}, {"c-b1-1", "acc-b1", 999},
	} {
		if _, err := bdb.Exec(seed, l.call, l.acct, l.amt); err != nil {
			t.Fatal(err)
		}
	}

	// Billing gRPC client for the handler under test
	bcc, err := grpc.NewClient(billingAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer bcc.Close()
	commission := NewCommissionServer(NewIdentityServer(pool), billingpb.NewBillingClient(bcc))

	// Reseller (tenant A) reports commission: reconciles to 142, never sees acc-b1
	req := httptest.NewRequest("GET",
		"/commission?period=2026-11&tenant_id="+tb.TenantID, nil)
	req.Header.Set("Authorization", "Bearer "+resTok)
	recw := httptest.NewRecorder()
	commission.ServeHTTP(recw, req)
	if recw.Code != http.StatusOK {
		t.Fatalf("reseller commission %d: %s", recw.Code, recw.Body)
	}
	var got struct {
		TotalMinor int64 `json:"total_minor"`
		Accounts   []struct {
			AccountID  string `json:"account_id"`
			TotalMinor int64  `json:"total_minor"`
		} `json:"accounts"`
	}
	if err := json.Unmarshal(recw.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.TotalMinor != 142 {
		t.Fatalf("total %d, want 142 (12+30+100)", got.TotalMinor)
	}
	if len(got.Accounts) != 2 {
		t.Fatalf("accounts %v, want 2 (tenant A only)", got.Accounts)
	}

	// Admin scopes to tenant B: 999
	req = httptest.NewRequest("GET", "/commission?period=2026-11&tenant_id="+tb.TenantID, nil)
	req.Header.Set("Authorization", "Bearer "+adminTok)
	recw = httptest.NewRecorder()
	commission.ServeHTTP(recw, req)
	if recw.Code != http.StatusOK {
		t.Fatalf("admin commission %d: %s", recw.Code, recw.Body)
	}
	if err := json.Unmarshal(recw.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.TotalMinor != 999 {
		t.Fatalf("tenant b total %d, want 999", got.TotalMinor)
	}

	// No invoice for a period: zero total, no error
	req = httptest.NewRequest("GET", "/commission?period=2026-10&tenant_id="+tenantA, nil)
	req.Header.Set("Authorization", "Bearer "+adminTok)
	recw = httptest.NewRecorder()
	commission.ServeHTTP(recw, req)
	if recw.Code != http.StatusOK {
		t.Fatalf("empty period %d: %s", recw.Code, recw.Body)
	}
	if err := json.Unmarshal(recw.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.TotalMinor != 0 {
		t.Fatalf("empty period total %d, want 0", got.TotalMinor)
	}
}
