package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Integration tests run against the dev identity DB (make up).
func dsn() string {
	if s := os.Getenv("IDENTITY_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/identity"
}

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	pool, err = pgxpool.New(ctx, dsn())
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// The handler needs an empty users table for bootstrap, so every test run
// resets it first (dev data, no other writers).
func newServer(t *testing.T) (http.Handler, string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE sessions, users, tenants, accounts CASCADE`); err != nil {
		t.Fatal(err)
	}
	id := NewIdentityServer(pool)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { id.ServeHTTP(w, r) }), ""
}

func (s *IdentityServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/bootstrap":
		s.Bootstrap(w, r)
	case "/users":
		s.CreateUser(w, r)
	case "/tenants":
		s.CreateTenant(w, r)
	case "/sessions":
		s.Login(w, r)
	case "/me":
		s.Me(w, r)
	case "/accounts":
		if r.Method == http.MethodPost {
			s.CreateAccount(w, r)
		} else {
			s.ListAccounts(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}

func call(t *testing.T, h http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode %d %q: %v", rec.Code, rec.Body, err)
	}
}

// TestBootstrapLoginAdminFlows: bootstrap → login → create tenant user (with a
// second login) → /me reports role and tenant.
func TestBootstrapLoginAdminFlows(t *testing.T) {
	h, _ := newServer(t)

	rec := call(t, h, "POST", "/bootstrap", "", map[string]any{
		"tenant_name": "tt", "email": "root@tt.example", "password": "hunter2hunter2"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("bootstrap %d: %s", rec.Code, rec.Body)
	}
	var boot struct {
		TenantID     string `json:"tenant_id"`
		SessionToken string `json:"session_token"`
	}
	decode(t, rec, &boot)

	rec = call(t, h, "POST", "/users", boot.SessionToken, map[string]any{
		"tenant_id": boot.TenantID, "email": "staff@tt.example", "password": "password123", "role": "user"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create user %d: %s", rec.Code, rec.Body)
	}
	var created struct {
		SessionToken string `json:"session_token"`
	}
	decode(t, rec, &created)

	rec = call(t, h, "GET", "/me", created.SessionToken, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("me %d: %s", rec.Code, rec.Body)
	}
	var me struct {
		Email    string `json:"email"`
		Role     string `json:"role"`
		TenantID string `json:"tenant_id"`
	}
	decode(t, rec, &me)
	if me.Email != "staff@tt.example" || me.Role != "user" || me.TenantID != boot.TenantID {
		t.Fatalf("me: %+v", me)
	}
}

// TestSecondBootstrapConflicts: only the first bootstrap succeeds.
func TestSecondBootstrapConflicts(t *testing.T) {
	h, _ := newServer(t)
	body := map[string]any{"tenant_name": "a", "email": "a@a.example", "password": "hunter2hunter2"}
	if rec := call(t, h, "POST", "/bootstrap", "", body); rec.Code != http.StatusCreated {
		t.Fatalf("first %d", rec.Code)
	}
	if rec := call(t, h, "POST", "/bootstrap", "", body); rec.Code != http.StatusConflict {
		t.Fatalf("second %d, want 409", rec.Code)
	}
}

// TestUserCannotCreateTenantScopedUsers: non-admins are 403.
func TestUserCannotCreateTenantScopedUsers(t *testing.T) {
	h, _ := newServer(t)
	rec := call(t, h, "POST", "/bootstrap", "", map[string]any{
		"tenant_name": "tt", "email": "root@tt.example", "password": "hunter2hunter2"})
	var boot struct {
		TenantID     string `json:"tenant_id"`
		SessionToken string `json:"session_token"`
	}
	decode(t, rec, &boot)

	rec = call(t, h, "POST", "/users", boot.SessionToken, map[string]any{
		"tenant_id": boot.TenantID, "email": "u@tt.example", "password": "password123", "role": "user"})
	var created struct {
		SessionToken string `json:"session_token"`
	}
	decode(t, rec, &created)

	rec = call(t, h, "POST", "/users", created.SessionToken, map[string]any{
		"tenant_id": boot.TenantID, "email": "x@tt.example", "password": "password123", "role": "admin"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("user creating users: %d, want 403", rec.Code)
	}
}

// TestLoginFailures: wrong password and unknown email are both 401, and a
// duplicate email creation is 409.
func TestLoginFailures(t *testing.T) {
	h, _ := newServer(t)
	rec := call(t, h, "POST", "/bootstrap", "", map[string]any{
		"tenant_name": "tt", "email": "root@tt.example", "password": "hunter2hunter2"})
	var boot struct {
		TenantID     string `json:"tenant_id"`
		SessionToken string `json:"session_token"`
	}
	decode(t, rec, &boot)

	if rec := call(t, h, "POST", "/sessions", "", map[string]any{
		"email": "root@tt.example", "password": "wrong"}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad password %d, want 401", rec.Code)
	}
	if rec := call(t, h, "POST", "/sessions", "", map[string]any{
		"email": "nobody@tt.example", "password": "hunter2hunter2"}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unknown email %d, want 401", rec.Code)
	}
	rec = call(t, h, "POST", "/users", boot.SessionToken, map[string]any{
		"tenant_id": boot.TenantID, "email": "root@tt.example", "password": "password123", "role": "user"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("dup email %d, want 409", rec.Code)
	}
}

// bootstrapPair creates a fresh tenant with an admin and a reseller, returning
// their tokens and the tenant id.
func bootstrapPair(t *testing.T, h http.Handler, name string) (adminTok, resellerTok, tenantID string) {
	t.Helper()
	rec := call(t, h, "POST", "/bootstrap", "", map[string]any{
		"tenant_name": name, "email": "root@" + name + ".example", "password": "hunter2hunter2"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("bootstrap %d: %s", rec.Code, rec.Body)
	}
	var boot struct {
		TenantID     string `json:"tenant_id"`
		SessionToken string `json:"session_token"`
	}
	decode(t, rec, &boot)
	rec = call(t, h, "POST", "/users", boot.SessionToken, map[string]any{
		"tenant_id": boot.TenantID, "email": "reseller@" + name + ".example",
		"password": "password123", "role": "reseller"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("reseller create %d: %s", rec.Code, rec.Body)
	}
	var res struct {
		SessionToken string `json:"session_token"`
	}
	decode(t, rec, &res)
	return boot.SessionToken, res.SessionToken, boot.TenantID
}

// TestAccountRegistrationScoped: admin may register accounts into any tenant,
// resellers only into their own; users get 403.
func TestAccountRegistrationScoped(t *testing.T) {
	h, _ := newServer(t)
	adminTok, resTok, tenantA := bootstrapPair(t, h, "ta")
	rec := call(t, h, "POST", "/tenants", adminTok, map[string]any{"name": "tb"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create tenant %d: %s", rec.Code, rec.Body)
	}
	var tb struct {
		TenantID string `json:"tenant_id"`
	}
	decode(t, rec, &tb)
	tenantB := tb.TenantID

	// admin into tenant B: allowed
	rec = call(t, h, "POST", "/accounts", adminTok, map[string]any{
		"account_id": "acc-b1", "tenant_id": tenantB})
	if rec.Code != http.StatusCreated {
		t.Fatalf("admin cross-tenant %d: %s", rec.Code, rec.Body)
	}
	// reseller into own tenant: allowed
	rec = call(t, h, "POST", "/accounts", resTok, map[string]any{
		"account_id": "acc-a1", "tenant_id": tenantA})
	if rec.Code != http.StatusCreated {
		t.Fatalf("reseller own-tenant %d: %s", rec.Code, rec.Body)
	}
	// reseller into other tenant: 403, and no row
	rec = call(t, h, "POST", "/accounts", resTok, map[string]any{
		"account_id": "acc-b2", "tenant_id": tenantB})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("reseller cross-tenant %d, want 403", rec.Code)
	}
	// duplicate account id: 409
	rec = call(t, h, "POST", "/accounts", adminTok, map[string]any{
		"account_id": "acc-b1", "tenant_id": tenantB})
	if rec.Code != http.StatusConflict {
		t.Fatalf("dup %d, want 409", rec.Code)
	}
}

// TestListAccountsScoped: resellers only ever see their own tenant's accounts.
func TestListAccountsScoped(t *testing.T) {
	h, _ := newServer(t)
	adminTok, resTok, tenantA := bootstrapPair(t, h, "ta")
	rec := call(t, h, "POST", "/tenants", adminTok, map[string]any{"name": "tb"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("create tenant %d: %s", rec.Code, rec.Body)
	}
	var tb struct {
		TenantID string `json:"tenant_id"`
	}
	decode(t, rec, &tb)
	tenantB := tb.TenantID
	if rec := call(t, h, "POST", "/accounts", adminTok, map[string]any{
		"account_id": "acc-a1", "tenant_id": tenantA}); rec.Code != http.StatusCreated {
		t.Fatal("seed a1")
	}
	if rec := call(t, h, "POST", "/accounts", adminTok, map[string]any{
		"account_id": "acc-b1", "tenant_id": tenantB}); rec.Code != http.StatusCreated {
		t.Fatal("seed b1")
	}

	// reseller asks for the other tenant explicitly: still scoped to own
	rec = call(t, h, "GET", "/accounts?tenant_id="+tenantB, resTok, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list %d", rec.Code)
	}
	var got []struct {
		AccountID string `json:"account_id"`
	}
	decode(t, rec, &got)
	if len(got) != 1 || got[0].AccountID != "acc-a1" {
		t.Fatalf("reseller saw %v, want only own tenant acc-a1", got)
	}
	// admin lists tenant B explicitly
	rec = call(t, h, "GET", "/accounts?tenant_id="+tenantB, adminTok, nil)
	decode(t, rec, &got)
	if len(got) != 1 || got[0].AccountID != "acc-b1" {
		t.Fatalf("admin saw %v, want acc-b1", got)
	}
}
