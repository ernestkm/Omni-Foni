package main

import (
	"context"
	_ "embed"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

//go:embed migrate.sql
var schemaSQL string

// Migrate applies the schema. Idempotent, safe to run at boot.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

// IdentityServer serves the admin/portal REST surface (stdlib net/http; HTTPS
// terminates at the mesh edge per plan section 3.3).
type IdentityServer struct {
	pool *pgxpool.Pool
}

func NewIdentityServer(pool *pgxpool.Pool) *IdentityServer {
	return &IdentityServer{pool: pool}
}

const sessionTTL = 24 * time.Hour

// Bootstrap creates the first tenant and an admin user, but only while the
// users table is empty (409 afterwards). Dev conveniences: production portals
// provision the first admin through an operator flow instead.
func (s *IdentityServer) Bootstrap(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TenantName string `json:"tenant_name"`
		Email      string `json:"email"`
		Password   string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantName == "" ||
		req.Email == "" || len(req.Password) < 8 {
		http.Error(w, "tenant_name, email, password(>=8) required", http.StatusBadRequest)
		return
	}
	var n int64
	if err := s.pool.QueryRow(r.Context(), `SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		s.err500(w, err)
		return
	}
	if n > 0 {
		http.Error(w, "already bootstrapped", http.StatusConflict)
		return
	}
	// ponytail: two statements, not one tx; bootstrap race window is benign
	var tenantID string
	if err := s.pool.QueryRow(r.Context(),
		`INSERT INTO tenants (name) VALUES ($1) RETURNING tenant_id::text`,
		req.TenantName).Scan(&tenantID); err != nil {
		s.err500(w, err)
		return
	}
	userID, token, err := s.createUser(r.Context(), tenantID, req.Email, req.Password, "admin")
	if err != nil {
		s.err500(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{
		"tenant_id": tenantID, "user_id": userID, "session_token": token})
}

func (s *IdentityServer) CreateUser(w http.ResponseWriter, r *http.Request) {
	_, role, err := s.authenticate(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if role != "admin" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var req struct {
		TenantID string `json:"tenant_id"`
		Email    string `json:"email"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantID == "" ||
		req.Email == "" || len(req.Password) < 8 || (req.Role != "admin" && req.Role != "reseller" && req.Role != "user") {
		http.Error(w, "tenant_id, email, password(>=8), role(admin|reseller|user)", http.StatusBadRequest)
		return
	}
	userID, token, err := s.createUser(r.Context(), req.TenantID, req.Email, req.Password, req.Role)
	if err != nil {
		if errors.Is(err, errDupEmail) || errors.Is(err, errNoTenant) {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		s.err500(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"user_id": userID, "session_token": token})
}

func (s *IdentityServer) Login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Email == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var userID, hash string
	err := s.pool.QueryRow(r.Context(),
		`SELECT user_id::text, password_hash FROM users WHERE email = $1`,
		req.Email).Scan(&userID, &hash)
	if errors.Is(err, errNotFound) || bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)) != nil {
		// Same response for unknown email and bad password.
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	if err != nil {
		s.err500(w, err)
		return
	}
	token, err := newToken()
	if err != nil {
		s.err500(w, err)
		return
	}
	if _, err := s.pool.Exec(r.Context(),
		`INSERT INTO sessions (token, user_id, expires_at) VALUES ($1, $2, $3)`,
		token, userID, time.Now().Add(sessionTTL)); err != nil {
		s.err500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"user_id": userID, "session_token": token})
}

func (s *IdentityServer) Me(w http.ResponseWriter, r *http.Request) {
	userID, role, err := s.authenticate(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var email, tenantID string
	if err := s.pool.QueryRow(r.Context(),
		`SELECT email, role, tenant_id::text FROM users WHERE user_id = $1`,
		userID).Scan(&email, &role, &tenantID); err != nil {
		s.err500(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"user_id": userID, "email": email, "role": role, "tenant_id": tenantID})
}

// authenticate resolves the bearer token to (user_id, role, nil) or an error.
func (s *IdentityServer) authenticate(r *http.Request) (string, string, error) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return "", "", errNotFound
	}
	var userID, role string
	err := s.pool.QueryRow(r.Context(),
		`SELECT u.user_id::text, u.role FROM sessions s
		 JOIN users u ON u.user_id = s.user_id
		 WHERE s.token = $1 AND s.expires_at > now()`,
		strings.TrimPrefix(h, "Bearer ")).Scan(&userID, &role)
	return userID, role, err
}

// CreateTenant registers a new tenant; admins only. This is the production
// path for additional tenants after bootstrap.
func (s *IdentityServer) CreateTenant(w http.ResponseWriter, r *http.Request) {
	if _, role, err := s.authenticate(r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	} else if role != "admin" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	var tenantID string
	if err := s.pool.QueryRow(r.Context(),
		`INSERT INTO tenants (name) VALUES ($1) RETURNING tenant_id::text`,
		req.Name).Scan(&tenantID); err != nil {
		if isUniqueViolation(err) {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		s.err500(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"tenant_id": tenantID})
}

// CreateAccount registers a charging account under a tenant. Admins may
// register into any tenant; resellers only into their own (zero cross-tenant
// access); plain users are forbidden entirely.
func (s *IdentityServer) CreateAccount(w http.ResponseWriter, r *http.Request) {
	userID, role, tenantID, err := s.authenticateWithTenant(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if role == "user" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var req struct {
		AccountID string `json:"account_id"`
		TenantID  string `json:"tenant_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AccountID == "" || req.TenantID == "" {
		http.Error(w, "account_id and tenant_id required", http.StatusBadRequest)
		return
	}
	if role != "admin" && req.TenantID != tenantID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if _, err := s.pool.Exec(r.Context(),
		`INSERT INTO accounts (account_id, tenant_id, created_by)
		 VALUES ($1, $2::uuid, $3)`,
		req.AccountID, req.TenantID, userID); err != nil {
		if isUniqueViolation(err) {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		s.err500(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{
		"account_id": req.AccountID, "tenant_id": req.TenantID})
}

// ListAccounts returns the caller-visible accounts: admins with tenant_id
// param may list any tenant; resellers and users only their own.
func (s *IdentityServer) ListAccounts(w http.ResponseWriter, r *http.Request) {
	_, role, tenantID, err := s.authenticateWithTenant(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	scope := r.URL.Query().Get("tenant_id")
	if role != "admin" {
		scope = tenantID
	}
	if scope == "" {
		http.Error(w, "tenant_id required", http.StatusBadRequest)
		return
	}
	rows, err := s.pool.Query(r.Context(),
		`SELECT account_id, tenant_id::text FROM accounts WHERE tenant_id = $1::uuid ORDER BY account_id`,
		scope)
	if err != nil {
		s.err500(w, err)
		return
	}
	defer rows.Close()
	type acct struct {
		AccountID string `json:"account_id"`
		TenantID  string `json:"tenant_id"`
	}
	out := []acct{}
	for rows.Next() {
		var a acct
		if err := rows.Scan(&a.AccountID, &a.TenantID); err != nil {
			s.err500(w, err)
			return
		}
		out = append(out, a)
	}
	writeJSON(w, http.StatusOK, out)
}

// authenticateWithTenant resolves the bearer token to (user_id, role, tenant_id).
func (s *IdentityServer) authenticateWithTenant(r *http.Request) (string, string, string, error) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return "", "", "", errNotFound
	}
	var userID, role, tenantID string
	err := s.pool.QueryRow(r.Context(),
		`SELECT u.user_id::text, u.role, u.tenant_id::text FROM sessions s
		 JOIN users u ON u.user_id = s.user_id
		 WHERE s.token = $1 AND s.expires_at > now()`,
		strings.TrimPrefix(h, "Bearer ")).Scan(&userID, &role, &tenantID)
	return userID, role, tenantID, err
}

// isUniqueViolation reports postgres unique_violation via pgconn.PgError.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

var errDupEmail = errors.New("duplicate email")
var errNoTenant = errors.New("no tenant")
var errNotFound = errors.New("not found")

// createUser hashes, inserts, and opens a session; duplicate emails and
// missing tenants are surfaced as typed errors.
func (s *IdentityServer) createUser(ctx context.Context, tenantID, email, password, role string) (string, string, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)`, email).Scan(&exists); err != nil {
		return "", "", err
	}
	if exists {
		return "", "", errDupEmail
	}
	var tenantOK bool
	if err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM tenants WHERE tenant_id = $1::uuid)`, tenantID).Scan(&tenantOK); err != nil {
		return "", "", err
	}
	if !tenantOK {
		return "", "", errNoTenant
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", "", err
	}
	var userID string
	if err := s.pool.QueryRow(ctx,
		`INSERT INTO users (tenant_id, email, password_hash, role)
		 VALUES ($1::uuid, $2, $3, $4) RETURNING user_id::text`,
		tenantID, email, string(hash), role).Scan(&userID); err != nil {
		return "", "", err
	}
	token, err := newToken()
	if err != nil {
		return "", "", err
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO sessions (token, user_id, expires_at) VALUES ($1, $2, $3)`,
		token, userID, time.Now().Add(sessionTTL)); err != nil {
		return "", "", err
	}
	return userID, token, nil
}

func newToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *IdentityServer) err500(w http.ResponseWriter, err error) {
	slog.Error("identity", "err", err)
	http.Error(w, "internal", http.StatusInternalServerError)
}
