package main

import (
	"context"
	"net/http"
	"regexp"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"omnifoni/observability"

	billingpb "omnifoni/billing/gen/billingpb"
)

// periodRE is the billing.v1 YYYY-MM period syntax.
var periodRE = regexp.MustCompile(`^\d{4}-(0[1-9]|1[0-2])$`)

// CommissionServer answers the reseller commission report: tenant accounts
// joined against billing invoice totals for one period.
type CommissionServer struct {
	id      *IdentityServer
	billing billingpb.BillingClient
}

func NewCommissionServer(id *IdentityServer, billing billingpb.BillingClient) *CommissionServer {
	return &CommissionServer{id: id, billing: billing}
}

// ServeHTTP implements GET /commission?tenant_id=&period=YYYY-MM.
// Scoping matches the account registry: admins may query any tenant,
// resellers only their own; the response reconciles the tenant's billed
// amounts exactly (P3 acceptance).
func (s *CommissionServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, role, tenantID, err := s.id.authenticateWithTenant(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if role == "user" {
		http.Error(w, "forbidden", http.StatusForbidden)
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
	period := r.URL.Query().Get("period")
	if !periodRE.MatchString(period) {
		http.Error(w, "period must be YYYY-MM", http.StatusBadRequest)
		return
	}

	// Tenant's accounts from the identity DB.
	rows, err := s.id.pool.Query(r.Context(),
		`SELECT account_id FROM accounts WHERE tenant_id = $1::uuid ORDER BY account_id`, scope)
	if err != nil {
		s.id.err500(r.Context(), w, err)
		return
	}
	defer rows.Close()
	var accountIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			s.id.err500(r.Context(), w, err)
			return
		}
		accountIDs = append(accountIDs, id)
	}
	if err := rows.Err(); err != nil {
		s.id.err500(r.Context(), w, err)
		return
	}
	if len(accountIDs) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"tenant_id": scope, "period": period,
			"total_minor": 0, "accounts": []any{}})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	res, err := s.billing.GetInvoicesByAccounts(ctx,
		&billingpb.GetInvoicesByAccountsRequest{AccountIds: accountIDs, Period: period})
	if err != nil {
		s.id.err500(r.Context(), w, err)
		return
	}
	var total int64
	accounts := make([]map[string]any, 0, len(res.GetTotals()))
	for _, t := range res.GetTotals() {
		total += t.GetTotalMinor()
		accounts = append(accounts, map[string]any{
			"account_id": t.GetAccountId(), "total_minor": t.GetTotalMinor(),
			"line_count": t.GetLineCount()})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id": scope, "period": period, "total_minor": total, "accounts": accounts})
}

// DialBilling opens the identity→billing gRPC client used by /commission.
func DialBilling(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, observability.ClientOptions()...)...)
}
