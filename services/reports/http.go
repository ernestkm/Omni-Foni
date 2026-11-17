package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/reports/gen/reportspb"
)

// API is the report surface the facade serves; *reportsServer implements it.
type API interface {
	ListCdrs(context.Context, *pb.ListCdrsRequest) (*pb.ListCdrsResponse, error)
	Summarize(context.Context, *pb.SummarizeRequest) (*pb.SummarizeResponse, error)
}

type Authenticator struct {
	base   string
	client *http.Client
}

func NewAuthenticator(base string) *Authenticator {
	return &Authenticator{base: base, client: &http.Client{}}
}

type meResponse struct {
	Role     string `json:"role"`
	TenantID string `json:"tenant_id"`
}

type accountResponse struct {
	AccountID string `json:"account_id"`
}

// Accounts validates the bearer token and returns the account IDs the caller
// may see. Status is the HTTP status to send when err is non-nil.
func (a *Authenticator) Accounts(ctx context.Context, token, requestedTenant string) ([]string, int, error) {
	me, code, err := a.getMe(ctx, token)
	if err != nil {
		return nil, code, err
	}
	if me.Role == "user" {
		return nil, http.StatusForbidden, errors.New("forbidden")
	}
	scope := me.TenantID
	if me.Role == "admin" {
		scope = requestedTenant
	}
	if scope == "" {
		return nil, http.StatusBadRequest, errors.New("tenant_id required")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		a.base+"/accounts?tenant_id="+url.QueryEscape(scope), nil)
	if err != nil {
		return nil, http.StatusBadGateway, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, http.StatusBadGateway, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, http.StatusBadGateway, errors.New("identity accounts failed")
	}
	var accts []accountResponse
	if err := json.NewDecoder(resp.Body).Decode(&accts); err != nil {
		return nil, http.StatusBadGateway, err
	}
	ids := make([]string, 0, len(accts))
	for _, a := range accts {
		ids = append(ids, a.AccountID)
	}
	return ids, http.StatusOK, nil
}

func (a *Authenticator) getMe(ctx context.Context, token string) (meResponse, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+"/me", nil)
	if err != nil {
		return meResponse{}, http.StatusBadGateway, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := a.client.Do(req)
	if err != nil {
		return meResponse{}, http.StatusBadGateway, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return meResponse{}, http.StatusUnauthorized, errors.New("unauthorized")
	}
	var me meResponse
	if err := json.NewDecoder(resp.Body).Decode(&me); err != nil {
		return meResponse{}, http.StatusBadGateway, err
	}
	return me, http.StatusOK, nil
}

type httpServer struct {
	api  API
	auth *Authenticator
}

func bearer(r *http.Request) (string, bool) {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
		return "", false
	}
	return h[len(prefix):], true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func intersect(allowed, requested []string) []string {
	ok := make(map[string]bool, len(allowed))
	for _, id := range allowed {
		ok[id] = true
	}
	out := make([]string, 0, len(requested))
	for _, id := range requested {
		if ok[id] {
			out = append(out, id)
		}
	}
	return out
}

type cdrJSON struct {
	CallID         string `json:"call_id"`
	AccountID      string `json:"account_id"`
	Destination    string `json:"destination"`
	CLI            string `json:"cli"`
	StartedAtMS    int64  `json:"started_at_ms"`
	EndedAtMS      int64  `json:"ended_at_ms"`
	ChargedSeconds int64  `json:"charged_seconds"`
	EndReason      string `json:"end_reason"`
	TrunkID        string `json:"trunk_id"`
	RatePerSec     string `json:"rate_per_sec"`
	AmountMinor    int64  `json:"amount_minor"`
	RateMissing    bool   `json:"rate_missing"`
}

type listJSON struct {
	Cdrs       []cdrJSON `json:"cdrs"`
	NextCursor string    `json:"next_cursor"`
}

type rowJSON struct {
	Key         string `json:"key"`
	Calls       int64  `json:"calls"`
	Seconds     int64  `json:"seconds"`
	AmountMinor int64  `json:"amount_minor"`
}

type summaryJSON struct {
	Rows   []rowJSON `json:"rows"`
	Totals rowJSON   `json:"totals"`
}

func (h *httpServer) ListCdrs(w http.ResponseWriter, r *http.Request) {
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	q := r.URL.Query()
	ids, code, err := h.auth.Accounts(r.Context(), token, q.Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	req := &pb.ListCdrsRequest{
		Filter: &pb.CdrFilter{
			AccountIds:        ids,
			From:              q.Get("from"),
			To:                q.Get("to"),
			DestinationPrefix: q.Get("destination_prefix"),
			TrunkId:           q.Get("trunk_id"),
			EndReason:         q.Get("end_reason"),
		},
		Cursor: q.Get("cursor"),
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid limit")
			return
		}
		req.Limit = int32(n)
	}
	if requested := q["account_id"]; len(requested) > 0 {
		req.Filter.AccountIds = intersect(ids, requested)
		if len(req.Filter.AccountIds) == 0 {
			writeJSON(w, http.StatusOK, listJSON{Cdrs: []cdrJSON{}})
			return
		}
	}

	resp, err := h.api.ListCdrs(r.Context(), req)
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	out := listJSON{Cdrs: []cdrJSON{}, NextCursor: resp.GetNextCursor()}
	for _, c := range resp.GetCdrs() {
		out.Cdrs = append(out.Cdrs, cdrJSON{
			CallID: c.GetCallId(), AccountID: c.GetAccountId(), Destination: c.GetDestination(),
			CLI: c.GetCli(), StartedAtMS: c.GetStartedAtMs(), EndedAtMS: c.GetEndedAtMs(),
			ChargedSeconds: c.GetChargedSeconds(), EndReason: c.GetEndReason(), TrunkID: c.GetTrunkId(),
			RatePerSec: c.GetRatePerSec(), AmountMinor: c.GetAmountMinor(), RateMissing: c.GetRateMissing(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *httpServer) Summarize(w http.ResponseWriter, r *http.Request) {
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	q := r.URL.Query()
	ids, code, err := h.auth.Accounts(r.Context(), token, q.Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	group, ok := groupFromString(q.Get("group_by"))
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid group_by")
		return
	}
	req := &pb.SummarizeRequest{
		Filter: &pb.CdrFilter{
			AccountIds:        ids,
			From:              q.Get("from"),
			To:                q.Get("to"),
			DestinationPrefix: q.Get("destination_prefix"),
			TrunkId:           q.Get("trunk_id"),
			EndReason:         q.Get("end_reason"),
		},
		GroupBy: group,
	}
	if requested := q["account_id"]; len(requested) > 0 {
		req.Filter.AccountIds = intersect(ids, requested)
		if len(req.Filter.AccountIds) == 0 {
			writeJSON(w, http.StatusOK, summaryJSON{Rows: []rowJSON{}, Totals: rowJSON{}})
			return
		}
	}

	resp, err := h.api.Summarize(r.Context(), req)
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	out := summaryJSON{Rows: []rowJSON{}, Totals: rowJSON{
		Key: resp.GetTotals().GetKey(), Calls: resp.GetTotals().GetCalls(),
		Seconds: resp.GetTotals().GetSeconds(), AmountMinor: resp.GetTotals().GetAmountMinor(),
	}}
	for _, row := range resp.GetRows() {
		out.Rows = append(out.Rows, rowJSON{
			Key: row.GetKey(), Calls: row.GetCalls(), Seconds: row.GetSeconds(), AmountMinor: row.GetAmountMinor()})
	}
	writeJSON(w, http.StatusOK, out)
}

func groupFromString(s string) (pb.GroupBy, bool) {
	switch s {
	case "day":
		return pb.GroupBy_DAY, true
	case "destination":
		return pb.GroupBy_DESTINATION, true
	case "account":
		return pb.GroupBy_ACCOUNT, true
	case "trunk":
		return pb.GroupBy_TRUNK, true
	}
	return pb.GroupBy_GROUP_BY_UNSPECIFIED, false
}

func grpcStatus(err error) int {
	if status.Code(err) == codes.InvalidArgument {
		return http.StatusBadRequest
	}
	return http.StatusServiceUnavailable
}
