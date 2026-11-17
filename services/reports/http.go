package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/reports/gen/reportspb"
)

// API is the report surface the facade serves; *reportsServer implements it.
type API interface {
	ListCdrs(context.Context, *pb.ListCdrsRequest) (*pb.ListCdrsResponse, error)
	Summarize(context.Context, *pb.SummarizeRequest) (*pb.SummarizeResponse, error)
	ListPayments(context.Context, *pb.ListPaymentsRequest) (*pb.ListPaymentsResponse, error)
	ListCharges(context.Context, *pb.ListChargesRequest) (*pb.ListChargesResponse, error)
	ListLowBalance(context.Context, *pb.LowBalanceRequest) (*pb.LowBalanceResponse, error)
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

const (
	csvMaxRows  = 10000
	csvPageSize = 1000
)

func csvRequested(r *http.Request) (bool, bool) {
	switch r.URL.Query().Get("format") {
	case "", "json":
		return false, true
	case "csv":
		return true, true
	}
	return false, false
}

func csvHeaders(w http.ResponseWriter, report string) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="reports-%s-%s.csv"`, report, time.Now().UTC().Format("2006-01-02")))
}

func writeCSVHeaderOnly(w http.ResponseWriter, report string, header []string) {
	csvHeaders(w, report)
	cw := csv.NewWriter(w)
	_ = cw.Write(header)
	cw.Flush()
}

// streamCSV fetches the first page before writing anything, so argument, auth
// and source errors still produce a normal JSON status. Later failures abort
// the response after logging. tail, when non-nil, is written after the rows.
func streamCSV(w http.ResponseWriter, r *http.Request, report string, header []string,
	fetch func(cursor string) ([][]string, string, error), tail func() []string) {
	rows, next, err := fetch("")
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	csvHeaders(w, report)
	cw := csv.NewWriter(w)
	_ = cw.Write(header)
	written := 0
	for {
		for _, row := range rows {
			if written >= csvMaxRows {
				break
			}
			_ = cw.Write(row)
			written++
		}
		cw.Flush()
		if written >= csvMaxRows || next == "" {
			break
		}
		rows, next, err = fetch(next)
		if err != nil {
			slog.ErrorContext(r.Context(), "csv stream aborted", "report", report, "err", err)
			return
		}
	}
	if tail != nil {
		_ = cw.Write(tail())
		cw.Flush()
	}
}

var cdrCSVHeader = []string{
	"call_id", "account_id", "destination", "cli", "started_at_ms", "ended_at_ms",
	"charged_seconds", "end_reason", "trunk_id", "rate_per_sec", "amount_minor", "rate_missing",
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

var summaryCSVHeader = []string{"key", "calls", "seconds", "amount_minor"}

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
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "cdrs", cdrCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, listJSON{Cdrs: []cdrJSON{}})
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
			if csvOut {
				writeCSVHeaderOnly(w, "cdrs", cdrCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, listJSON{Cdrs: []cdrJSON{}})
			return
		}
	}
	if csvOut {
		streamCSV(w, r, "cdrs", cdrCSVHeader, func(cursor string) ([][]string, string, error) {
			req.Cursor = cursor
			req.Limit = csvPageSize
			resp, err := h.api.ListCdrs(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			rows := make([][]string, 0, len(resp.GetCdrs()))
			for _, c := range resp.GetCdrs() {
				rows = append(rows, []string{
					c.GetCallId(), c.GetAccountId(), c.GetDestination(), c.GetCli(),
					strconv.FormatInt(c.GetStartedAtMs(), 10), strconv.FormatInt(c.GetEndedAtMs(), 10),
					strconv.FormatInt(c.GetChargedSeconds(), 10), c.GetEndReason(), c.GetTrunkId(),
					c.GetRatePerSec(), strconv.FormatInt(c.GetAmountMinor(), 10),
					strconv.FormatBool(c.GetRateMissing()),
				})
			}
			return rows, resp.GetNextCursor(), nil
		}, nil)
		return
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
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "summary", summaryCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, summaryJSON{Rows: []rowJSON{}, Totals: rowJSON{}})
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
	if v := q.Get("prefix_len"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid prefix_len")
			return
		}
		req.PrefixLen = int32(n)
	}
	if requested := q["account_id"]; len(requested) > 0 {
		req.Filter.AccountIds = intersect(ids, requested)
		if len(req.Filter.AccountIds) == 0 {
			if csvOut {
				writeCSVHeaderOnly(w, "summary", summaryCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, summaryJSON{Rows: []rowJSON{}, Totals: rowJSON{}})
			return
		}
	}
	if csvOut {
		var totals rowJSON
		streamCSV(w, r, "summary", summaryCSVHeader, func(string) ([][]string, string, error) {
			resp, err := h.api.Summarize(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			totals = rowJSON{
				Calls: resp.GetTotals().GetCalls(), Seconds: resp.GetTotals().GetSeconds(),
				AmountMinor: resp.GetTotals().GetAmountMinor(),
			}
			rows := make([][]string, 0, len(resp.GetRows()))
			for _, row := range resp.GetRows() {
				rows = append(rows, []string{
					row.GetKey(), strconv.FormatInt(row.GetCalls(), 10),
					strconv.FormatInt(row.GetSeconds(), 10), strconv.FormatInt(row.GetAmountMinor(), 10),
				})
			}
			return rows, "", nil
		}, func() []string {
			return []string{"TOTAL", strconv.FormatInt(totals.Calls, 10),
				strconv.FormatInt(totals.Seconds, 10), strconv.FormatInt(totals.AmountMinor, 10)}
		})
		return
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
	case "prefix":
		return pb.GroupBy_PREFIX, true
	}
	return pb.GroupBy_GROUP_BY_UNSPECIFIED, false
}

func grpcStatus(err error) int {
	if status.Code(err) == codes.InvalidArgument {
		return http.StatusBadRequest
	}
	return http.StatusServiceUnavailable
}

var paymentCSVHeader = []string{
	"payment_id", "event_id", "account_id", "gateway", "amount_minor", "status", "created_at_ms",
}

type paymentJSON struct {
	PaymentID   int64  `json:"payment_id"`
	EventID     string `json:"event_id"`
	AccountID   string `json:"account_id"`
	Gateway     string `json:"gateway"`
	AmountMinor int64  `json:"amount_minor"`
	Status      string `json:"status"`
	CreatedAtMS int64  `json:"created_at_ms"`
}

type paymentsJSON struct {
	Payments   []paymentJSON `json:"payments"`
	NextCursor string        `json:"next_cursor"`
}

var chargeCSVHeader = []string{
	"session_id", "account_id", "charged_seconds", "amount_minor", "balance_after", "last_charged_at_ms",
}

type chargeJSON struct {
	SessionID       string `json:"session_id"`
	AccountID       string `json:"account_id"`
	ChargedSeconds  int64  `json:"charged_seconds"`
	AmountMinor     int64  `json:"amount_minor"`
	BalanceAfter    int64  `json:"balance_after"`
	LastChargedAtMS int64  `json:"last_charged_at_ms"`
}

type chargesJSON struct {
	Charges    []chargeJSON `json:"charges"`
	NextCursor string       `json:"next_cursor"`
}

var balanceCSVHeader = []string{"account_id", "kind", "balance", "credit_limit", "currency"}

type balanceJSON struct {
	AccountID   string `json:"account_id"`
	Kind        string `json:"kind"`
	Balance     int64  `json:"balance"`
	CreditLimit int64  `json:"credit_limit"`
	Currency    string `json:"currency"`
}

type lowBalanceJSON struct {
	Accounts   []balanceJSON `json:"accounts"`
	NextCursor string        `json:"next_cursor"`
}

func (h *httpServer) Payments(w http.ResponseWriter, r *http.Request) {
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
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "payments", paymentCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, paymentsJSON{Payments: []paymentJSON{}})
		return
	}
	req := &pb.ListPaymentsRequest{
		Filter: &pb.MoneyFilter{AccountIds: ids, From: q.Get("from"), To: q.Get("to")},
		Status: q.Get("status"),
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
			if csvOut {
				writeCSVHeaderOnly(w, "payments", paymentCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, paymentsJSON{Payments: []paymentJSON{}})
			return
		}
	}
	if csvOut {
		streamCSV(w, r, "payments", paymentCSVHeader, func(cursor string) ([][]string, string, error) {
			req.Cursor = cursor
			req.Limit = csvPageSize
			resp, err := h.api.ListPayments(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			rows := make([][]string, 0, len(resp.GetPayments()))
			for _, p := range resp.GetPayments() {
				rows = append(rows, []string{
					strconv.FormatInt(p.GetPaymentId(), 10), p.GetEventId(), p.GetAccountId(),
					p.GetGateway(), strconv.FormatInt(p.GetAmountMinor(), 10), p.GetStatus(),
					strconv.FormatInt(p.GetCreatedAtMs(), 10),
				})
			}
			return rows, resp.GetNextCursor(), nil
		}, nil)
		return
	}

	resp, err := h.api.ListPayments(r.Context(), req)
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	out := paymentsJSON{Payments: []paymentJSON{}, NextCursor: resp.GetNextCursor()}
	for _, p := range resp.GetPayments() {
		out.Payments = append(out.Payments, paymentJSON{
			PaymentID: p.GetPaymentId(), EventID: p.GetEventId(), AccountID: p.GetAccountId(),
			Gateway: p.GetGateway(), AmountMinor: p.GetAmountMinor(), Status: p.GetStatus(),
			CreatedAtMS: p.GetCreatedAtMs(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *httpServer) Charges(w http.ResponseWriter, r *http.Request) {
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
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "charges", chargeCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, chargesJSON{Charges: []chargeJSON{}})
		return
	}
	req := &pb.ListChargesRequest{
		Filter: &pb.MoneyFilter{AccountIds: ids, From: q.Get("from"), To: q.Get("to")},
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
			if csvOut {
				writeCSVHeaderOnly(w, "charges", chargeCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, chargesJSON{Charges: []chargeJSON{}})
			return
		}
	}
	if csvOut {
		streamCSV(w, r, "charges", chargeCSVHeader, func(cursor string) ([][]string, string, error) {
			req.Cursor = cursor
			req.Limit = csvPageSize
			resp, err := h.api.ListCharges(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			rows := make([][]string, 0, len(resp.GetCharges()))
			for _, c := range resp.GetCharges() {
				rows = append(rows, []string{
					c.GetSessionId(), c.GetAccountId(),
					strconv.FormatInt(c.GetChargedSeconds(), 10), strconv.FormatInt(c.GetAmountMinor(), 10),
					strconv.FormatInt(c.GetBalanceAfter(), 10), strconv.FormatInt(c.GetLastChargedAtMs(), 10),
				})
			}
			return rows, resp.GetNextCursor(), nil
		}, nil)
		return
	}

	resp, err := h.api.ListCharges(r.Context(), req)
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	out := chargesJSON{Charges: []chargeJSON{}, NextCursor: resp.GetNextCursor()}
	for _, c := range resp.GetCharges() {
		out.Charges = append(out.Charges, chargeJSON{
			SessionID: c.GetSessionId(), AccountID: c.GetAccountId(),
			ChargedSeconds: c.GetChargedSeconds(), AmountMinor: c.GetAmountMinor(),
			BalanceAfter: c.GetBalanceAfter(), LastChargedAtMS: c.GetLastChargedAtMs(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *httpServer) LowBalance(w http.ResponseWriter, r *http.Request) {
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
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "low-balance", balanceCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, lowBalanceJSON{Accounts: []balanceJSON{}})
		return
	}
	req := &pb.LowBalanceRequest{AccountIds: ids, Cursor: q.Get("cursor")}
	if v := q.Get("threshold"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid threshold")
			return
		}
		req.ThresholdMinor = n
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
		req.AccountIds = intersect(ids, requested)
		if len(req.AccountIds) == 0 {
			if csvOut {
				writeCSVHeaderOnly(w, "low-balance", balanceCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, lowBalanceJSON{Accounts: []balanceJSON{}})
			return
		}
	}
	if csvOut {
		streamCSV(w, r, "low-balance", balanceCSVHeader, func(cursor string) ([][]string, string, error) {
			req.Cursor = cursor
			req.Limit = csvPageSize
			resp, err := h.api.ListLowBalance(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			rows := make([][]string, 0, len(resp.GetAccounts()))
			for _, a := range resp.GetAccounts() {
				rows = append(rows, []string{
					a.GetAccountId(), a.GetKind(), strconv.FormatInt(a.GetBalance(), 10),
					strconv.FormatInt(a.GetCreditLimit(), 10), a.GetCurrency(),
				})
			}
			return rows, resp.GetNextCursor(), nil
		}, nil)
		return
	}

	resp, err := h.api.ListLowBalance(r.Context(), req)
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	out := lowBalanceJSON{Accounts: []balanceJSON{}, NextCursor: resp.GetNextCursor()}
	for _, a := range resp.GetAccounts() {
		out.Accounts = append(out.Accounts, balanceJSON{
			AccountID: a.GetAccountId(), Kind: a.GetKind(), Balance: a.GetBalance(),
			CreditLimit: a.GetCreditLimit(), Currency: a.GetCurrency(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}
