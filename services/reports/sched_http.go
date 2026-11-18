package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const maxScheduleAccounts = 100

type scheduleBody struct {
	ReportKind string   `json:"report_kind"`
	WindowKind string   `json:"window_kind"`
	GroupBy    string   `json:"group_by"`
	PrefixLen  int      `json:"prefix_len"`
	Frequency  string   `json:"frequency"`
	AtHour     int      `json:"at_hour"`
	AtMinute   int      `json:"at_minute"`
	Weekday    *int     `json:"weekday"`
	DayOfMonth *int     `json:"day_of_month"`
	Enabled    *bool    `json:"enabled"`
	AccountIDs []string `json:"account_ids"`
}

func (b scheduleBody) schedule() Schedule {
	enabled := true
	if b.Enabled != nil {
		enabled = *b.Enabled
	}
	group := b.GroupBy
	if group == "" {
		group = "none"
	}
	prefix := b.PrefixLen
	if prefix == 0 {
		prefix = defaultPrefixLen
	}
	return Schedule{
		ReportKind: b.ReportKind, WindowKind: b.WindowKind, GroupBy: group,
		PrefixLen: prefix, Frequency: b.Frequency, AtHour: b.AtHour, AtMinute: b.AtMinute,
		Weekday: b.Weekday, DayOfMonth: b.DayOfMonth, Enabled: enabled,
		AccountIDs: b.AccountIDs,
	}
}

func scheduleJSON(s Schedule) map[string]any {
	return map[string]any{
		"schedule_id": s.ScheduleID, "tenant_id": s.TenantID, "account_ids": s.AccountIDs,
		"report_kind": s.ReportKind, "window_kind": s.WindowKind, "group_by": s.GroupBy,
		"prefix_len": s.PrefixLen, "frequency": s.Frequency, "at_hour": s.AtHour,
		"at_minute": s.AtMinute, "weekday": s.Weekday, "day_of_month": s.DayOfMonth,
		"enabled": s.Enabled, "created_by": s.CreatedBy,
		"created_at": s.CreatedAt.UTC().Format(time.RFC3339),
		"updated_at": s.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func artifactJSON(a Artifact) map[string]any {
	return map[string]any{
		"artifact_id": a.ArtifactID, "schedule_id": a.ScheduleID, "slot_key": a.SlotKey,
		"period_from": a.PeriodFrom.UTC().Format(time.RFC3339),
		"period_to":   a.PeriodTo.UTC().Format(time.RFC3339),
		"row_count":   a.RowCount,
		"created_at":  a.CreatedAt.UTC().Format(time.RFC3339),
	}
}

var scheduleKinds = map[string]bool{"cdrs": true, "summary": true, "payments": true, "charges": true, "low_balance": true}
var windowKinds = map[string]bool{"last_day": true, "last_week": true, "last_month": true, "previous_month": true}
var frequencies = map[string]bool{"daily": true, "weekly": true, "monthly": true}

// validateSchedule enforces the API-level rules; store-level CHECKs are the
// backstop. It returns the normalized schedule: account_ids intersected with
// the caller's allowed set, and weekday/day_of_month nulled when the frequency
// does not use them.
func validateSchedule(in Schedule, allowed []string) (Schedule, error) {
	if !scheduleKinds[in.ReportKind] {
		return in, fmt.Errorf("invalid report_kind %q", in.ReportKind)
	}
	if !windowKinds[in.WindowKind] {
		return in, fmt.Errorf("invalid window_kind %q", in.WindowKind)
	}
	if !frequencies[in.Frequency] {
		return in, fmt.Errorf("invalid frequency %q", in.Frequency)
	}
	if in.AtHour < 0 || in.AtHour > 23 || in.AtMinute < 0 || in.AtMinute > 59 {
		return in, errors.New("at_hour/at_minute out of range")
	}
	switch in.Frequency {
	case "weekly":
		if in.Weekday == nil || *in.Weekday < 0 || *in.Weekday > 6 {
			return in, errors.New("weekday required for weekly (0..6)")
		}
		in.DayOfMonth = nil
	case "monthly":
		if in.DayOfMonth == nil || *in.DayOfMonth < 1 || *in.DayOfMonth > 31 {
			return in, errors.New("day_of_month required for monthly (1..31)")
		}
		in.Weekday = nil
	default:
		in.Weekday = nil
		in.DayOfMonth = nil
	}
	switch in.GroupBy {
	case "none":
	case "prefix":
		if in.ReportKind != "summary" {
			return in, errors.New("group_by prefix only for summary")
		}
		if in.PrefixLen < 1 || in.PrefixLen > maxPrefixLen {
			return in, errors.New("prefix_len must be 1..15")
		}
	default:
		return in, fmt.Errorf("invalid group_by %q", in.GroupBy)
	}
	if len(in.AccountIDs) > maxScheduleAccounts {
		return in, fmt.Errorf("account_ids exceeds %d", maxScheduleAccounts)
	}
	if len(in.AccountIDs) > 0 {
		kept := intersect(allowed, in.AccountIDs)
		if len(kept) == 0 {
			return in, errors.New("no matching accounts")
		}
		in.AccountIDs = kept
	}
	// Account scope is captured at write time: an omitted filter becomes the
	// caller's tenant account set, so scheduled renders never pass an empty
	// (globally scoped) filter to the internal report APIs.
	if len(in.AccountIDs) == 0 {
		in.AccountIDs = append([]string(nil), allowed...)
	}
	if in.AccountIDs == nil {
		in.AccountIDs = []string{}
	}
	return in, nil
}

func (h *httpServer) requireSched(w http.ResponseWriter) bool {
	if h.sched == nil {
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return false
	}
	return true
}

func (h *httpServer) CreateSchedule(w http.ResponseWriter, r *http.Request) {
	if !h.requireSched(w) {
		return
	}
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var body scheduleBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	tenant, allowed, code, err := h.auth.Scope(r.Context(), token, r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	in, err := validateSchedule(body.schedule(), allowed)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	uid, code, err := h.auth.UserID(r.Context(), token)
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	in.TenantID = tenant
	in.CreatedBy = uid
	created, err := h.sched.store.CreateSchedule(r.Context(), in)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return
	}
	writeJSON(w, http.StatusCreated, scheduleJSON(created))
}

func (h *httpServer) ListSchedules(w http.ResponseWriter, r *http.Request) {
	if !h.requireSched(w) {
		return
	}
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	tenant, _, code, err := h.auth.Scope(r.Context(), token, r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	list, err := h.sched.store.ListSchedules(r.Context(), tenant)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return
	}
	out := make([]map[string]any, 0, len(list))
	for _, s := range list {
		out = append(out, scheduleJSON(s))
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": out})
}

func (h *httpServer) getOwnedSchedule(w http.ResponseWriter, r *http.Request, tenant string) (Schedule, bool) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		writeErr(w, http.StatusNotFound, "not found")
		return Schedule{}, false
	}
	sch, err := h.sched.store.GetSchedule(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "not found")
			return Schedule{}, false
		}
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return Schedule{}, false
	}
	if sch.TenantID != tenant {
		writeErr(w, http.StatusNotFound, "not found")
		return Schedule{}, false
	}
	return sch, true
}

func (h *httpServer) GetSchedule(w http.ResponseWriter, r *http.Request) {
	if !h.requireSched(w) {
		return
	}
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	tenant, _, code, err := h.auth.Scope(r.Context(), token, r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	sch, ok := h.getOwnedSchedule(w, r, tenant)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, scheduleJSON(sch))
}

// PatchSchedule replaces the mutable fields of a schedule; it is not a merge.
// Omitted fields take their defaults (group_by=none, prefix_len=3, enabled=true,
// account_ids=[]), so a partial body that omits a required field is rejected.
func (h *httpServer) PatchSchedule(w http.ResponseWriter, r *http.Request) {
	if !h.requireSched(w) {
		return
	}
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	tenant, allowed, code, err := h.auth.Scope(r.Context(), token, r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	sch, ok := h.getOwnedSchedule(w, r, tenant)
	if !ok {
		return
	}
	var body scheduleBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	next, err := validateSchedule(body.schedule(), allowed)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	sch.ReportKind, sch.WindowKind, sch.GroupBy, sch.PrefixLen = next.ReportKind, next.WindowKind, next.GroupBy, next.PrefixLen
	sch.Frequency, sch.AtHour, sch.AtMinute = next.Frequency, next.AtHour, next.AtMinute
	sch.Weekday, sch.DayOfMonth, sch.Enabled, sch.AccountIDs = next.Weekday, next.DayOfMonth, next.Enabled, next.AccountIDs
	if err := h.sched.store.UpdateSchedule(r.Context(), sch); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return
	}
	updated, err := h.sched.store.GetSchedule(r.Context(), sch.ScheduleID)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return
	}
	writeJSON(w, http.StatusOK, scheduleJSON(updated))
}

func (h *httpServer) DeleteSchedule(w http.ResponseWriter, r *http.Request) {
	if !h.requireSched(w) {
		return
	}
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	tenant, _, code, err := h.auth.Scope(r.Context(), token, r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	if _, ok := h.getOwnedSchedule(w, r, tenant); !ok {
		return
	}
	if err := h.sched.store.DeleteSchedule(r.Context(), r.PathValue("id")); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *httpServer) ListArtifacts(w http.ResponseWriter, r *http.Request) {
	if !h.requireSched(w) {
		return
	}
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	tenant, _, code, err := h.auth.Scope(r.Context(), token, r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	sch, ok := h.getOwnedSchedule(w, r, tenant)
	if !ok {
		return
	}
	list, err := h.sched.store.ListArtifacts(r.Context(), sch.ScheduleID)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return
	}
	out := make([]map[string]any, 0, len(list))
	for _, a := range list {
		out = append(out, artifactJSON(a))
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifacts": out})
}

func (h *httpServer) DownloadArtifact(w http.ResponseWriter, r *http.Request) {
	if !h.requireSched(w) {
		return
	}
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	tenant, _, code, err := h.auth.Scope(r.Context(), token, r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	sch, ok := h.getOwnedSchedule(w, r, tenant)
	if !ok {
		return
	}
	aid := r.PathValue("aid")
	if _, err := uuid.Parse(aid); err != nil {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	a, body, err := h.sched.store.GetArtifact(r.Context(), sch.ScheduleID, aid)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, http.StatusNotFound, "not found")
			return
		}
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="reports-scheduled-%s-%s.csv"`, sch.ScheduleID, a.SlotKey))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}

func (h *httpServer) RunSchedule(w http.ResponseWriter, r *http.Request) {
	if !h.requireSched(w) {
		return
	}
	token, ok := bearer(r)
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	tenant, _, code, err := h.auth.Scope(r.Context(), token, r.URL.Query().Get("tenant_id"))
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	sch, ok := h.getOwnedSchedule(w, r, tenant)
	if !ok {
		return
	}
	a, err := h.sched.RunScheduleNow(r.Context(), sch)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"artifact_id": a.ArtifactID})
}
