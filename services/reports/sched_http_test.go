package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func schedFacade(t *testing.T) *httptest.Server {
	srv, _ := schedFacadeWithSigner(t)
	return srv
}

func schedFacadeWithSigner(t *testing.T) (*httptest.Server, *ArtifactSigner) {
	t.Helper()
	api := &reportsServer{store: store}
	pub := &fakePublisher{}
	signer := NewArtifactSigner("test-secret", "http://reports.test", time.Hour)
	sched := NewScheduler(schedDB, api, pub, signer)
	h := &httpServer{api: api, auth: NewAuthenticator(fakeIdentity(t).URL), sched: sched, signer: signer}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /scheduled-reports", h.CreateSchedule)
	mux.HandleFunc("GET /scheduled-reports", h.ListSchedules)
	mux.HandleFunc("GET /scheduled-reports/{id}", h.GetSchedule)
	mux.HandleFunc("PATCH /scheduled-reports/{id}", h.PatchSchedule)
	mux.HandleFunc("DELETE /scheduled-reports/{id}", h.DeleteSchedule)
	mux.HandleFunc("GET /scheduled-reports/{id}/artifacts", h.ListArtifacts)
	mux.HandleFunc("GET /scheduled-reports/{id}/artifacts/{aid}/download", h.DownloadArtifact)
	mux.HandleFunc("POST /scheduled-reports/{id}/run", h.RunSchedule)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, signer
}

func schedBody(kind string) string {
	return fmt.Sprintf(`{"report_kind":%q,"window_kind":"previous_month","frequency":"daily","at_hour":6,"at_minute":30}`, kind)
}

func doJSON(t *testing.T, method, url, token, body string) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body == "" {
		rdr = bytes.NewReader(nil)
	} else {
		rdr = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func createSchedule(t *testing.T, srv *httptest.Server, token, tenant, kind string) string {
	t.Helper()
	code, body := doJSON(t, http.MethodPost,
		srv.URL+"/scheduled-reports?tenant_id="+tenant, token, schedBody(kind))
	if code != http.StatusCreated {
		t.Fatalf("create = %d %v", code, body)
	}
	id, _ := body["schedule_id"].(string)
	if id == "" {
		t.Fatalf("no schedule_id: %v", body)
	}
	return id
}

func TestSchedHTTPLifecycle(t *testing.T) {
	schedReset(t)
	srv := schedFacade(t)

	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("no bearer = %d", code)
	}
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports?tenant_id=t1", "user", ""); code != http.StatusForbidden {
		t.Fatalf("role user = %d", code)
	}
	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/scheduled-reports", "admin", schedBody("cdrs")); code != http.StatusBadRequest {
		t.Fatalf("admin without tenant = %d", code)
	}
	if code, body := doJSON(t, http.MethodPost,
		srv.URL+"/scheduled-reports?tenant_id=t1", "admin", schedBody("bogus")); code != http.StatusBadRequest {
		t.Fatalf("bad kind = %d %v", code, body)
	}

	id := createSchedule(t, srv, "reseller", "", "cdrs")

	code, one := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id, "reseller", "")
	if code != http.StatusOK || one["report_kind"] != "cdrs" || one["tenant_id"] != "t1" {
		t.Fatalf("get = %d %v", code, one)
	}
	if one["created_by"] != "u2" {
		t.Fatalf("created_by = %v, want caller user id u2", one["created_by"])
	}
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/not-a-uuid", "reseller", ""); code != http.StatusNotFound {
		t.Fatalf("malformed schedule id = %d, want 404", code)
	}
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id, "admin", ""); code != http.StatusBadRequest {
		// admin without tenant_id still needs it
		t.Fatalf("admin get without tenant = %d", code)
	}
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports?tenant_id=t2", "admin", ""); code != http.StatusOK {
		// t2 is a different tenant; the new schedule must not appear
		t.Fatalf("admin t2 = %d", code)
	}

	code, list := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports?tenant_id=t1", "admin", "")
	arr, _ := list["schedules"].([]any)
	if code != http.StatusOK || len(arr) != 1 {
		t.Fatalf("list = %d %v", code, list)
	}

	patch := `{"report_kind":"cdrs","window_kind":"previous_month","frequency":"weekly","at_hour":7,"at_minute":15,"weekday":3,"enabled":false}`
	if code, body := doJSON(t, http.MethodPatch, srv.URL+"/scheduled-reports/"+id, "reseller", patch); code != http.StatusOK {
		t.Fatalf("patch = %d %v", code, body)
	} else if body["frequency"] != "weekly" || body["enabled"] != false {
		t.Fatalf("patch body = %v", body)
	}

	code, arts := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id+"/artifacts", "reseller", "")
	if code != http.StatusOK {
		t.Fatalf("artifacts = %d %v", code, arts)
	}
	if arr, _ := arts["artifacts"].([]any); len(arr) != 0 {
		t.Fatalf("expected no artifacts yet: %v", arts)
	}

	code, run := doJSON(t, http.MethodPost, srv.URL+"/scheduled-reports/"+id+"/run", "reseller", "")
	if code != http.StatusAccepted || run["artifact_id"] == "" {
		t.Fatalf("run = %d %v", code, run)
	}
	aid := run["artifact_id"].(string)

	code, arts = doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id+"/artifacts", "reseller", "")
	arr, _ = arts["artifacts"].([]any)
	if code != http.StatusOK || len(arr) != 1 {
		t.Fatalf("artifacts after run = %d %v", code, arts)
	}

	dlURL := srv.URL + "/scheduled-reports/" + id + "/artifacts/" + aid + "/download"
	req, _ := http.NewRequest(http.MethodGet, dlURL, nil)
	req.Header.Set("Authorization", "Bearer reseller")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}
	if cd := resp.Header.Get("Content-Disposition"); !strings.HasPrefix(cd, "attachment; filename=") ||
		!strings.Contains(cd, "reports-scheduled-") {
		t.Fatalf("content-disposition = %q", cd)
	}
	gotBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	_, wantBody, err := schedDB.GetArtifact(context.Background(), id, aid)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBody) != wantBody {
		t.Fatalf("download body = %q, want %q", gotBody, wantBody)
	}
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id+"/artifacts/other/download", "reseller", ""); code != http.StatusNotFound {
		t.Fatalf("foreign artifact = %d", code)
	}
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id, "admin", ""); code == http.StatusOK {
		t.Fatal("admin without tenant_id must not read schedules")
	}

	if code, _ := doJSON(t, http.MethodDelete, srv.URL+"/scheduled-reports/"+id, "reseller", ""); code != http.StatusNoContent {
		t.Fatalf("delete = %d", code)
	}
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id, "reseller", ""); code != http.StatusNotFound {
		t.Fatalf("get after delete = %d", code)
	}
}

func TestSchedHTTPCrossTenant(t *testing.T) {
	schedReset(t)
	srv := schedFacade(t)
	id := createSchedule(t, srv, "reseller", "", "cdrs") // tenant t1

	code, run := doJSON(t, http.MethodPost, srv.URL+"/scheduled-reports/"+id+"/run", "reseller", "")
	if code != http.StatusAccepted {
		t.Fatalf("run = %d %v", code, run)
	}
	aid, _ := run["artifact_id"].(string)
	if aid == "" {
		t.Fatalf("no artifact_id: %v", run)
	}

	cases := []struct {
		name, method, path, body string
	}{
		{"get", http.MethodGet, "/scheduled-reports/" + id + "?tenant_id=t2", ""},
		{"artifacts", http.MethodGet, "/scheduled-reports/" + id + "/artifacts?tenant_id=t2", ""},
		{"download", http.MethodGet, "/scheduled-reports/" + id + "/artifacts/" + aid + "/download?tenant_id=t2", ""},
		{"patch", http.MethodPatch, "/scheduled-reports/" + id + "?tenant_id=t2", schedBody("cdrs")},
		{"delete", http.MethodDelete, "/scheduled-reports/" + id + "?tenant_id=t2", ""},
		{"run", http.MethodPost, "/scheduled-reports/" + id + "/run?tenant_id=t2", ""},
	}
	for _, tc := range cases {
		if code, body := doJSON(t, tc.method, srv.URL+tc.path, "admin", tc.body); code != http.StatusNotFound {
			t.Fatalf("cross-tenant %s = %d %v, want 404", tc.name, code, body)
		}
	}

	// The foreign attempts must not have changed or removed the t1 schedule.
	if code, body := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id, "reseller", ""); code != http.StatusOK {
		t.Fatalf("owner get after cross-tenant attempts = %d %v", code, body)
	}
}

func TestSchedHTTPAccountScopeFailsClosed(t *testing.T) {
	schedReset(t)
	srv := schedFacade(t)
	// The "empty" identity is a reseller on tenant t3, which owns no accounts.
	foreign := `{"report_kind":"cdrs","window_kind":"previous_month","frequency":"daily","at_hour":6,"at_minute":30,"account_ids":["a1"]}`
	if code, body := doJSON(t, http.MethodPost, srv.URL+"/scheduled-reports", "empty", foreign); code != http.StatusBadRequest {
		t.Fatalf("zero-account tenant create = %d %v, want 400", code, body)
	}
	if code, body := doJSON(t, http.MethodPost, srv.URL+"/scheduled-reports", "empty", schedBody("cdrs")); code != http.StatusCreated {
		t.Fatalf("empty account_ids create = %d %v, want 201", code, body)
	}
}

func TestSchedHTTPPatchReplacesFields(t *testing.T) {
	schedReset(t)
	srv := schedFacade(t)
	id := createSchedule(t, srv, "reseller", "", "cdrs")

	// PATCH is a replace, not a merge: omitted required fields are rejected.
	if code, body := doJSON(t, http.MethodPatch, srv.URL+"/scheduled-reports/"+id, "reseller",
		`{"enabled":false}`); code != http.StatusBadRequest {
		t.Fatalf("partial patch = %d %v, want 400", code, body)
	}

	full := `{"report_kind":"summary","window_kind":"last_week","group_by":"prefix","prefix_len":5,` +
		`"frequency":"monthly","at_hour":9,"at_minute":45,"day_of_month":20,"enabled":false,"account_ids":["a1"]}`
	code, patched := doJSON(t, http.MethodPatch, srv.URL+"/scheduled-reports/"+id, "reseller", full)
	if code != http.StatusOK {
		t.Fatalf("full patch = %d %v", code, patched)
	}
	if patched["report_kind"] != "summary" || patched["window_kind"] != "last_week" ||
		patched["group_by"] != "prefix" || patched["prefix_len"] != float64(5) ||
		patched["frequency"] != "monthly" || patched["at_hour"] != float64(9) ||
		patched["at_minute"] != float64(45) || patched["day_of_month"] != float64(20) ||
		patched["enabled"] != false || patched["weekday"] != nil {
		t.Fatalf("patched body = %v", patched)
	}
	if ids, _ := patched["account_ids"].([]any); len(ids) != 1 || ids[0] != "a1" {
		t.Fatalf("patched account_ids = %v", patched["account_ids"])
	}

	// A later body that omits optional fields resets them to their defaults.
	// account_ids is special: omitting it recaptures the caller's tenant set
	// instead of resetting to an empty (globally scoped) filter.
	minimal := `{"report_kind":"cdrs","window_kind":"last_day","frequency":"daily","at_hour":0,"at_minute":0}`
	code, reset := doJSON(t, http.MethodPatch, srv.URL+"/scheduled-reports/"+id, "reseller", minimal)
	if code != http.StatusOK {
		t.Fatalf("minimal patch = %d %v", code, reset)
	}
	if reset["group_by"] != "none" || reset["prefix_len"] != float64(3) ||
		reset["enabled"] != true || reset["day_of_month"] != nil {
		t.Fatalf("defaults not applied = %v", reset)
	}
	if ids, _ := reset["account_ids"].([]any); len(ids) != 1 || ids[0] != "a1" {
		t.Fatalf("account_ids not recaptured = %v", reset["account_ids"])
	}
}

func TestSchedHTTPAccountScopeCapturedAtWrite(t *testing.T) {
	schedReset(t)
	srv := schedFacade(t)

	code, created := doJSON(t, http.MethodPost, srv.URL+"/scheduled-reports", "reseller", schedBody("cdrs"))
	if code != http.StatusCreated {
		t.Fatalf("create = %d %v", code, created)
	}
	ids, _ := created["account_ids"].([]any)
	if len(ids) != 1 || ids[0] != "a1" {
		t.Fatalf("created account_ids = %v, want tenant set [a1]", created["account_ids"])
	}
	id, _ := created["schedule_id"].(string)
	if id == "" {
		t.Fatalf("no schedule_id: %v", created)
	}

	// PATCH is a replace and omitted account_ids recaptures the tenant set too.
	code, patched := doJSON(t, http.MethodPatch, srv.URL+"/scheduled-reports/"+id, "reseller",
		`{"report_kind":"cdrs","window_kind":"last_day","frequency":"daily","at_hour":1,"at_minute":2}`)
	if code != http.StatusOK {
		t.Fatalf("patch = %d %v", code, patched)
	}
	ids, _ = patched["account_ids"].([]any)
	if len(ids) != 1 || ids[0] != "a1" {
		t.Fatalf("patched account_ids = %v, want tenant set [a1]", patched["account_ids"])
	}

	stored, err := schedDB.GetSchedule(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.AccountIDs) != 1 || stored.AccountIDs[0] != "a1" {
		t.Fatalf("stored account_ids = %v, want [a1]", stored.AccountIDs)
	}
}

func TestSchedHTTPPatchReturnsFreshTimestamp(t *testing.T) {
	schedReset(t)
	srv := schedFacade(t)
	id := createSchedule(t, srv, "reseller", "", "cdrs")

	ctx := context.Background()
	if _, err := schedDB.pool.Exec(ctx,
		`UPDATE schedules SET updated_at = now() - interval '1 hour' WHERE schedule_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	before, err := schedDB.GetSchedule(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	code, patched := doJSON(t, http.MethodPatch, srv.URL+"/scheduled-reports/"+id, "reseller", schedBody("cdrs"))
	if code != http.StatusOK {
		t.Fatalf("patch = %d %v", code, patched)
	}
	stamp, ok := patched["updated_at"].(string)
	if !ok {
		t.Fatalf("no updated_at: %v", patched)
	}
	got, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		t.Fatal(err)
	}
	if !got.After(before.UpdatedAt) {
		t.Fatalf("updated_at = %v, want after %v", got, before.UpdatedAt)
	}
	stored, err := schedDB.GetSchedule(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if stamp != stored.UpdatedAt.UTC().Format(time.RFC3339) {
		t.Fatalf("response updated_at = %q, stored = %q", stamp, stored.UpdatedAt.UTC().Format(time.RFC3339))
	}
}

func TestSchedHTTPUnavailable(t *testing.T) {
	h := &httpServer{api: &reportsServer{store: store}, auth: NewAuthenticator(fakeIdentity(t).URL)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /scheduled-reports", h.ListSchedules)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports?tenant_id=t1", "reseller", ""); code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable = %d", code)
	}
}

func TestSchedHTTPDownloadSignedLink(t *testing.T) {
	schedReset(t)
	reset(t)
	srv, signer := schedFacadeWithSigner(t)
	id := createSchedule(t, srv, "reseller", "", "cdrs")
	code, run := doJSON(t, http.MethodPost, srv.URL+"/scheduled-reports/"+id+"/run", "reseller", "")
	if code != http.StatusAccepted {
		t.Fatalf("run = %d %v", code, run)
	}
	aid := run["artifact_id"].(string)
	raw, _ := signer.Sign(id, aid, time.Now())

	req, _ := http.NewRequest(http.MethodGet, srv.URL+strings.TrimPrefix(raw, "http://reports.test"), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signed download = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}

	expired, _ := signer.Sign(id, aid, time.Now().Add(-2*time.Hour))
	req, _ = http.NewRequest(http.MethodGet, srv.URL+strings.TrimPrefix(expired, "http://reports.test"), nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expired download = %d, want 403", resp.StatusCode)
	}

	tampered := strings.Replace(raw, "sig=", "sig=x", 1)
	req, _ = http.NewRequest(http.MethodGet, srv.URL+strings.TrimPrefix(tampered, "http://reports.test"), nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("tampered download = %d, want 403", resp.StatusCode)
	}

	other := strings.Replace(raw, "/artifacts/"+aid+"/", "/artifacts/not-a-uuid/", 1)
	req, _ = http.NewRequest(http.MethodGet, srv.URL+strings.TrimPrefix(other, "http://reports.test"), nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong artifact = %d, want 404 or 403", resp.StatusCode)
	}
}
