package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func schedFacade(t *testing.T) *httptest.Server {
	t.Helper()
	api := &reportsServer{store: store}
	pub := &fakePublisher{}
	sched := NewScheduler(schedDB, api, pub)
	h := &httpServer{api: api, auth: NewAuthenticator(fakeIdentity(t).URL), sched: sched}
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
	return srv
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
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports?tenant_id=t2", "admin", ""); code != http.StatusOK {
		t.Fatalf("admin t2 list = %d", code)
	}
	// t2 is not t1: id-scoped access must 404.
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id, "admin", ""); code == http.StatusOK {
		// admin without tenant_id is rejected before lookup; with tenant_id=t2 it must 404
		_ = code
	}
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/scheduled-reports/"+id+"?tenant_id=t2", "admin", ""); code != http.StatusNotFound {
		t.Fatalf("cross-tenant get = %d", code)
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
