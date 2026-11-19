# P5 Notifications Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver one complete notification path: a scheduled report publishes `report.ready` with the creator's email and an expiring signed CSV link, `services/notifications` durably records one outbox row per artifact/recipient, and an SMTP worker sends the link through Mailpit.

**Architecture:** `services/reports` keeps ownership of schedules, artifacts, and download authorization. It adds `recipient_email` to schedules, captures it from identity `/me` at create time, and signs artifact URLs with HMAC-SHA256. `services/notifications` is a new Kafka-only Go service with a Postgres outbox. It consumes `report.ready`, inserts idempotently by `dedupe_key`, commits Kafka offsets only after the insert, then a worker claims due rows with `FOR UPDATE SKIP LOCKED` and sends plain-text email via `net/smtp`.

**Tech Stack:** Go 1.27.4, pgx v5, franz-go, protobuf, stdlib `net/smtp`, stdlib `net/http`, Postgres 17, Kafka 3.9.1, Mailpit, Docker Compose, shared `omnifoni/observability`.

## Global Constraints

- Work only in `/tmp/opencode/omni-foni-p5-notifications` on branch `p5-notifications`.
- Clean-room: never read ASTPP source, schemas, or licensed assets. Feature claims come only from `docs/master-plan.md` sections 1.1, 5, and 7.
- FOSS-only; no Terraform; no Redis.
- Money stays integer minor units; this slice does not invent or recompute money.
- Never hand-edit `services/*/gen/`; regenerate with `make protoc`.
- Commit messages are lowercase conventional commits exactly as each task specifies.
- Run `gofmt -w` on every changed Go file and `go vet ./...` in each touched module before every commit.
- Do not add code comments except the existing required `//go:embed` directives and comments already present in edited blocks.
- Tests hit real Postgres and Kafka. The dev stack must be up: `make up` (never rebuilds; use `docker compose build <service>` after code changes).
- Host tests use `127.0.0.1` DSNs and the `/etc/hosts` entry `127.0.0.1 kafka` for Kafka.
- Reports test env overrides: `REPORTS_TEST_CLICKHOUSE` and `REPORTS_TEST_SCHED_DSN`.
- Notifications test env override: `NOTIFICATIONS_TEST_DSN`.
- Signed URL format is exactly:
  `<REPORTS_PUBLIC_BASE_URL>/scheduled-reports/<schedule_id>/artifacts/<artifact_id>/download?exp=<expires_at_ms>&sig=<base64url>`
- Signature input is exactly `<schedule_id>|<artifact_id>|<expires_at_ms>`.
- Invalid or expired signed download URLs return `403` with `{"error":"forbidden"}`.
- Kafka offset commit happens only after a successful outbox insert.
- `dedupe_key` is exactly `report-ready:<artifact_id>:<recipient_email>`.
- Terminal validation failures are stored as `failed` on first sight; SMTP failures retry up to `NOTIFICATIONS_MAX_ATTEMPTS`.
- No user-facing notifications API. Only `GET /healthz` is served by notifications.

---

### Task 1: Extend the report-ready event contract

**Files:**
- Modify: `proto/scheduling/v1/scheduling.proto`
- Modify: `services/reports/gen/schedulingpb/scheduling.pb.go` (generated)
- Modify: `services/reports/sched_event_test.go`

**Interfaces:**
- Produces: `schedulingpb.ReportReadyEvent` gains `RecipientEmail string`, `ArtifactUrl string`, `ExpiresAtMs int64` with getters `GetRecipientEmail()`, `GetArtifactUrl()`, `GetExpiresAtMs()`.

- [ ] **Step 1: Extend the failing round-trip test**

Replace `TestReportReadyEventRoundTrip` in `services/reports/sched_event_test.go` with:

```go
func TestReportReadyEventRoundTrip(t *testing.T) {
	in := &schedulingpb.ReportReadyEvent{
		ScheduleId: "s1", ArtifactId: "a1", TenantId: "t1", ReportKind: "cdrs",
		PeriodFrom: "2026-11-01T00:00:00Z", PeriodTo: "2026-11-02T00:00:00Z",
		RowCount: 42, CreatedAtMs: 1763740800000,
		RecipientEmail: "ops@example.test",
		ArtifactUrl:    "http://localhost:50062/scheduled-reports/s1/artifacts/a1/download?exp=1763740800000&sig=abc",
		ExpiresAtMs:    1763740800000,
	}
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out schedulingpb.ReportReadyEvent
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetScheduleId() != "s1" || out.GetArtifactId() != "a1" || out.GetTenantId() != "t1" ||
		out.GetReportKind() != "cdrs" || out.GetRowCount() != 42 || out.GetCreatedAtMs() != 1763740800000 ||
		out.GetRecipientEmail() != "ops@example.test" || out.GetArtifactUrl() != in.ArtifactUrl ||
		out.GetExpiresAtMs() != 1763740800000 {
		t.Fatalf("round trip mismatch: %+v", &out)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/reports && go test -run TestReportReadyEventRoundTrip ./...`

Expected: FAIL — `unknown field RecipientEmail` or `GetRecipientEmail undefined`.

- [ ] **Step 3: Extend the proto**

Replace the message in `proto/scheduling/v1/scheduling.proto` with:

```proto
message ReportReadyEvent {
  string schedule_id     = 1;
  string artifact_id     = 2;
  string tenant_id       = 3;
  string report_kind     = 4;
  string period_from     = 5; // RFC3339 UTC
  string period_to       = 6; // RFC3339 UTC
  int64  row_count       = 7;
  int64  created_at_ms   = 8;
  string recipient_email = 9;
  string artifact_url    = 10;
  int64  expires_at_ms   = 11;
}
```

- [ ] **Step 4: Regenerate stubs**

Run from the repo root:

```bash
make protoc
```

Expected: exit 0; `services/reports/gen/schedulingpb/scheduling.pb.go` now defines the three new fields.

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/reports && go test -run TestReportReadyEventRoundTrip ./...`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add proto/scheduling/v1/scheduling.proto services/reports/gen/schedulingpb/scheduling.pb.go services/reports/sched_event_test.go
git commit -m "feat: extend report-ready event for notifications"
```

---

### Task 2: Add signed artifact URL generation and validation

**Files:**
- Create: `services/reports/artifact_url.go`
- Create: `services/reports/artifact_url_test.go`

**Interfaces:**
- Produces: `ArtifactSigner`, `NewArtifactSigner(secret, baseURL string, ttl time.Duration) *ArtifactSigner`, `(*ArtifactSigner).Sign(scheduleID, artifactID string, now time.Time) (string, int64)`, `(*ArtifactSigner).Valid(scheduleID, artifactID, expRaw, sig string, now time.Time) bool`, `(*ArtifactSigner).ValidDownloadURL(raw string, now time.Time) (string, string, bool)`.
- `ValidDownloadURL` returns `(scheduleID, artifactID, true)` only when the path matches the scheduled-report download route and the signature is valid.

- [ ] **Step 1: Write the failing tests**

Create `services/reports/artifact_url_test.go`:

```go
package main

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestArtifactSignerRoundTrip(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	s := NewArtifactSigner("test-secret", "http://reports.test/", time.Hour)
	raw, exp := s.Sign("s1", "a1", now)
	if exp != now.Add(time.Hour).UnixMilli() {
		t.Fatalf("exp = %d", exp)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/scheduled-reports/s1/artifacts/a1/download" {
		t.Fatalf("path = %q", u.Path)
	}
	if u.Query().Get("exp") == "" || u.Query().Get("sig") == "" {
		t.Fatalf("missing query: %s", raw)
	}
	if !s.Valid("s1", "a1", u.Query().Get("exp"), u.Query().Get("sig"), now) {
		t.Fatal("valid signature rejected")
	}
	sid, aid, ok := s.ValidDownloadURL(raw, now)
	if !ok || sid != "s1" || aid != "a1" {
		t.Fatalf("ValidDownloadURL = %q %q %v", sid, aid, ok)
	}
}

func TestArtifactSignerRejectsExpiryAndTampering(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	s := NewArtifactSigner("test-secret", "http://reports.test", time.Hour)
	raw, _ := s.Sign("s1", "a1", now)
	u, _ := url.Parse(raw)
	exp := u.Query().Get("exp")
	sig := u.Query().Get("sig")

	if s.Valid("s1", "a1", exp, sig, now.Add(2*time.Hour)) {
		t.Fatal("expired signature accepted")
	}
	if s.Valid("s1", "a2", exp, sig, now) {
		t.Fatal("wrong artifact accepted")
	}
	badSig := "A" + sig[1:]
	if badSig == sig {
		badSig = "B" + sig[1:]
	}
	if s.Valid("s1", "a1", exp, badSig, now) {
		t.Fatal("tampered signature accepted")
	}
	if s.Valid("s1", "a1", "not-a-number", sig, now) {
		t.Fatal("bad exp accepted")
	}
	if _, _, ok := s.ValidDownloadURL(strings.Replace(raw, "/artifacts/a1/", "/artifacts/a2/", 1), now); ok {
		t.Fatal("wrong artifact path accepted")
	}
	if _, _, ok := s.ValidDownloadURL("http://reports.test/other", now); ok {
		t.Fatal("wrong path accepted")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd services/reports && go test -run TestArtifactSigner ./...`

Expected: FAIL — `undefined: NewArtifactSigner`.

- [ ] **Step 3: Implement the signer**

Create `services/reports/artifact_url.go`:

```go
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type ArtifactSigner struct {
	secret  []byte
	baseURL string
	ttl     time.Duration
}

func NewArtifactSigner(secret, baseURL string, ttl time.Duration) *ArtifactSigner {
	return &ArtifactSigner{secret: []byte(secret), baseURL: strings.TrimRight(baseURL, "/"), ttl: ttl}
}

func (s *ArtifactSigner) mac(scheduleID, artifactID string, exp int64) []byte {
	h := hmac.New(sha256.New, s.secret)
	fmt.Fprintf(h, "%s|%s|%d", scheduleID, artifactID, exp)
	return h.Sum(nil)
}

func (s *ArtifactSigner) Sign(scheduleID, artifactID string, now time.Time) (string, int64) {
	exp := now.Add(s.ttl).UnixMilli()
	sig := base64.RawURLEncoding.EncodeToString(s.mac(scheduleID, artifactID, exp))
	return fmt.Sprintf("%s/scheduled-reports/%s/artifacts/%s/download?exp=%d&sig=%s",
		s.baseURL, url.PathEscape(scheduleID), url.PathEscape(artifactID), exp, sig), exp
}

func (s *ArtifactSigner) Valid(scheduleID, artifactID, expRaw, sig string, now time.Time) bool {
	exp, err := strconv.ParseInt(expRaw, 10, 64)
	if err != nil || exp <= now.UnixMilli() {
		return false
	}
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return false
	}
	return hmac.Equal(got, s.mac(scheduleID, artifactID, exp))
}

func (s *ArtifactSigner) ValidDownloadURL(raw string, now time.Time) (string, string, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 5 || parts[0] != "scheduled-reports" || parts[2] != "artifacts" || parts[4] != "download" {
		return "", "", false
	}
	scheduleID, artifactID := parts[1], parts[3]
	if !s.Valid(scheduleID, artifactID, u.Query().Get("exp"), u.Query().Get("sig"), now) {
		return "", "", false
	}
	return scheduleID, artifactID, true
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd services/reports && go test -run TestArtifactSigner ./...`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/reports/artifact_url.go services/reports/artifact_url_test.go
git commit -m "feat: sign scheduled report artifact urls"
```

---

### Task 3: Accept signed links on the artifact download route

**Files:**
- Modify: `services/reports/http.go`
- Modify: `services/reports/sched_http.go`
- Modify: `services/reports/main.go`
- Modify: `services/reports/sched_http_test.go`

**Interfaces:**
- Consumes: `ArtifactSigner.ValidDownloadURL` from Task 2.
- Produces: `httpServer.signer *ArtifactSigner`; `DownloadArtifact` accepts either `Authorization: Bearer` or valid `exp`/`sig` query parameters.

- [ ] **Step 1: Write the failing route tests**

In `services/reports/sched_http_test.go`, replace `schedFacade` with:

```go
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
```

Append these tests:

```go
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd services/reports && go test -run 'TestSchedHTTPDownloadSignedLink|TestSchedHTTPLifecycle' ./...`

Expected: FAIL — `unknown field signer in struct literal` or `NewScheduler` argument mismatch.

- [ ] **Step 3: Add the signer field and signed download path**

In `services/reports/http.go`, replace the `httpServer` struct with:

```go
type httpServer struct {
	api    API
	auth   *Authenticator
	sched  *Scheduler
	signer *ArtifactSigner
}
```

In `services/reports/sched_http.go`, replace `DownloadArtifact` with:

```go
func (h *httpServer) DownloadArtifact(w http.ResponseWriter, r *http.Request) {
	if !h.requireSched(w) {
		return
	}
	aid := r.PathValue("aid")
	if _, err := uuid.Parse(aid); err != nil {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}

	var sch Schedule
	if token, ok := bearer(r); ok {
		tenant, _, code, err := h.auth.Scope(r.Context(), token, r.URL.Query().Get("tenant_id"))
		if err != nil {
			writeErr(w, code, err.Error())
			return
		}
		var owned bool
		sch, owned = h.getOwnedSchedule(w, r, tenant)
		if !owned {
			return
		}
	} else {
		if h.signer == nil {
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		sid, signedAid, ok := h.signer.ValidDownloadURL(r.URL.String(), time.Now())
		if !ok || signedAid != aid || sid != r.PathValue("id") {
			writeErr(w, http.StatusForbidden, "forbidden")
			return
		}
		var err error
		sch, err = h.sched.store.GetSchedule(r.Context(), sid)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				writeErr(w, http.StatusNotFound, "not found")
				return
			}
			writeErr(w, http.StatusServiceUnavailable, "schedule_unavailable")
			return
		}
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
```

- [ ] **Step 4: Wire the signer through main and scheduler**

In `services/reports/main.go`, replace the `httpServer` construction with:

```go
	h := &httpServer{api: api, auth: NewAuthenticator(identity), sched: sched, signer: signer}
```

Replace the existing `var sched` / `var schedStore` declarations with:

```go
	var sched *Scheduler
	var schedStore *SchedStore
	var signer *ArtifactSigner
```

Inside the `if schedDSN != ""` block, before `NewScheduler`, add:

```go
		signedTTL := int64(86400000)
		if v := os.Getenv("REPORTS_SIGNED_URL_TTL"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 1 {
				slog.Error("bad REPORTS_SIGNED_URL_TTL", "value", v)
				os.Exit(1)
			}
			signedTTL = n
		}
		signer = NewArtifactSigner(
			env("REPORTS_ARTIFACT_SIGNING_SECRET", "dev-only"),
			env("REPORTS_PUBLIC_BASE_URL", "http://localhost:50062"),
			time.Duration(signedTTL)*time.Millisecond,
		)
```

Change the scheduler construction to:

```go
		sched = NewScheduler(schedStore, api, newKafkaReportPublisher(prod, env("REPORTS_SCHED_TOPIC", "report.ready")), signer)
```

- [ ] **Step 5: Update the scheduler constructor for Task 3 compilation**

In `services/reports/scheduler.go`, change the struct and constructor to:

```go
type Scheduler struct {
	store  *SchedStore
	api    API
	pub    ReportPublisher
	signer *ArtifactSigner
}

func NewScheduler(store *SchedStore, api API, pub ReportPublisher, signer *ArtifactSigner) *Scheduler {
	return &Scheduler{store: store, api: api, pub: pub, signer: signer}
}
```

Update every `NewScheduler(` call in `services/reports/scheduler_test.go` and `services/reports/sched_http_test.go` to pass a signer. In `scheduler_test.go`, use:

```go
	signer := NewArtifactSigner("test-secret", "http://reports.test", time.Hour)
	s := NewScheduler(schedDB, &reportsServer{store: store}, &fakePublisher{}, signer)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd services/reports && go test -run 'TestSchedHTTPDownloadSignedLink|TestSchedHTTPLifecycle|TestRunScheduleNowUsesManualSlot' ./...`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add services/reports/http.go services/reports/sched_http.go services/reports/main.go services/reports/scheduler.go services/reports/scheduler_test.go services/reports/sched_http_test.go
git commit -m "feat: serve scheduled report downloads by signed link"
```

---

### Task 4: Capture and persist the schedule recipient email

**Files:**
- Modify: `services/reports/sched_migrate.sql`
- Modify: `services/reports/sched_store.go`
- Modify: `services/reports/sched_http.go`
- Modify: `services/reports/http.go`
- Modify: `services/reports/sched_http_test.go`

**Interfaces:**
- Produces: `meResponse.Email`, `(*Authenticator).Me(ctx, token) (meResponse, int, error)`, `Schedule.RecipientEmail`, `scheduleJSON` key `recipient_email`.
- `CreateSchedule` stores the authenticated identity email; `PATCH` never changes it.

- [ ] **Step 1: Write the failing assertions**

In `TestSchedHTTPLifecycle` in `services/reports/sched_http_test.go`, after the `created_by` assertion, add:

```go
	if one["recipient_email"] != "r@x" {
		t.Fatalf("recipient_email = %v, want identity email r@x", one["recipient_email"])
	}
```

Extend the existing PATCH `else if` chain so the `body` variable stays in scope:

```go
	if code, body := doJSON(t, http.MethodPatch, srv.URL+"/scheduled-reports/"+id, "reseller", patch); code != http.StatusOK {
		t.Fatalf("patch = %d %v", code, body)
	} else if body["frequency"] != "weekly" || body["enabled"] != false {
		t.Fatalf("patch body = %v", body)
	} else if body["recipient_email"] != "r@x" {
		t.Fatalf("patch changed recipient_email: %v", body["recipient_email"])
	}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/reports && go test -run TestSchedHTTPLifecycle ./...`

Expected: FAIL — `recipient_email` is missing from the response.

- [ ] **Step 3: Add the schema column**

In `services/reports/sched_migrate.sql`, add the column to `CREATE TABLE schedules`:

```sql
  recipient_email text NOT NULL DEFAULT '',
```

After the `CREATE TABLE schedules (...)` statement, add:

```sql
ALTER TABLE schedules ADD COLUMN IF NOT EXISTS recipient_email text NOT NULL DEFAULT '';
```

- [ ] **Step 4: Persist and expose the field**

In `services/reports/sched_store.go`, add to `Schedule`:

```go
	RecipientEmail string
```

Change `scheduleColumns` to:

```go
const scheduleColumns = `schedule_id, tenant_id, account_ids, report_kind, window_kind,
	group_by, prefix_len, frequency, at_hour, at_minute, weekday, day_of_month,
	enabled, created_by, recipient_email, created_at, updated_at`
```

Change `scanSchedule` to:

```go
func scanSchedule(row pgx.Row) (Schedule, error) {
	var s Schedule
	err := row.Scan(&s.ScheduleID, &s.TenantID, &s.AccountIDs, &s.ReportKind, &s.WindowKind,
		&s.GroupBy, &s.PrefixLen, &s.Frequency, &s.AtHour, &s.AtMinute, &s.Weekday, &s.DayOfMonth,
		&s.Enabled, &s.CreatedBy, &s.RecipientEmail, &s.CreatedAt, &s.UpdatedAt)
	return s, err
}
```

Change `CreateSchedule` to:

```go
func (s *SchedStore) CreateSchedule(ctx context.Context, in Schedule) (Schedule, error) {
	return scanSchedule(s.pool.QueryRow(ctx, `INSERT INTO schedules
		(tenant_id, account_ids, report_kind, window_kind, group_by, prefix_len,
		 frequency, at_hour, at_minute, weekday, day_of_month, enabled, created_by, recipient_email)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		RETURNING `+scheduleColumns,
		in.TenantID, in.AccountIDs, in.ReportKind, in.WindowKind, in.GroupBy, in.PrefixLen,
		in.Frequency, in.AtHour, in.AtMinute, in.Weekday, in.DayOfMonth, in.Enabled, in.CreatedBy,
		in.RecipientEmail))
}
```

- [ ] **Step 5: Expose the field in JSON**

In `services/reports/sched_http.go`, add to `scheduleJSON`:

```go
		"recipient_email": s.RecipientEmail,
```

- [ ] **Step 6: Return the identity email from the authenticator**

In `services/reports/http.go`, add to `meResponse`:

```go
	Email    string `json:"email"`
```

Add after `UserID`:

```go
func (a *Authenticator) Me(ctx context.Context, token string) (meResponse, int, error) {
	return a.getMe(ctx, token)
}
```

- [ ] **Step 7: Capture the email on create**

In `services/reports/sched_http.go`, replace the `UserID` call and the `in.TenantID` / `in.CreatedBy` assignments that follow it in `CreateSchedule` with:

```go
	me, code, err := h.auth.Me(r.Context(), token)
	if err != nil {
		writeErr(w, code, err.Error())
		return
	}
	in.TenantID = tenant
	in.CreatedBy = me.UserID
	in.RecipientEmail = me.Email
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cd services/reports && go test -run TestSchedHTTPLifecycle ./...`

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add services/reports/sched_migrate.sql services/reports/sched_store.go services/reports/sched_http.go services/reports/http.go services/reports/sched_http_test.go
git commit -m "feat: store scheduled report recipients"
```

---

### Task 5: Publish recipient and signed URL in `report.ready`

**Files:**
- Modify: `services/reports/scheduler.go`
- Modify: `services/reports/scheduler_test.go`

**Interfaces:**
- Consumes: `Schedule.RecipientEmail`, `ArtifactSigner.Sign`.
- Produces: `ReportReadyEvent` populated with `RecipientEmail`, `ArtifactUrl`, `ExpiresAtMs`.

- [ ] **Step 1: Write the failing publication test**

Append to `services/reports/scheduler_test.go`:

```go
func TestRunOnePublishesRecipientAndSignedURL(t *testing.T) {
	schedReset(t)
	reset(t)
	ctx := context.Background()
	sch, err := schedDB.CreateSchedule(ctx, Schedule{
		TenantID: "t1", AccountIDs: []string{"a1"}, ReportKind: "cdrs",
		WindowKind: "last_day", GroupBy: "none", PrefixLen: 3,
		Frequency: "daily", AtHour: 0, AtMinute: 0, Enabled: true,
		CreatedBy: "tester", RecipientEmail: "ops@example.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	seed(t, testRow("r1", "a1", "27820001", "trunk-1", "normal", time.Now().UTC().Add(-time.Minute), 60, 10))
	pub := &fakePublisher{}
	signer := NewArtifactSigner("test-secret", "http://reports.test", time.Hour)
	s := NewScheduler(schedDB, &reportsServer{store: store}, pub, signer)
	a, err := s.RunScheduleNow(ctx, sch)
	if err != nil {
		t.Fatal(err)
	}
	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1", len(pub.events))
	}
	ev := pub.events[0]
	if ev.GetRecipientEmail() != "ops@example.test" {
		t.Fatalf("recipient = %q", ev.GetRecipientEmail())
	}
	if ev.GetExpiresAtMs() == 0 {
		t.Fatal("missing expiry")
	}
	sid, aid, ok := signer.ValidDownloadURL(ev.GetArtifactUrl(), time.Now())
	if !ok || sid != sch.ScheduleID || aid != a.ArtifactID {
		t.Fatalf("artifact url = %q", ev.GetArtifactUrl())
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/reports && go test -run TestRunOnePublishesRecipientAndSignedURL ./...`

Expected: FAIL — event has empty recipient and URL.

- [ ] **Step 3: Populate the event**

In `services/reports/scheduler.go`, replace the `if s.pub != nil` block inside `runOne` with:

```go
	if s.pub != nil {
		ev := &schedulingpb.ReportReadyEvent{
			ScheduleId: a.ScheduleID, ArtifactId: a.ArtifactID, TenantId: sch.TenantID,
			ReportKind: sch.ReportKind,
			PeriodFrom: a.PeriodFrom.UTC().Format(time.RFC3339),
			PeriodTo:   a.PeriodTo.UTC().Format(time.RFC3339),
			RowCount:   a.RowCount, CreatedAtMs: a.CreatedAt.UnixMilli(),
			RecipientEmail: sch.RecipientEmail,
		}
		if s.signer != nil {
			ev.ArtifactUrl, ev.ExpiresAtMs = s.signer.Sign(a.ScheduleID, a.ArtifactID, time.Now())
		}
		if err := s.pub.PublishReportReady(ctx, ev); err != nil {
			slog.ErrorContext(ctx, "report.ready publish", "schedule", sch.ScheduleID, "err", err)
		}
	}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd services/reports && go test -run 'TestRunOnePublishesRecipientAndSignedURL|TestRunScheduleNowUsesManualSlot|TestRunDueSchedules' ./...`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add services/reports/scheduler.go services/reports/scheduler_test.go
git commit -m "feat: publish notification details in report-ready"
```

---

### Task 6: Wire reports configuration and compose

**Files:**
- Modify: `docker-compose.yml`
- Modify: `services/reports/main.go` (only if Task 3 did not already add the env parsing)

**Interfaces:**
- Produces: reports container env `REPORTS_ARTIFACT_SIGNING_SECRET`, `REPORTS_SIGNED_URL_TTL`, `REPORTS_PUBLIC_BASE_URL`.

- [ ] **Step 1: Add reports env to compose**

In `docker-compose.yml`, add under `reports.environment`:

```yaml
      REPORTS_ARTIFACT_SIGNING_SECRET: dev-only
      REPORTS_SIGNED_URL_TTL: "86400000"
      REPORTS_PUBLIC_BASE_URL: http://localhost:50062
```

- [ ] **Step 2: Verify the reports module still builds**

Run: `cd services/reports && go vet ./... && go test -run TestArtifactSigner ./...`

Expected: PASS.

- [ ] **Step 3: Verify compose syntax**

Run: `docker compose config -q`

Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml services/reports/main.go
git commit -m "chore: configure signed report links"
```

---

### Task 7: Add the notifications module and outbox store

**Files:**
- Create: `services/notifications/go.mod`
- Create: `services/notifications/migrate.sql`
- Create: `services/notifications/outbox.go`
- Create: `services/notifications/outbox_test.go`

**Interfaces:**
- Produces: `Notification`, `Store`, `Open`, `(*Store).Migrate`, `(*Store).Enqueue`, `(*Store).ClaimDue`, `(*Store).MarkSent`, `(*Store).MarkRetry`, `(*Store).MarkTerminal`.
- `Enqueue` returns `(Notification, bool, error)` where `bool` is false for a duplicate dedupe key and the existing row is untouched.

- [ ] **Step 1: Create the module and failing store test**

Create `services/notifications/go.mod`:

```go
module omnifoni/notifications

go 1.27.4

replace omnifoni/observability => ../observability
```

Create `services/notifications/outbox_test.go`:

```go
package main

import (
	"context"
	"os"
	"testing"
	"time"
)

var testStore *Store

func testDSN() string {
	if s := os.Getenv("NOTIFICATIONS_TEST_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/notifications"
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	testStore, err = Open(ctx, testDSN())
	if err != nil {
		panic(err)
	}
	if err := testStore.Migrate(ctx); err != nil {
		panic(err)
	}
	code := m.Run()
	testStore.Close()
	os.Exit(code)
}

func resetOutbox(t *testing.T) {
	t.Helper()
	if _, err := testStore.pool.Exec(context.Background(), "TRUNCATE TABLE outbox"); err != nil {
		t.Fatal(err)
	}
}

func testNotification(key string) Notification {
	return Notification{
		DedupeKey: key, Channel: "email", Recipient: "ops@example.test",
		Subject: "report ready", Body: "body", Status: "pending",
		NextAttemptAt: time.Now().UTC(),
	}
}

func TestEnqueueDedupes(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, inserted, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil || !inserted || n.NotificationID == "" {
		t.Fatalf("first enqueue = %+v %v %v", n, inserted, err)
	}
	dup, inserted, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil || inserted || dup.NotificationID != "" {
		t.Fatalf("duplicate = %+v %v %v", dup, inserted, err)
	}
}

func TestClaimAndTransitions(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, _, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil {
		t.Fatal(err)
	}
	due, err := testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 1 || due[0].NotificationID != n.NotificationID {
		t.Fatalf("claim = %+v %v", due, err)
	}
	if due[0].Attempts != 1 {
		t.Fatalf("attempts = %d", due[0].Attempts)
	}
	if err := testStore.MarkRetry(ctx, n.NotificationID, "smtp down", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := testStore.MarkSent(ctx, n.NotificationID); err != nil {
		t.Fatal(err)
	}
	due, err = testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("sent row reclaimed = %+v %v", due, err)
	}
}

func TestMarkTerminal(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, _, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := testStore.MarkTerminal(ctx, n.NotificationID, "missing recipient"); err != nil {
		t.Fatal(err)
	}
	due, err := testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("failed row reclaimed = %+v %v", due, err)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd services/notifications && go test ./...`

Expected: FAIL — `undefined: Store` / missing package dependencies.

- [ ] **Step 3: Create the outbox schema**

Create `services/notifications/migrate.sql`:

```sql
CREATE TABLE IF NOT EXISTS outbox (
  notification_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  dedupe_key text NOT NULL UNIQUE,
  channel text NOT NULL CHECK (channel = 'email'),
  recipient text NOT NULL,
  subject text NOT NULL,
  body text NOT NULL,
  status text NOT NULL CHECK (status IN ('pending','sent','failed')),
  attempts integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_error text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now(),
  sent_at timestamptz
);

CREATE INDEX IF NOT EXISTS outbox_due_idx ON outbox (next_attempt_at) WHERE status = 'pending';
```

- [ ] **Step 4: Implement the store**

Create `services/notifications/outbox.go`:

```go
package main

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrate.sql
var schemaSQL string

type Notification struct {
	NotificationID string
	DedupeKey      string
	Channel        string
	Recipient      string
	Subject        string
	Body           string
	Status         string
	Attempts       int
	NextAttemptAt  time.Time
	LastError      string
	CreatedAt      time.Time
	SentAt         *time.Time
}

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

const notificationColumns = `notification_id, dedupe_key, channel, recipient, subject,
	body, status, attempts, next_attempt_at, last_error, created_at, sent_at`

const notificationColumnsQualified = `o.notification_id, o.dedupe_key, o.channel, o.recipient, o.subject,
	o.body, o.status, o.attempts, o.next_attempt_at, o.last_error, o.created_at, o.sent_at`

func scanNotification(row pgx.Row) (Notification, error) {
	var n Notification
	err := row.Scan(&n.NotificationID, &n.DedupeKey, &n.Channel, &n.Recipient, &n.Subject,
		&n.Body, &n.Status, &n.Attempts, &n.NextAttemptAt, &n.LastError, &n.CreatedAt, &n.SentAt)
	return n, err
}

func (s *Store) Enqueue(ctx context.Context, n Notification) (Notification, bool, error) {
	if n.Status == "" {
		n.Status = "pending"
	}
	if n.NextAttemptAt.IsZero() {
		n.NextAttemptAt = time.Now().UTC()
	}
	out, err := scanNotification(s.pool.QueryRow(ctx, `INSERT INTO outbox
		(dedupe_key, channel, recipient, subject, body, status, attempts, next_attempt_at, last_error)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (dedupe_key) DO NOTHING
		RETURNING `+notificationColumns,
		n.DedupeKey, n.Channel, n.Recipient, n.Subject, n.Body, n.Status, n.Attempts,
		n.NextAttemptAt, n.LastError))
	if errors.Is(err, pgx.ErrNoRows) {
		return Notification{}, false, nil
	}
	if err != nil {
		return Notification{}, false, err
	}
	return out, true, nil
}

func (s *Store) ClaimDue(ctx context.Context, limit int) ([]Notification, error) {
	rows, err := s.pool.Query(ctx, `WITH due AS (
		SELECT notification_id FROM outbox
		WHERE status = 'pending' AND next_attempt_at <= now()
		ORDER BY next_attempt_at, created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE outbox o SET attempts = o.attempts + 1, next_attempt_at = now()
	FROM due WHERE o.notification_id = due.notification_id
	RETURNING `+notificationColumnsQualified, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *Store) MarkSent(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox SET status = 'sent', sent_at = now(), last_error = '' WHERE notification_id = $1`, id)
	return err
}

func (s *Store) MarkRetry(ctx context.Context, id, lastError string, next time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox SET status = 'pending', last_error = $2, next_attempt_at = $3 WHERE notification_id = $1`,
		id, lastError, next)
	return err
}

func (s *Store) MarkTerminal(ctx context.Context, id, lastError string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbox SET status = 'failed', last_error = $2, next_attempt_at = now() WHERE notification_id = $1`,
		id, lastError)
	return err
}
```

- [ ] **Step 5: Tidy and run the tests**

Run:

```bash
cd services/notifications
go mod tidy
go test ./...
```

Expected: PASS. `go mod tidy` adds `pgx/v5` to `go.mod` and creates `go.sum`.

- [ ] **Step 6: Commit**

```bash
git add services/notifications
git commit -m "feat: add notifications outbox store"
```

---

### Task 8: Map report-ready events and consume Kafka

**Files:**
- Create: `services/notifications/events.go`
- Create: `services/notifications/events_test.go`
- Create: `services/notifications/consumer.go`
- Create: `services/notifications/consumer_test.go`
- Create: `services/notifications/ingester.go`
- Modify: `Makefile`
- Create: `services/notifications/gen/schedulingpb/*.go` (generated)

**Interfaces:**
- Produces: `notificationFromEvent(ev *schedulingpb.ReportReadyEvent, now time.Time) Notification`, `dedupeKey(artifactID, recipient string) string`, `retryDelay(attempt int, base time.Duration) time.Duration`, `Handler`, `Run(ctx, cl kafkaClient, h Handler, retry time.Duration)`.
- `kafkaClient` is the minimal `PollRecords`/`CommitRecords` interface used by tests.

- [ ] **Step 1: Add notifications protobuf generation**

In `Makefile`, after the existing reports scheduling generation block, add:

```make
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/notifications --go_opt=module=omnifoni/notifications \
		--go_opt=Mscheduling/v1/scheduling.proto=omnifoni/notifications/gen/schedulingpb \
		proto/scheduling/v1/scheduling.proto
```

Run from the repo root:

```bash
make protoc
```

Expected: exit 0; `services/notifications/gen/schedulingpb/scheduling.pb.go` exists.

- [ ] **Step 2: Write the failing event tests**

Create `services/notifications/events_test.go`:

```go
package main

import (
	"strconv"
	"strings"
	"testing"
	"time"

	schedulingpb "omnifoni/notifications/gen/schedulingpb"
)

func validEvent(now time.Time) *schedulingpb.ReportReadyEvent {
	exp := now.Add(time.Hour).UnixMilli()
	return &schedulingpb.ReportReadyEvent{
		ScheduleId: "s1", ArtifactId: "a1", TenantId: "t1", ReportKind: "cdrs",
		PeriodFrom: "2026-11-01T00:00:00Z", PeriodTo: "2026-11-02T00:00:00Z",
		RowCount: 7, CreatedAtMs: now.UnixMilli(),
		RecipientEmail: "ops@example.test",
		ArtifactUrl:    "http://localhost:50062/scheduled-reports/s1/artifacts/a1/download?exp=" + strconv.FormatInt(exp, 10) + "&sig=abc",
		ExpiresAtMs:    exp,
	}
}

func TestNotificationFromEvent(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	ev := validEvent(now)
	n := notificationFromEvent(ev, now)
	if n.DedupeKey != "report-ready:a1:ops@example.test" {
		t.Fatalf("dedupe = %q", n.DedupeKey)
	}
	if n.Channel != "email" || n.Recipient != "ops@example.test" || n.Status != "pending" {
		t.Fatalf("notification = %+v", n)
	}
	if !strings.Contains(n.Body, ev.GetArtifactUrl()) {
		t.Fatalf("body missing url: %q", n.Body)
	}
}

func TestNotificationFromEventTerminalValidation(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		edit func(*schedulingpb.ReportReadyEvent)
		want string
	}{
		{"missing recipient", func(e *schedulingpb.ReportReadyEvent) { e.RecipientEmail = "" }, "missing recipient_email"},
		{"malformed url", func(e *schedulingpb.ReportReadyEvent) { e.ArtifactUrl = "not a url" }, "malformed artifact_url"},
		{"missing signature", func(e *schedulingpb.ReportReadyEvent) { e.ArtifactUrl = "http://localhost:50062/scheduled-reports/s1/artifacts/a1/download?exp=1" }, "missing artifact_url signature"},
		{"expiry mismatch", func(e *schedulingpb.ReportReadyEvent) { e.ExpiresAtMs++ }, "artifact_url expiry mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := validEvent(now)
			tc.edit(ev)
			n := notificationFromEvent(ev, now)
			if n.Status != "failed" || n.Attempts != 1 || !strings.Contains(n.LastError, tc.want) {
				t.Fatalf("notification = %+v", n)
			}
		})
	}
}

func TestRetryDelay(t *testing.T) {
	if got := retryDelay(1, time.Minute); got != time.Minute {
		t.Fatalf("attempt 1 = %s", got)
	}
	if got := retryDelay(3, time.Minute); got != 4*time.Minute {
		t.Fatalf("attempt 3 = %s", got)
	}
	if got := retryDelay(20, time.Minute); got != 32*time.Minute {
		t.Fatalf("attempt 20 = %s", got)
	}
}
```

- [ ] **Step 3: Run the event tests to verify they fail**

Run: `cd services/notifications && go test -run 'TestNotificationFromEvent|TestRetryDelay' ./...`

Expected: FAIL — `undefined: notificationFromEvent`.

- [ ] **Step 4: Implement event mapping**

Create `services/notifications/events.go`:

```go
package main

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	schedulingpb "omnifoni/notifications/gen/schedulingpb"
)

func dedupeKey(artifactID, recipient string) string {
	return "report-ready:" + artifactID + ":" + recipient
}

func retryDelay(attempt int, base time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 5 {
		shift = 5
	}
	return base * time.Duration(1<<shift)
}

func validateReportReady(ev *schedulingpb.ReportReadyEvent, now time.Time) string {
	if strings.TrimSpace(ev.GetRecipientEmail()) == "" {
		return "missing recipient_email"
	}
	raw := strings.TrimSpace(ev.GetArtifactUrl())
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "malformed artifact_url"
	}
	if u.Query().Get("sig") == "" {
		return "missing artifact_url signature"
	}
	if u.Query().Get("exp") != strconv.FormatInt(ev.GetExpiresAtMs(), 10) {
		return "artifact_url expiry mismatch"
	}
	if ev.GetExpiresAtMs() <= now.UnixMilli() {
		return "expired artifact_url"
	}
	return ""
}

func notificationFromEvent(ev *schedulingpb.ReportReadyEvent, now time.Time) Notification {
	subject := "Omni-Foní scheduled report ready"
	body := fmt.Sprintf("Your %s report is ready.\n\nPeriod: %s to %s\nRows: %d\n\nDownload: %s\n\nThis link expires at %s.\n",
		ev.GetReportKind(), ev.GetPeriodFrom(), ev.GetPeriodTo(), ev.GetRowCount(),
		ev.GetArtifactUrl(), time.UnixMilli(ev.GetExpiresAtMs()).UTC().Format(time.RFC3339))
	n := Notification{
		DedupeKey: dedupeKey(ev.GetArtifactId(), ev.GetRecipientEmail()),
		Channel:   "email", Recipient: ev.GetRecipientEmail(), Subject: subject, Body: body,
		Status: "pending", NextAttemptAt: now,
	}
	if reason := validateReportReady(ev, now); reason != "" {
		n.Status = "failed"
		n.Attempts = 1
		n.LastError = "terminal: " + reason
	}
	return n
}
```

- [ ] **Step 5: Run the event tests to verify they pass**

Run: `cd services/notifications && go test -run 'TestNotificationFromEvent|TestRetryDelay' ./...`

Expected: PASS.

- [ ] **Step 6: Write the failing consumer tests**

Create `services/notifications/consumer_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	schedulingpb "omnifoni/notifications/gen/schedulingpb"
)

type fakeKafka struct {
	fetches  kgo.Fetches
	polls    int
	cancel   context.CancelFunc
	commits  int
	commitErr error
}

func (f *fakeKafka) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	f.polls++
	if f.polls > 1 {
		if f.cancel != nil {
			f.cancel()
		}
		return nil
	}
	return f.fetches
}

func (f *fakeKafka) CommitRecords(ctx context.Context, _ ...*kgo.Record) error {
	f.commits++
	return f.commitErr
}

type fakeHandler struct {
	calls      int
	failFirst  bool
	onHandle   func()
}

func (h *fakeHandler) Handle(ctx context.Context, _ *kgo.Record) error {
	h.calls++
	if h.onHandle != nil {
		h.onHandle()
	}
	if h.failFirst && h.calls == 1 {
		return errors.New("boom")
	}
	return nil
}

func oneRecord(t *testing.T) *kgo.Record {
	t.Helper()
	b, err := proto.Marshal(validEvent(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "report.ready", Partition: 0, Value: b}
}

func TestRunCommitsOnlyAfterHandle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := oneRecord(t)
	f := &fakeKafka{
		fetches: kgo.Fetches{{
			Topics: []kgo.FetchTopic{{
				Topic: "report.ready",
				Partitions: []kgo.FetchPartition{{
					Partition: 0, Records: []*kgo.Record{rec},
				}},
			}},
		}},
		cancel: cancel,
	}
	h := &fakeHandler{}
	h.onHandle = func() {
		if f.commits != 0 {
			t.Fatalf("commit before handle: %d", f.commits)
		}
	}
	Run(ctx, f, h, time.Millisecond)
	if h.calls != 1 || f.commits != 1 {
		t.Fatalf("calls=%d commits=%d", h.calls, f.commits)
	}
}

func TestRunRetriesHandleFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &fakeKafka{
		fetches: kgo.Fetches{{
			Topics: []kgo.FetchTopic{{
				Topic: "report.ready",
				Partitions: []kgo.FetchPartition{{
					Partition: 0, Records: []*kgo.Record{oneRecord(t)},
				}},
			}},
		}},
		cancel: cancel,
	}
	h := &fakeHandler{failFirst: true}
	Run(ctx, f, h, time.Millisecond)
	if h.calls != 2 || f.commits != 1 {
		t.Fatalf("calls=%d commits=%d", h.calls, f.commits)
	}
}
```

- [ ] **Step 7: Run the consumer tests to verify they fail**

Run: `cd services/notifications && go test -run 'TestRunCommitsOnlyAfterHandle|TestRunRetriesHandleFailure' ./...`

Expected: FAIL — `undefined: Run`.

- [ ] **Step 8: Implement the consumer**

Create `services/notifications/consumer.go`:

```go
package main

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
)

type Handler interface {
	Handle(context.Context, *kgo.Record) error
}

type kafkaClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
}

func Run(ctx context.Context, cl kafkaClient, h Handler, retry time.Duration) {
	for {
		fetches := cl.PollRecords(ctx, 100)
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.ErrorContext(ctx, "kafka fetch", "topic", topic, "partition", partition, "err", err)
		})
		lasts := map[string]*kgo.Record{}
		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()
			for {
				err := h.Handle(ctx, rec)
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return
				}
				slog.ErrorContext(ctx, "notification handle failed, retrying", "err", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(retry):
				}
			}
			lasts[rec.Topic+"/"+strconv.Itoa(int(rec.Partition))] = rec
		}
		if ctx.Err() != nil {
			return
		}
		toCommit := make([]*kgo.Record, 0, len(lasts))
		for _, rec := range lasts {
			toCommit = append(toCommit, rec)
		}
		if len(toCommit) > 0 {
			if err := cl.CommitRecords(ctx, toCommit...); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "notification offset commit failed", "err", err)
			}
		}
	}
}
```

- [ ] **Step 9: Implement the event ingester**

Create `services/notifications/ingester.go`:

```go
package main

import (
	"context"
	"log/slog"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	schedulingpb "omnifoni/notifications/gen/schedulingpb"
	"omnifoni/observability"
)

type Ingester struct {
	store *Store
}

func NewIngester(store *Store) *Ingester {
	return &Ingester{store: store}
}

func (i *Ingester) Handle(ctx context.Context, rec *kgo.Record) error {
	var ev schedulingpb.ReportReadyEvent
	if err := proto.Unmarshal(rec.Value, &ev); err != nil {
		slog.ErrorContext(ctx, "report.ready decode failed", "err", err)
		return nil
	}
	ctx = observability.ExtractKafka(ctx, rec)
	ctx, span := observability.ConsumerSpan(ctx, "notifications.consume_report_ready", ev.GetArtifactId())
	defer span.End()
	n := notificationFromEvent(&ev, time.Now().UTC())
	if _, _, err := i.store.Enqueue(ctx, n); err != nil {
		span.RecordError(err)
		return err
	}
	if n.Status == "failed" {
		slog.WarnContext(ctx, "report.ready terminal validation failure",
			"artifact_id", ev.GetArtifactId(), "recipient", ev.GetRecipientEmail(), "reason", n.LastError)
	}
	return nil
}
```

Malformed protobuf has no event identity to key an outbox row, so log and commit it. Decoded events with a missing recipient, missing/invalid URL, or expired URL are stored as `failed`.

- [ ] **Step 10: Run all notifications tests**

Run:

```bash
cd services/notifications
go mod tidy
go test ./...
```

Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add Makefile services/notifications
git commit -m "feat: consume report-ready events into outbox"
```

---

### Task 9: Send email through SMTP and run the service

**Files:**
- Create: `services/notifications/mailer.go`
- Create: `services/notifications/mailer_test.go`
- Create: `services/notifications/worker.go`
- Create: `services/notifications/worker_test.go`
- Create: `services/notifications/main.go`
- Create: `services/notifications/Dockerfile`
- Modify: `services/notifications/go.mod` (via `go mod tidy`)

**Interfaces:**
- Produces: `smtpMessage(from, to, subject, body string) []byte`, `SMTPSender`, `NewSMTPSender(addr, from string, timeout time.Duration) *SMTPSender`, `Sender`, `Worker`, `NewWorker(store *Store, sender Sender, interval time.Duration, batch, maxAttempts int, backoff time.Duration) *Worker`, `(*Worker).Run`, `(*Worker).RunOnce`.

- [ ] **Step 1: Write the failing mailer test**

Create `services/notifications/mailer_test.go`:

```go
package main

import (
	"strings"
	"testing"
)

func TestSMTPMessage(t *testing.T) {
	b := string(smtpMessage("from@example.test", "to@example.test", "hello", "body line\nsecond"))
	for _, want := range []string{
		"From: from@example.test\r\n",
		"To: to@example.test\r\n",
		"Subject: hello\r\n",
		"MIME-Version: 1.0\r\n",
		"Content-Type: text/plain; charset=UTF-8\r\n",
		"\r\nbody line\nsecond\r\n",
	} {
		if !strings.Contains(b, want) {
			t.Fatalf("message missing %q: %q", want, b)
		}
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/notifications && go test -run TestSMTPMessage ./...`

Expected: FAIL — `undefined: smtpMessage`.

- [ ] **Step 3: Implement the mailer**

Create `services/notifications/mailer.go`:

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/smtp"
	"time"
)

type SMTPSender struct {
	addr    string
	from    string
	timeout time.Duration
}

func NewSMTPSender(addr, from string, timeout time.Duration) *SMTPSender {
	return &SMTPSender{addr: addr, from: from, timeout: timeout}
}

func smtpMessage(from, to, subject, body string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", to)
	fmt.Fprintf(&b, "Subject: %s\r\n", subject)
	fmt.Fprint(&b, "MIME-Version: 1.0\r\n")
	fmt.Fprint(&b, "Content-Type: text/plain; charset=UTF-8\r\n")
	fmt.Fprint(&b, "\r\n")
	fmt.Fprintf(&b, "%s\r\n", body)
	return b.Bytes()
}

func (s *SMTPSender) Send(ctx context.Context, n Notification) error {
	host, _, err := net.SplitHostPort(s.addr)
	if err != nil {
		return err
	}
	d := net.Dialer{Timeout: s.timeout}
	conn, err := d.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if s.timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(s.timeout))
	}
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Mail(s.from); err != nil {
		return err
	}
	if err := c.Rcpt(n.Recipient); err != nil {
		return err
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(smtpMessage(s.from, n.Recipient, n.Subject, n.Body)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}
```

- [ ] **Step 4: Run the mailer test to verify it passes**

Run: `cd services/notifications && go test -run TestSMTPMessage ./...`

Expected: PASS.

- [ ] **Step 5: Write the failing worker tests**

Create `services/notifications/worker_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeSender struct {
	errs []error
	sent []Notification
}

func (f *fakeSender) Send(ctx context.Context, n Notification) error {
	f.sent = append(f.sent, n)
	if len(f.errs) == 0 {
		return nil
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	return err
}

func TestWorkerMarksSent(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, _, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{}
	w := NewWorker(testStore, sender, time.Millisecond, 10, 5, time.Minute)
	if err := w.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 1 || sender.sent[0].NotificationID != n.NotificationID {
		t.Fatalf("sent = %+v", sender.sent)
	}
	due, err := testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("reclaimed = %+v %v", due, err)
	}
}

func TestWorkerRetriesThenFails(t *testing.T) {
	resetOutbox(t)
	ctx := context.Background()
	n, _, err := testStore.Enqueue(ctx, testNotification("k1"))
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{errs: []error{errors.New("smtp down")}}
	w := NewWorker(testStore, sender, time.Millisecond, 10, 1, time.Minute)
	if err := w.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	due, err := testStore.ClaimDue(ctx, 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("failed row reclaimed = %+v %v", due, err)
	}
	if sender.sent[0].NotificationID != n.NotificationID {
		t.Fatalf("sent = %+v", sender.sent)
	}
}
```

- [ ] **Step 6: Run the worker tests to verify they fail**

Run: `cd services/notifications && go test -run 'TestWorkerMarksSent|TestWorkerRetriesThenFails' ./...`

Expected: FAIL — `undefined: NewWorker`.

- [ ] **Step 7: Implement the worker**

Create `services/notifications/worker.go`:

```go
package main

import (
	"context"
	"log/slog"
	"time"
)

type Sender interface {
	Send(context.Context, Notification) error
}

type Worker struct {
	store       *Store
	sender      Sender
	interval    time.Duration
	batch       int
	maxAttempts int
	backoff     time.Duration
}

func NewWorker(store *Store, sender Sender, interval time.Duration, batch, maxAttempts int, backoff time.Duration) *Worker {
	return &Worker{store: store, sender: sender, interval: interval, batch: batch, maxAttempts: maxAttempts, backoff: backoff}
}

func (w *Worker) Run(ctx context.Context) {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
				slog.ErrorContext(ctx, "notification worker", "err", err)
			}
		}
	}
}

func (w *Worker) RunOnce(ctx context.Context) error {
	items, err := w.store.ClaimDue(ctx, w.batch)
	if err != nil {
		return err
	}
	for _, n := range items {
		if err := w.sender.Send(ctx, n); err == nil {
			if err := w.store.MarkSent(ctx, n.NotificationID); err != nil {
				return err
			}
			continue
		} else {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			last := truncate(err.Error(), 1000)
			if n.Attempts >= w.maxAttempts {
				if err := w.store.MarkTerminal(ctx, n.NotificationID, last); err != nil {
					return err
				}
				continue
			}
			if err := w.store.MarkRetry(ctx, n.NotificationID, last,
				time.Now().Add(retryDelay(n.Attempts, w.backoff))); err != nil {
				return err
			}
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
```

- [ ] **Step 8: Run the worker tests to verify they pass**

Run: `cd services/notifications && go test -run 'TestWorkerMarksSent|TestWorkerRetriesThenFails' ./...`

Expected: PASS.

- [ ] **Step 9: Implement the service entrypoint**

Create `services/notifications/main.go`:

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"

	"omnifoni/observability"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		slog.Error("bad "+key, "value", v)
		os.Exit(1)
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 1 {
		slog.Error("bad "+key, "value", v)
		os.Exit(1)
	}
	return d
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	observability.SetupLogging("notifications")
	shutdown, err := observability.Init(ctx, "notifications")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	store, err := Open(ctx, env("NOTIFICATIONS_DSN", "postgres://postgres:dev-only@postgres:5432/notifications"))
	if err != nil {
		slog.Error("notifications db", "err", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		slog.Error("notifications migrate", "err", err)
		os.Exit(1)
	}

	brokers := strings.Split(env("NOTIFICATIONS_KAFKA", "kafka:9092"), ",")
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(env("NOTIFICATIONS_GROUP", "notifications")),
		kgo.ConsumeTopics(env("NOTIFICATIONS_TOPIC_REPORT_READY", "report.ready")),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		slog.Error("kafka consumer", "err", err)
		os.Exit(1)
	}
	defer cl.Close()
	go Run(ctx, cl, NewIngester(store), time.Second)

	sender := NewSMTPSender(
		env("NOTIFICATIONS_SMTP_ADDR", "mailpit:1025"),
		env("NOTIFICATIONS_SMTP_FROM", "omnifoni@example.test"),
		10*time.Second,
	)
	worker := NewWorker(store, sender,
		envDuration("NOTIFICATIONS_POLL_INTERVAL", time.Second),
		envInt("NOTIFICATIONS_BATCH_SIZE", 32),
		envInt("NOTIFICATIONS_MAX_ATTEMPTS", 5),
		30*time.Second,
	)
	go worker.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Addr:              env("NOTIFICATIONS_ADDR", ":50064"),
		Handler:           observability.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("notifications up", "addr", srv.Addr, "brokers", brokers,
		"smtp", env("NOTIFICATIONS_SMTP_ADDR", "mailpit:1025"))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http serve failed", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 10: Create the Dockerfile**

Create `services/notifications/Dockerfile`:

```dockerfile
FROM golang:1.27 AS build
WORKDIR /src
COPY observability/ /src/observability/
COPY notifications/ /src/notifications/
WORKDIR /src/notifications
RUN CGO_ENABLED=0 go build -trimpath -o /out/notifications .

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/notifications /app/notifications
COPY --from=build /src/notifications/migrate.sql /app/migrate.sql
USER nonroot:nonroot
ENTRYPOINT ["/app/notifications"]
```

- [ ] **Step 11: Run all notifications tests and vet**

Run:

```bash
cd services/notifications
go mod tidy
gofmt -w .
go vet ./...
go test ./...
```

Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add services/notifications
git commit -m "feat: send notification emails through smtp"
```

---

### Task 10: Add Mailpit, notifications compose wiring, database init, and smoke assertions

**Files:**
- Modify: `docker-compose.yml`
- Modify: `ops/postgres-init.sql`
- Modify: `Makefile`
- Modify: `ops/smoke.sh`
- Modify: `AGENTS.md`

**Interfaces:**
- Produces: compose services `mailpit` and `notifications`; `notifications` DB; `make test` covers notifications; smoke asserts one Mailpit email and follows its signed link to CSV.

- [ ] **Step 1: Add the notifications database**

In `ops/postgres-init.sql`, append:

```sql
CREATE DATABASE notifications;
```

- [ ] **Step 2: Add compose services**

In `docker-compose.yml`, add before `reports`:

```yaml
  mailpit:
    image: axllent/mailpit:v1.21
    ports:
      - "1025:1025"
      - "8025:8025"

  notifications:
    build:
      context: ./services
      dockerfile: notifications/Dockerfile
    environment:
      NOTIFICATIONS_DSN: postgres://postgres:dev-only@postgres:5432/notifications
      NOTIFICATIONS_KAFKA: kafka:9092
      NOTIFICATIONS_TOPIC_REPORT_READY: report.ready
      NOTIFICATIONS_GROUP: notifications
      NOTIFICATIONS_SMTP_ADDR: mailpit:1025
      NOTIFICATIONS_SMTP_FROM: omnifoni@example.test
      NOTIFICATIONS_MAX_ATTEMPTS: "5"
      NOTIFICATIONS_POLL_INTERVAL: 1s
      NOTIFICATIONS_BATCH_SIZE: "32"
      NOTIFICATIONS_ADDR: ":50064"
      OTEL_EXPORTER_OTLP_ENDPOINT: jaeger:4317
      OBS_SAMPLE_RATIO: "1.0"
    ports:
      - "50064:50064"
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
      mailpit:
        condition: service_started
```

- [ ] **Step 3: Add notifications to `make test`**

In `Makefile`, add after the reports test line:

```make
	cd services/notifications && go vet ./... && go test ./...
```

- [ ] **Step 4: Update smoke database checks**

In `ops/smoke.sh`, replace the database count block with:

```bash
docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT 1 FROM pg_database WHERE datname='notifications'" | grep -q 1 || docker exec omni-fon-postgres-1 createdb -U postgres notifications

dbs=$(docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT count(*) FROM pg_database WHERE datistemplate=false AND datname IN ('identity','catalogue','charging','billing','payments','routing','sbc','fraud','mediation','reports_sched','notifications')")
[ "$dbs" = "11" ] || fail "expected 11 context databases, got $dbs"
```

- [ ] **Step 5: Update smoke service and trace lists**

Replace the service loop line with:

```bash
for s in charging switching mediation billing payments identity routing sbc fraud reconciliation reports notifications mailpit kamailio sipedge sipuas; do
```

Replace the Jaeger loop line with:

```bash
for s in reconciliation billing payments reports notifications fraud sipedge; do
```

- [ ] **Step 6: Add Mailpit reset and assertions**

Before the scheduled report run in `ops/smoke.sh`, add:

```bash
curl -fsS -X DELETE http://localhost:8025/api/v1/messages >/dev/null || fail "mailpit reset"
```

After `echo "ok: scheduled report"`, add:

```bash
mid=""
for _ in $(seq 1 30); do
  mid=$(curl -fsS http://localhost:8025/api/v1/messages | grep -o '"ID":"[^"]*"' | head -n1 | cut -d'"' -f4 || true)
  [ -n "$mid" ] && break
  sleep 1
done
[ -n "$mid" ] || fail "notification email missing"
msg=$(curl -fsS "http://localhost:8025/api/v1/message/$mid")
msg=$(printf '%s' "$msg" | sed 's/\\u0026/\&/g')
link=$(printf '%s' "$msg" | grep -oE 'http://localhost:50062/scheduled-reports/[^"\\ ]+/download\?[^"\\ ]+' | head -n1 || true)
[ -n "$link" ] || fail "notification link missing"
curl -fsS "$link" -o /tmp/notified.csv || fail "notification link download"
hdr=$(head -n 1 /tmp/notified.csv)
[ "$hdr" = "call_id,account_id,destination,cli,started_at_ms,ended_at_ms,charged_seconds,end_reason,trunk_id,rate_per_sec,amount_minor,rate_missing" ] || fail "notification csv"
echo "ok: notification email"
```

- [ ] **Step 7: Document the slice in AGENTS.md**

Append to the P5 bullets in `AGENTS.md`:

```markdown
- P5 slice six: `services/notifications` on :50064 consumes `report.ready` (group `notifications`), stores one deduped row per `artifact_id`+`recipient_email` in the `notifications` Postgres outbox, and sends plain-text email via `net/smtp` to Mailpit (`mailpit:1025`, UI http://localhost:8025). Kafka offsets commit only after the outbox insert; the worker claims due rows with `FOR UPDATE SKIP LOCKED`, retries SMTP failures up to `NOTIFICATIONS_MAX_ATTEMPTS` with exponential backoff, and marks terminal validation failures `failed`. Reports now stores the creator's identity email on schedules, publishes `recipient_email`/`artifact_url`/`expires_at_ms`, and accepts HMAC-SHA256 signed artifact download links (`REPORTS_ARTIFACT_SIGNING_SECRET`, `REPORTS_SIGNED_URL_TTL` ms, `REPORTS_PUBLIC_BASE_URL`). Invalid or expired links return 403. Envs: `NOTIFICATIONS_DSN`, `NOTIFICATIONS_KAFKA`, `NOTIFICATIONS_TOPIC_REPORT_READY`, `NOTIFICATIONS_GROUP`, `NOTIFICATIONS_SMTP_ADDR`, `NOTIFICATIONS_SMTP_FROM`, `NOTIFICATIONS_MAX_ATTEMPTS`, `NOTIFICATIONS_POLL_INTERVAL`, `NOTIFICATIONS_BATCH_SIZE`, `NOTIFICATIONS_ADDR`. `ops/smoke.sh` resets Mailpit, runs a scheduled report, waits for one email, and follows its signed link to CSV.
```

- [ ] **Step 8: Create the dev database in an existing volume**

Run:

```bash
docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT 1 FROM pg_database WHERE datname='notifications'" | grep -q 1 || docker exec omni-fon-postgres-1 createdb -U postgres notifications
```

Expected: exit 0.

- [ ] **Step 9: Build and start the new services**

Run:

```bash
docker compose build reports notifications
docker compose up -d postgres kafka mailpit notifications reports
```

Expected: exit 0; `docker inspect --format '{{.State.Status}}' omni-fon-notifications-1` reports `running`; `omni-fon-mailpit-1` reports `running`.

- [ ] **Step 10: Run the final gate**

Run each command from the repo root and record the output:

```bash
make protoc
git diff --exit-code
gofmt -l services/reports services/notifications
cd services/reports && go vet ./... && go test ./...
cd ../notifications && go vet ./... && go test ./...
cd ../..
bash -n ops/smoke.sh
docker compose config -q
make test
make smoke
bash ops/sip-e2e.sh
```

Expected: `make protoc` leaves no diff; `gofmt -l` prints nothing; all Go tests pass; `bash -n` and `docker compose config -q` exit 0; `make test` includes notifications; `make smoke` prints `ok: notification email` and `smoke: PASS`; `ops/sip-e2e.sh` remains green.

- [ ] **Step 11: Commit**

```bash
git add docker-compose.yml ops/postgres-init.sql Makefile ops/smoke.sh AGENTS.md
git commit -m "chore: wire notifications into the dev stack"
```

---

## Plan Self-Review

- Spec coverage: event contract, recipient capture, signed URLs, reports publication, outbox dedupe, Kafka commit ordering, SMTP retries, Mailpit, compose, DB init, Makefile, smoke, AGENTS are each covered by a task.
- Placeholder scan: every task has exact file paths, code, commands, and expected outcomes. No `TBD`, `TODO`, `FIXME`, or “similar to” shortcuts remain.
- Type consistency: `NewScheduler` gains `signer *ArtifactSigner` in Task 3 and every call site is updated in Task 3; `NewArtifactSigner` is defined in Task 2; `notificationFromEvent` is defined in Task 8 and used in Task 9; `Sender` is defined in Task 9 before `Worker`.
- Test ordering: every production change follows a failing test except compose, database init, Makefile, AGENTS, and smoke wiring, which are verified by `docker compose config -q`, `bash -n`, `make test`, and `make smoke`.
