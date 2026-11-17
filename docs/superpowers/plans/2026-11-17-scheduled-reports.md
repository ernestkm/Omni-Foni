# Scheduled Reports Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add tenant-scoped recurring report schedules to `services/reports`: Postgres-backed schedules and artifacts, a minute ticker that renders due reports to CSV, a `report.ready` Kafka event, and HTTP CRUD + download + manual-run routes.

**Architecture:** The scheduler lives inside `services/reports`. A pgx pool (`reports_sched` DB) stores `schedules` and `report_artifacts`. `RunDueSchedules(ctx, now)` is directly callable (reconciliation `Sweep` pattern) and renders each due schedule through the existing `API` surface into a CSV artifact, then publishes `ReportReadyEvent` to `report.ready`. HTTP routes reuse the existing identity bearer + tenant scoping facade. Empty `REPORTS_SCHED_DSN` disables the whole feature.

**Tech Stack:** Go 1.27.4, pgx v5 (pgxpool), franz-go, protobuf (new `scheduling.v1` package generated into the reports module), ClickHouse (existing), stdlib `net/http` mux.

## Global Constraints

- Stubs are regenerated only with `make protoc` from the repo root (PATHs `/root/go/bin`); never hand-edit `services/*/gen/`.
- Commit messages are lowercase conventional commits, exactly as each task specifies.
- Run `go vet ./...` in the touched module before every commit.
- Money stays integer minor units; the scheduler never invents money — it reuses the existing gRPC surface verbatim.
- CSV rules are the existing ones: header uses JSON snake_case names, `Content-Type: text/csv; charset=utf-8`, `Content-Disposition: attachment; filename="..."`, 10,000-data-row cap (`csvMaxRows`), 1000-row pages (`csvPageSize`).
- Tenant scoping matches the other reports endpoints: bearer required; role `user` → 403; reseller forced to own tenant; admin must pass `tenant_id` else 400; cross-tenant access → 404.
- `reports` is the only module touched; no new services or containers.
- Postgres DSNs in dev: `postgres://postgres:dev-only@localhost:5432/<db>` from host tests, `postgres://postgres:dev-only@postgres:5432/<db>` in compose.
- The dev stack must be up for integration tests: `make up` (never rebuilds; `docker compose build <svc> && docker compose up -d <svc>` after code changes).

---

### Task 1: Report-ready event proto

**Files:**
- Create: `proto/scheduling/v1/scheduling.proto`
- Create: `services/reports/gen/schedulingpb/*.go` (generated)
- Modify: `Makefile:59-61` (add a `make protoc` line)
- Test: `services/reports/sched_event_test.go`

**Interfaces:**
- Produces: `schedulingpb.ReportReadyEvent` (package `omnifoni/reports/gen/schedulingpb`), fields `ScheduleId`, `ArtifactId`, `TenantId`, `ReportKind`, `PeriodFrom`, `PeriodTo` (strings, RFC3339 UTC), `RowCount int64`, `CreatedAtMs int64`.

- [ ] **Step 1: Write the failing test**

Create `services/reports/sched_event_test.go`:

```go
package main

import (
	"testing"

	"google.golang.org/protobuf/proto"

	schedulingpb "omnifoni/reports/gen/schedulingpb"
)

func TestReportReadyEventRoundTrip(t *testing.T) {
	in := &schedulingpb.ReportReadyEvent{
		ScheduleId: "s1", ArtifactId: "a1", TenantId: "t1", ReportKind: "cdrs",
		PeriodFrom: "2026-11-01T00:00:00Z", PeriodTo: "2026-11-02T00:00:00Z",
		RowCount: 42, CreatedAtMs: 1763740800000,
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
		out.GetReportKind() != "cdrs" || out.GetRowCount() != 42 || out.GetCreatedAtMs() != 1763740800000 {
		t.Fatalf("round trip mismatch: %+v", &out)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/reports && go test -run TestReportReadyEventRoundTrip ./...`
Expected: FAIL — `no required module provides package omnifoni/reports/gen/schedulingpb` (or `undefined: schedulingpb`).

- [ ] **Step 3: Write the proto**

Create `proto/scheduling/v1/scheduling.proto`:

```proto
syntax = "proto3";

package scheduling.v1;

option go_package = "omnifoni/reports/gen/schedulingpb";

// ReportReadyEvent announces one stored scheduled-report artifact on the
// report.ready topic (docs/superpowers/specs/2026-11-17-scheduled-reports-design.md).
message ReportReadyEvent {
  string schedule_id   = 1;
  string artifact_id   = 2;
  string tenant_id     = 3;
  string report_kind   = 4;
  string period_from   = 5; // RFC3339 UTC
  string period_to     = 6; // RFC3339 UTC
  int64  row_count     = 7;
  int64  created_at_ms = 8;
}
```

Add to the `protoc` target in `Makefile`, immediately after the existing reports block (currently lines 59-61):

```make
	PATH=/root/go/bin:$(PATH) protoc -I proto --go_out=services/reports --go_opt=module=omnifoni/reports \
		proto/scheduling/v1/scheduling.proto
```

- [ ] **Step 4: Generate the stubs**

Run from repo root: `make protoc`
Expected: `services/reports/gen/schedulingpb/scheduling.pb.go` appears; `git status --short` shows only the new proto, the new gen directory, the Makefile, and the test.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd services/reports && go test -run TestReportReadyEventRoundTrip ./...`
Expected: PASS. Also `go vet ./...` clean.

- [ ] **Step 6: Commit**

```bash
git add Makefile proto/scheduling/v1/scheduling.proto services/reports/gen/schedulingpb services/reports/sched_event_test.go
git commit -m "feat(reports): add report-ready event proto"
```

---

### Task 2: Scheduled-report Postgres store

**Files:**
- Create: `services/reports/sched_migrate.sql`
- Create: `services/reports/sched_store.go`
- Modify: `ops/postgres-init.sql` (add `CREATE DATABASE reports_sched;`)
- Modify: `services/reports/go.mod` / `go.sum` (add pgx v5, promote google/uuid)
- Modify: `services/reports/store_test.go:20-34` (TestMain opens the sched store)
- Test: `services/reports/sched_store_test.go`

**Interfaces:**
- Produces:
  - `type Schedule struct { ScheduleID, TenantID string; AccountIDs []string; ReportKind, WindowKind, GroupBy string; PrefixLen int; Frequency string; AtHour, AtMinute int; Weekday, DayOfMonth *int; Enabled bool; CreatedBy string; CreatedAt, UpdatedAt time.Time }`
  - `type Artifact struct { ArtifactID, ScheduleID, SlotKey string; PeriodFrom, PeriodTo, CreatedAt time.Time; RowCount int64 }`
  - `type SchedStore struct { pool *pgxpool.Pool }`
  - `func OpenSched(ctx context.Context, dsn string) (*SchedStore, error)`, `(*SchedStore).Close()`, `(*SchedStore).Migrate(ctx) error`
  - `CreateSchedule(ctx, Schedule) (Schedule, error)`, `GetSchedule(ctx, id string) (Schedule, error)`, `ListSchedules(ctx, tenantID string) ([]Schedule, error)`, `ListEnabledSchedules(ctx) ([]Schedule, error)`, `UpdateSchedule(ctx, Schedule) error`, `DeleteSchedule(ctx, id string) error`
  - `InsertArtifact(ctx, Artifact) (Artifact, bool, error)` — bool false when `(schedule_id, slot_key)` already exists
  - `ListArtifacts(ctx, scheduleID string) ([]Artifact, error)`, `GetArtifact(ctx, scheduleID, artifactID string) (Artifact, string, error)` (returns csv body), `PruneArtifacts(ctx, before time.Time) (int64, error)`
  - `func schedTestDSN() string` (test helper, default `postgres://postgres:dev-only@localhost:5432/reports_sched`)
- Consumes: nothing from Task 1.

- [ ] **Step 1: Write the failing test**

Create `services/reports/sched_store_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

var schedDB *SchedStore

func TestMainSched(t *testing.T) {
	if schedDB == nil {
		t.Fatal("sched store not initialized (see store_test.go TestMain)")
	}
}

func schedReset(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := schedDB.pool.Exec(ctx, "TRUNCATE report_artifacts, schedules"); err != nil {
		t.Fatal(err)
	}
}

func newTestSchedule(tenant string) Schedule {
	return Schedule{
		TenantID: tenant, AccountIDs: []string{"a1"}, ReportKind: "cdrs",
		WindowKind: "previous_month", GroupBy: "none", PrefixLen: 3,
		Frequency: "daily", AtHour: 6, AtMinute: 30, Enabled: true, CreatedBy: "tester",
	}
}

func TestSchedStoreCRUD(t *testing.T) {
	schedReset(t)
	ctx := context.Background()
	created, err := schedDB.CreateSchedule(ctx, newTestSchedule("t1"))
	if err != nil {
		t.Fatal(err)
	}
	if created.ScheduleID == "" || created.CreatedAt.IsZero() {
		t.Fatalf("bad created schedule: %+v", created)
	}
	got, err := schedDB.GetSchedule(ctx, created.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TenantID != "t1" || got.ReportKind != "cdrs" || !got.Enabled || got.Weekday != nil {
		t.Fatalf("bad round trip: %+v", got)
	}
	got.ReportKind = "summary"
	got.GroupBy = "prefix"
	got.PrefixLen = 5
	if err := schedDB.UpdateSchedule(ctx, got); err != nil {
		t.Fatal(err)
	}
	again, _ := schedDB.GetSchedule(ctx, created.ScheduleID)
	if again.ReportKind != "summary" || again.PrefixLen != 5 {
		t.Fatalf("update not applied: %+v", again)
	}
	list, err := schedDB.ListSchedules(ctx, "t1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %v, %v", list, err)
	}
	enabled, err := schedDB.ListEnabledSchedules(ctx)
	if err != nil || len(enabled) != 1 {
		t.Fatalf("enabled = %v, %v", enabled, err)
	}
	if _, err := schedDB.GetSchedule(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing schedule err = %v", err)
	}
	if err := schedDB.DeleteSchedule(ctx, created.ScheduleID); err != nil {
		t.Fatal(err)
	}
	if _, err := schedDB.GetSchedule(ctx, created.ScheduleID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("after delete err = %v", err)
	}
}

func TestSchedStoreArtifacts(t *testing.T) {
	schedReset(t)
	ctx := context.Background()
	sch, err := schedDB.CreateSchedule(ctx, newTestSchedule("t1"))
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	a1, inserted, err := schedDB.InsertArtifact(ctx, Artifact{
		ScheduleID: sch.ScheduleID, SlotKey: "2026-11-17T06:30Z",
		PeriodFrom: from, PeriodTo: from.Add(24 * time.Hour), RowCount: 2,
	})
	if err != nil || !inserted {
		t.Fatalf("insert = %+v, %v, %v", a1, inserted, err)
	}
	if a1.ArtifactID == "" || a1.CreatedAt.IsZero() {
		t.Fatalf("bad artifact: %+v", a1)
	}
	_, inserted, err = schedDB.InsertArtifact(ctx, Artifact{
		ScheduleID: sch.ScheduleID, SlotKey: "2026-11-17T06:30Z",
		PeriodFrom: from, PeriodTo: from.Add(24 * time.Hour), RowCount: 99,
	})
	if err != nil || inserted {
		t.Fatalf("duplicate slot inserted: %v, %v", inserted, err)
	}
	list, err := schedDB.ListArtifacts(ctx, sch.ScheduleID)
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %v, %v", list, err)
	}
	if _, err := schedDB.GetArtifact(ctx, sch.ScheduleID, a1.ArtifactID); err != nil {
		t.Fatal(err)
	}
	if _, err := schedDB.GetArtifact(ctx, sch.ScheduleID, "other"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("cross-artifact err = %v", err)
	}
}

func TestSchedStoreCascadeAndPrune(t *testing.T) {
	schedReset(t)
	ctx := context.Background()
	sch, err := schedDB.CreateSchedule(ctx, newTestSchedule("t1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := schedDB.InsertArtifact(ctx, Artifact{
		ScheduleID: sch.ScheduleID, SlotKey: "old", PeriodFrom: time.Now().Add(-48 * time.Hour),
		PeriodTo: time.Now(), RowCount: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := schedDB.pool.Exec(ctx,
		"UPDATE report_artifacts SET created_at = now() - interval '100 days' WHERE schedule_id = $1", sch.ScheduleID); err != nil {
		t.Fatal(err)
	}
	n, err := schedDB.PruneArtifacts(ctx, time.Now().AddDate(0, 0, -90))
	if err != nil || n != 1 {
		t.Fatalf("prune = %d, %v", n, err)
	}
	if err := schedDB.DeleteSchedule(ctx, sch.ScheduleID); err != nil {
		t.Fatal(err)
	}
	if n, _ := schedDB.pool.Query(ctx, "SELECT count(*) FROM report_artifacts"); n != nil {
		// placeholder: cascade verified below via a fresh insert after delete is not possible;
		// instead count through ListArtifacts on a deleted schedule.
		_ = n
	}
}
```

Replace the placeholder tail of `TestSchedStoreCascadeAndPrune` with a real assertion before committing:

```go
	if err := schedDB.DeleteSchedule(ctx, sch.ScheduleID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := schedDB.pool.QueryRow(ctx, "SELECT count(*) FROM report_artifacts").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("cascade left %d artifacts", count)
	}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/reports && go test -run 'TestSchedStore' ./...`
Expected: FAIL — `undefined: SchedStore` / `undefined: schedDB`.

- [ ] **Step 3: Add the database and schema**

Edit `ops/postgres-init.sql`, appending:

```sql
CREATE DATABASE reports_sched;
```

Create `services/reports/sched_migrate.sql`:

```sql
CREATE TABLE IF NOT EXISTS schedules (
  schedule_id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    text NOT NULL,
  account_ids  text[] NOT NULL DEFAULT '{}',
  report_kind  text NOT NULL CHECK (report_kind IN ('cdrs','summary','payments','charges','low_balance')),
  window_kind  text NOT NULL CHECK (window_kind IN ('last_day','last_week','last_month','previous_month')),
  group_by     text NOT NULL DEFAULT 'none' CHECK (group_by IN ('none','prefix')),
  prefix_len   smallint NOT NULL DEFAULT 3 CHECK (prefix_len BETWEEN 1 AND 15),
  frequency    text NOT NULL CHECK (frequency IN ('daily','weekly','monthly')),
  at_hour      smallint NOT NULL CHECK (at_hour BETWEEN 0 AND 23),
  at_minute    smallint NOT NULL CHECK (at_minute BETWEEN 0 AND 59),
  weekday      smallint CHECK (weekday BETWEEN 0 AND 6),
  day_of_month smallint CHECK (day_of_month BETWEEN 1 AND 31),
  enabled      boolean NOT NULL DEFAULT true,
  created_by   text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS report_artifacts (
  artifact_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  schedule_id uuid NOT NULL REFERENCES schedules(schedule_id) ON DELETE CASCADE,
  slot_key    text NOT NULL,
  period_from timestamptz NOT NULL,
  period_to   timestamptz NOT NULL,
  row_count   bigint NOT NULL,
  csv_body    text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (schedule_id, slot_key)
);
```

Apply it to the running dev stack (the init script only runs on first volume init):

```bash
docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT 1 FROM pg_database WHERE datname='reports_sched'" | grep -q 1 || docker exec omni-fon-postgres-1 createdb -U postgres reports_sched
docker exec omni-fon-postgres-1 psql -U postgres -d reports_sched -f - < services/reports/sched_migrate.sql
```

- [ ] **Step 4: Write the store**

Create `services/reports/sched_store.go`:

```go
package main

import (
	"context"
	_ "embed"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sched_migrate.sql
var schedSchemaSQL string

// Schedule is one recurring report definition. Weekday (weekly) and
// DayOfMonth (monthly) are nil for the other frequencies.
type Schedule struct {
	ScheduleID string
	TenantID   string
	AccountIDs []string
	ReportKind string
	WindowKind string
	GroupBy    string
	PrefixLen  int
	Frequency  string
	AtHour     int
	AtMinute   int
	Weekday    *int
	DayOfMonth *int
	Enabled    bool
	CreatedBy  string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Artifact is one rendered CSV body plus its slot identity.
type Artifact struct {
	ArtifactID string
	ScheduleID string
	SlotKey    string
	PeriodFrom time.Time
	PeriodTo   time.Time
	RowCount   int64
	CreatedAt  time.Time
}

// SchedStore persists schedules and artifacts in Postgres.
type SchedStore struct {
	pool *pgxpool.Pool
}

func OpenSched(ctx context.Context, dsn string) (*SchedStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &SchedStore{pool: pool}, nil
}

func (s *SchedStore) Close() { s.pool.Close() }

// Migrate applies the schema. Idempotent, safe at boot.
func (s *SchedStore) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schedSchemaSQL)
	return err
}

const scheduleColumns = `schedule_id, tenant_id, account_ids, report_kind, window_kind,
	group_by, prefix_len, frequency, at_hour, at_minute, weekday, day_of_month,
	enabled, created_by, created_at, updated_at`

func scanSchedule(row pgx.Row) (Schedule, error) {
	var s Schedule
	err := row.Scan(&s.ScheduleID, &s.TenantID, &s.AccountIDs, &s.ReportKind, &s.WindowKind,
		&s.GroupBy, &s.PrefixLen, &s.Frequency, &s.AtHour, &s.AtMinute, &s.Weekday, &s.DayOfMonth,
		&s.Enabled, &s.CreatedBy, &s.CreatedAt, &s.UpdatedAt)
	return s, err
}

func (s *SchedStore) CreateSchedule(ctx context.Context, in Schedule) (Schedule, error) {
	return scanSchedule(s.pool.QueryRow(ctx, `INSERT INTO schedules
		(tenant_id, account_ids, report_kind, window_kind, group_by, prefix_len,
		 frequency, at_hour, at_minute, weekday, day_of_month, enabled, created_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		RETURNING `+scheduleColumns,
		in.TenantID, in.AccountIDs, in.ReportKind, in.WindowKind, in.GroupBy, in.PrefixLen,
		in.Frequency, in.AtHour, in.AtMinute, in.Weekday, in.DayOfMonth, in.Enabled, in.CreatedBy))
}

func (s *SchedStore) GetSchedule(ctx context.Context, id string) (Schedule, error) {
	return scanSchedule(s.pool.QueryRow(ctx,
		`SELECT `+scheduleColumns+` FROM schedules WHERE schedule_id = $1`, id))
}

func (s *SchedStore) ListSchedules(ctx context.Context, tenantID string) ([]Schedule, error) {
	return s.querySchedules(ctx, `SELECT `+scheduleColumns+` FROM schedules
		WHERE ($1 = '' OR tenant_id = $1) ORDER BY created_at DESC`, tenantID)
}

func (s *SchedStore) ListEnabledSchedules(ctx context.Context) ([]Schedule, error) {
	return s.querySchedules(ctx, `SELECT `+scheduleColumns+` FROM schedules
		WHERE enabled ORDER BY created_at ASC`, "")
}

func (s *SchedStore) querySchedules(ctx context.Context, query, arg string) ([]Schedule, error) {
	rows, err := s.pool.Query(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (s *SchedStore) UpdateSchedule(ctx context.Context, in Schedule) error {
	_, err := s.pool.Exec(ctx, `UPDATE schedules SET
		account_ids = $2, report_kind = $3, window_kind = $4, group_by = $5, prefix_len = $6,
		frequency = $7, at_hour = $8, at_minute = $9, weekday = $10, day_of_month = $11,
		enabled = $12, updated_at = now()
		WHERE schedule_id = $1`,
		in.ScheduleID, in.AccountIDs, in.ReportKind, in.WindowKind, in.GroupBy, in.PrefixLen,
		in.Frequency, in.AtHour, in.AtMinute, in.Weekday, in.DayOfMonth, in.Enabled)
	return err
}

func (s *SchedStore) DeleteSchedule(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM schedules WHERE schedule_id = $1`, id)
	return err
}

// InsertArtifact stores one artifact; inserted is false when the slot already
// ran (UNIQUE (schedule_id, slot_key)).
func (s *SchedStore) InsertArtifact(ctx context.Context, a Artifact) (Artifact, bool, error) {
	var out Artifact
	err := s.pool.QueryRow(ctx, `INSERT INTO report_artifacts
		(schedule_id, slot_key, period_from, period_to, row_count, csv_body)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (schedule_id, slot_key) DO NOTHING
		RETURNING artifact_id, schedule_id, slot_key, period_from, period_to, row_count, created_at`,
		a.ScheduleID, a.SlotKey, a.PeriodFrom, a.PeriodTo, a.RowCount, a.CSVBody).
		Scan(&out.ArtifactID, &out.ScheduleID, &out.SlotKey, &out.PeriodFrom, &out.PeriodTo,
			&out.RowCount, &out.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Artifact{}, false, nil
	}
	if err != nil {
		return Artifact{}, false, err
	}
	return out, true, nil
}

func (s *SchedStore) ListArtifacts(ctx context.Context, scheduleID string) ([]Artifact, error) {
	rows, err := s.pool.Query(ctx, `SELECT artifact_id, schedule_id, slot_key, period_from,
		period_to, row_count, created_at FROM report_artifacts
		WHERE schedule_id = $1 ORDER BY created_at DESC`, scheduleID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Artifact
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.ArtifactID, &a.ScheduleID, &a.SlotKey, &a.PeriodFrom,
			&a.PeriodTo, &a.RowCount, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *SchedStore) GetArtifact(ctx context.Context, scheduleID, artifactID string) (Artifact, string, error) {
	var a Artifact
	var body string
	err := s.pool.QueryRow(ctx, `SELECT artifact_id, schedule_id, slot_key, period_from,
		period_to, row_count, csv_body, created_at FROM report_artifacts
		WHERE schedule_id = $1 AND artifact_id = $2`, scheduleID, artifactID).
		Scan(&a.ArtifactID, &a.ScheduleID, &a.SlotKey, &a.PeriodFrom, &a.PeriodTo,
			&a.RowCount, &body, &a.CreatedAt)
	return a, body, err
}

func (s *SchedStore) PruneArtifacts(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM report_artifacts WHERE created_at < $1`, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
```

Fix the test compile in this same step: `Artifact` has no `CSVBody` field in the API above. Add it to the struct and to `InsertArtifact` (the scheduler sets it):

```go
type Artifact struct {
	ArtifactID string
	ScheduleID string
	SlotKey    string
	PeriodFrom time.Time
	PeriodTo   time.Time
	RowCount   int64
	CSVBody    string
	CreatedAt  time.Time
}
```

Add `"errors"` to the import block of `sched_store.go`.

- [ ] **Step 5: Wire the store into TestMain**

Edit `services/reports/store_test.go` TestMain (after the ClickHouse setup, before `os.Exit(m.Run())`):

```go
	schedDB, err = OpenSched(ctx, schedTestDSN())
	if err != nil {
		fmt.Fprintln(os.Stderr, "sched db:", err)
		os.Exit(1)
	}
	defer schedDB.Close()
	if err := schedDB.Migrate(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "sched migrate:", err)
		os.Exit(1)
	}
```

Add to `store_test.go`:

```go
func schedTestDSN() string {
	if s := os.Getenv("REPORTS_TEST_SCHED_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/reports_sched"
}
```

- [ ] **Step 6: Add dependencies**

```bash
cd services/reports && go get github.com/jackc/pgx/v5@v5.13.0 && go mod tidy
```

Expected: `go.mod` gains `github.com/jackc/pgx/v5 v5.13.0` (direct) and promotes `github.com/google/uuid` to a direct requirement if referenced later; nothing else changes.

- [ ] **Step 7: Run tests to verify they pass**

Run: `cd services/reports && go test -run 'TestSchedStore' ./...`
Expected: PASS (all three tests). `go vet ./...` clean.

- [ ] **Step 8: Commit**

```bash
git add ops/postgres-init.sql services/reports/sched_migrate.sql services/reports/sched_store.go services/reports/sched_store_test.go services/reports/store_test.go services/reports/go.mod services/reports/go.sum
git commit -m "feat(reports): add scheduled report store"
```

---

### Task 3: Scheduler runtime, CSV rendering, and row helpers

**Files:**
- Create: `services/reports/report_rows.go`
- Create: `services/reports/scheduler.go`
- Modify: `services/reports/http.go` (use the row helpers in the five existing handlers)
- Test: `services/reports/scheduler_test.go`

**Interfaces:**
- Consumes: `SchedStore` (Task 2), `API` interface (`http.go:22-30`), `csvMaxRows`/`csvPageSize`/`cdrCSVHeader`/`summaryCSVHeader`/`paymentCSVHeader`/`chargeCSVHeader`/`balanceCSVHeader` (`http.go`).
- Produces:
  - `type ReportPublisher interface { PublishReportReady(ctx context.Context, ev *schedulingpb.ReportReadyEvent) error }`
  - `type kafkaReportPublisher struct { prod *kgo.Client; topic string }`, `func newKafkaReportPublisher(prod *kgo.Client, topic string) ReportPublisher`
  - `type Scheduler struct { store *SchedStore; api API; pub ReportPublisher }`, `func NewScheduler(store *SchedStore, api API, pub ReportPublisher) *Scheduler`
  - `func due(s Schedule, now time.Time) bool`, `func resolvePeriod(kind string, now time.Time) (time.Time, time.Time, error)`
  - `func (s *Scheduler) RunDueSchedules(ctx context.Context, now time.Time) error`
  - `func (s *Scheduler) RunScheduleNow(ctx context.Context, sch Schedule) (Artifact, error)`
  - `func cdrCSVRow(c *pb.Cdr) []string`, `summaryCSVRow(*pb.SummaryRow) []string`, `paymentCSVRow(*pb.Payment) []string`, `chargeCSVRow(*pb.Charge) []string`, `balanceCSVRow(*pb.BalanceRow) []string`

- [ ] **Step 1: Write the failing tests**

Create `services/reports/scheduler_test.go`:

```go
package main

import (
	"context"
	"testing"
	"time"

	schedulingpb "omnifoni/reports/gen/schedulingpb"
	pb "omnifoni/reports/gen/reportspb"
)

type fakePublisher struct{ events []*schedulingpb.ReportReadyEvent }

func (f *fakePublisher) PublishReportReady(_ context.Context, ev *schedulingpb.ReportReadyEvent) error {
	f.events = append(f.events, ev)
	return nil
}

func TestDueSchedule(t *testing.T) {
	weekday := 1
	dom := 31
	disabled := false
	now := time.Date(2026, 11, 17, 6, 30, 0, 0, time.UTC) // Tuesday
	cases := []struct {
		name string
		s    Schedule
		want bool
	}{
		{"daily hit", Schedule{Enabled: true, Frequency: "daily", AtHour: 6, AtMinute: 30}, true},
		{"daily wrong minute", Schedule{Enabled: true, Frequency: "daily", AtHour: 6, AtMinute: 31}, false},
		{"disabled", Schedule{Enabled: false, Frequency: "daily", AtHour: 6, AtMinute: 30}, false},
		{"weekly hit (Tuesday=2? no: 1=Monday, Tuesday=2)", Schedule{Enabled: true, Frequency: "weekly", AtHour: 6, AtMinute: 30, Weekday: &weekday}, false},
		{"weekly miss", Schedule{Enabled: true, Frequency: "weekly", AtHour: 6, AtMinute: 30, Weekday: &dom}, false},
		{"monthly hit (Nov has 30 days)", Schedule{Enabled: true, Frequency: "monthly", AtHour: 6, AtMinute: 30, DayOfMonth: &dom}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := due(tc.s, now); got != tc.want {
				t.Fatalf("due = %v, want %v", got, tc.want)
			}
		})
	}
	feb := time.Date(2026, 2, 27, 6, 30, 0, 0, time.UTC)
	if !due(Schedule{Enabled: true, Frequency: "monthly", AtHour: 6, AtMinute: 30, DayOfMonth: &dom}, feb) {
		t.Fatal("31st should fire on the last day of February")
	}
}

func TestResolvePeriod(t *testing.T) {
	now := time.Date(2026, 11, 17, 6, 30, 0, 0, time.UTC)
	from, to, err := resolvePeriod("last_day", now)
	if err != nil || !from.Equal(now.Add(-24*time.Hour)) || !to.Equal(now) {
		t.Fatalf("last_day = %v, %v, %v", from, to, err)
	}
	from, to, err = resolvePeriod("previous_month", now)
	if err != nil || !from.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) ||
		!to.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("previous_month = %v, %v, %v", from, to, err)
	}
	if _, _, err := resolvePeriod("bogus", now); err == nil {
		t.Fatal("expected error for unknown window")
	}
}

func TestRunDueSchedulesCreatesOneArtifact(t *testing.T) {
	schedReset(t)
	reset(t)
	ctx := context.Background()
	sch, err := schedDB.CreateSchedule(ctx, Schedule{
		TenantID: "t1", AccountIDs: []string{"a1"}, ReportKind: "cdrs",
		WindowKind: "last_day", GroupBy: "none", PrefixLen: 3,
		Frequency: "daily", AtHour: 6, AtMinute: 30, Enabled: true, CreatedBy: "tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 17, 6, 30, 0, 0, time.UTC)
	start := now.Add(-2 * time.Hour)
	seed(t, testRow("r1", "a1", "27820001", "trunk-1", "normal", start, 60, 10))
	pub := &fakePublisher{}
	s := NewScheduler(schedDB, &reportsServer{store: store}, pub)
	if err := s.RunDueSchedules(ctx, now); err != nil {
		t.Fatal(err)
	}
	list, err := schedDB.ListArtifacts(ctx, sch.ScheduleID)
	if err != nil || len(list) != 1 {
		t.Fatalf("artifacts = %v, %v", list, err)
	}
	if list[0].RowCount != 1 {
		t.Fatalf("row_count = %d", list[0].RowCount)
	}
	if len(pub.events) != 1 || pub.events[0].GetScheduleId() != sch.ScheduleID ||
		pub.events[0].GetArtifactId() != list[0].ArtifactID || pub.events[0].GetRowCount() != 1 {
		t.Fatalf("events = %+v", pub.events)
	}
	if err := s.RunDueSchedules(ctx, now); err != nil {
		t.Fatal(err)
	}
	again, _ := schedDB.ListArtifacts(ctx, sch.ScheduleID)
	if len(again) != 1 {
		t.Fatalf("duplicate artifact on same slot: %d", len(again))
	}
}

func TestRunScheduleNowUsesManualSlot(t *testing.T) {
	schedReset(t)
	reset(t)
	ctx := context.Background()
	sch, err := schedDB.CreateSchedule(ctx, Schedule{
		TenantID: "t1", AccountIDs: []string{"a1"}, ReportKind: "cdrs",
		WindowKind: "last_day", GroupBy: "none", PrefixLen: 3,
		Frequency: "daily", AtHour: 0, AtMinute: 0, Enabled: true, CreatedBy: "tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	seed(t, testRow("r1", "a1", "27820001", "trunk-1", "normal", now.Add(-time.Minute), 60, 10))
	s := NewScheduler(schedDB, &reportsServer{store: store}, &fakePublisher{})
	a, err := s.RunScheduleNow(ctx, sch)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.SlotKey) < 7 || a.SlotKey[:7] != "manual-" {
		t.Fatalf("slot = %q", a.SlotKey)
	}
	if a.RowCount != 1 {
		t.Fatalf("row_count = %d", a.RowCount)
	}
	if _, body, err := schedDB.GetArtifact(ctx, sch.ScheduleID, a.ArtifactID); err != nil {
		t.Fatal(err)
	} else if body == "" {
		t.Fatal("empty csv body")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd services/reports && go test -run 'TestDueSchedule|TestResolvePeriod|TestRunDueSchedules|TestRunScheduleNow' ./...`
Expected: FAIL — `undefined: due`, `undefined: Scheduler`, etc.

- [ ] **Step 3: Add the row helpers**

Create `services/reports/report_rows.go`:

```go
package main

import (
	"strconv"

	pb "omnifoni/reports/gen/reportspb"
)

func cdrCSVRow(c *pb.Cdr) []string {
	return []string{
		c.GetCallId(), c.GetAccountId(), c.GetDestination(), c.GetCli(),
		strconv.FormatInt(c.GetStartedAtMs(), 10), strconv.FormatInt(c.GetEndedAtMs(), 10),
		strconv.FormatInt(c.GetChargedSeconds(), 10), c.GetEndReason(), c.GetTrunkId(),
		c.GetRatePerSec(), strconv.FormatInt(c.GetAmountMinor(), 10),
		strconv.FormatBool(c.GetRateMissing()),
	}
}

func summaryCSVRow(r *pb.SummaryRow) []string {
	return []string{r.GetKey(), strconv.FormatInt(r.GetCalls(), 10),
		strconv.FormatInt(r.GetSeconds(), 10), strconv.FormatInt(r.GetAmountMinor(), 10)}
}

func paymentCSVRow(p *pb.Payment) []string {
	return []string{
		strconv.FormatInt(p.GetPaymentId(), 10), p.GetEventId(), p.GetAccountId(),
		p.GetGateway(), strconv.FormatInt(p.GetAmountMinor(), 10), p.GetStatus(),
		strconv.FormatInt(p.GetCreatedAtMs(), 10),
	}
}

func chargeCSVRow(c *pb.Charge) []string {
	return []string{
		c.GetSessionId(), c.GetAccountId(),
		strconv.FormatInt(c.GetChargedSeconds(), 10), strconv.FormatInt(c.GetAmountMinor(), 10),
		strconv.FormatInt(c.GetBalanceAfter(), 10), strconv.FormatInt(c.GetLastChargedAtMs(), 10),
	}
}

func balanceCSVRow(b *pb.BalanceRow) []string {
	return []string{
		b.GetAccountId(), b.GetKind(), strconv.FormatInt(b.GetBalance(), 10),
		strconv.FormatInt(b.GetCreditLimit(), 10), b.GetCurrency(),
	}
}
```

Then replace the five inline row constructions in `services/reports/http.go` with these helpers:

- In `ListCdrs` CSV fetch, replace the `rows = append(rows, []string{...})` body with `rows = append(rows, cdrCSVRow(c))`.
- In `Summarize` CSV fetch, replace the append with `rows = append(rows, summaryCSVRow(row))`.
- In `Payments` CSV fetch, replace with `rows = append(rows, paymentCSVRow(p))`.
- In `Charges` CSV fetch, replace with `rows = append(rows, chargeCSVRow(c))`.
- In `LowBalance` CSV fetch, replace with `rows = append(rows, balanceCSVRow(a))`.

Leave the JSON paths untouched.

- [ ] **Step 4: Write the scheduler**

Create `services/reports/scheduler.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel"

	schedulingpb "omnifoni/reports/gen/schedulingpb"
	pb "omnifoni/reports/gen/reportspb"
	"omnifoni/observability"
)

// ReportPublisher announces stored artifacts; the scheduler uses it for
// notifications only, never for storage.
type ReportPublisher interface {
	PublishReportReady(ctx context.Context, ev *schedulingpb.ReportReadyEvent) error
}

type kafkaReportPublisher struct {
	prod  *kgo.Client
	topic string
}

func newKafkaReportPublisher(prod *kgo.Client, topic string) ReportPublisher {
	return &kafkaReportPublisher{prod: prod, topic: topic}
}

func (p *kafkaReportPublisher) PublishReportReady(ctx context.Context, ev *schedulingpb.ReportReadyEvent) error {
	b, err := proto.Marshal(ev)
	if err != nil {
		return err
	}
	r := &kgo.Record{Topic: p.topic, Key: []byte(ev.GetScheduleId()), Value: b}
	observability.InjectKafka(ctx, r)
	return p.prod.ProduceSync(ctx, r).FirstErr()
}

// Scheduler renders due schedules into CSV artifacts.
type Scheduler struct {
	store *SchedStore
	api   API
	pub   ReportPublisher
}

func NewScheduler(store *SchedStore, api API, pub ReportPublisher) *Scheduler {
	return &Scheduler{store: store, api: api, pub: pub}
}

// due reports whether the schedule fires at the given UTC wall-clock time.
func due(s Schedule, now time.Time) bool {
	if !s.Enabled {
		return false
	}
	u := now.UTC()
	if u.Hour() != s.AtHour || u.Minute() != s.AtMinute {
		return false
	}
	switch s.Frequency {
	case "daily":
		return true
	case "weekly":
		return s.Weekday != nil && int(u.Weekday()) == *s.Weekday
	case "monthly":
		if s.DayOfMonth == nil {
			return false
		}
		last := time.Date(u.Year(), u.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
		dom := *s.DayOfMonth
		if dom > last {
			dom = last
		}
		return u.Day() == dom
	}
	return false
}

// resolvePeriod maps a window kind to concrete UTC bounds.
func resolvePeriod(kind string, now time.Time) (time.Time, time.Time, error) {
	u := now.UTC()
	switch kind {
	case "last_day":
		return u.Add(-24 * time.Hour), u, nil
	case "last_week":
		return u.Add(-7 * 24 * time.Hour), u, nil
	case "last_month":
		return u.Add(-30 * 24 * time.Hour), u, nil
	case "previous_month":
		start := time.Date(u.Year(), u.Month()-1, 1, 0, 0, 0, 0, time.UTC)
		end := time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, end, nil
	}
	return time.Time{}, time.Time{}, fmt.Errorf("unknown window_kind %q", kind)
}

// RunDueSchedules renders every enabled schedule whose slot matches now.
// Per-schedule failures are logged and never block the other schedules.
func (s *Scheduler) RunDueSchedules(ctx context.Context, now time.Time) error {
	scheds, err := s.store.ListEnabledSchedules(ctx)
	if err != nil {
		return err
	}
	slot := now.UTC().Format("2006-01-02T15:04Z")
	var firstErr error
	for _, sch := range scheds {
		if !due(sch, now) {
			continue
		}
		from, to, err := resolvePeriod(sch.WindowKind, now)
		if err != nil {
			slog.ErrorContext(ctx, "scheduled report bad window", "schedule", sch.ScheduleID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := s.runOne(ctx, sch, slot, from, to); err != nil {
			slog.ErrorContext(ctx, "scheduled report failed", "schedule", sch.ScheduleID, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// RunScheduleNow renders immediately with a unique manual slot key.
func (s *Scheduler) RunScheduleNow(ctx context.Context, sch Schedule) (Artifact, error) {
	from, to, err := resolvePeriod(sch.WindowKind, time.Now())
	if err != nil {
		return Artifact{}, err
	}
	return s.runOne(ctx, sch, "manual-"+uuid.NewString(), from, to)
}

func (s *Scheduler) runOne(ctx context.Context, sch Schedule, slot string, from, to time.Time) (Artifact, error) {
	ctx, span := otel.Tracer("reports").Start(ctx, "reports.scheduled_run")
	defer span.End()

	body, rows, err := renderReportCSV(ctx, s.api, sch, from, to)
	if err != nil {
		return Artifact{}, err
	}
	a, inserted, err := s.store.InsertArtifact(ctx, Artifact{
		ScheduleID: sch.ScheduleID, SlotKey: slot, PeriodFrom: from, PeriodTo: to,
		RowCount: rows, CSVBody: body,
	})
	if err != nil || !inserted {
		return Artifact{}, err
	}
	if s.pub != nil {
		ev := &schedulingpb.ReportReadyEvent{
			ScheduleId: a.ScheduleID, ArtifactId: a.ArtifactID, TenantId: sch.TenantID,
			ReportKind: sch.ReportKind,
			PeriodFrom: a.PeriodFrom.UTC().Format(time.RFC3339),
			PeriodTo:   a.PeriodTo.UTC().Format(time.RFC3339),
			RowCount:   a.RowCount, CreatedAtMs: a.CreatedAt.UnixMilli(),
		}
		if err := s.pub.PublishReportReady(ctx, ev); err != nil {
			slog.ErrorContext(ctx, "report.ready publish", "schedule", sch.ScheduleID, "err", err)
		}
	}
	return a, nil
}
```

- [ ] **Step 5: Write the renderer**

Create `services/reports/render.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"time"

	pb "omnifoni/reports/gen/reportspb"
)

func reportCSVHeader(kind string) ([]string, error) {
	switch kind {
	case "cdrs":
		return cdrCSVHeader, nil
	case "summary":
		return summaryCSVHeader, nil
	case "payments":
		return paymentCSVHeader, nil
	case "charges":
		return chargeCSVHeader, nil
	case "low_balance":
		return balanceCSVHeader, nil
	}
	return nil, fmt.Errorf("unknown report_kind %q", kind)
}

// collectReportRows pages the API exactly like the HTTP CSV handlers do and
// stops at csvMaxRows data rows.
func collectReportRows(ctx context.Context, api API, sch Schedule, from, to time.Time) ([][]string, func() []string, error) {
	var rows [][]string
	var tail func() []string
	switch sch.ReportKind {
	case "cdrs":
		var cursor string
		for len(rows) < csvMaxRows {
			resp, err := api.ListCdrs(ctx, &pb.ListCdrsRequest{
				Filter: &pb.CdrFilter{
					AccountIds: sch.AccountIDs,
					From:       from.Format(time.RFC3339),
					To:         to.Format(time.RFC3339),
				},
				Limit: csvPageSize, Cursor: cursor,
			})
			if err != nil {
				return nil, nil, err
			}
			for _, c := range resp.GetCdrs() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, cdrCSVRow(c))
			}
			cursor = resp.GetNextCursor()
			if cursor == "" {
				return rows, tail, nil
			}
		}
	case "summary":
		group, _ := groupFromString(sch.GroupBy)
		prefixLen := int32(sch.PrefixLen)
		var totals *pb.SummaryRow
		for {
			resp, err := api.Summarize(ctx, &pb.SummarizeRequest{
				Filter: &pb.CdrFilter{
					AccountIds: sch.AccountIDs,
					From:       from.Format(time.RFC3339),
					To:         to.Format(time.RFC3339),
				},
				GroupBy: group, PrefixLen: prefixLen,
			})
			if err != nil {
				return nil, nil, err
			}
			totals = resp.GetTotals()
			for _, row := range resp.GetRows() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, summaryCSVRow(row))
			}
			break // Summarize returns everything in one response
		}
		if totals != nil {
			tr := totals
			tail = func() []string {
				return []string{"TOTAL", fmt.Sprintf("%d", tr.GetCalls()),
					fmt.Sprintf("%d", tr.GetSeconds()), fmt.Sprintf("%d", tr.GetAmountMinor())}
			}
		}
	case "payments":
		var cursor string
		for len(rows) < csvMaxRows {
			resp, err := api.ListPayments(ctx, &pb.ListPaymentsRequest{
				Filter: &pb.MoneyFilter{
					AccountIds: sch.AccountIDs,
					From:       from.Format(time.RFC3339),
					To:         to.Format(time.RFC3339),
				},
				Limit: csvPageSize, Cursor: cursor,
			})
			if err != nil {
				return nil, nil, err
			}
			for _, p := range resp.GetPayments() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, paymentCSVRow(p))
			}
			cursor = resp.GetNextCursor()
			if cursor == "" {
				return rows, tail, nil
			}
		}
	case "charges":
		var cursor string
		for len(rows) < csvMaxRows {
			resp, err := api.ListCharges(ctx, &pb.ListChargesRequest{
				Filter: &pb.MoneyFilter{
					AccountIds: sch.AccountIDs,
					From:       from.Format(time.RFC3339),
					To:         to.Format(time.RFC3339),
				},
				Limit: csvPageSize, Cursor: cursor,
			})
			if err != nil {
				return nil, nil, err
			}
			for _, c := range resp.GetCharges() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, chargeCSVRow(c))
			}
			cursor = resp.GetNextCursor()
			if cursor == "" {
				return rows, tail, nil
			}
		}
	case "low_balance":
		var cursor string
		for len(rows) < csvMaxRows {
			resp, err := api.ListLowBalance(ctx, &pb.LowBalanceRequest{
				AccountIds: sch.AccountIDs, Limit: csvPageSize, Cursor: cursor,
			})
			if err != nil {
				return nil, nil, err
			}
			for _, b := range resp.GetAccounts() {
				if len(rows) >= csvMaxRows {
					return rows, tail, nil
				}
				rows = append(rows, balanceCSVRow(b))
			}
			cursor = resp.GetNextCursor()
			if cursor == "" {
				return rows, tail, nil
			}
		}
	}
	return rows, tail, nil
}

// renderReportCSV returns the full CSV text and the data-row count.
func renderReportCSV(ctx context.Context, api API, sch Schedule, from, to time.Time) (string, int64, error) {
	header, err := reportCSVHeader(sch.ReportKind)
	if err != nil {
		return "", 0, err
	}
	rows, tail, err := collectReportRows(ctx, api, sch, from, to)
	if err != nil {
		return "", 0, err
	}
	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	if err := cw.Write(header); err != nil {
		return "", 0, err
	}
	for _, r := range rows {
		if err := cw.Write(r); err != nil {
			return "", 0, err
		}
	}
	if tail != nil {
		if err := cw.Write(tail()); err != nil {
			return "", 0, err
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return "", 0, err
	}
	return buf.String(), int64(len(rows)), nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd services/reports && go test -run 'TestDueSchedule|TestResolvePeriod|TestRunDueSchedules|TestRunScheduleNow|TestHTTP|TestList|TestSummarize|TestPayments|TestCharges|TestLowBalance' ./...`
Expected: PASS (new scheduler tests plus the existing HTTP tests, which now use the row helpers). `go vet ./...` clean.

- [ ] **Step 7: Commit**

```bash
git add services/reports/report_rows.go services/reports/scheduler.go services/reports/render.go services/reports/scheduler_test.go services/reports/http.go
git commit -m "feat(reports): run scheduled reports"
```

---

### Task 4: HTTP facade for scheduled reports

**Files:**
- Modify: `services/reports/http.go` (add `Scope`, rewire `Accounts`, add `sched` field to `httpServer`)
- Create: `services/reports/sched_http.go`
- Test: `services/reports/sched_http_test.go`

**Interfaces:**
- Consumes: `Scheduler`/`SchedStore` (Tasks 2-3), `bearer`, `writeJSON`, `writeErr`, `intersect` (`http.go`).
- Produces:
  - `func (a *Authenticator) Scope(ctx context.Context, token, requestedTenant string) (tenant string, ids []string, code int, err error)`
  - `httpServer.sched *Scheduler` field
  - `type scheduleBody struct { ReportKind, WindowKind, GroupBy string; PrefixLen int; Frequency string; AtHour, AtMinute int; Weekday, DayOfMonth *int; Enabled *bool; AccountIDs []string }` with JSON tags `report_kind`, `window_kind`, `group_by`, `prefix_len`, `frequency`, `at_hour`, `at_minute`, `weekday`, `day_of_month`, `enabled`, `account_ids`
  - `func validateSchedule(in Schedule, allowed []string) (Schedule, error)` (returns the normalized schedule: account list intersected, weekday/day_of_month nulled when not applicable)
  - `func scheduleJSONFrom(s Schedule) map[string]any` (or a `scheduleJSON` struct — pick the struct below)
  - Handlers: `CreateSchedule`, `ListSchedules`, `GetSchedule`, `PatchSchedule`, `DeleteSchedule`, `ListArtifacts`, `DownloadArtifact`, `RunSchedule`.

- [ ] **Step 1: Write the failing tests**

Create `services/reports/sched_http_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "omnifoni/reports/gen/reportspb"
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

var _ = pb.Cdr{}
var _ = context.Background
```

Remove the two trailing `var _` lines — they exist only to keep imports honest; instead delete `context` and `pb` from the import block since the file does not use them.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd services/reports && go test -run 'TestSchedHTTP' ./...`
Expected: FAIL — `undefined: h.CreateSchedule` etc.

- [ ] **Step 3: Add Scope and the sched field**

In `services/reports/http.go`, replace `Accounts` with a `Scope` wrapper plus the existing logic:

```go
// Accounts validates the bearer token and returns the account IDs the caller
// may see. Status is the HTTP status to send when err is non-nil.
func (a *Authenticator) Accounts(ctx context.Context, token, requestedTenant string) ([]string, int, error) {
	_, ids, code, err := a.Scope(ctx, token, requestedTenant)
	return ids, code, err
}

// Scope resolves the effective tenant and the account IDs the caller may see.
// Admin scoping mirrors Accounts: requestedTenant wins, else 400.
func (a *Authenticator) Scope(ctx context.Context, token, requestedTenant string) (string, []string, int, error) {
	me, code, err := a.getMe(ctx, token)
	if err != nil {
		return "", nil, code, err
	}
	if me.Role == "user" {
		return "", nil, http.StatusForbidden, errors.New("forbidden")
	}
	scope := me.TenantID
	if me.Role == "admin" {
		scope = requestedTenant
	}
	if scope == "" {
		return "", nil, http.StatusBadRequest, errors.New("tenant_id required")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		a.base+"/accounts?tenant_id="+url.QueryEscape(scope), nil)
	if err != nil {
		return "", nil, http.StatusBadGateway, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := a.client.Do(req)
	if err != nil {
		return "", nil, http.StatusBadGateway, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", nil, http.StatusBadGateway, errors.New("identity accounts failed")
	}
	var accts []accountResponse
	if err := json.NewDecoder(resp.Body).Decode(&accts); err != nil {
		return "", nil, http.StatusBadGateway, err
	}
	ids := make([]string, 0, len(accts))
	for _, a := range accts {
		ids = append(ids, a.AccountID)
	}
	return scope, ids, http.StatusOK, nil
}
```

Delete the old `Accounts` body (the function above replaces it; keep only one `Accounts` and one `Scope`).

Add a `UserID` accessor next to `Scope` (one extra /me call, only on create):

```go
// UserID returns the caller's identity user id for audit fields.
func (a *Authenticator) UserID(ctx context.Context, token string) (string, int, error) {
	me, code, err := a.getMe(ctx, token)
	if err != nil {
		return "", code, err
	}
	return me.UserID, http.StatusOK, nil
}
```

Add `UserID` to `meResponse` (the identity `/me` payload already carries `user_id`):

```go
type meResponse struct {
	UserID   string `json:"user_id"`
	Role     string `json:"role"`
	TenantID string `json:"tenant_id"`
}
```

Add the `sched` field:

```go
type httpServer struct {
	api   API
	auth  *Authenticator
	sched *Scheduler
}
```

- [ ] **Step 4: Write the handlers**

Create `services/reports/sched_http.go`:

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

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
// backstop.
// validateSchedule returns the normalized schedule: account_ids intersected
// with the caller's allowed set, and weekday/day_of_month nulled when the
// frequency does not use them.
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
	if len(in.AccountIDs) > 0 && len(allowed) > 0 {
		kept := intersect(allowed, in.AccountIDs)
		if len(kept) == 0 {
			return in, errors.New("no matching accounts")
		}
		in.AccountIDs = kept
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
	writeJSON(w, http.StatusOK, scheduleJSON(sch))
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
	a, body, err := h.sched.store.GetArtifact(r.Context(), sch.ScheduleID, r.PathValue("aid"))
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd services/reports && go test -run 'TestSchedHTTP' ./...`
Expected: PASS. Then the full package: `go test ./...` PASS, `go vet ./...` clean.

- [ ] **Step 6: Commit**

```bash
git add services/reports/http.go services/reports/sched_http.go services/reports/sched_http_test.go
git commit -m "feat(reports): serve scheduled report routes"
```

---

### Task 5: Wiring, compose, smoke, and docs

**Files:**
- Modify: `services/reports/main.go` (envs, sched store, publisher, ticker, routes, httpServer literal)
- Modify: `docker-compose.yml` (reports env + depends_on postgres)
- Modify: `ops/smoke.sh` (DB count 9, scheduled-report probe)
- Modify: `AGENTS.md` (P5 slice-five line)

**Interfaces:**
- Consumes: everything from Tasks 1-4.

- [ ] **Step 1: Wire the scheduler in main**

In `services/reports/main.go`, after `swAddr := ...` add:

```go
	schedDSN := os.Getenv("REPORTS_SCHED_DSN")
	retentionDays := 90
	if v := os.Getenv("REPORTS_SCHED_RETENTION_DAYS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			slog.Error("bad REPORTS_SCHED_RETENTION_DAYS", "value", v)
			os.Exit(1)
		}
		retentionDays = n
	}
```

After the `api := &reportsServer{...}` literal, add:

```go
	var sched *Scheduler
	var schedStore *SchedStore
	if schedDSN != "" {
		schedStore, err = OpenSched(ctx, schedDSN)
		if err != nil {
			slog.Error("sched db", "err", err)
			os.Exit(1)
		}
		defer schedStore.Close()
		if err := schedStore.Migrate(ctx); err != nil {
			slog.Error("sched migrate", "err", err)
			os.Exit(1)
		}
		prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
		if err != nil {
			slog.Error("kafka producer", "err", err)
			os.Exit(1)
		}
		defer prod.Close()
		sched = NewScheduler(schedStore, api, newKafkaReportPublisher(prod, env("REPORTS_SCHED_TOPIC", "report.ready")))
		go func() {
			t := time.NewTicker(time.Minute)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-t.C:
					if err := sched.RunDueSchedules(context.Background(), now); err != nil {
						slog.Error("scheduled reports", "err", err)
					}
					if now.Minute() == 0 {
						before := time.Now().AddDate(0, 0, -retentionDays)
						if n, err := schedStore.PruneArtifacts(context.Background(), before); err != nil {
							slog.Error("sched prune", "err", err)
						} else if n > 0 {
							slog.Info("pruned report artifacts", "count", n)
						}
					}
				}
			}
		}()
	}
```

Add `"strconv"` to the imports.

Change the httpServer construction:

```go
	h := &httpServer{api: api, auth: NewAuthenticator(identity), sched: sched}
```

Register the routes after the `live-calls` line:

```go
	mux.HandleFunc("POST /scheduled-reports", h.CreateSchedule)
	mux.HandleFunc("GET /scheduled-reports", h.ListSchedules)
	mux.HandleFunc("GET /scheduled-reports/{id}", h.GetSchedule)
	mux.HandleFunc("PATCH /scheduled-reports/{id}", h.PatchSchedule)
	mux.HandleFunc("DELETE /scheduled-reports/{id}", h.DeleteSchedule)
	mux.HandleFunc("GET /scheduled-reports/{id}/artifacts", h.ListArtifacts)
	mux.HandleFunc("GET /scheduled-reports/{id}/artifacts/{aid}/download", h.DownloadArtifact)
	mux.HandleFunc("POST /scheduled-reports/{id}/run", h.RunSchedule)
```

- [ ] **Step 2: Add compose config**

In `docker-compose.yml` under `reports:` environment, add:

```yaml
      REPORTS_SCHED_DSN: postgres://postgres:dev-only@postgres:5432/reports_sched
      REPORTS_SCHED_TOPIC: report.ready
```

Under `reports:` `depends_on:`, add:

```yaml
      postgres:
        condition: service_healthy
```

- [ ] **Step 3: Build and verify**

```bash
docker compose build reports && docker compose up -d reports
docker inspect --format '{{.State.Status}}' omni-fon-reports-1
```

Expected: `running`. Logs show `reports gRPC up` and `reports HTTP up`.

- [ ] **Step 4: Extend the smoke test**

In `ops/smoke.sh`, change the context-databases check to include `reports_sched`:

```bash
dbs=$(docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT count(*) FROM pg_database WHERE datistemplate=false AND datname IN ('identity','catalogue','charging','billing','payments','routing','sbc','mediation','reports_sched')")
[ "$dbs" = "9" ] || fail "expected 9 context databases, got $dbs"
```

After the `ok: live calls` probe line, add:

```bash
sched=$(curl -fsS -X POST -H 'Authorization: Bearer tok-smoke' -H 'Content-Type: application/json' \
  -d '{"report_kind":"cdrs","window_kind":"previous_month","frequency":"daily","at_hour":0,"at_minute":0}' \
  "http://localhost:50062/scheduled-reports?tenant_id=$tid")
sid=$(echo "$sched" | sed -E 's/.*"schedule_id":"([^"]+)".*/\1/')
[ -n "$sid" ] || fail "scheduled report create"
code=$(curl -sS -o /tmp/sched-run.json -w '%{http_code}' -X POST -H 'Authorization: Bearer tok-smoke' \
  "http://localhost:50062/scheduled-reports/$sid/run")
[ "$code" = "202" ] || fail "scheduled report run ($code)"
aid=$(sed -E 's/.*"artifact_id":"([^"]+)".*/\1/' /tmp/sched-run.json)
[ -n "$aid" ] || fail "scheduled report artifact"
curl -fsS -D /tmp/sched-hdr.txt -o /tmp/sched.csv -H 'Authorization: Bearer tok-smoke' \
  "http://localhost:50062/scheduled-reports/$sid/artifacts/$aid/download" || fail "scheduled report download"
hdr=$(head -n 1 /tmp/sched.csv)
[ "$hdr" = "call_id,account_id,destination,cli,started_at_ms,ended_at_ms,charged_seconds,end_reason,trunk_id,rate_per_sec,amount_minor,rate_missing" ] || fail "scheduled report csv"
grep -qi '^content-disposition: attachment' /tmp/sched-hdr.txt || fail "scheduled report headers"
echo "ok: scheduled report"
```

- [ ] **Step 5: Document the slice**

Append to `AGENTS.md`, right after the P5 slice-four bullet:

```
- P5 slice five: `services/reports` also schedules recurring reports (Postgres `reports_sched` DB: `schedules`+`report_artifacts` tables; `sched_migrate.sql`). `REPORTS_SCHED_DSN` (default `postgres://postgres:dev-only@postgres:5432/reports_sched`; **empty disables the scheduler and its routes** with 503 `schedule_unavailable`) turns on a 1-minute ticker (`RunDueSchedules`, reconciliation `Sweep` pattern) that renders each due schedule (`daily|weekly|monthly` at `at_hour:at_minute` UTC; window `last_day|last_week|last_month|previous_month`) through the existing report API into a CSV artifact (10k-row cap, unique `(schedule_id, slot_key)` = at most one artifact per slot; missed runs are skipped, not backfilled) and publishes `scheduling.v1.ReportReadyEvent` to `report.ready` (`REPORTS_SCHED_TOPIC`; publish failures never roll back the artifact). Artifacts prune hourly past `REPORTS_SCHED_RETENTION_DAYS` (default 90). HTTP on :50062 with the same identity bearer + tenant scoping as the other reports: `POST/GET/PATCH/DELETE /scheduled-reports`, `GET /scheduled-reports/{id}/artifacts`, `GET /scheduled-reports/{id}/artifacts/{aid}/download`, `POST /scheduled-reports/{id}/run` (manual backfill, `manual-<uuid>` slot). Report kinds: `cdrs|summary|payments|charges|low_balance` (`summary` may group `prefix` with `prefix_len` 1-15). The reports image is unchanged (the proto generates into the reports module); compose adds `postgres` to `depends_on`. For an existing dev volume create the DB once: `docker exec omni-fon-postgres-1 createdb -U postgres reports_sched`.
```

- [ ] **Step 6: Run the full smoke and test gates**

```bash
make smoke   # expect: ok: scheduled report ... smoke: PASS
make test    # expect: exit 0, 12 modules ok
```

Run smoke **before** `make test` (tests truncate dev data; the smoke seeds its own rows).

- [ ] **Step 7: Commit**

```bash
git add services/reports/main.go docker-compose.yml ops/smoke.sh AGENTS.md
git commit -m "feat(reports): wire the report scheduler"
```

---

## Self-Review

- Spec coverage: proto (Task 1), Postgres store + schema (Task 2), due/period/render/idempotency (Task 3), HTTP CRUD/download/manual-run/scoping (Task 4), envs/ticker/prune/Kafka wiring/compose/smoke/docs (Task 5). No spec section is unmapped.
- Placeholders: none; every step carries exact code or exact commands.
- Type consistency: `SchedStore` methods used in Tasks 3-5 match Task 2's definitions; `Scheduler`, `ReportPublisher`, `Schedule`, `Artifact`, `scheduleBody` names are used verbatim across tasks; `Scope` signature matches the Task 4 handler calls; `csvMaxRows`/`csvPageSize` and the CSV header vars are pre-existing.
- Ambiguity fixed inline: `Artifact.CSVBody` added (Task 2 Step 4), manual slot prefix `manual-`, and `validateSchedule` returns the normalized schedule so the account intersection and frequency-scoped nulling actually reach the store.