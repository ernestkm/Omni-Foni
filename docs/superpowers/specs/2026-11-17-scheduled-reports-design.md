# Scheduled reports — design

Date: 2026-11-17
Status: approved (design)
Slice: P5 (reports), following live-call monitoring (P5 slice four)

## 1. Context

P5 parity items still open after live calls: automated reports, audit log, PDF,
async export, dashboards, country/product-usage/activity/email-history reports.
"Automated reports" (master-plan section 1.1) is a listed add-on with no
detailed vendor spec, so this design defines a minimal, self-contained
scheduler that reuses everything Reports already has: the ClickHouse CDR store,
the money mirrors, the tenant-scoped facade, and the CSV writers.

Goals:

- A tenant-scoped operator can create schedules that render a stored report
  kind on a recurring UTC schedule.
- Each run produces a downloadable CSV artifact with a stable identity.
- Runs are idempotent per schedule slot; retries after crashes do not
  duplicate artifacts.
- Completed runs emit a `report.ready` Kafka event so notifications,
  downstream consumers, and tracing can build on top later.

Non-goals (YAGNI): email/SMTP delivery, PDF artifacts, cron expressions,
backfill of missed runs, per-user notification preferences, reseller
sub-reseller commission on reports.

## 2. Architecture

The scheduler lives inside `services/reports` because that service owns both
the data and the renderers. No new service, no new container.

Components:

- `services/reports/sched_store.go` — pgx pool against Postgres DB
  `reports_sched` (created by `ops/postgres-init.sql`). CRUD for `schedules`
  and `report_artifacts`.
- `services/reports/scheduler.go` — `RunDueSchedules(ctx, now)` plus the
  minute ticker wired in `main.go` (the `services/reconciliation` sweep
  pattern). Sequential execution is sufficient at this scale.
- `services/reports/sched_http.go` — handlers on the existing `:50062` mux.
- `services/reports/scheduler_test.go`, `sched_http_test.go`,
  `sched_store_test.go` — tests (section 8).
- `proto/scheduling/v1/scheduling.proto` — `ReportReadyEvent` (section 7).

Configuration (all new envs):

- `REPORTS_SCHED_DSN` — dev default
  `postgres://postgres:dev-only@postgres:5432/reports_sched`. **Unset or
  empty disables the scheduler and its endpoints** (they answer
  `503 schedule_unavailable`), so deployments that do not want it need no
  changes.
- `REPORTS_SCHED_TOPIC` — default `report.ready`.
- `REPORTS_SCHED_RETENTION_DAYS` — default 90.

Rendering reuses the facade internals (the same query path the HTTP handlers
call), so CSV semantics are not duplicated. Artifacts are always CSV and obey
the existing 10,000-data-row CSV cap.

Compose: `reports` gains `depends_on: postgres` (service already exists; no
new container).

## 3. Data model

Database `reports_sched`, migrations are `CREATE TABLE IF NOT EXISTS` only
(project convention):

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

Field semantics:

- `account_ids` empty = all accounts of `tenant_id` (same convention as the
  reports HTTP facade). At most 100 entries.
- `group_by`/`prefix_len` apply only to `summary`; other report kinds must
  use `group_by='none'` (validated at the API).
- `weekday` is required for `weekly` (0=Sunday .. 6=Saturday), `NULL` otherwise.
- `day_of_month` is required for `monthly` (1..31; a 31st in a short month
  fires on the last day), `NULL` otherwise.
- `slot_key` is the UTC minute of the fire, `YYYY-MM-DDTHH:MMZ`. The unique
  constraint makes each slot fire at most once per schedule.
- `csv_body` holds the full CSV text (headers included), capped by the
  existing CSV row cap.

## 4. Scheduler runtime

`RunDueSchedules(ctx, now)` (directly callable from tests, reconciliation
`Sweep` pattern):

1. Load enabled schedules.
2. Compute `slot_key` from `now.UTC()`.
3. Filter by `at_hour`/`at_minute` equality, then weekday (weekly) or
   day-of-month with month-length clamp (monthly).
4. Resolve the period from `window_kind`:
   - `last_day` — trailing 24 hours ending at `now`.
   - `last_week` — trailing 7 days.
   - `last_month` — trailing 30 days.
   - `previous_month` — first instant of the previous calendar month (UTC) to
     the first instant of the current month.
5. Render CSV via the same internal path as the HTTP handlers, with the
   schedule's `tenant_id` and `account_ids`.
6. Insert the artifact (conflict on `(schedule_id, slot_key)` = already ran,
   skip), then publish `ReportReadyEvent`.

Error handling: a render/insert failure for one schedule is logged and
skipped; it never blocks the other schedules. Because the artifact is
missing, the next minute's tick retries the same slot — at-least-once per
slot, deduplicated by the unique constraint. Missed runs while the process is
down are skipped, never backfilled.

Manual trigger: `POST /scheduled-reports/{id}/run` executes one render
immediately with a synthetic slot key (`manual-<uuid>`), ignoring the clock.
It is the ops/smoke backfill path.

Retention: on the same ticker, at minute 0 of each hour, delete artifacts
older than `REPORTS_SCHED_RETENTION_DAYS` (default 90). Deleted artifacts 404
on download.

Concurrency: the ticker is a single goroutine; no internal locking beyond the
store. Artifacts from concurrent manual runs are safe because each gets a
unique `manual-<uuid>` slot key.

## 5. HTTP API

Same identity bearer + tenant scoping as every other reports endpoint
(`services/reports/http.go` facade):

- Auth: missing/invalid bearer 401. Role `user` 403 on every route.
- Reseller is forced to their own tenant; admin must pass `tenant_id` on
  create (400 without it). All id-scoped routes 404 for schedules/artifacts
  of another tenant.

Routes (on `:50062`):

| Method | Path | Meaning |
|---|---|---|
| POST | `/scheduled-reports` | create → 201 schedule |
| GET | `/scheduled-reports` | list (tenant-scoped, newest first) |
| GET | `/scheduled-reports/{id}` | read one |
| PATCH | `/scheduled-reports/{id}` | full update (immutable: schedule_id, tenant_id, created_by, created_at) |
| DELETE | `/scheduled-reports/{id}` | delete (artifacts cascade) → 204 |
| GET | `/scheduled-reports/{id}/artifacts` | artifact metadata, newest first |
| GET | `/scheduled-reports/{id}/artifacts/{artifact_id}/download` | CSV attachment |
| POST | `/scheduled-reports/{id}/run` | manual trigger → 202 `{artifact_id}` |

Create/update body: `{report_kind, window_kind, frequency, at_hour, at_minute,
weekday?, day_of_month?, prefix_len?, group_by?, account_ids?}`.

Validation (400, error body `{"error": "..."}` consistent with the existing
facade): enums as in section 3; `at_hour` 0..23, `at_minute` 0..59;
`weekday` required 0..6 for weekly; `day_of_month` required 1..31 for
monthly; `prefix_len` 1..15 and `group_by='prefix'` only for `summary`;
`account_ids` at most 100 entries.

Download response headers: `Content-Type: text/csv; charset=utf-8`,
`Content-Disposition: attachment; filename="reports-scheduled-<schedule_id>-<slot_key>.csv"`.
When the scheduler DB is unreachable the schedule routes answer
`503 schedule_unavailable` (fail closed, like the other source outages).

## 6. Event

`ReportReadyEvent` (proto `scheduling.v1`):

```proto
message ReportReadyEvent {
  string schedule_id  = 1;
  string artifact_id  = 2;
  string tenant_id    = 3;
  string report_kind  = 4;
  string period_from  = 5;  // RFC3339 UTC
  string period_to    = 6;
  int64  row_count    = 7;
  int64  created_at_ms = 8;
}
```

- Topic `report.ready`, message key `schedule_id`.
- Published with franz-go after the artifact insert commits; `InjectKafka`
  trace propagation (observability module), same wiring as
  mediation/switching producers.
- A publish failure is logged and does not roll back the artifact: the event
  is notification, not storage.

## 7. Configuration summary

| Env | Default | Effect |
|---|---|---|
| `REPORTS_SCHED_DSN` | `postgres://postgres:dev-only@postgres:5432/reports_sched` | scheduler + schedule routes; empty disables |
| `REPORTS_SCHED_TOPIC` | `report.ready` | event topic |
| `REPORTS_SCHED_RETENTION_DAYS` | `90` | artifact retention |

## 8. Testing

- Unit, table-driven: due-slot computation (daily/weekly/monthly, month
  clamp, UTC minute boundaries), request validation, period resolution per
  window kind, tenant-scoping helpers.
- Store integration against dev Postgres (truncate-first convention):
  CRUD, unique-slot idempotency, cascade delete, prune cutoff.
- Facade tests (`facade(t)` + `fakeIdentity`): 401/403/400/404 matrix,
  create/list/update/delete shapes, download headers, manual-run with a
  stubbed runner.
- Scheduler tests: `RunDueSchedules` with fixed `now` → exactly one artifact
  and one event; same slot again → zero new artifacts; disabled schedule
  skipped; one failing render does not stop others; prune removes only old
  rows.
- E2E (stack): invoke `RunDueSchedules` directly and consume `report.ready`
  from Kafka, asserting fields (reconciliation `TestSweepPublishesDriftEvent`
  pattern).
- Smoke (`ops/smoke.sh`): create a schedule in the `smoke` tenant over HTTP,
  `POST .../run`, poll `/artifacts`, download, assert the CSV header, print
  `ok: scheduled report`.

## 9. Risks and accepted decisions

- Reports gains a second datastore (Postgres). Conventional: eight services
  already use pgx; the scheduler stays optional via empty DSN.
- Missed runs are skipped, not backfilled. Manual run covers backfills.
- The 10k CSV row cap applies to artifacts exactly as on-demand reports.
- Accounts re-homed between tenants after creation render empty rather than
  failing; schedules are not re-validated per run.
- New proto module adds a `make protoc` target and a `gen/` tree; no new
  service, no new container.
- The reports image must be able to reach Postgres; compose already runs it.