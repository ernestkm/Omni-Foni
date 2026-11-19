# P5 notifications — design

Date: 2026-11-19
Status: approved (design)
Slice: P5 reporting and PBX, first notifications slice

## 1. Context

The master plan lists notifications as a bounded context with email, SMS, push,
alarms, and a PostgreSQL outbox, and P5 acceptance includes notification. The
scheduled-reports slice already publishes `scheduling.v1.ReportReadyEvent` to
`report.ready`, but no service consumes it. Reports artifacts are downloadable
only through an authenticated reports HTTP route, so an email cannot currently
link directly to the generated CSV.

This slice delivers one complete vertical path: a scheduled report artifact
produces one durable email notification with a safe, expiring download link.
It creates a dedicated notifications service and a local Mailpit sink so the
whole path is testable in the dev stack.

Goals:

- Consume `report.ready` without losing events on service or SMTP failure.
- Send one email per distinct artifact and recipient, even under Kafka replay.
- Give recipients a direct, expiring link to the exact CSV artifact.
- Keep notification delivery outside reports and identity.

Non-goals:

- Fraud alerts, reconciliation drift, balance alerts, rate updates, invoices.
- SMS, push, webhook, or third-party email providers.
- User-facing notification preferences, history, retry, or CRUD APIs.
- PDF reports, report backfill, and changes to report rendering.
- Per-tenant SMTP credentials and reseller SMTP configuration.

Feature claims come only from `docs/master-plan.md` sections 1.1, 5, and 7. No
ASTPP source, schema, or licensed asset was consulted.

## 2. Architecture

```
reports scheduler
  -> report.ready (ReportReadyEvent + recipient + signed URL)
  -> services/notifications consumer
  -> notifications.outbox (Postgres, deduped by artifact + recipient)
  -> SMTP worker -> Mailpit (dev)
  -> recipient opens signed reports download link
```

`services/reports` remains the owner of schedules, artifacts, and download
authorization. It publishes the creator's email and a signed URL in the event.
`services/notifications` owns only delivery: Kafka consumption, outbox rows,
retries, and SMTP. Kafka offsets are committed only after the outbox insert
succeeds. There is no user-facing notifications API.

Both services use the shared `omnifoni/observability` module: `SetupLogging`,
`Init`, `ServerOptions`/`ClientOptions` where applicable, `Middleware`, Kafka
`ExtractKafka`, and fail-open telemetry. Email addresses are redacted by the
existing key-based log redaction.

## 3. Event contract

Extend `proto/scheduling/v1/scheduling.proto`:

```proto
message ReportReadyEvent {
  string schedule_id    = 1;
  string artifact_id    = 2;
  string tenant_id      = 3;
  string report_kind    = 4;
  string period_from    = 5;
  string period_to      = 6;
  int64  row_count      = 7;
  int64  created_at_ms  = 8;
  string recipient_email = 9;
  string artifact_url   = 10;
  int64  expires_at_ms  = 11;
}
```

The new fields are required for delivery. A consumer that receives an event
with an empty recipient or a malformed URL records a terminal validation
failure rather than retrying forever. Existing events without these fields are
treated as terminal validation failures; this is acceptable because the only
publisher is the reports scheduler and the dev smoke creates fresh schedules.

Regenerate all protobufs with `make protoc`; never hand-edit generated files.

## 4. Reports changes

### 4.1 Recipient capture

Add `recipient_email text NOT NULL DEFAULT ''` to `schedules` in
`services/reports/sched_migrate.sql`, using `ADD COLUMN IF NOT EXISTS` for
existing volumes so dev databases upgrade in place.

`POST /scheduled-reports` captures the authenticated identity `/me` email for
the requesting user and stores it as the schedule's recipient. `recipient_email`
is returned read-only in schedule JSON. PATCH does not change it. A schedule
created before this field existed keeps an empty recipient; when it renders,
the published event has an empty recipient and notifications records a terminal
validation failure with a warning log.

Implementation note: the reports identity client currently exposes only user
ID, role, and tenant from the auth helper. The create path will call `/me` (or
extend that helper to return email) so the recipient comes from trusted identity
data, not a caller-supplied body field.

### 4.2 Signed artifact URL

Add an expiring HMAC-SHA256 download signature to the existing artifact download
route. The URL is:

```
<REPORTS_PUBLIC_BASE_URL>/scheduled-reports/<schedule_id>/artifacts/<artifact_id>/download?exp=<expires_at_ms>&sig=<base64url>
```

The signature input is the exact string
`<schedule_id>|<artifact_id>|<expires_at_ms>`, signed with
`REPORTS_ARTIFACT_SIGNING_SECRET`. Comparison uses constant-time equality.
The route accepts either the existing identity bearer token or a valid,
unexpired signature; a valid signature authorizes only that schedule/artifact
pair. Invalid or expired signatures return 403 with the existing error shape.

New reports envs:

- `REPORTS_ARTIFACT_SIGNING_SECRET` — dev default `dev-only`.
- `REPORTS_SIGNED_URL_TTL` — milliseconds, default `86400000` (24h).
- `REPORTS_PUBLIC_BASE_URL` — default `http://localhost:50062` so Mailpit links
  work from a host browser.

The scheduler builds the URL at publish time and sets `expires_at_ms` in the
event. Publish failure remains non-transactional, as in the current scheduler.

## 5. Notifications service

New module `services/notifications` (`omnifoni/notifications`) with:

- `main.go` — signal context, observability setup, Postgres pool, Kafka
  consumer, outbox store, SMTP sender, and worker goroutines.
- `consumer.go` — `franz-go` consumer on `report.ready`, group `notifications`,
  manual commit after a successful outbox insert.
- `outbox.go` — idempotent insert, claim/due query, attempt recording, terminal
  failure marking.
- `mailer.go` — `net/smtp` delivery with a plain-text email body and the signed
  link.
- `migrate.sql` — idempotent outbox schema.
- No gRPC or user-facing HTTP server; only `/healthz` is served for probes.

Outbox schema:

```
notification_id uuid primary key
dedupe_key text not null unique
channel text not null check (channel = 'email')
recipient text not null
subject text not null
body text not null
status text not null check (status in ('pending','sent','failed'))
attempts integer not null default 0
next_attempt_at timestamptz not null default now()
last_error text not null default ''
created_at timestamptz not null default now()
sent_at timestamptz
```

`dedupe_key` is `report-ready:<artifact_id>:<recipient_email>`. A duplicate
insert leaves the existing row untouched and is treated as success, so Kafka
replay cannot send duplicate mail.

The worker claims due pending rows with `FOR UPDATE SKIP LOCKED`, bounded by a
configurable batch size. On SMTP success it marks `sent` and sets `sent_at`. On
failure it increments `attempts`, sets `last_error`, and either schedules the
next attempt with exponential backoff or marks `failed` at
`NOTIFICATIONS_MAX_ATTEMPTS`. Validation failures are terminal on first sight.

New envs:

- `NOTIFICATIONS_DSN` — default
  `postgres://postgres:dev-only@postgres:5432/notifications`.
- `NOTIFICATIONS_KAFKA` — default `kafka:9092`.
- `NOTIFICATIONS_TOPIC_REPORT_READY` — default `report.ready`.
- `NOTIFICATIONS_GROUP` — default `notifications`.
- `NOTIFICATIONS_SMTP_ADDR` — default `mailpit:1025`.
- `NOTIFICATIONS_SMTP_FROM` — default `omnifoni@example.test`.
- `NOTIFICATIONS_MAX_ATTEMPTS` — default `5`.
- `NOTIFICATIONS_POLL_INTERVAL` — default `1s`.
- `NOTIFICATIONS_BATCH_SIZE` — default `32`.
- `NOTIFICATIONS_ADDR` — default `:50064` for the health endpoint.

Compose adds `notifications` with `postgres`, `kafka`, and `mailpit`
dependencies, adds `mailpit` with its SMTP port and UI exposed on localhost
8025, and creates the `notifications` database in `ops/postgres-init.sql`.
`Makefile:test` gains the notifications module. Smoke's hardcoded DB,
container, and Jaeger lists gain the new service; no new smoke CLI is required
because Mailpit's HTTP API can assert delivered mail.

## 6. Error handling and delivery semantics

- Kafka consume failure before outbox commit: retry the record; do not commit.
- Outbox insert failure: retry the record; do not commit.
- Duplicate event: unique-key no-op; commit.
- SMTP failure: bounded exponential retry; terminal `failed` after max attempts.
- Missing recipient, malformed URL, or unknown channel: terminal validation
  failure, logged with redacted identifiers.
- Invalid/expired signed download URL: reports returns 403; notifications never
  generates URLs itself.
- Telemetry and log export failures never fail a notification or an RPC.

## 7. Tests and verification

Unit tests:

- HMAC signature generation and verification, including expiry and tampering.
- Event-to-outbox mapping, dedupe key, terminal validation paths, retry backoff,
  and transition to sent/failed.
- Consumer ordering: offset commit is not called before a successful insert.
- Reports schedule creation stores the identity email and PATCH preserves it.

Integration/smoke:

- `make test` includes the notifications module.
- `make smoke` creates a scheduled report, runs it, waits for Mailpit to show
  one message for the artifact, and follows the signed link to a CSV response.
- `bash ops/sip-e2e.sh` remains unchanged.
- `go vet`, `gofmt -l`, `bash -n` on scripts, and `docker compose config -q`
  are part of the final gate.

Verification claims will be recorded only after running the commands; this
design does not assume a green gate.

## 8. Scope boundary review

This design intentionally leaves fraud alerts, reconciliation drift, balance
alerts, rate notifications, invoices, SMS/push/webhook channels, user
preferences/history/retry APIs, and per-tenant SMTP settings to later slices.
Those additions should reuse the outbox and mailer boundaries defined here.
