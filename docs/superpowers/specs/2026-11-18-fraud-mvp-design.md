# Toll-fraud MVP — design

Date: 2026-11-18
Status: approved (design)
Slice: P6 (security and compliance hardening), first slice

## 1. Context

P6 begins security and compliance hardening. The master plan's verified feature
inventory lists fraud detection and toll-fraud threshold alerting, and its
security architecture requires that model output be advisory while
deterministic rules enforce. No fraud component exists yet.

This slice builds a minimal, testable toll-fraud MVP on the data Omni-Foní
already produces: call attempts from SBC/Switching and normalized CDRs from
mediation. It deliberately avoids an ML training pipeline, portal UI, spend-cap
integration, and deployment isolation work; those remain separate P6 slices.

Goals:

- Detect likely toll-fraud patterns with an explainable, versioned score.
- Enforce only explicit deterministic prefix/CLI denylists and velocity caps.
- Emit auditable alerts to Kafka and a queryable Postgres ledger.
- Publish a reproducible precision/recall baseline from labelled synthetic
  replay data.

Non-goals (YAGNI):

- Call blocking from the advisory score.
- Daily spend ceilings computed from OCS rates (needs post-Authorize wiring).
- Admin UI, alert acknowledgement workflow, email/SMS notification.
- Trained model artefacts, model registry, LLM usage.
- gVisor runtime rollout, POPIA control matrix, penetration test.

Feature claims come only from `docs/master-plan.md` sections 1.1, 5, 6, and 7.
No ASTPP source, schema, or licensed asset was consulted.

## 2. Architecture

New `services/fraud` gRPC service on `:50063`, backed by a new Postgres `fraud`
database. It owns features, scoring, hard-rule evaluation, alerts, and the
replay CLI. SBC consults it during `CheckCall`; the score is advisory, and only
configured hard rules deny calls.

Components:

- `services/fraud/fraud.go` — gRPC server: `CheckCall`, `EndCall`, `ListAlerts`.
- `services/fraud/store.go` — pgx store over the `fraud` DB.
- `services/fraud/consumer.go` — `cdrs.normalized` consumer; at-least-once with
  `call_id` dedup.
- `services/fraud/score.go` — versioned weighted scorer (`v1`).
- `services/fraud/migrate.sql` — idempotent schema.
- `cmd/fraudreplay` — labelled synthetic replay and baseline reporter.
- `cmd/fraudsmoke` — small live gRPC probe used by `ops/smoke.sh`.
- `proto/fraud/v1/fraud.proto` — service contract and `FraudAlert` event.
- `services/sbc` and `services/switching` — pass `account_id` and consult Fraud.

Configuration (all new envs):

- `FRAUD_ADDR` — default `:50063`.
- `FRAUD_DSN` — default `postgres://postgres:dev-only@postgres:5432/fraud`.
- `FRAUD_KAFKA` — default `kafka:9092`.
- `FRAUD_TOPIC_ALERTS` — default `fraud.alerts`.
- `FRAUD_ALERT_THRESHOLD` — default `60`.
- `FRAUD_SOFT_ACCOUNT_CALLS` — default `5`.
- `FRAUD_SOFT_PREFIX_CALLS` — default `2`.
- `SBC_FRAUD_ADDR` — SBC's Fraud target. Unset skips Fraud enforcement; when
  set, a Fraud outage denies with `fraud_unavailable`.

Compose adds `fraud` with `postgres` and `kafka` dependencies; `sbc` gains
`SBC_FRAUD_ADDR=fraud:50063`. Observability follows the shared module pattern:
`SetupLogging`/`Init`, `ServerOptions`/`ClientOptions`, `Middleware`, and
`InjectKafka`/`ExtractKafka`.

## 3. Proto contract

`proto/fraud/v1/fraud.proto`:

```proto
syntax = "proto3";

package fraud.v1;

option go_package = "omnifoni/fraud/gen/fraudpb";

service Fraud {
  rpc CheckCall(CheckCallRequest) returns (CheckCallResponse);
  rpc EndCall(EndCallRequest) returns (EndCallResponse);
  rpc ListAlerts(ListAlertsRequest) returns (ListAlertsResponse);
}

message CheckCallRequest {
  string call_id = 1;
  string account_id = 2;
  string destination = 3;
  string cli = 4;
}

message CheckCallResponse {
  bool allowed = 1;
  string reason = 2; // "" | fraud_destination | fraud_cli | fraud_velocity_account | fraud_velocity_prefix
  int32 risk_score = 3;
  string risk_level = 4; // low | medium | high
  repeated string reasons = 5;
  string rule_version = 6;
}

message EndCallRequest { string call_id = 1; }
message EndCallResponse {}

message ListAlertsRequest {
  string account_id = 1; // empty = all accounts
  int32 limit = 2;       // default 100, max 1000
  string cursor = 3;     // base64url("<created_at_ms>|<alert_id>")
}

message Alert {
  string alert_id = 1;
  string call_id = 2;
  string account_id = 3;
  int32 risk_score = 4;
  string risk_level = 5;
  repeated string reasons = 6;
  string rule_version = 7;
  int64 created_at_ms = 8;
  string status = 9; // new
}

message ListAlertsResponse {
  repeated Alert alerts = 1;
  string next_cursor = 2;
}

message FraudAlert {
  string event_id = 1;
  Alert alert = 2;
}
```

`proto/sbc/v1/sbc.proto` gains `string account_id = 6` on
`CheckCallRequest`. Switching fills it from the call's account. SBC passes it to
Fraud; an empty account id skips Fraud checks.

## 4. Data model

Database `fraud`, migrations are `CREATE TABLE IF NOT EXISTS` only (project
convention):

```sql
CREATE TABLE IF NOT EXISTS fraud_controls (
  account_id               text PRIMARY KEY,
  max_calls_per_day        integer CHECK (max_calls_per_day > 0),
  max_calls_per_prefix_day integer CHECK (max_calls_per_prefix_day > 0),
  enabled                  boolean NOT NULL DEFAULT true,
  updated_at               timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS prefix_blocks (
  account_id text NOT NULL,
  prefix     text NOT NULL,
  PRIMARY KEY (account_id, prefix)
);

CREATE TABLE IF NOT EXISTS cli_blocks (
  account_id text NOT NULL,
  prefix     text NOT NULL,
  PRIMARY KEY (account_id, prefix)
);

CREATE TABLE IF NOT EXISTS risk_prefixes (
  prefix text PRIMARY KEY,
  weight integer NOT NULL CHECK (weight BETWEEN 1 AND 100)
);

CREATE TABLE IF NOT EXISTS daily_usage (
  account_id   text NOT NULL,
  day          date NOT NULL,
  prefix       text NOT NULL,
  completed    integer NOT NULL DEFAULT 0,
  active       integer NOT NULL DEFAULT 0,
  seconds      bigint NOT NULL DEFAULT 0,
  amount_minor bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (account_id, day, prefix)
);

CREATE TABLE IF NOT EXISTS reservations (
  call_id    text PRIMARY KEY,
  account_id text NOT NULL,
  day        date NOT NULL,
  prefix     text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_cdrs (
  call_id      text PRIMARY KEY,
  account_id   text NOT NULL,
  destination  text NOT NULL,
  cli          text NOT NULL,
  seconds      bigint NOT NULL,
  amount_minor bigint NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
  alert_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  call_id     text NOT NULL,
  account_id  text NOT NULL,
  risk_score  integer NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
  risk_level  text NOT NULL CHECK (risk_level IN ('low','medium','high')),
  reasons     jsonb NOT NULL,
  rule_version text NOT NULL,
  status      text NOT NULL DEFAULT 'new' CHECK (status IN ('new')),
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alerts_account_created_idx
  ON alerts (account_id, created_at DESC, alert_id DESC);
CREATE INDEX IF NOT EXISTS processed_cdrs_account_cli_idx
  ON processed_cdrs (account_id, cli);
CREATE INDEX IF NOT EXISTS processed_cdrs_account_dest_idx
  ON processed_cdrs (account_id, left(destination, 4));
```

Field semantics:

- `prefix` in `daily_usage`/`reservations` is `''` for the account-wide row or
  `left(destination, 4)` for the destination-prefix row.
- `reservations` makes `EndCall` idempotent: it knows which active row to
  decrement.
- `processed_cdrs` is both the dedup ledger and the first-seen history.
- `alerts.status` is fixed to `new` in this slice; acknowledgement comes later.

## 5. Hard rules

Hard rules are deterministic and only apply when configured:

- Longest matching `prefix_blocks[account_id]` denies `fraud_destination`.
- Longest matching `cli_blocks[account_id]` denies `fraud_cli`.
- `fraud_controls.max_calls_per_day` denies `fraud_velocity_account` when
  `daily_usage` row `prefix=''` has `completed + active >= max`.
- `fraud_controls.max_calls_per_prefix_day` denies `fraud_velocity_prefix` when
  the row for `left(destination,4)` reaches the cap.
- No control row, disabled control, or null cap means no hard block.

All checks and the reservation insert run in one transaction with an advisory
lock keyed by `account_id`, so concurrent starts cannot exceed a cap. A repeated
`CheckCall` with the same `call_id` returns `allowed=true`, an empty denial
reason, and the current score without double-counting.

## 6. Advisory score

Scorer version is the constant `v1`. Signals are additive and capped at 100:

- Longest matching `risk_prefixes[prefix]` weight (default table empty): `+30`.
- First-seen account/destination prefix (`left(destination,4)`): `+20`.
- Elevated velocity: account `completed+active >= FRAUD_SOFT_ACCOUNT_CALLS` or
  prefix `completed+active >= FRAUD_SOFT_PREFIX_CALLS`: `+20` once.
- First-seen CLI for the account: `+15`.
- Start hour 00:00-05:59 UTC: `+10`.

Levels: `low` 0-39, `medium` 40-79, `high` 80-100. When
`risk_score >= FRAUD_ALERT_THRESHOLD`, Fraud inserts an alert row and publishes
`fraud.alerts`. The score never changes `allowed`.

## 7. Call and ingest flow

Call start:

1. SBC runs IP block, destination barring, CLI barring.
2. If `SBC_FRAUD_ADDR` is set and `account_id` is non-empty, SBC calls Fraud
   `CheckCall`.
3. Fraud evaluates hard rules, reserves velocity, computes the score, and may
   create/publish an alert. Denials return an allowed=false response.
4. SBC reserves the trunk slot. If that denies, SBC calls Fraud `EndCall` to
   release the reservation.
5. Fraud transport/DB errors make SBC deny with `fraud_unavailable`.

Call end:

1. SBC `EndCall` deletes its trunk slot and calls Fraud `EndCall`.
2. Fraud `EndCall` deletes the reservation and decrements the matching active
   counters; missing reservations are a successful no-op.

CDR ingest:

1. Consume `cdrs.normalized`, group `fraud`, manual commit after each batch.
2. In one transaction: ignore already-seen `call_id`; delete any matching
   reservation and decrement active; increment completed/seconds/amount for the
   account-wide and prefix rows; insert `processed_cdrs`; commit.
3. Commit Kafka offsets only after the DB transaction.
4. Alert publish failure after DB insert is logged and traced, never rolled
   back.

## 8. Observability and error taxonomy

Every RPC and Kafka batch creates spans via the shared observability module.
Structured logs carry `service`, `trace_id`, `span_id`, `request_id`, `call_id`,
and `account_id`; the existing redactor covers phone-like fields. Deny reasons
are the fixed strings above. Fraud never returns raw DB errors to callers; gRPC
errors are `codes.Internal` and SBC maps any Fraud failure to
`fraud_unavailable` when configured.

## 9. Replay baseline

`cmd/fraudreplay` reads labelled JSONL scenarios from
`services/fraud/testdata/fraud_cases.jsonl`. Each case supplies an account,
destination, CLI, UTC start time, and prior processed CDR history. The CLI uses
the same `score.go` as the service, compares predicted alerts against labels,
and prints TP/FP/FN/TN plus precision and recall. It writes
`docs/fraud-baseline.md` with the scorer version, case count, and measured
values; CI fails below precision `0.80` or recall `0.70`. The dataset must
contain at least 30 cases with both fraud and non-fraud examples.

## 10. Wire-up and tests

- Regenerate all protos with `make protoc`; never hand-edit generated files.
- Add `fraud` to `ops/postgres-init.sql`.
- `ops/smoke.sh` waits for the fraud container, calls `cmd/fraudsmoke` against
  the seeded velocity control, asserts `fraud_velocity_account`, asserts an
  alert appears via `ListAlerts`, and prints `ok: fraud`.
- Full gate after rebuild: `make smoke` then `make test` (tests need the dev
  stack). `go vet ./...` in `services/fraud`, `services/sbc`, and
  `services/switching`.

Tests:

- `services/fraud/score_test.go` — table-driven v1 signals, levels, threshold.
- `services/fraud/fraud_test.go` — hard rules, reservation, idempotent CheckCall
  and EndCall, alert ledger, ListAlerts paging.
- `services/fraud/consumer_test.go` — dedup, active-to-completed conversion,
  offset ordering.
- `services/sbc/sbc_test.go` — Fraud order, release on trunk denial, fail-closed
  outage, account_id propagation.
- `cmd/fraudreplay` golden baseline output.

## 11. Documentation update

`AGENTS.md` gains a P6 slice-one bullet recording the new service, port, DB,
envs, topic, proto, hard-rule/score split, replay baseline, and smoke assertion.
