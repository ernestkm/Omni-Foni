# Mediation slice design (P2, slice one)

Date: 2026-11-12. Status: approved in brainstorming.

## Goal

First P2 slice: `services/mediation`, the CDR pipeline consumer, plus the Switching → Kafka CDR emit. Follows the established one-service-per-slice pattern (`charging` :50051, `switching` :50052, `catalogue` :50053). Master plan context: section 3.1 (Mediation bounded context), section 3.2 (CDR event interface), P2 gate "exactly one billed record" (section 7.1).

## Decisions made

- Next slice is Mediation (roadmap order).
- CDR wire format is protobuf over Kafka (not Avro + schema registry); no new infra, reuses the `make protoc` stub pattern. Master plan section 3.2's "Avro" wording is superseded by this decision.
- Switching is the producer, included in this slice (master plan 3.2 lists Switching as the CDR source).
- Duplicate suppression lives in Mediation (Postgres ledger), so downstream Billing receives clean input.
- Approach A: Mediation has no gRPC port. Its contract is Kafka topics.

## Architecture

New files, mirroring the existing service layout:

- `proto/mediation/v1/mediation.proto` — package `mediation.v1`, `option go_package = "omnifoni/mediation/gen/mediationpb"`. Defines one message, no gRPC service.
- `services/mediation/` — `main.go` (env: `MEDIATION_DSN`, `MEDIATION_KAFKA`, `MEDIATION_TOPIC_RAW=cdrs.raw`, `MEDIATION_TOPIC_NORMALIZED=cdrs.normalized`, `MEDIATION_TOPIC_REJECTED=cdrs.rejected`; log), `mediation.go` (pipeline logic), `mediation_test.go`, `Dockerfile`, `go.mod` (module `omnifoni/mediation`).
- `ops/postgres-init.sql` gains a `mediation` database.
- `docker-compose.yml` gains a `mediation` service: `build: ./services/mediation`, env as above with `MEDIATION_DSN: postgres://postgres:dev-only@postgres:5432/mediation`, `MEDIATION_KAFKA: kafka:9092`, no ports, `depends_on` postgres + kafka healthy.
- `Makefile` gains a `protoc` line for the mediation proto and a `test` line (`cd services/mediation && go vet ./... && go test ./...`).
- `services/switching/go.mod` adds `replace omnifoni/mediation => ../mediation` (existing sibling-import pattern; switching builds with `context: ./services`, unchanged).
- `AGENTS.md` gains the slice status line.

Kafka client library: franz-go (BSD-3, FOSS-compliant).

## Contracts

Message `CallCdr` (proto, `mediation.v1`):

| Field | Type | Source |
|---|---|---|
| call_id | string | switching call ID (Kafka message key too) |
| account_id | string | StartCall request |
| destination | string | StartCall request |
| cli | string | StartCall request |
| started_at_ms | int64 | call start, unix millis |
| ended_at_ms | int64 | RELEASED time, unix millis |
| charged_seconds | int64 | total charged seconds from events |
| end_reason | string | `normal` \| `balance_exhausted` \| `ocs_unavailable` |

Topics:

- `cdrs.raw`: Switching produces exactly one CallCdr per finished call, on RELEASED.
- `cdrs.normalized`: Mediation output, one record per unique valid call_id. Consumers: Billing (later slice).
- `cdrs.rejected`: invalid messages, with a `reason` header (`decode_error`, `validation_error`). Audit trail; no silent drops.

Mediation pipeline per message:

1. Decode CallCdr; failure → `cdrs.rejected` (`decode_error`), commit offset.
2. Validate: call_id and account_id non-empty, charged_seconds ≥ 0, end_reason in the known set; failure → `cdrs.rejected` (`validation_error`), commit offset.
3. Upsert ledger row `INSERT INTO cdrs (call_id, published) VALUES ($1, false) ON CONFLICT (call_id) DO NOTHING`, then read the row's `published` flag.
4. If unpublished: produce to `cdrs.normalized`, then mark `published = true`, then commit the Kafka offset.
5. If published (redelivery of an already-handled record): commit offset, no output.

Switching change: wherever the call transitions to RELEASED — StopCall or internal balance-exhaustion termination — build CallCdr from the call record and synchronously produce one message to `cdrs.raw` (key `call_id`). Switching gains env `SWITCHING_KAFKA` (compose: `kafka:9092`). Producer failure does not fail the call stop (the call already released); it logs the error — CDR capture is best-effort at the producer edge and this limitation is explicit.

## Dedup ledger schema

```sql
CREATE TABLE IF NOT EXISTS cdrs (
  call_id   TEXT PRIMARY KEY,
  published BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`mediation.Migrate` is CREATE TABLE IF NOT EXISTS only, matching the charging pattern; schema changes require dropping the table until real migrations exist.

Correctness note: Kafka messages are keyed by `call_id`, so all records for one call land in one partition and are processed serially by one consumer — the insert-then-read of `published` has no race within a consumer group.

## Failure handling (crash-window analysis)

- Crash between insert and produce: redelivery finds the row unpublished → produce happens on retry. No loss.
- Crash after produce, before publish-flag/offset commit: redelivery finds the row published → skip. No duplicate output.
- Residual window: a crash between producing to `cdrs.normalized` and marking the row published makes the redelivered record produce once more after recovery. Mediation is therefore at-least-once; the downstream Billing `call_id` primary key collapses that residual duplicate, so the P2 "exactly one billed record" gate holds.
- DB or Kafka unavailable: retry with backoff, do not commit the offset, do not exit; log with call_id correlation. Mediation never fails open.

## Testing

- `make test` gains `services/mediation`; tests need the compose stack (real Postgres and Kafka), like the existing services.
- Mediation integration test: seed `cdrs.raw` with a producer (valid record, duplicate of it, malformed bytes, invalid-field record); read `cdrs.normalized` and `cdrs.rejected`; assert exactly one normalized record per unique valid call_id, malformed and invalid records in rejected with the right reason, and that a second pass of the same inputs produces zero new normalized records.
- Switching test: start and stop a call (existing harness), assert exactly one CallCdr lands on `cdrs.raw` with the call's fields.
- Smoke (`ops/smoke.sh`): the context-database count check gains `mediation` (7 → 8).

## Out of scope

- Billing/ClickHouse consumption of `cdrs.normalized` (next slices).
- Kafka ACLs, SPIFFE/mTLS, exact Kafka message header taxonomy beyond `reason`.
- Rate/codec validation of CDR fields beyond the checks listed.
