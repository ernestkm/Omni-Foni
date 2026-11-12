# AGENTS.md — Omni-Foní

Open-source clean-room BSS/OCS for VoIP operators. Master plan in `docs/master-plan.md`, read it before any implementation work.

## Status

- P0 foundations + first P1 slice: `services/charging`, the OCS core (gRPC `Authorize`/`Charge`/`Release`, `proto/charging/v1/charging.proto`).
- P1 slice two: `services/switching` on :50052, the call-lifecycle caller against the OCS (gRPC `StartCall`/`StopCall`/`CallEvents`, `proto/switching/v1/switching.proto`). Charges every whole second; terminates within one tick of exhaustion; fail-closed (`ocs_unavailable`) when Charging is unreachable. `make protoc` regenerates both protos (needs protoc and protoc-gen-go/-go-grpc in /root/go/bin; the Makefile PATHs them).
- P1 slice three: `services/catalogue` on :50053 owns all rates (gRPC `GetRate` longest-prefix match / `PutRate`, `proto/catalogue/v1/catalogue.proto`, rates table in the `catalogue` DB). Charging resolves rates from Catalogue over gRPC and fail-closes (`internal_error`) if it's unreachable; the charging DB has no rates table anymore (stale one may linger in old volumes, drop if it bothers you).
- P2 slice one: `services/mediation` consumes `cdrs.raw`, validates, dedups via the mediation DB `cdrs` ledger (call_id PK), publishes `cdrs.normalized` (and `cdrs.rejected` dead letters with a `reason` header), then commits Kafka offsets. No gRPC port. `services/switching` publishes one `mediation.v1.CallCdr` (proto in `proto/mediation/v1/mediation.proto`) to `cdrs.raw` on call end via franz-go; Kafka addr envs are `SWITCHING_KAFKA` and `MEDIATION_KAFKA` (default localhost:9092). Mediation is at-least-once with a tiny crash-window duplicate; Billing dedups by call_id. Design: `docs/superpowers/specs/2026-11-12-mediation-design.md`.
- Switching imports charging's stubs via `replace omnifoni/charging => ../charging`, so its image builds with `context: ./services` and `dockerfile: switching/Dockerfile` (new sibling-importing services copy that pattern). Charging imports catalogue's stubs the same way.
- Switching tests seed the charging DB directly and gRPC to localhost:50051; charging tests seed rates via catalogue gRPC on localhost:50053; bufconn targets must use `passthrough:///bufnet`.
- Dev data layer: `make up` (docker compose: PostgreSQL 17, Valkey 8, Kafka 3.9.1 KRaft, ClickHouse 24.10, charging service on :50051). Smoke: `make smoke`. Tests: `make test` (needs the stack up, they hit real Postgres).
- Regenerate gRPC stubs with `make protoc` (needs protoc and protoc-gen-go/-go-grpc in /root/go/bin, PATH it).
- `charging.Migrate` is CREATE TABLE IF NOT EXISTS only; schema changes require dropping the charging tables (`docker exec omni-fon-postgres-1 psql -U postgres -d charging -c 'DROP TABLE IF EXISTS ledger, sessions, rates, accounts CASCADE'`) until real migrations exist.
- Money is integer minor units (1/10000 currency); rate per second is numeric(14,6) minor units; per-event cost is ROUND(delta*rate), half away from zero. Postpaid balances go negative down to -credit_limit.
- The rates table inside the charging DB is a ponytail placeholder until the Catalogue service exists.
- Postgres databases are created by `ops/postgres-init.sql` on first init only; recreating them requires deleting the `pgdata` volume.
- The `mediation` database (like all context DBs) is created by `ops/postgres-init.sql` on first init only; a fresh volume (`docker volume rm omni-fon_pgdata`) is required if the volume predates it.
- Host port 9000 is ClickHouse native, don't bind anything else to it. Health status must be read with `docker inspect --format '{{.State.Health.Status}}'`, `docker compose ps --status healthy` returns no rows on this CLI version.

## Hard constraints

- Clean-room: never read ASTPP source code, schemas, or licensed assets. Feature claims come only from the four ASTPP marketing pages cited in `docs/master-plan.md` sections 0 and 8.
- FOSS-only stack, licences verified per `docs/master-plan.md` section 4. No Terraform (BUSL), no Redis (licence), use Valkey.

## Environment

- Go lives in `/usr/local/go`, symlinked to `/usr/local/bin/go`. Installed outside apt; update by replacing the tarball.
- Docker daemon was started with `nohup dockerd` (no systemd); after host restart run `nohup dockerd >/var/log/dockerd.log 2>&1 &` before `make up`.
- Host-side go tests reach the dev broker via 127.0.0.1 kafka in /etc/hosts (the broker advertises kafka:9092); keep the entry for host tests.

## Conventions for filling this file

Only add lines that a new session would likely get wrong without them:

- Exact dev commands (build, test, lint, typecheck) — especially non-obvious ones.
- Required command order when it matters.
- Entry points and package boundaries once they exist.
- Environment or setup quirks, generated code, codegen steps.
- Conventions that differ from language/framework defaults.

Verify every claim against the executable source of truth (scripts, config, CI) before writing it here. Omit anything speculative.
