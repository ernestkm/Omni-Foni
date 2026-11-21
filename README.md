# Omni-Foní

Omni-Foní is a clean-room BSS/OCS platform for VoIP operators. It provides call control, prepaid/postpaid charging, rating, mediation, billing, payments, reconciliation, identity and tenancy, routing and LCR, session-border policy, fraud controls, reporting, scheduled reports and notifications. The authoritative feature analysis, architecture, stack and roadmap are in [`docs/master-plan.md`](docs/master-plan.md).

Developed by Ernest Manamela. The project belongs to Hosted Cloud Services (Pty) Ltd.

## Prerequisites

- Linux, macOS or Windows with Docker Engine and the Docker Compose plugin
- GNU Make
- Go 1.27.4 or newer, `curl` and `bash` for the smoke test and host-side Go tests
- `protoc`, `protoc-gen-go` and `protoc-gen-go-grpc` only if you need to regenerate gRPC stubs

The supported install path below is a local Docker Compose stack for development and evaluation. It uses development credentials (`dev-only`) and publishes infrastructure ports on localhost; it is not a production deployment (no TLS, external databases or edge authentication).

## Install

1. Get the source:

   ```sh
   git clone git@github.com:ernestkm/Omni-Foni.git
   cd Omni-Foni
   ```

2. Set the Compose project name. The smoke test expects containers named `omni-fon-<service>-1`, so set this before starting the stack:

   ```sh
   export COMPOSE_PROJECT_NAME=omni-fon
   ```

3. Build the service images and start the stack:

   ```sh
   make up
   ```

   This is `docker compose up -d --wait`: images are built on first use and startup waits for configured health checks. On a host without systemd where Docker is not running, start the daemon first:

   ```sh
   nohup dockerd >/var/log/dockerd.log 2>&1 &
   ```

4. Confirm the services are up:

   ```sh
   make ps
   ```

5. Verify the installation end to end:

   ```sh
   make smoke
   ```

   The smoke test checks infrastructure health, creates smoke tenant/account/rate/route data, places SIP calls, checks denial and balance-exhaustion paths, exercises reports, scheduled reports, notifications and fraud, and prints `ok:` lines as each check passes.

If you change service code after the first install, `make up` will reuse the existing images. Rebuild the affected images with `docker compose build <service>` or restart with `docker compose up -d --build`.

## Access points

| Service | Local address |
|---|---|
| Charging (gRPC) | `localhost:50051` |
| Switching (gRPC / HTTP) | `localhost:50052` / `localhost:50070` |
| Catalogue (gRPC) | `localhost:50053` |
| Billing (gRPC) | `localhost:50054` |
| Payments (webhook HTTP / gRPC) | `localhost:50055` / `localhost:50060` |
| Identity (HTTP) | `localhost:50056` |
| Routing (gRPC) | `localhost:50057` |
| SBC (gRPC) | `localhost:50058` |
| Reconciliation (gRPC) | `localhost:50059` |
| Reports (gRPC / HTTP) | `localhost:50061` / `localhost:50062` |
| Fraud (gRPC) | `localhost:50063` |
| Notifications (gRPC) | `localhost:50064` |
| SIP edge (Kamailio UDP/TCP) | `localhost:5060` |
| Jaeger UI / OTLP | `localhost:16686` / `localhost:4317` |
| Mailpit SMTP / UI | `localhost:1025` / `localhost:8025` |
| PostgreSQL / Valkey / Kafka | `localhost:5432` / `localhost:6379` / `localhost:9092` |
| ClickHouse HTTP / native | `localhost:8123` / `localhost:9000` |

Do not bind another service to host port 9000; it is ClickHouse's native protocol port.

## Configuration

Service configuration is environment-driven; see [`docker-compose.yml`](docker-compose.yml) for every service's variables and default addresses. No `.env` file is required for the development stack. Replace the `dev-only` secrets, webhook secret, DSNs and signing keys before using any of it outside local development.

PostgreSQL databases are created by [`ops/postgres-init.sql`](ops/postgres-init.sql) on the first initialization of the `pgdata` volume only. If an existing volume predates a database, either create it explicitly, for example:

```sh
docker exec omni-fon-postgres-1 createdb -U postgres reports_sched
docker exec omni-fon-postgres-1 createdb -U postgres notifications
```

or remove the old volume with `docker volume rm omni-fon_pgdata` and start again (this deletes all local data).

Host-side Go tests connect to the broker at `kafka:9092`, as advertised by the container. Add this entry to `/etc/hosts` so those tests can resolve the broker:

```text
127.0.0.1 kafka
```

## Tests and checks

Run these with the stack up and `COMPOSE_PROJECT_NAME=omni-fon` set:

```sh
make compliance    # data-classification validator
make cleanroom     # blocking clean-room audit, writes artifacts/cleanroom/
make test          # compliance + cleanroom, then every service's go vet and go test
make smoke         # end-to-end smoke test
```

`make test` requires the running stack because service tests use real PostgreSQL and Kafka. `make cleanroom` exits `1` on findings and `2` on policy or I/O errors; reports are written under `artifacts/cleanroom/` and are not committed.

To regenerate all gRPC stubs after changing a `.proto` file:

```sh
make protoc
```

The Makefile expects `protoc`, `protoc-gen-go` and `protoc-gen-go-grpc` in `/root/go/bin`.

## Optional gVisor sandbox

An opt-in runtime override sandboxes the payments webhook processor with gVisor on a host where `runsc` is installed:

```sh
make up-gvisor
make check-gvisor
make smoke-gvisor
make down-gvisor
```

The default stack and CI use runc; the positive-path gVisor check is a manual gate. See [`docs/security/gvisor-runtime-hardening.md`](docs/security/gvisor-runtime-hardening.md).

## Day-to-day operations

```sh
make logs    # follow all container logs
make down    # stop and remove containers (named volumes are kept)
```

Health status must be read with `docker inspect --format '{{.State.Health.Status}}' <container>`; `docker compose ps --status healthy` reports no rows on some Compose versions.

## Documentation

- [`docs/master-plan.md`](docs/master-plan.md) — feature inventory, architecture, technology choices and roadmap
- [`docs/compliance/`](docs/compliance/) — data classification, POPIA control pack and clean-room policy/audit guide
- [`docs/governance/`](docs/governance/) — contribution, release, onboarding and RFC material
- [`AGENTS.md`](AGENTS.md) — implementation status and repository conventions
