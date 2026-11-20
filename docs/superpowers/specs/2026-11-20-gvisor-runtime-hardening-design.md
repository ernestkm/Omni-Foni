# gVisor runtime hardening — design

Date: 2026-11-20
Status: approved (design)
Slice: P6 (security and compliance hardening), third slice

## 1. Context

The master plan requires untrusted-input workloads to run under gVisor:
rate-file parsers, webhook processors, and AI inference pods. The SIP dataplane
is explicitly excluded and remains on bare nodes with read-only filesystems,
seccomp, and dropped capabilities (`docs/master-plan.md` sections 5 and 6).

The current tree has one unambiguous workload in those classes: `payments` is
an HMAC-signed webhook processor on `POST /webhook/{gateway}/payment`. There is
no rate-file import endpoint or parser, and no AI/ML inference: `fraud` is a
deterministic, advisory rule scorer. Kubernetes, OpenTofu, and Ansible manifests
do not exist; local development and CI use Docker Compose with stock runc.

This slice adds an opt-in gVisor runtime path for `payments` without breaking
the existing runc path. It does not install `runsc`, change the default compose
file, or claim coverage for workload classes that do not exist.

Feature claims come only from the four ASTPP marketing pages cited in
`docs/master-plan.md` sections 0 and 8. Security controls come from sections 5
and 6. No ASTPP source, schema, or licensed asset was consulted.

## 2. Approach

Three approaches were considered:

1. Add `runtime: runsc` directly to `docker-compose.yml` and make it the
   default for all local and CI runs.
2. Add an opt-in compose override for `payments`, with guarded verification and
   documentation.
3. Write target-state documentation only, with no runnable path.

Approach 2 is chosen. Approach 1 breaks `make up` and GitHub Actions on hosts
without `runsc`, and it would falsely apply gVisor to the SIP dataplane if done
globally. Approach 3 is too weak: it leaves no runnable evidence path. The
opt-in override keeps the existing gates green on stock runc while giving a
`runsc`-enabled host a concrete enforcement path.

## 3. Architecture

New paths:

- `docker-compose.gvisor.yml` — compose override that sets `runtime: runsc` on
  the existing `payments` service only.
- `ops/check-gvisor.sh` — fail-closed runtime assertion for the payments
  container.
- `docs/security/gvisor-runtime-hardening.md` — workload inventory, enablement
  steps, verification commands, and explicit non-claims.

Modified paths:

- `Makefile` — adds `up-gvisor`, `down-gvisor`, `check-gvisor`, and
  `smoke-gvisor` targets; leaves `up`, `down`, `smoke`, and `test` unchanged.
- `AGENTS.md` — adds a P6 slice-three status line.
- `docs/compliance/popia-control-matrix.md` — references the new security doc
  in the security-safeguards row and records that the gVisor sub-control is an
  opt-in `documented_target`, not default CI enforcement.

No new service, database, Kafka topic, HTTP endpoint, gRPC API, or application
code is added. The override changes only the container runtime of `payments`.

## 4. Compose override

`docker-compose.gvisor.yml` contains a single `services.payments.runtime: runsc`
entry. It is applied only with both compose files:

```sh
docker compose -f docker-compose.yml -f docker-compose.gvisor.yml up -d --wait
```

Ports, environment, dependencies, build context, and container name remain
those of the base `payments` service. The normal compose file remains valid on
stock runc, and default `make up` never loads the override.

## 5. Make targets and runtime check

The Makefile defines:

```make
COMPOSE_GVISOR := docker compose -f docker-compose.yml -f docker-compose.gvisor.yml
```

Targets:

- `up-gvisor` — starts the stack with both compose files.
- `down-gvisor` — stops the stack with both compose files.
- `check-gvisor` — runs `ops/check-gvisor.sh`.
- `smoke-gvisor` — runs `bash ops/smoke.sh`, then `bash ops/check-gvisor.sh`.

`ops/check-gvisor.sh` fails closed with distinct messages when:

- `runsc` is not registered in `docker info --format '{{json .Runtimes}}'`.
- `omni-fon-payments-1` is missing or not running.
- `docker inspect --format '{{.HostConfig.Runtime}}' omni-fon-payments-1` is not
  exactly `runsc`.

There is no silent skip. The check is called only by the opt-in targets, so the
default smoke suite remains runc-compatible.

## 6. Documentation and evidence

`docs/security/gvisor-runtime-hardening.md` records:

- The runtime inventory: `payments` in scope; no rate-file parser or AI
  inference workload; `fraud` is deterministic and advisory; SIP edge
  containers are a dev-only representation of a production bare-node
  dataplane.
- The host prerequisites: `runsc` installed and registered with Docker.
- The positive-path commands: `make up-gvisor`, `make smoke-gvisor`, and the
  expected runtime value `runsc` for `omni-fon-payments-1`.
- The limitation: positive-path execution is manual and is not part of stock
  GitHub Actions.

`AGENTS.md` receives a concise P6 slice-three bullet with the new targets and
the explicit non-claims. The POPIA matrix security-safeguards row adds the new
doc as evidence and states that gVisor is opt-in for the webhook processor
while parser and AI controls remain future targets. The compliance manifest and
validator are unchanged.

## 7. Verification and acceptance

Acceptance criteria:

- `docker compose -f docker-compose.yml -f docker-compose.gvisor.yml config -q`
  succeeds on a stock-runc host.
- `make compliance`, `COMPOSE_PROJECT_NAME=omni-fon make test`, and
  `COMPOSE_PROJECT_NAME=omni-fon make smoke` remain green with no override.
- `bash ops/check-gvisor.sh` exits non-zero on this runc-only host with a clear
  `runsc runtime not registered` message.
- On a `runsc`-enabled host, `make up-gvisor && make smoke-gvisor` starts the
  stack, passes the normal smoke suite, and reports `HostConfig.Runtime=runsc`
  for `omni-fon-payments-1`.
- Documentation contains no claim that rate-file parsing, AI inference, SIP
  sandboxing, Kubernetes admission control, CI-wide gVisor, or a penetration
  test is implemented by this slice.

## 8. Non-goals

- Installing or configuring `runsc` on this host.
- Changing the default compose runtime or GitHub Actions runner.
- Adding Kubernetes `RuntimeClass`, OpenTofu, Ansible, image signing, or
  admission control because no deployment manifests exist yet.
- Sandboxing `fraud` as an "AI inference pod"; it is a deterministic scorer.
- Adding rate-file import, parser workloads, or AI inference just to have
  gVisor targets.
- Sandboxing `kamailio`, `sipedge`, or `sipuas`.
- Adding container hardening flags such as `read_only`, `cap_drop`, or
  `no-new-privileges` to this minimal slice.
- Adding runtime deletion, KYC, lawful-intercept, residency enforcement, or
  operator UI work.
