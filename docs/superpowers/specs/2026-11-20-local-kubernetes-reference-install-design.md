# Local Kubernetes reference install — design

Date: 2026-11-20
Status: approved (design)
Slice: P7 (GA), first slice

## 1. Context

`docs/master-plan.md:177` defines P7 as documentation, migration tooling, and
community governance, with two measurable gates: install-to-first-billed-call
under 60 minutes on reference hardware, and zero proprietary-content findings in
a clean-room audit.

The repository currently has a Docker Compose development stack and no
Kubernetes or IaC manifests. `README.md` only shows `docker compose up -d`, and
`ops/smoke.sh` seeds data with direct SQL before placing a SIP test call. There
is no documented install path, no reference-hardware definition, and no timed
first-billed-call gate.

The master plan's technology view (`docs/master-plan.md:103`) is a single
Kubernetes deployment topology, and its FOSS stack table lists Kubernetes,
OpenTofu, and Ansible (`docs/master-plan.md:122-124`). This slice implements the
local Kubernetes reference install first. Cloud provisioning with OpenTofu and
Ansible is deferred to a later P7 slice.

Feature claims come only from the four ASTPP marketing pages cited in
`docs/master-plan.md` sections 0 and 8. No ASTPP source, schema, or licensed asset
was consulted.

## 2. Decisions and approach

Three implementation shapes were considered:

1. Kustomize baseline.
2. Helm chart.
3. Kustomize plus a dedicated Go CLI.

The Kustomize baseline is chosen. A Helm chart adds packaging machinery before
there is a stable reference topology. A dedicated Go CLI adds code and a larger
spec before the install path is proven. Kustomize and shell scripts can express
the current compose-equivalent topology directly and fit the existing
`ops/*.sh` conventions.

The following decisions were made during design:

- Target: local `kind` Kubernetes on a documented Linux host.
- Scope: all 22 services currently declared by `docker-compose.yml`.
- Evidence: automated scripts plus an operator guide.
- Seeding: product HTTP/gRPC APIs first; SQL only where no API exists.
- Reference hardware: documented minimum Linux host.
- Packaging: one Kustomize base plus one reference overlay.
- Network layout: one `omnifoni` namespace; three-zone namespaces and
  NetworkPolicies are deferred.
- Orchestration: separate install, seed, and first-call scripts with shared
  documentation.
- Timing: the 60-minute gate starts before cluster install and ends when the
  invoice assertion succeeds.

## 3. Architecture

New paths:

- `deploy/k8s/base/` — Kustomize base with namespace, ConfigMaps, Secrets,
  Services, Deployments, StatefulSets, PersistentVolumeClaims, and init Jobs.
- `deploy/k8s/overlays/reference/` — reference overlay with local-image tags,
  dev-only secrets, replica counts, host-port mappings, and readiness tuning.
- `ops/k8s/kind-config.yml` — `kind` cluster config with host port mappings for
  the HTTP facade and SIP edge.
- `ops/k8s-install.sh` — builds local images, loads them into `kind`, applies the
  overlay, and waits for data init Jobs and Deployments.
- `ops/k8s-seed.sh` — creates the first tenant, admin user, account, rate, route,
  trunk, Fraud/SBC minimal controls, and SIP source mapping.
- `ops/k8s-first-call.sh` — places one uniquely identified SIP call through the
  Kubernetes services and asserts one Billing invoice line.
- `docs/install/reference-local-kubernetes.md` — prerequisites, reference
  hardware, install commands, seed commands, first-call command, troubleshooting,
  and timing interpretation.

Modified paths:

- `Makefile` — adds `k8s-install`, `k8s-seed`, `k8s-first-call`, `k8s-down`, and
  `k8s-check` targets. Existing `up`, `down`, `smoke`, `test`, `compliance`, and
  gVisor targets are unchanged.
- `AGENTS.md` — adds a P7 slice-one status line.

No application source, proto, database schema, or `docker-compose.yml` changes
are required. No new service, endpoint, topic, or money rule is introduced.

## 4. Kubernetes topology

`deploy/k8s/base` mirrors the 22 compose services:

- StatefulSets with PVCs: `postgres`, `valkey`, `kafka`, `clickhouse`.
- Deployments: `catalogue`, `charging`, `routing`, `switching`, `billing`,
  `mediation`, `payments`, `identity`, `reconciliation`, `fraud`, `sbc`,
  `notifications`, `reports`, `sipedge`, `sipuas`, `kamailio`, `jaeger`,
  `mailpit`.
- Jobs: database initialization and first-call seeding. The first SIP call is
  run from the host through `kind` host ports, not as an in-cluster Job.

All application images are built with the same Dockerfile context rules used by
compose, especially the sibling `COPY` requirements for services that use local
`replace` directives. Images are loaded into the `kind` cluster before manifests
that reference them are applied.

Cluster DNS names match compose service names so existing environment contracts
such as `CHARGING_ADDR=charging:50051` remain valid. ClusterIP Services expose
internal ports. `kind` host port mappings expose only the ports needed by the
operator guide and SIP smoke path; all other access is through `kubectl exec` or
`kubectl port-forward`.

A single `omnifoni` namespace is used. Network zoning and NetworkPolicies are
explicitly out of scope for this slice.

## 5. Installation and seeding

`make k8s-install` performs these steps:

1. Check prerequisites: Docker, `kind`, `kubectl`, `make`, `bash`, `curl`, and
   GNU coreutils.
2. Create the `omnifoni-k8s` kind cluster if absent.
3. Build the local service images named by `docker-compose.yml`.
4. Load those images into the kind cluster.
5. Apply `deploy/k8s/overlays/reference`.
6. Wait for init Jobs to complete and Deployments to become available.
7. Write an install-start marker under `/tmp` for the timed gate.

`make k8s-seed` is idempotent. It uses APIs where they exist:

- `POST /bootstrap`, `POST /sessions`, `POST /tenants`, `POST /accounts` on
  Identity for the first tenant, admin, session, and account mapping.
- Catalogue `PutRate` for the first billing rate.
- Routing `PutTrunk` and `PutLcrRoute` for the first trunk and route.

SQL fallback is limited to seams without an admin API: Charging account and
SBC/Fraud minimal controls. The `SIPEDGE_TRUNK_ACCOUNTS` source-IP mapping is
startup configuration in the Kustomize overlay, not SQL; the first-call script
discovers the source IP and patches it before the real call. The SQL fallback
uses the same database names and table contracts as `ops/postgres-init.sql` and
the service `migrate.sql` files. Seeds are keyed by stable IDs and use upserts or
existence checks so reruns do not duplicate rows.

## 6. First billed call and timing

`make k8s-first-call` runs `ops/k8s-first-call.sh`. It:

1. Calls `ops/k8s-seed.sh` if the seed marker is absent.
2. Runs a probe SIP call from the host through `kind` host ports. The probe is
   expected to be denied as `ip_blocked`; the script extracts the source IP that
   Kamailio passed to sipedge from the sipedge structured log.
3. Patches `SIPEDGE_TRUNK_ACCOUNTS` in the reference overlay with that source IP
   and rolls out sipedge, then reruns the probe until it is allowed.
4. Generates a unique SIP Call-ID and runs the existing `cmd/sipsmoke` from the
   host against the `kind` host-port mapping, targeting the seeded SIPUAS trunk.
5. Waits for the call to end and for Mediation/Billing to process the CDR.
6. Asserts exactly one Billing invoice line for the seeded account created after
   the call started, then captures the resulting Billing `call_id`.
7. Reads the install-start marker, prints the total elapsed time, and succeeds
   only if the assertion passed.

The invoice assertion uses a read-only SQL check through the `postgres` pod,
querying `billing.invoice_lines` by `account_id` and a post-call timestamp rather
than by the SIP Call-ID: switching generates its own CDR/Billing `call_id`, which
is not propagated back from sipedge. A timeout, a missing invoice line, or more
than one invoice line fails the command. The host-run smoke uses the existing
`cmd/sipsmoke` source path, so no new sipsmoke image is required.

The reference-hardware gate passes only when the elapsed time from the start of
`make k8s-install` through the successful invoice assertion is under 60 minutes
on the documented Linux minimum: x86_64, 8 vCPU, 16 GiB RAM, 100 GiB free SSD,
Docker Engine 24+, `kind` 0.20+, `kubectl` 1.29+, and the common shell tools
listed in the operator guide.

## 7. Error handling and idempotency

All scripts use `set -euo pipefail`, check required commands before mutating
state, and print actionable errors. On rollout or assertion failure they print:

- `kubectl get pods -n omnifoni`;
- `kubectl describe` for failing pods and Jobs;
- recent logs for failing containers;
- the generated SIP Call-ID and Billing `call_id` when known, plus the specific
  failed assertion.

`make k8s-install` and `make k8s-seed` are safe to rerun. Kustomize apply is
idempotent, seed IDs are stable, and the first call always uses a fresh
`call_id`. `make k8s-down` deletes only the reference kind cluster and its local
marker files; it does not touch the Docker Compose stack.

## 8. Verification

Implementation is complete when:

- `kubectl kustomize deploy/k8s/overlays/reference` succeeds.
- `bash -n` passes for every new shell script.
- `make k8s-check` validates manifests and script syntax without requiring a
  running cluster.
- On a clean reference host, `make k8s-install && make k8s-first-call` brings up
  all 22 services, creates the first billed call, asserts one invoice line, and
  reports elapsed time under 60 minutes.
- `make test` remains green; no compose, application, schema, or CI behavior
  changes.

No CI workflow is changed by this slice. No SBC decision or LCR selection
assertion is added; the acceptance signal is the invoice line.

## 9. Scope boundaries

Out of scope:

- OpenTofu, Ansible, cloud provisioning, and production IaC.
- Helm, Kustomize bases for multiple environments, and remote registries.
- TLS, ingress, production storage classes, HA, backups, and upgrades.
- Three-zone namespaces and NetworkPolicies.
- New admin APIs; existing APIs are used where available.
- Clean-room scanner, SBOM, licence gate, and community governance files.
- Changes to Docker Compose, application code, protobufs, schemas, or CI.

## 10. Risks

- **Kind resource pressure:** all 22 services may exceed a small host. The
  documented hardware minimum and explicit readiness waits mitigate this.
- **API gaps:** several seed surfaces lack admin APIs. The SQL fallback is
  explicit, narrow, and keyed to stable IDs.
- **SIP ingress**: UDP host-port mapping in kind can be host dependent, and the
  source IP seen by sipedge is discovered by a probe call before the real call.
  The operator guide documents the supported Linux path and diagnosis steps.
- **Timing interpretation:** image build time is included in the install gate;
  the printed breakdown distinguishes build, rollout, seed, call, and assertion.
- **Clean-room separation:** no ASTPP material is read or copied; the design is
  based only on the four marketing pages cited by the master plan.
