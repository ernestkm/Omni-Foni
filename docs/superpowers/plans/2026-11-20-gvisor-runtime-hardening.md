# gVisor Runtime Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in gVisor runtime path for the payments webhook processor without changing the default runc path or claiming coverage for workload classes that do not exist.

**Architecture:** A `docker-compose.gvisor.yml` override sets `runtime: runsc` only on `payments`. New Make targets apply the override, and a fail-closed shell assertion verifies the registered runtime and container runtime identity. Documentation records the workload inventory and manual positive-path gate.

**Tech Stack:** Docker Compose v2 (`config --format json`), Docker runtime `runsc`, bash, Python 3 stdlib JSON parser, existing Go/Make gates.

## Global Constraints

- Clean-room: never read ASTPP source code, schemas, or licensed assets; feature claims come only from the four ASTPP marketing pages cited in `docs/master-plan.md` sections 0 and 8.
- FOSS-only stack; gVisor is Apache-2.0 and already listed in `docs/master-plan.md`.
- Do not install or configure `runsc` on this host.
- Do not change `docker-compose.yml`, default `make up`, default `make smoke`, or GitHub Actions.
- Do not set a runtime on `kamailio`, `sipedge`, `sipuas`, `fraud`, or data services.
- Do not claim a rate-file parser or AI inference workload exists; neither does.
- Do not add Kubernetes, OpenTofu, Ansible, admission control, image signing, or penetration testing.
- Use `COMPOSE_PROJECT_NAME=omni-fon` for compose and smoke commands.
- `rg` is unavailable; use `grep`. `jq` is unavailable; use `python3`.
- Health status must be read with `docker inspect --format '{{.State.Health.Status}}'`.
- Stage only the exact files in each commit step; do not stage `.superpowers/sdd/progress.md`.
- Do not add Go comments; no Go source is added by this plan.

## File Structure

- Create `docker-compose.gvisor.yml`: opt-in runtime override for `payments`.
- Create `ops/test-gvisor-config.sh`: verifies base and override compose runtime configuration.
- Create `ops/check-gvisor.sh`: verifies `runsc` registration and the payments container runtime.
- Create `ops/test-check-gvisor.sh`: stubs `docker` to test all check-script branches.
- Create `docs/security/gvisor-runtime-hardening.md`: workload inventory, enablement, verification, and non-claims.
- Modify `Makefile`: add `COMPOSE_GVISOR` and four opt-in targets.
- Modify `AGENTS.md`: add P6 slice-three status.
- Modify `docs/compliance/popia-control-matrix.md`: add the new evidence and opt-in limitation.

---

### Task 1: Compose override and opt-in Make targets

**Files:**
- Create: `docker-compose.gvisor.yml`
- Create: `ops/test-gvisor-config.sh`
- Modify: `Makefile:1-16`

**Interfaces:**
- Consumes: existing `payments` service in `docker-compose.yml:170-186`.
- Produces: override file loadable with `docker compose -f docker-compose.yml -f docker-compose.gvisor.yml`; Make targets `up-gvisor`, `down-gvisor`, `check-gvisor`, `smoke-gvisor`; `check-gvisor` initially calls an absent script that Task 2 creates.

- [ ] **Step 1: Write the failing compose-config test**

Create `ops/test-gvisor-config.sh` with exactly:

```bash
#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

docker compose -f docker-compose.yml config --format json > "$tmp/base.json"
docker compose -f docker-compose.yml -f docker-compose.gvisor.yml config --format json > "$tmp/gvisor.json"

python3 - "$tmp/base.json" "$tmp/gvisor.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    base = json.load(handle)
with open(sys.argv[2], encoding="utf-8") as handle:
    gvisor = json.load(handle)

base_runtime = {
    name: service.get("runtime")
    for name, service in base.get("services", {}).items()
    if service.get("runtime")
}
assert base_runtime == {}, f"base compose gained runtime: {base_runtime}"

services = gvisor.get("services", {})
assert services["payments"]["runtime"] == "runsc", services["payments"].get("runtime")

sandboxed = {
    name: service["runtime"]
    for name, service in services.items()
    if service.get("runtime")
}
assert sandboxed == {"payments": "runsc"}, sandboxed
PY
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bash ops/test-gvisor-config.sh`

Expected: FAIL because `docker-compose.gvisor.yml` does not exist.

- [ ] **Step 3: Create the compose override**

Create `docker-compose.gvisor.yml` with exactly:

```yaml
services:
  payments:
    runtime: runsc
```

- [ ] **Step 4: Add Make targets**

Replace `Makefile:1` with:

```make
.PHONY: up down ps smoke logs test protoc compliance up-gvisor down-gvisor check-gvisor smoke-gvisor
```

Insert this line immediately after the `.PHONY` line:

```make
COMPOSE_GVISOR := docker compose -f docker-compose.yml -f docker-compose.gvisor.yml
```

Insert these targets immediately after the `smoke` target:

```make
up-gvisor:
	$(COMPOSE_GVISOR) up -d --wait

down-gvisor:
	$(COMPOSE_GVISOR) down

check-gvisor:
	bash ops/check-gvisor.sh

smoke-gvisor:
	bash ops/smoke.sh
	bash ops/check-gvisor.sh
```

- [ ] **Step 5: Run the compose-config test**

Run: `bash ops/test-gvisor-config.sh`

Expected: PASS with no output.

- [ ] **Step 6: Check compose syntax and Make target expansion**

Run: `docker compose -f docker-compose.yml -f docker-compose.gvisor.yml config -q`

Expected: exit 0.

Run: `make -n up-gvisor down-gvisor check-gvisor smoke-gvisor`

Expected: printed commands using both compose files for `up-gvisor`/`down-gvisor`, `bash ops/check-gvisor.sh` for `check-gvisor`, and `bash ops/smoke.sh` followed by `bash ops/check-gvisor.sh` for `smoke-gvisor`.

- [ ] **Step 7: Commit**

```bash
git add docker-compose.gvisor.yml ops/test-gvisor-config.sh Makefile
git commit -m "feat: add opt-in gVisor compose override"
```

---

### Task 2: Fail-closed runtime check

**Files:**
- Create: `ops/check-gvisor.sh`
- Create: `ops/test-check-gvisor.sh`

**Interfaces:**
- Consumes: `check-gvisor` Make target from Task 1.
- Produces: `ops/check-gvisor.sh` with messages `runsc runtime not registered`, `payments container missing`, `payments container not running`, `payments runtime is <runtime>, expected runsc`, and success message `payments runtime is runsc`.

- [ ] **Step 1: Write the failing runtime-check test**

Create `ops/test-check-gvisor.sh` with exactly:

```bash
#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"

cat > "$tmp/bin/docker" <<'STUB'
#!/usr/bin/env bash
case "$1 $2" in
  "info --format") printf '%s\n' "${FAKE_RUNTIMES-}" ;;
  "inspect --format") printf '%s\n' "${FAKE_INSPECT-}" ;;
  *) exit 64 ;;
esac
STUB
chmod +x "$tmp/bin/docker"
export PATH="$tmp/bin:$PATH"

expect_fail() {
  local expected="$1"
  shift
  local output
  local status
  set +e
  output="$("$@" 2>&1)"
  status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "expected failure containing: $expected"
    exit 1
  fi
  if ! grep -qF "$expected" <<<"$output"; then
    echo "expected message: $expected"
    echo "actual output: $output"
    exit 1
  fi
}

expect_pass() {
  local expected="$1"
  shift
  local output
  output="$("$@" 2>&1)"
  if ! grep -qF "$expected" <<<"$output"; then
    echo "expected message: $expected"
    echo "actual output: $output"
    exit 1
  fi
}

FAKE_RUNTIMES='{"runc":{}}' FAKE_INSPECT='true runsc' \
  expect_fail "runsc runtime not registered" bash ops/check-gvisor.sh

FAKE_RUNTIMES='{"runsc":{}}' FAKE_INSPECT='' \
  expect_fail "payments container missing" bash ops/check-gvisor.sh

FAKE_RUNTIMES='{"runsc":{}}' FAKE_INSPECT='false runsc' \
  expect_fail "payments container not running" bash ops/check-gvisor.sh

FAKE_RUNTIMES='{"runsc":{}}' FAKE_INSPECT='true runc' \
  expect_fail "payments runtime is runc, expected runsc" bash ops/check-gvisor.sh

FAKE_RUNTIMES='{"runsc":{}}' FAKE_INSPECT='true runsc' \
  expect_pass "payments runtime is runsc" bash ops/check-gvisor.sh
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bash ops/test-check-gvisor.sh`

Expected: FAIL because `ops/check-gvisor.sh` does not exist.

- [ ] **Step 3: Implement the runtime check**

Create `ops/check-gvisor.sh` with exactly:

```bash
#!/usr/bin/env bash
set -euo pipefail

if ! docker info --format '{{json .Runtimes}}' 2>/dev/null | grep -q '"runsc"'; then
  echo "runsc runtime not registered" >&2
  exit 1
fi

inspect="$(docker inspect --format '{{.State.Running}} {{.HostConfig.Runtime}}' omni-fon-payments-1 2>/dev/null || true)"
if [[ -z "$inspect" ]]; then
  echo "payments container missing" >&2
  exit 1
fi

read -r running runtime <<<"$inspect"
if [[ "$running" != "true" ]]; then
  echo "payments container not running" >&2
  exit 1
fi
if [[ "$runtime" != "runsc" ]]; then
  echo "payments runtime is $runtime, expected runsc" >&2
  exit 1
fi

echo "payments runtime is runsc"
```

- [ ] **Step 4: Run the runtime-check test**

Run: `bash ops/test-check-gvisor.sh`

Expected: PASS with no output.

- [ ] **Step 5: Verify the negative path on this runc-only host**

Run: `bash ops/check-gvisor.sh`

Expected: FAIL with `runsc runtime not registered`.

- [ ] **Step 6: Commit**

```bash
git add ops/check-gvisor.sh ops/test-check-gvisor.sh
git commit -m "feat: verify gVisor payments runtime"
```

---

### Task 3: Documentation, status, and POPIA evidence

**Files:**
- Create: `docs/security/gvisor-runtime-hardening.md`
- Modify: `AGENTS.md`
- Modify: `docs/compliance/popia-control-matrix.md:11`

**Interfaces:**
- Consumes: files created in Tasks 1 and 2.
- Produces: operator-facing enablement doc; status line in `AGENTS.md`; security-safeguards evidence update.

- [ ] **Step 1: Write the runtime-hardening doc**

Create `docs/security/gvisor-runtime-hardening.md` with exactly:

````markdown
# gVisor runtime hardening

Status: opt-in runtime path; positive-path execution is a manual gate.
Master-plan control: `docs/master-plan.md` sections 5 and 6.

## Workload inventory

- In scope: `payments`, the HMAC-signed webhook processor at
  `POST /webhook/{gateway}/payment`.
- Not present: rate-file import parsers and AI/ML inference workloads. No doc in
  this slice claims gVisor coverage for them.
- Not classified as AI: `fraud` is a deterministic, advisory rule scorer.
- Not sandboxed: `kamailio`, `sipedge`, and `sipuas` represent the SIP dataplane.
  The master plan requires the production SIP dataplane to remain on bare nodes
  with read-only filesystems, seccomp, and dropped capabilities.
- Not introduced: Kubernetes `RuntimeClass`, OpenTofu, Ansible, admission
  control, image signing, or penetration testing.

## Default runtime

`docker-compose.yml` and GitHub Actions remain on stock runc. `make up`,
`make smoke`, `make compliance`, and `make test` do not load the gVisor
override. This keeps the existing developer and CI path unchanged on hosts
without `runsc`.

## Enabling on a runsc host

Prerequisites:

- `runsc` is installed and registered with Docker.
- `docker info --format '{{json .Runtimes}}'` includes `"runsc"`.

Run:

```sh
make up-gvisor
make smoke-gvisor
```

`make up-gvisor` applies `docker-compose.gvisor.yml`, which adds
`runtime: runsc` only to the `payments` service. `make smoke-gvisor` runs the
normal `ops/smoke.sh` suite and then checks the runtime identity.

## Runtime assertion

`make check-gvisor` runs `ops/check-gvisor.sh`. It fails closed when:

- `runsc` is not registered.
- `omni-fon-payments-1` is missing or not running.
- `docker inspect --format '{{.HostConfig.Runtime}}' omni-fon-payments-1` is not
  exactly `runsc`.

Success output is `payments runtime is runsc`. The check is never called by the
default smoke target.

## Verification boundary

Verified on stock-runc hosts:

- `docker compose -f docker-compose.yml -f docker-compose.gvisor.yml config -q`
- `bash ops/test-gvisor-config.sh`
- `bash ops/test-check-gvisor.sh`
- `bash ops/check-gvisor.sh` negative path
- `make compliance`, `make test`, and `make smoke`

Manual positive-path gate on a `runsc`-enabled host:

- `make up-gvisor && make smoke-gvisor`
- `docker inspect --format '{{.HostConfig.Runtime}}' omni-fon-payments-1`

The CI workflow does not install `runsc` and does not execute the positive path.

## Clean-room basis

Feature claims come only from the four ASTPP marketing pages cited in
`docs/master-plan.md` sections 0 and 8. Security controls come from sections 5
and 6. No ASTPP source, schema, or licensed asset was consulted.
````

- [ ] **Step 2: Add the AGENTS status line**

Insert this bullet immediately after the P6 POPIA slice-two bullet in `AGENTS.md`:

```markdown
- P6 slice three: `docs/security/gvisor-runtime-hardening.md` adds an opt-in `docker-compose.gvisor.yml` runtime override for the `payments` webhook processor (`make up-gvisor`, `make smoke-gvisor`, `make check-gvisor`); default compose/CI stay on runc, no rate-file parser or AI inference workload exists, and SIP/kamailio are not sandboxed. runsc installation and the positive-path gVisor smoke are manual gates on a runsc-enabled host.
```

- [ ] **Step 3: Update the POPIA security-safeguards row**

Replace `docs/compliance/popia-control-matrix.md:11` with:

```markdown
| Security safeguards | POPIA s19 / `popia-s19` | Tenant scoping, bcrypt, redacted JSON logs, signed artifact links, fraud/SBC controls, 24h sessions, and opt-in gVisor isolation for the payments webhook processor. | `cdr`, `charging_financial`, `identity`, `identity_session`, `fraud_security`, `sbc_security`, `report_artifact`, `report_configuration`, `notification_outbox` | `services/identity/identity.go`, `services/observability/log.go`, `services/reports/main.go`, `services/fraud/fraud.go`, `services/sbc/sbc.go`, `docs/security/gvisor-runtime-hardening.md` | implemented | Counsel and security review must confirm safeguard adequacy. gVisor is an opt-in `documented_target` on stock runc; no parser or AI inference workload exists. |  |  |  |
```

- [ ] **Step 4: Verify documentation references**

Run: `grep -n "P6 slice three" AGENTS.md`

Expected: one line containing the new status bullet.

Run: `grep -n "gvisor-runtime-hardening.md" docs/compliance/popia-control-matrix.md`

Expected: one line containing the updated security-safeguards row.

Run: `grep -n "runtime: runsc" docs/security/gvisor-runtime-hardening.md`

Expected: the enablement sentence containing `runtime: runsc`.

- [ ] **Step 5: Run the full default gate**

Run: `COMPOSE_PROJECT_NAME=omni-fon make compliance`

Expected: compliance validator passes.

Run: `COMPOSE_PROJECT_NAME=omni-fon make test`

Expected: all 15 module test suites pass.

Run: `COMPOSE_PROJECT_NAME=omni-fon make smoke`

Expected: existing smoke suite passes on the running stack.

- [ ] **Step 6: Commit**

```bash
git add docs/security/gvisor-runtime-hardening.md AGENTS.md docs/compliance/popia-control-matrix.md
git commit -m "docs: document gVisor runtime hardening"
```
