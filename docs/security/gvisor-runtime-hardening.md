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
