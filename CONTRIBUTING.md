# Contributing

Thanks for helping build Omni-Foní.

## Ground rules

- This is a clean-room project. Never read, copy, or summarize ASTPP source code, schemas, or licensed assets.
- Feature claims come only from the four marketing pages cited in `docs/master-plan.md` sections 0 and 8.
- Keep all dependencies FOSS and compatible with `docs/master-plan.md` section 4.
- Do not introduce Terraform, Redis, Helm charts, ingress, TLS, or cloud IaC without an RFC and roadmap update.

## Local setup

- Go is installed at `/usr/local/go` and symlinked to `/usr/local/bin/go`.
- Docker Compose is the dev data layer.
- Start the stack with `make up`.
- Run the full test suite with `COMPOSE_PROJECT_NAME=omni-fon make test`.
- Run the clean-room gate with `make cleanroom`.
- Regenerate gRPC stubs with `make protoc` when a `.proto` changes; `protoc` and plugins live in `/root/go/bin`.

## Workflow

1. Open an issue before large work.
2. Use an RFC from `docs/governance/rfc-template.md` for cross-cutting design changes.
3. Work on a focused branch or worktree.
4. Add or update tests for each behavior change.
5. Run `make cleanroom` and `COMPOSE_PROJECT_NAME=omni-fon make test`.
6. Open a PR with the PR template filled in.

## Review expectations

- Every PR needs one reviewer who did not write the change.
- Security, licence, clean-room, schema, proto, and deployment changes need a maintainer review.
- A green CI run is required before merge.
- Reviewers may request a clean-room checklist entry in `docs/governance/cleanroom-checklist.md` for any new external material.

## Commits

Use short imperative commit messages such as `feat: add rating cache` or `docs: clarify reporting policy`. Do not include secrets, tokens, customer data, or proprietary material.
