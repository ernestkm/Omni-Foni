# Maintainer Onboarding

## Prerequisites

A maintainer candidate should have a history of constructive reviews, at least one merged cross-cutting change, and familiarity with the master plan, clean-room rules, and release policy.

## Appointment

A current maintainer opens an issue with the candidate, rationale, and scope. After seven days and approval by at least two maintainers, add the candidate to repository maintainer settings and link the issue here.

## First-month checklist

- Review at least one PR per service area.
- Run `make cleanroom` and `COMPOSE_PROJECT_NAME=omni-fon make test`.
- Read `docs/master-plan.md` sections 0, 4, and 8.
- Review `docs/compliance/` and `docs/security/`.
- Triage open issues and label stale ones.

## Ongoing duties

Maintainers review, merge, release, handle private reports, enforce the code of conduct, and keep the roadmap aligned with implementation.
