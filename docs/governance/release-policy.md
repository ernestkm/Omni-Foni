# Release Policy

## Versioning

Releases use semantic versioning. Breaking API, proto, schema, or deployment changes require a minor version bump before 1.0 and a major bump after 1.0.

## Preconditions

A release may be tagged only when:

- `make cleanroom` passes;
- `COMPOSE_PROJECT_NAME=omni-fon make test` passes;
- `bash ops/smoke.sh` passes on a clean stack;
- release notes list changes, migrations, and new environment variables;
- any manual gate that cannot run in CI is recorded as unrun, not passed.

## Tagging and publication

Only maintainers create tags. Release notes must name the commit range and include upgrade instructions for schema or deployment changes.

## Rollback

If a release breaks a release gate, maintainers may delete the tag before downstream adoption. After adoption, publish a patch release rather than moving the tag.
