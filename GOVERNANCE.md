# Governance

## Roles

- Contributors open issues, propose changes, review work, and may become maintainers by contribution.
- Maintainers merge approved changes, manage releases, enforce the code of conduct, and maintain the roadmap.
- Reviewers are contributors or maintainers who review a specific change; at least one reviewer must be independent of the author.

## Decision making

Most decisions use lazy consensus: propose in an issue or RFC, allow at least seven full days for objections, then proceed if there is no unresolved objection. Cross-cutting design, licence, clean-room, schema, or deployment decisions require an RFC and explicit maintainer approval.

If consensus fails, maintainers decide by simple majority. A maintainer who authored the change does not vote on their own objection. Decisions are recorded in the issue, RFC, or PR that introduced them.

## RFC process

1. Copy `docs/governance/rfc-template.md` into `docs/superpowers/specs/` with a dated name.
2. Link the RFC from an issue.
3. Solve the problem statement, constraints, options, and verification plan.
4. Leave the RFC open for at least seven days.
5. Merge when approved by at least two maintainers.

## Releases

Releases follow `docs/governance/release-policy.md`. Only maintainers create tags and publish release notes.

## Conduct enforcement

Code of conduct reports are handled privately as described in `CODE_OF_CONDUCT.md`. Maintainers may act immediately when safety or confidentiality is at risk.

## Changing governance

Changes to this file require an RFC and approval by at least two maintainers.
