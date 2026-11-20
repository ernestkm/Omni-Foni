# Clean-room audit and community governance — design

Date: 2026-11-20
Status: approved (design)
Slice: P7 (GA), second slice

## 1. Context

`docs/master-plan.md:177` defines P7 as documentation, migration tooling, and
community governance, with two measurable gates: install-to-first-billed-call
under 60 minutes, and zero proprietary-content findings in a clean-room audit.
The first P7 slice delivered the local Kubernetes reference install; its live
timing gate remains a manual gate because `kind`, `kubectl`, and `grpcurl` are not
available on this host.

The clean-room gate has no implementation. `docs/master-plan.md:188` names clean
contamination as a legal risk, and `docs/master-plan.md:190` names licence drift
as a separate risk with an SBOM, licence gate, and vendored licence files. The
repository has `tools/compliancecheck` and `docs/compliance/` for POPIA evidence,
but no clean-room policy, scanner, CI gate, or community governance files.

Feature claims come only from the four marketing pages cited in
`docs/master-plan.md` sections 0 and 8. No ASTPP source, schema, or licensed asset
was consulted.

## 2. Decisions and approach

Three shapes were considered:

1. Stdlib Go checker with a versioned policy manifest.
2. Shell/Python grep-based checker.
3. Third-party SBOM scanner plus a custom feature-claim check.

Option 1 is chosen. It matches the stdlib-only pattern of `tools/compliancecheck`,
is deterministic, unit-testable, and adds no network or external tool
dependency. Option 2 is simpler but harder to test and more fragile across
shells. Option 3 broadens scope into tool acquisition and licence resolution
before the repository's own clean-room baseline is enforced.

Decisions made during design:

- Work happens on a new branch from `master`; the P7 install branch stays as-is.
- The gate is blocking for every PR and prerequisite of `make test`.
- The checker scans repository text files only; it never reads external ASTPP
  material and never fetches dependencies, licences, or SBOMs.
- Any finding fails the command. Exceptions require an owner, reason, expiry,
  and rule ID.
- Governance uses the full process pack, with reporting through GitHub private
  vulnerability reporting and no invented contact addresses.
- No application source, proto, database schema, Docker Compose, Kubernetes
  manifests, or generated stubs change.

## 3. Architecture

New paths:

- `tools/cleanroomcheck/` — stdlib-only Go module `omnifoni/cleanroomcheck`;
  `main.go` implements the checker, `main_test.go` covers rules and fixtures.
- `docs/compliance/cleanroom-policy.json` — versioned policy manifest defining
  source IDs, forbidden artifacts, forbidden terms and allowances, claim-bearing
  paths, licence policy, exclusions, and time-bounded exceptions.
- `docs/compliance/licence-manifest.json` — declared components and licences
  from `docs/master-plan.md` section 4, with distribution context where needed.
- `docs/compliance/cleanroom-audit.md` — operator guide for running, reading,
  and maintaining the gate.
- `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `GOVERNANCE.md`, `SECURITY.md`.
- `docs/governance/rfc-template.md`, `release-policy.md`,
  `maintainer-onboarding.md`, `cleanroom-checklist.md`.
- `.github/ISSUE_TEMPLATE/bug_report.yml`, `feature_request.yml`, and
  `.github/pull_request_template.md`.

Modified paths:

- `Makefile` — adds `cleanroom`, makes it a prerequisite of `test`.
- `.github/workflows/ci.yml` — adds a blocking clean-room step and uploads the
  text/JSON report with `if: always()`.
- `.gitignore` — ignores `artifacts/`, where local/CI reports are written.
- `AGENTS.md` — adds a short status line for this slice.

No service code, proto, schema, Compose, or Kubernetes manifest changes.

## 4. Policy manifest

`cleanroom-policy.json` has this contract:

- `version`: integer, currently `1`.
- `source_basis.allowed`: the four source IDs `S1`–`S4` with status and the only
  canonical location, `docs/master-plan.md`.
- `exclude_paths`: path prefixes skipped by the walker. Required defaults are
  `.git/`, `.worktrees/`, and `artifacts/`. Additional generated or binary paths
  must be explicit policy entries, not hard-coded behavior.
- `forbidden_artifact_patterns`: path patterns for vendor source trees,
  vendor schemas, vendor licence files, or downloaded vendor archives. Any
  match is a high-severity finding.
- `forbidden_terms`: literal strings that indicate proprietary source or asset
  reuse. Each term has `severity`, `allow_paths`, and optional `expires_on`.
  `ASTPP` and the four source-page URL fragments may only appear in allowlisted
  documentation.
- `claim_rules`: path globs for claim-bearing docs. Each rule requires either a
  `Source basis:` line or, for `docs/master-plan.md`, the canonical
  `## 0. Source status and verification basis` heading, and at least one valid
  source ID (`S1`–`S4`). The check is deterministic: it verifies declarations,
  not natural-language semantics.
- `licence_policy`: `manifest` points to `licence-manifest.json`; `allow` lists
  licences from master-plan section 4, including the server-side condition for
  AGPL components; `deny` includes `BUSL`, `RSALv2`, `SSPL`, `proprietary`, and
  `unknown`.
- `exceptions`: array of `{id, rule_id, path_glob, reason, owner, expires_on}`.
  Expired or incomplete exceptions fail closed.

The checker reads only these two JSON files and repository text files. It does
not parse URLs from the manifest into network requests.

## 5. Checker behavior

`tools/cleanroomcheck` accepts:

- `-policy` (default `docs/compliance/cleanroom-policy.json`)
- `-report` (optional report directory)
- `-format` (`text` or `json`, default `text`)

Execution steps:

1. Load and validate the policy and licence manifest; unknown top-level keys
   fail as a policy error.
2. Walk the repository from the policy root, skipping only `exclude_paths`.
3. Read regular UTF-8 text files; binary or undecodable files are reported as
   unsupported and fail closed unless their path is excluded.
4. Apply forbidden artifact pattern checks.
5. Apply forbidden term checks with allow-path and exception evaluation.
6. Apply claim citation declaration checks.
7. Apply licence allow/deny checks to every manifest entry.
8. Print findings, optionally write `cleanroom-report.txt` and
   `cleanroom-report.json`, and exit `1` if any finding or policy error exists.

A finding contains `severity`, `rule_id`, `path`, `line`, `snippet`, and
`remediation`. Output ordering is deterministic: findings sort by path, line,
rule ID, then severity. A clean run prints `ok: clean-room audit 0 findings`.

## 6. Error handling

The command fails closed when:

- the policy or licence manifest is missing, malformed, or has unknown keys;
- an in-scope file cannot be read or decoded;
- a forbidden artifact, term, missing source-basis declaration, or denied
  licence is found;
- an exception is malformed, missing owner/reason/expiry, or expired;
- the report directory cannot be created when `-report` is set.

No finding is downgraded to a warning. No environment variable disables the
gate. The only bypass is an explicit, unexpired, reviewed exception in the
policy manifest.

## 7. CI and Make integration

`make cleanroom` runs:

`cd tools/cleanroomcheck && go vet ./... && go test ./... && go run . -policy ../../docs/compliance/cleanroom-policy.json -report ../../artifacts/cleanroom`

`make test` depends on both `compliance` and `cleanroom`. CI adds a `cleanroom`
step before the data-layer start and smoke test, plus an artifact upload step
that runs even when the gate fails.

Reports are not committed. `artifacts/` is ignored. Operators can read the
latest local report under `artifacts/cleanroom/`.

## 8. Governance pack

The governance files define, without inventing external contacts:

- contribution workflow, local setup, branch/worktree convention, test commands,
  review expectations, and clean-room rules in `CONTRIBUTING.md`;
- community standards and private maintainer reporting in `CODE_OF_CONDUCT.md`;
- roles, lazy-consensus decisions, RFC process, release authority, and conduct
  enforcement in `GOVERNANCE.md`;
- vulnerability reporting through GitHub private vulnerability reporting in
  `SECURITY.md`;
- RFC, release, maintainer onboarding, and clean-room review templates under
  `docs/governance/`;
- bug/feature issue templates and a PR checklist under `.github/`.

No email addresses, external foundation affiliation, or trademark claims are
introduced.

## 9. Verification

Implementation is complete when:

- `go vet ./...` and `go test ./...` pass in `tools/cleanroomcheck`;
- `make cleanroom` passes on the repository baseline with zero findings;
- fixtures prove each rule, allow path, exception expiry, licence denial,
  unsupported file handling, and deterministic output;
- `make test` remains green;
- CI YAML parses and includes the blocking step and report upload;
- governance files exist, contain no placeholders, and name GitHub private
  reporting as the contact channel;
- no application source, proto, schema, Compose, or Kubernetes file changes.

## 10. Scope boundaries

Out of scope:

- ASTPP source, schema, or licensed asset acquisition or reading.
- Network access, SBOM generation, or vendored licence-file collection.
- Runtime retention/prune jobs, KYC, lawful intercept, or regional residency.
- Cloud IaC, NetworkPolicies, TLS, ingress, HA, backups, and upgrades.
- Re-running or replacing the P7 install timing manual gate.
- Changing service behavior, money rules, APIs, or generated code.

## 11. Risks

- **False positives:** allow paths and exceptions are explicit and reviewed;
  fixtures cover each allowance.
- **False negatives:** the checker is textual and declaration-based; the audit
  guide states clearly that it enforces repository policy, not semantic proof.
- **Policy drift:** the policy version is committed, self-scanned, and reviewed
  with every change.
- **Licence nuance:** the checker validates declared licences against master-plan
  policy; upstream licence-file verification remains a separate build gate.
- **Governance staleness:** release and maintainer documents cite this repository
  process and avoid external identifiers that could drift.
