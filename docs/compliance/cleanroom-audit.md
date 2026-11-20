# Clean-room audit

Source basis: S1, S2, S3, S4

Feature claims come only from the four ASTPP marketing pages cited in `docs/master-plan.md` sections 0 and 8. No proprietary source, schema, or licensed asset was consulted.

## Run

From the repository root:

```bash
make cleanroom
```

The command runs `go vet`, unit tests, and the checker. It exits `0` only with zero findings. Policy, manifest, or I/O errors exit `2` and write no report; findings exit `1` and still write reports.

For direct use:

```bash
cd tools/cleanroomcheck
go run . -policy ../../docs/compliance/cleanroom-policy.json -report ../../artifacts/cleanroom
go run . -policy ../../docs/compliance/cleanroom-policy.json -format json
```

## Policy

- `docs/compliance/cleanroom-policy.json` defines exclusions, forbidden artifacts, forbidden terms, claim-bearing docs, licence rules, and exceptions.
- `docs/compliance/licence-manifest.json` declares components and licences from `docs/master-plan.md` section 4.
- Allow-list rules may carry a `distribution` context (for example, GPL-3.0 is allowed only `separate-binary` for rtpengine and `tool` for Ansible); a component in an unapproved context is a finding.
- A forbidden term may carry `expires_on` (`YYYY-MM-DD`); after that date its allow paths stop applying and the term applies everywhere.
- Reports are written to `artifacts/cleanroom/` when `-report` is used. Reports are ignored and are not committed.

## Findings

Each finding names severity, rule ID, path, line, snippet, and remediation. Any finding fails CI. Do not downgrade a finding to a warning.

Rule IDs:

- `artifact.forbidden` — a vendor artifact path matched the policy.
- `term.forbidden` — a proprietary marker appeared outside an allow path.
- `claim.source_basis` — a claim-bearing doc lacks a source-basis declaration or S1-S4 marker.
- `licence.denied` — a declared licence is explicitly denied.
- `licence.not_allowed` — a licence is unlisted or used in the wrong distribution context.

The claim check is declaration-based: it verifies that the text contains a source-basis statement and an `S1`–`S4` token, not that citations are semantically accurate.

## Exceptions

Exceptions are rule-scoped: an exception for `term.forbidden` on a path exempts every forbidden term there, not one term. Exceptions require an ID, rule ID, path glob, reason, owner, and expiry date (`YYYY-MM-DD`). Expired or incomplete exceptions fail closed. Add exceptions only after review, keep them narrow, and remove them with the material they cover.

## Scope

The checker enforces repository policy over text files. It does not verify upstream licence files, detect semantic paraphrase, generate SBOMs, read proprietary material, access the network, or prove runtime behavior.
