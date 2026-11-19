# POPIA control pack — design

Date: 2026-11-19
Status: approved (design)
Slice: P6 (security and compliance hardening), second slice

## 1. Context

P6 begins with the toll-fraud MVP, which is complete. The remaining P6 items
in the roadmap are gVisor rollout, a POPIA pack, and penetration-test
readiness. The master plan's regulatory mapping requires purpose declarations,
data minimisation, documented Information Officer and PAIA duties, security
safeguards, breach notification, cross-border-transfer controls, RICA
assumptions, and a compliance documentation pack per deployment.

This slice delivers the first POPIA pack as an engineering-ready draft backed
by a machine-readable manifest and a CI validation gate. It deliberately does
not add runtime deletion jobs, KYC, lawful-intercept hooks, residency
enforcement, or new operator UIs. Legal interpretation and sign-off remain
explicit counsel gates.

Goals:

- Declare purpose, lawful basis, classification, and retention for every
  current data store.
- Produce a POPIA control matrix with evidence links and a blank sign-off
  block.
- Add PAIA, breach, and retention/legal-hold runbooks.
- Make the manifest mechanically checkable in CI.

Non-goals (YAGNI):

- Runtime CDR/financial-record deletion workers.
- KYC identity-document capture or RICA workflow.
- Lawful-intercept hooks at the SIP edge.
- Data-residency enforcement and jurisdiction profiles.
- External penetration testing or vulnerability remediation.
- Changes to billing, payment, identity, report, or notification behaviour.

Feature claims come only from `docs/master-plan.md` sections 1.1, 5, 6, and 7.
No ASTPP source, schema, or licensed asset was consulted. The seven-year and
one-year retention values below are conservative engineering defaults, not
legal advice.

## 2. Approach

Three approaches were considered:

1. Manifest plus validation gate.
2. Per-store runtime retention jobs.
3. Documentation-only control matrix.

Approach 1 is chosen. It gives reviewable, testable evidence for the POPIA
controls without touching production data flows. Approach 2 is stronger but
requires coordinated cleanup across ClickHouse and several Postgres databases,
with idempotency and legal-hold concerns outside this slice. Approach 3 is too
weak: a prose matrix cannot detect drift or missing evidence.

## 3. Architecture

New paths:

- `docs/compliance/README.md` — index of the pack and its evidence.
- `docs/compliance/data-classification.json` — machine-readable source of
  truth for purposes, classifications, storage targets, and retention.
- `docs/compliance/popia-control-matrix.md` — engineering-ready matrix with
  evidence links, counsel gates, and a blank sign-off block.
- `docs/compliance/runbooks/information-officer-paia.md` — role and PAIA
  request-handling runbook.
- `docs/compliance/runbooks/breach-notification.md` — 72-hour internal
  escalation and regulator-notification workflow template.
- `docs/compliance/runbooks/retention-legal-hold.md` — interpretation of
  retention targets and the manual legal-hold exception.
- `tools/compliancecheck/go.mod` — standalone module `omnifoni/compliancecheck`
  using only the Go standard library.
- `tools/compliancecheck/main.go` — CLI that loads and validates the manifest.
- `tools/compliancecheck/main_test.go` — validator tests.

No new runtime service, database, Kafka topic, HTTP endpoint, or gRPC API is
added. The tool has no network or database access.

## 4. Manifest

`data-classification.json` has this top-level shape:

```json
{
  "version": 1,
  "controls": [
    {
      "id": "popia-s11",
      "instrument": "POPIA s11",
      "class_ids": ["identity", "billing_financial"]
    }
  ],
  "classes": [
    {
      "id": "identity",
      "purpose": "Authenticate operators and authorise tenant-scoped access.",
      "lawful_basis": "POPIA s11",
      "personal_data": [
        {
          "field": "email",
          "classification": "contact",
          "purpose": "Account identification and notification delivery."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "identity",
          "table": "users",
          "columns": ["email", "role", "tenant_id"]
        }
      ],
      "retention": {
        "kind": "until_account_closure",
        "evidence": ["services/identity/migrate.sql"],
        "counsel_required": true,
        "assumption": "Closure criteria and retention after closure need counsel validation."
      },
      "enforcement_status": "documented_target"
    }
  ]
}
```

Rules:

- `version` must be a positive integer; only version 1 is accepted.
- Every class `id` is unique and referenced by at least one control.
- `purpose`, `lawful_basis`, and each `personal_data[].purpose` are non-empty.
- Allowed `classification` values are `identifier`, `contact`, `network`,
  `financial`, `content`, `security`, and `none`.
- Allowed retention `kind` values are `fixed_days`, `until_deleted`, and
  `until_account_closure`.
- `fixed_days` requires an integer `days` greater than zero.
- Every retention `evidence` path exists in the repository.
- Every storage target uses a known `system`, `database`, and `table` from the
  validator's allowlist.
- Every `counsel_required: true` row has non-empty `assumption` text.
- No control references an unknown class ID.

### 4.1 Data classes and defaults

| Class | Storage | Default retention | Enforcement evidence |
|---|---|---|---|
| `cdr` | `mediation.cdrs`, `reports.cdrs`, Kafka `cdrs.raw`/`cdrs.normalized` | 90 days | Documented target; retention worker is a follow-up |
| `charging_financial` | `charging.ledger` | 2555 days | Documented target; counsel validation required |
| `charging_session` | `charging.sessions` | 90 days | Documented target |
| `billing_financial` | `billing.invoice_lines`, `billing.cdr_period_totals` | 2555 days | Documented target; counsel validation required |
| `payment` | `payments.payments` | 2555 days | Documented target; counsel validation required |
| `identity` | `identity.tenants`, `identity.users`, `identity.accounts` | Until account closure | Documented target; counsel validation required |
| `identity_session` | `identity.sessions` | 1 day | `services/identity/identity.go` (`sessionTTL = 24h`) |
| `fraud_security` | `fraud.alerts`, `fraud.reservations`, `fraud.processed_cdrs`, `fraud.daily_usage` | 365 days | Documented target |
| `fraud_configuration` | `fraud.fraud_controls`, `fraud.prefix_blocks`, `fraud.cli_blocks`, `fraud.risk_prefixes` | Until deleted | Documented target |
| `sbc_security` | `sbc.auth_failures`, `sbc.active_calls` | 30 days | Documented target |
| `report_artifact` | `reports_sched.report_artifacts` | 90 days | `REPORTS_SCHED_RETENTION_DAYS` prune job |
| `report_configuration` | `reports_sched.schedules` | Until deleted | Documented target |
| `notification_outbox` | `notifications.outbox` | 90 days | Documented target |
| `configuration_catalogue_routing` | `catalogue.rates`, `routing.rules`, `routing.lcr_routes`, `routing.trunks`, `sbc.barring_rules`, `sbc.trunk_limits` | Until deleted | Documented target |

`enforcement_status` is `existing` only where a code or configuration
mechanism already prunes or expires the data. Everywhere else it is
`documented_target`; the matrix and runbook must say so plainly.

## 5. Control matrix

`popia-control-matrix.md` uses these columns:

- Obligation
- Instrument
- Design control from `docs/master-plan.md` section 6
- Manifest class IDs
- Evidence path
- Status (`implemented`, `documented_target`, or `counsel_required`)
- Assumption / counsel gate
- Engineering sign-off
- Information Officer sign-off
- Legal sign-off

Rows cover POPIA ss10, 11, 19, 22, 55-56, and 72; RICA subscriber
verification and interception assumptions; ICASA licence classification;
traffic-data retention; AU Malabo Convention; and AI governance. Sign-off
cells are blank in this slice. The document header states that it is an
engineering-ready draft and not legal advice.

## 6. Runbooks

- `information-officer-paia.md` records the Information Officer role, request
  intake fields, identity verification, tenant scoping, response assembly from
  the manifest, and audit logging. The target response window is recorded as
  30 days with a note that counsel must confirm the statutory deadline.
- `breach-notification.md` defines detection intake, containment, evidence
  preservation, 72-hour internal escalation, regulator-notification decision,
  and a notification template. Legal escalation remains a gate.
- `retention-legal-hold.md` explains `fixed_days`, `until_deleted`, and
  `until_account_closure`, records how to apply a manual legal hold, and lists
  the follow-up runtime retention work without claiming it exists.

## 7. CLI and CI gate

`tools/compliancecheck` exposes no flags. It reads the manifest path as its
only argument.

Success output:

```text
ok: compliance manifest 1 (14 classes)
```

Failure output lists each validation error with a JSON path and exits non-zero.

Add to the root `Makefile`:

```make
compliance:
	cd tools/compliancecheck && go vet ./... && go test ./... && go run . ../../docs/compliance/data-classification.json
```

Add `$(MAKE) compliance` as the first command in `make test`. This keeps the
gate part of the existing full verification command.

## 8. Tests

- `TestValidManifest` loads the checked-in manifest with an explicit repo-root
  path relative to the tool module and expects success.
- Table-driven invalid fixture tests cover duplicate ID, missing purpose,
  invalid classification, invalid retention kind, non-positive `fixed_days`,
  missing evidence path, unknown storage target, dangling control reference,
  and missing counsel assumption text.
- `TestAllowedStorageTargets` keeps the known table allowlist in sync with the
  manifest and fails when a class references an undeclared table.

## 9. Verification

Run and record:

```bash
make compliance
make protoc
git diff --exit-code
gofmt -l tools/compliancecheck
make test
make smoke
bash ops/sip-e2e.sh
docker compose config -q
```

No dev-data resets are required. The design-only files do not change runtime
behaviour, so image rebuilds are not required.

## 10. Sign-off block

The matrix ends with:

```text
Engineering review:  ____________________  Date: __________
Information Officer: ____________________  Date: __________
Legal / counsel:      ____________________  Date: __________
```

No sign-off is filled in by this slice.

## 11. Risks and follow-ups

- Retention targets without runtime deletion are a known gap. The matrix must
  not describe them as enforced.
- RICA and traffic-retention interpretations require counsel.
- The storage allowlist is manually maintained; new tables require a manifest
  and allowlist update.
- gVisor rollout, penetration-test readiness, KYC, lawful intercept, residency
  enforcement, and jurisdiction profiles remain separate P6 work.

## 12. Acceptance criteria

- All specified paths exist and the manifest validates with `make compliance`.
- The matrix covers every master-plan §6 row with evidence and blank sign-off
  cells.
- All current stores are represented exactly once in the manifest.
- Runbooks exist for PAIA, breach notification, and retention/legal hold.
- `make test`, `make smoke`, `bash ops/sip-e2e.sh`, `make protoc`, and
  `docker compose config -q` pass.
- No runtime service behaviour changes.
