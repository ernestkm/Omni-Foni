# Retention and legal-hold runbook

Status: engineering-ready draft. This runbook is not legal advice.

## Retention kinds

- `fixed_days`: retain for the declared number of days. Where no runtime
  deletion job exists, this is a documented target, not an enforced control.
- `until_deleted`: retain until an operator or customer deletes the record.
- `until_account_closure`: retain until the account-closure process completes.

Identity sessions expire after 24 hours in `services/identity/identity.go`.
Scheduled report artifacts are pruned by `REPORTS_SCHED_RETENTION_DAYS` in
`services/reports/main.go`. All other retention entries are documented targets.

## Legal hold

1. Record the hold reference, scope, start date, and authorising counsel.
2. Identify affected class IDs and storage targets from
   `../data-classification.json`.
3. Pause any planned retention work for those targets.
4. Notify the Information Officer and the system owners.
5. Record hold release with date, authoriser, and scope before normal
   retention resumes.

## Follow-up work

Runtime deletion workers for CDRs, financial records, fraud alerts, SBC
security data, and notification outbox rows are not implemented by this pack.
A future slice must define ordering, idempotency, observability, and legal-hold
interlocks before deleting production data.
