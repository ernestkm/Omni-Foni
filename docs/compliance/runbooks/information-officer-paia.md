# Information Officer and PAIA runbook

Status: engineering-ready draft. This runbook is not legal advice.

## Role

The deployment's Information Officer is the named operator contact for
personal-information requests. The repository does not assign a person; the
deployment record must do so.

## Intake

Record:

- request date and reference;
- requester identity and contact details;
- requested information or purpose;
- tenant or account scope claimed by the requester;
- requested format.

## Verification

1. Verify requester identity using operator-approved evidence.
2. Confirm the tenant and account scope with the identity registry.
3. Do not disclose another tenant's data. Resellers are scoped to their own
   tenant; users cannot list accounts.

## Response assembly

1. Find relevant classes in `../data-classification.json`.
2. Use the manifest's storage targets as the collection checklist.
3. Export only the requester's in-scope data.
4. Redact third-party personal information not belonging to the requester.
5. Record what was searched, what was disclosed, and why any item was withheld.

Target response window: 30 days. Counsel must confirm the statutory deadline
and permitted grounds for refusal.
