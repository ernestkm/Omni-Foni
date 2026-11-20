# Compliance pack

This pack is an engineering-ready draft, not legal advice. Feature claims come
only from the four ASTPP marketing pages cited in `docs/master-plan.md`
sections 0 and 8. Design controls are taken from sections 5 and 6.

- `data-classification.json` — authoritative purpose, classification, storage,
  and retention declarations.
- `popia-control-matrix.md` — control matrix with evidence and blank sign-off
  cells.
- `runbooks/information-officer-paia.md` — Information Officer and PAIA request
  handling.
- `runbooks/breach-notification.md` — breach escalation and notification
  template.
- `runbooks/retention-legal-hold.md` — retention defaults and manual legal
  holds.

Run `make compliance` from the repository root to validate the manifest and
docs. The validator is stdlib-only Go and has no network or database access.
