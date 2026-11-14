# Trunk id on the CDR — design

Date: 2026-11-14
Status: approved (design dialogue), pending implementation plan

## Problem

Switching's call path resolves a trunk via Routing's `ResolveLcr` (`P4 slice one`) but drops
it immediately after resolution: the `call` struct never stores it and `mediation.v1.CallCdr`
has no field for it. Downstream (mediation, billing) therefore cannot attribute calls — or
billed seconds — to the trunk that carried them. AGENTS.md records this gap: "the trunk id
itself is not yet carried onto the call/CDR."

## Scope

Carry the trunk id from LCR resolution onto the call and through the CDR pipeline as far as
`cdrs.normalized`. Billing is untouched (no schema change, no invoice-line column). Mediation
treats the field as pass-through: no validation, no new dead-letter classes.

Out of scope: per-trunk reporting RPCs, invoice_lines schema changes, Kamailio/SIP edge work.

## Decision

Add a proto field to `CallCdr` (option 1). Alternatives considered and rejected: a Kafka
message header (splits the record across channels, invisible to proto consumers) and having
mediation re-resolve the trunk via Routing (new dependency, nondeterministic — rate/rule
tables can drift between call time and CDR time).

## Changes

### Proto (`proto/mediation/v1/mediation.proto`)

`CallCdr` gains:

```proto
string trunk_id = 9; // LCR-resolved trunk; empty when routing is unwired or did not match
```

Empty is legal: dev/test runs construct the registry without a routing resolver, and those
runs still produce billable calls. Regenerate stubs with `make protoc`.

### Switching (`services/switching/calls.go`)

- `call` struct gains `trunkID string`.
- `StartCall` sets it right after `ResolveLcr` returns a non-empty trunk (the value is already
  in scope at the point the call is created).
- `buildCDR` copies `c.trunkID` into the published `CallCdr`. Every end reason
  (`normal`, `balance_exhausted`, `ocs_unavailable`) carries it; denied calls publish no CDR,
  so they need no trunk value.

### Mediation (`services/mediation`)

The normalizer copies `TrunkId` verbatim into the normalized CDR. No validation: an empty
trunk_id must never reject a CDR to `cdrs.rejected`, otherwise every CDR from a
routing-unwired run would dead-letter.

### Billing

No change.

## Error handling

No new failure modes. The field is optional-by-shape (proto3 string), mediation does not
branch on it, and switching's fail-closed denial paths are unchanged.

## Testing

- Switching: assert the published CDR carries the LCR-resolved trunk id (existing CDR-capture
  test harness in `main_test.go`).
- Mediation: assert pass-through of a populated `trunk_id`, and that an empty `trunk_id`
  still normalizes (does not dead-letter).
- Full suite: `make test`; stack smoke: `make smoke`.

## Acceptance criteria

- A call started through a registry wired with routing publishes a CDR whose `trunk_id`
  equals the trunk `ResolveLcr` returned.
- A call started without a routing resolver publishes a CDR with an empty `trunk_id`, which
  mediation normalizes normally.
- `AGENTS.md` P4 slice one note updated to drop the "not yet carried" caveat.
