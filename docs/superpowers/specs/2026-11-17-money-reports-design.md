# Money reports: payments, charges history and low balance

Status: approved design, 2026-11-17. Slice two of P5.

## 1. Context

P5 slice one (`services/reports`) ingests `cdrs.normalized` into ClickHouse and serves
`ListCdrs`/`Summarize` plus the authenticated HTTP facade. The P5 acceptance target is a
report suite parity checklist against the S1 report list in `docs/master-plan.md` §1.1:
CDR and summary (done), audit log, product usage, email history, payment, commission,
low balance, advance live call, activity, charges history.

This slice adds the three money reports that can be built from data the platform already
stores, without new stores or Kafka topics:

- **Payment report** — every gateway payment recorded by `services/payments`.
- **Charges history** — money moved out of account balances by the OCS, one row per call.
- **Low balance** — accounts at or below a caller-supplied balance threshold.

## 2. Scope

In scope:

- Read-only RPCs in the owning services: `payments.v1.Payments/ListPayments`,
  `charging.v1.Charging/ListCharges`, `charging.v1.Charging/ListLowBalance`.
- Matching RPCs on `reports.v1.Reports` that parse report date semantics once and forward
  epoch milliseconds to the owners.
- HTTP endpoints on the reports facade with the same bearer/tenant scoping as slice one.
- Wiring, tests, smoke probe, `AGENTS.md` update.

Out of scope: CSV/PDF export, scheduled or emailed reports, dashboards, audit/product/
email/commission/activity/advance-live-call reports, country or prefix grouping,
historical backfill, currency conversion, and any change to how charges or payments are
recorded.

## 3. Money and time rules

- All amounts are integer minor units (1/10000 currency), unchanged from the rest of the
  platform. `rate_per_sec` is not part of these reports.
- `charges.amount_minor` is negative: it is the sum of the ledger deltas for the session
  (`ledger.amount_minor` is `-cost` per charge). The report does not flip the sign.
- `low balance` compares `accounts.balance <= threshold_minor`. The threshold is an
  integer in minor units, supplied by the caller, default `0`. Negative thresholds are
  valid (postpaid accounts legitimately go below zero).
- `from`/`to` are half-open `[from, to)` in UTC. Date-only values are accepted:
  `from=YYYY-MM-DD` is 00:00 UTC, `to=YYYY-MM-DD` is the next day 00:00 UTC. `from` and
  `to` are required for the two dated reports; a reversed range is `InvalidArgument`.
  Low balance has no date window: it is current state.

## 4. Source RPCs

Both services already expose gRPC; the new methods are additive and internal/trusted.
Filters use epoch milliseconds to keep one date parser (in reports) as the single
authority for report date semantics.

### 4.1 payments.v1.Payments/ListPayments

```proto
message ListPaymentsRequest {
  repeated string account_ids = 1; // empty = all accounts
  int64 from_ms = 2;               // inclusive
  int64 to_ms = 3;                 // exclusive
  string status = 4;               // "" = all, else recorded|void
  int32 limit = 5;                 // default 100, clamped 1..1000
  string cursor = 6;               // opaque, base64url of "<created_at_ms>|<payment_id>"
}

message Payment {
  int64 payment_id = 1;
  string event_id = 2;
  string account_id = 3;
  string gateway = 4;
  int64 amount_minor = 5;
  string status = 6;
  int64 created_at_ms = 7;
}

message ListPaymentsResponse {
  repeated Payment payments = 1;
  string next_cursor = 2; // "" when the page is the last one
}
```

Query: `payments` rows with `created_at` in the window, optional `status`, optional
`account_id = ANY($account_ids)`, keyset `(created_at, payment_id) < (cursor)` when a
cursor is given, `ORDER BY created_at DESC, payment_id DESC`, fetch `limit+1` to decide
`next_cursor`. Invalid `status` or cursor → `InvalidArgument`. Void payments are included
because the report surfaces `status`; callers that want recorded money only pass
`status=recorded`.

### 4.2 charging.v1.Charging/ListCharges

One row per OCS session (call), aggregated from `ledger`:

```proto
message ListChargesRequest {
  repeated string account_ids = 1;
  int64 from_ms = 2;
  int64 to_ms = 3;
  int32 limit = 4;   // default 100, clamped 1..1000
  string cursor = 5; // "<last_charged_at_ms>|<session_id>"
}

message Charge {
  string session_id = 1;
  string account_id = 2;
  int64 charged_seconds = 3; // sessions.charged_seconds
  int64 amount_minor = 4;    // sum(ledger.amount_minor), negative
  int64 balance_after = 5;   // balance after the latest ledger row
  int64 last_charged_at_ms = 6;
}

message ListChargesResponse {
  repeated Charge charges = 1;
  string next_cursor = 2;
}
```

Query shape:

```sql
SELECT l.session_id::text, l.account_id, coalesce(s.charged_seconds, 0),
       sum(l.amount_minor)::bigint,
       (array_agg(l.balance_after ORDER BY l.ledger_id DESC))[1],
       floor(extract(epoch FROM max(l.created_at)) * 1000)::bigint
FROM ledger l
LEFT JOIN sessions s ON s.session_id = l.session_id
WHERE (cardinality($1::text[]) = 0 OR l.account_id = ANY($1))
GROUP BY l.session_id, l.account_id, s.charged_seconds
HAVING max(l.created_at) >= $2 AND max(l.created_at) < $3
   AND ($4::bigint = 0 OR (max(l.created_at), l.session_id) < ($4, $5))
ORDER BY max(l.created_at) DESC, l.session_id DESC
LIMIT $6
```

The report window filters on the last charge of the session, so a call that started
before `from` but was still charging inside the window is included. A session with no
ledger rows (authorized then released without charging) has no charge and does not
appear.

### 4.3 charging.v1.Charging/ListLowBalance

```proto
message ListLowBalanceRequest {
  repeated string account_ids = 1; // empty = all accounts
  int64 threshold_minor = 2;       // balance <= threshold
  int32 limit = 3;                 // default 100, clamped 1..1000
  string cursor = 4;               // "<balance>|<account_id>"
}

message BalanceRow {
  string account_id = 1;
  string kind = 2;        // prepaid|postpaid
  int64 balance = 3;
  int64 credit_limit = 4;
  string currency = 5;
}

message ListLowBalanceResponse {
  repeated BalanceRow accounts = 1;
  string next_cursor = 2;
}
```

`ORDER BY balance ASC, account_id ASC`, keyset `(balance, account_id) > (cursor)` for
forward paging. Both kinds are returned; `kind` lets a caller filter client-side.

## 5. Reports API

`reports.v1.Reports` gains three methods mirroring HTTP, using the same limit/cursor
conventions as slice one:

```proto
message MoneyFilter {
  repeated string account_ids = 1;
  string from = 2; // required
  string to = 3;   // required, half-open, date-only to includes that day
}

message ListPaymentsRequest {
  MoneyFilter filter = 1;
  string status = 2;
  int32 limit = 3;
  string cursor = 4;
}

message ListPaymentsResponse {
  repeated Payment payments = 1;
  string next_cursor = 2;
}

message ListChargesRequest {
  MoneyFilter filter = 1;
  int32 limit = 2;
  string cursor = 3;
}

message ListChargesResponse {
  repeated Charge charges = 1;
  string next_cursor = 2;
}

message LowBalanceRequest {
  repeated string account_ids = 1;
  int64 threshold_minor = 2;
  int32 limit = 3;
  string cursor = 4;
}

message LowBalanceResponse {
  repeated BalanceRow accounts = 1;
  string next_cursor = 2;
}
```

`Payment`, `Charge` and `BalanceRow` are report-owned messages with the same fields as the
source rows (proto field numbers may differ; the adapter maps explicitly). Reports parses
`from`/`to` with the existing `parseBound` helper, requires both, and forwards
`from_ms`/`to_ms` to the owner. Reports applies the same limit default (100) and clamp
(1..1000) and passes the cursor through opaquely.

The reports gRPC server holds two narrow interfaces so it can be tested with fakes:

```go
type PaymentSource interface {
    ListPayments(context.Context, *paymentspb.ListPaymentsRequest) (*paymentspb.ListPaymentsResponse, error)
}
type ChargeSource interface {
    ListCharges(context.Context, *chargingpb.ListChargesRequest) (*chargingpb.ListChargesResponse, error)
    ListLowBalance(context.Context, *chargingpb.ListLowBalanceRequest) (*chargingpb.ListLowBalanceResponse, error)
}
```

A source error maps to gRPC `Unavailable` (`InvalidArgument` passes through), and HTTP maps
`InvalidArgument` to 400 and everything else to 503.

## 6. HTTP facade

```
GET /reports/payments?from=&to=&account_id=&status=&limit=&cursor=
GET /reports/charges?from=&to=&account_id=&limit=&cursor=
GET /reports/low-balance?threshold=&account_id=&limit=&cursor=
```

Auth flow is unchanged from slice one: bearer token → identity `GET /me` (non-200 → 401),
role `user` → 403, admin requires `tenant_id` (400 when absent), reseller forced to its own
tenant, identity `/accounts` failure → 502, account intersection with repeated
`account_id`, empty tenant scope → empty 200. `threshold` parses as int64 minor units and
defaults to 0; a non-integer is 400. Responses mirror the proto JSON names
(`payments`, `charges`, `accounts`, `next_cursor`) with money as integers and no floats.

## 7. Configuration and deployment

| Env | Default | Purpose |
|---|---|---|
| `REPORTS_PAYMENTS_ADDR` | `payments:50060` | payments gRPC |
| `REPORTS_CHARGING_ADDR` | `charging:50051` | charging gRPC |

`services/reports` go.mod adds `replace` directives for `omnifoni/payments` and
`omnifoni/charging`; its Dockerfile COPYs `payments/` and `charging/`. Compose adds both
envs and `depends_on` entries (payments, charging started). No new ports, databases,
tables, or Kafka topics.

## 8. Error handling

- Source unavailable/timeout → gRPC `Unavailable`, HTTP 503. No partial pages, no zeros.
- Invalid filter (missing `from`/`to`, reversed range, bad `status`, bad cursor) → 400.
- Unknown account ids simply match nothing (the HTTP layer already intersected them).
- Payments with `status=void` are returned when no status filter is given; totals are not
  computed by this slice (the reconciliation service owns totals).

## 9. Testing

- `services/payments`: `TestListPayments` against real Postgres — window boundaries,
  status filter, account filter, keyset paging (insert enough rows for two pages), void
  rows present with `status=void`, invalid status/cursor errors.
- `services/charging`: `TestListChargesAggregatesLedger` — seed a session and several
  ledger rows, assert sum, latest `balance_after`, `charged_seconds`, window filtering on
  last charge, paging; `TestListLowBalance` — threshold boundary (`balance == threshold`
  included), kind surfaced, ordering, cursor, account filter.
- `services/reports`: gRPC tests with fake sources — date parsing/forwarding, limit clamp,
  missing `from`/`to` → `InvalidArgument`, source error → `Unavailable`; HTTP tests with
  the existing fake identity — 401/403/400/502, tenant scoping, empty tenant, threshold
  parsing, `account_id` intersection.
- `cmd/smokereports` also calls `ListPayments`, `ListCharges` and `ListLowBalance` over
  gRPC with a wide window and asserts no error, proving the compose wiring.
- `make test` and `make smoke` remain the gate.

## 10. Risks and open questions

- `ListCharges` groups by session at read time; with millions of ledger rows the
  `(account_id, created_at)` index helps the account filter but a global unfiltered scan
  is O(ledger). Acceptable for v1 (operator reports are account-scoped); a rollup table is
  a later optimization if the bench shows a problem.
- Session rows are never deleted today, so `charged_seconds` is available for history. If
  a retention policy later prunes sessions, `charged_seconds` falls back to 0 via
  `coalesce`; the amount and balance remain correct.
- Payment report has no currency column because the payments table does not store one;
  currency conversion and multi-currency reporting remain open.
