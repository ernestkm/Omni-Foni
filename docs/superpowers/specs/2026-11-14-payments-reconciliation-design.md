# Payments reconciliation slice — design

Date: 2026-11-14
Status: approved design, pre-plan
Scope: P2 remainder — the money-triangle reconciliation surface plus the payments gRPC it needs.

## 1. Goal

Close the P2 acceptance criterion "invoice totals reconcile to rated CDR sums to the cent" with a real, queryable reconciliation surface, and connect payments to invoices. One new service answers, for any account and period: is billing consistent, and did the customer pay?

## 2. Decisions taken in brainstorming

- **Scope**: full money triangle — invoices vs rated-CDR sums, payments vs invoice totals, and the combined per-account report.
- **Shape**: pull endpoints *and* a continuous sweep (not pull-only).
- **Placement**: new reconciliation service; it speaks gRPC to billing and payments, never to their databases (identity precedent).
- **Drift reaction**: all three of structured warn logs, per-comparison status fields in responses, and a `reconciliation.drift` Kafka topic.
- **PayFast**: keep the unified Stripe-style HMAC for every gateway including PayFast in this slice. The real PayFast form+MD5 scheme is its own follow-up slice.
- **Approach**: gRPC aggregator (option A). The rated-CDR total for a period comes from a billing ingest ledger written transactionally with invoice lines, not from re-rating CDRs or from ClickHouse.

## 3. Architecture

```
billing  :50054 -- GetPeriodLedger, GetInvoicesByAccounts, ListAccountsForPeriod --> reconciliation :50059
payments :50060 -- GetPaymentsTotals, ListAccountsForPeriod ----------------------> reconciliation :50059
reconciliation :50059 -- DriftEvent (proto) --> Kafka topic reconciliation.drift
```

Reconciliation owns no database. Billing and payments keep owning their schemas; the only cross-service coupling is their gRPC contracts.

## 4. Components

### 4.1 billing — ingest ledger + two RPCs

- New table `cdr_period_totals(period TEXT PRIMARY KEY, cdr_count BIGINT, amount_sum BIGINT)`.
- `Biller.Handle` writes the invoice line inside a Tx; when the `ON CONFLICT (call_id) DO NOTHING` insert reports `RowsAffected == 1`, the same Tx upserts the ledger for that CDR's period (`cdr_count += 1`, `amount_sum += amount_minor`). Replay inserts affect neither table, so crash replays stay exactly idempotent. If the ledger upsert fails, the whole Tx rolls back and the consumer retries the CDR (at-least-once preserved).
- Invariant: for every period, `SUM(invoice_lines.amount_minor)` equals the ledger's `amount_sum`, and `COUNT(*)` equals `cdr_count`. The ledger is the rated-CDR sum by construction: it increments only when a CDR was actually rated and billed.
- New RPCs on `proto/billing/v1/billing.proto`:
  - `GetPeriodLedger(period)` → `{cdr_count, amount_sum}`; unknown period returns zeros (an empty period is consistent, not an error).
  - `ListAccountsForPeriod(period)` → distinct `account_id`s from `invoice_lines`.

### 4.2 payments — gRPC surface on :50060

- The HTTP webhook stays on :50055 unchanged. A second listener serves gRPC on :50060 (`PAYMENTS_GRPC_ADDR`).
- New proto `proto/payments/v1/payments.proto`:
  - `GetPaymentsTotals(period)` → per-account `{payment_count, amount_sum}` batch (mirrors billing's `GetInvoicesByAccounts`).
  - `ListAccountsForPeriod(period)` → distinct `account_id`s from `payments`.
- Payment periods are `YYYY-MM` of `created_at` in UTC.

### 4.3 reconciliation — :50059

- Proto `proto/reconciliation/v1/reconciliation.proto`:
  - `ReconcileAccount(account_id, period)` → the two comparisons for one account.
  - `ReconcilePeriod(period)` → rows for every account in the union of both sides' account lists.
- Result rows carry per-comparison status fields:
  - **ledger-vs-invoices** is period-wide, reported once per period, not per account: status `OK` or `DRIFT`, with expected/actual/delta minor units on both the count and the amount.
  - **payments-vs-invoices** per account: status `PAID | UNDERPAID | OVERPAID | UNPAID | NO_ACTIVITY`, with invoice total, payments sum, and delta.
- Sweep: interval env `RECONCILIATION_SWEEP_INTERVAL` (default 15m, `0` disables). Scans the current and previous month. Genuine drift is logged with a warn and published to Kafka; payment deltas are logged at info and are never drift events (accounts-receivable semantics, not reconciliation failure).
- Kafka publish is best-effort (`ProduceSync`, errors logged, sweep continues).

### 4.4 DriftEvent

`reconciliation.v1.DriftEvent { period, account_id, kind, expected_minor, actual_minor, delta_minor, expected_count, actual_count, detected_at_ms }`; `account_id` is empty for ledger drift; `kind` is `ledger_amount_mismatch` or `ledger_count_mismatch`. Published to `reconciliation.drift` as proto bytes, like CDR topics.

## 5. Error handling

- Reconciliation is read-only: API callers get gRPC errors on upstream failure; the sweep logs and retries next interval. Nothing fail-closes on reconciliation.
- Missing period data on any side is `NO_ACTIVITY`/zeros, not an error.
- Money comparisons are integer sums; no rounding anywhere in this slice.

## 6. Compose / ops / docs

- New `services/reconciliation` (standard Dockerfile; no sibling imports).
- Envs: `BILLING_ADDR=billing:50054`, `PAYMENTS_ADDR=payments:50060` (gRPC), `RECONCILIATION_KAFKA=kafka:9092`, `RECONCILIATION_ADDR=:50059`, `RECONCILIATION_SWEEP_INTERVAL`.
- `make protoc` regenerates billing, payments, reconciliation stubs.
- AGENTS.md gains the P2 reconciliation bullet; `make smoke` gains one `ReconcilePeriod` call expecting all-OK.

## 7. Testing

- billing: ledger increments only on genuinely-new inserts; replayed CDRs leave ledger and invoice sums untouched; sums stay equal across crash replays; `GetPeriodLedger` zeros for unknown periods.
- payments: `GetPaymentsTotals` sums and account list; replays still single-row (webhook idempotency unchanged).
- reconciliation: comparison logic unit-tested with fake clients; e2e on the real stack seeds billing and payments DBs directly (dev-only TRUNCATE pattern, identity-style) and asserts triangle statuses through `ReconcilePeriod`; a direct ledger corruption produces a `DriftEvent` on `reconciliation.drift`; the sweep function is invoked directly (test hook), never by waiting for the ticker.

## 8. Out of scope

- Real PayFast form+MD5 verification (follow-up slice).
- Accounts-receivable aging, cross-period payment application, invoice documents.
- ClickHouse-backed reconciliation (P5 may later replace or supplement the ledger).
- Continuous-sweep alerting integrations beyond logs + Kafka (P0/P6 concerns).
