# Reports: prefix grouping and CSV export

Status: approved design, 2026-11-17. Slice three of P5.

## 1. Context

P5 slices one and two built the analytics store and seven reports over it: CDR list,
grouped summary, payments, charges history and low balance (plus the internal summary
groupings day/destination/account/trunk). The slice-one design deferred country reporting
as "a later prefix-grouping slice", and both slices excluded CSV export.

This slice adds those two capabilities without new stores, services or dependencies:

- **Prefix grouping** — a fifth `Summarize` grouping that buckets destinations by a
  caller-chosen number of leading digits.
- **CSV export** — every existing report endpoint can answer `format=csv` with the same
  filters, auth and tenant scoping as JSON.

## 2. Scope

In scope:

- `reports.v1.GroupBy` gains `PREFIX = 5`; `SummarizeRequest` gains `int32 prefix_len = 3`.
- `services/reports` store, gRPC handler and HTTP facade changes for the new grouping.
- `format=csv` on all five HTTP report endpoints: `GET /reports/cdrs`,
  `/reports/summary`, `/reports/payments`, `/reports/charges`, `/reports/low-balance`.
- A shared CSV streaming helper with a 10,000-row cap.
- Tests, smoke assertion, `AGENTS.md` update.

Out of scope: country names or any lookup dataset, CSV over gRPC, async export jobs or
download history, scheduled/emailed reports, dashboards, prefix grouping on the money
reports (they group by account only), currency conversion, and backfill.

## 3. Prefix grouping

### 3.1 Contract

```proto
enum GroupBy {
  GROUP_BY_UNSPECIFIED = 0;
  DAY = 1;
  DESTINATION = 2;
  ACCOUNT = 3;
  TRUNK = 4;
  PREFIX = 5;
}

message SummarizeRequest {
  CdrFilter filter = 1;
  GroupBy group_by = 2;
  int32 prefix_len = 3; // only for PREFIX; 0 = default 3, valid 1..15
}
```

- `prefix_len` is ignored for every other grouping.
- `0` means the default of 3; an explicit value outside `1..15` is `InvalidArgument`.
  The proto cannot distinguish "unset" from 0, so 0 is always the default.
- The key is the leftmost `prefix_len` digits of `destination`. A destination shorter
  than `prefix_len` groups under itself, so a 2-digit destination with `prefix_len=3`
  keys as those 2 digits.
- Keys are plain digit strings (`"278"`, `"27"`), never decorated with country names.

### 3.2 Store

`Store.Summarize` takes a `prefixLen int` argument. When the grouping is prefix, the
query selects `left(destination, ?)` as the key with `prefixLen` bound as `uint8`.
The handler validates the range before calling the store; the store trusts the value
and does not re-validate. All other groupings pass `0` and are unchanged.

## 4. CSV export

### 4.1 Selection

Every report endpoint accepts `format`. Absent or `json` returns today's JSON; `csv`
returns CSV; any other value is `400 {"error":"invalid format"}`. `format` is validated
after auth and scoping, alongside the other argument errors.

### 4.2 Body

- Header row uses the same snake_case names as the JSON bodies.
- One row per data row; values are written raw — money stays integer minor units and
  timestamps stay epoch milliseconds (no locale formatting, no currency symbols).
- `summary` writes its data rows and then one `TOTAL` row whose `key` is `TOTAL` and
  whose calls/seconds/amount_minor are the response totals.
- Response headers: `Content-Type: text/csv; charset=utf-8` and
  `Content-Disposition: attachment; filename="reports-<report>-<YYYY-MM-DD>.csv"`,
  where the date is the request's UTC date and `<report>` is one of
  `cdrs|summary|payments|charges|low-balance`.
- There is no cursor or paging metadata in CSV.

### 4.3 Paging and cap

- CSV ignores the client's `limit` and `cursor`. It calls the API with `Limit=1000`
  and follows `next_cursor` until the cursor is empty or 10,000 data rows have been
  written, whichever comes first. Reaching the cap stops without fetching another page.
- The summary `TOTAL` row is extra and does not count toward the cap.
- Rows are streamed page by page through `encoding/csv`; the server does not buffer the
  whole export.
- The first page is fetched before any response bytes are written, so auth, argument and
  source errors still produce the normal JSON error with the normal status. A failure
  after streaming starts is logged and aborts the response; the status cannot change.

### 4.4 Helper

One generic helper in `http.go`:

```go
func streamCSV(w http.ResponseWriter, r *http.Request, report string,
	header []string, fetch func(cursor string) ([][]string, string, error))
```

Each handler keeps its existing filter/scoping code and supplies a closure that calls
the same `API` method as its JSON path and maps rows to `[]string`. This keeps tenant
scoping and argument validation single-sourced.

## 5. Error handling

- Invalid `format`, `prefix_len` or existing filter arguments → `400` with the JSON error
  body, even for CSV requests.
- Source `InvalidArgument` → `400`; every other source error → `503`, unchanged from
  slices one and two.
- Mid-stream CSV failures are logged with the request id and abort the response; no
  partial-file retry or resume is offered.

## 6. Configuration and deployment

No new services, ports, environment variables or dependencies. The reports image,
compose block and smoke probe are unchanged except for the added CSV assertion.

## 7. Testing

- Store: prefix grouping over seeded CDRs returns digit keys, including a destination
  shorter than `prefix_len`; other groupings are unaffected.
- gRPC: `prefix_len=0` uses 3; `prefix_len=16` (and negative) is `InvalidArgument`;
  prefix keys are forwarded from the store.
- HTTP: `format=csv` on all five endpoints returns `text/csv`, the documented filename,
  a snake_case header and raw values; `summary` includes the `TOTAL` row; `format=json`
  and absent `format` are unchanged; `format=bogus` is `400`; a fake `API` returning
  many pages proves the 10,000-row stop; auth/tenant scoping applies identically to CSV.
- Gate: `make test` exit 0 with all 12 modules `ok`; `make smoke` PASS with the reports
  CSV assertion; live `curl` checks on a seeded account.

## 8. Risks and open questions

- **Cap truncation is silent.** The CSV has no cursor, so a caller cannot tell a
  complete export from a truncated one except by counting rows. Accepted for now;
  async export jobs are the follow-up when parity requires full dumps.
- **Prefix length is a reporting choice, not a rating one.** Two exports with different
  `prefix_len` are not additive across boundaries; this is documented behaviour.
- **No country names.** Turning prefixes into country labels needs a maintained dataset
  and is a separate decision.
