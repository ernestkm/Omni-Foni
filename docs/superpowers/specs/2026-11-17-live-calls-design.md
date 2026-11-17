# Live Call Monitoring Design

Date: 2026-11-17
Status: approved for planning

## 1. Context

P5 parity still misses the two live items from the reports list: "advance live call"
and "live switch monitoring". Switching already owns every in-flight call in
`Registry.calls`, but nothing exposes that state. This slice adds a read-only
snapshot: Switching serves live calls over gRPC, Reports re-exposes them behind
its tenant scoping, and the HTTP facade serves `/reports/live-calls` (JSON and
CSV).

The registry deliberately keeps finished calls for `CallEvents` replay
(`runCall`'s ponytail note), so "live" means `!done`, not map membership.

## 2. Scope

In scope:

- `switching.v1.Switching.ListLiveCalls` and the registry snapshot behind it.
- `reports.v1.Reports.ListLiveCalls`, tenant/account scoped like the other
  reports.
- `GET /reports/live-calls` with the existing auth/scoping rules and
  `format=csv`.
- Smoke coverage: a real call is started, listed, and stopped.

Out of scope:

- WebSocket/streaming push, polling intervals, or dashboards.
- Live balance per account (no Charging fan-out per call).
- Trunk-level aggregation, ASR/ACD, or QoS metrics.
- Call control beyond the existing `StartCall`/`StopCall`.
- Retention/eviction of ended calls (the ponytail note stays).

## 3. Switching contract

`proto/switching/v1/switching.proto`:

```proto
rpc ListLiveCalls(ListLiveCallsRequest) returns (ListLiveCallsResponse);

message ListLiveCallsRequest {
  repeated string account_ids = 1; // empty = all live calls (internal/trusted)
}

message LiveCall {
  string call_id = 1;
  string session_id = 2;
  string account_id = 3;
  string destination = 4;
  string cli = 5;
  string trunk_id = 6;
  int64 started_at_ms = 7;
  int64 elapsed_seconds = 8;
  int64 max_seconds = 9;
  int64 charged_seconds = 10;
}

message ListLiveCallsResponse {
  repeated LiveCall calls = 1;
}
```

Registry:

- `call` gains `maxSeconds int64` (set from `Authorize` before the goroutine
  starts, immutable afterwards) and `chargedSeconds int64` (written under
  `call.mu` on each tick in `runCall`, before the `Charge` RPC so the snapshot
  never leads the OCS by a whole tick).
- `Registry.LiveCalls(accountIDs []string) []LiveCallSnapshot` takes `r.mu`,
  collects non-nil calls whose `done` is false and whose account matches
  (empty filter = all), releases `r.mu`, then reads each `call.mu` once for
  `done` and `chargedSeconds`. Lock order is always `r.mu` then `call.mu`;
  `runCall` and `emit` never take `r.mu`.
- Sorting: `startedAt ASC, callID ASC`. Sorting happens in the registry so the
  gRPC adapter is a pure conversion.
- `elapsed_seconds` is computed by the adapter at response time as
  `int64(time.Since(startedAt).Seconds())` (whole seconds, never negative).
- `LiveCallSnapshot` is a plain Go struct; no protobuf types in the registry.

## 4. Reports chain

`proto/reports/v1/reports.proto` gains the same shape under its own package:

```proto
rpc ListLiveCalls(LiveCallsRequest) returns (LiveCallsResponse);

message LiveCallsRequest { repeated string account_ids = 1; }

message LiveCall {
  string call_id = 1;
  string session_id = 2;
  string account_id = 3;
  string destination = 4;
  string cli = 5;
  string trunk_id = 6;
  int64 started_at_ms = 7;
  int64 elapsed_seconds = 8;
  int64 max_seconds = 9;
  int64 charged_seconds = 10;
}

message LiveCallsResponse { repeated LiveCall calls = 1; }
```

Reports server:

- New narrow source interface next to `PaymentSource`/`ChargeSource`:

```go
type LiveCallSource interface {
	ListLiveCalls(ctx context.Context, req *switchingpb.ListLiveCallsRequest,
		opts ...grpc.CallOption) (*switchingpb.ListLiveCallsResponse, error)
}
```

- `reportsServer` gains `live LiveCallSource`; `ListLiveCalls` maps the source
  response one-to-one and returns `Unavailable` on source errors.
- Env `REPORTS_SWITCHING_ADDR` (default `switching:50052`). `main.go` dials it
  with `observability.ClientOptions()`, same as the other sources.
- Wiring: `services/reports/go.mod` gains `require`/`replace` for
  `omnifoni/switching => ../switching`. The Dockerfile adds `COPY switching/ ./switching/`
  plus the siblings switching replaces (`routing/`, `sbc/`; catalogue,
  mediation, charging and observability are already copied) — Go resolves the
  replaced module graph, so a missing sibling COPY fails the build (same
  failure mode recorded for switching's own Dockerfile). Compose adds
  `REPORTS_SWITCHING_ADDR` and a `switching` `depends_on` (`service_started`).

## 5. HTTP facade

`GET /reports/live-calls`:

- Bearer token required; identity `GET /me` resolves role/tenant.
- Role `user` → 403 (identity still has no user↔account link).
- Reseller: forced to own tenant. Admin: `tenant_id` required, else 400.
- Accounts come from identity `GET /accounts?tenant_id=`; the optional
  repeated `account_id` query intersects like the other reports.
- Empty account scope → `200 {"calls":[]}` (or header-only CSV) without
  calling switching.
- `format=csv` uses the existing `csvRequested`/`streamCSV` helpers with
  header `call_id,session_id,account_id,destination,cli,trunk_id,started_at_ms,elapsed_seconds,max_seconds,charged_seconds`,
  a single page (`next=""`), and no 10,000-row cap concern (bounded by live
  calls).
- Errors: source `InvalidArgument` → 400, everything else → 503, matching
  `grpcStatus`.
- JSON shape: `{"calls":[{...same snake_case fields...}]}`; no `next_cursor`.

## 6. Testing

Switching:

- Registry test with fakes: start two calls (different accounts/trunks), assert
  snapshot fields (`max_seconds` from the fake authorize, `charged_seconds`
  advanced by a tick), account filter, ordering by `started_at`, and that a
  stopped/ended call is excluded.
- gRPC adapter test: `ListLiveCalls` converts and computes elapsed ≥ 0.

Reports:

- gRPC test with a fake `LiveCallSource`: mapping, error → `Unavailable`.
- HTTP tests with the existing `facade`/`fakeIdentity`: reseller scoping,
  admin-without-tenant 400, role `user` 403, empty tenant empty list,
  CSV header + row, bogus format 400, source outage 503.

Smoke:

- `ops/smoke.sh` seeds (idempotent, `ON CONFLICT`) one row per source:
  `catalogue.rates(prefix, rate_per_sec)`,
  `charging.accounts(account_id, kind='prepaid', balance, credit_limit)`,
  `routing.trunks(trunk_id, enabled)` + `routing.lcr_routes(prefix, trunk_id, cost_per_sec)`,
  and `identity.accounts(account_id, tenant_id)` under the `smoke` tenant.
  The destination and the rate/LCR prefixes must match (e.g. `2782`).
- New `services/switching/cmd/smokelive` starts a call via switching gRPC,
  calls `reports.v1.Reports/ListLiveCalls` for that account, asserts the call
  id is present, then stops the call. Smoke prints `ok: live calls`.
- `make test` (12 modules, exit 0) and `make smoke` (PASS) are the gate.

## 7. Decisions and risks

- `max_seconds`/`charged_seconds` are stored on the call rather than derived:
  they are the OCS view of the call, which is what "advance live call" needs.
- Charged seconds lead the OCS by at most the in-flight `Charge` RPC; the
  counter is incremented before the RPC so a snapshot never shows less than
  the OCS has already granted.
- Finished calls are still retained in memory (unchanged); `ListLiveCalls`
  filters them, so the report stays correct without touching replay.
- No pagination: the response is bounded by concurrent calls; adding a cursor
  later is additive.
- Smoke now needs a real call; it seeds only dev databases and cleans up by
  stopping the call. Re-runs are safe via `ON CONFLICT`.
