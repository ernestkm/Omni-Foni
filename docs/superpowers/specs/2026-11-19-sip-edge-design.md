# SIP edge (Kamailio) design — real SIP on the existing money path

Version 1.0, 2026-11-19. Status: approved for planning.

## Goal

Give Omni-Foní a public SIP listener so real SIP calls flow through the existing
policy and charging chain (routing → LCR → SBC → fraud → OCS) and produce billed
CDRs. This closes the master plan's P4 "Kamailio edge" gap for signaling; media
handling (rtpengine), FreeSWITCH, normalization, and topology hiding stay out of
scope.

## Scope

In scope:

- Kamailio as the only public SIP listener (:5060 UDP+TCP), source-IP
  authenticated trunks.
- INVITE/BYE/CANCEL lifecycle wired to switching via a Go shim.
- Mid-call prepaid-exhaustion teardown into the live SIP dialog.
- HTTP/JSON facade on switching for the shim.
- Trunk SIP targets (`target_uri`) in routing, surfaced through LCR.

Out of scope (recorded, not built): registrations are an in-memory stub only
(REGISTERs do not fail, nothing consumes them); SDP is passed through untouched;
no rtpengine, no FreeSWITCH, no codec handling, no RPID/PAI rewriting, no
topology hiding, no digest auth.

## Architecture

Three moving pieces, no new database:

1. **Kamailio** (`kamailio` compose service, official 5.x image). Thin config:
   - `dialog` module tracks every call established through the proxy.
   - Trunk allow-list: source IPs of configured trunks; INVITEs from unknown
     IPs are answered `403` locally (matches the SBC's IP-block posture).
   - `http_async_client` calls the shim on INVITE and BYE/CANCEL.
   - `jsonrpcs` HTTP socket (default `:8080/rpc`) exposes `dlg_end_dlg` for
     the reaper and accepts `dlg_set_lifetime`-style capping via config.
   - On allow: relays the INVITE to the trunk `target_uri` returned by the
     shim. On deny: replies `403`/`503` per mapping below.
2. **Shim** (`services/sipedge`, `:50071`, env `SIPEDGE_SHIM_ADDR`). One Go
   service, three duties:
   - `/invite`: translates the Kamailio POST (call_id, source ip, from, to,
     r-uri) into switching's facade `start`; returns decision + `target_uri`.
     Idempotent per call_id: a retransmitted INVITE returns the first decision.
   - `/bye`: translates to facade `stop`; duplicate stops are no-ops (200).
   - Reaper loop (default 1 s, env `SIPEDGE_REAP_INTERVAL`): GET facade
     `live`, diff against tracked dialogs, `dlg_end_dlg` for vanished calls
     via Kamailio's jsonrpcs.
3. **Switching HTTP facade** (second port on `services/switching`, default
   `:50070`, env `SWITCHING_HTTP_ADDR`; empty disables the facade, matching
   the reports scheduler switch pattern). Endpoints:
   - `POST /start` → same registry path as gRPC `StartCall`, response JSON
     includes decision, reason, trunk_id, `target_uri`.
   - `POST /stop` → same as gRPC `StopCall`.
   - `GET /live` → JSON mirror of `ListLiveCalls` snapshot used by the reaper.

The registry remains the single source of truth for in-flight calls; the shim
keeps only ephemeral dialog state (call_id → dialog meta) in memory.

## Call flow

- INVITE arrives → Kamailio checks source IP against the trunk allow-list →
  unknown: `403` locally; known: track dialog, POST `/invite`.
- Shim → facade `start` → existing chain (routing, LCR, SBC incl. fraud, OCS
  authorize). The LCR join now returns the trunk's `target_uri`; switching
  carries it on `StartCallResponse` (new proto field).
- Deny → shim maps reason to SIP code (below) → Kamailio replies; nothing was
  reserved (existing guarantees hold).
- Allow → Kamailio relays INVITE to `target_uri`, SDP untouched, no media
  component. Kamailio caps dialog lifetime at authorized `max_seconds` plus a
  small margin (backstop, see Failure handling).
- BYE / CANCEL-before-200 → POST `/bye` → facade `stop` → charging stops, CDR
  published. Partial-second semantics are the existing StopCall semantics.
- In-dialog requests (re-INVITE, UPDATE) pass through untouched; no policy
  re-checks; only the dialog's BYE ends the call.

## Deny mapping

| Reason | SIP |
|---|---|
| `no_route`, `barring_destination`, `barring_cli`, `fraud_destination`, `fraud_cli`, `fraud_velocity_account`, `fraud_velocity_prefix`, `trunk_limit_exceeded`, `ip_blocked` | `403` |
| `ocs_unavailable`, `routing_unavailable`, `sbc_unavailable`, `fraud_unavailable`, `no_rate`, `internal_error` | `503` |

All denials carry `Reason: SIP;cause=<code>;text="<reason>"` and the reason in
the response text. An unrecognized reason maps to `503` (fail-closed posture:
an unrecognized reason is a control-plane problem, not a policy verdict).

## Schema and proto changes

- `routing.trunks`: `ALTER TABLE trunks ADD COLUMN target_uri text NOT NULL
  DEFAULT ''` — added to routing's migrate SQL. Empty target_uri means the
  trunk cannot be selected as an LCR relay target (excluded from `ResolveLcr`
  results, regardless of cost).
- `proto/routing/v1/routing.proto`: `ResolveLcrResponse` gains `target_uri`.
- `proto/switching/v1/switching.proto`: `StartCallResponse` gains `target_uri`.
- Regenerate with `make protoc`; no manual edits to generated code.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `SIPEDGE_SHIM_ADDR` | `:50071` | shim HTTP listen address |
| `SIPEDGE_SWITCHING_HTTP` | `switching:50070` | switching facade base URL |
| `SIPEDGE_KAMAILIO_RPC` | `http://localhost:8080/rpc` | Kamailio jsonrpcs endpoint |
| `SIPEDGE_REAP_INTERVAL` | `1s` | reaper tick; `0` disables reaping |
| `SWITCHING_HTTP_ADDR` | empty (disabled) | switching facade listen address |
| `SIPEDGE_HTTP_TIMEOUT` | `5s` | shim→facade and shim→Kamailio HTTP timeout |

The dialog lifetime cap is `max_seconds + 5s` (fixed margin, set in
Kamailio.cfg from the `start` response; no separate env).

The trunk allow-list lives in Kamailio.cfg (compose-configured IP list); no
DB-backed peer table this slice.

## Compose topology

Two services, `kamailio` and `sipedge` (cleaner health checks and restart
semantics than one container supervising two processes):

- `kamailio`: official image, mounts `ops/kamailio/kamailio.cfg`, publishes
  `5060/udp` + `5060/tcp`, exposes `8080` (jsonrpcs) internally only.
- `sipedge`: Go image built from `services/sipedge`, internal only.
- Both depend on `switching`; switching's image gains nothing except the new
  facade code.

## Failure handling

- **Facade down**: shim `/invite` errors → Kamailio `503` (fail-closed).
- **Shim down**: Kamailio http_async_client timeout → `503`. Signaling-only;
  no media impact.
- **Kamailio RPC unreachable** (reaper cannot end dialogs): reaper logs and
  retries next tick; the dialog-lifetime cap (max_seconds + margin) bounds
  un-billed talk time, so a dead reaper cannot leak talk time past
  authorization.
- **Crash between start and BYE**: call remains in the switching registry;
  when Kamailio's dialog times out (lifetime cap) the reaper diff reaps it and
  its BYE flows through `/bye` → StopCall once (idempotent).
- **Kamailio crash mid-call**: dialogs lost; switching's registry tick logic
  still terminates exhausted calls; non-exhausted orphans are bounded by the
  registry's existing max-seconds behavior.
- **Replay safety**: `/bye` followed by a late reap-triggered BYE is a no-op;
  INVITE retransmits return the first decision. The tiny crash window matches
  the existing at-least-once posture; billing dedups by call_id.

## Testing

- **Go unit tests**: deny-reason→SIP-code mapping table (including unknown
  reason → 503); `/invite` and `/bye` idempotency; facade handlers (JSON
  round-trip, disabled facade → 503, error passthrough); routing target_uri
  join and empty-target LCR exclusion; reaper diff logic (table-driven, no
  live Kamailio).
- **Integration (compose)**: seed one trunk with `target_uri` at a scripted
  SIP responder (`sipp` UAS in a test container). Assert:
  1. Golden call: INVITE → 200 (SDP passthrough) → BYE → exactly one CDR on
     `cdrs.raw`/`cdrs.normalized` with correct seconds, and the OCS ledger
     billed to the cent.
  2. Denials: barred destination → `403` with reason; unknown source IP →
     local `403`; facade disabled → `503`.
  3. Mid-call exhaustion: balance drained under a live call → dialog torn
     down (BYE observed) within ~2 s of the zero-balance tick.
- **Smoke**: `ops/smoke.sh` adds an `ok: sip` step (one sipp call through the
  edge, asserting 200 + live-call visibility + CDR). Context-DB count stays 10
  (no new DB).

## Acceptance criteria

1. Golden sipp call bills to the cent on the OCS ledger and produces one CDR.
2. Every documented deny reason maps to its SIP code, with `Reason` header.
3. Mid-call prepaid exhaustion tears the dialog down within 2 s.
4. Fail-closed on every control-plane outage case (facade, shim, Kamailio RPC
   degradation is bounded by the lifetime cap).
5. No new database; Kamailio.cfg stays under 200 lines; no manual edits to
   generated protobuf code.
