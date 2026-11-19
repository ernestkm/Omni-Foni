# Final review fixes (wave 2) — closure report

Starting state: branch `p4-sip-edge`, clean tree at `ccea11b89e96ec3f7646efcd823c3713c5bd1838`.
Delivered as one commit with message `fix(sipedge): retry failed stops and align edge defaults`. This report is included in that commit, so the commit cannot literally contain its own hash; the resulting hash is reported in the final response.

## Finding 1 (Important): `/bye` discarded `facade.stop` result; reaper could not retry a failed stop

Closure: `/bye` now writes the cancel decision, deletes the stale allow decision, and calls `facade.stop`. On error it records `stop_error` on the span and keeps the entry marked `stopFailed`; on success (including `stopped=false`) it removes the entry. The reaper retries `stopFailed` entries at the top of every tick before the `live` lookup, logs a warning and keeps the entry while the error persists, removes it on success, and skips those entries in the dialog-end path.

Files: `services/sipedge/server.go`, `services/sipedge/reap.go`, `services/sipedge/server_test.go`, `services/sipedge/reap_test.go`.

TDD evidence:
- RED `TestByeStopFailureLeavesPendingEntry`: `server_test.go:229: pending stop entry was dropped`.
- Guard `TestByeStopAlreadyAbsentRemovesEntry` covers nil-error `stopped=false`; no retry.
- GREEN: `go test -race -count=1 ./...` → PASS after the `server.go` change.
- RED `TestReaperRetriesPendingStopUntilSuccess`: `reap_test.go:52: want one stop retry, got 0`.
- RED `TestReaperRetriesPendingStopWhenLiveFails`: `reap_test.go:80: want pending stop retried despite live failure, got 0`.
- GREEN: `go test -race -count=1 ./...` → PASS after the `reap.go` change.
- Fixture note: the first version of `stopCountingFacade` answered the failing stop with HTTP 200 `{"stopped":false}`, which is a successful stop, not an error. It was corrected to HTTP 503 before the RED observations above.

## Finding 2 (Important): unset `SIPEDGE_REAP_INTERVAL` disabled the reaper instead of defaulting to 1s

Closure: added `reapInterval(raw)`; `""` → `1s`, enabled; `"0"` → disabled; any other value parsed with `time.ParseDuration`; invalid values remain a log-and-exit error in `main`. `main` now reads the env through `env("SIPEDGE_REAP_INTERVAL", "1s")`.

Files: `services/sipedge/main.go`, `services/sipedge/main_test.go` (new).

TDD evidence:
- RED `TestReapInterval`: `./main_test.go:23:20: undefined: reapInterval` (compile failure).
- GREEN: `go test -race -count=1 ./...` → PASS, all four subcases (`unset`, `explicit disable`, `explicit duration`, `invalid`).

## Finding 3 (Minor): positive decision cache checked before cancel, and never invalidated on BYE

Closure: `invite` checks `cancelCache` first; `bye` calls the new `decisionCache.delete` for the Call-ID, so a stale allow cannot survive the cancel-cache TTL.

Files: `services/sipedge/server.go`, `services/sipedge/server_test.go`.

TDD evidence:
- RED `TestInviteAfterByeDoesNotReplayStaleAllow`: `server_test.go:236: want 403 canceled, got 200 "300|sip:gw:5060"`.
- GREEN: `go test -race -count=1 ./...` → PASS.

## Finding 4 (Minor): Kamailio published on UDP only

Closure: added `"5060:5060/tcp"` beside the UDP mapping in `docker-compose.yml`. `docker compose config -q` passes; Kamailio startup log shows both `udp: 0.0.0.0:5060` and `tcp: 0.0.0.0:5060`.

Files: `docker-compose.yml`.

## Finding 5 (Minor): JSON-RPC params built by string concatenation

Closure: `kamRPC.endDialog` parses `hEntry`/`hID` with `strconv.ParseInt` (explicit `h_entry`/`h_id` errors) and builds the request with `json.Marshal`, so malformed coordinates cannot inject JSON-RPC fields. The successful `dlg.end_dlg` path and narrow "Dialog not found" 404 handling are unchanged.

Files: `services/sipedge/client.go`, `services/sipedge/client_test.go`.

TDD evidence:
- RED `TestEndDialogRejectsMalformedCoordinates`: `client_test.go:67: malformed coordinates must not reach kamailio` and `client_test.go:77: want h_entry error, got EOF`.
- Guard `TestEndDialogSendsNumericParams` asserts numeric params and the `dlg.end_dlg` method.
- GREEN: `go test -race -count=1 ./...` → PASS.

## Verification unblocker (pre-existing defect at HEAD)

The first `make up` after the compose change recreated Kamailio, which exited 255. Logs showed `bad PVE format: "^[a-z0-9_]+$"` and `error -1 while trying to fix configuration` at `ops/kamailio/kamailio.cfg:109`. Commit `ccea11b` had added the trailing `$` anchor; Kamailio parses `$` as a pseudo-variable marker. The prior commit used the unanchored regex. A throwaway container check with the anchor removed printed `config file ok`, confirming the hypothesis. Applied that one-line revert in `ops/kamailio/kamailio.cfg`; deny reasons and HTTP status mapping are unchanged.

## Verification

| Command | Outcome |
| --- | --- |
| `gofmt -l services/sipedge services/switching` | clean after formatting `server_test.go` |
| `cd services/sipedge && go test -race -count=1 ./...` | PASS |
| `cd services/sipedge && go vet ./...` | PASS |
| `cd services/switching && go vet ./...` | PASS |
| `bash -n ops/smoke.sh ops/sip-e2e.sh` | PASS |
| `docker compose config -q` | PASS |
| `docker compose build sipedge switching` | both images built |
| `make up && make test` | first run blocked by the pre-existing Kamailio config defect above; after the one-line revert, `make up` all services healthy and `make test` PASS |
| `docker exec omni-fon-kamailio-1 kamailio -c -f /etc/kamailio/kamailio.cfg \| grep -F "config file ok"` | `config file ok, exiting...`, exit 0 |
| `make smoke` | `smoke: PASS` |
| `bash ops/sip-e2e.sh` | `ok: sip e2e` |
| `git diff --check HEAD` | PASS |

## Concerns

(none)
