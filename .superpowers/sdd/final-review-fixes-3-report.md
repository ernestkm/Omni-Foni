# Final review fixes (wave 3) — closure report

Starting state: branch `p4-sip-edge`, clean tree at `3c2b181` (`fix(sipedge): harden retry state and reap interval parsing`). Fresh whole-branch review `d9b4ecd..3c2b181` returned **With fixes**: 0 Critical, 1 Important, 1 Minor. This report is included in the closing commit; the resulting hash is reported in the final response.

## Finding 1 (Important): stale stop-retry snapshot could issue an uncoordinated stop

Review detail: `services/sipedge/reap.go:25-35` snapshotted pending-stop entries with `store.all()` and called `facade.stop` without revalidating that the store entry was still the same pending-stop call. The final `takePendingStop` compare-and-remove was safe, but the external `StopCall` side effect was not claimed or serialized.

Closure: added `entry.stopRetrying` and two atomic store helpers in `services/sipedge/server.go`:

- `claimPendingStop(id, switchingID)` rechecks `stopFailed`, `stopRetrying`, and `switchingID` under the store lock before setting the claim.
- `releasePendingStop(id, switchingID)` clears the claim only for the same identity after a failed retry, so the next tick can retry.
- `markStopFailed` clears any stale claim when marking a pending stop.
- `reaper.retryStops` now claims before calling `stop`, releases on error, and keeps `takePendingStop` as the final compare-and-remove.

Files: `services/sipedge/server.go`, `services/sipedge/reap.go`, `services/sipedge/reap_test.go`.

TDD evidence:

- RED `TestClaimPendingStopIsSingleAndIdentityBound`: compile failure `st.claimPendingStop undefined (type *store has no field or method claimPendingStop)` at `reap_test.go:50`, `:53`, `:57`, `:60`.
- RED also covered by `TestRetryStopsDoesNotRemoveReplacementEntry`, which failed at the same compile stage.
- GREEN: `cd services/sipedge && go test -race -count=1 ./...` passed after the production change, including the existing `TestReaperRetriesPendingStopUntilSuccess` (two ticks: one failed stop, one successful stop) and `TestReaperRetriesPendingStopWhenLiveFails`.
- `go vet ./...` and `gofmt -l .` clean in `services/sipedge`.

## Finding 2 (Minor): Kamailio deny-reason validation was prefix-only

Review detail: `ops/kamailio/kamailio.cfg:108-112` used `search_str("$var(reason)", "^[a-z0-9_]+")`, which accepts CR/LF after a valid token prefix. sipedge already sanitizes deny reasons (`services/sipedge/policy.go:52-67`), but the Kamailio backstop was brittle.

Closure: replaced the prefix check with a full-string negative check that avoids the `$` anchor (Kamailio 5.8 parses a trailing `$` in this context as a pseudo-variable marker and refuses startup):

```
if ($var(reason) == "" || search_str("$var(reason)", "[^a-z0-9_]")) {
    $var(reason) = "policy_denied";
}
```

This rejects empty reasons and any character outside `[a-z0-9_]`, including CR/LF, before `append_to_reply` and `t_reply`. Files: `ops/kamailio/kamailio.cfg`.

Verification: `docker exec omni-fon-kamailio-1 kamailio -c -f /etc/kamailio/kamailio.cfg` printed `config file ok, exiting...`; `make smoke` still passed `ok: sip deny`.

## Verification

| Command | Outcome |
| --- | --- |
| `gofmt -l services/sipedge services/switching` | clean |
| `cd services/sipedge && go test -race -count=1 ./...` | PASS |
| `cd services/sipedge && go vet ./...` | PASS |
| `cd services/switching && go vet ./...` | PASS |
| `bash -n ops/smoke.sh ops/sip-e2e.sh` | PASS |
| `docker compose config -q` | PASS |
| `docker compose build sipedge switching` | both images built |
| `make up` | all services healthy |
| `make test` | PASS (14 modules, no FAIL) |
| `docker exec omni-fon-kamailio-1 kamailio -c -f /etc/kamailio/kamailio.cfg` | `config file ok, exiting...` |
| `make smoke` | `smoke: PASS` incl. `ok: sip`, `ok: sip deny`, `ok: sip exhaust`, `ok: traces for sipedge` |
| `bash ops/sip-e2e.sh` | `ok: sip e2e answer + cdr`, `ok: sip e2e deny`, `ok: sip exhaust`, `ok: sip e2e exhaust`, `ok: sip e2e` |
| `git diff --check d9b4ecd..HEAD` | PASS |

## Concerns

(none)
