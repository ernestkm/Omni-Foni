Task 1: complete (commits 1543450..d8e22cf, review clean; minor: report/code mkdir mismatch note, CI does not run make protoc)
Task 2: complete (commits d8e22cf..d47406d, review clean)
Task 3: complete (commits d47406d..348c903, review clean; minor flake vectors from brief test code; host prereqs: /etc/hosts kafka, mediation db)
Task 4: complete (commits 348c903..9de2845 incl. controller comment-fix; review clean)
Task 5: complete (commits 9de2845..a76648c, review clean; plan shape = PublishCDR method + one-arg NewRegistryWithCDRs(pub); controller prompt text was wrong)
Task 6: complete (commits a76648c..0347429; subagent aborted mid-task, controller finished+verified; report written)
Task 6: complete (commits a76648c..cfd7500 incl. controller poll fix; review clean)
Task 7: complete (commits cfd7500..90ade79; Dockerfile mediation COPY fix + AGENTS.md; review clean)
All 7 tasks complete; HEAD 90ade79; starting whole-branch review 1543450..90ade79
Whole-branch review: brought to merge-ready. Fixed: (1) kgo.AllowAutoTopicCreation on both clients + RecordDeliveryTimeout(10s); (2) Run logs fetch errors; (3) stray comment; (4) charging compose/Dockerfile sibling-import bug (image was unbuildable + stale). Proven with clean fresh-stack e2e (wipe+up+build, no manual topics): cdrs.raw 1 -> cdrs.normalized 1 -> ledger published=t. make test all 4 green.
Task 1: complete (commits a1e40dd..6a44c38, review clean) — trunk_id field on CallCdr
Task 2: complete (commits 6a44c38..1c8284a, review clean) — switching carries trunk on CDR
Task 3: complete (commits 1c8284a..2eb21fc, review clean) — mediation pass-through test
Task 4: complete (commits 2eb21fc..db39591, review clean) — AGENTS.md + full gate
Whole-slice review a1e40dd..db39591: ready to merge, no C/I findings. Minor: (1) mediation trunk assertion sits before call-id check (identical semantics); (2) empty-trunk normalization assert is implicit — could pin with explicit c-2 guard.
Task 1: complete (commits 3175944..b230274, review clean) — billing proto GetPeriodLedger/ListAccountsForPeriod

Task 2 fix: Important test-isolation flake fixed (TestListAccountsForPeriod ignores acc-inv; -count=8 + full suite green). Commit after 08681ff.
Task 2: complete (commits b230274..d8f68f0 incl. isolation fix, re-review approved) — ingest ledger + handlers; implementer cancelled, controller finished; validPeriod month 01-12 fix; env: billing container UNKNOWN_TOPIC_ID loop cleared by restart
Task 3: complete (commit 3a76996, review approved; implementer cancelled, controller finished compose+rebuild; minor: invalid-period returns codes.Unknown not InvalidArgument)
Task 4: complete (commit 8f9ca3f, review approved; implementer rate-limited, controller verified+committed; minors: double ListAccounts round-trip, unused InvoiceTotal/fakePublisher, PAYMENTS_ADDR env collision -> Task 6 compose must be explicit)
Task 5: complete (commits 14e189c + fix 4b50b90, review approved; controller-implemented; fixed AddDate prev-month rollover + e2e hardening)
Task 6: complete (commit b076f12 + fix d0eaf98, review approved; controller-implemented; compose build context must be ./services not ./services/reconciliation; full gate: make test all ok, make smoke PASS)
MERGED: p2-reconciliation fast-forwarded to master at 1d30eb2; make test + make smoke green on master; branch deleted.
Task 1: complete (commit 660762c, controller-verified after subagent cancel; spec aligned)
Task 2: complete (commit 1ae8b7d, controller-implemented, 11/11 tests)
Task 3: complete (commit e2434ee; jaeger 1.62.0 tag fix; make test+smoke PASS)
Task 4: complete (commit 29d3f82; spans verified in Jaeger: catalogue/charging/routing)
Task 5: complete (commit 8ba745f; switching span verified live via Jaeger)
Task 6: complete (commit c2de259; cross-kafka trace verified switching->mediation->billing)
Task 7: complete (commit 29c11c9; all 10 services visible in Jaeger, smoke PASS)
Task 8: complete (smoke asserts reconciliation/billing/payments traces; AGENTS.md observability section; request-scoped logs now use *Context so trace_id/span_id/request_id appear — verified live on charging "authorized")
Review: full gate green (make test no FAIL; make smoke PASS incl. trace assertions); e2e trace 19 spans switching->routing->sbc->charging->catalogue->mediation->billing
Merge: p0-observability fast-forwarded to master
Task 1: complete (commit ad38352, review approved; minor: grpc/protobuf marked indirect until later imports)
Task 2: complete (commit f554db6, review approved; controller finished after implementer cancel; minors plan-mandated: timing-based dedup test, TestMain no Close, single-element AccountIDs, Decimal write not read back, GroupAccount/Trunk untested)
Task 3: complete (commit a4b1b74, review approved; ⚠️ DisableAutoCommit lives in Task 6 main.go = resolved; minors: non-transient Handle error stalls partition (plan-mandated), Run doc comment attached to package, "mirrors billing" comment drift, no poison-pill decode test)
Task 4: complete (commit be1b163, review approved; ADJUDICATION: spec line 145 governs — date-only `to` includes that whole UTC day, so From==To same date is a valid 1-day range; plan's Task-4 test assertion was a plan bug, replaced with a genuinely reversed range. minors: store errors swallowed without log/span status, negative limit → default not floor, clamp untested, date-only semantics untested, loose cursor validation)
Dispatch unavailable from Task 5 on: subagent model deepseek-v4.1-flash EOL (410). Controller implementing inline; reviews via subagent also unavailable — controller self-review + final live gate.
Task 5: complete (commit eb07e32, controller-implemented + self-reviewed; fixed plan bug: admin without tenant_id fell back to own tenant (502) instead of 400 per spec §4.5)
Task 6: complete (commit dca363d, controller-implemented; fix: embed pb.UnimplementedReportsServer (plan omitted it, RegisterReportsServer failed); make smoke PASS incl. reports traces)
Task 7: complete (commit af7b9fc; bench p95 26 ms at 200k; live call 3s -> amount 2; live HTTP 401/403/400/200; make test exit 0 (12 ok); make smoke PASS)
Self-review fix: empty tenant scope leaked all accounts through the HTTP facade (gRPC treats empty AccountIds as no filter). Early empty 200 in both handlers + regression test; live-verified. Commit be69c7c; make test exit 0 (12 ok).
P5 slice two (money reports): branch p5-money-reports; plan committed a6d9aa3.
Task 1: complete (commit 69e337f; deviation: plan test event ids evt-1/2/3 collided with webhook tests' fixed ids, renamed to list-evt-*; payments module green)
Task 2: complete (commit acc395e; deviations: plan test reused $1 inside a CASE (Postgres "inconsistent types deduced for parameter $1") -> insert explicit account per ledger row; default zero-UUID cursor when absent to avoid an empty ::uuid cast)
Task 3: complete (commit 5354d1d; go mod tidy promoted otel to direct, requires re-added after grpc_money.go)
Task 4: complete (commit 882265d; fixed plan gap: API interface needed the three new methods)
P5 slice four (live calls): branch p5-live-calls; plan 3336010.
Task 1: complete (commit 3984f06, review approved; brief test-order bug fixed to c1,c3,c2; minors: _ = ctx, tautological elapsed assertion, lock-order comment wording)
Task 2: complete (commit df35a8f, review approved; minors: no OTel span on ListLiveCalls, InvalidArgument pass-through untested, service doc comment stale, plan step-4 expected-failure wording off)
Task 3: complete (commit a6a557f, review approved; minors: duplicated empty-scope response mirrors siblings, CSV snapshot silently capped at 10k)
Task 4: complete (commits 5b72407, 6955e44, review approved; minors: report says six seeds vs five, double StopCall, scan-after-found, lazy-dial error path)
Final review: clean, 0 critical/important; fixed span + proto doc (commit 4e9b3d9); smoke PASS + make test 12 ok; merge: p5-live-calls fast-forwarded to master (4e9b3d9), branch deleted.

## Slice five: scheduled reports (plan docs/superpowers/plans/2026-11-17-scheduled-reports.md)
Task 1: complete (commits a7b2a90..58cc7c1, review clean) — proto + sched_event_test
  minor (plan-inherited): round-trip omits period_from/to assertion; files lack trailing newline
Task 2: complete (commits 58cc7c1..8e16ee7, review clean after one fix round) — sched store + migrate + init.sql + tests
  fix round: 8e16ee7 tightened cascade/csv-body/row_count coverage, TestMain close, ListArtifacts doc
  minor deferred: global count in cascade test (non-parallel-safe only); TestSchedStoreCRUD discards GetSchedule err (plan-verbatim); gofmt on Task-1 sched_event_test.go (pre-existing)
Task 3: complete (commits 8e16ee7..e325c8b, review clean after one fix round) — scheduler + render + row extraction
  fix round: e325c8b TOTAL survives row cap, shared summaryTotalRow, period/weekly/monthly/render coverage
  minor deferred: nil-Totals corner emits header-only vs HTTP TOTAL,0,0,0; summary empty-result untested
Task 4: complete (commits e325c8b..268c46e, review clean after one fix round + cross-tenant fix) — sched HTTP routes + Scope/UserID
  fix round: 6ec18a8 fail-closed intersection, fresh PATCH row, replace semantics documented+tested, cross-tenant matrix
  cross-task fix: 268c46e capture tenant accounts at write time; empty scope renders header-only with no API calls (closes cross-tenant leak)
  minor deferred: maxScheduleAccounts cap bypassed by capture (same tenant); header-only uses strings.Join not csv.Writer; admin capture untested; store comment wording; gofmt on Task-1 sched_event_test.go
Task 5: complete (commit 268c46e..1f95314, review clean) — main wiring, compose, smoke, AGENTS
  minor deferred: smoke sed extraction not a real assertion; create failure diagnostic; no retry run->download; shutdown doesn't join in-flight tick; prune best-effort; AGENTS DSN wording
Final review a7b2a90..1f95314: controller-performed after 3 aborted reviewer dispatches; 0 critical/important, merge-ready with minors.
  minor deferred: duplicate slot returns zero Artifact with nil error (scheduler.go:153-155); smoke sed extraction not a strict assertion; shutdown doesn't join in-flight tick; maxScheduleAccounts cap bypassed by tenant capture; AGENTS DSN wording nuance
  hygiene commit 98ed996: go mod tidy promotes uuid direct; trailing newline in sched_event_test.go
Final gate: reports image rebuilt; make smoke PASS (incl. ok: scheduled report, 4 trace services); make test exit 0 (12 ok); go vet ./... clean.
MERGED: p5-scheduled-reports fast-forwarded to master at 98ed996; branch deleted; gate results above apply to the merged tree.

## P6 fraud MVP
Plan reviewed in place and corrected (scorer package boundary, longest-prefix weight, duplicate reservation handling, observability module dependency, variadic gRPC client adapter, smoke sequence, day-scoped velocity, reservation-day CDR completion, replay baseline path); final verification clean. Implementation plan committed with this ledger entry.
Task 1: complete (commits 54fb8ba..242f8b8, review clean; ⚠️ resolved by controller: focused vet/test green, `make protoc` idempotent with clean diff)
Task 2: complete (commits 242f8b8..6fa555c, review clean; ⚠️ resolved: fraud DB exists, vet/test green; minor deferred: test helper registers pool cleanup and Store.Close could double-close)
Task 3: complete (commits 6fa555c..68f41bd, review clean; minor deferred: DestinationPrefix byte-slicing assumes ASCII digits; clean-case nil reasons implicit in test; brief-internal nil/[]string{} mismatch resolved to nil)
Task 4: complete (commits 68f41bd..784db5b, review clean with follow-up fix 58315a0; fixed unique alerts.call_id and empty-destination double-count with TDD; verified unique index exists in dev DB; minors deferred: repeat check still runs block lookups first, riskPrefixWeight swallows DB errors, readSignals cfg unused, first-seen prefix scan ignores index, LIKE wildcards in block prefixes, coverage gaps for historical day/no-alert/disabled controls)
Task 5: complete (commits 58315a0..c1e6762, review clean with follow-up fixes b0b0035 and 2120294; invalid cursors map to InvalidArgument including non-UUID ids; minors deferred: out-of-range timestamp not bounded, empty-ID cursor case untested, invalid-cursor branch does not record span, go mod tidy dependency expansion, no dedicated publish-failure/duplicate-race/EndCall tests)
Task 6: complete (commits 2120294..1e5390a, review clean with follow-up fixes ac52669 and 03b0bb8; transactional dedup/ingest, poison drop, retry/commit-after-DB verified; amount_minor recorded as 0 because mediation.v1.CallCdr has no such field; minors deferred: commit failure not retried in loop, storeHandler bypasses Ingester validation, Kafka required for package tests, date OID cast implicit, test name/nanoseconds)
Task 7: complete (commits 03b0bb8..8cfbf7d, review clean with follow-up fix edb0b2e; runtime gRPC/Kafka/consumer/publisher wiring, Dockerfile, compose/postgres-init/Makefile wiring; go.mod direct deps/replaces verified; minors deferred: unbounded wait after forced Stop, shutdown test only covers pre-cancel ordering, invalid/non-positive env values untested)
Task 8: complete (commit 8f8d185, review clean; Fraud gate after barring and before trunk reservation, skip on unset addr/empty account, outage fraud_unavailable, hard-deny passthrough, reservation release on deny/reserve error/EndCall; minors deferred: stale CheckCall comment, releaseFraud error unlogged, EndCall error after slot release, ordering/empty-account/reserve-error coverage gaps)
Task 9: complete (commit e869735, review clean; sole SBC CheckCall call site now forwards StartCall account id verbatim, regression test verified; minor deferred: empty-account propagation untested)
Task 10: complete (commits 0204292 and fix 0c95563; brief's impossible 15/0/0/15 corrected by controller-approved Option D: 35 cases, 15/0/5/15, precision 1.000, recall 0.750; production threshold/scorer unchanged; f19 history day-scoped to keep replay production-faithful; minors deferred: empty StartedAt guard, account-call day scope, duplicated detection logic)
Task 11: complete (commit 7870e96; review clean after 2 aborted reviewer dispatches, controller-reviewed) — fraudsmoke probe, smoke.sh (10 DBs, fraud service/controls/risk-prefix/probe/Jaeger), AGENTS P6 bullet verbatim. Bundled justified fix: readSignals left rows open before same-tx QueryRow (`conn busy` broke CheckCall after any prefix-matching prior CDR); rows.Close() + TestReserveAfterProcessedCDR regression, TDD RED/GREEN in report. Minors deferred: test discards reserveDirect err, rows.Close() error ignored
Final gate: make test exit 0 (14 modules, fraud re-run -count=1), make smoke PASS incl. ok: fraud + traces for fraud; no TODO/TBD in fraud code; docs/fraud-baseline.md precision 1.000 / recall 0.750 (gates 0.80/0.70).
MERGED: p6-fraud-mvp fast-forwarded to master at 7870e96; branch deleted; gate results apply to merged tree.

## P4 remainder: SIP edge (plan docs/superpowers/plans/2026-11-19-sip-edge.md)
Task 1: complete (commits d9b4ecd..1e75991, review clean) — proto target_uri on routing (3/3) and switching (6); make protoc only, both modules build+vet.
Task 2: complete (commits 1e75991..c880f3a, review clean) — routing stores trunk target_uri; ResolveLcr excludes empty targets; migration idempotent; cost-0 unchanged.
Task 3: complete (commits c880f3a..c191510, review clean) — switching carries target_uri end to end; approved 2-line test adaptation (pb.NewSwitchingClient); denial order unchanged. Plan snippet corrected in d9b4ecd..HEAD docs fix.
Task 4: complete (commits c03ee25..c84c8ae, review clean) — switching HTTP facade /start /stop /live /healthz, env SWITCHING_HTTP_ADDR, thin adapter over registry; approved variable rename in test.
Task 5: complete (commits c84c8ae..65046d8, review approved with follow-up) — sipedge policy adapter, OTel spans; follow-up 65046d8..HEAD: unknown deny reason now 503 per design spec (plan/test/code reconciled). Handoff: Task 6 MUST remove compile-only kamRPC/endDialog/reaper/newReaper/run stubs from services/sipedge/client.go when adding real reaper.
Task 6: complete (commits 654d44e..3b5552a, review clean) — reaper polls switching /live, ends orphan Kamailio dialogs via dlg.end_dlg with numeric JSON-RPC params; Task 5 stubs removed; failure entries retained.
Task 7: complete (commits 3b5552a..61b0249, review clean) — Kamailio 5.8.3 cfg (jsonrpcs xhttp, dialog, async /invite /bye), sipedge Dockerfile, compose sip net + services; config adaptations verified with kamailio -c and live RPC.
Task 8: complete (commits 61b0249..32b7e01, review approved; controller root-caused two failures: Kamailio shm exhaustion after a retransmit storm, and sipuas emitting double-space CSeq headers that made sipsmoke miss the BYE 200; fresh controller `make smoke` PASS with ok: sip/deny/exhaust; review ⚠️ smoke claims resolved by controller verification, compose mapping confirmed; minors deferred: sipsmoke hardcoded gateway probe in localAddr (cmd/sipsmoke/main.go:234-243), smoke billed-session count is historical not delta (ops/smoke.sh:58-59), reaper 404 log noise (task-8-report.md:19), deny mode does not ACK the 403 (cmd/sipsmoke/main.go:99-104), unused 2783 LCR seed (ops/smoke.sh:40))
Task 9: complete (commits 32b7e01..c93628f, review approved) — ops/sip-e2e.sh answer+CDR, deny and exhaust script, executable; fresh controller `bash ops/sip-e2e.sh` PASS with all four markers; review ⚠️ branch process verified (p4-sip-edge, merge-base master=d9b4ecd); minor deferred: CDR check uses --from-beginning --max-messages 50 and can pass on a stale CDR (plan-mandated, ops/sip-e2e.sh:32-35)
Task 10: full gate initially FAILED make test (5 switching tests denied no_route): Task 2 made LCR skip targetless trunks (plan line 251 warned pre-existing seeds must add target_uri) but switching seedLcr still seeded no target. Fixed in commit 494b4da (seedLcr sets TargetUri; gofmt import order in calls.go/main.go). Re-ran full gate green: docker compose build switching + up -d; make test exit 0 (14 modules incl omnifoni/sipedge, switching fresh 11.959s); make smoke PASS incl ok: sip/deny/exhaust + traces for sipedge; bash ops/sip-e2e.sh PASS all four markers; git diff --check, bash -n both scripts, gofmt -l/go vet for sipedge+switching clean; no TODO/TBD/FIXME/XXX. Whole-branch review d9b4ecd..494b4da starting.
 104: Final review whole branch d9b4ecd..494b4da: 12 review findings closed plus self-review hardening in commit 819497e (15 files, 538+/63-) — OTel middleware/outbound instrumentation+request-id, idempotent invite replay with bounded 4096-entry 64s cache, max_seconds dialog cap, sanitized deny reason, trunk id on facade /start via Registry.callTrunkID (Finding #9 protobuf deviation documented), 404 end_dlg, fail-clear sipsmoke contact host, deny ACK, delta billing assertion, reason assertions, call-specific CDR check, Kamailio tm/sl order + healthcheck core.uptime + no xlog, From URI host fix for empty CLI, removed 2783 seeds, comment/param cleanup. Fresh gate after final edits: static checks clean; docker compose build sipedge switching + make up all healthy; kamailio -c ok, 144 lines; make test exit 0 (14 modules); make smoke PASS incl ok: sip/deny/exhaust + traces for sipedge; bash ops/sip-e2e.sh PASS all four markers; worktree clean. Report: .superpowers/sdd/final-review-fixes-report.md. No critical/important concerns.
105: Re-review d9b4ecd..819497e: 4 findings closed in ccea11b (timeout_noreset, per-Call-ID locks, cancel registry, reaper snapshot/recheck, narrow 404, t_newtran, anchored deny regex). Fresh review d9b4ecd..ccea11b then returned With fixes: 2 Important + 3 Minor, no Critical.
106: Second fix wave d09ab3b: /bye stop retries, reaper default 1s, cancel-before-decision, TCP 5060, numeric JSON-RPC params; also reverted the anchored deny regex because Kamailio parses the trailing `$` as a pseudo-variable and refuses startup. Follow-up 3c2b181 (TDD): markStopFailed bound to switchingID; reapInterval treats `0s`/negative as disabled. Full gate green.
107: Fresh whole-branch review d9b4ecd..3c2b181: With fixes; 0 Critical, 1 Important (reaper stop retry used unclaimed snapshot), 1 Minor (prefix-only Kamailio deny-reason validation). Closed in c3d6cbe with atomic claim/release helpers + replacement-entry tests and a full-string negative regex check that avoids the `$` pseudo-variable parse failure. Final gate green: gofmt/vet/race, make test 14 modules, kamailio -c, make smoke PASS incl sip/deny/exhaust/traces, ops/sip-e2e.sh PASS all markers, git diff --check. Report: .superpowers/sdd/final-review-fixes-3-report.md.
108: Ready to finish: branch p4-sip-edge HEAD c3d6cbe, worktree clean, final review findings closed. No remaining Critical/Important concerns. Next: finishing-a-development-branch.
