#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $1" >&2; exit 1; }

for c in postgres valkey kafka clickhouse jaeger; do
  h=$(docker inspect --format '{{.State.Health.Status}}' "omni-fon-$c-1" 2>/dev/null || echo missing)
  [ "$h" = "healthy" ] || fail "$c not healthy ($h)"
  echo "ok: $c healthy"
done

docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT 1 FROM pg_database WHERE datname='charging'" | grep -q 1 || fail "charging DB missing"

dbs=$(docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT count(*) FROM pg_database WHERE datistemplate=false AND datname IN ('identity','catalogue','charging','billing','payments','routing','sbc','mediation','reports_sched')")
[ "$dbs" = "9" ] || fail "expected 9 context databases, got $dbs"

docker exec omni-fon-valkey-1 valkey-cli ping | grep -q PONG || fail "valkey"
curl -fsS http://localhost:8123/ping >/dev/null || fail "clickhouse"
docker exec omni-fon-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 || fail "kafka"

for s in charging switching mediation billing payments identity routing sbc reconciliation reports; do
  st=$(docker inspect --format '{{.State.Status}}' "omni-fon-$s-1" 2>/dev/null || echo missing)
  [ "$st" = "running" ] || fail "$s not running ($st)"
  echo "ok: $s running"
done

tid=$(docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "SELECT tenant_id FROM tenants WHERE name='smoke' LIMIT 1")
if [ -z "$tid" ]; then
  tid=$(docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "INSERT INTO tenants(name) VALUES ('smoke') RETURNING tenant_id")
fi
uid=$(docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "INSERT INTO users(tenant_id,email,password_hash,role) VALUES ('$tid','smoke@local','x','admin') ON CONFLICT (email) DO UPDATE SET tenant_id=EXCLUDED.tenant_id RETURNING user_id")
docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "INSERT INTO sessions(token,user_id,expires_at) VALUES ('tok-smoke','$uid',now()+interval '1 hour') ON CONFLICT (token) DO UPDATE SET expires_at=now()+interval '1 hour'" >/dev/null
docker exec omni-fon-postgres-1 psql -U postgres -d catalogue -qAt -c "INSERT INTO rates(prefix,rate_per_sec) VALUES ('2782',0.010000) ON CONFLICT (prefix) DO UPDATE SET rate_per_sec=EXCLUDED.rate_per_sec" >/dev/null
docker exec omni-fon-postgres-1 psql -U postgres -d charging -qAt -c "INSERT INTO accounts(account_id,kind,balance,credit_limit) VALUES ('acc-smoke-live','prepaid',100000,0) ON CONFLICT (account_id) DO UPDATE SET balance=EXCLUDED.balance, kind='prepaid', credit_limit=0" >/dev/null
docker exec omni-fon-postgres-1 psql -U postgres -d routing -qAt -c "INSERT INTO trunks(trunk_id,enabled) VALUES ('trunk-smoke',true) ON CONFLICT (trunk_id) DO UPDATE SET enabled=true" >/dev/null
docker exec omni-fon-postgres-1 psql -U postgres -d routing -qAt -c "INSERT INTO lcr_routes(prefix,trunk_id,cost_per_sec) VALUES ('2782','trunk-smoke',1) ON CONFLICT (prefix,trunk_id) DO UPDATE SET cost_per_sec=EXCLUDED.cost_per_sec" >/dev/null
docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "INSERT INTO accounts(account_id,tenant_id,created_by) VALUES ('acc-smoke-live','$tid','$uid') ON CONFLICT (account_id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, created_by=EXCLUDED.created_by" >/dev/null
csv=$(curl -fsS -H 'Authorization: Bearer tok-smoke' "http://localhost:50062/reports/cdrs?from=2026-01-01&to=2026-01-02&tenant_id=$tid&format=csv")
hdr=${csv%%$'\n'*}
[ "$hdr" = "call_id,account_id,destination,cli,started_at_ms,ended_at_ms,charged_seconds,end_reason,trunk_id,rate_per_sec,amount_minor,rate_missing" ] || fail "reports csv"
echo "ok: reports csv"

(cd services/reconciliation && go run ./cmd/smokerecon) || fail "reconciliation probe"
(cd services/reports && go run ./cmd/smokereports) || fail "reports probe"
(cd services/reports && go run ./cmd/smokelive) || fail "live calls probe"

sched=$(curl -fsS -X POST -H 'Authorization: Bearer tok-smoke' -H 'Content-Type: application/json' \
  -d '{"report_kind":"cdrs","window_kind":"previous_month","frequency":"daily","at_hour":0,"at_minute":0}' \
  "http://localhost:50062/scheduled-reports?tenant_id=$tid")
sid=$(echo "$sched" | sed -E 's/.*"schedule_id":"([^"]+)".*/\1/')
[ -n "$sid" ] || fail "scheduled report create"
code=$(curl -sS -o /tmp/sched-run.json -w '%{http_code}' -X POST -H 'Authorization: Bearer tok-smoke' \
  "http://localhost:50062/scheduled-reports/$sid/run?tenant_id=$tid")
[ "$code" = "202" ] || fail "scheduled report run ($code)"
aid=$(sed -E 's/.*"artifact_id":"([^"]+)".*/\1/' /tmp/sched-run.json)
[ -n "$aid" ] || fail "scheduled report artifact"
curl -fsS -D /tmp/sched-hdr.txt -o /tmp/sched.csv -H 'Authorization: Bearer tok-smoke' \
  "http://localhost:50062/scheduled-reports/$sid/artifacts/$aid/download?tenant_id=$tid" || fail "scheduled report download"
hdr=$(head -n 1 /tmp/sched.csv)
[ "$hdr" = "call_id,account_id,destination,cli,started_at_ms,ended_at_ms,charged_seconds,end_reason,trunk_id,rate_per_sec,amount_minor,rate_missing" ] || fail "scheduled report csv"
grep -qi '^content-disposition: attachment' /tmp/sched-hdr.txt || fail "scheduled report headers"
echo "ok: scheduled report"

# The probe issues billing and payments RPCs through reconciliation, so the
# trace backend must have spans for all three; otherwise tracing is broken.
services=$(curl -fsS http://localhost:16686/api/services) || fail "jaeger api"
for s in reconciliation billing payments reports; do
  echo "$services" | grep -q "\"$s\"" || fail "no traces for $s"
  echo "ok: traces for $s"
done

echo "smoke: PASS"
