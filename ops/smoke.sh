#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $1" >&2; exit 1; }

for c in postgres valkey kafka clickhouse; do
  h=$(docker inspect --format '{{.State.Health.Status}}' "omni-fon-$c-1" 2>/dev/null || echo missing)
  [ "$h" = "healthy" ] || fail "$c not healthy ($h)"
  echo "ok: $c healthy"
done

docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT 1 FROM pg_database WHERE datname='charging'" | grep -q 1 || fail "charging DB missing"

dbs=$(docker exec omni-fon-postgres-1 psql -U postgres -At -c "SELECT count(*) FROM pg_database WHERE datistemplate=false AND datname IN ('identity','catalogue','charging','billing','payments','routing','sbc','mediation')")
[ "$dbs" = "8" ] || fail "expected 8 context databases, got $dbs"

docker exec omni-fon-valkey-1 valkey-cli ping | grep -q PONG || fail "valkey"
curl -fsS http://localhost:8123/ping >/dev/null || fail "clickhouse"
docker exec omni-fon-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 || fail "kafka"

echo "smoke: PASS"
