#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

fail() { echo "FAIL: $1" >&2; exit 1; }

seed_rate() {
  docker exec omni-fon-postgres-1 psql -U postgres -d catalogue -qAt -c \
    "INSERT INTO rates(prefix,rate_per_sec) VALUES ('$1','$2') ON CONFLICT (prefix) DO UPDATE SET rate_per_sec=EXCLUDED.rate_per_sec" >/dev/null
}
seed_account() {
  docker exec omni-fon-postgres-1 psql -U postgres -d charging -qAt -c \
    "INSERT INTO accounts(account_id,kind,balance,credit_limit) VALUES ('$1','prepaid',$2,0) ON CONFLICT (account_id) DO UPDATE SET balance=EXCLUDED.balance" >/dev/null
}
seed_route() {
  docker exec omni-fon-postgres-1 psql -U postgres -d routing -qAt -c \
    "INSERT INTO trunks(trunk_id,enabled,target_uri) VALUES ('$1',true,'sip:sipuas:5060') ON CONFLICT (trunk_id) DO UPDATE SET enabled=true,target_uri=EXCLUDED.target_uri" >/dev/null
  docker exec omni-fon-postgres-1 psql -U postgres -d routing -qAt -c \
    "INSERT INTO lcr_routes(prefix,trunk_id,cost_per_sec) VALUES ('$2','$1',1) ON CONFLICT (prefix,trunk_id) DO UPDATE SET cost_per_sec=EXCLUDED.cost_per_sec" >/dev/null
}

docker compose up -d sipuas

# SIPEDGE_TRUNK_ACCOUNTS maps the one smoke source IP (172.28.0.1) to one
# account, so all three scenarios run as acc-sip-smoke.

# 1. Golden answer path + CDR.
seed_rate 2782 0.010000
seed_account acc-sip-smoke 100000
seed_route trunk-e2e 2782
(cd services/sipedge && SIP_SMOKE_DEST=27821110000 go run ./cmd/sipsmoke) || fail "answer call"
cdr=$(docker exec omni-fon-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic cdrs.normalized \
  --from-beginning --max-messages 50 --timeout-ms 5000 2>/dev/null | grep -c 'acc-sip-smoke' || true)
[ "$cdr" -ge 1 ] || fail "no cdr for acc-sip-smoke"
echo "ok: sip e2e answer + cdr"

# 2. Denial path: barring rule returns 403.
docker exec omni-fon-postgres-1 psql -U postgres -d sbc -qAt -c \
  "INSERT INTO barring_rules(match_type,prefix,action) SELECT 'destination','27829999','deny' WHERE NOT EXISTS (SELECT 1 FROM barring_rules WHERE prefix='27829999')" >/dev/null
seed_rate 2783 0.010000
seed_account acc-sip-smoke 100000
seed_route trunk-deny 2783
(cd services/sipedge && SIP_SMOKE_DEST=27829990000 SIP_SMOKE_MODE=deny go run ./cmd/sipsmoke) || fail "deny call"
echo "ok: sip e2e deny"

# 3. Exhaustion path: 1 s of authorization at a high rate, expect BYE
# within 4 s (balance 100000 minor / rate 100000 minor per second = 1 s).
seed_rate 2788 100000.000000
seed_account acc-sip-smoke 100000
seed_route trunk-exh 2788
(cd services/sipedge && SIP_SMOKE_DEST=27881110000 SIP_SMOKE_MODE=exhaust go run ./cmd/sipsmoke) || fail "exhaust call"
echo "ok: sip e2e exhaust"

echo "ok: sip e2e"
