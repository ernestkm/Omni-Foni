#!/usr/bin/env bash
# Inserts N synthetic CDRs, times 20 account+window summary queries and prints
# p95 in milliseconds. Not part of `make test`; target is p95 < 2000 ms at N=10M.
set -euo pipefail
cd "$(dirname "$0")/.."

N=${1:-10000000}
ch() {
  docker exec omni-fon-clickhouse-1 clickhouse-client --user omnifoni --password dev-only "$@"
}

echo "inserting $N rows..."
ch --query "
INSERT INTO reports.cdrs
  (call_id, account_id, destination, cli, started_at, ended_at, charged_seconds,
   end_reason, trunk_id, rate_per_sec, amount_minor, rate_missing)
SELECT concat('bench-', toString(number)),
       concat('acct-', toString(number % 100)),
       concat('2782', toString(number % 10000)),
       '2721000',
       toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number % 7776000),
       toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number % 7776000) + 60,
       60, 'normal', concat('trunk-', toString(number % 5)),
       toDecimal64('0.012345', 6), 1, false
FROM numbers($N)"

times=()
for _ in $(seq 1 20); do
  t=$(ch --time --format Null --query "
    SELECT count(), sum(amount_minor)
    FROM reports.cdrs FINAL
    WHERE account_id IN ('acct-1','acct-2')
      AND started_at >= toDateTime64('2026-01-01 00:00:00', 3, 'UTC')
      AND started_at <  toDateTime64('2026-03-01 00:00:00', 3, 'UTC')" 2>&1 | tail -1)
  times+=("$t")
done

p95=$(printf '%s\n' "${times[@]}" | sort -n | awk '{a[NR]=$1} END {print a[int(NR*0.95)]}')
p95_ms=$(awk -v t="$p95" 'BEGIN {printf "%.0f", t*1000}')
echo "p95: ${p95_ms} ms"
awk -v ms="$p95_ms" 'BEGIN {if (ms > 2000) {print "WARN: p95 above the 2000 ms target"; exit 1}}'
echo "bench: PASS"

ch --query "DELETE FROM reports.cdrs WHERE call_id LIKE 'bench-%'"
echo "cleaned bench rows"
