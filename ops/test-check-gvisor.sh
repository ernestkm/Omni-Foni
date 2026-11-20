#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/bin"

cat > "$tmp/bin/docker" <<'STUB'
#!/usr/bin/env bash
case "$1 $2" in
  "info --format") printf '%s\n' "${FAKE_RUNTIMES-}" ;;
  "inspect --format") printf '%s\n' "${FAKE_INSPECT-}" ;;
  *) exit 64 ;;
esac
STUB
chmod +x "$tmp/bin/docker"
export PATH="$tmp/bin:$PATH"

expect_fail() {
  local expected="$1"
  shift
  local output
  local status
  set +e
  output="$("$@" 2>&1)"
  status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    echo "expected failure containing: $expected"
    exit 1
  fi
  if ! grep -qF "$expected" <<<"$output"; then
    echo "expected message: $expected"
    echo "actual output: $output"
    exit 1
  fi
}

expect_pass() {
  local expected="$1"
  shift
  local output
  output="$("$@" 2>&1)"
  if ! grep -qF "$expected" <<<"$output"; then
    echo "expected message: $expected"
    echo "actual output: $output"
    exit 1
  fi
}

FAKE_RUNTIMES='{"runc":{}}' FAKE_INSPECT='true runsc' \
  expect_fail "runsc runtime not registered" bash ops/check-gvisor.sh

FAKE_RUNTIMES='{"runsc":{}}' FAKE_INSPECT='' \
  expect_fail "payments container missing" bash ops/check-gvisor.sh

FAKE_RUNTIMES='{"runsc":{}}' FAKE_INSPECT='false runsc' \
  expect_fail "payments container not running" bash ops/check-gvisor.sh

FAKE_RUNTIMES='{"runsc":{}}' FAKE_INSPECT='true runc' \
  expect_fail "payments runtime is runc, expected runsc" bash ops/check-gvisor.sh

FAKE_RUNTIMES='{"runsc":{}}' FAKE_INSPECT='true runsc' \
  expect_pass "payments runtime is runsc" bash ops/check-gvisor.sh
