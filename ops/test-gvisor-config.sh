#!/usr/bin/env bash
set -euo pipefail

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

docker compose -f docker-compose.yml config --format json > "$tmp/base.json"
docker compose -f docker-compose.yml -f docker-compose.gvisor.yml config --format json > "$tmp/gvisor.json"

python3 - "$tmp/base.json" "$tmp/gvisor.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    base = json.load(handle)
with open(sys.argv[2], encoding="utf-8") as handle:
    gvisor = json.load(handle)

base_runtime = {
    name: service.get("runtime")
    for name, service in base.get("services", {}).items()
    if service.get("runtime")
}
assert base_runtime == {}, f"base compose gained runtime: {base_runtime}"

services = gvisor.get("services", {})
assert services["payments"]["runtime"] == "runsc", services["payments"].get("runtime")

sandboxed = {
    name: service["runtime"]
    for name, service in services.items()
    if service.get("runtime")
}
assert sandboxed == {"payments": "runsc"}, sandboxed
PY
