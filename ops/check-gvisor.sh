#!/usr/bin/env bash
set -euo pipefail

if ! docker info --format '{{json .Runtimes}}' 2>/dev/null | grep -q '"runsc"'; then
  echo "runsc runtime not registered" >&2
  exit 1
fi

inspect="$(docker inspect --format '{{.State.Running}} {{.HostConfig.Runtime}}' omni-fon-payments-1 2>/dev/null || true)"
if [[ -z "$inspect" ]]; then
  echo "payments container missing" >&2
  exit 1
fi

read -r running runtime <<<"$inspect"
if [[ "$running" != "true" ]]; then
  echo "payments container not running" >&2
  exit 1
fi
if [[ "$runtime" != "runsc" ]]; then
  echo "payments runtime is $runtime, expected runsc" >&2
  exit 1
fi

echo "payments runtime is runsc"
