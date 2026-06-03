#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_PATH="$ROOT_DIR/.tmp/core.cover.out"
MIN_COVERAGE="${1:-72.0}"

mkdir -p "$ROOT_DIR/.tmp"

pushd "$ROOT_DIR" >/dev/null

go test -coverprofile="$PROFILE_PATH" \
  ./internal/core/workflow \
  ./internal/state \
  ./internal/core/policyguard \
  ./internal/core/schemas

TOTAL_COVERAGE="$(go tool cover -func="$PROFILE_PATH" | awk '/^total:/ {gsub("%", "", $3); print $3}')"

python3 - "$TOTAL_COVERAGE" "$MIN_COVERAGE" <<'PY'
import sys

actual = float(sys.argv[1])
minimum = float(sys.argv[2])

if actual < minimum:
    print(f"core package coverage gate failed: {actual:.1f}% < {minimum:.1f}%")
    sys.exit(1)

print(f"core package coverage gate passed: {actual:.1f}% >= {minimum:.1f}%")
PY

popd >/dev/null
