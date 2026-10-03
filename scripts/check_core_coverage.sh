#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_PATH="$ROOT_DIR/.tmp/core.cover.out"
MIN_COVERAGE="${1:-72.0}"
COVERAGE_GO="${GO:-go}"
TEST_CPUS="$(python3 "$ROOT_DIR/scripts/test_cpu_budget.py")"
export KINGDOM_TEST_CPUS="$TEST_CPUS"
export GOMAXPROCS="$TEST_CPUS"

mkdir -p "$ROOT_DIR/.tmp"

pushd "$ROOT_DIR" >/dev/null

# Core state is exercised through its owning API, planning, and orchestration
# services as well as package-local tests. Instrument the core packages across
# those consumers so the gate measures real behavioral coverage rather than
# treating integration-owned paths as uncovered.
"$COVERAGE_GO" test -p=1 -parallel="$TEST_CPUS" \
  -coverpkg=./internal/core/workflow,./internal/state,./internal/core/policyguard,./internal/core/schemas \
  -coverprofile="$PROFILE_PATH" \
  ./internal/core/workflow \
  ./internal/state \
  ./internal/core/policyguard \
  ./internal/core/schemas \
  ./internal/planning \
  ./internal/api \
  ./internal/orchestration

TOTAL_COVERAGE="$("$COVERAGE_GO" tool cover -func="$PROFILE_PATH" | awk '/^total:/ {gsub("%", "", $3); print $3}')"

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
