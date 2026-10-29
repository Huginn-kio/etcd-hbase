#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
REPORT="$ROOT/.artifacts/m0/j07/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$REPORT"
python3 scripts/verify/j06-source.py "$REPORT"
python3 scripts/verify/source-frozen.py "$REPORT"
python3 scripts/verify/go-module-lock.py > "$REPORT/module-lock.log" 2>&1
python3 scripts/verify/test_api_matrix.py
python3 scripts/verify/test_evidence.py
scripts/dev/go-m0.sh mod verify > "$REPORT/module-verify.log"
verify_go_tests() {
  local report_name="$1"
  shift
  if ! scripts/dev/go-m0.sh test -json "$@" ./... > "$REPORT/$report_name.jsonl" 2> "$REPORT/$report_name.stderr.log"; then
    tail -n 50 "$REPORT/$report_name.stderr.log" >&2
    tail -n 20 "$REPORT/$report_name.jsonl" >&2
    return 1
  fi
}
verify_go_tests go-test
GORACE=halt_on_error=1 verify_go_tests go-race -race
scripts/dev/go-m0.sh vet ./... > "$REPORT/go-vet.log" 2>&1
scripts/verify/native-oracle.sh --artifact-root "$REPORT/native" > "$REPORT/native.log" 2>&1
scripts/verify/wire.sh --artifact-root "$REPORT/wire" > "$REPORT/wire.log" 2>&1 || {
  tail -n 60 "$REPORT/wire.log" >&2
  exit 1
}
python3 scripts/verify/j07-report.py "$REPORT"
git diff --check
