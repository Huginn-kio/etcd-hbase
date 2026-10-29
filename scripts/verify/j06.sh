#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
REPORT="$ROOT/.artifacts/m0/j06/$RUN_ID"
mkdir -p "$REPORT"
python3 scripts/verify/j06-source.py "$REPORT"
python3 scripts/verify/test_j06_source.py
python3 scripts/verify/test_j06_report.py
scripts/dev/mvn21.sh -B -ntp -pl server -am -Pj03-wire,j04-hbase-local clean verify > "$REPORT/maven.log" 2>&1 || {
  tail -n 70 "$REPORT/maven.log" >&2
  exit 1
}
python3 scripts/verify/j06-report.py "$REPORT"
git diff --check
