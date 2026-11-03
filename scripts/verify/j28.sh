#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
: "${ETCD_HBASE_JDK21_HOME:?Set ETCD_HBASE_JDK21_HOME to the pinned Temurin JDK 21 home.}"
REPORT="$ROOT/.artifacts/m2/j28/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$REPORT"

scripts/dev/mvn21.sh -B -ntp -pl core -am clean test > "$REPORT/maven.log" 2>&1 || {
  tail -n 100 "$REPORT/maven.log" >&2
  exit 1
}
python3 scripts/verify/j28-report.py "$REPORT"
git diff --check HEAD
