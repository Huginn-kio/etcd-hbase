#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
: "${ETCD_HBASE_JDK21_HOME:?Set ETCD_HBASE_JDK21_HOME to the pinned Temurin JDK 21 home.}"
REPORT="$ROOT/.artifacts/m2/j29/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$REPORT"

scripts/dev/mvn21.sh -B -ntp -pl hbase -am clean verify -Pj29-hbase-local \
  > "$REPORT/maven.log" 2>&1 || {
    tail -n 120 "$REPORT/maven.log" >&2
    exit 1
  }
python3 scripts/verify/j29-report.py "$REPORT"
git diff --check HEAD
