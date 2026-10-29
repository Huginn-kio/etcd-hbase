#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
REPORT="$ROOT/.artifacts/m0/j08/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$REPORT"
python3 scripts/verify/j06-source.py "$REPORT"
python3 scripts/verify/source-frozen.py "$REPORT"
python3 scripts/verify/test_m0_spi.py
scripts/dev/mvn21.sh -B -ntp -pl testkit,hbase -am clean verify > "$REPORT/maven.log" 2>&1 || {
  tail -n 60 "$REPORT/maven.log" >&2
  exit 1
}
python3 scripts/verify/m0-spi.py --jar spi/target/spi-0.1.0-SNAPSHOT.jar > "$REPORT/boundary.json"
python3 scripts/verify/j08-report.py "$REPORT"
git diff --check
