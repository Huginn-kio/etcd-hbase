#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
REPORT="$ROOT/.artifacts/m0/j06/$RUN_ID"
mkdir -p "$REPORT"
export J06_REPORT="$REPORT"
python3 - "$REPORT" <<'PY'
import json, pathlib, subprocess, sys, datetime
output=pathlib.Path(sys.argv[1])
def git(*args): return subprocess.check_output(['git',*args],text=True).strip()
(output/'source.json').write_text(json.dumps({'head':git('rev-parse','HEAD'), 'base':git('rev-parse','origin/main'), 'tracked_dirty':bool(git('status','--porcelain','--untracked-files=no')), 'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
PY
scripts/dev/mvn21.sh -B -ntp -pl server -am -Pj03-wire,j04-hbase-local clean verify > "$REPORT/maven.log" 2>&1 || {
  tail -n 70 "$REPORT/maven.log" >&2
  exit 1
}
python3 scripts/verify/j06-report.py "$REPORT"
git diff --check
