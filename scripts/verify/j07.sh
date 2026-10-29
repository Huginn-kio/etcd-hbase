#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
REPORT="$ROOT/.artifacts/m0/j07/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$REPORT"
python3 scripts/verify/j06-source.py "$REPORT"
python3 - "$REPORT" <<'PY_FROZEN'
import json
from pathlib import Path
import subprocess
import sys
source = json.loads((Path(sys.argv[1]) / "source.json").read_text())
untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"]).decode().split("\0")
local_only = ("docs/progress/", "docs/superpowers/", "scripts/verify/__pycache__/")
if source["tracked_dirty"] or any(path and not path.startswith(local_only) for path in untracked):
    raise SystemExit("J07 verification requires committed source; local execution records may remain untracked")
PY_FROZEN
python3 scripts/verify/go-module-lock.py > "$REPORT/module-lock.log" 2>&1
python3 scripts/verify/test_api_matrix.py
python3 scripts/verify/test_evidence.py
scripts/dev/go-m0.sh mod verify > "$REPORT/module-verify.log"
scripts/dev/go-m0.sh test -json ./... > "$REPORT/go-test.jsonl" 2>&1
GORACE=halt_on_error=1 scripts/dev/go-m0.sh test -race -json ./... > "$REPORT/go-race.jsonl" 2>&1
scripts/dev/go-m0.sh vet ./... > "$REPORT/go-vet.log" 2>&1
scripts/verify/native-oracle.sh --artifact-root "$REPORT/native" > "$REPORT/native.log" 2>&1
scripts/verify/wire.sh --artifact-root "$REPORT/wire" > "$REPORT/wire.log" 2>&1 || {
  tail -n 60 "$REPORT/wire.log" >&2
  exit 1
}
python3 scripts/verify/j07-report.py "$REPORT"
git diff --check
