#!/usr/bin/env bash
set -euo pipefail
umask 077
export PYTHONDONTWRITEBYTECODE=1
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"

scope=''
artifact_root=''
tasks=()
while (($#)); do
  case "$1" in
    --scope) (($# >= 2)) || { echo 'missing scope' >&2; exit 2; }; scope="$2"; shift 2 ;;
    --task) (($# >= 2)) || { echo 'missing task' >&2; exit 2; }; tasks+=("$2"); shift 2 ;;
    --artifact-root) (($# >= 2)) || { echo 'missing artifact root' >&2; exit 2; }; artifact_root="$2"; shift 2 ;;
    *) echo 'unknown M3 verification option' >&2; exit 2 ;;
  esac
done
if [[ "$scope" != public || -z "$artifact_root" || ${#tasks[@]} != 1 || "${tasks[0]}" != J30 ]]; then
  echo 'only public J30 verification is registered; other tasks are not yet delivered' >&2
  exit 2
fi
: "${ETCD_HBASE_JDK21_HOME:?Set ETCD_HBASE_JDK21_HOME to the pinned Temurin JDK 21 home.}"
if [[ -n "$(git status --porcelain)" ]]; then
  echo 'M3 evidence requires a committed, clean source tree' >&2
  exit 2
fi
source_commit="$(git rev-parse HEAD)"
artifact_root="$(python3 - "$ROOT" "$artifact_root" <<'PY'
from pathlib import Path
import re
import sys

repository = Path(sys.argv[1]).resolve()
allowed = repository / '.artifacts' / 'm3'
candidate = Path(sys.argv[2]).resolve()
if (not candidate.is_relative_to(allowed) or candidate == allowed
        or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', candidate.name)):
    raise SystemExit('M3 artifact root must be a fresh named directory under .artifacts/m3')
candidate.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
candidate.mkdir(mode=0o700)
print(candidate)
PY
)"
run_id="$(basename "$artifact_root")"
prefix="etcd-hbase-m3-test/$run_id/"
export M3_ARTIFACT_ROOT="$artifact_root" M3_RUN_ID="$run_id" M3_SOURCE_COMMIT="$source_commit"

python3 scripts/verify/test_m3_report.py
python3 scripts/verify/m3-report.py --validate-registry
python3 scripts/verify/test_evidence.py
python3 scripts/verify/go-module-lock.py
scripts/dev/go-m0.sh test -count=1 -json ./... > "$artifact_root/go-test.jsonl" 2> "$artifact_root/go-test.stderr" || {
  tail -n 100 "$artifact_root/go-test.jsonl" >&2
  tail -n 100 "$artifact_root/go-test.stderr" >&2
  exit 1
}
scripts/dev/go-program.sh m3driver --scenario testkit/scenarios/m3-put.yaml \
  --native-data-dir "$artifact_root/native-data" --scope native-etcd --prefix "$prefix" \
  --trace "$artifact_root/native-trace.jsonl" --metadata "$artifact_root/native-metadata.json" \
  > "$artifact_root/native.log" 2>&1 || {
  tail -n 100 "$artifact_root/native.log" >&2
  exit 1
}
scripts/dev/mvn21.sh -B -ntp -pl server -am clean verify -Pm3-hbase-local,m3-init-local,m3-public \
  > "$artifact_root/maven.log" 2>&1 || {
  tail -n 120 "$artifact_root/maven.log" >&2
  exit 1
}
for module in hbase admin server; do
  mkdir -p "$artifact_root/$module/target/failsafe-reports"
  cp "$module"/target/failsafe-reports/TEST-*.xml "$artifact_root/$module/target/failsafe-reports/"
done
python3 - "$artifact_root/run.json" <<'PY'
import json
import os
import sys

record = {'schema': 'etcd-hbase-m3-run/v1', 'scope': 'public-local-minicluster',
          'run_id': os.environ['M3_RUN_ID'], 'source_commit': os.environ['M3_SOURCE_COMMIT'],
          'tasks': ['J30'], 'commands_passed': True}
with open(sys.argv[1], 'x', encoding='utf-8') as stream:
    json.dump(record, stream)
    stream.write('\n')
PY
python3 scripts/verify/m3-report.py --artifact-root "$artifact_root"
git diff --check HEAD
