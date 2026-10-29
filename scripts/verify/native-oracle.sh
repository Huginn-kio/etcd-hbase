#!/usr/bin/env bash
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCENARIO="$REPO_ROOT/testkit/scenarios/m0-native.yaml"
ARTIFACT_ROOT="$REPO_ROOT/.artifacts/m0/native/$(date -u +%Y%m%dT%H%M%SZ)-$$"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario)
      [[ $# -ge 2 ]] || { echo "--scenario requires a path" >&2; exit 2; }
      SCENARIO="$2"
      shift 2
      ;;
    --artifact-root)
      [[ $# -ge 2 ]] || { echo "--artifact-root requires a path" >&2; exit 2; }
      ARTIFACT_ROOT="$2"
      shift 2
      ;;
    *)
      echo "usage: $0 [--scenario path] [--artifact-root path]" >&2
      exit 2
      ;;
  esac
done

if [[ "$SCENARIO" != /* ]]; then
  SCENARIO="$REPO_ROOT/$SCENARIO"
fi
if [[ "$ARTIFACT_ROOT" != /* ]]; then
  ARTIFACT_ROOT="$REPO_ROOT/$ARTIFACT_ROOT"
fi

mkdir -p "$ARTIFACT_ROOT"
RUN_ONE="$ARTIFACT_ROOT/run-one"
RUN_TWO="$ARTIFACT_ROOT/run-two"

python3 "$REPO_ROOT/scripts/verify/go-module-lock.py"

"$REPO_ROOT/scripts/dev/native-etcd.sh" run \
  --scenario "$SCENARIO" \
  --artifact-root "$RUN_ONE" \
  --trace "$RUN_ONE/trace.jsonl" \
  --metadata "$RUN_ONE/metadata.json"
"$REPO_ROOT/scripts/dev/native-etcd.sh" run \
  --scenario "$SCENARIO" \
  --artifact-root "$RUN_TWO" \
  --trace "$RUN_TWO/trace.jsonl" \
  --metadata "$RUN_TWO/metadata.json"

"$REPO_ROOT/scripts/dev/native-etcd.sh" compare \
  --left "$RUN_ONE/trace.jsonl" --right "$RUN_TWO/trace.jsonl"
"$REPO_ROOT/scripts/dev/native-etcd.sh" normalize \
  --input "$RUN_ONE/trace.jsonl" --output "$ARTIFACT_ROOT/normalized-one.jsonl"
"$REPO_ROOT/scripts/dev/native-etcd.sh" normalize \
  --input "$RUN_TWO/trace.jsonl" --output "$ARTIFACT_ROOT/normalized-two.jsonl"
cmp "$ARTIFACT_ROOT/normalized-one.jsonl" "$ARTIFACT_ROOT/normalized-two.jsonl"

expect_rejected() {
  local mutation="$1"
  local output="$ARTIFACT_ROOT/mutated-$mutation.jsonl"
  python3 "$REPO_ROOT/scripts/verify/mutate-native-trace.py" "$RUN_ONE/trace.jsonl" "$output" "$mutation"
  if "$REPO_ROOT/scripts/dev/native-etcd.sh" compare \
    --left "$RUN_ONE/trace.jsonl" --right "$output" \
    >"$ARTIFACT_ROOT/mutated-$mutation.log" 2>&1; then
    echo "native compare incorrectly accepted $mutation mutation" >&2
    exit 1
  fi
  python3 - "$ARTIFACT_ROOT/mutated-$mutation.log" <<'PY_CHECK'
import sys
from pathlib import Path
message = Path(sys.argv[1]).read_text()
if not message.startswith(("trace record ", "record count differs:", "read right trace:", "normalize right trace:")):
    raise SystemExit("negative oracle check failed for an unrelated reason")
PY_CHECK
}

for mutation in watch-event more deleted revision txn-branch prev-kv stream-terminal error-kind count-existence newline; do
  expect_rejected "$mutation"
done

echo "native etcd oracle passed; artifacts=$ARTIFACT_ROOT"
