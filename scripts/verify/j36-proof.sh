#!/usr/bin/env bash
set -euo pipefail
umask 077
export PYTHONDONTWRITEBYTECODE=1
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
if [[ $# != 2 || "$1" != --artifact-root || -z "$2" ]]; then
  echo 'usage: j36-proof.sh --artifact-root <fresh local artifact directory>' >&2
  exit 2
fi
: "${ETCD_HBASE_JDK21_HOME:?Set ETCD_HBASE_JDK21_HOME to the locked JDK 21 home.}"
if [[ -n "$(git status --porcelain)" ]]; then
  echo 'J36 component evidence requires a committed, clean source tree' >&2
  exit 2
fi
source_commit="$(git rev-parse HEAD)"
artifact_root="$(python3 - "$ROOT" "$2" <<'PY'
from pathlib import Path
import re
import sys
root = Path(sys.argv[1]).resolve() / '.artifacts' / 'j36-proof'
candidate = Path(sys.argv[2]).resolve()
if (not candidate.is_relative_to(root) or candidate == root
        or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}', candidate.name)):
    raise SystemExit('J36 proof artifacts must use a fresh directory under .artifacts/j36-proof')
candidate.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
candidate.mkdir(mode=0o700)
print(candidate)
PY
)"
python3 scripts/verify/test_j36_proof_report.py
scripts/dev/mvn21.sh -B -ntp -pl server -am -P m3-j35-public \
  -Dtest=ReadViewRecordStoreTest,ManifestPreparationTest,RangeResultCacheTest,BudgetManagerTest,RuntimeServerTest,M3RuntimeConfigurationTest,HBaseSystemRecordDecodeTest,M3CacheScopeDrainTest \
  -Dsurefire.failIfNoSpecifiedTests=false '-Dit.test=M2ReadViewIT,M3CacheProofIT,M3CacheRuntimeIT,M3CacheProcessIT,M3CacheOversizeIT,SecureHBaseAuthenticationIT#revokedPermissionDeniesLiveBackendReadsAndFreshAuthentication' \
  -Dfailsafe.failIfNoSpecifiedTests=false clean verify > "$artifact_root/maven.log" 2>&1 || {
    tail -n 120 "$artifact_root/maven.log" >&2
    exit 1
  }
for module in core hbase server; do
  for kind in surefire-reports failsafe-reports; do
    for report in "$module"/target/"$kind"/TEST-*.xml; do
      [[ -f "$report" ]] || continue
      mkdir -p "$artifact_root/$module/target/$kind"
      cp "$report" "$artifact_root/$module/target/$kind/"
    done
  done
done
python3 scripts/verify/j36-proof-report.py --artifact-root "$artifact_root" --source "$source_commit"
git diff --check HEAD
