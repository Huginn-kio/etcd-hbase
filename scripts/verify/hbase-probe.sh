#!/usr/bin/env bash
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
if [[ "$#" -lt 2 || "$1" != --profile ]]; then
  printf '%s\n' 'Usage: scripts/verify/hbase-probe.sh --profile public-local' >&2
  exit 2
fi
PROFILE="$2"
shift 2
if [[ "$PROFILE" == target ]]; then
  printf '%s\n' 'target-not-configured: target authentication, TLS and authorized resources are not yet verified; public-local is a separate scope' >&2
  exit 3
fi
if [[ "$PROFILE" != public-local || "$#" -ne 0 ]]; then
  printf '%s\n' 'Expected --profile public-local with no additional arguments' >&2
  exit 2
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
ARTIFACT_ROOT="$REPO_ROOT/.artifacts/m0/j04/$RUN_ID"
mkdir -p "$ARTIFACT_ROOT"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
record_exit() {
  python3 - "$ARTIFACT_ROOT" "$STARTED_AT" "$1" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
Path(sys.argv[1], "execution.json").write_text(json.dumps({
    "started_at": sys.argv[2], "finished_at": datetime.now(timezone.utc).isoformat(),
    "exit_code": int(sys.argv[3]),
}, indent=2) + "\n")
PY
}
trap 'record_exit "$?"' EXIT
"$REPO_ROOT/scripts/dev/mvn21.sh" -B -ntp -pl hbase -am clean verify -Pj04-hbase-local

python3 - "$REPO_ROOT" "$ARTIFACT_ROOT" <<'PY'
import hashlib
import json
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

root, artifacts = map(Path, sys.argv[1:])
totals = dict.fromkeys(("tests", "failures", "errors", "skipped"), 0)
suites = []
for module in ("spi", "core", "hbase"):
    reports = sorted((root / module / "target/surefire-reports").glob("TEST-*.xml"))
    if module == "hbase":
        reports += sorted((root / module / "target/failsafe-reports").glob("TEST-*.xml"))
    if not reports:
        raise SystemExit("missing J04 test reports for " + module)
    for report in reports:
        suite = ET.parse(report).getroot()
        counts = {key: int(suite.attrib.get(key, "0")) for key in totals}
        if not counts["tests"] or any(counts[key] for key in ("failures", "errors", "skipped")):
            raise SystemExit("J04 suite did not pass without skips: " + suite.attrib.get("name", ""))
        relative = report.relative_to(root)
        retained = artifacts / relative
        retained.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(report, retained)
        suites.append({"file": str(relative), "sha256": hashlib.sha256(report.read_bytes()).hexdigest(), **counts})
        for key, value in counts.items():
            totals[key] += value
integration = root / "hbase/target/failsafe-reports/TEST-io.etcdhbase.hbase.probe.LocalHBaseProbeIT.xml"
if not integration.is_file() or int(ET.parse(integration).getroot().attrib.get("tests", "0")) != 1:
    raise SystemExit("J04 real HBase integration test must execute exactly once")
output = "\n".join(item.text or "" for item in ET.parse(integration).getroot().iter("system-out"))
observed = re.search(r"hbase.version=(\S+) hbase.jvm=(\S+) client.jvm=(\S+)", output)
if observed is None:
    raise SystemExit("J04 integration report must contain the observed HBase and JVM versions")
lock_path = root / "build/dependency-lock.json"
lock = json.loads(lock_path.read_text())
summary = {
    "task": "J04", "scope": "public-local", "state": "passed", "tests": totals,
    "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    "tracked_changes": bool(subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True).strip()),
    "suites": suites,
    "versions": {"hbase_server": observed[1], "server_jvm": observed[2], "client_jvm": observed[3],
                 "hbase_client": lock["hbase"]["public"]["version"],
                 "hadoop_client": lock["hbase"]["public"]["hadoopVersion"]},
    "dependency_lock_sha256": hashlib.sha256(lock_path.read_bytes()).hexdigest(),
    "backend": "temporary HBase; simple authentication; not target credentials",
    "digest": "official HBase SASL client protocol fixture; not target HBase acceptance",
    "target_hbase": "not_verified", "target_authentication": "not_verified",
}
(artifacts / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print("J04 public-local verification passed: " + json.dumps(totals))
print("Evidence: " + str(artifacts / "summary.json"))
PY
