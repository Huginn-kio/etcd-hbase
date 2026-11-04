#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
REPORT="$ROOT/.artifacts/j23/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$REPORT"

scripts/dev/mvn21.sh -B -ntp -pl core,server,hbase -am clean verify -Pj23-review-local \
  > "$REPORT/maven.log" 2>&1 || {
  tail -n 80 "$REPORT/maven.log" >&2
  exit 1
}

python3 - "$REPORT" <<'PY'
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

root = Path.cwd()
report_dir = Path(sys.argv[1])
expected = {
    "core/target/surefire-reports/TEST-io.etcdhbase.core.commit.CommitPublisherTest.xml",
    "core/target/surefire-reports/TEST-io.etcdhbase.core.commit.CommitOutcomeResolverTest.xml",
    "server/target/surefire-reports/TEST-io.etcdhbase.server.commit.BoundedPublicationHandoffTest.xml",
    "hbase/target/surefire-reports/TEST-io.etcdhbase.hbase.codec.CommitDecisionMessagesTest.xml",
    "hbase/target/failsafe-reports/TEST-io.etcdhbase.hbase.storage.J23CommitDecisionRepositoryIT.xml",
}
results = []
for relative in sorted(expected):
    path = root / relative
    if not path.is_file():
        raise SystemExit(f"required J23 test report is missing: {relative}")
    suite = ET.parse(path).getroot()
    item = {
        "report": relative,
        "tests": int(suite.attrib.get("tests", "0")),
        "failures": int(suite.attrib.get("failures", "0")),
        "errors": int(suite.attrib.get("errors", "0")),
        "skipped": int(suite.attrib.get("skipped", "0")),
    }
    if item["tests"] == 0 or item["failures"] or item["errors"]:
        raise SystemExit(f"J23 test report is not green: {item}")
    results.append(item)

summary = {
    "checkout_sha": subprocess.run(["git", "rev-parse", "HEAD"], check=True,
                                    capture_output=True, text=True).stdout.strip(),
    "tests": sum(item["tests"] for item in results),
    "failures": sum(item["failures"] for item in results),
    "errors": sum(item["errors"] for item in results),
    "skipped": sum(item["skipped"] for item in results),
    "reports": results,
}
(report_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
PY

git diff --check
