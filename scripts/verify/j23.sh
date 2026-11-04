#!/usr/bin/env bash
set -euo pipefail
umask 077
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
REPORT="$ROOT/.artifacts/j23/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$REPORT"

python3 scripts/verify/test_evidence.py

scripts/dev/mvn21.sh -B -ntp -pl core,server,hbase -am clean verify -Pj23-review-local \
  > "$REPORT/maven.log" 2>&1 || {
  tail -n 80 "$REPORT/maven.log" >&2
  exit 1
}

python3 - "$REPORT" <<'PY'
import json
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path("scripts/verify").resolve()))
from evidence import java_reports

root = Path.cwd()
report_dir = Path(sys.argv[1])
required = {
    "io.etcdhbase.core.commit.CommitPublisherTest",
    "io.etcdhbase.core.commit.CommitOutcomeResolverTest",
    "io.etcdhbase.server.commit.BoundedPublicationHandoffTest",
    "io.etcdhbase.hbase.codec.CommitDecisionMessagesTest",
    "io.etcdhbase.hbase.storage.J23CommitDecisionRepositoryIT",
}
_, suites = java_reports(root, required, ("core", "server", "hbase"))
results = [{"suite": name, **suites[name]} for name in sorted(required)]

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
