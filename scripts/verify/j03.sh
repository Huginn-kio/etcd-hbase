#!/usr/bin/env bash
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
ARTIFACT_ROOT="$REPO_ROOT/.artifacts/m0/j03/$RUN_ID"
mkdir -p "$ARTIFACT_ROOT"

GO_ROOT="$("$REPO_ROOT/scripts/dev/go-j03.sh" env GOROOT)"
UNFORMATTED="$("$GO_ROOT/bin/gofmt" -l "$REPO_ROOT/testkit/j03-go/main.go")"
if [[ -n "$UNFORMATTED" ]]; then
  printf 'Go source needs gofmt: %s\n' "$UNFORMATTED" >&2
  exit 1
fi
"$REPO_ROOT/scripts/dev/go-j03.sh" mod verify
"$REPO_ROOT/scripts/dev/go-j03.sh" list -m all > "$ARTIFACT_ROOT/go-modules.txt"
python3 - "$REPO_ROOT/build/dependency-lock.json" "$ARTIFACT_ROOT/go-modules.txt" <<'PY'
import json
import sys
from pathlib import Path

lock = json.loads(Path(sys.argv[1]).read_text())["go"]["moduleVersions"]
modules = {}
for line in Path(sys.argv[2]).read_text().splitlines():
    fields = line.split()
    if len(fields) == 1:
        continue
    if len(fields) != 2:
        raise SystemExit("J03 smoke does not allow undeclared module replacements")
    modules[fields[0]] = fields[1]
for module in ("go.etcd.io/etcd/api/v3", "go.etcd.io/etcd/client/pkg/v3",
               "go.etcd.io/etcd/client/v3", "google.golang.org/grpc"):
    if modules.get(module) != lock.get(module):
        raise SystemExit("J03 Go module version differs from dependency-lock.json: " + module)
print("J03 Go module pins verified")
PY
"$REPO_ROOT/scripts/dev/go-j03.sh" vet .
python3 "$REPO_ROOT/scripts/verify/vendor-proto.py" --lock "$REPO_ROOT/protocol/upstream-lock.json" --root "$REPO_ROOT/protocol/src/main/proto"
"$REPO_ROOT/scripts/dev/mvn21.sh" -B -ntp -pl server -am clean verify -Pj03-wire

python3 - "$REPO_ROOT" "$ARTIFACT_ROOT" <<'PY'
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

root, artifacts = map(Path, sys.argv[1:])
totals = dict.fromkeys(("tests", "failures", "errors", "skipped"), 0)
suites = []
for module in ("protocol", "core", "grpc", "server"):
    for kind in ("surefire-reports", "failsafe-reports"):
        for report in sorted((root / module / "target" / kind).glob("TEST-*.xml")):
            suite = ET.parse(report).getroot()
            counts = {key: int(suite.attrib.get(key, "0")) for key in totals}
            suites.append({"file": str(report.relative_to(root)), **counts})
            for key, value in counts.items():
                totals[key] += value
interop = root / "server/target/failsafe-reports/TEST-io.etcdhbase.server.GoInteropIT.xml"
if not interop.is_file():
    raise SystemExit("J03 Go/Java integration report is missing")
suite = ET.parse(interop).getroot()
if int(suite.attrib.get("tests", "0")) != 1:
    raise SystemExit("J03 GoInteropIT must execute its transport integration test")
if not totals["tests"] or any(totals[key] for key in ("failures", "errors", "skipped")):
    raise SystemExit("J03 tests failed, were skipped, or did not run")
summary = {
    "task": "J03",
    "scope": "protocol-skeleton-and-transport-only",
    "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    "tracked_changes": bool(subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True).strip()),
    "state": "passed",
    "tests": totals,
    "suites": suites,
    "target_hbase": "not_part_of_j03",
}
(artifacts / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print("J03 verification passed: " + json.dumps(totals))
print("Evidence: " + str(artifacts / "summary.json"))
PY
