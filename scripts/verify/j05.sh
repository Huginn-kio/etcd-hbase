#!/usr/bin/env bash
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
if [[ "$#" -ne 2 || "$1" != --scope ]]; then
  printf '%s\n' 'Usage: scripts/verify/j05.sh --scope public|target' >&2
  exit 2
fi
if [[ "$2" == target ]]; then
  printf '%s\n' 'missing-input: B01 target salt and approved target identity/configuration are not yet verified' >&2
  exit 3
fi
if [[ "$2" != public ]]; then
  printf '%s\n' 'Expected --scope public or --scope target' >&2
  exit 2
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
ARTIFACT_ROOT="$REPO_ROOT/.artifacts/m0/j05/$RUN_ID"
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
python3 - "$REPO_ROOT" <<'PY'
import re
import sys
from pathlib import Path
root = Path(sys.argv[1])
for module in ("spi", "core", "hbase", "admin"):
    for source in (root / module / "src").rglob("*.java"):
        for line_number, line in enumerate(source.read_text().splitlines(), 1):
            if re.match(r"\s*import\s+(?:static\s+)?[\w.]+\.\*;", line):
                raise SystemExit(f"wildcard Java import: {source.relative_to(root)}:{line_number}")
print("J05 Java import checks passed")
PY
python3 "$REPO_ROOT/scripts/verify/salt-profile.py"
"$REPO_ROOT/scripts/dev/mvn21.sh" -B -ntp -pl admin -am clean verify -Pj05-hbase-local

python3 - "$REPO_ROOT" "$ARTIFACT_ROOT" <<'PY'
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

def verify_write_counts(counts, size, total, active):
    if (len(counts) != size or any(type(count) is not int or count < 0 for count in counts)
            or sum(counts) != total or sum(count > 0 for count in counts) != active):
        raise SystemExit("invalid actual region/server write distribution")


def verify_salt_distribution(report):
    suite = ET.parse(report).getroot()
    cases = {case.attrib["name"] for case in suite.iter("testcase")}
    if not {"saltingSpreadsSequentialAndCommonPrefixWritesInEveryBatch",
            "repeatedUpdatesToOneLogicalKeyStayOnOneRegionAndServer"} <= cases:
        raise SystemExit("both real multi-server salt distribution and hot-key cases must execute")
    output = "\n".join(item.text or "" for item in suite.iter("system-out"))
    pattern = (r"j05-salt workload=(SEQUENTIAL|SHARED_DIRECTORY) rows=(\d+) "
               r"batch_size=(\d+) batches=(\d+) salted_region_writes=(\[[^\n]*?\]) "
               r"salted_server_writes=(\[[^\n]*?\]) unsalted_region_writes=(\[[^\n]*?\]) "
               r"unsalted_server_writes=(\[[^\n]*?\]) per_batch=true readback=true status=passed")
    workloads = {}
    for match in re.finditer(pattern, output):
        workload, rows, batch, batches, *encoded = match.groups()
        if workload in workloads or (int(rows), int(batch), int(batches)) != (2048, 128, 16):
            raise SystemExit("salt distribution workloads must use the fixed bounded dataset")
        salted_regions, salted_servers, control_regions, control_servers = map(json.loads, encoded)
        for counts, size, active in ((salted_regions, 16, 16), (salted_servers, 3, 3),
                                     (control_regions, 16, 1), (control_servers, 3, 1)):
            verify_write_counts(counts, size, int(rows), active)
        if min(salted_regions) < 64 or max(salted_regions) > 256:
            raise SystemExit("the fixed workload has concentrated region writes")
        workloads[workload] = {
            "rows": int(rows), "batch_size": int(batch), "batches": int(batches),
            "salted_region_writes": salted_regions, "salted_server_writes": salted_servers,
            "unsalted_region_writes": control_regions, "unsalted_server_writes": control_servers,
            "per_batch_distribution": True, "readback": True,
        }
    if set(workloads) != {"SEQUENTIAL", "SHARED_DIRECTORY"}:
        raise SystemExit("both actual sequential and shared-directory salt workloads are required")
    if ("j05-salt workload=SINGLE_HOT_KEY updates=128 active_regions=1 active_servers=1 "
            "readback=true status=passed") not in output:
        raise SystemExit("single-key updates must retain their actual one-region hotspot")
    return {"workloads": workloads, "single_hot_key_updates": 128,
            "scope": "disposable_multi_server_routing_only"}


root, artifacts = map(Path, sys.argv[1:])
totals = dict.fromkeys(("tests", "failures", "errors", "skipped"), 0)
suites = []
server_logs = []
for module in ("spi", "core", "hbase", "admin"):
    reports = sorted((root / module / "target/surefire-reports").glob("TEST-*.xml"))
    if module in ("hbase", "admin"):
        reports += sorted((root / module / "target/failsafe-reports").glob("TEST-*.xml"))
    if not reports:
        raise SystemExit("missing J05 reactor reports for " + module)
    for report in reports:
        suite = ET.parse(report).getroot()
        counts = {key: int(suite.attrib.get(key, "0")) for key in totals}
        if counts["tests"] == 0 or any(counts[key] for key in ("failures", "errors", "skipped")):
            raise SystemExit("J05 suite did not pass without skips: " + suite.attrib.get("name", ""))
        retained = artifacts / report.relative_to(root)
        retained.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(report, retained)
        suites.append({"file": str(report.relative_to(root)),
                       "sha256": hashlib.sha256(report.read_bytes()).hexdigest(), **counts})
        for key, count in counts.items():
            totals[key] += count
    for log in sorted((root / module / "target/failsafe-reports").glob("m0-hbase-cluster-*.log")):
        retained = artifacts / log.relative_to(root)
        retained.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(log, retained)
        server_logs.append({"file": str(log.relative_to(root)),
                            "sha256": hashlib.sha256(log.read_bytes()).hexdigest()})

required_suites = {"HashSaltStrategyTest", "InitializationConfigurationLoaderTest", "TableGroupPlanFactoryTest",
                   "TableGroupInitializerTest", "HBaseOperationAwaiterTest", "InitializeTablesCommandTest",
                   "InitializeTablesIT", "SaltDistributionIT", "SecureHBaseAuthenticationIT", "LocalHBaseProbeIT"}
observed_suites = {Path(suite["file"]).stem.rsplit(".", 1)[-1] for suite in suites}
if not required_suites <= observed_suites:
    raise SystemExit("missing required salt, initialization or adjacent J04 suites")
integration = ET.parse(root / "admin/target/failsafe-reports/TEST-io.etcdhbase.admin.InitializeTablesIT.xml").getroot()
required_cases = {
    "actualCliCreatesBothLayoutsAndRepeatsWithoutChangingPersistedData",
    "realPartialCreationResumesAndConfiguredSiblingsKeepTheirOwnDataAndDescriptors",
    "deletedRecordsDoNotMakeAPartialGroupPristineAgain",
    "suppliedKerberosIdentityProvisionsSchemasAndAnUnprivilegedIdentityIsRejected",
}
if not required_cases <= {case.attrib["name"] for case in integration.iter("testcase")}:
    raise SystemExit("the complete J05 real HBase scenario set must execute")
output = "\n".join(item.text or "" for item in integration.iter("system-out"))
layouts = dict(re.findall(r"j05-real layout=(\S+) tables=(\d+) regions_per_table=16 repeated=true persisted=true status=passed", output))
if layouts != {"SEPARATE_HISTORY_TABLE": "3", "NATIVE_CELL_VERSIONS": "2"}:
    raise SystemExit("both actual HBase layout/region/persistence checks are required")
required_markers = {
    "j05-real recovery=partial-resume siblings-preserved=true status=passed",
    "j05-real recovery=deleted-records-refused raw-scan=true status=passed",
    "j05-real authentication=kerberos-keytab acl-enforced=true caller-owns-connection=true status=passed",
}
if any(marker not in output for marker in required_markers):
    raise SystemExit("J05 recovery, retained-data or secure identity evidence is missing")
distribution = verify_salt_distribution(
    root / "admin/target/failsafe-reports/TEST-io.etcdhbase.admin.SaltDistributionIT.xml")
if len(server_logs) != 5:
    raise SystemExit("J04/J05 must retain all five logs, including the multi-server salt cluster")
observed = re.search(r"j05-hbase server=(\S+) client.jvm=(\S+)", output)
if observed is None or not observed[2].startswith("21."):
    raise SystemExit("missing observed HBase/Java 21 versions")

classpath_file = root / "admin/target/j05-runtime-classpath.txt"
dependencies = classpath_file.read_text().strip()
entries = dependencies.split(os.pathsep)
if any("/target/classes" in entry or "/target/test-classes" in entry
       or entry.endswith("-tests.jar") or "protobuf-java-2.5.0.jar" in entry for entry in entries):
    raise SystemExit("packaged admin runtime contains development or test-server artifacts")
for module in ("core", "spi", "hbase"):
    if not any(Path(entry).name == module + "-0.1.0-SNAPSHOT.jar" for entry in entries):
        raise SystemExit("admin runtime must use packaged reactor dependencies")
java = Path(os.environ["ETCD_HBASE_JDK21_HOME"]) / "bin/java"
jar = root / "admin/target/admin-0.1.0-SNAPSHOT.jar"
cli_plans = []
for candidate in ("history-table.json", "native-cell-versions.json"):
    invocation = [str(java), "-cp", str(jar) + os.pathsep + dependencies,
                  "io.etcdhbase.admin.InitializeTablesCommand", "init", "plan", "--config",
                  str(root / "config/examples/j05" / candidate)]
    result = subprocess.run(invocation, capture_output=True, text=True, timeout=30, check=True)
    if "backend_contacted: false" not in result.stdout or "credential_reference" in result.stdout:
        raise SystemExit("offline packaged CLI did not produce the required safe plan")
    target = artifacts / (candidate + ".plan.txt")
    target.write_text(result.stdout)
    cli_plans.append({"candidate": candidate, "sha256": hashlib.sha256(target.read_bytes()).hexdigest()})

summary = {
    "task": "J05", "scope": "public-local", "state": "passed", "tests": totals,
    "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    "tracked_changes": bool(subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True).strip()),
    "suites": suites, "server_logs": server_logs, "layouts": layouts,
    "hbase_server": observed[1], "client_jvm": observed[2], "cli_plans": cli_plans,
    "salt_vectors": 8, "salt_split_keys": 15,
    "salt_distribution": distribution,
    "target_salt": "not_verified", "target_identity": "not_verified",
    "history_layout_decision": "prototype_candidates_only",
    "permission_intents": "not_acl_grants", "runtime_bootstrap": "not_implemented",
}
(artifacts / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print("J05 public verification passed: " + json.dumps(totals))
print("Evidence: " + str(artifacts / "summary.json"))
PY
