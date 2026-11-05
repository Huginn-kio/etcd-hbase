#!/usr/bin/env python3
"""Validate J29 public MiniCluster cost evidence and retain the exact workload report."""
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET

from evidence import java_reports


SUITE = "io.etcdhbase.hbase.storage.M2CostBenchmarkIT"
WORKLOAD_CASE = "reportsCommitReadAndSimulatedRenewalCostsForBothLayouts"


def load_workload_report(root: Path) -> tuple[Path, dict[str, int], str]:
    path = root / "hbase/target/failsafe-reports" / f"TEST-{SUITE}.xml"
    if not path.is_file():
        raise ValueError(f"missing J29 MiniCluster report: {path.name}")
    _, suites = java_reports(root, {SUITE}, modules=("hbase",),
                             required_cases={SUITE: {WORKLOAD_CASE}},
                             report_kinds=("failsafe-reports",))
    suite = ET.parse(path).getroot()
    if suite.get("name") != SUITE:
        raise ValueError("J29 workload report has an unexpected suite name")
    case = next(case for case in suite.findall("testcase") if case.get("name") == WORKLOAD_CASE)
    stdout = "\n".join(node.text or "" for node in [*suite.findall("system-out"), *case.iter("system-out")])
    return path, suites[SUITE], stdout


def observations(stdout: str) -> dict[str, list[dict]]:
    found: dict[str, list[dict]] = {}
    for line in stdout.splitlines():
        if not line.startswith("j29-"):
            continue
        marker, payload = line.split(" ", 1)
        found.setdefault(marker, []).append(json.loads(payload))
    return found


def require_layout_rows(rows: list[dict], layouts: set[str], marker: str) -> dict[str, dict]:
    selected = {row.get("layout"): row for row in rows}
    if len(rows) != len(layouts) or set(selected) != layouts:
        raise ValueError(f"{marker} does not contain both HISTORY and NATIVE profiles")
    digests = {row.get("workloadSha256") for row in selected.values()}
    seeds = {row.get("seed") for row in selected.values()}
    if len(digests) != 1 or not next(iter(digests)) or seeds != {20261104}:
        raise ValueError(f"{marker} workload seed or digest differs across layouts")
    return selected


def source_hash(root: Path) -> str:
    sources = [
        "hbase/pom.xml",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostBenchmarkIT.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostFairQueue.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostMetrics.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostMetricsTest.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostPinObservations.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostReadProfile.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostRecoveryInterlock.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostRuntimeSampler.java",
        "scripts/verify/j29-report.py",
        "scripts/verify/test_j29_report.py",
        "scripts/verify/evidence.py",
        "scripts/verify/test_evidence.py",
        "scripts/verify/j29.sh",
        "docs/testing/j29-public-cost-report.md",
    ]
    digest = hashlib.sha256()
    for relative in sources:
        path = root / relative
        if not path.is_file():
            raise ValueError(f"missing J29 source or report: {relative}")
        digest.update(relative.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def validate_baseline(layout: str, baseline: dict) -> None:
    if {sample["kind"] for sample in baseline["samples"]} != {
        "SHORT_KV", "LARGE_KV", "SIMULATED_RENEWAL"
    }:
        raise ValueError(f"{layout} commit baseline is incomplete")


def invalid_read_pages(read: dict) -> bool:
    return (read.get("returnedKeys") != 8 or read.get("bucketCount") != 4
            or read.get("pageRequests") != 12 or read.get("pageReopens") != 8)


def invalid_recovery_overlap(read: dict) -> bool:
    return (not 8 <= read.get("pagesWhileRecoveryActive", 0) <= 12
            or read.get("recoveryMaterializationCalls") != 8
            or read.get("recoveryElapsedMicros", 0) <= 0
            or read.get("recoveryPending") is not True
            or read.get("partiallyMaterialized") is not True)


def invalid_pin_lifecycle(read: dict) -> bool:
    return (read.get("pinRegisterCalls") != 1 or read.get("pinRenewCalls", 0) < 1
            or read.get("pinCompleteCalls", 0) < 1 or read.get("peakPinOccupancy") != 1)


def validate_read(layout: str, read: dict) -> None:
    if invalid_read_pages(read) or invalid_recovery_overlap(read) or invalid_pin_lifecycle(read):
        raise ValueError(f"{layout} pending/recovery read profile is incomplete")


def validate_queue(layout: str, queue: dict) -> None:
    if queue.get("submitted") != 8 or len(queue.get("samples", [])) != 8 \
            or queue.get("classSwitches", 0) < 1 \
            or queue.get("maxConsecutiveBusiness") > 3 \
            or queue.get("maxConsecutiveRenewals") > 1:
        raise ValueError(f"{layout} bounded-fair queue profile is incomplete")


def validate_resources(layout: str, resource: dict) -> None:
    if resource.get("activePinsAfterRelease") != 0 or resource.get("peakActiveScanners") != 1 \
            or resource.get("pinCapacity") != 32 \
            or any(resource.get(key, -1) < 0 for key in (
                "sampledPeakHeapBytes", "sampledPeakDirectBufferBytes",
                "sampledPeakThreadCount", "peakLogicalCandidatePayloadBytes")):
        raise ValueError(f"{layout} resource and cleanup evidence is incomplete")


def validate_totals(layout: str, total: dict) -> None:
    metrics = total.get("metrics", {})
    if total.get("publishedSequence") != 12 or total.get("publishedRevision") != 10:
        raise ValueError(f"{layout} publication and metadata-only revision totals differ")
    required = {
        "control.cas", "system.CANDIDATE.create", "system.MANIFEST.create",
        "system.DECISION.create", "system.SNAPSHOT_PIN.cas",
        "materialize.preflight", "materialize.apply",
    }
    if not required.issubset(metrics):
        raise ValueError(f"{layout} protocol operation counters are incomplete")
    if any(metric["unknown"] or metric["failed"] for metric in metrics.values()):
        raise ValueError(f"{layout} observed a failed or unknown application operation")
    if metrics["control.cas"]["conflicts"] != 0:
        raise ValueError(f"{layout} serial profile unexpectedly had a control CAS conflict")


def validate_measurements(observed: dict[str, list[dict]]) -> dict[str, dict[str, dict]]:
    required = ("j29-commit-baseline", "j29-read", "j29-queue", "j29-layout", "j29-resource")
    if not set(required).issubset(observed):
        raise ValueError("one or more J29 measurements are missing")
    layouts = {"HISTORY", "NATIVE"}
    profiles = {marker: require_layout_rows(observed[marker], layouts, marker) for marker in required}
    workload_identities = {
        (row["seed"], row["workloadSha256"])
        for rows in profiles.values() for row in rows.values()
    }
    if len(workload_identities) != 1:
        raise ValueError("J29 measurements use different workload identities")
    environment_profiles = {
        (row.get("hbaseVersion"), row.get("java"), row.get("bucketCount"))
        for row in profiles["j29-layout"].values()
    }
    if len(environment_profiles) != 1 or next(iter(environment_profiles))[2] != 4:
        raise ValueError("layout profiles were not measured on the same HBase/JDK/bucket setup")
    for layout in layouts:
        validate_baseline(layout, profiles["j29-commit-baseline"][layout])
        validate_read(layout, profiles["j29-read"][layout])
        validate_queue(layout, profiles["j29-queue"][layout])
        validate_resources(layout, profiles["j29-resource"][layout])
        validate_totals(layout, profiles["j29-layout"][layout])
    return profiles


def build_summary(root: Path, counts: dict[str, int], profiles: dict[str, dict[str, dict]]) -> dict:
    return {
        "phase": "J29.1-J29.3",
        "scope": "public-local-minicluster",
        "ended_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source_and_workload_sha256": source_hash(root),
        "minicluster_test": counts,
        "profiles": {
            layout: {
                "commit_baseline": profiles["j29-commit-baseline"][layout],
                "pending_read": profiles["j29-read"][layout],
                "simulated_renewal_queue": profiles["j29-queue"][layout],
                "sampled_resources": profiles["j29-resource"][layout],
                "application_operation_totals": profiles["j29-layout"][layout],
            }
            for layout in sorted(profiles["j29-layout"])
        },
        "byte_accounting": (
            "Selected application payload lengths only, not complete request or row sizes. "
            "System-record identities and types, physical row keys, cell overhead and other "
            "uncounted fields are not separately included; see docs/testing/j29-public-cost-report.md "
            "for per-operation inputs and outputs."
        ),
        "verified": [
            "full M2 invocation, manifest preparation, control publication, archive, materialization and finalization path",
            "application-port operation counts, observed CAS conflicts/unknown results, and selected payload bytes",
            "durable pin open and selected-candidate point read while RecoveryManager finalizes a partial publication",
            "four-bucket one-row page reopen sequence deterministically overlapped with recovery materialization",
            "pin registration, renewals, completion, occupancy, scanner concurrency, and sampled JVM heap/direct/thread gauges",
            "single-active mixed KV and metadata-only queue with a test-only bounded-fair schedule",
            "identical frozen request profile across HISTORY and NATIVE layouts",
        ],
        "unverified": [
            "B04/B05 target workload and acceptance thresholds; no target p99, throughput or capacity claim",
            "actual HBase point RPC/retry counts, server scan counters, WAL/HFile IO, and compaction costs",
            "Lease TTL/expiry semantics; metadata-only commits are a proxy and J29.4 remains M4",
            "JVM/direct-memory high-water and multi-process failure/takeover behavior",
            "maintainer review and target HBase acceptance",
        ],
    }


def collect(root: Path, output: Path) -> None:
    test_report, counts, stdout = load_workload_report(root)
    profiles = validate_measurements(observations(stdout))
    summary = build_summary(root, counts, profiles)
    shutil.copyfile(test_report, output / test_report.name)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({
        "phase": summary["phase"],
        "scope": summary["scope"],
        "minicluster_test": counts,
        "layouts": sorted(profiles["j29-layout"]),
        "workloadSha256": profiles["j29-commit-baseline"]["HISTORY"]["workloadSha256"],
    }))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, OSError, KeyError, IndexError, ET.ParseError, json.JSONDecodeError):
        raise SystemExit("J29 verification evidence rejected") from None
