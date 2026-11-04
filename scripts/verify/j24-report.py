#!/usr/bin/env python3
"""Validate the J24 MiniCluster report without treating it as target acceptance."""
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from evidence import java_reports


REQUIRED_SUITE = "io.etcdhbase.hbase.storage.M2CommitMaterializationIT"
REQUIRED_CASE = "materializesHistoryAndNativeVersionsAndFencesLateWriters"
TESTED_PATHS = (
    "core/src/main/java/io/etcdhbase/core/commit/CommitFinalizationService.java",
    "core/src/test/java/io/etcdhbase/core/commit/CommitFinalizationServiceTest.java",
    "core/src/test/java/io/etcdhbase/core/commit/CommitPublisherTest.java",
    "hbase/src/main/java/io/etcdhbase/hbase/storage/HBaseMaterializationStore.java",
    "hbase/src/main/java/io/etcdhbase/hbase/storage/HBaseRequestCompletion.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/HBaseCommitControlStoreCompletionTest.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/HBaseMaterializationStoreTest.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CommitMaterializationIT.java",
    "scripts/verify/j24-report.py",
    "scripts/verify/test_j24_report.py",
)


def inspect_report(root: Path):
    source = root / f"hbase/target/failsafe-reports/TEST-{REQUIRED_SUITE}.xml"
    if not source.is_file():
        raise ValueError("J24 MiniCluster report is missing")
    _, suites = java_reports(root, {REQUIRED_SUITE}, ("hbase",))
    suite = ET.parse(source).getroot()
    counts = suites[REQUIRED_SUITE]
    cases = {case.get("name") for case in suite.findall("testcase")}
    if counts != {"tests": 1, "failures": 0, "errors": 0, "skipped": 0}:
        raise ValueError("J24 MiniCluster test did not pass exactly once")
    if cases != {REQUIRED_CASE}:
        raise ValueError("required J24 MiniCluster case did not execute")
    stdout = "\n".join(element.text or "" for element in suite.iter("system-out"))
    for layout in ("HISTORY", "NATIVE"):
        if f"j24-materialization layout={layout} idempotent=true lateWriterFenced=true" not in stdout:
            raise ValueError(f"missing J24 evidence for {layout}")
    return source, counts


def collect(root: Path, output: Path) -> None:
    source, counts = inspect_report(root)
    shutil.copyfile(source, output / source.name)
    base_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()
    patch = subprocess.run(
        ["git", "diff", "--binary", "HEAD", "--", *TESTED_PATHS],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    summary = {
        "phase": "J24",
        "scope": "public-local-minicluster",
        "base_commit": base_commit,
        "tested_patch_sha256": hashlib.sha256(patch).hexdigest(),
        "source_report_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "ended_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        **counts,
        "layouts": ["HISTORY", "NATIVE"],
        "verified": [
            "idempotent replay",
            "late-writer fencing",
            "delete and recreate",
            "history reads and current value",
            "native per-version fence timestamps",
        ],
        "unverified": [
            "J52.1 retention and recovery comparison",
            "explicit compaction comparison between the HISTORY and NATIVE layouts",
            "final M2 history-layout selection with J25",
            "target HBase and cross-replica failure recovery",
            "maintainer review",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError, IndexError):
        raise SystemExit("J24 verification evidence rejected") from None
