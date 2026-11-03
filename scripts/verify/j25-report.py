#!/usr/bin/env python3
"""Validate J25 MiniCluster evidence without treating it as target acceptance."""
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET


def collect(root: Path, output: Path) -> None:
    source = root / "hbase/target/failsafe-reports/TEST-io.etcdhbase.hbase.storage.M2ReadViewIT.xml"
    core_source = root / "core/target/surefire-reports/TEST-io.etcdhbase.core.commit.ReadViewRecordStoreTest.xml"
    if not source.is_file():
        raise ValueError("J25 MiniCluster report is missing")
    if not core_source.is_file():
        raise ValueError("J25 core read-view report is missing")
    suite = ET.parse(source).getroot()
    counts = {name: int(suite.get(name, "0")) for name in ("tests", "failures", "errors", "skipped")}
    cases = {case.get("name") for case in suite.iter("testcase")}
    if counts != {"tests": 1, "failures": 0, "errors": 0, "skipped": 0}:
        raise ValueError("J25 MiniCluster test did not pass exactly once")
    if "readsSelectedPendingVersionsAndHistoricalRowsInBothLayouts" not in cases:
        raise ValueError("required J25 MiniCluster case did not execute")
    core_suite = ET.parse(core_source).getroot()
    core_counts = {
        name: int(core_suite.get(name, "0"))
        for name in ("tests", "failures", "errors", "skipped")
    }
    core_cases = {case.get("name") for case in core_suite.iter("testcase")}
    required_core_cases = {
        "currentReadOverlaysOnlyTheSelectedPendingTransactionBeforeRangeMerge",
        "historicalReadUsesItsOwnBarrierAndCanReadDeleteThenRecreateVersions",
        "selectedRootIsPinnedAndRecheckedBeforeAnyDataRead",
        "cancellingAReadKeepsItsPinUntilThePhysicalProducerStops",
    }
    if core_counts != {"tests": 4, "failures": 0, "errors": 0, "skipped": 0}:
        raise ValueError("J25 core read-view tests did not pass exactly once")
    if not required_core_cases.issubset(core_cases):
        raise ValueError("required J25 core read-view cases did not execute")
    stdout = "\n".join(element.text or "" for element in suite.iter("system-out"))
    for layout in ("HISTORY", "NATIVE"):
        if f"j25-readview layout={layout} selectedPending=true historical=true" not in stdout:
            raise ValueError(f"missing J25 selected/historical view evidence for {layout}")
        if "deleteVisible=true unselectedIsolated=true" not in stdout:
            raise ValueError(f"missing J25 deletion/isolation evidence for {layout}")
    shutil.copyfile(source, output / source.name)
    base_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()
    patch = subprocess.run(
        [
            "git", "diff", "--binary", "HEAD", "--",
            "core/src/main/java/io/etcdhbase/core/commit/ManifestCatalog.java",
            "core/src/main/java/io/etcdhbase/core/commit/PinnedReadView.java",
            "core/src/main/java/io/etcdhbase/core/commit/ReadView.java",
            "core/src/main/java/io/etcdhbase/core/commit/ReadViewPinRegistry.java",
            "core/src/main/java/io/etcdhbase/core/commit/ReadViewRecordStore.java",
            "core/src/main/java/io/etcdhbase/core/commit/ReadViewService.java",
            "core/src/main/java/io/etcdhbase/core/storage/StoragePage.java",
            "core/src/test/java/io/etcdhbase/core/commit/ReadViewRecordStoreTest.java",
            "hbase/pom.xml",
            "hbase/src/test/java/io/etcdhbase/hbase/storage/M2ReadViewIT.java",
            "scripts/verify/j25-report.py",
            "scripts/verify/j25.sh",
        ],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    summary = {
        "phase": "J25",
        "scope": "public-local-minicluster",
        "base_commit": base_commit,
        "implementation_patch_sha256": hashlib.sha256(patch).hexdigest(),
        "ended_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        **counts,
        "core_tests": core_counts["tests"],
        "layouts": ["HISTORY", "NATIVE"],
        "verified": [
            "selected pending update, delete, insert, and recreate overlay in core tests",
            "historical point reads and Range key enumeration use the captured revision",
            "selected candidate metadata and value remain consistent",
            "same-key unselected private candidates remain invisible",
            "Range head/value and point/range Exists use the same read view",
            "read cancellation retains the pin until physical producer completion",
        ],
        "minicluster_verified": [
            "HISTORY and NATIVE physical layouts",
            "selected update, delete, insert, and recreate visibility",
            "historical point reads and Range key enumeration",
            "same-key unselected private candidate isolation",
            "metadata and value version consistency",
        ],
        "unverified": [
            "cross-replica durable pin and GC competition from J26",
            "final history-layout selection with J52.1",
            "target HBase and cross-replica failure recovery",
            "maintainer review",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({
        key: summary[key]
        for key in ("phase", "scope", "base_commit", "tests", "core_tests")
    }))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError, IndexError):
        raise SystemExit("J25 verification evidence rejected") from None
