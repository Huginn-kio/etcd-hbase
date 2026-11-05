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
from evidence import java_reports


J25_SUITE = "io.etcdhbase.hbase.storage.M2ReadViewIT"
J25_CASE = "readsSelectedPendingVersionsAndHistoricalRowsInBothLayouts"
CORE_SUITE = "io.etcdhbase.core.commit.ReadViewRecordStoreTest"
REQUIRED_CORE_CASES = {
    "currentReadOverlaysOnlyTheSelectedPendingTransactionBeforeRangeMerge",
    "historicalReadUsesItsOwnBarrierAndCanReadDeleteThenRecreateVersions",
    "selectedRootIsPinnedAndRecheckedBeforeAnyDataRead",
    "cancellingAReadKeepsItsPinUntilThePhysicalProducerStops",
    "byteTruncatedPhysicalPageDoesNotSkipKeysDuringOverlayMerge",
    "byteTruncatedCandidatePageDoesNotSkipKeysDuringOverlayMerge",
    "cancellingReadResultDoesNotCancelPinAccounting",
    "scopeCancellationWaitsForManifestProducerBeforeReleasingRegisteredPin",
    "metadataHeadBudgetDoesNotRejectLargeCandidatePayload",
    "requestReadLimitStopsBeforeFetchingCandidateBeyondRemainingBytes",
    "candidateSliceResumesAfterBoundedSlice",
    "candidateMemoryRejectionOccursBeforeCandidateRead",
    "candidateValueMemoryIsReservedBeforeReadAndReleasedAfterProjection",
    "cancelledCandidateReadRetainsLeaseUntilProducerStops",
}


def collect(root: Path, output: Path) -> None:
    source = root / f"hbase/target/failsafe-reports/TEST-{J25_SUITE}.xml"
    if not source.is_file():
        raise ValueError("J25 MiniCluster report is missing")
    _, suites = java_reports(root, {J25_SUITE}, ("hbase",), {J25_SUITE: {J25_CASE}})
    _, core_suites = java_reports(root, {CORE_SUITE}, ("core",),
                                  {CORE_SUITE: REQUIRED_CORE_CASES})
    counts = suites[J25_SUITE]
    core_counts = core_suites[CORE_SUITE]
    if counts != {"tests": 1, "failures": 0, "errors": 0, "skipped": 0}:
        raise ValueError("J25 MiniCluster test did not pass exactly once")
    # Required case names and zero-failure suite counters are checked by java_reports;
    # report the actual count without rejecting later passing core regressions.
    suite = ET.parse(source).getroot()
    stdout = "\n".join(element.text or "" for element in suite.iter("system-out"))
    for layout in ("HISTORY", "NATIVE"):
        if f"j25-readview layout={layout} selectedPending=true historical=true" not in stdout:
            raise ValueError(f"missing J25 selected/historical view evidence for {layout}")
        if "deleteVisible=true unselectedIsolated=true" not in stdout:
            raise ValueError(f"missing J25 deletion/isolation evidence for {layout}")
    shutil.copyfile(source, output / source.name)
    source_commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()
    patch = subprocess.run(
        ["git", "diff", "--binary", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    summary = {
        "phase": "J25",
        "scope": "public-local-minicluster",
        "source_commit": source_commit,
        "tested_patch_sha256": hashlib.sha256(patch).hexdigest(),
        "source_report_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
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
            "request candidate-byte limits reject reads before fetching values beyond remaining allowance",
            "candidate buffers reserve request/group/global memory before reads and retain charges through cancelled physical producers",
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
        for key in ("phase", "scope", "source_commit", "tested_patch_sha256", "tests", "core_tests")
    }))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError, IndexError):
        raise SystemExit("J25 verification evidence rejected") from None
