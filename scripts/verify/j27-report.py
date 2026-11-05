#!/usr/bin/env python3
"""Reject incomplete J27 recovery and durable MiniCluster evidence."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

from evidence import java_reports

RECOVERY_TEST = "io.etcdhbase.core.commit.RecoveryManagerTest"
FINALIZATION_TEST = "io.etcdhbase.core.commit.CommitFinalizationServiceTest"
RECOVERY_IT = "io.etcdhbase.hbase.storage.M2RecoveryIT"
REQUIRED_CASES = {
    RECOVERY_TEST: {
        "takesOverSelectedSlotAndRepeatedStartupDoesNotAllocateAnotherRevision",
        "recoveryFinalizesSelectedWorkAfterCallerResultWindowCloses",
        "recoveryResumesAfterTheDecisionArchiveWasPersisted",
        "sealedButUnpublishedWorkStaysPrivateAfterOldOwnerIsFenced",
        "doesNotPreemptAnotherLiveOwnerOrScanInvocations",
        "rejectsProfileMismatchBeforeClaimingAnOwnerEpoch",
        "missingSelectedInvocationFailsClosedAfterOwnerFence",
    },
    FINALIZATION_TEST: {
        "archivesAndFinalizesSelectedMetadataOnlyCommitIdempotently",
        "materializesNonEmptySealedPlanBeforeAdvancingFinalizedSequence",
        "failedMaterializationDoesNotAdvanceFinalizedSequence",
        "conflictingDecisionArchiveCannotAdvanceFinalizedSequence",
    },
    RECOVERY_IT: {"freshManagerArchivesAndFinalizesSelectedMetadataOnlyPublication"},
}


def module_counts(directory: Path) -> dict[str, int]:
    totals = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
    reports = sorted(directory.glob("TEST-*.xml"))
    if not reports:
        raise ValueError(f"missing test reports: {directory}")
    for report in reports:
        for key in totals:
            totals[key] += int(ET.parse(report).getroot().get(key, "0"))
    if any(totals[key] != 0 for key in ("failures", "errors", "skipped")):
        raise ValueError(f"module tests did not pass cleanly: {directory}")
    return totals


def collect(root: Path, output: Path) -> None:
    core_dir = root / "core/target/surefire-reports"
    hbase_dir = root / "hbase/target/surefire-reports"
    failsafe = root / "hbase/target/failsafe-reports"
    for directory, suite in ((core_dir, RECOVERY_TEST), (core_dir, FINALIZATION_TEST),
                             (failsafe, RECOVERY_IT)):
        if not (directory / f"TEST-{suite}.xml").is_file():
            raise ValueError(f"required Java suite is in the wrong location: {suite}")
    _, suites = java_reports(root, set(REQUIRED_CASES), modules=("core", "hbase"),
                             required_cases=REQUIRED_CASES)
    recovery = suites[RECOVERY_TEST]
    finalization = suites[FINALIZATION_TEST]
    integration = suites[RECOVERY_IT]
    stdout = "\n".join(
        element.text or ""
        for element in ET.parse(failsafe / "TEST-io.etcdhbase.hbase.storage.M2RecoveryIT.xml").getroot().iter("system-out")
    )
    marker = "j27-recovery durableSelectedSlot=true archiveAndFinalize=true revisionStable=true"
    if marker not in stdout:
        raise ValueError("J27 MiniCluster evidence marker is missing")

    files = [
        "core/src/main/java/io/etcdhbase/core/commit/CommitOutcomeResolver.java",
        "core/src/main/java/io/etcdhbase/core/commit/CommitFinalizationService.java",
        "core/src/main/java/io/etcdhbase/core/commit/RecoveryManager.java",
        "core/src/test/java/io/etcdhbase/core/commit/RecoveryManagerTest.java",
        "hbase/pom.xml",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2RecoveryIT.java",
        "scripts/verify/j27-report.py",
        "scripts/verify/test_j27_report.py",
        "scripts/verify/j27.sh",
    ]
    patch = subprocess.run(
        ["git", "diff", "--binary", "HEAD", "--", *files], cwd=root,
        check=True, capture_output=True,
    ).stdout
    summary = {
        "phase": "J27",
        "scope": "public-core-and-local-minicluster",
        "base_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
        ).stdout.strip(),
        "implementation_patch_sha256": hashlib.sha256(patch).hexdigest(),
        "ended_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "module_tests": {
            "core": module_counts(core_dir),
            "hbase": module_counts(hbase_dir),
        },
        "recovery_manager_tests": recovery,
        "finalization_tests": finalization,
        "minicluster_recovery": integration,
        "verified": [
            "profile mismatch is rejected before owner claim",
            "active owner is not preempted and startup does not enumerate invocation records",
            "expired owner is fenced before a selected slot is resumed",
            "missing selected invocation fails closed without advancing finalized sequence",
            "caller result-window closure does not block internal finalization of a selected slot",
            "retry after recovery failure preserves the slot and does not allocate a new revision",
            "a new manager reads the selected publication, archives its decision, and advances finalized sequence in MiniCluster",
        ],
        "unverified": [
            "actual process termination, multi-host takeover, and HDFS/WAL node-failure persistence",
            "recovery identity activation, which requires the target identity contract",
            "cleanup of orphaned unpublished private pages; recovery intentionally does not scan all KV or invocation rows",
            "maintainer review and production target acceptance",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({
        "phase": summary["phase"],
        "scope": summary["scope"],
        "module_tests": summary["module_tests"],
        "recovery_manager_tests": recovery,
        "minicluster_recovery": integration,
    }))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError, IndexError):
        raise SystemExit("J27 verification evidence rejected") from None
