#!/usr/bin/env python3
"""Reject incomplete J28.1 bounded kernel history evidence."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from evidence import java_reports

MODEL = "io.etcdhbase.core.commit.KernelHistoryModelTest"
RECOVERY = "io.etcdhbase.core.commit.RecoveryManagerTest"
READ_VIEW = "io.etcdhbase.core.commit.ReadViewRecordStoreTest"
REQUIRED_CASES = {
    MODEL: {
        "selectedUnknownTransactionIsVisibleAcrossCancellationAndRecovery",
        "unresolvedUnknownMayHaveTakenEffectOrMayNotHaveTakenEffect",
    },
    RECOVERY: {
        "takesOverSelectedSlotAndRepeatedStartupDoesNotAllocateAnotherRevision",
        "recoveryFinalizesSelectedWorkAfterCallerResultWindowCloses",
        "recoveryResumesAfterTheDecisionArchiveWasPersisted",
        "sealedButUnpublishedWorkStaysPrivateAfterOldOwnerIsFenced",
        "doesNotPreemptAnotherLiveOwnerOrScanInvocations",
        "rejectsProfileMismatchBeforeClaimingAnOwnerEpoch",
        "missingSelectedInvocationFailsClosedAfterOwnerFence",
    },
    READ_VIEW: {
        "currentReadOverlaysOnlyTheSelectedPendingTransactionBeforeRangeMerge",
        "historicalReadUsesItsOwnBarrierAndCanReadDeleteThenRecreateVersions",
        "selectedRootIsPinnedAndRecheckedBeforeAnyDataRead",
        "cancellingAReadKeepsItsPinUntilThePhysicalProducerStops",
    },
}


def collect(root: Path, output: Path) -> None:
    reports = root / "core/target/surefire-reports"
    for name in REQUIRED_CASES:
        if not (reports / f"TEST-{name}.xml").is_file():
            raise ValueError(f"required core unit suite is missing: {name}")
    _, suites = java_reports(root, set(REQUIRED_CASES), modules=("core",),
                             required_cases=REQUIRED_CASES, report_kinds=("surefire-reports",))
    model = suites[MODEL]
    recovery = suites[RECOVERY]
    read_view = suites[READ_VIEW]
    core_tests = {key: sum(counts[key] for counts in suites.values())
                  for key in ("tests", "failures", "errors", "skipped")}
    tracked = [
        "core/src/test/java/io/etcdhbase/core/commit/KernelHistoryModelTest.java",
        "scripts/verify/j28-report.py",
        "scripts/verify/test_j28_report.py",
        "scripts/verify/evidence.py",
        "scripts/verify/test_evidence.py",
        "scripts/verify/j28.sh",
        "docs/testing/j28-kernel-history.md",
    ]
    digest = hashlib.sha256()
    for relative in tracked:
        path = root / relative
        if not path.is_file():
            raise ValueError(f"missing J28.1 source or documentation: {relative}")
        digest.update(relative.encode("utf-8"))
        digest.update(path.read_bytes())
    summary = {
        "phase": "J28.1",
        "scope": "bounded-core-history-model",
        "base_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True,
            capture_output=True, text=True,
        ).stdout.strip(),
        "implementation_sha256": digest.hexdigest(),
        "ended_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "core_tests": core_tests,
        "history_model": model,
        "recovery_regression": recovery,
        "read_view_regression": read_view,
        "verified": [
            "an independent bounded serial model accepts real-time-compatible Compare/Put/Delete/Read histories",
            "controlled stores drive the production publication, read-view, and recovery components",
            "selected publication decision time constrains overlapping reads even when the caller result is unknown",
            "caller cancellation after selection leaves the selected candidate observable and replay finalizes once",
            "an unresolved unknown result is explored as either applied or not applied, subject to later reads",
            "the stale compare fails after the selected write, and delete is reflected by a later read",
            "J25 read-view and J27 RecoveryManager core regressions pass in the same run",
        ],
        "unverified": [
            "end-to-end business transactions, production handoff, HBase RPC network ambiguity, and Go client behavior; J28.2 is M3",
            "cross-process crash, HDFS/WAL persistence, and production target acceptance",
            "maintainer review",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({
        "phase": summary["phase"],
        "scope": summary["scope"],
        "core_tests": summary["core_tests"],
        "history_model": model,
        "recovery_regression": recovery,
        "read_view_regression": read_view,
    }))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, OSError, KeyError, IndexError):
        raise SystemExit("J28.1 verification evidence rejected") from None
