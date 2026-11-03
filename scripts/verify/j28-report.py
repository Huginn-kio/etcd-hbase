#!/usr/bin/env python3
"""Reject incomplete J28.1 bounded kernel history evidence."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET


def read_suite(path: Path, required_cases: set[str]) -> dict[str, int]:
    if not path.is_file():
        raise ValueError(f"missing test report: {path.name}")
    suite = ET.parse(path).getroot()
    counts = {
        key: int(suite.get(key, "0"))
        for key in ("tests", "failures", "errors", "skipped")
    }
    names = {case.get("name") for case in suite.iter("testcase")}
    if counts["tests"] != len(required_cases) or any(
        counts[key] != 0 for key in ("failures", "errors", "skipped")
    ):
        raise ValueError(f"J28.1 suite did not pass exactly: {path.name}")
    if not required_cases.issubset(names):
        raise ValueError(f"required history checks are missing: {path.name}")
    return counts


def core_totals(directory: Path) -> dict[str, int]:
    totals = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
    reports = sorted(directory.glob("TEST-*.xml"))
    if not reports:
        raise ValueError("core test reports are missing")
    for path in reports:
        suite = ET.parse(path).getroot()
        for key in totals:
            totals[key] += int(suite.get(key, "0"))
    if any(totals[key] for key in ("failures", "errors", "skipped")):
        raise ValueError("core tests contain failures, errors, or skipped cases")
    return totals


def collect(root: Path, output: Path) -> None:
    reports = root / "core/target/surefire-reports"
    model = read_suite(reports / "TEST-io.etcdhbase.core.commit.KernelHistoryModelTest.xml", {
        "selectedUnknownTransactionIsVisibleAcrossCancellationAndRecovery",
        "unresolvedUnknownMayHaveTakenEffectOrMayNotHaveTakenEffect",
    })
    recovery = read_suite(reports / "TEST-io.etcdhbase.core.commit.RecoveryManagerTest.xml", {
        "takesOverSelectedSlotAndRepeatedStartupDoesNotAllocateAnotherRevision",
        "recoveryFinalizesSelectedWorkAfterCallerResultWindowCloses",
        "recoveryResumesAfterTheDecisionArchiveWasPersisted",
        "sealedButUnpublishedWorkStaysPrivateAfterOldOwnerIsFenced",
        "doesNotPreemptAnotherLiveOwnerOrScanInvocations",
        "rejectsProfileMismatchBeforeClaimingAnOwnerEpoch",
        "missingSelectedInvocationFailsClosedAfterOwnerFence",
    })
    read_view = read_suite(reports / "TEST-io.etcdhbase.core.commit.ReadViewRecordStoreTest.xml", {
        "currentReadOverlaysOnlyTheSelectedPendingTransactionBeforeRangeMerge",
        "historicalReadUsesItsOwnBarrierAndCanReadDeleteThenRecreateVersions",
        "selectedRootIsPinnedAndRecheckedBeforeAnyDataRead",
        "cancellingAReadKeepsItsPinUntilThePhysicalProducerStops",
    })
    tracked = [
        "core/src/test/java/io/etcdhbase/core/commit/KernelHistoryModelTest.java",
        "scripts/verify/j28-report.py",
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
        "core_tests": core_totals(reports),
        "history_model": model,
        "recovery_regression": recovery,
        "read_view_regression": read_view,
        "verified": [
            "an independent bounded serial model accepts real-time-compatible Compare/Put/Delete/Read histories",
            "selected publication decision time constrains overlapping reads even when the caller result is unknown",
            "caller cancellation after selection leaves the selected candidate observable and replay finalizes once",
            "an unresolved unknown result is explored as either applied or not applied, subject to later reads",
            "the stale compare fails after the selected write, and delete is reflected by a later read",
            "J25 read-view and J27 RecoveryManager core regressions pass in the same run",
        ],
        "unverified": [
            "real CommitPublisher/HBase RPC histories, network ambiguity, and Go client behavior; J28.2 is M3",
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
