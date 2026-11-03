#!/usr/bin/env python3
"""Reject incomplete J26 core and MiniCluster evidence."""
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET


def report_counts(path: Path, expected_tests: int, required_cases: set[str]) -> dict[str, int]:
    if not path.is_file():
        raise ValueError(f"missing test report: {path.name}")
    suite = ET.parse(path).getroot()
    counts = {
        key: int(suite.get(key, "0"))
        for key in ("tests", "failures", "errors", "skipped")
    }
    cases = {case.get("name") for case in suite.iter("testcase")}
    if counts != {"tests": expected_tests, "failures": 0, "errors": 0, "skipped": 0}:
        raise ValueError(f"test report did not pass exactly: {path.name}")
    if not required_cases.issubset(cases):
        raise ValueError(f"required cases are missing: {path.name}")
    return counts


def collect(root: Path, output: Path) -> None:
    hbase_reports = root / "hbase/target/failsafe-reports"
    core_reports = root / "core/target/surefire-reports"
    integration = {
        "read_view": report_counts(
            hbase_reports / "TEST-io.etcdhbase.hbase.storage.M2ReadViewIT.xml",
            1,
            {"readsSelectedPendingVersionsAndHistoricalRowsInBothLayouts"},
        ),
        "snapshot_pin": report_counts(
            hbase_reports / "TEST-io.etcdhbase.hbase.storage.M2SnapshotPinIT.xml",
            1,
            {"independentManagersShareDurablePinsFloorsAndExpiryFencing"},
        ),
    }
    core = {
        "read_view": report_counts(
            core_reports / "TEST-io.etcdhbase.core.commit.ReadViewRecordStoreTest.xml",
            4,
            {
                "currentReadOverlaysOnlyTheSelectedPendingTransactionBeforeRangeMerge",
                "historicalReadUsesItsOwnBarrierAndCanReadDeleteThenRecreateVersions",
                "selectedRootIsPinnedAndRecheckedBeforeAnyDataRead",
                "cancellingAReadKeepsItsPinUntilThePhysicalProducerStops",
            },
        ),
        "snapshot_pin": report_counts(
            core_reports / "TEST-io.etcdhbase.core.commit.SnapshotPinManagerTest.xml",
            7,
            {
                "durableFloorsProtectExistingViewsAndRejectNewOlderViews",
                "selectedMetadataDecisionUsesItsOwnSequenceBoundaryAndReference",
                "expiryAloneDoesNotReleaseProtectionAndRevocationFencesTheOldLease",
                "terminalSlotCanBeReusedOnlyWithANewerFencingGeneration",
                "unknownCasRepliesAreResolvedFromTheDurableSlot",
                "registrationRacesFloorAdvanceOnTheSameShardCas",
                "storedConfigurationIsImmutableAndShardRowsMustBeComplete",
            },
        ),
    }
    admin = report_counts(
        root / "admin/target/surefire-reports/"
        "TEST-io.etcdhbase.admin.config.InitializationConfigurationLoaderTest.xml",
        6,
        {
            "readsBothPublishedCandidatesRetainsTheAlternativeLayoutAndOwnsNoInput",
            "systemTableRequiresSnapshotPinCategory",
        },
    )
    minicluster = hbase_reports / "TEST-io.etcdhbase.hbase.storage.M2SnapshotPinIT.xml"
    stdout = "\n".join(
        element.text or ""
        for element in ET.parse(minicluster).getroot().iter("system-out")
    )
    if "j26-snapshot-pin sharedRows=true floorsDurable=true expiryRequiresRevocation=true" not in stdout:
        raise ValueError("J26 MiniCluster evidence marker is missing")

    implementation_files = [
        "core/src/main/java/io/etcdhbase/core/commit/PinnedReadView.java",
        "core/src/main/java/io/etcdhbase/core/commit/ReadViewPinRegistry.java",
        "core/src/main/java/io/etcdhbase/core/commit/ReadViewRecordStore.java",
        "core/src/main/java/io/etcdhbase/core/commit/ReadViewService.java",
        "core/src/main/java/io/etcdhbase/core/commit/SnapshotGcBoundary.java",
        "core/src/main/java/io/etcdhbase/core/commit/SnapshotPinConfiguration.java",
        "core/src/main/java/io/etcdhbase/core/commit/SnapshotPinManager.java",
        "core/src/main/java/io/etcdhbase/core/commit/SnapshotPinRecordCodec.java",
        "core/src/main/java/io/etcdhbase/core/commit/SnapshotPinShard.java",
        "core/src/main/java/io/etcdhbase/core/scan/ExistsExecutor.java",
        "core/src/main/java/io/etcdhbase/core/scan/PointRead.java",
        "core/src/main/java/io/etcdhbase/core/scan/SortedMergeCursor.java",
        "core/src/main/java/io/etcdhbase/core/storage/RecordStore.java",
        "core/src/test/java/io/etcdhbase/core/commit/ReadViewRecordStoreTest.java",
        "core/src/test/java/io/etcdhbase/core/commit/SnapshotPinManagerTest.java",
        "admin/src/main/java/io/etcdhbase/admin/config/InitializationConfiguration.java",
        "admin/src/main/java/io/etcdhbase/admin/config/InitializationConfigurationValidator.java",
        "admin/src/test/java/io/etcdhbase/admin/config/InitializationConfigurationLoaderTest.java",
        "config/schema/server.schema.json",
        "config/examples/j05/history-table.json",
        "config/examples/j05/native-cell-versions.json",
        "config/examples/j06/server.json",
        "docs/admin/j05-initialization.md",
        "hbase/pom.xml",
        "hbase/src/main/java/io/etcdhbase/hbase/storage/SystemTableBinding.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/J20CommitControlStoreIT.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/M2SnapshotPinIT.java",
        "hbase/src/test/java/io/etcdhbase/hbase/storage/SystemTableBindingTest.java",
    ]
    patch = subprocess.run(
        ["git", "diff", "--binary", "HEAD", "--", *implementation_files],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    shutil.copyfile(minicluster, output / minicluster.name)
    summary = {
        "phase": "J26",
        "scope": "public-local-minicluster",
        "base_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
        ).stdout.strip(),
        "implementation_patch_sha256": hashlib.sha256(patch).hexdigest(),
        "ended_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "integration": integration,
        "core": core,
        "admin_initialization_tests": admin,
        "verified": [
            "a second manager reads durable pin rows written by the first manager",
            "revision and sequence floors advance independently and survive manager reopen",
            "floor advancement preserves an existing lower-revision pin and rejects a new stale view",
            "expiry alone preserves protection; durable revocation fences the old generation",
            "core CAS race, slot exhaustion/reuse, config mismatch, and missing-shard fail-closed cases",
            "unary read completion waits for pin validation and request cleanup waits for release",
            "initializer schema and examples declare the SNAPSHOT_PIN system category",
        ],
        "unverified": [
            "multi-process or multi-host failure and clock-skew behavior",
            "a physical GC worker deleting HBase history; J26 exposes only the conservative boundary",
            "target HBase identity, permissions, and production acceptance",
            "maintainer review",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({
        "phase": summary["phase"],
        "scope": summary["scope"],
        "integration": integration,
        "admin_initialization_tests": admin,
    }))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError, IndexError):
        raise SystemExit("J26 verification evidence rejected") from None
