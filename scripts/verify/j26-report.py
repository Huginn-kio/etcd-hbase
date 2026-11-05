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

from evidence import java_reports


READ_VIEW_IT = "io.etcdhbase.hbase.storage.M2ReadViewIT"
SNAPSHOT_PIN_IT = "io.etcdhbase.hbase.storage.M2SnapshotPinIT"
READ_VIEW_TEST = "io.etcdhbase.core.commit.ReadViewRecordStoreTest"
SNAPSHOT_PIN_TEST = "io.etcdhbase.core.commit.SnapshotPinManagerTest"
INITIALIZATION_TEST = "io.etcdhbase.admin.config.InitializationConfigurationLoaderTest"

REQUIRED_CASES = {
    READ_VIEW_IT: {"readsSelectedPendingVersionsAndHistoricalRowsInBothLayouts"},
    SNAPSHOT_PIN_IT: {"independentManagersShareDurablePinsFloorsAndExpiryFencing"},
    READ_VIEW_TEST: {
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
        "failedRenewalPreventsPointAndScanDataReads",
        "finalPinValidationFailureDiscardsRangeAndReleasesOutputLease",
        "successfulFinalPinValidationDeliversAndTransfersRangeOwnership",
    },
    SNAPSHOT_PIN_TEST: {
        "durableFloorsProtectExistingViewsAndRejectNewOlderViews",
        "selectedMetadataDecisionUsesItsOwnSequenceBoundaryAndReference",
        "expiryAloneDoesNotReleaseProtectionAndRevocationFencesTheOldLease",
        "terminalSlotCanBeReusedOnlyWithANewerFencingGeneration",
        "unknownCasRepliesAreResolvedFromTheDurableSlot",
        "registrationRacesFloorAdvanceOnTheSameShardCas",
        "storedConfigurationIsImmutableAndShardRowsMustBeComplete",
        "interruptedInitialShardCreationCanResumeBeforeTheConfigurationReadyMarker",
        "missingConfigurationAndShardCannotChangeAnActiveRegistryBeforeOpenFails",
        "lateInitialCreateCannotRestoreAMissingShardOfAnEstablishedRegistry",
        "unknownInitializationReservationCanBeRetried",
        "unknownInitializationSealIsResolvedFromTheDurableWitness",
        "abandonedInitializationGenerationsAreBounded",
        "legacyConfigurationRequiresExplicitMigration",
        "missingInitializationWitnessCannotReopenConfiguredShards",
        "missingBothMetadataRecordsDoesNotBootstrapAnActiveRegistry",
        "emptyStoreRequiresExplicitBootstrap",
        "corruptedDurableShardFailsClosed",
        "completionCasLosingToDurableRevocationCannotReportSuccess",
    },
    INITIALIZATION_TEST: {
        "readsBothPublishedCandidatesRetainsTheAlternativeLayoutAndOwnsNoInput",
        "systemTableRequiresSnapshotPinCategory",
    },
}


def report_counts(root: Path, module: str, suite_name: str) -> dict[str, int]:
    _, suites = java_reports(
        root,
        required={suite_name},
        modules=(module,),
        required_cases={suite_name: REQUIRED_CASES[suite_name]},
    )
    return suites[suite_name]


def collect(root: Path, output: Path) -> None:
    hbase_reports = root / "hbase/target/failsafe-reports"
    integration = {
        "read_view": report_counts(root, "hbase", READ_VIEW_IT),
        "snapshot_pin": report_counts(root, "hbase", SNAPSHOT_PIN_IT),
    }
    core = {
        "read_view": report_counts(root, "core", READ_VIEW_TEST),
        "snapshot_pin": report_counts(root, "core", SNAPSHOT_PIN_TEST),
    }
    admin = report_counts(root, "admin", INITIALIZATION_TEST)
    minicluster = hbase_reports / "TEST-io.etcdhbase.hbase.storage.M2SnapshotPinIT.xml"
    stdout = "\n".join(
        element.text or ""
        for element in ET.parse(minicluster).getroot().iter("system-out")
    )
    marker = "j26-snapshot-pin sharedRows=true floorsDurable=true expiryRequiresRevocation=true"
    if marker not in stdout:
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
        "scripts/verify/j26-report.py",
        "scripts/verify/test_j26_report.py",
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
            "interrupted initialization, missing metadata, and late staged creates are fenced",
            "ordinary open refuses absent registry metadata; first creation requires explicit bootstrap",
            "unary read completion waits for pin validation and request cleanup waits for release",
            "initializer schema and examples declare the SNAPSHOT_PIN system category",
        ],
        "unverified": [
            "multi-process or multi-host failure and clock-skew behavior",
            "a physical GC worker deleting HBase history; J26 exposes only the conservative boundary",
            "target HBase identity, permissions, and production acceptance",
            "version 1 snapshot-pin format migration and mixed-version operation",
            "provisioning proof of a fresh table and exclusion of existing services during bootstrap",
            "recovery after the bounded initialization-attempt budget is exhausted; staged-row cleanup",
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
