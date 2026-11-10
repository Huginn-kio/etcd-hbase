#!/usr/bin/env python3
"""Validate one M3 public run; missing business evidence is never a pass."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

from evidence import java_reports


REPO = Path(__file__).resolve().parents[2]
PINNED_SOURCE = "1cf4bc40111fd95d1e14a90a09a146357b15eb13"
SUITE = REPO / "testkit/scenarios/m3-put.yaml"
TASKS = {"J30", "J31", "J32", "J33", "J34", "J35", "J36", "J28.2"}
TASK_CASES = {
    "J30": {"C01", "C07"}, "J31": {"C11", "C12"}, "J32": {"C13"},
    "J33": {"C09", "C10"}, "J34": {"C02", "C04", "C05", "C06"},
    "J35": {"C03"}, "J36": {"C23"},
    "J28.2": {"C02", "C03", "C07", "C09", "C10", "C11", "C12", "C13"},
}
FILTERS = {"min_mod_revision", "max_mod_revision", "min_create_revision", "max_create_revision"}
PUT_CASES = (
    "native-baseline", "J30-update", "J30-same-value", "J30-ignore-value",
    "J30-ignore-lease", "J30-ignore-both", "J30-client-ignore-value",
    "J30-client-ignore-lease", "J30-long-key", "J30-empty-key", "J30-value-provided",
    "J30-lease-provided", "J30-missing-ignore", "J30-request-too-large",
)
TXN_CASES = ("native-baseline",) + tuple(
    f"J31-compare-{target}-{relation}"
    for target in ("version", "create", "mod", "value", "lease")
    for relation in ("equal", "not-equal", "greater", "less")
) + tuple(f"J31-{case}" for case in (
    "compare-missing-version", "compare-missing-value", "compare-range", "then-write", "else-write",
    "no-change", "client-read", "empty-compare", "duplicate-put",
))
NESTED_CASES = ("native-baseline",) + tuple(f"J32-{case}" for case in (
    "branch-base", "historical-reads", "nested-else", "unselected-dynamic", "mutually-exclusive",
    "sibling-duplicate", "parent-overlap", "dynamic-priority", "future-revision",
    "negative-revision", "unselected-empty-key",
))
DELETE_CASES = ("native-baseline",) + tuple(f"J33-{case}" for case in (
    "point", "empty", "recreate", "recreate-read", "seed-range", "prefix", "seed-overlap", "overlap",
    "seed-zero", "zero-delete-put", "zero-put-delete", "seed-client", "client-prefix", "no-prev", "empty-key",
))
DELETE_METHODS = tuple(f"etcdserverpb.KV/{method}" for method in (
    "Put", "DeleteRange", "DeleteRange", "Put", "Txn", "Txn", "DeleteRange", "Txn", "Txn", "Put",
    "Txn", "Txn", "Txn", "DeleteRange", "DeleteRange", "DeleteRange",
))
RANGE_CASES = (
    "native-baseline",
    "J34-seed",
    "J34-update",
    "J34-current",
    "J34-historical",
    "J34-negative-revision",
    "J34-prefix-page",
    "J34-keys-only",
    "J34-negative-limit",
    "J34-max-limit",
    "J34-min-mod",
    "J34-max-mod",
    "J34-min-create",
    "J34-max-create",
    "J34-negative-min-mod",
    "J34-negative-max-mod",
    "J34-negative-min-create",
    "J34-negative-max-create",
    "J34-filtered-point-min-mod",
    "J34-filtered-range-min-mod",
    "J34-filtered-point-max-mod",
    "J34-filtered-range-max-mod",
    "J34-filtered-point-min-create",
    "J34-filtered-range-min-create",
    "J34-filtered-point-max-create",
    "J34-filtered-range-max-create",
    "J34-filtered-range-partial",
    "J34-filtered-point-match",
    "J34-filtered-nested-txn",
    "J34-count-point-min-mod",
    "J34-count-range-min-mod",
    "J34-count-point-max-mod",
    "J34-count-range-max-mod",
    "J34-count-point-min-create",
    "J34-count-range-min-create",
    "J34-count-point-max-create",
    "J34-count-range-max-create",
    "J34-count-missing",
    "J34-count-empty-range",
    "J34-txn-overlay",
    "J34-txn-history",
    "J34-txn-negative",
    "J34-delete",
    "J34-recreate",
    "J34-history-deleted",
    "J34-history-count-deleted",
    "J34-history-count-live",
    "J34-history-recreated",
    "J34-client-prefix",
    "J34-txn-count-after-delete",
    "J34-future",
    "J34-empty-key",
)
RANGE_METHODS = tuple("etcdserverpb.KV/" + {
    "native-baseline": "Put", "J34-seed": "Txn", "J34-update": "Put",
    "J34-filtered-nested-txn": "Txn", "J34-txn-overlay": "Txn", "J34-txn-history": "Txn",
    "J34-txn-negative": "Txn", "J34-txn-count-after-delete": "Txn",
    "J34-delete": "DeleteRange", "J34-recreate": "Put",
}.get(case, "Range") for case in RANGE_CASES)
HBASE_SUITE = "io.etcdhbase.hbase.storage.M3ResultRepositoryIT"
INIT_SUITE = "io.etcdhbase.admin.M3InitializeTablesIT"
SERVER_SUITE = "io.etcdhbase.server.M3PutRpcIT"
FIXTURE_SUITE = "io.etcdhbase.server.M3FixtureIT"
COMMIT_SUITE = "io.etcdhbase.server.M3PutCommitIT"
RECOVERY_SUITE = "io.etcdhbase.server.M3ProcessRecoveryIT"
GROUP_RUNTIME_SUITE = "io.etcdhbase.server.M3TableGroupRuntimeIT"
RUNTIME_SUITE = "io.etcdhbase.server.M3RuntimeIT"
REQUIRED_CASES = {
    HBASE_SUITE: {"persistsVerifiesAndRejectsDamagedResultOnDisposableHBase"},
    INIT_SUITE: {"initializesBothLayoutsAndNeverAdoptsExistingTables",
                 "commandInitializesFreshLocalTablesAndRejectsRepeat"},
    FIXTURE_SUITE: {"singleBucketWorksForBothDisposableLayouts", "provisionsBothLayoutsAndReadsBackFromRealHBase"},
    COMMIT_SUITE: {"preparesPublishesAndRecoversOriginalResultsOnBothLayouts"},
    SERVER_SUITE: {"realGoPutMatchesNativeAcrossLayouts"},
    RECOVERY_SUITE: {"selectedPutIsRecoveredAfterPublisherProcessExits"},
    GROUP_RUNTIME_SUITE: {"realRuntimeRecoversAndRetainsHeaderIdentityOnBothLayouts"},
    RUNTIME_SUITE: {"startsConfiguredServiceAndRecoversWithStableHeaders"},
}
GO_MODULE = "github.com/Reidddddd/etcd-hbase/testkit/go/"
GO_REQUIRED_CASES = {
    "cmd/m3driver": {"TestDriverCanRunAnIsolatedPinnedNativeServer",
                     "TestDriverKeepsFailureTraceAndRejectsFixedSuccessfulResponses"},
    "internal/scenario": {"TestM3PutScenarioRunsAgainstIndependentNativeServers",
                          "TestM3SuiteRejectsEmptyMissingDuplicateAndChangedVersion"},
    "internal/compare": {"TestM3PutComparisonAllowsOnlyTheDeclaredNonRaftTerm"},
    "internal/trace": {"TestReadJSONLRejectsInvalidOrderingAndSecrets"},
}
J31_GO_REQUIRED_CASES = {
    **{package: set(cases) for package, cases in GO_REQUIRED_CASES.items()},
    "cmd/m3driver": GO_REQUIRED_CASES["cmd/m3driver"] | {"TestDriverCanRunAnIsolatedPinnedNativeTxnServer"},
    "internal/scenario": GO_REQUIRED_CASES["internal/scenario"] | {
        "TestM3TxnScenarioRunsAgainstIndependentNativeServers", "TestM3TxnSuiteRejectsMissingOrAlteredSteps"},
    "internal/compare": GO_REQUIRED_CASES["internal/compare"] | {
        "TestM3TxnComparisonKeepsChildResponsesAndMethodsStrict",
        "TestM3TxnCountAllowanceMatchesOnlySelectedRangePosition",
        "TestM3TxnCountValidationDoesNotPartiallyMutateOnMalformedSibling",
        "TestM3TxnCountAllowanceRejectsMatchingMalformedOneof",
        "TestM3TxnCountAllowanceRejectsMalformedStrictSibling"},
}
J31_INTEGRATION_CASES = {
    "io.etcdhbase.hbase.storage.M2SnapshotPinIT": {"transactionRetainsCurrentAndHistoricalPinsUntilFinalValidation"},
    HBASE_SUITE: {"noChangeResultSurvivesLostCasAcknowledgementAndRejectsLateSeal"},
    "io.etcdhbase.server.M3TxnCommitIT": {"publishesMultipleKeysOnceAndReplaysAnEarlierNoChangeResultOnBothLayouts"},
    "io.etcdhbase.server.M3TxnRpcIT": {"goTransactionsMatchNativeOnBothLayouts"},
    GROUP_RUNTIME_SUITE: {"realRuntimeExecutesFlatTransactionsOnBothLayouts",
                          "realRuntimeRecoversAndRetainsHeaderIdentityOnBothLayouts"},
    RUNTIME_SUITE: {"startsConfiguredServiceAndRecoversWithStableHeaders"},
}
J31_UNIT_CASES = {
    "io.etcdhbase.core.kv.CompareEvaluatorTest": {"numericTargetsUseSignedInt64ForEveryRelation",
                                                   "valuesCompareUnsignedBytesIncludingEmptyAndHighBytes"},
    "io.etcdhbase.core.commit.InvocationServiceTest": {"concurrentSealAndNoChangeHaveExactlyOneDurableWinner"},
    "io.etcdhbase.core.commit.CommitPublisherTest": {"staleSealedObjectCannotPublishAfterNoChangeWinsDurably"},
    "io.etcdhbase.core.commit.ReadViewRecordStoreTest": {"transactionComparesPointAndRangeAgainstOneProtectedBase",
        "transactionComparePropagatesBackendFailureInsteadOfChoosingElse",
        "flatTransactionReadsItsPriorWriteAndUsesOneCommitRevision",
        "flatTransactionCanWriteInElse", "readOnlyTransactionNeedsNoInvocation",
        "cancellationDuringFinalPinValidationRejectsTheTransaction"},
    "io.etcdhbase.grpc.protocol.TxnRequestMapperTest": {"matchesPinnedGoEtcdInternalTxnEnvelopeSizeAcrossVarintBoundaries"},
    "io.etcdhbase.grpc.protocol.TxnResponseMapperTest": {"projectsOuterIdentityAndPerOperationRevisionsWithoutInventingChildIdentities"},
    "io.etcdhbase.server.RequestLifecycleTest": {"drainCompletionWaitsForAdmittedRequestCleanupAfterShutdownStarts"},
    "io.etcdhbase.server.M3RuntimeGroupsTest": {"backendFactoryWaitsForRuntimeScannerAndRequestDrain"},
}
J32_GO_REQUIRED_CASES = {
    **{package: set(cases) for package, cases in J31_GO_REQUIRED_CASES.items()},
    "internal/scenario": J31_GO_REQUIRED_CASES["internal/scenario"] | {
        "TestM3NestedTxnScenarioRunsAgainstIndependentNativeServers",
        "TestM3NestedTxnSuiteRejectsMissingOrAlteredSteps",
        "TestM3NativeNestedCompareUsesBaseAndChildHeaderIsEmpty",
        "TestM3NativeNestedHistoricalReadAndRevisionPrecheck",
        "TestM3NativeNestedStaticOverlapAndDynamicErrorOrder"},
    "internal/compare": J31_GO_REQUIRED_CASES["internal/compare"] | {
        "TestM3NestedTxnCountAllowanceIsRestrictedToItsSelectedRequestPath"},
}
J32_INTEGRATION_CASES = {
    "io.etcdhbase.hbase.storage.M2SnapshotPinIT": {
        "transactionRetainsCurrentAndHistoricalPinsUntilFinalValidation"},
    HBASE_SUITE: {"persistsVerifiesAndRejectsDamagedResultOnDisposableHBase",
                  "noChangeResultSurvivesLostCasAcknowledgementAndRejectsLateSeal"},
    "io.etcdhbase.server.M3TxnCommitIT": {
        "publishesMultipleKeysOnceAndReplaysAnEarlierNoChangeResultOnBothLayouts",
        "nestedWorksetExceedsMemoryAndDoesNotReplayAnUncertainPageOnBothLayouts"},
    "io.etcdhbase.server.M3NestedRpcIT": {"goNestedTransactionsMatchNativeOnBothLayouts"},
    "io.etcdhbase.server.M3WorksetIT": {"orderedPrivateOverlayMergesWithRealScansAcrossBucketsAndLayouts"},
}
J32_UNIT_CASES = {
    **{suite: set(cases) for suite, cases in J31_UNIT_CASES.items()},
    "io.etcdhbase.core.commit.ReadViewRecordStoreTest": J31_UNIT_CASES[
        "io.etcdhbase.core.commit.ReadViewRecordStoreTest"] | {
            "nestedCompareUsesBaseWhileNestedReadSeesEarlierPut",
            "nestedSelectionDoesNotReadUnselectedBranchesAndPropagatesBackendFailure",
            "selectedNestedHistoricalReadUsesSameBarrierWithoutOverlayingPriorPut",
            "nestedPreflightFollowsNativeDynamicErrorOrderAndSkipsUnselectedTargets",
            "uncertainPreparedInvocationCannotRerunItsComparisons",
            "abandonedHistoricalPinFutureCannotCancelInstallationOrLeakItsProtection",
            "failedHistoricalPinClosesThePreviouslyAcquiredBasePin",
            "tooManySelectedHistoricalPinsAreResourceExhaustion",
            "selectedHistoricalPinsKeepTheOriginalBarrierAfterAnotherCommit"},
    "io.etcdhbase.core.commit.PagedWorksetIndexTest": {
        "pagesLargeOperationAndReadsOnlyCompletedBucketKeyOrder",
        "incompleteOperationNeverChangesTheCommittedRootAndFailureFencesTheAttempt",
        "detectsCorruptPagesAndDoesNotReuseAnotherTransaction",
        "durableStartPreventsBlindReplayOfAnUnfinishedInvocation",
        "laterCompletedOperationReplacesOneKeyButKeepsAllEventOrdinals",
        "cancelledCursorFutureDoesNotReleaseItsInFlightReadOrSuppressFailure"},
    "io.etcdhbase.core.kv.TxnWorkingSetTest": {
        "orderedWritesShareOneRevisionAndOnlyTheWorkingViewSeesThem",
        "persistedPayloadCanExceedTheWorkingMemoryBound", "mergesCompletedOverlayAndFixedBaseInBucketKeyOrder",
        "failedPartialOperationFencesTheAttempt",
        "frozenCheckpointCannotRaceWithAnUnfinishedOrLaterWrite",
        "changedEventIsRejectedBeforeTheCandidateOrEventStreamCanComplete"},
    "io.etcdhbase.core.commit.ManifestPreparationTest": {
        "asynchronouslyStreamsCandidatesAndEventsBeforeSealing",
        "cancellationBeforeAnAsyncCursorReturnsCannotSealAManifest"},
    "io.etcdhbase.core.scan.BudgetManagerTest": {"internalPointReadsShareBothPointAndReadByteLimits"},
    "io.etcdhbase.grpc.protocol.TxnRequestMapperTest": J31_UNIT_CASES[
        "io.etcdhbase.grpc.protocol.TxnRequestMapperTest"] | {
            "matchesNativeNestedPotentialWriteIntervalsEvenInUnselectedBranches",
            "depthBudgetIncludesLeafResponseBeforeAnyTransactionStarts"},
    "io.etcdhbase.grpc.protocol.TxnResponseMapperTest": J31_UNIT_CASES[
        "io.etcdhbase.grpc.protocol.TxnResponseMapperTest"] | {
            "projectsNestedTxnHeaderAndOrderedChildrenWithoutInventingIdentity"},
}
J33_GO_REQUIRED_CASES = {
    **{package: set(cases) for package, cases in J32_GO_REQUIRED_CASES.items()},
    "internal/scenario": J32_GO_REQUIRED_CASES["internal/scenario"] | {
        "TestM3DeleteScenarioMatchesIndependentNativeServers", "TestM3DeleteSuiteRejectsMissingCoverage",
        "TestM3NativeDeleteOrderingAndEmptyRevision", "TestM3NativeZeroEndAllowsDeleteAndPutOfSameKey",
        "TestM3NativeDeleteEnvelopeSizes", "TestM3NativeFullKeyspaceDeleteOnDisposableServer"},
    "internal/compare": J32_GO_REQUIRED_CASES["internal/compare"] | {
        "TestM3DeleteComparisonKeepsPreviousValuesAndMethodsStrict",
        "TestM3DeleteComparisonKeepsTxnCountOneofsStrict"},
}
J33_INTEGRATION_CASES = {
    "io.etcdhbase.hbase.storage.M2SnapshotPinIT": {
        "transactionRetainsCurrentAndHistoricalPinsUntilFinalValidation"},
    HBASE_SUITE: {"persistsVerifiesAndRejectsDamagedResultOnDisposableHBase",
                  "noChangeResultSurvivesLostCasAcknowledgementAndRejectsLateSeal"},
    "io.etcdhbase.server.M3TxnCommitIT": {
        "publishesMultipleKeysOnceAndReplaysAnEarlierNoChangeResultOnBothLayouts",
        "nestedWorksetExceedsMemoryAndDoesNotReplayAnUncertainPageOnBothLayouts",
        "largeDeleteIsAtomicBudgetedAndReplayableOnBothLayouts",
        "partialDeleteMaterializationRecoversWithoutPartialReadsOnBothLayouts"},
    "io.etcdhbase.server.M3DeleteRpcIT": {"goDeletesMatchNativeOnBothLayouts"},
    "io.etcdhbase.server.M3ProcessRecoveryIT": {"selectedDeleteIsRecoveredAfterPublisherProcessExits"},
    "io.etcdhbase.server.M3WorksetIT": {"orderedPrivateOverlayMergesWithRealScansAcrossBucketsAndLayouts"},
}
J33_UNIT_CASES = {
    **{suite: set(cases) for suite, cases in J32_UNIT_CASES.items()},
    "io.etcdhbase.core.commit.ReadViewRecordStoreTest": J32_UNIT_CASES[
        "io.etcdhbase.core.commit.ReadViewRecordStoreTest"] | {
            "overlappingDeletesUseOnePrivateViewAndOneRevision",
            "zeroEndDeleteAndPutOfTheSameKeyUseTheirActualOperationOrder", "pointDeleteVisitsOnlyItsSelectedBucket",
            "deleteThenIgnoreValueRejectsBeforeAnyPublicMutation"},
    "io.etcdhbase.core.kv.TxnWorkingSetTest": J32_UNIT_CASES["io.etcdhbase.core.kv.TxnWorkingSetTest"] | {
        "deletesOnlyLiveRecordsAndKeepsRepeatedDeletesEmpty", "manyPriorTombstonesDoNotCreateEventsOrARecursiveFutureChain",
        "deleteAndPutOfTheSameKeyPreserveOrderedEventsAndFinalCandidate",
        "incompleteDeleteKeepsItsCheckpointPrivateAndFencesTheAttempt",
        "cancellingAnInFlightDeleteNeverMakesItsPartialOperationVisible",
        "leaseBearingKeyRejectsTheEntirePartiallyPreparedDelete",
        "failureAtEachDeleteEventLeavesTheOperationUncheckpointed",
        "deleteRejectsOutOfRangeAndUnorderedInputsBeforePublishingAnOperation"},
    "io.etcdhbase.core.kv.DeletePlannerTest": {
        "plansARealTombstoneAndRetainsTheCompletePreviousValueForTheEvent",
        "absentAndAlreadyDeletedRecordsDoNotProduceAnEvent", "rejectsUnresolvedLeaseMissingValuesFutureVersionsAndOverflow"},
    "io.etcdhbase.core.kv.OrderedViewCursorTest": {
        "mergesBucketsInByteOrderAndReleasesEveryHead", "rejectsDuplicateKeysWithoutLeakingTheAlreadyLoadedHeads",
        "abandoningAFutureDoesNotReleaseAStillRunningSource", "closingWhileAReadIsPendingDiscardsTheLateHead"},
    "io.etcdhbase.core.kv.BucketViewCursorTest": {
        "releasesEachPhysicalPageWhileRetainingTheReturnedHead", "closeCancelsThePhysicalReadAndRejectsALatePage"},
    "io.etcdhbase.grpc.protocol.TxnRequestMapperTest": J32_UNIT_CASES[
        "io.etcdhbase.grpc.protocol.TxnRequestMapperTest"] | {"mapsDeletePointFullSpaceAndEmptyRangesWithoutLosingPreviousValueFlag"},
    "io.etcdhbase.grpc.protocol.TxnResponseMapperTest": J32_UNIT_CASES[
        "io.etcdhbase.grpc.protocol.TxnResponseMapperTest"] | {"projectsChangedAndEmptyDeletesAtTheirActualOperationRevision"},
    "io.etcdhbase.grpc.protocol.DeleteProtocolLimitTest": {"matchesPinnedNativeEnvelopeSizesAndRejectsOnlyOverTheLimit"},
}


J34_GO_REQUIRED_CASES = {
    **{package: set(cases) for package, cases in J33_GO_REQUIRED_CASES.items()},
    "internal/scenario": J33_GO_REQUIRED_CASES["internal/scenario"] | {
        "TestM3RangeScenarioMatchesIndependentNativeServers", "TestM3RangeSuiteRejectsMissingCoverage"},
    "internal/compare": J33_GO_REQUIRED_CASES["internal/compare"] | {
        "TestM3RangeComparisonKeepsWireFieldsAndMethodsStrict", "TestM3RangeComparisonAllowsOnlyFilteredResultCounts",
        "TestM3RangeFilteredCountRejectsMalformedFilterAndImpossibleMore",
        "TestM3RangeComparisonAppliesOnlyExistingPageCountAllowance", "TestM3RangeComparisonKeepsTxnCountOneofsStrict"},
}
J34_INTEGRATION_CASES = {
    **{suite: set(cases) for suite, cases in J33_INTEGRATION_CASES.items()},
    "io.etcdhbase.server.M3RangeRpcIT": {"standaloneAndTransactionRangeUseTheSameProtectedViewOnBothLayouts"},
    "io.etcdhbase.server.M3RangeComparisonIT": {"goRangesMatchNativeOnBothLayouts"},
}
J34_UNIT_CASES = {
    **{suite: set(cases) for suite, cases in J33_UNIT_CASES.items()},
    "io.etcdhbase.core.commit.ReadViewRecordStoreTest": J33_UNIT_CASES[
        "io.etcdhbase.core.commit.ReadViewRecordStoreTest"] | {
            "transactionRangeUsesWorkingAndHistoricalViewsWithFiltersAndProjection",
            "rangeKeysOnlyAndCountOnlyDoNotFetchValues", "countOnlyUsesSelectedPendingCandidatesAndTombstones",
            "rangeCountOnlyStopsAfterAMatchButChecksEveryBucketForAbsence",
            "pointRangesUseOnePointReadAndRequestOnlyTheNeededProjection",
            "cancelledPointRangeRetainsItsPinUntilThePhysicalReadCompletes",
            "rangeOutputBudgetFailureDoesNotReturnPartialResults",
            "cancelledRangeKeepsItsPinUntilAnInFlightValueReadDrains",
            "rangeRejectsAValueThatDoesNotMatchItsSelectedMetadata",
            "filteredPointAndUnlimitedRangeCountOnlyReturnedRecords",
            "standaloneAndTransactionRangePreserveDifferentRevisionChecks"},
    "io.etcdhbase.core.kv.RangeCommandTest": {"revisionFilterAppliesEveryNonzeroBoundaryInclusively", "negativeBoundariesRemainRealBoundaries"},
    "io.etcdhbase.core.kv.RangeSelectorTest": {"limitAndMoreApplyAfterTombstonesAndRevisionFilters",
        "countOnlyUsesExistenceWithoutApplyingRevisionFilters", "unlimitedAndExtremeLimitsDoNotOverflowOrHidePreFilterCount"},
    "io.etcdhbase.core.kv.SequentialViewCursorTest": {"exhaustsTheMaximumBucketCountWithoutRecursionOverflow",
        "closingDuringAReadRejectsLateResultsAndConcurrentReads", "backendFailureIsNotInterpretedAsAnEmptyBucket"},
    "io.etcdhbase.grpc.protocol.RangeRequestMapperTest": {"decodesPointFromKeyFullSpaceAndEmptyIntervalsAsBytes",
        "preservesRequestContextAndAllOptionValuesWithoutNarrowing", "rejectsAnEmptyKeyBeforeConstructingAnyRange"},
}


# One registry supplies the evidence contract to every validation stage.
ALL_UNIT_MODULES = ("protocol", "spi", "core", "hbase", "grpc", "admin", "server")
RUN_TASKS = {
    "J30": {"cases": PUT_CASES, "methods": ("etcdserverpb.KV/Put",) * len(PUT_CASES),
            "prefix": "", "scenario": "m3-put-v1", "suite": "m3-put.yaml", "compare": "--m3-put",
            "go": GO_REQUIRED_CASES, "integration": REQUIRED_CASES, "units": None},
    "J31": {"cases": TXN_CASES, "methods": ("etcdserverpb.KV/Put",) + ("etcdserverpb.KV/Txn",) * (len(TXN_CASES) - 1),
            "prefix": "j31-", "scenario": "m3-txn-v1", "suite": "m3-txn.yaml", "compare": "--m3-txn",
            "go": J31_GO_REQUIRED_CASES, "integration": J31_INTEGRATION_CASES, "units": J31_UNIT_CASES,
            "unit_modules": ("core", "grpc", "server")},
    "J32": {"cases": NESTED_CASES, "methods": ("etcdserverpb.KV/Put",) + ("etcdserverpb.KV/Txn",) * (len(NESTED_CASES) - 1),
            "prefix": "j32-", "scenario": "m3-nested-txn-v1", "suite": "m3-nested-txn.yaml", "compare": "--m3-txn",
            "go": J32_GO_REQUIRED_CASES, "integration": J32_INTEGRATION_CASES, "units": J32_UNIT_CASES,
            "unit_modules": ALL_UNIT_MODULES},
    "J33": {"cases": DELETE_CASES, "methods": DELETE_METHODS, "prefix": "j33-",
            "scenario": "m3-delete-v1", "suite": "m3-delete.yaml", "compare": "--m3-delete",
            "go": J33_GO_REQUIRED_CASES, "integration": J33_INTEGRATION_CASES, "units": J33_UNIT_CASES,
            "unit_modules": ALL_UNIT_MODULES},
    "J34": {"cases": RANGE_CASES, "methods": RANGE_METHODS, "prefix": "j34-",
            "scenario": "m3-range-v1", "suite": "m3-range.yaml", "compare": "--m3-range",
            "go": J34_GO_REQUIRED_CASES, "integration": J34_INTEGRATION_CASES, "units": J34_UNIT_CASES,
            "unit_modules": ALL_UNIT_MODULES},
}


def unique_pairs(pairs):
    """Reject duplicate object keys wherever M3 metadata is parsed."""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate metadata key")
        result[key] = value
    return result


def read_json(path: Path) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1 << 20:
        raise ValueError("missing, linked or oversized M3 metadata")
    with path.open(encoding="utf-8") as stream:
        data = json.load(stream, object_pairs_hook=unique_pairs)
    if not isinstance(data, dict):
        raise ValueError("M3 metadata must be an object")
    return data


def file_digest(path: Path) -> str:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 256 << 20:
        raise ValueError("missing, linked or oversized M3 evidence")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_case_registry(registry: dict) -> None:
    """A registered case is planned coverage, never proof that a test ran."""
    if (registry.get("schema") != "etcd-hbase-m3-case-registry/v1"
            or registry.get("protocol") != "etcd-v3.5.23"):
        raise ValueError("M3 case registry version or protocol differs")
    tasks = registry.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != len(TASK_CASES):
        raise ValueError("M3 task coverage is incomplete")
    seen = set()
    for item in tasks:
        if (not isinstance(item, dict) or not isinstance(item.get("id"), str)
                or item["id"] not in TASK_CASES):
            raise ValueError("unknown M3 task")
        task = item["id"]
        cases = item.get("case_ids")
        if (task in seen or not isinstance(cases, list)
                or not all(isinstance(case, str) for case in cases)
                or len(cases) != len(set(cases)) or set(cases) != TASK_CASES[task]):
            raise ValueError("M3 task case coverage is missing or duplicated")
        seen.add(task)
    probes = registry.get("pending_native_probes")
    if not isinstance(probes, list) or len(probes) != 1 or not isinstance(probes[0], dict):
        raise ValueError("required native Count probe is missing")
    probe = probes[0]
    shapes, filters = probe.get("shapes"), probe.get("filters")
    if (probe.get("id") != "J34-filtered-ordinary-range-count" or probe.get("task") != "J34"
            or probe.get("case_id") != "C06" or not isinstance(shapes, list)
            or not all(isinstance(shape, str) for shape in shapes)
            or len(shapes) != 2 or set(shapes) != {"exact-key", "range-without-limit"}
            or not isinstance(filters, list) or len(filters) != len(FILTERS)
            or not all(isinstance(value, str) for value in filters)
            or set(filters) != FILTERS or not isinstance(probe.get("purpose"), str)
            or not probe["purpose"].strip()):
        raise ValueError("native Count probe coverage differs")


def validate_metadata(metadata: dict, scope: str, layout: str,
                      suite_sha: str, prefix_sha: str, trace_sha: str,
                      scenario_id: str = "m3-put-v1") -> None:
    build = metadata.get("build")
    if not isinstance(build, dict):
        raise ValueError("missing M3 Go build metadata")
    modules = build.get("modules")
    expected_modules = [
        {"path": "go.etcd.io/etcd/api/v3", "version": "v3.5.23"},
        {"path": "go.etcd.io/etcd/client/v3", "version": "v3.5.23"},
        {"path": "go.etcd.io/etcd/server/v3", "version": "v3.5.23"},
    ]
    if (metadata.get("status") != "passed" or metadata.get("evidence_scope") != scope
            or metadata.get("suite_sha256") != suite_sha or metadata.get("prefix_sha256") != prefix_sha
            or metadata.get("trace_sha256") != trace_sha or build.get("scenario_id") != scenario_id
            or build.get("protocol") != "etcd-v3.5.23" or build.get("source_revision") != PINNED_SOURCE
            or build.get("go_version") != "go1.27.2" or modules != expected_modules
            or metadata.get("embedded_native") is not (scope == "native-etcd")
            or metadata.get("layout") != layout):
        raise ValueError("M3 metadata differs from the pinned run, suite, or trace")


def validate_trace_cases(path: Path, scope: str, task: str = "J30") -> None:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 256 << 20:
        raise ValueError("M3 trace is missing, linked or oversized")
    spec = RUN_TASKS.get(task)
    if spec is None:
        raise ValueError("unregistered M3 trace task")
    cases, methods = spec["cases"], spec["methods"]
    records = []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if len(records) >= len(cases):
                raise ValueError("unexpected M3 trace length")
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError("M3 trace record must be an object")
            records.append(record)
    if (tuple(record.get("case_id") for record in records) != cases
            or any(record.get("schema") != "etcd-hbase-trace/v1"
                   or record.get("evidence_scope") != scope
                   or record.get("method") != methods[index]
                   for index, record in enumerate(records))):
        raise ValueError("M3 trace is incomplete or belongs to another scope")


def validate_go_report(path: Path, required_cases: dict = GO_REQUIRED_CASES) -> None:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 128 << 20:
        raise ValueError("M3 Go test evidence is missing, linked or oversized")
    packages, tests = set(), {}
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError("invalid M3 Go test event")
            action, package, test = event.get("Action"), event.get("Package", ""), event.get("Test")
            if action in {"skip", "fail"}:
                raise ValueError("M3 Go tests contain skipped or failed evidence")
            if action != "pass":
                continue
            if not isinstance(package, str) or not package.startswith(GO_MODULE):
                raise ValueError("unexpected Go test package")
            package = package.removeprefix(GO_MODULE)
            if test:
                tests.setdefault(package, set()).add(test)
            elif package in packages:
                raise ValueError("duplicate Go package result")
            else:
                packages.add(package)
    if not set(required_cases).issubset(packages) or any(
            not expected.issubset(tests.get(package, set())) for package, expected in required_cases.items()):
        raise ValueError("required M3 Go test cases are missing")


def validate_run(root: Path, repository: Path = REPO) -> dict:
    run, source, task = validate_run_identity(root, repository)
    prefix = validate_run_traces(root, repository, run, source, task)
    suites = validate_run_reports(root, task)
    compare_run_traces(root, repository, prefix, task)
    limitations = ["raft_term=0 is a disclosed non-Raft projection",
                   "public MiniCluster is not target HBase or target identity acceptance",
                   "an unconfirmed scanner close retires local capacity; remote lifetime across restarts is unverified"]
    if task in {"J32", "J33", "J34"}:
        limitations.extend([
            "private workset pages are non-expiring until long-term collection is implemented in J53",
            "a started invocation with an unknown outcome requires a durable decision or fencing proof; "
            "automatic recovery is not yet integrated",
        ])
    if task in {"J33", "J34"}:
        limitations.append("mixed-version recovery and rollback of same-revision previous-value events are unverified")
        limitations.append("zero-end delete followed by an ignore-value Put of the same key is rejected before publication; "
                           "native fault behavior is not compared")
    if task == "J34":
        limitations.append("filtered ordinary Range Count follows returned Kvs and may differ from native prefilter Count")
    return {"phase": "M3", "tasks": [task], "scope": run["scope"], "run_id": run["run_id"],
            "source_commit": source, "suites": suites,
            "trace_cases": list(RUN_TASKS[task]["cases"]),
            "limitations": limitations}


def validate_run_identity(root: Path, repository: Path) -> tuple[dict, str, str]:
    run = read_json(root / "run.json")
    run_id = run.get("run_id")
    tasks = run.get("tasks")
    if (run.get("schema") != "etcd-hbase-m3-run/v1"
            or run.get("scope") != "public-local-minicluster"
            or not isinstance(tasks, list) or len(tasks) != 1
            or not isinstance(tasks[0], str) or tasks[0] not in RUN_TASKS
            or not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", run_id)
            or root.name != run_id or run.get("commands_passed") is not True):
        raise ValueError("M3 run identity, scope, tasks, or command status is invalid")
    source = run.get("source_commit")
    if not isinstance(source, str) or not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("M3 source revision is invalid")
    current = subprocess.check_output(["git", "-C", str(repository), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(["git", "-C", str(repository), "status", "--porcelain"], text=True)
    if current != source or dirty:
        raise ValueError("M3 source changed or is uncommitted")

    return run, source, tasks[0]


def validate_run_traces(root: Path, repository: Path, run: dict, source: str, task: str) -> str:
    run_id = run["run_id"]
    validate_case_registry(read_json(repository / "testkit/cases/m3-cases.json"))
    spec = RUN_TASKS[task]
    prefix, scenario, suite_file = spec["prefix"], spec["scenario"], spec["suite"]
    suite_sha = file_digest(repository / "testkit/scenarios" / suite_file)
    prefix_sha = hashlib.sha256(f"etcd-hbase-m3-test/{run_id}/".encode()).hexdigest()
    for name, scope, layout in (("native", "native-etcd", ""), ("target-history", "target-hbase", "HISTORY"),
                                ("target-native", "target-hbase", "NATIVE")):
        trace = root / f"{prefix}{name}-trace.jsonl"
        validate_metadata(read_json(root / f"{prefix}{name}-metadata.json"), scope, layout, suite_sha,
                          prefix_sha, file_digest(trace), scenario)
        validate_trace_cases(trace, scope, task)

    fixture = read_json(root / f"{prefix}target-fixture.json")
    if (fixture.get("kind") != "java-grpc-hbase-minicluster" or fixture.get("run_id") != run_id
            or fixture.get("source_commit") != source or fixture.get("authentication") != "dev-no-auth"
            or fixture.get("layouts") != ["HISTORY", "NATIVE"]):
        raise ValueError("M3 target fixture provenance is incomplete or mismatched")
    return prefix


def validate_run_reports(root: Path, task: str) -> dict:
    spec = RUN_TASKS[task]
    validate_go_report(root / "go-test.jsonl", spec["go"])
    integration = spec["integration"]
    _, suites = java_reports(root, set(integration), modules=("hbase", "admin", "server"),
                             required_cases=integration, report_kinds=("failsafe-reports",))
    required_units = spec["units"]
    if required_units is not None:
        _, units = java_reports(root, set(required_units), modules=spec["unit_modules"],
                                required_cases=required_units, report_kinds=("surefire-reports",))
        suites.update(units)
    return suites


def compare_run_traces(root: Path, repository: Path, prefix: str, task: str) -> None:
    for layout in ("history", "native"):
        command = [str(repository / "scripts/dev/go-program.sh"), "native-etcd", "compare",
                   RUN_TASKS[task]["compare"],
                   "--left", str(root / f"{prefix}native-trace.jsonl"),
                   "--right", str(root / f"{prefix}target-{layout}-trace.jsonl")]
        subprocess.run(command, cwd=repository, check=True, stdout=subprocess.DEVNULL)


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--artifact-root", type=Path)
    mode.add_argument("--validate-registry", action="store_true")
    arguments = parser.parse_args()
    try:
        if arguments.validate_registry:
            validate_case_registry(read_json(REPO / "testkit/cases/m3-cases.json"))
            print("M3 planned case registry validated; no business test result is implied")
            return 0
        root = arguments.artifact_root.resolve()
        summary = validate_run(root)
        output = root / "summary.json"
        with output.open("x", encoding="utf-8") as stream:
            json.dump(summary, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        print(f"M3 public {summary['tasks'][0]} evidence validated")
        return 0
    except (ValueError, OSError, subprocess.SubprocessError, ET.ParseError, UnicodeError,
            KeyError, TypeError) as failure:
        print(f"M3 evidence validation failed: {type(failure).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
