#!/usr/bin/env python3
"""Validate J36 component evidence without claiming cache enablement."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

from evidence import java_reports

REPO = Path(__file__).resolve().parents[2]
UNITS = {
    'io.etcdhbase.core.kv.RangeResultCacheTest': {
        'evictsLeastRecentlyUsedEntriesWithinByteAndEntryBounds',
        'entryCannotCrossAuthorizationOrBarrierAndHitSurvivesEvictionOnlyWhileCharged',
        'refusesIncompleteOversizedAndUnmaterializedResults',
        'scopeCancellationClosesAnOutstandingHit',
        'cancellationKeepsHitChargedUntilPhysicalProducerDrains',
    },
    'io.etcdhbase.core.commit.ReadViewRecordStoreTest': {
        'cacheAdmissionRequiresACompletedStandaloneViewAndLookupRechecksTheBarrier',
        'validatedCacheEntryCanRepresentSelectedPendingValueAtTheSameBarrier',
        'cacheReuseAcrossBarriersRequiresCoverageAndOtherwiseFallsBackToDirectRead',
    },
    'io.etcdhbase.core.commit.ManifestPreparationTest': {
        'proofTraversalChargesMetadataAndFailsBeforeExceedingItsOwnPageLimit',
        'coverageProofRequiresContiguousDecisionsAndAuthenticCompleteManifests',
    },
    'io.etcdhbase.core.scan.BudgetManagerTest': {
        'serviceMemoryUsesGlobalAndGroupCapacityWithoutTakingRequestAdmission',
        'cancellationKeepsAdmissionAndIoReservedUntilTheirActualOwnerReleases',
    },
    'io.etcdhbase.server.config.M3RuntimeConfigurationTest': {
        'cacheDefaultsOffAndCannotBeEnabledWithoutIdentityLifecycleProof',
    },
    'io.etcdhbase.server.M3CacheScopeDrainTest': {
        'waitsForLateProducerAndCleanupAfterScopeClose',
    },
    'io.etcdhbase.server.RuntimeServerTest': {
        'rejectsCacheActivationBeforeStartingTheBackend(Path)',
    },
    'io.etcdhbase.hbase.storage.HBaseSystemRecordDecodeTest': {
        'rejectsOversizedCellBeforeCopyingItsValue',
        'acceptsExpectedSingleCell',
    },
}
INTEGRATION = {
    'io.etcdhbase.hbase.probe.SecureHBaseAuthenticationIT': {
        'revokedPermissionDeniesLiveBackendReadsAndFreshAuthentication',
    },
    'io.etcdhbase.hbase.storage.M2ReadViewIT': {'readsSelectedPendingVersionsAndHistoricalRowsInBothLayouts'},
    'io.etcdhbase.server.M3CacheProofIT': {
        'provesUnrelatedAndRejectsRelatedWritesOnBothLayouts',
        'cacheUsesProtectedRealHBaseViewsWithoutEnablingRuntimeCaching',
        'standaloneRangeExecutorUsesCacheOnlyAtTheCurrentProtectedBarrier',
    },
    'io.etcdhbase.server.M3CacheRuntimeIT': {
        'serviceOwnsCacheAndPreservesCurrentBarriersOnBothLayouts',
    },
    'io.etcdhbase.server.M3CacheOversizeIT': {
        'oversizedSystemCellReachesTheClientButIsRejectedBeforeApplicationCopy',
    },
    'io.etcdhbase.server.M3CacheProcessIT': {
        'independentProcessesObserveWritesAndProveUnrelatedKeysOnBothLayouts',
    },
}


def validate_reports(root: Path) -> dict:
    _, units = java_reports(root, set(UNITS), modules=('core', 'hbase', 'server'),
                            required_cases=UNITS, report_kinds=('surefire-reports',))
    _, integration = java_reports(root, set(INTEGRATION), modules=('hbase', 'server'),
                                  required_cases=INTEGRATION, report_kinds=('failsafe-reports',))
    return {**units, **integration}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--artifact-root', type=Path, required=True)
    parser.add_argument('--source', required=True)
    args = parser.parse_args()
    try:
        if not re.fullmatch(r'[0-9a-f]{40}', args.source):
            raise ValueError('invalid source commit')
        head = subprocess.check_output(['git', '-C', str(REPO), 'rev-parse', 'HEAD'], text=True).strip()
        dirty = subprocess.check_output(['git', '-C', str(REPO), 'status', '--porcelain'], text=True)
        if head != args.source or dirty:
            raise ValueError('source changed or is uncommitted')
        suites = validate_reports(args.artifact_root)
        result = {'scope': 'public-minicluster-components', 'source_commit': args.source,
                  'cache_enablement_verified': False, 'j36_complete': False, 'suites': suites,
                  'limitations': ['runtime cache activation is disabled', 'identity lifecycle is not verified',
                                  'a second full runtime cannot start while the owner is active',
                                  'pre-decode HBase response memory is not strictly bounded']}
        with (args.artifact_root / 'summary.json').open('x', encoding='utf-8') as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
        print('J36 component evidence validated; cache enablement is not verified')
        return 0
    except (ValueError, OSError, subprocess.SubprocessError, ET.ParseError, KeyError) as failure:
        print(f'J36 component evidence validation failed: {type(failure).__name__}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
