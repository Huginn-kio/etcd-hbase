#!/usr/bin/env python3
"""Positive and negative controls for J26's MiniCluster evidence gate."""
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET


SCRIPT = Path(__file__).with_name("j26-report.py")
SPEC = importlib.util.spec_from_file_location("j26_report", SCRIPT)
j26_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(j26_report)

EXPECTED_REQUIRED_CASES = {
    j26_report.READ_VIEW_IT: {"readsSelectedPendingVersionsAndHistoricalRowsInBothLayouts"},
    j26_report.SNAPSHOT_PIN_IT: {"independentManagersShareDurablePinsFloorsAndExpiryFencing"},
    j26_report.READ_VIEW_TEST: {
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
    j26_report.SNAPSHOT_PIN_TEST: {
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
        "corruptedDurableShardFailsClosed",
        "completionCasLosingToDurableRevocationCannotReportSuccess",
    },
    j26_report.INITIALIZATION_TEST: {
        "readsBothPublishedCandidatesRetainsTheAlternativeLayoutAndOwnsNoInput",
        "systemTableRequiresSnapshotPinCategory",
    },
}


class J26ReportTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        subprocess.run(["git", "-C", str(self.root), "config", "user.name", "J26 report test"], check=True)
        subprocess.run(["git", "-C", str(self.root), "config", "user.email", "j26-report@example.invalid"], check=True)
        marker = self.root / "tracked.txt"
        marker.write_text("evidence source\n")
        subprocess.run(["git", "-C", str(self.root), "add", "tracked.txt"], check=True)
        subprocess.run(["git", "-C", str(self.root), "commit", "-qm", "fixture"], check=True)
        self.output = self.root / "report"
        self.output.mkdir()
        self._write_reports()

    def _write_suite(self, module, suite_name, case_names, invalid_case=None, tests_count=None):
        report_kind = "failsafe-reports" if module == "hbase" else "surefire-reports"
        path = self.root / module / "target" / report_kind / f"TEST-{suite_name}.xml"
        path.parent.mkdir(parents=True, exist_ok=True)
        suite = ET.Element(
            "testsuite",
            name=suite_name,
            tests=str(len(case_names) if tests_count is None else tests_count),
            failures="0",
            errors="0",
            skipped="0",
        )
        for index, case_name in enumerate(case_names):
            case = ET.SubElement(suite, "testcase", name=case_name)
            if invalid_case and index == 0:
                ET.SubElement(case, invalid_case)
        if suite_name == j26_report.SNAPSHOT_PIN_IT:
            ET.SubElement(suite, "system-out").text = (
                "j26-snapshot-pin sharedRows=true floorsDurable=true expiryRequiresRevocation=true"
            )
        ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)
        return path

    def _reset_output(self):
        shutil.rmtree(self.output)
        self.output.mkdir()

    def _write_reports(self, omit_case=None, invalid_suite=None, invalid_case=None, extra_cases=(), tests_count=None):
        for suite_name in (j26_report.READ_VIEW_IT, j26_report.SNAPSHOT_PIN_IT):
            cases = set(EXPECTED_REQUIRED_CASES[suite_name])
            if omit_case and omit_case[0] == suite_name:
                cases.remove(omit_case[1])
            self._write_suite(
                "hbase", suite_name, sorted(cases),
                invalid_case if invalid_suite == suite_name else None,
                tests_count if invalid_suite == suite_name else None,
            )
        for suite_name in (j26_report.READ_VIEW_TEST, j26_report.SNAPSHOT_PIN_TEST, j26_report.INITIALIZATION_TEST):
            cases = set(EXPECTED_REQUIRED_CASES[suite_name])
            if omit_case and omit_case[0] == suite_name:
                cases.remove(omit_case[1])
            if suite_name == j26_report.SNAPSHOT_PIN_TEST:
                cases.update(extra_cases)
            self._write_suite(
                "core" if suite_name.startswith("io.etcdhbase.core.") else "admin",
                suite_name,
                sorted(cases),
                invalid_case if invalid_suite == suite_name else None,
                tests_count if invalid_suite == suite_name else None,
            )

    def test_collect_accepts_extra_successful_cases_and_preserves_minicluster_marker(self):
        self._reset_output()
        self._write_reports(extra_cases={"futurePinRegression"})
        j26_report.collect(self.root, self.output)
        summary = json.loads((self.output / "summary.json").read_text())
        self.assertEqual("J26", summary["phase"])
        self.assertEqual("public-local-minicluster", summary["scope"])
        self.assertEqual(len(j26_report.REQUIRED_CASES[j26_report.SNAPSHOT_PIN_TEST]) + 1,
                         summary["core"]["snapshot_pin"]["tests"])
        self.assertTrue((self.output / f"TEST-{j26_report.SNAPSHOT_PIN_IT}.xml").is_file())

    def test_required_cases_match_independent_inventory(self):
        self.assertEqual(EXPECTED_REQUIRED_CASES, j26_report.REQUIRED_CASES)

    def _assert_report_rejects_missing_case(self, suite_name, case_name):
        self._reset_output()
        self._write_reports(omit_case=(suite_name, case_name))
        with self.assertRaises(ValueError):
            j26_report.collect(self.root, self.output)
        self.assertFalse((self.output / "summary.json").exists())

    def test_report_rejects_each_missing_required_case(self):
        for suite_name, case_names in EXPECTED_REQUIRED_CASES.items():
            for case_name in sorted(case_names):
                with self.subTest(suite=suite_name, case=case_name):
                    self._assert_report_rejects_missing_case(suite_name, case_name)

    def test_report_rejects_suite_counter_contradiction(self):
        self._reset_output()
        self._write_reports(invalid_suite=j26_report.SNAPSHOT_PIN_TEST, tests_count=999)
        with self.assertRaises(ValueError):
            j26_report.collect(self.root, self.output)
        self.assertFalse((self.output / "summary.json").exists())

    def test_report_rejects_failure_error_and_skip_children_with_zero_suite_counters(self):
        for result_tag in ("failure", "error", "skipped"):
            with self.subTest(result_tag=result_tag):
                self._reset_output()
                self._write_reports(invalid_suite=j26_report.READ_VIEW_TEST, invalid_case=result_tag)
                with self.assertRaises(ValueError):
                    j26_report.collect(self.root, self.output)
                self.assertFalse((self.output / "summary.json").exists())


if __name__ == "__main__":
    unittest.main()
