#!/usr/bin/env python3
"""Controls for J28.1 history and regression evidence."""
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET


SPEC = importlib.util.spec_from_file_location("j28_report", Path(__file__).with_name("j28-report.py"))
j28_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(j28_report)

MODEL = "io.etcdhbase.core.commit.KernelHistoryModelTest"
RECOVERY = "io.etcdhbase.core.commit.RecoveryManagerTest"
READ_VIEW = "io.etcdhbase.core.commit.ReadViewRecordStoreTest"
EXPECTED_CASES = {
    MODEL: {
        "selectedUnknownTransactionIsVisibleAcrossCancellationAndRecovery",
        "unresolvedUnknownMayHaveTakenEffectOrMayNotHaveTakenEffect",
        "unknownCompareMismatchMayHaveNoEffect",
        "unknownNotSelectedCannotWrite",
        "selectedUnknownMayDecideAfterCallerReturns",
        "unresolvedUnknownMayDecideAfterCallerReturns",
        "completedAndSelectedOutcomesKeepTheirTimeAndCompareConstraints",
        "cancelledCallerMayObserveAServiceDecisionLater",
        "successfulOutcomeConstrainsAnUnknownPublicationObservation",
        "failedCompareConstrainsAnUnknownPublicationObservation",
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


class J28ReportTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        subprocess.run(["git", "-C", str(self.root), "config", "user.name", "J28 report test"], check=True)
        subprocess.run(["git", "-C", str(self.root), "config", "user.email", "j28-report@example.invalid"], check=True)
        (self.root / "tracked.txt").write_text("evidence source\n")
        subprocess.run(["git", "-C", str(self.root), "add", "tracked.txt"], check=True)
        subprocess.run(["git", "-C", str(self.root), "commit", "-qm", "fixture"], check=True)
        for name in ("core/src/test/java/io/etcdhbase/core/commit/KernelHistoryModelTest.java",
                     "scripts/verify/j28-report.py", "scripts/verify/j28.sh",
                     "scripts/verify/test_j28_report.py", "scripts/verify/evidence.py",
                     "scripts/verify/test_evidence.py", "docs/testing/j28-kernel-history.md"):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n")
        self.output = self.root / "output"
        self.output.mkdir()
        self.write_reports()

    def write_suite(self, name, cases, kind="surefire-reports", child_tag=None, tests_count=None):
        path = self.root / "core" / "target" / kind / f"TEST-{name}.xml"
        path.parent.mkdir(parents=True, exist_ok=True)
        suite = ET.Element("testsuite", name=name, tests=str(len(cases) if tests_count is None else tests_count),
                           failures="0", errors="0", skipped="0")
        for index, case_name in enumerate(sorted(cases)):
            case = ET.SubElement(suite, "testcase", name=case_name)
            if index == 0 and child_tag:
                ET.SubElement(case, child_tag)
        ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)

    def write_reports(self):
        for name, cases in EXPECTED_CASES.items():
            self.write_suite(name, cases)

    def assert_rejected(self):
        (self.output / "summary.json").unlink(missing_ok=True)
        with self.assertRaises(ValueError):
            j28_report.collect(self.root, self.output)
        self.assertFalse((self.output / "summary.json").exists())

    def test_accepts_additional_passing_cases_and_preserves_unit_totals(self):
        self.write_suite(READ_VIEW, EXPECTED_CASES[READ_VIEW] | {"additionalPassingCase"})
        self.write_suite("example.ExtraIT", {"integrationCase"}, kind="failsafe-reports")
        j28_report.collect(self.root, self.output)
        summary = json.loads((self.output / "summary.json").read_text())
        self.assertEqual(5, summary["read_view_regression"]["tests"])
        self.assertEqual(sum(map(len, EXPECTED_CASES.values())) + 1,
                         summary["core_tests"]["tests"])

    def test_rejects_each_missing_required_case(self):
        for name, cases in EXPECTED_CASES.items():
            for case in sorted(cases):
                with self.subTest(suite=name, case=case):
                    self.write_reports()
                    self.write_suite(name, (cases - {case}) | {"differentCase"})
                    self.assert_rejected()

    def test_rejects_contradictory_case_results_and_counts(self):
        for name in EXPECTED_CASES:
            for tag in ("failure", "error", "skipped"):
                with self.subTest(suite=name, result=tag):
                    self.write_reports()
                    self.write_suite(name, EXPECTED_CASES[name], child_tag=tag)
                    self.assert_rejected()
        self.write_reports()
        self.write_suite(RECOVERY, EXPECTED_CASES[RECOVERY], tests_count=999)
        self.assert_rejected()

    def test_rejects_suite_in_wrong_report_directory(self):
        source = self.root / "core/target/surefire-reports" / f"TEST-{MODEL}.xml"
        target = self.root / "core/target/failsafe-reports" / source.name
        target.parent.mkdir(parents=True)
        shutil.move(source, target)
        self.assert_rejected()

    def test_rejects_required_suite_only_present_in_integration_reports(self):
        self.write_suite(MODEL, EXPECTED_CASES[MODEL], kind="failsafe-reports")
        path = self.root / "core/target/surefire-reports" / f"TEST-{MODEL}.xml"
        suite = ET.parse(path).getroot()
        suite.set("name", "example.UnrelatedTest")
        ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)
        self.assert_rejected()

    def test_rejects_failure_in_an_additional_core_suite(self):
        self.write_suite("example.AdditionalTest", {"case"}, child_tag="failure")
        self.assert_rejected()


if __name__ == "__main__":
    unittest.main()
