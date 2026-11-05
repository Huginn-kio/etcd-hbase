#!/usr/bin/env python3
"""Positive and negative controls for J27 recovery evidence."""
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET


SCRIPT = Path(__file__).with_name("j27-report.py")
SPEC = importlib.util.spec_from_file_location("j27_report", SCRIPT)
j27_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(j27_report)

RECOVERY_TEST = "io.etcdhbase.core.commit.RecoveryManagerTest"
FINALIZATION_TEST = "io.etcdhbase.core.commit.CommitFinalizationServiceTest"
RECOVERY_IT = "io.etcdhbase.hbase.storage.M2RecoveryIT"
HBASE_UNIT = "io.etcdhbase.hbase.storage.ExampleUnitTest"
EXPECTED_CASES = {
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


class J27ReportTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        subprocess.run(["git", "-C", str(self.root), "config", "user.name", "J27 report test"], check=True)
        subprocess.run(["git", "-C", str(self.root), "config", "user.email", "j27-report@example.invalid"], check=True)
        (self.root / "tracked.txt").write_text("evidence source\n")
        subprocess.run(["git", "-C", str(self.root), "add", "tracked.txt"], check=True)
        subprocess.run(["git", "-C", str(self.root), "commit", "-qm", "fixture"], check=True)
        self.output = self.root / "report"
        self._write_reports()

    def _write_suite(self, module, kind, name, case_names, result_tag=None, tests_count=None, marker=True):
        path = self.root / module / "target" / kind / f"TEST-{name}.xml"
        path.parent.mkdir(parents=True, exist_ok=True)
        suite = ET.Element("testsuite", name=name, tests=str(len(case_names) if tests_count is None else tests_count),
                           failures="0", errors="0", skipped="0")
        for index, case_name in enumerate(sorted(case_names)):
            case = ET.SubElement(suite, "testcase", name=case_name)
            if result_tag and index == 0:
                ET.SubElement(case, result_tag)
        if name == RECOVERY_IT and marker:
            ET.SubElement(suite, "system-out").text = (
                "j27-recovery durableSelectedSlot=true archiveAndFinalize=true revisionStable=true"
            )
        ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)

    def _write_reports(self, omit=None, invalid_suite=None, result_tag=None, tests_count=None,
                       extra_finalization=False, marker=True):
        for name, required in EXPECTED_CASES.items():
            cases = set(required)
            if omit and omit[0] == name:
                cases.remove(omit[1])
            if name == FINALIZATION_TEST and extra_finalization:
                cases.add("additionalPassingCase")
            module = "hbase" if name == RECOVERY_IT else "core"
            kind = "failsafe-reports" if name == RECOVERY_IT else "surefire-reports"
            self._write_suite(module, kind, name, cases,
                              result_tag if invalid_suite == name else None,
                              tests_count if invalid_suite == name else None, marker)
        self._write_suite("hbase", "surefire-reports", HBASE_UNIT, {"unitCase"},
                          result_tag if invalid_suite == HBASE_UNIT else None,
                          tests_count if invalid_suite == HBASE_UNIT else None)

    def _reset_output(self):
        shutil.rmtree(self.output, ignore_errors=True)
        self.output.mkdir()

    def _assert_rejected(self):
        with self.assertRaises(ValueError):
            j27_report.collect(self.root, self.output)
        self.assertFalse((self.output / "summary.json").exists())

    def test_accepts_current_and_additional_passing_finalization_cases(self):
        self._reset_output()
        self._write_reports(extra_finalization=True)
        j27_report.collect(self.root, self.output)
        summary = json.loads((self.output / "summary.json").read_text())
        self.assertEqual(5, summary["finalization_tests"]["tests"])
        self.assertEqual(1, summary["minicluster_recovery"]["tests"])
        self.assertEqual(1, summary["module_tests"]["hbase"]["tests"])

    def test_required_cases_match_independent_inventory(self):
        self.assertEqual(EXPECTED_CASES, j27_report.REQUIRED_CASES)

    def test_rejects_each_missing_required_case(self):
        for suite_name, case_names in EXPECTED_CASES.items():
            for case_name in sorted(case_names):
                with self.subTest(suite=suite_name, case=case_name):
                    self._reset_output()
                    self._write_reports(omit=(suite_name, case_name))
                    self._assert_rejected()

    def test_rejects_contradictory_testcase_results_and_counts(self):
        for suite_name in (RECOVERY_TEST, RECOVERY_IT, HBASE_UNIT):
            for result_tag in ("failure", "error", "skipped"):
                with self.subTest(suite=suite_name, result=result_tag):
                    self._reset_output()
                    self._write_reports(invalid_suite=suite_name, result_tag=result_tag)
                    self._assert_rejected()
        self._reset_output()
        self._write_reports(invalid_suite=RECOVERY_TEST, tests_count=999)
        self._assert_rejected()

    def test_rejects_missing_minicluster_marker(self):
        self._reset_output()
        self._write_reports(marker=False)
        self._assert_rejected()

    def test_rejects_required_suite_in_wrong_module(self):
        self._reset_output()
        source = self.root / "core" / "target" / "surefire-reports" / f"TEST-{RECOVERY_TEST}.xml"
        target = self.root / "hbase" / "target" / "surefire-reports" / source.name
        shutil.move(source, target)
        self._assert_rejected()


if __name__ == "__main__":
    unittest.main()
