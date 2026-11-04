#!/usr/bin/env python3
"""Positive and negative controls for J25's MiniCluster evidence collector."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET


SCRIPT = Path(__file__).with_name("j25-report.py")
SPEC = importlib.util.spec_from_file_location("j25_report", SCRIPT)
j25_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(j25_report)


class J25ReportTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        subprocess.run(["git", "-C", str(self.root), "config", "user.name", "J25 report test"], check=True)
        subprocess.run(["git", "-C", str(self.root), "config", "user.email", "j25-report@example.invalid"], check=True)
        marker = self.root / "tracked.txt"
        marker.write_text("evidence source\n")
        subprocess.run(["git", "-C", str(self.root), "add", "tracked.txt"], check=True)
        subprocess.run(["git", "-C", str(self.root), "commit", "-qm", "fixture"], check=True)
        self.output = self.root / "report"
        self.output.mkdir()
        self._write_reports()

    def _write_suite(self, path, name, case_names, invalid=None):
        path.parent.mkdir(parents=True, exist_ok=True)
        suite = ET.Element("testsuite", name=name, tests=str(len(case_names)),
                           failures="0", errors="0", skipped="0")
        for case_name in case_names:
            case = ET.SubElement(suite, "testcase", name=case_name)
            if invalid and case_name == case_names[0]:
                ET.SubElement(case, invalid)
        ET.SubElement(suite, "system-out").text = (
            "j25-readview layout=HISTORY selectedPending=true historical=true "
            "deleteVisible=true unselectedIsolated=true\n"
            "j25-readview layout=NATIVE selectedPending=true historical=true "
            "deleteVisible=true unselectedIsolated=true"
        )
        ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)

    def _write_reports(self, invalid_suite=None, invalid_case=None):
        hbase_path = self.root / "hbase/target/failsafe-reports" / f"TEST-{j25_report.J25_SUITE}.xml"
        core_path = self.root / "core/target/surefire-reports" / f"TEST-{j25_report.CORE_SUITE}.xml"
        self._write_suite(hbase_path, j25_report.J25_SUITE, [j25_report.J25_CASE],
                          invalid_case if invalid_suite == "hbase" else None)
        self._write_suite(core_path, j25_report.CORE_SUITE,
                          sorted(j25_report.REQUIRED_CORE_CASES),
                          invalid_case if invalid_suite == "core" else None)

    def test_collect_accepts_required_cases_and_both_layout_markers(self):
        j25_report.collect(self.root, self.output)
        summary = json.loads((self.output / "summary.json").read_text())
        self.assertEqual(1, summary["tests"])
        self.assertEqual(4, summary["core_tests"])
        self.assertEqual("J25", summary["phase"])
        self.assertTrue(summary["source_commit"])
        self.assertTrue(summary["source_report_sha256"])

    def test_report_rejects_skipped_or_failed_required_case_with_zero_suite_counters(self):
        for suite in ("hbase", "core"):
            for case_result in ("skipped", "failure"):
                with self.subTest(suite=suite, case_result=case_result):
                    self._write_reports(suite, case_result)
                    with self.assertRaises(ValueError):
                        j25_report.collect(self.root, self.output)
                    self.assertFalse((self.output / "summary.json").exists())


if __name__ == "__main__":
    unittest.main()
