#!/usr/bin/env python3
"""Negative controls for the J24 MiniCluster evidence reader."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET


REPORTER_PATH = Path(__file__).with_name("j24-report.py")
SPEC = importlib.util.spec_from_file_location("j24_report", REPORTER_PATH)
REPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORTER)


class J24ReportTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.report = (self.root / "hbase/target/failsafe-reports"
                       / f"TEST-{REPORTER.REQUIRED_SUITE}.xml")
        self.report.parent.mkdir(parents=True)

    def write_report(self, testcase_marker=None):
        suite = ET.Element("testsuite", name=REPORTER.REQUIRED_SUITE,
                           tests="1", failures="0", errors="0", skipped="0")
        testcase = ET.SubElement(suite, "testcase", name=REPORTER.REQUIRED_CASE)
        if testcase_marker:
            ET.SubElement(testcase, testcase_marker)
        output = ET.SubElement(suite, "system-out")
        output.text = "\n".join(
            f"j24-materialization layout={layout} idempotent=true lateWriterFenced=true"
            for layout in ("HISTORY", "NATIVE"))
        ET.ElementTree(suite).write(self.report)

    def test_accepts_one_passing_case_with_both_layout_markers(self):
        self.write_report()

        source, counts = REPORTER.inspect_report(self.root)

        self.assertEqual(self.report, source)
        self.assertEqual({"tests": 1, "failures": 0, "errors": 0, "skipped": 0}, counts)

    def test_rejects_testcase_skipped_or_failure_hidden_by_suite_counters(self):
        for marker in ("skipped", "failure"):
            with self.subTest(marker=marker):
                self.write_report(marker)

                with self.assertRaises(ValueError):
                    REPORTER.inspect_report(self.root)


if __name__ == "__main__":
    unittest.main()
