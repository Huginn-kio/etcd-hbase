#!/usr/bin/env python3
"""Negative controls for the J06 test-evidence gate."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

spec = importlib.util.spec_from_file_location("j06_report", Path(__file__).with_name("j06-report.py"))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class EvidenceGateTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.paths = []
        for index, name in enumerate(sorted(report.REQUIRED)):
            path = Path(self.directory.name) / f"suite-{index}.xml"
            suite = ET.Element("testsuite", name=name, tests="1", failures="0", errors="0", skipped="0")
            ET.SubElement(suite, "testcase", name="fixture-case")
            ET.ElementTree(suite).write(path)
            self.paths.append(path)

    def test_requires_every_critical_suite_and_real_cases(self):
        self.assertEqual(len(report.REQUIRED), len(report.inspect_reports(self.paths)))
        with self.assertRaises(ValueError):
            report.inspect_reports(self.paths[1:])
        with self.assertRaises(ValueError):
            report.inspect_reports(self.paths + [self.paths[0]])
        suite = ET.parse(self.paths[0]).getroot()
        suite.remove(suite.find("testcase"))
        ET.ElementTree(suite).write(self.paths[0])
        with self.assertRaises(ValueError):
            report.inspect_reports(self.paths)

    def test_rejects_empty_skipped_failed_and_inconsistent_evidence(self):
        for field, value in (("tests", "0"), ("skipped", "1"), ("failures", "1"), ("errors", "1")):
            with self.subTest(field=field):
                suite = ET.parse(self.paths[0]).getroot()
                original = suite.get(field)
                suite.set(field, value)
                ET.ElementTree(suite).write(self.paths[0])
                with self.assertRaises(ValueError):
                    report.inspect_reports(self.paths)
                suite.set(field, original)
                ET.ElementTree(suite).write(self.paths[0])
        suite = ET.parse(self.paths[0]).getroot()
        ET.SubElement(suite.find("testcase"), "skipped")
        ET.ElementTree(suite).write(self.paths[0])
        with self.assertRaises(ValueError):
            report.inspect_reports(self.paths)
        self.paths[0].write_text("<testsuite")
        with self.assertRaises(ET.ParseError):
            report.inspect_reports(self.paths)


if __name__ == "__main__":
    unittest.main()
