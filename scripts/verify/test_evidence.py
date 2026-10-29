#!/usr/bin/env python3
"""Negative controls for shared Java/Go evidence readers."""
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
from evidence import GO_MODULE, GO_PACKAGES, go_report, java_reports


class EvidenceTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def test_java_requires_actual_cases_and_rejects_skips(self):
        path = self.root / "core/target/surefire-reports/TEST-scope.xml"
        path.parent.mkdir(parents=True)
        suite = ET.Element("testsuite", name="scope", tests="1", failures="0", errors="0", skipped="0")
        ET.SubElement(suite, "testcase", name="cancel")
        ET.ElementTree(suite).write(path)
        self.assertEqual(1, len(java_reports(self.root, {"scope"})[1]))
        for invalid in ("missing",):
            with self.assertRaises(ValueError):
                java_reports(self.root, {invalid})
        ET.SubElement(suite.find("testcase"), "skipped")
        ET.ElementTree(suite).write(path)
        with self.assertRaises(ValueError):
            java_reports(self.root, {"scope"})

    def test_go_requires_every_package_and_executed_tests(self):
        path = self.root / "go.jsonl"
        events = []
        for package in sorted(GO_PACKAGES):
            events.extend([{"Action": "pass", "Package": GO_MODULE + package, "Test": "TestContract"},
                           {"Action": "pass", "Package": GO_MODULE + package}])
        def write(items):
            path.write_text("".join(json.dumps(item) + "\n" for item in items))
        write(events)
        self.assertEqual(len(GO_PACKAGES), go_report(path)["passed_test_events"])
        for damaged in (events[:-1], events + [{"Action": "skip"}], events + [events[-1]],
                        [event for event in events if "Test" not in event]):
            write(damaged)
            with self.assertRaises(ValueError):
                go_report(path)


if __name__ == "__main__":
    unittest.main()
