#!/usr/bin/env python3
"""Reject missing, skipped, contradictory or tampered aggregate delivery evidence."""
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
from m0_evidence import STEPS, index_files, validate_reproducible, validate_steps, verify_index, write_index
from evidence import M0_JAVA_MODULES, M0_REQUIRED_SUITES


class AggregateEvidenceTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def test_step_ledger_cannot_convert_missing_or_skipped_checks_into_passes(self):
        steps = [{"id": name, "status": "passed", "exit_code": 0} for name in STEPS]
        validate_steps(steps)
        image = next(item for item in steps if item["id"] == "runtime-image")
        image.update(status="unavailable", exit_code=2)
        validate_steps(steps)
        for invalid in (steps[:-1], [dict(item, status="skipped") for item in steps],
                        [dict(item, exit_code=0) if item["id"] == "runtime-image" else item for item in steps]):
            with self.assertRaises(ValueError):
                validate_steps(invalid)

    def build_reports(self):
        builds = []
        jars = {f"{name}/target/{name}-fixture.jar": "a" * 64 for name in M0_JAVA_MODULES}
        jars["hbase/target/hbase-fixture-tests.jar"] = "b" * 64
        for run in (1, 2):
            directory = self.root / f"reproducible/run-{run}"
            suites = {}
            for name in M0_REQUIRED_SUITES:
                module = name.split(".")[2]
                path = directory / f"reports/{module}/target/surefire-reports/TEST-{name}.xml"
                path.parent.mkdir(parents=True, exist_ok=True)
                counts = {"tests": 1, "failures": 0, "errors": 0, "skipped": 0}
                suite = ET.Element("testsuite", name=name, **{key: str(value) for key, value in counts.items()})
                ET.SubElement(suite, "testcase", name="contract")
                ET.ElementTree(suite).write(path)
                suites[name] = counts
            summary = {"run": run, "tests": len(suites), "failures": 0, "errors": 0, "skipped": 0, "suites": suites, "jars": jars}
            (directory / "summary.json").write_text(json.dumps(summary))
            builds.append({key: summary[key] for key in ("run", "tests", "failures", "errors", "skipped")})
        summary = {"checkout": "fixture", "tracked_dirty": False, "identical_jars": True,
                   "command": ["-B", "-ntp", "-Pm0-public", "clean", "install"], "builds": builds, "jars": jars}
        (self.root / "reproducible/summary.json").write_text(json.dumps(summary))

    def test_build_summary_needs_actual_xml_and_identical_artifact_inventories(self):
        self.build_reports()
        source = {"checkout": "fixture"}
        validate_reproducible(self.root, source)
        report = next((self.root / "reproducible/run-2/reports").rglob("TEST-*.xml"))
        original = report.read_bytes()
        suite = ET.fromstring(original)
        ET.SubElement(suite.find("testcase"), "skipped")
        ET.ElementTree(suite).write(report)
        with self.assertRaises(ValueError):
            validate_reproducible(self.root, source)
        report.write_bytes(original)
        summary_path = self.root / "reproducible/run-2/summary.json"
        summary = json.loads(summary_path.read_text())
        summary["jars"][next(iter(summary["jars"]))] = "changed"
        summary_path.write_text(json.dumps(summary))
        with self.assertRaises(ValueError):
            validate_reproducible(self.root, source)
        report.unlink()
        with self.assertRaises(ValueError):
            validate_reproducible(self.root, source)

    def test_index_rejects_tampered_unindexed_and_symlinked_files(self):
        (self.root / "summary.json").write_text('{"status":"failed"}')
        payload = self.root / "evidence.log"
        payload.write_text("original")
        before = write_index(self.root)["files"]
        payload.write_text("changed")
        self.assertNotEqual(before, index_files(self.root))
        with self.assertRaises(ValueError):
            verify_index(self.root)
        (self.root / "outside-link").symlink_to(payload)
        with self.assertRaises(ValueError):
            write_index(self.root)


if __name__ == "__main__":
    unittest.main()
