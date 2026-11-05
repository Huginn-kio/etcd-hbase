#!/usr/bin/env python3
"""Controls for J29 MiniCluster evidence and its required measurement rows."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET


SPEC = importlib.util.spec_from_file_location("j29_report", Path(__file__).with_name("j29-report.py"))
j29_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(j29_report)

SUITE = "io.etcdhbase.hbase.storage.M2CostBenchmarkIT"
CASE = "reportsCommitReadAndSimulatedRenewalCostsForBothLayouts"
MARKERS = ("j29-commit-baseline", "j29-read", "j29-queue", "j29-layout", "j29-resource")
SOURCES = (
    "hbase/pom.xml",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostBenchmarkIT.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostFairQueue.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostMetrics.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostMetricsTest.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostPinObservations.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostReadProfile.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostRecoveryInterlock.java",
    "hbase/src/test/java/io/etcdhbase/hbase/storage/M2CostRuntimeSampler.java",
    "scripts/verify/j29-report.py", "scripts/verify/test_j29_report.py",
    "scripts/verify/evidence.py", "scripts/verify/test_evidence.py", "scripts/verify/j29.sh",
    "docs/testing/j29-public-cost-report.md",
)


def measurement_lines():
    lines = []
    for layout in ("HISTORY", "NATIVE"):
        common = {"layout": layout, "seed": 20261104, "workloadSha256": "fixture-digest"}
        rows = {
            "j29-commit-baseline": {"samples": [{"kind": kind} for kind in
                ("SHORT_KV", "LARGE_KV", "SIMULATED_RENEWAL")]},
            "j29-read": {"returnedKeys": 8, "bucketCount": 4, "pageRequests": 12,
                "pageReopens": 8, "pagesWhileRecoveryActive": 8,
                "recoveryMaterializationCalls": 8, "recoveryElapsedMicros": 1,
                "recoveryPending": True, "partiallyMaterialized": True,
                "pinRegisterCalls": 1, "pinRenewCalls": 1, "pinCompleteCalls": 1,
                "peakPinOccupancy": 1},
            "j29-queue": {"submitted": 8, "samples": [{} for _ in range(8)],
                "classSwitches": 1, "maxConsecutiveBusiness": 3, "maxConsecutiveRenewals": 1},
            "j29-layout": {"hbaseVersion": "fixture", "java": "21", "bucketCount": 4,
                "publishedSequence": 12, "publishedRevision": 10,
                "metrics": {name: {"unknown": 0, "failed": 0, "conflicts": 0} for name in
                    ("control.cas", "system.CANDIDATE.create", "system.MANIFEST.create",
                     "system.DECISION.create", "system.SNAPSHOT_PIN.cas",
                     "materialize.preflight", "materialize.apply")}},
            "j29-resource": {"activePinsAfterRelease": 0, "peakActiveScanners": 1,
                "pinCapacity": 32, "sampledPeakHeapBytes": 1, "sampledPeakDirectBufferBytes": 1,
                "sampledPeakThreadCount": 1, "peakLogicalCandidatePayloadBytes": 1},
        }
        lines.extend(f"{marker} {json.dumps(common | row)}" for marker, row in rows.items())
    return lines


class J29ReportTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.output = self.root / "output"
        self.output.mkdir()
        for name in SOURCES:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n")
        self.write_report()

    def write_report(self, *, counters=None, case_name=CASE, child=None, extra_case=False,
                     lines=None, suite_name=SUITE, kind="failsafe-reports"):
        path = self.root / "hbase" / "target" / kind / f"TEST-{SUITE}.xml"
        path.parent.mkdir(parents=True, exist_ok=True)
        counts = {"tests": "2" if extra_case else "1", "failures": "0", "errors": "0", "skipped": "0"}
        counts.update(counters or {})
        suite = ET.Element("testsuite", name=suite_name, **counts)
        case = ET.SubElement(suite, "testcase", name=case_name)
        if child:
            ET.SubElement(case, child)
        ET.SubElement(case, "system-out").text = "\n".join(measurement_lines() if lines is None else lines)
        if extra_case:
            ET.SubElement(suite, "testcase", name="additionalPassingCase")
        ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)
        return path

    def assert_rejected(self):
        (self.output / "summary.json").unlink(missing_ok=True)
        with self.assertRaises(ValueError):
            j29_report.collect(self.root, self.output)
        self.assertFalse((self.output / "summary.json").exists())

    def test_accepts_required_measurements_and_additional_passing_case(self):
        self.write_report(extra_case=True)
        j29_report.collect(self.root, self.output)
        summary = json.loads((self.output / "summary.json").read_text())
        self.assertEqual(2, summary["minicluster_test"]["tests"])
        self.assertEqual({"HISTORY", "NATIVE"}, set(summary["profiles"]))

    def test_rejects_failure_error_and_skip_counters(self):
        for key in ("failures", "errors", "skipped"):
            with self.subTest(counter=key):
                self.write_report(counters={key: "1"})
                self.assert_rejected()

    def test_rejects_contradictory_case_results_and_counts(self):
        for child in ("failure", "error", "skipped"):
            with self.subTest(child=child):
                self.write_report(child=child)
                self.assert_rejected()
        self.write_report(extra_case=True, counters={"tests": "1"})
        self.assert_rejected()

    def test_rejects_missing_required_case_or_wrong_suite(self):
        self.write_report(case_name="differentCase")
        self.assert_rejected()
        self.write_report(suite_name="example.OtherIT")
        self.assert_rejected()

    def test_rejects_each_missing_measurement_or_layout(self):
        lines = measurement_lines()
        for marker in MARKERS:
            for missing in (marker + " ", marker + ' {"layout": "HISTORY"'):
                with self.subTest(missing=missing):
                    self.write_report(lines=[line for line in lines if not line.startswith(missing)])
                    self.assert_rejected()

    def test_rejects_duplicate_layout_measurement(self):
        lines = measurement_lines()
        self.write_report(lines=lines + [lines[0]])
        self.assert_rejected()

    def test_does_not_accept_measurements_from_another_case(self):
        path = self.write_report(extra_case=True)
        suite = ET.parse(path).getroot()
        required, other = suite.findall("testcase")
        stdout = required.find("system-out")
        required.remove(stdout)
        other.append(stdout)
        ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)
        self.assert_rejected()

    def test_rejects_required_suite_in_unit_report_directory(self):
        self.write_report().unlink()
        self.write_report(kind="surefire-reports")
        self.assert_rejected()


if __name__ == "__main__":
    unittest.main()
