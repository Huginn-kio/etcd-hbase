import importlib.util
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

spec = importlib.util.spec_from_file_location("m1_report", Path(__file__).with_name("m1-report.py"))
reporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reporter)


class M1EvidenceTest(unittest.TestCase):
    def with_j22_report(self, include_case, verify):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "hbase/target/failsafe-reports/TEST-io.etcdhbase.hbase.storage.J22ManifestRepositoryIT.xml"
            report.parent.mkdir(parents=True)
            suite = ET.Element("testsuite", name="io.etcdhbase.hbase.storage.J22ManifestRepositoryIT",
                               tests="1", failures="0", errors="0", skipped="0")
            if include_case:
                ET.SubElement(suite, "testcase", name="persistsPagesEventsAndSealedRootOnDisposableHBase")
            else:
                ET.SubElement(suite, "testcase", name="unrelatedCase")
            ET.ElementTree(suite).write(report)
            verify(root)

    def test_j22_actual_hbase_case_is_required(self):
        self.with_j22_report(True, reporter.require_j22_manifest_case)

    def test_missing_j22_actual_hbase_case_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "required real J22 manifest repository case did not execute"):
            self.with_j22_report(False, reporter.require_j22_manifest_case)


if __name__ == "__main__":
    unittest.main()
