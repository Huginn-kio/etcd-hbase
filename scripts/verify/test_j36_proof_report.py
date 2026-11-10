#!/usr/bin/env python3
"""Negative checks for the component evidence gate; fixtures are not test results."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

spec = importlib.util.spec_from_file_location('j36_proof_report', Path(__file__).with_name('j36-proof-report.py'))
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class ProofReportTest(unittest.TestCase):
    def test_missing_or_skipped_component_test_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for required, kind in ((report.UNITS, 'surefire-reports'), (report.INTEGRATION, 'failsafe-reports')):
                for suite, cases in required.items():
                    module = 'hbase' if '.hbase.' in suite else 'server' if '.server.' in suite else 'core'
                    target = root / module / 'target' / kind
                    target.mkdir(parents=True, exist_ok=True)
                    xml = ET.Element('testsuite', name=suite, tests=str(len(cases)), failures='0', errors='0', skipped='0')
                    for name in sorted(cases):
                        ET.SubElement(xml, 'testcase', name=name)
                    ET.ElementTree(xml).write(target / f'TEST-{suite}.xml')
            self.assertEqual(len(report.UNITS) + len(report.INTEGRATION), len(report.validate_reports(root)))
            path = root / 'server/target/failsafe-reports/TEST-io.etcdhbase.server.M3CacheProofIT.xml'
            original = path.read_text()
            path.write_text(original.replace('skipped="0"', 'skipped="1"'))
            with self.assertRaises(ValueError):
                report.validate_reports(root)
            path.write_text(original)
            xml = ET.parse(path)
            xml.getroot().remove(xml.getroot().find('testcase'))
            xml.getroot().set('tests', '1')
            xml.write(path)
            with self.assertRaises(ValueError):
                report.validate_reports(root)


if __name__ == '__main__':
    unittest.main()
