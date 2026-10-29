#!/usr/bin/env python3
"""Reject descriptor drift and missing API/case coverage."""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("api_matrix", Path(__file__).with_name("api-matrix.py"))
matrix = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(matrix)
ROOT = Path(__file__).resolve().parents[2]


class APIMatrixTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        for relative in ("docs/api-matrix.md", "docs/milvus-etcd-usage.md", "protocol/baseline/wire-contract.json",
                         "testkit/cases/m0-wire.json", "testkit/count-policy.json"):
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, destination)
        self.args = argparse.Namespace(matrix=root / "docs/api-matrix.md", wire_contract=root / "protocol/baseline/wire-contract.json", cases=root / "testkit/cases/m0-wire.json")

    def test_inventory_matches_all_fields_and_rejects_descriptor_drift(self):
        self.assertEqual((41, 7, 33, 48), matrix.validate(self.args))
        data = json.loads(self.args.cases.read_text())
        data["descriptor_methods"][0]["server_streaming"] = not data["descriptor_methods"][0]["server_streaming"]
        self.args.cases.write_text(json.dumps(data))
        with self.assertRaises(matrix.MatrixError):
            matrix.validate(self.args)

    def test_missing_and_duplicate_cases_are_rejected(self):
        original = json.loads(self.args.cases.read_text())
        for cases in (original["cases"][:-1], original["cases"] + [original["cases"][0]]):
            data = dict(original, cases=cases)
            self.args.cases.write_text(json.dumps(data))
            with self.assertRaises(matrix.MatrixError):
                matrix.validate(self.args)


if __name__ == "__main__":
    unittest.main()
