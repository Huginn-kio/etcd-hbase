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

    def test_method_contract_and_source_drift_are_rejected(self):
        original = self.args.cases.read_text()
        mutations = (
            (lambda document: document["descriptor_methods"].pop(), "descriptor coverage mismatch"),
            (lambda document: document["descriptor_methods"].append(document["descriptor_methods"][0]),
             "duplicate method entries"),
            (lambda document: document["additional_methods"][0].update(source_revision="unfixed"),
             "lacks a fixed upstream source"),
            (lambda document: document["additional_methods"][0].update(server_streaming=True),
             "streaming shape differs"),
            (lambda document: document["descriptor_methods"][0].update(error_policy=""),
             "no explicit rejection/error policy"),
        )
        for mutate, expected_error in mutations:
            with self.subTest(expected_error=expected_error):
                document = json.loads(original)
                mutate(document)
                self.args.cases.write_text(json.dumps(document))
                with self.assertRaisesRegex(matrix.MatrixError, expected_error):
                    matrix.validate(self.args)

    def test_case_references_and_usage_baseline_are_enforced(self):
        original = self.args.cases.read_text()
        mutations = (
            (lambda case: case.update(methods=[]), "has no RPC method mapping"),
            (lambda case: case.update(methods=["unknown.Service/Call"]), "references unknown methods"),
            (lambda case: case.update(methods=["etcdserverpb.KV/Compact"]), "does not map back to method"),
            (lambda case: case.update(usage_ids=["U99"]), "references unknown usage IDs"),
            (lambda case: case.update(usage_ids=["U01", "U01"]), "duplicate usage IDs"),
            (lambda case: case.update(verification_state="assumed-pass"), "invalid verification_state"),
        )
        for mutate, expected_error in mutations:
            with self.subTest(expected_error=expected_error):
                document = json.loads(original)
                mutate(document["cases"][0])
                self.args.cases.write_text(json.dumps(document))
                with self.assertRaisesRegex(matrix.MatrixError, expected_error):
                    matrix.validate(self.args)

    def test_count_policy_requires_both_success_allowances(self):
        path = self.args.cases.parent.parent / "count-policy.json"
        matrix.validate_count_policy(path)
        original = path.read_text()
        for difference in json.loads(original)["accepted_differences"]:
            with self.subTest(missing_rule=difference["rule_id"]):
                document = json.loads(original)
                document["accepted_differences"].remove(difference)
                path.write_text(json.dumps(document))
                with self.assertRaisesRegex(matrix.MatrixError, "accepted Count differences"):
                    matrix.validate_count_policy(path)

    def test_count_exceptions_cannot_expand_beyond_success_count(self):
        path = self.args.cases.parent.parent / "count-policy.json"
        original = path.read_text()
        mutations = ({"case_ids": ["C04"]}, {"field": "Header"}, {"method": "etcdserverpb.KV/Put"},
                     {"preserved_fields": ["Kvs", "More", "Header", "revision"]},
                     {"successful_responses_only": False}, {"target_count": "total_count"})
        for rule_index in (0, 1):
            for change in mutations:
                with self.subTest(rule_index=rule_index, change=change):
                    document = json.loads(original)
                    document["accepted_differences"][rule_index].update(change)
                    path.write_text(json.dumps(document))
                    with self.assertRaisesRegex(matrix.MatrixError, "accepted Count differences"):
                        matrix.validate_count_policy(path)
        for invalid_rules in ([], None, {}, [None, None], [json.loads(original)["accepted_differences"][0]] * 2):
            with self.subTest(invalid_rules=invalid_rules):
                document = json.loads(original)
                document["accepted_differences"] = invalid_rules
                path.write_text(json.dumps(document))
                with self.assertRaisesRegex(matrix.MatrixError, "accepted Count differences"):
                    matrix.validate_count_policy(path)

    def test_descriptor_duplicates_and_missing_message_inventory_are_rejected(self):
        original = self.args.wire_contract.read_text()
        document = json.loads(original)
        source = next(source for source in document["files"] if source.get("services"))
        service = next(service for service in source["services"] if service.get("methods"))
        service["methods"].append(service["methods"][0])
        self.args.wire_contract.write_text(json.dumps(document))
        with self.assertRaisesRegex(matrix.MatrixError, "duplicate descriptor method"):
            matrix.validate(self.args)
        document = json.loads(original)
        for source in document["files"]:
            source["messages"] = []
        self.args.wire_contract.write_text(json.dumps(document))
        with self.assertRaisesRegex(matrix.MatrixError, "has no field inventory"):
            matrix.validate(self.args)


if __name__ == "__main__":
    unittest.main()
