#!/usr/bin/env python3
"""Negative tests for the M3 report gate; fixtures do not constitute runtime evidence."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("m3-report.py")
SPEC = importlib.util.spec_from_file_location("m3_report", SCRIPT)
M3 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M3)


def metadata(scope, layout=""):
    return {
        "status": "passed", "evidence_scope": scope, "suite_sha256": "suite",
        "prefix_sha256": "prefix", "trace_sha256": "trace", "embedded_native": scope == "native-etcd",
        "layout": layout,
        "build": {
            "scenario_id": "m3-put-v1", "protocol": "etcd-v3.5.23",
            "source_revision": M3.PINNED_SOURCE, "go_version": "go1.27.2",
            "modules": [
                {"path": f"go.etcd.io/etcd/{name}/v3", "version": "v3.5.23"}
                for name in ("api", "client", "server")
            ],
        },
    }


class M3ReportTest(unittest.TestCase):
    def test_registry_records_future_coverage_without_claiming_test_results(self):
        registry = M3.read_json(M3.REPO / "testkit/cases/m3-cases.json")
        M3.validate_case_registry(registry)
        mutations = []
        incomplete = copy.deepcopy(registry)
        incomplete["tasks"] = incomplete["tasks"][:-1]
        mutations.append(incomplete)
        duplicate = copy.deepcopy(registry)
        duplicate["tasks"][1] = copy.deepcopy(duplicate["tasks"][0])
        mutations.append(duplicate)
        missing_filter = copy.deepcopy(registry)
        missing_filter["pending_native_probes"][0]["filters"].pop()
        mutations.append(missing_filter)
        wrong_shape = copy.deepcopy(registry)
        wrong_shape["pending_native_probes"][0]["shapes"] = [["invalid"], "exact-key"]
        mutations.append(wrong_shape)
        for invalid in mutations:
            with self.assertRaises(ValueError):
                M3.validate_case_registry(invalid)

    def test_rejects_mixed_runs_failed_status_wrong_version_and_forged_native(self):
        for scope in ("native-etcd", "target-hbase"):
            layout = "" if scope == "native-etcd" else "HISTORY"
            good = metadata(scope, layout)
            M3.validate_metadata(good, scope, layout, "suite", "prefix", "trace")
            mutations = [
                ("status", "failed"), ("evidence_scope", "transport-fixture"),
                ("prefix_sha256", "other-run"), ("trace_sha256", "corrupt"),
                ("suite_sha256", "different-suite"), ("embedded_native", scope != "native-etcd"),
                ("layout", "wrong-layout"),
            ]
            for key, value in mutations:
                bad = copy.deepcopy(good)
                bad[key] = value
                with self.assertRaises(ValueError):
                    M3.validate_metadata(bad, scope, layout, "suite", "prefix", "trace")
            bad = copy.deepcopy(good)
            bad["build"]["modules"][0]["version"] = "v3.6.0"
            with self.assertRaises(ValueError):
                M3.validate_metadata(bad, scope, layout, "suite", "prefix", "trace")

    def test_rejects_empty_missing_duplicate_and_wrong_scope_trace(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.jsonl"
            records = [{"schema": "etcd-hbase-trace/v1", "case_id": case,
                        "evidence_scope": "native-etcd", "method": "etcdserverpb.KV/Put"}
                       for case in M3.PUT_CASES]
            variants = [[], records[:-1], records + records[:1],
                        [{**record, "evidence_scope": "target-hbase"} for record in records]]
            for variant in variants:
                path.write_text("".join(json.dumps(record) + "\n" for record in variant))
                with self.assertRaises(ValueError):
                    M3.validate_trace_cases(path, "native-etcd")
            path.write_text("".join(json.dumps(record) + "\n" for record in records))
            M3.validate_trace_cases(path, "native-etcd")

    def test_txn_trace_requires_each_registered_case_and_exact_method(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.jsonl"
            records = [{"schema": "etcd-hbase-trace/v1", "case_id": case,
                        "evidence_scope": "native-etcd",
                        "method": "etcdserverpb.KV/Put" if index == 0 else "etcdserverpb.KV/Txn"}
                       for index, case in enumerate(M3.TXN_CASES)]
            path.write_text("".join(json.dumps(record) + "\n" for record in records))
            M3.validate_trace_cases(path, "native-etcd", "J31")
            for invalid in (records[:-1], records + records[:1],
                            [records[0], {**records[1], "method": "etcdserverpb.KV/Put"}, *records[2:]]):
                path.write_text("".join(json.dumps(record) + "\n" for record in invalid))
                with self.assertRaises(ValueError):
                    M3.validate_trace_cases(path, "native-etcd", "J31")
            observed = metadata("native-etcd")
            with self.assertRaises(ValueError):
                M3.validate_metadata(observed, "native-etcd", "", "suite", "prefix", "trace", "m3-txn-v1")
            observed["build"]["scenario_id"] = "m3-txn-v1"
            M3.validate_metadata(observed, "native-etcd", "", "suite", "prefix", "trace", "m3-txn-v1")

    def test_delete_trace_requires_the_correct_method_for_each_case(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.jsonl"
            records = [{"schema": "etcd-hbase-trace/v1", "case_id": case,
                        "evidence_scope": "native-etcd", "method": M3.DELETE_METHODS[index]}
                       for index, case in enumerate(M3.DELETE_CASES)]
            path.write_text("".join(json.dumps(record) + "\n" for record in records))
            M3.validate_trace_cases(path, "native-etcd", "J33")
            records[1]["method"] = "etcdserverpb.KV/Txn"
            path.write_text("".join(json.dumps(record) + "\n" for record in records))
            with self.assertRaises(ValueError):
                M3.validate_trace_cases(path, "native-etcd", "J33")

    def test_txn_run_requires_named_unit_and_real_integration_cases(self):
        for task in ("J31", "J32", "J33"):
            with self.subTest(task=task):
                self.assert_txn_run_requires_cases(task)

    def assert_txn_run_requires_cases(self, task):
        with tempfile.TemporaryDirectory() as directory:
            root, repo, source = create_txn_report_fixture(Path(directory), task)
            with mock.patch.object(M3.subprocess, "check_output", side_effect=[source + "\n", ""]), \
                    mock.patch.object(M3.subprocess, "run"):
                self.assertEqual([task], M3.validate_run(root, repo)["tasks"])
            path = root / "core/target/surefire-reports/TEST-io.etcdhbase.core.commit.InvocationServiceTest.xml"
            path.write_text(path.read_text().replace('skipped="0"', 'skipped="1"'))
            with mock.patch.object(M3.subprocess, "check_output", side_effect=[source + "\n", ""]), \
                    mock.patch.object(M3.subprocess, "run"):
                with self.assertRaises(ValueError):
                    M3.validate_run(root, repo)
            path.write_text(path.read_text().replace('skipped="1"', 'skipped="0"'))
            go_report = root / "go-test.jsonl"
            for case in ("TestM3TxnCountAllowanceRejectsMatchingMalformedOneof",
                         "TestM3TxnCountAllowanceRejectsMalformedStrictSibling"):
                self.assert_missing_txn_case_rejected(root, repo, source, go_report, case)
            readview = root / "core/target/surefire-reports/TEST-io.etcdhbase.core.commit.ReadViewRecordStoreTest.xml"
            for case in ("flatTransactionCanWriteInElse", "readOnlyTransactionNeedsNoInvocation"):
                self.assert_missing_txn_case_rejected(root, repo, source, readview, case)
            if task in {"J32", "J33"}:
                workset = root / "server/target/failsafe-reports/TEST-io.etcdhbase.server.M3TxnCommitIT.xml"
                self.assert_missing_txn_case_rejected(root, repo, source, workset,
                    "nestedWorksetExceedsMemoryAndDoesNotReplayAnUncertainPageOnBothLayouts")
                hbase = root / f"hbase/target/failsafe-reports/TEST-{M3.HBASE_SUITE}.xml"
                self.assert_missing_txn_case_rejected(root, repo, source, hbase,
                    "noChangeResultSurvivesLostCasAcknowledgementAndRejectsLateSeal")
            if task == "J33":
                self.assert_missing_txn_case_rejected(root, repo, source, workset,
                    "largeDeleteIsAtomicBudgetedAndReplayableOnBothLayouts")
                self.assert_missing_txn_case_rejected(root, repo, source, go_report,
                    "TestM3DeleteComparisonKeepsTxnCountOneofsStrict")

    def assert_missing_txn_case_rejected(self, root, repo, source, report, case):
        original = report.read_text()
        self.assertIn(case, original)
        report.write_text(original.replace(case, "missingCase"))
        try:
            with mock.patch.object(M3.subprocess, "check_output", side_effect=[source + "\n", ""]), \
                    mock.patch.object(M3.subprocess, "run"):
                with self.assertRaises(ValueError):
                    M3.validate_run(root, repo)
        finally:
            report.write_text(original)

    def test_go_report_requires_named_passes_and_no_skips(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "go.jsonl"
            events = []
            for package, cases in M3.GO_REQUIRED_CASES.items():
                name = M3.GO_MODULE + package
                events.extend({"Action": "pass", "Package": name, "Test": case} for case in cases)
                events.append({"Action": "pass", "Package": name})
            path.write_text("".join(json.dumps(event) + "\n" for event in events))
            M3.validate_go_report(path)
            path.write_text(path.read_text() + json.dumps({"Action": "skip", "Package": "x"}) + "\n")
            with self.assertRaises(ValueError):
                M3.validate_go_report(path)
            path.write_text("")
            with self.assertRaises(ValueError):
                M3.validate_go_report(path)

    def test_rejects_duplicate_keys_and_missing_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "duplicate.json").write_text('{"scope":"public","scope":"target"}')
            with self.assertRaises(ValueError):
                M3.read_json(root / "duplicate.json")
            with self.assertRaises(ValueError):
                M3.validate_run(root)

    def test_report_requires_real_named_java_cases_and_zero_skips(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = create_report_fixture(Path(directory))
            self.assertEqual(["J30"], validate_report(fixture)["tasks"])
            for suite in (M3.INIT_SUITE, M3.GROUP_RUNTIME_SUITE, M3.RUNTIME_SUITE):
                report = fixture.report(suite)
                saved = report.read_text()
                report.unlink()
                self.assert_report_rejected(fixture)
                report.write_text(saved)
            server_report = fixture.report(M3.SERVER_SUITE)
            server_report.write_text(server_report.read_text().replace('skipped="0"', 'skipped="1"'))
            self.assert_report_rejected(fixture)
            server_report.unlink()
            self.assert_report_rejected(fixture)

    def assert_report_rejected(self, fixture):
        with self.assertRaises(ValueError):
            validate_report(fixture)


def create_txn_report_fixture(parent, task):
    root, repo = parent / "run-1", parent / "repo"
    root.mkdir()
    suite_name = {"J31": "m3-txn.yaml", "J32": "m3-nested-txn.yaml", "J33": "m3-delete.yaml"}[task]
    suite = repo / "testkit/scenarios" / suite_name
    suite.parent.mkdir(parents=True)
    suite.write_text("synthetic-validator-test-suite")
    registry = repo / "testkit/cases/m3-cases.json"
    registry.parent.mkdir(parents=True)
    registry.write_text((M3.REPO / "testkit/cases/m3-cases.json").read_text())
    source = "a" * 40
    (root / "run.json").write_text(json.dumps({
        "schema": "etcd-hbase-m3-run/v1", "scope": "public-local-minicluster",
        "run_id": root.name, "tasks": [task], "source_commit": source, "commands_passed": True,
    }))
    write_txn_trace_metadata(root, suite, source, task)
    write_txn_go_report(root, task)
    write_txn_java_reports(root, task)
    return root, repo, source


def write_txn_trace_metadata(root, suite, source, task):
    prefix = {"J31": "j31", "J32": "j32", "J33": "j33"}[task]
    scenario_id = {"J31": "m3-txn-v1", "J32": "m3-nested-txn-v1", "J33": "m3-delete-v1"}[task]
    suite_sha = M3.file_digest(suite)
    prefix_sha = hashlib.sha256(b"etcd-hbase-m3-test/run-1/").hexdigest()
    for name, scope, layout in (("native", "native-etcd", ""), ("target-history", "target-hbase", "HISTORY"),
                                ("target-native", "target-hbase", "NATIVE")):
        trace = root / f"{prefix}-{name}-trace.jsonl"
        trace.write_text("".join(json.dumps({"schema": "etcd-hbase-trace/v1", "case_id": case,
            "evidence_scope": scope, "method": M3.DELETE_METHODS[index] if task == "J33" else
            "etcdserverpb.KV/Put" if index == 0 else "etcdserverpb.KV/Txn"}) + "\n"
            for index, case in enumerate({"J31": M3.TXN_CASES, "J32": M3.NESTED_CASES, "J33": M3.DELETE_CASES}[task])))
        observed = metadata(scope, layout)
        observed["build"]["scenario_id"] = scenario_id
        observed.update(suite_sha256=suite_sha, prefix_sha256=prefix_sha, trace_sha256=M3.file_digest(trace))
        (root / f"{prefix}-{name}-metadata.json").write_text(json.dumps(observed))
    (root / f"{prefix}-target-fixture.json").write_text(json.dumps({
        "kind": "java-grpc-hbase-minicluster", "run_id": root.name, "source_commit": source,
        "authentication": "dev-no-auth", "layouts": ["HISTORY", "NATIVE"],
    }))


def write_txn_go_report(root, task):
    events = []
    required = {"J31": M3.J31_GO_REQUIRED_CASES, "J32": M3.J32_GO_REQUIRED_CASES, "J33": M3.J33_GO_REQUIRED_CASES}[task]
    for package, cases in required.items():
        name = M3.GO_MODULE + package
        events.extend({"Action": "pass", "Package": name, "Test": case} for case in cases)
        events.append({"Action": "pass", "Package": name})
    (root / "go-test.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events))


def write_txn_java_reports(root, task):
    integration = {"J31": M3.J31_INTEGRATION_CASES, "J32": M3.J32_INTEGRATION_CASES,
                   "J33": M3.J33_INTEGRATION_CASES}[task]
    units = {"J31": M3.J31_UNIT_CASES, "J32": M3.J32_UNIT_CASES, "J33": M3.J33_UNIT_CASES}[task]
    for suites, kind in ((integration, "failsafe-reports"), (units, "surefire-reports")):
        for name, names in suites.items():
            module = "hbase" if ".hbase." in name else "core" if ".core." in name else "grpc" if ".grpc." in name else "server"
            path = root / module / "target" / kind / f"TEST-{name}.xml"
            path.parent.mkdir(parents=True, exist_ok=True)
            cases = "".join(f'<testcase name="{case}"/>' for case in sorted(names))
            path.write_text(f'<testsuite name="{name}" tests="{len(names)}" failures="0" errors="0" skipped="0">{cases}</testsuite>')


def create_report_fixture(parent):
    root = parent / "run-1"
    root.mkdir()
    repo = parent / "repo"
    suite = repo / "testkit/scenarios/m3-put.yaml"
    suite.parent.mkdir(parents=True)
    suite.write_text("synthetic-validator-test-suite")
    registry = repo / "testkit/cases/m3-cases.json"
    registry.parent.mkdir(parents=True)
    registry.write_text((M3.REPO / "testkit/cases/m3-cases.json").read_text())
    source = "a" * 40
    (root / "run.json").write_text(json.dumps({
        "schema": "etcd-hbase-m3-run/v1", "scope": "public-local-minicluster",
        "run_id": root.name, "tasks": ["J30"], "source_commit": source,
        "commands_passed": True,
    }))
    suite_sha = M3.file_digest(suite)
    prefix_sha = hashlib.sha256(b"etcd-hbase-m3-test/run-1/").hexdigest()
    write_trace_metadata(root, suite_sha, prefix_sha)
    (root / "target-fixture.json").write_text(json.dumps({
        "kind": "java-grpc-hbase-minicluster", "run_id": root.name,
        "source_commit": source, "authentication": "dev-no-auth", "layouts": ["HISTORY", "NATIVE"],
    }))
    write_go_report(root)
    write_java_reports(root)
    return ReportFixture(root, repo, source)


def write_trace_metadata(root, suite_sha, prefix_sha):
    for name, scope, layout in (("native", "native-etcd", ""),
                                ("target-history", "target-hbase", "HISTORY"),
                                ("target-native", "target-hbase", "NATIVE")):
        trace = root / f"{name}-trace.jsonl"
        trace.write_text("".join(json.dumps({"schema": "etcd-hbase-trace/v1", "case_id": case,
            "evidence_scope": scope, "method": "etcdserverpb.KV/Put"}) + "\n"
            for case in M3.PUT_CASES))
        observed = metadata(scope, layout)
        observed.update(suite_sha256=suite_sha, prefix_sha256=prefix_sha,
                        trace_sha256=M3.file_digest(trace))
        (root / f"{name}-metadata.json").write_text(json.dumps(observed))


def write_go_report(root):
    events = []
    for package, cases in M3.GO_REQUIRED_CASES.items():
        name = M3.GO_MODULE + package
        events.extend({"Action": "pass", "Package": name, "Test": case} for case in cases)
        events.append({"Action": "pass", "Package": name})
    (root / "go-test.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events))


def write_java_reports(root):
    for name in M3.REQUIRED_CASES:
        module = "hbase" if name == M3.HBASE_SUITE else "admin" if name == M3.INIT_SUITE else "server"
        path = root / module / "target/failsafe-reports" / f"TEST-{name}.xml"
        path.parent.mkdir(parents=True, exist_ok=True)
        cases = "".join(f'<testcase name="{case}"/>' for case in sorted(M3.REQUIRED_CASES[name]))
        path.write_text(f'<testsuite name="{name}" tests="{len(M3.REQUIRED_CASES[name])}" '
                        f'failures="0" errors="0" skipped="0">{cases}</testsuite>')


def validate_report(fixture):
    with mock.patch.object(M3.subprocess, "check_output", side_effect=[fixture.source + "\n", ""]), \
            mock.patch.object(M3.subprocess, "run"):
        return M3.validate_run(fixture.root, fixture.repo)


class ReportFixture:
    def __init__(self, root, repo, source):
        self.root = root
        self.repo = repo
        self.source = source

    def report(self, suite):
        module = "hbase" if suite == M3.HBASE_SUITE else "admin" if suite == M3.INIT_SUITE else "server"
        return self.root / module / "target/failsafe-reports" / f"TEST-{suite}.xml"


if __name__ == "__main__":
    unittest.main()
