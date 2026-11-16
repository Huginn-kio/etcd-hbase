#!/usr/bin/env python3
"""Regression tests for fail-closed M4 evidence validation."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET


SCRIPT = Path(__file__).with_name("m4-report.py")
SPEC = importlib.util.spec_from_file_location("m4_report", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
m4_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m4_report)


class M4ReportTest(unittest.TestCase):
    AUTO_LEASE_ID = 101
    EXPLICIT_LEASE_ID = 202
    NEGATIVE_LEASE_ID = -303
    MISSING_LEASE_ID = 404

    def setUp(self) -> None:
        self.registry = m4_report.load_registry()

    def test_registry_requires_j40_rpc_layout_and_regression_coverage(self) -> None:
        self.assertIsNotNone(m4_report.validate_registry_document(self.registry))

        without_trace = copy.deepcopy(self.registry)
        without_trace["tasks"][0]["trace_cases"].pop()
        with self.assertRaises(m4_report.ReportError):
            m4_report.validate_registry_document(without_trace)

        without_rpc = copy.deepcopy(self.registry)
        without_rpc["tasks"][0]["maven_tests"] = [
            item for item in without_rpc["tasks"][0]["maven_tests"]
            if item["class"] != "io.etcdhbase.server.M4LeaseRpcIT"
        ]
        with self.assertRaises(m4_report.ReportError):
            m4_report.validate_registry_document(without_rpc)

        without_layout = copy.deepcopy(self.registry)
        without_layout["tasks"][0]["evidence"]["targets"].pop()
        with self.assertRaises(m4_report.ReportError):
            m4_report.validate_registry_document(without_layout)

    def test_registry_requires_critical_regression_cases(self) -> None:
        required_cases = {
            "io.etcdhbase.core.commit.CommitPublisherTest":
                "selectionConditionIsRecheckedAfterHandoffAndBeforeEachRetry",
            "io.etcdhbase.server.M3RuntimeBudgetsTest":
                "m4MinimumAccountsForEveryGroupAndConcurrentLeaseOwner",
            "io.etcdhbase.server.M4LeaseRpcIT":
                "minimumMemoryPreservesLeaseLifecycleWhenClockFailsBeforeSelection",
            "io.etcdhbase.core.commit.LeaseReadViewTest":
                "bindingReadMergesStableHistoryWithSelectedChangesAtOneBarrier",
            "io.etcdhbase.grpc.protocol.LeaseTimeToLiveResponseMapperTest":
                "includesEveryRequestedKeyAsOriginalBytesWithThePinnedHeader",
        }
        for class_name, required_case in required_cases.items():
            with self.subTest(class_name=class_name):
                missing_case = copy.deepcopy(self.registry)
                suite = next(item for item in missing_case["tasks"][0]["maven_tests"]
                             if item["class"] == class_name)
                suite["tests"].remove(required_case)
                with self.assertRaisesRegex(m4_report.ReportError, "binding, TTL projection"):
                    m4_report.validate_registry_document(missing_case)

    def test_both_registries_reject_malformed_or_duplicate_go_packages(self) -> None:
        for task_id in ("J40", "J41"):
            for invalid in (None, {"package": 1}, {"package": "extra", "tests": []},
                            {"package": "extra", "tests": [1]}):
                self._assert_invalid_registration(task_id, "go_tests", invalid)
            changed = copy.deepcopy(self.registry)
            task = next(item for item in changed["tasks"] if item["id"] == task_id)
            task["go_tests"].append(copy.deepcopy(task["go_tests"][0]))
            with self.assertRaisesRegex(m4_report.ReportError, "more than once"):
                m4_report.validate_registry_document(changed)

    def test_both_registries_reject_malformed_maven_suites(self) -> None:
        invalid_suites = (
            None,
            {"module": "outside", "kind": "surefire", "class": "Extra", "tests": ["test"]},
            {"module": "core", "kind": "unknown", "class": "Extra", "tests": ["test"]},
            {"module": "core", "kind": "surefire", "class": 1, "tests": ["test"]},
            {"module": "core", "kind": "surefire", "class": "Extra", "tests": []},
            {"module": "core", "kind": "surefire", "class": "Extra", "tests": [1]},
        )
        for task_id in ("J40", "J41"):
            for invalid in invalid_suites:
                self._assert_invalid_registration(task_id, "maven_tests", invalid)

    def _assert_invalid_registration(self, task_id: str, field: str, invalid: object) -> None:
        with self.subTest(task_id=task_id, field=field, invalid=invalid):
            changed = copy.deepcopy(self.registry)
            task = next(item for item in changed["tasks"] if item["id"] == task_id)
            task[field].append(invalid)
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_registry_document(changed)

    def test_all_tasks_is_rejected_while_j41_to_j47_are_pending(self) -> None:
        with self.assertRaisesRegex(m4_report.ReportError, "not registered"):
            m4_report.validate_all_tasks_registered(self.registry)

    def test_complete_j40_artifact_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            summary = m4_report.validate_artifact(
                m4_report.REPO, artifact, self.registry
            )
            self.assertEqual(summary["task"], "J40")
            self.assertEqual(summary["target_layouts"], ["HISTORY", "NATIVE"])
            self.assertGreater(summary["go_test_count"], 0)

    def test_complete_j41_artifact_is_read_from_its_registered_subdirectory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J41")
            summary = m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)
            self.assertEqual(summary["task"], "J41")
            self.assertEqual(summary["target_layouts"], ["HISTORY", "NATIVE"])

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J41")
            evidence = artifact / "j41"
            (evidence / "native-trace.jsonl").replace(artifact / "native-trace.jsonl")
            with self.assertRaisesRegex(m4_report.ReportError, "required M4 trace"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_j41_registration_rejects_unsafe_evidence_subdirectories(self) -> None:
        for subdirectory in ("../outside", "/tmp/evidence", "j41\\nested"):
            with self.subTest(subdirectory=subdirectory):
                changed = copy.deepcopy(self.registry)
                task = next(item for item in changed["tasks"] if item["id"] == "J41")
                task["artifact_subdir"] = subdirectory
                with self.assertRaises(m4_report.ReportError):
                    m4_report.validate_registry_document(changed)

    def test_j41_registry_requires_binding_plan_and_owner_fence_regressions(self) -> None:
        required = {
            "io.etcdhbase.core.lease.LeaseBindingStatePlannerTest":
                "attachmentReservesFutureUnbindAtTheExactCapacityBoundary",
            "io.etcdhbase.server.commit.LeaseBindingMutationPlannerTest":
                "revokedStateUpdatesDoNotWriteNewExpiryCandidates",
            "io.etcdhbase.server.commit.M3OwnerLeaseTest":
                "startupFormatUpgradeRotatesTheFenceWithoutLosingOwnerRenewal",
        }
        for class_name, method_name in required.items():
            with self.subTest(class_name=class_name):
                without_class = copy.deepcopy(self.registry)
                task = next(item for item in without_class["tasks"] if item["id"] == "J41")
                task["maven_tests"] = [item for item in task["maven_tests"]
                                        if item["class"] != class_name]
                with self.assertRaises(m4_report.ReportError):
                    m4_report.validate_registry_document(without_class)

                without_method = copy.deepcopy(self.registry)
                task = next(item for item in without_method["tasks"] if item["id"] == "J41")
                suite = next(item for item in task["maven_tests"] if item["class"] == class_name)
                suite["tests"].remove(method_name)
                with self.assertRaises(m4_report.ReportError):
                    m4_report.validate_registry_document(without_method)

    def test_j41_registry_requires_each_go_binding_regression(self) -> None:
        for package, required_tests in m4_report.J41_GO_TESTS.items():
            for test_name in required_tests:
                with self.subTest(package=package, test_name=test_name):
                    changed = copy.deepcopy(self.registry)
                    task = next(item for item in changed["tasks"] if item["id"] == "J41")
                    suite = next(item for item in task["go_tests"]
                                 if item["package"] == package)
                    suite["tests"].remove(test_name)
                    with self.assertRaisesRegex(
                        m4_report.ReportError, "Go binding regression coverage is incomplete"
                    ):
                        m4_report.validate_registry_document(changed)

    def test_missing_or_skipped_required_maven_case_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            report_path = artifact / "server/target/failsafe-reports/TEST-io.etcdhbase.server.M4LeaseRpcIT.xml"
            report_tree = ET.parse(report_path)
            suite = report_tree.getroot()
            suite.attrib["tests"] = "0"
            suite.clear()
            report_tree.write(report_path, encoding="utf-8", xml_declaration=True)
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            report_path = artifact / "server/target/failsafe-reports/TEST-io.etcdhbase.server.M4LeaseRpcIT.xml"
            report_tree = ET.parse(report_path)
            testcase = report_tree.getroot().find("testcase")
            assert testcase is not None
            ET.SubElement(testcase, "skipped")
            report_tree.getroot().attrib["skipped"] = "1"
            report_tree.write(report_path, encoding="utf-8", xml_declaration=True)
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_corrupt_reordered_or_missing_layout_trace_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            trace_path = artifact / "target-history-trace.jsonl"
            trace_lines = trace_path.read_text(encoding="utf-8").splitlines()
            trace_lines[0] = "{broken}"
            trace_path.write_text("".join(f"{line}\n" for line in trace_lines), encoding="utf-8")
            with self.assertRaisesRegex(m4_report.ReportError, "target-history-trace.jsonl line 1 is invalid"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            trace_path = artifact / "native-trace.jsonl"
            records = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
            records[0], records[1] = records[1], records[0]
            trace_path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_empty_successful_response_is_rejected_even_with_a_matching_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            trace_path = artifact / "native-trace.jsonl"
            records = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
            records[1]["response"] = {"fields": {}, "oneofs": {}}
            trace_path.write_text(
                "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
                encoding="utf-8",
            )
            self._refresh_trace_digest(artifact, trace_path, "native-metadata.json")

            with self.assertRaisesRegex(m4_report.ReportError, "J40 automatic Grant"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_lease_listing_must_contain_every_previously_granted_id(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            trace_path = artifact / "target-history-trace.jsonl"
            records = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
            records[8]["response"]["fields"]["leases"].pop()
            trace_path.write_text(
                "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
                encoding="utf-8",
            )
            self._refresh_trace_digest(artifact, trace_path, "target-history-metadata.json")

            with self.assertRaisesRegex(m4_report.ReportError, "Lease listing is incomplete"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_missing_target_trace_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            (artifact / "target-native-trace.jsonl").unlink()
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_trace_ordinals_are_monotonic_per_case_while_file_order_is_global(self) -> None:
        record = {
            "schema": "etcd-hbase-trace/v1",
            "case_id": "repeated-case",
            "ordinal": 1,
            "method": "etcdserverpb.Lease/LeaseLeases",
            "request": {"fields": {}, "oneofs": {}},
            "response": {"fields": {}, "oneofs": {}},
            "error": None,
            "stream_terminal": None,
            "evidence_scope": "native-etcd",
            "elapsed_ns": "1",
        }
        expected = [
            {"id": "repeated-case", "method": record["method"]},
            {"id": "repeated-case", "method": record["method"]},
        ]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trace.jsonl"
            second = {**record, "ordinal": 2}
            path.write_text(
                json.dumps(record) + "\n" + json.dumps(second) + "\n", encoding="utf-8"
            )
            m4_report._read_trace(path, expected, "native-etcd")

            second["ordinal"] = 1
            path.write_text(
                json.dumps(record) + "\n" + json.dumps(second) + "\n", encoding="utf-8"
            )
            with self.assertRaises(m4_report.ReportError):
                m4_report._read_trace(path, expected, "native-etcd")

    def test_trace_digest_and_source_identity_are_bound_to_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            metadata_path = artifact / "target-history-metadata.json"
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata["source_commit"] = "f" * 40
            metadata_path.write_text(json.dumps(metadata) + "\n", encoding="utf-8")
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_go_skip_or_missing_required_test_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            path = artifact / "go-test.jsonl"
            events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            events[0]["Action"] = "skip"
            path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            with self.assertRaises(m4_report.ReportError):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact)
            path = artifact / "go-test.jsonl"
            events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            events[0]["Action"] = "unknown"
            path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            with self.assertRaisesRegex(m4_report.ReportError, "unknown action"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def _write_valid_artifact(self, artifact: Path, task_id: str = "J40") -> None:
        task = next(item for item in self.registry["tasks"] if item["id"] == task_id)
        run_id = artifact.name
        source_commit = "a" * 40
        base_commit = "b" * 40
        evidence_root = artifact / task.get("artifact_subdir", "")
        evidence_root.mkdir(parents=True, exist_ok=True)
        command_logs = self._write_command_logs(artifact)
        commands = self._command_records(command_logs)
        self._write_run_manifest(artifact, task["id"], run_id, source_commit, base_commit, commands)
        self._write_go_test_events(artifact, task)
        self._write_maven_test_reports(artifact, task)
        suite_hash, prefix_hash = m4_report._scenario_hashes(m4_report.REPO, task, run_id)
        self._write_trace_artifacts(
            evidence_root, task, run_id, source_commit, suite_hash, prefix_hash
        )
        self._write_comparison_logs(evidence_root, task)

    @staticmethod
    def _refresh_trace_digest(artifact: Path, trace_path: Path, metadata_name: str) -> None:
        metadata_path = artifact / metadata_name
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["trace_sha256"] = hashlib.sha256(trace_path.read_bytes()).hexdigest()
        metadata_path.write_text(json.dumps(metadata) + "\n", encoding="utf-8")

    @staticmethod
    def _write_command_logs(artifact: Path) -> dict[str, str]:
        command_logs = {
            "go-module-lock": "go-module-lock.log",
            "go-tests": "go-test.jsonl",
            "maven-verify": "maven.log",
            "collect-maven-reports": "maven-reports.log",
        }
        for path in command_logs.values():
            target = artifact / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("completed\n", encoding="utf-8")
        (artifact / "go-test.stderr").write_text("", encoding="utf-8")
        return command_logs

    @staticmethod
    def _command_records(command_logs: dict[str, str]) -> list[dict[str, object]]:
        return [
            {
                "name": name,
                "argv": ["test-command", name],
                "exit_code": 0,
                "started_at": "2026-11-14T00:00:00Z",
                "finished_at": "2026-11-14T00:00:01Z",
                "stdout_log": path,
                "stderr_log": "go-test.stderr" if name == "go-tests" else None,
            }
            for name, path in command_logs.items()
        ]

    @staticmethod
    def _write_run_manifest(
        artifact: Path, task_id: str, run_id: str, source_commit: str,
        base_commit: str, commands: list[dict[str, object]],
    ) -> None:
        run = {
            "schema": "etcd-hbase-m4-run/v1",
            "run_id": run_id,
            "scope": "public-local-minicluster",
            "tasks": [task_id],
            "status": "passed",
            "source_commit": source_commit,
            "checkout": source_commit,
            "base_commit": base_commit,
            "worktree_clean": True,
            "started_at": "2026-11-14T00:00:00Z",
            "finished_at": "2026-11-14T00:00:02Z",
            "commands": commands,
        }
        (artifact / "run.json").write_text(json.dumps(run) + "\n", encoding="utf-8")

    @staticmethod
    def _write_go_test_events(artifact: Path, task: dict[str, object]) -> None:
        go_events = []
        for package in task["go_tests"]:
            module = f"github.com/Reidddddd/etcd-hbase/testkit/go/{package['package']}"
            go_events.append({"Action": "start", "Package": module})
            for test in package["tests"]:
                go_events.append({"Action": "pass", "Package": module, "Test": test})
            go_events.append({"Action": "pass", "Package": module})
        (artifact / "go-test.jsonl").write_text(
            "".join(json.dumps(event) + "\n" for event in go_events), encoding="utf-8"
        )

    @staticmethod
    def _write_maven_test_reports(artifact: Path, task: dict[str, object]) -> None:
        grouped: dict[tuple[str, str, str], set[str]] = {}
        for suite in task["maven_tests"]:
            grouped.setdefault((suite["module"], suite["kind"], suite["class"]), set()).update(suite["tests"])
        for (module, kind, class_name), test_names in grouped.items():
            report_dir = artifact / module / "target" / f"{kind}-reports"
            report_dir.mkdir(parents=True, exist_ok=True)
            root = ET.Element("testsuite", {
                "name": class_name,
                "tests": str(len(test_names)),
                "failures": "0",
                "errors": "0",
                "skipped": "0",
            })
            for name in sorted(test_names):
                ET.SubElement(root, "testcase", {"name": name, "classname": class_name})
            ET.ElementTree(root).write(
                report_dir / f"TEST-{class_name}.xml", encoding="utf-8", xml_declaration=True
            )

    @staticmethod
    def _trace_variants(task: dict[str, object]) -> list[tuple[dict[str, object], str, str, str]]:
        evidence = task["evidence"]
        trace_variants = [(evidence["native"], "native", "native-trace.jsonl", "native-metadata.json")]
        for target in evidence["targets"]:
            suffix = target["layout"].lower()
            trace_variants.append((target, suffix, f"target-{suffix}-trace.jsonl", f"target-{suffix}-metadata.json"))
        return trace_variants

    def _write_trace_artifacts(
        self, evidence_root: Path, task: dict[str, object], run_id: str, source_commit: str,
        suite_hash: str, prefix_hash: str,
    ) -> None:
        for descriptor, suffix, trace_name, metadata_name in self._trace_variants(task):
            self._write_trace_artifact(
                evidence_root, task, descriptor, suffix, trace_name, metadata_name,
                run_id, source_commit, suite_hash, prefix_hash,
            )

    @staticmethod
    def _write_trace_artifact(
        evidence_root: Path, task: dict[str, object], descriptor: dict[str, object],
        suffix: str, trace_name: str, metadata_name: str, run_id: str,
        source_commit: str, suite_hash: str, prefix_hash: str,
    ) -> None:
        records = M4ReportTest._valid_trace_records(task, descriptor)
        trace_bytes = "".join(
            json.dumps(record, separators=(",", ":")) + "\n" for record in records
        ).encode()
        (evidence_root / trace_name).write_bytes(trace_bytes)
        embedded_native = descriptor["embedded_native"]
        layout = descriptor.get("layout", "")
        metadata = {
            "schema": "etcd-hbase-m4-run/v1",
            "run_id": run_id,
            "source_commit": source_commit,
            "suite_sha256": suite_hash,
            "evidence_scope": descriptor["scope"],
            "prefix_sha256": prefix_hash,
            "trace_sha256": hashlib.sha256(trace_bytes).hexdigest(),
            "embedded_native": embedded_native,
            "layout": layout,
            "status": "passed",
        }
        (evidence_root / metadata_name).write_text(json.dumps(metadata) + "\n", encoding="utf-8")
        log_name = "native.log" if embedded_native else f"target-{suffix}.log"
        (evidence_root / log_name).write_text("M4 Lease scenario passed\n", encoding="utf-8")

    @staticmethod
    def _valid_trace_records(
        task: dict[str, object], descriptor: dict[str, object],
    ) -> list[dict[str, object]]:
        if task["id"] == "J41":
            return M4ReportTest._valid_binding_trace_records(task, descriptor)
        records: list[dict[str, object]] = []
        case_ordinals: dict[str, int] = {}
        for expected in task["trace_cases"]:
            case_id = expected["id"]
            case_ordinals[case_id] = case_ordinals.get(case_id, 0) + 1
            request, response, error = M4ReportTest._trace_step(case_id)
            records.append({
                "schema": "etcd-hbase-trace/v1",
                "case_id": case_id,
                "ordinal": case_ordinals[case_id],
                "method": expected["method"],
                "request": request,
                "response": response,
                "error": error,
                "stream_terminal": None,
                "evidence_scope": descriptor["scope"],
                "elapsed_ns": "1",
            })
        return records

    @staticmethod
    def _valid_binding_trace_records(
        task: dict[str, object], descriptor: dict[str, object],
    ) -> list[dict[str, object]]:
        errors = {
            "binding-invalid-lease-atomicity": {
                "grpc_code": "NotFound", "consumer_kind": "grpc-status",
                "semantic_kind": "LEASE_NOT_FOUND",
            },
            "binding-invalid-lease-put": {
                "grpc_code": "NotFound", "consumer_kind": "grpc-status",
                "semantic_kind": "LEASE_NOT_FOUND",
            },
        }
        records = []
        case_ordinals: dict[str, int] = {}
        for expected in task["trace_cases"]:
            case_id = expected["id"]
            case_ordinals[case_id] = case_ordinals.get(case_id, 0) + 1
            error = errors.get(case_id)
            records.append({
                "schema": "etcd-hbase-trace/v1",
                "case_id": case_id,
                "ordinal": case_ordinals[case_id],
                "method": expected["method"],
                "request": M4ReportTest._snapshot({}),
                "response": None if error else M4ReportTest._snapshot({}),
                "error": error,
                "stream_terminal": None,
                "evidence_scope": descriptor["scope"],
                "elapsed_ns": "1",
            })
        return records

    @staticmethod
    def _snapshot(fields: dict[str, object]) -> dict[str, object]:
        return {"fields": fields, "oneofs": {}}

    @classmethod
    def _lease_header(cls) -> dict[str, object]:
        return cls._snapshot({
            "clusterId": "1", "memberId": "2", "raftTerm": "1", "revision": "1",
        })

    @classmethod
    def _lease_response(cls, **fields: object) -> dict[str, object]:
        return cls._snapshot({"header": cls._lease_header(), **fields})

    @classmethod
    def _lease_statuses(cls, *lease_ids: int) -> list[dict[str, object]]:
        return [cls._snapshot({"ID": str(lease_id)}) for lease_id in lease_ids]

    @classmethod
    def _trace_step(
        cls, case_id: str,
    ) -> tuple[dict[str, object], dict[str, object] | None, dict[str, str] | None]:
        steps = cls._successful_lease_steps()
        steps.update(cls._lease_edge_steps())
        return steps[case_id]

    @classmethod
    def _successful_lease_steps(
        cls,
    ) -> dict[str, tuple[dict[str, object], dict[str, object] | None, dict[str, str] | None]]:
        empty_request = cls._snapshot({})
        return {
            "native-baseline": (
                empty_request, cls._lease_response(leases=[]), None,
            ),
            "auto-grant": (
                cls._snapshot({"ID": "0", "TTL": "0"}),
                cls._lease_response(ID=str(cls.AUTO_LEASE_ID), TTL="5", error=""), None,
            ),
            "auto-ttl": (
                cls._snapshot({"ID": str(cls.AUTO_LEASE_ID), "keys": True}),
                cls._lease_response(
                    ID=str(cls.AUTO_LEASE_ID), TTL="4", grantedTTL="5", keys=[]
                ), None,
            ),
            "explicit-grant": (
                cls._snapshot({"ID": str(cls.EXPLICIT_LEASE_ID), "TTL": "30"}),
                cls._lease_response(ID=str(cls.EXPLICIT_LEASE_ID), TTL="30", error=""), None,
            ),
            "explicit-ttl": (
                cls._snapshot({"ID": str(cls.EXPLICIT_LEASE_ID), "keys": True}),
                cls._lease_response(
                    ID=str(cls.EXPLICIT_LEASE_ID), TTL="29", grantedTTL="30", keys=[]
                ), None,
            ),
            "leases-list": (
                empty_request,
                cls._lease_response(leases=cls._lease_statuses(
                    cls.AUTO_LEASE_ID, cls.EXPLICIT_LEASE_ID, cls.NEGATIVE_LEASE_ID
                )), None,
            ),
        }

    @classmethod
    def _lease_edge_steps(
        cls,
    ) -> dict[str, tuple[dict[str, object], dict[str, object] | None, dict[str, str] | None]]:
        return {
            "duplicate-id": (
                cls._snapshot({"ID": str(cls.EXPLICIT_LEASE_ID), "TTL": "30"}), None,
                {"grpc_code": "FailedPrecondition", "consumer_kind": "grpc-status",
                 "semantic_kind": "LEASE_EXISTS"},
            ),
            "missing-ttl": (
                cls._snapshot({"ID": str(cls.MISSING_LEASE_ID), "keys": True}),
                cls._lease_response(
                    ID=str(cls.MISSING_LEASE_ID), TTL="-1", grantedTTL="0", keys=[]
                ), None,
            ),
            "negative-id": (
                cls._snapshot({"ID": str(cls.NEGATIVE_LEASE_ID), "TTL": "30"}),
                cls._lease_response(ID=str(cls.NEGATIVE_LEASE_ID), TTL="30", error=""), None,
            ),
            "oversized-ttl": (
                cls._snapshot({"ID": "0", "TTL": "9000000001"}), None,
                {"grpc_code": "OutOfRange", "consumer_kind": "grpc-status",
                 "semantic_kind": "OUT_OF_RANGE"},
            ),
            "zero-id-ttl": (
                cls._snapshot({"ID": "0", "keys": True}),
                cls._lease_response(ID="0", TTL="-1", grantedTTL="0", keys=[]), None,
            ),
        }

    @staticmethod
    def _write_comparison_logs(evidence_root: Path, task: dict[str, object]) -> None:
        success = (m4_report.J41_COMPARE_SUCCESS if task["id"] == "J41"
                   else m4_report.COMPARE_SUCCESS)
        for name in task["evidence"]["compare_logs"]:
            (evidence_root / name).write_text(success + "\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
