#!/usr/bin/env python3
"""Regression tests for fail-closed M4 evidence validation."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import re
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

    def test_registered_java_cases_have_a_matching_source_method(self) -> None:
        # This catches renamed registrations early; executed XML is still required by the report.
        for task in self.registry["tasks"]:
            for suite in task["maven_tests"]:
                source = (m4_report.REPO / suite["module"] / "src/test/java"
                          / Path(*suite["class"].split(".")).with_suffix(".java"))
                text = source.read_text(encoding="utf-8")
                for method in suite["tests"]:
                    with self.subTest(task=task["id"], suite=suite["class"], method=method):
                        self.assertRegex(text, rf"\bvoid\s+{re.escape(method)}\s*\(")

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
                "minimumMemoryPreservesLeaseLifecycleAcrossClockFailures",
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

    def test_j44_registry_requires_empty_replacement_index_regression(self) -> None:
        class_name = "io.etcdhbase.core.commit.EventSubscriptionIndexTest"
        method_name = "replacingRegistrationWithEmptyRangeRemovesPreviousPointOrInterval"

        for missing_suite in (True, False):
            changed = copy.deepcopy(self.registry)
            task = next(item for item in changed["tasks"] if item["id"] == "J44")
            if missing_suite:
                task["maven_tests"] = [suite for suite in task["maven_tests"]
                                        if suite["class"] != class_name]
            else:
                suite = next(item for item in task["maven_tests"] if item["class"] == class_name)
                suite["tests"].remove(method_name)
            with self.subTest(missing_suite=missing_suite):
                with self.assertRaises(m4_report.ReportError):
                    m4_report.validate_registry_document(changed)

    def test_j44_registry_requires_pin_lifecycle_and_owner_lane_regressions(self) -> None:
        required_cases = {
            "io.etcdhbase.core.commit.SealedEventCursorTest": {
                "measuredProofReadsChargeActualEncodedBytesAndAllowNinthSmallRead",
                "measuredProofReadsStillRejectActualBytesBeyondTheirCumulativeLimit",
                "sequenceReaderPersistsSelectedEventRootBeforeReadingItsIndex",
                "synchronousSequenceOpenFailureClosesTailerAndSubscriptions",
            },
            "io.etcdhbase.server.commit.EventHistoryWorkerTest": {
                "slowBackfillReadDoesNotBlockHeartbeatAndPublicationUsesOwnerLane",
            },
        }

        for class_name, methods in required_cases.items():
            for method_name in methods:
                with self.subTest(class_name=class_name, method_name=method_name):
                    changed = copy.deepcopy(self.registry)
                    task = next(item for item in changed["tasks"] if item["id"] == "J44")
                    suite = next(item for item in task["maven_tests"]
                                 if item["class"] == class_name)
                    suite["tests"].remove(method_name)
                    with self.assertRaises(m4_report.ReportError):
                        m4_report.validate_registry_document(changed)

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

    def test_j44_registry_requires_tailer_backfill_and_layout_regressions(self) -> None:
        missing_suite = copy.deepcopy(self.registry)
        task = next(item for item in missing_suite["tasks"] if item["id"] == "J44")
        task["maven_tests"] = [
            suite for suite in task["maven_tests"]
            if suite["class"] != "io.etcdhbase.hbase.storage.J44EventPinIT"
        ]
        with self.assertRaisesRegex(m4_report.ReportError, "suites are incomplete"):
            m4_report.validate_registry_document(missing_suite)

        missing_scan_suite = copy.deepcopy(self.registry)
        task = next(item for item in missing_scan_suite["tasks"] if item["id"] == "J44")
        task["maven_tests"] = [
            suite for suite in task["maven_tests"]
            if suite["class"] != "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT"
        ]
        with self.assertRaisesRegex(m4_report.ReportError, "suites are incomplete"):
            m4_report.validate_registry_document(missing_scan_suite)

        missing_scan_case = copy.deepcopy(self.registry)
        task = next(item for item in missing_scan_case["tasks"] if item["id"] == "J44")
        suite = next(item for item in task["maven_tests"]
                     if item["class"] == "io.etcdhbase.hbase.storage.HBaseSystemRecordStoreScanIT")
        suite["tests"].append("seeksLatestExpiryCheckpointAtTheReadBarrier")
        suite["tests"].remove("seeksEventRevisionsWithinOneBucketAndResumesExclusively")
        with self.assertRaisesRegex(m4_report.ReportError, "cursor, backfill, fairness, or layout"):
            m4_report.validate_registry_document(missing_scan_case)

        missing_fairness = copy.deepcopy(self.registry)
        task = next(item for item in missing_fairness["tasks"] if item["id"] == "J44")
        suite = next(item for item in task["maven_tests"]
                     if item["class"] == "io.etcdhbase.server.commit.SerialMutationQueueTest")
        suite["tests"].remove("backgroundWorkHasReservedCapacityAndRoundRobinsWithOtherLanes")
        with self.assertRaisesRegex(m4_report.ReportError, "fairness"):
            m4_report.validate_registry_document(missing_fairness)

    def test_j45_registry_requires_revision_barrier_and_cancellation_coverage(self) -> None:
        required_cases = (
            ("io.etcdhbase.server.commit.WatchStreamTest",
             "zeroStartWaitsForCreatedBarrierAndExcludesEarlierEvents"),
            ("io.etcdhbase.server.commit.WatchStreamTest",
             "negativeStartWithUncompactedHistorySkipsOldEventsAndDeliversNewEvents"),
            ("io.etcdhbase.server.commit.WatchGatewayTest",
             "currentRevisionReadsTheAuthoritativeControlSnapshotInsteadOfTheTailerCheckpoint"),
            ("io.etcdhbase.server.commit.WatchStreamTest",
             "cancelAcknowledgementWaitsForAndUsesTheCurrentSessionRevision"),
            ("io.etcdhbase.server.commit.WatchStreamTest",
             "createsAndCancelsRawWatchIdMinusOne"),
            ("io.etcdhbase.server.commit.WatchStreamTest",
             "createRejectionUsesTheCurrentSessionRevision"),
            ("io.etcdhbase.server.commit.WatchStreamTest",
             "resourceLimitTerminationCancelsOnlyItsWatchAndReleasesQueuedDeliveries"),
            ("io.etcdhbase.server.commit.WatchStreamTest",
             "closedTailerTerminationCancelsOnlyItsWatchAndReleasesQueuedDeliveries"),
            ("io.etcdhbase.server.commit.WatchStreamTest",
             "oversizedCompleteRevisionCancelsWatchWithoutSendingPartialEvents"),
        )
        for class_name, required_case in required_cases:
            with self.subTest(class_name=class_name, required_case=required_case):
                missing_case = copy.deepcopy(self.registry)
                task = next(item for item in missing_case["tasks"] if item["id"] == "J45")
                suite = next(item for item in task["maven_tests"]
                             if item["class"] == class_name)
                suite["tests"].remove(required_case)
                with self.assertRaisesRegex(m4_report.ReportError, "revision floors"):
                    m4_report.validate_registry_document(missing_case)

        integration_suite = copy.deepcopy(self.registry)
        task = next(item for item in integration_suite["tasks"] if item["id"] == "J45")
        task["maven_tests"][0]["kind"] = "failsafe"
        with self.assertRaisesRegex(m4_report.ReportError, "JVM unit tests"):
            m4_report.validate_registry_document(integration_suite)

        missing_native_case = copy.deepcopy(self.registry)
        task = next(item for item in missing_native_case["tasks"] if item["id"] == "J45")
        task["watch_start_matrix"].pop()
        with self.assertRaisesRegex(m4_report.ReportError, "initial -2/-1/0/future"):
            m4_report.validate_registry_document(missing_native_case)

        missing_raw_watch_id = copy.deepcopy(self.registry)
        task = next(item for item in missing_raw_watch_id["tasks"] if item["id"] == "J45")
        task.pop("watch_id_matrix")
        with self.assertRaisesRegex(m4_report.ReportError, "raw Watch ID -1"):
            m4_report.validate_registry_document(missing_raw_watch_id)

    def test_j45_covers_future_create_and_cancel_but_defers_future_delivery_to_j46_1(self) -> None:
        task = next(item for item in self.registry["tasks"] if item["id"] == "J45")
        expected_boundary = (
            "J45 public protocol evidence covers negative/zero startRevision cases, negative watch ID "
            "Create/Cancel, future startRevision Create/Cancel, and the Compact-boundary lifecycle. Future-event "
            "delivery, positive historical startRevision behavior, and replay/live handoff are assigned to J46.1. "
            "The native raw-proto event probe waits for etcd's synced progress marker as a harness barrier; this "
            "does not claim Java progress-request support, which is assigned to J46.2. Native raw-proto and local-service "
            "Go traces compare the registered start/cancel matrix; a separate Go-client lifecycle run exercises multiple "
            "watch IDs, byte ranges, filters/PrevKV, and client-context cancellation cleanup against "
            "Java WatchService. Normal server-side request half-close semantics remain assigned to J47. "
            "MiniCluster acceptance remains outside this unit evidence"
        )
        self.assertEqual(m4_report.J45_VERIFICATION_BOUNDARY, expected_boundary)
        self.assertEqual(task["verification_boundary"], expected_boundary)

        registered_tests = {
            test for suite in task["maven_tests"] for test in suite["tests"]
        }
        out_of_scope_tests = {
            "passesInclusiveStartRevisionToTailerSoEarlierEventsDoNotConsumeWatchBudget",
            "futureStartRevisionDropsOlderLiveEventsAndDeliversTheTargetInclusively",
            "rejectsHistoricalStartWithoutPublishingAWatchThatWouldMissEvents",
        }
        self.assertTrue(out_of_scope_tests.isdisjoint(registered_tests))

        broadened_boundary = copy.deepcopy(self.registry)
        j45 = next(item for item in broadened_boundary["tasks"] if item["id"] == "J45")
        j45["verification_boundary"] += "; J45 covers future start revisions"
        with self.assertRaisesRegex(m4_report.ReportError, "raw-proto oracle"):
            m4_report.validate_registry_document(broadened_boundary)

        missing_native_test = copy.deepcopy(self.registry)
        task = next(item for item in missing_native_test["tasks"] if item["id"] == "J45")
        task["go_tests"][1]["tests"].remove("TestM4WatchStartScenarioMatchesPinnedNativeOracle")
        with self.assertRaisesRegex(m4_report.ReportError, "native oracle tests are incomplete"):
            m4_report.validate_registry_document(missing_native_test)

        missing_local_service = copy.deepcopy(self.registry)
        task = next(item for item in missing_local_service["tasks"] if item["id"] == "J45")
        task["evidence"].pop("local_service")
        with self.assertRaisesRegex(m4_report.ReportError, "native and local Java WatchService"):
            m4_report.validate_registry_document(missing_local_service)

    def test_complete_j45_local_unit_artifact_is_accepted_without_minicluster_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J45")

            summary = m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

            self.assertEqual("J45", summary["task"])
            self.assertEqual("public-local-unit", summary["scope"])
            self.assertEqual(
                "JVM-Watch-units-plus-native-start-and-local-service-start-and-lifecycle-matrices",
                summary["evidence"]
            )
            self.assertEqual(8, summary["go_test_count"])
            self.assertEqual(39, summary["native_trace_cases"])
            self.assertEqual(7, summary["native_watch_start_cases"])
            self.assertEqual(2, summary["native_watch_id_cases"])
            self.assertEqual(30, summary["local_service_trace_cases"])
            self.assertGreater(summary["java_test_count"], 0)

            trace_path = artifact / "j45/native-trace.jsonl"
            records = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
            compacted = next(item for item in records if item["case_id"] == "initial-minus-two-compacted")
            compacted["response"]["fields"]["header"]["fields"]["revision"] = "1"
            trace_path.write_text("".join(json.dumps(item) + "\n" for item in records), encoding="utf-8")
            with self.assertRaisesRegex(m4_report.ReportError, "response fields differ"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_j45_initial_local_service_artifact_requires_lifecycle_success_record(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J45")
            log_path = artifact / "j45/local-lifecycle/initial.log"
            log_path.write_text("M4 Watch start scenario passed\n", encoding="utf-8")

            with self.assertRaisesRegex(m4_report.ReportError, "lifecycle success record"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_j45_local_service_trace_enforces_the_reported_compact_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J45")
            trace_path = artifact / "j45/local-service/compacted-trace.jsonl"
            records = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
            compacted = next(item for item in records
                             if item["case_id"] == "compacted-minus-two-compacted")
            compacted["response"]["fields"]["compactRevision"] = "5"
            trace_path.write_text("".join(json.dumps(item) + "\n" for item in records), encoding="utf-8")
            with self.assertRaisesRegex(m4_report.ReportError, "response fields differ"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_j45_zero_raft_term_is_valid_only_for_local_service_headers(self) -> None:
        response_fields = {
            "header": {
                "fields": {"clusterId": "11", "memberId": "22", "raftTerm": "0", "revision": "1"},
                "oneofs": {},
            },
        }

        self.assertEqual("0", m4_report._j45_header_fields(
            response_fields, "local Watch", allow_zero_raft_term=True)["raftTerm"])
        with self.assertRaisesRegex(m4_report.ReportError, "raftTerm must be positive"):
            m4_report._j45_header_fields(response_fields, "native Watch")

        for field in ("clusterId", "memberId"):
            with self.subTest(field=field):
                invalid_identity = copy.deepcopy(response_fields)
                invalid_identity["header"]["fields"][field] = "0"
                with self.assertRaisesRegex(m4_report.ReportError, f"header {field} must be positive"):
                    m4_report._j45_header_fields(
                        invalid_identity, "local Watch", allow_zero_raft_term=True)

    def test_complete_j44_minicluster_artifact_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            task = next(item for item in self.registry["tasks"] if item["id"] == "J44")
            run_id = artifact.name
            source_commit = "a" * 40
            base_commit = "b" * 40
            command_names = sorted(m4_report.J44_REQUIRED_COMMANDS)
            commands = []
            for name in command_names:
                output = f"{name}.log"
                error = f"{name}.stderr"
                (artifact / output).write_text("completed\n", encoding="utf-8")
                (artifact / error).write_text("", encoding="utf-8")
                commands.append({
                    "name": name,
                    "argv": ["verification", name],
                    "exit_code": 0,
                    "started_at": "2026-11-14T00:00:00Z",
                    "finished_at": "2026-11-14T00:00:01Z",
                    "stdout_log": output,
                    "stderr_log": error,
                })
            self._write_run_manifest(artifact, "J44", run_id, source_commit, base_commit, commands)
            self._write_maven_test_reports(artifact, task)

            summary = m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

            self.assertEqual("J44", summary["task"])
            self.assertEqual("real-HBase-MiniCluster-event-and-layout-tests", summary["evidence"])
            self.assertEqual(0, summary["go_test_count"])
            self.assertGreater(summary["java_test_count"], 0)

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

    def test_j42_registry_requires_keepalive_cases_and_their_actual_maven_module(self) -> None:
        required = m4_report.J42_CRITICAL_JAVA_TESTS
        task = next(item for item in self.registry["tasks"] if item["id"] == "J42")
        for class_name, methods in required.items():
            for method_name in methods:
                with self.subTest(class_name=class_name, method_name=method_name):
                    changed = copy.deepcopy(self.registry)
                    changed_task = next(item for item in changed["tasks"] if item["id"] == "J42")
                    changed_suite = next(item for item in changed_task["maven_tests"]
                                         if item["class"] == class_name)
                    changed_suite["tests"].remove(method_name)
                    with self.assertRaises(m4_report.ReportError):
                        m4_report.validate_registry_document(changed)

        self.assertEqual({"core", "server"}, {suite["module"] for suite in task["maven_tests"]})
        actual_modules = {suite["class"]: suite["module"] for suite in task["maven_tests"]}
        self.assertEqual(m4_report.J42_JAVA_CLASS_MODULES, actual_modules)
        wrong_module = copy.deepcopy(self.registry)
        wrong_task = next(item for item in wrong_module["tasks"] if item["id"] == "J42")
        stream_suite = next(item for item in wrong_task["maven_tests"]
                            if item["class"] == "io.etcdhbase.server.commit.LeaseKeepAliveStreamTest")
        stream_suite["module"] = "grpc"
        with self.assertRaisesRegex(m4_report.ReportError, "collected from their declared modules"):
            m4_report.validate_registry_document(wrong_module)

        missing_terminal = copy.deepcopy(self.registry)
        missing_task = next(item for item in missing_terminal["tasks"] if item["id"] == "J42")
        terminal_case = next(item for item in missing_task["trace_cases"]
                             if item["id"] == "keepalive-stream-a-repeat")
        terminal_case.pop("stream_terminal")
        with self.assertRaisesRegex(m4_report.ReportError, "stream terminal coverage"):
            m4_report.validate_registry_document(missing_terminal)

    def test_j43_registry_requires_expiry_scan_drain_and_owner_fence_tests(self) -> None:
        for class_name, method_name in (
            ("io.etcdhbase.server.commit.LeaseExpiryStepExecutorTest",
             "expiryPageBudgetRemainsReservedUntilThePhysicalScanDrains"),
            ("io.etcdhbase.server.commit.LeaseExpiryWorkerTest",
             "successfulCloseWaitsForBothCleanupAndPhysicalProducerCompletion"),
            ("io.etcdhbase.server.commit.LeaseExpiryWorkerTest",
             "staleExpiryWorkerDoesNotForwardAfterOwnerChangesDuringCheckpointRead"),
        ):
            with self.subTest(class_name=class_name, method_name=method_name):
                changed = copy.deepcopy(self.registry)
                task = next(item for item in changed["tasks"] if item["id"] == "J43")
                suite = next(item for item in task["maven_tests"] if item["class"] == class_name)
                suite["tests"].remove(method_name)
                with self.assertRaisesRegex(m4_report.ReportError,
                                            "checkpoint, clock, race, or mixed-load"):
                    m4_report.validate_registry_document(changed)

        without_step_executor = copy.deepcopy(self.registry)
        task = next(item for item in without_step_executor["tasks"] if item["id"] == "J43")
        task["maven_tests"] = [suite for suite in task["maven_tests"]
                               if suite["class"] != "io.etcdhbase.server.commit.LeaseExpiryStepExecutorTest"]
        with self.assertRaisesRegex(m4_report.ReportError, "expiry, Revoke, race"):
            m4_report.validate_registry_document(without_step_executor)

    def test_complete_j42_artifact_is_read_from_its_registered_subdirectory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J42")
            summary = m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)
            self.assertEqual(summary["task"], "J42")
            self.assertEqual(summary["target_layouts"], ["HISTORY", "NATIVE"])
            self.assertEqual(summary["keepalive_sampling_method"],
                             "per-runtime-cumulative-counter-snapshot")
            self.assertGreater(summary["keepalive_rpc_requests"], 0)
            self.assertGreater(summary["keepalive_queue_wait_samples"], 0)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J42")
            evidence = artifact / "j42"
            (evidence / "keepalive-history.log").unlink()
            with self.assertRaisesRegex(m4_report.ReportError, "comparison"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_complete_j43_artifact_requires_source_bound_mixed_load_and_takeover_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J43")
            summary = m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)
            self.assertEqual(["HISTORY", "NATIVE"], summary["j29_4_layouts"])
            self.assertEqual([1, 2, 4], summary["j29_4_concurrency"])
            self.assertEqual(6001, summary["owner_takeover_millis"])

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J43")
            report_path = artifact / "j43/mixed-lease-load.json"
            report_path.unlink()
            with self.assertRaisesRegex(m4_report.ReportError, "mixed-lease-load.json"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J43")
            report_path = artifact / "j43/mixed-lease-load.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["layouts"][0]["concurrency"][2]["expiry"]["stale_candidates"] = 0
            report_path.write_text(json.dumps(report) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(m4_report.ReportError, "expiry metric stale_candidates"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J43")
            report_path = artifact / "j43/mixed-lease-load.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["owner_takeover"]["new_owner_epoch"] = report["owner_takeover"]["old_owner_epoch"]
            report_path.write_text(json.dumps(report) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(m4_report.ReportError, "owner takeover epoch"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J43")
            report_path = artifact / "j43/mixed-lease-load.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["owner_takeover"]["new_owner_instance"] = "owner-b"
            report_path.write_text(json.dumps(report) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(m4_report.ReportError, "owner incarnation"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_j43_mixed_load_reports_resource_rejection_without_counting_it_as_success(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J43")
            report_path = artifact / "j43/mixed-lease-load.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))
            row = report["layouts"][0]["concurrency"][1]
            row["keepalive_client_resource_exhausted"] = 1
            row["business_put_resource_exhausted"] = 1
            row["keepalive_responses"] = 47
            row["selected_renewals"] = 47
            row["business_puts"] = 47
            row["combined_operations"] = 94
            row["keepalive_client_rpc_p99_samples"] = 47
            row["keepalive_client_rpc_latency_nanos"]["samples"] = 47
            row["business_put_client_rpc_latency_nanos"]["samples"] = 47
            row["owner_queue_wait_samples"] = 47
            row["owner_queue_wait_total_nanos"] = 4700
            row["unknown_outcomes"] = 1
            report_path.write_text(json.dumps(report) + "\n", encoding="utf-8")
            m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

            row["business_puts"] = 48
            report_path.write_text(json.dumps(report) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(m4_report.ReportError, "workload counts"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_j42_trace_terminals_match_registered_stream_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary)
            self._write_valid_artifact(artifact, "J42")
            trace_path = artifact / "j42/native-trace.jsonl"
            records = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual({"kind": "eof"}, records[9]["stream_terminal"])
            records[0]["stream_terminal"] = {"kind": "eof"}
            trace_path.write_text(
                "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
            )
            with self.assertRaisesRegex(m4_report.ReportError, "registered stream contract"):
                m4_report.validate_artifact(m4_report.REPO, artifact, self.registry)

    def test_j42_report_rejects_missing_or_malformed_runtime_metrics(self) -> None:
        for replacement, message in (
            (None, "do not cover both HBase layouts"),
            ("M4_KEEPALIVE_METRICS_V1 requests=8", "metric sample method is missing"),
        ):
            with self.subTest(replacement=replacement), tempfile.TemporaryDirectory() as temporary:
                artifact = Path(temporary)
                self._write_valid_artifact(artifact, "J42")
                report_path = artifact / (
                    "server/target/failsafe-reports/TEST-io.etcdhbase.server.M4LeaseKeepAliveRpcIT.xml"
                )
                report_tree = ET.parse(report_path)
                output = report_tree.getroot().find("system-out")
                assert output is not None
                if replacement is None:
                    report_tree.getroot().remove(output)
                else:
                    output.text = f"{replacement}\n{replacement}\n"
                report_tree.write(report_path, encoding="utf-8", xml_declaration=True)
                with self.assertRaisesRegex(m4_report.ReportError, message):
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
            "io.etcdhbase.server.commit.LeaseRevocationPreparerTest":
                "cancelledWaiterReclaimsALateSuccessfulPreparedResult",
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
        command_logs = self._write_command_logs(artifact, task_id, run_id)
        commands = self._command_records(command_logs, task_id, artifact, run_id)
        self._write_run_manifest(artifact, task["id"], run_id, source_commit, base_commit, commands)
        self._write_go_test_events(artifact, task)
        self._write_maven_test_reports(artifact, task)
        suite_hash, prefix_hash = m4_report._scenario_hashes(m4_report.REPO, task, run_id)
        self._write_trace_artifacts(
            evidence_root, task, run_id, source_commit, suite_hash, prefix_hash
        )
        if task_id == "J45":
            self._write_j45_local_service_artifacts(
                evidence_root, task, run_id, source_commit, suite_hash
            )
        if task.get("evidence", {}).get("compare_logs"):
            self._write_comparison_logs(evidence_root, task)
        if task_id == "J43":
            self._write_valid_j29_4_evidence(evidence_root, run_id, source_commit)

    @staticmethod
    def _refresh_trace_digest(artifact: Path, trace_path: Path, metadata_name: str) -> None:
        metadata_path = artifact / metadata_name
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["trace_sha256"] = hashlib.sha256(trace_path.read_bytes()).hexdigest()
        metadata_path.write_text(json.dumps(metadata) + "\n", encoding="utf-8")

    @staticmethod
    def _write_command_logs(artifact: Path, task_id: str, run_id: str) -> dict[str, str]:
        if task_id == "J45":
            command_logs = {
                "registry-check": "registry-check.log",
                "report-tests": "report-tests.log",
                "go-tests": "go-test.jsonl",
                "native-watch-oracle": "j45/native.log",
                "maven-unit-tests": "maven.log",
                "collect-maven-reports": "maven-reports.log",
            }
            stderr_logs = {
                "registry-check": "registry-check.stderr",
                "report-tests": "report-tests.stderr",
                "go-tests": "go-test.stderr",
                "native-watch-oracle": "j45/native.stderr",
                "maven-unit-tests": "maven.stderr.log",
                "collect-maven-reports": "maven-reports.stderr",
            }
            for path in [*command_logs.values(), *stderr_logs.values()]:
                target = artifact / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("completed\n" if path != "go-test.jsonl" else "", encoding="utf-8")
            return command_logs
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
    def _command_records(
        command_logs: dict[str, str], task_id: str, artifact: Path, run_id: str,
    ) -> list[dict[str, object]]:
        if task_id == "J45":
            go_test_pattern = (
                "^(TestM4WatchStartScenarioMatchesPinnedNativeOracle|"
                "TestM4WatchLifecycleMatchesPinnedNativeOracle|"
                "TestLoadM4WatchStartSuiteRejectsMissingOrChangedCoverage|"
                "TestRunNativeWatchStartWritesFreshTraceAndVerifiableMetadata|"
                "TestParseRunOptionsPinsLocalWatchPhaseAndCheckpoint|"
                "TestOpenEvidenceWritesLocalWatchCheckpointMetadata|"
                "TestValidateM4LeaseAcceptsLocalServiceEvidenceScope|"
                "TestTraceAcceptsLocalServiceEvidenceScope)$"
            )
            prefix = f"etcd-hbase-m4-test/{run_id}/watch/"
            argv_by_name = {
                "registry-check": ["python3", "scripts/verify/m4-report.py", "--validate-registry"],
                "report-tests": ["python3", "scripts/verify/test_m4_report.py"],
                "go-tests": ["scripts/dev/go-m0.sh", "test", "-json", "-count=1", "-run",
                             go_test_pattern, "./internal/scenario", "./cmd/m4driver",
                             "./internal/trace"],
                "native-watch-oracle": [
                    "scripts/dev/go-m0.sh", "run", "./cmd/m4driver", "run", "--scenario",
                    "../scenarios/m4-watch-starts.yaml", "--native-data-dir",
                    str(artifact / "j45/native-data"), "--scope", "native-etcd", "--prefix", prefix,
                    "--trace", str(artifact / "j45/native-trace.jsonl"), "--metadata",
                    str(artifact / "j45/native-metadata.json"),
                ],
                "maven-unit-tests": ["scripts/dev/mvn21.sh", "-B", "-ntp", "-pl", "server", "-am",
                                     "clean", "test", "-Pm4-j45-public"],
                "collect-maven-reports": ["collect-maven-reports"],
            }
            stderr_by_name = {
                "registry-check": "registry-check.stderr", "report-tests": "report-tests.stderr",
                "go-tests": "go-test.stderr", "native-watch-oracle": "j45/native.stderr",
                "maven-unit-tests": "maven.stderr.log",
                "collect-maven-reports": "maven-reports.stderr",
            }
            return [
                {
                    "name": name,
                    "argv": argv_by_name[name],
                    "exit_code": 0,
                    "started_at": "2026-11-17T00:00:00Z",
                    "finished_at": "2026-11-17T00:00:01Z",
                    "stdout_log": path,
                    "stderr_log": stderr_by_name[name],
                }
                for name, path in command_logs.items()
            ]
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
            "scope": "public-local-unit" if task_id == "J45" else "public-local-minicluster",
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
            if (task.get("id") == "J42"
                    and class_name == "io.etcdhbase.server.M4LeaseKeepAliveRpcIT"):
                metrics = {
                    "sampling_method": "per-runtime-cumulative-counter-snapshot",
                    "requests": "8", "responses": "8", "selected": "8", "unknown_outcomes": "0",
                    "request_bytes": "64", "response_bytes": "64", "auxiliary_records": "8",
                    "auxiliary_payload_bytes": "256", "hbase_client_operations": "16",
                    "logical_hbase_request_payload_bytes": "512",
                    "queue_samples": "8", "queue_total_nanos": "8000",
                    "prepare_samples": "8", "prepare_total_nanos": "16000",
                    "publish_and_finalize_samples": "8", "publish_and_finalize_total_nanos": "24000",
                    "result_resolution_samples": "8", "result_resolution_total_nanos": "8000",
                    "delivery_validation_samples": "8", "delivery_validation_total_nanos": "4000",
                    "outbound_ready_wait_samples": "8", "outbound_ready_wait_total_nanos": "12000",
                }
                output = ET.SubElement(root, "system-out")
                sample = "M4_KEEPALIVE_METRICS_V1 " + " ".join(
                    f"{name}={value}" for name, value in metrics.items()
                )
                output.text = f"{sample}\n{sample}\n"
            ET.ElementTree(root).write(
                report_dir / f"TEST-{class_name}.xml", encoding="utf-8", xml_declaration=True
            )

    @staticmethod
    def _trace_variants(task: dict[str, object]) -> list[tuple[dict[str, object], str, str, str]]:
        evidence = task["evidence"]
        if task.get("id") == "J45":
            return [(evidence["native"], "native", "native-trace.jsonl", "native-metadata.json")]
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

    @classmethod
    def _write_j45_local_service_artifacts(
        cls, evidence_root: Path, task: dict[str, object], run_id: str,
        source_commit: str, suite_hash: str,
    ) -> None:
        local_root = evidence_root / "local-service"
        local_root.mkdir(parents=True, exist_ok=True)
        native_records = cls._valid_j45_trace_records(task, {"scope": "native-etcd"})
        descriptor = task["evidence"]["local_service"]
        for phase, revision, compact_revision in m4_report.J45_LOCAL_SERVICE_PHASES:
            records = cls._valid_j45_local_trace_records(
                native_records, phase, revision, compact_revision
            )
            trace_bytes = "".join(
                json.dumps(record, separators=(",", ":")) + "\n" for record in records
            ).encode()
            (local_root / f"{phase}-trace.jsonl").write_bytes(trace_bytes)
            prefix = f"etcd-hbase-m4-test/{run_id}/watch/{phase}/"
            metadata = {
                "schema": "etcd-hbase-m4-run/v1", "run_id": run_id,
                "source_commit": source_commit, "suite_sha256": suite_hash,
                "evidence_scope": descriptor["scope"],
                "prefix_sha256": hashlib.sha256(prefix.encode()).hexdigest(),
                "trace_sha256": hashlib.sha256(trace_bytes).hexdigest(),
                "embedded_native": descriptor["embedded_native"], "layout": "",
                "watch_phase": phase, "watch_revision": revision,
                "watch_compact_revision": compact_revision, "status": "passed",
            }
            (local_root / f"{phase}-metadata.json").write_text(
                json.dumps(metadata) + "\n", encoding="utf-8"
            )
            (local_root / f"{phase}.log").write_text(
                "M4 Watch start scenario passed\n",
                encoding="utf-8",
            )
        lifecycle_root = evidence_root / "local-lifecycle"
        lifecycle_root.mkdir(parents=True, exist_ok=True)
        (lifecycle_root / "initial.log").write_text(
            "M4 Watch start scenario passed\nM4 Watch lifecycle interop passed\n",
            encoding="utf-8",
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
        marker = "M4 Watch start scenario passed" if task["id"] == "J45" else "M4 Lease scenario passed"
        (evidence_root / log_name).write_text(marker + "\n", encoding="utf-8")

    @staticmethod
    def _valid_trace_records(
        task: dict[str, object], descriptor: dict[str, object],
    ) -> list[dict[str, object]]:
        if task["id"] == "J45":
            return M4ReportTest._valid_j45_trace_records(task, descriptor)
        if task["id"] == "J41":
            return M4ReportTest._valid_binding_trace_records(task, descriptor)
        if task["id"] == "J42":
            return M4ReportTest._valid_keepalive_trace_records(task, descriptor)
        if task["id"] == "J43":
            return M4ReportTest._valid_revoke_trace_records(task, descriptor)
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

    @classmethod
    def _j45_header(cls, revision: int) -> dict[str, object]:
        return cls._snapshot({
            "clusterId": "11", "memberId": "22", "raftTerm": "1",
            "revision": str(revision),
        })

    @classmethod
    def _j45_watch_response(
        cls, watch_id: int, revision: int, *, created: bool, canceled: bool,
        compact_revision: int = 0, reason: str = "",
    ) -> dict[str, object]:
        return cls._snapshot({
            "header": cls._j45_header(revision), "watchId": str(watch_id),
            "created": created, "canceled": canceled,
            "compactRevision": str(compact_revision), "cancelReason": reason, "events": [],
        })

    @classmethod
    def _j45_create_request(
        cls, watch_id: int, start_revision: int, key: str = "aW5pdGlhbC1taW51cy1vbmU=",
    ) -> dict[str, object]:
        return {
            "fields": {"createRequest": cls._snapshot({
                "key": key, "watchId": str(watch_id),
                "startRevision": str(start_revision),
            })},
            "oneofs": {"request_union": "create_request"},
        }

    @classmethod
    def _j45_cancel_request(cls, watch_id: int) -> dict[str, object]:
        return {
            "fields": {"cancelRequest": cls._snapshot({"watchId": str(watch_id)})},
            "oneofs": {"request_union": "cancel_request"},
        }

    @classmethod
    def _j45_fixed_fixture_cases(cls) -> dict[str, tuple[dict[str, object], dict[str, object]]]:
        initial_revision = 2
        compact_revision = 3
        history_key = "aW5pdGlhbC1taW51cy1vbmU="
        history_value = "ajQ1LWhpc3Rvcnk="
        fixture_key = "am00"
        fixture_value = "ajQ1LXdhdGNoLWNvbXBhY3Q="
        initial_header = {"header": cls._j45_header(initial_revision)}
        compact_header = {"header": cls._j45_header(compact_revision)}
        return {
            "watch-start-history-put": (
                cls._snapshot({"key": history_key, "value": history_value}),
                cls._snapshot(initial_header),
            ),
            "watch-negative-live-put": (
                cls._snapshot({"key": history_key, "value": "ajQ1LXdhdGNoLWxpdmU="}),
                cls._snapshot({"header": cls._j45_header(initial_revision + 1)}),
            ),
            "watch-compact-fixture-put": (
                cls._snapshot({"key": fixture_key, "value": fixture_value}),
                cls._snapshot(compact_header),
            ),
            "watch-compact-fixture-range": (
                cls._snapshot({"key": fixture_key}),
                cls._snapshot({
                    "header": cls._j45_header(compact_revision),
                    "kvs": [cls._snapshot({"key": fixture_key, "value": fixture_value})],
                }),
            ),
            "watch-compact-fixture-compact": (
                cls._snapshot({"revision": str(compact_revision)}),
                cls._snapshot(compact_header),
            ),
        }

    @classmethod
    def _j45_negative_live_case(
        cls, case_id: str,
    ) -> tuple[dict[str, object], dict[str, object]]:
        initial_revision = 2
        live_revision = initial_revision + 1
        history_key = "aW5pdGlhbC1taW51cy1vbmU="
        live_value = "ajQ1LXdhdGNoLWxpdmU="
        if case_id.endswith("-created"):
            return (cls._j45_create_request(45, -1),
                    cls._j45_watch_response(45, initial_revision, created=True, canceled=False))
        if case_id.endswith("-progress"):
            return ({
                "fields": {"progressRequest": cls._snapshot({})},
                "oneofs": {"request_union": "progress_request"},
            }, cls._snapshot({
                "header": cls._j45_header(initial_revision), "watchId": "-1",
                "created": False, "canceled": False, "compactRevision": "0",
                "cancelReason": "", "events": [],
            }))
        if case_id.endswith("-event"):
            return (cls._j45_create_request(45, -1), cls._snapshot({
                "header": cls._j45_header(live_revision), "watchId": "45",
                "created": False, "canceled": False, "compactRevision": "0",
                "cancelReason": "", "events": [cls._snapshot({
                    "type": 0,
                    "kv": cls._snapshot({
                        "key": history_key, "createRevision": str(initial_revision),
                        "modRevision": str(live_revision), "version": "2",
                        "value": live_value, "lease": "0",
                    }),
                })],
            }))
        if case_id.endswith("-cancelled"):
            return (cls._j45_cancel_request(45),
                    cls._j45_watch_response(45, live_revision, created=False, canceled=True))
        raise AssertionError(f"unregistered J45 negative-live trace case {case_id}")

    @classmethod
    def _j45_start_matrix_case(
        cls, case_id: str,
    ) -> tuple[dict[str, object], dict[str, object]]:
        if case_id.startswith("initial-"):
            phase = "initial"
            response_revision = 2
            compact_boundary = -1
        elif case_id.startswith("compacted-"):
            phase = "compacted"
            response_revision = 3
            compact_boundary = 3
        else:
            raise AssertionError(f"unregistered J45 Watch matrix phase in {case_id}")

        if case_id.startswith(f"{phase}-watch-id-minus-one-"):
            base_id = f"{phase}-watch-id-minus-one"
            start_revision = 0
            watch_id = -1
        else:
            start_revisions = {"minus-two": -2, "minus-one": -1, "zero": 0, "future": 3}
            start_name = next(
                (name for name in start_revisions if case_id.startswith(f"{phase}-{name}-")),
                None,
            )
            if start_name is None:
                raise AssertionError(f"unregistered J45 Watch start case {case_id}")
            start_revision = start_revisions[start_name]
            base_id = f"{phase}-{start_name}"
            watch_id = {-2: 41, -1: 42, 0: 43, 3: 44}[start_revision]

        suffix = case_id[len(base_id) + 1:]
        if suffix == "created":
            return (cls._j45_create_request(watch_id, start_revision),
                    cls._j45_watch_response(watch_id, response_revision,
                                            created=True, canceled=False))
        if suffix == "compacted":
            return (cls._j45_create_request(watch_id, start_revision),
                    cls._j45_watch_response(watch_id, 0, created=False, canceled=True,
                                            compact_revision=compact_boundary))
        if suffix == "duplicate-id":
            return (cls._j45_create_request(watch_id, 0), cls._j45_watch_response(
                -1, response_revision, created=True, canceled=True,
                reason="mvcc: duplicate watch ID provided on the WatchStream",
            ))
        if suffix in {"cancel-id", "cancelled", "reused-cancelled"}:
            return (cls._j45_cancel_request(watch_id),
                    cls._j45_watch_response(watch_id, response_revision,
                                            created=False, canceled=True))
        if suffix == "reused-created":
            return (cls._j45_create_request(watch_id, 0),
                    cls._j45_watch_response(watch_id, response_revision,
                                            created=True, canceled=False))
        raise AssertionError(f"unregistered J45 Watch matrix suffix in {case_id}")

    @staticmethod
    def _j45_trace_record(
        case_id: str, method: str, request: dict[str, object], response: dict[str, object],
        evidence_scope: str,
    ) -> dict[str, object]:
        return {
            "schema": "etcd-hbase-trace/v1", "case_id": case_id, "ordinal": 1,
            "method": method, "request": request, "response": response,
            "error": None, "stream_terminal": None, "evidence_scope": evidence_scope,
            "elapsed_ns": "1",
        }

    @classmethod
    def _valid_j45_trace_records(
        cls, task: dict[str, object], descriptor: dict[str, object],
    ) -> list[dict[str, object]]:
        fixed_cases = cls._j45_fixed_fixture_cases()
        records = []
        for expected in task["trace_cases"]:
            case_id = expected["id"]
            setup = fixed_cases.get(case_id)
            if setup is None:
                if case_id.startswith("watch-negative-live-"):
                    setup = cls._j45_negative_live_case(case_id)
                else:
                    setup = cls._j45_start_matrix_case(case_id)
            request, response = setup
            records.append(cls._j45_trace_record(
                case_id, expected["method"], request, response, descriptor["scope"]
            ))
        return records

    @staticmethod
    def _valid_j45_local_trace_records(
        native_records: list[dict[str, object]], phase: str, revision: int,
        compact_revision: int,
    ) -> list[dict[str, object]]:
        records = [copy.deepcopy(record) for record in native_records
                   if record["case_id"].startswith(f"{phase}-")]
        for record in records:
            record["evidence_scope"] = "local-service"
            response = record["response"]["fields"]
            response_header = response["header"]["fields"]
            response_header["raftTerm"] = "0"
            if record["case_id"].endswith("-compacted"):
                response_header["revision"] = "0"
                response["compactRevision"] = str(compact_revision)
            else:
                response_header["revision"] = str(revision)
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
    def _valid_keepalive_trace_records(
        task: dict[str, object], descriptor: dict[str, object],
    ) -> list[dict[str, object]]:
        case_ordinals: dict[str, int] = {}
        records: list[dict[str, object]] = []
        for expected in task["trace_cases"]:
            case_id = expected["id"]
            case_ordinals[case_id] = case_ordinals.get(case_id, 0) + 1
            records.append({
                "schema": "etcd-hbase-trace/v1",
                "case_id": case_id,
                "ordinal": case_ordinals[case_id],
                "method": expected["method"],
                "request": M4ReportTest._snapshot({}),
                "response": M4ReportTest._snapshot({}),
                "error": None,
                "stream_terminal": expected.get("stream_terminal"),
                "evidence_scope": descriptor["scope"],
                "elapsed_ns": "1",
            })
        return records

    @staticmethod
    def _valid_revoke_trace_records(
        task: dict[str, object], descriptor: dict[str, object],
    ) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        case_ordinals: dict[str, int] = {}
        for expected in task["trace_cases"]:
            case_id = expected["id"]
            case_ordinals[case_id] = case_ordinals.get(case_id, 0) + 1
            error = None
            response = M4ReportTest._snapshot({})
            if case_id == "revoke-missing":
                error = {
                    "grpc_code": "NotFound",
                    "consumer_kind": "grpc-status",
                    "semantic_kind": "LEASE_NOT_FOUND",
                }
                response = None
            records.append({
                "schema": "etcd-hbase-trace/v1",
                "case_id": case_id,
                "ordinal": case_ordinals[case_id],
                "method": expected["method"],
                "request": M4ReportTest._snapshot({}),
                "response": response,
                "error": error,
                "stream_terminal": expected.get("stream_terminal"),
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
        success = {
            "J41": m4_report.J41_COMPARE_SUCCESS,
            "J42": m4_report.J42_COMPARE_SUCCESS,
            "J43": m4_report.J43_COMPARE_SUCCESS,
        }.get(task["id"], m4_report.COMPARE_SUCCESS)
        for name in task["evidence"]["compare_logs"]:
            (evidence_root / name).write_text(success + "\n", encoding="utf-8")


    def _write_valid_j29_4_evidence(self, evidence_root: Path, run_id: str,
                                    source_commit: str) -> None:
        source_files = {
            relative: hashlib.sha256((m4_report.REPO / relative).read_bytes()).hexdigest()
            for relative in m4_report.J29_4_SOURCE_FILES
        }
        source_digest = hashlib.sha256()
        for relative in m4_report.J29_4_SOURCE_FILES:
            source_digest.update(relative.encode("utf-8"))
            source_digest.update(b"\0")
            source_digest.update(source_files[relative].encode("ascii"))
            source_digest.update(b"\0")

        layouts = []
        for layout in ("HISTORY", "NATIVE"):
            concurrency = []
            for count in (1, 2, 4):
                requests = count * 24 if count != 4 else 48
                expiry = None
                run_mode = "fixed-iterations"
                iterations = 24
                if count == 4:
                    run_mode = "until-expiry-revocation"
                    iterations = 0
                    expiry = {
                        "expired_during_mixed_load": True,
                        "active_workers_at_revoke": 4,
                        "revocations": 1,
                        "stale_candidates": 1,
                        "candidates": 2,
                        "scan_rows": 2,
                        "scan_bytes": 512,
                        "retries": 0,
                        "trusted_delay_samples": 1,
                        "missing_trusted_delay_samples": 0,
                        "trusted_delay_total_millis": 12,
                        "observed_deadline_to_revoke_millis": 37,
                        "owner_epoch": 1,
                        "checkpoint_sequence": 24,
                        "window_stop_exclusive": 1_000,
                    }
                row = {
                    "layout": layout,
                    "concurrency": count,
                    "run_mode": run_mode,
                    "iterations_per_worker": iterations,
                    "elapsed_nanos": 1_000_000,
                    "keepalive_requests": requests,
                    "keepalive_client_attempts": requests,
                    "keepalive_client_resource_exhausted": 0,
                    "keepalive_responses": requests,
                    "selected_renewals": requests,
                    "business_puts": requests,
                    "business_put_attempts": requests,
                    "business_put_resource_exhausted": 0,
                    "combined_operations": requests * 2,
                    "keepalive_client_rpc_p99_samples": requests,
                    "keepalive_client_rpc_latency_nanos": {
                        "samples": requests, "p50_nanos": 1_000, "p95_nanos": 2_000,
                        "p99_nanos": 3_000,
                    },
                    "business_put_client_rpc_latency_nanos": {
                        "samples": requests, "p50_nanos": 2_000, "p95_nanos": 3_000,
                        "p99_nanos": 4_000,
                    },
                    "owner_queue_wait_samples": requests,
                    "owner_queue_wait_total_nanos": requests * 100,
                    "owner_queue_wait_mean_nanos": 100,
                    "renewal_logical_hbase_client_operations": requests * 4,
                    "renewal_logical_hbase_request_payload_bytes": requests * 128,
                    "logical_hbase_operations_per_server_keepalive_request": 4.0,
                    "logical_hbase_request_payload_bytes_per_server_keepalive_request": 128.0,
                    "unknown_outcomes": 0,
                    "request_proto_bytes": requests * 16,
                    "response_proto_bytes": requests * 24,
                }
                if expiry is not None:
                    row["expiry"] = expiry
                concurrency.append(row)
            layouts.append({"layout": layout, "concurrency": concurrency})

        report = {
            "schema": "etcd-hbase-m4-j29.4/v1",
            "status": "passed",
            "run_id": run_id,
            "source_commit": source_commit,
            "source_files": source_files,
            "source_sha256": source_digest.hexdigest(),
            "scope": "public-local-minicluster",
            "protocol": "etcd-v3.5.23",
            "target_capacity_accepted": False,
            "physical_hbase_rpc_bytes_collected": False,
            "recovery_reserve_usage_measured": False,
            "listener_max_concurrent_requests": 8,
            "listener_max_queued_requests": 0,
            "latency_sampling": "successful-client-rpcs",
            "recovery_reserve": {
                "scope": "configured-per-table-group",
                "requests": 2,
                "queued": 0,
                "operations": 2,
                "scanners": 1,
                "cleanup": 2,
                "queued_bytes": 0,
                "memory_bytes": 87162880,
                "expiry_worker_memory_bytes": 1118208,
                "expiry_worker_read_physical_rows": 16,
                "expiry_worker_read_cell_bytes": 1048576,
                "runtime_usage_peak_measured": False,
            },
            "layouts": layouts,
            "owner_takeover": {
                "scenario": "two-runtime-mtls-owner-takeover",
                "old_owner_epoch": 1,
                "new_owner_epoch": 2,
                "new_owner": "owner-b",
                "new_owner_instance": "~PDG6jU2rnh9F3EmdeI0dtEnJFhDBG5ESKxlM1-kF_io:00000000-0000-0000-0000-000000000001",
                "elapsed_millis": 6001,
            },
        }
        (evidence_root / "mixed-lease-load.json").write_text(
            json.dumps(report, sort_keys=True) + "\n", encoding="utf-8"
        )


if __name__ == "__main__":
    unittest.main()
