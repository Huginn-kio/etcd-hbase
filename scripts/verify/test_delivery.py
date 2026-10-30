#!/usr/bin/env python3
"""Negative controls for release inputs, reproducibility, runtime classpaths and probes."""
import hashlib
import importlib.util
from contextlib import contextmanager, redirect_stdout
import errno
import io
import json
import os
import signal
import subprocess
from pathlib import Path
import shutil
import sys
import tempfile
import time
import unittest
from unittest import mock
import zipfile
from runtime_probe import await_transport, check_observation
from build_process import run_logged, stop_group

ROOT = Path(__file__).resolve().parents[2]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


release = load("release_readiness", "release-readiness.py")
package = load("package_runtime", "package-runtime.py")
repro = load("reproducible", "maven-reproducible-build.py")
image = load("runtime_image", "runtime-image.py")
packaged = load("packaged_runtime", "packaged-runtime.py")


@contextmanager
def _controlled_process_group():
    program = "import os,signal,sys; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); sys.stdin.readline(); os._exit(0)"
    process = subprocess.Popen([sys.executable, "-c", program], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, start_new_session=True)
    try:
        if process.stdout.readline() != b"ready\n":
            raise AssertionError("controlled process group did not become ready")
        yield process
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        process.stdin.close()
        process.stdout.close()


def _finish_as_zombie(process):
    process.stdin.write(b"exit\n")
    process.stdin.flush()
    deadline = time.monotonic() + 5
    while True:
        state = subprocess.run(["ps", "-p", str(process.pid), "-o", "stat="], capture_output=True, text=True)
        if state.stdout.strip().startswith("Z"):
            return
        if state.stderr or time.monotonic() >= deadline:
            raise AssertionError("owned unreaped exit was not observed: " + state.stderr)
        time.sleep(0.01)


class DeliveryTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def test_missing_release_decisions_cannot_become_implicit_approval(self):
        (self.root / "build").mkdir()
        path = self.root / "build/release-decisions.json"
        shutil.copyfile(ROOT / "build/release-decisions.json", path)
        self.assertEqual(["I07", "I08"], release.inspect(self.root)["waiting_inputs"])
        data = json.loads(path.read_text())
        data["I07"].update(state="approved", license_spdx="fixture-license", third_party_notice_reviewed=True, approval_reference="fixture-reference")
        path.write_text(json.dumps(data))
        with self.assertRaises(ValueError):
            release.inspect(self.root)
        path.write_text('{"schema_version":1,"schema_version":1,"I07":{},"I08":{}}')
        with self.assertRaises(ValueError):
            release.inspect(self.root)

    def test_reproducibility_rejects_missing_and_changed_jars(self):
        first = {"server.jar": "first", "spi.jar": "same"}
        repro.compare_jars(first, dict(first))
        for second in ({}, {"spi.jar": "same"}, dict(first, **{"server.jar": "changed"})):
            with self.assertRaises(ValueError):
                repro.compare_jars(first, second)

    def jar(self, directory, name):
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("META-INF/NOTICE", "synthetic fixture notice")
        return path

    def test_runtime_preserves_order_and_rejects_stale_colliding_or_test_entries(self):
        server = self.jar(self.root / "project", "server.jar")
        dependency = self.jar(self.root / "repository", "dependency.jar")
        expected = {server.name: hashlib.sha256(server.read_bytes()).hexdigest()}
        ordered = package.validate_runtime_jars([server, dependency], expected)
        self.assertEqual([server.resolve(), dependency.resolve()], [path for path, _ in ordered])
        colliding = self.jar(self.root / "other-group", "dependency.jar")
        test_jar = self.jar(self.root / "repository", "hbase-tests.jar")
        testkit = self.jar(self.root / "repository", "testkit-0.1.0-SNAPSHOT.jar")
        legacy = self.jar(self.root / "repository", "protobuf-java-2.5.0.jar")
        for paths, hashes in (([server, dependency, colliding], expected), ([server, test_jar], expected),
                              ([server, legacy], expected), ([server, testkit], expected), ([server], {server.name: "wrong"}), ([dependency], expected)):
            with self.assertRaises(ValueError):
                package.validate_runtime_jars(paths, hashes)
        output = self.root / "output"
        output.mkdir()
        manifest = package.copy_jars(ordered, output)
        self.assertEqual(["META-INF/NOTICE"], manifest[0]["preserved_legal_entries"])
        self.assertEqual(0o755, (output / "lib").stat().st_mode & 0o777)
        self.assertEqual(0o644, (output / "lib/server.jar").stat().st_mode & 0o777)

    def observation(self, code="Unimplemented"):
        return {"case_id": "fixture", "evidence_scope": "transport", "method": "etcdserverpb.KV/Range", "response": None,
                "error": {"grpc_code": code}, "request": {"fields": {"key": "YQD/", "revision": "18014398509481984"}}}

    def test_runtime_probe_rejects_success_wrong_scope_and_int64_loss(self):
        record = self.observation()
        check_observation(json.dumps(record).encode() + b"\n", "fixture")
        for field, value in (("evidence_scope", "target-hbase"), ("response", {})):
            bad = dict(record, **{field: value})
            with self.assertRaises(ValueError):
                check_observation(json.dumps(bad).encode(), "fixture")
        record["request"]["fields"]["revision"] = 18014398509481984
        with self.assertRaises(ValueError):
            check_observation(json.dumps(record).encode(), "fixture")
        counter = self.root / "calls.txt"
        program = "from pathlib import Path; import sys; path=Path(sys.argv[1]); path.write_text(str(int(path.read_text())+1) if path.exists() else '1'); print(sys.argv[2])"
        with self.assertRaises(ValueError):
            await_transport([sys.executable, "-c", program, str(counter), json.dumps(self.observation())],
                            "fixture", self.root, timeout=2)
        self.assertEqual("1", counter.read_text(), "successful business response was retried")

    def test_group_cleanup_does_not_confuse_leader_exit_with_descendant_exit(self):
        pid_path = self.root / "child.pid"
        child = "import signal; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); signal.pause()"
        leader = "import subprocess,sys; from pathlib import Path; child=subprocess.Popen([sys.executable,'-c',sys.argv[2]],stdout=subprocess.PIPE); child.stdout.readline(); Path(sys.argv[1]).write_text(str(child.pid))"
        process = subprocess.Popen([sys.executable, "-c", leader, str(pid_path), child], start_new_session=True)
        try:
            process.wait(timeout=5)
            child_pid = int(pid_path.read_text())
            stop_group(process, grace=0.2)
            state = subprocess.run(["ps", "-p", str(child_pid), "-o", "stat="], capture_output=True, text=True).stdout.strip()
            self.assertTrue(not state or state.startswith("Z"), "TERM-ignoring descendant survived cleanup")
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)

    def test_group_cleanup_reaps_an_already_exited_leader(self):
        with _controlled_process_group() as process:
            _finish_as_zombie(process)
            stop_group(process, grace=0.2)
            self.assertEqual(0, process.returncode)

    def test_group_cleanup_handles_exit_between_poll_and_group_probe(self):
        with _controlled_process_group() as process:
            original_poll = process.poll
            released = False

            def poll_at_exit_boundary():
                nonlocal released
                result = original_poll()
                if result is None and not released:
                    released = True
                    _finish_as_zombie(process)
                return result

            with mock.patch.object(process, "poll", side_effect=poll_at_exit_boundary):
                stop_group(process, grace=0.2)
            self.assertTrue(released, "owned exit did not occur in the intended window")
            self.assertEqual(0, process.returncode)

    def test_group_permission_denial_for_a_live_leader_is_preserved(self):
        process = mock.Mock(pid=43210)
        process.poll.return_value = None
        error = PermissionError(errno.EPERM, "fixture group is not signalable")
        with mock.patch("build_process.os.killpg", side_effect=error):
            with self.assertRaises(PermissionError) as denied:
                stop_group(process, grace=0.01)
        self.assertIs(error, denied.exception)
        process.wait.assert_not_called()

    def test_group_permission_denial_after_leader_exit_is_preserved(self):
        process = mock.Mock(pid=43210)
        process.poll.return_value = 0
        error = PermissionError(errno.EPERM, "fixture remaining group is not signalable")
        with mock.patch("build_process.os.killpg", side_effect=error):
            with self.assertRaises(PermissionError) as denied:
                stop_group(process, grace=0.01)
        self.assertIs(error, denied.exception)
        process.wait.assert_not_called()

    def test_outer_cancellation_allows_the_resource_owner_to_finish_cleanup(self):
        pid_path = self.root / "child.pid"
        marker = self.root / "cleanup.finished"
        child = "import signal; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); signal.pause()"
        owner = """import subprocess,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from build_process import termination_is_exception,stop_group
with termination_is_exception():
 child=subprocess.Popen([sys.executable,'-c',sys.argv[4]],stdout=subprocess.PIPE,start_new_session=True)
 try:
  child.stdout.readline()
  Path(sys.argv[2]).write_text(str(child.pid))
  time.sleep(60)
 finally:
  time.sleep(0.3)
  stop_group(child,grace=0.05)
  Path(sys.argv[3]).write_text('finished')
"""
        try:
            with self.assertRaises(subprocess.TimeoutExpired):
                run_logged([sys.executable, "-c", owner, str(ROOT / "scripts/verify"), str(pid_path), str(marker), child],
                           self.root / "owner.log", ROOT, timeout=2, termination_grace=2)
            self.assertEqual("finished", marker.read_text())
            child_pid = int(pid_path.read_text())
            state = subprocess.run(["ps", "-p", str(child_pid), "-o", "stat="], capture_output=True, text=True).stdout.strip()
            self.assertTrue(not state or state.startswith("Z"), "owned resource survived outer cancellation")
        finally:
            if pid_path.exists():
                try:
                    os.killpg(int(pid_path.read_text()), signal.SIGKILL)
                except ProcessLookupError:
                    pass


class RuntimeImageLifecycleTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name) / "output"
        self.runtime = Path(directory.name) / "runtime"
        self.commands = []
        self.token = None
        self.built = False
        self.created = False
        self.failure = None
        self.container_id = "c" * 64
        self.image_id = "sha256:" + "d" * 64
        self.console = self.enterContext(redirect_stdout(io.StringIO()))
        self.enterContext(mock.patch.object(image.shutil, "which", return_value="docker"))
        self.enterContext(mock.patch.object(image, "run_logged", side_effect=self.build))
        self.enterContext(mock.patch.object(image.Docker, "call", autospec=True, side_effect=self.docker_call))
        self.probe = self.enterContext(mock.patch.object(image, "await_transport", return_value={"attempts": 3}))

    def build(self, command, log, cwd, **options):
        if command[0].endswith("go-m0.sh"):
            self.assertEqual("build", command[1])
            Path(command[command.index("-o") + 1]).write_bytes(b"fixture probe")
            return 0
        self.assertEqual(["docker", "--context", "default", "build"], command[:4])
        self.token = command[command.index("--label") + 1].split("=", 1)[1]
        self.built = True
        if self.failure == "build":
            raise subprocess.TimeoutExpired(command, 600)
        return 0

    def docker_call(self, docker, arguments, log=None, timeout=30):
        self.commands.append((arguments, timeout))
        operation = arguments[0]
        if operation == "info":
            if self.failure == "info":
                raise ValueError("fixture unavailable daemon")
            return "fixture daemon"
        if operation == "create":
            self.assertEqual("etcd-hbase.m0.verify=" + self.token, arguments[arguments.index("--label") + 1])
            self.created = True
            if self.failure == "create":
                raise subprocess.TimeoutExpired(arguments, timeout)
            return self.container_id
        if operation in ("container", "image") and arguments[1] == "ls":
            self.assertEqual("label=etcd-hbase.m0.verify=" + self.token, arguments[-1])
            resource_exists = self.created if operation == "container" else self.built
            return (self.container_id if operation == "container" else self.image_id) if resource_exists else ""
        if operation in ("container", "image") and arguments[1] == "rm":
            self.assertFalse((self.output / "summary.json").exists(), "passed report preceded owned cleanup")
            self.assertEqual([self.container_id if operation == "container" else self.image_id], arguments[3:])
            if self.failure == "cleanup":
                raise OSError("fixture removal failure")
            if operation == "container":
                self.created = False
            else:
                self.built = False
            return ""
        if operation == "inspect":
            if arguments[2] == "{{.Config.User}}":
                return "0:0" if self.failure == "user" else "65532:65532"
            return "137" if self.failure == "exit" else "143"
        if operation == "exec":
            (docker.output / log).write_text("java.specification.version = 21\njava.runtime.version = 21.0.12.1+1-LTS\njava.vendor = Eclipse Adoptium\n")
        if operation == "image" and arguments[1] == "inspect":
            return self.image_id
        if log and operation != "exec":
            (docker.output / log).write_text("fixture log\n")
        return ""

    def assert_released_without_pass(self):
        self.assertFalse(self.created)
        self.assertFalse(self.built)
        self.assertFalse((self.output / "summary.json").exists())

    def test_success_reports_only_after_owned_resource_removal(self):
        self.assertEqual(0, image.verify(self.runtime, self.output))
        summary = json.loads((self.output / "summary.json").read_text())
        lock = json.loads((ROOT / "build/dependency-lock.json").read_text())["runtimeImage"]
        self.assertEqual({"status": "passed", "scope": "runtime-image-transport", "image_test_passed": True,
                          "base": lock["reference"], "platform": "linux/amd64", "image_id": self.image_id,
                          "user": "65532:65532", "read_only": True, "network": "none",
                          "java": {"specification": "21", "runtime": "21.0.12.1+1-LTS", "vendor": "Eclipse Adoptium"},
                          "grpc_code": "Unimplemented", "attempts": 3, "exit_code": 143,
                          "backend_semantics_verified": False, "formal_image_published": False, "owned_resources_removed": True}, summary)
        self.assertEqual(summary, json.loads(self.console.getvalue()))
        self.assertFalse(self.created or self.built)

    def test_missing_cli_is_unavailable_without_allocating_resources(self):
        with mock.patch.object(image.shutil, "which", return_value=None):
            self.assertEqual(2, image.verify(self.runtime, self.output))
        self.assertEqual({"status": "unavailable", "reason": "docker-cli-unavailable", "image_test_passed": False},
                         json.loads((self.output / "summary.json").read_text()))
        self.assertEqual([], self.commands)

    def test_unavailable_daemon_is_distinct_from_image_failure(self):
        self.failure = "info"
        self.assertEqual(2, image.verify(self.runtime, self.output))
        self.assertEqual("default-local-docker-daemon-unavailable", json.loads((self.output / "summary.json").read_text())["reason"])
        self.assertFalse(self.created or self.built)

    def test_unknown_build_outcome_still_removes_labeled_image(self):
        self.failure = "build"
        with self.assertRaises(subprocess.TimeoutExpired):
            image.verify(self.runtime, self.output)
        self.assert_released_without_pass()

    def test_probe_preparation_failure_does_not_allocate_docker_resources(self):
        with mock.patch.object(image, "run_logged", return_value=1):
            with self.assertRaisesRegex(ValueError, "Linux runtime probe"):
                image.verify(self.runtime, self.output)
        self.assertEqual(["info"], [arguments[0] for arguments, _ in self.commands])
        self.assert_released_without_pass()

    def test_unknown_create_outcome_still_removes_labeled_container(self):
        self.failure = "create"
        with self.assertRaises(subprocess.TimeoutExpired):
            image.verify(self.runtime, self.output)
        self.assert_released_without_pass()

    def test_identity_rejection_still_releases_runtime(self):
        self.failure = "user"
        with self.assertRaisesRegex(ValueError, "unprivileged"):
            image.verify(self.runtime, self.output)
        self.assert_released_without_pass()

    def test_forced_exit_is_rejected_even_after_container_stop(self):
        self.failure = "exit"
        with self.assertRaisesRegex(ValueError, "forced termination"):
            image.verify(self.runtime, self.output)
        self.assert_released_without_pass()

    def test_probe_cancellation_releases_resources_and_propagates(self):
        self.probe.side_effect = KeyboardInterrupt("fixture cancellation")
        with self.assertRaises(KeyboardInterrupt):
            image.verify(self.runtime, self.output)
        self.assert_released_without_pass()

    def test_cleanup_failure_cannot_form_passed_summary(self):
        self.failure = "cleanup"
        with self.assertRaisesRegex(ValueError, "could not be fully released"):
            image.verify(self.runtime, self.output)
        self.assertFalse((self.output / "summary.json").exists())
        self.assertEqual(2, sum(arguments[1] == "rm" for arguments, _ in self.commands if arguments[0] in {"container", "image"}))

    def test_cleanup_budget_is_shared_across_resource_types(self):
        self.output.mkdir()
        self.token, self.built, self.created = "fixture", True, True
        with mock.patch.object(image.time, "monotonic", side_effect=[0, 10, 20, 29, 30.5]):
            with self.assertRaisesRegex(ValueError, "30 seconds"):
                image.Docker(self.output).cleanup(self.token)
        self.assertEqual([20, 10, 1], [timeout for _, timeout in self.commands])
        self.assertFalse(self.created)
        self.assertTrue(self.built, "image removal exceeded the remaining cleanup budget")


class PackagedRuntimeLifecycleTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        self.output = self.root / "output"
        (self.runtime / "server.json").write_text(json.dumps({"listeners": [{"port": 0}]}))
        self.write_server("signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(143))\nprint('listeners=1 data_readiness=UNIMPLEMENTED', flush=True)\nsignal.pause()\n")
        self.processes = []
        self.original_popen = subprocess.Popen
        self.force_cleanup_timeout = False
        self.enterContext(redirect_stdout(io.StringIO()))
        self.enterContext(mock.patch.dict(os.environ, {"ETCD_HBASE_JDK21_HOME": "/fixture/jdk", "JAVA_TOOL_OPTIONS": "fixture", "JDK_JAVA_OPTIONS": "fixture", "_JAVA_OPTIONS": "fixture"}))
        self.enterContext(mock.patch.object(packaged, "run_logged", return_value=0))
        self.enterContext(mock.patch.object(packaged.subprocess, "Popen", side_effect=self.launch))
        self.probe = self.enterContext(mock.patch.object(packaged, "await_transport", side_effect=self.observe))

    def write_server(self, program):
        path = self.runtime / "etcd-hbase"
        path.write_text(f"#!{sys.executable}\nimport signal,sys\n" + program)
        path.chmod(0o755)

    def launch(self, command, **options):
        self.assertEqual("/fixture/jdk", options["env"]["JAVA_HOME"])
        for variable in ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
            self.assertNotIn(variable, options["env"])
        process = self.original_popen(command, **options)
        self.processes.append(process)
        self.addCleanup(self.release_process, process)
        if self.force_cleanup_timeout:
            original_wait = process.wait
            def wait(timeout=None):
                if timeout == 15:
                    raise subprocess.TimeoutExpired(command, timeout)
                return original_wait(timeout=timeout)
            process.wait = wait
        return process

    def release_process(self, process):
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    def observe(self, command, case, output, alive):
        self.assertTrue(alive(), "probe observed a process it did not own")
        return {"attempts": 3}

    def assert_stopped_without_pass(self):
        self.assertEqual(1, len(self.processes))
        self.assertIsNotNone(self.processes[0].poll())
        self.assertFalse((self.output / "summary.json").exists())

    def test_success_stops_owned_process_and_retains_summary(self):
        summary = packaged.verify(self.runtime, self.output)
        self.assertEqual({"scope": "packaged-transport", "started": True, "grpc_code": "Unimplemented", "attempts": 3,
                          "exit_code": 143, "backend_semantics_verified": False}, summary)
        self.assertEqual(summary, json.loads((self.output / "summary.json").read_text()))
        self.assertEqual(143, self.processes[0].poll())

    def test_early_process_exit_does_not_produce_passed_report(self):
        self.write_server("raise SystemExit(17)\n")
        with self.assertRaisesRegex(ValueError, "listener startup"):
            packaged.verify(self.runtime, self.output)
        self.assert_stopped_without_pass()
        self.probe.assert_not_called()

    def test_driver_preparation_failure_does_not_start_process(self):
        with mock.patch.object(packaged, "run_logged", return_value=1):
            with self.assertRaisesRegex(ValueError, "driver build failed"):
                packaged.verify(self.runtime, self.output)
        self.assertEqual([], self.processes)
        self.assertFalse((self.output / "summary.json").exists())

    def test_protocol_failure_stops_owned_process_and_propagates(self):
        self.probe.side_effect = ValueError("fixture protocol failure")
        with self.assertRaisesRegex(ValueError, "fixture protocol failure"):
            packaged.verify(self.runtime, self.output)
        self.assert_stopped_without_pass()

    def test_bad_graceful_exit_is_not_rewritten_as_success(self):
        self.write_server("signal.signal(signal.SIGTERM, lambda _signum, _frame: sys.exit(9))\nprint('listeners=1 data_readiness=UNIMPLEMENTED', flush=True)\nsignal.pause()\n")
        with self.assertRaisesRegex(ValueError, "graceful shutdown"):
            packaged.verify(self.runtime, self.output)
        self.assert_stopped_without_pass()
        self.assertEqual(9, self.processes[0].returncode)

    def test_sigterm_during_observation_cleans_up_and_restores_handler(self):
        previous = signal.getsignal(signal.SIGTERM)
        self.probe.side_effect = lambda *args, **kwargs: os.kill(os.getpid(), signal.SIGTERM)
        with self.assertRaises(SystemExit) as cancelled:
            packaged.verify(self.runtime, self.output)
        self.assertEqual(143, cancelled.exception.code)
        self.assertEqual(previous, signal.getsignal(signal.SIGTERM))
        self.assert_stopped_without_pass()

    def test_cleanup_escalates_for_term_ignoring_process(self):
        self.write_server("signal.signal(signal.SIGTERM, signal.SIG_IGN)\nprint('listeners=1 data_readiness=UNIMPLEMENTED', flush=True)\nsignal.pause()\n")
        self.force_cleanup_timeout = True
        self.probe.side_effect = ValueError("fixture protocol failure")
        with self.assertRaisesRegex(ValueError, "fixture protocol failure"):
            packaged.verify(self.runtime, self.output)
        self.assert_stopped_without_pass()
        self.assertEqual(-signal.SIGKILL, self.processes[0].returncode)


if __name__ == "__main__":
    unittest.main()
