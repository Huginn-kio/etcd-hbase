#!/usr/bin/env python3
"""Negative controls for release inputs, reproducibility, runtime classpaths and probes."""
import hashlib
import importlib.util
import json
import os
import signal
import subprocess
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
import zipfile
from runtime_probe import await_transport, check_observation
from build_process import stop_group

ROOT = Path(__file__).resolve().parents[2]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


release = load("release_readiness", "release-readiness.py")
package = load("package_runtime", "package-runtime.py")
repro = load("reproducible", "maven-reproducible-build.py")


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
        legacy = self.jar(self.root / "repository", "protobuf-java-2.5.0.jar")
        for paths, hashes in (([server, dependency, colliding], expected), ([server, test_jar], expected),
                              ([server, legacy], expected), ([server], {server.name: "wrong"}), ([dependency], expected)):
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


if __name__ == "__main__":
    unittest.main()
