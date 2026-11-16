#!/usr/bin/env python3
"""Exercise the pinned Resolver against transient and permanent HTTP failures."""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("public_build_policy", ROOT / "scripts/verify/public-build-policy.py")
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)
PREP_SPEC = importlib.util.spec_from_file_location(
    "maven_preparation", ROOT / "scripts/dev/prepare-maven-dependencies.py")
PREPARATION = importlib.util.module_from_spec(PREP_SPEC)
PREP_SPEC.loader.exec_module(PREPARATION)

PARENT = b"""<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion><groupId>resilience.fixture</groupId>
  <artifactId>parent</artifactId><version>1</version><packaging>pom</packaging>
</project>"""


class MavenCachePolicyTest(unittest.TestCase):
    def test_actual_workflows_keep_public_pins_and_cache_barriers(self):
        self.assertTrue(POLICY.static_policy()["public_maven_only"])

    def test_consumer_rejects_incomplete_keys_and_failed_preparation_bypass(self):
        workflow = ROOT / ".github/workflows/m3.yml"
        content = workflow.read_text()
        damages = (
            content.replace("            build/dependency-lock.json\n", "", 1),
            content.replace("    needs: maven-dependencies\n", "", 1),
            content.replace("    if: ${{ !cancelled() }}\n", "", 1),
            content.replace('        run: test "$PREPARATION_RESULT" = success\n', "", 1),
            content.replace("          cache-read-only: true\n", "", 1),
            content.replace("uses: ./" + POLICY.MAVEN_PREPARATION, "uses: ./unexpected.yml", 1),
        )
        for index, damaged in enumerate(damages):
            with self.subTest(damage=index), self.assertRaises(ValueError):
                POLICY.maven_cache_policy(workflow, damaged)

    def test_only_approved_local_workflow_or_immutable_remote_actions_are_allowed(self):
        pins = json.loads((ROOT / "build/dependency-lock.json").read_text())["actions"]
        POLICY.workflow_actions("    uses: ./" + POLICY.MAVEN_PREPARATION, ROOT, pins)
        for action in ("actions/setup-java@main", "./../unexpected.yml", "./.github/workflows/other.yml"):
            with self.subTest(action=action), self.assertRaises(ValueError):
                POLICY.workflow_actions("    uses: " + action, ROOT, pins)


class MavenOfflineCacheRepairTest(unittest.TestCase):
    def test_missing_coordinates_preserve_classifier_and_deduplicate(self):
        diagnostic = ("net.bytebuddy:byte-buddy:jar:1.18.3 (absent), "
                      "com.google.protobuf:protoc:exe:linux-x86_64:4.35.0 (absent), "
                      "net.bytebuddy:byte-buddy:jar:1.18.3 (absent)")
        self.assertEqual([
            "com.google.protobuf:protoc:4.35.0:exe:linux-x86_64",
            "net.bytebuddy:byte-buddy:1.18.3:jar",
        ], PREPARATION.missing_transfer_coordinates(diagnostic))
        with self.assertRaisesRegex(ValueError, "reactor SNAPSHOTs"):
            PREPARATION.missing_transfer_coordinates("io.etcdhbase:core:jar:0.1.0-SNAPSHOT (absent)")

    def test_non_transfer_failure_is_not_retried_or_ignored(self):
        result = subprocess.CompletedProcess([], 1, "unrelated offline resolution failure")
        with patch.object(PREPARATION.subprocess, "run", return_value=result) as probe:
            with patch.object(PREPARATION, "run_maven") as fetch:
                with self.assertRaises(subprocess.CalledProcessError):
                    PREPARATION.ensure_offline_cache()
                self.assertEqual(1, probe.call_count)
                fetch.assert_not_called()

    def test_persistently_missing_transfer_repair_has_a_fixed_bound(self):
        result = subprocess.CompletedProcess([], 1, "net.bytebuddy:byte-buddy:jar:1.18.3 (absent)")
        with patch.object(PREPARATION.subprocess, "run", return_value=result) as probe:
            with patch.object(PREPARATION, "run_maven") as fetch:
                with self.assertRaises(subprocess.CalledProcessError):
                    PREPARATION.ensure_offline_cache()
                self.assertEqual(4, probe.call_count)
                self.assertEqual(3, fetch.call_count)
                fetch.assert_called_with("-N", PREPARATION.GOAL + "get",
                                         "-Dartifact=net.bytebuddy:byte-buddy:1.18.3:jar")


class MavenHttpResilienceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = dict(os.environ, MAVEN_ARGS="", MAVEN_OPTS="")
        java_home = cls.environment.get("ETCD_HBASE_JDK21_HOME") or cls.environment.get("JAVA_HOME")
        if not java_home:
            raise RuntimeError("Set ETCD_HBASE_JDK21_HOME or JAVA_HOME to the locked JDK")
        cls.environment["JAVA_HOME"] = java_home
        cls.environment.pop("MAVEN_PROJECTBASEDIR", None)
        cls.environment.pop("MAVEN_BASEDIR", None)
        result = subprocess.run([str(ROOT / "mvnw"), "--version"], cwd=ROOT,
                                env=cls.environment, capture_output=True, text=True, timeout=180, check=True)
        match = re.search(r"^Maven home: (.+)$", result.stdout, re.MULTILINE)
        if not match:
            raise RuntimeError("Pinned Maven distribution did not report its home")
        cls.maven = str(Path(match[1].strip()) / "bin/mvn")

    def fixture(self, status, failures):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / ".mvn").mkdir()
        shutil.copyfile(ROOT / ".mvn/maven.config", root / ".mvn/maven.config")
        (root / "pom.xml").write_text("""<project xmlns="http://maven.apache.org/POM/4.0.0">
          <modelVersion>4.0.0</modelVersion><parent><groupId>resilience.fixture</groupId>
          <artifactId>parent</artifactId><version>1</version><relativePath/></parent>
          <artifactId>child</artifactId><packaging>pom</packaging></project>""")
        observations = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                payload, code = self.response()
                self.send_response(code)
                if code == 429:
                    self.send_header("Retry-After", "1")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def response(self):
                if self.path.endswith("/parent-1.pom"):
                    observations.append(time.monotonic())
                    return (b"fixture failure", status) if len(observations) <= failures else (PARENT, 200)
                if self.path.endswith("/parent-1.pom.sha1"):
                    return hashlib.sha1(PARENT).hexdigest().encode(), 200
                return b"not found", 404

            def log_message(self, *_arguments):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def close_server():
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.addCleanup(close_server)
        (root / "settings.xml").write_text(f"""<settings><mirrors><mirror><id>fixture</id>
          <mirrorOf>*</mirrorOf><url>http://127.0.0.1:{server.server_port}</url>
          </mirror></mirrors></settings>""")
        return root, observations

    def validate(self, status, failures):
        root, observations = self.fixture(status, failures)
        started = time.monotonic()
        result = subprocess.run([self.maven, "-B", "-ntp", "-s", "settings.xml", "-gs", "settings.xml",
                                 "-Dmaven.repo.local=" + str(root / "repository"), "validate"],
                                cwd=root, env=self.environment, capture_output=True, text=True, timeout=25)
        return result, observations, time.monotonic() - started

    def test_four_rate_limited_requests_recover_and_obey_retry_after(self):
        result, observations, elapsed = self.validate(429, 4)
        self.assertEqual(0, result.returncode, result.stdout[-4000:] + result.stderr[-1000:])
        self.assertEqual(5, len(observations))
        self.assertGreaterEqual(observations[-1] - observations[0], 3.5)
        self.assertLess(elapsed, 20, "Retry-After must override the longer configured fallback interval")

    def test_persistent_rate_limit_stops_at_the_bound(self):
        result, observations, _elapsed = self.validate(429, 100)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(5, len(observations))

    def test_missing_artifact_is_not_retried(self):
        result, observations, _elapsed = self.validate(404, 100)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(1, len(observations))


if __name__ == "__main__":
    unittest.main(verbosity=2)
