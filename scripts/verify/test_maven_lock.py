#!/usr/bin/env python3
"""Regression tests for the Maven lock verifier used by the build entrypoint."""

from __future__ import annotations

import copy
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOCK_PATH = ROOT / "build" / "dependency-lock.json"
MODULE_PATH = ROOT / "scripts" / "verify" / "maven-lock.py"
SPEC = importlib.util.spec_from_file_location("maven_lock", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
maven_lock = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = maven_lock
SPEC.loader.exec_module(maven_lock)


class MavenLockTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = maven_lock.load_lock(LOCK_PATH)

    def test_project_pins_match_dependency_lock(self) -> None:
        self.assertEqual(9, maven_lock.validate_metadata(self.lock, ROOT / "pom.xml"))

    def test_duplicate_json_keys_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="m0-lock-duplicate-") as temp_dir:
            path = Path(temp_dir) / "duplicate.json"
            path.write_text('{"plugins": {}, "plugins": {}}', encoding="utf-8")
            with self.assertRaisesRegex(maven_lock.LockError, "duplicate JSON key"):
                maven_lock.load_lock(path)

    def test_changed_plugin_version_is_rejected(self) -> None:
        lock = copy.deepcopy(self.lock)
        lock["plugins"]["maven-enforcer-plugin"]["version"] = "0.0.0-review-mismatch"
        with self.assertRaisesRegex(maven_lock.LockError, "Maven lock metadata mismatch"):
            maven_lock.validate_metadata(lock, ROOT / "pom.xml")

    def test_wrong_native_classifier_is_rejected(self) -> None:
        lock = copy.deepcopy(self.lock)
        lock["nativeTools"]["protocGenGrpcJava"]["platformArtifacts"]["macos-aarch_64"]["coordinate"] = (
            "io.grpc:protoc-gen-grpc-java:exe:linux-x86_64"
        )
        with self.assertRaisesRegex(maven_lock.LockError, "classifier does not match"):
            maven_lock.validate_metadata(lock, ROOT / "pom.xml")

    def test_cached_artifact_digest_mismatch_is_rejected(self) -> None:
        artifact = maven_lock.Artifact("org.example:sample", "1.0", "0" * 64)
        with tempfile.TemporaryDirectory(prefix="m0-lock-artifact-") as temp_dir:
            path = Path(temp_dir) / artifact.relative_path
            path.parent.mkdir(parents=True)
            path.write_bytes(b"changed artifact")
            with self.assertRaisesRegex(maven_lock.LockError, "SHA-256 mismatch"):
                maven_lock.verify_artifacts([artifact], Path(temp_dir), require_all=True)

    def test_missing_artifact_is_deferred_for_partial_builds(self) -> None:
        artifact = maven_lock.Artifact("org.example:sample", "1.0", "0" * 64)
        with tempfile.TemporaryDirectory(prefix="m0-lock-missing-") as temp_dir:
            self.assertEqual((0, 1), maven_lock.verify_artifacts([artifact], Path(temp_dir), require_all=False))
            with self.assertRaisesRegex(maven_lock.LockError, "not cached"):
                maven_lock.verify_artifacts([artifact], Path(temp_dir), require_all=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
