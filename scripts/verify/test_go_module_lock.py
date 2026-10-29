#!/usr/bin/env python3
"""Regression tests for the Go module graph lock verifier."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "verify" / "go-module-lock.py"
SPEC = importlib.util.spec_from_file_location("go_module_lock", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
go_module_lock = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = go_module_lock
SPEC.loader.exec_module(go_module_lock)


class GoModuleLockTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="m0-go-lock-")
        self.addCleanup(self.temp_dir.cleanup)
        root = Path(self.temp_dir.name)
        self.lock_path = root / "dependency-lock.json"
        self.go_mod_path = root / "go.mod"
        self.go_sum_path = root / "go.sum"
        self.lock_path.write_text(
            json.dumps(
                {
                    "go": {
                        "goDirectiveVersion": "1.26.6",
                        "version": "1.27.2",
                        "moduleVersions": {"example.org/locked": "v1.2.3"},
                        "moduleChecksums": {
                            "example.org/locked@v1.2.3": {
                                "module": "h1:module-checksum",
                                "goMod": "h1:gomod-checksum",
                            }
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
        self.go_mod_path.write_text(
            "module example.org/test\ngo 1.26.6\ntoolchain go1.27.2\n",
            encoding="utf-8",
        )
        self.go_sum_path.write_text(
            "example.org/locked v1.2.3 h1:module-checksum\n"
            "example.org/locked v1.2.3/go.mod h1:gomod-checksum\n",
            encoding="utf-8",
        )
        for name, value in (
            ("LOCK_PATH", self.lock_path),
            ("GO_MOD_PATH", self.go_mod_path),
            ("GO_SUM_PATH", self.go_sum_path),
        ):
            patcher = patch.object(go_module_lock, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_exact_module_graph_matches_lock(self) -> None:
        with patch.object(
            go_module_lock,
            "module_graph",
            return_value={
                "example.org/test": {"Main": True},
                "example.org/locked": {"Version": "v1.2.3"},
                "example.org/transitive": {"Version": "v9.9.9", "Indirect": True},
            },
        ):
            self.assertEqual(1, go_module_lock.validate())

    def test_unlocked_direct_module_in_effective_graph_is_rejected(self) -> None:
        with patch.object(
            go_module_lock,
            "module_graph",
            return_value={
                "example.org/test": {"Main": True},
                "example.org/locked": {"Version": "v1.2.3"},
                "example.org/unlocked": {"Version": "v9.9.9", "Indirect": False},
            },
        ):
            with self.assertRaisesRegex(
                go_module_lock.LockError,
                "module graph contains unlocked direct modules: example.org/unlocked",
            ):
                go_module_lock.validate()

    def test_lock_toolchain_and_checksums_fail_closed(self) -> None:
        original = self.lock_path.read_text()
        mutations = (
            (lambda lock: lock.update(goDirectiveVersion="1.0"), "language version differs"),
            (lambda lock: lock.update(version="1.0"), "toolchain differs"),
            (lambda lock: lock.update(moduleChecksums={}), "do not describe the same modules"),
            (lambda lock: lock["moduleChecksums"]["example.org/locked@v1.2.3"].update(module="wrong"),
             "module checksum differs"),
            (lambda lock: lock["moduleChecksums"]["example.org/locked@v1.2.3"].update(goMod="wrong"),
             "go.mod checksum differs"),
            (lambda lock: lock["moduleChecksums"]["example.org/locked@v1.2.3"].pop("goMod"),
             "must lock module and go.mod checksums"),
        )
        graph = {"example.org/locked": {"Version": "v1.2.3"}}
        for mutate, expected_error in mutations:
            with self.subTest(expected_error=expected_error):
                document = json.loads(original)
                mutate(document["go"])
                self.lock_path.write_text(json.dumps(document))
                with patch.object(go_module_lock, "module_graph", return_value=graph):
                    with self.assertRaisesRegex(go_module_lock.LockError, expected_error):
                        go_module_lock.validate()

    def test_missing_or_different_effective_module_version_is_rejected(self) -> None:
        for graph in ({}, {"example.org/locked": {"Version": "v9.9.9"}}):
            with self.subTest(graph=graph):
                with patch.object(go_module_lock, "module_graph", return_value=graph):
                    with self.assertRaisesRegex(go_module_lock.LockError, "effective version is"):
                        go_module_lock.validate()


if __name__ == "__main__":
    unittest.main(verbosity=2)
