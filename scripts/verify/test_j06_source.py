#!/usr/bin/env python3
"""Exercise source manifests in Git checkouts with no origin/main reference."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("j06_source", Path(__file__).with_name("j06-source.py"))
source = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source)


class SourceManifestTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.git("init", "-q")
        (self.root / "file").write_text("public fixture")
        self.git("add", "file")
        self.git("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
        self.head = self.git("rev-parse", "HEAD")

    def git(self, *arguments):
        return subprocess.check_output(["git", "-C", str(self.root), *arguments], text=True).strip()

    def test_missing_remote_reference_does_not_block_ci_or_invent_a_base(self):
        metadata = source.capture(self.root)
        self.assertEqual(self.head, metadata["checkout"])
        self.assertIsNone(metadata["base"])
        self.assertFalse(metadata["tracked_dirty"])
        metadata = source.capture(self.root, {"after": self.head})
        self.assertEqual("ci-non-pr", metadata["checkout_kind"])
        self.assertIsNone(metadata["base"])

    def test_pull_request_event_keeps_head_and_target_distinct(self):
        event = {"pull_request": {"base": {"sha": "a" * 40}, "head": {"sha": self.head}}}
        metadata = source.capture(self.root, event)
        self.assertEqual(self.head, metadata["pr_head"])
        self.assertEqual("a" * 40, metadata["base"])
        self.assertEqual("pr-head", metadata["checkout_kind"])
        event["pull_request"]["head"]["sha"] = "b" * 40
        with self.assertRaises(ValueError):
            source.capture(self.root, event)

    def test_malformed_revision_is_rejected(self):
        with self.assertRaises(ValueError):
            source.capture(self.root, {"pull_request": {"base": {"sha": "untrusted-value"}, "head": {"sha": self.head}}})


if __name__ == "__main__":
    unittest.main()
