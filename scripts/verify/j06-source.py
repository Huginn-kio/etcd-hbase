#!/usr/bin/env python3
"""Capture the actual checkout without assuming a full local Git clone in CI."""
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def git(root, *arguments, optional=False):
    result = subprocess.run(["git", "-C", str(root), *arguments], text=True, capture_output=True)
    if result.returncode:
        if optional:
            return None
        raise ValueError("source metadata cannot be inspected")
    return result.stdout.strip()


def revision(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("source revision is invalid")
    return value


def capture(root, event=None):
    checkout = revision(git(root, "rev-parse", "HEAD"))
    parents = [revision(line.split()[1]) for line in git(root, "cat-file", "-p", checkout).splitlines() if line.startswith("parent ")]
    pull_request = event.get("pull_request") if event is not None else None
    if pull_request is not None:
        base = revision(pull_request["base"]["sha"])
        head = revision(pull_request["head"]["sha"])
        if checkout != head and parents != [base, head]:
            raise ValueError("checkout does not match the pull request integration")
        checkout_kind = "pr-head" if checkout == head else "pr-merge"
    else:
        base = git(root, "rev-parse", "--verify", "-q", "refs/remotes/origin/main", optional=True) if event is None else None
        head = checkout
        checkout_kind = "local" if event is None else "ci-non-pr"
    return {"head": checkout, "checkout": checkout, "pr_head": head, "base": base,
            "checkout_kind": checkout_kind, "parents": parents,
            "tracked_dirty": bool(git(root, "status", "--porcelain", "--untracked-files=no")),
            "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}


def main():
    root = Path(__file__).resolve().parents[2]
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    event = json.loads(Path(event_path).read_text()) if event_path else None
    metadata = capture(root, event)
    (Path(sys.argv[1]) / "source.json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
