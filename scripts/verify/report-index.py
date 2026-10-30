#!/usr/bin/env python3
"""Verify the M0 file index and its actual test evidence; partial evidence exits 2."""
import argparse
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET
from m0_evidence import verify_index

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("M0",), required=True)
    location = parser.add_mutually_exclusive_group(required=True)
    location.add_argument("--progress", type=Path)
    location.add_argument("--artifact-root", type=Path)
    args = parser.parse_args()
    if args.progress:
        match = re.search(r"^verification_index:\s*(\S+)\s*$", args.progress.read_text(), re.MULTILINE)
        if not match:
            raise ValueError("progress does not identify a verification index")
        index = (ROOT / match.group(1)).resolve()
        if not index.is_relative_to(ROOT / ".artifacts/m0") or index.name != "INDEX.json":
            raise ValueError("progress index must identify local M0 evidence")
        directory = index.parent
    else:
        directory = args.artifact_root.resolve()
    summary = verify_index(directory)
    print(json.dumps({key: summary[key] for key in ("phase", "status", "source", "java_builds", "go", "race", "runtime", "image", "m0_accepted", "waiting_inputs")}))
    return 0 if summary["status"] == "passed" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, TypeError, ET.ParseError):
        raise SystemExit("M0 evidence index rejected") from None
