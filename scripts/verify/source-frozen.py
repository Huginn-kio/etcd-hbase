#!/usr/bin/env python3
"""Require the captured checkout to remain the actual committed verification source."""
import json
from pathlib import Path
import sys
from evidence import require_unchanged_source

if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    try:
        require_unchanged_source(root, json.loads((Path(sys.argv[1]) / "source.json").read_text()))
    except (ValueError, KeyError, OSError):
        raise SystemExit("verification requires unchanged committed source") from None
