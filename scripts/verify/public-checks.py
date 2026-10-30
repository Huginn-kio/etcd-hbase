#!/usr/bin/env python3
"""Run the actual lightweight verification gates used by public M0 builds."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from build_process import run_logged

ROOT = Path(__file__).resolve().parents[2]
TESTS = ("test_maven_lock.py", "test_j06_source.py", "test_j06_report.py", "test_evidence.py",
         "test_go_module_lock.py", "test_api_matrix.py", "test_m0_spi.py", "test_delivery.py", "test_m0_delivery.py")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    arguments.output.mkdir(parents=True, exist_ok=False)
    for name in TESTS:
        log = arguments.output / (name + ".log")
        if run_logged([sys.executable, str(ROOT / "scripts/verify" / name)], log, ROOT, timeout=60):
            print(log.read_text(), file=sys.stderr)
            raise ValueError("a public verification guard failed")
    (arguments.output / "summary.json").write_text(json.dumps({"passed_suites": list(TESTS), "failed": 0}, indent=2) + "\n")
    print(f"public lightweight checks passed: {len(TESTS)} suites")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError):
        raise SystemExit("public verification guard failed") from None
