#!/usr/bin/env python3
"""Verify two frozen clean builds, actual tests, and every packaged module JAR."""
from __future__ import annotations
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import uuid
import xml.etree.ElementTree as ET
from evidence import M0_JAVA_MODULES as MODULES, M0_REQUIRED_SUITES as REQUIRED, java_reports, require_unchanged_source
from build_process import run_logged

ROOT = Path(__file__).resolve().parents[2]


def packaged_jars(root=ROOT):
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    parent = ET.parse(root / "pom.xml").getroot()
    version = parent.findtext("m:version", "", namespace)
    if not re.fullmatch(r"[A-Za-z0-9_.+-]+", version):
        raise ValueError("project version cannot identify packaged JARs")
    expected = {f"{module}/target/{module}-{version}.jar" for module in MODULES}
    expected.add(f"hbase/target/hbase-{version}-tests.jar")
    actual = {str(path.relative_to(root)) for module in MODULES for path in (root / module / "target").glob("*.jar")}
    if actual != expected:
        raise ValueError("packaged JAR inventory is incomplete or contains unexpected outputs")
    return {path: hashlib.sha256((root / path).read_bytes()).hexdigest() for path in sorted(expected)}


def compare_jars(first, second):
    if not first or first != second:
        raise ValueError("clean build JAR inventories or SHA-256 values differ")


def build_once(output, run, command, source):
    directory = output / f"run-{run}"
    directory.mkdir()
    code = run_logged(command, directory / "maven.log", ROOT)
    if code:
        print("\n".join((directory / "maven.log").read_text().splitlines()[-70:]), file=sys.stderr)
        raise ValueError(f"clean Maven build {run} failed")
    require_unchanged_source(ROOT, source)
    jars = packaged_jars()
    paths, suites = java_reports(ROOT, REQUIRED, MODULES)
    for path in paths:
        destination = directory / "reports" / path.relative_to(ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
    summary = {"run": run, "jars": jars, "tests": sum(suite["tests"] for suite in suites.values()),
               "failures": 0, "errors": 0, "skipped": 0, "suites": suites}
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"clean build {run}/2 passed: {summary['tests']} Java tests, {len(jars)} JARs", flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--profile", action="append", default=[])
    parser.add_argument("--phase", choices=("verify", "install"), default="verify")
    args = parser.parse_args()
    if any(not re.fullmatch(r"[A-Za-z0-9_-]+", profile) for profile in args.profile):
        raise ValueError("invalid Maven profile")
    output = args.output or ROOT / ".artifacts/m0/reproducible" / str(uuid.uuid4())
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    subprocess.run([sys.executable, str(ROOT / "scripts/verify/j06-source.py"), str(output)], check=True)
    source = json.loads((output / "source.json").read_text())
    require_unchanged_source(ROOT, source)
    source["source_tree"] = subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=ROOT, text=True).strip()
    # m0-public explicitly includes J03 and J07 ITs. No skip flag is used.
    profiles = args.profile or ["m0-public"]
    command = [str(ROOT / "scripts/dev/mvn21.sh"), "-B", "-ntp", "-P" + ",".join(profiles), "clean", args.phase]
    first, second = [build_once(output, run, command, source) for run in (1, 2)]
    compare_jars(first["jars"], second["jars"])
    source.update(phase="J09-reproducible", command=command[1:], jars=second["jars"],
                  builds=[{"run": item["run"], "tests": item["tests"], "failures": 0, "errors": 0, "skipped": 0} for item in (first, second)],
                  identical_jars=True, ended_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    (output / "summary.json").write_text(json.dumps(source, indent=2) + "\n")
    print(f"reproducibility passed: {len(second['jars'])} JARs have identical SHA-256; {output}")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, ET.ParseError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"reproducibility verification failed: {type(error).__name__}") from None
