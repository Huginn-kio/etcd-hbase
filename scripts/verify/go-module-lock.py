#!/usr/bin/env python3
"""Verify the Go module lock and its regression cases."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
LOCK_PATH = ROOT / "build" / "dependency-lock.json"
GO_MOD_PATH = ROOT / "testkit" / "go" / "go.mod"
GO_SUM_PATH = ROOT / "testkit" / "go" / "go.sum"
GO_COMMAND = ROOT / "scripts" / "dev" / "go-m0.sh"


class LockError(Exception):
    pass


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LockError(f"cannot read {path.name}: {error}") from error


def module_graph() -> dict[str, dict[str, Any]]:
    result = subprocess.run(
        [str(GO_COMMAND), "-C", "testkit/go", "list", "-m", "-json", "all"],
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise LockError(f"go list -m -json all failed: {result.stderr.strip()}")

    decoder = json.JSONDecoder()
    payload = result.stdout
    position = 0
    modules: dict[str, dict[str, Any]] = {}
    while position < len(payload):
        while position < len(payload) and payload[position].isspace():
            position += 1
        if position == len(payload):
            break
        module, position = decoder.raw_decode(payload, position)
        if not isinstance(module, dict):
            raise LockError("module graph contains a non-object entry")
        path = module.get("Path")
        if not path or path in modules:
            raise LockError("module graph contains an empty or duplicate module path")
        if module.get("Replace"):
            replacement = module["Replace"]
            raise LockError(
                f"module {path} is replaced by {replacement.get('Path', 'an unknown module')}"
            )
        modules[path] = module
    return modules


def go_sum_entries() -> set[tuple[str, str, str]]:
    try:
        lines = GO_SUM_PATH.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise LockError(f"cannot read {GO_SUM_PATH.name}: {error}") from error
    entries: set[tuple[str, str, str]] = set()
    for line in lines:
        fields = line.split()
        if len(fields) != 3:
            raise LockError("go.sum contains a malformed checksum row")
        module, version, checksum = fields
        entries.add((module, version, checksum))
    return entries


def read_go_lock() -> dict[str, Any]:
    lock_document = read_json(LOCK_PATH)
    if not isinstance(lock_document, dict) or not isinstance(lock_document.get("go"), dict):
        raise LockError("dependency lock has no Go toolchain section")
    return lock_document["go"]


def locked_module_inventory(
    lock: dict[str, Any],
) -> tuple[dict[str, str], dict[str, Any]]:
    expected_versions = lock.get("moduleVersions", {})
    expected_checksums = lock.get("moduleChecksums", {})
    if not isinstance(expected_versions, dict) or not isinstance(expected_checksums, dict):
        raise LockError("moduleVersions and moduleChecksums must be objects")
    expected_checksum_keys = {
        f"{module}@{version}" for module, version in expected_versions.items()
    }
    if not expected_versions or set(expected_checksums) != expected_checksum_keys:
        raise LockError("moduleVersions and moduleChecksums do not describe the same modules")
    return expected_versions, expected_checksums


def validate_toolchain(lock: dict[str, Any]) -> None:
    go_mod = GO_MOD_PATH.read_text(encoding="utf-8")
    directives = {
        fields[0]: fields[1]
        for line in go_mod.splitlines()
        if len(fields := line.split()) == 2 and fields[0] in {"go", "toolchain"}
    }
    if directives.get("go") != lock.get("goDirectiveVersion"):
        raise LockError("go.mod language version differs from the dependency lock")
    if directives.get("toolchain") not in (None, f"go{lock.get('version')}"):
        raise LockError("go.mod toolchain differs from the dependency lock")
    if directives.get("toolchain") is None and directives.get("go") != lock.get("version"):
        raise LockError("go.mod toolchain differs from the dependency lock")


def validate_direct_modules(
    actual_modules: dict[str, dict[str, Any]], expected_versions: dict[str, str]
) -> None:
    unlocked_direct_modules = sorted(
        module
        for module, metadata in actual_modules.items()
        if not metadata.get("Main")
        and not metadata.get("Indirect", False)
        and module not in expected_versions
    )
    if unlocked_direct_modules:
        raise LockError(
            "module graph contains unlocked direct modules: "
            + ", ".join(unlocked_direct_modules)
        )


def validate_module_version(
    module: str, expected_version: str, actual: dict[str, Any] | None
) -> None:
    if actual is None or actual.get("Version") != expected_version:
        actual_version = actual.get("Version", "missing") if actual else "missing"
        raise LockError(
            f"{module} effective version is {actual_version}; expected {expected_version}"
        )


def validate_module_checksums(
    module: str, version: str, checksums: Any, sums: set[tuple[str, str, str]]
) -> None:
    key = f"{module}@{version}"
    if not isinstance(checksums, dict) or set(checksums) != {"module", "goMod"}:
        raise LockError(f"{key} must lock module and go.mod checksums")
    if (module, version, checksums["module"]) not in sums:
        raise LockError(f"{key} module checksum differs from go.sum")
    if (module, version + "/go.mod", checksums["goMod"]) not in sums:
        raise LockError(f"{key} go.mod checksum differs from go.sum")


def validate() -> int:
    lock = read_go_lock()
    expected_versions, expected_checksums = locked_module_inventory(lock)
    validate_toolchain(lock)
    actual_modules = module_graph()
    validate_direct_modules(actual_modules, expected_versions)
    sums = go_sum_entries()
    for module, version in expected_versions.items():
        validate_module_version(module, version, actual_modules.get(module))
        validate_module_checksums(module, version, expected_checksums.get(f"{module}@{version}"), sums)
    return len(expected_versions)


def run_regression_tests() -> None:
    test_path = ROOT / "scripts" / "verify" / "test_go_module_lock.py"
    result = subprocess.run(
        [sys.executable, "-B", str(test_path)],
        cwd=ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise LockError(
            f"Go module lock regression tests failed with exit code {result.returncode}"
        )


def main() -> int:
    try:
        run_regression_tests()
        count = validate()
    except (LockError, OSError, TypeError, ValueError) as error:
        print(f"go-module-lock: {error}", file=sys.stderr)
        return 1
    print(f"go-module-lock: {count} locked modules match MVS and go.sum; no replacements found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
