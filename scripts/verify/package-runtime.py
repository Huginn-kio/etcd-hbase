#!/usr/bin/env python3
"""Stage runtime JARs from the verified build, preserving Maven classpath order."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import zipfile
from evidence import require_unchanged_source
from build_process import run_logged

ROOT = Path(__file__).resolve().parents[2]
PROJECT_MODULES = ("protocol", "spi", "core", "hbase", "grpc", "admin", "server")
JAR_NAME = re.compile(r"[A-Za-z0-9_.+-]+\.jar")
LEGAL_NAMES = {"LICENSE", "LICENSE.TXT", "LICENSE.MD", "NOTICE", "NOTICE.TXT", "NOTICE.MD", "COPYING"}


def jar_digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate_runtime_jars(paths, verified_projects):
    ordered, names, seen = [], {}, set()
    for source in paths:
        source = source.resolve(strict=True)
        if source in seen:
            continue
        seen.add(source)
        name = source.name
        if not source.is_file() or not JAR_NAME.fullmatch(name) or name.endswith("-tests.jar") or name == "testkit.jar" or name.startswith("testkit-") or name == "protobuf-java-2.5.0.jar":
            raise ValueError("runtime classpath includes development, test, legacy or invalid entries")
        if name in names:
            raise ValueError("runtime classpath has colliding JAR filenames")
        digest = jar_digest(source)
        names[name] = digest
        if name in verified_projects and digest != verified_projects[name]:
            raise ValueError("installed project JAR differs from the verified output")
        ordered.append((source, digest))
    if not set(verified_projects).issubset(names):
        raise ValueError("runtime classpath is missing verified project modules")
    return ordered


def copy_jars(ordered, output):
    directory = output / "lib"
    directory.mkdir(mode=0o755)
    directory.chmod(0o755)
    inventory = []
    for source, digest in ordered:
        destination = directory / source.name
        shutil.copyfile(source, destination)
        destination.chmod(0o644)
        with zipfile.ZipFile(source) as archive:
            legal = [name for name in archive.namelist() if name.upper().startswith("META-INF/") and PurePosixPath(name).name.upper() in LEGAL_NAMES]
        inventory.append({"file": source.name, "sha256": digest, "preserved_legal_entries": sorted(legal)})
    return inventory


def stage(output, verification):
    require_unchanged_source(ROOT, verification)
    if verification.get("identical_jars") is not True or not verification.get("jars"):
        raise ValueError("runtime staging requires successful reproducible-build evidence")
    # install ran after verify in both builds; the isolated local repository now
    # holds these exact project JARs. Never accept target/classes as a substitute.
    project_outputs = {Path(path).name: digest for path, digest in verification["jars"].items()
                       if any(path.startswith(module + "/target/") for module in PROJECT_MODULES) and not path.endswith("-tests.jar")}
    if len(project_outputs) != len(PROJECT_MODULES):
        raise ValueError("verified runtime project module inventory is incomplete")
    output.mkdir(parents=True, exist_ok=False)
    output.chmod(0o755)
    classpath = output / "maven-classpath.txt"
    command = [str(ROOT / "scripts/dev/mvn21.sh"), "-B", "-ntp", "-f", str(ROOT / "server/pom.xml"),
               "-DincludeScope=runtime", "-Dmdep.outputFile=" + str(classpath),
               "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:build-classpath"]
    if run_logged(command, output / "maven-classpath.log", ROOT, timeout=300):
        print("\n".join((output / "maven-classpath.log").read_text().splitlines()[-50:]), file=sys.stderr)
        raise ValueError("Maven runtime classpath generation failed")
    server = next(ROOT / path for path in verification["jars"] if path.startswith("server/target/") and not path.endswith("-tests.jar"))
    entries = classpath.read_text().strip().split(os.pathsep)
    if any(not entry for entry in entries):
        raise ValueError("runtime classpath contains an empty entry")
    dependencies = [Path(entry) for entry in entries]
    ordered = validate_runtime_jars([server, *dependencies], project_outputs)
    inventory = copy_jars(ordered, output)
    (output / "classpath.txt").write_text(os.pathsep.join("lib/" + item["file"] for item in inventory) + "\n")
    (output / "classpath.txt").chmod(0o644)
    shutil.copyfile(ROOT / "scripts/dev/packaged-server.sh", output / "etcd-hbase")
    (output / "etcd-hbase").chmod(0o755)
    config = json.loads((ROOT / "config/examples/j06/server.json").read_text())
    for name, port in (("server.json", 0), ("container.json", 2379)):
        config["listeners"][0]["port"] = port
        (output / name).write_text(json.dumps(config, indent=2) + "\n")
        (output / name).chmod(0o644)
    subprocess.run([sys.executable, str(ROOT / "scripts/verify/release-readiness.py"), "--output", str(output / "release-status.json")], check=True)
    (output / "release-status.json").chmod(0o644)
    manifest = {"source_checkout": verification["checkout"], "source_tree": verification.get("source_tree"),
                "classpath_order": inventory, "project_modules": sorted(PROJECT_MODULES),
                "formal_release": False, "complete_notice_review": False}
    (output / "runtime-dependencies.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (output / "runtime-dependencies.json").chmod(0o644)
    # Only these staged application files enter the Docker build context.
    for file in (classpath, output / "maven-classpath.log"):
        file.unlink()
    require_unchanged_source(ROOT, verification)
    print(f"staged {len(inventory)} runtime JARs, including {len(project_outputs)} verified project JARs")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verification", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    stage(arguments.output.resolve(), json.loads(arguments.verification.read_text()))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, zipfile.BadZipFile, subprocess.CalledProcessError) as failure:
        raise SystemExit(f"runtime staging failed: {type(failure).__name__}") from None
