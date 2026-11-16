#!/usr/bin/env python3
"""Warm external reactor dependencies, plugins and checksum-locked dynamic tools."""
import importlib.util
import os
import re
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("maven_lock", ROOT / "scripts/verify/maven-lock.py")
LOCK = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = LOCK
SPEC.loader.exec_module(LOCK)
GOAL = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0:"


def missing_artifacts(artifacts, repository):
    # The same native artifact can be referenced by both the library and tool lock sections.
    unique = {artifact.relative_path: artifact for artifact in artifacts}
    return [artifact for path, artifact in unique.items() if not (repository / path).is_file()]


def request_coordinate(artifact):
    coordinate = f"{artifact.coordinate}:{artifact.version}:{artifact.extension}"
    return coordinate + ":" + artifact.classifier if artifact.classifier else coordinate


def maven_command(*arguments):
    return [str(ROOT / "scripts/dev/mvn21.sh"), "-B", "-ntp",
            "-Dmaven.artifact.threads=1", *arguments]


def run_maven(*arguments):
    subprocess.run(maven_command(*arguments), cwd=ROOT, check=True)


def missing_transfer_coordinates(output):
    # Resolver's offline diagnostic names concrete absent artifacts, including optional classifier.
    # This is not a dependency-lock parser. Only dependency:get receives the validated coordinates.
    pattern = r"(?<![\w.:-])([\w.-]+):([\w.-]+):([\w.-]+):(?:([\w.-]+):)?([\w.-]+) \(absent\)"
    coordinates = set()
    for group, artifact, extension, classifier, version in re.findall(pattern, output, re.ASCII):
        if group == "io.etcdhbase":
            raise ValueError("cache preparation must not fetch reactor SNAPSHOTs")
        coordinate = f"{group}:{artifact}:{version}:{extension}"
        coordinates.add(coordinate + ":" + classifier if classifier else coordinate)
    if len(coordinates) > 64:
        raise ValueError("offline cache repair exceeded the bounded missing artifact set")
    return sorted(coordinates)


def ensure_offline_cache():
    command = maven_command("-o", GOAL + "go-offline", "-DexcludeReactor=true")
    # go-offline may leave normal transitive artifacts absent. Repair only those named by the
    # offline Resolver, at most three rounds; no test or build lifecycle is retried here.
    for attempt in range(4):
        result = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, check=False)
        print(result.stdout, end="", flush=True)
        if result.returncode == 0:
            return
        missing = missing_transfer_coordinates(result.stdout)
        if attempt == 3 or not missing:
            raise subprocess.CalledProcessError(result.returncode, command)
        for coordinate in missing:
            run_maven("-N", GOAL + "get", "-Dartifact=" + coordinate)


def main():
    lock = LOCK.load_lock(ROOT / "build/dependency-lock.json")
    LOCK.validate_metadata(lock, ROOT / "pom.xml")
    artifacts = LOCK.collect_artifacts(lock)
    repository = Path(os.environ.get("ETCD_HBASE_MAVEN_REPOSITORY") or Path.home() / ".m2/repository")
    repository = repository.expanduser().resolve()
    os.environ["ETCD_HBASE_MAVEN_REPOSITORY"] = str(repository)
    LOCK.verify_artifacts(artifacts, repository, require_all=False)
    # This resolution goal does not install reactor SNAPSHOTs or execute test/build lifecycles.
    run_maven(GOAL + "go-offline", "-DexcludeReactor=true")
    for artifact in missing_artifacts(artifacts, repository):
        # Dynamic protoc/copy goals are not part of go-offline's dependency graph.
        run_maven("-N", GOAL + "get", "-Dartifact=" + request_coordinate(artifact))
    # Surefire/Failsafe select their JUnit provider at execution time, outside go-offline.
    provider_versions = {lock["plugins"][name]["version"] for name in
                         ("maven-surefire-plugin", "maven-failsafe-plugin")}
    for version in sorted(provider_versions):
        run_maven("-N", GOAL + "get",
                  "-Dartifact=org.apache.maven.surefire:surefire-junit-platform:" + version)
    # JUnit 6 aligns its Platform launcher with the project's selected Jupiter version.
    junit = lock["libraries"]["tests"]["junitJupiter"]["coordinate"]
    junit_version = LOCK.coordinate_parts(junit)[2]
    for coordinate in ("org.junit.platform:junit-platform-launcher", "org.junit.vintage:junit-vintage-engine"):
        # HBase test dependencies also put JUnit 4 on the classpath, enabling Vintage discovery.
        run_maven("-N", GOAL + "get", "-Dartifact=" + coordinate + ":" + junit_version)
    # Exercise the normal HBase test classpath as well as the dependency plugin's graph.
    # test-compile downloads/generates inputs, but does not execute or skip any quality test.
    run_maven("-pl", "hbase", "-am", "test-compile")
    # Successful online resolution and the explicit checksum lock are insufficient proof that
    # the whole transitive cache can be consumed. Fail before saving an incomplete cache.
    ensure_offline_cache()
    checked, missing = LOCK.verify_artifacts(artifacts, repository, require_all=True)
    print(f"Maven dependency cache prepared: {checked} locked artifacts verified, {missing} missing")


if __name__ == "__main__":
    try:
        main()
    except (LOCK.LockError, OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Maven dependency preparation failed: {error}", file=sys.stderr)
        sys.exit(1)
