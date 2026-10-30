"""Validate public M0 evidence and build a tamper-evident local file index."""
import hashlib
import json
from pathlib import Path, PurePosixPath
from evidence import M0_JAVA_MODULES, M0_REQUIRED_SUITES, go_report, java_reports
from runtime_probe import check_observation

STEPS = ("guards", "public-policy", "module-lock", "module-verify", "go-test", "go-race", "go-vet",
         "native", "api-matrix", "proto-source", "reproducible", "spi-boundary", "package", "packaged-runtime",
         "runtime-image", "release-readiness", "diff-check")
WAITING_INPUTS = ("B01", "B02", "B03", "B06", "I07", "I08")
MUTATIONS = ("watch-event", "more", "deleted", "revision", "txn-branch", "prev-kv", "stream-terminal", "error-kind", "count-existence", "newline")


def read_json(path):
    return json.loads(path.read_text())


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def validate_steps(steps):
    if [step["id"] for step in steps] != list(STEPS):
        raise ValueError("public verification steps are incomplete or out of order")
    for step in steps:
        expected = 2 if step["id"] == "runtime-image" and step["status"] == "unavailable" else 0
        if step["exit_code"] != expected or step["status"] not in ({"passed", "unavailable"} if step["id"] == "runtime-image" else {"passed"}):
            raise ValueError("a required public check did not pass")


def validate_reproducible(output, source):
    directory = output / "reproducible"
    summary = read_json(directory / "summary.json")
    if summary["checkout"] != source["checkout"] or summary["tracked_dirty"] or summary["identical_jars"] is not True or summary["command"] != ["-B", "-ntp", "-Pm0-public", "clean", "install"]:
        raise ValueError("build evidence is not a frozen full public build")
    builds = []
    for run in (1, 2):
        record = read_json(directory / f"run-{run}/summary.json")
        _, suites = java_reports(directory / f"run-{run}/reports", M0_REQUIRED_SUITES, M0_JAVA_MODULES)
        count = sum(suite["tests"] for suite in suites.values())
        if record["run"] != run or record["suites"] != suites or record["tests"] != count or any(record[key] for key in ("failures", "errors", "skipped")):
            raise ValueError("reproducible build counters contradict actual XML reports")
        if record["jars"] != summary["jars"] or len(record["jars"]) != 9 or {Path(path).parts[0] for path in record["jars"]} != set(M0_JAVA_MODULES):
            raise ValueError("reproducible JAR inventory differs between builds")
        builds.append({"run": run, "tests": count, "failures": 0, "errors": 0, "skipped": 0})
    if summary["builds"] != builds:
        raise ValueError("aggregate build counters contradict the two builds")
    return summary


def validate_native(directory):
    for run in ("run-one", "run-two"):
        metadata = read_json(directory / run / "metadata.json")
        if metadata["protocol"] != "etcd-v3.5.23" or metadata["source_revision"] != "1cf4bc40111fd95d1e14a90a09a146357b15eb13" or len(metadata["modules"]) != 3:
            raise ValueError("native etcd module evidence is inconsistent")
    for mutation in MUTATIONS:
        if not (directory / f"mutated-{mutation}.jsonl").is_file() or not (directory / f"mutated-{mutation}.log").read_text().startswith(("trace record ", "record count differs:", "read right trace:", "normalize right trace:")):
            raise ValueError("native negative-control evidence is missing or unrelated")
    if not (directory / "normalized-one.jsonl").is_file() or not (directory / "normalized-one.jsonl").read_bytes() or (directory / "normalized-one.jsonl").read_bytes() != (directory / "normalized-two.jsonl").read_bytes():
        raise ValueError("native normalized evidence is empty or differs")
    return {"runs": 2, "rejected_mutations": len(MUTATIONS)}


def validate_runtime(output, build):
    directory = output / "runtime"
    manifest = read_json(directory / "runtime-dependencies.json")
    if manifest["source_checkout"] != build["checkout"] or manifest["formal_release"] is not False or manifest["complete_notice_review"] is not False:
        raise ValueError("runtime manifest overstates its source or release status")
    entries = manifest["classpath_order"]
    names = [entry["file"] for entry in entries]
    if len(set(names)) != len(names) or not names or any(Path(name).name != name or not name.endswith(".jar") or name.endswith("-tests.jar") or name == "testkit.jar" or name.startswith("testkit-") or name == "protobuf-java-2.5.0.jar" for name in names):
        raise ValueError("runtime classpath contains invalid or duplicate entries")
    if (directory / "classpath.txt").read_text() != ":".join("lib/" + name for name in names) + "\n":
        raise ValueError("runtime classpath order differs from Maven staging")
    projects = {Path(path).name: value for path, value in build["jars"].items() if not path.endswith("-tests.jar") and not path.startswith("testkit/")}
    if len(projects) != 7 or set(manifest["project_modules"]) != set(M0_JAVA_MODULES) - {"testkit"}:
        raise ValueError("runtime project module set is incomplete")
    seen = {}
    for entry in entries:
        actual = digest(directory / "lib" / entry["file"])
        if entry["sha256"] != actual or entry["file"] in projects and projects[entry["file"]] != actual:
            raise ValueError("runtime JAR differs from its verified source")
        seen[entry["file"]] = actual
    if not projects.items() <= seen.items():
        raise ValueError("runtime is missing verified project JARs")
    packaged = read_json(output / "packaged-runtime/summary.json")
    if packaged["started"] is not True or packaged["grpc_code"] != "Unimplemented" or packaged["exit_code"] not in (0, 143) or packaged["backend_semantics_verified"] is not False:
        raise ValueError("packaged runtime did not prove bounded transport startup and shutdown")
    check_observation((output / "packaged-runtime/transport.jsonl").read_bytes(), "m0-packaged-runtime")
    return {"project_jars": len(projects), "runtime_jars": len(entries), "grpc_code": "Unimplemented", "shutdown_exit_code": packaged["exit_code"]}


def collect(output):
    source = read_json(output / "source.json")
    if source["tracked_dirty"]:
        raise ValueError("verification started from dirty tracked source")
    steps = read_json(output / "steps.json")
    validate_steps(steps)
    guards = read_json(output / "guards/summary.json")
    if guards["failed"] or not guards["passed_suites"]:
        raise ValueError("lightweight guards did not run")
    policy = read_json(output / "public-policy.json")
    if not policy["public_maven_only"] or not policy["actual_go_environment"] or policy["workflows"] < 1:
        raise ValueError("public build policy evidence is incomplete")
    build = validate_reproducible(output, source)
    tests, race = go_report(output / "go-test.jsonl"), go_report(output / "go-race.jsonl")
    native = validate_native(output / "native")
    runtime = validate_runtime(output, build)
    image = read_json(output / "runtime-image/summary.json")
    image_step = next(step for step in steps if step["id"] == "runtime-image")
    if image["status"] == "passed":
        if image_step["status"] != "passed" or image["image_test_passed"] is not True or image["owned_resources_removed"] is not True or image["user"] != "65532:65532" or image["exit_code"] not in (0, 143) or image["backend_semantics_verified"] is not False:
            raise ValueError("container evidence is missing runtime or cleanup proof")
        check_observation((output / "runtime-image/transport.jsonl").read_bytes(), "m0-runtime-image")
    elif image["status"] != "unavailable" or image_step["status"] != "unavailable" or image["image_test_passed"] is not False:
        raise ValueError("image availability was incorrectly converted into a pass")
    release = read_json(output / "release-readiness.json")
    if release["publication_performed"] or release["publication_authorized_by_this_check"] or release["decision_inputs_complete"] != (not release["waiting_inputs"]):
        raise ValueError("release decisions must remain explicit until reviewed")
    return {"phase": "M0", "scope": "public", "status": "passed" if image["status"] == "passed" else "partial",
            "source": source, "java_builds": build["builds"], "identical_jars": True,
            "go": tests, "race": race, "native": native, "runtime": runtime, "image": image,
            "m0_accepted": False, "waiting_inputs": list(WAITING_INPUTS[:4]) + release["waiting_inputs"],
            "unverified": ["target HBase identity/permissions/TLS and target salt", "complete KV/Txn/Watch/Lease/Auth business semantics", "I07/I08 release decisions", "maintainer review of unmerged PRs"],
            "review_status": "recorded separately; build success does not imply review approval"}


def index_files(directory):
    files = {}
    for path in sorted(directory.rglob("*")):
        if path.is_symlink():
            raise ValueError("evidence index must not follow symlinks")
        if path.is_file() and path != directory / "INDEX.json":
            files[path.relative_to(directory).as_posix()] = digest(path)
    return files


def write_index(directory):
    index = {"phase": "M0", "summary": "summary.json", "files": index_files(directory)}
    (directory / "INDEX.json").write_text(json.dumps(index, indent=2) + "\n")
    return index


def verify_index(directory):
    index = read_json(directory / "INDEX.json")
    if index["phase"] != "M0" or index["summary"] != "summary.json" or not index["files"] or index["files"] != index_files(directory):
        raise ValueError("evidence files are missing, changed or unindexed")
    for name in index["files"]:
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("evidence index contains a path outside its directory")
    summary = read_json(directory / "summary.json")
    if summary["status"] not in {"passed", "partial"} or summary != collect(directory):
        raise ValueError("summary contradicts actual test and runtime evidence")
    return summary
