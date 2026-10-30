#!/usr/bin/env python3
"""Run the pinned Java 21 image as non-root, probe loopback RPC, then stop owned resources."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import uuid
from build_process import run_logged, termination_is_exception
from runtime_probe import await_transport, probe_arguments

ROOT = Path(__file__).resolve().parents[2]


class Docker:
    def __init__(self, output):
        self.output = output
        # Do not accidentally run verification against a user-selected remote context.
        self.prefix = ["docker", "--context", "default"]

    def call(self, args, log=None, timeout=30):
        result = subprocess.run([*self.prefix, *args], capture_output=True, timeout=timeout)
        if log:
            (self.output / log).write_bytes(result.stdout + result.stderr)
        if result.returncode:
            raise ValueError("owned Docker verification command failed: " + args[0])
        return result.stdout.decode().strip()

    def cleanup(self, token):
        # Labels are assigned before create/build. Even a timed-out command with
        # an unknown outcome can only leave resources bearing this run's UUID.
        deadline = time.monotonic() + 30
        failures = []
        for kind in ("container", "image"):
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ValueError("owned Docker cleanup exceeded its total budget")
                identifiers = self.call([kind, "ls", "--quiet", "--all", "--filter", "label=etcd-hbase.m0.verify=" + token], timeout=remaining).splitlines()
                if any(not re.fullmatch(r"(?:sha256:)?[0-9a-f]{12,64}", value) for value in identifiers):
                    raise ValueError("Docker returned an invalid owned resource ID")
                if identifiers:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ValueError("owned Docker cleanup exceeded its total budget")
                    self.call([kind, "rm", "--force", *identifiers], timeout=remaining)
            except (ValueError, OSError, subprocess.SubprocessError):
                failures.append(kind)
        if failures:
            raise ValueError("owned Docker resources could not be fully released within 30 seconds")


def java_properties(output, lock):
    def property_value(name):
        found = re.search(r"^\s*" + re.escape(name) + r" = ([^\r\n]+)$", output, re.MULTILINE)
        return found.group(1).strip() if found else None
    runtime = property_value("java.runtime.version")
    vendor = property_value("java.vendor")
    if property_value("java.specification.version") != "21" or runtime not in {lock["javaRelease"], lock["javaRelease"] + "-LTS"} or vendor != lock["javaVendor"]:
        raise ValueError("container JVM does not match the locked vendor and patch")
    return {"specification": "21", "runtime": runtime, "vendor": vendor}


def _docker_unavailable_reason(docker):
    if shutil.which("docker") is None:
        return "docker-cli-unavailable"
    try:
        docker.call(["info"], log="docker-info.log", timeout=15)
    except (ValueError, OSError, subprocess.SubprocessError):
        return "default-local-docker-daemon-unavailable"
    return None


def _prepare_image_probe(output):
    lock = json.loads((ROOT / "build/dependency-lock.json").read_text())["runtimeImage"]
    dockerfile = ROOT / "server/src/main/docker/Dockerfile.m0"
    if dockerfile.read_text().splitlines()[0] != "FROM " + lock["reference"] or lock["platform"] != "linux/amd64":
        raise ValueError("runtime Dockerfile and locked platform manifest differ")
    probe_directory = output / "probe"
    probe_directory.mkdir()
    probe_directory.chmod(0o755)
    driver = probe_directory / "m0driver"
    environment = dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH="amd64")
    if run_logged([str(ROOT / "scripts/dev/go-m0.sh"), "build", "-trimpath", "-o", str(driver), "./cmd/m0driver"],
                  output / "go-build.log", ROOT, timeout=300, environment=environment):
        raise ValueError("Linux runtime probe could not be built")
    driver.chmod(0o755)
    return lock, dockerfile, probe_directory


def _build_image(docker, runtime, lock, dockerfile, image, token):
    if run_logged([*docker.prefix, "build", "--platform", lock["platform"], "--network", "none", "--pull",
                   "--force-rm", "--label", "etcd-hbase.m0.verify=" + token, "--file", str(dockerfile), "--tag", image, str(runtime)],
                  docker.output / "docker-build.log", ROOT, timeout=600):
        raise ValueError("runtime image build failed")


def _create_container(docker, image, probe_directory, token):
    container = docker.call(["create", "--name", "etcd-hbase-m0-" + token,
        "--label", "etcd-hbase.m0.verify=" + token, "--read-only", "--tmpfs", "/tmp:rw,exec,nosuid,size=64m", "--network", "none",
        "--memory", "512m", "--pids-limit", "256", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--mount", f"type=bind,src={probe_directory},dst=/verify,readonly", image])
    if not re.fullmatch(r"[0-9a-f]{64}", container):
        raise ValueError("Docker did not return an owned container ID")
    return container


def _observe_container(docker, container, image, lock, output):
    docker.call(["start", container])
    user = docker.call(["inspect", "--format", "{{.Config.User}}", container])
    if user != "65532:65532":
        raise ValueError("runtime image must run as the unprivileged configured user")
    docker.call(["exec", container, "java", "-XshowSettings:properties", "-version"], log="java-version.log")
    # java writes version/properties to stderr, retained in the bounded command log.
    properties = (output / "java-version.log").read_text()
    java = java_properties(properties, lock)
    result = await_transport([*docker.prefix, "exec", container, "/verify/m0driver", *probe_arguments("m0-runtime-image", 2379)],
                             "m0-runtime-image", output)
    docker.call(["stop", "--time", "10", container], log="container-stop.log", timeout=20)
    code = int(docker.call(["inspect", "--format", "{{.State.ExitCode}}", container]))
    if code not in (0, 143):
        raise ValueError("container did not stop before forced termination")
    image_id = docker.call(["image", "inspect", "--format", "{{.Id}}", image])
    docker.call(["logs", container], log="server.log")
    return {"image_id": image_id, "user": user, "java": java, "attempts": result["attempts"], "exit_code": code}


def _run_owned_image(docker, runtime, lock, dockerfile, probe_directory, output):
    token = uuid.uuid4().hex
    image = "etcd-hbase-m0-verify:" + token
    # Ownership starts before commands whose outcome may be unknown on failure.
    try:
        _build_image(docker, runtime, lock, dockerfile, image, token)
        container = _create_container(docker, image, probe_directory, token)
        return _observe_container(docker, container, image, lock, output)
    finally:
        docker.cleanup(token)


def _image_summary(lock, observation):
    return {"status": "passed", "scope": "runtime-image-transport", "image_test_passed": True,
            "base": lock["reference"], "platform": lock["platform"], "image_id": observation["image_id"],
            "user": observation["user"], "read_only": True, "network": "none", "java": observation["java"],
            "grpc_code": "Unimplemented", "attempts": observation["attempts"], "exit_code": observation["exit_code"],
            "backend_semantics_verified": False, "formal_image_published": False, "owned_resources_removed": True}


def _write_summary(output, summary):
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))


def verify(runtime, output):
    output.mkdir(parents=True, exist_ok=False)
    docker = Docker(output)
    reason = _docker_unavailable_reason(docker)
    if reason:
        _write_summary(output, {"status": "unavailable", "reason": reason, "image_test_passed": False})
        return 2
    lock, dockerfile, probe_directory = _prepare_image_probe(output)
    observation = _run_owned_image(docker, runtime, lock, dockerfile, probe_directory, output)
    _write_summary(output, _image_summary(lock, observation))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        with termination_is_exception():
            raise SystemExit(verify(arguments.runtime.resolve(), arguments.output.resolve()))
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as failure:
        raise SystemExit(f"runtime image verification failed: {type(failure).__name__}; inspect owned image logs") from None
