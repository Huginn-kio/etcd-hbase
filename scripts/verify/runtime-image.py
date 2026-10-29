#!/usr/bin/env python3
"""Run the pinned Java 21 image as non-root, probe loopback RPC, then stop owned resources."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
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
        for kind in ("container", "image"):
            identifiers = self.call([kind, "ls", "--quiet", "--all", "--filter", "label=etcd-hbase.m0.verify=" + token]).splitlines()
            if any(not re.fullmatch(r"(?:sha256:)?[0-9a-f]{12,64}", value) for value in identifiers):
                raise ValueError("Docker returned an invalid owned resource ID")
            if identifiers:
                self.call([kind, "rm", "--force", *identifiers], timeout=30)


def java_properties(output, lock):
    def property_value(name):
        found = re.search(r"^\s*" + re.escape(name) + r" = ([^\r\n]+)$", output, re.MULTILINE)
        return found.group(1).strip() if found else None
    runtime = property_value("java.runtime.version")
    vendor = property_value("java.vendor")
    if property_value("java.specification.version") != "21" or runtime not in {lock["javaRelease"], lock["javaRelease"] + "-LTS"} or vendor != lock["javaVendor"]:
        raise ValueError("container JVM does not match the locked vendor and patch")
    return {"specification": "21", "runtime": runtime, "vendor": vendor}


def verify(runtime, output):
    output.mkdir(parents=True, exist_ok=False)
    if shutil.which("docker") is None:
        result = {"status": "unavailable", "reason": "docker-cli-unavailable", "image_test_passed": False}
        (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))
        return 2
    docker = Docker(output)
    try:
        docker.call(["info"], log="docker-info.log", timeout=15)
    except (ValueError, OSError, subprocess.SubprocessError):
        result = {"status": "unavailable", "reason": "default-local-docker-daemon-unavailable", "image_test_passed": False}
        (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))
        return 2
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
    token = uuid.uuid4().hex
    image = "etcd-hbase-m0-verify:" + token
    container = None
    summary = None
    try:
        if run_logged([*docker.prefix, "build", "--platform", lock["platform"], "--network", "none", "--pull",
                       "--force-rm", "--label", "etcd-hbase.m0.verify=" + token, "--file", str(dockerfile), "--tag", image, str(runtime)], output / "docker-build.log", ROOT, timeout=600):
            raise ValueError("runtime image build failed")
        container = docker.call(["create", "--name", "etcd-hbase-m0-" + token,
            "--label", "etcd-hbase.m0.verify=" + token, "--read-only", "--tmpfs", "/tmp:rw,exec,nosuid,size=64m", "--network", "none",
            "--memory", "512m", "--pids-limit", "256", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--mount", f"type=bind,src={probe_directory},dst=/verify,readonly", image])
        if not re.fullmatch(r"[0-9a-f]{64}", container):
            raise ValueError("Docker did not return an owned container ID")
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
        summary = {"status": "passed", "scope": "runtime-image-transport", "image_test_passed": True,
                   "base": lock["reference"], "platform": lock["platform"], "image_id": image_id,
                   "user": user, "read_only": True, "network": "none", "java": java,
                   "grpc_code": "Unimplemented", "attempts": result["attempts"], "exit_code": code,
                   "backend_semantics_verified": False, "formal_image_published": False}
    finally:
        docker.cleanup(token)
    summary["owned_resources_removed"] = True
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))
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
