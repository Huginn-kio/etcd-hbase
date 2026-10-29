#!/usr/bin/env python3
"""Run the frozen public M0 build, native oracle, package and actual image verification."""
import argparse
import datetime
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from build_process import run_logged, termination_is_exception
from evidence import require_unchanged_source
from m0_evidence import collect, write_index

ROOT = Path(__file__).resolve().parents[2]
PYTHON = sys.executable


class PublicVerification:
    def __init__(self, output):
        self.output = output
        self.steps = []
        self.environment = dict(os.environ, ETCD_HBASE_MAVEN_REPOSITORY=str(ROOT / ".artifacts/m0/j09/maven-repository"))

    def step(self, name, command, timeout=600, stdout=None, unavailable=False, environment=None):
        print(f"M0 {name}: running", flush=True)
        log = self.output / (stdout or name + ".log")
        stderr = self.output / (name + ".stderr.log") if stdout else None
        record = {"id": name, "status": "failed", "exit_code": 1, "log": log.name}
        self.steps.append(record)
        try:
            code = run_logged(command, log, ROOT, timeout=timeout,
                              environment=environment or self.environment, stderr_path=stderr)
            record.update(exit_code=code, status="passed" if code == 0 else "unavailable" if unavailable and code == 2 else "failed")
            if record["status"] == "failed":
                print("\n".join(log.read_text(errors="replace").splitlines()[-60:]), file=sys.stderr)
                if stderr:
                    print("\n".join(stderr.read_text(errors="replace").splitlines()[-30:]), file=sys.stderr)
                raise ValueError("required public verification step failed: " + name)
            print(f"M0 {name}: {record['status']}", flush=True)
        finally:
            (self.output / "steps.json").write_text(json.dumps(self.steps, indent=2) + "\n")

    def python(self, name, script, *arguments, **options):
        self.step(name, [PYTHON, str(ROOT / "scripts/verify" / script), *map(str, arguments)], **options)

    def run(self):
        output = self.output
        output.mkdir(parents=True, exist_ok=False)
        subprocess.run([PYTHON, str(ROOT / "scripts/verify/j06-source.py"), str(output)], cwd=ROOT, check=True)
        source = json.loads((output / "source.json").read_text())
        require_unchanged_source(ROOT, source)
        summary = {"phase": "M0", "scope": "public", "status": "failed", "source": source, "m0_accepted": False}
        try:
            self.python("guards", "public-checks.py", "--output", output / "guards")
            self.python("public-policy", "public-build-policy.py", "--output", output / "public-policy.json")
            self.python("module-lock", "go-module-lock.py")
            go = str(ROOT / "scripts/dev/go-m0.sh")
            self.step("module-verify", [go, "mod", "verify"])
            self.step("go-test", [go, "test", "-json", "./..."], stdout="go-test.jsonl")
            self.step("go-race", [go, "test", "-json", "-race", "./..."], stdout="go-race.jsonl",
                      environment=dict(self.environment, GORACE="halt_on_error=1"))
            self.step("go-vet", [go, "vet", "./..."])
            self.step("native", [str(ROOT / "scripts/verify/native-oracle.sh"), "--artifact-root", str(output / "native")])
            self.python("api-matrix", "api-matrix.py", "--matrix", "docs/api-matrix.md", "--wire-contract", "protocol/baseline/wire-contract.json", "--cases", "testkit/cases/m0-wire.json")
            self.python("proto-source", "vendor-proto.py")
            self.python("reproducible", "maven-reproducible-build.py", "--phase", "install", "--output", output / "reproducible", timeout=2400)
            self.python("spi-boundary", "m0-spi.py", "--jar", ROOT / "spi/target/spi-0.1.0-SNAPSHOT.jar")
            self.python("package", "package-runtime.py", "--verification", output / "reproducible/summary.json", "--output", output / "runtime")
            self.python("packaged-runtime", "packaged-runtime.py", "--runtime", output / "runtime", "--output", output / "packaged-runtime")
            self.python("runtime-image", "runtime-image.py", "--runtime", output / "runtime", "--output", output / "runtime-image", unavailable=True, timeout=1200)
            self.python("release-readiness", "release-readiness.py", "--output", output / "release-readiness.json")
            self.step("diff-check", ["git", "diff", "--check"])
            require_unchanged_source(ROOT, source)
            summary = collect(output)
        finally:
            (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            write_index(output)
            print(f"M0 evidence: {output / 'INDEX.json'}", flush=True)
        print(json.dumps({key: summary[key] for key in ("phase", "status", "source", "java_builds", "identical_jars", "go", "race", "native", "runtime", "image", "m0_accepted", "waiting_inputs")}))
        return 0 if summary["status"] == "passed" else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=("public",), required=True)
    parser.add_argument("--artifact-root", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    identifier = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    output = (args.artifact_root or ROOT / ".artifacts/m0/public" / identifier).resolve()
    if not output.is_relative_to(ROOT / ".artifacts/m0"):
        raise ValueError("public verification evidence must stay under .artifacts/m0")
    lock_path = ROOT / ".artifacts/m0/j09/m0.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock, termination_is_exception():
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("another public M0 build owns this worktree") from None
        return PublicVerification(output).run()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        raise SystemExit(f"M0 public verification failed: {error}") from None
