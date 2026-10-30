#!/usr/bin/env python3
"""Run the packaged JAR classpath on the locked local JDK and verify shutdown."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
from runtime_probe import await_transport, probe_arguments
from build_process import run_logged, termination_is_exception

ROOT = Path(__file__).resolve().parents[2]


def _prepare_probe(runtime, output):
    jdk = os.environ.get("ETCD_HBASE_JDK21_HOME")
    if not jdk:
        raise ValueError("set the locked JDK home for packaged runtime verification")
    driver = output / "m0driver"
    if run_logged([str(ROOT / "scripts/dev/go-m0.sh"), "build", "-trimpath", "-o", str(driver), "./cmd/m0driver"],
                  output / "go-build.log", ROOT, timeout=300):
        raise ValueError("runtime probe driver build failed")
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    config = json.loads((runtime / "server.json").read_text())
    config["listeners"][0]["port"] = port
    config_path = output / "server.json"
    config_path.write_text(json.dumps(config) + "\n")
    environment = dict(os.environ, JAVA_HOME=jdk)
    for name in ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
        environment.pop(name, None)
    return driver, port, config_path, environment


def _await_startup(process, output):
    deadline = time.monotonic() + 30
    while b"listeners=1 data_readiness=UNIMPLEMENTED" not in (output / "server.log").read_bytes():
        if (output / "server.log").stat().st_size > 1 << 20:
            raise ValueError("packaged JVM startup log exceeded its budget")
        if process.poll() is not None or time.monotonic() >= deadline:
            raise ValueError("packaged JVM did not report its owned listener startup")
        time.sleep(0.05)


def _stop_runtime(process):
    process.send_signal(signal.SIGTERM)
    process.wait(timeout=15)
    if process.returncode not in (0, 143):
        raise ValueError("packaged JVM did not finish graceful shutdown")


def _cleanup_runtime(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _run_owned_runtime(runtime, output, driver, port, config_path, environment):
    process = None
    with (output / "server.log").open("wb") as log, termination_is_exception():
        try:
            process = subprocess.Popen([str(runtime / "etcd-hbase"), "--config", str(config_path)],
                                       cwd=runtime, env=environment, stdout=log, stderr=subprocess.STDOUT)
            _await_startup(process, output)
            result = await_transport([str(driver), *probe_arguments("m0-packaged-runtime", port)],
                                     "m0-packaged-runtime", output, alive=lambda: process.poll() is None)
            _stop_runtime(process)
        finally:
            _cleanup_runtime(process)
    return {"attempts": result["attempts"], "exit_code": process.returncode}


def _write_summary(output, observation):
    summary = {"scope": "packaged-transport", "started": True, "grpc_code": "Unimplemented", "attempts": observation["attempts"],
               "exit_code": observation["exit_code"], "backend_semantics_verified": False}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))
    return summary


def verify(runtime, output):
    output.mkdir(parents=True, exist_ok=False)
    driver, port, config_path, environment = _prepare_probe(runtime, output)
    observation = _run_owned_runtime(runtime, output, driver, port, config_path, environment)
    return _write_summary(output, observation)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        verify(args.runtime.resolve(), args.output.resolve())
    except (ValueError, OSError, subprocess.SubprocessError):
        raise SystemExit("packaged runtime verification failed; inspect owned probe logs") from None
