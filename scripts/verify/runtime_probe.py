"""Bounded transport-only probes shared by local package and container verification."""
import json
import subprocess
import time


def decode_observation(output):
    lines = output.splitlines()
    if len(lines) != 1:
        raise ValueError("runtime probe requires exactly one JSONL observation")
    record = json.loads(lines[0])
    if not isinstance(record, dict) or not isinstance(record.get("error"), dict):
        raise ValueError("runtime probe must retain its classified failure")
    return record


def check_observation(output, expected_case):
    record = decode_observation(output)
    if record.get("case_id") != expected_case or record.get("evidence_scope") != "transport" or record.get("method") != "etcdserverpb.KV/Range":
        raise ValueError("runtime probe has wrong identity or evidence scope")
    if record.get("response") is not None or record["error"].get("grpc_code") != "Unimplemented":
        raise ValueError("runtime skeleton must return explicit UNIMPLEMENTED")
    request = record.get("request", {}).get("fields", {})
    if request.get("key") != "YQD/" or request.get("revision") != "18014398509481984":
        raise ValueError("runtime request bytes or int64 were not preserved")
    return record


def probe_arguments(case, port):
    return ["--endpoint", f"127.0.0.1:{port}", "--mode", "raw", "--case", case,
            "--revision", "18014398509481984", "--timeout", "1s", "--trace", "-"]


def await_transport(command, case, output, timeout=30, alive=None):
    deadline = time.monotonic() + timeout
    attempts = 0
    while time.monotonic() < deadline:
        if alive is not None and not alive():
            raise ValueError("owned runtime exited before protocol readiness")
        attempts += 1
        try:
            result = subprocess.run(command, capture_output=True, timeout=min(5, max(0.1, deadline-time.monotonic())))
        except subprocess.TimeoutExpired:
            continue
        if result.returncode != 1:
            raise ValueError("runtime probe unexpectedly succeeded or failed to execute")
        record = decode_observation(result.stdout)
        code = record["error"].get("grpc_code")
        if code == "Unimplemented":
            check_observation(result.stdout, case)
            (output / "transport.jsonl").write_bytes(result.stdout)
            (output / "probe.stderr.log").write_bytes(result.stderr)
            return {"attempts": attempts, "record": record}
        if record.get("response") is not None or code not in {"Unavailable", "DeadlineExceeded"}:
            raise ValueError("runtime probe observed an unexpected protocol outcome")
        # Only transport startup failures are retried, with one outer deadline.
        time.sleep(min(0.1, max(0, deadline-time.monotonic())))
    raise ValueError("runtime did not return the expected transport observation before deadline")
