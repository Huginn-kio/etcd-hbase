"""Run owned build process groups with bounded cancellation and captured diagnostics."""
from contextlib import contextmanager
import os
from pathlib import Path
import signal
import subprocess


@contextmanager
def termination_is_exception():
    previous = signal.getsignal(signal.SIGTERM)
    def terminate(signum, _frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, terminate)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def run_logged(command, log_path, cwd, timeout=1200, environment=None):
    with Path(log_path).open("wb") as log, termination_is_exception():
        process = subprocess.Popen(command, cwd=cwd, env=environment, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
            if code:
                stop_group(process)
            return code
        except BaseException:
            stop_group(process)
            raise
