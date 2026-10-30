"""Run owned build process groups with bounded cancellation and captured diagnostics."""
from contextlib import contextmanager
import os
from contextlib import ExitStack
from pathlib import Path
import signal
import subprocess
import time


def _terminate(signum, _frame):
    raise SystemExit(128 + signum)


@contextmanager
def termination_is_exception():
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _terminate)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _signal_group(process, signum, deadline):
    while True:
        # Exit publication and group signaling are not atomic on Darwin. Neither
        # a pending poll nor a reaped leader settles the state of the whole group.
        process.poll()
        try:
            os.killpg(process.pid, signum)
            return
        except PermissionError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(0.05, remaining))


def _terminate_group(process, deadline):
    try:
        _signal_group(process, signal.SIGTERM, deadline)
        while True:
            _signal_group(process, 0, deadline)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.05, remaining))
    except ProcessLookupError:
        return True
    except PermissionError:
        # An unresolved graceful phase still requires a real group KILL attempt.
        return False


def stop_group(process, grace=5):
    grace_deadline = time.monotonic() + max(0, grace)
    cleanup_deadline = grace_deadline + 5
    if not _terminate_group(process, grace_deadline):
        try:
            _signal_group(process, signal.SIGKILL, cleanup_deadline)
        except ProcessLookupError:
            pass
    process.wait(timeout=min(5, max(0, cleanup_deadline - time.monotonic())))


def run_logged(command, log_path, cwd, timeout=1200, environment=None, stderr_path=None, termination_grace=5):
    with ExitStack() as resources, termination_is_exception():
        log = resources.enter_context(Path(log_path).open("wb"))
        errors = resources.enter_context(Path(stderr_path).open("wb")) if stderr_path else subprocess.STDOUT
        process = subprocess.Popen(command, cwd=cwd, env=environment, stdout=log,
                                   stderr=errors, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except BaseException:
            stop_group(process, grace=termination_grace)
            raise
        if code:
            stop_group(process, grace=termination_grace)
        return code
