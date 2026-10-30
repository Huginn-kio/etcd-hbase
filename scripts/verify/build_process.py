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


def _signal_group(process, signum):
    try:
        os.killpg(process.pid, signum)
    except PermissionError:
        # Darwin can report EPERM for a group containing only an unreaped zombie.
        # Reap an exited leader and retry the OS check; a real denial still raises.
        if process.poll() is None:
            raise
        os.killpg(process.pid, signum)


def stop_group(process, grace=5):
    try:
        _signal_group(process, signal.SIGTERM)
    except ProcessLookupError:
        process.wait(timeout=5)
        return
    deadline = time.monotonic() + grace
    while True:
        process.poll()  # Reap the leader, but check the group independently.
        try:
            _signal_group(process, 0)
        except ProcessLookupError:
            break
        if time.monotonic() >= deadline:
            try:
                _signal_group(process, signal.SIGKILL)
            except ProcessLookupError:
                pass
            break
        time.sleep(0.05)
    process.wait(timeout=5)


def run_logged(command, log_path, cwd, timeout=1200, environment=None, stderr_path=None, termination_grace=5):
    with ExitStack() as resources, termination_is_exception():
        log = resources.enter_context(Path(log_path).open("wb"))
        errors = resources.enter_context(Path(stderr_path).open("wb")) if stderr_path else subprocess.STDOUT
        process = subprocess.Popen(command, cwd=cwd, env=environment, stdout=log,
                                   stderr=errors, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
            if code:
                stop_group(process, grace=termination_grace)
            return code
        except BaseException:
            stop_group(process, grace=termination_grace)
            raise
