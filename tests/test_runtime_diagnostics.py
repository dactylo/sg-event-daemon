from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon._worker import WorkerKind, WorkerRuntime, WorkerSpec

if TYPE_CHECKING:
    from collections.abc import Callable


def test_explicit_force_exit_is_preserved() -> None:
    exits: list[int] = []
    callback: Callable[[int], None] = exits.append
    runtime = WorkerRuntime(
        spec=WorkerSpec('worker', 'site', WorkerKind._SITE, 'site', (), ()),
        spawn_instance='explicit-exit',
        source_factory=lambda _name, _slots, _host: pytest.fail('no source requested'),
        lane_factory=lambda _name, _slots, _host: pytest.fail('no lane requested'),
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        force_exit=callback,
    )
    assert runtime._force_exit is callback
    runtime._force_exit(7)
    assert exits == [7]


@pytest.mark.parametrize('broken_destination', [False, True])
def test_hard_exit_preserves_evidence_outside_pytest_capture(
    pytester: pytest.Pytester, *, broken_destination: bool
) -> None:
    root = Path(__file__).resolve().parents[1]
    pytester.makeconftest(
        f'import sys\nsys.path.insert(0, {str(root)!r})\npytest_plugins = ("tests.conftest",)\n'
    )
    pytester.makepyfile(
        test_exit=f"""
import threading
from sg_event_daemon._worker import WorkerKind, WorkerRuntime, WorkerSpec
from tests._runtime_diagnostics import install_runtime_diagnostics
from tests.test_worker import _release_failed_source_runtime

class BlockedSource:
    def __init__(self):
        self.entered = threading.Event()

    def _run(self):
        self.entered.set()
        threading.Event().wait()

    def _request_stop(self, *, shutdown_deadline_at=None):
        pass

    def _close(self):
        pass

def test_exit(monkeypatch, tmp_path, _runtime_stderr_fd):
    if {broken_destination!r}:
        install_runtime_diagnostics(monkeypatch, tmp_path, _runtime_stderr_fd)
    source = BlockedSource()
    runtime = WorkerRuntime(
        spec=WorkerSpec("worker", "site", WorkerKind._SITE, "site", ("events",), ()),
        spawn_instance="diagnostic-spawn",
        source_factory=lambda _name, _slots, _host: source,
        lane_factory=lambda _name, _slots, _host: source,
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=0.03,
        max_concurrent_catchup_readers=1,
    )
    actual_exit = runtime._force_exit
    def exit_while_locks_are_held(code):
        runtime._state_lock.acquire()
        runtime._checkpoint_host._lock.acquire()
        actual_exit(code)
    runtime._force_exit = exit_while_locks_are_held
    runtime.start()
    assert source.entered.wait(2)
    print("CAPTURED-OUTPUT-MUST-NOT-SURVIVE", flush=True)
    runtime._fatal_reason = "diagnostic fatal reason"
    try:
        raise AssertionError("initiating assertion before shutdown")
    except AssertionError as error:
        error.add_note("initiating exception note")
        _release_failed_source_runtime(runtime, (None, None), None, None)
"""
    )
    base = pytester.path / 'runtime-temp'
    result = pytester.runpytest_subprocess('--basetemp', str(base), timeout=15)
    assert result.ret == 1
    stderr = result.stderr.str()
    evidence = [stderr]
    if not broken_destination:
        (diagnostic,) = base.glob('runtime-diagnostics-*.log')
        evidence.append(diagnostic.read_text())
    for record in evidence:
        for expected in (
            'runtime hard exit: 1',
            'test_exit.py::test_exit (call)',
            "stop_reason='test'",
            'diagnostic fatal reason',
            'shutdown_deadline_at=',
            "live_owned_threads=('source:events',)",
            "stuck_threads=('source:events',)",
            'initiating assertion before shutdown',
            'initiating exception note',
            'test_exit.py',
            'in _run',
            'in _monitor',
        ):
            assert expected in record
    assert 'CAPTURED-OUTPUT-MUST-NOT-SURVIVE' not in stderr + result.stdout.str()
