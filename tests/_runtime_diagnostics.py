from __future__ import annotations

import faulthandler
import os
import traceback
from contextlib import suppress
from typing import TYPE_CHECKING, Unpack

from sg_event_daemon._worker import WorkerRuntime

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

    from sg_event_daemon._worker import _WorkerRuntimeOptions


class _RuntimeDiagnostics:
    def __init__(self, destination: Path, stderr_fd: int) -> None:
        self._destination = destination
        self._stderr_fd = stderr_fd
        self._exception = 'no initiating exception recorded\n'

    @staticmethod
    def _write(fd: int, text: str, *, stacks: bool) -> None:
        pending = text.encode('utf-8', errors='backslashreplace')
        while pending:
            written = os.write(fd, pending)
            if written == 0:
                raise OSError('diagnostic write made no progress')
            pending = pending[written:]
        if stacks:
            faulthandler.dump_traceback(file=fd, all_threads=True)

    def _emit(self, text: str, *, stacks: bool = False) -> None:
        for destination in (self._stderr_fd, self._destination):
            # A missing file or closed stderr must not replace the real exit.
            with suppress(OSError, RuntimeError, ValueError):
                if isinstance(destination, int):
                    self._write(destination, text, stacks=stacks)
                else:
                    with destination.open('ab', buffering=0) as stream:
                        self._write(stream.fileno(), text, stacks=stacks)

    def _record_exception(self, error: BaseException) -> None:
        self._exception = ''.join(traceback.format_exception(error))
        self._emit('runtime initiating exception:\n' + self._exception)

    def _record_exit(self, runtime: WorkerRuntime, code: int) -> None:
        # Avoid runtime/cache accessors: a retained owner may hold their locks.
        try:
            owners = runtime._threads.copy()
            checkpoints = runtime._checkpoint_host._files.copy()
            completed = runtime._completed_thread_names.copy()
            owners.update(
                (thread.name.removeprefix('sg-event-daemon:'), thread)
                for _view, thread in checkpoints.values()
            )
            live = tuple(
                sorted(
                    identity
                    for identity, thread in owners.items()
                    if identity not in completed and thread.is_alive()
                )
            )
            snapshot = (
                f'stop_reason={runtime._stop_reason!r}\n'
                f'fatal_reason={runtime._fatal_reason!r}\n'
                f'shutdown_deadline_at={runtime._shutdown_deadline_at!r}\n'
                f'live_owned_threads={live!r}\n'
                f'stuck_threads={runtime._stuck_thread_names!r}\n'
            )
        except (AttributeError, RuntimeError, TypeError) as error:
            snapshot = f'runtime snapshot failed: {type(error).__name__}\n'
        self._emit(
            f'runtime hard exit: {code}\n'
            f'pid={os.getpid()} PYTEST_CURRENT_TEST={os.environ.get("PYTEST_CURRENT_TEST")!r}\n'
            f'{snapshot}{self._exception}',
            stacks=True,
        )


def install_runtime_diagnostics(
    monkeypatch: pytest.MonkeyPatch, destination: Path, stderr_fd: int
) -> None:
    initialize = WorkerRuntime.__init__
    hard_exit = os._exit

    def initialize_with_diagnostics(
        runtime: WorkerRuntime, **options: Unpack[_WorkerRuntimeOptions]
    ) -> None:
        if 'force_exit' in options:
            initialize(runtime, **options)
            return
        diagnostics = _RuntimeDiagnostics(destination, stderr_fd)

        def force_exit(code: int) -> None:
            try:
                diagnostics._record_exit(runtime, code)
            finally:
                hard_exit(code)

        options['force_exit'] = force_exit
        initialize(runtime, **options)
        vars(runtime)['_test_runtime_diagnostics'] = diagnostics

    monkeypatch.setattr(WorkerRuntime, '__init__', initialize_with_diagnostics)


def record_runtime_exception(runtime: WorkerRuntime | None, error: BaseException | None) -> None:
    if runtime is None or error is None:
        return
    diagnostics = vars(runtime).get('_test_runtime_diagnostics')
    if isinstance(diagnostics, _RuntimeDiagnostics):
        # Diagnostic failures must preserve the initiating test exception.
        with suppress(Exception):
            diagnostics._record_exception(error)
