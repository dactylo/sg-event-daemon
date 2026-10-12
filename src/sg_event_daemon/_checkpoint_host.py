"""Own independently timed checkpoint connections for each process-local file."""

from __future__ import annotations

import math
import threading
import time
from typing import TYPE_CHECKING, TypedDict, Unpack

from ._checkpoint import Checkpointer, CheckpointState
from ._resources import close_resources
from ._sqlite import Schema, open_database

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path
    from types import TracebackType
    from typing import Self


class _CheckpointFailure:
    """Report ordinary failures at the checkpoint thread ownership boundary."""

    def __init__(self, identity: str, on_failure: Callable[[str], None]) -> None:
        """Bind one file identity to the owning worker's fatal callback."""
        self._identity = identity
        self._on_failure = on_failure

    def __enter__(self) -> Self:
        """Enter the checkpoint thread's failure boundary."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Report ordinary errors once while propagating process-control errors."""
        if not isinstance(exc_value, Exception):
            return False
        self._on_failure(
            f'checkpoint:{self._identity} thread failed: {type(exc_value).__name__}: {exc_value}'
        )
        return True


class _CheckpointStatus(TypedDict):
    """Share a lock and the latest state between the owner and its views."""

    lock: threading.Lock
    state: CheckpointState


class CheckpointHostStoppedError(RuntimeError):
    """Report registration interrupted by the host's shutdown request."""


class CheckpointView:
    """Provide a thread-safe read-only view of one file's checkpoint state."""

    def __init__(self, status: _CheckpointStatus | None = None) -> None:
        """Share a host-owned state cell, or represent an unregistered idle file."""
        self._status: _CheckpointStatus = (
            status if status is not None else {'lock': threading.Lock(), 'state': 'idle'}
        )

    def state(self) -> CheckpointState:
        """Return idle before the first attempt, then complete or incomplete."""
        with self._status['lock']:
            return self._status['state']


class _CheckpointHostSeams(TypedDict, total=False):
    """Type the optional monotonic clock and checkpointer factory replacements."""

    clock: Callable[[], float]
    checkpointer_factory: Callable[..., Checkpointer]


class _CheckpointFile(TypedDict):
    """Hold one file's thread inputs and synchronized health output."""

    path: Path
    identity: str
    schema: Schema
    first_due: float
    status: _CheckpointStatus


class CheckpointHost:
    """Own one daemon checkpoint thread per resolved database path.

    Register files after their owner initializes them. Each checkpoint thread
    opens and closes its own existing-only connection. `close()` interrupts
    timer waits and joins all threads; registration after close is rejected.
    """

    def __init__(
        self,
        *,
        interval_secs: float,
        synchronous: str,
        on_failure: Callable[[str], None],
        **seams: Unpack[_CheckpointHostSeams],
    ) -> None:
        """Configure independent file timers and the fatal-failure callback.

        Args:
            interval_secs: Finite positive time between checkpoint attempts.
            synchronous: SQLite synchronous policy for each owned connection.
            on_failure: Called once when a file's checkpoint thread fails.
            **seams: Optional monotonic clock and checkpointer factory.

        Raises:
            ValueError: `interval_secs` is not finite and positive.
        """
        if not math.isfinite(interval_secs) or interval_secs <= 0:
            raise ValueError('checkpoint interval must be finite and positive')
        self._interval_secs = interval_secs
        self._synchronous = synchronous
        self._on_failure = on_failure
        self._clock = seams.get('clock', time.monotonic)
        self._checkpointer_factory = seams.get('checkpointer_factory', Checkpointer)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._files: dict[Path, tuple[CheckpointView, threading.Thread]] = {}

    def _run(self, file: _CheckpointFile) -> None:
        """Open, checkpoint, and close one file entirely on its owning thread."""
        with _CheckpointFailure(file['identity'], self._on_failure):
            database = open_database(
                file['path'],
                file['identity'],
                file['schema'],
                synchronous=self._synchronous,
                create=False,
            )
            try:
                checkpointer = self._checkpointer_factory(
                    database.connection,
                    file['path'],
                    interval_secs=self._interval_secs,
                )
                due = file['first_due']
                while not self._stop.wait(max(0.0, due - self._clock())):
                    try:
                        checkpointer.run_once()
                    finally:
                        with file['status']['lock']:
                            file['status']['state'] = checkpointer.state
                    elapsed = max(0.0, self._clock() - due)
                    due += (int(elapsed // self._interval_secs) + 1) * self._interval_secs
            except BaseException as error:
                close_resources((database,), primary_error=error)
                raise
            else:
                database.close()

    def register(self, path: Path, identity: str, schema: Schema) -> CheckpointView:
        """Start one timer for an initialized file, reusing resolved-path views.

        The first attempt waits one interval plus this registration's
        `interval * index / count` offset. Later registrations do not change
        existing timers.

        Args:
            path: Initialized database path; registration uses its resolved
                path.
            identity: Stable identity matching the database's stored identity.
                A mismatch fails asynchronously through `on_failure`.
            schema: Database schema used when the checkpoint thread opens it.

        Returns:
            The checkpoint view for the resolved path. Re-registration returns
            the existing view and ignores the new `identity` and `schema`.

        Raises:
            CheckpointHostStoppedError: This host is stopping or closed.
        """
        path = path.resolve()
        with self._lock:
            if self._stop.is_set():
                raise CheckpointHostStoppedError('checkpoint host is stopping or closed')
            existing = self._files.get(path)
            if existing is not None:
                return existing[0]
            index = len(self._files)
            first_due = self._clock() + self._interval_secs * (1 + index / (index + 1))
            file = _CheckpointFile(
                path=path,
                identity=identity,
                schema=schema,
                first_due=first_due,
                status={'lock': threading.Lock(), 'state': 'idle'},
            )
            view = CheckpointView(file['status'])
            thread = threading.Thread(
                target=self._run,
                args=(file,),
                daemon=True,
                name=f'sg-event-daemon:checkpoint:{identity}',
            )
            thread.start()
            self._files[path] = view, thread
            return view

    def request_stop(self) -> None:
        """Interrupt schedule waits and reject new registrations without joining."""
        self._stop.set()

    def threads(self) -> tuple[threading.Thread, ...]:
        """Return a stable snapshot of started owners for monitored, bounded joins.

        Each thread owns its database connection through final cleanup. A
        stopped thread therefore has no remaining connection work.
        """
        with self._lock:
            return tuple(thread for _view, thread in self._files.values())

    def close(self) -> None:
        """Interrupt timer waits and join threads after their connections close."""
        self.request_stop()
        for thread in self.threads():
            if thread is not threading.current_thread():
                thread.join()
