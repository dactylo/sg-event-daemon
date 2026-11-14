"""Own independently timed checkpoint connections for each process-local file."""

from __future__ import annotations

import math
import threading
import time
from typing import TYPE_CHECKING, TypedDict, Unpack

from ._checkpoint import Checkpointer
from ._resources import close_resources
from ._sqlite import (
    WAL_TRIGGER_BYTES,
    WAL_TRIGGER_CHECK_SECS,
    Schema,
    open_database,
    wal_file_bytes,
)

if TYPE_CHECKING:
    import logging
    from collections.abc import Callable, Mapping
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
    stalled: bool


class CheckpointHostStoppedError(RuntimeError):
    """Report registration interrupted by the host's shutdown request."""


class CheckpointView:
    """Provide a thread-safe read-only view of one file's checkpoint state."""

    def __init__(self, status: _CheckpointStatus | None = None) -> None:
        """Share a host-owned state cell, or represent an unregistered idle file."""
        self._status: _CheckpointStatus = (
            status if status is not None else {'lock': threading.Lock(), 'stalled': False}
        )

    def stalled(self) -> bool:
        """Return whether the latest attempt still crosses either bound."""
        with self._status['lock']:
            return self._status['stalled']


class _CheckpointHostSeams(TypedDict, total=False):
    """Type optional clock, checkpointer factory, and WAL trigger replacements."""

    clock: Callable[[], float]
    checkpointer_factory: Callable[..., Checkpointer]
    wal_trigger_bytes: int


class _CheckpointFile(TypedDict):
    """Hold one file's thread inputs and synchronized health output."""

    path: Path
    identity: str
    schema: Schema
    first_due: float
    status: _CheckpointStatus
    log_extra: Mapping[str, str]


class CheckpointHost:
    """Own one daemon checkpoint thread per resolved database path.

    Register files after their owner initializes them. Each checkpoint thread
    opens and closes its own existing-only connection. `close()` interrupts
    timer waits and joins every thread except the caller; registration after
    close is rejected.
    """

    def __init__(
        self,
        *,
        interval_secs: float,
        synchronous: str,
        on_failure: Callable[[str], None],
        logger: logging.Logger,
        **seams: Unpack[_CheckpointHostSeams],
    ) -> None:
        """Configure independent file timers and the fatal-failure callback.

        Args:
            interval_secs: Finite positive time between scheduled attempts.
            synchronous: SQLite synchronous policy for each owned connection.
            on_failure: Called once when a file's checkpoint thread fails.
            logger: Owning source logger for checkpoint transitions.
            **seams: Optional clock, checkpointer factory, and WAL trigger.

        Raises:
            ValueError: `interval_secs` is not finite and positive.
        """
        if not math.isfinite(interval_secs) or interval_secs <= 0:
            raise ValueError('checkpoint interval must be finite and positive')
        self._interval_secs = interval_secs
        self._synchronous = synchronous
        self._on_failure = on_failure
        self._logger = logger
        self._clock = seams.get('clock', time.monotonic)
        self._checkpointer_factory = seams.get('checkpointer_factory', Checkpointer)
        self._wal_trigger_bytes = seams.get('wal_trigger_bytes', WAL_TRIGGER_BYTES)
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
            checkpointer: Checkpointer | None = None
            try:
                checkpointer = self._checkpointer_factory(
                    database.connection,
                    file['path'],
                    interval_secs=self._interval_secs,
                    logger=self._logger,
                    log_extra=file['log_extra'],
                )
                due = file['first_due']
                trigger = self._wal_trigger_bytes
                while not self._stop.wait(
                    min(WAL_TRIGGER_CHECK_SECS, max(0.0, due - self._clock()))
                ):
                    size = wal_file_bytes(file['path'])
                    scheduled = self._clock() >= due
                    if not scheduled and size <= trigger:
                        continue
                    try:
                        attempt = checkpointer.run_once()
                    finally:
                        with file['status']['lock']:
                            file['status']['stalled'] = bool(checkpointer.crossed)
                    trigger = self._wal_trigger_bytes + (
                        0 if attempt.reuse == 'reused' else attempt.wal_bytes
                    )
                    if scheduled:
                        elapsed = max(0.0, self._clock() - due)
                        due += (int(elapsed // self._interval_secs) + 1) * self._interval_secs
            except BaseException as error:
                close_resources(
                    (database,) if checkpointer is None else (checkpointer, database),
                    primary_error=error,
                )
                raise
            else:
                close_resources((checkpointer, database))

    def register(
        self, path: Path, identity: str, schema: Schema, *, log_extra: Mapping[str, str]
    ) -> CheckpointView:
        """Start one timer for an initialized file, reusing resolved-path views.

        The first scheduled attempt waits one interval plus this registration's
        `interval * index / count` offset. Later registrations do not change
        existing timers.

        Args:
            path: Initialized database path; registration uses its resolved
                path.
            identity: Stable identity matching the database's stored identity.
                A mismatch fails asynchronously through `on_failure`.
            log_extra: Owner identity fields attached to transition messages.
            schema: Database schema used when the checkpoint thread opens it.

        Returns:
            The checkpoint view for the resolved path. Re-registration returns
            the existing view and ignores the new `identity` and `schema`.

        Raises:
            CheckpointHostStoppedError: This host is stopping or closed.
            OSError: The database path cannot be resolved.
            RuntimeError: The checkpoint thread cannot be started.
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
                log_extra=dict(log_extra),
                status={'lock': threading.Lock(), 'stalled': False},
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
        """Interrupt timer waits and join threads after their connections close.

        Skip the calling thread if invoked from a checkpoint thread; that
        thread finishes its own cleanup after this method returns.
        """
        self.request_stop()
        for thread in self.threads():
            if thread is not threading.current_thread():
                thread.join()
