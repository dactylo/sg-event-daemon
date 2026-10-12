"""Arbitrate SQLite write transactions on one source database file."""

from __future__ import annotations

import math
import os
import sqlite3
import threading
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, Protocol, TypeAlias, TypedDict, Unpack

from sg_event_daemon._platform import (
    release_flock,
    secure_open_regular_file,
    try_flock_exclusive,
)
from sg_event_daemon._sqlite import DatabaseError

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from contextlib import AbstractContextManager
    from pathlib import Path

WriteLockLevel: TypeAlias = Literal['process-local', 'file', 'SQLite']
WritePhase: TypeAlias = Literal['write-lock-wait', 'write-hold']

OPERATOR_WRITE_BUDGET_SECS = 2.0
WRITE_HOLD_DEADLINE_SECS = 5.0
_FILE_LOCK_RETRY_SECS = 0.001
_REGISTRY: dict[str, _ProcessEntry] = {}
_REGISTRY_LOCK = threading.Lock()
_SQLITE_BUSY = 5
_SQLITE_PRIMARY_RESULT_MASK = 0xFF
_STOP_POLL_SECS = 0.05


class _PhaseWatcher(Protocol):
    """Publish one write phase on the calling component's watchdog."""

    def __call__(self, phase: WritePhase, deadline_at: float, /) -> AbstractContextManager[None]:
        """Return a phase-reporting context with an absolute monotonic deadline."""
        ...


class SourceDatabaseBusyError(DatabaseError):
    """Report an operator or supervisor write that exhausted its acquisition budget."""


class WriteLockStoppedError(DatabaseError):
    """Report an acquisition abandoned because the caller's stop event was set."""


class WriteLockTimeoutError(DatabaseError):
    """Report an acquisition budget spent at one lock level.

    Attributes:
        level: Lock level whose wait exhausted the budget.
    """

    def __init__(self, message: str, level: WriteLockLevel) -> None:
        """Store the failure text and the level whose wait expired."""
        super().__init__(message)
        self.level: WriteLockLevel = level


@dataclass
class WriteTiming:
    """Record the monotonic phase boundaries of one write transaction.

    A field stays `nan` until the transaction reaches that boundary.

    Attributes:
        started_at: Entry to `transaction()`, where acquisition begins.
        local_acquired_at: Acquisition of the process-local lock.
        file_acquired_at: Acquisition of the cross-process file lock.
        begun_at: Successful return of `BEGIN IMMEDIATE`, which ends
            acquisition and begins the protected write.
        finished_at: Return of `COMMIT` or rollback, before both locks are
            released.
    """

    started_at: float = math.nan
    local_acquired_at: float = math.nan
    file_acquired_at: float = math.nan
    begun_at: float = math.nan
    finished_at: float = math.nan


@dataclass
class _ProcessEntry:
    """Share one process-local lock and lock-file descriptor for one path."""

    lock: threading.Lock
    fd: int
    references: int
    holder: int | None = None


@dataclass(frozen=True)
class _Attempt:
    """Carry one transaction's acquisition state between helper steps."""

    entry: _ProcessEntry
    timing: WriteTiming
    deadline_at: float
    stop: threading.Event | None


class _TransactionOptions(TypedDict, total=False):
    """Type optional per-transaction budget, stop, timing, and watch arguments."""

    budget_secs: float
    stop: threading.Event
    timing: WriteTiming
    watch: _PhaseWatcher


class _WriteLockSeams(TypedDict, total=False):
    """Type optional monotonic clock and sleep replacements for tests."""

    clock: Callable[[], float]
    sleep: Callable[[float], None]


class SourceWriteLock:
    """Serialize write transactions on one source database across threads and processes.

    Every instance for the same lock-file path in one process shares one
    process-local `threading.Lock` and one open lock-file descriptor, so
    threads queue in-process before one of them polls the cross-process
    `flock`. A transaction passes through two disjoint phases: acquisition,
    bounded by one budget across the process-local lock, the file lock, and
    SQLite's `BEGIN IMMEDIATE` wait; then the protected write, from `BEGIN
    IMMEDIATE` to `COMMIT` or rollback. A thread holds at most one transaction
    per lock path.
    Call `close()` when the owner stops; the shared descriptor closes with the
    last instance for its path.
    """

    def __init__(
        self,
        lock_path: Path,
        *,
        timeout_secs: float,
        wait_phase_grace_secs: float,
        **seams: Unpack[_WriteLockSeams],
    ) -> None:
        """Open or share this process's entry for `lock_path`.

        Args:
            lock_path: The source database's `.db.write-lock` file beneath a
                secure state directory; created owner-only when absent.
            timeout_secs: Default acquisition budget, normally
                `write_lock_timeout_secs()`.
            wait_phase_grace_secs: Allowance added to the budget for the
                `write-lock-wait` phase deadline, normally one health interval.
            **seams: Optional monotonic clock and sleep replacements.

        Raises:
            ValueError: A duration is not finite and positive.
            SecurityError: The lock file or its parent directory is unsafe.
        """
        _validate_positive(timeout_secs, 'write lock timeout')
        _validate_positive(wait_phase_grace_secs, 'write lock wait grace')
        self._path = lock_path
        self._key = str(lock_path.absolute())
        self._timeout_secs = timeout_secs
        self._wait_phase_grace_secs = wait_phase_grace_secs
        self._clock = seams.get('clock', time.monotonic)
        self._sleep = seams.get('sleep', time.sleep)
        self._entry: _ProcessEntry | None = _share_entry(self._key, lock_path)

    def _remaining(self, level: WriteLockLevel, attempt: _Attempt) -> float:
        """Return the budget left, refusing a stop request or a spent budget."""
        now = self._clock()
        waited = now - attempt.timing.started_at
        if attempt.stop is not None and attempt.stop.is_set():
            raise WriteLockStoppedError(
                f'write lock wait for {self._path} stopped after {waited:.3f}s'
            )
        remaining = attempt.deadline_at - now
        if remaining <= 0:
            raise WriteLockTimeoutError(
                f'write lock wait expired at the {level} level for {self._path} '
                f'after {waited:.3f}s',
                level,
            )
        return remaining

    def _acquire_file(self, attempt: _Attempt) -> None:
        """Poll the cross-process lock while the budget lasts and no stop is requested."""
        while True:
            remaining = self._remaining('file', attempt)
            if try_flock_exclusive(attempt.entry.fd):
                return
            self._sleep(min(_FILE_LOCK_RETRY_SECS, remaining))

    def _acquire_local(self, attempt: _Attempt) -> None:
        """Wait for the process-local lock while the budget lasts and no stop is requested."""
        while True:
            remaining = self._remaining('process-local', attempt)
            wait_secs = remaining if attempt.stop is None else min(remaining, _STOP_POLL_SECS)
            if attempt.entry.lock.acquire(timeout=wait_secs):
                break
        attempt.entry.holder = threading.get_ident()
        try:
            self._remaining('process-local', attempt)
        except BaseException:
            attempt.entry.holder = None
            attempt.entry.lock.release()
            raise

    def _begin(self, attempt: _Attempt, connection: sqlite3.Connection) -> None:
        """Begin the immediate transaction with SQLite's wait capped at the remaining budget."""
        try:
            (configured,) = connection.execute('PRAGMA busy_timeout').fetchone()
        except sqlite3.Error as exc:
            raise DatabaseError(str(exc)) from exc
        failure: BaseException | None = None
        try:
            remaining = self._remaining('SQLite', attempt)
            remaining_ms = max(1, math.floor(remaining * 1000))
            try:
                connection.execute(f'PRAGMA busy_timeout={remaining_ms}')
            except sqlite3.Error as exc:
                raise DatabaseError(str(exc)) from exc
            self._remaining('SQLite', attempt)
            try:
                connection.execute('BEGIN IMMEDIATE')
                attempt.timing.begun_at = self._clock()
            except sqlite3.Error as exc:
                if _is_busy(exc):
                    waited = self._clock() - attempt.timing.started_at
                    raise WriteLockTimeoutError(
                        f'write lock wait expired at the SQLite level for {self._path} '
                        f'after {waited:.3f}s',
                        'SQLite',
                    ) from exc
                raise DatabaseError(str(exc)) from exc
        except BaseException as exc:
            failure = exc
            raise
        finally:
            _restore_busy_timeout(connection, int(configured), failure)

    def _rollback(
        self, connection: sqlite3.Connection, primary: BaseException, timing: WriteTiming
    ) -> None:
        """Sample completion after rollback, preserving the primary failure."""
        try:
            _rollback(connection, primary)
        finally:
            timing.finished_at = self._clock()

    def _acquire(self, attempt: _Attempt, connection: sqlite3.Connection) -> None:
        """Take the process-local lock, the file lock, and then `BEGIN IMMEDIATE`."""
        self._acquire_local(attempt)
        try:
            attempt.timing.local_acquired_at = self._clock()
            self._acquire_file(attempt)
            try:
                attempt.timing.file_acquired_at = self._clock()
                self._begin(attempt, connection)
            except BaseException as exc:
                if connection.in_transaction:
                    self._rollback(connection, exc, attempt.timing)
                release_flock(attempt.entry.fd)
                raise
        except BaseException:
            attempt.entry.holder = None
            attempt.entry.lock.release()
            raise

    def _settle(
        self, connection: sqlite3.Connection, attempt: _Attempt, primary: BaseException | None
    ) -> None:
        """Roll back an open transaction before releasing both locks."""
        try:
            if connection.in_transaction:
                if primary is None:
                    try:
                        connection.rollback()
                    finally:
                        attempt.timing.finished_at = self._clock()
                else:
                    self._rollback(connection, primary, attempt.timing)
        finally:
            _release(attempt.entry)

    def close(self) -> None:
        """Release this instance's share of the process entry; repeated calls are harmless."""
        entry = self._entry
        if entry is None:
            return
        self._entry = None
        _release_entry(self._key, entry)

    @contextmanager
    def transaction(
        self,
        connection: sqlite3.Connection,
        **options: Unpack[_TransactionOptions],
    ) -> Generator[sqlite3.Connection]:
        """Run one arbitrated immediate transaction on `connection`.

        Args:
            connection: An open connection to the database this lock guards,
                opened with `isolation_level=None` on the calling thread.
            **options: `budget_secs` overrides the default acquisition budget;
                `stop` abandons acquisition once set; `watch` publishes the
                `write-lock-wait` and `write-hold` phases; `timing` receives
                the phase boundaries.

        Yields:
            The connection while its immediate transaction is active.

        Raises:
            WriteLockTimeoutError: The acquisition budget ran out at a named
                level.
            WriteLockStoppedError: `stop` was set during acquisition.
            DatabaseError: The instance is closed, this thread already holds a
                transaction on this lock path, or SQLite failed.
            ValueError: `budget_secs` is not finite and positive.
        """
        entry = self._entry
        if entry is None:
            raise DatabaseError(f'write lock is closed: {self._path}')
        if entry.holder == threading.get_ident():
            raise DatabaseError(f'nested write transaction on {self._path}')
        budget_secs = options.get('budget_secs', self._timeout_secs)
        _validate_positive(budget_secs, 'write lock budget')
        watch = options.get('watch', _no_watch)
        timing = options.get('timing')
        if timing is None:
            timing = WriteTiming()
        timing.started_at = self._clock()
        attempt = _Attempt(entry, timing, timing.started_at + budget_secs, options.get('stop'))
        acquired = False
        try:
            with watch(
                'write-lock-wait',
                write_lock_wait_budget_secs(attempt.deadline_at, self._wait_phase_grace_secs),
            ):
                self._acquire(attempt, connection)
                acquired = True
        except BaseException as exc:
            if acquired:
                # Acquisition succeeded but leaving the watch phase failed.
                self._settle(connection, attempt, exc)
            raise
        primary: BaseException | None = None
        try:
            with watch('write-hold', timing.begun_at + WRITE_HOLD_DEADLINE_SECS):
                try:
                    yield connection
                    connection.commit()
                    timing.finished_at = self._clock()
                except sqlite3.Error as exc:
                    error = DatabaseError(str(exc))
                    self._rollback(connection, error, timing)
                    raise error from exc
                except BaseException as exc:
                    self._rollback(connection, exc, timing)
                    raise
        except BaseException as exc:
            # Cleanup below must not replace the error in flight (review P1-S1).
            primary = exc
            raise
        finally:
            self._settle(connection, attempt, primary)


def _is_busy(error: sqlite3.Error) -> bool:
    """Return whether SQLite reported its busy result code."""
    return error.sqlite_errorcode & _SQLITE_PRIMARY_RESULT_MASK == _SQLITE_BUSY


def _no_watch(_phase: WritePhase, _deadline_at: float, /) -> AbstractContextManager[None]:
    """Publish nothing for callers without a watchdog."""
    return nullcontext()


@dataclass(frozen=True)
class WriteContext:
    """Bind one writer's lock, watchdog, stop event, and acquisition policy.

    The state facade takes ownership of `lock` and closes it with its database
    handles. Operator copies share that ownership and only override policy.
    """

    lock: SourceWriteLock
    watch: _PhaseWatcher = _no_watch
    stop: threading.Event | None = None
    budget_secs: float | None = None
    busy_is_command_failure: bool = False

    def for_operator(self) -> WriteContext:
        """Return a bounded retryable operator policy sharing this writer's resources."""
        return replace(self, budget_secs=OPERATOR_WRITE_BUDGET_SECS, busy_is_command_failure=True)

    @contextmanager
    def transaction(self, connection: sqlite3.Connection) -> Generator[sqlite3.Connection]:
        """Run one protected write, translating operator acquisition timeouts only.

        Commit on normal exit and roll back when the body raises.

        Args:
            connection: An open connection to the database guarded by `lock`,
                opened with `isolation_level=None` on the calling thread.

        Yields:
            The connection while its immediate transaction is active.

        Raises:
            SourceDatabaseBusyError: Acquisition timed out while
                `busy_is_command_failure` is true.
            WriteLockTimeoutError: Acquisition timed out while
                `busy_is_command_failure` is false.
            WriteLockStoppedError: `stop` was set during acquisition.
            DatabaseError: The lock is closed, this thread already holds a
                transaction on its path, or SQLite failed.
            ValueError: The acquisition budget is not finite and positive.
        """
        options: _TransactionOptions = {'watch': self.watch}
        if self.stop is not None:
            options['stop'] = self.stop
        if self.budget_secs is not None:
            options['budget_secs'] = self.budget_secs
        try:
            with self.lock.transaction(connection, **options) as active:
                yield active
        except WriteLockTimeoutError as error:
            if self.busy_is_command_failure:
                raise SourceDatabaseBusyError(str(error)) from error
            raise


def _release(entry: _ProcessEntry) -> None:
    """Release the file lock and then the process-local lock."""
    try:
        release_flock(entry.fd)
    finally:
        entry.holder = None
        entry.lock.release()


def _release_entry(key: str, entry: _ProcessEntry) -> None:
    """Drop one reference and close the shared descriptor with the last one."""
    with _REGISTRY_LOCK:
        entry.references -= 1
        if entry.references == 0:
            del _REGISTRY[key]
            os.close(entry.fd)


def _restore_busy_timeout(
    connection: sqlite3.Connection,
    configured: int,
    primary: BaseException | None,
) -> None:
    """Restore `busy_timeout`, noting a failure on the error in flight instead of replacing it."""
    try:
        connection.execute(f'PRAGMA busy_timeout={configured}')
    except sqlite3.Error as error:
        if primary is None:
            raise DatabaseError(str(error)) from error
        primary.add_note(f'busy_timeout restore failure: {error!r}')


def _rollback(connection: sqlite3.Connection, primary: BaseException) -> None:
    """Roll back after a failure, recording a rollback failure on the primary error."""
    try:
        connection.rollback()
    except sqlite3.Error as error:
        primary.add_note(f'rollback failure: {error!r}')


def _share_entry(key: str, lock_path: Path) -> _ProcessEntry:
    """Return this process's entry for `key`, opening its descriptor on first use."""
    with _REGISTRY_LOCK:
        entry = _REGISTRY.get(key)
        if entry is None:
            fd, _created = secure_open_regular_file(lock_path)
            entry = _ProcessEntry(lock=threading.Lock(), fd=fd, references=0)
            _REGISTRY[key] = entry
        entry.references += 1
        return entry


def _validate_positive(value: float, name: str) -> None:
    """Reject a non-finite or nonpositive duration."""
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f'{name} must be finite and positive')


def raise_owner_write_failure(error: Exception | None) -> None:
    """Keep acquisition failures on the component thread that owns the write.

    Retryable operator-busy errors remain command replies. The runtime, rather
    than a mailbox or transport, decides whether a stopped wait is cooperative.

    Args:
        error: Captured request failure, if any.

    Raises:
        WriteLockTimeoutError: The owning component exhausted its write budget.
        WriteLockStoppedError: The owning component's wait was interrupted.
    """
    if isinstance(error, (WriteLockTimeoutError, WriteLockStoppedError)):
        raise error


def write_lock_timeout_secs(
    health_interval_secs: float,
    supervisor_poll_interval_secs: float,
) -> float:
    """Return the acquisition budget that outlasts detection of an overdue holder.

    The budget is the hold deadline plus twice the slower of the two intervals
    plus one second, 10 s with the defaults. A holder past
    `WRITE_HOLD_DEADLINE_SECS` is reported within one health interval and acted
    on by the next supervisor poll, so a waiter that began acquiring after the
    holder's protected write began does not time out first.

    Args:
        health_interval_secs: Worker health publication interval.
        supervisor_poll_interval_secs: Supervisor poll interval.

    Returns:
        The acquisition budget in seconds.

    Raises:
        ValueError: An interval is not finite and positive.
    """
    _validate_positive(health_interval_secs, 'health interval')
    _validate_positive(supervisor_poll_interval_secs, 'supervisor poll interval')
    slower = max(health_interval_secs, supervisor_poll_interval_secs)
    return WRITE_HOLD_DEADLINE_SECS + 2 * slower + 1.0


def write_lock_wait_budget_secs(acquisition_budget_secs: float, grace_secs: float) -> float:
    """Add the watchdog grace to a write-lock acquisition budget or deadline.

    Args:
        acquisition_budget_secs: Acquisition duration or absolute monotonic
            deadline.
        grace_secs: Additional wait-phase allowance in seconds.
    """
    return acquisition_budget_secs + grace_secs
