"""Arbitrate SQLite write transactions on one source database file."""

from __future__ import annotations

import math
import os
import sqlite3
import threading
import time
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, Protocol, TypeAlias, TypedDict, Unpack

from sg_event_daemon._platform import (
    acquire_flock_exclusive,
    release_flock,
    secure_open_regular_file,
    try_flock_exclusive,
)
from sg_event_daemon._sqlite import WAL_IN_USE_LIMIT_BYTES, DatabaseError, wal_file_bytes

if TYPE_CHECKING:
    import logging
    from collections.abc import Callable, Generator, Mapping
    from contextlib import AbstractContextManager
    from pathlib import Path

    from sg_event_daemon._health import ConditionFlags

WriteLockLevel: TypeAlias = Literal['pause', 'process-local', 'file', 'SQLite']
WritePhase: TypeAlias = Literal['write-lock-wait', 'write-hold']
_WritePolicy: TypeAlias = Literal['wait', 'one-attempt', 'operator']

OPERATOR_WRITE_BUDGET_SECS = 2.0
WRITE_HOLD_DEADLINE_SECS = 5.0
_FILE_WAITS: dict[str, _FileWait] = {}
_FILE_WAIT_CHANGED = threading.Condition()
_PAUSE_RETRY_SECS = 0.05
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


@dataclass(frozen=True)
class _FileLease:
    """Identify the acquired open file description and its release ownership."""

    fd: int
    close_on_release: bool


@dataclass
class _FileWait:
    """Keep one outstanding kernel acquisition alive independently of entry references."""

    lease: _FileLease
    authorized: bool = False
    acquired: bool = False
    abandoned: bool = False
    error: OSError | None = None

    def result(self) -> _FileLease | None:
        """Return the acquired lease or raise the helper's acquisition failure.

        Returns:
            The acquired lease, or `None` while acquisition remains pending.

        Raises:
            OSError: The helper could not acquire the file lock.
        """
        if self.error is not None:
            raise self.error
        return self.lease if self.acquired else None


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
    slice_secs: float


class _TransactionOptions(TypedDict, total=False):
    """Type optional per-transaction budget, stop, timing, and watch arguments."""

    budget_secs: float
    slice_secs: float
    stop: threading.Event
    timing: WriteTiming
    watch: _PhaseWatcher


class _WriteLockSeams(TypedDict, total=False):
    """Type optional timing and WAL sampling replacements for tests."""

    clock: Callable[[], float]
    sleep: Callable[[float], None]
    wal_bytes: Callable[[], int]
    wal_limit_bytes: int


class SourceWriteLock:
    """Serialize write transactions on one source database across threads and processes.

    Every instance for the same lock-file path in one process shares one
    process-local `threading.Lock` and one open lock-file descriptor, so
    threads queue in-process before one of them waits for the cross-process
    `flock` on a helper with an independently opened descriptor. A transaction
    passes through two disjoint phases: acquisition, bounded by one budget
    across the process-local lock, the file lock, and SQLite's `BEGIN
    IMMEDIATE` wait; then the protected write, from `BEGIN IMMEDIATE` to
    `COMMIT` or rollback. A thread holds at most one transaction per lock path.
    A WAL past the growth bound expires acquisition before either lock is
    taken.
    Call `close()` when the owner stops; the shared descriptor closes with the
    last instance for its path.
    """

    def __init__(
        self,
        lock_path: Path,
        *,
        database_path: Path,
        timeout_secs: float,
        wait_phase_grace_secs: float,
        **seams: Unpack[_WriteLockSeams],
    ) -> None:
        """Open or share this process's entry for `lock_path`.

        Args:
            lock_path: The source database's `.db.write-lock` file beneath a
                secure state directory; created owner-only when absent.
            database_path: Source database whose WAL size gates each attempt.
            timeout_secs: Default acquisition budget, normally
                `write_lock_timeout_secs()`.
            wait_phase_grace_secs: Allowance added to the budget for the
                `write-lock-wait` phase deadline, normally the shared watchdog
                grace.
            **seams: Optional clock, sleep, WAL-size reader, and byte bound.

        Raises:
            ValueError: A duration is not finite and positive.
            SecurityError: The lock file or its parent directory is unsafe.
            OSError: Resolving or opening the lock-file path fails.
        """
        _validate_positive(timeout_secs, 'write lock timeout')
        _validate_positive(wait_phase_grace_secs, 'write lock wait grace')
        self.path = lock_path
        self.database_path = database_path
        self._key = str(lock_path.absolute())
        self._timeout_secs = timeout_secs
        self.wait_phase_grace_secs = wait_phase_grace_secs
        self.clock = seams.get('clock', time.monotonic)
        self.sleep = seams.get('sleep', time.sleep)
        self._wal_bytes = seams.get('wal_bytes', lambda: wal_file_bytes(database_path))
        self.wal_limit_bytes = seams.get('wal_limit_bytes', WAL_IN_USE_LIMIT_BYTES)
        self._entry: _ProcessEntry | None = _share_entry(self._key, lock_path)

    def _remaining(self, level: WriteLockLevel, attempt: _Attempt) -> float:
        """Return the budget left, refusing a stop request or a spent budget."""
        now = self.clock()
        waited = now - attempt.timing.started_at
        if attempt.stop is not None and attempt.stop.is_set():
            raise WriteLockStoppedError(
                f'write lock wait for {self.path} stopped after {waited:.3f}s'
            )
        remaining = attempt.deadline_at - now
        if remaining <= 0:
            raise WriteLockTimeoutError(
                f'write lock wait expired at the {level} level for {self.path} '
                f'after {waited:.3f}s',
                level,
            )
        return remaining

    def _acquire_file(self, attempt: _Attempt) -> _FileLease:
        """Try once, then claim a kernel helper's lease within the remaining budget."""
        self._remaining('file', attempt)
        if try_flock_exclusive(attempt.entry.fd):
            return _FileLease(attempt.entry.fd, close_on_release=False)
        slice_secs = (
            min(attempt.slice_secs, _STOP_POLL_SECS)
            if attempt.stop is not None
            else attempt.slice_secs
        )
        with _FILE_WAIT_CHANGED:
            # An abandoned job still owns its descriptor and registry slot until
            # the kernel returns. Retries wait here instead of queuing more jobs.
            while self._key in _FILE_WAITS:
                remaining = self._remaining('file', attempt)
                _FILE_WAIT_CHANGED.wait(min(remaining, slice_secs))
            self._remaining('file', attempt)
            fd, _created = secure_open_regular_file(self.path)
            job = _FileWait(_FileLease(fd, close_on_release=True))
            _FILE_WAITS[self._key] = job
            try:
                threading.Thread(
                    target=_wait_for_file,
                    args=(self._key, job),
                    name='source-write-lock',
                    daemon=True,
                ).start()
                job.authorized = True
                while True:
                    remaining = self._remaining('file', attempt)
                    lease = job.result()
                    if lease is not None:
                        del _FILE_WAITS[self._key]
                        _FILE_WAIT_CHANGED.notify_all()
                        return lease
                    _FILE_WAIT_CHANGED.wait(min(remaining, slice_secs))
            except BaseException:
                job.abandoned = True
                if not job.authorized or job.acquired or job.error is not None:
                    try:
                        if job.acquired:
                            _release_file(job.lease)
                        elif not job.authorized:
                            os.close(fd)
                    finally:
                        del _FILE_WAITS[self._key]
                        _FILE_WAIT_CHANGED.notify_all()
                raise

    def _acquire_local(self, attempt: _Attempt) -> None:
        """Wait for the process-local lock while the budget lasts and no stop is requested."""
        while True:
            remaining = self._remaining('process-local', attempt)
            wait_secs = (
                remaining
                if attempt.stop is None
                else min(remaining, _STOP_POLL_SECS, attempt.slice_secs)
            )
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
        while True:
            remaining = self._remaining('SQLite', attempt)
            failure: BaseException | None = None
            try:
                remaining_ms = max(1, math.floor(min(attempt.slice_secs, remaining) * 1000))
                try:
                    connection.execute(f'PRAGMA busy_timeout={remaining_ms}')
                except sqlite3.Error as exc:
                    raise DatabaseError(str(exc)) from exc
                self._remaining('SQLite', attempt)
                try:
                    connection.execute('BEGIN IMMEDIATE')
                except sqlite3.Error as exc:
                    if not _is_busy(exc):
                        raise DatabaseError(str(exc)) from exc
                else:
                    attempt.timing.begun_at = self.clock()
                    return
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
            timing.finished_at = self.clock()

    def _acquire(self, attempt: _Attempt, connection: sqlite3.Connection) -> _FileLease:
        """Take the process-local lock, the file lock, and then `BEGIN IMMEDIATE`."""
        self._remaining('process-local', attempt)
        if self._wal_bytes() > self.wal_limit_bytes:
            raise WriteLockTimeoutError(
                f'writes to {self.database_path} paused at the WAL growth bound', 'pause'
            )
        self._acquire_local(attempt)
        try:
            attempt.timing.local_acquired_at = self.clock()
            lease = self._acquire_file(attempt)
            try:
                attempt.timing.file_acquired_at = self.clock()
                self._begin(attempt, connection)
            except BaseException as exc:
                try:
                    if connection.in_transaction:
                        self._rollback(connection, exc, attempt.timing)
                finally:
                    _release_file(lease)
                raise
        except BaseException:
            attempt.entry.holder = None
            attempt.entry.lock.release()
            raise
        else:
            return lease

    def _settle(
        self,
        connection: sqlite3.Connection,
        attempt: _Attempt,
        lease: _FileLease,
        primary: BaseException | None,
    ) -> None:
        """Roll back an open transaction before releasing both locks."""
        try:
            if connection.in_transaction:
                if primary is None:
                    try:
                        connection.rollback()
                    finally:
                        attempt.timing.finished_at = self.clock()
                else:
                    self._rollback(connection, primary, attempt.timing)
        finally:
            _release(attempt.entry, lease)

    def close(self) -> None:
        """Release this instance's share of the process entry; repeated calls are harmless.

        Raises:
            OSError: Closing the last shared lock-file descriptor fails.
        """
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
                the phase boundaries; `slice_secs` caps each uninterruptible
                SQLite wait, in seconds.

        Yields:
            The connection while its immediate transaction is active.

        Raises:
            WriteLockTimeoutError: The WAL is over its growth bound, or the
                acquisition budget ran out at a named level.
            WriteLockStoppedError: `stop` was set during acquisition.
            DatabaseError: The instance is closed, this thread already holds a
                transaction on this lock path, or SQLite failed.
            ValueError: The acquisition budget or slice is not finite and
                positive.
            SecurityError: The lock file or its parent directory is unsafe.
            OSError: WAL inspection or lock-file acquisition or release fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
        """
        entry = self._entry
        if entry is None:
            raise DatabaseError(f'write lock is closed: {self.path}')
        if entry.holder == threading.get_ident():
            raise DatabaseError(f'nested write transaction on {self.path}')
        budget_secs = options.get('budget_secs', self._timeout_secs)
        _validate_positive(budget_secs, 'write lock budget')
        slice_secs = options.get('slice_secs', 1.0)
        _validate_positive(slice_secs, 'write lock slice')
        watch = options.get('watch', _no_watch)
        timing = options.get('timing')
        if timing is None:
            timing = WriteTiming()
        timing.started_at = self.clock()
        attempt = _Attempt(
            entry, timing, timing.started_at + budget_secs, options.get('stop'), slice_secs
        )
        lease: _FileLease | None = None
        try:
            with watch(
                'write-lock-wait',
                write_lock_wait_budget_secs(attempt.deadline_at, self.wait_phase_grace_secs),
            ):
                lease = self._acquire(attempt, connection)
        except BaseException as exc:
            if lease is not None:
                # Acquisition succeeded but leaving the watch phase failed.
                self._settle(connection, attempt, lease, exc)
            raise
        primary: BaseException | None = None
        try:
            with watch('write-hold', timing.begun_at + WRITE_HOLD_DEADLINE_SECS):
                try:
                    yield connection
                    connection.commit()
                    timing.finished_at = self.clock()
                except sqlite3.Error as exc:
                    error = DatabaseError(str(exc))
                    self._rollback(connection, error, timing)
                    raise error from exc
                except BaseException as exc:
                    self._rollback(connection, exc, timing)
                    raise
        except BaseException as exc:
            # Cleanup below must not replace the error in flight.
            primary = exc
            raise
        finally:
            self._settle(connection, attempt, lease, primary)


@dataclass
class _WaitState:
    """Remember a component's first expired attempt until its next committed write."""

    started_at: float | None = None


def _is_busy(error: sqlite3.Error) -> bool:
    """Return whether SQLite reported its busy result code."""
    return error.sqlite_errorcode & _SQLITE_PRIMARY_RESULT_MASK == _SQLITE_BUSY


def _no_watch(_phase: WritePhase, _deadline_at: float, /) -> AbstractContextManager[None]:
    """Publish nothing for callers without a watchdog."""
    return nullcontext()


@dataclass(frozen=True)
class WriteContext:
    """Bind one writer's lock, watchdog, stop event, and acquisition policy.

    The state facade owns `lock` and closes it with its database handles.
    Operator copies share those resources but keep independent warning state.
    """

    lock: SourceWriteLock
    watch: _PhaseWatcher = _no_watch
    stop: threading.Event | None = None
    policy: _WritePolicy = 'wait'
    conditions: ConditionFlags | None = None
    logger: logging.Logger | None = None
    log_extra: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    slice_secs: float = 1.0
    _wait: _WaitState = field(default_factory=_WaitState, init=False, compare=False)

    def _expired(self, error: WriteLockTimeoutError, timing: WriteTiming) -> None:
        """Publish one warning and retain its start across source poll retries."""
        if self._wait.started_at is not None:
            return
        self._wait.started_at = timing.started_at
        if self.conditions is not None:
            self.conditions.set('write-lock-stalled')
        if self.logger is not None:
            if error.level == 'pause':
                self.logger.warning(
                    'writes to %s paused: the WAL file is larger than %s bytes; '
                    'a connection outside the daemon may hold a read open or checkpoint the file',
                    self.lock.database_path,
                    self.lock.wal_limit_bytes,
                    extra=self.log_extra,
                )
                return
            self.logger.warning(
                'writes to %s waited %.1fs at the %s level: a connection outside the daemon, '
                'a queue of daemon writers, or writers paused at the WAL growth bound',
                self.lock.path,
                self.lock.clock() - timing.started_at,
                error.level,
                extra=self.log_extra,
            )

    def _pause_retry(self) -> None:
        """Space immediate growth expiries without delaying a stop request."""
        deadline_at = write_lock_wait_budget_secs(
            self.lock.clock() + _PAUSE_RETRY_SECS,
            self.lock.wait_phase_grace_secs,
        )
        with self.watch('write-lock-wait', deadline_at):
            if self.stop is None:
                self.lock.sleep(_PAUSE_RETRY_SECS)
            elif self.stop.wait(_PAUSE_RETRY_SECS):
                raise WriteLockStoppedError('WAL growth pause stopped')

    def _resumed(self) -> None:
        """Clear a warning only after this context has committed a write."""
        started_at = self._wait.started_at
        if started_at is None:
            return
        if self.conditions is not None:
            self.conditions.clear('write-lock-stalled')
        if self.logger is not None:
            self.logger.info(
                'writes to %s resumed after waiting %.1fs',
                self.lock.path,
                self.lock.clock() - started_at,
                extra=self.log_extra,
            )
        self._wait.started_at = None

    def for_operator(self) -> WriteContext:
        """Return one bounded operator attempt with fresh warning state."""
        return replace(self, policy='operator')

    @contextmanager
    def transaction(self, connection: sqlite3.Connection) -> Generator[sqlite3.Connection]:
        """Acquire according to policy, then run exactly one protected write.

        Commit on normal exit and roll back when the body raises. Only
        acquisition expiry is retried: failures in the body or commit always
        propagate.

        Args:
            connection: An open connection to the database guarded by `lock`,
                opened with `isolation_level=None` on the calling thread.

        Yields:
            The connection while its immediate transaction is active.

        Raises:
            SourceDatabaseBusyError: An operator's acquisition attempt expired.
            WriteLockTimeoutError: A one-attempt acquisition expired.
            WriteLockStoppedError: `stop` was set during acquisition.
            DatabaseError: The lock is closed, this thread already holds a
                transaction on its path, or SQLite failed.
            ValueError: The acquisition budget or slice is not finite and
                positive.
            SecurityError: The lock file or its parent directory is unsafe.
            OSError: WAL inspection or lock-file acquisition or release fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
        """
        options: _TransactionOptions = {'watch': self.watch, 'slice_secs': self.slice_secs}
        if self.stop is not None:
            options['stop'] = self.stop
        if self.policy == 'operator':
            options['budget_secs'] = OPERATOR_WRITE_BUDGET_SECS
        with ExitStack() as stack:
            while True:
                timing = WriteTiming()
                options['timing'] = timing
                try:
                    active = stack.enter_context(self.lock.transaction(connection, **options))
                except WriteLockTimeoutError as error:
                    if self.policy == 'operator':
                        raise SourceDatabaseBusyError(str(error)) from error
                    self._expired(error, timing)
                    if self.policy == 'one-attempt':
                        raise
                    if error.level == 'pause':
                        self._pause_retry()
                else:
                    break
            yield active
        self._resumed()


def _release_entry(key: str, entry: _ProcessEntry) -> None:
    """Drop one reference and close the shared descriptor with the last one."""
    with _REGISTRY_LOCK:
        entry.references -= 1
        if entry.references == 0:
            del _REGISTRY[key]
            os.close(entry.fd)


def _release_file(lease: _FileLease) -> None:
    """Unlock the acquired description, closing it only when the lease owns it."""
    try:
        release_flock(lease.fd)
    finally:
        if lease.close_on_release:
            os.close(lease.fd)


def _release(entry: _ProcessEntry, lease: _FileLease) -> None:
    """Release the file lock and then the process-local lock."""
    try:
        _release_file(lease)
    finally:
        entry.holder = None
        entry.lock.release()


def _restore_busy_timeout(
    connection: sqlite3.Connection,
    configured: int,
    primary: BaseException | None,
) -> None:
    """Restore `busy_timeout`, promoting a database failure over retryable expiry."""
    try:
        connection.execute(f'PRAGMA busy_timeout={configured}')
    except sqlite3.Error as error:
        if primary is None or isinstance(primary, WriteLockTimeoutError):
            failure = DatabaseError(str(error))
            if primary is not None:
                failure.add_note(f'write lock acquisition expiry: {primary!r}')
            raise failure from error
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


def _wait_for_file(key: str, job: _FileWait) -> None:
    """Publish one acquired lease or release it after the caller has abandoned it."""
    with _FILE_WAIT_CHANGED:
        # Thread.start can launch the OS thread and then raise. The caller
        # retains FD ownership until it authorizes this target under the same
        # condition, so a cancelled startup never touches a closed or reused FD.
        if not job.authorized:
            return
    try:
        acquire_flock_exclusive(job.lease.fd)
    except OSError as error:
        with _FILE_WAIT_CHANGED:
            os.close(job.lease.fd)
            job.error = error
            if job.abandoned:
                del _FILE_WAITS[key]
            _FILE_WAIT_CHANGED.notify_all()
    else:
        with _FILE_WAIT_CHANGED:
            if job.abandoned:
                try:
                    _release_file(job.lease)
                finally:
                    del _FILE_WAITS[key]
                    _FILE_WAIT_CHANGED.notify_all()
            else:
                job.acquired = True
                _FILE_WAIT_CHANGED.notify_all()


def raise_owner_write_failure(error: Exception | None) -> None:
    """Keep cooperative stops on the component thread that owns the write.

    Retryable operator-busy errors remain command replies. The runtime, rather
    than a mailbox or transport, decides whether a stopped wait is cooperative.

    Args:
        error: Captured request failure, if any.

    Raises:
        WriteLockStoppedError: The owning component's wait was interrupted.
    """
    if isinstance(error, WriteLockStoppedError):
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
