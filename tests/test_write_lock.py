from __future__ import annotations

import logging
import math
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Literal, cast

import pytest

from sg_event_daemon import _platform, _write_lock
from sg_event_daemon._health import ConditionFlags
from sg_event_daemon._platform import release_flock, secure_open_regular_file, try_flock_exclusive
from sg_event_daemon._sqlite import DatabaseError, DatabaseHandle, Schema, open_database
from sg_event_daemon._supervisor import Supervisor
from sg_event_daemon._watchdog import shutdown_slice_secs
from sg_event_daemon._worker import _SourceComponent
from sg_event_daemon._write_lock import (
    OPERATOR_WRITE_BUDGET_SECS,
    WRITE_HOLD_DEADLINE_SECS,
    SourceWriteLock,
    WriteLockStoppedError,
    WriteLockTimeoutError,
    WritePhase,
    WriteTiming,
    write_lock_timeout_secs,
)
from tests._storage_support import wait_for_future_signal
from tests._subprocess_support import subprocess_env
from tests.test_worker import wait_test_component

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator, Mapping
    from pathlib import Path
    from typing import Unpack

    from _typeshed import SupportsLenAndGetItem

    from tests._stall_policy import StallDetectors

_SCHEMA = Schema(
    version=1,
    statements=('CREATE TABLE entries (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)',),
)
_HOLDER_CODE = """
import sys
from pathlib import Path
from sg_event_daemon._sqlite import Schema, open_database
from sg_event_daemon._write_lock import SourceWriteLock

schema = Schema(
    version=1,
    statements=('CREATE TABLE entries (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)',),
)
handle = open_database(Path(sys.argv[1]), 'write-lock-test', schema, synchronous='NORMAL')
lock = SourceWriteLock(
    Path(sys.argv[2]), database_path=Path(sys.argv[1]),
    timeout_secs=10.0, wait_phase_grace_secs=1.0, clock=lambda: 0.0,
)
if len(sys.argv) > 3:
    from sg_event_daemon import _write_lock
    blocking = _write_lock.acquire_flock_exclusive
    def announce(fd):
        print('WAITING', flush=True)
        blocking(fd)
    _write_lock.acquire_flock_exclusive = announce
with lock.transaction(handle.connection):
    handle.connection.execute('INSERT INTO entries VALUES (99, 99)')
    print('HOLDING', flush=True)
    sys.stdin.read(1)
"""


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _LocalWait:
    def __init__(self, lock: threading.Lock, wait: Callable[[float], None]) -> None:
        self.lock = lock
        self.wait = wait
        self.slices: list[float] = []

    def acquire(self, *, timeout: float) -> bool:
        if self.lock.acquire(blocking=False):
            return True
        self.slices.append(timeout)
        self.wait(timeout)
        return False

    def release(self) -> None:
        self.lock.release()


class _PhaseLog:
    def __init__(self) -> None:
        self.events: list[tuple[str, WritePhase, float]] = []
        self.current: WritePhase | None = None
        self._guard = threading.Lock()

    @contextmanager
    def __call__(self, phase: WritePhase, deadline_at: float, /) -> Generator[None]:
        with self._guard:
            self.events.append(('enter', phase, deadline_at))
            self.current = phase
        try:
            yield
        finally:
            with self._guard:
                self.events.append(('exit', phase, deadline_at))
                self.current = None


def _advance_file_wait(monkeypatch: pytest.MonkeyPatch, clock: _Clock) -> list[float]:
    slices: list[float] = []

    def wait(timeout: float | None = None) -> bool:
        assert timeout is not None
        slices.append(timeout)
        clock.now += timeout
        return False

    monkeypatch.setattr(_write_lock._FILE_WAIT_CHANGED, 'wait', wait)
    return slices


def _assert_closed(fd: int) -> None:
    with pytest.raises(OSError, match='Bad file descriptor'):
        os.fstat(fd)


def _assert_remaining_sqlite_budget(
    statements: list[tuple[str, float]], timing: WriteTiming, budget_secs: float
) -> None:
    read, sqlite_started_at = statements[0]
    configured, _ = statements[1]
    assert read == 'PRAGMA busy_timeout'
    assert configured.startswith('PRAGMA busy_timeout=')
    remaining_ms = int(configured.removeprefix('PRAGMA busy_timeout='))
    # Observe the actual SQLite setting: elapsed time alone permits a reset
    # budget to hide within the busy handler's scheduling tolerance.
    assert 0 < remaining_ms <= 1000
    assert remaining_ms < budget_secs * 1000
    assert remaining_ms <= (timing.started_at + budget_secs - sqlite_started_at) * 1000


def _connect(path: Path) -> DatabaseHandle:
    return open_database(path, 'write-lock-test', _SCHEMA, synchronous='NORMAL')


def _deny_busy_timeout_restore(
    action: int, name: str | None, value: str | None, *_: str | None
) -> int:
    # The connection's configured 5000 ms is set again only by the restore.
    if action == sqlite3.SQLITE_PRAGMA and name == 'busy_timeout' and value == '5000':
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _deny_rollback(action: int, operation: str | None, *_: str | None) -> int:
    if action == sqlite3.SQLITE_TRANSACTION and operation == 'ROLLBACK':
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def _expire_file_wait(reason: str, clock: _Clock, stop: threading.Event) -> None:
    if reason == 'deadline':
        clock.now = 2
    else:
        stop.set()


def _fail_inside(lock: SourceWriteLock, handle: DatabaseHandle, error: BaseException) -> None:
    with lock.transaction(handle.connection):
        handle.connection.execute('INSERT INTO entries VALUES (7, 7)')
        raise error


def _interrupt_started_helper(
    monkeypatch: pytest.MonkeyPatch,
    entered: threading.Event,
    threads: list[threading.Thread],
) -> None:
    start = threading.Thread.start

    def interrupt_start(thread: threading.Thread) -> None:
        threads.append(thread)
        if len(threads) == 1:
            started: object = vars(thread)['_started']
            assert isinstance(started, threading.Event)
            wait = started.wait

            def interrupt_wait(timeout: float | None = None) -> bool:
                wait(timeout)
                assert entered.wait()
                raise KeyboardInterrupt('interrupted after OS launch')

            monkeypatch.setattr(started, 'wait', interrupt_wait)
        start(thread)

    monkeypatch.setattr(threading.Thread, 'start', interrupt_start)


def _observe_blocking_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[threading.Event, list[int], list[int], list[int]]:
    started = threading.Event()
    descriptors: list[int] = []
    released: list[int] = []
    closed: list[int] = []
    close = os.close
    blocking = _platform.acquire_flock_exclusive

    def acquire(fd: int) -> None:
        descriptors.append(fd)
        started.set()
        blocking(fd)

    def unlock(fd: int) -> None:
        released.append(fd)
        release_flock(fd)

    def close_descriptor(fd: int) -> None:
        if fd in descriptors:
            closed.append(fd)
        close(fd)

    monkeypatch.setattr(_write_lock, 'acquire_flock_exclusive', acquire)
    monkeypatch.setattr(_write_lock, 'release_flock', unlock)
    monkeypatch.setattr(os, 'close', close_descriptor)
    return started, descriptors, released, closed


def _read_child_notice(process: subprocess.Popen[str], expected: str) -> None:
    stdout = process.stdout
    assert stdout is not None
    assert os.read(stdout.fileno(), 4_096).decode().strip() == expected


def _rollback_then_interrupt(
    _lock: SourceWriteLock,
    connection: sqlite3.Connection,
    _primary: BaseException,
    _timing: WriteTiming,
) -> None:
    connection.rollback()
    raise RuntimeError('rollback interrupted')


def _rows(path: Path) -> list[tuple[int, int]]:
    with closing(sqlite3.connect(path)) as connection, connection:
        return connection.execute('SELECT id, value FROM entries ORDER BY id').fetchall()


def _wait_until(predicate: Callable[[], bool], check: Callable[[], object] = lambda: None) -> None:
    while not predicate():
        check()
        time.sleep(0.005)


@pytest.fixture(autouse=True)
def _default_lock_clock_is_stationary(
    monkeypatch: pytest.MonkeyPatch, stall_detectors: StallDetectors
) -> None:
    # Only deadline tests advance lock time. Host scheduling cannot spend an
    # unrelated acquisition's budget; SQLite and kernel locks remain real.
    stall_detectors.enable('phase')
    monkeypatch.setattr(
        _write_lock, 'time', SimpleNamespace(monotonic=lambda: 0.0, sleep=time.sleep)
    )


@pytest.fixture(autouse=True)
def _registry_is_empty_after_each_test() -> Iterator[None]:
    yield
    assert _write_lock._REGISTRY == {}
    _wait_until(lambda: not _write_lock._FILE_WAITS)


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / 'source.db'
    open_database(path, 'write-lock-test', _SCHEMA, synchronous='NORMAL').close()
    return path


def _lock_path(database: Path) -> Path:
    return database.with_name('source.db.write-lock')


def _write_one(
    lock: SourceWriteLock,
    handle: DatabaseHandle,
    row: tuple[int, int] = (1, 1),
    **options: Unpack[_write_lock._TransactionOptions],
) -> None:
    with lock.transaction(handle.connection, **options):
        handle.connection.execute('INSERT INTO entries VALUES (?, ?)', row)


@pytest.mark.parametrize('helper', [False, True])
def test_kernel_releases_both_locks_when_a_holder_process_dies(
    database: Path, monkeypatch: pytest.MonkeyPatch, helper: bool
) -> None:
    clock = _Clock()
    _advance_file_wait(monkeypatch, clock)
    lock = SourceWriteLock(
        _lock_path(database),
        database_path=database,
        timeout_secs=0.3,
        wait_phase_grace_secs=1.0,
        clock=clock,
    )
    handle = _connect(database)
    holder, _ = secure_open_regular_file(_lock_path(database))
    if helper:
        assert try_flock_exclusive(holder)
    process = subprocess.Popen(
        [
            sys.executable,
            '-c',
            _HOLDER_CODE,
            str(database),
            str(_lock_path(database)),
            *(['helper'] if helper else []),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=subprocess_env(),
    )
    try:
        if helper:
            _read_child_notice(process, 'WAITING')
            release_flock(holder)
        _read_child_notice(process, 'HOLDING')
        with pytest.raises(WriteLockTimeoutError, match='file level'):
            _write_one(lock, handle)
        process.kill()
        assert process.wait() == -signal.SIGKILL
        _wait_until(lambda: not _write_lock._FILE_WAITS)
        _write_one(lock, handle)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        os.close(holder)
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


@pytest.mark.parametrize('policy', ['wait', 'one-attempt', 'operator'])
def test_sqlite_restore_failure_after_expiry_is_not_retried_or_translated(
    database: Path, policy: _write_lock._WritePolicy, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_write_lock, 'OPERATOR_WRITE_BUDGET_SECS', OPERATOR_WRITE_BUDGET_SECS)
    clock = _Clock()
    phases = _PhaseLog()
    settings: list[str] = []
    bodies: list[str] = []

    def authorize(action: int, name: str | None, value: str | None, *_: str | None) -> int:
        if action == sqlite3.SQLITE_PRAGMA and name == 'busy_timeout' and value is not None:
            settings.append(value)
            if value == '1000':
                clock.now = 3.0
            elif value == '5000':
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=1,
                wait_phase_grace_secs=1,
                clock=clock,
            )
        ) as lock,
        _connect(database) as handle,
    ):
        handle.connection.set_authorizer(authorize)
        writes = _write_lock.WriteContext(lock, policy=policy, watch=phases)
        with (
            pytest.raises(DatabaseError, match='not authorized') as raised,
            writes.transaction(handle.connection),
        ):
            bodies.append('entered')
        assert type(raised.value) is DatabaseError
        assert isinstance(raised.value.__cause__, sqlite3.DatabaseError)
        assert any(
            'WriteLockTimeoutError' in note and 'SQLite level' in note
            for note in raised.value.__notes__
        )
        assert settings == ['1000', '5000']
        assert bodies == []
        assert [event[1] for event in phases.events if event[0] == 'enter'] == ['write-lock-wait']
        assert not handle.connection.in_transaction
        handle.connection.set_authorizer(None)


def test_abandoned_helper_does_not_hold_process_shutdown(database: Path) -> None:
    code = (
        _HOLDER_CODE[: _HOLDER_CODE.index('with lock.transaction')]
        + """
from sg_event_daemon import _write_lock
from sg_event_daemon._write_lock import WriteLockTimeoutError
import threading
clock = [0.0]
lock.clock = lambda: clock[0]
started = threading.Event()
blocking = _write_lock.acquire_flock_exclusive
condition_wait = _write_lock._FILE_WAIT_CHANGED.wait

def acquire(fd):
    with _write_lock._FILE_WAIT_CHANGED:
        started.set()
        _write_lock._FILE_WAIT_CHANGED.notify_all()
    blocking(fd)

def wait(timeout=None):
    if not started.is_set():
        return condition_wait()
    clock[0] += timeout
    return False

_write_lock.acquire_flock_exclusive = acquire
_write_lock._FILE_WAIT_CHANGED.wait = wait
try:
    with lock.transaction(handle.connection, budget_secs=0.05):
        raise AssertionError('unexpected acquisition')
except WriteLockTimeoutError:
    assert len(_write_lock._FILE_WAITS) == 1
    lock.close()
    handle.close()
"""
    )
    with _external_holder(database):
        process = subprocess.Popen(
            [sys.executable, '-c', code, str(database), str(_lock_path(database))],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=subprocess_env(),
        )
        try:
            output, error = process.communicate()
            assert process.returncode == 0, (output, error)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


@pytest.mark.parametrize('commits', [False, True])
def test_sqlite_wait_is_capped_by_the_remaining_budget_and_busy_timeout_is_restored(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
    commits: bool,
) -> None:
    clock = _Clock()
    lock = SourceWriteLock(
        _lock_path(database),
        database_path=database,
        timeout_secs=2,
        wait_phase_grace_secs=1,
        clock=clock,
    )
    rogue = sqlite3.connect(database, isolation_level=None)
    handle = _connect(database)
    timing = WriteTiming()
    statements: list[tuple[str, float]] = []
    acquire_file = SourceWriteLock._acquire_file

    def delayed_acquire(
        active: SourceWriteLock, attempt: _write_lock._Attempt
    ) -> _write_lock._FileLease:
        lease = acquire_file(active, attempt)
        clock.now += 1.25
        return lease

    def trace(statement: str) -> None:
        statements.append((statement, clock()))
        if statement == 'PRAGMA busy_timeout=750' and commits:
            rogue.commit()
        elif statement == 'PRAGMA busy_timeout=5000':
            clock.now = 2.0

    monkeypatch.setattr(SourceWriteLock, '_acquire_file', delayed_acquire)
    handle.connection.set_trace_callback(trace)
    try:
        rogue.execute('BEGIN IMMEDIATE')
        if commits:
            _write_one(lock, handle, timing=timing)
            assert timing.begun_at == 1.25
            assert _rows(database) == [(1, 1)]
        else:
            with pytest.raises(WriteLockTimeoutError, match='SQLite level') as caught:
                _write_one(lock, handle, timing=timing)
            assert caught.value.level == 'SQLite'
            assert str(_lock_path(database)) in str(caught.value)
        _assert_remaining_sqlite_budget(statements, timing, 2)
        assert statements[1] == ('PRAGMA busy_timeout=750', 1.25)
        assert handle.connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)
    finally:
        rogue.rollback()
        rogue.close()
        handle.close()
        lock.close()


def test_body_failure_rolls_back_and_releases_both_locks(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=1.0, wait_phase_grace_secs=1.0
    )
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)
    try:
        with pytest.raises(RuntimeError, match='boom'):
            _fail_inside(lock, handle, RuntimeError('boom'))
        assert not handle.connection.in_transaction
        assert try_flock_exclusive(other)
        release_flock(other)
        _write_one(lock, handle)
    finally:
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


def test_body_failure_survives_a_failed_rollback(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=1.0, wait_phase_grace_secs=1.0
    )
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)
    handle.connection.set_authorizer(_deny_rollback)
    try:
        with pytest.raises(RuntimeError, match='boom') as raised:
            _fail_inside(lock, handle, RuntimeError('boom'))
        handle.connection.set_authorizer(None)
        assert any(note.startswith('rollback failure') for note in raised.value.__notes__)
        assert try_flock_exclusive(other)
        release_flock(other)
        handle.connection.rollback()
        _write_one(lock, handle)
    finally:
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


def test_budget_spent_before_acquisition_does_not_report_a_wal_pause(database: Path) -> None:
    clock = _Clock()

    @contextmanager
    def delayed_watch(_phase: WritePhase, _deadline: float) -> Generator[None]:
        clock.now += 1
        yield

    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=1,
                wait_phase_grace_secs=1,
                clock=clock,
                wal_bytes=lambda: 0,
            )
        ) as lock,
        _connect(database) as handle,
    ):
        with (
            pytest.raises(WriteLockTimeoutError) as caught,
            lock.transaction(
                handle.connection,
                watch=delayed_watch,
            ),
        ):
            pytest.fail('expired acquisition entered the write')
        assert caught.value.level == 'process-local'


def test_budget_spent_during_busy_timeout_read_refuses_begin(database: Path) -> None:
    clock = _Clock()
    lock = SourceWriteLock(
        _lock_path(database),
        database_path=database,
        timeout_secs=1.0,
        wait_phase_grace_secs=1.0,
        clock=clock,
    )
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)
    begins: list[str] = []

    def spend_during_read(action: int, name: str | None, value: str | None, *_: str | None) -> int:
        if action == sqlite3.SQLITE_PRAGMA and name == 'busy_timeout' and value is None:
            clock.now = 2.0
        if action == sqlite3.SQLITE_TRANSACTION and name == 'BEGIN':
            begins.append(name)
        return sqlite3.SQLITE_OK

    handle.connection.set_authorizer(spend_during_read)
    try:
        with pytest.raises(WriteLockTimeoutError, match='SQLite level') as caught:
            _write_one(lock, handle)
        assert caught.value.level == 'SQLite'
        assert begins == []
        assert not handle.connection.in_transaction
        handle.connection.set_authorizer(None)
        assert handle.connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)
        assert try_flock_exclusive(other)
        release_flock(other)
    finally:
        handle.connection.set_authorizer(None)
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == []


def test_budget_spent_on_the_file_lock_names_that_level_and_frees_the_local_lock(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _advance_file_wait(monkeypatch, clock)
    lock = SourceWriteLock(
        _lock_path(database),
        database_path=database,
        timeout_secs=0.2,
        wait_phase_grace_secs=1.0,
        clock=clock,
    )
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)
    try:
        assert try_flock_exclusive(other)
        with pytest.raises(WriteLockTimeoutError, match='file level') as caught:
            _write_one(lock, handle)
        assert caught.value.level == 'file'
        release_flock(other)
        _wait_until(lambda: not _write_lock._FILE_WAITS)
        _write_one(lock, handle)
    finally:
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


def test_budget_spent_on_the_process_local_lock_names_that_level(database: Path) -> None:
    clock = _Clock()
    holder_lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=10.0, wait_phase_grace_secs=1.0
    )
    waiter_lock = SourceWriteLock(
        _lock_path(database),
        database_path=database,
        timeout_secs=0.2,
        wait_phase_grace_secs=1.0,
        clock=clock,
    )
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        handle = _connect(database)
        try:
            with holder_lock.transaction(handle.connection):
                entered.set()
                assert release.wait()
        finally:
            handle.close()

    handle = _connect(database)
    phases = _PhaseLog()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(hold)
            try:
                wait_for_future_signal(entered, future)
                assert waiter_lock._entry is not None

                def advance(seconds: float) -> None:
                    clock.now += seconds

                local = _LocalWait(waiter_lock._entry.lock, advance)
                waiter_lock._entry.lock = cast('threading.Lock', local)
                with pytest.raises(WriteLockTimeoutError, match='process-local level') as caught:
                    _write_one(waiter_lock, handle, watch=phases)
                assert caught.value.level == 'process-local'
                assert 'source.db.write-lock' in str(caught.value)
                assert local.slices == [0.2]
                assert clock.now == 0.2
                assert [phase for kind, phase, _ in phases.events if kind == 'enter'] == [
                    'write-lock-wait'
                ]
            finally:
                release.set()
            future.result()
    finally:
        handle.close()
        waiter_lock.close()
        holder_lock.close()


def test_contended_file_wait_uses_one_blocking_helper(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started, descriptors, released, closed = _observe_blocking_helper(monkeypatch)
    tries: list[int] = []

    def first_try(fd: int) -> bool:
        tries.append(fd)
        return try_flock_exclusive(fd)

    monkeypatch.setattr(_write_lock, 'try_flock_exclusive', first_try)
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=5,
                wait_phase_grace_secs=1,
            )
        ) as first,
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=5,
                wait_phase_grace_secs=1,
            )
        ) as second,
        _external_holder(database) as holder,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        active: list[int] = []
        queued = threading.Event()
        acquire_local = SourceWriteLock._acquire_local

        def signal_local(active_lock: SourceWriteLock, attempt: _write_lock._Attempt) -> None:
            if active_lock is second:
                queued.set()
            acquire_local(active_lock, attempt)

        monkeypatch.setattr(SourceWriteLock, '_acquire_local', signal_local)

        def write(lock: SourceWriteLock, row: int) -> None:
            with _connect(database) as handle, lock.transaction(handle.connection):
                assert not active
                active.append(row)
                handle.connection.execute('INSERT INTO entries VALUES (?, ?)', (row, row))
                active.remove(row)

        futures = [pool.submit(write, first, 1)]
        try:
            wait_for_future_signal(started, futures[0])
            futures.append(pool.submit(write, second, 2))
            wait_for_future_signal(queued, futures[1])
            assert len(tries) == len(descriptors) == 1
            assert descriptors[0] != tries[0]
        finally:
            release_flock(holder)
        for future in futures:
            future.result()
        assert closed.count(descriptors[0]) == released.count(descriptors[0]) == 1
        _assert_closed(descriptors[0])
    assert _rows(database) == [(1, 1), (2, 2)]


def test_entry_reopen_cannot_reassign_abandoned_helper(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _advance_file_wait(monkeypatch, clock)
    started, descriptors, released, closed = _observe_blocking_helper(monkeypatch)
    with _external_holder(database) as holder, _connect(database) as handle:
        lock = SourceWriteLock(
            _lock_path(database),
            database_path=database,
            timeout_secs=0.04,
            clock=clock,
            wait_phase_grace_secs=1,
        )
        assert lock._entry is not None
        old_fd = lock._entry.fd
        try:
            with pytest.raises(WriteLockTimeoutError):
                _write_one(lock, handle)
            assert started.wait()
        finally:
            lock.close()
        unrelated, _ = secure_open_regular_file(database.with_name('unrelated.lock'))
        if unrelated != old_fd:
            os.dup2(unrelated, old_fd)
            os.close(unrelated)
        identity = os.fstat(old_fd)
        assert try_flock_exclusive(old_fd)
        try:
            with closing(
                SourceWriteLock(
                    _lock_path(database),
                    database_path=database,
                    timeout_secs=0.04,
                    clock=clock,
                    wait_phase_grace_secs=1,
                )
            ) as reopened:
                with pytest.raises(WriteLockTimeoutError):
                    _write_one(reopened, handle)
                assert len(descriptors) == 1
                release_flock(holder)
                _wait_until(
                    lambda: str(_lock_path(database).absolute()) not in _write_lock._FILE_WAITS
                )
                assert closed == released == descriptors
                _assert_closed(descriptors[0])
                assert os.fstat(old_fd) == identity
                contender, _ = secure_open_regular_file(database.with_name('unrelated.lock'))
                try:
                    assert not try_flock_exclusive(contender)
                finally:
                    os.close(contender)
                _write_one(reopened, handle)
        finally:
            os.close(old_fd)
    assert _rows(database) == [(1, 1)]


def test_failed_busy_timeout_restore_after_begin_rolls_back_and_releases(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=1.0, wait_phase_grace_secs=1.0
    )
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)
    handle.connection.set_authorizer(_deny_busy_timeout_restore)
    try:
        with pytest.raises(DatabaseError, match='not authorized'):
            _write_one(lock, handle)
        handle.connection.set_authorizer(None)
        assert not handle.connection.in_transaction
        assert try_flock_exclusive(other)
        release_flock(other)
        _write_one(lock, handle)
    finally:
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


def test_failure_leaving_the_wait_phase_rolls_back_and_releases(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=1.0, wait_phase_grace_secs=1.0
    )
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)

    @contextmanager
    def failing_exit(phase: WritePhase, _deadline_at: float, /) -> Generator[None]:
        yield
        if phase == 'write-lock-wait':
            raise RuntimeError('watch exit failed')

    try:
        with pytest.raises(RuntimeError, match='watch exit failed'):
            _write_one(lock, handle, watch=failing_exit)
        assert not handle.connection.in_transaction
        assert try_flock_exclusive(other)
        release_flock(other)
        _write_one(lock, handle)
    finally:
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


@pytest.mark.parametrize(
    ('health', 'poll'),
    [(0.0, 1.0), (1.0, -1.0), (math.nan, 1.0), (1.0, math.inf)],
)
def test_timeout_formula_rejects_invalid_intervals(health: float, poll: float) -> None:
    with pytest.raises(ValueError, match='finite and positive'):
        write_lock_timeout_secs(health, poll)


@pytest.mark.parametrize('level', ['process-local', 'file'])
def test_stop_event_abandons_acquisition_promptly(
    database: Path, monkeypatch: pytest.MonkeyPatch, level: str
) -> None:
    clock = _Clock()
    stop = threading.Event()
    slices: list[float] = []

    def request_stop(seconds: float) -> None:
        slices.append(seconds)
        clock.now += seconds
        stop.set()

    def wait(timeout: float | None = None) -> bool:
        assert timeout is not None
        request_stop(timeout)
        return False

    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=10,
                wait_phase_grace_secs=1,
                clock=clock,
            )
        ) as lock,
        _connect(database) as handle,
        _external_holder(database),
    ):
        assert lock._entry is not None
        entry = lock._entry
        if level == 'process-local':
            held = threading.Lock()
            held.acquire()
            entry.lock = cast('threading.Lock', _LocalWait(held, request_stop))
        else:
            monkeypatch.setattr(_write_lock._FILE_WAIT_CHANGED, 'wait', wait)
        with pytest.raises(WriteLockStoppedError, match='stopped'):
            _write_one(lock, handle, stop=stop)
        assert slices == [0.05]
        assert clock.now == 0.05
        assert not handle.connection.in_transaction


@pytest.mark.parametrize('failure', ['none', 'wait-exit', 'hold-enter', 'body'])
def test_timing_brackets_sqlite_despite_delayed_cleanup(database: Path, failure: str) -> None:
    clock = _Clock()
    clock.now = 10.0
    lock = SourceWriteLock(
        _lock_path(database),
        database_path=database,
        timeout_secs=1.0,
        wait_phase_grace_secs=1.0,
        clock=clock,
    )
    handle = _connect(database)
    timing = WriteTiming()
    deadlines: list[float] = []

    def authorize(action: int, name: str | None, value: str | None, *_: str | None) -> int:
        if action == sqlite3.SQLITE_PRAGMA and name == 'busy_timeout' and value == '5000':
            clock.now += 2.0
        if action == sqlite3.SQLITE_TRANSACTION and name in {'COMMIT', 'ROLLBACK'}:
            clock.now += 3.0
        return sqlite3.SQLITE_OK

    @contextmanager
    def watch(phase: WritePhase, deadline_at: float, /) -> Generator[None]:
        if phase == 'write-hold':
            deadlines.append(deadline_at)
            if failure == 'hold-enter':
                raise RuntimeError('injected failure')
        yield
        if phase == 'write-lock-wait' and failure == 'wait-exit':
            raise RuntimeError('injected failure')
        if phase == 'write-hold':
            clock.now += 7.0

    handle.connection.set_authorizer(authorize)
    try:
        if failure == 'none':
            with lock.transaction(handle.connection, timing=timing, watch=watch):
                pass
        else:
            with (
                pytest.raises(RuntimeError, match='injected failure'),
                lock.transaction(handle.connection, timing=timing, watch=watch),
            ):
                raise RuntimeError('injected failure')
        assert timing.begun_at == 10.0
        assert timing.finished_at == 15.0
        assert deadlines == ([] if failure == 'wait-exit' else [15.0])
        assert not handle.connection.in_transaction
    finally:
        handle.close()
        lock.close()


def test_helper_start_failure_before_launch_closes_unassigned_descriptor(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: list[int] = []
    start = threading.Thread.start

    def open_file(path: Path) -> tuple[int, bool]:
        fd, created = secure_open_regular_file(path)
        opened.append(fd)
        return fd, created

    def fail_start(_thread: threading.Thread) -> None:
        raise RuntimeError('cannot launch helper')

    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=1,
                wait_phase_grace_secs=1,
            )
        ) as lock,
        _connect(database) as handle,
        _external_holder(database) as holder,
    ):
        monkeypatch.setattr(_write_lock, 'secure_open_regular_file', open_file)
        monkeypatch.setattr(threading.Thread, 'start', fail_start)
        with pytest.raises(RuntimeError, match='cannot launch helper'):
            _write_one(lock, handle)
        assert not _write_lock._FILE_WAITS
        assert len(opened) == 1
        _assert_closed(opened[0])
        assert not handle.connection.in_transaction
        monkeypatch.setattr(threading.Thread, 'start', start)
        release_flock(holder)
        _write_one(lock, handle)
    assert _rows(database) == [(1, 1)]


def test_instances_for_one_path_share_one_entry_and_close_with_the_last(database: Path) -> None:
    first = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=1.0, wait_phase_grace_secs=1.0
    )
    second = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=1.0, wait_phase_grace_secs=1.0
    )
    handle = _connect(database)
    try:
        assert len(_write_lock._REGISTRY) == 1
        first.close()
        first.close()
        assert len(_write_lock._REGISTRY) == 1
        _write_one(second, handle)
        second.close()
        assert _write_lock._REGISTRY == {}
        with pytest.raises(DatabaseError, match='closed'):
            _write_one(second, handle, (2, 2))
    finally:
        handle.close()
        first.close()
        second.close()


def test_interrupted_helper_start_revokes_descriptor_access(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    _advance_file_wait(monkeypatch, clock)
    entered = threading.Event()
    proceed = threading.Event()
    threads: list[threading.Thread] = []
    opened: list[int] = []
    _started, acquired, _released, _closed = _observe_blocking_helper(monkeypatch)
    wait_for_file = _write_lock._wait_for_file

    def open_file(path: Path) -> tuple[int, bool]:
        fd, created = secure_open_regular_file(path)
        opened.append(fd)
        return fd, created

    def delayed_wait(key: str, job: _write_lock._FileWait) -> None:
        if job.lease.fd == opened[0]:
            entered.set()
            assert proceed.wait()
        wait_for_file(key, job)

    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=0.04,
                clock=clock,
                wait_phase_grace_secs=1,
            )
        ) as lock,
        _connect(database) as handle,
        _external_holder(database) as holder,
    ):
        monkeypatch.setattr(_write_lock, 'secure_open_regular_file', open_file)
        monkeypatch.setattr(_write_lock, '_wait_for_file', delayed_wait)
        _interrupt_started_helper(monkeypatch, entered, threads)
        try:
            with pytest.raises(KeyboardInterrupt, match='interrupted after OS launch'):
                _write_one(lock, handle)
            assert threads[0].is_alive()
            assert not handle.connection.in_transaction
            assert not _write_lock._FILE_WAITS
            _assert_closed(opened[0])
            with _hold_reused_descriptor(
                database.with_name('startup-reuse.lock'), opened[0]
            ) as identity:
                for _ in range(3):
                    with pytest.raises(WriteLockTimeoutError):
                        _write_one(lock, handle)
                assert len(threads) == len(opened) == 2
                assert _started.wait()
                assert acquired == [opened[1]]
                assert len(_write_lock._FILE_WAITS) == 1
                proceed.set()
                threads[0].join()
                assert not threads[0].is_alive()
                assert acquired == [opened[1]]
                assert os.fstat(opened[0]) == identity
                assert len(_write_lock._FILE_WAITS) == 1
        finally:
            proceed.set()
            release_flock(holder)
            for thread in threads:
                thread.join()
                assert not thread.is_alive()
        assert not _write_lock._FILE_WAITS
        _assert_closed(opened[1])
        assert try_flock_exclusive(holder)


@pytest.mark.parametrize('observed', [False, True])
def test_wait_policy_repeats_attempts_until_the_lock_frees(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    observed: bool,
) -> None:
    clock = _Clock()
    flags = ConditionFlags()
    phases = _PhaseLog()
    logger = logging.getLogger('write-lock-test')
    caplog.set_level(logging.INFO, logger=logger.name)
    seen_stall: list[tuple[str, ...]] = []

    acquire_file = SourceWriteLock._acquire_file

    def acquire(lock: SourceWriteLock, attempt: _write_lock._Attempt) -> _write_lock._FileLease:
        clock.now = min(attempt.deadline_at, 2.5)
        seen_stall.append(flags.names())
        if clock.now >= attempt.deadline_at:
            raise WriteLockTimeoutError('file level', 'file')
        return acquire_file(lock, attempt)

    monkeypatch.setattr(SourceWriteLock, '_acquire_file', acquire)
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=1,
                wait_phase_grace_secs=0.1,
                clock=clock,
            )
        ) as lock,
        _connect(database) as handle,
    ):
        writes = (
            _write_lock.WriteContext(lock, watch=phases, conditions=flags, logger=logger)
            if observed
            else _write_lock.WriteContext(lock, watch=phases)
        )
        with writes.transaction(handle.connection):
            handle.connection.execute('INSERT INTO entries VALUES (1, 1)')
        assert (
            len([event for event in phases.events if event[:2] == ('enter', 'write-lock-wait')])
            == 3
        )
        assert all(math.isfinite(event[2]) for event in phases.events)
        assert flags.names() == ()
        if observed:
            assert ('write-lock-stalled',) in seen_stall
            assert caplog.messages == [
                (
                    f'writes to {_lock_path(database)} waited 1.0s at the file level: '
                    'a connection outside the daemon, a queue of daemon writers, '
                    'or writers paused at the WAL growth bound'
                ),
                f'writes to {_lock_path(database)} resumed after waiting 2.5s',
            ]
    assert _rows(database) == [(1, 1)]


def test_local_lock_won_after_a_stop_request_is_refused(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stop = threading.Event()
    remaining = SourceWriteLock._remaining
    acquire_file = SourceWriteLock._acquire_file
    reached_file: list[str] = []

    def stop_after_local_acquire(
        lock: SourceWriteLock, level: _write_lock.WriteLockLevel, attempt: _write_lock._Attempt
    ) -> float:
        if level == 'process-local' and attempt.entry.holder == threading.get_ident():
            stop.set()
        return remaining(lock, level, attempt)

    def record_file(
        lock: SourceWriteLock, attempt: _write_lock._Attempt
    ) -> _write_lock._FileLease:
        reached_file.append('file')
        return acquire_file(lock, attempt)

    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=10,
                wait_phase_grace_secs=1,
            )
        ) as lock,
        _connect(database) as handle,
    ):
        with monkeypatch.context() as patch:
            patch.setattr(SourceWriteLock, '_remaining', stop_after_local_acquire)
            patch.setattr(SourceWriteLock, '_acquire_file', record_file)
            with pytest.raises(WriteLockStoppedError, match='stopped'):
                _write_one(lock, handle, stop=stop)
        assert reached_file == []
        assert lock._entry is not None
        assert lock._entry.holder is None
        _write_one(lock, handle)
    assert _rows(database) == [(1, 1)]


def test_nested_transaction_in_one_thread_fails_at_once(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=10.0, wait_phase_grace_secs=1.0
    )
    outer = _connect(database)
    inner = _connect(database)
    try:
        with lock.transaction(outer.connection):

            def forbidden(*_args: object) -> None:
                pytest.fail('nested transaction attempted lock acquisition')

            monkeypatch.setattr(lock, '_acquire', forbidden)
            with pytest.raises(DatabaseError, match='nested write transaction'):
                _write_one(lock, inner)
    finally:
        inner.close()
        outer.close()
        lock.close()


def test_one_attempt_policy_raises_after_one_attempt_and_keeps_its_warning(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = _Clock()
    flags = ConditionFlags()
    logger = logging.getLogger('write-lock-test')
    caplog.set_level(logging.INFO, logger=logger.name)

    acquire_file = SourceWriteLock._acquire_file

    def acquire(lock: SourceWriteLock, attempt: _write_lock._Attempt) -> _write_lock._FileLease:
        clock.now = min(attempt.deadline_at, 2.5)
        if clock.now >= attempt.deadline_at:
            raise WriteLockTimeoutError('file level', 'file')
        return acquire_file(lock, attempt)

    monkeypatch.setattr(SourceWriteLock, '_acquire_file', acquire)
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=1,
                wait_phase_grace_secs=0.1,
                clock=clock,
            )
        ) as lock,
        _connect(database) as handle,
    ):
        writes = _write_lock.WriteContext(
            lock, policy='one-attempt', conditions=flags, logger=logger
        )
        for _ in range(2):
            with (
                pytest.raises(WriteLockTimeoutError, match='file level'),
                writes.transaction(handle.connection),
            ):
                pytest.fail('acquisition unexpectedly succeeded')
            assert flags.names() == ('write-lock-stalled',)
        assert len(caplog.messages) == 1
        with writes.transaction(handle.connection):
            handle.connection.execute('INSERT INTO entries VALUES (1, 1)')
        assert flags.names() == ()
        assert caplog.messages[-1].endswith('resumed after waiting 2.5s')
        assert len(caplog.messages) == 2


def test_owner_write_failure_reraises_only_a_stop() -> None:
    _write_lock.raise_owner_write_failure(WriteLockTimeoutError('expired', 'file'))
    _write_lock.raise_owner_write_failure(DatabaseError('database failed'))
    with pytest.raises(WriteLockStoppedError, match='stopped'):
        _write_lock.raise_owner_write_failure(WriteLockStoppedError('stopped'))


def test_per_call_budget_overrides_the_default(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    slices = _advance_file_wait(monkeypatch, clock)
    lock = SourceWriteLock(
        _lock_path(database),
        database_path=database,
        timeout_secs=10.0,
        wait_phase_grace_secs=1.0,
        clock=clock,
    )
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)
    try:
        assert try_flock_exclusive(other)
        with pytest.raises(WriteLockTimeoutError, match='file level'):
            _write_one(lock, handle, budget_secs=0.2)
        assert slices == [0.2]
        assert clock.now == 0.2
    finally:
        os.close(other)
        handle.close()
        lock.close()


def test_sqlite_error_in_the_body_becomes_database_error(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=1.0, wait_phase_grace_secs=1.0
    )
    handle = _connect(database)
    try:
        _write_one(lock, handle)
        with pytest.raises(DatabaseError, match='UNIQUE'):
            _write_one(lock, handle)
        assert not handle.connection.in_transaction
    finally:
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


@pytest.mark.parametrize('policy', ['wait', 'one-attempt', 'operator'])
def test_write_context_does_not_retry_body_expiry(
    database: Path, policy: _write_lock._WritePolicy
) -> None:
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=1,
                wait_phase_grace_secs=1,
            )
        ) as lock,
        _connect(database) as handle,
    ):
        writes = _write_lock.WriteContext(lock, policy=policy)
        error = WriteLockTimeoutError('body failure', 'file')

        def fail_body() -> None:
            with writes.transaction(handle.connection):
                handle.connection.execute('INSERT INTO entries VALUES (1, 1)')
                raise error

        with pytest.raises(WriteLockTimeoutError, match='body failure') as raised:
            fail_body()
        assert raised.value is error
    assert _rows(database) == []


def test_sqlite_slice_restore_failure_is_a_database_failure(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=0.2, wait_phase_grace_secs=1.0
    )
    rogue = sqlite3.connect(database, isolation_level=None)
    handle = _connect(database)
    handle.connection.set_authorizer(_deny_busy_timeout_restore)
    try:
        rogue.execute('BEGIN IMMEDIATE')
        with pytest.raises(DatabaseError, match='not authorized') as caught:
            _write_one(lock, handle)
        handle.connection.set_authorizer(None)
        assert not isinstance(caught.value, WriteLockTimeoutError)
    finally:
        rogue.rollback()
        rogue.close()
        handle.close()
        lock.close()


def test_sqlite_slices_follow_the_shutdown_deadline(database: Path) -> None:

    stop = threading.Event()
    settings: list[int] = []
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=2,
                wait_phase_grace_secs=0.1,
            )
        ) as lock,
        _connect(database) as handle,
        closing(sqlite3.connect(database, isolation_level=None)) as holder,
    ):
        holder.execute('BEGIN IMMEDIATE')

        def trace(statement: str) -> None:
            if statement.startswith('PRAGMA busy_timeout='):
                value = int(statement.partition('=')[2])
                settings.append(value)
                if value == 5000:
                    stop.set()

        handle.connection.set_trace_callback(trace)
        with (
            pytest.raises(WriteLockStoppedError),
            lock.transaction(
                handle.connection,
                stop=stop,
                slice_secs=shutdown_slice_secs(0.25),
            ),
        ):
            pytest.fail('outside write hold was ignored')
        assert settings == [62, 5000]
        assert handle.connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)


@pytest.mark.parametrize('policy', ['wait', 'one-attempt', 'operator'])
def test_attempt_over_the_growth_bound_expires_at_the_pause_level(
    database: Path,
    caplog: pytest.LogCaptureFixture,
    policy: _write_lock._WritePolicy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    sizes = [101]
    samples: list[float] = []
    sleeps: list[float] = []
    conditions = ConditionFlags()

    def wal_bytes() -> int:
        samples.append(clock.now)
        return sizes[0]

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.now += seconds
        if len(sleeps) == 3:
            sizes[0] = 100

    acquire_local = SourceWriteLock._acquire_local

    def reject_early_lock(lock: SourceWriteLock, attempt: _write_lock._Attempt) -> None:
        assert sizes[0] <= 100, 'over-bound attempt reached the process-local lock'
        acquire_local(lock, attempt)

    monkeypatch.setattr(SourceWriteLock, '_acquire_local', reject_early_lock)
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=1,
                wait_phase_grace_secs=1,
                wal_bytes=wal_bytes,
                wal_limit_bytes=100,
                clock=clock,
                sleep=sleep,
            )
        ) as lock,
        _connect(database) as handle,
        caplog.at_level(logging.INFO, logger='pause-test'),
    ):
        assert lock._entry is not None
        writes = _write_lock.WriteContext(
            lock, policy=policy, conditions=conditions, logger=logging.getLogger('pause-test')
        )
        if policy == 'wait':
            with writes.transaction(handle.connection):
                handle.connection.execute('INSERT INTO entries VALUES (1, 1)')
            assert sleeps == [0.05, 0.05, 0.05]
            assert samples == pytest.approx([0, 0.05, 0.10, 0.15])
            assert (
                len([record for record in caplog.records if record.levelno == logging.WARNING])
                == 1
            )
            assert 'paused: the WAL file is larger than 100 bytes' in caplog.text
            assert 'resumed after waiting' in caplog.text
        else:
            expected = (
                _write_lock.SourceDatabaseBusyError
                if policy == 'operator'
                else WriteLockTimeoutError
            )
            with pytest.raises(expected) as caught, writes.transaction(handle.connection):
                pytest.fail('oversized WAL allowed a write')
            if policy == 'one-attempt':
                assert isinstance(caught.value, WriteLockTimeoutError)
                assert caught.value.level == 'pause'
            assert clock.now == 0
            assert sleeps == []
            sizes[0] = 100
            with writes.transaction(handle.connection):
                handle.connection.execute('INSERT INTO entries VALUES (1, 1)')
        assert _rows(database) == [(1, 1)]
        assert not conditions.names()


def test_stop_requested_during_busy_timeout_set_refuses_begin(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=1.0, wait_phase_grace_secs=1.0
    )
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)
    stop = threading.Event()
    begins: list[str] = []

    def stop_during_set(action: int, name: str | None, value: str | None, *_: str | None) -> int:
        if (
            action == sqlite3.SQLITE_PRAGMA
            and name == 'busy_timeout'
            and value is not None
            and value != '5000'
        ):
            stop.set()
        if action == sqlite3.SQLITE_TRANSACTION and name == 'BEGIN':
            begins.append(name)
        return sqlite3.SQLITE_OK

    handle.connection.set_authorizer(stop_during_set)
    try:
        with pytest.raises(WriteLockStoppedError, match='stopped'):
            _write_one(lock, handle, stop=stop)
        assert stop.is_set()
        assert begins == []
        assert not handle.connection.in_transaction
        handle.connection.set_authorizer(None)
        assert handle.connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)
        assert try_flock_exclusive(other)
        release_flock(other)
    finally:
        handle.connection.set_authorizer(None)
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == []


@pytest.mark.parametrize('kind', ['source', 'lane'])
@pytest.mark.parametrize('interval', [0.05, 0.01])
def test_write_wait_overshoot_stays_inside_its_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: Literal['source', 'lane'],
    interval: float,
) -> None:
    component = wait_test_component(tmp_path, kind, interval)
    if isinstance(component, _SourceComponent):
        path, _socket = component._database_paths()
    else:
        path = component._database_path()
        path.parent.mkdir(mode=0o700, exist_ok=True)
    _connect(path).close()
    monkeypatch.setattr(_write_lock, 'WRITE_HOLD_DEADLINE_SECS', 0.5)
    budget = write_lock_timeout_secs(interval, interval)
    context = component._write_context()
    # A single attempt exercises the lane's actual lock and watch wiring without
    # entering its intentionally unbounded retry policy.
    samples: list[tuple[float, float]] = []
    clock = _Clock()
    context.lock.clock = clock
    monkeypatch.setattr(component._watchdog, '_clock', clock)

    class _LateBusyConnection(sqlite3.Connection):
        def execute(
            self,
            sql: str,
            parameters: SupportsLenAndGetItem[object] | Mapping[str, object] = (),
            /,
        ) -> sqlite3.Cursor:
            if sql != 'BEGIN IMMEDIATE':
                return super().execute(sql, parameters)
            try:
                return super().execute(sql, parameters)
            except sqlite3.OperationalError:
                clock.now = budget + 0.08
                sample = component._watchdog.sample()
                assert sample.phase == 'write-lock-wait'
                assert sample.deadline_at is not None
                samples.append((clock(), sample.deadline_at))
                raise

    timing = WriteTiming()
    try:
        with (
            closing(sqlite3.connect(path, isolation_level=None)) as outside,
            closing(
                sqlite3.connect(path, isolation_level=None, factory=_LateBusyConnection)
            ) as connection,
        ):
            outside.execute('BEGIN IMMEDIATE')
            with (
                pytest.raises(WriteLockTimeoutError, match='SQLite'),
                context.lock.transaction(
                    connection,
                    watch=context.watch,
                    slice_secs=context.slice_secs,
                    timing=timing,
                ),
            ):
                pytest.fail('outside writer must retain the transaction')
            ended_at = clock()
            assert outside.in_transaction
        assert context.slice_secs == shutdown_slice_secs(
            component._config.daemon.shutdown_deadline_secs
        )
        assert len(samples) == 1
        _observed_at, deadline = samples[0]
        supervisor = Supervisor(config=component._config, config_path=tmp_path / 'unused.toml')
        assert deadline == pytest.approx(
            timing.started_at + supervisor._trusted_write_watchdog_interval('write-lock-wait')
        )
        assert ended_at < deadline
        assert not supervisor._phase_confirmed_overdue(ended_at, deadline)
    finally:
        context.lock.close()


def test_threads_in_one_process_never_overlap_protected_writes(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=10.0, wait_phase_grace_secs=2.0
    )
    guard = threading.Lock()
    active = [0]
    peak = [0]

    def writer(index: int) -> None:
        handle = _connect(database)
        try:
            for step in range(50):
                with lock.transaction(handle.connection):
                    with guard:
                        active[0] += 1
                        peak[0] = max(peak[0], active[0])
                    (current,) = handle.connection.execute(
                        'SELECT COALESCE(MAX(value), 0) FROM entries'
                    ).fetchone()
                    handle.connection.execute(
                        'INSERT INTO entries VALUES (?, ?)', (index * 1_000 + step, current + 1)
                    )
                    with guard:
                        active[0] -= 1
        finally:
            handle.close()

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(writer, index) for index in range(4)]
            for future in futures:
                future.result()
    finally:
        lock.close()
    assert peak == [1]
    assert sorted(value for _, value in _rows(database)) == list(range(1, 201))


@pytest.mark.parametrize('kind', ['source', 'lane'])
def test_stuck_write_wait_phase_becomes_overdue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: Literal['source', 'lane'],
) -> None:
    component = wait_test_component(tmp_path, kind, 0.01)
    monkeypatch.setattr(component._watchdog, '_clock', lambda: 10.0)
    if isinstance(component, _SourceComponent):
        component._database_paths()
    else:
        component._database_path().parent.mkdir(mode=0o700, exist_ok=True)
    context = component._write_context()
    budget = write_lock_timeout_secs(0.01, 0.01)
    supervisor = Supervisor(config=component._config, config_path=tmp_path / 'unused.toml')
    try:
        with context.watch('write-lock-wait', 10.0 + budget + context.lock.wait_phase_grace_secs):
            sample = component._watchdog.sample()
            assert sample.deadline_at is not None
            assert supervisor._phase_confirmed_overdue(10.0 + budget + 0.5, sample.deadline_at)
    finally:
        context.lock.close()


@contextmanager
def _external_holder(database: Path) -> Generator[int]:
    fd, _ = secure_open_regular_file(_lock_path(database))
    assert try_flock_exclusive(fd)
    try:
        yield fd
    finally:
        release_flock(fd)
        os.close(fd)


def test_timeout_formula_uses_the_slower_interval() -> None:
    assert WRITE_HOLD_DEADLINE_SECS == 5.0
    assert write_lock_timeout_secs(2.0, 1.0) == 10.0
    assert write_lock_timeout_secs(1.0, 3.0) == 12.0
    assert write_lock_timeout_secs(6.0, 1.0) == 18.0


@pytest.mark.parametrize('reason', ['deadline', 'stop'])
def test_abandoned_file_wait_releases_late_acquisition(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
) -> None:
    started, descriptors, released, closed = _observe_blocking_helper(monkeypatch)
    stop = threading.Event()
    clock = _Clock()
    wait_slices: list[float] = []

    def wait(timeout: float | None = None) -> bool:
        assert timeout is not None
        if reason == 'stop':
            # Request stop just after the last pre-wait check, then model the
            # entire requested wait. No scheduler allowance hides a second slice.
            stop.set()
        wait_slices.append(timeout)
        clock.now += timeout
        return False

    monkeypatch.setattr(_write_lock._FILE_WAIT_CHANGED, 'wait', wait)
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=0.08 if reason == 'deadline' else 5,
                wait_phase_grace_secs=1,
                clock=clock,
            )
        ) as lock,
        _connect(database) as handle,
        _external_holder(database) as holder,
    ):
        try:
            error = WriteLockTimeoutError if reason == 'deadline' else WriteLockStoppedError
            options: _write_lock._TransactionOptions = {'slice_secs': 0.02}
            if reason == 'stop':
                options['stop'] = stop
            with pytest.raises(error) as caught:
                _write_one(lock, handle, **options)
            if reason == 'stop':
                assert wait_slices == [0.02]
                assert clock.now == 0.02
            if isinstance(caught.value, WriteLockTimeoutError):
                assert caught.value.level == 'file'
            assert started.wait()
            assert not handle.connection.in_transaction
            with pytest.raises(WriteLockTimeoutError):
                _write_one(lock, handle, budget_secs=0.03)
            assert len(descriptors) == 1
        finally:
            release_flock(holder)
        _wait_until(lambda: str(_lock_path(database).absolute()) not in _write_lock._FILE_WAITS)
        assert closed == released == descriptors
        _assert_closed(descriptors[0])
        assert try_flock_exclusive(holder)
    assert _rows(database) == []


def test_transaction_reports_disjoint_wait_and_hold_phases(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=10.0, wait_phase_grace_secs=2.0
    )
    handle = _connect(database)
    phases = _PhaseLog()
    timing = WriteTiming()
    try:
        with lock.transaction(handle.connection, watch=phases, timing=timing):
            assert phases.current == 'write-hold'
            assert handle.connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)
            handle.connection.execute('INSERT INTO entries VALUES (1, 1)')
    finally:
        handle.close()
        lock.close()
    assert [(kind, phase) for kind, phase, _ in phases.events] == [
        ('enter', 'write-lock-wait'),
        ('exit', 'write-lock-wait'),
        ('enter', 'write-hold'),
        ('exit', 'write-hold'),
    ]
    assert phases.events[0][2] == pytest.approx(timing.started_at + 12.0, abs=1e-9)
    assert phases.events[2][2] == pytest.approx(
        timing.begun_at + WRITE_HOLD_DEADLINE_SECS, abs=1e-9
    )
    assert (
        timing.started_at
        <= timing.local_acquired_at
        <= timing.file_acquired_at
        <= timing.begun_at
        <= timing.finished_at
    )
    assert _rows(database) == [(1, 1)]


@pytest.mark.parametrize('reason', ['deadline', 'stop'])
@pytest.mark.parametrize('winner', ['claim', 'abandon', 'ready-abandon'])
def test_file_wait_claim_and_abandon_are_atomic(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
    winner: str,
) -> None:
    started, descriptors, released, closed = _observe_blocking_helper(monkeypatch)
    acquired = threading.Event()
    publish = threading.Event()
    clock = _Clock()
    stop = threading.Event()

    def block(fd: int) -> None:
        descriptors.append(fd)
        started.set()
        _platform.acquire_flock_exclusive(fd)
        acquired.set()
        assert publish.wait()

    monkeypatch.setattr(_write_lock, 'acquire_flock_exclusive', block)
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=1,
                wait_phase_grace_secs=1,
                clock=clock,
            )
        ) as lock,
        _external_holder(database) as holder,
        ThreadPoolExecutor(max_workers=1) as pool,
    ):
        remaining = SourceWriteLock._remaining

        def check(
            lock: SourceWriteLock, level: _write_lock.WriteLockLevel, attempt: _write_lock._Attempt
        ) -> float:
            job = _write_lock._FILE_WAITS.get(str(_lock_path(database).absolute()))
            if winner == 'ready-abandon' and level == 'file' and job is not None and job.acquired:
                _expire_file_wait(reason, clock, stop)
            return remaining(lock, level, attempt)

        monkeypatch.setattr(SourceWriteLock, '_remaining', check)

        def write() -> None:
            with _connect(database) as handle:
                with lock.transaction(handle.connection, stop=stop, slice_secs=0.01):
                    assert winner == 'claim'
                    _expire_file_wait(reason, clock, stop)
                    handle.connection.execute('INSERT INTO entries VALUES (1, 1)')
                assert not handle.connection.in_transaction

        future = pool.submit(write)
        try:
            wait_for_future_signal(started, future)
            release_flock(holder)
            wait_for_future_signal(acquired, future)
            if winner == 'abandon':
                _expire_file_wait(reason, clock, stop)
            else:
                publish.set()
            if winner == 'claim':
                future.result()
            else:
                error = WriteLockTimeoutError if reason == 'deadline' else WriteLockStoppedError
                with pytest.raises(error):
                    future.result()
        finally:
            publish.set()
            release_flock(holder)
        _wait_until(lambda: str(_lock_path(database).absolute()) not in _write_lock._FILE_WAITS)
        assert closed == released == descriptors
        _assert_closed(descriptors[0])
        assert try_flock_exclusive(holder)
    assert _rows(database) == ([(1, 1)] if winner == 'claim' else [])


def test_waiter_owning_the_local_lock_reports_wait_until_the_file_lock_is_free(
    database: Path,
) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=10.0, wait_phase_grace_secs=2.0
    )
    other, _ = secure_open_regular_file(_lock_path(database))
    phases = _PhaseLog()
    timing = WriteTiming()

    def waiter() -> None:
        handle = _connect(database)
        try:
            with lock.transaction(handle.connection, watch=phases, timing=timing):
                handle.connection.execute('INSERT INTO entries VALUES (1, 1)')
        finally:
            handle.close()

    try:
        assert try_flock_exclusive(other)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(waiter)
            _wait_until(
                lambda: not math.isnan(timing.local_acquired_at),
                lambda: future.result() if future.done() else None,
            )
            assert phases.current == 'write-lock-wait'
            assert math.isnan(timing.file_acquired_at)
            release_flock(other)
            future.result()
    finally:
        os.close(other)
        lock.close()
    assert phases.current is None
    assert ('enter', 'write-hold') in [(kind, phase) for kind, phase, _ in phases.events]
    assert _rows(database) == [(1, 1)]


def test_write_context_converts_only_operator_timeout(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_write_lock, 'OPERATOR_WRITE_BUDGET_SECS', OPERATOR_WRITE_BUDGET_SECS)
    clock = _Clock()
    lock = SourceWriteLock(
        _lock_path(database),
        database_path=database,
        timeout_secs=10,
        wait_phase_grace_secs=1,
        clock=clock,
    )
    held = threading.Lock()
    held.acquire()

    def advance(seconds: float) -> None:
        clock.now += seconds

    local = _LocalWait(held, advance)
    assert lock._entry is not None
    lock._entry.lock = cast('threading.Lock', local)
    try:
        writes = _write_lock.WriteContext(lock, policy='operator')
        with _connect(database) as handle:
            with (
                pytest.raises(_write_lock.SourceDatabaseBusyError) as raised,
                writes.transaction(handle.connection),
            ):
                pytest.fail('busy acquisition entered its body')
            assert isinstance(raised.value.__cause__, WriteLockTimeoutError)
            assert str(raised.value) == str(raised.value.__cause__)
            assert local.slices == [2.0]
    finally:
        held.release()
        lock.close()


def test_write_context_delegates_options_and_operator_copy(database: Path) -> None:
    lock = SourceWriteLock(
        _lock_path(database), database_path=database, timeout_secs=0.2, wait_phase_grace_secs=1.0
    )
    stop = threading.Event()
    phases = _PhaseLog()
    try:
        writes = _write_lock.WriteContext(lock, watch=phases, stop=stop)
        operator = writes.for_operator()
        assert operator.lock is lock
        assert operator.stop is stop
        assert operator.watch is phases
        assert operator.policy == 'operator'
        assert writes.policy == 'wait'
        assert operator._wait is not writes._wait
        with _connect(database) as handle, writes.transaction(handle.connection):
            handle.connection.execute('INSERT INTO entries VALUES (1, 1)')
        assert [event[1] for event in phases.events if event[0] == 'enter'] == [
            'write-lock-wait',
            'write-hold',
        ]
        stop.set()
        with (
            _connect(database) as handle,
            pytest.raises(WriteLockStoppedError),
            operator.transaction(handle.connection),
        ):
            pytest.fail('stopped acquisition entered its body')
    finally:
        lock.close()


@pytest.mark.parametrize(
    'failure', ['begin', 'restore', 'begin-restore', 'rollback-interrupted', 'body', 'commit']
)
def test_helper_lease_is_released_after_failed_begin(
    database: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    started, descriptors, released, closed = _observe_blocking_helper(monkeypatch)
    if failure == 'rollback-interrupted':
        monkeypatch.setattr(SourceWriteLock, '_rollback', _rollback_then_interrupt)
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=5,
                wait_phase_grace_secs=1,
            )
        ) as lock,
        _external_holder(database) as holder,
        ThreadPoolExecutor(max_workers=1) as pool,
    ):

        def write() -> None:
            with _connect(database) as handle:

                def authorize(
                    action: int, name: str | None, value: str | None, *args: str | None
                ) -> int:
                    if (
                        failure in {'begin', 'begin-restore'}
                        and action == sqlite3.SQLITE_TRANSACTION
                        and name == 'BEGIN'
                    ):
                        return sqlite3.SQLITE_DENY
                    if failure in {'restore', 'begin-restore', 'rollback-interrupted'}:
                        return _deny_busy_timeout_restore(action, name, value, *args)
                    return sqlite3.SQLITE_OK

                handle.connection.set_authorizer(authorize)
                try:
                    if failure == 'body':
                        with pytest.raises(RuntimeError, match='primary'):
                            _fail_inside(lock, handle, RuntimeError('primary'))
                    elif failure == 'rollback-interrupted':
                        with pytest.raises(RuntimeError, match='rollback interrupted'):
                            _write_one(lock, handle)
                    elif failure == 'commit':
                        _write_one(lock, handle)
                    else:
                        with pytest.raises(DatabaseError, match='not authorized') as caught:
                            _write_one(lock, handle)
                        if failure == 'begin-restore':
                            assert any(
                                'busy_timeout restore failure' in note
                                for note in caught.value.__notes__
                            )
                    assert not handle.connection.in_transaction
                finally:
                    handle.connection.set_authorizer(None)
                _write_one(lock, handle, (2, 2))

        future = pool.submit(write)
        try:
            wait_for_future_signal(started, future)
        finally:
            release_flock(holder)
        future.result()
        assert closed.count(descriptors[0]) == released.count(descriptors[0]) == 1
        _assert_closed(descriptors[0])
        assert try_flock_exclusive(holder)
    assert _rows(database) == ([(1, 1), (2, 2)] if failure == 'commit' else [(2, 2)])


@contextmanager
def _hold_reused_descriptor(path: Path, fd: int) -> Generator[os.stat_result]:
    opened, _ = secure_open_regular_file(path)
    if opened != fd:
        os.dup2(opened, fd)
        os.close(opened)
    try:
        assert try_flock_exclusive(fd)
        yield os.fstat(fd)
        contender, _ = secure_open_regular_file(path)
        try:
            assert not try_flock_exclusive(contender)
        finally:
            os.close(contender)
    finally:
        os.close(fd)


def test_writer_check_ignores_missing_wal_file(database: Path) -> None:
    assert not database.with_name(database.name + '-wal').exists()
    with (
        closing(
            SourceWriteLock(
                _lock_path(database),
                database_path=database,
                timeout_secs=1,
                wait_phase_grace_secs=1,
            )
        ) as lock,
        closing(sqlite3.connect(database, isolation_level=None)) as connection,
    ):
        assert not database.with_name(database.name + '-wal').exists()
        with lock.transaction(connection):
            connection.execute('INSERT INTO entries VALUES (1, 1)')
    assert _rows(database) == [(1, 1)]
