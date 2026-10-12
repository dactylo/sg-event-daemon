from __future__ import annotations

import math
import os
import select
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon import _write_lock
from sg_event_daemon._platform import release_flock, secure_open_regular_file, try_flock_exclusive
from sg_event_daemon._sqlite import DatabaseError, DatabaseHandle, Schema, open_database
from sg_event_daemon._write_lock import (
    WRITE_HOLD_DEADLINE_SECS,
    SourceWriteLock,
    WriteLockStoppedError,
    WriteLockTimeoutError,
    WritePhase,
    WriteTiming,
    write_lock_timeout_secs,
)
from tests._subprocess_support import subprocess_env

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator
    from pathlib import Path
    from typing import Unpack

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
lock = SourceWriteLock(Path(sys.argv[2]), timeout_secs=10.0, wait_phase_grace_secs=1.0)
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
    assert 0 < remaining_ms < budget_secs * 1000
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


def _fail_inside(lock: SourceWriteLock, handle: DatabaseHandle, error: BaseException) -> None:
    with lock.transaction(handle.connection):
        handle.connection.execute('INSERT INTO entries VALUES (7, 7)')
        raise error


def _read_child_notice(process: subprocess.Popen[str], expected: str) -> None:
    stdout = process.stdout
    assert stdout is not None
    readable, _writable, _exceptional = select.select((stdout,), (), (), 10.0)
    assert readable, f'child did not announce {expected} within 10 s'
    assert os.read(stdout.fileno(), 4_096).decode().strip() == expected


@pytest.fixture(autouse=True)
def _registry_is_empty_after_each_test() -> Iterator[None]:
    yield
    assert _write_lock._REGISTRY == {}


def _rows(path: Path) -> list[tuple[int, int]]:
    with closing(sqlite3.connect(path)) as connection, connection:
        return connection.execute('SELECT id, value FROM entries ORDER BY id').fetchall()


def _wait_until(predicate: Callable[[], bool]) -> None:
    deadline = time.monotonic() + 5.0
    while not predicate():
        assert time.monotonic() < deadline, 'condition not reached within 5 s'
        time.sleep(0.005)


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


def test_body_failure_rolls_back_and_releases_both_locks(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0)
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
    lock = SourceWriteLock(_lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0)
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


def test_budget_spent_during_busy_timeout_read_refuses_begin(database: Path) -> None:
    clock = _Clock()
    lock = SourceWriteLock(
        _lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0, clock=clock
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
) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=0.2, wait_phase_grace_secs=1.0)
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)
    try:
        assert try_flock_exclusive(other)
        with pytest.raises(WriteLockTimeoutError, match='file level') as caught:
            _write_one(lock, handle)
        assert caught.value.level == 'file'
        release_flock(other)
        _write_one(lock, handle)
    finally:
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


def test_budget_spent_on_the_process_local_lock_names_that_level(database: Path) -> None:
    holder_lock = SourceWriteLock(
        _lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=1.0
    )
    waiter_lock = SourceWriteLock(
        _lock_path(database), timeout_secs=0.2, wait_phase_grace_secs=1.0
    )
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        handle = _connect(database)
        try:
            with holder_lock.transaction(handle.connection):
                entered.set()
                assert release.wait(10.0)
        finally:
            handle.close()

    handle = _connect(database)
    phases = _PhaseLog()
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(hold)
            try:
                assert entered.wait(5.0)
                started = time.monotonic()
                with pytest.raises(WriteLockTimeoutError, match='process-local level') as caught:
                    _write_one(waiter_lock, handle, watch=phases)
                assert caught.value.level == 'process-local'
                assert 'source.db.write-lock' in str(caught.value)
                assert time.monotonic() - started < 1.0
                assert [phase for kind, phase, _ in phases.events if kind == 'enter'] == [
                    'write-lock-wait'
                ]
            finally:
                release.set()
            future.result(timeout=5.0)
    finally:
        handle.close()
        waiter_lock.close()
        holder_lock.close()


def test_failed_busy_timeout_restore_after_begin_rolls_back_and_releases(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0)
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
    lock = SourceWriteLock(_lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0)
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


def test_file_lock_won_after_the_budget_expired_is_refused(database: Path) -> None:
    clock = _Clock()
    other, _ = secure_open_regular_file(_lock_path(database))

    def oversleep(_seconds: float) -> None:
        # The retry sleep overshoots the whole budget while the holder releases its lock.
        clock.now += 2.0
        release_flock(other)

    lock = SourceWriteLock(
        _lock_path(database),
        timeout_secs=1.0,
        wait_phase_grace_secs=1.0,
        clock=clock,
        sleep=oversleep,
    )
    handle = _connect(database)
    try:
        assert try_flock_exclusive(other)
        with pytest.raises(WriteLockTimeoutError, match='file level') as caught:
            _write_one(lock, handle)
        assert caught.value.level == 'file'
        assert not handle.connection.in_transaction
        assert try_flock_exclusive(other)
        release_flock(other)
        clock.now = 10.0
        _write_one(lock, handle, (2, 2))
    finally:
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == [(2, 2)]


def test_instances_for_one_path_share_one_entry_and_close_with_the_last(database: Path) -> None:
    first = SourceWriteLock(_lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0)
    second = SourceWriteLock(_lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0)
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


def test_kernel_releases_both_locks_when_a_holder_process_dies(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=0.3, wait_phase_grace_secs=1.0)
    handle = _connect(database)
    process = subprocess.Popen(
        [sys.executable, '-c', _HOLDER_CODE, str(database), str(_lock_path(database))],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=subprocess_env(),
    )
    try:
        _read_child_notice(process, 'HOLDING')
        with pytest.raises(WriteLockTimeoutError, match='file level'):
            _write_one(lock, handle)
        process.kill()
        assert process.wait(timeout=10.0) == -signal.SIGKILL
        released_at = time.monotonic()
        _write_one(lock, handle)
        # Recorded, not asserted against the 1 ms retry interval (spec 8.1).
        print(f'release-to-commit {time.monotonic() - released_at:.6f}s')
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10.0)
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


def test_local_lock_won_after_a_stop_request_is_refused(database: Path) -> None:
    holder_lock = SourceWriteLock(
        _lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=1.0
    )
    lock = SourceWriteLock(_lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=1.0)
    stop = threading.Event()
    entered = threading.Event()

    def hold() -> None:
        holder = _connect(database)
        try:
            with holder_lock.transaction(holder.connection):
                entered.set()
                time.sleep(0.2)
                # The stop request precedes the release that lets the waiter in.
                stop.set()
        finally:
            holder.close()

    handle = _connect(database)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(hold)
            assert entered.wait(5.0)
            with pytest.raises(WriteLockStoppedError, match='stopped'):
                _write_one(lock, handle, stop=stop)
            future.result(timeout=5.0)
        _write_one(lock, handle)
    finally:
        handle.close()
        lock.close()
        holder_lock.close()
    assert _rows(database) == [(1, 1)]


def test_nested_transaction_in_one_thread_fails_at_once(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=1.0)
    outer = _connect(database)
    inner = _connect(database)
    try:
        with lock.transaction(outer.connection):
            started = time.monotonic()
            with pytest.raises(DatabaseError, match='nested write transaction'):
                _write_one(lock, inner)
            assert time.monotonic() - started < 0.1
    finally:
        inner.close()
        outer.close()
        lock.close()


def test_per_call_budget_overrides_the_default(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=1.0)
    other, _ = secure_open_regular_file(_lock_path(database))
    handle = _connect(database)
    try:
        assert try_flock_exclusive(other)
        started = time.monotonic()
        with pytest.raises(WriteLockTimeoutError, match='file level'):
            _write_one(lock, handle, budget_secs=0.2)
        assert time.monotonic() - started < 1.0
    finally:
        os.close(other)
        handle.close()
        lock.close()


def test_sqlite_error_in_the_body_becomes_database_error(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0)
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


def test_sqlite_timeout_survives_a_failed_busy_timeout_restore(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=0.2, wait_phase_grace_secs=1.0)
    rogue = sqlite3.connect(database, isolation_level=None)
    handle = _connect(database)
    handle.connection.set_authorizer(_deny_busy_timeout_restore)
    try:
        rogue.execute('BEGIN IMMEDIATE')
        with pytest.raises(WriteLockTimeoutError, match='SQLite level') as caught:
            _write_one(lock, handle)
        handle.connection.set_authorizer(None)
        assert any(
            note.startswith('busy_timeout restore failure') for note in caught.value.__notes__
        )
    finally:
        rogue.rollback()
        rogue.close()
        handle.close()
        lock.close()


def test_sqlite_wait_is_capped_by_the_remaining_budget_and_busy_timeout_is_restored(
    database: Path,
) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=2.0, wait_phase_grace_secs=1.0)
    other, _ = secure_open_regular_file(_lock_path(database))
    rogue = sqlite3.connect(database, isolation_level=None)
    handle = _connect(database)
    timing = WriteTiming()
    statements: list[tuple[str, float]] = []
    handle.connection.set_trace_callback(
        lambda statement: statements.append((statement, time.monotonic()))
    )
    timer = threading.Timer(1.0, release_flock, args=(other,))
    try:
        assert try_flock_exclusive(other)
        rogue.execute('BEGIN IMMEDIATE')
        timer.start()
        started = time.monotonic()
        with pytest.raises(WriteLockTimeoutError, match='SQLite level') as caught:
            _write_one(lock, handle, timing=timing)
        elapsed = time.monotonic() - started
        assert caught.value.level == 'SQLite'
        assert str(_lock_path(database)) in str(caught.value)
        _assert_remaining_sqlite_budget(statements, timing, 2.0)
        # About 1 s on the file lock, then only the remaining ~1 s in SQLite, never 5 s more.
        assert 1.9 <= elapsed < 4.0
        assert handle.connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)
    finally:
        timer.cancel()
        timer.join()
        rogue.rollback()
        rogue.close()
        os.close(other)
        handle.close()
        lock.close()


def test_sqlite_wait_within_the_remaining_budget_commits(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=3.0, wait_phase_grace_secs=1.0)
    other, _ = secure_open_regular_file(_lock_path(database))
    rogue = sqlite3.connect(database, isolation_level=None, check_same_thread=False)
    handle = _connect(database)
    timing = WriteTiming()
    statements: list[tuple[str, float]] = []
    handle.connection.set_trace_callback(
        lambda statement: statements.append((statement, time.monotonic()))
    )
    release_file = threading.Timer(0.5, release_flock, args=(other,))
    commit_rogue = threading.Timer(1.0, rogue.commit)
    try:
        assert try_flock_exclusive(other)
        rogue.execute('BEGIN IMMEDIATE')
        release_file.start()
        commit_rogue.start()
        _write_one(lock, handle, timing=timing)
        _assert_remaining_sqlite_budget(statements, timing, 3.0)
        assert timing.finished_at < timing.started_at + 3.0
        assert handle.connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)
    finally:
        for timer in (release_file, commit_rogue):
            timer.cancel()
            timer.join()
        if rogue.in_transaction:
            rogue.rollback()
        rogue.close()
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == [(1, 1)]


def test_stop_requested_during_busy_timeout_set_refuses_begin(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0)
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


def test_stop_requested_while_waiting_wins_over_a_file_lock_won_later(database: Path) -> None:
    other, _ = secure_open_regular_file(_lock_path(database))
    stop = threading.Event()

    def stop_then_release(_seconds: float) -> None:
        stop.set()
        release_flock(other)

    lock = SourceWriteLock(
        _lock_path(database),
        timeout_secs=10.0,
        wait_phase_grace_secs=1.0,
        sleep=stop_then_release,
    )
    handle = _connect(database)
    try:
        assert try_flock_exclusive(other)
        with pytest.raises(WriteLockStoppedError, match='stopped'):
            _write_one(lock, handle, stop=stop)
        assert not handle.connection.in_transaction
        assert try_flock_exclusive(other)
        release_flock(other)
    finally:
        os.close(other)
        handle.close()
        lock.close()
    assert _rows(database) == []


def test_threads_in_one_process_never_overlap_protected_writes(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=2.0)
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
                future.result(timeout=30.0)
    finally:
        lock.close()
    assert peak == [1]
    assert sorted(value for _, value in _rows(database)) == list(range(1, 201))


@pytest.mark.parametrize(
    ('health', 'poll'),
    [(0.0, 1.0), (1.0, -1.0), (math.nan, 1.0), (1.0, math.inf)],
)
def test_timeout_formula_rejects_invalid_intervals(health: float, poll: float) -> None:
    with pytest.raises(ValueError, match='finite and positive'):
        write_lock_timeout_secs(health, poll)


@pytest.mark.parametrize('level', ['process-local', 'file'])
def test_stop_event_abandons_acquisition_promptly(database: Path, level: str) -> None:
    holder_lock = SourceWriteLock(
        _lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=1.0
    )
    lock = SourceWriteLock(_lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=1.0)
    other, _ = secure_open_regular_file(_lock_path(database))
    stop = threading.Event()
    entered = threading.Event()
    release = threading.Event()

    def hold() -> None:
        holder = _connect(database)
        try:
            with holder_lock.transaction(holder.connection):
                entered.set()
                assert release.wait(10.0)
        finally:
            holder.close()

    handle = _connect(database)
    timer = threading.Timer(0.2, stop.set)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = None
            if level == 'process-local':
                future = pool.submit(hold)
                assert entered.wait(5.0)
            else:
                assert try_flock_exclusive(other)
            try:
                timer.start()
                started = time.monotonic()
                with pytest.raises(WriteLockStoppedError, match='stopped'):
                    _write_one(lock, handle, stop=stop)
                assert time.monotonic() - started < 1.0
            finally:
                release.set()
            if future is not None:
                future.result(timeout=5.0)
    finally:
        timer.cancel()
        timer.join()
        os.close(other)
        handle.close()
        lock.close()
        holder_lock.close()


def test_timeout_formula_uses_the_slower_interval() -> None:
    assert WRITE_HOLD_DEADLINE_SECS == 5.0
    assert write_lock_timeout_secs(2.0, 1.0) == 10.0
    assert write_lock_timeout_secs(1.0, 3.0) == 12.0
    assert write_lock_timeout_secs(6.0, 1.0) == 18.0


@pytest.mark.parametrize('failure', ['none', 'wait-exit', 'hold-enter', 'body'])
def test_timing_brackets_sqlite_despite_delayed_cleanup(database: Path, failure: str) -> None:
    clock = _Clock()
    clock.now = 10.0
    lock = SourceWriteLock(
        _lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0, clock=clock
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


def test_transaction_reports_disjoint_wait_and_hold_phases(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=2.0)
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


def test_waiter_owning_the_local_lock_reports_wait_until_the_file_lock_is_free(
    database: Path,
) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=10.0, wait_phase_grace_secs=2.0)
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
            _wait_until(lambda: not math.isnan(timing.local_acquired_at))
            time.sleep(0.05)
            assert phases.current == 'write-lock-wait'
            assert math.isnan(timing.file_acquired_at)
            release_flock(other)
            future.result(timeout=5.0)
    finally:
        os.close(other)
        lock.close()
    assert phases.current is None
    assert ('enter', 'write-hold') in [(kind, phase) for kind, phase, _ in phases.events]
    assert _rows(database) == [(1, 1)]


def test_write_context_converts_only_operator_timeout(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=1.0, wait_phase_grace_secs=1.0)
    try:
        writes = _write_lock.WriteContext(lock, budget_secs=0.02, busy_is_command_failure=True)
        with ThreadPoolExecutor(max_workers=1) as executor:
            held = threading.Event()
            release = threading.Event()

            def hold() -> None:
                with _connect(database) as handle, lock.transaction(handle.connection):
                    held.set()
                    assert release.wait(5)

            future = executor.submit(hold)
            assert held.wait(5)
            try:
                with _connect(database) as handle:
                    with (
                        pytest.raises(_write_lock.SourceDatabaseBusyError) as raised,
                        writes.transaction(handle.connection),
                    ):
                        pytest.fail('busy acquisition entered its body')
                    assert isinstance(raised.value.__cause__, WriteLockTimeoutError)
                    assert str(raised.value) == str(raised.value.__cause__)
            finally:
                release.set()
                future.result(timeout=5)
    finally:
        lock.close()


def test_write_context_delegates_options_and_operator_copy(database: Path) -> None:
    lock = SourceWriteLock(_lock_path(database), timeout_secs=0.2, wait_phase_grace_secs=1.0)
    stop = threading.Event()
    phases = _PhaseLog()
    try:
        writes = _write_lock.WriteContext(lock, watch=phases, stop=stop)
        operator = writes.for_operator()
        assert operator.lock is lock
        assert operator.stop is stop
        assert operator.watch is phases
        assert operator.budget_secs == 2.0
        assert operator.busy_is_command_failure
        assert writes.budget_secs is None
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
