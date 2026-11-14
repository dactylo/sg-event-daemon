from __future__ import annotations

import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import ExitStack, closing
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pytest

from sg_event_daemon import _checkpoint
from sg_event_daemon._checkpoint import Checkpointer
from sg_event_daemon._sqlite import (
    DatabaseError,
    DatabaseHandle,
    Schema,
    open_database,
    transaction,
    wal_file_bytes,
)
from tests._storage_support import write_context_for
from tests._subprocess_support import subprocess_env

if TYPE_CHECKING:
    from collections.abc import Iterator

_LOGGER = logging.getLogger('checkpoint-tests')
_SCHEMA = Schema(
    version=1, statements=('CREATE TABLE entries (id INTEGER PRIMARY KEY, value BLOB)',)
)


def _reader(path: Path, stack: ExitStack) -> sqlite3.Connection:
    reader = stack.enter_context(closing(sqlite3.connect(path, isolation_level=None)))
    reader.execute('BEGIN')
    reader.execute('SELECT COUNT(*) FROM entries').fetchone()
    return reader


@pytest.fixture
def database(tmp_path: Path) -> Iterator[DatabaseHandle]:
    with open_database(
        tmp_path / 'source.db', 'checkpoint-test', _SCHEMA, synchronous='NORMAL'
    ) as handle:
        yield handle


def _write(handle: DatabaseHandle, count: int = 1, size: int = 64) -> None:
    with transaction(handle.connection):
        handle.connection.executemany(
            'INSERT INTO entries (value) VALUES (?)', ((b'x' * size,) for _ in range(count))
        )


def test_checkpoint_database_error_raises(database: DatabaseHandle) -> None:
    with closing(
        Checkpointer(
            database.connection, database.path, interval_secs=1, logger=_LOGGER, log_extra={}
        )
    ) as checkpointer:
        database.connection.close()
        with pytest.raises(DatabaseError, match='closed'):
            checkpointer.run_once()


def test_complete_clears_duration_even_when_reuse_is_busy(database: DatabaseHandle) -> None:
    now = [0.0]
    rows = iter([(0, 10, 5), (0, 10, 5), (0, 10, 10)])
    with closing(
        Checkpointer(
            database.connection,
            database.path,
            interval_secs=1,
            logger=_LOGGER,
            log_extra={},
            clock=lambda: now[0],
            run_checkpoint=lambda: next(rows),
            run_reuse=lambda: (1, 10, 10),
            sync_wal=lambda: None,
        )
    ) as checkpointer:
        checkpointer.run_once()
        now[0] = 5
        checkpointer.run_once()
        assert checkpointer.crossed
        attempt = checkpointer.run_once()
        assert attempt.outcome == 'complete'
        assert attempt.reuse == 'reuse-busy'
        assert not checkpointer.crossed


def test_completes_without_reset_cross_the_growth_bound(database: DatabaseHandle) -> None:
    with ExitStack() as stack:
        other = stack.enter_context(
            open_database(database.path, 'checkpoint-test', _SCHEMA, synchronous='NORMAL')
        )
        checkpointer = stack.enter_context(
            closing(
                Checkpointer(
                    other.connection,
                    database.path,
                    interval_secs=1,
                    logger=_LOGGER,
                    log_extra={},
                    wal_limit_bytes=20 * 4120,
                )
            )
        )
        previous = None
        for _ in range(20):
            _write(database, count=2, size=4096)
            reader = _reader(database.path, stack)
            if previous is not None:
                previous.execute('COMMIT')
            previous = reader
            attempt = checkpointer.run_once()
            assert attempt.outcome == 'complete'
            assert attempt.reuse == 'reuse-busy'
            assert 'incomplete-duration' not in checkpointer.crossed
            if checkpointer.crossed:
                break
        assert checkpointer.crossed == frozenset({'wal-growth'})


def test_equal_count_completes_never_cross_a_bound(database: DatabaseHandle) -> None:
    now = [0.0]
    with closing(
        Checkpointer(
            database.connection,
            database.path,
            interval_secs=1,
            logger=_LOGGER,
            log_extra={},
            clock=lambda: now[0],
        )
    ) as checkpointer:
        for second in range(1, 17):
            _write(database)
            now[0] = second
            attempt = checkpointer.run_once()
            assert attempt.outcome == 'complete'
            assert attempt.reuse == 'reused'
            assert not checkpointer.crossed


def test_growth_bound_is_evaluated_before_reuse(
    database: DatabaseHandle, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_LOGGER.name)
    _write(database, size=10000)
    with closing(
        Checkpointer(
            database.connection,
            database.path,
            interval_secs=1,
            logger=_LOGGER,
            log_extra={'source': 'test'},
            wal_limit_bytes=1,
        )
    ) as checkpointer:
        attempt = checkpointer.run_once()
        assert attempt.wal_bytes > 1
        assert attempt.reuse == 'reused'
        assert checkpointer.crossed == frozenset({'wal-growth'})
        assert Path(f'{database.path}-wal').stat().st_size == 0
        checkpointer.run_once()
        assert not checkpointer.crossed
        _write(database)
        checkpointer.run_once()
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 2
        assert len([r for r in caplog.records if r.levelno == logging.INFO]) == 1


def test_growth_from_file_size_while_every_attempt_is_contended(database: DatabaseHandle) -> None:
    now = [0.0]
    with closing(
        Checkpointer(
            database.connection,
            database.path,
            interval_secs=1,
            logger=_LOGGER,
            log_extra={},
            clock=lambda: now[0],
            run_checkpoint=lambda: (1, -1, -1),
            run_reuse=lambda: (1, -1, -1),
            wal_limit_bytes=1,
        )
    ) as checkpointer:
        _write(database)
        checkpointer.run_once()
        assert checkpointer.crossed == frozenset({'wal-growth'})
        now[0] = 5
        checkpointer.run_once()
        assert checkpointer.crossed == frozenset({'wal-growth', 'incomplete-duration'})


@pytest.mark.parametrize('overlap', [False, True])
def test_held_readers_cross_the_duration_bound_at_five_intervals(
    database: DatabaseHandle, caplog: pytest.LogCaptureFixture, overlap: bool
) -> None:
    caplog.set_level(logging.INFO, logger=_LOGGER.name)
    now = [0.0]
    syncs: list[float] = []
    with ExitStack() as stack:
        checkpoint_db = stack.enter_context(
            open_database(database.path, 'checkpoint-test', _SCHEMA, synchronous='NORMAL')
        )
        checkpointer = stack.enter_context(
            closing(
                Checkpointer(
                    checkpoint_db.connection,
                    database.path,
                    interval_secs=1,
                    logger=_LOGGER,
                    log_extra={},
                    clock=lambda: now[0],
                    sync_wal=lambda: syncs.append(now[0]),
                )
            )
        )
        _write(database)
        reader = _reader(database.path, stack)
        for second in range(1, 7):
            if overlap:
                next_reader = _reader(database.path, stack)
                reader.execute('COMMIT')
                reader = next_reader
            _write(database)
            now[0] = second
            attempt = checkpointer.run_once()
            expected = 'partial-progress' if overlap or second == 1 else 'partial-no-progress'
            assert attempt.outcome == expected
            assert attempt.reuse == 'reuse-busy'
            assert checkpointer.crossed == (
                frozenset({'incomplete-duration'}) if second >= 5 else frozenset()
            )
        assert syncs == list(range(1, 7))
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert str(database.path) in warnings[0]
        assert 'reuse-busy' in warnings[0]
        reader.execute('COMMIT')
        assert checkpointer.run_once().reuse == 'reused'
        assert not checkpointer.crossed
        assert len(syncs) == 6
        assert 'bound cleared' in caplog.records[-1].message
        _write(database)
        assert checkpointer.run_once().reuse == 'reused'


def test_missing_wal_file_counts_as_empty(tmp_path: Path) -> None:

    path = tmp_path / 'empty.db'
    assert wal_file_bytes(path) == 0
    with (
        closing(sqlite3.connect(path)) as connection,
        closing(
            Checkpointer(connection, path, interval_secs=1, logger=_LOGGER, log_extra={})
        ) as checkpointer,
    ):
        assert checkpointer.run_once().wal_bytes == 0
        assert not checkpointer.crossed


def test_outside_checkpoint_lock_makes_attempt_and_reuse_contended(
    database: DatabaseHandle,
) -> None:
    _write(database)
    now = [0.0]
    with open_database(database.path, 'checkpoint-test', _SCHEMA, synchronous='NORMAL') as other:
        database.connection.execute('BEGIN IMMEDIATE')
        process = subprocess.Popen(
            [
                sys.executable,
                '-c',
                (
                    'import sqlite3,sys,time\n'
                    'c=sqlite3.connect(sys.argv[1])\n'
                    "c.execute('PRAGMA busy_timeout=2147483647')\n"
                    # A competing PASSIVE probe can own the checkpoint mutex. SQLite
                    # returns busy immediately there, without invoking its busy handler.
                    "while c.execute('PRAGMA wal_checkpoint(FULL)').fetchone()[0]:\n"
                    '    time.sleep(0.001)\n'
                ),
                str(database.path),
            ],
            env=subprocess_env(),
        )
        try:
            while other.connection.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()[0] != 1:
                assert process.poll() is None, 'checkpoint holder exited'
                time.sleep(0.001)
            statements: list[str] = []
            other.connection.set_trace_callback(statements.append)
            with closing(
                Checkpointer(
                    other.connection,
                    database.path,
                    interval_secs=1,
                    logger=_LOGGER,
                    log_extra={},
                    clock=lambda: now[0],
                )
            ) as checkpointer:
                for second in range(1, 6):
                    now[0] = second
                    attempt = checkpointer.run_once()
                    assert attempt.outcome == 'contended'
                    assert attempt.reuse_result == (1, -1, -1)
                    assert bool(checkpointer.crossed) == (second == 5)
                assert statements.count('PRAGMA wal_checkpoint(PASSIVE)') == 5
                assert statements.count('PRAGMA wal_checkpoint(TRUNCATE)') == 5
                assert statements.count('PRAGMA busy_timeout=100') == 5
        finally:
            database.connection.execute('ROLLBACK')
            assert process.wait() == 0


def test_real_reset_observed_clears_duration_even_when_reuse_is_busy(
    database: DatabaseHandle,
) -> None:
    now = [0.0]
    with ExitStack() as stack:
        other = stack.enter_context(
            open_database(database.path, 'checkpoint-test', _SCHEMA, synchronous='NORMAL')
        )
        checkpointer = stack.enter_context(
            closing(
                Checkpointer(
                    other.connection,
                    database.path,
                    interval_secs=1,
                    logger=_LOGGER,
                    log_extra={},
                    clock=lambda: now[0],
                )
            )
        )
        _write(database, count=30, size=4096)
        reader = _reader(database.path, stack)
        _write(database, count=30, size=4096)
        checkpointer.run_once()
        now[0] = 5
        checkpointer.run_once()
        assert checkpointer.crossed == frozenset({'incomplete-duration'})
        reader.execute('COMMIT')
        database.connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        _write(database)
        _reader(database.path, stack)
        _write(database)
        attempt = checkpointer.run_once()
        assert attempt.outcome == 'reset-observed'
        assert attempt.reuse == 'reuse-busy'
        assert not checkpointer.crossed


def test_reset_observed_clears_the_duration_bound(database: DatabaseHandle) -> None:
    now = [0.0]
    rows = iter([(0, 20, 5), (0, 21, 5), (0, 4, 1)])
    reuse: list[bool] = []

    def busy() -> tuple[int, int, int]:
        reuse.append(True)
        return 1, 4, 1

    with closing(
        Checkpointer(
            database.connection,
            database.path,
            interval_secs=1,
            logger=_LOGGER,
            log_extra={},
            clock=lambda: now[0],
            run_checkpoint=lambda: next(rows),
            run_reuse=busy,
            sync_wal=lambda: None,
        )
    ) as checkpointer:
        checkpointer.run_once()
        now[0] = 5
        checkpointer.run_once()
        assert checkpointer.crossed == frozenset({'incomplete-duration'})
        now[0] = 6
        assert checkpointer.run_once().outcome == 'reset-observed'
        assert not checkpointer.crossed
        assert len(reuse) == 3


def test_reuse_busy_is_recorded_and_clears_nothing(
    database: DatabaseHandle, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    caplog.set_level(logging.INFO, logger=_LOGGER.name)
    now = [0.0]
    finished = threading.Event()
    started = threading.Event()
    reuse_entered = threading.Event()
    busy_timeouts: list[int] = []
    checkpoint = Checkpointer._checkpoint

    def observed_checkpoint(
        owner: Checkpointer, mode: Literal['PASSIVE', 'TRUNCATE']
    ) -> tuple[int, int, int]:
        if mode == 'TRUNCATE':
            busy_timeouts.append(owner._connection.execute('PRAGMA busy_timeout').fetchone()[0])
            reuse_entered.set()
            assert finished.wait()
        return checkpoint(owner, mode)

    monkeypatch.setattr(Checkpointer, '_checkpoint', observed_checkpoint)
    with ExitStack() as stack:
        other = stack.enter_context(
            open_database(database.path, 'checkpoint-test', _SCHEMA, synchronous='NORMAL')
        )
        other.connection.execute('PRAGMA busy_timeout=4321')
        checkpointer = stack.enter_context(
            closing(
                Checkpointer(
                    other.connection,
                    database.path,
                    interval_secs=1,
                    logger=_LOGGER,
                    log_extra={},
                    clock=lambda: now[0],
                    wal_limit_bytes=1,
                )
            )
        )
        _write(database)
        reader = _reader(database.path, stack)
        _write(database)

        def writer() -> None:
            with closing(sqlite3.connect(database.path, isolation_level=None)) as connection:
                started.set()
                connection.execute('INSERT INTO entries(value) VALUES (1)')
                finished.set()

        def during_reuse() -> None:
            reuse_entered.wait()
            writer()

        thread = threading.Thread(target=during_reuse)
        thread.start()
        try:
            now[0] = 5
            attempt = checkpointer.run_once()
            assert started.is_set()
            assert busy_timeouts == [100]
            assert attempt.reuse == 'reuse-busy'
            assert checkpointer.crossed == frozenset({'incomplete-duration', 'wal-growth'})
            assert finished.wait()
            assert other.connection.execute('PRAGMA busy_timeout').fetchone()[0] == 4321
            assert 'reuse-busy' in caplog.text
            reader.execute('COMMIT')
            assert checkpointer.run_once().reuse == 'reused'
            assert checkpointer.crossed == frozenset({'wal-growth'})
            checkpointer.run_once()
            assert not checkpointer.crossed
        finally:
            thread.join()
            assert not thread.is_alive()


def test_reuse_empties_the_wal_file(database: DatabaseHandle) -> None:
    _write(database, count=20, size=4096)
    wal = Path(f'{database.path}-wal')
    salt = wal.read_bytes()[16:24]
    with closing(
        Checkpointer(
            database.connection, database.path, interval_secs=1, logger=_LOGGER, log_extra={}
        )
    ) as checkpointer:
        assert checkpointer.run_once().reuse == 'reused'
        assert wal.stat().st_size == 0
        _write(database)
        assert wal.read_bytes()[16:24] != salt
        row = database.connection.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
        assert row[1] <= 4
        assert wal.stat().st_size == 32 + row[1] * 4120


@pytest.mark.parametrize('result', [(0, 1, 1), (0, 10, 2), (1, -1, -1)])
def test_reuse_follows_every_attempt(
    database: DatabaseHandle, result: tuple[int, int, int]
) -> None:
    calls: list[str] = []

    def passive() -> tuple[int, int, int]:
        calls.append('passive')
        return result

    def reuse() -> tuple[int, int, int]:
        calls.append('reuse')
        return 0, 0, 0

    with closing(
        Checkpointer(
            database.connection,
            database.path,
            interval_secs=1,
            logger=_LOGGER,
            log_extra={},
            run_checkpoint=passive,
            run_reuse=reuse,
        )
    ) as checkpointer:
        attempt = checkpointer.run_once()
        assert calls == ['passive', 'reuse']
        assert attempt.reuse == 'reused'
        assert attempt.reuse_result == (0, 0, 0)


@pytest.mark.parametrize('reused', [False, True])
def test_reuse_restarts_the_duration_timer_and_busy_reuse_does_not(
    database: DatabaseHandle, reused: bool
) -> None:
    now = [0.0]
    with closing(
        Checkpointer(
            database.connection,
            database.path,
            interval_secs=1,
            logger=_LOGGER,
            log_extra={},
            clock=lambda: now[0],
            run_checkpoint=lambda: (0, 10, 2),
            run_reuse=lambda: (0, 0, 0) if reused else (1, 10, 2),
            sync_wal=lambda: None,
        )
    ) as checkpointer:
        for second in range(1, 17):
            now[0] = second
            checkpointer.run_once()
            assert bool(checkpointer.crossed) == (not reused and second >= 5)


@pytest.mark.parametrize('overlap', [False, True])
def test_readers_ending_within_the_reuse_wait_cross_no_bound(
    database: DatabaseHandle, overlap: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [0.0]
    release = threading.Event()
    ended = threading.Event()
    checkpoint = Checkpointer._checkpoint

    def observed_checkpoint(
        owner: Checkpointer, mode: Literal['PASSIVE', 'TRUNCATE']
    ) -> tuple[int, int, int]:
        if mode == 'TRUNCATE':
            assert owner._connection.execute('PRAGMA busy_timeout').fetchone()[0] == 100
            release.set()
            ended.wait()
        return checkpoint(owner, mode)

    monkeypatch.setattr(Checkpointer, '_checkpoint', observed_checkpoint)
    with closing(
        Checkpointer(
            database.connection,
            database.path,
            interval_secs=1,
            logger=_LOGGER,
            log_extra={},
            clock=lambda: now[0],
        )
    ) as checkpointer:
        for second in range(1, 17):
            _write(database)
            ready = threading.Event()
            release.clear()
            ended.clear()

            def read(ready: threading.Event = ready) -> None:
                with ExitStack() as stack:
                    first = _reader(database.path, stack)
                    if overlap:
                        _reader(database.path, stack)
                        first.execute('COMMIT')
                    ready.set()
                    release.wait()
                ended.set()

            thread = threading.Thread(target=read)
            thread.start()
            try:
                assert ready.wait()
                _write(database)
                now[0] = second
                assert checkpointer.run_once().reuse == 'reused'
                assert not checkpointer.crossed
            finally:
                release.set()
                thread.join()


def test_reuse_takes_no_daemon_lock(database: DatabaseHandle) -> None:

    writes = write_context_for(database.path)
    held = threading.Event()
    release = threading.Event()
    entry = writes.lock._entry
    assert entry is not None

    def hold() -> None:
        with entry.lock:
            held.set()
            assert release.wait()

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert held.wait()
        _write(database)
        with closing(
            Checkpointer(
                database.connection, database.path, interval_secs=1, logger=_LOGGER, log_extra={}
            )
        ) as checkpointer:
            assert checkpointer.run_once().reuse == 'reused'
    finally:
        release.set()
        thread.join()
        writes.lock.close()


def test_spill_counts_toward_growth_before_commit(database: DatabaseHandle) -> None:
    with ExitStack() as stack:
        other = stack.enter_context(
            open_database(database.path, 'checkpoint-test', _SCHEMA, synchronous='NORMAL')
        )
        checkpointer = stack.enter_context(
            closing(
                Checkpointer(
                    other.connection,
                    database.path,
                    interval_secs=1,
                    logger=_LOGGER,
                    log_extra={},
                    wal_limit_bytes=200000,
                )
            )
        )
        database.connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        database.connection.execute('PRAGMA cache_size=10')
        database.connection.execute('BEGIN IMMEDIATE')
        try:
            database.connection.executemany(
                'INSERT INTO entries(value) VALUES (?)', ((b'x' * 4096,) for _ in range(200))
            )
            attempt = checkpointer.run_once()
            assert attempt.outcome == 'complete'
            assert attempt.result == (0, 0, 0)
            assert checkpointer.crossed == frozenset({'wal-growth'})
        finally:
            database.connection.execute('ROLLBACK')


def test_wal_sync_descriptor_is_reused_and_closed(
    database: DatabaseHandle, monkeypatch: pytest.MonkeyPatch
) -> None:

    synced: list[int] = []
    original = os.fsync

    def sync(fd: int) -> None:
        synced.append(fd)
        original(fd)

    monkeypatch.setattr(_checkpoint.os, 'fsync', sync)
    _write(database)
    checkpointer = Checkpointer(
        database.connection,
        database.path,
        interval_secs=1,
        logger=_LOGGER,
        log_extra={},
        run_checkpoint=lambda: (1, -1, -1),
        run_reuse=lambda: (1, -1, -1),
    )
    checkpointer.run_once()
    checkpointer.run_once()
    assert len(synced) == 2
    assert synced[0] == synced[1]
    checkpointer.close()
    with pytest.raises(OSError, match='Bad file descriptor'):
        os.fstat(synced[0])
