from __future__ import annotations

import json
import select
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon._checkpoint import (
    _WAL_IN_USE_LIMIT_BYTES,
    Checkpointer,
    CheckpointOutcome,
    CheckpointResult,
)
from sg_event_daemon._sqlite import (
    DatabaseError,
    DatabaseHandle,
    Schema,
    open_database,
    transaction,
)
from tests._subprocess_support import subprocess_env

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

_CONTENDED: CheckpointResult = (1, -1, -1)
_SCHEMA = Schema(
    version=1,
    statements=('CREATE TABLE entries (id INTEGER PRIMARY KEY, value BLOB NOT NULL)',),
)
_FULL_CHECKPOINT_CODE = """
import sqlite3
import sys
import time

connection = sqlite3.connect(sys.argv[1], isolation_level=None)
connection.execute('PRAGMA busy_timeout=5000')
print('CHECKPOINTING', flush=True)
while True:
    row = connection.execute('PRAGMA wal_checkpoint(FULL)').fetchone()
    if row[0] == 0:
        break
    time.sleep(0.001)
"""
_ALIGNED_CHECKPOINTER_CODE = """
import json
import sys
import time
from pathlib import Path
from sg_event_daemon._checkpoint import Checkpointer
from sg_event_daemon._sqlite import Schema, open_database

schema = Schema(
    version=1,
    statements=('CREATE TABLE entries (id INTEGER PRIMARY KEY, value BLOB NOT NULL)',),
)
path = Path(sys.argv[1])
interval = float(sys.argv[2])
attempts = int(sys.argv[3])
handle = open_database(path, 'checkpoint-test', schema, synchronous='NORMAL')
print('READY', flush=True)
assert sys.stdin.readline().strip() == 'CONTEND'
handle.connection.execute('PRAGMA busy_timeout=10000')
while True:
    row = handle.connection.execute('PRAGMA wal_checkpoint(FULL)').fetchone()
    if row[0] == 0:
        break
    assert row == (1, -1, -1)
    time.sleep(0.001)
print('RELEASED', flush=True)
now = 0.0
first_try_at = now

def passive_checkpoint():
    global first_try_at
    row = handle.connection.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
    if now != first_try_at:
        first_try_at = now
        print('FIRST_DONE', flush=True)
        assert sys.stdin.readline().strip() == 'FIRST_DONE'
    return row

checkpointer = Checkpointer(
    handle.connection, path, interval_secs=interval, clock=lambda: now,
    run_checkpoint=passive_checkpoint,
)
for index in range(attempts):
    due = float(sys.stdin.readline())
    time.sleep(max(0.0, due - time.monotonic()))
    now = (index + 1) * interval
    attempt = checkpointer.run_once()
    print(json.dumps([attempt.outcome, attempt.retried]), flush=True)
handle.close()
"""


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _attempt(checkpointer: Checkpointer, clock: _Clock) -> CheckpointOutcome:
    clock.now += 1.0
    return checkpointer.run_once().outcome


def _connect(path: Path) -> DatabaseHandle:
    return open_database(path, 'checkpoint-test', _SCHEMA, synchronous='NORMAL')


def _read_child_line(process: subprocess.Popen[str]) -> str:
    stdout = process.stdout
    assert stdout is not None
    readable, _writable, _exceptional = select.select((stdout,), (), (), 10.0)
    assert readable, 'child did not respond within 10 s'
    line = stdout.readline().strip()
    assert line, f'child closed stdout (exit status {process.poll()})'
    return line


def _read_child_notice(process: subprocess.Popen[str], expected: str) -> None:
    assert _read_child_line(process) == expected


def _scripted(results: list[CheckpointResult]) -> Callable[[], CheckpointResult]:
    remaining = iter(results)
    return lambda: next(remaining)


@pytest.fixture
def database(tmp_path: Path) -> Iterator[DatabaseHandle]:
    handle = open_database(
        tmp_path / 'source.db', 'checkpoint-test', _SCHEMA, synchronous='NORMAL'
    )
    try:
        yield handle
    finally:
        handle.close()


def _scripted_checkpointer(
    database: DatabaseHandle,
    clock: _Clock,
    results: list[CheckpointResult],
    wal_limit_bytes: int = _WAL_IN_USE_LIMIT_BYTES,
) -> Checkpointer:
    return Checkpointer(
        database.connection,
        database.path,
        interval_secs=1.0,
        clock=clock,
        sleep=lambda _seconds: None,
        random_value=lambda: 0.5,
        run_checkpoint=_scripted(results),
        wal_limit_bytes=wal_limit_bytes,
    )


def _write(handle: DatabaseHandle, count: int = 1, size: int = 64) -> None:
    with transaction(handle.connection):
        handle.connection.executemany(
            'INSERT INTO entries (value) VALUES (?)', ((b'x' * size,) for _ in range(count))
        )


def _checkpoint_under_chained_readers(
    database: DatabaseHandle,
    checkpointer: Checkpointer,
    clock: _Clock,
    readers: list[sqlite3.Connection],
    outcomes: list[CheckpointOutcome],
) -> None:
    # Each new reader starts before the previous one ends, so a reader always pins the WAL.
    for index in range(100):
        _write(database, count=2, size=1024)
        reader = sqlite3.connect(database.path, isolation_level=None)
        reader.execute('BEGIN')
        reader.execute('SELECT COUNT(*) FROM entries').fetchone()
        readers.append(reader)
        if index > 0:
            readers[index - 1].execute('COMMIT')
        outcomes.append(_attempt(checkpointer, clock))


def _checkpointer_after_contended_retry(
    database: DatabaseHandle,
    checkpoint_handle: DatabaseHandle,
    process: subprocess.Popen[str],
    interval: float,
    clock: _Clock,
) -> Checkpointer:
    rows: list[CheckpointResult] = []
    sleeps: list[float] = []
    first_try_at = clock.now
    stdin = process.stdin
    assert stdin is not None

    def passive_checkpoint() -> CheckpointResult:
        nonlocal first_try_at
        busy, log, checkpointed = checkpoint_handle.connection.execute(
            'PRAGMA wal_checkpoint(PASSIVE)'
        ).fetchone()
        row = (int(busy), int(log), int(checkpointed))
        rows.append(row)
        # Logical time advances once per aligned round. The forced prelude
        # and retries keep their timestamp and must bypass this barrier.
        if clock.now != first_try_at:
            first_try_at = clock.now
            _read_child_notice(process, 'FIRST_DONE')
            stdin.write('FIRST_DONE\n')
            stdin.flush()
        return row

    def retry_after_jitter(seconds: float) -> None:
        assert seconds == interval / 4
        sleeps.append(seconds)
        time.sleep(seconds)
        if database.connection.in_transaction:
            database.connection.execute('ROLLBACK')
            _read_child_notice(process, 'RELEASED')

    _read_child_notice(process, 'READY')
    _write(database)
    database.connection.execute('BEGIN IMMEDIATE')
    stdin.write('CONTEND\n')
    stdin.flush()
    # The peer's FULL checkpoint holds the checkpoint lock while this writer
    # blocks it. Observe that real lock before exercising retry; a startup
    # notice or a fixed sleep cannot establish lock ownership.
    deadline = time.monotonic() + 10.0
    while passive_checkpoint() != _CONTENDED:
        assert time.monotonic() < deadline, 'peer did not acquire the checkpoint lock'
        time.sleep(0.001)
    rows.clear()
    checkpointer = Checkpointer(
        checkpoint_handle.connection,
        database.path,
        interval_secs=interval,
        clock=clock,
        sleep=retry_after_jitter,
        random_value=lambda: 0.5,
        run_checkpoint=passive_checkpoint,
    )
    forced = checkpointer.run_once()
    assert forced.retried
    assert forced.outcome == 'complete'
    assert rows == [_CONTENDED, forced.result]
    assert sleeps == [interval / 4]
    return checkpointer


def test_bound_failure_exposes_the_classified_attempt(database: DatabaseHandle) -> None:
    clock = _Clock()
    checkpointer = _scripted_checkpointer(database, clock, [(0, 10, 0)], wal_limit_bytes=1)
    with pytest.raises(DatabaseError) as caught:
        checkpointer.run_once()
    attempt = getattr(caught.value, 'attempt', None)
    assert attempt is not None
    assert attempt.result == (0, 10, 0)
    assert attempt.outcome == 'partial-progress'
    assert attempt.finished_at == 100.0


def test_contention_from_another_process_takes_the_retry(database: DatabaseHandle) -> None:
    clock = _Clock()
    sleeps: list[float] = []
    _write(database, count=5)
    writer = _connect(database.path)
    checkpoint_handle = _connect(database.path)
    writer.connection.execute('BEGIN IMMEDIATE')
    # A FULL checkpoint takes the checkpoint lock, then waits for the writer while holding it.
    process = subprocess.Popen(
        [sys.executable, '-c', _FULL_CHECKPOINT_CODE, str(database.path)],
        stdout=subprocess.PIPE,
        text=True,
        env=subprocess_env(),
    )
    try:
        _read_child_notice(process, 'CHECKPOINTING')
        deadline = time.monotonic() + 10.0
        while checkpoint_handle.connection.execute(
            'PRAGMA wal_checkpoint(PASSIVE)'
        ).fetchone() != (_CONTENDED):
            assert time.monotonic() < deadline, 'peer did not acquire the checkpoint lock'
            time.sleep(0.001)
        checkpointer = Checkpointer(
            checkpoint_handle.connection,
            database.path,
            interval_secs=1.0,
            clock=clock,
            sleep=sleeps.append,
            random_value=lambda: 0.5,
        )
        clock.now += 1.0
        attempt = checkpointer.run_once()
        assert attempt.outcome == 'contended'
        assert attempt.retried
        assert attempt.result == (1, -1, -1)
        assert sleeps == [0.25]
    finally:
        writer.connection.execute('ROLLBACK')
        assert process.wait(timeout=10.0) == 0
        writer.close()
        checkpoint_handle.close()


def test_equal_count_completes_never_halt(database: DatabaseHandle) -> None:
    clock = _Clock()
    checkpointer = _scripted_checkpointer(database, clock, [(0, 1, 1)] * 20)
    assert {_attempt(checkpointer, clock) for _ in range(20)} == {'complete'}


def test_first_partial_zero_checkpoint_count_has_no_prior_progress_sample(
    database: DatabaseHandle,
) -> None:
    clock = _Clock()
    checkpointer = _scripted_checkpointer(database, clock, [(0, 10, 0)] * 2)
    assert _attempt(checkpointer, clock) == 'partial-progress'
    assert _attempt(checkpointer, clock) == 'partial-no-progress'


def test_growth_is_unobserved_while_every_attempt_is_contended(database: DatabaseHandle) -> None:
    clock = _Clock()
    limit = 5 * (4096 + 24)
    _write(database, count=20, size=4096)
    calls = [0]
    real = database.connection

    def mostly_contended() -> CheckpointResult:
        calls[0] += 1
        if calls[0] <= 6:
            return _CONTENDED
        busy, log, checkpointed = real.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
        return int(busy), int(log), int(checkpointed)

    checkpointer = Checkpointer(
        real,
        database.path,
        interval_secs=1.0,
        clock=clock,
        sleep=lambda _seconds: None,
        random_value=lambda: 0.5,
        run_checkpoint=mostly_contended,
        wal_limit_bytes=limit,
    )
    assert [_attempt(checkpointer, clock) for _ in range(3)] == ['contended'] * 3
    with pytest.raises(DatabaseError, match='WAL growth bound exceeded'):
        _attempt(checkpointer, clock)


def test_growth_tripwire_fires_on_a_complete_attempt(database: DatabaseHandle) -> None:
    clock = _Clock()
    frame = 4096 + 24
    checkpointer = _scripted_checkpointer(
        database, clock, [(0, 10, 10), (0, 11, 11)], wal_limit_bytes=10 * frame
    )
    assert _attempt(checkpointer, clock) == 'complete'
    with pytest.raises(DatabaseError, match='WAL growth bound exceeded') as caught:
        _attempt(checkpointer, clock)
    message = str(caught.value)
    assert str(database.path) in message
    assert 'complete (0, 10, 10)' in message
    assert 'since the last complete or reset-observed attempt' in message


@pytest.mark.parametrize(
    ('results', 'wal_limit_bytes', 'expected'),
    [
        ([(0, 10, 5)] * 5, _WAL_IN_USE_LIMIT_BYTES, 'partial-no-progress'),
        # Contended attempts report no log, so even a one-byte limit never trips.
        ([_CONTENDED] * 10, 1, 'contended'),
    ],
    ids=['partial', 'contended'],
)
def test_incomplete_attempts_halt_at_the_duration_bound_and_not_before(
    database: DatabaseHandle,
    results: list[CheckpointResult],
    wal_limit_bytes: int,
    expected: CheckpointOutcome,
) -> None:
    clock = _Clock()
    checkpointer = _scripted_checkpointer(database, clock, results, wal_limit_bytes)
    outcomes = [_attempt(checkpointer, clock) for _ in range(4)]
    assert outcomes[1:] == [expected] * 3
    with pytest.raises(DatabaseError, match='incomplete duration bound exceeded'):
        _attempt(checkpointer, clock)


def test_limit_constant_matches_policy() -> None:
    assert _WAL_IN_USE_LIMIT_BYTES == 256 * 1024 * 1024


def test_outcomes_follow_the_documented_order(database: DatabaseHandle) -> None:
    clock = _Clock()
    sleeps: list[float] = []
    checkpointer = Checkpointer(
        database.connection,
        database.path,
        interval_secs=1.0,
        clock=clock,
        sleep=sleeps.append,
        random_value=lambda: 0.5,
        run_checkpoint=_scripted(
            [
                (0, 0, 0),
                (0, 5, 5),
                _CONTENDED,
                (0, 3, 3),
                _CONTENDED,
                _CONTENDED,
                (0, 10, 4),
                (0, 12, 4),
                (0, 14, 9),
                (0, 3, 1),
            ]
        ),
    )
    outcomes = [_attempt(checkpointer, clock) for _ in range(8)]
    assert outcomes == [
        'complete',
        'complete',
        'complete',
        'contended',
        'partial-progress',
        'partial-no-progress',
        'partial-progress',
        'reset-observed',
    ]
    assert sleeps == [0.25, 0.25]
    assert checkpointer.state == 'complete'


def test_real_completes_without_reset_halt_on_growth(database: DatabaseHandle) -> None:
    clock = _Clock()
    checkpoint_handle = _connect(database.path)
    readers: list[sqlite3.Connection] = []
    try:
        checkpointer = Checkpointer(
            checkpoint_handle.connection,
            database.path,
            interval_secs=1.0,
            clock=clock,
            wal_limit_bytes=20 * (4096 + 24),
        )
        outcomes: list[CheckpointOutcome] = []
        with pytest.raises(DatabaseError, match='WAL growth bound exceeded'):
            _checkpoint_under_chained_readers(database, checkpointer, clock, readers, outcomes)
        # Every attempt completed, yet a reader always pinned the WAL so it never reset.
        assert set(outcomes) == {'complete'}
    finally:
        for reader in readers:
            if reader.in_transaction:
                reader.execute('COMMIT')
            reader.close()
        checkpoint_handle.close()


def test_real_equal_count_completes_recur_after_wal_reuse(database: DatabaseHandle) -> None:
    clock = _Clock()
    assert database.connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
    checkpoint_handle = _connect(database.path)
    try:
        checkpointer = Checkpointer(
            checkpoint_handle.connection, database.path, interval_secs=1.0, clock=clock
        )
        results: list[CheckpointResult] = []
        for _ in range(15):
            _write(database)
            clock.now += 1.0
            attempt = checkpointer.run_once()
            assert attempt.outcome == 'complete'
            results.append(attempt.result)
    finally:
        checkpoint_handle.close()
    # The WAL restarts after each complete checkpoint, so small equal counts recur.
    assert max(log for _busy, log, _ckpt in results) <= 4


def test_real_long_reader_halts_at_the_duration_bound(database: DatabaseHandle) -> None:
    clock = _Clock()
    checkpoint_handle = _connect(database.path)
    reader = sqlite3.connect(database.path, isolation_level=None)
    try:
        checkpointer = Checkpointer(
            checkpoint_handle.connection, database.path, interval_secs=1.0, clock=clock
        )
        _write(database)
        reader.execute('BEGIN')
        reader.execute('SELECT COUNT(*) FROM entries').fetchone()
        outcomes: list[CheckpointOutcome] = []
        for _ in range(4):
            _write(database)
            outcomes.append(_attempt(checkpointer, clock))
        assert outcomes[1:] == ['partial-no-progress'] * 3
        _write(database)
        with pytest.raises(DatabaseError, match='incomplete duration bound exceeded'):
            _attempt(checkpointer, clock)
    finally:
        reader.execute('COMMIT')
        reader.close()
        checkpoint_handle.close()


def test_real_overlapping_readers_halt_at_the_duration_bound(database: DatabaseHandle) -> None:
    clock = _Clock()
    checkpoint_handle = _connect(database.path)
    readers: list[sqlite3.Connection] = []
    try:
        checkpointer = Checkpointer(
            checkpoint_handle.connection, database.path, interval_secs=1.0, clock=clock
        )
        outcomes: list[CheckpointOutcome] = []
        for index in range(5):
            _write(database)
            reader = sqlite3.connect(database.path, isolation_level=None)
            reader.execute('BEGIN')
            reader.execute('SELECT COUNT(*) FROM entries').fetchone()
            readers.append(reader)
            _write(database)
            if index > 0:
                readers[index - 1].execute('COMMIT')
            if index < 4:
                outcomes.append(_attempt(checkpointer, clock))
        assert outcomes[1:] == ['partial-progress'] * 3
        with pytest.raises(DatabaseError, match='incomplete duration bound exceeded'):
            _attempt(checkpointer, clock)
    finally:
        for reader in readers:
            if reader.in_transaction:
                reader.execute('COMMIT')
            reader.close()
        checkpoint_handle.close()


def test_real_reset_is_observed_by_a_checkpointer_that_saw_a_partial(
    database: DatabaseHandle,
) -> None:
    clock = _Clock()
    checkpoint_handle = _connect(database.path)
    other = _connect(database.path)
    first_reader = sqlite3.connect(database.path, isolation_level=None)
    second_reader = sqlite3.connect(database.path, isolation_level=None)
    try:
        checkpointer = Checkpointer(
            checkpoint_handle.connection, database.path, interval_secs=1.0, clock=clock
        )
        _write(database, count=50, size=4096)
        first_reader.execute('BEGIN')
        first_reader.execute('SELECT COUNT(*) FROM entries').fetchone()
        _write(database, count=50, size=4096)
        assert _attempt(checkpointer, clock) in {'partial-progress', 'partial-no-progress'}
        first_reader.execute('COMMIT')
        busy, log, checkpointed = other.connection.execute(
            'PRAGMA wal_checkpoint(PASSIVE)'
        ).fetchone()
        assert busy == 0
        assert checkpointed == log
        _write(database)
        second_reader.execute('BEGIN')
        second_reader.execute('SELECT COUNT(*) FROM entries').fetchone()
        _write(database)
        assert _attempt(checkpointer, clock) == 'reset-observed'
        assert checkpointer.state == 'complete'
    finally:
        for reader in (first_reader, second_reader):
            if reader.in_transaction:
                reader.execute('COMMIT')
            reader.close()
        other.close()
        checkpoint_handle.close()


def test_reset_observed_clears_the_duration_bound(database: DatabaseHandle) -> None:
    clock = _Clock()
    results: list[CheckpointResult] = [
        (0, 20, 5),
        (0, 21, 5),
        (0, 22, 6),
        (0, 23, 6),
        (0, 4, 1),
        (0, 5, 1),
        (0, 6, 2),
        (0, 7, 2),
        (0, 8, 3),
    ]
    checkpointer = _scripted_checkpointer(database, clock, results)
    outcomes = [_attempt(checkpointer, clock) for _ in range(9)]
    assert outcomes[4] == 'reset-observed'
    assert 'complete' not in outcomes


def test_retried_attempt_reports_the_retry_row(database: DatabaseHandle) -> None:
    clock = _Clock()
    checkpointer = _scripted_checkpointer(database, clock, [_CONTENDED, (0, 3, 3)])
    clock.now += 1.0
    attempt = checkpointer.run_once()
    assert attempt.retried
    assert attempt.result == (0, 3, 3)
    assert attempt.outcome == 'complete'


def test_spill_before_commit_is_invisible_until_commit(database: DatabaseHandle) -> None:
    clock = _Clock()
    limit = 50 * (4096 + 24)
    checkpoint_handle = _connect(database.path)
    wal = Path(f'{database.path}-wal')
    try:
        checkpointer = Checkpointer(
            checkpoint_handle.connection,
            database.path,
            interval_secs=1.0,
            clock=clock,
            wal_limit_bytes=limit,
        )
        assert database.connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
        database.connection.execute('PRAGMA cache_size=10')
        database.connection.execute('BEGIN IMMEDIATE')
        database.connection.executemany(
            'INSERT INTO entries (value) VALUES (?)', ((b'y' * 4096,) for _ in range(200))
        )
        peak = wal.stat().st_size
        assert peak > limit
        assert _attempt(checkpointer, clock) == 'complete'
        database.connection.execute('COMMIT')
        print(f'peak WAL file size {peak} bytes')
        with pytest.raises(DatabaseError, match='WAL growth bound exceeded'):
            _attempt(checkpointer, clock)
    finally:
        if database.connection.in_transaction:
            database.connection.execute('ROLLBACK')
        checkpoint_handle.close()


def test_state_is_idle_before_the_first_attempt(database: DatabaseHandle) -> None:
    checkpointer = _scripted_checkpointer(database, _Clock(), [])
    assert checkpointer.state == 'idle'
    assert checkpointer.frame_bytes == 4096 + 24


def test_two_processes_with_aligned_schedules_checkpoint_without_halting(
    database: DatabaseHandle,
) -> None:
    interval = 0.05
    attempts = 10
    clock = _Clock()
    clock.now = 0.0
    checkpoint_handle = _connect(database.path)
    process = subprocess.Popen(
        [
            sys.executable,
            '-c',
            _ALIGNED_CHECKPOINTER_CODE,
            str(database.path),
            repr(interval),
            str(attempts),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=subprocess_env(),
    )

    try:
        checkpointer = _checkpointer_after_contended_retry(
            database, checkpoint_handle, process, interval, clock
        )
        stdin = process.stdin
        assert stdin is not None
        local: dict[str, int] = {}
        remote: dict[str, int] = {}
        retries = 0
        for index in range(attempts):
            _write(database)
            # First-call and round-end barriers guarantee peer lock release
            # before retry and keep the next aligned round from starting early.
            due = time.monotonic() + interval
            stdin.write(f'{due!r}\n')
            stdin.flush()
            time.sleep(max(0.0, due - time.monotonic()))
            clock.now = (index + 1) * interval
            attempt = checkpointer.run_once()
            assert attempt.outcome == 'complete'
            local[attempt.outcome] = local.get(attempt.outcome, 0) + 1
            remote_outcome, remote_retried = json.loads(_read_child_line(process))
            assert remote_outcome == 'complete'
            remote[remote_outcome] = remote.get(remote_outcome, 0) + 1
            retries += int(attempt.retried) + int(remote_retried)
        assert process.wait(timeout=10.0) == 0
    finally:
        if database.connection.in_transaction:
            database.connection.execute('ROLLBACK')
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10.0)
        checkpoint_handle.close()
    assert sum(local.values()) == attempts
    assert sum(remote.values()) == attempts
    print(f'local outcomes {local}; remote outcomes {remote}; aligned retries {retries}')
