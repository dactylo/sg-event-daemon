from __future__ import annotations

import errno
import logging
import sqlite3
import threading
import time
from contextlib import closing
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Mapping
    from pathlib import Path

import pytest

from sg_event_daemon import _checkpoint_host as host_module
from sg_event_daemon import _platform as platform_module
from sg_event_daemon._checkpoint import CheckpointAttempt, Checkpointer
from sg_event_daemon._sqlite import DatabaseHandle, Schema, open_database, wal_file_bytes

_LOGGER = logging.getLogger('checkpoint-host-tests')
_SCHEMA = Schema(version=1, statements=('CREATE TABLE entries (id INTEGER PRIMARY KEY)',))


def _database(path: Path, identity: str) -> None:
    with open_database(path, identity, _SCHEMA, synchronous='NORMAL'):
        pass


def _grow_wal(connection: sqlite3.Connection, path: Path, target: int) -> int:
    while wal_file_bytes(path) <= target:
        connection.execute('INSERT INTO entries DEFAULT VALUES')
    return wal_file_bytes(path)


def _wait_host_signal(event: threading.Event, host: host_module.CheckpointHost) -> None:
    while not event.is_set():
        assert any(thread.is_alive() for thread in host.threads()) or event.is_set(), (
            'checkpoint host exited before its signal'
        )
        time.sleep(0.001)


def test_checkpoint_runs_on_the_host_thread_with_its_own_connection(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    writer = open_database(path, 'owner', _SCHEMA, synchronous='NORMAL')
    finished = threading.Event()
    observed: list[tuple[bool, str, tuple[int, ...]]] = []

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        def checkpoint() -> tuple[int, int, int]:
            row = connection.execute('SELECT COUNT(*) FROM entries').fetchone()
            assert row is not None
            observed.append(
                (connection is writer.connection, threading.current_thread().name, row)
            )
            finished.set()
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            logger=logger,
            log_extra=log_extra,
            run_checkpoint=checkpoint,
        )

    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=0.02,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        checkpointer_factory=factory,
    )
    try:
        writer.connection.execute('INSERT INTO entries VALUES (1)')
        host.register(path, 'owner', _SCHEMA, log_extra={})
        _wait_host_signal(finished, host)
    finally:
        host.close()
        writer.close()
    assert observed[0] == (False, 'sg-event-daemon:checkpoint:owner', (1,))


def test_host_attempts_on_its_own_timer_without_component_activity(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'timer')
    attempts: list[float] = []
    clock = [0.0]

    class Schedule(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            assert timeout is not None
            if len(attempts) == 6:
                return True
            clock[0] += timeout
            return False

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        def checkpoint() -> tuple[int, int, int]:
            attempts.append(clock[0])
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            logger=logger,
            log_extra=log_extra,
            clock=lambda: clock[0],
            run_checkpoint=checkpoint,
        )

    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=0.05,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        clock=lambda: clock[0],
        checkpointer_factory=factory,
    )
    host._stop = Schedule()
    host._run(
        {
            'path': path,
            'identity': 'timer',
            'schema': _SCHEMA,
            'first_due': 0.05,
            'status': {'lock': threading.Lock(), 'stalled': False},
            'log_extra': {},
        }
    )
    assert attempts == pytest.approx([0.05, 0.10, 0.15, 0.20, 0.25, 0.30])


def test_host_bound_sets_view_stalled_without_failing(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'contended')
    failures: list[str] = []
    clock = [0.0]
    paused = threading.Event()
    release = threading.Event()

    class Schedule(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            assert timeout is not None
            if clock[0] >= 0.12:
                paused.set()
                release.wait()
                return True
            clock[0] += timeout
            return False

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            logger=logger,
            log_extra=log_extra,
            clock=lambda: clock[0],
            run_checkpoint=lambda: (1, -1, -1),
            run_reuse=lambda: (1, -1, -1),
            sync_wal=lambda: None,
        )

    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=0.02,
        synchronous='NORMAL',
        on_failure=failures.append,
        clock=lambda: clock[0],
        checkpointer_factory=factory,
    )
    host._stop = Schedule()
    try:
        view = host.register(path, 'contended', _SCHEMA, log_extra={})
        _wait_host_signal(paused, host)
        assert view.stalled()
        assert failures == []
        assert host.threads()[0].is_alive()
    finally:
        release.set()
        host.close()


def test_host_close_interrupts_waits_and_closes_connections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'close')
    connections: list[sqlite3.Connection] = []
    ready = threading.Event()

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        connections.append(connection)
        ready.set()
        return Checkpointer(
            connection, path, interval_secs=interval_secs, logger=logger, log_extra=log_extra
        )

    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=60,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        checkpointer_factory=factory,
    )
    host.register(path, 'close', _SCHEMA, log_extra={})
    _wait_host_signal(ready, host)
    thread = host.threads()[0]
    join = thread.join

    def stopped_before_join() -> None:
        assert host._stop.is_set()
        join()

    monkeypatch.setattr(thread, 'join', stopped_before_join)
    try:
        host.close()
        host.close()
    finally:
        host.request_stop()
        join()
    assert host._stop.is_set()
    assert all(not thread.is_alive() for thread in host.threads())
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        _ = connections[0].total_changes


def test_host_delayed_wakes_skip_elapsed_slots_without_catchup(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'timer')
    clock = [0.0]
    starts: list[float] = []
    deadlines: list[float] = []
    wakes = iter((0.095, 0.100, 0.275, 0.300))

    class Schedule(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            assert timeout is not None
            deadlines.append(clock[0] + timeout)
            wake = next(wakes, None)
            if wake is None:
                return True
            assert wake >= clock[0] + timeout - 1e-12
            clock[0] = max(wake, clock[0] + timeout)
            return False

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        def checkpoint() -> tuple[int, int, int]:
            starts.append(clock[0])
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            logger=logger,
            log_extra=log_extra,
            clock=lambda: clock[0],
            run_checkpoint=checkpoint,
        )

    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=0.05,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        clock=lambda: clock[0],
        checkpointer_factory=factory,
    )
    host._stop = Schedule()
    host._run(
        {
            'path': path,
            'identity': 'timer',
            'schema': _SCHEMA,
            'first_due': 0.05,
            'status': {'lock': threading.Lock(), 'stalled': False},
            'log_extra': {},
        }
    )
    assert starts == pytest.approx([0.095, 0.100, 0.275, 0.300])
    assert deadlines == pytest.approx([0.05, 0.10, 0.15, 0.30, 0.35])
    # The delayed 50 ms slot can start only 5 ms before the next grid slot.
    assert starts[1] - starts[0] == pytest.approx(0.005)


def test_host_missing_file_reports_failure_without_creating_it(tmp_path: Path) -> None:
    path = tmp_path / 'missing.db'
    failures: list[str] = []
    failed = threading.Event()

    def failure(reason: str) -> None:
        failures.append(reason)
        failed.set()

    host = host_module.CheckpointHost(
        logger=_LOGGER, interval_secs=0.02, synchronous='NORMAL', on_failure=failure
    )
    try:
        host.register(path, 'missing', _SCHEMA, log_extra={})
        _wait_host_signal(failed, host)
    finally:
        host.close()
    assert len(failures) == 1
    assert failures[0].startswith('checkpoint:missing thread failed: DatabaseError:')
    assert not path.exists()


def test_host_runs_one_thread_per_file_and_shares_state(tmp_path: Path) -> None:
    paths = [tmp_path / f'{index}.db' for index in range(3)]
    for index, path in enumerate(paths):
        _database(path, f'file-{index}')
    threads: list[threading.Thread] = []
    ready = threading.Event()

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        threads.append(threading.current_thread())
        if len(threads) == 3:
            ready.set()
        return Checkpointer(
            connection, path, interval_secs=interval_secs, logger=logger, log_extra=log_extra
        )

    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=0.02,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        checkpointer_factory=factory,
    )
    try:
        views = [
            host.register(path, f'file-{index}', _SCHEMA, log_extra={})
            for index, path in enumerate(paths)
        ]
        duplicate = host.register(
            paths[0].parent / '.' / paths[0].name, 'duplicate', _SCHEMA, log_extra={}
        )
        _wait_host_signal(ready, host)
        assert duplicate is views[0]
        assert {thread.name for thread in threads} == {
            'sg-event-daemon:checkpoint:file-0',
            'sg-event-daemon:checkpoint:file-1',
            'sg-event-daemon:checkpoint:file-2',
        }
        assert all(thread.daemon for thread in threads)
    finally:
        host.close()
    assert all(not thread.is_alive() for thread in threads)


def test_host_secure_open_failure_reports_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    failures: list[str] = []
    uncaught: list[BaseException | None] = []

    def exhausted(*_args: object, **_kwargs: object) -> int:
        raise OSError(errno.EMFILE, 'Too many open files')

    monkeypatch.setattr(platform_module.os, 'open', exhausted)

    def capture_uncaught(args: threading.ExceptHookArgs) -> None:
        uncaught.append(args.exc_value)

    monkeypatch.setattr(threading, 'excepthook', capture_uncaught)
    host = host_module.CheckpointHost(
        logger=_LOGGER, interval_secs=1, synchronous='NORMAL', on_failure=failures.append
    )
    try:
        host.register(tmp_path / 'unopened.db', 'secure-open', _SCHEMA, log_extra={})
        for thread in host.threads():
            thread.join()
            assert not thread.is_alive()
    finally:
        host.close()
    assert len(failures) == 1
    assert failures[0].startswith('checkpoint:secure-open thread failed: SecurityError:')
    assert 'Too many open files' in failures[0]
    assert uncaught == []
    assert not (tmp_path / 'unopened.db').exists()


def test_host_staggers_new_files_without_restarting_existing_timers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = [tmp_path / f'{index}.db' for index in range(3)]
    attempts: dict[Path, float] = {}
    changed = threading.Condition()

    def record_due(_host: host_module.CheckpointHost, file: host_module._CheckpointFile) -> None:
        with changed:
            attempts[file['path']] = file['first_due']
            changed.notify_all()

    monkeypatch.setattr(host_module.CheckpointHost, '_run', record_due)
    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=0.12,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        clock=lambda: 0.0,
    )
    try:
        for index, path in enumerate(paths):
            host.register(path, f'file-{index}', _SCHEMA, log_extra={})
        with changed:
            changed.wait_for(lambda: len(attempts) == 3)
        assert [attempts[path.resolve()] for path in paths] == pytest.approx([0.12, 0.18, 0.20])
    finally:
        host.close()


def test_view_is_not_stalled_before_the_first_attempt(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'state')
    host = host_module.CheckpointHost(
        logger=_LOGGER, interval_secs=60, synchronous='NORMAL', on_failure=pytest.fail
    )
    try:
        view = host.register(path, 'state', _SCHEMA, log_extra={})
        assert not view.stalled()
    finally:
        host.close()


@pytest.fixture
def trigger_database(
    tmp_path: Path,
) -> Generator[tuple[Path, sqlite3.Connection, sqlite3.Connection]]:
    path = tmp_path / 'trigger.db'
    with (
        open_database(path, 'trigger', _SCHEMA, synchronous='NORMAL') as writer,
        closing(sqlite3.connect(path, isolation_level=None)) as reader,
    ):
        writer.connection.execute('PRAGMA wal_autocheckpoint=0')
        writer.connection.execute('INSERT INTO entries DEFAULT VALUES')
        reader.execute('BEGIN')
        reader.execute('SELECT * FROM entries').fetchall()
        try:
            yield path, writer.connection, reader
        finally:
            reader.rollback()


def _trigger_factory(
    attempts: list[CheckpointAttempt], changed: threading.Condition, *, busy_reuses: int = 0
) -> Callable[..., Checkpointer]:
    class _ObservedCheckpointer(Checkpointer):
        def run_once(self) -> CheckpointAttempt:
            attempt = super().run_once()
            with changed:
                attempts.append(attempt)
                changed.notify_all()
            return attempt

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        def reuse() -> tuple[int, int, int]:
            if len(attempts) < busy_reuses:
                return 1, -1, -1
            row = connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
            assert row == (0, 0, 0)
            return 0, 0, 0

        return _ObservedCheckpointer(
            connection,
            path,
            interval_secs=interval_secs,
            logger=logger,
            log_extra=log_extra,
            run_reuse=reuse,
        )

    return factory


@pytest.mark.parametrize('boundary', ['factory', 'attempt', 'close', 'attempt-and-close'])
def test_host_ordinary_failure_preserves_primary_and_closes(
    boundary: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'ordinary')
    failures: list[str] = []
    closed: list[sqlite3.Connection] = []
    primary = ValueError('primary checkpoint failure')
    original_close = DatabaseHandle.close

    def close(database: DatabaseHandle) -> None:
        original_close(database)
        closed.append(database.connection)
        if boundary in {'close', 'attempt-and-close'}:
            raise OSError('cleanup failure')

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        if boundary == 'factory':
            raise primary

        def checkpoint() -> tuple[int, int, int]:
            raise primary

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            logger=logger,
            log_extra=log_extra,
            run_checkpoint=checkpoint,
        )

    monkeypatch.setattr(DatabaseHandle, 'close', close)
    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=1,
        synchronous='NORMAL',
        on_failure=failures.append,
        checkpointer_factory=factory,
    )
    if boundary == 'close':
        host.request_stop()
    host._run(
        {
            'path': path,
            'identity': 'ordinary',
            'schema': _SCHEMA,
            'first_due': 0,
            'status': {'lock': threading.Lock(), 'stalled': False},
            'log_extra': {},
        }
    )
    assert len(failures) == 1
    assert (
        'OSError: cleanup failure'
        if boundary == 'close'
        else 'ValueError: primary checkpoint failure'
    ) in failures[0]
    assert len(closed) == 1
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        closed[0].execute('SELECT 1')
    if boundary == 'attempt-and-close':
        assert any('cleanup failure' in note for note in primary.__notes__)


@pytest.mark.parametrize('control', [KeyboardInterrupt, SystemExit])
def test_host_process_control_preserves_primary_through_cleanup(
    control: type[BaseException], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'control')
    failures: list[str] = []
    primary = control('process control')
    closed: list[sqlite3.Connection] = []
    original_close = DatabaseHandle.close

    def close(database: DatabaseHandle) -> None:
        original_close(database)
        closed.append(database.connection)
        raise OSError('secondary cleanup failure')

    def factory(*_args: object, **_kwargs: object) -> Checkpointer:
        raise primary

    monkeypatch.setattr(DatabaseHandle, 'close', close)
    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=1,
        synchronous='NORMAL',
        on_failure=failures.append,
        checkpointer_factory=factory,
    )
    with pytest.raises(control) as caught:
        host._run(
            {
                'path': path,
                'identity': 'control',
                'schema': _SCHEMA,
                'first_due': 0,
                'status': {'lock': threading.Lock(), 'stalled': False},
                'log_extra': {},
            }
        )
    assert caught.value is primary
    assert failures == []
    assert len(closed) == 1
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        closed[0].execute('SELECT 1')
    assert any('secondary cleanup failure' in note for note in primary.__notes__)


def _run_trigger_steps(
    path: Path,
    attempts: list[CheckpointAttempt],
    trigger: int,
    step: Callable[[int], bool],
    *,
    busy_reuses: int = 0,
) -> None:
    class Schedule(threading.Event):
        index = 0

        def wait(self, timeout: float | None = None) -> bool:
            assert timeout == pytest.approx(0.1)
            stop = step(self.index)
            self.index += 1
            return stop

    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=30,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        wal_trigger_bytes=trigger,
        clock=lambda: 0.0,
        checkpointer_factory=_trigger_factory(
            attempts, threading.Condition(), busy_reuses=busy_reuses
        ),
    )
    host._stop = Schedule()
    host._run(
        {
            'path': path,
            'identity': 'trigger',
            'schema': _SCHEMA,
            'first_due': 30,
            'status': {'lock': threading.Lock(), 'stalled': False},
            'log_extra': {},
        }
    )


def test_host_attempts_when_the_wal_passes_the_trigger(
    trigger_database: tuple[Path, sqlite3.Connection, sqlite3.Connection],
) -> None:
    path, writer, reader = trigger_database
    reader.rollback()
    attempts: list[CheckpointAttempt] = []
    trigger = 64 * 1024

    def step(index: int) -> bool:
        if index == 0:
            assert not attempts
            assert wal_file_bytes(path) < trigger
        elif index == 1:
            assert not attempts
            _grow_wal(writer, path, trigger)
        else:
            assert len(attempts) == 1
            assert attempts[0].reuse == 'reused'
            assert wal_file_bytes(path) == 0
            return True
        return False

    _run_trigger_steps(path, attempts, trigger, step)


def test_host_rearms_the_trigger_after_a_busy_reuse(
    trigger_database: tuple[Path, sqlite3.Connection, sqlite3.Connection],
) -> None:
    path, writer, _reader = trigger_database
    attempts: list[CheckpointAttempt] = []
    trigger = 64 * 1024

    def step(index: int) -> bool:
        if index == 0:
            _grow_wal(writer, path, trigger)
        elif index == 1:
            assert len(attempts) == 1
            assert attempts[0].reuse == 'reuse-busy'
            sampled = attempts[0].wal_bytes
            below = _grow_wal(writer, path, sampled + trigger // 2)
            assert sampled < below < sampled + trigger
        elif index == 2:
            assert len(attempts) == 1
            _grow_wal(writer, path, attempts[0].wal_bytes + trigger)
        elif index == 3:
            assert len(attempts) == 2
            assert attempts[1].reuse == 'reuse-busy'
        else:
            assert len(attempts) == 2
            return True
        return False

    _run_trigger_steps(path, attempts, trigger, step, busy_reuses=2)


def test_host_returns_the_trigger_to_its_base_after_a_reuse(
    trigger_database: tuple[Path, sqlite3.Connection, sqlite3.Connection],
) -> None:
    path, writer, reader = trigger_database
    attempts: list[CheckpointAttempt] = []
    trigger = 64 * 1024
    threshold = [trigger]

    def step(index: int) -> bool:
        assert len(attempts) == index
        if index in (1, 2):
            assert attempts[-1].reuse == 'reuse-busy'
            threshold[0] = attempts[-1].wal_bytes + trigger
        if index == 2:
            reader.rollback()
        if index >= 3:
            assert attempts[-1].reuse == 'reused'
            assert wal_file_bytes(path) == 0
            if index == 4:
                return True
            size = _grow_wal(writer, path, trigger)
            assert size < threshold[0]
        else:
            _grow_wal(writer, path, threshold[0])
        return False

    _run_trigger_steps(path, attempts, trigger, step, busy_reuses=2)


def test_host_trigger_preserves_the_original_scheduled_time(
    trigger_database: tuple[Path, sqlite3.Connection, sqlite3.Connection],
) -> None:
    path, writer, reader = trigger_database
    reader.rollback()
    trigger = 64 * 1024
    clock = [0.0]
    starts: list[float] = []
    deadlines: list[float] = []

    class Schedule(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            assert timeout is not None
            if len(starts) == 2:
                return True
            deadlines.append(clock[0] + timeout)
            if clock[0] == 0:
                _grow_wal(writer, path, trigger)
            clock[0] += timeout
            return False

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        def checkpoint() -> tuple[int, int, int]:
            starts.append(clock[0])
            row = connection.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
            assert row is not None
            return row[0], row[1], row[2]

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            logger=logger,
            log_extra=log_extra,
            clock=lambda: clock[0],
            run_checkpoint=checkpoint,
        )

    host = host_module.CheckpointHost(
        logger=_LOGGER,
        interval_secs=30,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        wal_trigger_bytes=trigger,
        clock=lambda: clock[0],
        checkpointer_factory=factory,
    )
    host._stop = Schedule()
    host._run(
        {
            'path': path,
            'identity': 'trigger',
            'schema': _SCHEMA,
            'first_due': 30,
            'status': {'lock': threading.Lock(), 'stalled': False},
            'log_extra': {},
        }
    )
    assert starts == pytest.approx([0.1, 30])
    assert deadlines[0] == pytest.approx(0.1)
    assert (
        max(right - left for left, right in zip([0.0, *deadlines[:-1]], deadlines, strict=True))
        <= 0.1 + 1e-12
    )
    assert wal_file_bytes(path) == 0
