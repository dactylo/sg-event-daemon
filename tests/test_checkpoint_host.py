from __future__ import annotations

import errno
import sqlite3
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

import pytest

from sg_event_daemon import _checkpoint_host as host_module
from sg_event_daemon import _platform as platform_module
from sg_event_daemon._checkpoint import Checkpointer
from sg_event_daemon._sqlite import DatabaseHandle, Schema, open_database

_SCHEMA = Schema(version=1, statements=('CREATE TABLE entries (id INTEGER PRIMARY KEY)',))


def _database(path: Path, identity: str) -> None:
    with open_database(path, identity, _SCHEMA, synchronous='NORMAL'):
        pass


def test_checkpoint_runs_on_the_host_thread_with_its_own_connection(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    writer = open_database(path, 'owner', _SCHEMA, synchronous='NORMAL')
    finished = threading.Event()
    observed: list[tuple[bool, str, tuple[int, ...]]] = []

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
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
            connection, path, interval_secs=interval_secs, run_checkpoint=checkpoint
        )

    host = host_module.CheckpointHost(
        interval_secs=0.02,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        checkpointer_factory=factory,
    )
    try:
        writer.connection.execute('INSERT INTO entries VALUES (1)')
        host.register(path, 'owner', _SCHEMA)
        assert finished.wait(1)
    finally:
        host.close()
        writer.close()
    assert observed[0] == (False, 'sg-event-daemon:checkpoint:owner', (1,))


def test_host_attempts_on_its_own_timer_without_component_activity(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'timer')
    attempts: list[float] = []

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        def checkpoint() -> tuple[int, int, int]:
            attempts.append(time.monotonic())
            return 0, 0, 0

        return Checkpointer(
            connection, path, interval_secs=interval_secs, run_checkpoint=checkpoint
        )

    host = host_module.CheckpointHost(
        interval_secs=0.05,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        checkpointer_factory=factory,
    )
    started = time.monotonic()
    try:
        host.register(path, 'timer', _SCHEMA)
        time.sleep(0.5)
    finally:
        host.close()
    assert len(attempts) >= 6
    assert attempts[0] - started >= 0.05


def test_host_close_interrupts_waits_and_closes_connections(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'close')
    connections: list[sqlite3.Connection] = []
    ready = threading.Event()

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        connections.append(connection)
        ready.set()
        return Checkpointer(connection, path, interval_secs=interval_secs)

    host = host_module.CheckpointHost(
        interval_secs=60,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        checkpointer_factory=factory,
    )
    host.register(path, 'close', _SCHEMA)
    assert ready.wait(1)
    started = time.monotonic()
    host.close()
    host.close()
    assert time.monotonic() - started < 1
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
            clock[0] = wake
            return False

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        def checkpoint() -> tuple[int, int, int]:
            starts.append(clock[0])
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            clock=lambda: clock[0],
            run_checkpoint=checkpoint,
        )

    host = host_module.CheckpointHost(
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
            'status': {'lock': threading.Lock(), 'state': 'idle'},
        }
    )
    assert starts == [0.095, 0.100, 0.275, 0.300]
    assert deadlines == pytest.approx([0.05, 0.10, 0.15, 0.30, 0.35])
    # The delayed 50 ms slot can start only 5 ms before the next grid slot.
    assert starts[1] - starts[0] == pytest.approx(0.005)


def test_host_halt_calls_on_failure_once_with_identity_and_bound(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'contended')
    failures: list[str] = []
    failed = threading.Event()

    def failure(reason: str) -> None:
        failures.append(reason)
        failed.set()

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            run_checkpoint=lambda: (1, 5, 0),
            random_value=lambda: 0,
        )

    host = host_module.CheckpointHost(
        interval_secs=0.02, synchronous='NORMAL', on_failure=failure, checkpointer_factory=factory
    )
    try:
        view = host.register(path, 'contended', _SCHEMA)
        assert failed.wait(1)
        assert view.state() == 'incomplete'
    finally:
        host.close()
    assert len(failures) == 1
    assert failures[0].startswith('checkpoint:contended thread failed: CheckpointBoundError:')
    assert 'incomplete duration bound exceeded' in failures[0]
    assert str(path) in failures[0]


def test_host_missing_file_reports_failure_without_creating_it(tmp_path: Path) -> None:
    path = tmp_path / 'missing.db'
    failures: list[str] = []
    failed = threading.Event()

    def failure(reason: str) -> None:
        failures.append(reason)
        failed.set()

    host = host_module.CheckpointHost(interval_secs=0.02, synchronous='NORMAL', on_failure=failure)
    try:
        host.register(path, 'missing', _SCHEMA)
        assert failed.wait(1)
    finally:
        host.close()
    assert len(failures) == 1
    assert failures[0].startswith('checkpoint:missing thread failed: DatabaseError:')
    assert not path.exists()


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
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        if boundary == 'factory':
            raise primary

        def checkpoint() -> tuple[int, int, int]:
            raise primary

        return Checkpointer(
            connection, path, interval_secs=interval_secs, run_checkpoint=checkpoint
        )

    monkeypatch.setattr(DatabaseHandle, 'close', close)
    host = host_module.CheckpointHost(
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
            'status': {'lock': threading.Lock(), 'state': 'idle'},
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
                'status': {'lock': threading.Lock(), 'state': 'idle'},
            }
        )
    assert caught.value is primary
    assert failures == []
    assert len(closed) == 1
    with pytest.raises(sqlite3.ProgrammingError, match='closed'):
        closed[0].execute('SELECT 1')
    assert any('secondary cleanup failure' in note for note in primary.__notes__)


def test_host_reports_idle_before_first_attempt_then_complete(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    _database(path, 'state')
    host = host_module.CheckpointHost(
        interval_secs=0.05, synchronous='NORMAL', on_failure=pytest.fail
    )
    try:
        view = host.register(path, 'state', _SCHEMA)
        assert view.state() == 'idle'
        deadline = time.monotonic() + 1
        while view.state() == 'idle' and time.monotonic() < deadline:
            time.sleep(0.001)
        assert view.state() == 'complete'
    finally:
        host.close()


def test_host_runs_one_thread_per_file_and_shares_state(tmp_path: Path) -> None:
    paths = [tmp_path / f'{index}.db' for index in range(3)]
    for index, path in enumerate(paths):
        _database(path, f'file-{index}')
    threads: list[threading.Thread] = []
    ready = threading.Event()

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        threads.append(threading.current_thread())
        if len(threads) == 3:
            ready.set()
        return Checkpointer(connection, path, interval_secs=interval_secs)

    host = host_module.CheckpointHost(
        interval_secs=0.02,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        checkpointer_factory=factory,
    )
    try:
        views = [host.register(path, f'file-{index}', _SCHEMA) for index, path in enumerate(paths)]
        duplicate = host.register(paths[0].parent / '.' / paths[0].name, 'duplicate', _SCHEMA)
        assert ready.wait(1)
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
        interval_secs=1, synchronous='NORMAL', on_failure=failures.append
    )
    try:
        host.register(tmp_path / 'unopened.db', 'secure-open', _SCHEMA)
        for thread in host.threads():
            thread.join(1)
            assert not thread.is_alive()
    finally:
        host.close()
    assert len(failures) == 1
    assert failures[0].startswith('checkpoint:secure-open thread failed: SecurityError:')
    assert 'Too many open files' in failures[0]
    assert uncaught == []
    assert not (tmp_path / 'unopened.db').exists()


def test_host_staggers_new_files_without_restarting_existing_timers(tmp_path: Path) -> None:
    paths = [tmp_path / f'{index}.db' for index in range(3)]
    for index, path in enumerate(paths):
        _database(path, f'file-{index}')
    attempts: dict[Path, float] = {}
    lock = threading.Lock()
    ready = threading.Event()

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        def checkpoint() -> tuple[int, int, int]:
            with lock:
                attempts.setdefault(path, time.monotonic())
                if len(attempts) == 3:
                    ready.set()
            return 0, 0, 0

        return Checkpointer(
            connection, path, interval_secs=interval_secs, run_checkpoint=checkpoint
        )

    host = host_module.CheckpointHost(
        interval_secs=0.12,
        synchronous='NORMAL',
        on_failure=pytest.fail,
        checkpointer_factory=factory,
    )
    registered: list[float] = []
    try:
        for index, path in enumerate(paths):
            registered.append(time.monotonic())
            host.register(path, f'file-{index}', _SCHEMA)
        assert ready.wait(2)
    finally:
        host.close()
    for path, started, delay in zip(paths, registered, (0.12, 0.18, 0.20), strict=True):
        assert attempts[path] - started >= delay
