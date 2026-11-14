from __future__ import annotations

import json
import logging
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing
from typing import TYPE_CHECKING, Any

import pytest

from sg_event_daemon import _sqlite
from sg_event_daemon._checkpoint import Checkpointer
from sg_event_daemon._group_types import AuditIdentity
from sg_event_daemon._ipc import OperatorCommand, StopCommand
from sg_event_daemon._sqlite import DatabaseError, wal_file_bytes
from sg_event_daemon._watchdog import ComponentWatchdog
from sg_event_daemon._write_lock import SourceWriteLock, WriteContext, WriteLockStoppedError
from tests import _worker_processes
from tests._signals import read_complete_lines
from tests._storage_support import source_database, write_context_for
from tests._subprocess_support import subprocess_env
from tests._worker_processes import ChildPatches, Topology, running_supervisor, write_daemon_config

if TYPE_CHECKING:
    from pathlib import Path

    from sg_event_daemon._checkpoint import _CheckpointResult
    from tests._worker_processes import SupervisorProbe, WorkerSpawn


def _assert_pause_status(probe: SupervisorProbe) -> None:
    probe.wait_for(
        lambda: all(
            'write-lock-stalled' in component.conditions
            for health in probe.latest_health.values()
            for component in (
                *health.sources,
                *(group for group in health.groups if group.group != 'operator'),
            )
        )
    )
    status = probe.supervisor.status_mapping({})
    assert status['primary_state'] == 'write-lock-stalled'
    assert not status['readiness']
    assert status['liveness']
    conditions = {
        item.group: item.conditions
        for health in probe.latest_health.values()
        for item in health.groups
    }
    assert all('write-lock-stalled' in conditions[name] for name in ('local', 'remote'))
    warnings = [
        line
        for spawn in probe.spawns
        for line in spawn.log_path.read_text().splitlines()
        if 'paused: the WAL file' in line
    ]
    assert len(warnings) == 3
    # The source name remains readable even though it is also a credential profile name.
    for logger, owner in (
        ('source', 'site=test source=admin'),
        ('group', 'site=test source=admin group=local'),
        ('group', 'site=test source=admin group=remote'),
    ):
        assert (
            sum(
                f' WARNING sg_event_daemon.{logger}:' in line and line.endswith(f' {owner}')
                for line in warnings
            )
            == 1
        )


def _commit_marker(connection: sqlite3.Connection, path: Path, event_id: int, size: int) -> None:
    writes = write_context_for(path)
    try:
        with writes.transaction(connection):
            connection.execute(
                'INSERT INTO callback_marks VALUES (?, ?, 0, ?, ?, ?, 1)',
                ('checkpoint-probe', event_id, 'probe', 'x' * size, 'success'),
            )
    finally:
        writes.lock.close()


def _restart_remote(probe: SupervisorProbe, remote: WorkerSpawn) -> WorkerSpawn:
    remote.process.send_signal(signal.SIGKILL)
    probe.wait_for(lambda: len(probe.spawns) == 3, expected_exits=(remote.spawn_instance,))
    replacement = probe.spawns[-1]
    assert replacement.worker_id == remote.worker_id
    assert replacement.spawn_instance != remote.spawn_instance
    probe.wait_for(
        lambda: (
            remote.worker_id in probe.latest_health
            and probe.latest_health[remote.worker_id].worker.spawn_instance
            == replacement.spawn_instance
        ),
    )
    return replacement


def _set_event_frontier(root: Path, frontier: int) -> None:
    pending = root / 'visible-events.pending'
    pending.write_text(str(frontier))
    pending.replace(root / 'visible-events')


def _growth_config(tmp_path: Path, *, hold_callbacks: bool = False) -> Path:
    _set_event_frontier(tmp_path, 1)
    if hold_callbacks:
        (tmp_path / 'growth-callback-hold').touch()
    config = write_daemon_config(
        tmp_path,
        Topology('dedicated', ('local', 'remote', 'operator'), checkpoint_interval_secs=0.2),
    )
    config.write_text(
        config.read_text()
        .replace(
            '[sites.test.groups.local]\nworker = "dedicated"',
            '[sites.test.groups.local]\nworker = "default"',
        )
        .replace(
            '[sites.test.groups.operator]\nworker = "dedicated"',
            '[sites.test.groups.operator]\nworker = "default"',
        )
    )
    return config


def _growth_ready(probe: SupervisorProbe) -> WorkerSpawn:
    root = probe.spawns[0].log_path.parent
    probe.wait_for(
        lambda: (
            len(probe.latest_health) == 2
            and {
                group.group
                for health in probe.latest_health.values()
                for group in health.groups
                if group.join_phase == 'attached'
            }
            == {'local', 'remote', 'operator'}
        )
    )
    (root / 'events-ready').touch()
    _set_event_frontier(root, 2)
    probe.wait_for(
        lambda: (
            all(probe.records(group) for group in ('local', 'remote'))
            and len(probe.latest_health) == 2
        ),
    )
    result = probe.supervisor.dispatch_operator_command(
        OperatorCommand(
            request_id='park-probe',
            command='park',
            arguments={'site': 'test', 'group': 'operator', 'reason': 'probe'},
            identity=AuditIdentity.current(),
        ),
        timeout=float('inf'),
    )
    assert result.ok
    site = next(spawn for spawn in probe.spawns if json.loads(spawn.worker_id)[0] == 'site')
    if (root / 'growth-callback-hold').exists():
        probe.wait_for(
            lambda: all(
                (root / f'growth-callback-{group}-held').exists() for group in ('local', 'remote')
            )
        )
    return site


def _settle(probe: SupervisorProbe, seconds: float) -> None:
    # Negative-only observation: correct code passes at any scheduler speed.
    until = time.monotonic() + seconds
    probe.wait_for(lambda: time.monotonic() >= until)


def _stalled(probe: SupervisorProbe) -> bool:
    return any(
        'checkpoint-stalled' in source.conditions
        for health in probe.latest_health.values()
        for source in health.sources
    )


def _start_remote_delivery(probe: SupervisorProbe, root: Path) -> WorkerSpawn:
    probe.wait_for(
        lambda: (
            len(probe.latest_health) == 2
            and any(
                group.group == 'remote' and group.join_phase == 'attached'
                for health in probe.latest_health.values()
                for group in health.groups
            )
        ),
    )
    (root / 'events-ready').touch()
    _set_event_frontier(root, 2)
    probe.wait_for(lambda: bool(probe.records('remote')))
    return next(spawn for spawn in probe.spawns if json.loads(spawn.worker_id)[0] == 'site')


def _trace(spawn: WorkerSpawn, suffix: str) -> list[dict[str, Any]]:
    path = spawn.log_path.with_suffix(suffix)
    return [json.loads(line) for line in read_complete_lines(path)]


def _assert_incomplete_attempts_sync(site: WorkerSpawn) -> None:
    incomplete = [
        row
        for row in _trace(site, '.checkpoints')
        if row['reuse'] == 'reuse-busy' and row['outcome'] not in {'complete', 'reset-observed'}
    ]
    assert incomplete
    assert all(row['synced'] for row in incomplete)


def _pause_rows(probe: SupervisorProbe) -> list[dict[str, Any]]:
    return [row for spawn in probe.spawns for row in _trace(spawn, '.wal-writes')]


def _assert_recovered(probe: SupervisorProbe, after: float) -> None:
    names = {'source:admin', 'lane:local', 'lane:remote'}
    probe.wait_for(
        lambda: (
            names
            <= {
                row['thread']
                for row in _pause_rows(probe)
                if row['phase'] == 'commit' and row['at'] > after
            }
        ),
    )
    probe.wait_for(
        lambda: all(
            'write-lock-stalled' not in component.conditions
            for health in probe.latest_health.values()
            for component in (*health.sources, *health.groups)
        ),
    )
    for group in ('local', 'remote'):
        records = probe.records(group)
        assert len(records) == len(set(records))
    assert not probe.terminations
    assert all(spawn.process.poll() is None for spawn in probe.spawns)


def _wait_for_pause(probe: SupervisorProbe, names: tuple[str, ...]) -> None:
    probe.wait_for(
        lambda: (
            set(names) <= {row['thread'] for row in _pause_rows(probe) if row['phase'] == 'pause'}
        ),
    )


def test_checkpoint_trace_preserves_each_try_and_sqlite_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    passive = Checkpointer._passive_checkpoint
    contended = [(1, -1, -1)]

    def first_try_is_busy(checkpointer: Checkpointer) -> _CheckpointResult:
        return contended.pop() if contended else passive(checkpointer)

    monkeypatch.setattr(Checkpointer, '_passive_checkpoint', first_try_is_busy)
    log_path = tmp_path / 'worker.log'
    _worker_processes.install_checkpoints(monkeypatch, log_path)
    path = tmp_path / 'checkpoint.db'
    with (
        closing(sqlite3.connect(path)) as connection,
        closing(
            Checkpointer(
                connection,
                path,
                interval_secs=1,
                logger=logging.getLogger('checkpoint-test'),
                log_extra={},
            )
        ) as checkpointer,
    ):
        attempt = checkpointer.run_once()
        assert attempt.outcome == 'contended'
        assert attempt.reuse == 'reused'
        connection.close()
        with pytest.raises(DatabaseError, match='closed database'):
            checkpointer.run_once()
    rows = [
        json.loads(line)
        for line in log_path.with_suffix('.checkpoint-tries').read_text().splitlines()
    ]
    assert [row['result'] for row in rows] == [[1, -1, -1], None]
    assert 'closed database' in rows[-1]['error']


@pytest.mark.slow
def test_crossed_bound_warns_once_and_reaches_status(tmp_path: Path) -> None:
    config = write_daemon_config(
        tmp_path, Topology('dedicated', ('remote',), checkpoint_interval_secs=0.2)
    )
    path = source_database(tmp_path / 'state')
    _set_event_frontier(tmp_path, 1)
    with running_supervisor(
        config, ChildPatches(checkpoint_clock=True, gate_events=True)
    ) as probe:
        probe.wait_for(
            lambda: (
                len(probe.latest_health) == 2
                and any(
                    group.join_phase == 'attached'
                    for health in probe.latest_health.values()
                    for group in health.groups
                )
            )
        )
        (tmp_path / 'events-ready').touch()
        _set_event_frontier(tmp_path, 2)
        probe.wait_for(lambda: bool(probe.records('remote')))
        site = next(spawn for spawn in probe.spawns if json.loads(spawn.worker_id)[0] == 'site')
        with (
            closing(sqlite3.connect(path, isolation_level=None)) as reader,
            closing(sqlite3.connect(path, isolation_level=None)) as writer,
        ):
            writer.execute('PRAGMA wal_autocheckpoint=0')
            for crossing in range(2):
                reader.execute('BEGIN')
                reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
                _commit_marker(writer, path, crossing + 1, 1000)
                probe.wait_for(lambda: _stalled(probe))
                delivered = len(probe.records('remote'))
                _set_event_frontier(tmp_path, crossing + 3)
                probe.wait_for(
                    lambda delivered=delivered: len(probe.records('remote')) > delivered
                )
                assert _stalled(probe)
                status = probe.supervisor.status_mapping({})
                assert status['primary_state'] == 'checkpoint-stalled'
                assert not status['readiness']
                assert status['liveness']
                assert len(probe.spawns) == 2
                assert not probe.terminations
                warnings = [
                    line
                    for line in site.log_path.read_text().splitlines()
                    if 'bound exceeded' in line
                ]
                assert len(warnings) == crossing + 1
                assert str(path.parent) in warnings[-1]
                assert 'recent attempts:' in warnings[-1]
                reader.execute('COMMIT')
                probe.wait_for(lambda: not _stalled(probe))
                assert (
                    len(
                        [
                            line
                            for line in site.log_path.read_text().splitlines()
                            if 'bound cleared' in line
                        ]
                    )
                    == crossing + 1
                )
            assert all(spawn.process.poll() is None for spawn in probe.spawns)


@pytest.mark.slow
def test_growth_crossing_keeps_every_worker_up(tmp_path: Path) -> None:
    config = write_daemon_config(
        tmp_path, Topology('dedicated', ('remote',), checkpoint_interval_secs=0.2)
    )
    path = source_database(tmp_path / 'state')
    _set_event_frontier(tmp_path, 1)
    with running_supervisor(
        config,
        ChildPatches(
            checkpoint_clock=True,
            last_event_id=100,
            checkpoint_wal_limit_bytes=1_000_000,
            gate_events=True,
        ),
    ) as probe:
        site = _start_remote_delivery(probe, tmp_path)
        with (
            closing(sqlite3.connect(path, isolation_level=None)) as reader,
            closing(sqlite3.connect(path, isolation_level=None)) as writer,
        ):
            writer.execute('PRAGMA wal_autocheckpoint=0')
            reader.execute('BEGIN')
            reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
            _commit_marker(writer, path, 1, 2_000_000)
            assert wal_file_bytes(path) > 1_000_000
            probe.wait_for(lambda: 'WAL growth bound exceeded' in site.log_path.read_text())
            reader.execute('COMMIT')
            probe.wait_for(lambda: 'WAL growth bound cleared' in site.log_path.read_text())
            assert wal_file_bytes(path) <= 1_000_000
        assert not probe.terminations
        assert len(probe.spawns) == 2
        assert all(spawn.process.poll() is None for spawn in probe.spawns)


@pytest.mark.slow
def test_bound_reaches_status_while_the_source_is_inside_a_provider_call(tmp_path: Path) -> None:
    topology = Topology('default', ('local',), checkpoint_interval_secs=0.2)
    config = write_daemon_config(tmp_path, topology)
    path = source_database(tmp_path / 'state')
    _set_event_frontier(tmp_path, 1)
    with running_supervisor(
        config,
        ChildPatches(
            checkpoint_clock=True, last_event_id=100, hold_provider=True, gate_events=True
        ),
    ) as probe:
        probe.wait_for(
            lambda: any(
                group.group == 'local' and group.join_phase == 'attached'
                for health in probe.latest_health.values()
                for group in health.groups
            ),
        )
        (tmp_path / 'events-ready').touch()
        _set_event_frontier(tmp_path, 2)
        probe.wait_for(lambda: bool(probe.records('local')))
        site = probe.spawns[0]
        (tmp_path / 'provider-arm').touch()
        try:
            probe.wait_for((tmp_path / 'provider-entered').exists)
            with (
                closing(sqlite3.connect(path, isolation_level=None)) as reader,
                closing(sqlite3.connect(path, isolation_level=None)) as writer,
            ):
                reader.execute('BEGIN')
                reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
                _commit_marker(writer, path, 1, 1000)
                probe.wait_for(lambda: 'bound exceeded' in site.log_path.read_text())
                probe.wait_for(lambda: _stalled(probe))
                status = probe.supervisor.status_mapping({})
                assert status['primary_state'] == 'checkpoint-stalled'
                assert not status['readiness']
                assert status['liveness']
                reader.execute('COMMIT')
                probe.wait_for(lambda: 'bound cleared' in site.log_path.read_text())
                probe.wait_for(lambda: not _stalled(probe))
                assert not (tmp_path / 'provider-release').exists()
                assert probe.supervisor.status_mapping({})['readiness']
                assert not probe.terminations
        finally:
            (tmp_path / 'provider-release').touch()


def test_process_assertion_preserves_child_logs(tmp_path: Path) -> None:
    config = write_daemon_config(tmp_path, Topology('default', ('group',)))

    def fail() -> None:
        with running_supervisor(
            config,
            ChildPatches(
                checkpoint_clock=True,
            ),
        ) as probe:
            with probe.spawns[0].log_path.open('a') as stream:
                stream.write('retained child failure\n')
            raise AssertionError('parent assertion')

    with pytest.raises(AssertionError, match='parent assertion') as caught:
        fail()
    assert 'retained child failure' in '\n'.join(caught.value.__notes__)


@pytest.mark.slow
def test_reader_in_another_process_warns_and_syncs_without_failing(tmp_path: Path) -> None:

    config = write_daemon_config(
        tmp_path, Topology('dedicated', ('remote',), checkpoint_interval_secs=0.2)
    )
    path = source_database(tmp_path / 'state')
    _set_event_frontier(tmp_path, 1)
    with running_supervisor(
        config, ChildPatches(checkpoint_clock=True, last_event_id=100, gate_events=True)
    ) as probe:
        site = _start_remote_delivery(probe, tmp_path)
        process = subprocess.Popen(
            [
                sys.executable,
                '-c',
                (
                    'import sqlite3,sys; c=sqlite3.connect(sys.argv[1],isolation_level=None); '
                    "c.execute('BEGIN'); "
                    "c.execute('SELECT COUNT(*) FROM source_meta').fetchone(); "
                    "print('PINNED',flush=True); sys.stdin.readline(); c.close()"
                ),
                str(path),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            env=subprocess_env(),
        )
        try:
            assert process.stdout is not None
            assert process.stdout.readline().strip() == 'PINNED'
            with closing(sqlite3.connect(path, isolation_level=None)) as writer:
                _commit_marker(writer, path, 1, 1000)
            probe.wait_for(lambda: _stalled(probe))
            syncs = site.log_path.with_suffix('.syncs')
            probe.wait_for(lambda: len(_trace(site, '.checkpoints')) >= 4)
            _assert_incomplete_attempts_sync(site)
            syncs_before_restart = len(read_complete_lines(syncs))
            attempts = _trace(site, '.checkpoints')
            assert attempts[-1]['reuse'] == 'reuse-busy'
            assert process.poll() is None
            assert not probe.terminations
            remote = next(spawn for spawn in probe.spawns if spawn is not site)
            # Force only the remote process to restart; the independent reader
            # and the site worker retain their original ownership throughout.
            replacement = _restart_remote(probe, remote)
            checkpoint_path = site.log_path.with_suffix('.checkpoints')
            after_restart_count = len(read_complete_lines(checkpoint_path))
            probe.wait_for(
                lambda: len(read_complete_lines(checkpoint_path)) > after_restart_count,
            )
            restart_attempts = [
                json.loads(line) for line in read_complete_lines(checkpoint_path)[len(attempts) :]
            ]
            assert restart_attempts
            assert all(attempt['reuse'] == 'reuse-busy' for attempt in restart_attempts)
            assert all(row['synced'] for row in restart_attempts)
            assert len(read_complete_lines(syncs)) > syncs_before_restart
            assert process.poll() is None
            assert all(spawn.process.poll() is None for spawn in (site, replacement))
            assert _stalled(probe)
            assert len(probe.terminations) == 1
            assert probe.terminations[0].spawn_instance == remote.spawn_instance
            assert process.stdin is not None
            process.stdin.write('\n')
            process.stdin.flush()
            assert process.wait() == 0
            probe.wait_for(lambda: not _stalled(probe))
            released_attempts = _trace(site, '.checkpoints')
            assert released_attempts[-1]['reuse'] == 'reused'
            assert all(spawn.process.poll() is None for spawn in (site, replacement))
            assert len(probe.spawns) == 3
            assert len(probe.terminations) == 1
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


def test_wal_pause_renews_enclosing_lane_internal_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / 'source.db'
    with (
        closing(sqlite3.connect(path, isolation_level=None)) as connection,
        closing(sqlite3.connect(path, isolation_level=None)) as reader,
    ):
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('CREATE TABLE entries (value TEXT)')
        reader.execute('BEGIN')
        reader.execute('SELECT COUNT(*) FROM entries').fetchone()
        connection.execute('INSERT INTO entries VALUES (?)', ('held',))
        assert wal_file_bytes(path) > 0
        stop = threading.Event()
        now = [100.0]

        def stop_during_pause(timeout: float | None = None) -> bool:
            assert timeout is not None
            now[0] += 1.0
            stop.set()
            return True

        monkeypatch.setattr(stop, 'wait', stop_during_pause)
        lock = SourceWriteLock(
            path.with_suffix('.db.write-lock'),
            database_path=path,
            timeout_secs=0.2,
            wait_phase_grace_secs=0.1,
            wal_limit_bytes=0,
            clock=lambda: now[0],
        )
        watchdog = ComponentWatchdog(
            kind='lane',
            identity='test',
            worker_instance='test',
            registration_token=path.name,
            startup_deadline_secs=1.0,
            clock=lambda: now[0],
        )
        writes = WriteContext(
            lock,
            watch=lambda phase, deadline: watchdog.watch(phase, deadline_at=deadline),
            stop=stop,
        )
        try:
            with watchdog.watch('internal', deadline_at=100.1):
                with pytest.raises(WriteLockStoppedError), writes.transaction(connection):
                    connection.execute('INSERT INTO entries VALUES (?)', ('later',))
                sample = watchdog.sample()
                assert now[0] == 101.0
                assert wal_file_bytes(path) > 0
                assert sample.phase == 'internal'
                assert sample.deadline_at is not None
                assert sample.heartbeat_at == 101.0
                assert sample.deadline_at == pytest.approx(101.1)
        finally:
            lock.close()


@pytest.mark.slow
@pytest.mark.parametrize('delayed_checkpoint', [False, True])
def test_writers_pause_at_the_growth_bound_until_reuse_empties_the_wal(
    tmp_path: Path,
    delayed_checkpoint: bool,
) -> None:
    config = _growth_config(tmp_path, hold_callbacks=True)
    path = source_database(tmp_path / 'state')
    limit = 1_000_000
    with running_supervisor(
        config,
        ChildPatches(checkpoint_clock=True, gate_events=True, writer_wal_limit_bytes=limit),
    ) as probe:
        site = _growth_ready(probe)
        with closing(sqlite3.connect(path, isolation_level=None)) as reader:
            reader.execute('BEGIN')
            reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
            pinned = time.monotonic()
            if delayed_checkpoint:
                (tmp_path / 'checkpoint-hold').touch()
                probe.wait_for((tmp_path / 'checkpoint-held').exists)
            try:
                probe.wait_for(lambda: wal_file_bytes(path) > limit)
                (tmp_path / 'growth-callback-release').touch()
                before = _pause_rows(probe)
                _wait_for_pause(probe, ('source:admin', 'lane:local', 'lane:remote'))
                after = _pause_rows(probe)
                crossed = min(
                    row['at']
                    for row in after
                    if row['phase'] == 'commit' and row['bytes'] > limit and row['at'] > pinned
                )
                for name in ('source:admin', 'lane:local', 'lane:remote'):
                    assert (
                        sum(
                            row['thread'] == name
                            and row['phase'] == 'commit'
                            and row['at'] > crossed
                            for row in after
                        )
                        <= 1
                    )
                    assert any(
                        row['thread'] == name and row['phase'] == 'commit' for row in before
                    )
                _wait_for_pause(probe, ('source:admin', 'lane:local', 'lane:remote'))
                paused = time.monotonic()
                _settle(probe, 0.3)
                assert not [
                    row
                    for row in _trace(site, '.queries')
                    if row['thread'] == 'source:admin' and row['at'] > paused
                ]
                assert not [
                    row
                    for row in _pause_rows(probe)
                    if row['phase'] == 'commit' and row['at'] > paused
                ]
            finally:
                (tmp_path / 'checkpoint-release').touch()
            probe.wait_for(lambda: _stalled(probe))
            before_syncs = len(read_complete_lines(site.log_path.with_suffix('.syncs')))
            before_attempts = len(_trace(site, '.checkpoints'))
            probe.wait_for(lambda: len(_trace(site, '.checkpoints')) >= before_attempts + 2)
            assert all(row['synced'] for row in _trace(site, '.checkpoints')[before_attempts:])
            assert (
                len(read_complete_lines(site.log_path.with_suffix('.syncs'))) >= before_syncs + 2
            )
            attempts = _trace(site, '.checkpoints')
            recent = [row['at'] for row in attempts if row['at'] > paused]
            assert len(recent) >= 2
            # Exact scheduled cadence is covered by the host's controlled-clock tests.
            _assert_pause_status(probe)
            result = probe.supervisor.dispatch_operator_command(
                OperatorCommand(
                    request_id='paused-unpark',
                    command='unpark',
                    arguments={'site': 'test', 'group': 'operator', 'reason': 'probe'},
                    identity=AuditIdentity.current(),
                ),
                timeout=float('inf'),
            )
            assert not result.ok
            assert result.result['code'] == 'source-database-busy'
            assert not probe.terminations
            released = time.monotonic()
            reader.execute('COMMIT')
            _assert_recovered(probe, released)
            assert any(
                row['result'][0] == 0 and row['bytes'] == 0
                for row in _trace(site, '.reuse')
                if row['at'] > released
            )


@pytest.mark.slow
def test_stop_while_paused_is_cooperative(tmp_path: Path) -> None:
    config = _growth_config(tmp_path, hold_callbacks=True)
    path = source_database(tmp_path / 'state')
    with running_supervisor(
        config,
        ChildPatches(checkpoint_clock=True, gate_events=True, writer_wal_limit_bytes=1_000_000),
    ) as probe:
        site = _growth_ready(probe)
        with closing(sqlite3.connect(path, isolation_level=None)) as reader:
            reader.execute('BEGIN')
            reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
            with closing(sqlite3.connect(path, isolation_level=None)) as writer:
                writer.execute(
                    'INSERT INTO callback_marks VALUES (?, ?, 0, ?, ?, ?, 1)',
                    ('stop-pause-probe', 999999, 'probe', 'x' * 1_500_000, 'success'),
                )
            assert wal_file_bytes(path) > 1_000_000
            (tmp_path / 'growth-callback-release').touch()
            _wait_for_pause(probe, ('source:admin', 'lane:local', 'lane:remote'))
            worker = probe.supervisor._spawns[site.worker_id].worker
            worker.send(StopCommand(reason='test-pause-stop'))
            assert site.process.wait() == 0
            assert 'thread failed' not in site.log_path.read_text()
            assert 'stuck' not in site.log_path.read_text()


@pytest.mark.slow
def test_open_after_a_crash_truncates_a_large_wal_before_the_first_write(tmp_path: Path) -> None:
    config = _growth_config(tmp_path)
    path = source_database(tmp_path / 'state')
    limit = 1_000_000
    with running_supervisor(
        config,
        ChildPatches(checkpoint_clock=True, writer_wal_limit_bytes=limit, gate_events=True),
    ) as probe:
        site = _growth_ready(probe)
        with closing(sqlite3.connect(path, isolation_level=None)) as reader:
            reader.execute('BEGIN')
            reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
            _set_event_frontier(tmp_path, 1000)
            probe.wait_for(lambda: wal_file_bytes(path) > limit)
            salt = _worker_processes.wal_salt(path)
            originals = tuple(spawn.spawn_instance for spawn in probe.spawns)
            site.process.send_signal(signal.SIGKILL)
            site.process.wait()
            reader.execute('COMMIT')
            probe.wait_for(
                lambda: any(
                    spawn.worker_id == site.worker_id and spawn is not site
                    for spawn in probe.spawns
                ),
                expected_exits=originals,
            )
            replacement = next(
                spawn
                for spawn in probe.spawns
                if spawn.worker_id == site.worker_id and spawn is not site
            )
            probe.wait_for(
                lambda: bool(_trace(replacement, '.wal-writes')), expected_exits=originals
            )
            probe.wait_for(
                lambda: any(
                    row['phase'] == 'commit' for row in _trace(replacement, '.wal-writes')
                ),
                expected_exits=originals,
            )
            rows = _trace(replacement, '.wal-writes')
            assert rows[0]['phase'] == 'begin'
            assert rows[0]['bytes'] == 0
            first = next(row for row in rows if row['phase'] == 'commit')
            assert first['salt'] != salt
            assert 0 < first['bytes'] < limit


@pytest.mark.slow
def test_direct_disposition_after_a_crash_truncates_before_the_first_write(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    seed = subprocess.run(
        [
            sys.executable,
            '-c',
            (
                'from pathlib import Path; '
                'from tests._worker_processes import run_crashed_disposition_seed; '
                'import sys; run_crashed_disposition_seed(Path(sys.argv[1]))'
            ),
            str(path),
        ],
        env=subprocess_env(),
        capture_output=True,
        text=True,
        check=True,
    )
    assert wal_file_bytes(path) > 400_000
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            (
                'from pathlib import Path; '
                'from tests._worker_processes import run_direct_disposition_probe; '
                'import sys; run_direct_disposition_probe(Path(sys.argv[1]), 400000)'
            ),
            str(path),
        ],
        env=subprocess_env(),
        capture_output=True,
        text=True,
        check=True,
    )
    evidence = json.loads(result.stdout)
    assert evidence['before'] == 0
    assert evidence['parked']
    assert 0 < evidence['after'] < 400_000
    assert evidence['salt'] != seed.stdout.strip()


@pytest.mark.slow
def test_spill_pauses_other_writers_until_truncation(tmp_path: Path) -> None:
    config = _growth_config(tmp_path)
    path = source_database(tmp_path / 'state')
    signals = tmp_path / 'signals'
    signals.mkdir()
    with running_supervisor(
        config,
        ChildPatches(
            checkpoint_clock=True,
            writer_wal_limit_bytes=1_000_000,
            signal_dir=signals,
            gate_lane_write=True,
            gate_events=True,
        ),
    ) as probe:
        site = _growth_ready(probe)
        (signals / 'arm').touch()
        _set_event_frontier(tmp_path, 3)
        probe.wait_for((signals / 'lane:local.queued').exists)
        with closing(sqlite3.connect(path, isolation_level=None)) as writer:
            writer.execute('PRAGMA cache_size=5')
            writer.execute('PRAGMA wal_autocheckpoint=0')
            writer.execute('BEGIN IMMEDIATE')
            writer.execute(
                'INSERT INTO callback_marks VALUES (?, ?, 0, ?, ?, ?, 1)',
                ('spill', 1, 'probe', 'x' * 1_500_000, 'success'),
            )
            assert wal_file_bytes(path) > 1_000_000
            spilled = time.monotonic()
            (signals / 'write-release').touch()
            try:
                _wait_for_pause(probe, ('source:admin', 'lane:local'))
                assert not [
                    row
                    for row in _pause_rows(probe)
                    if row['phase'] == 'commit' and row['at'] > spilled
                ]
                probe.wait_for(
                    lambda: any(
                        row['outcome'] == 'complete'
                        for row in _trace(site, '.checkpoints')
                        if row['at'] > spilled
                    ),
                )
                assert not probe.terminations
            finally:
                writer.execute('COMMIT')
            released = time.monotonic()
            _set_event_frontier(tmp_path, 4)
            _assert_recovered(probe, released)


@pytest.mark.slow
@pytest.mark.parametrize('release_contention', [False, True])
def test_contended_attempts_still_pause_writers(tmp_path: Path, release_contention: bool) -> None:
    config = _growth_config(tmp_path, hold_callbacks=True)
    path = source_database(tmp_path / 'state')
    with running_supervisor(
        config,
        ChildPatches(checkpoint_clock=True, gate_events=True, writer_wal_limit_bytes=1_000_000),
    ) as probe:
        site = _growth_ready(probe)
        (tmp_path / 'checkpoint-contended').touch()
        try:
            probe.wait_for(lambda: wal_file_bytes(path) > 1_000_000)
            (tmp_path / 'growth-callback-release').touch()
            _wait_for_pause(probe, ('source:admin', 'lane:local', 'lane:remote'))
            paused = time.monotonic()
            probe.wait_for(
                lambda: (
                    len([row for row in _trace(site, '.checkpoints') if row['at'] > paused]) >= 5
                    and 'incomplete-duration' in _trace(site, '.checkpoints')[-1]['crossed']
                )
            )
            attempts = [row for row in _trace(site, '.checkpoints') if row['at'] > paused]
            assert len(attempts) >= 5
            assert all(
                row['outcome'] == 'contended' and row['reuse'] == 'reuse-busy' for row in attempts
            )
            assert 'incomplete-duration' in attempts[-1]['crossed']
            assert not [
                row
                for row in _pause_rows(probe)
                if row['phase'] == 'commit' and row['at'] > paused
            ]
            assert not probe.terminations
            if release_contention:
                released = time.monotonic()
                (tmp_path / 'checkpoint-contended').unlink()
                _assert_recovered(probe, released)
                reuses = [row for row in _trace(site, '.reuse') if row['at'] > released]
                assert reuses[0]['result'][0] == 0
                assert reuses[0]['bytes'] == 0
        finally:
            (tmp_path / 'checkpoint-contended').unlink(missing_ok=True)


@pytest.mark.parametrize('pinned', [False, True])
def test_open_truncation_uses_bounded_wait_and_restores_busy_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pinned: bool,
) -> None:
    path = tmp_path / 'open.db'
    with (
        closing(sqlite3.connect(path, isolation_level=None)) as connection,
        closing(sqlite3.connect(path, isolation_level=None)) as reader,
    ):
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('CREATE TABLE entries (value TEXT)')
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        if pinned:
            reader.execute('BEGIN')
            reader.execute('SELECT * FROM entries').fetchall()
        connection.execute("INSERT INTO entries VALUES ('value')")
        monkeypatch.setattr(_sqlite, 'WAL_IN_USE_LIMIT_BYTES', 100, raising=False)
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        assert _sqlite.truncate_wal_over_bound(connection, path)
        assert statements.count('PRAGMA wal_checkpoint(TRUNCATE)') == 1
        assert 'PRAGMA busy_timeout=100' in statements
        assert connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)
        assert (wal_file_bytes(path) > 0) is pinned
        reader.rollback()
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        statements.clear()
        assert not _sqlite.truncate_wal_over_bound(connection, path)
        assert not statements
