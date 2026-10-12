from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

import pytest

from sg_event_daemon import _checkpoint_host, _worker
from sg_event_daemon._checkpoint import _WAL_IN_USE_LIMIT_BYTES, CheckpointBoundError, Checkpointer
from sg_event_daemon._config import load_config
from sg_event_daemon._sqlite import DatabaseError
from tests import _worker_processes
from tests._storage_support import source_database, write_context_for
from tests._worker_processes import (
    ChildPatches,
    SupervisorProbe,
    Topology,
    WorkerSpawn,
    running_supervisor,
    write_daemon_config,
)

if TYPE_CHECKING:
    from pathlib import Path

    from sg_event_daemon._checkpoint import CheckpointAttempt, CheckpointResult


class _Attempt(TypedDict):
    outcome: str
    result: list[int]
    at: float
    error: str
    pid: int
    path: str
    frame_bytes: int
    wal_limit_bytes: int


@dataclass(frozen=True)
class _Injection:
    root: Path
    reader: bool
    tripwire: bool


class _ObservedCheckpointer(Checkpointer):
    def __init__(
        self,
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        injection: _Injection,
    ) -> None:
        super().__init__(
            connection,
            path,
            interval_secs=interval_secs,
            wal_limit_bytes=1 if injection.tripwire else _WAL_IN_USE_LIMIT_BYTES,
        )
        self._root = injection.root
        self._reader_owner = injection.reader
        self._reader: sqlite3.Connection | None = None
        self._tripwire = injection.tripwire
        spawn = sys.argv[sys.argv.index('--spawn-instance') + 1]
        self._attempts_path = injection.root / f'{spawn}.attempts'
        self._attempts_path.with_suffix('.created').write_text(str(self._cleared_at))

    def run_once(self) -> CheckpointAttempt:
        if self._reader_owner and self._reader is None and (self._root / 'arm').exists():
            self._reader = sqlite3.connect(self._path, isolation_level=None)
            self._reader.execute('BEGIN')
            self._reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
            (self._root / 'reader-open').write_text(str(os.getpid()))
        if self._tripwire:
            gate = 'tripwire-arm'
            deadline = time.monotonic() + 15
            while not (self._root / gate).exists():
                if time.monotonic() >= deadline:
                    raise AssertionError(f'checkpoint injection gate {gate} never opened')
                time.sleep(0.01)
        try:
            attempt = super().run_once()
        except CheckpointBoundError as error:
            _record(self._attempts_path, error.attempt, str(error), self)
            if self._reader is not None:
                self._reader.close()
                self._reader = None
                (self._root / 'reader-closed').write_text(str(time.monotonic()))
            raise
        _record(self._attempts_path, attempt, '', self)
        return attempt


def _attempts(spawn: WorkerSpawn) -> list[_Attempt]:
    path = spawn.log_path.with_suffix('.attempts')
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text().splitlines(keepends=True)
        if line.endswith('\n')
    ]


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


def _install_children(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    original = _worker_processes.child_code

    def code(config_path: Path, patches: ChildPatches) -> str:
        child = original(config_path, patches)
        preamble, invocation = child.rsplit('run_worker(', 1)
        return (
            preamble
            + 'from tests.test_storage_checkpoints import install_child_checkpoints\n'
            + f'install_child_checkpoints(Path({str(root)!r}))\n'
            + 'run_worker('
            + invocation
        )

    monkeypatch.setattr(_worker_processes, 'child_code', code)


def _record(
    path: Path, attempt: CheckpointAttempt, error: str, checkpointer: Checkpointer
) -> None:
    with path.open('a', encoding='utf-8') as stream:
        stream.write(
            json.dumps(
                {
                    'outcome': attempt.outcome,
                    'result': attempt.result,
                    'at': attempt.finished_at,
                    'error': error,
                    'pid': os.getpid(),
                    'path': str(checkpointer._path),
                    'frame_bytes': checkpointer.frame_bytes,
                    'wal_limit_bytes': checkpointer._wal_limit_bytes,
                }
            )
            + '\n'
        )


def _replacement(probe: SupervisorProbe, original: WorkerSpawn) -> WorkerSpawn:
    probe.wait_for(
        lambda: any(
            spawn.worker_id == original.worker_id
            and spawn.spawn_instance != original.spawn_instance
            for spawn in probe.spawns
        ),
        15,
    )
    completed = json.loads(original.log_path.with_suffix('.completed').read_text())
    termination = next(
        item for item in probe.terminations if item.spawn_instance == original.spawn_instance
    )
    fatal = json.loads(original.log_path.with_suffix('.fatal').read_text())
    assert completed['result'] == completed['live'] == []
    assert not completed['monitor_alive']
    assert not completed['resources_open']
    assert completed['fatal'] == fatal['fatal']
    assert fatal['at'] <= completed['at'] <= termination.at
    assert termination.reason in {'worker-control-eof', 'worker-exit-1'}
    assert original.process.poll() == 1 or (
        original.process.poll() == -9 and termination.reason == 'worker-control-eof'
    ), termination
    replacement = next(
        spawn
        for spawn in probe.spawns
        if spawn.worker_id == original.worker_id
        and spawn.spawn_instance != original.spawn_instance
    )
    assert probe.breakers[original.worker_id]._failure_total == 1
    assert not probe.breakers[original.worker_id]._is_open
    assert not probe.park_evidence
    return replacement


def _wait_for_failure(probe: SupervisorProbe, original: WorkerSpawn, bound: str) -> _Attempt:
    probe.wait_for(
        lambda: bool(_attempts(original)) and bool(_attempts(original)[-1]['error']), 15
    )
    attempt = _attempts(original)[-1]
    assert bound in attempt['error']
    if bound == 'WAL growth':
        assert attempt['wal_limit_bytes'] == 1
        assert attempt['result'][1] * attempt['frame_bytes'] > attempt['wal_limit_bytes']
    probe.wait_for(lambda: probe.breakers[original.worker_id]._failure_total == 1, 10)
    breaker = probe.breakers[original.worker_id]
    assert breaker._last_failure_at is not None
    assert breaker._restart_deadline_at is not None
    assert breaker._restart_deadline_at - breaker._last_failure_at == 1
    assert sum(spawn.worker_id == original.worker_id for spawn in probe.spawns) == 1
    termination = next(
        item for item in probe.terminations if item.spawn_instance == original.spawn_instance
    )
    fatal = json.loads(original.log_path.with_suffix('.fatal').read_text())
    assert 'checkpoint:' in fatal['fatal']
    assert bound in fatal['fatal']
    assert fatal['at'] <= termination.at

    return attempt


def install_child_checkpoints(root: Path) -> None:
    worker_id = sys.argv[sys.argv.index('--worker') + 1]
    with_reader = (root / 'with-reader').exists()
    is_remote = json.loads(worker_id)[0] != 'site'
    target = is_remote if with_reader else True
    claim = root / f'{worker_id}.claimed'
    tripwire = target and not claim.exists()
    if tripwire:
        claim.touch()
    reader = with_reader and not is_remote and not (root / 'reader-open').exists()

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        return _ObservedCheckpointer(
            connection,
            path,
            interval_secs=interval_secs,
            injection=_Injection(root, reader, tripwire),
        )

    _checkpoint_host.Checkpointer = factory
    failed = _worker.WorkerRuntime._component_failed

    def record_failure(runtime: _worker.WorkerRuntime, failure: str) -> None:
        failed(runtime, failure)
        spawn = sys.argv[sys.argv.index('--spawn-instance') + 1]
        (root / f'{spawn}.fatal').write_text(
            json.dumps(
                {
                    'at': time.monotonic(),
                    'fatal': runtime.fatal_reason,
                    'deadline': runtime._shutdown_deadline_at,
                }
            )
        )

    pytest.MonkeyPatch().setattr(_worker.WorkerRuntime, '_component_failed', record_failure)

    wait = _worker.WorkerRuntime.wait

    def record_completion(runtime: _worker.WorkerRuntime) -> tuple[str, ...]:
        result = wait(runtime)
        spawn = sys.argv[sys.argv.index('--spawn-instance') + 1]
        monitor = runtime._monitor_thread
        (root / f'{spawn}.completed').write_text(
            json.dumps(
                {
                    'at': time.monotonic(),
                    'result': result,
                    'fatal': runtime.fatal_reason,
                    'live': [
                        name for name, thread in runtime._owned_threads() if thread.is_alive()
                    ],
                    'monitor_alive': monitor is not None and monitor.is_alive(),
                    'resources_open': runtime._resources_open,
                }
            )
        )
        return result

    pytest.MonkeyPatch().setattr(_worker.WorkerRuntime, 'wait', record_completion)


def test_checkpoint_trace_preserves_each_try_and_sqlite_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    passive = Checkpointer._passive_checkpoint
    contended = [(1, -1, -1)]

    def first_try_is_busy(checkpointer: Checkpointer) -> CheckpointResult:
        return contended.pop() if contended else passive(checkpointer)

    monkeypatch.setattr(Checkpointer, '_passive_checkpoint', first_try_is_busy)
    log_path = tmp_path / 'worker.log'
    _worker_processes._install_checkpoints(monkeypatch, log_path)
    path = tmp_path / 'checkpoint.db'
    with closing(sqlite3.connect(path)) as connection:
        checkpointer = Checkpointer(connection, path, interval_secs=1, sleep=lambda _delay: None)
        attempt = checkpointer.run_once()
        assert attempt.retried
        assert attempt.outcome == 'complete'
    with pytest.raises(DatabaseError, match='closed database'):
        checkpointer.run_once()
    rows = [
        json.loads(line)
        for line in log_path.with_suffix('.checkpoint-tries').read_text().splitlines()
    ]
    assert [row['result'] for row in rows] == [[1, -1, -1], list(attempt.result), None]
    assert [row['error'] for row in rows[:2]] == [None, None]
    assert 'closed database' in rows[2]['error']
    assert all(row['started'] <= row['finished'] for row in rows)
    assert all(row['pid'] == os.getpid() and row['path'] == str(path) for row in rows)


@pytest.mark.slow
def test_growth_halt_restarts_with_backoff_and_observes_reset_without_readers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_children(monkeypatch, tmp_path)
    config = write_daemon_config(
        tmp_path, Topology('default', ('local',), checkpoint_interval_secs=0.5)
    )
    path = source_database(tmp_path / 'state')
    with running_supervisor(config, ChildPatches(last_event_id=100)) as probe:
        probe.wait_for(lambda: bool(probe.records('local')), 15)
        assert len(probe.spawns) == 1, probe.terminations
        original = probe.spawns[0]
        # Keep a connection, but no read transaction, open across the worker restart.
        with closing(sqlite3.connect(path, isolation_level=None)) as writer:
            writer.execute('PRAGMA wal_autocheckpoint=0')
            _commit_marker(writer, path, 1, 256_000)
            assert not writer.in_transaction
            (tmp_path / 'tripwire-arm').touch()
            halted = _wait_for_failure(probe, original, 'WAL growth')
            assert halted['outcome'] == 'complete'
            assert halted['result'][1] > 1
            replacement = _replacement(probe, original)
            probe.wait_for(
                lambda: any(row['outcome'] == 'complete' for row in _attempts(replacement)), 10
            )
            _commit_marker(writer, path, 2, 1)
            committed_at = time.monotonic()
            probe.wait_for(
                lambda: any(
                    row['at'] >= committed_at
                    and row['outcome'] in {'complete', 'reset-observed'}
                    and row['result'][1] < halted['result'][1]
                    for row in _attempts(replacement)
                ),
                10,
            )
            assert not writer.in_transaction
            assert len(probe.spawns) == 2, probe.terminations


def test_process_assertion_preserves_child_logs(tmp_path: Path) -> None:
    config_path = write_daemon_config(tmp_path, Topology('default', ('group',)))

    def fail_inside_supervisor() -> None:
        with running_supervisor(config_path, ChildPatches()) as probe:
            assert probe.spawns
            spawn = probe.spawns[0]
            with spawn.log_path.open('a') as stream:
                stream.write('retained child failure\n')
            with spawn.log_path.with_suffix('.checkpoint-tries').open('a') as stream:
                stream.write('{"result": [1, -1, -1], "started": 0, "finished": 0.8}\n')
            raise AssertionError('parent assertion')

    with pytest.raises(AssertionError, match='parent assertion') as caught:
        fail_inside_supervisor()
    notes = '\n'.join(caught.value.__notes__)
    assert 'retained child failure' in notes
    assert '"result": [1, -1, -1]' in notes
    assert 'spawn=' in notes
    assert 'exit=' in notes


@pytest.mark.slow
def test_remote_restart_keeps_reader_pinned_until_its_owner_halts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / 'with-reader').touch()
    _install_children(monkeypatch, tmp_path)
    config = write_daemon_config(
        tmp_path, Topology('dedicated', ('remote',), checkpoint_interval_secs=1)
    )
    path = source_database(tmp_path / 'state')
    with running_supervisor(config, ChildPatches(last_event_id=100)) as probe:
        probe.wait_for(lambda: bool(probe.records('remote')) and len(probe.latest_health) == 2, 15)
        assert len(probe.spawns) == 2, probe.terminations
        source = next(spawn for spawn in probe.spawns if json.loads(spawn.worker_id)[0] == 'site')
        remote = next(spawn for spawn in probe.spawns if spawn is not source)
        (tmp_path / 'arm').touch()
        probe.wait_for((tmp_path / 'reader-open').exists, 5)
        assert int((tmp_path / 'reader-open').read_text()) != os.getpid()
        with closing(sqlite3.connect(path, isolation_level=None)) as writer:
            writer.execute('PRAGMA wal_autocheckpoint=0')
            _commit_marker(writer, path, 1, 256_000)
            (tmp_path / 'tripwire-arm').touch()
            halted = _wait_for_failure(probe, remote, 'WAL growth')
            assert halted['result'][1] > halted['result'][2]
            replacement = _replacement(probe, remote)
            probe.wait_for(
                lambda: any(
                    row['outcome'].startswith('partial-') for row in _attempts(replacement)
                ),
                10,
            )
            assert source.process.poll() is None
            assert not (tmp_path / 'reader-closed').exists()
            partial = next(
                row for row in _attempts(replacement) if row['outcome'].startswith('partial-')
            )
            assert partial['outcome'].startswith('partial-')
            assert partial['result'][1] >= halted['result'][1]
            _commit_marker(writer, path, 2, 64_000)
            duration = _wait_for_failure(probe, source, 'incomplete duration')
            assert duration['outcome'].startswith('partial-')
            assert duration['result'][1] > partial['result'][1]
            assert duration['wal_limit_bytes'] == _WAL_IN_USE_LIMIT_BYTES
            assert duration['result'][1] * duration['frame_bytes'] < duration['wal_limit_bytes']
            assert duration['pid'] == int((tmp_path / 'reader-open').read_text())
            assert duration['pid'] != partial['pid']
            cleared = [row for row in _attempts(source) if row['outcome'] == 'complete']
            since = (
                cleared[-1]['at']
                if cleared
                else float(source.log_path.with_suffix('.created').read_text())
            )
            assert duration['at'] - since >= 5
            assert duration['path'] == partial['path'] == halted['path'] == str(path)
            assert (tmp_path / 'reader-closed').exists()
            released_at = float((tmp_path / 'reader-closed').read_text())
            _replacement(probe, source)
            # Source-owner loss can also retire its dependent remote replacement.
            probe.wait_for(
                lambda: any(
                    row['at'] >= released_at and row['outcome'] == 'complete'
                    for spawn in probe.spawns
                    for row in _attempts(spawn)
                ),
                15,
            )
            _commit_marker(writer, path, 3, 1)
            committed_at = time.monotonic()
            probe.wait_for(
                lambda: any(
                    row['at'] >= committed_at
                    and row['outcome'] in {'complete', 'reset-observed'}
                    and row['result'][1] < duration['result'][1]
                    for spawn in probe.spawns
                    for row in _attempts(spawn)
                ),
                15,
            )


def test_shared_fixture_tolerates_subsecond_checkpoint_contention(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
    config = load_config(write_daemon_config(tmp_path, Topology('named', ('group',))))
    now = 0.0
    with closing(sqlite3.connect(tmp_path / 'checkpoint.db')) as connection:
        checkpointer = Checkpointer(
            connection,
            tmp_path / 'checkpoint.db',
            interval_secs=config.daemon.checkpoint_interval_secs,
            clock=lambda: now,
            run_checkpoint=lambda: (1, -1, -1),
            sleep=lambda _seconds: None,
        )
        # Reproduce the confirmed CI failure beyond the former 0.5 s fixture
        # bound, without making the regression depend on a loaded scheduler.
        now = 0.8
        attempt = checkpointer.run_once()
        assert attempt.outcome == 'contended'
        assert attempt.retried
        now = 5.0
        with pytest.raises(CheckpointBoundError, match='incomplete duration'):
            checkpointer.run_once()
