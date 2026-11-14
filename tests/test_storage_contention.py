from __future__ import annotations

from contextlib import ExitStack, closing, contextmanager, nullcontext
from dataclasses import asdict
from functools import partial
from io import StringIO
from itertools import pairwise
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock

import pytest

from benchmarks import storage_contention as harness
from sg_event_daemon._config import DEFAULT_CHECKPOINT_INTERVAL_SECS
from sg_event_daemon._platform import release_flock, spawn_child, try_flock_exclusive
from sg_event_daemon._source_schema import SOURCE_SCHEMA, source_identity
from sg_event_daemon._sqlite import open_database, wal_file_bytes
from sg_event_daemon._write_lock import SourceWriteLock
from tests._storage_support import open_group_on_source, write_context_for

if TYPE_CHECKING:
    import logging
    import sqlite3
    from collections.abc import Callable, Generator, Mapping

    from sg_event_daemon._checkpoint_host import CheckpointView, _CheckpointFile


class _AdmissionProbe:
    def __init__(self, stage: str, reason: str, configured: int) -> None:
        self._stage = stage
        self._reason = reason
        self._configured = configured
        self.clock = 100.0
        self.stop = harness.threading.Event()
        self.begins: list[str] = []
        self.released = False

    def _interrupt(self) -> None:
        if self._reason == 'deadline':
            self.clock += 0.05
        else:
            self.stop.set()

    def acquire(self, *, timeout: float) -> bool:
        assert timeout > 0
        if self._stage == 'local':
            self._interrupt()
        return True

    def authorize(self, action: int, name: str | None, value: str | None, *_: str | None) -> int:
        if (
            self._stage == 'pragma'
            and action == harness.sqlite3.SQLITE_PRAGMA
            and name == 'busy_timeout'
            and value not in {None, str(self._configured)}
        ):
            self._interrupt()
        if action == harness.sqlite3.SQLITE_TRANSACTION and name == 'BEGIN':
            self.begins.append(name)
        return harness.sqlite3.SQLITE_OK

    def release(self) -> None:
        self.released = True


class _ArrivalClock:
    def __init__(self) -> None:
        self.now = 100.0

    def monotonic(self) -> float:
        return self.now

    def wait(self, timeout: float) -> bool:
        self.now += timeout
        return False


class _HarnessClock:
    def __init__(self) -> None:
        self.local = harness.threading.local()
        self.real_select = harness.select.select
        self.original_entry = harness._thread_entry
        self.original_run_threads = harness._run_threads

    def monotonic(self) -> float:
        now = cast('float', getattr(self.local, 'now', 90.0))
        self.local.now = now + 0.000001
        return now

    def advance(self, when: float, stop: harness.threading.Event) -> bool:
        runtime = cast('harness._Runtime | None', getattr(self.local, 'runtime', None))
        if (
            runtime is not None
            and harness.threading.current_thread().name == 'lifecycle'
            and runtime.spec.population == 'cap-stress'
            and when >= runtime.spec.start_at + 2
        ):
            while not runtime.capacity.ready.is_set():
                if stop.is_set():
                    return False
                harness.time.sleep(0.005)
        self.local.now = max(self.monotonic(), when)
        return not stop.is_set()

    def run_threads(self, runtime: harness._Runtime) -> None:
        wait_restore = runtime.capacity.restore.wait
        if runtime.spec.lifecycle and runtime.spec.population == 'cap-stress':
            restored = False

            def wait_for_restore(timeout: float | None = None) -> bool:
                nonlocal restored
                del timeout
                if restored and not runtime.capacity.restore.is_set():
                    self.local.now = runtime.spec.end_at
                    return False
                wait_restore()
                restored = True
                self.local.now = max(self.monotonic(), runtime.spec.start_at + 1)
                return not runtime.stop.is_set()

            runtime.capacity.restore.wait = wait_for_restore
        original_stop = runtime.stop.set

        def stop() -> None:
            original_stop()
            runtime.capacity.restore.set()

        runtime.stop.set = stop
        self.original_run_threads(runtime)
        self.local.now = runtime.spec.end_at
        runtime.finished_at = self.monotonic()

    def select(
        self, readers: list[int], writers: list[int], errors: list[int], timeout: float
    ) -> tuple[list[int], list[int], list[int]]:
        result = self.real_select(readers, writers, errors, 0)
        if not result[0]:
            self.local.now = self.monotonic() + timeout
        return result

    def thread_entry(self, runtime: harness._Runtime, job: harness._ThreadJob) -> None:
        self.local.end_at = runtime.spec.end_at
        self.local.runtime = runtime
        self.original_entry(runtime, job)


class _HintReader:
    def __init__(
        self,
        spec: harness._ProcessSpec,
        connection: sqlite3.Connection,
        stack: ExitStack,
    ) -> None:
        self.spec = spec
        self.connection = connection
        self.observed: list[dict[str, object]] = []
        self.lanes = [
            harness._LaneState(group, strict=group in harness._STRICT_GROUPS)
            for group in self.spec.lanes
        ]
        self.pipes = {
            lane.group_name: stack.enter_context(
                harness._range_hint_pipe(self.spec.database_path, lane.group_name)
            )
            for lane in self.lanes
        }
        for lane in self.lanes:
            if lane.group_name == 'g12':
                lane.scan_suppressed = frozenset({'scan'})
            harness._lane_resume(connection, lane)
            harness._lane_prepare(connection, lane, now=100.0)

    def observe(self, *, hinted: bool) -> None:
        for lane in self.lanes:
            fd = self.pipes[lane.group_name]
            notification = harness._take_range_hint(fd)
            assert notification is hinted
            assert not harness._take_range_hint(fd)
            deadline = lane.next_coordination_at
            if not hinted:
                harness._lane_prepare(self.connection, lane, now=100.6, hinted=False)
                assert lane.source_ranges['notified'].version[1] == 200001
            now = 100.4 if hinted else deadline
            harness._lane_prepare(self.connection, lane, now=now, hinted=notification)
            self.observed.append(
                {
                    'group': lane.group_name,
                    'now': now,
                    'deadline': deadline,
                    'hinted': notification,
                    'high': lane.source_ranges['notified'].version[1],
                }
            )


class _PhaseClock:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._wall = 100.0
        self._cpu = 1.0
        monkeypatch.setattr(harness.time, 'monotonic', lambda: self._wall)
        monkeypatch.setattr(harness.time, 'thread_time', lambda: self._cpu)
        monkeypatch.setattr(harness.time, 'process_time', lambda: self._cpu * 2)

    def advance(self, wall: float, cpu: float) -> None:
        self._wall += wall
        self._cpu += cpu


class _DelayedConnection:
    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: _PhaseClock,
        delays: dict[str, tuple[float, float]],
        failure: str = '',
    ) -> None:
        self._connection = connection
        self._clock = clock
        self._delays = delays
        self._failure = failure
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)

    def delay(self, phase: str) -> None:
        self.calls.append(phase)
        self._clock.advance(*self._delays.get(phase, (0.0, 0.0)))
        if self._failure == phase:
            self._connection.execute('SELECT injected')

    def commit(self) -> None:
        self.delay('commit')
        self._connection.commit()

    def execute(self, sql: str) -> sqlite3.Cursor:
        if sql.startswith('BEGIN'):
            self.delay('begin')
        elif 'wal_checkpoint' in sql:
            self.delay('passive' if 'PASSIVE' in sql else 'truncate')
        return self._connection.execute(sql)


def _assert_retained_short_cell(
    result: dict[str, Any],
    summary_run: harness._CellRun,
    summary_records: list[dict[str, Any]],
    process_count: int,
    summarize: Callable[[harness._CellRun, list[dict[str, Any]]], dict[str, Any]],
) -> None:
    assert result['evidence_complete'], result['evidence_errors']
    retained = harness._retained_process_records(result)
    harness._reconcile_arrival_evidence(result, retained)
    harness._reconcile_checkpoint_evidence(result, retained)
    assert len(retained) == process_count
    assert [raw for _, raw in retained] == harness.json.loads(harness.json.dumps(summary_records))
    for entry in result['record_manifest']:
        assert entry['command']
        assert Path(entry['spec_path']).exists()
        assert Path(entry['raw_path']).exists()
    child_path = Path(result['record_manifest'][-1]['raw_path'])
    original = child_path.read_bytes()
    portable = child_path.parent / 'copied-bundle' / 'result.json'
    harness._persist_cell_result(result, portable)
    persisted = harness.json.loads(portable.read_text())
    assert all(not Path(entry['raw_path']).is_absolute() for entry in persisted['record_manifest'])
    child_path.unlink()
    assert not summarize(summary_run, summary_records)['evidence_complete']
    copied = harness._read_result_files([portable])[0]
    assert harness._retained_process_records(copied) == retained
    child_path.write_text('{broken')
    corrupted = summarize(summary_run, summary_records)
    with pytest.raises(harness.json.JSONDecodeError):
        harness._retained_process_records(corrupted)
    child_path.write_bytes(original + b'\n')
    with pytest.raises(ValueError, match='hash mismatch'):
        harness._retained_process_records(result)
    child_path.write_bytes(original)


@pytest.fixture(autouse=True)
def _default_harness_clock_is_stationary(monkeypatch: pytest.MonkeyPatch) -> None:
    # Single-step contract tests advance time only where their behavior needs it.
    # Process runners below install a separate clock inside each child as well.
    monkeypatch.setattr(harness.time, 'monotonic', lambda: 100.0)


@contextmanager
def _controlled_harness(
    monkeypatch: pytest.MonkeyPatch, *, end_at: float = 102.0
) -> Generator[pytest.MonkeyPatch]:
    # Logical clocks preserve production job startup, joins and connection ownership.
    # Real thread joins await completion rather than a benchmark's wall-clock cutoff.
    clock = _HarnessClock()
    original_host = harness.CheckpointHost
    original_checkpointer = harness.Checkpointer
    join = harness.threading.Thread.join

    def join_complete(thread: harness.threading.Thread, timeout: float | None = None) -> None:
        del timeout
        join(thread)

    class ControlledCheckpointer(original_checkpointer):
        def __init__(self, *args: object, **kwargs: object) -> None:
            kwargs['clock'] = clock.monotonic
            cast('Callable[..., None]', super().__init__)(*args, **kwargs)

    class ControlledHost(original_host):
        def __init__(self, *args: object, **kwargs: object) -> None:
            kwargs['clock'] = clock.monotonic
            cast('Callable[..., None]', super().__init__)(*args, **kwargs)
            self.test_end = cast('float', getattr(clock.local, 'end_at', end_at))

        def _run(self, file: _CheckpointFile) -> None:
            clock.local.now = file['first_due'] - self._interval_secs

            def wait(timeout: float | None = None) -> bool:
                assert timeout is not None
                if self._stop.is_set() or clock.monotonic() >= self.test_end:
                    return True
                clock.local.now = min(self.test_end, clock.monotonic() + timeout)
                return False

            self._stop.wait = wait
            super()._run(file)

        def register(self, *args: object, **kwargs: object) -> CheckpointView:
            view = cast('Callable[..., CheckpointView]', super().register)(*args, **kwargs)
            for _view, thread in self._files.values():
                thread.join()
            return view

    clock_api = SimpleNamespace(**vars(harness.time))
    clock_api.monotonic = clock.monotonic
    select_api = SimpleNamespace(select=clock.select)
    with monkeypatch.context() as patch:
        patch.setattr(harness, 'time', clock_api)
        patch.setattr(harness, 'select', select_api)
        patch.setattr(harness, '_wait_until', clock.advance)
        patch.setattr(harness, '_run_threads', clock.run_threads)
        patch.setattr(harness, '_thread_entry', clock.thread_entry)
        patch.setattr(harness.threading.Thread, 'join', join_complete)
        patch.setattr(harness, 'CheckpointHost', ControlledHost)
        patch.setattr(harness, 'Checkpointer', ControlledCheckpointer)
        patch.setattr(harness, 'SourceWriteLock', partial(SourceWriteLock, clock=clock.monotonic))
        yield patch


def _controlled_child(spec_path: str) -> None:
    spec = harness._ProcessSpec.from_json(Path(spec_path).read_text())
    with pytest.MonkeyPatch.context() as patch, _controlled_harness(patch, end_at=spec.end_at):
        result = harness._run_process(spec, persist=True)
        raise SystemExit(result['exit_status'])


def _hint_plan(poller: harness._PollerState, _now: float) -> harness._PollPlan:
    proposed = harness._PollPlan(frontier=200001)
    if poller.cycle == 0:
        proposed.opened.append(('notified', 200000, 200001, 300.0))
    else:
        proposed.merged = ('notified', 200002)
    return proposed


def _lock(path: Path) -> SourceWriteLock:
    return SourceWriteLock(
        path.with_suffix('.db.write-lock'),
        database_path=path,
        timeout_secs=2.0,
        wait_phase_grace_secs=1.0,
    )


def _phase_durations(raw: dict[str, Any]) -> dict[str, tuple[float, float, float]]:
    return {
        row['phase']: tuple(
            row['end'][key] - row['start'][key]
            for key in ('monotonic', 'thread_cpu', 'process_cpu')
        )
        for row in raw['phases']
    }


def _spawn_controlled_child(
    command: list[str],
    *,
    env: Mapping[str, str] | None = None,
) -> harness.ChildProcess:
    assert command[-2] == '--spec'
    return spawn_child(
        [
            harness.sys.executable,
            '-c',
            (
                'import sys; from tests.test_storage_contention import '
                f'{_controlled_child.__name__}; '
                f'{_controlled_child.__name__}(sys.argv[1])'
            ),
            command[-1],
        ],
        env=env,
    )


def _database(path: Path, *, synchronous: str = 'NORMAL') -> sqlite3.Connection:
    return open_database(
        path, source_identity('bench', 'source'), SOURCE_SCHEMA, synchronous=synchronous
    ).connection


def _hint_topology_child(spec_json: str) -> None:
    spec = harness._ProcessSpec.from_json(spec_json)
    signals = Path(spec.database_path).parent / 'hint-signals'
    clock = [100.0]
    original_publish = harness._publish_range_hints
    publications = 0

    def wait(name: str) -> None:
        while not (signals / name).exists():
            harness.time.sleep(0.005)

    def publish(fds: tuple[int, ...]) -> None:
        nonlocal publications
        publications += 1
        if publications == 1:
            # Coalesce a committed burst; deliberately lose the second change.
            for _ in range(3):
                original_publish(fds)

    with pytest.MonkeyPatch.context() as patch, ExitStack() as stack:
        clock_api = SimpleNamespace(**vars(harness.time))
        clock_api.monotonic = lambda: clock[0]
        patch.setattr(harness, 'time', clock_api)
        patch.setattr(harness, '_poller_plan', _hint_plan)
        patch.setattr(harness, '_publish_range_hints', publish)
        connection = stack.enter_context(closing(_database(Path(spec.database_path))))
        lock = stack.enter_context(closing(_lock(Path(spec.database_path))))
        lock.clock = lambda: clock[0]
        runtime = harness._Runtime(spec, lock)
        reader = _HintReader(spec, connection, stack)
        (signals / f'{spec.role}.ready').touch()

        def execute(
            active: harness._Runtime, conn: sqlite3.Connection, writer: harness._Writer
        ) -> None:
            wait('publish')
            clock[0] = 100.4
            assert harness._attempt_write(active, conn, writer, 100.4)
            (signals / 'published').touch()
            reader.observe(hinted=True)
            (signals / f'{spec.role}.first').touch()
            wait('merge')
            clock[0] = 100.6
            assert harness._attempt_write(active, conn, writer, 100.6, 1)
            (signals / 'merged').touch()
            reader.observe(hinted=False)

        if spec.poller:
            patch.setattr(harness, '_execute_arrivals', execute)
            harness._poller_writer(runtime, connection)
        else:
            wait('published')
            reader.observe(hinted=True)
            (signals / f'{spec.role}.first').touch()
            wait('merged')
            reader.observe(hinted=False)
        Path(spec.output_path).write_text(harness.json.dumps({'observed': reader.observed}))


def _qualification_evidence(
    result: dict[str, Any], directory: Path, *, checkpoint_count: int = 600
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    result['evidence_complete'] = True
    result['evidence_errors'] = []
    manifest: list[dict[str, Any]] = []
    processes: list[dict[str, Any]] = []
    result['record_manifest'] = manifest
    result['processes'] = processes
    result['checkpointers'][0]['attempts'] = checkpoint_count
    result['checkpointers'][0]['role'] = 'site'
    duration = result['duration_secs']
    result['checkpointers'][0]['host_started_at'] = 0.0
    result['checkpointers'][0]['host_finished_at'] = duration
    longest = result['checkpointers'][0]['longest_uncleared_intervals']
    checkpoints = [
        {
            'finished_at': longest + (duration - longest) * index / (checkpoint_count - 1),
            'outcome': 'complete',
            'reuse': 'reused',
            'result': [0, 1, 1],
            'frame_bytes': 1024,
            'wal_file_bytes': result['peak_wal_file_bytes'],
        }
        for index in range(checkpoint_count)
    ]
    result['checkpointers'][0].update(harness._checkpoint_summary(checkpoints, 0, duration, 1))
    outstanding: list[str] = []
    offered = 0
    for role, lanes in harness._TOPOLOGIES[result['cell']['topology']]:
        process = {'role': role, 'lanes': list(lanes), 'exit_status': 0, 'errors': []}
        processes.append(process)
        arrivals: list[dict[str, Any]] = [
            {'arrival_id': f'{role}:writer:{ordinal}', 'scheduled_at': float(ordinal)}
            for ordinal in range(100)
        ]
        missing = int(result['backlog_at_end']) if role == 'site' else 0
        outstanding.extend(item['arrival_id'] for item in arrivals[100 - missing :])
        offered += len(arrivals)
        raw = {
            **process,
            'finished_at': 602.0,
            'offered': len(arrivals),
            'offered_arrivals': arrivals,
            'commits': [
                {'arrival_id': item['arrival_id'], 'finished_at': item['scheduled_at'] + 0.1}
                for item in arrivals[: 100 - missing]
            ],
            'busy_timeouts': 0,
            'checkpoint_crossings': 0,
            'checkpoint_halt': None,
            'checkpoint_started_at': 0.0 if role == 'site' else None,
            'checkpoint_finished_at': duration if role == 'site' else None,
            'checkpoint_owner': role == 'site',
            'checkpoints': checkpoints if role == 'site' else [],
            'extra_timing': result['cell']['number'] == 6,
            'diagnostic_cap': harness._DIAGNOSTIC_CAP,
            'diagnostic_overflow': 0,
            'evidence_complete': True,
            'evidence_errors': [],
            'diagnostics': [
                {
                    'kind': 'process',
                    'role': role,
                    'pid': 1,
                    'thread_id': 1,
                    'thread_name': 'main',
                    'outcome': 'completed',
                    'start': {'monotonic': 0.0, 'process_cpu': 0.0, 'thread_cpu': 0.0},
                    'end': {'monotonic': 602.0, 'process_cpu': 1.0, 'thread_cpu': 1.0},
                }
            ]
            if result['cell']['number'] == 6
            else [],
        }
        spec = {
            **process,
            'start_at': 0.0,
            'end_at': duration,
            'extra_timing': result['cell']['number'] == 6,
            'diagnostic_cap': harness._DIAGNOSTIC_CAP,
            'scenario': result['cell']['scenario'],
            'synchronous': result['cell']['synchronous'],
        }
        entry = {'role': role}
        for kind, data in [('spec', spec), ('raw', raw)]:
            path = directory / f'{role}-{kind}.json'
            content = harness.json.dumps(data).encode()
            path.write_bytes(content)
            entry[f'{kind}_path'] = str(path)
            entry[f'{kind}_sha256'] = harness.hashlib.sha256(content).hexdigest()
        manifest.append(entry)
    result['offered_commits'] = offered
    result['achieved_commits'] = offered - len(outstanding)
    result['achieved_ratio'] = result['achieved_commits'] / offered
    result['arrival_accounting'] = {
        boundary: {
            'offered': offered,
            'completed': offered - len(outstanding),
            'outstanding': len(outstanding),
            'outstanding_ids': outstanding,
        }
        for boundary in ('load_end', 'drain_end')
    }


def _runner_spec(
    tmp_path: Path,
    *,
    start_at: float | None = None,
    duration_secs: float = 0.1,
    **changes: object,
) -> harness._ProcessSpec:
    start = harness.time.monotonic() + harness._START_DELAY_SECS if start_at is None else start_at
    values: dict[str, object] = {
        'role': 'site',
        'database_path': str(tmp_path / 'source.db'),
        'lanes': (),
        'poller': False,
        'lifecycle': False,
        'status_reader': False,
        'scenario': 'documented',
        'synchronous': 'NORMAL',
        'arbitration': 'two-level',
        'population': 'representative',
        'start_at': start,
        'end_at': start + duration_secs,
        'harness_clock_start': 0.0,
        'checkpoint_interval_secs': 1.0,
        'diagnostic_cap': 64,
        'seed': 'test',
        'scan_suppressed': (),
        'output_path': str(tmp_path / 'records.json'),
    }
    values.update(changes)
    return harness._ProcessSpec.from_json(harness.json.dumps(values))


def _seeded(
    tmp_path: Path, *, synchronous: str = 'NORMAL'
) -> tuple[sqlite3.Connection, SourceWriteLock]:
    path = tmp_path / 'source.db'
    connection = _database(path, synchronous=synchronous)
    lock = _lock(path)
    with lock.transaction(connection):
        harness._seed_base(connection, strict_groups=harness._STRICT_GROUPS)
    return connection, lock


def _range(
    connection: sqlite3.Connection,
    obligation: str,
    *,
    state: str = 'open',
    group_state: str = 'pending',
    high: int = 11,
) -> None:
    connection.execute(
        'INSERT INTO discovery_ranges (obligation_id, low_id, high_id, '
        'opened_successful_at, visibility_deadline, source_revision, close_generation, '
        'range_state, scan_next_id, scan_pages, degraded) '
        'VALUES (?, ?, ?, 0.0, 300.0, 1, 0, ?, 10, 0, 0)',
        (obligation, high - 1, high, state),
    )
    connection.execute(
        'INSERT INTO range_groups (obligation_id, group_name, group_generation, '
        'delivery_floor, eligibility_point, obligation_state) '
        'VALUES (?, ?, 1, 0, 1.0, ?)',
        (obligation, 'g00', group_state),
    )


def test_arrival_evidence_separates_load_and_drain(tmp_path: Path) -> None:
    spec = _runner_spec(tmp_path, start_at=100.0, duration_secs=3.0, lifecycle=True)
    runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
    runtime.finished_at = 105.0
    for ordinal, finished in [(0, 102.0), (1, 104.0)]:
        runtime.commits.append(
            {
                'writer': 'lifecycle',
                'class': 'lifecycle',
                'scheduled_at': 100.0 + ordinal,
                'finished_at': finished,
            }
        )
    raw = harness._process_records(runtime)
    accounting = harness._arrival_accounting([raw], 103.0)
    assert accounting['load_end'] == {
        'offered': 3,
        'completed': 1,
        'outstanding': 2,
        'outstanding_ids': ['site:lifecycle:1', 'site:lifecycle:2'],
    }
    assert accounting['drain_end'] == {
        'offered': 3,
        'completed': 2,
        'outstanding': 1,
        'outstanding_ids': ['site:lifecycle:2'],
    }
    runtime.commits.append(
        {'writer': 'lifecycle', 'class': 'lifecycle', 'scheduled_at': 102.0, 'finished_at': 105.1}
    )
    raw = harness._process_records(runtime)
    assert len(raw['commits']) == 3
    assert harness._arrival_accounting([raw], 103.0) == accounting
    assert [c['arrival_id'] for c in raw['commits']] == [f'site:lifecycle:{i}' for i in range(3)]
    for commit in raw['commits']:
        commit.update(
            started_at=commit['scheduled_at'],
            local_acquired_at=commit['scheduled_at'],
            file_acquired_at=commit['scheduled_at'],
            begun_at=commit['scheduled_at'],
        )
    run = harness._CellRun(harness._CELLS[0], harness._RunOptions(tmp_path), 100.0, 103.0, {})
    summary = harness._summarize(run, [raw])
    assert summary['backlog_at_end'] == 1
    assert summary['achieved_commits'] == 2


def test_arrivals_are_open_loop_and_phase_aligned() -> None:
    segments = [(0.0, 1.0, 22.0)]
    first = list(harness._arrival_times(segments, 100.0, 101.0))
    assert first == list(harness._arrival_times(segments, 100.0, 101.0))
    assert len(first) == 22
    assert [b - a for a, b in pairwise(first)] == pytest.approx([1 / 22] * 21)
    assert len(list(harness._arrival_times([(0, 1, 2)], 100, 101))) == 2


def test_backlog_counts_every_arrival_not_committed_by_the_end_of_the_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _ArrivalClock()
    monkeypatch.setattr(harness.time, 'monotonic', clock.monotonic)
    connection, lock = _seeded(tmp_path)
    monkeypatch.setattr(harness, '_DRAIN_SECS', 0.1)
    spec = _runner_spec(tmp_path, start_at=clock.now)
    runtime = harness._Runtime(spec, lock)
    monkeypatch.setattr(runtime.stop, 'wait', clock.wait)

    def slow(_connection: sqlite3.Connection, _now: float) -> str:
        clock.now += 0.07
        return 'test'

    writer = harness._Writer('test', (spec.end_at - 0.01, spec.end_at - 0.005), slow)
    with closing(connection), closing(lock):
        harness._execute_arrivals(runtime, connection, writer)
    assert len(runtime.commits) == 2
    assert runtime.commits[0]['finished_at'] == pytest.approx(spec.end_at + 0.06)
    # The second transaction finishes beyond the drain and remains backlog.
    assert clock.now == pytest.approx(spec.end_at + 0.13)
    accounting = harness._arrival_accounting(
        [
            {
                'offered_arrivals': [{'arrival_id': str(index)} for index in range(2)],
                'commits': [
                    {**commit, 'arrival_id': str(index)}
                    for index, commit in enumerate(runtime.commits)
                ],
            }
        ],
        spec.end_at,
    )
    assert accounting['drain_end']['offered'] == 2
    assert accounting['drain_end']['outstanding'] == 1
    assert runtime.commits[1]['finished_at'] > spec.end_at + harness._DRAIN_SECS


def test_busy_only_lock_takes_no_file_lock(tmp_path: Path) -> None:
    connection, original = _seeded(tmp_path)
    lock_path = tmp_path / 'source.db.write-lock'
    lock = harness._BusyOnlyLock(timeout_secs=1)
    timing = harness.WriteTiming()
    with closing(connection), closing(original), closing(lock):
        with lock.transaction(connection, timing=timing):
            fd = harness.os.open(lock_path, harness.os.O_RDWR)
            try:
                assert try_flock_exclusive(fd)
                release_flock(fd)
            finally:
                harness.os.close(fd)
        assert timing.started_at <= timing.local_acquired_at == timing.file_acquired_at
        assert timing.file_acquired_at <= timing.begun_at <= timing.finished_at


def test_busy_only_timing_includes_rollback_and_preserves_primary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [10.0]
    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    connection, source_lock = _seeded(tmp_path)
    source_lock.close()
    lock = harness._BusyOnlyLock(timeout_secs=1)
    timing = harness.WriteTiming()

    def authorize(action: int, name: str | None, *_: str | None) -> int:
        if action == harness.sqlite3.SQLITE_TRANSACTION and name == 'ROLLBACK':
            clock[0] += 3
        return harness.sqlite3.SQLITE_OK

    connection.set_authorizer(authorize)
    with closing(connection):
        with (
            pytest.raises(RuntimeError, match='primary'),
            lock.transaction(connection, timing=timing),
        ):
            raise RuntimeError('primary')
        assert timing.finished_at == 13.0
        assert not connection.in_transaction


def test_busy_timeout_restore_failure_is_not_retryable_after_expiry() -> None:
    class FailingRestore:
        def execute(self, _statement: str) -> None:
            raise harness.sqlite3.OperationalError('restore failed')

    def expire() -> None:
        raise harness.WriteLockTimeoutError('acquisition expired', 'SQLite')

    try:
        expire()
    except harness.WriteLockTimeoutError:
        with pytest.raises(harness.DatabaseError, match='restore failed'):
            harness._busy_restore_timeout(cast('sqlite3.Connection', FailingRestore()), 5000)


def test_cap_stress_population_has_the_documented_shape(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    harness._seed_cap_stress(path)
    connection = _database(path)
    with closing(connection):
        counts = harness._population_counts(connection)
        assert counts['discovery_ranges'] == 1000
        assert counts['open_ranges'] == 500
        assert counts['pending:g11'] == counts['pending:g12'] == 1000
        assert counts['source_late_ids'] == 50_000
        assert counts['late_ids'] == 100_000
        assert counts['audit'] == 140_000
        assert counts['replay_markers'] == 1
        assert (
            connection.execute(
                'SELECT COUNT(*) FROM range_groups r '
                'JOIN discovery_ranges d USING (obligation_id) '
                "WHERE d.range_state = 'open' AND r.obligation_state = 'pending'"
            ).fetchone()[0]
            == 6500
        )
        assert connection.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()[1] == 0


def test_cap_stress_shape_survives_a_full_replay_cycle(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    harness._seed_cap_stress(path)
    connection = _database(path, synchronous='FULL')
    with closing(connection):
        lock = _lock(path)
        with closing(lock):
            for group in harness._REPLAY_GROUPS:
                with lock.transaction(connection):
                    harness._lifecycle_replay(connection, group, 30.0)
            poller = harness._PollerState(frontier=1_000_000, hold_open=True)
            harness._poller_reload(connection, poller)
            plan = harness._poller_plan(poller, 31.0)
            with lock.transaction(connection):
                harness._poller_observe(connection, poller, plan, 31.0)
            for obligation, _ in plan.closed:
                for group in harness._GROUP_NAMES:
                    generation = connection.execute(
                        'SELECT group_generation FROM groups WHERE group_name = ?', (group,)
                    ).fetchone()[0]
                    lane = harness._LaneState(group, generation=generation)
                    with lock.transaction(connection):
                        harness._lane_scan_complete(
                            connection, lane, harness._ScanWork(obligation, [])
                        )
            harness._poller_reload(connection, poller)
            with lock.transaction(connection):
                harness._poller_observe(
                    connection, poller, harness._PollPlan(frontier=poller.frontier), 31.0
                )
        counts = harness._population_counts(connection)
        assert counts['open_ranges'] == 500
        assert counts['pending:g11'] == 1000
        assert counts['discovery_ranges'] == 1000
        assert counts['source_late_ids'] == 50_000
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM late_ids WHERE group_name = 'g11'"
            ).fetchone()[0]
            == 50_000
        )
        assert (
            connection.execute(
                'SELECT COUNT(*) FROM range_groups r '
                'JOIN discovery_ranges d USING (obligation_id) '
                "WHERE d.range_state = 'open' AND r.obligation_state = 'pending'"
            ).fetchone()[0]
            == 6500
        )


def test_capacity_replay_and_restore_recreate_the_capacity_shape(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    harness._seed_cap_stress(path)
    connection = _database(path, synchronous='FULL')
    lock = _lock(path)
    lane = harness._LaneState(
        'g12', scan_suppressed=frozenset(f'seed-closed-{index:03d}' for index in range(900))
    )
    with closing(lock):
        assert harness._lane_refresh(connection, lane)
        assert sum(len(source.late_ids) for source in lane.source_ranges.values()) == 50_000
        with lock.transaction(connection):
            harness._lifecycle_replay(connection, 'g12', 10.0)
        assert harness._lane_refresh(connection, lane)
        assert not any(source.late_ids for source in lane.source_ranges.values())
        counts = harness._population_counts(connection)
        assert counts['late_ids'] == 50_000
        assert counts['pending:g12'] == 500
        revisions = connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone()
        generation = connection.execute(
            "SELECT group_generation FROM groups WHERE group_name = 'g12'"
        ).fetchone()[0]
        assert len(harness._capacity_chunks()) == 50
        for first, last in harness._capacity_chunks():
            with lock.transaction(connection):
                harness._capacity_restore(connection, 'g12', generation, first, last)
    counts = harness._population_counts(connection)
    assert counts['late_ids'] == 100_000
    assert counts['pending:g11'] == counts['pending:g12'] == 1000
    assert counts['source_late_ids'] == 50_000
    assert (
        connection.execute('SELECT index_revision, range_revision FROM source_meta').fetchone()
        == revisions
    )
    assert not harness._lane_refresh(connection, lane)
    connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
    assert harness._lane_refresh(connection, lane)
    assert sum(len(source.late_ids) for source in lane.source_ranges.values()) == 50_000
    assert not lane.scan_work
    connection.close()


def test_capacity_runner_restores_shape_without_counting_maintenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _controlled_harness(monkeypatch):
        harness._seed_cap_stress(tmp_path / 'source.db')
        monkeypatch.setattr(harness, '_CAPACITY_REPLAY_EVERY', 2)
        monkeypatch.setattr(harness, '_RESTORE_INTERVAL_SECS', 0.001)
        spec = _runner_spec(
            tmp_path,
            scenario='lifecycle',
            population='cap-stress',
            lifecycle=True,
            duration_secs=2.5,
        )
        result = harness._run_process(spec)
        assert result['exit_status'] == 0, result['errors']
        assert result['offered'] == 3
        assert len(result['commits']) == 3
        assert result['restore_commits'] == 50
        assert len(result['capacity_replays']) == 1
        assert result['capacity_replays'][0]['late_ids'] == 50_000
        assert result['capacity_replays'][0]['pending'] == 1000
        with closing(_database(tmp_path / 'source.db')) as connection:
            assert harness._capacity_counts(connection) == {'late_ids': 50_000, 'pending': 1000}


def test_catchup_segments_alternate_until_three_hundred_burst_seconds() -> None:
    scenario = harness._SCENARIOS['catchup']
    segments = scenario.segments('g12', 600)
    assert [rate for _, _, rate in segments] == [310, 10] * 5
    assert sum(end - start for start, end, rate in segments if rate == 310) == 300
    assert segments[-1][1] == 600
    assert harness._default_duration(scenario, harness._MIN_SAMPLES) >= 600
    assert scenario.segments('g00', 600) == [(0, 600, 10)]


@pytest.mark.parametrize('phase', ['passive', 'truncate', 'sync'])
@pytest.mark.parametrize('failure', [False, True])
def test_cell6_checkpoint_call_phases_keep_delays_and_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    failure: bool,
) -> None:
    clock = _PhaseClock(monkeypatch)
    connection, lock = _seeded(tmp_path)
    runtime = harness._Runtime(_runner_spec(tmp_path, extra_timing=True), lock)
    delayed = _DelayedConnection(
        connection, clock, {phase: (0.25, 0.125)}, phase if failure else ''
    )

    # Busy partial progress requires the fallback sync after both SQLite calls.
    def checkpoint(_self: harness.Checkpointer, mode: str) -> tuple[int, int, int]:
        delayed.execute(f'PRAGMA wal_checkpoint({mode})').fetchone()
        return (0, 2, 1) if mode == 'PASSIVE' else (1, 2, 1)

    def sync(_self: harness.Checkpointer) -> None:
        delayed.delay('sync')

    monkeypatch.setattr(harness.Checkpointer, '_checkpoint', checkpoint)
    monkeypatch.setattr(harness.Checkpointer, '_sync', sync)
    with (
        closing(connection),
        closing(lock),
        closing(
            harness._checkpoint_factory(runtime, 4096)(
                cast('sqlite3.Connection', delayed),
                tmp_path / 'source.db',
                interval_secs=1,
                logger=harness.logging.getLogger('test'),
                log_extra={},
                wal_bytes=lambda: 0,
            )
        ) as checkpointer,
    ):
        if failure:
            with pytest.raises((harness.DatabaseError, harness.sqlite3.Error), match='injected'):
                checkpointer.run_once()
        else:
            attempt = checkpointer.run_once()
            assert attempt.outcome == 'partial-progress'
            assert attempt.reuse == 'reuse-busy'
    raw = harness._process_records(runtime)
    durations = _phase_durations(raw)
    assert durations[phase] == pytest.approx((0.25, 0.125, 0.25))
    assert all(value == (0.0, 0.0, 0.0) for key, value in durations.items() if key != phase)
    assert [row['phase'] for row in raw['phases']] == ['passive', 'truncate', 'sync'][
        : ['passive', 'truncate', 'sync'].index(phase) + 1 if failure else 3
    ]
    assert raw['phases'][-1]['outcome'] == ('failed' if failure else 'completed')
    assert all(row['phase_id'] == runtime.diagnostics[0]['phase_id'] for row in raw['phases'])
    assert len(delayed.calls) == len(raw['phases'])
    assert runtime.diagnostics[0]['outcome'] == ('failed' if failure else 'completed')


def test_cell6_checkpoint_phases_preserve_sql_results_and_deferred_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcomes: list[dict[str, Any]] = []
    for enabled in (False, True):
        directory = tmp_path / str(enabled)
        directory.mkdir()
        _PhaseClock(monkeypatch)
        connection, lock = _seeded(directory)
        runtime = harness._Runtime(_runner_spec(directory, extra_timing=enabled), lock)
        statements: list[str] = []

        def no_disk(*_args: object, **_kwargs: object) -> None:
            pytest.fail('phase recording wrote a file during measurement')

        with (
            closing(connection),
            closing(lock),
            closing(
                harness._checkpoint_factory(runtime, 4096)(
                    connection,
                    directory / 'source.db',
                    interval_secs=1,
                    logger=harness.logging.getLogger('test'),
                    log_extra={},
                )
            ) as checkpointer,
        ):
            connection.set_trace_callback(statements.append)
            with monkeypatch.context() as patch:
                patch.setattr(Path, 'write_text', no_disk)
                attempts = [asdict(checkpointer.run_once()), asdict(checkpointer.run_once())]
            raw = harness._process_records(runtime)
            assert len(raw['phases']) == (4 if enabled else 0)
            assert harness.json.loads(harness.json.dumps(raw['phases'])) == raw['phases']
            outcomes.append(
                {
                    'attempts': attempts,
                    'sql': statements,
                    'checkpoints': raw['checkpoints'],
                    'summary': harness._checkpoint_summary(raw['checkpoints'], 100.0, 101.0, 1.0),
                }
            )
    assert outcomes[0] == outcomes[1]
    assert (
        outcomes[0]['sql']
        == [
            'PRAGMA wal_checkpoint(PASSIVE)',
            'PRAGMA busy_timeout',
            'PRAGMA busy_timeout=100',
            'PRAGMA wal_checkpoint(TRUNCATE)',
            'PRAGMA busy_timeout=5000',
        ]
        * 2
    )


def test_cell6_checkpoint_wrapper_retains_failed_span(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection, lock = _seeded(tmp_path)
    runtime = harness._Runtime(_runner_spec(tmp_path, extra_timing=True), lock)
    observed = harness._checkpoint_factory(runtime, 4096)

    def broken(_self: harness.Checkpointer) -> harness.CheckpointAttempt:
        raise ValueError('checkpoint failed')

    monkeypatch.setattr(harness.Checkpointer, 'run_once', broken)
    with (
        closing(connection),
        closing(lock),
        closing(
            observed(
                connection,
                tmp_path / 'source.db',
                interval_secs=1,
                logger=harness.logging.getLogger('test'),
                log_extra={},
            )
        ) as checkpointer,
        pytest.raises(ValueError, match='checkpoint failed'),
    ):
        checkpointer.run_once()
    assert runtime.checkpoints == []
    assert runtime.diagnostics[0]['kind'] == 'checkpoint'
    assert runtime.diagnostics[0]['outcome'] == 'failed'


def test_cell6_correlates_child_cpu_ownership_and_site_gc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    site = harness._Runtime(
        _runner_spec(tmp_path, extra_timing=True), harness._BusyOnlyLock(timeout_secs=1)
    )
    child = harness._Runtime(
        _runner_spec(tmp_path, role='g10', extra_timing=True),
        harness._BusyOnlyLock(timeout_secs=1),
    )
    clock = [100.0]
    cpu = [1.0]
    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(harness.time, 'process_time', lambda: cpu[0])
    monkeypatch.setattr(harness.time, 'thread_time', lambda: cpu[0])
    with harness._diagnostic_span(site, 'read'):
        clock[0] = 101.0
        cpu[0] = 10.0
        with harness._diagnostic_span(child, 'writer-attempt') as owned:
            owned['timing'] = {'file_acquired_at': 101.0, 'finished_at': 103.0}
            clock[0] = 103.0
            cpu[0] = 11.0
        cpu[0] = 1.0
        clock[0] = 104.0
    gc_enabled = harness.gc.isenabled()
    harness.gc.collect()
    harness.gc.disable()
    try:
        with harness._diagnostic_session(site):
            callback = harness.gc.callbacks[-1]
            callback('start', {'generation': 2})
            clock[0] = 105.0
            cpu[0] += 0.5
            callback('stop', {'generation': 2})
    finally:
        if gc_enabled:
            harness.gc.enable()
    read, attempt = site.diagnostics[0], child.diagnostics[0]
    assert read['end']['process_cpu'] == read['start']['process_cpu']
    assert attempt['end']['process_cpu'] - attempt['start']['process_cpu'] == 1.0
    assert read['start']['monotonic'] < attempt['timing']['file_acquired_at']
    assert attempt['timing']['finished_at'] < read['end']['monotonic']
    gc_records = [r for r in site.diagnostics if r['kind'] == 'gc']
    assert [r['monotonic'] for r in gc_records] == [104.0, 105.0]
    assert all('cause' not in r for r in [*site.diagnostics, *child.diagnostics])


def test_cell6_disabling_timing_preserves_controlled_database_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    outcomes: list[dict[str, Any]] = []
    for enabled in (True, False):
        directory = tmp_path / str(enabled)
        directory.mkdir()
        clock = _ArrivalClock()
        monkeypatch.setattr(harness.time, 'monotonic', clock.monotonic)
        connection, lock = _seeded(directory)
        spec = _runner_spec(
            directory, start_at=100, duration_secs=3, lifecycle=True, extra_timing=enabled
        )
        runtime = harness._Runtime(spec, lock)
        monkeypatch.setattr(runtime.stop, 'wait', clock.wait)
        statements: list[str] = []

        def perform(conn: sqlite3.Connection, now: float) -> str:
            harness._lifecycle_replay(conn, 'g00', now)
            return 'lifecycle'

        with closing(connection), closing(lock):
            connection.set_trace_callback(statements.append)
            writer = harness._Writer('lifecycle', (100.0, 101.0, 102.0), perform)
            harness._execute_arrivals(runtime, connection, writer)
            runtime.finished_at = spec.end_at
            raw = harness._process_records(runtime)
            outcomes.append(
                {
                    'statements': statements,
                    'total_changes': connection.total_changes,
                    'commits': raw['commits'],
                    'offered_arrivals': raw['offered_arrivals'],
                    'accounting': harness._arrival_accounting([raw], spec.end_at),
                }
            )
        assert bool(runtime.diagnostics) is enabled
        assert len(raw['phases']) == (9 if enabled else 0)
    assert outcomes[0] == outcomes[1]
    assert outcomes[0]['total_changes'] > 0
    assert outcomes[0]['accounting']['drain_end']['completed'] == 3


def test_cell6_evidence_is_bounded_and_flushed_after_measurement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _runner_spec(tmp_path, start_at=100, extra_timing=True, diagnostic_cap=2)
    runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
    runtime.finished_at = 102.1
    for _ in range(5):
        with harness._diagnostic_span(runtime, 'read'):
            pass
    assert len(runtime.diagnostics) == 2
    assert runtime.diagnostic_overflow == 3
    raw = harness._process_records(runtime)
    assert raw['finished_at'] == 102.1
    assert not raw['evidence_complete']
    assert raw['evidence_errors']
    assert raw['diagnostic_cap'] == 2
    assert raw['diagnostic_overflow'] == 3
    clock = [100.0]
    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    spec = _runner_spec(tmp_path, start_at=100, arbitration='busy-only', extra_timing=True)
    spec_path = tmp_path / 'child-spec.json'
    spec_path.write_text(spec.to_json())

    def run_threads(active: harness._Runtime) -> None:
        clock[0] = active.spec.end_at
        active.finished_at = clock[0]

    def wait_until(boundary: float, _stop: object) -> bool:
        clock[0] = max(clock[0], boundary)
        return True

    write_records = harness._write_process_records

    def serialize(active: harness._Runtime) -> None:
        assert clock[0] >= spec.end_at + harness._DRAIN_SECS
        clock[0] += 50
        write_records(active)

    monkeypatch.setattr(harness, '_run_threads', run_threads)
    monkeypatch.setattr(harness, '_wait_until', wait_until)
    monkeypatch.setattr(harness, '_write_process_records', serialize)
    assert harness._child_main(['child', '--spec', str(spec_path)]) == 0
    persisted = harness.json.loads(Path(spec.output_path).read_text())
    assert persisted['finished_at'] == spec.end_at
    assert clock[0] > persisted['finished_at'] + 50


def test_cell6_phase_overflow_invalidates_evidence_without_changing_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _PhaseClock(monkeypatch)
    connection, lock = _seeded(tmp_path)
    runtime = harness._Runtime(
        _runner_spec(tmp_path, start_at=100, extra_timing=True, diagnostic_cap=2), lock
    )

    def no_disk(*_args: object, **_kwargs: object) -> None:
        pytest.fail('phase recording wrote a file during measurement')

    monkeypatch.setattr(Path, 'write_text', no_disk)
    with closing(connection), closing(lock):
        assert harness._attempt_write(
            runtime, connection, harness._Writer('g00', (100.0,), lambda _c, _t: 'mark'), 100.0
        )
    raw = harness._process_records(runtime)
    assert len(raw['phases']) == 2
    assert raw['phase_overflow'] == 1
    assert not raw['evidence_complete']
    assert any('phase overflow' in error for error in raw['evidence_errors'])
    assert len(raw['commits']) == 1


def test_cell6_phase_recording_preserves_queued_arrival_debt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _PhaseClock(monkeypatch)
    connection, lock = _seeded(tmp_path)
    spec = _runner_spec(tmp_path, start_at=100, duration_secs=1, extra_timing=True)
    runtime = harness._Runtime(spec, lock)

    def perform(_conn: sqlite3.Connection, _now: float) -> str:
        if not runtime.commits:
            clock.advance(0.6, 0.01)
        return 'mark'

    with closing(connection), closing(lock):
        harness._execute_arrivals(
            runtime, connection, harness._Writer('g00', (100.0, 100.1), perform)
        )
    raw = harness._process_records(runtime)
    assert 'phases' in raw
    first, second = runtime.commits
    assert first['finished_at'] == pytest.approx(100.6)
    assert second['scheduled_at'] == 100.1
    assert second['arrival_id'] == 'site:g00:1'
    assert second['started_at'] - second['scheduled_at'] == pytest.approx(0.5)
    assert second['finished_at'] == second['begun_at']
    assert [row['phase'] for row in raw['phases']] == ['begin', 'body', 'commit'] * 2


def test_cell6_records_cpu_gc_reader_and_checkpoint_spans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _runner_spec(tmp_path, start_at=100, extra_timing=True)
    runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
    clock = [100.0]
    cpu = [1.0]
    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(harness.time, 'process_time', lambda: cpu[0])
    monkeypatch.setattr(harness.time, 'thread_time', lambda: cpu[0])
    callbacks = list(harness.gc.callbacks)

    def operation() -> int:
        clock[0] += 1
        cpu[0] += 0.5
        return 42

    def broken() -> None:
        operation()
        raise ValueError('read failed')

    gc_enabled = harness.gc.isenabled()
    harness.gc.collect()
    harness.gc.disable()
    try:
        with harness._diagnostic_session(runtime):
            assert harness._record_read(runtime, 'health-projection', operation) == 42
            with pytest.raises(ValueError, match='read failed'):
                harness._record_read(runtime, 'status', broken)
            callback = harness.gc.callbacks[-1]
            callback('start', {'generation': 2})
            operation()
            callback('stop', {'generation': 2})
            with harness._diagnostic_span(runtime, 'checkpoint'):
                operation()
        assert harness.gc.callbacks == callbacks
        with pytest.raises(ValueError, match='read failed'), harness._diagnostic_session(runtime):
            broken()
        assert harness.gc.callbacks == callbacks
    finally:
        if gc_enabled:
            harness.gc.enable()
    spans = [r for r in runtime.diagnostics if r['kind'] in {'read', 'checkpoint'}]
    assert [r['outcome'] for r in spans] == ['completed', 'failed', 'completed']
    assert all(r['end']['monotonic'] >= r['start']['monotonic'] for r in spans)
    assert all(r['end']['process_cpu'] - r['start']['process_cpu'] == 0.5 for r in spans)
    assert all(r['end']['thread_cpu'] - r['start']['thread_cpu'] == 0.5 for r in spans)
    assert [r['generation'] for r in runtime.diagnostics if r['kind'] == 'gc'] == [2, 2]
    assert runtime.reads[1]['outcome'] == 'failed'
    assert harness._population_summary(runtime.reads) == {}


def test_cell6_records_rejected_attempts_and_release_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [100.0]
    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    spec = _runner_spec(tmp_path, start_at=100.0, duration_secs=1, extra_timing=True)
    runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
    outcomes = iter(['rejected', 'success', 'expired', 'stopped'])

    @contextmanager
    def transaction(
        _self: object,
        connection: sqlite3.Connection,
        *,
        timing: harness.WriteTiming,
        budget_secs: float,
        stop: object,
    ) -> Generator[sqlite3.Connection]:
        assert budget_secs > 0
        assert stop is runtime.stop
        timing.started_at = clock[0]
        timing.local_acquired_at = clock[0]
        outcome = next(outcomes)
        if outcome == 'expired':
            raise harness.WriteLockTimeoutError('expired', 'file')
        if outcome == 'stopped':
            runtime.stop.set()
            raise harness.WriteLockStoppedError('stopped')
        timing.file_acquired_at = clock[0]
        timing.begun_at = clock[0]
        try:
            if outcome == 'rejected':
                raise harness._IndexChangedError
            yield connection
        finally:
            clock[0] += 0.01
            timing.finished_at = clock[0]
            clock[0] += 0.01

    monkeypatch.setattr(harness._BusyOnlyLock, 'transaction', transaction)
    with closing(
        open_database(
            tmp_path / 'empty.db',
            source_identity('bench', 'source'),
            SOURCE_SCHEMA,
            synchronous='NORMAL',
        )
    ) as handle:
        connection = handle.connection
        writer = harness._Writer(
            'poller', (100.0,), lambda _conn, _now: 'poller', reload=lambda _conn: None
        )
        assert harness._write_with_reload(runtime, connection, writer, 100.0)
        assert not harness._attempt_write(runtime, connection, writer, 100.0)
        assert not harness._attempt_write(runtime, connection, writer, 100.0)
        runtime.stop.clear()
        clock[0] = 103.0
        assert not harness._attempt_write(runtime, connection, writer, 100.0)
    attempts = [r for r in runtime.diagnostics if r['kind'] == 'writer-attempt']
    assert [r['outcome'] for r in attempts] == [
        'rejected',
        'committed',
        'acquisition-expired',
        'stopped',
        'drain-terminated',
    ]
    assert len(runtime.commits) == 1
    assert [r['attempt'] for r in attempts] == [1, 2, 3, 4, 5]
    assert all(r['arrival_id'] == 'site:poller:0' for r in attempts)
    assert all(r['pid'] and r['thread_id'] and r['role'] == 'site' for r in attempts)
    assert all(r['drain_deadline'] == 103.0 for r in attempts)
    assert all(r['acquisition_deadline'] <= r['drain_deadline'] for r in attempts[:-1])
    assert attempts[-1]['acquisition_deadline'] is None
    assert attempts[1]['release_upper_bound'] >= attempts[1]['timing']['finished_at']
    assert attempts[2]['level'] == 'file'
    assert attempts[2]['timing']['begun_at'] is None


@pytest.mark.parametrize('phase', ['begin', 'body', 'commit'])
@pytest.mark.parametrize('failure', [False, True])
def test_cell6_writer_call_phases_keep_delays_and_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    failure: bool,
) -> None:
    clock = _PhaseClock(monkeypatch)
    connection, lock = _seeded(tmp_path)
    runtime = harness._Runtime(_runner_spec(tmp_path, start_at=100, extra_timing=True), lock)
    delayed = _DelayedConnection(
        connection, clock, {phase: (0.25, 0.125)}, phase if failure else ''
    )

    def body(_conn: sqlite3.Connection, _now: float) -> str:
        delayed.delay('body')
        return 'mark'

    with closing(connection), closing(lock):
        if failure:
            with pytest.raises(harness.DatabaseError, match='injected'):
                harness._attempt_write(
                    runtime,
                    cast('sqlite3.Connection', delayed),
                    harness._Writer('g00', (100.0,), body),
                    100.0,
                )
        else:
            assert harness._attempt_write(
                runtime,
                cast('sqlite3.Connection', delayed),
                harness._Writer('g00', (100.0,), body),
                100.0,
            )
        assert not connection.in_transaction
    raw = harness._process_records(runtime)
    assert _phase_durations(raw)[phase] == pytest.approx((0.25, 0.125, 0.25))
    assert [row['phase'] for row in raw['phases']] == ['begin', 'body', 'commit'][
        : ['begin', 'body', 'commit'].index(phase) + 1 if failure else 3
    ]
    assert raw['phases'][-1]['outcome'] == ('failed' if failure else 'completed')
    assert all(row['phase_id'] == runtime.diagnostics[0]['phase_id'] for row in raw['phases'])
    assert delayed.calls == [row['phase'] for row in raw['phases']]
    assert runtime.diagnostics[0]['outcome'] == ('database-error' if failure else 'committed')
    assert len(runtime.commits) == (0 if failure else 1)


def test_cells_match_the_spec_table() -> None:
    expected = [
        ('documented', 'single', 'NORMAL', 'two-level', 'representative'),
        ('documented', 'dedicated', 'NORMAL', 'two-level', 'representative'),
        ('worst', 'single', 'NORMAL', 'two-level', 'representative'),
        ('worst', 'dedicated', 'NORMAL', 'two-level', 'representative'),
        ('catchup', 'single', 'NORMAL', 'two-level', 'representative'),
        ('catchup', 'dedicated', 'NORMAL', 'two-level', 'representative'),
        ('worst', 'single', 'FULL', 'two-level', 'representative'),
        ('worst', 'dedicated', 'FULL', 'two-level', 'representative'),
        ('worst', 'named', 'NORMAL', 'two-level', 'representative'),
        ('lifecycle', 'single', 'NORMAL', 'two-level', 'cap-stress'),
        ('lifecycle', 'single', 'FULL', 'two-level', 'cap-stress'),
        ('worst', 'dedicated', 'NORMAL', 'busy-only', 'representative'),
    ]
    assert [
        (c.scenario, c.topology, c.synchronous, c.arbitration, c.population)
        for c in harness._CELLS
    ] == expected
    assert [c.number for c in harness._CELLS] == list(range(1, 13))
    assert [c.number for c in harness._CELLS if not c.gated] == [12]


def test_checkpoint_database_failure_closes_before_process_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _controlled_harness(monkeypatch):
        connection, lock = _seeded(tmp_path)
        connection.close()
        lock.close()
        closed = harness.threading.Event()
        real_close = harness.Checkpointer.close

        def fail(_self: harness.Checkpointer) -> harness.CheckpointAttempt:
            raise harness.DatabaseError('injected checkpoint failure')

        def close(self: harness.Checkpointer) -> None:
            real_close(self)
            closed.set()

        monkeypatch.setattr(harness.Checkpointer, 'run_once', fail)
        monkeypatch.setattr(harness.Checkpointer, 'close', close)
        result = harness._run_process(
            _runner_spec(
                tmp_path,
                start_at=harness.time.monotonic() + 0.3,
                checkpoint_interval_secs=0.1,
                poller=True,
            )
        )
        assert result['exit_status'] == 1
        assert 'injected checkpoint failure' in result['checkpoint_halt']
        assert closed.is_set()
        assert not any(
            thread.name.startswith('sg-event-daemon:checkpoint:')
            for thread in harness.threading.enumerate()
        )


def test_checkpoint_failure_summary_stops_at_observed_lifetime(tmp_path: Path) -> None:
    spec = _runner_spec(tmp_path, start_at=100.0, end_at=160.0, poller=True)
    runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
    runtime.finished_at = 105.1
    runtime.checkpoint_halt = 'bound exceeded'
    runtime.errors.append('checkpoint halted')
    runtime.checkpoints.append(
        {
            'outcome': 'partial-progress',
            'reuse': 'reuse-busy',
            'result': (0, 5170, 100),
            'finished_at': 105.0,
            'frame_bytes': 4120,
            'wal_file_bytes': 22_000_000,
        }
    )
    run = harness._CellRun(
        harness._CELLS[0],
        harness._RunOptions(work_dir=tmp_path),
        100.0,
        160.0,
        {'population': 'representative', 'gate_eligible': False},
    )
    result = harness._summarize(run, [harness._process_records(runtime)])
    assert result['checkpointers'][0]['longest_uncleared_secs'] == pytest.approx(5.1)
    assert result['peak_wal_bytes_in_use'] == 21_300_400
    assert result['duration_secs'] == 60
    assert result['offered_commits'] == result['backlog_at_end'] == 120


def test_checkpoint_lifespan_timestamps_follow_owning_checkpointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection, lock = _seeded(tmp_path)
    spec = _runner_spec(tmp_path, poller=True)
    runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
    now = [spec.start_at + 0.1]
    monkeypatch.setattr(harness.time, 'monotonic', lambda: now[0])
    with closing(connection), closing(lock):
        checkpointer = harness._checkpoint_factory(runtime, 1024)(
            connection,
            tmp_path / 'source.db',
            interval_secs=1.0,
            logger=harness.logging.getLogger('test'),
            log_extra={},
        )
        assert runtime.checkpoint_started_at == now[0] > spec.start_at
        now[0] = spec.end_at - 0.1
        checkpointer.close()
    assert runtime.checkpoint_finished_at == now[0] < spec.end_at


def test_checkpoint_loop_retains_bound_crossing_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _controlled_harness(monkeypatch, end_at=90.45) as controlled:
        connection, lock = _seeded(tmp_path)
        runtime = harness._Runtime(
            _runner_spec(
                tmp_path,
                start_at=harness.time.monotonic() + 0.3,
                duration_secs=0.15,
                checkpoint_interval_secs=0.1,
            ),
            lock,
        )

        class BoundCheckpointer(harness.Checkpointer):
            def __init__(
                self,
                conn: sqlite3.Connection,
                path: Path,
                *,
                interval_secs: float,
                logger: logging.Logger,
                log_extra: Mapping[str, str],
            ) -> None:
                super().__init__(
                    conn,
                    path,
                    interval_secs=interval_secs,
                    logger=logger,
                    log_extra=log_extra,
                    wal_bytes=lambda: 2,
                    run_checkpoint=lambda: (0, 5170, 100),
                    wal_limit_bytes=1,
                )

        controlled.setattr(harness, 'Checkpointer', BoundCheckpointer)
        with closing(connection), closing(lock):
            harness._checkpoint_loop(runtime, connection)
        assert len(runtime.checkpoints) == 2
        assert runtime.checkpoints[0]['result'] == (0, 5170, 100)
        assert runtime.checkpoints[0]['outcome'] == 'partial-progress'
        assert runtime.checkpoint_halt is None
        assert runtime.checkpoint_crossings == 1


def test_checkpoint_reuse_clears_the_harness_duration_timer() -> None:
    attempts = [
        {
            'outcome': 'partial-progress',
            'reuse': 'reused',
            'result': (0, 10, 5),
            'finished_at': second,
        }
        for second in range(1, 7)
    ]
    summary = harness._checkpoint_summary(attempts, 0, 6, 1)
    assert summary['longest_uncleared_intervals'] == 1
    assert summary['outcomes'] == {'partial-progress': 6, 'reused': 6}


def test_checkpoint_setup_rejects_an_interval_without_registration_lead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _controlled_harness(monkeypatch):
        connection, lock = _seeded(tmp_path)
        connection.close()
        lock.close()
        result = harness._run_process(
            _runner_spec(tmp_path, checkpoint_interval_secs=6, poller=True)
        )
        assert result['exit_status'] == 1
        assert any('checkpoint interval 6' in error for error in result['errors'])
        assert not result['checkpoints']


def test_checkpoint_summary_reports_the_longest_uncleared_time() -> None:
    attempts = [
        {
            'outcome': 'complete',
            'reuse': 'reuse-busy',
            'result': [0, 10, 10],
            'finished_at': second,
        }
        for second in (1, 4)
    ]
    result = harness._checkpoint_summary(attempts, 0, 6, 1)
    assert result['longest_uncleared_secs'] == 3
    assert result['longest_uncleared_intervals'] == 3


def test_child_writes_setup_failure_records(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    spec = _runner_spec(tmp_path, database_path=str(tmp_path / 'missing' / 'source.db'))
    path = tmp_path / 'spec.json'
    path.write_text(spec.to_json())
    assert harness._child_main(['child', '--spec', str(path)]) == 1
    records = harness.json.loads((tmp_path / 'records.json').read_text())
    assert records['errors']
    assert capsys.readouterr().out == ''


def test_count_queries_agree_with_counted_tables() -> None:
    assert all(
        query.startswith('SELECT COUNT(*) FROM ')
        and query.removeprefix('SELECT COUNT(*) FROM ') == table
        for table, query in harness._COUNT_SQL.items()
    )


def test_diagnostic_cap_survives_reentrant_encoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = harness._Runtime(
        _runner_spec(tmp_path, extra_timing=True, diagnostic_cap=1),
        harness._BusyOnlyLock(timeout_secs=1),
    )
    dumps = harness.json.dumps
    reentered = False

    def encode(value: object, *, separators: tuple[str, str] | None = None) -> str:
        nonlocal reentered
        if not reentered:
            reentered = True
            # Model the diagnostic session's same-thread GC callback during encoding.
            harness._record_diagnostic(runtime, {'kind': 'gc', 'phase': 'start', 'generation': 2})
        return dumps(value, separators=separators)

    monkeypatch.setattr(harness.json, 'dumps', encode)
    harness._record_diagnostic(runtime, {'kind': 'process-sample'})
    assert len(runtime.diagnostics) == 1
    assert runtime.diagnostics[0]['kind'] == 'gc'
    assert runtime.diagnostic_overflow == 1
    raw = harness._process_records(runtime)
    assert not raw['evidence_complete']
    assert raw['evidence_errors'] == ['diagnostic overflow: 1']


def test_evaluate_rejects_malformed_external_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / 'cell-01.json'
    path.write_text('{}')
    assert harness._main(['evaluate', str(path)]) == 2
    assert 'result' in capsys.readouterr().err


def test_gate_checkpointer_runs_a_size_triggered_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _controlled_harness(monkeypatch, end_at=101):
        connection, lock = _seeded(tmp_path)
        spec = _runner_spec(tmp_path, start_at=100, duration_secs=1, checkpoint_interval_secs=2)
        runtime = harness._Runtime(spec, lock, wal_trigger_bytes=1024)
        run_once = harness.Checkpointer.run_once
        calls = 0

        def grow_after_first(checkpointer: harness.Checkpointer) -> harness.CheckpointAttempt:
            nonlocal calls
            attempt = run_once(checkpointer)
            calls += 1
            if calls == 1:
                checkpointer._connection.execute('UPDATE source_meta SET frontier = frontier + 1')
                assert wal_file_bytes(Path(spec.database_path)) > 1024
            return attempt

        monkeypatch.setattr(harness.Checkpointer, 'run_once', grow_after_first)
        with closing(connection), closing(lock):
            connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            harness._checkpoint_loop(runtime, connection)
        attempts = runtime.checkpoints
        assert len(attempts) == 2, runtime.checkpoint_halt
        assert attempts[0]['finished_at'] == pytest.approx(100, abs=0.0001)
        assert attempts[1]['finished_at'] == pytest.approx(100.1, abs=0.0001)
        assert runtime.checkpoint_halt is None


def test_gate_reports_unsafe_work_dir_without_traceback(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    work_dir = tmp_path / 'unsafe'
    work_dir.mkdir(mode=0o755)
    work_dir.chmod(0o755)

    result = harness._main(
        [
            'gate',
            '--work-dir',
            str(work_dir),
            '--results-dir',
            str(tmp_path / 'results'),
            '--device-class',
            'test',
            '--filesystem',
            'test',
        ]
    )

    assert result == 2
    assert capsys.readouterr().err.startswith('error: ')


def test_harness_health_projection_matches_production(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            for obligation, state in (
                ('a', 'closed'),
                ('b', 'closed'),
                ('open', 'open'),
                ('stale', 'closed'),
                ('complete', 'closed'),
            ):
                _range(connection, obligation, state=state)
            connection.execute(
                "UPDATE range_groups SET group_generation = 2 WHERE obligation_id = 'stale'"
            )
            connection.execute(
                "UPDATE range_groups SET obligation_state = 'scan-complete' "
                "WHERE obligation_id = 'complete'"
            )
            connection.executemany(
                'INSERT INTO source_late_ids VALUES (?, ?)',
                [
                    ('a', 12),
                    ('a', 13),
                    ('b', 12),
                    ('b', 14),
                    ('open', 15),
                    ('stale', 16),
                    ('complete', 17),
                ],
            )
            connection.execute("INSERT INTO late_ids VALUES ('g00', 'a', 13, 'done', NULL)")

        with open_group_on_source(
            tmp_path / 'source.db',
            writes=write_context_for(tmp_path / 'source.db'),
            site='bench',
            site_server_url='https://bench.invalid',
            source='source',
            group='g00',
            group_generation=1,
            ordering='strict',
            stream_fingerprint='bench-stream',
            source_generation='bench-generation',
            verified_incarnation='bench-incarnation',
            start_mode='resume',
        ) as state:
            expected_count, expected_prefix = state.unresolved_late_id_projection(limit=100)
            assert (expected_count, expected_prefix) == (2, (12, 14))
            harness_count, harness_prefix, pending, audit = harness._health_projection_read(
                connection, 'g00'
            )
            assert (harness_count, tuple(harness_prefix)) == (expected_count, expected_prefix)
            assert (pending, audit) == (4, 0)


@pytest.mark.parametrize('done', [False, True])
def test_health_projection_counts_beyond_its_bounded_prefix(tmp_path: Path, done: bool) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            _range(connection, 'r1', state='closed')
            connection.executemany(
                "INSERT INTO source_late_ids VALUES ('r1', ?)",
                [(event,) for event in reversed(range(12, 117))],
            )
            if done:
                connection.execute(
                    "INSERT INTO late_ids SELECT 'g00', obligation_id, event_id, 'done', NULL "
                    'FROM source_late_ids'
                )
        expected: tuple[int, list[int], int, int] = (
            (0, [], 1, 0) if done else (105, list(range(12, 112)), 1, 0)
        )
        assert harness._health_projection_read(connection, 'g00') == expected


def test_health_projection_deduplicates_missing_and_filters_terminal_states(
    tmp_path: Path,
) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            for obligation in ('a', 'b', 'completed'):
                _range(connection, obligation, state='closed')
            connection.execute(
                "UPDATE range_groups SET obligation_state = 'scan-complete' "
                "WHERE obligation_id = 'completed'"
            )
            connection.execute(
                'INSERT INTO range_groups SELECT obligation_id, group_name, 2, '
                'delivery_floor, eligibility_point, obligation_state FROM range_groups '
                "WHERE obligation_id = 'a'"
            )
            connection.executemany(
                'INSERT INTO source_late_ids VALUES (?, ?)',
                [('a', 12), ('a', 13), ('a', 14), ('a', 15), ('b', 12), ('completed', 16)],
            )
            connection.executemany(
                'INSERT INTO late_ids VALUES (?, ?, ?, ?, NULL)',
                [
                    ('g00', 'a', 12, 'done'),
                    ('g00', 'a', 13, 'capacity-pending'),
                    ('g00', 'a', 14, 'pending'),
                    ('g01', 'a', 15, 'done'),
                ],
            )
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        result = harness._health_projection_read(connection, 'g00')
        connection.set_trace_callback(None)
        assert result == (4, [12, 13, 14, 15], 3, 0)
        assert len([sql for sql in statements if 'FROM source_late_ids' in sql]) == 2
        with lock.transaction(connection):
            harness._lifecycle_replay(connection, 'g01', 1.0)
        assert harness._health_projection_read(connection, 'g01') == (0, [], 0, 1)


def test_health_reads_follow_each_write_and_the_health_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection, lock = _seeded(tmp_path)
    runtime = harness._Runtime(_runner_spec(tmp_path), lock)
    lane = harness._LaneState('g00')
    clock = [100.0]
    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    with closing(connection), closing(lock):
        for _ in range(3):
            harness._lane_health(runtime, connection, lane)
        assert [r['kind'] for r in runtime.reads].count('health-projection') == 3
        assert [r['kind'] for r in runtime.reads].count('snapshot') == 1
        clock[0] += 2.1
        harness._lane_health(runtime, connection, lane)
        assert [r['kind'] for r in runtime.reads].count('snapshot') == 2


def test_health_snapshot_and_status_reads_are_read_only(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection):
        lock.close()
        changes = connection.total_changes
        harness._health_projection_read(connection, 'g00')
        assert not connection.in_transaction
        harness._snapshot_read(connection, 'g00')
        assert not connection.in_transaction
        counts = harness._status_read(connection)
        assert not connection.in_transaction
        assert connection.total_changes == changes
        assert counts['source_meta'] == 1
        assert counts['open_ranges'] == 0
        assert counts['pending:g00'] == 0


def test_lane_backlog_uses_current_time_without_moving_offered_arrivals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection, lock = _seeded(tmp_path)
    clock = _ArrivalClock()
    clock.now = harness.time.monotonic()
    scheduled = clock.now - 10.0
    spec = _runner_spec(tmp_path, start_at=scheduled, duration_secs=10.0, lanes=('g00',))
    runtime = harness._Runtime(spec, lock)
    monkeypatch.setattr(harness.time, 'monotonic', clock.monotonic)
    seen: list[float] = []
    prepare = harness._lane_prepare

    def record_prepare(
        conn: sqlite3.Connection,
        lane: harness._LaneState,
        *,
        now: float,
        hinted: bool = False,
        force: bool = False,
    ) -> None:
        seen.append(now)
        prepare(conn, lane, now=now, hinted=hinted, force=force)

    def execute(
        active: harness._Runtime, conn: sqlite3.Connection, writer: harness._Writer
    ) -> None:
        assert harness._attempt_write(active, conn, writer, scheduled)

    monkeypatch.setattr(harness, '_lane_prepare', record_prepare)
    monkeypatch.setattr(harness, '_execute_arrivals', execute)
    with closing(connection), closing(lock):
        harness._lane_writer(runtime, connection, 'g00')
    assert seen == [clock.now]
    assert runtime.commits[0]['scheduled_at'] == scheduled


def test_lane_classes_follow_the_documented_priority(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection):
        lane = harness._LaneState('g00', strict=True, arrivals=9)
        lane.scan_work = [harness._ScanWork('r1', [12])]
        with closing(lock):
            with lock.transaction(connection):
                _range(connection, 'r1', state='closed')
            with lock.transaction(connection):
                assert harness._lane_arrival(connection, lane, 10.0) == 'late-mark'
            with lock.transaction(connection):
                assert harness._lane_arrival(connection, lane, 11.0) == 'scan-complete'
            lane.arrivals = 19
            with lock.transaction(connection):
                assert harness._lane_arrival(connection, lane, 20.0) == 'epoch'
            lane.arrivals = 24
            with lock.transaction(connection):
                assert harness._lane_arrival(connection, lane, 25.0) == 'reader-clock'
            with lock.transaction(connection):
                assert harness._lane_arrival(connection, lane, 26.0) == 'mark'
            with lock.transaction(connection):
                assert harness._lane_arrival(connection, lane, 27.0) == 'cursor'
        assert lane.in_flight is None
        assert lane.next_event_id == 2
        assert connection.execute('SELECT COUNT(*) FROM epochs').fetchone()[0] == 1
        assert (
            connection.execute(
                "SELECT reader_clock_checkpoint FROM group_state WHERE group_name = 'g00'"
            ).fetchone()[0]
            == 25.0
        )


def test_lane_health_late_ids_follow_projection_grid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection, lock = _seeded(tmp_path)
    runtime = harness._Runtime(_runner_spec(tmp_path), lock)
    lane = harness._LaneState('g00')
    clock = [0.0]
    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    statements: list[str] = []
    read_times: list[float] = []
    expected_late: tuple[int, list[int]] = (0, [])
    with closing(connection), closing(lock):
        for index, now in enumerate((0.0, 0.2, 1.9, 2.1, 4.0, 4.2, 5.9, 6.0)):
            clock[0] = now
            with lock.transaction(connection):
                _range(connection, f'r{index}', state='closed', high=index + 11)
                connection.execute(
                    'INSERT INTO source_late_ids VALUES (?, ?)',
                    (f'r{index}', index + 10),
                )
                connection.execute(
                    'INSERT INTO audit (site, group_name, command, wall_clock_time, '
                    'prior_revision, new_revision, reason, local_uid, username, pid, '
                    "result_snapshot_json) VALUES ('bench', 'g00', 'test', ?, "
                    "0, 1, 'test', 0, 'bench', 0, '{}')",
                    (now,),
                )
            statements.clear()
            connection.set_trace_callback(statements.append)
            harness._lane_health(runtime, connection, lane)
            connection.set_trace_callback(None)
            if any('FROM source_late_ids' in sql and 'COUNT' in sql for sql in statements):
                read_times.append(now)
                expected_late = (index + 1, list(range(10, 11 + index)))
            projection = runtime.reads[-1]['counts']
            assert projection == [*expected_late, index + 1, index + 1]
        assert read_times == [0.0, 2.1, 4.0, 6.0]


def test_lane_prepared_write_ignores_other_group_scan(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState('g00')
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            _range(connection, 'peer', state='closed')
            connection.execute("UPDATE range_groups SET group_name = 'g01'")
        harness._lane_prepare(connection, lane, now=harness.time.monotonic())
        revision = lane.revision_seen
        with lock.transaction(connection):
            harness._lane_scan_complete(
                connection, harness._LaneState('g01'), harness._ScanWork('peer', [])
            )
        with lock.transaction(connection):
            assert harness._lane_guarded_arrival(connection, lane, 1.0) == 'mark'
        assert lane.revision_seen == revision
        assert connection.execute(
            "SELECT in_flight_event_id FROM group_state WHERE group_name = 'g00'"
        ).fetchone() == (1,)


def test_lane_prepared_write_rejects_own_replay(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState('g00')
    with closing(connection), closing(lock):
        harness._lane_prepare(connection, lane, now=harness.time.monotonic())
        with lock.transaction(connection):
            connection.execute('UPDATE source_meta SET frontier = 200000')
            harness._lifecycle_replay(connection, 'g00', 1.0)
        before = list(connection.iterdump())
        with pytest.raises(harness._LaneChangedError), lock.transaction(connection):
            harness._lane_guarded_arrival(connection, lane, 1.0)
        assert list(connection.iterdump()) == before
        assert lane.arrivals == 0
        harness._lane_prepare(connection, lane, now=harness.time.monotonic(), force=True)
        assert lane.generation == 2
        with lock.transaction(connection):
            assert harness._lane_guarded_arrival(connection, lane, 1.0) == 'mark'
        assert connection.execute(
            "SELECT cursor, in_flight_event_id FROM group_state WHERE group_name = 'g00'"
        ).fetchone() == (190000, 190001)


def test_lane_refresh_publishes_revision_from_the_metadata_snapshot(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    writer = _database(tmp_path / 'source.db')
    lane = harness._LaneState('g00')

    def before_begin(statement: str) -> None:
        if statement.startswith('BEGIN'):
            with lock.transaction(writer):
                _range(writer, 'r1', state='closed')
                writer.execute("INSERT INTO source_late_ids VALUES ('r1', 12)")
                writer.execute('UPDATE source_meta SET range_revision = range_revision + 1')

    with closing(connection), closing(writer), closing(lock):
        connection.set_trace_callback(before_begin)
        assert harness._lane_refresh(connection, lane)
        connection.set_trace_callback(None)
        assert lane.revision_seen == 1
        assert lane.scan_work[0].late_ids == [12]


def test_lane_refresh_reloads_only_after_range_revision_changes(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection):
        lane = harness._LaneState('g00', scan_suppressed=frozenset({'suppressed'}))
        assert harness._lane_refresh(connection, lane)
        assert not harness._lane_refresh(connection, lane)
        with closing(lock), lock.transaction(connection):
            _range(connection, 'r1', state='closed')
            _range(connection, 'suppressed', state='closed')
            connection.execute("INSERT INTO source_late_ids VALUES ('r1', 12)")
            connection.execute("INSERT INTO source_late_ids VALUES ('r1', 13)")
            connection.execute("INSERT INTO source_late_ids VALUES ('suppressed', 14)")
            connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
        assert harness._lane_refresh(connection, lane)
        assert [(work.obligation_id, work.late_ids) for work in lane.scan_work] == [
            ('r1', [12, 13])
        ]
        lane.scan_work[0].late_ids.pop(0)
        assert not harness._lane_refresh(connection, lane)


def test_lane_refresh_reloads_open_ranges_before_scheduling_their_close(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState('g00')
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            _range(connection, 'r1', state='closed')
            connection.execute("INSERT INTO source_late_ids VALUES ('r1', 12)")
        harness._lane_refresh(connection, lane)
        lane.scan_work[0].late_ids.clear()
        with lock.transaction(connection):
            connection.execute("UPDATE discovery_ranges SET range_state = 'open'")
            connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
        harness._lane_refresh(connection, lane)
        assert not lane.scan_work
        with lock.transaction(connection):
            connection.execute("INSERT INTO source_late_ids VALUES ('r1', 13)")
            connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        harness._lane_refresh(connection, lane)
        connection.set_trace_callback(None)
        assert not any('FROM source_late_ids' in sql for sql in statements)
        with lock.transaction(connection):
            connection.execute("UPDATE discovery_ranges SET range_state = 'closed'")
            connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
        harness._lane_refresh(connection, lane)
        assert lane.scan_work[0].late_ids == [12, 13]


def test_lane_refresh_replaces_work_when_replay_restores_membership_before_refresh(
    tmp_path: Path,
) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState('g00')
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            _range(connection, 'r1', state='closed')
            connection.executemany("INSERT INTO source_late_ids VALUES ('r1', ?)", [(12,), (13,)])
        harness._lane_refresh(connection, lane)
        lane.scan_work[0].late_ids.pop(0)
        with lock.transaction(connection):
            harness._lifecycle_replay(connection, 'g00', 1.0)
            connection.execute(
                "INSERT INTO range_groups VALUES ('r1', 'g00', 2, 0, 1.0, 'pending')"
            )
        harness._lane_prepare(connection, lane, now=harness.time.monotonic())
        assert lane.generation == 2
        assert lane.scan_work[0].late_ids == [12, 13]


@pytest.mark.parametrize('suppressed', [False, True])
def test_lane_refresh_reuses_source_ids_separately_from_scan_progress(
    tmp_path: Path, suppressed: bool
) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState(
        'g00', scan_suppressed=frozenset({'r1'}) if suppressed else frozenset()
    )
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            _range(connection, 'r1', state='closed')
            connection.executemany("INSERT INTO source_late_ids VALUES ('r1', ?)", [(12,), (13,)])
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        assert harness._lane_refresh(connection, lane)
        assert any('FROM source_late_ids' in sql for sql in statements)
        if not suppressed:
            with lock.transaction(connection):
                assert harness._lane_arrival(connection, lane, 1.0) == 'late-mark'
        with lock.transaction(connection):
            connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
        statements.clear()
        assert harness._lane_refresh(connection, lane)
        assert not any('FROM source_late_ids' in sql for sql in statements)
        assert [work.late_ids for work in lane.scan_work] == ([] if suppressed else [[13]])
        assert lane.source_ranges['r1'].late_ids == (12, 13)
        statements.clear()
        restarted = harness._LaneState('g00', scan_suppressed=lane.scan_suppressed)
        assert harness._lane_refresh(connection, restarted)
        assert any('FROM source_late_ids' in sql for sql in statements)
        assert [work.late_ids for work in restarted.scan_work] == (
            [] if suppressed else [[12, 13]]
        )
        connection.set_trace_callback(None)


@pytest.mark.parametrize(
    'change',
    [
        'UPDATE discovery_ranges SET source_revision = source_revision + 1',
        'UPDATE discovery_ranges SET close_generation = close_generation + 1',
        'UPDATE discovery_ranges SET high_id = high_id + 1',
        'UPDATE discovery_ranges SET low_id = low_id - 1',
    ],
)
def test_lane_refresh_reloads_changed_source_evidence(tmp_path: Path, change: str) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState('g00')
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            _range(connection, 'r1', state='closed')
            connection.executemany("INSERT INTO source_late_ids VALUES ('r1', ?)", [(12,), (13,)])
        harness._lane_refresh(connection, lane)
        lane.scan_work[0].late_ids.pop(0)
        with lock.transaction(connection):
            connection.execute("INSERT INTO source_late_ids VALUES ('r1', 14)")
            connection.execute(change)
            connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
        harness._lane_refresh(connection, lane)
        assert lane.scan_work[0].late_ids == [12, 13, 14]


@pytest.mark.parametrize('removal', ['replay', 'scan-complete', 'retire'])
def test_lane_refresh_discards_removed_membership_and_its_scan_progress(
    tmp_path: Path, removal: str
) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState('g00')
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            _range(connection, 'r1', state='closed')
            connection.execute("INSERT INTO source_late_ids VALUES ('r1', 12)")
        harness._lane_refresh(connection, lane)
        lane.scan_work[0].late_ids.clear()
        with lock.transaction(connection):
            if removal == 'replay':
                harness._lifecycle_replay(connection, 'g00', 1.0)
            elif removal == 'scan-complete':
                harness._lane_scan_complete(connection, lane, lane.scan_work[0])
            else:
                connection.execute("DELETE FROM discovery_ranges WHERE obligation_id = 'r1'")
                connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
        harness._lane_refresh(connection, lane)
        assert not lane.scan_work
        assert not lane.source_ranges
        with lock.transaction(connection):
            if removal == 'retire':
                _range(connection, 'r1', state='closed')
                connection.execute("INSERT INTO source_late_ids VALUES ('r1', 12)")
            elif removal == 'replay':
                connection.execute(
                    "INSERT INTO range_groups VALUES ('r1', 'g00', ?, 0, 1.0, 'pending')",
                    (lane.generation,),
                )
            else:
                connection.execute("UPDATE range_groups SET obligation_state = 'pending'")
            connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
        harness._lane_refresh(connection, lane)
        assert lane.scan_work[0].late_ids == [12]


def test_lane_reload_reads_late_ids_only_for_closed_ranges(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState('g00')
    statements: list[str] = []
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            _range(connection, 'opened')
            _range(connection, 'closed', state='closed')
            connection.execute("INSERT INTO source_late_ids VALUES ('opened', 10)")
            connection.execute("INSERT INTO source_late_ids VALUES ('closed', 10)")
        connection.set_trace_callback(statements.append)
        assert harness._lane_refresh(connection, lane)
        reads = [sql for sql in statements if 'FROM source_late_ids' in sql]
        assert len(reads) == 1
        assert "'closed'" in reads[0]
        assert lane.source_ranges['opened'].late_ids == ()
        assert lane.source_ranges['closed'].late_ids == (10,)
        # Every reuse must follow metadata and revision reads in this transaction.
        connection.execute(harness.BUMP_RANGE_REVISION)
        statements.clear()
        assert harness._lane_refresh(connection, lane)
        assert statements[0].startswith('BEGIN')
        assert statements[-1] == 'COMMIT'
        assert any('range_revision' in sql for sql in statements)
        assert any('JOIN discovery_ranges' in sql for sql in statements)
        assert not any('FROM source_late_ids' in sql for sql in statements)
        connection.set_trace_callback(None)


def test_lane_resumes_seeded_in_flight_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _controlled_harness(monkeypatch):
        connection, lock = _seeded(tmp_path)
        with lock.transaction(connection):
            lane = harness._LaneState('g00', strict=True)
            harness._lane_arrival(connection, lane, 0)
        spec = _runner_spec(tmp_path, lanes=('g00',))
        connection.close()
        lock.close()
        result = harness._run_process(spec)
        assert result['exit_status'] == 0, result['errors']
        assert result['commits'][0]['class'] == 'cursor'


def test_lane_scan_complete_changes_only_its_row_and_bumps_both_revisions(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection):
        with closing(lock):
            with lock.transaction(connection):
                _range(connection, 'r1', state='closed')
                connection.execute(
                    "INSERT INTO range_groups VALUES ('r1', 'g01', 1, 0, 1.0, 'pending')"
                )
                connection.execute("INSERT INTO late_ids VALUES ('g00', 'r1', 12, 'done', NULL)")
                connection.execute("INSERT INTO late_ids VALUES ('g01', 'r1', 12, 'done', NULL)")
            lane = harness._LaneState('g00')
            work = harness._ScanWork('r1', [])
            with lock.transaction(connection):
                harness._lane_scan_complete(connection, lane, work)
            with lock.transaction(connection):
                harness._lane_scan_complete(connection, lane, work)
        assert connection.execute(
            'SELECT group_name, obligation_state FROM range_groups ORDER BY group_name'
        ).fetchall() == [('g00', 'scan-complete'), ('g01', 'pending')]
        assert connection.execute('SELECT group_name FROM late_ids').fetchall() == [('g01',)]
        assert connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone() == (1, 1)


def test_latency_runs_from_the_scheduled_time_and_includes_queueing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _ArrivalClock()
    monkeypatch.setattr(harness.time, 'monotonic', clock.monotonic)
    connection, lock = _seeded(tmp_path)
    spec = _runner_spec(tmp_path, start_at=harness.time.monotonic() + 0.1)
    runtime = harness._Runtime(spec, lock)
    monkeypatch.setattr(runtime.stop, 'wait', clock.wait)
    counter = 0

    def perform(_connection: sqlite3.Connection, _now: float) -> str:
        nonlocal counter
        if counter == 0:
            clock.now += 0.1
        counter += 1
        return 'test'

    writer = harness._Writer(
        'test', (spec.start_at, spec.start_at + 0.01, spec.start_at + 0.02), perform
    )
    with closing(connection), closing(lock):
        harness._execute_arrivals(runtime, connection, writer)
    assert len(runtime.commits) == 3
    assert [
        record['finished_at'] - record['scheduled_at'] for record in runtime.commits[1:]
    ] == pytest.approx([0.09, 0.08])
    assert runtime.commits[0]['finished_at'] - runtime.commits[0]['begun_at'] == pytest.approx(0.1)


def test_lifecycle_replay_rebinds_open_ranges_for_the_new_generation(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path, synchronous='FULL')
    with closing(connection):
        with closing(lock):
            with lock.transaction(connection):
                connection.execute('UPDATE source_meta SET frontier = 20000')
                _range(connection, 'open-above', high=15000)
                _range(connection, 'open-below', high=5000)
                _range(connection, 'closed', state='closed')
                connection.execute(
                    "INSERT INTO callback_marks VALUES ('g00', 12, 0, 'bench', 'cb', 'done', 1)"
                )
                connection.execute(
                    "INSERT INTO late_ids VALUES ('g00', 'closed', 12, 'done', NULL)"
                )
                connection.execute(
                    'INSERT INTO epochs (group_name, low_id, high_id, captured_at, '
                    'eligibility_point, reader_clock_checkpoint, source_generation, '
                    "group_generation, state) VALUES ('g00', 1, 2, 0, 0, 0, "
                    "'bench-generation', 1, 'pending')"
                )
                connection.execute("INSERT INTO join_commits VALUES ('g00', 'a', '{}', 0, '{}')")
                connection.execute(
                    "INSERT INTO replay_markers VALUES ('g00', 1, 'bench-generation', 'm', 1, 'd')"
                )
            with lock.transaction(connection):
                harness._lifecycle_replay(connection, 'g00', 10.0)
        assert (
            connection.execute(
                "SELECT group_generation FROM groups WHERE group_name = 'g00'"
            ).fetchone()[0]
            == 2
        )
        assert connection.execute(
            "SELECT obligation_id, obligation_state FROM range_groups WHERE group_name = 'g00' "
            'ORDER BY obligation_id'
        ).fetchall() == [('open-above', 'pending'), ('open-below', 'satisfied-by-floor')]
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM callback_marks WHERE group_name = 'g00'"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM late_ids WHERE group_name = 'g00'"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM epochs WHERE group_name = 'g00'").fetchone()[
                0
            ]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM join_commits WHERE group_name = 'g00'"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM replay_markers WHERE group_name = 'g00'"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT length(result_snapshot_json) FROM audit WHERE group_name = 'g00'"
            ).fetchone()[0]
            == 2048
        )
        assert connection.execute('SELECT COUNT(*) FROM disposition_audit').fetchone()[0] == 1
        assert connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone() == (1, 1)


def test_lifecycle_target_replays_the_capacity_group_when_due_and_ready() -> None:
    state = harness._LifecycleState()
    targets = [
        harness._lifecycle_target(state, i, capacity_ready=True, population='cap-stress')
        for i in range(60)
    ]
    assert targets[:59] == [harness._REPLAY_GROUPS[i % 11] for i in range(59)]
    assert targets[59] == 'g12'
    assert state.position == 59
    delayed = harness._LifecycleState()
    assert (
        harness._lifecycle_target(delayed, 59, capacity_ready=False, population='cap-stress')
        == 'g00'
    )
    assert (
        harness._lifecycle_target(delayed, 60, capacity_ready=True, population='cap-stress')
        == 'g12'
    )
    assert delayed.position == 1
    assert (
        harness._lifecycle_target(state, 119, capacity_ready=True, population='representative')
        == 'g04'
    )


def test_main_reports_usage_errors_with_status_2() -> None:
    assert harness._main(['run']) == 2
    assert harness._main(['evaluate', '/nonexistent/results']) == 2


def test_missing_child_records_preserve_all_offered_arrivals(tmp_path: Path) -> None:
    spec = _runner_spec(tmp_path, role='g10', lanes=('g10',))
    process = harness.spawn_child([harness.sys.executable, '-c', 'raise SystemExit(7)'])
    result = harness._child_result(spec, process)
    assert result['exit_status'] != 0
    assert result['offered'] == len(harness._offered_arrivals(spec)) > 0
    cell = harness._Cell(
        2, 'documented', 'dedicated', 'NORMAL', 'two-level', 'representative', gated=True
    )
    run = harness._CellRun(cell, harness._RunOptions(tmp_path), spec.start_at, spec.end_at, {})
    summary = harness._summarize(run, [result])
    assert summary['backlog_at_end'] == summary['offered_commits'] == result['offered']
    assert summary['achieved_commits'] == 0
    assert summary['achieved_ratio'] == 0


@pytest.mark.parametrize('after_begin', [False, True])
def test_only_pre_begin_busy_failures_continue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, after_begin: bool
) -> None:
    clock = _ArrivalClock()
    monkeypatch.setattr(harness.time, 'monotonic', clock.monotonic)
    connection, lock = _seeded(tmp_path)
    spec = _runner_spec(tmp_path, start_at=harness.time.monotonic() + 0.1)
    runtime = harness._Runtime(spec, lock)
    monkeypatch.setattr(runtime.stop, 'wait', clock.wait)
    calls = 0

    def perform(_connection: sqlite3.Connection, _now: float) -> str:
        nonlocal calls
        calls += 1
        raise harness.DatabaseError('database is locked')

    writer = harness._Writer('test', (spec.start_at, spec.start_at + 0.01), perform)
    if not after_begin:
        writer = harness._Writer('test', writer.arrivals, perform, before=perform)
    with closing(connection), closing(lock):
        if after_begin:
            with pytest.raises(harness.DatabaseError, match='database is locked'):
                harness._execute_arrivals(runtime, connection, writer)
            assert calls == 1
        else:
            harness._execute_arrivals(runtime, connection, writer)
            assert runtime.busy_timeouts == 2
            assert calls == 2


@pytest.mark.parametrize('stage', ['local', 'pragma'])
@pytest.mark.parametrize('reason', ['deadline', 'stop'])
def test_busy_only_rechecks_admission_after_local_wait_and_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str, reason: str
) -> None:
    connection, original_lock = _seeded(tmp_path)
    lock = harness._BusyOnlyLock(timeout_secs=0.02)
    configured = connection.execute('PRAGMA busy_timeout').fetchone()[0]
    probe = _AdmissionProbe(stage, reason, configured)
    monkeypatch.setattr(lock, '_local', probe)
    monkeypatch.setattr(harness.time, 'monotonic', lambda: probe.clock)
    expected = (
        harness.WriteLockTimeoutError if reason == 'deadline' else harness.WriteLockStoppedError
    )
    with closing(connection), closing(original_lock), closing(lock):
        connection.set_authorizer(probe.authorize)
        with (
            pytest.raises(expected) as caught,
            lock.transaction(connection, timing=harness.WriteTiming(), stop=probe.stop),
        ):
            pytest.fail('admitted after the acquisition boundary')
        connection.set_authorizer(None)
        assert not probe.begins
        assert not connection.in_transaction
        assert probe.released
        assert connection.execute('PRAGMA busy_timeout').fetchone()[0] == configured
        if isinstance(caught.value, harness.WriteLockTimeoutError):
            assert caught.value.level == ('process-local' if stage == 'local' else 'SQLite')


def _gate_result(cell_number: int = 1, *, repetition: int = 1) -> dict[str, Any]:
    cell = harness._CELLS[cell_number - 1]
    shape = {
        key: {'min': value, 'max': value}
        for key, value in {
            'discovery_ranges': 1000,
            'open_ranges': 500,
            'pending:g11': 1000,
            'source_late_ids': 50_000,
            'late_ids': 50_000,
        }.items()
    }
    return {
        **harness._summary_metadata(
            harness._CellRun(
                cell,
                harness._RunOptions(work_dir=Path.cwd()),
                0.0,
                600.0,
                {
                    'population': cell.population,
                    'harness_clock': 600,
                    'row_counts': {},
                    'warmup_secs': 600,
                    'gate_eligible': True,
                },
            )
        ),
        'cell': asdict(cell),
        'repetition': repetition,
        'gate_eligible': True,
        'duration_secs': 600,
        'commit_latency_ms': {'p99': 15, 'p999': 50, 'max': 100},
        'all_commit_latency_ms': {'max': 100},
        'full_lifecycle_latency_ms': {'p99': 80} if cell.scenario == 'lifecycle' else None,
        'achieved_ratio': 1.0,
        'backlog_at_end': 0,
        'expired_attempts': {},
        'busy_timeouts': 0,
        'checkpointers': [
            {
                'halt': None,
                'bound_crossings': 0,
                'host_started_at': 0.0,
                'host_finished_at': 600.0,
                'longest_uncleared_intervals': 1,
            }
        ],
        'peak_wal_bytes_in_use': 1024,
        'peak_wal_file_bytes': 1024,
        'population_during_run': shape,
        'capacity_replays': {
            'count': 9,
            'min_late_ids_before': 50_000,
            'min_pending_before': 1000,
        }
        if cell.population == 'cap-stress'
        else None,
        'processes': [{'exit_status': 0}],
    }


def test_busy_only_cell_is_reported_not_gated() -> None:
    assert harness._evaluate([_gate_result(12)])['verdict'] == 'reported'


def test_cap_stress_cells_require_the_shape_to_hold() -> None:
    result = _gate_result(10)
    assert harness._evaluate([result])['verdict'] == 'pass'
    result['population_during_run']['open_ranges']['min'] = 499
    assert harness._evaluate([result])['verdict'] == 'not-eligible'
    result = _gate_result(10)
    result['capacity_replays']['count'] = 8
    assert harness._evaluate([result])['verdict'] == 'not-eligible'
    result = _gate_result(10)
    result['capacity_replays']['min_late_ids_before'] = 49_999
    assert harness._evaluate([result])['verdict'] == 'not-eligible'


def test_checkpoint_crossings_fail_a_run_even_without_worker_failure() -> None:
    result = _gate_result()
    result['checkpointers'][0]['bound_crossings'] = 1
    assert result['checkpointers'][0]['halt'] is None
    assert 'checkpoint_bound_crossings' in harness._evaluate([result])['failures']


def test_checkpoint_eligibility_uses_full_host_lifespan_not_attempt_count() -> None:
    result = _gate_result(5)
    result['processes'] = [{'exit_status': 0, 'errors': []}]
    result['checkpointers'][0].update(
        attempts=599, host_started_at=0.0, host_finished_at=result['duration_secs']
    )
    assert harness._result_eligible(result)
    result['checkpointers'][0]['host_started_at'] = 0.1
    assert not harness._result_eligible(result)
    result['checkpointers'][0]['host_started_at'] = 0.0
    result['checkpointers'][0]['host_finished_at'] = result['duration_secs'] - 0.1
    assert not harness._result_eligible(result)
    result['checkpointers'][0]['host_finished_at'] = result['duration_secs']
    result['checkpointers'][0]['halt'] = 'checkpoint failed'
    assert not harness._result_eligible(result)


def test_evaluate_applies_limits_by_scenario_and_durability() -> None:
    result = _gate_result()
    assert harness._evaluate([result])['verdict'] == 'pass'
    for p99, verdict in [(19, 'repeat'), (21, 'fail')]:
        result['commit_latency_ms']['p99'] = p99
        assert harness._evaluate([result])['verdict'] == verdict
    full = _gate_result(7)
    for p99, verdict in [(35, 'pass'), (45, 'repeat')]:
        full['commit_latency_ms']['p99'] = p99
        assert harness._evaluate([full])['verdict'] == verdict
    catchup = _gate_result(5)
    catchup['commit_latency_ms']['p99'] = 500
    assert harness._evaluate([catchup])['verdict'] == 'fail'
    catchup['commit_latency_ms']['p99'] = 15
    catchup['all_commit_latency_ms']['max'] = 450
    assert harness._evaluate([catchup])['verdict'] == 'repeat'
    catchup['expired_attempts'] = {'file': 1}
    assert harness._evaluate([catchup])['verdict'] == 'fail'
    catchup_full = _gate_result(5)
    catchup_full['cell']['synchronous'] = 'FULL'
    catchup_full['commit_latency_ms']['p99'] = 500
    assert harness._evaluate([catchup_full])['verdict'] == 'pass'
    lifecycle = _gate_result(11)
    lifecycle['full_lifecycle_latency_ms']['p99'] = 200.001
    assert harness._evaluate([lifecycle])['verdict'] == 'fail'
    lifecycle['full_lifecycle_latency_ms']['p99'] = 80
    lifecycle['all_commit_latency_ms']['max'] = 1500
    assert harness._evaluate([lifecycle])['verdict'] == 'fail'
    for key, value in [('backlog_at_end', 1), ('peak_wal_file_bytes', 70 * 1024**2)]:
        failed = _gate_result()
        failed[key] = value
        assert harness._evaluate([failed])['verdict'] == 'fail'
    uncleared = _gate_result()
    uncleared['checkpointers'][0]['longest_uncleared_intervals'] = 3
    assert harness._evaluate([uncleared])['verdict'] == 'fail'


def test_evaluate_rejects_distinct_files_with_duplicate_repetition(tmp_path: Path) -> None:
    result = _gate_result()
    result['commit_latency_ms']['p99'] = 18
    files = [tmp_path / f'cell-01-copy-{number}.json' for number in (1, 2, 3)]
    for path in files:
        path.write_text(harness.json.dumps(result))
    assert harness._main(['evaluate', *(str(path) for path in files)]) == 2
    for repetition, path in enumerate(files, 1):
        result['repetition'] = repetition
        path.write_text(harness.json.dumps(result))
    assert harness._main(['evaluate', *(str(path) for path in files)]) == 0


def test_evaluate_rejects_duplicate_file_inputs(tmp_path: Path) -> None:
    path = tmp_path / 'cell-01.json'
    result = _gate_result()
    result['commit_latency_ms']['p99'] = 18
    path.write_text(harness.json.dumps(result))
    assert harness._main(['evaluate', str(path), str(path), str(path)]) == 2
    assert harness._main(['evaluate', str(tmp_path), str(path)]) == 2


def test_gate_evaluates_cell_once_and_retains_specific_evidence_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _gate_result()
    result.update(
        gate_eligible=False,
        evidence_complete=False,
        evidence_errors=['site: raw hash mismatch'],
    )

    def run_cell(
        _cell: harness._Cell, _population: Path, _options: harness._RunOptions
    ) -> dict[str, Any]:
        return result

    def seed(_population: str, _path: Path, _warmup_secs: float) -> dict[str, Any]:
        return {}

    monkeypatch.setattr(harness, '_run_cell', run_cell)
    monkeypatch.setattr(harness, '_seed_command', seed)
    evaluate = harness._evaluate
    calls = 0

    def counted(
        results: list[dict[str, Any]], *, qualification: str = 'all-modes'
    ) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return evaluate(results, qualification=qualification)

    monkeypatch.setattr(harness, '_evaluate', counted)
    arguments = harness.argparse.Namespace(
        results_dir=tmp_path / 'results',
        work_dir=tmp_path / 'work',
        cells='1',
        warmup_secs=1.0,
        device_class='test',
        filesystem='test',
        commit='test',
        qualification='normal-m7i-flex.large',
    )
    evaluations = harness._gate_command(arguments, ['gate'])
    assert calls == 1
    assert evaluations[0]['evidence_errors'] == ['site: raw hash mismatch']
    assert harness.json.loads((arguments.results_dir / 'gate-summary.json').read_text())['cells'][
        0
    ]['evidence_errors'] == ['site: raw hash mismatch']


def test_gate_judges_the_wal_file_size() -> None:
    result = _gate_result()
    result['peak_wal_file_bytes'] = 70 * 1024**2
    verdict = harness._evaluate([result])
    assert verdict['verdict'] == 'fail'
    assert verdict['failures'] == ['peak_wal_file_bytes']

    result['peak_wal_file_bytes'] = 1024
    result['peak_wal_bytes_in_use'] = 70 * 1024**2
    assert harness._evaluate([result])['verdict'] == 'pass'

    runs = [_gate_result(repetition=number) for number in (1, 2, 3)]
    for run in runs:
        run['peak_wal_file_bytes'] = 60 * 1024**2
    assert harness._evaluate(runs[:1])['verdict'] == 'repeat'
    assert harness._evaluate(runs[:2])['verdict'] == 'incomplete'
    assert harness._evaluate(runs)['verdict'] == 'pass'


@pytest.mark.parametrize('cell', [7, 8, 11])
def test_normal_qualification_retains_full_capacity_failures(tmp_path: Path, cell: int) -> None:
    result = _gate_result(cell)
    result['commit_latency_ms'] = {'p99': 51, 'p999': 251}
    result['all_commit_latency_ms']['max'] = 1001
    if cell == 11:
        result['full_lifecycle_latency_ms']['p99'] = 200.001
    result['backlog_at_end'] = 2 * len(harness._TOPOLOGIES[result['cell']['topology']])
    result['expired_attempts'] = {'file': 1}
    result['checkpointers'][0]['longest_uncleared_intervals'] = 2.000001
    _qualification_evidence(result, tmp_path)
    assert result['achieved_ratio'] == 0.98
    evaluation = harness._evaluate([result], qualification='normal-m7i-flex.large')
    assert evaluation['verdict'] == 'fail'
    assert evaluation['qualification_verdict'] == 'pass'
    assert evaluation['repeat_required'] is False
    assert evaluation['safety_failures'] == []
    assert set(evaluation['capacity_failures']) == (set(evaluation['failures']) - {'timeouts'}) | {
        'expired_attempts'
    }
    assert {'achieved_ratio', 'backlog_at_end', 'timeouts'} <= set(evaluation['failures'])
    assert harness._evaluate([result])['qualification_verdict'] == 'fail'
    assert (
        harness._evaluate([_gate_result(12)], qualification='normal-m7i-flex.large')[
            'qualification_verdict'
        ]
        == 'reported'
    )


@pytest.mark.parametrize('cell', [7, 8, 11])
@pytest.mark.parametrize('peak', [67108863, 67108864, 67108865])
def test_full_wal_headroom_is_capacity_only(tmp_path: Path, cell: int, peak: int) -> None:
    runs = [_gate_result(cell, repetition=number) for number in (1, 2, 3)]
    for result in runs:
        result['peak_wal_file_bytes'] = peak
        _qualification_evidence(result, tmp_path / str(result['repetition']))
    for count, expected in [(1, 'repeat'), (2, 'incomplete'), (3, 'pass')]:
        evaluation = harness._evaluate(runs[:count], qualification='normal-m7i-flex.large')
        assert evaluation['qualification_verdict'] == expected
        assert evaluation['repeat_required'] is (count < 3)
        assert evaluation['safety_failures'] == []
        assert evaluation['failures'] == (['peak_wal_file_bytes'] if peak > 67108864 else [])
        assert evaluation['capacity_failures'] == evaluation['failures']
        assert evaluation['verdict'] == ('fail' if peak > 67108864 else expected)
    assert harness._evaluate(runs)['qualification_verdict'] == (
        'fail' if peak > 67108864 else 'pass'
    )


@pytest.mark.parametrize('cell', [1, 2, 3, 4, 5, 6, 9, 10])
def test_normal_qualification_keeps_normal_limits_binding(tmp_path: Path, cell: int) -> None:
    result = _gate_result(cell)
    _qualification_evidence(result, tmp_path)
    if cell == 10:
        result['full_lifecycle_latency_ms']['p99'] = 200.001
    else:
        result['all_commit_latency_ms']['max'] = 500.001
    evaluation = harness._evaluate([result], qualification='normal-m7i-flex.large')
    assert evaluation['qualification_verdict'] == 'fail'
    assert not evaluation['capacity_failures']
    assert evaluation['repeat_required'] is True
    result['peak_wal_file_bytes'] = 67108865
    _qualification_evidence(result, tmp_path)
    evaluation = harness._evaluate([result], qualification='normal-m7i-flex.large')
    assert 'peak_wal_file_bytes' in evaluation['safety_failures']
    assert evaluation['qualification_verdict'] == 'fail'


@pytest.mark.parametrize(
    'defect',
    [
        'busy',
        'crossing',
        'halt',
        'process',
        'wal',
        'missing',
        'unaccounted',
        'absent',
        'error',
        'corrupt',
        'hash',
        'short',
        'duplicate',
        'raw-busy',
    ],
)
def test_normal_qualification_keeps_full_safety_binding(tmp_path: Path, defect: str) -> None:
    result = _gate_result(8)
    _qualification_evidence(result, tmp_path)
    changes = {
        'crossing': (result['checkpointers'][0], 'bound_crossings', 1),
        'halt': (result['checkpointers'][0], 'halt', 'halt'),
        'process': (result['processes'][-1], 'exit_status', 1),
        'wal': (result, 'peak_wal_file_bytes', 268435457),
        'unaccounted': (result['arrival_accounting']['drain_end'], 'offered', 401),
        'error': (result['processes'][-1], 'errors', ['execution error']),
        'hash': (result['record_manifest'][-1], 'raw_sha256', '0' * 64),
        'short': (result, 'duration_secs', 1),
    }
    if defect in changes:
        mapping, key, value = changes[defect]
        mapping[key] = value
    elif defect == 'busy':
        result['busy_timeouts'] = 1
        result['expired_attempts'] = {'file': 1}
    elif defect == 'missing':
        Path(result['record_manifest'][-1]['raw_path']).unlink()
    elif defect == 'absent':
        del result['record_manifest']
    elif defect == 'corrupt':
        Path(result['record_manifest'][-1]['raw_path']).write_text('{')
    elif defect == 'duplicate':
        result['record_manifest'].append(result['record_manifest'][-1])
    else:
        entry = result['record_manifest'][-1]
        path = Path(entry['raw_path'])
        raw = harness.json.loads(path.read_text())
        raw['busy_timeouts'] = 1
        content = harness.json.dumps(raw).encode()
        path.write_bytes(content)
        entry['raw_sha256'] = harness.hashlib.sha256(content).hexdigest()
    evaluation = harness._evaluate([result], qualification='normal-m7i-flex.large')
    assert evaluation['qualification_verdict'] != 'pass'
    assert evaluation['safety_failures']
    if defect == 'busy':
        assert 'busy_timeouts' in evaluation['safety_failures']
        assert 'expired_attempts' in evaluation['capacity_failures']


@pytest.mark.parametrize('cell', [7, 8, 11])
@pytest.mark.parametrize('peak', [268435455, 268435456, 268435457])
def test_full_wal_runtime_size_remains_binding(tmp_path: Path, cell: int, peak: int) -> None:
    runs = [_gate_result(cell, repetition=number) for number in (1, 2, 3)]
    for result in runs:
        result['peak_wal_file_bytes'] = peak
        _qualification_evidence(result, tmp_path / str(result['repetition']))
        assert result['checkpointers'][0]['bound_crossings'] == 0
        assert result['checkpointers'][0]['halt'] is None
    evaluated = harness._evaluate(runs, qualification='normal-m7i-flex.large')
    assert evaluated['verdict'] == 'fail'
    assert evaluated['failures'] == ['peak_wal_file_bytes']
    assert evaluated['capacity_failures'] == ['peak_wal_file_bytes']
    assert evaluated['safety_failures'] == (['wal_runtime_size'] if peak > 268435456 else [])
    assert evaluated['qualification_verdict'] == ('fail' if peak > 268435456 else 'pass')


@pytest.mark.parametrize('counter', ['absent', 'invalid', 'false'])
def test_full_wal_runtime_size_rejects_bad_counter(
    tmp_path: Path, counter: str, capsys: pytest.CaptureFixture[str]
) -> None:
    result = _gate_result(8)
    result['peak_wal_file_bytes'] = 268435457
    _qualification_evidence(result, tmp_path / 'records')
    checkpoint = result['checkpointers'][0]
    if counter == 'absent':
        del checkpoint['bound_crossings']
    elif counter == 'invalid':
        checkpoint['bound_crossings'] = 'invalid'
    else:
        checkpoint['bound_crossings'] = False
    with pytest.raises(ValueError, match='bound_crossings'):
        harness._evaluate([result], qualification='normal-m7i-flex.large')
    report = tmp_path / 'cell-08.json'
    report.write_text(harness.json.dumps(result))
    assert (
        harness._main(['evaluate', '--qualification', 'normal-m7i-flex.large', str(report)]) == 2
    )
    assert 'bound_crossings' in capsys.readouterr().err


@pytest.mark.parametrize('value', [float('nan'), float('inf'), 'invalid', None])
def test_normal_qualification_rejects_malformed_metrics(tmp_path: Path, value: object) -> None:
    result = _gate_result(7)
    _qualification_evidence(result, tmp_path)
    result['commit_latency_ms']['p99'] = value
    with pytest.raises(ValueError, match=r'commit_latency_ms\.p99'):
        harness._evaluate([result], qualification='normal-m7i-flex.large')


def test_overlap_chains_measure_transitively_overlapping_readers() -> None:
    assert harness._overlap_chains([(0, 2), (1, 3), (2.5, 4), (5, 6)]) == (3, 4.0)


def test_packed_buffer_overflow_rejects_incomplete_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OverflowingBuffer(bytearray):
        def extend(self, _data: object) -> None:
            raise OverflowError('buffer size exceeded')

    monkeypatch.setattr(harness, 'bytearray', OverflowingBuffer, raising=False)
    records = harness._PackedRecords()
    with pytest.raises(OverflowError, match='buffer size exceeded'):
        records.append({'kind': 'read'})
    assert len(records) == 0
    with pytest.raises(OverflowError, match='evidence is incomplete'):
        list(records.records())


def test_packed_offset_overflow_rejects_incomplete_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    array = harness.array

    def narrow_offsets(_kind: str, values: list[int]) -> harness.array[int]:
        return array('B', values)

    monkeypatch.setattr(harness, 'array', narrow_offsets)
    runtime = harness._Runtime(
        _runner_spec(tmp_path, extra_timing=True), harness._BusyOnlyLock(timeout_secs=1)
    )
    with pytest.raises(OverflowError):
        harness._record_diagnostic(runtime, {'kind': 'read', 'large': 'x' * 256})
    assert len(runtime.diagnostics) == 0
    with pytest.raises(OverflowError):
        harness._process_records(runtime)


def test_packed_records_preserve_snapshots_and_sequence_access() -> None:
    records = harness._PackedRecords()
    original = {'clock': {'monotonic': -0.0}, 'identity': (2**70, 'site')}
    records.append(original)
    original['clock'] = {'monotonic': 1.0}
    records.append({'outcome': 'failed'})
    assert len(records) == 2
    assert records[0] == {'clock': {'monotonic': -0.0}, 'identity': [2**70, 'site']}
    assert records[-1] == {'outcome': 'failed'}
    assert records[::-1] == list(reversed(list(records.records())))
    assert records[:1] == [records[0]]
    assert list(records) == list(records.records())
    with pytest.raises(IndexError):
        _ = records[2]
    with pytest.raises(IndexError):
        _ = records[-3]


def test_packed_retention_keeps_gc_graph_constant() -> None:
    def tracked_graph_size(root: object) -> int:
        visited: set[int] = set()
        pending = [root]
        while pending:
            item = pending.pop()
            if isinstance(item, type) or not harness.gc.is_tracked(item) or id(item) in visited:
                continue
            visited.add(id(item))
            pending.extend(harness.gc.get_referents(item))
        return len(visited)

    records = harness._PackedRecords()
    empty_size = tracked_graph_size(records)
    for index in range(256):
        records.append({'kind': 'read', 'counts': {'nested': [index, {'clock': 0.125}]}})
    assert len(records) == 256
    assert tracked_graph_size(records) == empty_size


def test_percentiles_use_nearest_rank() -> None:
    assert harness._percentiles(list(range(1, 101))) == {
        'p50': 50,
        'p99': 99,
        'p999': 100,
        'max': 100,
    }


def test_poller_holds_the_open_range_cap(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    harness._seed_cap_stress(path)
    connection = _database(path)
    with closing(connection):
        lock = _lock(path)
        poller = harness._PollerState(frontier=1_000_000, hold_open=True)
        harness._poller_reload(connection, poller)
        plan = harness._poller_plan(poller, 10.0)
        assert len(plan.closed) == len(plan.opened) == 16
        with closing(lock), lock.transaction(connection):
            harness._poller_observe(connection, poller, plan, 10.0)
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM discovery_ranges WHERE range_state = 'open'"
            ).fetchone()[0]
            == 500
        )
        poller.cycle = next(
            index for index in range(100) if harness._unit(poller.seed, index) < 0.125
        )
        plan = harness._poller_plan(poller, 10.1)
        assert not plan.opened
        assert plan.merged is not None


def test_poller_open_binds_every_non_orphaned_group(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    connection = _database(path)
    with closing(connection):
        with closing(_lock(path)) as lock:
            with lock.transaction(connection):
                harness._seed_base(connection, strict_groups=harness._STRICT_GROUPS)
                connection.execute(
                    "INSERT INTO groups VALUES ('orphan', 1, 'eager', 'https://bench.invalid', "
                    "'bench-incarnation', 1.0, 0, 'orphaned')"
                )
                connection.execute(
                    'INSERT INTO group_state (group_name, revision, cursor, delivery_floor, '
                    'last_delivered_id) '
                    "VALUES ('orphan', 0, 0, 0, 0)"
                )
                connection.execute(
                    "UPDATE group_state SET delivery_floor = 100 WHERE group_name = 'g12'"
                )
            poller = harness._PollerState()
            plan = harness._PollPlan(frontier=2, opened=[('r1', 1, 2, 300.0)])
            with lock.transaction(connection):
                harness._poller_observe(connection, poller, plan, 0.0)
        assert connection.execute('SELECT COUNT(*) FROM discovery_ranges').fetchone()[0] == 1
        assert connection.execute('SELECT COUNT(*) FROM range_groups').fetchone()[0] == 13
        assert (
            connection.execute(
                "SELECT obligation_state FROM range_groups WHERE group_name = 'g12'"
            ).fetchone()[0]
            == 'satisfied-by-floor'
        )
        assert connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone() == (0, 1)


def test_poller_retires_ranges_without_pending_rows(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection):
        with closing(lock):
            with lock.transaction(connection):
                _range(connection, 'closed', state='closed', group_state='scan-complete')
                _range(connection, 'open', group_state='satisfied-by-floor')
                _range(connection, 'keep', state='closed')
                connection.execute("INSERT INTO source_late_ids VALUES ('closed', 12)")
                connection.execute(
                    "INSERT INTO late_ids VALUES ('g00', 'closed', 12, 'done', NULL)"
                )
            poller = harness._PollerState()
            with lock.transaction(connection):
                harness._poller_observe(connection, poller, harness._PollPlan(frontier=1), 0.0)
        assert connection.execute('SELECT obligation_id FROM discovery_ranges').fetchall() == [
            ('keep',)
        ]
        assert connection.execute('SELECT COUNT(*) FROM source_late_ids').fetchone()[0] == 0
        assert connection.execute('SELECT COUNT(*) FROM late_ids').fetchone()[0] == 0
        assert connection.execute('SELECT range_revision FROM source_meta').fetchone()[0] == 1


def test_poller_rolls_back_when_index_revision_moved(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    connection = _database(path)
    with closing(connection):
        with closing(_lock(path)) as lock:
            with lock.transaction(connection):
                harness._seed_base(connection, strict_groups=harness._STRICT_GROUPS)
            poller = harness._PollerState()
            with lock.transaction(connection):
                connection.execute('UPDATE source_meta SET index_revision = index_revision + 1')
            with pytest.raises(harness._IndexChangedError), lock.transaction(connection):
                harness._poller_observe(
                    connection,
                    poller,
                    harness._PollPlan(frontier=2, opened=[('r1', 1, 2, 300.0)]),
                    0.0,
                )
        assert connection.execute('SELECT frontier FROM source_meta').fetchone()[0] == 0
        assert connection.execute('SELECT COUNT(*) FROM discovery_ranges').fetchone()[0] == 0
        harness._poller_reload(connection, poller)
        assert poller.index_revision == 1


def test_process_export_rejects_packed_overflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = harness._Runtime(_runner_spec(tmp_path), harness._BusyOnlyLock(timeout_secs=1))
    monkeypatch.setattr(runtime.diagnostics, '_overflowed', True)
    with pytest.raises(OverflowError, match='evidence is incomplete'):
        harness._write_process_records(runtime)


def test_process_export_writes_each_record_before_decoding_next(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = _runner_spec(tmp_path, start_at=100, extra_timing=True)
    runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
    for index in range(4):
        runtime.diagnostics.append({'export_probe': index})
    output = StringIO()
    getitem = harness._PackedRecords.__getitem__

    def incremental(records: harness._PackedRecords, index: int) -> dict[str, Any]:
        if records is runtime.diagnostics and 0 < index <= len(records):
            assert f'"export_probe": {index - 1}' in output.getvalue()
        return getitem(records, index)

    def open_output(path: Path, mode: str, *, encoding: str) -> nullcontext[StringIO]:
        assert path == Path(spec.output_path)
        assert mode == 'w'
        assert encoding == 'utf-8'
        return nullcontext(output)

    monkeypatch.setattr(harness._PackedRecords, '__getitem__', incremental)
    monkeypatch.setattr(Path, 'open', open_output)
    harness._write_process_records(runtime)
    assert harness.json.loads(output.getvalue())['diagnostics'] == [
        {'export_probe': index} for index in range(4)
    ]


def test_process_records_late_start_and_unexpected_thread_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection, lock = _seeded(tmp_path)
    connection.close()
    lock.close()
    with monkeypatch.context() as patch:
        clock_api = SimpleNamespace(**vars(harness.time))
        clock_api.monotonic = lambda: 100.0
        patch.setattr(harness, 'time', clock_api)

        def skip_wait(_at: float, _stop: harness.threading.Event) -> bool:
            return True

        patch.setattr(harness, '_wait_until', skip_wait)
        late = harness._run_process(_runner_spec(tmp_path, start_at=99, end_at=99.1))
    assert late['exit_status'] == 1
    assert 'late-start' in late['errors']

    def broken(_runtime: harness._Runtime, _connection: sqlite3.Connection) -> None:
        raise ValueError('injected reader failure')

    monkeypatch.setattr(harness, '_status_reader', broken)
    with _controlled_harness(monkeypatch):
        failed = harness._run_process(_runner_spec(tmp_path, status_reader=True))
    assert failed['exit_status'] == 1
    assert any('injected reader failure' in error for error in failed['errors'])


def test_process_spec_round_trips_all_fields(tmp_path: Path) -> None:
    spec = _runner_spec(tmp_path, lanes=('g00',), scan_suppressed=('closed',))
    assert harness._ProcessSpec.from_json(spec.to_json()) == spec


def test_qualification_cli_preserves_raw_verdicts_and_repeat_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    results: dict[int, dict[str, Any]] = {}
    for cell in range(1, 13):
        result = _gate_result(cell)
        _qualification_evidence(result, tmp_path / f'evidence-{cell}')
        if cell in {7, 8, 11}:
            result['all_commit_latency_ms']['max'] = 1001
        results[cell] = result
        (tmp_path / f'cell-{cell:02d}.json').write_text(harness.json.dumps(result))
    assert harness._main(['evaluate', str(tmp_path)]) == 1
    assert (
        harness._main(['evaluate', '--qualification', 'normal-m7i-flex.large', str(tmp_path)]) == 0
    )
    output = capsys.readouterr().out
    assert 'NORMAL on m7i-flex.large; FULL capacity not qualified' in output
    assert 'Raw' in output
    assert 'Qualification' in output
    evaluations = harness._evaluate_files([tmp_path], qualification='normal-m7i-flex.large')
    assert [item['cell'] for item in evaluations] == list(range(1, 13))
    assert evaluations[6]['verdict'] == 'fail'
    assert evaluations[6]['qualification_verdict'] == 'pass'
    assert not any(item['repeat_required'] for item in evaluations)

    def seed(*_args: object) -> dict[str, Any]:
        return {}

    def run(args: harness.argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
        result = results[args.cell]
        return result, harness._evaluate([result], qualification=args.qualification)

    monkeypatch.setattr(harness, '_seed_command', seed)
    monkeypatch.setattr(harness, '_run_command', run)
    results[6]['all_commit_latency_ms']['max'] = 500.001
    gate_dir = tmp_path / 'gate'
    assert (
        harness._main(
            [
                'gate',
                '--qualification',
                'normal-m7i-flex.large',
                '--work-dir',
                str(tmp_path / 'work'),
                '--results-dir',
                str(gate_dir),
                '--device-class',
                'test',
                '--filesystem',
                'test',
            ]
        )
        == 1
    )
    summary = harness.json.loads((gate_dir / 'gate-summary.json').read_text())
    assert summary['repeat'] == [6]
    assert summary['cells'][5]['verdict'] == 'fail'
    assert summary['matrix_complete'] is True
    assert summary['qualification_verdict'] == 'fail'
    capsys.readouterr()
    assert (
        harness._main(
            [
                'evaluate',
                '--qualification',
                'normal-m7i-flex.large',
                str(tmp_path / 'cell-01.json'),
            ]
        )
        == 0
    )
    assert 'incomplete matrix' in capsys.readouterr().out


def test_qualification_evaluate_copied_relative_evidence(tmp_path: Path) -> None:
    original = tmp_path / 'original'
    result = _gate_result(8)
    _qualification_evidence(result, original / 'records')
    for entry in result['record_manifest']:
        for key in ('spec_path', 'raw_path'):
            entry[key] = str(Path(entry[key]).relative_to(original))
    (original / 'cell-08.json').write_text(harness.json.dumps(result))
    copied = tmp_path / 'copied'
    harness.shutil.copytree(original, copied)
    harness.shutil.rmtree(original)
    assert (
        harness._evaluate_files([copied], qualification='normal-m7i-flex.large')[0][
            'qualification_verdict'
        ]
        == 'pass'
    )
    (copied / result['record_manifest'][-1]['raw_path']).unlink()
    evaluation = harness._evaluate_files([copied], qualification='normal-m7i-flex.large')[0]
    assert evaluation['qualification_verdict'] == 'fail'
    assert evaluation['verdict'] == 'pass'
    assert evaluation['safety_failures'] == ['qualification_evidence']


def test_qualification_judges_slow_full_lifespan_checkpoint_cell(tmp_path: Path) -> None:
    result = _gate_result(5)
    _qualification_evidence(result, tmp_path, checkpoint_count=599)
    assert harness._result_eligible(result)
    evaluation = harness._evaluate([result], qualification='normal-m7i-flex.large')
    assert evaluation['qualification_verdict'] == 'pass'


@pytest.mark.parametrize(
    'defect',
    [
        'missing',
        'empty',
        'attempts-nan',
        'attempts-inf',
        'attempts-bool',
        'attempts-mismatch',
        'raw-wal',
        'raw-wal-nan',
        'raw-finish-nan',
        'raw-frames-nan',
        'raw-shape',
        'clearing-mismatch',
    ],
)
def test_qualification_rejects_incomplete_checkpoint_evidence(tmp_path: Path, defect: str) -> None:
    result = _gate_result(8)
    _qualification_evidence(result, tmp_path)
    assert (
        harness._evaluate([result], qualification='normal-m7i-flex.large')['qualification_verdict']
        == 'pass'
    )
    entry = result['record_manifest'][0]
    path = Path(entry['raw_path'])
    raw = harness.json.loads(path.read_text())
    summary_values = {
        'attempts-nan': float('nan'),
        'attempts-inf': float('inf'),
        'attempts-bool': True,
        'attempts-mismatch': 601,
    }
    if defect in summary_values:
        result['checkpointers'][0]['attempts'] = summary_values[defect]
    elif defect == 'missing':
        del raw['checkpoints']
    elif defect == 'empty':
        raw['checkpoints'] = []
    elif defect == 'clearing-mismatch':
        raw['checkpoints'][0]['finished_at'] = 0.0
    else:
        changes = {
            'raw-wal': ('wal_file_bytes', 67108865),
            'raw-wal-nan': ('wal_file_bytes', float('nan')),
            'raw-finish-nan': ('finished_at', float('nan')),
            'raw-frames-nan': ('result', [0, float('nan'), 0]),
            'raw-shape': ('result', [0]),
        }
        key, value = changes[defect]
        raw['checkpoints'][0][key] = value
    content = harness.json.dumps(raw).encode()
    path.write_bytes(content)
    entry['raw_sha256'] = harness.hashlib.sha256(content).hexdigest()
    evaluated = harness._evaluate([result], qualification='normal-m7i-flex.large')
    assert evaluated['qualification_verdict'] == 'fail'
    assert evaluated['safety_failures'] == ['qualification_evidence']
    assert evaluated['capacity_failures'] == []
    assert evaluated['verdict'] == 'pass'
    assert harness._evaluate([result])['verdict'] == 'pass'
    assert evaluated['evidence_errors']
    report = tmp_path / 'cell-08.json'
    report.write_text(harness.json.dumps(result))
    assert harness._evaluate_files([report], qualification='normal-m7i-flex.large')[0][
        'safety_failures'
    ] == ['qualification_evidence']


def test_qualification_repeats_use_first_run_band(tmp_path: Path) -> None:
    runs = [_gate_result(repetition=number) for number in (1, 2, 3)]
    for result in runs:
        _qualification_evidence(result, tmp_path / str(result['repetition']))
    runs[1]['commit_latency_ms']['p99'] = 19
    evaluated = harness._evaluate(runs, qualification='normal-m7i-flex.large')
    assert evaluated['qualification_verdict'] == 'pass'
    assert evaluated['repeat_required'] is False
    runs[2]['environment']['commit'] = 'different'
    with pytest.raises(ValueError, match='incompatible repeat identity'):
        harness._evaluate(runs, qualification='normal-m7i-flex.large')


def test_qualification_run_saves_both_verdicts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _gate_result(7)
    _qualification_evidence(result, tmp_path / 'records')
    result['all_commit_latency_ms']['max'] = 1001

    def run(*_args: object) -> dict[str, Any]:
        return result

    monkeypatch.setattr(harness, '_run_cell', run)
    output = tmp_path / 'cell-07.json'
    assert (
        harness._main(
            [
                'run',
                '--qualification',
                'normal-m7i-flex.large',
                '--cell',
                '7',
                '--population',
                str(tmp_path / 'seed.db'),
                '--work-dir',
                str(tmp_path / 'work'),
                '--device-class',
                'test',
                '--filesystem',
                'test',
                '--output',
                str(output),
            ]
        )
        == 0
    )
    saved = harness.json.loads(output.read_text())
    assert saved['verdict'] == 'fail'
    assert saved['qualification_verdict'] == 'pass'
    assert saved['failures'] == ['all_commit_latency_ms.max']


def test_range_hints_coalesce_and_full_pipe_drops_without_blocking(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState('g00')
    statements: list[str] = []
    with (
        closing(connection),
        closing(lock),
        harness._range_hint_pipe(str(tmp_path / 'source.db'), 'g00') as reader,
        harness._range_hint_pipe(str(tmp_path / 'source.db'), 'g00') as writer,
    ):
        harness._lane_prepare(connection, lane, now=harness.time.monotonic())
        for _ in range(100_000):
            harness._publish_range_hints((writer,))
        connection.set_trace_callback(statements.append)
        for _ in range(3):
            harness._lane_prepare(
                connection,
                lane,
                now=harness.time.monotonic(),
                hinted=harness._take_range_hint(reader),
            )
        connection.set_trace_callback(None)
        assert sum('SELECT range_revision' in sql for sql in statements) == 1
        assert not harness._take_range_hint(reader)


def test_refresh_and_status_use_only_the_prescribed_reads(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection):
        lane = harness._LaneState('g00')
        with closing(lock):
            with lock.transaction(connection):
                _range(connection, 'r1', state='closed')
                connection.execute("INSERT INTO source_late_ids VALUES ('r1', 12)")
                connection.execute("INSERT INTO source_late_ids VALUES ('r1', 13)")
                connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
            assert harness._lane_refresh(connection, lane)
            with lock.transaction(connection):
                harness._lane_arrival(connection, lane, 1.0)
                connection.execute('UPDATE source_meta SET range_revision = range_revision + 1')
            statements: list[str] = []
            connection.set_trace_callback(statements.append)
            try:
                assert harness._lane_refresh(connection, lane)
            finally:
                connection.set_trace_callback(None)
            assert lane.scan_work[0].late_ids == [13]
            assert not any('FROM late_ids' in statement for statement in statements)

        statements = []
        connection.set_trace_callback(statements.append)
        try:
            counts = harness._status_read(connection)
        finally:
            connection.set_trace_callback(None)
        reads = [statement for statement in statements if statement.lstrip().startswith('SELECT')]
        assert len(reads) == 8
        assert set(counts) == {
            'source_meta',
            'audit',
            'disposition_audit',
            'stream_adoptions',
            'discovery_ranges',
            'open_ranges',
            'source_late_ids',
            'late_ids',
            *(f'pending:{group}' for group in harness._GROUP_NAMES),
        }
        assert counts['source_meta'] == 1
        assert counts['discovery_ranges'] == 1
        assert counts['open_ranges'] == 0
        assert counts['pending:g00'] == 1
        assert counts['audit'] == 0
        assert counts['source_late_ids'] == 2
        assert counts['late_ids'] == 1


def test_repeat_identity_matches_within_duration_roundoff() -> None:
    runs = [_gate_result(repetition=number) for number in (1, 2, 3)]
    runs[0]['duration_secs'] = 599.9999999999995
    runs[0]['commit_latency_ms']['p99'] = 18
    harness._validate_repetitions(runs)
    assert harness._evaluate(runs)['verdict'] == 'pass'
    assert runs[0]['duration_secs'] == 599.9999999999995
    runs[2]['commit_latency_ms']['p99'] = 20.001
    assert harness._evaluate(runs)['verdict'] == 'fail'
    runs[0]['duration_secs'] = 600 - 0.000002
    with pytest.raises(ValueError, match='incompatible repeat identity: duration_secs'):
        harness._validate_repetitions(runs)
    runs[0]['duration_secs'] = 600
    runs[0]['checkpoint_interval_secs'] += 0.0000005
    with pytest.raises(ValueError, match='incompatible repeat identity: checkpoint_interval_secs'):
        harness._validate_repetitions(runs)


@pytest.mark.parametrize('cell', [6, 7])
def test_repeat_requirement_survives_a_hard_failure(tmp_path: Path, cell: int) -> None:
    runs = [_gate_result(cell, repetition=number) for number in (1, 2, 3)]
    for result in runs:
        _qualification_evidence(result, tmp_path / str(result['repetition']))
    if cell == 6:
        runs[0]['all_commit_latency_ms']['max'] = 500.001
    else:
        runs[0]['peak_wal_file_bytes'] = 67108865
    _qualification_evidence(runs[0], tmp_path / '1')
    for selected in (runs[:1], runs[:2]):
        evaluated = harness._evaluate(selected, qualification='normal-m7i-flex.large')
        assert evaluated['verdict'] == 'fail'
        assert evaluated['qualification_verdict'] == (
            'fail' if cell == 6 else ('repeat' if len(selected) == 1 else 'incomplete')
        )
        assert evaluated['repeat_required'] is True
        assert 'qualification_evidence' not in evaluated['safety_failures']
    evaluated = harness._evaluate(runs, qualification='normal-m7i-flex.large')
    assert evaluated['qualification_verdict'] == ('fail' if cell == 6 else 'pass')
    assert evaluated['repeat_required'] is False
    assert (
        harness._evaluate(runs[1:], qualification='normal-m7i-flex.large')['qualification_verdict']
        == 'incomplete'
    )
    if cell == 7:
        runs[1]['peak_wal_file_bytes'] = 268435457
        _qualification_evidence(runs[1], tmp_path / '2')
        later = harness._evaluate(runs, qualification='normal-m7i-flex.large')
        assert later['qualification_verdict'] == 'fail'
        assert later['safety_failures'] == ['wal_runtime_size']
        assert later['capacity_failures'] == ['peak_wal_file_bytes']
        runs[1]['peak_wal_file_bytes'] = 1024
        _qualification_evidence(runs[1], tmp_path / '2')
        runs[0]['peak_wal_file_bytes'] = 0.81 * 67108864
        runs[0]['all_commit_latency_ms']['max'] = 1001
        _qualification_evidence(runs[0], tmp_path / '1')
        assert (
            harness._evaluate(runs[:1], qualification='normal-m7i-flex.large')['repeat_required']
            is True
        )
        runs[0]['peak_wal_file_bytes'] = 67108864
        _qualification_evidence(runs[0], tmp_path / '1')
        assert (
            'peak_wal_file_bytes'
            not in harness._evaluate(runs[:1], qualification='normal-m7i-flex.large')['failures']
        )


@pytest.mark.parametrize('elapsed', [1.6, 1.600000001, 2.0, 2.000000001])
@pytest.mark.parametrize('cell', [1, 7])
def test_checkpoint_margin_uses_exact_elapsed_boundary(
    tmp_path: Path, elapsed: float, cell: int
) -> None:
    result = _gate_result(cell)
    result['checkpointers'][0].update(
        harness._checkpoint_summary(
            [
                {
                    'finished_at': elapsed,
                    'outcome': 'complete',
                    'reuse': 'none',
                    'result': [0, 0, 0],
                }
            ],
            0.0,
            elapsed,
            1.0,
        )
    )
    _qualification_evidence(result, tmp_path)
    evaluated = harness._evaluate([result], qualification='normal-m7i-flex.large')
    assert 'qualification_evidence' not in evaluated['safety_failures']
    assert ('checkpointers[0].longest_uncleared_intervals' in evaluated['failures']) == (
        elapsed > 2.0
    )
    assert evaluated['repeat_required'] is (cell == 1 and elapsed > 1.6)


@pytest.mark.parametrize(
    ('section', 'key', 'value'),
    [
        ('environment', 'platform', 'other-platform'),
        ('environment', 'machine', 'other-architecture'),
        ('environment', 'device_class', 'other-device'),
        ('environment', 'filesystem', 'other-filesystem'),
        ('environment', 'commit', 'other-commit'),
        ('model', 'seed', 'other-seed'),
        ('model', 'lane_rate', 999),
        ('population', 'harness_clock', 123),
        ('', 'checkpoint_interval_secs', 2),
        ('', 'min_samples', 99),
        ('', 'duration_secs', 601),
    ],
)
def test_evaluate_rejects_incompatible_repeats(
    tmp_path: Path, section: str, key: str, value: object
) -> None:
    runs = [_gate_result(repetition=number) for number in (1, 2, 3)]
    (runs[2][section] if section else runs[2])[key] = value
    paths: list[str] = []
    for index, result in enumerate(runs):
        result['commit_latency_ms']['p99'] = 18
        path = tmp_path / f'cell-{index}.json'
        path.write_text(harness.json.dumps(result))
        paths.append(str(path))
    assert harness._main(['evaluate', *paths]) == 2


@pytest.mark.parametrize(
    ('key', 'value'),
    [
        ('checkpointers', [dict[str, object]()]),
        ('checkpointers', {}),
        (
            'checkpointers',
            [{'halt': None, 'bound_crossings': 0, 'longest_uncleared_intervals': 'one'}],
        ),
        ('checkpointers', [{'halt': list[object](), 'longest_uncleared_intervals': 1}]),
        ('expired_attempts', []),
        ('expired_attempts', {'SQLite': 'one'}),
        ('commit_latency_ms', {'p99': float('nan'), 'p999': 10}),
        ('all_commit_latency_ms', {'max': None}),
        ('achieved_ratio', True),
        ('busy_timeouts', -1),
        ('processes', [{'exit_status': 'zero'}]),
        ('population_during_run', {'open_ranges': []}),
        ('capacity_replays', {'count': 'nine'}),
    ],
)
def test_evaluate_nested_malformed_metrics_are_usage_errors(
    tmp_path: Path, key: str, value: object
) -> None:
    result = _gate_result(10)
    result[key] = value
    path = tmp_path / 'cell-10.json'
    path.write_text(harness.json.dumps(result))
    assert harness._main(['evaluate', str(path)]) == 2


@pytest.mark.parametrize(
    ('section', 'key'),
    [
        ('environment', 'commit'),
        ('environment', 'filesystem'),
        ('model', 'seed'),
        ('population', 'row_counts'),
        ('', 'min_samples'),
    ],
)
def test_evaluate_rejects_incomplete_repeat_identity(
    tmp_path: Path, section: str, key: str
) -> None:
    paths: list[str] = []
    for repetition in (1, 2, 3):
        result = _gate_result(repetition=repetition)
        del (result[section] if section else result)[key]
        path = tmp_path / f'cell-{repetition}.json'
        path.write_text(harness.json.dumps(result))
        paths.append(str(path))
    assert harness._main(['evaluate', *paths]) == 2


@pytest.mark.parametrize('service_secs', [0.0, 0.3])
def test_lane_range_refresh_runs_on_tick_or_committed_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_secs: float
) -> None:
    connection, lock = _seeded(tmp_path)
    lane = harness._LaneState('g00')
    clock = [0.0]
    checks: list[float] = []
    refresh = harness._lane_refresh

    def timed_refresh(conn: sqlite3.Connection, state: harness._LaneState) -> bool:
        checks.append(clock[0])
        result = refresh(conn, state)
        clock[0] += service_secs
        return result

    monkeypatch.setattr(harness, '_lane_refresh', timed_refresh)
    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    with closing(connection), closing(lock):
        harness._lane_prepare(connection, lane, now=0.0)
        clock[0] = 0.2 + service_secs
        with lock.transaction(connection):
            _range(connection, 'opened')
            connection.execute(harness.BUMP_RANGE_REVISION)
        hinted_at = clock[0]
        harness._lane_prepare(connection, lane, now=clock[0], hinted=True)
        assert 'opened' in lane.source_ranges
        assert lane.next_coordination_at == hinted_at + service_secs + 1.0
        # A peer scan completion has no poller hint and waits for the rearmed tick.
        with lock.transaction(connection):
            connection.execute(harness.BUMP_RANGE_REVISION)
        for now in (1.0 + service_secs, lane.next_coordination_at - 0.01):
            clock[0] = now
            harness._lane_prepare(connection, lane, now=now)
        assert checks == [0.0, hinted_at]
        clock[0] = lane.next_coordination_at
        tick_at = clock[0]
        harness._lane_prepare(connection, lane, now=clock[0])
        assert checks == [0.0, hinted_at, tick_at]
        assert lane.next_coordination_at == tick_at + service_secs + 1.0
        clock[0] += 0.1
        forced_at = clock[0]
        harness._lane_prepare(connection, lane, now=clock[0], force=True)
        assert checks[-1] == forced_at
        assert lane.next_coordination_at == forced_at + service_secs + 1.0


@pytest.mark.parametrize('action', ['open', 'merge', 'close', 'retire', 'unchanged'])
@pytest.mark.parametrize('rollback', [False, True])
def test_poller_hints_publish_only_committed_range_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, action: str, rollback: bool
) -> None:
    connection, lock = _seeded(tmp_path)
    spec = _runner_spec(tmp_path, start_at=harness.time.monotonic())
    runtime = harness._Runtime(spec, lock)
    plan = harness._PollPlan(frontier=30)
    with closing(connection), closing(lock):
        with lock.transaction(connection):
            if action in {'merge', 'close', 'retire'}:
                _range(connection, 'existing', state='closed' if action == 'retire' else 'open')
                if action == 'retire':
                    connection.execute("UPDATE range_groups SET obligation_state='scan-complete'")
        if action == 'open':
            plan.opened.append(('new', 20, 21, 300.0))
        elif action == 'merge':
            plan.merged = ('existing', 20)
        elif action == 'close':
            plan.closed.append(('existing', [10]))

        def proposed_plan(_poller: harness._PollerState, _now: float) -> harness._PollPlan:
            return plan

        monkeypatch.setattr(harness, '_poller_plan', proposed_plan)
        original = harness._poller_observe

        def observe(
            conn: sqlite3.Connection,
            poller: harness._PollerState,
            proposed: harness._PollPlan,
            now: float,
        ) -> bool:
            changed = original(conn, poller, proposed, now)
            if rollback:
                raise ValueError('rollback probe')
            return changed

        monkeypatch.setattr(harness, '_poller_observe', observe)

        def execute(
            active: harness._Runtime, conn: sqlite3.Connection, writer: harness._Writer
        ) -> None:
            expectation = (
                pytest.raises(ValueError, match='rollback probe') if rollback else nullcontext()
            )
            with expectation:
                assert harness._attempt_write(active, conn, writer, spec.start_at)

        monkeypatch.setattr(harness, '_execute_arrivals', execute)
        with harness._range_hint_pipe(spec.database_path, 'g00') as reader:
            harness._poller_writer(runtime, connection)
            assert harness._take_range_hint(reader) is (not rollback and action != 'unchanged')
            assert not harness._take_range_hint(reader)


@pytest.mark.parametrize('topology', ['single', 'dedicated', 'named'])
def test_poller_hints_reach_each_topology(tmp_path: Path, topology: str) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection), closing(lock), lock.transaction(connection):
        _range(connection, 'scan', state='closed')
        connection.execute("INSERT INTO range_groups VALUES ('scan', 'g12', 1, 0, 1.0, 'pending')")
    cell = harness._Cell(
        1, 'documented', topology, 'NORMAL', 'two-level', 'representative', gated=True
    )
    options = harness._RunOptions(tmp_path)
    start = 100.0
    signals = tmp_path / 'hint-signals'
    signals.mkdir()
    run = harness._CellRun(cell, options, start, start + 2.0, {})
    specs = harness._cell_specs(run, tmp_path)
    children = [
        harness.spawn_child(
            [
                harness.sys.executable,
                '-c',
                (
                    'import sys; from tests.test_storage_contention import '
                    f'{_hint_topology_child.__name__}; '
                    f'{_hint_topology_child.__name__}(sys.argv[1])'
                ),
                spec.to_json(),
            ]
        )
        for spec in specs
    ]

    def wait_for_stage(stage: str) -> None:
        while not all((signals / f'{spec.role}.{stage}').exists() for spec in specs):
            assert all(child.poll() is None for child in children), (
                'topology child exited before signal'
            )
            harness.time.sleep(0.005)

    try:
        wait_for_stage('ready')
        (signals / 'publish').touch()
        wait_for_stage('first')
        (signals / 'merge').touch()
        assert all(child.wait() == 0 for child in children)
        results = [harness.json.loads(Path(spec.output_path).read_text()) for spec in specs]
    finally:
        for name in ('publish', 'merge', 'published', 'merged'):
            (signals / name).touch()
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()
    observed = [row for result in results for row in result['observed']]
    for group in harness._GROUP_NAMES:
        rows = [row for row in observed if row['group'] == group]
        hinted = [row for row in rows if row['hinted']]
        assert hinted, (group, rows)
        assert all(row['now'] < row['deadline'] for row in hinted), (group, rows)
        assert any(row['high'] == 200001 for row in hinted), (group, rows)
        assert not any(row['hinted'] and row['now'] < start + 0.4 for row in rows)
        converged = [row for row in rows if row['high'] == 200002]
        assert converged, (group, rows)
        assert all(not row['hinted'] and row['now'] >= row['deadline'] for row in converged)


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('only-exit-status', None),
        ('commits', {}),
        ('commits', [dict[str, object]()]),
        ('commits', [{'finished_at': 'bad'}]),
        ('reads', 'bad'),
        ('reads', [dict[str, object]()]),
        ('reads', [{'kind': 'status', 'start': 1, 'end': 2, 'counts': list[object]()}]),
        ('checkpoints', [None]),
        ('offered', []),
        ('expired_attempts', {'file': []}),
        ('offered_arrivals', [dict[str, object]()]),
        ('capacity_replays', [dict[str, object]()]),
        ('errors', 'bad'),
        ('finished_at', None),
        ('health_projection_reads', {}),
    ],
)
def test_child_corrupt_record_shape_produces_incomplete_summary(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    run = harness._CellRun(
        harness._CELLS[1], harness._RunOptions(tmp_path), 100.0, 100.1, {}, tmp_path
    )
    records: list[dict[str, Any]] = []
    specs = harness._cell_specs(run, tmp_path)
    for spec in specs:
        (tmp_path / f'{spec.role}-spec.json').write_text(spec.to_json())
        runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
        runtime.finished_at = spec.end_at
        raw = harness._process_records(runtime)
        records.append(raw)
        Path(spec.output_path).write_text(harness.json.dumps(raw))
    corrupt = {'exit_status': 0} if field == 'only-exit-status' else {**records[-1], field: value}
    path = Path(specs[-1].output_path)
    original = harness.json.dumps(corrupt).encode()
    path.write_bytes(original)
    process = Mock(wait=Mock(return_value=0))
    records[-1] = harness._child_result(specs[-1], process)
    result = harness._summarize(run, records)
    assert not result['evidence_complete']
    assert records[-1]['exit_status'] == 1
    assert 'without usable records' in records[-1]['errors'][0]
    assert records[-1]['offered'] == len(harness._offered_arrivals(specs[-1])) > 0
    assert result['backlog_at_end'] == result['offered_commits']
    assert path.read_bytes() == original
    assert (
        result['record_manifest'][-1]['raw_sha256'] == harness.hashlib.sha256(original).hexdigest()
    )


@pytest.mark.parametrize(
    ('target', 'field', 'value'),
    [
        ('raw', 'diagnostic_overflow', 10),
        ('raw', 'diagnostic_overflow', True),
        ('raw', 'diagnostic_overflow', -1),
        ('raw', 'diagnostic_overflow', 0.5),
        ('raw', 'diagnostic_overflow', float('nan')),
        ('raw', 'diagnostic_overflow', '0'),
        ('raw', 'evidence_complete', False),
        ('raw', 'evidence_complete', 1),
        ('raw', 'evidence_errors', ['diagnostic overflow: 10']),
        ('raw', 'diagnostic_cap', 0),
        ('raw', 'diagnostic_cap', True),
        ('raw', 'diagnostic_cap', -1),
        ('raw', 'diagnostic_cap', 1.5),
        ('raw', 'diagnostic_cap', float('nan')),
        ('spec', 'diagnostic_cap', 1),
        ('raw', 'extra_timing', False),
        ('raw', 'extra_timing', 1),
        ('spec', 'extra_timing', False),
        ('spec', 'extra_timing', 1),
        ('raw', 'diagnostics', {}),
        ('raw', 'diagnostics', [None]),
        ('raw', 'diagnostics', []),
        ('raw', 'diagnostics', [{'kind': 'bogus'}]),
        ('span', 'pid', 0),
        ('span', 'role', 'wrong'),
        ('span', 'start', {}),
        ('span', 'end', {'monotonic': 602.0, 'process_cpu': -1.0, 'thread_cpu': 1.0}),
        ('span', 'start', {'monotonic': 603.0, 'process_cpu': 2.0, 'thread_cpu': 2.0}),
        ('both', 'extra_timing', False),
        ('both', 'diagnostic_cap', 1),
        ('missing-raw', 'diagnostic_overflow', None),
        ('missing-raw', 'diagnostic_cap', None),
        ('missing-raw', 'evidence_complete', None),
        ('missing-raw', 'evidence_errors', None),
        ('missing-raw', 'extra_timing', None),
        ('missing-raw', 'diagnostics', None),
        ('missing-spec', 'extra_timing', None),
        ('missing-spec', 'diagnostic_cap', None),
    ],
)
def test_qualification_rejects_contradictory_diagnostic_evidence(
    tmp_path: Path,
    target: str,
    field: str,
    value: object,
) -> None:
    original = tmp_path / 'original'
    result = _gate_result(6)
    _qualification_evidence(result, original / 'records')
    assert (
        harness._evaluate([result], qualification='normal-m7i-flex.large')['qualification_verdict']
        == 'pass'
    )
    entry = result['record_manifest'][-1]
    for kind in ('spec', 'raw'):
        if kind not in target and target != 'both' and not (target == 'span' and kind == 'raw'):
            continue
        path = Path(entry[f'{kind}_path'])
        data = harness.json.loads(path.read_text())
        if target == 'span':
            data['diagnostics'][0][field] = value
        elif target.startswith('missing-'):
            del data[field]
        else:
            data[field] = value
        if target == 'both' and field == 'diagnostic_cap' and kind == 'raw':
            data['diagnostics'] *= 2
        content = harness.json.dumps(data).encode()
        path.write_bytes(content)
        entry[f'{kind}_sha256'] = harness.hashlib.sha256(content).hexdigest()
    for entry in result['record_manifest']:
        for key in ('spec_path', 'raw_path'):
            entry[key] = str(Path(entry[key]).relative_to(original))
    (original / 'cell-06.json').write_text(harness.json.dumps(result))
    copied = tmp_path / 'copied'
    harness.shutil.copytree(original, copied)
    harness.shutil.rmtree(original)
    evaluation = harness._evaluate_files([copied], qualification='normal-m7i-flex.large')[0]
    assert evaluation['verdict'] == 'pass'
    assert evaluation['qualification_verdict'] == 'fail'
    assert evaluation['safety_failures'] == ['qualification_evidence']


@pytest.mark.parametrize('outcome', [None, 'unknown-outcome'])
def test_qualification_rejects_malformed_disabled_diagnostic_outcome(
    tmp_path: Path,
    outcome: object,
) -> None:
    result = _gate_result()
    _qualification_evidence(result, tmp_path)
    entry = result['record_manifest'][0]
    path = Path(entry['raw_path'])
    raw = harness.json.loads(path.read_text())
    raw['diagnostics'] = [
        {
            'kind': 'writer-attempt',
            'role': 'site',
            'pid': 1,
            'thread_id': 1,
            'thread_name': 'writer',
            'outcome': outcome,
        }
    ]
    content = harness.json.dumps(raw).encode()
    path.write_bytes(content)
    entry['raw_sha256'] = harness.hashlib.sha256(content).hexdigest()
    evaluated = harness._evaluate([result], qualification='normal-m7i-flex.large')
    assert evaluated['qualification_verdict'] == 'fail'
    assert evaluated['safety_failures'] == ['qualification_evidence']


@pytest.mark.parametrize('cell', [10, 11])
def test_lifecycle_limit_preserves_repeats_and_commit_max(cell: int) -> None:
    runs = [_gate_result(cell, repetition=number) for number in (1, 2, 3)]
    for value, expected in [
        (127.6188, 'pass'),
        (160, 'pass'),
        (160.001, 'repeat'),
        (200, 'repeat'),
        (200.001, 'fail'),
    ]:
        runs[0]['full_lifecycle_latency_ms']['p99'] = value
        assert harness._evaluate(runs[:1])['verdict'] == expected
    runs[0]['full_lifecycle_latency_ms']['p99'] = 180
    assert harness._evaluate(runs)['verdict'] == 'pass'
    runs[2]['full_lifecycle_latency_ms']['p99'] = 200.001
    assert harness._evaluate(runs)['verdict'] == 'fail'
    runs[2]['full_lifecycle_latency_ms']['p99'] = 80
    runs[2]['all_commit_latency_ms']['max'] = (500 if cell == 10 else 1000) + 0.001
    assert harness._evaluate(runs)['verdict'] == 'fail'
    runs[2]['all_commit_latency_ms']['max'] = 100
    for key, value in [('peak_wal_file_bytes', 70 * 1024**2), ('achieved_ratio', 0.98)]:
        baseline = runs[2][key]
        runs[2][key] = value
        assert harness._evaluate(runs)['verdict'] == 'fail'
        runs[2][key] = baseline


@pytest.mark.parametrize(
    ('duration', 'accepted'),
    [
        (599.9999999999995, True),
        (600.0, True),
        (600.5, True),
        (600 - 0.0000005, True),
        (600 - 0.000002, False),
        (600 - 0.001, False),
        (599.0, False),
    ],
)
def test_duration_eligibility_tolerates_only_absolute_roundoff(
    tmp_path: Path,
    duration: float,
    accepted: bool,
) -> None:
    result = _gate_result(5)
    result['duration_secs'] = duration
    result['end_at'] = duration
    _qualification_evidence(result, tmp_path)
    assert harness._result_eligible(result) is accepted
    verdict = harness._evaluate([result], qualification='normal-m7i-flex.large')
    assert (verdict['qualification_verdict'] == 'pass') is accepted
    if not accepted:
        return
    for field, bad in [
        ('duration_override', True),
        ('min_samples', 0),
        ('population', {'gate_eligible': False}),
        ('evidence_complete', False),
    ]:
        previous = result[field]
        result[field] = bad
        assert (
            harness._evaluate([result], qualification='normal-m7i-flex.large')[
                'qualification_verdict'
            ]
            != 'pass'
        )
        result[field] = previous
    result['processes'][0]['errors'] = ['late-start']
    assert not harness._result_eligible(result)
    assert (
        harness._evaluate([result], qualification='normal-m7i-flex.large')['qualification_verdict']
        != 'pass'
    )


class _ListRecords(list[dict[str, Any]]):
    def append_if_room(self, record: dict[str, Any], cap: int) -> bool:
        if len(self) >= cap:
            return False
        self.append(record)
        return True

    def records(self) -> Generator[dict[str, Any]]:
        for record in self:
            yield dict(record)


def test_packed_retention_matches_list_output_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    outputs: list[bytes] = []
    for packed in (False, True):
        clock = _PhaseClock(monkeypatch)
        spec = _runner_spec(tmp_path, start_at=100, extra_timing=True)
        runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
        if packed:
            assert isinstance(runtime.commits, harness._PackedRecords)
            assert isinstance(runtime.reads, harness._PackedRecords)
            assert isinstance(runtime.diagnostics, harness._PackedRecords)
        else:
            for name in ('commits', 'reads', 'diagnostics'):
                monkeypatch.setattr(runtime, name, _ListRecords())
        rejected = True

        def perform(
            connection: sqlite3.Connection, _now: float, clock: _PhaseClock = clock
        ) -> str:
            nonlocal rejected
            clock.advance(0.125, 0.0625)
            if rejected:
                rejected = False
                raise harness._IndexChangedError
            connection.execute('CREATE TABLE sample (value INTEGER)')
            return 'poller'

        def read(clock: _PhaseClock = clock) -> dict[str, object]:
            clock.advance(0.25, 0.125)
            return {'zero': -0.0, 'identity': (2**65, 'site'), 'counts': {'late': 3}}

        def broken(clock: _PhaseClock = clock) -> None:
            clock.advance(0.25, 0.125)
            raise ValueError('read failed')

        writer = harness._Writer('poller', (100.0,), perform, reload=lambda _conn: None)
        with closing(harness.sqlite3.connect(':memory:')) as connection:
            assert harness._write_with_reload(runtime, connection, writer, 100.0)
        assert harness._record_read(runtime, 'health-projection', read)
        with pytest.raises(ValueError, match='read failed'):
            harness._record_read(runtime, 'status', broken)
        with (
            pytest.raises(ValueError, match='read failed'),
            harness._diagnostic_span(runtime, 'checkpoint'),
        ):
            broken()
        raw = harness._process_records(runtime)
        assert [r['outcome'] for r in raw['diagnostics']] == [
            'rejected',
            'committed',
            'completed',
            'failed',
            'failed',
        ]
        assert [r['attempt'] for r in raw['diagnostics'][:2]] == [1, 2]
        assert tuple(raw['reads'][0]['counts']['identity']) == (2**65, 'site')
        assert raw['reads'][1]['outcome'] == 'failed'
        assert len(raw['commits']) == 1
        outputs.append(harness.json.dumps(raw).encode())
    assert outputs[0] == outputs[1]


@pytest.mark.parametrize('populated', [False, True])
def test_process_export_preserves_json_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, populated: bool
) -> None:
    clock = _PhaseClock(monkeypatch)
    spec = _runner_spec(tmp_path, start_at=100, extra_timing=True, poller=True)
    runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
    if populated:
        runtime.commits.append(
            {'writer': 'poller', 'scheduled_at': 100.0, 'arrival_id': 'old', 'value': -0.0}
        )
        runtime.commits.append({'maintenance': True, 'value': 2**65})
        runtime.reads.append(
            {'kind': 'health-projection', 'counts': {'nested': (1, ('\u00e9', None))}}
        )
        runtime.diagnostics.append({'outcome': 'failed', 'error': 'quote"\n\\'})
        runtime.diagnostics.append({'outcome': 'rejected', 'attempt': 2})
        with runtime.phases.span('begin', 1):
            clock.advance(0.25, 0.125)
        runtime.errors.append('failed operation')
        runtime.diagnostic_overflow = 1
        runtime.phases.overflow = 1
    expected = harness.json.dumps(harness._process_records(runtime)).encode()
    harness._write_process_records(runtime)
    assert Path(spec.output_path).read_bytes() == expected


def test_repeat_runs_reach_a_final_verdict() -> None:
    runs = [_gate_result(repetition=number) for number in (1, 2, 3)]
    for result in runs:
        result['commit_latency_ms']['p99'] = 18
    assert harness._evaluate(runs[:1])['verdict'] == 'repeat'
    assert harness._evaluate(runs[:2])['verdict'] == 'incomplete'
    assert harness._evaluate(runs)['verdict'] == 'pass'
    runs[2]['commit_latency_ms']['p99'] = 21
    assert harness._evaluate(runs)['verdict'] == 'fail'


def test_retired_open_range_is_removed_from_poller_cache(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    with closing(connection):
        with closing(lock):
            with lock.transaction(connection):
                _range(connection, 'r1', group_state='satisfied-by-floor', high=2)
            poller = harness._PollerState()
            harness._poller_reload(connection, poller)
            assert [entry[0] for entry in poller.open_ranges] == ['r1']
            with lock.transaction(connection):
                harness._poller_observe(connection, poller, harness._PollPlan(frontier=1), 0.0)
            assert poller.open_ranges == []
            poller.cycle = next(
                index
                for index in range(1000)
                if harness._unit(poller.seed, index * 10_000 + 1) < 0.2
            )
            plan = harness._poller_plan(poller, 301.0)
            assert plan.closed == []
            with lock.transaction(connection):
                harness._poller_observe(connection, poller, plan, 301.0)
        assert connection.execute('SELECT COUNT(*) FROM discovery_ranges').fetchone()[0] == 0


def test_run_command_writes_an_ineligible_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _controlled_harness(monkeypatch):
        population = tmp_path / 'representative.db'
        assert (
            harness._main(
                [
                    'seed',
                    '--population',
                    'representative',
                    '--output',
                    str(population),
                    '--warmup-secs',
                    '1',
                ]
            )
            == 0
        )
        output = tmp_path / 'cell-01.json'
        assert (
            harness._main(
                [
                    'run',
                    '--cell',
                    '1',
                    '--population',
                    str(population),
                    '--work-dir',
                    str(tmp_path / 'work'),
                    '--device-class',
                    'test',
                    '--filesystem',
                    'test',
                    '--output',
                    str(output),
                    '--duration-secs',
                    '2',
                    '--checkpoint-interval-secs',
                    '0.1',
                ]
            )
            == 1
        )
        assert output.exists()
        assert harness.json.loads(output.read_text())['verdict'] == 'not-eligible'


def test_seeded_open_ranges_stay_pending_through_their_lifetime(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    harness._seed_cap_stress(path)
    connection = _database(path)
    with closing(connection):
        poller = harness._PollerState(hold_open=True)
        harness._poller_reload(connection, poller)
        with closing(_lock(path)) as lock:
            for cycle in range(1, 601):
                now = cycle / poller.poller_rate
                with lock.transaction(connection):
                    harness._poller_observe(
                        connection, poller, harness._poller_plan(poller, now), now
                    )
                if cycle % 120 == 0:
                    with lock.transaction(connection):
                        harness._lifecycle_replay(connection, 'g00', now)
                    harness._poller_reload(connection, poller)
                    open_pending = connection.execute(
                        'SELECT COUNT(*) FROM range_groups r '
                        'JOIN discovery_ranges d USING (obligation_id) '
                        "WHERE r.group_name = 'g00' AND r.obligation_state = 'pending' "
                        "AND d.range_state = 'open'"
                    ).fetchone()[0]
                    assert open_pending == harness.DEFAULT_MAX_OPEN_RANGES, now
                    surviving_seed = connection.execute(
                        'SELECT COUNT(*) FROM discovery_ranges '
                        "WHERE obligation_id LIKE 'seed-open-%' AND range_state = 'open'"
                    ).fetchone()[0]
                    assert surviving_seed == max(0, 500 - int(now / 0.6)), now


def test_short_cell_runs_in_single_and_dedicated_topologies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _controlled_harness(monkeypatch):
        harness._seed_representative(tmp_path / 'source.db', 1.0)
        original_run_process = harness._run_process
        original_child_result = harness._child_result
        original_summarize = harness._summarize
        monkeypatch.setattr(harness, 'spawn_child', _spawn_controlled_child)
        site_results: list[tuple[Path, dict[str, Any]]] = []
        summary_calls: list[tuple[harness._CellRun, list[dict[str, Any]]]] = []
        child_collections = 0

        def capture_site(spec: harness._ProcessSpec, *, persist: bool = False) -> dict[str, Any]:
            result = original_run_process(spec, persist=persist)
            assert persist
            site_results.append(
                (Path(spec.output_path), harness.json.loads(Path(spec.output_path).read_text()))
            )
            return result

        def capture_child(
            spec: harness._ProcessSpec, process: harness.ChildProcess
        ) -> dict[str, Any]:
            nonlocal child_collections
            assert site_results
            assert site_results[-1][0].exists()
            child_collections += 1
            return original_child_result(spec, process)

        def capture_summary(
            run: harness._CellRun, records: list[dict[str, Any]]
        ) -> dict[str, Any]:
            assert site_results[-1][0].exists()
            summary_calls.append((run, records))
            return original_summarize(run, records)

        monkeypatch.setattr(harness, '_run_process', capture_site)
        monkeypatch.setattr(harness, '_child_result', capture_child)
        monkeypatch.setattr(harness, '_summarize', capture_summary)
        required = {
            'environment',
            'cell',
            'model',
            'population',
            'gate_eligible',
            'repetition',
            'duration_secs',
            'drain_secs',
            'commit_latency_ms',
            'all_commit_latency_ms',
            'commit_latency_ms_by_class',
            'acquisition_ms',
            'service_ms',
            'process_local_wait_ms',
            'file_wait_ms',
            'sqlite_wait_ms',
            'expired_attempts',
            'busy_timeouts',
            'offered_commits',
            'achieved_commits',
            'achieved_ratio',
            'backlog_at_end',
            'index_reloads',
            'full_lifecycle_latency_ms',
            'checkpointers',
            'peak_wal_bytes_in_use',
            'peak_wal_file_bytes',
            'readers',
            'population_during_run',
            'capacity_replays',
            'processes',
        }
        for topology, process_count in [('single', 1), ('dedicated', 4)]:
            cell = harness._CELLS[0 if topology == 'single' else 1]
            options = harness._RunOptions(
                work_dir=tmp_path / topology,
                duration_secs=2,
                checkpoint_interval_secs=DEFAULT_CHECKPOINT_INTERVAL_SECS,
            )
            result = harness._run_cell(cell, tmp_path / 'source.db', options)
            site_path, site_result = site_results[-1]
            summary_run, summary_records = summary_calls[-1]
            assert site_path.name == 'site-records.json'
            assert site_path.parent.parent == options.work_dir
            assert {
                'commits',
                'reads',
                'checkpoints',
                'errors',
                'finished_at',
            } <= site_result.keys()
            assert site_result['commits']
            assert site_result['checkpoints']
            assert harness.json.loads(site_path.read_text()) == harness.json.loads(
                harness.json.dumps(site_result)
            )
            assert summary_records[0] == site_result
            assert len(summary_records) == process_count
            assert result == original_summarize(summary_run, summary_records)
            _assert_retained_short_cell(
                result, summary_run, summary_records, process_count, original_summarize
            )
            assert child_collections == process_count - 1
            assert required <= result.keys()
            assert len(result['processes']) == process_count
            assert len(result['checkpointers']) == 1
            assert all(p['exit_status'] == 0 for p in result['processes']), [
                p['errors'] for p in result['processes']
            ]
            assert not any(result['expired_attempts'].values())
            assert all(c['halt'] is None for c in result['checkpointers'])
            assert result['readers']['durations_ms']['health-projection']['max'] > 0
            assert all(p['health_projection_reads'] > 0 for p in result['processes'])
            assert not result['gate_eligible']


def test_shortened_runs_are_never_gate_eligible() -> None:
    result = _gate_result()
    result['gate_eligible'] = False
    assert harness._evaluate([result])['verdict'] == 'not-eligible'


def test_sqlite_acquisition_wait_is_capped_at_drain_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _ArrivalClock()
    monkeypatch.setattr(harness.time, 'monotonic', clock.monotonic)
    connection, lock = _seeded(tmp_path)
    monkeypatch.setattr(harness, '_DRAIN_SECS', 0.1)
    spec = _runner_spec(tmp_path, start_at=clock.now)
    runtime = harness._Runtime(spec, lock)
    monkeypatch.setattr(runtime.stop, 'wait', clock.wait)
    (configured_timeout,) = connection.execute('PRAGMA busy_timeout').fetchone()
    statements: list[str] = []

    def record(statement: str) -> None:
        statements.append(statement)
        if statement == f'PRAGMA busy_timeout={configured_timeout}':
            # Advance the fake clock by the real busy wait before the next deadline check.
            clock.now += 0.11

    connection.set_trace_callback(record)
    with closing(connection), closing(lock), closing(_database(tmp_path / 'source.db')) as blocker:
        blocker.execute('BEGIN IMMEDIATE')
        writer = harness._Writer('g00', (spec.end_at - 0.01,), lambda _connection, _now: 'test')
        harness._execute_arrivals(runtime, connection, writer)
        assert not runtime.commits
        assert runtime.expired_attempts == ['SQLite']
        timeouts = [
            int(statement.removeprefix('PRAGMA busy_timeout='))
            for statement in statements
            if statement.startswith('PRAGMA busy_timeout=')
        ]
        assert len(timeouts) == 2
        # SQLite accepts whole milliseconds; 10 ms of arrivals plus 100 ms of drain remain.
        assert timeouts[0] == pytest.approx(110, abs=1)
        assert timeouts[1] == configured_timeout
        blocker.rollback()


def test_unit_is_deterministic_and_bounded() -> None:
    values = [harness._unit('seed', index) for index in range(10_000)]
    assert values == [harness._unit('seed', index) for index in range(10_000)]
    assert all(0 <= value < 1 for value in values)
    assert abs(sum(values) / len(values) - 0.5) < 0.02


def test_worst_of_repeats_takes_the_worst_value() -> None:
    first = _gate_result()
    second = _gate_result()
    second['commit_latency_ms']['p99'] = 19
    second['achieved_ratio'] = 0.99
    worst = harness._worst_of([first, second])
    assert worst['commit_latency_ms']['p99'] == 19
    assert worst['achieved_ratio'] == 0.99
