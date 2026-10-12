from __future__ import annotations

from contextlib import closing
from dataclasses import asdict
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from benchmarks import storage_contention as harness
from sg_event_daemon._config import DEFAULT_CHECKPOINT_INTERVAL_SECS
from sg_event_daemon._platform import release_flock, try_flock_exclusive
from sg_event_daemon._source_schema import SOURCE_SCHEMA, source_identity
from sg_event_daemon._sqlite import open_database
from sg_event_daemon._write_lock import SourceWriteLock
from tests._storage_support import open_group_on_source, write_context_for

if TYPE_CHECKING:
    import sqlite3


class _AdmissionProbe:
    def __init__(self, stage: str, reason: str, configured: int) -> None:
        self.stage = stage
        self.reason = reason
        self.configured = configured
        self.clock = 100.0
        self.stop = harness.threading.Event()
        self.begins: list[str] = []
        self.released = False

    def interrupt(self) -> None:
        if self.reason == 'deadline':
            self.clock += 0.05
        else:
            self.stop.set()

    def acquire(self, *, timeout: float) -> bool:
        assert timeout > 0
        if self.stage == 'local':
            self.interrupt()
        return True

    def authorize(self, action: int, name: str | None, value: str | None, *_: str | None) -> int:
        if (
            self.stage == 'pragma'
            and action == harness.sqlite3.SQLITE_PRAGMA
            and name == 'busy_timeout'
            and value not in {None, str(self.configured)}
        ):
            self.interrupt()
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


def _database(path: Path, *, synchronous: str = 'NORMAL') -> sqlite3.Connection:
    return open_database(
        path, source_identity('bench', 'source'), SOURCE_SCHEMA, synchronous=synchronous
    ).connection


def _lock(path: Path) -> SourceWriteLock:
    return SourceWriteLock(
        path.with_suffix('.db.write-lock'), timeout_secs=2.0, wait_phase_grace_secs=1.0
    )


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
        'seed': 'test',
        'scan_suppressed': (),
        'output_path': str(tmp_path / 'records.json'),
    }
    values.update(changes)
    return harness._ProcessSpec._from_json(harness.json.dumps(values))


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
    assert runtime.offered == 2
    assert len(runtime.commits) == 1
    assert runtime.commits[0]['finished_at'] == pytest.approx(spec.end_at + 0.06)
    # The second transaction finishes beyond the drain and remains backlog.
    assert clock.now == pytest.approx(spec.end_at + 0.13)
    assert runtime.offered - len(runtime.commits) == 1


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


def test_cap_stress_population_has_the_documented_shape(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    harness._seed_cap_stress(path)
    connection = _database(path)
    with closing(connection):
        counts = harness._population_counts(connection)
        assert counts['discovery_ranges'] == 1000
        assert counts['open_ranges'] == 100
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
            == 1300
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
        assert counts['open_ranges'] == 100
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
            == 1300
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
        assert counts['pending:g12'] == 100
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
    segments = scenario._segments('g12', 600)
    assert [rate for _, _, rate in segments] == [310, 10] * 5
    assert sum(end - start for start, end, rate in segments if rate == 310) == 300
    assert segments[-1][1] == 600
    assert scenario.duration_secs >= 600
    assert scenario._segments('g00', 600) == [(0, 600, 10)]


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


def test_checkpoint_failure_summary_stops_at_observed_lifetime(tmp_path: Path) -> None:
    spec = _runner_spec(tmp_path, start_at=100.0, end_at=160.0)
    runtime = harness._Runtime(spec, harness._BusyOnlyLock(timeout_secs=1))
    runtime.offered = 1000
    runtime.finished_at = 105.1
    runtime.checkpoint_halt = 'bound exceeded'
    runtime.errors.append('checkpoint halted')
    runtime.checkpoints.append(
        {
            'outcome': 'partial-progress',
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
    assert result['offered_commits'] == result['backlog_at_end'] == 1000


def test_checkpoint_loop_retains_bound_crossing_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection, lock = _seeded(tmp_path)
    runtime = harness._Runtime(_runner_spec(tmp_path, start_at=0, end_at=2), lock)
    real_checkpointer = harness.Checkpointer

    def checkpointer(
        conn: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> harness.Checkpointer:
        return real_checkpointer(
            conn,
            path,
            interval_secs=interval_secs,
            run_checkpoint=lambda: (0, 5170, 100),
            wal_limit_bytes=1,
        )

    monkeypatch.setattr(harness, 'Checkpointer', checkpointer)
    with closing(connection), closing(lock), pytest.raises(harness.DatabaseError):
        harness._checkpoint_loop(runtime, connection)
    assert len(runtime.checkpoints) == 1
    assert runtime.checkpoints[0]['result'] == (0, 5170, 100)
    assert runtime.checkpoints[0]['outcome'] == 'partial-progress'
    assert runtime.checkpoint_halt is not None


def test_checkpoint_summary_reports_the_longest_uncleared_time() -> None:
    attempts = [
        {'outcome': 'complete', 'result': [0, 10, 10], 'finished_at': second} for second in (1, 4)
    ]
    result = harness._checkpoint_summary(attempts, 0, 6, 1)
    assert result['longest_uncleared_secs'] == 3
    assert result['longest_uncleared_intervals'] == 3


def test_child_writes_setup_failure_records(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    spec = _runner_spec(tmp_path, database_path=str(tmp_path / 'missing' / 'source.db'))
    path = tmp_path / 'spec.json'
    path.write_text(spec._to_json())
    assert harness._child_main(['child', '--spec', str(path)]) == 1
    records = harness.json.loads((tmp_path / 'records.json').read_text())
    assert records['errors']
    assert capsys.readouterr().out == ''


def test_count_queries_agree_with_counted_tables() -> None:
    assert tuple(harness._COUNT_SQL) == harness._COUNTED_TABLES
    assert (
        tuple(query.removeprefix('SELECT COUNT(*) FROM ') for query in harness._COUNT_SQL.values())
        == harness._COUNTED_TABLES
    )


def test_evaluate_rejects_malformed_external_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / 'cell-01.json'
    path.write_text('{}')
    assert harness.main(['evaluate', str(path)]) == 2
    assert 'result' in capsys.readouterr().err


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
        assert any('FROM source_late_ids' in sql for sql in statements)
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
        harness._lane_prepare(connection, lane)
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


def test_lane_resumes_seeded_in_flight_delivery(tmp_path: Path) -> None:
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


def test_latency_runs_from_the_scheduled_time_and_includes_queueing(tmp_path: Path) -> None:
    connection, lock = _seeded(tmp_path)
    spec = _runner_spec(tmp_path, start_at=harness.time.monotonic() + 0.1)
    runtime = harness._Runtime(spec, lock)
    counter = 0

    def perform(_connection: sqlite3.Connection, _now: float) -> str:
        nonlocal counter
        if counter == 0:
            harness.time.sleep(0.1)
        counter += 1
        return 'test'

    writer = harness._Writer(
        'test', (spec.start_at, spec.start_at + 0.01, spec.start_at + 0.02), perform
    )
    with closing(connection), closing(lock):
        harness._execute_arrivals(runtime, connection, writer)
    assert runtime.offered == 3
    assert len(runtime.commits) == 3
    assert all(
        record['finished_at'] - record['scheduled_at'] >= 0.08 for record in runtime.commits[1:]
    )
    assert runtime.commits[0]['finished_at'] - runtime.commits[0]['begun_at'] >= 0.1


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
    assert harness.main(['run']) == 2
    assert harness.main(['evaluate', '/nonexistent/results']) == 2


def test_missing_child_records_preserve_all_offered_arrivals(tmp_path: Path) -> None:
    spec = _runner_spec(tmp_path, role='g10', lanes=('g10',))
    process = harness.spawn_child([harness.sys.executable, '-c', 'raise SystemExit(7)'])
    result = harness._child_result(spec, process)
    assert result['exit_status'] != 0
    assert result['offered'] == harness._offered_count(spec) > 0
    cell = harness._Cell(
        2, 'documented', 'dedicated', 'NORMAL', 'two-level', 'representative', gated=True
    )
    run = harness._CellRun(cell, harness._RunOptions(tmp_path), spec.start_at, spec.end_at, {})
    summary = harness._summarize(run, [result])
    assert summary['backlog_at_end'] == summary['offered_commits'] == result['offered']
    assert summary['achieved_commits'] == 0
    assert summary['achieved_ratio'] == 0


@pytest.mark.parametrize('after_begin', [False, True])
def test_only_pre_begin_busy_failures_continue(tmp_path: Path, after_begin: bool) -> None:
    connection, lock = _seeded(tmp_path)
    spec = _runner_spec(tmp_path, start_at=harness.time.monotonic() + 0.1)
    runtime = harness._Runtime(spec, lock)
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


@pytest.mark.parametrize('interleaving', ['replay', 'retire'])
def test_lane_revalidates_prepared_work_inside_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interleaving: str
) -> None:
    connection, lock = _seeded(tmp_path)
    with lock.transaction(connection):
        harness._lane_mark(connection, harness._LaneState('g00'))
        connection.execute('UPDATE source_meta SET frontier=200000')
        if interleaving == 'retire':
            _range(connection, 'retiring', state='closed')
            connection.execute(
                'INSERT INTO source_late_ids (obligation_id, event_id) VALUES (?, ?)',
                ('retiring', 12),
            )
    spec = _runner_spec(
        tmp_path, lanes=('g00',), start_at=harness.time.monotonic() + 0.1, duration_secs=0.05
    )
    runtime = harness._Runtime(spec, lock)
    original_prepare = harness._lane_prepare
    changed = False

    def prepare_then_change(conn: sqlite3.Connection, lane: harness._LaneState) -> None:
        nonlocal changed
        original_prepare(conn, lane)
        if changed:
            return
        changed = True
        with lock.transaction(conn):
            if interleaving == 'replay':
                harness._lifecycle_replay(conn, 'g00', 1)
            else:
                conn.execute('DELETE FROM discovery_ranges WHERE obligation_id=?', ('retiring',))
                conn.execute(harness.BUMP_RANGE_REVISION)

    monkeypatch.setattr(harness, '_lane_prepare', prepare_then_change)
    with closing(connection), closing(lock):
        harness._lane_writer(runtime, connection, 'g00')
        state = connection.execute(
            'SELECT cursor, delivery_floor, in_flight_event_id FROM group_state '
            "WHERE group_name='g00'"
        ).fetchone()
        if interleaving == 'replay':
            assert state == (190000, 190000, 190001)
            assert runtime.commits[0]['class'] == 'mark'
        else:
            assert state == (1, 0, None)
            assert runtime.commits[0]['class'] == 'cursor'
            assert connection.execute('SELECT COUNT(*) FROM late_ids').fetchone()[0] == 0
    assert runtime.offered == len(runtime.commits) == 1
    assert runtime.commits[0]['scheduled_at'] == spec.start_at
    assert runtime.index_reloads == 0


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
            'open_ranges': 100,
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
        'wait_timeouts': {},
        'busy_timeouts': 0,
        'checkpointers': [{'halt': None, 'longest_uncleared_intervals': 1}],
        'peak_wal_bytes_in_use': 1024,
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
    result['population_during_run']['open_ranges']['min'] = 99
    assert harness._evaluate([result])['verdict'] == 'not-eligible'
    result = _gate_result(10)
    result['capacity_replays']['count'] = 8
    assert harness._evaluate([result])['verdict'] == 'not-eligible'
    result = _gate_result(10)
    result['capacity_replays']['min_late_ids_before'] = 49_999
    assert harness._evaluate([result])['verdict'] == 'not-eligible'


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
    catchup['wait_timeouts'] = {'file': 1}
    assert harness._evaluate([catchup])['verdict'] == 'fail'
    catchup_full = _gate_result(5)
    catchup_full['cell']['synchronous'] = 'FULL'
    catchup_full['commit_latency_ms']['p99'] = 500
    assert harness._evaluate([catchup_full])['verdict'] == 'pass'
    lifecycle = _gate_result(11)
    lifecycle['full_lifecycle_latency_ms']['p99'] = 120
    assert harness._evaluate([lifecycle])['verdict'] == 'fail'
    lifecycle['full_lifecycle_latency_ms']['p99'] = 80
    lifecycle['all_commit_latency_ms']['max'] = 1500
    assert harness._evaluate([lifecycle])['verdict'] == 'fail'
    for key, value in [('backlog_at_end', 1), ('peak_wal_bytes_in_use', 70 * 1024**2)]:
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
    assert harness.main(['evaluate', *(str(path) for path in files)]) == 2
    for repetition, path in enumerate(files, 1):
        result['repetition'] = repetition
        path.write_text(harness.json.dumps(result))
    assert harness.main(['evaluate', *(str(path) for path in files)]) == 0


def test_evaluate_rejects_duplicate_file_inputs(tmp_path: Path) -> None:
    path = tmp_path / 'cell-01.json'
    result = _gate_result()
    result['commit_latency_ms']['p99'] = 18
    path.write_text(harness.json.dumps(result))
    assert harness.main(['evaluate', str(path), str(path), str(path)]) == 2
    assert harness.main(['evaluate', str(tmp_path), str(path)]) == 2


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
    assert harness.main(['evaluate', *paths]) == 2


@pytest.mark.parametrize(
    ('key', 'value'),
    [
        ('checkpointers', [dict[str, object]()]),
        ('checkpointers', {}),
        ('checkpointers', [{'halt': None, 'longest_uncleared_intervals': 'one'}]),
        ('checkpointers', [{'halt': list[object](), 'longest_uncleared_intervals': 1}]),
        ('wait_timeouts', []),
        ('wait_timeouts', {'SQLite': 'one'}),
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
    assert harness.main(['evaluate', str(path)]) == 2


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
    assert harness.main(['evaluate', *paths]) == 2


def test_overlap_chains_measure_transitively_overlapping_readers() -> None:
    assert harness._overlap_chains([(0, 2), (1, 3), (2.5, 4), (5, 6)]) == (3, 4.0)


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
        assert len(plan.closed) == len(plan.opened) == 3
        with closing(lock), lock.transaction(connection):
            harness._poller_observe(connection, poller, plan, 10.0)
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM discovery_ranges WHERE range_state = 'open'"
            ).fetchone()[0]
            == 100
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


def test_process_records_late_start_and_unexpected_thread_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection, lock = _seeded(tmp_path)
    connection.close()
    lock.close()
    past = harness.time.monotonic() - 1
    late = harness._run_process(_runner_spec(tmp_path, start_at=past, end_at=past + 0.1))
    assert late['exit_status'] == 1
    assert 'late-start' in late['errors']

    def broken(_runtime: harness._Runtime, _connection: sqlite3.Connection) -> None:
        raise ValueError('injected reader failure')

    monkeypatch.setattr(harness, '_status_reader', broken)
    failed = harness._run_process(_runner_spec(tmp_path, status_reader=True))
    assert failed['exit_status'] == 1
    assert any('injected reader failure' in error for error in failed['errors'])


def test_process_spec_round_trips_all_fields(tmp_path: Path) -> None:
    spec = _runner_spec(tmp_path, lanes=('g00',), scan_suppressed=('closed',))
    assert harness._ProcessSpec._from_json(spec._to_json()) == spec


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


def test_run_command_writes_an_ineligible_result(tmp_path: Path) -> None:
    population = tmp_path / 'representative.db'
    assert (
        harness.main(
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
        harness.main(
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
                    assert surviving_seed == max(0, 100 - int(now / 3)), now


def test_short_cell_runs_in_single_and_dedicated_topologies(tmp_path: Path) -> None:
    harness._seed_representative(tmp_path / 'source.db', 1.0)
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
        'wait_timeouts',
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
        cell = harness._Cell(
            1, 'documented', topology, 'NORMAL', 'two-level', 'representative', gated=True
        )
        options = harness._RunOptions(
            work_dir=tmp_path / topology,
            duration_secs=2,
            checkpoint_interval_secs=DEFAULT_CHECKPOINT_INTERVAL_SECS,
        )
        result = harness._run_cell(cell, tmp_path / 'source.db', options)
        assert required <= result.keys()
        assert len(result['processes']) == process_count
        assert len(result['checkpointers']) == process_count
        assert all(p['exit_status'] == 0 for p in result['processes']), [
            p['errors'] for p in result['processes']
        ]
        assert not any(result['wait_timeouts'].values())
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
    connection.set_trace_callback(statements.append)
    with closing(connection), closing(lock), closing(_database(tmp_path / 'source.db')) as blocker:
        blocker.execute('BEGIN IMMEDIATE')
        writer = harness._Writer('g00', (spec.end_at - 0.01,), lambda _connection, _now: 'test')
        harness._execute_arrivals(runtime, connection, writer)
        assert runtime.offered == 1
        assert not runtime.commits
        assert runtime.wait_timeouts == ['SQLite']
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
