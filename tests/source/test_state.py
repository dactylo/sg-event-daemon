from __future__ import annotations

import os
import select
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

import sg_event_daemon._source_store as source_store_module
from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._platform import AlreadyLockedError, FileLock
from sg_event_daemon._source_state import (
    DiscoveryRange,
    SourceCapacityError,
    SourceSnapshot,
    SourceState,
)
from sg_event_daemon._sqlite import DatabaseError, DatabaseHandle
from tests._storage_support import insert_replay_marker, orphan_group, write_context_for
from tests.source._support import (
    SITE as _SITE,
)
from tests.source._support import (
    SOURCE as _SOURCE,
)
from tests.source._support import (
    SOURCE_GENERATION as _SOURCE_GENERATION,
)
from tests.source._support import (
    STREAM as _STREAM,
)
from tests.source._support import (
    adopt_halted_stream as _adopt_halted_stream,
)
from tests.source._support import open_group as _open_group
from tests.source._support import (
    open_source as _open_source,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence


def _token_sequence(*tokens: str) -> Callable[[], str]:
    iterator: Iterator[str] = iter(tokens)
    return lambda: next(iterator)


def open_source_range(
    state: SourceState,
    low_id: int,
    high_id: int,
    *,
    successful_clock: float,
) -> DiscoveryRange:
    frontier = state.snapshot().frontier
    assert frontier < low_id
    state.commit_observations(
        (*range(frontier + 1, low_id), high_id + 1), successful_clock=successful_clock
    )
    retained = state.range_for(f'range-{low_id}-{high_id}')
    assert retained is not None
    return retained


def record_source_late_ids(
    state: SourceState,
    obligation_id: str,
    event_ids: Sequence[int],
    *,
    successful_clock: float,
) -> None:
    current = state.begin_range_scan(obligation_id, successful_clock=successful_clock)
    state.record_range_scan_page(
        obligation_id,
        scanned_through=max(event_ids, default=current.low_id),
        event_ids=event_ids,
        successful_clock=successful_clock,
        query_page_complete=False,
    )


def test_batch_atomically_commits_frontier_range_and_every_group_obligation(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, token_factory=_token_sequence('range-token-1')) as state:
        state.initialize(100, incarnation='incarnation-1')

        committed = state.commit_observations(
            (event_id for event_id in (102, 103)),
            successful_clock=10.0,
        )
        snapshot = state.snapshot()

        assert committed.range_changed
        assert snapshot.frontier == 103
        assert snapshot.successful_clock == 10.0
        assert snapshot.ranges == (
            DiscoveryRange(
                obligation_id='range-101-101',
                low_id=101,
                high_id=101,
                opened_successful_at=10.0,
                _visibility_deadline=310.0,
                _revision=1,
                _close_generation=0,
                state='open',
                late_ids=(),
                group_eligibility=(('editorial', 10.0), ('render', 10.0)),
                _scan_started_at=None,
                scan_next_id=101,
                _scan_pages=0,
                _degraded=False,
            ),
        )
        assert snapshot.pending_obligations == 2
        assert state._database.connection.execute(
            """SELECT group_name, group_generation, obligation_state
               FROM range_groups WHERE obligation_id = ? ORDER BY group_name""",
            ('range-101-101',),
        ).fetchall() == [('editorial', 7, 'pending'), ('render', 3, 'pending')]


def test_capacity_halt_condition_is_independent_of_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def reword_capacity(*_args: object, **_kwargs: object) -> object:
        raise SourceCapacityError('new capacity explanation', code='range-capacity-exhausted')

    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        monkeypatch.setattr(source_store_module, 'stage_open_range', reword_capacity)
        with pytest.raises(SourceCapacityError, match='new capacity explanation') as raised:
            open_source_range(source, 101, 101, successful_clock=1.0)
        assert raised.value.code == 'range-capacity-exhausted'
        assert source.snapshot().status == 'range-capacity-exhausted'


def test_distinct_identity_halts_reserve_fresh_adoption_ids_for_same_reason(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    changed_once = stream_fingerprint(
        'https://changed-once.example.invalid',
        'admin',
        'event-reader',
    )
    changed_twice = stream_fingerprint(
        'https://changed-twice.example.invalid',
        'admin',
        'event-reader',
    )
    tokens = _token_sequence('adoption-1', 'adoption-2')
    with _open_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')

    with _open_source(
        path,
        configured_stream=changed_once,
        token_factory=tokens,
    ) as source:
        first = source.reserve_stream_adoption(
            command_fingerprint='same-semantic-command',
            reason='approved identity recovery',
        )
        source.adopt_stream(
            adoption_id=first.adoption_id,
            command_fingerprint=first.command_fingerprint,
            new_stream_fingerprint=changed_once,
            reason='approved identity recovery',
            groups_parked=True,
        )
        source.initialize(200)

    with _open_source(
        path,
        configured_stream=changed_twice,
        token_factory=tokens,
    ) as source:
        second = source.reserve_stream_adoption(
            command_fingerprint='same-semantic-command',
            reason='approved identity recovery',
        )

    assert first.adoption_id == 'adoption-1'
    assert second.adoption_id == 'adoption-2'
    assert second.adoption_id != first.adoption_id


def test_first_start_initializes_latest_frontier_and_restarts(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as state:
        initial = state.snapshot()
        assert initial == SourceSnapshot(
            site=_SITE,
            source=_SOURCE,
            initialized=False,
            frontier=0,
            successful_clock=0.0,
            stream_fingerprint=_STREAM,
            stream_incarnation=None,
            source_generation=_SOURCE_GENERATION,
            status='uninitialized',
            conditions=(),
            ranges=(),
            pending_obligations=0,
            replay_markers=(),
            adoption_count=0,
        )

        initialized = state.initialize(41, incarnation='incarnation-1')
        assert initialized.initialized
        assert initialized.frontier == 41
        assert initialized.stream_incarnation == 'incarnation-1'
        handoff = vars(state)['_handoff']
        assert handoff.connection.execute('PRAGMA synchronous').fetchone() == (2,)

    with _open_source(path) as reopened:
        assert reopened.snapshot() == initialized


def test_killed_batch_transaction_reopens_at_prior_frontier(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as state:
        state.initialize(100, incarnation='incarnation-1')

    code = """
import sys
from pathlib import Path
from sg_event_daemon._source_state import SourceState
from tests._storage_support import write_context_for

path = Path(sys.argv[1])
state = SourceState.open(
    path, writes=write_context_for(path),
    create=True,
    site='studio',
    source='events',
    stream_fingerprint=sys.argv[2],
    source_generation='source-spawn-1',
)
def trace(statement):
    if statement.lstrip().startswith('UPDATE source_meta') and 'frontier' in statement:
        print('READY', flush=True)
        sys.stdin.read()
vars(state)['_database'].connection.set_trace_callback(trace)
state.commit_observations((102,), successful_clock=10.0)
"""
    environment = dict(os.environ)
    environment['PYTHONPATH'] = os.fspath(Path(__file__).parents[2] / 'src')
    process = subprocess.Popen(
        [sys.executable, '-c', code, str(path), _STREAM],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=environment,
    )
    assert process.stdout is not None
    return_code: int | None = None
    try:
        deadline = time.monotonic() + 2.0
        readable, _writable, _exceptional = select.select(
            (process.stdout,),
            (),
            (),
            max(0.0, deadline - time.monotonic()),
        )
        assert readable, 'child readiness timed out'
        assert os.read(process.stdout.fileno(), 4_096).decode().strip() == 'READY'
        process.kill()
        return_code = process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if process.stdin is not None:
            process.stdin.close()
        process.stdout.close()
    assert return_code is not None
    assert return_code != 0

    with _open_source(path) as reopened:
        snapshot = reopened.snapshot()
        assert snapshot.frontier == 100
        assert snapshot.ranges == ()
        assert snapshot.pending_obligations == 0


def test_later_matching_halt_reserves_fresh_adoption_id_for_same_command(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    changed_stream = stream_fingerprint(
        'https://replacement.example.invalid',
        'admin',
        'event-reader',
    )
    command_fingerprint = 'same-semantic-command'
    reason = 'approved identity recovery'
    tokens = _token_sequence('adoption-1', 'adoption-2')
    with _open_source(path, token_factory=tokens) as source:
        source.initialize(100, incarnation='incarnation-1')
        _adopt_halted_stream(
            source,
            command_fingerprint=command_fingerprint,
            reason=reason,
        )
        source.initialize(200)
        completed = source.reserve_stream_adoption(
            command_fingerprint=command_fingerprint,
            reason=reason,
        )
        assert completed.adoption_id == 'adoption-1'
        assert completed.recorded

    with _open_source(
        path,
        configured_stream=changed_stream,
        token_factory=tokens,
    ) as source:
        fresh = source.reserve_stream_adoption(
            command_fingerprint=command_fingerprint,
            reason=reason,
        )

    assert fresh.adoption_id == 'adoption-2'
    assert not fresh.recorded


def test_narrow_source_facade_reads_current_poll_range_and_deadline(
    tmp_path: Path,
) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        token_factory=_token_sequence('range-1'),
        visibility_overlap_secs=10.0,
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 102, successful_clock=1.0)
        source.record_successful_query(11.0)

        current = source._poll_state()
        retained = source.range_for('range-101-102')

        assert current.initialized
        assert current.frontier == 103
        assert current.successful_clock == 11.0
        assert current.source_generation == _SOURCE_GENERATION
        assert current.stream_incarnation == 'incarnation-1'
        assert source.successful_clock() == 11.0
        assert retained is not None
        assert (retained.low_id, retained.high_id) == (101, 102)
        assert source.range_for('missing') is None
        assert source.due_range_id() == 'range-101-102'


def test_orphan_full_transaction_commits_generation_retirement(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        token_factory=_token_sequence('open-token', 'close-token', 'marker-token'),
        visibility_overlap_secs=1.0,
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 110, successful_clock=1.0)
        record_source_late_ids(source, opened.obligation_id, (101,), successful_clock=2.0)
        source._close_range(opened.obligation_id, successful_clock=2.0)
        database = vars(source)['_database'].connection
        handoff = vars(source)['_handoff'].connection
        database_statements: list[str] = []
        handoff_statements: list[str] = []
        database.set_trace_callback(database_statements.append)
        handoff.set_trace_callback(handoff_statements.append)

        with _open_group(path) as group:
            group_handoff = vars(group)['_handoff'].connection
            group_statements: list[str] = []
            group_handoff.set_trace_callback(group_statements.append)
            group_synchronous = group_handoff.execute('PRAGMA synchronous').fetchone()
            orphaned = orphan_group(group)
        (marker,) = source.snapshot().replay_markers

        assert group_synchronous == (2,)
        assert group_statements.count('BEGIN IMMEDIATE') == 1
        assert not any(statement == 'BEGIN IMMEDIATE' for statement in database_statements)
        assert not any(statement == 'BEGIN IMMEDIATE' for statement in handoff_statements)
        assert orphaned.status == 'orphaned'
        assert orphaned.replay_required_from == 101
        assert handoff.execute(
            """
            SELECT active, lifecycle FROM groups
            WHERE group_name = 'editorial' AND group_generation = 7
            """
        ).fetchone() == (0, 'orphaned')
        assert handoff.execute(
            """SELECT COUNT(*) FROM range_groups
               WHERE group_name = 'editorial' AND group_generation = 7"""
        ).fetchone() == (0,)
        assert handoff.execute(
            """SELECT COUNT(*) FROM range_groups
               WHERE group_name = 'render' AND group_generation = 3
                 AND obligation_state = 'pending'"""
        ).fetchone() == (1,)
        assert source.snapshot().replay_markers == (marker,)
        assert handoff.execute(
            "SELECT command FROM audit WHERE group_name = 'editorial' AND command = 'orphan'"
        ).fetchall() == [('orphan',)]


def test_orphan_full_transaction_rolls_back_every_generation_row(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        token_factory=_token_sequence('open-token', 'close-token', 'marker-token'),
        visibility_overlap_secs=1.0,
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 110, successful_clock=1.0)
        record_source_late_ids(source, opened.obligation_id, (101,), successful_clock=2.0)
        source._close_range(opened.obligation_id, successful_clock=2.0)
        database = vars(source)['_database'].connection
        handoff = vars(source)['_handoff'].connection
        before = (
            tuple(handoff.execute('SELECT * FROM groups ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM range_groups ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM source_late_ids ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM late_ids ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM replay_markers ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM group_state ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM audit ORDER BY rowid').fetchall()),
        )
        handoff.execute(
            """
            CREATE TRIGGER fail_orphan_marker
            BEFORE INSERT ON replay_markers
            WHEN NEW.group_name = 'editorial' AND NEW.group_generation = 7
            BEGIN
                SELECT RAISE(ABORT, 'forced orphan failure');
            END
            """
        )
        database_statements: list[str] = []
        handoff_statements: list[str] = []
        database.set_trace_callback(database_statements.append)
        handoff.set_trace_callback(handoff_statements.append)

        with _open_group(path) as group:
            group_statements: list[str] = []
            vars(group)['_handoff'].connection.set_trace_callback(group_statements.append)
            with pytest.raises(DatabaseError, match='forced orphan failure'):
                orphan_group(group)

        after = (
            tuple(handoff.execute('SELECT * FROM groups ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM range_groups ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM source_late_ids ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM late_ids ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM replay_markers ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM group_state ORDER BY rowid').fetchall()),
            tuple(handoff.execute('SELECT * FROM audit ORDER BY rowid').fetchall()),
        )
        assert after == before
        assert group_statements.count('BEGIN IMMEDIATE') == 1
        assert not any(statement == 'BEGIN IMMEDIATE' for statement in database_statements)
        assert not any(statement == 'BEGIN IMMEDIATE' for statement in handoff_statements)


def test_pending_group_status_reads_unresolved_source_ids_before_lane_copy(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, visibility_overlap_secs=1.0) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 110, successful_clock=1.0)
        record_source_late_ids(source, opened.obligation_id, (101, 103), successful_clock=2.0)
        source._close_range(opened.obligation_id, successful_clock=2.0)
        assert source._database.connection.execute('SELECT COUNT(*) FROM late_ids').fetchone() == (
            0,
        )
        record = source.pending_group_record('render', limit=1)
        assert record.late_id_count == 2
        assert record.late_ids == (101,)


@pytest.mark.parametrize(('synchronous', 'expected'), [('NORMAL', 1), ('FULL', 2)])
def test_primary_synchronous_policy_survives_reopen_with_full_handoff(
    tmp_path: Path,
    synchronous: str,
    expected: int,
) -> None:
    for _ in range(2):
        with SourceState.open(
            tmp_path / 'source.db',
            writes=write_context_for(tmp_path / 'source.db'),
            create=True,
            site=_SITE,
            source=_SOURCE,
            stream_fingerprint=_STREAM,
            source_generation=_SOURCE_GENERATION,
            synchronous=synchronous,
        ) as state:
            primary = cast('DatabaseHandle', vars(state)['_database']).connection
            handoff = cast('DatabaseHandle', vars(state)['_handoff']).connection
            assert primary.execute('PRAGMA synchronous').fetchone() == (expected,)
            assert handoff.execute('PRAGMA synchronous').fetchone() == (2,)


def test_range_preflight_does_not_wait_for_a_writer(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as state:
        state.initialize(100, incarnation='incarnation-1')
        writer = sqlite3.connect(path, isolation_level=None)
        try:
            writer.execute('BEGIN IMMEDIATE')
            started = time.monotonic()
            assert state._preflight_range(102) == set()
            assert time.monotonic() - started < 0.5
        finally:
            writer.rollback()
            writer.close()


def test_record_successful_query_returns_none_without_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')

        def reject_snapshot() -> SourceSnapshot:
            raise AssertionError('record_successful_query composed a snapshot')

        monkeypatch.setattr(source, 'snapshot', reject_snapshot)

        assert source.record_successful_query(1.0) is None
        assert source.successful_clock() == 1.0


def test_reopen_prunes_unbound_ranges_without_discarding_pending_bindings(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, groups={'editorial': 7}) as source:
        source.initialize(100, incarnation='incarnation-1')
        source.commit_observations((103, 106), successful_clock=1.0)
        orphan, pending = 'range-101-102', 'range-104-105'
        connection = source._database.connection
        connection.execute('DELETE FROM range_groups WHERE obligation_id = ?', (orphan,))
    with _open_source(path, groups={'editorial': 7}) as reopened:
        assert reopened.range_for(orphan) is None
        assert reopened.range_for(pending) is not None
        assert reopened.snapshot().pending_obligations == 1


def test_retirement_releases_only_unbound_range_and_late_capacity(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, max_open_ranges=1, max_source_late_ids=1) as source:
        source.initialize(100, incarnation='incarnation-1')
        source.commit_observations((103,), successful_clock=1.0)
        obligation_id = 'range-101-102'
        record_source_late_ids(source, obligation_id, (101,), successful_clock=2.0)
        with _open_group(path, group='render', generation=3) as group:
            orphan_group(group)
        assert source.range_for(obligation_id) is not None
        with _open_group(path) as group:
            orphan_group(group)
        source.begin_cycle()
        assert source.range_for(obligation_id) is None
        with _open_group(path, group='newcomer', generation=8):
            source.commit_observations((106,), successful_clock=3.0)
            next_id = 'range-104-105'
            record_source_late_ids(source, next_id, (104,), successful_clock=4.0)
            assert source.snapshot().ranges[0].late_ids == (104,)


@pytest.mark.parametrize('range_count', [0, 1, 5])
def test_snapshot_query_count_is_constant_for_discovery_ranges(
    tmp_path: Path,
    range_count: int,
) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        connection = vars(source)['_database'].connection
        expected: list[DiscoveryRange] = []
        for index in range(range_count):
            low_id = 101 + index * 10
            high_id = low_id + 2
            obligation_id = f'range-{low_id}-{high_id}'
            connection.execute(
                """
                INSERT INTO discovery_ranges (
                    obligation_id, low_id, high_id, opened_successful_at,
                    visibility_deadline, source_revision,
                    close_generation, range_state, scan_started_at,
                    scan_next_id, scan_pages, degraded, scan_cycle_pages
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    obligation_id,
                    low_id,
                    high_id,
                    float(index + 1),
                    float(index + 11),
                    index + 1,
                    index,
                    'open',
                    float(index + 2),
                    low_id + 1,
                    index + 3,
                    index % 2,
                    index + 4,
                ),
            )
            connection.executemany(
                'INSERT INTO source_late_ids VALUES (?, ?)',
                ((obligation_id, low_id), (obligation_id, high_id)),
            )
            connection.executemany(
                'INSERT INTO range_groups VALUES (?, ?, ?, ?, ?, ?)',
                (
                    (obligation_id, 'editorial', 7, 0, float(index + 5), 'pending'),
                    (obligation_id, 'render', 3, 0, float(index + 6), 'pending'),
                ),
            )
            expected.append(
                DiscoveryRange(
                    obligation_id=obligation_id,
                    low_id=low_id,
                    high_id=high_id,
                    opened_successful_at=float(index + 1),
                    _visibility_deadline=float(index + 11),
                    _revision=index + 1,
                    _close_generation=index,
                    state='open',
                    late_ids=(low_id, high_id),
                    group_eligibility=(
                        ('editorial', float(index + 5)),
                        ('render', float(index + 6)),
                    ),
                    _scan_started_at=float(index + 2),
                    scan_next_id=low_id + 1,
                    _scan_pages=index + 3,
                    _degraded=bool(index % 2),
                    scan_cycle_pages=index + 4,
                )
            )
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        snapshot = source.snapshot()

        range_owned = tuple(
            statement
            for statement in statements
            if any(
                f'FROM {table}' in statement
                for table in ('discovery_ranges', 'source_late_ids', 'range_groups')
            )
        )
        assert len(range_owned) == 4
        assert snapshot.ranges == tuple(expected)


@pytest.mark.parametrize('failures', [frozenset({0}), frozenset({1}), frozenset({0, 1})])
def test_close_attempts_all_owned_cleanup(failures: frozenset[int]) -> None:
    attempts: list[int] = []
    failure = OSError('first cleanup failure')

    class Resource:
        def __init__(self, index: int) -> None:
            self.index = index

        def close(self) -> None:
            attempts.append(self.index)
            if self.index == 0 and 0 in failures:
                raise failure
            if self.index == 1 and 1 in failures:
                raise KeyboardInterrupt('second cleanup failure')

    state = SourceState.__new__(SourceState)
    vars(state).update(
        zip(('_database', '_handoff', '_lock'), (Resource(i) for i in range(3)), strict=True)
    )
    vars(state)['_writes'] = SimpleNamespace(lock=Resource(3))
    with pytest.raises(BaseException, match='fail') as caught:
        state.close()
    if 0 in failures:
        assert caught.value is failure
    else:
        assert isinstance(caught.value, KeyboardInterrupt)
    assert attempts == [0, 1, 2, 3]
    if len(failures) > 1:
        assert any('second cleanup failure' in note for note in failure.__notes__)


@pytest.mark.parametrize('database_failures', [frozenset({0}), frozenset({1}), frozenset({0, 1})])
def test_failed_open_attempts_all_owned_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    database_failures: frozenset[int],
) -> None:
    attempts: list[str] = []
    initiating = ValueError('construction failed')
    cleanup = OSError('database cleanup failure')
    original_database_close = DatabaseHandle.close
    original_lock_close = FileLock.close

    def close_database(handle: DatabaseHandle) -> None:
        original_database_close(handle)
        attempts.append('database')
        if len(attempts) - 1 in database_failures:
            raise cleanup

    def close_lock(lock: FileLock) -> None:
        original_lock_close(lock)
        attempts.append('lock')
        if len(database_failures) > 1:
            raise KeyboardInterrupt('lock cleanup failure')

    def fail_construction(*_args: object, **_kwargs: object) -> None:
        raise initiating

    monkeypatch.setattr(DatabaseHandle, 'close', close_database)
    monkeypatch.setattr(FileLock, 'close', close_lock)
    monkeypatch.setattr(SourceState, '__init__', fail_construction)
    with pytest.raises(ValueError, match='failed') as caught:
        _open_source(tmp_path / 'state.db')
    assert caught.value is initiating
    assert attempts == ['database', 'database', 'lock']
    assert len(caught.value.__notes__) == len(database_failures) + (len(database_failures) > 1)
    assert all(
        'database cleanup failure' in note
        for note in caught.value.__notes__[: len(database_failures)]
    )
    if len(database_failures) > 1:
        assert 'lock cleanup failure' in caught.value.__notes__[-1]


def test_source_audit_count_includes_stream_adoptions(
    tmp_path: Path,
) -> None:
    with _open_source(tmp_path / 'source.db', groups={'editorial': 7}) as source:
        source.initialize(100, incarnation='incarnation-1')
        _adopt_halted_stream(
            source,
            command_fingerprint='adoption-audit-count',
            reason='approved identity recovery',
        )

        assert source.audit_count() == 1
        path = tmp_path / 'source.db'
        expected_size = sum(
            candidate.stat().st_size
            for candidate in (
                path,
                Path(f'{path}-wal'),
                Path(f'{path}-shm'),
            )
            if candidate.exists()
        )
        assert source.database_size_bytes() == expected_size


def test_source_owner_lock_rejects_a_second_poller(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path), pytest.raises(AlreadyLockedError):
        _open_source(path)


def test_stream_adoption_schema_has_only_retired_evidence_counts(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        connection = vars(source)['_database'].connection
        assert [row[1] for row in connection.execute('PRAGMA table_info(stream_adoptions)')] == [
            'adoption_id',
            'command_fingerprint',
            'stream_fingerprint',
            'reason',
            'retired_range_count',
            'retired_late_id_count',
            'retired_marker_count',
        ]


def test_stream_identity_controls_preserve_new_mismatch_and_other_halt(
    tmp_path: Path,
) -> None:
    changed_stream = stream_fingerprint(
        'https://replacement.example.invalid',
        'admin',
        'event-reader',
    )
    identity_path = tmp_path / 'identity.db'
    with _open_source(identity_path) as source:
        source.initialize(100, incarnation='incarnation-1')

    with _open_source(identity_path, configured_stream=changed_stream) as changed:
        assert changed.snapshot().status == 'stream-identity-changed'
    with _open_source(identity_path, configured_stream=changed_stream) as still_changed:
        assert still_changed.snapshot().status == 'stream-identity-changed'

    capacity_path = tmp_path / 'capacity.db'
    with _open_source(
        capacity_path,
        max_open_ranges=1,
        token_factory=_token_sequence('range-token-1', 'range-token-2'),
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 101, successful_clock=1.0)
        with pytest.raises(SourceCapacityError, match='range-capacity-exhausted'):
            source.commit_observations((104,), successful_clock=2.0)
        assert source.snapshot().status == 'range-capacity-exhausted'

    with _open_source(capacity_path) as reopened:
        assert reopened.snapshot().status == 'range-capacity-exhausted'


def test_strict_ranges_ignore_clock_and_scan_cursor_only_progress(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        token_factory=_token_sequence('open-1'),
        visibility_overlap_secs=10.0,
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 105, successful_clock=1.0)
        assert source.strict_ranges_if_changed() is not None

        source.record_successful_query(2.0)
        source.begin_range_scan(opened.obligation_id, successful_clock=2.0)
        source.record_range_scan_page(
            opened.obligation_id,
            scanned_through=102,
            event_ids=(),
            successful_clock=2.0,
        )
        assert source.strict_ranges_if_changed() is None

        source.record_range_scan_page(
            opened.obligation_id,
            scanned_through=103,
            event_ids=(event_id for event_id in (103,)),
            successful_clock=3.0,
        )
        changed = source.strict_ranges_if_changed()

    assert changed is not None
    assert changed[0].late_ids == (103,)


def test_strict_ranges_rebuild_once_per_dirty_generation(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        first = source.strict_ranges_if_changed()
        clean = source.strict_ranges_if_changed()
        open_source_range(source, 101, 102, successful_clock=1.0)
        changed = source.strict_ranges_if_changed()

    assert first == ()
    assert clean is None
    assert changed is not None
    assert [(item.low_id, item.high_id) for item in changed] == [(101, 102)]


@pytest.mark.parametrize('coverage', ['floor', 'marker'])
def test_unneeded_gaps_do_not_consume_range_capacity(tmp_path: Path, coverage: str) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        delivery_floors={'editorial': 200 if coverage == 'floor' else 0},
        max_open_ranges=1,
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        if coverage == 'marker':
            insert_replay_marker(source._database.connection, 'editorial', 7, 0)
        committed = source.commit_observations((102, 104), successful_clock=1.0)
        assert not committed.range_changed
        snapshot = source.snapshot()
        assert snapshot.frontier == 104
        assert snapshot.ranges == ()
        assert source.due_range_id() is None
