from __future__ import annotations

import os
import select
import sqlite3
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

import sg_event_daemon._source_store as source_store_module
from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._platform import AlreadyLockedError, FileLock
from sg_event_daemon._source_state import (
    DiscoveryRange,
    SourceCommitResult,
    SourceSnapshot,
    SourceState,
)
from sg_event_daemon._sqlite import DatabaseError, DatabaseHandle
from sg_event_daemon._strict import StrictBuffer
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
from tests.source._support import stored_range_details

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
    with _open_source(path) as state:
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
                state='open',
                late_id_count=0,
                scan_next_id=101,
            ),
        )
        assert snapshot.pending_obligations == 2
        assert state._database.connection.execute(
            """SELECT group_name, group_generation, obligation_state
               FROM range_groups WHERE obligation_id = ? ORDER BY group_name""",
            ('range-101-101',),
        ).fetchall() == [('editorial', 7, 'pending'), ('render', 3, 'pending')]


@pytest.mark.parametrize('limit', ['max_open_ranges', 'max_pending_group_obligations'])
def test_chunk_with_more_gaps_than_limit_commits_its_prefix(tmp_path: Path, limit: str) -> None:
    with _open_source(tmp_path / 'source.db', groups={'editorial': 7}) as source:
        source._settings = replace(source._settings, **{limit: 1})
        source.initialize(100, incarnation='incarnation-1')
        result = source.commit_observations((102, 103, 105), successful_clock=2)
        assert result.committed_count == 2
        assert result.range_changed
        assert result.pause is not None
        assert (
            result.pause.setting,
            result.pause.limit,
            result.pause.count,
            result.pause.blocked_id,
        ) == (limit, 1, 2, 105)
        snapshot = source.snapshot()
        assert snapshot.frontier == 103
        assert [(r.low_id, r.high_id) for r in snapshot.ranges] == [(101, 101)]
        assert snapshot.pending_obligations == 1
        assert not snapshot.replay_markers
        assert snapshot.conditions
        assert source._database.connection.execute(
            'SELECT obligation_id FROM range_groups'
        ).fetchall() == [('range-101-101',)]


def test_default_range_limit_admits_bulk_gaps(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site=_SITE,
        source=_SOURCE,
        stream_fingerprint=_STREAM,
        source_generation=_SOURCE_GENERATION,
    ) as source:
        connection = source._database.connection
        for group in ('editorial', 'render'):
            connection.execute(
                "INSERT INTO groups VALUES (?, 1, 'eager', ?, 'incarnation-1', 0, 1, 'active')",
                (group, 'https://studio.example.invalid'),
            )
            connection.execute(
                'INSERT INTO group_state '
                '(group_name, revision, cursor, delivery_floor, last_delivered_id) '
                'VALUES (?, 0, 0, 0, 0)',
                (group,),
            )
        source.initialize(100, incarnation='incarnation-1')
        admitted = source.commit_observations(range(102, 1101, 2), successful_clock=1)
        assert admitted.pause is None
        assert admitted.committed_count == 500
        snapshot = source.snapshot()
        assert not snapshot.conditions
        assert len(snapshot.ranges) == 500
        assert all(item.low_id == item.high_id for item in snapshot.ranges)
        assert snapshot.pending_obligations == 1_000

        refused = source.commit_observations((1102,), successful_clock=2)
        assert refused.committed_count == 0
        assert refused.pause is not None
        assert (refused.pause.setting, refused.pause.limit, refused.pause.count) == (
            'max_open_ranges',
            500,
            501,
        )
        assert source.snapshot().conditions == ('range-capacity-exhausted',)
        assert len(source.snapshot().ranges) == 500


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


def test_failed_open_releases_write_lock_without_masking_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / 'source.db'
    writes = write_context_for(path)
    original = writes.lock.close

    def close() -> None:
        original()
        raise RuntimeError('close failure')

    monkeypatch.setattr(writes.lock, 'close', close)
    with pytest.raises(ValueError, match='source limits') as raised:
        SourceState.open(
            path,
            writes=writes,
            create=True,
            site='studio',
            source='events',
            stream_fingerprint='fingerprint',
            source_generation='generation',
            max_open_ranges=0,
        )
    assert any('close failure' in note for note in raised.value.__notes__)
    assert writes.lock._entry is None
    assert not path.exists()


def test_first_gap_refused_commits_only_the_condition(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db', max_pending_group_obligations=1) as source:
        source.initialize(100, incarnation='incarnation-1')
        result = source.commit_observations((102, 104), successful_clock=2)
        assert result.committed_count == 0
        assert not result.range_changed
        assert result.pause is not None
        assert result.pause.blocked_id == 102
        snapshot = source.snapshot()
        assert snapshot.frontier == 100
        assert snapshot.successful_clock == 0
        assert not snapshot.ranges
        assert snapshot.pending_obligations == 0
        assert snapshot.conditions == ('obligation-capacity-exhausted',)


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
from dataclasses import replace
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
        readable, _writable, _exceptional = select.select(
            (process.stdout,),
            (),
            (),
            None,
        )
        assert readable, 'child stdout closed without readiness'
        assert os.read(process.stdout.fileno(), 4_096).decode().strip() == 'READY'
        process.kill()
        return_code = process.wait()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
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


def test_late_id_limit_commits_contiguous_prefix_then_refuses_new_gap(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db', max_source_late_ids=1) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 103, successful_clock=1)
        record_source_late_ids(source, opened.obligation_id, (101,), successful_clock=2)
        result = source.commit_observations((105, 106), successful_clock=3)
        assert result.committed_count == 2
        assert result.pause is None
        result = source.commit_observations((108,), successful_clock=4)
        assert result.committed_count == 0
        assert result.pause is not None
        assert (result.pause.setting, result.pause.count, result.pause.blocked_id) == (
            'max_source_late_ids',
            1,
            108,
        )
        assert source.snapshot().frontier == 106
        assert len(source.snapshot().ranges) == 1


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


def test_multiple_gaps_count_late_ids_once_per_transaction(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        statements: list[str] = []
        source._database.connection.set_trace_callback(statements.append)
        try:
            result = source.commit_observations((102, 104, 106), successful_clock=1)
        finally:
            source._database.connection.set_trace_callback(None)
        assert result.committed_count == 3
        assert (
            sum(
                statement.startswith('SELECT COUNT(*) FROM source_late_ids')
                for statement in statements
            )
            == 1
        )


def test_narrow_source_facade_reads_current_poll_range_and_deadline(
    tmp_path: Path,
) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        late_event_window_secs=10.0,
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


def test_open_existing_source_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / 'readonly-open.db'
    with _open_source(path):
        pass
    statements: list[str] = []
    original = sqlite3.connect

    def traced_connect(
        database: str | Path,
        *,
        isolation_level: None,
        timeout: float,
        uri: bool,
    ) -> sqlite3.Connection:
        connection = original(database, isolation_level=isolation_level, timeout=timeout, uri=uri)
        connection.set_trace_callback(statements.append)
        return connection

    with original(path, isolation_level=None) as holder:
        holder.execute('BEGIN IMMEDIATE')
        monkeypatch.setattr(sqlite3, 'connect', traced_connect)
        with SourceState.open(
            path,
            writes=replace(write_context_for(path), policy='one-attempt'),
            site=_SITE,
            source=_SOURCE,
            stream_fingerprint=_STREAM,
            source_generation='replacement',
        ) as state:
            assert state.snapshot().source_generation == _SOURCE_GENERATION
            assert not state.stamped
        holder.rollback()
    assert not any(
        sql.lstrip()
        .upper()
        .startswith(('BEGIN IMMEDIATE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE'))
        for sql in statements
    )


def test_orphan_full_transaction_commits_generation_retirement(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        late_event_window_secs=1.0,
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
        late_event_window_secs=1.0,
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
    with _open_source(path, late_event_window_secs=1.0) as source:
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


def test_projections_read_no_late_ids(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        connection = source._database.connection
        for index in range(20):
            low_id = 101 + index * 101
            obligation_id = f'range-{low_id}-{low_id + 99}'
            connection.execute(
                'INSERT INTO discovery_ranges ('
                'obligation_id, low_id, high_id, opened_successful_at, '
                'visibility_deadline, source_revision, close_generation, '
                'range_state, scan_next_id, scan_pages, degraded) '
                'VALUES (?, ?, ?, 1, 301, 1, 0, ?, ?, 0, 0)',
                (obligation_id, low_id, low_id + 99, 'open', low_id),
            )
            connection.executemany(
                'INSERT INTO source_late_ids VALUES (?, ?)',
                ((obligation_id, event_id) for event_id in range(low_id, low_id + 100)),
            )
            connection.execute(
                "INSERT INTO range_groups VALUES (?, 'editorial', 7, 0, 1, 'pending')",
                (obligation_id,),
            )
        source.invalidate_strict_ranges()
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        try:
            snapshot = source.snapshot()
            strict_ranges = source.strict_ranges_if_changed()
            refreshed = source.range_for('range-101-200')
            strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
            assert strict_ranges is not None
            strict.synchronize_ranges(strict_ranges)
            with _open_group(tmp_path / 'source.db') as group:
                group_connection = group._database.connection
                group_connection.set_trace_callback(statements.append)
                try:
                    _progress, pending = group.refresh_ranges()
                finally:
                    group_connection.set_trace_callback(None)
                assert pending is not None
                assert len(pending) == 20
        finally:
            connection.set_trace_callback(None)
        assert sum(item.late_id_count for item in snapshot.ranges) == 2_000
        assert sum(item.late_id_count for item in strict_ranges) == 2_000
        assert refreshed is not None
        assert refreshed.late_id_count == 100
        assert not any(
            'event_id' in statement.lower() and 'source_late_ids' in statement.lower()
            for statement in statements
        )


def test_range_preflight_does_not_wait_for_a_writer(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as state:
        state.initialize(100, incarnation='incarnation-1')
        writer = sqlite3.connect(path, isolation_level=None)
        try:
            writer.execute('BEGIN IMMEDIATE')
            state._database.connection.execute('PRAGMA busy_timeout=0')
            assert state._preflight_range(102) == set()
        finally:
            writer.rollback()
            writer.close()


def test_record_missing_rows_preserves_deadline_cursor_and_invalidates_projection(
    tmp_path: Path,
) -> None:
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 105, successful_clock=1.0)
        record_source_late_ids(state, gap.obligation_id, (102,), successful_clock=2.0)
        before = state.range_for(gap.obligation_id)
        assert before is not None
        before_details = stored_range_details(state, before.obligation_id)
        assert state.strict_ranges_if_changed() is not None
        assert state.strict_ranges_if_changed() is None
        committed = state.record_missing_rows(gap.obligation_id, (104,), successful_clock=3.0)
        after = state.range_for(gap.obligation_id)
        assert after is not None
        assert committed.range_changed
        assert after.late_id_count == 2
        assert (
            stored_range_details(state, after.obligation_id).scan_started_at,
            after.scan_next_id,
            after.scan_cycle_pages,
        ) == (
            before_details.scan_started_at,
            before.scan_next_id,
            before.scan_cycle_pages,
        )
        assert state.successful_clock() == 3.0
        assert state.strict_ranges_if_changed() is not None


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


def test_refused_candidate_rolls_back_its_gap_cap_collapse(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db', groups={'editorial': 7}, max_group_obligations=1, max_open_ranges=1
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        source.commit_observations((102,), successful_clock=1)
        before = source.snapshot()
        result = source.commit_observations((104,), successful_clock=2)
        assert result.pause is not None
        assert source.snapshot().ranges == before.ranges
        assert source.snapshot().replay_markers == ()
        assert source.snapshot().pending_obligations == 1
        assert source.group_snapshot('editorial').replay_required_from is None


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
            assert source.snapshot().ranges[0].late_id_count == 1


def test_retiring_empty_orphan_reports_range_change_at_late_capacity(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db', max_source_late_ids=1) as source:
        source.initialize(100, incarnation='incarnation-1')
        source.commit_observations((102, 104), successful_clock=1)
        record_source_late_ids(source, 'range-103-103', (103,), successful_clock=2)
        assert source.strict_ranges_if_changed() is not None
        connection = source._database.connection
        connection.execute('DELETE FROM range_groups WHERE obligation_id = ?', ('range-101-101',))
        result = source.commit_observations((106,), successful_clock=3)
        assert result.pause is not None
        assert result.pause.setting == 'max_source_late_ids'
        assert result.range_changed
        assert source.range_for('range-101-101') is None
        assert source.range_for('range-103-103') is not None
        assert source.snapshot().replay_markers == ()
        assert source.group_snapshot('editorial').status == 'running'
        assert source.strict_ranges_if_changed() is not None


def test_schema_without_metadata_row_is_refused_as_uninitialized(tmp_path: Path) -> None:
    path = tmp_path / 'uninitialized.db'
    with _open_source(path) as state:
        state._database.connection.execute('DELETE FROM source_meta')
    with pytest.raises(source_store_module.SourceIdentityError) as raised:
        _open_source(path)
    assert str(path) in str(raised.value)
    assert 'source-database-uninitialized' in str(raised.value)


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
                    state='open',
                    late_id_count=2,
                    scan_next_id=low_id + 1,
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
        assert len(range_owned) == 3
        assert snapshot.ranges == tuple(expected)


@pytest.mark.parametrize('failures', [frozenset({0}), frozenset({1}), frozenset({0, 1})])
def test_close_attempts_all_owned_cleanup(failures: frozenset[int]) -> None:
    attempts: list[int] = []
    failure = OSError('first cleanup failure')

    class Resource:
        def __init__(self, index: int) -> None:
            self._index = index

        def close(self) -> None:
            attempts.append(self._index)
            if self._index == 0 and 0 in failures:
                raise failure
            if self._index == 1 and 1 in failures:
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


def test_stamp_prunes_unbound_ranges_without_discarding_pending_bindings(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, groups={'editorial': 7}) as source:
        source.initialize(100, incarnation='incarnation-1')
        source.commit_observations((103, 106), successful_clock=1.0)
        orphan, pending = 'range-101-102', 'range-104-105'
        connection = source._database.connection
        connection.execute('DELETE FROM range_groups WHERE obligation_id = ?', (orphan,))
    with _open_source(path, groups={'editorial': 7}) as reopened:
        assert reopened.range_for(orphan) is not None
        reopened.stamp_generation()
        assert reopened.range_for(orphan) is None
        assert reopened.range_for(pending) is not None
        assert reopened.snapshot().pending_obligations == 1


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
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 101, successful_clock=1.0)
        assert source.commit_observations((104,), successful_clock=2.0).pause is not None
        assert source.snapshot().status == 'range-capacity-exhausted'

    with _open_source(capacity_path) as reopened:
        assert reopened.snapshot().status == 'range-capacity-exhausted'


def test_strict_ranges_ignore_clock_and_scan_cursor_only_progress(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        late_event_window_secs=10.0,
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
    assert changed[0].late_id_count == 1


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


@pytest.mark.parametrize('scan_page', [False, True])
def test_range_closes_atomically_with_final_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scan_page: bool,
) -> None:
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 102, successful_clock=1.0)
        state.record_missing_rows(gap.obligation_id, (101,), successful_clock=2.0)
        if scan_page:
            state.begin_range_scan(gap.obligation_id, successful_clock=3.0)
        assert state.strict_ranges_if_changed() is not None
        assert state.strict_ranges_if_changed() is None

        def record() -> SourceCommitResult:
            if scan_page:
                return state.record_range_scan_page(
                    gap.obligation_id,
                    scanned_through=102,
                    event_ids=(102,),
                    successful_clock=3.0,
                )
            return state.record_missing_rows(gap.obligation_id, (102,), successful_clock=3.0)

        def fail_close(*_args: object, **_kwargs: object) -> bool:
            raise RuntimeError('close write failed')

        with monkeypatch.context() as patch:
            patch.setattr(source_store_module, 'stage_close_range', fail_close)
            with pytest.raises(RuntimeError, match='close write failed'):
                record()
        retained = state.range_for(gap.obligation_id)
        assert retained is not None
        assert (retained.state, retained.late_id_count) == ('open', 1)
        assert state.successful_clock() == 2.0
        assert state.strict_ranges_if_changed() is None
        committed = record()
        assert committed.range_changed
        assert committed.closed is not None
        assert (
            committed.closed.low_id,
            committed.closed.high_id,
            committed.closed.secs,
            committed.closed.missing,
        ) == (101, 102, 2.0, ())
        retained = state.range_for(gap.obligation_id)
        assert retained is not None
        assert (
            retained.state,
            retained.late_id_count,
            stored_range_details(state, retained.obligation_id).close_generation,
        ) == (
            'closed',
            2,
            1,
        )
        projected = state.strict_ranges_if_changed()
        assert projected is not None
        assert projected[0].state == 'closed'
        assert state._close_range(gap.obligation_id, successful_clock=3.0).closed is None
