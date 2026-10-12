from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest

import sg_event_daemon._source_store as source_store_module
from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._source_state import SourceCapacityError
from tests.source._support import SOURCE_GENERATION as _SOURCE_GENERATION
from tests.source._support import open_source as _open_source
from tests.source.test_state import open_source_range, record_source_late_ids

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


def _token_sequence(*tokens: str) -> Callable[[], str]:
    iterator: Iterator[str] = iter(tokens)
    return lambda: next(iterator)


def test_due_range_id_reads_only_deadline_and_active_eligibility(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        token_factory=_token_sequence('later-high', 'earliest', 'later-low'),
        visibility_overlap_secs=10.0,
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 101, successful_clock=1.0)
        open_source_range(source, 201, 201, successful_clock=1.0)
        open_source_range(source, 301, 301, successful_clock=1.0)
        source._database.connection.execute(
            'UPDATE discovery_ranges SET visibility_deadline = 12 WHERE low_id < 301'
        )
        record_source_late_ids(source, 'range-301-301', (301,), successful_clock=2.0)
        source.record_successful_query(12.0)
        connection = vars(source)['_database'].connection
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        obligation_id = source_store_module.due_range_id(connection)

    assert obligation_id == 'range-301-301'
    touched = {
        table
        for table in (
            'discovery_ranges',
            'range_groups',
            'replay_markers',
            'source_late_ids',
            'source_meta',
        )
        if any(table in statement.lower() for statement in statements)
    }
    assert touched == {'discovery_ranges', 'range_groups', 'source_meta'}


def test_empty_late_id_page_enforces_lowered_capacity(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, max_source_late_ids=2) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 103, successful_clock=1.0)
        record_source_late_ids(source, opened.obligation_id, (101, 102), successful_clock=2.0)
    with _open_source(path, max_source_late_ids=1) as source:
        with pytest.raises(SourceCapacityError, match='late-capacity-exhausted'):
            record_source_late_ids(source, opened.obligation_id, (), successful_clock=3.0)
        assert source.snapshot().ranges[0].late_ids == (101, 102)


def test_empty_source_commits_zero_frontier_with_initialized_marker(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as state:
        initialized = state.initialize(None, incarnation='incarnation-empty')

        assert initialized.initialized
        assert initialized.frontier == 0
        database = vars(state)['_database']
        row = database.connection.execute(
            'SELECT initialized, frontier, stream_incarnation FROM source_meta'
        ).fetchone()
        assert row == (1, 0, 'incarnation-empty')


def test_final_scan_must_begin_after_deadline_and_page_cap_degrades(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        groups={'editorial': 7},
        max_range_query_pages=1,
        token_factory=_token_sequence('open', 'close'),
        visibility_overlap_secs=10.0,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        obligation_id = open_source_range(state, 101, 105, successful_clock=1.0).obligation_id

        state.begin_range_scan(obligation_id, successful_clock=5.0)
        unchanged = state.record_range_scan_page(
            obligation_id,
            scanned_through=105,
            event_ids=(),
            successful_clock=5.0,
        )
        assert not unchanged.range_changed
        assert state.snapshot().ranges[0].state == 'open'

        reset = state.begin_range_scan(obligation_id, successful_clock=11.0)
        assert reset._scan_started_at == 11.0
        assert reset.scan_next_id == 101
        state.record_range_scan_page(
            obligation_id,
            scanned_through=102,
            event_ids=(101,),
            successful_clock=11.0,
        )
        degraded = state.snapshot().ranges[0]
        assert degraded.state == 'open'
        assert degraded.scan_next_id == 103
        assert degraded._degraded

        state.begin_range_scan(obligation_id, successful_clock=12.0)
        closed = state.record_range_scan_page(
            obligation_id,
            scanned_through=105,
            event_ids=(103,),
            successful_clock=12.0,
        )
        assert closed.range_changed
        assert state.snapshot().ranges[0].state == 'closed'
        assert state.snapshot().ranges[0].late_ids == (101, 103)


def test_late_id_capacity_halts_before_new_evidence_or_frontier(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        max_source_late_ids=1,
        token_factory=_token_sequence('range-1'),
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        obligation_id = open_source_range(state, 101, 103, successful_clock=1.0).obligation_id
        record_source_late_ids(state, obligation_id, (101,), successful_clock=2.0)
        before = state.snapshot()

        with pytest.raises(SourceCapacityError, match='late-capacity-exhausted'):
            record_source_late_ids(state, obligation_id, (102,), successful_clock=3.0)

        after = state.snapshot()
        assert after.frontier == before.frontier
        assert after.ranges[0].late_ids == (101,)
        assert after.status == 'late-capacity-exhausted'


@pytest.mark.parametrize('incoming', [(), (101,), (101, 101, 102)])
def test_late_id_pages_use_indexed_candidates_and_preserve_exact_capacity(
    tmp_path: Path, incoming: tuple[int, ...]
) -> None:
    with _open_source(tmp_path / 'source.db', max_source_late_ids=2) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 103, successful_clock=1.0)
        record_source_late_ids(source, opened.obligation_id, (101,), successful_clock=2.0)
        connection = source._database.connection
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        record_source_late_ids(source, opened.obligation_id, incoming, successful_clock=3.0)
        connection.set_trace_callback(None)
        assert source.snapshot().ranges[0].late_ids == tuple(sorted({101, *incoming}))
        evidence_queries = [
            statement.lower()
            for statement in statements
            if statement.lstrip().lower().startswith('select') and 'source_late_ids' in statement
        ]
        assert evidence_queries
        assert all('count(*)' in query or 'where' in query for query in evidence_queries)


def test_poll_state_reads_only_source_meta(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 102, successful_clock=1.0)
        connection = vars(source)['_database'].connection
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        current = source_store_module.poll_state(connection)

    assert current.initialized
    assert current.frontier == 103
    assert current.successful_clock == 1.0
    assert statements
    assert all('source_meta' in statement.lower() for statement in statements)


def test_range_capacity_halts_before_frontier_or_evidence_eviction(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        max_open_ranges=1,
        token_factory=_token_sequence('range-1', 'range-2'),
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        before = state.snapshot()

        with pytest.raises(SourceCapacityError, match='range-capacity-exhausted'):
            state.commit_observations((104,), successful_clock=2.0)

        after = state.snapshot()
        assert after.frontier == before.frontier
        assert after.ranges == before.ranges
        assert after.pending_obligations == before.pending_obligations
        assert after.status == 'range-capacity-exhausted'


def test_range_for_reads_only_one_range_projection(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        token_factory=_token_sequence('range-1', 'range-2'),
        visibility_overlap_secs=10.0,
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 102, successful_clock=1.0)
        open_source_range(source, 201, 202, successful_clock=2.0)
        record_source_late_ids(source, 'range-101-102', (101,), successful_clock=2.0)
        connection = vars(source)['_database'].connection
        connection.execute(
            'INSERT INTO replay_markers VALUES (?, ?, ?, ?, ?, ?)',
            ('retired', 1, _SOURCE_GENERATION, 'marker-1', 1, 'marker-digest'),
        )
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        retained = source_store_module.range_for(connection, 'range-101-102')

    assert retained == source_store_module.DiscoveryRange(
        obligation_id='range-101-102',
        low_id=101,
        high_id=102,
        opened_successful_at=1.0,
        _visibility_deadline=11.0,
        _revision=2,
        _close_generation=0,
        state='open',
        late_ids=(101,),
        group_eligibility=(('editorial', 1.0),),
        _scan_started_at=2.0,
        scan_next_id=102,
        _scan_pages=0,
        _degraded=False,
        scan_cycle_pages=0,
    )
    touched = {
        table
        for table in (
            'discovery_ranges',
            'range_groups',
            'replay_markers',
            'source_late_ids',
            'source_meta',
        )
        if any(table in statement.lower() for statement in statements)
    }
    assert touched == {'discovery_ranges', 'range_groups', 'source_late_ids'}


def test_range_merge_uses_intervals_and_never_shortens_frozen_times(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        group_holdbacks={'editorial': 500.0, 'render': 20.0},
        token_factory=_token_sequence('open-token', 'merge-token'),
        visibility_overlap_secs=300.0,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 199, successful_clock=10.0)

    with _open_source(
        path,
        group_holdbacks={'editorial': 1.0, 'render': 1.0},
        token_factory=_token_sequence('merge-token'),
        visibility_overlap_secs=1.0,
    ) as reopened:
        mutation = source_store_module.stage_open_range(
            reopened._database.connection,
            reopened._settings,
            low_id=150,
            high_id=250,
            successful_clock=20.0,
            new_token=lambda: 'merge-token',
        )
        merged = reopened.snapshot().ranges[0]

        assert (merged.low_id, merged.high_id) == (101, 250)
        assert merged.opened_successful_at == 10.0
        assert merged._visibility_deadline == 310.0
        assert merged.group_eligibility == (('editorial', 510.0), ('render', 30.0))
        assert merged._revision == 2
        assert mutation.revision == 2


def test_range_page_cap_survives_other_range_completion(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        groups={'editorial': 7},
        max_range_query_pages=1,
        token_factory=_token_sequence('open-1', 'open-2', 'close-1', 'close-2'),
        visibility_overlap_secs=10.0,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        first_id = open_source_range(state, 101, 103, successful_clock=1.0).obligation_id
        second_id = open_source_range(state, 201, 203, successful_clock=1.0).obligation_id
        for obligation_id, event_id in ((first_id, 101), (second_id, 201)):
            state.begin_range_scan(obligation_id, successful_clock=11.0)
            state.record_range_scan_page(
                obligation_id,
                scanned_through=event_id,
                event_ids=(event_id,),
                successful_clock=11.0,
            )
        assert state.snapshot().conditions == ('range-query-page-cap',)

        state.begin_range_scan(first_id, successful_clock=12.0)
        state.record_range_scan_page(
            first_id,
            scanned_through=103,
            event_ids=(102, 103),
            successful_clock=12.0,
        )
        ranges = {item.obligation_id: item for item in state.snapshot().ranges}
        assert ranges[first_id].state == 'closed'
        assert ranges[second_id].state == 'open'
        assert ranges[second_id]._degraded
        assert state.snapshot().conditions == ('range-query-page-cap',)

        state.begin_range_scan(second_id, successful_clock=13.0)
        state.record_range_scan_page(
            second_id,
            scanned_through=203,
            event_ids=(202, 203),
            successful_clock=13.0,
        )
        assert state.snapshot().conditions == ()


def test_range_page_cap_survives_unrelated_capacity_progress(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        groups={'editorial': 7},
        max_range_query_pages=1,
        token_factory=_token_sequence('open-1'),
        visibility_overlap_secs=10.0,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        capped_id = open_source_range(state, 101, 103, successful_clock=1.0).obligation_id
        state.begin_range_scan(capped_id, successful_clock=11.0)
        state.record_range_scan_page(
            capped_id,
            scanned_through=101,
            event_ids=(101,),
            successful_clock=11.0,
        )
        assert state.snapshot().conditions == ('range-query-page-cap',)

        state.commit_observations((104,), successful_clock=12.0)

        capped = next(item for item in state.snapshot().ranges if item.obligation_id == capped_id)
        assert capped.state == 'open'
        assert capped._degraded
        assert state.snapshot().conditions == ('range-query-page-cap',)


def test_restored_stream_identity_preserves_same_stream_evidence(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    changed_stream = stream_fingerprint(
        'https://replacement.example.invalid',
        'admin',
        'event-reader',
    )
    with _open_source(
        path,
        token_factory=_token_sequence('range-token'),
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        source.record_successful_query(7.5)
        open_source_range(source, 101, 105, successful_clock=7.5)
        original = source.snapshot()

    with _open_source(path, configured_stream=changed_stream) as changed:
        assert changed.snapshot().status == 'stream-identity-changed'

    with _open_source(path) as restored:
        snapshot = restored.snapshot()

    assert snapshot.status == 'running'
    assert snapshot.conditions == ()
    assert snapshot.frontier == original.frontier == 106
    assert snapshot.stream_incarnation == original.stream_incarnation == 'incarnation-1'
    assert snapshot.successful_clock == original.successful_clock == 7.5
    assert snapshot.ranges == original.ranges


def test_snapshot_rejects_unknown_condition_before_stream_mismatch_precedence(
    tmp_path: Path,
) -> None:
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        connection = vars(state)['_database'].connection
        connection.execute(
            """
            UPDATE source_meta
            SET stream_fingerprint = 'mismatched-stream',
                condition = 'unknown-source-condition'
            """,
        )

        with pytest.raises(RuntimeError, match='unknown persisted source condition'):
            state.snapshot()


def test_snapshot_rejects_unknown_condition_before_uninitialized_precedence(
    tmp_path: Path,
) -> None:
    with _open_source(tmp_path / 'source.db') as state:
        connection = vars(state)['_database'].connection
        connection.execute("UPDATE source_meta SET condition = 'unknown-source-condition'")

        with pytest.raises(RuntimeError, match='unknown persisted source condition'):
            state.snapshot()


def test_snapshot_rejects_unknown_persisted_source_condition(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        connection = vars(state)['_database'].connection
        connection.execute("UPDATE source_meta SET condition = 'unknown-source-condition'")

        with pytest.raises(RuntimeError, match='unknown persisted source condition'):
            state.snapshot()


def test_source_schema_contains_no_event_body_or_payload_column(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as state:
        state.initialize(0, incarnation='incarnation-1')
    connection = sqlite3.connect(path)
    try:
        tables = tuple(
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
        )
        columns = tuple(
            (table, str(row[1]))
            for table in tables
            for row in connection.execute(f'PRAGMA table_info({table})')
        )
    finally:
        connection.close()

    forbidden = ('body', 'event_json', 'payload')
    assert not any(
        fragment in column.lower() for _table, column in columns for fragment in forbidden
    )
    assert {'groups', 'group_state', 'source_meta'} <= set(tables)
    assert {'group_generation', 'ordering_mode', 'verified_incarnation', 'lifecycle'} <= {
        column for table, column in columns if table == 'groups'
    }


def test_successful_query_clock_never_moves_backward(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as state:
        state.initialize(100, incarnation='incarnation-1')
        state.record_successful_query(5.0)
        assert state.snapshot().successful_clock == 5.0

        state.record_successful_query(7.5)
        with pytest.raises(ValueError, match='successful clock'):
            state.record_successful_query(7.0)
        assert state.snapshot().successful_clock == 7.5


def test_unfinished_range_scan_persists_logical_and_cycle_budget(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        groups={'editorial': 7},
        max_range_query_pages=2,
        token_factory=_token_sequence('open'),
        visibility_overlap_secs=10.0,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        obligation_id = open_source_range(state, 101, 107, successful_clock=1.0).obligation_id
        state.begin_range_scan(obligation_id, successful_clock=11.0)
        state.record_range_scan_page(
            obligation_id,
            scanned_through=102,
            event_ids=(101, 102),
            successful_clock=11.0,
        )
        first_page = state.snapshot().ranges[0]
        assert first_page.scan_next_id == 103
        assert first_page._scan_pages == 1
        assert first_page.scan_cycle_pages == 1
        assert not first_page._degraded

    with _open_source(path, max_range_query_pages=2) as state:
        reopened = state.snapshot().ranges[0]
        assert reopened.scan_next_id == 103
        assert reopened._scan_pages == 1
        assert reopened.scan_cycle_pages == 1
        assert not reopened._degraded

        resumed = state.begin_range_scan(obligation_id, successful_clock=12.0)
        assert resumed.scan_next_id == 103
        assert resumed._scan_pages == 1
        assert resumed.scan_cycle_pages == 1
        assert not resumed._degraded
        state.record_range_scan_page(
            obligation_id,
            scanned_through=104,
            event_ids=(103, 104),
            successful_clock=12.0,
        )
        capped = state.snapshot().ranges[0]
        assert capped.scan_next_id == 105
        assert capped._scan_pages == 2
        assert capped.scan_cycle_pages == 2
        assert capped._degraded
        assert state.snapshot().conditions == ('range-query-page-cap',)

    with _open_source(path, max_range_query_pages=2) as state:
        reopened = state.snapshot().ranges[0]
        assert reopened.scan_next_id == 105
        assert reopened._scan_pages == 2
        assert reopened.scan_cycle_pages == 2
        assert reopened._degraded
        assert state.snapshot().conditions == ('range-query-page-cap',)

        next_cycle = state.begin_range_scan(obligation_id, successful_clock=13.0)
        assert next_cycle.scan_next_id == 105
        assert next_cycle._scan_pages == 2
        assert next_cycle.scan_cycle_pages == 0
        assert next_cycle._degraded
        assert state.snapshot().conditions == ('range-query-page-cap',)
        closed = state.record_range_scan_page(
            obligation_id,
            scanned_through=107,
            event_ids=(105, 106, 107),
            successful_clock=13.0,
        )
        assert closed.range_changed
        completed = state.snapshot().ranges[0]
        assert completed.state == 'closed'
        assert not completed._degraded
        assert state.snapshot().conditions == ()
