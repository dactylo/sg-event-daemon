from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import sg_event_daemon._source_store as source_store_module
from sg_event_daemon._identity import stream_fingerprint
from tests.source._support import SOURCE_GENERATION as _SOURCE_GENERATION
from tests.source._support import open_source as _open_source
from tests.source._support import stored_range_details
from tests.source.test_state import open_source_range, record_source_late_ids

if TYPE_CHECKING:
    from pathlib import Path


def test_due_range_id_reads_only_deadline_and_active_eligibility(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        late_event_window_secs=10.0,
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 101, successful_clock=1.0)
        open_source_range(source, 201, 201, successful_clock=1.0)
        open_source_range(source, 301, 302, successful_clock=1.0)
        source._database.connection.execute(
            'UPDATE discovery_ranges SET visibility_deadline = 12 WHERE low_id < 301'
        )
        record_source_late_ids(source, 'range-301-302', (301,), successful_clock=2.0)
        source.record_successful_query(12.0)
        connection = vars(source)['_database'].connection
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        obligation_id = source_store_module.due_range_id(connection)

    assert obligation_id == 'range-301-302'
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


def test_empty_late_id_page_records_above_lowered_limit(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, max_source_late_ids=2) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 103, successful_clock=1.0)
        record_source_late_ids(source, opened.obligation_id, (101, 102), successful_clock=2.0)
    with _open_source(path, max_source_late_ids=1) as source:
        scan = source.begin_range_scan(opened.obligation_id, successful_clock=3.0)
        source.record_range_scan_page(
            opened.obligation_id,
            scanned_through=scan.scan_next_id - 1,
            event_ids=(),
            successful_clock=3.0,
        )
        assert source.successful_clock() == 3.0
        assert source.snapshot().ranges[0].late_id_count == 2


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
        late_event_window_secs=10.0,
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
        assert stored_range_details(state, obligation_id).scan_started_at == 11.0
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
        assert stored_range_details(state, obligation_id).degraded

        state.begin_range_scan(obligation_id, successful_clock=12.0)
        closed = state.record_range_scan_page(
            obligation_id,
            scanned_through=105,
            event_ids=(103,),
            successful_clock=12.0,
        )
        assert closed.range_changed
        assert closed.closed is not None
        assert (closed.closed.low_id, closed.closed.high_id, closed.closed.secs) == (
            101,
            105,
            11.0,
        )
        assert closed.closed.missing == ((102, 102), (104, 105))
        assert state.snapshot().ranges[0].state == 'closed'
        assert state.snapshot().ranges[0].late_id_count == 2


@pytest.mark.parametrize('incoming', [(), (101,), (101, 101, 102)])
def test_late_id_pages_use_indexed_candidates_and_preserve_exact_evidence(
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
        assert source.snapshot().ranges[0].late_id_count == len({101, *incoming})
        assert source.snapshot().ranges[0].state == 'open'
        assert tuple(
            int(row[0])
            for row in connection.execute(
                'SELECT event_id FROM source_late_ids WHERE obligation_id = ? ORDER BY event_id',
                (opened.obligation_id,),
            )
        ) == tuple(sorted({101, *incoming}))
        evidence_queries = [
            statement.lower()
            for statement in statements
            if statement.lstrip().lower().startswith('select') and 'source_late_ids' in statement
        ]
        assert evidence_queries
        assert all('count(*)' in query or 'where' in query for query in evidence_queries)


def test_late_ids_record_over_the_limit_for_open_ranges(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        max_source_late_ids=1,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        obligation_id = open_source_range(state, 101, 103, successful_clock=1.0).obligation_id
        record_source_late_ids(state, obligation_id, (101,), successful_clock=2.0)
        before = state.snapshot()

        record_source_late_ids(state, obligation_id, (102,), successful_clock=3.0)

        after = state.snapshot()
        assert after.frontier == before.frontier
        assert after.ranges[0].late_id_count == 2
        assert after.status == 'running'


def test_missing_runs_reads_only_one_open_ranges_recorded_ids(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 107, successful_clock=1.0)
        record_source_late_ids(
            source, opened.obligation_id, (101, 103, 104, 107), successful_clock=2.0
        )
        connection = source._database.connection
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        try:
            runs = source_store_module.missing_runs(connection, opened.obligation_id)
        finally:
            connection.set_trace_callback(None)
        assert runs == ((102, 102), (105, 106))
        assert sum('source_late_ids' in statement.lower() for statement in statements) == 1
        with pytest.raises(ValueError, match='open discovery range'):
            source_store_module.missing_runs(connection, 'unknown')
        connection.execute(
            'DELETE FROM source_late_ids WHERE obligation_id = ?', (opened.obligation_id,)
        )
        assert source_store_module.missing_runs(connection, opened.obligation_id) == ((101, 107),)
        connection.execute(
            "UPDATE discovery_ranges SET range_state = 'closed' WHERE obligation_id = ?",
            (opened.obligation_id,),
        )
        with pytest.raises(ValueError, match='open discovery range'):
            source_store_module.missing_runs(connection, opened.obligation_id)


def test_page_cap_during_pause_keeps_capacity_condition_and_returns_after(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db', max_open_ranges=1, max_range_query_pages=1
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 105, successful_clock=1)
        assert source.commit_observations((108,), successful_clock=2).pause is not None
        source.begin_range_scan(opened.obligation_id, successful_clock=3)
        source.record_range_scan_page(
            opened.obligation_id, scanned_through=101, event_ids=(101,), successful_clock=3
        )
        assert stored_range_details(source, opened.obligation_id).degraded
        assert source.snapshot().conditions == ('range-capacity-exhausted',)
        source.end_capacity_pause()
        assert source.snapshot().conditions == ('range-query-page-cap',)


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


def test_range_capacity_pauses_before_frontier_or_evidence_eviction(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        max_open_ranges=1,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        before = state.snapshot()

        assert state.commit_observations((104,), successful_clock=2.0).pause is not None

        after = state.snapshot()
        assert after.frontier == before.frontier
        assert after.ranges == before.ranges
        assert after.pending_obligations == before.pending_obligations
        assert after.status == 'range-capacity-exhausted'


def test_range_for_reads_only_one_range_projection(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        late_event_window_secs=10.0,
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
        stored = stored_range_details(source, 'range-101-102')

    assert retained == source_store_module.DiscoveryRange(
        obligation_id='range-101-102',
        low_id=101,
        high_id=102,
        opened_successful_at=1.0,
        state='open',
        late_id_count=1,
        scan_next_id=102,
        scan_cycle_pages=0,
    )
    assert stored == (11.0, 2, 0, 2.0, 0, False)
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
    assert touched == {'discovery_ranges', 'source_late_ids'}


def test_range_page_cap_survives_other_range_completion(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        groups={'editorial': 7},
        max_range_query_pages=1,
        late_event_window_secs=10.0,
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
        assert stored_range_details(state, second_id).degraded
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
        late_event_window_secs=10.0,
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
        assert stored_range_details(state, capped_id).degraded
        assert state.snapshot().conditions == ('range-query-page-cap',)


def test_range_staging_rejects_overlap_with_open_range(tmp_path: Path) -> None:
    # Prevents a merge from deleting late-id marks written while a range is open.
    queries = (
        'SELECT * FROM discovery_ranges ORDER BY obligation_id',
        'SELECT * FROM range_groups ORDER BY obligation_id, group_name',
        'SELECT * FROM source_late_ids ORDER BY obligation_id, event_id',
        'SELECT * FROM late_ids ORDER BY group_name, obligation_id, event_id',
    )
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        retained = open_source_range(state, 101, 199, successful_clock=10.0)
        connection = state._database.connection
        connection.execute(
            'INSERT INTO late_ids (group_name, obligation_id, event_id, terminal_state) '
            "VALUES ('editorial', ?, 150, 'done')",
            (retained.obligation_id,),
        )
        before = [connection.execute(query).fetchall() for query in queries]
        with pytest.raises(RuntimeError, match='open range overlaps an existing open range'):
            source_store_module.stage_open_range(
                connection,
                state._settings,
                low_id=150,
                high_id=250,
                successful_clock=20.0,
            )
        after = [connection.execute(query).fetchall() for query in queries]

    assert after == before


def test_restored_stream_identity_preserves_same_stream_evidence(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    changed_stream = stream_fingerprint(
        'https://replacement.example.invalid',
        'admin',
        'event-reader',
    )
    with _open_source(
        path,
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
        late_event_window_secs=10.0,
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
        assert stored_range_details(state, obligation_id).scan_pages == 1
        assert first_page.scan_cycle_pages == 1
        assert not stored_range_details(state, obligation_id).degraded

    with _open_source(path, max_range_query_pages=2) as state:
        reopened = state.snapshot().ranges[0]
        assert reopened.scan_next_id == 103
        assert stored_range_details(state, obligation_id).scan_pages == 1
        assert reopened.scan_cycle_pages == 1
        assert not stored_range_details(state, obligation_id).degraded

        resumed = state.begin_range_scan(obligation_id, successful_clock=12.0)
        assert resumed.scan_next_id == 103
        assert stored_range_details(state, obligation_id).scan_pages == 1
        assert resumed.scan_cycle_pages == 1
        assert not stored_range_details(state, obligation_id).degraded
        state.record_range_scan_page(
            obligation_id,
            scanned_through=104,
            event_ids=(103, 104),
            successful_clock=12.0,
        )
        capped = state.snapshot().ranges[0]
        assert capped.scan_next_id == 105
        assert stored_range_details(state, obligation_id).scan_pages == 2
        assert capped.scan_cycle_pages == 2
        assert stored_range_details(state, obligation_id).degraded
        assert state.snapshot().conditions == ('range-query-page-cap',)

    with _open_source(path, max_range_query_pages=2) as state:
        reopened = state.snapshot().ranges[0]
        assert reopened.scan_next_id == 105
        assert stored_range_details(state, obligation_id).scan_pages == 2
        assert reopened.scan_cycle_pages == 2
        assert stored_range_details(state, obligation_id).degraded
        assert state.snapshot().conditions == ('range-query-page-cap',)

        next_cycle = state.begin_range_scan(obligation_id, successful_clock=13.0)
        assert next_cycle.scan_next_id == 105
        assert stored_range_details(state, obligation_id).scan_pages == 2
        assert next_cycle.scan_cycle_pages == 0
        assert stored_range_details(state, obligation_id).degraded
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
        assert not stored_range_details(state, obligation_id).degraded
        assert state.snapshot().conditions == ()
