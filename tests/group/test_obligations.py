from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING, cast

import pytest

from sg_event_daemon._group_errors import InvalidTransitionError
from tests._storage_support import (
    bind_group,
    insert_range,
    insert_source_late_ids,
    open_test_group,
)
from tests.source._support import open_source

if TYPE_CHECKING:
    from pathlib import Path
    from sqlite3 import Connection

    from sg_event_daemon._group_state import GroupState


def _database_connection(state: GroupState) -> Connection:
    handle = object.__getattribute__(state, '_handoff')
    return cast('Connection', object.__getattribute__(handle, 'connection'))


def _open_group(
    path: Path,
    *,
    max_group_obligations: int = 1_000,
    max_late_ids_per_group: int = 50_000,
) -> GroupState:
    return open_test_group(
        path,
        max_group_obligations=max_group_obligations,
        max_late_ids_per_group=max_late_ids_per_group,
    )


def test_health_late_projection_uses_closed_pending_evidence(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'source.db') as state:
        connection = _database_connection(state)
        for obligation_id, range_state, row_state in (
            ('closed', 'closed', 'pending'),
            ('open', 'open', 'pending'),
            ('complete', 'closed', 'scan-complete'),
        ):
            insert_range(connection, obligation_id, 42, 50, state=range_state)
            bind_group(
                connection,
                obligation_id,
                'editorial',
                7,
                floor=41,
                eligibility_point=0,
                state=row_state,
            )
        insert_source_late_ids(connection, 'closed', (42, 43, 44))
        insert_source_late_ids(connection, 'open', (45,))
        insert_source_late_ids(connection, 'complete', (46,))
        snapshot = state.reserve_late_event(
            'closed', 43, expected_revision=state.snapshot().revision
        )
        state.complete_late_id('closed', 43, expected_revision=snapshot.revision)
        assert state.unresolved_late_id_projection(limit=1) == (2, (42,))


@pytest.mark.parametrize('low_id', [42, 45])
def test_live_irrelevant_prefix_is_atomic_and_keeps_direct_rejection(
    tmp_path: Path, low_id: int
) -> None:
    with _open_group(tmp_path / 'source.db') as state:
        connection = _database_connection(state)
        connection.execute("UPDATE groups SET ordering_mode='strict'")
        insert_range(connection, 'fence', low_id, low_id, state='closed')
        bind_group(connection, 'fence', 'editorial', 7, floor=41, eligibility_point=0)
        before = state.snapshot()
        advanced = state.advance_irrelevant_prefix(50, expected_revision=before.revision)
        assert advanced.cursor == low_id - 1
        assert advanced.revision == before.revision + (low_id > 42)
        with pytest.raises(InvalidTransitionError):
            state.advance_irrelevant(50, expected_revision=advanced.revision)
        assert state.snapshot() == advanced


def test_refresh_source_facts_and_rows_share_one_read_transaction(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with open_source(path, configured_stream='stream-v1') as source, _open_group(path) as state:
        source.initialize(50, incarnation='incarnation-1')
        connection = _database_connection(state)
        connection.execute("""
            UPDATE source_meta SET initialized=1, frontier=50, successful_clock=1,
            source_generation='source-spawn-1', stream_incarnation='incarnation-1'
            """)
        insert_range(connection, 'range-a', 42, 50)
        bind_group(connection, 'range-a', 'editorial', 7, floor=41, eligibility_point=0)
        writer = sqlite3.connect(path, isolation_level=None)
        changed = False

        def after_meta(statement: str) -> None:
            nonlocal changed
            if changed or 'FROM range_groups AS bindings' not in statement:
                return
            changed = True
            writer.execute("""
            UPDATE discovery_ranges SET range_state='closed', close_generation=1,
            source_revision=2 WHERE obligation_id='range-a'
            """)
            insert_source_late_ids(writer, 'range-a', (42,))
            writer.execute('UPDATE source_meta SET frontier=60, successful_clock=2')

        primary = state._database.connection
        primary.set_trace_callback(after_meta)
        try:
            before, old_ranges = state.refresh_ranges()
            assert old_ranges is not None
            (old_range,) = old_ranges
        finally:
            primary.set_trace_callback(None)
            writer.close()
        assert changed
        assert (before.frontier, before.source_successful_clock) == (50, 1)
        assert not old_range.closed
        assert old_range.late_ids == ()
        after, pending = state.refresh_ranges(before.range_revision)
        assert pending is not None
        assert (after.frontier, after.source_successful_clock) == (60, 2)
        assert pending[0].closed
        assert pending[0].late_ids == (42,)


def test_reservation_rejects_satisfied_group_membership(tmp_path: Path) -> None:

    with _open_group(tmp_path / 'source.db') as state:
        connection = _database_connection(state)
        insert_range(connection, 'range-a', 42, 50)
        bind_group(
            connection,
            'range-a',
            'editorial',
            7,
            floor=41,
            eligibility_point=0,
            state='satisfied-by-floor',
        )
        with pytest.raises(InvalidTransitionError):
            state.reserve_late_event('range-a', 42, expected_revision=state.snapshot().revision)


@pytest.mark.parametrize('kind', ['missing', 'open', 'complete'])
def test_running_scan_completion_rejects_invalid_obligation(tmp_path: Path, kind: str) -> None:
    with _open_group(tmp_path / 'source.db') as state:
        connection = _database_connection(state)
        if kind != 'missing':
            insert_range(
                connection, 'range-a', 42, 50, state='open' if kind == 'open' else 'closed'
            )
            bind_group(
                connection,
                'range-a',
                'editorial',
                7,
                floor=41,
                eligibility_point=0,
                state='scan-complete' if kind == 'complete' else 'pending',
            )
        before = state.snapshot()
        assert before.status == 'running'
        with pytest.raises(InvalidTransitionError, match='not closed and pending'):
            state.complete_obligation_scan('range-a')
        assert state.snapshot() == before


@pytest.mark.parametrize('unfinished', [False, True])
def test_scan_completion_requires_above_floor_source_ids_done(
    tmp_path: Path, unfinished: bool
) -> None:

    with _open_group(tmp_path / 'source.db') as state:
        connection = _database_connection(state)
        insert_range(connection, 'range-a', 40, 50, state='closed')
        bind_group(connection, 'range-a', 'editorial', 7, floor=41, eligibility_point=0)
        insert_source_late_ids(connection, 'range-a', [40, 42])
        if unfinished:
            with pytest.raises(InvalidTransitionError):
                state.complete_obligation_scan('range-a')
            assert state.pending_ranges()[0].obligation_id == 'range-a'
        else:
            snapshot = state.reserve_late_event(
                'range-a', 42, expected_revision=state.snapshot().revision
            )
            state.complete_late_id('range-a', 42, expected_revision=snapshot.revision)
            state.complete_obligation_scan('range-a')
            assert state.pending_ranges() == ()
            assert connection.execute('SELECT COUNT(*) FROM late_ids').fetchone()[0] == 0
            assert (
                connection.execute('SELECT obligation_state FROM range_groups').fetchone()[0]
                == 'scan-complete'
            )
