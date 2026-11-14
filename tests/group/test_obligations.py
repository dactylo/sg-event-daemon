from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from sg_event_daemon._group_errors import InvalidTransitionError
from sg_event_daemon._group_types import CompleteLateStep, ReserveLateStep
from tests._storage_support import (
    bind_group,
    insert_range,
    insert_source_late_ids,
    open_test_group,
)
from tests.group._support import commit_step

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


def test_completion_excuses_exactly_the_missing_ids(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'source.db') as state:
        connection = _database_connection(state)
        insert_range(connection, 'range-a', 42, 50, state='closed')
        bind_group(connection, 'range-a', 'editorial', 7, floor=41, eligibility_point=0)
        insert_source_late_ids(connection, 'range-a', (42, 43, 45))
        snapshot = commit_step(
            state, ReserveLateStep('range-a', 43), expected_revision=state.snapshot().revision
        )
        commit_step(state, CompleteLateStep('range-a', 43), expected_revision=snapshot.revision)
        state.complete_obligation_scan('range-a', excused=((42, 42), (45, 45)))
        assert state.pending_ranges() == ()
        assert (
            connection.execute('SELECT operational_late_id_count FROM group_state').fetchone()[0]
            == 0
        )


def test_missing_late_runs_matches_completion_predicate(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'source.db') as state:
        connection = _database_connection(state)
        connection.execute('UPDATE group_state SET delivery_floor=103')
        insert_range(connection, 'range-a', 101, 110, state='closed')
        bind_group(connection, 'range-a', 'editorial', 7, floor=103, eligibility_point=0)
        insert_source_late_ids(connection, 'range-a', range(101, 111))
        for event_id in (104, 108):
            snapshot = commit_step(
                state,
                ReserveLateStep('range-a', event_id),
                expected_revision=state.snapshot().revision,
            )
            commit_step(
                state, CompleteLateStep('range-a', event_id), expected_revision=snapshot.revision
            )
        assert state.missing_late_runs('range-a') == ((105, 107), (109, 110))
        with pytest.raises(InvalidTransitionError, match='excused ids differ'):
            state.complete_obligation_scan('range-a')
        for event_id in (105, 106, 107, 109, 110):
            snapshot = commit_step(
                state,
                ReserveLateStep('range-a', event_id),
                expected_revision=state.snapshot().revision,
            )
            commit_step(
                state, CompleteLateStep('range-a', event_id), expected_revision=snapshot.revision
            )
        assert state.missing_late_runs('range-a') == ()
        state.complete_obligation_scan('range-a')
        assert state.pending_ranges() == ()


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
            commit_step(
                state, ReserveLateStep('range-a', 42), expected_revision=state.snapshot().revision
            )


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
            snapshot = commit_step(
                state, ReserveLateStep('range-a', 42), expected_revision=state.snapshot().revision
            )
            commit_step(
                state, CompleteLateStep('range-a', 42), expected_revision=snapshot.revision
            )
            state.complete_obligation_scan('range-a')
            assert state.pending_ranges() == ()
            assert connection.execute('SELECT COUNT(*) FROM late_ids').fetchone()[0] == 0
            assert (
                connection.execute('SELECT obligation_state FROM range_groups').fetchone()[0]
                == 'scan-complete'
            )


@pytest.mark.parametrize('excused', [((42, 42),), ((42, 44),)])
def test_completion_with_different_excused_set_raises(
    tmp_path: Path, excused: tuple[tuple[int, int], ...]
) -> None:
    with _open_group(tmp_path / 'source.db') as state:
        connection = _database_connection(state)
        insert_range(connection, 'range-a', 42, 50, state='closed')
        bind_group(connection, 'range-a', 'editorial', 7, floor=41, eligibility_point=0)
        insert_source_late_ids(connection, 'range-a', (42, 43))
        before = state.snapshot()
        with pytest.raises(
            InvalidTransitionError, match='excused ids differ from the missing ids'
        ):
            state.complete_obligation_scan('range-a', excused=excused)
        assert state.snapshot() == before
        assert state.pending_ranges()
