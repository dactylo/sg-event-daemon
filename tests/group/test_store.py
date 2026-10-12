from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING, Literal, cast

import pytest

from sg_event_daemon._group_errors import InvalidTransitionError, RevisionConflictError
from sg_event_daemon._group_obligations import _promote_capacity_pending
from sg_event_daemon._group_store import snapshot as store_snapshot
from sg_event_daemon._group_types import (
    AuditIdentity,
    LiveJoinIdentity,
)
from sg_event_daemon._plugins import CallbackIdentity, CallbackManifest
from tests._storage_support import open_test_group
from tests.group._support import assert_operational_count as _assert_operational_count
from tests.group._support import dispose_at_frontier, seed_pending_range

if TYPE_CHECKING:
    from pathlib import Path

    from sg_event_daemon._group_state import GroupState


_JOIN_ID = 'join'


def _handoff_connection(state: GroupState) -> sqlite3.Connection:
    handle = object.__getattribute__(state, '_handoff')
    return cast('sqlite3.Connection', object.__getattribute__(handle, 'connection'))


def _manifest() -> CallbackManifest:
    return CallbackManifest(
        callbacks=(
            CallbackIdentity(
                ordinal=0,
                plugin_id='publish',
                callback_id='publish',
                execution='sync',
                filter_digest='filter-0',
                config_digest='config-0',
                secret_refs=(('token', 'PUBLISH_TOKEN'),),
                credential_name='publisher',
                credential_fingerprint='publisher:publish-script',
            ),
        )
    )


def _open_group(
    path: Path,
    *,
    start_mode: Literal['initialize', 'resume'] | None = None,
    group: str = 'editorial',
) -> GroupState:
    if start_mode is None:
        return open_test_group(path, group=group)
    return open_test_group(path, group=group, start_mode=start_mode)


def _seed_other_group(connection: sqlite3.Connection) -> dict[str, str]:
    seed_pending_range(connection, group='beta')
    connection.executescript(
        """
        INSERT INTO groups VALUES ('beta', 7, 'eager',
        'https://studio.example.invalid', 'incarnation-1', 0, 1, 'active');
        INSERT INTO group_state (group_name,
        revision, cursor, delivery_floor, parked, last_delivered_id) VALUES ('beta',
        0, 41, 41, 1, 41); INSERT INTO callback_marks (group_name, event_id, ordinal,
        plugin_id, callback_id, outcome, delivery_attempt) VALUES ('beta', 42, 0,
        'publish', 'publish', 'pending', 1); INSERT INTO late_ids (group_name,
        obligation_id, event_id, terminal_state) VALUES ( 'beta', 'range-1', 42,
        'pending'); INSERT INTO epochs (group_name, low_id, high_id, captured_at,
        eligibility_point, reader_clock_checkpoint, source_generation,
        group_generation, state) VALUES ('beta', 1, 42, 1, 3, 1, 'source-spawn-1', 7,
        'pending'); INSERT INTO join_commits (group_name, attempt_token,
        identity_json, result_revision, snapshot_json) VALUES ('beta', 'join', '{}',
        0, '{}'); INSERT INTO
        audit (group_name, site, command, wall_clock_time, prior_revision,
        new_revision, reason, local_uid, username, pid) VALUES ('beta', 'studio',
        'park', 1, 0, 1, 'beta reason', 1, 'operator', 1); INSERT INTO
        conditional_park_results (group_name, evidence_id, group_generation,
        evidence_fingerprint, outcome, resulting_revision, decided_at_wall_clock)
        VALUES ( 'beta', 'evidence', 7, 'fingerprint', 'parked', 1, 1);
        """
    )
    return {
        'groups': "SELECT * FROM groups WHERE group_name = 'beta'",
        'group_state': "SELECT * FROM group_state WHERE group_name = 'beta'",
        'callback_marks': "SELECT * FROM callback_marks WHERE group_name = 'beta'",
        'range_groups': "SELECT * FROM range_groups WHERE group_name = 'beta'",
        'late_ids': "SELECT * FROM late_ids WHERE group_name = 'beta'",
        'epochs': "SELECT * FROM epochs WHERE group_name = 'beta'",
        'join_commits': "SELECT * FROM join_commits WHERE group_name = 'beta'",
        'audit': "SELECT * FROM audit WHERE group_name = 'beta'",
        'conditional_park_results': (
            "SELECT * FROM conditional_park_results WHERE group_name = 'beta'"
        ),
    }


def test_first_start_full_commits_identity_and_position(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        snapshot = state.snapshot()
        assert snapshot.site == 'studio'
        assert snapshot.source == 'events'
        assert snapshot.group == 'editorial'
        assert snapshot.group_generation == 7
        assert snapshot.revision == 0
        assert snapshot.status == 'running'
        assert snapshot.conditions == ()
        assert snapshot.cursor == 41
        assert snapshot.delivery_floor == 41
        assert snapshot.verified_incarnation == 'incarnation-1'
        assert _handoff_connection(state).execute('PRAGMA synchronous').fetchone() == (2,)

    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute(
            """
            SELECT site, site_server_url, source_name, group_name, group_generation,
                   ordering_mode, stream_fingerprint
            FROM groups CROSS JOIN source_meta
            WHERE group_name = 'editorial'
            """
        ).fetchone() == (
            'studio',
            'https://studio.example.invalid',
            'events',
            'editorial',
            7,
            'eager',
            'stream-v1',
        )


def test_group_schema_initializes_checked_operational_late_id_count(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        connection = _handoff_connection(state)
        assert connection.execute(
            "SELECT operational_late_id_count FROM group_state WHERE group_name = 'editorial'"
        ).fetchone() == (0,)
        _assert_operational_count(connection)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                'UPDATE group_state SET operational_late_id_count = -1 '
                "WHERE group_name = 'editorial'"
            )


def test_group_schema_keys_operational_tables_by_group_name(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'group.db') as state:
        connection = _handoff_connection(state)
        for table in (
            'groups',
            'group_state',
            'callback_marks',
            'late_ids',
            'epochs',
            'join_commits',
            'audit',
            'conditional_park_results',
        ):
            columns = connection.execute(f'PRAGMA table_info({table})').fetchall()
            indexes = connection.execute(f'PRAGMA index_list({table})').fetchall()
            for index in indexes:
                if index[4]:
                    assert (
                        connection.execute(f'PRAGMA index_info({index[1]})').fetchone()[2]
                        == 'group_name'
                    )
            assert 'group_name' in {column[1] for column in columns}, table
            if table != 'groups':
                assert next(column[3] for column in columns if column[1] == 'group_name') == 1
            primary_key = [
                column[1] for column in sorted(columns, key=lambda row: row[5]) if column[5]
            ]
            if table in {'epochs', 'audit'}:
                id_column = 'epoch_id' if table == 'epochs' else 'audit_id'
                assert primary_key == [id_column]
                assert ['group_name', id_column] in [
                    [row[2] for row in connection.execute(f'PRAGMA index_info({index[1]})')]
                    for index in indexes
                ]
            else:
                assert primary_key[0] == 'group_name', table


def test_group_schema_separates_durable_responsibilities(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        connection = _handoff_connection(state)
        names = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert {
            'audit',
            'callback_marks',
            'conditional_park_results',
            'discovery_ranges',
            'epochs',
            'groups',
            'group_state',
            'join_commits',
            'late_ids',
            'range_groups',
            'replay_markers',
            'source_late_ids',
        } <= names
        epoch_columns = {str(row[1]) for row in connection.execute('PRAGMA table_info(epochs)')}
        assert all('body' not in column for column in epoch_columns)
        assert {str(row[1]) for row in connection.execute('PRAGMA table_info(range_groups)')} >= {
            'obligation_id',
            'group_name',
            'group_generation',
            'obligation_state',
        }


def test_group_statements_ignore_other_group_rows(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'alpha.db', group='alpha') as state:
        connection = _handoff_connection(state)
        tables = _seed_other_group(connection)
        connection.execute(
            "INSERT INTO late_ids VALUES ('beta', 'range-1', 43, 'capacity-pending', 1)"
        )
        before = {table: connection.execute(sql).fetchall() for table, sql in tables.items()}
        assert state.snapshot().conditions == ()
        assert state.snapshot().obligation_count == 0
        assert state.snapshot().late_id_count == 0
        assert state.snapshot().epochs == ()
        assert state.audit_count() == 1
        assert state.park_reason() is None
        assert tuple(row.command for row in state._audit_records()) == ('bootstrap',)
        assert state.pending_ranges() == ()
        assert state.unresolved_late_id_projection(limit=10) == (0, ())
        assert state.unresolved_obligation_bounds() == ()
        state.park(
            expected_revision=state.snapshot().revision,
            reason='alpha reason',
            identity=AuditIdentity.current(),
            wall_clock_time=2.0,
        )
        state.unpark(
            expected_revision=state.snapshot().revision,
            reason='alpha reason',
            identity=AuditIdentity.current(),
            wall_clock_time=2.0,
        )
        state.start_event(42, _manifest(), expected_revision=state.snapshot().revision)
        state.begin_callback(0, expected_revision=state.snapshot().revision)
        state.callback_done(0, expected_revision=state.snapshot().revision)
        state.commit_live_join(
            LiveJoinIdentity(
                site='studio',
                source='events',
                group='alpha',
                join_kind='eager',
                source_generation='source-spawn-1',
                group_generation=7,
                boundary=42,
                attempt_token=_JOIN_ID,
            ),
            expected_revision=state.snapshot().revision,
        )
        seed_pending_range(connection, 'alpha-range', 42, 43, group='alpha')
        state.reserve_late_event('alpha-range', 42, expected_revision=state.snapshot().revision)
        assert state.snapshot().late_id_count == 1
        state.start_event(
            42,
            _manifest(),
            obligation_id='alpha-range',
            expected_revision=state.snapshot().revision,
        )
        state.begin_callback(0, expected_revision=state.snapshot().revision)
        state.callback_done(0, expected_revision=state.snapshot().revision)
        assert state.snapshot().late_id_count == 0
        state.reserve_late_event('alpha-range', 43, expected_revision=state.snapshot().revision)
        state.complete_late_id('alpha-range', 43, expected_revision=state.snapshot().revision)
        assert state.snapshot().late_id_count == 0
        state.park(
            expected_revision=state.snapshot().revision,
            reason='alpha reason',
            identity=AuditIdentity.current(),
            wall_clock_time=3.0,
        )
        dispose_at_frontier(
            state,
            'replay',
            disposition_id='replay',
            new_generation=8,
            from_event_id=42,
            source_frontier=43,
            expected_revision=state.snapshot().revision,
            reason='alpha reason',
            identity=AuditIdentity.current(),
            wall_clock_time=3.0,
        )
        assert state.snapshot().group == 'alpha'
        for table, sql in tables.items():
            assert connection.execute(sql).fetchall() == before[table], table


@pytest.mark.parametrize('history_count', [1_000, 100_000])
def test_operational_indexes_bound_empty_queries_over_retained_history(
    tmp_path: Path,
    history_count: int,
) -> None:
    with _open_group(tmp_path / 'group.db') as state:
        connection = _handoff_connection(state)
        seed_pending_range(connection)
        connection.execute('BEGIN')
        connection.executemany(
            "INSERT INTO late_ids VALUES ('editorial', 'range-1', ?, 'done', NULL)",
            ((index,) for index in range(42, history_count + 42)),
        )
        connection.commit()
        # Obtain SQL from the live operational entrypoints, outside measured VM work.
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        try:
            store_snapshot(connection, 'editorial')
            _promote_capacity_pending(connection, 'editorial', 1)
        finally:
            connection.set_trace_callback(None)
        predicates = (
            (
                (
                    "FROM late_ids WHERE group_name = 'editorial' AND "
                    "terminal_state = 'capacity-pending' ORDER BY"
                ),
                'late_ids_capacity_pending',
            ),
            (
                (
                    "FROM late_ids WHERE group_name = 'editorial' AND "
                    "terminal_state = 'capacity-pending' LIMIT"
                ),
                'late_ids_capacity_pending',
            ),
        )
        measurements: list[tuple[str, int, str]] = []
        for fragment, index_name in predicates:
            matching = [sql for sql in statements if fragment in ' '.join(sql.split())]
            assert len(matching) == 1, (fragment, statements)
            sql = matching[0]
            plan = ' '.join(str(row[3]) for row in connection.execute('EXPLAIN QUERY PLAN ' + sql))
            steps = 0

            def count_step() -> int:
                nonlocal steps
                steps += 1
                return 0

            connection.set_progress_handler(count_step, 1)
            try:
                assert connection.execute(sql).fetchall() == []
            finally:
                connection.set_progress_handler(None, 0)
            measurements.append((index_name, steps, plan))
        # Keep all measurements visible even when a baseline query fails its bound.
        assert all(
            index in plan and 'TEMP B-TREE' not in plan and steps <= 100
            for index, steps, plan in measurements
        ), (
            history_count,
            measurements,
        )


def test_revision_conflict_and_invalid_transition_preserve_state(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        before = state.start_event(42, _manifest(), expected_revision=0)
        with pytest.raises(RevisionConflictError):
            state.advance_irrelevant(43, expected_revision=0)
        assert state.snapshot() == before
        with pytest.raises(InvalidTransitionError):
            state.begin_callback(1, expected_revision=1)
        assert state.snapshot() == before
