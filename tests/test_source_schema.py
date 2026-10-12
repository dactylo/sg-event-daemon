from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon._source_schema import (
    BUMP_INDEX_REVISION,
    BUMP_RANGE_REVISION,
    SOURCE_SCHEMA,
    source_identity,
)
from sg_event_daemon._sqlite import DatabaseHandle, open_database

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

_EXPECTED_COLUMNS = {
    'source_meta': [
        'singleton',
        'site',
        'source_name',
        'stream_fingerprint',
        'stream_incarnation',
        'source_generation',
        'initialized',
        'frontier',
        'successful_clock',
        'condition',
        'index_revision',
        'range_revision',
    ],
    'groups': [
        'group_name',
        'group_generation',
        'ordering_mode',
        'site_server_url',
        'verified_incarnation',
        'holdback_secs',
        'active',
        'lifecycle',
    ],
    'group_state': [
        'group_name',
        'revision',
        'cursor',
        'delivery_floor',
        'in_flight_event_id',
        'in_flight_obligation_id',
        'manifest_json',
        'required_event_id',
        'retry_event_id',
        'failure_code',
        'failure_text',
        'failure_byte_count',
        'failure_digest',
        'failure_ordinal',
        'parked',
        'is_operator_replay',
        'replay_required_from',
        'history_truncated_floor',
        'ordering_anomaly_id',
        'strict_capacity_abort_streak',
        'strict_capacity_attempt_token',
        'strict_capacity_last_caused',
        'strict_capacity_exhausted',
        'shape_mismatch',
        'orphaned',
        'stream_identity_changed',
        'ordering_changed',
        'reader_clock_checkpoint',
        'last_epoch_capture',
        'latest_join_boundary',
        'operational_late_id_count',
        'last_delivered_id',
    ],
    'callback_marks': [
        'group_name',
        'event_id',
        'ordinal',
        'plugin_id',
        'callback_id',
        'outcome',
        'delivery_attempt',
    ],
    'discovery_ranges': [
        'obligation_id',
        'low_id',
        'high_id',
        'opened_successful_at',
        'visibility_deadline',
        'source_revision',
        'close_generation',
        'range_state',
        'scan_started_at',
        'scan_next_id',
        'scan_pages',
        'degraded',
        'scan_cycle_pages',
    ],
    'range_groups': [
        'obligation_id',
        'group_name',
        'group_generation',
        'delivery_floor',
        'eligibility_point',
        'obligation_state',
    ],
    'source_late_ids': ['obligation_id', 'event_id'],
    'late_ids': ['group_name', 'obligation_id', 'event_id', 'terminal_state', 'capacity_revision'],
    'replay_markers': [
        'group_name',
        'group_generation',
        'source_generation',
        'marker_id',
        'from_event_id',
        'command_digest',
    ],
    'epochs': [
        'epoch_id',
        'group_name',
        'low_id',
        'high_id',
        'captured_at',
        'eligibility_point',
        'reader_clock_checkpoint',
        'source_generation',
        'group_generation',
        'state',
    ],
    'join_commits': [
        'group_name',
        'attempt_token',
        'identity_json',
        'result_revision',
        'snapshot_json',
    ],
    'audit': [
        'audit_id',
        'site',
        'group_name',
        'event_id',
        'plugin_id',
        'callback_id',
        'command',
        'wall_clock_time',
        'prior_revision',
        'new_revision',
        'reason',
        'operator_label',
        'local_uid',
        'username',
        'pid',
        'disposition_id',
        'command_fingerprint',
        'terminal_result',
        'result_snapshot_json',
    ],
    'conditional_park_results': [
        'group_name',
        'evidence_id',
        'group_generation',
        'evidence_fingerprint',
        'outcome',
        'resulting_revision',
        'decided_at_wall_clock',
    ],
    'disposition_audit': [
        'disposition_id',
        'group_name',
        'command',
        'command_fingerprint',
        'old_generation',
        'new_generation',
        'cursor',
        'frontier',
        'stream_incarnation',
        'terminal_result',
    ],
    'stream_adoptions': [
        'adoption_id',
        'command_fingerprint',
        'stream_fingerprint',
        'reason',
        'retired_range_count',
        'retired_late_id_count',
        'retired_marker_count',
    ],
    'pending_adoptions': [
        'adoption_id',
        'command_fingerprint',
        'prior_incarnation',
        'prior_stream_fingerprint',
        'target_stream_fingerprint',
        'reason',
    ],
}
_COUNT_STATEMENTS = {
    'late_ids': 'SELECT COUNT(*) FROM late_ids',
    'range_groups': 'SELECT COUNT(*) FROM range_groups',
    'source_late_ids': 'SELECT COUNT(*) FROM source_late_ids',
}
_EXPECTED_INDEXES = {
    'audit_by_disposition',
    'audit_by_group',
    'epochs_by_group',
    'late_ids_by_obligation',
    'late_ids_capacity_pending',
    'range_groups_by_group',
}


def _seed(connection: sqlite3.Connection) -> None:
    connection.execute(
        'INSERT INTO source_meta (singleton, site, source_name, stream_fingerprint, '
        'stream_incarnation, source_generation, initialized, frontier, successful_clock) '
        "VALUES (1, 'site', 'source', 'fingerprint', 'incarnation', 'generation', 1, 0, 0.0)"
    )
    connection.execute(
        "INSERT INTO groups VALUES ('g', 1, 'eager', 'https://site.invalid', 'incarnation', "
        "1.0, 1, 'active')"
    )
    connection.execute(
        'INSERT INTO group_state (group_name, revision, cursor, delivery_floor, '
        "last_delivered_id) VALUES ('g', 0, 0, 0, 0)"
    )
    connection.execute(
        'INSERT INTO discovery_ranges (obligation_id, low_id, high_id, opened_successful_at, '
        'visibility_deadline, source_revision, close_generation, range_state, scan_next_id, '
        "scan_pages, degraded) VALUES ('r', 10, 12, 1.0, 2.0, 1, 0, 'open', 10, 0, 0)"
    )


@pytest.fixture
def database(tmp_path: Path) -> Iterator[DatabaseHandle]:
    handle = open_database(
        tmp_path / 'source.db',
        source_identity('site', 'source'),
        SOURCE_SCHEMA,
        synchronous='NORMAL',
    )
    try:
        yield handle
    finally:
        handle.close()


def test_group_rows_require_a_known_lifecycle_and_group(database: DatabaseHandle) -> None:
    connection = database.connection
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "INSERT INTO groups VALUES ('g', 1, 'eager', 'u', 'i', 1.0, 1, 'retired')"
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            'INSERT INTO group_state (group_name, revision, cursor, delivery_floor, '
            "last_delivered_id) VALUES ('missing', 0, 0, 0, 0)"
        )


def test_group_state_rejects_null_group_name(database: DatabaseHandle) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        database.connection.execute(
            'INSERT INTO group_state (group_name, revision, cursor, delivery_floor, '
            'last_delivered_id) VALUES (NULL, 0, 0, 0, 0)'
        )


def test_identity_and_version_one(database: DatabaseHandle) -> None:
    connection = database.connection
    assert SOURCE_SCHEMA.version == 1
    assert connection.execute('PRAGMA user_version').fetchone() == (1,)
    assert connection.execute(
        "SELECT value FROM _daemon_metadata WHERE key = 'identity'"
    ).fetchone() == ('source:site:source',)


def test_indexes_support_group_scoped_reads(database: DatabaseHandle) -> None:
    indexes = {
        name
        for (name,) in database.connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL"
        )
    }
    assert indexes == _EXPECTED_INDEXES


def test_range_groups_reject_unknown_states_and_cascade_with_their_range(
    database: DatabaseHandle,
) -> None:
    connection = database.connection
    _seed(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("INSERT INTO range_groups VALUES ('r', 'g', 1, 0, 2.0, 'transferred')")
    connection.execute("INSERT INTO range_groups VALUES ('r', 'g', 1, 0, 2.0, 'pending')")
    connection.execute("INSERT INTO source_late_ids VALUES ('r', 11)")
    connection.execute("INSERT INTO late_ids VALUES ('g', 'r', 11, 'done', NULL)")
    connection.execute("DELETE FROM discovery_ranges WHERE obligation_id = 'r'")
    for table, statement in _COUNT_STATEMENTS.items():
        assert connection.execute(statement).fetchone() == (0,), table


def test_revision_statements_increment_independently(database: DatabaseHandle) -> None:
    connection = database.connection
    _seed(connection)
    connection.execute(BUMP_INDEX_REVISION)
    connection.execute(BUMP_INDEX_REVISION)
    connection.execute(BUMP_RANGE_REVISION)
    assert connection.execute(
        'SELECT index_revision, range_revision FROM source_meta'
    ).fetchone() == (2, 1)


def test_tables_and_columns_match_the_per_source_layout(database: DatabaseHandle) -> None:
    connection = database.connection
    tables = {
        name
        for (name,) in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT IN ('_daemon_metadata', 'sqlite_sequence')"
        )
    }
    assert tables == set(_EXPECTED_COLUMNS)
    for table, columns in _EXPECTED_COLUMNS.items():
        actual = [
            name
            for (name,) in connection.execute(
                'SELECT name FROM pragma_table_info(?) ORDER BY cid', (table,)
            )
        ]
        assert actual == columns, table
