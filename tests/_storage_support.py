from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack

from sg_event_daemon._group_recovery import LifecycleRequest
from sg_event_daemon._group_state import GroupOpenOptions, GroupState
from sg_event_daemon._group_types import AuditIdentity, GroupSnapshot
from sg_event_daemon._platform import ensure_secure_state_dir
from sg_event_daemon._source_schema import (
    BUMP_INDEX_REVISION,
    BUMP_RANGE_REVISION,
    SOURCE_SCHEMA,
    source_identity,
)
from sg_event_daemon._source_store import stage_close_range
from sg_event_daemon._sqlite import open_database
from sg_event_daemon._write_lock import SourceWriteLock, WriteContext

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

SITE = 'test'
SOURCE = 'admin'


class _TestGroupOptions(TypedDict, total=False):
    group: str
    group_generation: int
    initial_position: int
    max_failure_text_bytes: int
    max_group_obligations: int
    max_late_ids_per_group: int
    max_manifest_bytes: int
    max_pending_epochs: int
    ordering: Literal['eager', 'strict']
    resume_direct: bool
    site: str
    site_server_url: str
    source: str
    source_generation: str
    start_mode: Literal['initialize', 'resume']
    stream_fingerprint: str
    strict_holdback_secs: float
    verified_incarnation: str
    writes: WriteContext


def close_test_range(connection: sqlite3.Connection, obligation_id: str) -> None:
    row = connection.execute(
        'SELECT successful_clock FROM source_meta WHERE singleton = 1'
    ).fetchone()
    assert row is not None
    assert stage_close_range(connection, obligation_id, successful_clock=float(row[0]))


def group_exists(database_path: Path, group: str) -> bool:
    if not database_path.exists():
        return False
    with closing(sqlite3.connect(f'{database_path.as_uri()}?mode=ro', uri=True)) as connection:
        return (
            connection.execute('SELECT 1 FROM groups WHERE group_name = ?', (group,)).fetchone()
            is not None
        )


def group_lock(database_path: Path, group: str) -> Path:
    parent = database_path.parent
    site_dir = parent.parent if parent.name == 'sources' else parent
    return ensure_secure_state_dir(site_dir / 'groups') / f'{group}.lock'


def insert_replay_marker(
    connection: sqlite3.Connection,
    group: str,
    generation: int,
    from_event_id: int,
) -> None:
    connection.execute(
        'INSERT INTO replay_markers VALUES (?, ?, ?, ?, ?, ?)',
        (group, generation, 'source-spawn-1', f'marker-{group}', from_event_id, 'digest'),
    )
    connection.execute(BUMP_INDEX_REVISION)


def insert_source_late_ids(
    connection: sqlite3.Connection,
    obligation_id: str,
    event_ids: Iterable[int],
) -> None:
    changed = sum(
        connection.execute(
            'INSERT OR IGNORE INTO source_late_ids VALUES (?, ?)',
            (obligation_id, event_id),
        ).rowcount
        for event_id in event_ids
    )
    if changed:
        connection.execute(
            'UPDATE discovery_ranges SET source_revision = source_revision + 1 '
            'WHERE obligation_id = ?',
            (obligation_id,),
        )
        connection.execute(BUMP_INDEX_REVISION)
        connection.execute(BUMP_RANGE_REVISION)


def orphan_group(state: GroupState) -> GroupSnapshot:
    snapshot = state.snapshot()
    return state.change_lifecycle(
        LifecycleRequest(
            command='orphan',
            expected_lifecycle='active',
            expected_generation=snapshot.group_generation,
            transition_generation=snapshot.group_generation,
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
        )
    )


def write_context_for(database_path: Path) -> WriteContext:
    return WriteContext(
        SourceWriteLock(
            database_path.with_suffix('.db.write-lock'),
            timeout_secs=10.0,
            wait_phase_grace_secs=1.0,
        )
    )


def create_source_database(path: Path, *, site: str = SITE, source: str = SOURCE) -> None:
    writes = write_context_for(path)
    try:
        with open_database(
            path,
            source_identity(site, source),
            SOURCE_SCHEMA,
            synchronous='NORMAL',
            write_transaction=writes.transaction,
        ):
            pass
    finally:
        writes.lock.close()


def open_group_on_source(database_path: Path, **options: Unpack[GroupOpenOptions]) -> GroupState:
    if 'writes' not in options:
        options['writes'] = write_context_for(database_path)
    if not database_path.exists():
        create_source_database(database_path, site=options['site'], source=options['source'])
    with closing(sqlite3.connect(database_path)) as connection, connection:
        if connection.execute('SELECT 1 FROM source_meta').fetchone() is None:
            connection.execute(
                """INSERT INTO source_meta
                (singleton, site, source_name, stream_fingerprint, stream_incarnation,
                source_generation, initialized, frontier, successful_clock, condition)
                VALUES (1, ?, ?, ?, ?, ?, 1, ?, 0, NULL)""",
                (
                    options['site'],
                    options['source'],
                    options['stream_fingerprint'],
                    options['verified_incarnation'],
                    options['source_generation'],
                    options.get('initial_last_delivered_id', 0),
                ),
            )
        else:
            connection.execute(
                'UPDATE source_meta SET stream_incarnation = ? '
                'WHERE initialized = 0 AND stream_incarnation IS NULL',
                (options['verified_incarnation'],),
            )
    if 'start_mode' not in options:
        options['start_mode'] = (
            'resume' if group_exists(database_path, options['group']) else 'initialize'
        )
    return GroupState.open(database_path, group_lock(database_path, options['group']), **options)


def open_test_group(path: Path, **selection: Unpack[_TestGroupOptions]) -> GroupState:
    group = selection.get('group', 'editorial')
    selected_mode = selection.get('start_mode') or (
        'resume' if group_exists(path, group) else 'initialize'
    )
    writes = selection.get('writes')
    options: GroupOpenOptions = {
        'writes': write_context_for(path) if writes is None else writes,
        'site': selection.get('site', 'studio'),
        'site_server_url': selection.get('site_server_url', 'https://studio.example.invalid'),
        'source': selection.get('source', 'events'),
        'group': group,
        'group_generation': selection.get('group_generation', 7),
        'ordering': selection.get('ordering', 'eager'),
        'stream_fingerprint': selection.get('stream_fingerprint', 'stream-v1'),
        'source_generation': selection.get('source_generation', 'source-spawn-1'),
        'verified_incarnation': selection.get('verified_incarnation', 'incarnation-1'),
        'start_mode': selected_mode,
    }
    if selected_mode == 'initialize':
        initial_position = selection.get('initial_position', 41)
        options['initial_cursor'] = initial_position
        options['initial_delivery_floor'] = initial_position
        options['initial_last_delivered_id'] = initial_position
    if 'max_failure_text_bytes' in selection:
        options['max_failure_text_bytes'] = selection['max_failure_text_bytes']
    if 'max_group_obligations' in selection:
        options['max_group_obligations'] = selection['max_group_obligations']
    if 'max_late_ids_per_group' in selection:
        options['max_late_ids_per_group'] = selection['max_late_ids_per_group']
    if 'max_manifest_bytes' in selection:
        options['max_manifest_bytes'] = selection['max_manifest_bytes']
    if 'max_pending_epochs' in selection:
        options['max_pending_epochs'] = selection['max_pending_epochs']
    if 'strict_holdback_secs' in selection:
        options['strict_holdback_secs'] = selection['strict_holdback_secs']
    if selection.get('resume_direct', False) and selected_mode == 'resume':
        return GroupState.open(path, group_lock(path, group), **options)
    return open_group_on_source(path, **options)


def source_database(state_dir: Path, *, site: str = SITE, source: str = SOURCE) -> Path:
    sites = ensure_secure_state_dir(ensure_secure_state_dir(state_dir) / 'sites')
    site_dir = ensure_secure_state_dir(sites / site)
    return ensure_secure_state_dir(site_dir / 'sources') / f'{source}.db'


class _RangeOptions(TypedDict, total=False):
    state: str
    opened_successful_at: float
    visibility_deadline: float


class _BindingOptions(TypedDict):
    floor: int
    eligibility_point: float
    state: NotRequired[str]


def bind_group(
    connection: sqlite3.Connection,
    obligation_id: str,
    group: str,
    generation: int,
    **options: Unpack[_BindingOptions],
) -> None:
    floor = options['floor']
    eligibility_point = options['eligibility_point']
    state = options.get('state', 'pending')
    connection.execute(
        'INSERT INTO range_groups VALUES (?, ?, ?, ?, ?, ?)',
        (obligation_id, group, generation, floor, eligibility_point, state),
    )
    connection.execute(BUMP_INDEX_REVISION)
    connection.execute(BUMP_RANGE_REVISION)


def insert_range(
    connection: sqlite3.Connection,
    obligation_id: str,
    low_id: int,
    high_id: int,
    **options: Unpack[_RangeOptions],
) -> None:
    state = options.get('state', 'open')
    opened_successful_at = options.get('opened_successful_at', 0.0)
    visibility_deadline = options.get('visibility_deadline', 300.0)
    connection.execute(
        """
        INSERT INTO discovery_ranges (
            obligation_id, low_id, high_id, opened_successful_at,
            visibility_deadline, source_revision, close_generation,
            range_state, scan_next_id, scan_pages, degraded
        ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, 0, 0)
        """,
        (
            obligation_id,
            low_id,
            high_id,
            opened_successful_at,
            visibility_deadline,
            int(state == 'closed'),
            state,
            low_id,
        ),
    )
    connection.execute(BUMP_INDEX_REVISION)
    connection.execute(BUMP_RANGE_REVISION)
