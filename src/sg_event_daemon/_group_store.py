"""Revision CAS and the single group condition projection."""

from __future__ import annotations

import sqlite3
from typing import Literal, TypeAlias, cast, get_args

from ._group_errors import InvalidTransitionError, RevisionConflictError
from ._group_types import (
    CatchupEpoch,
    GroupSnapshot,
    GroupStatus,
    manifest_from_json,
)

GroupHealthCondition: TypeAlias = Literal[
    'orphaned',
    'stream-identity-changed',
    'ordering-changed',
    'parked',
    'history-truncated',
    'replay-required',
    'shape-mismatch',
    'callback-failed',
    'event-unavailable',
    'required-event',
    'ordering-anomaly',
    'strict-capacity-exhausted',
    'late-capacity-exhausted',
]
GROUP_HEALTH_CONDITIONS: tuple[GroupHealthCondition, ...] = get_args(GroupHealthCondition)

_OPERATIONAL_LATE_ID_KEYS_STATEMENT = """
SELECT obligation_id, event_id
FROM late_ids
WHERE group_name = ? AND terminal_state = 'pending'

"""

_OPERATIONAL_LATE_ID_COUNT_REFRESH_STATEMENT = f"""
UPDATE group_state
SET operational_late_id_count = (
    SELECT COUNT(*) FROM ({_OPERATIONAL_LATE_ID_KEYS_STATEMENT})
)
WHERE group_name = ?
"""  # noqa: S608


def _project_status(conditions: tuple[str, ...]) -> GroupStatus:
    """Project display status without deleting independent evidence."""
    for condition in GROUP_HEALTH_CONDITIONS:
        if condition != 'required-event' and condition in conditions:
            return condition
    return 'running'


def adjust_operational_late_id_count(
    connection: sqlite3.Connection, group_name: str, delta: int
) -> None:
    """Apply an exact identity delta inside the caller's transaction.

    Args:
        connection: Live facade-owned transaction connection.
        group_name: Exact group whose durable rows are accessed.
        delta: Change in the number of pending `late_ids` identities, computed
            with the corresponding row transitions in this transaction.
    """
    if delta:
        connection.execute(
            """
            UPDATE group_state
            SET operational_late_id_count = operational_late_id_count + ?
            WHERE group_name = ?
            """,
            (delta, group_name),
        )


def bump_revision(connection: sqlite3.Connection, group_name: str, expected_revision: int) -> None:
    """Complete one compare-and-swap revision update.

    Args:
        connection: Live facade-owned transaction connection.
        group_name: Exact group whose durable rows are accessed.
        expected_revision: Required current revision.

    Raises:
        RevisionConflictError: If no row has the expected revision.
    """
    changed = connection.execute(
        """
        UPDATE group_state
        SET revision = revision + 1
        WHERE group_name = ? AND revision = ?
        """,
        (group_name, expected_revision),
    ).rowcount
    if changed != 1:
        raise RevisionConflictError(f'expected group revision {expected_revision}')


def check_revision(
    connection: sqlite3.Connection, group_name: str, expected_revision: int
) -> None:
    """Reject a stale expected revision before transition validation.

    Args:
        connection: Live facade-owned transaction connection.
        group_name: Exact group whose durable rows are accessed.
        expected_revision: Required current revision.

    Raises:
        RevisionConflictError: If the expected revision differs.
    """
    row = connection.execute(
        'SELECT revision FROM group_state WHERE group_name = ?', (group_name,)
    ).fetchone()
    if row is None or int(row[0]) != expected_revision:
        raise RevisionConflictError(f'expected group revision {expected_revision}')


def clear_failure(connection: sqlite3.Connection, group_name: str) -> None:
    """Clear callback failure evidence inside the caller's live transaction."""
    connection.execute(
        """
        UPDATE group_state
        SET failure_code = NULL,
            failure_text = NULL,
            failure_byte_count = NULL,
            failure_digest = NULL,
            failure_ordinal = NULL
        WHERE group_name = ?
        """,
        (group_name,),
    )


def refresh_operational_late_id_count(connection: sqlite3.Connection, group_name: str) -> None:
    """Refresh the exact operational late-id scalar inside the live transaction.

    Args:
        connection: Live facade-owned transaction connection.
        group_name: Exact group whose durable rows are accessed.
    """
    connection.execute(_OPERATIONAL_LATE_ID_COUNT_REFRESH_STATEMENT, (group_name, group_name))


def snapshot(connection: sqlite3.Connection, group_name: str) -> GroupSnapshot:
    """Read one immutable snapshot from a facade-owned connection.

    Args:
        connection: Open configured group database connection.
        group_name: Exact group whose durable rows are accessed.

    Returns:
        The latest committed or transaction-local group state.
    """
    cursor = connection.cursor()
    cursor.row_factory = sqlite3.Row
    row = cursor.execute(
        """
        SELECT
            sm.site, sm.source_name AS source, m.group_name, m.group_generation,
            m.ordering_mode, sm.stream_fingerprint, sm.source_generation,
            m.verified_incarnation,
            s.revision, s.cursor, s.delivery_floor,
            s.in_flight_event_id, s.in_flight_obligation_id,
            s.manifest_json, s.required_event_id, s.retry_event_id,
            s.failure_code, s.failure_text, s.failure_byte_count,
            s.failure_digest, s.failure_ordinal, s.parked,
            s.is_operator_replay, s.replay_required_from,
            s.history_truncated_floor, s.ordering_anomaly_id,
            s.strict_capacity_abort_streak,
            s.strict_capacity_attempt_token,
            s.strict_capacity_exhausted,
            s.shape_mismatch, s.orphaned,
            s.stream_identity_changed, s.ordering_changed,
            s.reader_clock_checkpoint, s.latest_join_boundary,
            s.operational_late_id_count
        FROM groups AS m CROSS JOIN source_meta AS sm
        JOIN group_state AS s ON s.group_name = m.group_name
        WHERE m.group_name = ?
        """,
        (group_name,),
    ).fetchone()
    if row is None:
        raise RuntimeError('group state is not initialized')
    conditions: list[GroupHealthCondition] = []
    condition_checks: tuple[tuple[bool, GroupHealthCondition], ...] = (
        (bool(row['orphaned']), 'orphaned'),
        (bool(row['stream_identity_changed']), 'stream-identity-changed'),
        (bool(row['ordering_changed']), 'ordering-changed'),
        (bool(row['parked']), 'parked'),
        (row['history_truncated_floor'] is not None, 'history-truncated'),
        (row['replay_required_from'] is not None, 'replay-required'),
        (bool(row['shape_mismatch']), 'shape-mismatch'),
        (row['failure_code'] == 'callback-failed', 'callback-failed'),
    )
    for active, name in condition_checks:
        if active:
            conditions.append(name)
    if row['required_event_id'] is not None:
        conditions.extend(('event-unavailable', 'required-event'))
    if row['ordering_anomaly_id'] is not None:
        conditions.append('ordering-anomaly')
    if bool(row['strict_capacity_exhausted']):
        conditions.append('strict-capacity-exhausted')
    if (
        connection.execute(
            (
                'SELECT 1 FROM late_ids WHERE group_name = ? AND '
                "terminal_state = 'capacity-pending' LIMIT 1"
            ),
            (group_name,),
        ).fetchone()
        is not None
    ):
        conditions.append('late-capacity-exhausted')
    condition_tuple = tuple(conditions)
    manifest = (
        None if row['manifest_json'] is None else manifest_from_json(str(row['manifest_json']))
    )
    marks = tuple(
        (int(mark[0]), str(mark[1]), str(mark[2]), str(mark[3]), int(mark[4]))
        for mark in connection.execute(
            """
            SELECT ordinal, plugin_id, callback_id, outcome, delivery_attempt
            FROM callback_marks
            WHERE group_name = ?
            ORDER BY ordinal
            """,
            (group_name,),
        ).fetchall()
    )
    epochs = tuple(
        CatchupEpoch(
            epoch_id=int(epoch[0]),
            low_id=int(epoch[1]),
            high_id=int(epoch[2]),
            captured_at=float(epoch[3]),
            eligibility_point=float(epoch[4]),
            reader_clock_checkpoint=float(epoch[5]),
            source_generation=str(epoch[6]),
            group_generation=int(epoch[7]),
            state=cast("Literal['delivering', 'waiting']", epoch[8]),
        )
        for epoch in connection.execute(
            """
            SELECT epoch_id, low_id, high_id, captured_at,
                   eligibility_point, reader_clock_checkpoint,
                   source_generation, group_generation, state
            FROM epochs
            WHERE group_name = ?
            ORDER BY low_id
            """,
            (group_name,),
        ).fetchall()
    )
    return GroupSnapshot(
        site=str(row['site']),
        source=str(row['source']),
        group=str(row['group_name']),
        group_generation=int(row['group_generation']),
        revision=int(row['revision']),
        status=_project_status(condition_tuple),
        conditions=condition_tuple,
        cursor=int(row['cursor']),
        delivery_floor=int(row['delivery_floor']),
        verified_incarnation=str(row['verified_incarnation']),
        ordering=cast("Literal['eager', 'strict']", row['ordering_mode']),
        stream_fingerprint=str(row['stream_fingerprint']),
        source_generation=str(row['source_generation']),
        in_flight_event_id=None
        if row['in_flight_event_id'] is None
        else int(row['in_flight_event_id']),
        in_flight_obligation_id=None
        if row['in_flight_obligation_id'] is None
        else str(row['in_flight_obligation_id']),
        manifest=manifest,
        required_event_id=None
        if row['required_event_id'] is None
        else int(row['required_event_id']),
        retry_event_id=None if row['retry_event_id'] is None else int(row['retry_event_id']),
        callback_marks=marks,
        failure_code=None if row['failure_code'] is None else str(row['failure_code']),
        failure_text=None if row['failure_text'] is None else str(row['failure_text']),
        failure_byte_count=None
        if row['failure_byte_count'] is None
        else int(row['failure_byte_count']),
        failure_digest=None if row['failure_digest'] is None else str(row['failure_digest']),
        failure_ordinal=None if row['failure_ordinal'] is None else int(row['failure_ordinal']),
        parked=bool(row['parked']),
        is_operator_replay=bool(row['is_operator_replay']),
        replay_required_from=None
        if row['replay_required_from'] is None
        else int(row['replay_required_from']),
        history_truncated_floor=None
        if row['history_truncated_floor'] is None
        else int(row['history_truncated_floor']),
        ordering_anomaly_id=None
        if row['ordering_anomaly_id'] is None
        else int(row['ordering_anomaly_id']),
        strict_capacity_abort_streak=int(row['strict_capacity_abort_streak']),
        strict_capacity_attempt_token=None
        if row['strict_capacity_attempt_token'] is None
        else str(row['strict_capacity_attempt_token']),
        reader_clock_checkpoint=float(row['reader_clock_checkpoint']),
        latest_join_boundary=None
        if row['latest_join_boundary'] is None
        else int(row['latest_join_boundary']),
        obligation_count=int(
            connection.execute(
                (
                    'SELECT COUNT(*) FROM range_groups WHERE group_name = ? '
                    "AND obligation_state = 'pending' "
                    'AND group_generation = (SELECT group_generation FROM '
                    'groups WHERE group_name = ?)'
                ),
                (group_name, group_name),
            ).fetchone()[0]
        ),
        late_id_count=int(row['operational_late_id_count']),
        epochs=epochs,
    )


def ensure_delivery_ready(connection: sqlite3.Connection, group_name: str) -> None:
    """Reject ordinary scheduling while lifecycle blockers remain.

    Args:
        connection: Live facade-owned transaction connection.
        group_name: Exact group whose durable rows are accessed.

    Raises:
        InvalidTransitionError: If any lifecycle condition or event is active.
    """
    current = snapshot(connection, group_name)
    if current.conditions:
        raise InvalidTransitionError(f'delivery is blocked by {", ".join(current.conditions)}')
    if current.in_flight_event_id is not None:
        raise InvalidTransitionError('another event is already in flight')
