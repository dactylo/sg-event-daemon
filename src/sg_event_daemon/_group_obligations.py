"""Manage durable obligation scans, late marks, and capacity transitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._group_errors import InvalidTransitionError, TerminalLateEventError
from ._group_store import (
    adjust_operational_late_id_count,
    bump_revision,
    check_revision,
    refresh_operational_late_id_count,
    snapshot,
)
from ._source_schema import BUMP_INDEX_REVISION, BUMP_RANGE_REVISION

if TYPE_CHECKING:
    import sqlite3

    from ._group_types import GroupSnapshot


@dataclass(frozen=True)
class LateEventRequest:
    """Identify one exact late event and its owning obligation."""

    obligation_id: str
    event_id: int


def _promote_capacity_pending(
    connection: sqlite3.Connection,
    group_name: str,
    max_late_ids_per_group: int,
) -> None:
    """Admit the stable reservation prefix that fits current operational capacity."""
    count = int(
        connection.execute(
            'SELECT operational_late_id_count FROM group_state WHERE group_name = ?', (group_name,)
        ).fetchone()[0]
    )
    delta = 0
    while True:
        candidate = connection.execute(
            """
            SELECT obligation_id, event_id
            FROM late_ids
            WHERE group_name = ? AND terminal_state = 'capacity-pending'
            ORDER BY capacity_revision, obligation_id, event_id
            LIMIT 1
            """,
            (group_name,),
        ).fetchone()
        if candidate is None:
            break
        obligation_id, event_id = str(candidate[0]), int(candidate[1])
        cost = 1
        if count + delta + cost > max_late_ids_per_group:
            break
        connection.execute(
            """
            UPDATE late_ids
            SET terminal_state = 'pending', capacity_revision = NULL
            WHERE group_name = ? AND obligation_id = ? AND event_id = ?
            """,
            (group_name, obligation_id, event_id),
        )
        delta += cost
    adjust_operational_late_id_count(connection, group_name, delta)


def complete_late_id(
    connection: sqlite3.Connection,
    group_name: str,
    request: LateEventRequest,
    expected_revision: int,
    max_late_ids_per_group: int,
) -> GroupSnapshot:
    """Mark one source-listed late id terminal inside a live transaction.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: Owning obligation and exact source-listed event ID.
        expected_revision: Required compare-and-swap revision.
        max_late_ids_per_group: Maximum operational late-id capacity.

    Returns:
        Immutable snapshot containing the terminal late-id mark and any
        promoted reservations. The transaction promotes queued capacity-pending
        reservations, so the count can remain unchanged and the capacity
        condition can clear.

    Raises:
        RevisionConflictError: The expected revision differs.
        TerminalLateEventError: The late id is not pending.
    """
    check_revision(connection, group_name, expected_revision)
    changed = connection.execute(
        """
        UPDATE late_ids SET terminal_state = 'done'
        WHERE group_name = ? AND obligation_id = ? AND event_id = ?
          AND terminal_state = 'pending'
        """,
        (group_name, request.obligation_id, request.event_id),
    ).rowcount
    if changed != 1:
        raise TerminalLateEventError('late id is not pending')
    adjust_operational_late_id_count(
        connection,
        group_name,
        -1,
    )
    _promote_capacity_pending(connection, group_name, max_late_ids_per_group)
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def complete_obligation_scan(
    connection: sqlite3.Connection,
    group_name: str,
    obligation_id: str,
    expected_revision: int,
) -> GroupSnapshot:
    """Complete a closed pending row only after all above-floor source IDs are done.

    Args:
        connection: Live configured-level transaction connection.
        group_name: Exact owning group.
        obligation_id: Range whose post-close scan finished.
        expected_revision: Required group revision before admission.

    Raises:
        InvalidTransitionError: The range is not closed and pending, or an ID
            remains unfinished.
        RevisionConflictError: The expected group revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    row = connection.execute(
        """SELECT 1 FROM range_groups JOIN discovery_ranges USING (obligation_id)
        WHERE group_name = ? AND group_generation = ? AND obligation_id = ?
          AND obligation_state = 'pending' AND range_state = 'closed'""",
        (group_name, current.group_generation, obligation_id),
    ).fetchone()
    if row is None:
        raise InvalidTransitionError('obligation is not closed and pending')
    missing = connection.execute(
        """SELECT 1 FROM source_late_ids AS source
        WHERE source.obligation_id = ? AND source.event_id > ? AND NOT EXISTS (
            SELECT 1 FROM late_ids AS mark
            WHERE mark.group_name = ? AND mark.obligation_id = source.obligation_id
              AND mark.event_id = source.event_id AND mark.terminal_state = 'done'
        ) LIMIT 1""",
        (obligation_id, current.delivery_floor, group_name),
    ).fetchone()
    if missing is not None:
        raise InvalidTransitionError('source-listed late ids remain nonterminal')
    connection.execute(
        """
        UPDATE range_groups SET obligation_state = 'scan-complete' WHERE group_name =
        ? AND group_generation = ? AND obligation_id = ?
        """,
        (group_name, current.group_generation, obligation_id),
    )
    connection.execute(
        'DELETE FROM late_ids WHERE group_name = ? AND obligation_id = ?',
        (group_name, obligation_id),
    )
    refresh_operational_late_id_count(connection, group_name)
    connection.execute(BUMP_RANGE_REVISION)
    connection.execute(BUMP_INDEX_REVISION)
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def reserve_late_event(
    connection: sqlite3.Connection,
    group_name: str,
    request: LateEventRequest,
    expected_revision: int,
    max_late_ids_per_group: int,
) -> GroupSnapshot:
    """Reserve one exact late event or retain its capacity blocker.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: Owning obligation and exact source-listed event ID.
        expected_revision: Required compare-and-swap revision.
        max_late_ids_per_group: Maximum operational late-id capacity.

    Returns:
        Current or revision-advanced immutable reservation snapshot.

    Raises:
        InvalidTransitionError: No current-generation obligation owns the event
            or the event lies outside its retained bounds.
        RevisionConflictError: The expected revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    obligation = connection.execute(
        """
        SELECT ranges.low_id, ranges.high_id, bindings.group_generation
        FROM range_groups AS bindings JOIN discovery_ranges AS ranges USING (obligation_id)
        WHERE bindings.group_name = ? AND bindings.obligation_id = ?
          AND bindings.obligation_state = 'pending'
        """,
        (group_name, request.obligation_id),
    ).fetchone()
    if obligation is None or int(obligation[2]) != current.group_generation:
        raise InvalidTransitionError('late event has no current-generation obligation')
    if not int(obligation[0]) <= request.event_id <= int(obligation[1]):
        raise InvalidTransitionError('late event is outside its obligation bounds')
    existing = connection.execute(
        """
        SELECT terminal_state
        FROM late_ids
        WHERE group_name = ? AND obligation_id = ? AND event_id = ?
        """,
        (group_name, request.obligation_id, request.event_id),
    ).fetchone()
    if existing is not None and str(existing[0]) != 'capacity-pending':
        return current
    cost = 1
    if current.late_id_count + cost > max_late_ids_per_group:
        if existing is None:
            connection.execute(
                """
                INSERT INTO late_ids (
                    group_name, obligation_id, event_id, terminal_state, capacity_revision
                ) VALUES (?, ?, ?, 'capacity-pending', ?)
                """,
                (group_name, request.obligation_id, request.event_id, expected_revision + 1),
            )
    else:
        if existing is None:
            connection.execute(
                """
                INSERT INTO late_ids (group_name, obligation_id, event_id, terminal_state)
                VALUES (?, ?, ?, 'pending')
                """,
                (group_name, request.obligation_id, request.event_id),
            )
        else:
            connection.execute(
                """
                UPDATE late_ids
                SET terminal_state = 'pending', capacity_revision = NULL
                WHERE group_name = ? AND obligation_id = ? AND event_id = ?
                """,
                (group_name, request.obligation_id, request.event_id),
            )
        adjust_operational_late_id_count(connection, group_name, cost)
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)
