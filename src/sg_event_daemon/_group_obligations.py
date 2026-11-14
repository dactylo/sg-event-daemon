"""Manage durable obligation scans, late marks, and capacity transitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._gaps import id_runs
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
    from collections.abc import Sequence

    from ._gaps import IdRun
    from ._group_types import GroupSnapshot


@dataclass(frozen=True)
class LateEventRequest:
    """Identify one exact late event and its owning obligation."""

    obligation_id: str
    event_id: int


def missing_late_runs(
    connection: sqlite3.Connection,
    group_name: str,
    obligation_id: str,
) -> tuple[IdRun, ...]:
    """Return above-floor source IDs without done marks for the current generation.

    Args:
        connection: Live group-state database connection.
        group_name: Exact owning group.
        obligation_id: Range whose unfinished source-listed IDs are requested.

    Returns:
        Ascending inclusive runs of unfinished IDs.

    Raises:
        sqlite3.Error: SQLite cannot read the unfinished IDs.
    """
    rows = connection.execute(
        """SELECT source.event_id FROM source_late_ids AS source
        JOIN range_groups AS binding USING (obligation_id)
        JOIN groups AS meta ON meta.group_name = binding.group_name
          AND meta.group_generation = binding.group_generation
        JOIN group_state AS state ON state.group_name = meta.group_name
        WHERE binding.group_name = ? AND source.obligation_id = ?
          AND source.event_id > state.delivery_floor AND NOT EXISTS (
            SELECT 1 FROM late_ids AS mark
            WHERE mark.group_name = binding.group_name
              AND mark.obligation_id = source.obligation_id
              AND mark.event_id = source.event_id AND mark.terminal_state = 'done'
        ) ORDER BY source.event_id""",
        (group_name, obligation_id),
    )
    return id_runs(int(row[0]) for row in rows)


def complete_obligation_scan(
    connection: sqlite3.Connection,
    group_name: str,
    obligation_id: str,
    expected_revision: int,
    excused: Sequence[IdRun] = (),
) -> GroupSnapshot:
    """Complete a closed pending row with exactly its unfinished IDs excused.

    Args:
        connection: Live configured-level transaction connection.
        group_name: Exact owning group.
        obligation_id: Range whose post-close scan finished.
        expected_revision: Required group revision before admission.
        excused: Ascending runs of IDs given up by the completed read.

    Raises:
        InvalidTransitionError: The range is not closed and pending, or its
            unfinished IDs differ from the excused set.
        RevisionConflictError: The expected group revision differs.
        ValueError: The stored callback manifest is invalid.
        sqlite3.Error: SQLite cannot read or complete the obligation.
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
    excused_runs = id_runs(event_id for low, high in excused for event_id in range(low, high + 1))
    if missing_late_runs(connection, group_name, obligation_id) != excused_runs:
        raise InvalidTransitionError('excused ids differ from the missing ids')
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


def promote_capacity_pending(
    connection: sqlite3.Connection,
    group_name: str,
    max_late_ids_per_group: int,
) -> int:
    """Admit the stable reservation prefix that fits current operational capacity.

    Args:
        connection: Live transaction connection for the initialized group.
        group_name: Exact owning group.
        max_late_ids_per_group: Maximum number of operational pending IDs.

    Returns:
        Number promoted from capacity-pending to pending, selected by capacity
        revision, obligation id, and event id. The durable pending count is
        increased by the same amount.

    Raises:
        sqlite3.Error: SQLite cannot read or promote the reservations.
    """
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
    if delta:
        adjust_operational_late_id_count(connection, group_name, delta)
    return delta


def complete_late_id_step(
    connection: sqlite3.Connection,
    group_name: str,
    request: LateEventRequest,
    expected_revision: int,
    max_late_ids_per_group: int,
) -> int:
    """Complete a late reservation and return the advanced revision.

    Released capacity admits the oldest capacity-pending reservations within
    the same transaction.

    Args:
        connection: Live transaction connection for the initialized group.
        group_name: Exact owning group.
        request: Exact pending obligation and event identity to complete.
        expected_revision: Required group revision before admission.
        max_late_ids_per_group: Maximum number of operational pending IDs after
            capacity promotion.

    Raises:
        RevisionConflictError: The expected group revision differs.
        TerminalLateEventError: The requested late id is not pending.
        sqlite3.Error: SQLite cannot complete the reservation or promote
            waiting IDs.
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
    promote_capacity_pending(connection, group_name, max_late_ids_per_group)
    bump_revision(connection, group_name, expected_revision)
    return expected_revision + 1


def reserve_late_event_step(
    connection: sqlite3.Connection,
    group_name: str,
    request: LateEventRequest,
    expected_revision: int,
    max_late_ids_per_group: int,
) -> int:
    """Reserve a late event and return its current or advanced revision.

    An existing reservation outside capacity-pending is unchanged. Otherwise,
    reserve operational capacity when available or retain a capacity-pending
    reservation, then advance the group revision even if it still cannot fit.

    Args:
        connection: Live transaction connection for the initialized group.
        group_name: Exact owning group.
        request: Event identity and its current-generation pending obligation.
        expected_revision: Required group revision before admission.
        max_late_ids_per_group: Maximum number of operational pending IDs.

    Returns:
        The unchanged revision for an existing pending or done reservation;
        otherwise the revision advanced by one.

    Raises:
        RevisionConflictError: The expected group revision differs.
        InvalidTransitionError: The obligation is absent, is not pending in the
            current generation, or does not contain the event id.
        sqlite3.Error: SQLite cannot read or reserve the late event.
    """
    check_revision(connection, group_name, expected_revision)
    generation, late_id_count = connection.execute(
        'SELECT m.group_generation, s.operational_late_id_count FROM groups AS m '
        'JOIN group_state AS s USING (group_name) WHERE m.group_name = ?',
        (group_name,),
    ).fetchone()
    obligation = connection.execute(
        """
        SELECT ranges.low_id, ranges.high_id, bindings.group_generation
        FROM range_groups AS bindings JOIN discovery_ranges AS ranges USING (obligation_id)
        WHERE bindings.group_name = ? AND bindings.obligation_id = ?
          AND bindings.obligation_state = 'pending'
        """,
        (group_name, request.obligation_id),
    ).fetchone()
    if obligation is None or int(obligation[2]) != int(generation):
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
        return expected_revision
    cost = 1
    if int(late_id_count) + cost > max_late_ids_per_group:
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
    return expected_revision + 1
