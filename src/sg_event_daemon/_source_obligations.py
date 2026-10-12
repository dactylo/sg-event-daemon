"""Manage durable source obligation rows, capacity, and retained lifecycle transitions."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, TypedDict, Unpack, cast

from . import _source_store as store
from ._group_types import (
    GroupSnapshot,
    ReplayRequiredNotice,
)
from ._source_schema import BUMP_INDEX_REVISION, BUMP_RANGE_REVISION

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Collection, Container


class _AdoptionOptions(TypedDict):
    """Type one immutable stream-adoption audit row."""

    adoption_id: str
    command_fingerprint: str
    stream_fingerprint: str
    reason: str
    retired_counts: tuple[int, int, int]


class _MarkerOptions(TypedDict):
    """Type one exact whole-group replay marker request."""

    group: str
    generation: int
    from_event_id: int
    new_token: Callable[[], str]


class _PendingAdoptionOptions(TypedDict):
    """Type one durable pending stream-adoption reservation."""

    adoption_id: str
    command_fingerprint: str
    prior_incarnation: str
    prior_stream_fingerprint: str
    target_stream_fingerprint: str
    reason: str


def _lowest_group_replay_id(
    connection: sqlite3.Connection,
    group: str,
    generation: int,
) -> int | None:
    """Return the lowest replay position retained for one group."""
    row = connection.execute(
        """
        SELECT MIN(replay_id)
        FROM (
            SELECT from_event_id AS replay_id
            FROM replay_markers
            WHERE group_name = ? AND group_generation = ?
            UNION ALL
            SELECT ranges.low_id AS replay_id
            FROM range_groups AS bindings
            JOIN discovery_ranges AS ranges USING (obligation_id)
            WHERE bindings.group_name = ?
              AND bindings.group_generation = ?
              AND bindings.obligation_state = 'pending'
        )
        """,
        (group, generation, group, generation),
    ).fetchone()
    return None if row is None or row[0] is None else int(row[0])


def _notice_from_row(
    site: str,
    source: str,
    row: tuple[str, int, str, str, int, str],
) -> ReplayRequiredNotice:
    """Restore one complete replay-marker row under its source identity."""
    return ReplayRequiredNotice(
        site=site,
        source=source,
        group=str(row[0]),
        group_generation=int(row[1]),
        source_generation=str(row[2]),
        marker_id=str(row[3]),
        from_event_id=int(row[4]),
        digest=str(row[5]),
    )


def _marker_for(
    connection: sqlite3.Connection,
    site: str,
    source: str,
    group: str,
    generation: int,
) -> ReplayRequiredNotice | None:
    """Return one existing group replay marker."""
    row = connection.execute(
        """
        SELECT group_name, group_generation, source_generation,
               marker_id, from_event_id, command_digest
        FROM replay_markers
        WHERE group_name = ? AND group_generation = ?
        """,
        (group, generation),
    ).fetchone()
    return None if row is None else _notice_from_row(site, source, row)


def _write_marker(connection: sqlite3.Connection, marker: ReplayRequiredNotice) -> None:
    """Upsert every marker field together, including its digest's source generation."""
    connection.execute(
        """
        INSERT INTO replay_markers (
            group_name, group_generation, source_generation,
            marker_id, from_event_id, command_digest
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (group_name, group_generation) DO UPDATE SET
            source_generation = excluded.source_generation,
            marker_id = excluded.marker_id,
            from_event_id = excluded.from_event_id,
            command_digest = excluded.command_digest
        """,
        (
            marker.group,
            marker.group_generation,
            marker.source_generation,
            marker.marker_id,
            marker.from_event_id,
            marker.digest,
        ),
    )


def _upsert_marker(
    connection: sqlite3.Connection,
    settings: store.SourceSettings,
    **options: Unpack[_MarkerOptions],
) -> ReplayRequiredNotice:
    """Create or lower one exact whole-group replay marker."""
    group = options['group']
    generation = options['generation']
    from_event_id = options['from_event_id']
    existing = _marker_for(connection, settings.site, settings.source, group, generation)
    if existing is not None:
        if from_event_id >= existing.from_event_id:
            return existing
        marker_identity = existing.marker_id
    else:
        marker_identity = options['new_token']()
    marker = ReplayRequiredNotice.create(
        site=settings.site,
        source=settings.source,
        group=group,
        group_generation=generation,
        source_generation=settings.source_generation,
        marker_id=marker_identity,
        from_event_id=from_event_id,
    )
    _write_marker(connection, marker)
    return marker


def _collapse_group(
    connection: sqlite3.Connection,
    settings: store.SourceSettings,
    mutation: store.RangeMutation,
    group: str,
    generation: int,
) -> None:
    """Replace one group's pending memberships with an atomically published replay halt."""
    retained = _lowest_group_replay_id(connection, group, generation)
    low = mutation.low_id if retained is None else min(mutation.low_id, retained)
    _upsert_marker(
        connection,
        settings,
        group=group,
        generation=generation,
        from_event_id=low,
        new_token=lambda: f'collapse-{mutation.obligation_id}-{generation}',
    )
    connection.execute(
        """DELETE FROM late_ids WHERE group_name = ? AND obligation_id IN (
            SELECT obligation_id FROM range_groups
            WHERE group_name = ? AND group_generation = ? AND obligation_state = 'pending'
        )""",
        (group, group, generation),
    )
    connection.execute(
        """
        DELETE FROM range_groups WHERE group_name = ? AND group_generation = ? AND
        obligation_state = 'pending'
        """,
        (group, generation),
    )
    connection.execute(
        """
        UPDATE group_state SET replay_required_from = CASE WHEN replay_required_from
        IS NULL OR replay_required_from > ? THEN ? ELSE replay_required_from END,
        operational_late_id_count = ( SELECT COUNT(*) FROM late_ids WHERE group_name
        = ? AND terminal_state = 'pending' ), revision = revision + 1 WHERE
        group_name = ? AND EXISTS ( SELECT 1 FROM groups WHERE group_name = ? AND
        group_generation = ? )
        """,
        (low, low, group, group, group, generation),
    )
    connection.execute(BUMP_RANGE_REVISION)


def adoption_count(connection: sqlite3.Connection) -> int:
    """Return retained stream-adoption audit count.

    Args:
        connection: Facade-owned connection.

    Returns:
        Number of immutable adoption audit rows.
    """
    return int(connection.execute('SELECT COUNT(*) FROM stream_adoptions').fetchone()[0])


def adoption_counts(connection: sqlite3.Connection) -> tuple[int, int, int]:
    """Return body-free evidence counts retired by stream adoption.

    Args:
        connection: Facade-owned connection.

    Returns:
        Range, source late-id, and marker counts.
    """
    queries = (
        'SELECT COUNT(*) FROM discovery_ranges',
        'SELECT COUNT(*) FROM source_late_ids',
        'SELECT COUNT(*) FROM replay_markers',
    )
    return cast(
        'tuple[int, int, int]',
        tuple(int(connection.execute(query).fetchone()[0]) for query in queries),
    )


def existing_adoption(
    connection: sqlite3.Connection,
    adoption_id: str,
) -> tuple[str, str, str] | None:
    """Return one retained stream-adoption command identity.

    Args:
        connection: Facade-owned connection.
        adoption_id: Stable adoption id.

    Returns:
        Command fingerprint, stream fingerprint, and reason, or `None`.
    """
    row = connection.execute(
        """
        SELECT command_fingerprint, stream_fingerprint, reason
        FROM stream_adoptions
        WHERE adoption_id = ?
        """,
        (adoption_id,),
    ).fetchone()
    return None if row is None else (str(row[0]), str(row[1]), str(row[2]))


def has_terminal_disposition(
    connection: sqlite3.Connection,
    *,
    group: str,
    disposition_id: str,
    command_fingerprint: str,
    terminal_result: str,
) -> bool:
    """Return whether one exact terminal source disposition is audited.

    Args:
        connection: Facade-owned connection.
        group: Stable group identity.
        disposition_id: Stable disposition identity.
        command_fingerprint: Exact command fingerprint.
        terminal_result: Exact audited terminal result.

    Returns:
        Whether the exact terminal proof exists.

    Raises:
        SourceIdentityError: The audited identity conflicts or is incomplete.
    """
    row = connection.execute(
        """
        SELECT group_name, command, command_fingerprint, terminal_result
        FROM disposition_audit
        WHERE disposition_id = ?
        """,
        (disposition_id,),
    ).fetchone()
    if row is None:
        return False
    if (str(row[0]), str(row[2]), str(row[3])) != (
        group,
        command_fingerprint,
        terminal_result,
    ):
        raise store.SourceIdentityError(
            'terminal disposition identity conflicts', code='terminal-disposition-id-reused'
        )
    if row[1] is None:
        raise store.SourceIdentityError(
            'terminal disposition identity is incomplete', code='terminal-disposition-incomplete'
        )
    return True


def insert_adoption(
    connection: sqlite3.Connection,
    **options: Unpack[_AdoptionOptions],
) -> None:
    """Stage one immutable stream-adoption audit record.

    Args:
        connection: Facade-owned FULL transaction connection.
        options: Stable command and stream identities, reason, and exact
            retired evidence counts.
    """
    connection.execute(
        """
        INSERT INTO stream_adoptions (
            adoption_id, command_fingerprint, stream_fingerprint,
            reason, retired_range_count, retired_late_id_count,
            retired_marker_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            options['adoption_id'],
            options['command_fingerprint'],
            options['stream_fingerprint'],
            options['reason'],
            *options['retired_counts'],
        ),
    )


def orphan(connection: sqlite3.Connection, current: GroupSnapshot) -> ReplayRequiredNotice:
    """Collapse one generation's memberships and retire it within a lifecycle transaction."""
    low = _lowest_group_replay_id(connection, current.group, current.group_generation)
    if current.replay_required_from is not None:
        low = (
            min(low, current.replay_required_from)
            if low is not None
            else current.replay_required_from
        )
    if low is None:
        low = int(store.meta(connection)[3]) + 1
    previous = _marker_for(
        connection, current.site, current.source, current.group, current.group_generation
    )
    marker = ReplayRequiredNotice.create(
        site=current.site,
        source=current.source,
        group=current.group,
        group_generation=current.group_generation,
        source_generation=current.source_generation,
        marker_id=previous.marker_id if previous else uuid.uuid4().hex,
        from_event_id=low,
    )
    connection.execute('DELETE FROM late_ids WHERE group_name = ?', (current.group,))
    connection.execute('DELETE FROM range_groups WHERE group_name = ?', (current.group,))
    connection.execute(
        "UPDATE groups SET active = 0, lifecycle = 'orphaned' WHERE group_name = ?",
        (current.group,),
    )
    _write_marker(connection, marker)
    connection.execute(
        'DELETE FROM discovery_ranges WHERE NOT EXISTS (SELECT 1 FROM range_groups '
        'WHERE range_groups.obligation_id = discovery_ranges.obligation_id '
        "AND obligation_state = 'pending')"
    )
    return marker


def pending_adoption(
    connection: sqlite3.Connection,
    adoption_id: str,
) -> tuple[str, str, str, str, str] | None:
    """Return one retained stream-adoption reservation.

    Args:
        connection: Facade-owned connection.
        adoption_id: Stable adoption id.

    Returns:
        Command fingerprint, prior incarnation, prior stream fingerprint,
        target stream fingerprint, and reason, or `None`.
    """
    row = connection.execute(
        """
        SELECT command_fingerprint, prior_incarnation,
               prior_stream_fingerprint, target_stream_fingerprint, reason
        FROM pending_adoptions
        WHERE adoption_id = ?
        """,
        (adoption_id,),
    ).fetchone()
    return (
        None if row is None else (str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]))
    )


def pending_adoption_for_halt(
    connection: sqlite3.Connection,
    *,
    command_fingerprint: str,
    prior_incarnation: str,
    prior_stream_fingerprint: str,
    target_stream_fingerprint: str,
) -> tuple[str, str] | None:
    """Return one reservation for the exact halted stream identity.

    Args:
        connection: Facade-owned connection.
        command_fingerprint: Stable semantic operator command identity.
        prior_incarnation: Incarnation halted before adoption.
        prior_stream_fingerprint: Persisted identity halted before adoption.
        target_stream_fingerprint: Configured identity to adopt.

    Returns:
        Adoption id and reason, or `None`.
    """
    row = connection.execute(
        """
        SELECT adoption_id, reason
        FROM pending_adoptions
        WHERE command_fingerprint = ?
          AND prior_incarnation = ?
          AND prior_stream_fingerprint = ?
          AND target_stream_fingerprint = ?
        """,
        (
            command_fingerprint,
            prior_incarnation,
            prior_stream_fingerprint,
            target_stream_fingerprint,
        ),
    ).fetchone()
    return None if row is None else (str(row[0]), str(row[1]))


def pending_count(connection: sqlite3.Connection, *, open_only: bool = False) -> int:
    """Count pending group memberships and replay markers.

    Args:
        connection: Facade-owned connection.
        open_only: Charge only open memberships to aggregate source capacity.
    """
    rows = connection.execute(
        """SELECT COUNT(*) FROM range_groups JOIN discovery_ranges USING (obligation_id)
        WHERE obligation_state = 'pending' AND (? = 0 OR range_state = 'open')""",
        (int(open_only),),
    ).fetchone()[0]
    markers = connection.execute('SELECT COUNT(*) FROM replay_markers').fetchone()[0]
    return int(rows) + int(markers)


def check_range_capacity(
    connection: sqlite3.Connection,
    settings: store.SourceSettings,
    eligible_groups: Collection[tuple[str, int]],
) -> set[tuple[str, int]]:
    """Check projected retained rows before admitting an incoming range.

    Args:
        connection: Facade-owned live transaction connection.
        settings: Source identity and aggregate capacity settings.
        eligible_groups: Group generations requiring the incoming range.

    Returns:
        Group identities already covered by replay markers.

    Raises:
        SourceCapacityError: Projected retained rows exceed the aggregate
            capacity.
    """
    marker_covered: set[tuple[str, int]] = set()
    projected = pending_count(connection, open_only=True)
    for group, generation in eligible_groups:
        if _marker_for(connection, settings.site, settings.source, group, generation) is None:
            pending, open_pending = connection.execute(
                """SELECT COUNT(*), COALESCE(SUM(range_state = 'open'), 0)
                FROM range_groups JOIN discovery_ranges USING (obligation_id)
                WHERE group_name = ? AND group_generation = ?
                AND obligation_state = 'pending'""",
                (group, generation),
            ).fetchone()
            projected += 1
            if pending >= settings.max_group_obligations:
                projected -= open_pending
        else:
            marker_covered.add((group, generation))
    if projected > settings.max_pending_group_obligations:
        raise store.SourceCapacityError(
            'obligation-capacity-exhausted', code='obligation-capacity-exhausted'
        )
    return marker_covered


def replace_range_revision(
    connection: sqlite3.Connection,
    settings: store.SourceSettings,
    mutation: store.RangeMutation,
    marker_covered: Container[tuple[str, int]],
) -> None:
    """Bind every active group in the range's own transaction.

    Args:
        connection: Facade-owned live transaction connection.
        settings: Source identity and capacity policies.
        mutation: Newly staged range revision and prior eligibility points.
        marker_covered: Group generations already requiring replay.
    """
    connection.execute(
        'DELETE FROM range_groups WHERE obligation_id = ?', (mutation.obligation_id,)
    )
    eligibility: dict[tuple[str, int], float] = {}
    for group, generation, point in mutation.existing_eligibility:
        eligibility[group, generation] = max(eligibility.get((group, generation), 0.0), point)
    for group, generation, floor, holdback in connection.execute(
        """
        SELECT group_name, group_generation, delivery_floor, holdback_secs FROM
        groups JOIN group_state USING (group_name) WHERE active = 1
        """
    ).fetchall():
        pending = floor < mutation.high_id and (group, generation) not in marker_covered
        pending = (
            pending
            and _marker_for(connection, settings.site, settings.source, group, generation) is None
        )
        count = connection.execute(
            """
            SELECT COUNT(*) FROM range_groups WHERE group_name = ? AND
            group_generation = ? AND obligation_state = 'pending'
            """,
            (group, generation),
        ).fetchone()[0]
        if pending and count >= settings.max_group_obligations:
            _collapse_group(connection, settings, mutation, group, generation)
            pending = False
        connection.execute(
            'INSERT INTO range_groups VALUES (?, ?, ?, ?, ?, ?)',
            (
                mutation.obligation_id,
                group,
                generation,
                floor,
                max(
                    mutation.opened_successful_at + holdback,
                    eligibility.get((group, generation), 0.0),
                ),
                'pending' if pending else 'satisfied-by-floor',
            ),
        )
    connection.execute(BUMP_RANGE_REVISION)


def replay_markers(
    connection: sqlite3.Connection,
    settings: store.SourceSettings,
) -> tuple[ReplayRequiredNotice, ...]:
    """Restore every retained whole-group replay marker.

    Args:
        connection: Facade-owned connection.
        settings: Source identity settings.

    Returns:
        Exact markers in deterministic group order.
    """
    rows = connection.execute(
        """
        SELECT group_name, group_generation, source_generation,
               marker_id, from_event_id, command_digest
        FROM replay_markers
        ORDER BY group_name, group_generation
        """
    ).fetchall()
    return tuple(_notice_from_row(settings.site, settings.source, row) for row in rows)


def reserve_adoption(
    connection: sqlite3.Connection,
    **options: Unpack[_PendingAdoptionOptions],
) -> None:
    """Stage one fresh durable stream-adoption reservation.

    Args:
        connection: Facade-owned FULL transaction connection.
        options: Fresh identity, halted-stream binding, target, and reason.
    """
    connection.execute(
        """
        INSERT INTO pending_adoptions (
            adoption_id, command_fingerprint, prior_incarnation,
            prior_stream_fingerprint, target_stream_fingerprint, reason
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            options['adoption_id'],
            options['command_fingerprint'],
            options['prior_incarnation'],
            options['prior_stream_fingerprint'],
            options['target_stream_fingerprint'],
            options['reason'],
        ),
    )


def retire_adoption_reservation(
    connection: sqlite3.Connection,
    adoption_id: str,
) -> None:
    """Stage retirement of one completed adoption reservation.

    Args:
        connection: Facade-owned FULL transaction connection.
        adoption_id: Completed stable adoption identity.
    """
    connection.execute(
        'DELETE FROM pending_adoptions WHERE adoption_id = ?',
        (adoption_id,),
    )


def retire_stream_evidence(
    connection: sqlite3.Connection,
) -> None:
    """Stage retirement of every operational old-stream obligation row.

    Args:
        connection: Facade-owned FULL transaction connection.
    """
    statements = (
        'DELETE FROM replay_markers',
        'DELETE FROM discovery_ranges',
    )
    for statement in statements:
        connection.execute(statement)
    connection.execute(BUMP_INDEX_REVISION)
    connection.execute(BUMP_RANGE_REVISION)
