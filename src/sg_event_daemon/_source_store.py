"""Body-free source range projections and primitives."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import TYPE_CHECKING, Literal, TypeAlias, TypedDict, Unpack, cast

from ._source_schema import BUMP_RANGE_REVISION

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Iterable, Sequence

    from ._group_types import (
        ReplayRequiredNotice,
    )

_SourceCapacityCode: TypeAlias = Literal[
    'frame-too-large',
    'late-capacity-exhausted',
    'obligation-capacity-exhausted',
    'range-capacity-exhausted',
]

_SourceIdentityCode: TypeAlias = Literal[
    'adoption-command-conflict',
    'adoption-id-not-current',
    'adoption-id-reused',
    'adoption-identity-required',
    'adoption-requires-identity-halt',
    'adoption-requires-parked-groups',
    'adoption-requires-prior-incarnation',
    'adoption-reservation-invalid',
    'adoption-target-mismatch',
    'configured-stream-conflict',
    'source-database-identity-changed',
    'stream-identity-changed',
    'terminal-disposition-id-reused',
    'terminal-disposition-incomplete',
]

_SourceStatus: TypeAlias = Literal[
    'late-capacity-exhausted',
    'obligation-capacity-exhausted',
    'range-capacity-exhausted',
    'range-query-page-cap',
    'running',
    'stream-identity-changed',
    'uninitialized',
]
_SourceMetaRow: TypeAlias = tuple[str, str, int, int, float, str, str | None, str, str | None]
_DiscoveryRangeRow: TypeAlias = tuple[
    str,
    int,
    int,
    float,
    float,
    int,
    int,
    str,
    float | None,
    int,
    int,
    int,
    int,
]


CAPACITY_CONDITIONS = frozenset(
    {
        'late-capacity-exhausted',
        'obligation-capacity-exhausted',
        'range-capacity-exhausted',
    }
)

_SOURCE_CONDITION_STATUSES: dict[str, _SourceStatus] = {
    'late-capacity-exhausted': 'late-capacity-exhausted',
    'obligation-capacity-exhausted': 'obligation-capacity-exhausted',
    'range-capacity-exhausted': 'range-capacity-exhausted',
    'range-query-page-cap': 'range-query-page-cap',
    'stream-identity-changed': 'stream-identity-changed',
}


@dataclass(frozen=True)
class DiscoveryRange:
    """Describe one inclusive body-free late-discovery range.

    `scan_cycle_pages` counts completed provider pages in the current query
    cycle; resuming after the page cap resets that cycle count while retaining
    cumulative logical-scan progress. `scan_next_id` is the inclusive position
    for the next provider query.
    """

    obligation_id: str
    low_id: int
    high_id: int
    opened_successful_at: float
    _visibility_deadline: float
    _revision: int
    _close_generation: int
    state: Literal['closed', 'open']
    late_ids: tuple[int, ...]
    group_eligibility: tuple[tuple[str, float], ...]
    _scan_started_at: float | None
    scan_next_id: int
    _scan_pages: int
    _degraded: bool
    scan_cycle_pages: int = 0


@dataclass(frozen=True)
class RangeMutation:
    """Describe one staged inclusive range revision.

    Each `existing_eligibility` item contains `(group_name, group_generation,
    eligibility_point)` for one retained group revision.
    """

    obligation_id: str
    merged_ids: tuple[str, ...]
    low_id: int
    high_id: int
    opened_successful_at: float
    visibility_deadline: float
    revision: int
    existing_eligibility: tuple[tuple[str, int, float], ...]


@dataclass(frozen=True)
class RangeScanProgress:
    """Describe one commit chunk within a provider range-query page."""

    scanned_through: int
    query_page_complete: bool


class SourceCapacityError(RuntimeError):
    """Report a source limit that halted before affected evidence committed.

    `code` identifies the capacity limit independently of the diagnostic.
    """

    def __init__(self, message: str, *, code: _SourceCapacityCode | None = None) -> None:
        """Keep the diagnostic separate from the machine-readable reason.

        Args:
            message: Human-readable diagnostic preserved by `str(error)`.
            code: Stable reason for control flow; `None` is unclassified.
        """
        super().__init__(message)
        self.code = code


class SourceIdentityError(RuntimeError):
    """Report a rejected stream identity or adoption transition.

    `code` identifies the rejected transition independently of the diagnostic.
    """

    def __init__(self, message: str, *, code: _SourceIdentityCode | None = None) -> None:
        """Keep the diagnostic separate from the machine-readable reason.

        Args:
            message: Human-readable diagnostic preserved by `str(error)`.
            code: Stable reason for control flow; `None` is unclassified.
        """
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class SourcePollState:
    """Project the source fields needed by one polling cycle."""

    initialized: bool
    frontier: int
    successful_clock: float
    source_generation: str
    stream_incarnation: str | None


@dataclass(frozen=True)
class SourceSettings:
    """Hold validated process-local source policies."""

    site: str
    source: str
    configured_fingerprint: str
    source_generation: str
    visibility_overlap_secs: float
    max_open_ranges: int
    max_range_query_pages: int
    max_pending_group_obligations: int
    max_group_obligations: int
    max_source_late_ids: int
    max_ipc_frame_bytes: int
    commit_batch_max_bytes: int


@dataclass(frozen=True)
class SourceSnapshot:
    """Describe one immutable body-free source-state projection."""

    site: str
    source: str
    initialized: bool
    frontier: int
    successful_clock: float
    stream_fingerprint: str
    stream_incarnation: str | None
    source_generation: str
    status: _SourceStatus
    conditions: tuple[str, ...]
    ranges: tuple[DiscoveryRange, ...]
    pending_obligations: int
    replay_markers: tuple[ReplayRequiredNotice, ...]
    adoption_count: int


class _RangeStageOptions(TypedDict):
    """Type one inclusive range observation."""

    low_id: int
    high_id: int
    successful_clock: float
    new_token: Callable[[], str]


class _SnapshotOptions(TypedDict):
    """Type obligation-owned evidence composed into a source snapshot."""

    pending_obligations: int
    replay_markers: tuple[ReplayRequiredNotice, ...]
    adoption_count: int


def _active_groups(
    connection: sqlite3.Connection,
) -> tuple[tuple[str, int, int, float], ...]:
    """Return active group, generation, floor, and holdback tuples in identity order."""
    return tuple(
        (
            str(row[0]),
            int(row[1]),
            int(row[2]),
            float(row[3]),
        )
        for row in connection.execute(
            """
            SELECT group_name, group_generation, delivery_floor,
                   holdback_secs
            FROM groups JOIN group_state USING (group_name)
            WHERE active = 1
            ORDER BY group_name, group_generation
            """
        )
    )


def _clear_range_query_page_condition(connection: sqlite3.Connection) -> None:
    """Clear an obsolete range-query page-cap condition."""
    connection.execute(
        """
        UPDATE source_meta
        SET condition = NULL
        WHERE singleton = 1
          AND condition = 'range-query-page-cap'
          AND NOT EXISTS (
              SELECT 1
              FROM discovery_ranges
              WHERE range_state = 'open' AND degraded = 1
          )
        """
    )


def _discovery_range_from_row(
    row: Sequence[object],
    late_ids: Sequence[int],
    group_eligibility: Sequence[tuple[str, float]],
) -> DiscoveryRange:
    """Convert one persisted range row and its owned evidence."""
    normalized = cast('_DiscoveryRangeRow', tuple(row))
    return DiscoveryRange(
        obligation_id=str(normalized[0]),
        low_id=int(normalized[1]),
        high_id=int(normalized[2]),
        opened_successful_at=float(normalized[3]),
        _visibility_deadline=float(normalized[4]),
        _revision=int(normalized[5]),
        _close_generation=int(normalized[6]),
        state=cast("Literal['closed', 'open']", str(normalized[7])),
        late_ids=tuple(late_ids),
        group_eligibility=tuple(group_eligibility),
        _scan_started_at=None if normalized[8] is None else float(normalized[8]),
        scan_next_id=int(normalized[9]),
        _scan_pages=int(normalized[10]),
        _degraded=bool(normalized[11]),
        scan_cycle_pages=int(normalized[12]),
    )


def _has_open_degraded_range(connection: sqlite3.Connection) -> bool:
    """Return whether an open range still owns page-cap evidence."""
    return bool(
        connection.execute(
            """
            SELECT 1
            FROM discovery_ranges
            WHERE range_state = 'open' AND degraded = 1
            LIMIT 1
            """
        ).fetchone()
    )


def _range_is_closed(
    connection: sqlite3.Connection,
    obligation_id: str,
) -> bool:
    """Return whether a retained range is closed, rejecting unknown ranges."""
    row = connection.execute(
        'SELECT range_state FROM discovery_ranges WHERE obligation_id = ?',
        (obligation_id,),
    ).fetchone()
    if row is None:
        raise ValueError('unknown discovery range')
    return str(row[0]) == 'closed'


def _range_late_ids(
    connection: sqlite3.Connection,
    obligation_id: str,
) -> tuple[int, ...]:
    """Return exact retained late ids for one range."""
    return tuple(
        int(item[0])
        for item in connection.execute(
            """
            SELECT event_id FROM source_late_ids
            WHERE obligation_id = ? ORDER BY event_id
            """,
            (obligation_id,),
        )
    )


def _require_nonnegative_integer(value: int, name: str) -> None:
    """Reject Boolean, negative, or non-integer ids."""
    if type(value) is not int or value < 0:
        raise ValueError(f'{name} must be a nonnegative integer')


def begin_range_scan(
    connection: sqlite3.Connection,
    settings: SourceSettings,
    obligation_id: str,
    *,
    successful_clock: float,
) -> None:
    """Stage the start or resumption of one bounded range scan.

    Args:
        connection: Facade-owned live transaction connection.
        settings: Validated source scan limit.
        obligation_id: Exact retained range.
        successful_clock: Current successful-query clock.

    Raises:
        ValueError: The obligation does not identify a retained open range.
    """
    row = connection.execute(
        """
        SELECT low_id, high_id, visibility_deadline, scan_started_at,
               scan_next_id, scan_cycle_pages
        FROM discovery_ranges
        WHERE obligation_id = ? AND range_state = 'open'
        """,
        (obligation_id,),
    ).fetchone()
    if row is None:
        raise ValueError('unknown open discovery range')
    low_id = int(row[0])
    high_id = int(row[1])
    deadline = float(row[2])
    scan_started = None if row[3] is None else float(row[3])
    scan_next = int(row[4])
    scan_cycle_pages = int(row[5])
    new_sweep = scan_started is None or (
        scan_next > high_id and successful_clock >= deadline and scan_started < deadline
    )
    if new_sweep:
        scan_started = successful_clock
        scan_next = low_id
        connection.execute(
            """
            UPDATE discovery_ranges
            SET scan_started_at = ?, scan_next_id = ?,
                scan_pages = 0, degraded = 0, scan_cycle_pages = 0
            WHERE obligation_id = ?
            """,
            (scan_started, scan_next, obligation_id),
        )
    elif scan_cycle_pages >= settings.max_range_query_pages:
        connection.execute(
            """
            UPDATE discovery_ranges
            SET scan_cycle_pages = 0
            WHERE obligation_id = ?
            """,
            (obligation_id,),
        )


def can_query(connection: sqlite3.Connection, configured_fingerprint: str) -> bool:
    """Return whether the persisted and configured stream identities match.

    Args:
        connection: Facade-owned connection.
        configured_fingerprint: Current configured stream identity.

    Returns:
        `True` when SG queries are permitted.
    """
    row = connection.execute('SELECT stream_fingerprint, condition FROM source_meta').fetchone()
    return (
        row is not None
        and str(row[0]) == configured_fingerprint
        and row[1] != 'stream-identity-changed'
    )


def clear_capacity_condition(connection: sqlite3.Connection) -> None:
    """Clear a recoverable capacity condition after caller-verified recovery.

    The caller must establish the applicable recovery predicate before this
    call. An open degraded range preserves or restores `range-query-page-cap`
    instead of returning the source to running.

    Args:
        connection: Facade-owned live transaction connection.
    """
    row = connection.execute('SELECT condition FROM source_meta').fetchone()
    recoverable = CAPACITY_CONDITIONS | {'range-query-page-cap'}
    if row is None or row[0] not in recoverable:
        return
    condition = 'range-query-page-cap' if _has_open_degraded_range(connection) else None
    connection.execute(
        'UPDATE source_meta SET condition = ? WHERE singleton = 1',
        (condition,),
    )


def disposition_audit_count(connection: sqlite3.Connection) -> int:
    """Return the cumulative immutable disposition audit count.

    Args:
        connection: Facade-owned source connection.

    Returns:
        Number of retained disposition audit rows.
    """
    return int(connection.execute('SELECT COUNT(*) FROM disposition_audit').fetchone()[0])


def due_range_id(connection: sqlite3.Connection) -> str | None:
    """Return the first due active range identity.

    Args:
        connection: Facade-owned connection.

    Returns:
        One due range in deadline order, or `None`.
    """
    row = connection.execute(
        """
        SELECT ranges.obligation_id
        FROM discovery_ranges AS ranges
        WHERE ranges.range_state = 'open'
          AND ranges.visibility_deadline <= (
              SELECT successful_clock FROM source_meta WHERE singleton = 1
          )
          AND EXISTS (
              SELECT 1
              FROM range_groups AS groups
              WHERE groups.obligation_id = ranges.obligation_id
                AND groups.obligation_state = 'pending'
          )
        ORDER BY ranges.visibility_deadline, ranges.low_id,
                 ranges.obligation_id
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else str(row[0])


def eligible_groups(
    connection: sqlite3.Connection,
    high_id: int,
) -> tuple[tuple[str, int, int, float], ...]:
    """Return active generations whose delivery floor is below a range.

    Args:
        connection: Facade-owned connection.
        high_id: Inclusive range upper bound.

    Returns:
        Rows shaped as `(group_name, group_generation, delivery_floor,
        holdback_secs)`, ordered by group name and generation.
    """
    return tuple(row for row in _active_groups(connection) if row[2] < high_id)


def initialize(
    connection: sqlite3.Connection,
    latest_id: int | None,
    *,
    incarnation: str | None,
) -> None:
    """Stage first-start frontier, marker, and incarnation.

    Already initialized state is unchanged. First initialization requires an
    incarnation when none is retained.

    Args:
        connection: Facade-owned FULL transaction connection.
        latest_id: Nonnegative integer latest visible id, or `None` for an
            empty source; validated even when already initialized.
        incarnation: Initial non-reusable stream incarnation.

    Raises:
        ValueError: The latest id is not a nonnegative integer or first
            initialization lacks a stream incarnation.
    """
    frontier = 0 if latest_id is None else latest_id
    _require_nonnegative_integer(frontier, 'latest id')
    existing = connection.execute(
        'SELECT initialized, stream_incarnation FROM source_meta'
    ).fetchone()
    if existing is None:
        raise RuntimeError('source metadata is missing')
    if bool(existing[0]):
        return
    stream_incarnation = (None if existing[1] is None else str(existing[1])) or incarnation
    if not stream_incarnation:
        raise ValueError('first source initialization requires an incarnation')
    connection.execute(
        """
        UPDATE source_meta
        SET initialized = 1,
            frontier = ?,
            stream_incarnation = ?,
            successful_clock = 0.0,
            condition = NULL
        WHERE singleton = 1
        """,
        (frontier, stream_incarnation),
    )


def initialize_meta(
    connection: sqlite3.Connection,
    *,
    site: str,
    source: str,
    stream_fingerprint: str,
    source_generation: str,
) -> None:
    """Create or validate the singleton source identity row.

    Args:
        connection: Facade-owned live transaction connection.
        site: Stable site identity.
        source: Stable configured source name.
        stream_fingerprint: Non-secret configured stream identity.
        source_generation: Supervisor-issued source process identity.

    Raises:
        SourceIdentityError: The database belongs to another source.
    """
    row = connection.execute(
        'SELECT site, source_name, stream_fingerprint FROM source_meta'
    ).fetchone()
    if row is None:
        connection.execute(
            """
            INSERT INTO source_meta (
                singleton, site, source_name, stream_fingerprint,
                stream_incarnation, source_generation, initialized,
                frontier, successful_clock, condition
            ) VALUES (1, ?, ?, ?, NULL, ?, 0, 0, 0.0, NULL)
            """,
            (site, source, stream_fingerprint, source_generation),
        )
        return
    if str(row[0]) != site or str(row[1]) != source:
        raise SourceIdentityError(
            'configured source database identity changed', code='source-database-identity-changed'
        )
    connection.execute(
        """
        UPDATE source_meta
        SET source_generation = ?,
            condition = CASE
                WHEN stream_fingerprint != ?
                THEN 'stream-identity-changed'
                WHEN condition = 'stream-identity-changed'
                THEN NULL
                ELSE condition
            END
        WHERE singleton = 1
        """,
        (source_generation, stream_fingerprint),
    )


def meta(connection: sqlite3.Connection) -> _SourceMetaRow:
    """Return the singleton source metadata row.

    Args:
        connection: Facade-owned connection.

    Returns:
        The exact `(site, source_name, initialized, frontier, successful_clock,
        stream_fingerprint, stream_incarnation, source_generation, condition)`
        persisted tuple.

    Raises:
        RuntimeError: The database lacks its singleton metadata row.
    """
    row = connection.execute(
        """
        SELECT site, source_name, initialized, frontier, successful_clock,
               stream_fingerprint, stream_incarnation, source_generation,
               condition
        FROM source_meta
        WHERE singleton = 1
        """
    ).fetchone()
    if row is None:
        raise RuntimeError('source metadata is missing')
    return cast('_SourceMetaRow', tuple(row))


def poll_state(connection: sqlite3.Connection) -> SourcePollState:
    """Return the source fields needed by one polling cycle.

    Args:
        connection: Facade-owned connection.

    Returns:
        Initialization, frontier, successful clock, and stream identities.

    Raises:
        RuntimeError: The database lacks its singleton metadata row.
    """
    row = connection.execute(
        """
        SELECT initialized, frontier, successful_clock, source_generation, stream_incarnation
        FROM source_meta
        WHERE singleton = 1
        """
    ).fetchone()
    if row is None:
        raise RuntimeError('source metadata is missing')
    return SourcePollState(
        initialized=bool(row[0]),
        frontier=int(row[1]),
        successful_clock=float(row[2]),
        source_generation=str(row[3]),
        stream_incarnation=None if row[4] is None else str(row[4]),
    )


def prune_unbound_ranges(connection: sqlite3.Connection) -> None:
    """Retire every range without pending delivery work, including open ranges."""
    changed = connection.execute(
        """
        DELETE FROM discovery_ranges
        WHERE NOT EXISTS (
            SELECT 1 FROM range_groups
            WHERE range_groups.obligation_id = discovery_ranges.obligation_id
              AND obligation_state = 'pending'
        )
        """
    ).rowcount
    if changed:
        connection.execute(BUMP_RANGE_REVISION)


def range_for(
    connection: sqlite3.Connection,
    obligation_id: str,
) -> DiscoveryRange | None:
    """Return one exact body-free discovery range.

    Args:
        connection: Facade-owned connection.
        obligation_id: Exact retained range identity.

    Returns:
        The retained range, or `None` when it is absent.
    """
    row = connection.execute(
        """
        SELECT obligation_id, low_id, high_id, opened_successful_at,
               visibility_deadline, source_revision,
               close_generation, range_state, scan_started_at,
               scan_next_id, scan_pages, degraded, scan_cycle_pages
        FROM discovery_ranges
        WHERE obligation_id = ?
        """,
        (obligation_id,),
    ).fetchone()
    if row is None:
        return None
    late_ids = _range_late_ids(connection, obligation_id)
    eligibility = tuple(
        (str(group), float(point))
        for group, point in connection.execute(
            """
            SELECT group_name, eligibility_point
            FROM range_groups
            WHERE obligation_id = ? AND obligation_state = 'pending'
            ORDER BY group_name, group_generation
            """,
            (obligation_id,),
        )
    )
    return _discovery_range_from_row(row, late_ids, eligibility)


def ranges(connection: sqlite3.Connection) -> tuple[DiscoveryRange, ...]:
    """Project every body-free range in inclusive order.

    Args:
        connection: Facade-owned connection.

    Returns:
        Immutable retained ranges.
    """
    rows = connection.execute(
        """
        SELECT obligation_id, low_id, high_id, opened_successful_at,
               visibility_deadline, source_revision,
               close_generation, range_state, scan_started_at,
               scan_next_id, scan_pages, degraded, scan_cycle_pages
        FROM discovery_ranges
        ORDER BY low_id, high_id, obligation_id
        """
    ).fetchall()
    late_ids_by_range: dict[str, list[int]] = {}
    for obligation_id, event_id in connection.execute(
        """
        SELECT obligation_id, event_id
        FROM source_late_ids
        ORDER BY obligation_id, event_id
        """
    ):
        late_ids_by_range.setdefault(str(obligation_id), []).append(int(event_id))
    eligibility_by_range: dict[str, list[tuple[str, float]]] = {}
    for obligation_id, group_name, eligibility_point in connection.execute(
        """
        SELECT obligation_id, group_name, eligibility_point
        FROM range_groups
        WHERE obligation_state = 'pending'
        ORDER BY obligation_id, group_name, group_generation
        """
    ):
        eligibility_by_range.setdefault(str(obligation_id), []).append(
            (str(group_name), float(eligibility_point))
        )
    result: list[DiscoveryRange] = []
    for row in rows:
        obligation_id = str(row[0])
        result.append(
            _discovery_range_from_row(
                row,
                late_ids_by_range.get(obligation_id, ()),
                eligibility_by_range.get(obligation_id, ()),
            )
        )
    return tuple(result)


def reset_for_adoption(
    connection: sqlite3.Connection,
    *,
    stream_fingerprint: str,
    incarnation: str,
) -> None:
    """Stage source meta reset for an adopted stream.

    Args:
        connection: Facade-owned FULL transaction connection.
        stream_fingerprint: Adopted configured stream identity.
        incarnation: Stable adoption and next initialization identity.
    """
    connection.execute(
        """
        UPDATE source_meta
        SET stream_fingerprint = ?,
            stream_incarnation = ?,
            initialized = 0,
            frontier = 0,
            successful_clock = 0.0,
            condition = NULL
        WHERE singleton = 1
        """,
        (stream_fingerprint, incarnation),
    )


def set_condition(connection: sqlite3.Connection, condition: str) -> None:
    """Stage one source halt condition.

    Args:
        connection: Facade-owned live transaction connection.
        condition: Exact source condition code.
    """
    connection.execute(
        'UPDATE source_meta SET condition = ? WHERE singleton = 1',
        (condition,),
    )


def record_range_scan_page(
    connection: sqlite3.Connection,
    settings: SourceSettings,
    obligation_id: str,
    progress: RangeScanProgress,
    *,
    successful_clock: float,
) -> bool:
    """Stage page progress and report whether the final scan completed.

    Args:
        connection: Facade-owned live transaction connection.
        settings: Validated source scan limit.
        obligation_id: Exact retained open range.
        progress: Inclusive committed bound and provider-page completion.
        successful_clock: Current successful-query clock.

    Returns:
        `True` only when a complete scan begun after the deadline finished.

    Raises:
        ValueError: The range is not open or progress is outside its current
            inclusive bounds.
        TypeError: The page completion flag is not a Boolean.
    """
    row = connection.execute(
        """
        SELECT high_id, visibility_deadline, scan_started_at,
               scan_next_id, scan_pages, degraded, scan_cycle_pages
        FROM discovery_ranges
        WHERE obligation_id = ? AND range_state = 'open'
        """,
        (obligation_id,),
    ).fetchone()
    if row is None:
        raise ValueError('unknown open discovery range')
    high_id = int(row[0])
    deadline = float(row[1])
    scan_started = None if row[2] is None else float(row[2])
    scan_next = int(row[3])
    scanned_through = progress.scanned_through
    if scanned_through < scan_next - 1 or scanned_through > high_id:
        raise ValueError('range scan page does not advance valid inclusive bounds')
    if type(progress.query_page_complete) is not bool:
        raise TypeError('query page completion flag must be a Boolean')
    next_id = scanned_through + 1
    complete = next_id > high_id
    page_complete = int(progress.query_page_complete)
    pages = int(row[4]) + page_complete
    cycle_pages = int(row[6]) + page_complete
    hit_cap = (
        not complete
        and progress.query_page_complete
        and cycle_pages >= settings.max_range_query_pages
    )
    degraded = False if complete else bool(row[5]) or hit_cap
    connection.execute(
        """
        UPDATE discovery_ranges
        SET scan_next_id = ?, scan_pages = ?, degraded = ?,
            scan_cycle_pages = ?
        WHERE obligation_id = ?
        """,
        (next_id, pages, int(degraded), cycle_pages, obligation_id),
    )
    if hit_cap:
        set_condition(connection, 'range-query-page-cap')
    elif complete:
        _clear_range_query_page_condition(connection)
    return (
        complete
        and scan_started is not None
        and scan_started >= deadline
        and successful_clock >= deadline
    )


def snapshot(
    connection: sqlite3.Connection,
    settings: SourceSettings,
    **options: Unpack[_SnapshotOptions],
) -> SourceSnapshot:
    """Compose the immutable source projection from domain-owned evidence.

    Args:
        connection: Facade-owned connection.
        settings: Validated source identity and limits.
        options: Complete obligation-owned evidence projection.

    Returns:
        Complete immutable body-free source snapshot.
    """
    row = meta(connection)
    stored_fingerprint = str(row[5])
    initialized = bool(row[2])
    condition = None if row[8] is None else str(row[8])
    try:
        condition_status = None if condition is None else _SOURCE_CONDITION_STATUSES[condition]
    except KeyError:
        raise RuntimeError(f'unknown persisted source condition: {condition}') from None
    if stored_fingerprint != settings.configured_fingerprint:
        status: _SourceStatus = 'stream-identity-changed'
        conditions = ('stream-identity-changed',)
    elif not initialized:
        status = 'uninitialized'
        conditions = ()
    elif condition_status is not None:
        status = condition_status
        conditions = (condition_status,)
    else:
        status = 'running'
        conditions = ()
    return SourceSnapshot(
        site=str(row[0]),
        source=str(row[1]),
        initialized=initialized,
        frontier=int(row[3]),
        successful_clock=float(row[4]),
        stream_fingerprint=stored_fingerprint,
        stream_incarnation=None if row[6] is None else str(row[6]),
        source_generation=str(row[7]),
        status=status,
        conditions=conditions,
        ranges=ranges(connection),
        pending_obligations=options['pending_obligations'],
        replay_markers=options['replay_markers'],
        adoption_count=options['adoption_count'],
    )


def stage_open_range(
    connection: sqlite3.Connection,
    settings: SourceSettings,
    **options: Unpack[_RangeStageOptions],
) -> RangeMutation:
    """Stage one inclusive range insertion or overlap merge.

    Args:
        connection: Facade-owned live transaction connection.
        settings: Validated source limits.
        options: Inclusive bounds, observation clock, and non-reusable token
            source.

    Returns:
        Exact range revision staged in the current transaction.

    Raises:
        SourceCapacityError: A new nonoverlapping range cannot fit.
    """
    low_id = options['low_id']
    high_id = options['high_id']
    successful_clock = options['successful_clock']
    overlapping = connection.execute(
        """
        SELECT obligation_id, low_id, high_id, opened_successful_at,
               visibility_deadline, source_revision
        FROM discovery_ranges
        WHERE range_state = 'open'
          AND low_id <= ?
          AND high_id >= ?
        ORDER BY low_id, high_id, obligation_id
        """,
        (high_id, low_id),
    ).fetchall()
    if overlapping:
        obligation_id = str(overlapping[0][0])
        merged_ids = tuple(str(row[0]) for row in overlapping)
        merged_low = min(low_id, *(int(row[1]) for row in overlapping))
        merged_high = max(high_id, *(int(row[2]) for row in overlapping))
        opened_at = min(successful_clock, *(float(row[3]) for row in overlapping))
        deadline = max(
            successful_clock + settings.visibility_overlap_secs,
            *(float(row[4]) for row in overlapping),
        )
        revision = max(int(row[5]) for row in overlapping) + 1
        existing_eligibility = tuple(
            (
                str(item[0]),
                int(item[1]),
                float(item[2]),
            )
            for merged_id in merged_ids
            for item in connection.execute(
                """
                SELECT group_name, group_generation, eligibility_point
                FROM range_groups
                WHERE obligation_id = ?
                """,
                (merged_id,),
            )
        )
        late_ids = {
            int(item[0])
            for merged_id in merged_ids
            for item in connection.execute(
                'SELECT event_id FROM source_late_ids WHERE obligation_id = ?',
                (merged_id,),
            )
        }
        for merged_id in merged_ids[1:]:
            connection.execute(
                'DELETE FROM discovery_ranges WHERE obligation_id = ?',
                (merged_id,),
            )
        connection.execute(
            """
            UPDATE discovery_ranges
            SET low_id = ?, high_id = ?, opened_successful_at = ?,
                visibility_deadline = ?, source_revision = ?,
                scan_started_at = NULL,
                scan_next_id = ?, scan_pages = 0, degraded = 0,
                scan_cycle_pages = 0
            WHERE obligation_id = ?
            """,
            (
                merged_low,
                merged_high,
                opened_at,
                deadline,
                revision,
                merged_low,
                obligation_id,
            ),
        )
        connection.executemany(
            'INSERT OR IGNORE INTO source_late_ids VALUES (?, ?)',
            ((obligation_id, event_id) for event_id in late_ids),
        )
    else:
        count = int(
            connection.execute(
                "SELECT COUNT(*) FROM discovery_ranges WHERE range_state = 'open'"
            ).fetchone()[0]
        )
        if count >= settings.max_open_ranges:
            raise SourceCapacityError('range-capacity-exhausted', code='range-capacity-exhausted')
        obligation_id = f'range-{low_id}-{high_id}'
        merged_ids = (obligation_id,)
        merged_low = low_id
        merged_high = high_id
        opened_at = successful_clock
        deadline = successful_clock + settings.visibility_overlap_secs
        revision = 1
        existing_eligibility = ()
        connection.execute(
            """
            INSERT INTO discovery_ranges (
                obligation_id, low_id, high_id, opened_successful_at,
                visibility_deadline, source_revision,
                close_generation, range_state, scan_started_at,
                scan_next_id, scan_pages, degraded
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 'open', NULL, ?, 0, 0)
            """,
            (
                obligation_id,
                low_id,
                high_id,
                opened_at,
                deadline,
                revision,
                low_id,
            ),
        )
    connection.execute(BUMP_RANGE_REVISION)
    return RangeMutation(
        obligation_id=obligation_id,
        merged_ids=merged_ids,
        low_id=merged_low,
        high_id=merged_high,
        opened_successful_at=opened_at,
        visibility_deadline=deadline,
        revision=revision,
        existing_eligibility=existing_eligibility,
    )


def uncovered_intervals(
    connection: sqlite3.Connection,
    low_id: int,
    high_id: int,
) -> tuple[tuple[int, int], ...]:
    """Subtract retained open ranges from one inclusive missing interval.

    Args:
        connection: Facade-owned connection.
        low_id: Inclusive candidate lower bound.
        high_id: Inclusive candidate upper bound.

    Returns:
        Ordered nonoverlapping intervals not already retained.
    """
    if low_id > high_id:
        return ()
    covered = connection.execute(
        """
        SELECT low_id, high_id
        FROM discovery_ranges
        WHERE range_state = 'open'
          AND low_id <= ?
          AND high_id >= ?
        ORDER BY low_id, high_id
        """,
        (high_id, low_id),
    ).fetchall()
    result: list[tuple[int, int]] = []
    cursor = low_id
    for row in covered:
        covered_low = max(low_id, int(row[0]))
        covered_high = min(high_id, int(row[1]))
        if covered_low > cursor:
            result.append((cursor, covered_low - 1))
        cursor = max(cursor, covered_high + 1)
        if cursor > high_id:
            break
    if cursor <= high_id:
        result.append((cursor, high_id))
    return tuple(result)


def validate_successful_clock(
    connection: sqlite3.Connection,
    successful_clock: float,
) -> None:
    """Validate a monotonic successful-query clock.

    Args:
        connection: Facade-owned connection.
        successful_clock: Finite nonnegative clock at or beyond the durable
            checkpoint.

    Raises:
        ValueError: The clock is invalid or moves backward.
    """
    if not isfinite(successful_clock) or successful_clock < 0:
        raise ValueError('successful clock must be finite and nonnegative')
    current = float(connection.execute('SELECT successful_clock FROM source_meta').fetchone()[0])
    if successful_clock < current:
        raise ValueError('successful clock cannot move backward')


def record_late_ids(
    connection: sqlite3.Connection,
    settings: SourceSettings,
    obligation_id: str,
    event_ids: Iterable[int],
    *,
    successful_clock: float,
) -> None:
    """Stage exact late-id evidence and successful-clock progress.

    Args:
        connection: Facade-owned live transaction connection.
        settings: Validated source limits.
        obligation_id: Exact retained owning range.
        event_ids: Exact positive integers within the range's inclusive bounds.
        successful_clock: Finite nonnegative successful-query clock at or
            beyond the durable checkpoint.

    Raises:
        ValueError: An id is invalid, the range is unknown, an id lies outside
            its range, or the successful clock is invalid or moves backward.
        SourceCapacityError: The retained late-id set cannot fit.
    """
    ids = tuple(event_ids)
    if any(type(event_id) is not int or event_id <= 0 for event_id in ids):
        raise ValueError('late ids must be positive integers')
    row = connection.execute(
        'SELECT low_id, high_id FROM discovery_ranges WHERE obligation_id = ?',
        (obligation_id,),
    ).fetchone()
    if row is None:
        raise ValueError('unknown discovery range')
    low_id, high_id = int(row[0]), int(row[1])
    if any(event_id < low_id or event_id > high_id for event_id in ids):
        raise ValueError('late id lies outside its discovery range')
    retained_count = int(connection.execute('SELECT COUNT(*) FROM source_late_ids').fetchone()[0])
    incoming_count = sum(
        connection.execute(
            'SELECT 1 FROM source_late_ids WHERE obligation_id = ? AND event_id = ?',
            (obligation_id, event_id),
        ).fetchone()
        is None
        for event_id in set(ids)
    )
    if retained_count + incoming_count > settings.max_source_late_ids:
        raise SourceCapacityError('late-capacity-exhausted', code='late-capacity-exhausted')
    validate_successful_clock(connection, successful_clock)
    connection.executemany(
        'INSERT OR IGNORE INTO source_late_ids VALUES (?, ?)',
        ((obligation_id, event_id) for event_id in ids),
    )
    if incoming_count:
        connection.execute(
            (
                'UPDATE discovery_ranges SET source_revision = '
                'source_revision + 1 WHERE obligation_id = ?'
            ),
            (obligation_id,),
        )
        connection.execute(BUMP_RANGE_REVISION)
    connection.execute(
        'UPDATE source_meta SET successful_clock = ?',
        (successful_clock,),
    )


def record_successful_query(
    connection: sqlite3.Connection,
    successful_clock: float,
) -> None:
    """Stage a monotonic successful-query clock checkpoint.

    Args:
        connection: Facade-owned live transaction connection.
        successful_clock: Finite nonnegative clock at or beyond the durable
            checkpoint.

    Raises:
        ValueError: The clock is invalid or moves backward.
    """
    validate_successful_clock(connection, successful_clock)
    connection.execute(
        'UPDATE source_meta SET successful_clock = ? WHERE singleton = 1',
        (successful_clock,),
    )


def stage_close_range(
    connection: sqlite3.Connection,
    obligation_id: str,
    *,
    successful_clock: float,
) -> bool:
    """Close a retained range atomically with its source and close revisions.

    Args:
        connection: Facade-owned live transaction connection.
        obligation_id: Exact retained range identity.
        successful_clock: Source successful-query clock. When newly closing a
            range, it must be finite, nonnegative, and no earlier than the
            stored source clock.

    Returns:
        Whether this transaction first closed the range.

    Raises:
        ValueError: The range is unknown, or a newly closed range is supplied
            an invalid or regressing successful clock.
    """
    if _range_is_closed(connection, obligation_id):
        return False
    validate_successful_clock(connection, successful_clock)
    connection.execute(
        """
        UPDATE discovery_ranges
        SET source_revision = source_revision + 1,
            close_generation = close_generation + 1, range_state = 'closed'
        WHERE obligation_id = ?
        """,
        (obligation_id,),
    )
    connection.execute(BUMP_RANGE_REVISION)
    record_successful_query(connection, successful_clock)
    return True
