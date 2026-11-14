"""Cursor, callback, manifest, and required-event transitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._group_errors import (
    InvalidTransitionError,
    StrictObligationBlockedError,
    TerminalLateEventError,
)
from ._group_store import (
    adjust_operational_late_id_count,
    bump_revision,
    check_revision,
    clear_failure,
    ensure_delivery_ready,
    group_conditions,
    snapshot,
)
from ._group_types import (
    GroupSnapshot,
    digest,
    manifest_from_json,
)

if TYPE_CHECKING:
    import sqlite3

    from ._plugins import CallbackManifest


@dataclass(frozen=True)
class _CompletionState:
    """Hold freshly read completion checks only within the caller's transaction."""

    in_flight_event_id: int | None
    in_flight_obligation_id: str | None
    recovery_blocked: bool


@dataclass(frozen=True)
class CallbackFailureRequest:
    """Hold the callback ordinal and bounded failure text."""

    ordinal: int
    failure_text: str
    max_failure_text_bytes: int


@dataclass(frozen=True)
class EventStartRequest:
    """Hold one exact event start and manifest policy."""

    event_id: int
    manifest: CallbackManifest
    max_manifest_bytes: int
    obligation_id: str | None


def _earliest_unresolved_obligation(
    connection: sqlite3.Connection,
    group_name: str,
) -> tuple[str, int, int] | None:
    """Return the earliest unresolved obligation identity and bounds."""
    row = connection.execute(
        """
        SELECT obligation_id, low_id, high_id
        FROM range_groups JOIN discovery_ranges USING (obligation_id)
        WHERE group_name = ? AND obligation_state = 'pending'
          AND group_generation = (SELECT group_generation FROM groups WHERE group_name = ?)
        ORDER BY low_id, high_id, obligation_id
        LIMIT 1
        """,
        (group_name, group_name),
    ).fetchone()
    if row is None:
        return None
    return str(row[0]), int(row[1]), int(row[2])


def _insert_pending_marks(
    connection: sqlite3.Connection,
    group_name: str,
    event_id: int,
    manifest: CallbackManifest,
) -> None:
    """Insert fresh callback marks inside the caller's live transaction."""
    connection.executemany(
        """
        INSERT INTO callback_marks (
            group_name, event_id, ordinal, plugin_id, callback_id, outcome, delivery_attempt
        ) VALUES (?, ?, ?, ?, ?, 'pending', 0)
        """,
        (
            (
                group_name,
                event_id,
                callback.ordinal,
                callback.plugin_id,
                callback.callback_id,
            )
            for callback in manifest.callbacks
        ),
    )


def _read_completion_state(connection: sqlite3.Connection, group_name: str) -> _CompletionState:
    """Read and validate transaction-local state without projecting ordinary health."""
    row = connection.execute(
        'SELECT in_flight_event_id, in_flight_obligation_id, manifest_json, '
        'failure_code, required_event_id, shape_mismatch '
        'FROM group_state WHERE group_name = ?',
        (group_name,),
    ).fetchone()
    if row is None:
        raise RuntimeError('group state is not initialized')
    if row[0] is None or row[1] is not None or row[3] is not None or row[4] is not None or row[5]:
        # Keep exceptional and late completion on the existing snapshot path.
        current = snapshot(connection, group_name)
        return _CompletionState(
            current.in_flight_event_id,
            current.in_flight_obligation_id,
            current.failure_code is not None
            or current.required_event_id is not None
            or 'shape-mismatch' in current.conditions,
        )
    if row[2] is not None:
        manifest_from_json(str(row[2]))
    return _CompletionState(int(row[0]), None, recovery_blocked=False)


def _validated_manifest_json(
    manifest: CallbackManifest,
    max_manifest_bytes: int,
) -> str:
    """Return one nonempty, consecutive manifest within its byte limit."""
    manifest_json = manifest.to_json()
    if not manifest.callbacks:
        raise InvalidTransitionError('matching events require at least one callback')
    if len(manifest_json.encode()) > max_manifest_bytes:
        raise InvalidTransitionError('callback manifest exceeds max_manifest_bytes')
    if any(callback.ordinal != ordinal for ordinal, callback in enumerate(manifest.callbacks)):
        raise InvalidTransitionError('callback manifest ordinals must be consecutive')
    return manifest_json


def _write_terminal_event(connection: sqlite3.Connection, group_name: str, event_id: int) -> None:
    """Advance and clear an event after the caller validates terminal completion."""
    connection.execute(
        """
        UPDATE group_state
        SET cursor = MAX(cursor, ?),
            last_delivered_id = MAX(last_delivered_id, ?),
            in_flight_event_id = NULL,
            in_flight_obligation_id = NULL,
            manifest_json = NULL
        WHERE group_name = ?
        """,
        (event_id, event_id, group_name),
    )
    connection.execute(
        'DELETE FROM callback_marks WHERE group_name = ? AND event_id = ?',
        (group_name, event_id),
    )


def callback_failed(
    connection: sqlite3.Connection,
    group_name: str,
    request: CallbackFailureRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Persist one bounded callback failure inside a live transaction.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: In-flight ordinal, redacted failure text, and UTF-8 retention
            limit.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot containing the callback failure evidence.

    Raises:
        InvalidTransitionError: The callback was not invoked and pending.
        RevisionConflictError: The expected revision differs.
        sqlite3.Error: Reading or updating durable group state fails.
        ValueError: The stored callback manifest is invalid.
    """
    raw = request.failure_text.encode()
    retained = raw[: request.max_failure_text_bytes].decode(errors='ignore')
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    if current.in_flight_event_id is None:
        raise InvalidTransitionError('no event is in flight')
    mark = connection.execute(
        """
        SELECT outcome, delivery_attempt
        FROM callback_marks
        WHERE group_name = ? AND event_id = ? AND ordinal = ?
        """,
        (group_name, current.in_flight_event_id, request.ordinal),
    ).fetchone()
    if mark is None or str(mark[0]) != 'pending' or int(mark[1]) <= 0:
        raise InvalidTransitionError('callback was not invoked and pending')
    connection.execute(
        """
        UPDATE group_state
        SET failure_code = 'callback-failed',
            failure_text = ?,
            failure_byte_count = ?,
            failure_digest = ?,
            failure_ordinal = ?
        WHERE group_name = ?
        """,
        (retained, len(raw), digest(raw), request.ordinal, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def require_event(
    connection: sqlite3.Connection,
    group_name: str,
    event_id: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Persist an exact required event independently of a manifest.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        event_id: Exact required event id.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot halted on the required event.

    Raises:
        InvalidTransitionError: Another required event is active or the event
            does not match the in-flight event.
        RevisionConflictError: The expected revision differs.
        sqlite3.Error: Reading or updating durable group state fails.
        ValueError: The stored callback manifest is invalid.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    if current.in_flight_event_id is not None and current.in_flight_event_id != event_id:
        raise InvalidTransitionError('required event does not match the in-flight event')
    if current.required_event_id is not None:
        raise InvalidTransitionError('another required event remains active')
    connection.execute(
        """
        UPDATE group_state
        SET required_event_id = ?, retry_event_id = NULL
        WHERE group_name = ?
        """,
        (event_id, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def restart_event_manifest(
    connection: sqlite3.Connection,
    group_name: str,
    manifest: CallbackManifest,
    expected_revision: int,
    max_manifest_bytes: int,
) -> GroupSnapshot:
    """Restart one in-flight event under the complete current manifest.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        manifest: Complete current callback manifest.
        expected_revision: Required compare-and-swap revision.
        max_manifest_bytes: Maximum encoded manifest size.

    Returns:
        Immutable snapshot with fresh pending callback marks and cleared
        callback-failure and shape-mismatch evidence.

    Raises:
        InvalidTransitionError: The manifest is empty, oversized, or has
            nonconsecutive ordinals, or no event is in flight.
        RevisionConflictError: The expected revision differs.
        sqlite3.Error: Reading or updating durable group state fails.
        ValueError: The stored callback manifest is invalid.
    """
    manifest_json = _validated_manifest_json(manifest, max_manifest_bytes)
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    if current.in_flight_event_id is None:
        raise InvalidTransitionError('no event is in flight')
    connection.execute(
        'DELETE FROM callback_marks WHERE group_name = ? AND event_id = ?',
        (group_name, current.in_flight_event_id),
    )
    _insert_pending_marks(connection, group_name, current.in_flight_event_id, manifest)
    connection.execute(
        """
        UPDATE group_state
        SET manifest_json = ?,
            shape_mismatch = 0
        WHERE group_name = ?
        """,
        (manifest_json, group_name),
    )
    clear_failure(connection, group_name)
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def restore_required_event(
    connection: sqlite3.Connection,
    group_name: str,
    event_id: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Clear evidence only after the exact authorized event is restored.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        event_id: Exact restored event id.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot with the required-event evidence cleared.

    Raises:
        InvalidTransitionError: The event does not match the authorized
            refetch.
        RevisionConflictError: The expected revision differs.
        sqlite3.Error: Reading or updating durable group state fails.
        ValueError: The stored callback manifest is invalid.
    """
    check_revision(connection, group_name, expected_revision)
    row = connection.execute(
        'SELECT required_event_id, retry_event_id FROM group_state WHERE group_name = ?',
        (group_name,),
    ).fetchone()
    if (
        row is None
        or row[0] is None
        or int(row[0]) != event_id
        or row[1] is None
        or int(row[1]) != event_id
    ):
        raise InvalidTransitionError('event does not match the authorized required refetch')
    connection.execute(
        """
        UPDATE group_state
        SET required_event_id = NULL, retry_event_id = NULL
        WHERE group_name = ?
        """,
        (group_name,),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def strict_obligation_fence(
    connection: sqlite3.Connection,
    group_name: str,
) -> tuple[str, int, int] | None:
    """Return the earliest unresolved obligation for a strict group.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.

    Returns:
        Earliest obligation identity and bounds, or `None` outside a strict
        fence.

    Raises:
        sqlite3.Error: Reading the strict obligation fence fails.
    """
    ordering = str(
        connection.execute(
            'SELECT ordering_mode FROM groups WHERE group_name = ?', (group_name,)
        ).fetchone()[0]
    )
    if ordering != 'strict':
        return None
    return _earliest_unresolved_obligation(connection, group_name)


def _check_strict_obligation_fence(
    connection: sqlite3.Connection,
    group_name: str,
    event_id: int,
    obligation_id: str | None,
) -> None:
    """Reject an event outside the live strict obligation fence."""
    fence = strict_obligation_fence(connection, group_name)
    if fence is None:
        return
    earliest_obligation_id, low_id, _ = fence
    if obligation_id is None:
        if event_id >= low_id:
            raise StrictObligationBlockedError(
                'strict delivery is blocked by an unresolved obligation'
            )
    elif obligation_id != earliest_obligation_id:
        raise InvalidTransitionError('strict delivery is blocked by an earlier obligation')


def _finish_ordinary_event(
    connection: sqlite3.Connection, group_name: str, current: _CompletionState
) -> None:
    """Finish the ordinary event using only this transaction's completion read."""
    if current.in_flight_event_id is None:
        raise InvalidTransitionError('no event is in flight')
    _check_strict_obligation_fence(connection, group_name, current.in_flight_event_id, None)
    _write_terminal_event(connection, group_name, current.in_flight_event_id)


def advance_irrelevant(
    connection: sqlite3.Connection,
    group_name: str,
    high_id: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Advance across one consecutive irrelevant range inside a live transaction.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        high_id: Inclusive greatest irrelevant event id.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot advanced through `high_id`.

    Raises:
        InvalidTransitionError: Delivery is blocked, `high_id` does not advance
            the cursor, or strict progress reaches an unresolved obligation.
        RevisionConflictError: The expected revision differs.
        sqlite3.Error: Reading or updating durable group state fails.
        ValueError: The stored callback manifest is invalid.
    """
    check_revision(connection, group_name, expected_revision)
    ensure_delivery_ready(connection, group_name)
    cursor = int(
        connection.execute(
            'SELECT cursor FROM group_state WHERE group_name = ?', (group_name,)
        ).fetchone()[0]
    )
    if high_id <= cursor:
        raise InvalidTransitionError('irrelevant range must advance the cursor')
    fence = strict_obligation_fence(connection, group_name)
    if fence is not None and fence[1] <= high_id:
        raise StrictObligationBlockedError(
            'strict progress is blocked by an unresolved obligation'
        )
    connection.execute(
        """
        UPDATE group_state
        SET cursor = ?, last_delivered_id = MAX(last_delivered_id, ?)
        WHERE group_name = ?
        """,
        (high_id, high_id, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def advance_irrelevant_prefix(
    connection: sqlite3.Connection,
    group_name: str,
    high_id: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Advance the live irrelevant prefix below the transaction's current strict fence.

    Args:
        connection: Live configured-level transaction connection.
        group_name: Exact group whose durable rows are accessed.
        high_id: Inclusive greatest buffered irrelevant event id.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Group snapshot at the actual safe cursor. If no prefix is safe, the
        snapshot is unchanged without a revision bump.

    Raises:
        InvalidTransitionError: A recovery condition or in-flight event
            prevents delivery progress.
        RevisionConflictError: The expected revision differs.
        sqlite3.Error: Reading or updating durable group state fails.
        ValueError: The stored callback manifest is invalid.
    """
    check_revision(connection, group_name, expected_revision)
    ensure_delivery_ready(connection, group_name)
    current = snapshot(connection, group_name)
    fence = strict_obligation_fence(connection, group_name)
    safe_high = high_id if fence is None else min(high_id, fence[1] - 1)
    if safe_high <= current.cursor:
        return current
    return advance_irrelevant(connection, group_name, safe_high, expected_revision)


def begin_callback_step(
    connection: sqlite3.Connection,
    group_name: str,
    ordinal: int,
    expected_revision: int,
) -> int:
    """Record one callback attempt and return its advanced revision.

    Args:
        connection: Live group-state transaction connection; the caller owns
            commit or rollback.
        group_name: Exact group whose durable rows are accessed.
        ordinal: Pending callback ordinal for the in-flight event.
        expected_revision: Required compare-and-swap revision.

    Returns:
        `expected_revision + 1` after incrementing the callback attempt count.

    Raises:
        InvalidTransitionError: Delivery is blocked, no event is in flight, the
            callback is not pending, an earlier callback is pending, or the
            strict obligation fence blocks delivery.
        RevisionConflictError: The expected revision differs.
        sqlite3.Error: Reading or updating durable group state fails.
    """
    check_revision(connection, group_name, expected_revision)
    if group_conditions(connection, group_name):
        raise InvalidTransitionError('callback delivery is blocked')
    event_id, obligation_id = connection.execute(
        'SELECT in_flight_event_id, in_flight_obligation_id FROM group_state WHERE group_name = ?',
        (group_name,),
    ).fetchone()
    if event_id is None:
        raise InvalidTransitionError('no event is in flight')
    mark = connection.execute(
        """
        SELECT outcome
        FROM callback_marks
        WHERE group_name = ? AND event_id = ? AND ordinal = ?
        """,
        (group_name, event_id, ordinal),
    ).fetchone()
    if mark is None or str(mark[0]) != 'pending':
        raise InvalidTransitionError('callback is not pending')
    if (
        connection.execute(
            """
            SELECT 1 FROM callback_marks
            WHERE group_name = ? AND event_id = ? AND ordinal < ? AND outcome = 'pending'
            LIMIT 1
            """,
            (group_name, event_id, ordinal),
        ).fetchone()
        is not None
    ):
        raise InvalidTransitionError('an earlier callback remains pending')
    _check_strict_obligation_fence(
        connection,
        group_name,
        event_id,
        obligation_id,
    )
    connection.execute(
        """
        UPDATE callback_marks
        SET delivery_attempt = delivery_attempt + 1
        WHERE group_name = ? AND event_id = ? AND ordinal = ?
        """,
        (group_name, event_id, ordinal),
    )
    bump_revision(connection, group_name, expected_revision)
    return expected_revision + 1


def finish_terminal_event(connection: sqlite3.Connection, group_name: str) -> None:
    """Finish the in-flight event inside the caller's transaction.

    Args:
        connection: Live group-state connection after its final callback mark
            becomes terminal. The caller owns commit, rollback, revision,
            failure clearing, and any operator audit.
        group_name: Exact group whose durable rows are accessed.

    Raises:
        InvalidTransitionError: No event is in flight or the strict obligation
            fence blocks terminal completion.
        sqlite3.Error: Reading or updating durable group state fails.
        RuntimeError: Group state is not initialized.
        ValueError: The stored callback manifest is invalid.
    """
    current = snapshot(connection, group_name)
    if current.in_flight_event_id is None:
        raise InvalidTransitionError('no event is in flight')
    _check_strict_obligation_fence(
        connection,
        group_name,
        current.in_flight_event_id,
        current.in_flight_obligation_id,
    )
    if current.in_flight_obligation_id is not None:
        changed = connection.execute(
            """
            UPDATE late_ids
            SET terminal_state = 'done'
            WHERE group_name = ? AND obligation_id = ? AND event_id = ?
              AND terminal_state = 'pending'
            """,
            (group_name, current.in_flight_obligation_id, current.in_flight_event_id),
        ).rowcount
        if changed:
            adjust_operational_late_id_count(
                connection,
                group_name,
                -1,
            )
    _write_terminal_event(connection, group_name, current.in_flight_event_id)


def callback_done_step(
    connection: sqlite3.Connection,
    group_name: str,
    ordinal: int,
    expected_revision: int,
) -> int:
    """Record one callback success and return its advanced transaction-local revision.

    Args:
        connection: Live group-state transaction connection; the caller owns
            commit or rollback, including partial writes on rejection.
        group_name: Exact group whose durable rows are accessed.
        ordinal: In-flight callback ordinal.
        expected_revision: Required compare-and-swap revision.

    Returns:
        `expected_revision + 1` after recording the callback and finishing its
        event when no pending callback remains. Ordinary completion builds no
        full snapshot; the caller projects one at its transaction boundary.

    Raises:
        InvalidTransitionError: Recovery evidence or callback state rejects
            completion, or a strict fence blocks the final mark.
        RevisionConflictError: The expected revision differs.
        ValueError: The stored callback manifest is invalid.
        sqlite3.Error: Reading or updating durable group state fails.
    """
    check_revision(connection, group_name, expected_revision)
    current = _read_completion_state(connection, group_name)
    if current.in_flight_event_id is None:
        raise InvalidTransitionError('no event is in flight')
    if current.recovery_blocked:
        raise InvalidTransitionError('callback completion is blocked by recovery evidence')
    mark = connection.execute(
        """
        SELECT outcome, delivery_attempt
        FROM callback_marks
        WHERE group_name = ? AND event_id = ? AND ordinal = ?
        """,
        (group_name, current.in_flight_event_id, ordinal),
    ).fetchone()
    if mark is None or str(mark[0]) != 'pending' or int(mark[1]) <= 0:
        raise InvalidTransitionError('callback was not invoked and pending')
    connection.execute(
        (
            "UPDATE callback_marks SET outcome = 'done' WHERE group_name "
            '= ? AND event_id = ? AND ordinal = ?'
        ),
        (group_name, current.in_flight_event_id, ordinal),
    )
    if (
        connection.execute(
            """
            SELECT 1 FROM callback_marks
            WHERE group_name = ? AND event_id = ? AND outcome = 'pending'
            LIMIT 1
            """,
            (group_name, current.in_flight_event_id),
        ).fetchone()
        is None
    ):
        if current.in_flight_obligation_id is None:
            _finish_ordinary_event(connection, group_name, current)
        else:
            finish_terminal_event(connection, group_name)
        clear_failure(connection, group_name)
    bump_revision(connection, group_name, expected_revision)
    return expected_revision + 1


def callback_done(
    connection: sqlite3.Connection,
    group_name: str,
    ordinal: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Record one callback success and finish a final mark in the live transaction.

    Args:
        connection: Live group-state transaction connection; the caller commits
            or rolls back.
        group_name: Exact group whose durable rows are accessed.
        ordinal: In-flight callback ordinal.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot with the callback done and, for the final mark, the
        event completed.

    Raises:
        InvalidTransitionError: Recovery evidence blocks completion, the
            callback was not invoked and pending, or a strict obligation blocks
            the final mark.
        RevisionConflictError: The expected revision differs.
        ValueError: The stored callback manifest is invalid.
        sqlite3.Error: Reading or updating durable group state fails.
    """
    callback_done_step(connection, group_name, ordinal, expected_revision)
    return snapshot(connection, group_name)


def start_event_step(
    connection: sqlite3.Connection,
    group_name: str,
    request: EventStartRequest,
    expected_revision: int,
) -> int:
    """Record one event and its manifest and return the advanced revision.

    Args:
        connection: Live group-state transaction connection; the caller owns
            commit or rollback.
        group_name: Exact group whose durable rows are accessed.
        request: Event identity, complete callback manifest, manifest byte
            limit, and optional late-event obligation reservation.
        expected_revision: Required compare-and-swap revision.

    Returns:
        `expected_revision + 1` after recording the in-flight event and fresh
        pending callback marks.

    Raises:
        InvalidTransitionError: The manifest is empty, oversized, or has
            nonconsecutive ordinals; delivery is blocked; an ordinary event
            does not advance the cursor; a late event lacks an exact pending
            reservation; or the strict obligation fence blocks delivery.
        RevisionConflictError: The expected revision differs.
        sqlite3.Error: Reading or updating durable group state fails.
    """
    manifest_json = _validated_manifest_json(request.manifest, request.max_manifest_bytes)
    check_revision(connection, group_name, expected_revision)
    ensure_delivery_ready(connection, group_name)
    cursor = int(
        connection.execute(
            'SELECT cursor FROM group_state WHERE group_name = ?', (group_name,)
        ).fetchone()[0]
    )
    if request.obligation_id is None and request.event_id <= cursor:
        raise InvalidTransitionError('ordinary event does not advance the cursor')
    if request.obligation_id is not None:
        reservation = connection.execute(
            """
            SELECT terminal_state
            FROM late_ids
            WHERE group_name = ? AND obligation_id = ? AND event_id = ?
            """,
            (group_name, request.obligation_id, request.event_id),
        ).fetchone()
        if reservation is None or str(reservation[0]) != 'pending':
            raise TerminalLateEventError('late event does not have an exact reservation')
    _check_strict_obligation_fence(
        connection,
        group_name,
        request.event_id,
        request.obligation_id,
    )
    connection.execute(
        """
        UPDATE group_state
        SET in_flight_event_id = ?, in_flight_obligation_id = ?, manifest_json = ?
        WHERE group_name = ?
        """,
        (request.event_id, request.obligation_id, manifest_json, group_name),
    )
    _insert_pending_marks(connection, group_name, request.event_id, request.manifest)
    bump_revision(connection, group_name, expected_revision)
    return expected_revision + 1


def verify_manifest(
    connection: sqlite3.Connection,
    group_name: str,
    manifest: CallbackManifest,
    expected_revision: int,
) -> GroupSnapshot:
    """Validate current callback shape without reconciling completed marks.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        manifest: Complete current callback manifest.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Current snapshot, updated with callback-shape mismatch state when
        required.

    Raises:
        RevisionConflictError: The expected revision differs.
        sqlite3.Error: Reading or updating durable group state fails.
        ValueError: The stored callback manifest is invalid.
    """
    check_revision(connection, group_name, expected_revision)
    row = connection.execute(
        (
            'SELECT in_flight_event_id, manifest_json, shape_mismatch '
            'FROM group_state WHERE group_name = ?'
        ),
        (group_name,),
    ).fetchone()
    if row is None:
        raise RuntimeError('group state is not initialized')
    if row[0] is not None:
        desired = int(str(row[1]) != manifest.to_json())
        if int(row[2]) != desired:
            connection.execute(
                'UPDATE group_state SET shape_mismatch = ? WHERE group_name = ?',
                (desired, group_name),
            )
            bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)
