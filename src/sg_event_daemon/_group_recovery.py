"""Epoch, park, disposition, audit, and live-join transitions."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal, cast

from . import _group_delivery as delivery
from . import _source_obligations as source_obligations
from ._group_errors import InvalidTransitionError
from ._group_store import (
    bump_revision,
    check_revision,
    clear_failure,
    refresh_operational_late_id_count,
    snapshot,
)
from ._group_types import (
    AuditIdentity,
    AuditRecord,
    ConditionalParkDirective,
    ConditionalParkOutcome,
    ConditionalParkResult,
    GroupSnapshot,
    LiveJoinIdentity,
    canonical_bytes,
    digest,
    snapshot_from_json,
    snapshot_to_json,
)
from ._source_schema import BUMP_INDEX_REVISION, BUMP_RANGE_REVISION

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Mapping

    from ._fanout import StrictOrderingControl
    from ._plugins import CallbackManifest


@dataclass(frozen=True)
class BootstrapRequest:
    """Hold exact captured positions and immutable policy for one generation."""

    generation: int
    cursor: int
    delivery_floor: int
    last_delivered_id: int
    ordering: Literal['eager', 'strict']
    site_server_url: str
    verified_incarnation: str
    holdback_secs: float


@dataclass(frozen=True)
class DispositionTarget:
    """Hold configured group policy and expected source authority for a new command."""

    site_server_url: str
    ordering: Literal['eager', 'strict']
    holdback_secs: float
    stream_fingerprint: str
    source_generation: str
    verified_incarnation: str


@dataclass(frozen=True)
class DispositionRequest:
    """Hold one revision-fenced operator request with observation-free retry identity."""

    command: Literal['replay', 'reset-to-latest']
    group: str
    disposition_id: str
    reserved_generation: int
    expected_revision: int
    from_event_id: int | None
    acknowledge_truncation: bool
    reason: str
    identity: AuditIdentity
    wall_clock_time: float
    operator_label: str | None = None


@dataclass(frozen=True)
class DispositionCommand:
    """Hold validated replay or reset values before a new generation is fenced.

    Instances are built by `disposition_command`. Audit identity belongs to the
    executing process; the operator label retains the trusted caller
    attribution.
    Use `request` after reserving a generation and observing the group
    revision.

    Attributes:
        command: Canonical replay or reset operation.
        disposition_id: Nonempty stable retry identity.
        from_event_id: Nonnegative replay position, or None for reset.
        acknowledge_truncation: Whether the operator accepts unavailable
            history.
        reason: Nonempty operator explanation.
        identity: Executing process audit identity.
        wall_clock_time: Audit observation in seconds since the Unix epoch.
        operator_label: Trusted caller attribution, or None when unavailable.
    """

    command: Literal['replay', 'reset-to-latest']
    disposition_id: str
    from_event_id: int | None
    acknowledge_truncation: bool
    reason: str
    identity: AuditIdentity
    wall_clock_time: float
    operator_label: str | None

    def request(
        self, group: str, reserved_generation: int, expected_revision: int
    ) -> DispositionRequest:
        """Return the revision-fenced request for one reserved generation.

        Args:
            group: Target group name.
            reserved_generation: Supervisor-reserved generation to install.
            expected_revision: Observed group revision fencing the transaction.

        Returns:
            A request carrying the validated values and supplied transaction
            fences.
        """
        return DispositionRequest(
            command=self.command,
            group=group,
            disposition_id=self.disposition_id,
            reserved_generation=reserved_generation,
            expected_revision=expected_revision,
            from_event_id=self.from_event_id,
            acknowledge_truncation=self.acknowledge_truncation,
            reason=self.reason,
            identity=self.identity,
            wall_clock_time=self.wall_clock_time,
            operator_label=self.operator_label,
        )


@dataclass(frozen=True)
class EpochCaptureRequest:
    """Hold one exact strict epoch capture."""

    low_id: int
    high_id: int
    captured_at: float
    eligibility_point: float
    reader_clock_checkpoint: float
    source_generation: str


@dataclass(frozen=True)
class EpochPolicy:
    """Hold strict epoch capacity and timing policy."""

    max_pending_epochs: int
    capture_interval: float
    holdback_secs: float


@dataclass(frozen=True)
class LifecycleRequest:
    """Fence one orphan or re-add transition by durable lifecycle and supervisor intent.

    The transition generation is the supervisor's durable
    reservation/high-water observation. It distinguishes repeated removals
    before a disposition installs a new stored generation. Audit identity and
    time do not affect retry identity.
    """

    command: Literal['orphan', 'readd']
    expected_lifecycle: Literal['active', 'orphaned', 'readded-parked']
    expected_generation: int
    transition_generation: int
    identity: AuditIdentity
    wall_clock_time: float


@dataclass(frozen=True)
class ManifestRestartRequest:
    """Hold a complete replacement manifest and its byte limit."""

    manifest: CallbackManifest
    max_manifest_bytes: int


@dataclass(frozen=True)
class OperatorRequest:
    """Hold trusted local operator audit context."""

    reason: str
    identity: AuditIdentity
    wall_clock_time: float
    operator_label: str | None = None


@dataclass(frozen=True)
class CallbackRecoveryRequest:
    """Hold one exact callback recovery target and audit context."""

    event_id: int
    callback_ordinal: int | None
    operator: OperatorRequest


@dataclass(frozen=True)
class OrderingAnomalyRequest:
    """Hold one exact ordering-anomaly acknowledgement."""

    event_id: int
    operator: OperatorRequest


@dataclass(frozen=True)
class ReaderClockRequest:
    """Hold a source reader clock and minimum checkpoint interval."""

    reader_clock_checkpoint: float
    source_generation: str
    checkpoint_interval: float


@dataclass(frozen=True)
class StrictJoinOutcomeRequest:
    """Hold one strict join attempt and its capacity outcome policy."""

    attempt_token: str
    capacity_caused: bool
    abort_limit: int


@dataclass(frozen=True)
class _AuditEntry:
    """Hold one immutable audit-row insertion."""

    _command: str
    _operator: OperatorRequest
    _prior_revision: int
    _event_id: int | None = None
    _callback_ordinal: int | None = None
    _disposition_id: str | None = None
    _command_fingerprint: str | None = None
    _terminal_result: str | None = None
    _result_snapshot_json: str | None = None


@dataclass(frozen=True)
class _DispositionRequest:
    """Hold normalized replay or reset state."""

    _disposition_id: str
    _command: Literal['replay', 'reset-to-latest']
    _new_generation: int
    _cursor: int
    _delivery_floor: int
    _source_frontier: int
    _replay_from: int | None
    _acknowledge_truncation: bool
    _operator: OperatorRequest


@dataclass(frozen=True)
class _FingerprintInput:
    """Hold one exact operator fingerprint input."""

    _command: str
    _group_generation: int
    _operator: OperatorRequest
    _event_id: int | None = None
    _callback_ordinal: int | None = None
    _extra: dict[str, object] | None = None


@dataclass(frozen=True)
class _FloorSkipRequest:
    """Hold one generation-scoped delivery-floor audit."""

    _command: Literal['delivery-floor-skip', 'strict-delivery-floor-skip']
    _event_id: int
    _delivery_floor: int
    _operator: OperatorRequest
    _expected_revision: int


@dataclass(frozen=True)
class _RecordedCommand:
    """Identify one previously committed operator command result."""

    _command: str
    _command_fingerprint: str
    _disposition_id: str | None = None
    _terminal_result: str | None = None


def _bind_open_ranges(connection: sqlite3.Connection, group_name: str) -> None:
    """Bind the current generation to open ranges under its persisted floor and holdback."""
    connection.execute(
        """INSERT INTO range_groups
        (obligation_id, group_name, group_generation, delivery_floor,
         eligibility_point, obligation_state)
        SELECT obligation_id, group_name, group_generation, delivery_floor,
        opened_successful_at + holdback_secs,
        CASE WHEN delivery_floor < high_id THEN 'pending' ELSE 'satisfied-by-floor' END
        FROM discovery_ranges CROSS JOIN groups JOIN group_state USING (group_name)
        WHERE group_name = ? AND range_state = 'open'""",
        (group_name,),
    )


def _check_disposition_source(
    connection: sqlite3.Connection,
    target: DispositionTarget,
) -> int:
    """Fence a new command to the current configured source and return its frontier."""
    row = connection.execute(
        'SELECT frontier, stream_fingerprint, stream_incarnation, source_generation '
        'FROM source_meta'
    ).fetchone()
    if row is None or tuple(row[1:]) != (
        target.stream_fingerprint,
        target.verified_incarnation,
        target.source_generation,
    ):
        raise InvalidTransitionError('disposition source authority changed')
    return int(row[0])


def _command_fingerprint(command: str, payload: dict[str, object]) -> str:
    """Return one stable fingerprint for an exact recovery command."""
    return digest(canonical_bytes({'command': command, **payload}))


def _disposition_fingerprint(request: DispositionRequest) -> str:
    """Hash only the operator request, excluding authority and runtime observations."""
    return _command_fingerprint(
        request.command,
        {
            'group': request.group,
            'disposition_id': request.disposition_id,
            'reserved_generation': request.reserved_generation,
            'from_event_id': request.from_event_id,
            'acknowledge_truncation': request.acknowledge_truncation,
            'reason': request.reason,
            'operator_label': request.operator_label,
        },
    )


def _finish_audit(
    connection: sqlite3.Connection,
    group_name: str,
    audit_id: int,
    result_snapshot: GroupSnapshot,
) -> None:
    """Attach one result snapshot to its staged audit record."""
    connection.execute(
        'UPDATE audit SET result_snapshot_json = ? WHERE group_name = ? AND audit_id = ?',
        (snapshot_to_json(result_snapshot), group_name, audit_id),
    )


def _bump_and_finish_audit(
    connection: sqlite3.Connection,
    group_name: str,
    audit_id: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Advance the revision and attach the resulting snapshot to an existing audit."""
    bump_revision(connection, group_name, expected_revision)
    result = snapshot(connection, group_name)
    _finish_audit(connection, group_name, audit_id, result)
    return result


def _insert_conditional_park_result(
    connection: sqlite3.Connection,
    group_name: str,
    directive: ConditionalParkDirective,
    result: ConditionalParkResult,
    decided_at_wall_clock: float,
) -> None:
    """Retain one conditional-park decision inside the caller's transaction."""
    connection.execute(
        """
        INSERT INTO conditional_park_results (
            group_name, evidence_id, group_generation, evidence_fingerprint,
            outcome, resulting_revision, decided_at_wall_clock
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            group_name,
            result.evidence_id,
            directive.group_generation,
            directive.fingerprint(),
            result.outcome,
            result.resulting_revision,
            decided_at_wall_clock,
        ),
    )


def _install_disposition(
    connection: sqlite3.Connection,
    group_name: str,
    request: _DispositionRequest,
    target: DispositionTarget,
) -> None:
    """Replace one generation and its local recovery evidence without committing."""
    for statement in (
        'DELETE FROM callback_marks WHERE group_name = ?',
        'DELETE FROM epochs WHERE group_name = ?',
        'DELETE FROM join_commits WHERE group_name = ?',
        'DELETE FROM late_ids WHERE group_name = ?',
        'DELETE FROM range_groups WHERE group_name = ?',
        'DELETE FROM replay_markers WHERE group_name = ?',
    ):
        connection.execute(statement, (group_name,))
    connection.execute(
        """UPDATE groups SET site_server_url = ?, group_generation = ?, ordering_mode = ?,
        verified_incarnation = ?, holdback_secs = ?, active = 1, lifecycle = 'active'
        WHERE group_name = ?""",
        (
            target.site_server_url,
            request._new_generation,
            target.ordering,
            target.verified_incarnation,
            target.holdback_secs,
            group_name,
        ),
    )
    connection.execute(
        """UPDATE group_state SET cursor = ?, delivery_floor = ?,
        in_flight_event_id = NULL, in_flight_obligation_id = NULL, manifest_json = NULL,
        required_event_id = NULL, retry_event_id = NULL, is_operator_replay = ?,
        replay_required_from = NULL, history_truncated_floor = NULL, ordering_anomaly_id = NULL,
        strict_capacity_abort_streak = 0, strict_capacity_attempt_token = NULL,
        strict_capacity_last_caused = NULL, strict_capacity_exhausted = 0, shape_mismatch = 0,
        orphaned = 0, stream_identity_changed = 0, ordering_changed = 0,
        reader_clock_checkpoint = 0, last_epoch_capture = NULL, latest_join_boundary = NULL,
        last_delivered_id = ?, operational_late_id_count = 0 WHERE group_name = ?""",
        (
            request._cursor,
            request._delivery_floor,
            int(request._command == 'replay'),
            request._cursor,
            group_name,
        ),
    )
    clear_failure(connection, group_name)
    _bind_open_ranges(connection, group_name)


def _lifecycle_fingerprint(group_name: str, request: LifecycleRequest) -> str:
    """Identify one durable supervisor intent independently of its audit actor."""
    return _command_fingerprint(
        request.command,
        {
            'group': group_name,
            'expected_lifecycle': request.expected_lifecycle,
            'expected_generation': request.expected_generation,
            'transition_generation': request.transition_generation,
        },
    )


def _live_join_identity_json(identity: LiveJoinIdentity) -> str:
    """Return canonical storage for one live-join identity."""
    return canonical_bytes(
        {
            'site': identity.site,
            'source': identity.source,
            'group': identity.group,
            'join_kind': identity.join_kind,
            'source_generation': identity.source_generation,
            'group_generation': identity.group_generation,
            'boundary': identity.boundary,
            'attempt_token': identity.attempt_token,
        }
    ).decode()


def _normalize_request(request: DispositionRequest, frontier: int) -> _DispositionRequest:
    """Capture the source frontier without including it in command identity."""
    replay_from = request.from_event_id
    if request.command == 'replay' and replay_from is None:
        raise InvalidTransitionError('replay requires a starting event id')
    if request.command == 'reset-to-latest' and replay_from is not None:
        raise InvalidTransitionError('reset does not accept a replay position')
    cursor = frontier if replay_from is None else replay_from - 1
    return _DispositionRequest(
        request.disposition_id,
        request.command,
        request.reserved_generation,
        cursor,
        cursor,
        frontier,
        replay_from,
        request.acknowledge_truncation,
        OperatorRequest(
            request.reason, request.identity, request.wall_clock_time, request.operator_label
        ),
    )


def _operator_fingerprint(fingerprint_input: _FingerprintInput) -> str:
    """Return a stable fingerprint for one exact operator request."""
    operator = fingerprint_input._operator
    return _command_fingerprint(
        fingerprint_input._command,
        {
            'group_generation': fingerprint_input._group_generation,
            'reason': operator.reason,
            'wall_clock_time': operator.wall_clock_time,
            'operator_label': operator.operator_label,
            'event_id': fingerprint_input._event_id,
            'callback_ordinal': fingerprint_input._callback_ordinal,
            **({} if fingerprint_input._extra is None else fingerprint_input._extra),
        },
    )


def _purge_obligation_tables(
    connection: sqlite3.Connection,
    group_name: str,
    *,
    include_marks: bool,
) -> None:
    """Retire operational obligations, optionally including callback marks."""
    if include_marks:
        connection.execute('DELETE FROM callback_marks WHERE group_name = ?', (group_name,))
    connection.execute('DELETE FROM epochs WHERE group_name = ?', (group_name,))
    connection.execute('DELETE FROM join_commits WHERE group_name = ?', (group_name,))
    connection.execute('DELETE FROM late_ids WHERE group_name = ?', (group_name,))
    changed = connection.execute(
        """
        DELETE FROM range_groups WHERE group_name = ? AND group_generation = (SELECT
        group_generation FROM groups WHERE group_name = ?)
        """,
        (group_name, group_name),
    ).rowcount
    if changed:
        connection.execute(BUMP_INDEX_REVISION)
        connection.execute(BUMP_RANGE_REVISION)


def _record_audit(connection: sqlite3.Connection, group_name: str, entry: _AuditEntry) -> int:
    """Append one locally attributed immutable audit record."""
    operator = entry._operator
    if not operator.reason:
        raise InvalidTransitionError('operator transition reason must not be empty')
    if operator.identity != AuditIdentity.current():
        raise InvalidTransitionError('audit identity does not match the local process')
    meta = connection.execute(
        'SELECT site, group_name FROM groups CROSS JOIN source_meta WHERE group_name = ?',
        (group_name,),
    ).fetchone()
    if meta is None:
        raise RuntimeError('group identity metadata is missing')
    plugin_id: str | None = None
    callback_id: str | None = None
    if entry._event_id is not None and entry._callback_ordinal is not None:
        mark = connection.execute(
            """
            SELECT plugin_id, callback_id
            FROM callback_marks
            WHERE group_name = ? AND event_id = ? AND ordinal = ?
            """,
            (group_name, entry._event_id, entry._callback_ordinal),
        ).fetchone()
        if mark is not None:
            plugin_id = str(mark[0])
            callback_id = str(mark[1])
    cursor = connection.execute(
        """
        INSERT INTO audit (
            site, group_name, event_id, plugin_id, callback_id,
            command, wall_clock_time, prior_revision, new_revision,
            reason, operator_label, local_uid, username, pid,
            disposition_id, command_fingerprint, terminal_result,
            result_snapshot_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(meta[0]),
            str(meta[1]),
            entry._event_id,
            plugin_id,
            callback_id,
            entry._command,
            operator.wall_clock_time,
            entry._prior_revision,
            entry._prior_revision + 1,
            operator.reason,
            operator.operator_label,
            operator.identity.local_uid,
            operator.identity.username,
            operator.identity.pid,
            entry._disposition_id,
            entry._command_fingerprint,
            entry._terminal_result,
            entry._result_snapshot_json,
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError('audit insert did not produce an identity')
    return int(cursor.lastrowid)


def _audit_and_bump(
    connection: sqlite3.Connection,
    group_name: str,
    entry: _AuditEntry,
) -> GroupSnapshot:
    """Append an audit and finish its revision after command-specific updates."""
    audit_id = _record_audit(connection, group_name, entry)
    return _bump_and_finish_audit(connection, group_name, audit_id, entry._prior_revision)


def _recorded_live_join(
    connection: sqlite3.Connection,
    group_name: str,
    identity: LiveJoinIdentity,
    identity_json: str,
) -> GroupSnapshot | None:
    """Return a recorded live join or reject conflicting token reuse."""
    existing = connection.execute(
        """
        SELECT identity_json, snapshot_json
        FROM join_commits
        WHERE group_name = ? AND attempt_token = ?
        """,
        (group_name, identity.attempt_token),
    ).fetchone()
    if existing is None:
        return None
    if str(existing[0]) != identity_json:
        raise InvalidTransitionError('live-join token was reused for another identity')
    return snapshot_from_json(str(existing[1]))


def _recorded_snapshot(
    connection: sqlite3.Connection, group_name: str, record: _RecordedCommand
) -> GroupSnapshot | None:
    """Return one exact historical command result when already committed."""
    row = connection.execute(
        """
        SELECT result_snapshot_json
        FROM audit
        WHERE group_name = ? AND command = ?
          AND command_fingerprint = ?
          AND disposition_id IS ?
          AND terminal_result IS ?
          AND result_snapshot_json IS NOT NULL
        ORDER BY audit_id DESC
        LIMIT 1
        """,
        (
            group_name,
            record._command,
            record._command_fingerprint,
            record._disposition_id,
            record._terminal_result,
        ),
    ).fetchone()
    if row is None:
        return None
    return snapshot_from_json(str(row[0]))


def _operator_receipt(
    connection: sqlite3.Connection,
    group_name: str,
    command: _FingerprintInput,
) -> tuple[str, GroupSnapshot | None]:
    """Fingerprint an operator command and look up its exact historical receipt."""
    fingerprint = _operator_fingerprint(command)
    recorded = _recorded_snapshot(
        connection,
        group_name,
        _RecordedCommand(_command=command._command, _command_fingerprint=fingerprint),
    )
    return fingerprint, recorded


def _record_floor_skip(
    connection: sqlite3.Connection,
    group_name: str,
    current: GroupSnapshot,
    request: _FloorSkipRequest,
) -> GroupSnapshot:
    """Persist one generation-scoped idempotent delivery-floor audit."""
    fingerprint = _command_fingerprint(
        request._command,
        {
            'delivery_floor': request._delivery_floor,
            'event_id': request._event_id,
            'group_generation': current.group_generation,
        },
    )
    recorded = _recorded_snapshot(
        connection,
        group_name,
        _RecordedCommand(_command=request._command, _command_fingerprint=fingerprint),
    )
    if recorded is not None:
        return current
    check_revision(connection, group_name, request._expected_revision)
    if request._delivery_floor != current.delivery_floor:
        raise InvalidTransitionError('delivery-floor identity conflicts')
    if request._event_id > request._delivery_floor:
        raise InvalidTransitionError('delivery-floor skip event identity conflicts')
    return _audit_and_bump(
        connection,
        group_name,
        _AuditEntry(
            _command=request._command,
            _operator=request._operator,
            _prior_revision=request._expected_revision,
            _event_id=request._event_id,
            _command_fingerprint=fingerprint,
        ),
    )


def _record_strict_floor_skip(
    connection: sqlite3.Connection,
    group_name: str,
    current: GroupSnapshot,
    request: _FloorSkipRequest,
) -> GroupSnapshot:
    """Validate strict ordering before the common delivery-floor audit."""
    if current.ordering != 'strict':
        raise InvalidTransitionError('strict floor skips require strict ordering')
    return _record_floor_skip(connection, group_name, current, request)


def _replay_covers_active_event(current: GroupSnapshot, request: _DispositionRequest) -> bool:
    """Require replay coverage or explicit proof the active event is unavailable."""
    event_id = current.in_flight_event_id
    replay_from = request._replay_from
    if event_id is None or replay_from is None:
        return False
    if replay_from <= event_id:
        return True
    floor = current.history_truncated_floor
    return (
        request._acknowledge_truncation
        and floor is not None
        and floor > event_id
        and replay_from >= floor
    )


def _collapsed_event_replay_allowed(
    connection: sqlite3.Connection,
    group_name: str,
    current: GroupSnapshot,
    request: _DispositionRequest,
    target: DispositionTarget,
) -> bool:
    """Fence the audited replay exception to the current active collapsed generation."""
    if (
        request._command != 'replay'
        or not current.parked
        or 'orphaned' in current.conditions
        or 'stream-identity-changed' in current.conditions
        or current.replay_required_from is None
        or target.source_generation != current.source_generation
        or target.verified_incarnation != current.verified_incarnation
        or target.stream_fingerprint != current.stream_fingerprint
        or not _replay_covers_active_event(current, request)
    ):
        return False
    return (
        connection.execute(
            """SELECT 1 FROM replay_markers AS rm
        JOIN groups AS sg USING (group_name, group_generation)
        WHERE rm.group_name = ? AND rm.group_generation = ?
            AND rm.from_event_id = ? AND sg.active = 1 AND rm.source_generation = ?""",
            (
                group_name,
                current.group_generation,
                current.replay_required_from,
                target.source_generation,
            ),
        ).fetchone()
        is not None
    )


def _truncated_event_retirement_allowed(
    connection: sqlite3.Connection,
    group_name: str,
    current: GroupSnapshot,
    request: _DispositionRequest,
    target: DispositionTarget,
) -> bool:
    """Allow retiring an in-flight event that SG retention has provably deleted."""
    event_id = current.in_flight_event_id
    floor = current.history_truncated_floor
    if (
        event_id is None
        or floor is None
        or floor <= event_id
        or not current.parked
        or 'orphaned' in current.conditions
        or 'stream-identity-changed' in current.conditions
        or target.source_generation != current.source_generation
        or target.verified_incarnation != current.verified_incarnation
        or target.stream_fingerprint != current.stream_fingerprint
    ):
        return False
    if request._command == 'replay' and not (
        request._acknowledge_truncation
        and request._replay_from is not None
        and request._replay_from >= floor
    ):
        return False
    return (
        connection.execute(
            'SELECT 1 FROM groups WHERE group_name = ? AND group_generation = ? AND active = 1',
            (group_name, current.group_generation),
        ).fetchone()
        is not None
    )


def _validate_live_join_blockers(
    connection: sqlite3.Connection,
    group_name: str,
    current: GroupSnapshot,
    identity: LiveJoinIdentity,
) -> None:
    """Reject unresolved group-local work through a live-join boundary."""
    if current.in_flight_event_id is not None or current.conditions:
        raise InvalidTransitionError('group-local recovery evidence blocks live join')
    if identity.join_kind == 'eager':
        return
    epoch_pending = connection.execute(
        'SELECT 1 FROM epochs WHERE group_name = ? AND low_id <= ? LIMIT 1',
        (group_name, identity.boundary),
    ).fetchone()
    obligation_pending = connection.execute(
        """
        SELECT 1 FROM range_groups JOIN discovery_ranges USING (obligation_id) WHERE
        group_name = ? AND obligation_state = 'pending' AND low_id <= ? AND
        group_generation = (SELECT group_generation FROM groups WHERE group_name
        = ?) LIMIT 1
        """,
        (group_name, identity.boundary, group_name),
    ).fetchone()
    if epoch_pending is not None or obligation_pending is not None:
        raise InvalidTransitionError('strict catch-up remains unresolved through the boundary')


def _validate_live_join_identity(
    current: GroupSnapshot,
    identity: LiveJoinIdentity,
) -> None:
    """Validate one live join against current durable identity and position."""
    if not identity.attempt_token:
        raise InvalidTransitionError('live-join attempt token must not be empty')
    if (
        identity.site != current.site
        or identity.source != current.source
        or identity.group != current.group
        or identity.group_generation != current.group_generation
        or identity.source_generation != current.source_generation
    ):
        raise InvalidTransitionError('live join targets stale group or source identity')
    if identity.join_kind != current.ordering:
        raise InvalidTransitionError('live-join kind does not match group ordering')
    if (
        current.latest_join_boundary is not None
        and identity.boundary < current.latest_join_boundary
    ):
        raise InvalidTransitionError('live-join boundary moved backward')
    if current.cursor < identity.boundary:
        raise InvalidTransitionError('group cursor has not reached the live-join boundary')


def _validate_replay_position(
    current: GroupSnapshot,
    request: _DispositionRequest,
) -> None:
    """Validate inclusive replay bounds and retained evidence."""
    replay_from = request._replay_from
    if replay_from is None:
        return
    if replay_from < 1 or replay_from > request._source_frontier:
        raise InvalidTransitionError('replay position exceeds the source frontier')
    if (
        current.history_truncated_floor is not None
        and replay_from < current.history_truncated_floor
    ):
        raise InvalidTransitionError('replay position is below the visible retention floor')
    if current.replay_required_from is None or replay_from <= current.replay_required_from:
        return
    override_allowed = (
        request._acknowledge_truncation
        and current.history_truncated_floor is not None
        and current.history_truncated_floor > current.replay_required_from
        and replay_from >= current.history_truncated_floor
    )
    if not override_allowed:
        raise InvalidTransitionError('replay position is later than retained replay evidence')


def _validate_strict_live_control(
    connection: sqlite3.Connection,
    group_name: str,
    current: GroupSnapshot,
    identity: LiveJoinIdentity,
) -> None:
    """Fence one strict control to the current committed live attempt."""
    _validate_live_join_identity(current, identity)
    if identity.join_kind != 'strict' or current.ordering != 'strict':
        raise InvalidTransitionError('strict control requires strict ordering')
    if current.strict_capacity_attempt_token != identity.attempt_token:
        raise InvalidTransitionError('strict control targets a stale live attempt')
    row = connection.execute(
        """
        SELECT identity_json
        FROM join_commits
        WHERE group_name = ? AND attempt_token = ?
        """,
        (group_name, identity.attempt_token),
    ).fetchone()
    if row is None or str(row[0]) != _live_join_identity_json(identity):
        raise InvalidTransitionError('strict control live identity conflicts')


def acknowledge_ordering_anomaly(
    connection: sqlite3.Connection,
    group_name: str,
    request: OrderingAnomalyRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Audit an exact strict-ordering anomaly skip.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: Exact anomaly acknowledgement and operator context.
        expected_revision: Required for a new transition. An exact recorded
            retry returns its historical snapshot before revision comparison.

    Returns:
        New or recorded immutable snapshot without the acknowledged anomaly.

    Raises:
        InvalidTransitionError: The event does not match the active anomaly, or
            a new audit has an empty operator reason or nonlocal identity.
        RevisionConflictError: The expected revision differs for an unrecorded
            transition.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    current = snapshot(connection, group_name)
    command = 'acknowledge-ordering-anomaly'
    fingerprint, recorded = _operator_receipt(
        connection,
        group_name,
        _FingerprintInput(
            _command=command,
            _group_generation=current.group_generation,
            _operator=request.operator,
            _event_id=request.event_id,
        ),
    )
    if recorded is not None:
        return recorded
    check_revision(connection, group_name, expected_revision)
    if current.ordering_anomaly_id != request.event_id:
        raise InvalidTransitionError('ordering acknowledgement does not match the anomaly')
    audit_id = _record_audit(
        connection,
        group_name,
        _AuditEntry(
            _command=command,
            _operator=request.operator,
            _prior_revision=expected_revision,
            _event_id=request.event_id,
            _command_fingerprint=fingerprint,
        ),
    )
    connection.execute(
        'UPDATE group_state SET ordering_anomaly_id = NULL WHERE group_name = ?', (group_name,)
    )
    return _bump_and_finish_audit(connection, group_name, audit_id, expected_revision)


def audit_records(connection: sqlite3.Connection, group_name: str) -> tuple[AuditRecord, ...]:
    """Return cumulative audit history from one open facade connection.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.

    Returns:
        Immutable audit records in durable insertion order.
    """
    return tuple(
        AuditRecord(
            site=str(row[0]),
            group=str(row[1]),
            event_id=None if row[2] is None else int(row[2]),
            plugin_id=None if row[3] is None else str(row[3]),
            callback_id=None if row[4] is None else str(row[4]),
            command=str(row[5]),
            wall_clock_time=float(row[6]),
            prior_revision=int(row[7]),
            new_revision=int(row[8]),
            reason=str(row[9]),
            operator_label=None if row[10] is None else str(row[10]),
            local_uid=int(row[11]),
            username=str(row[12]),
            pid=int(row[13]),
            disposition_id=None if row[14] is None else str(row[14]),
            command_fingerprint=None if row[15] is None else str(row[15]),
            terminal_result=None if row[16] is None else str(row[16]),
        )
        for row in connection.execute(
            """
            SELECT site, group_name, event_id, plugin_id, callback_id,
                   command, wall_clock_time, prior_revision, new_revision,
                   reason, operator_label, local_uid, username, pid,
                   disposition_id, command_fingerprint, terminal_result
            FROM audit
            WHERE group_name = ?
            ORDER BY audit_id
            """,
            (group_name,),
        ).fetchall()
    )


def capture_epoch(
    connection: sqlite3.Connection,
    group_name: str,
    request: EpochCaptureRequest,
    policy: EpochPolicy,
    expected_revision: int,
) -> GroupSnapshot:
    """Persist one disjoint body-free strict epoch inside a live transaction.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: Exact epoch bounds, clocks, and source generation.
        policy: Strict epoch capacity and timing policy.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot containing the waiting epoch.

    Raises:
        InvalidTransitionError: Ordering, generation, bounds, capacity,
            sequence, or timing does not permit the epoch.
        RevisionConflictError: The expected revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    if current.ordering != 'strict':
        raise InvalidTransitionError('catch-up epochs require strict ordering')
    if request.source_generation != current.source_generation:
        raise InvalidTransitionError('epoch source generation is stale')
    if request.low_id > request.high_id:
        raise InvalidTransitionError('epoch bounds are inverted')
    if len(current.epochs) >= policy.max_pending_epochs:
        raise InvalidTransitionError('max_pending_epochs is exhausted')
    expected_low = current.cursor + 1 if not current.epochs else current.epochs[-1].high_id + 1
    if request.low_id != expected_low:
        raise InvalidTransitionError('epoch range is not the next disjoint range')
    last_capture = connection.execute(
        'SELECT last_epoch_capture FROM group_state WHERE group_name = ?', (group_name,)
    ).fetchone()[0]
    if (
        last_capture is not None
        and request.captured_at < float(last_capture) + policy.capture_interval
    ):
        raise InvalidTransitionError('epoch capture is earlier than the capture interval')
    if (
        request.reader_clock_checkpoint < request.captured_at
        or request.reader_clock_checkpoint < current.reader_clock_checkpoint
        or request.eligibility_point < request.captured_at + policy.holdback_secs
    ):
        raise InvalidTransitionError('epoch timing is not conservative')
    connection.execute(
        """
        INSERT INTO epochs (
            group_name, low_id, high_id, captured_at, eligibility_point,
            reader_clock_checkpoint, source_generation,
            group_generation, state
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'waiting')
        """,
        (
            group_name,
            request.low_id,
            request.high_id,
            request.captured_at,
            request.eligibility_point,
            request.reader_clock_checkpoint,
            request.source_generation,
            current.group_generation,
        ),
    )
    connection.execute(
        """
        UPDATE group_state
        SET reader_clock_checkpoint = ?, last_epoch_capture = ?
        WHERE group_name = ?
        """,
        (request.reader_clock_checkpoint, request.captured_at, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def checkpoint_reader_clock(
    connection: sqlite3.Connection,
    group_name: str,
    request: ReaderClockRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Persist an idle-frontier reader-clock checkpoint.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: Source generation, monotonic clock value, and minimum clock
            advance.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot containing the reader-clock checkpoint.

    Raises:
        InvalidTransitionError: No strict epoch is pending, the source
            generation is stale, or the checkpoint is too early.
        RevisionConflictError: The expected revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    if current.ordering != 'strict' or not current.epochs:
        raise InvalidTransitionError('reader-clock checkpoints require pending strict epochs')
    if request.source_generation != current.source_generation:
        raise InvalidTransitionError('reader-clock source generation is stale')
    if (
        request.reader_clock_checkpoint
        < current.reader_clock_checkpoint + request.checkpoint_interval
    ):
        raise InvalidTransitionError(
            'reader-clock checkpoint is earlier than the checkpoint interval'
        )
    connection.execute(
        'UPDATE group_state SET reader_clock_checkpoint = ? WHERE group_name = ?',
        (request.reader_clock_checkpoint, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def complete_epoch(
    connection: sqlite3.Connection,
    group_name: str,
    epoch_id: int,
    cursor: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Atomically advance through and remove one delivering epoch.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        epoch_id: Durable identity of the delivering epoch.
        cursor: Required upper-bound cursor for the epoch.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot advanced through the completed epoch.

    Raises:
        InvalidTransitionError: Recovery evidence or an obligation blocks
            completion, the epoch is not delivering, or the cursor is wrong.
        RevisionConflictError: The expected revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    if current.required_event_id is not None or current.conditions:
        raise InvalidTransitionError('epoch completion is blocked by recovery evidence')
    epoch = connection.execute(
        'SELECT low_id, high_id, state FROM epochs WHERE group_name = ? AND epoch_id = ?',
        (group_name, epoch_id),
    ).fetchone()
    if epoch is None or str(epoch[2]) != 'delivering':
        raise InvalidTransitionError('epoch is not delivering')
    if cursor != int(epoch[1]):
        raise InvalidTransitionError('cursor does not equal the epoch upper bound')
    fence = delivery.strict_obligation_fence(connection, group_name)
    if fence is not None and fence[1] <= int(epoch[1]):
        raise InvalidTransitionError('epoch completion is blocked by an unresolved obligation')
    connection.execute(
        """
        UPDATE group_state
        SET cursor = MAX(cursor, ?), last_delivered_id = MAX(last_delivered_id, ?)
        WHERE group_name = ?
        """,
        (cursor, cursor, group_name),
    )
    connection.execute(
        'DELETE FROM epochs WHERE group_name = ? AND high_id <= ?',
        (group_name, cursor),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def control_peer_label(identity: AuditIdentity) -> str:
    """Build a retry-stable label for a trusted control peer.

    Args:
        identity: Authenticated control peer; its process ID may change on
            retry.

    Returns:
        The peer's username and local UID without its process ID.
    """
    return f'{identity.username} uid={identity.local_uid}'


def disposition_admitted(*, parked: bool, lifecycle: str) -> bool:
    """Return whether a group may admit a position-changing disposition."""
    return parked or lifecycle == 'orphaned'


def _validate_disposition(
    connection: sqlite3.Connection,
    group_name: str,
    current: GroupSnapshot,
    request: _DispositionRequest,
    target: DispositionTarget,
) -> Literal['replay-collapsed-event', 'retire-truncated-event'] | None:
    """Validate one new replay or reset disposition."""
    if not request._disposition_id:
        raise InvalidTransitionError('disposition id must not be empty')
    if request._new_generation <= current.group_generation:
        raise InvalidTransitionError('new group generation must increase')
    retirement = None
    if current.in_flight_event_id is not None:
        if _collapsed_event_replay_allowed(connection, group_name, current, request, target):
            retirement = 'replay-collapsed-event'
        elif _truncated_event_retirement_allowed(connection, group_name, current, request, target):
            retirement = 'retire-truncated-event'
        else:
            raise InvalidTransitionError('position changes require no active event')
    if not disposition_admitted(parked=current.parked, lifecycle=current.status):
        raise InvalidTransitionError('position changes require a parked or orphaned group')
    _validate_replay_position(current, request)
    return retirement


def disposition_command(
    command: str,
    arguments: Mapping[str, object],
    *,
    disposition_id: str,
    wall_clock_time: float,
    operator_label: str | None,
) -> DispositionCommand:
    """Validate replay or reset arguments once for every route.

    Args:
        command: Replay, reset-to-latest, or its reset-group-to-latest operator
            name.
        arguments: Semantic values: replay position, boolean truncation
            acknowledgment, and nonempty reason. Reset must have no replay
            position.
        disposition_id: Nonempty stable retry identity.
        wall_clock_time: Audit observation in seconds since the Unix epoch.
        operator_label: Trusted caller attribution, or None when unavailable.

    Returns:
        Validated semantic values with the current executing process's audit
        identity.

    Raises:
        ValueError: The command, arguments, or disposition identity is invalid.
    """
    if command == 'replay':
        kind: Literal['replay', 'reset-to-latest'] = 'replay'
    elif command in {'reset-group-to-latest', 'reset-to-latest'}:
        kind = 'reset-to-latest'
    else:
        raise ValueError('group disposition command is invalid')
    from_event_id = arguments.get('from_event_id')
    if kind == 'replay':
        if type(from_event_id) is not int or from_event_id < 0:
            raise ValueError('replay position must be a nonnegative integer')
    elif from_event_id is not None:
        raise ValueError('reset-to-latest must not include a replay position')
    acknowledge = arguments.get('acknowledge_truncation', False)
    if type(acknowledge) is not bool:
        raise ValueError('replay truncation acknowledgement must be a boolean')
    reason = arguments.get('reason')
    if not isinstance(reason, str) or not reason:
        raise ValueError('group disposition reason must be nonempty text')
    if not disposition_id:
        raise ValueError('disposition id must not be empty')
    return DispositionCommand(
        command=kind,
        disposition_id=disposition_id,
        from_event_id=from_event_id,
        acknowledge_truncation=acknowledge,
        reason=reason,
        identity=AuditIdentity.current(),
        wall_clock_time=wall_clock_time,
        operator_label=operator_label,
    )


def group_result_mapping(snapshot: GroupSnapshot) -> Mapping[str, object]:
    """Project one body-free recovery result for the control plane."""
    return {
        'site': snapshot.site,
        'source': snapshot.source,
        'group': snapshot.group,
        'group_generation': snapshot.group_generation,
        'revision': snapshot.revision,
        'status': snapshot.status,
        'conditions': snapshot.conditions,
        'cursor': snapshot.cursor,
        'parked': snapshot.parked,
    }


def mark_epoch_eligible(
    connection: sqlite3.Connection,
    group_name: str,
    epoch_id: int,
    reader_clock_checkpoint: float,
    expected_revision: int,
) -> GroupSnapshot:
    """Persist one eligibility transition and reader-clock checkpoint.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        epoch_id: Durable identity of the waiting epoch.
        reader_clock_checkpoint: Reader clock proving epoch eligibility.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot with the epoch marked for delivery.

    Raises:
        InvalidTransitionError: The epoch is absent, not next, not waiting, or
            not yet eligible, or the reader clock moves backward.
        RevisionConflictError: The expected revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    epoch = connection.execute(
        (
            'SELECT low_id, eligibility_point, state FROM epochs WHERE '
            'group_name = ? AND epoch_id = ?'
        ),
        (group_name, epoch_id),
    ).fetchone()
    if epoch is None or str(epoch[2]) != 'waiting':
        raise InvalidTransitionError('epoch is not waiting')
    if (
        connection.execute(
            'SELECT 1 FROM epochs WHERE group_name = ? AND low_id < ? LIMIT 1',
            (group_name, int(epoch[0])),
        ).fetchone()
        is not None
    ):
        raise InvalidTransitionError('an earlier epoch must complete first')
    if reader_clock_checkpoint < float(epoch[1]):
        raise InvalidTransitionError('reader clock has not reached epoch eligibility')
    current_clock = float(
        connection.execute(
            'SELECT reader_clock_checkpoint FROM group_state WHERE group_name = ?', (group_name,)
        ).fetchone()[0]
    )
    if reader_clock_checkpoint < current_clock:
        raise InvalidTransitionError('reader clock cannot move backward')
    connection.execute(
        """
        UPDATE epochs
        SET state = 'delivering', reader_clock_checkpoint = ?
        WHERE group_name = ? AND epoch_id = ?
        """,
        (reader_clock_checkpoint, group_name, epoch_id),
    )
    connection.execute(
        'UPDATE group_state SET reader_clock_checkpoint = ? WHERE group_name = ?',
        (reader_clock_checkpoint, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def mark_history_truncated(
    connection: sqlite3.Connection,
    group_name: str,
    retention_floor: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Persist one visible-history retention floor halt.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        retention_floor: Positive earliest visible event id.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Current or revision-advanced immutable retention-floor snapshot.

    Raises:
        InvalidTransitionError: The retention floor is not positive.
        RevisionConflictError: The expected revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    if retention_floor < 1:
        raise InvalidTransitionError('history retention floor must be positive')
    if (
        current.history_truncated_floor is not None
        and retention_floor <= current.history_truncated_floor
    ):
        return current
    connection.execute(
        'UPDATE group_state SET history_truncated_floor = ? WHERE group_name = ?',
        (retention_floor, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def mark_ordering_anomaly(
    connection: sqlite3.Connection,
    group_name: str,
    event_id: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Persist one strict late-discovery ordering anomaly.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        event_id: Late event id that decreases delivery order.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Immutable snapshot containing the active ordering anomaly.

    Raises:
        InvalidTransitionError: Ordering is not strict, the event does not
            identify a new anomaly, or another anomaly is active.
        RevisionConflictError: The expected revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    if current.ordering != 'strict':
        raise InvalidTransitionError('ordering anomalies require strict ordering')
    last_delivered = int(
        connection.execute(
            'SELECT last_delivered_id FROM group_state WHERE group_name = ?', (group_name,)
        ).fetchone()[0]
    )
    if event_id <= current.delivery_floor:
        raise InvalidTransitionError('event is at or below the audited delivery floor')
    if event_id >= last_delivered:
        raise InvalidTransitionError('event does not decrease strict delivery order')
    if current.ordering_anomaly_id is not None:
        raise InvalidTransitionError('another ordering anomaly remains active')
    connection.execute(
        'UPDATE group_state SET ordering_anomaly_id = ? WHERE group_name = ?',
        (event_id, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def mark_strict_capacity_exhausted(
    connection: sqlite3.Connection,
    group_name: str,
    identity: LiveJoinIdentity,
    *,
    abort_limit: int,
    expected_revision: int,
) -> GroupSnapshot:
    """Persist one complete-identity live-window capacity halt.

    Args:
        group_name: Exact group whose durable rows are accessed.
        connection: Live group-state database connection.
        identity: Exact committed strict live-join identity.
        abort_limit: Positive capacity-abort threshold to persist.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Current or revision-advanced immutable capacity-halt snapshot.

    Raises:
        InvalidTransitionError: The live identity is stale or conflicting,
            ordering is not strict, or the abort limit is invalid.
        RevisionConflictError: The expected revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    _validate_strict_live_control(connection, group_name, current, identity)
    if type(abort_limit) is not int or abort_limit <= 0:
        raise InvalidTransitionError('strict capacity abort limit must be positive')
    row = connection.execute(
        """
        SELECT strict_capacity_exhausted, strict_capacity_attempt_token
        FROM group_state
        WHERE group_name = ?
        """,
        (group_name,),
    ).fetchone()
    if row is None:
        raise RuntimeError('group state is not initialized')
    if bool(row[0]):
        if str(row[1]) != identity.attempt_token:
            raise InvalidTransitionError('strict capacity halt targets a stale attempt')
        return current
    connection.execute(
        """
        UPDATE group_state
        SET strict_capacity_abort_streak = ?,
            strict_capacity_attempt_token = ?,
            strict_capacity_last_caused = 1,
            strict_capacity_exhausted = 1
        WHERE group_name = ?
        """,
        (abort_limit, identity.attempt_token, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def park(
    connection: sqlite3.Connection,
    group_name: str,
    operator: OperatorRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Park callback scheduling inside a live transaction.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        operator: Trusted local operator audit context.
        expected_revision: Required for a new transition. An exact recorded
            retry returns its historical snapshot before revision comparison.

    Returns:
        New or recorded immutable parked snapshot.

    Raises:
        InvalidTransitionError: The group is already parked, or a new audit has
            an empty operator reason or nonlocal identity.
        RevisionConflictError: The expected revision differs for an unrecorded
            transition.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    current = snapshot(connection, group_name)
    command = 'park'
    fingerprint, recorded = _operator_receipt(
        connection,
        group_name,
        _FingerprintInput(
            _command=command,
            _group_generation=current.group_generation,
            _operator=operator,
            _event_id=current.in_flight_event_id,
        ),
    )
    if recorded is not None:
        return recorded
    check_revision(connection, group_name, expected_revision)
    if current.parked:
        raise InvalidTransitionError('group is already parked')
    connection.execute('UPDATE group_state SET parked = 1 WHERE group_name = ?', (group_name,))
    return _audit_and_bump(
        connection,
        group_name,
        _AuditEntry(
            _command=command,
            _operator=operator,
            _prior_revision=expected_revision,
            _event_id=current.in_flight_event_id,
            _command_fingerprint=fingerprint,
        ),
    )


def record_delivery_floor_skip(
    connection: sqlite3.Connection,
    group_name: str,
    event_id: int,
    operator: OperatorRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Stage one ordering-neutral delivery-floor audit.

    Args:
        connection: Live configured-level group-state transaction connection;
            the caller commits or rolls back.
        group_name: Exact group whose durable rows are accessed.
        event_id: Event id skipped at or below the delivery floor.
        operator: Trusted local operator audit context.
        expected_revision: Required for a new audit. An exact recorded retry
            returns the current snapshot before revision comparison.

    Returns:
        Current or revision-advanced immutable snapshot.

    Raises:
        InvalidTransitionError: The event is above the durable delivery floor,
            or a new audit has an empty operator reason or nonlocal identity.
        RevisionConflictError: The expected revision differs for an unrecorded
            transition.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    current = snapshot(connection, group_name)
    return _record_floor_skip(
        connection,
        group_name,
        current,
        _FloorSkipRequest(
            _command='delivery-floor-skip',
            _event_id=event_id,
            _delivery_floor=current.delivery_floor,
            _operator=operator,
            _expected_revision=expected_revision,
        ),
    )


def record_strict_floor_skip(
    connection: sqlite3.Connection,
    group_name: str,
    event_id: int,
    operator: OperatorRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Stage one detached strict delivery-floor audit.

    Args:
        connection: Live configured-level group-state transaction connection;
            the caller commits or rolls back.
        group_name: Exact group whose durable rows are accessed.
        event_id: Strict event id skipped at or below the delivery floor.
        operator: Trusted local operator audit context.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Current or revision-advanced immutable snapshot.

    Raises:
        InvalidTransitionError: Ordering is not strict, the event is above the
            delivery floor, or a new audit has an empty operator reason or
            nonlocal identity.
        RevisionConflictError: The expected revision differs.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    return _record_strict_floor_skip(
        connection,
        group_name,
        current,
        _FloorSkipRequest(
            _command='strict-delivery-floor-skip',
            _event_id=event_id,
            _delivery_floor=current.delivery_floor,
            _operator=operator,
            _expected_revision=expected_revision,
        ),
    )


def record_strict_join_outcome(
    connection: sqlite3.Connection,
    group_name: str,
    request: StrictJoinOutcomeRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Persist one bounded token-fenced strict join outcome.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: Attempt token, capacity cause, and positive consecutive abort
            limit.
        expected_revision: Required for a new outcome. An exact retry of the
            retained token returns the current snapshot before comparison.

    Returns:
        Current or revision-advanced immutable strict-capacity snapshot.

    Raises:
        InvalidTransitionError: Ordering is not strict, an input is invalid, or
            the attempt token conflicts with its recorded outcome.
        RevisionConflictError: The expected revision differs for an unrecorded
            transition.
        RuntimeError: Durable group state is not initialized.
    """
    current = snapshot(connection, group_name)
    if current.ordering != 'strict':
        raise InvalidTransitionError('strict join outcomes require strict ordering')
    if not request.attempt_token:
        raise InvalidTransitionError('strict join attempt token must not be empty')
    if type(request.capacity_caused) is not bool:
        raise InvalidTransitionError('strict join capacity outcome must be Boolean')
    if type(request.abort_limit) is not int or request.abort_limit <= 0:
        raise InvalidTransitionError('strict join abort limit must be positive')
    row = connection.execute(
        """
        SELECT strict_capacity_abort_streak,
               strict_capacity_attempt_token,
               strict_capacity_last_caused,
               strict_capacity_exhausted
        FROM group_state
        WHERE group_name = ?
        """,
        (group_name,),
    ).fetchone()
    if row is None:
        raise RuntimeError('group state is not initialized')
    streak = int(row[0])
    recorded_token = None if row[1] is None else str(row[1])
    recorded_caused = None if row[2] is None else bool(row[2])
    exhausted = bool(row[3])
    if recorded_token == request.attempt_token and recorded_caused is not None:
        if recorded_caused != request.capacity_caused:
            raise InvalidTransitionError('strict join outcome conflicts for attempt token')
        return current
    check_revision(connection, group_name, expected_revision)
    if exhausted:
        return current
    next_streak = streak + 1 if request.capacity_caused else 0
    connection.execute(
        """
        UPDATE group_state
        SET strict_capacity_abort_streak = ?,
            strict_capacity_attempt_token = ?,
            strict_capacity_last_caused = ?,
            strict_capacity_exhausted = ?
        WHERE group_name = ?
        """,
        (
            next_streak,
            request.attempt_token,
            int(request.capacity_caused),
            int(request.capacity_caused and next_streak >= request.abort_limit),
            group_name,
        ),
    )
    bump_revision(connection, group_name, expected_revision)
    return snapshot(connection, group_name)


def record_strict_ordering_decision(
    connection: sqlite3.Connection,
    group_name: str,
    control: StrictOrderingControl,
    operator: OperatorRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Stage one complete-identity ordering halt or floor audit.

    Args:
        connection: Live configured-level group-state transaction connection;
            the caller commits or rolls back.
        group_name: Exact group whose durable rows are accessed.
        control: Exact token-fenced strict ordering decision.
        operator: Trusted local operator audit context.
        expected_revision: Required compare-and-swap revision.

    Returns:
        Current or revision-advanced immutable ordering-decision snapshot.

    Raises:
        InvalidTransitionError: The live control identity, ordering state, or
            event action conflicts with durable state, or a new audit has an
            empty operator reason or nonlocal identity.
        RevisionConflictError: The expected revision differs.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    _validate_strict_live_control(connection, group_name, current, control.identity)
    durable_last_delivered = int(
        connection.execute(
            'SELECT last_delivered_id FROM group_state WHERE group_name = ?', (group_name,)
        ).fetchone()[0]
    )
    if control.last_delivered_id != durable_last_delivered:
        raise InvalidTransitionError('strict ordering last-delivered identity conflicts')
    if control.delivery_floor != current.delivery_floor:
        raise InvalidTransitionError('strict ordering delivery-floor identity conflicts')
    if control.action == 'halt':
        if (
            control.event_id <= control.delivery_floor
            or control.event_id >= control.last_delivered_id
        ):
            raise InvalidTransitionError('strict ordering anomaly event identity conflicts')
        if current.ordering_anomaly_id == control.event_id:
            return current
        if current.ordering_anomaly_id is not None:
            raise InvalidTransitionError('another ordering anomaly remains active')
        connection.execute(
            'UPDATE group_state SET ordering_anomaly_id = ? WHERE group_name = ?',
            (control.event_id, group_name),
        )
        bump_revision(connection, group_name, expected_revision)
        return snapshot(connection, group_name)
    return _record_strict_floor_skip(
        connection,
        group_name,
        current,
        _FloorSkipRequest(
            _command='strict-delivery-floor-skip',
            _event_id=control.event_id,
            _delivery_floor=control.delivery_floor,
            _operator=operator,
            _expected_revision=expected_revision,
        ),
    )


def recorded_bootstrap(
    connection: sqlite3.Connection,
    group_name: str,
    request: BootstrapRequest,
) -> GroupSnapshot | None:
    """Return the exact current-generation bootstrap receipt without changing progress.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose bootstrap receipt is requested.
        request: Captured positions and immutable policy for the requested
            generation.

    Returns:
        The recorded bootstrap snapshot when the current group generation,
        active lifecycle, and request match, or `None` when the group is
        absent.

    Raises:
        InvalidTransitionError: Existing generation, lifecycle, or bootstrap
            identity conflicts with the request.
        ValueError: The recorded snapshot JSON is invalid.
    """
    current = connection.execute(
        'SELECT group_generation, active, lifecycle FROM groups WHERE group_name = ?',
        (group_name,),
    ).fetchone()
    if current is None:
        return None
    if int(current[0]) != request.generation:
        raise InvalidTransitionError('initialize group generation conflicts')
    if not current[1] or current[2] != 'active':
        raise InvalidTransitionError('initialize group lifecycle conflicts')
    row = connection.execute(
        'SELECT command_fingerprint, result_snapshot_json FROM audit WHERE group_name = ? '
        "AND command = 'bootstrap' AND disposition_id = ?",
        (group_name, str(request.generation)),
    ).fetchone()
    fingerprint = _command_fingerprint('bootstrap', {'group': group_name, **asdict(request)})
    if row is None or str(row[0]) != fingerprint:
        raise InvalidTransitionError('initialize conflicts with existing group state')
    return snapshot_from_json(str(row[1]))


def bootstrap(
    connection: sqlite3.Connection,
    group_name: str,
    request: BootstrapRequest,
) -> GroupSnapshot:
    """Initialize group policy, positions, memberships and its retry receipt atomically.

    The caller commits or rolls back the FULL transaction. New initialization
    binds the group to open ranges and advances source index and range
    revisions.

    Args:
        connection: Live FULL group-state transaction connection.
        group_name: Exact group to initialize.
        request: Captured positions and immutable policy, including the
            verified source incarnation and requested group generation.

    Returns:
        The newly initialized snapshot or the exact current-generation
        bootstrap receipt.

    Raises:
        InvalidTransitionError: Existing group state conflicts with the
            request, or the verified source incarnation has changed.
        ValueError: A recorded snapshot JSON value is invalid.
    """
    recorded = recorded_bootstrap(connection, group_name, request)
    if recorded is not None:
        return recorded
    incarnation = connection.execute('SELECT stream_incarnation FROM source_meta').fetchone()
    if incarnation is None or incarnation[0] != request.verified_incarnation:
        raise InvalidTransitionError('bootstrap source incarnation changed')
    connection.execute(
        """INSERT INTO groups VALUES (?, ?, ?, ?, ?, ?, 1, 'active')""",
        (
            group_name,
            request.generation,
            request.ordering,
            request.site_server_url,
            request.verified_incarnation,
            request.holdback_secs,
        ),
    )
    connection.execute(
        'INSERT INTO group_state '
        '(group_name, revision, cursor, delivery_floor, last_delivered_id) '
        'VALUES (?, 0, ?, ?, ?)',
        (group_name, request.cursor, request.delivery_floor, request.last_delivered_id),
    )
    _bind_open_ranges(connection, group_name)
    connection.execute(BUMP_INDEX_REVISION)
    connection.execute(BUMP_RANGE_REVISION)
    result = snapshot(connection, group_name)
    _record_audit(
        connection,
        group_name,
        _AuditEntry(
            _command='bootstrap',
            _operator=OperatorRequest('initialize group', AuditIdentity.current(), time.time()),
            _prior_revision=-1,
            _disposition_id=str(request.generation),
            _command_fingerprint=_command_fingerprint(
                'bootstrap', {'group': group_name, **asdict(request)}
            ),
            _terminal_result='applied',
            _result_snapshot_json=snapshot_to_json(result),
        ),
    )
    return result


def recorded_disposition(
    connection: sqlite3.Connection,
    group_name: str,
    request: DispositionRequest,
) -> GroupSnapshot | None:
    """Return the latest exact disposition receipt, rejecting stale identity reuse.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose receipt is requested.
        request: Replay or reset request carrying the disposition identity and
            semantic command fingerprint.

    Returns:
        The recorded snapshot without current revision or generation checks, or
        `None` when no receipt exists.

    Raises:
        InvalidTransitionError: The disposition identity has different command
            content or a newer disposition has committed.
        ValueError: The recorded snapshot JSON is invalid.
    """
    row = connection.execute(
        """SELECT command_fingerprint, result_snapshot_json, audit_id FROM audit
        WHERE group_name = ? AND disposition_id = ?
        AND command IN ('replay', 'reset-to-latest')""",
        (group_name, request.disposition_id),
    ).fetchone()
    if row is None:
        return None
    if str(row[0]) != _disposition_fingerprint(request):
        raise InvalidTransitionError('disposition identity has conflicting content')
    latest = connection.execute(
        """SELECT audit_id FROM audit
        WHERE group_name = ? AND command IN ('replay', 'reset-to-latest')
        ORDER BY audit_id DESC LIMIT 1""",
        (group_name,),
    ).fetchone()
    if latest is None or int(row[2]) != int(latest[0]):
        raise InvalidTransitionError('intervening disposition makes retry stale')
    return snapshot_from_json(str(row[1]))


def dispose(
    connection: sqlite3.Connection,
    group_name: str,
    request: DispositionRequest,
    *,
    target: DispositionTarget,
) -> GroupSnapshot:
    """Apply one complete replay or reset within the caller's FULL transaction.

    Exact recorded retries return their historical result before current
    revision and source-authority checks. New work replaces the generation,
    positions, and recovery evidence and records the disposition atomically.
    The caller commits or rolls back.

    Args:
        connection: Live FULL group-state transaction connection.
        group_name: Exact group named by the request.
        request: Revision-fenced replay or reset with a nonempty disposition
            identity, increasing reserved generation, and trusted local audit
            context. Replay supplies an inclusive starting event id; reset
            supplies no replay position.
        target: Configured replacement policy and expected source authority.

    Returns:
        The replacement snapshot or the exact historical receipt.

    Raises:
        InvalidTransitionError: Command identity or source authority conflicts;
            generation, replay bounds, retained-history acknowledgement,
            parked/orphaned state, or in-flight recovery requirements are
            invalid; or the audit reason or local identity is invalid.
        RevisionConflictError: The expected revision differs for new work.
        ValueError: A recorded snapshot JSON value is invalid.
    """
    if request.group != group_name:
        raise InvalidTransitionError('disposition group identity conflicts')
    recorded = recorded_disposition(connection, group_name, request)
    if recorded is not None:
        return recorded
    check_revision(connection, group_name, request.expected_revision)
    normalized = _normalize_request(request, _check_disposition_source(connection, target))
    current = snapshot(connection, group_name)
    retirement = _validate_disposition(connection, group_name, current, normalized, target)
    fingerprint = _disposition_fingerprint(request)
    if retirement is not None:
        _record_audit(
            connection,
            group_name,
            _AuditEntry(
                _command=retirement,
                _operator=normalized._operator,
                _prior_revision=request.expected_revision,
                _event_id=current.in_flight_event_id,
                _disposition_id=request.disposition_id,
                _command_fingerprint=fingerprint,
                _result_snapshot_json=snapshot_to_json(current),
            ),
        )
    audit_id = _record_audit(
        connection,
        group_name,
        _AuditEntry(
            _command=request.command,
            _operator=normalized._operator,
            _prior_revision=request.expected_revision,
            _disposition_id=request.disposition_id,
            _command_fingerprint=fingerprint,
            _terminal_result='applied',
        ),
    )
    _install_disposition(connection, group_name, normalized, target)
    bump_revision(connection, group_name, request.expected_revision)
    connection.execute(BUMP_INDEX_REVISION)
    connection.execute(BUMP_RANGE_REVISION)
    result = snapshot(connection, group_name)
    _finish_audit(connection, group_name, audit_id, result)
    connection.execute(
        """INSERT INTO disposition_audit
        (disposition_id, group_name, command, command_fingerprint, old_generation,
        new_generation, cursor, frontier, stream_incarnation, terminal_result)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'applied')""",
        (
            request.disposition_id,
            group_name,
            request.command,
            fingerprint,
            current.group_generation,
            result.group_generation,
            result.cursor,
            normalized._source_frontier,
            target.verified_incarnation,
        ),
    )
    return result


def recorded_lifecycle(
    connection: sqlite3.Connection, group_name: str, request: LifecycleRequest
) -> GroupSnapshot | None:
    """Read an exact lifecycle receipt or validate the fence for new work.

    Historical exact retries return their recorded snapshot without mutating
    the current lifecycle or any newer supervisor reservation. Transition
    generation validity is checked even for an exact retry.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose lifecycle receipt is requested.
        request: Expected lifecycle and generation plus transition identity.
            The transition generation must not precede the expected generation
            and must increase it for a re-add.

    Returns:
        The historical exact receipt, or `None` after validating the current
        lifecycle and generation for new work.

    Raises:
        InvalidTransitionError: Transition generation or receipt identity is
            invalid, or new work conflicts with the current lifecycle,
            generation, or command preconditions.
        ValueError: The recorded snapshot JSON is invalid.
    """
    if request.transition_generation < request.expected_generation or (
        request.command == 'readd' and request.transition_generation == request.expected_generation
    ):
        raise InvalidTransitionError('lifecycle transition generation is invalid')
    row = connection.execute(
        'SELECT command_fingerprint, result_snapshot_json FROM audit '
        'WHERE group_name = ? AND command = ? AND disposition_id = ?',
        (group_name, request.command, str(request.transition_generation)),
    ).fetchone()
    if row is not None:
        if str(row[0]) != _lifecycle_fingerprint(group_name, request):
            raise InvalidTransitionError('lifecycle identity has conflicting content')
        return snapshot_from_json(str(row[1]))
    current = connection.execute(
        'SELECT group_generation, lifecycle FROM groups WHERE group_name = ?', (group_name,)
    ).fetchone()
    if current is None or int(current[0]) != request.expected_generation:
        raise InvalidTransitionError('lifecycle generation fence conflicts')
    if current[1] != request.expected_lifecycle:
        raise InvalidTransitionError('lifecycle fence conflicts')
    if request.command == 'readd' and request.expected_lifecycle != 'orphaned':
        raise InvalidTransitionError('re-add requires orphaned lifecycle')
    if request.command == 'orphan' and request.expected_lifecycle == 'orphaned':
        raise InvalidTransitionError('orphan requires an active or re-added lifecycle')
    return None


def change_lifecycle(
    connection: sqlite3.Connection, group_name: str, request: LifecycleRequest
) -> GroupSnapshot:
    """Apply one audited orphan or re-add in the caller's sole FULL transaction.

    Orphaning parks the group and collapses its obligation memberships into
    replay evidence. Re-adding leaves the group parked and inactive pending
    disposition. Exact recorded retries return before applying new work.
    The caller commits or rolls back.

    Args:
        connection: Live FULL group-state transaction connection.
        group_name: Exact group whose lifecycle changes.
        request: Generation- and lifecycle-fenced transition with trusted local
            audit identity.

    Returns:
        The changed snapshot or the exact historical lifecycle receipt.

    Raises:
        InvalidTransitionError: Transition identity or lifecycle/generation
            fences conflict, an event is active, or the audit identity is not
            local.
        ValueError: A recorded snapshot JSON value is invalid.
    """
    recorded = recorded_lifecycle(connection, group_name, request)
    if recorded is not None:
        return recorded
    current = snapshot(connection, group_name)
    if current.in_flight_event_id is not None:
        raise InvalidTransitionError('group lifecycle change requires no active event')
    if request.command == 'orphan':
        marker = source_obligations.orphan(connection, current)
        _purge_obligation_tables(connection, group_name, include_marks=False)
        connection.execute(
            'UPDATE group_state SET orphaned = 1, parked = 1, replay_required_from = ? '
            'WHERE group_name = ?',
            (marker.from_event_id, group_name),
        )
    else:
        connection.execute(
            "UPDATE groups SET lifecycle = 'readded-parked', active = 0 WHERE group_name = ?",
            (group_name,),
        )
        connection.execute(
            'UPDATE group_state SET orphaned = 0, parked = 1 WHERE group_name = ?', (group_name,)
        )
    refresh_operational_late_id_count(connection, group_name)
    bump_revision(connection, group_name, current.revision)
    connection.execute(BUMP_INDEX_REVISION)
    connection.execute(BUMP_RANGE_REVISION)
    result = snapshot(connection, group_name)
    _record_audit(
        connection,
        group_name,
        _AuditEntry(
            _command=request.command,
            _operator=OperatorRequest(
                'configured group ' + request.command, request.identity, request.wall_clock_time
            ),
            _prior_revision=current.revision,
            _disposition_id=str(request.transition_generation),
            _command_fingerprint=_lifecycle_fingerprint(group_name, request),
            _terminal_result='applied',
            _result_snapshot_json=snapshot_to_json(result),
        ),
    )
    return result


def resolve_conditional_park(
    connection: sqlite3.Connection,
    group_name: str,
    directive: ConditionalParkDirective,
    *,
    identity: AuditIdentity,
    decided_at_wall_clock: float,
) -> ConditionalParkResult:
    """Record or replay one group-owned conditional-park decision.

    Args:
        group_name: Exact group whose durable rows are accessed.
        connection: Live facade-owned transaction connection.
        directive: Exact immutable supervisor evidence.
        identity: Trusted local process identity for a parked-result audit.
        decided_at_wall_clock: Wall-clock time of the group-owned decision.

    Returns:
        The new or previously recorded typed result.

    Raises:
        InvalidTransitionError: If evidence conflicts with durable group
            authority.
    """
    fingerprint = directive.fingerprint()
    row = connection.execute(
        """
        SELECT group_generation, evidence_fingerprint, outcome,
               resulting_revision
        FROM conditional_park_results
        WHERE group_name = ? AND evidence_id = ?
        """,
        (group_name, directive.evidence_id),
    ).fetchone()
    if row is not None:
        if str(row[1]) != fingerprint:
            raise InvalidTransitionError('conditional park evidence id reuse')
        outcome = str(row[2])
        if int(row[0]) != directive.group_generation or outcome not in {
            'parked',
            'stale',
        }:
            raise InvalidTransitionError('conditional park recorded result conflicts')
        return ConditionalParkResult(
            evidence_id=directive.evidence_id,
            outcome=cast('ConditionalParkOutcome', outcome),
            resulting_revision=int(row[3]),
        )

    current = snapshot(connection, group_name)
    if current.site != directive.site or current.group != directive.group:
        raise InvalidTransitionError('conditional park group identity conflicts')
    if current.group_generation != directive.group_generation:
        raise InvalidTransitionError('conditional park group generation conflicts')
    if current.revision < directive.group_revision:
        raise InvalidTransitionError('conditional park revision regression')
    if current.revision > directive.group_revision:
        result = ConditionalParkResult(
            evidence_id=directive.evidence_id,
            outcome='stale',
            resulting_revision=current.revision,
        )
        _insert_conditional_park_result(
            connection, group_name, directive, result, decided_at_wall_clock
        )
        return result
    if current.in_flight_event_id != directive.event_id or current.parked:
        raise InvalidTransitionError('conditional park evidence is ambiguous')

    connection.execute(
        """
        UPDATE group_state
        SET parked = 1
        WHERE group_name = ?
        """,
        (group_name,),
    )
    _record_audit(
        connection,
        group_name,
        _AuditEntry(
            _command='conditional-park',
            _operator=OperatorRequest(
                reason='supervisor-overdue-lane',
                identity=identity,
                wall_clock_time=decided_at_wall_clock,
            ),
            _prior_revision=current.revision,
            _event_id=directive.event_id,
        ),
    )
    bump_revision(connection, group_name, current.revision)
    result = ConditionalParkResult(
        evidence_id=directive.evidence_id,
        outcome='parked',
        resulting_revision=current.revision + 1,
    )
    _insert_conditional_park_result(
        connection, group_name, directive, result, decided_at_wall_clock
    )
    return result


def restart_event(
    connection: sqlite3.Connection,
    group_name: str,
    request: ManifestRestartRequest,
    operator: OperatorRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Audit and restart one in-flight event under a complete manifest.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: Complete current callback manifest and its encoded byte limit.
        operator: Trusted local operator audit context.
        expected_revision: Required for a new transition. An exact recorded
            retry returns its historical snapshot before revision comparison.

    Returns:
        New or recorded immutable snapshot with fresh callback marks.

    Raises:
        InvalidTransitionError: No event is active or the manifest is empty,
            oversized, or has nonconsecutive ordinals, or a new audit has an
            empty operator reason or nonlocal identity.
        RevisionConflictError: The expected revision differs for an unrecorded
            transition.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    current = snapshot(connection, group_name)
    if current.in_flight_event_id is None:
        raise InvalidTransitionError('no event is in flight')
    command = 'restart-event'
    fingerprint, recorded = _operator_receipt(
        connection,
        group_name,
        _FingerprintInput(
            _command=command,
            _group_generation=current.group_generation,
            _operator=operator,
            _event_id=current.in_flight_event_id,
            _extra={'manifest': request.manifest.to_json()},
        ),
    )
    if recorded is not None:
        return recorded
    audit_id = _record_audit(
        connection,
        group_name,
        _AuditEntry(
            _command=command,
            _operator=operator,
            _prior_revision=expected_revision,
            _event_id=current.in_flight_event_id,
            _callback_ordinal=current.failure_ordinal,
            _command_fingerprint=fingerprint,
        ),
    )
    result_snapshot = delivery.restart_event_manifest(
        connection,
        group_name,
        request.manifest,
        expected_revision,
        request.max_manifest_bytes,
    )
    _finish_audit(connection, group_name, audit_id, result_snapshot)
    return result_snapshot


def retry(
    connection: sqlite3.Connection,
    group_name: str,
    request: CallbackRecoveryRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Audit an exact callback retry or required-event refetch.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: Exact event, optional callback, and operator context.
        expected_revision: Required for a new transition. An exact recorded
            retry returns its historical snapshot before revision comparison.

    Returns:
        New or recorded immutable retry snapshot.

    Raises:
        InvalidTransitionError: The request does not match the required event
            or failed callback, or a new audit has an empty operator reason or
            nonlocal identity.
        RevisionConflictError: The expected revision differs for an unrecorded
            transition.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    current = snapshot(connection, group_name)
    command = 'retry'
    fingerprint, recorded = _operator_receipt(
        connection,
        group_name,
        _FingerprintInput(
            _command=command,
            _group_generation=current.group_generation,
            _operator=request.operator,
            _event_id=request.event_id,
            _callback_ordinal=request.callback_ordinal,
        ),
    )
    if recorded is not None:
        return recorded
    check_revision(connection, group_name, expected_revision)
    if request.callback_ordinal is None:
        if current.required_event_id != request.event_id:
            raise InvalidTransitionError('retry does not match the required event')
        connection.execute(
            'UPDATE group_state SET retry_event_id = ? WHERE group_name = ?',
            (request.event_id, group_name),
        )
    else:
        if (
            current.in_flight_event_id != request.event_id
            or current.failure_code != 'callback-failed'
            or current.failure_ordinal != request.callback_ordinal
        ):
            raise InvalidTransitionError('retry does not match the failed callback')
        clear_failure(connection, group_name)
    return _audit_and_bump(
        connection,
        group_name,
        _AuditEntry(
            _command=command,
            _operator=request.operator,
            _prior_revision=expected_revision,
            _event_id=request.event_id,
            _callback_ordinal=request.callback_ordinal,
            _command_fingerprint=fingerprint,
        ),
    )


def skip_callback(
    connection: sqlite3.Connection,
    group_name: str,
    request: CallbackRecoveryRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Audit one exact callback skip and continue the manifest.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: Exact event, callback ordinal, and operator context.
        expected_revision: Required for a new transition. An exact recorded
            retry returns its historical snapshot before revision comparison.

    Returns:
        New or recorded immutable callback-skip snapshot. Skipping the final
        pending callback atomically completes the event, advances delivery
        positions, and clears in-flight event, manifest, and callback state.

    Raises:
        InvalidTransitionError: Recovery evidence blocks the skip or the
            request does not identify an attempted pending callback, or a new
            audit has an empty operator reason or nonlocal identity.
        RevisionConflictError: The expected revision differs for an unrecorded
            transition.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    if request.callback_ordinal is None:
        raise InvalidTransitionError('callback skip requires an exact callback ordinal')
    current = snapshot(connection, group_name)
    if 'shape-mismatch' in current.conditions:
        raise InvalidTransitionError('shape mismatch blocks callback skip')
    command = 'skip-callback'
    fingerprint, recorded = _operator_receipt(
        connection,
        group_name,
        _FingerprintInput(
            _command=command,
            _group_generation=current.group_generation,
            _operator=request.operator,
            _event_id=request.event_id,
            _callback_ordinal=request.callback_ordinal,
        ),
    )
    if recorded is not None:
        return recorded
    check_revision(connection, group_name, expected_revision)
    if current.required_event_id is not None:
        raise InvalidTransitionError('required event blocks callback skip')
    if current.in_flight_event_id != request.event_id:
        raise InvalidTransitionError('skip does not match the in-flight event')
    mark = connection.execute(
        """
        SELECT outcome, delivery_attempt
        FROM callback_marks
        WHERE group_name = ? AND event_id = ? AND ordinal = ?
        """,
        (group_name, request.event_id, request.callback_ordinal),
    ).fetchone()
    if mark is None or str(mark[0]) != 'pending' or int(mark[1]) < 1:
        raise InvalidTransitionError('skip does not match an attempted pending callback')
    if current.failure_code is not None and current.failure_ordinal != request.callback_ordinal:
        raise InvalidTransitionError('another callback failure blocks skip')
    audit_id = _record_audit(
        connection,
        group_name,
        _AuditEntry(
            _command=command,
            _operator=request.operator,
            _prior_revision=expected_revision,
            _event_id=request.event_id,
            _callback_ordinal=request.callback_ordinal,
            _command_fingerprint=fingerprint,
        ),
    )
    connection.execute(
        (
            "UPDATE callback_marks SET outcome = 'skipped' WHERE "
            'group_name = ? AND event_id = ? AND ordinal = ?'
        ),
        (group_name, request.event_id, request.callback_ordinal),
    )
    clear_failure(connection, group_name)
    if (
        connection.execute(
            """
            SELECT 1 FROM callback_marks
            WHERE group_name = ? AND event_id = ? AND outcome = 'pending'
            LIMIT 1
            """,
            (group_name, request.event_id),
        ).fetchone()
        is None
    ):
        delivery.finish_terminal_event(connection, group_name)
    return _bump_and_finish_audit(connection, group_name, audit_id, expected_revision)


def terminal_disposition_identity(
    connection: sqlite3.Connection,
    group_name: str,
    group_generation: int,
) -> tuple[str, str, str] | None:
    """Return the exact terminal disposition proof for one generation.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        group_generation: Nonnegative generation to identify.

    Returns:
        The disposition id, command fingerprint, and terminal result, or `None`
        when the generation has no terminal proof.

    Raises:
        InvalidTransitionError: Multiple terminal identities exist for the
            generation.
        ValueError: The group generation is not a nonnegative integer or
            restored durable snapshot JSON is invalid.
    """
    if type(group_generation) is not int or group_generation < 0:
        raise ValueError('group generation must be a nonnegative integer')
    identities: set[tuple[str, str, str]] = set()
    for row in connection.execute(
        """
        SELECT disposition_id, command_fingerprint,
               terminal_result, result_snapshot_json
        FROM audit
        WHERE group_name = ? AND command IN ('replay', 'reset-to-latest')
          AND disposition_id IS NOT NULL
          AND command_fingerprint IS NOT NULL
          AND terminal_result IS NOT NULL
          AND result_snapshot_json IS NOT NULL
        """,
        (group_name,),
    ).fetchall():
        if snapshot_from_json(str(row[3])).group_generation != group_generation:
            continue
        identities.add((str(row[0]), str(row[1]), str(row[2])))
    if not identities:
        return None
    if len(identities) != 1:
        raise InvalidTransitionError('terminal disposition rows conflict')
    return identities.pop()


def unpark(
    connection: sqlite3.Connection,
    group_name: str,
    operator: OperatorRequest,
    expected_revision: int,
) -> GroupSnapshot:
    """Clear only park evidence inside a live transaction.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        operator: Trusted local operator audit context.
        expected_revision: Required for a new transition. An exact recorded
            retry returns its historical snapshot before revision comparison.

    Returns:
        New or recorded immutable unparked snapshot.

    Raises:
        InvalidTransitionError: The group is not parked, or a new audit has an
            empty operator reason or nonlocal identity.
        RevisionConflictError: The expected revision differs for an unrecorded
            transition.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    current = snapshot(connection, group_name)
    command = 'unpark'
    fingerprint, recorded = _operator_receipt(
        connection,
        group_name,
        _FingerprintInput(
            _command=command,
            _group_generation=current.group_generation,
            _operator=operator,
            _event_id=current.in_flight_event_id,
        ),
    )
    if recorded is not None:
        return recorded
    check_revision(connection, group_name, expected_revision)
    if not current.parked:
        raise InvalidTransitionError('group is not parked')
    connection.execute(
        """
        UPDATE group_state
        SET parked = 0
        WHERE group_name = ?
        """,
        (group_name,),
    )
    return _audit_and_bump(
        connection,
        group_name,
        _AuditEntry(
            _command=command,
            _operator=operator,
            _prior_revision=expected_revision,
            _event_id=current.in_flight_event_id,
            _command_fingerprint=fingerprint,
        ),
    )


def validate_disposition(
    connection: sqlite3.Connection,
    group_name: str,
    request: DispositionRequest,
    target: DispositionTarget,
) -> None:
    """Check a prospective new disposition without changing durable state.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        request: New replay or reset request with a higher generation.
        target: Source identity that the durable transition would install.

    Raises:
        InvalidTransitionError: Group recovery state, generation, pending work,
            or replay bounds prevent the transition.
    """
    _validate_disposition(
        connection,
        group_name,
        snapshot(connection, group_name),
        _normalize_request(request, _check_disposition_source(connection, target)),
        target,
    )


def validate_live_join(
    connection: sqlite3.Connection,
    group_name: str,
    identity: LiveJoinIdentity,
    expected_revision: int,
) -> GroupSnapshot:
    """Validate one live join without mutating durable group state.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        identity: Exact eager or strict live-join identity.
        expected_revision: Required current revision.

    Returns:
        Current immutable snapshot when the live join is valid.

    Raises:
        InvalidTransitionError: The identity is stale or conflicting, the
            cursor is behind the boundary, or recovery work blocks the join.
        RevisionConflictError: The expected revision differs.
    """
    check_revision(connection, group_name, expected_revision)
    current = snapshot(connection, group_name)
    _validate_live_join_identity(current, identity)
    _validate_live_join_blockers(connection, group_name, current, identity)
    return current


def commit_live_join(
    connection: sqlite3.Connection,
    group_name: str,
    identity: LiveJoinIdentity,
    expected_revision: int,
) -> GroupSnapshot:
    """Commit one token-fenced eager or strict live join.

    A new commit clears operator-replay mode, retains exact-result replay only
    for the current committed token, and removes prior join receipts.

    Args:
        connection: Live group-state database connection.
        group_name: Exact group whose durable rows are accessed.
        identity: Exact eager or strict live-join identity.
        expected_revision: Required for a new join. An exact retry of the
            current retained token returns its historical snapshot before
            comparison.

    Returns:
        New or recorded immutable live-join snapshot.

    Raises:
        InvalidTransitionError: The token was reused for another identity, the
            identity is stale or conflicting, or recovery work blocks the join.
        RevisionConflictError: The expected revision differs for an unrecorded
            transition.
        ValueError: Restored durable snapshot JSON is invalid.
    """
    identity_json = _live_join_identity_json(identity)
    recorded = _recorded_live_join(connection, group_name, identity, identity_json)
    if recorded is not None:
        return recorded
    validate_live_join(connection, group_name, identity, expected_revision)
    connection.execute(
        """
        UPDATE group_state
        SET is_operator_replay = 0,
            latest_join_boundary = MAX(COALESCE(latest_join_boundary, ?), ?),
            strict_capacity_attempt_token = ?,
            strict_capacity_last_caused = NULL
        WHERE group_name = ?
        """,
        (identity.boundary, identity.boundary, identity.attempt_token, group_name),
    )
    bump_revision(connection, group_name, expected_revision)
    result_snapshot = snapshot(connection, group_name)
    connection.execute('DELETE FROM join_commits WHERE group_name = ?', (group_name,))
    connection.execute(
        """
        INSERT INTO join_commits (
            group_name, attempt_token, identity_json, result_revision, snapshot_json
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            group_name,
            identity.attempt_token,
            identity_json,
            result_snapshot.revision,
            snapshot_to_json(result_snapshot),
        ),
    )
    return result_snapshot
