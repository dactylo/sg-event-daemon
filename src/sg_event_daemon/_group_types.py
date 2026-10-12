"""Immutable values and canonical payloads for durable group state."""

from __future__ import annotations

import json
import os
import pwd
from dataclasses import dataclass
from math import isfinite
from typing import TYPE_CHECKING, Literal, TypeAlias, TypedDict, TypeGuard, Unpack, cast, get_args

from ._health import looks_secret
from ._identity import canonical_bytes, digest
from ._plugins import CallbackIdentity, CallbackManifest

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Self

_CALLBACK_FIELDS = frozenset(
    {
        'callback_id',
        'config_digest',
        'credential_fingerprint',
        'credential_name',
        'execution',
        'filter_digest',
        'ordinal',
        'plugin_id',
        'secret_refs',
    }
)
_CALLBACK_MARK_ITEM_COUNT = 5

_EPOCH_FIELDS = frozenset(
    {
        'captured_at',
        'eligibility_point',
        'epoch_id',
        'group_generation',
        'high_id',
        'low_id',
        'reader_clock_checkpoint',
        'source_generation',
        'state',
    }
)
GroupStatus: TypeAlias = Literal[
    'callback-failed',
    'event-unavailable',
    'history-truncated',
    'late-capacity-exhausted',
    'ordering-anomaly',
    'ordering-changed',
    'orphaned',
    'parked',
    'replay-required',
    'running',
    'shape-mismatch',
    'stream-identity-changed',
    'strict-capacity-exhausted',
]
GROUP_STATUSES: tuple[GroupStatus, ...] = get_args(GroupStatus)
_GROUP_STATUSES = frozenset(GROUP_STATUSES)
_MANIFEST_FIELDS = frozenset({'callbacks'})
_SECRET_REF_ITEM_COUNT = 2
_SNAPSHOT_REQUIRED_FIELDS = frozenset(
    {
        'callback_marks',
        'conditions',
        'cursor',
        'delivery_floor',
        'epochs',
        'failure_byte_count',
        'failure_code',
        'failure_digest',
        'failure_ordinal',
        'failure_text',
        'group',
        'group_generation',
        'history_truncated_floor',
        'in_flight_event_id',
        'in_flight_obligation_id',
        'is_operator_replay',
        'late_id_count',
        'latest_join_boundary',
        'manifest',
        'obligation_count',
        'ordering',
        'ordering_anomaly_id',
        'parked',
        'reader_clock_checkpoint',
        'replay_required_from',
        'required_event_id',
        'retry_event_id',
        'revision',
        'site',
        'source',
        'source_generation',
        'status',
        'stream_fingerprint',
        'strict_capacity_abort_streak',
        'strict_capacity_attempt_token',
        'verified_incarnation',
    }
)

ConditionalParkOutcome: TypeAlias = Literal['parked', 'stale']
StartMode: TypeAlias = Literal['initialize', 'resume']


@dataclass(frozen=True)
class AuditIdentity:
    """Carry trusted local process identity for an operator transition."""

    local_uid: int
    username: str
    pid: int

    @classmethod
    def current(cls) -> Self:
        """Capture identity from the current local process.

        Returns:
            The effective UID, system username (or numeric UID when no passwd
            entry exists), and current PID.
        """
        uid = os.geteuid()
        try:
            username = pwd.getpwuid(uid).pw_name
        except KeyError:
            username = str(uid)
        return cls(local_uid=uid, username=username, pid=os.getpid())


@dataclass(frozen=True)
class AuditRecord:
    """Contain one immutable operator-transition audit row."""

    site: str
    group: str
    event_id: int | None
    plugin_id: str | None
    callback_id: str | None
    command: str
    wall_clock_time: float
    prior_revision: int
    new_revision: int
    reason: str
    operator_label: str | None
    local_uid: int
    username: str
    pid: int
    disposition_id: str | None
    command_fingerprint: str | None
    terminal_result: str | None


@dataclass(frozen=True)
class CatchupEpoch:
    """Carry one body-free strict catch-up epoch record."""

    epoch_id: int
    low_id: int
    high_id: int
    captured_at: float
    eligibility_point: float
    reader_clock_checkpoint: float
    source_generation: str
    group_generation: int
    state: Literal['delivering', 'waiting']

    def __post_init__(self) -> None:
        """Normalize accepted numeric values before snapshot serialization."""
        _normalize_float_fields(
            self,
            ('captured_at', 'eligibility_point', 'reader_clock_checkpoint'),
        )


@dataclass(frozen=True)
class ConditionalParkDirective:
    """Carry one group-owned overdue-lane observation."""

    evidence_id: str
    site: str
    group: str
    group_generation: int
    failed_spawn_instance: str
    event_id: int
    group_revision: int
    observed_at_wall_clock: float

    def __post_init__(self) -> None:
        """Reject malformed or secret-looking recovery evidence at construction."""
        identities = (
            self.evidence_id,
            self.site,
            self.group,
            self.failed_spawn_instance,
        )
        if any(not value for value in identities):
            raise ValueError('conditional park directive identity must not be empty')
        if any(looks_secret(value) for value in identities):
            raise ValueError('conditional park directive contains secret-looking text')
        if any(
            type(value) is not int or value < 0
            for value in (self.group_generation, self.event_id, self.group_revision)
        ):
            raise ValueError('conditional park directive values are invalid')
        observed_at = self.observed_at_wall_clock
        try:
            valid_time = (
                type(observed_at) in {float, int} and isfinite(observed_at) and observed_at >= 0
            )
        except OverflowError:
            valid_time = False
        if not valid_time:
            raise ValueError('conditional park directive observation time is invalid')

    def fingerprint(self) -> str:
        """Hash every immutable evidence identity and value.

        Returns:
            Canonical SHA-256 evidence fingerprint.
        """
        return digest(
            canonical_bytes(
                {
                    'evidence_id': self.evidence_id,
                    'site': self.site,
                    'group': self.group,
                    'group_generation': self.group_generation,
                    'failed_spawn_instance': self.failed_spawn_instance,
                    'event_id': self.event_id,
                    'group_revision': self.group_revision,
                    'observed_at_wall_clock': self.observed_at_wall_clock,
                }
            )
        )


@dataclass(frozen=True)
class ConditionalParkResult:
    """Return one immutable group-owned conditional-park decision."""

    evidence_id: str
    outcome: ConditionalParkOutcome
    resulting_revision: int


@dataclass(frozen=True)
class GroupSnapshot:
    """Describe one immutable durable group-state revision.

    Each `callback_marks` entry has positions `(ordinal, plugin_id,
    callback_id, outcome, delivery_attempt)`. `status` projects the
    highest-precedence display condition, falling back to running, while
    `conditions` retains every independently applicable condition.
    `late_id_count` counts distinct `(obligation_id, event_id)` identities in
    this group's `late_ids` rows whose `terminal_state` is `pending`, rather
    than all retained evidence.
    """

    site: str
    source: str
    group: str
    group_generation: int
    revision: int
    status: GroupStatus
    conditions: tuple[str, ...]
    cursor: int
    delivery_floor: int
    verified_incarnation: str
    ordering: Literal['eager', 'strict']
    stream_fingerprint: str
    source_generation: str
    in_flight_event_id: int | None
    in_flight_obligation_id: str | None
    manifest: CallbackManifest | None
    required_event_id: int | None
    retry_event_id: int | None
    callback_marks: tuple[tuple[int, str, str, str, int], ...]
    failure_code: str | None
    failure_text: str | None
    failure_byte_count: int | None
    failure_digest: str | None
    failure_ordinal: int | None
    parked: bool
    is_operator_replay: bool
    replay_required_from: int | None
    history_truncated_floor: int | None
    ordering_anomaly_id: int | None
    strict_capacity_abort_streak: int
    strict_capacity_attempt_token: str | None
    reader_clock_checkpoint: float
    latest_join_boundary: int | None
    obligation_count: int
    late_id_count: int
    epochs: tuple[CatchupEpoch, ...]

    def __post_init__(self) -> None:
        """Normalize accepted numeric values before durable serialization."""
        _normalize_float_fields(self, ('reader_clock_checkpoint',))


@dataclass(frozen=True)
class LiveJoinIdentity:
    """Identify one non-reusable eager or strict live-join attempt."""

    site: str
    source: str
    group: str
    join_kind: Literal['eager', 'strict']
    source_generation: str
    group_generation: int
    boundary: int
    attempt_token: str

    def wire_payload(self) -> dict[str, object]:
        """Return the complete event-channel identity payload.

        Returns:
            Existing protocol-version-one identity fields in canonical order.
        """
        return {
            'attempt_token': self.attempt_token,
            'boundary': self.boundary,
            'group': self.group,
            'group_generation': self.group_generation,
            'join_kind': self.join_kind,
            'site': self.site,
            'source': self.source,
            'source_generation': self.source_generation,
        }


class _ReplayRequiredNoticeFields(TypedDict):
    """Type the complete undigested replay-required fields."""

    site: str
    source: str
    group: str
    group_generation: int
    source_generation: str
    marker_id: str
    from_event_id: int


@dataclass(frozen=True)
class ReplayRequiredNotice:
    """Carry one durable whole-group replay marker."""

    site: str
    source: str
    group: str
    group_generation: int
    source_generation: str
    marker_id: str
    from_event_id: int
    digest: str

    def canonical_bytes(self) -> bytes:
        """Return canonical notice bytes excluding the attached digest.

        Returns:
            Deterministic bytes covering the complete replay marker.
        """
        return canonical_bytes(
            {
                'site': self.site,
                'source': self.source,
                'group': self.group,
                'group_generation': self.group_generation,
                'source_generation': self.source_generation,
                'marker_id': self.marker_id,
                'from_event_id': self.from_event_id,
            }
        )

    @classmethod
    def create(
        cls,
        **fields: Unpack[_ReplayRequiredNoticeFields],
    ) -> Self:
        """Build one replay notice with its canonical digest.

        Args:
            fields: Complete immutable replay-marker fields excluding the
                digest.

        Returns:
            An immutable canonically digested replay notice.
        """
        command = cls(**fields, digest='')
        object.__setattr__(command, 'digest', digest(command.canonical_bytes()))
        return command


def _callback_marks_from_payload(
    payload: Mapping[str, object],
    message: str,
) -> tuple[tuple[int, str, str, str, int], ...]:
    """Restore exact five-item callback-mark arrays."""
    value = payload['callback_marks']
    if type(value) is not list:
        raise ValueError(message)
    marks: list[tuple[int, str, str, str, int]] = []
    for raw_mark in cast('list[object]', value):
        if type(raw_mark) is not list:
            raise ValueError(message)
        mark = cast('list[object]', raw_mark)
        if len(mark) != _CALLBACK_MARK_ITEM_COUNT:
            raise ValueError(message)
        if (
            type(mark[0]) is not int
            or type(mark[1]) is not str
            or type(mark[2]) is not str
            or type(mark[3]) is not str
            or type(mark[4]) is not int
        ):
            raise ValueError(message)
        marks.append(
            (
                mark[0],
                mark[1],
                mark[2],
                mark[3],
                mark[4],
            )
        )
    return tuple(marks)


def _normalize_float_fields(instance: object, fields: tuple[str, ...]) -> None:
    """Normalize accepted integer values in floating-point record fields."""
    for field in fields:
        value = getattr(instance, field)
        if type(value) is int:
            object.__setattr__(instance, field, float(value))


def _payload_bool(payload: Mapping[str, object], key: str, message: str) -> bool:
    """Return one exact Boolean payload field."""
    value = payload[key]
    if type(value) is not bool:
        raise ValueError(message)
    return value


def _payload_float(payload: Mapping[str, object], key: str, message: str) -> float:
    """Return one exact floating-point payload field."""
    value = payload[key]
    if type(value) is not float or not isfinite(value):
        raise ValueError(message)
    return value


def _payload_int(payload: Mapping[str, object], key: str, message: str) -> int:
    """Return one exact integer payload field without accepting Boolean values."""
    value = payload[key]
    if type(value) is not int:
        raise ValueError(message)
    return value


def _payload_object(
    value: object,
    fields: frozenset[str],
    message: str,
) -> dict[str, object]:
    """Return one JSON object containing exactly the required fields."""
    if type(value) is not dict:
        raise TypeError(message)
    payload = cast('dict[str, object]', value)
    actual = frozenset(payload)
    if actual != fields:
        raise ValueError(message)
    return payload


def _payload_optional_int(
    payload: Mapping[str, object],
    key: str,
    message: str,
) -> int | None:
    """Return one exact optional integer payload field."""
    value = payload[key]
    if value is None:
        return None
    if type(value) is not int:
        raise ValueError(message)
    return value


def _payload_optional_str(
    payload: Mapping[str, object],
    key: str,
    message: str,
) -> str | None:
    """Return one exact optional string payload field."""
    value = payload[key]
    if value is None:
        return None
    if type(value) is not str:
        raise ValueError(message)
    return value


def _payload_str(payload: Mapping[str, object], key: str, message: str) -> str:
    """Return one exact string payload field."""
    value = payload[key]
    if type(value) is not str:
        raise ValueError(message)
    return value


def _payload_literal(
    payload: Mapping[str, object],
    key: str,
    values: frozenset[str],
    message: str,
) -> str:
    """Return one exact string payload field from a closed literal set."""
    value = _payload_str(payload, key, message)
    if value not in values:
        raise ValueError(message)
    return value


def _epochs_from_payload(
    payload: Mapping[str, object],
    message: str,
) -> tuple[CatchupEpoch, ...]:
    """Restore exact closed epoch objects."""
    value = payload['epochs']
    if type(value) is not list:
        raise ValueError(message)
    epochs: list[CatchupEpoch] = []
    for raw_epoch in cast('list[object]', value):
        epoch = _payload_object(raw_epoch, _EPOCH_FIELDS, message)
        epochs.append(
            CatchupEpoch(
                epoch_id=_payload_int(epoch, 'epoch_id', message),
                low_id=_payload_int(epoch, 'low_id', message),
                high_id=_payload_int(epoch, 'high_id', message),
                captured_at=_payload_float(epoch, 'captured_at', message),
                eligibility_point=_payload_float(epoch, 'eligibility_point', message),
                reader_clock_checkpoint=_payload_float(
                    epoch,
                    'reader_clock_checkpoint',
                    message,
                ),
                source_generation=_payload_str(epoch, 'source_generation', message),
                group_generation=_payload_int(epoch, 'group_generation', message),
                state=cast(
                    "Literal['delivering', 'waiting']",
                    _payload_literal(
                        epoch,
                        'state',
                        frozenset({'delivering', 'waiting'}),
                        message,
                    ),
                ),
            )
        )
    return tuple(epochs)


def _payload_str_list(
    payload: Mapping[str, object],
    key: str,
    message: str,
) -> tuple[str, ...]:
    """Return one exact list of string payload fields."""
    value = payload[key]
    if type(value) is not list:
        raise ValueError(message)
    items = cast('list[object]', value)
    if any(type(item) is not str for item in items):
        raise ValueError(message)
    return tuple(cast('str', item) for item in items)


def _secret_refs_from_payload(
    payload: Mapping[str, object],
    message: str,
) -> tuple[tuple[str, str], ...]:
    """Restore exact two-item secret-reference arrays."""
    value = payload['secret_refs']
    if type(value) is not list:
        raise ValueError(message)
    references: list[tuple[str, str]] = []
    for raw_reference in cast('list[object]', value):
        if type(raw_reference) is not list:
            raise ValueError(message)
        reference = cast('list[object]', raw_reference)
        if len(reference) != _SECRET_REF_ITEM_COUNT:
            raise ValueError(message)
        if type(reference[0]) is not str or type(reference[1]) is not str:
            raise ValueError(message)
        references.append((reference[0], reference[1]))
    return tuple(references)


def _manifest_from_value(value: object, message: str) -> CallbackManifest:
    """Construct one manifest after its complete payload shape is validated."""
    payload = _payload_object(value, _MANIFEST_FIELDS, message)
    raw_callbacks = payload['callbacks']
    if type(raw_callbacks) is not list:
        raise ValueError(message)
    callbacks: list[CallbackIdentity] = []
    for expected_ordinal, raw_callback in enumerate(cast('list[object]', raw_callbacks)):
        callback = _payload_object(raw_callback, _CALLBACK_FIELDS, message)
        ordinal = _payload_int(callback, 'ordinal', message)
        if ordinal != expected_ordinal:
            raise ValueError(message)
        callbacks.append(
            CallbackIdentity(
                ordinal=ordinal,
                plugin_id=_payload_str(callback, 'plugin_id', message),
                callback_id=_payload_str(callback, 'callback_id', message),
                execution=cast(
                    "Literal['sync', 'async']",
                    _payload_literal(
                        callback,
                        'execution',
                        frozenset({'async', 'sync'}),
                        message,
                    ),
                ),
                filter_digest=_payload_str(callback, 'filter_digest', message),
                config_digest=_payload_str(callback, 'config_digest', message),
                secret_refs=_secret_refs_from_payload(callback, message),
                credential_name=_payload_str(callback, 'credential_name', message),
                credential_fingerprint=_payload_str(
                    callback,
                    'credential_fingerprint',
                    message,
                ),
            )
        )
    return CallbackManifest(callbacks=tuple(callbacks))


def is_start_mode(value: object) -> TypeGuard[StartMode]:
    """Check whether a value is an accepted group start mode."""
    return isinstance(value, str) and value in ('initialize', 'resume')


def manifest_from_json(value: str) -> CallbackManifest:
    """Restore one callback manifest from durable safe JSON.

    Args:
        value: Canonical callback-manifest JSON.

    Returns:
        The exact immutable callback manifest.

    Raises:
        ValueError: If the durable JSON does not have the exact manifest shape.
    """
    message = 'invalid callback manifest JSON'
    try:
        manifest = _manifest_from_value(json.loads(value), message)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(message) from exc
    else:
        return manifest


def _snapshot_from_payload(payload: Mapping[str, object], message: str) -> GroupSnapshot:
    """Construct one snapshot after its complete payload shape is validated."""
    manifest_value = payload['manifest']
    if manifest_value is not None and type(manifest_value) is not str:
        raise ValueError(message)
    return GroupSnapshot(
        site=_payload_str(payload, 'site', message),
        source=_payload_str(payload, 'source', message),
        group=_payload_str(payload, 'group', message),
        group_generation=_payload_int(payload, 'group_generation', message),
        revision=_payload_int(payload, 'revision', message),
        status=cast(
            'GroupStatus',
            _payload_literal(payload, 'status', _GROUP_STATUSES, message),
        ),
        conditions=_payload_str_list(payload, 'conditions', message),
        cursor=_payload_int(payload, 'cursor', message),
        delivery_floor=_payload_int(payload, 'delivery_floor', message),
        verified_incarnation=_payload_str(payload, 'verified_incarnation', message),
        ordering=cast(
            "Literal['eager', 'strict']",
            _payload_literal(
                payload,
                'ordering',
                frozenset({'eager', 'strict'}),
                message,
            ),
        ),
        stream_fingerprint=_payload_str(payload, 'stream_fingerprint', message),
        source_generation=_payload_str(payload, 'source_generation', message),
        in_flight_event_id=_payload_optional_int(payload, 'in_flight_event_id', message),
        in_flight_obligation_id=_payload_optional_str(
            payload,
            'in_flight_obligation_id',
            message,
        ),
        manifest=None if manifest_value is None else manifest_from_json(manifest_value),
        required_event_id=_payload_optional_int(payload, 'required_event_id', message),
        retry_event_id=_payload_optional_int(payload, 'retry_event_id', message),
        callback_marks=_callback_marks_from_payload(payload, message),
        failure_code=_payload_optional_str(payload, 'failure_code', message),
        failure_text=_payload_optional_str(payload, 'failure_text', message),
        failure_byte_count=_payload_optional_int(payload, 'failure_byte_count', message),
        failure_digest=_payload_optional_str(payload, 'failure_digest', message),
        failure_ordinal=_payload_optional_int(payload, 'failure_ordinal', message),
        parked=_payload_bool(payload, 'parked', message),
        is_operator_replay=_payload_bool(payload, 'is_operator_replay', message),
        replay_required_from=_payload_optional_int(payload, 'replay_required_from', message),
        history_truncated_floor=_payload_optional_int(
            payload,
            'history_truncated_floor',
            message,
        ),
        ordering_anomaly_id=_payload_optional_int(payload, 'ordering_anomaly_id', message),
        strict_capacity_abort_streak=_payload_int(
            payload, 'strict_capacity_abort_streak', message
        ),
        strict_capacity_attempt_token=_payload_optional_str(
            payload, 'strict_capacity_attempt_token', message
        ),
        reader_clock_checkpoint=_payload_float(payload, 'reader_clock_checkpoint', message),
        latest_join_boundary=_payload_optional_int(payload, 'latest_join_boundary', message),
        obligation_count=_payload_int(payload, 'obligation_count', message),
        late_id_count=_payload_int(payload, 'late_id_count', message),
        epochs=_epochs_from_payload(payload, message),
    )


def snapshot_from_json(value: str) -> GroupSnapshot:
    """Restore one previously recorded immutable receipt snapshot.

    Args:
        value: Canonical snapshot JSON.

    Returns:
        The exact immutable historical snapshot.

    Raises:
        ValueError: If the durable JSON does not have the exact snapshot shape.
    """
    message = 'invalid group snapshot JSON'
    try:
        payload = _payload_object(
            json.loads(value),
            _SNAPSHOT_REQUIRED_FIELDS,
            message,
        )
        snapshot = _snapshot_from_payload(payload, message)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(message) from exc
    else:
        return snapshot


def snapshot_to_json(snapshot: GroupSnapshot) -> str:
    """Serialize one receipt snapshot without event bodies or secrets.

    Args:
        snapshot: Immutable snapshot to preserve with a receipt.

    Returns:
        Canonical compact JSON.
    """
    return canonical_bytes(
        {
            'site': snapshot.site,
            'source': snapshot.source,
            'group': snapshot.group,
            'group_generation': snapshot.group_generation,
            'revision': snapshot.revision,
            'status': snapshot.status,
            'conditions': list(snapshot.conditions),
            'cursor': snapshot.cursor,
            'delivery_floor': snapshot.delivery_floor,
            'verified_incarnation': snapshot.verified_incarnation,
            'ordering': snapshot.ordering,
            'stream_fingerprint': snapshot.stream_fingerprint,
            'source_generation': snapshot.source_generation,
            'in_flight_event_id': snapshot.in_flight_event_id,
            'in_flight_obligation_id': snapshot.in_flight_obligation_id,
            'manifest': None if snapshot.manifest is None else snapshot.manifest.to_json(),
            'required_event_id': snapshot.required_event_id,
            'retry_event_id': snapshot.retry_event_id,
            'callback_marks': [list(mark) for mark in snapshot.callback_marks],
            'failure_code': snapshot.failure_code,
            'failure_text': snapshot.failure_text,
            'failure_byte_count': snapshot.failure_byte_count,
            'failure_digest': snapshot.failure_digest,
            'failure_ordinal': snapshot.failure_ordinal,
            'parked': snapshot.parked,
            'is_operator_replay': snapshot.is_operator_replay,
            'replay_required_from': snapshot.replay_required_from,
            'history_truncated_floor': snapshot.history_truncated_floor,
            'ordering_anomaly_id': snapshot.ordering_anomaly_id,
            'strict_capacity_abort_streak': snapshot.strict_capacity_abort_streak,
            'strict_capacity_attempt_token': snapshot.strict_capacity_attempt_token,
            'reader_clock_checkpoint': snapshot.reader_clock_checkpoint,
            'latest_join_boundary': snapshot.latest_join_boundary,
            'obligation_count': snapshot.obligation_count,
            'late_id_count': snapshot.late_id_count,
            'epochs': [
                {
                    'epoch_id': epoch.epoch_id,
                    'low_id': epoch.low_id,
                    'high_id': epoch.high_id,
                    'captured_at': epoch.captured_at,
                    'eligibility_point': epoch.eligibility_point,
                    'reader_clock_checkpoint': epoch.reader_clock_checkpoint,
                    'source_generation': epoch.source_generation,
                    'group_generation': epoch.group_generation,
                    'state': epoch.state,
                }
                for epoch in snapshot.epochs
            ],
        }
    ).decode()
