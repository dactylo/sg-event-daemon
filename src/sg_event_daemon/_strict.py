"""Bounded shared strict holdback and release."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, replace
from math import isfinite
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack

from ._fanout import (
    EncodedEvent,
    EventDelivery,
    GroupChannelInterface,
    StrictCapacityControl,
    StrictOrderingControl,
)
from ._source import (
    Attached,
    JoinAttemptIdentity,
    JoinCapacityError,
    JoinFinalized,
    JoinProtocolError,
    JoinRetry,
    JoinTarget,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping, Sequence

    from ._group_types import LiveJoinIdentity
    from ._source_store import DiscoveryRange

# This plan-level circuit-breaker value is intentionally not configuration.
JOIN_ABORT_LIMIT = 8


@dataclass(frozen=True)
class OrderingAnomaly:
    """Describe one body-free strict-ordering halt or delivery-floor skip."""

    group: str
    group_generation: int
    event_id: int
    last_delivered_id: int
    delivery_floor: int
    action: Literal['halt', 'skip']


@dataclass(frozen=True)
class _StrictObservation:
    """Retain one shared encoded payload and its conservative ordering time."""

    _event_id: int
    _encoded: EncodedEvent
    _ordering_time: float
    _range_ids: tuple[str, ...]


@dataclass(frozen=True)
class _StrictRelease:
    """Transfer one eligible shared payload to one strict group."""

    _group: str
    _event_id: int
    _encoded: EncodedEvent
    _ordering_time: float
    _is_operator_replay: bool


@dataclass(frozen=True)
class _StrictReleaseWatermark:
    """Describe one group's strict release and durable comparison progress."""

    _group: str
    _group_generation: int
    _holdback_secs: float
    _delivery_floor: int
    _released_through: int
    _last_delivered_id: int
    _is_operator_replay: bool
    _provisional: bool


class _AttachGroupOptions(TypedDict):
    """Type one restored body-free strict group policy."""

    holdback_secs: float
    delivery_floor: int
    last_delivered_id: NotRequired[int | None]
    halted: NotRequired[bool]
    is_operator_replay: NotRequired[bool]


@dataclass
class _JoinRecord:
    """Retain one token-specific strict join lifecycle."""

    _identity: LiveJoinIdentity
    _initial: JoinTarget | JoinRetry
    _phase: Literal['aborted', 'finalized', 'prepared', 'reserved']
    _sequence: int
    _channel: GroupChannelInterface | None = None
    _attached: Attached | None = None
    _finalized: JoinFinalized | None = None
    _abort_cause: Literal['capacity', 'non-capacity'] | None = None


@dataclass
class _PendingControl:
    """Retain one strict control until an exact configured-level lane commit is acknowledged."""

    _control: StrictCapacityControl | StrictOrderingControl
    _offered_channel: GroupChannelInterface | None = None


@dataclass
class _GroupReleaseState:
    """Hold one process-local joined or provisional strict release state."""

    _group: str
    _group_generation: int
    _holdback_secs: float
    _delivery_floor: int
    _released_through: int
    _last_delivered_id: int
    _is_operator_replay: bool
    _provisional: bool
    _channel: GroupChannelInterface | None
    _halted: bool
    _identity: LiveJoinIdentity | None
    _pending_controls: deque[_PendingControl]
    _capacity_pending: bool


class StrictBuffer:
    """Retain each strict payload once and schedule independent group release."""

    def __init__(
        self,
        *,
        max_events: int,
        max_bytes: int,
        group_holdbacks: Mapping[str, float] | None = None,
        group_delivery_floors: Mapping[str, int] | None = None,
        retry_after_secs: float = 1.0,
    ) -> None:
        """Create one bounded source-local strict payload authority.

        Args:
            max_events: Maximum distinct retained encoded events.
            max_bytes: Maximum complete framed bytes across those events.
            group_holdbacks: Finite positive holdbacks used by fixed-boundary
                joins.
            group_delivery_floors: Nonnegative integer floors used by joined
                decisions.
            retry_after_secs: Finite nonnegative delay in seconds returned when
                join headroom is low.

        Raises:
            ValueError: Event or byte limits are not positive integers, a
                holdback or floor is invalid, or the retry delay is not finite
                and nonnegative.
        """
        if type(max_events) is not int or max_events <= 0:
            raise ValueError('strict event limit must be positive')
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError('strict byte limit must be positive')
        if not isfinite(retry_after_secs) or retry_after_secs < 0:
            raise ValueError('strict join retry delay must be finite and nonnegative')
        holdbacks = {} if group_holdbacks is None else dict(group_holdbacks)
        delivery_floors = {} if group_delivery_floors is None else dict(group_delivery_floors)
        if any(not isfinite(value) or value <= 0 for value in holdbacks.values()):
            raise ValueError('strict group holdbacks must be finite and positive')
        if any(type(value) is not int or value < 0 for value in delivery_floors.values()):
            raise ValueError('strict group delivery floors must be nonnegative integers')
        self._acknowledged_controls: dict[
            str,
            StrictCapacityControl | StrictOrderingControl,
        ] = {}
        self._archived_watermarks: dict[str, _StrictReleaseWatermark] = {}
        self._byte_count = 0
        self._capacity_aborts: dict[str, int] = {}
        self._detached_halts: dict[str, bool] = {}
        self._group_holdbacks = holdbacks
        self._group_delivery_floors = delivery_floors
        self._groups: dict[str, _GroupReleaseState] = {}
        self._halt_reasons: dict[str, str] = {}
        self._join_records: dict[str, _JoinRecord] = {}
        self._join_sequence = 0
        self._max_bytes = max_bytes
        self._max_events = max_events
        self._observation_buffer: dict[int, _StrictObservation] = {}
        self._ordered_observation_ids: tuple[int, ...] | None = None
        self._pending_controls: dict[str, deque[_PendingControl]] = {}
        self._ranges: dict[str, _RangeState] = {}
        self._relief_action_buffer: deque[str] = deque(maxlen=100)
        self._retry_after_secs = float(retry_after_secs)

    def _insert_observation(self, observation: _StrictObservation) -> None:
        """Insert one new observation and invalidate the ordered key view."""
        self._observation_buffer[observation._event_id] = observation
        self._ordered_observation_ids = None

    def _observation_ids(self) -> tuple[int, ...]:
        """Return the shared ascending observation-key view."""
        ordered = self._ordered_observation_ids
        if ordered is None:
            ordered = tuple(sorted(self._observation_buffer))
            self._ordered_observation_ids = ordered
        return ordered

    def _provisional_release_state(
        self,
        identity: LiveJoinIdentity,
        channel: GroupChannelInterface,
        holdback: float,
        pending_controls: deque[_PendingControl],
    ) -> _GroupReleaseState:
        """Build one release state from durable policy and pending controls."""
        capacity_pending = any(
            isinstance(item._control, StrictCapacityControl) for item in pending_controls
        )
        control_halt_pending = capacity_pending or any(
            isinstance(item._control, StrictOrderingControl) and item._control.action == 'halt'
            for item in pending_controls
        )
        delivery_floor = self._group_delivery_floors.get(identity.group, identity.boundary)
        restored = self._archived_watermarks.get(identity.group)
        if restored is not None and restored._group_generation != identity.group_generation:
            raise JoinProtocolError('strict restored generation conflicts')
        comparison = max(identity.boundary, delivery_floor)
        replay = False
        restored_halt = False
        if restored is not None:
            comparison = max(comparison, restored._last_delivered_id)
            replay = restored._is_operator_replay
            restored_halt = self._detached_halts.get(identity.group, False)
        return _GroupReleaseState(
            _group=identity.group,
            _group_generation=identity.group_generation,
            _holdback_secs=holdback,
            _delivery_floor=delivery_floor,
            _released_through=comparison,
            _last_delivered_id=comparison,
            _is_operator_replay=replay,
            _provisional=True,
            _channel=channel,
            _halted=restored_halt or control_halt_pending,
            _identity=identity,
            _pending_controls=pending_controls,
            _capacity_pending=capacity_pending,
        )

    def _queue_control(
        self,
        state: _GroupReleaseState,
        control: StrictCapacityControl | StrictOrderingControl,
    ) -> None:
        """Retain one new exact control without duplicating acknowledged work."""
        if self._acknowledged_controls.get(state._group) == control:
            return
        pending = self._pending_controls.setdefault(state._group, deque())
        if any(item._control == control for item in pending):
            return
        pending.append(_PendingControl(control))

    def _range_blocks(
        self,
        state: _GroupReleaseState,
        observation: _StrictObservation,
        successful_time: float,
    ) -> bool:
        """Return whether range timing or an unresolved lower range blocks release."""
        for range_state in self._ranges.values():
            if (
                not range_state._closed
                and range_state._high_id > state._released_through
                and range_state._low_id < observation._event_id
            ):
                return True
        for obligation_id in observation._range_ids:
            range_state = self._ranges.get(obligation_id)
            if range_state is None:
                continue
            if not range_state._closed:
                return True
            point = range_state._eligibility_points.get(state._group)
            if point is not None and successful_time < point:
                return True
        return False

    def _ready(
        self,
        state: _GroupReleaseState,
        successful_time: float,
        *,
        limit: int | None,
    ) -> Iterator[_StrictRelease]:
        """Yield one currently eligible ascending prefix without mutation."""
        if state._halted:
            return
        release_count = 0
        for event_id in self._observation_ids():
            if event_id <= state._released_through:
                continue
            observation = self._observation_buffer[event_id]
            if successful_time < observation._ordering_time + state._holdback_secs:
                break
            if self._range_blocks(state, observation, successful_time):
                break
            yield _StrictRelease(
                _group=state._group,
                _event_id=event_id,
                _encoded=observation._encoded,
                _ordering_time=observation._ordering_time,
                _is_operator_replay=state._is_operator_replay,
            )
            release_count += 1
            if limit is not None and release_count >= limit:
                break

    def _rebind_pending_controls(
        self,
        group: str,
        identity: LiveJoinIdentity,
    ) -> deque[_PendingControl]:
        """Fence retained controls to one fresh exact live identity."""
        pending = self._pending_controls.setdefault(group, deque())
        if any(
            item._control.identity.group_generation > identity.group_generation for item in pending
        ):
            raise JoinProtocolError('strict retained control generation is in the future')
        retained = deque(
            item
            for item in pending
            if item._control.identity.group_generation == identity.group_generation
        )
        for item in retained:
            item._control = replace(item._control, identity=identity)
            item._offered_channel = None
        self._pending_controls[group] = retained
        return retained

    @property
    def _relief_actions(self) -> tuple[str, ...]:
        """Return deterministic capacity-relief actions."""
        return tuple(self._relief_action_buffer)

    def _remove_observation(self, event_id: int) -> _StrictObservation:
        """Remove one observation and invalidate the ordered key view."""
        observation = self._observation_buffer.pop(event_id)
        self._ordered_observation_ids = None
        return observation

    @staticmethod
    def _reset_control_offers(state: _GroupReleaseState) -> None:
        """Make every unacknowledged control eligible for a future channel."""
        for item in state._pending_controls:
            item._offered_channel = None

    def _retire_terminal_history(self, group: str) -> None:
        """Discard the superseded terminal token for one group."""
        for token, previous in tuple(self._join_records.items()):
            if previous._identity.group != group:
                continue
            if previous._phase not in {'aborted', 'finalized'}:
                raise JoinProtocolError('strict join history is not terminal')
            del self._join_records[token]

    def _transition_halt(
        self,
        group: str,
        *,
        halted: bool,
        reason: str | None = None,
        state: _GroupReleaseState | None = None,
    ) -> None:
        """Keep one group's halt state and process-local reason synchronized."""
        if state is not None:
            state._halted = halted
        self._detached_halts[group] = halted
        if not halted:
            self._halt_reasons.pop(group, None)
            return
        current_reason = reason if reason is not None else self._halt_reasons.get(group)
        self._halt_reasons[group] = (
            'strict-policy-halted' if current_reason is None else current_reason
        )

    def _capacity_abort(self, group: str) -> None:
        """Record one capacity-caused join failure and trip the fixed bound."""
        count = self._capacity_aborts.get(group, 0) + 1
        self._capacity_aborts[group] = count
        if count >= JOIN_ABORT_LIMIT:
            self._transition_halt(
                group,
                halted=True,
                reason='strict-capacity-exhausted',
                state=self._groups.get(group),
            )

    def _group_decision(
        self,
        state: _GroupReleaseState,
        event_id: int,
    ) -> OrderingAnomaly | None:
        """Return a new floor skip or decreasing-order halt."""
        if event_id <= state._delivery_floor:
            return OrderingAnomaly(
                group=state._group,
                group_generation=state._group_generation,
                event_id=event_id,
                last_delivered_id=state._last_delivered_id,
                delivery_floor=state._delivery_floor,
                action='skip',
            )
        comparison = max(state._last_delivered_id, state._released_through)
        if event_id < comparison:
            self._transition_halt(
                state._group,
                halted=True,
                reason='ordering-anomaly',
                state=state,
            )
            return OrderingAnomaly(
                group=state._group,
                group_generation=state._group_generation,
                event_id=event_id,
                last_delivered_id=comparison,
                delivery_floor=state._delivery_floor,
                action='halt',
            )
        return None

    @staticmethod
    def _project_watermark(state: _GroupReleaseState) -> _StrictReleaseWatermark:
        """Build one immutable group release projection."""
        return _StrictReleaseWatermark(
            _group=state._group,
            _group_generation=state._group_generation,
            _holdback_secs=state._holdback_secs,
            _delivery_floor=state._delivery_floor,
            _released_through=state._released_through,
            _last_delivered_id=state._last_delivered_id,
            _is_operator_replay=state._is_operator_replay,
            _provisional=state._provisional,
        )

    def _archive_group(self, state: _GroupReleaseState) -> None:
        """Retain one body-free terminal watermark after detachment."""
        self._archived_watermarks[state._group] = self._project_watermark(state)

    @property
    def _leases(self) -> tuple[LiveJoinIdentity, ...]:
        """Return active join-retention leases in request order."""
        return tuple(
            record._identity
            for record in sorted(
                self._join_records.values(),
                key=lambda item: item._sequence,
            )
            if record._phase in {'prepared', 'reserved'}
        )

    def _payload_needed(
        self,
        event_id: int,
        leases: tuple[LiveJoinIdentity, ...] | None = None,
    ) -> bool:
        """Return whether any joined group or leased boundary retains one id."""
        if any(
            not state._capacity_pending and event_id > state._released_through
            for state in self._groups.values()
        ):
            return True
        if leases is None:
            leases = self._leases
        return any(event_id > identity.boundary for identity in leases)

    @property
    def _observations(self) -> tuple[_StrictObservation, ...]:
        """Return retained observations in ascending event-id order."""
        return tuple(self._observation_buffer[event_id] for event_id in self._observation_ids())

    def _retention_floor(self) -> int | None:
        """Return the minimum active group or live lease boundary."""
        boundaries = [
            state._released_through
            for state in self._groups.values()
            if not state._capacity_pending
        ]
        boundaries.extend(identity.boundary for identity in self._leases)
        return min(boundaries, default=None)

    def _free_unused(self) -> None:
        """Discard payloads no remaining group or join floor needs."""
        retention_floor = self._retention_floor()
        removable = (
            list(self._observation_buffer)
            if retention_floor is None
            else [event_id for event_id in self._observation_buffer if event_id <= retention_floor]
        )
        for event_id in removable:
            observation = self._remove_observation(event_id)
            self._byte_count -= observation._encoded.byte_count

    def _detach(
        self,
        state: _GroupReleaseState,
        *,
        reason: str,
    ) -> None:
        """Detach one exact group while retaining only body-free status."""
        self._reset_control_offers(state)
        self._transition_halt(state._group, halted=state._halted, state=state)
        self._groups.pop(state._group, None)
        self._archive_group(state)
        if state._channel is not None:
            state._channel.detach(reason)
        self._free_unused()

    def _abort_capacity_join(self, record: _JoinRecord) -> None:
        """Abort one newest join for measured capacity relief."""
        self._relief_action_buffer.append(
            f'abort-join:{record._identity.group}:{record._identity.attempt_token}'
        )
        self._capacity_abort(record._identity.group)
        state = self._groups.get(record._identity.group)
        if state is not None and state._identity == record._identity:
            self._detach(state, reason='strict-buffer-capacity')
        elif record._channel is not None:
            record._channel.detach('strict-buffer-capacity')
        record._phase = 'aborted'
        record._abort_cause = 'capacity'

    def _ensure_capacity(self, encoded: EncodedEvent) -> bool:
        """Apply deterministic relief until one new distinct frame fits."""
        while (
            len(self._observation_buffer) + 1 > self._max_events
            or self._byte_count + encoded.byte_count > self._max_bytes
        ):
            active_joins = [
                record
                for record in self._join_records.values()
                if record._phase in {'prepared', 'reserved'}
            ]
            if active_joins:
                self._abort_capacity_join(max(active_joins, key=lambda item: item._sequence))
                self._free_unused()
                continue
            states = tuple(self._groups.values())
            if len(states) > 1:
                state = min(states, key=lambda item: (item._released_through, item._group))
                self._relief_action_buffer.append(f'detach-group:{state._group}')
                self._detach(state, reason='strict-buffer-capacity')
                continue
            if len(states) == 1:
                state = states[0]
                self._transition_halt(
                    state._group,
                    halted=True,
                    reason='strict-capacity-exhausted',
                    state=state,
                )
                self._relief_action_buffer.append(f'halt-group:{state._group}')
                if state._identity is not None and state._channel is not None:
                    if not state._capacity_pending:
                        self._queue_control(
                            state,
                            StrictCapacityControl(
                                identity=state._identity,
                                abort_limit=JOIN_ABORT_LIMIT,
                            ),
                        )
                    state._capacity_pending = True
                    self._free_unused()
                    return False
                self._detach(state, reason='strict-capacity-exhausted')
                continue
            return False
        return True

    def _observe(
        self,
        encoded: EncodedEvent,
        *,
        successful_time: float,
        range_ids: Sequence[str] = (),
    ) -> tuple[OrderingAnomaly, ...]:
        """Retain one payload once and record strict ordering decisions."""
        if not isfinite(successful_time) or successful_time < 0:
            raise ValueError('strict observation time must be finite and nonnegative')
        normalized_ranges = tuple(sorted(set(range_ids)))
        unknown = tuple(item for item in normalized_ranges if item not in self._ranges)
        if unknown:
            raise ValueError(f'strict observation names unknown ranges: {unknown!r}')
        existing = self._observation_buffer.get(encoded.event_id)
        range_time = min(
            (
                self._ranges[obligation_id]._opened_successful_at
                for obligation_id in normalized_ranges
            ),
            default=successful_time,
        )
        if existing is not None:
            if existing._encoded != encoded:
                raise ValueError('strict duplicate event body conflicts')
            combined = tuple(sorted(set(existing._range_ids) | set(normalized_ranges)))
            self._observation_buffer[encoded.event_id] = _StrictObservation(
                _event_id=encoded.event_id,
                _encoded=encoded,
                _ordering_time=min(existing._ordering_time, range_time),
                _range_ids=combined,
            )
            return ()
        decisions: list[OrderingAnomaly] = []
        for state in tuple(self._groups.values()):
            decision = self._group_decision(state, encoded.event_id)
            if decision is not None:
                decisions.append(decision)
                if state._identity is not None and state._channel is not None:
                    self._queue_control(
                        state,
                        StrictOrderingControl(
                            identity=state._identity,
                            event_id=decision.event_id,
                            last_delivered_id=decision.last_delivered_id,
                            delivery_floor=decision.delivery_floor,
                            action=decision.action,
                        ),
                    )
        if not self._payload_needed(encoded.event_id):
            return tuple(decisions)
        if not self._ensure_capacity(encoded) or not self._payload_needed(encoded.event_id):
            return tuple(decisions)
        self._insert_observation(
            _StrictObservation(
                _event_id=encoded.event_id,
                _encoded=encoded,
                _ordering_time=range_time,
                _range_ids=normalized_ranges,
            )
        )
        self._byte_count += encoded.byte_count
        return tuple(decisions)

    def _pump_controls(
        self,
        state: _GroupReleaseState,
        channel: GroupChannelInterface,
    ) -> bool:
        """Offer the pending body-free controls before any event frame."""
        if not state._pending_controls:
            return True
        pending = state._pending_controls[0]
        if pending._offered_channel is channel:
            return False
        if pending._offered_channel is not None:
            raise JoinProtocolError('strict control offer channel conflicts')
        if not channel.offer_paced(pending._control):
            if not channel.status.attached:
                self._detach(
                    state,
                    reason=channel.status.reason or 'channel-lost',
                )
            return False
        pending._offered_channel = channel
        return False

    def _pump_releases(
        self,
        state: _GroupReleaseState,
        channel: GroupChannelInterface,
        successful_time: float,
    ) -> tuple[_StrictRelease, ...]:
        """Fill available channel space with the ready ascending prefix."""
        if state._halted:
            return ()
        accepted: list[_StrictRelease] = []
        for release in self._ready(state, successful_time, limit=None):
            if not channel.offer_paced(EventDelivery(release._encoded)):
                if not channel.status.attached:
                    self._detach(
                        state,
                        reason=channel.status.reason or 'channel-lost',
                    )
                break
            state._released_through = release._event_id
            accepted.append(release)
        return tuple(accepted)

    def _pump_without_reclaim(
        self,
        group: str,
        successful_time: float,
    ) -> tuple[_StrictRelease, ...]:
        """Pump one strict group without the normal reclamation boundary."""
        state = self._groups.get(group)
        if state is None or state._channel is None:
            return ()
        channel = state._channel
        status = channel.status
        if not status.attached:
            self._detach(state, reason=status.reason or 'channel-lost')
            return ()
        if not self._pump_controls(state, channel):
            return ()
        return self._pump_releases(state, channel, successful_time)

    def _watermark(self, group: str) -> _StrictReleaseWatermark:
        """Return one group's live or last detached release projection."""
        state = self._groups.get(group)
        if state is not None:
            return self._project_watermark(state)
        archived = self._archived_watermarks.get(group)
        if archived is None:
            raise KeyError(group)
        return archived

    def abort(self, identity: LiveJoinIdentity) -> None:
        """Remove one exact provisional or joined strict attempt.

        Args:
            identity: Complete token-specific attempt identity.
        """
        record = self._join_records.get(identity.attempt_token)
        if record is None or record._identity != identity or record._phase == 'aborted':
            return
        state = self._groups.get(identity.group)
        if state is not None and state._identity == identity:
            self._detach(state, reason='strict-join-aborted')
        elif record._channel is not None:
            record._channel.detach('strict-join-aborted')
        record._phase = 'aborted'
        record._abort_cause = 'non-capacity'
        self._free_unused()

    def acknowledge_control(
        self,
        control: StrictCapacityControl | StrictOrderingControl,
    ) -> None:
        """Retire one exact control only after the configured-level lane commit.

        After that commit, acknowledging a capacity or ordering-halt control
        detaches the group. A floor-skip acknowledgement permits subsequent
        work without detachment.

        Args:
            control: Full current live identity and committed decision.

        Raises:
            JoinProtocolError: The control is stale, conflicts with retained
                identity, or was not offered.
        """
        record = self._join_records.get(control.identity.attempt_token)
        if record is None or record._identity != control.identity:
            raise JoinProtocolError('strict control acknowledgement is stale')
        if self._acknowledged_controls.get(control.identity.group) == control:
            return
        state = self._groups.get(control.identity.group)
        if state is None or state._identity != control.identity:
            raise JoinProtocolError('strict control acknowledgement is stale')
        pending = self._pending_controls.get(control.identity.group)
        if pending is None or not pending or pending[0]._control != control:
            raise JoinProtocolError('strict control acknowledgement conflicts')
        if pending[0]._offered_channel is None:
            raise JoinProtocolError('strict control was not offered')
        pending.popleft()
        self._acknowledged_controls[control.identity.group] = control
        capacity_control = isinstance(control, StrictCapacityControl)
        if capacity_control or control.action == 'halt':
            self._detach(
                state,
                reason=('strict-capacity-exhausted' if capacity_control else 'ordering-anomaly'),
            )

    @property
    def byte_count(self) -> int:
        """Return retained complete framed bytes.

        Returns:
            Exact sum across distinct shared payloads.
        """
        return self._byte_count

    def detach_group(self, group: str, *, reason: str) -> None:
        """Detach one exact group and free only now-unneeded payloads.

        Args:
            group: Stable joined identity.
            reason: Body-free channel disposition.
        """
        state = self._groups.get(group)
        if state is not None:
            self._detach(state, reason=reason)

    @property
    def event_count(self) -> int:
        """Return the number of distinct retained payloads.

        Returns:
            Shared payload count.
        """
        return len(self._observation_buffer)

    def finalize(self, identity: LiveJoinIdentity) -> JoinFinalized:
        """Promote one exact prepared queue and release watermark.

        Repeated finalization is idempotent only while retained token history
        contains the result. Reserving a fresh attempt retires prior terminal
        history, making older tokens stale.

        Args:
            identity: Complete token-specific attempt identity.

        Returns:
            Exact idempotent finalization acknowledgement.

        Raises:
            JoinCapacityError: If the attempt was aborted for insufficient
                capacity.
            JoinProtocolError: If the identity is stale or the lifecycle or
                provisional release state conflicts.
        """
        record = self._join_records.get(identity.attempt_token)
        if record is None or record._identity != identity:
            raise JoinProtocolError('strict join attempt is stale')
        if record._phase == 'aborted':
            if record._abort_cause == 'capacity':
                raise JoinCapacityError('strict join attempt exhausted capacity')
            raise JoinProtocolError('strict join attempt was aborted')
        if record._phase == 'finalized' and record._finalized is not None:
            return record._finalized
        if record._phase != 'prepared':
            raise JoinProtocolError('strict join attempt is not prepared')
        state = self._groups.get(identity.group)
        if state is None or state._identity != identity or not state._provisional:
            raise JoinProtocolError('strict provisional release state conflicts')
        state._provisional = False
        record._phase = 'finalized'
        record._finalized = JoinFinalized(identity)
        self._capacity_aborts[identity.group] = 0
        if (
            self._halt_reasons.get(identity.group) == 'strict-capacity-exhausted'
            and not state._capacity_pending
        ):
            self._transition_halt(identity.group, halted=False, state=state)
        else:
            self._transition_halt(identity.group, halted=state._halted, state=state)
        return record._finalized

    def halt_for_replay(self, group: str, generation: int) -> None:
        """Apply a durable replay halt only to the matching release generation.

        Args:
            group: Stable group identity whose pending rows collapsed.
            generation: Exact generation named by the durable replay marker.
        """
        state = self._groups.get(group)
        watermark = self._archived_watermarks.get(group)
        current = state if state is not None else watermark
        if current is None or current._group_generation != generation:
            return
        self._transition_halt(group, halted=True, reason='replay-required', state=state)

    def observe_many(
        self,
        encoded_events: Iterable[EncodedEvent],
        *,
        successful_time: float,
        range_ids: Mapping[int, Sequence[str]] | None = None,
    ) -> tuple[OrderingAnomaly, ...]:
        """Retain an ascending source chunk without duplicating payloads.

        Deterministic capacity relief may abort the newest active join, detach
        groups, or halt the remaining group. Relief can discard an incoming
        payload that still does not fit or is no longer needed. Processing is
        incremental; a later invalid observation does not roll back earlier
        work.

        Args:
            encoded_events: Complete immutable source chunk.
            successful_time: Finite nonnegative shared successful-query time in
                seconds.
            range_ids: Optional per-event identities already synchronized into
                the buffer.

        Returns:
            New body-free ordering decisions across the chunk.

        Raises:
            ValueError: Observation time is invalid, a range is unknown, or a
                duplicate event id carries a conflicting payload.
        """
        relationships: Mapping[int, Sequence[str]] = {} if range_ids is None else range_ids
        decisions: list[OrderingAnomaly] = []
        for encoded in encoded_events:
            decisions.extend(
                self._observe(
                    encoded,
                    successful_time=successful_time,
                    range_ids=relationships.get(encoded.event_id, ()),
                )
            )
        return tuple(decisions)

    def prepare(
        self,
        identity: LiveJoinIdentity,
        channel: GroupChannelInterface,
    ) -> Attached:
        """Prepare one exact provisional release queue after barriers.

        Repeated preparation on the same channel is idempotent only while
        retained token history contains the acknowledgement. Reserving a fresh
        attempt retires prior terminal history, making older tokens stale.

        Args:
            identity: Complete token-specific attempt identity.
            channel: Attempt-owned bounded strict channel.

        Returns:
            Exact idempotent provisional acknowledgement.

        Raises:
            JoinCapacityError: If the attempt was aborted for insufficient
                capacity.
            JoinProtocolError: If the identity is stale or the lifecycle,
                channel, or restored group policy conflicts.
        """
        record = self._join_records.get(identity.attempt_token)
        if record is None or record._identity != identity:
            raise JoinProtocolError('strict join attempt is stale')
        if record._phase == 'aborted':
            if record._abort_cause == 'capacity':
                raise JoinCapacityError('strict join attempt exhausted capacity')
            raise JoinProtocolError('strict join attempt was aborted')
        if record._phase in {'finalized', 'prepared'}:
            if record._channel is not channel or record._attached is None:
                raise JoinProtocolError('strict join channel conflicts')
            return record._attached
        if channel.group != identity.group or channel.ordering != 'strict':
            raise JoinProtocolError('strict join channel identity conflicts')
        if not channel.status.attached:
            raise JoinProtocolError('strict join channel is detached')
        holdback = self._group_holdbacks.get(identity.group)
        if holdback is None:
            raise JoinProtocolError('strict group holdback is unavailable')
        pending_controls = self._rebind_pending_controls(identity.group, identity)
        state = self._provisional_release_state(
            identity,
            channel,
            holdback,
            pending_controls,
        )
        self._groups[identity.group] = state
        self._transition_halt(identity.group, halted=state._halted, state=state)
        record._channel = channel
        record._attached = Attached(identity)
        record._phase = 'prepared'
        return record._attached

    def pump_many(
        self,
        groups: Iterable[str],
        successful_time: float,
    ) -> tuple[_StrictRelease, ...]:
        """Queue-pace a stable group sequence and reclaim once afterward.

        Args:
            groups: Stable group order to pump without re-sorting.
            successful_time: Current source successful-query time.

        Returns:
            Accepted releases in supplied group order.
        """
        releases: list[_StrictRelease] = []
        for group in groups:
            releases.extend(self._pump_without_reclaim(group, successful_time))
        self._free_unused()
        return tuple(releases)

    def range_ids_for(self, event_id: int) -> tuple[str, ...]:
        """Return durable ranges that classified one observed late id.

        Args:
            event_id: Exact EventLogEntry id.

        Returns:
            Stable range identities in lexical order.
        """
        return tuple(
            sorted(
                obligation_id
                for obligation_id, range_state in self._ranges.items()
                if event_id in range_state._late_ids
            )
        )

    def reserve(self, identity: LiveJoinIdentity) -> JoinTarget | JoinRetry:
        """Reserve one measured fixed-boundary strict retention lease.

        Args:
            identity: Source-captured complete live-join identity.

        Returns:
            Exact target when both half-headroom checks pass, otherwise a
            bounded fresh-token retry. An exact retained token returns its
            original result without measuring headroom again.

        Raises:
            JoinProtocolError: The identity is not a valid strict join, its
                token was reused for another identity, or the group already
                owns live release state.
        """
        if identity.join_kind != 'strict':
            raise JoinProtocolError('strict retention requires a strict join')
        if type(identity.boundary) is not int or identity.boundary < 0:
            raise JoinProtocolError('strict join boundary is invalid')
        if not identity.attempt_token:
            raise JoinProtocolError('strict join token must not be empty')
        existing = self._join_records.get(identity.attempt_token)
        if existing is not None:
            if existing._identity != identity:
                raise JoinProtocolError('strict join token was reused')
            return existing._initial
        if identity.group in self._groups or any(
            record._identity.group == identity.group and record._phase in {'prepared', 'reserved'}
            for record in self._join_records.values()
        ):
            raise JoinProtocolError('strict group already owns live release state')
        self._retire_terminal_history(identity.group)
        self._join_sequence += 1
        headroom_events = self._max_events - len(self._observation_buffer)
        headroom_bytes = self._max_bytes - self._byte_count
        enough = headroom_events * 2 >= self._max_events and headroom_bytes * 2 >= self._max_bytes
        if enough:
            initial: JoinTarget | JoinRetry = JoinTarget(identity)
            phase: Literal['aborted', 'reserved'] = 'reserved'
        else:
            attempt = JoinAttemptIdentity(
                site=identity.site,
                source=identity.source,
                group=identity.group,
                join_kind='strict',
                group_generation=identity.group_generation,
                attempt_token=identity.attempt_token,
            )
            initial = JoinRetry(
                attempt=attempt,
                retry_after_secs=self._retry_after_secs,
                headroom_events=headroom_events,
                headroom_bytes=headroom_bytes,
            )
            phase = 'aborted'
            self._capacity_abort(identity.group)
        self._join_records[identity.attempt_token] = _JoinRecord(
            _identity=identity,
            _initial=initial,
            _phase=phase,
            _sequence=self._join_sequence,
            _abort_cause='capacity' if isinstance(initial, JoinRetry) else None,
        )
        return initial

    def restore_group_policy(
        self,
        group: str,
        group_generation: int,
        **options: Unpack[_AttachGroupOptions],
    ) -> _StrictReleaseWatermark:
        """Restore durable group policy without attaching a live channel.

        Args:
            group: Stable group identity.
            group_generation: Exact durable generation.
            holdback_secs: Finite positive successful-query holdback in
                seconds.
            delivery_floor: Nonnegative durable generation delivery floor.
            last_delivered_id: Comparison point at or above the delivery floor;
                omission or `None` uses the delivery floor.
            halted: Whether durable callback delivery is halted.
            is_operator_replay: Whether the generation is an operator replay.

        Returns:
            Restored non-attached body-free watermark.

        Raises:
            ValueError: The group is already attached, or an identity,
                generation, holdback, floor, or comparison point is invalid.
        """
        holdback_secs = options['holdback_secs']
        delivery_floor = options['delivery_floor']
        comparison = options.get('last_delivered_id')
        halted = options.get('halted', False)
        is_operator_replay = options.get('is_operator_replay', False)
        if not group:
            raise ValueError('strict group identity must not be empty')
        if type(group_generation) is not int or group_generation < 0:
            raise ValueError('strict group generation must be nonnegative')
        if not isfinite(holdback_secs) or holdback_secs <= 0:
            raise ValueError('strict group holdback must be finite and positive')
        if type(delivery_floor) is not int or delivery_floor < 0:
            raise ValueError('strict delivery floor must be nonnegative')
        last_delivered_id = delivery_floor if comparison is None else comparison
        if type(last_delivered_id) is not int or last_delivered_id < delivery_floor:
            raise ValueError('strict last-delivered id conflicts with the floor')
        if group in self._groups:
            raise ValueError(f'strict group is already attached: {group}')
        watermark = _StrictReleaseWatermark(
            _group=group,
            _group_generation=group_generation,
            _holdback_secs=float(holdback_secs),
            _delivery_floor=delivery_floor,
            _released_through=last_delivered_id,
            _last_delivered_id=last_delivered_id,
            _is_operator_replay=is_operator_replay,
            _provisional=False,
        )
        self._archived_watermarks[group] = watermark
        self._transition_halt(group, halted=halted)
        self._group_holdbacks[group] = float(holdback_secs)
        self._group_delivery_floors[group] = delivery_floor
        return watermark

    def retention_health(self) -> tuple[tuple[str, int, int, int], ...]:
        """Snapshot each active group's independent shared-payload retention.

        Returns:
            Rows sorted by group containing `(group, retention_floor,
            retained_event_count, retained_byte_count)`. The floor is the
            minimum active release watermark or join-lease boundary for that
            group. Counts include retained payloads strictly above that floor;
            a payload needed by several groups counts for each.
            Owners with no needed payloads have zero counts. Groups without an
            active retention boundary are omitted.
        """
        floors = {
            state._group: state._released_through
            for state in self._groups.values()
            if not state._capacity_pending
        }
        for identity in self._leases:
            floors[identity.group] = min(
                floors.get(identity.group, identity.boundary),
                identity.boundary,
            )
        event_ids = self._observation_ids()
        remaining_events = len(event_ids)
        remaining_bytes = self._byte_count
        index = 0
        rows: list[tuple[str, int, int, int]] = []
        for group, floor in sorted(floors.items(), key=lambda item: item[1]):
            while index < len(event_ids) and event_ids[index] <= floor:
                remaining_bytes -= self._observation_buffer[event_ids[index]]._encoded.byte_count
                remaining_events -= 1
                index += 1
            rows.append((group, floor, remaining_events, remaining_bytes))
        return tuple(sorted(rows))

    def synchronize_ranges(self, ranges: Iterable[DiscoveryRange]) -> None:
        """Replace range timing from the sole durable `SourceState` projection.

        Args:
            ranges: Current body-free durable range snapshots.
        """
        synchronized: dict[str, _RangeState] = {}
        for item in ranges:
            synchronized[item.obligation_id] = _RangeState(
                _obligation_id=item.obligation_id,
                _low_id=item.low_id,
                _high_id=item.high_id,
                _opened_successful_at=item.opened_successful_at,
                _eligibility_points=dict(item.group_eligibility),
                _closed=item.state == 'closed',
                _late_ids=set(item.late_ids),
            )
        self._ranges = synchronized


@dataclass
class _RangeState:
    """Retain one body-free range's conservative timing and closure state."""

    _obligation_id: str
    _low_id: int
    _high_id: int
    _opened_successful_at: float
    _eligibility_points: dict[str, float]
    _closed: bool
    _late_ids: set[int] = field(default_factory=set[int])
