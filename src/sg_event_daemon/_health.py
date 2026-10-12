"""Immutable in-memory worker health and status aggregation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from itertools import pairwise
from math import isfinite
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Literal,
    NotRequired,
    TypeAlias,
    TypedDict,
    Unpack,
    cast,
    get_args,
    overload,
)

from . import _logging as logging_support
from ._watchdog import GROUP_PHASES, WATCHDOG_RULES, GroupPhase

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping, Sequence

    from ._lane import LaneHealth
    from ._source import SourceHaltStatus, SourceHealth
    from ._watchdog import WatchdogKind, WatchdogPhase

_PrimaryState: TypeAlias = Literal[
    'worker-breaker-open',
    'stale',
    'stream-identity-changed',
    'ordering-changed',
    'event-unavailable',
    'history-truncated',
    'ordering-anomaly',
    'capacity-exhausted',
    'callback-failed',
    'source-degraded',
    'catching-up',
    'parked',
    'idle',
    'healthy',
]
HEALTH_LATE_ID_LIMIT = 100
HEALTH_SECRET_TEXT_TERMS = ('api_key=', 'apikey=', 'password=', 'secret=', 'token=')
HEALTH_TEXT_MAX_BYTES = 4096
_HEALTHY_STATES = frozenset({'healthy', 'idle', 'parked'})

_STATUS_PRECEDENCE: tuple[_PrimaryState, ...] = get_args(_PrimaryState)
_SOURCE_HALT_CONDITIONS: dict[SourceHaltStatus, str] = {
    'encoder-died': 'event-unavailable',
    'encoder-failed': 'event-unavailable',
    'encoder-shutdown': 'event-unavailable',
    'event-too-large': 'event-unavailable',
    'frame-too-large': 'event-unavailable',
    'late-capacity-exhausted': 'capacity-exhausted',
    'obligation-capacity-exhausted': 'capacity-exhausted',
    'range-capacity-exhausted': 'capacity-exhausted',
    'range-query-page-cap': 'capacity-exhausted',
    'stream-identity-changed': 'stream-identity-changed',
}


@dataclass(frozen=True)
class BreakerTransition:
    """Record one bounded worker-breaker history transition."""

    sequence: int
    worker_id: str
    state: Literal['closed', 'open']
    reason: str

    def __post_init__(self) -> None:
        """Bound the diagnostic transition reason."""
        object.__setattr__(self, 'reason', bound_health_text(self.reason))


@dataclass(frozen=True)
class EventSocketHealth:
    """Describe one source server or dependent client event socket."""

    source: str
    group: str | None
    role: Literal['server', 'client']
    connected: bool
    requested: int
    send: int
    receive: int
    queue_event_count: int
    queue_byte_count: int
    reason: str | None

    def __post_init__(self) -> None:
        """Validate socket telemetry and bound its diagnostic reason.

        Raises:
            ValueError: `source` is empty, `group` is an empty string, `role`
                is neither `server` nor `client`, or a socket-buffer or queue
                count is negative.
        """
        if not self.source or self.group == '':
            raise ValueError('event socket identity must not be empty')
        if self.role not in {'server', 'client'}:
            raise ValueError('event socket role is invalid')
        if any(
            value < 0
            for value in (
                self.requested,
                self.send,
                self.receive,
                self.queue_event_count,
                self.queue_byte_count,
            )
        ):
            raise ValueError('event socket counts must be nonnegative')
        object.__setattr__(self, 'reason', bound_health_text(self.reason))


@dataclass(frozen=True)
class WatchdogHealth:
    """Carry one compact live component watchdog projection.

    Every record requires a nonempty `site` and `worker_instance`. A `source`
    watchdog requires a source and no group; a `lane` watchdog requires both;
    an `operator` watchdog may include a group only with a source. Valid phases
    are source startup, provider-query, internal, wait, write-hold,
    write-lock-wait, and shutdown; lane startup, setup, callback, internal,
    idle, write-hold, write-lock-wait, and shutdown; and operator idle,
    internal, and shutdown. An idle operator has no deadline. Every other
    accepted kind and phase requires a finite nonnegative deadline.
    """

    kind: WatchdogKind
    site: str
    source: str | None
    group: str | None
    worker_instance: str
    phase: WatchdogPhase
    heartbeat_at: float
    deadline_at: float | None
    event_id: int | None = None
    plugin_id: str | None = None
    callback_id: str | None = None
    delivery_attempt: int | None = None
    operator_command: str | None = None

    def __post_init__(self) -> None:
        """Validate one closed component identity, phase, and deadline.

        Raises:
            TypeError: A required identity is not exactly a string or an
                optional identity is neither a string nor `None`.
            ValueError: Identity, phase, or deadline violates the class
                contract; an optional plugin, callback, or command identity is
                empty; an event id or delivery attempt is not an exact
                nonnegative integer; or a supplied time is not a finite
                nonnegative integer or float.
        """
        if any(
            type(value) is not str
            for value in (self.kind, self.site, self.worker_instance, self.phase)
        ):
            raise TypeError('watchdog required identity must be text')
        if any(
            value is not None and type(value) is not str
            for value in (
                self.source,
                self.group,
                self.plugin_id,
                self.callback_id,
                self.operator_command,
            )
        ):
            raise TypeError('watchdog optional identity must be text or None')
        _validate_watchdog_phase(self.kind, self.phase)
        _validate_watchdog_identity(
            self.kind,
            self.site,
            self.source,
            self.group,
            self.worker_instance,
        )
        if any(value == '' for value in (self.plugin_id, self.callback_id, self.operator_command)):
            raise ValueError('watchdog phase identity must not be empty')
        if self.event_id is not None and (type(self.event_id) is not int or self.event_id < 0):
            raise ValueError('watchdog event id must be a nonnegative integer')
        if self.delivery_attempt is not None and (
            type(self.delivery_attempt) is not int or self.delivery_attempt < 0
        ):
            raise ValueError('watchdog delivery attempt must be a nonnegative integer')
        _validate_health_time(self.heartbeat_at, 'watchdog heartbeat')
        _validate_watchdog_deadline(self.kind, self.phase, self.deadline_at)


@dataclass(frozen=True)
class WorkerHealth:
    """Describe one worker process, monitor, and owned threads."""

    site: str
    worker_id: str
    spawn_instance: str
    heartbeat_at: float
    monitor_alive: bool
    thread_names: tuple[str, ...]
    stuck_threads: tuple[str, ...]
    breaker_open: bool
    breaker_reason: str | None
    socket_buffer_requested: int
    socket_send_buffer: int
    socket_receive_buffer: int
    source_outages: tuple[tuple[str, str], ...] = ()
    heartbeat_age: float = 0.0
    watchdog_failure: str | None = None

    def __post_init__(self) -> None:
        """Validate worker clocks and bound diagnostic reason fields.

        Raises:
            ValueError: `heartbeat_at` or `heartbeat_age` is not a finite
                nonnegative integer or float.
        """
        _validate_health_time(self.heartbeat_at, 'worker heartbeat')
        _validate_health_time(self.heartbeat_age, 'worker heartbeat age')
        object.__setattr__(self, 'breaker_reason', bound_health_text(self.breaker_reason))
        object.__setattr__(self, 'watchdog_failure', bound_health_text(self.watchdog_failure))
        object.__setattr__(
            self,
            'source_outages',
            tuple((source, bound_health_text(reason)) for source, reason in self.source_outages),
        )


class _GroupHealthOptions(TypedDict):
    """Define complete durable group-health projection inputs."""

    queue_event_count: int
    queue_byte_count: int
    catchup_frontier: int | None
    pending_catchup_epoch_count: int
    oldest_catchup_epoch_age: float | None
    join_phase: str
    outstanding_late_obligation_count: int
    outstanding_late_id_count: int
    outstanding_late_ids: Iterable[int]
    outstanding_late_ids_truncated: bool
    parked: bool
    park_reason: str | None
    failure_kind: str | None
    failure_text: str | None
    checkpoint_state: str
    conditions: Iterable[str]
    audit_count: NotRequired[int]


@dataclass(frozen=True)
class GroupHealth:
    """Describe one complete immutable group status projection.

    `outstanding_late_ids` is a distinct ascending prefix of at most 100 late
    ids.
    `outstanding_late_id_count` is the total distinct late-id count and cannot
    be less than the prefix length. `outstanding_late_ids_truncated` is true
    exactly when the total exceeds the prefix length.
    """

    site: str
    source: str
    group: str
    worker_instance: str
    execution: Literal['sync', 'async']
    ordering: Literal['eager', 'strict']
    phase: GroupPhase
    status: str
    cursor: int
    current_event_id: int | None
    current_plugin_id: str | None
    current_callback_id: str | None
    delivery_attempt: int | None
    heartbeat_at: float
    heartbeat_age: float
    queue_event_count: int
    queue_byte_count: int
    catchup_frontier: int | None
    pending_catchup_epoch_count: int
    oldest_catchup_epoch_age: float | None
    join_phase: str
    outstanding_late_obligation_count: int
    outstanding_late_id_count: int
    outstanding_late_ids: tuple[int, ...]
    outstanding_late_ids_truncated: bool
    parked: bool
    park_reason: str | None
    failure_kind: str | None
    failure_text: str | None
    checkpoint_state: str
    conditions: tuple[str, ...]
    audit_count: int = 0
    deadline_at: float | None = None
    overdue_reason: str | None = None

    def __post_init__(self) -> None:
        """Validate phase telemetry and bound diagnostic detail.

        Raises:
            ValueError: The phase is unsupported; a supplied heartbeat or
                deadline is not a finite nonnegative integer or float;
                late-id values, count, or truncation flag violate the class
                contract; or `audit_count` is negative. Late ids and their
                count must be exact integers, and the truncation flag must be
                an exact boolean.
        """
        if self.phase not in GROUP_PHASES:
            raise ValueError('group health phase is invalid')
        _validate_health_time(self.heartbeat_at, 'group heartbeat')
        _validate_health_time(self.heartbeat_age, 'group heartbeat age')
        if self.deadline_at is not None:
            _validate_health_time(self.deadline_at, 'group deadline')
        late_ids = self.outstanding_late_ids
        late_id_count = self.outstanding_late_id_count
        if type(late_id_count) is not int or late_id_count < len(late_ids):
            raise ValueError('group late-id count is invalid')
        if len(late_ids) > HEALTH_LATE_ID_LIMIT:
            raise ValueError('group late-id prefix exceeds health limit')
        if any(type(late_id) is not int for late_id in late_ids) or any(
            first >= second for first, second in pairwise(late_ids)
        ):
            raise ValueError('group late-id prefix must be distinct and ascending')
        expected_truncated = late_id_count > len(late_ids)
        if (
            type(self.outstanding_late_ids_truncated) is not bool
            or self.outstanding_late_ids_truncated is not expected_truncated
        ):
            raise ValueError('group late-id truncation metadata is inconsistent')
        if self.audit_count < 0:
            raise ValueError('group telemetry scalars must be nonnegative')
        object.__setattr__(self, 'park_reason', bound_health_text(self.park_reason))
        object.__setattr__(self, 'failure_text', bound_health_text(self.failure_text))
        object.__setattr__(self, 'overdue_reason', bound_health_text(self.overdue_reason))

    @classmethod
    def from_lane(
        cls,
        lane: LaneHealth,
        **options: Unpack[_GroupHealthOptions],
    ) -> GroupHealth:
        """Combine authoritative lane state with durable queue/recovery state.

        Args:
            lane: Current live-lane health authority.
            queue_event_count: Complete queued message count.
            queue_byte_count: Complete queued frame bytes.
            catchup_frontier: Current detached catch-up frontier.
            pending_catchup_epoch_count: Persisted pending strict epochs.
            oldest_catchup_epoch_age: Supervisor-clock age of the oldest epoch.
            join_phase: Current live-join phase.
            outstanding_late_obligation_count: Unresolved range count.
            outstanding_late_id_count: Exact distinct unresolved late-id count.
            outstanding_late_ids: Bounded ascending unresolved late-id prefix.
            outstanding_late_ids_truncated: Whether the prefix omits late ids.
            parked: Whether the group is intentionally parked.
            park_reason: Body-free park detail.
            failure_kind: Setup or callback failure kind.
            failure_text: Bounded failure detail.
            checkpoint_state: Current database checkpoint condition.
            conditions: Every independently applicable status condition.
            audit_count: Cumulative group-owner audit rows.

        Returns:
            A complete immutable group projection.

        Raises:
            ValueError: A queue depth, pending epoch count, or outstanding
                obligation count is negative, or the resulting projection
                violates `GroupHealth` phase, time, late-id, or audit
                invariants.
        """
        if options['queue_event_count'] < 0 or options['queue_byte_count'] < 0:
            raise ValueError('group queue depths must be nonnegative')
        if (
            options['pending_catchup_epoch_count'] < 0
            or options['outstanding_late_obligation_count'] < 0
        ):
            raise ValueError('group recovery counts must be nonnegative')
        return cls(
            site=lane.site,
            source=lane.source,
            group=lane.group,
            worker_instance=lane.worker_instance,
            execution=lane.execution,
            ordering=lane.ordering,
            phase=lane.phase,
            status=lane.status,
            cursor=lane.cursor,
            current_event_id=lane.event_id,
            current_plugin_id=lane.plugin_id,
            current_callback_id=lane.callback_id,
            delivery_attempt=lane.delivery_attempt,
            heartbeat_at=lane.heartbeat_at,
            heartbeat_age=lane.heartbeat_age,
            queue_event_count=options['queue_event_count'],
            queue_byte_count=options['queue_byte_count'],
            catchup_frontier=options['catchup_frontier'],
            pending_catchup_epoch_count=options['pending_catchup_epoch_count'],
            oldest_catchup_epoch_age=options['oldest_catchup_epoch_age'],
            join_phase=options['join_phase'],
            outstanding_late_obligation_count=options['outstanding_late_obligation_count'],
            outstanding_late_id_count=options['outstanding_late_id_count'],
            outstanding_late_ids=tuple(options['outstanding_late_ids']),
            outstanding_late_ids_truncated=options['outstanding_late_ids_truncated'],
            parked=options['parked'],
            park_reason=options['park_reason'],
            failure_kind=options['failure_kind'],
            failure_text=options['failure_text'],
            checkpoint_state=options['checkpoint_state'],
            conditions=tuple(dict.fromkeys(options['conditions'])),
            audit_count=options.get('audit_count', 0),
            deadline_at=lane.deadline_at,
            overdue_reason=lane.overdue_reason,
        )


class _AggregateStatusOptions(TypedDict):
    """Define status aggregation inputs and optional cumulative telemetry."""

    sources: Iterable[SourceHealth]
    groups: Iterable[GroupHealth]
    workers: Iterable[WorkerHealth]
    now: float
    stale_after_secs: float
    audit_count: NotRequired[int]
    database_sizes: NotRequired[Iterable[tuple[str, int]]]
    breaker_history: NotRequired[Sequence[BreakerTransition]]
    breaker_transition_count: NotRequired[int]
    worker_breaker_open: NotRequired[bool]


@dataclass(frozen=True)
class StatusSnapshot:
    """Contain one complete ephemeral supervisor status snapshot.

    Attributes:
        generated_at: Time when this projection was generated.
        primary_state: Highest-priority aggregate state.
        conditions: Active aggregate condition labels.
        sources: Source health projections.
        groups: Group health projections.
        workers: Worker health projections.
        liveness: Whether the process is live.
        readiness: Whether the service is ready.
        audit_count: Cumulative audit record count.
        database_sizes: Named database sizes in bytes.
        breaker_history: Recent worker-breaker transitions.
        breaker_transition_count: Cumulative breaker transition count.
    """

    generated_at: float
    primary_state: _PrimaryState
    conditions: tuple[str, ...]
    sources: tuple[SourceHealth, ...]
    groups: tuple[GroupHealth, ...]
    workers: tuple[WorkerHealth, ...]
    liveness: bool
    readiness: bool
    audit_count: int
    database_sizes: tuple[tuple[str, int], ...]
    breaker_history: tuple[BreakerTransition, ...]
    breaker_transition_count: int

    def to_mapping(self) -> Mapping[str, object]:
        """Return a recursively copied structured status payload.

        Returns:
            Read-only top-level mapping containing every status field.
        """
        return MappingProxyType(cast('dict[str, object]', asdict(self)))


class _ProbeHealthOptions(TypedDict):
    """Define one complete in-memory probe health projection."""

    sources: Iterable[SourceHealth]
    groups: Iterable[GroupHealth]
    workers: Iterable[WorkerHealth]
    now: float
    stale_after_secs: float
    worker_breaker_open: NotRequired[bool]
    inventory_complete: NotRequired[bool]


def _conditions_ready(conditions: Sequence[str]) -> bool:
    """Return whether every ordered health condition is readiness-safe."""
    return not any(state not in _HEALTHY_STATES for state in conditions)


def _current_health_inputs(
    options: _ProbeHealthOptions,
) -> tuple[
    tuple[SourceHealth, ...],
    tuple[GroupHealth, ...],
    tuple[WorkerHealth, ...],
    tuple[str, ...],
]:
    """Refresh component ages and derive shared supervisor conditions."""
    now = options['now']
    current_sources = tuple(
        replace(source, heartbeat_age=max(0.0, now - source.heartbeat_at))
        for source in options['sources']
    )
    current_groups = tuple(
        replace(group, heartbeat_age=max(0.0, now - group.heartbeat_at))
        for group in options['groups']
    )
    current_workers = tuple(
        replace(worker, heartbeat_age=max(0.0, now - worker.heartbeat_at))
        for worker in options['workers']
    )
    extra_conditions = (
        *(('worker-breaker-open',) if options.get('worker_breaker_open', False) else ()),
        *(('stale',) if not options.get('inventory_complete', True) else ()),
    )
    return current_sources, current_groups, current_workers, extra_conditions


def _group_conditions(group: GroupHealth) -> set[str]:
    """Return every explicit and derived group condition."""
    conditions = set(group.conditions)
    if group.overdue_reason is not None:
        conditions.add('stale')
    if conditions.intersection(
        {
            'late-capacity-exhausted',
            'strict-capacity-exhausted',
        }
    ):
        conditions.difference_update(
            {
                'late-capacity-exhausted',
                'strict-capacity-exhausted',
            }
        )
        conditions.add('capacity-exhausted')
    if group.parked:
        conditions.add('parked')
    if group.failure_kind in {'callback', 'setup'}:
        conditions.add('callback-failed')
    if group.pending_catchup_epoch_count or group.join_phase in {
        'catching-up',
        'detached',
        'joining',
    }:
        conditions.add('catching-up')
    if group.phase == 'idle':
        conditions.add('idle')
    return conditions


def _source_conditions(source: SourceHealth) -> set[str]:
    """Return every derived source condition."""
    conditions: set[str] = set()
    if source.overdue_reason is not None:
        conditions.add('stale')
    status = source.status
    if status in _SOURCE_HALT_CONDITIONS:
        conditions.add(_SOURCE_HALT_CONDITIONS[status])
    elif status == 'uninitialized':
        conditions.add('source-degraded')
    if source.retry_attempt or source.next_retry_secs is not None or source.last_error is not None:
        conditions.add('source-degraded')
    return conditions


def _validate_health_time(value: float, name: str) -> None:
    """Reject a non-finite or negative health timestamp or age."""
    if type(value) not in {float, int} or not isfinite(value) or value < 0:
        raise ValueError(f'{name} must be finite and nonnegative')


def _validate_watchdog_deadline(
    kind: WatchdogKind,
    phase: WatchdogPhase,
    deadline_at: float | None,
) -> None:
    """Enforce the component phase's closed deadline shape."""
    if deadline_at is not None:
        _validate_health_time(deadline_at, 'watchdog deadline')
    if kind == 'operator' and phase == 'idle':
        if deadline_at is not None:
            raise ValueError('inactive operator watchdog must not have a deadline')
    elif deadline_at is None:
        raise ValueError('active watchdog phase requires a deadline')


def _validate_watchdog_identity(
    kind: WatchdogKind,
    site: str,
    source: str | None,
    group: str | None,
    worker_instance: str,
) -> None:
    """Validate one compact component and worker identity."""
    if not site or not worker_instance:
        raise ValueError('watchdog identity must not be empty')
    if source == '' or group == '':
        raise ValueError('watchdog component identity must not be empty')
    if kind == 'source' and (source is None or group is not None):
        raise ValueError('source watchdog identity is invalid')
    if kind == 'lane' and (source is None or group is None):
        raise ValueError('lane watchdog identity is invalid')
    if kind == 'operator' and group is not None and source is None:
        raise ValueError('operator watchdog group requires a source')


def _validate_watchdog_phase(kind: WatchdogKind, phase: WatchdogPhase) -> None:
    """Reject a phase outside the component kind's closed subset."""
    rule = WATCHDOG_RULES.get(kind)
    if rule is None or phase not in rule[0]:
        raise ValueError('watchdog phase is invalid for component kind')


def _visible_health_conditions(
    sources: Iterable[SourceHealth],
    groups: Iterable[GroupHealth],
    workers: Iterable[WorkerHealth],
    *,
    stale_after_secs: float,
    extra_conditions: Sequence[str] = (),
) -> tuple[str, ...]:
    """Return ordered body-free conditions from current in-memory health."""
    conditions: set[str] = set()
    for source in sources:
        conditions.update(_source_conditions(source))
    for group in groups:
        conditions.update(_group_conditions(group))
    for worker in workers:
        if worker.breaker_open:
            conditions.add('worker-breaker-open')
        if not worker.monitor_alive or worker.heartbeat_age > stale_after_secs:
            conditions.add('stale')
        if worker.watchdog_failure is not None:
            conditions.add('stale')
    conditions.update(extra_conditions)
    if not conditions:
        conditions.add('healthy')
    ordered = tuple(state for state in _STATUS_PRECEDENCE if state in conditions)
    return (*ordered, *sorted(conditions.difference(ordered)))


def _readiness_health_conditions(
    sources: Iterable[SourceHealth],
    groups: Iterable[GroupHealth],
    workers: Iterable[WorkerHealth],
    *,
    stale_after_secs: float,
    extra_conditions: Sequence[str] = (),
) -> tuple[str, ...]:
    """Return ordered readiness conditions with parked group blockers omitted."""
    return _visible_health_conditions(
        sources,
        tuple(group for group in groups if not group.parked),
        workers,
        stale_after_secs=stale_after_secs,
        extra_conditions=extra_conditions,
    )


def _worker_liveness(workers: Iterable[WorkerHealth], stale_after_secs: float) -> bool:
    """Return whether all worker monitors have fresh heartbeats."""
    return all(
        worker.monitor_alive and worker.heartbeat_age <= stale_after_secs for worker in workers
    )


def looks_secret(value: str) -> bool:
    """Detect a named secret assignment in diagnostic text."""
    normalized = value.casefold()
    return any(term in normalized for term in HEALTH_SECRET_TEXT_TERMS)


def primary_state_for_conditions(conditions: Collection[str]) -> _PrimaryState:
    """Return the highest-precedence state from ordered health conditions.

    Args:
        conditions: Body-free health conditions.

    Returns:
        The highest-precedence existing primary state, or healthy.
    """
    for state in _STATUS_PRECEDENCE:
        if state in conditions:
            return state
    return 'healthy'


def probe_health_conditions(**options: Unpack[_ProbeHealthOptions]) -> tuple[str, ...]:
    """Project ordered probe conditions from in-memory worker health.

    Args:
        sources: Current source projections.
        groups: Current group projections.
        workers: Current worker projections.
        now: Supervisor monotonic observation time.
        stale_after_secs: Maximum worker heartbeat age.
        worker_breaker_open: Whether any supervisor breaker is open.
        inventory_complete: Whether all expected first-health inventory exists.

    Returns:
        Ordered body-free visible health conditions.
    """
    current_sources, current_groups, current_workers, extra_conditions = _current_health_inputs(
        options
    )
    return _visible_health_conditions(
        current_sources,
        current_groups,
        current_workers,
        stale_after_secs=options['stale_after_secs'],
        extra_conditions=extra_conditions,
    )


def probe_health_ready(**options: Unpack[_ProbeHealthOptions]) -> bool:
    """Return readiness from structured in-memory component ownership.

    Args:
        sources: Current source projections.
        groups: Current group projections.
        workers: Current worker projections.
        now: Supervisor monotonic observation time.
        stale_after_secs: Maximum worker heartbeat age.
        worker_breaker_open: Whether any supervisor breaker is open.
        inventory_complete: Whether all expected first-health inventory exists.

    Returns:
        Whether every non-parked component condition is readiness-safe.
    """
    current_sources, current_groups, current_workers, extra_conditions = _current_health_inputs(
        options
    )
    conditions = _readiness_health_conditions(
        current_sources,
        current_groups,
        current_workers,
        stale_after_secs=options['stale_after_secs'],
        extra_conditions=extra_conditions,
    )
    return _worker_liveness(current_workers, options['stale_after_secs']) and _conditions_ready(
        conditions
    )


def aggregate_status(**options: Unpack[_AggregateStatusOptions]) -> StatusSnapshot:
    """Aggregate authoritative component records into one status snapshot.

    Args:
        sources: Current source health records.
        groups: Current complete group projections.
        workers: Current process and monitor records.
        now: Supervisor monotonic observation time.
        stale_after_secs: Maximum worker heartbeat age before stale status.
        audit_count: Cumulative durable audit count.
        database_sizes: Current database byte sizes.
        breaker_history: Recent worker breaker transitions.
        breaker_transition_count: Cumulative transition count.
        worker_breaker_open: Whether a supervisor-level worker breaker is open.
            Defaults to `False`; when true, adds the worker-breaker-open
            condition independently of the worker records.

    Returns:
        One immutable ephemeral status snapshot.

    Raises:
        ValueError: The stale threshold is not positive or a cumulative counter
            is negative.
    """
    now = options['now']
    stale_after_secs = options['stale_after_secs']
    audit_count = options.get('audit_count', 0)
    breaker_history = options.get('breaker_history', ())
    breaker_transition_count = options.get('breaker_transition_count', 0)
    if stale_after_secs <= 0:
        raise ValueError('stale threshold must be positive')
    if audit_count < 0 or breaker_transition_count < 0:
        raise ValueError('cumulative health counters must be nonnegative')
    current_sources, current_groups, current_workers, extra_conditions = _current_health_inputs(
        {
            'sources': options['sources'],
            'groups': options['groups'],
            'workers': options['workers'],
            'now': now,
            'stale_after_secs': stale_after_secs,
            'worker_breaker_open': options.get('worker_breaker_open', False),
        }
    )
    visible_conditions = _visible_health_conditions(
        current_sources,
        current_groups,
        current_workers,
        stale_after_secs=stale_after_secs,
        extra_conditions=extra_conditions,
    )
    primary = primary_state_for_conditions(visible_conditions)
    liveness = _worker_liveness(current_workers, stale_after_secs)
    readiness = probe_health_ready(
        sources=current_sources,
        groups=current_groups,
        workers=current_workers,
        now=now,
        stale_after_secs=stale_after_secs,
        worker_breaker_open=options.get('worker_breaker_open', False),
    )
    history = tuple(breaker_history[-100:])
    total_transitions = max(breaker_transition_count, len(breaker_history))
    return StatusSnapshot(
        generated_at=now,
        primary_state=primary,
        conditions=visible_conditions,
        sources=current_sources,
        groups=current_groups,
        workers=current_workers,
        liveness=liveness,
        readiness=readiness,
        audit_count=audit_count,
        database_sizes=tuple(options.get('database_sizes', ())),
        breaker_history=history,
        breaker_transition_count=total_transitions,
    )


@overload
def redact_health_text(value: str) -> str: ...


@overload
def redact_health_text(value: None) -> None: ...


@overload
def redact_health_text(value: str | None) -> str | None: ...


def redact_health_text(value: str | None) -> str | None:
    """Apply registry replacement, then the named-secret fallback.

    Args:
        value: Optional body-free health detail.

    Returns:
        Registry-redacted safe text, `None`, or the redaction marker.
    """
    if value is None:
        return None
    redacted = logging_support.redact_text(value)
    if looks_secret(redacted):
        return '<redacted>'
    return redacted


@overload
def bound_health_text(value: str) -> str: ...


@overload
def bound_health_text(value: None) -> None: ...


@overload
def bound_health_text(value: str | None) -> str | None: ...


def bound_health_text(value: str | None) -> str | None:
    """Return redacted health detail within the fixed UTF-8 byte limit.

    Args:
        value: Optional body-free health detail.

    Returns:
        Redacted and boundary-safe diagnostic text, or `None`.
    """
    if value is not None and type(value) is not str:
        raise TypeError('health text must be text or None')
    redacted = redact_health_text(value)
    if redacted is None:
        return None
    return redacted.encode()[:HEALTH_TEXT_MAX_BYTES].decode(errors='ignore')
