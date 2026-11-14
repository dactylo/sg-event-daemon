"""Packed worker specifications and injected foreground runtime."""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import threading
import time
from collections import deque
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import AbstractContextManager, closing, contextmanager, nullcontext, suppress
from dataclasses import dataclass, field, replace
from enum import StrEnum
from functools import partial
from math import ceil, isfinite, nextafter
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Generic,
    Literal,
    NotRequired,
    Protocol,
    Self,
    TypeAlias,
    TypedDict,
    TypeVar,
    Unpack,
    cast,
    get_args,
)

from ._checkpoint_host import CheckpointHost, CheckpointHostStoppedError, CheckpointView
from ._closeout import CloseoutRetries
from ._config import (
    ConfigError,
    group_state_options,
    source_registration_holdback_secs,
    source_state_options,
    source_stream_fingerprint,
)
from ._control import (
    GROUP_BUSY_CODE,
    GROUP_DISPOSITION_ARGUMENTS,
    OPERATOR_COMMANDS,
    SOURCE_ADOPTION_ARGUMENTS,
    SOURCE_ADOPTION_EXECUTION_ARGUMENTS,
    SOURCE_BUSY_CODE,
    SOURCE_DATABASE_BUSY_CODE,
    SOURCE_OPERATOR_COMMANDS,
)
from ._event_frame import IPC_LENGTH_BYTES
from ._fanout import (
    EventDelivery,
    EventFanout,
    FanoutMessage,
    GroupChannel,
    GroupChannelInterface,
    RangeChanged,
    StrictOrderingControl,
)
from ._gaps import clip_runs, log_closeout_gave_up, log_closeout_retrying, subtract_ids
from ._group_recovery import (
    DispositionCommand,
    control_peer_label,
    disposition_command,
    group_result_mapping,
)
from ._group_state import GroupState, PendingRange, SourceProgress
from ._group_store import GROUP_HEALTH_CONDITIONS
from ._group_types import (
    GROUP_STATUSES,
    AuditIdentity,
    GroupSnapshot,
    canonical_bytes,
    digest,
)
from ._health import (
    HEALTH_LATE_ID_LIMIT,
    HEALTH_TEXT_MAX_BYTES,
    STALL_CONDITIONS,
    ConditionFlags,
    EventSocketHealth,
    GroupHealth,
    WatchdogHealth,
    WorkerHealth,
    bound_health_text,
)
from ._ipc import (
    OUTCOME_KEY,
    OUTCOME_UNKNOWN,
    ActiveSourceGroupAssignment,
    CommandResult,
    DeliveryGroupAssignment,
    EventClient,
    EventPeerGrant,
    EventServer,
    GroupBootstrapObservation,
    GroupBootstrapPending,
    GroupBootstrapRequest,
    GroupRecoveryReady,
    HealthUpdate,
    OperatorCommand,
    PendingDispositionSourceGroupAssignment,
    SocketBufferSizes,
    SourceEndpointReady,
    SourceGroupAssignment,
    StopCommand,
    WorkerAssignment,
    WorkerControl,
    WorkerControlOutbox,
    encode_control_message,
)
from ._lane import Delivery, GroupLane, LaneHealth
from ._platform import (
    AlreadyLockedError,
    ensure_secure_state_dir,
    local_socket_path,
    local_socket_path_candidate,
)
from ._plugins import load_plugins
from ._providers import load_provider
from ._resources import Closeable, close_resources
from ._source import (
    HALTS_IN_PLACE,
    SOURCE_RUN_STATUSES,
    Attached,
    CatchupReader,
    HistoryTruncatedError,
    JoinAbort,
    JoinCapacityError,
    JoinConfirm,
    JoinCoordinator,
    JoinFinalize,
    JoinFinalized,
    JoinProtocolError,
    JoinRequest,
    JoinRetry,
    JoinTarget,
    LiveChannelDetachedError,
    SourceHaltedError,
    SourceHealth,
    SourceOwnerError,
    SourcePoller,
    StrictDeliveryOutcome,
    history_truncation,
    jitter_retry_delay,
    saturating_retry_delay,
)
from ._source_schema import SOURCE_SCHEMA, source_identity
from ._source_state import (
    DurableGroupPolicy,
    PendingGroupRecord,
    SourceState,
    StreamAdoptionReservation,
)
from ._source_store import SourceSnapshot
from ._state_paths import (
    group_lock_path,
    site_groups_directory,
    site_sources_directory,
    source_database_path,
    source_socket_kind,
    source_write_lock_path,
)
from ._strict import StrictBuffer
from ._watchdog import (
    GROUP_PHASES,
    SOURCE_WATCHDOG_PHASES,
    ComponentWatchdog,
    GroupPhase,
    SourceWatchdogPhase,
    WatchdogKind,
    WatchdogSample,
    internal_stall_deadline_secs,
    shutdown_slice_secs,
    watchdog_grace_secs,
)
from ._write_lock import (
    SourceDatabaseBusyError,
    SourceWriteLock,
    WriteContext,
    WriteLockStoppedError,
    WritePhase,
    raise_owner_write_failure,
    write_lock_timeout_secs,
)

if TYPE_CHECKING:
    from types import TracebackType

    from ._closeout import ObligationKey
    from ._config import (
        Config,
        CredentialConfig,
        GroupConfig,
        SiteConfig,
        SourceConfig,
    )
    from ._events import Event
    from ._gaps import IdRun
    from ._group_state import GroupOpenOptions
    from ._providers import Provider

_OwnerT = TypeVar('_OwnerT')
_Result = TypeVar('_Result')

_BOOTSTRAP_RETRY_POLL_SECS = 0.05
_DEFAULT_SOCKET_BUFFERS = SocketBufferSizes(requested=0, send=0, receive=0)
_LANE_OPERATOR_ADMISSION_SECS = 1.0
_MAXIMUM_HEALTH_FLOAT = float.fromhex('0x1.fffffffffffffp+1023')
_MAXIMUM_HEALTH_IDENTITY = 'x' * 64
_MAXIMUM_HEALTH_INT = 2**63 - 1
_MAXIMUM_HEALTH_TEXT = '\0' * HEALTH_TEXT_MAX_BYTES
_MONITOR_THREAD_NAME = 'sg-event-daemon:monitor'
OPEN_FILE_MARGIN = 32
_OPERATOR_EXECUTOR_IDENTITY = 'operator-executor'

_ComponentStopPhase: TypeAlias = Literal[
    'eager-catchup',
    'bootstrap',
    'local-call',
    'reader-slot',
    'resume',
    'strict-catchup',
]

_ComponentFactory: TypeAlias = Callable[
    [str, '_CatchupLimiter', CheckpointHost], '_WorkerComponent'
]
_ForceExit: TypeAlias = Callable[[int], object]
_HealthSink: TypeAlias = Callable[[WorkerHealth], float | None]
_MonitorGate: TypeAlias = Callable[[], AbstractContextManager[object]]
_WatchdogKey: TypeAlias = tuple[WatchdogKind, str, str, str]
_WatchdogOwnerKey: TypeAlias = tuple[WatchdogKind, str, str]
_LaneOwnerPhase: TypeAlias = Literal[
    'idle', 'internal', 'shutdown', 'startup', 'write-hold', 'write-lock-wait'
]


class _CatchupLimiter:
    """Bound concurrent detached catch-up readers within one worker."""

    def __init__(self, maximum: int) -> None:
        """Create a fair-enough bounded semaphore."""
        if type(maximum) is not int or maximum <= 0:
            raise ValueError('catch-up reader limit must be positive')
        self._semaphore = threading.BoundedSemaphore(maximum)

    def acquire(self, *, timeout: float) -> bool:
        """Wait up to the caller's bounded slot interval.

        Args:
            timeout: Maximum seconds to wait for a reader slot.

        Returns:
            `True` after acquiring a slot that the caller must release, or
            `False` when the wait expires without acquiring one.
        """
        return self._semaphore.acquire(timeout=timeout)

    def release(self) -> None:
        """Return one acquired reader slot."""
        self._semaphore.release()


@dataclass(frozen=True)
class _ComponentHealth:
    """Hold one atomic immutable component-health cache snapshot."""

    watchdogs: tuple[WatchdogHealth, ...]
    sources: tuple[SourceHealth, ...]
    groups: tuple[GroupHealth, ...]
    event_sockets: tuple[EventSocketHealth, ...]
    watchdog_failure: str | None


class _ComponentWaiter:
    """Pump source-owned IPC while waiting between provider polls."""

    def __init__(
        self,
        stop: threading.Event,
    ) -> None:
        """Store the shared stop signal before source service is composed."""
        self._service: Callable[[], None] | None = None
        self._stop = stop

    def configure_service(self, service: Callable[[], None]) -> None:
        """Install the source-thread callback exactly once."""
        if self._service is not None:
            raise RuntimeError('component waiter service is already configured')
        self._service = service

    def pump(self) -> None:
        """Run one configured source-thread service pass.

        Raises:
            Exception: The configured source-thread service callback fails.
        """
        service = self._service
        if service is None:
            raise RuntimeError('component waiter service is not configured')
        service()

    def set(self) -> None:
        """Interrupt every current or future wait."""
        self._stop.set()

    def wait(self, seconds: float) -> bool:
        """Pump component work until stopped or the delay elapses.

        Args:
            seconds: Maximum delay in seconds.

        Returns:
            Whether the shared stop signal is set.

        Raises:
            RuntimeError: No service callback has been installed.
        """
        deadline = time.monotonic() + seconds
        while not self._stop.is_set():
            self.pump()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            self._stop.wait(min(0.01, remaining))
        return self._stop.is_set()


class _CooperativeComponentStopError(RuntimeError):
    """Signal one known operation interrupted by an earlier stop request."""

    def __init__(self, phase: _ComponentStopPhase) -> None:
        """Record the exact interruptible operation that observed stop."""
        self.phase = phase
        super().__init__(f'component stopped during {phase}')


class _DeliveryFloorChannel(GroupChannel):
    """Drop live event bodies already covered by one lane delivery floor."""

    def __init__(
        self,
        *,
        group: str,
        ordering: Literal['eager', 'strict'],
        max_events: int,
        max_bytes: int,
        delivery_floor: int,
    ) -> None:
        """Create a bounded channel with one immutable delivery floor."""
        if type(delivery_floor) is not int or delivery_floor < 0:
            raise ValueError('delivery floor must be a nonnegative integer')
        super().__init__(
            group=group,
            ordering=ordering,
            max_events=max_events,
            max_bytes=max_bytes,
        )
        self._delivery_floor = delivery_floor
        self._hint_due = False
        self._hint_lock = threading.Lock()
        self._wake: threading.Event | None = None

    def _below_floor(self, message: FanoutMessage) -> bool:
        """Return whether one event body is already covered by the floor."""
        return (
            isinstance(message, EventDelivery) and message.encoded.event_id <= self._delivery_floor
        )

    def activate(self, *, provisional: bool) -> None:
        """Clear a prior attempt's advisory tick on activation."""
        super().activate(provisional=provisional)
        with self._hint_lock:
            self._hint_due = False

    def offer(self, message: FanoutMessage) -> bool:
        """Offer one frame unless its event body is already covered.

        Args:
            message: Immutable event delivery or body-free control.

        Returns:
            `True` when queued or safely discarded below the delivery floor.
            `False` means the channel is detached or was detached because the
            frame exceeded queue capacity. Below-floor discard succeeds even
            when detached.
        """
        return True if self._below_floor(message) else super().offer(message)

    def offer_hint(self, message: RangeChanged) -> bool:
        """Mark coordination due without queueing or detaching a live channel.

        Returns:
            `True` when a coordination tick is due and any configured owner
            wake event has been signaled. `False` when the channel is detached.
        """
        del message
        if not self.status.attached:
            return False
        with self._hint_lock:
            self._hint_due = True
        if self._wake is not None:
            self._wake.set()
        return True

    def offer_paced(self, message: FanoutMessage) -> bool:
        """Offer one strict frame unless its event body is already covered.

        Args:
            message: Immutable strict event or body-free control frame.

        Returns:
            `True` when queued or safely discarded below the delivery floor.
            `False` means the channel is detached or currently lacks capacity;
            capacity does not detach it. Below-floor discard succeeds even when
            detached.

        Raises:
            ValueError: A frame not discarded below the delivery floor is
                offered to a non-strict channel.
        """
        return True if self._below_floor(message) else super().offer_paced(message)

    def set_wake(self, wake: threading.Event) -> None:
        """Connect this channel to its owning lane's wake signal."""
        self._wake = wake

    def take_hint(self) -> bool:
        """Consume the coalesced hint at the lane coordination boundary."""
        with self._hint_lock:
            due = self._hint_due
            self._hint_due = False
            return due


@dataclass(frozen=True)
class _DispositionFence:
    """Hold one exact old-to-new group generation reservation."""

    orphan_generation: int
    reserved_generation: int


class _EventWaiter:
    """Adapt one threading event to the source reader waiter protocol."""

    def __init__(self, event: threading.Event) -> None:
        """Store the event shared with the lane component."""
        self._event = event

    def set(self) -> None:
        """Request lane shutdown."""
        self._event.set()

    def wait(self, seconds: float) -> bool:
        """Wait for the requested interval or until lane shutdown.

        Args:
            seconds: Maximum number of seconds to wait.

        Returns:
            Whether shutdown was requested.
        """
        return self._event.wait(seconds)


class _ExceptionCapture:
    """Capture ordinary exceptions at an explicit worker ownership boundary."""

    def __init__(self) -> None:
        """Create an empty ordinary-exception capture."""
        self.error: Exception | None = None

    def __enter__(self) -> Self:
        """Return this active capture."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Capture only ordinary exceptions, not process-control exceptions."""
        if not isinstance(exc_value, Exception):
            return False
        self.error = exc_value
        return True


class _GroupHaltedCatchupError(RuntimeError):
    """Signal one expected durable group halt during catch-up."""


@dataclass(frozen=True)
class _LaneConfiguration:
    """Hold resolved immutable configuration for one concrete lane."""

    assignment: DeliveryGroupAssignment
    credential: CredentialConfig
    group: GroupConfig
    holdback_secs: float
    site: SiteConfig
    source_config: SourceConfig


@dataclass
class _LaneLifecycle:
    """Track mutable process-local state for one persistent lane."""

    channel: GroupChannelInterface
    joined: bool = False
    next_coordination_at: float = 0.0
    setup_attempted: bool = False
    setup_failure: object | None = None
    was_running: bool = False
    halt_logged: str | None = None
    rejoin_reason: str | None = None


class _RemoteBootstrapTransportError(RuntimeError):
    """Signal one retryable remote bootstrap transport failure."""


class _InitialGroupQueryError(RuntimeError):
    """Signal one retryable first query for a newly initialized group."""


class _WorkerComponent(Protocol):
    """Define one injected source or lane resource owned by a worker."""

    def close(self) -> None:
        """Retire the component's health registration after its owner thread stops."""
        ...

    def execute_operator_command(
        self,
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        """Execute one body-free command on the owning component thread."""
        ...

    def request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        """Interrupt the component's current or future wait."""
        ...

    def run(self) -> None:
        """Run until a stop request or worker-fatal return."""
        ...


class _WorkerKind(StrEnum):
    """Classify source-owning, named packed, and dedicated workers."""

    SITE = 'site'
    NAMED = 'named'
    DEDICATED = 'dedicated'


@dataclass(frozen=True)
class WorkerSpec:
    """Describe one packed worker assignment.

    Each configured group belongs to exactly one specification.
    `build_worker_specs()` returns the source-owning site worker first,
    followed by used named workers and dedicated workers.

    Attributes:
        worker_id: Stable opaque process identity.
        site: Configured site owned by the process.
        kind: Site, named, or dedicated worker classification.
        name: Display identity within the worker kind.
        sources: Source authorities owned by the process.
        groups: Groups assigned exactly to this process.
    """

    worker_id: str
    site: str
    kind: _WorkerKind
    name: str
    sources: tuple[str, ...]
    groups: tuple[str, ...]


@dataclass(frozen=True)
class _WatchdogRegistration:
    """Hold one current body-free component watchdog supplier."""

    token: str
    supplier: Callable[[], WatchdogSample]
    conditions: Callable[[], tuple[str, ...]]


class _ComponentHealthCache:
    """Cache immutable health records published by their owning threads."""

    def __init__(
        self,
        *,
        site: str | None = None,
        group_sources: Mapping[str, str] | None = None,
    ) -> None:
        """Create one empty worker-local component-health cache."""
        self._event_sockets: dict[
            tuple[Literal['server', 'client'], str, str | None],
            EventSocketHealth,
        ] = {}
        self._group_sources = {} if group_sources is None else dict(group_sources)
        self._groups: dict[str, GroupHealth] = {}
        self._lock = threading.Lock()
        self._operator_targets: dict[_WatchdogKey, tuple[str, str | None]] = {}
        self._pending_groups: dict[str, tuple[str, str]] = {}
        self._samples: dict[_WatchdogKey, WatchdogSample] = {}
        self._site = site
        self._sources: dict[str, SourceHealth] = {}
        self._watchdogs: dict[_WatchdogKey, _WatchdogRegistration] = {}

    @staticmethod
    def _key(sample: WatchdogSample) -> _WatchdogKey:
        """Return one sample's complete registration key."""
        return (
            sample.kind,
            sample.identity,
            sample.worker_instance,
            sample.registration_token,
        )

    def _merge_group(
        self,
        health: GroupHealth,
        sample: WatchdogSample,
        conditions: tuple[str, ...],
    ) -> GroupHealth:
        """Merge only current live lane fields over durable group detail."""
        phase = cast('GroupPhase', sample.phase)
        merged_conditions = tuple(
            dict.fromkeys(
                (
                    *conditions,
                    *(name for name in health.conditions if name not in STALL_CONDITIONS),
                )
            )
        )
        live_fields = (
            phase,
            sample.event_id,
            sample.plugin_id,
            sample.callback_id,
            sample.delivery_attempt,
            sample.heartbeat_at,
            sample.deadline_at,
            merged_conditions,
        )
        current_fields = (
            health.phase,
            health.current_event_id,
            health.current_plugin_id,
            health.current_callback_id,
            health.delivery_attempt,
            health.heartbeat_at,
            health.deadline_at,
            health.conditions,
        )
        if live_fields == current_fields:
            return health
        return replace(
            health,
            phase=phase,
            current_event_id=sample.event_id,
            current_plugin_id=sample.plugin_id,
            current_callback_id=sample.callback_id,
            delivery_attempt=sample.delivery_attempt,
            heartbeat_at=sample.heartbeat_at,
            deadline_at=sample.deadline_at,
            conditions=merged_conditions,
        )

    def _merge_source(
        self,
        health: SourceHealth,
        sample: WatchdogSample,
        conditions: tuple[str, ...],
    ) -> SourceHealth:
        """Merge only current live source fields over durable source detail."""
        phase = cast('SourceWatchdogPhase', sample.phase)
        merged_conditions = tuple(
            dict.fromkeys(
                (
                    *conditions,
                    *(name for name in health.conditions if name not in STALL_CONDITIONS),
                )
            )
        )
        live_fields = (phase, sample.heartbeat_at, sample.deadline_at, merged_conditions)
        current_fields = (health.phase, health.heartbeat_at, health.deadline_at, health.conditions)
        if live_fields == current_fields:
            return health
        return replace(
            health,
            phase=phase,
            heartbeat_at=sample.heartbeat_at,
            deadline_at=sample.deadline_at,
            conditions=merged_conditions,
        )

    @staticmethod
    def _owner_key(sample: WatchdogSample) -> _WatchdogOwnerKey:
        """Return one sample's token-independent component-owner key."""
        return (sample.kind, sample.identity, sample.worker_instance)

    def _transport(
        self,
        sample: WatchdogSample,
        sources: Mapping[str, SourceHealth],
        groups: Mapping[str, GroupHealth],
    ) -> WatchdogHealth:
        """Convert one accepted local sample to its compact transport shape."""
        site = self._site
        source: str | None
        group: str | None
        if sample.kind == 'source':
            detail = sources.get(sample.identity)
            if site is None and detail is not None:
                site = detail.site
            source = sample.identity
            group = None
        elif sample.kind == 'lane':
            detail = groups.get(sample.identity)
            if site is None and detail is not None:
                site = detail.site
            source = self._group_sources.get(sample.identity)
            if source is None and detail is not None:
                source = detail.source
            group = sample.identity
        else:
            source, group = (
                self._operator_targets.get(self._key(sample), (None, None))
                if sample.phase != 'idle'
                else (None, None)
            )
        if site is None:
            raise RuntimeError('watchdog cache site identity is unavailable')
        return WatchdogHealth(
            kind=sample.kind,
            site=site,
            source=source,
            group=group,
            worker_instance=sample.worker_instance,
            phase=sample.phase,
            heartbeat_at=sample.heartbeat_at,
            deadline_at=sample.deadline_at,
            event_id=sample.event_id,
            plugin_id=sample.plugin_id,
            callback_id=sample.callback_id,
            delivery_attempt=sample.delivery_attempt,
            operator_command=sample.operator_command,
        )

    def deregister_watchdog(
        self,
        *,
        kind: WatchdogKind,
        identity: str,
        worker_instance: str,
        token: str,
    ) -> None:
        """Remove exactly the current matching registration."""
        key = (kind, identity, worker_instance, token)
        with self._lock:
            registration = self._watchdogs.get(key)
            if registration is None:
                return
            del self._watchdogs[key]
            self._operator_targets.pop(key, None)
            self._samples.pop(key, None)
            if kind == 'source':
                self._sources.pop(identity, None)
                for group, owner in tuple(self._pending_groups.items()):
                    if owner == (identity, worker_instance):
                        self._pending_groups.pop(group)
                        self._groups.pop(group, None)
            elif kind == 'lane':
                self._groups.pop(identity, None)

    def publish_event_socket(self, health: EventSocketHealth) -> None:
        """Replace one owner-thread event-socket projection."""
        with self._lock:
            self._event_sockets[(health.role, health.source, health.group)] = health

    def publish_group(
        self,
        health: GroupHealth,
        *,
        sample: WatchdogSample,
    ) -> None:
        """Replace one lane-thread group projection."""
        key = self._key(sample)
        with self._lock:
            registration = self._watchdogs.get(key)
            if (
                registration is None
                or registration.token != sample.registration_token
                or sample.kind != 'lane'
                or sample.identity != health.group
                or sample.worker_instance != health.worker_instance
            ):
                return
            self._groups[health.group] = health
            self._pending_groups.pop(health.group, None)
            current = self._samples.get(key)
            if current is None or sample.transition_sequence >= current.transition_sequence:
                self._samples[key] = sample

    def publish_pending_group(self, health: GroupHealth, *, sample: WatchdogSample) -> None:
        """Cache one source-owned parked group without creating a lane watchdog."""
        with self._lock:
            registration = self._watchdogs.get(self._key(sample))
            if (
                registration is None
                or sample.kind != 'source'
                or sample.identity != health.source
                or sample.worker_instance != health.worker_instance
            ):
                return
            self._groups[health.group] = health
            self._pending_groups[health.group] = (health.source, health.worker_instance)

    def publish_source(
        self,
        health: SourceHealth,
        *,
        sample: WatchdogSample,
    ) -> None:
        """Replace one source-thread source projection."""
        key = self._key(sample)
        with self._lock:
            registration = self._watchdogs.get(key)
            if (
                registration is None
                or registration.token != sample.registration_token
                or sample.kind != 'source'
                or sample.identity != health.source
                or sample.worker_instance != health.worker_instance
            ):
                return
            self._sources[health.source] = health
            current = self._samples.get(key)
            if current is None or sample.transition_sequence >= current.transition_sequence:
                self._samples[key] = sample

    def register_watchdog(
        self,
        *,
        key: _WatchdogKey,
        supplier: Callable[[], WatchdogSample],
        conditions: Callable[[], tuple[str, ...]] = lambda: (),
    ) -> None:
        """Register one current body-free watchdog supplier.

        A new token replaces prior registrations for the same kind, identity,
        and worker instance and retires their cached health.

        Args:
            key: Component kind, identity, worker instance, and registration
                token.
            supplier: Callable returning that exact registration's current
                watchdog sample.
            conditions: Callable returning its current condition names.

        Raises:
            ValueError: The identity, worker instance, or registration token is
                empty.
        """
        kind, identity, worker_instance, token = key
        if not identity or not worker_instance or not token:
            raise ValueError('watchdog registration identity must not be empty')
        owner_key = (kind, identity, worker_instance)
        with self._lock:
            predecessors = tuple(
                registered
                for registered in self._watchdogs
                if registered[:3] == owner_key and registered != key
            )
            for predecessor in predecessors:
                del self._watchdogs[predecessor]
                self._operator_targets.pop(predecessor, None)
                self._samples.pop(predecessor, None)
            if predecessors:
                if kind == 'source':
                    self._sources.pop(identity, None)
                elif kind == 'lane':
                    self._groups.pop(identity, None)
            self._watchdogs[key] = _WatchdogRegistration(
                token=token,
                supplier=supplier,
                conditions=conditions,
            )

    def remove_pending_group(self, group: str, *, sample: WatchdogSample) -> None:
        """Remove only a parked record still owned by this source watchdog."""
        with self._lock:
            registration = self._watchdogs.get(self._key(sample))
            if (
                registration is None
                or registration.token != sample.registration_token
                or self._pending_groups.get(group) != (sample.identity, sample.worker_instance)
            ):
                return
            self._pending_groups.pop(group)
            self._groups.pop(group, None)

    def set_operator_target(
        self,
        *,
        sample: WatchdogSample,
        source: str | None,
        group: str | None,
    ) -> None:
        """Attach or clear the exact owner routed to one operator cell."""
        key = self._key(sample)
        with self._lock:
            registration = self._watchdogs.get(key)
            if registration is None or registration.token != sample.registration_token:
                return
            if source is None:
                self._operator_targets.pop(key, None)
            else:
                self._operator_targets[key] = (source, group)

    def snapshot(self) -> _ComponentHealth:
        """Return an immutable health projection from current registrations.

        Returns:
            Current source, group, socket, and watchdog projections. Suppliers
            are sampled outside the cache lock; a supplier failure or
            mismatched identity returns empty projections with
            `watchdog_failure='watchdog-sampling-failed'`.
        """
        with self._lock:
            registrations = tuple(self._watchdogs.items())
            sources = dict(self._sources)
            groups = dict(self._groups)
            event_sockets = dict(self._event_sockets)
        sampled: list[tuple[_WatchdogKey, WatchdogSample, tuple[str, ...]]] = []
        for key, registration in registrations:
            try:
                sample = registration.supplier()
                conditions = registration.conditions()
            except Exception:  # noqa: BLE001
                return _ComponentHealth(
                    watchdogs=(),
                    sources=(),
                    groups=(),
                    event_sockets=(),
                    watchdog_failure='watchdog-sampling-failed',
                )
            if self._key(sample) != key or sample.registration_token != registration.token:
                return _ComponentHealth(
                    watchdogs=(),
                    sources=(),
                    groups=(),
                    event_sockets=(),
                    watchdog_failure='watchdog-sampling-failed',
                )
            sampled.append((key, sample, conditions))
        kind_order = {'source': 0, 'lane': 1, 'operator': 2}
        with self._lock:
            conditions_by_owner: dict[_WatchdogOwnerKey, tuple[str, ...]] = {}
            for key, sample, conditions in sampled:
                registration = self._watchdogs.get(key)
                if registration is None or registration.token != sample.registration_token:
                    continue
                conditions_by_owner[self._owner_key(sample)] = conditions
                current = self._samples.get(key)
                if current is None or sample.transition_sequence >= current.transition_sequence:
                    self._samples[key] = sample
            current_sources = dict(self._sources)
            current_groups = dict(self._groups)
            sources = {
                name: current_sources[name]
                for name in sources | current_sources
                if name in current_sources
            }
            groups = {
                name: current_groups[name]
                for name in groups | current_groups
                if name in current_groups
            }
            event_sockets.update(self._event_sockets)
            samples_by_owner = {
                self._owner_key(sample): sample for sample in self._samples.values()
            }
            accepted = tuple(
                self._samples[key]
                for key in sorted(
                    self._watchdogs,
                    key=lambda item: (kind_order[item[0]], item[1], item[2]),
                )
                if key in self._samples
                and self._watchdogs[key].token == self._samples[key].registration_token
            )
            merged_sources = {
                name: self._merge_source(
                    health,
                    samples_by_owner[('source', name, health.worker_instance)],
                    conditions_by_owner.get(('source', name, health.worker_instance), ()),
                )
                if ('source', name, health.worker_instance) in samples_by_owner
                else health
                for name, health in sources.items()
            }
            merged_groups = {
                name: self._merge_group(
                    health,
                    samples_by_owner[('lane', name, health.worker_instance)],
                    conditions_by_owner.get(('lane', name, health.worker_instance), ()),
                )
                if ('lane', name, health.worker_instance) in samples_by_owner
                else health
                for name, health in groups.items()
            }
            self._sources.update(merged_sources)
            self._groups.update(merged_groups)
            return _ComponentHealth(
                watchdogs=tuple(
                    self._transport(sample, merged_sources, merged_groups) for sample in accepted
                ),
                sources=tuple(merged_sources[name] for name in sorted(merged_sources)),
                groups=tuple(merged_groups[name] for name in sorted(merged_groups)),
                event_sockets=tuple(event_sockets[key] for key in sorted(event_sockets)),
                watchdog_failure=None,
            )


class _SourceComponentOptions(TypedDict):
    """Type one concrete source component composition."""

    assignment: WorkerAssignment
    checkpoint_host: CheckpointHost
    config: Config
    control: WorkerControlOutbox
    health_cache: _ComponentHealthCache
    source: str
    spec: WorkerSpec


class _SourceJoinSurfaceOptions(TypedDict):
    """Type one source join surface composition."""

    bootstrap_retry_secs: float
    coordinator: JoinCoordinator
    poller: SourcePoller
    state: SourceState
    inventory: Mapping[str, SourceGroupAssignment]
    strict_buffer: StrictBuffer | None


class _LaneCallBusyError(Exception):
    """Reject a lane command that remained queued past its admission budget."""


class _SourceCallBusyError(Exception):
    """Reject an operator call withdrawn before the source starts it."""


@dataclass
class _OwnerCall(Generic[_OwnerT]):
    """Carry one call from a requesting thread to its owner thread."""

    invoke: Callable[[_OwnerT], object]
    complete: threading.Event = field(default_factory=threading.Event)
    result: object | None = None
    error: Exception | None = None


class _OwnerMailbox(Generic[_OwnerT]):
    """Queue calls for one owner thread and withdraw calls that have not started."""

    def __init__(self, name: str, wake: threading.Event | None = None) -> None:
        """Create an empty mailbox for one named owner."""
        self._calls: deque[_OwnerCall[_OwnerT]] = deque()
        self._lock = threading.Lock()
        self._name = name
        self._wake = wake

    def submit(self, invoke: Callable[[_OwnerT], object]) -> _OwnerCall[_OwnerT]:
        """Queue one call and wake its owner."""
        call = _OwnerCall(invoke)
        with self._lock:
            self._calls.append(call)
        if self._wake is not None:
            self._wake.set()
        return call

    def take(self) -> _OwnerCall[_OwnerT] | None:
        """Dequeue the next call, marking it started, or return None if empty."""
        with self._lock:
            return self._calls.popleft() if self._calls else None

    def withdraw(self, call: _OwnerCall[_OwnerT]) -> bool:
        """Remove a call that is still queued and report whether it was removed."""
        with self._lock:
            try:
                self._calls.remove(call)
            except ValueError:
                return False
        return True


class _SourceJoinSurface:
    """Combine source join coordination with poller channel-loss handling."""

    def __init__(self, **options: Unpack[_SourceJoinSurfaceOptions]) -> None:
        """Store component-thread-owned source authorities."""
        self._bootstrap_retry_secs = options['bootstrap_retry_secs']
        self._coordinator = options['coordinator']
        self._inventory = dict(options['inventory'])
        self._poller = options['poller']
        self._index_revision: int | None = None
        self._durable_generations: dict[str, int] = {}
        self._inactive_groups: set[str] = set()
        self.state = options['state']
        self._strict_buffer = options['strict_buffer']

    def _restore_group_policy(self, policy: DurableGroupPolicy) -> None:
        """Install file-derived strict progress after the prior live authority is detached."""
        if policy.holdback_secs > 0 and self._strict_buffer is not None:
            self._strict_buffer.restore_group_policy(
                policy.group,
                policy.generation,
                holdback_secs=policy.holdback_secs,
                delivery_floor=policy.delivery_floor,
                last_delivered_id=policy.last_delivered_id,
                halted=policy.halted or not policy.active,
                is_operator_replay=policy.is_operator_replay,
            )

    def _restore_resumed_group(self, group: str) -> None:
        """Refresh same-generation durable delivery progress for a replacement peer."""
        _revision, policies = self.state.group_policies()
        for policy in policies:
            if policy.group == group:
                self._coordinator.group_died(group, policy.generation)
                self._poller.event_channel_lost(group, reason='group-resumed')
                self._restore_group_policy(policy)
                return

    def abort(self, abort: JoinAbort) -> None:
        """Abort one exact live-join attempt.

        Args:
            abort: Exact join attempt to abort.
        """
        self._coordinator.abort(abort)

    def acknowledge_strict_control(
        self,
        control: StrictOrderingControl,
    ) -> None:
        """Retire one exact lane-committed strict control.

        Args:
            control: Ordering halt committed by the lane.

        Raises:
            JoinProtocolError: The control is stale or is not finalized.
        """
        self._coordinator.acknowledge_strict_control(control)

    def confirm(self, confirmation: JoinConfirm) -> Attached | JoinRetry:
        """Prepare one exact caught-up join attempt.

        Args:
            confirmation: Exact join boundary confirmation.

        Returns:
            Attached identity or retry instruction.

        Raises:
            JoinProtocolError: The attempt identity or phase conflicts.
        """
        return self._coordinator.confirm(confirmation)

    def event_channel_lost(self, group: str, *, reason: str) -> None:
        """Release source fanout retained for one disconnected group.

        Args:
            group: Stable group identity.
            reason: Body-free channel-loss condition.
        """
        assignment = self._inventory.get(group)
        if isinstance(assignment, ActiveSourceGroupAssignment):
            self._coordinator.group_died(
                group,
                assignment.group_generation,
            )
            self._poller.event_channel_lost(group, reason=reason)

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        """Promote one exact prepared join attempt.

        Args:
            finalization: Exact prepared join identity to finalize.

        Returns:
            Finalized join identity.

        Raises:
            JoinProtocolError: The attempt identity or phase conflicts.
        """
        return self._coordinator.finalize(finalization)

    def reconcile_groups(self) -> None:
        """Refresh file-owned generations and replace every old live release authority."""
        revision, policies = self.state.group_policies(after_revision=self._index_revision)
        if revision == self._index_revision:
            return
        for policy in policies:
            assignment = self._inventory.get(policy.group)
            if assignment is None:
                continue
            previous = self._durable_generations.get(policy.group)
            was_inactive = policy.group in self._inactive_groups
            if not policy.active:
                self._inactive_groups.add(policy.group)
            else:
                self._inactive_groups.discard(policy.group)
            if previous == policy.generation and was_inactive == (not policy.active):
                continue
            if previous is not None:
                self._coordinator.group_died(policy.group, previous)
                self._poller.event_channel_lost(policy.group, reason='operator-disposition')
            if policy.active:
                self._inventory[policy.group] = ActiveSourceGroupAssignment(
                    source=assignment.source,
                    group=policy.group,
                    group_generation=policy.generation,
                    start_mode='resume',
                )
            self._restore_group_policy(policy)
            self._durable_generations[policy.group] = policy.generation
        self.state.invalidate_strict_ranges()
        self._index_revision = revision

    def observe_group(
        self,
        request: GroupBootstrapRequest,
    ) -> GroupBootstrapObservation | GroupBootstrapPending:
        """Capture current source facts for one assigned group.

        Args:
            request: Exact group bootstrap observation request.

        Returns:
            Current source observation or pending retry instruction.

        Raises:
            ValueError: The request conflicts with the source inventory.
        """
        if not self.state.stamped:
            return GroupBootstrapPending(
                source=request.source,
                group=request.group,
                group_generation=request.group_generation,
                retry_after_secs=self._bootstrap_retry_secs,
            )
        self.reconcile_groups()
        expected = self._inventory.get(request.group)
        if isinstance(expected, ActiveSourceGroupAssignment):
            valid = (
                expected.source == request.source
                and expected.group_generation == request.group_generation
                and (expected.start_mode == 'initialize' or request.start_mode == 'resume')
            )
        elif isinstance(expected, PendingDispositionSourceGroupAssignment):
            valid = (
                expected.source == request.source
                and expected.orphan_generation == request.group_generation
                and request.start_mode == 'resume'
            )
        else:
            valid = False
        if not valid:
            raise ValueError('group bootstrap request conflicts with source inventory')
        if request.start_mode == 'resume':
            self._restore_resumed_group(request.group)
        snapshot = self.state.snapshot()
        incarnation = snapshot.stream_incarnation
        if not snapshot.initialized or incarnation is None:
            return GroupBootstrapPending(
                source=request.source,
                group=request.group,
                group_generation=request.group_generation,
                retry_after_secs=self._bootstrap_retry_secs,
            )
        return GroupBootstrapObservation(
            source=request.source,
            group=request.group,
            group_generation=request.group_generation,
            source_generation=snapshot.source_generation,
            stream_incarnation=incarnation,
            captured_frontier=snapshot.frontier,
            source_successful_clock=snapshot.successful_clock,
        )

    def request(
        self,
        request: JoinRequest,
        channel: GroupChannelInterface,
    ) -> JoinTarget | JoinRetry:
        """Capture one token-fenced live-join target.

        Args:
            request: Exact live-join request.
            channel: Attempt-owned bounded group fanout channel.

        Returns:
            Captured join target or retry instruction.

        Raises:
            JoinProtocolError: The request or attempt conflicts with source
                state.
            RuntimeError: The source stream is not initialized.
        """
        self.reconcile_groups()
        expected = self._inventory.get(request.attempt.group)
        if not isinstance(expected, ActiveSourceGroupAssignment):
            raise JoinProtocolError('pending disposition source inventory forbids fanout')
        if (
            expected.source != request.attempt.source
            or expected.group_generation != request.attempt.group_generation
        ):
            raise JoinProtocolError('join request conflicts with source inventory')
        if (
            self._durable_generations.get(request.attempt.group)
            != request.attempt.group_generation
            or request.attempt.group in self._inactive_groups
        ):
            raise JoinProtocolError('join request conflicts with durable group generation')
        result = self._coordinator.request(request, channel)
        if isinstance(result, JoinRetry):
            return result
        snapshot = self.state.snapshot()
        incarnation = snapshot.stream_incarnation
        if incarnation is None:
            raise RuntimeError('source stream is not initialized')
        return JoinTarget(
            result.identity,
            incarnation,
            snapshot.successful_clock,
        )

    def require_stamped(self) -> None:
        """Fence operator writes from the previous process generation.

        Raises:
            SourceDatabaseBusyError: This source owner's generation stamp has
                not committed.
        """
        if not self.state.stamped:
            raise SourceDatabaseBusyError('source generation stamp is pending')

    def adopt_stream(
        self,
        *,
        adoption_id: str,
        command_fingerprint: str,
        new_stream_fingerprint: str,
        reason: str,
        groups_parked: bool,
    ) -> SourceSnapshot:
        """Detach old lanes, adopt, initialize, and resume source polling."""
        self.require_stamped()
        self.reconcile_groups()
        with self.state.operator_writes():
            for group in self._inventory:
                self._poller.event_channel_lost(
                    group,
                    reason='stream-adopted',
                )
            self._index_revision = None
            return self._poller.adopt_stream(
                adoption_id=adoption_id,
                command_fingerprint=command_fingerprint,
                new_stream_fingerprint=new_stream_fingerprint,
                reason=reason,
                groups_parked=groups_parked,
            )

    def reserve_stream_adoption(
        self,
        *,
        command_fingerprint: str,
        reason: str,
    ) -> tuple[StreamAdoptionReservation, SourceSnapshot]:
        """Reserve one qualifying adoption before supervisor quiescence."""
        self.require_stamped()
        self.reconcile_groups()
        with self.state.operator_writes():
            reservation = self.state.reserve_stream_adoption(
                command_fingerprint=command_fingerprint,
                reason=reason,
            )
            return reservation, self.state.snapshot()


class _SourceComponent:
    """Own one concrete source state, provider, poller, and event endpoint."""

    def __init__(self, **options: Unpack[_SourceComponentOptions]) -> None:
        """Store immutable bootstrap inputs without opening resources."""
        self._assignment = options['assignment']
        self._checkpoint_host = options['checkpoint_host']
        self._checkpoint_view = CheckpointView()
        self._conditions = ConditionFlags()
        self._config = options['config']
        self._control = options['control']
        self._grant_lock = threading.Lock()
        self._grants: deque[EventPeerGrant] = deque()
        self._health_cache = options['health_cache']
        self._health_interval_secs = self._config.daemon.health_interval_secs
        self._mailbox = _OwnerMailbox[_SourceJoinSurface](options['source'])
        self._next_health_refresh_at: float | None = None
        self._next_pending_health_refresh_at: float | None = None
        self._pending_group_assignments: tuple[PendingDispositionSourceGroupAssignment, ...] = ()
        self._pending_group_detail: dict[str, PendingGroupRecord] = {}
        self._pending_group_detail_dirty = True
        self._poller: SourcePoller | None = None
        self._source = options['source']
        self._source_health: SourceHealth | None = None
        self._source_health_dirty = True
        self._shutdown_deadline_at: float | None = None
        self._spec = options['spec']
        self._stop = threading.Event()
        site = self._config.sites[self._spec.site]
        source_config = site.sources[self._source]
        credential = site.credentials[source_config.credentials]
        self._coordination_deadline_secs = internal_stall_deadline_secs(
            self._config.daemon.health_interval_secs,
            credential,
        )
        self._watchdog = ComponentWatchdog(
            kind='source',
            identity=self._source,
            worker_instance=self._assignment.spawn_instance,
            registration_token=os.urandom(16).hex(),
            startup_deadline_secs=self._coordination_deadline_secs,
        )
        sample = self._watchdog.sample()
        self._health_cache.register_watchdog(
            key=(sample.kind, sample.identity, sample.worker_instance, sample.registration_token),
            supplier=self._watchdog.sample,
            conditions=self._checkpoint_conditions,
        )

    def _check_local_call(self, stop: threading.Event) -> None:
        """End a source call only when its caller or source stops."""
        if stop.is_set() or self._stop.is_set():
            error = ConnectionError('source component stopped during local call')
            raise _CooperativeComponentStopError('local-call') from error

    def _checkpoint_conditions(self) -> tuple[str, ...]:
        """Merge the independently updated checkpoint view into monitor health."""
        names = self._conditions.names()
        return (*names, 'checkpoint-stalled') if self._checkpoint_view.stalled() else names

    def _database_paths(self) -> tuple[Path, Path]:
        """Create secure parents and return source database and socket paths."""
        ensure_secure_state_dir(self._config.daemon.state_dir)
        sources_dir = site_sources_directory(self._config, self._spec.site)
        ensure_secure_state_dir(sources_dir.parent.parent)
        ensure_secure_state_dir(sources_dir.parent)
        ensure_secure_state_dir(sources_dir)
        socket_kind = source_socket_kind(self._source)
        return (
            source_database_path(self._config, self._spec.site, self._source),
            local_socket_path(
                sources_dir,
                socket_kind,
                (self._spec.site, self._source),
                timeout_secs=shutdown_slice_secs(self._config.daemon.shutdown_deadline_secs),
            ),
        )

    def _health_snapshot_from_result(
        self,
        result: object,
    ) -> SourceSnapshot | None:
        """Return a complete snapshot embedded in a successful local result."""
        self._source_health_dirty = True
        if isinstance(result, (SourceSnapshot, GroupSnapshot)):
            self._pending_group_detail_dirty = True
        if isinstance(result, SourceSnapshot):
            self._source_health_dirty = False
            return result
        if not isinstance(result, tuple):
            return None
        for item in reversed(cast('tuple[object, ...]', result)):
            if isinstance(item, SourceSnapshot):
                self._pending_group_detail_dirty = True
                self._source_health_dirty = False
                return item
            if isinstance(item, GroupSnapshot):
                self._pending_group_detail_dirty = True
        return None

    def _mark_source_health_dirty(self) -> None:
        """Require the next owner service pass to rebuild source detail."""
        self._source_health_dirty = True
        self._pending_group_detail_dirty = True

    def _publish_pending_groups(self, surface: _SourceJoinSurface) -> None:
        """Refresh configured lane-less records without a lane liveness cell."""
        assignments = tuple(
            assignment
            for assignment in self._assignment.source_group_assignments
            if isinstance(assignment, PendingDispositionSourceGroupAssignment)
            and assignment.source == self._source
        )
        if assignments != self._pending_group_assignments:
            removed = {item.group for item in self._pending_group_assignments} - {
                item.group for item in assignments
            }
            for group in removed:
                self._pending_group_detail.pop(group, None)
                self._health_cache.remove_pending_group(group, sample=self._watchdog.sample())
            self._pending_group_assignments = assignments
            self._pending_group_detail_dirty = True
        now = time.monotonic()
        deadline = self._next_pending_health_refresh_at
        if self._pending_group_detail_dirty or deadline is None or now >= deadline:
            self._pending_group_detail = {
                assignment.group: surface.state.pending_group_record(
                    assignment.group, limit=HEALTH_LATE_ID_LIMIT
                )
                for assignment in assignments
            }
            self._pending_group_detail_dirty = False
            self._next_pending_health_refresh_at = _advance_health_refresh_deadline(
                deadline, now, self._health_interval_secs
            )
        site = self._config.sites[self._spec.site]
        for assignment in assignments:
            detail = self._pending_group_detail[assignment.group]
            current = detail.snapshot
            config = site.groups[assignment.group]
            lane = LaneHealth(
                site=site.name,
                source=self._source,
                group=assignment.group,
                worker_instance=self._assignment.spawn_instance,
                execution=config.execution,
                ordering=config.ordering,
                phase='idle',
                status=current.status,
                cursor=current.cursor,
                event_id=current.in_flight_event_id,
                plugin_id=None,
                callback_id=None,
                delivery_attempt=None,
                heartbeat_at=now,
                heartbeat_age=0.0,
                deadline_at=None,
                internal_stall_deadline_secs=self._coordination_deadline_secs,
                overdue_reason=None,
            )
            health = GroupHealth.from_lane(
                lane,
                queue_event_count=0,
                queue_byte_count=0,
                catchup_frontier=None,
                join_phase='parked',
                **_snapshot_health_options(
                    current,
                    (detail.late_id_count, detail.late_ids),
                    detail.park_reason,
                    detail.audit_count,
                ),
            )
            self._health_cache.publish_pending_group(health, sample=self._watchdog.sample())

    def _shutdown_resources(
        self,
        poller: SourcePoller | None,
        server: EventServer | None,
        state: SourceState | None,
        first_error: tuple[BaseException, TracebackType | None] | None,
    ) -> None:
        """Publish shutdown and exhaust source cleanup in owner order."""
        deadline_at = self._shutdown_deadline_at
        if deadline_at is None:
            deadline_at = time.monotonic() + self._config.daemon.shutdown_deadline_secs
        self._watchdog.transition(
            'shutdown',
            deadline_at=deadline_at,
        )
        self._poller = None
        _close_resources((poller, server, state), first_error)

    def _write_watch(
        self, phase: WritePhase, deadline_at: float, /
    ) -> AbstractContextManager[None]:
        """Publish source-file arbitration on this component's watchdog."""
        return self._watchdog.watch(phase, deadline_at=deadline_at)

    def _write_context(self) -> WriteContext:
        """Open the source-file lock owned by the next state facade."""
        daemon = self._config.daemon
        lock = SourceWriteLock(
            source_write_lock_path(self._config, self._spec.site, self._source),
            database_path=source_database_path(self._config, self._spec.site, self._source),
            timeout_secs=write_lock_timeout_secs(
                daemon.health_interval_secs,
                daemon.supervisor_poll_interval_secs,
            ),
            wait_phase_grace_secs=watchdog_grace_secs(daemon.health_interval_secs),
        )
        return WriteContext(
            lock,
            watch=self._write_watch,
            stop=self._stop,
            policy='one-attempt',
            conditions=self._conditions,
            logger=logging.getLogger('sg_event_daemon.source'),
            log_extra={'site': self._spec.site, 'source': self._source},
            slice_secs=shutdown_slice_secs(daemon.shutdown_deadline_secs),
        )

    def call(
        self,
        invoke: Callable[[_SourceJoinSurface], _Result],
        *,
        stop: threading.Event,
        wait_phase: Callable[[], AbstractContextManager[None]],
        queue_deadline_secs: float | None = None,
    ) -> _Result:
        """Renew caller health while awaiting source work or cooperative stop.

        Args:
            invoke: Operation executed against the source-thread-owned surface.
            stop: Caller stop signal, checked alongside the source stop signal.
            wait_phase: Context factory publishing the caller's wait phase.
            queue_deadline_secs: Optional admission timeout in seconds. Once
                dequeued, the operation owns its result and this timeout no
                longer limits its execution.

        Returns:
            The source operation's result.

        Raises:
            RuntimeError: The caller or source component stops while waiting.
            Exception: The admission timeout withdraws a still-queued
                operation, or the source operation or wait-phase context fails.
                Source operation failures propagate unchanged.
        """
        self._check_local_call(stop)
        deadline = None if queue_deadline_secs is None else time.monotonic() + queue_deadline_secs
        call = self._mailbox.submit(invoke)
        interval = self._health_interval_secs
        slice_secs = shutdown_slice_secs(self._config.daemon.shutdown_deadline_secs)
        try:
            while True:
                phase_end = time.monotonic() + interval
                with wait_phase():
                    while True:
                        self._check_local_call(stop)
                        now = time.monotonic()
                        if deadline is not None and now >= deadline:
                            if self._mailbox.withdraw(call):
                                raise _SourceCallBusyError(SOURCE_BUSY_CODE)
                            # A dequeued call owns its result, however long it takes.
                            deadline = None
                        if call.complete.is_set():
                            return cast('_Result', _owner_call_result(call))
                        if now >= phase_end:
                            break
                        wait = min(slice_secs, phase_end - now)
                        if deadline is not None:
                            wait = min(wait, max(0.0, deadline - now))
                        call.complete.wait(wait)
        except _CooperativeComponentStopError:
            self._mailbox.withdraw(call)
            raise

    def _execute_group_disposition(
        self, command: OperatorCommand, *, wait_phase: Callable[[], AbstractContextManager[None]]
    ) -> Mapping[str, object]:
        """Run a lane-less group's transaction on the source owner thread."""
        if (
            command.arguments.get('site') != self._spec.site
            or command.arguments.get('source') != self._source
        ):
            raise ValueError('group disposition source conflicts with owner')
        group = command.arguments.get('group')
        inner_command = command.arguments.get('command')
        reserved_generation = command.arguments.get('reserved_generation')
        expected_revision = command.arguments.get('expected_revision')
        if (
            not isinstance(group, str)
            or not isinstance(inner_command, str)
            or inner_command not in {'replay', 'reset-to-latest'}
            or type(reserved_generation) is not int
            or type(expected_revision) is not int
        ):
            raise ValueError('group disposition target is invalid')
        disposition_id = command.arguments.get('disposition_id')
        if not isinstance(disposition_id, str) or not disposition_id:
            raise ValueError('group disposition identity must be nonempty text')
        operator_label = command.arguments.get('operator_label')
        if operator_label is not None and not isinstance(operator_label, str):
            raise ValueError('group disposition operator label must be text')
        request = disposition_command(
            inner_command,
            command.arguments,
            disposition_id=disposition_id,
            wall_clock_time=time.time(),
            operator_label=operator_label,
        )

        def execute(surface: _SourceJoinSurface) -> GroupSnapshot:
            surface.require_stamped()
            source_state = surface.state
            current = source_state.group_snapshot(group)
            authority = source_state.snapshot()
            incarnation = authority.stream_incarnation
            if incarnation is None:
                raise RuntimeError('durable group source incarnation is unavailable')
            site = self._config.sites[self._spec.site]
            configured = site.groups.get(group)
            source_config = site.sources[self._source]
            with GroupState.open_for_disposition(
                source_database_path(self._config, self._spec.site, self._source),
                group_lock_path(self._config, self._spec.site, group),
                writes=self._write_context().for_operator(),
                **group_state_options(self._config.daemon, source_config, configured),
                site=self._spec.site,
                source=self._source,
                group=group,
                group_generation=current.group_generation,
                ordering=current.ordering if configured is None else configured.ordering,
                site_server_url=site.server,
                stream_fingerprint=source_stream_fingerprint(site, source_config),
                source_generation=authority.source_generation,
                verified_incarnation=incarnation,
                start_mode='resume',
            ) as state:
                return state.dispose(
                    request.request(
                        group,
                        reserved_generation,
                        expected_revision,
                    )
                )

        result = self.call(
            execute,
            queue_deadline_secs=self._coordination_deadline_secs,
            stop=self._stop,
            wait_phase=wait_phase,
        )
        return group_result_mapping(result)

    def _publish_health(
        self,
        poller: SourcePoller,
        server: EventServer,
        *,
        snapshot: SourceSnapshot | None = None,
    ) -> None:
        """Publish source and server-adapter health from this thread."""
        statuses = server.connected_group_statuses()
        buffers = server.socket_buffers
        now = time.monotonic()
        deadline = self._next_health_refresh_at
        if (
            snapshot is not None
            or self._source_health is None
            or self._source_health_dirty
            or (deadline is not None and now >= deadline)
        ):
            self._source_health = (
                poller.health if snapshot is None else poller.health_from_snapshot(snapshot)
            )
            self._source_health_dirty = False
            self._next_health_refresh_at = _advance_health_refresh_deadline(
                deadline,
                now,
                self._health_interval_secs,
            )
        source_health = self._source_health
        self._health_cache.publish_source(
            source_health,
            sample=self._watchdog.sample(),
        )
        self._health_cache.publish_event_socket(
            EventSocketHealth(
                source=self._source,
                group=None,
                role='server',
                connected=server.connected,
                requested=buffers.requested,
                send=buffers.send,
                receive=buffers.receive,
                queue_event_count=sum(status.queued_count for status in statuses),
                queue_byte_count=sum(status.queued_bytes for status in statuses),
                reason=None if server.connected else 'not-listening',
            )
        )

    def _service_internal(
        self,
        server: EventServer,
        surface: _SourceJoinSurface,
        poller: SourcePoller,
    ) -> None:
        """Drive endpoint work and grants on this thread."""
        surface.reconcile_groups()
        server.poll(0.0)
        server.dispatch_join_requests(surface)
        mutation_snapshot: SourceSnapshot | None = None
        while True:
            call = self._mailbox.take()
            if call is None:
                break
            capture = _ExceptionCapture()
            with capture:
                call.result = call.invoke(surface)
            if isinstance(capture.error, SourceOwnerError):
                raise capture.error.error from capture.error.error.__cause__
            raise_owner_write_failure(capture.error)
            call.error = capture.error
            # Synchronization only reads metadata and outboxes. Preserve a prior
            # mutation's snapshot or dirty flag while serving this read.
            if call.error is None:
                mutation_snapshot = self._health_snapshot_from_result(call.result)
            call.complete.set()
        while True:
            with self._grant_lock:
                grant = self._grants.popleft() if self._grants else None
            if grant is None:
                break
            ready = server.install_grant(grant)
            self._control.enqueue(ready)
        self._publish_pending_groups(surface)
        if mutation_snapshot is None:
            self._publish_health(poller, server)
        else:
            self._publish_health(poller, server, snapshot=mutation_snapshot)

    def _service(
        self,
        server: EventServer,
        surface: _SourceJoinSurface,
        poller: SourcePoller,
    ) -> None:
        """Publish one bounded internal phase around source-owned service."""
        with self._watchdog.watch(
            'internal',
            deadline_at=time.monotonic() + self._coordination_deadline_secs,
        ):
            self._service_internal(server, surface, poller)

    def _run_body(self) -> None:
        """Open and drive all source-owned resources on this thread."""
        site = self._config.sites[self._spec.site]
        source_config = site.sources[self._source]
        credential = site.credentials[source_config.credentials]
        source_generation = dict(self._assignment.source_generations)[self._source]
        inventory = {
            item.group: item
            for item in self._assignment.source_group_assignments
            if item.source == self._source
        }
        group_holdbacks: dict[str, float] = {
            group: source_registration_holdback_secs(site.groups[group], source_config)
            for group, item in inventory.items()
            if isinstance(item, ActiveSourceGroupAssignment)
        }
        strict_holdbacks = {
            group: holdback for group, holdback in group_holdbacks.items() if holdback > 0
        }
        database_path, socket_path = self._database_paths()
        state: SourceState | None = None
        server: EventServer | None = None
        poller: SourcePoller | None = None
        first_error: tuple[BaseException, TracebackType | None] | None = None
        try:
            provider = load_provider(
                self._config.daemon.sg_api_provider,
                {'sync'},
            )
            fanout = EventFanout()
            state = SourceState.open(
                database_path,
                create=True,
                writes=self._write_context(),
                **source_state_options(self._config.daemon, source_config),
                site=site.name,
                source=self._source,
                stream_fingerprint=source_stream_fingerprint(site, source_config),
                source_generation=source_generation,
            )
            strict_buffer = (
                None
                if not strict_holdbacks
                else StrictBuffer(
                    max_events=source_config.strict_max_events,
                    max_bytes=source_config.strict_max_bytes,
                    group_holdbacks=strict_holdbacks,
                    group_delivery_floors=dict.fromkeys(strict_holdbacks, 0),
                )
            )
            if (
                strict_buffer is not None
                and (strict_ranges := state.strict_ranges_if_changed()) is not None
            ):
                strict_buffer.synchronize_ranges(strict_ranges)
            self._checkpoint_view = self._checkpoint_host.register(
                database_path,
                source_identity(site.name, self._source),
                SOURCE_SCHEMA,
                log_extra={'site': site.name, 'source': self._source},
            )
            server = EventServer(
                path=socket_path,
                source=self._source,
                max_frame_bytes=self._config.daemon.max_ipc_frame_bytes,
                queue_max_entries=source_config.queue_max_events,
                queue_max_bytes=source_config.queue_max_bytes,
                socket_probe_timeout_secs=shutdown_slice_secs(
                    self._config.daemon.shutdown_deadline_secs
                ),
            )
            waiter = _ComponentWaiter(self._stop)
            poller = SourcePoller(
                state=state,
                fanout=fanout,
                provider=provider,
                site_url=site.server,
                credential=credential,
                config=source_config,
                max_ipc_frame_bytes=self._config.daemon.max_ipc_frame_bytes,
                source_generation=source_generation,
                waiter=waiter,
                service=waiter.pump,
                query_service=partial(server.poll, 0.0),
                random_value=random.random,
                watchdog=self._watchdog,
                internal_deadline_secs=self._coordination_deadline_secs,
                wait_tolerance_secs=watchdog_grace_secs(self._health_interval_secs),
                health_dirty=self._mark_source_health_dirty,
                worker_instance=self._assignment.spawn_instance,
                strict_buffer=strict_buffer,
                strict_retention_health=(
                    (lambda: ()) if strict_buffer is None else strict_buffer.retention_health
                ),
            )
            coordinator = JoinCoordinator(
                state=state,
                fanout=fanout,
                source_generation=source_generation,
                strict_retention=strict_buffer,
            )
            surface = _SourceJoinSurface(
                bootstrap_retry_secs=source_config.poll_retry_base_secs,
                coordinator=coordinator,
                poller=poller,
                state=state,
                inventory=inventory,
                strict_buffer=strict_buffer,
            )
            waiter.configure_service(
                lambda: self._service(
                    server,
                    surface,
                    poller,
                ),
            )
            self._poller = poller
            server.start()
            self._publish_health(poller, server)
            self._control.enqueue(
                SourceEndpointReady(
                    source=self._source,
                    source_generation=source_generation,
                    path=os.fspath(socket_path),
                )
            )
            poller.__enter__()
            while not self._stop.is_set():
                waiter.pump()
                try:
                    surface.reconcile_groups()
                    poller.run(max_cycles=1)
                except SourceHaltedError:
                    self._source_health_dirty = True
                    self._publish_health(poller, server)
                    source_health = self._source_health
                    if source_health is None or source_health.status not in HALTS_IN_PLACE:
                        raise
                    waiter.wait(source_config.fetch_interval_secs)
        except BaseException as error:  # noqa: BLE001
            first_error = (error, error.__traceback__)
        finally:
            self._shutdown_resources(poller, server, state, first_error)

    def close(self) -> None:
        """Retire health after the owner and monitor stop; `run` closes resources."""
        _retire_component_watchdog(self._watchdog, self._health_cache)

    def request_grant(self, grant: EventPeerGrant) -> None:
        """Post one exact event grant for source-thread installation.

        Raises:
            ValueError: The grant names a different source from this component.
        """
        if grant.source != self._source:
            raise ValueError('event grant source conflicts with component')
        with self._grant_lock:
            self._grants.append(grant)

    def request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        """Interrupt polling and retain the runtime's absolute deadline.

        Args:
            shutdown_deadline_at: Finite nonnegative monotonic deadline, or
                `None` to preserve the previously retained deadline.

        Raises:
            ValueError: The deadline is negative or non-finite.
        """
        if shutdown_deadline_at is not None:
            if not isfinite(shutdown_deadline_at) or shutdown_deadline_at < 0:
                raise ValueError('source shutdown deadline must be finite and nonnegative')
            self._shutdown_deadline_at = shutdown_deadline_at
        self._stop.set()
        poller = self._poller
        if poller is not None:
            poller.stop()

    def execute_operator_command(
        self,
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        """Execute one source-owned lifecycle command.

        Args:
            command: Group disposition, source-adoption preflight, or source
                adoption with exact target and audit arguments.
            wait_phase: Context factory publishing waits for source-thread
                work.

        Returns:
            A body-free mapping of group disposition or source adoption state.
            Adoption preflight also returns the reserved adoption identity,
            command fingerprint, and whether completion was already recorded.

        Raises:
            ValueError: The command, target, arguments, reason, or adoption
                reservation identity is invalid.
            RuntimeError: The source component stops while the command waits.
            Exception: Queue admission expires, or the source-owned state,
                provider, or wait-phase operation fails. Owner failures
                propagate unchanged.
        """
        if command.command == 'group-disposition':
            return self._execute_group_disposition(command, wait_phase=wait_phase)
        if command.command not in SOURCE_OPERATOR_COMMANDS:
            raise ValueError(f'unsupported source operator command {command.command!r}')
        expected_arguments = (
            SOURCE_ADOPTION_ARGUMENTS
            if command.command == 'preflight-source-adoption'
            else SOURCE_ADOPTION_EXECUTION_ARGUMENTS
        )
        if frozenset(command.arguments) != expected_arguments:
            raise ValueError('source adoption arguments are invalid')
        if (
            command.arguments.get('site') != self._spec.site
            or command.arguments.get('source') != self._source
        ):
            raise ValueError('source adoption target conflicts with owner')
        if (
            command.command == 'adopt-source-stream'
            and command.arguments.get('groups_parked') is not True
        ):
            raise ValueError('source adoption requires parked-group evidence')
        reason = _operator_reason(command.arguments)
        site = self._config.sites[self._spec.site]
        source_config = site.sources[self._source]
        semantic_arguments = {
            'site': self._spec.site,
            'source': self._source,
            'reason': reason,
        }
        command_fingerprint = stable_operator_id(
            'adopt-source-stream-fingerprint',
            semantic_arguments,
        )
        if command.command == 'preflight-source-adoption':
            reservation, snapshot = self.call(
                lambda surface: surface.reserve_stream_adoption(
                    command_fingerprint=command_fingerprint,
                    reason=reason,
                ),
                queue_deadline_secs=self._coordination_deadline_secs,
                stop=self._stop,
                wait_phase=wait_phase,
            )
            return {
                'adoption_id': reservation.adoption_id,
                'command_fingerprint': reservation.command_fingerprint,
                'recorded': reservation.recorded,
                'site': snapshot.site,
                'source': snapshot.source,
                'status': snapshot.status,
                'conditions': snapshot.conditions,
                'stream_incarnation': snapshot.stream_incarnation,
                'initialized': snapshot.initialized,
                'adoption_count': snapshot.adoption_count,
            }
        adoption_id = command.arguments.get('adoption_id')
        supplied_fingerprint = command.arguments.get('command_fingerprint')
        if (
            not isinstance(adoption_id, str)
            or not adoption_id
            or supplied_fingerprint != command_fingerprint
        ):
            raise ValueError('source adoption reservation identity is invalid')
        snapshot = self.call(
            lambda surface: surface.adopt_stream(
                adoption_id=adoption_id,
                command_fingerprint=command_fingerprint,
                new_stream_fingerprint=source_stream_fingerprint(site, source_config),
                reason=reason,
                groups_parked=True,
            ),
            queue_deadline_secs=self._coordination_deadline_secs,
            stop=self._stop,
            wait_phase=wait_phase,
        )
        return {
            'site': snapshot.site,
            'source': snapshot.source,
            'status': snapshot.status,
            'conditions': snapshot.conditions,
            'stream_incarnation': snapshot.stream_incarnation,
            'initialized': snapshot.initialized,
            'adoption_count': snapshot.adoption_count,
        }

    def run(self) -> None:
        """Complete thread-owned resources while retaining health until close.

        Run the owner's service loop on the calling thread until stop or
        failure, then clean up its thread-owned resources. Health registration
        remains until `close()`.

        Raises:
            Exception: Resource setup, owner-thread work, or cleanup fails.
        """
        self._run_body()


class _LaneComponentOptions(TypedDict):
    """Type one concrete lane component composition."""

    assignment: WorkerAssignment
    checkpoint_host: CheckpointHost
    config: Config
    control: WorkerControlOutbox
    group: str
    health_cache: _ComponentHealthCache
    limiter: _CatchupLimiter
    local_source: _SourceComponent | None
    spec: WorkerSpec


class _LocalJoinClient:
    """Expose the source join surface through a component-thread mailbox."""

    def __init__(
        self,
        source: _SourceComponent,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]],
        source_name: str,
        group: str,
        stop: threading.Event,
    ) -> None:
        """Store the source component that owns the join authority."""
        self._wait_phase = wait_phase
        self._group = group
        self._last_join_target: JoinTarget | None = None
        self._source = source
        self._source_name = source_name
        self._stop = stop

    def abort(self, abort: JoinAbort) -> None:
        """Abort one exact in-process join.

        Args:
            abort: Exact join attempt to abort.

        Raises:
            RuntimeError: The lane or source component stops.
        """
        self._source.call(
            lambda surface: surface.abort(abort),
            wait_phase=self._wait_phase,
            stop=self._stop,
        )

    def acknowledge_strict_control(
        self,
        control: StrictOrderingControl,
    ) -> None:
        """Acknowledge one exact in-process strict control.

        Args:
            control: Ordering halt committed by the lane.

        Raises:
            JoinProtocolError: The control is stale or is not finalized.
            RuntimeError: The lane or source component stops.
        """
        self._source.call(
            lambda surface: surface.acknowledge_strict_control(control),
            wait_phase=self._wait_phase,
            stop=self._stop,
        )

    def close(self) -> None:
        """Release no resources because the source component owns them."""

    def confirm(self, confirmation: JoinConfirm) -> Attached | JoinRetry:
        """Confirm one exact in-process join.

        Args:
            confirmation: Exact join boundary confirmation.

        Returns:
            Attached identity or retry instruction.

        Raises:
            JoinProtocolError: The attempt identity or phase conflicts.
            RuntimeError: The lane or source component stops.
        """
        return self._source.call(
            lambda surface: surface.confirm(confirmation),
            wait_phase=self._wait_phase,
            stop=self._stop,
        )

    def event_channel_lost(self, *, reason: str) -> None:
        """Retire one exact in-process live channel.

        Args:
            reason: Body-free channel-loss condition.

        Raises:
            RuntimeError: The lane or source component stops.
        """
        self._source.call(
            lambda surface: surface.event_channel_lost(
                self._group,
                reason=reason,
            ),
            wait_phase=self._wait_phase,
            stop=self._stop,
        )

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        """Finalize one exact in-process join.

        Args:
            finalization: Exact prepared join identity to finalize.

        Returns:
            Finalized join identity.

        Raises:
            JoinProtocolError: The attempt identity or phase conflicts.
            RuntimeError: The lane or source component stops.
        """
        return self._source.call(
            lambda surface: surface.finalize(finalization),
            wait_phase=self._wait_phase,
            stop=self._stop,
        )

    @property
    def last_join_target(self) -> JoinTarget | None:
        """Return the last captured in-process source observation.

        Returns:
            Current join target, or `None` before a target is captured or after
            a retry.
        """
        return self._last_join_target

    def observe_group(
        self,
        request: GroupBootstrapRequest,
    ) -> GroupBootstrapObservation | GroupBootstrapPending:
        """Capture one in-process source observation.

        Args:
            request: Exact group bootstrap observation request.

        Returns:
            Current source observation or pending retry instruction.

        Raises:
            ValueError: The request conflicts with the source inventory.
            RuntimeError: The lane or source component stops.
        """
        return self._source.call(
            lambda surface: surface.observe_group(request),
            wait_phase=self._wait_phase,
            stop=self._stop,
        )

    def request(
        self,
        request: JoinRequest,
        channel: GroupChannelInterface,
    ) -> JoinTarget | JoinRetry:
        """Capture one in-process source boundary.

        Args:
            request: Exact live-join request.
            channel: Attempt-owned bounded group fanout channel.

        Returns:
            Captured join target or retry instruction.

        Raises:
            JoinProtocolError: The request or attempt conflicts with source
                state.
            RuntimeError: The source stream is not initialized, or the lane or
                source component stops.
        """
        result = self._source.call(
            lambda surface: surface.request(request, channel),
            wait_phase=self._wait_phase,
            stop=self._stop,
        )
        self._last_join_target = result if isinstance(result, JoinTarget) else None
        return result


class _ObligationScanUnavailableError(Exception):
    """Defer a retained scan after an ordinary provider failure."""


class _LaneRefreshOptions(TypedDict):
    """Define one lane-owned body-free coordination driver."""

    lane: GroupLane
    slot: Callable[[], AbstractContextManager[None]]
    reader: CatchupReader
    state: GroupState
    joined: Callable[[], bool]
    fetch_interval_secs: float
    late_event_window_secs: float
    log_extra: Mapping[str, str]


@dataclass(frozen=True)
class _ScanView:
    """Hold one pass's group snapshot and strict obligation fence."""

    snapshot: GroupSnapshot
    strict_fence: tuple[str, int, int] | None


class _LaneRefresh:
    """Refresh durable range work on the lane thread."""

    def __init__(self, **options: Unpack[_LaneRefreshOptions]) -> None:
        """Store lane-thread-owned coordination authorities."""
        self._last_revision: int | None = None
        self._pending: tuple[PendingRange, ...] = ()
        self._lane = options['lane']
        self._slot = options['slot']
        self._mutation_snapshot: GroupSnapshot | None = None
        self._reader = options['reader']
        self._state = options['state']
        self._joined = options['joined']
        self._fetch_interval_secs = options['fetch_interval_secs']
        self._late_event_window_secs = options['late_event_window_secs']
        self._log_extra = options['log_extra']
        self._retries = CloseoutRetries(
            fetch_interval_secs=self._fetch_interval_secs,
            window_secs=self._late_event_window_secs,
        )

    def _eligible_scan_fence(
        self,
        revision: PendingRange,
        synchronization: SourceProgress,
        scan_fence: tuple[str, int, int],
    ) -> bool:
        """Recheck an exact lowest scan against current durable source evidence."""
        snapshot = self._state.snapshot()
        return (
            self._state.strict_obligation_fence() == scan_fence
            and (revision.obligation_id, revision.low_id, revision.high_id) == scan_fence
            and snapshot.source_generation == synchronization.source_generation
            and snapshot.verified_incarnation == synchronization.stream_incarnation
            and synchronization.source_successful_clock >= revision.eligibility_point
        )

    def _remember_mutation(self, snapshot: GroupSnapshot) -> GroupSnapshot:
        """Retain the latest complete snapshot committed by this cycle."""
        self._mutation_snapshot = snapshot
        return snapshot

    def _halt_history_truncated(self, error: HistoryTruncatedError) -> None:
        """Commit one reader-proven retained-history loss at configured durability."""
        snapshot = self._state.snapshot()
        self._remember_mutation(
            self._state.mark_history_truncated(
                error.retention_floor,
                expected_revision=snapshot.revision,
            )
        )

    def _require_retained(
        self,
        required_event_id: int | None,
        frontier: int,
    ) -> None:
        """Raise after committing any proven history loss at configured durability."""
        if required_event_id is None:
            return
        floor = _scan_provider_call(self._reader.retention_floor)
        error = history_truncation(floor, required_event_id, frontier)
        if error is None:
            return
        self._halt_history_truncated(error)
        raise error

    def _scan_ready(
        self,
        revision: PendingRange,
        synchronization: SourceProgress,
        scan_fence: tuple[str, int, int] | None,
        view: _ScanView,
    ) -> bool:
        """Reject ineligible work before any retention or missing-ID provider call."""
        snapshot = view.snapshot
        if not revision.closed:
            return False
        if scan_fence is not None and (
            view.strict_fence != scan_fence
            or (revision.obligation_id, revision.low_id, revision.high_id) != scan_fence
            or snapshot.source_generation != synchronization.source_generation
            or snapshot.verified_incarnation != synchronization.stream_incarnation
        ):
            return False
        if snapshot.ordering == 'eager':
            return snapshot.cursor >= revision.high_id
        if scan_fence is None:
            if view.strict_fence != (
                revision.obligation_id,
                revision.low_id,
                revision.high_id,
            ):
                return False
            if max(snapshot.cursor, snapshot.delivery_floor) < revision.low_id - 1:
                return False
        return synchronization.source_successful_clock >= revision.eligibility_point

    def _scan_view(self, snapshot: GroupSnapshot) -> _ScanView:
        """Read the strict fence once for a durable group snapshot."""
        return _ScanView(
            snapshot,
            self._state.strict_obligation_fence() if snapshot.ordering == 'strict' else None,
        )

    def _take_retry_turn(
        self,
        revision: PendingRange,
        synchronization: SourceProgress,
        scan_fence: tuple[str, int, int] | None,
        view: _ScanView,
        selection: dict[int, ObligationKey | None],
    ) -> bool:
        """Reuse retry selection until a durable mutation changes eligibility."""
        snapshot = view.snapshot
        key = (snapshot.group_generation, revision.obligation_id)
        if not self._retries.retrying(key):
            return True
        clock = synchronization.source_successful_clock
        if snapshot.revision not in selection:
            selection.clear()
            keys = [(snapshot.group_generation, item.obligation_id) for item in self._pending]
            # Check cadence and due times before issuing eligibility reads.
            if self._retries.next_retry(keys, clock) is not None:
                keys = [
                    (snapshot.group_generation, item.obligation_id)
                    for item in self._pending
                    if self._scan_ready(item, synchronization, scan_fence, view)
                    and (
                        snapshot.ordering != 'eager'
                        or not self._joined()
                        or clock >= item.visibility_deadline
                    )
                ]
            else:
                keys = []
            selection[snapshot.revision] = self._retries.next_retry(keys, clock)
        if key != selection[snapshot.revision]:
            return False
        self._retries.take_turn(clock)
        selection.clear()
        selection[snapshot.revision] = None
        return True

    def synchronize(self) -> SourceProgress:
        """Read durable source facts and refresh outstanding work unconditionally.

        Raises:
            DatabaseError: Reading or completing the SQLite read transaction
                fails.
            RuntimeError: Source metadata or its stream incarnation is missing.
        """
        progress, pending = self._state.refresh_ranges()
        self._last_revision = progress.range_revision
        if pending is not None:
            self._pending = pending
        return progress

    def _retry_after_pass(
        self,
        key: ObligationKey,
        synchronization: SourceProgress,
        *,
        missed: bool = False,
    ) -> bool:
        """Start retry waits from fresh completion evidence with the same identity."""
        progress = self.synchronize()
        snapshot = self._state.snapshot()
        if (
            snapshot.group_generation != key[0]
            or progress.source_generation != synchronization.source_generation
            or progress.stream_incarnation != synchronization.stream_incarnation
            or snapshot.source_generation != progress.source_generation
            or snapshot.verified_incarnation != progress.stream_incarnation
        ):
            return False
        if missed:
            return self._retries.missed(key, progress.source_successful_clock)
        self._retries.failed(key, progress.source_successful_clock)
        return False

    def _strict_prefix(
        self,
        revision: PendingRange,
        events: Sequence[Event],
        omitted: tuple[IdRun, ...],
        synchronization: SourceProgress,
        required_event_id: int | None,
    ) -> None:
        """Confirm a first omission only after its safe prefix finishes delivery."""
        clock = synchronization.source_successful_clock
        key = (self._state.snapshot().group_generation, revision.obligation_id)
        snapshot, consumed = self._lane.deliver_obligation_rows(
            revision,
            (event for event in events if cast('int', event['id']) < omitted[0][0]),
            source_successful_clock=clock,
        )
        self._remember_mutation(snapshot)
        if not consumed:
            self._retry_after_pass(key, synchronization)
            return
        self._require_retained(required_event_id, synchronization.frontier)
        if self._retry_after_pass(key, synchronization, missed=True):
            log_closeout_retrying(
                self._log_extra,
                revision.low_id,
                revision.high_id,
                omitted,
                self._late_event_window_secs,
            )

    def _fetch_strict_obligation(
        self,
        revision: PendingRange,
        missing: tuple[IdRun, ...],
        synchronization: SourceProgress,
    ) -> GroupSnapshot | None:
        """Confirm each omitted page before any callback can cross its missing IDs."""
        snapshot = self._state.snapshot()
        given_up = list(clip_runs(missing, high=snapshot.cursor))
        remaining = clip_runs(missing, low=snapshot.cursor + 1)
        required_event_id = remaining[0][0] if remaining else None
        clock = synchronization.source_successful_clock
        key = (snapshot.group_generation, revision.obligation_id)
        ended = self._retries.grace_ended(key, clock)
        try:
            self._require_retained(required_event_id, synchronization.frontier)
            pages = _scan_provider_call(partial(self._reader.fetch_id_runs, remaining))
            with closing(pages):
                while (page := _scan_provider_call(partial(next, pages, None))) is not None:
                    omitted = subtract_ids(
                        clip_runs(remaining, high=page.covered_through),
                        (cast('int', event['id']) for event in page.events),
                    )
                    remaining = clip_runs(remaining, low=page.covered_through + 1)
                    if omitted and not ended:
                        self._strict_prefix(
                            revision, page.events, omitted, synchronization, required_event_id
                        )
                        return None
                    if omitted:
                        self._require_retained(required_event_id, synchronization.frontier)
                        log_closeout_gave_up(
                            self._log_extra, revision.low_id, revision.high_id, omitted
                        )
                        given_up.extend(omitted)
                    snapshot, consumed = self._lane.deliver_obligation_rows(
                        revision, page.events, source_successful_clock=clock
                    )
                    self._remember_mutation(snapshot)
                    del page
                    if not consumed:
                        self._retry_after_pass(key, synchronization)
                        return None
            self._require_retained(required_event_id, synchronization.frontier)
            return self._remember_mutation(
                self._lane.complete_obligation(revision.obligation_id, excused=given_up)
            )
        except (HistoryTruncatedError, _ObligationScanUnavailableError):
            self._retry_after_pass(key, synchronization)
            return None

    def _fetch_obligation(
        self,
        revision: PendingRange,
        missing: tuple[IdRun, ...],
        synchronization: SourceProgress,
    ) -> GroupSnapshot | None:
        """Complete one retained fetch, stopping before completion on interruption."""
        if self._state.snapshot().ordering == 'strict':
            return self._fetch_strict_obligation(revision, missing, synchronization)
        required_event_id = missing[0][0] if missing else None
        clock = synchronization.source_successful_clock
        key = (self._state.snapshot().group_generation, revision.obligation_id)
        try:
            self._require_retained(required_event_id, synchronization.frontier)
            pages = _scan_provider_call(partial(self._reader.fetch_id_runs, missing))
            with closing(pages):
                while (page := _scan_provider_call(partial(next, pages, None))) is not None:
                    snapshot, consumed = self._lane.deliver_obligation_rows(
                        revision,
                        page.events,
                        source_successful_clock=synchronization.source_successful_clock,
                    )
                    self._remember_mutation(snapshot)
                    # The provider must never overlap ownership of two pages.
                    del page
                    if not consumed:
                        if snapshot.status == 'running':
                            self._retry_after_pass(key, synchronization)
                        return None
            self._require_retained(required_event_id, synchronization.frontier)
            omitted = self._state.missing_late_runs(revision.obligation_id)
            if omitted:
                snapshot = self._state.snapshot()
                if snapshot.ordering == 'eager' and self._retries.grace_ended(key, clock):
                    queued = self._lane.queued_event_ids(revision.low_id, revision.high_id)
                    if any(
                        low <= event_id <= high for low, high in omitted for event_id in queued
                    ):
                        self._retry_after_pass(key, synchronization)
                        return None
                    log_closeout_gave_up(
                        self._log_extra, revision.low_id, revision.high_id, omitted
                    )
                else:
                    if self._retry_after_pass(key, synchronization, missed=True):
                        log_closeout_retrying(
                            self._log_extra,
                            revision.low_id,
                            revision.high_id,
                            omitted,
                            self._late_event_window_secs,
                        )
                    return snapshot
            snapshot = self._remember_mutation(
                self._lane.complete_obligation(revision.obligation_id, excused=omitted)
            )
        except (HistoryTruncatedError, _ObligationScanUnavailableError):
            self._retry_after_pass(key, synchronization)
            return None
        return snapshot

    def _scan_obligations(
        self,
        synchronization: SourceProgress,
        scan_fence: tuple[str, int, int] | None = None,
        *,
        slot_held: bool = False,
    ) -> None:
        """Fetch only unfinished IDs once a closed obligation may be serviced."""
        snapshot = self._state.snapshot()
        view = self._scan_view(snapshot)
        self._retries.keep(
            {(snapshot.group_generation, item.obligation_id) for item in self._pending}
        )
        selection: dict[int, ObligationKey | None] = {}
        for revision in self._pending:
            if not self._scan_ready(revision, synchronization, scan_fence, view):
                continue
            missing = self._state.missing_late_runs(revision.obligation_id)
            if snapshot.ordering == 'eager':
                if not missing:
                    snapshot = self._remember_mutation(
                        self._lane.complete_obligation(revision.obligation_id)
                    )
                    view = self._scan_view(snapshot)
                    if snapshot.status != 'running':
                        return
                    self._pending = tuple(
                        item
                        for item in self._pending
                        if item.obligation_id != revision.obligation_id
                    )
                    continue
                if (
                    self._joined()
                    and synchronization.source_successful_clock < revision.visibility_deadline
                ):
                    continue
            if not self._take_retry_turn(revision, synchronization, scan_fence, view, selection):
                continue
            with nullcontext() if slot_held else self._slot():
                if (
                    snapshot.ordering == 'strict'
                    and view.strict_fence is not None
                    and not self._eligible_scan_fence(revision, synchronization, view.strict_fence)
                ):
                    return
                snapshot = (
                    self._fetch_obligation(revision, missing, synchronization)
                    or self._state.snapshot()
                )
            if snapshot.status != 'running' or scan_fence is not None:
                return
            self._pending = self._state.pending_ranges()
            view = self._scan_view(snapshot)

    def service(
        self,
        *,
        scan_obligations: bool = True,
        scan_fence: tuple[str, int, int] | None = None,
        force: bool = False,
        slot_held: bool = False,
    ) -> GroupSnapshot | None:
        """Refresh durable ranges and service eligible closed obligations.

        Args:
            scan_obligations: Whether to attempt scans for a running group.
                Source facts and pending ranges are refreshed even when false.
            scan_fence: Optional exact obligation identity and inclusive bounds
                limiting which strict scan may advance.
            force: Read pending-range rows even when their revision is
                unchanged.
            slot_held: Whether the caller already owns a catch-up reader slot.

        Returns:
            The last group snapshot changed during this pass, or `None` when no
            mutation was recorded. Outstanding scans still obey retry,
            visibility, ordering, and reader-slot admission rules.

        Raises:
            DatabaseError: A state transaction fails or write acquisition is
                stopped or expires under a bounded policy.
            sqlite3.Error: Reading durable group state outside a transaction
                fails.
            InvalidTransitionError: Durable state rejects an obligation
                transition.
            RevisionConflictError: A required group revision changed.
            ValueError: Retained manifest or delivery data is invalid.
            SecurityError: A source write-lock path fails safety checks.
            OSError: WAL inspection or lock acquisition or release fails.
            RuntimeError: Required source metadata is missing or a lock helper
                thread cannot start.
        """
        self._mutation_snapshot = None
        progress, pending = self._state.refresh_ranges(None if force else self._last_revision)
        self._last_revision = progress.range_revision
        if pending is not None:
            self._pending = pending
        if scan_obligations and self._state.snapshot().status == 'running':
            self._scan_obligations(progress, scan_fence, slot_held=slot_held)
        return self._mutation_snapshot


@dataclass(frozen=True)
class _LaneResources:
    """Hold thread-owned resources composed for one concrete lane."""

    context: _LaneConfiguration
    client: EventClient | _LocalJoinClient
    reader: CatchupReader
    state: GroupState
    lane: GroupLane
    channel: GroupChannelInterface
    coordination: _LaneRefresh


@dataclass
class _PartialLaneAttempt:
    """Track resources opened during one incomplete lane construction."""

    channel: GroupChannelInterface | None = None
    client: EventClient | _LocalJoinClient | None = None
    lane: GroupLane | None = None
    reader: CatchupReader | None = None
    state: GroupState | None = None


@dataclass
class _GroupHealthRefresh:
    """Retain a lane's projection, late-ID display, revision, and deadline."""

    cached: GroupHealth | None = None
    dirty: bool = True
    revision: int | None = None
    deadline_at: float | None = None
    late_projection: tuple[int, tuple[int, ...]] | None = None


class _LaneComponent:
    """Own one concrete group state, plugin host, and serial lane."""

    def __init__(self, **options: Unpack[_LaneComponentOptions]) -> None:
        """Store immutable lane bootstrap inputs without opening resources."""
        self._assignment = options['assignment']
        self._checkpoint_host = options['checkpoint_host']
        self._conditions = ConditionFlags()
        self._config = options['config']
        self._control = options['control']
        self._group = options['group']
        self._health_refresh = _GroupHealthRefresh()
        self._health_cache = options['health_cache']
        self._health_interval_secs = self._config.daemon.health_interval_secs
        self._lane: GroupLane | None = None
        self._lifecycle: _LaneLifecycle | None = None
        self._limiter = options['limiter']
        self._local_source = options['local_source']
        self._next_catchup_closeout_at = 0.0
        self._pending_health_snapshot: GroupSnapshot | None = None
        self._reload = threading.Event()
        self._resolved_recovery: set[str] = set()
        self._shutdown_deadline_at: float | None = None
        self._spec = options['spec']
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._mailbox = _OwnerMailbox[_LaneResources](self._group, self._wake)
        site = self._config.sites[self._spec.site]
        group = site.groups[self._group]
        source = site.sources[group.source]
        credential = site.credentials[source.credentials]
        self._internal_stall_deadline_secs = internal_stall_deadline_secs(
            self._health_interval_secs, credential
        )
        self._watchdog = ComponentWatchdog(
            kind='lane',
            identity=self._group,
            worker_instance=self._assignment.spawn_instance,
            registration_token=os.urandom(16).hex(),
            startup_deadline_secs=self._internal_stall_deadline_secs,
        )
        sample = self._watchdog.sample()
        self._health_cache.register_watchdog(
            key=(sample.kind, sample.identity, sample.worker_instance, sample.registration_token),
            supplier=self._watchdog.sample,
            conditions=self._conditions.names,
        )

    @staticmethod
    def _audit_context(
        command: OperatorCommand,
    ) -> tuple[AuditIdentity, str, float]:
        """Capture worker-local identity plus trusted control-peer attribution."""
        peer = command.identity
        return (
            AuditIdentity.current(),
            control_peer_label(peer),
            time.time(),
        )

    def _build_channel(
        self,
        context: _LaneConfiguration,
        state: GroupState,
    ) -> GroupChannelInterface:
        """Build one fresh attempt-owned bounded live channel."""
        channel = _DeliveryFloorChannel(
            group=self._group,
            ordering=context.group.ordering,
            max_events=context.source_config.queue_max_events,
            max_bytes=context.source_config.queue_max_bytes,
            delivery_floor=state.snapshot().delivery_floor,
        )
        channel.set_wake(self._wake)
        return channel

    def _call(
        self,
        invoke: Callable[
            [GroupState, EventClient | _LocalJoinClient],
            _Result,
        ],
    ) -> _Result:
        """Execute one command on the lane-owner thread."""
        admission_deadline: float | None = time.monotonic() + _LANE_OPERATOR_ADMISSION_SECS
        call = self._mailbox.submit(lambda resources: invoke(resources.state, resources.client))
        while True:
            if call.complete.is_set():
                return cast('_Result', _owner_call_result(call))
            if self._stop.is_set():
                self._mailbox.withdraw(call)
                error = ConnectionError('lane component stopped during local call')
                raise _CooperativeComponentStopError('local-call') from error
            wait_secs = min(0.1, shutdown_slice_secs(self._config.daemon.shutdown_deadline_secs))
            if admission_deadline is not None:
                remaining = admission_deadline - time.monotonic()
                if remaining <= 0:
                    if self._mailbox.withdraw(call):
                        raise _LaneCallBusyError('lane is busy; retry the command')
                    admission_deadline = None
                else:
                    wait_secs = min(wait_secs, remaining)
            call.complete.wait(wait_secs)

    def _catchup_deliveries(self, events: Sequence[Event]) -> Generator[Delivery]:
        """Yield the retained page suffix until component stop is requested."""
        for event in events:
            if self._stop.is_set():
                return
            yield Delivery(event)

    def _cleanup_deadline(self) -> float:
        """Return the retained runtime deadline or one bounded local fallback."""
        return _component_cleanup_deadline(
            self._shutdown_deadline_at,
            self._internal_stall_deadline_secs,
        )

    def _configuration(self) -> _LaneConfiguration:
        """Resolve the immutable configuration owned by this lane."""
        site = self._config.sites[self._spec.site]
        group = site.groups[self._group]
        source_config = site.sources[group.source]
        credential = site.credentials[source_config.credentials]
        group_assignment = next(
            item for item in self._assignment.group_assignments if item.group == self._group
        )
        return _LaneConfiguration(
            assignment=group_assignment,
            credential=credential,
            group=group,
            holdback_secs=source_registration_holdback_secs(group, source_config),
            site=site,
            source_config=source_config,
        )

    def _database_path(self) -> Path:
        """Create secure lock parents and return this group source database path."""
        ensure_secure_state_dir(self._config.daemon.state_dir)
        groups_dir = site_groups_directory(self._config, self._spec.site)
        ensure_secure_state_dir(groups_dir.parent.parent)
        ensure_secure_state_dir(groups_dir.parent)
        ensure_secure_state_dir(groups_dir)
        return source_database_path(
            self._config,
            self._spec.site,
            self._config.sites[self._spec.site].groups[self._group].source,
        )

    def _disposition_request(
        self,
        command: OperatorCommand,
        disposition_id: str,
    ) -> DispositionCommand:
        """Build one validated active-lane disposition request."""
        _identity, operator_label, wall_clock_time = self._audit_context(command)
        return disposition_command(
            command.command,
            command.arguments,
            disposition_id=disposition_id,
            wall_clock_time=wall_clock_time,
            operator_label=operator_label,
        )

    def _execute_reasoned_operator(
        self,
        state: GroupState,
        snapshot: GroupSnapshot,
        command: OperatorCommand,
    ) -> GroupSnapshot:
        """Execute one reason-bearing recovery against a captured revision."""
        with state.operator_writes():
            reason = _operator_reason(command.arguments)
            identity, operator_label, wall_clock_time = self._audit_context(command)
            if command.command == 'park':
                return state.park(
                    reason=reason,
                    identity=identity,
                    operator_label=operator_label,
                    wall_clock_time=wall_clock_time,
                    expected_revision=snapshot.revision,
                )
            if command.command == 'unpark':
                return state.unpark(
                    reason=reason,
                    identity=identity,
                    operator_label=operator_label,
                    wall_clock_time=wall_clock_time,
                    expected_revision=snapshot.revision,
                )
            if command.command == 'skip-callback':
                if snapshot.in_flight_event_id is None or snapshot.failure_ordinal is None:
                    raise ValueError('skip-callback requires a failed callback in flight')
                return state.skip_callback(
                    event_id=snapshot.in_flight_event_id,
                    callback_ordinal=snapshot.failure_ordinal,
                    reason=reason,
                    identity=identity,
                    operator_label=operator_label,
                    wall_clock_time=wall_clock_time,
                    expected_revision=snapshot.revision,
                )
            if command.command == 'restart-event':
                if snapshot.manifest is None or snapshot.in_flight_event_id is None:
                    raise ValueError('restart-event requires an event in flight')
                lane = self._lane
                if lane is None:
                    raise RuntimeError('restart-event requires an active lane')
                manifest = lane.restart_event_manifest(snapshot.in_flight_event_id)
                return state.restart_event(
                    manifest,
                    reason=reason,
                    identity=identity,
                    operator_label=operator_label,
                    wall_clock_time=wall_clock_time,
                    expected_revision=snapshot.revision,
                )
            if command.command == 'acknowledge-ordering-anomaly':
                if snapshot.ordering_anomaly_id is None:
                    raise ValueError('no ordering anomaly is awaiting acknowledgement')
                return state.acknowledge_ordering_anomaly(
                    event_id=snapshot.ordering_anomaly_id,
                    reason=reason,
                    identity=identity,
                    operator_label=operator_label,
                    wall_clock_time=wall_clock_time,
                    expected_revision=snapshot.revision,
                )
            raise ValueError(f'unsupported lane operator command {command.command!r}')

    def _execute_retry(
        self,
        state: GroupState,
        snapshot: GroupSnapshot,
        command: OperatorCommand,
    ) -> GroupSnapshot:
        """Retry failed setup or the exact failed callback or required event."""
        with state.operator_writes():
            lifecycle = self._lifecycle
            if lifecycle is not None and lifecycle.setup_failure is not None:
                if self._stop.is_set() or snapshot.status != 'running':
                    raise ValueError('setup retry requires a running group and lane')
                lane = self._lane
                if lane is None:
                    raise ValueError('setup retry requires an active lane')
                lifecycle.setup_attempted = True
                lifecycle.setup_failure = lane.setup()
                self._health_refresh.dirty = True
                if lifecycle.setup_failure is not None:
                    raise ValueError(f'setup retry failed again: {lifecycle.setup_failure}')
                return snapshot
            event_id = (
                snapshot.required_event_id
                if snapshot.required_event_id is not None
                else snapshot.in_flight_event_id
            )
            if event_id is None:
                raise ValueError('retry requires a failed callback or required event')
            identity, operator_label, wall_clock_time = self._audit_context(command)
            return state.retry(
                event_id=event_id,
                callback_ordinal=(
                    None if snapshot.required_event_id is not None else snapshot.failure_ordinal
                ),
                expected_revision=snapshot.revision,
                reason='operator retry',
                identity=identity,
                operator_label=operator_label,
                wall_clock_time=wall_clock_time,
            )

    def _health_snapshot_from_result(self, result: object) -> None:
        """Retain a complete mutation snapshot or dirty the projection."""
        self._pending_health_snapshot = _group_health_snapshot(result)
        self._health_refresh.dirty = self._pending_health_snapshot is None

    def _promote_group_assignment(self, group_generation: int) -> None:
        """Replace this lane's exact assignment after terminal disposition."""
        found = False
        group_assignments: list[DeliveryGroupAssignment] = []
        for assignment in self._assignment.group_assignments:
            if assignment.group != self._group:
                group_assignments.append(assignment)
                continue
            found = True
            group_assignments.append(
                replace(
                    assignment,
                    group_generation=group_generation,
                    start_mode='resume',
                    conditional_park=None,
                )
            )
        if not found:
            raise RuntimeError('active lane disposition assignment is missing')
        self._assignment = replace(
            self._assignment,
            group_assignments=tuple(group_assignments),
        )

    def _resolve_conditional_park(
        self,
        context: _LaneConfiguration,
        state: GroupState,
    ) -> _LaneConfiguration:
        """Resolve and acknowledge assigned recovery before source registration."""
        directive = context.assignment.conditional_park
        if directive is None:
            return context
        if directive.evidence_id in self._resolved_recovery:
            return replace(
                context,
                assignment=replace(
                    context.assignment,
                    conditional_park=None,
                ),
            )
        result = state.resolve_conditional_park(
            directive,
            identity=AuditIdentity.current(),
            decided_at_wall_clock=time.time(),
        )
        if result.evidence_id != directive.evidence_id:
            raise RuntimeError('group recovery result evidence identity conflicts')
        self._control.enqueue(
            GroupRecoveryReady(
                spawn_instance=self._assignment.spawn_instance,
                evidence_id=result.evidence_id,
                site=context.site.name,
                group=self._group,
                group_generation=context.assignment.group_generation,
                outcome=result.outcome,
                resulting_revision=result.resulting_revision,
            )
        )
        self._resolved_recovery.add(directive.evidence_id)
        return replace(
            context,
            assignment=replace(
                context.assignment,
                conditional_park=None,
            ),
        )

    @contextmanager
    def _watch_phase(
        self,
        phase: _LaneOwnerPhase,
        *,
        deadline_at: float | None = None,
    ) -> Generator[None]:
        """Watch worker-level lane work through the installed shared cell."""
        if self._lane is not None:
            with self._lane.watch_phase(phase, deadline_at=deadline_at):
                yield
            return
        if deadline_at is None:
            deadline_at = time.monotonic() + self._internal_stall_deadline_secs
        with self._watchdog.watch(phase, deadline_at=deadline_at):
            yield

    def _observe_source(
        self,
        client: EventClient | _LocalJoinClient,
        *,
        assignment: GroupBootstrapRequest,
    ) -> GroupBootstrapObservation:
        """Capture source incarnation and frontier before opening GroupState."""
        while not self._stop.is_set():
            try:
                with self._watch_phase('startup'):
                    result = client.observe_group(assignment)
            except ConnectionError as error:
                if isinstance(client, EventClient):
                    raise _RemoteBootstrapTransportError(str(error)) from error
                raise
            if isinstance(result, GroupBootstrapPending):
                if (
                    result.source != assignment.source
                    or result.group != assignment.group
                    or result.group_generation != assignment.group_generation
                ):
                    raise JoinProtocolError('group bootstrap pending identity conflicts')
                intended_wake_at = time.monotonic() + result.retry_after_secs
                with self._watch_phase(
                    'idle',
                    deadline_at=(
                        intended_wake_at + watchdog_grace_secs(self._health_interval_secs)
                    ),
                ):
                    stopped = self._stop.wait(result.retry_after_secs)
                if stopped:
                    break
                continue
            return result
        error = InterruptedError('lane stopped during source bootstrap')
        raise _CooperativeComponentStopError('bootstrap') from error

    def _provider_reader(
        self,
        context: _LaneConfiguration,
    ) -> tuple[Provider, CatchupReader]:
        """Build the selected provider and detached catch-up reader."""
        required = {'sync'}
        if context.group.execution == 'async':
            required.add('async')
        provider = load_provider(
            self._config.daemon.sg_api_provider,
            required,
        )
        reader = CatchupReader(
            provider=provider,
            site_url=context.site.server,
            credential=context.credential,
            batch_size=context.source_config.batch_size,
            fetch_interval_secs=context.source_config.fetch_interval_secs,
            waiter=_EventWaiter(self._stop),
            log_extra={
                'site': context.site.name,
                'source': context.group.source,
                'group': context.group.name,
            },
            provider_watch=lambda: self._watch_phase('internal'),
        )
        return provider, reader

    @contextmanager
    def _reader_slot(self) -> Generator[None]:
        """Wait for a reader permit in bounded, stop-aware idle phases."""
        interval = self._health_interval_secs
        slice_secs = shutdown_slice_secs(self._config.daemon.shutdown_deadline_secs)
        acquired = False
        while not acquired:
            phase_end = time.monotonic() + interval
            with self._watch_phase('idle', deadline_at=phase_end + watchdog_grace_secs(interval)):
                while not acquired:
                    if self._stop.is_set():
                        raise _CooperativeComponentStopError('reader-slot')
                    remaining = phase_end - time.monotonic()
                    if remaining <= 0:
                        break
                    acquired = self._limiter.acquire(timeout=min(slice_secs, remaining))
        try:
            yield
        finally:
            self._limiter.release()

    def _coordinate(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
        *,
        scan: bool,
        force: bool,
        observed: GroupSnapshot | None = None,
    ) -> None:
        """Run one due coordination tick across every persistent lane state."""
        current = time.monotonic()
        channel = lifecycle.channel
        hinted = isinstance(channel, _DeliveryFloorChannel) and channel.take_hint()
        if (
            observed is None
            and lifecycle.was_running
            and current < lifecycle.next_coordination_at
            and not hinted
            and not force
        ):
            return
        running = (
            resources.state.snapshot() if observed is None else observed
        ).status == 'running'
        force = force or (running and not lifecycle.was_running)
        lifecycle.was_running = running
        if current < lifecycle.next_coordination_at and not hinted and not force:
            return
        interval = resources.context.source_config.fetch_interval_secs
        lifecycle.next_coordination_at = current + interval
        try:
            with self._watch_phase('internal'):
                snapshot = resources.coordination.service(
                    scan_obligations=scan and running, force=force or hinted
                )
        except (ConnectionError, JoinProtocolError):
            self._health_snapshot_from_result(None)
            raise
        finally:
            lifecycle.next_coordination_at = time.monotonic() + interval
        if isinstance(snapshot, GroupSnapshot):
            lifecycle.was_running = snapshot.status == 'running'
            self._health_snapshot_from_result(snapshot)

    @staticmethod
    def _expected_lane_failure(error: BaseException, *, remote: bool) -> bool:
        """Classify the original cause without inspecting already settled owners."""
        if isinstance(error, LiveChannelDetachedError):
            return error.reason != 'channel-closed'
        return isinstance(error, JoinCapacityError) or (
            remote and isinstance(error, ConnectionError)
        )

    @staticmethod
    def _disposition_fence(command: OperatorCommand) -> _DispositionFence:
        """Validate and return one exact reserved generation fence."""
        orphan_generation = command.arguments.get('orphan_generation')
        reserved_generation = command.arguments.get('reserved_generation')
        if (
            type(orphan_generation) is not int
            or type(reserved_generation) is not int
            or orphan_generation <= 0
            or reserved_generation < orphan_generation
        ):
            raise ValueError('group disposition generation fence is invalid')
        return _DispositionFence(
            orphan_generation=orphan_generation,
            reserved_generation=reserved_generation,
        )

    def _execute_disposition_operator(
        self,
        state: GroupState,
        client: EventClient | _LocalJoinClient,
        command: OperatorCommand,
    ) -> Mapping[str, object]:
        """Apply reserved generation replacement on the lane owner thread."""
        del client
        with state.operator_writes():
            fence = self._disposition_fence(command)
            disposition_id = command.arguments.get('disposition_id')
            if not isinstance(disposition_id, str) or not disposition_id:
                raise ValueError('group disposition identity must be nonempty text')
            command_request = self._disposition_request(command, disposition_id)
            snapshot = state.snapshot()
            if fence.orphan_generation == fence.reserved_generation:
                terminal = state.terminal_disposition_identity(fence.reserved_generation)
                if terminal is not None and terminal[0] == disposition_id:
                    request = command_request.request(
                        self._group, fence.reserved_generation, snapshot.revision
                    )
                    return group_result_mapping(state.dispose(request))
                state.validate_disposition(
                    command_request.request(
                        self._group,
                        fence.reserved_generation + 1,
                        snapshot.revision,
                    )
                )
                return {'disposition_required': True}
            result = state.dispose(
                command_request.request(
                    self._group,
                    fence.reserved_generation,
                    snapshot.revision,
                )
            )
            self._promote_group_assignment(fence.reserved_generation)
            self._reload.set()
            return group_result_mapping(result)

    def _reconnect_remote(
        self,
        resources: _LaneResources,
        channel: GroupChannelInterface,
    ) -> tuple[GroupChannelInterface, bool]:
        """Reconnect the same grant and allocate a fresh join channel."""
        client = resources.client
        if not isinstance(client, EventClient):
            raise TypeError('local source client cannot reconnect remotely')
        with self._watch_phase('internal'):
            channel.close()
        with self._watch_phase('startup'):
            try:
                client.reconnect()
            except ConnectionError:
                return channel, False
        with self._watch_phase('internal'):
            replacement = self._build_channel(resources.context, resources.state)
        return replacement, True

    def _retire_channel(
        self,
        resources: _LaneResources,
        channel: GroupChannelInterface,
        *,
        reason: str,
    ) -> GroupChannelInterface:
        """Retire one live attempt and return a fresh detached channel."""
        with self._watch_phase('internal'):
            channel.close()
            client = resources.client
            if isinstance(client, EventClient):
                client.close()
            else:
                client.event_channel_lost(reason=reason)
            return self._build_channel(resources.context, resources.state)

    def _recover_lane_failure(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
        error: ConnectionError | JoinProtocolError,
    ) -> bool:
        """Retire an expected detach and rejoin through a fresh catch-up attempt."""
        remote = isinstance(resources.client, EventClient)
        if not self._expected_lane_failure(error, remote=remote):
            return False
        reason = (
            error.reason
            if isinstance(error, LiveChannelDetachedError)
            else 'strict-buffer-capacity'
            if isinstance(error, JoinCapacityError)
            else 'transport-lost'
        )
        if remote:
            lifecycle.channel, _connected = self._reconnect_remote(
                resources,
                lifecycle.channel,
            )
        else:
            lifecycle.channel = self._retire_channel(
                resources,
                lifecycle.channel,
                reason=reason,
            )
        lifecycle.joined = False
        lifecycle.rejoin_reason = reason
        return True

    def _coordinate_detached(self, resources: _LaneResources, lifecycle: _LaneLifecycle) -> None:
        """Service returned lifecycle calls and recover expected remote failures."""
        try:
            self._coordinate(resources, lifecycle, scan=False, force=False)
        except (ConnectionError, JoinProtocolError) as error:
            if not self._recover_lane_failure(resources, lifecycle, error):
                raise

    def _callback_gate_snapshot(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
    ) -> GroupSnapshot | None:
        """Open the callback gate and return this iteration's durable observation."""
        if self._stop.is_set():
            return None
        snapshot = resources.state.snapshot()
        if snapshot.status != 'running':
            lifecycle.was_running = False
            return None
        if not lifecycle.setup_attempted:
            lifecycle.setup_attempted = True
            lifecycle.setup_failure = resources.lane.setup()
            self._coordinate_detached(resources, lifecycle)
        if lifecycle.setup_failure is None:
            lifecycle.halt_logged = None
            return snapshot
        return None

    def _service_calls(self, resources: _LaneResources) -> None:
        """Execute every queued command on the group-owner thread."""
        while True:
            call = self._mailbox.take()
            if call is None:
                return
            capture = _ExceptionCapture()
            with capture, self._watch_phase('internal'):
                call.result = call.invoke(resources)
            raise_owner_write_failure(capture.error)
            call.error = capture.error
            if call.error is None:
                self._health_snapshot_from_result(call.result)
            call.complete.set()

    def _service_closeout_in_catch_up(self, resources: _LaneResources) -> None:
        """Finish eligible gaps between pages using catch-up's held reader permit."""
        if resources.state.snapshot().status == 'running':
            current = time.monotonic()
            if current >= self._next_catchup_closeout_at:
                interval = resources.context.source_config.fetch_interval_secs
                self._next_catchup_closeout_at = current + interval
                with self._watch_phase('internal'):
                    snapshot = resources.coordination.service(
                        scan_obligations=True, slot_held=True
                    )
                if snapshot is not None:
                    self._health_snapshot_from_result(snapshot)
        if resources.state.snapshot().status != 'running':
            raise _GroupHaltedCatchupError('group halted during eager catch-up')

    def _source_wait_phase(self) -> AbstractContextManager[None]:
        """Renew the lane's bounded phase while another owner serves a call."""
        interval = self._health_interval_secs
        return self._watch_phase(
            'idle', deadline_at=time.monotonic() + interval + watchdog_grace_secs(interval)
        )

    def _build_client(
        self,
        context: _LaneConfiguration,
    ) -> tuple[EventClient | _LocalJoinClient, str]:
        """Build the assigned local or authenticated remote source client."""
        with self._watch_phase('startup'):
            group = context.group
            if self._local_source is not None:
                client: EventClient | _LocalJoinClient = _LocalJoinClient(
                    self._local_source,
                    wait_phase=self._source_wait_phase,
                    source_name=group.source,
                    group=self._group,
                    stop=self._stop,
                )
                source_generation = dict(self._assignment.source_generations)[group.source]
                return client, source_generation
            endpoint = next(
                (
                    item
                    for item in self._assignment.source_endpoints
                    if item.source == group.source
                ),
                None,
            )
            grant = next(
                (
                    item
                    for item in self._assignment.event_grants
                    if item.source == group.source and item.group == self._group
                ),
                None,
            )
            if endpoint is None or grant is None:
                raise ValueError('dependent lane endpoint grant is missing')
            if endpoint.source_generation == '':
                raise ValueError('dependent lane source generation is empty')
            client = EventClient(
                path=Path(endpoint.path),
                source=group.source,
                group=self._group,
                spawn_instance=self._assignment.spawn_instance,
                capability=grant.capability,
                max_frame_bytes=self._config.daemon.max_ipc_frame_bytes,
                queue_max_entries=context.source_config.queue_max_events,
                queue_max_bytes=context.source_config.queue_max_bytes,
                coordination_deadline_secs=self._internal_stall_deadline_secs,
                stop=self._stop,
                wait_phase=self._source_wait_phase,
                wait_slice_secs=min(
                    self._health_interval_secs,
                    shutdown_slice_secs(self._config.daemon.shutdown_deadline_secs),
                ),
            )
            try:
                client.connect()
            except ConnectionError as error:
                client.close()
                raise _RemoteBootstrapTransportError(str(error)) from error
            except BaseException:
                client.close()
                raise
            return client, endpoint.source_generation

    def _synchronize(self, resources: _LaneResources) -> SourceProgress:
        """Bound the source progress read within one catch-up pass."""
        with self._watch_phase('internal'):
            return resources.coordination.synchronize()

    def _write_watch(
        self, phase: WritePhase, deadline_at: float, /
    ) -> AbstractContextManager[None]:
        """Publish source-file arbitration on this component's watchdog."""
        return self._watch_phase(phase, deadline_at=deadline_at)

    def _write_context(self) -> WriteContext:
        """Open the source-file lock owned by the next state facade."""
        daemon = self._config.daemon
        lock = SourceWriteLock(
            source_write_lock_path(
                self._config, self._spec.site, self._configuration().group.source
            ),
            database_path=self._database_path(),
            timeout_secs=write_lock_timeout_secs(
                daemon.health_interval_secs,
                daemon.supervisor_poll_interval_secs,
            ),
            wait_phase_grace_secs=watchdog_grace_secs(daemon.health_interval_secs),
        )
        group = self._configuration().group
        return WriteContext(
            lock,
            watch=self._write_watch,
            stop=self._stop,
            policy='wait',
            conditions=self._conditions,
            logger=logging.getLogger('sg_event_daemon.group'),
            log_extra={'site': self._spec.site, 'source': group.source, 'group': group.name},
            slice_secs=shutdown_slice_secs(daemon.shutdown_deadline_secs),
        )

    def _open_group_state(
        self,
        context: _LaneConfiguration,
        observation: GroupBootstrapObservation,
        reader: CatchupReader,
    ) -> GroupState:
        """Open initialized or resumed group state from captured source facts."""
        initial_delivery_floor = 0
        if context.assignment.start_mode == 'initialize':
            with self._reader_slot(), self._watch_phase('startup'), reader:
                try:
                    fresh_latest = reader.latest_visible_id()
                except _CooperativeComponentStopError:
                    raise
                except Exception as error:
                    raise _InitialGroupQueryError(str(error)) from error
            initial_delivery_floor = max(
                observation.captured_frontier,
                0 if fresh_latest is None else fresh_latest,
            )
        database_path = self._database_path()
        lock_path = group_lock_path(self._config, self._spec.site, self._group)
        options: GroupOpenOptions = {
            'writes': self._write_context(),
            **group_state_options(self._config.daemon, context.source_config, context.group),
            'site': context.site.name,
            'site_server_url': context.site.server,
            'source': context.group.source,
            'group': self._group,
            'group_generation': context.assignment.group_generation,
            'ordering': context.group.ordering,
            'stream_fingerprint': source_stream_fingerprint(context.site, context.source_config),
            'source_generation': observation.source_generation,
            'verified_incarnation': observation.stream_incarnation,
            'start_mode': context.assignment.start_mode,
        }
        if context.assignment.start_mode == 'initialize':
            options['initial_cursor'] = observation.captured_frontier
            options['initial_delivery_floor'] = initial_delivery_floor
            options['initial_last_delivered_id'] = initial_delivery_floor
        return GroupState.open(
            database_path,
            lock_path,
            **options,
        )

    def close(self) -> None:
        """Retire health after the owner and monitor stop; `run` closes resources."""
        _retire_component_watchdog(self._watchdog, self._health_cache)

    def execute_operator_command(
        self,
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        """Execute one audited recovery command against the owned group.

        Args:
            command: Exact group-targeted recovery command and audit arguments.
            wait_phase: Accepted for the shared component interface; unused by
                this lane's command path.

        Returns:
            A body-free mapping of the resulting group recovery state or the
            disposition command's recorded result.

        Raises:
            ValueError: The command target, arguments, or reason is invalid.
            RuntimeError: The lane stops or is unavailable while the command
                waits.
            Exception: The lane-owned recovery, provider, or coordination
                operation fails. Owner failures propagate unchanged.
        """
        del wait_phase
        _validate_group_operator_target(
            command,
            site=self._spec.site,
            group=self._group,
        )

        def _execute(
            state: GroupState,
            client: EventClient | _LocalJoinClient,
        ) -> GroupSnapshot | Mapping[str, object]:
            if command.command in {'replay', 'reset-group-to-latest'}:
                return self._execute_disposition_operator(state, client, command)
            snapshot = state.snapshot()
            if command.command == 'retry':
                return self._execute_retry(state, snapshot, command)
            return self._execute_reasoned_operator(state, snapshot, command)

        result = self._call(_execute)
        if isinstance(result, Mapping):
            return result
        return group_result_mapping(result)

    @staticmethod
    def _lane_health(
        resources: _LaneResources,
        snapshot: GroupSnapshot,
    ) -> LaneHealth:
        """Combine process-local lane phase with durable in-flight identity."""
        return _LaneComponent._snapshot_lane_health(resources.lane.health(), snapshot)

    def _group_health(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
        snapshot: GroupSnapshot,
        *,
        refresh_late_ids: bool,
    ) -> GroupHealth:
        """Build one complete immutable lane-thread health projection."""
        channel = lifecycle.channel.status
        join_target = resources.client.last_join_target
        catchup_frontier = (
            snapshot.latest_join_boundary if join_target is None else join_target.identity.boundary
        )
        if snapshot.parked:
            join_phase = 'parked'
        elif lifecycle.joined and channel.attached:
            join_phase = 'attached'
        elif isinstance(resources.client, EventClient) and not resources.client.connected:
            join_phase = 'detached'
        elif join_target is not None:
            join_phase = 'catching-up'
        else:
            join_phase = 'joining'
        late_projection = self._health_refresh.late_projection
        if refresh_late_ids or late_projection is None:
            late_projection = resources.state.unresolved_late_id_projection(
                limit=HEALTH_LATE_ID_LIMIT
            )
            self._health_refresh.late_projection = late_projection
        durable_health = _snapshot_health_options(
            snapshot,
            late_projection,
            resources.state.park_reason() if snapshot.parked else None,
            resources.state.audit_count(),
        )
        if lifecycle.setup_failure is not None:
            durable_health['failure_kind'] = 'setup'
            durable_health['failure_text'] = str(lifecycle.setup_failure)
            durable_health['conditions'] = (*snapshot.conditions, 'callback-failed')
        return GroupHealth.from_lane(
            self._lane_health(resources, snapshot),
            queue_event_count=channel.queued_count,
            queue_byte_count=channel.queued_bytes,
            catchup_frontier=catchup_frontier,
            join_phase=join_phase,
            **durable_health,
        )

    def _publish_health(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
    ) -> None:
        """Publish one owner-thread group projection to the monitor cache."""
        pending = self._pending_health_snapshot
        self._pending_health_snapshot = None
        health = _refresh_group_health(
            pending,
            self._health_refresh,
            self._health_interval_secs,
            resources.state.snapshot,
            lambda snapshot, refresh_late_ids: self._group_health(
                resources, lifecycle, snapshot, refresh_late_ids=refresh_late_ids
            ),
        )
        self._health_cache.publish_group(
            health,
            sample=self._watchdog.sample(),
        )
        client = resources.client
        if isinstance(client, EventClient):
            channel = lifecycle.channel.status
            buffers = client.socket_buffers
            self._health_cache.publish_event_socket(
                EventSocketHealth(
                    source=resources.context.group.source,
                    group=self._group,
                    role='client',
                    connected=client.connected,
                    requested=buffers.requested,
                    send=buffers.send,
                    receive=buffers.receive,
                    queue_event_count=channel.queued_count,
                    queue_byte_count=channel.queued_bytes,
                    reason=(channel.reason if client.connected else client.detach_reason),
                )
            )

    @staticmethod
    def _snapshot_lane_health(health: LaneHealth, snapshot: GroupSnapshot) -> LaneHealth:
        """Retain persisted in-flight identity before a callback begins running."""
        event_id = snapshot.in_flight_event_id
        manifest = snapshot.manifest
        if health.event_id is not None or event_id is None or manifest is None:
            return health
        ordinal = snapshot.failure_ordinal
        if ordinal is None:
            ordinal = next(
                (mark[0] for mark in snapshot.callback_marks if mark[3] == 'pending'),
                None,
            )
        callback = next(
            (item for item in manifest.callbacks if item.ordinal == ordinal),
            None,
        )
        mark = next(
            (item for item in snapshot.callback_marks if item[0] == ordinal),
            None,
        )
        return replace(
            health,
            event_id=event_id,
            plugin_id=None if callback is None else callback.plugin_id,
            callback_id=None if callback is None else callback.callback_id,
            delivery_attempt=None if mark is None else mark[4],
        )

    @staticmethod
    def _poll_remote(client: EventClient) -> None:
        """Drain readable transport frames before releasing live bodies."""
        timeout = 0.01
        while True:
            added = client.poll(timeout)
            if not client.connected:
                raise ConnectionError('remote event channel detached')
            if added == 0:
                return
            timeout = 0.0

    def _poll_lane(self, client: EventClient | _LocalJoinClient) -> None:
        """Poll a remote channel or briefly yield one local lane thread."""
        intended_wake_at = time.monotonic() + 0.01
        with self._watch_phase(
            'idle',
            deadline_at=(intended_wake_at + watchdog_grace_secs(self._health_interval_secs)),
        ):
            if isinstance(client, EventClient):
                self._poll_remote(client)
            else:
                self._wake.wait(0.01)
                self._wake.clear()

    def _publish_startup_group_health(
        self, context: _LaneConfiguration, state: GroupState
    ) -> None:
        """Publish persisted lane state before recovery can wait on an outside writer."""
        snapshot = state.snapshot()
        sample = self._watchdog.sample()
        lane = self._snapshot_lane_health(
            LaneHealth(
                site=context.site.name,
                source=context.group.source,
                group=self._group,
                worker_instance=self._assignment.spawn_instance,
                execution=context.group.execution,
                ordering=context.group.ordering,
                phase=cast('GroupPhase', sample.phase),
                status=snapshot.status,
                cursor=snapshot.cursor,
                event_id=sample.event_id,
                plugin_id=sample.plugin_id,
                callback_id=sample.callback_id,
                delivery_attempt=sample.delivery_attempt,
                heartbeat_at=sample.heartbeat_at,
                heartbeat_age=max(0.0, time.monotonic() - sample.heartbeat_at),
                deadline_at=sample.deadline_at,
                internal_stall_deadline_secs=self._internal_stall_deadline_secs,
                overdue_reason=None,
            ),
            snapshot,
        )
        late_count, late_ids = state.unresolved_late_id_projection(limit=HEALTH_LATE_ID_LIMIT)
        health = GroupHealth.from_lane(
            lane,
            queue_event_count=0,
            queue_byte_count=0,
            catchup_frontier=snapshot.latest_join_boundary,
            join_phase='parked' if snapshot.parked else 'joining',
            **_snapshot_health_options(
                snapshot,
                (late_count, late_ids),
                state.park_reason() if snapshot.parked else None,
                state.audit_count(),
            ),
        )
        self._health_cache.publish_group(health, sample=sample)

    @staticmethod
    def _validate_source_generation(
        observation: GroupBootstrapObservation,
        expected: str,
    ) -> None:
        """Reject a source observation that conflicts with the assignment."""
        if observation.source_generation != expected:
            raise ValueError('source bootstrap generation conflicts with assignment')

    def _close_partial_attempt(
        self,
        attempt: _PartialLaneAttempt,
        first_error: tuple[BaseException, TracebackType | None] | None = None,
    ) -> None:
        """Close only resources opened by one failed construction attempt."""
        try:
            self._watchdog.transition('shutdown', deadline_at=self._cleanup_deadline())
            _close_lane_resources(attempt, attempt.channel, first_error)
        finally:
            self._lane = None

    def _build_resources(self, context: _LaneConfiguration) -> _LaneResources:
        """Compose all resources after source observation and state registration."""
        attempt = _PartialLaneAttempt()
        try:
            with self._watch_phase('startup'):
                client, expected_source_generation = self._build_client(context)
                attempt.client = client
                observation = self._observe_source(
                    client,
                    assignment=GroupBootstrapRequest(
                        source=context.group.source,
                        group=self._group,
                        group_generation=context.assignment.group_generation,
                        start_mode=context.assignment.start_mode,
                    ),
                )
                self._validate_source_generation(
                    observation,
                    expected_source_generation,
                )
                provider, reader = self._provider_reader(context)
                attempt.reader = reader
                state = self._open_group_state(context, observation, reader)
                attempt.state = state
                self._publish_startup_group_health(context, state)
                context = self._resolve_conditional_park(context, state)
                plugins = _load_shared_plugins(
                    lambda: load_plugins(context.site, context.group, provider)
                )
                lane = GroupLane(
                    state=state,
                    site=context.site,
                    group=context.group,
                    provider=provider,
                    plugins=plugins,
                    worker_instance=self._assignment.spawn_instance,
                    health_interval_secs=self._config.daemon.health_interval_secs,
                    watchdog=self._watchdog,
                    async_cancel_grace_secs=self._config.daemon.async_cancel_grace_secs,
                    wait=self._stop.wait,
                )
                self._lane = lane
                attempt.lane = lane
                channel = self._build_channel(context, state)
                attempt.channel = channel
                coordination = _LaneRefresh(
                    lane=lane,
                    slot=self._reader_slot,
                    reader=reader,
                    state=state,
                    joined=lambda: (
                        self._lifecycle is not None
                        and self._lifecycle.joined
                        and self._lifecycle.channel.status.attached
                    ),
                    fetch_interval_secs=context.source_config.fetch_interval_secs,
                    late_event_window_secs=context.source_config.late_event_window_secs,
                    log_extra={
                        'site': context.site.name,
                        'source': context.group.source,
                        'group': context.group.name,
                    },
                )
                resources = _LaneResources(
                    context=context,
                    client=client,
                    reader=reader,
                    state=state,
                    lane=lane,
                    channel=channel,
                    coordination=coordination,
                )
        except BaseException as error:
            self._close_partial_attempt(
                attempt,
                (error, error.__traceback__),
            )
            raise
        intended_wake_at = time.monotonic() + context.source_config.fetch_interval_secs
        self._watchdog.transition(
            'idle',
            deadline_at=(
                intended_wake_at + watchdog_grace_secs(self._config.daemon.health_interval_secs)
            ),
        )
        return resources

    def _pause(
        self,
        resources: _LaneResources,
        *,
        seconds: float | None = None,
        recover_remote: bool = True,
        interrupt_on_wake: bool = True,
    ) -> None:
        """Wait with ticks, retaining join-owned recovery during catch-up."""
        interval = (
            resources.context.source_config.fetch_interval_secs if seconds is None else seconds
        )
        intended_wake_at = time.monotonic() + interval
        lifecycle = self._lifecycle
        while not self._stop.is_set():
            if lifecycle is not None:
                if recover_remote:
                    self._coordinate_detached(resources, lifecycle)
                else:
                    self._coordinate(resources, lifecycle, scan=False, force=False)
            current = time.monotonic()
            if current >= intended_wake_at:
                return
            wake_at = intended_wake_at
            if lifecycle is not None:
                wake_at = min(wake_at, lifecycle.next_coordination_at)
            with self._watch_phase(
                'idle',
                deadline_at=(wake_at + watchdog_grace_secs(self._health_interval_secs)),
            ):
                awakened = self._wake.wait(max(0.0, wake_at - current))
                self._wake.clear()
            if awakened:
                if lifecycle is not None:
                    if recover_remote:
                        self._coordinate_detached(resources, lifecycle)
                    else:
                        self._coordinate(resources, lifecycle, scan=False, force=False)
                if interrupt_on_wake:
                    return

    def _drive_halted(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
    ) -> None:
        """Service body-free work while one lane cannot run callbacks."""
        snapshot = resources.state.snapshot()
        status = (
            'callback-failed'
            if snapshot.status == 'running' and lifecycle.setup_failure is not None
            else snapshot.status
        )
        if status not in {'running', 'parked'} and lifecycle.halt_logged != status:
            group = resources.context.group
            logging.getLogger('sg_event_daemon.group').warning(
                'group halted: %s; delivery stops until it is cleared',
                status,
                extra={'site': self._spec.site, 'source': group.source, 'group': group.name},
            )
            lifecycle.halt_logged = status
        if lifecycle.joined:
            lifecycle.channel = self._retire_channel(
                resources,
                lifecycle.channel,
                reason='lane-halted',
            )
            lifecycle.joined = False
        try:
            self._pause(resources)
        except (ConnectionError, JoinProtocolError) as error:
            self._health_snapshot_from_result(None)
            if not self._recover_lane_failure(resources, lifecycle, error):
                raise

    def _resume_lane(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
    ) -> bool:
        """Resume durable callback work before one detached lane joins."""
        if lifecycle.joined:
            return True
        try:
            snapshot = resources.lane.resume()
        except InterruptedError as error:
            if not self._stop.is_set():
                raise
            raise _CooperativeComponentStopError('resume') from error
        self._health_snapshot_from_result(snapshot)
        self._coordinate_detached(resources, lifecycle)
        if self._pending_health_snapshot is not None:
            snapshot = self._pending_health_snapshot
        if snapshot.status == 'running':
            return True
        self._drive_halted(resources, lifecycle)
        return False

    def _service_catchup(
        self,
        resources: _LaneResources,
        seconds: float = 0.0,
        *,
        phase: _ComponentStopPhase = 'eager-catchup',
    ) -> None:
        """Service due body-free work after catch-up calls or during gap waits."""
        if self._stop.is_set():
            if seconds:
                raise _CooperativeComponentStopError(phase)
            return
        if self._lifecycle is not None:
            if seconds:
                self._pause(
                    resources, seconds=seconds, recover_remote=False, interrupt_on_wake=False
                )
            else:
                self._coordinate(resources, self._lifecycle, scan=False, force=False)
        elif seconds:
            with self._watch_phase(
                'idle',
                deadline_at=time.monotonic()
                + seconds
                + watchdog_grace_secs(self._health_interval_secs),
            ):
                if self._stop.wait(seconds):
                    raise _CooperativeComponentStopError(phase)
        if seconds and self._stop.is_set():
            raise _CooperativeComponentStopError(phase)

    def _catch_up_strict(
        self,
        resources: _LaneResources,
        join_target: JoinTarget,
    ) -> None:
        """Drive strict catch-up to one captured live boundary."""
        state = resources.state

        def deliver_strict(
            events: Sequence[Event], _ordering_time: float
        ) -> StrictDeliveryOutcome:
            snapshot, consumed = resources.lane.deliver_many_counted(
                self._catchup_deliveries(events),
                deadline_at=(
                    self._lifecycle.next_coordination_at
                    if self._lifecycle is not None
                    else float('inf')
                ),
            )
            accepted = (
                consumed > 0
                and snapshot.status == 'running'
                and snapshot.cursor >= cast('int', events[consumed - 1]['id'])
                and snapshot.in_flight_event_id is None
            )
            return StrictDeliveryOutcome(
                accepted=accepted,
                continue_epoch=accepted and not self._stop.is_set(),
                consumed=consumed,
            )

        while not self._stop.is_set():
            self._service_catchup(resources)
            try:
                with self._reader_slot():
                    strict_result = resources.reader.strict_step(
                        state=state,
                        source_progress=partial(self._synchronize, resources),
                        deliver=deliver_strict,
                        holdback_secs=resources.context.holdback_secs,
                        max_pending_epochs=self._config.daemon.max_pending_epochs,
                        unresolved_bounds=state.unresolved_obligation_bounds(),
                        join_boundary=join_target.identity.boundary,
                        service=partial(self._service_catchup, resources, phase='strict-catchup'),
                    )
            except HistoryTruncatedError as error:
                state.mark_history_truncated(
                    error.retention_floor,
                    expected_revision=state.snapshot().revision,
                )
                raise _GroupHaltedCatchupError('group halted during strict catch-up') from error
            self._service_catchup(resources)
            if self._stop.is_set():
                break
            with self._watch_phase('internal'):
                snapshot = state.snapshot()
                if snapshot.status != 'running':
                    raise _GroupHaltedCatchupError('group halted during strict catch-up')
                fence = state.strict_obligation_fence()
                if (
                    fence is not None
                    and strict_result.exhausted_fence == fence
                    and strict_result.exhausted_through == fence[1] - 1
                    and snapshot.epochs
                    and snapshot.epochs[0].epoch_id == strict_result.exhausted_epoch_id
                    and snapshot.epochs[0].state == 'delivering'
                    and snapshot.source_generation == strict_result.source_generation
                    and snapshot.verified_incarnation == strict_result.stream_incarnation
                ):
                    resources.coordination.service(scan_fence=fence)
                if (
                    strict_result.join_ready
                    and not snapshot.epochs
                    and (fence is None or fence[1] > join_target.identity.boundary)
                ):
                    return
            self._service_catchup(
                resources,
                resources.context.source_config.fetch_interval_secs,
                phase='strict-catchup',
            )
        error = InterruptedError('lane stopped during strict catch-up')
        raise _CooperativeComponentStopError('strict-catchup') from error

    def _deliver_catchup_page(
        self, resources: _LaneResources, events: Sequence[Event]
    ) -> tuple[GroupSnapshot, int]:
        """Commit bounded page prefixes before servicing due coordination."""
        consumed = 0
        while True:
            self._service_catchup(resources)
            snapshot, count = resources.lane.deliver_many_counted(
                self._catchup_deliveries(events[consumed:]),
                deadline_at=(
                    self._lifecycle.next_coordination_at
                    if self._lifecycle is not None
                    else float('inf')
                ),
            )
            consumed += count
            prior_mutation = self._pending_health_snapshot
            self._service_catchup(resources)
            if (
                self._pending_health_snapshot is not None
                and self._pending_health_snapshot is not prior_mutation
            ):
                snapshot = self._pending_health_snapshot
            if consumed == len(events) or snapshot.status != 'running' or self._stop.is_set():
                return snapshot, consumed
            if resources.lane.shutdown_requested():
                return snapshot, consumed

    def _catch_up_eager(
        self,
        resources: _LaneResources,
        join_target: JoinTarget,
    ) -> None:
        """Drive eager catch-up to one captured live boundary."""
        state = resources.state
        snapshot = state.snapshot()

        def deliver_page(events: Sequence[Event]) -> None:
            delivered, _consumed = self._deliver_catchup_page(resources, events)
            if resources.lane.shutdown_requested():
                raise _CooperativeComponentStopError('eager-catchup')
            if delivered.status != 'running' or _consumed != len(events):
                raise _GroupHaltedCatchupError('group halted during eager catch-up')

        try:
            resources.reader.catch_up(
                cursor=snapshot.cursor,
                frontier=join_target.identity.boundary,
                delivery_floor=snapshot.delivery_floor,
                source_progress=partial(self._synchronize, resources),
                deliver_page=deliver_page,
                between_pages=partial(self._service_closeout_in_catch_up, resources),
                replay_from=snapshot.replay_required_from,
                unresolved_bounds=state.unresolved_obligation_bounds(),
                service=partial(self._service_catchup, resources),
            )
        except HistoryTruncatedError as error:
            state.mark_history_truncated(
                error.retention_floor,
                expected_revision=state.snapshot().revision,
            )
            raise _GroupHaltedCatchupError('group halted during eager catch-up') from error
        current = state.snapshot()
        if current.status != 'running':
            raise _GroupHaltedCatchupError('group halted during eager catch-up')
        # The reader omits callbacks at or below the initialized delivery floor;
        # persist that authorized skip when it alone reaches the join boundary.
        skipped_through = min(snapshot.delivery_floor, join_target.identity.boundary)
        if current.cursor < skipped_through:
            state.advance_irrelevant(skipped_through, expected_revision=current.revision)

    def _catch_up(self, resources: _LaneResources, _frontier: int) -> None:
        """Drive eager or strict catch-up to the captured live boundary."""
        lifecycle = self._lifecycle
        if lifecycle is not None and lifecycle.rejoin_reason is not None:
            group = resources.context.group
            logging.getLogger('sg_event_daemon.group').info(
                'live channel detached: %s; rejoining through catch-up',
                lifecycle.rejoin_reason,
                extra={'site': self._spec.site, 'source': group.source, 'group': group.name},
            )
            lifecycle.rejoin_reason = None
        with self._watch_phase('internal'):
            join_target = resources.client.last_join_target
            if join_target is None or join_target.stream_incarnation is None:
                raise RuntimeError('source join observation is unavailable')
        if resources.context.group.ordering == 'eager':
            with self._reader_slot():
                self._catch_up_eager(resources, join_target)
        else:
            self._catch_up_strict(resources, join_target)

    def _join_lane(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
    ) -> bool:
        """Attempt one fresh-token join and retain only a live result."""
        try:
            result = resources.lane.join(
                resources.client,
                lifecycle.channel,
                catch_up=partial(self._catch_up, resources),
            )
        except _GroupHaltedCatchupError:
            return False
        except (ConnectionError, JoinProtocolError) as error:
            if not self._recover_lane_failure(resources, lifecycle, error):
                raise
            self._pause(resources)
            return False
        if isinstance(result, JoinRetry):
            with self._watch_phase('internal'):
                lifecycle.channel.close()
                lifecycle.channel = self._build_channel(
                    resources.context,
                    resources.state,
                )
            self._pause(
                resources,
                seconds=max(
                    result.retry_after_secs,
                    resources.context.source_config.fetch_interval_secs,
                ),
            )
            return False
        lifecycle.joined = True
        lifecycle.next_coordination_at = time.monotonic()
        return True

    def _try_remote_connect(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
    ) -> bool:
        """Reconnect a currently detached remote client before joining."""
        client = resources.client
        if not isinstance(client, EventClient) or client.connected:
            return True
        lifecycle.channel, connected = self._reconnect_remote(
            resources,
            lifecycle.channel,
        )
        if not connected:
            self._pause(resources)
        return connected

    def request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        """Interrupt owned work and retain the runtime's absolute deadline.

        Args:
            shutdown_deadline_at: Finite nonnegative monotonic deadline, or
                `None` to preserve the previously retained deadline.

        Raises:
            ValueError: The deadline is negative or non-finite.
            RuntimeError: The asynchronous callback cancellation timer cannot
                start.
        """
        if shutdown_deadline_at is not None:
            if not isfinite(shutdown_deadline_at) or shutdown_deadline_at < 0:
                raise ValueError('lane shutdown deadline must be finite and nonnegative')
            self._shutdown_deadline_at = shutdown_deadline_at
        self._stop.set()
        self._wake.set()
        lane = self._lane
        if lane is not None:
            lane.request_shutdown()

    def _service_due(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
        *,
        observed: GroupSnapshot | None = None,
    ) -> None:
        """Service one live tick with scans enabled only for running groups."""
        self._coordinate(resources, lifecycle, scan=True, force=False, observed=observed)

    def _drive_live(
        self,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
        *,
        idle_snapshot: GroupSnapshot | None = None,
    ) -> None:
        """Poll, deliver, and coordinate one bounded live lane cycle."""
        try:
            self._poll_lane(resources.client)
            prior_mutation = self._pending_health_snapshot
            self._service_due(resources, lifecycle, observed=idle_snapshot)
            if self._pending_health_snapshot is not prior_mutation:
                idle_snapshot = None
            snapshot = resources.lane.drain_live(
                lifecycle.channel,
                idle_snapshot=idle_snapshot,
                deadline_at=lifecycle.next_coordination_at,
            )
            self._health_snapshot_from_result(snapshot)
            self._service_due(resources, lifecycle, observed=snapshot)
        except (ConnectionError, JoinProtocolError) as error:
            if not self._recover_lane_failure(resources, lifecycle, error):
                raise
        else:
            if snapshot.status != 'running':
                lifecycle.channel = self._retire_channel(
                    resources,
                    lifecycle.channel,
                    reason='lane-halted',
                )
                lifecycle.joined = False

    def _drive_lane(self, resources: _LaneResources) -> None:
        """Persist one lane across local halts and remote source restarts."""
        lifecycle = _LaneLifecycle(channel=resources.channel)
        self._lifecycle = lifecycle
        first_error: tuple[BaseException, TracebackType | None] | None = None
        try:
            with self._watch_phase('startup'):
                resources.reader.__enter__()
            while not self._stop.is_set():
                self._service_calls(resources)
                if self._reload.is_set():
                    break
                self._publish_health(resources, lifecycle)
                reuse_idle_snapshot = lifecycle.joined and lifecycle.setup_attempted
                prior_channel = lifecycle.channel
                snapshot = self._callback_gate_snapshot(resources, lifecycle)
                if snapshot is None:
                    self._drive_halted(resources, lifecycle)
                    self._publish_health(resources, lifecycle)
                    continue
                if not self._resume_lane(resources, lifecycle):
                    self._publish_health(resources, lifecycle)
                    continue
                if not self._try_remote_connect(resources, lifecycle):
                    self._publish_health(resources, lifecycle)
                    continue
                if not lifecycle.joined and not self._join_lane(
                    resources,
                    lifecycle,
                ):
                    self._publish_health(resources, lifecycle)
                    continue
                self._drive_live(
                    resources,
                    lifecycle,
                    idle_snapshot=(
                        snapshot
                        if reuse_idle_snapshot and lifecycle.channel is prior_channel
                        else None
                    ),
                )
                self._publish_health(resources, lifecycle)
        except BaseException as error:  # noqa: BLE001
            first_error = (error, error.__traceback__)
        finally:
            try:
                self._watchdog.transition('shutdown', deadline_at=self._cleanup_deadline())
                _close_lane_resources(resources, lifecycle.channel, first_error)
            finally:
                self._lane = None
                self._lifecycle = None

    def _run_body(self) -> None:
        """Open and drive all lane-owned resources on this thread."""
        query_retry_attempt = 0
        while not self._stop.is_set():
            self._watchdog.transition(
                'startup', deadline_at=time.monotonic() + self._internal_stall_deadline_secs
            )
            context = self._configuration()
            try:
                resources = self._build_resources(context)
            except _InitialGroupQueryError as error:
                query_retry_attempt += 1
                source = context.source_config
                delay = saturating_retry_delay(
                    query_retry_attempt,
                    base_secs=source.poll_retry_base_secs,
                    max_secs=source.poll_retry_max_secs,
                    multiplier=source.poll_retry_multiplier,
                )
                delay = jitter_retry_delay(
                    delay,
                    fraction=source.poll_retry_jitter_fraction,
                    sample=random.SystemRandom().random(),
                )
                logging.getLogger('sg_event_daemon.group').warning(
                    'could not read the latest event from SG to start the group: '
                    '%s; retrying in %.1fs',
                    error,
                    delay,
                    extra={
                        'site': context.site.name,
                        'source': context.group.source,
                        'group': context.group.name,
                    },
                )
                intended_wake_at = time.monotonic() + delay
                self._watchdog.transition(
                    'idle',
                    deadline_at=(
                        intended_wake_at + watchdog_grace_secs(self._health_interval_secs)
                    ),
                )
                self._stop.wait(delay)
                continue
            except _RemoteBootstrapTransportError:
                intended_wake_at = time.monotonic() + context.source_config.fetch_interval_secs
                self._watchdog.transition(
                    'idle',
                    deadline_at=(
                        intended_wake_at + watchdog_grace_secs(self._health_interval_secs)
                    ),
                )
                self._stop.wait(context.source_config.fetch_interval_secs)
                continue
            query_retry_attempt = 0
            self._health_refresh = _GroupHealthRefresh()
            self._pending_health_snapshot = None
            self._drive_lane(resources)
            if self._reload.is_set():
                self._reload.clear()
                continue
            break
        self._watchdog.transition('shutdown', deadline_at=self._cleanup_deadline())

    def run(self) -> None:
        """Complete thread-owned resources while retaining health until close.

        Run the owner's service loop on the calling thread until stop or
        failure, then clean up its thread-owned resources. Health registration
        remains until `close()`.

        Raises:
            Exception: Resource setup, owner-thread work, or cleanup fails.
        """
        self._run_body()


class _WatchdogSamplingError(RuntimeError):
    """Signal one worker-fatal live watchdog supplier failure."""


class _WorkerRuntimeOptions(TypedDict):
    """Define required runtime composition and optional production or injected seams."""

    spec: WorkerSpec
    spawn_instance: str
    source_factory: _ComponentFactory
    lane_factory: _ComponentFactory
    health_sink: _HealthSink
    health_interval_secs: float
    checkpoint_interval_secs: float
    shutdown_deadline_secs: float
    max_concurrent_catchup_readers: int
    control: NotRequired[WorkerControl | None]
    clock: NotRequired[Callable[[], float]]
    force_exit: NotRequired[_ForceExit]
    group_sources: NotRequired[Mapping[str, str]]
    health_cache: _ComponentHealthCache
    monitor_gate: NotRequired[_MonitorGate]
    operator_deadline_secs_by_source: NotRequired[Mapping[str, float]]
    prepare: NotRequired[Callable[[], None]]
    socket_buffers: NotRequired[SocketBufferSizes]
    sqlite_synchronous: NotRequired[str]


class _WorkerRuntime:
    """Run injected source and serial lane components under one monitor."""

    def __init__(self, **options: Unpack[_WorkerRuntimeOptions]) -> None:
        """Configure an unopened injected worker runtime."""
        spec = options['spec']
        spawn_instance = options['spawn_instance']
        health_interval_secs = options['health_interval_secs']
        checkpoint_interval_secs = options['checkpoint_interval_secs']
        shutdown_deadline_secs = options['shutdown_deadline_secs']
        if not spawn_instance:
            raise ValueError('worker spawn instance must not be empty')
        if (
            health_interval_secs <= 0
            or checkpoint_interval_secs <= 0
            or shutdown_deadline_secs <= 0
        ):
            raise ValueError('worker intervals and deadline must be positive')
        self._catchup_limiter = _CatchupLimiter(options['max_concurrent_catchup_readers'])
        self._clock = options.get('clock', time.monotonic)
        self._completed_thread_names: set[str] = set()
        self._components: dict[str, _WorkerComponent] = {}
        self._construction_order: list[str] = []
        self._control = options.get('control')
        self._outbox: WorkerControlOutbox | None = None
        self._fatal_reason: str | None = None
        self._force_exit = options.get('force_exit', os._exit)
        self._health_interval_secs = health_interval_secs
        self._health_cache = options['health_cache']
        self._health_sink = options['health_sink']
        self._group_sources = dict(options.get('group_sources', {}))
        self._lane_factory = options['lane_factory']
        self._monitor_gate = options.get('monitor_gate', nullcontext)
        self._monitor_thread: threading.Thread | None = None
        self._monitor_wake = threading.Event()
        self._operator_command: OperatorCommand | None = None
        self._operator_deadline_secs_by_source = dict(
            options.get('operator_deadline_secs_by_source', {})
        )
        self._operator_result: CommandResult | None = None
        self._operator_wake = threading.Event()
        self._operator_watchdog = self._build_operator_watchdog(
            worker_instance=spawn_instance,
            startup_deadline_secs=shutdown_deadline_secs,
            clock=self._clock,
        )
        self._prepare = options.get('prepare', lambda: None)
        self._has_open_resources = False
        self._shutdown_deadline_at: float | None = None
        self._shutdown_deadline_secs = shutdown_deadline_secs
        self._socket_buffers = options.get('socket_buffers', _DEFAULT_SOCKET_BUFFERS)
        self._source_factory = options['source_factory']
        self._spawn_instance = spawn_instance
        self._spec = spec
        self._state_lock = threading.RLock()
        self._stop_event = threading.Event()
        self._stop_reason: str | None = None
        self._startup_failed = False
        self._stuck_thread_names: tuple[str, ...] = ()
        self._threads: dict[str, threading.Thread] = {}
        self._watchdog_failure: str | None = None
        self._checkpoint_host = CheckpointHost(
            interval_secs=checkpoint_interval_secs,
            synchronous=options.get('sqlite_synchronous', 'NORMAL'),
            on_failure=self._component_failed,
            logger=logging.getLogger('sg_event_daemon.source'),
        )

    @staticmethod
    def _build_operator_watchdog(
        *,
        worker_instance: str,
        startup_deadline_secs: float,
        clock: Callable[[], float],
    ) -> ComponentWatchdog:
        """Build one initially idle operator cell."""
        return ComponentWatchdog(
            kind='operator',
            identity=_OPERATOR_EXECUTOR_IDENTITY,
            worker_instance=worker_instance,
            registration_token=os.urandom(16).hex(),
            startup_deadline_secs=startup_deadline_secs,
            clock=clock,
        )

    def _complete_runtime_thread(self, identity: str) -> None:
        """Record one target as complete before its terminal monitor wake."""
        with self._state_lock:
            self._completed_thread_names.add(identity)
        self._monitor_wake.set()

    def _construct_components(self) -> None:
        """Construct sources then lanes in deterministic ownership order."""
        for source in self._spec.sources:
            identity = f'source:{source}'
            self._components[identity] = self._source_factory(
                source,
                self._catchup_limiter,
                self._checkpoint_host,
            )
            self._construction_order.append(identity)
        for group in self._spec.groups:
            identity = f'lane:{group}'
            self._components[identity] = self._lane_factory(
                group,
                self._catchup_limiter,
                self._checkpoint_host,
            )
            self._construction_order.append(identity)

    def _deregister_operator_watchdog(self) -> None:
        """Retire the operator cell only after its owner and publisher stop."""
        operator = self._threads.get(_OPERATOR_EXECUTOR_IDENTITY)
        monitor = self._monitor_thread
        if (operator is not None and operator.is_alive()) or (
            monitor is not None and monitor.is_alive()
        ):
            return
        sample = self._operator_watchdog.sample()
        self._health_cache.deregister_watchdog(
            kind=sample.kind,
            identity=sample.identity,
            worker_instance=sample.worker_instance,
            token=sample.registration_token,
        )

    def _close_components(self) -> None:
        """Close stopped owners only after their health publisher has stopped."""
        monitor = self._monitor_thread
        if monitor is not None and monitor.is_alive():
            return
        live = {identity for identity, thread in self._threads.items() if thread.is_alive()}
        if _OPERATOR_EXECUTOR_IDENTITY in live:
            return
        capture = _ExceptionCapture()
        with capture:
            self._deregister_operator_watchdog()
        first_error = capture.error
        for identity in reversed(tuple(self._construction_order)):
            if identity in live:
                continue
            capture = _ExceptionCapture()
            with capture:
                self._components[identity].close()
                self._construction_order.remove(identity)
            if first_error is None:
                first_error = capture.error
        with self._state_lock:
            self._has_open_resources = bool(self._construction_order)
        if first_error is not None:
            raise first_error

    def _monitor_wait_secs(self, pending_deadline: float | None = None) -> float:
        """Bound the next observation by its pending phase and shutdown deadlines."""
        wait = min(self._health_interval_secs, shutdown_slice_secs(self._shutdown_deadline_secs))
        now = self._clock()
        if pending_deadline is not None:
            wait = min(wait, max(0.0, pending_deadline - now))
        deadline = self._shutdown_deadline_at
        if self._stop_event.is_set() and deadline is not None:
            wait = min(wait, max(0.0, deadline - now))
        return wait

    @staticmethod
    def _operator_failure(message: OperatorCommand, reason: str) -> CommandResult:
        """Build one correlated operator-command failure."""
        return CommandResult(
            command=message.command,
            ok=False,
            message=reason,
            request_id=message.request_id,
            result={},
        )

    def _operator_owner(self, message: OperatorCommand) -> tuple[str, str | None]:
        """Resolve one accepted command to its exact source and optional group."""
        site = message.arguments.get('site')
        source = message.arguments.get('source')
        group = message.arguments.get('group')
        if site != self._spec.site:
            raise ValueError('operator command owner is invalid')
        if (
            isinstance(source, str)
            and source
            and (group is None or message.command == 'group-disposition')
        ):
            if source not in self._spec.sources:
                raise ValueError('operator command owner is invalid')
            return source, None
        if isinstance(group, str) and group and source is None:
            resolved = self._group_sources.get(group)
            if group not in self._spec.groups or resolved is None:
                raise ValueError('operator command owner is invalid')
            return resolved, group
        raise ValueError('operator command owner is invalid')

    def _operator_wait_phase(self) -> AbstractContextManager[None]:
        """Renew the operator's bounded phase while the source serves a call."""
        watchdog = self._operator_watchdog
        interval = self._health_interval_secs
        return watchdog.watch(
            'internal',
            deadline_at=self._clock() + interval + watchdog_grace_secs(interval),
            operator_command=watchdog.sample().operator_command,
        )

    def _operator_response(self, message: OperatorCommand) -> CommandResult:
        """Execute one owner-routed command and build its correlated result."""
        try:
            source, group = self._operator_owner(message)
            identity = f'source:{source}' if group is None else f'lane:{group}'
            component = self._components.get(identity)
            if component is None:
                return self._operator_failure(
                    message, 'operator command target is not owned by worker'
                )
            if group is None:
                result = component.execute_operator_command(
                    message, wait_phase=self._operator_wait_phase
                )
            else:
                with self._state_lock:
                    watchdog = self._operator_watchdog
                    if self._operator_command is message and not self._stop_event.is_set():
                        watchdog.transition('idle', deadline_at=None)
                try:
                    result = component.execute_operator_command(message)
                finally:
                    with self._state_lock:
                        if self._operator_command is message and not self._stop_event.is_set():
                            watchdog.transition(
                                'internal',
                                deadline_at=(
                                    self._clock() + self._operator_deadline_secs_by_source[source]
                                ),
                                operator_command=message.command,
                            )
        except Exception as error:  # noqa: BLE001
            return CommandResult(
                command=message.command,
                ok=False,
                message=str(error),
                request_id=message.request_id,
                result=(
                    {'code': SOURCE_BUSY_CODE}
                    if isinstance(error, _SourceCallBusyError)
                    else {'code': SOURCE_DATABASE_BUSY_CODE}
                    if isinstance(error, SourceDatabaseBusyError)
                    else {'code': GROUP_BUSY_CODE}
                    if isinstance(error, (AlreadyLockedError, _LaneCallBusyError))
                    else {}
                ),
            )
        return CommandResult(
            command=message.command,
            ok=True,
            message='accepted',
            request_id=message.request_id,
            result=result,
        )

    def _owned_threads(self) -> tuple[tuple[str, threading.Thread], ...]:
        """Snapshot component, operator, and checkpoint owners for this runtime."""
        with self._state_lock:
            components = tuple(self._threads.items())
        checkpoints = tuple(
            (thread.name.removeprefix('sg-event-daemon:'), thread)
            for thread in self._checkpoint_host.threads()
        )
        return components + checkpoints

    def _live_thread_names(self) -> tuple[str, ...]:
        """Return runtime targets that can still perform owned work."""
        owners = self._owned_threads()
        with self._state_lock:
            return tuple(
                identity
                for identity, thread in owners
                if identity not in self._completed_thread_names and thread.is_alive()
            )

    def _poll_operator_result(self) -> None:
        """Queue one completed or interrupted result and release its command slot."""
        with self._state_lock:
            result = self._operator_result
        if result is None:
            return
        outbox = self._outbox
        if outbox is None:
            raise RuntimeError('operator response control is unavailable')
        outbox.enqueue(result)
        with self._state_lock:
            if self._operator_result is result:
                self._operator_command = None
                self._operator_result = None
                watchdog = self._operator_watchdog
                if not self._stop_event.is_set():
                    watchdog.transition('idle', deadline_at=None)
                    self._health_cache.set_operator_target(
                        sample=watchdog.sample(),
                        source=None,
                        group=None,
                    )

    def _queue_operator_command(self, message: OperatorCommand) -> CommandResult | None:
        """Queue one command or return its correlated busy rejection."""
        try:
            source, group = self._operator_owner(message)
        except ValueError:
            return self._operator_failure(message, 'operator command owner is invalid')
        watchdog = self._operator_watchdog
        deadline_secs = self._operator_deadline_secs_by_source.get(source)
        if deadline_secs is None:
            return self._operator_failure(message, 'operator command owner is invalid')
        with self._state_lock:
            self._poll_operator_result()
            if self._operator_command is not None:
                return self._operator_failure(
                    message,
                    'operator command executor is busy',
                )
            self._operator_command = message
            self._operator_result = None
            self._health_cache.set_operator_target(
                sample=watchdog.sample(),
                source=source,
                group=group,
            )
            watchdog.transition(
                'internal',
                deadline_at=self._clock() + deadline_secs,
                operator_command=message.command,
            )
        self._operator_wake.set()
        return None

    def _request_components_stop(self) -> None:
        """Request stop in reverse construction order."""
        for identity in reversed(self._construction_order):
            component = self._components[identity]
            component.request_stop(shutdown_deadline_at=self._shutdown_deadline_at)

    @property
    def _resources_open(self) -> bool:
        """Return whether component or checkpoint resources remain owned."""
        with self._state_lock:
            components_open = self._has_open_resources
        return components_open or any(
            thread.is_alive() for thread in self._checkpoint_host.threads()
        )

    def _run_operator_commands(self) -> None:
        """Execute accepted commands serially until runtime shutdown."""
        while True:
            self._operator_wake.wait()
            self._operator_wake.clear()
            with self._state_lock:
                command = self._operator_command
                stopping = self._stop_event.is_set()
            if command is None:
                if stopping:
                    return
                continue
            if stopping:
                return
            result = self._operator_response(command)
            with self._state_lock:
                if self._operator_command is command and self._operator_result is None:
                    self._operator_result = result
                stopping = self._stop_event.is_set()
            self._monitor_wake.set()
            if stopping:
                return

    @property
    def _stuck_threads(self) -> tuple[str, ...]:
        """Return runtime work threads alive beyond the shutdown deadline."""
        with self._state_lock:
            return self._stuck_thread_names

    def _current_health(self, now: float) -> WorkerHealth:
        """Return health from the executing monitor, including its live-thread evidence."""
        monitor = threading.current_thread()
        return WorkerHealth(
            site=self._spec.site,
            worker_id=self._spec.worker_id,
            spawn_instance=self._spawn_instance,
            heartbeat_at=now,
            monitor_alive=monitor.is_alive(),
            thread_names=tuple(sorted(identity for identity, _thread in self._owned_threads())),
            stuck_threads=self._stuck_threads,
            socket_buffer_requested=self._socket_buffers.requested,
            socket_send_buffer=self._socket_buffers.send,
            socket_receive_buffer=self._socket_buffers.receive,
            watchdog_failure=self._watchdog_failure,
        )

    def _publish_monitor_health(self, now: float) -> float | None:
        """Offer one health sample and return its earliest pending phase deadline."""
        try:
            return self._health_sink(self._current_health(now))
        except _WatchdogSamplingError:
            if self._outbox is not None:
                with suppress(EOFError):
                    self._outbox.write_available()
            raise

    @property
    def fatal_reason(self) -> str | None:
        """Return the first worker-fatal thread condition.

        Returns:
            Body-free fatal reason, otherwise `None`.
        """
        with self._state_lock:
            return self._fatal_reason

    def request_stop(self, reason: str) -> None:
        """Request bounded graceful shutdown idempotently.

        Args:
            reason: Nonempty body-free stop condition.

        Raises:
            ValueError: The reason is empty.
        """
        if not reason:
            raise ValueError('worker stop reason must not be empty')
        with self._state_lock:
            if self._stop_event.is_set():
                return
            self._stop_reason = reason
            self._shutdown_deadline_at = self._clock() + self._shutdown_deadline_secs
            self._stop_event.set()
            watchdog = self._operator_watchdog
            watchdog.transition(
                'shutdown',
                deadline_at=self._shutdown_deadline_at,
            )
            if self._operator_command is not None and self._operator_result is None:
                self._operator_result = replace(
                    self._operator_failure(
                        self._operator_command,
                        'operator command interrupted by worker shutdown',
                    ),
                    result={OUTCOME_KEY: OUTCOME_UNKNOWN},
                )
        self._checkpoint_host.request_stop()
        self._operator_wake.set()
        self._request_components_stop()
        self._monitor_wake.set()

    def _component_failed(self, failure: str) -> None:
        """Retain the first fatal thread error and request worker shutdown."""
        with self._state_lock:
            if self._fatal_reason is None:
                self._fatal_reason = failure
        self.request_stop(failure)

    def _component_interrupted(self, identity: str, error: KeyboardInterrupt | SystemExit) -> None:
        """Stop the worker without replacing its component's original interrupt."""
        failure = f'{identity} thread failed: {type(error).__name__}'
        with self._state_lock:
            if self._fatal_reason is None:
                self._fatal_reason = failure
        try:
            self.request_stop(failure)
        except BaseException as stop_error:  # noqa: BLE001
            error.add_note(f'Worker stop also failed: {type(stop_error).__name__}')
        finally:
            self._complete_runtime_thread(identity)

    def _component_main(self, identity: str, component: _WorkerComponent) -> None:
        """Run one component behind the worker-fatal thread boundary."""
        failure: str | None = None
        capture = _ExceptionCapture()
        try:
            with capture:
                component.run()
        except (KeyboardInterrupt, SystemExit) as error:
            self._component_interrupted(identity, error)
            raise
        error = capture.error
        if error is not None:
            component_kind, separator, _name = identity.partition(':')
            cooperative_stop = self._stop_event.is_set() and (
                isinstance(error, (CheckpointHostStoppedError, WriteLockStoppedError))
                or (
                    isinstance(error, _CooperativeComponentStopError)
                    and separator == ':'
                    and component_kind == 'lane'
                    and error.phase in get_args(_ComponentStopPhase)
                )
            )
            if not cooperative_stop:
                failure = f'{identity} thread failed: {type(error).__name__}: {error}'
        elif not self._stop_event.is_set():
            failure = f'{identity} thread exited'
        if failure is not None:
            self._component_failed(failure)
        self._complete_runtime_thread(identity)

    def _control_transport_closed(self, error: EOFError) -> None:
        """Keep peer closure cooperative and other write transport failures fatal."""
        cause = error.__cause__
        if cause is None or isinstance(cause, BrokenPipeError):
            self.request_stop('control-eof')
        else:
            detail = bound_health_text(str(cause))
            self._component_failed(f'monitor thread failed: {type(cause).__name__}: {detail}')

    def _finish_monitor_shutdown(self, now: float) -> bool:
        """Give the retained shutdown deadline priority over final control traffic."""
        if not self._stop_event.is_set():
            return False
        outbox = self._outbox
        alive = self._live_thread_names()
        deadline = self._shutdown_deadline_at
        if deadline is not None and now >= deadline:
            if outbox is not None:
                outbox.abandon()
            with self._state_lock:
                self._stuck_thread_names = tuple(sorted(alive))
            if outbox is None:
                self._publish_monitor_health(now)
            if alive:
                self._force_exit(1)
            return True
        if alive:
            return False
        self._publish_monitor_health(now)
        if outbox is not None:
            try:
                outbox.write_available()
            except EOFError as error:
                self._control_transport_closed(error)
            outbox.abandon()
        return True

    def _install_event_grant(self, grant: EventPeerGrant) -> None:
        """Route one source grant or fail the worker closed."""
        component = self._components.get(f'source:{grant.source}')
        if not isinstance(component, _SourceComponent):
            self.request_stop('event grant source is not owned by worker')
            return
        component.request_grant(grant)

    def _apply_control_message(self, message: object) -> None:
        """Apply one decoded post-assignment control message."""
        if isinstance(message, StopCommand):
            self.request_stop(message.reason)
        elif isinstance(message, EventPeerGrant):
            self._install_event_grant(message)
        elif isinstance(message, OperatorCommand):
            result = self._queue_operator_command(message)
            if result is not None:
                outbox = self._outbox
                if outbox is None:
                    raise RuntimeError('operator response control is unavailable')
                outbox.enqueue(result)
        else:
            reason = f'unsupported runtime control message: {type(message).__name__}'
            self._component_failed(reason)

    def _operator_main(self) -> None:
        """Run serial operator work behind the worker-fatal boundary."""
        failure: str | None = None
        capture = _ExceptionCapture()
        with capture:
            self._run_operator_commands()
        error = capture.error
        if error is not None:
            failure = (
                f'{_OPERATOR_EXECUTOR_IDENTITY} thread failed: {type(error).__name__}: {error}'
            )
        elif not self._stop_event.is_set():
            failure = f'{_OPERATOR_EXECUTOR_IDENTITY} thread exited'
        if failure is not None:
            self._component_failed(failure)
        self._complete_runtime_thread(_OPERATOR_EXECUTOR_IDENTITY)

    def _poll_control(self) -> None:
        """Apply one available control message or stop on control EOF."""
        if self._control is None or self._stop_event.is_set():
            return
        try:
            message = self._control.receive(timeout=0.0)
        except TimeoutError:
            return
        except EOFError:
            self.request_stop('control-eof')
            return
        self._apply_control_message(message)

    def _monitor(self) -> None:
        """Sample health and service control without waiting for a receiver."""
        outbox = self._outbox
        with self._monitor_gate():
            while True:
                self._monitor_wake.clear()
                now = self._clock()
                self._poll_control()
                self._poll_operator_result()
                if self._finish_monitor_shutdown(now):
                    return
                pending_deadline = self._publish_monitor_health(now)
                next_sample = now + self._health_interval_secs
                if pending_deadline is not None:
                    next_sample = min(next_sample, pending_deadline)
                if outbox is None:
                    self._monitor_wake.wait(self._monitor_wait_secs(next_sample))
                else:
                    try:
                        outbox.write_available()
                        outbox.wait(self._monitor_wait_secs(next_sample))
                    except EOFError as error:
                        self._control_transport_closed(error)
                        outbox.abandon()
                        outbox = None

    def _monitor_main(self) -> None:
        """Run the monitor behind the worker-fatal thread boundary."""
        failure: str | None = None
        capture = _ExceptionCapture()
        with capture:
            self._monitor()
        error = capture.error
        if error is not None:
            if isinstance(error, _WatchdogSamplingError):
                failure = 'watchdog-sampling-failed'
            else:
                detail = bound_health_text(str(error))
                failure = f'monitor thread failed: {type(error).__name__}: {detail}'
        elif not self._stop_event.is_set():
            failure = 'monitor thread exited'
        if failure is not None:
            if failure == 'watchdog-sampling-failed':
                with self._state_lock:
                    self._watchdog_failure = failure
            self._component_failed(failure)

    def _shutdown(self) -> tuple[str, ...]:
        """Wait for graceful completion and close resources when possible."""
        if not self._stop_event.is_set():
            self.request_stop('runtime-shutdown')
        deadline = self._shutdown_deadline_at
        if deadline is None:
            deadline = self._clock() + self._shutdown_deadline_secs
        first_error: Exception | None = None
        for _identity, thread in self._owned_threads():
            capture = _ExceptionCapture()
            with capture:
                thread.join(max(0.0, deadline - self._clock()))
            if first_error is None:
                first_error = capture.error
        monitor = self._monitor_thread
        if monitor is not None and monitor is not threading.current_thread():
            capture = _ExceptionCapture()
            with capture:
                monitor.join(max(0.0, deadline - self._clock()))
            if first_error is None:
                first_error = capture.error
        live = tuple(
            sorted(identity for identity, thread in self._owned_threads() if thread.is_alive())
        )
        if live:
            with self._state_lock:
                self._stuck_thread_names = live
        capture = _ExceptionCapture()
        with capture:
            self._close_components()
        if first_error is None:
            first_error = capture.error
        if first_error is not None:
            raise first_error
        return live

    def _start_threads(self) -> None:
        """Acquire components and record threads only after successful starts."""
        with self._state_lock:
            self._prepare()
            self._has_open_resources = True
            self._construct_components()
        for identity in self._construction_order:
            component = self._components[identity]
            thread = threading.Thread(
                target=self._component_main,
                args=(identity, component),
                name=f'sg-event-daemon:{identity}',
            )
            thread.start()
            self._threads[identity] = thread
        watchdog = self._operator_watchdog
        sample = watchdog.sample()
        self._health_cache.register_watchdog(
            key=(
                sample.kind,
                sample.identity,
                sample.worker_instance,
                sample.registration_token,
            ),
            supplier=watchdog.sample,
        )
        operator_thread = threading.Thread(
            target=self._operator_main,
            name=f'sg-event-daemon:{_OPERATOR_EXECUTOR_IDENTITY}',
        )
        operator_thread.start()
        self._threads[_OPERATOR_EXECUTOR_IDENTITY] = operator_thread
        monitor = threading.Thread(
            target=self._monitor_main,
            name=_MONITOR_THREAD_NAME,
        )
        monitor.start()
        self._monitor_thread = monitor

    def require_outbox(self) -> WorkerControlOutbox:
        """Require start to have transferred socket writing to the monitor."""
        outbox = self._outbox
        if outbox is None:
            raise RuntimeError('worker control outbox is not started')
        return outbox

    def start(self) -> None:
        """Start owned work, unwinding a failed attempt within the shutdown budget.

        If startup has not failed, calling while resources are already owned is
        a no-op.
        A runtime whose startup failed cannot be started again.

        Raises:
            RuntimeError: An earlier startup attempt failed or an owned thread
                cannot start.
            Exception: The supplied preparation callback or a component factory
                fails; the original startup failure is re-raised after bounded
                cleanup.
        """
        with self._state_lock:
            if self._startup_failed:
                raise RuntimeError('worker runtime startup failed; reuse is unavailable')
            if self._has_open_resources:
                return
        capture = _ExceptionCapture()
        with capture:
            if self._control is not None:
                self._outbox = WorkerControlOutbox(self._control)
            self._start_threads()
        error = capture.error
        if error is not None:
            with self._state_lock:
                self._startup_failed = True
            with _ExceptionCapture():
                self.request_stop('runtime-startup-failed')
            with _ExceptionCapture():
                self._shutdown()
            if _OPERATOR_EXECUTOR_IDENTITY not in self._threads:
                with _ExceptionCapture():
                    self._deregister_operator_watchdog()
            raise error

    def wait(self) -> tuple[str, ...]:
        """Wait for the monitor, join runtime work threads, and close resources.

        Returns:
            Stable source, lane, `operator-executor`, and
            `checkpoint:<identity>` identities that missed the graceful
            deadline. A nonempty result means the listed owners retain
            resources; `operator-executor` retains all component resources.
            Checkpoint connections stay with their owning threads until those
            threads finish. An empty result follows attempted closure of every
            component.

        Raises:
            RuntimeError: The runtime has not started.
            Exception: Stopping or closing an owned component fails. Close
                failures are re-raised after other eligible component cleanup
                is attempted.
        """
        monitor = self._monitor_thread
        if monitor is None:
            raise RuntimeError('worker runtime is not started')
        monitor.join()
        return self._shutdown()


def _advance_health_refresh_deadline(
    deadline: float | None,
    now: float,
    interval: float,
) -> float:
    """Advance one owner deadline by complete intervals strictly past now."""
    if deadline is None:
        next_deadline = now + interval
    elif now < deadline:
        return deadline
    else:
        elapsed_interval_ratio = (nextafter(now, float('inf')) - deadline) / interval
        next_deadline = deadline
        if isfinite(elapsed_interval_ratio):
            elapsed_intervals = ceil(elapsed_interval_ratio)
            next_deadline += elapsed_intervals * interval
    return next_deadline if next_deadline > now else nextafter(now, float('inf'))


def _assigned_groups(
    site: SiteConfig,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
) -> dict[str, DeliveryGroupAssignment]:
    """Validate and return worker-owned lane assignments."""
    group_assignments = {item.group: item for item in assignment.group_assignments}
    if set(group_assignments) != set(spec.groups):
        raise ValueError('worker assignment groups conflict with packed spec')
    for group, item in group_assignments.items():
        if item.source != site.groups[group].source:
            raise ValueError('worker group assignment source conflicts with config')
    return group_assignments


def _close_resources(
    resources: tuple[Closeable | None, ...],
    first_error: tuple[BaseException, TracebackType | None] | None = None,
) -> None:
    """Close every resource and raise the first unexpected error."""
    fallback_error: tuple[BaseException, TracebackType | None] | None = None
    if first_error is not None and isinstance(
        first_error[0],
        (_CooperativeComponentStopError, _RemoteBootstrapTransportError),
    ):
        fallback_error = first_error
        first_error = None
    close_resources(
        resources,
        primary_error=first_error[0] if first_error is not None else None,
    )
    if first_error is None:
        first_error = fallback_error
    if first_error is not None:
        error, traceback = first_error
        raise error.with_traceback(traceback)


def _close_lane_resources(
    resources: _LaneResources | _PartialLaneAttempt,
    channel: GroupChannelInterface | None,
    first_error: tuple[BaseException, TracebackType | None] | None = None,
) -> None:
    """Close a lane's current channel before its execution and storage resources."""
    _close_resources(
        (channel, resources.lane, resources.state, resources.reader, resources.client),
        first_error,
    )


class _SnapshotHealthOptions(TypedDict):
    """Define durable group fields shared by every health publisher."""

    pending_catchup_epoch_count: int
    oldest_catchup_epoch_age: float | None
    outstanding_late_obligation_count: int
    outstanding_late_id_count: int
    outstanding_late_ids: tuple[int, ...]
    outstanding_late_ids_truncated: bool
    parked: bool
    park_reason: str | None
    failure_kind: str | None
    failure_text: str | None
    conditions: tuple[str, ...]
    audit_count: int
    replay_required_from: int | None
    history_truncated_floor: int | None


def _component_cleanup_deadline(deadline_at: float | None, stall_secs: float) -> float:
    """Return the retained shutdown deadline or one bounded local fallback."""
    if deadline_at is None:
        return time.monotonic() + stall_secs
    return deadline_at


def _group_failure_kind(failure_code: str | None) -> str | None:
    """Map durable callback failure codes to the health vocabulary."""
    return 'callback' if failure_code == 'callback-failed' else None


def _group_health_snapshot(result: object) -> GroupSnapshot | None:
    """Retain a complete mutation snapshot when one was returned."""
    return result if isinstance(result, GroupSnapshot) else None


def _internal_worker_id(
    kind: _WorkerKind,
    site: str,
    name: str | None = None,
) -> str:
    """Return one collision-free structured internal worker id."""
    fields = [kind.value, site]
    if name is not None:
        fields.append(name)
    return json.dumps(fields, ensure_ascii=True, separators=(',', ':'))


def _operator_reason(arguments: Mapping[str, object]) -> str:
    """Return one required nonempty audited reason."""
    reason = arguments.get('reason')
    if not isinstance(reason, str) or not reason:
        raise ValueError('operator command reason must be nonempty text')
    return reason


def _owner_call_result(call: _OwnerCall[_OwnerT]) -> object:
    """Raise the recorded error or return the completed owner call's result."""
    if call.error is not None:
        raise call.error
    return call.result


def _preflight_compact_health_update(
    config: Config,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
    *,
    spawn_instance: str,
    socket_buffers: SocketBufferSizes,
) -> None:
    """Reject a packed inventory whose maximum compact health cannot fit."""
    site = config.sites[spec.site]
    source_generations = dict(assignment.source_generations)
    thread_names = tuple(
        sorted(
            (
                *(
                    f'checkpoint:source:{spec.site}:{source}'
                    for source in set(spec.sources)
                    | {site.groups[group].source for group in spec.groups}
                ),
                *(f'source:{source}' for source in spec.sources),
                *(f'lane:{group}' for group in spec.groups),
                _OPERATOR_EXECUTOR_IDENTITY,
            )
        )
    )
    maximum_late_ids = tuple(
        range(
            _MAXIMUM_HEALTH_INT - HEALTH_LATE_ID_LIMIT + 1,
            _MAXIMUM_HEALTH_INT + 1,
        )
    )
    conditions = (*GROUP_HEALTH_CONDITIONS, *sorted(STALL_CONDITIONS))
    join_phases = ('attached', 'catching-up', 'detached', 'joining', 'parked')
    failure_kinds = ('callback', 'setup')
    group_operator_command = max(OPERATOR_COMMANDS, key=len)
    source_operator_command = max(SOURCE_OPERATOR_COMMANDS, key=len)
    sources = tuple(
        SourceHealth(
            site=spec.site,
            source=source,
            source_generation=source_generations[source],
            status=max(SOURCE_RUN_STATUSES, key=len),
            frontier=_MAXIMUM_HEALTH_INT,
            successful_clock=_MAXIMUM_HEALTH_FLOAT,
            retry_attempt=_MAXIMUM_HEALTH_INT,
            next_retry_secs=_MAXIMUM_HEALTH_FLOAT,
            open_range_count=_MAXIMUM_HEALTH_INT,
            late_id_count=_MAXIMUM_HEALTH_INT,
            last_error=_MAXIMUM_HEALTH_TEXT,
            worker_instance=spawn_instance,
            initialized=False,
            pending_obligation_count=_MAXIMUM_HEALTH_INT,
            orphan_marker_count=_MAXIMUM_HEALTH_INT,
            strict_buffer_event_count=_MAXIMUM_HEALTH_INT,
            strict_buffer_byte_count=_MAXIMUM_HEALTH_INT,
            strict_retention=tuple(
                (
                    item.group,
                    _MAXIMUM_HEALTH_INT,
                    _MAXIMUM_HEALTH_INT,
                    _MAXIMUM_HEALTH_INT,
                )
                for item in assignment.source_group_assignments
                if item.source == source and site.groups[item.group].ordering == 'strict'
            ),
            conditions=tuple(sorted(STALL_CONDITIONS)),
            stream_incarnation=_MAXIMUM_HEALTH_IDENTITY,
            audit_count=_MAXIMUM_HEALTH_INT,
            database_size_bytes=_MAXIMUM_HEALTH_INT,
            phase=max(SOURCE_WATCHDOG_PHASES, key=len),
            heartbeat_at=_MAXIMUM_HEALTH_FLOAT,
            heartbeat_age=_MAXIMUM_HEALTH_FLOAT,
            deadline_at=_MAXIMUM_HEALTH_FLOAT,
            overdue_reason=_MAXIMUM_HEALTH_TEXT,
        )
        for source in spec.sources
    )
    groups = tuple(
        GroupHealth(
            site=spec.site,
            source=site.groups[group].source,
            group=group,
            worker_instance=spawn_instance,
            execution=site.groups[group].execution,
            ordering=site.groups[group].ordering,
            phase=max(GROUP_PHASES, key=len),
            status=max(GROUP_STATUSES, key=len),
            cursor=_MAXIMUM_HEALTH_INT,
            current_event_id=_MAXIMUM_HEALTH_INT,
            current_plugin_id=_MAXIMUM_HEALTH_IDENTITY,
            current_callback_id=_MAXIMUM_HEALTH_IDENTITY,
            delivery_attempt=_MAXIMUM_HEALTH_INT,
            heartbeat_at=_MAXIMUM_HEALTH_FLOAT,
            heartbeat_age=_MAXIMUM_HEALTH_FLOAT,
            queue_event_count=_MAXIMUM_HEALTH_INT,
            queue_byte_count=_MAXIMUM_HEALTH_INT,
            catchup_frontier=_MAXIMUM_HEALTH_INT,
            pending_catchup_epoch_count=_MAXIMUM_HEALTH_INT,
            oldest_catchup_epoch_age=_MAXIMUM_HEALTH_FLOAT,
            join_phase=max(join_phases, key=len),
            outstanding_late_obligation_count=_MAXIMUM_HEALTH_INT,
            outstanding_late_id_count=_MAXIMUM_HEALTH_INT,
            outstanding_late_ids=maximum_late_ids,
            outstanding_late_ids_truncated=True,
            parked=False,
            park_reason=_MAXIMUM_HEALTH_TEXT,
            failure_kind=max(failure_kinds, key=len),
            failure_text=_MAXIMUM_HEALTH_TEXT,
            conditions=conditions,
            audit_count=_MAXIMUM_HEALTH_INT,
            replay_required_from=_MAXIMUM_HEALTH_INT,
            history_truncated_floor=_MAXIMUM_HEALTH_INT,
            deadline_at=_MAXIMUM_HEALTH_FLOAT,
            overdue_reason=_MAXIMUM_HEALTH_TEXT,
        )
        for group in sorted(
            set(spec.groups)
            | {
                item.group
                for item in assignment.source_group_assignments
                if isinstance(item, PendingDispositionSourceGroupAssignment)
            }
        )
    )
    watchdogs = tuple(
        [
            WatchdogHealth(
                kind='source',
                site=spec.site,
                source=source,
                group=None,
                worker_instance=spawn_instance,
                phase=max(SOURCE_WATCHDOG_PHASES, key=len),
                heartbeat_at=_MAXIMUM_HEALTH_FLOAT,
                deadline_at=_MAXIMUM_HEALTH_FLOAT,
            )
            for source in spec.sources
        ]
        + [
            WatchdogHealth(
                kind='lane',
                site=spec.site,
                source=site.groups[group].source,
                group=group,
                worker_instance=spawn_instance,
                phase=max(GROUP_PHASES, key=len),
                heartbeat_at=_MAXIMUM_HEALTH_FLOAT,
                deadline_at=_MAXIMUM_HEALTH_FLOAT,
                event_id=_MAXIMUM_HEALTH_INT,
                plugin_id=_MAXIMUM_HEALTH_IDENTITY,
                callback_id=_MAXIMUM_HEALTH_IDENTITY,
                delivery_attempt=_MAXIMUM_HEALTH_INT,
            )
            for group in spec.groups
        ]
    )
    if spec.groups:
        operator_group = max(
            spec.groups,
            key=lambda group: len(
                canonical_bytes({'group': group, 'source': site.groups[group].source})
            ),
        )
        operator_source = site.groups[operator_group].source
        operator_phase: Literal['idle', 'internal'] = 'internal'
        operator_deadline: float | None = _MAXIMUM_HEALTH_FLOAT
        operator_command = group_operator_command
        if spec.sources:
            source_name = max(spec.sources, key=lambda source: len(canonical_bytes(source)))
            group_record = canonical_bytes(
                {
                    'group': operator_group,
                    'source': operator_source,
                    'operator_command': operator_command,
                }
            )
            source_record = canonical_bytes(
                {
                    'group': None,
                    'source': source_name,
                    'operator_command': source_operator_command,
                }
            )
            if len(source_record) > len(group_record):
                operator_group = None
                operator_source = source_name
                operator_command = source_operator_command
    elif spec.sources:
        operator_group = None
        operator_source = max(spec.sources, key=len)
        operator_phase = 'internal'
        operator_deadline = _MAXIMUM_HEALTH_FLOAT
        operator_command = source_operator_command
    else:
        operator_group = None
        operator_source = None
        operator_phase = 'idle'
        operator_deadline = None
        operator_command = None
    watchdogs = (
        *watchdogs,
        WatchdogHealth(
            kind='operator',
            site=spec.site,
            source=operator_source,
            group=operator_group,
            worker_instance=spawn_instance,
            phase=operator_phase,
            heartbeat_at=_MAXIMUM_HEALTH_FLOAT,
            deadline_at=operator_deadline,
            operator_command=operator_command,
        ),
    )
    event_sockets = tuple(
        [
            EventSocketHealth(
                source=source,
                group=None,
                role='server',
                connected=False,
                requested=_MAXIMUM_HEALTH_INT,
                send=_MAXIMUM_HEALTH_INT,
                receive=_MAXIMUM_HEALTH_INT,
                queue_event_count=_MAXIMUM_HEALTH_INT,
                queue_byte_count=_MAXIMUM_HEALTH_INT,
                reason=_MAXIMUM_HEALTH_TEXT,
            )
            for source in spec.sources
        ]
        + [
            EventSocketHealth(
                source=site.groups[group].source,
                group=group,
                role='client',
                connected=False,
                requested=_MAXIMUM_HEALTH_INT,
                send=_MAXIMUM_HEALTH_INT,
                receive=_MAXIMUM_HEALTH_INT,
                queue_event_count=_MAXIMUM_HEALTH_INT,
                queue_byte_count=_MAXIMUM_HEALTH_INT,
                reason=_MAXIMUM_HEALTH_TEXT,
            )
            for group in spec.groups
            if site.groups[group].source not in spec.sources
        ]
    )
    update = HealthUpdate(
        worker=WorkerHealth(
            site=spec.site,
            worker_id=spec.worker_id,
            spawn_instance=spawn_instance,
            heartbeat_at=_MAXIMUM_HEALTH_FLOAT,
            monitor_alive=False,
            thread_names=thread_names,
            stuck_threads=thread_names,
            socket_buffer_requested=socket_buffers.requested,
            socket_send_buffer=socket_buffers.send,
            socket_receive_buffer=socket_buffers.receive,
            heartbeat_age=_MAXIMUM_HEALTH_FLOAT,
            watchdog_failure=_MAXIMUM_HEALTH_TEXT,
        ),
        watchdogs=watchdogs,
        sources=sources,
        groups=groups,
        event_sockets=event_sockets,
    )
    required_bytes = len(encode_control_message(update)) - IPC_LENGTH_BYTES
    if required_bytes > config.daemon.max_ipc_frame_bytes:
        raise ConfigError(
            f'worker {spec.worker_id} compact health requires {required_bytes} bytes; '
            f'max_ipc_frame_bytes is {config.daemon.max_ipc_frame_bytes}'
        )


def _prepare_worker_directories(config: Config, spec: WorkerSpec) -> None:
    """Create every worker-owned state and socket parent before threads start."""
    ensure_secure_state_dir(config.daemon.state_dir)
    sources_path = site_sources_directory(config, spec.site)
    ensure_secure_state_dir(sources_path.parent.parent)
    ensure_secure_state_dir(sources_path.parent)
    fallback_parents: set[Path] = set()
    if spec.sources:
        sources_dir = ensure_secure_state_dir(sources_path)
        for source in spec.sources:
            socket_kind = source_socket_kind(source)
            candidate = local_socket_path_candidate(
                sources_dir,
                socket_kind,
                (spec.site, source),
            )
            if candidate.parent != sources_dir:
                fallback_parents.add(candidate.parent)
    if spec.groups:
        ensure_secure_state_dir(site_groups_directory(config, spec.site))
    for parent in sorted(fallback_parents, key=os.fspath):
        ensure_secure_state_dir(parent)


def _refresh_group_health(
    pending: GroupSnapshot | None,
    cache: _GroupHealthRefresh,
    interval: float,
    snapshot_supplier: Callable[[], GroupSnapshot],
    project: Callable[[GroupSnapshot, bool], GroupHealth],
) -> GroupHealth:
    """Refresh a group projection only for a change or elapsed interval."""
    cached = cache.cached
    dirty = cache.dirty
    revision = cache.revision
    deadline = cache.deadline_at
    now = time.monotonic()
    snapshot_is_new = pending is not None and pending.revision != revision
    deadline_due = deadline is not None and now >= deadline
    if cached is None or dirty or snapshot_is_new or deadline_due:
        if pending is None or (dirty and not snapshot_is_new):
            pending = snapshot_supplier()
        cached = project(pending, cached is None or deadline_due)
        deadline = _advance_health_refresh_deadline(deadline, now, interval)
        cache.cached = cached
        cache.dirty = False
        cache.revision = pending.revision
        cache.deadline_at = deadline
    return cached


def _restorable_signals() -> tuple[signal.Signals, ...]:
    """Return process signals whose Python handlers can be restored."""
    excluded = {signal.SIGKILL, signal.SIGSTOP}
    return tuple(item for item in signal.Signals if item not in excluded)


def _load_shared_plugins(loader: Callable[[], _Result]) -> _Result:
    """Load trusted plugins while restoring enforceable daemon globals."""
    handlers = {item: signal.getsignal(item) for item in _restorable_signals()}
    root = logging.getLogger()
    logging_disabled = root.disabled
    logging_filters = tuple(root.filters)
    logging_handlers = tuple(root.handlers)
    logging_level = root.level
    try:
        return loader()
    finally:
        for item, handler in handlers.items():
            if signal.getsignal(item) != handler:
                with suppress(OSError, RuntimeError, ValueError):
                    signal.signal(item, handler)
        root.disabled = logging_disabled
        root.filters[:] = logging_filters
        root.handlers[:] = logging_handlers
        root.setLevel(logging_level)


def _retire_component_watchdog(
    watchdog: ComponentWatchdog,
    cache: _ComponentHealthCache,
) -> None:
    """Retire one live component watchdog from the shared health cache."""
    sample = watchdog.sample()
    cache.deregister_watchdog(
        kind=sample.kind,
        identity=sample.identity,
        worker_instance=sample.worker_instance,
        token=sample.registration_token,
    )


def _scan_provider_call(call: Callable[[], _Result]) -> _Result:
    """Capture only provider execution, preserving storage ownership failures."""
    with _ExceptionCapture() as capture:
        return call()
    if capture.error is None:
        raise RuntimeError('provider capture has no error')
    raise_owner_write_failure(capture.error)
    raise _ObligationScanUnavailableError from capture.error


def _snapshot_health_options(
    snapshot: GroupSnapshot,
    late_projection: tuple[int, tuple[int, ...]],
    park_reason: str | None,
    audit_count: int,
) -> _SnapshotHealthOptions:
    """Project durable recovery fields identically for pending, startup, and live groups."""
    late_count, late_ids = late_projection
    failure_kind = _group_failure_kind(snapshot.failure_code)
    return {
        'pending_catchup_epoch_count': len(snapshot.epochs),
        'oldest_catchup_epoch_age': (
            max(
                0.0,
                snapshot.reader_clock_checkpoint
                - min(epoch.captured_at for epoch in snapshot.epochs),
            )
            if snapshot.epochs
            else None
        ),
        'outstanding_late_obligation_count': snapshot.obligation_count,
        'outstanding_late_id_count': late_count,
        'outstanding_late_ids': late_ids,
        'outstanding_late_ids_truncated': late_count > len(late_ids),
        'parked': snapshot.parked,
        'park_reason': park_reason if snapshot.parked else None,
        'failure_kind': failure_kind,
        'failure_text': snapshot.failure_text if failure_kind == 'callback' else None,
        'conditions': snapshot.conditions,
        'audit_count': audit_count,
        'replay_required_from': snapshot.replay_required_from,
        'history_truncated_floor': snapshot.history_truncated_floor,
    }


def _validate_group_operator_target(
    command: OperatorCommand,
    *,
    site: str,
    group: str,
) -> None:
    """Require exact configured ownership and closed command arguments."""
    disposition_expected = GROUP_DISPOSITION_ARGUMENTS.get(command.command)
    expected = (
        disposition_expected
        if disposition_expected is not None
        else frozenset({'group', 'site'})
        if command.command == 'retry'
        else frozenset({'group', 'reason', 'site'})
    )
    supplied = frozenset(command.arguments)
    disposition_fields = frozenset({'disposition_id', 'orphan_generation', 'reserved_generation'})
    if disposition_expected is not None:
        valid = supplied in (expected, expected | disposition_fields)
    else:
        valid = supplied == expected
    if not valid:
        raise ValueError('group operator command arguments are invalid')
    if command.arguments.get('site') != site or command.arguments.get('group') != group:
        raise ValueError('group operator command target conflicts with owner')


def _validate_remote_dependencies(
    site: SiteConfig,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
) -> None:
    """Validate exact endpoint sources and source/group grant pairs."""
    expected_pairs = {
        (site.groups[group].source, group)
        for group in spec.groups
        if site.groups[group].source not in spec.sources
    }
    expected_sources = {source for source, _group in expected_pairs}
    endpoint_sources = {endpoint.source for endpoint in assignment.source_endpoints}
    if endpoint_sources != expected_sources:
        raise ValueError('source endpoint dependencies conflict with packed spec')
    grant_pairs = {(grant.source, grant.group) for grant in assignment.event_grants}
    if grant_pairs != expected_pairs:
        raise ValueError('event grant dependencies conflict with packed spec')


def _validate_source_inventory(
    site: SiteConfig,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
    group_assignments: Mapping[
        str,
        DeliveryGroupAssignment,
    ],
) -> None:
    """Validate complete source-owned group policy inventory."""
    expected_inventory = {
        group.name for group in site.groups.values() if group.source in spec.sources
    }
    source_inventory = {item.group: item for item in assignment.source_group_assignments}
    if set(source_inventory) != expected_inventory:
        raise ValueError('source group inventory conflicts with config')
    for group, item in source_inventory.items():
        configured = site.groups[group]
        if item.source != configured.source:
            raise ValueError('source group inventory source conflicts with config')
        local = group_assignments.get(group)
        if local is None:
            continue
        if (
            isinstance(item, ActiveSourceGroupAssignment)
            and local.group_generation == item.group_generation
            and local.start_mode == item.start_mode
        ):
            continue
        raise ValueError('source and lane group assignments conflict')


def _validate_worker_assignment(
    config: Config,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
) -> None:
    """Validate one exact worker assignment against packed configuration."""
    if assignment.worker_id != spec.worker_id:
        raise ValueError('worker assignment id conflicts with argv')
    if set(dict(assignment.source_generations)) != set(spec.sources):
        raise ValueError('worker assignment sources conflict with packed spec')
    site = config.sites[spec.site]
    group_assignments = _assigned_groups(site, spec, assignment)
    _validate_remote_dependencies(site, spec, assignment)
    _validate_source_inventory(site, spec, assignment, group_assignments)


def build_worker_runtime(
    config: Config,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
    control: WorkerControl,
) -> _WorkerRuntime:
    """Build one concrete assigned runtime without opening component resources.

    Construction validates the packed assignment and maximum compact health
    frame before opening resources.

    Args:
        config: Fully resolved immutable daemon configuration.
        spec: Configured packed worker identity.
        assignment: Supervisor-decided generations and endpoint grants.
        control: Authenticated inherited worker channel.

    Returns:
        A concrete unopened packed runtime.

    Raises:
        ValueError: The assignment conflicts with the packed specification or
            configuration.
        ConfigError: The maximum compact health frame exceeds the configured
            frame budget.
    """
    _validate_worker_assignment(config, spec, assignment)
    _preflight_compact_health_update(
        config,
        spec,
        assignment,
        spawn_instance=assignment.spawn_instance,
        socket_buffers=control.socket_buffers,
    )
    site = config.sites[spec.site]
    group_sources = {group: site.groups[group].source for group in spec.groups}
    health_cache = _ComponentHealthCache(
        site=spec.site,
        group_sources=group_sources,
    )
    source_components: dict[str, _SourceComponent] = {}

    def _source_factory(
        source: str,
        _limiter: _CatchupLimiter,
        checkpoint_host: CheckpointHost,
    ) -> _WorkerComponent:
        component = _SourceComponent(
            assignment=assignment,
            checkpoint_host=checkpoint_host,
            config=config,
            control=runtime.require_outbox(),
            health_cache=health_cache,
            source=source,
            spec=spec,
        )
        source_components[source] = component
        return component

    def _lane_factory(
        group: str,
        limiter: _CatchupLimiter,
        checkpoint_host: CheckpointHost,
    ) -> _WorkerComponent:
        source = config.sites[spec.site].groups[group].source
        return _LaneComponent(
            assignment=assignment,
            checkpoint_host=checkpoint_host,
            config=config,
            control=runtime.require_outbox(),
            group=group,
            health_cache=health_cache,
            limiter=limiter,
            local_source=source_components.get(source),
            spec=spec,
        )

    def _publish_health(worker: WorkerHealth) -> float | None:
        components = health_cache.snapshot()
        if components.watchdog_failure is not None:
            update = HealthUpdate(
                worker=replace(
                    worker,
                    watchdog_failure=components.watchdog_failure,
                ),
                watchdogs=(),
                sources=(),
                groups=(),
                event_sockets=(),
            )
        else:
            update = HealthUpdate(
                worker=worker,
                watchdogs=components.watchdogs,
                sources=components.sources,
                groups=components.groups,
                event_sockets=components.event_sockets,
            )
        runtime.require_outbox().offer_health(update)
        if components.watchdog_failure is not None:
            raise _WatchdogSamplingError(components.watchdog_failure)
        # A deadline crossed during sampling still requires an immediate observation.
        # Filter relative to this frame, then measure the wait after publication.
        return min(
            (
                watchdog.deadline_at
                for watchdog in update.watchdogs
                if watchdog.deadline_at is not None
                and watchdog.deadline_at > update.worker.heartbeat_at
            ),
            default=None,
        )

    daemon = config.daemon
    operator_sources = set(spec.sources) | set(group_sources.values())
    operator_deadlines = {
        source: internal_stall_deadline_secs(
            daemon.health_interval_secs,
            site.credentials[site.sources[source].credentials],
        )
        for source in operator_sources
    }
    runtime = _WorkerRuntime(
        spec=spec,
        spawn_instance=assignment.spawn_instance,
        source_factory=_source_factory,
        lane_factory=_lane_factory,
        health_sink=_publish_health,
        health_interval_secs=daemon.health_interval_secs,
        checkpoint_interval_secs=daemon.checkpoint_interval_secs,
        sqlite_synchronous=daemon.sqlite_synchronous.upper(),
        shutdown_deadline_secs=daemon.shutdown_deadline_secs,
        max_concurrent_catchup_readers=daemon.max_concurrent_catchup_readers,
        control=control,
        group_sources=group_sources,
        health_cache=health_cache,
        operator_deadline_secs_by_source=operator_deadlines,
        prepare=partial(_prepare_worker_directories, config, spec),
        socket_buffers=control.socket_buffers,
    )
    return runtime


def build_worker_specs(site: SiteConfig) -> tuple[WorkerSpec, ...]:
    """Pack one site's sources and groups into deterministic worker specs.

    Args:
        site: Fully resolved immutable site configuration.

    Returns:
        Source-owning site worker followed by used named and dedicated workers.
    """
    default_groups = tuple(
        sorted(group.name for group in site.groups.values() if group.worker == 'default')
    )
    specs = [
        WorkerSpec(
            worker_id=_internal_worker_id(_WorkerKind.SITE, site.name),
            site=site.name,
            kind=_WorkerKind.SITE,
            name=site.name,
            sources=tuple(sorted(site.sources)),
            groups=default_groups,
        )
    ]
    named: dict[str, list[str]] = {}
    dedicated: list[str] = []
    for group in site.groups.values():
        if group.worker == 'dedicated':
            dedicated.append(group.name)
        elif group.worker != 'default':
            named.setdefault(group.worker, []).append(group.name)
    specs.extend(
        WorkerSpec(
            worker_id=_internal_worker_id(_WorkerKind.NAMED, site.name, worker_name),
            site=site.name,
            kind=_WorkerKind.NAMED,
            name=worker_name,
            sources=(),
            groups=tuple(sorted(groups)),
        )
        for worker_name, groups in sorted(named.items())
    )
    specs.extend(
        WorkerSpec(
            worker_id=_internal_worker_id(_WorkerKind.DEDICATED, site.name, group_name),
            site=site.name,
            kind=_WorkerKind.DEDICATED,
            name=group_name,
            sources=(),
            groups=(group_name,),
        )
        for group_name in sorted(dedicated)
    )
    packed = [group for spec in specs for group in spec.groups]
    if len(packed) != len(site.groups) or set(packed) != set(site.groups):
        raise RuntimeError('worker packing did not assign every group exactly once')
    return tuple(specs)


def open_file_need(site: SiteConfig, spec: WorkerSpec) -> int:
    """Estimate packed worker descriptors before the transient-open margin.

    Args:
        site: Configuration containing every local and remote group.
        spec: One worker assignment from `build_worker_specs`.

    Returns:
        Descriptor need from the measured source and lane ownership table.
    """
    groups = [site.groups[name] for name in spec.groups]
    if spec.sources:
        remote_lanes = sum(
            group.source in spec.sources and group.name not in spec.groups
            for group in site.groups.values()
        )
        return (
            7
            + 12 * len(spec.sources)
            + sum(6 + len(group.plugins) for group in groups)
            + 2 * remote_lanes
        )
    source_files = {group.source for group in groups}
    return 5 + 2 * len(source_files) + sum(10 + len(group.plugins) for group in groups)


def stable_operator_id(command: str, arguments: Mapping[str, object]) -> str:
    """Return one stable semantic operator command identity.

    Args:
        command: Closed operator command name.
        arguments: Semantic body-free command fields.

    Returns:
        Canonical deterministic command digest.
    """
    return digest(
        canonical_bytes(
            {
                'command': command,
                'arguments': dict(arguments),
            }
        )
    )
