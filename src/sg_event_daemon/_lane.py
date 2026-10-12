"""Mode-neutral serial group scheduling and callback execution."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import inspect
import threading
import time
import traceback
import uuid
from collections import deque
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from itertools import takewhile
from math import isfinite
from typing import (
    TYPE_CHECKING,
    Literal,
    NotRequired,
    Protocol,
    TypedDict,
    TypeVar,
    Unpack,
    cast,
)

from ._events import EVENT_FIELDS, Event, decode_event
from ._fanout import (
    EventDelivery,
    FanoutMessage,
    GroupChannelInterface,
    RangeChanged,
    StrictCapacityControl,
    StrictOrderingControl,
)
from ._group_errors import (
    InvalidTransitionError,
    RevisionConflictError,
    StrictObligationBlockedError,
    TerminalLateEventError,
)
from ._plugins import (
    CallbackManifest,
    CallbackSpec,
    EventContext,
    PluginError,
    PluginHost,
    SetupContext,
    validate_sync_callback_result,
    with_heartbeat,
)
from ._source import (
    Attached,
    JoinAbort,
    JoinAttemptIdentity,
    JoinCapacityError,
    JoinConfirm,
    JoinFinalize,
    JoinFinalized,
    JoinProtocolError,
    JoinRequest,
    JoinRetry,
    JoinTarget,
    history_truncation,
)
from ._strict import JOIN_ABORT_LIMIT, OrderingAnomaly
from ._watchdog import (
    ComponentWatchdog,
    internal_stall_deadline_secs,
)
from ._write_lock import WriteLockStoppedError, WriteLockTimeoutError

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable, Iterator

    from ._config import GroupConfig, SiteConfig
    from ._group_state import GroupState, PendingRange
    from ._group_types import (
        GroupSnapshot,
        LiveJoinIdentity,
    )
    from ._plugins import PluginSpec
    from ._providers import Provider, SyncSG
    from ._watchdog import GroupPhase

_Item = TypeVar('_Item')

_STALE_REPLY_LIMIT = 8


@dataclass(frozen=True)
class Delivery:
    """Carry one immutable event and optional late-range ownership."""

    event: Event
    obligation_id: str | None = None


@dataclass(frozen=True)
class LaneHealth:
    """Describe one body-free immutable lane health observation."""

    site: str
    source: str
    group: str
    worker_instance: str
    execution: Literal['sync', 'async']
    ordering: Literal['eager', 'strict']
    phase: GroupPhase
    status: str
    cursor: int
    event_id: int | None
    plugin_id: str | None
    callback_id: str | None
    delivery_attempt: int | None
    heartbeat_at: float
    heartbeat_age: float
    deadline_at: float | None
    internal_stall_deadline_secs: float
    overdue_reason: str | None


class _GroupLaneOptions(TypedDict):
    """Type one complete group-lane construction request."""

    state: GroupState
    site: SiteConfig
    group: GroupConfig
    provider: Provider
    plugins: Iterable[PluginSpec]
    worker_instance: str
    health_interval_secs: float
    watchdog: ComponentWatchdog
    async_cancel_grace_secs: NotRequired[float]
    clock: NotRequired[Callable[[], float] | None]
    token_factory: NotRequired[Callable[[], str] | None]
    wait: NotRequired[Callable[[float], bool] | None]


class _JoinSource(Protocol):
    """Define the source coordinator surface used by one lane."""

    def abort(self, abort: JoinAbort) -> None:
        """Abort one exact attempt.

        Args:
            abort: Exact join attempt to abort.
        """
        ...

    def acknowledge_strict_control(
        self,
        control: StrictCapacityControl | StrictOrderingControl,
    ) -> None:
        """Retire one exact lane-committed strict control.

        Args:
            control: Capacity or ordering control committed by the lane.
        """
        ...

    def confirm(self, confirmation: JoinConfirm) -> Attached | JoinRetry:
        """Prepare one exact caught-up attempt.

        Args:
            confirmation: Exact join boundary confirmation.

        Returns:
            Attached identity or retry instruction.
        """
        ...

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        """Finalize one exact group-committed attempt.

        Args:
            finalization: Exact join identity to finalize.

        Returns:
            Finalized join identity.
        """
        ...

    def request(
        self,
        request: JoinRequest,
        channel: GroupChannelInterface,
    ) -> JoinTarget | JoinRetry:
        """Capture one source boundary.

        Args:
            request: Exact live-join request.
            channel: Attempt-owned bounded group fanout channel.

        Returns:
            Captured join target or retry instruction.
        """
        ...


class _JoinState:
    """Mark one exact lane-owned live-join lifecycle state."""


@dataclass(frozen=True)
class _DetachedJoin(_JoinState):
    """Represent a lane with no live channel ownership."""


@dataclass(frozen=True)
class _FinalizedJoin(_JoinState):
    """Own one exact finalized callback gate and pending messages."""

    _identity: LiveJoinIdentity
    _channel: GroupChannelInterface
    _pending: deque[FanoutMessage]
    _source: _JoinSource


@dataclass(frozen=True)
class _Joining(_JoinState):
    """Own one exact provisional attempt and its quarantined messages."""

    _attempt: JoinAttemptIdentity
    _channel: GroupChannelInterface
    _identity: LiveJoinIdentity | None
    _pending: deque[FanoutMessage]


class _LaneWatchOptions(TypedDict, total=False):
    """Type optional identities and deadline for one lane phase."""

    deadline_at: float | None
    callback_id: str | None
    delivery_attempt: int | None
    event_id: int | None
    plugin_id: str | None


class GroupLane:
    """Run one group's setup and callbacks serially in the owning thread.

    Setup must succeed before callback delivery, resume, or live joining.
    The lane owns its plugin and refetch handles and, for asynchronous groups,
    a loop used only by the owning thread. `close()` releases those resources;
    the supplied `GroupState` remains owned by the caller.
    """

    def __init__(self, **options: Unpack[_GroupLaneOptions]) -> None:
        """Create one unstarted serial lane.

        Args:
            **options: State, configuration, plugins, worker identity, health
                interval, async cancellation grace, and optional injected
                clock, token, and wait functions.

        Raises:
            ValueError: `worker_instance` is empty, `health_interval_secs` is
                not positive, or `async_cancel_grace_secs` is not finite and
                positive.
        """
        state = options['state']
        site = options['site']
        group = options['group']
        provider = options['provider']
        plugins = options['plugins']
        worker_instance = options['worker_instance']
        health_interval_secs = options['health_interval_secs']
        watchdog = options['watchdog']
        async_cancel_grace_secs = options.get('async_cancel_grace_secs', 5.0)
        clock = options.get('clock')
        token_factory = options.get('token_factory')
        wait = options.get('wait')
        self._validate_runtime(
            worker_instance,
            health_interval_secs,
            async_cancel_grace_secs,
        )
        self._initialize_invocation_state()
        self._async_cancel_grace_secs = float(async_cancel_grace_secs)
        self._callback_deadline_secs = group.callback_deadline_secs
        self._clock = time.monotonic if clock is None else clock
        self._group = group
        self._state_lock = threading.Lock()
        self._health_interval_secs = health_interval_secs
        source = site.sources[group.source]
        source_credential = site.credentials[source.credentials]
        self._internal_stall_deadline_secs = internal_stall_deadline_secs(
            health_interval_secs,
            source_credential,
        )
        self._watchdog = watchdog
        runtime_plugins = self._wrapped_plugins(plugins)
        self._host = PluginHost(
            site,
            group,
            provider,
            runtime_plugins,
            heartbeat=self.heartbeat,
            shutdown_requested=self.shutdown_requested,
        )
        self._initialize_strict_state()
        self._setup_failure: PluginError | None = None
        self._setup_finished = False
        self._shutdown_requested = False
        self._provider = provider
        self._refetch_handle: SyncSG | None = None
        self._site = site
        self._source = source
        self._state = state
        self._last_snapshot = state.snapshot()
        self._token_factory = _token if token_factory is None else token_factory
        self._used_tokens: set[str] | None = None if token_factory is None else set()
        self._wait = _sleep_wait if wait is None else wait
        self._worker_instance = worker_instance

    def _async_loop(self) -> asyncio.AbstractEventLoop:
        """Return the persistent event loop owned by this lane thread."""
        thread_ident = threading.get_ident()
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            self._loop_thread_ident = thread_ident
        elif self._loop_thread_ident != thread_ident:
            raise RuntimeError('async lane loop used outside its owning thread')
        return self._loop

    def _cancel_async_task(self, task: asyncio.Task[PluginError | None]) -> None:
        """Record and schedule one exact daemon-requested cancellation."""
        with self._async_lock:
            if self._async_cancel_task is not task:
                return
            self._async_cancel_task = None
            self._async_cancel_timer = None
            if (
                self._current_async_task is not task
                or self._daemon_cancelled_task is task
                or task.done()
                or self._async_loop_closing
            ):
                return
            loop = self._loop
            if loop is None:
                return
            self._daemon_cancelled_task = task
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                if self._daemon_cancelled_task is task:
                    self._daemon_cancelled_task = None

    def _clear_async_cancel_timer_locked(
        self,
        task: asyncio.Task[PluginError | None] | None = None,
    ) -> None:
        """Cancel and clear one pending exact-task grace timer."""
        if task is not None and self._async_cancel_task is not task:
            return
        timer = self._async_cancel_timer
        self._async_cancel_task = None
        self._async_cancel_timer = None
        if timer is not None:
            timer.cancel()

    def _detach_join(self, owned: _Joining | _FinalizedJoin) -> None:
        """Detach only the exact currently owned channel."""
        if self._join_state is not owned:
            return
        owned._pending.clear()
        self._join_state = _DetachedJoin()
        with suppress(BaseException):
            owned._channel.close()

    @staticmethod
    def _event_manifest(
        callbacks: tuple[tuple[PluginSpec, CallbackSpec], ...],
    ) -> CallbackManifest:
        """Build the exact event-local ordered callback manifest."""
        return CallbackManifest(
            callbacks=tuple(
                replace(callback.identity, ordinal=ordinal)
                for ordinal, (_plugin, callback) in enumerate(callbacks)
            )
        )

    def _failure_text(
        self,
        plugin_id: str,
        callback_id: str,
        failure: BaseException,
    ) -> str:
        """Build one redacted traceback without exception-local values."""
        frames = self._callback_tracebacks.pop((plugin_id, callback_id), ())
        lines = ['Traceback (most recent call last):']
        lines.extend(
            f'  File "{frame.filename}", line {frame.lineno}, in {frame.name}'
            for frame in frames[-1:]
        )
        lines.append(f'{type(failure).__name__}: {failure}')
        return '\n'.join(lines)

    def _finish_invocation(self, generation: int) -> None:
        """Clear only the exact completed plugin invocation."""
        with self._state_lock:
            if self._active_invocation_generation == generation:
                self._active_invocation_generation = None

    def _heartbeat_invocation(self, generation: int) -> None:
        """Refresh only the exact currently active plugin invocation."""
        with self._state_lock:
            if (
                self._active_invocation_generation != generation
                or self._watchdog.sample().phase not in {'callback', 'setup'}
            ):
                return
            self._watchdog.heartbeat(sliding_deadline_secs=self._callback_deadline_secs)

    def _initialize_invocation_state(self) -> None:
        """Initialize loop, cancellation, heartbeat, and traceback ownership."""
        self._active_invocation_generation: int | None = None
        self._async_cancel_task: asyncio.Task[PluginError | None] | None = None
        self._async_cancel_timer: threading.Timer | None = None
        self._async_lock = threading.Lock()
        self._async_loop_closing = False
        self._callback_tracebacks: dict[
            tuple[str, str],
            tuple[traceback.FrameSummary, ...],
        ] = {}
        self._current_async_task: asyncio.Task[PluginError | None] | None = None
        self._daemon_cancelled_task: asyncio.Task[PluginError | None] | None = None
        self._invocation_generation = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread_ident: int | None = None

    def _initialize_strict_state(self) -> None:
        """Initialize process-local join and strict audit projections."""
        self._join_state: _JoinState = _DetachedJoin()
        self._ordering_anomaly_buffer: deque[OrderingAnomaly] = deque(maxlen=100)
        self._source_successful_clock: float | None = None

    def _invoke_async_callback(
        self,
        plugin: PluginSpec,
        callback: CallbackSpec,
        event: Event,
        *,
        delivery_attempt: int,
        is_operator_replay: bool,
    ) -> tuple[BaseException | None, bool]:
        """Await one callback and classify daemon-requested cancellation."""
        loop = self._async_loop()
        with self._async_lock:
            if self._shutdown_requested or self._async_loop_closing:
                return None, True
            task = loop.create_task(
                self._host.invoke_async(
                    plugin,
                    callback,
                    event,
                    delivery_attempt=delivery_attempt,
                    is_operator_replay=is_operator_replay,
                )
            )
            self._current_async_task = task
        failure: BaseException | None
        try:
            failure = loop.run_until_complete(task)
        except asyncio.CancelledError as error:
            self._callback_tracebacks[(plugin.id, callback.id)] = tuple(
                traceback.extract_tb(error.__traceback__)
            )
            failure = asyncio.CancelledError()
        finally:
            with self._async_lock:
                interrupted = self._daemon_cancelled_task is task
                if self._current_async_task is task:
                    self._current_async_task = None
                if self._daemon_cancelled_task is task:
                    self._daemon_cancelled_task = None
                self._clear_async_cancel_timer_locked(task)
        if interrupted:
            return None, True
        return failure, False

    @staticmethod
    def _join_identity(
        attempt: JoinAttemptIdentity,
        target: JoinTarget,
        snapshot: GroupSnapshot,
    ) -> LiveJoinIdentity:
        """Validate and return one complete source-captured identity."""
        identity = target.identity
        if (
            identity.site != attempt.site
            or identity.source != attempt.source
            or identity.group != attempt.group
            or identity.join_kind != attempt.join_kind
            or identity.group_generation != attempt.group_generation
            or identity.attempt_token != attempt.attempt_token
            or identity.source_generation != snapshot.source_generation
        ):
            raise JoinProtocolError('join target identity conflicts')
        if type(identity.boundary) is not int or identity.boundary < snapshot.cursor:
            raise JoinProtocolError('join target boundary conflicts')
        return identity

    @staticmethod
    def _join_retry(
        retry: JoinRetry,
        attempt: JoinAttemptIdentity,
    ) -> JoinRetry:
        """Validate one exact bounded fresh-token retry."""
        if retry.attempt != attempt:
            raise JoinProtocolError('join retry identity conflicts')
        return retry

    def _mint_attempt(self, snapshot: GroupSnapshot) -> JoinAttemptIdentity:
        """Mint one non-reusable attempt for the current durable identity."""
        token = self._token_factory()
        used_tokens = self._used_tokens
        if not token or (used_tokens is not None and token in used_tokens):
            raise JoinProtocolError('join attempt token is empty or reused')
        if used_tokens is not None:
            used_tokens.add(token)
        return JoinAttemptIdentity(
            site=snapshot.site,
            source=snapshot.source,
            group=snapshot.group,
            join_kind=snapshot.ordering,
            group_generation=snapshot.group_generation,
            attempt_token=token,
        )

    @property
    def _ordering_anomalies(self) -> tuple[OrderingAnomaly, ...]:
        """Return body-free strict ordering decisions."""
        return tuple(self._ordering_anomaly_buffer)

    def _promote_join(
        self,
        source: _JoinSource,
        joining: _Joining,
        identity: LiveJoinIdentity,
    ) -> None:
        """Promote only an attached exact attempt to its callback gate."""
        if self._join_state is not joining or not joining._channel.status.attached:
            raise JoinProtocolError('live channel detached during finalize')
        self._join_state = _FinalizedJoin(
            _identity=identity,
            _channel=joining._channel,
            _pending=joining._pending,
            _source=source,
        )

    def _remember(self, snapshot: GroupSnapshot) -> GroupSnapshot:
        """Publish one immutable snapshot to cross-thread health readers."""
        with self._state_lock:
            self._last_snapshot = snapshot
        return snapshot

    @staticmethod
    def _retain_first_cleanup_error(
        first_error: BaseException | None,
        error: BaseException,
    ) -> BaseException:
        """Retain the earliest cleanup error."""
        return error if first_error is None else first_error

    def _start_invocation(self) -> tuple[int, Callable[[], None]]:
        """Bind one heartbeat callback to a fresh invocation generation."""
        with self._state_lock:
            self._invocation_generation += 1
            generation = self._invocation_generation
            self._active_invocation_generation = generation
        return generation, lambda: self._heartbeat_invocation(generation)

    def _synchronize_source_clock(self, source_successful_clock: float) -> float:
        """Accept one finite nonnegative non-regressing source clock."""
        if not isfinite(source_successful_clock) or source_successful_clock < 0:
            raise ValueError('source successful clock must be finite and nonnegative')
        current = self._source_successful_clock
        if current is not None and source_successful_clock < current:
            raise ValueError('source successful clock regressed')
        self._source_successful_clock = float(source_successful_clock)
        return self._source_successful_clock

    @staticmethod
    def _validate_runtime(
        worker_instance: str,
        health_interval_secs: float,
        async_cancel_grace_secs: float,
    ) -> None:
        """Validate supervisor-issued lane runtime settings."""
        if not worker_instance:
            raise ValueError('worker instance must not be empty')
        if health_interval_secs <= 0:
            raise ValueError('health interval must be positive')
        if not isfinite(async_cancel_grace_secs) or async_cancel_grace_secs <= 0:
            raise ValueError('async cancellation grace must be finite and positive')

    def _wrap_callback(
        self,
        plugin_id: str,
        callback: CallbackSpec,
    ) -> Callable[[EventContext], object]:
        """Bind one callback heartbeat generation and traceback capture."""
        key = (plugin_id, callback.id)
        if self._group.execution == 'async':

            async def invoke_async_callback(context: EventContext) -> None:
                generation, heartbeat = self._start_invocation()
                scoped = with_heartbeat(context, heartbeat)
                try:
                    await _invoke_async(callback, scoped)
                except Exception as error:
                    self._callback_tracebacks[key] = tuple(
                        traceback.extract_tb(error.__traceback__)
                    )
                    raise
                finally:
                    self._finish_invocation(generation)

            return invoke_async_callback

        def invoke_sync_callback(context: EventContext) -> None:
            generation, heartbeat = self._start_invocation()
            scoped = with_heartbeat(context, heartbeat)
            try:
                _invoke_sync(callback, scoped)
            except Exception as error:
                self._callback_tracebacks[key] = tuple(traceback.extract_tb(error.__traceback__))
                raise
            finally:
                self._finish_invocation(generation)

        return invoke_sync_callback

    def _wrap_setup(
        self,
        setup: Callable[[SetupContext], object],
    ) -> Callable[[SetupContext], object]:
        """Bind one setup heartbeat generation."""
        if self._group.execution == 'async':

            async def setup_async(context: SetupContext) -> None:
                generation, heartbeat = self._start_invocation()
                scoped = with_heartbeat(context, heartbeat)
                try:
                    result = setup(scoped)
                    if not inspect.isawaitable(result):
                        raise TypeError('async setup did not return an awaitable')
                    await result
                finally:
                    self._finish_invocation(generation)

            return setup_async

        def setup_sync(context: SetupContext) -> object:
            generation, heartbeat = self._start_invocation()
            scoped = with_heartbeat(context, heartbeat)
            try:
                return setup(scoped)
            finally:
                self._finish_invocation(generation)

        return setup_sync

    def _wrapped_plugins(
        self,
        plugins: Iterable[PluginSpec],
    ) -> tuple[PluginSpec, ...]:
        """Bind invocation generations and callback traceback capture."""
        return tuple(
            replace(
                plugin,
                setup=None if plugin.setup is None else self._wrap_setup(plugin.setup),
                callbacks=tuple(
                    replace(
                        callback,
                        callback=self._wrap_callback(plugin.id, callback),
                    )
                    for callback in plugin.callbacks
                ),
            )
            for plugin in plugins
        )

    def health(self) -> LaneHealth:
        """Return one body-free immutable lane health observation.

        Returns:
            Current phase, durable position, and watchdog evidence.
        """
        now = self._clock()
        sample = self._watchdog.sample()
        with self._state_lock:
            snapshot = self._last_snapshot
        phase = cast('GroupPhase', sample.phase)
        deadline_at = sample.deadline_at
        heartbeat_at = sample.heartbeat_at
        overdue_reason: str | None = None
        if deadline_at is not None and now >= deadline_at:
            overdue_reason = {
                'callback': 'callback overdue',
                'internal': 'internal stall',
                'shutdown': 'internal stall',
                'setup': 'setup overdue',
                'startup': 'internal stall',
                'write-hold': 'internal stall',
                'write-lock-wait': 'internal stall',
            }.get(phase)
        return LaneHealth(
            site=self._site.name,
            source=self._group.source,
            group=self._group.name,
            worker_instance=self._worker_instance,
            execution=self._group.execution,
            ordering=self._group.ordering,
            phase=phase,
            status=snapshot.status,
            cursor=snapshot.cursor,
            event_id=sample.event_id,
            plugin_id=sample.plugin_id,
            callback_id=sample.callback_id,
            delivery_attempt=sample.delivery_attempt,
            heartbeat_at=heartbeat_at,
            heartbeat_age=max(0.0, now - heartbeat_at),
            deadline_at=deadline_at,
            internal_stall_deadline_secs=self._internal_stall_deadline_secs,
            overdue_reason=overdue_reason,
        )

    def heartbeat(self) -> None:
        """Refresh the current invocation watchdog."""
        with self._state_lock:
            generation = self._active_invocation_generation
        if generation is not None:
            self._heartbeat_invocation(generation)

    def request_shutdown(self) -> None:
        """Request cooperative shutdown between serial event deliveries.

        For async delivery, an active async task is cancelled after the
        configured grace period.
        The interrupted callback remains pending for recovery, including when
        it suppresses cancellation.
        """
        if self._group.execution != 'async':
            self._shutdown_requested = True
            return
        timer: threading.Timer | None = None
        with self._async_lock:
            self._shutdown_requested = True
            task = self._current_async_task
            if (
                task is not None
                and not task.done()
                and self._daemon_cancelled_task is not task
                and self._async_cancel_task is not task
                and not self._async_loop_closing
            ):
                timer = threading.Timer(
                    self._async_cancel_grace_secs,
                    self._cancel_async_task,
                    args=(task,),
                )
                timer.daemon = True
                self._async_cancel_task = task
                self._async_cancel_timer = timer
        if timer is not None:
            timer.start()

    def shutdown_requested(self) -> bool:
        """Return whether cooperative lane shutdown was requested.

        Returns:
            Current cooperative shutdown state.
        """
        return self._shutdown_requested

    @contextmanager
    def watch_phase(
        self,
        phase: GroupPhase,
        **options: Unpack[_LaneWatchOptions],
    ) -> Generator[None]:
        """Watch one lane operation through the shared component cell.

        Args:
            phase: Scoped lane phase.
            **options: Optional absolute deadline and in-flight identities.

        Yields:
            Control while the watched operation runs without a phase lock.
        """
        deadline_at = options.get('deadline_at')
        if deadline_at is None and phase != 'idle':
            deadline_secs = (
                self._callback_deadline_secs
                if phase in {'callback', 'setup'}
                else self._internal_stall_deadline_secs
            )
            deadline_at = self._clock() + deadline_secs
        with self._watchdog.watch(
            phase,
            deadline_at=deadline_at,
            callback_id=options.get('callback_id'),
            delivery_attempt=options.get('delivery_attempt'),
            event_id=options.get('event_id'),
            plugin_id=options.get('plugin_id'),
        ):
            yield

    @contextmanager
    def _watch_phase(
        self,
        phase: Literal['callback', 'internal', 'setup'],
        **options: Unpack[_LaneWatchOptions],
    ) -> Generator[None]:
        """Watch one lane-owned operation through the shared phase cell."""
        with self.watch_phase(
            phase,
            **options,
        ):
            yield

    def _acknowledge_strict_control(
        self,
        source: _JoinSource,
        control: StrictCapacityControl | StrictOrderingControl,
    ) -> None:
        """Acknowledge one exact strict control after the lane commit."""
        with self._watch_phase('internal'):
            source.acknowledge_strict_control(control)

    def _confirm_join(
        self,
        source: _JoinSource,
        identity: LiveJoinIdentity,
        attempt: JoinAttemptIdentity,
    ) -> Attached | JoinRetry:
        """Accept one exact source preparation or current-token retry."""
        confirmation = JoinConfirm(identity)
        for _attempt_number in range(_STALE_REPLY_LIMIT):
            with self._watch_phase('internal'):
                candidate = source.confirm(confirmation)
            candidate_token = (
                candidate.attempt.attempt_token
                if isinstance(candidate, JoinRetry)
                else candidate.identity.attempt_token
            )
            if candidate_token != identity.attempt_token:
                continue
            if isinstance(candidate, JoinRetry):
                return self._join_retry(candidate, attempt)
            if candidate != Attached(identity):
                raise JoinProtocolError('join attached identity conflicts')
            return candidate
        raise JoinProtocolError('join confirmation received only stale replies')

    def _consume_strict_control(
        self,
        control: StrictCapacityControl | StrictOrderingControl,
        snapshot: GroupSnapshot,
    ) -> tuple[GroupSnapshot, bool]:
        """Fence and commit one strict live control at the configured durability level."""
        owned = self._join_state
        if (
            not isinstance(owned, _FinalizedJoin)
            or owned._identity != control.identity
            or owned._channel.group != control.identity.group
        ):
            raise JoinProtocolError('strict control targets a stale live identity')
        if isinstance(control, StrictCapacityControl):
            with self._watch_phase('internal'):
                halted = self._state.mark_strict_capacity_exhausted(
                    control.identity,
                    abort_limit=control.abort_limit,
                    expected_revision=snapshot.revision,
                )
            self._acknowledge_strict_control(owned._source, control)
            owned._channel.detach('strict-capacity-exhausted')
            return self._remember(halted), True
        with self._watch_phase('internal'):
            decided = self._state.record_strict_ordering_decision(
                control,
                expected_revision=snapshot.revision,
            )
        self._acknowledge_strict_control(owned._source, control)
        self._ordering_anomaly_buffer.append(
            OrderingAnomaly(
                group=control.identity.group,
                group_generation=control.identity.group_generation,
                event_id=control.event_id,
                last_delivered_id=control.last_delivered_id,
                delivery_floor=control.delivery_floor,
                action=control.action,
            )
        )
        if control.action == 'halt':
            owned._channel.detach('ordering-anomaly')
        return self._remember(decided), True

    def _consume_live_message(
        self,
        message: RangeChanged | StrictCapacityControl | StrictOrderingControl,
        snapshot: GroupSnapshot,
    ) -> tuple[GroupSnapshot, bool]:
        """Consume one pending live message through its durable boundary."""
        if isinstance(message, RangeChanged):
            return snapshot, True
        return self._consume_strict_control(message, snapshot)

    def _drain_channel(
        self,
        owned: _Joining | _FinalizedJoin,
    ) -> tuple[FanoutMessage, ...]:
        """Drain only an attached exact channel or detach its ownership."""
        if self._join_state is not owned:
            raise JoinProtocolError('live channel ownership changed')
        if not owned._channel.status.attached:
            self._detach_join(owned)
            raise JoinProtocolError('live channel detached before drain')
        with self._watch_phase('internal'):
            messages = owned._channel.drain()
        if not owned._channel.status.attached:
            self._detach_join(owned)
            raise JoinProtocolError('live channel detached during drain')
        return messages

    def _drain_attached(
        self,
        joining: _Joining,
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Retain quarantined event bodies until attachment commits."""
        for message in self._drain_channel(joining):
            joining._pending.append(message)
        if not joining._channel.status.attached:
            self._detach_join(joining)
            raise JoinProtocolError('live channel detached during attachment')
        return snapshot

    def _finalize_join(
        self,
        source: _JoinSource,
        identity: LiveJoinIdentity,
    ) -> None:
        """Obtain one exact source finalize acknowledgement."""
        finalization = JoinFinalize(identity)
        for _attempt_number in range(_STALE_REPLY_LIMIT):
            with self._watch_phase('internal'):
                finalized = source.finalize(finalization)
            if finalized.identity.attempt_token != identity.attempt_token:
                continue
            if finalized != JoinFinalized(identity):
                raise JoinProtocolError('join finalized identity conflicts')
            return
        raise JoinProtocolError('join finalize received only stale replies')

    def _flush_irrelevant(
        self,
        snapshot: GroupSnapshot,
        high_id: int | None,
        *,
        live_prefix: bool = False,
    ) -> GroupSnapshot:
        """Commit one accumulated irrelevant range, clipping live progress at a fence."""
        if high_id is None or high_id <= snapshot.cursor:
            return snapshot
        with self._watch_phase('internal'):
            advance = (
                self._state.advance_irrelevant_prefix
                if live_prefix
                else self._state.advance_irrelevant
            )
            advanced = advance(high_id, expected_revision=snapshot.revision)
        self._remember(advanced)
        if advanced.cursor < high_id:
            raise StrictObligationBlockedError('live irrelevant suffix is blocked')
        return advanced

    def _internal_items(self, items: Iterable[_Item]) -> Iterator[_Item]:
        """Yield items while watching each potentially blocking read."""
        iterator = iter(items)
        while True:
            try:
                with self._watch_phase('internal'):
                    item = next(iterator)
            except StopIteration:
                return
            yield item

    def _invoke_callbacks(
        self,
        event: Event,
        callbacks: tuple[tuple[PluginSpec, CallbackSpec], ...],
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Invoke pending callbacks and preserve terminal marks."""
        marks = {mark[0]: mark for mark in snapshot.callback_marks}
        for ordinal, (plugin, callback) in enumerate(callbacks):
            mark = marks.get(ordinal)
            if mark is None:
                raise RuntimeError('durable callback manifest is incomplete')
            if mark[3] != 'pending':
                continue
            with self._watch_phase('internal'):
                started = self._state.begin_callback(
                    ordinal,
                    expected_revision=snapshot.revision,
                )
            snapshot = self._remember(started)
            delivery_attempt = snapshot.callback_marks[ordinal][4]
            self._callback_tracebacks.pop((plugin.id, callback.id), None)
            with self._watch_phase(
                'callback',
                callback_id=callback.id,
                delivery_attempt=delivery_attempt,
                event_id=cast('int', event['id']),
                plugin_id=plugin.id,
            ):
                if self._group.execution == 'async':
                    failure, interrupted = self._invoke_async_callback(
                        plugin,
                        callback,
                        event,
                        delivery_attempt=delivery_attempt,
                        is_operator_replay=snapshot.is_operator_replay,
                    )
                else:
                    failure = self._host.invoke_sync(
                        plugin,
                        callback,
                        event,
                        delivery_attempt=delivery_attempt,
                        is_operator_replay=snapshot.is_operator_replay,
                    )
                    interrupted = False
            if interrupted:
                return snapshot
            if failure is not None:
                with self._watch_phase('internal'):
                    failed = self._state.callback_failed(
                        ordinal,
                        self._failure_text(plugin.id, callback.id, failure),
                        expected_revision=snapshot.revision,
                    )
                return self._remember(failed)
            with self._watch_phase('internal'):
                completed = self._state.callback_done(
                    ordinal,
                    expected_revision=snapshot.revision,
                )
            snapshot = self._remember(completed)
        return snapshot

    def _invoke_event(
        self,
        delivery: Delivery,
        callbacks: tuple[tuple[PluginSpec, CallbackSpec], ...],
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Persist and invoke one matching event in manifest order."""
        event_id = cast('int', delivery.event['id'])
        with self._watch_phase('internal'):
            started = self._state.start_event(
                event_id,
                self._event_manifest(callbacks),
                expected_revision=snapshot.revision,
                obligation_id=delivery.obligation_id,
            )
        snapshot = self._remember(started)
        return self._invoke_callbacks(delivery.event, callbacks, snapshot)

    def _live_deliveries(self, owned: _FinalizedJoin, deadline_at: float) -> Iterator[Delivery]:
        """Decode an attached event prefix, stopping at detachment or controls."""
        for message in owned._pending:
            if (
                self._clock() >= deadline_at
                or not owned._channel.status.attached
                or not isinstance(message, EventDelivery)
            ):
                return
            if self._group.ordering == 'strict':
                with self._watch_phase('internal'):
                    fence = self._state.strict_obligation_fence()
                if fence is not None and message.encoded.event_id >= fence[1]:
                    return
            event = decode_event(message.encoded.event_frame)
            if event['id'] != message.encoded.event_id:
                raise ValueError('live event frame identity conflicts')
            yield Delivery(event)

    def _preflight_join(
        self,
        identity: LiveJoinIdentity,
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Validate one exact group-side join without durable mutation."""
        try:
            with self._watch_phase('internal'):
                validated = self._state.validate_live_join(
                    identity,
                    expected_revision=snapshot.revision,
                )
        except (InvalidTransitionError, RevisionConflictError) as error:
            if snapshot.cursor < identity.boundary:
                message = 'join catch-up did not reach a committable boundary'
            else:
                message = f'join preflight rejected: {error}'
            raise JoinProtocolError(message) from error
        return self._remember(validated)

    def _prepare_live_drain(
        self,
        channel: GroupChannelInterface,
        idle_snapshot: GroupSnapshot | None,
    ) -> tuple[_FinalizedJoin, GroupSnapshot | None]:
        """Validate the live owner and reuse its gate observation only when empty."""
        owned = self._join_state
        if (
            not isinstance(owned, _FinalizedJoin)
            or owned._channel is not channel
            or owned._identity.group != channel.group
        ):
            raise JoinProtocolError('live callback gate is closed')
        if idle_snapshot is None:
            return owned, None
        if not channel.status.attached:
            self._detach_join(owned)
            raise JoinProtocolError('live channel detached before processing')
        if not owned._pending:
            owned._pending.extend(self._drain_channel(owned))
        return owned, None if owned._pending else self._remember(idle_snapshot)

    def _query_event(self, event_id: int) -> Event | None:
        """Read one exact event body through a lane-owned source handle."""
        if self._refetch_handle is None:
            credential = self._site.credentials[self._source.credentials]
            with self._watch_phase('internal'):
                self._refetch_handle = self._provider.create_sync(
                    self._site.server,
                    credential,
                )
        with self._watch_phase('internal'):
            rows = self._refetch_handle.find(
                'EventLogEntry',
                [['id', 'is', event_id]],
                list(EVENT_FIELDS),
                limit=1,
                order=None,
                page=0,
            )
        if not rows:
            return None
        if len(rows) != 1:
            raise ValueError('exact event query returned more than one row')
        event = Event(rows[0])
        if event['id'] != event_id:
            raise ValueError('exact event query returned another event id')
        return event

    def _request_join(
        self,
        source: _JoinSource,
        request: JoinRequest,
        channel: GroupChannelInterface,
    ) -> JoinTarget | JoinRetry:
        """Ignore old-token replies and idempotently resend one attempt."""
        for _attempt_number in range(_STALE_REPLY_LIMIT):
            with self._watch_phase('internal'):
                response = source.request(request, channel)
            response_token = (
                response.attempt.attempt_token
                if isinstance(response, JoinRetry)
                else response.identity.attempt_token
            )
            if response_token != request.attempt.attempt_token:
                continue
            return response
        raise JoinProtocolError('join request received only stale replies')

    def _retention_floor(self) -> int | None:
        """Read and validate the current lowest visible event id."""
        if self._refetch_handle is None:
            raise RuntimeError('refetch handle is not open')
        with self._watch_phase('internal'):
            rows = self._refetch_handle.find(
                'EventLogEntry',
                [],
                ['id'],
                limit=1,
                order=[{'field_name': 'id', 'direction': 'asc'}],
                page=0,
            )
        if not rows:
            return None
        floor = rows[0].get('id')
        if type(floor) is not int or floor <= 0:
            raise ValueError('lowest visible EventLogEntry id is invalid')
        return floor

    def _restore_event(
        self,
        snapshot: GroupSnapshot,
    ) -> tuple[GroupSnapshot, Event | None]:
        """Refetch an in-flight body or persist exact absence evidence."""
        event_id = snapshot.in_flight_event_id
        if event_id is None:
            return snapshot, None
        event = self._query_event(event_id)
        if event is None:
            with self._watch_phase('internal'):
                interrupted = self._wait(self._source.fetch_interval_secs)
            if interrupted:
                raise InterruptedError('in-flight event refetch interrupted')
            event = self._query_event(event_id)
        if event is None:
            frontier = self._state.source_frontier()
            floor = self._retention_floor()
            error = history_truncation(floor, event_id, frontier)
            if error is not None:
                with self._watch_phase('internal'):
                    truncated = self._state.mark_history_truncated(
                        error.retention_floor,
                        expected_revision=snapshot.revision,
                    )
                return (
                    truncated,
                    None,
                )
            if snapshot.required_event_id is None:
                with self._watch_phase('internal'):
                    snapshot = self._state.require_event(
                        event_id,
                        expected_revision=snapshot.revision,
                    )
            return snapshot, None
        if snapshot.required_event_id is not None:
            if snapshot.retry_event_id != event_id:
                return snapshot, None
            with self._watch_phase('internal'):
                snapshot = self._state.restore_required_event(
                    event_id,
                    expected_revision=snapshot.revision,
                )
        return snapshot, event

    def _snapshot(self) -> GroupSnapshot:
        """Read and publish one durable snapshot as internal work."""
        with self._watch_phase('internal'):
            snapshot = self._state.snapshot()
        return self._remember(snapshot)

    def _catch_up_join(
        self,
        identity: LiveJoinIdentity,
        catch_up: Callable[[int], None],
    ) -> GroupSnapshot:
        """Catch up one attempt through its fixed source boundary."""
        catch_up(identity.boundary)
        return self._snapshot()

    def _commit_join(
        self,
        identity: LiveJoinIdentity,
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Compare-and-swap one exact group-side join identity."""
        try:
            with self._watch_phase('internal'):
                committed = self._state.commit_live_join(
                    identity,
                    expected_revision=snapshot.revision,
                )
        except RevisionConflictError as error:
            if self._snapshot().status == 'running':
                raise
            raise JoinProtocolError('group halted during live join') from error
        return self._remember(committed)

    def _complete_scan(self, obligation_id: str) -> GroupSnapshot:
        """Commit completion unless a competing collapse has halted delivery."""
        try:
            with self._watch_phase('internal'):
                completed = self._state.complete_obligation_scan(obligation_id)
        except RevisionConflictError:
            current = self._snapshot()
            if current.status == 'running':
                raise
            return current
        return self._remember(completed)

    def _invoke_late(
        self,
        delivery: Delivery,
        callbacks: tuple[tuple[PluginSpec, CallbackSpec], ...],
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Reserve and terminally process one obligation-owned event."""
        obligation_id = delivery.obligation_id
        if obligation_id is None:
            raise RuntimeError('late delivery has no obligation identity')
        event_id = cast('int', delivery.event['id'])
        with self._watch_phase('internal'):
            reserved = self._state.reserve_late_event(
                obligation_id,
                event_id,
                expected_revision=snapshot.revision,
            )
        snapshot = self._remember(reserved)
        if 'late-capacity-exhausted' in snapshot.conditions:
            return snapshot
        try:
            if callbacks:
                return self._invoke_event(delivery, callbacks, snapshot)
            with self._watch_phase('internal'):
                completed = self._state.complete_late_id(
                    obligation_id,
                    event_id,
                    expected_revision=snapshot.revision,
                )
            return self._remember(completed)
        except TerminalLateEventError:
            return self._snapshot()

    def _record_strict_join_outcome(
        self,
        attempt: JoinAttemptIdentity,
        *,
        capacity_caused: bool,
    ) -> GroupSnapshot:
        """Commit one exact strict join capacity outcome at configured durability."""
        snapshot = self._snapshot()
        if attempt.join_kind != 'strict':
            return snapshot
        with self._watch_phase('internal'):
            recorded = self._state.record_strict_join_outcome(
                attempt_token=attempt.attempt_token,
                capacity_caused=capacity_caused,
                abort_limit=JOIN_ABORT_LIMIT,
                expected_revision=snapshot.revision,
            )
        return self._remember(recorded)

    def _abort_join(
        self,
        source: _JoinSource,
        joining: _Joining,
        *,
        capacity_caused: bool,
    ) -> None:
        """Abort one exact source attempt and discard its queued bodies."""
        if self._join_state is not joining:
            return
        try:
            if joining._identity is not None:
                with self._watch_phase('internal'):
                    source.abort(JoinAbort(joining._identity))
        finally:
            try:
                self._record_strict_join_outcome(
                    joining._attempt,
                    capacity_caused=capacity_caused,
                )
            finally:
                self._detach_join(joining)

    def _cleanup_join(self, source: _JoinSource, joining: _Joining, error: BaseException) -> None:
        """Best-effort cleanup one exact failed attempt."""
        if self._join_state is not joining:
            return
        if isinstance(error, (WriteLockTimeoutError, WriteLockStoppedError)):
            # Owner-fatal writes must escape without another RPC or state write.
            # Closing the local channel lets peer-loss cleanup release the lease.
            self._detach_join(joining)
            return
        with suppress(BaseException):
            self._abort_join(source, joining, capacity_caused=False)
        self._detach_join(joining)

    def _resume_guarded(self) -> GroupSnapshot:
        """Resume one durable in-flight event after setup or recovery."""
        if not self._setup_finished:
            raise RuntimeError('lane setup has not completed')
        snapshot = self._snapshot()
        if snapshot.in_flight_event_id is None:
            return snapshot
        if snapshot.status not in {'event-unavailable', 'running'}:
            return snapshot
        snapshot, event = self._restore_event(snapshot)
        snapshot = self._remember(snapshot)
        if event is None:
            return snapshot
        with self._watch_phase('internal'):
            callbacks = self._host.matching_callbacks(event)
            verified = self._state.verify_manifest(
                self._event_manifest(callbacks),
                expected_revision=snapshot.revision,
            )
        snapshot = self._remember(verified)
        if snapshot.status != 'running':
            return snapshot
        return self._invoke_callbacks(event, callbacks, snapshot)

    def _strict_preflight(
        self,
        event_id: int,
        snapshot: GroupSnapshot,
    ) -> tuple[GroupSnapshot, bool]:
        """Halt decreasing strict delivery or skip its audited floor."""
        if snapshot.ordering != 'strict':
            return snapshot, False
        if event_id <= snapshot.delivery_floor:
            with self._watch_phase('internal'):
                audited = self._state.record_strict_floor_skip(
                    event_id,
                    expected_revision=snapshot.revision,
                )
            self._ordering_anomaly_buffer.append(
                OrderingAnomaly(
                    group=snapshot.group,
                    group_generation=snapshot.group_generation,
                    event_id=event_id,
                    last_delivered_id=snapshot.cursor,
                    delivery_floor=snapshot.delivery_floor,
                    action='skip',
                )
            )
            return self._remember(audited), True
        if event_id < snapshot.cursor:
            with self._watch_phase('internal'):
                halted = self._state.mark_ordering_anomaly(
                    event_id,
                    expected_revision=snapshot.revision,
                )
            self._ordering_anomaly_buffer.append(
                OrderingAnomaly(
                    group=snapshot.group,
                    group_generation=snapshot.group_generation,
                    event_id=event_id,
                    last_delivered_id=snapshot.cursor,
                    delivery_floor=snapshot.delivery_floor,
                    action='halt',
                )
            )
            return self._remember(halted), True
        if event_id == snapshot.cursor:
            return snapshot, True
        return snapshot, False

    def _deliver_one(
        self,
        delivery: Delivery,
        snapshot: GroupSnapshot,
        irrelevant_high: int | None,
        *,
        live_prefix: bool = False,
    ) -> tuple[GroupSnapshot, int | None]:
        """Apply one delivery through strict, late, or ordinary ownership."""
        event_id = delivery.event['id']
        if type(event_id) is not int or event_id <= 0:
            raise ValueError('delivery event id must be a positive integer')
        if (
            snapshot.ordering == 'strict'
            and irrelevant_high is not None
            and event_id <= irrelevant_high
        ):
            snapshot = self._flush_irrelevant(snapshot, irrelevant_high, live_prefix=live_prefix)
            irrelevant_high = None
        snapshot, strict_consumed = self._strict_preflight(event_id, snapshot)
        if strict_consumed:
            return snapshot, irrelevant_high
        with self._watch_phase('internal'):
            callbacks = self._host.matching_callbacks(delivery.event)
        if delivery.obligation_id is not None:
            snapshot = self._flush_irrelevant(snapshot, irrelevant_high, live_prefix=live_prefix)
            return self._invoke_late(delivery, callbacks, snapshot), None
        if not callbacks:
            return snapshot, event_id
        snapshot = self._flush_irrelevant(snapshot, irrelevant_high, live_prefix=live_prefix)
        return self._invoke_event(delivery, callbacks, snapshot), None

    def _deliver_many_guarded(
        self,
        deliveries: Iterable[Delivery],
        snapshot: GroupSnapshot | None = None,
        *,
        live_ids: list[int] | None = None,
    ) -> tuple[GroupSnapshot, int]:
        """Deliver events and report the exact consumed prefix length."""
        if not self._setup_finished:
            raise RuntimeError('lane setup has not completed')
        if snapshot is None:
            snapshot = self._snapshot()
        if snapshot.status != 'running':
            return snapshot, 0
        irrelevant_high: int | None = None
        live_prefix = live_ids is not None
        consumed = 0
        for delivery in self._internal_items(deliveries):
            if self._shutdown_requested:
                return self._flush_irrelevant(
                    snapshot, irrelevant_high, live_prefix=live_prefix
                ), consumed
            snapshot, irrelevant_high = self._deliver_one(
                delivery,
                snapshot,
                irrelevant_high,
                live_prefix=live_prefix,
            )
            consumed += 1
            if live_ids is not None:
                live_ids.append(cast('int', delivery.event['id']))
            if snapshot.status != 'running':
                return snapshot, consumed
        return self._flush_irrelevant(snapshot, irrelevant_high, live_prefix=live_prefix), consumed

    def _deliver_many(
        self,
        deliveries: Iterable[Delivery],
        snapshot: GroupSnapshot | None = None,
        *,
        live_prefix: bool = False,
    ) -> tuple[GroupSnapshot, int]:
        """Stop a delivery batch when an external collapse invalidates its revision."""
        live_ids: list[int] | None = [] if live_prefix else None
        try:
            return self._deliver_many_guarded(deliveries, snapshot, live_ids=live_ids)
        except StrictObligationBlockedError:
            if live_ids is None:
                raise
            current = self._snapshot()
            committed = sum(
                1 for _ in takewhile(lambda event_id: event_id <= current.cursor, live_ids)
            )
            return current, committed
        except RevisionConflictError:
            current = self._snapshot()
            if current.status == 'running':
                raise
            return current, 0

    def close(self) -> None:
        """Close every resource owned by this lane.

        Cleanup attempts the refetch handle, plugin host, and async event loop
        even after an earlier failure. It clears lane ownership and raises the
        first cleanup failure after all applicable attempts.
        """
        sample = self._watchdog.sample()
        deadline_at = (
            sample.deadline_at
            if sample.phase == 'shutdown'
            else self._clock() + self._internal_stall_deadline_secs
        )
        with self.watch_phase('shutdown', deadline_at=deadline_at):
            first_error: BaseException | None = None
            if self._refetch_handle is not None:
                try:
                    self._refetch_handle.close()
                except BaseException as error:  # noqa: BLE001
                    first_error = self._retain_first_cleanup_error(first_error, error)
                finally:
                    self._refetch_handle = None
            if self._group.execution == 'async' and self._loop is not None:
                loop = self._async_loop()
                with self._async_lock:
                    self._async_loop_closing = True
                    self._clear_async_cancel_timer_locked()
                    try:
                        loop.run_until_complete(self._host.close_async())
                    except BaseException as error:  # noqa: BLE001
                        first_error = self._retain_first_cleanup_error(first_error, error)
                    try:
                        loop.close()
                    except BaseException as error:  # noqa: BLE001
                        first_error = self._retain_first_cleanup_error(first_error, error)
                    finally:
                        self._loop = None
                        self._loop_thread_ident = None
            else:
                try:
                    self._host.close_sync()
                except BaseException as error:  # noqa: BLE001
                    first_error = self._retain_first_cleanup_error(first_error, error)
            if first_error is not None:
                raise first_error

    def deliver_many_counted(self, deliveries: Iterable[Delivery]) -> tuple[GroupSnapshot, int]:
        """Deliver one page and return its durable snapshot and consumed prefix length.

        Args:
            deliveries: Ordered catch-up event bodies from one provider page.

        Returns:
            Latest snapshot and the number of deliveries offered before a halt
            or cooperative shutdown.
        """
        return self._deliver_many(deliveries)

    def drain_live(
        self,
        channel: GroupChannelInterface,
        *,
        idle_snapshot: GroupSnapshot | None = None,
        deadline_at: float = float('inf'),
    ) -> GroupSnapshot:
        """Release one exact finalized channel through ordinary scheduling.

        Args:
            channel: Finalized attempt-owned live channel.
            deadline_at: Monotonic deadline checked between deliveries. An
                active callback finishes before returning at the deadline.
            idle_snapshot: Optional callback-gate observation from this owner
                iteration, after commands and without intervening setup,
                resume, or join work. Used only if the channel is empty;
                queued delivery always reads fresh durable state.

        Returns:
            Immutable durable snapshot for this iteration, freshly read before
            queued delivery or taken at the callback gate for an empty drain.

        Raises:
            JoinProtocolError: The callback and event-body gate is closed.
        """
        owned, snapshot = self._prepare_live_drain(channel, idle_snapshot)
        if snapshot is not None:
            return snapshot
        snapshot = self._snapshot()
        while self._clock() < deadline_at:
            if not channel.status.attached:
                self._detach_join(owned)
                raise JoinProtocolError('live channel detached before processing')
            if not owned._pending:
                owned._pending.extend(self._drain_channel(owned))
                if not owned._pending:
                    return snapshot
            message = owned._pending[0]
            if isinstance(message, EventDelivery):
                snapshot, consumed = self._deliver_many(
                    self._live_deliveries(owned, deadline_at),
                    snapshot,
                    live_prefix=snapshot.ordering == 'strict',
                )
            else:
                snapshot, accepted = self._consume_live_message(message, snapshot)
                consumed = int(accepted)
            for _ in range(consumed):
                owned._pending.popleft()
            if snapshot.status != 'running' and not channel.status.attached:
                self._detach_join(owned)
            if snapshot.status != 'running' or (not consumed and channel.status.attached):
                return snapshot
        return snapshot

    def join(
        self,
        source: _JoinSource,
        channel: GroupChannelInterface,
        *,
        catch_up: Callable[[int], None],
    ) -> LiveJoinIdentity | JoinRetry:
        """Run one mode-neutral token-fenced live-join attempt.

        Args:
            source: Source-side join coordinator.
            channel: Attempt-owned quarantined live queue.
            catch_up: Driver that reaches the captured inclusive boundary.

        Returns:
            Finalized live identity or a bounded fresh-token retry.

        Raises:
            JoinProtocolError: Any current-token protocol identity conflicts.
            RuntimeError: Setup has not completed.
            WriteLockStoppedError: Owner write acquisition was stopped.
            WriteLockTimeoutError: Owner write acquisition exhausted its
                budget.
        """
        if not self._setup_finished:
            raise RuntimeError('lane setup has not completed')
        owned = self._join_state
        if isinstance(owned, _FinalizedJoin):
            if owned._channel.status.attached:
                raise JoinProtocolError('live join is already finalized')
            self._detach_join(owned)
        elif isinstance(owned, _Joining):
            raise JoinProtocolError('live join attempt is already active')
        snapshot = self._snapshot()
        attempt = self._mint_attempt(snapshot)
        joining = _Joining(
            _attempt=attempt,
            _channel=channel,
            _identity=None,
            _pending=deque(),
        )
        self._join_state = joining
        request = JoinRequest(attempt)
        try:
            response = self._request_join(source, request, channel)
            if isinstance(response, JoinRetry):
                retry = self._join_retry(response, attempt)
                self._record_strict_join_outcome(
                    attempt,
                    capacity_caused=snapshot.ordering == 'strict',
                )
                self._detach_join(joining)
                return retry
            identity = self._join_identity(attempt, response, snapshot)
            joining = replace(joining, _identity=identity)
            self._join_state = joining
            snapshot = self._catch_up_join(identity, catch_up)
            snapshot = self._preflight_join(identity, snapshot)
            attached = self._confirm_join(source, identity, attempt)
            if isinstance(attached, JoinRetry):
                self._abort_join(source, joining, capacity_caused=True)
                return attached
            snapshot = self._drain_attached(joining, snapshot)
            snapshot = self._preflight_join(identity, snapshot)
            self._commit_join(identity, snapshot)
            try:
                self._finalize_join(source, identity)
            except JoinCapacityError:
                self._abort_join(source, joining, capacity_caused=True)
                raise
            self._record_strict_join_outcome(
                attempt,
                capacity_caused=False,
            )
            self._promote_join(source, joining, identity)
        except BaseException as error:
            self._cleanup_join(source, joining, error)
            raise
        else:
            return identity

    def restart_event_manifest(self, event_id: int) -> CallbackManifest:
        """Return the active event-local callback manifest for a restart.

        Args:
            event_id: Exact in-flight event to refetch and match.

        Returns:
            Current ordered identities for callbacks matching the event.

        Raises:
            ValueError: The current event is unavailable.
        """
        event = self._query_event(event_id)
        if event is None:
            raise ValueError('restart-event current event is unavailable')
        with self._watch_phase('internal'):
            callbacks = self._host.matching_callbacks(event)
        return self._event_manifest(callbacks)

    def resume(self) -> GroupSnapshot:
        """Resume durable callback work, stopping on an externally published halt.

        Returns:
            Latest durable snapshot after delivery or a competing halt.
        """
        try:
            return self._resume_guarded()
        except RevisionConflictError:
            current = self._snapshot()
            if current.status == 'running':
                raise
            return current

    def scan_obligation(
        self,
        revision: PendingRange,
        events: Iterable[Event],
        *,
        source_successful_clock: float | None = None,
    ) -> GroupSnapshot:
        """Run one complete post-close range scan through ordinary delivery.

        Scanning consumes event bodies through ordinary delivery and can invoke
        matching plugin callbacks.

        Args:
            revision: Immutable bounds of the closed obligation range.
            events: Ascending source rows from the complete retained range.
            source_successful_clock: Most recently synchronized source clock.
                When supplied, it must be finite, nonnegative, and
                non-regressing for either ordering mode; it is required for
                strict scans. A strict clock before the eligibility point
                returns without consuming `events`.

        Returns:
            Resolved or halted group snapshot.

        Raises:
            ValueError: The range is not closed, a required strict clock is
                missing, a supplied clock is invalid or regresses, or row ids
                are invalid, out of bounds, or not strictly ascending.
        """
        if not revision.closed:
            raise ValueError('post-close scan requires a closed range')
        snapshot = self._snapshot()
        if source_successful_clock is not None:
            synchronized_clock = self._synchronize_source_clock(source_successful_clock)
        elif snapshot.ordering == 'strict':
            raise ValueError('strict post-close scan requires a synchronized source clock')
        else:
            synchronized_clock = None
        if (
            snapshot.ordering == 'strict'
            and synchronized_clock is not None
            and synchronized_clock < revision.eligibility_point
        ):
            return snapshot
        previous_id: int | None = None
        for event in self._internal_items(events):
            event_id = event['id']
            if type(event_id) is not int:
                raise ValueError('post-close scan event id is invalid')
            if not revision.low_id <= event_id <= revision.high_id:
                raise ValueError('post-close scan event is outside retained bounds')
            if previous_id is not None and event_id <= previous_id:
                raise ValueError('post-close scan rows are not strictly ascending')
            previous_id = event_id
            snapshot, consumed = self._deliver_many(
                (Delivery(event, obligation_id=revision.obligation_id),)
            )
            if not consumed or snapshot.status != 'running':
                return snapshot
        return self._complete_scan(revision.obligation_id)

    def setup(self) -> PluginError | None:
        """Run ordered mode-specific setup before delivery or join.

        Returns:
            A structured redacted setup failure, otherwise `None`.
        """
        with self._watch_phase('setup'):
            if self._group.execution == 'async':
                failure = self._async_loop().run_until_complete(self._host.setup_async())
            else:
                failure = self._host.setup_sync()
        self._setup_failure = failure
        self._setup_finished = failure is None
        return failure


async def _invoke_async(callback: CallbackSpec, context: EventContext) -> None:
    """Invoke one asynchronous callback directly in the lane loop."""
    result = callback.callback(context)
    if not inspect.isawaitable(result):
        raise TypeError(f'async callback {callback.id!r} did not return an awaitable')
    await result


def _invoke_sync(callback: CallbackSpec, context: EventContext) -> None:
    """Invoke directly and reject an awaitable result."""
    result = callback.callback(context)
    validate_sync_callback_result(result, callback.id)


def _sleep_wait(delay: float) -> bool:
    """Wait for one non-interrupted fetch interval."""
    time.sleep(delay)
    return False


def _token() -> str:
    """Mint one non-reusable live-join attempt token."""
    return uuid.uuid4().hex
