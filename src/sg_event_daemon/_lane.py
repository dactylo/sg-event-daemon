"""Mode-neutral serial group scheduling and callback execution."""

from __future__ import annotations

import asyncio
import inspect
import threading
import time
import traceback
import uuid
from bisect import bisect_right
from collections import deque
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from itertools import takewhile
from math import isfinite
from pathlib import Path
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
    StrictOrderingControl,
)
from ._group_errors import (
    InvalidTransitionError,
    RevisionConflictError,
    StrictObligationBlockedError,
    TerminalLateEventError,
)
from ._group_types import (
    BeginCallbackStep,
    CallbackDoneStep,
    CompleteLateStep,
    ReserveLateStep,
    StartEventStep,
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
from ._resources import close_callables
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
    LiveChannelDetachedError,
    edge_visible_id,
    history_truncation,
)
from ._strict import JOIN_ABORT_LIMIT
from ._watchdog import (
    ComponentWatchdog,
    internal_stall_deadline_secs,
)
from ._write_lock import WriteLockStoppedError

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable, Iterator, Sequence

    from ._config import GroupConfig, SiteConfig
    from ._gaps import IdRun
    from ._group_state import GroupState, PendingRange
    from ._group_types import (
        GroupSnapshot,
        LiveJoinIdentity,
        PreInvocationResult,
        PreInvocationStep,
    )
    from ._plugins import PluginSpec
    from ._providers import Provider, SyncSG
    from ._watchdog import GroupPhase

_Item = TypeVar('_Item')

_STALE_REPLY_LIMIT = 8


class _JoinState:
    """Mark one exact lane-owned live-join lifecycle state."""


@dataclass(frozen=True)
class _PendingDone:
    """Retain only the identity of a successful, not yet committed callback."""

    ordinal: int
    event_id: int
    revision: int


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
        control: StrictOrderingControl,
    ) -> None:
        """Retire one exact lane-committed strict control.

        Args:
            control: Ordering halt committed by the lane.
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


@dataclass(frozen=True)
class _DetachedJoin(_JoinState):
    """Represent a lane with no live channel ownership."""


@dataclass(frozen=True)
class _FinalizedJoin(_JoinState):
    """Own one exact finalized callback gate and pending messages."""

    identity: LiveJoinIdentity
    channel: GroupChannelInterface
    pending: deque[FanoutMessage]
    source: _JoinSource


@dataclass(frozen=True)
class _Joining(_JoinState):
    """Own one exact provisional attempt and its quarantined messages."""

    attempt: JoinAttemptIdentity
    channel: GroupChannelInterface
    identity: LiveJoinIdentity | None
    pending: deque[FanoutMessage]


@dataclass(frozen=True)
class _SourceFinalizedJoin(_Joining):
    """Own source completion while persistence keeps the callback gate closed."""


class _LaneWatchOptions(TypedDict, total=False):
    """Type optional identities and deadline for one lane phase."""

    deadline_at: float | None
    callback_id: str | None
    delivery_attempt: int | None
    event_id: int | None
    plugin_id: str | None


@dataclass
class _DeliverySlice:
    """Share one consumed-progress allowance across a delivery slice."""

    deadline_at: float
    consumed: bool = False

    def allows(self, now: float) -> bool:
        """Allow the first consumed unit or work before the absolute deadline."""
        return not self.consumed or now < self.deadline_at


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
                positive, or the stored callback manifest is invalid.
            sqlite3.Error: Reading the initial group snapshot fails.
            RuntimeError: Group state is not initialized.
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
            heartbeat=self._heartbeat,
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
        self._pending_done: _PendingDone | None = None
        self._pending_done_attempted = False
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
        owned.pending.clear()
        self._join_state = _DetachedJoin()
        with suppress(BaseException):
            owned.channel.close()

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
        package = Path(__file__).parent
        plugin_frames = [
            frame for frame in frames if not Path(frame.filename).is_relative_to(package)
        ]
        selected = plugin_frames[-1:] or frames[-1:]
        lines = ['Traceback (most recent call last):']
        lines.extend(
            f'  File "{frame.filename}", line {frame.lineno}, in {frame.name}'
            for frame in selected
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

    def _heartbeat(self) -> None:
        """Refresh the current invocation watchdog."""
        with self._state_lock:
            generation = self._active_invocation_generation
        if generation is not None:
            self._heartbeat_invocation(generation)

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

    def _remember(self, snapshot: GroupSnapshot) -> GroupSnapshot:
        """Publish one immutable snapshot to cross-thread health readers."""
        with self._state_lock:
            self._last_snapshot = snapshot
        return snapshot

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

    def request_shutdown(self) -> None:
        """Request cooperative shutdown between serial event deliveries.

        For async delivery, an active async task is cancelled after the
        configured grace period.
        The interrupted callback remains pending for recovery, including when
        it suppresses cancellation.

        Raises:
            RuntimeError: The asynchronous cancellation timer cannot start.
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

        The previous phase and identities are restored on exit, with heartbeat
        and deadline adjustments made by the component watchdog.

        Args:
            phase: Scoped lane phase.
            **options: Optional absolute monotonic deadline and in-flight
                identities. With no deadline, callback and setup phases use the
                callback budget, other non-idle phases use the internal-stall
                budget, and idle has no deadline.

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
        control: StrictOrderingControl,
    ) -> None:
        """Acknowledge one exact strict control after the lane commit."""
        with self._watch_phase('internal'):
            source.acknowledge_strict_control(control)

    def _begin_callback(
        self, steps: tuple[PreInvocationStep, ...], snapshot: GroupSnapshot
    ) -> PreInvocationResult:
        """Commit the prior success and exactly one next attempt before invocation."""
        pending = self._pending_done
        expected_revision = snapshot.revision if pending is None else pending.revision
        if pending is not None:
            steps = (CallbackDoneStep(pending.ordinal), *steps)
            self._pending_done_attempted = True
        with self._watch_phase('internal'):
            result = self._state.commit_pre_invocation(
                steps,
                expected_revision=expected_revision,
            )
        # The store checks this exact starting revision and retains the leading
        # done step across successor rejection. Only this operation's progress
        # can therefore retire the token, never a fresh public-call snapshot.
        if pending is not None and result.snapshot.revision > expected_revision:
            self._pending_done = None
        return result

    def _commit_pending_done(self) -> GroupSnapshot:
        """Commit a successful callback before leaving queued ordinary delivery."""
        pending = self._pending_done
        if pending is None:
            return self._last_snapshot
        self._pending_done_attempted = True
        with self._watch_phase('internal'):
            completed = self._state.callback_done(
                pending.ordinal,
                expected_revision=pending.revision,
            )
        self._pending_done = None
        return self._remember(completed)

    def _complete_callback(self, ordinal: int, snapshot: GroupSnapshot) -> GroupSnapshot:
        """Defer ordinary success only until the next queued attempt or batch tail."""
        if snapshot.in_flight_obligation_id is None:
            self._pending_done = _PendingDone(
                ordinal,
                cast('int', snapshot.in_flight_event_id),
                snapshot.revision,
            )
            self._pending_done_attempted = False
            return snapshot
        with self._watch_phase('internal'):
            completed = self._state.callback_done(ordinal, expected_revision=snapshot.revision)
        return self._remember(completed)

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
        control: StrictOrderingControl,
        snapshot: GroupSnapshot,
    ) -> tuple[GroupSnapshot, bool]:
        """Fence one strict live control and commit a halt when requested."""
        owned = self._join_state
        if (
            not isinstance(owned, _FinalizedJoin)
            or owned.identity != control.identity
            or owned.channel.group != control.identity.group
        ):
            raise JoinProtocolError('strict control targets a stale live identity')
        with self._watch_phase('internal'):
            decided = self._state.record_strict_ordering_decision(
                control,
                expected_revision=snapshot.revision,
            )
        self._acknowledge_strict_control(owned.source, control)
        owned.channel.detach('ordering-anomaly')
        return self._remember(decided), True

    def _consume_live_message(
        self,
        message: RangeChanged | StrictOrderingControl,
        snapshot: GroupSnapshot,
    ) -> tuple[GroupSnapshot, bool]:
        """Consume one pending live message through its durable boundary."""
        if isinstance(message, RangeChanged):
            return snapshot, True
        return self._consume_strict_control(message, snapshot)

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

    @contextmanager
    def _pending_done_boundary(self) -> Generator[None]:
        """Flush success on delivery exit without retrying a failed completion."""
        # Iterator advancement and pure successor preparation belong to this
        # boundary too. A fresh delivery call may retry a retained completion;
        # this call's unwind must not retry a write it already attempted.
        self._pending_done_attempted = False
        try:
            yield
        finally:
            if self._pending_done is not None and not self._pending_done_attempted:
                self._commit_pending_done()

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

    @staticmethod
    def _require_attached(owned: _Joining | _FinalizedJoin) -> None:
        """Report a detach without changing the join owner's settlement state."""
        status = owned.channel.status
        if not status.attached:
            raise LiveChannelDetachedError(cast('str', status.reason))

    @staticmethod
    def _attached(owned: _Joining | _FinalizedJoin) -> bool:
        """Observe attachment where detachment only stops the current work."""
        return owned.channel.status.attached

    def _drain_channel(
        self,
        owned: _Joining | _FinalizedJoin,
    ) -> tuple[FanoutMessage, ...]:
        """Drain one exact owner while preserving any pre-cleanup detach reason."""
        if self._join_state is not owned:
            raise JoinProtocolError('live channel ownership changed')
        self._require_attached(owned)
        with self._watch_phase('internal'):
            messages = owned.channel.drain()
        self._require_attached(owned)
        return messages

    def _drain_attached(
        self,
        joining: _Joining,
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Retain quarantined event bodies until attachment commits."""
        for message in self._drain_channel(joining):
            joining.pending.append(message)
        self._require_attached(joining)
        return snapshot

    def _flush_irrelevant(
        self,
        snapshot: GroupSnapshot,
        high_id: int | None,
        *,
        live_prefix: bool = False,
    ) -> GroupSnapshot:
        """Commit one accumulated irrelevant range, clipping live progress at a fence."""
        if high_id is not None and self._pending_done is not None:
            snapshot = self._commit_pending_done()
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

    def _invoke_callbacks(
        self,
        event: Event,
        callbacks: tuple[tuple[PluginSpec, CallbackSpec], ...],
        snapshot: GroupSnapshot,
        *,
        initial_steps: tuple[PreInvocationStep, ...] = (),
    ) -> GroupSnapshot:
        """Invoke pending callbacks and preserve terminal marks."""
        marks = {} if initial_steps else {mark[0]: mark for mark in snapshot.callback_marks}
        for ordinal, (plugin, callback) in enumerate(callbacks):
            if self._shutdown_requested:
                return self._commit_pending_done()
            mark = marks.get(ordinal)
            if mark is None and not initial_steps:
                raise RuntimeError('durable callback manifest is incomplete')
            if mark is not None and mark[3] != 'pending':
                continue
            result = self._begin_callback((*initial_steps, BeginCallbackStep(ordinal)), snapshot)
            snapshot = self._remember(result.snapshot)
            if result.rejection is not None:
                raise result.rejection
            if 'late-capacity-exhausted' in snapshot.conditions:
                return snapshot
            initial_steps = ()
            marks = {mark[0]: mark for mark in snapshot.callback_marks}
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
            snapshot = self._complete_callback(ordinal, snapshot)
        return snapshot

    def _invoke_event(
        self,
        delivery: Delivery,
        callbacks: tuple[tuple[PluginSpec, CallbackSpec], ...],
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Persist and invoke one matching event in manifest order."""
        event_id = cast('int', delivery.event['id'])
        steps: tuple[PreInvocationStep, ...] = (
            StartEventStep(event_id, self._event_manifest(callbacks), delivery.obligation_id),
        )
        if delivery.obligation_id is not None:
            steps = (ReserveLateStep(delivery.obligation_id, event_id), *steps)
        return self._invoke_callbacks(delivery.event, callbacks, snapshot, initial_steps=steps)

    def _live_deliveries(self, owned: _FinalizedJoin) -> Iterator[Delivery]:
        """Decode an attached event prefix, stopping at detachment or controls."""
        for message in owned.pending:
            if not self._attached(owned) or not isinstance(message, EventDelivery):
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

    def _prepare_live_drain(
        self,
        channel: GroupChannelInterface,
        idle_snapshot: GroupSnapshot | None,
    ) -> tuple[_FinalizedJoin, GroupSnapshot | None]:
        """Validate the live owner and reuse its gate observation only when empty."""
        owned = self._join_state
        if (
            not isinstance(owned, _FinalizedJoin)
            or owned.channel is not channel
            or owned.identity.group != channel.group
        ):
            raise JoinProtocolError('live callback gate is closed')
        self._require_attached(owned)
        if idle_snapshot is None:
            return owned, None
        if not owned.pending:
            owned.pending.extend(self._drain_channel(owned))
        return owned, None if owned.pending else self._remember(idle_snapshot)

    def _retention_floor(self) -> int | None:
        """Read and validate the current lowest visible event id."""
        if self._refetch_handle is None:
            raise RuntimeError('refetch handle is not open')
        with self._watch_phase('internal'):
            return edge_visible_id(self._refetch_handle, 'asc')

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
        try:
            if callbacks:
                return self._invoke_event(delivery, callbacks, snapshot)
            with self._watch_phase('internal'):
                result = self._state.commit_pre_invocation(
                    (
                        ReserveLateStep(obligation_id, event_id),
                        CompleteLateStep(obligation_id, event_id),
                    ),
                    expected_revision=snapshot.revision,
                )
            snapshot = self._remember(result.snapshot)
            if result.rejection is not None:
                raise result.rejection
        except TerminalLateEventError:
            return self._snapshot()
        else:
            return snapshot

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
        primary_error: BaseException | None = None,
    ) -> None:
        """Abort one exact source attempt and discard its queued bodies."""
        if self._join_state is not joining:
            return
        try:
            if joining.identity is not None:
                with self._watch_phase('internal'):
                    try:
                        source.abort(JoinAbort(joining.identity))
                    except (ConnectionError, JoinProtocolError):
                        if primary_error is None:
                            raise
        finally:
            try:
                self._record_strict_join_outcome(
                    joining.attempt,
                    capacity_caused=capacity_caused,
                )
            finally:
                self._detach_join(joining)

    def _cleanup_join(
        self,
        source: _JoinSource,
        joining: _Joining | _FinalizedJoin,
        error: BaseException,
    ) -> None:
        """Settle one failed attempt without hiding an outcome-write failure."""
        if self._join_state is not joining:
            return
        if isinstance(joining, (_SourceFinalizedJoin, _FinalizedJoin)) or isinstance(
            error, WriteLockStoppedError
        ):
            # Finalized owners never repeat settlement; stopped writes likewise
            # require local teardown without another RPC or state write.
            self._detach_join(joining)
            return
        self._abort_join(
            source,
            joining,
            capacity_caused=(
                isinstance(error, JoinCapacityError)
                or (
                    isinstance(error, LiveChannelDetachedError)
                    and error.reason == 'strict-buffer-capacity'
                )
            ),
            primary_error=error,
        )

    def _promote_join(
        self,
        source: _JoinSource,
        joining: _Joining,
        identity: LiveJoinIdentity,
    ) -> None:
        """Settle one source-finalized owner before opening its callback gate."""
        if self._join_state is not joining:
            raise JoinProtocolError('live channel ownership changed')
        finalized = _SourceFinalizedJoin(
            attempt=joining.attempt,
            identity=identity,
            channel=joining.channel,
            pending=joining.pending,
        )
        self._join_state = finalized
        try:
            self._record_strict_join_outcome(joining.attempt, capacity_caused=False)
            self._require_attached(finalized)
        except BaseException as error:
            self._cleanup_join(source, finalized, error)
            raise
        self._join_state = _FinalizedJoin(
            identity=identity,
            channel=finalized.channel,
            pending=finalized.pending,
            source=source,
        )

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
        """Halt decreasing strict delivery or consume an event below its floor."""
        if snapshot.ordering != 'strict':
            return snapshot, False
        if event_id <= snapshot.delivery_floor:
            return snapshot, True
        if event_id < snapshot.cursor:
            with self._watch_phase('internal'):
                halted = self._state.mark_ordering_anomaly(
                    event_id,
                    expected_revision=snapshot.revision,
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
        coverage: tuple[tuple[str, int, int], ...] | None = None,
        live_prefix: bool = False,
    ) -> tuple[GroupSnapshot, int | None]:
        """Apply one delivery through strict, late, or ordinary ownership."""
        event_id = delivery.event['id']
        if type(event_id) is not int or event_id <= 0:
            raise ValueError('delivery event id must be a positive integer')
        pending = self._pending_done
        if pending is not None and (
            event_id <= pending.event_id
            or delivery.obligation_id is not None
            or irrelevant_high is not None
        ):
            snapshot = self._commit_pending_done()
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
        if coverage is not None and delivery.obligation_id is None:
            # The pending obligation covering an id owns it on every path. An uncovered id
            # at or below this batch's position was already processed or discarded.
            index = bisect_right(coverage, event_id, key=lambda bounds: bounds[1]) - 1
            if index >= 0 and event_id <= coverage[index][2]:
                delivery = replace(delivery, obligation_id=coverage[index][0])
            elif event_id <= max(snapshot.cursor, irrelevant_high or snapshot.cursor):
                return snapshot, irrelevant_high
        with self._watch_phase('internal'):
            callbacks = self._host.matching_callbacks(delivery.event)
        if delivery.obligation_id is not None:
            snapshot = self._commit_pending_done()
            snapshot = self._flush_irrelevant(snapshot, irrelevant_high, live_prefix=live_prefix)
            return self._invoke_late(delivery, callbacks, snapshot), None
        if not callbacks:
            return self._commit_pending_done(), event_id
        snapshot = self._flush_irrelevant(snapshot, irrelevant_high, live_prefix=live_prefix)
        return self._invoke_event(delivery, callbacks, snapshot), None

    def _deliver_many_guarded(
        self,
        deliveries: Iterable[Delivery],
        snapshot: GroupSnapshot | None = None,
        *,
        live_ids: list[int] | None = None,
        delivery_slice: _DeliverySlice,
    ) -> tuple[GroupSnapshot, int]:
        """Deliver events and report the exact consumed prefix length."""
        if not self._setup_finished:
            raise RuntimeError('lane setup has not completed')
        if snapshot is None:
            snapshot = self._snapshot()
        if snapshot.status != 'running':
            return snapshot, 0
        coverage: tuple[tuple[str, int, int], ...] | None = None
        irrelevant_high: int | None = None
        live_prefix = live_ids is not None
        consumed = 0
        iterator = self._internal_items(deliveries)
        while delivery_slice.allows(self._clock()):
            try:
                delivery = next(iterator)
            except StopIteration:
                break
            if self._shutdown_requested:
                return self._flush_irrelevant(
                    snapshot, irrelevant_high, live_prefix=live_prefix
                ), consumed
            if (
                coverage is None
                and snapshot.ordering == 'eager'
                and delivery.obligation_id is None
            ):
                # Read after the batch's rows are obtained; never skip it on a cached count.
                with self._watch_phase('internal'):
                    coverage = self._state.pending_obligation_bounds()
            snapshot, irrelevant_high = self._deliver_one(
                delivery,
                snapshot,
                irrelevant_high,
                coverage=coverage,
                live_prefix=live_prefix,
            )
            consumed += 1
            delivery_slice.consumed = True
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
        delivery_slice: _DeliverySlice | None = None,
    ) -> tuple[GroupSnapshot, int]:
        """Stop a delivery batch when an external collapse invalidates its revision."""
        live_ids: list[int] | None = [] if live_prefix else None
        if delivery_slice is None:
            delivery_slice = _DeliverySlice(float('inf'))
        try:
            with self._pending_done_boundary():
                result, consumed = self._deliver_many_guarded(
                    deliveries, snapshot, live_ids=live_ids, delivery_slice=delivery_slice
                )
                if self._pending_done is not None:
                    result = self._commit_pending_done()
        except StrictObligationBlockedError:
            if self._pending_done is not None or live_ids is None:
                raise
            current = self._snapshot()
            committed = sum(
                1 for _ in takewhile(lambda event_id: event_id <= current.cursor, live_ids)
            )
            return current, committed
        except RevisionConflictError:
            if self._pending_done is not None:
                raise
            current = self._snapshot()
            if current.status == 'running':
                raise
            return current, 0
        else:
            return result, consumed

    def close(self) -> None:
        """Close every owned resource, preserving the first cleanup failure.

        Later failures are attached as notes after the refetch handle, plugin
        host, and async event loop have each had a cleanup attempt.

        Raises:
            PluginError: Plugin-handle teardown reports an ordinary failure.
            Exception: Closing the supplied refetch handle or async event-loop
                resources fails.
        """
        sample = self._watchdog.sample()
        deadline_at = (
            sample.deadline_at
            if sample.phase == 'shutdown'
            else self._clock() + self._internal_stall_deadline_secs
        )

        def close_refetch() -> None:
            handle = self._refetch_handle
            try:
                if handle is not None:
                    handle.close()
            finally:
                self._refetch_handle = None

        def close_host() -> None:
            if self._group.execution != 'async' or self._loop is None:
                self._host.close_sync()
                return
            loop = self._async_loop()
            with self._async_lock:
                self._async_loop_closing = True
                self._clear_async_cancel_timer_locked()
                try:
                    close_callables(
                        (lambda: loop.run_until_complete(self._host.close_async()), loop.close)
                    )
                finally:
                    self._loop = None
                    self._loop_thread_ident = None

        with self.watch_phase('shutdown', deadline_at=deadline_at):
            close_callables((close_refetch, close_host))

    def complete_obligation(
        self, obligation_id: str, *, excused: Sequence[IdRun] = ()
    ) -> GroupSnapshot:
        """Commit completion unless a competing collapse has halted delivery.

        Args:
            obligation_id: Closed pending range whose source-listed rows are
                terminal or explicitly given up.
            excused: Ascending runs of the exact unfinished IDs given up.

        Returns:
            Completed snapshot, or the current snapshot if delivery halted.

        Raises:
            InvalidTransitionError: The obligation is not closed and pending,
                or unfinished IDs differ from the excused set.
            RevisionConflictError: Running group state changed before
                completion.
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            sqlite3.Error: Reading durable group state outside a transaction
                fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
            ValueError: The stored callback manifest is invalid.
        """
        try:
            with self._watch_phase('internal'):
                completed = self._state.complete_obligation_scan(obligation_id, excused=excused)
        except RevisionConflictError:
            current = self._snapshot()
            if current.status == 'running':
                raise
            return current
        return self._remember(completed)

    def deliver_many_counted(
        self, deliveries: Iterable[Delivery], *, deadline_at: float = float('inf')
    ) -> tuple[GroupSnapshot, int]:
        """Deliver one page and return its durable snapshot and consumed prefix length.

        Args:
            deliveries: Ordered catch-up event bodies from one provider page.
            deadline_at: Absolute monotonic deadline, enforced after the first
                consumed delivery. Omitted for unbounded delivery.

        Returns:
            Latest snapshot and the number of deliveries offered before a halt,
            cooperative shutdown, or slice deadline.

        Raises:
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            sqlite3.Error: Reading durable group state outside a transaction
                fails.
            RuntimeError: Setup has not completed or the file-lock acquisition
                helper thread cannot start.
            ValueError: The stored callback manifest or delivery event data is
                invalid.
            InvalidTransitionError: Durable callback or strict-obligation state
                rejects delivery.
            RevisionConflictError: Durable state changes while completion is
                pending or the group remains running.
        """
        return self._deliver_many(deliveries, delivery_slice=_DeliverySlice(deadline_at))

    def deliver_obligation_rows(
        self,
        revision: PendingRange,
        events: Iterable[Event],
        *,
        source_successful_clock: float,
    ) -> tuple[GroupSnapshot, bool]:
        """Deliver one closed obligation's rows without completing the obligation.

        Args:
            revision: Immutable bounds of the closed obligation range.
            events: Strictly ascending rows within those bounds.
            source_successful_clock: Finite, nonnegative, non-regressing source
                clock.

        Returns:
            Current snapshot and whether every supplied row was consumed while
            running without a shutdown request.

        Raises:
            ValueError: The range is open, the clock is invalid or regresses,
                the row IDs are invalid, out of bounds, or not ascending, or
                the stored callback manifest is invalid.
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            sqlite3.Error: Reading durable group state outside a transaction
                fails.
            RuntimeError: Setup has not completed or the file-lock acquisition
                helper thread cannot start.
            InvalidTransitionError: Durable callback or obligation state
                rejects delivery.
            RevisionConflictError: Durable state changes while completion is
                pending or the group remains running.
        """
        if not revision.closed:
            raise ValueError('post-close scan requires a closed range')
        self._synchronize_source_clock(source_successful_clock)
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
            if not consumed or snapshot.status != 'running' or self.shutdown_requested():
                return snapshot, False
        snapshot = self._snapshot()
        return snapshot, snapshot.status == 'running' and not self.shutdown_requested()

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
            deadline_at: Absolute monotonic deadline enforced after the first
                consumed event or control. An active callback finishes before
                returning at the deadline.
            idle_snapshot: Optional callback-gate observation from this owner
                iteration, after commands and without intervening setup,
                resume, or join work. Used only if the channel is empty;
                queued delivery always reads fresh durable state.

        Returns:
            Immutable durable snapshot for this iteration, freshly read before
            queued delivery or taken at the callback gate for an empty drain.

        Raises:
            JoinProtocolError: The callback gate is closed, a live control
                identity conflicts, or the finalized channel detaches.
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            sqlite3.Error: Reading durable group state outside a transaction
                fails.
            RuntimeError: Setup has not completed or the file-lock acquisition
                helper thread cannot start.
            ValueError: The stored callback manifest or delivery event data is
                invalid.
            InvalidTransitionError: Durable callback or obligation state
                rejects delivery.
            RevisionConflictError: Durable state changes while completion is
                pending or the group remains running.
        """
        # Attachment can fail only after _prepare_live_drain validates this owner.
        owned = cast('_FinalizedJoin', self._join_state)
        try:
            owned, snapshot = self._prepare_live_drain(channel, idle_snapshot)
            if snapshot is not None:
                return snapshot
            delivery_slice = _DeliverySlice(deadline_at)
            snapshot = self._snapshot()
            while delivery_slice.allows(self._clock()) and snapshot.status == 'running':
                self._require_attached(owned)
                if not owned.pending:
                    owned.pending.extend(self._drain_channel(owned))
                if not owned.pending or self._shutdown_requested:
                    return snapshot
                message = owned.pending[0]
                if isinstance(message, EventDelivery):
                    snapshot, consumed = self._deliver_many(
                        self._live_deliveries(owned),
                        snapshot,
                        live_prefix=snapshot.ordering == 'strict',
                        delivery_slice=delivery_slice,
                    )
                else:
                    snapshot, accepted = self._consume_live_message(message, snapshot)
                    consumed = int(accepted)
                    delivery_slice.consumed |= accepted
                for _ in range(consumed):
                    owned.pending.popleft()
                if snapshot.status != 'running' and not self._attached(owned):
                    self._detach_join(owned)
                if snapshot.status != 'running' or (not consumed and self._attached(owned)):
                    return snapshot
        except LiveChannelDetachedError:
            self._detach_join(owned)
            raise
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
            RuntimeError: Setup has not completed or the file-lock acquisition
                helper thread cannot start.
            WriteLockStoppedError: Owner write acquisition was stopped.
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            sqlite3.Error: Reading durable group state outside a transaction
                fails.
            ValueError: The stored callback manifest or delivery event data is
                invalid.
            Exception: The supplied catch-up driver, source coordination, or
                join-cleanup operation fails.
        """
        if not self._setup_finished:
            raise RuntimeError('lane setup has not completed')
        owned = self._join_state
        if isinstance(owned, _FinalizedJoin):
            if owned.channel.status.attached:
                raise JoinProtocolError('live join is already finalized')
            self._detach_join(owned)
        elif isinstance(owned, _Joining):
            raise JoinProtocolError('live join attempt is already active')
        snapshot = self._snapshot()
        attempt = self._mint_attempt(snapshot)
        joining = _Joining(
            attempt=attempt,
            channel=channel,
            identity=None,
            pending=deque(),
        )
        self._join_state = joining
        request = JoinRequest(attempt)
        try:
            response = self._request_join(source, request, channel)
            if isinstance(response, JoinRetry):
                retry = self._join_retry(response, attempt)
                try:
                    self._record_strict_join_outcome(
                        attempt,
                        capacity_caused=snapshot.ordering == 'strict',
                    )
                finally:
                    self._detach_join(joining)
                return retry
            identity = self._join_identity(attempt, response, snapshot)
            joining = replace(joining, identity=identity)
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
            self._finalize_join(source, identity)
            self._promote_join(source, joining, identity)
        except BaseException as error:
            self._cleanup_join(source, joining, error)
            raise
        else:
            return identity

    def queued_event_ids(self, low_id: int, high_id: int) -> frozenset[int]:
        """Retain queued messages in order and report live event IDs within the bounds.

        Returns:
            Matching IDs for a finalized join, otherwise an empty set.

        Raises:
            JoinProtocolError: The finalized channel detaches during the drain.
        """
        owned = self._join_state
        if not isinstance(owned, _FinalizedJoin):
            return frozenset()
        owned.pending.extend(self._drain_channel(owned))
        return frozenset(
            message.encoded.event_id
            for message in owned.pending
            if isinstance(message, EventDelivery) and low_id <= message.encoded.event_id <= high_id
        )

    def restart_event_manifest(self, event_id: int) -> CallbackManifest:
        """Return the active event-local callback manifest for a restart.

        Args:
            event_id: Exact in-flight event to refetch and match.

        Returns:
            Current ordered identities for callbacks matching the event.

        Raises:
            ValueError: The current event is unavailable, malformed, or does
                not match the requested exact ID.
            Exception: Creating the supplied provider handle or querying the
                exact event fails.
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

        Raises:
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            sqlite3.Error: Reading durable group state outside a transaction
                fails.
            RuntimeError: Setup has not completed or the file-lock acquisition
                helper thread cannot start.
            ValueError: The stored callback manifest or delivery event data is
                invalid.
            RevisionConflictError: Durable state changes while completion is
                pending or the group remains running.
            InvalidTransitionError: Durable callback or required-event state
                rejects recovery.
            InterruptedError: Cooperative interruption stops a required-event
                refetch wait.
            Exception: Creating the supplied provider handle or refetching the
                event fails.
        """
        try:
            with self._pending_done_boundary():
                resumed = self._resume_guarded()
                return self._commit_pending_done() if self._pending_done is not None else resumed
        except RevisionConflictError:
            if self._pending_done is not None:
                raise
            current = self._snapshot()
            if current.status == 'running':
                raise
            return current

    def setup(self) -> PluginError | None:
        """Run ordered mode-specific setup before delivery or join.

        Returns:
            A structured redacted setup failure, otherwise `None`.

        Raises:
            OSError: Creating the asynchronous event loop fails.
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
