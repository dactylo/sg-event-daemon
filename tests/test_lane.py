from __future__ import annotations

import asyncio
import gc
import hashlib
import inspect
import threading
import time
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack, cast

import pytest

from sg_event_daemon._config import (
    CredentialConfig,
    GroupConfig,
    SecretValue,
    SiteConfig,
    SourceConfig,
)
from sg_event_daemon._events import Event, encode_event, normalize_filter
from sg_event_daemon._fanout import (
    EncodedEvent,
    EventDelivery,
    EventFanout,
    GroupChannel,
    GroupChannelInterface,
    StrictCapacityControl,
    StrictOrderingControl,
)
from sg_event_daemon._group_errors import InvalidTransitionError
from sg_event_daemon._group_recovery import DispositionRequest
from sg_event_daemon._group_types import (
    AuditIdentity,
    LiveJoinIdentity,
)
from sg_event_daemon._lane import (
    Delivery,
    GroupLane,
    LaneHealth,
    _FinalizedJoin,
    _invoke_sync,
)
from sg_event_daemon._plugins import (
    CallbackIdentity,
    CallbackManifest,
    CallbackSpec,
    PluginSpec,
)
from sg_event_daemon._providers import Provider, ProviderCapabilities
from sg_event_daemon._source import (
    Attached,
    JoinAbort,
    JoinAttemptIdentity,
    JoinCapacityError,
    JoinConfirm,
    JoinCoordinator,
    JoinFinalize,
    JoinFinalized,
    JoinProtocolError,
    JoinRequest,
    JoinRetry,
    JoinTarget,
)
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._strict import JOIN_ABORT_LIMIT, OrderingAnomaly, StrictBuffer
from sg_event_daemon._watchdog import ComponentWatchdog, internal_stall_deadline_secs
from sg_event_daemon._write_lock import WriteLockStoppedError, WriteLockTimeoutError
from tests._storage_support import (
    bind_group,
    group_exists,
    insert_range,
    insert_source_late_ids,
    open_group_on_source,
    write_context_for,
)
from tests.source.test_state import record_source_late_ids
from tests.test_strict import attach_strict_group

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine, Generator, Iterator
    from pathlib import Path

    from sg_event_daemon._group_state import GroupOpenOptions, GroupState, PendingRange
    from sg_event_daemon._group_types import GroupSnapshot
    from sg_event_daemon._plugins import EventContext, SetupContext

_ATTEMPT_1 = hashlib.sha256(b'attempt-1').hexdigest()
_ATTEMPT_2 = hashlib.sha256(b'attempt-2').hexdigest()
_ATTEMPT_3 = hashlib.sha256(b'attempt-3').hexdigest()
_ATTEMPT_ACCEPTED = hashlib.sha256(b'attempt-accepted').hexdigest()
_ATTEMPT_CURRENT = hashlib.sha256(b'attempt-current').hexdigest()
_ATTEMPT_OLD = hashlib.sha256(b'attempt-old').hexdigest()
_ATTEMPT_REJECTED = hashlib.sha256(b'attempt-rejected').hexdigest()
_ATTEMPT_RESTARTED = hashlib.sha256(b'attempt-restarted').hexdigest()
_WATCHDOG_IDENTITY = 'registration-1'


class _AsyncCloseRaceState:
    def __init__(self) -> None:
        self.cleanup_requested = threading.Event()
        self._factories: list[_HandleFactory] = []
        self._holder: list[GroupLane] = []
        self._lane_finished = threading.Event()
        self._snapshots: list[GroupSnapshot] = []


@dataclass
class _Backend:
    _events: dict[int, Event]
    _exact_reads: dict[int, list[Event | None]]
    _retention_floor: int | None = None


class _AsyncHandle:
    def __init__(
        self,
        _site: str,
        *,
        _script_name: str,
        _api_key: str,
        _connect: bool,
        backend: _Backend,
        cleanup_attempts: list[int],
        close_observations: list[tuple[int, bool]],
        ordinal: int,
    ) -> None:
        self._backend = backend
        self._closed = False
        self._close_error: BaseException | None = None
        self._cleanup_attempts = cleanup_attempts
        self._close_observations = close_observations
        self.config = _HandleConfig()
        self._created_in = threading.get_ident()
        self._created_loop = asyncio.get_running_loop()
        self._find_calls: list[tuple[str, object]] = []
        self._invocation_loops: list[asyncio.AbstractEventLoop] = []
        self._ordinal = ordinal
        self._session_uuids: list[str | None] = []

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        self._cleanup_attempts.append(self._ordinal)
        self._invocation_loops.append(loop)
        self._close_observations.append((self._ordinal, loop.is_closed()))
        if self._close_error is not None:
            raise self._close_error
        self._closed = True

    async def find(
        self,
        entity_type: str,
        filters: object,
        _fields: list[str] | None = None,
        **_kwargs: object,
    ) -> list[dict[str, object]]:
        self._invocation_loops.append(asyncio.get_running_loop())
        self._find_calls.append((entity_type, filters))
        if filters == []:
            if self._backend._retention_floor is None:
                return []
            return [{'id': self._backend._retention_floor}]
        event_id = _exact_filter_id(filters)
        if event_id is None:
            return []
        scripted = self._backend._exact_reads.get(event_id)
        event = scripted.pop(0) if scripted else self._backend._events.get(event_id)
        return [] if event is None else [dict(event)]

    def set_session_uuid(self, session_uuid: str | None) -> None:
        self._invocation_loops.append(asyncio.get_running_loop())
        self._session_uuids.append(session_uuid)


class _Coordinator:
    def __init__(
        self,
        *,
        boundary: int,
        source_generation: str = 'source-1',
    ) -> None:
        self._aborts: list[JoinAbort] = []
        self._control_acks: list[StrictCapacityControl | StrictOrderingControl] = []
        self._boundary = boundary
        self._confirms: list[JoinConfirm] = []
        self._finalizes: list[JoinFinalize] = []
        self._on_confirm: Callable[[JoinConfirm], Attached | JoinRetry] | None = None
        self._on_finalize: Callable[[JoinFinalize], JoinFinalized] | None = None
        self._on_request: (
            Callable[[JoinRequest, GroupChannelInterface], JoinTarget | JoinRetry] | None
        ) = None
        self._requests: list[JoinRequest] = []
        self._source_generation = source_generation

    def _identity(self, attempt: JoinAttemptIdentity) -> LiveJoinIdentity:
        return LiveJoinIdentity(
            site=attempt.site,
            source=attempt.source,
            group=attempt.group,
            join_kind=attempt.join_kind,
            source_generation=self._source_generation,
            group_generation=attempt.group_generation,
            boundary=self._boundary,
            attempt_token=attempt.attempt_token,
        )

    def abort(self, abort: JoinAbort) -> None:
        self._aborts.append(abort)

    def acknowledge_strict_control(
        self,
        control: StrictCapacityControl | StrictOrderingControl,
    ) -> None:
        self._control_acks.append(control)

    def confirm(self, confirmation: JoinConfirm) -> Attached | JoinRetry:
        self._confirms.append(confirmation)
        if self._on_confirm is not None:
            return self._on_confirm(confirmation)
        return Attached(confirmation.identity)

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        self._finalizes.append(finalization)
        if self._on_finalize is not None:
            return self._on_finalize(finalization)
        return JoinFinalized(finalization.identity)

    def request(
        self,
        request: JoinRequest,
        channel: GroupChannelInterface,
    ) -> JoinTarget | JoinRetry:
        self._requests.append(request)
        if self._on_request is not None:
            return self._on_request(request, channel)
        channel.activate(provisional=True)
        return JoinTarget(self._identity(request.attempt))


class _Handle:
    def __init__(
        self,
        _site: str,
        *,
        _script_name: str,
        _api_key: str,
        _connect: bool,
        backend: _Backend,
        cleanup_attempts: list[int],
        ordinal: int,
    ) -> None:
        self._backend = backend
        self._closed = False
        self._close_error: BaseException | None = None
        self._cleanup_attempts = cleanup_attempts
        self.config = _HandleConfig()
        self._created_in = threading.get_ident()
        self._find_calls: list[tuple[str, object]] = []
        self._ordinal = ordinal
        self._session_uuids: list[str | None] = []

    def close(self) -> None:
        self._cleanup_attempts.append(self._ordinal)
        if self._close_error is not None:
            raise self._close_error
        self._closed = True

    def find(
        self,
        entity_type: str,
        filters: object,
        _fields: list[str] | None = None,
        **_kwargs: object,
    ) -> list[dict[str, object]]:
        self._find_calls.append((entity_type, filters))
        if filters == []:
            if self._backend._retention_floor is None:
                return []
            return [{'id': self._backend._retention_floor}]
        event_id = _exact_filter_id(filters)
        if event_id is None:
            return []
        scripted = self._backend._exact_reads.get(event_id)
        event = scripted.pop(0) if scripted else self._backend._events.get(event_id)
        return [] if event is None else [dict(event)]

    def set_session_uuid(self, session_uuid: str | None) -> None:
        self._session_uuids.append(session_uuid)


class _HandleFactory:
    def __init__(self, backend: _Backend) -> None:
        self._backend = backend
        self._cleanup_attempts: list[int] = []
        self._close_observations: list[tuple[int, bool]] = []
        self._handles: list[_Handle | _AsyncHandle] = []

    def __call__(
        self,
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _Handle:
        handle = _Handle(
            site,
            _script_name=script_name,
            _api_key=api_key,
            _connect=connect,
            backend=self._backend,
            cleanup_attempts=self._cleanup_attempts,
            ordinal=len(self._handles),
        )
        self._handles.append(handle)
        return handle

    def create_async(
        self,
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _AsyncHandle:
        ordinal = sum(isinstance(handle, _AsyncHandle) for handle in self._handles)
        handle = _AsyncHandle(
            site,
            _script_name=script_name,
            _api_key=api_key,
            _connect=connect,
            backend=self._backend,
            cleanup_attempts=self._cleanup_attempts,
            close_observations=self._close_observations,
            ordinal=ordinal,
        )
        self._handles.append(handle)
        return handle


@dataclass(frozen=True)
class _LaneThreadCleanup:
    holder: list[GroupLane]
    cleanup_requested: threading.Event
    blockers: tuple[threading.Event, ...] = ()
    futures: list[asyncio.Future[None]] | None = None
    other_threads: tuple[threading.Thread | None, ...] = ()


class _CallbackOptions(TypedDict):
    event_type: NotRequired[str | None]
    execution: NotRequired[Literal['sync', 'async']]
    wrap_async: NotRequired[bool]


@dataclass
class _Clock:
    _now: float = 0.0

    def __call__(self) -> float:
        return self._now

    def _advance(self, seconds: float) -> None:
        self._now += seconds


@dataclass(frozen=True)
class _DeadlineCase:
    _health_interval_secs: float
    _timeout_secs: float
    _attempts: int
    _retry_interval_secs: float
    _expected: float


@dataclass
class _HandleConfig:
    max_rpc_attempts: int = 0
    rpc_attempt_interval: int = 0
    timeout_secs: float = 0.0


class _LaneOptions(TypedDict):
    async_cancel_grace_secs: NotRequired[float]
    backend: NotRequired[_Backend | None]
    callback_deadline_secs: NotRequired[float]
    clock: NotRequired[Callable[[], float] | None]
    credential_attempts: NotRequired[int]
    credential_interval_secs: NotRequired[float]
    credential_timeout_secs: NotRequired[float]
    execution: NotRequired[Literal['sync', 'async']]
    extra_plugin_count: NotRequired[int]
    health_interval_secs: NotRequired[float]
    ordering: NotRequired[Literal['eager', 'strict']]
    setup: NotRequired[Callable[[SetupContext], object] | None]
    token_factory: NotRequired[Callable[[], str] | None]
    wait: NotRequired[Callable[[float], bool] | None]
    watchdog: NotRequired[ComponentWatchdog]


class _OperatorOptions(TypedDict):
    reason: str
    identity: AuditIdentity
    wall_clock_time: float
    operator_label: NotRequired[str | None]


class _StrictSourceOptions(TypedDict, total=False):
    delivery_floors: dict[str, int]
    group_holdbacks: dict[str, float]


def _callback(
    ordinal: int,
    callback_id: str,
    callback: Callable[[EventContext], object],
    **options: Unpack[_CallbackOptions],
) -> CallbackSpec:
    event_type = options.get('event_type')
    execution = options.get('execution', 'sync')
    wrap_async = options.get('wrap_async', True)
    configured_callback = callback
    if execution == 'async' and wrap_async:

        async def invoke(context: EventContext) -> object:
            result = callback(context)
            if inspect.isawaitable(result):
                return await result
            return result

        configured_callback = invoke
    return CallbackSpec(
        id=callback_id,
        callback=configured_callback,
        events=None if event_type is None else normalize_filter({event_type: None}),
        identity=CallbackIdentity(
            ordinal=ordinal,
            plugin_id='plugin',
            callback_id=callback_id,
            execution=execution,
            filter_digest=f'filter-{callback_id}',
            config_digest='config',
            secret_refs=(),
            credential_name='callback',
            credential_fingerprint='credential',
        ),
    )


def _context_event_id(context: EventContext) -> int:
    event_id = context.event['id']
    if type(event_id) is not int:
        raise AssertionError('test event id is not an integer')
    return event_id


def _event(
    event_id: int,
    *,
    event_type: str = 'Shotgun_Test_Change',
    session_uuid: str | None = None,
) -> Event:
    return Event(
        {
            'id': event_id,
            'type': 'EventLogEntry',
            'event_type': event_type,
            'attribute_name': None,
            'meta': {},
            'entity': None,
            'user': None,
            'project': None,
            'session_uuid': session_uuid,
            'created_at': datetime(2026, 7, 27, tzinfo=UTC),
        }
    )


def _exact_filter_id(filters: object) -> int | None:
    if not isinstance(filters, list):
        return None
    filter_items = cast('list[object]', filters)
    if len(filter_items) != 1:
        return None
    clause = filter_items[0]
    if not isinstance(clause, list):
        return None
    clause_items = cast('list[object]', clause)
    if (
        len(clause_items) != 3
        or clause_items[0] != 'id'
        or clause_items[1] != 'is'
        or type(clause_items[2]) is not int
    ):
        return None
    return clause_items[2]


def _group_state(
    path: Path,
    *,
    initial_position: int = 10,
    max_failure_text_bytes: int = 262_144,
    max_late_ids_per_group: int = 50_000,
    ordering: Literal['eager', 'strict'] = 'eager',
    **source_options: Unpack[_StrictSourceOptions],
) -> GroupState:
    options: GroupOpenOptions = {
        'writes': write_context_for(path),
        'site': 'alpha',
        'site_server_url': 'https://alpha.example.com',
        'source': 'events',
        'group': 'serial',
        'group_generation': 1,
        'ordering': ordering,
        'stream_fingerprint': 'stream',
        'source_generation': 'source-1',
        'verified_incarnation': 'incarnation',
        'start_mode': 'resume' if group_exists(path, 'serial') else 'initialize',
        'max_failure_text_bytes': max_failure_text_bytes,
        'max_late_ids_per_group': max_late_ids_per_group,
    }
    holdbacks = source_options.get('group_holdbacks', {})
    if 'serial' in holdbacks:
        options['strict_holdback_secs'] = holdbacks['serial']
    if options['start_mode'] == 'initialize':
        options['initial_cursor'] = initial_position
        options['initial_delivery_floor'] = source_options.get('delivery_floors', {}).get(
            'serial', initial_position
        )
        options['initial_last_delivered_id'] = initial_position
    return open_group_on_source(path, **options)


@contextmanager
def _concrete_strict_states(
    source_path: Path,
    *,
    initial_position: int,
    source_initial: int | None,
    **source_options: Unpack[_StrictSourceOptions],
) -> Generator[tuple[SourceState, GroupState]]:
    with (
        SourceState.open(
            source_path,
            writes=write_context_for(source_path),
            create=True,
            site='alpha',
            source='events',
            stream_fingerprint='stream',
            source_generation='source-1',
        ) as source_state,
        _group_state(
            source_path,
            initial_position=initial_position,
            ordering='strict',
            **source_options,
        ) as group_state,
    ):
        source_state.initialize(source_initial, incarnation='incarnation')
        yield source_state, group_state


def _lane(
    state: GroupState,
    callbacks: tuple[CallbackSpec, ...],
    **options: Unpack[_LaneOptions],
) -> tuple[GroupLane, _HandleFactory]:
    async_cancel_grace_secs = options.get('async_cancel_grace_secs', 5.0)
    backend = options.get('backend')
    callback_deadline_secs = options.get('callback_deadline_secs', 120.0)
    clock = options.get('clock')
    credential_attempts = options.get('credential_attempts', 3)
    credential_interval_secs = options.get('credential_interval_secs', 2.0)
    credential_timeout_secs = options.get('credential_timeout_secs', 4.0)
    execution = options.get('execution', 'sync')
    extra_plugin_count = options.get('extra_plugin_count', 0)
    health_interval_secs = options.get('health_interval_secs', 2.0)
    ordering = options.get('ordering', 'eager')
    setup = options.get('setup')
    token_factory = options.get('token_factory')
    wait = options.get('wait')
    watchdog = options.get('watchdog')
    credential = CredentialConfig(
        name='callback',
        script_name='daemon',
        api_key=SecretValue('api-key'),
        timeout_secs=credential_timeout_secs,
        max_rpc_attempts=credential_attempts,
        rpc_attempt_interval_secs=credential_interval_secs,
    )
    group = GroupConfig(
        name='serial',
        source='events',
        plugins=(),
        callback_credentials='callback',
        callback_deadline_secs=callback_deadline_secs,
        execution=execution,
        ordering=ordering,
    )
    site = SiteConfig(
        name='alpha',
        server='https://alpha.example.com',
        default_source='events',
        credentials=MappingProxyType({'callback': credential}),
        sources=MappingProxyType(
            {
                'events': SourceConfig(
                    name='events',
                    credentials='callback',
                    fetch_interval_secs=1.0,
                )
            }
        ),
        groups=MappingProxyType({'serial': group}),
        workers=MappingProxyType({}),
    )
    configured_setup = setup
    if execution == 'async' and setup is not None:

        async def invoke_setup(context: SetupContext) -> object:
            result = setup(context)
            if inspect.isawaitable(result):
                return await result
            return result

        configured_setup = invoke_setup
    plugin = PluginSpec(
        id='plugin',
        module_name='tests.plugin',
        credentials='callback',
        config=MappingProxyType({}),
        secrets=(),
        setup=configured_setup,
        callbacks=callbacks,
    )
    plugins = (
        plugin,
        *(
            PluginSpec(
                id=f'plugin-{index + 2}',
                module_name=f'tests.plugin_{index + 2}',
                credentials='callback',
                config=MappingProxyType({}),
                secrets=(),
                setup=None,
                callbacks=(),
            )
            for index in range(extra_plugin_count)
        ),
    )
    factory = _HandleFactory(_Backend(_events={}, _exact_reads={}) if backend is None else backend)
    provider = Provider(
        name='sg-python-api' if execution == 'async' else 'shotgun-api3',
        capabilities=ProviderCapabilities(
            sync=True,
            async_=execution == 'async',
        ),
        _sync_factory=factory,
        _async_factory=factory.create_async if execution == 'async' else None,
    )
    if watchdog is None:
        watchdog = ComponentWatchdog(
            kind='lane',
            identity='serial',
            worker_instance='worker-1',
            registration_token=_WATCHDOG_IDENTITY,
            startup_deadline_secs=internal_stall_deadline_secs(
                health_interval_secs,
                credential,
            ),
            clock=clock,
        )
        watchdog.transition('idle', deadline_at=None)
    return (
        GroupLane(
            async_cancel_grace_secs=async_cancel_grace_secs,
            state=state,
            site=site,
            group=group,
            provider=provider,
            plugins=(plugin for plugin in plugins),
            worker_instance='worker-1',
            health_interval_secs=health_interval_secs,
            watchdog=watchdog,
            clock=clock,
            token_factory=token_factory,
            wait=wait,
        ),
        factory,
    )


def _concrete_strict_lane(
    source_state: SourceState,
    group_state: GroupState,
    strict: StrictBuffer,
    callbacks: tuple[CallbackSpec, ...],
    *,
    fanout: EventFanout,
) -> tuple[JoinCoordinator, GroupLane]:
    coordinator = JoinCoordinator(
        state=source_state,
        fanout=fanout,
        source_generation='source-1',
        strict_retention=strict,
    )
    lane, _factory = _lane(
        group_state,
        callbacks,
        ordering='strict',
        token_factory=lambda: _ATTEMPT_1,
    )
    assert lane.setup() is None
    return coordinator, lane


def _lane_callback_can_wait(
    callback_started: threading.Event,
    cleanup_requested: threading.Event,
) -> bool:
    callback_started.set()
    return not cleanup_requested.is_set()


def _operator() -> _OperatorOptions:
    return {
        'reason': 'test recovery',
        'identity': AuditIdentity.current(),
        'wall_clock_time': 100.0,
    }


def _pausing_cancel_scheduler(
    schedule: Callable[[Callable[[], object]], asyncio.Handle],
    *,
    allow_schedule: threading.Event,
    scheduling_errors: list[str],
    scheduling_paused: threading.Event,
) -> Callable[[Callable[[], object]], asyncio.Handle]:

    def pause_task_cancel(
        callback_to_schedule: Callable[[], object],
    ) -> asyncio.Handle:
        if getattr(callback_to_schedule, '__name__', None) == 'cancel':
            scheduling_paused.set()
            assert allow_schedule.wait(timeout=1.0)
        try:
            return schedule(callback_to_schedule)
        except RuntimeError as error:
            scheduling_errors.append(str(error))
            return cast('asyncio.Handle', object())

    return pause_task_cancel


def _pending_lane_future(
    releases: list[asyncio.Future[None]],
    callback_started: threading.Event,
    cleanup_requested: threading.Event,
) -> asyncio.Future[None] | None:
    release = asyncio.get_running_loop().create_future()
    releases.append(release)
    callback_started.set()
    return None if cleanup_requested.is_set() else release


def _pending_range(
    state: GroupState,
    *,
    closed: bool = True,
    late_ids: tuple[int, ...] = (),
) -> PendingRange:
    connection = state._database.connection
    insert_range(connection, 'range-1', 11, 15, state='closed' if closed else 'open')
    bind_group(connection, 'range-1', 'serial', 1, floor=10, eligibility_point=30.0)
    insert_source_late_ids(connection, 'range-1', late_ids)
    (revision,) = state.pending_ranges()
    return revision


def _publish_lane_for_cleanup(
    lane: GroupLane,
    holder: list[GroupLane],
    cleanup_requested: threading.Event,
) -> bool:
    holder.append(lane)
    if not cleanup_requested.is_set():
        return True
    lane.request_shutdown()
    lane.close()
    return False


def _release_lane_thread(
    thread: threading.Thread,
    cleanup: _LaneThreadCleanup,
) -> None:
    cleanup.cleanup_requested.set()
    for blocker in cleanup.blockers:
        blocker.set()
    for future in cleanup.futures or ():
        if not future.done():
            future.get_loop().call_soon_threadsafe(future.set_result, None)
    if cleanup.holder:
        cleanup.holder[0].request_shutdown()
    for other in cleanup.other_threads:
        if other is not None:
            other.join(timeout=2.0)
    thread.join(timeout=2.0)
    assert not thread.is_alive(), 'lane test thread did not stop during cleanup'


def _resume_cancelled_lane(path: Path, attempts: list[int]) -> GroupSnapshot:
    with _group_state(path) as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: attempts.append(context.delivery_attempt),
                    execution='async',
                ),
            ),
            backend=_Backend(_events={11: _event(11)}, _exact_reads={}),
            execution='async',
        )
        assert lane.setup() is None
        completed = lane.resume()
        lane.close()

    return completed


def _run_async_close_race(
    path: Path,
    callback: Callable[[EventContext], object],
    race: _AsyncCloseRaceState,
) -> None:
    with _group_state(path) as state:
        lane, factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution='async'),),
            async_cancel_grace_secs=0.05,
            execution='async',
            extra_plugin_count=1,
        )
        if not _publish_lane_for_cleanup(lane, race._holder, race.cleanup_requested):
            return
        race._factories.append(factory)
        assert lane.setup() is None
        race._snapshots.append(lane.deliver_many_counted((Delivery(_event(11)),))[0])
        lane.close()
    race._lane_finished.set()


def test_async_callback_completes_normally_during_cancellation_grace(
    tmp_path: Path,
) -> None:
    callback_started = threading.Event()
    cancellation_observed = threading.Event()
    holder: list[GroupLane] = []
    releases: list[asyncio.Future[None]] = []
    snapshots: list[GroupSnapshot] = []
    cleanup_requested = threading.Event()
    path = tmp_path / 'group.sqlite3'

    async def callback(_context: EventContext) -> None:
        release = _pending_lane_future(releases, callback_started, cleanup_requested)
        if release is None:
            return
        try:
            await release
        except asyncio.CancelledError:
            cancellation_observed.set()
            raise

    def run() -> None:
        with _group_state(path) as state:
            lane, _factory = _lane(
                state,
                (_callback(0, 'callback', callback, execution='async'),),
                async_cancel_grace_secs=1.0,
                execution='async',
            )
            holder.append(lane)
            assert lane.setup() is None
            snapshots.append(lane.deliver_many_counted((Delivery(_event(11)),))[0])
            lane.close()

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert callback_started.wait(timeout=1.0)
        holder[0].request_shutdown()
        cancelled_before_release = cancellation_observed.wait(timeout=0.05)
        if not cancelled_before_release:
            release = releases[0]
            release.get_loop().call_soon_threadsafe(release.set_result, None)
        thread.join(timeout=2.0)
    finally:
        _release_lane_thread(
            thread,
            _LaneThreadCleanup(
                holder=holder,
                cleanup_requested=cleanup_requested,
                futures=releases,
            ),
        )

    assert not cancelled_before_release
    assert not thread.is_alive()
    assert not cancellation_observed.is_set()
    assert snapshots[0].cursor == 11
    assert snapshots[0].in_flight_event_id is None
    assert snapshots[0].failure_text is None
    assert holder[0]._async_cancel_timer is None


def test_async_callback_must_return_an_awaitable(tmp_path: Path) -> None:
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda _context: None,
                    execution='async',
                    wrap_async=False,
                ),
            ),
            execution='async',
        )
        assert lane.setup() is None
        snapshot = lane.deliver_many_counted((Delivery(_event(11)),))[0]
        lane.close()

    assert snapshot.status == 'callback-failed'
    assert "async callback 'callback' did not return an awaitable" in (snapshot.failure_text or '')


@pytest.mark.parametrize('grace', [0.0, -1.0, float('inf'), float('nan')])
def test_async_cancellation_grace_must_be_finite_and_positive(
    tmp_path: Path,
    grace: float,
) -> None:
    with (
        _group_state(tmp_path / 'group.sqlite3') as state,
        pytest.raises(
            ValueError,
            match='async cancellation grace must be finite and positive',
        ),
    ):
        _lane(
            state,
            (_callback(0, 'callback', lambda _context: None, execution='async'),),
            async_cancel_grace_secs=grace,
            execution='async',
        )


def test_async_cancellation_scheduling_is_serialized_with_loop_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allow_schedule = threading.Event()
    callback_completed = threading.Event()
    callback_started = threading.Event()
    race = _AsyncCloseRaceState()
    releases: list[asyncio.Future[None]] = []
    scheduling_errors: list[str] = []
    path = tmp_path / 'group.sqlite3'
    scheduling_paused = threading.Event()

    async def callback(_context: EventContext) -> None:
        release = _pending_lane_future(
            releases,
            callback_started,
            race.cleanup_requested,
        )
        if release is not None:
            await release
            callback_completed.set()

    lane_thread = threading.Thread(
        target=_run_async_close_race,
        args=(path, callback, race),
    )
    shutdown_thread: threading.Thread | None = None
    lane_thread.start()
    try:
        assert callback_started.wait(timeout=1.0)
        async_handle = cast('_AsyncHandle', race._factories[0]._handles[0])
        loop = async_handle._created_loop
        schedule = loop.call_soon_threadsafe

        monkeypatch.setattr(
            loop,
            'call_soon_threadsafe',
            _pausing_cancel_scheduler(
                schedule,
                allow_schedule=allow_schedule,
                scheduling_errors=scheduling_errors,
                scheduling_paused=scheduling_paused,
            ),
        )
        shutdown_thread = threading.Thread(target=race._holder[0].request_shutdown)
        shutdown_thread.start()
        assert scheduling_paused.wait(timeout=1.0)
        schedule(releases[0].set_result, None)
        assert callback_completed.wait(timeout=1.0)
        cleanup_finished_before_schedule = race._lane_finished.wait(timeout=0.1)
        close_started_before_schedule = bool(race._factories[0]._close_observations)
        allow_schedule.set()
        shutdown_thread.join(timeout=2.0)
        lane_thread.join(timeout=2.0)
    finally:
        _release_lane_thread(
            lane_thread,
            _LaneThreadCleanup(
                holder=race._holder,
                cleanup_requested=race.cleanup_requested,
                blockers=(allow_schedule,),
                futures=releases,
                other_threads=(shutdown_thread,),
            ),
        )

    assert shutdown_thread is not None
    assert not shutdown_thread.is_alive()
    assert not lane_thread.is_alive()
    assert not cleanup_finished_before_schedule
    assert not close_started_before_schedule
    assert scheduling_errors == []
    assert race._factories[0]._close_observations == [(1, False), (0, False)]
    assert loop.is_closed()
    assert race._snapshots[0].cursor == 10
    assert race._snapshots[0].in_flight_event_id == 11
    assert race._holder[0]._current_async_task is None
    assert race._holder[0]._daemon_cancelled_task is None
    assert race._holder[0]._async_cancel_task is None
    assert race._holder[0]._async_cancel_timer is None


def test_async_cancellation_suppression_requires_worker_kill_escalation(
    tmp_path: Path,
) -> None:
    async_cancel_grace_secs = 0.05
    callback_started = threading.Event()
    cancellation_suppressed = threading.Event()
    cleanup_requested = threading.Event()
    releases: list[asyncio.Future[None]] = []
    holder: list[GroupLane] = []
    path = tmp_path / 'group.sqlite3'

    async def callback(_context: EventContext) -> None:
        release = _pending_lane_future(releases, callback_started, cleanup_requested)
        if release is None:
            return
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancellation_observed_at.append(time.monotonic())
            cancellation_suppressed.set()
            await release

    def run() -> None:
        with _group_state(path) as state:
            lane, _factory = _lane(
                state,
                (_callback(0, 'callback', callback, execution='async'),),
                async_cancel_grace_secs=async_cancel_grace_secs,
                execution='async',
            )
            if not _publish_lane_for_cleanup(lane, holder, cleanup_requested):
                return
            assert lane.setup() is None
            lane.deliver_many_counted((Delivery(_event(11)),))[0]
            lane.close()

    cancellation_observed_at: list[float] = []
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert callback_started.wait(timeout=1.0)
        cancellation_deadline = time.monotonic() + async_cancel_grace_secs
        holder[0].request_shutdown()
        holder[0].request_shutdown()
        assert cancellation_suppressed.wait(timeout=1.0)
        suppressed_past_grace = thread.is_alive()

        health = holder[0].health()
        release = releases[0]
        release.get_loop().call_soon_threadsafe(release.set_result, None)
        thread.join(timeout=2.0)
    finally:
        _release_lane_thread(
            thread,
            _LaneThreadCleanup(
                holder=holder,
                cleanup_requested=cleanup_requested,
                futures=releases,
            ),
        )

    assert cancellation_observed_at[0] >= cancellation_deadline
    assert (suppressed_past_grace, thread.is_alive()) == (True, False)
    assert (health.phase, health.event_id) == ('callback', 11)
    assert holder[0].shutdown_requested()
    with _group_state(path) as state:
        snapshot = state.snapshot()
    assert (snapshot.parked, snapshot.in_flight_event_id, snapshot.failure_text) == (
        False,
        11,
        None,
    )


def test_async_groups_own_distinct_persistent_lane_thread_loops(
    tmp_path: Path,
) -> None:
    loops: list[asyncio.AbstractEventLoop] = []
    factories: list[_HandleFactory] = []
    lane_threads: list[int] = []

    def _run_group(index: int) -> None:
        def setup(_context: object) -> None:
            loops.append(asyncio.get_running_loop())
            lane_threads.append(threading.get_ident())

        def callback(_context: EventContext) -> None:
            loops.append(asyncio.get_running_loop())
            lane_threads.append(threading.get_ident())

        with _group_state(tmp_path / f'group-{index}.sqlite3') as state:
            lane, factory = _lane(
                state,
                (_callback(0, 'callback', callback, execution='async'),),
                execution='async',
                setup=setup,
            )
            factories.append(factory)
            assert lane.setup() is None
            lane.deliver_many_counted((Delivery(_event(11)), Delivery(_event(12))))[0]
            lane.close()

    thread = threading.Thread(target=lambda: (_run_group(1), _run_group(2)))
    thread.start()
    thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert len(loops) == 6
    assert loops[0] is loops[1] is loops[2]
    assert loops[3] is loops[4] is loops[5]
    assert loops[0] is not loops[3]
    assert len(set(lane_threads)) == 1
    for index, factory in enumerate(factories):
        handle = cast('_AsyncHandle', factory._handles[0])
        assert handle._created_loop is loops[index * 3]
        assert handle._created_in == lane_threads[index * 3]
        assert handle._closed
        assert handle._created_loop.is_closed()


def test_async_lane_closes_loop_after_multiple_handle_close_failures(
    tmp_path: Path,
) -> None:
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, factory = _lane(
            state,
            (),
            execution='async',
            extra_plugin_count=2,
        )
        assert lane.setup() is None
        handles = tuple(cast('_AsyncHandle', handle) for handle in factory._handles)
        loop = handles[0]._created_loop
        first_error = KeyboardInterrupt('first close failure')
        handles[2]._close_error = first_error
        handles[0]._close_error = SystemExit('later close failure')

        with pytest.raises(KeyboardInterrupt) as raised:
            lane.close()

        close_observations = tuple(factory._close_observations)
        loop_closed = loop.is_closed()
        loop_owner_cleared = getattr(lane, '_loop', loop) is None
        if not loop_closed:
            for handle in handles:
                handle._close_error = None
            lane.close()

    assert raised.value is first_error
    assert close_observations == ((2, False), (1, False), (0, False))
    assert loop_closed
    assert loop_owner_cleared


def test_async_setup_cancelled_error_retries_same_lane_and_handle(
    tmp_path: Path,
) -> None:
    attempts = 0
    setup_handles: list[object] = []

    def setup(context: SetupContext) -> None:
        nonlocal attempts
        attempts += 1
        setup_handles.append(context.sg)
        if attempts == 1:
            raise asyncio.CancelledError('unexpected setup cancellation')

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, factory = _lane(
            state,
            (),
            execution='async',
            setup=setup,
        )

        first = lane.setup()
        second = lane.setup()
        lane.close()

    assert first is not None
    assert first.code == 'setup-failed'
    assert second is None
    assert attempts == 2
    assert len(factory._handles) == 1
    assert setup_handles[0] is setup_handles[1] is factory._handles[0]


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_callback_failure_halts_the_group_and_blocks_later_work(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    actions: list[str] = []

    def fail(_context: object) -> None:
        actions.append('failed')
        raise RuntimeError('broken callback')

    def later(_context: object) -> None:
        actions.append('later')

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(0, 'fail', fail, execution=execution),
                _callback(1, 'later', later, execution=execution),
            ),
            execution=execution,
        )
        assert lane.setup() is None

        failed = lane.deliver_many_counted(
            (
                Delivery(_event(11)),
                Delivery(_event(12)),
            )
        )[0]
        blocked = lane.deliver_many_counted((Delivery(_event(13)),))[0]
        lane.close()

    assert actions == ['failed']
    assert failed.status == 'callback-failed'
    assert failed.cursor == 10
    assert failed.in_flight_event_id == 11
    assert failed.failure_ordinal == 0
    assert 'broken callback' in (failed.failure_text or '')
    assert blocked == failed


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_callback_retry_resumes_failed_callback_in_same_lane(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    attempts: list[int] = []
    event = _event(11)
    backend = _Backend(_events={11: event}, _exact_reads={})

    def callback(context: EventContext) -> None:
        attempts.append(context.delivery_attempt)
        if len(attempts) == 1:
            raise RuntimeError('retry me')

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution=execution),),
            backend=backend,
            execution=execution,
        )
        assert lane.setup() is None
        failed = lane.deliver_many_counted((Delivery(event),))[0]
        state.retry(
            event_id=11,
            callback_ordinal=0,
            expected_revision=failed.revision,
            **_operator(),
        )
        completed = lane.resume()
        lane.close()

    assert attempts == [1, 2]
    assert completed.cursor == 11
    assert len(factory._handles) == 2


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_callback_traceback_is_redacted_and_utf8_bounded(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    sensitive_value = 'api-key'
    sensitive_digest = hashlib.sha256(sensitive_value.encode()).hexdigest()

    def fail(_context: object) -> None:
        raise RuntimeError(f'{sensitive_value}:{sensitive_digest}:' + '\u00e9' * 200)

    with _group_state(
        tmp_path / 'group.sqlite3',
        max_failure_text_bytes=320,
    ) as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', fail, execution=execution),),
            execution=execution,
        )
        assert lane.setup() is None
        snapshot = lane.deliver_many_counted((Delivery(_event(11)),))[0]
        lane.close()

    failure_text = snapshot.failure_text or ''
    assert 'Traceback (most recent call last):' in failure_text
    assert '<redacted>' in failure_text
    assert sensitive_value not in failure_text
    assert sensitive_digest not in failure_text
    assert len(failure_text.encode()) <= 320
    assert snapshot.failure_byte_count is not None
    assert snapshot.failure_byte_count > len(failure_text.encode())
    assert snapshot.failure_digest is not None
    assert len(snapshot.failure_digest) == 64


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('boundary', ['start_event', 'begin_callback', 'callback_done'])
def test_collapse_between_refresh_and_guarded_write_stops_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    boundary: str,
) -> None:
    delivered: list[int] = []
    path = tmp_path / 'source.db'

    def callback(context: EventContext) -> None:
        delivered.append(cast('int', context.event['id']))

    with (
        SourceState.open(
            path,
            writes=write_context_for(path),
            create=True,
            site='alpha',
            source='events',
            stream_fingerprint='stream',
            source_generation='source-1',
            max_group_obligations=1,
        ) as source,
        _group_state(path, ordering=ordering) as state,
    ):
        source.initialize(10, incarnation='incarnation')
        source.commit_observations((12,), successful_clock=0)
        source._close_range('range-11-11', successful_clock=0)
        (pending,) = state.pending_ranges()
        lane, _factory = _lane(state, (_callback(0, 'callback', callback),), ordering=ordering)
        assert lane.setup() is None
        original = getattr(state, boundary)

        def collapse_then_write(*args: object, **kwargs: object) -> object:
            source.commit_observations((14,), successful_clock=0)
            return original(*args, **kwargs)

        monkeypatch.setattr(state, boundary, collapse_then_write)
        halted = lane.scan_obligation(
            pending, (_event(11),), source_successful_clock=pending.eligibility_point
        )
        assert halted.status == 'replay-required'
        assert halted.cursor == 10
        assert state._bootstrap_state().last_delivered_id == 10
        assert delivered == ([11] if boundary == 'callback_done' else [])
        monkeypatch.setattr(state, boundary, original)
        before_replay = len(delivered)
        operator: _OperatorOptions = {
            'reason': 'replay collapsed range',
            'identity': AuditIdentity.current(),
            'wall_clock_time': 1.0,
        }
        state.park(expected_revision=halted.revision, **operator)
        state.dispose(
            DispositionRequest(
                command='replay',
                group='serial',
                disposition_id='replay',
                reserved_generation=2,
                expected_revision=state.snapshot().revision,
                from_event_id=11,
                acknowledge_truncation=False,
                reason=operator['reason'],
                identity=operator['identity'],
                wall_clock_time=operator['wall_clock_time'],
            )
        )
        state.unpark(expected_revision=state.snapshot().revision, **operator)
        assert all(item.obligation_id != pending.obligation_id for item in state.pending_ranges())
        replayed, consumed = lane.deliver_many_counted((Delivery(_event(11)),))
        assert replayed.cursor == 11
        assert consumed == 1
        assert delivered[before_replay:] == [11]
        lane.close()


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_common_delivery_failure_retry_and_filter_contract(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    actions: list[tuple[str, int]] = []
    event = _event(11, event_type='Shotgun_Match')
    backend = _Backend(_events={11: event}, _exact_reads={})

    def first(context: EventContext) -> None:
        actions.append(('first', context.delivery_attempt))

    def failed(context: EventContext) -> None:
        actions.append(('failed', context.delivery_attempt))
        if context.delivery_attempt == 1:
            raise RuntimeError('retry common contract')

    def later(context: EventContext) -> None:
        actions.append(('later', context.delivery_attempt))

    callbacks = (
        _callback(
            0,
            'ignored',
            lambda _context: actions.append(('ignored', 0)),
            event_type='Shotgun_Ignore',
            execution=execution,
        ),
        _callback(
            1,
            'first',
            first,
            event_type='Shotgun_Match',
            execution=execution,
        ),
        _callback(
            2,
            'failed',
            failed,
            event_type='Shotgun_Match',
            execution=execution,
        ),
        _callback(
            3,
            'later',
            later,
            event_type='Shotgun_Match',
            execution=execution,
        ),
    )
    with _group_state(tmp_path / f'{execution}.sqlite3') as state:
        lane, _factory = _lane(
            state,
            callbacks,
            backend=backend,
            execution=execution,
        )
        assert lane.setup() is None
        failed_snapshot = lane.deliver_many_counted(
            (
                Delivery(event),
                Delivery(_event(12, event_type='Shotgun_Match')),
            )
        )[0]
        state.retry(
            event_id=11,
            callback_ordinal=1,
            expected_revision=failed_snapshot.revision,
            **_operator(),
        )
        completed = lane.resume()
        lane.close()

    assert actions == [
        ('first', 1),
        ('failed', 1),
        ('failed', 2),
        ('later', 1),
    ]
    assert failed_snapshot.status == 'callback-failed'
    assert failed_snapshot.cursor == 10
    assert failed_snapshot.in_flight_event_id == 11
    assert completed.status == 'running'
    assert completed.cursor == 11
    assert completed.in_flight_event_id is None
    assert completed.callback_marks == ()


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_common_shape_restart_strict_and_audit_contract(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    actions: list[str] = []
    event = _event(11)
    backend = _Backend(_events={11: event}, _exact_reads={})
    path = tmp_path / f'{execution}.sqlite3'

    def crash(_context: EventContext) -> None:
        actions.append('old')
        raise SystemExit(19)

    with _group_state(path, ordering='strict') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'old', crash, execution=execution),),
            backend=backend,
            execution=execution,
            ordering='strict',
        )
        assert lane.setup() is None
        with pytest.raises(SystemExit, match='19'):
            lane.deliver_many_counted((Delivery(event),))[0]
        lane.close()

    current = (
        _callback(
            0,
            'replacement',
            lambda _context: actions.append('replacement'),
            execution=execution,
        ),
    )
    manifest = CallbackManifest(callbacks=tuple(callback.identity for callback in current))
    with _group_state(path, ordering='strict') as state:
        lane, _factory = _lane(
            state,
            current,
            backend=backend,
            execution=execution,
            ordering='strict',
        )
        assert lane.setup() is None
        mismatch = lane.resume()
        assert mismatch.status == 'shape-mismatch'
        state.restart_event(
            manifest,
            expected_revision=mismatch.revision,
            **_operator(),
        )
        completed = lane.resume()
        skipped = lane.deliver_many_counted((Delivery(_event(10)),))[0]
        records = state._audit_records()
        lane.close()

    assert actions == ['old', 'replacement']
    assert completed.cursor == 11
    assert skipped.cursor == 11
    assert lane._ordering_anomalies[-1].action == 'skip'
    floor_skips = tuple(
        record for record in records if record.command == 'strict-delivery-floor-skip'
    )
    assert len(floor_skips) == 1
    assert floor_skips[0].event_id == 10


def test_concrete_strict_attached_before_group_cas_aborts_exact_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strict = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'serial': 1.0},
    )
    fanout = EventFanout()
    with _concrete_strict_states(
        tmp_path / 'source.db',
        initial_position=10,
        source_initial=10,
    ) as (source_state, group_state):
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (_callback(0, 'callback', lambda _context: None),),
            fanout=fanout,
        )
        channel = GroupChannel(
            group='serial',
            ordering='strict',
            max_events=10,
            max_bytes=10_000_000,
        )

        def fail_group_cas(
            _identity: LiveJoinIdentity,
            *,
            expected_revision: int,
        ) -> GroupSnapshot:
            raise ConnectionError(f'group CAS lost at revision {expected_revision}')

        monkeypatch.setattr(group_state, 'commit_live_join', fail_group_cas)

        def catch_up(_boundary: int) -> None:
            strict._observe(
                EncodedEvent.from_event_frame(11, encode_event(_event(11))),
                successful_time=0.0,
            )

        with pytest.raises(ConnectionError, match='group CAS lost'):
            lane.join(coordinator, channel, catch_up=catch_up)
        snapshot = group_state.snapshot()
        lane.close()

    assert snapshot.latest_join_boundary is None
    assert strict.event_count == 0
    assert strict._leases == ()
    assert not channel.status.attached


def test_concrete_strict_buffer_gates_join_and_callback(
    tmp_path: Path,
) -> None:
    callback_ids: list[int] = []
    strict = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'serial': 1.0},
    )
    fanout = EventFanout()
    source_path = tmp_path / 'source.db'
    with _concrete_strict_states(
        source_path,
        initial_position=10,
        source_initial=10,
    ) as (source_state, group_state):
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (
                _callback(
                    0, 'callback', lambda context: callback_ids.append(_context_event_id(context))
                ),
            ),
            fanout=fanout,
        )
        channel = GroupChannel(
            group='serial',
            ordering='strict',
            max_events=10,
            max_bytes=10_000_000,
        )

        def catch_up(boundary: int) -> None:
            assert boundary == 10
            strict._observe(
                EncodedEvent.from_event_frame(11, encode_event(_event(11))),
                successful_time=0.0,
            )

        joined = lane.join(coordinator, channel, catch_up=catch_up)
        assert isinstance(joined, LiveJoinIdentity)
        assert callback_ids == []
        assert [release._event_id for release in strict.pump_many(('serial',), 1.0)] == [11]
        drained = lane.drain_live(channel)

        assert drained.cursor == 11
        assert callback_ids == [11]
        assert strict._watermark('serial')._released_through == 11


def test_concrete_strict_group_cas_before_source_finalize_survives_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group_path = tmp_path / 'source.db'
    strict = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'serial': 1.0},
    )
    fanout = EventFanout()
    with _concrete_strict_states(
        group_path,
        initial_position=10,
        source_initial=10,
    ) as (source_state, group_state):
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (_callback(0, 'callback', lambda _context: None),),
            fanout=fanout,
        )
        channel = GroupChannel(
            group='serial',
            ordering='strict',
            max_events=10,
            max_bytes=10_000_000,
        )

        def fail_finalize(_finalization: JoinFinalize) -> JoinFinalized:
            raise ConnectionError('source finalize unavailable')

        monkeypatch.setattr(coordinator, 'finalize', fail_finalize)

        def catch_up(_boundary: int) -> None:
            strict._observe(
                EncodedEvent.from_event_frame(11, encode_event(_event(11))),
                successful_time=0.0,
            )

        with pytest.raises(ConnectionError, match='source finalize unavailable'):
            lane.join(coordinator, channel, catch_up=catch_up)
        committed = group_state.snapshot()
        lane.close()

    assert committed.latest_join_boundary == 10
    assert strict.event_count == 0
    assert strict._leases == ()
    assert not channel.status.attached
    with _group_state(
        group_path,
        initial_position=10,
        ordering='strict',
    ) as reopened:
        assert reopened.snapshot().latest_join_boundary == 10


def test_concrete_strict_join_abort_frees_retained_payload_after_preflight_rejection(
    tmp_path: Path,
) -> None:
    strict = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'serial': 1.0},
    )
    fanout = EventFanout()
    with _concrete_strict_states(
        tmp_path / 'source.db',
        initial_position=10,
        source_initial=12,
    ) as (source_state, group_state):
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (_callback(0, 'callback', lambda _context: None),),
            fanout=fanout,
        )
        channel = GroupChannel(
            group='serial',
            ordering='strict',
            max_events=10,
            max_bytes=10_000_000,
        )

        def incomplete_catch_up(boundary: int) -> None:
            assert boundary == 12
            strict._observe(
                EncodedEvent.from_event_frame(13, encode_event(_event(13))),
                successful_time=0.0,
            )

        with pytest.raises(JoinProtocolError, match='catch-up did not reach'):
            lane.join(coordinator, channel, catch_up=incomplete_catch_up)

        assert strict.event_count == 0
        assert strict._leases == ()
        assert not channel.status.attached


def test_concrete_strict_post_cas_capacity_relief_settles_exact_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strict = StrictBuffer(
        max_events=1,
        max_bytes=10_000_000,
        group_holdbacks={'retainer': 300.0, 'serial': 300.0},
    )
    attach_strict_group(strict, 'retainer', 1, holdback_secs=300.0, delivery_floor=0)
    with _concrete_strict_states(
        tmp_path / 'source.db',
        initial_position=1,
        source_initial=1,
    ) as (source_state, group_state):
        prior = group_state.record_strict_join_outcome(
            attempt_token=_ATTEMPT_OLD,
            capacity_caused=True,
            abort_limit=JOIN_ABORT_LIMIT,
            expected_revision=0,
        )
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (_callback(0, 'callback', lambda _context: None),),
            fanout=EventFanout(),
        )
        channel = GroupChannel(
            group='serial',
            ordering='strict',
            max_events=10,
            max_bytes=10_000_000,
        )
        commit_live_join = group_state.commit_live_join

        def commit_then_relieve_capacity(
            identity: LiveJoinIdentity,
            expected_revision: int,
        ) -> GroupSnapshot:
            committed = commit_live_join(identity, expected_revision)
            strict._observe(
                EncodedEvent.from_event_frame(3, encode_event(_event(3))),
                successful_time=0.0,
            )
            return committed

        monkeypatch.setattr(
            group_state,
            'commit_live_join',
            commit_then_relieve_capacity,
        )

        def catch_up(_boundary: int) -> None:
            strict._observe(
                EncodedEvent.from_event_frame(2, encode_event(_event(2))),
                successful_time=0.0,
            )

        with pytest.raises(JoinCapacityError, match='exhausted capacity'):
            lane.join(coordinator, channel, catch_up=catch_up)
        recorded = group_state.snapshot()
        with pytest.raises(JoinProtocolError, match='callback gate is closed'):
            lane.drain_live(channel)
        lane.close()

    assert prior.strict_capacity_abort_streak == 1
    assert recorded.strict_capacity_abort_streak == 2
    assert recorded.strict_capacity_attempt_token == _ATTEMPT_1
    assert strict._capacity_aborts.get('serial', 0) == 1
    assert not channel.status.attached


def test_concrete_strict_success_resets_capacity_before_gate_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strict = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'serial': 1.0},
    )
    with _concrete_strict_states(
        tmp_path / 'source.db',
        initial_position=10,
        source_initial=10,
    ) as (source_state, group_state):
        group_state.record_strict_join_outcome(
            attempt_token=_ATTEMPT_OLD,
            capacity_caused=True,
            abort_limit=JOIN_ABORT_LIMIT,
            expected_revision=0,
        )
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (_callback(0, 'callback', lambda _context: None),),
            fanout=EventFanout(),
        )
        channel = GroupChannel(
            group='serial',
            ordering='strict',
            max_events=10,
            max_bytes=10_000_000,
        )
        finalize = coordinator.finalize
        record_strict_join_outcome = group_state.record_strict_join_outcome
        observations: list[tuple[str, int]] = []

        def finalize_while_pending(finalization: JoinFinalize) -> JoinFinalized:
            before = group_state.snapshot()
            observations.append(('before-finalize', before.strict_capacity_abort_streak))
            assert before.strict_capacity_attempt_token == _ATTEMPT_1
            finalized = finalize(finalization)
            after = group_state.snapshot()
            observations.append(('after-finalize', after.strict_capacity_abort_streak))
            return finalized

        def record_before_gate(
            *,
            attempt_token: str,
            capacity_caused: bool,
            abort_limit: int,
            expected_revision: int,
        ) -> GroupSnapshot:
            settled = record_strict_join_outcome(
                attempt_token=attempt_token,
                capacity_caused=capacity_caused,
                abort_limit=abort_limit,
                expected_revision=expected_revision,
            )
            observations.append(('before-gate', settled.strict_capacity_abort_streak))
            with pytest.raises(JoinProtocolError, match='callback gate is closed'):
                lane.drain_live(channel)
            return settled

        monkeypatch.setattr(coordinator, 'finalize', finalize_while_pending)
        monkeypatch.setattr(group_state, 'record_strict_join_outcome', record_before_gate)

        joined = lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        snapshot = group_state.snapshot()
        lane.close()

    assert isinstance(joined, LiveJoinIdentity)
    assert observations == [
        ('before-finalize', 1),
        ('after-finalize', 1),
        ('before-gate', 0),
    ]
    assert snapshot.strict_capacity_abort_streak == 0
    assert snapshot.strict_capacity_attempt_token == _ATTEMPT_1


def test_counted_delivery_reports_cooperative_rejection(tmp_path: Path) -> None:
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
        )
        assert lane.setup() is None
        lane.request_shutdown()
        before = state.snapshot()

        snapshot, consumed = lane.deliver_many_counted((Delivery(_event(11)),))
        lane.close()

    assert snapshot == before
    assert consumed == 0


def test_counted_delivery_reports_failed_callback(tmp_path: Path) -> None:
    def fail(_context: object) -> None:
        raise RuntimeError('broken callback')

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', fail),),
        )
        assert lane.setup() is None

        snapshot, consumed = lane.deliver_many_counted((Delivery(_event(11)),))
        lane.close()

    assert snapshot.status == 'callback-failed'
    assert snapshot.cursor == 10
    assert snapshot.in_flight_event_id == 11
    assert snapshot.callback_marks[0][3] == 'pending'
    assert consumed == 1


def test_counted_delivery_reports_interrupted_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invoked: list[int] = []
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: invoked.append(_context_event_id(context)),
                    execution='async',
                ),
            ),
            execution='async',
        )
        assert lane.setup() is None
        begin_callback = state.begin_callback

        def begin_then_shutdown(
            ordinal: int,
            *,
            expected_revision: int,
        ) -> GroupSnapshot:
            snapshot = begin_callback(
                ordinal,
                expected_revision=expected_revision,
            )
            lane.request_shutdown()
            return snapshot

        monkeypatch.setattr(state, 'begin_callback', begin_then_shutdown)
        snapshot, consumed = lane.deliver_many_counted((Delivery(_event(11)),))
        lane.close()

    assert invoked == []
    assert snapshot.status == 'running'
    assert snapshot.cursor == 10
    assert snapshot.in_flight_event_id == 11
    assert snapshot.callback_marks[0][3] == 'pending'
    assert consumed == 1


def test_counted_delivery_reports_irrelevant_event(tmp_path: Path) -> None:
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda _context: None,
                    event_type='Shotgun_Match',
                ),
            ),
        )
        assert lane.setup() is None

        snapshot, consumed = lane.deliver_many_counted((Delivery(_event(11)),))
        lane.close()

    assert snapshot.cursor == 11
    assert snapshot.in_flight_event_id is None
    assert consumed == 1


def test_counted_delivery_reports_terminal_event(tmp_path: Path) -> None:
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
        )
        assert lane.setup() is None

        snapshot, consumed = lane.deliver_many_counted((Delivery(_event(11)),))
        lane.close()

    assert snapshot.cursor == 11
    assert snapshot.in_flight_event_id is None
    assert consumed == 1


@pytest.mark.parametrize('catch_cancellation', [False, True])
def test_daemon_async_cancellation_preserves_in_flight_for_restart(
    tmp_path: Path,
    catch_cancellation: bool,
) -> None:
    async_cancel_grace_secs = 0.05
    callback_started = threading.Event()
    cancellation_observed = threading.Event()
    cancellation_observed_at: list[float] = []
    cleanup_requested = threading.Event()
    cleanup_finished = threading.Event()
    attempts: list[int] = []
    holder: list[GroupLane] = []
    snapshots: list[GroupSnapshot] = []
    path = tmp_path / f'cancel-{catch_cancellation}.sqlite3'

    async def callback(context: EventContext) -> None:
        attempts.append(context.delivery_attempt)
        try:
            if _lane_callback_can_wait(callback_started, cleanup_requested):
                await asyncio.Future()
        except asyncio.CancelledError:
            cancellation_observed_at.append(time.monotonic())
            cancellation_observed.set()
            if not catch_cancellation:
                raise
        finally:
            cleanup_finished.set()

    def run() -> None:
        with _group_state(path) as state:
            lane, _factory = _lane(
                state,
                (
                    _callback(
                        0,
                        'callback',
                        callback,
                        execution='async',
                    ),
                ),
                async_cancel_grace_secs=async_cancel_grace_secs,
                execution='async',
            )
            if not _publish_lane_for_cleanup(lane, holder, cleanup_requested):
                return
            assert lane.setup() is None
            snapshots.append(lane.deliver_many_counted((Delivery(_event(11)),))[0])
            lane.close()

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert callback_started.wait(timeout=1.0)
        cancellation_deadline = time.monotonic() + async_cancel_grace_secs
        holder[0].request_shutdown()
        assert cancellation_observed.wait(timeout=1.0)
        thread.join(timeout=2.0)
    finally:
        _release_lane_thread(
            thread,
            _LaneThreadCleanup(
                holder=holder,
                cleanup_requested=cleanup_requested,
            ),
        )

    assert cancellation_observed_at[0] >= cancellation_deadline
    assert (thread.is_alive(), cleanup_finished.is_set()) == (False, True)
    interrupted = snapshots[0]
    assert (
        interrupted.status,
        interrupted.cursor,
        interrupted.in_flight_event_id,
        interrupted.failure_text,
    ) == ('running', 10, 11, None)
    assert interrupted.callback_marks[0][3] == 'pending'
    assert holder[0]._async_cancel_timer is None

    completed = _resume_cancelled_lane(path, attempts)

    assert attempts == [1, 2]
    assert completed.cursor == 11


def test_default_token_factory_retains_no_attempt_history(tmp_path: Path) -> None:
    coordinator = _Coordinator(boundary=10)
    coordinator._on_request = lambda request, _channel: JoinRetry(
        attempt=request.attempt,
        retry_after_secs=0.0,
        headroom_events=0,
        headroom_bytes=0,
    )
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
        )
        assert lane.setup() is None
        for _ in range(3):
            retry = lane.join(
                coordinator,
                GroupChannel(
                    group='serial',
                    ordering='eager',
                    max_events=10,
                    max_bytes=1_000_000,
                ),
                catch_up=lambda _boundary: None,
            )
            assert isinstance(retry, JoinRetry)
        lane.close()

    tokens = [request.attempt.attempt_token for request in coordinator._requests]
    assert all(tokens)
    assert len(set(tokens)) == 3
    assert vars(lane)['_used_tokens'] is None


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_eager_late_event_deduplicates_until_complete_post_close_scan(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    invoked: list[int] = []
    path = tmp_path / 'group.sqlite3'

    def callback(context: EventContext) -> None:
        invoked.append(_context_event_id(context))

    with _group_state(path) as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution=execution),),
            execution=execution,
        )
        assert lane.setup() is None
        lane.deliver_many_counted((Delivery(_event(20)),))[0]
        obligation = _pending_range(state, late_ids=(15,))
        late = lane.deliver_many_counted(
            (Delivery(_event(15), obligation_id=obligation.obligation_id),)
        )[0]
        lane.close()

    assert late.cursor == 20
    assert late.late_id_count == 0
    assert invoked == [20, 15]

    with _group_state(path) as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution=execution),),
            execution=execution,
        )
        assert lane.setup() is None
        duplicate = lane.deliver_many_counted(
            (Delivery(_event(15), obligation_id=obligation.obligation_id),)
        )[0]
        resolved = lane.scan_obligation(
            obligation,
            tuple(_event(event_id) for event_id in range(11, 16)),
        )
        pending = state.pending_ranges()
        lane.close()

    assert duplicate.cursor == 20
    assert invoked == [20, 15, 11, 12, 13, 14]
    assert resolved.cursor == 20
    assert resolved.late_id_count == 0
    assert resolved.obligation_count == 0
    assert pending == ()


def test_finalized_gate_is_bound_to_exact_attempt_channel(tmp_path: Path) -> None:
    coordinator = _Coordinator(boundary=10)
    current = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    foreign = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    live_event = _event(11)

    def request(request: JoinRequest, channel: GroupChannelInterface) -> JoinTarget:
        channel.activate(provisional=True)
        assert channel.offer(
            EventDelivery(EncodedEvent.from_event_frame(11, encode_event(live_event)))
        )
        return JoinTarget(coordinator._identity(request.attempt))

    coordinator._on_request = request
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: _ATTEMPT_CURRENT,
        )
        assert lane.setup() is None
        lane.join(coordinator, current, catch_up=lambda _boundary: None)

        with pytest.raises(JoinProtocolError, match='gate is closed'):
            lane.drain_live(foreign)
        completed = lane.drain_live(current)
        lane.close()

    assert completed.cursor == 11


def test_finalized_gate_requires_explicit_detach_before_another_join(
    tmp_path: Path,
) -> None:
    tokens = iter((_ATTEMPT_1, _ATTEMPT_2))
    coordinator = _Coordinator(boundary=10)
    first = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    second = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    event = _event(11)

    def request(request: JoinRequest, owned_channel: GroupChannelInterface) -> JoinTarget:
        owned_channel.activate(provisional=True)
        if owned_channel is first:
            assert owned_channel.offer(
                EventDelivery(EncodedEvent.from_event_frame(11, encode_event(event)))
            )
        return JoinTarget(coordinator._identity(request.attempt))

    coordinator._on_request = request
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: next(tokens),
        )
        assert lane.setup() is None
        first_identity = lane.join(coordinator, first, catch_up=lambda _boundary: None)
        assert isinstance(first_identity, LiveJoinIdentity)

        with pytest.raises(JoinProtocolError, match='finalized'):
            lane.join(coordinator, second, catch_up=lambda _boundary: None)
        completed = lane.drain_live(first)
        first.detach('source-aborted')
        with pytest.raises(JoinProtocolError, match='detached'):
            lane.drain_live(first)
        coordinator._boundary = 11
        second_identity = lane.join(coordinator, second, catch_up=lambda _boundary: None)
        lane.close()

    assert completed.cursor == 11
    assert isinstance(second_identity, LiveJoinIdentity)
    assert second_identity.attempt_token == _ATTEMPT_2


def test_group_lane_health_and_watch_phase_share_supplied_watchdog(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    watchdog = ComponentWatchdog(
        kind='lane',
        identity='serial',
        worker_instance='worker-1',
        registration_token=_WATCHDOG_IDENTITY,
        startup_deadline_secs=30.0,
        clock=clock,
    )
    watchdog.transition('idle', deadline_at=None)
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            clock=clock,
            watchdog=watchdog,
        )

        with lane.watch_phase('internal'):
            assert watchdog.sample().phase == 'internal'
            assert lane.health().phase == 'internal'
        lane.close()

    assert watchdog.sample().phase == 'idle'


def test_injected_token_factory_still_rejects_reuse(tmp_path: Path) -> None:
    coordinator = _Coordinator(boundary=10)
    coordinator._on_request = lambda request, _channel: JoinRetry(
        attempt=request.attempt,
        retry_after_secs=0.0,
        headroom_events=0,
        headroom_bytes=0,
    )
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: _ATTEMPT_1,
        )
        assert lane.setup() is None
        retry = lane.join(
            coordinator,
            GroupChannel(
                group='serial',
                ordering='eager',
                max_events=10,
                max_bytes=1_000_000,
            ),
            catch_up=lambda _boundary: None,
        )
        assert isinstance(retry, JoinRetry)
        with pytest.raises(
            JoinProtocolError,
            match='join attempt token is empty or reused',
        ):
            lane.join(
                coordinator,
                GroupChannel(
                    group='serial',
                    ordering='eager',
                    max_events=10,
                    max_bytes=1_000_000,
                ),
                catch_up=lambda _boundary: None,
            )
        lane.close()


def test_internal_stall_is_visible_at_the_derived_deadline(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    observations: list[LaneHealth] = []
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            clock=clock,
            token_factory=lambda: _ATTEMPT_1,
        )
        assert lane.setup() is None

        def catch_up(_boundary: int) -> None:
            with lane.watch_phase('internal'):
                clock._advance(32.0)
                observations.append(lane.health())

        lane.join(coordinator, channel, catch_up=catch_up)
        lane.close()

    assert observations[0].overdue_reason == 'internal stall'
    assert observations[0].deadline_at == 32.0


def test_invoke_sync_calls_directly_and_does_not_catch_base_exceptions() -> None:
    contexts: list[object] = []
    context = cast('EventContext', object())
    callback = _callback(0, 'callback', contexts.append)

    def interrupt(_context: EventContext) -> None:
        raise KeyboardInterrupt

    _invoke_sync(callback, context)

    assert contexts == [context]
    interrupting = replace(
        callback,
        callback=interrupt,
    )
    with pytest.raises(KeyboardInterrupt):
        _invoke_sync(interrupting, context)


def test_invoke_sync_rejects_and_closes_native_awaitable_result() -> None:
    returned: list[object] = []
    context = cast('EventContext', object())

    async def _awaitable_result() -> None:
        pass

    def callback(_context: EventContext) -> object:
        result = _awaitable_result()
        returned.append(result)
        return result

    configured = _callback(0, 'awaitable-result', callback)

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter('always')
        with pytest.raises(TypeError, match='awaitable-result'):
            _invoke_sync(configured, context)
        coroutine = returned.pop()
        assert inspect.getcoroutinestate(cast('Coroutine[object, object, None]', coroutine)) == (
            inspect.CORO_CLOSED
        )
        del coroutine
        gc.collect()

    assert captured == []


def test_join_aborts_if_catchup_does_not_reach_boundary(
    tmp_path: Path,
) -> None:
    coordinator = _Coordinator(boundary=11)
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: _ATTEMPT_1,
        )
        assert lane.setup() is None
        with pytest.raises(JoinProtocolError, match='catch-up did not reach'):
            lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        snapshot = state.snapshot()
        lane.close()

    assert coordinator._confirms == []
    assert len(coordinator._aborts) == 1
    assert snapshot.latest_join_boundary is None
    assert not channel.status.attached


def test_join_channel_loss_after_group_commit_aborts_source_not_group(
    tmp_path: Path,
) -> None:
    tokens = iter((_ATTEMPT_1, _ATTEMPT_2))
    invoked: list[int] = []
    source_finalized: list[LiveJoinIdentity] = []
    coordinator = _Coordinator(boundary=11)
    live_event = _event(12)

    def request(
        request: JoinRequest,
        owned_channel: GroupChannelInterface,
    ) -> JoinTarget:
        owned_channel.activate(provisional=True)
        assert owned_channel.offer(
            EventDelivery(EncodedEvent.from_event_frame(12, encode_event(live_event)))
        )
        return JoinTarget(coordinator._identity(request.attempt))

    def finalize(finalization: JoinFinalize) -> JoinFinalized:
        source_finalized.append(finalization.identity)
        raise ConnectionError('channel lost')

    coordinator._on_request = request
    coordinator._on_finalize = finalize
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    recovered_channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: invoked.append(_context_event_id(context)),
                ),
            ),
            token_factory=lambda: next(tokens),
        )
        assert lane.setup() is None

        def catch_up(boundary: int) -> None:
            if state.snapshot().cursor < boundary:
                lane.deliver_many_counted((Delivery(_event(boundary)),))[0]

        with pytest.raises(ConnectionError, match='channel lost'):
            lane.join(coordinator, channel, catch_up=catch_up)
        committed = state.snapshot()
        coordinator._on_finalize = None
        recovered = lane.join(
            coordinator,
            recovered_channel,
            catch_up=catch_up,
        )
        completed = lane.drain_live(recovered_channel)
        lane.close()

    assert committed.cursor == 11
    assert committed.latest_join_boundary == 11
    assert isinstance(recovered, LiveJoinIdentity)
    assert recovered.attempt_token == _ATTEMPT_2
    assert source_finalized == [coordinator._finalizes[0].identity]
    assert invoked == [11, 12]
    assert completed.cursor == 12
    assert len(coordinator._aborts) == 1
    assert not channel.status.attached


def test_join_ignores_old_token_replies_without_releasing_new_queue(
    tmp_path: Path,
) -> None:
    coordinator = _Coordinator(boundary=10)
    old = LiveJoinIdentity(
        site='alpha',
        source='events',
        group='serial',
        join_kind='eager',
        source_generation='source-1',
        group_generation=1,
        boundary=10,
        attempt_token=_ATTEMPT_OLD,
    )
    confirm_count = 0
    finalize_count = 0

    def confirm(confirmation: JoinConfirm) -> Attached:
        nonlocal confirm_count
        confirm_count += 1
        return Attached(old if confirm_count == 1 else confirmation.identity)

    def finalize(finalization: JoinFinalize) -> JoinFinalized:
        nonlocal finalize_count
        finalize_count += 1
        return JoinFinalized(old if finalize_count == 1 else finalization.identity)

    coordinator._on_confirm = confirm
    coordinator._on_finalize = finalize
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: _ATTEMPT_CURRENT,
        )
        assert lane.setup() is None
        joined = lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        lane.close()

    assert isinstance(joined, LiveJoinIdentity)
    assert joined.attempt_token == _ATTEMPT_CURRENT
    assert confirm_count == 2
    assert finalize_count == 2
    assert coordinator._aborts == []


def test_join_ignores_old_token_target_and_resends_current_attempt(
    tmp_path: Path,
) -> None:
    coordinator = _Coordinator(boundary=10)
    old = LiveJoinIdentity(
        site='alpha',
        source='events',
        group='serial',
        join_kind='eager',
        source_generation='source-1',
        group_generation=1,
        boundary=10,
        attempt_token=_ATTEMPT_OLD,
    )
    requests = 0

    def request(request: JoinRequest, channel: GroupChannelInterface) -> JoinTarget:
        nonlocal requests
        requests += 1
        channel.activate(provisional=True)
        if requests == 1:
            return JoinTarget(old)
        return JoinTarget(coordinator._identity(request.attempt))

    coordinator._on_request = request
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: _ATTEMPT_CURRENT,
        )
        assert lane.setup() is None
        joined = lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        lane.close()

    assert isinstance(joined, LiveJoinIdentity)
    assert joined.attempt_token == _ATTEMPT_CURRENT
    assert requests == 2
    assert coordinator._requests[0] == coordinator._requests[1]
    assert coordinator._aborts == []


def test_join_rejection_and_restart_each_mint_fresh_token(
    tmp_path: Path,
) -> None:
    tokens = iter((_ATTEMPT_REJECTED, _ATTEMPT_ACCEPTED, _ATTEMPT_RESTARTED))
    path = tmp_path / 'group.sqlite3'
    coordinator = _Coordinator(boundary=10)
    rejected = True

    def confirm(confirmation: JoinConfirm) -> Attached:
        nonlocal rejected
        if rejected:
            rejected = False
            return Attached(
                replace(
                    confirmation.identity,
                    source_generation='source-stale',
                )
            )
        return Attached(confirmation.identity)

    coordinator._on_confirm = confirm
    with _group_state(path) as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: next(tokens),
        )
        assert lane.setup() is None
        with pytest.raises(JoinProtocolError, match='identity conflicts'):
            lane.join(
                coordinator,
                GroupChannel(
                    group='serial',
                    ordering='eager',
                    max_events=10,
                    max_bytes=1_000_000,
                ),
                catch_up=lambda _boundary: None,
            )
        accepted = lane.join(
            coordinator,
            GroupChannel(
                group='serial',
                ordering='eager',
                max_events=10,
                max_bytes=1_000_000,
            ),
            catch_up=lambda _boundary: None,
        )
        lane.close()

    with _group_state(path) as state:
        restarted_lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: next(tokens),
        )
        assert restarted_lane.setup() is None
        restarted = restarted_lane.join(
            coordinator,
            GroupChannel(
                group='serial',
                ordering='eager',
                max_events=10,
                max_bytes=1_000_000,
            ),
            catch_up=lambda _boundary: None,
        )
        restarted_lane.close()

    assert isinstance(accepted, LiveJoinIdentity)
    assert isinstance(restarted, LiveJoinIdentity)
    assert [request.attempt.attempt_token for request in coordinator._requests] == [
        _ATTEMPT_REJECTED,
        _ATTEMPT_ACCEPTED,
        _ATTEMPT_RESTARTED,
    ]


def test_join_rejects_stale_revision_after_confirm_with_queued_live_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    live_event = _event(12)

    def request(request: JoinRequest, owned_channel: GroupChannelInterface) -> JoinTarget:
        owned_channel.activate(provisional=True)
        assert owned_channel.offer(
            EventDelivery(EncodedEvent.from_event_frame(12, encode_event(live_event)))
        )
        return JoinTarget(coordinator._identity(request.attempt))

    coordinator._on_request = request
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: _ATTEMPT_1,
        )
        assert lane.setup() is None
        preflight_revisions: list[int] = []
        validate_live_join = state.validate_live_join

        def validate(
            identity: LiveJoinIdentity,
            expected_revision: int,
        ) -> GroupSnapshot:
            preflight_revisions.append(expected_revision)
            return validate_live_join(identity, expected_revision)

        monkeypatch.setattr(state, 'validate_live_join', validate)

        def confirm(confirmation: JoinConfirm) -> Attached:
            state.advance_irrelevant(11, expected_revision=state.snapshot().revision)
            return Attached(confirmation.identity)

        coordinator._on_confirm = confirm
        with pytest.raises(JoinProtocolError, match='expected group revision 0'):
            lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        after_rejection = state.snapshot()
        lane.close()

    assert preflight_revisions == [0, 0]
    assert len(coordinator._confirms) == 1
    assert coordinator._finalizes == []
    assert after_rejection.revision == 1
    assert after_rejection.cursor == 11
    assert after_rejection.obligation_count == 0


def test_lane_cleanup_releases_thread_when_lane_is_published_late(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    race = _AsyncCloseRaceState()
    releases: list[asyncio.Future[None]] = []
    original_lane = _lane

    def delayed_lane(*args: object, **kwargs: object) -> tuple[GroupLane, _HandleFactory]:
        race.cleanup_requested.wait(timeout=0.05)
        factory = cast('Callable[..., tuple[GroupLane, _HandleFactory]]', original_lane)
        return factory(*args, **kwargs)

    async def callback(_context: EventContext) -> None:
        release = asyncio.get_running_loop().create_future()
        releases.append(release)
        if race.cleanup_requested.is_set():
            return
        await release

    monkeypatch.setitem(globals(), '_lane', delayed_lane)
    thread = threading.Thread(
        target=_run_async_close_race,
        args=(tmp_path / 'late-lane.sqlite3', callback, race),
    )
    thread.start()
    try:
        _release_lane_thread(
            thread,
            _LaneThreadCleanup(
                holder=race._holder,
                cleanup_requested=race.cleanup_requested,
                futures=releases,
            ),
        )
    finally:
        race.cleanup_requested.set()
        for release in releases:
            if not release.done():
                release.get_loop().call_soon_threadsafe(release.set_result, None)
        if race._holder:
            race._holder[0].request_shutdown()
        thread.join(timeout=2.0)

    assert race._holder
    assert not thread.is_alive()


@pytest.mark.parametrize(
    'diagnostic',
    ['late event does not have an exact reservation', 'late id is not pending'],
)
def test_late_delivery_propagates_unrelated_invalid_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    diagnostic: str,
) -> None:
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None, event_type='Other'),),
        )
        assert lane.setup() is None
        obligation = _pending_range(state, closed=False)

        def invalid_complete(
            _obligation_id: str,
            _event_id: int,
            *,
            expected_revision: int,
        ) -> GroupSnapshot:
            assert state.snapshot().revision == expected_revision
            raise InvalidTransitionError(diagnostic)

        monkeypatch.setattr(state, 'complete_late_id', invalid_complete)
        try:
            with pytest.raises(InvalidTransitionError, match=diagnostic):
                lane.deliver_many_counted(
                    (Delivery(_event(15), obligation_id=obligation.obligation_id),)
                )[0]
        finally:
            lane.close()


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_event_unavailable_retry_refetches_without_worker_restart(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    invoked: list[int] = []
    event = _event(11)
    backend = _Backend(
        _events={},
        _exact_reads={11: [None, None]},
        _retention_floor=1,
    )
    path = tmp_path / 'group.sqlite3'

    with _group_state(path) as state:
        callback = _callback(
            0,
            'callback',
            lambda context: invoked.append(_context_event_id(context)),
            execution=execution,
        )
        state.start_event(
            11,
            CallbackManifest(callbacks=(callback.identity,)),
            expected_revision=0,
        )
        lane, factory = _lane(
            state,
            (callback,),
            backend=backend,
            execution=execution,
            wait=lambda _delay: False,
        )
        assert lane.setup() is None
        unavailable = lane.resume()
        assert unavailable.status == 'event-unavailable'
        assert lane.health().status == 'event-unavailable'

        backend._events[11] = event
        retried = state.retry(
            event_id=11,
            callback_ordinal=None,
            expected_revision=unavailable.revision,
            **_operator(),
        )
        assert retried.retry_event_id == 11
        completed = lane.resume()
        lane.close()

    assert invoked == [11]
    assert completed.cursor == 11
    assert completed.in_flight_event_id is None
    assert len(factory._handles) == 2


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_filtered_event_manifest_uses_event_local_callback_order(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    def fail(_context: object) -> None:
        raise RuntimeError('stop')

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'ignored',
                    lambda _context: None,
                    event_type='Shotgun_Ignore',
                    execution=execution,
                ),
                _callback(
                    1,
                    'matching',
                    fail,
                    event_type='Shotgun_Match',
                    execution=execution,
                ),
            ),
            execution=execution,
        )
        assert lane.setup() is None

        snapshot = lane.deliver_many_counted((Delivery(_event(11, event_type='Shotgun_Match')),))[
            0
        ]
        lane.close()

    assert snapshot.manifest is not None
    assert tuple(
        (identity.ordinal, identity.callback_id) for identity in snapshot.manifest.callbacks
    ) == ((0, 'matching'),)


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_five_hundred_irrelevant_events_use_one_cursor_transaction(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'matching',
                    lambda _context: None,
                    event_type='Shotgun_Match',
                    execution=execution,
                ),
            ),
            execution=execution,
        )
        assert lane.setup() is None

        snapshot, consumed = lane.deliver_many_counted(
            tuple(
                Delivery(_event(event_id, event_type='Shotgun_Ignore'))
                for event_id in range(11, 511)
            )
        )
        lane.close()

    assert snapshot.cursor == 510
    assert snapshot.revision == 1
    assert consumed == 500


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_blocked_callback_publishes_exact_identity_and_sliding_heartbeat(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    callback_entered = threading.Event()
    release_callback = threading.Event()
    retained: list[EventContext] = []
    watchdog = ComponentWatchdog(
        kind='lane',
        identity='serial',
        worker_instance='worker-1',
        registration_token=_WATCHDOG_IDENTITY,
        startup_deadline_secs=30.0,
    )
    watchdog.transition('idle', deadline_at=None)

    def callback(context: EventContext) -> None:
        retained.append(context)
        callback_entered.set()
        assert release_callback.wait(1.0)

    def run() -> None:
        with _group_state(tmp_path / 'group.sqlite3') as state:
            lane, _factory = _lane(
                state,
                (_callback(0, 'blocked', callback, execution=execution),),
                callback_deadline_secs=10.0,
                execution=execution,
                watchdog=watchdog,
            )
            assert lane.setup() is None
            lane.deliver_many_counted((Delivery(_event(11)),))[0]
            lane.close()

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert callback_entered.wait(1.0)
        before = watchdog.sample()
        retained[0].heartbeat()
        after = watchdog.sample()
    finally:
        release_callback.set()
        thread.join(2.0)

    assert not thread.is_alive()
    assert (
        before.phase,
        before.event_id,
        before.plugin_id,
        before.callback_id,
        before.delivery_attempt,
    ) == ('callback', 11, 'plugin', 'blocked', 1)
    assert before.deadline_at is not None
    assert after.phase == 'callback'
    assert after.heartbeat_at >= before.heartbeat_at
    assert after.deadline_at is not None
    assert after.deadline_at >= before.deadline_at


@pytest.mark.parametrize(
    'case',
    [
        _DeadlineCase(3.0, 1.0, 1, 0.0, 30.0),
        _DeadlineCase(2.0, 4.0, 3, 2.0, 32.0),
    ],
)
def test_internal_stall_deadline_uses_doubled_larger_budget(
    tmp_path: Path,
    case: _DeadlineCase,
) -> None:
    with _group_state(tmp_path / f'{case._expected}.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            credential_attempts=case._attempts,
            credential_interval_secs=case._retry_interval_secs,
            credential_timeout_secs=case._timeout_secs,
            health_interval_secs=case._health_interval_secs,
        )

        health = lane.health()
        lane.close()

    assert isinstance(health, LaneHealth)
    assert health.internal_stall_deadline_secs == case._expected


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_irrelevant_runs_batch_around_one_matching_event(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    invoked: list[int] = []

    def callback(context: EventContext) -> None:
        invoked.append(_context_event_id(context))

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'matching',
                    callback,
                    event_type='Shotgun_Match',
                    execution=execution,
                ),
            ),
            execution=execution,
        )
        assert lane.setup() is None

        snapshot = lane.deliver_many_counted(
            (
                Delivery(_event(11, event_type='Shotgun_Ignore')),
                Delivery(_event(12, event_type='Shotgun_Ignore')),
                Delivery(_event(13, event_type='Shotgun_Match')),
                Delivery(_event(14, event_type='Shotgun_Ignore')),
                Delivery(_event(15, event_type='Shotgun_Ignore')),
            )
        )[0]
        lane.close()

    assert invoked == [13]
    assert snapshot.cursor == 15
    assert snapshot.revision == 5


@pytest.mark.parametrize('failure_boundary', ['request', 'catch-up', 'confirm', 'finalize'])
def test_join_base_exception_cleanup_is_exact_and_preserves_original(
    failure_boundary: str,
    tmp_path: Path,
) -> None:
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    failure = KeyboardInterrupt(failure_boundary)

    def request(request: JoinRequest, owned_channel: GroupChannelInterface) -> JoinTarget:
        owned_channel.activate(provisional=True)
        if failure_boundary == 'request':
            raise failure
        return JoinTarget(coordinator._identity(request.attempt))

    def catch_up(_boundary: int) -> None:
        if failure_boundary == 'catch-up':
            raise failure

    def confirm(confirmation: JoinConfirm) -> Attached:
        if failure_boundary == 'confirm':
            raise failure
        return Attached(confirmation.identity)

    def finalize(finalization: JoinFinalize) -> JoinFinalized:
        if failure_boundary == 'finalize':
            raise failure
        return JoinFinalized(finalization.identity)

    coordinator._on_request = request
    coordinator._on_confirm = confirm
    coordinator._on_finalize = finalize
    with _group_state(tmp_path / f'{failure_boundary}.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: _ATTEMPT_1,
        )
        assert lane.setup() is None
        with pytest.raises(KeyboardInterrupt) as raised:
            lane.join(coordinator, channel, catch_up=catch_up)
        lane.close()

    assert raised.value is failure
    assert not channel.status.attached
    if failure_boundary == 'request':
        assert coordinator._aborts == []
    else:
        assert len(coordinator._aborts) == 1
        assert coordinator._aborts[0].identity.attempt_token == _ATTEMPT_1


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('stopped', [False, True])
def test_join_owner_write_failure_detaches_without_abort_or_outcome_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    stopped: bool,
) -> None:
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(group='serial', ordering=ordering, max_events=10, max_bytes=1000)
    failure = (
        WriteLockStoppedError('owner stopped')
        if stopped
        else WriteLockTimeoutError('owner wait expired', 'file')
    )
    path = tmp_path / 'source.db'
    with _group_state(path, ordering=ordering) as state:
        if ordering == 'strict':
            state.record_strict_join_outcome(
                attempt_token=_ATTEMPT_OLD,
                capacity_caused=True,
                abort_limit=JOIN_ABORT_LIMIT,
                expected_revision=state.snapshot().revision,
            )
        lane, _factory = _lane(state, (), ordering=ordering, token_factory=lambda: _ATTEMPT_1)
        assert lane.setup() is None
        before = state.snapshot()
        outcomes: list[str] = []

        def fail_write(_identity: LiveJoinIdentity, expected_revision: int) -> GroupSnapshot:
            assert expected_revision == before.revision
            raise failure

        def record_outcome(**_options: object) -> GroupSnapshot:
            outcomes.append('unexpected additional write')
            return state.snapshot()

        monkeypatch.setattr(state, 'commit_live_join', fail_write)
        monkeypatch.setattr(state, 'record_strict_join_outcome', record_outcome)
        with pytest.raises(type(failure)) as raised:
            lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        assert raised.value is failure
        assert not channel.status.attached
        assert coordinator._aborts == []
        assert outcomes == []
        assert state.snapshot() == before
        lane.close()
    with _group_state(path, ordering=ordering) as reopened:
        assert reopened.snapshot() == before


@pytest.mark.parametrize('stopped', [False, True])
def test_join_owner_outcome_failure_preserves_committed_strict_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stopped: bool
) -> None:
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(group='serial', ordering='strict', max_events=10, max_bytes=1000)
    failure = (
        WriteLockStoppedError('owner stopped')
        if stopped
        else WriteLockTimeoutError('owner wait expired', 'file')
    )
    path = tmp_path / 'source.db'
    with _group_state(path, ordering='strict') as state:
        state.record_strict_join_outcome(
            attempt_token=_ATTEMPT_OLD,
            capacity_caused=True,
            abort_limit=JOIN_ABORT_LIMIT,
            expected_revision=state.snapshot().revision,
        )
        lane, _factory = _lane(state, (), ordering='strict', token_factory=lambda: _ATTEMPT_1)
        assert lane.setup() is None
        before_failure: list[GroupSnapshot] = []

        def fail_outcome(**_options: object) -> GroupSnapshot:
            before_failure.append(state.snapshot())
            raise failure

        monkeypatch.setattr(state, 'record_strict_join_outcome', fail_outcome)
        with pytest.raises(type(failure)) as raised:
            lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        assert raised.value is failure
        assert not channel.status.attached
        assert coordinator._aborts == []
        assert len(before_failure) == 1
        committed = before_failure[0]
        assert state.snapshot() == committed
        assert committed.latest_join_boundary == 10
        assert committed.strict_capacity_abort_streak == 1
        assert committed.strict_capacity_attempt_token == _ATTEMPT_1
        lane.close()
    with _group_state(path, ordering='strict') as reopened:
        assert reopened.snapshot() == committed


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_join_keeps_live_gate_closed_through_exact_finalize(
    tmp_path: Path,
    ordering: Literal['eager', 'strict'],
) -> None:
    actions: list[str] = []
    live_event = _event(11)
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(
        group='serial',
        ordering=ordering,
        max_events=10,
        max_bytes=1_000_000,
    )

    def request(
        request: JoinRequest,
        owned_channel: GroupChannelInterface,
    ) -> JoinTarget:
        actions.append('request')
        owned_channel.activate(provisional=True)
        encoded = EncodedEvent.from_event_frame(11, encode_event(live_event))
        assert owned_channel.offer(EventDelivery(encoded))
        return JoinTarget(coordinator._identity(request.attempt))

    coordinator._on_request = request
    coordinator._on_confirm = lambda confirmation: (
        actions.append('confirm') or Attached(confirmation.identity)
    )
    coordinator._on_finalize = lambda finalization: (
        actions.append('finalize') or JoinFinalized(finalization.identity)
    )

    with _group_state(
        tmp_path / 'group.sqlite3',
        ordering=ordering,
    ) as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda _context: actions.append('callback'),
                ),
            ),
            ordering=ordering,
            setup=lambda _context: actions.append('setup'),
            token_factory=lambda: _ATTEMPT_1,
        )
        assert lane.setup() is None
        identity = lane.join(
            coordinator,
            channel,
            catch_up=lambda boundary: actions.append(f'catch-up:{boundary}'),
        )
        before_drain = state.snapshot()
        assert 'callback' not in actions
        completed = lane.drain_live(channel)
        lane.close()

    assert isinstance(identity, LiveJoinIdentity)
    assert identity.attempt_token == _ATTEMPT_1
    assert before_drain.latest_join_boundary == 10
    assert actions == [
        'setup',
        'request',
        'catch-up:10',
        'confirm',
        'finalize',
        'callback',
    ]
    assert completed.cursor == 11
    assert coordinator._confirms == [JoinConfirm(identity)]
    assert coordinator._finalizes == [JoinFinalize(identity)]
    assert coordinator._aborts == []


@pytest.mark.parametrize(
    ('field', 'replacement'),
    [
        ('source_generation', 'source-old'),
        ('group_generation', 0),
        ('boundary', 9),
        ('join_kind', 'strict'),
    ],
)
def test_join_rejects_same_token_identity_mismatch_before_group_mutation(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    coordinator = _Coordinator(boundary=10)
    coordinator._on_confirm = lambda confirmation: Attached(
        replace(confirmation.identity, **{field: replacement})
    )
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    with _group_state(tmp_path / f'{field}.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: _ATTEMPT_CURRENT,
        )
        assert lane.setup() is None
        with pytest.raises(JoinProtocolError, match='identity conflicts'):
            lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        snapshot = state.snapshot()
        lane.close()

    assert snapshot.latest_join_boundary is None
    assert len(coordinator._aborts) == 1
    assert coordinator._aborts[0].identity.attempt_token == _ATTEMPT_CURRENT
    assert not channel.status.attached


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_lane_close_retains_first_refetch_error_through_complete_cleanup(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    event = _event(11)
    callback = _callback(0, 'callback', lambda _context: None, execution=execution)
    backend = _Backend(_events={11: event}, _exact_reads={})

    with _group_state(tmp_path / 'group.sqlite3') as state:
        state.start_event(
            11,
            CallbackManifest(callbacks=(callback.identity,)),
            expected_revision=0,
        )
        lane, factory = _lane(
            state,
            (callback,),
            backend=backend,
            execution=execution,
            extra_plugin_count=1,
        )
        assert lane.setup() is None
        assert lane.resume().cursor == 11
        handles = tuple(factory._handles)
        assert len(handles) == 3
        refetch_handle = cast('_Handle', handles[2])
        first_error = KeyboardInterrupt('refetch close failure')
        refetch_handle._close_error = first_error
        handles[1]._close_error = SystemExit('later plugin close failure')
        loop = cast('_AsyncHandle', handles[0])._created_loop if execution == 'async' else None

        with pytest.raises((KeyboardInterrupt, SystemExit)) as raised:
            lane.close()

        cleanup_attempts = tuple(factory._cleanup_attempts)
        lane_state = vars(lane)
        refetch_ownership_cleared = lane_state['_refetch_handle'] is None
        host_ownership_cleared = not vars(lane_state['_host'])['_handles']
        loop_closed = loop is None or loop.is_closed()
        loop_ownership_cleared = lane_state['_loop'] is None
        task_markers = (
            lane_state['_async_cancel_task'],
            lane_state['_async_cancel_timer'],
            lane_state['_current_async_task'],
            lane_state['_daemon_cancelled_task'],
        )
        if not (
            refetch_ownership_cleared
            and host_ownership_cleared
            and loop_closed
            and loop_ownership_cleared
        ):
            for handle in handles:
                handle._close_error = None
            lane.close()

    assert raised.value is first_error
    assert cleanup_attempts == (2, 1, 0)
    assert refetch_ownership_cleared
    assert host_ownership_cleared
    assert loop_closed
    assert loop_ownership_cleared
    assert task_markers == (None, None, None, None)


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_late_id_completion_releases_capacity_for_next_callback(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    invoked: list[int] = []

    with _group_state(
        tmp_path / 'group.sqlite3',
        max_late_ids_per_group=1,
    ) as state:
        obligation = _pending_range(state, late_ids=(12,))
        state.reserve_late_event(
            obligation.obligation_id,
            12,
            expected_revision=state.snapshot().revision,
        )
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: invoked.append(_context_event_id(context)),
                    execution=execution,
                ),
            ),
            execution=execution,
        )
        assert lane.setup() is None
        first = lane.deliver_many_counted(
            (Delivery(_event(12), obligation_id=obligation.obligation_id),)
        )[0]
        second = lane.deliver_many_counted(
            (Delivery(_event(13), obligation_id=obligation.obligation_id),)
        )[0]
        lane.close()

    assert first.status == 'running'
    assert invoked == [12, 13]
    assert second.status == 'running'
    assert 'late-capacity-exhausted' not in second.conditions
    assert second.in_flight_event_id is None


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_late_id_capacity_is_preflighted_before_callback_side_effect(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    invoked: list[int] = []

    with _group_state(
        tmp_path / 'group.sqlite3',
        max_late_ids_per_group=1,
    ) as state:
        obligation = _pending_range(state, late_ids=(12,))
        state.reserve_late_event(
            obligation.obligation_id,
            12,
            expected_revision=state.snapshot().revision,
        )
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: invoked.append(_context_event_id(context)),
                    execution=execution,
                ),
            ),
            execution=execution,
        )
        assert lane.setup() is None
        blocked = lane.deliver_many_counted(
            (Delivery(_event(13), obligation_id=obligation.obligation_id),)
        )[0]
        lane.close()

    assert invoked == []
    assert blocked.status == 'late-capacity-exhausted'
    assert 'late-capacity-exhausted' in blocked.conditions
    assert blocked.in_flight_event_id is None
    assert blocked.late_id_count == 1


def test_live_drain_detects_capacity_detach_before_processing(
    tmp_path: Path,
) -> None:
    tokens = iter((_ATTEMPT_1, _ATTEMPT_2))
    coordinator = _Coordinator(boundary=10)
    detached = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=1,
        max_bytes=1_000_000,
    )
    recovered = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    invoked: list[int] = []
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: invoked.append(_context_event_id(context)),
                ),
            ),
            token_factory=lambda: next(tokens),
        )
        assert lane.setup() is None
        first_identity = lane.join(
            coordinator,
            detached,
            catch_up=lambda _boundary: None,
        )
        assert isinstance(first_identity, LiveJoinIdentity)
        assert detached.offer(
            EventDelivery(EncodedEvent.from_event_frame(11, encode_event(_event(11))))
        )
        assert not detached.offer(
            EventDelivery(EncodedEvent.from_event_frame(12, encode_event(_event(12))))
        )

        with pytest.raises(JoinProtocolError, match='detached'):
            lane.drain_live(detached)
        second_identity = lane.join(
            coordinator,
            recovered,
            catch_up=lambda _boundary: None,
        )
        assert isinstance(second_identity, LiveJoinIdentity)
        with pytest.raises(JoinProtocolError, match='gate is closed'):
            lane.drain_live(detached)
        lane.close()

    assert invoked == []
    assert first_identity.attempt_token == _ATTEMPT_1
    assert second_identity.attempt_token == _ATTEMPT_2
    assert recovered.status.attached


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_live_drain_retains_unprocessed_tail_across_callback_retry(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    attempts: list[int] = []
    first = _event(11)
    second = _event(12)
    backend = _Backend(_events={11: first, 12: second}, _exact_reads={})
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )

    def callback(context: EventContext) -> None:
        event_id = _context_event_id(context)
        attempts.append(event_id)
        if event_id == 11 and attempts.count(11) == 1:
            raise RuntimeError('retry the first event')

    def request(request: JoinRequest, owned_channel: GroupChannelInterface) -> JoinTarget:
        owned_channel.activate(provisional=True)
        for event in (first, second):
            assert owned_channel.offer(
                EventDelivery(
                    EncodedEvent.from_event_frame(
                        cast('int', event['id']),
                        encode_event(event),
                    )
                )
            )
        return JoinTarget(coordinator._identity(request.attempt))

    coordinator._on_request = request
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution=execution),),
            backend=backend,
            execution=execution,
            token_factory=lambda: _ATTEMPT_1,
        )
        assert lane.setup() is None
        lane.join(coordinator, channel, catch_up=lambda _boundary: None)

        failed = lane.drain_live(channel)
        state.retry(
            event_id=11,
            callback_ordinal=0,
            expected_revision=failed.revision,
            **_operator(),
        )
        resumed = lane.resume()
        completed = lane.drain_live(channel)
        lane.close()

    assert failed.status == 'callback-failed'
    assert resumed.cursor == 11
    assert attempts == [11, 11, 12]
    assert completed.cursor == 12


def test_live_idle_snapshot_handoff_reads_fresh_state_before_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    with _group_state(tmp_path / 'idle-handoff.db') as state:
        lane, _factory = _lane(state, ())
        assert lane.setup() is None
        lane.join(_Coordinator(boundary=10), channel, catch_up=lambda _boundary: None)
        gate_snapshot = state.snapshot()
        snapshots = 0
        real_snapshot = state.snapshot

        def counted_snapshot() -> GroupSnapshot:
            nonlocal snapshots
            snapshots += 1
            return real_snapshot()

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        assert lane.drain_live(channel, idle_snapshot=gate_snapshot) == gate_snapshot
        assert snapshots == 0
        fresh = state.advance_irrelevant(11, expected_revision=gate_snapshot.revision)
        assert channel.offer(
            EventDelivery(EncodedEvent.from_event_frame(12, encode_event(_event(12))))
        )
        delivered = lane.drain_live(channel, idle_snapshot=gate_snapshot)
        assert snapshots == 1
        assert delivered.cursor == 12
        assert delivered.revision > fresh.revision
        assert lane.drain_live(channel).cursor == 12
        assert snapshots == 2
        lane.close()


def test_live_irrelevant_batches_commit_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=501,
        max_bytes=10_000_000,
    )
    with _group_state(tmp_path / 'group.db') as state:
        lane, _factory = _lane(state, ())
        assert lane.setup() is None
        lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        for event_id in range(11, 511):
            assert channel.offer(
                EventDelivery(
                    EncodedEvent.from_event_frame(
                        event_id,
                        encode_event(_event(event_id)),
                    )
                )
            )
        snapshot_reads = 0
        read_snapshot = state.snapshot

        def snapshot() -> GroupSnapshot:
            nonlocal snapshot_reads
            snapshot_reads += 1
            return read_snapshot()

        monkeypatch.setattr(state, 'snapshot', snapshot)
        statements: list[str] = []
        state._database.connection.set_trace_callback(statements.append)
        completed = lane.drain_live(channel)
        state._database.connection.set_trace_callback(None)
        assert snapshot_reads == 1
        state.advance_irrelevant(511, expected_revision=completed.revision)
        assert lane.drain_live(channel).cursor == 511
        assert snapshot_reads == 2
        lane.close()

    assert completed.cursor == 510
    assert statements.count('COMMIT') == 1


def test_live_strict_window_capacity_halt_is_durable_before_detach(
    tmp_path: Path,
) -> None:
    group_path = tmp_path / 'source.db'
    first = EncodedEvent.from_event_frame(1, encode_event(_event(1)))
    second = EncodedEvent.from_event_frame(2, encode_event(_event(2)))
    strict = StrictBuffer(
        max_events=1,
        max_bytes=max(first.byte_count, second.byte_count),
        group_holdbacks={'serial': 300.0},
        group_delivery_floors={'serial': 0},
    )
    fanout = EventFanout()
    with _concrete_strict_states(
        group_path,
        initial_position=0,
        source_initial=None,
        delivery_floors={'serial': 0},
        group_holdbacks={'serial': 300.0},
    ) as (source_state, group_state):
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (_callback(0, 'callback', lambda _context: None),),
            fanout=fanout,
        )
        channel = GroupChannel(
            group='serial',
            ordering='strict',
            max_events=10,
            max_bytes=10_000_000,
        )
        joined = lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        assert isinstance(joined, LiveJoinIdentity)
        strict._observe(first, successful_time=0.0)
        strict._observe(second, successful_time=0.0)
        strict.pump_many(tuple(sorted(strict._groups)), 0.0)
        halted = lane.drain_live(channel)
        lane.close()

    assert halted.status == 'strict-capacity-exhausted'
    assert halted.cursor == 0
    assert halted.strict_capacity_abort_streak == JOIN_ABORT_LIMIT
    with _group_state(
        group_path,
        initial_position=0,
        ordering='strict',
    ) as reopened:
        assert reopened.snapshot().status == 'strict-capacity-exhausted'


@pytest.mark.parametrize(
    ('retention_floor', 'expected_status'),
    [
        (12, 'history-truncated'),
        (1, 'event-unavailable'),
    ],
)
@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_missing_in_flight_event_rechecks_then_classifies_against_floor(
    tmp_path: Path,
    retention_floor: int,
    expected_status: str,
    execution: Literal['sync', 'async'],
) -> None:
    waits: list[float] = []
    backend = _Backend(
        _events={},
        _exact_reads={11: [None, None]},
        _retention_floor=retention_floor,
    )
    path = tmp_path / 'group.sqlite3'

    with _group_state(path) as state:
        state.start_event(
            11,
            CallbackManifest(
                callbacks=(
                    _callback(
                        0,
                        'callback',
                        lambda _context: None,
                        execution=execution,
                    ).identity,
                )
            ),
            expected_revision=0,
        )

    with _group_state(path) as state:
        lane, factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda _context: None,
                    execution=execution,
                ),
            ),
            backend=backend,
            execution=execution,
            wait=lambda delay: waits.append(delay) or False,
        )
        assert lane.setup() is None
        snapshot = lane.resume()
        lane.close()

    assert snapshot.status == expected_status
    assert snapshot.cursor == 10
    assert waits == [1.0]
    assert factory._handles[-1]._find_calls == [
        ('EventLogEntry', [['id', 'is', 11]]),
        ('EventLogEntry', [['id', 'is', 11]]),
        ('EventLogEntry', []),
    ]


def test_ordering_anomalies_retain_latest_hundred(tmp_path: Path) -> None:
    with _group_state(
        tmp_path / 'group.db',
        initial_position=150,
        ordering='strict',
    ) as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            ordering='strict',
        )
        assert lane.setup() is None
        delivered = lane.deliver_many_counted((Delivery(_event(200)),))[0]

        for event_id in range(1, 151):
            lane.deliver_many_counted((Delivery(_event(event_id)),))[0]

        anomalies = lane._ordering_anomalies
        lane.close()

    assert delivered.cursor == 200
    assert len(anomalies) == 100
    assert anomalies[0].event_id == 51
    assert anomalies[-1].event_id == 150
    assert tuple(anomaly.event_id for anomaly in anomalies) == tuple(range(51, 151))
    assert all(anomaly.action == 'skip' for anomaly in anomalies)


def test_overdue_write_hold_reports_internal_stall(tmp_path: Path) -> None:
    clock = _Clock()
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            clock=clock,
            token_factory=lambda: _ATTEMPT_1,
        )
        assert lane.setup() is None
        with lane.watch_phase('write-hold', deadline_at=clock() + 5.0):
            clock._advance(5.0)
            health = lane.health()
        lane.close()

    assert health.phase == 'write-hold'
    assert health.overdue_reason == 'internal stall'


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_post_close_scan_routes_unseen_rows_and_halts_before_completion(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    invoked: list[int] = []

    def callback(context: EventContext) -> None:
        event_id = _context_event_id(context)
        invoked.append(event_id)
        if event_id == 12:
            raise RuntimeError('scan callback failed')

    with _group_state(tmp_path / 'group.sqlite3') as state:
        obligation = _pending_range(state)
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution=execution),),
            execution=execution,
        )
        assert lane.setup() is None

        snapshot = lane.scan_obligation(
            obligation,
            (_event(11), _event(12), _event(13)),
        )
        pending = state.pending_ranges()
        lane.close()

    assert invoked == [11, 12]
    assert snapshot.status == 'callback-failed'
    assert snapshot.in_flight_event_id == 12
    assert snapshot.in_flight_obligation_id == obligation.obligation_id
    assert pending == (obligation,)


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_post_close_scan_shutdown_leaves_unseen_rows_pending(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    invoked: list[int] = []
    lane: GroupLane

    def callback(context: EventContext) -> None:
        invoked.append(_context_event_id(context))
        lane.request_shutdown()

    with _group_state(tmp_path / 'group.sqlite3') as state:
        obligation = _pending_range(state)
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution=execution),),
            execution=execution,
        )
        assert lane.setup() is None

        snapshot = lane.scan_obligation(
            obligation,
            (_event(11), _event(12)),
        )
        pending = state.pending_ranges()
        lane.close()

    assert invoked == [11]
    assert snapshot.obligation_count == 1
    assert pending == (obligation,)


def test_post_reserve_capacity_abort_increments_durable_streak(
    tmp_path: Path,
) -> None:
    strict = StrictBuffer(
        max_events=1,
        max_bytes=10_000_000,
        group_holdbacks={'retainer': 300.0, 'serial': 300.0},
    )
    attach_strict_group(strict, 'retainer', 1, holdback_secs=300.0, delivery_floor=0)
    with _concrete_strict_states(
        tmp_path / 'source.db',
        initial_position=1,
        source_initial=1,
    ) as (source_state, group_state):
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (_callback(0, 'callback', lambda _context: None),),
            fanout=EventFanout(),
        )

        def exhaust_after_reserve(_boundary: int) -> None:
            strict._observe(
                EncodedEvent.from_event_frame(2, encode_event(_event(2))),
                successful_time=0.0,
            )
            strict._observe(
                EncodedEvent.from_event_frame(3, encode_event(_event(3))),
                successful_time=0.0,
            )

        response = lane.join(
            coordinator,
            GroupChannel(
                group='serial',
                ordering='strict',
                max_events=10,
                max_bytes=10_000_000,
            ),
            catch_up=exhaust_after_reserve,
        )
        snapshot = group_state.snapshot()
        lane.close()

    assert isinstance(response, JoinRetry)
    assert snapshot.strict_capacity_abort_streak == 1
    assert snapshot.strict_capacity_attempt_token == _ATTEMPT_1


def test_pre_cas_channel_loss_aborts_and_next_attempt_uses_fresh_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tokens = iter((_ATTEMPT_1, _ATTEMPT_2))
    coordinator = _Coordinator(boundary=10)
    first_channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    second_channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', lambda _context: None),),
            token_factory=lambda: next(tokens),
        )
        assert lane.setup() is None
        commit = state.commit_live_join

        def lose_commit(
            _identity: LiveJoinIdentity,
            expected_revision: int,
        ) -> GroupSnapshot:
            raise ConnectionError(f'pre-CAS channel lost at revision {expected_revision}')

        monkeypatch.setattr(
            state,
            'commit_live_join',
            lose_commit,
        )
        with pytest.raises(ConnectionError, match='pre-CAS channel lost'):
            lane.join(coordinator, first_channel, catch_up=lambda _boundary: None)
        before_retry = state.snapshot()

        monkeypatch.setattr(state, 'commit_live_join', commit)
        joined = lane.join(
            coordinator,
            second_channel,
            catch_up=lambda _boundary: None,
        )
        lane.close()

    assert before_retry.latest_join_boundary is None
    assert isinstance(joined, LiveJoinIdentity)
    assert [request.attempt.attempt_token for request in coordinator._requests] == [
        _ATTEMPT_1,
        _ATTEMPT_2,
    ]
    assert coordinator._aborts[0].identity.attempt_token == _ATTEMPT_1
    assert not first_channel.status.attached


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_provider_refetch_uses_internal_watchdog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution: Literal['sync', 'async'],
) -> None:
    clock = _Clock()
    observations: list[LaneHealth] = []
    callback = _callback(0, 'callback', lambda _context: None, execution=execution)
    event = _event(11)
    backend = _Backend(_events={11: event}, _exact_reads={})
    original_find = _Handle.find
    with _group_state(tmp_path / 'group.sqlite3') as state:
        state.start_event(
            11,
            CallbackManifest(callbacks=(callback.identity,)),
            expected_revision=0,
        )
        lane, _factory = _lane(
            state,
            (callback,),
            backend=backend,
            clock=clock,
            execution=execution,
        )

        def find(
            handle: _Handle,
            entity_type: str,
            filters: object,
            fields: list[str] | None = None,
            **kwargs: object,
        ) -> list[dict[str, object]]:
            clock._advance(32.0)
            observations.append(lane.health())
            return original_find(
                handle,
                entity_type,
                filters,
                fields,
                **kwargs,
            )

        monkeypatch.setattr(_Handle, 'find', find)
        assert lane.setup() is None
        lane.resume()
        lane.close()

    assert observations[0].overdue_reason == 'internal stall'
    assert observations[0].deadline_at == 32.0


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_restart_after_done_commit_does_not_refetch_or_reinvoke(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution: Literal['sync', 'async'],
) -> None:
    actions: list[str] = []
    event = _event(11)
    backend = _Backend(_events={11: event}, _exact_reads={})
    path = tmp_path / 'group.sqlite3'

    with _group_state(path) as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda _context: actions.append('callback'),
                    execution=execution,
                ),
            ),
            backend=backend,
            execution=execution,
        )
        assert lane.setup() is None
        callback_done = state.callback_done

        def commit_then_crash(ordinal: int, *, expected_revision: int) -> object:
            callback_done(ordinal, expected_revision=expected_revision)
            raise SystemExit(18)

        monkeypatch.setattr(state, 'callback_done', commit_then_crash)
        with pytest.raises(SystemExit, match='18'):
            lane.deliver_many_counted((Delivery(event),))[0]
        lane.close()

    with _group_state(path) as state:
        lane, factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda _context: actions.append('callback'),
                    execution=execution,
                ),
            ),
            backend=backend,
            execution=execution,
        )
        assert lane.setup() is None
        snapshot = lane.resume()
        lane.close()

    assert actions == ['callback']
    assert snapshot.cursor == 11
    assert all(not handle._find_calls for handle in factory._handles)


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_restart_event_after_shape_mismatch_clears_all_marks(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    actions: list[str] = []
    event = _event(11)
    backend = _Backend(_events={11: event}, _exact_reads={})
    path = tmp_path / 'group.sqlite3'

    def crash(_context: object) -> None:
        raise SystemExit(19)

    with _group_state(path) as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'first',
                    lambda _context: actions.append('old-first'),
                    execution=execution,
                ),
                _callback(1, 'second', crash, execution=execution),
            ),
            backend=backend,
            execution=execution,
        )
        assert lane.setup() is None
        with pytest.raises(SystemExit, match='19'):
            lane.deliver_many_counted((Delivery(event),))[0]
        lane.close()

    current = (
        _callback(
            0,
            'first',
            lambda _context: actions.append('new-first'),
            execution=execution,
        ),
        _callback(
            1,
            'replacement',
            lambda _context: actions.append('replacement'),
            execution=execution,
        ),
    )
    current_manifest = CallbackManifest(callbacks=tuple(callback.identity for callback in current))
    with _group_state(path) as state:
        lane, _factory = _lane(
            state,
            current,
            backend=backend,
            execution=execution,
        )
        assert lane.setup() is None
        mismatch = lane.resume()
        assert mismatch.status == 'shape-mismatch'
        restarted = state.restart_event(
            current_manifest,
            expected_revision=mismatch.revision,
            **_operator(),
        )
        assert {mark[3] for mark in restarted.callback_marks} == {'pending'}
        assert {mark[4] for mark in restarted.callback_marks} == {0}
        completed = lane.resume()
        lane.close()

    assert actions == ['old-first', 'new-first', 'replacement']
    assert completed.cursor == 11


def test_restart_event_manifest_rejects_unavailable_event_without_state_change(
    tmp_path: Path,
) -> None:
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, factory = _lane(
            state,
            (_callback(0, 'current', lambda _context: None),),
            backend=_Backend(_events={}, _exact_reads={}),
        )
        before = state.snapshot()

        with pytest.raises(ValueError, match='current event is unavailable'):
            lane.restart_event_manifest(11)

        assert state.snapshot() == before
        lane.close()

    assert factory._handles[-1]._find_calls == [('EventLogEntry', [['id', 'is', 11]])]


def test_restart_event_manifest_uses_current_event_local_callback_order(
    tmp_path: Path,
) -> None:
    event = _event(11, event_type='Shotgun_Match')
    callbacks = (
        _callback(
            0,
            'ignored',
            lambda _context: None,
            event_type='Shotgun_Ignore',
        ),
        _callback(
            1,
            'first',
            lambda _context: None,
            event_type='Shotgun_Match',
        ),
        _callback(2, 'second', lambda _context: None),
    )
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, factory = _lane(
            state,
            callbacks,
            backend=_Backend(_events={11: event}, _exact_reads={}),
        )
        before = state.snapshot()

        manifest = lane.restart_event_manifest(11)

        assert state.snapshot() == before
        lane.close()

    assert manifest == CallbackManifest(
        callbacks=(
            replace(callbacks[1].identity, ordinal=0),
            replace(callbacks[2].identity, ordinal=1),
        )
    )
    assert factory._handles[-1]._find_calls == [('EventLogEntry', [['id', 'is', 11]])]


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_restart_refetch_skips_done_callback_and_replays_running_callback(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    actions: list[str] = []
    crash = True
    event = _event(11)
    backend = _Backend(_events={11: event}, _exact_reads={})

    def first(_context: object) -> None:
        actions.append('first')

    def second(_context: object) -> None:
        nonlocal crash
        actions.append('second')
        if crash:
            crash = False
            raise SystemExit(17)

    path = tmp_path / 'group.sqlite3'
    with _group_state(path) as state:
        lane, _factory = _lane(
            state,
            (
                _callback(0, 'first', first, execution=execution),
                _callback(1, 'second', second, execution=execution),
            ),
            backend=backend,
            execution=execution,
        )
        assert lane.setup() is None
        with pytest.raises(SystemExit, match='17'):
            lane.deliver_many_counted((Delivery(event),))[0]
        lane.close()

    with _group_state(path) as state:
        lane, factory = _lane(
            state,
            (
                _callback(0, 'first', first, execution=execution),
                _callback(1, 'second', second, execution=execution),
            ),
            backend=backend,
            execution=execution,
        )
        assert lane.setup() is None
        snapshot = lane.resume()
        lane.close()

    assert actions == ['first', 'second', 'second']
    assert snapshot.cursor == 11
    assert snapshot.in_flight_event_id is None
    assert factory._handles[-1]._find_calls == [
        ('EventLogEntry', [['id', 'is', 11]]),
    ]


def test_restore_uses_frontier_read_before_floor_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / 'source.db'
    callback = _callback(0, 'callback', lambda _context: None)
    with _concrete_strict_states(path, initial_position=10, source_initial=20) as (source, state):
        state.start_event(11, CallbackManifest((callback.identity,)), expected_revision=0)
        lane, _factory = _lane(state, (callback,), ordering='strict', wait=lambda _delay: False)

        def advance_source_during_floor_query() -> None:
            source.commit_observations(range(21, 31), successful_clock=1.0)

        monkeypatch.setattr(lane, '_retention_floor', advance_source_during_floor_query)
        try:
            assert lane.setup() is None
            restored = lane.resume()
        finally:
            lane.close()
        assert source.snapshot().frontier == 30
        assert restored.history_truncated_floor == 21
        assert restored.required_event_id is None
        assert restored.in_flight_event_id == 11
    with _group_state(path, ordering='strict') as reopened:
        assert reopened.snapshot() == restored


@pytest.mark.parametrize(
    ('frontier', 'expected_floor', 'expected_required'),
    [(20, 21, None), (0, None, 11)],
    ids=['marks_history_truncated', 'zero_frontier_requires_event'],
)
def test_restore_with_no_visible_history(
    tmp_path: Path,
    frontier: int,
    expected_floor: int | None,
    expected_required: int | None,
) -> None:
    path = tmp_path / 'source.db'
    callback = _callback(0, 'callback', lambda _context: None)
    with _concrete_strict_states(path, initial_position=10, source_initial=frontier or None) as (
        _source,
        state,
    ):
        state.start_event(11, CallbackManifest((callback.identity,)), expected_revision=0)
        lane, _factory = _lane(state, (callback,), ordering='strict', wait=lambda _delay: False)
        try:
            assert lane.setup() is None
            restored = lane.resume()
        finally:
            lane.close()
        assert restored.history_truncated_floor == expected_floor
        assert restored.required_event_id == expected_required
        assert restored.in_flight_event_id == 11
    with _group_state(path, ordering='strict') as reopened:
        assert reopened.snapshot() == restored


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_retained_context_heartbeat_cannot_extend_another_invocation(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    clock = _Clock()
    retained: list[EventContext] = []
    observations: list[LaneHealth] = []
    lane: GroupLane

    def first(context: EventContext) -> None:
        retained.append(context)
        clock._advance(1.0)

    def second(context: EventContext) -> None:
        clock._advance(5.0)
        observations.append(lane.health())
        retained[0].heartbeat()
        observations.append(lane.health())
        context.heartbeat()
        observations.append(lane.health())

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(0, 'first', first, execution=execution),
                _callback(1, 'second', second, execution=execution),
            ),
            callback_deadline_secs=10.0,
            clock=clock,
            execution=execution,
        )
        assert lane.setup() is None
        lane.deliver_many_counted((Delivery(_event(11)),))[0]
        lane.close()

    before, after_retained, after_current = observations
    assert before.deadline_at == 11.0
    assert after_retained.deadline_at == before.deadline_at
    assert after_retained.heartbeat_at == before.heartbeat_at
    assert after_current.deadline_at == 16.0
    assert after_current.heartbeat_at == 6.0


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_retained_context_heartbeat_is_ignored_during_internal_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution: Literal['sync', 'async'],
) -> None:
    clock = _Clock()
    retained: list[EventContext] = []
    observations: list[tuple[LaneHealth, LaneHealth]] = []

    def callback(context: EventContext) -> None:
        retained.append(context)

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    callback,
                    event_type='Shotgun_Match',
                    execution=execution,
                ),
            ),
            clock=clock,
            execution=execution,
        )
        advance_irrelevant = state.advance_irrelevant

        def advance(
            new_cursor: int,
            *,
            expected_revision: int,
        ) -> GroupSnapshot:
            before = lane.health()
            clock._advance(5.0)
            retained[0].heartbeat()
            observations.append((before, lane.health()))
            return advance_irrelevant(
                new_cursor,
                expected_revision=expected_revision,
            )

        monkeypatch.setattr(state, 'advance_irrelevant', advance)
        assert lane.setup() is None
        lane.deliver_many_counted(
            (
                Delivery(_event(11, event_type='Shotgun_Match')),
                Delivery(_event(12, event_type='Shotgun_Ignore')),
            )
        )[0]
        lane.close()

    before, after = observations[0]
    assert before.phase == 'internal'
    assert after.deadline_at == before.deadline_at
    assert after.heartbeat_at == before.heartbeat_at


def test_scan_completion_collapse_returns_halt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _concrete_strict_states(
        tmp_path / 'source.db', initial_position=10, source_initial=20
    ) as (source, state):
        connection = source._database.connection
        insert_range(connection, 'range-a', 11, 12, state='closed')
        bind_group(connection, 'range-a', 'serial', 1, floor=10, eligibility_point=0)
        lane, _factory = _lane(state, (), ordering='strict')
        assert lane.setup() is None
        (pending,) = state.pending_ranges()
        snapshot = state.snapshot
        complete = state.complete_obligation_scan

        def complete_after_collapse(obligation_id: str) -> GroupSnapshot:
            def stale_snapshot() -> GroupSnapshot:
                before = snapshot()
                connection.execute(
                    'UPDATE group_state SET replay_required_from=11, '
                    "revision=revision+1 WHERE group_name='serial'"
                )
                monkeypatch.setattr(state, 'snapshot', snapshot)
                return before

            monkeypatch.setattr(state, 'snapshot', stale_snapshot)
            return complete(obligation_id)

        monkeypatch.setattr(state, 'complete_obligation_scan', complete_after_collapse)
        result = lane.scan_obligation(pending, (), source_successful_clock=0)
        assert result.status == 'replay-required'
        assert state.pending_ranges() == (pending,)
        lane.close()


@pytest.mark.parametrize('has_event', [False, True], ids=['empty', 'final-event'])
def test_scan_completion_returns_real_collapse_before_snapshot(
    tmp_path: Path, has_event: bool
) -> None:
    path = tmp_path / 'source.db'
    invoked: list[int] = []
    with (
        SourceState.open(
            path,
            writes=write_context_for(path),
            create=True,
            site='alpha',
            source='events',
            stream_fingerprint='stream',
            source_generation='source-1',
            max_group_obligations=1,
        ) as source,
        _group_state(path, ordering='strict') as state,
    ):
        source.initialize(10, incarnation='incarnation')
        source.commit_observations((12,), successful_clock=0)
        if has_event:
            record_source_late_ids(source, 'range-11-11', (11,), successful_clock=0)
        source._close_range('range-11-11', successful_clock=0)
        (pending,) = state.pending_ranges()
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0, 'callback', lambda context: invoked.append(cast('int', context.event['id']))
                ),
            ),
            ordering='strict',
        )
        assert lane.setup() is None
        collapsed: list[GroupSnapshot] = []
        completion_statements: list[str] = []

        def provider_iteration() -> Generator[Event]:
            if has_event:
                yield _event(11)
            source.commit_observations((14,), successful_clock=0)
            collapsed.append(state.snapshot())
            assert state.pending_ranges() == ()
            state._database.connection.set_trace_callback(completion_statements.append)

        result = lane.scan_obligation(
            pending, provider_iteration(), source_successful_clock=pending.eligibility_point
        )
        assert len(collapsed) == 1
        assert result == collapsed[0] == state.snapshot()
        assert result.status == 'replay-required'
        assert result.cursor == (11 if has_event else 10)
        assert invoked == ([11] if has_event else [])
        assert not any(statement.startswith('BEGIN') for statement in completion_statements)
        assert source._database.connection.execute(
            "SELECT COUNT(*) FROM range_groups WHERE obligation_state='pending'"
        ).fetchone() == (0,)
        state._database.connection.set_trace_callback(None)
        lane.close()


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_setup_and_callback_watchdogs_are_sliding_only_on_heartbeat(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    clock = _Clock()
    observations: list[LaneHealth] = []
    lane: GroupLane

    def setup(_context: object) -> None:
        clock._advance(10.0)
        observations.append(lane.health())

    def callback(context: EventContext) -> None:
        clock._advance(9.0)
        observations.append(lane.health())
        context.heartbeat()
        observations.append(lane.health())
        clock._advance(10.0)
        observations.append(lane.health())

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution=execution),),
            callback_deadline_secs=10.0,
            clock=clock,
            execution=execution,
            setup=setup,
        )

        assert lane.setup() is None
        lane.deliver_many_counted((Delivery(_event(11)),))[0]
        lane.close()

    setup_overdue, before_heartbeat, after_heartbeat, callback_overdue = observations
    assert setup_overdue.overdue_reason == 'setup overdue'
    assert before_heartbeat.overdue_reason is None
    assert before_heartbeat.deadline_at == 20.0
    assert after_heartbeat.deadline_at == 29.0
    assert after_heartbeat.heartbeat_at == 19.0
    assert callback_overdue.overdue_reason == 'callback overdue'


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_setup_precedes_serial_callbacks_and_handles_stay_in_lane_thread(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    actions: list[tuple[str, int]] = []

    def setup(_context: object) -> None:
        actions.append(('setup', threading.get_ident()))

    def first(_context: object) -> None:
        actions.append(('first', threading.get_ident()))

    def second(_context: object) -> None:
        actions.append(('second', threading.get_ident()))

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, factory = _lane(
            state,
            (
                _callback(0, 'first', first, execution=execution),
                _callback(1, 'second', second, execution=execution),
            ),
            execution=execution,
            setup=setup,
        )

        assert lane.setup() is None
        snapshot = lane.deliver_many_counted((Delivery(_event(11, session_uuid='session-1')),))[0]
        lane.close()

    lane_thread = threading.get_ident()
    assert actions == [
        ('setup', lane_thread),
        ('first', lane_thread),
        ('second', lane_thread),
    ]
    assert snapshot.cursor == 11
    assert snapshot.in_flight_event_id is None
    assert len(factory._handles) == 1
    assert factory._handles[0]._created_in == lane_thread
    assert factory._handles[0]._session_uuids == ['session-1', 'session-1']
    assert factory._handles[0]._closed


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_setup_retry_reruns_setup_without_replacing_lane_or_handle(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    attempts = 0

    def setup(_context: object) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError('setup failed')

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda _context: None,
                    execution=execution,
                ),
            ),
            execution=execution,
            setup=setup,
        )

        first = lane.setup()
        second = lane.setup()
        lane.close()

    assert first is not None
    assert first.code == 'setup-failed'
    assert second is None
    assert attempts == 2
    assert len(factory._handles) == 1


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_shutdown_stops_between_events(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    invoked: list[int] = []
    lane: GroupLane

    def callback(context: EventContext) -> None:
        invoked.append(_context_event_id(context))
        lane.request_shutdown()

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution=execution),),
            execution=execution,
        )
        assert lane.setup() is None
        snapshot = lane.deliver_many_counted(
            (
                Delivery(_event(11)),
                Delivery(_event(12)),
            )
        )[0]
        lane.close()

    assert invoked == [11]
    assert snapshot.cursor == 11
    assert lane.shutdown_requested()


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_skip_callback_continues_later_callbacks(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    actions: list[str] = []
    event = _event(11)
    backend = _Backend(_events={11: event}, _exact_reads={})

    def fail(_context: object) -> None:
        actions.append('failed')
        raise RuntimeError('skip me')

    def later(_context: object) -> None:
        actions.append('later')

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(0, 'failed', fail, execution=execution),
                _callback(1, 'later', later, execution=execution),
            ),
            backend=backend,
            execution=execution,
        )
        assert lane.setup() is None
        failed = lane.deliver_many_counted((Delivery(event),))[0]
        skipped = state.skip_callback(
            event_id=11,
            callback_ordinal=0,
            expected_revision=failed.revision,
            **_operator(),
        )
        assert skipped.callback_marks[0][3] == 'skipped'
        completed = lane.resume()
        lane.close()

    assert actions == ['failed', 'later']
    assert completed.cursor == 11


def test_source_below_floor_skip_is_full_audited_and_idempotent(
    tmp_path: Path,
) -> None:
    group_path = tmp_path / 'source.db'
    strict = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'serial': 1.0},
        group_delivery_floors={'serial': 99},
    )
    fanout = EventFanout()
    with _concrete_strict_states(
        group_path,
        initial_position=99,
        source_initial=102,
        delivery_floors={'serial': 99},
        group_holdbacks={'serial': 1.0},
    ) as (source_state, group_state):
        group_state.advance_irrelevant(102, expected_revision=0)
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (_callback(0, 'callback', lambda _context: None),),
            fanout=fanout,
        )
        channel = GroupChannel(
            group='serial',
            ordering='strict',
            max_events=10,
            max_bytes=10_000_000,
        )
        joined = lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        assert isinstance(joined, LiveJoinIdentity)
        encoded = EncodedEvent.from_event_frame(99, encode_event(_event(99)))

        strict._observe(encoded, successful_time=1.0)
        strict.pump_many(tuple(sorted(strict._groups)), 1.0)
        first = lane.drain_live(channel)
        strict._observe(encoded, successful_time=2.0)
        strict.pump_many(tuple(sorted(strict._groups)), 2.0)
        duplicate = lane.drain_live(channel)
        records = group_state._audit_records()
        lane.close()

    assert first.status == 'running'
    assert first.cursor == 102
    assert duplicate == first
    assert first.late_id_count == 0
    skips = tuple(record for record in records if record.command == 'strict-delivery-floor-skip')
    assert len(skips) == 1
    assert skips[0].event_id == 99
    with _group_state(
        group_path,
        initial_position=99,
        ordering='strict',
    ) as reopened:
        assert reopened.snapshot().cursor == 102
        assert (
            len(
                tuple(
                    record
                    for record in reopened._audit_records()
                    if record.command == 'strict-delivery-floor-skip'
                )
            )
            == 1
        )


def test_source_ordering_anomaly_crosses_strict_channel_and_survives_reopen(
    tmp_path: Path,
) -> None:
    callbacks: list[int] = []
    group_path = tmp_path / 'source.db'
    strict = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'serial': 1.0},
        group_delivery_floors={'serial': 99},
    )
    fanout = EventFanout()
    with _concrete_strict_states(
        group_path,
        initial_position=99,
        source_initial=102,
        delivery_floors={'serial': 99},
        group_holdbacks={'serial': 1.0},
    ) as (source_state, group_state):
        group_state.advance_irrelevant(102, expected_revision=0)
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (
                _callback(
                    0, 'callback', lambda context: callbacks.append(_context_event_id(context))
                ),
            ),
            fanout=fanout,
        )
        channel = GroupChannel(
            group='serial',
            ordering='strict',
            max_events=10,
            max_bytes=10_000_000,
        )
        joined = lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        assert isinstance(joined, LiveJoinIdentity)

        strict._observe(
            EncodedEvent.from_event_frame(101, encode_event(_event(101))),
            successful_time=1.0,
        )
        strict.pump_many(tuple(sorted(strict._groups)), 1.0)
        halted = lane.drain_live(channel)
        lane.close()

    assert halted.status == 'ordering-anomaly'
    assert halted.ordering_anomaly_id == 101
    assert halted.cursor == 102
    assert callbacks == []
    with _group_state(
        group_path,
        initial_position=99,
        ordering='strict',
    ) as reopened:
        assert reopened.snapshot().status == 'ordering-anomaly'
        assert reopened.snapshot().ordering_anomaly_id == 101


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_sqlite_progress_uses_internal_watchdog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution: Literal['sync', 'async'],
) -> None:
    clock = _Clock()
    observations: list[LaneHealth] = []
    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda _context: None,
                    event_type='Shotgun_Match',
                    execution=execution,
                ),
            ),
            clock=clock,
            execution=execution,
        )
        original_advance = state.advance_irrelevant

        def advance_irrelevant(
            new_cursor: int,
            *,
            expected_revision: int,
        ) -> GroupSnapshot:
            clock._advance(32.0)
            observations.append(lane.health())
            return original_advance(
                new_cursor,
                expected_revision=expected_revision,
            )

        monkeypatch.setattr(state, 'advance_irrelevant', advance_irrelevant)
        assert lane.setup() is None
        lane.deliver_many_counted((Delivery(_event(11, event_type='Shotgun_Ignore')),))[0]
        lane.close()

    assert observations[0].overdue_reason == 'internal stall'
    assert observations[0].deadline_at == 32.0


def test_strict_join_capacity_retry_is_recorded_through_lane_boundary(
    tmp_path: Path,
) -> None:
    strict = StrictBuffer(
        max_events=1,
        max_bytes=10_000_000,
        group_holdbacks={'retainer': 300.0, 'serial': 300.0},
    )
    attach_strict_group(strict, 'retainer', 1, holdback_secs=300.0, delivery_floor=0)
    strict._observe(
        EncodedEvent.from_event_frame(1, encode_event(_event(1))),
        successful_time=0.0,
    )
    with _concrete_strict_states(
        tmp_path / 'source.db',
        initial_position=1,
        source_initial=1,
    ) as (source_state, group_state):
        coordinator, lane = _concrete_strict_lane(
            source_state,
            group_state,
            strict,
            (_callback(0, 'callback', lambda _context: None),),
            fanout=EventFanout(),
        )
        retry = lane.join(
            coordinator,
            GroupChannel(
                group='serial',
                ordering='strict',
                max_events=10,
                max_bytes=10_000_000,
            ),
            catch_up=lambda _boundary: None,
        )
        snapshot = group_state.snapshot()
        lane.close()

    assert isinstance(retry, JoinRetry)
    assert snapshot.strict_capacity_abort_streak == 1
    assert snapshot.strict_capacity_attempt_token == _ATTEMPT_1


def test_strict_lane_halts_ordering_anomaly_before_callback_and_resumes_after_audit(
    tmp_path: Path,
) -> None:
    callback_ids: list[int] = []
    with _group_state(
        tmp_path / 'group.db',
        initial_position=10,
        ordering='strict',
    ) as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: callback_ids.append(_context_event_id(context)),
                ),
            ),
            ordering='strict',
        )
        assert lane.setup() is None
        delivered = lane.deliver_many_counted((Delivery(_event(12)),))[0]
        assert delivered.cursor == 12
        assert callback_ids == [12]

        anomaly = lane.deliver_many_counted((Delivery(_event(11)),))[0]
        assert anomaly.status == 'ordering-anomaly'
        assert anomaly.ordering_anomaly_id == 11
        assert callback_ids == [12]
        assert lane._ordering_anomalies[-1] == OrderingAnomaly(
            group='serial',
            group_generation=1,
            event_id=11,
            last_delivered_id=12,
            delivery_floor=10,
            action='halt',
        )

        acknowledged = state.acknowledge_ordering_anomaly(
            event_id=11,
            expected_revision=anomaly.revision,
            **_operator(),
        )
        lane._snapshot()
        continued = lane.deliver_many_counted((Delivery(_event(13)),))[0]
        assert acknowledged.ordering_anomaly_id is None
        assert continued.cursor == 13
        assert callback_ids == [12, 13]


def test_strict_lane_skips_below_delivery_floor_with_body_free_mark(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'group.db'
    callback_ids: list[int] = []
    with _group_state(
        path,
        initial_position=12,
        ordering='strict',
    ) as state:
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: callback_ids.append(_context_event_id(context)),
                ),
            ),
            ordering='strict',
        )
        assert lane.setup() is None

        first = lane.deliver_many_counted((Delivery(_event(11)),))[0]
        second = lane.deliver_many_counted((Delivery(_event(11)),))[0]
        records = state._audit_records()

        assert first.status == 'running'
        assert first.cursor == 12
        assert second == first
        assert callback_ids == []
        assert lane._ordering_anomalies == (
            OrderingAnomaly(
                group='serial',
                group_generation=1,
                event_id=11,
                last_delivered_id=12,
                delivery_floor=12,
                action='skip',
            ),
            OrderingAnomaly(
                group='serial',
                group_generation=1,
                event_id=11,
                last_delivered_id=12,
                delivery_floor=12,
                action='skip',
            ),
        )
        floor_skips = tuple(
            record for record in records if record.command == 'strict-delivery-floor-skip'
        )
        assert len(floor_skips) == 1
        assert floor_skips[0].event_id == 11

    with _group_state(path, initial_position=12, ordering='strict') as reopened:
        assert reopened.snapshot().cursor == 12
        assert (
            len(
                tuple(
                    record
                    for record in reopened._audit_records()
                    if record.command == 'strict-delivery-floor-skip'
                )
            )
            == 1
        )


def test_strict_post_close_scan_waits_for_monotonic_source_clock(
    tmp_path: Path,
) -> None:
    invoked: list[int] = []
    iterated: list[int] = []

    def _events() -> Iterator[Event]:
        for event_id in range(11, 16):
            iterated.append(event_id)
            yield _event(event_id)

    with _group_state(
        tmp_path / 'group.sqlite3',
        ordering='strict',
    ) as state:
        obligation = _pending_range(state)
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: invoked.append(_context_event_id(context)),
                ),
            ),
            ordering='strict',
        )
        assert lane.setup() is None

        waiting = lane.scan_obligation(
            obligation,
            _events(),
            source_successful_clock=29.0,
        )
        with pytest.raises(ValueError, match='regress'):
            lane.scan_obligation(
                obligation,
                (),
                source_successful_clock=28.0,
            )
        for invalid in (-1.0, float('inf'), float('nan')):
            with pytest.raises(ValueError, match='finite and nonnegative'):
                lane.scan_obligation(
                    obligation,
                    (),
                    source_successful_clock=invalid,
                )
        resolved = lane.scan_obligation(
            obligation,
            _events(),
            source_successful_clock=30.0,
        )
        lane.close()

    assert waiting.obligation_count == 1
    assert iterated == [11, 12, 13, 14, 15]
    assert invoked == [11, 12, 13, 14, 15]
    assert resolved.obligation_count == 0


def test_sync_callback_awaitable_failure_retains_lane_traceback(tmp_path: Path) -> None:
    seen: list[int | str] = []
    returned: list[Coroutine[object, object, None]] = []

    async def work() -> None:
        seen.append('executed')

    def callback(context: EventContext) -> object:
        seen.append(_context_event_id(context))
        result = work()
        returned.append(result)
        return result

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(state, (_callback(0, 'returned-coroutine', callback),))
        assert lane.setup() is None
        snapshot = lane.deliver_many_counted((Delivery(_event(11)), Delivery(_event(12))))[0]
        lane.close()

    assert seen == [11]
    assert len(returned) == 1
    assert inspect.getcoroutinestate(returned[0]) == inspect.CORO_CLOSED
    assert snapshot.status == 'callback-failed'
    assert snapshot.in_flight_event_id == 11
    assert snapshot.failure_ordinal == 0
    failure_text = snapshot.failure_text or ''
    assert 'Traceback (most recent call last):' in failure_text
    assert 'in validate_sync_callback_result' in failure_text
    assert 'PluginError: callback-failed' in failure_text
    assert "sync callback 'returned-coroutine' returned an awaitable" in failure_text


@pytest.mark.parametrize('execution', ['sync', 'async'])
@pytest.mark.parametrize('matching', [True, False])
def test_terminal_late_duplicate_after_reopen_ignores_diagnostic_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution: Literal['sync', 'async'],
    matching: bool,
) -> None:
    invoked: list[int] = []
    path = tmp_path / 'group.sqlite3'
    callbacks = (
        _callback(
            0,
            'callback',
            lambda context: invoked.append(_context_event_id(context)),
            execution=execution,
            event_type='Shotgun_Test_Change' if matching else 'Other',
        ),
    )
    delivery = Delivery(_event(15), obligation_id='range-1')
    with _group_state(path) as state:
        lane, _factory = _lane(state, callbacks, execution=execution)
        assert lane.setup() is None
        _pending_range(state, closed=False)
        completed = lane.deliver_many_counted((delivery,))[0]
        lane.close()

    with _group_state(path) as state:
        lane, _factory = _lane(state, callbacks, execution=execution)
        assert lane.setup() is None
        original_start = state.start_event
        original_complete = state.complete_late_id

        def renamed_start(
            event_id: int,
            manifest: CallbackManifest,
            *,
            expected_revision: int,
            obligation_id: str | None = None,
        ) -> GroupSnapshot:
            try:
                return original_start(
                    event_id,
                    manifest,
                    expected_revision=expected_revision,
                    obligation_id=obligation_id,
                )
            except InvalidTransitionError as error:
                raise type(error)('reworded terminal start diagnostic') from error

        def renamed_complete(
            obligation_id: str,
            event_id: int,
            *,
            expected_revision: int,
        ) -> GroupSnapshot:
            try:
                return original_complete(
                    obligation_id,
                    event_id,
                    expected_revision=expected_revision,
                )
            except InvalidTransitionError as error:
                raise type(error)('reworded terminal completion diagnostic') from error

        monkeypatch.setattr(state, 'start_event', renamed_start)
        monkeypatch.setattr(state, 'complete_late_id', renamed_complete)
        try:
            duplicate = lane.deliver_many_counted((delivery,))[0]
        finally:
            lane.close()

        assert duplicate == completed
        assert state.snapshot() == completed
    assert invoked == ([15] if matching else [])


@pytest.mark.parametrize('stop', ['failure', 'shutdown', 'before'])
def test_live_batch_retains_exact_unconsumed_suffix(
    tmp_path: Path,
    stop: str,
) -> None:
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    invoked: list[int] = []

    def callback(context: EventContext) -> None:
        invoked.append(_context_event_id(context))
        if stop == 'failure':
            raise RuntimeError('stop at matching event')
        lane.request_shutdown()

    with _group_state(tmp_path / 'group.db') as state:
        lane, _factory = _lane(
            state, (_callback(0, 'matching', callback, event_type='Shotgun_Match'),)
        )
        assert lane.setup() is None
        lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        for event_id in range(11, 16):
            event = _event(
                event_id, event_type=('Shotgun_Match' if event_id == 13 else 'Shotgun_Ignore')
            )
            assert channel.offer(
                EventDelivery(
                    EncodedEvent.from_event_frame(
                        event_id,
                        encode_event(event),
                    )
                )
            )
        if stop == 'before':
            lane.request_shutdown()
        stopped = lane.drain_live(channel)
        owned = lane._join_state
        assert isinstance(owned, _FinalizedJoin)
        retained = [
            message.encoded.event_id
            for message in owned._pending
            if isinstance(message, EventDelivery)
        ]
        again = lane.drain_live(channel)
        lane.close()

    assert invoked == ([] if stop == 'before' else [13])
    assert stopped.cursor == {'failure': 12, 'shutdown': 13, 'before': 10}[stop]
    assert stopped.in_flight_event_id == (13 if stop == 'failure' else None)
    assert retained == (list(range(11, 16)) if stop == 'before' else [14, 15])
    assert again == stopped


@pytest.mark.parametrize('repeated_id', [11, 12])
def test_live_strict_batch_checks_order_against_irrelevant_prefix(
    tmp_path: Path,
    repeated_id: int,
) -> None:
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(
        group='serial',
        ordering='strict',
        max_events=10,
        max_bytes=1_000_000,
    )
    with _group_state(tmp_path / 'group.db', ordering='strict') as state:
        lane, _factory = _lane(state, (), ordering='strict')
        assert lane.setup() is None
        lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        for event_id in (11, 12, repeated_id, 13):
            assert channel.offer(
                EventDelivery(
                    EncodedEvent.from_event_frame(
                        event_id,
                        encode_event(_event(event_id)),
                    )
                )
            )
        snapshot = lane.drain_live(channel)
        owned = lane._join_state
        assert isinstance(owned, _FinalizedJoin)
        retained = [
            message.encoded.event_id
            for message in owned._pending
            if isinstance(message, EventDelivery)
        ]
        lane.close()
    assert snapshot.cursor == (12 if repeated_id == 11 else 13)
    assert snapshot.status == ('ordering-anomaly' if repeated_id == 11 else 'running')
    assert retained == ([13] if repeated_id == 11 else [])


@pytest.mark.parametrize('detach_during', ['callback', 'irrelevant'])
def test_live_batch_stops_at_detachment_before_next_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    detach_during: str,
) -> None:
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(
        group='serial',
        ordering='eager',
        max_events=10,
        max_bytes=1_000_000,
    )
    invoked: list[int] = []

    def callback(context: EventContext) -> None:
        invoked.append(_context_event_id(context))
        channel.detach('source-lost')

    with _group_state(tmp_path / 'group.db') as state:
        callbacks = () if detach_during == 'irrelevant' else (_callback(0, 'callback', callback),)
        lane, _factory = _lane(state, callbacks)
        assert lane.setup() is None
        lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        if detach_during == 'irrelevant':
            matching_callbacks = lane._host.matching_callbacks

            def match_then_detach(event: Event) -> tuple[tuple[PluginSpec, CallbackSpec], ...]:
                matched = matching_callbacks(event)
                channel.detach('source-lost')
                return matched

            monkeypatch.setattr(lane._host, 'matching_callbacks', match_then_detach)
        for event_id in (11, 12):
            assert channel.offer(
                EventDelivery(
                    EncodedEvent.from_event_frame(
                        event_id,
                        encode_event(_event(event_id)),
                    )
                )
            )
        with pytest.raises(JoinProtocolError, match='detached before processing'):
            lane.drain_live(channel)
        snapshot = state.snapshot()
        with pytest.raises(JoinProtocolError, match='gate is closed'):
            lane.drain_live(channel)
        lane.close()

    assert invoked == ([11] if detach_during == 'callback' else [])
    assert snapshot.cursor == 11
    assert snapshot.in_flight_event_id is None


@pytest.mark.parametrize('matching', [False, True])
def test_drain_live_returns_by_its_deadline_under_sustained_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    matching: bool,
) -> None:
    clock = [100.0]
    channel = GroupChannel(group='serial', ordering='eager', max_events=100, max_bytes=1_000_000)
    invoked: list[int] = []

    def callback(context: EventContext) -> None:
        invoked.append(_context_event_id(context))
        clock[0] += 0.25

    with _group_state(tmp_path / 'group.db') as state:
        callbacks = (_callback(0, 'callback', callback),) if matching else ()
        lane, _factory = _lane(state, callbacks, clock=lambda: clock[0])
        assert lane.setup() is None
        lane.join(_Coordinator(boundary=10), channel, catch_up=lambda _boundary: None)
        deliver = lane._deliver_one

        def advance_time(
            delivery: Delivery,
            snapshot: GroupSnapshot,
            irrelevant_high: int | None,
            *,
            live_prefix: bool = False,
        ) -> tuple[GroupSnapshot, int | None]:
            result = deliver(delivery, snapshot, irrelevant_high, live_prefix=live_prefix)
            if not matching:
                clock[0] += 0.25
            return result

        monkeypatch.setattr(lane, '_deliver_one', advance_time)
        for event_id in range(11, 31):
            assert channel.offer(
                EventDelivery(
                    EncodedEvent.from_event_frame(event_id, encode_event(_event(event_id)))
                )
            )
        snapshot = lane.drain_live(channel, deadline_at=101.0)
        assert clock[0] == 101.0
        assert snapshot.cursor == 14
        assert invoked == ([11, 12, 13, 14] if matching else [])
        final = lane.drain_live(channel, deadline_at=110.0)
        assert final.cursor == 30
        lane.close()


@pytest.mark.parametrize(
    'boundary', ['precheck', 'matching-start', 'irrelevant-flush', 'after-flush']
)
def test_live_strict_fence_preserves_committed_prefix_and_queued_suffix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    invoked: list[int] = []
    coordinator = _Coordinator(boundary=10)
    channel = GroupChannel(group='serial', ordering='strict', max_events=10, max_bytes=1_000_000)
    with _group_state(tmp_path / 'source.db', ordering='strict') as state:
        matching = (
            {11, 12, 13, 14, 15}
            if boundary in {'precheck', 'matching-start'}
            else {11, 13, 14}
            if boundary == 'after-flush'
            else {11, 13}
        )
        lane, _factory = _lane(
            state,
            (
                _callback(
                    0,
                    'callback',
                    lambda context: invoked.append(_context_event_id(context)),
                    event_type='Match',
                ),
            ),
            ordering='strict',
        )
        assert lane.setup() is None
        lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        inserted = False

        def insert_fence() -> None:
            nonlocal inserted
            if inserted:
                return
            inserted = True
            connection = state._database.connection
            insert_range(connection, 'fence', 13, 13, state='closed')
            bind_group(connection, 'fence', 'serial', 1, floor=10, eligibility_point=0)
            insert_source_late_ids(connection, 'fence', (13,))

        if boundary == 'precheck':
            insert_fence()
        elif boundary == 'irrelevant-flush':
            advance = state.advance_irrelevant_prefix

            def fence_before_flush(high_id: int, *, expected_revision: int) -> GroupSnapshot:
                insert_fence()
                return advance(high_id, expected_revision=expected_revision)

            monkeypatch.setattr(state, 'advance_irrelevant_prefix', fence_before_flush)
        else:
            start = state.start_event

            def fence_before_start(
                event_id: int,
                manifest: CallbackManifest,
                *,
                expected_revision: int,
                obligation_id: str | None = None,
            ) -> GroupSnapshot:
                if event_id == 14:
                    insert_fence()
                return start(
                    event_id,
                    manifest,
                    expected_revision=expected_revision,
                    obligation_id=obligation_id,
                )

            monkeypatch.setattr(state, 'start_event', fence_before_start)
        for event_id in (11, 12, 14, 15):
            event = _event(event_id, event_type='Match' if event_id in matching else 'Other')
            assert channel.offer(
                EventDelivery(EncodedEvent.from_event_frame(event_id, encode_event(event)))
            )
        blocked = lane.drain_live(channel)
        assert inserted
        assert blocked.cursor == 12
        assert blocked.status == 'running'
        owned = lane._join_state
        assert isinstance(owned, _FinalizedJoin)
        assert [
            item.encoded.event_id for item in owned._pending if isinstance(item, EventDelivery)
        ] == [14, 15]
        assert invoked == ([11, 12] if boundary in {'precheck', 'matching-start'} else [11])
        (pending,) = state.pending_ranges()
        lane.scan_obligation(pending, (_event(13, event_type='Match'),), source_successful_clock=1)
        finished = lane.drain_live(channel)
        assert finished.cursor == 15
        assert not owned._pending
        assert invoked == [event_id for event_id in (11, 12, 13, 14, 15) if event_id in matching]
        lane.close()


def test_unexpected_async_cancelled_error_is_callback_failure(
    tmp_path: Path,
) -> None:
    async def callback(_context: EventContext) -> None:
        raise asyncio.CancelledError('unexpected cancellation')

    with _group_state(tmp_path / 'group.sqlite3') as state:
        lane, _factory = _lane(
            state,
            (_callback(0, 'callback', callback, execution='async'),),
            execution='async',
        )
        assert lane.setup() is None
        snapshot = lane.deliver_many_counted((Delivery(_event(11)),))[0]
        lane.close()

    assert snapshot.status == 'callback-failed'
    assert snapshot.cursor == 10
    assert snapshot.in_flight_event_id == 11
    assert 'CancelledError' in (snapshot.failure_text or '')
