from __future__ import annotations

import errno
import logging
import multiprocessing
import os
import selectors
import signal
import socket
import sqlite3
import sys
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, closing, contextmanager, nullcontext, suppress
from dataclasses import dataclass, replace
from functools import partial, partialmethod
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import (
    TYPE_CHECKING,
    Literal,
    NotRequired,
    Protocol,
    Self,
    TypedDict,
    TypeVar,
    Unpack,
    assert_type,
    cast,
    get_args,
)
from unittest.mock import Mock

import pytest

import sg_event_daemon._checkpoint_host as checkpoint_host_module
import sg_event_daemon._cli as cli_module
import sg_event_daemon._control as control_module
import sg_event_daemon._platform as platform_module
import sg_event_daemon._worker as worker_module
from sg_event_daemon._checkpoint import CheckpointAttempt, Checkpointer
from sg_event_daemon._checkpoint_host import CheckpointHost, CheckpointView
from sg_event_daemon._config import (
    Config,
    ConfigError,
    CredentialConfig,
    DaemonConfig,
    GroupConfig,
    SecretValue,
    SiteConfig,
    SourceConfig,
    WorkerConfig,
)
from sg_event_daemon._events import Event, encode_event, normalize_filter
from sg_event_daemon._fanout import (
    EncodedEvent,
    EventDelivery,
    EventFanout,
    GroupChannel,
    RangeChanged,
)
from sg_event_daemon._group_errors import InvalidTransitionError
from sg_event_daemon._group_recovery import DispositionRequest, LifecycleRequest
from sg_event_daemon._group_state import GroupOpenOptions, GroupState, PendingRange, SourceProgress
from sg_event_daemon._group_types import (
    AuditIdentity,
    ConditionalParkDirective,
    ConditionalParkResult,
    LiveJoinIdentity,
)
from sg_event_daemon._health import GroupHealth, WatchdogHealth, WorkerHealth, aggregate_status
from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._ipc import (
    ActiveSourceGroupAssignment,
    CommandResult,
    DeliveryGroupAssignment,
    EventClient,
    EventPeerGrant,
    EventPeerReady,
    EventServer,
    GroupBootstrapObservation,
    GroupBootstrapPending,
    GroupBootstrapRequest,
    HealthUpdate,
    OperatorCommand,
    PendingDispositionSourceGroupAssignment,
    SourceEndpointReady,
    StopCommand,
    WorkerAssignment,
    WorkerControl,
    _decode_event_wire,
    _encode_fanout_message,
    encode_control_message,
)
from sg_event_daemon._lane import Delivery, GroupLane, LaneHealth, _FinalizedJoin, _JoinSource
from sg_event_daemon._platform import (
    ensure_secure_state_dir,
    local_socket_path,
    local_socket_path_candidate,
)
from sg_event_daemon._plugins import (
    CallbackIdentity,
    CallbackManifest,
    CallbackSpec,
    EventContext,
    PluginHost,
    PluginSpec,
    SetupContext,
)
from sg_event_daemon._providers import Provider, ProviderCapabilities, _FindOptions
from sg_event_daemon._source import (
    Attached,
    CatchupReader,
    HistoryTruncatedError,
    JoinAbort,
    JoinAttemptIdentity,
    JoinConfirm,
    JoinCoordinator,
    JoinFinalize,
    JoinFinalized,
    JoinProtocolError,
    JoinRequest,
    JoinRetry,
    JoinTarget,
    SourceHealth,
    SourcePoller,
    StrictDeliveryOutcome,
    _SuccessfulQueryClock,
)
from sg_event_daemon._source_schema import BUMP_INDEX_REVISION, BUMP_RANGE_REVISION
from sg_event_daemon._source_state import SourceSnapshot, SourceState
from sg_event_daemon._sqlite import DatabaseHandle, Schema, open_database
from sg_event_daemon._strict import StrictBuffer
from sg_event_daemon._watchdog import ComponentWatchdog, WatchdogKind, WatchdogSample
from sg_event_daemon._worker import (
    WorkerKind,
    WorkerRuntime,
    WorkerSpec,
    _CatchupLimiter,
    _ComponentHealthCache,
    _ComponentStopPhase,
    _DeliveryFloorChannel,
    _LaneComponent,
    _LaneResources,
    _load_shared_plugins,
    _OwnerCall,
    _OwnerMailbox,
    _RemoteBootstrapTransportError,
    _SourceComponent,
    _WorkerComponent,
    build_worker_runtime,
    build_worker_specs,
)
from sg_event_daemon._worker import (
    _LaneRefresh as LaneRefresh,
)
from sg_event_daemon._write_lock import (
    SourceDatabaseBusyError,
    WriteLockStoppedError,
    WriteLockTimeoutError,
)
from tests._ipc_support import saturate_worker_control_sender as _saturate_worker_control_sender
from tests._runtime_diagnostics import record_runtime_exception
from tests._storage_support import (
    bind_group,
    close_test_range,
    group_exists,
    insert_range,
    insert_source_late_ids,
    open_group_on_source,
    orphan_group,
    write_context_for,
)
from tests.source.test_state import open_source_range

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Iterable, Mapping, Sequence
    from concurrent.futures import Future
    from multiprocessing.connection import Connection
    from typing import NoReturn

    from sg_event_daemon._fanout import GroupChannelInterface
    from sg_event_daemon._group_types import GroupSnapshot
    from sg_event_daemon._ipc import ControlMessage
    from sg_event_daemon._watchdog import WatchdogPhase
    from sg_event_daemon._worker import (
        _LaneConfiguration,
        _LaneLifecycle,
        _SourceJoinSurface,
    )
    from sg_event_daemon._write_lock import SourceWriteLock, WriteContext, _TransactionOptions

_ComponentT = TypeVar('_ComponentT')
_Message = TypeVar('_Message')


class _CleanupProbe:
    def __init__(
        self,
        name: str,
        operations: list[str],
        error: RuntimeError | None,
    ) -> None:
        self._error = error
        self._name = name
        self._operations = operations
        self._traceback: object | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        pass

    def close(self) -> None:
        self._operations.append(self._name)
        if self._error is not None:
            try:
                raise self._error
            except RuntimeError as error:
                self._traceback = error.__traceback__
                raise


class _Component:
    def __init__(
        self,
        name: str,
        operations: list[str],
        *,
        wedge: bool = False,
    ) -> None:
        self._name = name
        self._operations = operations
        self._finished = threading.Event()
        self._release = threading.Event()
        self._started = threading.Event()
        self._wedge = wedge

    def _close(self) -> None:
        self._operations.append(f'close:{self._name}')

    def _execute_operator_command(
        self,
        command: OperatorCommand,
    ) -> Mapping[str, object]:
        self._operations.append(f'operator:{self._name}:{command.command}')
        return {'owner': self._name, 'command': command.command}

    def _request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        del shutdown_deadline_at
        self._operations.append(f'stop:{self._name}')
        if not self._wedge:
            self._release.set()

    def _run(self) -> None:
        self._operations.append(f'run:{self._name}:{threading.get_ident()}')
        self._started.set()
        self._release.wait()
        self._finished.set()


class _BlockingOperatorComponent(_Component):
    def __init__(
        self,
        name: str,
        operations: list[str],
        *,
        interrupt_operator: bool,
    ) -> None:
        super().__init__(name, operations)
        self._interrupt_operator = interrupt_operator
        self._operator_entered = threading.Event()
        self._operator_release = threading.Event()

    def _execute_operator_command(
        self,
        command: OperatorCommand,
    ) -> Mapping[str, object]:
        self._operator_entered.set()
        self._operator_release.wait()
        return super()._execute_operator_command(command)

    def _request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        super()._request_stop(shutdown_deadline_at=shutdown_deadline_at)
        if self._interrupt_operator:
            self._operator_release.set()


class _ControlSendComponent:
    def __init__(self, action: Callable[[], None]) -> None:
        self._action = action

    @staticmethod
    def _close() -> None:
        pass

    @staticmethod
    def _execute_operator_command(command: OperatorCommand) -> Mapping[str, object]:
        del command
        raise AssertionError('control-send component does not execute operator commands')

    @staticmethod
    def _request_stop(*, shutdown_deadline_at: float | None = None) -> None:
        del shutdown_deadline_at

    def _run(self) -> None:
        self._action()


class _CoordinationLane:
    def __init__(self, state: GroupState) -> None:
        self._state = state

    def scan_obligation(
        self,
        revision: PendingRange,
        events: Iterable[Event],
        *,
        source_successful_clock: float | None = None,
    ) -> object:
        del source_successful_clock
        assert tuple(events) == ()
        return self._state.complete_obligation_scan(revision.obligation_id)


class _CoordinationReader:
    def __init__(self) -> None:
        self._ranges: list[tuple[int, int]] = []

    def retention_floor(self) -> int:
        return 1

    def scan_range(self, low_id: int, high_id: int) -> Generator[Event, None, None]:
        self._ranges.append((low_id, high_id))
        yield from ()


class _DispositionComponent(Protocol):
    def _execute_operator_command(
        self,
        command: OperatorCommand,
    ) -> Mapping[str, object]: ...

    def _request_stop(self, *, shutdown_deadline_at: float | None = None) -> None: ...

    def _run(self) -> None: ...


class _DurableCoordinationSurface:
    def __init__(self, state: SourceState) -> None:
        self._state = state
        self.join_coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation='source-1',
            strict_retention=StrictBuffer(
                max_events=20,
                max_bytes=262_144,
                group_holdbacks={'local': 300.0},
            ),
        )

    def abort(self, abort: JoinAbort) -> None:
        self.join_coordinator.abort(abort)

    def confirm(self, confirmation: JoinConfirm) -> Attached | JoinRetry:
        return self.join_coordinator.confirm(confirmation)

    def event_channel_lost(self, _group: str, *, reason: str) -> None:
        assert reason == 'event-channel-lost'

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        return self.join_coordinator.finalize(finalization)

    def request(self, request: JoinRequest, channel: GroupChannel) -> JoinTarget | JoinRetry:
        result = self.join_coordinator.request(request, channel)
        if isinstance(result, JoinRetry):
            return result
        snapshot = self._state.snapshot()
        return replace(
            result,
            stream_incarnation=snapshot.stream_incarnation,
            source_successful_clock=snapshot.successful_clock,
        )


class _DirectSourceComponent:
    def __init__(self, surface: _DurableCoordinationSurface) -> None:
        self._surface = surface

    def _call(
        self,
        invoke: Callable[[object], object],
        *,
        deadline_secs: float,
        stop: threading.Event,
    ) -> object:
        del deadline_secs, stop
        return invoke(self._surface)


class _FloorChannelFactory(Protocol):
    def __call__(
        self,
        *,
        group: str,
        ordering: Literal['eager', 'strict'],
        max_events: int,
        max_bytes: int,
        delivery_floor: int,
    ) -> object: ...


class _GrantRequester(Protocol):
    def _request_grant(self, grant: EventPeerGrant) -> None: ...


class _GroupHealthLane:
    def __init__(self, snapshot: GroupSnapshot) -> None:
        self._drain_snapshot = snapshot

    def drain_live(
        self,
        _channel: object,
        *,
        idle_snapshot: object | None = None,
        deadline_at: float = float('inf'),
    ) -> GroupSnapshot:
        del idle_snapshot, deadline_at
        return self._drain_snapshot

    @staticmethod
    def health() -> object:
        return LaneHealth(
            site='main',
            source='events',
            group='local',
            worker_instance='site-spawn-1',
            execution='sync',
            ordering='eager',
            phase='idle',
            status='running',
            cursor=41,
            event_id=None,
            plugin_id=None,
            callback_id=None,
            delivery_attempt=None,
            heartbeat_at=0.0,
            heartbeat_age=0.0,
            deadline_at=None,
            internal_stall_deadline_secs=5.0,
            overdue_reason=None,
        )

    def resume(self) -> GroupSnapshot:
        return self._drain_snapshot


class _IdleCheckpointHost:
    def register(self, _path: object, _identity: str, _schema: Schema) -> CheckpointView:
        return CheckpointView()


class _InterruptibleComponent:
    def __init__(
        self,
        run: Callable[[], object],
        request_stop: Callable[[], None],
    ) -> None:
        self._stop_callback = request_stop
        self._run_callback = run

    def _close(self) -> None:
        pass

    def _execute_operator_command(
        self,
        command: OperatorCommand,
    ) -> Mapping[str, object]:
        del command
        raise AssertionError('interruptible probe received an operator command')

    def _request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        del shutdown_deadline_at
        self._stop_callback()

    def _run(self) -> None:
        self._run_callback()


class _LaneCatchupDriver(Protocol):
    def __call__(
        self,
        component: object,
        resources: object,
        frontier: int,
    ) -> None: ...


class _LaneLifecycleDriver(Protocol):
    def __call__(self, component: object, resources: object) -> None: ...


class _LifecycleChannel:
    def __init__(self) -> None:
        self._closed = False

    def close(self) -> None:
        self._closed = True


class _LifecycleComponent:
    def __init__(self, component: object, resources: object) -> None:
        self._component = component
        self._resources = resources

    def _close(self) -> None:
        return None

    def _execute_operator_command(
        self,
        command: OperatorCommand,
    ) -> Mapping[str, object]:
        del command
        raise AssertionError('lifecycle-only component received an operator command')

    def _request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        del shutdown_deadline_at
        stop = cast('threading.Event', vars(self._component)['_stop'])
        stop.set()

    def _run(self) -> None:
        component_type = type(self._component)
        driver = cast(
            '_LaneLifecycleDriver',
            vars(component_type)['_drive_lane'],
        )
        driver(self._component, self._resources)


class _LifecycleCoordination:
    def __init__(self) -> None:
        self._calls: list[bool] = []
        self._serviced = threading.Event()

    def _service(self, *, scan_obligations: bool = True, force: bool = False) -> object:
        del force
        self._calls.append(scan_obligations)
        self._serviced.set()
        return None


class _LifecycleLane:
    def __init__(
        self,
        setup_failure: RuntimeError | None,
        resume_status: str,
    ) -> None:
        self._closed = False
        self._drains = 0
        self._joined = 0
        self._operations: list[str] = []
        self._resume_status = resume_status
        self._setup_failure = setup_failure

    def close(self) -> None:
        self._closed = True

    def drain_live(
        self,
        _channel: object,
        *,
        idle_snapshot: object | None = None,
        deadline_at: float = float('inf'),
    ) -> object:
        del idle_snapshot, deadline_at
        self._operations.append('drain_live')
        self._drains += 1
        return SimpleNamespace(status='running')

    def join(self, *_args: object, **_kwargs: object) -> object:
        self._operations.append('join')
        self._joined += 1
        return object()

    def resume(self) -> object:
        self._operations.append('resume')
        return SimpleNamespace(status=self._resume_status)

    def setup(self) -> RuntimeError | None:
        self._operations.append('setup')
        return self._setup_failure


class _LifecycleReader:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def close(self) -> None:
        return None


class _LifecycleState:
    def __init__(self, status: str) -> None:
        self._status = status

    def close(self) -> None:
        return None

    def operator_writes(self) -> AbstractContextManager[None]:
        return nullcontext()

    def snapshot(self) -> object:
        return SimpleNamespace(status=self._status)


class _LocalJoinClientFactory(Protocol):
    def __call__(
        self,
        source: object,
        *,
        deadline_secs: float,
        source_name: str,
        group: str,
        stop: threading.Event,
    ) -> object: ...


class _NontransportConstructionFailure:
    def __init__(
        self,
        stage: Literal['provider', 'state', 'plugin'],
        stop: threading.Event,
    ) -> None:
        self._attempts = 0
        self._client = _LifecycleReader()
        self._reader = _LifecycleReader()
        self._stage = stage
        self._state = _LifecycleReader()
        self._stop = stop

    def _build_client(self, _context: object) -> tuple[object, str]:
        return self._client, 'source-generation-1'

    def _fail(self) -> NoReturn:
        self._attempts += 1
        self._stop.set()
        raise ConnectionError(f'{self._stage} construction failed')

    def _fail_plugins(self, _loader: object) -> NoReturn:
        self._fail()

    def _observe_source(
        self,
        _client: object,
        *,
        assignment: object,
    ) -> object:
        del assignment
        return SimpleNamespace(source_generation='source-generation-1')

    def _open_state(
        self,
        _context: object,
        _observation: object,
        _reader: object,
    ) -> object:
        if self._stage == 'state':
            self._fail()
        return self._state

    def _provider_reader(self, _context: object) -> tuple[object, object]:
        if self._stage == 'provider':
            self._fail()
        return object(), self._reader

    def _install(
        self,
        component: object,
        context: object,
        database_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        if self._stage == 'plugin':
            monkeypatch.setattr(
                worker_module,
                '_load_shared_plugins',
                self._fail_plugins,
            )
        vars(component).update(
            {
                '_build_client': self._build_client,
                '_config': SimpleNamespace(daemon=SimpleNamespace(sqlite_synchronous='FULL')),
                '_configuration': lambda: context,
                '_database_path': lambda: database_path,
                '_group': 'remote',
                '_lane': None,
                '_local_source': None,
                '_observe_source': self._observe_source,
                '_open_group_state': self._open_state,
                '_provider_reader': self._provider_reader,
                '_stop': self._stop,
            }
        )


@dataclass(frozen=True)
class _PressureCatchupContext:
    _channel: GroupChannel
    _client: EventClient
    _lane: GroupLane
    _state: GroupState


class _PressureCleanupServer:
    def __init__(self, attempts: list[str]) -> None:
        self.attempts = attempts

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        self.attempts.append('server')

    def dispatch_join_requests(self, _source: object) -> None:
        pass

    def install_grant(self, _grant: object) -> None:
        pass

    def start(self) -> None:
        pass


@dataclass(frozen=True)
class _PressureJoinContext:
    _group: GroupConfig
    _plugin: PluginSpec
    _provider: Provider
    _site: SiteConfig
    _socket_path: Path


class _PressureJoinSource:
    def __init__(self, *, boundary: int, live_event_id: int | None) -> None:
        self._boundary = boundary
        self._live_event_id = live_event_id

    def abort(self, _abort: JoinAbort) -> None:
        pass

    def confirm(self, confirmation: JoinConfirm) -> Attached:
        return Attached(confirmation.identity)

    def event_channel_lost(self, _group: str, *, reason: str) -> None:
        assert reason == 'event-channel-lost'

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        return JoinFinalized(finalization.identity)

    def request(
        self,
        request: JoinRequest,
        remote_channel: GroupChannel,
    ) -> JoinTarget:
        identity = LiveJoinIdentity(
            site=request.attempt.site,
            source=request.attempt.source,
            group=request.attempt.group,
            join_kind=request.attempt.join_kind,
            source_generation='source-1',
            group_generation=request.attempt.group_generation,
            boundary=self._boundary,
            attempt_token=request.attempt.attempt_token,
        )
        if self._live_event_id is not None:
            event = Event(_runtime_event(self._live_event_id))
            assert remote_channel.offer(
                EventDelivery(
                    EncodedEvent.from_event_frame(
                        self._live_event_id,
                        encode_event(event),
                    )
                )
            )
        return JoinTarget(
            identity,
            source_successful_clock=float(self._boundary),
            stream_incarnation='incarnation-1',
        )


class _ProbeLane:
    def __init__(self) -> None:
        self._delivered = False

    def deliver_many(self, _deliveries: object) -> None:
        self._delivered = True


class _ProbeState:
    def snapshot(self) -> object:
        return SimpleNamespace(
            cursor=40,
            delivery_floor=40,
            replay_required_from=None,
            status='running',
            epochs=(),
        )

    def strict_obligation_fence(self) -> None:
        return None

    def unresolved_obligation_bounds(self) -> tuple[tuple[int, int], ...]:
        return ()


class _ProviderConfig:
    max_rpc_attempts = 0
    rpc_attempt_interval = 0
    timeout_secs = 0.0


@dataclass(frozen=True)
class _RecoveryAssignmentProbe:
    conditional_park: ConditionalParkDirective | None
    group_generation: int
    start_mode: Literal['initialize', 'resume']


class _RecoveryConstructionClient:
    def __init__(self) -> None:
        self.last_join_target = SimpleNamespace(
            identity=SimpleNamespace(boundary=42),
            stream_incarnation='incarnation-1',
        )

    def close(self) -> None:
        pass


class _RecoveryConstructionControl:
    def __init__(self, operations: list[str]) -> None:
        self._messages: list[object] = []
        self._operations = operations

    def send(self, message: object) -> None:
        self._messages.append(message)
        self._operations.append('ready')


class _RecoveryConstructionReader:
    def __init__(self, operations: list[str]) -> None:
        self._operations = operations

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        pass

    def catch_up(self, **options: object) -> object:
        self._operations.append('catchup')
        deliver = cast('Callable[[Sequence[Event]], None]', options['deliver_page'])
        deliver((Event(_runtime_event(42)),))
        return SimpleNamespace(status='caught-up')

    def close(self) -> None:
        pass


class _RecoveryConstructionState:
    def __init__(
        self,
        operations: list[str],
        *,
        outcome: Literal['parked', 'stale'],
        resulting_revision: int,
        recovery_error: RuntimeError | None,
    ) -> None:
        self._decided_at_wall_clock: float | None = None
        self._operations = operations
        self._outcome: Literal['parked', 'stale'] = outcome
        self._recovery_error = recovery_error
        self._resulting_revision = resulting_revision

    def close(self) -> None:
        pass

    def resolve_conditional_park(
        self,
        directive: ConditionalParkDirective,
        *,
        identity: AuditIdentity,
        decided_at_wall_clock: float,
    ) -> ConditionalParkResult:
        del identity
        self._operations.append('resolve')
        self._decided_at_wall_clock = decided_at_wall_clock
        if self._recovery_error is not None:
            raise self._recovery_error
        return ConditionalParkResult(
            evidence_id=directive.evidence_id,
            outcome=self._outcome,
            resulting_revision=self._resulting_revision,
        )

    def snapshot(self) -> object:
        return SimpleNamespace(
            cursor=40,
            delivery_floor=40,
            replay_required_from=None,
            status='running',
        )

    def unresolved_obligation_bounds(self) -> tuple[()]:
        return ()


@dataclass(frozen=True)
class _RecoveryContextProbe:
    _assignment: _RecoveryAssignmentProbe
    _group: object
    _site: object
    _source_config: object


class _RecoveryGateCoordination:
    def _service(self, *, scan_obligations: bool = True, force: bool = False) -> object:
        del force
        del scan_obligations
        return None

    def synchronize(self) -> object:
        return object()


class _RecoveryGateLane:
    def __init__(
        self,
        operations: list[str],
        stop: threading.Event,
        watchdog: ComponentWatchdog,
    ) -> None:
        self._operations = operations
        self._stop = stop
        self.watch_phase = watchdog.watch

    def close(self) -> None:
        pass

    def deliver_many_counted(self, deliveries: Iterable[object]) -> tuple[object, int]:
        self._operations.append('callbacks')
        return SimpleNamespace(status='running'), len(tuple(deliveries))

    def drain_live(
        self,
        _channel: object,
        *,
        idle_snapshot: object | None = None,
        deadline_at: float = float('inf'),
    ) -> object:
        del idle_snapshot, deadline_at
        self._operations.append('delivery')
        self._stop.set()
        return SimpleNamespace(status='running')

    def join(
        self,
        _client: object,
        _channel: object,
        *,
        catch_up: Callable[[int], None],
    ) -> object:
        self._operations.append('join')
        catch_up(42)
        return object()

    def resume(self) -> object:
        self._operations.append('resume')
        return SimpleNamespace(status='running')

    def setup(self) -> None:
        self._operations.append('setup')

    def shutdown_requested(self) -> bool:
        return self._stop.is_set()


class _RecoveryConstructionReachedError(RuntimeError):
    pass


class _RestartProviderState:
    def __init__(self) -> None:
        self._events: dict[int, dict[str, object]] = {}
        self._lock = threading.Lock()
        self._observed: dict[int, set[str]] = {}

    def _publish(self, event_id: int) -> None:
        with self._lock:
            self._events[event_id] = _runtime_event(event_id)

    def _record_query(self, rows: list[dict[str, object]]) -> None:
        with self._lock:
            for row in rows:
                event_id = row['id']
                if type(event_id) is int:
                    self._observed.setdefault(event_id, set()).add(threading.current_thread().name)

    def _rows(self) -> tuple[dict[str, object], ...]:
        with self._lock:
            return tuple(self._events[key] for key in sorted(self._events))

    def _wait_for_query(self, event_id: int, thread_fragment: str) -> bool:
        for _ in range(2_000):
            with self._lock:
                names = self._observed.get(event_id, set())
                if any(thread_fragment in name for name in names):
                    return True
            time.sleep(0.001)
        return False


class _AsyncRestartProviderHandle:
    def __init__(self, state: _RestartProviderState) -> None:
        self._sync = _RestartProviderHandle(state)
        self.config = self._sync.config

    async def close(self) -> None:
        self._sync.close()

    async def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        return self._sync.find(entity_type, filters, fields, **options)

    def set_session_uuid(self, session_uuid: str | None) -> None:
        self._sync.set_session_uuid(session_uuid)


class _InitializationOutageProviderState(_RestartProviderState):
    def __init__(self) -> None:
        super().__init__()
        self._initialization_attempted = threading.Event()
        self._initialization_ready = threading.Event()


class _RestartProviderHandle:
    def __init__(self, state: _RestartProviderState) -> None:
        self.config = _ProviderConfig()
        self._state = state

    @staticmethod
    def _bounds(filters: object) -> tuple[int, bool, int | None]:
        low_id = 0
        inclusive = False
        high_id: int | None = None
        if not isinstance(filters, list):
            return low_id, inclusive, high_id
        for clause in cast('list[object]', filters):
            if not isinstance(clause, list):
                continue
            clause_items = cast('list[object]', clause)
            if len(clause_items) != 3:
                continue
            field, operator, value = clause_items
            if field != 'id' or type(value) is not int:
                continue
            if operator == 'greater_than':
                low_id = value
                inclusive = False
            elif operator == 'greater_than_or_equal':
                low_id = value
                inclusive = True
            elif operator == 'less_than_or_equal':
                high_id = value
            elif operator == 'less_than':
                high_id = value - 1
        return low_id, inclusive, high_id

    def close(self) -> None:
        return None

    def find(
        self,
        _entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        rows = self._state._rows()
        order = options.get('order')
        if order:
            direction = order[0].get('direction')
            if direction == 'desc' and not filters:
                latest = 41 if not rows else cast('int', rows[-1]['id'])
                return [{'id': latest}]
            if direction == 'asc' and not filters:
                return [{'id': 1}]
        low_id, inclusive, high_id = self._bounds(filters)
        selected = [
            row
            for row in rows
            if (cast('int', row['id']) >= low_id if inclusive else cast('int', row['id']) > low_id)
            and (high_id is None or cast('int', row['id']) <= high_id)
        ]
        limit = options.get('limit')
        if type(limit) is int and limit > 0:
            selected = selected[:limit]
        if fields == ['id']:
            result = [{'id': row['id']} for row in selected]
            self._state._record_query(result)
            return result
        self._state._record_query(selected)
        return selected

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        return None


class _InitializationOutageProviderHandle(_RestartProviderHandle):
    def __init__(self, state: _InitializationOutageProviderState) -> None:
        super().__init__(state)
        self._initialization_state = state

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        order = options.get('order')
        if not filters and order and order[0].get('direction') == 'desc':
            self._initialization_state._initialization_attempted.set()
            if not self._initialization_state._initialization_ready.is_set():
                raise RuntimeError('source initialization unavailable')
        return super().find(
            entity_type,
            filters,
            fields,
            **options,
        )


class _RetentionCoordinationReader:
    def __init__(
        self,
        floors: tuple[int | None, ...],
        events: tuple[Event, ...] = (),
    ) -> None:
        self._events = events
        self._floors = iter(floors)
        self._ranges: list[tuple[int, int]] = []

    def retention_floor(self) -> int | None:
        return next(self._floors)

    def scan_range(self, low_id: int, high_id: int) -> Generator[Event, None, None]:
        self._ranges.append((low_id, high_id))
        yield from self._events


class _RetentionCoordinationState:
    def __init__(self, *, delivery_floor: int = 50) -> None:
        self._audited: list[int] = []
        self._delivery_floor = delivery_floor
        self._revision = 0
        self._status = 'running'
        self._truncated_floor: int | None = None
        self._pending = (
            PendingRange(
                obligation_id='range-1',
                low_id=40,
                high_id=60,
                closed=True,
                close_generation=1,
                eligibility_point=0.0,
                late_ids=(),
            ),
        )

    def pending_ranges(self) -> tuple[PendingRange, ...]:
        return self._pending

    def refresh_ranges(
        self, _last_revision: int | None = None
    ) -> tuple[SourceProgress, tuple[PendingRange, ...]]:
        return _synchronization(60), self._pending

    def snapshot(self) -> object:
        return SimpleNamespace(
            delivery_floor=self._delivery_floor,
            revision=self._revision,
            status=self._status,
        )

    def mark_history_truncated(
        self,
        retention_floor: int,
        *,
        expected_revision: int,
    ) -> object:
        assert expected_revision == self._revision
        self._revision += 1
        self._status = 'history-truncated'
        self._truncated_floor = retention_floor
        return self.snapshot()

    def record_delivery_floor_skip(
        self,
        event_id: int,
        *,
        expected_revision: int,
    ) -> object:
        assert expected_revision == self._revision
        self._audited.append(event_id)
        self._revision += 1
        return self.snapshot()


class _RetentionCoordinationLane:
    def __init__(self, state: _RetentionCoordinationState) -> None:
        self._delivered: list[int] = []
        self._resolved = False
        self._state = state

    def scan_obligation(
        self,
        _revision: object,
        events: Iterable[Event],
        *,
        source_successful_clock: float | None = None,
    ) -> object:
        del source_successful_clock
        self._delivered.extend(cast('int', event['id']) for event in events)
        self._resolved = True
        return self._state.snapshot()


class _RuntimeProviderHandle:
    def __init__(
        self,
        latest: Callable[[], int],
        calls: list[str] | None = None,
    ) -> None:
        self.config = _ProviderConfig()
        self._calls = calls
        self._latest = latest

    def close(self) -> None:
        pass

    def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        if self._calls is not None:
            self._calls.append(threading.current_thread().name)
        order = options.get('order')
        if order:
            direction = order[0].get('direction')
            if direction == 'desc':
                return [{'id': self._latest()}]
            if direction == 'asc':
                return [{'id': 1}]
        return []

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        pass


class _GapProviderHandle(_RuntimeProviderHandle):
    def find(
        self,
        _entity_type: str,
        filters: object,
        _fields: list[str] | None = None,
        **_options: object,
    ) -> list[dict[str, object]]:
        if not isinstance(filters, list):
            return []
        filter_items = cast('list[object]', filters)
        if not filter_items or not isinstance(filter_items[0], list):
            return []
        clause = cast('list[object]', filter_items[0])
        if (
            len(clause) == 3
            and clause[0:2] == ['id', 'greater_than']
            and type(clause[2]) is int
            and clause[2] < 60
        ):
            return [_runtime_event(60)]
        return []


class _OutageProviderHandle(_RuntimeProviderHandle):
    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        if isinstance(filters, list) and filters:
            raise RuntimeError('token=provider-secret outage')
        return super().find(
            entity_type,
            cast('object', filters),
            fields,
            **options,
        )


class _RangeCloseProviderHandle(_RuntimeProviderHandle):
    def __init__(self) -> None:
        super().__init__(lambda: 102)
        self._gap_served = False
        self._recheck_returned = threading.Event()

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        if isinstance(filters, list):
            clauses = cast('list[object]', filters)
            if clauses == [['id', 'greater_than', 100]] and not self._gap_served:
                self._gap_served = True
                return [_runtime_event(102)]
            if clauses == [
                ['id', 'greater_than', 100],
                ['id', 'less_than', 102],
            ]:
                self._recheck_returned.set()
                return []
            if clauses:
                return []
        return super().find(entity_type, cast('object', filters), fields, **options)


class _RuntimeStateOptions(TypedDict):
    in_flight: NotRequired[tuple[int, CallbackManifest] | None]
    park_reason: NotRequired[str | None]


class _SourceHealthServer:
    connected = True
    socket_buffers = SimpleNamespace(requested=0, send=0, receive=0)

    @staticmethod
    def connected_group_statuses() -> tuple[()]:
        return ()

    @staticmethod
    def dispatch_join_requests(_surface: object) -> None:
        return None

    @staticmethod
    def poll(_timeout: float) -> None:
        return None


class _SourceHealthWaiter:
    @staticmethod
    def set() -> None:
        return None

    @staticmethod
    def wait(seconds: float) -> bool:
        del seconds
        return False


@dataclass
class _SourceRuntimeSession:
    _runtime: WorkerRuntime
    _parent: WorkerControl
    _child: WorkerControl
    _drain_stop: threading.Event
    _drain_thread: threading.Thread
    _endpoint: SourceEndpointReady

    def _close(self) -> None:
        _release_failed_source_runtime(
            self._runtime,
            (self._parent, self._child),
            None,
            (self._drain_stop, self._drain_thread),
        )


@dataclass(frozen=True)
class _RestartContinuity:
    _callback_entered: threading.Event
    _config: Config
    _dependent_runtime: WorkerRuntime
    _first_source: _SourceRuntimeSession
    _grants: tuple[EventPeerGrant, ...]
    _invoked: list[int]
    _provider_state: _RestartProviderState
    _release_callback: threading.Event
    _site_spec: WorkerSpec
    _source_assignment: WorkerAssignment


class _SynchronizationProbeReader:
    def __init__(self, ordering: Literal['eager', 'strict']) -> None:
        self._observed: list[SourceProgress] = []
        self._ordering = ordering
        self._queried = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def catch_up(self, **options: object) -> object:
        synchronize = cast('Callable[[], SourceProgress]', options['source_progress'])
        first = synchronize()
        self._observed.append(first)
        self._observed.append(synchronize())
        self._queried = True
        return SimpleNamespace(status='caught-up')

    def strict_step(self, **options: object) -> object:
        synchronize = cast('Callable[[], SourceProgress]', options['source_progress'])
        synchronization = synchronize()
        self._observed.append(synchronization)
        self._queried = True
        return SimpleNamespace(
            status='join-ready' if len(self._observed) == 2 else 'catching-up',
            join_ready=len(self._observed) == 2,
        )


class _SyncingClient:
    def __init__(self, synchronizations: tuple[SourceProgress, ...]) -> None:
        self._calls: list[int] = []
        self._synchronizations = iter(synchronizations)
        self.last_join_target = SimpleNamespace(
            identity=SimpleNamespace(
                boundary=45,
                source_generation='source-1',
            ),
            source_successful_clock=1.0,
            stream_incarnation='incarnation-1',
        )

    def read_progress(self, group_generation: int) -> SourceProgress:
        self._calls.append(group_generation)
        return next(self._synchronizations)


class _ProbeCoordination:
    def __init__(self, client: _SyncingClient) -> None:
        self._client = client

    @staticmethod
    def _service(*, scan_obligations: bool = True, force: bool = False) -> None:
        del force
        assert not scan_obligations

    def synchronize(self) -> SourceProgress:
        return self._client.read_progress(7)


class _TerminalWakeGate:
    def __init__(
        self,
        terminal_identity: Literal['operator-executor', 'source:events'],
    ) -> None:
        self._clock_lock = threading.Lock()
        self._clock_value = 0.0
        self._published_health_count = 0
        self._health_published = threading.Condition()
        self._monitor_wake = threading.Event()
        self._release_terminal_wake = threading.Event()
        self._terminal_identity = terminal_identity
        self._terminal_wake_started = threading.Event()
        self._bounded_join_started = threading.Event()

    def _health_count(self) -> int:
        with self._health_published:
            return self._published_health_count

    def _observe_target_thread(self, runtime: WorkerRuntime) -> None:
        threads = vars(runtime)['_threads']
        target = threads[self._terminal_identity]
        assert isinstance(target, threading.Thread)

        def join_target(timeout: float | None = None) -> None:
            if timeout == 0.0:
                self._bounded_join_started.set()
            target.join(timeout)

        threads[self._terminal_identity] = SimpleNamespace(
            is_alive=target.is_alive,
            join=join_target,
        )

    def _release(self) -> None:
        self._release_terminal_wake.set()

    def _set_monitor_wake(self) -> None:
        if threading.current_thread().name == f'sg-event-daemon:{self._terminal_identity}':
            with self._clock_lock:
                self._clock_value = 2.0
            self._monitor_wake.set()
            self._terminal_wake_started.set()
            self._release_terminal_wake.wait()
            return
        self._monitor_wake.set()

    def _install(self, runtime: WorkerRuntime) -> None:
        vars(runtime)['_monitor_wake'] = SimpleNamespace(
            clear=self._monitor_wake.clear,
            set=self._set_monitor_wake,
            wait=self._monitor_wake.wait,
        )

    def _wait_for_bounded_join(self) -> bool:
        return self._bounded_join_started.wait(1.0)

    def _wait_for_health_after(self, previous_count: int) -> bool:
        with self._health_published:
            return self._health_published.wait_for(
                lambda: self._published_health_count > previous_count,
                timeout=1.0,
            )

    def _wait_for_terminal_wake(self) -> bool:
        return self._terminal_wake_started.wait(1.0)

    def _wake_monitor(self) -> None:
        self._monitor_wake.set()

    def clock(self) -> float:
        with self._clock_lock:
            return self._clock_value

    def publish_health(self, update: WorkerHealth) -> None:
        del update
        with self._health_published:
            self._published_health_count += 1
            self._health_published.notify_all()


class _WatchdogBlockingClose:
    def __init__(
        self,
        entered: threading.Event,
        release: threading.Event,
        operations: list[str],
    ) -> None:
        self._entered = entered
        self._entered_at: float | None = None
        self._operations = operations
        self._release = release

    def close(self) -> None:
        self._operations.append('close')
        self._entered_at = time.monotonic()
        self._entered.set()
        assert self._release.wait(1.0)


def _assert_blocked_callback_health(update: HealthUpdate) -> None:
    callback_health = update.groups[0]
    callback_watchdog = next(item for item in update.watchdogs if item.kind == 'lane')
    assert (
        callback_health.phase,
        callback_health.current_event_id,
        callback_health.current_plugin_id,
        callback_health.current_callback_id,
        callback_health.delivery_attempt,
    ) == ('callback', 42, 'restart', 'record', 1)
    assert callback_health.deadline_at is not None
    assert callback_watchdog.phase == 'callback'
    assert callback_watchdog.event_id == 42
    assert callback_watchdog.plugin_id == 'restart'
    assert callback_watchdog.callback_id == 'record'
    assert callback_watchdog.delivery_attempt == 1
    assert callback_watchdog.deadline_at == callback_health.deadline_at


def _assert_bounded_checkpoint_shutdown(
    runtime: WorkerRuntime,
    release: threading.Event,
    forced: list[tuple[int, str]],
    closed_on: list[str],
) -> None:
    result: list[tuple[str, ...]] = []
    waiter = threading.Thread(target=lambda: result.append(runtime.wait()))
    waiter.start()
    try:
        waiter.join(0.3)
        assert not waiter.is_alive()
        assert forced == [(1, 'sg-event-daemon:monitor')]
        assert result == [('checkpoint:events',)]
        assert closed_on == []
        assert runtime._resources_open
    finally:
        release.set()
        waiter.join(1)


def _assert_complete_group_health(health: GroupHealth) -> None:
    assert (health.site, health.source, health.group) == (
        'main',
        'events',
        'local',
    )
    assert health.worker_instance == 'site-spawn-1'
    assert health.execution == 'sync'
    assert health.ordering == 'eager'
    assert health.phase in {'callback', 'idle', 'internal', 'setup'}
    assert health.status
    assert type(health.cursor) is int
    assert health.current_event_id is None
    assert health.current_plugin_id is None
    assert health.current_callback_id is None
    assert health.delivery_attempt is None
    assert health.heartbeat_at >= 0.0
    assert health.heartbeat_age >= 0.0
    assert health.queue_event_count >= 0
    assert health.queue_byte_count >= 0
    assert health.catchup_frontier is None or health.catchup_frontier >= 0
    assert health.pending_catchup_epoch_count >= 0
    assert health.oldest_catchup_epoch_age is None or health.oldest_catchup_epoch_age >= 0.0
    assert health.join_phase in {
        'attached',
        'catching-up',
        'detached',
        'joining',
    }
    assert health.outstanding_late_obligation_count >= 0
    assert isinstance(health.outstanding_late_ids, tuple)
    assert not health.parked
    assert health.park_reason is None
    assert health.failure_kind is None
    assert health.failure_text is None
    assert health.checkpoint_state in {'complete', 'idle', 'incomplete'}
    assert isinstance(health.conditions, tuple)


def _assert_complete_source_health(health: SourceHealth) -> None:
    assert (health.site, health.source) == ('main', 'events')
    assert health.source_generation == 'source-generation-1'
    assert health.status == 'running'
    assert health.frontier >= 0
    assert health.successful_clock >= 0.0
    assert health.retry_attempt == 0
    assert health.next_retry_secs is None
    assert health.open_range_count >= 0
    assert health.late_id_count >= 0
    assert health.last_error is None
    assert health.worker_instance == 'site-spawn-1'
    assert health.initialized
    assert health.pending_obligation_count >= 0
    assert health.orphan_marker_count >= 0
    assert health.strict_buffer_event_count >= 0
    assert health.strict_buffer_byte_count >= 0
    assert isinstance(health.strict_retention, tuple)
    assert health.checkpoint_state in {'complete', 'idle', 'incomplete'}


def _assert_detached_strict_join(
    component: _LaneComponent,
    resources: _LaneResources,
    observations: tuple[list[int], list[str], list[float]],
    case: str,
) -> None:
    callbacks, trace, wall = observations
    state = resources._state
    lane = resources._lane
    assert lane.setup() is None
    with closing(
        GroupChannel(
            group='local',
            ordering='strict',
            max_events=8,
            max_bytes=262_144,
        )
    ) as channel:
        if case == 'callback-stop':
            with pytest.raises(RuntimeError, match='component stopped'):
                lane.join(
                    resources._client,
                    channel,
                    catch_up=partial(component._catch_up, resources),
                )
            assert callbacks == [42]
            assert state.snapshot().epochs
            assert state.snapshot().obligation_count == 1
            assert state.snapshot().latest_join_boundary is None
            assert not channel.status.attached
            assert 'confirm' not in trace
            return
        identity = lane.join(
            resources._client,
            channel,
            catch_up=partial(component._catch_up, resources),
        )
    assert isinstance(identity, LiveJoinIdentity)
    assert identity.boundary == 51
    assert state.snapshot().epochs == ()
    assert state.snapshot().cursor == 51
    assert callbacks == ([42, 44, 46, 48, 50] if case == 'new-fence' else [42, 44, 48, 50])
    assert trace[-1] == 'confirm'
    if case == 'new-fence':
        assert trace[:3] == ['callback-42', 'import-earlier', 'callback-44']
    assert wall[0] >= 300.0


def _assert_interrupted_worker_stopped(
    runtime: WorkerRuntime, error_type: type[BaseException]
) -> None:
    assert runtime._stop_event.is_set()
    assert runtime.fatal_reason == f'lane:local thread failed: {error_type.__name__}'
    assert runtime._shutdown_deadline_at is not None
    assert 'lane:local' in runtime._completed_thread_names


def _assert_maximum_watchdog_health(maximum: HealthUpdate) -> None:
    assert tuple(
        (watchdog.kind, watchdog.source, watchdog.group) for watchdog in maximum.watchdogs
    ) == (
        ('source', 'events', None),
        ('lane', 'events', 'local'),
        ('operator', 'events', 'local'),
    )
    assert maximum.sources[0].phase == 'write-lock-wait'
    assert maximum.groups[0].phase == 'write-lock-wait'
    assert maximum.sources[0].deadline_at is not None
    assert maximum.groups[0].deadline_at is not None


def _assert_monitor_stopped(runtime: WorkerRuntime) -> None:
    monitor = vars(runtime)['_monitor_thread']
    assert isinstance(monitor, threading.Thread)
    assert not monitor.is_alive()


def _assert_remote_initialization_pending(
    runtime: WorkerRuntime,
    pending_results: Collection[object],
    pending_remote: threading.Event,
) -> None:
    for _ in range(1_000):
        if pending_remote.is_set() or runtime.fatal_reason is not None:
            break
        time.sleep(0.001)

    assert runtime.fatal_reason is None
    assert pending_remote.is_set()
    remote_pending = tuple(
        result for result in pending_results if vars(result).get('group') == 'remote'
    )
    assert remote_pending
    assert all(
        vars(result)
        == {
            'source': 'events',
            'group': 'remote',
            'group_generation': 7,
            'retry_after_secs': 0.01,
        }
        for result in remote_pending
    )
    threads = runtime._threads
    assert threads['lane:healthy'].is_alive()
    assert threads['lane:remote'].is_alive()


def _assert_shutdown_inventory(update: HealthUpdate, deadline: float | None) -> None:
    assert {watchdog.kind for watchdog in update.watchdogs} == {'source', 'lane', 'operator'}
    assert {watchdog.deadline_at for watchdog in update.watchdogs} == {deadline}
    assert len(update.sources) == len(update.groups) == 1


def _attempt_cleanup(cleanup: Callable[[], None], errors: list[BaseException]) -> None:
    try:
        cleanup()
    except BaseException as error:  # noqa: BLE001
        errors.append(error)


def _bind_worker_range(state: GroupState, obligation_id: str, event_id: int) -> None:
    connection = state._database.connection
    with state._writes.transaction(connection):
        insert_range(connection, obligation_id, event_id, event_id, state='closed')
        bind_group(connection, obligation_id, 'local', 7, floor=41, eligibility_point=3.0)


def _configure_shutdown_probe_clock(monkeypatch: pytest.MonkeyPatch, now: list[float]) -> None:
    original_start = WorkerRuntime.start
    original_construct = WorkerRuntime._construct_components

    def construct(runtime: WorkerRuntime) -> None:
        original_construct(runtime)
        for component in runtime._components.values():
            assert isinstance(component, (_SourceComponent, _LaneComponent))
            watchdog = component._watchdog
            monkeypatch.setattr(watchdog, '_clock', lambda: now[0])
            phase = 'wait' if isinstance(component, _SourceComponent) else 'idle'
            watchdog.transition(phase, deadline_at=101.0)
            sample = watchdog.sample()
            WatchdogHealth(
                kind=sample.kind,
                site='main',
                source='events',
                group='local' if sample.kind == 'lane' else None,
                worker_instance=sample.worker_instance,
                phase=sample.phase,
                heartbeat_at=sample.heartbeat_at,
                deadline_at=sample.deadline_at,
            )

    def start(runtime: WorkerRuntime) -> None:
        monkeypatch.setattr(runtime, '_clock', lambda: now[0])
        assert runtime._operator_watchdog is not None
        monkeypatch.setattr(runtime._operator_watchdog, '_clock', lambda: now[0])
        runtime._operator_watchdog.transition('idle', deadline_at=None)
        original_start(runtime)

    monkeypatch.setattr(WorkerRuntime, '_construct_components', construct)
    monkeypatch.setattr(WorkerRuntime, 'start', start)


def _detached_holdback_wait(
    component: _LaneComponent,
    close_range: Callable[[], None],
    wall: list[float],
    *,
    check_watchdog: bool,
) -> threading.Event:
    watchdog = component._watchdog
    limiter = component._limiter

    class HoldbackWait(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            assert timeout is not None
            assert wall[0] < 900.0, 'detached join stalled'
            sample = watchdog.sample()
            if check_watchdog:
                assert sample.phase == 'idle'
                assert sample.deadline_at is not None
                assert sample.deadline_at > wall[0] + timeout
                with limiter._slot():
                    pass
            if wall[0] == 100.0:
                close_range()
            wall[0] += 50.0
            return self.is_set()

    return HoldbackWait()


def _drift_disposition_policy(path: Path, group: str, drift: str) -> tuple[str, ...]:
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            'INSERT INTO epochs (group_name, low_id, high_id, captured_at, eligibility_point, '
            'reader_clock_checkpoint, source_generation, group_generation, state) '
            "SELECT group_name, 42, 45, 1, 2, 1, source_generation, group_generation, 'waiting' "
            'FROM groups CROSS JOIN source_meta WHERE group_name = ?',
            (group,),
        )
        if drift == 'identity':
            connection.execute("UPDATE source_meta SET stream_incarnation = 'later-incarnation'")
        else:
            connection.execute(
                "UPDATE groups SET ordering_mode = 'strict' WHERE group_name = ?", (group,)
            )
        return tuple(connection.iterdump())


def _gap_runtime_provider() -> Provider:

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _GapProviderHandle:
        del site
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return _GapProviderHandle(lambda: 60)

    return Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(
            sync=True,
            async_=False,
        ),
        _sync_factory=factory,
        _async_factory=None,
    )


def _gate_checkpoint_startup_operator(
    runtime: WorkerRuntime,
    monkeypatch: pytest.MonkeyPatch,
    checkpoint_release: threading.Event,
    checkpoint_join_timeouts: list[float | None],
) -> tuple[threading.Event, threading.Event]:
    original_join = threading.Thread.join
    original_operator_main = runtime._operator_main
    operator_returned = threading.Event()
    operator_release = threading.Event()

    def join(thread: threading.Thread, timeout: float | None = None) -> None:
        if thread.name == 'sg-event-daemon:checkpoint:events' and not checkpoint_release.is_set():
            checkpoint_join_timeouts.append(timeout)
        if (
            thread.name == 'sg-event-daemon:operator-executor'
            and timeout is not None
            and timeout > 0
        ):
            operator_release.set()
        original_join(thread, timeout)

    def operator_main() -> None:
        original_operator_main()
        operator_returned.set()
        assert operator_release.wait(10)

    monkeypatch.setattr(threading.Thread, 'join', join)
    monkeypatch.setattr(runtime, '_operator_main', operator_main)
    return operator_returned, operator_release


def _group(
    name: str,
    *,
    worker: str,
    source: str = 'events',
    execution: Literal['sync', 'async'] = 'sync',
    ordering: Literal['eager', 'strict'] = 'eager',
) -> GroupConfig:
    return GroupConfig(
        name=name,
        source=source,
        plugins=(),
        callback_credentials='source',
        worker=worker,
        execution=execution,
        ordering=ordering,
    )


def _bootstrap_coordination_group(
    path: Path, *, ordering: Literal['eager', 'strict'] = 'eager'
) -> None:
    with open_group_on_source(
        path,
        writes=write_context_for(path),
        site='main',
        site_server_url='https://example.test',
        source='events',
        group='local',
        group_generation=7,
        ordering=ordering,
        stream_fingerprint='stream',
        source_generation='source-1',
        verified_incarnation='incarnation-1',
        start_mode='initialize',
        initial_cursor=41,
        initial_delivery_floor=41,
        initial_last_delivered_id=41,
        strict_holdback_secs=300.0,
    ):
        pass


def _health_failure_plugin(
    failure_kind: Literal['setup', 'callback'],
) -> PluginSpec:

    def callback(_context: EventContext) -> None:
        raise RuntimeError('password=callback-secret failure')

    def setup(_context: SetupContext) -> None:
        raise RuntimeError('token=setup-secret failure')

    identity = CallbackIdentity(
        ordinal=0,
        plugin_id='health-failure',
        callback_id='fail',
        execution='sync',
        filter_digest='all-events',
        config_digest='empty-config',
        secret_refs=(),
        credential_name='source',
        credential_fingerprint='source:daemon',
    )
    return PluginSpec(
        id='health-failure',
        module_name='tests.health_failure',
        credentials='source',
        config=MappingProxyType({}),
        secrets=(),
        setup=setup if failure_kind == 'setup' else None,
        callbacks=(
            ()
            if failure_kind == 'setup'
            else (
                CallbackSpec(
                    id='fail',
                    callback=callback,
                    events=None,
                    identity=identity,
                ),
            )
        ),
    )


def _catchup_tick_plugin(clock: list[float], invoked: list[int]) -> PluginSpec:
    def callback(context: EventContext) -> None:
        invoked.append(cast('int', context.event['id']))
        clock[0] += 0.25

    plugin = _health_failure_plugin('callback')
    return replace(plugin, callbacks=(replace(plugin.callbacks[0], callback=callback),))


def _health_failure_plugin_loader(
    failure_kind: Literal['setup', 'callback'],
) -> Callable[[SiteConfig, GroupConfig, Provider], tuple[PluginSpec, ...]]:

    def load(
        _site: SiteConfig,
        _group: GroupConfig,
        _provider: Provider,
    ) -> tuple[PluginSpec, ...]:
        return (_health_failure_plugin(failure_kind),)

    return load


def _initialization_outage_provider(
    state: _InitializationOutageProviderState,
) -> Provider:

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _InitializationOutageProviderHandle:
        del site
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return _InitializationOutageProviderHandle(state)

    return Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(
            sync=True,
            async_=False,
        ),
        _sync_factory=factory,
        _async_factory=None,
    )


def _initializing_source_surface(state: SourceState) -> _SourceJoinSurface:
    return worker_module._SourceJoinSurface(
        bootstrap_retry_secs=0.05,
        inventory={
            'local': ActiveSourceGroupAssignment(
                source='events', group='local', group_generation=7, start_mode='initialize'
            )
        },
        coordinator=cast('JoinCoordinator', SimpleNamespace(group_died=Mock())),
        poller=cast('SourcePoller', SimpleNamespace(event_channel_lost=Mock())),
        state=state,
        strict_buffer=None,
    )


def _inject_detached_coordination(
    state: GroupState,
    provider_state: _RestartProviderState,
    observations: tuple[list[int], list[str]],
    case: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    callbacks, trace = observations
    original_refresh = state.refresh_ranges
    imported_earlier = False
    earlier_pending = False

    def refresh(
        last_revision: int | None = None,
    ) -> tuple[SourceProgress, tuple[PendingRange, ...] | None]:
        nonlocal imported_earlier, earlier_pending
        connection = state._database.connection
        if callbacks == [42] and case == 'new-fence' and not imported_earlier:
            imported_earlier = True
            with state._writes.transaction(connection):
                insert_range(connection, 'restored-earlier-fence', 44, 44)
                bind_group(
                    connection,
                    'restored-earlier-fence',
                    'local',
                    7,
                    floor=41,
                    eligibility_point=2.0,
                )
            earlier_pending = True
            provider_state._publish(44)
            trace.append('import-earlier')
        elif earlier_pending:
            with state._writes.transaction(connection):
                close_test_range(connection, 'restored-earlier-fence')
            earlier_pending = False
        return original_refresh(last_revision)

    monkeypatch.setattr(state, 'refresh_ranges', refresh)


def _install_recovery_gate_probes(
    monkeypatch: pytest.MonkeyPatch,
    component: _LaneComponent,
    operations: list[str],
) -> None:
    stop = component._stop
    channel = _LifecycleChannel()
    coordination = _RecoveryGateCoordination()
    lane = _RecoveryGateLane(
        operations,
        stop,
        component._watchdog,
    )

    def load_plugins(_loader: object) -> object:
        operations.append('plugins')
        return object()

    def build_channel(_context: object, _state: object) -> object:
        return channel

    def build_coordination(**_options: object) -> object:
        return coordination

    def build_lane(**_options: object) -> object:
        return lane

    def database_path() -> str:
        operations.append('checkpoint-path')
        return 'recovery-gate.db'

    def publish_health(_resources: object, _lifecycle: object) -> None:
        return None

    monkeypatch.setattr(worker_module, 'GroupLane', build_lane)
    monkeypatch.setattr(worker_module, '_LaneRefresh', build_coordination)
    monkeypatch.setattr(worker_module, '_load_shared_plugins', load_plugins)
    vars(component).update(
        {
            '_build_channel': build_channel,
            '_database_path': database_path,
            '_publish_health': publish_health,
        }
    )


def _lane_bootstrap_operation(
    stage: Literal['connect', 'observation'],
    component: _LaneComponent,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    block: Callable[[], None],
) -> Callable[[], object]:
    component_type = type(component)
    context = cast('Callable[[object], object]', vars(component_type)['_configuration'])(component)
    if stage == 'connect':
        assignment = component._assignment
        source = vars(context)['_group'].source
        vars(component)['_assignment'] = SimpleNamespace(
            spawn_instance=assignment.spawn_instance,
            source_generations=(),
            source_endpoints=(
                SourceEndpointReady(
                    source=source,
                    source_generation='source-generation-1',
                    path=str(tmp_path / 'events.sock'),
                ),
            ),
            event_grants=(
                EventPeerGrant(
                    source=source,
                    group='local',
                    dependent_spawn_instance=assignment.spawn_instance,
                    capability='capability-1',
                ),
            ),
        )

        def connect(_client: EventClient) -> None:
            block()

        def operation() -> object:
            build = cast(
                'Callable[[object, object], object]',
                vars(component_type)['_build_client'],
            )
            return build(component, context)

        monkeypatch.setattr(EventClient, 'connect', connect)
        return operation
    if stage == 'observation':
        observation = GroupBootstrapObservation(
            source='events',
            group='local',
            group_generation=7,
            source_generation='source-generation-1',
            stream_incarnation='incarnation-1',
            captured_frontier=41,
            source_successful_clock=1.0,
        )

        def observe(_request: object) -> GroupBootstrapObservation:
            block()
            return observation

        def operation() -> object:
            return _LaneComponent._observe_source(
                component,
                cast('EventClient', SimpleNamespace(observe_group=observe)),
                assignment=GroupBootstrapRequest(
                    source='events',
                    group='local',
                    group_generation=7,
                    start_mode='resume',
                ),
            )

        return operation

    raise AssertionError(f'unsupported bootstrap phase: {stage}')


def _local_assignment(
    spec: WorkerSpec,
    group: str,
    start_mode: Literal['initialize', 'resume'],
) -> WorkerAssignment:
    return WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(
            DeliveryGroupAssignment(
                source='events',
                group=group,
                group_generation=7,
                start_mode=start_mode,
            ),
        ),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='events',
                group=group,
                group_generation=7,
                start_mode=start_mode,
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )


def _local_call_stop_component(
    *,
    fail_before_stop: bool,
) -> tuple[_InterruptibleComponent, threading.Event, type[Exception], str]:
    stop = threading.Event()
    entered = threading.Event()
    message = 'source component stopped during local call'
    component_type = _SourceComponent
    component = object.__new__(component_type)

    class _FailingMailbox(_OwnerMailbox[object]):
        def _submit(self, invoke: Callable[[object], object]) -> _OwnerCall[object]:
            call = super()._submit(invoke)
            call._error = ConnectionError(message)
            call._complete.set()
            return call

    vars(component).update(
        {
            '_mailbox': _FailingMailbox('source') if fail_before_stop else _OwnerMailbox('source'),
            '_stop': stop,
        }
    )
    call = component_type._call

    def invoke(_surface: object) -> None:
        return None

    def _run() -> object:
        entered.set()
        return call(component, invoke, deadline_secs=30.0, stop=stop)

    return _InterruptibleComponent(_run, stop.set), entered, ConnectionError, message


def _open_coordination_source(path: Path) -> SourceState:
    return SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site='main',
        source='events',
        stream_fingerprint='stream',
        source_generation='source-1',
        visibility_overlap_secs=1.0,
    )


def _open_group_health_state(path: Path, *, delivery_floor: int = 41) -> GroupState:
    options: GroupOpenOptions = {
        'writes': write_context_for(path),
        'site': 'main',
        'site_server_url': 'https://example.test',
        'source': 'events',
        'group': 'local',
        'group_generation': 7,
        'ordering': 'eager',
        'stream_fingerprint': stream_fingerprint(
            'https://example.test',
            'source',
            'daemon',
        ),
        'source_generation': 'source-generation-1',
        'verified_incarnation': 'stream-incarnation-1',
        'start_mode': 'resume' if group_exists(path, 'local') else 'initialize',
    }
    if options['start_mode'] == 'initialize':
        options['initial_cursor'] = 41
        options['initial_delivery_floor'] = delivery_floor
        options['initial_last_delivered_id'] = delivery_floor
    return open_group_on_source(path, **options)


def _outage_runtime_provider() -> Provider:

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _OutageProviderHandle:
        del site
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return _OutageProviderHandle(lambda: 41)

    return Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(
            sync=True,
            async_=False,
        ),
        _sync_factory=factory,
        _async_factory=None,
    )


def _pending_group_command(
    snapshot: GroupSnapshot, command: Literal['replay', 'reset-to-latest']
) -> OperatorCommand:
    return OperatorCommand(
        request_id='pending-disposition',
        command='group-disposition',
        arguments={
            'site': 'main',
            'source': 'events',
            'group': 'local',
            'command': command,
            'disposition_id': 'pending-disposition',
            'reserved_generation': 11,
            'expected_revision': snapshot.revision,
            'from_event_id': 40 if command == 'replay' else None,
            'reason': 'restore configured group',
            'acknowledge_truncation': False,
        },
        identity=AuditIdentity.current(),
    )


def _provider_loader(
    provider: Provider,
) -> Callable[[str, Collection[str]], Provider]:

    def load(expected: str, required: Collection[str]) -> Provider:
        del expected, required
        return provider

    return load


def _publish_group_health(
    component: object,
    resources: object,
    lifecycle: object,
) -> None:
    assert isinstance(component, _LaneComponent)
    component._publish_health(cast('_LaneResources', resources), cast('_LaneLifecycle', lifecycle))


def _raise_cleanup_errors(initiating: BaseException | None, errors: list[BaseException]) -> None:
    if not errors:
        return
    first = initiating if initiating is not None else errors.pop(0)
    for error in errors:
        first.add_note(f'cleanup failure: {error!r}')
    if initiating is None:
        raise first


def _close_pressure_join(
    consumers: tuple[EventClient, GroupChannel, GroupLane],
    source: tuple[threading.Event, threading.Thread | None],
    server: EventServer,
) -> None:
    client, channel, lane = consumers
    stop, source_thread = source
    initiating = sys.exception()
    errors: list[BaseException] = []
    cleanups = [client.close, stop.set]
    if source_thread is not None:
        cleanups.append(partial(source_thread.join, 1.0))
    cleanups.extend((channel.close, lane.close))
    for cleanup in cleanups:
        _attempt_cleanup(cleanup, errors)
    if source_thread is not None and source_thread.is_alive():
        errors.append(AssertionError('source thread remained alive; retained server'))
    else:
        _attempt_cleanup(server.close, errors)
    _raise_cleanup_errors(initiating, errors)


def _close_remote_coordination_client(
    client: EventClient | None,
    stop: threading.Event,
    future: Future[None] | None,
    executor: ThreadPoolExecutor,
) -> None:
    initiating = sys.exception()
    errors: list[BaseException] = []
    cleanups = [stop.set] if client is None else [client.close, stop.set]
    if future is not None:
        cleanups.append(partial(future.result, timeout=1.0))
    for cleanup in cleanups:
        _attempt_cleanup(cleanup, errors)
    _attempt_cleanup(lambda: executor.shutdown(wait=future is not None and future.done()), errors)
    _raise_cleanup_errors(initiating, errors)


def _receive_type(
    control: WorkerControl,
    expected: type[_Message],
    *,
    deadline: float | None = None,
) -> _Message:
    if deadline is None:
        deadline = time.monotonic() + 10.0
    # Unrelated health traffic must not consume the startup allowance.
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            message = control.receive(timeout=min(1.0, remaining))
        except TimeoutError:
            continue
        if isinstance(message, expected):
            return message
    raise AssertionError(f'worker did not publish {expected.__name__}')


def _assert_operator_retry(control: WorkerControl, request_id: str) -> None:
    control.send(
        OperatorCommand(
            request_id=request_id,
            command='retry',
            arguments={'site': 'main', 'group': 'local'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=123),
        )
    )
    result = _receive_type(control, CommandResult)
    assert result.ok, result.message


def _receive_typed_health(
    control: WorkerControl,
    ready: Callable[[HealthUpdate], bool],
) -> HealthUpdate:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        update = _receive_type(control, HealthUpdate, deadline=deadline)
        if ready(update):
            return update
    raise AssertionError('worker did not publish complete typed health')


def _assert_callback_heartbeat_slides_real_health(
    control: WorkerControl,
    update: HealthUpdate,
    context: EventContext,
) -> None:
    deadline_at = update.groups[0].deadline_at
    assert deadline_at is not None
    context.heartbeat()
    update = _receive_typed_health(
        control,
        lambda item: any(
            group_health.phase == 'callback'
            and group_health.deadline_at is not None
            and group_health.deadline_at > deadline_at
            for group_health in item.groups
        ),
    )
    _assert_blocked_callback_health(update)


def _assert_remote_client_health(
    control: WorkerControl,
    config: Config,
) -> None:
    update = _receive_typed_health(
        control,
        lambda item: any(socket_health.role == 'client' for socket_health in item.event_sockets),
    )
    client_health = next(item for item in update.event_sockets if item.role == 'client')
    assert client_health.source == 'events'
    assert client_health.group == 'remote'
    assert client_health.requested == config.daemon.max_ipc_frame_bytes
    assert type(client_health.send) is int
    assert type(client_health.receive) is int
    assert client_health.queue_event_count >= 0
    assert client_health.queue_byte_count >= 0


def _assert_remote_server_health(
    control: WorkerControl,
    config: Config,
) -> None:
    update = _receive_typed_health(
        control,
        lambda item: any(socket_health.role == 'server' for socket_health in item.event_sockets),
    )
    server_health = next(item for item in update.event_sockets if item.role == 'server')
    assert server_health.source == 'events'
    assert server_health.group is None
    assert server_health.connected
    assert server_health.requested == config.daemon.max_ipc_frame_bytes
    assert type(server_health.send) is int
    assert type(server_health.receive) is int
    assert server_health.queue_event_count >= 0
    assert server_health.queue_byte_count >= 0


def _resume_stop_component(
    *,
    fail_before_stop: bool,
) -> tuple[_InterruptibleComponent, threading.Event, type[Exception], str]:
    stop = threading.Event()
    entered = threading.Event()
    interrupted_message = 'in-flight event refetch interrupted'
    provider_message = 'simulated durable refetch provider failure'
    component_type = _LaneComponent
    component = object.__new__(component_type)
    vars(component)['_stop'] = stop

    def resume() -> object:
        entered.set()
        if fail_before_stop:
            raise RuntimeError(provider_message)
        stop.wait()
        raise InterruptedError(interrupted_message)

    lifecycle_type = worker_module._LaneLifecycle
    lifecycle = lifecycle_type(_channel=cast('GroupChannel', SimpleNamespace()))
    resume_lane = cast(
        'Callable[[object, object, object], bool]',
        vars(component_type)['_resume_lane'],
    )

    def _run() -> object:
        return resume_lane(
            component,
            SimpleNamespace(_lane=SimpleNamespace(resume=resume)),
            lifecycle,
        )

    error_type = RuntimeError if fail_before_stop else InterruptedError
    message = provider_message if fail_before_stop else interrupted_message
    return _InterruptibleComponent(_run, stop.set), entered, error_type, message


def _run_blocked_retirement(
    probe: SimpleNamespace,
    operation: Callable[[], object],
    *,
    expect_reconnect: bool,
) -> tuple[WatchdogHealth, WatchdogHealth | None, object, float, float]:
    internal_budget = cast(
        'float',
        vars(probe.component)['_internal_stall_deadline_secs'],
    )
    before = time.monotonic()
    startup: WatchdogHealth | None = None
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(operation)
        try:
            assert probe.close_entered.wait(1.0)
            internal = cast('_ComponentHealthCache', probe.cache)._snapshot()._watchdogs[0]
            probe.close_release.set()
            if expect_reconnect:
                assert probe.reconnect_entered.wait(1.0)
                startup = cast('_ComponentHealthCache', probe.cache)._snapshot()._watchdogs[0]
                probe.reconnect_release.set()
            result = future.result(timeout=2.0)
        finally:
            probe.close_release.set()
            probe.reconnect_release.set()
    return internal, startup, result, before, internal_budget


def _run_cooperative_stop_component(
    component: _InterruptibleComponent,
    entered: threading.Event,
    *,
    request_stop: bool,
) -> tuple[str | None, list[int]]:
    spec = WorkerSpec(
        worker_id='["named","main","default"]',
        site='main',
        kind=WorkerKind._NAMED,
        name='default',
        sources=(),
        groups=('local',),
    )
    forced: list[int] = []
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: component,
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        force_exit=forced.append,
    )
    runtime.start()
    try:
        assert entered.wait(1.0)
        if request_stop:
            runtime.request_stop('test-stop')
        assert runtime.wait() == ()
    finally:
        runtime.request_stop('test-cleanup')
        assert runtime._shutdown() == ()
    return runtime.fatal_reason, forced


def _run_worker_runtime_cli(
    monkeypatch: pytest.MonkeyPatch,
    runtime: WorkerRuntime,
) -> int:

    class _ControlProbe:
        def close(self) -> None:
            return None

        def send(self, _message: object) -> None:
            return None

    config = cast(
        'Config',
        SimpleNamespace(
            daemon=SimpleNamespace(
                health_interval_secs=0.25,
                max_ipc_frame_bytes=131_072,
                worker_startup_deadline_secs=0.75,
            )
        ),
    )
    control = cast('WorkerControl', _ControlProbe())
    spec = runtime._spec

    def load_config(_path: Path) -> Config:
        return config

    def worker_spec(_config: Config, _site: str, _worker_id: str) -> WorkerSpec:
        return spec

    def control_from_fd(
        _descriptor: int,
        *,
        max_frame_bytes: int,
        send_deadline_secs: float,
    ) -> WorkerControl:
        assert max_frame_bytes == 131_072
        assert send_deadline_secs == 0.5
        return control

    def receive_assignment(
        _control: WorkerControl,
        _spec: WorkerSpec,
        _spawn_instance: str,
        *,
        timeout: float,
    ) -> WorkerAssignment:
        assert timeout == 0.75
        return WorkerAssignment(
            worker_id=spec.worker_id,
            spawn_instance=_spawn_instance,
            source_generations=tuple((source, 'generation-1') for source in spec.sources),
            group_assignments=tuple(
                DeliveryGroupAssignment('events', group, 0, 'initialize') for group in spec.groups
            ),
            source_group_assignments=(),
            source_endpoints=(),
            event_grants=(),
        )

    def build_runtime(
        _config: Config,
        _spec: WorkerSpec,
        _assignment: WorkerAssignment,
        _control: WorkerControl,
    ) -> WorkerRuntime:
        return runtime

    def ignore_logging(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(cli_module, 'load_config', load_config)
    monkeypatch.setattr(cli_module, '_worker_spec', worker_spec)
    monkeypatch.setattr(
        cli_module.WorkerControl,
        'from_fd',
        control_from_fd,
    )
    monkeypatch.setattr(cli_module, '_receive_assignment', receive_assignment)
    monkeypatch.setattr(cli_module, 'build_worker_runtime', build_runtime)
    monkeypatch.setattr(cli_module, 'configure_logging', ignore_logging)
    return cli_module.main(
        [
            'run-worker',
            '--config',
            'config.toml',
            '--site',
            spec.site,
            '--worker',
            spec.worker_id,
            '--control-fd',
            '7',
            '--spawn-instance',
            'spawn-1',
        ]
    )


def _runtime_config(state_dir: Path, site: SiteConfig) -> Config:
    return Config(
        daemon=DaemonConfig(
            state_dir=state_dir,
            sg_api_provider='shotgun-api3',
            health_interval_secs=0.01,
            shutdown_deadline_secs=2.0,
        ),
        sites=MappingProxyType({site.name: site}),
    )


def _runtime_event(event_id: int) -> dict[str, object]:
    return {
        'id': event_id,
        'type': 'EventLogEntry',
        'event_type': 'Shotgun_Test',
        'attribute_name': None,
        'meta': {},
        'entity': None,
        'user': None,
        'project': None,
        'session_uuid': None,
        'created_at': None,
    }


def _catch_up_pressure_join(
    context: _PressureCatchupContext,
    *,
    boundary: int,
    pressure: bool,
) -> None:
    if pressure:
        for _ in range(100):
            context._client.poll(0.001)
            if context._channel.status.queued_count == 1:
                break
        assert context._channel.status.queued_count == 1
        assert context._channel.status.queued_bytes > 0
    before = context._state.snapshot()
    assert before.cursor == boundary - 1
    assert context._client.last_join_target is not None
    assert context._client.last_join_target.identity.boundary == boundary
    assert context._state.snapshot().cursor == boundary - 1
    context._lane.deliver_many_counted((Delivery(Event(_runtime_event(boundary))),))


def _run_pressure_join(
    context: _PressureJoinContext,
    state: GroupState,
    *,
    boundary: int,
    live_event_id: int | None,
    pressure: bool,
) -> None:
    server = EventServer(
        path=context._socket_path,
        source='events',
        max_frame_bytes=131_072,
        queue_max_entries=1,
        queue_max_bytes=262_144,
    )
    client = EventClient(
        path=context._socket_path,
        source='events',
        group='remote',
        spawn_instance='dependent-1',
        capability='capability-1',
        max_frame_bytes=131_072,
        queue_max_entries=1,
        queue_max_bytes=262_144,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    watchdog = ComponentWatchdog(
        kind='lane',
        identity='remote',
        worker_instance='dependent-1',
        registration_token=os.urandom(16).hex(),
        startup_deadline_secs=30.0,
    )
    watchdog.transition('idle', deadline_at=None)
    lane = GroupLane(
        state=state,
        site=context._site,
        group=context._group,
        provider=context._provider,
        plugins=(context._plugin,),
        worker_instance='dependent-1',
        health_interval_secs=0.01,
        watchdog=watchdog,
    )
    assert lane.setup() is None
    channel = GroupChannel(
        group='remote',
        ordering='eager',
        max_events=1,
        max_bytes=262_144,
    )
    source = _PressureJoinSource(
        boundary=boundary,
        live_event_id=live_event_id,
    )
    catchup = _PressureCatchupContext(
        _channel=channel,
        _client=client,
        _lane=lane,
        _state=state,
    )
    stop = threading.Event()
    dispatch = cast('Callable[[object], None]', server.dispatch_join_requests)

    def serve() -> None:
        while not stop.is_set():
            server.poll(0.001)
            dispatch(source)

    server.start()
    source_thread: threading.Thread | None = None
    try:
        server.install_grant(
            EventPeerGrant(
                source='events',
                group='remote',
                dependent_spawn_instance='dependent-1',
                capability='capability-1',
            )
        )
        source_thread = threading.Thread(target=serve)
        source_thread.start()
        client.connect()
        joined = lane.join(
            client,
            channel,
            catch_up=lambda captured: _catch_up_pressure_join(
                catchup,
                boundary=captured,
                pressure=pressure,
            ),
        )
        assert isinstance(joined, LiveJoinIdentity)
        assert joined.boundary == boundary
        lane.drain_live(channel)
        assert state.snapshot().cursor == boundary + (1 if pressure else 0)
    finally:
        _close_pressure_join((client, channel, lane), (stop, source_thread), server)


def _runtime_lane(runtime: WorkerRuntime, group: str) -> GroupLane | None:
    components = runtime._components
    component = components[f'lane:{group}']
    return cast('_LaneComponent', component)._lane


def _assert_retained_history_halt(
    runtime: WorkerRuntime,
    control: WorkerControl,
    halted_serviced: threading.Event,
) -> None:
    update: HealthUpdate | None = None
    for _ in range(400):
        if runtime.fatal_reason is not None:
            break
        try:
            message = control.receive(timeout=0.01)
        except TimeoutError:
            continue
        if not isinstance(message, HealthUpdate):
            continue
        by_group = {group.group: group for group in message.groups}
        halted = by_group.get('halted')
        healthy = by_group.get('healthy')
        if (
            halted is not None
            and healthy is not None
            and 'history-truncated' in halted.conditions
            and healthy.join_phase == 'attached'
        ):
            update = message
            break

    assert runtime.fatal_reason is None
    assert update is not None
    assert halted_serviced.wait(1.0)
    health = {group.group: group for group in update.groups}
    assert health['halted'].join_phase != 'attached'
    assert health['healthy'].join_phase == 'attached'
    threads = runtime._threads
    assert threads['lane:halted'].is_alive()
    assert threads['lane:healthy'].is_alive()
    halted_lane = _runtime_lane(runtime, 'halted')
    assert halted_lane is not None
    assert halted_lane.health().status == 'history-truncated'


def _runtime_site(group: GroupConfig) -> SiteConfig:
    credential = CredentialConfig(
        name='source',
        script_name='daemon',
        api_key=SecretValue('key'),
    )
    workers: Mapping[str, WorkerConfig] = (
        MappingProxyType({'pool': WorkerConfig(name='pool')})
        if group.worker == 'pool'
        else MappingProxyType({})
    )
    return SiteConfig(
        name='main',
        server='https://example.test',
        default_source='events',
        credentials=MappingProxyType({'source': credential}),
        sources=MappingProxyType({'events': SourceConfig(name='events', credentials='source')}),
        groups=MappingProxyType({group.name: group}),
        workers=workers,
    )


def _group_health_component(
    state_dir: Path,
    state: GroupState,
    *,
    health_interval_secs: float,
) -> tuple[_LaneComponent, _ComponentHealthCache, SimpleNamespace, SimpleNamespace]:
    group = _group('local', worker='default')
    site = _runtime_site(group)
    config = _runtime_config(state_dir, site)
    config = replace(
        config,
        daemon=replace(
            config.daemon,
            health_interval_secs=health_interval_secs,
        ),
    )
    spec = build_worker_specs(site)[0]
    cache = _ComponentHealthCache(
        site=site.name,
        group_sources=MappingProxyType({'local': 'events'}),
    )

    def send(_message: object) -> None:
        return None

    component = _LaneComponent(
        checkpoint_host=cast('CheckpointHost', _IdleCheckpointHost()),
        assignment=_local_assignment(spec, 'local', 'resume'),
        config=config,
        control=cast('WorkerControl', SimpleNamespace(send=send)),
        group='local',
        health_cache=cache,
        limiter=_CatchupLimiter(1),
        local_source=None,
        spec=spec,
    )
    context = SimpleNamespace(
        _group=group,
        _site=site,
        _source_config=site.sources['events'],
    )
    lane = _GroupHealthLane(state.snapshot())
    channel = SimpleNamespace(
        close=lambda: None,
        status=SimpleNamespace(attached=True, queued_bytes=0, queued_count=0),
    )
    resources = SimpleNamespace(
        _client=SimpleNamespace(last_join_target=None),
        _context=context,
        _coordination=SimpleNamespace(_service=_ProbeCoordination._service),
        _lane=lane,
        _state=state,
    )
    lifecycle = SimpleNamespace(
        _channel=channel,
        _joined=True,
        _next_coordination_at=0.0,
        _was_running=True,
        _setup_failure=None,
    )
    return component, cache, resources, lifecycle


def _delivery_retirement_probe(tmp_path: Path, state: GroupState) -> SimpleNamespace:
    component, cache, resources, _lifecycle = _group_health_component(
        tmp_path,
        state,
        health_interval_secs=0.01,
    )
    watchdog = component._watchdog
    token = watchdog.sample().registration_token
    watchdog.transition(
        'callback',
        deadline_at=time.monotonic() + 10.0,
        event_id=42,
        plugin_id='restart',
        callback_id='record',
        delivery_attempt=1,
    )
    close_entered = threading.Event()
    close_release = threading.Event()
    operations: list[str] = []
    return SimpleNamespace(
        cache=cache,
        channel=_WatchdogBlockingClose(close_entered, close_release, operations),
        close_entered=close_entered,
        close_release=close_release,
        component=component,
        operations=operations,
        reconnect_entered=threading.Event(),
        reconnect_release=threading.Event(),
        replacement=object(),
        resources=resources,
        token=token,
        watchdog=watchdog,
    )


def _runtime_delivery_site(
    group: GroupConfig,
    start_mode: Literal['initialize', 'resume'],
) -> SiteConfig:
    site = _runtime_site(group)
    if group.ordering == 'strict' and start_mode == 'resume':
        source = replace(
            site.sources[group.source],
            fetch_interval_secs=0.01,
            visibility_overlap_secs=0.01,
        )
        site = replace(site, sources=MappingProxyType({group.source: source}))
    return site


def _runtime_threads_alive(runtime: WorkerRuntime | None) -> bool:
    if runtime is None:
        return False
    threads = runtime._threads
    monitor = runtime._monitor_thread
    return any(thread.is_alive() for thread in threads.values()) or (
        monitor is not None and monitor.is_alive()
    )


def _seed_coordination_range(state: SourceState) -> str:
    state.initialize(41, incarnation='incarnation-1')
    _bootstrap_coordination_group(state._database.path)
    opened = open_source_range(state, 42, 45, successful_clock=1.0)
    return opened.obligation_id


def _service_source_health(
    component: object,
    server: _SourceHealthServer,
    poller: SourcePoller,
) -> None:
    _SourceComponent._service(
        cast('_SourceComponent', component),
        cast('EventServer', server),
        cast('_SourceJoinSurface', SimpleNamespace(_reconcile_groups=lambda: None)),
        poller,
    )


def _site(groups: tuple[GroupConfig, ...]) -> SiteConfig:
    credential = CredentialConfig(
        name='source',
        script_name='daemon',
        api_key=SecretValue('key'),
    )
    return SiteConfig(
        name='main',
        server='https://example.test',
        default_source='events',
        credentials=MappingProxyType({'source': credential}),
        sources=MappingProxyType(
            {
                'events': SourceConfig(name='events', credentials='source'),
                'audit': SourceConfig(name='audit', credentials='source'),
            }
        ),
        groups=MappingProxyType({group.name: group for group in groups}),
        workers=MappingProxyType({'pool': WorkerConfig(name='pool')}),
    )


@contextmanager
def _monitor_control_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[tuple[WorkerRuntime, WorkerControl, WorkerControl, _Component]]:
    site = _site(())
    config = _runtime_config(tmp_path, site)
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='monitor-peer-close-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    component = _Component('source:events', [])
    runtime = build_worker_runtime(config, spec, assignment, child)

    def source_factory(_name: str, _slots: _CatchupLimiter, _host: CheckpointHost) -> _Component:
        return component

    monkeypatch.setattr(runtime, '_source_factory', source_factory)
    try:
        yield runtime, parent, child, component
    finally:
        component._release.set()
        if component._started.is_set():
            assert component._finished.wait(1.0)
        runtime.request_stop('test-cleanup')
        runtime._shutdown()
        parent.close()
        child.close()


def _source_database_path(state_dir: Path) -> Path:
    sites = ensure_secure_state_dir(ensure_secure_state_dir(state_dir) / 'sites')
    site = ensure_secure_state_dir(sites / 'main')
    sources = ensure_secure_state_dir(site / 'sources')
    return sources / 'events.db'


def _pending_group_runtime(
    state_dir: Path,
) -> tuple[Config, WorkerSpec, WorkerAssignment, GroupSnapshot]:
    site = _runtime_site(_group('local', worker='default'))
    source = replace(site.sources['events'], fetch_interval_secs=60.0)
    site = replace(site, sources=MappingProxyType({'events': source}))
    config = _runtime_config(state_dir, site)
    spec = replace(build_worker_specs(site)[0], groups=())
    assignment = replace(
        _local_assignment(spec, 'local', 'resume'),
        group_assignments=(),
        source_group_assignments=(
            PendingDispositionSourceGroupAssignment(
                source='events', group='local', orphan_generation=7, reserved_generation=11
            ),
        ),
    )
    with _open_group_health_state(_source_database_path(state_dir)) as state:
        orphan_group(state)
        snapshot = state.change_lifecycle(
            LifecycleRequest(
                command='readd',
                expected_lifecycle='orphaned',
                expected_generation=7,
                transition_generation=11,
                identity=AuditIdentity.current(),
                wall_clock_time=2.0,
            )
        )
    return config, spec, assignment, snapshot


@contextmanager
def _pending_group_health_probe(
    state_dir: Path,
) -> Generator[tuple[_SourceComponent, _ComponentHealthCache, SourceState], None, None]:
    config, spec, assignment, _snapshot = _pending_group_runtime(state_dir)
    config = replace(config, daemon=replace(config.daemon, health_interval_secs=1.0))
    cache = _ComponentHealthCache(site='main', group_sources={'local': 'events'})

    def send(_message: object) -> None:
        return None

    component = _SourceComponent(
        checkpoint_host=cast('CheckpointHost', _IdleCheckpointHost()),
        assignment=assignment,
        config=config,
        control=cast('WorkerControl', SimpleNamespace(send=send)),
        health_cache=cache,
        source='events',
        spec=spec,
    )
    site = config.sites['main']
    source_config = site.sources['events']
    path = _source_database_path(state_dir)
    with SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site='main',
        source='events',
        stream_fingerprint=stream_fingerprint(
            site.server,
            source_config.credentials,
            site.credentials[source_config.credentials].script_name,
        ),
        source_generation='source-generation-1',
    ) as state:
        yield component, cache, state


def _prepare_retained_history_runtime_state(
    state_dir: Path,
    site: SiteConfig,
    groups: tuple[GroupConfig, GroupConfig],
) -> None:
    source_config = site.sources['events']
    credential = site.credentials[source_config.credentials]
    fingerprint = stream_fingerprint(
        site.server,
        source_config.credentials,
        credential.script_name,
    )
    cursors = {'halted': 41, 'healthy': 60}
    with SourceState.open(
        _source_database_path(state_dir),
        writes=write_context_for(_source_database_path(state_dir)),
        create=True,
        site=site.name,
        source='events',
        stream_fingerprint=fingerprint,
        source_generation='source-generation-old',
    ) as source_state:
        source_state.initialize(60, incarnation='stream-incarnation-1')
    for group in groups:
        cursor = cursors[group.name]
        with open_group_on_source(
            _source_database_path(state_dir),
            writes=write_context_for(_source_database_path(state_dir)),
            site=site.name,
            site_server_url=site.server,
            source='events',
            group=group.name,
            group_generation=7,
            ordering=group.ordering,
            stream_fingerprint=fingerprint,
            source_generation='source-generation-old',
            verified_incarnation='stream-incarnation-1',
            start_mode='initialize',
            initial_cursor=cursor,
            initial_delivery_floor=cursor,
            initial_last_delivered_id=cursor,
            strict_holdback_secs=source_config.visibility_overlap_secs,
        ):
            pass


def _prepare_runtime_state(
    state_dir: Path,
    site: SiteConfig,
    group: GroupConfig,
    start_mode: Literal['initialize', 'resume'],
    **options: Unpack[_RuntimeStateOptions],
) -> None:
    if start_mode == 'initialize':
        return
    source_config = site.sources[group.source]
    credential = site.credentials[source_config.credentials]
    fingerprint = stream_fingerprint(
        site.server,
        source_config.credentials,
        credential.script_name,
    )
    with SourceState.open(
        _source_database_path(state_dir),
        writes=write_context_for(_source_database_path(state_dir)),
        create=True,
        site=site.name,
        source=group.source,
        stream_fingerprint=fingerprint,
        source_generation='source-generation-old',
    ) as source_state:
        source_state.initialize(41, incarnation='stream-incarnation-1')
    with open_group_on_source(
        _source_database_path(state_dir),
        writes=write_context_for(_source_database_path(state_dir)),
        site=site.name,
        site_server_url=site.server,
        source=group.source,
        group=group.name,
        group_generation=7,
        ordering=group.ordering,
        stream_fingerprint=fingerprint,
        source_generation='source-generation-old',
        verified_incarnation='stream-incarnation-1',
        start_mode='initialize',
        initial_cursor=41,
        initial_delivery_floor=57,
        initial_last_delivered_id=57,
        strict_holdback_secs=source_config.visibility_overlap_secs,
    ) as state:
        in_flight = options.get('in_flight')
        if in_flight is not None:
            event_id, manifest = in_flight
            state.start_event(
                event_id,
                manifest,
                expected_revision=state.snapshot().revision,
            )
        park_reason = options.get('park_reason')
        if park_reason is not None:
            snapshot = state.snapshot()
            state.park(
                reason=park_reason,
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
                expected_revision=snapshot.revision,
            )


def _source_health_component(
    state_dir: Path,
    *,
    health_interval_secs: float,
) -> tuple[_SourceComponent, _ComponentHealthCache]:
    site = _runtime_site(_group('local', worker='default'))
    config = _runtime_config(state_dir, site)
    config = replace(
        config,
        daemon=replace(
            config.daemon,
            health_interval_secs=health_interval_secs,
        ),
    )
    spec = build_worker_specs(site)[0]
    cache = _ComponentHealthCache(
        site=site.name,
        group_sources=MappingProxyType({'local': 'events'}),
    )
    component_type = worker_module._SourceComponent

    def send(_message: object) -> None:
        return None

    component = component_type(
        checkpoint_host=cast('CheckpointHost', _IdleCheckpointHost()),
        assignment=_local_assignment(spec, 'local', 'resume'),
        config=config,
        control=cast('WorkerControl', SimpleNamespace(send=send)),
        health_cache=cache,
        source='events',
        spec=spec,
    )
    return component, cache


def _start_control_drain(
    control: WorkerControl,
) -> tuple[threading.Event, threading.Thread]:
    stop = threading.Event()

    def drain() -> None:
        while not stop.is_set():
            try:
                control.receive(timeout=0.01)
            except TimeoutError:
                continue
            except EOFError:
                return

    thread = threading.Thread(target=drain)
    thread.start()
    return stop, thread


def _authorize_remote_source(
    control: WorkerControl,
    grant: EventPeerGrant,
    config: Config,
) -> tuple[threading.Event, threading.Thread]:
    control.send(grant)
    assert _receive_type(control, EventPeerReady) == EventPeerReady(
        source=grant.source,
        group=grant.group,
        dependent_spawn_instance=grant.dependent_spawn_instance,
    )
    _assert_remote_server_health(control, config)
    return _start_control_drain(control)


def _start_disposition_component(
    component: _DispositionComponent,
    host: CheckpointHost,
    errors: list[Exception],
) -> threading.Thread:
    def run() -> None:
        try:
            component._run()
        except (
            AssertionError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            sqlite3.Error,
        ) as error:
            errors.append(error)
        finally:
            host.close()

    thread = threading.Thread(target=run)
    thread.start()
    return thread


def _stop_control_drain(
    stop: threading.Event,
    thread: threading.Thread,
) -> None:
    stop.set()
    thread.join(1.0)
    assert not thread.is_alive()


def _stop_runtime(runtime: WorkerRuntime | None) -> None:
    if runtime is None:
        return
    runtime.request_stop('test')
    assert runtime.wait() == ()
    _assert_monitor_stopped(runtime)


def _release_failed_source_runtime(
    runtime: WorkerRuntime | None,
    controls: tuple[WorkerControl | None, WorkerControl | None],
    sockets: tuple[socket.socket, socket.socket] | None,
    drain: tuple[threading.Event, threading.Thread] | None,
    extra_cleanup: tuple[Callable[[], None], ...] = (),
) -> None:
    initiating = sys.exception()
    record_runtime_exception(runtime, initiating)
    errors: list[BaseException] = []
    _attempt_cleanup(lambda: _stop_runtime(runtime), errors)
    for cleanup in extra_cleanup:
        _attempt_cleanup(cleanup, errors)
    runtime_alive = _runtime_threads_alive(runtime)
    if drain is not None and not runtime_alive:
        _attempt_cleanup(lambda: _stop_control_drain(*drain), errors)
    drain_alive = drain is not None and drain[1].is_alive()
    if runtime_alive or drain_alive:
        errors.append(AssertionError('retained controls still owned by live runtime or drain'))
    else:
        resources = tuple(
            control if control is not None else sock
            for control, sock in zip(controls, sockets or (None, None), strict=True)
        )
        for resource in (owned for owned in resources if owned is not None):
            _attempt_cleanup(resource.close, errors)
    _raise_cleanup_errors(initiating, errors)


def _start_source_runtime(
    config: Config,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
    grants: tuple[EventPeerGrant, ...],
) -> _SourceRuntimeSession:
    parent_socket, child_socket = socket.socketpair()
    parent: WorkerControl | None = None
    child: WorkerControl | None = None
    runtime: WorkerRuntime | None = None
    drain_stop: threading.Event | None = None
    drain_thread: threading.Thread | None = None
    transferred = False
    try:
        parent = WorkerControl(
            parent_socket,
            max_frame_bytes=131_072,
            send_deadline_secs=1.0,
        )
        child = WorkerControl(
            child_socket,
            max_frame_bytes=131_072,
            send_deadline_secs=1.0,
        )
        runtime = build_worker_runtime(
            config,
            spec,
            assignment,
            child,
        )
        runtime.start()
        endpoints = {
            endpoint.source: endpoint
            for endpoint in (_receive_type(parent, SourceEndpointReady) for _ in spec.sources)
        }
        endpoint = endpoints[grants[0].source]
        for grant in grants:
            parent.send(grant)
            assert _receive_type(parent, EventPeerReady) == EventPeerReady(
                source=grant.source,
                group=grant.group,
                dependent_spawn_instance=grant.dependent_spawn_instance,
            )
        drain_stop, drain_thread = _start_control_drain(parent)
        session = _SourceRuntimeSession(
            _runtime=runtime,
            _parent=parent,
            _child=child,
            _drain_stop=drain_stop,
            _drain_thread=drain_thread,
            _endpoint=endpoint,
        )
        transferred = True
        return session
    finally:
        if not transferred:
            _release_failed_source_runtime(
                runtime,
                (parent, child),
                (parent_socket, child_socket),
                (
                    (drain_stop, drain_thread)
                    if drain_stop is not None and drain_thread is not None
                    else None
                ),
            )


def _strict_prepared_probe_step(
    state: GroupState,
    clock: list[float],
    provider_delay: float,
    active_delivery: list[bool],
    /,
    **options: object,
) -> object:
    deliver = cast('Callable[[Sequence[Event], float], StrictDeliveryOutcome]', options['deliver'])
    service = cast('Callable[[float], None]', options['service'])
    clock[0] += provider_delay
    remaining = tuple(
        Event(_runtime_event(event_id)) for event_id in range(state.snapshot().cursor + 1, 48)
    )
    while remaining:
        service(0.0)
        if state.snapshot().status != 'running':
            break
        active_delivery[0] = True
        try:
            outcome = deliver(remaining, 0.0)
        finally:
            active_delivery[0] = False
        if not outcome.accepted or not outcome.continue_epoch:
            break
        remaining = remaining[outcome.consumed :]
    return SimpleNamespace(status='caught-up', join_ready=state.snapshot().cursor == 47)


def _synchronization(
    frontier: int,
) -> SourceProgress:
    return SourceProgress(
        range_revision=0,
        frontier=frontier,
        source_generation='source-1',
        stream_incarnation='incarnation-1',
        source_successful_clock=float(frontier),
    )


def _terminal_monitor_frame(
    runtime: WorkerRuntime, control: WorkerControl, previous: HealthUpdate
) -> HealthUpdate:
    monitor = runtime._monitor_thread
    assert monitor is not None
    monitor.join(1)
    assert not monitor.is_alive()
    terminal = previous
    while True:
        try:
            message = control.receive(timeout=0)
        except TimeoutError:
            return terminal
        assert isinstance(message, HealthUpdate)
        terminal = message


def _track_halted_lane_service(
    monkeypatch: pytest.MonkeyPatch,
    condition: str,
) -> threading.Event:
    halted_serviced = threading.Event()
    drive_halted = _LaneComponent._drive_halted

    def track_halted_service(
        component: _LaneComponent,
        resources: _LaneResources,
        lifecycle: _LaneLifecycle,
    ) -> None:
        drive_halted(component, resources, lifecycle)
        if condition in resources._state.snapshot().conditions:
            halted_serviced.set()

    monkeypatch.setattr(_LaneComponent, '_drive_halted', track_halted_service)
    return halted_serviced


def _track_retained_history_service(
    monkeypatch: pytest.MonkeyPatch,
) -> threading.Event:
    halted_serviced = threading.Event()
    service = LaneRefresh._service

    def track_halted_service(
        coordinator: object,
        *,
        scan_obligations: bool = True,
        force: bool = False,
    ) -> object:
        result = service(
            cast('LaneRefresh', coordinator),
            scan_obligations=scan_obligations,
            force=force,
        )
        state = cast('GroupState', vars(coordinator)['_state'])
        snapshot = state.snapshot()
        if (
            snapshot.group == 'halted'
            and snapshot.status == 'history-truncated'
            and not scan_obligations
        ):
            halted_serviced.set()
        return result

    monkeypatch.setattr(LaneRefresh, '_service', track_halted_service)
    return halted_serviced


def _unconnected_event_client(
    path: Path,
) -> EventClient:
    return EventClient(
        path=path,
        source='events',
        group='local',
        spawn_instance='site-spawn-1',
        capability='capability-1',
        max_frame_bytes=131_072,
        queue_max_entries=8,
        queue_max_bytes=262_144,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )


def _unopened_group_component(component_type: type[_ComponentT]) -> _ComponentT:
    def send(_message: object) -> None:
        return None

    site = _runtime_site(_group('local', worker='default'))
    spec = build_worker_specs(site)[0]
    factory = cast('Callable[..., _ComponentT]', component_type)
    return factory(
        checkpoint_host=cast('CheckpointHost', _IdleCheckpointHost()),
        assignment=_local_assignment(spec, 'local', 'resume'),
        config=_runtime_config(Path.cwd(), site),
        control=cast('WorkerControl', SimpleNamespace(send=send)),
        group='local',
        health_cache=_ComponentHealthCache(
            site=site.name,
            group_sources=MappingProxyType({'local': 'events'}),
        ),
        limiter=_CatchupLimiter(1),
        local_source=None,
        spec=spec,
    )


def _bootstrap_stop_component(
    *,
    fail_before_stop: bool,
) -> tuple[_InterruptibleComponent, threading.Event, type[Exception], str]:
    stop = threading.Event()
    entered = threading.Event()
    message = 'lane stopped during source bootstrap'
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(component)['_stop'] = stop
    request = GroupBootstrapRequest(
        source='events',
        group='local',
        group_generation=7,
        start_mode='resume',
    )

    def observe_group(_request: GroupBootstrapRequest) -> object:
        entered.set()
        if fail_before_stop:
            raise InterruptedError(message)
        return worker_module.GroupBootstrapPending(
            source='events',
            group='local',
            group_generation=7,
            retry_after_secs=10.0,
        )

    observe = component_type._observe_source

    def _run() -> object:
        return observe(
            component,
            cast('EventClient', SimpleNamespace(observe_group=observe_group)),
            assignment=request,
        )

    return _InterruptibleComponent(_run, stop.set), entered, InterruptedError, message


def _eager_catchup_stop_component(
    *,
    fail_before_stop: bool,
) -> tuple[_InterruptibleComponent, threading.Event, type[Exception], str]:
    stop = threading.Event()
    entered = threading.Event()
    message = 'lane stopped during eager catch-up'
    component = _unopened_group_component(_LaneComponent)

    def catch_up(**options: object) -> None:
        entered.set()
        if fail_before_stop:
            raise InterruptedError(message)
        assert stop.wait(1.0)
        deliver_page = cast('Callable[[Sequence[Event]], None]', options['deliver_page'])
        deliver_page((Event(_runtime_event(41)),))

    def deliver_many_counted(deliveries: Iterable[object]) -> tuple[object, int]:
        return SimpleNamespace(status='running'), len(tuple(deliveries))

    resources = SimpleNamespace(
        _state=SimpleNamespace(
            snapshot=lambda: SimpleNamespace(
                cursor=40,
                delivery_floor=40,
                replay_required_from=None,
            ),
            unresolved_obligation_bounds=lambda: (),
        ),
        _coordination=SimpleNamespace(
            synchronize=lambda: None,
        ),
        _lane=SimpleNamespace(
            deliver_many_counted=deliver_many_counted,
            shutdown_requested=stop.is_set,
        ),
        _reader=SimpleNamespace(catch_up=catch_up),
    )

    def _run() -> object:
        return _LaneComponent._catch_up_eager(
            component,
            cast('_LaneResources', resources),
            cast('JoinTarget', SimpleNamespace(identity=SimpleNamespace(boundary=41))),
        )

    return _InterruptibleComponent(_run, stop.set), entered, InterruptedError, message


def _lifecycle_probe(
    execution: Literal['sync', 'async'],
    *,
    durable_status: str,
    setup_failure: RuntimeError | None,
    resume_status: str = 'running',
) -> tuple[_LaneComponent, SimpleNamespace]:
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)

    def ignore_health(_resources: object, _lifecycle: object) -> None:
        return None

    vars(component).update(
        {
            '_lane': None,
            '_publish_health': ignore_health,
            '_stop': threading.Event(),
        }
    )
    resources = SimpleNamespace(
        _channel=_LifecycleChannel(),
        _client=_LifecycleReader(),
        _context=SimpleNamespace(
            _group=SimpleNamespace(
                execution=execution,
                ordering='eager',
            ),
            _source_config=SimpleNamespace(fetch_interval_secs=0.001),
        ),
        _coordination=_LifecycleCoordination(),
        _lane=_LifecycleLane(setup_failure, resume_status),
        _reader=_LifecycleReader(),
        _state=_LifecycleState(durable_status),
    )
    return component, resources


@contextmanager
def _local_coordination_client(
    source_path: Path,
) -> Generator[tuple[object, _DurableCoordinationSurface, Callable[[], None]]]:
    with _open_coordination_source(source_path) as source_state:
        surface = _DurableCoordinationSurface(
            source_state,
        )
        factory = cast(
            '_LocalJoinClientFactory',
            vars(worker_module)['_LocalJoinClient'],
        )

        def _close_range() -> None:
            retained = source_state.snapshot().ranges
            assert len(retained) == 1
            source_state._close_range(
                retained[0].obligation_id,
                successful_clock=2.0,
            )

        yield (
            factory(
                _DirectSourceComponent(surface),
                deadline_secs=30.0,
                source_name='events',
                group='local',
                stop=threading.Event(),
            ),
            surface,
            _close_range,
        )


def _probe_lane_catchup(
    ordering: Literal['eager', 'strict'],
    synchronizations: tuple[SourceProgress, ...],
) -> tuple[_SyncingClient, _SynchronizationProbeReader, RuntimeError | None]:
    client = _SyncingClient(synchronizations)
    reader = _SynchronizationProbeReader(ordering)
    lane = _ProbeLane()
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(component).update(
        {
            '_config': SimpleNamespace(
                daemon=SimpleNamespace(max_pending_epochs=8),
            ),
            '_limiter': _CatchupLimiter(1),
            '_health_interval_secs': 1.0,
            '_stop': threading.Event(),
        }
    )
    resources = SimpleNamespace(
        _client=client,
        _coordination=_ProbeCoordination(client),
        _context=SimpleNamespace(
            _assignment=SimpleNamespace(group_generation=7),
            _group=SimpleNamespace(ordering=ordering),
            _holdback_secs=1.0,
            _source_config=SimpleNamespace(fetch_interval_secs=0.001),
        ),
        _lane=lane,
        _reader=reader,
        _state=_ProbeState(),
    )
    driver = cast(
        '_LaneCatchupDriver',
        vars(component_type)['_catch_up'],
    )
    error: RuntimeError | None = None
    try:
        driver(component, resources, 45)
    except RuntimeError as exception:
        error = exception
    return client, reader, error


def _recovery_construction_probe(
    *,
    outcome: Literal['parked', 'stale'] = 'parked',
    recovery_error: RuntimeError | None = None,
    resulting_revision: int = 12,
    stop_at_construction: bool = True,
) -> tuple[
    _LaneComponent,
    _RecoveryContextProbe,
    list[str],
    _RecoveryConstructionControl,
    _RecoveryConstructionState,
]:
    operations: list[str] = []
    control = _RecoveryConstructionControl(operations)
    state = _RecoveryConstructionState(
        operations,
        outcome=outcome,
        resulting_revision=resulting_revision,
        recovery_error=recovery_error,
    )
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    directive = ConditionalParkDirective(
        evidence_id='evidence-1',
        site='main',
        group='remote',
        group_generation=7,
        failed_spawn_instance='failed-spawn-1',
        event_id=42,
        group_revision=11,
        observed_at_wall_clock=1234.5,
    )
    context = _RecoveryContextProbe(
        _assignment=_RecoveryAssignmentProbe(
            conditional_park=directive,
            group_generation=7,
            start_mode='resume',
        ),
        _group=SimpleNamespace(ordering='eager', source='events'),
        _site=SimpleNamespace(name='main'),
        _source_config=SimpleNamespace(fetch_interval_secs=0.01),
    )
    client = _RecoveryConstructionClient()
    reader = _RecoveryConstructionReader(operations)
    observation = SimpleNamespace(source_generation='source-generation-1')

    def build_client(_context: object) -> tuple[object, str]:
        operations.append('client')
        return client, 'source-generation-1'

    def observe_source(_client: object, *, assignment: object) -> object:
        del assignment
        operations.append('observe')
        return observation

    def open_state(
        _context: object,
        _observation: object,
        _reader: object,
    ) -> object:
        operations.append('open')
        return state

    def provider_reader(_context: object) -> tuple[object, object]:
        operations.append('provider')
        return object(), reader

    def database_path() -> Path:
        operations.append('checkpoint-path')
        if stop_at_construction:
            raise _RecoveryConstructionReachedError
        return Path('unused.db')

    def validate_source_generation(_observation: object, _expected: str) -> None:
        operations.append('validate')

    vars(component).update(
        {
            '_assignment': SimpleNamespace(spawn_instance='current-spawn-1'),
            '_build_client': build_client,
            '_control': control,
            '_config': SimpleNamespace(
                daemon=SimpleNamespace(
                    async_cancel_grace_secs=0.0,
                    health_interval_secs=1.0,
                    max_pending_epochs=1,
                    sqlite_synchronous='FULL',
                )
            ),
            '_group': 'remote',
            '_health_interval_secs': 1.0,
            '_internal_stall_deadline_secs': 10.0,
            '_lane': None,
            '_limiter': _CatchupLimiter(1),
            '_mailbox': _OwnerMailbox('test'),
            '_observe_source': observe_source,
            '_open_group_state': open_state,
            '_provider_reader': provider_reader,
            '_checkpoint_view': CheckpointView(),
            '_database_path': database_path,
            '_reload': threading.Event(),
            '_resolved_recovery': set(),
            '_stop': threading.Event(),
            '_shutdown_deadline_at': None,
            '_validate_source_generation': validate_source_generation,
            '_wake': threading.Event(),
            '_watchdog': ComponentWatchdog(
                kind='lane',
                identity='remote',
                worker_instance='current-spawn-1',
                registration_token=os.urandom(16).hex(),
                startup_deadline_secs=10.0,
            ),
        }
    )
    return component, context, operations, control, state


def _recovery_owner_phase_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[_LaneComponent, _LaneConfiguration, list[float], _ComponentHealthCache]:
    raw, context, operations, _control, _state = _recovery_construction_probe(
        stop_at_construction=False
    )
    component = raw
    _install_recovery_gate_probes(monkeypatch, component, operations)
    now = [100.0]
    monkeypatch.setattr(
        worker_module, 'time', SimpleNamespace(monotonic=lambda: now[0], time=time.time)
    )
    monkeypatch.setattr(component._watchdog, '_clock', lambda: now[0])
    component._watchdog.transition('idle', deadline_at=135.0)
    context = replace(context, _source_config=SimpleNamespace(fetch_interval_secs=30.0))
    cache = _ComponentHealthCache(
        site='main', group_sources=MappingProxyType({'remote': 'events'})
    )
    sample = component._watchdog.sample()
    cache._register_watchdog(
        kind=sample.kind,
        identity=sample.identity,
        worker_instance=sample.worker_instance,
        token=sample.registration_token,
        supplier=component._watchdog.sample,
    )
    assert cache._snapshot()._watchdogs[0].phase == 'idle'
    return component, cast('_LaneConfiguration', context), now, cache


@contextmanager
def _remote_coordination_client(
    source_path: Path,
    socket_path: Path,
) -> Generator[tuple[object, _DurableCoordinationSurface, Callable[[], None]]]:
    close_complete = threading.Event()
    close_requested = threading.Event()
    ready = threading.Event()
    stop = threading.Event()
    holder: list[_DurableCoordinationSurface] = []

    def serve() -> None:
        with (
            _open_coordination_source(source_path) as source_state,
            EventServer(
                path=socket_path,
                source='events',
                max_frame_bytes=131_072,
                queue_max_entries=8,
                queue_max_bytes=262_144,
            ) as server,
        ):
            server.install_grant(
                EventPeerGrant(
                    source='events',
                    group='local',
                    dependent_spawn_instance='dependent-1',
                    capability='capability-1',
                )
            )
            surface = _DurableCoordinationSurface(
                source_state,
            )
            holder.append(surface)
            ready.set()
            dispatch = cast(
                'Callable[[object], None]',
                server.dispatch_join_requests,
            )
            while not stop.is_set():
                if close_requested.is_set() and not close_complete.is_set():
                    retained = source_state.snapshot().ranges
                    assert len(retained) == 1
                    source_state._close_range(
                        retained[0].obligation_id,
                        successful_clock=2.0,
                    )
                    close_complete.set()
                server.poll(0.001)
                dispatch(surface)

    executor = ThreadPoolExecutor(max_workers=1)
    future: Future[None] | None = None
    client: EventClient | None = None
    try:
        future = executor.submit(serve)
        if not ready.wait(1.0):
            stop.set()
            future.result(timeout=1.0)
            raise AssertionError('coordination server did not become ready')
        if future.done():
            future.result()
        client = EventClient(
            path=socket_path,
            source='events',
            group='local',
            spawn_instance='dependent-1',
            capability='capability-1',
            max_frame_bytes=131_072,
            queue_max_entries=8,
            queue_max_bytes=262_144,
            coordination_deadline_secs=30.0,
            stop=threading.Event(),
        )
        client.connect()

        def _close_range() -> None:
            close_requested.set()
            if close_complete.wait(1.0):
                return
            if future.done():
                future.result()
            raise AssertionError('coordination source did not close its range')

        yield client, holder[0], _close_range
    finally:
        _close_remote_coordination_client(client, stop, future, executor)


@contextmanager
def _coordination_client(
    transport: Literal['local', 'remote'],
    source_path: Path,
    socket_path: Path,
) -> Generator[tuple[object, _DurableCoordinationSurface, Callable[[], None]]]:
    if transport == 'local':
        with _local_coordination_client(source_path) as resources:
            yield resources
        return
    with _remote_coordination_client(source_path, socket_path) as resources:
        yield resources


def _restart_callback_plugin(
    invoked: list[int],
    *,
    callback_entered: threading.Event,
    execution: Literal['sync', 'async'] = 'sync',
    retained_contexts: list[EventContext] | None = None,
    release_callback: threading.Event,
) -> PluginSpec:

    def callback(context: EventContext) -> None:
        event_id = context.event['id']
        assert type(event_id) is int
        invoked.append(event_id)
        if retained_contexts is not None:
            retained_contexts.append(context)
        if event_id == 42:
            callback_entered.set()
            release_callback.wait()

    async def callback_async(context: EventContext) -> None:
        callback(context)

    identity = CallbackIdentity(
        ordinal=0,
        plugin_id='restart',
        callback_id='record',
        execution=execution,
        filter_digest='all-events',
        config_digest='empty-config',
        secret_refs=(),
        credential_name='source',
        credential_fingerprint='source:daemon',
    )
    return PluginSpec(
        id='restart',
        module_name='tests.restart',
        credentials='source',
        config=MappingProxyType({}),
        secrets=(),
        setup=None,
        callbacks=(
            CallbackSpec(
                id='record',
                callback=callback_async if execution == 'async' else callback,
                events=None,
                identity=identity,
            ),
        ),
    )


def _restart_provider(
    state: _RestartProviderState,
    *,
    async_: bool = False,
) -> Provider:

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _RestartProviderHandle:
        del site
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return _RestartProviderHandle(state)

    def async_factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _AsyncRestartProviderHandle:
        del site
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return _AsyncRestartProviderHandle(state)

    return Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(
            sync=True,
            async_=async_,
        ),
        _sync_factory=factory,
        _async_factory=async_factory if async_ else None,
    )


def _detached_strict_join(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    modes: tuple[Literal['local', 'remote'], Literal['sync', 'async']],
    case: str,
) -> None:
    execution = modes[1]
    wall = [0.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: wall[0])
    source_path = _source_database_path(tmp_path)
    with _open_coordination_source(source_path) as source:
        source.initialize(41, incarnation='incarnation-1')
        _bootstrap_coordination_group(source_path, ordering='strict')
        open_source_range(source, 46 if case == 'new-fence' else 44, 48, successful_clock=1.0)
        source.commit_observations((50, 51), successful_clock=1.0)
        # The obligation was already eligible before the detached epoch's holdback.
        source._database.connection.execute('UPDATE range_groups SET eligibility_point = 1.0')
        source._database.connection.execute(BUMP_RANGE_REVISION)
    provider_state = _RestartProviderState()
    for event_id in (42, 46 if case == 'new-fence' else 44, 48, 50):
        provider_state._publish(event_id)
    provider = _restart_provider(provider_state, async_=execution == 'async')
    callbacks: list[int] = []
    plugin = _restart_callback_plugin(
        callbacks,
        callback_entered=threading.Event(),
        execution=execution,
        release_callback=threading.Event(),
    )
    with (
        open_group_on_source(
            source_path,
            writes=write_context_for(source_path),
            site='main',
            site_server_url='https://example.test',
            source='events',
            group='local',
            group_generation=7,
            ordering='strict',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='resume',
            strict_holdback_secs=300.0,
        ) as state,
        _coordination_client(
            modes[0],
            source_path,
            local_socket_path(tmp_path, 'coordination', ('events', 'local')),
        ) as (client, _surface, close_range),
    ):
        if case == 'cross-epoch':
            for low_id, high_id, captured_at in ((42, 46, 0.0), (47, 51, 50.0)):
                state.capture_epoch(
                    low_id=low_id,
                    high_id=high_id,
                    captured_at=captured_at,
                    eligibility_point=captured_at + 300.0,
                    reader_clock_checkpoint=captured_at,
                    source_generation='source-1',
                    expected_revision=state.snapshot().revision,
                )
        component, _cache, resources, lifecycle = cast(
            'tuple[_LaneComponent, object, _LaneResources, object]',
            _group_health_component(
                tmp_path,
                state,
                health_interval_secs=5.0,
            ),
        )
        group = replace(_group('local', worker='default'), ordering='strict', execution=execution)
        site = _runtime_site(group)
        watchdog = component._watchdog
        watchdog.transition('idle', deadline_at=None)
        limiter = component._limiter
        stop = _detached_holdback_wait(
            component, close_range, wall, check_watchdog=case == 'watchdog'
        )
        trace: list[str] = []
        _inject_detached_coordination(state, provider_state, (callbacks, trace), case, monkeypatch)

        def callback(context: EventContext) -> None:
            event_id = cast('int', context.event['id'])
            callbacks.append(event_id)
            trace.append(f'callback-{event_id}')
            if case == 'callback-stop' and event_id == 42:
                stop.set()

        async def callback_async(context: EventContext) -> None:
            callback(context)

        plugin = replace(
            plugin,
            callbacks=(
                replace(
                    plugin.callbacks[0],
                    callback=callback_async if execution == 'async' else callback,
                ),
            ),
        )
        vars(component).update(_stop=stop, _lifecycle=lifecycle)
        monkeypatch.setattr(component._wake, 'wait', stop.wait)
        with (
            CatchupReader(
                provider=provider,
                site_url=site.server,
                credential=site.credentials['source'],
                batch_size=1,
                fetch_interval_secs=1.0,
                successful_clock=_SuccessfulQueryClock(clock=lambda: wall[0]),
                waiter=_SourceHealthWaiter(),
            ) as reader,
            closing(
                GroupLane(
                    state=state,
                    site=site,
                    group=group,
                    provider=provider,
                    plugins=(plugin,),
                    worker_instance='site-spawn-1',
                    health_interval_secs=5.0,
                    watchdog=watchdog,
                )
            ) as lane,
        ):
            vars(component)['_lane'] = lane
            vars(resources)['_client'] = client
            original_confirm = resources._client.confirm

            def confirm(confirmation: JoinConfirm) -> Attached | JoinRetry:
                assert state.snapshot().obligation_count == 0
                trace.append('confirm')
                return original_confirm(confirmation)

            monkeypatch.setattr(resources._client, 'confirm', confirm)
            vars(resources).update(
                _context=SimpleNamespace(
                    _group=group,
                    _holdback_secs=300.0,
                    _source_config=replace(site.sources['events'], fetch_interval_secs=50.0),
                ),
                _lane=lane,
                _reader=reader,
                _coordination=LaneRefresh(
                    lane=lane,
                    limiter=limiter,
                    reader=reader,
                    state=state,
                ),
            )
            _assert_detached_strict_join(component, resources, (callbacks, trace, wall), case)


def _runtime_provider(*, calls: list[str] | None = None) -> Provider:
    lock = threading.Lock()
    latest_calls = 0

    def _latest() -> int:
        nonlocal latest_calls
        with lock:
            latest_calls += 1
            return 41 if latest_calls == 1 else 57

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _RuntimeProviderHandle:
        del site
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return _RuntimeProviderHandle(_latest, calls)

    return Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(
            sync=True,
            async_=False,
        ),
        _sync_factory=factory,
        _async_factory=None,
    )


def _concrete_delivery_provider(
    monkeypatch: pytest.MonkeyPatch,
    start_mode: Literal['initialize', 'resume'],
) -> tuple[Provider, _RestartProviderState, list[int]]:
    provider_state = _RestartProviderState()
    invoked: list[int] = []
    if start_mode == 'initialize':
        return _runtime_provider(), provider_state, invoked
    release_callback = threading.Event()
    release_callback.set()
    plugin = _restart_callback_plugin(
        invoked,
        callback_entered=threading.Event(),
        release_callback=release_callback,
    )

    def load_plugins(
        _site: SiteConfig,
        _group: GroupConfig,
        _provider: Provider,
    ) -> tuple[PluginSpec, ...]:
        return (plugin,)

    monkeypatch.setattr(worker_module, 'load_plugins', load_plugins)
    return _restart_provider(provider_state), provider_state, invoked


@contextmanager
def _running_concrete_runtime(
    monkeypatch: pytest.MonkeyPatch,
    config: Config,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
    provider: Provider,
) -> Generator[tuple[WorkerRuntime, WorkerControl]]:
    monkeypatch.setattr(
        worker_module,
        'load_provider',
        _provider_loader(provider),
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = build_worker_runtime(config, spec, assignment, child)
    runtime.start()
    try:
        yield runtime, parent
    finally:
        drain: tuple[threading.Event, threading.Thread] | None = None
        try:
            drain = _start_control_drain(parent)
        finally:
            _release_failed_source_runtime(
                runtime,
                (parent, child),
                (parent_socket, child_socket),
                drain,
            )


def _strict_catchup_stop_component(
    *,
    fail_before_stop: bool,
) -> tuple[_InterruptibleComponent, threading.Event, type[Exception], str]:
    stop = threading.Event()
    entered = threading.Event()
    message = 'lane stopped during strict catch-up'
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(component).update(
        {
            '_config': SimpleNamespace(
                daemon=SimpleNamespace(max_pending_epochs=10),
            ),
            '_stop': stop,
            '_limiter': _CatchupLimiter(1),
            '_health_interval_secs': 1.0,
        }
    )

    def strict_step(**_options: object) -> object:
        entered.set()
        if fail_before_stop:
            raise InterruptedError(message)
        return SimpleNamespace(join_ready=False, status='caught-up')

    def deliver_one(_delivery: object) -> tuple[object, bool]:
        return object(), True

    resources = SimpleNamespace(
        _context=SimpleNamespace(
            _holdback_secs=1.0,
            _source_config=SimpleNamespace(fetch_interval_secs=10.0),
        ),
        _coordination=SimpleNamespace(
            synchronize=lambda: None,
            _service=_ProbeCoordination._service,
        ),
        _lane=SimpleNamespace(deliver_one=deliver_one),
        _reader=SimpleNamespace(strict_step=strict_step),
        _state=_ProbeState(),
    )
    catch_up = component_type._catch_up_strict

    def _run() -> object:
        return catch_up(
            component,
            cast('_LaneResources', resources),
            cast('JoinTarget', SimpleNamespace(identity=SimpleNamespace(boundary=41))),
        )

    return _InterruptibleComponent(_run, stop.set), entered, InterruptedError, message


def _cooperative_stop_component(
    phase: _ComponentStopPhase,
    *,
    fail_before_stop: bool,
) -> tuple[_InterruptibleComponent, threading.Event, type[Exception], str]:
    factories = {
        'bootstrap': _bootstrap_stop_component,
        'eager-catchup': _eager_catchup_stop_component,
        'local-call': _local_call_stop_component,
        'resume': _resume_stop_component,
        'strict-catchup': _strict_catchup_stop_component,
    }
    return factories[phase](fail_before_stop=fail_before_stop)


def _wait_for_group_database(runtime: WorkerRuntime, path: Path, group: str) -> None:
    row: tuple[int] | None = None
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        if runtime.fatal_reason is not None:
            break
        if path.exists():
            with closing(sqlite3.connect(path)) as connection:
                tables = connection.execute(
                    'SELECT COUNT(*) FROM sqlite_master '
                    "WHERE type = 'table' AND name IN ('groups', 'group_state')"
                ).fetchone()
                if tables == (2,):
                    row = connection.execute(
                        'SELECT groups.group_generation FROM groups '
                        'JOIN group_state USING (group_name) '
                        'WHERE group_name = ? AND active = 1',
                        (group,),
                    ).fetchone()
        if row == (7,):
            break
        time.sleep(0.005)
    assert runtime.fatal_reason is None
    assert row == (7,), (group, row)


def _wait_for_group_delivery(
    runtime: WorkerRuntime,
    path: Path,
    invoked: list[int],
    event_id: int,
) -> None:
    row: tuple[int, int] | None = None
    for _ in range(2_000):
        if path.exists():
            with closing(sqlite3.connect(path)) as connection:
                row = connection.execute(
                    'SELECT cursor, last_delivered_id FROM group_state'
                ).fetchone()
        if row == (event_id, event_id) and invoked == [event_id]:
            break
        if runtime.fatal_reason is not None:
            break
        time.sleep(0.001)
    assert runtime.fatal_reason is None
    assert invoked == [event_id]
    assert row == (event_id, event_id)


def _wait_for_lane_token(
    runtime: WorkerRuntime,
    group: str,
    prior_token: str | None = None,
    *,
    timeout: float = 2.0,
) -> str | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        lane = _runtime_lane(runtime, group)
        if lane is not None:
            join_state = vars(lane)['_join_state']
            # A provisional attempt does not establish a ready delivery endpoint.
            if isinstance(join_state, _FinalizedJoin):
                token = join_state._identity.attempt_token
            else:
                token = None
            if token and token != prior_token:
                return token
        if runtime.fatal_reason is not None:
            break
        time.sleep(0.01)
    return None


def _assert_initial_endpoint_delivery(
    runtime: WorkerRuntime,
    provider_state: _RestartProviderState,
    invoked: list[int],
    *,
    timeout: float,
) -> None:
    assert _wait_for_lane_token(runtime, 'remote', timeout=timeout) is not None
    assert _wait_for_lane_token(runtime, 'healthy', timeout=timeout) is not None
    provider_state._publish(42)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remote_lane = _runtime_lane(runtime, 'remote')
        if (
            invoked == [42]
            and remote_lane is not None
            and vars(remote_lane)['_last_snapshot'].cursor == 42
        ):
            break
        time.sleep(0.01)
    remote_lane = _runtime_lane(runtime, 'remote')
    threads = runtime._threads
    assert invoked == [42]
    assert runtime.fatal_reason is None
    assert threads['lane:healthy'].is_alive()
    assert remote_lane is not None
    assert vars(remote_lane)['_last_snapshot'].cursor == 42


def _assert_resumed_delivery(
    runtime: WorkerRuntime,
    path: Path,
    provider_state: _RestartProviderState,
    invoked: list[int],
    group: str,
) -> None:
    assert _wait_for_lane_token(runtime, group) is not None
    provider_state._publish(58)
    _wait_for_group_delivery(runtime, path, invoked, 58)


def _assert_source_restart_continuity(scenario: _RestartContinuity) -> None:
    first_token = _wait_for_lane_token(
        scenario._dependent_runtime,
        'remote',
    )
    assert first_token is not None
    healthy_first_token = _wait_for_lane_token(
        scenario._dependent_runtime,
        'healthy',
    )
    assert healthy_first_token is not None
    scenario._provider_state._publish(42)
    assert scenario._callback_entered.wait(2.0)
    scenario._provider_state._publish(43)
    assert scenario._provider_state._wait_for_query(43, 'source:events')
    first_endpoint = scenario._first_source._endpoint
    scenario._first_source._close()
    scenario._release_callback.set()
    time.sleep(0.05)
    assert scenario._invoked == [42]
    assert scenario._dependent_runtime.fatal_reason is None
    second_source = _start_source_runtime(
        scenario._config,
        scenario._site_spec,
        scenario._source_assignment,
        scenario._grants,
    )
    try:
        assert second_source._endpoint == first_endpoint
        second_token = _wait_for_lane_token(
            scenario._dependent_runtime,
            'remote',
            first_token,
        )
        assert second_token is not None, {
            'source_fatal_reason': second_source._runtime.fatal_reason,
            'dependent_fatal_reason': scenario._dependent_runtime.fatal_reason,
            'remote_lifecycle': cast(
                '_LaneComponent', scenario._dependent_runtime._components['lane:remote']
            )._lifecycle,
            'owner_health': scenario._dependent_runtime._current_health(time.monotonic()),
        }
        assert second_token != first_token
        healthy_second_token = _wait_for_lane_token(
            scenario._dependent_runtime,
            'healthy',
            healthy_first_token,
        )
        assert healthy_second_token is not None
        assert healthy_second_token != healthy_first_token
        for _ in range(2_000):
            remote_lane = _runtime_lane(
                scenario._dependent_runtime,
                'remote',
            )
            if (
                scenario._invoked == [42, 43]
                and remote_lane is not None
                and vars(remote_lane)['_last_snapshot'].cursor == 43
            ):
                break
            time.sleep(0.001)
        remote_lane = _runtime_lane(scenario._dependent_runtime, 'remote')
        healthy_lane = _runtime_lane(scenario._dependent_runtime, 'healthy')
        assert scenario._invoked == [42, 43], {
            'source_fatal_reason': second_source._runtime.fatal_reason,
            'dependent_fatal_reason': scenario._dependent_runtime.fatal_reason,
            'remote_snapshot': None
            if remote_lane is None
            else vars(remote_lane)['_last_snapshot'],
            'healthy_snapshot': None
            if healthy_lane is None
            else vars(healthy_lane)['_last_snapshot'],
        }
        assert remote_lane is not None
        assert healthy_lane is not None
        assert vars(remote_lane)['_last_snapshot'].cursor == 43
        assert vars(healthy_lane)['_last_snapshot'].status == 'running'
        assert scenario._dependent_runtime.fatal_reason is None
    finally:
        second_source._close()


def _wait_for_source_component_progress(
    ready: Callable[[], bool],
    errors: list[Exception],
    *,
    attempts: int,
    pump: Callable[[], object] | None = None,
) -> None:
    reached = False
    for _ in range(attempts):
        if pump is not None:
            pump()
        reached = ready()
        if reached or errors:
            break
        time.sleep(0.001)
    assert errors == []
    assert reached


def _watchdog_group_health(
    group: str,
    *,
    source: str,
    worker_instance: str,
) -> GroupHealth:
    return GroupHealth(
        site='main',
        source=source,
        group=group,
        worker_instance=worker_instance,
        execution='sync',
        ordering='eager',
        phase='startup',
        status='callback-failed',
        cursor=41,
        current_event_id=None,
        current_plugin_id=None,
        current_callback_id=None,
        delivery_attempt=None,
        heartbeat_at=1.0,
        heartbeat_age=9.0,
        queue_event_count=3,
        queue_byte_count=300,
        catchup_frontier=45,
        pending_catchup_epoch_count=2,
        oldest_catchup_epoch_age=4.0,
        join_phase='detached',
        outstanding_late_obligation_count=1,
        outstanding_late_id_count=2,
        outstanding_late_ids=(43, 44),
        outstanding_late_ids_truncated=False,
        parked=True,
        park_reason='durable park',
        failure_kind='callback',
        failure_text='durable failure',
        checkpoint_state='incomplete',
        conditions=('callback-failed', 'parked'),
        audit_count=7,
        deadline_at=30.0,
        overdue_reason='durable overdue',
    )


def _watchdog_source_health(
    source: str,
    *,
    worker_instance: str,
) -> SourceHealth:
    return SourceHealth(
        site='main',
        source=source,
        source_generation=f'{source}-generation-1',
        status='uninitialized',
        frontier=41,
        successful_clock=2.0,
        retry_attempt=3,
        next_retry_secs=4.0,
        open_range_count=5,
        late_id_count=6,
        last_error='durable provider failure',
        worker_instance=worker_instance,
        initialized=True,
        pending_obligation_count=7,
        orphan_marker_count=8,
        strict_buffer_event_count=11,
        strict_buffer_byte_count=12,
        strict_retention=(('strict', 13, 14, 15),),
        checkpoint_state='incomplete',
        stream_incarnation='incarnation-1',
        audit_count=16,
        database_size_bytes=17,
        phase='wait',
        heartbeat_at=1.0,
        heartbeat_age=9.0,
        deadline_at=30.0,
        overdue_reason='durable overdue',
    )


def _run_shutdown_probe_body(
    component: _SourceComponent | _LaneComponent, releases: dict[str, threading.Event]
) -> None:
    watchdog = component._watchdog
    sample = watchdog.sample()
    cache = component._health_cache
    if sample.kind == 'source':
        cache._publish_source(
            _watchdog_source_health(
                'events', worker_instance=component._assignment.spawn_instance
            ),
            sample=sample,
        )
    else:
        cache._publish_group(
            _watchdog_group_health(
                'local', source='events', worker_instance=component._assignment.spawn_instance
            ),
            sample=sample,
        )
    assert component._stop.wait(2)
    if isinstance(component, _LaneComponent):
        component._close_partial_attempt(worker_module._PartialLaneAttempt())
    else:
        watchdog.transition('shutdown', deadline_at=component._shutdown_deadline_at)
    assert releases[sample.kind].wait(2)


def _watchdog_token() -> str:
    return os.urandom(16).hex()


def _run_health_cache_snapshot_probe(connection: Connection) -> None:
    try:
        cache = _ComponentHealthCache(site='main', group_sources=MappingProxyType({}))
        successor = WatchdogSample(
            registration_token=_watchdog_token(),
            transition_sequence=1,
            kind='source',
            identity='events',
            worker_instance='spawn-1',
            phase='startup',
            heartbeat_at=2.0,
            deadline_at=12.0,
        )
        predecessor = replace(
            successor,
            registration_token=_watchdog_token(),
            heartbeat_at=1.0,
            deadline_at=11.0,
        )

        def replace_registration() -> WatchdogSample:
            cache._register_watchdog(
                kind='source',
                identity='events',
                worker_instance='spawn-1',
                token=successor.registration_token,
                supplier=lambda: successor,
            )
            return predecessor

        cache._register_watchdog(
            kind='source',
            identity='events',
            worker_instance='spawn-1',
            token=predecessor.registration_token,
            supplier=replace_registration,
        )
        first = cache._snapshot()
        cache._deregister_watchdog(
            kind='source',
            identity='events',
            worker_instance='spawn-1',
            token=predecessor.registration_token,
        )
        second = cache._snapshot()
        connection.send(
            (
                tuple(first._watchdogs),
                tuple((item.source, item.heartbeat_at) for item in second._watchdogs),
            )
        )
    finally:
        connection.close()


def _run_hung_strict_step_probe(connection: Connection, path: Path) -> None:
    wall = [0.0]
    worker_module.time.monotonic = lambda: wall[0]
    with _open_group_health_state(path / 'hung-group.db') as state:
        component_object, _cache, resources_object, _lifecycle = _group_health_component(
            path,
            state,
            health_interval_secs=5.0,
        )
        component = component_object
        resources = cast('_LaneResources', resources_object)
        watchdog = ComponentWatchdog(
            kind='lane',
            identity='local',
            worker_instance='site-spawn-1',
            registration_token=_watchdog_token(),
            startup_deadline_secs=192.0,
            clock=lambda: wall[0],
        )
        watchdog.transition('idle', deadline_at=None)
        vars(component)['_watchdog'] = watchdog
        assert vars(component)['_internal_stall_deadline_secs'] == 192.0
        entered = threading.Event()

        def strict_step(**_options: object) -> NoReturn:
            entered.set()
            threading.Event().wait()
            raise AssertionError('hung reader unexpectedly returned')

        vars(resources).update(
            _context=SimpleNamespace(
                _group=SimpleNamespace(ordering='strict'), _holdback_secs=300.0
            ),
            _client=SimpleNamespace(
                last_join_target=SimpleNamespace(
                    identity=SimpleNamespace(boundary=45),
                    stream_incarnation='stream-incarnation-1',
                )
            ),
            _reader=SimpleNamespace(strict_step=strict_step),
            _coordination=SimpleNamespace(synchronize=lambda: None),
        )

        def observe() -> None:
            assert entered.wait(1.0)
            wall[0] = 193.0
            connection.send(watchdog.sample())

        threading.Thread(target=observe).start()
        component._catch_up(resources, 45)


def _service_queued_local_calls(component: object, surface: object) -> None:

    class _Server:
        @staticmethod
        def dispatch_join_requests(_surface: object) -> None:
            return None

        @staticmethod
        def poll(_timeout: float) -> None:
            return None

    def publish_health(_poller: object, _server: object) -> None:
        return None

    vars(component).update(
        {
            '_assignment': SimpleNamespace(source_group_assignments=()),
            '_config': SimpleNamespace(sites={'main': SimpleNamespace(groups={})}),
            '_grant_lock': threading.Lock(),
            '_grants': deque(),
            '_health_interval_secs': 1.0,
            '_next_pending_health_refresh_at': None,
            '_pending_group_assignments': (),
            '_pending_group_detail': {},
            '_pending_group_detail_dirty': True,
            '_publish_health': publish_health,
            '_coordination_deadline_secs': 10.0,
            '_source': 'events',
            '_spec': SimpleNamespace(site='main'),
            '_watchdog': ComponentWatchdog(
                kind='source',
                identity='events',
                worker_instance='source-service-test',
                registration_token=_watchdog_token(),
                startup_deadline_secs=10.0,
            ),
        }
    )
    component_type = _SourceComponent
    service = component_type._service
    service(
        cast('_SourceComponent', component),
        cast('EventServer', _Server()),
        cast('_SourceJoinSurface', surface),
        cast('SourcePoller', object()),
    )


def _source_health_poller(state: SourceState, provider: Provider) -> SourcePoller:
    site = _runtime_site(_group('local', worker='default'))
    source = site.sources['events']
    watchdog = ComponentWatchdog(
        kind='source',
        identity='events',
        worker_instance='site-spawn-1',
        registration_token=_watchdog_token(),
        startup_deadline_secs=192.0,
    )
    return SourcePoller(
        state=state,
        fanout=EventFanout(),
        provider=provider,
        site_url=site.server,
        credential=site.credentials[source.credentials],
        config=source,
        max_ipc_frame_bytes=8_388_608,
        source_generation='source-generation-1',
        waiter=_SourceHealthWaiter(),
        random_value=lambda: 0.5,
        watchdog=watchdog,
        internal_deadline_secs=192.0,
        wait_tolerance_secs=5.0,
        worker_instance='site-spawn-1',
        checkpoint_state=lambda: 'idle',
        successful_clock=_SuccessfulQueryClock(clock=lambda: 0.0),
    )


def test_authorize_remote_source_does_not_start_drain_after_health_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    grant = EventPeerGrant(
        source='events',
        group='remote',
        dependent_spawn_instance='dependent-spawn-1',
        capability='remote-capability',
    )
    ready = EventPeerReady(
        source=grant.source,
        group=grant.group,
        dependent_spawn_instance=grant.dependent_spawn_instance,
    )
    sent: list[object] = []
    drain_starts: list[WorkerControl] = []
    control = cast('WorkerControl', SimpleNamespace(send=sent.append))

    def receive(_control: WorkerControl, expected: type[object]) -> object:
        assert expected is EventPeerReady
        return ready

    def fail_health(_control: WorkerControl, _config: Config) -> NoReturn:
        raise RuntimeError('injected health failure')

    def start_drain(value: WorkerControl) -> tuple[threading.Event, threading.Thread]:
        drain_starts.append(value)
        return threading.Event(), threading.Thread()

    module = sys.modules[__name__]
    monkeypatch.setattr(module, '_receive_type', receive)
    monkeypatch.setattr(module, '_assert_remote_server_health', fail_health)
    monkeypatch.setattr(module, '_start_control_drain', start_drain)

    with pytest.raises(RuntimeError, match='injected health failure'):
        _authorize_remote_source(control, grant, cast('Config', object()))

    assert sent == [grant]
    assert drain_starts == []


@pytest.mark.parametrize(
    'path',
    ['reconnect', 'join-retry'],
)
def test_blocked_attempt_channel_retirement_publishes_internal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    path: Literal['reconnect', 'join-retry'],
) -> None:
    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        probe = _delivery_retirement_probe(tmp_path, state)
        component = cast('object', probe.component)

        def build_channel(_context: object, _state: object) -> object:
            probe.operations.append('build')
            return probe.replacement

        def pause(_resources: object, **_options: object) -> None:
            return None

        component_values = cast('dict[str, object]', vars(component))
        component_values['_build_channel'] = build_channel
        component_values['_pause'] = pause
        lifecycle: SimpleNamespace | None = None
        if path == 'reconnect':

            def reconnect(_client: EventClient) -> None:
                probe.operations.append('reconnect')
                probe.reconnect_entered.set()
                assert probe.reconnect_release.wait(1.0)

            monkeypatch.setattr(EventClient, 'reconnect', reconnect)
            vars(probe.resources)['_client'] = _unconnected_event_client(tmp_path / 'events.sock')
            operation = partial(
                cast('type[_LaneComponent]', type(component))._reconnect_remote,
                cast('_LaneComponent', component),
                probe.resources,
                probe.channel,
            )
        else:
            retry = JoinRetry(
                attempt=JoinAttemptIdentity(
                    site='main',
                    source='events',
                    group='local',
                    join_kind='eager',
                    group_generation=7,
                    attempt_token=os.urandom(16).hex(),
                ),
                retry_after_secs=0.25,
                headroom_events=0,
                headroom_bytes=0,
            )

            def return_join_retry(
                *_args: object,
                **_options: object,
            ) -> JoinRetry:
                return retry

            vars(probe.resources)['_lane'] = SimpleNamespace(join=return_join_retry)
            lifecycle = SimpleNamespace(_channel=probe.channel, _joined=False)
            operation = partial(
                cast('type[_LaneComponent]', type(component))._join_lane,
                cast('_LaneComponent', component),
                probe.resources,
                cast('_LaneLifecycle', lifecycle),
            )
        internal, startup, result, before, internal_budget = _run_blocked_retirement(
            probe,
            operation,
            expect_reconnect=path == 'reconnect',
        )

    assert probe.channel._entered_at is not None
    assert internal.phase == 'internal'
    assert internal.deadline_at is not None
    assert (
        before + internal_budget
        <= internal.deadline_at
        <= (probe.channel._entered_at + internal_budget)
    )
    assert (
        internal.event_id,
        internal.plugin_id,
        internal.callback_id,
        internal.delivery_attempt,
    ) == (None, None, None, None)
    assert probe.watchdog.sample().registration_token == probe.token
    if path == 'reconnect':
        assert startup is not None
        assert startup.phase == 'startup'
        assert startup.deadline_at is not None
        assert (
            startup.event_id,
            startup.plugin_id,
            startup.callback_id,
            startup.delivery_attempt,
        ) == (None, None, None, None)
        assert result == (probe.replacement, True)
        assert probe.operations == ['close', 'reconnect', 'build']
    else:
        assert result is False
        assert lifecycle is not None
        assert lifecycle._channel is probe.replacement
        assert probe.operations == ['close', 'build']


@pytest.mark.parametrize('stage', ['connect', 'observation'])
def test_blocked_lane_bootstrap_call_publishes_startup(
    stage: Literal['connect', 'observation'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, _resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        watchdog = component._watchdog
        watchdog.transition('idle', deadline_at=time.monotonic() + 10.0)

        def _block() -> None:
            entered.set()
            assert release.wait(1.0)

        operation = _lane_bootstrap_operation(
            stage,
            component,
            monkeypatch,
            tmp_path,
            _block,
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(operation)
            try:
                assert entered.wait(1.0)
                snapshot = cache._snapshot()
            finally:
                release.set()
            future.result(timeout=2.0)

    assert snapshot._watchdogs[0].phase == 'startup'
    assert snapshot._watchdogs[0].deadline_at is not None


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('stage', ['reader', 'synchronization'])
def test_blocked_lane_catchup_publishes_internal(
    ordering: Literal['eager', 'strict'],
    stage: Literal['reader', 'synchronization'],
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        watchdog = component._watchdog
        watchdog.transition('idle', deadline_at=time.monotonic() + 10.0)
        context = vars(resources)['_context']
        context._group = replace(context._group, ordering=ordering)
        context._holdback_secs = 0.0
        vars(resources)['_state'] = _ProbeState()

        def block() -> None:
            entered.set()
            assert release.wait(1.0)

        synchronize = block if stage == 'synchronization' else lambda: None
        vars(resources)['_coordination'] = SimpleNamespace(
            synchronize=synchronize,
            _service=_ProbeCoordination._service,
        )
        vars(resources)['_client'].last_join_target = SimpleNamespace(
            identity=SimpleNamespace(boundary=45),
            stream_incarnation='incarnation-1',
        )

        if ordering == 'eager':

            def eager_catch_up(**options: object) -> object:
                if stage == 'synchronization':
                    cast('Callable[[], object]', options['source_progress'])()
                else:
                    block()
                return SimpleNamespace(status='caught-up')

            vars(resources)['_reader'] = SimpleNamespace(
                catch_up=eager_catch_up,
            )
        else:

            def strict_step(**options: object) -> object:
                if stage == 'synchronization':
                    cast('Callable[[], object]', options['source_progress'])()
                else:
                    block()
                return SimpleNamespace(status='caught-up', join_ready=True)

            vars(resources)['_reader'] = SimpleNamespace(
                strict_step=strict_step,
            )
        run_catch_up = cast(
            'Callable[[object, object, int], None]', vars(type(component))['_catch_up']
        )

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_catch_up, component, resources, 45)
            try:
                assert entered.wait(1.0)
                snapshot = cache._snapshot()
            finally:
                release.set()
            future.result(timeout=2.0)

    health = snapshot._watchdogs[0]
    assert health.phase == 'internal'
    assert health.deadline_at is not None
    assert (
        health.event_id,
        health.plugin_id,
        health.callback_id,
        health.delivery_attempt,
    ) == (None, None, None, None)


@pytest.mark.parametrize('cleanup', ['partial', 'final'])
def test_blocked_lane_cleanup_publishes_shutdown(
    cleanup: Literal['partial', 'final'],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    class _BlockingClose:
        def close(self) -> None:
            entered.set()
            assert release.wait(1.0)

    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        watchdog = component._watchdog
        now = [100.0]
        monkeypatch.setattr(watchdog, '_clock', lambda: now[0])
        watchdog.transition('idle', deadline_at=101.0)
        assert isinstance(component, _LaneComponent)
        component._request_stop(shutdown_deadline_at=110.0)
        _publish_group_health(component, resources, _lifecycle)
        if cleanup == 'partial':
            attempt_type = worker_module._PartialLaneAttempt
            attempt = attempt_type(_channel=cast('GroupChannel', _BlockingClose()))

            def operation() -> None:
                close_partial = cast(
                    'Callable[[object, object], None]',
                    vars(type(component))['_close_partial_attempt'],
                )
                close_partial(component, attempt)

        else:
            component._stop.set()
            vars(resources)['_channel'] = _BlockingClose()
            vars(resources)['_client'] = SimpleNamespace(close=lambda: None)
            vars(resources)['_lane'] = SimpleNamespace(close=lambda: None)
            vars(resources)['_reader'] = SimpleNamespace(
                __enter__=lambda: None,
                close=lambda: None,
            )
            vars(resources)['_state'] = SimpleNamespace(close=lambda: None)

            def operation() -> None:
                drive_lane = cast(
                    'Callable[[object, object], None]',
                    vars(type(component))['_drive_lane'],
                )
                drive_lane(component, resources)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(operation)
            try:
                assert entered.wait(1.0)
                snapshot = cache._snapshot()
            finally:
                release.set()
            future.result(timeout=2.0)
        now[0] = 102.0
        completed = cache._snapshot()
        assert completed._watchdogs[0].phase == 'shutdown'
        assert completed._watchdogs[0].deadline_at == 110.0
        assert completed._groups[0].deadline_at == 110.0
        assert 101.0 < now[0] < 110.0

    assert snapshot._watchdogs[0].phase == 'shutdown'
    assert snapshot._watchdogs[0].deadline_at is not None
    assert snapshot._groups[0].phase == 'shutdown'


def test_blocked_lane_construction_publishes_startup_over_cached_idle(
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, resources, lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        assert lifecycle is not None
        watchdog = component._watchdog
        watchdog.transition('idle', deadline_at=time.monotonic() + 10.0)
        _publish_group_health(component, resources, lifecycle)
        token = watchdog.sample().registration_token
        assert cache._snapshot()._groups[0].phase == 'idle'

        def build_client(_context: object) -> NoReturn:
            entered.set()
            assert release.wait(1.0)
            raise RuntimeError('construction probe released')

        vars(component)['_build_client'] = build_client
        configuration = cast(
            'Callable[[object], _LaneConfiguration]',
            vars(type(component))['_configuration'],
        )
        context = configuration(component)
        build_resources = cast(
            'Callable[[object, object], object]',
            vars(type(component))['_build_resources'],
        )

        def construct() -> None:
            with pytest.raises(RuntimeError, match=r'^construction probe released$'):
                build_resources(component, context)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(construct)
            try:
                assert entered.wait(1.0)
                snapshot = cache._snapshot()
                assert snapshot._watchdogs[0].phase == 'startup'
                assert snapshot._watchdogs[0].deadline_at is not None
                assert snapshot._groups[0].phase == 'startup'
                assert watchdog.sample().registration_token == token
            finally:
                release.set()
                future.result(timeout=2.0)


def test_blocked_lane_latest_visible_id_publishes_startup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    class _BlockingReader:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def latest_visible_id(self) -> int:
            entered.set()
            assert release.wait(1.0)
            return 41

    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, _resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        watchdog = component._watchdog
        watchdog.transition('idle', deadline_at=time.monotonic() + 10.0)
        configuration = cast(
            'Callable[[object], _LaneConfiguration]',
            vars(type(component))['_configuration'],
        )
        context = configuration(component)
        context = replace(
            context, _assignment=replace(context._assignment, start_mode='initialize')
        )
        observation = GroupBootstrapObservation(
            source='events',
            group='local',
            group_generation=7,
            source_generation='source-generation-1',
            stream_incarnation='incarnation-1',
            captured_frontier=40,
            source_successful_clock=1.0,
        )

        def open_group_state(*_args: object, **_options: object) -> object:
            cast('WriteContext', _options['writes']).lock.close()
            return object()

        monkeypatch.setattr(GroupState, 'open', open_group_state)
        monkeypatch.setattr(
            component, '_write_context', lambda: write_context_for(tmp_path / 'group.sqlite3')
        )
        open_state = type(component)._open_group_state
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                open_state,
                component,
                context,
                observation,
                cast('CatchupReader', _BlockingReader()),
            )
            try:
                assert entered.wait(1.0)
                snapshot = cache._snapshot()
            finally:
                release.set()
            future.result(timeout=2.0)

    assert snapshot._watchdogs[0].phase == 'startup'
    assert snapshot._watchdogs[0].deadline_at is not None


def test_blocked_lane_pause_publishes_idle_with_wake_tolerance(tmp_path: Path) -> None:
    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        watchdog = component._watchdog
        watchdog.transition('internal', deadline_at=time.monotonic() + 10.0)
        wake = component._wake
        pause = type(component)._pause
        before = time.monotonic()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                pause,
                component,
                cast('_LaneResources', resources),
                seconds=1.0,
            )
            snapshot = cache._snapshot()
            for _ in range(1_000):
                snapshot = cache._snapshot()
                if snapshot._watchdogs[0].phase == 'idle':
                    break
                time.sleep(0.001)
            wake.set()
            future.result(timeout=2.0)

    health = snapshot._watchdogs[0]
    assert health.phase == 'idle'
    assert health.deadline_at is not None
    assert before + 1.05 <= health.deadline_at <= time.monotonic() + 1.05


def test_blocked_lane_reader_entry_publishes_startup(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()

    class _BlockingReader:
        def __enter__(self) -> Self:
            entered.set()
            assert release.wait(1.0)
            stop.set()
            return self

        def close(self) -> None:
            return None

    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        stop = component._stop
        watchdog = component._watchdog
        watchdog.transition('idle', deadline_at=time.monotonic() + 10.0)
        vars(resources).update(
            _channel=SimpleNamespace(close=lambda: None),
            _client=SimpleNamespace(close=lambda: None),
            _lane=SimpleNamespace(close=lambda: None),
            _reader=_BlockingReader(),
            _state=SimpleNamespace(close=lambda: None),
        )
        drive_lane = cast('Callable[[object, object], None]', vars(type(component))['_drive_lane'])
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(drive_lane, component, resources)
            try:
                assert entered.wait(1.0)
                snapshot = cache._snapshot()
            finally:
                release.set()
            future.result(timeout=2.0)

    assert snapshot._watchdogs[0].phase == 'startup'
    assert snapshot._watchdogs[0].deadline_at is not None


def test_build_worker_specs_packs_every_group_exactly_once() -> None:
    site = _site(
        (
            _group('default-a', worker='default'),
            _group('default-b', worker='default', source='audit'),
            _group('named-a', worker='pool'),
            _group('named-b', worker='pool', source='audit'),
            _group('dedicated-a', worker='dedicated'),
            _group('dedicated-b', worker='dedicated'),
        )
    )

    specs = build_worker_specs(site)

    assert tuple(spec.kind for spec in specs) == (
        WorkerKind._SITE,
        WorkerKind._NAMED,
        WorkerKind._DEDICATED,
        WorkerKind._DEDICATED,
    )
    assert specs[0].sources == ('audit', 'events')
    assert specs[0].groups == ('default-a', 'default-b')
    assert specs[1].groups == ('named-a', 'named-b')
    assert specs[2].groups == ('dedicated-a',)
    assert specs[3].groups == ('dedicated-b',)
    assert len({spec.worker_id for spec in specs}) == len(specs)
    assert sorted(group for spec in specs for group in spec.groups) == sorted(site.groups)


def test_busy_lane_leaves_worker_and_packed_sibling_alive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(worker_module, '_LANE_OPERATOR_ADMISSION_SECS', 0.05)
    blocked = _group('blocked', worker='default', ordering='eager')
    sibling = _group('sibling', worker='default', ordering='eager')
    site = replace(
        _runtime_site(blocked),
        groups=MappingProxyType({blocked.name: blocked, sibling.name: sibling}),
    )
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    first = _local_assignment(spec, blocked.name, 'initialize')
    second = _local_assignment(spec, sibling.name, 'initialize')
    assignment = replace(
        first,
        group_assignments=first.group_assignments + second.group_assignments,
        source_group_assignments=first.source_group_assignments + second.source_group_assignments,
    )
    entered = threading.Event()
    release = threading.Event()
    provider_state = _RestartProviderState()
    plugin = _restart_callback_plugin([], callback_entered=entered, release_callback=release)

    def load_plugins(
        _site: SiteConfig, group: GroupConfig, _provider: Provider
    ) -> tuple[PluginSpec, ...]:
        return (plugin,) if group.name == blocked.name else ()

    def park(control: WorkerControl, group: str) -> CommandResult:
        request = OperatorCommand(
            request_id=f'park-{group}',
            command='park',
            arguments={'site': site.name, 'group': group, 'reason': 'maintenance'},
            identity=AuditIdentity.current(),
        )
        control.send(request)
        response = _receive_type(control, CommandResult, deadline=time.monotonic() + 2.0)
        assert response.request_id == request.request_id
        return response

    monkeypatch.setattr(worker_module, 'load_plugins', load_plugins)
    with _running_concrete_runtime(
        monkeypatch, config, spec, assignment, _restart_provider(provider_state)
    ) as (runtime, control):
        try:
            _receive_typed_health(
                control,
                lambda item: (
                    len(item.groups) == 2
                    and all(group.join_phase == 'attached' for group in item.groups)
                ),
            )
            provider_state._publish(42)
            assert entered.wait(2.0)
            response = park(control, blocked.name)
            assert not response.ok
            assert response.result == {'code': 'group-busy'}
            assert runtime.fatal_reason is None
            assert park(control, sibling.name).ok
            assert runtime.fatal_reason is None
            assert runtime._threads['lane:blocked'].is_alive()
            assert runtime._threads['lane:sibling'].is_alive()
        finally:
            release.set()


def test_callback_gate_records_nonrunning_observation_before_next_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component_object, resources_object = _lifecycle_probe(
        'sync', durable_status='parked', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(_channel=resources._channel)
    lifecycle._was_running = True
    lifecycle._next_coordination_at = time.monotonic() + 60.0
    calls: list[tuple[bool, bool]] = []
    monkeypatch.setattr(
        resources._coordination,
        '_service',
        lambda *, scan_obligations=True, force=False: calls.append((scan_obligations, force)),
    )
    assert component._callback_gate_snapshot(resources, lifecycle) is None
    vars(resources._state)['_status'] = 'running'
    component._service_due(resources, lifecycle)
    assert calls == [(True, True)]


def test_catchup_reader_slots_are_bounded_without_losing_waiters() -> None:
    spec = build_worker_specs(_site((_group('default', worker='default'),)))[0]
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=1.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )
    entered: list[str] = []
    first_holds = threading.Event()
    release_first = threading.Event()

    def reader(name: str) -> None:
        with runtime._catchup_limiter._slot():
            entered.append(name)
            if name == 'first':
                first_holds.set()
                release_first.wait()

    first = threading.Thread(target=reader, args=('first',))
    second = threading.Thread(target=reader, args=('second',))
    second_started = False
    first.start()
    try:
        assert first_holds.wait(1.0)
        second.start()
        second_started = True
        time.sleep(0.02)
        assert entered == ['first']
        release_first.set()
        first.join(1.0)
        second.join(1.0)
    finally:
        release_first.set()
        first.join(1.0)
        if second_started:
            second.join(1.0)
    assert not first.is_alive()
    assert not second.is_alive()
    assert entered == ['first', 'second']


def test_checkpoint_halt_fails_worker_through_component_failure_path(tmp_path: Path) -> None:
    spec = build_worker_specs(_site(()))[0]
    components = {name: _Component(name, []) for name in spec.sources}
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='checkpoint-failure',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=lambda _health: None,
        health_interval_secs=0.001,
        checkpoint_interval_secs=0.02,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            run_checkpoint=lambda: (1, 5, 0),
            random_value=lambda: 0,
        )

    runtime._checkpoint_host = CheckpointHost(
        interval_secs=0.02,
        synchronous='NORMAL',
        on_failure=runtime._component_failed,
        checkpointer_factory=factory,
    )
    path = tmp_path / 'state.db'
    schema = Schema(version=1, statements=('CREATE TABLE entries (id INTEGER)',))
    with open_database(path, 'events', schema, synchronous='NORMAL'):
        pass
    runtime.start()
    try:
        runtime._checkpoint_host.register(path, 'events', schema)
        assert runtime.wait() == ()
        reason = runtime.fatal_reason
        assert reason is not None
        assert reason.startswith('checkpoint:events thread failed: CheckpointBoundError:')
        assert 'incomplete duration bound exceeded' in reason
        runtime._component_failed('later failure')
        assert runtime.fatal_reason == reason
    finally:
        runtime.request_stop('test')
        runtime._shutdown()


def test_compact_health_preflight_accounts_for_pending_group_records(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, _snapshot = _pending_group_runtime(tmp_path)
    encoded: list[bytes] = []

    def capture(update: HealthUpdate) -> bytes:
        frame = encode_control_message(update)
        encoded.append(frame)
        return frame

    monkeypatch.setattr(worker_module, 'encode_control_message', capture)
    parent_socket, child_socket = socket.socketpair()
    with parent_socket, child_socket:
        control = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
        empty_config = replace(
            config,
            sites=MappingProxyType(
                {'main': replace(config.sites['main'], groups=MappingProxyType({}))}
            ),
        )
        build_worker_runtime(
            empty_config, spec, replace(assignment, source_group_assignments=()), control
        )
        assert len(encoded) == 1
        source_only_budget = len(encoded[0])
        limited = replace(
            config, daemon=replace(config.daemon, max_ipc_frame_bytes=source_only_budget)
        )
        with pytest.raises(ConfigError, match=r'compact health.*max_ipc_frame_bytes'):
            build_worker_runtime(limited, spec, assignment, control)


def test_compact_health_preflight_maximum_shape_bounds_real_update(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    group = _group('local', worker='default', ordering='strict')
    site = _runtime_site(group)
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(
            DeliveryGroupAssignment(
                source='events',
                group='local',
                group_generation=7,
                start_mode='initialize',
            ),
        ),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='events',
                group='local',
                group_generation=7,
                start_mode='initialize',
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )
    maximum_updates: list[HealthUpdate] = []

    def capture_maximum(update: HealthUpdate) -> bytes:
        maximum_updates.append(update)
        return encode_control_message(update)

    monkeypatch.setattr(worker_module, 'encode_control_message', capture_maximum)
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        _runtime_provider(),
    ) as (runtime, control):
        actual: HealthUpdate | None = None
        for _ in range(400):
            try:
                message = control.receive(timeout=0.01)
            except TimeoutError:
                if runtime.fatal_reason is not None:
                    break
                continue
            if (
                isinstance(message, HealthUpdate)
                and len(message.sources) == 1
                and len(message.groups) == 1
                and len(message.event_sockets) == 1
            ):
                actual = message
                break
        assert actual is not None, runtime.fatal_reason

    assert len(maximum_updates) == 1
    maximum = maximum_updates[0]
    maximum_int = 2**63 - 1
    assert tuple(source.source for source in maximum.sources) == spec.sources
    assert tuple(group_health.group for group_health in maximum.groups) == spec.groups
    assert tuple(
        (socket_health.role, socket_health.source, socket_health.group)
        for socket_health in maximum.event_sockets
    ) == (('server', 'events', None),)
    assert maximum.worker.thread_names == (
        'checkpoint:source:main:events',
        'lane:local',
        'operator-executor',
        'source:events',
    )
    assert maximum.worker.stuck_threads == maximum.worker.thread_names
    assert maximum.worker.source_outages[0][0] == 'events'
    _assert_maximum_watchdog_health(maximum)
    assert maximum.sources[0].frontier == maximum_int
    assert maximum.groups[0].cursor == maximum_int
    assert (
        maximum.sources[0].audit_count,
        maximum.sources[0].database_size_bytes,
        maximum.groups[0].audit_count,
    ) == (maximum_int,) * 3
    assert maximum.event_sockets[0].queue_byte_count == maximum_int
    assert len(maximum.groups[0].outstanding_late_ids) == 100
    assert all(
        len(str(late_id)) == len(str(maximum_int))
        for late_id in maximum.groups[0].outstanding_late_ids
    )
    diagnostics = (
        maximum.sources[0].last_error,
        maximum.groups[0].park_reason,
        maximum.groups[0].failure_text,
        maximum.event_sockets[0].reason,
        maximum.worker.breaker_reason,
        maximum.worker.source_outages[0][1],
        maximum.worker.watchdog_failure,
        maximum.sources[0].overdue_reason,
        maximum.groups[0].overdue_reason,
    )
    assert all(detail is not None and len(detail.encode()) == 4_096 for detail in diagnostics)
    assert len(encode_control_message(actual)) <= len(encode_control_message(maximum))
    escaped_diagnostic = '\0' * 4_096
    escaped_actual = replace(
        actual,
        worker=replace(
            actual.worker,
            breaker_reason=escaped_diagnostic,
            source_outages=(('events', escaped_diagnostic),),
        ),
        sources=(replace(actual.sources[0], last_error=escaped_diagnostic),),
        groups=(
            replace(
                actual.groups[0],
                park_reason=escaped_diagnostic,
                failure_text=escaped_diagnostic,
            ),
        ),
        event_sockets=(replace(actual.event_sockets[0], reason=escaped_diagnostic),),
    )
    assert len(encode_control_message(escaped_actual)) <= len(encode_control_message(maximum))


def test_compact_health_preflight_rejects_unfit_inventory_before_thread_start(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:

    def _construct(group_count: int, *, max_frame_bytes: int = 524_288) -> WorkerRuntime:
        groups = tuple(
            _group(f'group-{index:02}', worker='default') for index in range(group_count)
        )
        site = replace(
            _runtime_site(groups[0]),
            groups=MappingProxyType({group.name: group for group in groups}),
        )
        config = Config(
            daemon=DaemonConfig(
                state_dir=tmp_path,
                sg_api_provider='shotgun-api3',
                max_ipc_frame_bytes=max_frame_bytes,
            ),
            sites=MappingProxyType({site.name: site}),
        )
        spec = build_worker_specs(site)[0]
        group_assignments = tuple(
            DeliveryGroupAssignment(
                source='events',
                group=group.name,
                group_generation=index + 1,
                start_mode='initialize',
            )
            for index, group in enumerate(groups)
        )
        assignment = WorkerAssignment(
            worker_id=spec.worker_id,
            spawn_instance='site-spawn-1',
            source_generations=(('events', 'source-generation-1'),),
            group_assignments=group_assignments,
            source_group_assignments=tuple(
                ActiveSourceGroupAssignment(
                    source=item.source,
                    group=item.group,
                    group_generation=item.group_generation,
                    start_mode=item.start_mode,
                )
                for item in group_assignments
            ),
            source_endpoints=(),
            event_grants=(),
        )
        parent_socket, child_socket = socket.socketpair()
        control = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
        try:
            return build_worker_runtime(config, spec, assignment, control)
        finally:
            parent_socket.close()
            control.close()

    def fail_start(_thread: threading.Thread) -> None:
        raise AssertionError('compact health preflight started a thread')

    monkeypatch.setattr(threading.Thread, 'start', fail_start)

    with pytest.raises(ConfigError, match=r'compact health.*max_ipc_frame_bytes'):
        _construct(8)
    with pytest.raises(ConfigError, match=r'compact health.*max_ipc_frame_bytes'):
        _construct(1, max_frame_bytes=30_000)

    assert isinstance(_construct(2), WorkerRuntime)
    assert isinstance(_construct(8, max_frame_bytes=8_388_608), WorkerRuntime)


def test_compact_health_source_only_preflight_uses_longest_operator_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = replace(
        _runtime_site(_group('local', worker='default')),
        groups=MappingProxyType({}),
    )
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='source-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )
    maximum_updates: list[HealthUpdate] = []

    def capture_maximum(update: HealthUpdate) -> bytes:
        maximum_updates.append(update)
        return encode_control_message(update)

    monkeypatch.setattr(worker_module, 'encode_control_message', capture_maximum)
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    try:
        build_worker_runtime(config, spec, assignment, control)
        maximum = maximum_updates[-1]
        operator = maximum.watchdogs[-1]
        old_maximum = replace(
            maximum,
            watchdogs=(
                *maximum.watchdogs[:-1],
                replace(operator, operator_command='adopt-source-stream'),
            ),
        )
        long_maximum = replace(
            maximum,
            watchdogs=(
                *maximum.watchdogs[:-1],
                replace(operator, operator_command='preflight-source-adoption'),
            ),
        )
        old_body_bytes = len(encode_control_message(old_maximum)) - 4
        long_body_bytes = len(encode_control_message(long_maximum)) - 4
        assert long_body_bytes - old_body_bytes == 6

        exact_old_config = replace(
            config,
            daemon=replace(config.daemon, max_ipc_frame_bytes=old_body_bytes),
        )
        with pytest.raises(
            ConfigError,
            match=rf'compact health requires {long_body_bytes} bytes; '
            rf'max_ipc_frame_bytes is {old_body_bytes}',
        ):
            build_worker_runtime(exact_old_config, spec, assignment, control)
        assert maximum_updates[-1].watchdogs[-1].operator_command == ('preflight-source-adoption')
    finally:
        parent_socket.close()
        control.close()


@pytest.mark.parametrize(
    ('component_name', 'failure'),
    [
        ('_SourceComponent', False),
        ('_SourceComponent', True),
        ('_LaneComponent', False),
        ('_LaneComponent', True),
    ],
)
def test_component_close_retires_watchdog_after_every_run_return(
    component_name: str,
    failure: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component_type = cast('type[object]', vars(worker_module)[component_name])
    component = object.__new__(component_type)
    identity = 'events' if component_name == '_SourceComponent' else 'local'
    kind = 'source' if component_name == '_SourceComponent' else 'lane'
    cache = _ComponentHealthCache(
        site='main',
        group_sources=MappingProxyType({'local': 'events'}),
    )
    watchdog = ComponentWatchdog(
        kind=kind,
        identity=identity,
        worker_instance='site-spawn-1',
        registration_token=_watchdog_token(),
        startup_deadline_secs=10.0,
    )
    sample = watchdog.sample()
    cache._register_watchdog(
        kind=sample.kind,
        identity=sample.identity,
        worker_instance=sample.worker_instance,
        token=sample.registration_token,
        supplier=watchdog.sample,
    )
    vars(component).update(_health_cache=cache, _watchdog=watchdog)

    def run_body(_component: object) -> None:
        if failure:
            raise RuntimeError('component failed')

    monkeypatch.setattr(component_type, '_run_body', run_body)
    run_component = cast('Callable[[object], None]', vars(component_type)['_run'])
    assert [(item.kind, item.source, item.group) for item in cache._snapshot()._watchdogs] == [
        ('source', 'events', None)
        if component_name == '_SourceComponent'
        else ('lane', 'events', 'local')
    ]

    if failure:
        with pytest.raises(RuntimeError, match='component failed'):
            run_component(component)
    else:
        run_component(component)

    assert len(cache._snapshot()._watchdogs) == 1
    close_component = cast('Callable[[object], None]', vars(component_type)['_close'])
    close_component(component)
    close_component(component)
    assert cache._snapshot()._watchdogs == ()


def test_component_envelopes_construct_one_bounded_startup_watchdog(tmp_path: Path) -> None:
    source, _source_cache = _source_health_component(
        tmp_path,
        health_interval_secs=2.0,
    )
    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        delivery, _delivery_cache, _delivery_resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=2.0,
        )

    samples: list[WatchdogSample] = []
    for component in (source, delivery):
        watchdog = vars(component).get('_watchdog')
        assert isinstance(watchdog, ComponentWatchdog)
        samples.append(watchdog.sample())

    assert [(sample.kind, sample.identity) for sample in samples] == [
        ('source', 'events'),
        ('lane', 'local'),
    ]
    assert all(sample.worker_instance == 'site-spawn-1' for sample in samples)
    assert all(sample.phase == 'startup' for sample in samples)
    assert all(sample.deadline_at == sample.heartbeat_at + 192.0 for sample in samples)
    assert len({sample.registration_token for sample in samples}) == 2


@pytest.mark.parametrize('execution', ['sync', 'async'])
@pytest.mark.parametrize('error_type', [KeyboardInterrupt, SystemExit])
def test_concrete_callback_interrupt_preserves_in_flight_state_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    execution: Literal['sync', 'async'],
    error_type: type[BaseException],
) -> None:
    group = _group('local', worker='default', execution=execution)
    site = _runtime_site(group)
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    provider_state = _RestartProviderState()
    failure = error_type('callback interrupted')
    errors: list[BaseException | None] = []
    interrupted = threading.Event()
    forced: list[int] = []

    def fail(_context: EventContext) -> NoReturn:
        raise failure

    async def async_fail(_context: EventContext) -> NoReturn:
        raise failure

    plugin = _health_failure_plugin('callback')
    callback = replace(
        plugin.callbacks[0],
        callback=async_fail if execution == 'async' else fail,
        identity=replace(plugin.callbacks[0].identity, execution=execution),
    )
    plugin = replace(plugin, callbacks=(callback,))

    def load(*_args: object) -> tuple[PluginSpec, ...]:
        return (plugin,)

    original_hook = threading.excepthook

    def observe_error(args: threading.ExceptHookArgs) -> None:
        errors.append(args.exc_value)
        if args.exc_value is failure:
            interrupted.set()
        else:
            original_hook(args)

    monkeypatch.setattr(worker_module, 'load_plugins', load)
    monkeypatch.setattr(threading, 'excepthook', observe_error)
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        _local_assignment(spec, group.name, 'initialize'),
        _restart_provider(provider_state, async_=execution == 'async'),
    ) as (runtime, control):
        runtime._force_exit = forced.append
        _receive_typed_health(
            control,
            lambda item: any(health.join_phase == 'attached' for health in item.groups),
        )
        cache = runtime._health_cache
        assert cache is not None
        assert any(item.group == group.name for item in cache._snapshot()._watchdogs)
        provider_state._publish(42)
        assert interrupted.wait(3.0)
        runtime._threads['lane:local'].join(1.0)
        assert not runtime._threads['lane:local'].is_alive()
        _assert_interrupted_worker_stopped(runtime, error_type)
        assert _runtime_lane(runtime, group.name) is None

        database = _source_database_path(tmp_path)
        with closing(sqlite3.connect(f'{database.as_uri()}?mode=ro', uri=True)) as connection:
            snapshot = connection.execute(
                'SELECT cursor, in_flight_event_id, parked, failure_code, failure_text '
                'FROM group_state'
            ).fetchone()
            audits = connection.execute('SELECT command FROM audit').fetchall()
        assert snapshot == (41, 42, 0, None, None)
        assert ('conditional-park',) not in audits

    assert cache._snapshot()._watchdogs == ()
    assert len(errors) == 1
    assert errors[0] is failure
    assert not runtime._resources_open
    assert not _runtime_threads_alive(runtime)
    assert forced == []


@pytest.mark.parametrize(('synchronous', 'expected'), [('normal', 1), ('full', 2)])
def test_concrete_runtime_database_uses_configured_synchronous_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    synchronous: Literal['normal', 'full'],
    expected: int,
) -> None:
    group = _group('local', worker='default')
    site = _runtime_site(group)
    config = _runtime_config(tmp_path, site)
    config = replace(config, daemon=replace(config.daemon, sqlite_synchronous=synchronous))
    spec = build_worker_specs(site)[0]
    observed: dict[str, set[tuple[int, int]]] = {'source': set(), 'group': set()}
    source_snapshot = SourceState.snapshot
    group_snapshot = GroupState.snapshot

    def observe(state: SourceState | GroupState, kind: str) -> None:
        primary = state._database.connection
        handoff = state._handoff.connection
        observed[kind].add(
            (
                primary.execute('PRAGMA synchronous').fetchone()[0],
                handoff.execute('PRAGMA synchronous').fetchone()[0],
            )
        )

    def observe_source(state: SourceState) -> SourceSnapshot:
        observe(state, 'source')
        return source_snapshot(state)

    def observe_group(state: GroupState) -> GroupSnapshot:
        observe(state, 'group')
        return group_snapshot(state)

    monkeypatch.setattr(SourceState, 'snapshot', observe_source)
    monkeypatch.setattr(GroupState, 'snapshot', observe_group)
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        _local_assignment(spec, group.name, 'initialize'),
        _runtime_provider(),
    ) as (runtime, control):
        _wait_for_group_database(runtime, _source_database_path(tmp_path), group.name)
        _receive_typed_health(control, lambda item: bool(item.sources and item.groups))
        assert observed == {'source': {(expected, 2)}, 'group': {(expected, 2)}}


def test_concrete_runtime_health_redacts_source_outage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = _site(())
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=tuple((source, f'{source}-generation-1') for source in spec.sources),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        _outage_runtime_provider(),
    ) as (runtime, control):
        update = _receive_typed_health(
            control,
            lambda item: any(source.retry_attempt > 0 for source in item.sources),
        )
        degraded = tuple(source for source in update.sources if source.retry_attempt > 0)
        assert degraded
        assert all(source.last_error == '<redacted>' for source in degraded)
        assert all(source.next_retry_secs is not None for source in degraded)
        assert runtime.fatal_reason is None


def test_concrete_runtime_health_surfaces_failed_callback_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    group = _group('local', worker='default', ordering='eager')
    site = _runtime_site(group)
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    provider_state = _RestartProviderState()
    monkeypatch.setattr(
        worker_module,
        'load_plugins',
        _health_failure_plugin_loader('callback'),
    )
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        _local_assignment(spec, group.name, 'initialize'),
        _restart_provider(provider_state),
    ) as (runtime, control):
        _receive_typed_health(
            control,
            lambda item: any(
                group_health.join_phase == 'attached' for group_health in item.groups
            ),
        )
        provider_state._publish(42)
        health = _receive_typed_health(
            control,
            lambda item: any(
                group_health.failure_kind == 'callback' for group_health in item.groups
            ),
        ).groups[0]
        assert health.phase in {'idle', 'internal'}
        assert health.current_event_id is None
        assert health.current_plugin_id is None
        assert health.current_callback_id is None
        assert health.delivery_attempt is None
        assert health.failure_text is not None
        assert '<redacted>' in health.failure_text
        assert 'password=' not in health.failure_text
        assert runtime.fatal_reason is None


def test_concrete_runtime_health_surfaces_park_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    group = _group('local', worker='default', ordering='eager')
    site = _runtime_site(group)
    _prepare_runtime_state(
        tmp_path,
        site,
        group,
        'resume',
        park_reason='token=operator-secret maintenance',
    )
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        _local_assignment(spec, group.name, 'resume'),
        _runtime_provider(),
    ) as (runtime, control):
        health = _receive_typed_health(
            control,
            lambda item: any(group_health.parked for group_health in item.groups),
        ).groups[0]
        assert health.parked
        assert health.join_phase == 'parked'
        assert health.park_reason == '<redacted>'
        assert 'parked' in health.conditions
        assert runtime.fatal_reason is None


def test_concrete_runtime_health_surfaces_setup_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    group = _group('local', worker='default', ordering='eager')
    site = _runtime_site(group)
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    monkeypatch.setattr(
        worker_module,
        'load_plugins',
        _health_failure_plugin_loader('setup'),
    )
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        _local_assignment(spec, group.name, 'initialize'),
        _runtime_provider(),
    ) as (runtime, control):
        health = _receive_typed_health(
            control,
            lambda item: any(group_health.failure_kind == 'setup' for group_health in item.groups),
        ).groups[0]
        assert health.failure_kind == 'setup'
        assert health.failure_text is not None
        assert '<redacted>' in health.failure_text
        assert 'token=' not in health.failure_text
        assert 'callback-failed' in health.conditions
        assert runtime.fatal_reason is None


def test_concrete_runtime_health_uses_component_thread_caches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    group = _group('local', worker='default', ordering='eager')
    site = _runtime_site(group)
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    source_threads: list[str] = []
    group_threads: list[str] = []
    source_snapshot = SourceState.snapshot
    group_snapshot = GroupState.snapshot

    def guarded_source_snapshot(state: SourceState) -> object:
        thread_name = threading.current_thread().name
        source_threads.append(thread_name)
        assert thread_name != 'sg-event-daemon:monitor'
        return source_snapshot(state)

    def guarded_group_snapshot(state: GroupState) -> object:
        thread_name = threading.current_thread().name
        group_threads.append(thread_name)
        assert thread_name != 'sg-event-daemon:monitor'
        return group_snapshot(state)

    def telemetry_value(_state: SourceState | GroupState, value: int) -> int:
        return value

    monkeypatch.setattr(SourceState, 'snapshot', guarded_source_snapshot)
    monkeypatch.setattr(GroupState, 'snapshot', guarded_group_snapshot)
    monkeypatch.setattr(
        SourceState,
        'audit_count',
        partialmethod(telemetry_value, value=11),
    )
    monkeypatch.setattr(
        GroupState,
        'audit_count',
        partialmethod(telemetry_value, value=13),
    )
    monkeypatch.setattr(
        SourceState,
        'database_size_bytes',
        partialmethod(telemetry_value, value=12_345),
    )
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        _local_assignment(spec, group.name, 'initialize'),
        _runtime_provider(),
    ) as (runtime, control):
        _wait_for_group_database(
            runtime,
            _source_database_path(tmp_path),
            group.name,
        )
        update = _receive_typed_health(
            control,
            lambda item: bool(item.sources and item.groups),
        )
        assert any('source:events' in name for name in source_threads)
        assert any('lane:local' in name for name in group_threads)
        assert isinstance(update.worker, WorkerHealth)
        assert tuple(item.source for item in update.sources) == ('events',)
        assert all(isinstance(item, SourceHealth) for item in update.sources)
        assert tuple(item.group for item in update.groups) == ('local',)
        assert all(isinstance(item, GroupHealth) for item in update.groups)
        _assert_complete_source_health(update.sources[0])
        _assert_complete_group_health(update.groups[0])
        assert (
            update.sources[0].audit_count,
            update.sources[0].database_size_bytes,
            update.groups[0].audit_count,
        ) == (11, 12_345, 13)


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_concrete_runtime_publishes_retention_on_source_thread(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    ordering: Literal['eager', 'strict'],
) -> None:
    group = _group('local', worker='default', ordering=ordering)
    site = _runtime_site(group)
    spec = build_worker_specs(site)[0]
    calls: list[str] = []
    retention_health = StrictBuffer.retention_health

    def snapshot(buffer: StrictBuffer) -> tuple[tuple[str, int, int, int], ...]:
        calls.append(threading.current_thread().name)
        return retention_health(buffer)

    monkeypatch.setattr(StrictBuffer, 'retention_health', snapshot)
    with _running_concrete_runtime(
        monkeypatch,
        _runtime_config(tmp_path, site),
        spec,
        _local_assignment(spec, group.name, 'initialize'),
        _restart_provider(_RestartProviderState()),
    ) as (_runtime, control):
        update = _receive_typed_health(
            control,
            lambda item: (
                bool(item.sources)
                and any(health.join_phase == 'attached' for health in item.groups)
                and (ordering == 'eager' or bool(item.sources[0].strict_retention))
            ),
        )
        if ordering == 'strict':
            assert update.sources[0].strict_retention == (('local', 41, 0, 0),)
            assert calls
            assert all(name.endswith('source:events') for name in calls)
        else:
            assert update.sources[0].strict_retention == ()
            assert calls == []


def test_cooperative_lane_stop_does_not_hide_cleanup_failure() -> None:
    order = ('channel', 'lane', 'state', 'reader', 'client')
    operations: list[str] = []
    cleanup_error = RuntimeError('lane cleanup failed')
    probes = {
        name: _CleanupProbe(
            name,
            operations,
            cleanup_error if name == 'lane' else None,
        )
        for name in order
    }
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(probes['lane'])['watch_phase'] = component._watchdog.watch
    stop = threading.Event()
    entered = threading.Event()
    vars(component).update({'_lane': probes['lane'], '_stop': stop})
    resources = SimpleNamespace(
        _channel=probes['channel'],
        _client=probes['client'],
        _lane=probes['lane'],
        _reader=probes['reader'],
        _state=probes['state'],
    )
    cooperative_stop = cast(
        'Callable[[Literal["local-call"]], RuntimeError]',
        vars(worker_module)['_CooperativeComponentStopError'],
    )
    drive = cast('_LaneLifecycleDriver', vars(component_type)['_drive_lane'])

    def service_calls(_resources: object) -> NoReturn:
        entered.set()
        assert stop.wait(1.0)
        raise cooperative_stop('local-call')

    vars(component)['_service_calls'] = service_calls
    runtime_component = _InterruptibleComponent(
        lambda: drive(component, resources),
        stop.set,
    )

    fatal_reason, forced = _run_cooperative_stop_component(
        runtime_component,
        entered,
        request_stop=True,
    )

    assert fatal_reason == 'lane:local thread failed: RuntimeError: lane cleanup failed'
    assert forced == []
    assert operations == list(order)
    assert vars(component)['_lane'] is None


def test_current_health_reads_stuck_threads_under_state_lock() -> None:
    spec = build_worker_specs(_site(()))[0]
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=1.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )
    expected = ('source:events',)

    class _StateLock:
        def __enter__(self) -> None:
            vars(runtime)['_stuck_thread_names'] = expected

        def __exit__(self, *_args: object) -> None:
            vars(runtime)['_stuck_thread_names'] = ()

    vars(runtime)['_state_lock'] = _StateLock()

    health = runtime._current_health(12.0)

    assert health.stuck_threads == expected


def test_detached_remote_tick_refreshes_durable_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    component_object, resources_object = _lifecycle_probe(
        'sync', durable_status='running', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(_channel=resources._channel)
    client = EventClient(
        path=tmp_path / 'absent.sock',
        source='events',
        group='local',
        spawn_instance='worker-1',
        capability='a' * 64,
        max_frame_bytes=262_144,
        queue_max_entries=10,
        queue_max_bytes=262_144,
        coordination_deadline_secs=10.0,
        stop=threading.Event(),
    )
    vars(resources)['_client'] = client

    calls: list[str] = []

    def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
        del force
        del scan_obligations
        calls.append('refresh')

    monkeypatch.setattr(resources._coordination, '_service', coordinate)
    component._coordinate(resources, lifecycle, scan=False, force=False)
    assert calls == ['refresh']
    assert lifecycle._next_coordination_at > time.monotonic()


def test_detached_strict_holdback_outlives_internal_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _detached_strict_join(
        tmp_path,
        monkeypatch,
        modes=('local', 'sync'),
        case='watchdog',
    )


def test_eager_catchup_callback_failure_remains_group_local(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    groups = (
        _group('halted', worker='default'),
        _group('healthy', worker='default'),
    )
    site = replace(
        _runtime_site(groups[0]),
        groups=MappingProxyType({group.name: group for group in groups}),
    )
    _prepare_retained_history_runtime_state(tmp_path, site, groups)
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = replace(
        _local_assignment(spec, 'halted', 'resume'),
        group_assignments=tuple(
            DeliveryGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='resume',
            )
            for group in groups
        ),
        source_group_assignments=tuple(
            ActiveSourceGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='resume',
            )
            for group in groups
        ),
    )
    provider_state = _RestartProviderState()
    provider_state._publish(42)
    provider_state._publish(60)
    invoked: list[int] = []
    failed_plugin = _health_failure_plugin('callback')

    def record(context: EventContext) -> None:
        event_id = context.event['id']
        assert type(event_id) is int
        invoked.append(event_id)

    healthy_plugin = replace(
        failed_plugin,
        callbacks=(replace(failed_plugin.callbacks[0], callback=record),),
    )

    def load(_site: SiteConfig, group: GroupConfig, _provider: Provider) -> tuple[PluginSpec, ...]:
        return (failed_plugin if group.name == 'halted' else healthy_plugin,)

    monkeypatch.setattr(worker_module, 'load_plugins', load)
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        _restart_provider(provider_state),
    ) as (runtime, control):
        update: HealthUpdate | None = None
        for _ in range(400):
            if runtime.fatal_reason is not None:
                break
            try:
                message = control.receive(timeout=0.01)
            except TimeoutError:
                continue
            if isinstance(message, HealthUpdate) and (
                any(group.failure_kind == 'callback' for group in message.groups)
                and any(group.join_phase == 'attached' for group in message.groups)
            ):
                update = message
                break
        assert runtime.fatal_reason is None
        assert update is not None
        health = {group.group: group for group in update.groups}
        assert health['halted'].status == 'callback-failed'
        assert health['halted'].join_phase != 'attached'
        assert health['healthy'].join_phase == 'attached'
        assert runtime.fatal_reason is None
        provider_state._publish(61)
        _receive_typed_health(
            control,
            lambda item: any(
                group.group == 'healthy' and group.cursor == 61 for group in item.groups
            ),
        )
        assert invoked == [61]
        assert runtime.fatal_reason is None
        assert runtime._threads['lane:halted'].is_alive()
        assert runtime._threads['lane:healthy'].is_alive()

    with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
        failed = connection.execute(
            'SELECT cursor, in_flight_event_id, failure_code FROM group_state'
        ).fetchone()
    assert failed == (41, 42, 'callback-failed')


@pytest.mark.parametrize(
    ('boundary', 'fail_callback'), [(42, False), (43, False), (45, False), (45, True)]
)
def test_eager_catchup_commits_initialized_floor_without_skipping_eligible_events(
    tmp_path: Path,
    boundary: int,
    fail_callback: bool,
) -> None:
    # Bootstrap captured 41; the later fresh query chose delivery floor 43.
    provider_state = _RestartProviderState()
    for event_id in range(41, 46):
        provider_state._publish(event_id)
    provider = _restart_provider(provider_state)
    group = _group('local', worker='default')
    site = _runtime_site(group)
    component = _unopened_group_component(_LaneComponent)
    watchdog = component._watchdog
    plugin = _health_failure_plugin('callback')
    invoked: list[int] = []

    def callback(context: EventContext) -> None:
        invoked.append(cast('int', context.event['id']))
        if fail_callback:
            raise RuntimeError('eligible callback failed')

    plugin = replace(plugin, callbacks=(replace(plugin.callbacks[0], callback=callback),))
    with (
        _open_group_health_state(tmp_path / 'group.db', delivery_floor=43) as state,
        CatchupReader(
            provider=provider,
            site_url=site.server,
            credential=site.credentials['source'],
            batch_size=3,
            fetch_interval_secs=1.0,
            waiter=_SourceHealthWaiter(),
        ) as reader,
        closing(
            GroupLane(
                state=state,
                site=site,
                group=group,
                provider=provider,
                plugins=(plugin,),
                worker_instance='worker-1',
                health_interval_secs=1.0,
                watchdog=watchdog,
            )
        ) as lane,
    ):
        assert lane.setup() is None
        resources = SimpleNamespace(
            _state=state,
            _lane=lane,
            _reader=reader,
            _coordination=SimpleNamespace(
                synchronize=lambda: _synchronization(boundary),
            ),
        )
        identity = LiveJoinIdentity(
            site='main',
            source='events',
            group='local',
            join_kind='eager',
            source_generation='source-generation-1',
            group_generation=7,
            boundary=boundary,
            attempt_token=str(boundary),
        )
        target = JoinTarget(identity=identity)
        if fail_callback:
            with pytest.raises(worker_module._GroupHaltedCatchupError):
                _LaneComponent._catch_up_eager(
                    component,
                    cast('_LaneResources', resources),
                    target,
                )
            snapshot = state.snapshot()
            assert snapshot.cursor == 41
            assert snapshot.in_flight_event_id == 44
            assert state._bootstrap_state().last_delivered_id == 43
            assert invoked == [44]
        else:
            _LaneComponent._catch_up_eager(component, cast('_LaneResources', resources), target)
            snapshot = state.snapshot()
            assert snapshot.cursor == boundary
            assert snapshot.delivery_floor == 43
            assert state._bootstrap_state().last_delivered_id == max(43, boundary)
            assert invoked == ([44, 45] if boundary == 45 else [])
            assert state.validate_live_join(identity, snapshot.revision).cursor == boundary


def test_eager_catchup_uses_unresolved_obligation_bounds_for_retention() -> None:

    class _Reader:
        def catch_up(self, **options: object) -> object:
            if options.get('unresolved_bounds') == ((11, 20),):
                raise HistoryTruncatedError(15, 11)
            return SimpleNamespace(status='caught-up')

    class _State:
        def __init__(self) -> None:
            self._truncated_floor: int | None = None

        def mark_history_truncated(
            self,
            retention_floor: int,
            *,
            expected_revision: int,
        ) -> object:
            assert expected_revision == 0
            self._truncated_floor = retention_floor
            return self.snapshot()

        def snapshot(self) -> object:
            return SimpleNamespace(
                cursor=40,
                delivery_floor=0,
                replay_required_from=None,
                revision=0,
            )

        def unresolved_obligation_bounds(self) -> tuple[tuple[int, int], ...]:
            return ((11, 20),)

    state = _State()
    component_type = _LaneComponent
    component = object.__new__(component_type)

    resources = SimpleNamespace(
        _coordination=SimpleNamespace(
            synchronize=lambda: _synchronization(45),
        ),
        _lane=_ProbeLane(),
        _reader=_Reader(),
        _state=state,
    )
    driver = cast(
        'Callable[[object, object, object], None]',
        vars(component_type)['_catch_up_eager'],
    )
    halted_error = cast(
        'type[RuntimeError]',
        vars(worker_module)['_GroupHaltedCatchupError'],
    )

    with pytest.raises(halted_error, match='group halted during eager catch-up'):
        driver(
            component,
            resources,
            SimpleNamespace(identity=SimpleNamespace(boundary=45)),
        )

    assert state._truncated_floor == 15


def test_event_grant_rejects_non_source_component_with_grant_method() -> None:
    grant = EventPeerGrant(
        source='events',
        group='remote',
        dependent_spawn_instance='dependent-spawn-1',
        capability='remote-capability',
    )
    requested: list[EventPeerGrant] = []
    stopped: list[str] = []
    runtime = object.__new__(WorkerRuntime)
    vars(runtime)['_components'] = {
        'source:events': SimpleNamespace(_request_grant=requested.append)
    }
    vars(runtime)['request_stop'] = stopped.append

    runtime._install_event_grant(grant)

    assert requested == []
    assert stopped == ['event grant source is not owned by worker']


def test_floor_spanning_post_close_scan_skips_delivery_floor_events() -> None:
    state = _RetentionCoordinationState()
    reader = _RetentionCoordinationReader(
        (1, 1),
        tuple(Event(_runtime_event(event_id)) for event_id in range(49, 53)),
    )
    lane = _RetentionCoordinationLane(state)
    coordinator = LaneRefresh(
        lane=cast('GroupLane', lane),
        limiter=_CatchupLimiter(1),
        reader=cast('CatchupReader', reader),
        state=cast('GroupState', state),
    )
    scan = cast(
        'Callable[[object, SourceProgress], None]',
        vars(type(coordinator))['_scan_obligations'],
    )

    coordinator.synchronize()
    scan(coordinator, _synchronization(60))

    assert reader._ranges == [(40, 60)]
    assert state._audited == [49, 50]
    assert lane._delivered == [51, 52]
    assert lane._resolved


def test_group_health_refresh_uses_interval_deadlines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_group_health_state(tmp_path / 'delivery.db') as state:
        before_park = state.snapshot()
        state.park(
            reason='maintenance',
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
            expected_revision=before_park.revision,
        )
        component, cache, resources, lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=1.0,
        )
        calls = {'park_reason': 0, 'projection': 0, 'snapshot': 0}
        real_park_reason = state.park_reason
        real_projection = state.unresolved_late_id_projection
        real_snapshot = state.snapshot

        def counted_park_reason() -> str | None:
            calls['park_reason'] += 1
            return real_park_reason()

        def counted_projection(*, limit: int) -> tuple[int, tuple[int, ...]]:
            calls['projection'] += 1
            return real_projection(limit=limit)

        def counted_snapshot() -> GroupSnapshot:
            calls['snapshot'] += 1
            return real_snapshot()

        monkeypatch.setattr(state, 'park_reason', counted_park_reason)
        monkeypatch.setattr(
            state,
            'unresolved_late_id_projection',
            counted_projection,
        )
        monkeypatch.setattr(state, 'snapshot', counted_snapshot)

        _publish_group_health(component, resources, lifecycle)
        initial = cache._snapshot()._groups[0]
        assert calls == {'park_reason': 1, 'projection': 1, 'snapshot': 1}
        assert component._health_refresh._deadline_at == 1.0

        for pass_number in range(1, 100):
            clock.now = pass_number / 100
            _publish_group_health(component, resources, lifecycle)

        assert calls == {'park_reason': 1, 'projection': 1, 'snapshot': 1}
        assert cache._snapshot()._groups[0] == initial

        clock.now = 1.0
        _publish_group_health(component, resources, lifecycle)
        assert calls == {'park_reason': 2, 'projection': 2, 'snapshot': 2}
        assert component._health_refresh._deadline_at == 2.0

        clock.now = 4.25
        _publish_group_health(component, resources, lifecycle)
        assert calls == {'park_reason': 3, 'projection': 3, 'snapshot': 3}
        assert component._health_refresh._deadline_at == 5.0


@pytest.mark.parametrize(
    'error',
    [
        WriteLockTimeoutError('owner wait expired', 'process-local'),
        WriteLockStoppedError('owner stopped'),
        SourceDatabaseBusyError('command busy'),
        ValueError('invalid command'),
    ],
)
def test_group_mailbox_preserves_acquisition_failure_ownership(
    error: Exception,
) -> None:
    def invoke(_resources: _LaneResources) -> object:
        raise error

    component = _LaneComponent.__new__(_LaneComponent)
    component._mailbox = _OwnerMailbox[_LaneResources]('lane')
    call = component._mailbox._submit(invoke)
    resources = SimpleNamespace(_state=None, _client=None)
    service = cast('Callable[[object], None]', component._service_calls)
    component._lane = None
    watchdog_identity = 'mailbox'
    component._watchdog = ComponentWatchdog(
        kind='lane',
        identity=watchdog_identity,
        worker_instance='worker',
        registration_token=watchdog_identity,
        startup_deadline_secs=10,
    )
    component._internal_stall_deadline_secs = 5
    if isinstance(error, (WriteLockTimeoutError, WriteLockStoppedError)):
        with pytest.raises(type(error), match=str(error)):
            service(resources)
        assert not call._complete.is_set()
    else:
        service(resources)
        assert call._complete.is_set()
        assert call._error is error


def test_group_mutation_snapshot_from_halted_coordination_supersedes_resume(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 0.0)
    with _open_group_health_state(tmp_path / 'halted-mutation.db') as state:
        initial = state.snapshot()
        parked = state.park(
            reason='maintenance',
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
            expected_revision=initial.revision,
        )
        component, cache, resources, lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=1.0,
        )
        assert lifecycle is not None
        vars(lifecycle)['_joined'] = False
        _publish_group_health(component, resources, lifecycle)
        assert cache._snapshot()._groups[0].outstanding_late_obligation_count == 0

        def coordinate(*, scan_obligations: bool = True, force: bool = False) -> GroupSnapshot:
            del force
            assert not scan_obligations
            _bind_worker_range(state, 'range-1', 42)
            return state.record_delivery_floor_skip(41, expected_revision=parked.revision)

        vars(resources)['_coordination'] = SimpleNamespace(_service=coordinate)
        vars(component)['_lifecycle'] = lifecycle

        def awakened(_seconds: float | None = None) -> bool:
            return True

        monkeypatch.setattr(component._wake, 'wait', awakened)
        resume_lane = type(component)._resume_lane

        assert not resume_lane(
            component,
            cast('_LaneResources', resources),
            cast('_LaneLifecycle', lifecycle),
        )
        _publish_group_health(component, resources, lifecycle)

        health = cache._snapshot()._groups[0]
        assert health.parked
        assert health.outstanding_late_obligation_count == 1


def test_health_cache_blocking_supplier_stops_monitor_publication(
    tmp_path: Path,
) -> None:
    site = _site(())
    config = _runtime_config(tmp_path, site)
    spec = WorkerSpec(
        worker_id='["named","empty"]',
        site='main',
        kind=WorkerKind._NAMED,
        name='empty',
        sources=(),
        groups=(),
    )
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='sampling-block-1',
        source_generations=(),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = build_worker_runtime(config, spec, assignment, child)
    cache = runtime._health_cache
    assert cache is not None
    entered = threading.Event()
    release = threading.Event()
    sample = WatchdogSample(
        registration_token=_watchdog_token(),
        transition_sequence=1,
        kind='source',
        identity='events',
        worker_instance='sampling-block-1',
        phase='startup',
        heartbeat_at=1.0,
        deadline_at=11.0,
    )

    def block() -> WatchdogSample:
        entered.set()
        release.wait()
        return sample

    cache._register_watchdog(
        kind='source',
        identity='events',
        worker_instance='sampling-block-1',
        token=sample.registration_token,
        supplier=block,
    )
    cache._publish_source(
        _watchdog_source_health('events', worker_instance='sampling-block-1'),
        sample=sample,
    )
    runtime.start()
    try:
        assert entered.wait(1.0)
        with pytest.raises(TimeoutError):
            parent.receive(timeout=0.05)
    finally:
        release.set()
        runtime.request_stop('test-cleanup')
        assert runtime.wait() == ()
        parent.close()
        child.close()


def test_health_cache_lane_idle_sample_has_intended_wake_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component, context, operations, _control, _state = _recovery_construction_probe(
        stop_at_construction=False
    )
    _install_recovery_gate_probes(monkeypatch, component, operations)
    component_type = _LaneComponent
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )

    resources = build_resources(component, context)
    sample = component._watchdog.sample()
    cast('_LaneLifecycleDriver', vars(component_type)['_drive_lane'])(component, resources)

    assert sample.phase == 'idle'
    assert sample.deadline_at is not None
    assert sample.heartbeat_at < sample.deadline_at <= sample.heartbeat_at + 5.1


def test_health_cache_merge_changes_only_live_group_phase_fields() -> None:
    cache = _ComponentHealthCache(
        site='main',
        group_sources=MappingProxyType({'local': 'events'}),
    )
    published = WatchdogSample(
        registration_token=_watchdog_token(),
        transition_sequence=2,
        kind='lane',
        identity='local',
        worker_instance='spawn-1',
        phase='startup',
        heartbeat_at=2.0,
        deadline_at=12.0,
    )
    live = replace(
        published,
        transition_sequence=3,
        phase='callback',
        heartbeat_at=3.0,
        deadline_at=13.0,
        event_id=42,
        plugin_id='plugin-1',
        callback_id='callback-1',
        delivery_attempt=4,
    )
    durable = _watchdog_group_health(
        'local',
        source='events',
        worker_instance='spawn-1',
    )
    cache._register_watchdog(
        kind='lane',
        identity='local',
        worker_instance='spawn-1',
        token=published.registration_token,
        supplier=lambda: live,
    )
    cache._publish_group(durable, sample=published)

    merged = cache._snapshot()._groups[0]

    assert (
        merged.phase,
        merged.heartbeat_at,
        merged.deadline_at,
        merged.current_event_id,
        merged.current_plugin_id,
        merged.current_callback_id,
        merged.delivery_attempt,
    ) == ('callback', 3.0, 13.0, 42, 'plugin-1', 'callback-1', 4)
    assert (
        merged.cursor,
        merged.queue_event_count,
        merged.catchup_frontier,
        merged.park_reason,
        merged.failure_text,
        merged.checkpoint_state,
        merged.audit_count,
    ) == (41, 3, 45, 'durable park', 'durable failure', 'incomplete', 7)


def test_health_cache_merge_clears_stale_group_phase_identities() -> None:
    cache = _ComponentHealthCache(
        site='main',
        group_sources=MappingProxyType({'local': 'events'}),
    )
    published = WatchdogSample(
        registration_token=_watchdog_token(),
        transition_sequence=2,
        kind='lane',
        identity='local',
        worker_instance='spawn-1',
        phase='callback',
        heartbeat_at=2.0,
        deadline_at=12.0,
        event_id=42,
        plugin_id='plugin-1',
        callback_id='callback-1',
        delivery_attempt=4,
    )
    live = replace(
        published,
        transition_sequence=3,
        phase='idle',
        heartbeat_at=3.0,
        deadline_at=13.0,
        event_id=None,
        plugin_id=None,
        callback_id=None,
        delivery_attempt=None,
    )
    durable = replace(
        _watchdog_group_health(
            'local',
            source='events',
            worker_instance='spawn-1',
        ),
        current_event_id=42,
        current_plugin_id='plugin-1',
        current_callback_id='callback-1',
        delivery_attempt=4,
    )
    cache._register_watchdog(
        kind='lane',
        identity='local',
        worker_instance='spawn-1',
        token=published.registration_token,
        supplier=lambda: live,
    )
    cache._publish_group(durable, sample=published)

    merged = cache._snapshot()._groups[0]

    assert merged.phase == 'idle'
    assert merged.deadline_at == 13.0
    assert (
        merged.current_event_id,
        merged.current_plugin_id,
        merged.current_callback_id,
        merged.delivery_attempt,
    ) == (None, None, None, None)
    assert merged.failure_kind == 'callback'
    assert merged.failure_text == 'durable failure'
    assert merged.cursor == 41
    assert merged.queue_event_count == 3


@pytest.mark.parametrize(
    ('arguments', 'owner', 'budget'),
    [
        ({'site': 'main', 'source': 'events'}, 'events', 13.0),
        ({'site': 'main', 'group': 'local'}, 'local', 13.0),
    ],
)
def test_health_cache_operator_registration_routes_one_source_budget(
    arguments: dict[str, str],
    owner: str,
    budget: float,
) -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=('local',),
    )
    cache = _ComponentHealthCache(
        site='main',
        group_sources=MappingProxyType({'local': 'events'}),
    )
    operations: list[str] = []
    components = {
        'events': _BlockingOperatorComponent(
            'events',
            operations,
            interrupt_operator=True,
        ),
        'local': _BlockingOperatorComponent(
            'local',
            operations,
            interrupt_operator=True,
        ),
    }
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='operator-watchdog-1',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        control=child,
        clock=lambda: 100.0,
        health_cache=cache,
        group_sources=MappingProxyType({'local': 'events'}),
        operator_deadline_secs_by_source=MappingProxyType({'events': budget}),
    )
    command = OperatorCommand(
        request_id=f'operator-{owner}',
        command='retry' if owner == 'local' else 'adopt-source-stream',
        arguments={**arguments, 'reason': 'operator watchdog test'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=123),
    )
    runtime.start()
    try:
        parent.send(command)
        assert components[owner]._operator_entered.wait(1.0)
        watchdog = next(item for item in cache._snapshot()._watchdogs if item.kind == 'operator')
        components[owner]._operator_release.set()
        result = parent.receive(timeout=1.0)
        assert isinstance(result, CommandResult)
        idle_deadline = time.monotonic() + 1.0
        while True:
            idle = next(item for item in cache._snapshot()._watchdogs if item.kind == 'operator')
            if idle.phase == 'idle' or time.monotonic() >= idle_deadline:
                break
            time.sleep(0.001)
    finally:
        for component in components.values():
            component._operator_release.set()
        runtime.request_stop('test-cleanup')
        assert runtime.wait() == ()
        parent.close()
        child.close()

    assert result.ok
    assert watchdog.phase == 'internal'
    assert watchdog.heartbeat_at == 100.0
    assert watchdog.deadline_at == 100.0 + budget
    assert watchdog.operator_command == command.command
    assert watchdog.source == 'events'
    assert watchdog.group == arguments.get('group')
    assert idle.phase == 'idle'
    assert idle.deadline_at is None
    assert idle.source is None
    assert idle.group is None


def test_health_cache_registration_samples_and_orders_live_watchdogs() -> None:
    cache = _ComponentHealthCache(
        site='main',
        group_sources=MappingProxyType({'zulu': 'events', 'alpha': 'audit'}),
    )
    samples = (
        WatchdogSample(
            registration_token=_watchdog_token(),
            transition_sequence=4,
            kind='lane',
            identity='zulu',
            worker_instance='spawn-1',
            phase='idle',
            heartbeat_at=4.0,
            deadline_at=14.0,
        ),
        WatchdogSample(
            registration_token=_watchdog_token(),
            transition_sequence=3,
            kind='source',
            identity='events',
            worker_instance='spawn-1',
            phase='wait',
            heartbeat_at=3.0,
            deadline_at=13.0,
        ),
        WatchdogSample(
            registration_token=_watchdog_token(),
            transition_sequence=2,
            kind='operator',
            identity='operator-executor',
            worker_instance='spawn-1',
            phase='idle',
            heartbeat_at=2.0,
            deadline_at=None,
        ),
        WatchdogSample(
            registration_token=_watchdog_token(),
            transition_sequence=5,
            kind='lane',
            identity='alpha',
            worker_instance='spawn-1',
            phase='startup',
            heartbeat_at=5.0,
            deadline_at=15.0,
        ),
        WatchdogSample(
            registration_token=_watchdog_token(),
            transition_sequence=1,
            kind='source',
            identity='audit',
            worker_instance='spawn-1',
            phase='startup',
            heartbeat_at=1.0,
            deadline_at=11.0,
        ),
    )
    for sample in samples:
        cache._register_watchdog(
            kind=sample.kind,
            identity=sample.identity,
            worker_instance=sample.worker_instance,
            token=sample.registration_token,
            supplier=lambda sample=sample: sample,
        )
    cache._publish_source(
        _watchdog_source_health('events', worker_instance='spawn-1'),
        sample=samples[1],
    )
    cache._publish_source(
        _watchdog_source_health('audit', worker_instance='spawn-1'),
        sample=samples[4],
    )
    cache._publish_group(
        _watchdog_group_health('zulu', source='events', worker_instance='spawn-1'),
        sample=samples[0],
    )
    cache._publish_group(
        _watchdog_group_health('alpha', source='audit', worker_instance='spawn-1'),
        sample=samples[3],
    )

    snapshot = cache._snapshot()
    registration_keys = cast('dict[tuple[object, ...], object]', vars(cache)['_watchdogs'])

    assert tuple(source.source for source in snapshot._sources) == ('audit', 'events')
    assert tuple(group.group for group in snapshot._groups) == ('alpha', 'zulu')
    assert tuple(
        (watchdog.kind, watchdog.source, watchdog.group) for watchdog in snapshot._watchdogs
    ) == (
        ('source', 'audit', None),
        ('source', 'events', None),
        ('lane', 'audit', 'alpha'),
        ('lane', 'events', 'zulu'),
        ('operator', None, None),
    )
    assert all(
        'registration_token' not in vars(watchdog) and 'transition_sequence' not in vars(watchdog)
        for watchdog in snapshot._watchdogs
    )
    assert all(len(key) == 4 for key in registration_keys)


def test_health_cache_sequence_fence_preserves_newer_owner_publication() -> None:
    cache = _ComponentHealthCache(site='main', group_sources=MappingProxyType({}))
    sampled = WatchdogSample(
        registration_token=_watchdog_token(),
        transition_sequence=2,
        kind='source',
        identity='events',
        worker_instance='spawn-1',
        phase='startup',
        heartbeat_at=2.0,
        deadline_at=12.0,
    )
    published = replace(
        sampled,
        transition_sequence=3,
        phase='internal',
        heartbeat_at=3.0,
        deadline_at=13.0,
    )
    cache._register_watchdog(
        kind='source',
        identity='events',
        worker_instance='spawn-1',
        token=sampled.registration_token,
        supplier=lambda: sampled,
    )
    cache._publish_source(
        _watchdog_source_health('events', worker_instance='spawn-1'),
        sample=published,
    )

    snapshot = cache._snapshot()

    assert snapshot._watchdogs[0].phase == 'internal'
    assert snapshot._watchdogs[0].heartbeat_at == 3.0
    assert snapshot._sources[0].phase == 'internal'
    assert snapshot._sources[0].frontier == 41
    assert snapshot._sources[0].retry_attempt == 3
    assert snapshot._sources[0].checkpoint_state == 'incomplete'
    assert snapshot._sources[0].audit_count == 16
    assert snapshot._sources[0].database_size_bytes == 17


@pytest.mark.parametrize('kind', ['source', 'lane'])
def test_health_cache_stale_detail_cannot_replace_successor(kind: WatchdogKind) -> None:
    cache = _ComponentHealthCache(site='main', group_sources=MappingProxyType({'local': 'events'}))
    predecessor = WatchdogSample(
        registration_token=_watchdog_token(),
        transition_sequence=1,
        kind=kind,
        identity='events' if kind == 'source' else 'local',
        worker_instance='spawn-1',
        phase='startup',
        heartbeat_at=1.0,
        deadline_at=11.0,
    )
    successor = replace(
        predecessor,
        registration_token=_watchdog_token(),
        heartbeat_at=2.0,
        deadline_at=12.0,
    )
    cache._register_watchdog(
        kind=kind,
        identity='events' if kind == 'source' else 'local',
        worker_instance='spawn-1',
        token=predecessor.registration_token,
        supplier=lambda: predecessor,
    )
    cache._register_watchdog(
        kind=kind,
        identity='events' if kind == 'source' else 'local',
        worker_instance='spawn-1',
        token=successor.registration_token,
        supplier=lambda: successor,
    )

    source = _watchdog_source_health('events', worker_instance='spawn-1')
    group = _watchdog_group_health('local', source='events', worker_instance='spawn-1')

    def publish(sample: WatchdogSample) -> None:
        if kind == 'source':
            cache._publish_source(source, sample=sample)
        else:
            cache._publish_group(group, sample=sample)

    publish(predecessor)
    snapshot = cache._snapshot()
    assert snapshot._sources == ()
    assert snapshot._groups == ()
    assert [item.heartbeat_at for item in snapshot._watchdogs] == [successor.heartbeat_at]

    publish(successor)
    snapshot = cache._snapshot()
    if kind == 'source':
        assert len(snapshot._sources) == 1
        assert snapshot._sources[0].frontier == source.frontier
        assert snapshot._sources[0].heartbeat_at == successor.heartbeat_at
    else:
        assert len(snapshot._groups) == 1
        assert snapshot._groups[0].cursor == group.cursor
        assert snapshot._groups[0].heartbeat_at == successor.heartbeat_at

    publish(predecessor)
    cache._deregister_watchdog(
        kind=kind,
        identity=predecessor.identity,
        worker_instance=predecessor.worker_instance,
        token=predecessor.registration_token,
    )
    assert cache._snapshot() == snapshot


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_concrete_resume_worker_completes_persisted_callback_before_live_delivery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    group = _group(
        'local',
        worker='default',
        execution=execution,
        ordering='eager',
    )
    site = _runtime_site(group)
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    provider_state = _RestartProviderState()
    invoked: list[int] = []
    retained_contexts: list[EventContext] = []
    callback_entered = threading.Event()
    release_callback = threading.Event()
    plugin = _restart_callback_plugin(
        invoked,
        callback_entered=callback_entered,
        execution=execution,
        retained_contexts=retained_contexts,
        release_callback=release_callback,
    )
    manifest = CallbackManifest(
        callbacks=tuple(callback.identity for callback in plugin.callbacks),
    )
    _prepare_runtime_state(
        tmp_path,
        site,
        group,
        'resume',
        in_flight=(42, manifest),
    )
    provider_state._publish(42)

    def plugins(
        _site: object,
        _group: object,
        _provider: object,
    ) -> tuple[PluginSpec, ...]:
        return (plugin,)

    monkeypatch.setattr(worker_module, 'load_plugins', plugins)
    durable_snapshot: GroupSnapshot | None = None
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        _local_assignment(spec, group.name, 'resume'),
        _restart_provider(provider_state, async_=execution == 'async'),
    ) as (runtime, control):
        try:
            for _ in range(2_000):
                if invoked == [42] or runtime.fatal_reason is not None:
                    break
                time.sleep(0.001)
            assert invoked == [42], runtime.fatal_reason
            assert runtime.fatal_reason is None
            update = _receive_typed_health(
                control,
                lambda item: any(group_health.phase == 'callback' for group_health in item.groups),
            )
            _assert_blocked_callback_health(update)
            _assert_callback_heartbeat_slides_real_health(
                control,
                update,
                retained_contexts[0],
            )
            release_callback.set()
            for _ in range(2_000):
                lane = _runtime_lane(runtime, group.name)
                if lane is not None:
                    snapshot = lane._last_snapshot
                    if snapshot.cursor == 42 and snapshot.in_flight_event_id is None:
                        durable_snapshot = snapshot
                        break
                if runtime.fatal_reason is not None:
                    break
                time.sleep(0.001)
            assert runtime.fatal_reason is None
            assert durable_snapshot is not None
        finally:
            release_callback.set()

    assert durable_snapshot is not None
    source_config = site.sources[group.source]
    credential = site.credentials[source_config.credentials]
    fingerprint = stream_fingerprint(
        site.server,
        source_config.credentials,
        credential.script_name,
    )
    with open_group_on_source(
        _source_database_path(tmp_path),
        writes=write_context_for(_source_database_path(tmp_path)),
        site=site.name,
        site_server_url=site.server,
        source=group.source,
        group=group.name,
        group_generation=7,
        ordering=group.ordering,
        stream_fingerprint=fingerprint,
        source_generation=durable_snapshot.source_generation,
        verified_incarnation=durable_snapshot.verified_incarnation,
        start_mode='resume',
        strict_holdback_secs=source_config.visibility_overlap_secs,
    ) as state:
        snapshot = state.snapshot()
        assert snapshot.cursor == 42
        assert snapshot.in_flight_event_id is None


@pytest.mark.parametrize('peer_closed', [False, True])
def test_health_cache_supplier_exception_stops_monitor_without_stale_fallback(
    tmp_path: Path,
    peer_closed: bool,
) -> None:
    site = _site(())
    config = _runtime_config(tmp_path, site)
    spec = WorkerSpec(
        worker_id='["named","empty"]',
        site='main',
        kind=WorkerKind._NAMED,
        name='empty',
        sources=(),
        groups=(),
    )
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='sampling-failure-1',
        source_generations=(),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = build_worker_runtime(config, spec, assignment, child)
    cache = runtime._health_cache
    assert cache is not None
    healthy = WatchdogSample(
        registration_token=_watchdog_token(),
        transition_sequence=1,
        kind='source',
        identity='events',
        worker_instance='sampling-failure-1',
        phase='startup',
        heartbeat_at=1.0,
        deadline_at=11.0,
    )

    def fail() -> NoReturn:
        raise RuntimeError('token=must-not-escape')

    cache._register_watchdog(
        kind='source',
        identity='events',
        worker_instance='sampling-failure-1',
        token=healthy.registration_token,
        supplier=fail,
    )
    cache._publish_source(
        _watchdog_source_health('events', worker_instance='sampling-failure-1'),
        sample=healthy,
    )
    try:
        if peer_closed:
            parent.close()
        runtime.start()
        update = None if peer_closed else _receive_type(parent, HealthUpdate)
        assert runtime.wait() == ()
    finally:
        runtime.request_stop('test-cleanup')
        runtime._shutdown()
        parent.close()
        child.close()

    if update is not None:
        assert update.worker.watchdog_failure == 'watchdog-sampling-failed'
        assert update.watchdogs == ()
        assert update.sources == ()
        assert update.groups == ()
        assert update.event_sockets == ()
    assert runtime.fatal_reason == 'watchdog-sampling-failed'


def test_health_cache_supplier_runs_outside_lock_and_successor_token_wins() -> None:
    context = multiprocessing.get_context('spawn')
    parent, child = context.Pipe(duplex=False)
    process = context.Process(target=_run_health_cache_snapshot_probe, args=(child,))
    process.start()
    child.close()
    try:
        assert parent.poll(2.0), 'health-cache supplier probe timed out'
        first, second = parent.recv()
        process.join(2.0)
    finally:
        if process.is_alive():
            process.terminate()
            process.join(2.0)
        if process.is_alive():
            process.kill()
            process.join(2.0)
        parent.close()

    assert not process.is_alive()
    assert process.exitcode == 0
    assert first == ()
    assert second == (('events', 2.0),)


def test_hung_detached_strict_step_remains_visible_and_killable(tmp_path: Path) -> None:
    context = multiprocessing.get_context('spawn')
    parent, child = context.Pipe(duplex=False)
    process = context.Process(target=_run_hung_strict_step_probe, args=(child, tmp_path))
    process.start()
    child.close()
    try:
        assert parent.poll(5.0), 'hung strict step did not publish its watchdog'
        sample = parent.recv()
        assert isinstance(sample, WatchdogSample)
        assert sample.phase == 'internal'
        assert sample.deadline_at == 192.0
        assert sample.heartbeat_at == 0.0
        assert process.is_alive()
    finally:
        if process.is_alive():
            process.terminate()
            process.join(2.0)
        if process.is_alive():
            process.kill()
            process.join(2.0)
        parent.close()
    assert not process.is_alive()
    assert process.exitcode is not None
    assert process.exitcode != 0


def test_initially_absent_remote_endpoint_recovers_without_stopping_packed_sibling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    groups = (
        _group('healthy', worker='pool'),
        _group('remote', worker='pool'),
    )
    site = _site(groups)
    site = replace(
        site,
        sources=MappingProxyType(
            {
                name: replace(source, fetch_interval_secs=0.01)
                for name, source in site.sources.items()
            }
        ),
    )
    config = _runtime_config(tmp_path, site)
    site_spec, dependent_spec = build_worker_specs(site)
    provider_state = _RestartProviderState()
    invoked: list[int] = []
    release_callback = threading.Event()
    release_callback.set()
    plugin = _restart_callback_plugin(
        invoked,
        callback_entered=threading.Event(),
        release_callback=release_callback,
    )
    monkeypatch.setattr(
        worker_module,
        'load_provider',
        _provider_loader(_restart_provider(provider_state)),
    )

    def plugins(_site: object, group: object, _provider: object) -> tuple[PluginSpec, ...]:
        return (plugin,) if cast('GroupConfig', group).name == 'remote' else ()

    monkeypatch.setattr(worker_module, 'load_plugins', plugins)
    source_assignment = WorkerAssignment(
        worker_id=site_spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=tuple((source, 'source-generation-1') for source in site_spec.sources),
        group_assignments=(),
        source_group_assignments=tuple(
            ActiveSourceGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='initialize',
            )
            for group in groups
        ),
        source_endpoints=(),
        event_grants=(),
    )
    grants = tuple(
        EventPeerGrant(
            source='events',
            group=group.name,
            dependent_spawn_instance='dependent-spawn-1',
            capability=f'{group.name}-capability',
        )
        for group in groups
    )
    source_path = _source_database_path(tmp_path)
    endpoint = SourceEndpointReady(
        source='events',
        source_generation='source-generation-1',
        path=str(
            local_socket_path(
                source_path.parent,
                'events-events',
                ('main', 'events'),
            )
        ),
    )
    dependent_parent_socket, dependent_child_socket = socket.socketpair()
    dependent_parent = WorkerControl(
        dependent_parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0
    )
    dependent_child = WorkerControl(
        dependent_child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0
    )
    dependent_assignment = WorkerAssignment(
        worker_id=dependent_spec.worker_id,
        spawn_instance='dependent-spawn-1',
        source_generations=(),
        group_assignments=tuple(
            DeliveryGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='initialize',
            )
            for group in groups
        ),
        source_group_assignments=(),
        source_endpoints=(endpoint,),
        event_grants=grants,
    )
    dependent_runtime = build_worker_runtime(
        config,
        dependent_spec,
        dependent_assignment,
        dependent_child,
    )
    absent_endpoint = threading.Event()
    original_connect = EventClient.connect

    def connect(client: EventClient) -> None:
        try:
            original_connect(client)
        except ConnectionError:
            if client._group == 'remote':
                absent_endpoint.set()
            raise

    monkeypatch.setattr(EventClient, 'connect', connect)
    source_session: _SourceRuntimeSession | None = None
    dependent_runtime.start()
    drain_stop, drain_thread = _start_control_drain(dependent_parent)
    try:
        assert absent_endpoint.wait(config.daemon.worker_startup_deadline_secs)
        threads = cast('dict[str, threading.Thread]', vars(dependent_runtime)['_threads'])
        assert dependent_runtime.fatal_reason is None
        assert threads['lane:healthy'].is_alive()
        assert threads['lane:remote'].is_alive()

        source_session = _start_source_runtime(
            config,
            site_spec,
            source_assignment,
            grants,
        )
        assert source_session._endpoint == endpoint
        _assert_initial_endpoint_delivery(
            dependent_runtime,
            provider_state,
            invoked,
            timeout=config.daemon.worker_startup_deadline_secs,
        )
    finally:
        _release_failed_source_runtime(
            dependent_runtime,
            (dependent_parent, dependent_child),
            (dependent_parent_socket, dependent_child_socket),
            (drain_stop, drain_thread),
            (() if source_session is None else (source_session._close,)),
        )


def test_join_after_bootstrap_is_not_refused(tmp_path: Path) -> None:
    path = _source_database_path(tmp_path)
    with _open_coordination_source(path) as source:
        source.initialize(41, incarnation='incarnation-1')
        fanout = EventFanout()
        coordinator = JoinCoordinator(state=source, fanout=fanout, source_generation='source-1')
        surface = worker_module._SourceJoinSurface(
            bootstrap_retry_secs=0.05,
            coordinator=coordinator,
            poller=cast('SourcePoller', SimpleNamespace(event_channel_lost=Mock())),
            state=source,
            inventory={
                'local': ActiveSourceGroupAssignment(
                    source='events', group='local', group_generation=7, start_mode='initialize'
                )
            },
            strict_buffer=None,
        )
        observation = surface.observe_group(
            GroupBootstrapRequest(
                source='events', group='local', group_generation=7, start_mode='initialize'
            )
        )
        assert isinstance(observation, GroupBootstrapObservation)
        assert source.group_policies()[1] == ()
        with pytest.raises(JoinProtocolError, match='durable group generation'):
            surface.request(
                JoinRequest(
                    JoinAttemptIdentity(
                        site='main',
                        source='events',
                        group='local',
                        join_kind='eager',
                        group_generation=7,
                        attempt_token=os.urandom(16).hex(),
                    )
                ),
                GroupChannel(group='local', ordering='eager', max_events=20, max_bytes=262_144),
            )
        _bootstrap_coordination_group(path)
        target = surface.request(
            JoinRequest(
                JoinAttemptIdentity(
                    site='main',
                    source='events',
                    group='local',
                    join_kind='eager',
                    group_generation=7,
                    attempt_token=os.urandom(16).hex(),
                )
            ),
            GroupChannel(group='local', ordering='eager', max_events=20, max_bytes=262_144),
        )
        assert isinstance(target, JoinTarget)
        assert target.identity.group_generation == 7
        assert surface._durable_generations == {'local': 7}
        assert surface._inventory['local'] == ActiveSourceGroupAssignment(
            source='events', group='local', group_generation=7, start_mode='resume'
        )


def test_lane_admission_budget_is_below_request_timeout() -> None:
    assert worker_module._LANE_OPERATOR_ADMISSION_SECS < control_module.REQUEST_TIMEOUT_SECS


def test_lane_cleanup_retains_original_worker_shutdown_deadline(
    tmp_path: Path,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    class _BlockingClose:
        def close(self) -> None:
            entered.set()
            assert release.wait(1.0)

    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, _resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        watchdog = component._watchdog
        token = watchdog.sample().registration_token
        assert isinstance(component, _LaneComponent)
        component._request_stop(shutdown_deadline_at=110.0)
        attempt_type = worker_module._PartialLaneAttempt
        attempt = attempt_type(_channel=cast('GroupChannel', _BlockingClose()))
        close_resources = cast(
            'Callable[[object, object], None]',
            vars(type(component))['_close_partial_attempt'],
        )
        operation = partial(close_resources, component, attempt)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(operation)
            try:
                assert entered.wait(1.0)
                snapshot = cache._snapshot()
            finally:
                release.set()
            future.result(timeout=2.0)

    assert snapshot._watchdogs[0].phase == 'shutdown'
    assert snapshot._watchdogs[0].deadline_at == 110.0
    assert watchdog.sample().registration_token == token


def test_lane_command_service_completes_queued_call() -> None:
    component = _unopened_group_component(_LaneComponent)
    result = object()

    def invoke(_resources: _LaneResources) -> object:
        return result

    call = component._mailbox._submit(invoke)

    component._service_calls(
        cast('_LaneResources', SimpleNamespace(_state=object(), _client=object()))
    )

    assert call._complete.is_set()
    assert call._result is result
    assert call._error is None
    assert component._mailbox._take() is None


def test_lane_control_saturation_stops_runtime_with_bounded_reason() -> None:
    lane_component, context, _operations, _control, _state = _recovery_construction_probe()
    component_type = _LaneComponent
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )
    peer_socket, sender_socket = socket.socketpair()
    peer = WorkerControl(peer_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    sender = WorkerControl(sender_socket, max_frame_bytes=131_072, send_deadline_secs=0.05)
    vars(lane_component)['_control'] = sender

    def _action() -> None:
        build_resources(lane_component, context)

    spec = WorkerSpec(
        worker_id='["lane","saturation"]',
        site='main',
        kind=WorkerKind._NAMED,
        name='pool',
        sources=(),
        groups=('remote',),
    )
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='lane-saturation-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: _ControlSendComponent(_action),
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )
    _saturate_worker_control_sender(sender)
    try:
        runtime.start()
        assert runtime.wait() == ()
        assert runtime.fatal_reason == 'worker-control-send-timeout'
    finally:
        runtime.request_stop('test-cleanup')
        runtime.wait()
        peer.close()
        sender.close()


def test_lane_disposition_retry_with_new_pid_after_restart(tmp_path: Path) -> None:
    path = _source_database_path(tmp_path)
    spec = build_worker_specs(_site((_group('local', worker='default'),)))[0]
    component = object.__new__(_LaneComponent)
    vars(component).update(
        _assignment=_local_assignment(spec, 'local', 'resume'),
        _group='local',
        _reload=threading.Event(),
        _spec=spec,
    )
    command = OperatorCommand(
        request_id='restart-retry',
        command='reset-group-to-latest',
        arguments={
            'site': 'main',
            'group': 'local',
            'reason': 'accept current frontier',
            'orphan_generation': 7,
            'reserved_generation': 8,
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with (
        _open_coordination_source(path) as source,
        open_group_on_source(
            path,
            writes=write_context_for(path),
            site='main',
            site_server_url='https://example.test',
            source='events',
            group='local',
            group_generation=7,
            ordering='eager',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='initialize',
            initial_cursor=41,
            initial_delivery_floor=41,
            initial_last_delivered_id=41,
        ) as group,
    ):
        source.initialize(41, incarnation='incarnation-1')
        group.park(
            reason='maintenance',
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
            expected_revision=0,
        )
        surface = _DurableCoordinationSurface(source)
        client_factory = cast('_LocalJoinClientFactory', vars(worker_module)['_LocalJoinClient'])
        client = client_factory(
            _DirectSourceComponent(surface),
            deadline_secs=30.0,
            source_name='events',
            group='local',
            stop=threading.Event(),
        )

        def call_first(invoke: Callable[[GroupState, object], object]) -> object:
            return invoke(group, client)

        vars(component)['_call'] = call_first
        first = component._execute_operator_command(command)
        assert first['group_generation'] == 8

    with open_group_on_source(
        path,
        writes=write_context_for(path),
        site='main',
        site_server_url='https://example.test',
        source='events',
        group='local',
        group_generation=8,
        ordering='eager',
        stream_fingerprint='stream',
        source_generation='source-1',
        verified_incarnation='incarnation-1',
        start_mode='resume',
    ) as reopened:

        def call_reopened(invoke: Callable[[GroupState, object], object]) -> object:
            return invoke(reopened, object())

        vars(component)['_call'] = call_reopened
        retry = replace(
            command,
            arguments={**command.arguments, 'orphan_generation': 8},
            identity=replace(command.identity, pid=789),
        )
        repeated = component._execute_operator_command(retry)
        assert repeated == first
        before_conflicts = reopened.snapshot()
        audit_before = reopened._database.connection.execute(
            'SELECT COUNT(*) FROM audit'
        ).fetchone()
        for changed_identity in (
            replace(retry.identity, username='other'),
            replace(retry.identity, local_uid=502),
        ):
            with pytest.raises(InvalidTransitionError, match='conflicting content'):
                component._execute_operator_command(replace(retry, identity=changed_identity))
        recorded_identity = reopened.terminal_disposition_identity(8)
        assert recorded_identity is not None
        semantic_conflict = component._disposition_request(
            replace(retry, arguments={**retry.arguments, 'reason': 'different recovery'}),
            recorded_identity[0],
        )
        with pytest.raises(InvalidTransitionError, match='conflicting content'):
            reopened.dispose(semantic_conflict.request('local', 8, before_conflicts.revision))
        assert reopened.snapshot() == before_conflicts
        assert reopened._database.connection.execute('SELECT COUNT(*) FROM audit').fetchone() == (
            audit_before
        )
        assert reopened.snapshot().group_generation == 8
        assert reopened._database.connection.execute(
            "SELECT COUNT(*) FROM audit WHERE command = 'reset-to-latest'"
        ).fetchone() == (1,)


def test_lane_does_not_join_when_durable_resume_halts() -> None:
    component, resources = _lifecycle_probe(
        'sync',
        durable_status='running',
        setup_failure=None,
        resume_status='callback-failed',
    )
    component_type = type(component)
    driver = cast(
        '_LaneLifecycleDriver',
        vars(component_type)['_drive_lane'],
    )
    stop = component._stop
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(driver, component, resources)
        try:
            assert resources._coordination._serviced.wait(1.0)
        finally:
            stop.set()
        future.result(timeout=1.0)

    assert 'resume' in resources._lane._operations
    assert resources._lane._joined == 0
    assert resources._lane._drains == 0


def test_lane_does_not_repeat_acknowledged_recovery_during_construction_retry() -> None:
    component, context, operations, control, _state = _recovery_construction_probe()
    component_type = _LaneComponent
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )

    for _attempt in range(2):
        with pytest.raises(_RecoveryConstructionReachedError):
            build_resources(component, context)

    assert len(control._messages) == 1
    assert operations.count('resolve') == 1
    assert operations.count('checkpoint-path') == 2


def test_lane_initial_stop_publishes_original_shutdown_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component, _context, now, cache = _recovery_owner_phase_probe(monkeypatch)
    component._request_stop(shutdown_deadline_at=200.0)
    component._run_body()
    now[0] = 136.0
    assert cache._snapshot()._watchdogs[0].phase == 'shutdown'
    assert cache._snapshot()._watchdogs[0].deadline_at == 200.0


@pytest.mark.parametrize('error_type', [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize('stop_fails', [False, True])
def test_lane_interrupt_survives_cleanup_failure_at_worker_boundary(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
    *,
    stop_fails: bool,
) -> None:
    order = ('channel', 'lane', 'state', 'reader', 'client')
    operations: list[str] = []
    failure = error_type('lane interrupted')
    cleanup_error = RuntimeError('lane cleanup failed')
    probes = {
        name: _CleanupProbe(
            name,
            operations,
            cleanup_error if name == 'lane' else None,
        )
        for name in order
    }
    component = _unopened_group_component(_LaneComponent)
    vars(probes['lane'])['watch_phase'] = component._watchdog.watch
    vars(component)['_lane'] = probes['lane']
    resources = SimpleNamespace(
        _channel=probes['channel'],
        _client=probes['client'],
        _lane=probes['lane'],
        _reader=probes['reader'],
        _state=probes['state'],
    )

    def service_calls(_resources: object) -> NoReturn:
        raise failure

    vars(component)['_service_calls'] = service_calls
    runtime_component = _LifecycleComponent(component, resources)
    site = _runtime_site(_group('local', worker='default'))
    forced: list[int] = []
    runtime = WorkerRuntime(
        spec=build_worker_specs(site)[0],
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: runtime_component,
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        force_exit=forced.append,
    )

    request_stop = runtime.request_stop

    def stop_then_fail(reason: str) -> None:
        request_stop(reason)
        raise RuntimeError('secondary stop failure')

    if stop_fails:
        monkeypatch.setattr(runtime, 'request_stop', stop_then_fail)

    with pytest.raises(error_type) as raised:
        runtime._component_main('lane:local', runtime_component)

    assert raised.value is failure
    if stop_fails:
        assert 'Worker stop also failed: RuntimeError' in failure.__notes__
    _assert_interrupted_worker_stopped(runtime, error_type)
    assert operations == list(order)
    assert vars(component)['_lane'] is None
    assert forced == []


@pytest.mark.parametrize('error_type', [KeyboardInterrupt, SystemExit])
def test_callback_interrupt_activates_monitor_containment(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
) -> None:
    site = _runtime_site(_group('local', worker='default'))
    spec = build_worker_specs(site)[0]
    operations: list[str] = []
    source = _Component('source:events', operations, wedge=True)
    release_failure = threading.Event()
    interrupted = threading.Event()
    forced = threading.Event()
    exits: list[tuple[int, str]] = []
    errors: list[BaseException | None] = []
    failure = error_type('callback interrupted')

    def fail() -> None:
        release_failure.wait()
        raise failure

    def observe_error(args: threading.ExceptHookArgs) -> None:
        errors.append(args.exc_value)
        interrupted.set()

    def force_exit(code: int) -> None:
        exits.append((code, threading.current_thread().name))
        forced.set()

    monkeypatch.setattr(threading, 'excepthook', observe_error)
    lane = _InterruptibleComponent(fail, release_failure.set)
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: source,
        lane_factory=lambda _name, _slots, _host: lane,
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=0.02,
        max_concurrent_catchup_readers=1,
        force_exit=force_exit,
    )
    try:
        runtime.start()
        assert source._started.wait(1.0)
        release_failure.set()
        assert interrupted.wait(1.0)
        assert errors == [failure]
        assert runtime.fatal_reason == f'lane:local thread failed: {error_type.__name__}'
        assert forced.wait(1.0)
        assert exits == [(1, 'sg-event-daemon:monitor')]
        assert runtime._stuck_threads == ('source:events',)
        assert 'close:source:events' not in operations
    finally:
        release_failure.set()
        source._release.set()
        runtime.request_stop('test cleanup')
        for thread in runtime._threads.values():
            thread.join(1.0)
        runtime._shutdown()


def test_lane_less_configured_group_is_visible_in_worker_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = _pending_group_runtime(tmp_path)
    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        runtime,
        control,
    ):
        update = _receive_typed_health(control, lambda item: len(item.groups) == 1)
        health = update.groups[0]
        assert health.group == 'local'
        assert health.parked
        assert health.join_phase == 'parked'
        assert health.status == 'parked'
        assert health.conditions == ('parked', 'replay-required')
        assert health.cursor == snapshot.cursor == 41
        assert health.audit_count == 3
        assert health.park_reason == 'configured group readd'
        assert all(item.kind != 'lane' for item in update.watchdogs)
        assert 'lane:local' not in update.worker.thread_names
        assert runtime.fatal_reason is None
        with _open_group_health_state(_source_database_path(tmp_path)) as state:
            state.mark_history_truncated(50, expected_revision=snapshot.revision)
        refreshed = _receive_typed_health(
            control,
            lambda item: bool(item.groups) and 'history-truncated' in item.groups[0].conditions,
        )
        assert refreshed.groups[0].audit_count == 3
        assert refreshed.groups[0].conditions == ('parked', 'history-truncated', 'replay-required')
        assert all(item.kind != 'lane' for item in refreshed.watchdogs)


def test_lane_never_starts_plugins_catchup_join_or_delivery_before_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = RuntimeError('conditional recovery failed closed')
    component, context, operations, _control, _state = _recovery_construction_probe(
        recovery_error=failure
    )
    _install_recovery_gate_probes(monkeypatch, component, operations)
    component_type = _LaneComponent
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )
    drive_lane = cast(
        '_LaneLifecycleDriver',
        vars(component_type)['_drive_lane'],
    )

    with pytest.raises(RuntimeError, match='conditional recovery failed closed'):
        build_resources(component, context)

    gated_operations = {
        'callbacks',
        'catchup',
        'delivery',
        'join',
        'plugins',
        'checkpoint-path',
        'resume',
        'setup',
    }
    assert gated_operations.isdisjoint(operations)
    assert operations[-1] == 'resolve'

    component, context, operations, _control, _state = _recovery_construction_probe(
        stop_at_construction=False
    )
    _install_recovery_gate_probes(monkeypatch, component, operations)
    resources = build_resources(component, context)
    drive_lane(component, resources)

    assert [item for item in operations if item in {*gated_operations, 'resolve'}] == [
        'resolve',
        'checkpoint-path',
        'plugins',
        'setup',
        'resume',
        'join',
        'catchup',
        'callbacks',
        'delivery',
    ]


def test_lane_operator_call_completed_at_deadline_returns_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component = _unopened_group_component(_LaneComponent)
    now = 0.0

    class ImmediateMailbox(_OwnerMailbox[object]):
        def _submit(self, invoke: Callable[[object], object]) -> _OwnerCall[object]:
            nonlocal now
            call = super()._submit(invoke)
            assert self._take() is call
            call._result = invoke(SimpleNamespace(_state=object(), _client=object()))
            call._complete.set()
            now = 1.0
            return call

    vars(component)['_mailbox'] = ImmediateMailbox('local')
    monkeypatch.setattr(worker_module, '_LANE_OPERATOR_ADMISSION_SECS', 0.05)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: now)

    assert component._call(lambda _state, _client: 'completed') == 'completed'
    assert now == 1.0


def test_lane_operator_call_returns_group_busy_while_lane_is_occupied(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(worker_module, '_LANE_OPERATOR_ADMISSION_SECS', 0.05)
    component = _unopened_group_component(_LaneComponent)
    runtime = WorkerRuntime(
        spec=component._spec,
        spawn_instance='busy-lane-test',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: component,
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        group_sources={'local': 'events'},
    )
    runtime._components['lane:local'] = component
    command = OperatorCommand(
        request_id='busy-park',
        command='park',
        arguments={'site': 'main', 'group': 'local', 'reason': 'maintenance'},
        identity=AuditIdentity.current(),
    )
    invoked: list[str] = []
    execute = component._execute_reasoned_operator

    def observe(
        state: GroupState, snapshot: GroupSnapshot, command: OperatorCommand
    ) -> GroupSnapshot:
        invoked.append(command.command)
        return execute(state, snapshot, command)

    monkeypatch.setattr(component, '_execute_reasoned_operator', observe)
    with _open_group_health_state(tmp_path / 'busy.db') as state:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(runtime._operator_response, command)
            try:
                assert component._wake.wait(1.0)
                response = future.result(timeout=1.0)
            finally:
                component._stop.set()
        assert not response.ok
        assert response.result == {'code': 'group-busy'}
        component._service_calls(
            cast('_LaneResources', SimpleNamespace(_state=state, _client=object()))
        )
        assert not state.snapshot().parked
        assert invoked == []


def test_lane_operator_call_started_before_deadline_returns_real_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component = _unopened_group_component(_LaneComponent)
    entered = threading.Event()
    deadline_checked = threading.Event()
    release = threading.Event()

    def clock() -> float:
        if entered.is_set():
            deadline_checked.set()
            return 1.0
        return 0.0

    def invoke(_state: GroupState, _client: object) -> str:
        entered.set()
        assert release.wait(2.0)
        return 'real-result'

    monkeypatch.setattr(worker_module, '_LANE_OPERATOR_ADMISSION_SECS', 0.05)
    monkeypatch.setattr(worker_module.time, 'monotonic', clock)
    resources = cast('_LaneResources', SimpleNamespace(_state=object(), _client=object()))
    with ThreadPoolExecutor(max_workers=2) as executor:
        caller = executor.submit(component._call, invoke)
        try:
            assert component._wake.wait(1.0)
            owner = executor.submit(component._service_calls, resources)
            assert entered.wait(1.0)
            assert deadline_checked.wait(1.0)
            assert not caller.done()
        finally:
            release.set()
            if not entered.is_set():
                component._stop.set()
        owner.result(timeout=1.0)
        assert caller.result(timeout=1.0) == 'real-result'


def test_lane_operator_call_withdrawn_on_shutdown_never_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module, '_LANE_OPERATOR_ADMISSION_SECS', 0.05)
    component = _unopened_group_component(_LaneComponent)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 0.0)
    operations: list[str] = []

    def invoke(_state: object, _client: object) -> None:
        operations.append('invoked')

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(component._call, invoke)
        try:
            assert component._wake.wait(1.0)
        finally:
            component._stop.set()
        with pytest.raises(worker_module._CooperativeComponentStopError) as raised:
            future.result(timeout=1.0)

    assert isinstance(raised.value.__cause__, ConnectionError)
    assert str(raised.value.__cause__) == 'lane component stopped during local call'
    component._service_calls(
        cast('_LaneResources', SimpleNamespace(_state=object(), _client=object()))
    )
    assert operations == []


def test_lane_operator_command_parks_through_owned_group_state(tmp_path: Path) -> None:
    component_type = _LaneComponent
    component = cast('_DispositionComponent', object.__new__(component_type))
    with open_group_on_source(
        tmp_path / 'local.db',
        writes=write_context_for(tmp_path / 'local.db'),
        site='main',
        site_server_url='https://example.test',
        source='events',
        group='local',
        group_generation=7,
        ordering='eager',
        stream_fingerprint='stream-1',
        source_generation='source-1',
        verified_incarnation='incarnation-1',
        start_mode='initialize',
        initial_cursor=41,
        initial_delivery_floor=41,
        initial_last_delivered_id=41,
    ) as state:

        def call(invoke: Callable[[GroupState, object], object]) -> object:
            return invoke(state, object())

        vars(component).update(
            {
                '_group': 'local',
                '_spec': SimpleNamespace(site='main'),
                '_call': call,
            }
        )
        result = component._execute_operator_command(
            OperatorCommand(
                request_id='park-1',
                command='park',
                arguments={
                    'site': 'main',
                    'group': 'local',
                    'reason': 'operator maintenance',
                },
                identity=AuditIdentity(local_uid=501, username='operator', pid=123),
            )
        )

        snapshot = state.snapshot()

    assert snapshot.parked
    assert result['site'] == 'main'
    assert result['group'] == 'local'
    assert result['group_generation'] == 7
    assert result['revision'] == 1
    assert result['parked'] is True


def test_lane_operator_reset_commits_reserved_generation(tmp_path: Path) -> None:
    site = _site((_group('local', worker='default'),))
    spec = build_worker_specs(site)[0]
    assignment = _local_assignment(spec, 'local', 'resume')
    component_type = _LaneComponent
    component = cast('_DispositionComponent', object.__new__(component_type))
    source_path = _source_database_path(tmp_path)
    group_path = _source_database_path(tmp_path)
    with (
        _open_coordination_source(source_path) as source,
        open_group_on_source(
            group_path,
            writes=write_context_for(group_path),
            site='main',
            site_server_url='https://example.test',
            source='events',
            group='local',
            group_generation=7,
            ordering='eager',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='initialize',
            initial_cursor=41,
            initial_delivery_floor=41,
            initial_last_delivered_id=41,
        ) as group,
    ):
        source.initialize(41, incarnation='incarnation-1')
        group.park(
            reason='operator maintenance',
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
            expected_revision=0,
        )
        surface = _DurableCoordinationSurface(source)
        client_factory = cast(
            '_LocalJoinClientFactory',
            vars(worker_module)['_LocalJoinClient'],
        )
        client = client_factory(
            _DirectSourceComponent(surface),
            deadline_secs=30.0,
            source_name='events',
            group='local',
            stop=threading.Event(),
        )

        def call(invoke: Callable[[GroupState, object], object]) -> object:
            return invoke(group, client)

        vars(component).update(
            {
                '_assignment': assignment,
                '_group': 'local',
                '_reload': threading.Event(),
                '_spec': SimpleNamespace(site='main'),
                '_call': call,
            }
        )
        command = OperatorCommand(
            request_id='reset-active-1',
            command='reset-group-to-latest',
            arguments={
                'site': 'main',
                'group': 'local',
                'reason': 'accept current frontier',
                'orphan_generation': 7,
                'reserved_generation': 8,
            },
            identity=AuditIdentity(local_uid=501, username='operator', pid=123),
        )
        before = group.snapshot()
        probe = replace(command, arguments={**command.arguments, 'reserved_generation': 7})
        assert component._execute_operator_command(probe) == {'disposition_required': True}
        assert group.snapshot() == before
        result = component._execute_operator_command(command)
        assert group._database.connection.execute(
            "SELECT operator_label FROM audit WHERE command = 'reset-to-latest'"
        ).fetchone() == ('operator uid=501',)
        group.unpark(
            reason='resume after disposition',
            identity=AuditIdentity.current(),
            wall_clock_time=2.0,
            expected_revision=group.snapshot().revision,
        )
        first_audit_count = group._database.connection.execute(
            'SELECT COUNT(*) FROM audit'
        ).fetchone()
        retry = replace(
            command,
            arguments={
                **command.arguments,
                'orphan_generation': 8,
                'reserved_generation': 8,
            },
            identity=replace(command.identity, pid=789),
        )
        assert component._execute_operator_command(retry)['group_generation'] == 8
        assert group._database.connection.execute('SELECT COUNT(*) FROM audit').fetchone() == (
            first_audit_count
        )
        different = replace(retry, arguments={**retry.arguments, 'reason': 'new disposition'})
        with pytest.raises(InvalidTransitionError, match='parked or orphaned'):
            component._execute_operator_command(different)

        snapshot = group.snapshot()
        identity = group.terminal_disposition_identity(8)
        source_completed = source._database.connection.execute(
            "SELECT group_generation FROM groups WHERE group_name = 'local'"
        ).fetchone() == (8,)
        updated = cast('_LaneComponent', component)._assignment
        assert updated.group_assignments == (
            DeliveryGroupAssignment(
                source='events',
                group='local',
                group_generation=8,
                start_mode='resume',
            ),
        )
        group.park(
            reason='subsequent maintenance',
            identity=AuditIdentity.current(),
            wall_clock_time=3.0,
            expected_revision=snapshot.revision,
        )
        intervening = replace(
            retry,
            request_id='intervening-reset',
            arguments={
                **retry.arguments,
                'reason': 'subsequent recovery',
                'reserved_generation': 9,
            },
        )
        assert component._execute_operator_command(intervening)['group_generation'] == 9
        after_intervening = group.snapshot()
        audit_after_intervening = group._database.connection.execute(
            'SELECT COUNT(*) FROM audit'
        ).fetchone()
        with pytest.raises(InvalidTransitionError, match='intervening disposition'):
            component._execute_operator_command(retry)
        assert group.snapshot() == after_intervening
        assert group._database.connection.execute('SELECT COUNT(*) FROM audit').fetchone() == (
            audit_after_intervening
        )

    assert snapshot.group_generation == 8
    assert not snapshot.parked
    assert identity is not None
    assert source_completed
    assert control_module._encode_response(
        control_module._ControlResponse(
            _version=1,
            _request_id=command.request_id,
            _ok=True,
            _result=CommandResult(
                command=command.command,
                request_id=command.request_id,
                ok=True,
                message='accepted',
                result=result,
            ).result,
            _error=None,
        )
    )[4:] == (
        b'{"version":1,"request_id":"reset-active-1","ok":true,"result":'
        b'{"site":"main","source":"events","group":"local","group_generation":8,'
        b'"revision":2,"status":"parked","conditions":["parked"],"cursor":41,'
        b'"parked":true},"error":null}'
    )
    assert vars(component)['_reload'].is_set()


def test_lane_operator_restart_uses_current_event_registration(tmp_path: Path) -> None:
    actions: list[str] = []
    group = _group('local', worker='default', ordering='eager')
    site = _runtime_site(group)
    provider_state = _RestartProviderState()
    provider_state._publish(42)

    def callback(label: str) -> Callable[[EventContext], None]:
        def invoke(_context: EventContext) -> None:
            actions.append(label)

        return invoke

    def identity(ordinal: int, callback_id: str) -> CallbackIdentity:
        return CallbackIdentity(
            ordinal=ordinal,
            plugin_id='restart',
            callback_id=callback_id,
            execution='sync',
            filter_digest=f'filter-{callback_id}',
            config_digest='empty-config',
            secret_refs=(),
            credential_name='source',
            credential_fingerprint='source:daemon',
        )

    old_manifest = CallbackManifest(
        callbacks=(identity(0, 'old-first'), identity(1, 'old-second')),
    )
    current_callbacks = (
        CallbackSpec(
            id='current-first',
            callback=callback('current-first'),
            events=None,
            identity=identity(0, 'current-first'),
        ),
        CallbackSpec(
            id='ignored',
            callback=callback('ignored'),
            events=normalize_filter({'Shotgun_Ignore': None}),
            identity=identity(1, 'ignored'),
        ),
        CallbackSpec(
            id='current-second',
            callback=callback('current-second'),
            events=normalize_filter({'Shotgun_Test': None}),
            identity=identity(2, 'current-second'),
        ),
    )
    plugin = PluginSpec(
        id='restart',
        module_name='tests.restart_current_registration',
        credentials='source',
        config=MappingProxyType({}),
        secrets=(),
        setup=None,
        callbacks=current_callbacks,
    )
    current_manifest = CallbackManifest(
        callbacks=(
            current_callbacks[0].identity,
            replace(current_callbacks[2].identity, ordinal=1),
        )
    )
    with _open_group_health_state(_source_database_path(tmp_path)) as state:
        started = state.start_event(42, old_manifest, expected_revision=0)
        first = state.begin_callback(0, expected_revision=started.revision)
        done = state.callback_done(0, expected_revision=first.revision)
        state.begin_callback(1, expected_revision=done.revision)
        watchdog = ComponentWatchdog(
            kind='lane',
            identity=group.name,
            worker_instance='worker-1',
            registration_token=_watchdog_token(),
            startup_deadline_secs=30.0,
        )
        watchdog.transition('idle', deadline_at=None)
        lane = GroupLane(
            state=state,
            site=site,
            group=group,
            provider=_restart_provider(provider_state),
            plugins=(plugin,),
            worker_instance='worker-1',
            health_interval_secs=0.01,
            watchdog=watchdog,
        )
        assert lane.setup() is None
        mismatch = lane.resume()
        assert mismatch.status == 'shape-mismatch'
        assert mismatch.in_flight_event_id == 42
        assert mismatch.callback_marks == (
            (0, 'restart', 'old-first', 'done', 1),
            (1, 'restart', 'old-second', 'pending', 1),
        )
        component_type = _LaneComponent
        component = cast('_DispositionComponent', object.__new__(component_type))

        def call(invoke: Callable[[GroupState, object], object]) -> object:
            return invoke(state, object())

        vars(component).update(
            {
                '_group': group.name,
                '_lane': lane,
                '_spec': SimpleNamespace(site=site.name),
                '_call': call,
            }
        )
        result = component._execute_operator_command(
            OperatorCommand(
                request_id='restart-event-1',
                command='restart-event',
                arguments={
                    'site': site.name,
                    'group': group.name,
                    'reason': 'activate current callback registration',
                },
                identity=AuditIdentity(local_uid=501, username='operator', pid=123),
            )
        )
        restarted = state.snapshot()
        completed = lane.resume()
        records = state._audit_records()
        lane.close()

    assert result['revision'] == restarted.revision
    assert restarted.status == 'running'
    assert restarted.in_flight_event_id == 42
    assert restarted.manifest == current_manifest
    assert restarted.callback_marks == (
        (0, 'restart', 'current-first', 'pending', 0),
        (1, 'restart', 'current-second', 'pending', 0),
    )
    assert actions == ['current-first', 'current-second']
    assert completed.cursor == 42
    assert completed.in_flight_event_id is None
    assert tuple(record.command for record in records) == ('bootstrap', 'restart-event')


def test_lane_refresh_service_returns_final_mutation_snapshot(tmp_path: Path) -> None:
    path = tmp_path / 'refresh-mutation.db'
    with _open_group_health_state(path) as state:
        _bind_worker_range(state, 'refresh-range', 42)
        connection = state._database.connection
        with state._writes.transaction(connection):
            connection.execute(
                "UPDATE discovery_ranges SET range_state = 'closed', close_generation = 1, "
                "source_revision = source_revision + 1 WHERE obligation_id = 'refresh-range'"
            )
            connection.execute(BUMP_INDEX_REVISION)
            connection.execute(BUMP_RANGE_REVISION)
        coordinator = LaneRefresh(
            lane=cast('GroupLane', _CoordinationLane(state)),
            limiter=_CatchupLimiter(1),
            reader=cast('CatchupReader', _CoordinationReader()),
            state=state,
        )
        assert state.snapshot().obligation_count == 1

        result = coordinator._service()

        assert result == state.snapshot()
        assert result is not None
        assert result.obligation_count == 0


def test_lane_reload_rebuilds_resources(tmp_path: Path) -> None:
    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, _cache, resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        reload_requested = component._reload
        stop = component._stop
        build_count = 0

        def build_resources(_context: object) -> object:
            nonlocal build_count
            build_count += 1
            if build_count == 2:
                stop.set()
            return resources

        def drive_lane(_resources: object) -> None:
            reload_requested.set()

        vars(component).update(
            _build_resources=build_resources,
            _configuration=object,
            _drive_lane=drive_lane,
        )
        run = cast('Callable[[], None]', vars(type(component))['_run'].__get__(component))
        run()

    assert build_count == 2


def test_lane_resolves_directive_before_checkpoint_construction() -> None:
    component, context, operations, _control, _state = _recovery_construction_probe()
    component_type = _LaneComponent
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )

    with pytest.raises(_RecoveryConstructionReachedError):
        build_resources(component, context)

    ordered_boundary = [
        item
        for item in operations
        if item in {'client', 'observe', 'validate', 'open', 'resolve', 'ready', 'checkpoint-path'}
    ]
    assert ordered_boundary == [
        'client',
        'observe',
        'validate',
        'open',
        'resolve',
        'ready',
        'checkpoint-path',
    ]


def test_lane_resumes_durable_event_after_setup_before_join() -> None:
    component, resources = _lifecycle_probe(
        'sync',
        durable_status='running',
        setup_failure=None,
    )
    component_type = type(component)
    driver = cast(
        '_LaneLifecycleDriver',
        vars(component_type)['_drive_lane'],
    )
    stop = component._stop
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(driver, component, resources)
        try:
            for _ in range(1_000):
                if resources._lane._joined:
                    break
                time.sleep(0.001)
        finally:
            stop.set()
        future.result(timeout=1.0)

    assert resources._lane._operations[:3] == ['setup', 'resume', 'join']
    assert resources._lane._operations.count('resume') == 1
    assert resources._lane._joined == 1


@pytest.mark.parametrize('command', ['replay', 'reset-group-to-latest'])
def test_lane_run_disposition_detaches_and_reregisters_at_the_source(
    tmp_path: Path, command: str
) -> None:
    path = _source_database_path(tmp_path)
    with _open_coordination_source(path) as source:
        source.initialize(41, incarnation='incarnation-1')
        _bootstrap_coordination_group(path, ordering='strict')
        strict = StrictBuffer(max_events=20, max_bytes=262_144, group_holdbacks={'local': 300.0})
        fanout = EventFanout()
        coordinator = JoinCoordinator(
            state=source, fanout=fanout, source_generation='source-1', strict_retention=strict
        )
        detached: list[tuple[str, str]] = []

        def detach(group: str, *, reason: str) -> None:
            detached.append((group, reason))
            fanout.detach(group, reason=reason)
            strict.detach_group(group, reason=reason)

        surface = worker_module._SourceJoinSurface(
            bootstrap_retry_secs=0.05,
            coordinator=coordinator,
            poller=cast('SourcePoller', SimpleNamespace(event_channel_lost=detach)),
            state=source,
            inventory={
                'local': ActiveSourceGroupAssignment(
                    source='events', group='local', group_generation=7, start_mode='resume'
                )
            },
            strict_buffer=strict,
        )
        channel = GroupChannel(group='local', ordering='strict', max_events=20, max_bytes=262_144)
        attempt = JoinAttemptIdentity(
            site='main',
            source='events',
            group='local',
            join_kind='strict',
            group_generation=7,
            attempt_token=os.urandom(16).hex(),
        )
        target = surface.request(JoinRequest(attempt), channel)
        assert isinstance(target, JoinTarget)
        assert isinstance(surface.confirm(JoinConfirm(target.identity)), Attached)
        assert channel.status.attached
        with open_group_on_source(
            path,
            writes=write_context_for(path),
            site='main',
            site_server_url='https://example.test',
            source='events',
            group='local',
            group_generation=7,
            ordering='strict',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='resume',
            strict_holdback_secs=300.0,
        ) as group:
            group.park(
                reason='maintenance',
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
                expected_revision=group.snapshot().revision,
            )
            component = object.__new__(_LaneComponent)
            spec = build_worker_specs(_site((_group('local', worker='default'),)))[0]

            def call(invoke: Callable[[GroupState, object], object]) -> object:
                return invoke(group, object())

            vars(component).update(
                _assignment=_local_assignment(spec, 'local', 'resume'),
                _group='local',
                _reload=threading.Event(),
                _spec=spec,
                _call=call,
            )
            arguments: dict[str, object] = {
                'site': 'main',
                'group': 'local',
                'reason': 'replace generation',
                'orphan_generation': 7,
                'reserved_generation': 8,
            }
            if command == 'replay':
                arguments['from_event_id'] = 41
                arguments['acknowledge_truncation'] = False
            result = component._execute_operator_command(
                OperatorCommand(
                    request_id='replace-generation',
                    command=command,
                    arguments=arguments,
                    identity=AuditIdentity.current(),
                )
            )
            assert result['group_generation'] == 8
            with pytest.raises(JoinProtocolError, match='source inventory'):
                surface.request(
                    JoinRequest(replace(attempt, attempt_token=os.urandom(16).hex())), channel
                )
            assert not channel.status.attached
            assert detached == [('local', 'operator-disposition')]
            with pytest.raises(JoinProtocolError):
                surface.confirm(JoinConfirm(target.identity))
            current_assignment = surface._inventory['local']
            assert isinstance(current_assignment, ActiveSourceGroupAssignment)
            assert current_assignment.group_generation == 8
            watermark = strict._watermark('local')
            assert watermark._group_generation == 8
            assert watermark._delivery_floor == group.snapshot().delivery_floor
            assert watermark._last_delivered_id == group._bootstrap_state().last_delivered_id
            assert watermark._is_operator_replay is (command == 'replay')
            assert strict._detached_halts['local']
            fresh = surface.request(
                JoinRequest(
                    replace(attempt, group_generation=8, attempt_token=os.urandom(16).hex())
                ),
                GroupChannel(group='local', ordering='strict', max_events=20, max_bytes=262_144),
            )
            assert isinstance(fresh, JoinTarget)
            assert fresh.identity.group_generation == 8
            assert detached == [('local', 'operator-disposition')]


def test_live_idle_snapshot_handoff_is_limited_to_already_joined_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 100.0)
    component, resources = _lifecycle_probe(
        'sync',
        durable_status='running',
        setup_failure=None,
    )
    snapshots: list[object] = []
    handed_off: list[object | None] = []
    stop = component._stop

    def snapshot() -> object:
        current = SimpleNamespace(status='running')
        snapshots.append(current)
        return current

    def drain(
        _channel: object, *, idle_snapshot: object | None = None, deadline_at: float = float('inf')
    ) -> object:
        del deadline_at
        handed_off.append(idle_snapshot)
        if len(handed_off) == 3:
            stop.set()
        return SimpleNamespace(status='running')

    monkeypatch.setattr(resources._state, 'snapshot', snapshot)
    monkeypatch.setattr(resources._lane, 'drain_live', drain)
    drive = cast('_LaneLifecycleDriver', vars(type(component))['_drive_lane'])
    drive(component, resources)
    assert len(snapshots) == 5
    assert handed_off == [None, snapshots[3], snapshots[4]]
    assert resources._lane._operations.count('setup') == 1
    assert resources._lane._operations.count('resume') == 1
    assert resources._lane._operations.count('join') == 1


def test_local_source_initialization_returns_typed_pending(tmp_path: Path) -> None:
    source_path = _source_database_path(tmp_path)
    with _open_coordination_source(source_path) as source_state:
        surface = worker_module._SourceJoinSurface(
            bootstrap_retry_secs=0.05,
            coordinator=cast('JoinCoordinator', SimpleNamespace(group_died=Mock())),
            poller=cast('SourcePoller', SimpleNamespace(event_channel_lost=Mock())),
            inventory={
                'local': ActiveSourceGroupAssignment(
                    source='events',
                    group='local',
                    group_generation=7,
                    start_mode='initialize',
                )
            },
            state=source_state,
            strict_buffer=None,
        )
        local_factory = cast(
            '_LocalJoinClientFactory',
            vars(worker_module)['_LocalJoinClient'],
        )
        client = local_factory(
            _DirectSourceComponent(cast('_DurableCoordinationSurface', surface)),
            deadline_secs=30.0,
            source_name='events',
            group='local',
            stop=threading.Event(),
        )
        observe_group = cast(
            'Callable[[object, GroupBootstrapRequest], object]',
            vars(type(client))['observe_group'],
        )

        pending = observe_group(
            client,
            GroupBootstrapRequest(
                source='events',
                group='local',
                group_generation=7,
                start_mode='initialize',
            ),
        )

    pending_type = cast('type[object]', vars(worker_module)['GroupBootstrapPending'])
    assert isinstance(pending, pending_type)
    assert vars(pending) == {
        'source': 'events',
        'group': 'local',
        'group_generation': 7,
        'retry_after_secs': 0.05,
    }


def test_monitor_blocked_by_native_work_fails_closed() -> None:
    spec = build_worker_specs(_site(()))[0]
    component = _Component('source', [])
    forced: list[int] = []
    block_monitor = threading.Event()
    monitor_entered = threading.Event()

    @contextmanager
    def monitor_gate() -> Generator[None]:
        monitor_entered.set()
        block_monitor.wait()
        yield

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=0.01,
        max_concurrent_catchup_readers=1,
        force_exit=forced.append,
        monitor_gate=monitor_gate,
    )
    runtime.start()
    try:
        assert monitor_entered.wait(1.0)
        runtime.request_stop('control-eof')
        time.sleep(0.03)
        assert forced == []
        assert runtime._resources_open
    finally:
        block_monitor.set()
        component._release.set()
        runtime.request_stop('test-cleanup')
        assert runtime._shutdown() == ()


def test_monitor_control_eof_broken_pipe_preserves_hard_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _monitor_control_runtime(tmp_path, monkeypatch) as (runtime, parent, _child, component):
        forced: list[int] = []
        monkeypatch.setattr(component, '_wedge', True)
        monkeypatch.setattr(runtime, '_force_exit', forced.append)
        monkeypatch.setattr(runtime, '_shutdown_deadline_secs', 0.02)
        runtime.start()
        assert component._started.wait(1.0)
        _receive_type(parent, HealthUpdate)
        parent.close()
        monitor = vars(runtime)['_monitor_thread']
        monitor.join(1.0)

        assert not monitor.is_alive()
        assert forced == [1]
        assert runtime.fatal_reason is None
        assert runtime._stuck_threads == ('source:events',)
        assert runtime._resources_open
        assert vars(runtime)['_stop_reason'] == 'control-eof'

    assert component._finished.is_set()
    assert not runtime._resources_open
    assert component._operations[-1] == 'close:source:events'


@pytest.mark.parametrize('prior_failure', [False, True])
def test_monitor_peer_close_between_poll_and_health_send_preserves_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prior_failure: bool
) -> None:
    with _monitor_control_runtime(tmp_path, monkeypatch) as (runtime, parent, child, component):
        receive = child.receive
        send = child.send
        events: list[str] = []
        original_deadline: list[float | None] = []

        def close_after_empty_poll(*, timeout: float) -> ControlMessage:
            try:
                return receive(timeout=timeout)
            except TimeoutError:
                events.append('poll-empty')
                if prior_failure:
                    runtime._component_failed('original-component-failure')
                    original_deadline.append(vars(runtime)['_shutdown_deadline_at'])
                parent.close()
                events.append('peer-closed')
                raise

        def observe_send(message: ControlMessage) -> None:
            try:
                send(message)
            except BrokenPipeError:
                events.append('send-broken-pipe')
                raise

        monkeypatch.setattr(child, 'receive', close_after_empty_poll)
        monkeypatch.setattr(child, 'send', observe_send)
        result = _run_worker_runtime_cli(monkeypatch, runtime)

        assert events[:3] == ['poll-empty', 'peer-closed', 'send-broken-pipe']
        assert result == int(prior_failure)
        assert runtime.fatal_reason == ('original-component-failure' if prior_failure else None)
        assert vars(runtime)['_stop_reason'] == (
            'original-component-failure' if prior_failure else 'control-eof'
        )
        if prior_failure:
            assert [vars(runtime)['_shutdown_deadline_at']] == original_deadline
        assert component._finished.is_set()
        assert not runtime._resources_open
        assert component._operations[-1] == 'close:source:events'
        assert all(not thread.is_alive() for _name, thread in runtime._owned_threads())


def test_monitor_reports_thread_exit_and_heartbeat_survives_source_outage() -> None:
    spec = build_worker_specs(_site(()))[0]
    operations: list[str] = []
    sources = {name: _Component(f'source:{name}', operations) for name in spec.sources}
    health: list[WorkerHealth] = []
    health_published = threading.Condition()

    def publish_health(update: WorkerHealth) -> None:
        with health_published:
            health.append(update)
            health_published.notify_all()

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=publish_health,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )
    runtime.start()
    try:
        assert all(source._started.wait(1.0) for source in sources.values())
        expected_outages = (('events', 'provider unavailable'),)
        runtime._report_source_outage('events', RuntimeError('provider unavailable'))
        with health_published:
            assert health_published.wait_for(
                lambda: len(health) >= 2 and health[-1].source_outages == expected_outages,
                timeout=1.0,
            )
        assert len(health) >= 2
        assert health[-1].monitor_alive
        assert health[-1].source_outages == expected_outages
        sources['events']._release.set()
        for _ in range(100):
            if runtime.fatal_reason is not None:
                break
            time.sleep(0.001)
        assert runtime.fatal_reason == 'source:events thread exited'
    finally:
        for source in sources.values():
            source._release.set()
        runtime.request_stop('test')
        assert runtime._shutdown() == ()


def test_monitor_requested_clean_return_remains_nonfatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = build_worker_specs(_site(()))[0]
    operations: list[str] = []
    components = {name: _Component(f'source:{name}', operations) for name in spec.sources}
    runtime: WorkerRuntime

    @contextmanager
    def request_stop() -> Generator[None]:
        runtime.request_stop('test-requested-stop')
        yield

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        monitor_gate=request_stop,
    )

    result = _run_worker_runtime_cli(monkeypatch, runtime)

    assert result == 0
    assert runtime.fatal_reason is None
    assert all(component._finished.is_set() for component in components.values())
    assert not runtime._resources_open


def test_monitor_unexpected_clean_return_is_worker_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = build_worker_specs(_site(()))[0]
    operations: list[str] = []
    components = {name: _Component(f'source:{name}', operations) for name in spec.sources}
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )
    monkeypatch.setattr(runtime, '_monitor', lambda: None)

    result = _run_worker_runtime_cli(monkeypatch, runtime)

    assert result == 1
    assert runtime.fatal_reason == 'monitor thread exited'
    assert all(component._finished.is_set() for component in components.values())
    assert not runtime._resources_open


def test_owner_calls_preserve_callback_result_types() -> None:
    class ImmediateMailbox(_OwnerMailbox[object]):
        def __init__(self, owner: object) -> None:
            super().__init__('test')
            self.owner = owner

        def _submit(self, invoke: Callable[[object], object]) -> _OwnerCall[object]:
            call = super()._submit(invoke)
            call._result = call._invoke(self.owner)
            call._complete.set()
            return call

    def prepare(component: object, owner: object) -> None:
        vars(component).update(
            {
                '_mailbox': ImmediateMailbox(owner),
                '_stop': threading.Event(),
            }
        )

    ready = GroupBootstrapRequest(
        source='events', group='local', group_generation=7, start_mode='resume'
    )
    source = object.__new__(_SourceComponent)
    prepare(source, object())
    source_result = assert_type(
        source._call(lambda _surface: ready, deadline_secs=30.0, stop=threading.Event()),
        GroupBootstrapRequest,
    )

    def acknowledge(_state: GroupState, _client: object) -> GroupBootstrapRequest | None:
        return None

    lane = object.__new__(_LaneComponent)
    prepare(lane, SimpleNamespace(_state=object(), _client=object()))
    observation = assert_type(lane._call(acknowledge), GroupBootstrapRequest | None)
    assert source_result is ready
    assert observation is None


def test_packed_worker_prepares_all_state_and_socket_parents(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    shared_root = Path(os.sep, 'tmp')
    monkeypatch.setattr(
        platform_module.tempfile,
        'gettempdir',
        lambda: os.fspath(shared_root),
    )
    with tempfile.TemporaryDirectory(prefix='sged-', dir=shared_root) as directory:
        runtime_root = Path(directory)
        monkeypatch.setenv('XDG_RUNTIME_DIR', directory)
        state_dir = tmp_path / f'state-{"x" * 64}'
        site = _site((_group('local', worker='default'),))
        config = _runtime_config(state_dir, site)
        spec = build_worker_specs(site)[0]
        sources_dir = state_dir / 'sites' / site.name / 'sources'
        fallback_parents = {
            candidate.parent
            for source in spec.sources
            if (
                candidate := local_socket_path_candidate(
                    sources_dir,
                    f'{source.replace("_", "-")}-events',
                    (spec.site, source),
                )
            ).parent
            != sources_dir
        }
        expected_fallback = runtime_root / f'sg-event-daemon-{os.geteuid()}'
        assert fallback_parents == {expected_fallback}
        prepare = cast(
            'Callable[[Config, WorkerSpec], None]',
            vars(worker_module)['_prepare_worker_directories'],
        )

        prepare(config, spec)

        prepared_directories = (
            state_dir,
            state_dir / 'sites',
            state_dir / 'sites' / site.name,
            sources_dir,
            state_dir / 'sites' / site.name / 'groups',
            *sorted(fallback_parents),
        )
        assert all(directory.stat().st_mode & 0o777 == 0o700 for directory in prepared_directories)
        previous_umask = os.umask(0o177)
        try:
            validated = tuple(
                ensure_secure_state_dir(directory) for directory in prepared_directories
            )
        finally:
            os.umask(previous_umask)
        assert validated == prepared_directories
        assert all(directory.stat().st_mode & 0o777 == 0o700 for directory in prepared_directories)


def test_parked_lane_skips_join_and_callbacks_but_services_coordination() -> None:
    component, resources = _lifecycle_probe(
        'sync',
        durable_status='parked',
        setup_failure=None,
    )
    component_type = type(component)
    driver = cast(
        '_LaneLifecycleDriver',
        vars(component_type)['_drive_lane'],
    )
    stop = component._stop
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(driver, component, resources)
        try:
            assert resources._coordination._serviced.wait(1.0)
            assert not future.done()
            assert resources._lane._joined == 0
            assert resources._lane._drains == 0
            assert resources._coordination._calls
            assert not any(resources._coordination._calls)
        finally:
            stop.set()
        future.result(timeout=1.0)


def test_partial_startup_closes_lane_resources_on_owner_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = build_worker_specs(_site((_group('local', worker='default'),)))[0]
    component, resources = _lifecycle_probe(
        'sync',
        durable_status='running',
        setup_failure=None,
    )
    closed: list[tuple[str, str]] = []
    for name in ('channel', 'lane', 'state', 'reader', 'client'):
        resource = vars(resources)[f'_{name}']
        original_close = cast('Callable[[], None]', resource.close)

        def close(name: str = name, original_close: Callable[[], None] = original_close) -> None:
            closed.append((name, threading.current_thread().name))
            original_close()

        monkeypatch.setattr(resource, 'close', close)
    lane = _LifecycleComponent(component, resources)
    sources = {name: _Component(name, []) for name in spec.sources}
    original_start = threading.Thread.start

    def start(thread: threading.Thread) -> None:
        if thread.name == 'sg-event-daemon:operator-executor':
            assert resources._coordination._serviced.wait(1.0)
            raise RuntimeError('operator startup failed')
        original_start(thread)

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='partial-lifecycle',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda _name, _slots, _host: lane,
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        force_exit=lambda _code: pytest.fail('caller invoked hard exit'),
    )
    monkeypatch.setattr(threading.Thread, 'start', start)
    try:
        with pytest.raises(RuntimeError, match='operator startup failed'):
            runtime.start()
        assert closed == [
            (name, 'sg-event-daemon:lane:local')
            for name in ('channel', 'lane', 'state', 'reader', 'client')
        ]
        assert not runtime._resources_open
    finally:
        runtime.request_stop('test-cleanup')
        assert runtime._shutdown() == ()


def test_partial_startup_joins_all_started_owners_with_one_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = build_worker_specs(_site((_group('local', worker='default'),)))[0]
    operations: list[str] = []
    components = {
        name: _Component(name, operations, wedge=name == 'source:audit')
        for name in ('source:audit', 'source:events', 'lane:local')
    }
    now = 0.0

    def clock() -> float:
        return now

    joins: list[tuple[str, float | None]] = []
    started: list[threading.Thread] = []
    forced: list[int] = []
    thread_type = threading.Thread

    class StartupThread(thread_type):
        def join(self, timeout: float | None = None) -> None:
            nonlocal now
            joins.append((self.name, timeout))
            super().join(timeout)
            if self.name == 'sg-event-daemon:source:audit' and timeout is not None:
                now += timeout

        def start(self) -> None:
            if self.name == 'sg-event-daemon:monitor':
                assert all(component._started.wait(1.0) for component in components.values())
                raise RuntimeError('monitor startup failed')
            super().start()
            started.append(self)

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='partial-stuck',
        source_factory=lambda name, _slots, _host: components[f'source:{name}'],
        lane_factory=lambda name, _slots, _host: components[f'lane:{name}'],
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=0.05,
        max_concurrent_catchup_readers=1,
        clock=clock,
        force_exit=forced.append,
    )
    monkeypatch.setattr(worker_module.threading, 'Thread', StartupThread)
    try:
        with pytest.raises(RuntimeError, match='monitor startup failed'):
            runtime.start()
        assert joins == [
            ('sg-event-daemon:source:audit', 0.05),
            ('sg-event-daemon:source:events', 0.0),
            ('sg-event-daemon:lane:local', 0.0),
            ('sg-event-daemon:operator-executor', 0.0),
        ]
        assert runtime._stuck_threads == ('source:audit',)
        assert runtime._resources_open
        assert 'close:source:audit' not in operations
        assert [item for item in operations if item.startswith('close:')] == [
            'close:lane:local',
            'close:source:events',
        ]
        assert runtime._shutdown() == ('source:audit',)
        assert all(timeout == 0.0 for _name, timeout in joins[4:])
        assert forced == []
    finally:
        for component in components.values():
            component._release.set()
        for thread in started:
            thread_type.join(thread, 1.0)
        assert runtime._shutdown() == ()
    assert not runtime._resources_open
    assert operations.count('close:source:events') == 1


def test_pending_group_detail_rebuilt_once_per_health_interval(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _pending_group_health_probe(tmp_path) as (component, cache, state):
        clock = SimpleNamespace(now=0.0)
        monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
        calls = 0
        record = state.pending_group_record

        def counted_record(group: str, *, limit: int) -> object:
            nonlocal calls
            calls += 1
            return record(group, limit=limit)

        monkeypatch.setattr(state, 'pending_group_record', counted_record)
        surface = cast('_SourceJoinSurface', SimpleNamespace(_state=state))
        component._publish_pending_groups(surface)
        first = cache._snapshot()._groups[0]
        watchdog_before = cache._snapshot()._watchdogs[0].heartbeat_at
        clock.now = 0.5
        component._watchdog.heartbeat()
        component._publish_pending_groups(surface)
        second = cache._snapshot()._groups[0]
        assert calls == 1
        assert first.heartbeat_at == 0.0
        assert second.heartbeat_at == 0.5
        assert cache._snapshot()._watchdogs[0].heartbeat_at > watchdog_before

        clock.now = 1.0
        component._publish_pending_groups(surface)
        assert calls == 2


def test_pending_group_detail_refreshed_after_local_mutation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _pending_group_health_probe(tmp_path) as (component, cache, state):
        clock = SimpleNamespace(now=0.0)
        monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
        calls = 0
        record = state.pending_group_record

        def counted_record(group: str, *, limit: int) -> object:
            nonlocal calls
            calls += 1
            return record(group, limit=limit)

        monkeypatch.setattr(state, 'pending_group_record', counted_record)
        surface = cast('_SourceJoinSurface', SimpleNamespace(_state=state))
        component._publish_pending_groups(surface)
        clock.now = 0.05
        component._publish_pending_groups(surface)
        assert calls == 1
        before = cache._snapshot()._groups[0]

        def dispose(_surface: _SourceJoinSurface) -> GroupSnapshot:
            with _open_group_health_state(_source_database_path(tmp_path)) as group:
                return group.dispose(
                    DispositionRequest(
                        command='replay',
                        group='local',
                        disposition_id='pending-health-replay',
                        reserved_generation=11,
                        expected_revision=group.snapshot().revision,
                        from_event_id=40,
                        acknowledge_truncation=False,
                        reason='restore configured group',
                        identity=AuditIdentity.current(),
                        wall_clock_time=3.0,
                    )
                )

        clock.now = 0.1
        call = component._mailbox._submit(dispose)

        def publish_health(
            _poller: SourcePoller,
            _server: EventServer,
            *,
            snapshot: SourceSnapshot | None = None,
        ) -> None:
            del snapshot

        monkeypatch.setattr(component, '_publish_health', publish_health)
        component._service_internal(
            cast('EventServer', _SourceHealthServer()),
            cast(
                '_SourceJoinSurface',
                SimpleNamespace(_state=state, _reconcile_groups=lambda: None),
            ),
            cast('SourcePoller', object()),
        )
        assert call._complete.is_set()
        assert call._error is None
        assert calls == 2
        after = cache._snapshot()._groups[0]
        assert after.heartbeat_at == 0.1
        assert after.audit_count == before.audit_count + 1
        assert state.group_snapshot('local').is_operator_replay

        clock.now = 0.2
        component._mark_source_health_dirty()
        component._publish_pending_groups(surface)
        assert calls == 3


@pytest.mark.parametrize('removal', ['pending-assignment', 'source-deregistration'])
def test_pending_group_removal_preserves_newer_lane_health(tmp_path: Path, removal: str) -> None:
    with _pending_group_health_probe(tmp_path) as (component, cache, state):
        surface = cast('_SourceJoinSurface', SimpleNamespace(_state=state))
        component._publish_pending_groups(surface)
        pending = cache._snapshot()._groups[0]
        lane_sample = WatchdogSample(
            registration_token=_watchdog_token(),
            transition_sequence=1,
            kind='lane',
            identity='local',
            worker_instance=pending.worker_instance,
            phase='startup',
            heartbeat_at=2.0,
            deadline_at=12.0,
        )
        lane = _watchdog_group_health(
            'local', source='events', worker_instance=pending.worker_instance
        )
        cache._register_watchdog(
            kind='lane',
            identity='local',
            worker_instance=lane_sample.worker_instance,
            token=lane_sample.registration_token,
            supplier=lambda: lane_sample,
        )
        cache._publish_group(lane, sample=lane_sample)
        before = cache._snapshot()
        assert before._groups[0].audit_count == lane.audit_count
        assert before._groups != (pending,)
        lane_watchdogs = tuple(watch for watch in before._watchdogs if watch.kind == 'lane')
        assert len(lane_watchdogs) == 1
        assert lane_watchdogs[0].group == 'local'
        if removal == 'pending-assignment':
            component._assignment = replace(component._assignment, source_group_assignments=())
            component._publish_pending_groups(surface)
        else:
            component._close()
        after = cache._snapshot()
        assert after._groups == before._groups
        assert tuple(watch for watch in after._watchdogs if watch.kind == 'lane') == lane_watchdogs


def test_pending_group_reports_callback_failure_kind(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _pending_group_health_probe(tmp_path) as (component, cache, state):
        record = state.pending_group_record

        def failed_record(group: str, *, limit: int) -> object:
            detail = record(group, limit=limit)
            return replace(
                detail,
                snapshot=replace(
                    detail.snapshot,
                    failure_code='callback-failed',
                    failure_text='callback unavailable',
                ),
            )

        monkeypatch.setattr(state, 'pending_group_record', failed_record)
        component._publish_pending_groups(
            cast('_SourceJoinSurface', SimpleNamespace(_state=state))
        )
        assert cache._snapshot()._groups[0].failure_kind == 'callback'


def test_pending_source_inventory_allows_orphan_observation_but_rejects_join(
    tmp_path: Path,
) -> None:
    source_path = _source_database_path(tmp_path)
    with _open_coordination_source(source_path) as source_state:
        source_state.initialize(41, incarnation='incarnation-1')
        _bootstrap_coordination_group(source_path)
        with open_group_on_source(
            source_path,
            writes=write_context_for(source_path),
            site='main',
            site_server_url='https://example.test',
            source='events',
            group='local',
            group_generation=7,
            ordering='eager',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='resume',
        ) as orphan:
            orphan_group(orphan)
        marker = source_state.snapshot().replay_markers[0]
        surface = worker_module._SourceJoinSurface(
            bootstrap_retry_secs=0.05,
            coordinator=cast('JoinCoordinator', SimpleNamespace(group_died=Mock())),
            poller=cast('SourcePoller', SimpleNamespace(event_channel_lost=Mock())),
            inventory={
                'local': PendingDispositionSourceGroupAssignment(
                    source='events',
                    group='local',
                    orphan_generation=7,
                    reserved_generation=11,
                )
            },
            state=source_state,
            strict_buffer=None,
        )

        observation = surface.observe_group(
            GroupBootstrapRequest(
                source='events',
                group='local',
                group_generation=7,
                start_mode='resume',
            )
        )
        assert observation == GroupBootstrapObservation(
            source='events',
            group='local',
            group_generation=7,
            source_generation='source-1',
            stream_incarnation='incarnation-1',
            captured_frontier=41,
            source_successful_clock=0.0,
        )
        channel = GroupChannel(
            group='local',
            ordering='eager',
            max_events=1,
            max_bytes=1_024,
        )
        with pytest.raises(JoinProtocolError, match='forbids fanout'):
            surface.request(
                JoinRequest(
                    JoinAttemptIdentity(
                        site='main',
                        source='events',
                        group='local',
                        join_kind='eager',
                        group_generation=7,
                        attempt_token=f'pending-attempt-{marker.from_event_id}',
                    )
                ),
                channel,
            )

        assert source_state.snapshot().replay_markers == (marker,)
        assert channel.status.queued_count == 0


def test_post_close_scan_checks_retention_before_first_page() -> None:
    state = _RetentionCoordinationState()
    reader = _RetentionCoordinationReader((55,))
    lane = _RetentionCoordinationLane(state)
    coordinator = LaneRefresh(
        lane=cast('GroupLane', lane),
        limiter=_CatchupLimiter(1),
        reader=cast('CatchupReader', reader),
        state=cast('GroupState', state),
    )
    scan = cast(
        'Callable[[object, SourceProgress], None]',
        vars(type(coordinator))['_scan_obligations'],
    )

    coordinator.synchronize()
    scan(coordinator, _synchronization(60))

    assert state._truncated_floor == 55
    assert state._status == 'history-truncated'
    assert reader._ranges == []
    assert not lane._resolved


def test_post_close_scan_rechecks_retention_before_resolution() -> None:
    state = _RetentionCoordinationState()
    reader = _RetentionCoordinationReader(
        (1, 55),
        (Event(_runtime_event(51)),),
    )
    lane = _RetentionCoordinationLane(state)
    coordinator = LaneRefresh(
        lane=cast('GroupLane', lane),
        limiter=_CatchupLimiter(1),
        reader=cast('CatchupReader', reader),
        state=cast('GroupState', state),
    )
    scan = cast(
        'Callable[[object, SourceProgress], None]',
        vars(type(coordinator))['_scan_obligations'],
    )

    coordinator.synchronize()
    scan(coordinator, _synchronization(60))

    assert lane._delivered == [51]
    assert state._truncated_floor == 55
    assert state._status == 'history-truncated'
    assert not lane._resolved


def test_post_close_scans_share_worker_catchup_limiter_without_double_acquisition() -> None:
    state = _RetentionCoordinationState()
    reader = _RetentionCoordinationReader((1, 1))
    lane = _RetentionCoordinationLane(state)
    limiter = _CatchupLimiter(1)
    coordinator = LaneRefresh(
        lane=cast('GroupLane', lane),
        limiter=limiter,
        reader=cast('CatchupReader', reader),
        state=cast('GroupState', state),
    )
    scan = cast(
        'Callable[[object, SourceProgress], None]',
        vars(type(coordinator))['_scan_obligations'],
    )
    completed = threading.Event()
    started = threading.Event()
    original_scan = reader.scan_range

    def blocking_scan(low_id: int, high_id: int) -> Generator[Event, None, None]:
        started.set()
        yield from original_scan(low_id, high_id)

    reader.scan_range = blocking_scan

    coordinator.synchronize()
    with limiter._slot():
        thread = threading.Thread(
            target=lambda: (
                scan(coordinator, _synchronization(60)),
                completed.set(),
            )
        )
        thread.start()
        assert not started.wait(0.05)
        assert not completed.is_set()
    thread.join(1.0)

    assert not thread.is_alive()
    assert started.is_set()
    assert completed.is_set()
    assert lane._resolved


def test_preflight_covers_source_command_operator_record(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source_name = 'a' * 20
    group = replace(_group('g', worker='default'), source='b')
    site = _runtime_site(group)
    site = replace(
        site,
        sources=MappingProxyType(
            {
                source_name: replace(site.sources['events'], name=source_name),
                'b': replace(site.sources['events'], name='b'),
            }
        ),
        groups=MappingProxyType({'g': group}),
    )
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=((source_name, 'source-generation-1'), ('b', 'source-generation-1')),
        group_assignments=(
            DeliveryGroupAssignment(
                source='b', group='g', group_generation=7, start_mode='initialize'
            ),
        ),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='b', group='g', group_generation=7, start_mode='initialize'
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )
    updates: list[HealthUpdate] = []

    def capture(update: HealthUpdate) -> bytes:
        updates.append(update)
        return encode_control_message(update)

    monkeypatch.setattr(worker_module, 'encode_control_message', capture)
    parent_socket, child_socket = socket.socketpair()
    with parent_socket, child_socket:
        control = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
        build_worker_runtime(config, spec, assignment, control)
    assert len(updates) == 1
    maximum = updates[0]
    source_command = replace(
        maximum,
        watchdogs=(
            *maximum.watchdogs[:-1],
            replace(
                maximum.watchdogs[-1],
                source=source_name,
                group=None,
                operator_command='preflight-source-adoption',
            ),
        ),
    )
    assert len(encode_control_message(maximum)) >= len(encode_control_message(source_command))


def test_range_changed_makes_the_coordination_tick_due_now(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    wake = threading.Event()
    channel = _DeliveryFloorChannel(
        group='editorial',
        ordering='eager',
        max_events=1,
        max_bytes=262_144,
        delivery_floor=0,
    )
    channel._set_wake(wake)
    channel.activate(provisional=False)
    event = EventDelivery(EncodedEvent.from_event_frame(7, encode_event(Event(_runtime_event(7)))))
    assert channel.offer(event)
    assert channel.offer_hint(RangeChanged())
    assert wake.is_set()
    assert channel.status.queued_count == 1
    assert channel.drain() == (event,)

    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component_object, _cache, resources_object, lifecycle_object = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        component = component_object
        resources = cast('_LaneResources', resources_object)
        assert lifecycle_object is not None
        lifecycle = cast('_LaneLifecycle', lifecycle_object)
        lifecycle._channel = channel
        lifecycle._next_coordination_at = time.monotonic() + 60.0
        calls: list[str] = []

        def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
            del force
            del scan_obligations
            calls.append('tick')

        monkeypatch.setattr(resources._coordination, '_service', coordinate)
        component._service_due(resources, lifecycle)
        assert calls == ['tick']
        assert lifecycle._next_coordination_at > time.monotonic()
        component._service_due(resources, lifecycle)
        assert calls == ['tick']


@pytest.mark.parametrize('typed_health', [False, True])
@pytest.mark.parametrize('ready', [False, True])
def test_readiness_helpers_bound_elapsed_time_not_frame_count(
    monkeypatch: pytest.MonkeyPatch, *, typed_health: bool, ready: bool
) -> None:
    now = [0.0]
    received = [0]
    health = Mock(spec=HealthUpdate)
    endpoint = SourceEndpointReady('events', 'generation', '/runtime/events.sock')

    def receive(*, timeout: float) -> object:
        assert 0 < timeout <= 1.0
        now[0] += min(timeout, 0.025)
        received[0] += 1
        if not typed_health and ready and received[0] == 150:
            return endpoint
        return health

    monkeypatch.setattr(time, 'monotonic', lambda: now[0])
    control = cast('WorkerControl', SimpleNamespace(receive=receive))

    def await_ready() -> object:
        if typed_health:
            return _receive_typed_health(control, lambda _update: ready and received[0] == 150)
        return _receive_type(control, SourceEndpointReady)

    if ready:
        assert await_ready() is (health if typed_health else endpoint)
        assert received[0] == 150
    else:
        with pytest.raises(AssertionError, match='worker did not publish'):
            await_ready()
        assert now[0] == pytest.approx(10.0)


def test_rejoin_resets_the_coordination_due_time() -> None:
    component_object, resources_object = _lifecycle_probe(
        'sync', durable_status='running', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(_channel=resources._channel)
    lifecycle._next_coordination_at = time.monotonic() + 60.0
    assert component._join_lane(resources, lifecycle)
    assert lifecycle._next_coordination_at <= time.monotonic()


def test_remote_bootstrap_retry_does_not_hide_partial_cleanup_failure() -> None:
    operations: list[str] = []
    cleanup_error = RuntimeError('reader cleanup failed')
    client = _CleanupProbe('client', operations, None)
    reader = _CleanupProbe('reader', operations, cleanup_error)
    state = _CleanupProbe('state', operations, None)
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(component).update({'_group': 'remote', '_lane': None})
    context = SimpleNamespace(
        _assignment=SimpleNamespace(
            conditional_park=None,
            group_generation=7,
            start_mode='resume',
        ),
        _group=SimpleNamespace(source='events'),
    )
    retry_error_type = cast(
        'Callable[[str], RuntimeError]',
        vars(worker_module)['_RemoteBootstrapTransportError'],
    )
    retry_error = retry_error_type('construction interrupted')

    def build_client(_context: object) -> tuple[object, str]:
        return client, 'source-generation-1'

    def observe(_client: object, *, assignment: object) -> object:
        del assignment
        return SimpleNamespace(source_generation='source-generation-1')

    def open_state(
        _context: object,
        _observation: object,
        _reader: object,
    ) -> object:
        return state

    def provider_reader(_context: object) -> tuple[object, object]:
        return object(), reader

    def database_path() -> Path:
        raise retry_error

    def resolve(context: object, _state: object) -> object:
        return context

    vars(component).update(
        {
            '_build_client': build_client,
            '_observe_source': observe,
            '_open_group_state': open_state,
            '_provider_reader': provider_reader,
            '_database_path': database_path,
            '_resolve_conditional_park': resolve,
        }
    )
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )

    with pytest.raises(RuntimeError) as raised:
        build_resources(component, context)

    traceback = raised.value.__traceback__
    while traceback is not None and traceback.tb_next is not None:
        traceback = traceback.tb_next
    assert operations == ['state', 'reader', 'client']
    assert vars(component)['_lane'] is None
    assert raised.value is cleanup_error
    assert traceback is reader._traceback


def test_remote_coordination_cleanup_stops_server_after_client_close_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[threading.Event] = []
    event_type = threading.Event
    attempts: list[str] = []
    initiating = ValueError('connect failed')

    def event() -> threading.Event:
        result = event_type()
        events.append(result)
        if len(events) == 3:
            result.set()
        return result

    def connect() -> None:
        raise initiating

    def close() -> None:
        attempts.append('client')
        raise OSError('client close failed')

    def result(**_kwargs: object) -> None:
        attempts.append('future')
        assert events[3].is_set()
        raise TimeoutError('source thread remained alive')

    future = SimpleNamespace(done=lambda: False, result=result)

    class Executor:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            self.shutdown(wait=True)

        def submit(self, *_args: object) -> object:
            return future

        def shutdown(self, *, wait: bool) -> None:
            attempts.append(f'shutdown-{wait}')

    executor = Executor()
    module = sys.modules[__name__]
    monkeypatch.setattr(module.threading, 'Event', event)
    monkeypatch.setattr(module, 'ThreadPoolExecutor', Mock(return_value=executor))
    monkeypatch.setattr(
        module, 'EventClient', Mock(return_value=SimpleNamespace(connect=connect, close=close))
    )
    with (
        pytest.raises(ValueError, match='connect failed') as caught,
        _remote_coordination_client(tmp_path / 'source.db', tmp_path / 'events.sock'),
    ):
        pytest.fail('connect should fail')
    assert caught.value is initiating
    assert attempts == ['client', 'future', 'shutdown-False']
    assert any('client close failed' in note for note in initiating.__notes__)


def test_remote_join_coordination_progresses_with_full_event_stream_and_restart(
    tmp_path: Path,
) -> None:
    group = _group('remote', worker='pool')
    site = _site((group,))
    source_config = site.sources['events']
    credential = site.credentials[source_config.credentials]
    fingerprint = stream_fingerprint(
        site.server,
        source_config.credentials,
        credential.script_name,
    )
    state_path = _source_database_path(tmp_path)
    socket_path = local_socket_path(
        tmp_path,
        'pressure-events',
        ('events', 'remote'),
    )
    provider_state = _RestartProviderState()
    invoked: list[int] = []
    callback_release = threading.Event()
    callback_release.set()
    plugin = _restart_callback_plugin(
        invoked,
        callback_entered=threading.Event(),
        release_callback=callback_release,
    )
    context = _PressureJoinContext(
        _group=group,
        _plugin=plugin,
        _provider=_restart_provider(provider_state),
        _site=site,
        _socket_path=socket_path,
    )

    with open_group_on_source(
        state_path,
        writes=write_context_for(state_path),
        site='main',
        site_server_url=site.server,
        source='events',
        group='remote',
        group_generation=7,
        ordering='eager',
        stream_fingerprint=fingerprint,
        source_generation='source-1',
        verified_incarnation='incarnation-1',
        start_mode='initialize',
        initial_cursor=40,
        initial_delivery_floor=40,
        initial_last_delivered_id=40,
    ) as state:
        _run_pressure_join(
            context,
            state,
            boundary=41,
            live_event_id=42,
            pressure=True,
        )
        assert invoked == [41, 42]

    with open_group_on_source(
        state_path,
        writes=write_context_for(state_path),
        site='main',
        site_server_url=site.server,
        source='events',
        group='remote',
        group_generation=7,
        ordering='eager',
        stream_fingerprint=fingerprint,
        source_generation='source-1',
        verified_incarnation='incarnation-1',
        start_mode='resume',
    ) as restarted:
        assert restarted.snapshot().cursor == 42
        _run_pressure_join(
            context,
            restarted,
            boundary=43,
            live_event_id=None,
            pressure=False,
        )
        assert restarted.snapshot().cursor == 43

    assert invoked == [41, 42, 43]


def test_remote_lane_batches_irrelevant_burst(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group = _group('remote', worker='pool')
    site = _runtime_site(group)
    provider = _restart_provider(_RestartProviderState())
    observed: list[tuple[int, int]] = []
    callback_release = threading.Event()
    callback_release.set()
    plugin = _restart_callback_plugin(
        [], callback_entered=threading.Event(), release_callback=callback_release
    )

    with open_group_on_source(
        tmp_path / 'group.db',
        writes=write_context_for(tmp_path / 'group.db'),
        site='main',
        site_server_url=site.server,
        source='events',
        group='remote',
        group_generation=7,
        ordering='eager',
        stream_fingerprint=stream_fingerprint(site.server, 'source', 'daemon'),
        source_generation='source-1',
        verified_incarnation='incarnation-1',
        start_mode='initialize',
        initial_cursor=41,
        initial_delivery_floor=41,
        initial_last_delivered_id=41,
    ) as state:

        def callback(context: EventContext) -> None:
            event_id = context.event['id']
            assert type(event_id) is int
            observed.append((event_id, state.snapshot().cursor))

        plugin = replace(
            plugin,
            callbacks=(
                replace(
                    plugin.callbacks[0],
                    callback=callback,
                    events=normalize_filter({'Shotgun_Test': None}),
                ),
            ),
        )
        watchdog = ComponentWatchdog(
            kind='lane',
            identity='remote',
            worker_instance='dependent-1',
            registration_token=_watchdog_token(),
            startup_deadline_secs=30.0,
        )
        watchdog.transition('idle', deadline_at=None)
        with closing(
            GroupLane(
                state=state,
                site=site,
                group=group,
                provider=provider,
                plugins=(plugin,),
                worker_instance='dependent-1',
                health_interval_secs=1.0,
                watchdog=watchdog,
            )
        ) as lane:
            assert lane.setup() is None
            channel = GroupChannel(
                group='remote', ordering='eager', max_events=9, max_bytes=262_144
            )
            lane.join(
                cast('_JoinSource', _PressureJoinSource(boundary=41, live_event_id=None)),
                channel,
                catch_up=lambda _boundary: None,
            )
            client = EventClient(
                path=tmp_path / 'unused.sock',
                source='events',
                group='remote',
                spawn_instance='dependent-1',
                capability='capability-1',
                max_frame_bytes=131_072,
                queue_max_entries=9,
                queue_max_bytes=262_144,
                coordination_deadline_secs=1.0,
                stop=threading.Event(),
            )
            reader, writer = socket.socketpair()
            coordination_reader, coordination_writer = socket.socketpair()
            reader.settimeout(0.0)
            coordination_reader.settimeout(0.0)
            selector = selectors.DefaultSelector()
            selector.register(reader, selectors.EVENT_READ)
            coordination_selector = selectors.DefaultSelector()
            coordination_selector.register(coordination_reader, selectors.EVENT_READ)
            vars(client).update(
                _socket=reader,
                _selector=selector,
                _coordination_socket=coordination_reader,
                _coordination_selector=coordination_selector,
                _join_channel=channel,
            )
            try:
                frames = tuple(
                    _encode_fanout_message(
                        EventDelivery(
                            EncodedEvent.from_event_frame(
                                event_id,
                                encode_event(
                                    Event(
                                        {
                                            **_runtime_event(event_id),
                                            'event_type': (
                                                'Shotgun_Test'
                                                if event_id == 46
                                                else 'Shotgun_Ignore'
                                            ),
                                        }
                                    )
                                ),
                            )
                        )
                    )
                    for event_id in range(42, 51)
                )
                writer.sendall(b''.join(part for frame in frames for part in frame._parts))
                _LaneComponent._poll_remote(client)
                assert channel.status.queued_count == 9
                advances: list[int] = []
                real_advance = state.advance_irrelevant

                def advance(high_id: int, *, expected_revision: int) -> GroupSnapshot:
                    advances.append(high_id)
                    return real_advance(high_id, expected_revision=expected_revision)

                monkeypatch.setattr(state, 'advance_irrelevant', advance)
                completed = lane.drain_live(channel)
                assert completed.cursor == 50
                assert observed == [(46, 45)]
                assert advances == [45, 50]
                assert len(advances) < 8
            finally:
                client.close()
                writer.close()
                coordination_writer.close()


def test_remote_lane_closes_partial_attempt_after_transport_failure() -> None:
    closed: list[str] = []
    client = _CleanupProbe('client', closed, None)
    component = _unopened_group_component(_LaneComponent)
    vars(component)['_group'] = 'remote'
    context = SimpleNamespace(
        _assignment=SimpleNamespace(
            conditional_park=None, group_generation=7, start_mode='resume'
        ),
        _group=SimpleNamespace(source='events'),
    )

    def observe(_client: object, *, assignment: object) -> NoReturn:
        del assignment
        raise ConnectionError('observation transport failed')

    def build_client(_context: object) -> tuple[object, str]:
        return client, 'source-generation-1'

    vars(component).update(_build_client=build_client, _observe_source=observe)
    with pytest.raises(ConnectionError, match='observation transport failed'):
        _LaneComponent._build_resources(component, cast('_LaneConfiguration', context))
    assert closed == ['client']


def test_remote_source_initialization_pending_recovers_with_packed_sibling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    groups = (
        _group('healthy', worker='pool'),
        _group('remote', worker='pool'),
    )
    site = _site(groups)
    site = replace(
        site,
        sources=MappingProxyType(
            {
                name: replace(
                    source,
                    fetch_interval_secs=0.01,
                    poll_retry_base_secs=0.01,
                    poll_retry_jitter_fraction=0.0,
                    poll_retry_max_secs=0.01,
                )
                for name, source in site.sources.items()
            }
        ),
    )
    config = _runtime_config(tmp_path, site)
    site_spec, dependent_spec = build_worker_specs(site)
    provider_state = _InitializationOutageProviderState()
    invoked: list[int] = []
    release_callback = threading.Event()
    release_callback.set()
    plugin = _restart_callback_plugin(
        invoked,
        callback_entered=threading.Event(),
        release_callback=release_callback,
    )
    monkeypatch.setattr(
        worker_module,
        'load_provider',
        _provider_loader(_initialization_outage_provider(provider_state)),
    )

    def plugins(_site: object, group: object, _provider: object) -> tuple[PluginSpec, ...]:
        return (plugin,) if cast('GroupConfig', group).name == 'remote' else ()

    monkeypatch.setattr(worker_module, 'load_plugins', plugins)
    pending_results: list[object] = []
    pending_remote = threading.Event()
    surface_type = cast('type[object]', vars(worker_module)['_SourceJoinSurface'])
    observe_group = cast(
        'Callable[[object, GroupBootstrapRequest], object]',
        vars(surface_type)['observe_group'],
    )

    def track_pending(
        surface: object,
        request: GroupBootstrapRequest,
    ) -> object:
        result = observe_group(surface, request)
        if isinstance(result, GroupBootstrapPending):
            pending_results.append(result)
            if request.group == 'remote':
                pending_remote.set()
        return result

    monkeypatch.setattr(surface_type, 'observe_group', track_pending)
    source_assignment = WorkerAssignment(
        worker_id=site_spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=tuple((source, 'source-generation-1') for source in site_spec.sources),
        group_assignments=(),
        source_group_assignments=tuple(
            ActiveSourceGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='initialize',
            )
            for group in groups
        ),
        source_endpoints=(),
        event_grants=(),
    )
    grants = tuple(
        EventPeerGrant(
            source='events',
            group=group.name,
            dependent_spawn_instance='dependent-spawn-1',
            capability=f'{group.name}-capability',
        )
        for group in groups
    )
    source_session = _start_source_runtime(
        config,
        site_spec,
        source_assignment,
        grants,
    )
    try:
        assert provider_state._initialization_attempted.wait(1.0)
        assert source_session._runtime.fatal_reason is None
        dependent_parent_socket, dependent_child_socket = socket.socketpair()
        dependent_parent = WorkerControl(
            dependent_parent_socket,
            max_frame_bytes=131_072,
            send_deadline_secs=1.0,
        )
        dependent_child = WorkerControl(
            dependent_child_socket,
            max_frame_bytes=131_072,
            send_deadline_secs=1.0,
        )
        dependent_assignment = WorkerAssignment(
            worker_id=dependent_spec.worker_id,
            spawn_instance='dependent-spawn-1',
            source_generations=(),
            group_assignments=tuple(
                DeliveryGroupAssignment(
                    source='events',
                    group=group.name,
                    group_generation=7,
                    start_mode='initialize',
                )
                for group in groups
            ),
            source_group_assignments=(),
            source_endpoints=(source_session._endpoint,),
            event_grants=grants,
        )
        dependent_runtime = build_worker_runtime(
            config,
            dependent_spec,
            dependent_assignment,
            dependent_child,
        )
        drain: tuple[threading.Event, threading.Thread] | None = None
        try:
            dependent_runtime.start()
            drain = _start_control_drain(dependent_parent)
            _assert_remote_initialization_pending(
                dependent_runtime,
                pending_results,
                pending_remote,
            )
            provider_state._initialization_ready.set()
            _assert_initial_endpoint_delivery(
                dependent_runtime,
                provider_state,
                invoked,
                timeout=config.daemon.worker_startup_deadline_secs,
            )
        finally:
            provider_state._initialization_ready.set()
            _release_failed_source_runtime(
                dependent_runtime,
                (dependent_parent, dependent_child),
                (dependent_parent_socket, dependent_child_socket),
                drain,
            )
    finally:
        provider_state._initialization_ready.set()
        source_session._close()


def test_remote_source_restart_rejoins_without_stale_or_gap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    groups = (
        _group('healthy', worker='pool'),
        _group('remote', worker='pool'),
    )
    site = _site(groups)
    site = replace(
        site,
        sources=MappingProxyType(
            {
                name: replace(source, fetch_interval_secs=0.01)
                for name, source in site.sources.items()
            }
        ),
    )
    config = _runtime_config(tmp_path, site)
    site_spec, dependent_spec = build_worker_specs(site)
    provider_state = _RestartProviderState()
    invoked: list[int] = []
    callback_entered = threading.Event()
    release_callback = threading.Event()
    plugin = _restart_callback_plugin(
        invoked,
        callback_entered=callback_entered,
        release_callback=release_callback,
    )
    monkeypatch.setattr(
        worker_module,
        'load_provider',
        _provider_loader(_restart_provider(provider_state)),
    )

    def plugins(_site: object, group: object, _provider: object) -> tuple[PluginSpec, ...]:
        return (plugin,) if cast('GroupConfig', group).name == 'remote' else ()

    monkeypatch.setattr(worker_module, 'load_plugins', plugins)
    source_assignment = WorkerAssignment(
        worker_id=site_spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=tuple((source, 'source-generation-1') for source in site_spec.sources),
        group_assignments=(),
        source_group_assignments=tuple(
            ActiveSourceGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='initialize',
            )
            for group in groups
        ),
        source_endpoints=(),
        event_grants=(),
    )
    grants = tuple(
        EventPeerGrant(
            source='events',
            group=group.name,
            dependent_spawn_instance='dependent-spawn-1',
            capability=f'{group.name}-capability',
        )
        for group in groups
    )
    first_source = _start_source_runtime(
        config,
        site_spec,
        source_assignment,
        grants,
    )
    dependent_parent_socket, dependent_child_socket = socket.socketpair()
    dependent_parent = WorkerControl(
        dependent_parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0
    )
    dependent_child = WorkerControl(
        dependent_child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0
    )
    dependent_assignment = WorkerAssignment(
        worker_id=dependent_spec.worker_id,
        spawn_instance='dependent-spawn-1',
        source_generations=(),
        group_assignments=tuple(
            DeliveryGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='initialize',
            )
            for group in groups
        ),
        source_group_assignments=(),
        source_endpoints=(first_source._endpoint,),
        event_grants=grants,
    )
    dependent_runtime = build_worker_runtime(
        config,
        dependent_spec,
        dependent_assignment,
        dependent_child,
    )
    dependent_runtime.start()
    drain_stop, drain_thread = _start_control_drain(dependent_parent)
    try:
        _assert_source_restart_continuity(
            _RestartContinuity(
                _callback_entered=callback_entered,
                _config=config,
                _dependent_runtime=dependent_runtime,
                _first_source=first_source,
                _grants=grants,
                _invoked=invoked,
                _provider_state=provider_state,
                _release_callback=release_callback,
                _site_spec=site_spec,
                _source_assignment=source_assignment,
            )
        )
    finally:
        release_callback.set()
        _release_failed_source_runtime(
            dependent_runtime,
            (dependent_parent, dependent_child),
            (dependent_parent_socket, dependent_child_socket),
            (drain_stop, drain_thread),
            (first_source._close,) if first_source._runtime._resources_open else (),
        )


def test_removed_pending_group_leaves_no_stale_entry(tmp_path: Path) -> None:
    with _pending_group_health_probe(tmp_path) as (component, cache, state):
        surface = cast('_SourceJoinSurface', SimpleNamespace(_state=state))
        component._publish_pending_groups(surface)
        assert tuple(group.group for group in cache._snapshot()._groups) == ('local',)
        component._assignment = replace(component._assignment, source_group_assignments=())
        component._publish_pending_groups(surface)
        assert cache._snapshot()._groups == ()


def test_resume_stops_when_return_tick_halts_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _open_group_health_state(tmp_path / 'group.db') as state:
        component_object, _cache, resources_object, lifecycle_object = _group_health_component(
            tmp_path, state, health_interval_secs=1.0
        )
        component = component_object
        resources = cast('_LaneResources', resources_object)
        lifecycle = cast('_LaneLifecycle', lifecycle_object)
        lifecycle._joined = False

        def coordinate(*, scan_obligations: bool = True, force: bool = False) -> GroupSnapshot:
            del force
            assert not scan_obligations
            return state.park(
                reason='maintenance',
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
                expected_revision=state.snapshot().revision,
            )

        def pause(_resources: _LaneResources) -> None:
            pass

        monkeypatch.setattr(resources._coordination, '_service', coordinate)
        monkeypatch.setattr(component, '_pause', pause)
        assert not component._resume_lane(resources, lifecycle)
        assert state.snapshot().status == 'parked'


def test_return_to_running_forces_the_next_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    component_object, resources_object = _lifecycle_probe(
        'sync', durable_status='parked', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(_channel=resources._channel)
    component._lifecycle = lifecycle
    calls: list[tuple[bool, bool]] = []
    monkeypatch.setattr(
        resources._coordination,
        '_service',
        lambda *, scan_obligations=True, force=False: calls.append((scan_obligations, force)),
    )
    component._coordinate(resources, lifecycle, scan=False, force=False)
    lifecycle._next_coordination_at = time.monotonic() + 60.0
    vars(resources._state)['_status'] = 'running'
    component._coordinate(resources, lifecycle, scan=True, force=False)
    assert calls == [(False, False), (True, True)]
    assert lifecycle._was_running


def test_running_runtime_cleanup_preserves_body_error_and_attempts_controls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[str] = []
    controls = iter(
        (
            SimpleNamespace(close=lambda: attempts.append('parent')),
            SimpleNamespace(close=lambda: attempts.append('child')),
        )
    )
    runtime = SimpleNamespace(start=lambda: None, _threads={}, _monitor_thread=None)
    module = sys.modules[__name__]
    monkeypatch.setattr(module, 'WorkerControl', Mock(side_effect=controls))
    monkeypatch.setattr(module, 'build_worker_runtime', Mock(return_value=runtime))
    monkeypatch.setattr(module.socket, 'socketpair', lambda: (object(), object()))
    monkeypatch.setattr(
        module, '_start_control_drain', Mock(return_value=(threading.Event(), threading.Thread()))
    )

    def stop_drain(*_args: object) -> None:
        attempts.append('drain')

    monkeypatch.setattr(module, '_stop_control_drain', stop_drain)

    def stop(_runtime: object) -> None:
        attempts.append('runtime')
        raise AssertionError('runtime cleanup failed')

    monkeypatch.setattr(module, '_stop_runtime', stop)
    initiating = ValueError('body failed')
    with (
        pytest.raises(ValueError, match='body failed') as caught,
        _running_concrete_runtime(
            monkeypatch,
            cast('Config', object()),
            cast('WorkerSpec', object()),
            cast('WorkerAssignment', object()),
            cast('Provider', object()),
        ),
    ):
        raise initiating
    assert caught.value is initiating
    assert attempts == ['runtime', 'drain', 'parent', 'child']
    assert any('runtime cleanup failed' in note for note in initiating.__notes__)


def test_runtime_blocked_operator_executor_reaches_forced_exit() -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    operations: list[str] = []
    component = _BlockingOperatorComponent(
        'source:events',
        operations,
        interrupt_operator=False,
    )
    health: list[WorkerHealth] = []
    forced: list[int] = []
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=health.append,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=0.02,
        max_concurrent_catchup_readers=1,
        control=child,
        force_exit=forced.append,
    )
    command = OperatorCommand(
        request_id='operator-runtime-stuck',
        command='adopt-source-stream',
        arguments={
            'site': 'main',
            'source': 'events',
            'reason': 'permissions changed',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=123),
    )
    runtime.start()
    operator_thread = runtime._threads['operator-executor']
    try:
        assert component._started.wait(1.0)
        parent.send(command)
        assert component._operator_entered.wait(1.0)
        parent.send(StopCommand(reason='operator-stop'))
        result = parent.receive(timeout=1.0)
        for _ in range(100):
            if forced:
                break
            time.sleep(0.002)
        stuck = runtime.wait()
        resources_open = runtime._resources_open
        closed_before_release = any(operation.startswith('close:') for operation in operations)
    finally:
        component._operator_release.set()
        component._release.set()
        operator_thread.join(1.0)
        cleanup_result = runtime._shutdown()
        parent.close()
        child.close()

    assert result == CommandResult(
        command='adopt-source-stream',
        ok=False,
        message='operator command interrupted by worker shutdown',
        request_id='operator-runtime-stuck',
        result={},
    )
    assert forced == [1]
    assert stuck == ('operator-executor',)
    assert runtime._stuck_threads == ('operator-executor',)
    assert health[-1].stuck_threads == ('operator-executor',)
    assert resources_open
    assert not closed_before_release
    assert cleanup_result == ()
    assert operations[-1] == 'close:source:events'


def test_runtime_blocking_operator_keeps_heartbeats_and_stop_responsive() -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    operations: list[str] = []
    component = _BlockingOperatorComponent(
        'source:events',
        operations,
        interrupt_operator=True,
    )
    health: list[WorkerHealth] = []
    forced: list[int] = []
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=health.append,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=0.1,
        max_concurrent_catchup_readers=1,
        control=child,
        force_exit=forced.append,
    )
    command = OperatorCommand(
        request_id='operator-runtime-blocked',
        command='adopt-source-stream',
        arguments={
            'site': 'main',
            'source': 'events',
            'reason': 'permissions changed',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=123),
    )
    result: object | None = None
    runtime.start()
    try:
        assert component._started.wait(1.0)
        parent.send(command)
        assert component._operator_entered.wait(1.0)
        initial_health_count = len(health)
        for _ in range(100):
            if len(health) >= initial_health_count + 2:
                break
            time.sleep(0.001)
        health_during_operator = tuple(health[initial_health_count:])
        parent.send(StopCommand(reason='operator-stop'))
        with suppress(TimeoutError):
            result = parent.receive(timeout=0.1)
    finally:
        component._operator_release.set()
        runtime.request_stop('test-cleanup')
        shutdown_result = runtime._shutdown()
        parent.close()
        child.close()

    assert len(health_during_operator) >= 2
    assert health_during_operator[-1].monitor_alive
    assert 'operator-executor' in health_during_operator[-1].thread_names
    assert result == CommandResult(
        command='adopt-source-stream',
        ok=False,
        message='operator command interrupted by worker shutdown',
        request_id='operator-runtime-blocked',
        result={},
    )
    assert shutdown_result == ()
    assert forced == []


def test_runtime_busy_operator_preserves_single_result_publication() -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    component = _BlockingOperatorComponent(
        'events',
        [],
        interrupt_operator=True,
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    health_published = threading.Event()

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: health_published.set(),
        health_interval_secs=60.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        control=child,
    )
    command_a = OperatorCommand(
        request_id='operator-runtime-a',
        command='adopt-source-stream',
        arguments={
            'site': 'main',
            'source': 'events',
            'reason': 'permissions changed',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=123),
    )
    command_b = replace(command_a, request_id='operator-runtime-b')
    monitor_wake = runtime._monitor_wake

    runtime.start()
    try:
        assert component._started.wait(1.0)
        assert health_published.wait(1.0)

        health_published.clear()
        parent.send(command_a)
        monitor_wake.set()
        assert component._operator_entered.wait(1.0)
        assert health_published.wait(1.0)

        health_published.clear()
        parent.send(command_b)
        monitor_wake.set()
        assert parent.receive(timeout=1.0) == CommandResult(
            command='adopt-source-stream',
            ok=False,
            message='operator command executor is busy',
            request_id='operator-runtime-b',
            result={},
        )
        assert health_published.wait(1.0)

        health_published.clear()
        component._operator_release.set()
        assert parent.receive(timeout=1.0) == CommandResult(
            command='adopt-source-stream',
            ok=True,
            message='accepted',
            request_id='operator-runtime-a',
            result={
                'owner': 'events',
                'command': 'adopt-source-stream',
            },
        )
        assert health_published.wait(1.0)

        health_published.clear()
        monitor_wake.set()
        assert health_published.wait(1.0)
        with pytest.raises(TimeoutError):
            parent.receive(timeout=0.0)
    finally:
        component._operator_release.set()
        runtime.request_stop('test-cleanup')
        assert runtime.wait() == ()
        parent.close()
        child.close()


def test_runtime_cooperative_source_bootstrap_stop_remains_fatal() -> None:
    entered = threading.Event()
    stop = threading.Event()
    cooperative_stop = cast(
        'type[RuntimeError]',
        vars(worker_module)['_CooperativeComponentStopError'],
    )

    def _run() -> None:
        entered.set()
        stop.wait()
        raise cooperative_stop('bootstrap')

    component = _InterruptibleComponent(_run, stop.set)
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    forced: list[int] = []
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        force_exit=forced.append,
    )

    runtime.start()
    try:
        assert entered.wait(1.0)
        runtime.request_stop('test-stop')
        assert runtime.wait() == ()
    finally:
        stop.set()
        runtime.request_stop('test-cleanup')
        assert runtime._shutdown() == ()

    assert runtime.fatal_reason == (
        'source:events thread failed: _CooperativeComponentStopError: '
        'component stopped during bootstrap'
    )
    assert forced == []


def test_runtime_dispatches_operator_command_to_owning_component() -> None:
    spec = build_worker_specs(_site(()))[0]
    components = {name: _Component(name, []) for name in spec.sources}
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        control=child,
    )
    command = OperatorCommand(
        request_id='operator-runtime-1',
        command='adopt-source-stream',
        arguments={
            'site': 'main',
            'source': 'events',
            'reason': 'permissions changed',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=123),
    )
    runtime.start()
    try:
        assert components['events']._started.wait(1.0)
        parent.send(command)

        result = parent.receive(timeout=1.0)
    finally:
        runtime.request_stop('test')
        assert runtime.wait() == ()
        parent.close()

    assert result == CommandResult(
        command='adopt-source-stream',
        ok=True,
        message='accepted',
        request_id='operator-runtime-1',
        result={
            'owner': 'events',
            'command': 'adopt-source-stream',
        },
    )
    assert 'operator:events:adopt-source-stream' in components['events']._operations


def test_runtime_first_heartbeat_reports_live_monitor_before_start_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = build_worker_specs(_site(()))[0]
    components = {name: _Component(name, []) for name in spec.sources}
    published = threading.Event()
    health: list[WorkerHealth] = []
    original_start = threading.Thread.start

    def publish(update: WorkerHealth) -> None:
        health.append(update)
        published.set()

    def start(thread: threading.Thread) -> None:
        original_start(thread)
        if thread.name == 'sg-event-daemon:monitor':
            assert published.wait(1.0), 'monitor did not publish before start returned'
            assert thread.is_alive()

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='first-monitor-heartbeat',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: _Component(name, []),
        health_sink=publish,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        force_exit=lambda _code: pytest.fail('healthy runtime invoked hard exit'),
    )
    monkeypatch.setattr(threading.Thread, 'start', start)
    try:
        runtime.start()
        assert health[0].monitor_alive
    finally:
        runtime.request_stop('test-cleanup')
        assert runtime._shutdown() == ()
        _assert_monitor_stopped(runtime)


def test_runtime_gives_every_group_one_concurrent_lane_thread() -> None:
    site = _site(
        (
            _group('default-a', worker='default'),
            _group('default-b', worker='default'),
        )
    )
    spec = build_worker_specs(site)[0]
    operations: list[str] = []
    sources = {name: _Component(f'source:{name}', operations) for name in spec.sources}
    lanes = {name: _Component(f'lane:{name}', operations) for name in spec.groups}
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda name, _slots, _host: lanes[name],
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )

    runtime.start()
    try:
        assert all(
            component._started.wait(1.0) for component in (*sources.values(), *lanes.values())
        )
        lane_threads = {
            operation.rsplit(':', maxsplit=1)[-1]
            for operation in operations
            if operation.startswith('run:lane:')
        }
        assert len(lane_threads) == len(lanes)
    finally:
        runtime.request_stop('test')
        assert runtime._shutdown() == ()


def test_runtime_prepares_before_constructing_or_starting_components() -> None:
    site = _site((_group('local', worker='default'),))
    spec = build_worker_specs(site)[0]
    operations: list[str] = []
    prepared = threading.Event()

    class _PreparationComponent(_Component):
        def _run(self) -> None:
            operations.append(f'observe:{self._name}:{prepared.is_set()}')
            super()._run()

    def prepare() -> None:
        operations.append('prepare')
        prepared.set()

    def source_factory(
        name: str, _limiter: _CatchupLimiter, _host: CheckpointHost
    ) -> _WorkerComponent:
        assert _host is runtime._checkpoint_host
        operations.append(f'construct:source:{name}')
        return _PreparationComponent(f'source:{name}', operations)

    def lane_factory(
        name: str, _limiter: _CatchupLimiter, _host: CheckpointHost
    ) -> _WorkerComponent:
        assert _host is runtime._checkpoint_host
        operations.append(f'construct:lane:{name}')
        return _PreparationComponent(f'lane:{name}', operations)

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=source_factory,
        lane_factory=lane_factory,
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        prepare=prepare,
    )

    runtime.start()
    components = cast('dict[str, _PreparationComponent]', runtime._components)
    try:
        assert all(component._started.wait(1.0) for component in components.values())
    finally:
        runtime.request_stop('test')
        assert runtime.wait() == ()

    assert operations[0] == 'prepare'
    observations = {operation for operation in operations if operation.startswith('observe:')}
    assert observations == {
        *(f'observe:source:{source}:True' for source in spec.sources),
        *(f'observe:lane:{group}:True' for group in spec.groups),
    }


def test_runtime_redacts_plugin_teardown_failure_from_worker_diagnostic() -> None:
    protected_value = 'key'

    class _CloseFailureHandle(_RuntimeProviderHandle):
        def close(self) -> None:
            raise RuntimeError(f'handle close exposed {protected_value}')

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _CloseFailureHandle:
        assert site == 'https://example.test'
        assert script_name == 'daemon'
        assert api_key == protected_value
        assert not connect
        return _CloseFailureHandle(lambda: 41)

    group = _group('local', worker='default')
    site = _runtime_site(group)
    provider = Provider(
        name='teardown-failure',
        capabilities=ProviderCapabilities(
            sync=True,
            async_=False,
        ),
        _sync_factory=factory,
        _async_factory=None,
    )
    host = PluginHost(
        site,
        group,
        provider,
        (_health_failure_plugin('callback'),),
        heartbeat=lambda: None,
        shutdown_requested=lambda: False,
    )
    assert host.setup_sync() is None
    entered = threading.Event()

    def _close_host() -> None:
        entered.set()
        host.close_sync()

    fatal_reason, forced = _run_cooperative_stop_component(
        _InterruptibleComponent(_close_host, lambda: None),
        entered,
        request_stop=False,
    )

    assert fatal_reason is not None
    assert 'PluginError: teardown-failed' in fatal_reason
    assert "plugin 'health-failure'" in fatal_reason
    assert protected_value not in fatal_reason
    assert '<redacted>' in fatal_reason
    assert forced == []


def test_setup_failed_lane_does_not_stop_unrelated_packed_lane() -> None:
    site = _site(
        (
            _group('broken', worker='default'),
            _group('healthy', worker='default'),
        )
    )
    spec = build_worker_specs(site)[0]
    component, resources = _lifecycle_probe(
        'sync',
        durable_status='running',
        setup_failure=RuntimeError('setup-failed'),
    )
    broken = _LifecycleComponent(component, resources)
    healthy = _Component('healthy', [])
    sources = {name: _Component(name, []) for name in spec.sources}
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda name, _slots, _host: broken if name == 'broken' else healthy,
        health_sink=lambda _health: None,
        health_interval_secs=0.001,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )
    runtime.start()
    try:
        assert resources._coordination._serviced.wait(1.0)
        assert healthy._started.wait(1.0)
        time.sleep(0.01)
        assert runtime.fatal_reason is None
        assert not healthy._finished.is_set()
    finally:
        runtime.request_stop('test')
        assert runtime.wait() == ()


def test_setup_failure_remains_group_local_and_services_body_free_work() -> None:
    component, resources = _lifecycle_probe(
        'sync',
        durable_status='running',
        setup_failure=RuntimeError('setup-failed'),
    )
    component_type = type(component)
    driver = cast(
        '_LaneLifecycleDriver',
        vars(component_type)['_drive_lane'],
    )
    stop = component._stop
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(driver, component, resources)
        try:
            assert resources._coordination._serviced.wait(1.0)
            assert not future.done()
            assert resources._lane._joined == 0
            assert resources._coordination._calls
            assert not any(resources._coordination._calls)
        finally:
            stop.set()
        future.result(timeout=1.0)


def test_shared_plugin_load_restores_signal_and_logging_configuration() -> None:
    original_handler = signal.getsignal(signal.SIGTERM)
    root = logging.getLogger()
    original_level = root.level
    extra_handler = logging.NullHandler()

    def loader() -> str:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        root.setLevel(logging.CRITICAL)
        root.addHandler(extra_handler)
        return 'loaded'

    try:
        assert _load_shared_plugins(loader) == 'loaded'
        assert signal.getsignal(signal.SIGTERM) == original_handler
        assert root.level == original_level
        assert extra_handler not in root.handlers
    finally:
        signal.signal(signal.SIGTERM, original_handler)
        root.removeHandler(extra_handler)
        root.setLevel(original_level)


def test_shutdown_closes_reverse_order_and_monitor_forces_wedged_exit() -> None:
    site = _site((_group('default', worker='default'),))
    spec = build_worker_specs(site)[0]
    operations: list[str] = []
    sources = {name: _Component(f'source:{name}', operations) for name in spec.sources}
    lane = _Component('lane:default', operations, wedge=True)
    forced: list[tuple[int, str]] = []
    monitor_entered = threading.Event()
    release_monitor = threading.Event()

    @contextmanager
    def monitor_gate() -> Generator[None]:
        monitor_entered.set()
        release_monitor.wait()
        yield

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda _name, _slots, _host: lane,
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=0.02,
        max_concurrent_catchup_readers=1,
        force_exit=lambda code: forced.append((code, threading.current_thread().name)),
        monitor_gate=monitor_gate,
    )
    runtime.start()
    try:
        assert all(source._started.wait(1.0) for source in sources.values())
        assert lane._started.wait(1.0)
        assert monitor_entered.wait(1.0)
        runtime.request_stop('control-eof')
        assert all(source._finished.wait(1.0) for source in sources.values())
        release_monitor.set()
        for _ in range(100):
            if forced:
                break
            time.sleep(0.002)
        assert forced == [(1, 'sg-event-daemon:monitor')]
        assert runtime._stuck_threads == ('lane:default',)
        assert 'close:lane:default' not in operations
        lane._release.set()
        assert lane._finished.wait(1.0)
    finally:
        release_monitor.set()
        lane._release.set()
        for source in sources.values():
            source._release.set()
        runtime.request_stop('test-cleanup')
        assert runtime._shutdown() == ()

    assert operations[-3:] == [
        'close:lane:default',
        'close:source:events',
        'close:source:audit',
    ]


def test_site_worker_exists_without_default_groups() -> None:
    site = _site((_group('named', worker='pool'),))

    specs = build_worker_specs(site)

    assert specs[0].kind is WorkerKind._SITE
    assert specs[0].groups == ()
    assert specs[0].sources == ('audit', 'events')


def test_slow_coordination_return_allows_catchup_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    component = _unopened_group_component(_LaneComponent)
    group = _group('local', worker='default')
    site = _runtime_site(group)
    invoked: list[int] = []
    with (
        _open_group_health_state(tmp_path / 'group.db') as state,
        closing(
            GroupLane(
                state=state,
                site=site,
                group=group,
                provider=_runtime_provider(),
                plugins=(_catchup_tick_plugin(clock, invoked),),
                worker_instance='worker-1',
                health_interval_secs=1.0,
                watchdog=component._watchdog,
            )
        ) as lane,
    ):
        assert lane.setup() is None
        channel = GroupChannel(group='local', ordering='eager', max_events=10, max_bytes=262_144)
        component._lifecycle = worker_module._LaneLifecycle(_channel=channel)

        def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
            del force
            assert not scan_obligations
            clock[0] += 2.0

        resources = cast(
            '_LaneResources',
            SimpleNamespace(
                _state=state,
                _lane=lane,
                _client=SimpleNamespace(),
                _context=SimpleNamespace(_source_config=SimpleNamespace(fetch_interval_secs=1.0)),
                _coordination=SimpleNamespace(_service=coordinate),
            ),
        )
        snapshot, consumed = component._deliver_catchup_page(
            resources, (Event(_runtime_event(42)),)
        )
        assert (snapshot.cursor, consumed, invoked) == (42, 1, [42])
        assert component._lifecycle._next_coordination_at == 103.0
        assert clock[0] == 102.25


def test_source_assignment_inventory_includes_groups_owned_by_remote_workers(
    tmp_path: Path,
) -> None:
    site = _site(
        (
            _group('local', worker='default'),
            _group('named-remote', worker='pool'),
            _group('dedicated-remote', worker='dedicated', source='audit'),
        )
    )
    config = Config(
        daemon=DaemonConfig(
            state_dir=tmp_path,
            sg_api_provider='shotgun-api3',
        ),
        sites=MappingProxyType({site.name: site}),
    )
    spec = build_worker_specs(site)[0]
    group_assignments = (
        DeliveryGroupAssignment(
            source='events',
            group='local',
            group_generation=7,
            start_mode='initialize',
        ),
    )
    complete_inventory = (
        ActiveSourceGroupAssignment(
            source='audit',
            group='dedicated-remote',
            group_generation=13,
            start_mode='resume',
        ),
        ActiveSourceGroupAssignment(
            source='events',
            group='local',
            group_generation=7,
            start_mode='initialize',
        ),
        ActiveSourceGroupAssignment(
            source='events',
            group='named-remote',
            group_generation=11,
            start_mode='resume',
        ),
    )
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    try:
        assignment = WorkerAssignment(
            worker_id=spec.worker_id,
            spawn_instance='source-spawn-1',
            source_generations=(
                ('audit', 'audit-generation-1'),
                ('events', 'events-generation-1'),
            ),
            group_assignments=group_assignments,
            source_group_assignments=complete_inventory,
            source_endpoints=(),
            event_grants=(),
        )
        build_worker_runtime(config, spec, assignment, control)

        with pytest.raises(ValueError, match='source group inventory'):
            build_worker_runtime(
                config,
                spec,
                replace(
                    assignment,
                    source_group_assignments=complete_inventory[:-1],
                ),
                control,
            )
    finally:
        child_socket.close()
        control.close()


def test_source_blocked_internal_boundary_merges_over_durable_detail(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component, cache = _source_health_component(tmp_path, health_interval_secs=1.0)
    watchdog = component._watchdog
    published = watchdog.sample()
    durable = _watchdog_source_health('events', worker_instance='site-spawn-1')
    cache._publish_source(durable, sample=published)
    entered = threading.Event()
    release = threading.Event()

    class _Server:
        def dispatch_join_requests(self, _surface: object) -> None:
            return None

        def poll(self, _timeout: float) -> None:
            entered.set()
            assert release.wait(1.0)

    def publish_health(*_args: object, **_options: object) -> None:
        return None

    monkeypatch.setattr(type(component), '_publish_health', publish_health)
    service = type(component)._service
    thread = threading.Thread(
        target=partial(
            service,
            component,
            cast('EventServer', _Server()),
            cast('_SourceJoinSurface', SimpleNamespace(_reconcile_groups=lambda: None)),
            cast('SourcePoller', object()),
        ),
    )
    thread.start()
    try:
        assert entered.wait(1.0)
        snapshot = cache._snapshot()
        live = snapshot._watchdogs[0]
        merged = snapshot._sources[0]
        assert live.phase == merged.phase == 'internal'
        assert live.deadline_at == merged.deadline_at
        assert live.heartbeat_at == merged.heartbeat_at
        assert merged.frontier == durable.frontier
        assert merged.status == durable.status
        assert merged.last_error == durable.last_error
    finally:
        release.set()
        thread.join(1.0)
        assert not thread.is_alive()


def test_source_component_call_caller_stop_precedes_immediate_completion() -> None:
    component_type = _SourceComponent
    component = object.__new__(component_type)
    appended: list[object] = []

    class _ImmediateMailbox(_OwnerMailbox[object]):
        def _submit(self, invoke: Callable[[object], object]) -> _OwnerCall[object]:
            call = super()._submit(invoke)
            appended.append(call)
            call._result = 'completed'
            call._complete.set()
            return call

    vars(component).update(
        {
            '_mailbox': _ImmediateMailbox('source'),
            '_stop': threading.Event(),
        }
    )
    call = component_type._call
    cooperative_error = cast(
        'type[RuntimeError]',
        vars(worker_module)['_CooperativeComponentStopError'],
    )
    caller_stop = threading.Event()
    caller_stop.set()

    def invoke(_surface: object) -> str:
        return 'completed'

    with pytest.raises(cooperative_error):
        call(
            component,
            invoke,
            deadline_secs=30.0,
            stop=caller_stop,
        )

    assert appended == []


def test_source_component_call_expiry_precedes_immediate_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component_type = _SourceComponent
    component = object.__new__(component_type)

    class _ImmediateMailbox(_OwnerMailbox[object]):
        def _submit(self, invoke: Callable[[object], object]) -> _OwnerCall[object]:
            call = super()._submit(invoke)
            call._result = 'completed'
            call._complete.set()
            return call

    mailbox = _ImmediateMailbox('source')
    vars(component).update(
        {
            '_mailbox': mailbox,
            '_stop': threading.Event(),
        }
    )
    moments = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: next(moments))
    call = component_type._call

    def invoke(_surface: object) -> str:
        return 'completed'

    with pytest.raises(ConnectionError, match='source component call timed out'):
        call(
            component,
            invoke,
            deadline_secs=1.0,
            stop=threading.Event(),
        )

    assert mailbox._take() is None


def test_source_component_call_times_out_when_unserviced() -> None:
    component_type = _SourceComponent
    component = object.__new__(component_type)
    vars(component).update(
        {
            '_mailbox': _OwnerMailbox('test'),
            '_stop': threading.Event(),
        }
    )
    call = component_type._call
    caller_stop = threading.Event()
    operations: list[str] = []
    start = time.monotonic()

    def invoke(_surface: object) -> None:
        operations.append('invoked')

    with pytest.raises(ConnectionError, match='source component call timed out'):
        call(
            component,
            invoke,
            deadline_secs=0.05,
            stop=caller_stop,
        )

    assert 0.04 <= time.monotonic() - start < 0.5
    _service_queued_local_calls(component, SimpleNamespace(_reconcile_groups=lambda: None))
    assert operations == []


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_source_component_disposes_pending_group_through_worker_control(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    command: Literal['replay', 'reset-to-latest'],
) -> None:
    config, spec, assignment, snapshot = _pending_group_runtime(tmp_path)
    calls: list[str] = []
    with _running_concrete_runtime(
        monkeypatch, config, spec, assignment, _runtime_provider(calls=calls)
    ) as (runtime, control):
        _receive_typed_health(control, lambda update: len(update.groups) == 1)
        pending = _pending_group_command(snapshot, command)
        request = replace(
            pending,
            arguments={**pending.arguments, 'operator_label': 'operator uid=501'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=456),
        )
        control.send(request)
        result = _receive_type(control, CommandResult)
        assert result.ok, result.message
        assert result.request_id == request.request_id
        assert result.result['group_generation'] == 11
        assert result.result['cursor'] == (39 if command == 'replay' else 41)
        assert 'lane:local' not in runtime._threads
        assert runtime.fatal_reason is None
        with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
            assert connection.execute(
                'SELECT group_generation, lifecycle, active FROM groups WHERE group_name = ?',
                ('local',),
            ).fetchone() == (11, 'active', 1)
            assert connection.execute(
                'SELECT COUNT(*), MIN(operator_label) FROM audit '
                'WHERE command = ? AND disposition_id = ?',
                (command, 'pending-disposition'),
            ).fetchone() == (1, 'operator uid=501')
            assert connection.execute('SELECT COUNT(*) FROM replay_markers').fetchone() == (0,)
        control.send(replace(request, identity=replace(request.identity, pid=789)))
        assert _receive_type(control, CommandResult) == result
        for changed in (
            replace(request, arguments={**request.arguments, 'operator_label': 'other uid=501'}),
            replace(
                request, arguments={**request.arguments, 'operator_label': 'operator uid=502'}
            ),
            replace(request, arguments={**request.arguments, 'reason': 'different recovery'}),
        ):
            control.send(replace(changed, identity=replace(request.identity, pid=789)))
            conflict = _receive_type(control, CommandResult)
            assert not conflict.ok
            assert 'disposition identity has conflicting content' in conflict.message
        with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
            assert connection.execute('SELECT group_generation FROM groups').fetchone() == (11,)
            assert connection.execute(
                'SELECT COUNT(*) FROM audit WHERE command = ? AND disposition_id = ?',
                (command, 'pending-disposition'),
            ).fetchone() == (1,)
            revision = connection.execute('SELECT revision FROM group_state').fetchone()
            assert revision is not None
        intervening = replace(
            request,
            arguments={
                **request.arguments,
                'disposition_id': 'intervening-disposition',
                'reserved_generation': 12,
                'expected_revision': revision[0],
                'reason': 'subsequent recovery',
            },
        )
        control.send(intervening)
        second = _receive_type(control, CommandResult)
        assert second.ok, second.message
        assert second.result['group_generation'] == 12
        control.send(replace(request, identity=replace(request.identity, pid=789)))
        stale = _receive_type(control, CommandResult)
        assert not stale.ok
        assert 'intervening disposition' in stale.message
        with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
            assert connection.execute('SELECT group_generation FROM groups').fetchone() == (12,)
            assert connection.execute(
                "SELECT COUNT(*) FROM audit WHERE command IN ('replay', 'reset-to-latest')"
            ).fetchone() == (2,)
        assert calls
        assert set(calls) == {'sg-event-daemon:source:events'}


def test_source_component_loop_drives_range_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = _runtime_site(_group('local', worker='default'))
    source_config = replace(
        site.sources['events'],
        fetch_interval_secs=0.001,
        visibility_overlap_secs=0.001,
    )
    site = replace(
        site,
        sources=MappingProxyType({'events': source_config}),
    )
    spec = build_worker_specs(site)[0]
    fingerprint = stream_fingerprint(
        site.server,
        source_config.credentials,
        site.credentials[source_config.credentials].script_name,
    )
    database_path = _source_database_path(tmp_path)
    with SourceState.open(
        database_path,
        writes=write_context_for(database_path),
        create=True,
        site=site.name,
        source='events',
        stream_fingerprint=fingerprint,
        source_generation='source-generation-1',
        visibility_overlap_secs=source_config.visibility_overlap_secs,
    ) as state:
        state.initialize(100, incarnation='source-generation-1')
        with open_group_on_source(
            database_path,
            writes=write_context_for(database_path),
            site=site.name,
            site_server_url=site.server,
            source='events',
            group='local',
            group_generation=7,
            ordering='eager',
            stream_fingerprint=fingerprint,
            source_generation='source-generation-1',
            verified_incarnation='source-generation-1',
            start_mode='initialize',
            initial_cursor=100,
            initial_delivery_floor=100,
            initial_last_delivered_id=100,
        ):
            pass

    channel = GroupChannel(
        group='local',
        ordering='eager',
        max_events=source_config.queue_max_events,
        max_bytes=source_config.queue_max_bytes,
    )
    fanout = EventFanout()
    fanout.attach(channel)
    monkeypatch.setattr(worker_module, 'EventFanout', lambda: fanout)
    handle = _RangeCloseProviderHandle()

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _RangeCloseProviderHandle:
        del site
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return handle

    monkeypatch.setattr(
        worker_module,
        'load_provider',
        _provider_loader(
            Provider(
                name='shotgun-api3',
                capabilities=ProviderCapabilities(
                    sync=True,
                    async_=False,
                ),
                _sync_factory=factory,
                _async_factory=None,
            )
        ),
    )
    messages: list[object] = []
    errors: list[Exception] = []
    host = CheckpointHost(
        interval_secs=60,
        synchronous='NORMAL',
        on_failure=lambda reason: errors.append(RuntimeError(reason)),
    )
    component = worker_module._SourceComponent(
        checkpoint_host=host,
        assignment=_local_assignment(spec, 'local', 'resume'),
        config=_runtime_config(tmp_path, site),
        control=cast('WorkerControl', SimpleNamespace(send=messages.append)),
        health_cache=worker_module._ComponentHealthCache(
            site='main',
            group_sources=MappingProxyType({'local': 'events'}),
        ),
        source='events',
        spec=spec,
    )

    def range_closed() -> bool:
        with closing(sqlite3.connect(database_path)) as connection:
            return (
                connection.execute(
                    "SELECT 1 FROM discovery_ranges WHERE range_state = 'closed'"
                ).fetchone()
                is not None
            )

    thread = _start_disposition_component(component, host, errors)
    try:
        _wait_for_source_component_progress(
            handle._recheck_returned.is_set,
            errors,
            attempts=2_000,
        )
        _wait_for_source_component_progress(
            range_closed,
            errors,
            attempts=10_000,
            pump=lambda: messages.extend(channel.drain()),
        )
    finally:
        primary_error = sys.exception()
        component._request_stop()
        thread.join(2.0)
        if primary_error is not None:
            if thread.is_alive():
                primary_error.add_note('source component thread remained alive after cleanup')
            if errors:
                primary_error.add_note(f'source component errors during cleanup: {errors!r}')
        else:
            assert not thread.is_alive()
            # This raw component thread bypasses WorkerRuntime's cooperative-stop boundary.
            assert errors == [] or (
                len(errors) == 1 and isinstance(errors[0], WriteLockStoppedError)
            )

    with SourceState.open(
        database_path,
        writes=write_context_for(database_path),
        create=True,
        site=site.name,
        source='events',
        stream_fingerprint=fingerprint,
        source_generation='source-generation-1',
        visibility_overlap_secs=source_config.visibility_overlap_secs,
    ) as state:
        ranges = state.snapshot().ranges
        assert ranges
        assert all(item.state == 'closed' for item in ranges)


def test_source_component_pending_disposition_checks_configured_stream_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = _pending_group_runtime(tmp_path)
    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        runtime,
        control,
    ):
        _receive_typed_health(control, lambda update: len(update.groups) == 1)
        source = runtime._components['source:events']
        assert isinstance(source, _SourceComponent)
        site = config.sites['main']
        changed_site = replace(
            site,
            credentials=MappingProxyType(
                {'source': replace(site.credentials['source'], script_name='changed-script')}
            ),
        )
        source._config = replace(config, sites=MappingProxyType({'main': changed_site}))
        with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection, connection:
            before = tuple(connection.iterdump())
        control.send(_pending_group_command(snapshot, 'replay'))
        result = _receive_type(control, CommandResult)
        assert not result.ok
        with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
            assert tuple(connection.iterdump()) == before
            assert connection.execute(
                'SELECT group_generation, lifecycle FROM groups WHERE group_name = ?', ('local',)
            ).fetchone() == (7, 'readded-parked')
            assert connection.execute(
                "SELECT COUNT(*) FROM audit WHERE command = 'replay'"
            ).fetchone() == (0,)
        assert runtime.fatal_reason is None


def test_source_component_pending_disposition_refuses_held_group_lock(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = _pending_group_runtime(tmp_path)
    with (
        _open_group_health_state(_source_database_path(tmp_path)) as owner,
        _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
            runtime,
            control,
        ),
    ):
        _receive_typed_health(control, lambda update: len(update.groups) == 1)
        started = time.monotonic()
        control.send(_pending_group_command(snapshot, 'replay'))
        result = _receive_type(control, CommandResult)
        assert not result.ok
        assert result.result == {'code': 'group-busy'}
        assert time.monotonic() - started < 1.0
        assert owner.snapshot() == snapshot
        assert runtime.fatal_reason is None


def test_source_component_pending_disposition_uses_operator_database_budget(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = _pending_group_runtime(tmp_path)
    config = replace(config, daemon=replace(config.daemon, health_interval_secs=0.05))
    operator_budgets: list[float] = []
    original_transaction = worker_module.SourceWriteLock.transaction

    def transaction(
        lock: SourceWriteLock,
        connection: sqlite3.Connection,
        **options: Unpack[_TransactionOptions],
    ) -> AbstractContextManager[sqlite3.Connection]:
        budget = options.get('budget_secs')
        if budget is not None:
            operator_budgets.append(budget)
        return original_transaction(lock, connection, **options)

    monkeypatch.setattr(worker_module.SourceWriteLock, 'transaction', transaction)
    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        runtime,
        control,
    ):
        _receive_typed_health(
            control,
            lambda update: (
                len(update.groups) == 1 and any(item.phase == 'wait' for item in update.sources)
            ),
        )
        with closing(sqlite3.connect(_source_database_path(tmp_path))) as blocker:
            blocker.execute('BEGIN IMMEDIATE')
            try:
                started = time.monotonic()
                control.send(_pending_group_command(snapshot, 'replay'))
                result = _receive_type(control, CommandResult)
                elapsed = time.monotonic() - started
            finally:
                blocker.rollback()
        assert not result.ok
        assert result.result == {'code': 'source-database-busy'}
        assert operator_budgets == [2.0]
        assert elapsed >= 1.8
        assert runtime.fatal_reason is None
        control.send(_pending_group_command(snapshot, 'replay'))
        assert _receive_type(control, CommandResult).ok


def test_source_control_saturation_stops_runtime_with_bounded_reason(tmp_path: Path) -> None:
    source_component, _cache = _source_health_component(
        tmp_path,
        health_interval_secs=0.01,
    )

    class _GrantServer(_SourceHealthServer):
        @staticmethod
        def install_grant(grant: EventPeerGrant) -> EventPeerReady:
            return EventPeerReady(
                source=grant.source,
                group=grant.group,
                dependent_spawn_instance=grant.dependent_spawn_instance,
            )

    peer_socket, sender_socket = socket.socketpair()
    peer = WorkerControl(peer_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    sender = WorkerControl(sender_socket, max_frame_bytes=131_072, send_deadline_secs=0.05)
    vars(source_component)['_control'] = sender
    request_grant = cast('_GrantRequester', source_component)._request_grant
    request_grant(
        EventPeerGrant(
            source='events',
            group='remote',
            dependent_spawn_instance='dependent-saturation-1',
            capability='saturated-source-grant',
        )
    )

    def _action() -> None:
        _service_source_health(
            source_component,
            _GrantServer(),
            cast('SourcePoller', object()),
        )

    spec = WorkerSpec(
        worker_id='["source","saturation"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='source-saturation-1',
        source_factory=lambda _name, _slots, _host: _ControlSendComponent(_action),
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )
    _saturate_worker_control_sender(sender)
    try:
        runtime.start()
        assert runtime.wait() == ()
        assert runtime.fatal_reason == 'worker-control-send-timeout'
    finally:
        runtime.request_stop('test-cleanup')
        runtime.wait()
        peer.close()
        sender.close()


def test_source_disposition_uses_current_incarnation_after_adoption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = _pending_group_runtime(tmp_path)
    path = _source_database_path(tmp_path)
    with SourceState.open(
        path,
        writes=write_context_for(path),
        site='main',
        source='events',
        stream_fingerprint=snapshot.stream_fingerprint,
        source_generation=snapshot.source_generation,
    ) as source:
        source._database.connection.execute(
            "UPDATE source_meta SET condition = 'stream-identity-changed'"
        )
        reservation = source.reserve_stream_adoption(
            command_fingerprint='adopt-new-incarnation',
            reason='same credential new stream',
        )
        source.adopt_stream(
            adoption_id=reservation.adoption_id,
            command_fingerprint=reservation.command_fingerprint,
            new_stream_fingerprint=snapshot.stream_fingerprint,
            reason='same credential new stream',
            groups_parked=True,
        )
        source.initialize(41)
        assert source.snapshot().stream_incarnation != snapshot.verified_incarnation
    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        _runtime,
        control,
    ):
        _receive_typed_health(control, lambda update: len(update.groups) == 1)
        control.send(_pending_group_command(snapshot, 'reset-to-latest'))
        result = _receive_type(control, CommandResult)
        assert result.ok, result.message
        with closing(sqlite3.connect(path)) as connection, connection:
            assert connection.execute(
                'SELECT verified_incarnation, group_generation FROM groups'
            ).fetchone() == (reservation.adoption_id, 11)


def test_source_health_refresh_fractional_interval_advances_past_now(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component, _cache = _source_health_component(tmp_path, health_interval_secs=0.1)
    server = _SourceHealthServer()
    provider = _runtime_provider()
    clock = SimpleNamespace(now=0.1)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_coordination_source(tmp_path / 'source-fractional-health.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        snapshot_calls = 0
        real_snapshot = state.snapshot

        def counted_snapshot() -> SourceSnapshot:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return real_snapshot()

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        with _source_health_poller(state, provider) as poller:
            _service_source_health(component, server, poller)
            assert vars(component)['_next_health_refresh_at'] == pytest.approx(0.2)

            clock.now = 0.5
            _service_source_health(component, server, poller)
            assert snapshot_calls == 2
            assert vars(component)['_next_health_refresh_at'] > clock.now
            assert vars(component)['_next_health_refresh_at'] == pytest.approx(0.6)

            clock.now = 0.51
            _service_source_health(component, server, poller)

    assert snapshot_calls == 2


def test_source_health_refresh_later_dirty_call_supersedes_mutation_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component, cache = _source_health_component(tmp_path, health_interval_secs=1.0)
    server = _SourceHealthServer()
    provider = _runtime_provider()
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 0.0)
    with _open_coordination_source(tmp_path / 'source-dirty-order.db') as state:
        snapshot_calls = 0
        real_snapshot = state.snapshot

        def counted_snapshot() -> SourceSnapshot:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return real_snapshot()

        def initialize(_surface: object) -> SourceSnapshot:
            return state.initialize(100, incarnation='incarnation-1')

        def advance_observations(_surface: object) -> None:
            state.commit_observations((101,), successful_clock=2.0)

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        with _source_health_poller(state, provider) as poller:
            _service_source_health(component, server, poller)
            calls = (
                component._mailbox._submit(initialize),
                component._mailbox._submit(advance_observations),
            )

            _service_source_health(component, server, poller)
            assert all(call._complete.is_set() and call._error is None for call in calls)
            health = cache._snapshot()._sources[0]
            assert health.frontier == 101
            assert health.successful_clock == 2.0

    assert snapshot_calls == 3


def test_source_health_refresh_uses_interval_deadlines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component, cache = _source_health_component(tmp_path, health_interval_secs=1.0)
    server = _SourceHealthServer()
    provider = _runtime_provider()
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_coordination_source(tmp_path / 'source-health.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        snapshot_calls = 0
        real_snapshot = state.snapshot

        def counted_snapshot() -> SourceSnapshot:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return real_snapshot()

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        with _source_health_poller(state, provider) as poller:
            _service_source_health(component, server, poller)
            initial = cache._snapshot()._sources
            assert snapshot_calls == 1
            assert vars(component)['_next_health_refresh_at'] == 1.0

            for pass_number in range(1, 100):
                clock.now = pass_number / 100
                _service_source_health(component, server, poller)

            assert snapshot_calls == 1
            refreshed = cache._snapshot()._sources
            assert len(refreshed) == len(initial) == 1
            assert (
                replace(
                    refreshed[0],
                    heartbeat_at=initial[0].heartbeat_at,
                )
                == initial[0]
            )

            clock.now = 1.0
            _service_source_health(component, server, poller)
            assert snapshot_calls == 2
            assert vars(component)['_next_health_refresh_at'] == 2.0

            clock.now = 4.25
            _service_source_health(component, server, poller)
            assert snapshot_calls == 3
            assert vars(component)['_next_health_refresh_at'] == 5.0


def test_source_mutation_snapshot_refresh_avoids_second_snapshot_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component, cache = _source_health_component(tmp_path, health_interval_secs=1.0)
    server = _SourceHealthServer()
    provider = _runtime_provider()
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_coordination_source(tmp_path / 'source-mutation.db') as state:
        snapshot_calls = 0
        real_snapshot = state.snapshot

        def counted_snapshot() -> SourceSnapshot:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return real_snapshot()

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        with _source_health_poller(state, provider) as poller:
            _service_source_health(component, server, poller)
            assert snapshot_calls == 1

            def initialize(_surface: object) -> SourceSnapshot:
                return state.initialize(100, incarnation='incarnation-1')

            call = component._mailbox._submit(initialize)
            clock.now = 0.1

            _service_source_health(component, server, poller)

    assert call._complete.is_set()
    assert snapshot_calls == 2
    assert cache._snapshot()._sources[0].initialized


def test_source_owner_disposition_rejects_nonint_position(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = _pending_group_runtime(tmp_path)
    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        runtime,
        control,
    ):
        _receive_typed_health(control, lambda update: len(update.groups) == 1)
        source = runtime._components['source:events']
        assert isinstance(source, _SourceComponent)
        command = _pending_group_command(snapshot, 'replay')
        command = replace(command, arguments={**command.arguments, 'from_event_id': 1.5})
        with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
            before = connection.execute(
                "SELECT * FROM groups WHERE group_name = 'local'"
            ).fetchone()
            with pytest.raises(ValueError, match='replay position must be a nonnegative integer'):
                source._execute_group_disposition(command)
            assert (
                connection.execute("SELECT * FROM groups WHERE group_name = 'local'").fetchone()
                == before
            )


def test_source_owner_disposition_retry_with_new_pid_after_restart(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = _pending_group_runtime(tmp_path)
    pending = _pending_group_command(snapshot, 'reset-to-latest')
    command = replace(
        pending,
        arguments={**pending.arguments, 'operator_label': 'operator uid=501'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        _runtime,
        control,
    ):
        _receive_typed_health(control, lambda update: len(update.groups) == 1)
        control.send(command)
        first = _receive_type(control, CommandResult)
        assert first.ok, first.message

    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        _runtime,
        control,
    ):
        _receive_typed_health(control, lambda update: len(update.groups) == 1)
        control.send(replace(command, identity=replace(command.identity, pid=789)))
        repeated = _receive_type(control, CommandResult)
        assert repeated.ok, repeated.message
        assert repeated.result == first.result
    with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
        assert connection.execute('SELECT group_generation FROM groups').fetchone() == (11,)
        assert connection.execute(
            "SELECT COUNT(*) FROM audit WHERE command = 'reset-to-latest'"
        ).fetchone() == (1,)


def test_source_registration_outlives_failing_execution_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component, cache = _source_health_component(tmp_path, health_interval_secs=1.0)
    operations: list[str] = []

    class _Resource:
        _writes: WriteContext | None = None

        _error: str | None = None
        _name = ''

        def __init__(self, **_options: object) -> None:
            pass

        def close(self) -> None:
            assert cache._snapshot()._watchdogs
            watchdog = component._watchdog.sample()
            assert watchdog.phase == 'shutdown'
            assert watchdog.deadline_at is not None
            operations.append(f'close:{self._name}')
            if self._writes is not None:
                self._writes.lock.close()
            if self._error is not None:
                raise RuntimeError(self._error)

    state = _Resource()
    state._name = 'state'

    def open_state(*_args: object, **_options: object) -> _Resource:
        state._writes = cast('WriteContext', _options['writes'])
        return state

    def load_test_provider(*_args: object, **_options: object) -> object:
        return object()

    def publish_health(*_args: object, **_options: object) -> None:
        return None

    class _Server(_Resource):
        _error = 'server-close-failed'
        _name = 'server'
        start = staticmethod(partial(operations.append, 'start:server'))

    class _Poller(_Resource, AbstractContextManager['_Poller']):
        _error = 'poller-close-failed'
        _name = 'poller'

        def __exit__(
            self,
            _exc_type: type[BaseException] | None,
            _exc_value: BaseException | None,
            _traceback: object,
        ) -> None:
            self.close()

    original_deregister = cache._deregister_watchdog

    def deregister(
        *,
        kind: WatchdogKind,
        identity: str,
        worker_instance: str,
        token: str,
    ) -> None:
        operations.append('deregister')
        original_deregister(
            kind=kind,
            identity=identity,
            worker_instance=worker_instance,
            token=token,
        )

    monkeypatch.setattr(cache, '_deregister_watchdog', deregister)
    monkeypatch.setattr(
        worker_module,
        'SourceState',
        SimpleNamespace(open=open_state),
    )
    monkeypatch.setattr(worker_module, 'EventServer', _Server)
    monkeypatch.setattr(worker_module, 'SourcePoller', _Poller)
    monkeypatch.setattr(worker_module, 'load_provider', load_test_provider)
    monkeypatch.setattr(type(component), '_publish_health', publish_health)
    component._stop.set()

    run_component = cast(
        'Callable[[object], None]',
        vars(type(component))['_run'],
    )
    with pytest.raises(RuntimeError, match='poller-close-failed'):
        run_component(component)

    assert cache._snapshot()._watchdogs
    cast('Callable[[object], None]', vars(type(component))['_close'])(component)

    assert operations == [
        'start:server',
        'close:poller',
        'close:server',
        'close:state',
        'deregister',
    ]
    assert cache._snapshot()._watchdogs == ()


def test_source_registration_outlives_partial_construction_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component, cache = _source_health_component(tmp_path, health_interval_secs=1.0)
    operations: list[str] = []

    class _Resource:
        _writes: WriteContext | None = None

        def __init__(self, name: str, error: str | None = None) -> None:
            self._error = error
            self._name = name

        def close(self) -> None:
            assert cache._snapshot()._watchdogs
            watchdog = component._watchdog.sample()
            assert watchdog.phase == 'shutdown'
            assert watchdog.deadline_at is not None
            operations.append(f'close:{self._name}')
            if self._writes is not None:
                self._writes.lock.close()
            if self._error is not None:
                raise RuntimeError(self._error)

    state = _Resource('state')

    def open_state(*_args: object, **_options: object) -> _Resource:
        state._writes = cast('WriteContext', _options['writes'])
        return state

    def load_test_provider(*_args: object, **_options: object) -> object:
        return object()

    def fail_server(**_options: object) -> object:
        raise RuntimeError('server-construction-failed')

    original_deregister = cache._deregister_watchdog

    def deregister(
        *,
        kind: WatchdogKind,
        identity: str,
        worker_instance: str,
        token: str,
    ) -> None:
        operations.append('deregister')
        original_deregister(
            kind=kind,
            identity=identity,
            worker_instance=worker_instance,
            token=token,
        )

    monkeypatch.setattr(cache, '_deregister_watchdog', deregister)
    monkeypatch.setattr(
        worker_module,
        'SourceState',
        SimpleNamespace(open=open_state),
    )
    monkeypatch.setattr(worker_module, 'EventServer', fail_server)
    monkeypatch.setattr(worker_module, 'load_provider', load_test_provider)

    run_component = cast(
        'Callable[[object], None]',
        vars(type(component))['_run'],
    )
    with pytest.raises(RuntimeError, match='server-construction-failed'):
        run_component(component)

    assert cache._snapshot()._watchdogs
    cast('Callable[[object], None]', vars(type(component))['_close'])(component)

    assert operations == ['close:state', 'deregister']
    assert cache._snapshot()._watchdogs == ()


def test_source_retry_first_health_update_degrades_and_recovers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    component, cache = _source_health_component(
        tmp_path,
        health_interval_secs=10.0,
    )
    server, site = (
        _SourceHealthServer(),
        _runtime_site(_group('local', worker='default')),
    )
    source_config = site.sources['events']

    class _Handle(_RuntimeProviderHandle):
        def __init__(self) -> None:
            super().__init__(lambda: 41)
            self._responses: deque[list[dict[str, object]] | Exception] = deque(
                (RuntimeError('controlled provider outage'), [])
            )

        def find(
            self,
            entity_type: str,
            filters: object,
            fields: list[str] | None = None,
            **options: Unpack[_FindOptions],
        ) -> list[dict[str, object]]:
            if not isinstance(filters, list):
                return super().find(entity_type, filters, fields, **options)
            if not filters:
                empty_filters: list[object] = []
                return super().find(entity_type, empty_filters, fields, **options)
            response = self._responses.popleft()
            if isinstance(response, Exception):
                raise response
            return response

    handle = _Handle()

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _Handle:
        assert site == 'https://example.test'
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return handle

    provider = Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        _sync_factory=factory,
        _async_factory=None,
    )
    pollers: list[SourcePoller] = []

    class _ServiceWaiter:
        @staticmethod
        def set() -> None:
            return None

        @staticmethod
        def wait(seconds: float) -> bool:
            assert seconds >= 0
            _service_source_health(component, server, pollers[0])
            return False

    updates: list[HealthUpdate] = []

    def publish(worker: WorkerHealth) -> None:
        snapshot = cache._snapshot()
        updates.append(
            HealthUpdate(
                worker=replace(worker, monitor_alive=True),
                watchdogs=snapshot._watchdogs,
                sources=snapshot._sources,
                groups=snapshot._groups,
                event_sockets=snapshot._event_sockets,
            )
        )

    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='site-spawn-1',
        source_factory=lambda _name, _slots, _host: cast('_WorkerComponent', component),
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=publish,
        health_interval_secs=10.0,
        checkpoint_interval_secs=10.0,
        shutdown_deadline_secs=10.0,
        max_concurrent_catchup_readers=1,
        clock=lambda: clock.now,
    )
    publish_monitor_health = cast(
        'Callable[[WorkerRuntime, float], None]',
        vars(type(runtime))['_publish_monitor_health'],
    )
    with _open_coordination_source(tmp_path / 'source-retry-dirty.db') as state:
        state.initialize(41, incarnation='incarnation-1')
        poller = SourcePoller(
            state=state,
            fanout=EventFanout(),
            provider=provider,
            site_url=site.server,
            credential=site.credentials[source_config.credentials],
            config=source_config,
            max_ipc_frame_bytes=8_388_608,
            source_generation='source-generation-1',
            waiter=_ServiceWaiter(),
            random_value=lambda: 0.5,
            watchdog=component._watchdog,
            internal_deadline_secs=192.0,
            wait_tolerance_secs=50.0,
            worker_instance='site-spawn-1',
            checkpoint_state=lambda: 'idle',
            successful_clock=_SuccessfulQueryClock(clock=lambda: clock.now),
            health_dirty=lambda: setattr(component, '_source_health_dirty', True),
        )
        pollers.append(poller)
        with poller:
            _service_source_health(component, server, poller)
            publish_monitor_health(runtime, 0.0)
            ready = updates[-1]
            assert ready.sources[0].retry_attempt == 0

            clock.now = 0.1
            poller.run(max_cycles=1)
            publish_monitor_health(runtime, 0.1)
            degraded = updates[-1]
            degraded_status = aggregate_status(
                sources=degraded.sources,
                groups=degraded.groups,
                workers=(degraded.worker,),
                now=0.1,
                stale_after_secs=1.0,
            )
            assert (
                degraded.sources[0].retry_attempt,
                degraded.sources[0].next_retry_secs,
                degraded.sources[0].last_error,
            ) == (1, 1.0, 'controlled provider outage')
            assert 'source-degraded' in degraded_status.conditions
            assert degraded_status.liveness
            assert not degraded_status.readiness

            clock.now = 0.2
            poller.run(max_cycles=1)
            publish_monitor_health(runtime, 0.2)
            recovered = updates[-1]
            recovered_status = aggregate_status(
                sources=recovered.sources,
                groups=recovered.groups,
                workers=(recovered.worker,),
                now=0.2,
                stale_after_secs=1.0,
            )
            assert (
                recovered.sources[0].retry_attempt,
                recovered.sources[0].next_retry_secs,
                recovered.sources[0].last_error,
            ) == (0, None, None)
            assert 'source-degraded' not in recovered_status.conditions
            assert recovered_status.liveness
            assert recovered_status.readiness


def test_source_shutdown_uses_original_worker_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    component, _cache = _source_health_component(
        tmp_path,
        health_interval_secs=1.0,
    )
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='site-spawn-1',
        source_factory=lambda _name, _slots, _host: cast('_WorkerComponent', component),
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=1.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=10.0,
        max_concurrent_catchup_readers=1,
        clock=lambda: clock.now,
    )
    vars(runtime)['_components'] = {'source:events': component}
    vars(runtime)['_construction_order'] = ['source:events']

    runtime.request_stop('controlled-stop')
    original_deadline = cast('float', runtime._shutdown_deadline_at)
    clock.now = 150.0
    assert isinstance(component, _SourceComponent)
    component._shutdown_resources(None, None, None, None)

    sample = component._watchdog.sample()
    assert original_deadline == 110.0
    assert sample.phase == 'shutdown'
    assert sample.deadline_at == original_deadline


def test_start_source_runtime_attempts_all_cleanup_when_runtime_stop_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_socket, child_socket = socket.socketpair()
    endpoint = SourceEndpointReady(
        source='events',
        source_generation='source-generation-1',
        path='/private/events.sock',
    )
    grant = EventPeerGrant(
        source='events',
        group='local',
        dependent_spawn_instance='dependent-spawn-1',
        capability='remote-capability',
    )
    runtime = cast(
        'WorkerRuntime',
        SimpleNamespace(start=lambda: None, _threads={}, _monitor_thread=None),
    )
    drain: list[tuple[threading.Event, threading.Thread]] = []
    cleanup_attempts: list[str] = []
    responses = iter((endpoint, EventPeerReady('events', 'local', 'dependent-spawn-1')))
    original_start_drain = _start_control_drain
    original_stop_drain = _stop_control_drain

    def receive(_control: WorkerControl, _expected: type[object]) -> object:
        return next(responses)

    def build_runtime(*_args: object) -> WorkerRuntime:
        return runtime

    def start_drain(control: WorkerControl) -> tuple[threading.Event, threading.Thread]:
        owned = original_start_drain(control)
        drain.append(owned)
        return owned

    def fail_session(**_kwargs: object) -> NoReturn:
        raise RuntimeError('injected session acquisition failure')

    def fail_runtime_stop(_runtime: WorkerRuntime | None) -> NoReturn:
        cleanup_attempts.append('runtime')
        raise AssertionError('injected runtime cleanup failure')

    module = sys.modules[__name__]
    monkeypatch.setattr(socket, 'socketpair', lambda: (parent_socket, child_socket))
    monkeypatch.setattr(module, 'build_worker_runtime', build_runtime)
    monkeypatch.setattr(module, '_receive_type', receive)
    monkeypatch.setattr(module, '_start_control_drain', start_drain)
    monkeypatch.setattr(module, '_SourceRuntimeSession', fail_session)
    monkeypatch.setattr(module, '_stop_runtime', fail_runtime_stop)
    try:
        with pytest.raises(RuntimeError, match='injected session acquisition failure') as caught:
            _start_source_runtime(
                cast('Config', object()),
                cast('WorkerSpec', SimpleNamespace(sources=('events',))),
                cast('WorkerAssignment', object()),
                (grant,),
            )

        assert any('injected runtime cleanup failure' in note for note in caught.value.__notes__)
        assert cleanup_attempts == ['runtime']
        assert len(drain) == 1
        drain_stop, drain_thread = drain[0]
        assert drain_stop.is_set()
        assert not drain_thread.is_alive()
        assert parent_socket.fileno() == -1
        assert child_socket.fileno() == -1
    finally:
        if drain:
            original_stop_drain(*drain[0])
        parent_socket.close()
        child_socket.close()


def test_start_source_runtime_releases_acquisitions_when_readiness_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    runtime_thread = threading.Thread(target=release.wait)
    parent_socket, child_socket = socket.socketpair()

    def start() -> None:
        runtime_thread.start()

    def request_stop(_reason: str) -> None:
        release.set()

    def wait() -> tuple[str, ...]:
        runtime_thread.join(1.0)
        return ()

    runtime = cast(
        'WorkerRuntime',
        SimpleNamespace(
            start=start,
            request_stop=request_stop,
            wait=wait,
            _threads={'source:events': runtime_thread},
            _monitor_thread=None,
        ),
    )

    def build_runtime(*_args: object) -> WorkerRuntime:
        return runtime

    def fail_receive(_control: WorkerControl, _expected: type[object]) -> NoReturn:
        raise RuntimeError('injected readiness failure')

    def ignore_monitor(_runtime: WorkerRuntime) -> None:
        return None

    module = sys.modules[__name__]
    monkeypatch.setattr(socket, 'socketpair', lambda: (parent_socket, child_socket))
    monkeypatch.setattr(module, 'build_worker_runtime', build_runtime)
    monkeypatch.setattr(module, '_receive_type', fail_receive)
    monkeypatch.setattr(module, '_assert_monitor_stopped', ignore_monitor)
    try:
        with pytest.raises(RuntimeError, match='injected readiness failure'):
            _start_source_runtime(
                cast('Config', object()),
                cast('WorkerSpec', SimpleNamespace(sources=('events',))),
                cast('WorkerAssignment', object()),
                (),
            )

        assert not runtime_thread.is_alive()
        assert parent_socket.fileno() == -1
        assert child_socket.fileno() == -1
    finally:
        release.set()
        if runtime_thread.ident is not None:
            runtime_thread.join(1.0)
        parent_socket.close()
        child_socket.close()


@pytest.mark.parametrize('reconciled', [False, True])
def test_surviving_source_admits_resuming_initialized_peer(
    tmp_path: Path, reconciled: bool
) -> None:
    path = _source_database_path(tmp_path)
    with _open_coordination_source(path) as state:
        state.initialize(41, incarnation='incarnation-1')
        surface = _initializing_source_surface(state)
        with open_group_on_source(
            path,
            writes=write_context_for(path),
            site='main',
            site_server_url='https://example.test',
            source='events',
            group='local',
            group_generation=7,
            ordering='eager',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='initialize',
            initial_cursor=41,
            initial_delivery_floor=41,
            initial_last_delivered_id=41,
        ):
            pass
        request = GroupBootstrapRequest(
            source='events', group='local', group_generation=7, start_mode='resume'
        )
        if reconciled:
            surface.observe_group(request)
        before = state._database.connection.total_changes
        observation = surface.observe_group(request)
        assert isinstance(observation, GroupBootstrapObservation)
        assert observation.captured_frontier == 41
        assert surface.observe_group(request) == observation
        assert state._database.connection.total_changes == before
        with pytest.raises(ValueError, match='bootstrap request conflicts'):
            surface.observe_group(replace(request, start_mode='initialize'))


@pytest.mark.parametrize('halted', [False, True])
def test_surviving_source_restores_progress_before_replacement_join(
    tmp_path: Path, halted: bool
) -> None:
    path = _source_database_path(tmp_path)
    with _open_coordination_source(path) as source:
        source.initialize(41, incarnation='incarnation-1')
        _bootstrap_coordination_group(path, ordering='strict')
        source.commit_observations((42, 43, 44), successful_clock=1.0)
        strict = StrictBuffer(max_events=20, max_bytes=262_144, group_holdbacks={'local': 300.0})
        fanout = EventFanout()
        coordinator = JoinCoordinator(
            state=source, fanout=fanout, source_generation='source-1', strict_retention=strict
        )

        def detach(group: str, *, reason: str) -> None:
            fanout.detach(group, reason=reason)
            strict.detach_group(group, reason=reason)

        surface = worker_module._SourceJoinSurface(
            bootstrap_retry_secs=0.05,
            coordinator=coordinator,
            poller=cast('SourcePoller', SimpleNamespace(event_channel_lost=detach)),
            state=source,
            inventory={
                'local': ActiveSourceGroupAssignment(
                    source='events', group='local', group_generation=7, start_mode='initialize'
                )
            },
            strict_buffer=strict,
        )
        channel = GroupChannel(group='local', ordering='strict', max_events=20, max_bytes=262_144)
        target = surface.request(
            JoinRequest(
                JoinAttemptIdentity(
                    site='main',
                    source='events',
                    group='local',
                    join_kind='strict',
                    group_generation=7,
                    attempt_token=os.urandom(16).hex(),
                )
            ),
            channel,
        )
        assert isinstance(target, JoinTarget)
        assert isinstance(surface.confirm(JoinConfirm(target.identity)), Attached)
        index_revision = source.group_policies()[0]
        with source._writes.transaction(source._database.connection):
            source._database.connection.execute(
                'UPDATE group_state SET last_delivered_id = 44, cursor = 44, parked = ?, '
                "revision = revision + 1 WHERE group_name = 'local'",
                (int(halted),),
            )
        assert source.group_policies()[0] == index_revision
        request = GroupBootstrapRequest(
            source='events', group='local', group_generation=7, start_mode='resume'
        )
        before = source._database.connection.total_changes
        observation = surface.observe_group(request)
        assert isinstance(observation, GroupBootstrapObservation)
        assert observation.captured_frontier == 44
        assert not channel.status.attached
        with pytest.raises(JoinProtocolError):
            surface.confirm(JoinConfirm(target.identity))
        watermark = strict._watermark('local')
        assert watermark._group_generation == 7
        assert watermark._delivery_floor == 41
        assert watermark._last_delivered_id == 44
        assert strict._detached_halts['local'] is halted
        assert surface.observe_group(request) == observation
        assert strict._watermark('local') == watermark
        assert source._database.connection.total_changes == before
        with pytest.raises(ValueError, match='bootstrap request conflicts'):
            surface.observe_group(replace(request, start_mode='initialize'))


@pytest.mark.parametrize('blocked', ['parked', 'stopping'])
def test_setup_retry_rejects_parked_or_stopping_lane(blocked: str) -> None:
    component, resources = _lifecycle_probe(
        'sync',
        durable_status='parked' if blocked == 'parked' else 'running',
        setup_failure=RuntimeError('setup unavailable'),
    )
    lifecycle_factory = worker_module._LaneLifecycle
    lifecycle = lifecycle_factory(
        _channel=resources._channel,
        _setup_attempted=True,
        _setup_failure=RuntimeError('setup unavailable'),
    )
    vars(component)['_lifecycle'] = lifecycle
    vars(component)['_lane'] = resources._lane
    if blocked == 'stopping':
        component._stop.set()
    component_type = type(component)
    callbacks_allowed = component_type._callback_gate_snapshot
    execute_retry = component_type._execute_retry
    command = OperatorCommand(
        request_id='blocked-setup-retry',
        command='retry',
        arguments={'site': 'main', 'group': 'local'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=123),
    )

    with pytest.raises(ValueError, match='setup retry requires a running group and lane'):
        execute_retry(
            component,
            resources._state,
            resources._state.snapshot(),
            command,
        )
    vars(lifecycle)['_setup_attempted'] = False
    vars(lifecycle)['_setup_failure'] = None
    assert not callbacks_allowed(
        component,
        cast('_LaneResources', resources),
        lifecycle,
    )
    assert resources._lane._operations == []


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_operator_retry_recovers_setup_failure_and_preserves_callback_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    group = _group('local', worker='default', execution=execution)
    site = _runtime_site(group)
    healthy = replace(group, name='healthy')
    site = replace(site, groups=MappingProxyType({'local': group, 'healthy': healthy}))
    spec = build_worker_specs(site)[0]
    assignment = _local_assignment(spec, group.name, 'initialize')
    healthy_assignment = _local_assignment(spec, healthy.name, 'initialize')
    assignment = replace(
        assignment,
        group_assignments=(*assignment.group_assignments, *healthy_assignment.group_assignments),
        source_group_assignments=(
            *assignment.source_group_assignments,
            *healthy_assignment.source_group_assignments,
        ),
    )
    provider_state = _RestartProviderState()
    setup_handles: list[object] = []
    setup_threads: list[int] = []
    invoked: list[int] = []

    def setup(context: SetupContext) -> None:
        setup_handles.append(context.sg)
        setup_threads.append(threading.get_ident())
        if len(setup_handles) <= 2:
            raise RuntimeError('setup unavailable')

    async def setup_async(context: SetupContext) -> None:
        setup(context)

    def callback(context: EventContext) -> None:
        event_id = context.event['id']
        assert type(event_id) is int
        invoked.append(event_id)
        if len(invoked) == 1:
            raise RuntimeError('callback unavailable')

    async def callback_async(context: EventContext) -> None:
        callback(context)

    plugin = _restart_callback_plugin(
        [],
        callback_entered=threading.Event(),
        release_callback=threading.Event(),
        execution=execution,
    )
    plugin = replace(
        plugin,
        setup=setup_async if execution == 'async' else setup,
        callbacks=(
            replace(
                plugin.callbacks[0],
                callback=callback_async if execution == 'async' else callback,
            ),
        ),
    )

    def load(
        _site: SiteConfig, selected: GroupConfig, _provider: Provider
    ) -> tuple[PluginSpec, ...]:
        return (plugin,) if selected.name == group.name else ()

    monkeypatch.setattr(worker_module, 'load_plugins', load)
    with _running_concrete_runtime(
        monkeypatch,
        _runtime_config(tmp_path, site),
        spec,
        assignment,
        _restart_provider(provider_state, async_=execution == 'async'),
    ) as (runtime, control):
        _receive_typed_health(
            control,
            lambda item: any(
                health.group == 'local' and health.failure_kind == 'setup'
                for health in item.groups
            ),
        )
        lane = _runtime_lane(runtime, 'local')
        assert lane is not None

        for attempt in (2, 3):
            _assert_operator_retry(control, f'setup-retry-{attempt}')
            assert len(setup_handles) == attempt
            _receive_typed_health(
                control,
                lambda item, expected_attempt=attempt: (
                    any(
                        entry.group == 'local'
                        and (
                            entry.failure_kind == 'setup'
                            if expected_attempt == 2
                            else entry.join_phase == 'attached' and entry.failure_kind is None
                        )
                        for entry in item.groups
                    )
                    and any(
                        entry.group == 'healthy' and entry.join_phase == 'attached'
                        for entry in item.groups
                    )
                ),
            )

        provider_state._publish(42)
        _receive_typed_health(
            control,
            lambda item: any(entry.failure_kind == 'callback' for entry in item.groups),
        )
        provider_state._publish(43)
        _assert_operator_retry(control, 'callback-retry')
        _receive_typed_health(
            control,
            lambda item: any(
                entry.group == 'local' and entry.cursor == 43 for entry in item.groups
            ),
        )
        assert invoked == [42, 42, 43]
        assert len(setup_handles) == 3
        assert all(handle is setup_handles[0] for handle in setup_handles)
        assert len(set(setup_threads)) == 1
        assert setup_threads[0] != threading.get_ident()
        assert _runtime_lane(runtime, 'local') is lane
        assert runtime.fatal_reason is None


@pytest.mark.parametrize('outage', [False, True])
def test_source_empty_poll_and_retry_wait_preserve_worker_liveness(
    monkeypatch: pytest.MonkeyPatch,
    outage: bool,
    tmp_path: Path,
) -> None:
    site = replace(
        _runtime_site(_group('local', worker='default')),
        groups=MappingProxyType({}),
    )
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance=f'source-poll-{outage}',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )

    class _Handle(_RuntimeProviderHandle):
        def find(
            self,
            entity_type: str,
            filters: object,
            fields: list[str] | None = None,
            **options: Unpack[_FindOptions],
        ) -> list[dict[str, object]]:
            if not isinstance(filters, list):
                return super().find(entity_type, filters, fields, **options)
            if not filters:
                empty_filters: list[object] = []
                return super().find(entity_type, empty_filters, fields, **options)
            if outage:
                raise RuntimeError('provider outage')
            return []

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _Handle:
        assert site == 'https://example.test'
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return _Handle(lambda: 41)

    provider = Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        _sync_factory=factory,
        _async_factory=None,
    )
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        provider,
    ) as (runtime, parent):
        _receive_type(parent, SourceEndpointReady)
        update = _receive_typed_health(
            parent,
            lambda item: (
                bool(item.sources)
                and item.sources[0].initialized
                and item.sources[0].phase == 'wait'
                and ((item.sources[0].retry_attempt > 0) == outage)
            ),
        )
        source = update.sources[0]
        status = aggregate_status(
            sources=update.sources,
            groups=update.groups,
            workers=(update.worker,),
            now=update.worker.heartbeat_at,
            stale_after_secs=1.0,
        )
        assert source.frontier == 41
        assert runtime.fatal_reason is None
        assert update.worker.watchdog_failure is None
        assert status.liveness
        if outage:
            cap = site.sources['events'].poll_retry_max_secs * (
                1 + site.sources['events'].poll_retry_jitter_fraction
            )
            assert source.next_retry_secs is not None
            assert 0 <= source.next_retry_secs <= cap
            assert 'source-degraded' in status.conditions
            assert not status.readiness
        else:
            assert source.retry_attempt == 0
            assert source.next_retry_secs is None
            assert source.last_error is None
            assert 'source-degraded' not in status.conditions
            assert status.readiness


@pytest.mark.parametrize(
    'arguments',
    [
        {'site': 'main'},
        {'site': 'main', 'source': 'events', 'group': 'local'},
        {'site': 'main', 'source': 'missing'},
        {'site': 'main', 'group': 'missing'},
    ],
)
def test_health_cache_operator_registration_rejects_invalid_target_without_budget(
    arguments: dict[str, str],
) -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=('local',),
    )
    cache = _ComponentHealthCache(
        site='main',
        group_sources=MappingProxyType({'local': 'events'}),
    )
    components = {
        'events': _Component('events', []),
        'local': _Component('local', []),
    }
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='operator-watchdog-invalid-1',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        control=child,
        clock=lambda: 100.0,
        health_cache=cache,
        group_sources=MappingProxyType({'local': 'events'}),
        operator_deadline_secs_by_source=MappingProxyType({'events': 13.0}),
    )
    command = OperatorCommand(
        request_id='operator-invalid',
        command='retry',
        arguments={**arguments, 'reason': 'operator watchdog test'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=123),
    )
    runtime.start()
    try:
        parent.send(command)
        result = parent.receive(timeout=1.0)
        assert isinstance(result, CommandResult)
        watchdog = next(item for item in cache._snapshot()._watchdogs if item.kind == 'operator')
    finally:
        runtime.request_stop('test-cleanup')
        assert runtime.wait() == ()
        parent.close()
        child.close()

    assert not result.ok
    assert result.message == 'operator command owner is invalid'
    assert watchdog.phase == 'idle'
    assert all(
        'operator:' not in operation
        for component in components.values()
        for operation in component._operations
    )


@pytest.mark.parametrize('active', [False, True], ids=['idle', 'active-command'])
def test_health_cache_operator_shutdown_preserves_current_target(active: bool) -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=('local',),
    )
    cache = _ComponentHealthCache(
        site='main',
        group_sources=MappingProxyType({'local': 'events'}),
    )
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='operator-shutdown-1',
        source_factory=lambda name, _slots, _host: _Component(name, []),
        lane_factory=lambda name, _slots, _host: _Component(name, []),
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        clock=lambda: 100.0,
        health_cache=cache,
        group_sources=MappingProxyType({'local': 'events'}),
        operator_deadline_secs_by_source=MappingProxyType({'events': 13.0}),
    )
    watchdog = runtime._operator_watchdog
    assert watchdog is not None
    sample = watchdog.sample()
    cache._register_watchdog(
        kind=sample.kind,
        identity=sample.identity,
        worker_instance=sample.worker_instance,
        token=sample.registration_token,
        supplier=watchdog.sample,
    )
    idle = next(item for item in cache._snapshot()._watchdogs if item.kind == 'operator')
    assert idle.phase == 'idle'
    assert (idle.source, idle.group) == (None, None)

    if active:
        command = OperatorCommand(
            request_id='operator-shutdown-command',
            command='park',
            arguments={'site': 'main', 'group': 'local', 'reason': 'maintenance'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=123),
        )
        queue = cast(
            'Callable[[OperatorCommand], CommandResult | None]',
            vars(type(runtime))['_queue_operator_command'].__get__(runtime),
        )
        assert queue(command) is None
        internal = next(item for item in cache._snapshot()._watchdogs if item.kind == 'operator')
        assert internal.phase == 'internal'
        assert (internal.source, internal.group) == ('events', 'local')

    runtime.request_stop('test-shutdown')
    shutdown = next(item for item in cache._snapshot()._watchdogs if item.kind == 'operator')

    assert shutdown.phase == 'shutdown'
    assert shutdown.deadline_at == 101.0
    assert (shutdown.source, shutdown.group) == (('events', 'local') if active else (None, None))


@pytest.mark.parametrize('transport', ['local', 'remote'])
def test_concrete_eager_channel_drops_delivery_floor_events(
    transport: str,
) -> None:
    factory = cast(
        '_FloorChannelFactory',
        vars(worker_module)['_DeliveryFloorChannel'],
    )
    channel = cast(
        'GroupChannel',
        factory(
            group='editorial',
            ordering='eager',
            max_events=8,
            max_bytes=262_144,
            delivery_floor=57,
        ),
    )
    for event_id in (55, 57, 58, 59):
        message = EventDelivery(
            EncodedEvent.from_event_frame(
                event_id,
                encode_event(Event(_runtime_event(event_id))),
            )
        )
        if transport == 'remote':
            message = cast(
                'EventDelivery',
                _decode_event_wire(b''.join(_encode_fanout_message(message)._parts)),
            )
        assert channel.offer(message)

    assert [
        message.encoded.event_id
        for message in channel.drain()
        if isinstance(message, EventDelivery)
    ] == [58, 59]


@pytest.mark.parametrize(
    ('worker', 'ordering', 'start_mode'),
    [
        ('pool', 'eager', 'initialize'),
        ('pool', 'eager', 'resume'),
        ('pool', 'strict', 'initialize'),
        ('pool', 'strict', 'resume'),
        ('dedicated', 'eager', 'initialize'),
        ('dedicated', 'eager', 'resume'),
        ('dedicated', 'strict', 'initialize'),
        ('dedicated', 'strict', 'resume'),
    ],
)
def test_concrete_remote_named_and_dedicated_worker_runs_lane(
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    start_mode: Literal['initialize', 'resume'],
    tmp_path: Path,
    worker: str,
) -> None:
    group = _group('remote', worker=worker, ordering=ordering)
    site = _runtime_delivery_site(group, start_mode)
    _prepare_runtime_state(tmp_path, site, group, start_mode)
    config = _runtime_config(tmp_path, site)
    site_spec, dependent_spec = build_worker_specs(site)
    provider, provider_state, invoked = _concrete_delivery_provider(monkeypatch, start_mode)
    monkeypatch.setattr(worker_module, 'load_provider', _provider_loader(provider))
    source_parent_socket, source_child_socket = socket.socketpair()
    source_parent = WorkerControl(
        source_parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0
    )
    source_child = WorkerControl(
        source_child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0
    )
    source_assignment = WorkerAssignment(
        worker_id=site_spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='events',
                group='remote',
                group_generation=7,
                start_mode=start_mode,
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )
    source_runtime = build_worker_runtime(
        config,
        site_spec,
        source_assignment,
        source_child,
    )
    dependent_parent_socket, dependent_child_socket = socket.socketpair()
    dependent_parent = WorkerControl(
        dependent_parent_socket,
        max_frame_bytes=131_072,
        send_deadline_secs=1.0,
    )
    dependent_child = WorkerControl(
        dependent_child_socket,
        max_frame_bytes=131_072,
        send_deadline_secs=1.0,
    )
    dependent_runtime: WorkerRuntime | None = None
    dependent_drain_stop: threading.Event | None = None
    dependent_drain_thread: threading.Thread | None = None
    source_drain_stop: threading.Event | None = None
    source_drain_thread: threading.Thread | None = None
    try:
        source_runtime.start()
        endpoint = _receive_type(source_parent, SourceEndpointReady)
        grant = EventPeerGrant(
            source='events',
            group='remote',
            dependent_spawn_instance='dependent-spawn-1',
            capability='remote-capability',
        )
        source_drain_stop, source_drain_thread = _authorize_remote_source(
            source_parent,
            grant,
            config,
        )
        dependent_assignment = WorkerAssignment(
            worker_id=dependent_spec.worker_id,
            spawn_instance='dependent-spawn-1',
            source_generations=(),
            group_assignments=(
                DeliveryGroupAssignment(
                    source='events',
                    group='remote',
                    group_generation=7,
                    start_mode=start_mode,
                ),
            ),
            source_group_assignments=(),
            source_endpoints=(endpoint,),
            event_grants=(grant,),
        )
        dependent_runtime = build_worker_runtime(
            config,
            dependent_spec,
            dependent_assignment,
            dependent_child,
        )
        dependent_runtime.start()
        try:
            _assert_remote_client_health(dependent_parent, config)
        finally:
            dependent_drain_stop, dependent_drain_thread = _start_control_drain(dependent_parent)
        group_path = _source_database_path(tmp_path)
        _wait_for_group_database(dependent_runtime, group_path, 'remote')
        if start_mode == 'resume':
            _assert_resumed_delivery(
                dependent_runtime, group_path, provider_state, invoked, 'remote'
            )
        assert source_runtime.fatal_reason is None
    finally:

        def close_source_runtime() -> None:
            _release_failed_source_runtime(
                source_runtime,
                (source_parent, source_child),
                (source_parent_socket, source_child_socket),
                (source_drain_stop, source_drain_thread)
                if source_drain_stop is not None and source_drain_thread is not None
                else None,
            )

        _release_failed_source_runtime(
            dependent_runtime,
            (dependent_parent, dependent_child),
            (dependent_parent_socket, dependent_child_socket),
            (dependent_drain_stop, dependent_drain_thread)
            if dependent_drain_stop is not None and dependent_drain_thread is not None
            else None,
            (close_source_runtime,),
        )


@pytest.mark.parametrize(
    ('ordering', 'start_mode'),
    [
        ('eager', 'initialize'),
        ('eager', 'resume'),
        ('strict', 'initialize'),
        ('strict', 'resume'),
    ],
)
def test_concrete_site_worker_runs_local_eager_and_strict_lane(
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    start_mode: Literal['initialize', 'resume'],
    tmp_path: Path,
) -> None:
    group = _group('local', worker='default', ordering=ordering)
    site = _runtime_delivery_site(group, start_mode)
    _prepare_runtime_state(tmp_path, site, group, start_mode)
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(
            DeliveryGroupAssignment(
                source='events',
                group='local',
                group_generation=7,
                start_mode=start_mode,
            ),
        ),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='events',
                group='local',
                group_generation=7,
                start_mode=start_mode,
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )
    provider, provider_state, invoked = _concrete_delivery_provider(monkeypatch, start_mode)
    monkeypatch.setattr(worker_module, 'load_provider', _provider_loader(provider))
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = build_worker_runtime(config, spec, assignment, child)
    drain_stop: threading.Event | None = None
    drain_thread: threading.Thread | None = None
    try:
        runtime.start()
        drain_stop, drain_thread = _start_control_drain(parent)
        group_path = _source_database_path(tmp_path)
        _wait_for_group_database(runtime, group_path, 'local')
        if start_mode == 'resume':
            _assert_resumed_delivery(runtime, group_path, provider_state, invoked, 'local')
    finally:
        _release_failed_source_runtime(
            runtime,
            (parent, child),
            (parent_socket, child_socket),
            (drain_stop, drain_thread)
            if drain_stop is not None and drain_thread is not None
            else None,
        )


@pytest.mark.parametrize(
    'fault',
    ['channel', 'lane', 'state', 'reader', 'client'],
)
def test_delivery_lane_cleanup_completes_after_each_fault(fault: str) -> None:
    order = ('channel', 'lane', 'state', 'reader', 'client')
    operations: list[str] = []
    first_fault = order.index(fault)
    probes = {
        name: _CleanupProbe(
            name,
            operations,
            RuntimeError(f'{name} cleanup failed') if index >= first_fault else None,
        )
        for index, name in enumerate(order)
    }
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(probes['lane'])['watch_phase'] = component._watchdog.watch
    stop = threading.Event()
    stop.set()
    vars(component).update({'_lane': probes['lane'], '_stop': stop})
    resources = SimpleNamespace(
        _channel=probes['channel'],
        _client=probes['client'],
        _lane=probes['lane'],
        _reader=probes['reader'],
        _state=probes['state'],
    )
    drive = cast('_LaneLifecycleDriver', vars(component_type)['_drive_lane'])
    first_error = probes[fault]._error
    assert first_error is not None

    with pytest.raises(RuntimeError) as raised:
        drive(component, resources)

    traceback = raised.value.__traceback__
    while traceback is not None and traceback.tb_next is not None:
        traceback = traceback.tb_next
    assert operations == list(order)
    assert vars(component)['_lane'] is None
    assert raised.value is first_error
    assert traceback is probes[fault]._traceback


@pytest.mark.parametrize('transport', ['local', 'remote'])
@pytest.mark.parametrize('execution', ['sync', 'async'])
@pytest.mark.parametrize('case', ['single', 'cross-epoch', 'new-fence', 'callback-stop'])
def test_detached_strict_join_orders_sparse_obligation_and_suffix(
    transport: Literal['local', 'remote'],
    execution: Literal['sync', 'async'],
    case: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _detached_strict_join(
        tmp_path,
        monkeypatch,
        modes=(transport, execution),
        case=case,
    )


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_detached_catchup_requests_fresh_durable_progress(
    ordering: Literal['eager', 'strict'],
) -> None:
    client, reader, error = _probe_lane_catchup(
        ordering,
        (_synchronization(41), _synchronization(42)),
    )

    assert error is None
    assert client._calls == [7, 7]
    assert [item.frontier for item in reader._observed] == [41, 42]


@pytest.mark.parametrize('transport', ['local', 'remote'])
def test_durable_range_refresh_converges_locally_and_remotely(
    tmp_path: Path,
    transport: Literal['local', 'remote'],
) -> None:
    source_path = _source_database_path(tmp_path)
    with _open_coordination_source(source_path) as source_state:
        _seed_coordination_range(source_state)

    with open_group_on_source(
        source_path,
        writes=write_context_for(source_path),
        site='main',
        site_server_url='https://example.test',
        source='events',
        group='local',
        group_generation=7,
        ordering='eager',
        stream_fingerprint='stream',
        source_generation='source-1',
        verified_incarnation='incarnation-1',
        start_mode='resume',
    ) as group_state:
        reader = _CoordinationReader()
        with _coordination_client(
            transport,
            source_path,
            local_socket_path(
                tmp_path,
                'coordination',
                ('events', 'local'),
            ),
        ) as (_client, _surface, close_range):
            coordinator = LaneRefresh(
                lane=cast('GroupLane', _CoordinationLane(group_state)),
                limiter=_CatchupLimiter(1),
                reader=cast('CatchupReader', reader),
                state=group_state,
            )
            coordinator._service()
            close_range()
            for _ in range(3):
                coordinator._service()

        assert group_state.snapshot().obligation_count == 0
        assert reader._ranges == [(42, 45)]

    with _open_coordination_source(source_path) as source_state:
        assert source_state.snapshot().ranges == ()
        assert source_state.snapshot().pending_obligations == 0


@pytest.mark.parametrize(
    ('coordinate_later', 'expected_obligation_count'),
    [(False, 1), (True, 2)],
)
def test_group_mutation_snapshot_from_lane_transition_avoids_second_snapshot_query(
    coordinate_later: bool,
    expected_obligation_count: int,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 0.0)
    with _open_group_health_state(tmp_path / 'lane-mutation.db') as state:
        _bind_worker_range(state, 'range-1', 42)
        accepted_revision = state.snapshot().revision
        component, cache, resources, lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=1.0,
        )
        snapshot_calls = 0
        projection_calls = 0
        real_projection = state.unresolved_late_id_projection
        real_snapshot = state.snapshot

        def counted_projection(*, limit: int) -> tuple[int, tuple[int, ...]]:
            nonlocal projection_calls
            projection_calls += 1
            return real_projection(limit=limit)

        def counted_snapshot() -> GroupSnapshot:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return real_snapshot()

        monkeypatch.setattr(
            state,
            'unresolved_late_id_projection',
            counted_projection,
        )
        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        _publish_group_health(component, resources, lifecycle)
        assert snapshot_calls == 1
        assert cache._snapshot()._groups[0].outstanding_late_id_count == 0

        mutation = state.reserve_late_event(
            'range-1',
            42,
            expected_revision=accepted_revision,
        )
        with state._writes.transaction(state._database.connection):
            insert_source_late_ids(state._database.connection, 'range-1', (42,))

        def ignore_client(_client: object) -> None:
            return None

        def coordinate(
            *, scan_obligations: bool = True, force: bool = False
        ) -> GroupSnapshot | None:
            del force
            del scan_obligations
            if not coordinate_later:
                return None
            _bind_worker_range(state, 'range-2', 43)
            return state.record_delivery_floor_skip(41, expected_revision=mutation.revision)

        clock = [0.0]
        monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
        assert lifecycle is not None
        vars(lifecycle)['_next_coordination_at'] = 1.0

        def drain(
            _channel: object,
            *,
            idle_snapshot: object | None = None,
            deadline_at: float = float('inf'),
        ) -> GroupSnapshot:
            del idle_snapshot, deadline_at
            clock[0] = 1.0
            return mutation

        monkeypatch.setattr(vars(resources)['_lane'], 'drain_live', drain)
        vars(component)['_poll_lane'] = ignore_client
        vars(resources)['_coordination'] = SimpleNamespace(_service=coordinate)
        assert isinstance(component, _LaneComponent)
        component._drive_live(cast('_LaneResources', resources), cast('_LaneLifecycle', lifecycle))
        _publish_group_health(component, resources, lifecycle)

        assert snapshot_calls == 1
        assert projection_calls == 2
        health = cache._snapshot()._groups[0]
        assert health.outstanding_late_id_count == 1
        assert health.outstanding_late_ids == (42,)
        assert health.outstanding_late_obligation_count == expected_obligation_count


@pytest.mark.parametrize(
    ('outcome', 'resulting_revision'),
    [('parked', 12), ('stale', 15)],
)
def test_lane_sends_recorded_stale_or_parked_result_before_checkpoint_construction(
    monkeypatch: pytest.MonkeyPatch,
    outcome: Literal['parked', 'stale'],
    resulting_revision: int,
) -> None:
    component, context, operations, control, state = _recovery_construction_probe(
        outcome=outcome,
        resulting_revision=resulting_revision,
    )
    wall_clock_calls: list[None] = []

    def wall_clock() -> float:
        wall_clock_calls.append(None)
        return 2000.5

    monkeypatch.setattr(worker_module.time, 'time', wall_clock)
    component_type = _LaneComponent
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )

    with pytest.raises(_RecoveryConstructionReachedError):
        build_resources(component, context)

    assert wall_clock_calls == [None]
    assert state._decided_at_wall_clock == 2000.5
    assert len(control._messages) == 1
    ready = cast('SimpleNamespace', control._messages[0])
    assert (
        type(ready).__name__,
        ready.spawn_instance,
        ready.evidence_id,
        ready.site,
        ready.group,
        ready.group_generation,
        ready.outcome,
        ready.resulting_revision,
    ) == (
        'GroupRecoveryReady',
        'current-spawn-1',
        'evidence-1',
        'main',
        'remote',
        7,
        outcome,
        resulting_revision,
    )
    assert operations.index('ready') < operations.index('checkpoint-path')


@pytest.mark.parametrize(
    ('failure_site', 'error', 'expected_detail'),
    [
        ('control', ValueError('x' * 5_000), 'x' * 4_096),
        ('health', BrokenPipeError('token=protected-value'), '<redacted>'),
        ('health-after-eof', BrokenPipeError('token=protected-value'), '<redacted>'),
    ],
    ids=['control-poll', 'health-publication', 'health-publication-after-eof'],
)
def test_monitor_exception_is_worker_fatal_and_closes_runtime(
    monkeypatch: pytest.MonkeyPatch,
    failure_site: Literal['control', 'health', 'health-after-eof'],
    error: Exception,
    expected_detail: str,
) -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('audit', 'events'),
        groups=('default',),
    )
    operations: list[str] = []
    components = {
        identity: _Component(identity, operations)
        for identity in ('source:audit', 'source:events', 'lane:default')
    }

    def fail(*_args: object) -> NoReturn:
        raise error

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: components[f'source:{name}'],
        lane_factory=lambda name, _slots, _host: components[f'lane:{name}'],
        health_sink=fail if failure_site != 'control' else lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
    )
    if failure_site == 'control':
        monkeypatch.setattr(runtime, '_poll_control', fail)
    elif failure_site == 'health-after-eof':
        monkeypatch.setattr(runtime, '_poll_control', lambda: runtime.request_stop('control-eof'))

    result = _run_worker_runtime_cli(monkeypatch, runtime)

    assert result == 1
    assert runtime.fatal_reason == (
        f'monitor thread failed: {type(error).__name__}: {expected_detail}'
    )
    assert all(component._finished.is_set() for component in components.values())
    assert all(not thread.is_alive() for thread in runtime._threads.values())
    assert not runtime._resources_open
    assert {operation for operation in operations if operation.startswith('stop:')} == {
        'stop:lane:default',
        'stop:source:audit',
        'stop:source:events',
    }
    assert operations[-3:] == [
        'close:lane:default',
        'close:source:events',
        'close:source:audit',
    ]


@pytest.mark.parametrize('terminal_identity', ['operator-executor', 'source:events'])
def test_monitor_waits_between_shutdown_health_updates_before_force_exit(
    terminal_identity: Literal['operator-executor', 'source:events'],
) -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',) if terminal_identity == 'source:events' else (),
        groups=(),
    )
    component = _Component('source:events', [], wedge=True)
    forced: list[int] = []
    gate = _TerminalWakeGate(terminal_identity)

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=gate.publish_health,
        health_interval_secs=10.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        clock=gate.clock,
        force_exit=forced.append,
    )
    gate._install(runtime)
    runtime.start()
    gate._observe_target_thread(runtime)
    wait_result: list[tuple[str, ...]] = []
    wait_thread: threading.Thread | None = None
    try:
        if terminal_identity == 'source:events':
            assert component._started.wait(1.0)
        assert gate._wait_for_health_after(0)
        runtime.request_stop('test')
        if terminal_identity == 'source:events':
            operator = vars(runtime)['_threads']['operator-executor']
            operator.join(1.0)
            assert not operator.is_alive()
            previous_health_count = gate._health_count()
            gate._wake_monitor()
            assert gate._wait_for_health_after(previous_health_count)
            component._release.set()
        assert gate._wait_for_terminal_wake()
        monitor = vars(runtime)['_monitor_thread']
        assert isinstance(monitor, threading.Thread)
        monitor.join(1.0)
        assert not monitor.is_alive()
        wait_thread = threading.Thread(
            target=lambda: wait_result.append(runtime.wait()),
            name='worker-wait-probe',
        )
        wait_thread.start()
        assert gate._wait_for_bounded_join()
        wait_thread.join(1.0)
        assert not wait_thread.is_alive()
        assert wait_result == [(terminal_identity,)]
        assert forced == []
        assert runtime._stuck_threads == (terminal_identity,)
        assert runtime._resources_open
        gate._release()
    finally:
        gate._release()
        component._release.set()
        if wait_thread is not None:
            wait_thread.join(1.0)
        for thread in vars(runtime)['_threads'].values():
            thread.join(1.0)
        assert runtime._shutdown() == ()
        assert not runtime._resources_open


@pytest.mark.parametrize(
    'fault',
    ['channel', 'lane', 'state', 'reader', 'client'],
)
def test_partial_lane_cleanup_completes_after_each_fault(fault: str) -> None:
    order = ('channel', 'lane', 'state', 'reader', 'client')
    operations: list[str] = []
    first_fault = order.index(fault)
    probes = {
        name: _CleanupProbe(
            name,
            operations,
            RuntimeError(f'{name} cleanup failed') if index >= first_fault else None,
        )
        for index, name in enumerate(order)
    }
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(probes['lane'])['watch_phase'] = component._watchdog.watch
    vars(component)['_lane'] = probes['lane']
    attempt_type = cast('Callable[..., object]', worker_module._PartialLaneAttempt)
    attempt = attempt_type(**{f'_{name}': probe for name, probe in probes.items()})
    close_partial = cast(
        'Callable[[object, object], None]',
        vars(component_type)['_close_partial_attempt'],
    )
    first_error = probes[fault]._error
    assert first_error is not None

    with pytest.raises(RuntimeError) as raised:
        close_partial(component, attempt)

    traceback = raised.value.__traceback__
    while traceback is not None and traceback.tb_next is not None:
        traceback = traceback.tb_next
    assert operations == list(order)
    assert vars(component)['_lane'] is None
    assert raised.value is first_error
    assert traceback is probes[fault]._traceback


@pytest.mark.parametrize('stage', ['connect', 'observation'])
def test_remote_bootstrap_transport_failure_uses_private_retry_signal(
    stage: Literal['connect', 'observation'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(component).update(
        {
            '_assignment': WorkerAssignment(
                worker_id='["worker","pool"]',
                spawn_instance='dependent-1',
                source_generations=(),
                group_assignments=(),
                source_group_assignments=(),
                source_endpoints=(
                    SourceEndpointReady(
                        source='events',
                        source_generation='source-generation-1',
                        path=str(tmp_path / 'events.sock'),
                    ),
                ),
                event_grants=(
                    EventPeerGrant(
                        source='events',
                        group='remote',
                        dependent_spawn_instance='dependent-1',
                        capability='capability-1',
                    ),
                ),
            ),
            '_config': SimpleNamespace(
                daemon=SimpleNamespace(
                    health_interval_secs=1.0,
                    max_ipc_frame_bytes=131_072,
                )
            ),
            '_group': 'remote',
            '_internal_stall_deadline_secs': 10.0,
            '_local_source': None,
            '_stop': threading.Event(),
        }
    )
    context = SimpleNamespace(
        _assignment=SimpleNamespace(
            conditional_park=None,
            group_generation=7,
            start_mode='resume',
        ),
        _credential=SimpleNamespace(
            max_rpc_attempts=3,
            rpc_attempt_interval_secs=0.1,
            timeout_secs=1.0,
        ),
        _group=SimpleNamespace(source='events'),
        _holdback_secs=0.0,
        _source_config=SimpleNamespace(
            queue_max_bytes=262_144,
            queue_max_events=8,
        ),
    )
    client = EventClient(
        path=tmp_path / 'events.sock',
        source='events',
        group='remote',
        spawn_instance='dependent-1',
        capability='capability-1',
        max_frame_bytes=131_072,
        queue_max_entries=8,
        queue_max_bytes=262_144,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )

    def fail_transport(*_args: object, **_options: object) -> object:
        raise ConnectionError(f'{stage} transport failed')

    build_client = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_client'],
    )
    observe_source = component_type._observe_source

    def _connect_operation() -> object:
        return build_client(component, context)

    def _observe_operation() -> object:
        return observe_source(
            component,
            client,
            assignment=GroupBootstrapRequest(
                source='events',
                group='remote',
                group_generation=7,
                start_mode='resume',
            ),
        )

    try:
        if stage == 'connect':
            monkeypatch.setattr(EventClient, 'connect', fail_transport)
            operation = _connect_operation
        else:
            monkeypatch.setattr(EventClient, 'observe_group', fail_transport)
            operation = _observe_operation

        with pytest.raises(
            _RemoteBootstrapTransportError,
            match=f'{stage} transport failed',
        ) as raised:
            operation()
    finally:
        client.close()

    assert isinstance(raised.value.__cause__, ConnectionError)


@pytest.mark.parametrize('stage', ['provider', 'state', 'plugin'])
def test_remote_lane_does_not_retry_nontransport_connection_error(
    stage: Literal['provider', 'state', 'plugin'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stop = threading.Event()
    failure = _NontransportConstructionFailure(stage, stop)
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    context = SimpleNamespace(
        _assignment=SimpleNamespace(
            conditional_park=None,
            group_generation=7,
            start_mode='resume',
        ),
        _group=SimpleNamespace(source='events'),
        _site=SimpleNamespace(name='main'),
        _source_config=SimpleNamespace(fetch_interval_secs=0.0),
    )

    failure._install(
        component,
        context,
        tmp_path / f'{stage}.db',
        monkeypatch,
    )
    run = cast('Callable[[object], None]', vars(component_type)['_run'])

    with pytest.raises(ConnectionError, match=f'{stage} construction failed'):
        run(component)

    assert failure._attempts == 1


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_remote_retained_history_halt_remains_group_local(
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    tmp_path: Path,
) -> None:
    groups = (
        _group('halted', worker='pool', ordering=ordering),
        _group('healthy', worker='pool', ordering=ordering),
    )
    site = replace(
        _runtime_site(groups[0]),
        groups=MappingProxyType({group.name: group for group in groups}),
        sources=MappingProxyType(
            {
                'events': replace(
                    _runtime_site(groups[0]).sources['events'],
                    fetch_interval_secs=0.01,
                )
            }
        ),
    )
    _prepare_retained_history_runtime_state(tmp_path, site, groups)
    config = _runtime_config(tmp_path, site)
    site_spec, dependent_spec = build_worker_specs(site)
    provider = _gap_runtime_provider()
    monkeypatch.setattr(worker_module, 'load_provider', _provider_loader(provider))
    component_type = _LaneComponent
    recover = component_type._recover_remote_failure
    classified_errors: list[ConnectionError | JoinProtocolError] = []

    def track_remote_failure(
        component: object,
        resources: object,
        lifecycle: object,
        error: ConnectionError | JoinProtocolError,
    ) -> bool:
        if vars(component)['_group'] == 'halted':
            classified_errors.append(error)
        return recover(
            cast('_LaneComponent', component),
            cast('_LaneResources', resources),
            cast('_LaneLifecycle', lifecycle),
            error,
        )

    monkeypatch.setattr(
        component_type,
        '_recover_remote_failure',
        track_remote_failure,
    )
    source_assignment = WorkerAssignment(
        worker_id=site_spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(),
        source_group_assignments=tuple(
            ActiveSourceGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='resume',
            )
            for group in groups
        ),
        source_endpoints=(),
        event_grants=(),
    )
    grants = tuple(
        EventPeerGrant(
            source='events',
            group=group.name,
            dependent_spawn_instance='dependent-spawn-1',
            capability=f'{group.name}-capability',
        )
        for group in groups
    )
    source_session = _start_source_runtime(
        config,
        site_spec,
        source_assignment,
        grants,
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    assignment = WorkerAssignment(
        worker_id=dependent_spec.worker_id,
        spawn_instance='dependent-spawn-1',
        source_generations=(),
        group_assignments=tuple(
            DeliveryGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='resume',
            )
            for group in groups
        ),
        source_group_assignments=(),
        source_endpoints=(source_session._endpoint,),
        event_grants=grants,
    )
    runtime = build_worker_runtime(
        config,
        dependent_spec,
        assignment,
        child,
    )
    halted_serviced = _track_halted_lane_service(monkeypatch, 'history-truncated')
    drain_stop: threading.Event | None = None
    drain_thread: threading.Thread | None = None
    try:
        runtime.start()
        _assert_retained_history_halt(runtime, parent, halted_serviced)
        assert not any(
            isinstance(error, JoinProtocolError)
            and 'join catch-up did not reach a committable boundary' in str(error)
            for error in classified_errors
        )
    finally:
        drain_stop, drain_thread = _start_control_drain(parent)
        _release_failed_source_runtime(
            runtime,
            (parent, child),
            (parent_socket, child_socket),
            (drain_stop, drain_thread),
            (source_session._close,),
        )

    with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
        truncated = connection.execute(
            'SELECT history_truncated_floor FROM group_state'
        ).fetchone()
    assert truncated == (61,)


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_retained_history_halt_remains_group_local(
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    tmp_path: Path,
) -> None:
    groups = (
        _group('halted', worker='default', ordering=ordering),
        _group('healthy', worker='default', ordering=ordering),
    )
    site = replace(
        _runtime_site(groups[0]),
        groups=MappingProxyType({group.name: group for group in groups}),
    )
    _prepare_retained_history_runtime_state(tmp_path, site, groups)
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=tuple(
            DeliveryGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='resume',
            )
            for group in groups
        ),
        source_group_assignments=tuple(
            ActiveSourceGroupAssignment(
                source='events',
                group=group.name,
                group_generation=7,
                start_mode='resume',
            )
            for group in groups
        ),
        source_endpoints=(),
        event_grants=(),
    )
    halted_serviced = _track_retained_history_service(monkeypatch)
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        _gap_runtime_provider(),
    ) as (runtime, control):
        _assert_retained_history_halt(runtime, control, halted_serviced)

    with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
        truncated = connection.execute(
            'SELECT history_truncated_floor FROM group_state'
        ).fetchone()
    assert truncated == (61,)


@pytest.mark.parametrize(
    'phase',
    get_args(_ComponentStopPhase),
)
def test_runtime_classifies_known_cooperative_component_stop(
    phase: _ComponentStopPhase,
) -> None:
    component, entered, _error_type, _message = _cooperative_stop_component(
        phase,
        fail_before_stop=False,
    )

    fatal_reason, forced = _run_cooperative_stop_component(
        component,
        entered,
        request_stop=True,
    )

    assert fatal_reason is None
    assert forced == []


@pytest.mark.parametrize(
    'phase',
    get_args(_ComponentStopPhase),
)
def test_runtime_keeps_same_operation_exception_fatal_before_stop(
    phase: _ComponentStopPhase,
) -> None:
    component, entered, error_type, message = _cooperative_stop_component(
        phase,
        fail_before_stop=True,
    )

    fatal_reason, forced = _run_cooperative_stop_component(
        component,
        entered,
        request_stop=False,
    )

    assert fatal_reason == (f'lane:local thread failed: {error_type.__name__}: {message}')
    assert forced == []


@pytest.mark.parametrize('stop_owner', ['caller', 'component'])
def test_source_component_call_stop_is_not_serviced_later(
    stop_owner: Literal['caller', 'component'],
) -> None:
    component_type = _SourceComponent
    component = object.__new__(component_type)
    queued = threading.Event()
    component_stop = threading.Event()

    vars(component).update(
        {
            '_mailbox': _OwnerMailbox('source', queued),
            '_stop': component_stop,
        }
    )
    call = component_type._call
    cooperative_error = cast(
        'type[RuntimeError]',
        vars(worker_module)['_CooperativeComponentStopError'],
    )
    caller_stop = threading.Event()
    operations: list[str] = []

    def invoke(_surface: object) -> None:
        operations.append('invoked')

    def run() -> object:
        return call(
            component,
            invoke,
            deadline_secs=30.0,
            stop=caller_stop,
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        assert queued.wait(1.0)
        {'caller': caller_stop, 'component': component_stop}[stop_owner].set()
        with pytest.raises(cooperative_error) as raised:
            future.result(timeout=1.0)

    assert caller_stop.is_set() == (stop_owner == 'caller')
    assert component_stop.is_set() == (stop_owner == 'component')
    assert isinstance(raised.value.__cause__, ConnectionError)
    assert str(raised.value.__cause__) == 'source component stopped during local call'
    _service_queued_local_calls(component, SimpleNamespace(_reconcile_groups=lambda: None))
    assert operations == []


@pytest.mark.parametrize('health_interval_secs', [1e-100, 5e-324])
def test_source_health_refresh_extreme_interval_terminates_with_future_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    health_interval_secs: float,
) -> None:
    component, _cache = _source_health_component(
        tmp_path,
        health_interval_secs=health_interval_secs,
    )
    server = _SourceHealthServer()
    provider = _runtime_provider()
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_coordination_source(tmp_path / 'source-extreme-health.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _source_health_poller(state, provider) as poller:
            _service_source_health(component, server, poller)
            clock.now = 1.0

            def timeout(_signum: int, _frame: object) -> None:
                raise TimeoutError('fractional health deadline did not advance')

            previous_handler = signal.signal(signal.SIGALRM, timeout)
            signal.setitimer(signal.ITIMER_REAL, 0.25)
            try:
                _service_source_health(component, server, poller)
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0.0)
                signal.signal(signal.SIGALRM, previous_handler)

    assert vars(component)['_next_health_refresh_at'] > clock.now


@pytest.mark.parametrize(
    ('start_mode', 'group_generation', 'seed_existing'),
    [
        ('resume', 7, True),
        ('initialize', 8, False),
        ('resume', 8, False),
    ],
)
def test_source_open_waits_for_durable_group_bootstrap(
    group_generation: int,
    monkeypatch: pytest.MonkeyPatch,
    seed_existing: bool,
    start_mode: Literal['initialize', 'resume'],
    tmp_path: Path,
) -> None:
    group = _group('remote', worker='pool')
    site = _runtime_site(group)
    source_config = replace(
        site.sources['events'],
        fetch_interval_secs=0.001,
    )
    site = replace(
        site,
        sources=MappingProxyType({'events': source_config}),
    )
    config = Config(
        daemon=DaemonConfig(
            state_dir=tmp_path,
            sg_api_provider='shotgun-api3',
            health_interval_secs=0.01,
            shutdown_deadline_secs=2.0,
        ),
        sites=MappingProxyType({site.name: site}),
    )
    database_path = _source_database_path(tmp_path)
    fingerprint = stream_fingerprint(
        site.server,
        source_config.credentials,
        site.credentials[source_config.credentials].script_name,
    )
    with SourceState.open(
        database_path,
        writes=write_context_for(database_path),
        create=True,
        site=site.name,
        source='events',
        stream_fingerprint=fingerprint,
        source_generation='source-generation-old',
    ) as state:
        state.initialize(41, incarnation='stream-incarnation-1')
        if seed_existing:
            with open_group_on_source(
                database_path,
                writes=write_context_for(database_path),
                site=site.name,
                site_server_url=site.server,
                source='events',
                group='remote',
                group_generation=7,
                ordering='eager',
                stream_fingerprint=fingerprint,
                source_generation='source-generation-old',
                verified_incarnation='stream-incarnation-1',
                start_mode='initialize',
                initial_cursor=57,
                initial_delivery_floor=57,
                initial_last_delivered_id=57,
            ):
                pass

    monkeypatch.setattr(
        worker_module,
        'load_provider',
        _provider_loader(_gap_runtime_provider()),
    )
    spec = build_worker_specs(site)[0]
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='source-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='events',
                group='remote',
                group_generation=group_generation,
                start_mode=start_mode,
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = build_worker_runtime(config, spec, assignment, child)
    drain_stop: threading.Event | None = None
    drain_thread: threading.Thread | None = None
    row: tuple[int] | None = None
    try:
        runtime.start()
        _receive_type(parent, SourceEndpointReady)
        drain_stop, drain_thread = _start_control_drain(parent)
        for _ in range(200):
            with closing(sqlite3.connect(database_path)) as connection:
                row = connection.execute(
                    'SELECT frontier FROM source_meta WHERE singleton = 1'
                ).fetchone()
            if row == (60,) or runtime.fatal_reason is not None:
                break
            time.sleep(0.005)
        assert runtime.fatal_reason is None
        assert row == (60,)
    finally:
        _release_failed_source_runtime(
            runtime,
            (parent, child),
            (parent_socket, child_socket),
            (drain_stop, drain_thread)
            if drain_stop is not None and drain_thread is not None
            else None,
        )

    with closing(sqlite3.connect(database_path)) as connection:
        group_rows = connection.execute(
            """
            SELECT group_generation, delivery_floor
            FROM groups JOIN group_state USING (group_name)
            WHERE group_name = 'remote' AND active = 1
            ORDER BY group_generation
            """
        ).fetchall()
        range_rows = connection.execute(
            """
            SELECT group_generation, delivery_floor
            FROM range_groups
            WHERE group_name = 'remote'
            ORDER BY group_generation
            """
        ).fetchall()
    if seed_existing:
        assert group_rows == [(7, 57)]
        assert range_rows == [(7, 57)]
    else:
        assert group_rows == []
        assert range_rows == []


@pytest.mark.parametrize(
    ('mismatch', 'condition'),
    [
        ('stream', 'stream-identity-changed'),
        ('ordering', 'ordering-changed'),
    ],
)
def test_stream_and_ordering_mismatch_remain_visible_without_group_query(
    condition: str,
    mismatch: Literal['stream', 'ordering'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    halted_serviced = _track_halted_lane_service(monkeypatch, condition)
    group = _group('local', worker='default', ordering='eager')
    site = _runtime_site(group)
    _prepare_runtime_state(tmp_path, site, group, 'resume')
    with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
        if mismatch == 'stream':
            connection.execute(
                'UPDATE groups SET verified_incarnation = '
                "'stale-incarnation' WHERE group_name = 'local'"
            )
        else:
            connection.execute(
                "UPDATE groups SET ordering_mode = 'strict' WHERE group_name = 'local'"
            )
        connection.commit()
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='site-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(
            DeliveryGroupAssignment(
                source='events',
                group='local',
                group_generation=7,
                start_mode='resume',
            ),
        ),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='events',
                group='local',
                group_generation=7,
                start_mode='resume',
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )
    calls: list[str] = []
    monkeypatch.setattr(
        worker_module,
        'load_provider',
        _provider_loader(_runtime_provider(calls=calls)),
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = build_worker_runtime(config, spec, assignment, child)
    drain_stop: threading.Event | None = None
    drain_thread: threading.Thread | None = None
    try:
        runtime.start()
        try:
            health = _receive_typed_health(
                parent,
                lambda item: any(
                    condition in group_health.conditions for group_health in item.groups
                ),
            ).groups[0]
        finally:
            drain_stop, drain_thread = _start_control_drain(parent)
        assert halted_serviced.wait(2.0), 'halted lane did not complete a service cycle'
        lane_thread = runtime._threads['lane:local']
        assert runtime.fatal_reason is None
        assert lane_thread.is_alive()
        assert not any('lane:local' in name for name in calls)
        assert condition in health.conditions
    finally:
        _release_failed_source_runtime(
            runtime,
            (parent, child),
            (parent_socket, child_socket),
            (drain_stop, drain_thread)
            if drain_stop is not None and drain_thread is not None
            else None,
        )
    with closing(sqlite3.connect(_source_database_path(tmp_path))) as connection:
        changed = (
            connection.execute('SELECT stream_identity_changed FROM group_state').fetchone()
            if mismatch == 'stream'
            else connection.execute('SELECT ordering_changed FROM group_state').fetchone()
        )
    assert changed == (1,)


@pytest.mark.parametrize(
    ('accepted', 'stop_after_delivery', 'expected'),
    [
        (False, False, (False, False, 0)),
        (True, False, (True, True, 1)),
        (True, True, (True, False, 1)),
    ],
)
def test_strict_catchup_maps_acceptance_and_stop_independently(
    accepted: bool,
    stop_after_delivery: bool,
    expected: tuple[bool, bool, int],
) -> None:
    outcomes: list[StrictDeliveryOutcome] = []
    stop = threading.Event()

    def deliver_many_counted(_deliveries: object) -> tuple[object, int]:
        if stop_after_delivery:
            stop.set()
        return (
            SimpleNamespace(
                status='running',
                cursor=41 if accepted else 40,
                in_flight_event_id=None,
            ),
            int(accepted),
        )

    def strict_step(**options: object) -> object:
        deliver = cast(
            'Callable[[Sequence[Event], float], StrictDeliveryOutcome]',
            options['deliver'],
        )
        outcomes.append(deliver((Event(_runtime_event(41)),), 0.0))
        return SimpleNamespace(status='join-ready', join_ready=True)

    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(component).update(
        {
            '_config': SimpleNamespace(
                daemon=SimpleNamespace(max_pending_epochs=8),
            ),
            '_stop': stop,
            '_limiter': _CatchupLimiter(1),
            '_health_interval_secs': 1.0,
        }
    )
    resources = SimpleNamespace(
        _context=SimpleNamespace(
            _holdback_secs=1.0,
            _source_config=SimpleNamespace(fetch_interval_secs=0.001),
        ),
        _coordination=SimpleNamespace(
            synchronize=lambda: _synchronization(45),
            _service=_ProbeCoordination._service,
        ),
        _lane=SimpleNamespace(deliver_many_counted=deliver_many_counted),
        _reader=SimpleNamespace(strict_step=strict_step),
        _state=_ProbeState(),
    )
    driver = cast(
        'Callable[[object, object, object], None]',
        vars(component_type)['_catch_up_strict'],
    )

    with (
        pytest.raises(RuntimeError, match='component stopped')
        if stop_after_delivery
        else nullcontext()
    ):
        driver(
            component,
            resources,
            SimpleNamespace(identity=SimpleNamespace(boundary=45)),
        )

    assert [(item.accepted, item.continue_epoch, item.consumed) for item in outcomes] == [expected]


@pytest.mark.parametrize(
    'conflict',
    [
        'missing-endpoint',
        'unused-endpoint',
        'missing-grant',
        'cross-group-grant',
        'unassigned-lane-grant',
    ],
)
def test_worker_assignment_requires_exact_remote_dependencies(
    conflict: str,
    tmp_path: Path,
) -> None:
    site = _site((_group('remote', worker='pool'),))
    config = Config(
        daemon=DaemonConfig(
            state_dir=tmp_path,
            sg_api_provider='shotgun-api3',
        ),
        sites=MappingProxyType({site.name: site}),
    )
    spec = next(item for item in build_worker_specs(site) if item.kind is WorkerKind._NAMED)
    endpoint = SourceEndpointReady(
        source='events',
        source_generation='events-generation-1',
        path='/private/events.sock',
    )
    grant = EventPeerGrant(
        source='events',
        group='remote',
        dependent_spawn_instance='dependent-spawn-1',
        capability='remote-capability',
    )
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='dependent-spawn-1',
        source_generations=(),
        group_assignments=(
            DeliveryGroupAssignment(
                source='events',
                group='remote',
                group_generation=7,
                start_mode='resume',
            ),
        ),
        source_group_assignments=(),
        source_endpoints=(endpoint,),
        event_grants=(grant,),
    )
    if conflict == 'missing-endpoint':
        assignment = replace(assignment, source_endpoints=())
    elif conflict == 'unused-endpoint':
        assignment = replace(
            assignment,
            source_endpoints=(
                endpoint,
                SourceEndpointReady(
                    source='audit',
                    source_generation='audit-generation-1',
                    path='/private/audit.sock',
                ),
            ),
        )
    elif conflict == 'missing-grant':
        assignment = replace(assignment, event_grants=())
    elif conflict == 'cross-group-grant':
        assignment = replace(
            assignment,
            event_grants=(
                replace(
                    grant,
                    source='audit',
                    capability='cross-group-capability',
                ),
            ),
        )
    else:
        assignment = replace(
            assignment,
            event_grants=(
                grant,
                EventPeerGrant(
                    source='events',
                    group='unassigned',
                    dependent_spawn_instance='dependent-spawn-1',
                    capability='unassigned-capability',
                ),
            ),
        )

    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    try:
        with pytest.raises(ValueError, match='dependencies'):
            build_worker_runtime(config, spec, assignment, control)
        assert tuple(tmp_path.iterdir()) == ()
    finally:
        child_socket.close()
        control.close()


@pytest.mark.parametrize('fault', ['runtime', 'drain', 'parent'])
def test_source_session_cleanup_attempts_all_independent_resources(
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    attempts: list[str] = []
    failure = AssertionError('injected cleanup failure')

    def attempt(name: str) -> None:
        attempts.append(name)
        if name == fault:
            raise failure

    runtime = cast('WorkerRuntime', SimpleNamespace(_threads={}, _monitor_thread=None))
    parent = cast('WorkerControl', SimpleNamespace(close=lambda: attempt('parent')))
    child = cast('WorkerControl', SimpleNamespace(close=lambda: attempt('child')))

    def stop_runtime(_runtime: object) -> None:
        attempt('runtime')

    monkeypatch.setattr(sys.modules[__name__], '_stop_runtime', stop_runtime)

    def stop_drain(*_args: object) -> None:
        attempt('drain')

    monkeypatch.setattr(sys.modules[__name__], '_stop_control_drain', stop_drain)
    session = _SourceRuntimeSession(
        runtime,
        parent,
        child,
        threading.Event(),
        threading.Thread(),
        SourceEndpointReady('events', 'source', '/private/events.sock'),
    )
    with pytest.raises(AssertionError, match='injected cleanup failure') as caught:
        session._close()
    assert caught.value is failure
    assert attempts == ['runtime', 'drain', 'parent', 'child']


@pytest.mark.parametrize('owner', ['runtime', 'monitor', 'drain'])
def test_source_session_retains_controls_used_by_live_runtime(
    monkeypatch: pytest.MonkeyPatch,
    owner: str,
) -> None:
    release = threading.Event()
    thread = threading.Thread(target=release.wait)
    thread.start()
    attempts: list[str] = []
    runtime = cast(
        'WorkerRuntime',
        SimpleNamespace(
            _threads={'source': thread} if owner == 'runtime' else {},
            _monitor_thread=thread if owner == 'monitor' else None,
        ),
    )
    control = cast('WorkerControl', SimpleNamespace(close=lambda: attempts.append('control')))

    def stop(_runtime: WorkerRuntime | None) -> None:
        raise AssertionError('runtime remained alive')

    monkeypatch.setattr(sys.modules[__name__], '_stop_runtime', stop)
    session = _SourceRuntimeSession(
        runtime,
        control,
        control,
        threading.Event(),
        thread if owner == 'drain' else threading.Thread(),
        SourceEndpointReady('events', 'source', '/private/events.sock'),
    )
    try:
        with pytest.raises(AssertionError, match='runtime remained alive'):
            session._close()
        assert attempts == []
    finally:
        release.set()
        thread.join(1.0)


@pytest.mark.parametrize('alive', [False, True])
def test_pressure_join_cleanup_attempts_independent_resources_and_retains_live_server(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    alive: bool,
) -> None:
    attempts: list[str] = []
    initiating = ValueError('join failed')

    def join(*_args: object, **_kwargs: object) -> None:
        raise initiating

    def close_client() -> None:
        attempts.append('client')
        raise OSError('client cleanup failed')

    def record_join(_timeout: float) -> None:
        attempts.append('join')

    module = sys.modules[__name__]
    monkeypatch.setattr(module, 'EventServer', Mock(return_value=_PressureCleanupServer(attempts)))
    monkeypatch.setattr(
        module,
        'EventClient',
        Mock(return_value=SimpleNamespace(connect=lambda: None, close=close_client)),
    )
    monkeypatch.setattr(
        module,
        'GroupChannel',
        Mock(return_value=SimpleNamespace(close=lambda: attempts.append('channel'))),
    )
    monkeypatch.setattr(
        module,
        'GroupLane',
        Mock(
            return_value=SimpleNamespace(
                setup=lambda: None, join=join, close=lambda: attempts.append('lane')
            )
        ),
    )
    monkeypatch.setattr(
        module.threading,
        'Thread',
        Mock(
            return_value=SimpleNamespace(
                start=lambda: None,
                join=record_join,
                is_alive=lambda: alive,
            )
        ),
    )
    context = cast(
        '_PressureJoinContext',
        SimpleNamespace(
            _socket_path=tmp_path / 'events.sock',
            _site=object(),
            _group=object(),
            _provider=object(),
            _plugin=object(),
        ),
    )
    with pytest.raises(ValueError, match='join failed') as caught:
        _run_pressure_join(
            context, cast('GroupState', object()), boundary=41, live_event_id=None, pressure=False
        )
    assert caught.value is initiating
    assert attempts == ['client', 'join', 'channel', 'lane'] + ([] if alive else ['server'])
    assert any('client cleanup failed' in note for note in initiating.__notes__)
    if alive:
        assert any('source thread remained alive' in note for note in initiating.__notes__)


@pytest.mark.parametrize('phase', ['construct', 'start'])
@pytest.mark.parametrize(
    'failed_name',
    ['source:audit', 'source:events', 'lane:local', 'operator-executor', 'monitor'],
)
def test_runtime_unwinds_partial_thread_startup(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    failed_name: str,
) -> None:
    spec = build_worker_specs(_site((_group('local', worker='default'),)))[0]
    operations: list[str] = []
    components = {
        name: _Component(name, operations)
        for name in ('source:audit', 'source:events', 'lane:local')
    }
    cache = _ComponentHealthCache(site='main')
    original = RuntimeError('injected startup failure')
    started: list[threading.Thread] = []
    joined: list[str] = []
    forced: list[int] = []
    thread_type = threading.Thread

    class StartupThread(thread_type):
        def __init__(self, **kwargs: object) -> None:
            name = cast('str', kwargs['name'])
            if phase == 'construct' and name == f'sg-event-daemon:{failed_name}':
                raise original
            super().__init__(
                target=cast('Callable[..., object]', kwargs['target']),
                args=cast('tuple[object, ...]', kwargs.get('args', ())),
                name=name,
            )

        def join(self, timeout: float | None = None) -> None:
            assert self in started, 'joined a thread whose start failed'
            joined.append(self.name)
            super().join(timeout)

        def start(self) -> None:
            if phase == 'start' and self.name == f'sg-event-daemon:{failed_name}':
                raise original
            super().start()
            started.append(self)

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='partial-start',
        source_factory=lambda name, _slots, _host: components[f'source:{name}'],
        lane_factory=lambda name, _slots, _host: components[f'lane:{name}'],
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        force_exit=forced.append,
        health_cache=cache,
    )
    monkeypatch.setattr(worker_module.threading, 'Thread', StartupThread)
    try:
        with pytest.raises(RuntimeError, match='injected startup failure') as caught:
            runtime.start()
        assert caught.value is original
        assert all(f'stop:{name}' in operations for name in components)
        assert all(not thread.is_alive() for thread in started)
        assert set(joined) == {thread.name for thread in started}
        assert operations[-3:] == [
            'close:lane:local',
            'close:source:events',
            'close:source:audit',
        ]
        assert not runtime._resources_open
        assert runtime._stuck_threads == ()
        assert forced == []
        assert cache._snapshot()._watchdogs == ()
        with pytest.raises(RuntimeError, match='startup failed'):
            runtime.start()
        with pytest.raises(RuntimeError, match='not started'):
            runtime.wait()
        assert runtime._shutdown() == ()
        assert operations.count('close:source:audit') == 1
    finally:
        for component in components.values():
            component._release.set()
        runtime.request_stop('test-cleanup')
        for thread in started:
            thread.join(1.0)


@pytest.mark.parametrize('phase', ['prepare', 'factory', 'close'])
def test_partial_startup_preserves_original_error_through_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    spec = build_worker_specs(_site(()))[0]
    original = RuntimeError('original startup failure')
    operations: list[str] = []
    component = _Component('audit', operations)

    def prepare() -> None:
        if phase == 'prepare':
            raise original

    def source_factory(
        name: str, _slots: _CatchupLimiter, _host: CheckpointHost
    ) -> _WorkerComponent:
        if name == 'events':
            raise original
        return component

    def close() -> None:
        raise RuntimeError('secondary cleanup failure')

    if phase == 'close':
        monkeypatch.setattr(component, '_close', close)
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='partial-construction',
        source_factory=source_factory,
        lane_factory=lambda _name, _slots, _host: component,
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        prepare=prepare,
    )
    with pytest.raises(RuntimeError, match='original startup failure') as caught:
        runtime.start()
    assert caught.value is original
    assert runtime._resources_open == (phase == 'close')
    if phase == 'factory':
        assert operations == ['stop:audit', 'close:audit']
    with pytest.raises(RuntimeError, match='startup failed'):
        runtime.start()


@pytest.mark.parametrize('stop', ['none', 'failure', 'shutdown'])
def test_eager_catchup_batches_real_reader_pages_and_stops_before_next_fetch(
    tmp_path: Path,
    stop: str,
) -> None:
    provider_state = _RestartProviderState()
    for event_id in range(42, 48):
        provider_state._publish(event_id)
    provider = _restart_provider(provider_state)
    group = _group('local', worker='default')
    site = _runtime_site(group)
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    watchdog = component._watchdog
    plugin = _health_failure_plugin('callback')
    invoked: list[int] = []

    def callback(context: EventContext) -> None:
        invoked.append(cast('int', context.event['id']))
        if stop == 'failure':
            raise RuntimeError('halt this page')
        lane.request_shutdown()

    plugin = replace(plugin, callbacks=(replace(plugin.callbacks[0], callback=callback),))
    with (
        _open_group_health_state(tmp_path / 'group.db') as state,
        CatchupReader(
            provider=provider,
            site_url=site.server,
            credential=site.credentials['source'],
            batch_size=3,
            fetch_interval_secs=1.0,
            waiter=_SourceHealthWaiter(),
        ) as reader,
        closing(
            GroupLane(
                state=state,
                site=site,
                group=group,
                provider=provider,
                plugins=() if stop == 'none' else (plugin,),
                worker_instance='worker-1',
                health_interval_secs=1.0,
                watchdog=watchdog,
            )
        ) as lane,
    ):
        assert lane.setup() is None
        resources = SimpleNamespace(
            _state=state,
            _lane=lane,
            _reader=reader,
            _coordination=SimpleNamespace(
                synchronize=lambda: _synchronization(47),
            ),
        )
        driver = cast(
            'Callable[[object, object, object], None]', vars(component_type)['_catch_up_eager']
        )
        statements: list[str] = []
        state._database.connection.set_trace_callback(statements.append)
        if stop == 'none':
            driver(component, resources, SimpleNamespace(identity=SimpleNamespace(boundary=47)))
        else:
            error_type = cast(
                'type[RuntimeError]',
                vars(worker_module)[
                    '_GroupHaltedCatchupError'
                    if stop == 'failure'
                    else '_CooperativeComponentStopError'
                ],
            )
            with pytest.raises(error_type):
                driver(
                    component, resources, SimpleNamespace(identity=SimpleNamespace(boundary=47))
                )
        state._database.connection.set_trace_callback(None)
        snapshot = state.snapshot()

    if stop == 'none':
        assert snapshot.cursor == 47
        assert statements.count('COMMIT') == 2
        assert invoked == []
    else:
        assert snapshot.cursor == (41 if stop == 'failure' else 42)
        assert invoked == [42]
        assert set(provider_state._observed) == {42, 43, 44}


def _checkpoint_shutdown_runtime(
    forced: list[tuple[int, str]],
    *,
    health_interval_secs: float = 0.005,
) -> WorkerRuntime:
    spec = build_worker_specs(_site(()))[0]
    components = {name: _Component(name, []) for name in spec.sources}
    return WorkerRuntime(
        spec=spec,
        spawn_instance='checkpoint-shutdown',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=lambda _health: None,
        health_interval_secs=health_interval_secs,
        checkpoint_interval_secs=0.4,
        shutdown_deadline_secs=0.03,
        max_concurrent_catchup_readers=1,
        force_exit=lambda code: forced.append((code, threading.current_thread().name)),
    )


def _finish_checkpoint_runtime(
    runtime: WorkerRuntime, *, cleanup_timeout_secs: float = 10.0
) -> None:
    runtime._checkpoint_host.request_stop()
    deadline_at = time.monotonic() + cleanup_timeout_secs
    threads = list(runtime._owned_threads())
    monitor = runtime._monitor_thread
    if monitor is not None:
        threads.append(('monitor', monitor))
    for _identity, thread in threads:
        thread.join(max(0.0, deadline_at - time.monotonic()))
    live = tuple(identity for identity, thread in threads if thread.is_alive())
    assert live == (), f'checkpoint cleanup left live threads: {live}'
    runtime._checkpoint_host.close()
    assert runtime._shutdown() == ()


def test_checkpoint_cleanup_reports_thread_that_remains_alive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _checkpoint_shutdown_runtime([])
    entered = threading.Event()
    release = threading.Event()

    def block() -> None:
        entered.set()
        release.wait()

    thread = threading.Thread(target=block)
    thread.start()
    monkeypatch.setattr(runtime, '_owned_threads', lambda: (('blocked-operator', thread),))
    try:
        assert entered.wait(1)
        with pytest.raises(AssertionError, match='blocked-operator'):
            _finish_checkpoint_runtime(runtime, cleanup_timeout_secs=0.01)
    finally:
        release.set()
        thread.join(1)
        assert not thread.is_alive()
        runtime._checkpoint_host.close()


def test_checkpoint_registration_after_stop_is_cooperative(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    schema = Schema(version=1, statements=('CREATE TABLE entries (id INTEGER)',))
    with open_database(path, 'events', schema, synchronous='NORMAL'):
        pass
    entered = threading.Event()
    release = threading.Event()
    opened: list[Path] = []
    forced: list[tuple[int, str]] = []
    runtime = _checkpoint_shutdown_runtime(forced)

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        opened.append(path)
        return Checkpointer(connection, path, interval_secs=interval_secs)

    host = CheckpointHost(
        interval_secs=60,
        synchronous='NORMAL',
        on_failure=runtime._component_failed,
        checkpointer_factory=factory,
    )
    runtime._checkpoint_host = host

    def run() -> None:
        entered.set()
        assert release.wait(1)
        host.register(path, 'events', schema)

    component = _InterruptibleComponent(run, lambda: None)

    def source_factory(
        _name: str, _slots: _CatchupLimiter, _host: CheckpointHost
    ) -> _InterruptibleComponent:
        return component

    runtime._source_factory = source_factory
    runtime.start()
    try:
        assert entered.wait(1)
        runtime.request_stop('startup-stopped')
        release.set()
        assert runtime.wait() == ()
        assert runtime.fatal_reason is None
        assert opened == []
        assert forced == []
    finally:
        release.set()
        runtime.request_stop('test-cleanup')
        _finish_checkpoint_runtime(runtime)


@pytest.mark.parametrize('health_interval_secs', [0.005, 0.5])
def test_checkpoint_retry_retains_monitor_until_shutdown_deadline(
    tmp_path: Path,
    health_interval_secs: float,
) -> None:
    path = tmp_path / 'state.db'
    schema = Schema(version=1, statements=('CREATE TABLE entries (id INTEGER)',))
    with open_database(path, 'events', schema, synchronous='NORMAL'):
        pass
    sleeping = threading.Event()
    forced: list[tuple[int, str]] = []
    runtime = _checkpoint_shutdown_runtime(forced, health_interval_secs=health_interval_secs)

    def retry_sleep(seconds: float) -> None:
        sleeping.set()
        time.sleep(seconds)

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            run_checkpoint=lambda: (1, 5, 0),
            random_value=lambda: 1,
            sleep=retry_sleep,
        )

    host = CheckpointHost(
        interval_secs=0.4,
        synchronous='NORMAL',
        on_failure=runtime._component_failed,
        checkpointer_factory=factory,
    )
    runtime._checkpoint_host = host
    runtime.start()
    try:
        host.register(path, 'events', schema)
        assert sleeping.wait(2)
        started = time.monotonic()
        runtime.request_stop('during-checkpoint-retry')
        live = runtime.wait()
        elapsed = time.monotonic() - started
        assert forced == [(1, 'sg-event-daemon:monitor')]
        assert live == ('checkpoint:events',)
        assert runtime._stuck_threads == live
        assert 'checkpoint:events' in runtime._current_health(time.monotonic()).thread_names
        assert elapsed < 0.15
        assert runtime._resources_open
    finally:
        runtime.request_stop('test-cleanup')
        _finish_checkpoint_runtime(runtime)
    assert not runtime._resources_open


@pytest.mark.parametrize('boundary', ['open', 'attempt', 'close'])
def test_checkpoint_blocked_owner_remains_monitored_through_cleanup(
    boundary: Literal['open', 'attempt', 'close'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / 'state.db'
    schema = Schema(version=1, statements=('CREATE TABLE entries (id INTEGER)',))
    with open_database(path, 'events', schema, synchronous='NORMAL'):
        pass
    entered = threading.Event()
    opened = threading.Event()
    release = threading.Event()
    closed_on: list[str] = []
    forced: list[tuple[int, str]] = []
    runtime = _checkpoint_shutdown_runtime(forced)

    def block() -> None:
        entered.set()
        assert release.wait(2)

    original_close = DatabaseHandle.close

    def close_checkpoint(database: DatabaseHandle) -> None:
        if boundary == 'close':
            block()
        original_close(database)
        closed_on.append(threading.current_thread().name)

    def open_checkpoint(*_args: object, **_options: object) -> DatabaseHandle:
        if boundary == 'open':
            block()
        database = open_database(path, 'events', schema, synchronous='NORMAL', create=False)
        opened.set()
        return database

    def factory(
        connection: sqlite3.Connection, path: Path, *, interval_secs: float
    ) -> Checkpointer:
        def checkpoint() -> tuple[int, int, int]:
            if boundary == 'attempt':
                block()
            return 0, 0, 0

        return Checkpointer(
            connection, path, interval_secs=interval_secs, run_checkpoint=checkpoint
        )

    monkeypatch.setattr(checkpoint_host_module, 'open_database', open_checkpoint)
    monkeypatch.setattr(DatabaseHandle, 'close', close_checkpoint)
    host = CheckpointHost(
        interval_secs=0.01,
        synchronous='NORMAL',
        on_failure=runtime._component_failed,
        checkpointer_factory=factory,
    )
    runtime._checkpoint_host = host
    runtime.start()
    try:
        host.register(path, 'events', schema)
        assert (opened if boundary == 'close' else entered).wait(1)
        runtime.request_stop(f'blocked-checkpoint-{boundary}')
        assert entered.wait(1)
        _assert_bounded_checkpoint_shutdown(runtime, release, forced, closed_on)
    finally:
        release.set()
        runtime.request_stop('test-cleanup')
        _finish_checkpoint_runtime(runtime)
    assert closed_on == ['sg-event-daemon:checkpoint:events']
    assert not runtime._resources_open


def test_checkpoint_secure_open_failure_stops_runtime_and_retains_reason(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    forced: list[tuple[int, str]] = []
    runtime = _checkpoint_shutdown_runtime(forced)
    uncaught: list[BaseException | None] = []

    def open_file(*_args: object, **_kwargs: object) -> int:
        raise OSError(errno.EMFILE, 'Too many open files')

    monkeypatch.setattr(platform_module.os, 'open', open_file)

    def capture_uncaught(args: threading.ExceptHookArgs) -> None:
        uncaught.append(args.exc_value)

    monkeypatch.setattr(threading, 'excepthook', capture_uncaught)
    runtime.start()
    try:
        runtime._checkpoint_host.register(
            tmp_path / 'unopened.db', 'events', Schema(version=1, statements=())
        )
        for thread in runtime._checkpoint_host.threads():
            thread.join(1)
            assert not thread.is_alive()
        reason = runtime.fatal_reason
        assert reason is not None
        assert reason.startswith('checkpoint:events thread failed: SecurityError:')
        assert runtime._stop_event.is_set()
        assert runtime.wait() == ()
        assert runtime.fatal_reason == reason
        assert not runtime._resources_open
        assert runtime._monitor_thread is not None
        assert not runtime._monitor_thread.is_alive()
        assert uncaught == []
        assert forced == []
    finally:
        runtime.request_stop('test-cleanup')
        runtime.wait()


def test_checkpoint_startup_unwind_uses_remaining_shutdown_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / 'state.db'
    schema = Schema(version=1, statements=('CREATE TABLE entries (id INTEGER)',))
    with open_database(path, 'events', schema, synchronous='NORMAL'):
        pass
    entered = threading.Event()
    release = threading.Event()
    forced: list[tuple[int, str]] = []
    runtime = _checkpoint_shutdown_runtime(forced)
    original_start = threading.Thread.start
    original_request_stop = runtime.request_stop
    checkpoint_join_timeouts: list[float | None] = []
    operator_returned, operator_release = _gate_checkpoint_startup_operator(
        runtime, monkeypatch, release, checkpoint_join_timeouts
    )

    def open_checkpoint(*_args: object, **_options: object) -> DatabaseHandle:
        entered.set()
        assert release.wait(2)
        return open_database(path, 'events', schema, synchronous='NORMAL', create=False)

    def start(thread: threading.Thread) -> None:
        if thread.name == 'sg-event-daemon:monitor':
            assert entered.wait(1)
            raise RuntimeError('monitor-start-failed')
        original_start(thread)

    def request_stop(reason: str) -> None:
        original_request_stop(reason)
        if reason == 'runtime-startup-failed':
            assert runtime._shutdown_deadline_at is not None
            runtime._shutdown_deadline_at = runtime._clock() - 1.0

    monkeypatch.setattr(checkpoint_host_module, 'open_database', open_checkpoint)
    monkeypatch.setattr(threading.Thread, 'start', start)
    monkeypatch.setattr(runtime, 'request_stop', request_stop)
    runtime._checkpoint_host.register(path, 'events', schema)
    try:
        with pytest.raises(RuntimeError, match='monitor-start-failed'):
            runtime.start()
        assert operator_returned.wait(2)
        assert checkpoint_join_timeouts == [0.0]
        assert 'checkpoint:events' in runtime._stuck_threads
        assert runtime._resources_open
        assert forced == []
    finally:
        release.set()
        try:
            _finish_checkpoint_runtime(runtime)
        finally:
            operator_release.set()
    assert not runtime._resources_open


@pytest.mark.parametrize(
    'lane_state',
    ['live-idle', 'detached', 'join-retry', 'parked', 'halted'],
)
def test_lane_ticks_once_per_interval_in_each_scheduling_path(
    monkeypatch: pytest.MonkeyPatch, lane_state: str
) -> None:
    component_object, resources_object = _lifecycle_probe(
        'sync',
        durable_status='parked' if lane_state == 'parked' else 'running',
        setup_failure=None,
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(_channel=resources._channel)
    component._lifecycle = lifecycle
    clock = [100.0]
    calls: list[tuple[float, bool]] = []
    vars(resources._context._source_config)['fetch_interval_secs'] = 1.0
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])

    def wait(timeout: float | None = None) -> bool:
        assert timeout is not None
        clock[0] += timeout
        return False

    def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
        del force
        calls.append((clock[0], scan_obligations))

    monkeypatch.setattr(component._wake, 'wait', wait)
    monkeypatch.setattr(resources._coordination, '_service', coordinate)
    for _ in range(4):
        if lane_state.startswith('live'):
            component._service_due(resources, lifecycle)
            clock[0] += 1.0
        elif lane_state in {'parked', 'halted'}:
            component._drive_halted(resources, lifecycle)
        else:
            component._pause(resources, seconds=3.0 if lane_state == 'join-retry' else None)
    assert len(calls) >= 4
    assert calls[0][0] == 100.0
    assert all(0 < right[0] - left[0] <= 1.05 for left, right in pairwise(calls))
    assert all(scan == lane_state.startswith('live') for _at, scan in calls)


@pytest.mark.parametrize(
    ('ordering', 'provider_delay'), [('eager', 0.0), ('strict', 0.0), ('strict', 1.0)]
)
@pytest.mark.parametrize('halt_cursor', [None, 43, 47])
def test_catchup_ticks_between_committed_callback_prefixes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: str,
    provider_delay: float,
    halt_cursor: int | None,
) -> None:
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    component = _unopened_group_component(_LaneComponent)
    provider_state = _RestartProviderState()
    for event_id in range(42, 48):
        provider_state._publish(event_id)
    provider = _restart_provider(provider_state)
    group = _group('local', worker='default')
    site = _runtime_site(group)
    invoked: list[int] = []
    ticks: list[tuple[float, int]] = []
    active_delivery = [False]

    with (
        _open_group_health_state(tmp_path / 'group.db') as state,
        CatchupReader(
            provider=provider,
            site_url=site.server,
            credential=site.credentials['source'],
            batch_size=10,
            fetch_interval_secs=0.5,
            waiter=_SourceHealthWaiter(),
        ) as reader,
        closing(
            GroupLane(
                state=state,
                site=site,
                group=group,
                provider=provider,
                plugins=(_catchup_tick_plugin(clock, invoked),),
                worker_instance='worker-1',
                health_interval_secs=1.0,
                watchdog=component._watchdog,
            )
        ) as lane,
    ):
        assert lane.setup() is None
        channel = GroupChannel(group='local', ordering='eager', max_events=10, max_bytes=262_144)
        component._lifecycle = worker_module._LaneLifecycle(_channel=channel)

        def coordinate(
            *, scan_obligations: bool = True, force: bool = False
        ) -> GroupSnapshot | None:
            del force
            assert not scan_obligations
            assert not active_delivery[0], 'coordination mutated an active delivery snapshot'
            snapshot = state.snapshot()
            ticks.append((clock[0], snapshot.cursor))
            assert snapshot.in_flight_event_id is None
            if snapshot.cursor == halt_cursor:
                return state.park(
                    reason='maintenance',
                    identity=AuditIdentity.current(),
                    wall_clock_time=1.0,
                    expected_revision=snapshot.revision,
                )
            if snapshot.cursor != 43 or snapshot.obligation_count:
                return None
            _bind_worker_range(state, 'range-90', 90)
            return state.record_delivery_floor_skip(41, expected_revision=snapshot.revision)

        resources = cast(
            '_LaneResources',
            SimpleNamespace(
                _state=state,
                _lane=lane,
                _reader=reader
                if ordering == 'eager'
                else SimpleNamespace(
                    strict_step=partial(
                        _strict_prepared_probe_step, state, clock, provider_delay, active_delivery
                    )
                ),
                _client=SimpleNamespace(),
                _context=SimpleNamespace(
                    _holdback_secs=0.0, _source_config=SimpleNamespace(fetch_interval_secs=0.5)
                ),
                _coordination=SimpleNamespace(
                    synchronize=lambda: _synchronization(47),
                    _service=coordinate,
                ),
            ),
        )

        def wait(seconds: float | None = None) -> bool:
            assert seconds is not None
            clock[0] += seconds
            return False

        monkeypatch.setattr(component._wake, 'wait', wait)
        target = cast('JoinTarget', SimpleNamespace(identity=SimpleNamespace(boundary=47)))
        driver = component._catch_up_eager if ordering == 'eager' else component._catch_up_strict
        with (
            pytest.raises(worker_module._GroupHaltedCatchupError) if halt_cursor else nullcontext()
        ):
            driver(resources, target)
        assert invoked == list(range(42, (halt_cursor or 47) + 1))
        offset = 1 if provider_delay else 0
        assert ticks[0] == (100.0, 41)
        assert ticks[1 + offset] == (100.5 + provider_delay, 43)
        assert all(right[0] - left[0] <= 0.55 + provider_delay for left, right in pairwise(ticks))
        assert state.snapshot().cursor == (halt_cursor or 47)


@pytest.mark.parametrize('operation', ['setup', 'resume'])
def test_due_coordination_runs_when_lifecycle_callback_returns(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    component_object, resources_object = _lifecycle_probe(
        'sync', durable_status='running', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(_channel=resources._channel)
    lifecycle._was_running = True
    lifecycle._next_coordination_at = 101.0
    component._lifecycle = lifecycle
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    calls: list[tuple[float, bool]] = []

    def callback() -> object:
        clock[0] = 102.0
        return None if operation == 'setup' else SimpleNamespace(status='running')

    def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
        del force
        calls.append((clock[0], scan_obligations))

    monkeypatch.setattr(resources._lane, operation, callback)
    monkeypatch.setattr(resources._coordination, '_service', coordinate)
    if operation == 'setup':
        component._callback_gate_snapshot(resources, lifecycle)
    else:
        component._resume_lane(resources, lifecycle)
    assert calls == [(102.0, False)]


@pytest.mark.parametrize('failure', ['remote', 'local', 'write', 'catchup'])
def test_failed_join_pause_preserves_failure_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    component_object, resources_object = _lifecycle_probe(
        'sync', durable_status='running', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(_channel=resources._channel)
    component._lifecycle = lifecycle
    connected = [True]
    reconnects: list[float] = []
    if failure != 'local':
        vars(resources)['_client'] = _unconnected_event_client(tmp_path / 'events.sock')
    monkeypatch.setattr(EventClient, 'connected', property(lambda _client: connected[0]))

    def reconnect(_client: EventClient) -> NoReturn:
        reconnects.append(clock[0])
        connected[0] = False
        raise ConnectionError('source remains unavailable')

    def wait(seconds: float) -> bool:
        clock[0] += seconds
        return False

    error = (
        WriteLockTimeoutError('owner write budget', 'file')
        if failure == 'write'
        else ConnectionError('event coordination disconnected')
    )

    def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
        del force
        assert not scan_obligations
        if failure != 'remote':
            raise error

    def join(*_args: object, **_kwargs: object) -> JoinRetry:
        if failure in {'remote', 'local'}:
            raise error
        return JoinRetry(
            attempt=JoinAttemptIdentity(
                site='main',
                source='events',
                group='local',
                join_kind='eager',
                group_generation=7,
                attempt_token=os.urandom(16).hex(),
            ),
            retry_after_secs=0.001,
            headroom_events=0,
            headroom_bytes=0,
        )

    def build_channel(_context: object, _state: object) -> GroupChannelInterface:
        return resources._channel

    monkeypatch.setattr(EventClient, 'reconnect', reconnect)
    monkeypatch.setattr(component._wake, 'wait', wait)
    monkeypatch.setattr(component, '_build_channel', build_channel)
    monkeypatch.setattr(resources._lane, 'join', join)
    monkeypatch.setattr(resources._coordination, '_service', coordinate)
    if failure == 'remote':
        assert not component._join_lane(resources, lifecycle)
        assert reconnects == [100.0]
        assert clock[0] == 100.001
        assert not lifecycle._joined
    else:
        operation = (
            partial(component._service_catchup, resources, 0.001)
            if failure == 'catchup'
            else partial(component._join_lane, resources, lifecycle)
        )
        with pytest.raises(type(error)) as caught:
            operation()
        assert caught.value is error
        assert reconnects == []


@pytest.mark.parametrize('command', ['adopt', 'reserve'])
def test_laneless_second_mailbox_command_refreshes_changed_group_index(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    path = _source_database_path(tmp_path)
    with _open_coordination_source(path) as source:
        source.initialize(41, incarnation='incarnation-1')
        poller = cast(
            'SourcePoller',
            SimpleNamespace(
                event_channel_lost=Mock(),
                adopt_stream=Mock(return_value=source.snapshot()),
            ),
        )
        surface = worker_module._SourceJoinSurface(
            bootstrap_retry_secs=0.05,
            coordinator=JoinCoordinator(
                state=source, fanout=EventFanout(), source_generation='source-1'
            ),
            poller=poller,
            state=source,
            inventory={
                'local': ActiveSourceGroupAssignment(
                    source='events',
                    group='local',
                    group_generation=7,
                    start_mode='initialize',
                )
            },
            strict_buffer=None,
        )
        surface._reconcile_groups()
        assert surface._durable_generations == {}
        monkeypatch.setattr(source, 'reserve_stream_adoption', Mock())

        def bootstrap(_surface: _SourceJoinSurface) -> None:
            _bootstrap_coordination_group(path)

        def invoke(active: _SourceJoinSurface) -> object:
            if command == 'reserve':
                return active._reserve_stream_adoption(
                    command_fingerprint='fingerprint', reason='test'
                )
            return active._adopt_stream(
                adoption_id='adoption',
                command_fingerprint='fingerprint',
                new_stream_fingerprint='replacement',
                reason='test',
                groups_parked=True,
            )

        mailbox = _OwnerMailbox[worker_module._SourceJoinSurface]('source')
        first = mailbox._submit(bootstrap)
        second = mailbox._submit(invoke)
        component = object.__new__(_SourceComponent)
        vars(component).update(
            _assignment=SimpleNamespace(source_group_assignments=()),
            _config=SimpleNamespace(sites={'main': SimpleNamespace(groups={})}),
            _mailbox=mailbox,
            _grant_lock=threading.Lock(),
            _grants=deque(),
            _health_interval_secs=1.0,
            _next_pending_health_refresh_at=None,
            _pending_group_assignments=(),
            _pending_group_detail={},
            _pending_group_detail_dirty=True,
            _publish_health=Mock(),
            _source='events',
            _spec=SimpleNamespace(site='main'),
        )
        server = cast('EventServer', SimpleNamespace(poll=Mock(), dispatch_join_requests=Mock()))
        component._service_internal(server, surface, poller)
        assert first._complete.is_set()
        assert first._error is None
        assert second._complete.is_set()
        assert second._error is None
        assert surface._durable_generations == {'local': 7}


@pytest.mark.parametrize('drift', ['identity', 'ordering'])
@pytest.mark.parametrize('retry', ['exact', 'conflict', 'stale'])
def test_source_disposition_setup_preserves_durable_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, drift: str, retry: str
) -> None:
    config, spec, assignment, snapshot = _pending_group_runtime(tmp_path)
    path = _source_database_path(tmp_path)
    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        _runtime,
        control,
    ):
        _receive_typed_health(control, lambda update: len(update.groups) == 1)
        command = _pending_group_command(snapshot, 'reset-to-latest')
        control.send(command)
        applied = _receive_type(control, CommandResult)
        assert applied.ok
        before = _drift_disposition_policy(path, 'local', drift)
        if retry == 'conflict':
            command = replace(command, arguments={**command.arguments, 'reason': 'conflicting'})
        elif retry == 'stale':
            command = replace(
                command,
                arguments={
                    **command.arguments,
                    'disposition_id': 'new-stale',
                    'reserved_generation': 12,
                },
            )
        control.send(command)
        result = _receive_type(control, CommandResult)
        assert result.ok == (retry == 'exact')
        if retry == 'exact':
            assert result.result == applied.result
        with closing(sqlite3.connect(path)) as connection, connection:
            assert tuple(connection.iterdump()) == before
            assert connection.execute('SELECT COUNT(*) FROM epochs').fetchone() == (1,)


@pytest.mark.parametrize('fatal', [False, True], ids=['ordinary-stop', 'checkpoint-fatal'])
def test_operator_shutdown_registration_outlives_monitor_publications(fatal: bool) -> None:
    spec = build_worker_specs(_site(()))[0]
    cache = _ComponentHealthCache(site=spec.site)
    components = {name: _Component(name, [], wedge=True) for name in spec.sources}
    published_after_operator = threading.Event()
    snapshots: list[tuple[WatchdogHealth, ...]] = []
    runtime: WorkerRuntime

    def publish(_health: WorkerHealth) -> None:
        operator = runtime._threads.get('operator-executor')
        if operator is not None and not operator.is_alive():
            snapshots.append(cache._snapshot()._watchdogs)
            published_after_operator.set()

    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='shutdown-inventory',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=publish,
        health_cache=cache,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1,
        shutdown_deadline_secs=2,
        max_concurrent_catchup_readers=1,
        force_exit=lambda _code: pytest.fail('cooperative shutdown forced exit'),
    )
    runtime.start()
    try:
        assert all(component._started.wait(1) for component in components.values())
        if fatal:
            runtime._component_failed('checkpoint:events thread failed: injected bound')
        else:
            runtime.request_stop('ordinary-stop')
        operator = runtime._threads['operator-executor']
        operator.join(1)
        assert not operator.is_alive()
        runtime._monitor_wake.set()
        assert published_after_operator.wait(1)
        assert runtime._monitor_thread is not None
        assert runtime._monitor_thread.is_alive()
        for watchdogs in snapshots:
            assert len(watchdogs) == 1
            assert watchdogs[0].kind == 'operator'
            assert watchdogs[0].phase == 'shutdown'
            assert watchdogs[0].deadline_at == runtime._shutdown_deadline_at
        runtime._deregister_operator_watchdog()
        assert len(cache._snapshot()._watchdogs) == 1
        for component in components.values():
            component._release.set()
        assert runtime.wait() == ()
        assert not runtime._monitor_thread.is_alive()
        assert cache._snapshot()._watchdogs == ()
        assert snapshots[-1][0].deadline_at == runtime._shutdown_deadline_at
    finally:
        for component in components.values():
            component._release.set()
        runtime.request_stop('test-cleanup')
        runtime._shutdown()


@pytest.mark.parametrize('first', ['source', 'lane'])
@pytest.mark.parametrize('fatal', [False, True], ids=['ordinary-stop', 'checkpoint-fatal'])
def test_completed_component_health_survives_terminal_monitor_frame(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, first: str, fatal: bool
) -> None:
    site = _runtime_site(_group('local', worker='default'))
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = _local_assignment(spec, 'local', 'initialize')
    releases = {kind: threading.Event() for kind in ('source', 'lane')}
    now = [100.0]
    _configure_shutdown_probe_clock(monkeypatch, now)

    def run_body(component: _SourceComponent | _LaneComponent) -> None:
        _run_shutdown_probe_body(component, releases)

    monkeypatch.setattr(_SourceComponent, '_run_body', run_body)
    monkeypatch.setattr(_LaneComponent, '_run_body', run_body)
    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        runtime,
        control,
    ):
        cache = runtime._health_cache
        assert cache is not None
        _receive_typed_health(control, lambda update: bool(update.sources) and bool(update.groups))
        try:
            if fatal:
                runtime._component_failed('checkpoint:events thread failed: injected bound')
            else:
                runtime.request_stop('ordinary-stop')
            releases[first].set()
            identity = 'source:events' if first == 'source' else 'lane:local'
            runtime._threads[identity].join(1)
            runtime._threads['operator-executor'].join(1)
            assert not runtime._threads[identity].is_alive()
            runtime._close_components()
            assert len(cache._snapshot()._watchdogs) == 3
            now[0] = 101.5
            assert runtime._shutdown_deadline_at == 102.0
            runtime._monitor_wake.set()
            update = _receive_typed_health(
                control,
                lambda item: all(watchdog.phase == 'shutdown' for watchdog in item.watchdogs),
            )
            _assert_shutdown_inventory(update, runtime._shutdown_deadline_at)
            for release in releases.values():
                release.set()
            terminal = _terminal_monitor_frame(runtime, control, update)
            _assert_shutdown_inventory(terminal, runtime._shutdown_deadline_at)
            assert runtime.wait() == ()
            assert cache._snapshot()._watchdogs == ()
            assert cache._snapshot()._sources == ()
            assert cache._snapshot()._groups == ()
        finally:
            for release in releases.values():
                release.set()


@pytest.mark.parametrize('stop_during_wait', [False, True])
def test_lane_retry_handoff_keeps_current_owner_phase(
    monkeypatch: pytest.MonkeyPatch, stop_during_wait: bool
) -> None:
    component, context, now, cache = _recovery_owner_phase_probe(monkeypatch)
    observations: list[tuple[str, WatchdogHealth]] = []
    original_is_set = component._stop.is_set
    waited = False

    def configure() -> _LaneConfiguration:
        if waited:
            observations.append(('configure', cache._snapshot()._watchdogs[0]))
            component._request_stop(shutdown_deadline_at=200.0)
        return context

    def build_client(_context: _LaneConfiguration) -> tuple[EventClient, str]:
        raise worker_module._RemoteBootstrapTransportError('injected remote retry')

    def wait(timeout: float | None = None) -> bool:
        nonlocal waited
        assert timeout == 30.0
        observations.append(('wait', cache._snapshot()._watchdogs[0]))
        now[0] = 130.0
        if stop_during_wait or waited:
            component._request_stop(shutdown_deadline_at=200.0)
        waited = True
        return original_is_set()

    def is_set() -> bool:
        if waited:
            observations.append(('handoff', cache._snapshot()._watchdogs[0]))
        return original_is_set()

    monkeypatch.setattr(component, '_configuration', configure)
    monkeypatch.setattr(component, '_build_client', build_client)
    monkeypatch.setattr(component._stop, 'wait', wait)
    monkeypatch.setattr(component._stop, 'is_set', is_set)
    component._run_body()
    first_handoff = next(sample for boundary, sample in observations if boundary == 'handoff')
    assert first_handoff.phase == 'idle'
    assert first_handoff.deadline_at == 135.0
    if not stop_during_wait:
        configured = next(sample for boundary, sample in observations if boundary == 'configure')
        assert configured.phase == 'startup'
        assert configured.deadline_at == 140.0
    now[0] = 136.0
    terminal = cache._snapshot()._watchdogs[0]
    assert terminal.phase == 'shutdown'
    assert terminal.deadline_at == 200.0
    assert terminal.deadline_at is not None
    assert 135.0 < now[0] < terminal.deadline_at


@pytest.mark.parametrize('stop_with_reload', [False, True])
def test_lane_reload_handoff_restores_current_attempt_phase(
    monkeypatch: pytest.MonkeyPatch, stop_with_reload: bool
) -> None:
    component, context, now, cache = _recovery_owner_phase_probe(monkeypatch)
    observations: list[WatchdogHealth] = []
    builds = 0
    original_transition = component._watchdog.transition

    def configure() -> _LaneConfiguration:
        nonlocal builds
        builds += 1
        if builds == 2:
            observations.append(cache._snapshot()._watchdogs[0])
        return context

    def transition(
        phase: WatchdogPhase, *, deadline_at: float | None, **identities: object
    ) -> None:
        assert not identities
        if phase == 'idle' and builds == 2:
            observations.append(cache._snapshot()._watchdogs[0])
        original_transition(phase, deadline_at=deadline_at)

    def drive(_resources: _LaneResources) -> None:
        component._close_partial_attempt(worker_module._PartialLaneAttempt())
        if builds == 1:
            assert component._watchdog.sample().deadline_at == 110.0
            now[0] = 130.0
            component._reload.set()
        if stop_with_reload or builds == 2:
            component._request_stop(shutdown_deadline_at=200.0)

    monkeypatch.setattr(component, '_configuration', configure)
    monkeypatch.setattr(component, '_drive_lane', drive)
    monkeypatch.setattr(component._watchdog, 'transition', transition)
    component._run_body()
    assert builds == (1 if stop_with_reload else 2)
    assert len(observations) == (0 if stop_with_reload else 2)
    for sample in observations:
        assert sample.phase == 'startup'
        assert sample.deadline_at == 140.0
    terminal = cache._snapshot()._watchdogs[0]
    assert terminal.phase == 'shutdown'
    assert terminal.deadline_at == 200.0


@contextmanager
def _deadline_monitor_runtime(
    tmp_path: Path,
) -> Generator[tuple[WorkerRuntime, WorkerControl, WorkerControl, list[float]]]:
    config = _runtime_config(tmp_path, _site(()))
    spec = WorkerSpec(
        worker_id='["named","empty"]',
        site='main',
        kind=WorkerKind._NAMED,
        name='empty',
        sources=(),
        groups=(),
    )
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='deadline-sample',
        source_generations=(),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1)
    now = [4.99]
    try:
        runtime = build_worker_runtime(config, spec, assignment, child)
        runtime._clock = lambda: now[0]
        runtime._health_interval_secs = 6
        cache = runtime._health_cache
        assert cache is not None
        yield runtime, parent, child, now
    finally:
        parent.close()
        child.close()
        parent_socket.close()
        child_socket.close()


def test_monitor_observes_hold_entered_after_frame_timestamp_and_honors_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _deadline_monitor_runtime(tmp_path) as (runtime, parent, _child, now):
        cache = runtime._health_cache
        assert cache is not None
        token = _watchdog_token()
        cell = ComponentWatchdog(
            kind='source',
            identity='events',
            worker_instance='deadline-sample',
            registration_token=token,
            startup_deadline_secs=30,
            clock=lambda: now[0],
        )
        cache._register_watchdog(
            kind='source',
            identity='events',
            worker_instance='deadline-sample',
            token=token,
            supplier=cell.sample,
        )
        waits: list[float] = []

        def enter_hold() -> None:
            if not runtime._stop_event.is_set():
                now[0] = 5
                cell.transition('write-hold', deadline_at=10)

        def stop_after_wait(seconds: float) -> bool:
            waits.append(seconds)
            runtime.request_stop('observed')
            return True

        monkeypatch.setattr(runtime, '_poll_control', enter_hold)
        monkeypatch.setattr(runtime._monitor_wake, 'wait', stop_after_wait)
        runtime._monitor()
        first = parent.receive(timeout=1)
        assert isinstance(first, HealthUpdate)
        hold = next(item for item in first.watchdogs if item.kind == 'source')
        assert first.worker.heartbeat_at == 4.99 < hold.heartbeat_at == 5
        assert hold.deadline_at == 10
        assert waits == [5]
        assert isinstance(parent.receive(timeout=1), HealthUpdate)


def test_monitor_pending_deadlines_follow_callback_slide_and_outer_restoration(
    tmp_path: Path,
) -> None:
    with _deadline_monitor_runtime(tmp_path) as (runtime, parent, _child, now):
        cache = runtime._health_cache
        assert cache is not None
        cache._group_sources['local'] = 'events'
        token = _watchdog_token()
        cell = ComponentWatchdog(
            kind='lane',
            identity='local',
            worker_instance='deadline-sample',
            registration_token=token,
            startup_deadline_secs=30,
            clock=lambda: now[0],
        )
        cache._register_watchdog(
            kind='lane',
            identity='local',
            worker_instance='deadline-sample',
            token=token,
            supplier=cell.sample,
        )
        cell.transition('callback', deadline_at=5)
        assert runtime._publish_monitor_health(now[0]) == 5
        assert isinstance(parent.receive(timeout=1), HealthUpdate)
        now[0] = 5.1
        cell.heartbeat(sliding_deadline_secs=2)
        assert runtime._publish_monitor_health(now[0]) == 7.1
        assert isinstance(parent.receive(timeout=1), HealthUpdate)
        cell.transition('internal', deadline_at=6)
        with cell.watch('write-hold', deadline_at=10):
            now[0] = 7
        assert runtime._publish_monitor_health(now[0]) is None
        restored = parent.receive(timeout=1)
        assert isinstance(restored, HealthUpdate)
        lane = next(item for item in restored.watchdogs if item.kind == 'lane')
        assert lane.phase == 'internal'
        assert lane.deadline_at == 6 < restored.worker.heartbeat_at
        assert runtime._monitor_wait_secs(None) == 6


@pytest.mark.parametrize('during_send', [False, True])
def test_monitor_schedules_from_sent_snapshot_and_confirms_without_spin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, during_send: bool
) -> None:
    with _deadline_monitor_runtime(tmp_path) as (runtime, parent, child, now):
        cache = runtime._health_cache
        assert cache is not None
        samples: list[str] = []
        for identity, deadline in (('events', 7.0), ('later', 5.0)):
            cell = ComponentWatchdog(
                kind='source',
                identity=identity,
                worker_instance='deadline-sample',
                registration_token=identity,
                startup_deadline_secs=30,
                clock=lambda: now[0],
            )
            cell.transition('write-hold', deadline_at=deadline)

            def sample(cell: ComponentWatchdog = cell) -> WatchdogSample:
                value = cell.sample()
                samples.append(value.identity)
                return value

            cache._register_watchdog(
                kind='source',
                identity=identity,
                worker_instance='deadline-sample',
                token=identity,
                supplier=sample,
            )
        send = child.send

        def delayed_send(message: ControlMessage) -> None:
            send(message)
            if during_send:
                now[0] = max(now[0], 5.005)

        monkeypatch.setattr(child, 'send', delayed_send)
        pending = runtime._publish_monitor_health(4.99)
        frame = parent.receive(timeout=1)
        assert isinstance(frame, HealthUpdate)
        assert samples == ['events', 'later']
        assert frame.worker.heartbeat_at == 4.99
        assert pending == 5
        assert runtime._monitor_wait_secs(pending) == pytest.approx(0 if during_send else 0.01)
        now[0] = 5.005
        pending = runtime._publish_monitor_health(now[0])
        first_confirmation = parent.receive(timeout=1)
        assert isinstance(first_confirmation, HealthUpdate)
        assert first_confirmation.worker.heartbeat_at >= 5
        assert pending == 7
        assert runtime._monitor_wait_secs(pending) == pytest.approx(1.995)
        now[0] = 7
        pending = runtime._publish_monitor_health(now[0])
        confirmed = parent.receive(timeout=1)
        assert isinstance(confirmed, HealthUpdate)
        assert confirmed.worker.heartbeat_at == 7
        assert pending is None
        assert runtime._monitor_wait_secs(pending) == 6
        runtime.request_stop('test-stop')
        runtime._shutdown_deadline_at = 7.25
        assert runtime._monitor_wait_secs(8) == 0.25


def test_worker_checkpoints_each_open_file_once_per_interval(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = _runtime_site(_group('default-a', worker='default'))
    site = replace(
        site,
        groups=MappingProxyType(
            {
                'default-a': _group('default-a', worker='default'),
                'default-b': _group('default-b', worker='default'),
            }
        ),
    )
    config = _runtime_config(tmp_path, site)
    config = replace(config, daemon=replace(config.daemon, checkpoint_interval_secs=0.05))
    spec = build_worker_specs(site)[0]
    assignment = _local_assignment(spec, 'default-a', 'initialize')
    assignment = replace(
        assignment,
        group_assignments=tuple(
            replace(assignment.group_assignments[0], group=name) for name in spec.groups
        ),
        source_group_assignments=tuple(
            replace(assignment.source_group_assignments[0], group=name) for name in spec.groups
        ),
    )
    attempts: dict[Path, list[float]] = {}
    owners: set[tuple[int, int]] = set()
    lock = threading.Lock()
    original = Checkpointer.run_once

    def checkpoint(checkpointer: Checkpointer) -> CheckpointAttempt:
        with lock:
            attempts.setdefault(checkpointer._path, []).append(time.monotonic())
            owners.add((id(checkpointer), id(checkpointer._connection)))
        return original(checkpointer)

    monkeypatch.setattr(Checkpointer, 'run_once', checkpoint)
    with _running_concrete_runtime(monkeypatch, config, spec, assignment, _runtime_provider()) as (
        runtime,
        control,
    ):
        _receive_typed_health(
            control,
            lambda update: (
                len(update.groups) == 2
                and len(update.sources) == 1
                and all(
                    item.checkpoint_state == 'complete'
                    for item in (*update.groups, *update.sources)
                )
            ),
        )
        drain_stop, drain_thread = _start_control_drain(control)
        try:
            deadline = time.monotonic() + 2
            completed = False
            while time.monotonic() < deadline:
                with lock:
                    completed = len(attempts) == 1 and all(
                        len(times) >= 2 for times in attempts.values()
                    )
                if completed:
                    break
                time.sleep(0.001)
            assert completed
            with lock:
                observed = {path: list(times) for path, times in attempts.items()}
            assert {path.name for path in observed} == {'events.db'}
            assert len(owners) == 1
            assert (
                len(
                    [
                        thread
                        for thread in threading.enumerate()
                        if thread.name.startswith('sg-event-daemon:checkpoint:')
                    ]
                )
                == 1
            )
            assert runtime.fatal_reason is None
        finally:
            _stop_control_drain(drain_stop, drain_thread)
    assert not any(
        thread.name.startswith('sg-event-daemon:checkpoint:') for thread in threading.enumerate()
    )


def test_worker_monitor_health_saturation_stops_runtime_with_bounded_reason(
    tmp_path: Path,
) -> None:
    site = _site(())
    config = _runtime_config(tmp_path, site)
    spec = WorkerSpec(
        worker_id='["named","empty"]',
        site=site.name,
        kind=WorkerKind._NAMED,
        name='empty',
        sources=(),
        groups=(),
    )
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='monitor-saturation-1',
        source_generations=(),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )
    peer_socket, sender_socket = socket.socketpair()
    peer = WorkerControl(peer_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    sender = WorkerControl(sender_socket, max_frame_bytes=131_072, send_deadline_secs=0.05)
    runtime = build_worker_runtime(config, spec, assignment, sender)
    _saturate_worker_control_sender(sender)
    try:
        runtime.start()
        assert runtime.wait() == ()
        assert runtime.fatal_reason == 'worker-control-send-timeout'
    finally:
        runtime.request_stop('test-cleanup')
        runtime.wait()
        peer.close()
        sender.close()


def test_worker_monitor_samples_blocked_source_provider_creation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = replace(
        _runtime_site(_group('local', worker='default')),
        groups=MappingProxyType({}),
    )
    config = _runtime_config(tmp_path, site)
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=WorkerKind._SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    assignment = WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance='source-blocked-provider-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )
    entered = threading.Event()
    release = threading.Event()

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _RuntimeProviderHandle:
        assert site == 'https://example.test'
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        entered.set()
        assert release.wait(2.0)
        return _RuntimeProviderHandle(lambda: 41)

    provider = Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        _sync_factory=factory,
        _async_factory=None,
    )
    monkeypatch.setattr(worker_module, 'load_provider', _provider_loader(provider))
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    runtime = build_worker_runtime(config, spec, assignment, child)
    runtime.start()
    try:
        _receive_type(parent, SourceEndpointReady)
        assert entered.wait(1.0)
        update = _receive_typed_health(
            parent,
            lambda item: any(
                watchdog.kind == 'source'
                and watchdog.source == 'events'
                and watchdog.phase == 'provider-query'
                for watchdog in item.watchdogs
            ),
        )
        watchdog = next(item for item in update.watchdogs if item.kind == 'source')
        source = next(item for item in update.sources if item.source == 'events')
        assert watchdog.phase == source.phase == 'provider-query'
        assert watchdog.deadline_at == source.deadline_at
        assert watchdog.heartbeat_at == source.heartbeat_at
        assert watchdog.deadline_at is not None
        assert watchdog.heartbeat_at < watchdog.deadline_at
        assert source.status == 'uninitialized'
        assert runtime.fatal_reason is None
        assert update.worker.watchdog_failure is None
    finally:
        release.set()
        _release_failed_source_runtime(
            runtime,
            (parent, child),
            (parent_socket, child_socket),
            None,
        )


def test_worker_operator_result_saturation_stops_runtime_with_bounded_reason() -> None:
    spec = build_worker_specs(_site(()))[0]
    components = {name: _Component(name, []) for name in spec.sources}
    peer_socket, sender_socket = socket.socketpair()
    peer = WorkerControl(peer_socket, max_frame_bytes=131_072, send_deadline_secs=1.0)
    sender = WorkerControl(sender_socket, max_frame_bytes=131_072, send_deadline_secs=0.05)
    runtime = WorkerRuntime(
        spec=spec,
        spawn_instance='operator-saturation-1',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=lambda _health: None,
        health_interval_secs=0.005,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=1.0,
        max_concurrent_catchup_readers=1,
        control=sender,
    )
    command = OperatorCommand(
        request_id='operator-saturation-1',
        command='adopt-source-stream',
        arguments={
            'site': 'main',
            'source': 'events',
            'reason': 'exercise saturated result publication',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=123),
    )
    _saturate_worker_control_sender(sender)
    try:
        runtime.start()
        assert components['events']._started.wait(1.0)
        peer.send(command)
        assert runtime.wait() == ()
        assert runtime.fatal_reason == 'worker-control-send-timeout'
    finally:
        runtime.request_stop('test-cleanup')
        runtime.wait()
        peer.close()
        sender.close()
