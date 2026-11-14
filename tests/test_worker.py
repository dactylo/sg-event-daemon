from __future__ import annotations

import errno
import json
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
import sg_event_daemon._health as health_module
import sg_event_daemon._platform as platform_module
import sg_event_daemon._worker as worker_module
import sg_event_daemon._write_lock as write_lock_module
from sg_event_daemon._checkpoint import CheckpointAttempt, Checkpointer
from sg_event_daemon._checkpoint_host import CheckpointHost, CheckpointView
from sg_event_daemon._config import (
    Config,
    ConfigError,
    CredentialConfig,
    GroupConfig,
    SecretValue,
    SiteConfig,
    SourceConfig,
    WorkerConfig,
    _DaemonConfig,
    load_config,
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
    BeginCallbackStep,
    CatchupEpoch,
    CompleteLateStep,
    ConditionalParkDirective,
    ConditionalParkResult,
    LiveJoinIdentity,
    ReserveLateStep,
    StartEventStep,
)
from sg_event_daemon._health import GroupHealth, WatchdogHealth, WorkerHealth
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
    WorkerControlOutbox,
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
    _IdRunPage,
    _SuccessfulQueryClock,
)
from sg_event_daemon._source_schema import BUMP_INDEX_REVISION, BUMP_RANGE_REVISION
from sg_event_daemon._source_state import SourceSnapshot, SourceState
from sg_event_daemon._sqlite import (
    DatabaseError,
    DatabaseHandle,
    Schema,
    open_database,
    wal_file_bytes,
)
from sg_event_daemon._strict import StrictBuffer
from sg_event_daemon._supervisor import Supervisor
from sg_event_daemon._watchdog import (
    ComponentWatchdog,
    WatchdogKind,
    WatchdogSample,
    internal_stall_deadline_secs,
)
from sg_event_daemon._worker import (
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
    _WorkerKind,
    _WorkerRuntime,
    build_worker_runtime,
    build_worker_specs,
)
from sg_event_daemon._worker import (
    _LaneRefresh as LaneRefresh,
)
from sg_event_daemon._write_lock import (
    OPERATOR_WRITE_BUDGET_SECS,
    SourceDatabaseBusyError,
    WriteLockStoppedError,
    WriteLockTimeoutError,
)
from tests._ipc_support import receive_control, send_control
from tests._ipc_support import saturate_worker_control_sender as _saturate_worker_control_sender
from tests._runtime_diagnostics import record_runtime_exception
from tests._signals import publish_text, read_complete_lines
from tests._stall_policy import lift_config, stall_deadline
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
from tests._strict_state import strict_watermark
from tests._worker_processes import (
    ChildPatches,
    DeterministicHandle,
    SupervisorProbe,
    Topology,
    _BusySourceProbe,
    _install_busy_source_calls,
    _install_busy_source_queries,
    _install_busy_source_request,
    _install_connection_socket_faults,
    _install_lane_call_wait_probe,
    _install_retry_service_probe,
    running_supervisor,
    write_daemon_config,
)
from tests.group._support import audit_records, commit_step
from tests.health_helpers import status_from_health
from tests.source._support import EventLogSG
from tests.source.test_state import open_source_range

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Iterable, Mapping, Sequence
    from concurrent.futures import Future
    from multiprocessing.connection import Connection
    from typing import NoReturn

    from sg_event_daemon._fanout import GroupChannelInterface
    from sg_event_daemon._gaps import IdRun
    from sg_event_daemon._group_types import GroupSnapshot
    from sg_event_daemon._ipc import ControlMessage
    from sg_event_daemon._providers import ProviderInterface
    from sg_event_daemon._watchdog import WatchdogPhase
    from sg_event_daemon._worker import (
        _LaneConfiguration,
        _LaneLifecycle,
        _SourceJoinSurface,
    )
    from sg_event_daemon._write_lock import SourceWriteLock, WriteContext, _TransactionOptions
    from tests._stall_policy import StallDetectors

_ComponentT = TypeVar('_ComponentT')
_Message = TypeVar('_Message')


class RuntimeProviderHandle:
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


class _AdoptionBlockedHandle:
    def __init__(self, probe: _AdoptionServiceProbe) -> None:
        self.config = _ProviderConfig()
        self._probe = probe

    def close(self) -> None:
        assert self._probe.release.is_set()
        self._probe.operations.append('close')

    def find(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
        probe = self._probe
        probe.operations.append('find')
        probe.entered.set()
        assert probe.release.wait()
        probe.operations.append('return')
        if not probe.service_failure:
            raise probe.provider_error
        return [{'id': 41}]

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        pass


class _CleanupProbe:
    def __init__(
        self,
        name: str,
        operations: list[str],
        error: RuntimeError | None,
    ) -> None:
        self.error = error
        self._name = name
        self._operations = operations
        self.traceback: object | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        pass

    def close(self) -> None:
        self._operations.append(self._name)
        if self.error is not None:
            try:
                raise self.error
            except RuntimeError as error:
                self.traceback = error.__traceback__
                raise


class _CoordinationLane:
    def __init__(self, state: GroupState) -> None:
        self._state = state

    def complete_obligation(self, obligation_id: str) -> GroupSnapshot:
        return self._state.complete_obligation_scan(obligation_id)


class _CoordinationReader:
    def __init__(self) -> None:
        self.ranges: list[tuple[int, int]] = []

    def fetch_id_runs(self, runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
        self.ranges.extend(runs)
        yield _IdRunPage((), runs[-1][1])

    def retention_floor(self) -> int:
        return 1


class _DurableCoordinationSurface:
    def __init__(self, state: SourceState) -> None:
        self._state = state
        self._join_coordinator = JoinCoordinator(
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
        self._join_coordinator.abort(abort)

    def confirm(self, confirmation: JoinConfirm) -> Attached | JoinRetry:
        return self._join_coordinator.confirm(confirmation)

    def event_channel_lost(self, _group: str, *, reason: str) -> None:
        assert reason == 'event-channel-lost'

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        return self._join_coordinator.finalize(finalization)

    def request(self, request: JoinRequest, channel: GroupChannel) -> JoinTarget | JoinRetry:
        result = self._join_coordinator.request(request, channel)
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

    def call(
        self,
        invoke: Callable[[object], object],
        *,
        wait_phase: Callable[[], AbstractContextManager[None]],
        stop: threading.Event,
    ) -> object:
        del wait_phase, stop
        return invoke(self._surface)


class _IdleCheckpointHost:
    def register(
        self, _path: object, _identity: str, _schema: Schema, *, log_extra: Mapping[str, str]
    ) -> CheckpointView:
        assert all(isinstance(value, str) for value in log_extra.values())
        return CheckpointView()


class _LifecycleChannel:
    def __init__(self) -> None:
        self._closed = False

    def close(self) -> None:
        self._closed = True


class _LifecycleCoordination:
    def __init__(self) -> None:
        self.calls: list[bool] = []
        self.serviced = threading.Event()

    def service(self, *, scan_obligations: bool = True, force: bool = False) -> object:
        del force
        self.calls.append(scan_obligations)
        self.serviced.set()
        return None


class _LifecycleReader:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def close(self) -> None:
        return None


class _LifecycleState:
    def __init__(self, status: str) -> None:
        self.status = status

    def close(self) -> None:
        return None

    def operator_writes(self) -> AbstractContextManager[None]:
        return nullcontext()

    def snapshot(self) -> object:
        return SimpleNamespace(status=self.status)


class _NontransportConstructionFailure:
    def __init__(
        self,
        stage: Literal['provider', 'state', 'plugin'],
        stop: threading.Event,
    ) -> None:
        self.attempts = 0
        self._client = _LifecycleReader()
        self._reader = _LifecycleReader()
        self._stage = stage
        self._state = _LifecycleReader()
        self._stop = stop

    def _build_client(self, _context: object) -> tuple[object, str]:
        return self._client, 'source-generation-1'

    def _fail(self) -> NoReturn:
        self.attempts += 1
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

    def install(
        self,
        component: object,
        context: object,
        database_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def publish_startup_health(_context: object, _state: object) -> None:
            pass

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
                '_publish_startup_group_health': publish_startup_health,
                '_stop': self._stop,
            }
        )


class _PressureCleanupServer:
    def __init__(self, attempts: list[str]) -> None:
        self._attempts = attempts

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        self._attempts.append('server')

    def dispatch_join_requests(self, _source: object) -> None:
        pass

    def install_grant(self, _grant: object) -> None:
        pass

    def start(self) -> None:
        pass


class _PressureJoinSource:
    def __init__(
        self,
        *,
        boundary: int,
        live_event_id: int | None,
        source_generation: str = 'source-1',
        stream_incarnation: str = 'incarnation-1',
    ) -> None:
        self._boundary = boundary
        self._live_event_id = live_event_id
        self._source_generation = source_generation
        self._stream_incarnation = stream_incarnation

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
            source_generation=self._source_generation,
            group_generation=request.attempt.group_generation,
            boundary=self._boundary,
            attempt_token=request.attempt.attempt_token,
        )
        if self._live_event_id is not None:
            event = Event(runtime_event(self._live_event_id))
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
            stream_incarnation=self._stream_incarnation,
        )


class _ProbeLane:
    pass


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
        self.messages: list[object] = []
        self._operations = operations

    def enqueue(self, message: object) -> None:
        self.messages.append(message)
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
        deliver((Event(runtime_event(42)),))
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
        self.decided_at_wall_clock: float | None = None
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
        self.decided_at_wall_clock = decided_at_wall_clock
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


class _RecoveryGateCoordination:
    def service(self, *, scan_obligations: bool = True, force: bool = False) -> object:
        del force
        del scan_obligations
        return None

    def synchronize(self) -> object:
        return object()


class _RestartProviderState:
    def __init__(self) -> None:
        self.events: dict[int, dict[str, object]] = {}
        self.lock = threading.Lock()
        self.observed: dict[int, set[str]] = {}

    def publish(self, event_id: int) -> None:
        with self.lock:
            self.events[event_id] = runtime_event(event_id)

    def record_query(self, rows: list[dict[str, object]]) -> None:
        with self.lock:
            for row in rows:
                event_id = row['id']
                if type(event_id) is int:
                    self.observed.setdefault(event_id, set()).add(threading.current_thread().name)

    def rows(self) -> tuple[dict[str, object], ...]:
        with self.lock:
            return tuple(self.events[key] for key in sorted(self.events))

    def wait_for_query(self, event_id: int, thread_fragment: str) -> bool:
        while True:
            with self.lock:
                names = self.observed.get(event_id, set())
                if any(thread_fragment in name for name in names):
                    return True
            time.sleep(0.001)


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


class _RetentionCoordinationReader:
    def __init__(
        self,
        floors: tuple[int | None, ...],
        events: tuple[Event, ...] = (),
    ) -> None:
        self._events = events
        self._floors = iter(floors)
        self.ranges: list[tuple[int, int]] = []

    def fetch_id_runs(self, runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
        self.ranges.extend(runs)
        events = tuple(
            event
            for event in self._events
            if any(low <= cast('int', event['id']) <= high for low, high in runs)
        )
        yield _IdRunPage(events, runs[-1][1])

    def retention_floor(self) -> int | None:
        return next(self._floors)


class _RetentionCoordinationState:
    def __init__(self, *, cursor: int = 60, delivery_floor: int = 50) -> None:
        self.cursor = cursor
        self._delivery_floor = delivery_floor
        self._revision = 0
        self.status = 'running'
        self.truncated_floor: int | None = None
        self._pending = (
            PendingRange(
                obligation_id='range-1',
                low_id=40,
                high_id=60,
                closed=True,
                close_generation=1,
                eligibility_point=0.0,
                visibility_deadline=0.0,
            ),
        )

    def missing_late_runs(self, _obligation_id: str) -> tuple[IdRun, ...]:
        return ((max(40, self._delivery_floor + 1), 60),)

    def pending_ranges(self) -> tuple[PendingRange, ...]:
        return self._pending

    def refresh_ranges(
        self, _last_revision: int | None = None
    ) -> tuple[SourceProgress, tuple[PendingRange, ...]]:
        return _synchronization(60), self._pending

    def snapshot(self) -> object:
        return SimpleNamespace(
            cursor=self.cursor,
            delivery_floor=self._delivery_floor,
            ordering='eager',
            group_generation=1,
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            revision=self._revision,
            status=self.status,
        )

    def mark_history_truncated(
        self,
        retention_floor: int,
        *,
        expected_revision: int,
    ) -> object:
        assert expected_revision == self._revision
        self._revision += 1
        self.status = 'history-truncated'
        self.truncated_floor = retention_floor
        return self.snapshot()


class _RetentionCoordinationLane:
    def __init__(self, state: _RetentionCoordinationState) -> None:
        self.delivered: list[int] = []
        self.resolved = False
        self._state = state

    def complete_obligation(self, _obligation_id: str, *, excused: Sequence[IdRun] = ()) -> object:
        assert not excused
        self.resolved = True
        return self._state.snapshot()

    def deliver_obligation_rows(
        self,
        _revision: object,
        events: Iterable[Event],
        *,
        source_successful_clock: float | None = None,
    ) -> tuple[object, bool]:
        del source_successful_clock
        self.delivered.extend(cast('int', event['id']) for event in events)
        return self._state.snapshot(), True


class _SynchronizationProbeReader:
    def __init__(self, ordering: Literal['eager', 'strict']) -> None:
        self.observed: list[SourceProgress] = []
        self._ordering = ordering
        self._queried = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def catch_up(self, **options: object) -> object:
        synchronize = cast('Callable[[], SourceProgress]', options['source_progress'])
        first = synchronize()
        self.observed.append(first)
        self.observed.append(synchronize())
        self._queried = True
        return SimpleNamespace(status='caught-up')

    def strict_step(self, **options: object) -> object:
        synchronize = cast('Callable[[], SourceProgress]', options['source_progress'])
        synchronization = synchronize()
        self.observed.append(synchronization)
        self._queried = True
        return SimpleNamespace(
            status='join-ready' if len(self.observed) == 2 else 'catching-up',
            join_ready=len(self.observed) == 2,
        )


class _SyncingClient:
    def __init__(self, synchronizations: tuple[SourceProgress, ...]) -> None:
        self.calls: list[int] = []
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
        self.calls.append(group_generation)
        return next(self._synchronizations)


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

    def _set_monitor_wake(self) -> None:
        if threading.current_thread().name == f'sg-event-daemon:{self._terminal_identity}':
            with self._clock_lock:
                self._clock_value = 2.0
            self._monitor_wake.set()
            self._terminal_wake_started.set()
            self._release_terminal_wake.wait()
            return
        self._monitor_wake.set()

    def clock(self) -> float:
        with self._clock_lock:
            return self._clock_value

    def health_count(self) -> int:
        with self._health_published:
            return self._published_health_count

    def install(self, runtime: _WorkerRuntime) -> None:
        vars(runtime)['_monitor_wake'] = SimpleNamespace(
            clear=self._monitor_wake.clear,
            set=self._set_monitor_wake,
            wait=self._monitor_wake.wait,
        )

    def observe_target_thread(self, runtime: _WorkerRuntime) -> None:
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

    def publish_health(self, update: WorkerHealth) -> None:
        del update
        with self._health_published:
            self._published_health_count += 1
            self._health_published.notify_all()

    def release(self) -> None:
        self._release_terminal_wake.set()

    def wait_for_bounded_join(self) -> bool:
        return self._bounded_join_started.wait()

    def wait_for_health_after(self, previous_count: int) -> bool:
        with self._health_published:
            return self._health_published.wait_for(
                lambda: self._published_health_count > previous_count,
            )

    def wait_for_terminal_wake(self) -> bool:
        return self._terminal_wake_started.wait()

    def wake_monitor(self) -> None:
        self._monitor_wake.set()


class _WatchdogBlockingClose:
    def __init__(
        self,
        entered: threading.Event,
        release: threading.Event,
        operations: list[str],
    ) -> None:
        self._entered = entered
        self._operations = operations
        self._release = release

    def close(self) -> None:
        self._operations.append('close')
        self._entered.set()
        assert self._release.wait()


def _arm_busy_source(probe: SupervisorProbe, root: Path) -> None:
    probe.wait_for(
        lambda: any(
            group.join_phase == 'attached'
            for health in probe.latest_health.values()
            for group in health.groups
        ),
    )
    (root / 'busy-arm').touch()
    probe.wait_for(lambda: (root / 'busy-entered').exists() or bool(probe.terminations))
    assert not probe.terminations, probe.failure_details()


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
    assert isinstance(health.conditions, tuple)


def _assert_detached_strict_join(
    component: _LaneComponent,
    resources: _LaneResources,
    observations: tuple[list[int], list[str], list[float]],
    case: str,
) -> None:
    callbacks, trace, wall = observations
    state = resources.state
    lane = resources.lane
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
                    resources.client,
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
            resources.client,
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
    runtime: _WorkerRuntime, error_type: type[BaseException]
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


def _assert_monitor_stopped(runtime: _WorkerRuntime) -> None:
    monitor = vars(runtime)['_monitor_thread']
    assert isinstance(monitor, threading.Thread)
    assert not monitor.is_alive()


def _assert_remote_initialization_pending(
    runtime: _WorkerRuntime,
    pending_results: Collection[object],
    pending_remote: threading.Event,
) -> None:
    while True:
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


def _assert_settlement_probe(
    records: list[dict[str, object]],
    probe: SupervisorProbe,
    injected: list[dict[str, object]],
    settlement: tuple[str | None, bool],
    repeats: int,
) -> None:
    assert len(injected) == repeats, probe.failure_details()
    abort_failure, outcome_failure = settlement
    if outcome_failure:
        token = injected[0]['token']
        assert [
            item['capacity']
            for item in records
            if item['kind'] == 'outcome-attempt' and item['token'] == token
        ] == [True]
        assert (
            len([item for item in records if item['kind'] == 'abort' and item['token'] == token])
            == 1
        )
        assert not any(item['kind'] == 'outcome' and item['token'] == token for item in records)
        assert any(
            'OperationalError: outcome write failed' in spawn.log_path.read_text()
            for spawn in probe.spawns
        )
    if abort_failure is not None:
        assert len([item for item in records if item['kind'] == 'source-abort']) == 1


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


def _busy_source_budget(config_path: Path) -> float:
    config = load_config(config_path)
    site = config.sites['test']
    credential = site.credentials[site.sources['admin'].credentials]
    return internal_stall_deadline_secs(config.daemon.health_interval_secs, credential)


def _busy_source_config(tmp_path: Path, case: str) -> Path:
    packing = 'named' if case in {'remote-cap', 'remote-bootstrap'} else 'default'
    return write_daemon_config(tmp_path, Topology(packing, ('local',)))


def _busy_source_operator() -> OperatorCommand:
    return OperatorCommand(
        command='preflight-source-adoption',
        request_id='busy-source',
        arguments={'site': 'test', 'source': 'admin', 'reason': 'exercise queued source command'},
        identity=AuditIdentity.current(),
    )


def _configure_shutdown_probe_clock(monkeypatch: pytest.MonkeyPatch, now: list[float]) -> None:
    original_start = _WorkerRuntime.start
    original_construct = _WorkerRuntime._construct_components

    def construct(runtime: _WorkerRuntime) -> None:
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

    def start(runtime: _WorkerRuntime) -> None:
        monkeypatch.setattr(runtime, '_clock', lambda: now[0])
        assert runtime._operator_watchdog is not None
        monkeypatch.setattr(runtime._operator_watchdog, '_clock', lambda: now[0])
        runtime._operator_watchdog.transition('idle', deadline_at=None)
        original_start(runtime)

    monkeypatch.setattr(_WorkerRuntime, '_construct_components', construct)
    monkeypatch.setattr(_WorkerRuntime, 'start', start)


def _expire_lane_admission(monkeypatch: pytest.MonkeyPatch, component: _LaneComponent) -> None:
    local = threading.local()
    submit = component._mailbox.submit

    def monotonic() -> float:
        return getattr(local, 'now', time.monotonic())

    def submitted(
        invoke: Callable[[_LaneResources], object],
    ) -> _OwnerCall[_LaneResources]:
        call = submit(invoke)
        local.now = monotonic() + worker_module._LANE_OPERATOR_ADMISSION_SECS
        return call

    # Advance only this command's caller after its mailbox admission. Owner,
    # sibling, monitor, and test threads continue to use the real clock.
    monkeypatch.setattr(
        worker_module, 'time', SimpleNamespace(monotonic=monotonic, time=time.time)
    )
    monkeypatch.setattr(component._mailbox, 'submit', submitted)


def _expire_shutdown(runtime: _WorkerRuntime, stuck: Collection[str]) -> None:
    assert runtime._stop_event.wait()
    for identity, thread in runtime._owned_threads():
        if identity not in stuck:
            thread.join()
    deadline = runtime._shutdown_deadline_at
    assert deadline is not None
    runtime._clock = lambda: deadline
    runtime._monitor_wake.set()


def _assert_bounded_checkpoint_shutdown(
    runtime: _WorkerRuntime,
    release: threading.Event,
    forced: list[tuple[int, str]],
    closed_on: list[str],
) -> None:
    _expire_shutdown(runtime, {'checkpoint:events'})
    result: list[tuple[str, ...]] = []
    waiter = threading.Thread(target=lambda: result.append(runtime.wait()))
    waiter.start()
    try:
        waiter.join()
        assert not waiter.is_alive()
        assert forced == [(1, 'sg-event-daemon:monitor')]
        assert result == [('checkpoint:events',)]
        assert closed_on == []
        assert runtime._resources_open
    finally:
        release.set()
        waiter.join()


def _finish_checkpoint_runtime(runtime: _WorkerRuntime) -> None:
    runtime._checkpoint_host.request_stop()
    threads = list(runtime._owned_threads())
    monitor = runtime._monitor_thread
    if monitor is not None:
        threads.append(('monitor', monitor))
    for _identity, thread in threads:
        thread.join()
    live = tuple(identity for identity, thread in threads if thread.is_alive())
    assert live == (), f'checkpoint cleanup left live threads: {live}'
    runtime._checkpoint_host.close()
    assert runtime._shutdown() == ()


def _gate_burst_callbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    original_plugins = worker_module.load_plugins
    exhausted = threading.Event()
    handle = logging.Logger.handle

    def observed_log(logger: logging.Logger, record: logging.LogRecord) -> None:
        handle(logger, record)
        if record.getMessage() == (
            'detached group remote from its live channel: strict-buffer-capacity'
        ):
            exhausted.set()

    monkeypatch.setattr(logging.Logger, 'handle', observed_log)

    def slow_plugins(
        site: SiteConfig, group: GroupConfig, provider: ProviderInterface
    ) -> tuple[PluginSpec, ...]:
        plugins = original_plugins(site, group, provider)
        original_callback = plugins[0].callbacks[0].callback

        def slow_callback(context: EventContext) -> None:
            exhausted.wait()
            original_callback(context)

        return (
            replace(
                plugins[0], callbacks=(replace(plugins[0].callbacks[0], callback=slow_callback),)
            ),
        )

    monkeypatch.setattr(worker_module, 'load_plugins', slow_plugins)


def _gate_checkpoint_startup_operator(
    runtime: _WorkerRuntime,
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
        assert operator_release.wait()

    monkeypatch.setattr(threading.Thread, 'join', join)
    monkeypatch.setattr(runtime, '_operator_main', operator_main)
    return operator_returned, operator_release


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
            provider_state.publish(44)
            trace.append('import-earlier')
        elif earlier_pending:
            with state._writes.transaction(connection):
                close_test_range(connection, 'restored-earlier-fence')
            earlier_pending = False
        return original_refresh(last_revision)

    monkeypatch.setattr(state, 'refresh_ranges', refresh)


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
        source = vars(context)['group'].source
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


def _lane_detach_config(
    tmp_path: Path,
    packing: Literal['default', 'named'],
    fault: str,
) -> Path:
    ordering = 'eager' if fault in {'overflow', 'remote-queue', 'peer-disconnect'} else 'strict'
    config = write_daemon_config(tmp_path, Topology(packing, ('local',)))
    content = config.read_text().replace(
        '[sites.test.groups.local]',
        f'[sites.test.groups.local]\nordering = "{ordering}"'
        + ('\nstrict_holdback_secs = 0.02' if ordering == 'strict' else ''),
    )
    if fault in {'overflow', 'remote-queue'}:
        content = content.replace(
            'fetch_interval_secs = 0.05', 'fetch_interval_secs = 0.05\nqueue_max_events = 1'
        )
    config.write_text(content)
    return config


def _lane_probe_records(root: Path) -> list[dict[str, object]]:
    path = root / 'lane-probe.jsonl'
    return [json.loads(line) for line in read_complete_lines(path)]


def _overflow_recovery_promoted(records: list[dict[str, object]], repeats: int) -> bool:
    injections = [index for index, item in enumerate(records) if item['kind'] == 'injected']
    catchups = [item for item in records if item['kind'] == 'catchup']
    return (
        len(injections) == repeats
        and bool(catchups)
        and any(
            index > injections[-1]
            and item['kind'] == 'promoted'
            and item['token'] == catchups[-1]['token']
            for index, item in enumerate(records)
        )
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


@pytest.fixture
def _production_admission_deadlines(stall_detectors: StallDetectors) -> None:
    stall_detectors.enable('coordination', 'control')


@pytest.fixture
def _production_coordination_deadlines(stall_detectors: StallDetectors) -> None:
    stall_detectors.enable('coordination')


@pytest.fixture
def _production_phase_deadlines(stall_detectors: StallDetectors) -> None:
    stall_detectors.enable('phase')


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
        cleanups.append(source_thread.join)
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
        cleanups.append(future.result)
    for cleanup in cleanups:
        _attempt_cleanup(cleanup, errors)
    _attempt_cleanup(lambda: executor.shutdown(wait=future is not None and future.done()), errors)
    _raise_cleanup_errors(initiating, errors)


def _run_blocked_retirement(
    probe: SimpleNamespace,
    operation: Callable[[], object],
    *,
    expect_reconnect: bool,
) -> tuple[WatchdogHealth, WatchdogHealth | None, object, float]:
    internal_budget = cast(
        'float',
        vars(probe.component)['_internal_stall_deadline_secs'],
    )
    startup: WatchdogHealth | None = None
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(operation)
        try:
            assert probe.close_entered.wait()
            internal = cast('_ComponentHealthCache', probe.cache).snapshot().watchdogs[0]
            probe.close_release.set()
            if expect_reconnect:
                assert probe.reconnect_entered.wait()
                startup = cast('_ComponentHealthCache', probe.cache).snapshot().watchdogs[0]
                probe.reconnect_release.set()
            result = future.result()
        finally:
            probe.close_release.set()
            probe.reconnect_release.set()
    return internal, startup, result, internal_budget


def _run_plugin_exit_process(
    tmp_path: Path,
    stage: str,
    exit_call: str,
    class_name: str,
    *,
    async_callback: bool,
) -> None:
    config = write_daemon_config(tmp_path, Topology('default', ('bad', 'good')))
    original_plugin = Path(__file__).parent / 'fixtures' / 'recording_plugin.py'
    bad_plugin = tmp_path / 'exiting_plugin.py'
    prefix = 'async ' if async_callback else ''
    bad_plugin.write_text(
        'from pathlib import Path\n\n'
        f'{prefix}def setup(ctx):\n'
        f'    {exit_call if stage == "setup" else "pass"}\n\n'
        f'{prefix}def callback(ctx):\n'
        f'    Path({str(tmp_path / "bad-callback-ran")!r}).touch()\n'
        f'    {exit_call if stage == "callback" else "pass"}\n\n'
        'def register(reg):\n    reg.set_setup(setup)\n'
        "    reg.add_callback(id='only', callback=callback)\n",
        encoding='utf-8',
    )
    text = config.read_text(encoding='utf-8').replace(
        f'path = "{original_plugin}"', f'path = "{bad_plugin}"', 1
    )
    if async_callback:
        text = text.replace(
            '[sites.test.groups.bad]\n', '[sites.test.groups.bad]\nexecution = "async"\n', 1
        )
        text = text.replace(
            'sg_api_provider = "shotgun-api3"', 'sg_api_provider = "sg-python-api"', 1
        )
    config.write_text(text, encoding='utf-8')

    def reveal_events(last_id: int) -> None:
        pending = tmp_path / 'visible-events.pending'
        pending.write_text(str(last_id))
        pending.replace(tmp_path / 'visible-events')

    reveal_events(1)
    with running_supervisor(
        config, ChildPatches(gate_events=True, async_provider=async_callback)
    ) as probe:
        probe.wait_for(
            lambda: (
                any(
                    group.group == 'good' and group.join_phase == 'attached'
                    for update in probe.latest_health.values()
                    for group in update.groups
                )
                and any(
                    group.group == 'bad'
                    and (
                        group.join_phase == 'attached'
                        if stage == 'callback'
                        else group.failure_kind == 'setup'
                    )
                    for update in probe.latest_health.values()
                    for group in update.groups
                )
            ),
        )
        (tmp_path / 'events-ready').touch()
        reveal_events(2)

        def bad_failed() -> bool:
            return any(
                group.group == 'bad' and group.failure_kind == stage
                for update in probe.latest_health.values()
                for group in update.groups
            )

        probe.wait_for(lambda: bad_failed() or bool(probe.terminations))
        assert not probe.terminations, probe.failure_details()
        bad = next(
            group
            for update in probe.latest_health.values()
            for group in update.groups
            if group.group == 'bad'
        )
        assert bad.failure_kind == stage, probe.failure_details()
        assert bad.failure_text is not None
        assert class_name in bad.failure_text
        if stage == 'setup':
            assert 'callback-failed' in bad.conditions
        else:
            assert bad.status == 'callback-failed'
        before = len(probe.records('good'))
        reveal_events(3)
        probe.wait_for(lambda: len(probe.records('good')) > before or bool(probe.terminations))
        assert len(probe.records('good')) > before, probe.failure_details()
        assert not probe.terminations, probe.failure_details()
        assert len(probe.spawns) == 1, probe.failure_details()
        if stage == 'setup':
            assert not (tmp_path / 'bad-callback-ran').exists()


def _run_worker_runtime_cli(
    monkeypatch: pytest.MonkeyPatch,
    runtime: _WorkerRuntime,
) -> int:

    class _ControlProbe:
        def close(self) -> None:
            return None

        def send(self, _message: object, *, deadline_secs: float) -> None:
            assert deadline_secs == 0.5

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
    ) -> WorkerControl:
        assert max_frame_bytes == 131_072
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
    ) -> _WorkerRuntime:
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


def _runtime_lane(runtime: _WorkerRuntime, group: str) -> GroupLane | None:
    components = runtime._components
    component = components[f'lane:{group}']
    return cast('_LaneComponent', component)._lane


def _assert_retained_history_halt(
    runtime: _WorkerRuntime,
    control: WorkerControl,
    halted_serviced: threading.Event,
) -> None:
    update: HealthUpdate | None = None
    while True:
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
    assert halted_serviced.wait()
    health = {group.group: group for group in update.groups}
    assert health['halted'].join_phase != 'attached'
    assert health['healthy'].join_phase == 'attached'
    threads = runtime._threads
    assert threads['lane:halted'].is_alive()
    assert threads['lane:healthy'].is_alive()
    halted_lane = _runtime_lane(runtime, 'halted')
    assert halted_lane is not None
    assert halted_lane.health().status == 'history-truncated'


def _runtime_threads_alive(runtime: _WorkerRuntime | None) -> bool:
    if runtime is None:
        return False
    threads = runtime._threads
    monitor = runtime._monitor_thread
    return any(thread.is_alive() for thread in threads.values()) or (
        monitor is not None and monitor.is_alive()
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


def _stop_after_empty_slice(
    stop: Literal['park', 'component', 'lane'] | None,
    state: GroupState,
    snapshot: GroupSnapshot,
    component: _LaneComponent,
    lane: GroupLane,
) -> GroupSnapshot | None:
    if stop == 'park':
        return state.park(
            reason='maintenance',
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
            expected_revision=snapshot.revision,
        )
    if stop == 'component':
        component._stop.set()
    if stop == 'lane':
        lane.request_shutdown()
    return None


def _stop_runtime(runtime: _WorkerRuntime | None) -> None:
    if runtime is None:
        return
    runtime.request_stop('test')
    assert runtime.wait() == ()
    _assert_monitor_stopped(runtime)


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
    runtime: _WorkerRuntime, control: WorkerControl, previous: HealthUpdate
) -> HealthUpdate:
    monitor = runtime._monitor_thread
    assert monitor is not None
    monitor.join()
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
        if condition in resources.state.snapshot().conditions:
            halted_serviced.set()

    monkeypatch.setattr(_LaneComponent, '_drive_halted', track_halted_service)
    return halted_serviced


def _track_retained_history_service(
    monkeypatch: pytest.MonkeyPatch,
) -> threading.Event:
    halted_serviced = threading.Event()
    service = LaneRefresh.service

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

    monkeypatch.setattr(LaneRefresh, 'service', track_halted_service)
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
        coordination_deadline_secs=stall_deadline('coordination', 30.0),
        stop=threading.Event(),
        wait_phase=nullcontext,
        wait_slice_secs=0.1,
    )


def _wait_for_detach_delivery(
    probe: SupervisorProbe,
    tmp_path: Path,
    fault: str,
    repeats: int,
    *,
    expected_exits: tuple[str, ...],
) -> tuple[int, ...]:
    expected_callbacks = tuple(range(2, 6 * repeats + 1))
    if fault == 'overflow':
        probe.wait_for(
            lambda: (
                bool(probe.terminations)
                or (
                    probe.records('local') == expected_callbacks
                    and _overflow_recovery_promoted(_lane_probe_records(tmp_path), repeats)
                )
            ),
            expected_exits=expected_exits,
        )
    else:
        probe.wait_for(
            lambda: bool(probe.terminations) or probe.records('local') == expected_callbacks,
            expected_exits=expected_exits,
        )
    return expected_callbacks


def _wait_for_group_database(runtime: _WorkerRuntime, path: Path, group: str) -> None:
    row: tuple[int] | None = None
    while True:
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
    runtime: _WorkerRuntime,
    path: Path,
    invoked: list[int],
    event_id: int,
) -> None:
    row: tuple[int, int] | None = None
    while True:
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


def _wait_for_runtime_stop(runtime: _WorkerRuntime) -> tuple[str, ...]:
    def state() -> str:
        return (
            f'fatal_reason={runtime.fatal_reason!r}, '
            f'stop_reason={runtime._stop_reason!r}, '
            f'stuck_threads={runtime._stuck_threads!r}'
        )

    assert runtime._stop_event.wait(), state()
    monitor = runtime._monitor_thread
    assert monitor is not None
    monitor.join()
    assert not monitor.is_alive(), state()
    return runtime.wait()


def _wait_process_config(tmp_path: Path, case: str) -> Path:
    remote = case.startswith(('connect', 'reconnect'))
    return write_daemon_config(
        tmp_path,
        Topology('named' if remote else 'default', ('waiting',)),
    )


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
        cache.publish_source(
            _watchdog_source_health(
                'events', worker_instance=component._assignment.spawn_instance
            ),
            sample=sample,
        )
    else:
        cache.publish_group(
            _watchdog_group_health(
                'local', source='events', worker_instance=component._assignment.spawn_instance
            ),
            sample=sample,
        )
    assert component._stop.wait()
    if isinstance(component, _LaneComponent):
        component._close_partial_attempt(worker_module._PartialLaneAttempt())
    else:
        watchdog.transition('shutdown', deadline_at=component._shutdown_deadline_at)
    assert releases[sample.kind].wait()


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
            cache.register_watchdog(
                key=('source', 'events', 'spawn-1', successor.registration_token),
                supplier=lambda: successor,
            )
            return predecessor

        cache.register_watchdog(
            key=('source', 'events', 'spawn-1', predecessor.registration_token),
            supplier=replace_registration,
        )
        first = cache.snapshot()
        cache.deregister_watchdog(
            kind='source',
            identity='events',
            worker_instance='spawn-1',
            token=predecessor.registration_token,
        )
        second = cache.snapshot()
        connection.send(
            (
                tuple(first.watchdogs),
                tuple((item.source, item.heartbeat_at) for item in second.watchdogs),
            )
        )
    finally:
        connection.close()


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


def drift_disposition_policy(path: Path, group: str, drift: str) -> tuple[str, ...]:
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


def health_failure_plugin(
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

    plugin = health_failure_plugin('callback')
    return replace(plugin, callbacks=(replace(plugin.callbacks[0], callback=callback),))


def _health_failure_plugin_loader(
    failure_kind: Literal['setup', 'callback'],
) -> Callable[[SiteConfig, GroupConfig, Provider], tuple[PluginSpec, ...]]:

    def load(
        _site: SiteConfig,
        _group: GroupConfig,
        _provider: Provider,
    ) -> tuple[PluginSpec, ...]:
        return (health_failure_plugin(failure_kind),)

    return load


def local_assignment(
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


def provider_loader(
    provider: Provider,
) -> Callable[[str, Collection[str]], Provider]:

    def load(expected: str, required: Collection[str]) -> Provider:
        del expected, required
        return provider

    return load


def receive_type(control: WorkerControl, expected: type[_Message]) -> _Message:
    while True:
        message = receive_control(control)
        if isinstance(message, expected):
            return message


def receive_typed_health(
    control: WorkerControl,
    ready: Callable[[HealthUpdate], bool],
) -> HealthUpdate:
    while True:
        update = receive_type(control, HealthUpdate)
        if ready(update):
            return update


def _assert_callback_heartbeat_slides_real_health(
    control: WorkerControl,
    update: HealthUpdate,
    context: EventContext,
) -> None:
    deadline_at = update.groups[0].deadline_at
    assert deadline_at is not None
    context.heartbeat()
    update = receive_typed_health(
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
    update = receive_typed_health(
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
    update = receive_typed_health(
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


def runtime_config(state_dir: Path, site: SiteConfig) -> Config:
    return lift_config(
        Config(
            daemon=_DaemonConfig(
                state_dir=state_dir,
                sg_api_provider='shotgun-api3',
            ),
            sites=MappingProxyType({site.name: site}),
        )
    )


def runtime_event(event_id: int) -> dict[str, object]:
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


def _assert_strict_burst_delivery(
    dependent_runtime: _WorkerRuntime,
    group_path: Path,
    provider_state: _RestartProviderState,
    invoked: list[int],
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Wait for the real remote lane to join before releasing one 200-row page.
    lane = dependent_runtime._components['lane:remote']
    assert isinstance(lane, _LaneComponent)
    while True:
        assert dependent_runtime.fatal_reason is None, dependent_runtime.fatal_reason
        if lane._lifecycle is not None and lane._lifecycle.joined:
            break
        time.sleep(0.005)
    assert lane._lifecycle is not None
    assert lane._lifecycle.joined
    initial_channel = lane._lifecycle.channel
    assert lane._lane is not None
    initial_join = lane._lane._join_state
    assert isinstance(initial_join, _FinalizedJoin)
    with provider_state.lock:
        provider_state.events.update(
            {identifier: runtime_event(identifier) for identifier in range(58, 258)}
        )
    rejoined = False
    while True:
        current_join = lane._lane._join_state
        rejoined = (
            isinstance(current_join, _FinalizedJoin)
            and current_join.identity.attempt_token != initial_join.identity.attempt_token
            and lane._lifecycle.joined
            and lane._lifecycle.channel.status.attached
        )
        if len(invoked) == 200 and rejoined:
            break
        if dependent_runtime.fatal_reason is not None:
            break
        time.sleep(0.01)
    with closing(sqlite3.connect(group_path)) as database, database:
        exhausted = database.execute(
            'SELECT strict_capacity_exhausted FROM group_state WHERE group_name = ?',
            ('remote',),
        ).fetchone()[0]
    assert exhausted == 0
    assert not initial_channel.status.attached
    assert any(
        record.name == 'sg_event_daemon.source'
        and record.getMessage()
        == 'detached group remote from its live channel: strict-buffer-capacity'
        for record in caplog.records
    )
    assert rejoined
    assert invoked == list(range(58, 258))
    assert lane._lane is not None
    assert lane._lane.health().status == 'running'
    assert not any('group halted' in message for message in caplog.messages)
    assert dependent_runtime.fatal_reason is None


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
        Event(runtime_event(event_id)) for event_id in range(state.snapshot().cursor + 1, 48)
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


def runtime_site(group: GroupConfig) -> SiteConfig:
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


def _runtime_delivery_site(
    group: GroupConfig,
    start_mode: Literal['initialize', 'resume'],
) -> SiteConfig:
    site = runtime_site(group)
    if group.ordering == 'strict' and start_mode == 'resume':
        source = replace(
            site.sources[group.source],
            fetch_interval_secs=0.01,
            late_event_window_secs=0.01,
        )
        site = replace(site, sources=MappingProxyType({group.source: source}))
    return site


def source_database_path(state_dir: Path) -> Path:
    sites = ensure_secure_state_dir(ensure_secure_state_dir(state_dir) / 'sites')
    site = ensure_secure_state_dir(sites / 'main')
    sources = ensure_secure_state_dir(site / 'sources')
    return sources / 'events.db'


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
        source_database_path(state_dir),
        writes=write_context_for(source_database_path(state_dir)),
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
            source_database_path(state_dir),
            writes=write_context_for(source_database_path(state_dir)),
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
            strict_holdback_secs=source_config.late_event_window_secs,
        ):
            pass


def start_control_drain(
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
    send_control(control, grant)
    assert receive_type(control, EventPeerReady) == EventPeerReady(
        source=grant.source,
        group=grant.group,
        dependent_spawn_instance=grant.dependent_spawn_instance,
    )
    _assert_remote_server_health(control, config)
    return start_control_drain(control)


def stop_control_drain(
    stop: threading.Event,
    thread: threading.Thread,
) -> None:
    stop.set()
    thread.join()
    assert not thread.is_alive()


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

    def send(_control: WorkerControl, message: ControlMessage) -> None:
        sent.append(message)

    control = cast('WorkerControl', object())

    def receive(_control: WorkerControl, expected: type[object]) -> object:
        assert expected is EventPeerReady
        return ready

    def fail_health(_control: WorkerControl, _config: Config) -> NoReturn:
        raise RuntimeError('injected health failure')

    def start_drain(value: WorkerControl) -> tuple[threading.Event, threading.Thread]:
        drain_starts.append(value)
        return threading.Event(), threading.Thread()

    module = sys.modules[__name__]
    monkeypatch.setattr(module, 'send_control', send)
    monkeypatch.setattr(module, 'receive_type', receive)
    monkeypatch.setattr(module, '_assert_remote_server_health', fail_health)
    monkeypatch.setattr(module, 'start_control_drain', start_drain)

    with pytest.raises(RuntimeError, match='injected health failure'):
        _authorize_remote_source(control, grant, cast('Config', object()))

    assert sent == [grant]
    assert drain_starts == []


def test_eager_catchup_uses_unresolved_obligation_bounds_for_retention() -> None:

    class _Reader:
        def catch_up(self, **options: object) -> object:
            if options.get('unresolved_bounds') == ((11, 20),):
                raise HistoryTruncatedError(15, 11)
            return SimpleNamespace(status='caught-up')

    class _State:
        def __init__(self) -> None:
            self.truncated_floor: int | None = None

        def mark_history_truncated(
            self,
            retention_floor: int,
            *,
            expected_revision: int,
        ) -> object:
            assert expected_revision == 0
            self.truncated_floor = retention_floor
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
        coordination=SimpleNamespace(
            synchronize=lambda: _synchronization(45),
        ),
        lane=_ProbeLane(),
        reader=_Reader(),
        state=state,
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

    assert state.truncated_floor == 15


def test_event_grant_rejects_non_source_component_with_grant_method() -> None:
    grant = EventPeerGrant(
        source='events',
        group='remote',
        dependent_spawn_instance='dependent-spawn-1',
        capability='remote-capability',
    )
    requested: list[EventPeerGrant] = []
    stopped: list[str] = []
    runtime = object.__new__(_WorkerRuntime)
    vars(runtime)['_components'] = {
        'source:events': SimpleNamespace(request_grant=requested.append)
    }
    vars(runtime)['request_stop'] = stopped.append

    runtime._install_event_grant(grant)

    assert requested == []
    assert stopped == ['event grant source is not owned by worker']


def test_health_cache_blocking_supplier_stops_monitor_publication(
    tmp_path: Path,
) -> None:
    site = _site(())
    config = runtime_config(tmp_path, site)
    spec = WorkerSpec(
        worker_id='["named","empty"]',
        site='main',
        kind=_WorkerKind.NAMED,
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
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

    cache.register_watchdog(
        key=('source', 'events', 'sampling-block-1', sample.registration_token),
        supplier=block,
    )
    cache.publish_source(
        _watchdog_source_health('events', worker_instance='sampling-block-1'),
        sample=sample,
    )
    runtime.start()
    try:
        assert entered.wait()
        # Negative-only check: a blocked supplier cannot publish at any runner speed.
        with pytest.raises(TimeoutError):
            parent.receive(timeout=0.05)
    finally:
        release.set()
        runtime.request_stop('test-cleanup')
        assert runtime.wait() == ()
        parent.close()
        child.close()


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
    cache.register_watchdog(
        key=('lane', 'local', 'spawn-1', published.registration_token),
        supplier=lambda: live,
    )
    cache.publish_group(durable, sample=published)

    merged = cache.snapshot().groups[0]

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
        merged.audit_count,
    ) == (41, 3, 45, 'durable park', 'durable failure', 7)


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
    cache.register_watchdog(
        key=('lane', 'local', 'spawn-1', published.registration_token),
        supplier=lambda: live,
    )
    cache.publish_group(durable, sample=published)

    merged = cache.snapshot().groups[0]

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
        cache.register_watchdog(
            key=(sample.kind, sample.identity, sample.worker_instance, sample.registration_token),
            supplier=lambda sample=sample: sample,
        )
    cache.publish_source(
        _watchdog_source_health('events', worker_instance='spawn-1'),
        sample=samples[1],
    )
    cache.publish_source(
        _watchdog_source_health('audit', worker_instance='spawn-1'),
        sample=samples[4],
    )
    cache.publish_group(
        _watchdog_group_health('zulu', source='events', worker_instance='spawn-1'),
        sample=samples[0],
    )
    cache.publish_group(
        _watchdog_group_health('alpha', source='audit', worker_instance='spawn-1'),
        sample=samples[3],
    )

    snapshot = cache.snapshot()
    registration_keys = cast('dict[tuple[object, ...], object]', vars(cache)['_watchdogs'])

    assert tuple(source.source for source in snapshot.sources) == ('audit', 'events')
    assert tuple(group.group for group in snapshot.groups) == ('alpha', 'zulu')
    assert tuple(
        (watchdog.kind, watchdog.source, watchdog.group) for watchdog in snapshot.watchdogs
    ) == (
        ('source', 'audit', None),
        ('source', 'events', None),
        ('lane', 'audit', 'alpha'),
        ('lane', 'events', 'zulu'),
        ('operator', None, None),
    )
    assert all(
        'registration_token' not in vars(watchdog) and 'transition_sequence' not in vars(watchdog)
        for watchdog in snapshot.watchdogs
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
    cache.register_watchdog(
        key=('source', 'events', 'spawn-1', sampled.registration_token),
        supplier=lambda: sampled,
    )
    cache.publish_source(
        _watchdog_source_health('events', worker_instance='spawn-1'),
        sample=published,
    )

    snapshot = cache.snapshot()

    assert snapshot.watchdogs[0].phase == 'internal'
    assert snapshot.watchdogs[0].heartbeat_at == 3.0
    assert snapshot.sources[0].phase == 'internal'
    assert snapshot.sources[0].frontier == 41
    assert snapshot.sources[0].retry_attempt == 3
    assert snapshot.sources[0].conditions == ()
    assert snapshot.sources[0].audit_count == 16
    assert snapshot.sources[0].database_size_bytes == 17


def test_health_cache_supplier_runs_outside_lock_and_successor_token_wins() -> None:
    context = multiprocessing.get_context('spawn')
    parent, child = context.Pipe(duplex=False)
    process = context.Process(target=_run_health_cache_snapshot_probe, args=(child,))
    process.start()
    child.close()
    try:
        first, second = parent.recv()
        process.join()
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        if process.is_alive():
            process.kill()
            process.join()
        parent.close()

    assert not process.is_alive()
    assert process.exitcode == 0
    assert first == ()
    assert second == (('events', 2.0),)


@pytest.mark.usefixtures('_production_admission_deadlines')
def test_lane_admission_budget_is_below_request_timeout() -> None:
    assert worker_module._LANE_OPERATOR_ADMISSION_SECS < control_module.REQUEST_TIMEOUT_SECS


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
        result = component.execute_operator_command(
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


def test_lane_operator_result_wait_uses_shutdown_slice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = threading.Event()
    waits: list[float | None] = []
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 10.0)

    class Completion(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            waits.append(timeout)
            stop.set()
            return False

    class Mailbox(_OwnerMailbox[worker_module._LaneResources]):
        def submit(
            self, invoke: Callable[[worker_module._LaneResources], object]
        ) -> _OwnerCall[worker_module._LaneResources]:
            call = super().submit(invoke)
            call.complete = Completion()
            return call

    component = object.__new__(_LaneComponent)
    vars(component).update(
        _stop=stop,
        _mailbox=Mailbox('lane'),
        _config=SimpleNamespace(daemon=SimpleNamespace(shutdown_deadline_secs=0.25)),
    )
    with pytest.raises(worker_module._CooperativeComponentStopError, match='local-call'):
        component._call(lambda _state, _client: None)
    assert waits == [0.0625]


def test_long_recheck_probe_queues_join_only_after_page_one_starts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    probe = _BusySourceProbe('long-recheck', tmp_path)
    (tmp_path / 'busy-arm').touch()
    (tmp_path / 'busy-entered').touch()
    client = object.__new__(worker_module._LocalJoinClient)
    request = JoinRequest(
        JoinAttemptIdentity(
            site='test',
            source='admin',
            group='local',
            join_kind='eager',
            group_generation=1,
            attempt_token=os.urandom(16).hex(),
        )
    )
    waits: list[int] = []

    def start_page(_seconds: float) -> None:
        assert (tmp_path / 'join-ready').exists()
        assert not (tmp_path / 'busy-call-queued').exists()
        waits.append(probe.page)
        probe.page = 1
        (tmp_path / 'busy-query-entered').touch()

    def enqueue(
        _client: worker_module._LocalJoinClient,
        value: JoinRequest,
        _channel: GroupChannelInterface,
    ) -> str:
        assert value is request
        assert probe.page == 1
        assert (tmp_path / 'busy-query-entered').exists()
        return 'queued'

    monkeypatch.setattr(worker_module._LocalJoinClient, 'request', enqueue)
    monkeypatch.setattr(
        sys.modules[_install_busy_source_request.__module__],
        'time',
        SimpleNamespace(monotonic=time.monotonic, sleep=start_page),
    )
    _install_busy_source_request(monkeypatch, probe, worker_module._LocalJoinClient)
    assert client.request(request, cast('GroupChannelInterface', object())) == 'queued'
    assert waits == [0]


def test_monitor_merges_stall_flags_into_every_record() -> None:
    cache = _ComponentHealthCache(site='main', group_sources={'local': 'events'})
    source_flags = health_module.ConditionFlags()
    lane_flags = health_module.ConditionFlags()
    source_sample = WatchdogSample(
        registration_token=_watchdog_token(),
        transition_sequence=1,
        kind='source',
        identity='events',
        worker_instance='spawn-1',
        phase='wait',
        heartbeat_at=2.0,
        deadline_at=12.0,
    )
    lane_sample = WatchdogSample(
        registration_token=_watchdog_token(),
        transition_sequence=1,
        kind='lane',
        identity='local',
        worker_instance='spawn-1',
        phase='idle',
        heartbeat_at=2.0,
        deadline_at=12.0,
    )
    source = replace(
        _watchdog_source_health('events', worker_instance='spawn-1'),
        conditions=('source-degraded',),
    )
    lane = replace(
        _watchdog_group_health('local', source='events', worker_instance='spawn-1'),
        conditions=('parked',),
    )
    for sample, flags in ((source_sample, source_flags), (lane_sample, lane_flags)):
        cache.register_watchdog(
            key=(sample.kind, sample.identity, sample.worker_instance, sample.registration_token),
            supplier=lambda sample=sample: sample,
            conditions=flags.names,
        )
    cache.publish_source(source, sample=source_sample)
    cache.publish_group(lane, sample=lane_sample)
    source_flags.set('checkpoint-stalled')
    source_flags.set('write-lock-stalled')
    lane_flags.set('write-lock-stalled')
    stalled = cache.snapshot()
    assert stalled.sources[0] == replace(
        source,
        phase='wait',
        heartbeat_at=2.0,
        deadline_at=12.0,
        conditions=('write-lock-stalled', 'checkpoint-stalled', 'source-degraded'),
    )
    assert stalled.groups[0] == replace(
        lane,
        phase='idle',
        heartbeat_at=2.0,
        deadline_at=12.0,
        conditions=('write-lock-stalled', 'parked'),
    )
    source_flags.clear('checkpoint-stalled')
    source_flags.clear('write-lock-stalled')
    lane_flags.clear('write-lock-stalled')
    clear = cache.snapshot()
    assert clear.sources[0].conditions == ('source-degraded',)
    assert clear.groups[0].conditions == ('parked',)


def test_new_group_first_query_retries_without_restarting_delivering_worker(
    tmp_path: Path,
) -> None:
    config = write_daemon_config(tmp_path, Topology('default', ('existing', 'new')))
    config.write_text(
        config.read_text().replace(
            'fetch_interval_secs = 0.05',
            'fetch_interval_secs = 0.05\n'
            'poll_retry_base_secs = 0.2\n'
            'poll_retry_multiplier = 1.5\n'
            'poll_retry_max_secs = 0.3\n'
            'poll_retry_jitter_fraction = 0',
        )
    )
    publish_text(tmp_path / 'visible-events', '1')
    with running_supervisor(
        config,
        ChildPatches(
            initial_group_query_failures=2,
            gate_events=True,
            last_event_id=4,
        ),
    ) as probe:
        probe.wait_for(
            lambda: (
                any(
                    group.group == 'existing' and group.join_phase == 'attached'
                    for health in probe.latest_health.values()
                    for group in health.groups
                )
                or bool(probe.terminations)
            ),
        )
        (tmp_path / 'events-ready').touch()
        publish_text(tmp_path / 'visible-events', '2')
        try:
            for attempt, event_id in ((1, 3), (2, 4)):
                probe.wait_for(
                    lambda attempt=attempt: (
                        len(read_complete_lines(tmp_path / 'first-group-retry-waits')) == attempt
                    ),
                )
                assert probe.records('existing') == tuple(range(2, event_id))
                publish_text(tmp_path / 'visible-events', str(event_id))
                probe.wait_for(lambda event_id=event_id: event_id in probe.records('existing'))
                # This delivery happened after the failed query and before
                # its retry was allowed to proceed, regardless of scheduling.
                (tmp_path / f'first-group-retry-{attempt}-release').touch()
        finally:
            for attempt in (1, 2):
                (tmp_path / f'first-group-retry-{attempt}-release').touch()
        probe.wait_for(
            lambda: (
                any(
                    group.group == 'new' and group.join_phase == 'attached'
                    for health in probe.latest_health.values()
                    for group in health.groups
                )
                or bool(probe.terminations)
            ),
        )
        assert not probe.terminations, probe.failure_details()
        assert len(probe.spawns) == 1, probe.failure_details()
        assert sum(breaker._failure_total for breaker in probe.breakers.values()) == 0
        attempts = [int(line) for line in read_complete_lines(tmp_path / 'first-group-queries')]
        assert attempts == [1, 2, 3]
        retry_waits = [
            float(line) for line in (tmp_path / 'first-group-retry-waits').read_text().splitlines()
        ]
        assert retry_waits == [0.2, 0.3]
        log = probe.spawns[0].log_path.read_text()
        warnings = [line for line in log.splitlines() if 'could not read the latest event' in line]
        assert [line.partition('sg_event_daemon.group: ')[2] for line in warnings] == [
            'could not read the latest event from SG to start the group: '
            f'injected first-query outage; retrying in {secs}s '
            'site=test source=admin group=new'
            for secs in ('0.2', '0.3')
        ], log
        assert all(' WARNING sg_event_daemon.group: ' in line for line in warnings)


def test_open_file_need_follows_the_measured_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
    config = load_config(write_daemon_config(tmp_path, Topology('default', ('a', 'b', 'c'))))
    site = config.sites['test']
    assert worker_module.open_file_need(site, build_worker_specs(site)[0]) == 40
    groups = dict(site.groups)
    groups['c'] = replace(groups['c'], worker='dedicated', plugins=groups['c'].plugins * 2)
    site = replace(site, groups=groups)
    local, dedicated = build_worker_specs(site)
    assert worker_module.open_file_need(site, local) == 35
    assert worker_module.open_file_need(site, dedicated) == 19
    site = replace(
        site, sources={**site.sources, 'other': replace(site.sources['admin'], name='other')}
    )
    assert worker_module.open_file_need(site, build_worker_specs(site)[0]) == 47
    groups = {name: replace(group, worker='pool') for name, group in site.groups.items()}
    site = replace(site, groups=groups)
    local, named = build_worker_specs(site)
    assert worker_module.open_file_need(site, local) == 37
    assert worker_module.open_file_need(site, named) == 41
    groups['c'] = replace(groups['c'], source='other')
    site = replace(site, groups=groups)
    assert worker_module.open_file_need(site, named) == 43
    assert worker_module.OPEN_FILE_MARGIN == 32


def test_owner_calls_preserve_callback_result_types() -> None:
    class ImmediateMailbox(_OwnerMailbox[object]):
        def __init__(self, owner: object) -> None:
            super().__init__('test')
            self._owner = owner

        def submit(self, invoke: Callable[[object], object]) -> _OwnerCall[object]:
            call = super().submit(invoke)
            call.result = call.invoke(self._owner)
            call.complete.set()
            return call

    def prepare(component: object, owner: object) -> None:
        vars(component).update(
            {
                '_mailbox': ImmediateMailbox(owner),
                '_health_interval_secs': 0.05,
                '_config': SimpleNamespace(daemon=SimpleNamespace(shutdown_deadline_secs=1)),
                '_stop': threading.Event(),
            }
        )

    ready = GroupBootstrapRequest(
        source='events', group='local', group_generation=7, start_mode='resume'
    )
    source = object.__new__(_SourceComponent)
    prepare(source, object())
    source_result = assert_type(
        source.call(lambda _surface: ready, wait_phase=nullcontext, stop=threading.Event()),
        GroupBootstrapRequest,
    )

    def acknowledge(_state: GroupState, _client: object) -> GroupBootstrapRequest | None:
        return None

    lane = object.__new__(_LaneComponent)
    prepare(lane, SimpleNamespace(state=object(), client=object()))
    observation = assert_type(lane._call(acknowledge), GroupBootstrapRequest | None)
    assert source_result is ready
    assert observation is None


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


def test_source_component_call_caller_stop_precedes_immediate_completion() -> None:
    component_type = _SourceComponent
    component = object.__new__(component_type)
    appended: list[object] = []

    class _ImmediateMailbox(_OwnerMailbox[object]):
        def submit(self, invoke: Callable[[object], object]) -> _OwnerCall[object]:
            call = super().submit(invoke)
            appended.append(call)
            call.result = 'completed'
            call.complete.set()
            return call

    vars(component).update(
        {
            '_mailbox': _ImmediateMailbox('source'),
            '_stop': threading.Event(),
        }
    )
    call = component_type.call
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
            wait_phase=nullcontext,
            stop=caller_stop,
        )

    assert appended == []


def test_source_operator_result_wait_uses_shutdown_slice(monkeypatch: pytest.MonkeyPatch) -> None:
    stop = threading.Event()
    waits: list[float | None] = []
    monkeypatch.setattr(time, 'monotonic', lambda: 10.0)

    class Completion(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            waits.append(timeout)
            stop.set()
            return False

    class Mailbox(_OwnerMailbox[worker_module._SourceJoinSurface]):
        def submit(
            self, invoke: Callable[[_SourceJoinSurface], object]
        ) -> _OwnerCall[_SourceJoinSurface]:
            call = super().submit(invoke)
            call.complete = Completion()
            return call

    component = object.__new__(_SourceComponent)
    vars(component).update(
        _stop=stop,
        _mailbox=Mailbox('source', threading.Event()),
        _health_interval_secs=0.5,
        _config=SimpleNamespace(daemon=SimpleNamespace(shutdown_deadline_secs=0.25)),
    )
    with pytest.raises(worker_module._CooperativeComponentStopError, match='local-call'):
        component.call(lambda _surface: None, stop=stop, wait_phase=nullcontext)
    assert waits == [0.0625]


def test_stop_during_reader_permit_wait_completes_without_forced_exit(tmp_path: Path) -> None:
    config = _wait_process_config(tmp_path, 'permit')
    with running_supervisor(config, ChildPatches(wait_case='permit', gate_events=True)) as probe:
        probe.wait_for(
            lambda: (
                (tmp_path / 'wait-entered').exists()
                and bool((tmp_path / 'wait-entered').read_text())
            )
        )
        timeout = float((tmp_path / 'wait-entered').read_text())
        assert (
            0
            < timeout
            <= worker_module.shutdown_slice_secs(load_config(config).daemon.shutdown_deadline_secs)
        )
        worker = next(iter(probe.supervisor._spawns.values())).worker
        assert worker.shutdown('test-permit-stop') == 0, probe.failure_details()


def test_strict_floor_above_cursor_counts_as_progress() -> None:
    class StrictFloorState(_RetentionCoordinationState):
        def __init__(self) -> None:
            super().__init__(cursor=100, delivery_floor=110)
            self._pending = (
                PendingRange(
                    obligation_id='range-floor',
                    low_id=106,
                    high_id=112,
                    closed=True,
                    close_generation=1,
                    eligibility_point=0.0,
                    visibility_deadline=0.0,
                ),
            )

        def missing_late_runs(self, _obligation_id: str) -> tuple[IdRun, ...]:
            return ((111, 112),)

        def strict_obligation_fence(self) -> tuple[str, int, int]:
            return ('range-floor', 106, 112)

        def snapshot(self) -> object:
            return SimpleNamespace(
                cursor=100,
                delivery_floor=110,
                ordering='strict',
                group_generation=1,
                source_generation='source-1',
                verified_incarnation='incarnation-1',
                revision=0,
                status='running',
            )

    state = StrictFloorState()
    reader = _RetentionCoordinationReader(
        (1, 1), (Event(runtime_event(111)), Event(runtime_event(112)))
    )
    lane = _RetentionCoordinationLane(state)
    refresh = LaneRefresh(
        lane=cast('GroupLane', lane),
        state=cast('GroupState', state),
        reader=cast('CatchupReader', reader),
        slot=nullcontext,
        joined=lambda: True,
        fetch_interval_secs=1,
        late_event_window_secs=300,
        log_extra={},
    )
    refresh.service()
    assert reader.ranges == [(111, 112)]
    assert lane.delivered == [111, 112]
    assert lane.resolved


@pytest.mark.slow
def test_wal_paused_lane_writes_after_clear(
    tmp_path: Path,
) -> None:
    config = write_daemon_config(
        tmp_path,
        Topology('dedicated', ('local',), checkpoint_interval_secs=0.2),
    )
    path = tmp_path / 'state/sites/test/sources/admin.db'
    signals = tmp_path / 'signals'
    signals.mkdir()

    def reveal_events(last_id: int) -> None:
        pending = tmp_path / 'visible-events.pending'
        pending.write_text(str(last_id))
        pending.replace(tmp_path / 'visible-events')

    def lane_rows(probe: SupervisorProbe) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for spawn in probe.spawns:
            trace = spawn.log_path.with_suffix('.wal-writes')
            rows.extend(
                cast('dict[str, object]', json.loads(line)) for line in read_complete_lines(trace)
            )
        return [row for row in rows if row['thread'] == 'lane:local']

    reveal_events(1)
    with running_supervisor(
        config,
        ChildPatches(
            writer_wal_limit_bytes=1_000_000,
            signal_dir=signals,
            gate_lane_write=True,
            gate_events=True,
            last_event_id=4,
        ),
    ) as probe:
        probe.wait_for(
            lambda: any(
                group.group == 'local' and group.join_phase == 'attached'
                for health in probe.latest_health.values()
                for group in health.groups
            ),
        )
        reveal_events(3)
        probe.wait_for(lambda: probe.records('local') == (2, 3))
        charges = sum(breaker._failure_total for breaker in probe.breakers.values())
        (signals / 'arm').touch()
        reveal_events(4)
        probe.wait_for((signals / 'lane:local.queued').exists)
        with (
            closing(sqlite3.connect(path, isolation_level=None)) as reader,
            closing(sqlite3.connect(path, isolation_level=None)) as writer,
        ):
            reader.execute('BEGIN')
            reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
            writer.execute(
                'INSERT INTO callback_marks VALUES (?, ?, 0, ?, ?, ?, 1)',
                ('pause-probe', 999999, 'probe', 'x' * 1_500_000, 'success'),
            )
            assert wal_file_bytes(path) > 1_000_000
            (signals / 'write-release').touch()
            probe.wait_for(
                lambda: any(row['phase'] == 'pause' for row in lane_rows(probe)),
            )
            probe.wait_for(
                lambda: len([row for row in lane_rows(probe) if row['phase'] == 'pause']) >= 3,
            )
            assert wal_file_bytes(path) > 1_000_000
            assert not probe.terminations, probe.failure_details()
            assert sum(breaker._failure_total for breaker in probe.breakers.values()) == charges
            released = time.monotonic()
            reader.execute('COMMIT')
            probe.wait_for(
                lambda: any(
                    row['phase'] == 'commit' and cast('float', row['at']) > released
                    for row in lane_rows(probe)
                ),
            )
            assert not probe.terminations, probe.failure_details()
            assert sum(breaker._failure_total for breaker in probe.breakers.values()) == charges


class _Component:
    def __init__(
        self,
        name: str,
        operations: list[str],
        *,
        wedge: bool = False,
    ) -> None:
        self._name = name
        self.operations = operations
        self.finished = threading.Event()
        self.release = threading.Event()
        self.started = threading.Event()
        self.wedge = wedge

    def close(self) -> None:
        self.operations.append(f'close:{self._name}')

    def execute_operator_command(
        self,
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        del wait_phase
        self.operations.append(f'operator:{self._name}:{command.command}')
        return {'owner': self._name, 'command': command.command}

    def request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        del shutdown_deadline_at
        self.operations.append(f'stop:{self._name}')
        if not self.wedge:
            self.release.set()

    def run(self) -> None:
        self.operations.append(f'run:{self._name}:{threading.get_ident()}')
        self.started.set()
        self.release.wait()
        self.finished.set()


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
        self.operator_entered = threading.Event()
        self.operator_release = threading.Event()

    def execute_operator_command(
        self,
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        del wait_phase
        self.operator_entered.set()
        self.operator_release.wait()
        return super().execute_operator_command(command)

    def request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        super().request_stop(shutdown_deadline_at=shutdown_deadline_at)
        if self._interrupt_operator:
            self.operator_release.set()


class _ControlSendComponent:
    def __init__(self, action: Callable[[], None]) -> None:
        self._action = action

    @staticmethod
    def close() -> None:
        pass

    @staticmethod
    def execute_operator_command(
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        del wait_phase
        del command
        raise AssertionError('control-send component does not execute operator commands')

    @staticmethod
    def request_stop(*, shutdown_deadline_at: float | None = None) -> None:
        del shutdown_deadline_at

    def run(self) -> None:
        self._action()


class _DispositionComponent(Protocol):
    def execute_operator_command(
        self,
        command: OperatorCommand,
    ) -> Mapping[str, object]: ...

    def request_stop(self, *, shutdown_deadline_at: float | None = None) -> None: ...

    def run(self) -> None: ...


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
    def request_grant(self, grant: EventPeerGrant) -> None: ...


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


class InterruptibleComponent:
    def __init__(
        self,
        run: Callable[[], object],
        request_stop: Callable[[], None],
    ) -> None:
        self._stop_callback = request_stop
        self._run_callback = run

    def close(self) -> None:
        pass

    def execute_operator_command(
        self,
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        del wait_phase
        del command
        raise AssertionError('interruptible probe received an operator command')

    def request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        del shutdown_deadline_at
        self._stop_callback()

    def run(self) -> None:
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


class _LifecycleComponent:
    def __init__(self, component: object, resources: object) -> None:
        self._component = component
        self._resources = resources

    def close(self) -> None:
        return None

    def execute_operator_command(
        self,
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        del wait_phase
        del command
        raise AssertionError('lifecycle-only component received an operator command')

    def request_stop(self, *, shutdown_deadline_at: float | None = None) -> None:
        del shutdown_deadline_at
        stop = cast('threading.Event', vars(self._component)['_stop'])
        stop.set()

    def run(self) -> None:
        component_type = type(self._component)
        driver = cast(
            '_LaneLifecycleDriver',
            vars(component_type)['_drive_lane'],
        )
        driver(self._component, self._resources)


class _LifecycleLane:
    def __init__(
        self,
        setup_failure: RuntimeError | None,
        resume_status: str,
    ) -> None:
        self._closed = False
        self.drains = 0
        self.joined = 0
        self.operations: list[str] = []
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
        self.operations.append('drain_live')
        self.drains += 1
        return SimpleNamespace(status='running')

    def join(self, *_args: object, **_kwargs: object) -> object:
        self.operations.append('join')
        self.joined += 1
        return object()

    def resume(self) -> object:
        self.operations.append('resume')
        return SimpleNamespace(status=self._resume_status)

    def setup(self) -> RuntimeError | None:
        self.operations.append('setup')
        return self._setup_failure


class _LocalJoinClientFactory(Protocol):
    def __call__(
        self,
        source: object,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]],
        source_name: str,
        group: str,
        stop: threading.Event,
    ) -> object: ...


@dataclass(frozen=True)
class _PressureCatchupContext:
    channel: GroupChannel
    client: EventClient
    lane: GroupLane
    state: GroupState


@dataclass(frozen=True)
class _PressureJoinContext:
    group: GroupConfig
    plugin: PluginSpec
    provider: Provider
    site: SiteConfig
    socket_path: Path


@dataclass(frozen=True)
class _RecoveryAssignmentProbe:
    conditional_park: ConditionalParkDirective | None
    group_generation: int
    start_mode: Literal['initialize', 'resume']


@dataclass(frozen=True)
class _RecoveryContextProbe:
    assignment: _RecoveryAssignmentProbe
    group: object
    site: object
    source_config: object


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

    def deliver_many_counted(
        self, deliveries: Iterable[object], *, deadline_at: float = float('inf')
    ) -> tuple[object, int]:
        del deadline_at
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


class _InitializationOutageProviderState(_RestartProviderState):
    def __init__(self) -> None:
        super().__init__()
        self.initialization_attempted = threading.Event()
        self.initialization_ready = threading.Event()


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
        rows = self._state.rows()
        if isinstance(filters, list) and any(
            isinstance(clause, dict) for clause in cast('list[object]', filters)
        ):
            matched_rows = EventLogSG(self._state.rows()).find(
                _entity_type, cast('list[object]', filters), fields, **options
            )
            self._state.record_query(matched_rows)
            return matched_rows
        order = options.get('order')
        if order:
            direction = order[0].get('direction')
            if direction == 'desc' and not filters:
                latest = 41 if not rows else cast('int', rows[-1]['id'])
                return [{'id': latest}]
            if direction == 'asc' and not filters:
                return [{'id': 1}]
        low_id, inclusive, high_id = self._bounds(cast('object', filters))
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
            self._state.record_query(result)
            return result
        self._state.record_query(selected)
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
            self._initialization_state.initialization_attempted.set()
            if not self._initialization_state.initialization_ready.is_set():
                raise RuntimeError('source initialization unavailable')
        return super().find(
            entity_type,
            filters,
            fields,
            **options,
        )


class _GapProviderHandle(RuntimeProviderHandle):
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
            return [runtime_event(60)]
        return []


class _OutageProviderHandle(RuntimeProviderHandle):
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


class _RangeCloseProviderHandle(RuntimeProviderHandle):
    def __init__(self) -> None:
        super().__init__(lambda: 102)
        self._gap_served = False
        self.recheck_returned = threading.Event()

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
                return [runtime_event(102)]
            if clauses == [
                ['id', 'greater_than', 100],
                ['id', 'less_than', 102],
            ]:
                self.recheck_returned.set()
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


class SourceHealthWaiter:
    @staticmethod
    def set() -> None:
        return None

    @staticmethod
    def wait(seconds: float) -> bool:
        del seconds
        return False


@dataclass
class _SourceRuntimeSession:
    runtime: _WorkerRuntime
    parent: WorkerControl
    child: WorkerControl
    drain_stop: threading.Event
    drain_thread: threading.Thread
    endpoint: SourceEndpointReady

    def close(self) -> None:
        release_failed_source_runtime(
            self.runtime,
            (self.parent, self.child),
            None,
            (self.drain_stop, self.drain_thread),
        )


@dataclass(frozen=True)
class _RestartContinuity:
    callback_entered: threading.Event
    config: Config
    dependent_runtime: _WorkerRuntime
    first_source: _SourceRuntimeSession
    grants: tuple[EventPeerGrant, ...]
    invoked: list[int]
    provider_state: _RestartProviderState
    release_callback: threading.Event
    site_spec: WorkerSpec
    source_assignment: WorkerAssignment


class _ProbeCoordination:
    def __init__(self, client: _SyncingClient) -> None:
        self._client = client

    @staticmethod
    def service(*, scan_obligations: bool = True, force: bool = False) -> None:
        del force
        assert not scan_obligations

    def synchronize(self) -> SourceProgress:
        return self._client.read_progress(7)


def _catch_up_pressure_join(
    context: _PressureCatchupContext,
    *,
    boundary: int,
    pressure: bool,
) -> None:
    if pressure:
        while True:
            context.client.poll(0.001)
            if context.channel.status.queued_count == 1:
                break
        assert context.channel.status.queued_count == 1
        assert context.channel.status.queued_bytes > 0
    before = context.state.snapshot()
    assert before.cursor == boundary - 1
    assert context.client.last_join_target is not None
    assert context.client.last_join_target.identity.boundary == boundary
    assert context.state.snapshot().cursor == boundary - 1
    context.lane.deliver_many_counted((Delivery(Event(runtime_event(boundary))),))


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
        sync_factory=factory,
        async_factory=None,
    )


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
        sync_factory=factory,
        async_factory=None,
    )


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

    def publish_health(_resources: object, _lifecycle: object) -> None:
        return None

    monkeypatch.setattr(worker_module, 'GroupLane', build_lane)
    monkeypatch.setattr(worker_module, '_LaneRefresh', build_coordination)
    monkeypatch.setattr(worker_module, '_load_shared_plugins', load_plugins)
    vars(component).update(
        {
            '_build_channel': build_channel,
            '_publish_health': publish_health,
        }
    )


def _local_call_stop_component(
    *,
    fail_before_stop: bool,
) -> tuple[InterruptibleComponent, threading.Event, type[Exception], str]:
    stop = threading.Event()
    entered = threading.Event()
    message = 'source component stopped during local call'
    component_type = _SourceComponent
    component = object.__new__(component_type)

    class _FailingMailbox(_OwnerMailbox[object]):
        def submit(self, invoke: Callable[[object], object]) -> _OwnerCall[object]:
            call = super().submit(invoke)
            call.error = ConnectionError(message)
            call.complete.set()
            return call

    vars(component).update(
        {
            '_mailbox': _FailingMailbox('source') if fail_before_stop else _OwnerMailbox('source'),
            '_health_interval_secs': 0.05,
            '_config': SimpleNamespace(daemon=SimpleNamespace(shutdown_deadline_secs=1.0)),
            '_stop': stop,
        }
    )
    call = component_type.call

    def invoke(_surface: object) -> None:
        return None

    def _run() -> object:
        entered.set()
        return call(component, invoke, wait_phase=nullcontext, stop=stop)

    return InterruptibleComponent(_run, stop.set), entered, ConnectionError, message


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
        sync_factory=factory,
        async_factory=None,
    )


def _resume_stop_component(
    *,
    fail_before_stop: bool,
) -> tuple[InterruptibleComponent, threading.Event, type[Exception], str]:
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
    lifecycle = lifecycle_type(channel=cast('GroupChannel', SimpleNamespace()))
    resume_lane = cast(
        'Callable[[object, object, object], bool]',
        vars(component_type)['_resume_lane'],
    )

    def _run() -> object:
        return resume_lane(
            component,
            SimpleNamespace(lane=SimpleNamespace(resume=resume)),
            lifecycle,
        )

    error_type = RuntimeError if fail_before_stop else InterruptedError
    message = provider_message if fail_before_stop else interrupted_message
    return InterruptibleComponent(_run, stop.set), entered, error_type, message


def _run_pressure_join(
    context: _PressureJoinContext,
    state: GroupState,
    *,
    boundary: int,
    live_event_id: int | None,
    pressure: bool,
) -> None:
    server = EventServer(
        path=context.socket_path,
        source='events',
        max_frame_bytes=131_072,
        queue_max_entries=1,
        queue_max_bytes=262_144,
    )
    client = EventClient(
        path=context.socket_path,
        source='events',
        group='remote',
        spawn_instance='dependent-1',
        capability='capability-1',
        max_frame_bytes=131_072,
        queue_max_entries=1,
        queue_max_bytes=262_144,
        coordination_deadline_secs=stall_deadline('coordination', 30.0),
        stop=threading.Event(),
        wait_phase=nullcontext,
        wait_slice_secs=0.1,
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
        site=context.site,
        group=context.group,
        provider=context.provider,
        plugins=(context.plugin,),
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
        channel=channel,
        client=client,
        lane=lane,
        state=state,
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


def _service_source_health(
    component: object,
    server: _SourceHealthServer,
    poller: SourcePoller,
) -> None:
    _SourceComponent._service(
        cast('_SourceComponent', component),
        cast('EventServer', server),
        cast('_SourceJoinSurface', SimpleNamespace(reconcile_groups=lambda: None)),
        poller,
    )


def _start_disposition_component(
    component: _DispositionComponent,
    host: CheckpointHost,
    errors: list[Exception],
) -> threading.Thread:
    def run() -> None:
        try:
            component.run()
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


def group_config(
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


def _group_health_component(
    state_dir: Path,
    state: GroupState,
    *,
    health_interval_secs: float,
) -> tuple[_LaneComponent, _ComponentHealthCache, SimpleNamespace, SimpleNamespace]:
    group = group_config('local', worker='default')
    site = runtime_site(group)
    config = runtime_config(state_dir, site)
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
        assignment=local_assignment(spec, 'local', 'resume'),
        config=config,
        control=cast('WorkerControlOutbox', SimpleNamespace(enqueue=send)),
        group='local',
        health_cache=cache,
        limiter=_CatchupLimiter(1),
        local_source=None,
        spec=spec,
    )
    context = SimpleNamespace(
        group=group,
        site=site,
        source_config=site.sources['events'],
    )
    lane = _GroupHealthLane(state.snapshot())
    channel = SimpleNamespace(
        close=lambda: None,
        status=SimpleNamespace(attached=True, queued_bytes=0, queued_count=0),
    )
    resources = SimpleNamespace(
        client=SimpleNamespace(last_join_target=None),
        context=context,
        coordination=SimpleNamespace(service=_ProbeCoordination.service),
        lane=lane,
        state=state,
    )
    lifecycle = SimpleNamespace(
        channel=channel,
        joined=True,
        next_coordination_at=0.0,
        was_running=True,
        setup_failure=None,
        rejoin_reason=None,
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


def _open_coordination_source(path: Path, *, stamped: bool = True) -> SourceState:
    state = SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site='main',
        source='events',
        stream_fingerprint='stream',
        source_generation='source-1',
        late_event_window_secs=1.0,
    )

    if stamped:
        state.stamp_generation()
    return state


def _open_group_health_state(
    path: Path,
    *,
    delivery_floor: int = 41,
    max_late_ids_per_group: int = 50_000,
    ordering: Literal['eager', 'strict'] = 'eager',
) -> GroupState:
    options: GroupOpenOptions = {
        'writes': write_context_for(path),
        'site': 'main',
        'site_server_url': 'https://example.test',
        'source': 'events',
        'group': 'local',
        'group_generation': 7,
        'ordering': ordering,
        'stream_fingerprint': stream_fingerprint(
            'https://example.test',
            'source',
            'daemon',
        ),
        'source_generation': 'source-generation-1',
        'verified_incarnation': 'stream-incarnation-1',
        'start_mode': 'resume' if group_exists(path, 'local') else 'initialize',
        'max_late_ids_per_group': max_late_ids_per_group,
    }
    if options['start_mode'] == 'initialize':
        options['initial_cursor'] = 41
        options['initial_delivery_floor'] = delivery_floor
        options['initial_last_delivered_id'] = delivery_floor
    return open_group_on_source(path, **options)


def _assert_operator_retry(
    control: WorkerControl, request_id: str, *, fails_again: bool = False
) -> None:
    send_control(
        control,
        OperatorCommand(
            request_id=request_id,
            command='retry',
            arguments={'site': 'main', 'group': 'local'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=123),
        ),
    )
    result = receive_type(control, CommandResult)
    if fails_again:
        assert not result.ok
        assert 'setup retry failed again' in result.message
    else:
        assert result.ok, result.message


@contextmanager
def _monitor_control_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[tuple[_WorkerRuntime, WorkerControl, WorkerControl, _Component]]:
    site = _site(())
    config = runtime_config(tmp_path, site)
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=_WorkerKind.SITE,
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    component = _Component('source:events', [])
    runtime = build_worker_runtime(config, spec, assignment, child)

    def source_factory(_name: str, _slots: _CatchupLimiter, _host: CheckpointHost) -> _Component:
        return component

    monkeypatch.setattr(runtime, '_source_factory', source_factory)
    try:
        yield runtime, parent, child, component
    finally:
        component.release.set()
        if component.started.is_set():
            assert component.finished.wait()
        runtime.request_stop('test-cleanup')
        runtime._shutdown()
        parent.close()
        child.close()


def _run_hung_strict_synchronization_probe(connection: Connection, path: Path) -> None:
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
        watchdog.transition('idle', deadline_at=26.0)
        vars(component)['_watchdog'] = watchdog
        assert vars(component)['_internal_stall_deadline_secs'] == 192.0
        entered = threading.Event()

        def synchronize() -> NoReturn:
            entered.set()
            threading.Event().wait()
            raise AssertionError('hung synchronization unexpectedly returned')

        def strict_step(**options: object) -> NoReturn:
            cast('Callable[[], SourceProgress]', options['source_progress'])()
            raise AssertionError('hung strict step unexpectedly returned')

        vars(resources).update(
            context=SimpleNamespace(group=SimpleNamespace(ordering='strict'), holdback_secs=300.0),
            client=SimpleNamespace(
                last_join_target=SimpleNamespace(
                    identity=SimpleNamespace(boundary=45),
                    stream_incarnation='stream-incarnation-1',
                )
            ),
            reader=SimpleNamespace(strict_step=strict_step),
            coordination=SimpleNamespace(synchronize=synchronize),
        )

        def observe() -> None:
            assert entered.wait()
            wall[0] = 193.0
            connection.send(watchdog.sample())

        threading.Thread(target=observe).start()
        component._catch_up(resources, 45)


def _seed_coordination_range(state: SourceState) -> str:
    state.initialize(41, incarnation='incarnation-1')
    _bootstrap_coordination_group(state._database.path)
    opened = open_source_range(state, 42, 45, successful_clock=1.0)
    return opened.obligation_id


def _source_health_component(
    state_dir: Path,
    *,
    health_interval_secs: float,
) -> tuple[_SourceComponent, _ComponentHealthCache]:
    site = runtime_site(group_config('local', worker='default'))
    config = runtime_config(state_dir, site)
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
        assignment=local_assignment(spec, 'local', 'resume'),
        config=config,
        control=cast('WorkerControlOutbox', SimpleNamespace(enqueue=send)),
        health_cache=cache,
        source='events',
        spec=spec,
    )
    return component, cache


def _source_health_poller(state: SourceState, provider: Provider) -> SourcePoller:
    site = runtime_site(group_config('local', worker='default'))
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
        waiter=SourceHealthWaiter(),
        random_value=lambda: 0.5,
        watchdog=watchdog,
        internal_deadline_secs=192.0,
        wait_tolerance_secs=5.0,
        worker_instance='site-spawn-1',
        successful_clock=_SuccessfulQueryClock(clock=lambda: 0.0),
    )


def _unopened_group_component(component_type: type[_ComponentT]) -> _ComponentT:
    def send(_message: object) -> None:
        return None

    site = runtime_site(group_config('local', worker='default'))
    spec = build_worker_specs(site)[0]
    factory = cast('Callable[..., _ComponentT]', component_type)
    return factory(
        checkpoint_host=cast('CheckpointHost', _IdleCheckpointHost()),
        assignment=local_assignment(spec, 'local', 'resume'),
        config=runtime_config(Path.cwd(), site),
        control=cast('WorkerControlOutbox', SimpleNamespace(enqueue=send)),
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
) -> tuple[InterruptibleComponent, threading.Event, type[Exception], str]:
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

    return InterruptibleComponent(_run, stop.set), entered, InterruptedError, message


def _eager_catchup_stop_component(
    *,
    fail_before_stop: bool,
) -> tuple[InterruptibleComponent, threading.Event, type[Exception], str]:
    stop = threading.Event()
    entered = threading.Event()
    message = 'lane stopped during eager catch-up'
    component = _unopened_group_component(_LaneComponent)

    def catch_up(**options: object) -> None:
        entered.set()
        if fail_before_stop:
            raise InterruptedError(message)
        assert stop.wait()
        deliver_page = cast('Callable[[Sequence[Event]], None]', options['deliver_page'])
        deliver_page((Event(runtime_event(41)),))

    def deliver_many_counted(
        deliveries: Iterable[object], *, deadline_at: float = float('inf')
    ) -> tuple[object, int]:
        del deadline_at
        return SimpleNamespace(status='running'), len(tuple(deliveries))

    resources = SimpleNamespace(
        state=SimpleNamespace(
            snapshot=lambda: SimpleNamespace(
                cursor=40,
                delivery_floor=40,
                replay_required_from=None,
            ),
            unresolved_obligation_bounds=lambda: (),
        ),
        coordination=SimpleNamespace(
            synchronize=lambda: None,
        ),
        lane=SimpleNamespace(
            deliver_many_counted=deliver_many_counted,
            shutdown_requested=stop.is_set,
        ),
        reader=SimpleNamespace(catch_up=catch_up),
    )

    def _run() -> object:
        return _LaneComponent._catch_up_eager(
            component,
            cast('_LaneResources', resources),
            cast('JoinTarget', SimpleNamespace(identity=SimpleNamespace(boundary=41))),
        )

    return InterruptibleComponent(_run, stop.set), entered, InterruptedError, message


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
                daemon=SimpleNamespace(max_pending_epochs=8, shutdown_deadline_secs=1.0),
            ),
            '_limiter': _CatchupLimiter(1),
            '_health_interval_secs': 1.0,
            '_stop': threading.Event(),
        }
    )
    resources = SimpleNamespace(
        client=client,
        coordination=_ProbeCoordination(client),
        context=SimpleNamespace(
            assignment=SimpleNamespace(group_generation=7),
            group=SimpleNamespace(ordering=ordering),
            holdback_secs=1.0,
            source_config=SimpleNamespace(fetch_interval_secs=0.001),
        ),
        lane=lane,
        reader=reader,
        state=_ProbeState(),
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


def _reader_slot_stop_component(
    *, fail_before_stop: bool
) -> tuple[InterruptibleComponent, threading.Event, type[Exception], str]:
    stop = threading.Event()
    entered = threading.Event()
    message = 'reader slot acquisition failed'
    component = _unopened_group_component(_LaneComponent)
    component._stop = stop

    def acquire(*, timeout: float) -> bool:
        entered.set()
        if fail_before_stop:
            raise RuntimeError(message)
        stop.wait(timeout)
        return False

    component._limiter.acquire = acquire

    def run() -> None:
        with component._reader_slot():
            pytest.fail('the held reader slot was acquired')

    return InterruptibleComponent(run, stop.set), entered, RuntimeError, message


def _strict_catchup_stop_component(
    *,
    fail_before_stop: bool,
) -> tuple[InterruptibleComponent, threading.Event, type[Exception], str]:
    stop = threading.Event()
    entered = threading.Event()
    message = 'lane stopped during strict catch-up'
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(component).update(
        {
            '_config': SimpleNamespace(
                daemon=SimpleNamespace(max_pending_epochs=10, shutdown_deadline_secs=1.0),
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
        context=SimpleNamespace(
            holdback_secs=1.0,
            source_config=SimpleNamespace(fetch_interval_secs=10.0),
        ),
        coordination=SimpleNamespace(
            synchronize=lambda: None,
            service=_ProbeCoordination.service,
        ),
        lane=SimpleNamespace(deliver_one=deliver_one),
        reader=SimpleNamespace(strict_step=strict_step),
        state=_ProbeState(),
    )
    catch_up = component_type._catch_up_strict

    def _run() -> object:
        return catch_up(
            component,
            cast('_LaneResources', resources),
            cast('JoinTarget', SimpleNamespace(identity=SimpleNamespace(boundary=41))),
        )

    return InterruptibleComponent(_run, stop.set), entered, InterruptedError, message


def _cooperative_stop_component(
    phase: _ComponentStopPhase,
    *,
    fail_before_stop: bool,
) -> tuple[InterruptibleComponent, threading.Event, type[Exception], str]:
    factories = {
        'bootstrap': _bootstrap_stop_component,
        'eager-catchup': _eager_catchup_stop_component,
        'local-call': _local_call_stop_component,
        'reader-slot': _reader_slot_stop_component,
        'resume': _resume_stop_component,
        'strict-catchup': _strict_catchup_stop_component,
    }
    return factories[phase](fail_before_stop=fail_before_stop)


def pending_group_runtime(
    state_dir: Path,
) -> tuple[Config, WorkerSpec, WorkerAssignment, GroupSnapshot]:
    site = runtime_site(group_config('local', worker='default'))
    source = replace(site.sources['events'], fetch_interval_secs=60.0)
    site = replace(site, sources=MappingProxyType({'events': source}))
    config = runtime_config(state_dir, site)
    spec = replace(build_worker_specs(site)[0], groups=())
    assignment = replace(
        local_assignment(spec, 'local', 'resume'),
        group_assignments=(),
        source_group_assignments=(
            PendingDispositionSourceGroupAssignment(
                source='events', group='local', orphan_generation=7, reserved_generation=11
            ),
        ),
    )
    with _open_group_health_state(source_database_path(state_dir)) as state:
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
    config, spec, assignment, _snapshot = pending_group_runtime(state_dir)
    config = replace(config, daemon=replace(config.daemon, health_interval_secs=1.0))
    cache = _ComponentHealthCache(site='main', group_sources={'local': 'events'})

    def send(_message: object) -> None:
        return None

    component = _SourceComponent(
        checkpoint_host=cast('CheckpointHost', _IdleCheckpointHost()),
        assignment=assignment,
        config=config,
        control=cast('WorkerControlOutbox', SimpleNamespace(enqueue=send)),
        health_cache=cache,
        source='events',
        spec=spec,
    )
    site = config.sites['main']
    source_config = site.sources['events']
    path = source_database_path(state_dir)
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


def prepare_runtime_state(
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
        source_database_path(state_dir),
        writes=write_context_for(source_database_path(state_dir)),
        create=True,
        site=site.name,
        source=group.source,
        stream_fingerprint=fingerprint,
        source_generation='source-generation-old',
    ) as source_state:
        source_state.initialize(41, incarnation='stream-incarnation-1')
    with open_group_on_source(
        source_database_path(state_dir),
        writes=write_context_for(source_database_path(state_dir)),
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
        strict_holdback_secs=source_config.late_event_window_secs,
    ) as state:
        in_flight = options.get('in_flight')
        if in_flight is not None:
            event_id, manifest = in_flight
            commit_step(
                state,
                StartEventStep(event_id, manifest, None),
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


def release_failed_source_runtime(
    runtime: _WorkerRuntime | None,
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
        _attempt_cleanup(lambda: stop_control_drain(*drain), errors)
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


@contextmanager
def _held_limiter_slot(limiter: _CatchupLimiter) -> Generator[None]:
    while not limiter.acquire(timeout=0.1):
        pass
    try:
        yield
    finally:
        limiter.release()


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
                # Holdback waits release the permit; in-epoch gap waits retain it.
                if wall[0] < 300.0:
                    with _held_limiter_slot(limiter):
                        pass
            if wall[0] == 100.0:
                close_range()
            wall[0] += 50.0
            return self.is_set()

    return HoldbackWait()


def lifecycle_probe(
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
        channel=_LifecycleChannel(),
        client=_LifecycleReader(),
        context=SimpleNamespace(
            group=SimpleNamespace(
                execution=execution,
                ordering='eager',
                name='local',
                source='events',
            ),
            source_config=SimpleNamespace(fetch_interval_secs=0.001),
        ),
        coordination=_LifecycleCoordination(),
        lane=_LifecycleLane(setup_failure, resume_status),
        reader=_LifecycleReader(),
        state=_LifecycleState(durable_status),
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
                wait_phase=nullcontext,
                source_name='events',
                group='local',
                stop=threading.Event(),
            ),
            surface,
            _close_range,
        )


def _recovery_construction_probe(
    monkeypatch: pytest.MonkeyPatch,
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
        assignment=_RecoveryAssignmentProbe(
            conditional_park=directive,
            group_generation=7,
            start_mode='resume',
        ),
        group=SimpleNamespace(name='remote', ordering='eager', source='events'),
        site=SimpleNamespace(name='main'),
        source_config=SimpleNamespace(fetch_interval_secs=0.01, late_event_window_secs=300.0),
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

    def publish_startup_health(_context: object, _state: object) -> None:
        operations.append('initial-health')

    def provider_reader(_context: object) -> tuple[object, object]:
        operations.append('provider')
        return object(), reader

    def stop_plugins(_loader: object) -> object:
        operations.append('plugin-construction')
        raise _RecoveryConstructionReachedError

    if stop_at_construction:
        monkeypatch.setattr(worker_module, '_load_shared_plugins', stop_plugins)

    def validate_source_generation(_observation: object, _expected: str) -> None:
        operations.append('validate')

    vars(component).update(
        {
            '_assignment': SimpleNamespace(spawn_instance='current-spawn-1'),
            '_build_client': build_client,
            '_control': control,
            '_publish_startup_group_health': publish_startup_health,
            '_config': SimpleNamespace(
                daemon=SimpleNamespace(
                    async_cancel_grace_secs=0.0,
                    health_interval_secs=1.0,
                    max_pending_epochs=1,
                    shutdown_deadline_secs=1.0,
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
        monkeypatch, stop_at_construction=False
    )
    component = raw
    _install_recovery_gate_probes(monkeypatch, component, operations)
    now = [100.0]
    monkeypatch.setattr(
        worker_module, 'time', SimpleNamespace(monotonic=lambda: now[0], time=time.time)
    )
    monkeypatch.setattr(component._watchdog, '_clock', lambda: now[0])
    component._watchdog.transition('idle', deadline_at=135.0)
    context = replace(
        context,
        source_config=SimpleNamespace(fetch_interval_secs=30.0, late_event_window_secs=300.0),
    )
    cache = _ComponentHealthCache(
        site='main', group_sources=MappingProxyType({'remote': 'events'})
    )
    sample = component._watchdog.sample()
    cache.register_watchdog(
        key=(sample.kind, sample.identity, sample.worker_instance, sample.registration_token),
        supplier=component._watchdog.sample,
    )
    assert cache.snapshot().watchdogs[0].phase == 'idle'
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
        if not ready.wait():
            stop.set()
            future.result()
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
            coordination_deadline_secs=stall_deadline('coordination', 30.0),
            stop=threading.Event(),
            wait_phase=nullcontext,
            wait_slice_secs=0.1,
        )
        client.connect()

        def _close_range() -> None:
            close_requested.set()
            if close_complete.wait():
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
        sync_factory=factory,
        async_factory=async_factory if async_ else None,
    )


def _assert_catchup_outlasts_internal_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    ordering: Literal['eager', 'strict'],
) -> None:
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    group = group_config('local', worker='default', ordering=ordering)
    site = runtime_site(group)
    config = lift_config(
        Config(
            daemon=_DaemonConfig(state_dir=tmp_path, sg_api_provider='shotgun-api3'),
            sites=MappingProxyType({site.name: site}),
        )
    )
    spec = build_worker_specs(site)[0]
    cache = _ComponentHealthCache(site=site.name, group_sources={'local': 'events'})

    def send(_message: object) -> None:
        return None

    component = _LaneComponent(
        checkpoint_host=cast('CheckpointHost', _IdleCheckpointHost()),
        assignment=local_assignment(spec, 'local', 'resume'),
        config=config,
        control=cast('WorkerControlOutbox', SimpleNamespace(enqueue=send)),
        group='local',
        health_cache=cache,
        limiter=_CatchupLimiter(1),
        local_source=None,
        spec=spec,
    )
    monkeypatch.setattr(component._watchdog, '_clock', lambda: clock[0])
    assert component._internal_stall_deadline_secs == 192.0
    component._watchdog.transition('idle', deadline_at=111.0)
    provider_state = _RestartProviderState()
    for event_id in range(42, 72):
        provider_state.publish(event_id)
    provider = _restart_provider(provider_state)
    observations: list[tuple[float, WatchdogHealth]] = []
    synchronizations: list[WatchdogSample] = []

    def synchronize() -> SourceProgress:
        synchronizations.append(component._watchdog.sample())
        return _synchronization(71)

    path = tmp_path / 'group.db'
    with (
        open_group_on_source(
            path,
            writes=write_context_for(path),
            site='main',
            site_server_url=site.server,
            source='events',
            group='local',
            group_generation=7,
            ordering=ordering,
            stream_fingerprint=stream_fingerprint(site.server, 'source', 'daemon'),
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='initialize',
            initial_cursor=41,
            initial_delivery_floor=41,
            initial_last_delivered_id=41,
            strict_holdback_secs=1.0,
        ) as state,
        CatchupReader(
            provider=provider,
            site_url=site.server,
            credential=site.credentials['source'],
            batch_size=1,
            fetch_interval_secs=1.0,
            waiter=SourceHealthWaiter(),
            provider_watch=lambda: component._watch_phase('internal'),
        ) as reader,
        closing(
            GroupLane(
                state=state,
                site=site,
                group=group,
                provider=provider,
                plugins=(),
                worker_instance=component._assignment.spawn_instance,
                health_interval_secs=2.0,
                watchdog=component._watchdog,
            )
        ) as lane,
    ):
        assert lane.setup() is None
        if ordering == 'strict':
            snapshot = state.capture_epoch(
                low_id=42,
                high_id=71,
                captured_at=0.0,
                eligibility_point=1.0,
                reader_clock_checkpoint=0.0,
                source_generation='source-1',
                expected_revision=state.snapshot().revision,
            )
            state.mark_epoch_eligible(
                snapshot.epochs[0].epoch_id,
                reader_clock_checkpoint=1.0,
                expected_revision=snapshot.revision,
            )
        validate = reader._events

        def slow_validation(rows: Sequence[Mapping[str, object]]) -> tuple[Event, ...]:
            clock[0] += 8.0
            # Cache snapshots construct and validate the published health projection.
            observations.append((clock[0], cache.snapshot().watchdogs[0]))
            return validate(rows)

        monkeypatch.setattr(reader, '_events', slow_validation)
        resources = cast(
            '_LaneResources',
            SimpleNamespace(
                state=state,
                lane=lane,
                reader=reader,
                context=SimpleNamespace(
                    group=group,
                    holdback_secs=1.0,
                    source_config=site.sources['events'],
                ),
                coordination=SimpleNamespace(
                    synchronize=synchronize, service=Mock(return_value=None)
                ),
                client=SimpleNamespace(
                    last_join_target=SimpleNamespace(
                        identity=SimpleNamespace(boundary=71),
                        stream_incarnation='incarnation-1',
                    )
                ),
            ),
        )
        component._catch_up(resources, 71)
        assert state.snapshot().cursor == 71
        assert state.snapshot().epochs == ()
    assert clock[0] == 340.0
    assert len(observations) == 30
    assert all(sample.deadline_at is not None for _, sample in observations)
    assert not [
        (at, sample.phase, sample.deadline_at)
        for at, sample in observations
        if sample.deadline_at is not None and at > sample.deadline_at
    ]
    assert len(synchronizations) == 1
    assert synchronizations[0].phase == 'internal'
    assert synchronizations[0].deadline_at == 292.0


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
    source_path = source_database_path(tmp_path)
    with _open_coordination_source(source_path) as source:
        source.initialize(41, incarnation='incarnation-1')
        _bootstrap_coordination_group(source_path, ordering='strict')
        open_source_range(source, 46 if case == 'new-fence' else 44, 48, successful_clock=1.0)
        source.commit_observations((50, 51), successful_clock=1.0)
        # The obligation was already eligible before the detached epoch's holdback.
        source._database.connection.execute('UPDATE range_groups SET eligibility_point = 1.0')
        source._database.connection.execute(BUMP_RANGE_REVISION)
        if case == 'stored-capacity':
            source._database.connection.execute(
                "UPDATE group_state SET strict_capacity_exhausted = 1 WHERE group_name = 'local'"
            )
    provider_state = _RestartProviderState()
    for event_id in (42, 46 if case == 'new-fence' else 44, 48, 50):
        provider_state.publish(event_id)
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
        group = replace(
            group_config('local', worker='default'), ordering='strict', execution=execution
        )
        site = runtime_site(group)
        watchdog = component._watchdog
        watchdog.transition('idle', deadline_at=None)
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
                waiter=SourceHealthWaiter(),
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
            vars(resources)['client'] = client
            original_confirm = resources.client.confirm

            def confirm(confirmation: JoinConfirm) -> Attached | JoinRetry:
                assert state.snapshot().obligation_count == 0
                trace.append('confirm')
                return original_confirm(confirmation)

            monkeypatch.setattr(resources.client, 'confirm', confirm)
            vars(resources).update(
                context=SimpleNamespace(
                    group=group,
                    holdback_secs=300.0,
                    source_config=replace(site.sources['events'], fetch_interval_secs=50.0),
                ),
                lane=lane,
                reader=reader,
                coordination=LaneRefresh(
                    joined=lambda: False,
                    fetch_interval_secs=1,
                    late_event_window_secs=300,
                    log_extra={},
                    lane=lane,
                    slot=component._reader_slot,
                    reader=reader,
                    state=state,
                ),
            )
            _assert_detached_strict_join(component, resources, (callbacks, trace, wall), case)


def run_cooperative_stop_component(
    component: InterruptibleComponent,
    entered: threading.Event,
    *,
    request_stop: bool,
) -> tuple[str | None, list[int]]:
    spec = WorkerSpec(
        worker_id='["named","main","default"]',
        site='main',
        kind=_WorkerKind.NAMED,
        name='default',
        sources=(),
        groups=('local',),
    )
    forced: list[int] = []
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: component,
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
        force_exit=forced.append,
    )
    runtime.start()
    try:
        assert entered.wait()
        if request_stop:
            runtime.request_stop('test-stop')
        assert _wait_for_runtime_stop(runtime) == ()
    finally:
        runtime.request_stop('test-cleanup')
        assert runtime._shutdown() == ()
    return runtime.fatal_reason, forced


def runtime_provider(*, calls: list[str] | None = None) -> Provider:
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
    ) -> RuntimeProviderHandle:
        del site
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        return RuntimeProviderHandle(_latest, calls)

    return Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(
            sync=True,
            async_=False,
        ),
        sync_factory=factory,
        async_factory=None,
    )


def concrete_delivery_provider(
    monkeypatch: pytest.MonkeyPatch,
    start_mode: Literal['initialize', 'resume'],
) -> tuple[Provider, _RestartProviderState, list[int]]:
    provider_state = _RestartProviderState()
    invoked: list[int] = []
    if start_mode == 'initialize':
        return runtime_provider(), provider_state, invoked
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
def running_concrete_runtime(
    monkeypatch: pytest.MonkeyPatch,
    config: Config,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
    provider: Provider,
) -> Generator[tuple[_WorkerRuntime, WorkerControl]]:
    monkeypatch.setattr(
        worker_module,
        'load_provider',
        provider_loader(provider),
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = build_worker_runtime(config, spec, assignment, child)
    runtime.start()
    try:
        yield runtime, parent
    finally:
        drain: tuple[threading.Event, threading.Thread] | None = None
        try:
            drain = start_control_drain(parent)
        finally:
            release_failed_source_runtime(
                runtime,
                (parent, child),
                (parent_socket, child_socket),
                drain,
            )


def _wait_for_lane_token(
    runtime: _WorkerRuntime,
    group: str,
    prior_token: str | None = None,
) -> str | None:
    while True:
        lane = _runtime_lane(runtime, group)
        if lane is not None:
            assert lane._last_snapshot.status == 'running', lane.health()
            join_state = vars(lane)['_join_state']
            # A provisional attempt does not establish a ready delivery endpoint.
            if isinstance(join_state, _FinalizedJoin):
                token = join_state.identity.attempt_token
            else:
                token = None
            if token and token != prior_token:
                return token
        if runtime.fatal_reason is not None:
            break
        time.sleep(0.01)
    return None


def _assert_initial_endpoint_delivery(
    runtime: _WorkerRuntime,
    provider_state: _RestartProviderState,
    invoked: list[int],
) -> None:
    assert _wait_for_lane_token(runtime, 'remote') is not None
    assert _wait_for_lane_token(runtime, 'healthy') is not None
    provider_state.publish(42)
    while True:
        assert runtime.fatal_reason is None, runtime.fatal_reason
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
    runtime: _WorkerRuntime,
    path: Path,
    provider_state: _RestartProviderState,
    invoked: list[int],
    group: str,
) -> None:
    assert _wait_for_lane_token(runtime, group) is not None
    provider_state.publish(58)
    _wait_for_group_delivery(runtime, path, invoked, 58)


def _wait_for_source_component_progress(
    ready: Callable[[], bool],
    errors: list[Exception],
    *,
    pump: Callable[[], object] | None = None,
) -> None:
    reached = False
    while True:
        if pump is not None:
            pump()
        reached = ready()
        if reached or errors:
            break
        time.sleep(0.001)
    assert errors == []
    assert reached


def start_source_runtime(
    config: Config,
    spec: WorkerSpec,
    assignment: WorkerAssignment,
    grants: tuple[EventPeerGrant, ...],
) -> _SourceRuntimeSession:
    parent_socket, child_socket = socket.socketpair()
    parent: WorkerControl | None = None
    child: WorkerControl | None = None
    runtime: _WorkerRuntime | None = None
    drain_stop: threading.Event | None = None
    drain_thread: threading.Thread | None = None
    transferred = False
    try:
        parent = WorkerControl(
            parent_socket,
            max_frame_bytes=131_072,
        )
        child = WorkerControl(
            child_socket,
            max_frame_bytes=131_072,
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
            for endpoint in (receive_type(parent, SourceEndpointReady) for _ in spec.sources)
        }
        endpoint = endpoints[grants[0].source]
        for grant in grants:
            send_control(parent, grant)
            assert receive_type(parent, EventPeerReady) == EventPeerReady(
                source=grant.source,
                group=grant.group,
                dependent_spawn_instance=grant.dependent_spawn_instance,
            )
        drain_stop, drain_thread = start_control_drain(parent)
        session = _SourceRuntimeSession(
            runtime=runtime,
            parent=parent,
            child=child,
            drain_stop=drain_stop,
            drain_thread=drain_thread,
            endpoint=endpoint,
        )
        transferred = True
        return session
    finally:
        if not transferred:
            release_failed_source_runtime(
                runtime,
                (parent, child),
                (parent_socket, child_socket),
                (
                    (drain_stop, drain_thread)
                    if drain_stop is not None and drain_thread is not None
                    else None
                ),
            )


def _assert_source_restart_continuity(
    scenario: _RestartContinuity, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_token = _wait_for_lane_token(
        scenario.dependent_runtime,
        'remote',
    )
    assert first_token is not None
    healthy_first_token = _wait_for_lane_token(
        scenario.dependent_runtime,
        'healthy',
    )
    assert healthy_first_token is not None
    scenario.provider_state.publish(42)
    assert scenario.callback_entered.wait()
    scenario.provider_state.publish(43)
    assert scenario.provider_state.wait_for_query(43, 'source:events')
    first_endpoint = scenario.first_source.endpoint
    grants_ready = threading.Event()
    original_connect = EventClient.connect

    def connect(client: EventClient) -> None:
        assert grants_ready.wait()
        original_connect(client)

    # This fixture keeps dependents alive while the replacement source installs grants.
    monkeypatch.setattr(EventClient, 'connect', connect)
    try:
        scenario.first_source.close()
        scenario.release_callback.set()
        # Negative-only observation: correct code cannot deliver from the closed source.
        time.sleep(0.05)
        assert scenario.invoked == [42]
        assert scenario.dependent_runtime.fatal_reason is None
        second_source = start_source_runtime(
            scenario.config,
            scenario.site_spec,
            scenario.source_assignment,
            scenario.grants,
        )
    finally:
        grants_ready.set()
    try:
        assert second_source.endpoint == first_endpoint
        second_token = _wait_for_lane_token(
            scenario.dependent_runtime,
            'remote',
            first_token,
        )
        assert second_token is not None, {
            'source_fatal_reason': second_source.runtime.fatal_reason,
            'dependent_fatal_reason': scenario.dependent_runtime.fatal_reason,
            'remote_lifecycle': cast(
                '_LaneComponent', scenario.dependent_runtime._components['lane:remote']
            )._lifecycle,
            'owner_health': scenario.dependent_runtime._current_health(time.monotonic()),
        }
        assert second_token != first_token
        healthy_second_token = _wait_for_lane_token(
            scenario.dependent_runtime,
            'healthy',
            healthy_first_token,
        )
        assert healthy_second_token is not None
        assert healthy_second_token != healthy_first_token
        while True:
            remote_lane = _runtime_lane(
                scenario.dependent_runtime,
                'remote',
            )
            if (
                scenario.invoked == [42, 43]
                and remote_lane is not None
                and vars(remote_lane)['_last_snapshot'].cursor == 43
            ):
                break
            time.sleep(0.001)
        remote_lane = _runtime_lane(scenario.dependent_runtime, 'remote')
        healthy_lane = _runtime_lane(scenario.dependent_runtime, 'healthy')
        assert scenario.invoked == [42, 43], {
            'source_fatal_reason': second_source.runtime.fatal_reason,
            'dependent_fatal_reason': scenario.dependent_runtime.fatal_reason,
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
        assert scenario.dependent_runtime.fatal_reason is None
    finally:
        second_source.close()


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
        monkeypatch.setattr(
            worker_module, 'time', SimpleNamespace(monotonic=lambda: 100.0, time=time.time)
        )
        monkeypatch.setattr(probe.watchdog, '_clock', lambda: 100.0)

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
                assert probe.reconnect_release.wait()

            monkeypatch.setattr(EventClient, 'reconnect', reconnect)
            vars(probe.resources)['client'] = _unconnected_event_client(tmp_path / 'events.sock')
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

            vars(probe.resources)['lane'] = SimpleNamespace(join=return_join_retry)
            lifecycle = SimpleNamespace(channel=probe.channel, joined=False)
            operation = partial(
                cast('type[_LaneComponent]', type(component))._join_lane,
                cast('_LaneComponent', component),
                probe.resources,
                cast('_LaneLifecycle', lifecycle),
            )
        internal, startup, result, internal_budget = _run_blocked_retirement(
            probe,
            operation,
            expect_reconnect=path == 'reconnect',
        )

    assert internal.phase == 'internal'
    assert internal.heartbeat_at == 100.0
    assert internal.deadline_at == 100.0 + internal_budget
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
        assert lifecycle.channel is probe.replacement
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
            assert release.wait()

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
                assert entered.wait()
                snapshot = cache.snapshot()
            finally:
                release.set()
            future.result()

    assert snapshot.watchdogs[0].phase == 'startup'
    assert snapshot.watchdogs[0].deadline_at is not None


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('stage', ['reader', 'synchronization'])
def test_blocked_lane_catchup_publishes_bounded_phase(
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
        context = vars(resources)['context']
        context.group = replace(context.group, ordering=ordering)
        context.holdback_secs = 0.0
        vars(resources)['state'] = _ProbeState()

        def block() -> None:
            entered.set()
            assert release.wait()

        synchronize = block if stage == 'synchronization' else lambda: None
        vars(resources)['coordination'] = SimpleNamespace(
            synchronize=synchronize,
            service=_ProbeCoordination.service,
        )
        vars(resources)['client'].last_join_target = SimpleNamespace(
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

            vars(resources)['reader'] = SimpleNamespace(
                catch_up=eager_catch_up,
            )
        else:

            def strict_step(**options: object) -> object:
                if stage == 'synchronization':
                    cast('Callable[[], object]', options['source_progress'])()
                else:
                    block()
                return SimpleNamespace(status='caught-up', join_ready=True)

            vars(resources)['reader'] = SimpleNamespace(
                strict_step=strict_step,
            )
        run_catch_up = cast(
            'Callable[[object, object, int], None]', vars(type(component))['_catch_up']
        )

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_catch_up, component, resources, 45)
            try:
                assert entered.wait()
                snapshot = cache.snapshot()
            finally:
                release.set()
            future.result()

    health = snapshot.watchdogs[0]
    assert health.phase == ('internal' if stage == 'synchronization' else 'idle')
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
            assert release.wait()

    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        watchdog = component._watchdog
        token = watchdog.sample().registration_token
        now = [100.0]
        monkeypatch.setattr(watchdog, '_clock', lambda: now[0])
        watchdog.transition('idle', deadline_at=101.0)
        assert isinstance(component, _LaneComponent)
        component.request_stop(shutdown_deadline_at=110.0)
        _publish_group_health(component, resources, _lifecycle)
        if cleanup == 'partial':
            attempt_type = worker_module._PartialLaneAttempt
            attempt = attempt_type(channel=cast('GroupChannel', _BlockingClose()))

            def operation() -> None:
                close_partial = cast(
                    'Callable[[object, object], None]',
                    vars(type(component))['_close_partial_attempt'],
                )
                close_partial(component, attempt)

        else:
            component._stop.set()
            vars(resources)['channel'] = _BlockingClose()
            vars(resources)['client'] = SimpleNamespace(close=lambda: None)
            vars(resources)['lane'] = SimpleNamespace(close=lambda: None)
            vars(resources)['reader'] = SimpleNamespace(
                __enter__=lambda: None,
                close=lambda: None,
            )
            vars(resources)['state'] = SimpleNamespace(close=lambda: None)

            def operation() -> None:
                drive_lane = cast(
                    'Callable[[object, object], None]',
                    vars(type(component))['_drive_lane'],
                )
                drive_lane(component, resources)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(operation)
            try:
                assert entered.wait()
                snapshot = cache.snapshot()
                if cleanup == 'partial':
                    assert watchdog.sample().registration_token == token
            finally:
                release.set()
            future.result()
        now[0] = 102.0
        completed = cache.snapshot()
        assert completed.watchdogs[0].phase == 'shutdown'
        assert completed.watchdogs[0].deadline_at == 110.0
        assert completed.groups[0].deadline_at == 110.0
        assert 101.0 < now[0] < 110.0

    assert snapshot.watchdogs[0].phase == 'shutdown'
    assert snapshot.watchdogs[0].deadline_at == 110.0
    assert snapshot.groups[0].phase == 'shutdown'


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
        assert cache.snapshot().groups[0].phase == 'idle'

        def build_client(_context: object) -> NoReturn:
            entered.set()
            assert release.wait()
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
                assert entered.wait()
                snapshot = cache.snapshot()
                assert snapshot.watchdogs[0].phase == 'startup'
                assert snapshot.watchdogs[0].deadline_at is not None
                assert snapshot.groups[0].phase == 'startup'
                assert watchdog.sample().registration_token == token
            finally:
                release.set()
                future.result()


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
            assert release.wait()
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
        context = replace(context, assignment=replace(context.assignment, start_mode='initialize'))
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
                assert entered.wait()
                snapshot = cache.snapshot()
            finally:
                release.set()
            future.result()

    assert snapshot.watchdogs[0].phase == 'startup'
    assert snapshot.watchdogs[0].deadline_at is not None


def test_blocked_lane_pause_publishes_idle_with_wake_tolerance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 100.0)
    with _open_group_health_state(tmp_path / 'group.sqlite3') as state:
        component, cache, resources, _lifecycle = _group_health_component(
            tmp_path,
            state,
            health_interval_secs=0.01,
        )
        watchdog = component._watchdog
        watchdog.transition('internal', deadline_at=time.monotonic() + 10.0)
        wake = component._wake
        entered = threading.Event()
        release = threading.Event()

        def wait(_timeout: float) -> bool:
            entered.set()
            return release.wait()

        monkeypatch.setattr(wake, 'wait', wait)
        pause = type(component)._pause
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                pause,
                component,
                cast('_LaneResources', resources),
                seconds=1.0,
            )
            future.add_done_callback(lambda _completed: entered.set())
            assert entered.wait()
            if future.done():
                future.result()
            snapshot = cache.snapshot()
            release.set()
            future.result()

    health = snapshot.watchdogs[0]
    assert health.phase == 'idle'
    assert health.deadline_at is not None
    wait_budget = 1.0 + worker_module.watchdog_grace_secs(0.01)
    assert health.deadline_at == 100.0 + wait_budget


def test_blocked_lane_reader_entry_publishes_startup(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()

    class _BlockingReader:
        def __enter__(self) -> Self:
            entered.set()
            assert release.wait()
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
            channel=SimpleNamespace(close=lambda: None),
            client=SimpleNamespace(close=lambda: None),
            lane=SimpleNamespace(close=lambda: None),
            reader=_BlockingReader(),
            state=SimpleNamespace(close=lambda: None),
        )
        drive_lane = cast('Callable[[object, object], None]', vars(type(component))['_drive_lane'])
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(drive_lane, component, resources)
            try:
                assert entered.wait()
                snapshot = cache.snapshot()
            finally:
                release.set()
            future.result()

    assert snapshot.watchdogs[0].phase == 'startup'
    assert snapshot.watchdogs[0].deadline_at is not None


def test_bootstrap_waits_for_the_stamp_with_one_second_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _open_coordination_source(tmp_path / 'pending-stamp.db', stamped=False) as state:
        state.initialize(41, incarnation='incarnation')
        surface = _initializing_source_surface(state)
        surface._bootstrap_retry_secs = 1.0
        restored = Mock()
        monkeypatch.setattr(surface, '_restore_resumed_group', restored)
        result = surface.observe_group(
            GroupBootstrapRequest(
                source='events',
                group='local',
                group_generation=7,
                start_mode='resume',
            )
        )
        assert isinstance(result, GroupBootstrapPending)
        assert result.retry_after_secs == 1.0
        restored.assert_not_called()


def test_build_worker_specs_packs_every_group_exactly_once() -> None:
    site = _site(
        (
            group_config('default-a', worker='default'),
            group_config('default-b', worker='default', source='audit'),
            group_config('named-a', worker='pool'),
            group_config('named-b', worker='pool', source='audit'),
            group_config('dedicated-a', worker='dedicated'),
            group_config('dedicated-b', worker='dedicated'),
        )
    )

    specs = build_worker_specs(site)

    assert tuple(spec.kind for spec in specs) == (
        _WorkerKind.SITE,
        _WorkerKind.NAMED,
        _WorkerKind.DEDICATED,
        _WorkerKind.DEDICATED,
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
    blocked = group_config('blocked', worker='default', ordering='eager')
    sibling = group_config('sibling', worker='default', ordering='eager')
    site = replace(
        runtime_site(blocked),
        groups=MappingProxyType({blocked.name: blocked, sibling.name: sibling}),
    )
    config = runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    first = local_assignment(spec, blocked.name, 'initialize')
    second = local_assignment(spec, sibling.name, 'initialize')
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
        send_control(control, request)
        response = receive_type(control, CommandResult)
        assert response.request_id == request.request_id
        return response

    monkeypatch.setattr(worker_module, 'load_plugins', load_plugins)
    with running_concrete_runtime(
        monkeypatch, config, spec, assignment, _restart_provider(provider_state)
    ) as (runtime, control):
        try:
            receive_typed_health(
                control,
                lambda item: (
                    len(item.groups) == 2
                    and all(group.join_phase == 'attached' for group in item.groups)
                ),
            )
            provider_state.publish(42)
            assert entered.wait()
            with monkeypatch.context() as admission:
                component = runtime._components['lane:blocked']
                assert isinstance(component, _LaneComponent)
                _expire_lane_admission(admission, component)
                response = park(control, blocked.name)
            assert not response.ok
            assert response.result == {'code': 'group-busy'}
            assert runtime.fatal_reason is None
            while True:
                sibling_response = park(control, sibling.name)
                if sibling_response.ok:
                    break
                assert sibling_response.result == {'code': 'group-busy'}, sibling_response
                assert runtime.fatal_reason is None
            assert runtime.fatal_reason is None
            assert runtime._threads['lane:blocked'].is_alive()
            assert runtime._threads['lane:sibling'].is_alive()
        finally:
            release.set()


def test_callback_gate_records_nonrunning_observation_before_next_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 100.0)
    component_object, resources_object = lifecycle_probe(
        'sync', durable_status='parked', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(channel=resources.channel)
    lifecycle.was_running = True
    lifecycle.next_coordination_at = time.monotonic() + 60.0
    calls: list[tuple[bool, bool]] = []
    monkeypatch.setattr(
        resources.coordination,
        'service',
        lambda *, scan_obligations=True, force=False: calls.append((scan_obligations, force)),
    )
    assert component._callback_gate_snapshot(resources, lifecycle) is None
    vars(resources.state)['status'] = 'running'
    component._service_due(resources, lifecycle)
    assert calls == [(True, True)]


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.usefixtures('_production_coordination_deadlines')
def test_catch_up_outlasting_internal_budget_is_not_overdue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
) -> None:
    _assert_catchup_outlasts_internal_budget(tmp_path, monkeypatch, ordering=ordering)


def test_catchup_reader_slots_are_bounded_without_losing_waiters() -> None:
    spec = build_worker_specs(_site((group_config('default', worker='default'),)))[0]
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
    )
    entered: list[str] = []
    first_holds = threading.Event()
    release_first = threading.Event()

    def reader(name: str) -> None:
        with _held_limiter_slot(runtime._catchup_limiter):
            entered.append(name)
            if name == 'first':
                first_holds.set()
                release_first.wait()

    first = threading.Thread(target=reader, args=('first',))
    second = threading.Thread(target=reader, args=('second',))
    second_started = False
    first.start()
    try:
        assert first_holds.wait()
        assert not runtime._catchup_limiter.acquire(timeout=0.0)
        second.start()
        second_started = True
        release_first.set()
        first.join()
        second.join()
    finally:
        release_first.set()
        first.join()
        if second_started:
            second.join()
    assert not first.is_alive()
    assert not second.is_alive()
    assert entered == ['first', 'second']


def test_checkpoint_database_error_fails_worker_through_component_failure_path(
    tmp_path: Path,
) -> None:
    spec = build_worker_specs(_site(()))[0]
    components = {name: _Component(name, []) for name in spec.sources}
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='checkpoint-failure',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=0.02,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
    )

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            run_checkpoint=lambda: (_ for _ in ()).throw(DatabaseError('checkpoint corruption')),
            logger=logger,
            log_extra=log_extra,
        )

    runtime._checkpoint_host = CheckpointHost(
        logger=logging.getLogger('sg_event_daemon.source'),
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
        runtime._checkpoint_host.register(path, 'events', schema, log_extra={})
        assert _wait_for_runtime_stop(runtime) == ()
        reason = runtime.fatal_reason
        assert reason is not None
        assert reason.startswith('checkpoint:events thread failed: DatabaseError:')
        assert 'checkpoint corruption' in reason
        runtime._component_failed('later failure')
        assert runtime.fatal_reason == reason
    finally:
        runtime.request_stop('test')
        runtime._shutdown()


def test_compact_health_preflight_accounts_for_pending_group_records(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, _snapshot = pending_group_runtime(tmp_path)
    encoded: list[bytes] = []

    def capture(update: HealthUpdate) -> bytes:
        frame = encode_control_message(update)
        encoded.append(frame)
        return frame

    monkeypatch.setattr(worker_module, 'encode_control_message', capture)
    parent_socket, child_socket = socket.socketpair()
    with parent_socket, child_socket:
        control = WorkerControl(child_socket, max_frame_bytes=131_072)
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
    group = group_config('local', worker='default', ordering='strict')
    site = runtime_site(group)
    config = runtime_config(tmp_path, site)
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
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        runtime_provider(),
    ) as (runtime, control):
        actual: HealthUpdate | None = None
        while True:
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
    _assert_maximum_watchdog_health(maximum)
    assert maximum.sources[0].frontier == maximum_int
    assert maximum.groups[0].cursor == maximum_int
    assert maximum.groups[0].replay_required_from == maximum_int
    assert maximum.groups[0].history_truncated_floor == maximum_int
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
            watchdog_failure=escaped_diagnostic,
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

    def _construct(group_count: int, *, max_frame_bytes: int = 524_288) -> _WorkerRuntime:
        groups = tuple(
            group_config(f'group-{index:02}', worker='default') for index in range(group_count)
        )
        site = replace(
            runtime_site(groups[0]),
            groups=MappingProxyType({group.name: group for group in groups}),
        )
        config = lift_config(
            Config(
                daemon=_DaemonConfig(
                    state_dir=tmp_path,
                    sg_api_provider='shotgun-api3',
                    max_ipc_frame_bytes=max_frame_bytes,
                ),
                sites=MappingProxyType({site.name: site}),
            )
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
        control = WorkerControl(child_socket, max_frame_bytes=131_072)
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

    assert isinstance(_construct(2), _WorkerRuntime)
    assert isinstance(_construct(8, max_frame_bytes=8_388_608), _WorkerRuntime)


def test_compact_health_source_only_preflight_uses_longest_operator_command(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = replace(
        runtime_site(group_config('local', worker='default')),
        groups=MappingProxyType({}),
    )
    config = runtime_config(tmp_path, site)
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
    control = WorkerControl(child_socket, max_frame_bytes=131_072)
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
    cache.register_watchdog(
        key=(sample.kind, sample.identity, sample.worker_instance, sample.registration_token),
        supplier=watchdog.sample,
    )
    vars(component).update(_health_cache=cache, _watchdog=watchdog)

    def run_body(_component: object) -> None:
        if failure:
            raise RuntimeError('component failed')

    monkeypatch.setattr(component_type, '_run_body', run_body)
    run_component = cast('Callable[[object], None]', vars(component_type)['run'])
    assert [(item.kind, item.source, item.group) for item in cache.snapshot().watchdogs] == [
        ('source', 'events', None)
        if component_name == '_SourceComponent'
        else ('lane', 'events', 'local')
    ]

    if failure:
        with pytest.raises(RuntimeError, match='component failed'):
            run_component(component)
    else:
        run_component(component)

    assert len(cache.snapshot().watchdogs) == 1
    close_component = cast('Callable[[object], None]', vars(component_type)['close'])
    close_component(component)
    close_component(component)
    assert cache.snapshot().watchdogs == ()


@pytest.mark.usefixtures('_production_coordination_deadlines')
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
def test_concrete_callback_exit_halts_group_and_preserves_in_flight_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    execution: Literal['sync', 'async'],
    error_type: type[BaseException],
) -> None:
    group = group_config('local', worker='default', execution=execution)
    site = runtime_site(group)
    config = runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    provider_state = _RestartProviderState()
    failure = error_type('callback interrupted')

    def fail(_context: EventContext) -> NoReturn:
        raise failure

    async def async_fail(_context: EventContext) -> NoReturn:
        raise failure

    plugin = health_failure_plugin('callback')
    callback = replace(
        plugin.callbacks[0],
        callback=async_fail if execution == 'async' else fail,
        identity=replace(plugin.callbacks[0].identity, execution=execution),
    )
    plugin = replace(plugin, callbacks=(callback,))

    def load(*_args: object) -> tuple[PluginSpec, ...]:
        return (plugin,)

    monkeypatch.setattr(worker_module, 'load_plugins', load)
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        local_assignment(spec, group.name, 'initialize'),
        _restart_provider(provider_state, async_=execution == 'async'),
    ) as (runtime, control):
        receive_typed_health(
            control,
            lambda item: any(health.join_phase == 'attached' for health in item.groups),
        )
        cache = runtime._health_cache
        assert cache is not None
        assert any(item.group == group.name for item in cache.snapshot().watchdogs)
        provider_state.publish(42)
        update = receive_typed_health(
            control,
            lambda item: any(health.failure_kind == 'callback' for health in item.groups),
        )
        health = update.groups[0]
        assert health.status == 'callback-failed'
        assert health.failure_text is not None
        assert error_type.__name__ in health.failure_text
        assert runtime.fatal_reason is None
        assert runtime._threads['lane:local'].is_alive()

        database = source_database_path(tmp_path)
        with closing(sqlite3.connect(f'{database.as_uri()}?mode=ro', uri=True)) as connection:
            snapshot = connection.execute(
                'SELECT cursor, in_flight_event_id, parked, failure_code, failure_text '
                'FROM group_state'
            ).fetchone()
            audits = connection.execute('SELECT command FROM audit').fetchall()
        assert snapshot is not None
        assert snapshot[:4] == (41, 42, 0, 'callback-failed')
        assert error_type.__name__ in snapshot[4]
        assert ('conditional-park',) not in audits

    assert cache.snapshot().watchdogs == ()
    assert not runtime._resources_open
    assert not _runtime_threads_alive(runtime)


@pytest.mark.parametrize(('synchronous', 'expected'), [('normal', 1), ('full', 2)])
def test_concrete_runtime_database_uses_configured_synchronous_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    synchronous: Literal['normal', 'full'],
    expected: int,
) -> None:
    group = group_config('local', worker='default')
    site = runtime_site(group)
    config = runtime_config(tmp_path, site)
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
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        local_assignment(spec, group.name, 'initialize'),
        runtime_provider(),
    ) as (runtime, control):
        receive_typed_health(control, lambda item: bool(item.sources and item.groups))
        _wait_for_group_database(runtime, source_database_path(tmp_path), group.name)
        assert observed == {'source': {(expected, 2)}, 'group': {(expected, 2)}}


def test_concrete_runtime_health_redacts_source_outage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = _site(())
    config = runtime_config(tmp_path, site)
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
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        _outage_runtime_provider(),
    ) as (runtime, control):
        update = receive_typed_health(
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
    group = group_config('local', worker='default', ordering='eager')
    site = runtime_site(group)
    config = runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    provider_state = _RestartProviderState()
    monkeypatch.setattr(
        worker_module,
        'load_plugins',
        _health_failure_plugin_loader('callback'),
    )
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        local_assignment(spec, group.name, 'initialize'),
        _restart_provider(provider_state),
    ) as (runtime, control):
        receive_typed_health(
            control,
            lambda item: any(
                group_health.join_phase == 'attached' for group_health in item.groups
            ),
        )
        provider_state.publish(42)
        health = receive_typed_health(
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
    group = group_config('local', worker='default', ordering='eager')
    site = runtime_site(group)
    prepare_runtime_state(
        tmp_path,
        site,
        group,
        'resume',
        park_reason='token=operator-secret maintenance',
    )
    config = runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        local_assignment(spec, group.name, 'resume'),
        runtime_provider(),
    ) as (runtime, control):
        health = receive_typed_health(
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
    group = group_config('local', worker='default', ordering='eager')
    site = runtime_site(group)
    config = runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    monkeypatch.setattr(
        worker_module,
        'load_plugins',
        _health_failure_plugin_loader('setup'),
    )
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        local_assignment(spec, group.name, 'initialize'),
        runtime_provider(),
    ) as (runtime, control):
        health = receive_typed_health(
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
    group = group_config('local', worker='default', ordering='eager')
    site = runtime_site(group)
    config = runtime_config(tmp_path, site)
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
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        local_assignment(spec, group.name, 'initialize'),
        _restart_provider(_RestartProviderState()),
    ) as (runtime, control):
        # Bootstrap can wait one configured retry for the source generation stamp.
        update = receive_typed_health(
            control,
            lambda item: (
                bool(item.sources)
                and any(health.join_phase == 'attached' for health in item.groups)
            ),
        )
        _wait_for_group_database(
            runtime,
            source_database_path(tmp_path),
            group.name,
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
    group = group_config('local', worker='default', ordering=ordering)
    site = runtime_site(group)
    spec = build_worker_specs(site)[0]
    calls: list[str] = []
    retention_health = StrictBuffer.retention_health

    def snapshot(buffer: StrictBuffer) -> tuple[tuple[str, int, int, int], ...]:
        calls.append(threading.current_thread().name)
        return retention_health(buffer)

    monkeypatch.setattr(StrictBuffer, 'retention_health', snapshot)
    with running_concrete_runtime(
        monkeypatch,
        runtime_config(tmp_path, site),
        spec,
        local_assignment(spec, group.name, 'initialize'),
        _restart_provider(_RestartProviderState()),
    ) as (_runtime, control):
        update = receive_typed_health(
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
        channel=probes['channel'],
        client=probes['client'],
        lane=probes['lane'],
        reader=probes['reader'],
        state=probes['state'],
    )
    cooperative_stop = cast(
        'Callable[[Literal["local-call"]], RuntimeError]',
        vars(worker_module)['_CooperativeComponentStopError'],
    )
    drive = cast('_LaneLifecycleDriver', vars(component_type)['_drive_lane'])

    def service_calls(_resources: object) -> NoReturn:
        entered.set()
        assert stop.wait()
        raise cooperative_stop('local-call')

    vars(component)['_service_calls'] = service_calls
    runtime_component = InterruptibleComponent(
        lambda: drive(component, resources),
        stop.set,
    )

    fatal_reason, forced = run_cooperative_stop_component(
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
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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


def test_detached_eager_catch_up_completes_closed_gap_before_joining(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component = _unopened_group_component(_LaneComponent)
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    provider_state = _RestartProviderState()
    for event_id in (42, *range(45, 101)):
        provider_state.publish(event_id)
    provider = _restart_provider(provider_state)
    group = group_config('local', worker='default')
    site = runtime_site(group)
    invoked: list[int] = []
    acquisitions: list[float] = []
    pending_during_ordinary: list[tuple[PendingRange, ...]] = []
    acquire = component._limiter.acquire

    def acquire_once(*, timeout: float) -> bool:
        assert not acquisitions, 'catch-up attempted a second reader permit'
        acquisitions.append(timeout)
        return acquire(timeout=timeout)

    monkeypatch.setattr(component._limiter, 'acquire', acquire_once)
    channel = GroupChannel(group='local', ordering='eager', max_events=10, max_bytes=262_144)
    lifecycle = worker_module._LaneLifecycle(channel=channel)
    component._lifecycle = lifecycle

    def callback(context: EventContext) -> None:
        event_id = cast('int', context.event['id'])
        invoked.append(event_id)
        clock[0] += 0.25
        assert not lifecycle.joined
        if event_id >= 45:
            pending_during_ordinary.append(state.pending_ranges())

    plugin = health_failure_plugin('callback')
    plugin = replace(plugin, callbacks=(replace(plugin.callbacks[0], callback=callback),))
    with (
        _open_group_health_state(tmp_path / 'group.db') as state,
        CatchupReader(
            provider=provider,
            site_url=site.server,
            credential=site.credentials['source'],
            batch_size=2,
            fetch_interval_secs=0.5,
            waiter=SourceHealthWaiter(),
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
                watchdog=component._watchdog,
            )
        ) as lane,
    ):
        assert lane.setup() is None
        _bind_worker_range(state, 'closed-gap', 42)
        with state._writes.transaction(state._database.connection):
            insert_source_late_ids(state._database.connection, 'closed-gap', (42,))
            state._database.connection.execute('UPDATE source_meta SET frontier=100')
        state.advance_irrelevant(44, expected_revision=state.snapshot().revision)
        coordination = LaneRefresh(
            joined=lambda: lifecycle.joined,
            fetch_interval_secs=0.5,
            late_event_window_secs=300,
            log_extra={},
            lane=lane,
            slot=component._reader_slot,
            reader=reader,
            state=state,
        )
        target = SimpleNamespace(
            identity=SimpleNamespace(boundary=100), stream_incarnation='stream-incarnation-1'
        )
        resources = cast(
            '_LaneResources',
            SimpleNamespace(
                state=state,
                lane=lane,
                reader=reader,
                coordination=coordination,
                client=SimpleNamespace(last_join_target=target),
                context=SimpleNamespace(
                    group=group, source_config=SimpleNamespace(fetch_interval_secs=0.5)
                ),
            ),
        )
        component._catch_up(resources, 100)
        assert all(not pending for pending in pending_during_ordinary)
        assert state.snapshot().cursor == 100
        assert state.pending_ranges() == ()
        assert not lifecycle.joined
        assert invoked == [42, *range(45, 101)]
        assert len(acquisitions) == 1


def test_detached_remote_tick_refreshes_durable_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 100.0)
    component_object, resources_object = lifecycle_probe(
        'sync', durable_status='running', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(channel=resources.channel)
    client = EventClient(
        path=tmp_path / 'absent.sock',
        source='events',
        group='local',
        spawn_instance='worker-1',
        capability='a' * 64,
        max_frame_bytes=262_144,
        queue_max_entries=10,
        queue_max_bytes=262_144,
        coordination_deadline_secs=stall_deadline('coordination', 10.0),
        stop=threading.Event(),
        wait_phase=nullcontext,
        wait_slice_secs=0.1,
    )
    vars(resources)['client'] = client

    calls: list[str] = []

    def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
        del force
        del scan_obligations
        calls.append('refresh')

    monkeypatch.setattr(resources.coordination, 'service', coordinate)
    component._coordinate(resources, lifecycle, scan=False, force=False)
    assert calls == ['refresh']
    assert lifecycle.next_coordination_at > time.monotonic()


@pytest.mark.usefixtures('_production_coordination_deadlines', '_production_phase_deadlines')
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


def test_durable_range_refresh_converges_from_source_state(
    tmp_path: Path,
) -> None:
    source_path = source_database_path(tmp_path)
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
        group_state.advance_irrelevant(46, expected_revision=group_state.snapshot().revision)
        reader = _CoordinationReader()
        with _open_coordination_source(source_path) as source_state:
            coordinator = LaneRefresh(
                joined=lambda: False,
                fetch_interval_secs=1,
                late_event_window_secs=300,
                log_extra={},
                lane=cast('GroupLane', _CoordinationLane(group_state)),
                slot=partial(_held_limiter_slot, _CatchupLimiter(1)),
                reader=cast('CatchupReader', reader),
                state=group_state,
            )
            coordinator.service()
            retained = source_state.snapshot().ranges
            assert len(retained) == 1
            source_state._close_range(retained[0].obligation_id, successful_clock=2.0)
            for _ in range(3):
                coordinator.service()

        assert group_state.snapshot().obligation_count == 0
        assert reader.ranges == []

    with _open_coordination_source(source_path) as source_state:
        assert source_state.snapshot().ranges == ()
        assert source_state.snapshot().pending_obligations == 0


def test_eager_catch_up_closeout_throttles_and_stops_after_halt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component = _unopened_group_component(_LaneComponent)
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    calls: list[float] = []
    with _open_group_health_state(tmp_path / 'group.db') as state:

        def service(*, scan_obligations: bool, slot_held: bool) -> GroupSnapshot:
            assert scan_obligations
            assert slot_held
            calls.append(clock[0])
            if clock[0] == 102:
                return state.park(
                    reason='maintenance',
                    identity=AuditIdentity.current(),
                    wall_clock_time=1.0,
                    expected_revision=state.snapshot().revision,
                )
            return state.snapshot()

        resources = cast(
            '_LaneResources',
            SimpleNamespace(
                state=state,
                coordination=SimpleNamespace(service=service),
                context=SimpleNamespace(source_config=SimpleNamespace(fetch_interval_secs=1.0)),
            ),
        )
        for at in (100, 100, 100.5, 101, 101.5):
            clock[0] = at
            component._service_closeout_in_catch_up(resources)
        assert calls == [100, 101]
        clock[0] = 102
        for _ in range(2):
            with pytest.raises(worker_module._GroupHaltedCatchupError):
                component._service_closeout_in_catch_up(resources)
        assert calls == [100, 101, 102]
        assert component._pending_health_snapshot == state.snapshot()


def test_eager_catchup_callback_failure_remains_group_local(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    groups = (
        group_config('halted', worker='default'),
        group_config('healthy', worker='default'),
    )
    site = replace(
        runtime_site(groups[0]),
        groups=MappingProxyType({group.name: group for group in groups}),
    )
    _prepare_retained_history_runtime_state(tmp_path, site, groups)
    config = runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = replace(
        local_assignment(spec, 'halted', 'resume'),
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
    provider_state.publish(42)
    provider_state.publish(60)
    invoked: list[int] = []
    failed_plugin = health_failure_plugin('callback')

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
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        _restart_provider(provider_state),
    ) as (runtime, control):
        update: HealthUpdate | None = None
        while True:
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
        provider_state.publish(61)
        receive_typed_health(
            control,
            lambda item: any(
                group.group == 'healthy' and group.cursor == 61 for group in item.groups
            ),
        )
        assert invoked == [61]
        assert runtime.fatal_reason is None
        assert runtime._threads['lane:halted'].is_alive()
        assert runtime._threads['lane:healthy'].is_alive()

    with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
        failed = connection.execute(
            'SELECT cursor, in_flight_event_id, failure_code FROM group_state'
        ).fetchone()
    assert failed == (41, 42, 'callback-failed')


def test_eager_post_close_scan_waits_until_cursor_reaches_range_high() -> None:
    # Prevents N2: a lagging eager scan moving the cursor past queued ordinary events.
    state = _RetentionCoordinationState(cursor=59)
    reader = _RetentionCoordinationReader((1, 1), (Event(runtime_event(55)),))
    lane = _RetentionCoordinationLane(state)
    coordinator = LaneRefresh(
        joined=lambda: False,
        fetch_interval_secs=1,
        late_event_window_secs=300,
        log_extra={},
        lane=cast('GroupLane', lane),
        slot=partial(_held_limiter_slot, _CatchupLimiter(1)),
        reader=cast('CatchupReader', reader),
        state=cast('GroupState', state),
    )
    scan = cast(
        'Callable[[object, SourceProgress], None]',
        vars(type(coordinator))['_scan_obligations'],
    )
    coordinator.synchronize()

    scan(coordinator, _synchronization(60))

    assert reader.ranges == []
    assert not lane.resolved

    state.cursor = 60
    scan(coordinator, _synchronization(60))

    assert reader.ranges == [(51, 60)]
    assert lane.delivered == [55]
    assert not lane.resolved


def test_group_health_cached_late_ids_do_not_control_admission_or_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    path = tmp_path / 'health-admission.db'
    with _open_group_health_state(path, max_late_ids_per_group=1) as state:
        connection = state._database.connection
        with state._writes.transaction(connection):
            insert_range(connection, 'late-range', 50, 51, state='closed')
            bind_group(connection, 'late-range', 'local', 7, floor=41, eligibility_point=3.0)
            insert_source_late_ids(connection, 'late-range', (50, 51))
            connection.execute('UPDATE source_meta SET frontier = 51')
        component, cache, resources, lifecycle = _group_health_component(
            tmp_path, state, health_interval_secs=2.0
        )
        reads: list[float] = []
        real_projection = state.unresolved_late_id_projection

        def counted_projection(*, limit: int) -> tuple[int, tuple[int, ...]]:
            reads.append(clock.now)
            return real_projection(limit=limit)

        monkeypatch.setattr(state, 'unresolved_late_id_projection', counted_projection)
        component._publish_startup_group_health(resources.context, state)
        assert reads == [0.0]
        _publish_group_health(component, resources, lifecycle)
        assert reads == [0.0, 0.0]
        assert cache.snapshot().groups[0].outstanding_late_id_count == 2

        admitted = commit_step(
            state, ReserveLateStep('late-range', 50), expected_revision=state.snapshot().revision
        )
        assert (
            admitted.status,
            admitted.obligation_count,
            admitted.late_id_count,
            admitted.conditions,
        ) == ('running', 1, 1, ())
        component._pending_health_snapshot = admitted
        clock.now = 0.2
        _publish_group_health(component, resources, lifecycle)
        assert reads == [0.0, 0.0]
        active_health = cache.snapshot().groups[0]
        assert active_health.outstanding_late_id_count == 2
        assert status_from_health(
            sources=(), groups=(active_health,), workers=(), now=0.2, stale_after_secs=10.0
        ).readiness

        refused = commit_step(
            state, ReserveLateStep('late-range', 51), expected_revision=admitted.revision
        )
        assert (refused.late_id_count, 'late-capacity-exhausted' in refused.conditions) == (
            1,
            True,
        )
        component._pending_health_snapshot = refused
        clock.now = 0.5
        _publish_group_health(component, resources, lifecycle)
        blocked_health = cache.snapshot().groups[0]
        assert (
            blocked_health.outstanding_late_id_count,
            'late-capacity-exhausted' in blocked_health.conditions,
        ) == (2, True)
        assert not status_from_health(
            sources=(), groups=(blocked_health,), workers=(), now=0.5, stale_after_secs=10.0
        ).readiness

        completed = commit_step(
            state, CompleteLateStep('late-range', 50), expected_revision=refused.revision
        )
        assert completed.late_id_count == 1
        component._pending_health_snapshot = completed

        clock.now = 2.0
        _publish_group_health(component, resources, lifecycle)
        assert cache.snapshot().groups[0].outstanding_late_ids == (51,)
        assert reads == [0.0, 0.0, 2.0]

        component._health_refresh = worker_module._GroupHealthRefresh()
        clock.now = 2.1
        _publish_group_health(component, resources, lifecycle)
        assert reads == [0.0, 0.0, 2.0, 2.1]


def test_group_health_cached_late_ids_do_not_control_capacity_collapse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / 'health-collapse.db'
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_group_health_state(path, max_late_ids_per_group=1) as state:
        connection = state._database.connection
        with state._writes.transaction(connection):
            insert_range(connection, 'late-range', 50, 51, state='closed')
            bind_group(connection, 'late-range', 'local', 7, floor=41, eligibility_point=3.0)
            insert_source_late_ids(connection, 'late-range', (50, 51))
            connection.execute('UPDATE source_meta SET frontier = 51')
        component, cache, resources, lifecycle = _group_health_component(
            tmp_path, state, health_interval_secs=2.0
        )
        reads: list[float] = []
        real_projection = state.unresolved_late_id_projection

        def counted_projection(*, limit: int) -> tuple[int, tuple[int, ...]]:
            reads.append(clock.now)
            return real_projection(limit=limit)

        monkeypatch.setattr(state, 'unresolved_late_id_projection', counted_projection)
        _publish_group_health(component, resources, lifecycle)
        assert cache.snapshot().groups[0].outstanding_late_id_count == 2
        first = commit_step(
            state, ReserveLateStep('late-range', 50), expected_revision=state.snapshot().revision
        )
        refused = commit_step(
            state, ReserveLateStep('late-range', 51), expected_revision=first.revision
        )
        assert refused.late_id_count == 1
        assert 'late-capacity-exhausted' in refused.conditions

        with SourceState.open(
            path,
            writes=write_context_for(path),
            site='main',
            source='events',
            stream_fingerprint=refused.stream_fingerprint,
            source_generation=refused.source_generation,
            max_source_late_ids=2,
        ) as source:
            result = source.commit_observations((53,), successful_clock=1.0)
            assert result.committed_count == 1
            collapsed = source.group_snapshot('local')
        assert collapsed.replay_required_from == 50
        assert collapsed.obligation_count == 0
        assert collapsed.late_id_count == 0
        component._pending_health_snapshot = collapsed
        clock.now = 1.0
        _publish_group_health(component, resources, lifecycle)
        collapsed_health = cache.snapshot().groups[0]
        assert collapsed_health.outstanding_late_id_count == 2
        assert 'replay-required' in collapsed_health.conditions
        assert not status_from_health(
            sources=(), groups=(collapsed_health,), workers=(), now=1.0, stale_after_secs=10.0
        ).readiness
        assert reads == [0.0]

        clock.now = 2.0
        _publish_group_health(component, resources, lifecycle)
        assert cache.snapshot().groups[0].outstanding_late_id_count == 0
        assert reads == [0.0, 2.0]


def test_group_health_delayed_pass_keeps_interval_grid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_group_health_state(tmp_path / 'health-delayed.db') as state:
        component, _cache, resources, lifecycle = _group_health_component(
            tmp_path, state, health_interval_secs=2.0
        )
        reads: list[tuple[float, float | None]] = []
        real_projection = state.unresolved_late_id_projection

        def counted_projection(*, limit: int) -> tuple[int, tuple[int, ...]]:
            reads.append((clock.now, component._health_refresh.deadline_at))
            return real_projection(limit=limit)

        monkeypatch.setattr(state, 'unresolved_late_id_projection', counted_projection)
        for moment in (0.0, 3.9, 4.0, 9.9, 10.0):
            clock.now = moment
            _publish_group_health(component, resources, lifecycle)
        assert reads == [(0.0, None), (3.9, 2.0), (4.0, 4.0), (9.9, 6.0), (10.0, 10.0)]


def test_group_health_late_ids_share_projection_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_group_health_state(tmp_path / 'health-grid.db') as state:
        connection = state._database.connection
        queries: list[str] = []
        connection.set_trace_callback(queries.append)
        with state._writes.transaction(connection):
            insert_range(connection, 'late-range', 50, 160, state='closed')
            bind_group(connection, 'late-range', 'local', 7, floor=41, eligibility_point=3.0)
        component, cache, resources, lifecycle = _group_health_component(
            tmp_path, state, health_interval_secs=2.0
        )
        reads: list[tuple[float, int, tuple[int, ...]]] = []
        real_projection = state.unresolved_late_id_projection

        def counted_projection(*, limit: int) -> tuple[int, tuple[int, ...]]:
            assert limit == 100
            result = real_projection(limit=limit)
            reads.append((clock.now, *result))
            return result

        monkeypatch.setattr(state, 'unresolved_late_id_projection', counted_projection)
        for index, moment in enumerate((0.0, 0.2, 0.5, 1.9, 2.0)):
            clock.now = moment
            with state._writes.transaction(connection):
                insert_source_late_ids(connection, 'late-range', range(50 + index, 151 + index))
            snapshot = state.advance_irrelevant(
                42 + index, expected_revision=state.snapshot().revision
            )
            monkeypatch.setattr(
                resources.lane,
                'health',
                lambda snapshot=snapshot: replace(
                    cast('LaneHealth', _GroupHealthLane.health()),
                    cursor=snapshot.cursor,
                    status=snapshot.status,
                ),
            )
            lifecycle.channel.status.queued_count = index
            lifecycle.channel.status.queued_bytes = index * 10
            component._pending_health_snapshot = snapshot
            _publish_group_health(component, resources, lifecycle)
            health = cache.snapshot().groups[0]
            assert health.cursor == snapshot.cursor
            assert health.status == snapshot.status
            assert health.conditions == snapshot.conditions
            assert health.queue_event_count == index
            assert health.queue_byte_count == index * 10
            assert health.outstanding_late_obligation_count == snapshot.obligation_count
            assert health.catchup_frontier == snapshot.latest_join_boundary
            expected_count = 101 if moment < 2.0 else 105
            assert health.outstanding_late_id_count == expected_count
            assert health.outstanding_late_ids == tuple(range(50, 150))
            assert health.outstanding_late_ids_truncated
        assert [moment for moment, _count, _ids in reads] == [0.0, 2.0]
        assert reads == [
            (0.0, 101, tuple(range(50, 150))),
            (2.0, 105, tuple(range(50, 150))),
        ]
        assert sum(query.startswith('SELECT COUNT(*) FROM (') for query in queries) == 2
        assert sum(query.startswith('SELECT DISTINCT sl.event_id') for query in queries) == 2
        connection.set_trace_callback(None)


@pytest.mark.parametrize('dirty', [False, True])
def test_group_health_off_grid_late_ids_refresh_when_projection_is_due(
    dirty: bool,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_group_health_state(tmp_path / 'health-off-grid.db') as state:
        _bind_worker_range(state, 'late-range', 50)
        component, cache, resources, lifecycle = _group_health_component(
            tmp_path, state, health_interval_secs=2.0
        )
        reads: list[float] = []
        real_projection = state.unresolved_late_id_projection

        def counted_projection(*, limit: int) -> tuple[int, tuple[int, ...]]:
            reads.append(clock.now)
            return real_projection(limit=limit)

        monkeypatch.setattr(state, 'unresolved_late_id_projection', counted_projection)
        for moment in (0.0, 2.1, 4.0, 4.2, 5.9, 6.0):
            clock.now = moment
            if moment == 4.0:
                with state._writes.transaction(state._database.connection):
                    insert_source_late_ids(state._database.connection, 'late-range', (50,))
            if dirty and moment in (4.0, 4.2, 5.9):
                component._pending_health_snapshot = state.advance_irrelevant(
                    {4.0: 42, 4.2: 43, 5.9: 44}[moment],
                    expected_revision=state.snapshot().revision,
                )
            if dirty:
                component._health_refresh.dirty = True
            _publish_group_health(component, resources, lifecycle)
            health = cache.snapshot().groups[0]
            assert health.outstanding_late_ids == (() if moment < 4.0 else (50,))
        assert reads == [0.0, 2.1, 4.0, 6.0]


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
        initial = cache.snapshot().groups[0]
        assert calls == {'park_reason': 1, 'projection': 1, 'snapshot': 1}
        assert component._health_refresh.deadline_at == 1.0

        for pass_number in range(1, 100):
            clock.now = pass_number / 100
            _publish_group_health(component, resources, lifecycle)

        assert calls == {'park_reason': 1, 'projection': 1, 'snapshot': 1}
        assert cache.snapshot().groups[0] == initial

        clock.now = 1.0
        _publish_group_health(component, resources, lifecycle)
        assert calls == {'park_reason': 2, 'projection': 2, 'snapshot': 2}
        assert component._health_refresh.deadline_at == 2.0

        clock.now = 4.25
        _publish_group_health(component, resources, lifecycle)
        assert calls == {'park_reason': 3, 'projection': 3, 'snapshot': 3}
        assert component._health_refresh.deadline_at == 5.0


def test_group_mutation_snapshot_from_halted_coordination_supersedes_resume(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 0.0)
    with _open_group_health_state(tmp_path / 'halted-mutation.db') as state:
        initial = state.snapshot()
        state.park(
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
        vars(lifecycle)['joined'] = False
        _publish_group_health(component, resources, lifecycle)
        assert cache.snapshot().groups[0].outstanding_late_obligation_count == 0

        def coordinate(*, scan_obligations: bool = True, force: bool = False) -> GroupSnapshot:
            del force
            assert not scan_obligations
            _bind_worker_range(state, 'range-1', 42)
            return commit_step(
                state, ReserveLateStep('range-1', 42), expected_revision=state.snapshot().revision
            )

        vars(resources)['coordination'] = SimpleNamespace(service=coordinate)
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

        health = cache.snapshot().groups[0]
        assert health.parked
        assert health.outstanding_late_obligation_count == 1


def test_halted_group_logs_one_warning_and_no_rejoin_line(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    component, raw_resources = lifecycle_probe(
        'sync', durable_status='ordering-anomaly', setup_failure=None
    )
    resources = cast('_LaneResources', raw_resources)
    lifecycle = worker_module._LaneLifecycle(channel=resources.channel)
    component._lifecycle = lifecycle
    client = object.__new__(EventClient)
    raw_resources.client = client
    identity = LiveJoinIdentity(
        site='test',
        source='events',
        group='local',
        join_kind='eager',
        source_generation='source-1',
        group_generation=1,
        boundary=0,
        attempt_token='1' * 64,
    )
    client._last_join_target = JoinTarget(identity, stream_incarnation='incarnation')

    def pause(_resources: object) -> None:
        pass

    def reconnect(_resources: object, _channel: object) -> tuple[GroupChannelInterface, bool]:
        return resources.channel, True

    def catch_up(_resources: object, _target: object) -> None:
        assert any('rejoining through catch-up' in message for message in caplog.messages)

    monkeypatch.setattr(component, '_pause', pause)
    monkeypatch.setattr(component, '_reconnect_remote', reconnect)
    monkeypatch.setattr(component, '_reader_slot', nullcontext)
    monkeypatch.setattr(component, '_catch_up_eager', catch_up)
    caplog.set_level(logging.INFO, logger='sg_event_daemon')
    assert component._recover_lane_failure(resources, lifecycle, ConnectionError('transport'))
    component._drive_halted(resources, lifecycle)
    component._drive_halted(resources, lifecycle)
    line = 'group halted: ordering-anomaly; delivery stops until it is cleared'
    assert caplog.messages.count(line) == 1
    assert not any('rejoining through catch-up' in message for message in caplog.messages)
    raw_resources.state.status = 'running'
    assert component._callback_gate_snapshot(resources, lifecycle) is not None
    component._catch_up(resources, 0)
    assert sum('rejoining through catch-up' in message for message in caplog.messages) == 1
    raw_resources.state.status = 'ordering-anomaly'
    component._drive_halted(resources, lifecycle)
    assert caplog.messages.count(line) == 2


@pytest.mark.usefixtures('_production_phase_deadlines')
def test_health_cache_lane_idle_sample_has_intended_wake_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component, context, operations, _control, _state = _recovery_construction_probe(
        monkeypatch, stop_at_construction=False
    )
    _install_recovery_gate_probes(monkeypatch, component, operations)
    monkeypatch.setattr(
        worker_module, 'time', SimpleNamespace(monotonic=lambda: 100.0, time=time.time)
    )
    monkeypatch.setattr(component._watchdog, '_clock', lambda: 100.0)
    component_type = _LaneComponent
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )

    resources = build_resources(component, context)
    sample = component._watchdog.sample()
    cast('_LaneLifecycleDriver', vars(component_type)['_drive_lane'])(component, resources)

    assert sample.phase == 'idle'
    assert sample.heartbeat_at == 100.0
    assert sample.deadline_at == 105.01


def test_hung_detached_strict_synchronization_remains_visible_and_killable(tmp_path: Path) -> None:
    context = multiprocessing.get_context('spawn')
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_run_hung_strict_synchronization_probe, args=(child, tmp_path)
    )
    process.start()
    child.close()
    try:
        sample = parent.recv()
        assert isinstance(sample, WatchdogSample)
        assert sample.phase == 'internal'
        assert sample.deadline_at == 192.0
        assert sample.heartbeat_at == 0.0
        assert process.is_alive()
    finally:
        if process.is_alive():
            process.terminate()
            process.join()
        if process.is_alive():
            process.kill()
            process.join()
        parent.close()
    assert not process.is_alive()
    assert process.exitcode is not None
    assert process.exitcode != 0


def test_initially_absent_remote_endpoint_recovers_without_stopping_packed_sibling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    groups = (
        group_config('healthy', worker='pool'),
        group_config('remote', worker='pool'),
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
    config = runtime_config(tmp_path, site)
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
        provider_loader(_restart_provider(provider_state)),
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
    source_path = source_database_path(tmp_path)
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
    dependent_parent = WorkerControl(dependent_parent_socket, max_frame_bytes=131_072)
    dependent_child = WorkerControl(dependent_child_socket, max_frame_bytes=131_072)
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
    absent_endpoint, grants_ready = threading.Event(), threading.Event()
    original_connect = EventClient.connect

    def connect(client: EventClient) -> None:
        # Production publishes dependent assignments only after grant acknowledgements.
        if client._group != 'remote' or absent_endpoint.is_set():
            assert grants_ready.wait()
        try:
            original_connect(client)
        except ConnectionError:
            absent_endpoint.set()
            raise

    monkeypatch.setattr(EventClient, 'connect', connect)
    source_session: _SourceRuntimeSession | None = None
    dependent_runtime.start()
    drain_stop, drain_thread = start_control_drain(dependent_parent)
    try:
        assert absent_endpoint.wait()
        threads = cast('dict[str, threading.Thread]', vars(dependent_runtime)['_threads'])
        assert dependent_runtime.fatal_reason is None
        assert threads['lane:healthy'].is_alive()
        assert threads['lane:remote'].is_alive()

        source_session = start_source_runtime(
            config,
            site_spec,
            source_assignment,
            grants,
        )
        grants_ready.set()
        assert source_session.endpoint == endpoint
        _assert_initial_endpoint_delivery(
            dependent_runtime,
            provider_state,
            invoked,
        )
    finally:
        grants_ready.set()
        release_failed_source_runtime(
            dependent_runtime,
            (dependent_parent, dependent_child),
            (dependent_parent_socket, dependent_child_socket),
            (drain_stop, drain_thread),
            (() if source_session is None else (source_session.close,)),
        )


def test_join_after_bootstrap_is_not_refused(tmp_path: Path) -> None:
    path = source_database_path(tmp_path)
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


def test_lane_command_service_completes_queued_call() -> None:
    component = _unopened_group_component(_LaneComponent)
    result = object()

    def invoke(_resources: _LaneResources) -> object:
        return result

    call = component._mailbox.submit(invoke)

    component._service_calls(
        cast('_LaneResources', SimpleNamespace(state=object(), client=object()))
    )

    assert call.complete.is_set()
    assert call.result is result
    assert call.error is None
    assert component._mailbox.take() is None


def test_lane_control_saturation_keeps_producer_nonblocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lane_component, context, _operations, _control, _state = _recovery_construction_probe(
        monkeypatch
    )
    component_type = _LaneComponent
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )
    peer_socket, sender_socket = socket.socketpair()
    peer = WorkerControl(peer_socket, max_frame_bytes=131_072)
    sender = WorkerControl(sender_socket, max_frame_bytes=131_072)
    vars(lane_component)['_control'] = WorkerControlOutbox(sender)

    def _action() -> None:
        with pytest.raises(_RecoveryConstructionReachedError):
            build_resources(lane_component, context)
        runtime.request_stop('producer-complete')

    spec = WorkerSpec(
        worker_id='["lane","saturation"]',
        site='main',
        kind=_WorkerKind.NAMED,
        name='pool',
        sources=(),
        groups=('remote',),
    )
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='lane-saturation-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: _ControlSendComponent(_action),
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
    )
    _saturate_worker_control_sender(sender)
    try:
        runtime.start()
        assert _wait_for_runtime_stop(runtime) == ()
        assert runtime.fatal_reason is None
    finally:
        peer.close()
        runtime.request_stop('test-cleanup')
        runtime._shutdown()
        sender.close()


def test_lane_disposition_retry_with_new_pid_after_restart(tmp_path: Path) -> None:
    path = source_database_path(tmp_path)
    spec = build_worker_specs(_site((group_config('local', worker='default'),)))[0]
    component = object.__new__(_LaneComponent)
    vars(component).update(
        _assignment=local_assignment(spec, 'local', 'resume'),
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
            'disposition_id': 'provided-disposition',
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
            wait_phase=nullcontext,
            source_name='events',
            group='local',
            stop=threading.Event(),
        )

        def call_first(invoke: Callable[[GroupState, object], object]) -> object:
            return invoke(group, client)

        vars(component)['_call'] = call_first
        first = component.execute_operator_command(command)
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
        forwarded_identity = reopened.terminal_disposition_identity(8)
        assert forwarded_identity is not None
        assert forwarded_identity[0] == 'provided-disposition'

        def call_reopened(invoke: Callable[[GroupState, object], object]) -> object:
            return invoke(reopened, object())

        vars(component)['_call'] = call_reopened
        retry = replace(
            command,
            arguments={**command.arguments, 'orphan_generation': 8},
            identity=replace(command.identity, pid=789),
        )
        repeated = component.execute_operator_command(retry)
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
                component.execute_operator_command(replace(retry, identity=changed_identity))
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
    component, resources = lifecycle_probe(
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
            assert resources.coordination.serviced.wait()
        finally:
            stop.set()
        future.result()

    assert 'resume' in resources.lane.operations
    assert resources.lane.joined == 0
    assert resources.lane.drains == 0


def test_lane_does_not_repeat_acknowledged_recovery_during_construction_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component, context, operations, control, _state = _recovery_construction_probe(monkeypatch)
    component_type = _LaneComponent
    build_resources = cast(
        'Callable[[object, object], object]',
        vars(component_type)['_build_resources'],
    )

    for _attempt in range(2):
        with pytest.raises(_RecoveryConstructionReachedError):
            build_resources(component, context)

    assert len(control.messages) == 1
    assert operations.count('resolve') == 1
    assert operations.count('plugin-construction') == 2


def test_lane_initial_stop_publishes_original_shutdown_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component, _context, now, cache = _recovery_owner_phase_probe(monkeypatch)
    component.request_stop(shutdown_deadline_at=200.0)
    component._run_body()
    now[0] = 136.0
    assert cache.snapshot().watchdogs[0].phase == 'shutdown'
    assert cache.snapshot().watchdogs[0].deadline_at == 200.0


def test_lane_less_configured_group_is_visible_in_worker_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = pending_group_runtime(tmp_path)
    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        runtime,
        control,
    ):
        update = receive_typed_health(control, lambda item: len(item.groups) == 1)
        health = update.groups[0]
        assert health.group == 'local'
        assert health.parked
        assert health.join_phase == 'parked'
        assert health.status == 'parked'
        assert health.conditions == ('parked', 'replay-required')
        assert health.replay_required_from == snapshot.replay_required_from
        assert health.history_truncated_floor is None
        assert health.cursor == snapshot.cursor == 41
        assert health.audit_count == 3
        assert health.park_reason == 'configured group readd'
        assert all(item.kind != 'lane' for item in update.watchdogs)
        assert 'lane:local' not in update.worker.thread_names
        assert runtime.fatal_reason is None
        with _open_group_health_state(source_database_path(tmp_path)) as state:
            state.mark_history_truncated(50, expected_revision=snapshot.revision)
        refreshed = receive_typed_health(
            control,
            lambda item: bool(item.groups) and 'history-truncated' in item.groups[0].conditions,
        )
        assert refreshed.groups[0].history_truncated_floor == 50
        assert refreshed.groups[0].audit_count == 3
        assert refreshed.groups[0].conditions == ('parked', 'history-truncated', 'replay-required')
        assert all(item.kind != 'lane' for item in refreshed.watchdogs)


def test_lane_never_starts_plugins_catchup_join_or_delivery_before_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = RuntimeError('conditional recovery failed closed')
    component, context, operations, _control, _state = _recovery_construction_probe(
        monkeypatch, recovery_error=failure
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
        'plugin-construction',
        'resume',
        'setup',
    }
    assert gated_operations.isdisjoint(operations)
    assert operations[-1] == 'resolve'
    assert (
        operations.index('open') < operations.index('initial-health') < operations.index('resolve')
    )

    component, context, operations, _control, _state = _recovery_construction_probe(
        monkeypatch, stop_at_construction=False
    )
    _install_recovery_gate_probes(monkeypatch, component, operations)
    resources = build_resources(component, context)
    drive_lane(component, resources)

    assert [item for item in operations if item in {*gated_operations, 'resolve'}] == [
        'resolve',
        'plugins',
        'setup',
        'resume',
        'join',
        'catchup',
        'callbacks',
        'delivery',
    ]


@pytest.mark.usefixtures('_production_coordination_deadlines')
def test_lane_operator_call_completed_at_deadline_returns_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component = _unopened_group_component(_LaneComponent)
    now = 0.0

    class ImmediateMailbox(_OwnerMailbox[object]):
        def submit(self, invoke: Callable[[object], object]) -> _OwnerCall[object]:
            nonlocal now
            call = super().submit(invoke)
            assert self.take() is call
            call.result = invoke(SimpleNamespace(state=object(), client=object()))
            call.complete.set()
            now = 1.0
            return call

    vars(component)['_mailbox'] = ImmediateMailbox('local')
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: now)

    assert component._call(lambda _state, _client: 'completed') == 'completed'
    assert now == 1.0


def test_lane_operator_call_returns_group_busy_while_lane_is_occupied(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    component = _unopened_group_component(_LaneComponent)
    _expire_lane_admission(monkeypatch, component)
    runtime = _WorkerRuntime(
        spec=component._spec,
        health_cache=_ComponentHealthCache(site=component._spec.site, group_sources={}),
        spawn_instance='busy-lane-test',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: component,
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
                assert component._wake.wait()
                response = future.result()
            finally:
                component._stop.set()
        assert not response.ok
        assert response.result == {'code': 'group-busy'}
        component._service_calls(
            cast('_LaneResources', SimpleNamespace(state=state, client=object()))
        )
        assert not state.snapshot().parked
        assert invoked == []


@pytest.mark.usefixtures('_production_coordination_deadlines')
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
        assert release.wait()
        return 'real-result'

    monkeypatch.setattr(worker_module.time, 'monotonic', clock)
    resources = cast('_LaneResources', SimpleNamespace(state=object(), client=object()))
    with ThreadPoolExecutor(max_workers=2) as executor:
        caller = executor.submit(component._call, invoke)
        try:
            assert component._wake.wait()
            owner = executor.submit(component._service_calls, resources)
            assert entered.wait()
            assert deadline_checked.wait()
            assert not caller.done()
        finally:
            release.set()
            if not entered.is_set():
                component._stop.set()
        owner.result()
        assert caller.result() == 'real-result'


def test_lane_operator_call_withdrawn_on_shutdown_never_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component = _unopened_group_component(_LaneComponent)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 0.0)
    operations: list[str] = []

    def invoke(_state: object, _client: object) -> None:
        operations.append('invoked')

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(component._call, invoke)
        try:
            assert component._wake.wait()
        finally:
            component._stop.set()
        with pytest.raises(worker_module._CooperativeComponentStopError) as raised:
            future.result()

    assert isinstance(raised.value.__cause__, ConnectionError)
    assert str(raised.value.__cause__) == 'lane component stopped during local call'
    component._service_calls(
        cast('_LaneResources', SimpleNamespace(state=object(), client=object()))
    )
    assert operations == []


def test_lane_operator_reset_commits_reserved_generation(tmp_path: Path) -> None:
    site = _site((group_config('local', worker='default'),))
    spec = build_worker_specs(site)[0]
    assignment = local_assignment(spec, 'local', 'resume')
    component_type = _LaneComponent
    component = cast('_DispositionComponent', object.__new__(component_type))
    source_path = source_database_path(tmp_path)
    group_path = source_database_path(tmp_path)
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
            wait_phase=nullcontext,
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
                'disposition_id': 'provided-disposition',
                'orphan_generation': 7,
                'reserved_generation': 8,
            },
            identity=AuditIdentity(local_uid=501, username='operator', pid=123),
        )
        before = group.snapshot()
        probe = replace(command, arguments={**command.arguments, 'reserved_generation': 7})
        assert component.execute_operator_command(probe) == {'disposition_required': True}
        assert group.snapshot() == before
        result = component.execute_operator_command(command)
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
        assert component.execute_operator_command(retry)['group_generation'] == 8
        assert group._database.connection.execute('SELECT COUNT(*) FROM audit').fetchone() == (
            first_audit_count
        )
        different = replace(
            retry,
            arguments={
                **retry.arguments,
                'disposition_id': 'new-disposition',
                'reason': 'new disposition',
            },
        )
        with pytest.raises(InvalidTransitionError, match='parked or orphaned'):
            component.execute_operator_command(different)

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
                'disposition_id': 'intervening-reset',
                'reason': 'subsequent recovery',
                'reserved_generation': 9,
            },
        )
        assert component.execute_operator_command(intervening)['group_generation'] == 9
        after_intervening = group.snapshot()
        audit_after_intervening = group._database.connection.execute(
            'SELECT COUNT(*) FROM audit'
        ).fetchone()
        with pytest.raises(InvalidTransitionError, match='intervening disposition'):
            component.execute_operator_command(retry)
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
            version=1,
            request_id=command.request_id,
            ok=True,
            result=CommandResult(
                command=command.command,
                request_id=command.request_id,
                ok=True,
                message='accepted',
                result=result,
            ).result,
            error=None,
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
    group = group_config('local', worker='default', ordering='eager')
    site = runtime_site(group)
    provider_state = _RestartProviderState()
    provider_state.publish(42)

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
    with _open_group_health_state(source_database_path(tmp_path)) as state:
        started = commit_step(state, StartEventStep(42, old_manifest, None), expected_revision=0)
        first = commit_step(state, BeginCallbackStep(0), expected_revision=started.revision)
        done = state.callback_done(0, expected_revision=first.revision)
        commit_step(state, BeginCallbackStep(1), expected_revision=done.revision)
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
        result = component.execute_operator_command(
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
        records = audit_records(state)
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
        state.advance_irrelevant(43, expected_revision=state.snapshot().revision)
        coordinator = LaneRefresh(
            joined=lambda: False,
            fetch_interval_secs=1,
            late_event_window_secs=300,
            log_extra={},
            lane=cast('GroupLane', _CoordinationLane(state)),
            slot=partial(_held_limiter_slot, _CatchupLimiter(1)),
            reader=cast('CatchupReader', _CoordinationReader()),
            state=state,
        )
        assert state.snapshot().obligation_count == 1

        result = coordinator.service()

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
        run = cast('Callable[[], None]', vars(type(component))['run'].__get__(component))
        run()

    assert build_count == 2


def test_lane_resolves_directive_before_plugin_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    component, context, operations, _control, _state = _recovery_construction_probe(monkeypatch)
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
        if item
        in {'client', 'observe', 'validate', 'open', 'resolve', 'ready', 'plugin-construction'}
    ]
    assert ordered_boundary == [
        'client',
        'observe',
        'validate',
        'open',
        'resolve',
        'ready',
        'plugin-construction',
    ]


def test_lane_resumes_durable_event_after_setup_before_join() -> None:
    component, resources = lifecycle_probe(
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
            while True:
                if resources.lane.joined:
                    break
                time.sleep(0.001)
        finally:
            stop.set()
        future.result()

    assert resources.lane.operations[:3] == ['setup', 'resume', 'join']
    assert resources.lane.operations.count('resume') == 1
    assert resources.lane.joined == 1


def test_lane_retry_setup_heartbeating_past_operator_budget_is_not_overdue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    group = group_config('local', worker='default')
    site = runtime_site(group)
    spec = build_worker_specs(site)[0]
    assignment = local_assignment(spec, group.name, 'initialize')
    samples: list[tuple[float, WatchdogSample]] = []
    clock = [100.0]
    attempts = 0
    runtime: _WorkerRuntime

    def setup(context: SetupContext) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError('setup unavailable')
        for _ in range(3):
            clock[0] += runtime._operator_deadline_secs_by_source['events'] + 1.0
            context.heartbeat()
            watchdog = runtime._operator_watchdog
            assert watchdog is not None
            samples.append((clock[0], watchdog.sample()))

    plugin = replace(
        _restart_callback_plugin(
            [], callback_entered=threading.Event(), release_callback=threading.Event()
        ),
        setup=setup,
    )

    def load(
        _site: SiteConfig, _group: GroupConfig, _provider: Provider
    ) -> tuple[PluginSpec, ...]:
        return (plugin,)

    monkeypatch.setattr(worker_module, 'load_plugins', load)
    with running_concrete_runtime(
        monkeypatch,
        runtime_config(tmp_path, site),
        spec,
        assignment,
        _restart_provider(_RestartProviderState()),
    ) as (runtime, control):
        receive_typed_health(
            control,
            lambda update: any(entry.failure_kind == 'setup' for entry in update.groups),
        )
        monkeypatch.setattr(runtime, '_clock', lambda: clock[0])
        monkeypatch.setattr(runtime._operator_watchdog, '_clock', lambda: clock[0])
        send_control(
            control,
            OperatorCommand(
                request_id='long-setup-retry',
                command='retry',
                arguments={'site': 'main', 'group': 'local'},
                identity=AuditIdentity(local_uid=501, username='operator', pid=123),
            ),
        )
        result = receive_type(control, CommandResult)

    assert attempts == 2
    assert samples[-1][0] - samples[0][0] == 2 * (
        runtime._operator_deadline_secs_by_source['events'] + 1.0
    )
    assert all(sample.phase == 'idle' and sample.deadline_at is None for _, sample in samples)
    assert result.ok
    assert result.result['group'] == 'local'


def test_live_idle_snapshot_handoff_is_limited_to_already_joined_iteration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 100.0)
    component, resources = lifecycle_probe(
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

    monkeypatch.setattr(resources.state, 'snapshot', snapshot)
    monkeypatch.setattr(resources.lane, 'drain_live', drain)
    drive = cast('_LaneLifecycleDriver', vars(type(component))['_drive_lane'])
    drive(component, resources)
    assert len(snapshots) == 5
    assert handed_off == [None, snapshots[3], snapshots[4]]
    assert resources.lane.operations.count('setup') == 1
    assert resources.lane.operations.count('resume') == 1
    assert resources.lane.operations.count('join') == 1


def test_local_source_initialization_returns_typed_pending(tmp_path: Path) -> None:
    source_path = source_database_path(tmp_path)
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
            wait_phase=nullcontext,
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

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
        force_exit=forced.append,
        monitor_gate=monitor_gate,
    )
    runtime.start()
    try:
        assert monitor_entered.wait()
        runtime.request_stop('control-eof')
        _expire_shutdown(runtime, set())
        assert runtime._shutdown() == ()
        assert runtime._monitor_thread is not None
        assert runtime._monitor_thread.is_alive()
        assert forced == []
        assert runtime._resources_open
    finally:
        block_monitor.set()
        component.release.set()
        runtime.request_stop('test-cleanup')
        assert runtime._monitor_thread is not None
        runtime._monitor_thread.join()
        assert runtime._shutdown() == ()


def test_monitor_control_eof_broken_pipe_preserves_hard_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _monitor_control_runtime(tmp_path, monkeypatch) as (runtime, parent, _child, component):
        forced: list[int] = []
        monkeypatch.setattr(component, 'wedge', True)
        monkeypatch.setattr(runtime, '_force_exit', forced.append)
        runtime.start()
        assert component.started.wait()
        receive_type(parent, HealthUpdate)
        parent.close()
        _expire_shutdown(runtime, {'source:events'})
        monitor = vars(runtime)['_monitor_thread']
        monitor.join()

        assert not monitor.is_alive()
        assert forced == [1]
        assert runtime.fatal_reason is None
        assert runtime._stuck_threads == ('source:events',)
        assert runtime._resources_open
        assert vars(runtime)['_stop_reason'] == 'control-eof'

    assert component.finished.is_set()
    assert not runtime._resources_open
    assert component.operations[-1] == 'close:source:events'


def test_monitor_outbox_transport_failure_keeps_fatal_classification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _monitor_control_runtime(tmp_path, monkeypatch) as (runtime, _parent, _child, component):

        def fail_write(_outbox: WorkerControlOutbox) -> NoReturn:
            error = OSError('injected control transport failure')
            raise EOFError('worker control is closed') from error

        monkeypatch.setattr(WorkerControlOutbox, 'write_available', fail_write)
        runtime.start()
        assert _wait_for_runtime_stop(runtime) == ()
        assert (
            runtime.fatal_reason
            == 'monitor thread failed: OSError: injected control transport failure'
        )
        assert component.finished.is_set()


def test_monitor_reports_thread_exit_and_keeps_heartbeat_until_exit() -> None:
    spec = build_worker_specs(_site(()))[0]
    operations: list[str] = []
    sources = {name: _Component(f'source:{name}', operations) for name in spec.sources}
    health: list[WorkerHealth] = []
    health_published = threading.Condition()

    def publish_health(update: WorkerHealth) -> None:
        with health_published:
            health.append(update)
            health_published.notify_all()

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=publish_health,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
    )
    runtime.start()
    try:
        assert all(source.started.wait() for source in sources.values())
        with health_published:
            assert health_published.wait_for(lambda: len(health) >= 2)
        assert health[-1].monitor_alive
        sources['events'].release.set()
        while True:
            if runtime.fatal_reason is not None:
                break
            time.sleep(0.001)
        assert runtime.fatal_reason == 'source:events thread exited'
    finally:
        for source in sources.values():
            source.release.set()
        runtime.request_stop('test')
        assert runtime._shutdown() == ()


def test_monitor_requested_clean_return_remains_nonfatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = build_worker_specs(_site(()))[0]
    operations: list[str] = []
    components = {name: _Component(f'source:{name}', operations) for name in spec.sources}
    runtime: _WorkerRuntime

    @contextmanager
    def request_stop() -> Generator[None]:
        runtime.request_stop('test-requested-stop')
        yield

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
        monitor_gate=request_stop,
    )

    result = _run_worker_runtime_cli(monkeypatch, runtime)

    assert result == 0
    assert runtime.fatal_reason is None
    assert all(component.finished.is_set() for component in components.values())
    assert not runtime._resources_open


def test_monitor_samples_before_each_writable_control_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _monitor_control_runtime(tmp_path, monkeypatch) as (runtime, _parent, _child, _component):
        events: list[str] = []
        waits = 0

        def sample(_health: WorkerHealth) -> None:
            events.append('sample')

        def write() -> None:
            events.append('write')

        def wait(_timeout: float) -> None:
            nonlocal waits
            waits += 1
            if waits == 2:
                runtime.request_stop('two-writable-passes')

        monkeypatch.setattr(runtime, '_clock', lambda: 0.0)
        monkeypatch.setattr(runtime, '_health_sink', sample)
        monkeypatch.setattr(
            runtime,
            '_outbox',
            SimpleNamespace(write_available=write, wait=wait, abandon=lambda: None),
        )
        runtime._monitor()
        assert events[:4] == ['sample', 'write', 'sample', 'write']


def test_monitor_unexpected_clean_return_is_worker_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = build_worker_specs(_site(()))[0]
    operations: list[str] = []
    components = {name: _Component(f'source:{name}', operations) for name in spec.sources}
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
    )
    monkeypatch.setattr(runtime, '_monitor', lambda: None)

    result = _run_worker_runtime_cli(monkeypatch, runtime)

    assert result == 1
    assert runtime.fatal_reason == 'monitor thread exited'
    assert all(component.finished.is_set() for component in components.values())
    assert not runtime._resources_open


def test_operator_completion_during_admission_is_queued_before_new_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _monitor_control_runtime(tmp_path, monkeypatch) as (runtime, parent, child, _component):
        first = OperatorCommand(
            command='restart-event',
            request_id='completed',
            arguments={'site': 'main', 'source': 'events'},
            identity=AuditIdentity(local_uid=1, username='operator', pid=1),
        )
        completed = CommandResult(
            command=first.command,
            request_id=first.request_id,
            ok=True,
            message='completed',
            result={},
        )
        runtime._outbox = WorkerControlOutbox(child)
        runtime._operator_command = first
        owner = runtime._operator_owner

        def complete_during_lookup(message: OperatorCommand) -> tuple[str, str | None]:
            target = owner(message)
            with runtime._state_lock:
                runtime._operator_result = completed
            return target

        monkeypatch.setattr(runtime, '_operator_owner', complete_during_lookup)
        next_command = replace(first, request_id='next')
        assert runtime._queue_operator_command(next_command) is None
        assert runtime._operator_command is next_command
        runtime._outbox.write_available()
        assert receive_control(parent) == completed


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
        site = _site((group_config('local', worker='default'),))
        config = runtime_config(state_dir, site)
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
    component, resources = lifecycle_probe(
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
            assert resources.coordination.serviced.wait()
            assert not future.done()
            assert resources.lane.joined == 0
            assert resources.lane.drains == 0
            assert resources.coordination.calls
            assert not any(resources.coordination.calls)
        finally:
            stop.set()
        future.result()


def test_partial_startup_closes_lane_resources_on_owner_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = build_worker_specs(_site((group_config('local', worker='default'),)))[0]
    component, resources = lifecycle_probe(
        'sync',
        durable_status='running',
        setup_failure=None,
    )
    closed: list[tuple[str, str]] = []
    for name in ('channel', 'lane', 'state', 'reader', 'client'):
        resource = vars(resources)[name]
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
            assert resources.coordination.serviced.wait()
            raise RuntimeError('operator startup failed')
        original_start(thread)

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='partial-lifecycle',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda _name, _slots, _host: lane,
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
    spec = build_worker_specs(_site((group_config('local', worker='default'),)))[0]
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
            if self.name == 'sg-event-daemon:source:audit' and timeout is not None:
                now += timeout
            else:
                super().join()

        def start(self) -> None:
            if self.name == 'sg-event-daemon:monitor':
                assert all(component.started.wait() for component in components.values())
                raise RuntimeError('monitor startup failed')
            super().start()
            started.append(self)

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='partial-stuck',
        source_factory=lambda name, _slots, _host: components[f'source:{name}'],
        lane_factory=lambda name, _slots, _host: components[f'lane:{name}'],
        health_sink=lambda _health: None,
        health_interval_secs=0.01,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=10.0,
        max_concurrent_catchup_readers=1,
        clock=clock,
        force_exit=forced.append,
    )
    monkeypatch.setattr(worker_module.threading, 'Thread', StartupThread)
    try:
        with pytest.raises(RuntimeError, match='monitor startup failed'):
            runtime.start()
        assert joins == [
            ('sg-event-daemon:source:audit', 10.0),
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
            component.release.set()
        for thread in started:
            thread_type.join(thread)
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
        surface = cast('_SourceJoinSurface', SimpleNamespace(state=state))
        component._publish_pending_groups(surface)
        first = cache.snapshot().groups[0]
        watchdog_before = cache.snapshot().watchdogs[0].heartbeat_at
        clock.now = 0.5
        component._watchdog.heartbeat()
        component._publish_pending_groups(surface)
        second = cache.snapshot().groups[0]
        assert calls == 1
        assert first.heartbeat_at == 0.0
        assert second.heartbeat_at == 0.5
        assert cache.snapshot().watchdogs[0].heartbeat_at > watchdog_before

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
        surface = cast('_SourceJoinSurface', SimpleNamespace(state=state))
        component._publish_pending_groups(surface)
        clock.now = 0.05
        component._publish_pending_groups(surface)
        assert calls == 1
        before = cache.snapshot().groups[0]

        def dispose(_surface: _SourceJoinSurface) -> GroupSnapshot:
            with _open_group_health_state(source_database_path(tmp_path)) as group:
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
        call = component._mailbox.submit(dispose)

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
                SimpleNamespace(state=state, reconcile_groups=lambda: None),
            ),
            cast('SourcePoller', object()),
        )
        assert call.complete.is_set()
        assert call.error is None
        assert calls == 2
        after = cache.snapshot().groups[0]
        assert after.heartbeat_at == 0.1
        assert after.audit_count == before.audit_count + 1
        assert state.group_snapshot('local').is_operator_replay

        clock.now = 0.2
        component._mark_source_health_dirty()
        component._publish_pending_groups(surface)
        assert calls == 3


def test_pending_group_health_reports_epoch_age_and_gates_park_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _pending_group_health_probe(tmp_path) as (component, cache, state):
        record = state.pending_group_record

        def retained_epoch(group: str, *, limit: int) -> object:
            detail = record(group, limit=limit)
            snapshot = replace(
                detail.snapshot,
                reader_clock_checkpoint=15.0,
                parked=False,
                failure_text='obsolete callback detail',
                epochs=(
                    CatchupEpoch(
                        epoch_id=1,
                        low_id=42,
                        high_id=50,
                        captured_at=3.0,
                        eligibility_point=5.0,
                        reader_clock_checkpoint=15.0,
                        source_generation=detail.snapshot.source_generation,
                        group_generation=detail.snapshot.group_generation,
                        state='waiting',
                    ),
                ),
            )
            return replace(detail, snapshot=snapshot)

        monkeypatch.setattr(state, 'pending_group_record', retained_epoch)
        surface = cast('_SourceJoinSurface', SimpleNamespace(state=state))
        component._publish_pending_groups(surface)
        health = cache.snapshot().groups[0]
        assert health.pending_catchup_epoch_count == 1
        assert health.oldest_catchup_epoch_age == 12.0
        assert health.park_reason is None
        assert health.failure_text is None


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
        component._publish_pending_groups(cast('_SourceJoinSurface', SimpleNamespace(state=state)))
        assert cache.snapshot().groups[0].failure_kind == 'callback'


def test_pending_source_inventory_allows_orphan_observation_but_rejects_join(
    tmp_path: Path,
) -> None:
    source_path = source_database_path(tmp_path)
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


@pytest.mark.parametrize(
    ('stage', 'exit_call', 'class_name'),
    [
        ('callback', 'raise SystemExit(3)', 'SystemExit'),
        ('setup', 'raise SystemExit(3)', 'SystemExit'),
        ('callback', 'raise KeyboardInterrupt()', 'KeyboardInterrupt'),
    ],
)
def test_plugin_exit_parks_only_its_group(
    tmp_path: Path, stage: str, exit_call: str, class_name: str
) -> None:
    _run_plugin_exit_process(tmp_path, stage, exit_call, class_name, async_callback=False)


@pytest.mark.parametrize(
    ('exit_call', 'class_name'),
    [('raise SystemExit(3)', 'SystemExit'), ('raise KeyboardInterrupt()', 'KeyboardInterrupt')],
)
def test_async_callback_exit_parks_group_and_loop_survives(
    tmp_path: Path, exit_call: str, class_name: str
) -> None:
    _run_plugin_exit_process(tmp_path, 'callback', exit_call, class_name, async_callback=True)


@pytest.mark.parametrize('operation', ['create_sync', 'find'])
@pytest.mark.usefixtures('_production_coordination_deadlines')
def test_hung_catch_up_provider_call_is_overdue_after_one_budget(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    component = _unopened_group_component(_LaneComponent)
    now = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(component._watchdog, '_clock', lambda: now[0])
    budget = component._internal_stall_deadline_secs
    component._watchdog.transition('startup', deadline_at=130.0)
    observed: list[WatchdogSample] = []

    def observe_call() -> None:
        started = now[0]
        sample = component._watchdog.sample()
        observed.append(sample)
        assert sample.phase == 'internal'
        assert sample.deadline_at == started + budget
        now[0] += budget + 0.01
        deadline_at = component._watchdog.sample().deadline_at
        assert deadline_at is not None
        assert deadline_at < now[0]

    def find(
        _entity_type: str, _filters: object, _fields: list[str], **_options: object
    ) -> list[dict[str, object]]:
        if operation == 'find':
            observe_call()
        return [{'id': 41}]

    handle = SimpleNamespace(find=find, close=lambda: None)

    def create_sync(_site: str, _credential: CredentialConfig) -> object:
        if operation == 'create_sync':
            observe_call()
        return handle

    provider = cast('Provider', SimpleNamespace(create_sync=create_sync))

    def load_provider(_name: str, _required: set[str]) -> Provider:
        return provider

    monkeypatch.setattr(worker_module, 'load_provider', load_provider)
    _, reader = component._provider_reader(component._configuration())
    with reader:
        if operation == 'find':
            now[0] = 104.0
            assert reader.latest_visible_id() == 41
    assert len(observed) == 1


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
        provider_state.publish(event_id)
    provider = _restart_provider(provider_state)
    group = group_config('local', worker='default')
    site = runtime_site(group)
    component = _unopened_group_component(_LaneComponent)
    watchdog = component._watchdog
    plugin = health_failure_plugin('callback')
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
            waiter=SourceHealthWaiter(),
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
            state=state,
            lane=lane,
            reader=reader,
            context=SimpleNamespace(source_config=site.sources['events']),
            coordination=SimpleNamespace(
                service=Mock(return_value=None),
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
    call = component._mailbox.submit(invoke)
    resources = SimpleNamespace(state=None, client=None)
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
    if isinstance(error, WriteLockStoppedError):
        with pytest.raises(type(error), match=str(error)):
            service(resources)
        assert not call.complete.is_set()
    else:
        service(resources)
        assert call.complete.is_set()
        assert call.error is error


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
        kind=_WorkerKind.SITE,
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = _WorkerRuntime(
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
        send_control(parent, command)
        assert components[owner].operator_entered.wait()
        watchdog = next(item for item in cache.snapshot().watchdogs if item.kind == 'operator')
        components[owner].operator_release.set()
        result = receive_control(parent)
        assert isinstance(result, CommandResult)
        while True:
            idle = next(item for item in cache.snapshot().watchdogs if item.kind == 'operator')
            if idle.phase == 'idle':
                break
            time.sleep(0.001)
    finally:
        for component in components.values():
            component.operator_release.set()
        runtime.request_stop('test-cleanup')
        assert runtime.wait() == ()
        parent.close()
        child.close()

    assert result.ok
    assert watchdog.phase == ('idle' if owner == 'local' else 'internal')
    assert watchdog.heartbeat_at == 100.0
    assert watchdog.deadline_at == (None if owner == 'local' else 100.0 + budget)
    assert watchdog.operator_command == (None if owner == 'local' else command.command)
    assert watchdog.source == ('events' if owner == 'events' else None)
    assert watchdog.group is None
    assert idle.phase == 'idle'
    assert idle.deadline_at is None
    assert idle.source is None
    assert idle.group is None


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
    cache.register_watchdog(
        key=(
            kind,
            'events' if kind == 'source' else 'local',
            'spawn-1',
            predecessor.registration_token,
        ),
        supplier=lambda: predecessor,
    )
    cache.register_watchdog(
        key=(
            kind,
            'events' if kind == 'source' else 'local',
            'spawn-1',
            successor.registration_token,
        ),
        supplier=lambda: successor,
    )

    source = _watchdog_source_health('events', worker_instance='spawn-1')
    group = _watchdog_group_health('local', source='events', worker_instance='spawn-1')

    def publish(sample: WatchdogSample) -> None:
        if kind == 'source':
            cache.publish_source(source, sample=sample)
        else:
            cache.publish_group(group, sample=sample)

    publish(predecessor)
    snapshot = cache.snapshot()
    assert snapshot.sources == ()
    assert snapshot.groups == ()
    assert [item.heartbeat_at for item in snapshot.watchdogs] == [successor.heartbeat_at]

    publish(successor)
    snapshot = cache.snapshot()
    if kind == 'source':
        assert len(snapshot.sources) == 1
        assert snapshot.sources[0].frontier == source.frontier
        assert snapshot.sources[0].heartbeat_at == successor.heartbeat_at
    else:
        assert len(snapshot.groups) == 1
        assert snapshot.groups[0].cursor == group.cursor
        assert snapshot.groups[0].heartbeat_at == successor.heartbeat_at

    publish(predecessor)
    cache.deregister_watchdog(
        kind=kind,
        identity=predecessor.identity,
        worker_instance=predecessor.worker_instance,
        token=predecessor.registration_token,
    )
    assert cache.snapshot() == snapshot


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_concrete_resume_worker_completes_persisted_callback_before_live_delivery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    group = group_config(
        'local',
        worker='default',
        execution=execution,
        ordering='eager',
    )
    site = runtime_site(group)
    config = runtime_config(tmp_path, site)
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
    prepare_runtime_state(
        tmp_path,
        site,
        group,
        'resume',
        in_flight=(42, manifest),
    )
    provider_state.publish(42)

    def plugins(
        _site: object,
        _group: object,
        _provider: object,
    ) -> tuple[PluginSpec, ...]:
        return (plugin,)

    monkeypatch.setattr(worker_module, 'load_plugins', plugins)
    durable_snapshot: GroupSnapshot | None = None
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        local_assignment(spec, group.name, 'resume'),
        _restart_provider(provider_state, async_=execution == 'async'),
    ) as (runtime, control):
        try:
            while invoked != [42] and runtime.fatal_reason is None:
                time.sleep(0.001)
            assert invoked == [42], runtime.fatal_reason
            assert runtime.fatal_reason is None
            update = receive_typed_health(
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
            while True:
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
        source_database_path(tmp_path),
        writes=write_context_for(source_database_path(tmp_path)),
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
        strict_holdback_secs=source_config.late_event_window_secs,
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
    config = runtime_config(tmp_path, site)
    spec = WorkerSpec(
        worker_id='["named","empty"]',
        site='main',
        kind=_WorkerKind.NAMED,
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
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

    cache.register_watchdog(
        key=('source', 'events', 'sampling-failure-1', healthy.registration_token),
        supplier=fail,
    )
    cache.publish_source(
        _watchdog_source_health('events', worker_instance='sampling-failure-1'),
        sample=healthy,
    )
    try:
        if peer_closed:
            parent.close()
        runtime.start()
        update = None if peer_closed else receive_type(parent, HealthUpdate)
        assert _wait_for_runtime_stop(runtime) == ()
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
        channel=probes['channel'],
        client=probes['client'],
        lane=probes['lane'],
        reader=probes['reader'],
        state=probes['state'],
    )

    def service_calls(_resources: object) -> NoReturn:
        raise failure

    vars(component)['_service_calls'] = service_calls
    runtime_component = _LifecycleComponent(component, resources)
    site = runtime_site(group_config('local', worker='default'))
    forced: list[int] = []
    runtime = _WorkerRuntime(
        spec=build_worker_specs(site)[0],
        health_cache=_ComponentHealthCache(
            site=build_worker_specs(site)[0].site, group_sources={}
        ),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: runtime_component,
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
    site = runtime_site(group_config('local', worker='default'))
    spec = build_worker_specs(site)[0]
    operations: list[str] = []
    source = _Component('source:events', operations, wedge=True)
    release_failure = threading.Event()
    interrupted = threading.Event()
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

    monkeypatch.setattr(threading, 'excepthook', observe_error)
    lane = InterruptibleComponent(fail, release_failure.set)
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: source,
        lane_factory=lambda _name, _slots, _host: lane,
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
        force_exit=force_exit,
    )
    try:
        runtime.start()
        assert source.started.wait()
        release_failure.set()
        assert interrupted.wait()
        assert errors == [failure]
        assert runtime.fatal_reason == f'lane:local thread failed: {error_type.__name__}'
        _expire_shutdown(runtime, {'source:events'})
        assert runtime._monitor_thread is not None
        runtime._monitor_thread.join()
        assert exits == [(1, 'sg-event-daemon:monitor')]
        assert runtime._stuck_threads == ('source:events',)
        assert 'close:source:events' not in operations
    finally:
        release_failure.set()
        source.release.set()
        runtime.request_stop('test cleanup')
        for thread in runtime._threads.values():
            thread.join()
        runtime._shutdown()


@pytest.mark.parametrize('command', ['replay', 'reset-group-to-latest'])
def test_lane_run_disposition_detaches_and_reregisters_at_the_source(
    tmp_path: Path, command: str
) -> None:
    path = source_database_path(tmp_path)
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
            spec = build_worker_specs(_site((group_config('local', worker='default'),)))[0]

            def call(invoke: Callable[[GroupState, object], object]) -> object:
                return invoke(group, object())

            vars(component).update(
                _assignment=local_assignment(spec, 'local', 'resume'),
                _group='local',
                _reload=threading.Event(),
                _spec=spec,
                _call=call,
            )
            arguments: dict[str, object] = {
                'site': 'main',
                'group': 'local',
                'reason': 'replace generation',
                'disposition_id': 'provided-disposition',
                'orphan_generation': 7,
                'reserved_generation': 8,
            }
            if command == 'replay':
                arguments['from_event_id'] = 41
                arguments['acknowledge_truncation'] = False
            result = component.execute_operator_command(
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
            watermark = strict_watermark(strict, 'local')
            assert watermark.group_generation == 8
            assert watermark.delivery_floor == group.snapshot().delivery_floor
            assert watermark.last_delivered_id == group._bootstrap_state().last_delivered_id
            assert watermark.is_operator_replay is (command == 'replay')
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


@pytest.mark.parametrize('prior_failure', [False, True])
def test_monitor_peer_close_between_poll_and_health_send_preserves_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prior_failure: bool
) -> None:
    with _monitor_control_runtime(tmp_path, monkeypatch) as (runtime, parent, child, component):
        receive = child.receive
        write = WorkerControlOutbox.write_available
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

        def observe_write(outbox: WorkerControlOutbox) -> None:
            try:
                write(outbox)
            except EOFError:
                events.append('send-broken-pipe')
                raise

        monkeypatch.setattr(child, 'receive', close_after_empty_poll)
        monkeypatch.setattr(WorkerControlOutbox, 'write_available', observe_write)
        result = _run_worker_runtime_cli(monkeypatch, runtime)

        assert events[:3] == ['poll-empty', 'peer-closed', 'send-broken-pipe']
        assert result == int(prior_failure)
        assert runtime.fatal_reason == ('original-component-failure' if prior_failure else None)
        assert vars(runtime)['_stop_reason'] == (
            'original-component-failure' if prior_failure else 'control-eof'
        )
        if prior_failure:
            assert [vars(runtime)['_shutdown_deadline_at']] == original_deadline
        assert component.finished.is_set()
        assert not runtime._resources_open
        assert component.operations[-1] == 'close:source:events'
        assert all(not thread.is_alive() for _name, thread in runtime._owned_threads())


@pytest.mark.parametrize('removal', ['pending-assignment', 'source-deregistration'])
def test_pending_group_removal_preserves_newer_lane_health(tmp_path: Path, removal: str) -> None:
    with _pending_group_health_probe(tmp_path) as (component, cache, state):
        surface = cast('_SourceJoinSurface', SimpleNamespace(state=state))
        component._publish_pending_groups(surface)
        pending = cache.snapshot().groups[0]
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
        cache.register_watchdog(
            key=('lane', 'local', lane_sample.worker_instance, lane_sample.registration_token),
            supplier=lambda: lane_sample,
        )
        cache.publish_group(lane, sample=lane_sample)
        before = cache.snapshot()
        assert before.groups[0].audit_count == lane.audit_count
        assert before.groups != (pending,)
        lane_watchdogs = tuple(watch for watch in before.watchdogs if watch.kind == 'lane')
        assert len(lane_watchdogs) == 1
        assert lane_watchdogs[0].group == 'local'
        if removal == 'pending-assignment':
            component._assignment = replace(component._assignment, source_group_assignments=())
            component._publish_pending_groups(surface)
        else:
            component.close()
        after = cache.snapshot()
        assert after.groups == before.groups
        assert tuple(watch for watch in after.watchdogs if watch.kind == 'lane') == lane_watchdogs


def test_post_close_scan_checks_retention_before_first_page() -> None:
    state = _RetentionCoordinationState()
    reader = _RetentionCoordinationReader((55,))
    lane = _RetentionCoordinationLane(state)
    coordinator = LaneRefresh(
        joined=lambda: False,
        fetch_interval_secs=1,
        late_event_window_secs=300,
        log_extra={},
        lane=cast('GroupLane', lane),
        slot=partial(_held_limiter_slot, _CatchupLimiter(1)),
        reader=cast('CatchupReader', reader),
        state=cast('GroupState', state),
    )
    scan = cast(
        'Callable[[object, SourceProgress], None]',
        vars(type(coordinator))['_scan_obligations'],
    )

    coordinator.synchronize()
    scan(coordinator, _synchronization(60))

    assert state.truncated_floor == 55
    assert state.status == 'history-truncated'
    assert reader.ranges == []
    assert not lane.resolved


def test_post_close_scan_rechecks_retention_before_resolution() -> None:
    state = _RetentionCoordinationState()
    reader = _RetentionCoordinationReader(
        (1, 55),
        (Event(runtime_event(51)),),
    )
    lane = _RetentionCoordinationLane(state)
    coordinator = LaneRefresh(
        joined=lambda: False,
        fetch_interval_secs=1,
        late_event_window_secs=300,
        log_extra={},
        lane=cast('GroupLane', lane),
        slot=partial(_held_limiter_slot, _CatchupLimiter(1)),
        reader=cast('CatchupReader', reader),
        state=cast('GroupState', state),
    )
    scan = cast(
        'Callable[[object, SourceProgress], None]',
        vars(type(coordinator))['_scan_obligations'],
    )

    coordinator.synchronize()
    scan(coordinator, _synchronization(60))

    assert lane.delivered == [51]
    assert state.truncated_floor == 55
    assert state.status == 'history-truncated'
    assert not lane.resolved


def test_post_close_scans_share_worker_catchup_limiter_without_double_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _RetentionCoordinationState()
    reader = _RetentionCoordinationReader((1, 1))
    lane = _RetentionCoordinationLane(state)
    limiter = _CatchupLimiter(1)
    coordinator = LaneRefresh(
        joined=lambda: False,
        fetch_interval_secs=1,
        late_event_window_secs=300,
        log_extra={},
        lane=cast('GroupLane', lane),
        slot=partial(_held_limiter_slot, limiter),
        reader=cast('CatchupReader', reader),
        state=cast('GroupState', state),
    )
    scan = cast(
        'Callable[[object, SourceProgress], None]',
        vars(type(coordinator))['_scan_obligations'],
    )
    completed = threading.Event()
    started = threading.Event()
    acquisition_attempted = threading.Event()
    acquired_while_held: list[bool] = []
    original_scan = reader.fetch_id_runs
    acquire = limiter.acquire

    def observed_acquire(*, timeout: float) -> bool:
        acquired = acquire(timeout=0.0)
        acquired_while_held.append(acquired)
        acquisition_attempted.set()
        return acquired or acquire(timeout=timeout)

    def blocking_scan(runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
        started.set()
        yield from original_scan(runs)

    reader.fetch_id_runs = blocking_scan

    coordinator.synchronize()
    with _held_limiter_slot(limiter):
        monkeypatch.setattr(limiter, 'acquire', observed_acquire)
        thread = threading.Thread(
            target=lambda: (
                scan(coordinator, _synchronization(60)),
                completed.set(),
            )
        )
        thread.start()
        assert acquisition_attempted.wait()
        assert acquired_while_held
        assert not any(acquired_while_held)
        assert not started.is_set()
        assert not completed.is_set()
    thread.join()

    assert not thread.is_alive()
    assert started.is_set()
    assert completed.is_set()
    assert not lane.resolved


def test_preflight_covers_source_command_operator_record(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source_name = 'a' * 20
    group = replace(group_config('g', worker='default'), source='b')
    site = runtime_site(group)
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
    config = runtime_config(tmp_path, site)
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
        control = WorkerControl(child_socket, max_frame_bytes=131_072)
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
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 100.0)
    wake = threading.Event()
    channel = _DeliveryFloorChannel(
        group='editorial',
        ordering='eager',
        max_events=1,
        max_bytes=262_144,
        delivery_floor=0,
    )
    channel.set_wake(wake)
    channel.activate(provisional=False)
    event = EventDelivery(EncodedEvent.from_event_frame(7, encode_event(Event(runtime_event(7)))))
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
        lifecycle.channel = channel
        lifecycle.next_coordination_at = time.monotonic() + 60.0
        calls: list[str] = []

        def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
            del force
            del scan_obligations
            calls.append('tick')

        monkeypatch.setattr(resources.coordination, 'service', coordinate)
        component._service_due(resources, lifecycle)
        assert calls == ['tick']
        assert lifecycle.next_coordination_at > time.monotonic()
        component._service_due(resources, lifecycle)
        assert calls == ['tick']


@pytest.mark.parametrize('caller', ['eager', 'initialize', 'scan'])
@pytest.mark.usefixtures('_production_phase_deadlines')
def test_reader_slot_wait_is_health_valid_and_stoppable(
    monkeypatch: pytest.MonkeyPatch, caller: str
) -> None:
    clock = [100.0]
    component = _unopened_group_component(_LaneComponent)
    component._health_interval_secs = 1.0
    component._internal_stall_deadline_secs = 3.0
    component._watchdog = ComponentWatchdog(
        kind='lane',
        identity='main:events:local',
        worker_instance='worker-1',
        registration_token=component._watchdog.sample().registration_token,
        startup_deadline_secs=3.0,
        clock=lambda: clock[0],
    )
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    samples: list[WatchdogHealth] = []

    class HeldPermit:
        def acquire(self, blocking: bool = True, timeout: float | None = None) -> bool:
            assert blocking
            clock[0] += 15.0 if timeout is None else timeout
            sample = component._watchdog.sample()
            samples.append(
                WatchdogHealth(
                    kind='lane',
                    site='main',
                    source='events',
                    group='local',
                    worker_instance=sample.worker_instance,
                    phase=sample.phase,
                    heartbeat_at=sample.heartbeat_at,
                    deadline_at=sample.deadline_at,
                )
            )
            assert sample.deadline_at is not None
            assert sample.deadline_at > clock[0]
            if clock[0] >= 115.0:
                component._stop.set()
            return False

        def release(self) -> None:
            pytest.fail('unacquired reader permit was released')

    component._limiter._semaphore = cast('threading.BoundedSemaphore', HeldPermit())
    if caller == 'eager':
        resources = cast(
            '_LaneResources',
            SimpleNamespace(
                client=SimpleNamespace(
                    last_join_target=SimpleNamespace(stream_incarnation='incarnation')
                ),
                context=SimpleNamespace(group=SimpleNamespace(ordering='eager')),
            ),
        )
        with (
            component._watch_phase('idle', deadline_at=103.0),
            pytest.raises(worker_module._CooperativeComponentStopError) as raised,
        ):
            component._catch_up(resources, 0)
    elif caller == 'initialize':
        context = cast(
            '_LaneConfiguration',
            SimpleNamespace(assignment=SimpleNamespace(start_mode='initialize')),
        )
        with (
            component._watch_phase('startup'),
            pytest.raises(worker_module._CooperativeComponentStopError) as raised,
        ):
            component._open_group_state(
                context,
                cast('GroupBootstrapObservation', object()),
                cast('CatchupReader', object()),
            )
    else:
        state = _RetentionCoordinationState()
        refresh = LaneRefresh(
            joined=lambda: False,
            fetch_interval_secs=1,
            late_event_window_secs=300,
            log_extra={},
            lane=cast('GroupLane', _RetentionCoordinationLane(state)),
            slot=component._reader_slot,
            reader=cast('CatchupReader', _RetentionCoordinationReader((1, 1))),
            state=cast('GroupState', state),
        )
        refresh.synchronize()
        with (
            component._watch_phase('internal'),
            pytest.raises(worker_module._CooperativeComponentStopError) as raised,
        ):
            refresh._scan_obligations(_synchronization(60))

    assert raised.value.phase == 'reader-slot'
    assert clock[0] >= 115.0
    assert samples
    assert all(sample.phase == 'idle' for sample in samples)


@pytest.mark.parametrize('typed_health', [False, True])
@pytest.mark.parametrize('ready', [False, True])
def test_readiness_helpers_follow_signals_and_propagate_worker_eof(
    monkeypatch: pytest.MonkeyPatch, *, typed_health: bool, ready: bool
) -> None:
    received = [0]
    health = Mock(spec=HealthUpdate)
    endpoint = SourceEndpointReady('events', 'generation', '/runtime/events.sock')

    def receive(_control: WorkerControl) -> object:
        received[0] += 1
        if received[0] == 150:
            if not ready:
                raise EOFError('test worker exited before readiness')
            if not typed_health:
                return endpoint
        return health

    monkeypatch.setattr(sys.modules[__name__], 'receive_control', receive)
    control = cast('WorkerControl', object())

    def await_ready() -> object:
        if typed_health:
            return receive_typed_health(control, lambda _update: ready and received[0] == 150)
        return receive_type(control, SourceEndpointReady)

    if ready:
        assert await_ready() is (health if typed_health else endpoint)
    else:
        with pytest.raises(EOFError, match='worker exited before readiness'):
            await_ready()
    assert received[0] == 150


def test_real_pause_fails_readiness_and_passes_liveness(
    tmp_path: Path,
) -> None:
    component, cache = _source_health_component(tmp_path, health_interval_secs=1.0)
    server = _SourceHealthServer()
    site = runtime_site(group_config('local', worker='default'))
    spec = build_worker_specs(site)[0]

    class _Handle(RuntimeProviderHandle):
        def find(
            self,
            entity_type: str,
            filters: object,
            fields: list[str] | None = None,
            **options: Unpack[_FindOptions],
        ) -> list[dict[str, object]]:
            del entity_type, fields, options
            if isinstance(filters, list) and filters and isinstance(filters[0], list):
                return [runtime_event(106)]
            return []

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _Handle:
        assert (site, script_name, api_key, connect) == (
            'https://example.test',
            'daemon',
            'key',
            False,
        )
        return _Handle(lambda: 100)

    provider = Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        sync_factory=factory,
        async_factory=None,
    )
    updates: list[HealthUpdate] = []
    published = threading.Event()

    def publish(worker: WorkerHealth) -> None:
        snapshot = cache.snapshot()
        updates.append(
            HealthUpdate(
                worker=worker,
                watchdogs=snapshot.watchdogs,
                sources=snapshot.sources,
                groups=snapshot.groups,
                event_sockets=snapshot.event_sockets,
            )
        )

        if snapshot.sources and snapshot.sources[0].status == 'range-capacity-exhausted':
            published.set()

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='site-spawn-1',
        source_factory=lambda _name, _slots, _host: _Component('source', []),
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=publish,
        health_interval_secs=2.0,
        checkpoint_interval_secs=10.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
    )
    with _open_coordination_source(tmp_path / 'source.db') as state:
        state._settings = replace(state._settings, max_open_ranges=1)
        state.initialize(100, incarnation='incarnation-1')
        _bootstrap_coordination_group(state._database.path)
        open_source_range(state, 101, 103, successful_clock=0.0)
        with _source_health_poller(state, provider) as poller:
            assert poller._poll_once() == 0
            _service_source_health(component, server, poller)
            runtime.start()
            try:
                assert published.wait()
            finally:
                runtime.request_stop('test-cleanup')
                assert runtime.wait() == ()
        update = next(
            item
            for item in updates
            if item.sources and item.sources[0].status == 'range-capacity-exhausted'
        )
        assert update.worker.monitor_alive
        status = status_from_health(
            sources=update.sources,
            groups=update.groups,
            workers=(update.worker,),
            now=update.worker.heartbeat_at,
            stale_after_secs=1.0,
        )
        assert update.sources[0].status == 'range-capacity-exhausted'
        assert 'capacity-exhausted' in status.conditions
        assert status.liveness
        assert not status.readiness


def test_rejoin_resets_the_coordination_due_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: 100.0)
    component_object, resources_object = lifecycle_probe(
        'sync', durable_status='running', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(channel=resources.channel)
    lifecycle.next_coordination_at = time.monotonic() + 60.0
    assert component._join_lane(resources, lifecycle)
    assert lifecycle.next_coordination_at <= time.monotonic()


def test_remote_bootstrap_retry_does_not_hide_partial_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations: list[str] = []
    cleanup_error = RuntimeError('reader cleanup failed')
    client = _CleanupProbe('client', operations, None)
    reader = _CleanupProbe('reader', operations, cleanup_error)
    state = _CleanupProbe('state', operations, None)
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(component).update({'_group': 'remote', '_lane': None})
    context = SimpleNamespace(
        assignment=SimpleNamespace(
            conditional_park=None,
            group_generation=7,
            start_mode='resume',
        ),
        group=SimpleNamespace(source='events'),
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

    def load_plugins(_loader: object) -> object:
        raise retry_error

    monkeypatch.setattr(worker_module, '_load_shared_plugins', load_plugins)

    def resolve(context: object, _state: object) -> object:
        return context

    def publish_startup_health(_context: object, _state: object) -> None:
        pass

    vars(component).update(
        {
            '_build_client': build_client,
            '_observe_source': observe,
            '_open_group_state': open_state,
            '_provider_reader': provider_reader,
            '_publish_startup_group_health': publish_startup_health,
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
    assert traceback is reader.traceback


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
    group = group_config('remote', worker='pool')
    site = _site((group,))
    source_config = site.sources['events']
    credential = site.credentials[source_config.credentials]
    fingerprint = stream_fingerprint(
        site.server,
        source_config.credentials,
        credential.script_name,
    )
    state_path = source_database_path(tmp_path)
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
        group=group,
        plugin=plugin,
        provider=_restart_provider(provider_state),
        site=site,
        socket_path=socket_path,
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
    group = group_config('remote', worker='pool')
    site = runtime_site(group)
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
                coordination_deadline_secs=stall_deadline('coordination', 1.0),
                stop=threading.Event(),
                wait_phase=nullcontext,
                wait_slice_secs=0.1,
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
                                            **runtime_event(event_id),
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
                writer.sendall(b''.join(part for frame in frames for part in frame.parts))
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
        assignment=SimpleNamespace(conditional_park=None, group_generation=7, start_mode='resume'),
        group=SimpleNamespace(source='events'),
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
        group_config('healthy', worker='pool'),
        group_config('remote', worker='pool'),
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
    config = runtime_config(tmp_path, site)
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
        provider_loader(_initialization_outage_provider(provider_state)),
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
    source_session = start_source_runtime(
        config,
        site_spec,
        source_assignment,
        grants,
    )
    try:
        assert provider_state.initialization_attempted.wait()
        assert source_session.runtime.fatal_reason is None
        dependent_parent_socket, dependent_child_socket = socket.socketpair()
        dependent_parent = WorkerControl(
            dependent_parent_socket,
            max_frame_bytes=131_072,
        )
        dependent_child = WorkerControl(
            dependent_child_socket,
            max_frame_bytes=131_072,
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
            source_endpoints=(source_session.endpoint,),
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
            drain = start_control_drain(dependent_parent)
            _assert_remote_initialization_pending(
                dependent_runtime,
                pending_results,
                pending_remote,
            )
            provider_state.initialization_ready.set()
            _assert_initial_endpoint_delivery(
                dependent_runtime,
                provider_state,
                invoked,
            )
        finally:
            provider_state.initialization_ready.set()
            release_failed_source_runtime(
                dependent_runtime,
                (dependent_parent, dependent_child),
                (dependent_parent_socket, dependent_child_socket),
                drain,
            )
    finally:
        provider_state.initialization_ready.set()
        source_session.close()


def test_remote_source_restart_rejoins_without_stale_or_gap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    groups = (
        group_config('healthy', worker='pool'),
        group_config('remote', worker='pool'),
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
    config = runtime_config(tmp_path, site)
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
        provider_loader(_restart_provider(provider_state)),
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
    first_source = start_source_runtime(
        config,
        site_spec,
        source_assignment,
        grants,
    )
    dependent_parent_socket, dependent_child_socket = socket.socketpair()
    dependent_parent = WorkerControl(dependent_parent_socket, max_frame_bytes=131_072)
    dependent_child = WorkerControl(dependent_child_socket, max_frame_bytes=131_072)
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
        source_endpoints=(first_source.endpoint,),
        event_grants=grants,
    )
    dependent_runtime = build_worker_runtime(
        config,
        dependent_spec,
        dependent_assignment,
        dependent_child,
    )
    dependent_runtime.start()
    drain_stop, drain_thread = start_control_drain(dependent_parent)
    try:
        _assert_source_restart_continuity(
            _RestartContinuity(
                callback_entered=callback_entered,
                config=config,
                dependent_runtime=dependent_runtime,
                first_source=first_source,
                grants=grants,
                invoked=invoked,
                provider_state=provider_state,
                release_callback=release_callback,
                site_spec=site_spec,
                source_assignment=source_assignment,
            ),
            monkeypatch,
        )
    finally:
        release_callback.set()
        release_failed_source_runtime(
            dependent_runtime,
            (dependent_parent, dependent_child),
            (dependent_parent_socket, dependent_child_socket),
            (drain_stop, drain_thread),
            (first_source.close,) if first_source.runtime._resources_open else (),
        )


def test_removed_pending_group_leaves_no_stale_entry(tmp_path: Path) -> None:
    with _pending_group_health_probe(tmp_path) as (component, cache, state):
        surface = cast('_SourceJoinSurface', SimpleNamespace(state=state))
        component._publish_pending_groups(surface)
        assert tuple(group.group for group in cache.snapshot().groups) == ('local',)
        component._assignment = replace(component._assignment, source_group_assignments=())
        component._publish_pending_groups(surface)
        assert cache.snapshot().groups == ()


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
        lifecycle.joined = False

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

        monkeypatch.setattr(resources.coordination, 'service', coordinate)
        monkeypatch.setattr(component, '_pause', pause)
        assert not component._resume_lane(resources, lifecycle)
        assert state.snapshot().status == 'parked'


def test_return_to_running_forces_the_next_tick(monkeypatch: pytest.MonkeyPatch) -> None:
    component_object, resources_object = lifecycle_probe(
        'sync', durable_status='parked', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(channel=resources.channel)
    component._lifecycle = lifecycle
    calls: list[tuple[bool, bool]] = []
    monkeypatch.setattr(
        resources.coordination,
        'service',
        lambda *, scan_obligations=True, force=False: calls.append((scan_obligations, force)),
    )
    component._coordinate(resources, lifecycle, scan=False, force=False)
    lifecycle.next_coordination_at = time.monotonic() + 60.0
    vars(resources.state)['status'] = 'running'
    component._coordinate(resources, lifecycle, scan=True, force=False)
    assert calls == [(False, False), (True, True)]
    assert lifecycle.was_running


def test_running_monitor_transports_source_and_lane_stall_flags(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    group = group_config('local', worker='default', ordering='eager')
    site = runtime_site(group)
    config = runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        local_assignment(spec, group.name, 'initialize'),
        runtime_provider(),
    ) as (runtime, control):
        initial = receive_typed_health(
            control, lambda update: bool(update.sources and update.groups)
        )
        source = runtime._components['source:events']
        lane = runtime._components['lane:local']
        assert isinstance(source, _SourceComponent)
        assert isinstance(lane, _LaneComponent)
        source._conditions.set('checkpoint-stalled')
        source._conditions.set('write-lock-stalled')
        lane._conditions.set('write-lock-stalled')
        set_at = time.monotonic()
        stalled = receive_typed_health(
            control,
            lambda update: (
                update.worker.heartbeat_at >= set_at and bool(update.sources and update.groups)
            ),
        )
        assert stalled.sources[0].conditions == ('write-lock-stalled', 'checkpoint-stalled')
        assert stalled.groups[0].conditions == ('write-lock-stalled',)
        assert stalled.sources[0].frontier == initial.sources[0].frontier
        assert stalled.groups[0].cursor == initial.groups[0].cursor
        source._conditions.clear('checkpoint-stalled')
        source._conditions.clear('write-lock-stalled')
        lane._conditions.clear('write-lock-stalled')
        clear_at = time.monotonic()
        cleared = receive_typed_health(
            control,
            lambda update: (
                update.worker.heartbeat_at >= clear_at and bool(update.sources and update.groups)
            ),
        )
        assert cleared.sources[0].conditions == ()
        assert cleared.groups[0].conditions == ()


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
        module, 'start_control_drain', Mock(return_value=(threading.Event(), threading.Thread()))
    )

    def stop_drain(*_args: object) -> None:
        attempts.append('drain')

    monkeypatch.setattr(module, 'stop_control_drain', stop_drain)

    def stop(_runtime: object) -> None:
        attempts.append('runtime')
        raise AssertionError('runtime cleanup failed')

    monkeypatch.setattr(module, '_stop_runtime', stop)
    initiating = ValueError('body failed')
    with (
        pytest.raises(ValueError, match='body failed') as caught,
        running_concrete_runtime(
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
        kind=_WorkerKind.SITE,
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
    forced: list[int] = []
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        operator_deadline_secs_by_source={'events': stall_deadline('coordination', 1.0)},
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
        assert component.started.wait()
        send_control(parent, command)
        assert component.operator_entered.wait()
        send_control(parent, StopCommand(reason='operator-stop'))
        result = receive_control(parent)
        _expire_shutdown(runtime, {'operator-executor'})
        stuck = runtime.wait()
        resources_open = runtime._resources_open
        closed_before_release = any(operation.startswith('close:') for operation in operations)
    finally:
        component.operator_release.set()
        component.release.set()
        operator_thread.join()
        cleanup_result = runtime._shutdown()
        parent.close()
        child.close()

    assert result == CommandResult(
        command='adopt-source-stream',
        ok=False,
        message='operator command interrupted by worker shutdown',
        request_id='operator-runtime-stuck',
        result={'outcome': 'unknown'},
    )
    assert forced == [1]
    assert stuck == ('operator-executor',)
    assert runtime._stuck_threads == ('operator-executor',)
    assert resources_open
    assert not closed_before_release
    assert cleanup_result == ()
    assert operations[-1] == 'close:source:events'


def test_runtime_blocking_operator_keeps_heartbeats_and_stop_responsive() -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=_WorkerKind.SITE,
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', operations),
        health_sink=health.append,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        operator_deadline_secs_by_source={'events': stall_deadline('coordination', 1.0)},
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
        assert component.started.wait()
        send_control(parent, command)
        assert component.operator_entered.wait()
        initial_health_count = len(health)
        while True:
            if len(health) >= initial_health_count + 2:
                break
            time.sleep(0.001)
        health_during_operator = tuple(health[initial_health_count:])
        send_control(parent, StopCommand(reason='operator-stop'))
        with suppress(TimeoutError):
            result = receive_control(parent)
    finally:
        component.operator_release.set()
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
        result={'outcome': 'unknown'},
    )
    assert shutdown_result == ()
    assert forced == []


def test_runtime_busy_operator_preserves_single_result_publication() -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=_WorkerKind.SITE,
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    health_published = threading.Event()

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: health_published.set(),
        health_interval_secs=60.0,
        checkpoint_interval_secs=1.0,
        operator_deadline_secs_by_source={'events': stall_deadline('coordination', 1.0)},
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
        assert component.started.wait()
        assert health_published.wait()

        health_published.clear()
        send_control(parent, command_a)
        monitor_wake.set()
        assert component.operator_entered.wait()
        assert health_published.wait()

        health_published.clear()
        send_control(parent, command_b)
        monitor_wake.set()
        assert receive_control(parent) == CommandResult(
            command='adopt-source-stream',
            ok=False,
            message='operator command executor is busy',
            request_id='operator-runtime-b',
            result={},
        )
        assert health_published.wait()

        health_published.clear()
        component.operator_release.set()
        assert receive_control(parent) == CommandResult(
            command='adopt-source-stream',
            ok=True,
            message='accepted',
            request_id='operator-runtime-a',
            result={
                'owner': 'events',
                'command': 'adopt-source-stream',
            },
        )
        assert health_published.wait()

        health_published.clear()
        monitor_wake.set()
        assert health_published.wait()
        with pytest.raises(TimeoutError):
            parent.receive(timeout=0.0)
    finally:
        component.operator_release.set()
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

    component = InterruptibleComponent(_run, stop.set)
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=_WorkerKind.SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    forced: list[int] = []
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda _name, _slots, _host: component,
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
        force_exit=forced.append,
    )

    runtime.start()
    try:
        assert entered.wait()
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        operator_deadline_secs_by_source={'events': stall_deadline('coordination', 1.0)},
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
        assert components['events'].started.wait()
        send_control(parent, command)

        result = receive_control(parent)
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
    assert 'operator:events:adopt-source-stream' in components['events'].operations


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
            assert published.wait(), 'monitor did not publish before start returned'
            assert thread.is_alive()

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='first-monitor-heartbeat',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: _Component(name, []),
        health_sink=publish,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
            group_config('default-a', worker='default'),
            group_config('default-b', worker='default'),
        )
    )
    spec = build_worker_specs(site)[0]
    operations: list[str] = []
    sources = {name: _Component(f'source:{name}', operations) for name in spec.sources}
    lanes = {name: _Component(f'lane:{name}', operations) for name in spec.groups}
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda name, _slots, _host: lanes[name],
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
    )

    runtime.start()
    try:
        assert all(component.started.wait() for component in (*sources.values(), *lanes.values()))
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
    site = _site((group_config('local', worker='default'),))
    spec = build_worker_specs(site)[0]
    operations: list[str] = []
    prepared = threading.Event()

    class _PreparationComponent(_Component):
        def run(self) -> None:
            operations.append(f'observe:{self._name}:{prepared.is_set()}')
            super().run()

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

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=source_factory,
        lane_factory=lane_factory,
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
        prepare=prepare,
    )

    runtime.start()
    components = cast('dict[str, _PreparationComponent]', runtime._components)
    try:
        assert all(component.started.wait() for component in components.values())
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

    class _CloseFailureHandle(RuntimeProviderHandle):
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

    group = group_config('local', worker='default')
    site = runtime_site(group)
    provider = Provider(
        name='teardown-failure',
        capabilities=ProviderCapabilities(
            sync=True,
            async_=False,
        ),
        sync_factory=factory,
        async_factory=None,
    )
    host = PluginHost(
        site,
        group,
        provider,
        (health_failure_plugin('callback'),),
        heartbeat=lambda: None,
        shutdown_requested=lambda: False,
    )
    assert host.setup_sync() is None
    entered = threading.Event()

    def _close_host() -> None:
        entered.set()
        host.close_sync()

    fatal_reason, forced = run_cooperative_stop_component(
        InterruptibleComponent(_close_host, lambda: None),
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
            group_config('broken', worker='default'),
            group_config('healthy', worker='default'),
        )
    )
    spec = build_worker_specs(site)[0]
    component, resources = lifecycle_probe(
        'sync',
        durable_status='running',
        setup_failure=RuntimeError('setup-failed'),
    )
    broken = _LifecycleComponent(component, resources)
    healthy = _Component('healthy', [])
    sources = {name: _Component(name, []) for name in spec.sources}
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda name, _slots, _host: broken if name == 'broken' else healthy,
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
    )
    runtime.start()
    try:
        assert resources.coordination.serviced.wait()
        assert healthy.started.wait()
        # Negative-only observation: setup failure must never stop the healthy lane.
        time.sleep(0.01)
        assert runtime.fatal_reason is None
        assert not healthy.finished.is_set()
    finally:
        runtime.request_stop('test')
        assert runtime.wait() == ()


def test_setup_failure_remains_group_local_and_services_body_free_work() -> None:
    component, resources = lifecycle_probe(
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
            assert resources.coordination.serviced.wait()
            assert not future.done()
            assert resources.lane.joined == 0
            assert resources.coordination.calls
            assert not any(resources.coordination.calls)
        finally:
            stop.set()
        future.result()


def test_setup_halt_warns_once_until_recovered(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    component, raw_resources = lifecycle_probe(
        'sync', durable_status='running', setup_failure=RuntimeError('setup failed')
    )
    resources = cast('_LaneResources', raw_resources)
    lifecycle = worker_module._LaneLifecycle(channel=resources.channel)

    def pause(_resources: object) -> None:
        pass

    monkeypatch.setattr(component, '_pause', pause)
    caplog.set_level(logging.WARNING, logger='sg_event_daemon')
    for _index in range(2):
        assert component._callback_gate_snapshot(resources, lifecycle) is None
        component._drive_halted(resources, lifecycle)
    assert caplog.messages == ['group halted: callback-failed; delivery stops until it is cleared']


def test_shutdown_closes_reverse_order_and_monitor_forces_wedged_exit() -> None:
    site = _site((group_config('default', worker='default'),))
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

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: sources[name],
        lane_factory=lambda _name, _slots, _host: lane,
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
        force_exit=lambda code: forced.append((code, threading.current_thread().name)),
        monitor_gate=monitor_gate,
    )
    runtime.start()
    try:
        assert all(source.started.wait() for source in sources.values())
        assert lane.started.wait()
        assert monitor_entered.wait()
        runtime.request_stop('control-eof')
        assert all(source.finished.wait() for source in sources.values())
        _expire_shutdown(runtime, {'lane:default'})
        release_monitor.set()
        assert runtime._monitor_thread is not None
        runtime._monitor_thread.join()
        assert forced == [(1, 'sg-event-daemon:monitor')]
        assert runtime._stuck_threads == ('lane:default',)
        assert 'close:lane:default' not in operations
        lane.release.set()
        assert lane.finished.wait()
    finally:
        release_monitor.set()
        lane.release.set()
        for source in sources.values():
            source.release.set()
        runtime.request_stop('test-cleanup')
        for thread in runtime._threads.values():
            thread.join()
        assert runtime._shutdown() == ()

    assert operations[-3:] == [
        'close:lane:default',
        'close:source:events',
        'close:source:audit',
    ]


def test_site_worker_exists_without_default_groups() -> None:
    site = _site((group_config('named', worker='pool'),))

    specs = build_worker_specs(site)

    assert specs[0].kind is _WorkerKind.SITE
    assert specs[0].groups == ()
    assert specs[0].sources == ('audit', 'events')


def test_slow_coordination_return_allows_catchup_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    component = _unopened_group_component(_LaneComponent)
    group = group_config('local', worker='default')
    site = runtime_site(group)
    invoked: list[int] = []
    with (
        _open_group_health_state(tmp_path / 'group.db') as state,
        closing(
            GroupLane(
                state=state,
                site=site,
                group=group,
                provider=runtime_provider(),
                plugins=(_catchup_tick_plugin(clock, invoked),),
                worker_instance='worker-1',
                health_interval_secs=1.0,
                watchdog=component._watchdog,
            )
        ) as lane,
    ):
        assert lane.setup() is None
        channel = GroupChannel(group='local', ordering='eager', max_events=10, max_bytes=262_144)
        component._lifecycle = worker_module._LaneLifecycle(channel=channel)

        def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
            del force
            assert not scan_obligations
            clock[0] += 2.0

        resources = cast(
            '_LaneResources',
            SimpleNamespace(
                state=state,
                lane=lane,
                client=SimpleNamespace(),
                context=SimpleNamespace(source_config=SimpleNamespace(fetch_interval_secs=1.0)),
                coordination=SimpleNamespace(service=coordinate),
            ),
        )
        snapshot, consumed = component._deliver_catchup_page(
            resources, (Event(runtime_event(42)),)
        )
        assert (snapshot.cursor, consumed, invoked) == (42, 1, [42])
        assert component._lifecycle.next_coordination_at == 103.0
        assert clock[0] == 102.25


def test_sole_strict_group_catches_up_after_a_burst_larger_than_its_buffer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    start_mode = 'resume'
    group = group_config('remote', worker='dedicated', ordering='strict')
    logger = logging.getLogger('sg_event_daemon')
    monkeypatch.setattr(logger, 'handlers', [caplog.handler])
    monkeypatch.setattr(logger, 'propagate', False)
    caplog.set_level(logging.INFO, logger='sg_event_daemon')
    site = _runtime_delivery_site(group, start_mode)
    site = replace(
        site,
        sources=MappingProxyType(
            {'events': replace(site.sources['events'], strict_max_events=50, batch_size=200)}
        ),
    )
    prepare_runtime_state(tmp_path, site, group, start_mode)
    config = runtime_config(tmp_path, site)
    site_spec, dependent_spec = build_worker_specs(site)
    provider, provider_state, invoked = concrete_delivery_provider(monkeypatch, start_mode)
    _gate_burst_callbacks(monkeypatch)
    monkeypatch.setattr(worker_module, 'load_provider', provider_loader(provider))
    source_parent_socket, source_child_socket = socket.socketpair()
    source_parent = WorkerControl(source_parent_socket, max_frame_bytes=131_072)
    source_child = WorkerControl(source_child_socket, max_frame_bytes=131_072)
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
    )
    dependent_child = WorkerControl(
        dependent_child_socket,
        max_frame_bytes=131_072,
    )
    dependent_runtime: _WorkerRuntime | None = None
    dependent_drain_stop: threading.Event | None = None
    dependent_drain_thread: threading.Thread | None = None
    source_drain_stop: threading.Event | None = None
    source_drain_thread: threading.Thread | None = None
    try:
        source_runtime.start()
        endpoint = receive_type(source_parent, SourceEndpointReady)
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
            dependent_drain_stop, dependent_drain_thread = start_control_drain(dependent_parent)
        group_path = source_database_path(tmp_path)
        _wait_for_group_database(dependent_runtime, group_path, 'remote')
        _assert_strict_burst_delivery(
            dependent_runtime, group_path, provider_state, invoked, caplog
        )
        assert source_runtime.fatal_reason is None
    finally:

        def close_source_runtime() -> None:
            release_failed_source_runtime(
                source_runtime,
                (source_parent, source_child),
                (source_parent_socket, source_child_socket),
                (source_drain_stop, source_drain_thread)
                if source_drain_stop is not None and source_drain_thread is not None
                else None,
            )

        release_failed_source_runtime(
            dependent_runtime,
            (dependent_parent, dependent_child),
            (dependent_parent_socket, dependent_child_socket),
            (dependent_drain_stop, dependent_drain_thread)
            if dependent_drain_stop is not None and dependent_drain_thread is not None
            else None,
            (close_source_runtime,),
        )


def test_source_assignment_inventory_includes_groups_owned_by_remote_workers(
    tmp_path: Path,
) -> None:
    site = _site(
        (
            group_config('local', worker='default'),
            group_config('named-remote', worker='pool'),
            group_config('dedicated-remote', worker='dedicated', source='audit'),
        )
    )
    config = lift_config(
        Config(
            daemon=_DaemonConfig(
                state_dir=tmp_path,
                sg_api_provider='shotgun-api3',
            ),
            sites=MappingProxyType({site.name: site}),
        )
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
    control = WorkerControl(parent_socket, max_frame_bytes=131_072)
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
    cache.publish_source(durable, sample=published)
    entered = threading.Event()
    release = threading.Event()

    class _Server:
        def dispatch_join_requests(self, _surface: object) -> None:
            return None

        def poll(self, _timeout: float) -> None:
            entered.set()
            assert release.wait()

    def publish_health(*_args: object, **_options: object) -> None:
        return None

    monkeypatch.setattr(type(component), '_publish_health', publish_health)
    service = type(component)._service
    thread = threading.Thread(
        target=partial(
            service,
            component,
            cast('EventServer', _Server()),
            cast('_SourceJoinSurface', SimpleNamespace(reconcile_groups=lambda: None)),
            cast('SourcePoller', object()),
        ),
    )
    thread.start()
    try:
        assert entered.wait()
        snapshot = cache.snapshot()
        live = snapshot.watchdogs[0]
        merged = snapshot.sources[0]
        assert live.phase == merged.phase == 'internal'
        assert live.deadline_at == merged.deadline_at
        assert live.heartbeat_at == merged.heartbeat_at
        assert merged.frontier == durable.frontier
        assert merged.status == durable.status
        assert merged.last_error == durable.last_error
    finally:
        release.set()
        thread.join()
        assert not thread.is_alive()


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_source_component_disposes_pending_group_through_worker_control(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    command: Literal['replay', 'reset-to-latest'],
) -> None:
    config, spec, assignment, snapshot = pending_group_runtime(tmp_path)
    calls: list[str] = []
    with running_concrete_runtime(
        monkeypatch, config, spec, assignment, runtime_provider(calls=calls)
    ) as (runtime, control):
        receive_typed_health(control, lambda update: len(update.groups) == 1)
        pending = _pending_group_command(snapshot, command)
        request = replace(
            pending,
            arguments={**pending.arguments, 'operator_label': 'operator uid=501'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=456),
        )
        send_control(control, request)
        result = receive_type(control, CommandResult)
        assert result.ok, result.message
        assert result.request_id == request.request_id
        assert result.result['group_generation'] == 11
        assert result.result['cursor'] == (39 if command == 'replay' else 41)
        assert 'lane:local' not in runtime._threads
        assert runtime.fatal_reason is None
        with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
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
        send_control(control, replace(request, identity=replace(request.identity, pid=789)))
        assert receive_type(control, CommandResult) == result
        for changed in (
            replace(request, arguments={**request.arguments, 'operator_label': 'other uid=501'}),
            replace(
                request, arguments={**request.arguments, 'operator_label': 'operator uid=502'}
            ),
            replace(request, arguments={**request.arguments, 'reason': 'different recovery'}),
        ):
            send_control(control, replace(changed, identity=replace(request.identity, pid=789)))
            conflict = receive_type(control, CommandResult)
            assert not conflict.ok
            assert 'disposition identity has conflicting content' in conflict.message
        with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
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
        send_control(control, intervening)
        second = receive_type(control, CommandResult)
        assert second.ok, second.message
        assert second.result['group_generation'] == 12
        send_control(control, replace(request, identity=replace(request.identity, pid=789)))
        stale = receive_type(control, CommandResult)
        assert not stale.ok
        assert 'intervening disposition' in stale.message
        with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
            assert connection.execute('SELECT group_generation FROM groups').fetchone() == (12,)
            assert connection.execute(
                "SELECT COUNT(*) FROM audit WHERE command IN ('replay', 'reset-to-latest')"
            ).fetchone() == (2,)
        assert calls
        assert set(calls) == {'sg-event-daemon:source:events:query_0'}


def test_source_component_loop_drives_range_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = runtime_site(group_config('local', worker='default'))
    source_config = replace(
        site.sources['events'],
        fetch_interval_secs=0.001,
        late_event_window_secs=0.001,
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
    database_path = source_database_path(tmp_path)
    with SourceState.open(
        database_path,
        writes=write_context_for(database_path),
        create=True,
        site=site.name,
        source='events',
        stream_fingerprint=fingerprint,
        source_generation='source-generation-1',
        late_event_window_secs=source_config.late_event_window_secs,
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
        provider_loader(
            Provider(
                name='shotgun-api3',
                capabilities=ProviderCapabilities(
                    sync=True,
                    async_=False,
                ),
                sync_factory=factory,
                async_factory=None,
            )
        ),
    )
    messages: list[object] = []
    errors: list[Exception] = []
    host = CheckpointHost(
        logger=logging.getLogger('sg_event_daemon.source'),
        interval_secs=60,
        synchronous='NORMAL',
        on_failure=lambda reason: errors.append(RuntimeError(reason)),
    )
    component = worker_module._SourceComponent(
        checkpoint_host=host,
        assignment=local_assignment(spec, 'local', 'resume'),
        config=runtime_config(tmp_path, site),
        control=cast('WorkerControlOutbox', SimpleNamespace(enqueue=messages.append)),
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
            handle.recheck_returned.is_set,
            errors,
        )
        _wait_for_source_component_progress(
            range_closed,
            errors,
            pump=lambda: messages.extend(channel.drain()),
        )
    finally:
        primary_error = sys.exception()
        component.request_stop()
        thread.join()
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
        late_event_window_secs=source_config.late_event_window_secs,
    ) as state:
        ranges = state.snapshot().ranges
        assert ranges
        assert all(item.state == 'closed' for item in ranges)


def test_source_component_pending_disposition_checks_configured_stream_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = pending_group_runtime(tmp_path)
    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        runtime,
        control,
    ):
        receive_typed_health(control, lambda update: len(update.groups) == 1)
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
        with closing(sqlite3.connect(source_database_path(tmp_path))) as connection, connection:
            before = tuple(connection.iterdump())
        send_control(control, _pending_group_command(snapshot, 'replay'))
        result = receive_type(control, CommandResult)
        assert not result.ok
        with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
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
    config, spec, assignment, snapshot = pending_group_runtime(tmp_path)
    with (
        _open_group_health_state(source_database_path(tmp_path)) as owner,
        running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
            runtime,
            control,
        ),
    ):
        receive_typed_health(control, lambda update: len(update.groups) == 1)
        send_control(control, _pending_group_command(snapshot, 'replay'))
        result = receive_type(control, CommandResult)
        assert not result.ok
        assert result.result == {'code': 'group-busy'}
        assert owner.snapshot() == snapshot
        assert runtime.fatal_reason is None


def test_source_component_pending_disposition_uses_operator_database_budget(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        write_lock_module, 'OPERATOR_WRITE_BUDGET_SECS', OPERATOR_WRITE_BUDGET_SECS
    )
    config, spec, assignment, snapshot = pending_group_runtime(tmp_path)
    operator_budgets: list[float] = []
    expired_budgets: list[float] = []
    blocker_active = threading.Event()
    original_transaction = worker_module.SourceWriteLock.transaction

    @contextmanager
    def transaction(
        lock: SourceWriteLock,
        connection: sqlite3.Connection,
        **options: Unpack[_TransactionOptions],
    ) -> Generator[sqlite3.Connection]:
        budget = options.get('budget_secs')
        if budget is not None:
            operator_budgets.append(budget)
        if budget is None or not blocker_active.is_set():
            with original_transaction(lock, connection, **options) as opened:
                yield opened
            return

        original_clock = lock.clock
        started = original_clock()
        now = [started]

        def expire_sqlite_admission(statement: str) -> None:
            # The configured SQLite wait marks the actual acquisition boundary.
            # Step this lock alone before BEGIN can wait on the held writer.
            if statement.startswith('PRAGMA busy_timeout=') and now[0] == started:
                now[0] = started + budget
                expired_budgets.append(budget)

        lock.clock = lambda: now[0]
        connection.set_trace_callback(expire_sqlite_admission)
        try:
            with original_transaction(lock, connection, **options) as opened:
                yield opened
        finally:
            connection.set_trace_callback(None)
            lock.clock = original_clock

    monkeypatch.setattr(worker_module.SourceWriteLock, 'transaction', transaction)
    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        runtime,
        control,
    ):
        receive_typed_health(
            control,
            lambda update: (
                len(update.groups) == 1 and any(item.phase == 'wait' for item in update.sources)
            ),
        )
        with closing(sqlite3.connect(source_database_path(tmp_path))) as blocker:
            blocker.execute('BEGIN IMMEDIATE')
            blocker_active.set()
            try:
                send_control(control, _pending_group_command(snapshot, 'replay'))
                result = receive_type(control, CommandResult)
            finally:
                blocker_active.clear()
                blocker.rollback()
        assert not result.ok
        assert result.result == {'code': 'source-database-busy'}
        assert operator_budgets == [2.0]
        assert expired_budgets == [2.0]
        assert runtime.fatal_reason is None
        send_control(control, _pending_group_command(snapshot, 'replay'))
        assert receive_type(control, CommandResult).ok


def test_source_control_saturation_keeps_producer_nonblocking(tmp_path: Path) -> None:
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
    peer = WorkerControl(peer_socket, max_frame_bytes=131_072)
    sender = WorkerControl(sender_socket, max_frame_bytes=131_072)
    vars(source_component)['_control'] = WorkerControlOutbox(sender)
    request_grant = cast('_GrantRequester', source_component).request_grant
    request_grant(
        EventPeerGrant(
            source='events',
            group='remote',
            dependent_spawn_instance='dependent-saturation-1',
            capability='saturated-source-grant',
        )
    )

    def _action() -> None:
        with _open_coordination_source(tmp_path / 'source-control.db') as state:
            state.initialize(100, incarnation='incarnation-1')
            with _source_health_poller(state, runtime_provider()) as poller:
                _service_source_health(source_component, _GrantServer(), poller)
        runtime.request_stop('producer-complete')

    spec = WorkerSpec(
        worker_id='["source","saturation"]',
        site='main',
        kind=_WorkerKind.SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='source-saturation-1',
        source_factory=lambda _name, _slots, _host: _ControlSendComponent(_action),
        lane_factory=lambda _name, _slots, _host: _Component('lane', []),
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
    )
    _saturate_worker_control_sender(sender)
    try:
        runtime.start()
        assert runtime.wait() == ()
        assert runtime.fatal_reason is None
    finally:
        runtime.request_stop('test-cleanup')
        runtime.wait()
        peer.close()
        sender.close()


def test_source_disposition_uses_current_incarnation_after_adoption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = pending_group_runtime(tmp_path)
    path = source_database_path(tmp_path)
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
    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        _runtime,
        control,
    ):
        receive_typed_health(control, lambda update: len(update.groups) == 1)
        send_control(control, _pending_group_command(snapshot, 'reset-to-latest'))
        result = receive_type(control, CommandResult)
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
    provider = runtime_provider()
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
    provider = runtime_provider()
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
                component._mailbox.submit(initialize),
                component._mailbox.submit(advance_observations),
            )

            _service_source_health(component, server, poller)
            assert all(call.complete.is_set() and call.error is None for call in calls)
            health = cache.snapshot().sources[0]
            assert health.frontier == 101
            assert health.successful_clock == 2.0

    assert snapshot_calls == 3


def test_source_health_refresh_uses_interval_deadlines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component, cache = _source_health_component(tmp_path, health_interval_secs=1.0)
    server = _SourceHealthServer()
    provider = runtime_provider()
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
            initial = cache.snapshot().sources
            assert snapshot_calls == 1
            assert vars(component)['_next_health_refresh_at'] == 1.0

            for pass_number in range(1, 100):
                clock.now = pass_number / 100
                _service_source_health(component, server, poller)

            assert snapshot_calls == 1
            refreshed = cache.snapshot().sources
            assert len(refreshed) == len(initial) == 1
            assert (
                replace(
                    refreshed[0],
                    heartbeat_at=initial[0].heartbeat_at,
                    deadline_at=initial[0].deadline_at,
                )
                == initial[0]
            )
            assert refreshed[0].deadline_at == component._watchdog.sample().deadline_at

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
    provider = runtime_provider()
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

            call = component._mailbox.submit(initialize)
            clock.now = 0.1

            _service_source_health(component, server, poller)

    assert call.complete.is_set()
    assert snapshot_calls == 2
    assert cache.snapshot().sources[0].initialized


def test_source_owner_disposition_rejects_nonint_position(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = pending_group_runtime(tmp_path)
    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        runtime,
        control,
    ):
        receive_typed_health(control, lambda update: len(update.groups) == 1)
        source = runtime._components['source:events']
        assert isinstance(source, _SourceComponent)
        command = _pending_group_command(snapshot, 'replay')
        command = replace(command, arguments={**command.arguments, 'from_event_id': 1.5})
        with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
            before = connection.execute(
                "SELECT * FROM groups WHERE group_name = 'local'"
            ).fetchone()
            with pytest.raises(ValueError, match='replay position must be a positive event ID'):
                source._execute_group_disposition(command, wait_phase=nullcontext)
            assert (
                connection.execute("SELECT * FROM groups WHERE group_name = 'local'").fetchone()
                == before
            )


def test_source_owner_disposition_retry_with_new_pid_after_restart(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, snapshot = pending_group_runtime(tmp_path)
    pending = _pending_group_command(snapshot, 'reset-to-latest')
    command = replace(
        pending,
        arguments={**pending.arguments, 'operator_label': 'operator uid=501'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        _runtime,
        control,
    ):
        receive_typed_health(control, lambda update: len(update.groups) == 1)
        send_control(control, command)
        first = receive_type(control, CommandResult)
        assert first.ok, first.message

    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        _runtime,
        control,
    ):
        receive_typed_health(control, lambda update: len(update.groups) == 1)
        send_control(control, replace(command, identity=replace(command.identity, pid=789)))
        repeated = receive_type(control, CommandResult)
        assert repeated.ok, repeated.message
        assert repeated.result == first.result
    with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
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
        writes: WriteContext | None = None

        _error: str | None = None
        name = ''

        def __init__(self, **_options: object) -> None:
            pass

        def close(self) -> None:
            assert cache.snapshot().watchdogs
            watchdog = component._watchdog.sample()
            assert watchdog.phase == 'shutdown'
            assert watchdog.deadline_at is not None
            operations.append(f'close:{self.name}')
            if self.writes is not None:
                self.writes.lock.close()
            if self._error is not None:
                raise RuntimeError(self._error)

    state = _Resource()
    state.name = 'state'

    def open_state(*_args: object, **_options: object) -> _Resource:
        state.writes = cast('WriteContext', _options['writes'])
        return state

    def load_test_provider(*_args: object, **_options: object) -> object:
        return object()

    def publish_health(*_args: object, **_options: object) -> None:
        return None

    class _Server(_Resource):
        _error = 'server-close-failed'
        name = 'server'
        start = staticmethod(partial(operations.append, 'start:server'))
        poll = staticmethod(publish_health)

    class _Poller(_Resource, AbstractContextManager['_Poller']):
        _error = 'poller-close-failed'
        name = 'poller'

        def __exit__(
            self,
            _exc_type: type[BaseException] | None,
            _exc_value: BaseException | None,
            _traceback: object,
        ) -> None:
            self.close()

    original_deregister = cache.deregister_watchdog

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

    monkeypatch.setattr(cache, 'deregister_watchdog', deregister)
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
        vars(type(component))['run'],
    )
    with pytest.raises(RuntimeError, match='poller-close-failed'):
        run_component(component)

    assert cache.snapshot().watchdogs
    cast('Callable[[object], None]', vars(type(component))['close'])(component)

    assert operations == [
        'start:server',
        'close:poller',
        'close:server',
        'close:state',
        'deregister',
    ]
    assert cache.snapshot().watchdogs == ()


def test_source_registration_outlives_partial_construction_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    component, cache = _source_health_component(tmp_path, health_interval_secs=1.0)
    operations: list[str] = []

    class _Resource:
        writes: WriteContext | None = None

        def __init__(self, name: str, error: str | None = None) -> None:
            self._error = error
            self._name = name

        def close(self) -> None:
            assert cache.snapshot().watchdogs
            watchdog = component._watchdog.sample()
            assert watchdog.phase == 'shutdown'
            assert watchdog.deadline_at is not None
            operations.append(f'close:{self._name}')
            if self.writes is not None:
                self.writes.lock.close()
            if self._error is not None:
                raise RuntimeError(self._error)

    state = _Resource('state')

    def open_state(*_args: object, **_options: object) -> _Resource:
        state.writes = cast('WriteContext', _options['writes'])
        return state

    def load_test_provider(*_args: object, **_options: object) -> object:
        return object()

    def fail_server(**_options: object) -> object:
        raise RuntimeError('server-construction-failed')

    original_deregister = cache.deregister_watchdog

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

    monkeypatch.setattr(cache, 'deregister_watchdog', deregister)
    monkeypatch.setattr(
        worker_module,
        'SourceState',
        SimpleNamespace(open=open_state),
    )
    monkeypatch.setattr(worker_module, 'EventServer', fail_server)
    monkeypatch.setattr(worker_module, 'load_provider', load_test_provider)

    run_component = cast(
        'Callable[[object], None]',
        vars(type(component))['run'],
    )
    with pytest.raises(RuntimeError, match='server-construction-failed'):
        run_component(component)

    assert cache.snapshot().watchdogs
    cast('Callable[[object], None]', vars(type(component))['close'])(component)

    assert operations == ['close:state', 'deregister']
    assert cache.snapshot().watchdogs == ()


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
        runtime_site(group_config('local', worker='default')),
    )
    source_config = site.sources['events']

    class _Handle(RuntimeProviderHandle):
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
        sync_factory=factory,
        async_factory=None,
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
        snapshot = cache.snapshot()
        updates.append(
            HealthUpdate(
                worker=replace(worker, monitor_alive=True),
                watchdogs=snapshot.watchdogs,
                sources=snapshot.sources,
                groups=snapshot.groups,
                event_sockets=snapshot.event_sockets,
            )
        )

    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=_WorkerKind.SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
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
        'Callable[[_WorkerRuntime, float], None]',
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
            degraded_status = status_from_health(
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
            recovered_status = status_from_health(
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
        kind=_WorkerKind.SITE,
        name='main',
        sources=('events',),
        groups=(),
    )
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
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
        '_WorkerRuntime',
        SimpleNamespace(start=lambda: None, _threads={}, _monitor_thread=None),
    )
    drain: list[tuple[threading.Event, threading.Thread]] = []
    cleanup_attempts: list[str] = []
    responses = iter((endpoint, EventPeerReady('events', 'local', 'dependent-spawn-1')))
    original_start_drain = start_control_drain
    original_stop_drain = stop_control_drain

    def receive(_control: WorkerControl, _expected: type[object]) -> object:
        return next(responses)

    def build_runtime(*_args: object) -> _WorkerRuntime:
        return runtime

    def start_drain(control: WorkerControl) -> tuple[threading.Event, threading.Thread]:
        owned = original_start_drain(control)
        drain.append(owned)
        return owned

    def fail_session(**_kwargs: object) -> NoReturn:
        raise RuntimeError('injected session acquisition failure')

    def fail_runtime_stop(_runtime: _WorkerRuntime | None) -> NoReturn:
        cleanup_attempts.append('runtime')
        raise AssertionError('injected runtime cleanup failure')

    module = sys.modules[__name__]
    monkeypatch.setattr(socket, 'socketpair', lambda: (parent_socket, child_socket))
    monkeypatch.setattr(module, 'build_worker_runtime', build_runtime)
    monkeypatch.setattr(module, 'receive_type', receive)
    monkeypatch.setattr(module, 'start_control_drain', start_drain)
    monkeypatch.setattr(module, '_SourceRuntimeSession', fail_session)
    monkeypatch.setattr(module, '_stop_runtime', fail_runtime_stop)
    try:
        with pytest.raises(RuntimeError, match='injected session acquisition failure') as caught:
            start_source_runtime(
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
        runtime_thread.join()
        return ()

    runtime = cast(
        '_WorkerRuntime',
        SimpleNamespace(
            start=start,
            request_stop=request_stop,
            wait=wait,
            _threads={'source:events': runtime_thread},
            _monitor_thread=None,
        ),
    )

    def build_runtime(*_args: object) -> _WorkerRuntime:
        return runtime

    def fail_receive(_control: WorkerControl, _expected: type[object]) -> NoReturn:
        raise RuntimeError('injected readiness failure')

    def ignore_monitor(_runtime: _WorkerRuntime) -> None:
        return None

    module = sys.modules[__name__]
    monkeypatch.setattr(socket, 'socketpair', lambda: (parent_socket, child_socket))
    monkeypatch.setattr(module, 'build_worker_runtime', build_runtime)
    monkeypatch.setattr(module, 'receive_type', fail_receive)
    monkeypatch.setattr(module, '_assert_monitor_stopped', ignore_monitor)
    try:
        with pytest.raises(RuntimeError, match='injected readiness failure'):
            start_source_runtime(
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
            runtime_thread.join()
        parent_socket.close()
        child_socket.close()


def test_strict_gap_with_two_late_rows_delivers_each_once_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def find(
        _handle: _GapProviderHandle,
        _entity: str,
        filters: object,
        _fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        if filters == []:
            return [{'id': 1}]
        if filters == [['id', 'greater_than', 100]]:
            return [runtime_event(104)]
        if filters == [['id', 'greater_than', 100], ['id', 'less_than', 104]]:
            return [runtime_event(101), runtime_event(103)]
        return EventLogSG((runtime_event(101), runtime_event(103))).find(
            _entity, filters, _fields, **options
        )

    monkeypatch.setattr(_GapProviderHandle, 'find', find)
    provider = _gap_runtime_provider()
    group = replace(
        group_config('local', worker='default', ordering='strict'), strict_holdback_secs=1.0
    )
    site = runtime_site(group)
    invoked: list[int] = []
    plugin = health_failure_plugin('callback')

    def callback(context: EventContext) -> None:
        event_id = context.event['id']
        assert type(event_id) is int
        invoked.append(event_id)

    plugin = replace(plugin, callbacks=(replace(plugin.callbacks[0], callback=callback),))
    path = tmp_path / 'source.db'
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000, group_holdbacks={'local': 1.0})
    fanout = EventFanout()
    channel = GroupChannel(group='local', ordering='strict', max_events=20, max_bytes=10_000_000)
    with _open_coordination_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')
        with open_group_on_source(
            path,
            writes=write_context_for(path),
            site='main',
            site_server_url=site.server,
            source='events',
            group='local',
            group_generation=7,
            ordering='strict',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='initialize',
            initial_cursor=100,
            initial_delivery_floor=100,
            initial_last_delivered_id=100,
            strict_holdback_secs=1.0,
        ) as state:
            watchdog = ComponentWatchdog(
                kind='lane',
                identity='local',
                worker_instance='worker-1',
                registration_token=_watchdog_token(),
                startup_deadline_secs=30.0,
            )
            with closing(
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
            ) as lane:
                assert lane.setup() is None
                coordinator = JoinCoordinator(
                    state=source,
                    fanout=fanout,
                    source_generation='source-1',
                    strict_retention=strict,
                )
                lane.join(coordinator, channel, catch_up=lambda _boundary: None)
                with SourcePoller(
                    state=source,
                    fanout=fanout,
                    provider=provider,
                    site_url=site.server,
                    credential=site.credentials['source'],
                    config=site.sources['events'],
                    max_ipc_frame_bytes=8_388_608,
                    source_generation='source-1',
                    strict_buffer=strict,
                    internal_deadline_secs=30.0,
                    wait_tolerance_secs=5.0,
                    random_value=lambda: 0.5,
                    waiter=SourceHealthWaiter(),
                    successful_clock=_SuccessfulQueryClock(clock=lambda: 0.0),
                    watchdog=ComponentWatchdog(
                        kind='source',
                        identity='events',
                        worker_instance='worker-1',
                        registration_token=_watchdog_token(),
                        startup_deadline_secs=30.0,
                    ),
                ) as poller:
                    assert poller._poll_once() == 1
                    retained = source.snapshot().ranges[0]
                    with closing(sqlite3.connect(path)) as connection:
                        row = connection.execute(
                            'SELECT visibility_deadline FROM discovery_ranges '
                            'WHERE obligation_id = ?',
                            (retained.obligation_id,),
                        ).fetchone()
                    assert row is not None
                    source.record_successful_query(float(row[0]))
                    assert poller._recheck_once(retained.obligation_id) == 2
                    with CatchupReader(
                        provider=provider,
                        site_url=site.server,
                        credential=site.credentials['source'],
                        batch_size=20,
                        fetch_interval_secs=1.0,
                        waiter=SourceHealthWaiter(),
                    ) as reader:
                        refresh = LaneRefresh(
                            joined=lambda: False,
                            fetch_interval_secs=1,
                            late_event_window_secs=300,
                            log_extra={},
                            lane=lane,
                            slot=partial(_held_limiter_slot, _CatchupLimiter(1)),
                            reader=reader,
                            state=state,
                        )
                        refresh.service()
                    assert invoked == [101, 103]
                    completed = lane.drain_live(channel)
                    assert completed.status == 'running'
                    assert completed.cursor == 104
                    assert state.pending_ranges() == ()
    assert invoked == [101, 103, 104]


def test_strict_restart_after_delivery_above_given_up_id_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests: list[object] = []
    available = [102, 103, 104, 105]

    def find(
        _handle: _GapProviderHandle,
        entity: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        if filters == []:
            return [{'id': 1}]
        requests.append(filters)
        if len(requests) == 3:
            raise ConnectionError('later page unavailable')
        return EventLogSG(runtime_event(event_id) for event_id in available).find(
            entity, filters, fields, **options
        )

    monkeypatch.setattr(_GapProviderHandle, 'find', find)
    provider = _gap_runtime_provider()
    group = replace(
        group_config('local', worker='default', ordering='strict'), strict_holdback_secs=1.0
    )
    site = runtime_site(group)
    delivered: list[int] = []

    def callback(context: EventContext) -> None:
        delivered.append(cast('int', context.event['id']))

    plugin = health_failure_plugin('callback')
    plugin = replace(plugin, callbacks=(replace(plugin.callbacks[0], callback=callback),))
    path = tmp_path / 'source.db'
    with _open_coordination_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')
        with open_group_on_source(
            path,
            writes=write_context_for(path),
            site='main',
            site_server_url=site.server,
            source='events',
            group='local',
            group_generation=7,
            ordering='strict',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='initialize',
            initial_cursor=100,
            initial_delivery_floor=100,
            initial_last_delivered_id=100,
            strict_holdback_secs=1.0,
        ) as state:
            connection = state._database.connection
            connection.execute('UPDATE source_meta SET frontier=110, successful_clock=30')
            insert_range(connection, 'range-a', 101, 105, state='closed')
            bind_group(connection, 'range-a', 'local', 7, floor=100, eligibility_point=1)
            insert_source_late_ids(connection, 'range-a', range(101, 106))
            for restart in (False, True):
                with (
                    closing(
                        GroupLane(
                            state=state,
                            site=site,
                            group=group,
                            provider=provider,
                            plugins=(plugin,),
                            worker_instance='worker-1',
                            health_interval_secs=1.0,
                            watchdog=ComponentWatchdog(
                                kind='lane',
                                identity='local',
                                worker_instance='worker-1',
                                registration_token=_watchdog_token(),
                                startup_deadline_secs=30.0,
                            ),
                        )
                    ) as lane,
                    CatchupReader(
                        provider=provider,
                        site_url=site.server,
                        credential=site.credentials['source'],
                        batch_size=2,
                        fetch_interval_secs=1.0,
                        waiter=SourceHealthWaiter(),
                    ) as reader,
                ):
                    assert lane.setup() is None
                    refresh = LaneRefresh(
                        lane=lane,
                        state=state,
                        reader=reader,
                        slot=nullcontext,
                        joined=lambda: False,
                        fetch_interval_secs=1,
                        late_event_window_secs=300,
                        log_extra={},
                    )
                    if not restart:
                        refresh.service()
                        assert delivered == []
                        connection.execute('UPDATE source_meta SET successful_clock=330')
                        refresh.service()
                        assert delivered == [102, 103]
                        assert state.snapshot().cursor == 103
                        assert state.pending_ranges()
                        available.insert(0, 101)
                        requests.clear()
                    else:
                        refresh.service()
                        assert requests == [
                            [{'filter_operator': 'any', 'filters': [['id', 'between', 104, 105]]}]
                        ]
                        assert delivered == [102, 103, 104, 105]
                        assert state.pending_ranges() == ()
                        assert state.snapshot().status == 'running'


@pytest.mark.parametrize('reconciled', [False, True])
def test_surviving_source_admits_resuming_initialized_peer(
    tmp_path: Path, reconciled: bool
) -> None:
    path = source_database_path(tmp_path)
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
    path = source_database_path(tmp_path)
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
        watermark = strict_watermark(strict, 'local')
        assert watermark.group_generation == 7
        assert watermark.delivery_floor == 41
        assert watermark.last_delivered_id == 44
        assert strict._detached_halts['local'] is halted
        assert surface.observe_group(request) == observation
        assert strict_watermark(strict, 'local') == watermark
        assert source._database.connection.total_changes == before
        with pytest.raises(ValueError, match='bootstrap request conflicts'):
            surface.observe_group(replace(request, start_mode='initialize'))


@pytest.mark.parametrize('blocked', ['parked', 'stopping'])
def test_setup_retry_rejects_parked_or_stopping_lane(blocked: str) -> None:
    component, resources = lifecycle_probe(
        'sync',
        durable_status='parked' if blocked == 'parked' else 'running',
        setup_failure=RuntimeError('setup unavailable'),
    )
    lifecycle_factory = worker_module._LaneLifecycle
    lifecycle = lifecycle_factory(
        channel=resources.channel,
        setup_attempted=True,
        setup_failure=RuntimeError('setup unavailable'),
    )
    vars(component)['_lifecycle'] = lifecycle
    vars(component)['_lane'] = resources.lane
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
            resources.state,
            resources.state.snapshot(),
            command,
        )
    vars(lifecycle)['setup_attempted'] = False
    vars(lifecycle)['setup_failure'] = None
    assert not callbacks_allowed(
        component,
        cast('_LaneResources', resources),
        lifecycle,
    )
    assert resources.lane.operations == []


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_operator_retry_recovers_setup_failure_and_preserves_callback_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    group = group_config('local', worker='default', execution=execution)
    site = runtime_site(group)
    healthy = replace(group, name='healthy')
    site = replace(site, groups=MappingProxyType({'local': group, 'healthy': healthy}))
    spec = build_worker_specs(site)[0]
    assignment = local_assignment(spec, group.name, 'initialize')
    healthy_assignment = local_assignment(spec, healthy.name, 'initialize')
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
    with running_concrete_runtime(
        monkeypatch,
        runtime_config(tmp_path, site),
        spec,
        assignment,
        _restart_provider(provider_state, async_=execution == 'async'),
    ) as (runtime, control):
        receive_typed_health(
            control,
            lambda item: any(
                health.group == 'local' and health.failure_kind == 'setup'
                for health in item.groups
            ),
        )
        lane = _runtime_lane(runtime, 'local')
        assert lane is not None

        for attempt in (2, 3):
            _assert_operator_retry(control, f'setup-retry-{attempt}', fails_again=attempt == 2)
            assert len(setup_handles) == attempt
            receive_typed_health(
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

        provider_state.publish(42)
        receive_typed_health(
            control,
            lambda item: any(entry.failure_kind == 'callback' for entry in item.groups),
        )
        provider_state.publish(43)
        _assert_operator_retry(control, 'callback-retry')
        receive_typed_health(
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
        runtime_site(group_config('local', worker='default')),
        groups=MappingProxyType({}),
    )
    config = runtime_config(tmp_path, site)
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

    class _Handle(RuntimeProviderHandle):
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
        sync_factory=factory,
        async_factory=None,
    )
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        provider,
    ) as (runtime, parent):
        receive_type(parent, SourceEndpointReady)
        update = receive_typed_health(
            parent,
            lambda item: (
                bool(item.sources)
                and item.sources[0].initialized
                and item.sources[0].phase == 'wait'
                and ((item.sources[0].retry_attempt > 0) == outage)
            ),
        )
        source = update.sources[0]
        status = status_from_health(
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
        kind=_WorkerKind.SITE,
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = _WorkerRuntime(
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
        send_control(parent, command)
        result = receive_control(parent)
        assert isinstance(result, CommandResult)
        watchdog = next(item for item in cache.snapshot().watchdogs if item.kind == 'operator')
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
        for operation in component.operations
    )


@pytest.mark.parametrize('active', [False, True], ids=['idle', 'active-command'])
def test_health_cache_operator_shutdown_preserves_current_target(active: bool) -> None:
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=_WorkerKind.SITE,
        name='main',
        sources=('events',),
        groups=('local',),
    )
    cache = _ComponentHealthCache(
        site='main',
        group_sources=MappingProxyType({'local': 'events'}),
    )
    runtime = _WorkerRuntime(
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
    cache.register_watchdog(
        key=(sample.kind, sample.identity, sample.worker_instance, sample.registration_token),
        supplier=watchdog.sample,
    )
    idle = next(item for item in cache.snapshot().watchdogs if item.kind == 'operator')
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
        internal = next(item for item in cache.snapshot().watchdogs if item.kind == 'operator')
        assert internal.phase == 'internal'
        assert (internal.source, internal.group) == ('events', 'local')

    runtime.request_stop('test-shutdown')
    shutdown = next(item for item in cache.snapshot().watchdogs if item.kind == 'operator')

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
                encode_event(Event(runtime_event(event_id))),
            )
        )
        if transport == 'remote':
            message = cast(
                'EventDelivery',
                _decode_event_wire(b''.join(_encode_fanout_message(message).parts)),
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
    group = group_config('remote', worker=worker, ordering=ordering)
    site = _runtime_delivery_site(group, start_mode)
    prepare_runtime_state(tmp_path, site, group, start_mode)
    config = runtime_config(tmp_path, site)
    site_spec, dependent_spec = build_worker_specs(site)
    provider, provider_state, invoked = concrete_delivery_provider(monkeypatch, start_mode)
    monkeypatch.setattr(worker_module, 'load_provider', provider_loader(provider))
    source_parent_socket, source_child_socket = socket.socketpair()
    source_parent = WorkerControl(source_parent_socket, max_frame_bytes=131_072)
    source_child = WorkerControl(source_child_socket, max_frame_bytes=131_072)
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
    )
    dependent_child = WorkerControl(
        dependent_child_socket,
        max_frame_bytes=131_072,
    )
    dependent_runtime: _WorkerRuntime | None = None
    dependent_drain_stop: threading.Event | None = None
    dependent_drain_thread: threading.Thread | None = None
    source_drain_stop: threading.Event | None = None
    source_drain_thread: threading.Thread | None = None
    try:
        source_runtime.start()
        endpoint = receive_type(source_parent, SourceEndpointReady)
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
            dependent_drain_stop, dependent_drain_thread = start_control_drain(dependent_parent)
        group_path = source_database_path(tmp_path)
        _wait_for_group_database(dependent_runtime, group_path, 'remote')
        if start_mode == 'resume':
            _assert_resumed_delivery(
                dependent_runtime, group_path, provider_state, invoked, 'remote'
            )
        assert source_runtime.fatal_reason is None
    finally:

        def close_source_runtime() -> None:
            release_failed_source_runtime(
                source_runtime,
                (source_parent, source_child),
                (source_parent_socket, source_child_socket),
                (source_drain_stop, source_drain_thread)
                if source_drain_stop is not None and source_drain_thread is not None
                else None,
            )

        release_failed_source_runtime(
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
    group = group_config('local', worker='default', ordering=ordering)
    site = _runtime_delivery_site(group, start_mode)
    prepare_runtime_state(tmp_path, site, group, start_mode)
    config = runtime_config(tmp_path, site)
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
    provider, provider_state, invoked = concrete_delivery_provider(monkeypatch, start_mode)
    monkeypatch.setattr(worker_module, 'load_provider', provider_loader(provider))
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = build_worker_runtime(config, spec, assignment, child)
    drain_stop: threading.Event | None = None
    drain_thread: threading.Thread | None = None
    try:
        runtime.start()
        receive_typed_health(parent, lambda item: bool(item.sources and item.groups))
        drain_stop, drain_thread = start_control_drain(parent)
        group_path = source_database_path(tmp_path)
        _wait_for_group_database(runtime, group_path, 'local')
        if start_mode == 'resume':
            _assert_resumed_delivery(runtime, group_path, provider_state, invoked, 'local')
    finally:
        release_failed_source_runtime(
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
        channel=probes['channel'],
        client=probes['client'],
        lane=probes['lane'],
        reader=probes['reader'],
        state=probes['state'],
    )
    drive = cast('_LaneLifecycleDriver', vars(component_type)['_drive_lane'])
    first_error = probes[fault].error
    assert first_error is not None

    with pytest.raises(RuntimeError) as raised:
        drive(component, resources)

    traceback = raised.value.__traceback__
    while traceback is not None and traceback.tb_next is not None:
        traceback = traceback.tb_next
    assert operations == list(order)
    assert vars(component)['_lane'] is None
    assert raised.value is first_error
    assert traceback is probes[fault].traceback


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
    assert client.calls == [7, 7]
    assert [item.frontier for item in reader.observed] == [41, 42]


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
        assert cache.snapshot().groups[0].outstanding_late_id_count == 0

        mutation = commit_step(
            state, ReserveLateStep('range-1', 42), expected_revision=accepted_revision
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
            return state.advance_irrelevant(42, expected_revision=mutation.revision)

        clock = [0.0]
        monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
        assert lifecycle is not None
        vars(lifecycle)['next_coordination_at'] = 1.0

        def drain(
            _channel: object,
            *,
            idle_snapshot: object | None = None,
            deadline_at: float = float('inf'),
        ) -> GroupSnapshot:
            del idle_snapshot, deadline_at
            clock[0] = 1.0
            return mutation

        monkeypatch.setattr(vars(resources)['lane'], 'drain_live', drain)
        vars(component)['_poll_lane'] = ignore_client
        vars(resources)['coordination'] = SimpleNamespace(service=coordinate)
        assert isinstance(component, _LaneComponent)
        component._drive_live(cast('_LaneResources', resources), cast('_LaneLifecycle', lifecycle))
        _publish_group_health(component, resources, lifecycle)

        assert snapshot_calls == 1
        assert projection_calls == 2
        health = cache.snapshot().groups[0]
        assert health.outstanding_late_id_count == 1
        assert health.outstanding_late_ids == (42,)
        assert health.outstanding_late_obligation_count == expected_obligation_count


@pytest.mark.parametrize(
    ('outcome', 'resulting_revision'),
    [('parked', 12), ('stale', 15)],
)
def test_lane_sends_recorded_stale_or_parked_result_before_plugin_construction(
    monkeypatch: pytest.MonkeyPatch,
    outcome: Literal['parked', 'stale'],
    resulting_revision: int,
) -> None:
    component, context, operations, control, state = _recovery_construction_probe(
        monkeypatch,
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
    assert state.decided_at_wall_clock == 2000.5
    assert len(control.messages) == 1
    ready = cast('SimpleNamespace', control.messages[0])
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
    assert operations.index('ready') < operations.index('plugin-construction')


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
        kind=_WorkerKind.SITE,
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

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='spawn-1',
        source_factory=lambda name, _slots, _host: components[f'source:{name}'],
        lane_factory=lambda name, _slots, _host: components[f'lane:{name}'],
        health_sink=fail if failure_site != 'control' else lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
    assert all(component.finished.is_set() for component in components.values())
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
        kind=_WorkerKind.SITE,
        name='main',
        sources=('events',) if terminal_identity == 'source:events' else (),
        groups=(),
    )
    component = _Component('source:events', [], wedge=True)
    forced: list[int] = []
    gate = _TerminalWakeGate(terminal_identity)

    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
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
    gate.install(runtime)
    runtime.start()
    gate.observe_target_thread(runtime)
    wait_result: list[tuple[str, ...]] = []
    wait_thread: threading.Thread | None = None
    try:
        if terminal_identity == 'source:events':
            assert component.started.wait()
        assert gate.wait_for_health_after(0)
        runtime.request_stop('test')
        if terminal_identity == 'source:events':
            operator = vars(runtime)['_threads']['operator-executor']
            operator.join()
            assert not operator.is_alive()
            previous_health_count = gate.health_count()
            gate.wake_monitor()
            assert gate.wait_for_health_after(previous_health_count)
            component.release.set()
        assert gate.wait_for_terminal_wake()
        monitor = vars(runtime)['_monitor_thread']
        assert isinstance(monitor, threading.Thread)
        monitor.join()
        assert not monitor.is_alive()
        wait_thread = threading.Thread(
            target=lambda: wait_result.append(runtime.wait()),
            name='worker-wait-probe',
        )
        wait_thread.start()
        assert gate.wait_for_bounded_join()
        wait_thread.join()
        assert not wait_thread.is_alive()
        assert wait_result == [(terminal_identity,)]
        assert forced == []
        assert runtime._stuck_threads == (terminal_identity,)
        assert runtime._resources_open
        gate.release()
    finally:
        gate.release()
        component.release.set()
        if wait_thread is not None:
            wait_thread.join()
        for thread in vars(runtime)['_threads'].values():
            thread.join()
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
    attempt = attempt_type(**probes)
    close_partial = cast(
        'Callable[[object, object], None]',
        vars(component_type)['_close_partial_attempt'],
    )
    first_error = probes[fault].error
    assert first_error is not None

    with pytest.raises(RuntimeError) as raised:
        close_partial(component, attempt)

    traceback = raised.value.__traceback__
    while traceback is not None and traceback.tb_next is not None:
        traceback = traceback.tb_next
    assert operations == list(order)
    assert vars(component)['_lane'] is None
    assert raised.value is first_error
    assert traceback is probes[fault].traceback


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
                    shutdown_deadline_secs=2.0,
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
        assignment=SimpleNamespace(
            conditional_park=None,
            group_generation=7,
            start_mode='resume',
        ),
        credential=SimpleNamespace(
            max_rpc_attempts=3,
            rpc_attempt_interval_secs=0.1,
            timeout_secs=1.0,
        ),
        group=SimpleNamespace(source='events'),
        holdback_secs=0.0,
        source_config=SimpleNamespace(
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
        coordination_deadline_secs=stall_deadline('coordination', 30.0),
        stop=threading.Event(),
        wait_phase=nullcontext,
        wait_slice_secs=0.1,
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
        assignment=SimpleNamespace(
            conditional_park=None,
            group_generation=7,
            start_mode='resume',
        ),
        group=SimpleNamespace(source='events'),
        site=SimpleNamespace(name='main'),
        source_config=SimpleNamespace(fetch_interval_secs=0.0),
    )

    failure.install(
        component,
        context,
        tmp_path / f'{stage}.db',
        monkeypatch,
    )
    run = cast('Callable[[object], None]', vars(component_type)['run'])

    with pytest.raises(ConnectionError, match=f'{stage} construction failed'):
        run(component)

    assert failure.attempts == 1


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_remote_retained_history_halt_remains_group_local(
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    tmp_path: Path,
) -> None:
    groups = (
        group_config('halted', worker='pool', ordering=ordering),
        group_config('healthy', worker='pool', ordering=ordering),
    )
    site = replace(
        runtime_site(groups[0]),
        groups=MappingProxyType({group.name: group for group in groups}),
        sources=MappingProxyType(
            {
                'events': replace(
                    runtime_site(groups[0]).sources['events'],
                    fetch_interval_secs=0.01,
                )
            }
        ),
    )
    _prepare_retained_history_runtime_state(tmp_path, site, groups)
    config = runtime_config(tmp_path, site)
    site_spec, dependent_spec = build_worker_specs(site)
    provider = _gap_runtime_provider()
    monkeypatch.setattr(worker_module, 'load_provider', provider_loader(provider))
    component_type = _LaneComponent
    recover = component_type._recover_lane_failure
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
        '_recover_lane_failure',
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
    source_session = start_source_runtime(
        config,
        site_spec,
        source_assignment,
        grants,
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
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
        source_endpoints=(source_session.endpoint,),
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
        drain_stop, drain_thread = start_control_drain(parent)
        release_failed_source_runtime(
            runtime,
            (parent, child),
            (parent_socket, child_socket),
            (drain_stop, drain_thread),
            (source_session.close,),
        )

    with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
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
        group_config('halted', worker='default', ordering=ordering),
        group_config('healthy', worker='default', ordering=ordering),
    )
    site = replace(
        runtime_site(groups[0]),
        groups=MappingProxyType({group.name: group for group in groups}),
    )
    _prepare_retained_history_runtime_state(tmp_path, site, groups)
    config = runtime_config(tmp_path, site)
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
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        assignment,
        _gap_runtime_provider(),
    ) as (runtime, control):
        _assert_retained_history_halt(runtime, control, halted_serviced)

    with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
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

    fatal_reason, forced = run_cooperative_stop_component(
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

    fatal_reason, forced = run_cooperative_stop_component(
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
            '_health_interval_secs': 0.05,
            '_config': SimpleNamespace(daemon=SimpleNamespace(shutdown_deadline_secs=1)),
            '_stop': component_stop,
        }
    )
    call = component_type.call
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
            wait_phase=nullcontext,
            stop=caller_stop,
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        assert queued.wait()
        {'caller': caller_stop, 'component': component_stop}[stop_owner].set()
        with pytest.raises(cooperative_error) as raised:
            future.result()

    assert caller_stop.is_set() == (stop_owner == 'caller')
    assert component_stop.is_set() == (stop_owner == 'component')
    assert isinstance(raised.value.__cause__, ConnectionError)
    assert str(raised.value.__cause__) == 'source component stopped during local call'
    _service_queued_local_calls(component, SimpleNamespace(reconcile_groups=lambda: None))
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
    provider = runtime_provider()
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock.now)
    with _open_coordination_source(tmp_path / 'source-extreme-health.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _source_health_poller(state, provider) as poller:
            _service_source_health(component, server, poller)
            clock.now = 1.0
            _service_source_health(component, server, poller)

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
    group = group_config('remote', worker='pool')
    site = runtime_site(group)
    source_config = replace(
        site.sources['events'],
        fetch_interval_secs=0.001,
    )
    site = replace(
        site,
        sources=MappingProxyType({'events': source_config}),
    )
    config = lift_config(
        Config(
            daemon=_DaemonConfig(
                state_dir=tmp_path,
                sg_api_provider='shotgun-api3',
            ),
            sites=MappingProxyType({site.name: site}),
        )
    )
    database_path = source_database_path(tmp_path)
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
        provider_loader(_gap_runtime_provider()),
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = build_worker_runtime(config, spec, assignment, child)
    drain_stop: threading.Event | None = None
    drain_thread: threading.Thread | None = None
    row: tuple[int] | None = None
    try:
        runtime.start()
        receive_type(parent, SourceEndpointReady)
        drain_stop, drain_thread = start_control_drain(parent)
        while True:
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
        release_failed_source_runtime(
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
    group = group_config('local', worker='default', ordering='eager')
    site = runtime_site(group)
    prepare_runtime_state(tmp_path, site, group, 'resume')
    with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
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
    config = runtime_config(tmp_path, site)
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
        provider_loader(runtime_provider(calls=calls)),
    )
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = build_worker_runtime(config, spec, assignment, child)
    drain_stop: threading.Event | None = None
    drain_thread: threading.Thread | None = None
    try:
        runtime.start()
        try:
            health = receive_typed_health(
                parent,
                lambda item: any(
                    condition in group_health.conditions for group_health in item.groups
                ),
            ).groups[0]
        finally:
            drain_stop, drain_thread = start_control_drain(parent)
        assert halted_serviced.wait(), 'halted lane did not complete a service cycle'
        lane_thread = runtime._threads['lane:local']
        assert runtime.fatal_reason is None
        assert lane_thread.is_alive()
        assert not any('lane:local' in name for name in calls)
        assert condition in health.conditions
    finally:
        release_failed_source_runtime(
            runtime,
            (parent, child),
            (parent_socket, child_socket),
            (drain_stop, drain_thread)
            if drain_stop is not None and drain_thread is not None
            else None,
        )
    with closing(sqlite3.connect(source_database_path(tmp_path))) as connection:
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

    def deliver_many_counted(
        _deliveries: object, *, deadline_at: float = float('inf')
    ) -> tuple[object, int]:
        del deadline_at
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
        outcomes.append(deliver((Event(runtime_event(41)),), 0.0))
        return SimpleNamespace(status='join-ready', join_ready=True)

    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    vars(component).update(
        {
            '_config': SimpleNamespace(
                daemon=SimpleNamespace(max_pending_epochs=8, shutdown_deadline_secs=1.0),
            ),
            '_stop': stop,
            '_limiter': _CatchupLimiter(1),
            '_health_interval_secs': 1.0,
        }
    )
    resources = SimpleNamespace(
        context=SimpleNamespace(
            holdback_secs=1.0,
            source_config=SimpleNamespace(fetch_interval_secs=0.001),
        ),
        coordination=SimpleNamespace(
            synchronize=lambda: _synchronization(45),
            service=_ProbeCoordination.service,
        ),
        lane=SimpleNamespace(deliver_many_counted=deliver_many_counted),
        reader=SimpleNamespace(strict_step=strict_step),
        state=_ProbeState(),
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


def test_wal_pause_does_not_consume_lane_internal_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    component = _unopened_group_component(_LaneComponent)
    clock = [100.0]
    watchdog = component._watchdog
    monkeypatch.setattr(watchdog, '_clock', lambda: clock[0])
    budget = component._internal_stall_deadline_secs
    watchdog.transition('internal', deadline_at=clock[0] + budget)
    phases: list[str] = []

    def pause(_seconds: float) -> None:
        sample = watchdog.sample()
        phases.append(sample.phase)
        assert sample.deadline_at is not None
        assert sample.deadline_at > clock[0]
        clock[0] += 2 * budget

    with closing(
        worker_module.SourceWriteLock(
            tmp_path / 'pause.write-lock',
            database_path=tmp_path / 'pause.db',
            timeout_secs=20.0,
            wait_phase_grace_secs=10.0,
            clock=lambda: clock[0],
            sleep=pause,
        )
    ) as lock:
        writes = worker_module.WriteContext(lock, watch=component._write_watch)
        for _ in range(3):
            writes._pause_retry()
            sample = watchdog.sample()
            assert sample.phase == 'internal'
            assert sample.deadline_at == clock[0] + budget
    assert phases == ['write-lock-wait'] * 3


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
    site = _site((group_config('remote', worker='pool'),))
    config = lift_config(
        Config(
            daemon=_DaemonConfig(
                state_dir=tmp_path,
                sg_api_provider='shotgun-api3',
            ),
            sites=MappingProxyType({site.name: site}),
        )
    )
    spec = next(item for item in build_worker_specs(site) if item.kind is _WorkerKind.NAMED)
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
    control = WorkerControl(parent_socket, max_frame_bytes=131_072)
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

    runtime = cast('_WorkerRuntime', SimpleNamespace(_threads={}, _monitor_thread=None))
    parent = cast('WorkerControl', SimpleNamespace(close=lambda: attempt('parent')))
    child = cast('WorkerControl', SimpleNamespace(close=lambda: attempt('child')))

    def stop_runtime(_runtime: object) -> None:
        attempt('runtime')

    monkeypatch.setattr(sys.modules[__name__], '_stop_runtime', stop_runtime)

    def stop_drain(*_args: object) -> None:
        attempt('drain')

    monkeypatch.setattr(sys.modules[__name__], 'stop_control_drain', stop_drain)
    session = _SourceRuntimeSession(
        runtime,
        parent,
        child,
        threading.Event(),
        threading.Thread(),
        SourceEndpointReady('events', 'source', '/private/events.sock'),
    )
    with pytest.raises(AssertionError, match='injected cleanup failure') as caught:
        session.close()
    assert caught.value is failure
    assert attempts == ['runtime', 'drain', 'parent', 'child']


@pytest.mark.parametrize('owner', ['runtime', 'monitor', 'drain'])
def test_source_session_retains_controls_used_by_live_runtime(
    monkeypatch: pytest.MonkeyPatch,
    owner: str,
) -> None:
    thread = cast('threading.Thread', SimpleNamespace(is_alive=lambda: True, join=lambda: None))
    attempts: list[str] = []
    runtime = cast(
        '_WorkerRuntime',
        SimpleNamespace(
            _threads={'source': thread} if owner == 'runtime' else {},
            _monitor_thread=thread if owner == 'monitor' else None,
        ),
    )
    control = cast('WorkerControl', SimpleNamespace(close=lambda: attempts.append('control')))

    def stop(_runtime: _WorkerRuntime | None) -> None:
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
    with pytest.raises(AssertionError, match='runtime remained alive'):
        session.close()
    assert attempts == []


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

    def record_join() -> None:
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
            socket_path=tmp_path / 'events.sock',
            site=object(),
            group=object(),
            provider=object(),
            plugin=object(),
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
    spec = build_worker_specs(_site((group_config('local', worker='default'),)))[0]
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

    runtime = _WorkerRuntime(
        spec=spec,
        spawn_instance='partial-start',
        source_factory=lambda name, _slots, _host: components[f'source:{name}'],
        lane_factory=lambda name, _slots, _host: components[f'lane:{name}'],
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
        assert cache.snapshot().watchdogs == ()
        with pytest.raises(RuntimeError, match='startup failed'):
            runtime.start()
        with pytest.raises(RuntimeError, match='not started'):
            runtime.wait()
        assert runtime._shutdown() == ()
        assert operations.count('close:source:audit') == 1
    finally:
        for component in components.values():
            component.release.set()
        runtime.request_stop('test-cleanup')
        for thread in started:
            thread.join()


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
        monkeypatch.setattr(component, 'close', close)
    runtime = _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='partial-construction',
        source_factory=source_factory,
        lane_factory=lambda _name, _slots, _host: component,
        health_sink=lambda _health: None,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
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
        provider_state.publish(event_id)
    provider = _restart_provider(provider_state)
    group = group_config('local', worker='default')
    site = runtime_site(group)
    component_type = _LaneComponent
    component = _unopened_group_component(component_type)
    watchdog = component._watchdog
    plugin = health_failure_plugin('callback')
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
            waiter=SourceHealthWaiter(),
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
            state=state,
            lane=lane,
            reader=reader,
            context=SimpleNamespace(source_config=site.sources['events']),
            coordination=SimpleNamespace(
                service=Mock(return_value=None),
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
        assert set(provider_state.observed) == {42, 43, 44}


def _checkpoint_shutdown_runtime(
    forced: list[tuple[int, str]],
    *,
    health_interval_secs: float = 2.0,
) -> _WorkerRuntime:
    spec = build_worker_specs(_site(()))[0]
    components = {name: _Component(name, []) for name in spec.sources}
    return _WorkerRuntime(
        spec=spec,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        spawn_instance='checkpoint-shutdown',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=lambda _health: None,
        health_interval_secs=health_interval_secs,
        checkpoint_interval_secs=0.4,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
        force_exit=lambda code: forced.append((code, threading.current_thread().name)),
    )


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
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        opened.append(path)
        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            logger=logger,
            log_extra=log_extra,
        )

    host = CheckpointHost(
        logger=logging.getLogger('sg_event_daemon.source'),
        interval_secs=60,
        synchronous='NORMAL',
        on_failure=runtime._component_failed,
        checkpointer_factory=factory,
    )
    runtime._checkpoint_host = host

    def run() -> None:
        entered.set()
        assert release.wait()
        host.register(path, 'events', schema, log_extra={})

    component = InterruptibleComponent(run, lambda: None)

    def source_factory(
        _name: str, _slots: _CatchupLimiter, _host: CheckpointHost
    ) -> InterruptibleComponent:
        return component

    runtime._source_factory = source_factory
    runtime.start()
    try:
        assert entered.wait()
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
def test_checkpoint_reuse_retains_monitor_until_shutdown_deadline(
    tmp_path: Path,
    health_interval_secs: float,
) -> None:
    path = tmp_path / 'state.db'
    schema = Schema(version=1, statements=('CREATE TABLE entries (id INTEGER)',))
    with open_database(path, 'events', schema, synchronous='NORMAL'):
        pass
    sleeping = threading.Event()
    release = threading.Event()
    forced: list[tuple[int, str]] = []
    runtime = _checkpoint_shutdown_runtime(forced, health_interval_secs=health_interval_secs)

    def blocked_reuse() -> tuple[int, int, int]:
        sleeping.set()
        release.wait()
        return 1, 5, 0

    def factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            run_checkpoint=lambda: (1, 5, 0),
            run_reuse=blocked_reuse,
            logger=logger,
            log_extra=log_extra,
        )

    host = CheckpointHost(
        logger=logging.getLogger('sg_event_daemon.source'),
        interval_secs=0.4,
        synchronous='NORMAL',
        on_failure=runtime._component_failed,
        checkpointer_factory=factory,
    )
    runtime._checkpoint_host = host
    runtime.start()
    try:
        host.register(path, 'events', schema, log_extra={})
        assert sleeping.wait()
        runtime.request_stop('during-checkpoint-reuse')
        _expire_shutdown(runtime, {'checkpoint:events'})
        live = runtime.wait()
        assert forced == [(1, 'sg-event-daemon:monitor')]
        assert live == ('checkpoint:events',)
        assert runtime._stuck_threads == live
        assert 'checkpoint:events' in runtime._current_health(time.monotonic()).thread_names
        assert runtime._resources_open
    finally:
        release.set()
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
        assert release.wait()

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
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
    ) -> Checkpointer:
        def checkpoint() -> tuple[int, int, int]:
            if boundary == 'attempt':
                block()
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            run_checkpoint=checkpoint,
            logger=logger,
            log_extra=log_extra,
        )

    monkeypatch.setattr(checkpoint_host_module, 'open_database', open_checkpoint)
    monkeypatch.setattr(DatabaseHandle, 'close', close_checkpoint)
    host = CheckpointHost(
        logger=logging.getLogger('sg_event_daemon.source'),
        interval_secs=0.01,
        synchronous='NORMAL',
        on_failure=runtime._component_failed,
        checkpointer_factory=factory,
    )
    runtime._checkpoint_host = host
    runtime.start()
    try:
        host.register(path, 'events', schema, log_extra={})
        assert (opened if boundary == 'close' else entered).wait()
        runtime.request_stop(f'blocked-checkpoint-{boundary}')
        assert entered.wait()
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
            tmp_path / 'unopened.db', 'events', Schema(version=1, statements=()), log_extra={}
        )
        for thread in runtime._checkpoint_host.threads():
            thread.join()
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
        assert release.wait()
        return open_database(path, 'events', schema, synchronous='NORMAL', create=False)

    def start(thread: threading.Thread) -> None:
        if thread.name == 'sg-event-daemon:monitor':
            assert entered.wait()
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
    runtime._checkpoint_host.register(path, 'events', schema, log_extra={})
    try:
        with pytest.raises(RuntimeError, match='monitor-start-failed'):
            runtime.start()
        assert operator_returned.wait()
        assert checkpoint_join_timeouts == [0.0]
        assert 'checkpoint:events' in runtime._stuck_threads
        assert runtime._resources_open
        assert forced == []
    finally:
        release.set()
        operator_release.set()
        _finish_checkpoint_runtime(runtime)
    assert not runtime._resources_open


@pytest.mark.parametrize(
    'lane_state',
    ['live-idle', 'detached', 'join-retry', 'parked', 'halted'],
)
def test_lane_ticks_once_per_interval_in_each_scheduling_path(
    monkeypatch: pytest.MonkeyPatch, lane_state: str
) -> None:
    component_object, resources_object = lifecycle_probe(
        'sync',
        durable_status='parked' if lane_state == 'parked' else 'running',
        setup_failure=None,
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(channel=resources.channel)
    component._lifecycle = lifecycle
    clock = [100.0]
    calls: list[tuple[float, bool]] = []
    vars(resources.context.source_config)['fetch_interval_secs'] = 1.0
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])

    def wait(timeout: float | None = None) -> bool:
        assert timeout is not None
        clock[0] += timeout
        return False

    def coordinate(*, scan_obligations: bool = True, force: bool = False) -> None:
        del force
        calls.append((clock[0], scan_obligations))

    monkeypatch.setattr(component._wake, 'wait', wait)
    monkeypatch.setattr(resources.coordination, 'service', coordinate)
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
        provider_state.publish(event_id)
    provider = _restart_provider(provider_state)
    group = group_config('local', worker='default')
    site = runtime_site(group)
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
            waiter=SourceHealthWaiter(),
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
        component._lifecycle = worker_module._LaneLifecycle(channel=channel)

        def coordinate(
            *, scan_obligations: bool = True, force: bool = False, slot_held: bool = False
        ) -> GroupSnapshot | None:
            del force
            if scan_obligations:
                assert slot_held
                return None
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
            return state.snapshot()

        resources = cast(
            '_LaneResources',
            SimpleNamespace(
                state=state,
                lane=lane,
                reader=reader
                if ordering == 'eager'
                else SimpleNamespace(
                    strict_step=partial(
                        _strict_prepared_probe_step, state, clock, provider_delay, active_delivery
                    )
                ),
                client=SimpleNamespace(),
                context=SimpleNamespace(
                    holdback_secs=0.0, source_config=SimpleNamespace(fetch_interval_secs=0.5)
                ),
                coordination=SimpleNamespace(
                    synchronize=lambda: _synchronization(47),
                    service=coordinate,
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


@dataclass
class _CatchupSliceProbe:
    component: _LaneComponent
    state: GroupState
    lane: GroupLane
    clock: list[float]
    trace: list[tuple[str, int, int]]
    prefix_count: int
    stop: Literal['park', 'component', 'lane'] | None
    setup_stop: bool | None
    _setup_entries: int = 0
    _entries: int = 0
    _attempt_budget: int = 0

    def _setup_snapshot(self, original: Callable[[], GroupSnapshot]) -> GroupSnapshot:
        snapshot = original()
        if snapshot.cursor < 41 + self.prefix_count:
            return snapshot
        lifecycle = self.component._lifecycle
        assert lifecycle is not None
        self._setup_entries += 1
        self.clock[0] = max(self.clock[0], lifecycle.next_coordination_at) + 0.01
        self.trace.append(('setup', snapshot.cursor, self._setup_entries))
        if self.setup_stop and self._setup_entries == 3:
            stopped = _stop_after_empty_slice(
                self.stop, self.state, snapshot, self.component, self.lane
            )
            if stopped is not None:
                return stopped
        return snapshot

    def coordinate(
        self, *, scan_obligations: bool = True, force: bool = False, slot_held: bool = False
    ) -> GroupSnapshot | None:
        del force
        if scan_obligations:
            assert slot_held
            return None
        snapshot = self.state.snapshot()
        self.trace.append(('coordinate', snapshot.cursor, 0))
        if self.setup_stop is True:
            return None
        if self.setup_stop is False and self._setup_entries < 3:
            return None
        if self.setup_stop is None:
            return None
        return _stop_after_empty_slice(self.stop, self.state, snapshot, self.component, self.lane)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original_deliveries = self.component._catchup_deliveries
        original_snapshot = self.lane._snapshot
        original_counted = self.lane.deliver_many_counted
        in_delivery = False

        def snapshot() -> GroupSnapshot:
            return self._setup_snapshot(original_snapshot) if in_delivery else original_snapshot()

        def counted(deliveries: Iterable[Delivery], **options: float) -> tuple[GroupSnapshot, int]:
            nonlocal in_delivery
            in_delivery = True
            try:
                return original_counted(deliveries, **options)
            finally:
                in_delivery = False

        def deliveries(events: Sequence[Event]) -> Generator[Delivery]:
            self._entries += 1
            if self._entries == 1:
                # Bound attempts by the original page, never its shrinking suffix.
                self._attempt_budget = len(events)
            assert self._entries <= self._attempt_budget, (
                f'empty slice retried without progress: {self.trace}'
            )
            cursor = self.state.snapshot().cursor
            if self.setup_stop is None and self._entries == (1 if self.prefix_count == 0 else 2):
                lifecycle = self.component._lifecycle
                assert lifecycle is not None
                self.clock[0] = lifecycle.next_coordination_at
                _stop_after_empty_slice(
                    self.stop, self.state, self.state.snapshot(), self.component, self.lane
                )
            self.trace.append(('entry', cursor, len(events)))
            yielded = 0
            try:
                for delivery in original_deliveries(events):
                    yielded += 1
                    yield delivery
            finally:
                self.trace.append(('slice', cursor, yielded))

        monkeypatch.setattr(self.component, '_catchup_deliveries', deliveries)
        if self.setup_stop is not None:
            monkeypatch.setattr(self.lane, '_snapshot', snapshot)
            monkeypatch.setattr(self.lane, 'deliver_many_counted', counted)


@contextmanager
def _empty_slice_catchup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prefix_count: int,
    guard: tuple[Literal['park', 'component', 'lane'] | None, bool | None] = (None, None),
    *,
    ordering: Literal['eager', 'strict'] = 'eager',
) -> Generator[
    tuple[
        _LaneComponent,
        _LaneResources,
        GroupState,
        GroupLane,
        list[int],
        list[tuple[str, int, int]],
    ]
]:
    stop, setup_stop = guard
    clock = [100.0]
    monkeypatch.setattr(worker_module.time, 'monotonic', lambda: clock[0])
    component = _unopened_group_component(_LaneComponent)
    provider_state = _RestartProviderState()
    for event_id in range(42, 48):
        provider_state.publish(event_id)
    provider = _restart_provider(provider_state)
    group = group_config('local', worker='default', ordering=ordering)
    site = runtime_site(group)
    invoked: list[int] = []
    trace: list[tuple[str, int, int]] = []

    with (
        _open_group_health_state(tmp_path / 'group.db', ordering=ordering) as state,
        CatchupReader(
            provider=provider,
            site_url=site.server,
            credential=site.credentials['source'],
            batch_size=10,
            fetch_interval_secs=0.5,
            waiter=SourceHealthWaiter(),
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
        lifecycle = worker_module._LaneLifecycle(channel=channel)
        component._lifecycle = lifecycle
        probe = _CatchupSliceProbe(
            component, state, lane, clock, trace, prefix_count, stop, setup_stop
        )
        probe.install(monkeypatch)

        def wait(seconds: float | None = None) -> bool:
            assert seconds is not None
            clock[0] += seconds
            return False

        monkeypatch.setattr(component._wake, 'wait', wait)

        resources = cast(
            '_LaneResources',
            SimpleNamespace(
                state=state,
                lane=lane,
                reader=reader,
                client=SimpleNamespace(),
                context=SimpleNamespace(
                    holdback_secs=0.0, source_config=SimpleNamespace(fetch_interval_secs=0.5)
                ),
                coordination=SimpleNamespace(
                    synchronize=lambda: _synchronization(47),
                    service=probe.coordinate,
                ),
            ),
        )
        yield component, resources, state, lane, invoked, trace


@pytest.mark.parametrize('prefix_count', [0, 2])
def test_eager_catchup_empty_slice_keeps_join(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefix_count: int
) -> None:
    with _empty_slice_catchup(tmp_path, monkeypatch, prefix_count) as (
        component,
        resources,
        state,
        lane,
        invoked,
        trace,
    ):
        source = _PressureJoinSource(
            boundary=47,
            live_event_id=None,
            source_generation='source-generation-1',
            stream_incarnation='stream-incarnation-1',
        )
        request = Mock(wraps=source.request)
        confirm = Mock(wraps=source.confirm)
        finalize = Mock(wraps=source.finalize)
        abort = Mock(wraps=source.abort)
        monkeypatch.setattr(source, 'request', request)
        monkeypatch.setattr(source, 'confirm', confirm)
        monkeypatch.setattr(source, 'finalize', finalize)
        monkeypatch.setattr(source, 'abort', abort)
        assert component._lifecycle is not None
        joined = lane.join(
            cast('_JoinSource', source),
            component._lifecycle.channel,
            catch_up=lambda boundary: component._catch_up_eager(
                resources,
                cast('JoinTarget', SimpleNamespace(identity=SimpleNamespace(boundary=boundary))),
            ),
        )
        assert isinstance(joined, LiveJoinIdentity)
        token = joined.attempt_token
        assert request.call_count == confirm.call_count == finalize.call_count == 1
        assert request.call_args.args[0].attempt.attempt_token == token
        assert confirm.call_args.args[0].identity.attempt_token == token
        assert finalize.call_args.args[0].identity.attempt_token == token
        assert abort.call_count == 0
        assert isinstance(lane._join_state, _FinalizedJoin)
        assert lane._join_state.identity.attempt_token == token
        assert invoked == list(range(42, 48))
        assert state.snapshot().cursor == 47
        slices = [(cursor, count) for kind, cursor, count in trace if kind == 'slice']
        assert sum(count for _cursor, count in slices) == 6
        assert len(slices) <= 6
        assert all(count > 0 for _cursor, count in slices)
        assert any(kind == 'coordinate' for kind, _cursor, _count in trace)


@pytest.mark.parametrize('prefix_count', [0, 2])
def test_eager_catchup_repeated_setup_expiry_has_bounded_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefix_count: int
) -> None:
    with _empty_slice_catchup(tmp_path, monkeypatch, prefix_count, guard=(None, False)) as (
        component,
        resources,
        state,
        lane,
        invoked,
        trace,
    ):
        source = _PressureJoinSource(
            boundary=47,
            live_event_id=None,
            source_generation='source-generation-1',
            stream_incarnation='stream-incarnation-1',
        )
        request = Mock(wraps=source.request)
        confirm = Mock(wraps=source.confirm)
        finalize = Mock(wraps=source.finalize)
        abort = Mock(wraps=source.abort)
        monkeypatch.setattr(source, 'request', request)
        monkeypatch.setattr(source, 'confirm', confirm)
        monkeypatch.setattr(source, 'finalize', finalize)
        monkeypatch.setattr(source, 'abort', abort)
        assert component._lifecycle is not None
        joined = lane.join(
            cast('_JoinSource', source),
            component._lifecycle.channel,
            catch_up=lambda boundary: component._catch_up_eager(
                resources,
                cast('JoinTarget', SimpleNamespace(identity=SimpleNamespace(boundary=boundary))),
            ),
        )
        assert isinstance(joined, LiveJoinIdentity)
        token = joined.attempt_token
        assert request.call_count == confirm.call_count == finalize.call_count == 1
        assert request.call_args.args[0].attempt.attempt_token == token
        assert confirm.call_args.args[0].identity.attempt_token == token
        assert finalize.call_args.args[0].identity.attempt_token == token
        assert abort.call_count == 0
        assert isinstance(lane._join_state, _FinalizedJoin)
        assert lane._join_state.identity.attempt_token == token
        assert invoked == list(range(42, 48))
        assert state.snapshot().cursor == 47
        setups = [count for kind, _cursor, count in trace if kind == 'setup']
        assert len(setups) == 6 - prefix_count
        assert all(count > 0 for kind, _cursor, count in trace if kind == 'slice')
        assert sum(kind == 'coordinate' for kind, _cursor, _count in trace) >= len(setups)


@pytest.mark.parametrize('prefix_count', [0, 2])
@pytest.mark.parametrize('stop', ['park', 'component', 'lane'])
def test_eager_catchup_empty_slice_honors_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prefix_count: int,
    stop: Literal['park', 'component', 'lane'],
) -> None:
    with _empty_slice_catchup(tmp_path, monkeypatch, prefix_count, guard=(stop, None)) as (
        component,
        resources,
        state,
        _lane,
        invoked,
        trace,
    ):
        snapshot, consumed = component._deliver_catchup_page(
            resources, tuple(Event(runtime_event(event_id)) for event_id in range(42, 48))
        )
        empty_cursor = 41 + prefix_count
        assert any(kind == 'entry' and cursor == empty_cursor for kind, cursor, _count in trace)
        if stop != 'component':
            assert trace[-1] == ('coordinate', empty_cursor, 0)
        assert (consumed, invoked, snapshot.cursor) == (
            prefix_count,
            list(range(42, 42 + prefix_count)),
            empty_cursor,
        )
        assert state.snapshot().cursor == empty_cursor
        assert snapshot.status == ('parked' if stop == 'park' else 'running')


@pytest.mark.parametrize('prefix_count', [0, 2])
@pytest.mark.parametrize('stop', ['park', 'component', 'lane'])
@pytest.mark.parametrize('stop_during_setup', [False, True])
def test_eager_catchup_repeated_setup_expiry_honors_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prefix_count: int,
    stop: Literal['park', 'component', 'lane'],
    *,
    stop_during_setup: bool,
) -> None:
    with _empty_slice_catchup(
        tmp_path,
        monkeypatch,
        prefix_count,
        guard=(stop, stop_during_setup),
    ) as (component, resources, state, _lane, invoked, trace):
        snapshot, consumed = component._deliver_catchup_page(
            resources, tuple(Event(runtime_event(event_id)) for event_id in range(42, 48))
        )
        setups = [(cursor, count) for kind, cursor, count in trace if kind == 'setup']
        assert len(setups) == 3
        if stop_during_setup:
            assert snapshot.cursor == setups[-1][0]
        else:
            assert trace[-1][0] == 'coordinate'
        assert consumed == len(invoked)
        assert invoked == list(range(42, 42 + consumed))
        assert state.snapshot().cursor == snapshot.cursor == 41 + consumed
        assert consumed < 6
        assert snapshot.status == ('parked' if stop == 'park' else 'running')


@pytest.mark.parametrize('operation', ['setup', 'resume'])
def test_due_coordination_runs_when_lifecycle_callback_returns(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    component_object, resources_object = lifecycle_probe(
        'sync', durable_status='running', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(channel=resources.channel)
    lifecycle.was_running = True
    lifecycle.next_coordination_at = 101.0
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

    monkeypatch.setattr(resources.lane, operation, callback)
    monkeypatch.setattr(resources.coordination, 'service', coordinate)
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
    component_object, resources_object = lifecycle_probe(
        'sync', durable_status='running', setup_failure=None
    )
    component = component_object
    resources = cast('_LaneResources', resources_object)
    lifecycle = worker_module._LaneLifecycle(channel=resources.channel)
    component._lifecycle = lifecycle
    connected = [True]
    reconnects: list[float] = []
    if failure != 'local':
        vars(resources)['client'] = _unconnected_event_client(tmp_path / 'events.sock')
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
        return resources.channel

    monkeypatch.setattr(EventClient, 'reconnect', reconnect)
    monkeypatch.setattr(component._wake, 'wait', wait)
    monkeypatch.setattr(component, '_build_channel', build_channel)
    monkeypatch.setattr(resources.lane, 'join', join)
    monkeypatch.setattr(resources.coordination, 'service', coordinate)
    if failure == 'remote':
        assert not component._join_lane(resources, lifecycle)
        assert reconnects == [100.0]
        assert clock[0] == 100.001
        assert not lifecycle.joined
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
    path = source_database_path(tmp_path)
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
        surface.reconcile_groups()
        assert surface._durable_generations == {}
        monkeypatch.setattr(source, 'reserve_stream_adoption', Mock())

        def bootstrap(_surface: _SourceJoinSurface) -> None:
            _bootstrap_coordination_group(path)

        def invoke(active: _SourceJoinSurface) -> object:
            if command == 'reserve':
                return active.reserve_stream_adoption(
                    command_fingerprint='fingerprint', reason='test'
                )
            return active.adopt_stream(
                adoption_id='adoption',
                command_fingerprint='fingerprint',
                new_stream_fingerprint='replacement',
                reason='test',
                groups_parked=True,
            )

        mailbox = _OwnerMailbox[worker_module._SourceJoinSurface]('source')
        first = mailbox.submit(bootstrap)
        second = mailbox.submit(invoke)
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
        assert first.complete.is_set()
        assert first.error is None
        assert second.complete.is_set()
        assert second.error is None
        assert surface._durable_generations == {'local': 7}


@pytest.mark.parametrize('drift', ['identity', 'ordering'])
@pytest.mark.parametrize('retry', ['exact', 'conflict', 'stale'])
def test_source_disposition_setup_preserves_durable_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, drift: str, retry: str
) -> None:
    config, spec, assignment, snapshot = pending_group_runtime(tmp_path)
    path = source_database_path(tmp_path)
    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        _runtime,
        control,
    ):
        receive_typed_health(control, lambda update: len(update.groups) == 1)
        command = _pending_group_command(snapshot, 'reset-to-latest')
        send_control(control, command)
        applied = receive_type(control, CommandResult)
        assert applied.ok
        before = drift_disposition_policy(path, 'local', drift)
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
        send_control(control, command)
        result = receive_type(control, CommandResult)
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
    runtime: _WorkerRuntime

    def publish(_health: WorkerHealth) -> None:
        operator = runtime._threads.get('operator-executor')
        if operator is not None and not operator.is_alive():
            snapshots.append(cache.snapshot().watchdogs)
            published_after_operator.set()

    runtime = _WorkerRuntime(
        spec=spec,
        spawn_instance='shutdown-inventory',
        source_factory=lambda name, _slots, _host: components[name],
        lane_factory=lambda name, _slots, _host: components[name],
        health_sink=publish,
        health_cache=cache,
        health_interval_secs=2.0,
        checkpoint_interval_secs=1,
        shutdown_deadline_secs=stall_deadline('shutdown', 10.0),
        max_concurrent_catchup_readers=1,
        force_exit=lambda _code: pytest.fail('cooperative shutdown forced exit'),
    )
    runtime.start()
    try:
        assert all(component.started.wait() for component in components.values())
        if fatal:
            runtime._component_failed('checkpoint:events thread failed: injected bound')
        else:
            runtime.request_stop('ordinary-stop')
        operator = runtime._threads['operator-executor']
        operator.join()
        assert not operator.is_alive()
        runtime._monitor_wake.set()
        assert published_after_operator.wait()
        assert runtime._monitor_thread is not None
        assert runtime._monitor_thread.is_alive()
        for watchdogs in snapshots:
            assert len(watchdogs) == 1
            assert watchdogs[0].kind == 'operator'
            assert watchdogs[0].phase == 'shutdown'
            assert watchdogs[0].deadline_at == runtime._shutdown_deadline_at
        runtime._deregister_operator_watchdog()
        assert len(cache.snapshot().watchdogs) == 1
        for component in components.values():
            component.release.set()
        assert runtime.wait() == ()
        assert not runtime._monitor_thread.is_alive()
        assert cache.snapshot().watchdogs == ()
        assert snapshots[-1][0].deadline_at == runtime._shutdown_deadline_at
    finally:
        for component in components.values():
            component.release.set()
        runtime.request_stop('test-cleanup')
        runtime._shutdown()


@pytest.mark.parametrize('first', ['source', 'lane'])
@pytest.mark.parametrize('fatal', [False, True], ids=['ordinary-stop', 'checkpoint-fatal'])
def test_completed_component_health_survives_terminal_monitor_frame(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, first: str, fatal: bool
) -> None:
    site = runtime_site(group_config('local', worker='default'))
    config = runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    assignment = local_assignment(spec, 'local', 'initialize')
    releases = {kind: threading.Event() for kind in ('source', 'lane')}
    now = [100.0]
    _configure_shutdown_probe_clock(monkeypatch, now)

    def run_body(component: _SourceComponent | _LaneComponent) -> None:
        _run_shutdown_probe_body(component, releases)

    monkeypatch.setattr(_SourceComponent, '_run_body', run_body)
    monkeypatch.setattr(_LaneComponent, '_run_body', run_body)
    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        runtime,
        control,
    ):
        cache = runtime._health_cache
        assert cache is not None
        receive_typed_health(control, lambda update: bool(update.sources) and bool(update.groups))
        try:
            if fatal:
                runtime._component_failed('checkpoint:events thread failed: injected bound')
            else:
                runtime.request_stop('ordinary-stop')
            releases[first].set()
            identity = 'source:events' if first == 'source' else 'lane:local'
            runtime._threads[identity].join()
            runtime._threads['operator-executor'].join()
            assert not runtime._threads[identity].is_alive()
            runtime._close_components()
            assert len(cache.snapshot().watchdogs) == 3
            now[0] = 101.5
            assert runtime._shutdown_deadline_at == 100.0 + config.daemon.shutdown_deadline_secs
            runtime._monitor_wake.set()
            update = receive_typed_health(
                control,
                lambda item: all(watchdog.phase == 'shutdown' for watchdog in item.watchdogs),
            )
            _assert_shutdown_inventory(update, runtime._shutdown_deadline_at)
            for release in releases.values():
                release.set()
            terminal = _terminal_monitor_frame(runtime, control, update)
            _assert_shutdown_inventory(terminal, runtime._shutdown_deadline_at)
            assert runtime.wait() == ()
            assert cache.snapshot().watchdogs == ()
            assert cache.snapshot().sources == ()
            assert cache.snapshot().groups == ()
        finally:
            for release in releases.values():
                release.set()


@pytest.mark.parametrize('stop_during_wait', [False, True])
@pytest.mark.usefixtures('_production_phase_deadlines')
def test_lane_retry_handoff_keeps_current_owner_phase(
    monkeypatch: pytest.MonkeyPatch, stop_during_wait: bool
) -> None:
    component, context, now, cache = _recovery_owner_phase_probe(monkeypatch)
    observations: list[tuple[str, WatchdogHealth]] = []
    original_is_set = component._stop.is_set
    waited = False

    def configure() -> _LaneConfiguration:
        if waited:
            observations.append(('configure', cache.snapshot().watchdogs[0]))
            component.request_stop(shutdown_deadline_at=200.0)
        return context

    def build_client(_context: _LaneConfiguration) -> tuple[EventClient, str]:
        raise worker_module._RemoteBootstrapTransportError('injected remote retry')

    def wait(timeout: float | None = None) -> bool:
        nonlocal waited
        assert timeout == 30.0
        observations.append(('wait', cache.snapshot().watchdogs[0]))
        now[0] = 130.0
        if stop_during_wait or waited:
            component.request_stop(shutdown_deadline_at=200.0)
        waited = True
        return original_is_set()

    def is_set() -> bool:
        if waited:
            observations.append(('handoff', cache.snapshot().watchdogs[0]))
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
    terminal = cache.snapshot().watchdogs[0]
    assert terminal.phase == 'shutdown'
    assert terminal.deadline_at == 200.0
    assert terminal.deadline_at is not None
    assert 135.0 < now[0] < terminal.deadline_at


@pytest.mark.parametrize('stop_with_reload', [False, True])
@pytest.mark.usefixtures('_production_phase_deadlines')
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
            observations.append(cache.snapshot().watchdogs[0])
        return context

    def transition(
        phase: WatchdogPhase, *, deadline_at: float | None, **identities: object
    ) -> None:
        assert not identities
        if phase == 'idle' and builds == 2:
            observations.append(cache.snapshot().watchdogs[0])
        original_transition(phase, deadline_at=deadline_at)

    def drive(_resources: _LaneResources) -> None:
        component._close_partial_attempt(worker_module._PartialLaneAttempt())
        if builds == 1:
            assert component._watchdog.sample().deadline_at == 110.0
            now[0] = 130.0
            component._reload.set()
        if stop_with_reload or builds == 2:
            component.request_stop(shutdown_deadline_at=200.0)

    monkeypatch.setattr(component, '_configuration', configure)
    monkeypatch.setattr(component, '_drive_lane', drive)
    monkeypatch.setattr(component._watchdog, 'transition', transition)
    component._run_body()
    assert builds == (1 if stop_with_reload else 2)
    assert len(observations) == (0 if stop_with_reload else 2)
    for sample in observations:
        assert sample.phase == 'startup'
        assert sample.deadline_at == 140.0
    terminal = cache.snapshot().watchdogs[0]
    assert terminal.phase == 'shutdown'
    assert terminal.deadline_at == 200.0


@contextmanager
def _deadline_monitor_runtime(
    tmp_path: Path,
) -> Generator[tuple[_WorkerRuntime, WorkerControl, WorkerControl, list[float]]]:
    config = runtime_config(tmp_path, _site(()))
    spec = WorkerSpec(
        worker_id='["named","empty"]',
        site='main',
        kind=_WorkerKind.NAMED,
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
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    now = [4.99]
    try:
        runtime = build_worker_runtime(config, spec, assignment, child)
        runtime._outbox = WorkerControlOutbox(child)
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
        cache.register_watchdog(
            key=('source', 'events', 'deadline-sample', token),
            supplier=cell.sample,
        )
        waits: list[float] = []

        def enter_hold() -> None:
            if not runtime._stop_event.is_set():
                now[0] = 5
                cell.transition('write-hold', deadline_at=10)

        def stop_after_wait(seconds: float) -> None:
            waits.append(seconds)
            runtime.request_stop('observed')

        monkeypatch.setattr(runtime, '_poll_control', enter_hold)
        assert runtime._outbox is not None
        monkeypatch.setattr(runtime._outbox, 'wait', stop_after_wait)
        runtime._monitor()
        first = receive_control(parent)
        assert isinstance(first, HealthUpdate)
        hold = next(item for item in first.watchdogs if item.kind == 'source')
        assert first.worker.heartbeat_at == 4.99 < hold.heartbeat_at == 5
        assert hold.deadline_at == 10
        assert waits == [worker_module.shutdown_slice_secs(runtime._shutdown_deadline_secs)]
        assert isinstance(receive_control(parent), HealthUpdate)


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
        cache.register_watchdog(
            key=('lane', 'local', 'deadline-sample', token),
            supplier=cell.sample,
        )
        cell.transition('callback', deadline_at=5)
        assert runtime._publish_monitor_health(now[0]) == 5
        assert runtime._outbox is not None
        runtime._outbox.write_available()
        assert isinstance(receive_control(parent), HealthUpdate)
        now[0] = 5.1
        cell.heartbeat(sliding_deadline_secs=2)
        assert runtime._publish_monitor_health(now[0]) == 7.1
        assert runtime._outbox is not None
        runtime._outbox.write_available()
        assert isinstance(receive_control(parent), HealthUpdate)
        cell.transition('internal', deadline_at=6)
        with cell.watch('write-hold', deadline_at=10):
            now[0] = 7
        assert runtime._publish_monitor_health(now[0]) == 7.9
        assert runtime._outbox is not None
        runtime._outbox.write_available()
        restored = receive_control(parent)
        assert isinstance(restored, HealthUpdate)
        lane = next(item for item in restored.watchdogs if item.kind == 'lane')
        assert lane.phase == 'internal'
        assert lane.deadline_at == 7.9 > restored.worker.heartbeat_at
        # The pending deadline at 7.9 precedes the next periodic wake from 7.0.
        assert runtime._monitor_wait_secs(7.9) == pytest.approx(0.9)


@pytest.mark.parametrize('during_send', [False, True])
def test_monitor_schedules_from_sent_snapshot_and_confirms_without_spin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, during_send: bool
) -> None:
    with _deadline_monitor_runtime(tmp_path) as (runtime, parent, _child, now):
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

            cache.register_watchdog(
                key=('source', identity, 'deadline-sample', identity),
                supplier=sample,
            )
        assert runtime._outbox is not None
        write = runtime._outbox.write_available

        def delayed_write() -> None:
            write()
            if during_send:
                now[0] = max(now[0], 5.005)

        monkeypatch.setattr(runtime._outbox, 'write_available', delayed_write)
        pending = runtime._publish_monitor_health(4.99)
        runtime._outbox.write_available()
        frame = receive_control(parent)
        assert isinstance(frame, HealthUpdate)
        assert samples == ['events', 'later']
        assert frame.worker.heartbeat_at == 4.99
        assert pending == 5
        assert runtime._monitor_wait_secs(pending) == pytest.approx(0 if during_send else 0.01)
        now[0] = 5.005
        pending = runtime._publish_monitor_health(now[0])
        runtime._outbox.write_available()
        first_confirmation = receive_control(parent)
        assert isinstance(first_confirmation, HealthUpdate)
        assert first_confirmation.worker.heartbeat_at >= 5
        assert pending == 7
        assert runtime._monitor_wait_secs(pending) == pytest.approx(
            worker_module.shutdown_slice_secs(runtime._shutdown_deadline_secs)
        )
        now[0] = 7
        pending = runtime._publish_monitor_health(now[0])
        runtime._outbox.write_available()
        confirmed = receive_control(parent)
        assert isinstance(confirmed, HealthUpdate)
        assert confirmed.worker.heartbeat_at == 7
        assert pending is None
        assert runtime._monitor_wait_secs(pending) == worker_module.shutdown_slice_secs(
            runtime._shutdown_deadline_secs
        )
        runtime.request_stop('test-stop')
        runtime._shutdown_deadline_at = 7.25
        assert runtime._monitor_wait_secs(8) == 0.25


@pytest.mark.parametrize('operation', ['reserve', 'adopt'])
def test_operator_writes_are_busy_until_the_stamp(tmp_path: Path, operation: str) -> None:
    with _open_coordination_source(tmp_path / 'operator-stamp.db', stamped=False) as state:
        surface = _initializing_source_surface(state)
        surface._poller = cast(
            'SourcePoller',
            SimpleNamespace(
                event_channel_lost=Mock(),
                adopt_stream=Mock(),
            ),
        )
        if operation == 'reserve':
            with pytest.raises(SourceDatabaseBusyError):
                surface.reserve_stream_adoption(command_fingerprint='command', reason='test')
        else:
            with pytest.raises(SourceDatabaseBusyError):
                surface.adopt_stream(
                    adoption_id='adoption',
                    command_fingerprint='command',
                    new_stream_fingerprint='changed',
                    reason='test',
                    groups_parked=True,
                )


def _run_lane_detach_process(
    tmp_path: Path,
    packing: Literal['default', 'named'],
    fault: str,
    *,
    repeats: int = 1,
    settlement: tuple[str | None, bool] = (None, False),
) -> None:
    abort_failure, outcome_failure = settlement
    fatal = outcome_failure or fault in {
        'boundary-conflict',
        'ownership-conflict',
        'channel-closed',
    }
    config = _lane_detach_config(tmp_path, packing, fault)
    reason = 'channel-closed' if fault == 'channel-closed' else 'strict-buffer-capacity'
    patches = ChildPatches(
        gate_events=True,
        last_event_id=6 * repeats,
        lane_fault=fault,
        lane_fault_count=repeats,
        lane_detach_reason=reason,
        lane_abort_failure=abort_failure,
        lane_outcome_failure=outcome_failure,
    )
    with running_supervisor(config, patches) as probe:
        expected_exits = tuple(
            spawn.worker.spawn_instance
            for spawn in probe.supervisor._spawns.values()
            if fatal and 'local' in spawn.worker.spec.groups
        )
        probe.wait_for(
            lambda: (
                bool(probe.terminations)
                or any(item['kind'] == 'promoted' for item in _lane_probe_records(tmp_path))
            ),
            expected_exits=expected_exits,
        )
        if fault not in {'overflow', 'remote-queue'}:
            (tmp_path / 'events-ready').touch()
        expected_callbacks = _wait_for_detach_delivery(
            probe, tmp_path, fault, repeats, expected_exits=expected_exits
        )
        records = _lane_probe_records(tmp_path)
        injected = [item for item in records if item['kind'] == 'injected']
        _assert_settlement_probe(records, probe, injected, settlement, repeats)
        if fatal:
            assert probe.terminations, probe.failure_details()
            assert not any(
                'rejoining through catch-up' in spawn.log_path.read_text()
                for spawn in probe.spawns
            )
            return
        assert not probe.terminations, probe.failure_details()
        assert len(probe.spawns) == (1 if packing == 'default' else 2), probe.failure_details()
        assert probe.records('local') == expected_callbacks
        catchups = [item for item in records if item['kind'] == 'catchup']
        assert len(catchups) == repeats + 1
        assert len({item['token'] for item in catchups}) == repeats + 1
        if fault == 'overflow':
            assert _overflow_recovery_promoted(records, repeats), probe.failure_details()
        logs = '\n'.join(spawn.log_path.read_text() for spawn in probe.spawns)
        lines = [line for line in logs.splitlines() if 'live channel detached:' in line]
        assert len(lines) == repeats, logs
        assert all(
            'INFO sg_event_daemon.group:' in line
            and 'group=local' in line
            and 'rejoining through catch-up' in line
            for line in lines
        )
        if fault not in {'overflow', 'remote-queue', 'peer-disconnect'}:
            assert all(f'live channel detached: {reason};' in line for line in lines)
        elif fault in {'overflow', 'remote-queue'}:
            observed = [item['reason'] for item in records if item['kind'] == 'overflow']
            assert len(observed) == repeats
            assert all(
                f'live channel detached: {first_reason};' in line
                for line, first_reason in zip(lines, observed, strict=True)
            )
        if fault == 'peer-disconnect':
            assert 'live channel detached: transport-lost;' in lines[0]
        assert 'WARNING' not in logs, logs
        database = tmp_path / 'state/sites/test/sources/admin.db'
        with closing(sqlite3.connect(f'file:{database}?mode=ro', uri=True)) as connection:
            assert connection.execute(
                "SELECT ordering_anomaly_id FROM group_state WHERE group_name = 'local'"
            ).fetchone() == (None,)
        assert all(item['anomalies'] == 0 for item in records if item['kind'] == 'promoted')
        if fault in {'request-confirm', 'confirm-finalize', 'finalize-capacity'}:
            token = injected[0]['token']
            assert [
                item['capacity']
                for item in records
                if item['kind'] == 'abort' and item['token'] == token
            ] == [True]
            assert [
                item['capacity']
                for item in records
                if item['kind'] == 'outcome' and item['token'] == token
            ] == [True]
        if fault in {'overflow', 'remote-queue'} and packing == 'named':
            assert len([item for item in records if item['kind'] == 'server-overflow']) == repeats
        if fault == 'idle-snapshot':
            assert any(item['kind'] == 'idle-snapshot' for item in records)


@pytest.mark.parametrize('packing', ['default', 'named'])
@pytest.mark.parametrize(
    'entry',
    ['request-confirm', 'confirm-finalize', 'finalize-promote', 'idle-snapshot', 'live-loop'],
)
def test_detached_lane_rejoins_through_catch_up(
    tmp_path: Path,
    packing: Literal['default', 'named'],
    entry: str,
) -> None:
    _run_lane_detach_process(tmp_path, packing, entry)


@pytest.mark.parametrize('packing', ['default', 'named'])
@pytest.mark.parametrize('reason', ['queue-count-exhausted', 'channel-closed'])
def test_detach_at_source_promotion_settles_each_owner_once(
    tmp_path: Path, packing: Literal['default', 'named'], reason: str
) -> None:
    config = _lane_detach_config(tmp_path, packing, 'source-promotion')
    patches = ChildPatches(
        gate_events=True,
        last_event_id=6,
        lane_fault='source-promotion',
        promotion_detach_reason=reason,
    )
    with running_supervisor(config, patches) as probe:
        probe.wait_for(
            lambda: (
                bool(probe.terminations)
                or any(item['kind'] == 'promoted' for item in _lane_probe_records(tmp_path))
            ),
            expected_exits=tuple(
                spawn.worker.spawn_instance
                for spawn in probe.supervisor._spawns.values()
                if reason == 'channel-closed' and 'local' in spawn.worker.spec.groups
            ),
        )
        records = _lane_probe_records(tmp_path)
        injected = [item for item in records if item['kind'] == 'injected']
        assert len(injected) == 1, probe.failure_details()
        token = injected[0]['token']
        if reason == 'channel-closed':
            assert probe.terminations, probe.failure_details()
            logs = '\n'.join(spawn.log_path.read_text() for spawn in probe.spawns)
            assert 'rejoining through catch-up' not in logs
            return
        assert not probe.terminations, probe.failure_details()
        (tmp_path / 'events-ready').touch()
        probe.wait_for(lambda: probe.records('local') == (2, 3, 4, 5, 6))
        assert not probe.terminations, probe.failure_details()
        assert len(probe.spawns) == (1 if packing == 'default' else 2)
        assert probe.records('local') == (2, 3, 4, 5, 6)
        records = _lane_probe_records(tmp_path)
        catchups = [item for item in records if item['kind'] == 'catchup']
        assert len(catchups) == 2
        assert len({item['token'] for item in catchups}) == 2
        logs = '\n'.join(spawn.log_path.read_text() for spawn in probe.spawns)
        assert logs.count('live channel detached: queue-count-exhausted; rejoining') == 1
        assert 'WARNING' not in logs
        for kind in ('source-settlement', 'abort', 'outcome-attempt', 'outcome'):
            settlements = [
                item for item in records if item['kind'] == kind and item['token'] == token
            ]
            assert len(settlements) == 1, (kind, records, probe.failure_details())
            if kind != 'source-settlement':
                assert settlements[0]['capacity'] is False
        assert [item['reason'] for item in records if item['kind'] == 'source-detach'] == [reason]
        if packing == 'named':
            assert [item['reason'] for item in records if item['kind'] == 'transport-held'] == [
                reason
            ]
            assert [
                (item['reason'], item['error']) for item in records if item['kind'] == 'wire-reply'
            ] == [(reason, None)]
            assert [
                (item['reason'], item['connected'])
                for item in records
                if item['kind'] == 'remote-detach'
            ] == [(reason, True)]


@pytest.mark.parametrize('packing', ['default', 'named'])
@pytest.mark.parametrize('entry', ['success', 'request-retry'])
def test_join_outcome_write_failure_fails_worker_without_retry(
    tmp_path: Path,
    packing: Literal['default', 'named'],
    entry: Literal['success', 'request-retry'],
) -> None:
    config = _lane_detach_config(tmp_path, packing, 'success-outcome')
    with running_supervisor(
        config,
        ChildPatches(
            gate_events=True,
            lane_fault='success-outcome',
            lane_outcome_write_fault=entry,
        ),
    ) as probe:
        probe.wait_for(
            lambda: bool(probe.terminations),
            expected_exits=tuple(
                spawn.worker.spawn_instance
                for spawn in probe.supervisor._spawns.values()
                if 'local' in spawn.worker.spec.groups
            ),
        )
        records = _lane_probe_records(tmp_path)
        attempts = [item for item in records if item['kind'] == 'outcome-attempt']
        assert len(attempts) == 1, records
        assert attempts[0]['capacity'] is (entry == 'request-retry')
        assert not any(item['kind'] in {'outcome', 'abort', 'promoted'} for item in records)
        assert not probe.records('local')
        logs = '\n'.join(spawn.log_path.read_text() for spawn in probe.spawns)
        assert 'OperationalError: join outcome write failed' in logs
        assert 'rejoining through catch-up' not in logs


@pytest.mark.parametrize(
    ('fault', 'status', 'remedy'),
    [
        ('encoder-failed', 'encoder-failed', 'encoder'),
        ('event-too-large', 'event-too-large', 'max_event_bytes'),
        ('frame-too-large', 'frame-too-large', 'max_ipc_frame_bytes'),
        ('commit-batch-capacity-exhausted', 'frame-too-large', 'commit_batch_max_bytes'),
    ],
)
def test_data_halt_keeps_the_worker_up(
    tmp_path: Path, fault: str, status: str, remedy: str
) -> None:
    config = write_daemon_config(tmp_path, Topology('default', ('local',)))
    commit_limit = (
        'commit_batch_max_bytes = 6000000\n' if fault == 'commit-batch-capacity-exhausted' else ''
    )
    content = config.read_text().replace(
        '[sites.test.sources.admin]\n',
        '[sites.test.sources.admin]\nbatch_size = 1\n' + commit_limit,
    )
    config.write_text(content)
    patches = ChildPatches(
        source_halt=fault, gate_events=True, last_event_id=6, hold_callback=True
    )
    halts = tmp_path / 'source-halts'
    queries = tmp_path / 'source-queries'
    callback_entered = tmp_path / 'callback-entered'
    callback_release = tmp_path / 'callback-release'
    with running_supervisor(config, patches) as probe:
        probe.wait_for(
            lambda: any(
                update.sources
                and update.sources[0].initialized
                and update.groups
                and update.groups[0].join_phase == 'attached'
                for update in probe.latest_health.values()
            ),
        )
        (tmp_path / 'callback-arm').touch()
        (tmp_path / 'events-ready').touch()
        try:
            probe.wait_for(callback_entered.exists)
            probe.wait_for(halts.exists)
            assert not callback_release.exists()
            assert probe.records('local') == ()
            assert not probe.terminations, probe.failure_details()
            probe.wait_for(
                lambda: any(
                    update.sources and update.sources[0].status == status
                    for update in probe.latest_health.values()
                ),
            )
            update = next(
                update
                for update in probe.latest_health.values()
                if update.sources and update.sources[0].status == status
            )
            aggregate = status_from_health(
                sources=update.sources,
                groups=update.groups,
                workers=(update.worker,),
                now=update.worker.heartbeat_at,
                stale_after_secs=1.0,
            )
            assert aggregate.liveness
            assert not aggregate.readiness
            assert update.sources[0].status == status
            assert update.sources[0].frontier == 4
        finally:
            callback_release.touch()
        probe.wait_for(lambda: probe.records('local') == (2, 3, 4))
        assert not probe.terminations, probe.failure_details()
        probe.wait_for(lambda: len(read_complete_lines(halts)) >= 4)
        query_count = len(read_complete_lines(queries))
        first_halt_count = len(read_complete_lines(halts))
        probe.wait_for(lambda: len(read_complete_lines(halts)) >= first_halt_count + 3)
        assert len(read_complete_lines(queries)) == query_count
        assert not probe.terminations, probe.failure_details()

    if remedy == 'encoder':
        (tmp_path / 'encoder-fixed').touch()
    elif remedy == 'max_ipc_frame_bytes':
        content = content.replace('[daemon]\n', '[daemon]\nmax_ipc_frame_bytes = 16000000\n')
    elif remedy == 'commit_batch_max_bytes':
        content = content.replace(
            'commit_batch_max_bytes = 6000000', 'commit_batch_max_bytes = 12000000'
        )
    else:
        content = content.replace(
            '[sites.test.sources.admin]\n',
            '[sites.test.sources.admin]\nmax_event_bytes = 10000000\n',
        )
        content = content.replace('[daemon]\n', '[daemon]\nmax_ipc_frame_bytes = 16000000\n')
    config.write_text(content)
    with running_supervisor(config, patches) as probe:
        probe.wait_for(lambda: probe.records('local') == (2, 3, 4, 5, 6))
        assert not probe.terminations, probe.failure_details()


def test_remote_peer_disconnect_rejoins_on_next_iteration(tmp_path: Path) -> None:
    _run_lane_detach_process(tmp_path, 'named', 'peer-disconnect')


@pytest.mark.parametrize('packing', ['default', 'named'])
def test_strict_finalize_capacity_rejoins(
    tmp_path: Path, packing: Literal['default', 'named']
) -> None:
    _run_lane_detach_process(tmp_path, packing, 'finalize-capacity')


@pytest.mark.parametrize('packing', ['default', 'named'])
@pytest.mark.parametrize('conflict', ['boundary-conflict', 'ownership-conflict', 'channel-closed'])
def test_protocol_conflicts_fail_the_worker(
    tmp_path: Path,
    packing: Literal['default', 'named'],
    conflict: str,
) -> None:
    _run_lane_detach_process(tmp_path, packing, conflict)


@pytest.mark.parametrize('packing', ['default', 'named'])
def test_eager_lane_rejoins_after_repeated_overflow(
    tmp_path: Path,
    packing: Literal['default', 'named'],
) -> None:
    _run_lane_detach_process(tmp_path, packing, 'overflow', repeats=3)


@pytest.mark.parametrize('packing', ['default', 'named'])
@pytest.mark.parametrize('entry', ['request-confirm', 'finalize-capacity'])
def test_join_outcome_write_failure_fails_worker(
    tmp_path: Path,
    packing: Literal['default', 'named'],
    entry: str,
) -> None:
    _run_lane_detach_process(tmp_path, packing, entry, settlement=(None, True))


@pytest.mark.parametrize('packing', ['default', 'named'])
@pytest.mark.parametrize('abort_failure', ['transport', 'protocol'])
def test_typed_detach_rejoins_after_expected_source_abort_failure(
    tmp_path: Path,
    packing: Literal['default', 'named'],
    abort_failure: str,
) -> None:
    _run_lane_detach_process(
        tmp_path, packing, 'request-confirm', settlement=(abort_failure, False)
    )


class _BusySourceRecord(TypedDict):
    kind: str
    at: float
    reason: NotRequired[str]
    timeout: NotRequired[float]
    spawn_instance: NotRequired[str]
    logical_at: NotRequired[float]
    deadline_at: NotRequired[float]
    deadline_secs: NotRequired[float]
    page: NotRequired[int]
    pages: NotRequired[int]
    token: NotRequired[str]


def _busy_source_records(root: Path) -> list[_BusySourceRecord]:
    path = root / 'busy-source.jsonl'
    return [json.loads(line) for line in read_complete_lines(path)]


def _wait_for_cap_heartbeat(probe: SupervisorProbe, root: Path) -> tuple[_BusySourceRecord, float]:
    probe.wait_for(
        lambda: (
            any(item['kind'] == 'cap-enter' for item in _busy_source_records(root))
            or bool(probe.terminations)
        ),
    )
    assert not probe.terminations, probe.failure_details()
    entered = next(item for item in _busy_source_records(root) if item['kind'] == 'cap-enter')
    expected_spawn = entered.get('spawn_instance')
    assert expected_spawn is not None
    receipt: list[float] = []

    def received_while_blocked() -> bool:
        records = _busy_source_records(root)
        assert not any(item['kind'] == 'cap-exit' for item in records), probe.failure_details()
        assert not any(
            item['kind'] == 'connect-retry'
            or (
                item['kind'] == 'remote-detach'
                and item.get('reason') == 'coordination-reply-timeout'
            )
            for item in records
        ), 'cap finished before heartbeat observation'
        assert not probe.terminations, probe.failure_details()
        for spawn_instance, heartbeat_at, received in probe.cap_heartbeats:
            if spawn_instance == expected_spawn and heartbeat_at > entered['at']:
                receipt.append(received)
                with (root / 'cap-heartbeat.jsonl').open('a') as stream:
                    stream.write(
                        json.dumps(
                            {
                                'entered_at': entered['at'],
                                'heartbeat_at': heartbeat_at,
                                'received_at': received,
                                'spawn_instance': spawn_instance,
                            }
                        )
                        + '\n'
                    )
                return True
        return False

    probe.wait_for(received_while_blocked)
    return entered, receipt[0]


def test_busy_source_deadline_probe_selects_only_its_operator_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    probe = _BusySourceProbe('operator-expired', tmp_path)
    component = object.__new__(_SourceComponent)
    component._mailbox = _OwnerMailbox('source')

    def call(
        owner: _SourceComponent,
        invoke: Callable[[_SourceJoinSurface], object],
        *,
        stop: threading.Event,
        wait_phase: Callable[[], AbstractContextManager[None]],
        queue_deadline_secs: float | None = None,
    ) -> object:
        del stop, wait_phase, queue_deadline_secs
        queued = owner._mailbox.submit(invoke)
        assert not queued.complete.wait(0)
        assert owner._mailbox.take() is queued
        return 'observed'

    def execute(
        owner: _SourceComponent,
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        del command
        return {
            'result': owner.call(
                lambda _surface: None,
                stop=threading.Event(),
                wait_phase=wait_phase,
                queue_deadline_secs=192.0,
            )
        }

    monkeypatch.setattr(_SourceComponent, 'call', call)
    monkeypatch.setattr(_SourceComponent, 'execute_operator_command', execute)
    _install_busy_source_calls(monkeypatch, probe)
    component.call(
        lambda _surface: None,
        stop=threading.Event(),
        wait_phase=nullcontext,
        queue_deadline_secs=192.0,
    )
    supervisor = object.__new__(Supervisor)
    supervisor._operator_request_sequence = 0
    command = supervisor._next_operator_transport_command(_busy_source_operator())
    assert command.request_id == 'operator-1'
    component.execute_operator_command(replace(command, command='adopt-source-stream'))
    assert _busy_source_records(tmp_path) == []
    assert not (tmp_path / 'operator-call-finished').exists()
    assert component.execute_operator_command(command) == {'result': 'observed'}
    assert [item['kind'] for item in _busy_source_records(tmp_path)] == [
        'operator-deadline-crossed'
    ]
    assert (tmp_path / 'operator-call-finished').exists()


def test_busy_source_join_gate_waits_for_the_selected_join(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    probe = _BusySourceProbe('long-recheck', tmp_path)
    (tmp_path / 'busy-entered').touch()
    probe.calls.join_token = None
    component = object.__new__(_SourceComponent)
    component._mailbox = _OwnerMailbox('source')
    selected = False

    def call(
        owner: _SourceComponent,
        invoke: Callable[[_SourceJoinSurface], object],
        *,
        stop: threading.Event,
        wait_phase: Callable[[], AbstractContextManager[None]],
        queue_deadline_secs: float | None = None,
    ) -> object:
        del stop, wait_phase, queue_deadline_secs
        queued = owner._mailbox.submit(invoke)
        assert (tmp_path / 'source-join-submitted').exists() is selected
        assert owner._mailbox.take() is queued
        probe.page = 1
        result = queued.invoke(cast('_SourceJoinSurface', object()))
        # A delayed caller must not change the source-owner service page.
        probe.page = 2
        return result

    monkeypatch.setattr(_SourceComponent, 'call', call)
    _install_busy_source_calls(monkeypatch, probe)
    assert (
        component.call(lambda _surface: 'metadata', stop=threading.Event(), wait_phase=nullcontext)
        == 'metadata'
    )
    assert _busy_source_records(tmp_path) == []
    selected = True
    probe.page = 0
    probe.calls.join_request = True
    token = os.urandom(16).hex()
    probe.calls.join_token = token
    assert (
        component.call(lambda _surface: 'joined', stop=threading.Event(), wait_phase=nullcontext)
        == 'joined'
    )
    assert [
        (item['kind'], item.get('page'), item.get('token'))
        for item in _busy_source_records(tmp_path)
    ] == [
        ('join-submitted', 0, token),
        ('join-start', 1, token),
        ('join-served', 1, token),
    ]


@pytest.mark.parametrize(
    'case',
    ['connect', 'connect-auth', 'reconnect', 'reconnect-auth', 'connect-cap', 'connect-auth-cap'],
)
def test_connection_probe_publishes_wait_evidence_before_readiness(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, case: str
) -> None:
    probe = _BusySourceProbe(case, tmp_path)
    client = object.__new__(EventClient)
    client._stop = threading.Event()
    client._stop.set()
    client._spawn_instance = 'test-worker'
    active = threading.local()
    active.client = client
    active.cap_entered = False
    (tmp_path / 'cap-heartbeat.jsonl').touch()
    touch = Path.touch
    publications: list[float] = []

    def publish(path: Path, mode: int = 0o666, exist_ok: bool = True) -> None:
        if path.name == 'wait-entered':
            waits = [
                item for item in _busy_source_records(tmp_path) if item['kind'] == 'socket-wait'
            ]
            assert len(waits) == 1
            recorded_timeout = waits[0].get('timeout')
            assert recorded_timeout is not None
            publications.append(recorded_timeout)
        touch(path, mode=mode, exist_ok=exist_ok)

    def timeout() -> float:
        return 0.5

    monkeypatch.setattr(Path, 'touch', publish)
    _install_connection_socket_faults(monkeypatch, probe, active)
    sock = cast('socket.socket', SimpleNamespace(gettimeout=timeout))

    def block() -> None:
        if 'auth' in case:
            socket.socket.send(sock, b'auth')
        else:
            socket.socket.connect(sock, 'unused')

    with pytest.raises(TimeoutError, match='injected full event socket'):
        block()
    assert publications == [0.5]


def test_lane_result_probe_observes_only_the_selected_retry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    probe = _BusySourceProbe('lane-result', tmp_path)
    waiting = object.__new__(_LaneComponent)
    healthy = object.__new__(_LaneComponent)
    for component, group in ((waiting, 'waiting'), (healthy, 'healthy')):
        component._group = group
        component._mailbox = _OwnerMailbox(group)
    serviced: list[str] = []

    def call(
        owner: _LaneComponent,
        invoke: Callable[[GroupState, EventClient | worker_module._LocalJoinClient], object],
    ) -> Mapping[str, object]:
        del invoke
        owner._service_calls(cast('_LaneResources', object()))
        queued = owner._mailbox.submit(lambda _resources: None)
        assert not queued.complete.wait(0.0)
        assert owner._mailbox.take() is queued
        return {'observed': True}

    def execute(
        owner: _LaneComponent,
        command: OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        del command, wait_phase
        return owner._call(lambda _state, _client: {'observed': True})

    def service(owner: _LaneComponent, _resources: _LaneResources) -> None:
        serviced.append(owner._group)

    monkeypatch.setattr(_LaneComponent, '_call', call)
    monkeypatch.setattr(_LaneComponent, 'execute_operator_command', execute)
    monkeypatch.setattr(_LaneComponent, '_service_calls', service)
    _install_lane_call_wait_probe(monkeypatch, probe)
    command = OperatorCommand(
        request_id='probe',
        command='retry',
        arguments={'site': 'test', 'group': 'waiting'},
        identity=AuditIdentity.current(),
    )
    waiting._call(lambda _state, _client: None)
    healthy.execute_operator_command(
        replace(command, arguments={'site': 'test', 'group': 'healthy'})
    )
    waiting.execute_operator_command(replace(command, command='park'))
    assert not (tmp_path / 'wait-entered').exists()
    assert _busy_source_records(tmp_path) == []
    assert waiting.execute_operator_command(command) == {'observed': True}
    assert (tmp_path / 'wait-entered').exists()
    waiting._call(lambda _state, _client: None)
    assert serviced == ['waiting', 'healthy', 'waiting', 'waiting']
    assert [item['kind'] for item in _busy_source_records(tmp_path)] == ['lane-result-wait']


def test_long_recheck_probe_holds_page_one_until_join_submission(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    probe = _BusySourceProbe('long-recheck', tmp_path, active=True)
    handle = DeterministicHandle(seed=0.0, last_event_id=1, event_gate=None)
    waits: list[int] = []

    def submit_join(_seconds: float) -> None:
        assert probe.page == 1
        assert (tmp_path / 'busy-query-entered').exists()
        assert [item['kind'] for item in _busy_source_records(tmp_path)] == ['page-start']
        waits.append(probe.page)
        (tmp_path / 'source-join-submitted').touch()

    helper = sys.modules[_install_busy_source_queries.__module__]
    monkeypatch.setattr(
        helper, 'time', SimpleNamespace(monotonic=time.monotonic, sleep=submit_join)
    )
    monkeypatch.setattr(threading.current_thread(), 'name', 'test:source:admin')
    _install_busy_source_queries(monkeypatch, probe)
    assert handle.find('EventLogEntry', [], ['id'], limit=2)
    assert waits == [1]
    assert [item['kind'] for item in _busy_source_records(tmp_path)] == ['page-start', 'page-end']


def test_long_recheck_serves_a_join_within_one_page(tmp_path: Path) -> None:
    config = _busy_source_config(tmp_path, 'long-recheck')
    with running_supervisor(
        config, ChildPatches(busy_source_case='long-recheck', gate_events=True, last_event_id=1)
    ) as probe:
        _arm_busy_source(probe, tmp_path)
        probe.wait_for(lambda: (tmp_path / 'busy-finished').exists() or bool(probe.terminations))
        records = _busy_source_records(tmp_path)
        queued = next(item for item in records if item['kind'] == 'join-submitted')
        first_page = next(item for item in records if item['kind'] == 'page-start')
        served = [item for item in records if item['kind'] == 'join-served']
        assert queued.get('page') == 1
        assert records.index(first_page) < records.index(queued)
        assert served, probe.failure_details()
        token = queued.get('token')
        assert token is not None
        assert served[0].get('token') == token
        assert records.index(first_page) < records.index(queued) < records.index(served[0])
        assert served[0].get('page') == 1
        end = next(item for item in records if item['kind'] == 'cycle-end')
        assert end.get('pages', 0) > 3
        assert not probe.terminations, probe.failure_details()


@pytest.mark.parametrize('retry', [False, True])
def test_operator_call_expired_in_queue_is_source_busy(tmp_path: Path, retry: bool) -> None:
    case = 'operator-retry' if retry else 'operator-expired'
    config = _busy_source_config(tmp_path, case)
    with running_supervisor(
        config, ChildPatches(busy_source_case=case, gate_events=True, last_event_id=1)
    ) as probe:
        _arm_busy_source(probe, tmp_path)
        command = _busy_source_operator()
        result = probe.supervisor.dispatch_operator_command(command, timeout=float('inf'))
        assert not result.ok
        assert not any(
            item['kind'] == 'operator-result' for item in _busy_source_records(tmp_path)
        )
        assert not probe.terminations, probe.failure_details()
        if retry:
            probe.wait_for((tmp_path / 'busy-finished').exists)
            first = probe.supervisor.dispatch_operator_command(command, timeout=float('inf'))
            second = probe.supervisor.dispatch_operator_command(command, timeout=float('inf'))
            assert first.ok
            assert second.ok
            assert first.result == second.result

        assert result.result.get('code') == 'source-busy', probe.failure_details()
        assert (
            sum(
                item['kind'] == 'operator-deadline-crossed'
                for item in _busy_source_records(tmp_path)
            )
            == 1
        )


def test_operator_call_started_before_its_deadline_returns_its_result(tmp_path: Path) -> None:
    config = _busy_source_config(tmp_path, 'operator-started')
    with running_supervisor(
        config,
        ChildPatches(busy_source_case='operator-started', gate_events=True, last_event_id=1),
    ) as probe:
        probe.wait_for(lambda: ('test', 'admin') in probe.supervisor._source_workers)
        (tmp_path / 'busy-arm').touch()
        result = probe.supervisor.dispatch_operator_command(
            _busy_source_operator(), timeout=float('inf')
        )
        assert result.ok, probe.failure_details()
        records = _busy_source_records(tmp_path)
        assert [item['kind'] for item in records if item['kind'].startswith('operator-')] == [
            'operator-start',
            'operator-deadline-crossed',
            'operator-result',
        ]
        assert isinstance(result.result['adoption_id'], str)
        assert not probe.terminations, probe.failure_details()


@pytest.mark.parametrize('case', ['operator-started', 'operator-expired', 'operator-retry'])
def test_operator_probe_leaves_an_initial_join_after_arming_ungated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, case: str
) -> None:
    probe = _BusySourceProbe(case, tmp_path)
    (tmp_path / 'busy-arm').touch()
    client = object.__new__(EventClient)
    result = object()
    request = JoinRequest(
        JoinAttemptIdentity(
            site='test',
            source='admin',
            group='local',
            join_kind='eager',
            group_generation=1,
            attempt_token=os.urandom(16).hex(),
        )
    )

    def original(
        _client: EventClient, value: JoinRequest, _channel: GroupChannelInterface
    ) -> object:
        assert value is request
        return result

    def unexpected_wait(_seconds: float) -> NoReturn:
        pytest.fail('operator probe gated an unrelated initial join')

    monkeypatch.setattr(EventClient, 'request', original)
    monkeypatch.setattr(
        sys.modules[_install_busy_source_request.__module__],
        'time',
        SimpleNamespace(monotonic=time.monotonic, time=time.time, sleep=unexpected_wait),
    )
    _install_busy_source_request(monkeypatch, probe, EventClient)
    with closing(
        GroupChannel(group='local', ordering='eager', max_events=8, max_bytes=262_144)
    ) as channel:
        assert client.request(request, channel) is result
    assert _busy_source_records(tmp_path) == []
    assert not (tmp_path / 'busy-call-queued').exists()


def test_remote_bootstrap_request_outlasting_its_cap_retries(tmp_path: Path) -> None:
    config = _busy_source_config(tmp_path, 'remote-bootstrap')
    with running_supervisor(
        config,
        ChildPatches(busy_source_case='remote-bootstrap', gate_events=True, last_event_id=1),
    ) as probe:
        probe.wait_for(
            lambda: (
                any(
                    group.join_phase == 'attached'
                    for health in probe.latest_health.values()
                    for group in health.groups
                )
                or bool(probe.terminations)
            ),
        )
        records = _busy_source_records(tmp_path)
        assert any(
            item['kind'] == 'remote-detach' and item.get('reason') == 'coordination-reply-timeout'
            for item in records
        ), probe.failure_details()
        sent = next(item for item in records if item['kind'] == 'observe-send')
        expired = next(
            item
            for item in records
            if item['kind'] == 'remote-detach'
            and item.get('reason') == 'coordination-reply-timeout'
        )
        assert expired.get('logical_at', float('nan')) - sent.get(
            'logical_at', float('nan')
        ) == pytest.approx(_busy_source_budget(config))
        assert sent.get('deadline_secs') == _busy_source_budget(config)
        assert not probe.terminations, probe.failure_details()
        assert any(
            group.join_phase == 'attached'
            for health in probe.latest_health.values()
            for group in health.groups
        )


def test_remote_request_times_out_at_its_cap_and_rejoins(tmp_path: Path) -> None:
    config = _busy_source_config(tmp_path, 'remote-cap')
    with running_supervisor(
        config, ChildPatches(busy_source_case='remote-cap', gate_events=True, last_event_id=1)
    ) as probe:
        _arm_busy_source(probe, tmp_path)
        entered, received = _wait_for_cap_heartbeat(probe, tmp_path)
        probe.wait_for(
            lambda: (
                any(item['kind'] == 'catchup' for item in _busy_source_records(tmp_path))
                or bool(probe.terminations)
            ),
        )
        records = _busy_source_records(tmp_path)
        assert any(
            item['kind'] == 'remote-detach' and item.get('reason') == 'coordination-reply-timeout'
            for item in records
        ), probe.failure_details()
        queued = next(item for item in records if item['kind'] == 'call-queued')
        timed_out = next(
            item
            for item in records
            if item['kind'] == 'remote-detach'
            and item.get('reason') == 'coordination-reply-timeout'
        )
        assert queued['at'] < timed_out['at']
        assert not probe.terminations, probe.failure_details()
        ended = next(item for item in records if item['kind'] == 'cap-exit')
        assert entered['at'] < received < ended['at']
        attempt = next(item for item in records if item['kind'] == 'request-attempt')
        assert received < timed_out['at']
        assert timed_out.get('logical_at', float('nan')) - attempt.get(
            'logical_at', float('nan')
        ) == pytest.approx(_busy_source_budget(config))
        assert attempt.get('deadline_secs') == _busy_source_budget(config)
        assert len([item for item in records if item['kind'] == 'request-wait']) > 2
        probe.wait_for(
            lambda: (
                any(
                    health.worker.spawn_instance == entered.get('spawn_instance')
                    and health.worker.heartbeat_at > ended['at']
                    and any(group.join_phase == 'attached' for group in health.groups)
                    for health in probe.latest_health.values()
                )
                or bool(probe.terminations)
            ),
        )
        assert not probe.terminations, probe.failure_details()


def test_retry_service_probe_waits_for_its_poller_and_waiter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    probe = _BusySourceProbe('retry-service', tmp_path)
    selected = object.__new__(SourcePoller)
    unrelated = object.__new__(SourcePoller)
    selected._waiter = worker_module._ComponentWaiter(threading.Event())
    unrelated_waiter = worker_module._ComponentWaiter(threading.Event())
    selected._watchdog = ComponentWatchdog(
        kind='source',
        identity='events',
        worker_instance='worker-1',
        registration_token=os.urandom(16).hex(),
        startup_deadline_secs=30.0,
        clock=lambda: 10.0,
    )
    selected._watchdog.transition('internal', deadline_at=40.0)
    serviced: list[SourcePoller] = []

    def service(
        _owner: _SourceComponent,
        _server: EventServer,
        _surface: _SourceJoinSurface,
        poller: SourcePoller,
    ) -> None:
        serviced.append(poller)

    def poll(_poller: SourcePoller) -> int:
        return 0

    def wait(_waiter: worker_module._ComponentWaiter, _seconds: float) -> bool:
        return False

    monkeypatch.setattr(SourcePoller, '_poll_once', poll)
    monkeypatch.setattr(_SourceComponent, '_service_internal', service)
    monkeypatch.setattr(worker_module._ComponentWaiter, 'wait', wait)
    _install_retry_service_probe(monkeypatch, probe)
    (tmp_path / 'wait-arm').touch()
    with pytest.raises(OSError, match='injected retry'):
        selected._poll_once()
    owner = object.__new__(_SourceComponent)
    server = cast('EventServer', object())
    surface = cast('_SourceJoinSurface', object())
    owner._service_internal(server, surface, unrelated)
    assert _busy_source_records(tmp_path) == []
    owner._service_internal(server, surface, selected)
    assert serviced == [unrelated, selected]
    assert not unrelated_waiter.wait(0.0)
    assert not (tmp_path / 'wait-finished').exists()
    assert [item['kind'] for item in _busy_source_records(tmp_path)] == [
        'service-start',
        'service-end',
    ]
    selected._watchdog.transition('wait', deadline_at=40.0)
    assert not selected._waiter.wait(0.0)
    assert (tmp_path / 'wait-finished').exists()
    assert [item['kind'] for item in _busy_source_records(tmp_path)] == [
        'service-start',
        'service-end',
        'restored-wait',
    ]


def test_service_pass_inside_the_retry_wait_does_not_expire_it(tmp_path: Path) -> None:
    config = _wait_process_config(tmp_path, 'retry-service')
    with running_supervisor(
        config, ChildPatches(wait_case='retry-service', gate_events=True)
    ) as probe:
        probe.wait_for(lambda: bool(probe.latest_health))
        (tmp_path / 'wait-arm').touch()
        probe.wait_for(lambda: (tmp_path / 'wait-finished').exists() or bool(probe.terminations))
        probe.supervisor._tick()
        assert not probe.terminations, probe.failure_details()
        records = _busy_source_records(tmp_path)
        assert [item['kind'] for item in records] == [
            'service-start',
            'service-end',
            'restored-wait',
        ]


def test_slow_page_writes_hold_a_queued_call_until_done(tmp_path: Path) -> None:
    config = _busy_source_config(tmp_path, 'slow-page')
    with running_supervisor(
        config, ChildPatches(busy_source_case='slow-page', gate_events=True, last_event_id=1)
    ) as probe:
        _arm_busy_source(probe, tmp_path)
        probe.wait_for(
            lambda: (
                any(item['kind'] == 'call-result' for item in _busy_source_records(tmp_path))
                or bool(probe.terminations)
            ),
        )
        records = _busy_source_records(tmp_path)
        queued = next(item for item in records if item['kind'] == 'join-submitted')
        results = [item for item in records if item['kind'] == 'call-result']
        assert results, probe.failure_details()
        started = next(item for item in records if item['kind'] == 'join-start')
        served = next(item for item in records if item['kind'] == 'join-served')
        writes = [item for item in records if item['kind'] == 'write-end']
        assert len(writes) == 20
        token = queued.get('token')
        assert token is not None
        assert started.get('token') == served.get('token') == token
        assert records.index(queued) < records.index(writes[0])
        assert records.index(writes[-1]) < records.index(started) < records.index(served)
        assert not probe.terminations, probe.failure_details()


def test_source_call_completes_after_a_blocked_source_resumes(tmp_path: Path) -> None:
    config = _busy_source_config(tmp_path, 'blocked-resume')
    with running_supervisor(
        config, ChildPatches(busy_source_case='blocked-resume', gate_events=True, last_event_id=1)
    ) as probe:
        _arm_busy_source(probe, tmp_path)
        probe.wait_for(
            lambda: (tmp_path / 'source-wait-past-deadline').exists() or bool(probe.terminations)
        )
        try:
            # The source gate and queued-call signal establish the blocked state.
            assert not (tmp_path / 'busy-finished').exists()
            assert not probe.terminations, probe.failure_details()
            assert not any(
                item['kind'] == 'call-result' for item in _busy_source_records(tmp_path)
            )
        finally:
            (tmp_path / 'busy-release').touch()
        probe.wait_for(
            lambda: (
                any(item['kind'] == 'call-result' for item in _busy_source_records(tmp_path))
                or bool(probe.terminations)
            ),
        )
        assert not probe.terminations, probe.failure_details()


def test_source_call_ends_on_a_cooperative_stop(tmp_path: Path) -> None:
    config = _busy_source_config(tmp_path, 'cooperative-stop')
    with running_supervisor(
        config,
        ChildPatches(busy_source_case='cooperative-stop', gate_events=True, last_event_id=1),
    ) as probe:
        _arm_busy_source(probe, tmp_path)
        probe.wait_for((tmp_path / 'source-wait-past-deadline').exists)
        waits = [
            item for item in _busy_source_records(tmp_path) if item['kind'] == 'source-result-wait'
        ]
        assert waits
        assert all(0 < item.get('timeout', 0) <= 1.0 for item in waits)
        worker = next(iter(probe.supervisor._spawns.values())).worker
        code = worker.shutdown('test-cooperative-stop')
        assert code == 0, probe.failure_details()
        logs = '\n'.join(spawn.log_path.read_text() for spawn in probe.spawns)
        assert 'forced' not in logs


def test_source_stale_socket_probes_use_shutdown_slice(tmp_path: Path) -> None:
    config = _wait_process_config(tmp_path, 'socket-probe')
    with running_supervisor(
        config, ChildPatches(wait_case='socket-probe', gate_events=True)
    ) as probe:
        probe.wait_for(lambda: len(_busy_source_records(tmp_path)) >= 2)
        records = _busy_source_records(tmp_path)
        assert [item['kind'] for item in records] == ['source-path', 'server-path']
        assert all(item.get('timeout') == 1.0 for item in records)


def test_stop_during_lane_operator_result_wait_completes_without_forced_exit(
    tmp_path: Path,
) -> None:
    config = _wait_process_config(tmp_path, 'lane-result')
    with running_supervisor(
        config, ChildPatches(wait_case='lane-result', gate_events=True)
    ) as probe:
        probe.wait_for(
            lambda: any(
                group.join_phase == 'attached'
                for health in probe.latest_health.values()
                for group in health.groups
            ),
        )
        worker = next(iter(probe.supervisor._spawns.values())).worker
        worker.send(
            OperatorCommand(
                request_id='lane-wait',
                command='retry',
                arguments={'site': 'test', 'group': 'waiting'},
                identity=AuditIdentity.current(),
            )
        )
        probe.wait_for((tmp_path / 'wait-entered').exists)
        assert worker.shutdown('test-lane-result-stop') == 0, probe.failure_details()
        records = _busy_source_records(tmp_path)
        assert all(item.get('timeout', 1.0) <= 0.1 for item in records)


@pytest.mark.parametrize('case', ['connect', 'connect-auth', 'reconnect', 'reconnect-auth'])
def test_stop_during_remote_connect_completes_without_forced_exit(
    tmp_path: Path, case: str
) -> None:
    config = _wait_process_config(tmp_path, case)
    with running_supervisor(config, ChildPatches(wait_case=case, gate_events=True)) as probe:
        if case.startswith('reconnect'):
            probe.wait_for(
                lambda: any(
                    group.join_phase == 'attached'
                    for health in probe.latest_health.values()
                    for group in health.groups
                ),
            )
            (tmp_path / 'wait-arm').touch()
        probe.wait_for((tmp_path / 'wait-entered').exists)
        worker = next(
            spawn.worker for spawn in probe.supervisor._spawns.values() if spawn.worker.spec.groups
        )
        waits = [item for item in _busy_source_records(tmp_path) if item['kind'] == 'socket-wait']
        assert waits
        assert all(0 < item.get('timeout', 0) <= 1.0 for item in waits)
        assert worker.shutdown('test-wait-stop') == 0, probe.failure_details()
        assert 'forced' not in '\n'.join(spawn.log_path.read_text() for spawn in probe.spawns)


@pytest.mark.parametrize('case', ['connect-cap', 'connect-auth-cap'])
def test_remote_connect_wait_retries_at_its_cap(tmp_path: Path, case: str) -> None:
    config = _wait_process_config(tmp_path, case)
    with running_supervisor(config, ChildPatches(wait_case=case, gate_events=True)) as probe:
        entered, received = _wait_for_cap_heartbeat(probe, tmp_path)
        probe.wait_for(
            lambda: (
                any(
                    group.join_phase == 'attached'
                    for health in probe.latest_health.values()
                    for group in health.groups
                )
                or bool(probe.terminations)
            ),
        )
        assert not probe.terminations, probe.failure_details()
        records = _busy_source_records(tmp_path)
        attempt = next(item for item in records if item['kind'] == 'connect-attempt')
        retry = next(item for item in records if item['kind'] == 'connect-retry')
        assert received < retry['at']
        queued = next(item for item in records if item['kind'] == 'connect-queued')
        assert attempt.get('logical_at', float('nan')) - queued.get(
            'logical_at', float('nan')
        ) == pytest.approx(0.2)
        assert retry.get('logical_at', float('nan')) - attempt.get(
            'logical_at', float('nan')
        ) == pytest.approx(_busy_source_budget(config))
        assert attempt.get('deadline_secs') == _busy_source_budget(config)
        ended = next(item for item in records if item['kind'] == 'cap-exit')
        assert entered['at'] < received < ended['at']
        assert not probe.terminations, probe.failure_details()
        assert any(
            group.join_phase == 'attached'
            for health in probe.latest_health.values()
            for group in health.groups
        )
        waits = [item for item in records if item['kind'] == 'socket-wait']
        assert len(waits) > 2


def test_worker_checkpoints_each_open_file_once_per_interval(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = runtime_site(group_config('default-a', worker='default'))
    site = replace(
        site,
        groups=MappingProxyType(
            {
                'default-a': group_config('default-a', worker='default'),
                'default-b': group_config('default-b', worker='default'),
            }
        ),
    )
    config = runtime_config(tmp_path, site)
    config = replace(config, daemon=replace(config.daemon, checkpoint_interval_secs=0.05))
    spec = build_worker_specs(site)[0]
    assignment = local_assignment(spec, 'default-a', 'initialize')
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
    with running_concrete_runtime(monkeypatch, config, spec, assignment, runtime_provider()) as (
        runtime,
        control,
    ):
        receive_typed_health(
            control,
            lambda update: (
                len(update.groups) == 2
                and len(update.sources) == 1
                and all(not item.conditions for item in (*update.groups, *update.sources))
            ),
        )
        drain_stop, drain_thread = start_control_drain(control)
        try:
            while True:
                with lock:
                    completed = len(attempts) == 1 and all(
                        len(times) >= 2 for times in attempts.values()
                    )
                if completed:
                    break
                assert runtime.fatal_reason is None
                time.sleep(0.001)
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
            stop_control_drain(drain_stop, drain_thread)
    assert not any(
        thread.name.startswith('sg-event-daemon:checkpoint:') for thread in threading.enumerate()
    )


def test_worker_monitor_samples_blocked_source_provider_creation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    site = replace(
        runtime_site(group_config('local', worker='default')),
        groups=MappingProxyType({}),
    )
    config = runtime_config(tmp_path, site)
    spec = WorkerSpec(
        worker_id='["site","main"]',
        site='main',
        kind=_WorkerKind.SITE,
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
    ) -> RuntimeProviderHandle:
        assert site == 'https://example.test'
        assert script_name == 'daemon'
        assert api_key == 'key'
        assert not connect
        entered.set()
        assert release.wait()
        return RuntimeProviderHandle(lambda: 41)

    provider = Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        sync_factory=factory,
        async_factory=None,
    )
    monkeypatch.setattr(worker_module, 'load_provider', provider_loader(provider))
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=131_072)
    child = WorkerControl(child_socket, max_frame_bytes=131_072)
    runtime = build_worker_runtime(config, spec, assignment, child)
    runtime.start()
    try:
        receive_type(parent, SourceEndpointReady)
        assert entered.wait()
        update = receive_typed_health(
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
        release_failed_source_runtime(
            runtime,
            (parent, child),
            (parent_socket, child_socket),
            None,
        )


def wait_test_component(
    tmp_path: Path,
    kind: Literal['source', 'lane'],
    interval: float,
) -> _SourceComponent | _LaneComponent:
    source, cache = _source_health_component(tmp_path, health_interval_secs=interval)
    source._config = replace(
        source._config,
        daemon=replace(
            source._config.daemon,
            supervisor_poll_interval_secs=interval,
        ),
    )
    if kind == 'source':
        return source
    return _LaneComponent(
        checkpoint_host=source._checkpoint_host,
        assignment=source._assignment,
        config=source._config,
        control=source._control,
        group='local',
        health_cache=cache,
        limiter=_CatchupLimiter(1),
        local_source=source,
        spec=source._spec,
    )


def test_composed_source_service_preserves_the_retry_wait_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [10.0]
    component = wait_test_component(tmp_path, 'source', 2.0)
    assert isinstance(component, _SourceComponent)
    monkeypatch.setattr(time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(component._watchdog, '_clock', lambda: now[0])
    component._watchdog.transition('wait', deadline_at=25.0)
    operations: list[str] = []

    def service(_server: EventServer, _surface: _SourceJoinSurface, _poller: SourcePoller) -> None:
        operations.append('service')
        # Cross the entire enclosing wait budget while nested service owns the thread.
        now[0] = 26.0

    monkeypatch.setattr(component, '_service_internal', service)
    waiter = worker_module._ComponentWaiter(threading.Event())
    waiter.configure_service(
        lambda: component._service(
            cast('EventServer', object()),
            cast('_SourceJoinSurface', object()),
            cast('SourcePoller', object()),
        )
    )
    assert not waiter.wait(5.0)
    assert operations == ['service']
    sample = component._watchdog.sample()
    assert sample.phase == 'wait'
    assert sample.heartbeat_at == 26.0
    assert sample.deadline_at == 41.0


@pytest.mark.parametrize('kind', ['source', 'lane'])
@pytest.mark.usefixtures('_production_phase_deadlines')
def test_late_wait_return_stays_inside_its_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: Literal['source', 'lane'],
) -> None:
    now = [10.0]
    component = wait_test_component(tmp_path, kind, 0.01)
    monkeypatch.setattr(time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(component._watchdog, '_clock', lambda: now[0])
    samples: list[WatchdogSample] = []

    def late_wait(timeout: float | None = None) -> bool:
        assert timeout is not None
        now[0] += timeout + 0.08
        samples.append(component._watchdog.sample())
        component._stop.set()
        return True

    def source_wait(_self: worker_module._ComponentWaiter, timeout: float) -> bool:
        return late_wait(timeout)

    if isinstance(component, _LaneComponent):
        monkeypatch.setattr(component._wake, 'wait', late_wait)
        component._poll_lane(cast('worker_module._LocalJoinClient', object()))
    else:
        monkeypatch.setattr(worker_module, 'load_provider', provider_loader(runtime_provider()))
        monkeypatch.setattr(worker_module._ComponentWaiter, 'wait', source_wait)
        component.run()
    assert len(samples) == 1
    sample = samples[0]
    assert sample.phase == ('idle' if kind == 'lane' else 'wait')
    assert sample.deadline_at is not None
    assert now[0] < sample.deadline_at
    assert not Supervisor._phase_confirmed_overdue(now[0], sample.deadline_at)


class _AdoptionServiceProbe:
    def __init__(self, *, service_failure: bool) -> None:
        self.service_failure = service_failure
        self.entered, self.release, self._serviced, self._queued = (
            threading.Event() for _ in range(4)
        )
        self._caller_done = threading.Event()
        self.service_error = OSError('adoption socket service failure')
        self.provider_error = RuntimeError('adoption provider failure')
        self.operations: list[str] = []
        self.owner_errors: list[BaseException] = []
        self.caller_errors: list[BaseException] = []
        self._component = object.__new__(_SourceComponent)
        vars(self._component).update(
            _mailbox=_OwnerMailbox('source', self._queued),
            _stop=threading.Event(),
            _health_interval_secs=0.01,
            _config=SimpleNamespace(daemon=SimpleNamespace(shutdown_deadline_secs=1)),
            _grant_lock=threading.Lock(),
            _grants=deque(),
            _poller=None,
            _publish_pending_groups=self._ignore,
            _publish_health=self._ignore,
        )
        self.runtime = _checkpoint_shutdown_runtime([])
        self.runtime._components['source:events'] = self._component
        self.runtime._construction_order.append('source:events')
        self.runtime._operator_command = _busy_source_operator()
        self._state = cast(
            'SourceState',
            SimpleNamespace(
                capacity_paused=False,
                successful_clock=lambda: 0,
                adopt_stream=self._adopt,
                initialize=self._initialize,
                operator_writes=nullcontext,
            ),
        )
        self._surface = cast(
            '_SourceJoinSurface',
            SimpleNamespace(
                reconcile_groups=lambda: None,
                require_stamped=lambda: None,
                state=self._state,
                _inventory={},
            ),
        )

    def _adopt(self, **_kwargs: object) -> None:
        self.operations.append('adopt')

    def _handle(self, *_args: object, **_kwargs: object) -> _AdoptionBlockedHandle:
        return _AdoptionBlockedHandle(self)

    @staticmethod
    def _ignore(*_args: object, **_kwargs: object) -> None:
        pass

    def _initialize(self, _latest: int | None) -> None:
        self.operations.append('initialize')

    @staticmethod
    def _invoke(active: _SourceJoinSurface) -> object:
        return worker_module._SourceJoinSurface.adopt_stream(
            active,
            adoption_id='adoption',
            command_fingerprint='command',
            new_stream_fingerprint='stream',
            reason='test',
            groups_parked=True,
        )

    def _caller(self) -> None:
        try:
            self._component.call(self._invoke, stop=threading.Event(), wait_phase=nullcontext)
        except (RuntimeError, OSError) as error:
            self.caller_errors.append(error)
        finally:
            self._caller_done.set()

    def _service(self) -> None:
        self._serviced.set()
        if self.service_failure:
            raise self.service_error

    def _run(self) -> None:
        provider = Provider(
            name='shotgun-api3',
            capabilities=ProviderCapabilities(sync=True, async_=False),
            sync_factory=self._handle,
            async_factory=None,
        )
        try:
            with SourcePoller(
                state=self._state,
                fanout=EventFanout(),
                provider=provider,
                site_url='https://example.invalid',
                credential=runtime_site(group_config('local', worker='default')).credentials[
                    'source'
                ],
                config=SourceConfig(name='events', credentials='source'),
                max_ipc_frame_bytes=131_072,
                source_generation='source-1',
                waiter=SourceHealthWaiter(),
                random_value=lambda: 0.5,
                watchdog=ComponentWatchdog(
                    kind='source',
                    identity='events',
                    worker_instance='source-1',
                    registration_token=_watchdog_token(),
                    startup_deadline_secs=10,
                ),
                internal_deadline_secs=10,
                wait_tolerance_secs=1,
                query_service=self._service,
            ) as poller:
                self._component._poller = poller
                vars(self._surface)['_poller'] = poller
                assert self._queued.wait()
                server = cast(
                    'EventServer',
                    SimpleNamespace(poll=self._ignore, dispatch_join_requests=self._ignore),
                )
                self._component._service_internal(server, self._surface, poller)
                if not self.service_failure:
                    assert self._caller_done.wait()
                    self.runtime.request_stop('test finished')
        except BaseException as error:
            self.owner_errors.append(error)
            raise

    def exercise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(self._component, 'run', self._run)
        owner = threading.Thread(
            target=self.runtime._component_main, args=('source:events', self._component)
        )
        waiting = threading.Thread(target=self._caller)
        owner.start()
        waiting.start()
        try:
            assert self.entered.wait(), (self.owner_errors, self.caller_errors, self.operations)
            assert self._serviced.wait(), 'adoption did not service sockets while blocked'
            assert not self._caller_done.is_set()
            assert owner.is_alive()
            assert 'close' not in self.operations
        finally:
            self.release.set()
            owner.join()
            self._component._stop.set()
            waiting.join()
        assert not owner.is_alive()
        assert not waiting.is_alive()


@pytest.mark.parametrize('service_failure', [True, False])
def test_adoption_query_service_failure_fails_the_source_owner(
    monkeypatch: pytest.MonkeyPatch,
    service_failure: bool,
) -> None:
    probe = _AdoptionServiceProbe(service_failure=service_failure)
    probe.exercise(monkeypatch)
    assert probe.operations == ['adopt', 'find', 'return', 'close']
    if service_failure:
        assert probe.owner_errors == [probe.service_error]
        assert isinstance(probe.caller_errors[0], worker_module._CooperativeComponentStopError)
        assert probe.runtime._operator_result is not None
        assert probe.runtime._operator_result.result == {'outcome': 'unknown'}
        assert probe.runtime.fatal_reason is not None
        assert 'adoption socket service failure' in probe.runtime.fatal_reason
    else:
        assert probe.owner_errors == []
        assert probe.caller_errors == [probe.provider_error]
        assert probe.runtime.fatal_reason is None


@pytest.mark.parametrize('attempt_kind', ['connect-attempt', 'request-attempt'])
@pytest.mark.parametrize('finished', [False, True])
def test_cap_heartbeat_rejects_finished_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    attempt_kind: str,
    finished: bool,
) -> None:
    records: list[_BusySourceRecord] = [
        {'kind': attempt_kind, 'at': 10.2},
        {'kind': 'cap-enter', 'at': 10.201, 'spawn_instance': 'synthetic-spawn'},
    ]
    if finished:
        records.append(
            {
                'kind': 'connect-retry' if attempt_kind == 'connect-attempt' else 'remote-detach',
                'at': 11.0,
                'reason': 'coordination-reply-timeout',
            }
        )
    (tmp_path / 'busy-source.jsonl').write_text(
        ''.join(json.dumps(record) + '\n' for record in records)
    )
    probe = object.__new__(SupervisorProbe)
    probe.terminations = []
    probe.cap_heartbeats = [('synthetic-spawn', 10.5, 50.0)]

    def wait_for(predicate: Callable[[], bool]) -> None:
        assert predicate(), 'synthetic progress did not reach its boundary'

    monkeypatch.setattr(probe, 'wait_for', wait_for)
    monkeypatch.setattr(probe, 'failure_details', lambda: 'synthetic cap evidence')
    if finished:
        with pytest.raises(AssertionError, match='cap finished'):
            _wait_for_cap_heartbeat(probe, tmp_path)
        assert not (tmp_path / 'cap-heartbeat.jsonl').exists()
    else:
        entered, received = _wait_for_cap_heartbeat(probe, tmp_path)
        assert entered['at'] == 10.201
        assert received == 50.0
        assert json.loads((tmp_path / 'cap-heartbeat.jsonl').read_text())['received_at'] == 50.0


@pytest.mark.parametrize('prefix_count', [0, 2])
def test_strict_counted_catchup_progress_after_expired_setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefix_count: int
) -> None:
    with _empty_slice_catchup(
        tmp_path, monkeypatch, prefix_count, guard=(None, False), ordering='strict'
    ) as (
        component,
        resources,
        state,
        _lane,
        invoked,
        _trace,
    ):
        outcomes: list[StrictDeliveryOutcome] = []

        def strict_step(**options: object) -> object:
            deliver = cast(
                'Callable[[Sequence[Event], float], StrictDeliveryOutcome]', options['deliver']
            )
            remaining = tuple(
                Event(runtime_event(event_id))
                for event_id in range(state.snapshot().cursor + 1, 48)
            )
            outcome = deliver(remaining, 0.0)
            outcomes.append(outcome)
            assert outcome.accepted
            assert outcome.continue_epoch
            assert outcome.consumed > 0
            assert state.snapshot().in_flight_event_id is None
            assert state.snapshot().cursor == remaining[outcome.consumed - 1]['id']
            return SimpleNamespace(join_ready=state.snapshot().cursor == 47)

        vars(resources)['reader'] = SimpleNamespace(strict_step=strict_step)
        component._catch_up_strict(
            resources, cast('JoinTarget', SimpleNamespace(identity=SimpleNamespace(boundary=47)))
        )
        assert len(outcomes) <= 6
        assert sum(outcome.consumed for outcome in outcomes) == 6
        assert invoked == list(range(42, 48))
        assert state.snapshot().cursor == 47


@pytest.mark.parametrize('transport', ['local', 'remote'])
def test_stored_strict_capacity_flag_resumes_ordered_catchup_and_rejoins(
    transport: Literal['local', 'remote'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _detached_strict_join(tmp_path, monkeypatch, modes=(transport, 'sync'), case='stored-capacity')
