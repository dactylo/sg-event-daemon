from __future__ import annotations

import errno
import json
import multiprocessing
import os
import select
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections import deque
from contextlib import ExitStack, closing, contextmanager
from dataclasses import replace
from pathlib import Path
from queue import Queue
from types import MappingProxyType, SimpleNamespace
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack, cast
from unittest.mock import Mock

import pytest

import sg_event_daemon._control as control_module
import sg_event_daemon._ipc as ipc_module
import sg_event_daemon._providers as providers_module
import sg_event_daemon._supervisor as supervisor_module
import sg_event_daemon._worker as worker_module
import sg_event_daemon._write_lock as write_lock_module
import tests.test_worker as worker_tests
from sg_event_daemon._checkpoint import Checkpointer
from sg_event_daemon._config import load_config
from sg_event_daemon._control import (
    _MAX_MESSAGE_BYTES,
    _PROTOCOL_VERSION,
    ControlClient,
    ControlServer,
    encode_json_body,
)
from sg_event_daemon._group_errors import InvalidTransitionError, RevisionConflictError
from sg_event_daemon._group_recovery import DispositionRequest, LifecycleRequest
from sg_event_daemon._group_state import GroupOpenOptions, GroupState
from sg_event_daemon._group_types import (
    AuditIdentity,
    ConditionalParkDirective,
    GroupSnapshot,
)
from sg_event_daemon._health import (
    BreakerTransition,
    GroupHealth,
    StatusSnapshot,
    WatchdogHealth,
    WorkerHealth,
    aggregate_status,
)
from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._ipc import (
    ActiveSourceGroupAssignment,
    CommandResult,
    DeliveryGroupAssignment,
    EventPeerGrant,
    EventPeerReady,
    GroupRecoveryReady,
    HealthUpdate,
    OperatorCommand,
    PendingDispositionSourceGroupAssignment,
    SourceEndpointReady,
    StopCommand,
    WorkerAssignment,
    WorkerControl,
)
from sg_event_daemon._platform import (
    AlreadyLockedError,
    ChildProcess,
    acquire_lock,
    ensure_secure_state_dir,
    reap_child,
    secure_open_regular_file,
    spawn_child,
    terminate_child,
)
from sg_event_daemon._plugins import CallbackIdentity, CallbackManifest
from sg_event_daemon._probe import ProbeResult, ProbeServer, ProbeSnapshot
from sg_event_daemon._source import SourceHealth
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._sqlite import (
    DatabaseError,
    transaction,
)
from sg_event_daemon._supervisor import (
    Supervisor,
    _allocate_generation,
    _ParkEvidence,
    _SupervisorState,
    _UnreapedWorkerSpawnError,
    _WorkerBreaker,
    _WorkerInstance,
)
from sg_event_daemon._watchdog import (
    ComponentWatchdog,
    WatchdogKind,
    WatchdogPhase,
    internal_stall_deadline_secs,
)
from sg_event_daemon._worker import WorkerSpec, build_worker_specs
from sg_event_daemon._write_lock import write_lock_timeout_secs
from tests._ipc_support import (
    partial_startup_child_code as _partial_startup_child_code,
)
from tests._ipc_support import (
    saturate_worker_control_sender as _saturate_worker_control_sender,
)
from tests._storage_support import (
    group_exists,
    group_lock,
    open_group_on_source,
    source_database,
    write_context_for,
)
from tests.source.test_state import open_source_range

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator, Mapping
    from multiprocessing.connection import Connection
    from multiprocessing.process import BaseProcess

    from sg_event_daemon._config import Config
    from sg_event_daemon._ipc import ControlMessage
    from sg_event_daemon._sqlite import DatabaseHandle
    from sg_event_daemon._supervisor import _WorkerHandle


class _ControlChildSpawner:
    def __init__(self, code: str) -> None:
        self._code = code
        self._processes: list[ChildProcess] = []

    def __call__(
        self,
        argv: list[str],
        *,
        pass_fds: tuple[int, ...] = (),
        env: Mapping[str, str] | None = None,
    ) -> ChildProcess:
        control_fd = pass_fds[0]
        spawn_instance = argv[argv.index('--spawn-instance') + 1]
        worker_id = argv[argv.index('--worker') + 1]
        process = spawn_child(
            [
                argv[0],
                '-c',
                self._code,
                str(control_fd),
                spawn_instance,
                worker_id,
            ],
            pass_fds=pass_fds,
            env=env,
        )
        self._processes.append(process)
        return process


class _FailedSpawnSocket:
    def __init__(self, name: str, attempts: list[str]) -> None:
        self.name = name
        self.attempts = attempts

    def close(self) -> None:
        self.attempts.append(self.name)
        if self.name != 'child' or self.attempts.count('child') > 1:
            raise OSError(self.name + ' cleanup failed')

    def fileno(self) -> int:
        return 42


class _FakeClock:
    def __init__(self) -> None:
        self._now = 0.0

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


class _HealthPublications:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        pending: Queue[tuple[bytes, float]] = Queue()
        self._received: dict[int, tuple[HealthUpdate, float]] = {}
        original_send = WorkerControl.send
        original_receive = WorkerControl.receive

        def send(control: WorkerControl, message: ControlMessage) -> None:
            if isinstance(message, HealthUpdate):
                published_at = time.monotonic()
                # The sole runtime monitor has assembled the entire message.
                # Record before send can make its unchanged bytes observable.
                pending.put((ipc_module.encode_control_message(message), published_at))
            original_send(control, message)

        def receive(control: WorkerControl, *, timeout: float) -> ControlMessage:
            message = original_receive(control, timeout=timeout)
            if isinstance(message, HealthUpdate):
                frame, published_at = pending.get_nowait()
                assert ipc_module.encode_control_message(message) == frame
                self._received[id(message)] = (message, published_at)
            return message

        monkeypatch.setattr(WorkerControl, 'send', send)
        monkeypatch.setattr(WorkerControl, 'receive', receive)

    def _published_at(self, update: HealthUpdate) -> float:
        received, published_at = self._received[id(update)]
        assert received is update
        return published_at


class _LaneRecoveryControl:
    def __init__(self, *, drop_ready: bool) -> None:
        self._drop_ready = drop_ready
        self._messages: list[GroupRecoveryReady] = []

    def send(self, message: ControlMessage) -> None:
        if not isinstance(message, GroupRecoveryReady):
            raise TypeError(type(message).__name__)
        self._messages.append(message)
        if self._drop_ready:
            raise EOFError('simulated lost group recovery ready')


class _ObservedForegroundWait:
    def __init__(self, *, fails: bool = False) -> None:
        self._bindings: list[Callable[[], None] | None] = []
        self._calls: list[float] = []
        self.fails = fails

    def serve_once(self, timeout: float) -> None:
        callback = self._bindings[-1]
        assert callback is not None
        callback()
        self._calls.append(timeout)
        if self.fails:
            raise RuntimeError('wait failed')

    def set_foreground_progress(self, callback: Callable[[], None] | None) -> None:
        self._bindings.append(callback)


class _ParkEvidenceOptions(TypedDict, total=False):
    evidence_id: str
    site: str
    group: str
    group_generation: int
    failed_worker_id: str
    failed_spawn_instance: str
    event_id: int
    group_revision: int
    observed_at_wall_clock: float


class _ScriptedWorker:
    def __init__(
        self,
        spec: WorkerSpec,
        spawn_instance: str,
        operations: list[str],
        assignment: WorkerAssignment | None,
        *,
        authentication_ready: bool,
    ) -> None:
        self._authentication_calls = 0
        self._batch_acquisitions = 0
        self._authentication_ready = authentication_ready
        self._assignment: WorkerAssignment | None = None
        self._auto_endpoint_ready = True
        self._auto_grant_ready = True
        self._close_calls = 0
        self._exit_status: int | None = None
        self._killed = False
        self._on_kill: Callable[[], None] | None = None
        self._operator_handler: Callable[[OperatorCommand], CommandResult] | None = None
        self._operations = operations
        self._receive_timeouts: list[float | None] = []
        self._messages_before_grant: deque[ControlMessage] = deque()
        self._sent_commands: list[OperatorCommand] = []
        self._sent_grants: list[EventPeerGrant] = []
        self._shutdown_status: int | None = 0
        self._spawn_instance = spawn_instance
        self._spec = spec
        self._wait_results: deque[int | None] = deque()
        self._messages: deque[ControlMessage] = deque()
        if assignment is not None:
            self._assign(assignment)

    def _assign(self, assignment: WorkerAssignment) -> None:
        if self._assignment is not None:
            raise ValueError('duplicate assignment')
        self._assignment = assignment
        self._operations.append(f'assign:{self._spec.kind.value}')
        if self._auto_endpoint_ready:
            for source, generation in assignment.source_generations:
                self._messages.append(
                    SourceEndpointReady(
                        source=source,
                        source_generation=generation,
                        path=os.fspath(Path.cwd() / f'{source}.sock'),
                    )
                )

    def _authenticate(self, *, timeout: float = 0.0) -> bool:
        if timeout != 0.0:
            raise AssertionError('monitor tick waited while authenticating a worker')
        self._authentication_calls += 1
        return self._authentication_ready

    def _close(self) -> None:
        self._close_calls += 1
        self._operations.append(f'close:{self._spec.kind.value}')

    def _kill(self) -> None:
        if self._on_kill is not None:
            self._on_kill()
        self._killed = True
        self._exit_status = -9
        self._operations.append(f'kill:{self._spec.kind.value}')

    def _receive(self, *, timeout: float | None = None) -> ControlMessage:
        self._receive_timeouts.append(timeout)
        if not self._messages:
            raise TimeoutError('scripted worker has no message')
        message = self._messages.popleft()
        if isinstance(message, SourceEndpointReady):
            self._operations.append(f'receive:endpoint:{message.source}')
        elif isinstance(message, EventPeerReady):
            self._operations.append(f'receive:grant-ready:{message.group}')
        return message

    def _receive_available(self) -> Iterator[ControlMessage]:
        self._batch_acquisitions += 1
        count = len(self._messages)
        return (self._receive(timeout=0.0) for _ in range(count))

    def _send(self, message: ControlMessage) -> None:
        if isinstance(message, OperatorCommand):
            self._sent_commands.append(message)
            self._operations.append(f'operator:{message.command}')
            if self._operator_handler is not None:
                self._messages.append(self._operator_handler(message))
            return
        if not isinstance(message, EventPeerGrant):
            raise TypeError(type(message).__name__)
        self._sent_grants.append(message)
        self._operations.append(f'grant:{message.group}')
        self._messages.extend(self._messages_before_grant)
        self._messages_before_grant.clear()
        if self._auto_grant_ready:
            self._messages.append(
                EventPeerReady(
                    source=message.source,
                    group=message.group,
                    dependent_spawn_instance=message.dependent_spawn_instance,
                )
            )

    def _shutdown(self, reason: str) -> int | None:
        self._operations.append(f'shutdown:{self._spec.kind.value}:{reason}')
        self._exit_status = self._shutdown_status
        return self._shutdown_status

    def _wait(self, *, timeout: float = 0.0) -> int | None:
        if timeout != 0.0:
            raise AssertionError('monitor tick waited while reaping a worker')
        if self._wait_results:
            return self._wait_results.popleft()
        return self._exit_status

    @property
    def _worker_id(self) -> str:
        return self._spec.worker_id


class _ScriptedWorkerOptions(TypedDict):
    config_path: Path
    spec: WorkerSpec
    spawn_instance: str
    max_frame_bytes: int
    send_deadline_secs: float
    startup_deadline_secs: float
    shutdown_deadline_secs: float
    foreground_progress: NotRequired[Callable[[], None] | None]
    wait_slice_secs: NotRequired[float]
    env: NotRequired[Mapping[str, str] | None]


class _ScriptedWorkerFactory:
    def __init__(self, *, authentication_ready: bool = True) -> None:
        self._authentication_ready = authentication_ready
        self._history: dict[str, list[_ScriptedWorker]] = {}
        self._instances: dict[str, _ScriptedWorker] = {}
        self._operations: list[str] = []
        self._send_deadlines: list[float] = []

    def __call__(self, **options: Unpack[_ScriptedWorkerOptions]) -> _ScriptedWorker:
        spec = options['spec']
        spawn_instance = options['spawn_instance']
        self._send_deadlines.append(options['send_deadline_secs'])
        self._operations.append(f'spawn:{spec.kind.value}')
        instance = _ScriptedWorker(
            spec,
            spawn_instance,
            self._operations,
            None,
            authentication_ready=self._authentication_ready,
        )
        self._history.setdefault(spec.worker_id, []).append(instance)
        self._instances[spec.worker_id] = instance
        return instance


def _adopt_lifecycle_stream(
    source: SourceState,
    *,
    adoption_id: str,
    command_fingerprint: str,
    reason: str,
) -> None:
    database = vars(source)['_database']
    database.connection.execute(
        """
        UPDATE source_meta
        SET condition = 'stream-identity-changed'
        WHERE singleton = 1
        """
    )
    vars(source)['_token_factory'] = lambda: adoption_id
    reservation = source.reserve_stream_adoption(
        command_fingerprint=command_fingerprint,
        reason=reason,
    )
    assert reservation.adoption_id == adoption_id
    source.adopt_stream(
        adoption_id=reservation.adoption_id,
        command_fingerprint=reservation.command_fingerprint,
        new_stream_fingerprint=source.snapshot().stream_fingerprint,
        reason=reason,
        groups_parked=True,
    )


def _assert_pending_provider_publication(
    supervisor: Supervisor,
    worker: _ScriptedWorker,
    update: HealthUpdate,
    *,
    published_at: float,
) -> None:
    original_clock = supervisor._clock
    supervisor._clock = lambda: published_at
    try:
        contained = supervisor._accept_worker_health(worker, update)
        evidence = (
            published_at,
            update.worker.heartbeat_at,
            supervisor._spawns,
        )
        assert not contained, evidence
        assert not supervisor._monitor_worker_health(published_at), evidence
        assert all(spawn._termination_reason is None for spawn in supervisor._spawns.values())
        assert supervisor._breakers[worker._worker_id]._failure_total == 0
        assert not worker._killed
        assert supervisor.status_mapping({})['groups']
    finally:
        supervisor._clock = original_clock


def _begin_readd_disposition(
    source: SourceState,
    group: GroupState,
    command: Literal['replay', 'reset-to-latest'],
) -> GroupSnapshot:
    if command == 'replay' and group.snapshot().status != 'orphaned':
        frontier = source.snapshot().frontier
        open_source_range(source, frontier + 1, frontier + 1, successful_clock=1.0)
    marker = Supervisor._orphan_group(source, group)
    return group.dispose(
        DispositionRequest(
            command=command,
            group=group.snapshot().group,
            disposition_id=f'{command}-readd',
            reserved_generation=2,
            expected_revision=group.snapshot().revision,
            from_event_id=marker.from_event_id if command == 'replay' else None,
            acknowledge_truncation=False,
            reason='configured group re-added',
            identity=AuditIdentity.current(),
            wall_clock_time=10.0,
        )
    )


def _breaker_audit_rows(state: _SupervisorState, worker_id: str) -> list[tuple[int]]:
    rows = state._database.connection.execute(
        'SELECT 1 FROM breaker_audit WHERE worker_id = ?',
        (worker_id,),
    ).fetchall()
    return [(int(row[0]),) for row in rows]


def _child_environment() -> dict[str, str]:
    environment = dict(os.environ)
    environment['PYTHONPATH'] = os.fspath(Path(__file__).parents[1] / 'src')
    environment['SG_EVENT_ADMIN_KEY'] = 'test-key'
    return environment


def _child_probe_address(server: ProbeServer) -> tuple[str, int]:
    listener = cast('socket.socket', vars(server)['_listener'])
    host, port = listener.getsockname()[:2]
    return str(host), int(port)


def _child_probe_response(address: tuple[str, int], *, timeout: float) -> bytes:
    with socket.create_connection(address, timeout=timeout) as client:
        client.settimeout(timeout)
        client.sendall(b'GET /livez HTTP/1.1\r\n\r\n')
        chunks: list[bytes] = []
        while chunk := client.recv(4_096):
            chunks.append(chunk)
    return b''.join(chunks)


def _complete_readd_disposition(source: SourceState, group: GroupState) -> None:
    source.initialize(100, incarnation='incarnation-1')
    _begin_readd_disposition(source, group, 'reset-to-latest')


def _force_reap_test_process(process: BaseProcess) -> None:
    process.terminate()
    process.join(2.0)
    if process.is_alive():
        process.kill()
        process.join(2.0)
    assert not process.is_alive()
    assert process.exitcode is not None


def _idle_source_health(
    worker: _ScriptedWorker,
    *,
    source: str,
    heartbeat_at: float,
) -> SourceHealth:
    assignment = worker._assignment
    generations = {} if assignment is None else dict(assignment.source_generations)
    return SourceHealth(
        site=worker._spec.site,
        source=source,
        source_generation=generations.get(source, 'source-generation-1'),
        status='running',
        frontier=0,
        successful_clock=heartbeat_at,
        retry_attempt=0,
        next_retry_secs=None,
        open_range_count=0,
        late_id_count=0,
        last_error=None,
        worker_instance=worker._spawn_instance,
        initialized=True,
        phase='wait',
        heartbeat_at=heartbeat_at,
        deadline_at=heartbeat_at + 1.25,
    )


def _in_flight_group_health(
    worker: _ScriptedWorker,
    *,
    heartbeat_at: float,
    group: str = 'remote',
    phase: Literal['callback', 'idle', 'internal', 'write-lock-wait', 'write-hold'] = 'callback',
) -> GroupHealth:
    return GroupHealth(
        site=worker._spec.site,
        source='admin',
        group=group,
        worker_instance=worker._spawn_instance,
        execution='sync',
        ordering='eager',
        phase=phase,
        status='running',
        cursor=41,
        current_event_id=42 if phase == 'callback' else None,
        current_plugin_id='noop' if phase == 'callback' else None,
        current_callback_id='handle' if phase == 'callback' else None,
        delivery_attempt=1 if phase == 'callback' else None,
        heartbeat_at=heartbeat_at,
        heartbeat_age=0.0,
        queue_event_count=0,
        queue_byte_count=0,
        catchup_frontier=None,
        pending_catchup_epoch_count=0,
        oldest_catchup_epoch_age=None,
        join_phase='attached',
        outstanding_late_obligation_count=0,
        outstanding_late_id_count=0,
        outstanding_late_ids=(),
        outstanding_late_ids_truncated=False,
        parked=False,
        park_reason=None,
        failure_kind=None,
        failure_text=None,
        checkpoint_state='healthy',
        conditions=(),
        deadline_at=heartbeat_at + (1.25 if phase == 'idle' else 10_000.0),
    )


def _health_update(
    worker: _ScriptedWorker,
    *,
    heartbeat_at: float,
    groups: tuple[GroupHealth, ...] | None = None,
    sources: tuple[SourceHealth, ...] | None = None,
    watchdogs: tuple[WatchdogHealth, ...] | None = None,
) -> HealthUpdate:
    current_sources = (
        tuple(
            _idle_source_health(worker, source=source, heartbeat_at=heartbeat_at)
            for source in worker._spec.sources
        )
        if sources is None
        else sources
    )
    current_groups = (
        tuple(
            _in_flight_group_health(worker, group=group, heartbeat_at=heartbeat_at, phase='idle')
            for group in worker._spec.groups
        )
        if groups is None
        else groups
    )
    current_watchdogs = (
        (
            *(
                WatchdogHealth(
                    kind='source',
                    site=source.site,
                    source=source.source,
                    group=None,
                    worker_instance=worker._spawn_instance,
                    phase=source.phase,
                    heartbeat_at=source.heartbeat_at,
                    deadline_at=source.deadline_at,
                )
                for source in current_sources
            ),
            *(
                WatchdogHealth(
                    kind='lane',
                    site=group.site,
                    source=group.source,
                    group=group.group,
                    worker_instance=worker._spawn_instance,
                    phase=group.phase,
                    heartbeat_at=group.heartbeat_at,
                    deadline_at=group.deadline_at,
                    event_id=group.current_event_id,
                    plugin_id=group.current_plugin_id,
                    callback_id=group.current_callback_id,
                    delivery_attempt=group.delivery_attempt,
                )
                for group in current_groups
            ),
            WatchdogHealth(
                kind='operator',
                site=worker._spec.site,
                source=None,
                group=None,
                worker_instance=worker._spawn_instance,
                phase='idle',
                heartbeat_at=heartbeat_at,
                deadline_at=None,
            ),
        )
        if watchdogs is None
        else watchdogs
    )
    return HealthUpdate(
        worker=WorkerHealth(
            site=worker._spec.site,
            worker_id=worker._worker_id,
            spawn_instance=worker._spawn_instance,
            heartbeat_at=heartbeat_at,
            monitor_alive=True,
            thread_names=(),
            stuck_threads=(),
            breaker_open=False,
            breaker_reason=None,
            socket_buffer_requested=1024,
            socket_send_buffer=2048,
            socket_receive_buffer=2048,
        ),
        watchdogs=current_watchdogs,
        sources=current_sources,
        groups=current_groups,
        event_sockets=(),
    )


def _inject_orphan_disposition_failure(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
) -> None:
    failed = False
    dispose = GroupState.dispose
    complete = _SupervisorState._complete_orphan_disposition

    def fail_once(stage: str) -> None:
        nonlocal failed
        if failure_stage == stage and not failed:
            failed = True
            raise TimeoutError(f'injected {stage} failure')

    def dispose_with_failure(group: GroupState, request: DispositionRequest) -> GroupSnapshot:
        fail_once('reservation')
        result = dispose(group, request)
        fail_once('source-committed')
        return result

    def complete_with_failure(
        state: _SupervisorState,
        *,
        site: str,
        group: str,
        orphan_generation: int,
        reserved_generation: int,
    ) -> None:
        complete(
            state,
            site=site,
            group=group,
            orphan_generation=orphan_generation,
            reserved_generation=reserved_generation,
        )
        fail_once('inventory-completed')

    monkeypatch.setattr(GroupState, 'dispose', dispose_with_failure)
    monkeypatch.setattr(_SupervisorState, '_complete_orphan_disposition', complete_with_failure)


def _install_source_grant_control_failure(
    monkeypatch: pytest.MonkeyPatch,
    source: _ScriptedWorker,
    *,
    grant_operation: Literal['send', 'receive'],
    error_factory: Callable[[], BaseException],
) -> list[ControlMessage | None]:
    failure_calls: list[ControlMessage | None] = []
    if grant_operation == 'send':

        def fail_send(message: ControlMessage) -> None:
            assert isinstance(message, EventPeerGrant)
            failure_calls.append(message)
            raise error_factory()

        def reject_post_failure_receive(*, timeout: float | None = None) -> ControlMessage:
            assert timeout == 0.0
            pytest.fail('terminated source control was reused by ordinary IPC ingestion')

        monkeypatch.setattr(source, '_send', fail_send)
        monkeypatch.setattr(source, '_receive', reject_post_failure_receive)
    else:

        def fail_receive(*, timeout: float | None = None) -> ControlMessage:
            assert timeout == 0.0
            failure_calls.append(None)
            raise error_factory()

        monkeypatch.setattr(source, '_receive', fail_receive)
    return failure_calls


def _lane_recovery_probe(
    config: Config,
    assignment: WorkerAssignment,
    control: _LaneRecoveryControl,
) -> tuple[worker_module._LaneComponent, worker_module._LaneConfiguration]:
    delivery = next(
        item for item in assignment.group_assignments if item.conditional_park is not None
    )
    site = config.sites['test']
    group = site.groups[delivery.group]
    source = site.sources[group.source]
    context_factory = worker_module._LaneConfiguration
    context = context_factory(
        _assignment=delivery,
        _credential=site.credentials[source.credentials],
        _group=group,
        _holdback_secs=0.0,
        _site=site,
        _source_config=source,
    )
    component = object.__new__(worker_module._LaneComponent)
    vars(component).update(
        {
            '_assignment': assignment,
            '_control': control,
            '_group': delivery.group,
            '_resolved_recovery': set(),
        }
    )
    return component, context


def _latest_available_health(control: WorkerControl, initial: HealthUpdate) -> HealthUpdate:
    latest = initial
    for _ in range(64):
        try:
            message = control.receive(timeout=0.0)
        except TimeoutError:
            return latest
        assert isinstance(message, HealthUpdate), message
        latest = message
    raise AssertionError('health queue did not drain within 64 available frames')


def _lifecycle_group(
    path: Path,
    *,
    generation: int = 1,
    group: str = 'remote',
    verified_incarnation: str = 'incarnation-1',
) -> GroupState:
    options: GroupOpenOptions = {
        'writes': write_context_for(path),
        'site': 'test',
        'site_server_url': 'https://example.invalid',
        'source': 'admin',
        'group': group,
        'group_generation': generation,
        'ordering': 'eager',
        'stream_fingerprint': stream_fingerprint(
            'https://example.invalid',
            'admin',
            'test',
        ),
        'source_generation': 'source-generation-1',
        'verified_incarnation': verified_incarnation,
        'start_mode': 'resume' if group_exists(path, group) else 'initialize',
    }
    if options['start_mode'] == 'initialize':
        options['initial_cursor'] = 100
        options['initial_delivery_floor'] = 100
        options['initial_last_delivered_id'] = 100
    return open_group_on_source(path, **options)


def _execute_source_group_command(path: Path, command: OperatorCommand) -> CommandResult:
    with closing(sqlite3.connect(path)) as connection, connection:
        generation = int(connection.execute('SELECT group_generation FROM groups').fetchone()[0])
    with _lifecycle_group(path, generation=generation) as group:
        result = group.dispose(
            DispositionRequest(
                command=cast("Literal['replay', 'reset-to-latest']", command.arguments['command']),
                group='remote',
                disposition_id=cast('str', command.arguments['disposition_id']),
                reserved_generation=cast('int', command.arguments['reserved_generation']),
                expected_revision=cast('int', command.arguments['expected_revision']),
                from_event_id=cast('int | None', command.arguments.get('from_event_id')),
                acknowledge_truncation=command.arguments.get('acknowledge_truncation', False)
                is True,
                reason=cast('str', command.arguments['reason']),
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
                operator_label=cast('str | None', command.arguments.get('operator_label')),
            )
        )
    return CommandResult(
        command=command.command,
        ok=True,
        message='accepted',
        request_id=command.request_id,
        result=supervisor_module.group_result_mapping(result),
    )


def _lifecycle_source(path: Path) -> SourceState:
    return SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site='test',
        source='admin',
        stream_fingerprint=stream_fingerprint(
            'https://example.invalid',
            'admin',
            'test',
        ),
        source_generation='source-generation-1',
    )


def _maximum_status_snapshot(*, reverse: bool = False) -> StatusSnapshot:
    diagnostic = 'x' * 4096
    indices = range(99, -1, -1) if reverse else range(100)
    sources = tuple(
        SourceHealth(
            site=f'site-{index:03}',
            source=f'source-{index:03}',
            source_generation=f'generation-{index:03}',
            status='running',
            frontier=index,
            successful_clock=100.0,
            retry_attempt=1,
            next_retry_secs=1.0,
            open_range_count=0,
            late_id_count=0,
            last_error=diagnostic,
            worker_instance=f'spawn-{index:03}',
            initialized=True,
        )
        for index in indices
    )
    groups = tuple(
        GroupHealth(
            site=f'site-{index:03}',
            source=f'source-{index:03}',
            group=f'group-{index:03}',
            worker_instance=f'spawn-{index:03}',
            execution='sync',
            ordering='eager',
            phase='idle',
            status='running',
            cursor=index,
            current_event_id=None,
            current_plugin_id=None,
            current_callback_id=None,
            delivery_attempt=None,
            heartbeat_at=100.0,
            heartbeat_age=0.0,
            queue_event_count=0,
            queue_byte_count=0,
            catchup_frontier=None,
            pending_catchup_epoch_count=0,
            oldest_catchup_epoch_age=None,
            join_phase='attached',
            outstanding_late_obligation_count=1,
            outstanding_late_id_count=101,
            outstanding_late_ids=tuple(range(100)),
            outstanding_late_ids_truncated=True,
            parked=False,
            park_reason=diagnostic,
            failure_kind=None,
            failure_text=diagnostic,
            checkpoint_state='complete',
            conditions=(),
        )
        for index in indices
    )
    workers = tuple(
        WorkerHealth(
            site=f'site-{index:03}',
            worker_id=f'worker-{index:03}',
            spawn_instance=f'spawn-{index:03}',
            heartbeat_at=100.0,
            monitor_alive=True,
            thread_names=(f'lane:group-{index:03}',),
            stuck_threads=(),
            breaker_open=False,
            breaker_reason=diagnostic,
            socket_buffer_requested=1024,
            socket_send_buffer=2048,
            socket_receive_buffer=2048,
        )
        for index in indices
    )
    database_sizes = tuple(
        (f'sites/site-{index:03}/sources/source-{index:03}.db', index) for index in indices
    )
    transitions = tuple(
        BreakerTransition(
            sequence=index,
            worker_id=f'worker-{index:03}',
            state='open' if index % 2 else 'closed',
            reason=diagnostic,
        )
        for index in indices
    )
    return aggregate_status(
        sources=sources,
        groups=groups,
        workers=workers,
        now=100.0,
        stale_after_secs=10.0,
        audit_count=123_456,
        database_sizes=database_sizes,
        breaker_history=transitions,
        breaker_transition_count=900,
    )


def _never_reap(*, timeout: float = 0.0) -> None:
    assert timeout == 0.0


def _open_conditional_group(
    path: Path,
    *,
    group: str = 'remote',
) -> GroupState:
    options: GroupOpenOptions = {
        'writes': write_context_for(path),
        'site': 'test',
        'site_server_url': 'https://example.invalid',
        'source': 'admin',
        'group': group,
        'group_generation': 1,
        'ordering': 'eager',
        'stream_fingerprint': 'stream-1',
        'source_generation': 'source-generation-1',
        'verified_incarnation': 'incarnation-1',
        'start_mode': 'resume' if group_exists(path, group) else 'initialize',
    }
    if options['start_mode'] == 'initialize':
        options['initial_cursor'] = 36
        options['initial_delivery_floor'] = 36
        options['initial_last_delivered_id'] = 36
    return open_group_on_source(path, **options)


def _conditional_group(
    path: Path,
    *,
    group: str = 'remote',
) -> GroupState:
    state = _open_conditional_group(path, group=group)
    for event_id in range(37, 42):
        state.advance_irrelevant(
            event_id,
            expected_revision=state.snapshot().revision,
        )
    manifest = CallbackManifest(
        callbacks=(
            CallbackIdentity(
                ordinal=0,
                plugin_id='noop',
                callback_id='publish',
                execution='sync',
                filter_digest='filter-1',
                config_digest='config-1',
                secret_refs=(),
                credential_name='admin',
                credential_fingerprint='admin:test',
            ),
        )
    )
    state.start_event(42, manifest, expected_revision=5)
    started = state.begin_callback(0, expected_revision=6)
    assert started.revision == 7
    return state


def _create_recovery_group_database(
    config: Config,
    *,
    group: str = 'remote',
) -> None:
    path = source_database(config.daemon.state_dir)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with _conditional_group(path, group=group):
        pass


def _park_directive(evidence: _ParkEvidence) -> ConditionalParkDirective:
    return ConditionalParkDirective(
        evidence_id=evidence._evidence_id,
        site=evidence._site,
        group=evidence._group,
        group_generation=evidence._group_generation,
        failed_spawn_instance=evidence._failed_spawn_instance,
        event_id=evidence._event_id,
        group_revision=evidence._group_revision,
        observed_at_wall_clock=evidence._observed_at_wall_clock,
    )


def _park_evidence(
    **options: Unpack[_ParkEvidenceOptions],
) -> _ParkEvidence:
    return _ParkEvidence(
        _evidence_id=options.get('evidence_id', 'evidence-1'),
        _site=options.get('site', 'test'),
        _group=options.get('group', 'remote'),
        _group_generation=options.get('group_generation', 1),
        _failed_worker_id=options.get('failed_worker_id', 'dependent'),
        _failed_spawn_instance=options.get(
            'failed_spawn_instance',
            'dependent-spawn-1',
        ),
        _event_id=options.get('event_id', 42),
        _group_revision=options.get('group_revision', 7),
        _observed_at_wall_clock=options.get('observed_at_wall_clock', 10.0),
    )


def _parked_group_rows(state: _SupervisorState) -> list[tuple[str, str, int]]:
    rows = state._database.connection.execute(
        'SELECT site, group_name, group_generation FROM group_inventory '
        "WHERE configured = 1 AND lifecycle = 'readded-parked' "
        'ORDER BY site, group_name'
    ).fetchall()
    return [(str(row[0]), str(row[1]), int(row[2])) for row in rows]


def _prepare_absent_orphan(config: Config) -> tuple[Path, Path]:
    ensure_secure_state_dir(config.daemon.state_dir)
    source_path = config.daemon.state_dir / 'sites' / 'test' / 'sources' / 'admin.db'
    group_path = source_database(config.daemon.state_dir)
    source_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    group_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (
        _lifecycle_source(source_path) as source,
        _lifecycle_group(group_path) as group,
    ):
        source.initialize(100, incarnation='incarnation-1')
        Supervisor._orphan_group(source, group)
    with _SupervisorState._open(config.daemon.state_dir) as state:
        state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 1},
        )
        state._reconcile_groups(
            configured={},
            generations={('test', 'remote'): 1},
        )
    return source_path, group_path


def _prepare_readd_inventory(path: Path) -> None:
    with _SupervisorState._open(path) as state:
        state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 1},
        )
        state._reconcile_groups(
            configured={},
            generations={('test', 'remote'): 1},
        )
        state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 1},
        )


def _project_status(
    snapshot: StatusSnapshot,
    *,
    group_filter: str | None = None,
    site_filter: str | None = None,
) -> dict[str, object]:
    budget_status = supervisor_module._budget_status_payload
    return budget_status(
        dict(snapshot.to_mapping()),
        group_filter=group_filter,
        site_filter=site_filter,
    )


def _read_pipe(fd: int, size: int) -> bytes:
    deadline = time.monotonic() + 2.0
    readable, _writable, _exceptional = select.select(
        (fd,),
        (),
        (),
        max(0.0, deadline - time.monotonic()),
    )
    assert readable, 'child readiness timed out'
    return os.read(fd, size)


def _receive_source_endpoint_ready(
    worker: _WorkerInstance, *, deadline: float
) -> SourceEndpointReady:
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            message = worker._receive(timeout=min(1.0, remaining))
        except TimeoutError:
            continue
        if isinstance(message, SourceEndpointReady):
            return message
    raise AssertionError('worker did not publish SourceEndpointReady before deadline')


def _recovery_ready(
    worker: _ScriptedWorker,
    evidence: _ParkEvidence,
    *,
    outcome: Literal['parked', 'stale'] = 'parked',
    resulting_revision: int | None = None,
    spawn_instance: str | None = None,
) -> ControlMessage:
    ready_type = vars(ipc_module).get('GroupRecoveryReady')
    assert ready_type is not None
    revision = evidence._group_revision + 1 if resulting_revision is None else resulting_revision
    return cast(
        'ControlMessage',
        ready_type(
            spawn_instance=(worker._spawn_instance if spawn_instance is None else spawn_instance),
            evidence_id=evidence._evidence_id,
            site=evidence._site,
            group=evidence._group,
            group_generation=evidence._group_generation,
            outcome=outcome,
            resulting_revision=revision,
        ),
    )


def _replace_config_text(text: str, old: str, new: str) -> str:
    assert text.count(old) == 1, f'expected exactly one configuration fragment: {old!r}'
    return text.replace(old, new)


def _resolve_lane_recovery(
    config: Config,
    assignment: WorkerAssignment,
    group_path: Path,
    *,
    drop_ready: bool,
) -> GroupRecoveryReady:
    control = _LaneRecoveryControl(drop_ready=drop_ready)
    lane, context = _lane_recovery_probe(config, assignment, control)
    lane_type = cast('type[object]', vars(worker_module)['_LaneComponent'])
    resolve = cast(
        'Callable[[object, object, GroupState], object]',
        vars(lane_type)['_resolve_conditional_park'],
    )
    if drop_ready:
        with (
            _open_conditional_group(group_path) as group,
            pytest.raises(EOFError, match='lost group recovery ready'),
        ):
            resolve(lane, context, group)
    else:
        with _open_conditional_group(group_path) as group:
            resolve(lane, context, group)
    assert len(control._messages) == 1
    return control._messages[0]


def _source_adoption_command(request_id: str) -> OperatorCommand:
    return OperatorCommand(
        request_id=request_id,
        command='adopt-source-stream',
        arguments={
            'site': 'test',
            'source': 'admin',
            'reason': 'permission role changed',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )


def _source_adoption_result(message: OperatorCommand) -> CommandResult:
    if message.command == 'preflight-source-adoption':
        result: Mapping[str, object] = {
            'adoption_id': 'adoption-reservation-1',
            'command_fingerprint': 'adoption-fingerprint-1',
            'recorded': False,
        }
    elif message.command == 'park':
        result = {'group': 'remote', 'group_generation': 1, 'parked': True}
    else:
        result = {
            'adoption_count': 1,
            'initialized': True,
            'source': 'admin',
            'status': 'running',
            'stream_incarnation': 'adoption-reservation-1',
        }
    return CommandResult(
        command=message.command,
        ok=True,
        message='accepted',
        request_id=message.request_id,
        result=result,
    )


def _source_assignment(spec: WorkerSpec, spawn_instance: str) -> WorkerAssignment:
    return WorkerAssignment(
        worker_id=spec.worker_id,
        spawn_instance=spawn_instance,
        source_generations=(('admin', 'source-generation-1'),),
        group_assignments=(),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='initialize',
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )


def _source_only_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    path = tmp_path / 'worker.toml'
    plugin_path = Path(__file__).parent / 'fixtures' / 'noop_plugin.py'
    path.write_text(
        '\n'.join(
            (
                '[daemon]',
                f'state_dir = "{tmp_path / "state"}"',
                'sg_api_provider = "shotgun-api3"',
                'checkpoint_interval_secs = 0.1',
                'health_interval_secs = 0.05',
                'shutdown_deadline_secs = 2.0',
                'worker_startup_deadline_secs = 10.0',
                '',
                '[sites.test]',
                'server = "https://example.invalid"',
                'default_source = "admin"',
                '',
                '[sites.test.credentials.admin]',
                'script_name = "test"',
                'api_key = { env = "SG_EVENT_ADMIN_KEY" }',
                '',
                '[sites.test.sources.admin]',
                'credentials = "admin"',
                '',
                '[sites.test.workers.pool]',
                '',
                '[sites.test.groups.remote]',
                'worker = "pool"',
                '',
                '[[sites.test.groups.remote.plugins]]',
                'id = "noop"',
                f'path = "{plugin_path}"',
            )
        )
        + '\n',
        encoding='utf-8',
    )
    return path, load_config(path)


def _absent_orphan_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    path, config = _source_only_config(tmp_path, monkeypatch)
    site = config.sites['test']
    return (
        path,
        replace(
            config,
            sites=MappingProxyType(
                {
                    'test': replace(
                        site,
                        groups=MappingProxyType({}),
                    )
                }
            ),
        ),
    )


def _default_packed_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    config_path, _config = _source_only_config(tmp_path, monkeypatch)
    config_path.write_text(
        _replace_config_text(config_path.read_text(encoding='utf-8'), 'worker = "pool"\n', ''),
        encoding='utf-8',
    )
    return config_path, load_config(config_path)


def _local_and_remote_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    path, _config = _source_only_config(tmp_path, monkeypatch)
    plugin_path = Path(__file__).parent / 'fixtures' / 'noop_plugin.py'
    path.write_text(
        path.read_text(encoding='utf-8')
        + '\n'.join(
            (
                '',
                '[sites.test.groups.local]',
                '',
                '[[sites.test.groups.local.plugins]]',
                'id = "noop"',
                f'path = "{plugin_path}"',
                '',
            )
        ),
        encoding='utf-8',
    )
    return path, load_config(path)


def _source_watchdog(
    worker: _ScriptedWorker,
    *,
    deadline_at: float,
    heartbeat_at: float,
    phase: Literal['internal', 'provider-query', 'shutdown', 'startup', 'wait'],
) -> WatchdogHealth:
    source = worker._spec.sources[0]
    return WatchdogHealth(
        kind='source',
        site=worker._spec.site,
        source=source,
        group=None,
        worker_instance=worker._spawn_instance,
        phase=phase,
        heartbeat_at=heartbeat_at,
        deadline_at=deadline_at,
    )


def _spawn_health(supervisor: Supervisor, worker_id: str) -> HealthUpdate:
    health = supervisor._spawns[worker_id]._health
    assert health is not None
    return health


def _spawn_lifecycle(supervisor: Supervisor, worker_id: str) -> supervisor_module._WorkerLifecycle:
    lifecycle = supervisor._spawns[worker_id]._lifecycle
    assert lifecycle is not None
    return lifecycle


def _start_blocked_dependent_grant(
    supervisor: Supervisor,
    factory: _ScriptedWorkerFactory,
) -> tuple[_ScriptedWorker, _ScriptedWorker]:
    supervisor._reconcile(generations={})
    supervisor.start()
    source = next(worker for worker in factory._instances.values() if worker._spec.sources)
    source._authentication_ready = True
    supervisor._tick()
    dependent = next(worker for worker in factory._instances.values() if not worker._spec.sources)
    source._auto_grant_ready = False
    dependent._authentication_ready = True
    supervisor._tick()
    return source, dependent


def _two_dependent_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    path, _config = _source_only_config(tmp_path, monkeypatch)
    plugin_path = Path(__file__).parent / 'fixtures' / 'noop_plugin.py'
    path.write_text(
        path.read_text(encoding='utf-8')
        + '\n'.join(
            (
                '',
                '[sites.test.workers.second]',
                '',
                '[sites.test.groups.second]',
                'worker = "second"',
                '',
                '[[sites.test.groups.second.plugins]]',
                'id = "noop"',
                f'path = "{plugin_path}"',
                '',
            )
        ),
        encoding='utf-8',
    )
    return path, load_config(path)


def _two_group_shared_worker_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    path, _config = _source_only_config(tmp_path, monkeypatch)
    plugin_path = Path(__file__).parent / 'fixtures' / 'noop_plugin.py'
    path.write_text(
        path.read_text(encoding='utf-8')
        + '\n'.join(
            (
                '',
                '[sites.test.groups.second]',
                'worker = "pool"',
                '',
                '[[sites.test.groups.second.plugins]]',
                'id = "noop"',
                f'path = "{plugin_path}"',
                '',
            )
        ),
        encoding='utf-8',
    )
    return path, load_config(path)


def _wait_for_supervisor_probe_ready(
    supervisor: Supervisor,
    factory: _ScriptedWorkerFactory,
) -> None:
    startup_deadline = time.monotonic() + 2.0
    while True:
        heartbeat_at = time.monotonic()
        for worker in tuple(factory._instances.values()):
            worker._messages.append(_health_update(worker, heartbeat_at=heartbeat_at))
        supervisor._tick()
        if supervisor.probe_result().readiness:
            return
        if time.monotonic() >= startup_deadline:
            raise TimeoutError('supervisor did not publish healthy probe state')


def _run_frozen_supervisor_probe_child(
    config_path: str,
    connection: Connection,
) -> None:
    probe: ProbeServer | None = None
    try:
        control_module.REQUEST_TIMEOUT_SECS = 0.05
        supervisor_module.BUSY_TIMEOUT_SECS = 0.05
        config = load_config(Path(config_path))
        config = replace(
            config,
            daemon=replace(
                config.daemon,
                health_interval_secs=0.05,
                supervisor_poll_interval_secs=0.05,
            ),
        )
        factory = _ScriptedWorkerFactory()
        with Supervisor(
            config=config,
            config_path=Path(config_path),
            worker_factory=factory,
        ) as supervisor:
            supervisor._reconcile(generations={})
            supervisor.start()
            _wait_for_supervisor_probe_ready(supervisor, factory)

            probe = ProbeServer('127.0.0.1', 0, supervisor.probe_result)
            probe.start()
            progress = vars(supervisor)['_progress']
            stale_after = cast('float', vars(progress)['_supervisor_stale_after'])
            connection.send(('ready', _child_probe_address(probe), stale_after))
            while True:
                if connection.poll(0.005) and connection.recv() == 'freeze':
                    connection.send(('frozen', time.monotonic()))
                    threading.Event().wait()
                heartbeat_at = time.monotonic()
                for worker in tuple(factory._instances.values()):
                    worker._messages.append(_health_update(worker, heartbeat_at=heartbeat_at))
                supervisor._tick()
    except (AssertionError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError) as error:
        connection.send(('error', f'{type(error).__name__}: {error}'))
    finally:
        if probe is not None:
            probe.close()
            probe.join(timeout=1.0)
        connection.close()


def kill_supervisor_store_boundary(config_path: Path, mode: str, stage: str, proof: Path) -> None:
    config = load_config(config_path)
    if mode == 'absent':
        config = replace(config, sites={'test': replace(config.sites['test'], groups={})})
    command = OperatorCommand(
        request_id='cross-store-kill',
        command='reset-group-to-latest',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'cross-store recovery'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    original_dispose = GroupState.dispose

    def die() -> None:
        with proof.open('w') as stream:
            stream.write(stage)
            stream.flush()
            os.fsync(stream.fileno())
        os.kill(os.getpid(), signal.SIGKILL)

    def dispose(state: GroupState, request: DispositionRequest) -> GroupSnapshot:
        if stage == 'reservation':
            die()
        result = original_dispose(state, request)
        if stage == 'source-committed':
            die()
        return result

    original_lifecycle = GroupState.change_lifecycle

    def change_lifecycle(state: GroupState, request: LifecycleRequest) -> GroupSnapshot:
        result = original_lifecycle(state, request)
        if stage == 'orphan-committed' and request.command == 'orphan':
            die()
        return result

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
        patch.setattr(GroupState, 'change_lifecycle', change_lifecycle)
        patch.setattr(GroupState, 'dispose', dispose)
        with Supervisor(config=config, config_path=config_path) as supervisor:
            supervisor.reconcile_durable_state()
            result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
            assert result.ok
            assert stage == 'inventory-completed'
            die()


def test_absent_orphan_replay_commits_once_and_retry_returns_recorded_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, _group_path = _prepare_absent_orphan(config)
    with _lifecycle_source(
        source_path,
    ) as source:
        source.commit_observations((110,), successful_clock=1.0)
    command = OperatorCommand(
        request_id='replay-absent-once',
        command='replay',
        arguments={
            'site': 'test',
            'group': 'remote',
            'reason': 'recover absent history',
            'from_event_id': 101,
            'acknowledge_truncation': False,
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    transaction_levels: list[int] = []
    original = write_lock_module.WriteContext.transaction

    @contextmanager
    def record_transaction(
        writes: write_lock_module.WriteContext,
        connection: sqlite3.Connection,
    ) -> Generator[sqlite3.Connection]:
        database = connection.execute('PRAGMA database_list').fetchone()
        selected = database is not None and Path(database[2]) == source_path
        level = connection.execute('PRAGMA synchronous').fetchone()[0]
        with original(writes, connection) as active:
            yield active
        if selected:
            transaction_levels.append(level)

    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=_ScriptedWorkerFactory(),
    ) as supervisor:
        supervisor.reconcile_durable_state()
        with monkeypatch.context() as recording:
            recording.setattr(write_lock_module.WriteContext, 'transaction', record_transaction)
            first = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
            assert first.ok
            assert first.result['group_generation'] == 2
            assert transaction_levels == [2]
            repeated = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
            assert repeated.message == 'recorded'
            assert repeated.result == first.result
            assert transaction_levels == [2]
        with _lifecycle_source(
            source_path,
        ) as source:
            source.commit_observations((120,), successful_clock=2.0)
        with monkeypatch.context() as recording:
            recording.setattr(write_lock_module.WriteContext, 'transaction', record_transaction)
            advanced = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
            assert advanced.result == first.result
            assert transaction_levels == [2]
        state = cast('_SupervisorState', vars(supervisor)['_state'])
        record = state._group_inventory_record(site='test', group='remote')
        assert record is not None
        assert (record._orphan_generation, record._generation) == (None, 2)
    with _lifecycle_group(source_path, generation=2) as group:
        assert group.snapshot().cursor == 100
        assert group.snapshot().replay_required_from is None
        terminal = group.terminal_disposition_identity(2)
        assert terminal is not None
        assert terminal[2] == 'applied'
        assert sum(item.command == 'replay' for item in group._audit_records()) == 1


def test_adopt_source_stream_completes_on_default_packing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _default_packed_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    command = _source_adoption_command('adopt-default-packed')
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_spec = build_worker_specs(config.sites['test'])[0]
        source_owner = factory._instances[source_spec.worker_id]
        source_owner._operator_handler = _source_adoption_result

        result = supervisor.adopt_source_stream_command(command, timeout=1.0)

        replacement = factory._instances[source_spec.worker_id]
        assert result.ok, result.message
        assert replacement is not source_owner
        assert replacement._assignment is not None
        assert replacement._assignment.source_generations


def test_adopt_source_stream_reports_default_worker_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _default_packed_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    monkeypatch.setattr(supervisor_module.time, 'sleep', clock.advance)
    command = _source_adoption_command('adopt-default-packed-failure')
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_spec = build_worker_specs(config.sites['test'])[0]
        source_owner = factory._instances[source_spec.worker_id]
        source_owner._operator_handler = _source_adoption_result
        factory._authentication_ready = False

        result = supervisor.adopt_source_stream_command(command, timeout=1.0)

        replacement = factory._instances[source_spec.worker_id]
        assert not result.ok
        assert source_spec.worker_id in result.message
        assert replacement._assignment is None


def test_channel_recovery_cannot_borrow_another_spawns_lifecycle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    _create_recovery_group_database(config)
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=_FakeClock(), worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={('test', 'remote'): 1})
        supervisor.start()
        current = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        old = _ScriptedWorker(current._spec, 'old-spawn', [], None, authentication_ready=True)
        assert _spawn_lifecycle(supervisor, current._worker_id)._worker is current
        with pytest.raises(ValueError, match='channel lifecycle'):
            supervisor._accept_worker_monitor_message(old, _recovery_ready(current, evidence))
        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == (evidence,)
        assert not current._killed


def test_checkpoint_error_is_not_replaced_by_shutdown_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    clock = _FakeClock()
    factory = _ScriptedWorkerFactory()

    def checkpointer_factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        clock: Callable[[], float],
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            clock=clock,
            run_checkpoint=lambda: (1, 5, 0),
            random_value=lambda: 0,
        )

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        checkpointer_factory=checkpointer_factory,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        dependent._shutdown_status = None
        for _ in range(4):
            clock.advance(config.daemon.checkpoint_interval_secs)
            assert supervisor._tick()

        clock.advance(config.daemon.checkpoint_interval_secs)
        with pytest.raises(
            DatabaseError, match='checkpoint incomplete duration bound exceeded'
        ) as caught:
            supervisor._tick()

        checkpoint_error = str(caught.value)
        assert caught.value.__notes__ == [
            f'worker child remained unreaped: {dependent._worker_id}'
        ]
        assert supervisor._halt_reason_value == checkpoint_error
    finally:
        supervisor._close()


def test_child_process_stale_supervisor_changes_livez_to_503(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, _config = _source_only_config(tmp_path, monkeypatch)
    context = multiprocessing.get_context('spawn')
    parent, child = context.Pipe()
    process = context.Process(
        target=_run_frozen_supervisor_probe_child,
        args=(os.fspath(config_path), child),
    )
    process.start()
    child.close()
    try:
        assert parent.poll(5.0), 'supervisor child did not report probe startup'
        message = parent.recv()
        assert message[0] == 'ready', message
        address = cast('tuple[str, int]', message[1])
        stale_after = cast('float', message[2])
        assert stale_after == pytest.approx(0.2)
        assert _child_probe_response(address, timeout=1.0).startswith(b'HTTP/1.1 200 OK\r\n')

        parent.send('freeze')
        assert parent.poll(2.0), 'supervisor child did not acknowledge the foreground freeze'
        frozen = parent.recv()
        assert frozen[0] == 'frozen', frozen
        response = b''
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            response = _child_probe_response(address, timeout=1.0)
            if response.startswith(b'HTTP/1.1 503 Service Unavailable\r\n'):
                break
            time.sleep(0.005)

        assert response == (
            b'HTTP/1.1 503 Service Unavailable\r\n'
            b'Content-Type: text/plain; charset=us-ascii\r\n'
            b'Content-Length: 12\r\n'
            b'Connection: close\r\n'
            b'\r\n'
            b'unavailable\n'
        )
    finally:
        _force_reap_test_process(process)
        parent.close()


def test_completed_adoption_keeps_unpark_fenced_by_durable_group_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    identity = AuditIdentity(local_uid=501, username='operator', pid=456)
    adoption = OperatorCommand(
        request_id='adopt-with-backlog',
        command='adopt-source-stream',
        arguments={
            'site': 'test',
            'source': 'admin',
            'reason': 'permission role changed',
        },
        identity=identity,
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_owner = next(
            worker for worker in factory._instances.values() if worker._spec.sources
        )
        group_owner = next(worker for worker in factory._instances.values() if worker._spec.groups)

        def _adopt_source(message: OperatorCommand) -> CommandResult:
            result: Mapping[str, object]
            if message.command == 'preflight-source-adoption':
                result = {
                    'adoption_id': 'adoption-reservation-1',
                    'command_fingerprint': 'adoption-fingerprint-1',
                    'recorded': False,
                }
            else:
                result = {
                    'adoption_count': 1,
                    'initialized': True,
                    'source': 'admin',
                    'status': 'running',
                    'stream_incarnation': 'adoption-reservation-1',
                }
            return CommandResult(
                command=message.command,
                ok=True,
                message='accepted',
                request_id=message.request_id,
                result=result,
            )

        source_owner._operator_handler = _adopt_source
        group_owner._operator_handler = lambda message: CommandResult(
            command=message.command,
            ok=True,
            message='accepted',
            request_id=message.request_id,
            result={
                'group': 'remote',
                'group_generation': 1,
                'parked': message.command == 'park',
                'revision': 1,
            },
        )

        result = supervisor.adopt_source_stream_command(adoption, timeout=1.0)

        assert result.ok
        with pytest.raises(ValueError, match='disposition'):
            supervisor.dispatch_operator_command(
                OperatorCommand(
                    request_id='queued-unpark',
                    command='unpark',
                    arguments={
                        'site': 'test',
                        'group': 'remote',
                        'group_generation': 1,
                        'reason': 'queued before adoption completed',
                    },
                    identity=identity,
                ),
                timeout=1.0,
            )

    assert all(
        sent.command != 'unpark'
        for history in factory._history.values()
        for worker in history
        for sent in worker._sent_commands
    )


def test_containment_kills_overdue_worker_when_park_evidence_read_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config = _two_group_shared_worker_config(tmp_path, monkeypatch)
    clock = _FakeClock()
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        wall_clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        remote = next(w for w in factory._instances.values() if not w._spec.sources)
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=0))
        supervisor._tick()
        clock.advance(10.1)
        confirmed = replace(_in_flight_group_health(remote, heartbeat_at=0), deadline_at=9.9)
        for worker in factory._instances.values():
            worker._messages.append(
                _health_update(
                    worker, heartbeat_at=10, groups=(confirmed,) if worker is remote else None
                )
            )
        calls: list[tuple[str, float, bool]] = []
        original_connect = sqlite3.connect

        def fail_evidence(
            database: str, *, timeout: float = 5.0, uri: bool = False
        ) -> sqlite3.Connection:
            if uri:
                calls.append((database, timeout, uri))
                raise sqlite3.OperationalError('park evidence unavailable')
            return original_connect(database, timeout=timeout, uri=uri)

        path = source_database(config.daemon.state_dir)
        with _conditional_group(path), monkeypatch.context() as patch:
            patch.setattr(sqlite3, 'connect', fail_evidence)
            supervisor._tick()
        assert calls == [(f'{path.as_uri()}?mode=ro', 0.0, True)]
        assert remote._killed
        assert supervisor._park_evidence_by_group == {}


def test_dependent_grant_discards_late_operator_result_before_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        dependent._wait_results.append(None)
        grant = source._sent_grants[-1]
        ready = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=grant.dependent_spawn_instance,
        )
        source._messages.extend(
            (
                CommandResult(
                    command='adopt-source-stream',
                    ok=True,
                    message='late',
                    request_id='operator-1',
                    result={'source': grant.source, 'status': 'running'},
                ),
                ready,
            )
        )

        supervisor._tick()

        assert supervisor._spawns[dependent._worker_id]._termination_reason is None
        assert not dependent._killed
        assert tuple(source._messages) == ()
        assert not dependent._killed
        assert dependent._assignment is not None
        assert supervisor._spawns[dependent._worker_id]._lifecycle is None


def test_direct_disposition_preserves_control_response_field_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    _prepare_absent_orphan(config)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor.reconcile_durable_state()
        result = supervisor.dispatch_group_disposition_command(
            OperatorCommand(
                command='reset-group-to-latest',
                request_id='direct-wire-order',
                arguments={'site': 'test', 'group': 'remote', 'reason': 'verify result order'},
                identity=AuditIdentity.current(),
            ),
            timeout=1.0,
        )
    frame = control_module._encode_response(
        control_module._ControlResponse(1, result.request_id, result.ok, result.result, None)
    )
    assert frame[4:] == (
        b'{"version":1,"request_id":"direct-wire-order","ok":true,"result":'
        b'{"conditions":["parked"],"cursor":100,"group":"remote","group_generation":2,'
        b'"parked":true,"revision":3,"site":"test","source":"admin","status":"parked"},'
        b'"error":null}'
    )


def test_discard_worker_lifecycle_clears_attributed_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    with Supervisor(config=config, config_path=config_path, worker_factory=factory) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(worker for worker in factory._instances.values() if worker._spec.groups)
        supervisor._spawns[worker._worker_id]._attributed_park_failure = True
        supervisor._replace_worker(worker._worker_id)
        replacement = factory._instances[worker._worker_id]
        replacement._exit_status = 1
        supervisor._tick()
        breaker = supervisor._breakers[worker._worker_id]
        assert breaker._failure_total == 1
        assert breaker._restart_deadline_at is not None


@pytest.mark.parametrize('message_kind', ['fatal', 'recovery'])
def test_expired_grant_owner_preserves_consumed_source_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, message_kind: str
) -> None:
    config_path, config = _local_and_remote_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        assert source._assignment is not None
        assert dependent._assignment is not None
        assert not supervisor._accept_worker_health(source, _health_update(source, heartbeat_at=0))
        source._auto_grant_ready = False
        supervisor._spawns[dependent._worker_id]._lifecycle = supervisor_module._WorkerLifecycle(
            _assignment=dependent._assignment,
            _deadline_at=0.1,
            _worker=dependent,
            _authenticated=True,
        )
        # Establish the grant owner before queuing source recovery traffic.
        supervisor._advance_dependent_lifecycle(_spawn_lifecycle(supervisor, dependent._worker_id))
        evidence = _park_evidence(group='local')
        assert supervisor._state is not None
        if message_kind == 'fatal':
            healthy = _health_update(source, heartbeat_at=0)
            message = replace(healthy, worker=replace(healthy.worker, monitor_alive=False))
        else:
            assert supervisor._state._record_park_evidence(evidence)
            supervisor._park_evidence_by_group[('test', 'local')] = evidence
            supervisor._spawns[source._worker_id]._lifecycle = supervisor_module._WorkerLifecycle(
                _assignment=source._assignment,
                _deadline_at=10,
                _worker=source,
                _authenticated=True,
                _assigned=True,
                _ready=True,
                _pending_recovery={evidence._evidence_id: _park_directive(evidence)},
            )
            message = _recovery_ready(source, evidence)
        tail = _health_update(source, heartbeat_at=0.1)
        late_ready = EventPeerReady(
            source=dependent._assignment.event_grants[0].source,
            group=dependent._assignment.event_grants[0].group,
            dependent_spawn_instance=dependent._spawn_instance,
        )
        source._messages.extend(
            (message, tail) if message_kind == 'fatal' else (message, late_ready, tail)
        )
        observed: list[ControlMessage] = []
        receive = source._receive

        def cross_deadline(*, timeout: float | None = None) -> ControlMessage:
            current = receive(timeout=timeout)
            observed.append(current)
            clock._now = 0.1
            return current

        monkeypatch.setattr(source, '_receive', cross_deadline)
        acquisitions = source._batch_acquisitions
        supervisor._advance_worker_messages()
        assert observed[0] is message
        assert dependent._killed
        assert (
            supervisor._spawns[dependent._worker_id]._termination_reason
            == 'worker-startup-timeout'
        )
        assert source._worker_id not in supervisor._grant_worker_ids()
        assert source._batch_acquisitions == acquisitions + 1
        if message_kind == 'fatal':
            assert source._killed
            assert (
                supervisor._spawns[source._worker_id]._termination_reason
                == 'worker-heartbeat-overdue'
            )
            assert tuple(source._messages) == (tail,)
        else:
            assert not source._killed
            assert observed == [message, late_ready, tail]
            assert late_ready not in supervisor._stale_grant_readies
            assert supervisor._state._pending_park_evidence() == ()


@pytest.mark.parametrize('owner', ['steady', 'startup', 'ready', 'grant', 'operator'])
@pytest.mark.parametrize('known', [False, True])
def test_channel_owners_accept_only_exact_fenced_grant_replies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, owner: str, known: bool
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=_FakeClock(), worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        assert source._assignment is not None
        assert dependent._assignment is not None
        grant = dependent._assignment.event_grants[0]
        fenced = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=dependent._spawn_instance,
        )
        supervisor._stale_grant_readies.add(fenced)
        message = fenced if known else replace(fenced, dependent_spawn_instance='unknown-spawn')
        lifecycle = supervisor_module._WorkerLifecycle(
            _assignment=dependent._assignment if owner == 'grant' else source._assignment,
            _deadline_at=10,
            _worker=dependent if owner == 'grant' else source,
            _authenticated=True,
            _assigned=True,
            _ready=owner == 'ready',
        )

        def dispatch() -> object:
            if owner == 'steady':
                return supervisor._accept_worker_monitor_message(source, message)
            if owner == 'startup':
                return supervisor._advance_source_message(lifecycle, message)
            if owner == 'grant':
                return supervisor._accept_dependent_grant_message(
                    lifecycle, grant, source, message
                )
            source._messages.append(message)
            if owner == 'ready':
                return supervisor._advance_ready_lifecycle(lifecycle)
            return supervisor._accept_operator_batch(
                source,
                _source_adoption_command('fenced-reply'),
                source._receive_available(),
                time.monotonic() + 1,
            )

        if known:
            dispatch()
            assert fenced not in supervisor._stale_grant_readies
        else:
            with pytest.raises((TypeError, ValueError)):
                dispatch()
            assert fenced in supervisor._stale_grant_readies
        assert not source._killed
        assert not lifecycle._endpoint_messages
        assert lifecycle._grant_index == 0


def test_expired_grant_releases_source_batch_without_consuming_a_frame(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _local_and_remote_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        assert source._assignment is not None
        assert dependent._assignment is not None
        assert not supervisor._accept_worker_health(source, _health_update(source, heartbeat_at=0))
        evidence = _park_evidence(group='local')
        state = supervisor._state
        assert state is not None
        assert state._record_park_evidence(evidence)
        supervisor._park_evidence_by_group[('test', 'local')] = evidence
        supervisor._spawns[source._worker_id]._lifecycle = supervisor_module._WorkerLifecycle(
            _assignment=source._assignment,
            _deadline_at=10,
            _worker=source,
            _authenticated=True,
            _assigned=True,
            _ready=True,
            _pending_recovery={evidence._evidence_id: _park_directive(evidence)},
        )
        supervisor._spawns[dependent._worker_id]._lifecycle = supervisor_module._WorkerLifecycle(
            _assignment=dependent._assignment,
            _deadline_at=0.1,
            _worker=dependent,
            _authenticated=True,
            _grant_sent=True,
        )
        grant = dependent._assignment.event_grants[0]
        late_ready = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=dependent._spawn_instance,
        )
        source._messages.extend(
            (
                _health_update(source, heartbeat_at=0.5),
                late_ready,
                _recovery_ready(source, evidence),
            )
        )
        acquisitions = source._batch_acquisitions
        clock._now = 0.5
        supervisor._advance_worker_messages()
        assert dependent._killed
        assert not source._killed
        assert source._batch_acquisitions == acquisitions + 1
        assert not source._messages
        assert _spawn_health(supervisor, source._worker_id).worker.heartbeat_at == 0.5
        assert late_ready not in supervisor._stale_grant_readies
        assert state._pending_park_evidence() == ()


def test_failed_lane_disposition_keeps_spawn_health_inventory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    path = source_database(config.daemon.state_dir)
    with _lifecycle_group(path) as group:
        group.park(
            reason='prepare reset',
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
            expected_revision=group.snapshot().revision,
        )
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(w for w in factory._instances.values() if w._spec.sources)
        lane = next(w for w in factory._instances.values() if w._spec.groups)
        for worker in (source, lane):
            assert not supervisor._accept_worker_health(
                worker, _health_update(worker, heartbeat_at=clock())
            )
        results = iter(({'disposition_required': True}, {'code': 'source-database-busy'}))
        lane._operator_handler = lambda command: CommandResult(
            command=command.command,
            request_id=command.request_id,
            ok=(result := next(results)).get('disposition_required') is True,
            message='test result',
            result=result,
        )
        command = OperatorCommand(
            command='reset-group-to-latest',
            request_id='busy-lane',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'retry busy lane'},
            identity=AuditIdentity.current(),
        )
        assert not supervisor.dispatch_group_disposition_command(command, timeout=1.0).ok
        for worker in (source, lane):
            update = _health_update(worker, heartbeat_at=clock())
            assert not supervisor._accept_worker_health(worker, update)
        assert not supervisor._monitor_worker_health(clock())
        assert all(spawn._termination_reason is None for spawn in supervisor._spawns.values())
        assert all(b._failure_total == 0 for b in supervisor._breakers.values())
        assert supervisor._pending_worker_groups(source) == set()
        supervisor._shutdown_workers('replace after retained reservation')
        supervisor.start()
        replacement = factory._instances[source._worker_id]
        assert replacement._spawn_instance != source._spawn_instance
        assert supervisor._pending_worker_groups(source) == set()
        assert supervisor._pending_worker_groups(replacement) == {'remote'}
        assert all(not w._spec.groups for w in factory._instances.values())
        group_health = replace(
            _in_flight_group_health(
                replacement, group='remote', heartbeat_at=clock(), phase='idle'
            ),
            parked=True,
            conditions=('parked',),
        )
        update = _health_update(replacement, heartbeat_at=clock())
        assert not supervisor._accept_worker_health(
            replacement, replace(update, groups=(group_health,))
        )
        assert all(spawn._termination_reason is None for spawn in supervisor._spawns.values())

        replacement._operator_handler = lambda message: _execute_source_group_command(
            path, message
        )
        result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert result.ok
        current_source = factory._instances[source._worker_id]
        assert current_source is not replacement
        assert supervisor._pending_worker_groups(current_source) == set()
        assert all(b._failure_total == 0 for b in supervisor._breakers.values())


def test_failed_spawn_cleanup_slices_forced_reap_under_one_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FakeClock()
    advances: list[None] = []
    timeouts: list[float] = []
    process = cast('ChildProcess', SimpleNamespace(returncode=None))
    spec = cast('WorkerSpec', SimpleNamespace(worker_id='source-worker'))

    def sliced_reap(
        child: ChildProcess,
        *,
        timeout: float = 0.0,
    ) -> int | None:
        assert child is process
        timeouts.append(timeout)
        clock.advance(timeout)
        return 0 if len(timeouts) == 3 else None

    monkeypatch.setattr(supervisor_module.time, 'monotonic', clock)
    monkeypatch.setattr(supervisor_module, 'reap_child', sliced_reap)

    def terminate(_child: ChildProcess, *, force: bool) -> None:
        assert force

    monkeypatch.setattr(supervisor_module, 'terminate_child', terminate)

    cleanup = supervisor_module._cleanup_failed_worker_spawn
    cleanup(
        process=process,
        shutdown_deadline_secs=1.0,
        wait_slice_secs=0.25,
        foreground_progress=lambda: advances.append(None),
        spec=spec,
        spawn_instance='failed-spawn',
    )

    assert timeouts == [0.25, 0.25, 0.25]
    assert max(timeouts) <= 0.25
    # Force termination, entry to the bounded wait, and the successful reap
    # are semantic advances; the two `None` slice returns are not.
    assert len(advances) == 3


@pytest.mark.parametrize('command_name', ['replay', 'reset-group-to-latest'])
def test_group_disposition_forwards_validated_arguments(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, command_name: str
) -> None:
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    _prepare_absent_orphan(config)
    factory = _ScriptedWorkerFactory()
    arguments: dict[str, object] = {'site': 'test', 'group': 'remote', 'reason': 'recover'}
    if command_name == 'replay':
        arguments.update(from_event_id=50, acknowledge_truncation=True)
    command = OperatorCommand(
        request_id='validated-values',
        command=command_name,
        arguments=arguments,
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(config=config, config_path=config_path, worker_factory=factory) as supervisor:
        supervisor.reconcile_durable_state()
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        source._operator_handler = lambda sent: CommandResult(
            command=sent.command,
            request_id=sent.request_id,
            ok=False,
            message='captured',
            result={},
        )
        supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert len(source._sent_commands) == 1
        forwarded = source._sent_commands[0]
        assert forwarded.command == 'group-disposition'
        assert forwarded.arguments['disposition_id'] == supervisor_module.stable_operator_id(
            command.command, command.arguments
        )
        assert forwarded.arguments['from_event_id'] == (50 if command_name == 'replay' else None)
        assert forwarded.arguments['acknowledge_truncation'] is (command_name == 'replay')
        assert forwarded.arguments['reason'] == 'recover'
        assert forwarded.arguments['operator_label'] == 'operator uid=501'


@pytest.mark.parametrize(
    'running_lane', [False, True], ids=['before_reservation', 'before_lane_dispatch']
)
def test_group_disposition_rejects_invalid_position(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, running_lane: bool
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    with _lifecycle_group(source_database(config.daemon.state_dir)) as group:
        group.park(
            reason='prepare replay',
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
            expected_revision=group.snapshot().revision,
        )
    command = OperatorCommand(
        request_id='invalid-position',
        command='replay',
        arguments={
            'site': 'test',
            'group': 'remote',
            'reason': 'recover',
            'from_event_id': 'x',
            'acknowledge_truncation': False,
        },
        identity=AuditIdentity.current(),
    )
    with Supervisor(config=config, config_path=config_path, worker_factory=factory) as supervisor:
        supervisor.reconcile_durable_state()
        if running_lane:
            supervisor.start()
            assert any('remote' in worker._spec.groups for worker in factory._instances.values())
        assert supervisor._state is not None
        before = supervisor._state._inventory_records()[('test', 'remote')]
        with pytest.raises(ValueError, match='replay position must be a nonnegative integer'):
            supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert supervisor._state._inventory_records()[('test', 'remote')] == before
        assert all(not worker._sent_commands for worker in factory._instances.values())


def test_group_omission_and_readd_cannot_route_old_generation_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._reconcile(
            generations={},
        )
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(_park_evidence(group_revision=0))

    omitted_config = replace(
        config,
        sites={
            'test': replace(
                config.sites['test'],
                groups={},
                workers={},
            )
        },
    )
    with (
        _lifecycle_source(tmp_path / 'readd-source.db') as source,
        _lifecycle_group(tmp_path / 'readd-source.db') as group,
    ):
        source.initialize(100, incarnation='incarnation-1')
        with Supervisor(
            config=omitted_config,
            config_path=config_path,
        ) as supervisor:
            supervisor._reconcile(
                generations={('test', 'remote'): 1},
                source_states={('test', 'admin'): source},
                group_states={('test', 'remote'): group},
            )

        with Supervisor(config=config, config_path=config_path) as supervisor:
            supervisor._reconcile(
                generations={('test', 'remote'): 1},
            )
            build_assignment = Supervisor._build_worker_assignment
            assignment = build_assignment(
                supervisor,
                build_worker_specs(config.sites['test'])[1],
                'pending-spawn-1',
                (),
                (),
                (),
            )
            assert assignment.group_assignments == ()

        assert group.snapshot().group_generation == 1
        assert group.snapshot().status == 'orphaned'

    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._pending_park_evidence() == ()
        assert _parked_group_rows(state) == [('test', 'remote', 2)]


def test_healthy_breaker_decay_waits_for_empty_pending_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, _config = _source_only_config(tmp_path, monkeypatch)
    config_path.write_text(
        _replace_config_text(
            config_path.read_text(encoding='utf-8'),
            '[sites.test.workers.pool]',
            '[sites.test.workers.pool]\nhealthy_reset_secs = 0.1',
        ),
        encoding='utf-8',
    )
    config = load_config(config_path)
    evidence = _park_evidence()
    clock = _FakeClock()
    dependent_spec = next(
        spec for spec in build_worker_specs(config.sites['test']) if not spec.sources
    )
    policy = config.sites['test'].workers['pool']
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
        breaker = state._restore_worker_breaker(
            worker_id=dependent_spec.worker_id,
            restart_limit=policy.restart_limit,
            restart_window_secs=policy.restart_window_secs,
            healthy_reset_secs=policy.healthy_reset_secs,
            clock=clock,
        )
        assert breaker._record_failure('previous-unattributed-failure') == 1.0
        assert breaker._mark_restart_started()
        state._persist_worker_breaker(breaker)
    _create_recovery_group_database(config)
    factory = _ScriptedWorkerFactory()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'identity-{token_index}'

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        clock.advance(0.2)
        dependent._messages.append(_health_update(dependent, heartbeat_at=clock()))

        supervisor._tick()

        current_breaker = cast(
            'dict[str, _WorkerBreaker]',
            vars(supervisor)['_breakers'],
        )[dependent._worker_id]
        assert len(current_breaker._failure_times) == 1
        dependent._messages.append(_recovery_ready(dependent, evidence))

        supervisor._tick()

        assert len(current_breaker._failure_times) == 0


def test_operator_batch_exhaustion_does_not_extend_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    monkeypatch.setattr(time, 'monotonic', clock)
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory._instances.values() if item._spec.groups)
        worker._operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={},
        )

        def slow_empty_batch() -> Iterator[ControlMessage]:
            clock.advance(2)
            yield from ()

        monkeypatch.setattr(worker, '_receive_available', slow_empty_batch)
        command = OperatorCommand(
            request_id='batch-timeout',
            command='park',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=456),
        )
        with pytest.raises(TimeoutError, match='response timed out'):
            supervisor.dispatch_operator_command(command, timeout=1)
        assert clock() == 2
        assert worker._killed


def test_operator_retry_ignores_result_from_prior_transport_attempt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='caller-retry-1',
        command='park',
        arguments={
            'site': 'test',
            'group': 'remote',
            'reason': 'maintenance',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    revisions = iter((8, 9))

    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        owner = next(
            worker for worker in factory._instances.values() if 'remote' in worker._spec.groups
        )
        owner._operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'revision': next(revisions), 'status': 'parked'},
        )

        first = supervisor.dispatch_operator_command(command, timeout=1.0)
        owner._messages.append(
            CommandResult(
                command=command.command,
                ok=True,
                message='late',
                request_id=owner._sent_commands[0].request_id,
                result={'revision': 8, 'status': 'parked'},
            )
        )
        second = supervisor.dispatch_operator_command(command, timeout=1.0)

    assert owner._sent_commands[0].request_id != owner._sent_commands[1].request_id
    assert first.request_id == command.request_id
    assert second.request_id == command.request_id
    assert second.result == {'revision': 9, 'status': 'parked'}


def test_operator_waits_for_dependent_grant_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    command = OperatorCommand(
        request_id='dependent-grant-operator-1',
        command='adopt-source-stream',
        arguments={
            'site': 'test',
            'source': 'admin',
            'reason': 'rotate source identity',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        grant = source._sent_grants[-1]
        ready = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=grant.dependent_spawn_instance,
        )
        source._messages.append(ready)
        queued = tuple(source._messages)

        with pytest.raises(
            ValueError,
            match='operator command does not have one running owner',
        ):
            supervisor.dispatch_operator_command(command, timeout=1.0)

        assert tuple(source._messages) == queued
        assert source._sent_commands == []
        for _ in range(2):
            supervisor._tick()
            if supervisor._spawns[dependent._worker_id]._lifecycle is None:
                break
        else:
            pytest.fail('dependent grant lifecycle did not complete')

        source._operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'source': 'admin', 'status': 'running'},
        )
        result = supervisor.dispatch_operator_command(command, timeout=1.0)

    assert result.request_id == command.request_id
    assert result.result == {'source': 'admin', 'status': 'running'}
    assert len(source._sent_commands) == 1


def test_operator_waits_for_worker_lifecycle_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    _create_recovery_group_database(config)
    factory = _ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='worker-lifecycle-operator-1',
        command='park',
        arguments={
            'site': 'test',
            'group': 'remote',
            'reason': 'maintenance',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()
        owner = next(
            worker for worker in factory._instances.values() if 'remote' in worker._spec.groups
        )
        assert supervisor._spawns[owner._worker_id]._lifecycle is not None
        owner._messages.append(_recovery_ready(owner, evidence))
        queued = tuple(owner._messages)

        with pytest.raises(
            ValueError,
            match='operator command does not have one running owner',
        ):
            supervisor.dispatch_operator_command(command, timeout=1.0)

        assert tuple(owner._messages) == queued
        assert owner._sent_commands == []


def test_pending_evidence_follows_group_across_default_named_and_dedicated_packing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    original_config = config_path.read_text(encoding='utf-8')
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    _create_recovery_group_database(config)

    for packing in ('default', 'pool', 'dedicated'):
        config_path.write_text(
            _replace_config_text(original_config, 'worker = "pool"', f'worker = "{packing}"'),
            encoding='utf-8',
        )
        packed_config = load_config(config_path)
        factory = _ScriptedWorkerFactory()
        token_index = 0

        def token(*, prefix: str = packing) -> str:
            nonlocal token_index
            token_index += 1
            return f'{prefix}-identity-{token_index}'

        with Supervisor(
            config=packed_config,
            config_path=config_path,
            worker_factory=factory,
            token_factory=token,
        ) as supervisor:
            supervisor._reconcile(
                generations={('test', 'remote'): 1},
            )
            supervisor.start()
            owner = next(
                worker
                for worker in factory._instances.values()
                if worker._assignment is not None
                and any(item.group == 'remote' for item in worker._assignment.group_assignments)
            )
            assert owner._assignment is not None
            delivery = next(
                item for item in owner._assignment.group_assignments if item.group == 'remote'
            )
            assert isinstance(delivery, DeliveryGroupAssignment)
            assert delivery.conditional_park == _park_directive(evidence)


def test_pending_provider_publication_accepts_source_transition_after_worker_sample(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    clock = _FakeClock()
    clock.advance(100)
    monkeypatch.setattr(time, 'monotonic', clock)
    publications = _HealthPublications(monkeypatch)
    factory = _ScriptedWorkerFactory()
    first_socket, second_socket = socket.socketpair()
    sender = WorkerControl(first_socket, max_frame_bytes=131072, send_deadline_secs=1)
    receiver = WorkerControl(second_socket, max_frame_bytes=131072, send_deadline_secs=1)
    with (
        closing(sender),
        closing(receiver),
        Supervisor(
            config=config, config_path=config_path, clock=clock, worker_factory=factory
        ) as supervisor,
    ):
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory._instances.values() if item._spec.sources)
        dependent = next(item for item in factory._instances.values() if item._spec.groups)
        assert not supervisor._accept_worker_health(
            dependent, _health_update(dependent, heartbeat_at=clock())
        )
        assert worker._assignment is not None
        runtime = worker_module.build_worker_runtime(
            config, worker._spec, worker._assignment, sender
        )
        runtime._construct_components()
        source = runtime._components['source:admin']
        assert isinstance(source, worker_module._SourceComponent)
        cache = runtime._health_cache
        assert cache is not None
        cache._publish_source(
            _idle_source_health(worker, source='admin', heartbeat_at=clock()),
            sample=source._watchdog.sample(),
        )
        current_health = runtime._current_health

        def transition_after_sample(now: float) -> WorkerHealth:
            health = current_health(now)
            clock.advance(0.001)
            source._watchdog.transition('provider-query', deadline_at=110)
            clock.advance(0.001)
            return health

        monkeypatch.setattr(runtime, '_current_health', transition_after_sample)
        runtime._publish_monitor_health(clock())
        update = worker_tests._receive_type(receiver, HealthUpdate)
        watch = next(item for item in update.watchdogs if item.kind == 'source')
        published_at = publications._published_at(update)
        assert update.worker.heartbeat_at == 100
        assert watch.heartbeat_at == 100.001
        assert watch.phase == 'provider-query'
        assert update.worker.heartbeat_at < watch.heartbeat_at < published_at
        assert published_at == clock()
        assert watch.deadline_at is not None
        assert published_at < watch.deadline_at
        _assert_pending_provider_publication(supervisor, worker, update, published_at=published_at)
        # The same real frame is invalid only when evaluated at the old proxy.
        supervisor._clock = lambda: update.worker.heartbeat_at
        assert supervisor._accept_worker_health(worker, update)
        assert worker._killed
        assert supervisor._spawns[worker._worker_id]._termination_reason == 'source-phase-overdue'


def test_post_adoption_startup_halts_only_the_affected_group(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _two_dependent_config(tmp_path, monkeypatch)
    identity = AuditIdentity.current()
    remote_path = tmp_path / 'post-adoption-source.db'
    second_path = remote_path
    with _lifecycle_source(
        tmp_path / 'post-adoption-source.db',
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        with _lifecycle_group(remote_path) as remote:
            _begin_readd_disposition(source, remote, 'reset-to-latest')
        with _lifecycle_group(second_path, group='second') as second:
            second.park(
                expected_revision=second.snapshot().revision,
                reason='test adoption park',
                identity=identity,
                wall_clock_time=10.0,
            )

        _adopt_lifecycle_stream(
            source,
            adoption_id='adoption-startup',
            command_fingerprint='adoption-startup-fingerprint',
            reason='permission role changed',
        )
        source.initialize(200)
        with (
            _lifecycle_group(
                remote_path,
                generation=2,
                verified_incarnation='adoption-startup',
            ) as remote,
            _lifecycle_group(
                second_path,
                group='second',
                verified_incarnation='adoption-startup',
            ) as second,
            Supervisor(config=config, config_path=config_path) as supervisor,
        ):
            second.dispose(
                DispositionRequest(
                    command='reset-to-latest',
                    group='second',
                    disposition_id='second-new-stream',
                    reserved_generation=2,
                    expected_revision=second.snapshot().revision,
                    from_event_id=None,
                    acknowledge_truncation=False,
                    reason='test new stream reset',
                    identity=identity,
                    wall_clock_time=11.0,
                )
            )

            supervisor._reconcile(
                generations={
                    ('test', 'remote'): 2,
                    ('test', 'second'): 2,
                },
                source_states={('test', 'admin'): source},
                group_states={
                    ('test', 'remote'): remote,
                    ('test', 'second'): second,
                },
            )

            assert remote.snapshot().status == 'stream-identity-changed'
            terminal = second.terminal_disposition_identity(2)
            assert terminal is not None
            assert source.has_terminal_disposition('second', *terminal)
            assert second.snapshot().verified_incarnation == 'adoption-startup'
            assert second.snapshot().status == 'parked'


def test_probe_snapshot_parked_group_failure_is_visible_without_blocking_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        source_update = _health_update(source, heartbeat_at=0.0)
        dependent_update = _health_update(dependent, heartbeat_at=0.0)
        parked_failure = replace(
            dependent_update.groups[0],
            parked=True,
            park_reason='operator',
            failure_kind='callback',
            failure_text='failed',
            join_phase='parked',
            conditions=('parked', 'callback-failed'),
        )
        updates = {
            source._worker_id: source_update,
            dependent._worker_id: replace(dependent_update, groups=(parked_failure,)),
        }
        for worker_id, update in updates.items():
            supervisor._spawns[worker_id]._health = update
            supervisor._spawns[worker_id]._health_inventory_complete = True
        build_snapshot = cast(
            'Callable[[float], ProbeSnapshot]',
            vars(Supervisor)['_probe_snapshot'].__get__(supervisor),
        )

        snapshot = build_snapshot(0.0)

    assert snapshot.complete
    assert snapshot.ready
    assert 'parked' in snapshot.conditions
    assert 'callback-failed' in snapshot.conditions


def test_recovery_timeout_retains_evidence_and_counts_an_unattributed_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    _create_recovery_group_database(config)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'identity-{token_index}'

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        clock.advance(config.daemon.worker_startup_deadline_secs)
        source._messages.append(_health_update(source, heartbeat_at=clock()))

        supervisor._tick()

        assert dependent._killed
        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == (evidence,)
            policy = config.sites['test'].workers['pool']
            breaker = state._restore_worker_breaker(
                worker_id=dependent._worker_id,
                restart_limit=policy.restart_limit,
                restart_window_secs=policy.restart_window_secs,
                healthy_reset_secs=policy.healthy_reset_secs,
                clock=clock,
            )
            assert len(breaker._failure_times) == 1


def test_registration_failure_shuts_down_the_spawned_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    spawned = False
    failed = False

    def clock() -> float:
        nonlocal failed
        if spawned and not failed:
            failed = True
            raise RuntimeError('registration clock failed')
        return 0.0

    def spawn(**options: Unpack[_ScriptedWorkerOptions]) -> _ScriptedWorker:
        nonlocal spawned
        worker = factory(**options)
        spawned = True
        return worker

    with Supervisor(
        config=config, config_path=config_path, worker_factory=spawn, clock=clock
    ) as supervisor:
        supervisor._reconcile(generations={})
        with pytest.raises(RuntimeError, match='registration clock failed'):
            supervisor.start()

        worker = next(iter(factory._instances.values()))
        assert worker._close_calls == 1
        assert sum(operation.startswith('shutdown:') for operation in factory._operations) == 1
        assert not supervisor._spawns


def test_repeated_startup_readd_uses_inventory_high_water_for_each_transition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    path, _ = _prepare_absent_orphan(config)
    omitted = replace(config, sites={'test': replace(config.sites['test'], groups={})})
    for expected, current_config in ((2, config), (2, omitted), (3, config)):
        with Supervisor(config=current_config, config_path=config_path) as supervisor:
            supervisor.reconcile_durable_state()
            assert supervisor._state is not None
            assert supervisor._state._generation_high_water('test', 'remote') == expected
        with closing(sqlite3.connect(path)) as connection, connection:
            assert connection.execute('SELECT group_generation FROM groups').fetchone() == (1,)
    with closing(sqlite3.connect(path)) as connection, connection:
        assert connection.execute(
            "SELECT command, disposition_id FROM audit WHERE command IN ('orphan', 'readd') "
            'ORDER BY audit_id'
        ).fetchall() == [('orphan', '1'), ('readd', '2'), ('orphan', '2'), ('readd', '3')]
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor.reconcile_durable_state()
        assert supervisor._reconciled is not None
        assignment = supervisor._reconciled[('test', 'remote')]
        assert isinstance(assignment, PendingDispositionSourceGroupAssignment)
        assert (assignment.orphan_generation, assignment.reserved_generation) == (1, 3)


def test_reserved_generation_is_not_active_before_group_full_commit(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    _prepare_readd_inventory(state_dir)
    with _SupervisorState._open(state_dir) as state:
        assignments = state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 2},
        )
        assert assignments[('test', 'remote')] == (
            PendingDispositionSourceGroupAssignment(
                source='admin',
                group='remote',
                orphan_generation=1,
                reserved_generation=2,
            )
        )
        with pytest.raises(RuntimeError, match='durable generation exceeds reserved'):
            state._reconcile_groups(
                configured={('test', 'remote'): 'admin'}, generations={('test', 'remote'): 3}
            )


def test_shutdown_clears_every_per_worker_collection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    with Supervisor(config=config, config_path=config_path, worker_factory=factory) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(iter(factory._instances.values()))
        supervisor._spawns[worker._worker_id]._attributed_park_failure = True
        close = worker._close

        def fail_close() -> None:
            close()
            raise OSError('worker close failed')

        monkeypatch.setattr(worker, '_close', fail_close)

        with pytest.raises(OSError, match='worker close failed'):
            supervisor._shutdown_workers('test shutdown')

        assert worker._close_calls == 1
        assert not supervisor._spawns


def test_source_adoption_reports_dependent_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    monkeypatch.setattr(supervisor_module.time, 'sleep', clock.advance)
    command = _source_adoption_command('adopt-dependent-failure')
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_owner = next(
            worker for worker in factory._instances.values() if worker._spec.sources
        )
        group_owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        source_owner._operator_handler = _source_adoption_result
        group_owner._operator_handler = _source_adoption_result

        def spawn_with_dependent_failure(
            **options: Unpack[_ScriptedWorkerOptions],
        ) -> _ScriptedWorker:
            factory._authentication_ready = bool(options['spec'].sources)
            return factory(**options)

        vars(supervisor)['_worker_factory'] = spawn_with_dependent_failure

        result = supervisor.adopt_source_stream_command(command, timeout=1.0)

        source_replacement = factory._instances[source_owner._worker_id]
        group_replacement = factory._instances[group_owner._worker_id]

    assert source_replacement is not source_owner
    assert source_replacement._assignment is not None
    assert group_replacement is not group_owner
    assert group_replacement._assignment is None
    assert not result.ok
    assert group_owner._worker_id in result.message


def test_source_endpoint_readiness_admits_remote_dependent_during_local_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _local_and_remote_config(tmp_path, monkeypatch)
    evidence = _park_evidence(group='local')
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    _create_recovery_group_database(config, group='local')
    _create_recovery_group_database(config, group='remote')
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'identity-{token_index}'

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        supervisor._reconcile(
            generations={
                ('test', 'local'): 1,
                ('test', 'remote'): 1,
            },
        )
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        source._authentication_ready = True
        supervisor._tick()
        remote = next(worker for worker in factory._instances.values() if not worker._spec.sources)
        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == (evidence,)
        assert source._assignment is not None
        local = next(
            item for item in source._assignment.group_assignments if item.group == 'local'
        )
        assert isinstance(local, DeliveryGroupAssignment)
        assert local.conditional_park == _park_directive(evidence)
        source._messages_before_grant.append(
            _health_update(
                source,
                heartbeat_at=0.0,
                groups=(
                    replace(
                        _in_flight_group_health(source, heartbeat_at=0.0),
                        group='local',
                    ),
                ),
            )
        )
        remote._authentication_ready = True

        supervisor._tick()
        supervisor._tick()

        assert not source._killed
        assert not remote._killed
        assert remote._assignment is not None
        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == (evidence,)

        source._messages.append(_recovery_ready(source, evidence))
        supervisor._tick()

        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == ()


@pytest.mark.parametrize('heartbeat', [None, 0.0, 0.5])
def test_source_readiness_preserves_current_spawn_health_and_startup_deadline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, heartbeat: float | None
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(iter(factory._instances.values()))
        original_deadline = supervisor._spawns[source._worker_id]._health_deadline
        source._authentication_ready = True
        assign = source._assign
        accepted: list[HealthUpdate] = []

        def assign_with_health(assignment: WorkerAssignment) -> None:
            assign(assignment)
            if heartbeat is not None:
                update = _health_update(source, heartbeat_at=heartbeat)
                source._messages.appendleft(update)
                accepted.append(update)

        monkeypatch.setattr(source, '_assign', assign_with_health)
        clock._now = 0.5
        supervisor._advance_worker_messages()
        assert source._assignment is not None
        assert not source._messages
        if heartbeat is None:
            assert not source._killed
            assert supervisor._spawns[source._worker_id]._health_deadline == original_deadline
        else:
            assert len(accepted) == 1
            assert _spawn_health(supervisor, source._worker_id).worker.heartbeat_at == heartbeat
            assert supervisor._spawns[source._worker_id]._health_deadline == heartbeat + 0.25
            assert source._killed is (heartbeat == 0)


@pytest.mark.parametrize('expire', [False, True])
def test_source_recovery_restart_uses_finite_health_acquisition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, expire: bool
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    monkeypatch.setattr(time, 'sleep', clock.advance)
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        originals = tuple(factory._instances.values())
        counts = {worker._worker_id: worker._batch_acquisitions for worker in originals}
        assign = _ScriptedWorker._assign
        injections: list[_ScriptedWorker] = []

        def assign_with_history(worker: _ScriptedWorker, assignment: WorkerAssignment) -> None:
            assign(worker, assignment)
            if worker._spec.sources:
                injections.append(worker)
                old = _health_update(worker, heartbeat_at=0)
                fresh = _health_update(worker, heartbeat_at=0.5)
                worker._messages.extendleft((fresh, old))
                clock._now = config.daemon.worker_startup_deadline_secs if expire else 0.5

        monkeypatch.setattr(_ScriptedWorker, '_assign', assign_with_history)
        result = _source_adoption_result(_source_adoption_command('review-restart'))
        actual = supervisor._restart_source_owners('test', 'admin', ('remote',), result)
        assert len(injections) == 1
        replacement = injections[0]
        assert replacement._batch_acquisitions > 0
        assert all(worker._batch_acquisitions == counts[worker._worker_id] for worker in originals)
        assert all(not worker._killed for worker in originals)
        assert all(breaker._failure_total == 0 for breaker in supervisor._breakers.values())
        if expire:
            assert not actual.ok
            assert 'worker-startup-timeout' in actual.message
            assert replacement._killed
        else:
            assert actual == result
            assert not replacement._killed
            assert _spawn_health(supervisor, replacement._worker_id).worker.heartbeat_at == 0.5
            assert not replacement._messages


@pytest.mark.parametrize('worker_mode', ['default', 'pool', 'dedicated'])
def test_automatic_live_replacement_reconciles_completed_readd(
    worker_mode: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, loaded = _source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    group_config = replace(site.groups['remote'], worker=worker_mode)
    config = replace(
        loaded,
        sites={'test': replace(site, groups={'remote': group_config})},
    )
    _prepare_readd_inventory(config.daemon.state_dir)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'readd-replacement-{token_index}'

    configured_group_path = source_database(config.daemon.state_dir)
    group_path = tmp_path / 'readd-group.db'
    configured_group_path.symlink_to(group_path)
    with (
        _lifecycle_source(
            group_path,
        ) as source,
        _lifecycle_group(group_path) as group,
        Supervisor(
            config=config,
            config_path=config_path,
            clock=clock,
            worker_factory=factory,
            token_factory=token,
        ) as supervisor,
    ):
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()
        initial_assignments = tuple(
            worker._assignment
            for workers in factory._history.values()
            for worker in workers
            if worker._assignment is not None
        )
        assert any(
            isinstance(assignment, PendingDispositionSourceGroupAssignment)
            for worker_assignment in initial_assignments
            for assignment in worker_assignment.source_group_assignments
        )
        assert all(
            not worker_assignment.group_assignments for worker_assignment in initial_assignments
        )

        _complete_readd_disposition(source, group)
        state = cast('_SupervisorState', vars(supervisor)['_state'])
        assert state._promote_readded_group(
            site='test',
            group='remote',
            source_state=source,
            group_state=group,
        )
        source_spec = build_worker_specs(config.sites['test'])[0]
        owner_spec = next(
            spec for spec in build_worker_specs(config.sites['test']) if 'remote' in spec.groups
        )
        source_worker = factory._instances[source_spec.worker_id]
        source_history_count = len(factory._history[source_spec.worker_id])
        owner_history_count = len(factory._history[owner_spec.worker_id])
        operations_before_exit = len(factory._operations)
        source_worker._exit_status = 1
        for _ in range(10):
            clock.advance(1.0)
            supervisor._tick()
            if (
                len(factory._history[source_spec.worker_id]) > source_history_count
                and len(factory._history[owner_spec.worker_id]) > owner_history_count
                and factory._history[owner_spec.worker_id][-1]._assignment is not None
            ):
                break
        else:
            pytest.fail('automatic replacement did not become ready')

        replacement = factory._history[owner_spec.worker_id][-1]
        assert replacement is not factory._history[owner_spec.worker_id][0]
        assert replacement._assignment is not None
        assert replacement._assignment.group_assignments == (
            DeliveryGroupAssignment(
                source='admin',
                group='remote',
                group_generation=2,
                start_mode='resume',
            ),
        )
        source_replacement = factory._history[source_spec.worker_id][-1]
        assert source_replacement._assignment is not None
        assert source_replacement._assignment.source_group_assignments == (
            ActiveSourceGroupAssignment(
                source='admin',
                group='remote',
                group_generation=2,
                start_mode='resume',
            ),
        )
        replacement_operations = factory._operations[operations_before_exit:]
        if owner_spec.worker_id != source_spec.worker_id:
            assert replacement_operations.index('spawn:site') < replacement_operations.index(
                f'spawn:{owner_spec.kind.value}'
            )
            assert replacement_operations.index('grant:remote') < replacement_operations.index(
                f'assign:{owner_spec.kind.value}'
            )
        else:
            assert replacement_operations.count('spawn:site') == 1
        assert all(
            worker._assignment is not None
            for workers in factory._history.values()
            for worker in workers
        )


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_completed_readd_restarts_as_delivery_resume_without_a_second_live_assignment(
    command: Literal['replay', 'reset-to-latest'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    _prepare_readd_inventory(config.daemon.state_dir)
    factory = _ScriptedWorkerFactory()
    configured_group_path = source_database(config.daemon.state_dir)
    group_path = tmp_path / 'group.db'
    configured_group_path.symlink_to(group_path)
    with (
        _lifecycle_source(
            group_path,
        ) as source,
        _lifecycle_group(group_path) as group,
    ):
        source.initialize(100, incarnation='incarnation-1')
        _begin_readd_disposition(source, group, command)

        with Supervisor(
            config=config,
            config_path=config_path,
            worker_factory=factory,
        ) as supervisor:
            supervisor._reconcile(
                generations={('test', 'remote'): 2},
                source_states={('test', 'admin'): source},
                group_states={('test', 'remote'): group},
            )
            supervisor.start()

    assignments = tuple(
        worker._assignment
        for workers in factory._history.values()
        for worker in workers
        if worker._assignment is not None
    )
    group_owners = tuple(
        (worker_assignment, group_assignment)
        for worker_assignment in assignments
        for group_assignment in worker_assignment.group_assignments
        if group_assignment.group == 'remote'
    )
    assert len(group_owners) == 1
    owner_assignment, delivery = group_owners[0]
    assert delivery == DeliveryGroupAssignment(
        source='admin',
        group='remote',
        group_generation=2,
        start_mode='resume',
    )
    assert len(factory._history[owner_assignment.worker_id]) == 1
    assert (
        sum(
            source_assignment.group == 'remote'
            for worker_assignment in assignments
            for source_assignment in worker_assignment.source_group_assignments
        )
        == 1
    )


@pytest.mark.parametrize(
    ('advance_before_result', 'expected_outcome'),
    [(False, 'parked'), (True, 'stale')],
)
def test_lost_ready_after_park_and_stale_replays_the_recorded_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    advance_before_result: bool,
    expected_outcome: Literal['parked', 'stale'],
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    group_path = source_database(config.daemon.state_dir)
    with _conditional_group(group_path) as group:
        if advance_before_result:
            completed = group.callback_done(0, expected_revision=7)
            assert completed.revision == 8

    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'token-{token_index}'

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()
        owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        assert owner._assignment is not None
        first_ready = _resolve_lane_recovery(
            config,
            owner._assignment,
            group_path,
            drop_ready=True,
        )
        assert first_ready.outcome == expected_outcome
        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == (evidence,)

        owner._exit_status = 1
        supervisor._tick()
        clock.advance(1.0)
        supervisor._tick()
        supervisor._tick()

        replacement = factory._instances[owner._worker_id]
        assert replacement is not owner
        assert replacement._assignment is not None
        replayed_ready = _resolve_lane_recovery(
            config,
            replacement._assignment,
            group_path,
            drop_ready=False,
        )
        assert (
            replayed_ready.evidence_id,
            replayed_ready.outcome,
            replayed_ready.resulting_revision,
        ) == (
            first_ready.evidence_id,
            first_ready.outcome,
            first_ready.resulting_revision,
        )

        replacement._messages.append(replayed_ready)
        supervisor._tick()

        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == ()


@pytest.mark.parametrize('delay_before_observation', [False, True])
def test_pending_provider_publication_survives_consumer_scheduling_delay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, delay_before_observation: bool
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory._instances.values() if item._spec.groups)
        update = _health_update(worker, heartbeat_at=clock())
        published_at = clock()
        accept = supervisor._accept_worker_health

        def delayed_accept(current: _ScriptedWorker, message: HealthUpdate) -> bool:
            contained = accept(current, message)
            clock.advance(1)
            return contained

        monkeypatch.setattr(supervisor, '_accept_worker_health', delayed_accept)
        monkeypatch.setattr(time, 'monotonic', clock)
        if delay_before_observation:
            clock.advance(1)
        _assert_pending_provider_publication(supervisor, worker, update, published_at=published_at)
        assert clock() == (2 if delay_before_observation else 1)
        assert supervisor._clock() == clock()


@pytest.mark.parametrize('failure', ['worker-heartbeat-overdue', 'source-phase-overdue'])
def test_pending_provider_publication_rejects_dead_monitor_and_overdue_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory._instances.values() if item._spec.sources)
        clock.advance(1)
        update = _health_update(worker, heartbeat_at=clock())
        if failure == 'worker-heartbeat-overdue':
            update = replace(update, worker=replace(update.worker, monitor_alive=False))
        else:
            source = replace(
                update.sources[0], phase='provider-query', heartbeat_at=0, deadline_at=1
            )
            update = replace(
                update,
                sources=(source,),
                watchdogs=tuple(
                    replace(watch, phase='provider-query', heartbeat_at=0, deadline_at=1)
                    if watch.kind == 'source'
                    else watch
                    for watch in update.watchdogs
                ),
            )
        with pytest.raises(AssertionError):
            _assert_pending_provider_publication(supervisor, worker, update, published_at=clock())
        assert worker._killed
        assert supervisor._spawns[worker._worker_id]._termination_reason == failure


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_readd_source_commit_reconciles_inventory_idempotently(
    command: Literal['replay', 'reset-to-latest'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    _prepare_readd_inventory(config.daemon.state_dir)
    source_path = source_database(config.daemon.state_dir)
    group_path = source_path
    with (
        _lifecycle_source(
            source_path,
        ) as source,
        _lifecycle_group(group_path) as group,
    ):
        source.initialize(100, incarnation='incarnation-1')
        _begin_readd_disposition(source, group, command)

    with (
        Supervisor(config=config, config_path=config_path) as supervisor,
        _lifecycle_source(
            source_path,
        ) as source,
        _lifecycle_group(group_path, generation=2) as group,
    ):
        supervisor._reconcile(
            generations={('test', 'remote'): 2},
            source_states={('test', 'admin'): source},
            group_states={('test', 'remote'): group},
        )
        build_assignment = Supervisor._build_worker_assignment
        first = build_assignment(
            supervisor,
            build_worker_specs(config.sites['test'])[1],
            'dependent-spawn-1',
            (),
            (),
            (),
        )
        second = build_assignment(
            supervisor,
            build_worker_specs(config.sites['test'])[1],
            'dependent-spawn-2',
            (),
            (),
            (),
        )

    assert first.group_assignments == (
        DeliveryGroupAssignment(
            source='admin',
            group='remote',
            group_generation=2,
            start_mode='resume',
        ),
    )
    assert second.group_assignments == first.group_assignments


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_readd_reconciliation_requires_atomic_disposition_proof(
    command: Literal['replay', 'reset-to-latest'],
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    _prepare_readd_inventory(state_dir)
    with (
        _lifecycle_source(
            tmp_path / 'source.db',
        ) as source,
        _lifecycle_group(tmp_path / 'source.db') as group,
        _SupervisorState._open(state_dir) as state,
    ):
        source.initialize(100, incarnation='incarnation-1')
        if command == 'replay':
            open_source_range(source, 101, 101, successful_clock=1.0)
        Supervisor._orphan_group(source, group)
        assert not state._promote_readded_group(
            site='test',
            group='remote',
            source_state=source,
            group_state=group,
        )
        _begin_readd_disposition(source, group, command)
        assert state._promote_readded_group(
            site='test',
            group='remote',
            source_state=source,
            group_state=group,
        )
        assert state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 2},
        ) == {
            ('test', 'remote'): ActiveSourceGroupAssignment(
                source='admin',
                group='remote',
                group_generation=2,
                start_mode='resume',
            )
        }


@pytest.mark.parametrize('worker_mode', ['default', 'pool', 'dedicated'])
def test_readded_group_has_no_lane_and_one_source_assignment(
    worker_mode: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, loaded = _source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    group = replace(site.groups['remote'], worker=worker_mode)
    config = replace(
        loaded,
        sites={'test': replace(site, groups={'remote': group})},
    )
    with _SupervisorState._open(config.daemon.state_dir) as state:
        state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={},
        )
        state._reconcile_groups(
            configured={},
            generations={('test', 'remote'): 1},
        )

    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        build_assignment = Supervisor._build_worker_assignment
        assignments = tuple(
            build_assignment(
                supervisor,
                spec,
                f'spawn-{spec.kind.value}',
                (('admin', 'source-1'),) if spec.sources else (),
                (),
                (),
            )
            for spec in build_worker_specs(config.sites['test'])
        )

    group_assignments = tuple(
        assignment
        for worker_assignment in assignments
        for assignment in worker_assignment.group_assignments
    )
    source_assignments = tuple(
        assignment
        for worker_assignment in assignments
        for assignment in worker_assignment.source_group_assignments
    )
    assert group_assignments == ()
    assert source_assignments == (
        PendingDispositionSourceGroupAssignment(
            source='admin',
            group='remote',
            orphan_generation=1,
            reserved_generation=2,
        ),
    )


def test_startup_batch_receive_cannot_complete_after_absolute_deadline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(iter(factory._instances.values()))
        source._authentication_ready = True
        receive = source._receive
        observed: list[ControlMessage] = []

        def slow_receive(*, timeout: float | None = None) -> ControlMessage:
            message = receive(timeout=timeout)
            observed.append(message)
            clock.advance(config.daemon.worker_startup_deadline_secs)
            return message

        monkeypatch.setattr(source, '_receive', slow_receive)
        supervisor._tick()
        assert len(observed) == 1
        assert isinstance(observed[0], SourceEndpointReady)
        assert source._killed
        assert not supervisor._source_workers
        assert len(factory._instances) == 1


def test_startup_error_is_not_replaced_by_shutdown_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    startup_error = ValueError('startup lifecycle failed')
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=iter(('source-generation-1', 'source-spawn-1')).__next__,
    )

    def fail_lifecycle_advance(_now: float) -> bool:
        source = next(iter(factory._instances.values()))
        source._shutdown_status = None
        raise startup_error

    monkeypatch.setattr(supervisor, '_advance_lifecycles', fail_lifecycle_advance)
    try:
        supervisor._open()
        supervisor._reconcile(generations={})

        with pytest.raises(ValueError, match='startup lifecycle failed') as caught:
            supervisor.start()

        source = next(iter(factory._instances.values()))
        assert caught.value is startup_error
        assert caught.value.__notes__ == [f'worker child remained unreaped: {source._worker_id}']
    finally:
        supervisor._close()


def test_startup_readd_checks_incarnation_before_lifecycle_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    source_path, _ = _prepare_absent_orphan(config)
    with (
        Supervisor(config=config, config_path=config_path) as supervisor,
        _lifecycle_source(source_path) as source,
        _lifecycle_group(source_path) as group,
    ):
        with closing(sqlite3.connect(source_path)) as connection, connection:
            connection.execute("UPDATE source_meta SET stream_incarnation = 'different-stream'")
            before = tuple(connection.iterdump())
        with pytest.raises(RuntimeError, match='incarnation conflicts'):
            supervisor._reconcile(
                generations={('test', 'remote'): 1},
                source_states={('test', 'admin'): source},
                group_states={('test', 'remote'): group},
            )
        with closing(sqlite3.connect(source_path)) as connection, connection:
            assert tuple(connection.iterdump()) == before


def test_status_before_first_snapshot_is_stale_and_unready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()

        status = supervisor.status_mapping({})

    assert status['readiness'] is False
    assert status['conditions'] == ('stale',)
    assert status['primary_state'] == 'stale'


def test_status_budget_projects_complete_records_deterministically() -> None:
    snapshot = _maximum_status_snapshot()
    reversed_snapshot = _maximum_status_snapshot(reverse=True)

    status = _project_status(snapshot)
    reversed_status = _project_status(reversed_snapshot)

    assert encode_json_body(status) == encode_json_body(reversed_status)
    assert len(encode_json_body(status)) <= supervisor_module._STATUS_RESULT_MAX_BYTES
    counts = cast('Mapping[str, Mapping[str, int]]', status['projection_counts'])
    details = {
        name: cast('tuple[object, ...]', status[name])
        for name in (
            'sources',
            'groups',
            'workers',
            'database_sizes',
            'breaker_history',
        )
    }
    complete_details = {
        name: cast('tuple[object, ...]', snapshot.to_mapping()[name]) for name in details
    }
    assert set(counts) == set(details)
    for name, records in details.items():
        assert counts[name] == {'total': 100, 'included': len(records)}
        assert all(record in complete_details[name] for record in records)
    assert any(counts[name]['included'] < 100 for name in counts)
    assert status['projection_truncated'] is True
    for name in (
        'liveness',
        'readiness',
        'primary_state',
        'conditions',
        'audit_count',
        'breaker_transition_count',
    ):
        assert status[name] == snapshot.to_mapping()[name]


def test_status_has_one_size_entry_per_source_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_owner = next(
            worker for worker in factory._instances.values() if worker._spec.sources
        )
        group_owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        source_owner._messages.append(
            _health_update(
                source_owner,
                heartbeat_at=clock(),
                sources=(
                    SourceHealth(
                        site='test',
                        source='admin',
                        source_generation='source-generation-1',
                        status='running',
                        frontier=100,
                        successful_clock=clock(),
                        retry_attempt=0,
                        next_retry_secs=None,
                        open_range_count=0,
                        late_id_count=0,
                        last_error=None,
                        worker_instance=source_owner._spawn_instance,
                        initialized=True,
                        stream_incarnation='incarnation-1',
                        phase='wait',
                        heartbeat_at=clock(),
                        deadline_at=clock() + 1.25,
                    ),
                ),
            )
        )
        group_owner._messages.append(
            _health_update(
                group_owner,
                heartbeat_at=clock(),
                groups=(
                    replace(
                        _in_flight_group_health(group_owner, heartbeat_at=clock()),
                        current_event_id=None,
                        current_plugin_id=None,
                        current_callback_id=None,
                        delivery_attempt=None,
                        phase='idle',
                    ),
                ),
            )
        )
        supervisor._tick()

        status = supervisor.status_mapping({'site': 'test', 'group': 'remote'})

    assert status['liveness'] is True
    assert status['readiness'] is True
    sources = cast('tuple[Mapping[str, object], ...]', status['sources'])
    groups = cast('tuple[Mapping[str, object], ...]', status['groups'])
    assert [source['source'] for source in sources] == ['admin']
    assert sources[0]['stream_incarnation'] == 'incarnation-1'
    assert [group['group'] for group in groups] == ['remote']
    assert status['projection_counts'] == {
        'sources': {'total': 1, 'included': 1},
        'groups': {'total': 1, 'included': 1},
        'workers': {'total': 2, 'included': 2},
        'database_sizes': {'total': 2, 'included': 2},
        'breaker_history': {'total': 0, 'included': 0},
    }
    assert {
        name for name, _size in cast('tuple[tuple[str, int], ...]', status['database_sizes'])
    } == {
        'supervisor.db',
        'sites/test/sources/admin.db',
    }
    assert all('database_size_bytes' not in group for group in groups)
    assert status['projection_truncated'] is False


def test_status_projection_filters_precede_totals_and_prioritize_selected_group() -> None:
    snapshot = _maximum_status_snapshot()
    payload = dict(snapshot.to_mapping())
    sources = cast('tuple[Mapping[str, object], ...]', payload['sources'])
    payload['sources'] = tuple(
        {**source, 'site': f'copy-{copy}-{source["site"]}'}
        for copy in range(2)
        for source in sources
    )
    budget_status = supervisor_module._budget_status_payload
    group_status = budget_status(
        payload,
        group_filter='group-050',
        site_filter=None,
    )
    site_status = _project_status(
        snapshot,
        site_filter='site-050',
    )

    group_counts = cast(
        'Mapping[str, Mapping[str, int]]',
        group_status['projection_counts'],
    )
    assert group_counts['sources']['total'] == 200
    assert group_counts['sources']['included'] < 200
    assert group_counts['groups'] == {'total': 1, 'included': 1}
    assert group_counts['database_sizes']['included'] > 0
    groups = cast('tuple[Mapping[str, object], ...]', group_status['groups'])
    assert [group['group'] for group in groups] == ['group-050']
    site_counts = cast(
        'Mapping[str, Mapping[str, int]]',
        site_status['projection_counts'],
    )
    assert site_counts['sources']['total'] == 1
    assert site_counts['groups']['total'] == 1
    assert site_counts['workers']['total'] == 1
    assert group_status['projection_truncated'] is True


def test_status_projection_ignores_worker_update_insertion_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    snapshot = _maximum_status_snapshot()
    updates = tuple(
        HealthUpdate(
            worker=worker,
            watchdogs=(),
            sources=(source,),
            groups=(group,),
            event_sockets=(),
        )
        for source, group, worker in zip(
            snapshot.sources,
            snapshot.groups,
            snapshot.workers,
            strict=True,
        )
    )
    first = Supervisor(config=config, config_path=config_path, clock=lambda: 100.0)
    second = Supervisor(config=config, config_path=config_path, clock=lambda: 100.0)
    template_spec = build_worker_specs(config.sites['test'])[0]
    for supervisor, ordered_updates in ((first, updates), (second, tuple(reversed(updates)))):
        for update in ordered_updates:
            worker = _ScriptedWorker(
                template_spec,
                update.worker.spawn_instance,
                [],
                None,
                authentication_ready=True,
            )
            supervisor._spawns[update.worker.worker_id] = supervisor_module._WorkerSpawn(
                _worker=worker,
                _health_deadline=101.0,
                _pending_groups=frozenset(),
                _lifecycle=None,
                _health=update,
            )

    with _SupervisorState._open(tmp_path / 'status-state') as state:
        vars(first)['_state'] = state
        vars(second)['_state'] = state
        first_status = first.status_mapping({})
        second_status = second.status_mapping({})

    assert encode_json_body(first_status) == encode_json_body(second_status)


def test_status_projection_reports_included_group_late_id_truncation() -> None:
    snapshot = _maximum_status_snapshot()
    payload = dict(snapshot.to_mapping())
    groups = cast('tuple[Mapping[str, object], ...]', payload['groups'])
    payload['sources'] = ()
    payload['groups'] = (groups[0],)
    payload['workers'] = ()
    payload['database_sizes'] = ()
    payload['breaker_history'] = ()
    budget_status = supervisor_module._budget_status_payload

    status = budget_status(payload, group_filter=None, site_filter=None)

    counts = cast('Mapping[str, Mapping[str, int]]', status['projection_counts'])
    assert all(item['included'] == item['total'] for item in counts.values())
    assert status['projection_truncated'] is True


@pytest.mark.parametrize(
    'failure_stage',
    [
        'reservation',
        'source-committed',
        'inventory-completed',
    ],
)
def test_supervisor_absent_orphan_disposition_retry_preserves_progress(
    failure_stage: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, group_path = _prepare_absent_orphan(config)
    command = OperatorCommand(
        request_id='reset-absent-retry',
        command='reset-group-to-latest',
        arguments={
            'site': 'test',
            'group': 'remote',
            'reason': 'retire absent group history',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    _inject_orphan_disposition_failure(monkeypatch, failure_stage)
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=_ScriptedWorkerFactory(),
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        with pytest.raises(TimeoutError, match=f'injected {failure_stage} failure'):
            supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        state = cast('_SupervisorState', vars(supervisor)['_state'])
        record = state._group_inventory_record(site='test', group='remote')
        assert record is not None
        assert record._generation == 2
        assert record._orphan_generation == (None if failure_stage == 'inventory-completed' else 1)
        generation = 1 if failure_stage == 'reservation' else 2
        with (
            _lifecycle_group(group_path, generation=generation) as group,
            _lifecycle_source(
                source_path,
            ) as source,
        ):
            assert group.snapshot().group_generation == generation
            terminal = group.terminal_disposition_identity(generation)
            assert (terminal is not None) == (failure_stage != 'reservation')

        # The durable inventory must still fence the source-file observation.
        for conflicting_record in (
            replace(record, _source='different-source'),
            replace(record, _generation=99, _orphan_generation=98),
        ):
            original_record = state._group_inventory_record
            observations = 0

            def read_record(
                *,
                site: str,
                group: str,
                conflicting: supervisor_module._InventoryRecord = conflicting_record,
                original: Callable[
                    ..., supervisor_module._InventoryRecord | None
                ] = original_record,
            ) -> supervisor_module._InventoryRecord | None:
                nonlocal observations
                observations += 1
                return conflicting if observations == 1 else original(site=site, group=group)

            with monkeypatch.context() as conflict:
                conflict.setattr(state, '_group_inventory_record', read_record)
                with pytest.raises(RuntimeError, match='inventory conflicts'):
                    supervisor.dispatch_group_disposition_command(command, timeout=1.0)
            assert state._group_inventory_record == original_record
        if failure_stage != 'reservation':
            for changed in (
                replace(
                    command,
                    identity=replace(command.identity, username='different-operator', pid=789),
                ),
                replace(command, identity=replace(command.identity, local_uid=502, pid=789)),
            ):
                with pytest.raises((InvalidTransitionError, ValueError), match='disposition'):
                    supervisor.dispatch_group_disposition_command(changed, timeout=1.0)
        result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert result.ok
        assert result.result['group_generation'] == 2
        completed = state._group_inventory_record(site='test', group='remote')
        assert completed is not None
        assert completed._generation == 2
        assert completed._orphan_generation is None
        repeated = supervisor.dispatch_group_disposition_command(
            replace(command, identity=replace(command.identity, pid=789)), timeout=1.0
        )
        assert repeated.result == result.result

    with (
        _lifecycle_group(group_path, generation=2) as group,
        _lifecycle_source(
            source_path,
        ) as source,
    ):
        terminal = group.terminal_disposition_identity(2)
        assert terminal is not None
        assert source.has_terminal_disposition('remote', *terminal)
        assert source.snapshot().replay_markers == ()
        assert group._database.connection.execute(
            "SELECT COUNT(*) FROM audit WHERE command = 'reset-to-latest'"
        ).fetchone() == (1,)


def test_supervisor_absent_orphan_terminal_retry_preserves_newer_reservation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, group_path = _prepare_absent_orphan(config)
    command = OperatorCommand(
        request_id='first-absent-reset',
        command='reset-group-to-latest',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'first reset'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    replacement = replace(
        command,
        request_id='next-absent-reset',
        arguments={**command.arguments, 'reason': 'next reset'},
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=_ScriptedWorkerFactory(),
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        first = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert first.result['group_generation'] == 2
        # Record the replacement generation's orphan evidence for the next reset.
        with (
            _lifecycle_group(group_path, generation=2) as group,
            _lifecycle_source(
                source_path,
            ) as source,
        ):
            Supervisor._orphan_group(source, group)
        _inject_orphan_disposition_failure(monkeypatch, 'reservation')
        with pytest.raises(TimeoutError, match='injected reservation failure'):
            supervisor.dispatch_group_disposition_command(replacement, timeout=1.0)
        state = cast('_SupervisorState', vars(supervisor)['_state'])
        reserved = state._group_inventory_record(site='test', group='remote')
        assert reserved is not None
        assert (reserved._orphan_generation, reserved._generation) == (2, 3)

        recorded = supervisor.dispatch_group_disposition_command(
            replace(command, identity=replace(command.identity, pid=789)), timeout=1.0
        )

        assert recorded.ok
        assert recorded.message == 'recorded'
        assert recorded.result['group_generation'] == 2
        assert state._group_inventory_record(site='test', group='remote') == reserved
        completed = supervisor.dispatch_group_disposition_command(replacement, timeout=1.0)
        assert completed.ok
        assert completed.result['group_generation'] == 3
        with pytest.raises((InvalidTransitionError, ValueError), match='disposition'):
            supervisor.dispatch_group_disposition_command(
                replace(command, identity=replace(command.identity, pid=789)), timeout=1.0
            )
        inventory = state._group_inventory_record(site='test', group='remote')
        assert inventory is not None
        assert (inventory._orphan_generation, inventory._generation) == (None, 3)
    with (
        _lifecycle_group(group_path, generation=3) as group,
        _lifecycle_source(
            source_path,
        ) as source,
    ):
        terminal = group.terminal_disposition_identity(3)
        assert terminal is not None
        assert source.has_terminal_disposition('remote', *terminal)
        assert source.snapshot().replay_markers == ()
        assert group._database.connection.execute(
            "SELECT COUNT(*) FROM audit WHERE command = 'reset-to-latest'"
        ).fetchone() == (2,)


@pytest.mark.parametrize('current_spawn', [True, False])
def test_supervisor_accepts_ready_only_from_authenticated_current_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    current_spawn: bool,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    _create_recovery_group_database(config)
    factory = _ScriptedWorkerFactory()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'identity-{token_index}'

    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        dependent._messages.append(
            _recovery_ready(
                dependent,
                evidence,
                spawn_instance=(dependent._spawn_instance if current_spawn else 'stale-spawn'),
            )
        )

        supervisor._tick()

        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == (() if current_spawn else (evidence,))
        assert dependent._killed is not current_spawn


def test_supervisor_active_dependent_grant_requires_current_source_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'active-owner-{token_index}'

    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        _source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        lifecycle = _spawn_lifecycle(supervisor, dependent._worker_id)
        assert vars(lifecycle)['_grant_sent']
        assert dependent._assignment is None
        source_workers = cast(
            'dict[tuple[str, str], _ScriptedWorker]',
            vars(supervisor)['_source_workers'],
        )
        source_workers.pop(('test', 'admin'))

        with pytest.raises(
            RuntimeError,
            match='active dependent grant source owner is unavailable',
        ):
            supervisor._tick()

        assert not dependent._killed
        assert dependent._assignment is None


def test_supervisor_advances_startups_independently_and_still_checkpoints(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _two_dependent_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    token_index = 0
    checkpoint_calls: list[object] = []

    def checkpoint(connection: object) -> bool:
        checkpoint_calls.append(connection)
        return True

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'token-{token_index}'

    def checkpointer_factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        clock: Callable[[], float],
    ) -> Checkpointer:
        def run_checkpoint() -> tuple[int, int, int]:
            checkpoint(connection)
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            clock=clock,
            run_checkpoint=run_checkpoint,
        )

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        checkpointer_factory=checkpointer_factory,
        worker_factory=factory,
        token_factory=token,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        source._authentication_ready = True
        supervisor._tick()
        dependents = sorted(
            (worker for worker in factory._instances.values() if not worker._spec.sources),
            key=lambda worker: worker._worker_id,
        )
        assert len(dependents) == 2
        dependents[0]._authentication_ready = True
        clock.advance(config.daemon.checkpoint_interval_secs)

        supervisor._tick()

        assert dependents[0]._assignment is not None
        assert dependents[1]._assignment is None
        assert len(checkpoint_calls) == 1
        assert all(
            timeout == 0.0
            for worker in factory._instances.values()
            for timeout in worker._receive_timeouts
        )

        clock.advance(config.daemon.worker_startup_deadline_secs)
        source._messages.append(_health_update(source, heartbeat_at=clock()))
        dependents[0]._messages.append(_health_update(dependents[0], heartbeat_at=clock()))
        supervisor._tick()

        assert not dependents[0]._killed
        assert dependents[1]._killed
    finally:
        supervisor._close()


def test_supervisor_ambiguous_overdue_worker_creates_no_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _two_group_shared_worker_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    )
    contain = supervisor._contain_current_worker
    observed_containment: list[tuple[str, set[str]]] = []

    def contain_current_worker(
        worker: _WorkerHandle,
        reason: str,
        *,
        watchdog_failure: bool = False,
    ) -> bool:
        observed_containment.append(
            (reason, {group.group for group in supervisor._overdue_group_health(worker)})
        )
        return contain(worker, reason, watchdog_failure=watchdog_failure)

    monkeypatch.setattr(supervisor, '_contain_current_worker', contain_current_worker)
    with supervisor:
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        supervisor._tick()
        assert dependent._assignment is not None
        remote = replace(
            _in_flight_group_health(dependent, heartbeat_at=clock()),
            current_callback_id='publish',
            deadline_at=clock() + config.sites['test'].groups['remote'].callback_deadline_secs,
        )
        second = replace(remote, group='second')
        group_path = source_database(config.daemon.state_dir)
        with (
            _conditional_group(group_path) as remote_state,
            _conditional_group(group_path, group='second') as second_state,
        ):
            remote_before = remote_state.snapshot()
            second_before = second_state.snapshot()
            assert (remote_before.in_flight_event_id, second_before.in_flight_event_id) == (
                remote.current_event_id,
                second.current_event_id,
            )
        for worker in factory._instances.values():
            worker._messages.append(
                _health_update(
                    worker,
                    heartbeat_at=clock(),
                    groups=(remote, second) if worker is dependent else None,
                )
            )
        supervisor._tick()

        clock.advance(config.sites['test'].groups['remote'].callback_deadline_secs + 0.001)
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        source._messages.append(_health_update(source, heartbeat_at=clock()))
        dependent._messages.append(
            _health_update(dependent, heartbeat_at=clock(), groups=(remote, second))
        )
        supervisor._tick()

        assert dependent._killed
        assert observed_containment == [('lane-phase-overdue', {'remote', 'second'})]
        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == ()
            policy = config.sites['test'].workers['pool']
            restored = state._restore_worker_breaker(
                worker_id=dependent._worker_id,
                restart_limit=policy.restart_limit,
                restart_window_secs=policy.restart_window_secs,
                healthy_reset_secs=policy.healthy_reset_secs,
                clock=clock,
            )
            assert restored._failure_total == 1
        assert supervisor._park_evidence_by_group == {}
        with (
            _open_conditional_group(group_path) as remote_state,
            _open_conditional_group(group_path, group='second') as second_state,
        ):
            assert remote_state.snapshot() == remote_before
            assert second_state.snapshot() == second_before

        clock.advance(1.001)
        source._messages.append(_health_update(source, heartbeat_at=clock()))
        for _ in range(4):
            supervisor._tick()
            replacement = factory._instances[dependent._worker_id]
            if replacement is not dependent and replacement._assignment is not None:
                break
        else:
            pytest.fail('ambiguous worker replacement did not become ready')
        assert all(
            isinstance(assignment, DeliveryGroupAssignment) and assignment.conditional_park is None
            for assignment in replacement._assignment.group_assignments
        )


def test_supervisor_batch_verdict_uses_time_after_consumption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory._instances.values() if item._spec.sources)
        worker._messages.append(_health_update(worker, heartbeat_at=0))
        acquire = worker._receive_available

        def delayed_batch() -> Iterator[ControlMessage]:
            batch = acquire()

            def messages() -> Iterator[ControlMessage]:
                yield from batch
                clock.advance(0.5)

            return messages()

        monkeypatch.setattr(worker, '_receive_available', delayed_batch)
        supervisor._tick()
        assert clock() == 0.5
        assert worker._killed
        assert supervisor._breakers[worker._worker_id]._failure_total == 1


def test_supervisor_capture_persists_wall_clock_but_not_monotonic_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    monotonic_clock = _FakeClock()
    wall_clock = _FakeClock()
    wall_clock.advance(1000.0)
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=monotonic_clock,
        wall_clock=wall_clock,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
                'evidence-1',
                'dependent-spawn-2',
                'capability-2',
                'evidence-2',
            )
        ).__next__,
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        group_health = _in_flight_group_health(
            dependent,
            heartbeat_at=monotonic_clock(),
        )
        for worker in factory._instances.values():
            worker._messages.append(
                _health_update(
                    worker,
                    heartbeat_at=monotonic_clock(),
                    groups=(group_health,) if worker is dependent else (),
                )
            )
        supervisor._tick()
        group_path = source_database(config.daemon.state_dir)
        observed_before_kill: list[_ParkEvidence] = []
        dependent._on_kill = lambda: observed_before_kill.extend(
            supervisor._park_evidence_by_group.values()
        )

        with _conditional_group(group_path):
            monotonic_clock.advance(
                config.sites['test'].groups['remote'].callback_deadline_secs + 0.001
            )
            source = next(worker for worker in factory._instances.values() if worker._spec.sources)
            source._messages.append(_health_update(source, heartbeat_at=monotonic_clock()))
            dependent._messages.append(
                _health_update(dependent, heartbeat_at=monotonic_clock(), groups=(group_health,))
            )
            supervisor._tick()

        assert dependent._killed
        original = _park_evidence(
            failed_worker_id=dependent._worker_id,
            observed_at_wall_clock=1000.0,
        )
        assert observed_before_kill == [original]
        assert original._observed_at_wall_clock != monotonic_clock()
        assert len(factory._history[dependent._worker_id]) == 2
        assert factory._operations.index('kill:named') < max(
            index
            for index, operation in enumerate(factory._operations)
            if operation == 'spawn:named'
        )

        repacked = factory._instances[dependent._worker_id]
        repacked_health = _in_flight_group_health(
            repacked,
            heartbeat_at=monotonic_clock(),
        )
        for worker in factory._instances.values():
            worker._messages.append(
                _health_update(
                    worker,
                    heartbeat_at=monotonic_clock(),
                    groups=(repacked_health,) if worker is repacked else (),
                )
            )
        supervisor._tick()
        monotonic_clock.advance(
            config.sites['test'].groups['remote'].callback_deadline_secs + 0.001
        )
        wall_clock.advance(1000.0)
        source._messages.append(_health_update(source, heartbeat_at=monotonic_clock()))
        supervisor._tick()

        assert repacked._killed
        assert supervisor._park_evidence_by_group == {('test', 'remote'): original}
        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == (original,)
    finally:
        supervisor._close()


def test_supervisor_checkpoints_state_on_configured_monotonic_cadence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    clock = _FakeClock()
    checkpoint_calls: list[object] = []

    def checkpoint(connection: object) -> bool:
        checkpoint_calls.append(connection)
        return True

    def checkpointer_factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        clock: Callable[[], float],
    ) -> Checkpointer:
        def run_checkpoint() -> tuple[int, int, int]:
            checkpoint(connection)
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            clock=clock,
            run_checkpoint=run_checkpoint,
        )

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        checkpointer_factory=checkpointer_factory,
    ) as supervisor:
        assert not supervisor._tick()
        clock.advance(config.daemon.checkpoint_interval_secs - 0.001)
        assert not supervisor._tick()
        assert checkpoint_calls == []

        clock.advance(0.001)
        assert supervisor._tick()
        assert len(checkpoint_calls) == 1

        clock.advance(config.daemon.checkpoint_interval_secs * 3)
        assert supervisor._tick()
        assert len(checkpoint_calls) == 2
        assert not supervisor._tick()


def test_supervisor_close_continues_after_unreaped_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
    )
    supervisor._open()
    supervisor._reconcile(generations={})
    supervisor.start()
    source = next(worker for worker in factory._instances.values() if worker._spec.sources)
    dependent = next(worker for worker in factory._instances.values() if not worker._spec.sources)
    source._messages.append(_health_update(source, heartbeat_at=0.0))
    dependent._messages.append(_health_update(dependent, heartbeat_at=0.0))
    for _ in range(4):
        supervisor._tick()
        if {
            worker_id
            for worker_id, spawn in supervisor._spawns.items()
            if spawn._health_inventory_complete
        } == {
            source._worker_id,
            dependent._worker_id,
        }:
            break
    assert {
        worker_id
        for worker_id, spawn in supervisor._spawns.items()
        if spawn._health_inventory_complete
    } == {
        source._worker_id,
        dependent._worker_id,
    }
    dependent._shutdown_status = None
    later_discard_error = OSError('later worker discard failed')
    source_close = source._close

    def fail_source_close() -> None:
        source_close()
        raise later_discard_error

    monkeypatch.setattr(source, '_close', fail_source_close)
    supervisor._spawns[dependent._worker_id]._health_deadline = 1.0
    vars(supervisor)['_restart_deadlines'][source._worker_id] = 2.0
    supervisor._spawns[source._worker_id]._termination_reason = 'test-termination'
    supervisor._spawns[source._worker_id]._health_failure = 'test-health-failure'
    supervisor._spawns[dependent._worker_id]._health = _health_update(
        dependent,
        heartbeat_at=0.0,
    )

    with pytest.raises(RuntimeError) as caught:
        supervisor._close()

    assert str(caught.value) == f'worker child remained unreaped: {dependent._worker_id}'
    assert sum(operation.startswith('shutdown:') for operation in factory._operations) == 2
    assert dependent._close_calls == 1
    assert source._close_calls == 1
    for projection in (
        '_restart_deadlines',
        '_source_endpoints',
        '_source_workers',
        '_stale_grant_readies',
        '_spawns',
    ):
        assert not vars(supervisor)[projection]
    assert vars(supervisor)['_state'] is None
    assert vars(supervisor)['_lock'] is None

    replacement = Supervisor(config=config, config_path=config_path)
    replacement._open()
    replacement._close()
    supervisor._close()


def test_supervisor_close_continues_with_first_unreaped_halt_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
    )
    supervisor._open()
    supervisor._reconcile(generations={})
    supervisor.start()
    shutdown_order = tuple(
        cast('_ScriptedWorker', spawn._worker)
        for spawn in reversed(tuple(supervisor._spawns.values()))
    )
    first_worker, later_worker = shutdown_order
    first_worker._shutdown_status = None
    later_worker._shutdown_status = None

    with pytest.raises(RuntimeError) as caught:
        supervisor._close()

    expected = f'worker child remained unreaped: {first_worker._worker_id}'
    assert str(caught.value) == expected
    assert supervisor._halt_reason_value == expected


def test_supervisor_close_continues_with_fresh_reconciliation_after_reopen(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
        worker_factory=_ScriptedWorkerFactory(),
    )
    supervisor._open()
    supervisor._reconcile(generations={})
    supervisor._close()
    supervisor._open()
    try:
        with pytest.raises(RuntimeError, match='must reconcile before spawning workers'):
            supervisor.start()
    finally:
        supervisor._close()


def test_supervisor_closed_batch_defers_new_arrivals_without_starving_another_worker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=_FakeClock(), worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        workers = tuple(factory._instances.values())
        for worker in workers:
            worker._messages.append(_health_update(worker, heartbeat_at=0))
        first = workers[0]
        later = _health_update(first, heartbeat_at=0)
        receive = first._receive

        def replenish(*, timeout: float | None = None) -> ControlMessage:
            message = receive(timeout=timeout)
            first._messages.append(later)
            return message

        monkeypatch.setattr(first, '_receive', replenish)
        acquisitions = {worker._worker_id: worker._batch_acquisitions for worker in workers}
        supervisor._tick()
        assert tuple(first._messages) == (later,)
        assert not workers[1]._messages
        assert all(
            worker._batch_acquisitions == acquisitions[worker._worker_id] + 1 for worker in workers
        )
        assert sum(spawn._health is not None for spawn in supervisor._spawns.values()) == len(
            workers
        )
        assert not any(worker._killed for worker in workers)


def test_supervisor_command_saturation_enters_worker_failure_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    monkeypatch.setenv('PYTHONPATH', os.fspath(Path(__file__).parents[1] / 'src'))
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    spawner = _ControlChildSpawner(
        """
import os
import sys
import time
from sg_event_daemon._ipc import ControlHello, WorkerControl

control = WorkerControl.from_fd(
    int(sys.argv[1]),
    max_frame_bytes=8_388_608,
    send_deadline_secs=1.0,
)
control.send(
    ControlHello(
        spawn_instance=sys.argv[2],
        worker_id=sys.argv[3],
        pid=os.getpid(),
    )
)
time.sleep(30)
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = cast('_WorkerInstance', next(iter(supervisor._spawns.values()))._worker)
        assert worker._authenticate(timeout=5.0)
        control = cast('WorkerControl', vars(worker)['_control'])
        _saturate_worker_control_sender(control)

        assert supervisor._tick()

        assert (
            supervisor._spawns[worker._worker_id]._termination_reason
            == 'worker-control-send-timeout'
        )
        assert worker._wait(timeout=5.0) not in (None, 0)


@pytest.mark.parametrize(('synchronous', 'expected'), [('normal', 1), ('full', 2)])
def test_supervisor_database_opens_use_configured_synchronous_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    synchronous: Literal['normal', 'full'],
    expected: int,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    site = config.sites['test']
    source = site.sources['admin']
    strict_holdback = source.visibility_overlap_secs + 10.0
    site = replace(
        site,
        groups=MappingProxyType(
            {
                'remote': replace(
                    site.groups['remote'], ordering='strict', strict_holdback_secs=strict_holdback
                )
            }
        ),
    )
    config = replace(
        config,
        daemon=replace(config.daemon, sqlite_synchronous=synchronous),
        sites=MappingProxyType({'test': site}),
    )
    _prepare_absent_orphan(config)
    supervisor = Supervisor(config=config, config_path=config_path)

    def assert_policy(state: SourceState | GroupState) -> None:
        primary = cast('sqlite3.Connection', vars(state)['_database'].connection)
        handoff = cast('sqlite3.Connection', vars(state)['_handoff'].connection)
        assert primary.execute('PRAGMA synchronous').fetchone() == (expected,)
        assert handoff.execute('PRAGMA synchronous').fetchone() == (2,)

    for configured in ({('test', 'remote'): 'admin'}, {}):
        with ExitStack() as stack:
            sources = supervisor._open_reconciliation_sources(stack, {('test', 'admin')})
            _group_generations, groups = supervisor._open_reconciliation_groups(
                stack, {('test', 'remote')}, configured, sources
            )
            assert_policy(sources[('test', 'admin')])
            group = groups[('test', 'remote')]
            assert_policy(group)
            assert group._settings._strict_holdback_secs == (strict_holdback)


def test_supervisor_derives_probe_freshness_from_existing_budgets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, loaded = _source_only_config(tmp_path, monkeypatch)
    config = replace(
        loaded,
        daemon=replace(
            loaded.daemon,
            health_interval_secs=0.25,
            supervisor_poll_interval_secs=0.125,
        ),
    )
    supervisor = Supervisor(config=config, config_path=config_path, clock=lambda: 0.0)
    progress = vars(supervisor)['_progress']
    progress.advance(now=0.0)
    progress.publish(
        ProbeSnapshot(
            generated_at=0.0,
            ready=True,
            complete=True,
            conditions=('healthy',),
            shutting_down=False,
            halted=False,
        )
    )

    assert progress.result(now=0.499).readiness
    assert not progress.result(now=0.5).readiness
    assert progress.result(now=9.999).liveness
    assert not progress.result(now=10.0).liveness


def test_supervisor_evidence_retirement_restarts_from_the_durable_group_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    _create_recovery_group_database(config)
    group_path = source_database(config.daemon.state_dir)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'retired-evidence-{token_index}'

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()
        owner = next(worker for worker in factory._instances.values() if not worker._spec.sources)
        assert owner._assignment is not None

        ready = _resolve_lane_recovery(
            config,
            owner._assignment,
            group_path,
            drop_ready=False,
        )
        owner._messages.append(ready)
        supervisor._tick()

        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == ()
        with _open_conditional_group(group_path) as group:
            parked = group.snapshot()
            assert parked.parked
            assert parked.revision == evidence._group_revision + 1
            recorded = group.resolve_conditional_park(
                _park_directive(evidence),
                identity=AuditIdentity.current(),
                decided_at_wall_clock=999.0,
            )
            assert recorded.outcome == 'parked'
            assert recorded.resulting_revision == parked.revision

        owner._exit_status = 1
        supervisor._tick()
        clock.advance(1.0)
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        source._messages.append(_health_update(source, heartbeat_at=clock()))
        supervisor._tick()
        supervisor._tick()

        replacement = factory._instances[owner._worker_id]
        assert replacement is not owner
        assert replacement._assignment is not None
        assert replacement._assignment.group_assignments == (
            DeliveryGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='resume',
                conditional_park=None,
            ),
        )


@pytest.mark.parametrize('owner', ['steady', 'startup', 'ready', 'grant'])
@pytest.mark.parametrize('fresh', [False, True])
def test_supervisor_exhausts_owner_health_batch_before_temporal_verdict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, owner: str, fresh: bool
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for _ in range(3):
            supervisor._tick()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(worker for worker in factory._instances.values() if worker._spec.groups)
        assert source._assignment is not None
        assert dependent._assignment is not None
        if owner in {'startup', 'ready'}:
            supervisor._spawns[source._worker_id]._lifecycle = supervisor_module._WorkerLifecycle(
                _assignment=source._assignment,
                _deadline_at=10,
                _worker=source,
                _assigned=True,
                _authenticated=True,
                _ready=owner == 'ready',
            )
        elif owner == 'grant':
            source._auto_grant_ready = False
            supervisor._spawns[
                dependent._worker_id
            ]._lifecycle = supervisor_module._WorkerLifecycle(
                _assignment=dependent._assignment,
                _deadline_at=10,
                _worker=dependent,
                _authenticated=True,
            )
        source._messages.append(_health_update(source, heartbeat_at=0))
        clock.advance(0.5)
        latest = _health_update(source, heartbeat_at=clock())
        if fresh:
            source._messages.append(latest)
        supervisor._tick()
        assert source._killed is not fresh
        if fresh:
            assert not source._messages
            assert (
                _spawn_health(supervisor, source._worker_id).worker.heartbeat_at
                == latest.worker.heartbeat_at
            )
            assert supervisor._breakers[source._worker_id]._failure_total == 0
        else:
            assert supervisor._breakers[source._worker_id]._failure_total == 1


@pytest.mark.parametrize('tail', ['fresh', 'stale', 'fatal'])
def test_operator_health_batch_checks_tail_after_correlated_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, tail: str
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for _ in range(3):
            supervisor._tick()
        owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        owner._messages.append(_health_update(owner, heartbeat_at=0))
        clock.advance(0.5)
        latest = _health_update(owner, heartbeat_at=clock() if tail != 'stale' else 0)
        if tail == 'fatal':
            latest = replace(latest, worker=replace(latest.worker, monitor_alive=False))
        owner._operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'revision': 3},
        )
        send = owner._send

        def send_with_tail(message: ControlMessage) -> None:
            send(message)
            owner._messages.append(latest)

        monkeypatch.setattr(owner, '_send', send_with_tail)
        command = OperatorCommand(
            request_id='batch-command',
            command='park',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=456),
        )
        if tail == 'fresh':
            result = supervisor.dispatch_operator_command(command, timeout=1)
            assert result.request_id == command.request_id
            assert not owner._killed
            assert not owner._messages
            assert _spawn_health(supervisor, owner._worker_id).worker.heartbeat_at == clock()
        else:
            with pytest.raises(ValueError, match='health'):
                supervisor.dispatch_operator_command(command, timeout=1)
            assert owner._killed


def test_supervisor_failed_open_attempts_lock_after_state_close_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(config=config, config_path=config_path)
    attempts: list[str] = []
    initiating = ValueError('pending evidence failed')

    def pending() -> object:
        raise initiating

    def close_state() -> None:
        attempts.append('state')
        raise OSError('state cleanup failed')

    def close_lock() -> None:
        attempts.append('lock')

    state = SimpleNamespace(_pending_park_evidence=pending, close=close_state)
    monkeypatch.setattr(
        supervisor_module, 'acquire_lock', Mock(return_value=SimpleNamespace(close=close_lock))
    )
    monkeypatch.setattr(supervisor_module._SupervisorState, '_open', Mock(return_value=state))
    with pytest.raises(ValueError, match='failed') as caught:
        supervisor._open()
    assert caught.value is initiating
    assert attempts == ['state', 'lock']
    assert any('state cleanup failed' in note for note in caught.value.__notes__)


def test_supervisor_future_watchdog_heartbeat_fails_at_first_observation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        future_detail = replace(
            _idle_source_health(source, source='admin', heartbeat_at=100.0),
            phase='provider-query',
            deadline_at=10_000.0,
        )
        future = _health_update(
            source,
            heartbeat_at=0.0,
            sources=(future_detail,),
            watchdogs=(
                _source_watchdog(
                    source,
                    deadline_at=10_000.0,
                    heartbeat_at=100.0,
                    phase='provider-query',
                ),
                WatchdogHealth(
                    kind='operator',
                    site='test',
                    source=None,
                    group=None,
                    worker_instance=source._spawn_instance,
                    phase='idle',
                    heartbeat_at=0.0,
                    deadline_at=None,
                ),
            ),
        )
        source._messages.append(future)
        dependent._messages.append(_health_update(dependent, heartbeat_at=0.0))
        monkeypatch.setattr(source, '_wait', _never_reap)

        supervisor._tick()

        assert source._killed
        assert supervisor._spawns[source._worker_id]._termination_reason == (
            'source-phase-overdue'
        )
        first = next(
            item
            for item in _spawn_health(supervisor, source._worker_id).watchdogs
            if item.kind == 'source'
        )
        assert first.heartbeat_at == 0.0
        assert first.deadline_at == 0.0

        clock.advance(100.0)
        source._messages.append(future)
        dependent._messages.append(_health_update(dependent, heartbeat_at=clock()))
        supervisor._tick()
        repeated = next(
            item
            for item in _spawn_health(supervisor, source._worker_id).watchdogs
            if item.kind == 'source'
        )
        assert repeated.deadline_at == 0.0
    finally:
        supervisor._close()


def test_supervisor_grant_health_failure_kills_source_author(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    )
    try:
        supervisor._open()
        source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        source._messages.append(
            replace(
                _health_update(source, heartbeat_at=0.0),
                worker=replace(
                    _health_update(source, heartbeat_at=0.0).worker,
                    site='wrong-site',
                ),
            )
        )
        monkeypatch.setattr(source, '_wait', _never_reap)

        supervisor._tick()

        assert source._killed
        assert not dependent._killed
        assert supervisor._spawns[source._worker_id]._termination_reason == (
            'watchdog-sampling-failed'
        )
        assert supervisor._spawns[dependent._worker_id]._termination_reason is None
    finally:
        supervisor._close()


def test_supervisor_halts_workers_after_checkpoint_failure_interval_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    clock = _FakeClock()
    factory = _ScriptedWorkerFactory()

    def checkpointer_factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        clock: Callable[[], float],
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            clock=clock,
            run_checkpoint=lambda: (1, 5, 0),
            random_value=lambda: 0,
        )

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        checkpointer_factory=checkpointer_factory,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        for _ in range(4):
            clock.advance(config.daemon.checkpoint_interval_secs)
            assert supervisor._tick()
            assert not any(operation.startswith('shutdown:') for operation in factory._operations)

        clock.advance(config.daemon.checkpoint_interval_secs)
        with pytest.raises(DatabaseError, match='checkpoint incomplete duration bound exceeded'):
            supervisor._tick()

        assert supervisor._halt_reason_value is not None
        assert factory._operations[-4:] == [
            'shutdown:named:supervisor-checkpoint-failure',
            'close:named',
            'shutdown:site:supervisor-checkpoint-failure',
            'close:site',
        ]
        with pytest.raises(RuntimeError, match='halted'):
            supervisor._tick()


@pytest.mark.parametrize('failure', ['dead-monitor', 'sampling', 'inventory', 'future-watchdog'])
@pytest.mark.parametrize('owner', ['steady', 'startup', 'ready', 'grant', 'operator'])
def test_supervisor_health_batch_cannot_erase_explicit_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str, owner: str
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=_FakeClock(), worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory._instances.values() if item._spec.sources)
        assert worker._assignment is not None
        if owner in {'startup', 'ready'}:
            supervisor._spawns[worker._worker_id]._lifecycle = supervisor_module._WorkerLifecycle(
                _assignment=worker._assignment,
                _deadline_at=10,
                _worker=worker,
                _authenticated=True,
                _assigned=True,
                _ready=owner == 'ready',
            )
        elif owner == 'grant':
            dependent = next(
                item for item in factory._instances.values() if not item._spec.sources
            )
            assert dependent._assignment is not None
            worker._auto_grant_ready = False
            supervisor._spawns[
                dependent._worker_id
            ]._lifecycle = supervisor_module._WorkerLifecycle(
                _assignment=dependent._assignment,
                _deadline_at=10,
                _worker=dependent,
                _authenticated=True,
            )
        healthy = _health_update(worker, heartbeat_at=0)
        assert not supervisor._accept_worker_health(worker, healthy)
        if failure == 'dead-monitor':
            bad = replace(healthy, worker=replace(healthy.worker, monitor_alive=False))
        elif failure == 'sampling':
            bad = replace(
                healthy,
                worker=replace(healthy.worker, watchdog_failure='watchdog-sampling-failed'),
            )
        elif failure == 'inventory':
            bad = replace(healthy, sources=(), watchdogs=())
        else:
            bad = replace(
                healthy,
                watchdogs=(replace(healthy.watchdogs[0], heartbeat_at=1), *healthy.watchdogs[1:]),
            )
        worker._messages.extend((bad, healthy))
        if owner == 'operator':
            with pytest.raises(ValueError, match='health'):
                supervisor._accept_operator_batch(
                    worker,
                    _source_adoption_command('fatal-batch'),
                    worker._receive_available(),
                    time.monotonic() + 1,
                )
        else:
            supervisor._advance_worker_messages()
        assert worker._killed
        assert tuple(worker._messages) == (healthy,)
        assert (
            supervisor._spawns[worker._worker_id]._termination_reason
            == {
                'dead-monitor': 'worker-heartbeat-overdue',
                'sampling': 'watchdog-sampling-failed',
                'inventory': 'source-phase-overdue',
                'future-watchdog': 'source-phase-overdue',
            }[failure]
        )


def test_supervisor_health_receipt_time_accepts_heartbeat_after_tick_entry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, _config = _source_only_config(tmp_path, monkeypatch)
    config_path.write_text(
        _replace_config_text(
            config_path.read_text(encoding='utf-8'),
            'health_interval_secs = 0.05',
            'health_interval_secs = 1.0',
        ),
        encoding='utf-8',
    )
    config = load_config(config_path)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    clock._now = 10.0
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        source._messages.append(_health_update(source, heartbeat_at=10.5))
        dependent._messages.append(_health_update(dependent, heartbeat_at=10.5))
        original_receive = source._receive

        def receive_after_clock_advance(
            *,
            timeout: float | None = None,
        ) -> ControlMessage:
            message = original_receive(timeout=timeout)
            if isinstance(message, HealthUpdate):
                clock.advance(1.0)
            return message

        monkeypatch.setattr(source, '_receive', receive_after_clock_advance)

        supervisor._tick()

        assert clock() == 11.0
        assert not source._killed
        assert supervisor._spawns[source._worker_id]._termination_reason is None
        assert _spawn_health(supervisor, source._worker_id).worker.heartbeat_at == 10.5


def test_supervisor_ingest_advances_between_pending_worker_messages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(iter(factory._instances.values()))
        update = _health_update(worker, heartbeat_at=0.0)
        worker._messages.extend((update, update))
        advances: list[None] = []
        receive = worker._receive
        receive_count = 0

        def observe_receive(*, timeout: float | None = None) -> ControlMessage:
            nonlocal receive_count
            if receive_count == 1:
                assert advances == [None]
            receive_count += 1
            return receive(timeout=timeout)

        monkeypatch.setattr(worker, '_receive', observe_receive)
        monkeypatch.setattr(supervisor, '_advance_progress', lambda: advances.append(None))
        ingest = cast(
            'Callable[[], bool]',
            vars(Supervisor)['_ingest_worker_ipc'].__get__(supervisor),
        )

        assert ingest()
    finally:
        supervisor._close()


def test_supervisor_inventory_orphans_removal_and_parks_readded_generation(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    with _SupervisorState._open(state_dir) as state:
        initial = state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={},
        )
        initial_assignment = initial[('test', 'remote')]
        assert isinstance(initial_assignment, ActiveSourceGroupAssignment)
        assert initial_assignment.group_generation == 1
        assert (
            state._reconcile_groups(
                configured={},
                generations={('test', 'remote'): 1},
            )
            == {}
        )
        assert state._orphaned_groups() == {('test', 'remote'): 1}

    with _SupervisorState._open(state_dir) as state:
        assignments = state._reconcile_groups(
            configured={
                ('test', 'remote'): 'admin',
                ('test', 'renamed'): 'admin',
            },
            generations={('test', 'remote'): 1},
        )

        assert assignments == {
            ('test', 'remote'): PendingDispositionSourceGroupAssignment(
                source='admin',
                group='remote',
                orphan_generation=1,
                reserved_generation=2,
            ),
            ('test', 'renamed'): ActiveSourceGroupAssignment(
                source='admin',
                group='renamed',
                group_generation=1,
                start_mode='initialize',
            ),
        }
        assert _parked_group_rows(state) == [('test', 'remote', 2)]

        stale_assignments = state._reconcile_groups(
            configured={
                ('test', 'remote'): 'admin',
                ('test', 'renamed'): 'admin',
            },
            generations={('test', 'remote'): 1},
        )
        assert stale_assignments[('test', 'remote')] == (
            PendingDispositionSourceGroupAssignment(
                source='admin',
                group='remote',
                orphan_generation=1,
                reserved_generation=2,
            )
        )
        assert _parked_group_rows(state) == [('test', 'remote', 2)]

        reconciled = state._reconcile_groups(
            configured={
                ('test', 'remote'): 'admin',
                ('test', 'renamed'): 'admin',
            },
            generations={('test', 'remote'): 2},
        )
        assert reconciled[('test', 'remote')] == PendingDispositionSourceGroupAssignment(
            source='admin',
            group='remote',
            orphan_generation=1,
            reserved_generation=2,
        )
        assert _parked_group_rows(state) == [('test', 'remote', 2)]


def test_supervisor_liveness_requires_run_loop_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._reconcile(generations={})

        status = supervisor.status_mapping({})

    assert status['liveness'] is False
    assert status['readiness'] is False


def test_supervisor_missing_lane_after_complete_startup_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        dependent._messages.append(_health_update(dependent, heartbeat_at=0.0))
        supervisor._tick()
        dependent._messages.append(
            _health_update(
                dependent,
                heartbeat_at=0.0,
                groups=(),
                watchdogs=(
                    WatchdogHealth(
                        kind='operator',
                        site='test',
                        source=None,
                        group=None,
                        worker_instance=dependent._spawn_instance,
                        phase='idle',
                        heartbeat_at=0.0,
                        deadline_at=None,
                    ),
                ),
            )
        )
        monkeypatch.setattr(dependent, '_wait', _never_reap)

        supervisor._tick()

        assert dependent._killed
        assert supervisor._spawns[dependent._worker_id]._termination_reason == (
            'lane-phase-overdue'
        )
    finally:
        supervisor._close()


def test_supervisor_open_restores_pending_conditional_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)

    with Supervisor(config=config, config_path=config_path) as supervisor:
        assert supervisor._park_evidence_by_group == {('test', 'remote'): evidence}


def test_supervisor_operator_dispatch_rejects_terminating_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    )
    command = OperatorCommand(
        request_id='terminating-owner-command',
        command='park',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=0.0))
        supervisor._tick()
        owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        owner._messages.append(
            _health_update(
                owner,
                heartbeat_at=0.0,
                groups=(),
                watchdogs=(
                    WatchdogHealth(
                        kind='operator',
                        site='test',
                        source=None,
                        group=None,
                        worker_instance=owner._spawn_instance,
                        phase='idle',
                        heartbeat_at=0.0,
                        deadline_at=None,
                    ),
                ),
            )
        )
        monkeypatch.setattr(owner, '_wait', _never_reap)
        owner._operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted by dying worker',
            request_id=sent.request_id,
            result={'revision': 99},
        )
        supervisor._tick()
        assert owner._killed
        assert supervisor._spawns[owner._worker_id]._termination_reason == ('lane-phase-overdue')
        sent_before = tuple(owner._sent_commands)

        with pytest.raises(
            ValueError,
            match='operator command does not have one running owner',
        ):
            supervisor.dispatch_operator_command(command, timeout=1.0)

        assert tuple(owner._sent_commands) == sent_before
        assert supervisor._spawns[owner._worker_id]._termination_reason == ('lane-phase-overdue')
    finally:
        supervisor._close()


def test_supervisor_operator_wait_contains_partial_health_before_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='partial-health-command',
        command='park',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=0.0))
        supervisor._tick()
        owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        owner._messages.append(
            _health_update(
                owner,
                heartbeat_at=0.0,
                groups=(),
                watchdogs=(
                    WatchdogHealth(
                        kind='operator',
                        site='test',
                        source=None,
                        group=None,
                        worker_instance=owner._spawn_instance,
                        phase='idle',
                        heartbeat_at=0.0,
                        deadline_at=None,
                    ),
                ),
            )
        )
        owner._operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'revision': 3},
        )

        with pytest.raises(ValueError, match='health'):
            supervisor.dispatch_operator_command(command, timeout=1.0)

        assert owner._killed
        assert supervisor._spawns[owner._worker_id]._termination_reason == ('lane-phase-overdue')
        assert not _spawn_health(supervisor, owner._worker_id).worker.monitor_alive


def test_supervisor_operator_watchdog_overdue_uses_exact_reason_without_park(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )

        def _operator_update() -> HealthUpdate:
            group = _in_flight_group_health(
                dependent,
                heartbeat_at=clock(),
                phase='idle',
            )
            return _health_update(
                dependent,
                heartbeat_at=clock(),
                groups=(group,),
                watchdogs=(
                    WatchdogHealth(
                        kind='lane',
                        site='test',
                        source='admin',
                        group='remote',
                        worker_instance=dependent._spawn_instance,
                        phase='idle',
                        heartbeat_at=clock(),
                        deadline_at=clock() + 1.25,
                    ),
                    WatchdogHealth(
                        kind='operator',
                        site='test',
                        source='admin',
                        group='remote',
                        worker_instance=dependent._spawn_instance,
                        phase='internal',
                        heartbeat_at=0.0,
                        deadline_at=10_000.0,
                        operator_command='park',
                    ),
                ),
            )

        source._messages.append(_health_update(source, heartbeat_at=clock()))
        dependent._messages.append(_operator_update())
        supervisor._tick()
        credential = config.sites['test'].credentials['admin']
        clock.advance(
            internal_stall_deadline_secs(
                config.daemon.health_interval_secs,
                credential,
            )
        )
        source._messages.append(_health_update(source, heartbeat_at=clock()))
        dependent._messages.append(_operator_update())
        monkeypatch.setattr(dependent, '_wait', _never_reap)

        supervisor._tick()

        assert dependent._killed
        assert supervisor._spawns[dependent._worker_id]._termination_reason == (
            'operator-phase-overdue'
        )
        assert supervisor._park_evidence_by_group == {}
    finally:
        supervisor._close()


def test_supervisor_orphans_before_retiring_same_generation_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._reconcile(
            generations={},
        )
    evidence = _park_evidence(group_revision=0)
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)

    omitted_config = replace(
        config,
        sites={
            'test': replace(
                config.sites['test'],
                groups={},
                workers={},
            )
        },
    )
    retirement_statuses: list[str] = []
    original_retire = _SupervisorState._retire_park_evidence
    with (
        _lifecycle_source(tmp_path / 'orphan-source.db') as source,
        _lifecycle_group(tmp_path / 'orphan-source.db') as group,
    ):
        source.initialize(100, incarnation='incarnation-1')

        def observe_retirement(state: _SupervisorState, evidence_id: str) -> None:
            retirement_statuses.append(group.snapshot().status)
            original_retire(state, evidence_id)

        monkeypatch.setattr(
            _SupervisorState,
            '_retire_park_evidence',
            observe_retirement,
        )
        with Supervisor(
            config=omitted_config,
            config_path=config_path,
        ) as supervisor:
            supervisor._reconcile(
                generations={('test', 'remote'): 1},
                source_states={('test', 'admin'): source},
                group_states={('test', 'remote'): group},
            )

        assert group.snapshot().status == 'orphaned'
        assert retirement_statuses == ['orphaned']

    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._pending_park_evidence() == ()


def test_supervisor_orphans_to_one_collapsed_marker_and_fences_old_generation(
    tmp_path: Path,
) -> None:
    with (
        _lifecycle_source(tmp_path / 'source.db') as source,
        _lifecycle_group(tmp_path / 'source.db') as group,
    ):
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 110, successful_clock=1.0)

        marker = Supervisor._orphan_group(source, group)
        repeated = Supervisor._orphan_group(source, group)

        assert repeated == marker
        assert marker.from_event_id == 101
        assert source.snapshot().replay_markers == (marker,)
        orphaned = group.snapshot()
        assert orphaned.status == 'orphaned'
        assert orphaned.replay_required_from == 101

        group.dispose(
            DispositionRequest(
                command='replay',
                group='remote',
                disposition_id='replay-orphan',
                reserved_generation=2,
                expected_revision=group.snapshot().revision,
                from_event_id=101,
                acknowledge_truncation=False,
                reason='recover orphan',
                identity=AuditIdentity.current(),
                wall_clock_time=10.0,
            )
        )
        assert source.snapshot().pending_obligations == 0
        assert source.snapshot().ranges == ()
        open_source_range(source, 112, 120, successful_clock=2.0)
        assert source.snapshot().pending_obligations == 1
        rows = source._database.connection.execute(
            'SELECT group_generation, obligation_state FROM range_groups WHERE group_name = ?',
            ('remote',),
        ).fetchall()
        assert rows == [(2, 'pending')]
        assert source.snapshot().replay_markers == ()
        assert group.snapshot().group_generation == 2


def test_supervisor_outer_loop_ignores_consumed_restart_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'restart-deadline-token-{token_index}'

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        token_factory=token,
        worker_factory=factory,
    )
    waits: list[float] = []
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        dependent._exit_status = 1
        supervisor._tick()
        factory._authentication_ready = False
        clock.advance(1.0)
        supervisor._tick()
        replacement = factory._instances[dependent._worker_id]

        supervisor.run(
            should_stop=lambda: bool(waits),
            wait=waits.append,
            set_foreground_progress=lambda _callback: None,
        )

        assert replacement is not dependent
        assert replacement._authentication_calls > 0
        assert waits == [pytest.approx(config.daemon.checkpoint_interval_secs)]
    finally:
        supervisor._close()


def test_supervisor_outer_loop_retires_one_overdue_zero_wait(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'overdue-deadline-token-{token_index}'

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        token_factory=token,
        worker_factory=factory,
    )
    waits: list[float] = []
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        dependent._wait_results.extend((None, None))
        health = _health_update(dependent, heartbeat_at=clock())
        dependent._messages.append(
            replace(
                health,
                worker=replace(health.worker, monitor_alive=False),
            )
        )
        supervisor._tick()

        with ControlServer(
            tmp_path / 'control-state',
            lambda _request, _identity: {},
        ) as control:

            def wait(seconds: float) -> None:
                waits.append(seconds)
                control.serve_once(seconds)

            supervisor.run(
                should_stop=lambda: len(waits) == 2,
                wait=wait,
                set_foreground_progress=control.set_foreground_progress,
            )

        assert waits[0] == 0.0
        assert waits[1] > 0.0
    finally:
        supervisor._close()


def test_supervisor_outer_loop_waits_for_confirming_group_phase_frame(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, _config = _two_dependent_config(tmp_path, monkeypatch)
    config_path.write_text(
        _replace_config_text(
            _replace_config_text(
                _replace_config_text(
                    _replace_config_text(
                        config_path.read_text(encoding='utf-8'),
                        'checkpoint_interval_secs = 0.1',
                        'checkpoint_interval_secs = 10.0',
                    ),
                    'health_interval_secs = 0.05',
                    'health_interval_secs = 10.0',
                ),
                'api_key = { env = "SG_EVENT_ADMIN_KEY" }',
                'api_key = { env = "SG_EVENT_ADMIN_KEY" }\n'
                'timeout_secs = 0.05\n'
                'max_rpc_attempts = 1\n'
                'rpc_attempt_interval_secs = 0.01',
            ),
            '[sites.test.groups.remote]\nworker = "pool"',
            '[sites.test.groups.remote]\nworker = "pool"\ncallback_deadline_secs = 0.25',
        ),
        encoding='utf-8',
    )
    config = load_config(config_path)
    factory = _ScriptedWorkerFactory()
    monotonic_clock = _FakeClock()
    wall_clock = _FakeClock()
    wall_clock.advance(1000.0)
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'deadline-token-{token_index}'

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=monotonic_clock,
        wall_clock=wall_clock,
        token_factory=token,
        worker_factory=factory,
    )
    waits: list[float] = []
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        remote = next(
            worker for worker in factory._instances.values() if 'remote' in worker._spec.groups
        )
        other = next(
            worker
            for worker in factory._instances.values()
            if not worker._spec.sources and worker is not remote
        )
        remote_health = _in_flight_group_health(
            remote,
            heartbeat_at=monotonic_clock(),
        )
        for worker in factory._instances.values():
            worker._messages.append(
                _health_update(
                    worker,
                    heartbeat_at=monotonic_clock(),
                    groups=(remote_health,) if worker is remote else (),
                )
            )
        supervisor._tick()
        other._exit_status = 1
        supervisor._tick()
        for worker in factory._instances.values():
            worker._receive_timeouts.clear()

        group_path = source_database(config.daemon.state_dir)

        def wait(seconds: float) -> None:
            waits.append(seconds)
            monotonic_clock.advance(seconds)
            assert not remote._killed
            assert not supervisor._park_evidence_by_group
            if len(waits) == 2:
                remote._messages.append(
                    _health_update(remote, heartbeat_at=monotonic_clock(), groups=(remote_health,))
                )

        with _conditional_group(group_path):
            supervisor.run(
                should_stop=lambda: remote._killed or len(waits) >= 3,
                wait=wait,
                set_foreground_progress=lambda _callback: None,
            )

        assert remote._killed
        assert waits == [config.daemon.supervisor_poll_interval_secs] * 2
        assert monotonic_clock() == sum(waits)
        evidence = supervisor._park_evidence_by_group[('test', 'remote')]
        assert evidence._failed_worker_id == remote._worker_id
        assert evidence._failed_spawn_instance == remote._spawn_instance
        assert evidence._event_id == 42
        assert evidence._group_revision == 7
        assert evidence._observed_at_wall_clock == 1000.0
        assert all(
            timeout == 0.0
            for worker in factory._instances.values()
            for timeout in worker._receive_timeouts
        )
    finally:
        supervisor._close()


def test_supervisor_outer_loop_waits_until_the_earliest_monitor_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, _config = _source_only_config(tmp_path, monkeypatch)
    config_path.write_text(
        _replace_config_text(
            _replace_config_text(
                config_path.read_text(encoding='utf-8'),
                'checkpoint_interval_secs = 0.1',
                'checkpoint_interval_secs = 10.0',
            ),
            'health_interval_secs = 0.05',
            'health_interval_secs = 10.0',
        ),
        encoding='utf-8',
    )
    config = load_config(config_path)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'token-{token_index}'

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        token_factory=token,
        worker_factory=factory,
    )
    waits: list[float] = []
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        dependent._exit_status = 1
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        for worker in factory._instances.values():
            worker._receive_timeouts.clear()

        supervisor.run(
            should_stop=lambda: bool(waits),
            wait=waits.append,
            set_foreground_progress=lambda _callback: None,
        )

        assert waits == [1.0]
        assert all(
            timeout == 0.0
            for worker in factory._instances.values()
            for timeout in worker._receive_timeouts
        )
    finally:
        supervisor._close()


def test_supervisor_park_evidence_selects_only_frame_confirmed_lane(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config = _two_group_shared_worker_config(tmp_path, monkeypatch)
    clock = _FakeClock()
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        wall_clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        remote = next(w for w in factory._instances.values() if not w._spec.sources)
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=0))
        supervisor._tick()
        clock.advance(10.1)
        confirmed = replace(_in_flight_group_health(remote, heartbeat_at=0), deadline_at=9.9)
        unconfirmed = replace(
            _in_flight_group_health(remote, heartbeat_at=0, group='second'), deadline_at=10.05
        )
        for worker in factory._instances.values():
            worker._messages.append(
                _health_update(
                    worker,
                    heartbeat_at=10,
                    groups=(confirmed, unconfirmed) if worker is remote else None,
                )
            )
        with _conditional_group(source_database(config.daemon.state_dir)):
            supervisor._tick()
        assert remote._killed
        assert set(supervisor._park_evidence_by_group) == {('test', 'remote')}
        evidence = supervisor._park_evidence_by_group[('test', 'remote')]
        assert evidence._failed_spawn_instance == remote._spawn_instance
        assert evidence._event_id == 42
        assert evidence._group_revision == 7


def test_supervisor_partial_startup_health_does_not_extend_startup_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        clock.advance(config.daemon.worker_startup_deadline_secs - 0.01)
        source._messages.append(
            _health_update(
                source,
                heartbeat_at=clock(),
                sources=(),
                groups=(),
                watchdogs=(),
            )
        )
        supervisor._tick()
        assert not source._killed

        clock.advance(0.01)
        monkeypatch.setattr(source, '_wait', _never_reap)
        supervisor._tick()

        assert source._killed
        assert supervisor._spawns[source._worker_id]._termination_reason == (
            'worker-heartbeat-overdue'
        )
    finally:
        supervisor._close()


def test_supervisor_preflights_adoption_before_parking_healthy_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='adopt-healthy',
        command='adopt-source-stream',
        arguments={
            'site': 'test',
            'source': 'admin',
            'reason': 'permission role changed',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_owner = next(
            worker for worker in factory._instances.values() if worker._spec.sources
        )
        group_owner = next(worker for worker in factory._instances.values() if worker._spec.groups)

        def _reject_adoption(message: OperatorCommand) -> CommandResult:
            return CommandResult(
                command=message.command,
                ok=False,
                message='source adoption requires stream-identity-changed halt',
                request_id=message.request_id,
                result={},
            )

        source_owner._operator_handler = _reject_adoption
        group_owner._operator_handler = lambda message: CommandResult(
            command=message.command,
            ok=True,
            message='accepted',
            request_id=message.request_id,
            result={'group': 'remote', 'parked': True},
        )

        result = supervisor.adopt_source_stream_command(command, timeout=1.0)

    assert not result.ok
    assert [item.command for item in source_owner._sent_commands] == ['preflight-source-adoption']
    assert group_owner._sent_commands == []


def test_supervisor_probe_mapping_uses_one_exact_observation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(config=config, config_path=config_path)
    observations = 0

    def observe() -> ProbeResult:
        nonlocal observations
        observations += 1
        return ProbeResult(
            liveness=True,
            readiness=False,
            complete=True,
            conditions=('source-degraded',),
            generated_at=10.0,
            snapshot_age_seconds=0.25,
        )

    monkeypatch.setattr(supervisor, 'probe_result', observe)

    result = supervisor.probe_mapping()

    assert result == {
        'liveness': True,
        'readiness': False,
        'complete': True,
        'conditions': ('source-degraded',),
        'generated_at': 10.0,
        'snapshot_age_seconds': 0.25,
    }
    assert tuple(result) == (
        'liveness',
        'readiness',
        'complete',
        'conditions',
        'generated_at',
        'snapshot_age_seconds',
    )
    assert observations == 1


def test_supervisor_probe_result_does_not_republish_halt_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(config=config, config_path=config_path, clock=lambda: 0.0)
    progress = vars(supervisor)['_progress']
    halt_writes: list[str] = []
    vars(supervisor)['_halt_reason_value'] = 'already halted'
    monkeypatch.setattr(progress, 'set_halted', lambda: halt_writes.append('halted'))

    assert supervisor._halt_reason_value == 'already halted'
    supervisor.probe_result()
    supervisor.probe_result()

    assert halt_writes == []


def test_supervisor_ready_lifecycle_health_failure_uses_watchdog_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    _create_recovery_group_database(config)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(config=config, config_path=config_path, worker_factory=factory)
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()
        owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        assert _spawn_lifecycle(supervisor, owner._worker_id)._ready
        owner._messages.append(
            replace(
                _health_update(owner, heartbeat_at=0.0),
                worker=replace(
                    _health_update(owner, heartbeat_at=0.0).worker,
                    site='wrong-site',
                ),
            )
        )
        monkeypatch.setattr(owner, '_wait', _never_reap)

        supervisor._tick()

        assert owner._killed
        assert supervisor._spawns[owner._worker_id]._termination_reason == (
            'watchdog-sampling-failed'
        )
    finally:
        supervisor._close()


def test_supervisor_real_watchdog_phase_anchor_and_callback_slide(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(config=config, config_path=config_path)
    clock = _FakeClock()
    clock.advance(10.0)
    source_cell = ComponentWatchdog(
        kind='source',
        identity='admin',
        worker_instance='phase-spawn-1',
        registration_token=os.urandom(16).hex(),
        startup_deadline_secs=2.0,
        clock=clock,
    )
    source_cell.transition('provider-query', deadline_at=10_000.0)
    lane_cell = ComponentWatchdog(
        kind='lane',
        identity='remote',
        worker_instance='phase-spawn-1',
        registration_token=os.urandom(16).hex(),
        startup_deadline_secs=2.0,
        clock=clock,
    )
    lane_cell.transition('callback', deadline_at=130.0)
    trusted = cast(
        'Callable[[Supervisor, WatchdogHealth, Mapping[str, SourceHealth]], float | None]',
        vars(Supervisor)['_trusted_watchdog_deadline'],
    )

    def _transported_source() -> WatchdogHealth:
        sample = source_cell.sample()
        return WatchdogHealth(
            kind='source',
            site='test',
            source='admin',
            group=None,
            worker_instance=sample.worker_instance,
            phase=sample.phase,
            heartbeat_at=sample.heartbeat_at,
            deadline_at=sample.deadline_at,
        )

    def _transported_lane() -> WatchdogHealth:
        sample = lane_cell.sample()
        return WatchdogHealth(
            kind='lane',
            site='test',
            source='admin',
            group='remote',
            worker_instance=sample.worker_instance,
            phase=sample.phase,
            heartbeat_at=sample.heartbeat_at,
            deadline_at=sample.deadline_at,
            event_id=42,
            plugin_id='noop',
            callback_id='handle',
            delivery_attempt=1,
        )

    first_source_deadline = trusted(supervisor, _transported_source(), {})
    first_lane_deadline = trusted(supervisor, _transported_lane(), {})
    assert first_lane_deadline is not None
    clock.advance(10.0)
    assert trusted(supervisor, _transported_source(), {}) == first_source_deadline
    lane_cell.heartbeat(sliding_deadline_secs=120.0)
    next_lane_deadline = trusted(supervisor, _transported_lane(), {})
    assert next_lane_deadline is not None
    assert next_lane_deadline > first_lane_deadline


def test_supervisor_reaps_partial_startup_without_worker_monitor_and_schedules_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    monkeypatch.setenv('PYTHONPATH', os.fspath(Path(__file__).parents[1] / 'src'))
    report_path = tmp_path / 'startup-report.json'
    spawner = _ControlChildSpawner(_partial_startup_child_code(config_path, report_path))
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)
    supervisor = Supervisor(config=config, config_path=config_path)
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        deadline = time.monotonic() + 5.0
        while not vars(supervisor)['_restart_deadlines']:
            assert time.monotonic() < deadline, 'failed child was not contained and reaped'
            supervisor._tick()
            time.sleep(0.005)
        assert len(spawner._processes) == 1
        assert reap_child(spawner._processes[0]) is not None
        assert json.loads(report_path.read_text()) == {
            'stuck': ['source:admin'],
            'resources_open': True,
            'monitor_started': False,
            'forced': [],
        }
        spec = build_worker_specs(config.sites['test'])[0]
        assert spec.worker_id in vars(supervisor)['_restart_deadlines']
        assert spec.worker_id not in supervisor._spawns
    finally:
        supervisor._close()
        for process in spawner._processes:
            terminate_child(process, force=True)
            assert reap_child(process, timeout=5.0) is not None


def test_supervisor_reconcile_orphans_once_and_starts_parked_readd(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._reconcile(
            generations={},
        )

    omitted_config = replace(
        config,
        sites={
            'test': replace(
                config.sites['test'],
                groups={},
                workers={},
            )
        },
    )
    source_path = source_database(config.daemon.state_dir)
    group_path = source_path
    with (
        Supervisor(config=omitted_config, config_path=config_path) as supervisor,
        _lifecycle_source(source_path) as source,
        _lifecycle_group(group_path) as group,
    ):
        source.initialize(100, incarnation='incarnation-1')
        for _ in range(2):
            supervisor._reconcile(
                generations={('test', 'remote'): 1},
                source_states={('test', 'admin'): source},
                group_states={('test', 'remote'): group},
            )

        marker = source.snapshot().replay_markers
        orphaned = group.snapshot()
        assert len(marker) == 1
        assert orphaned.status == 'orphaned'
        assert orphaned.replay_required_from == marker[0].from_event_id
        assert orphaned.revision == 1

    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()

    assert all(not worker._spec.groups for worker in factory._instances.values())
    source_owner = next(worker for worker in factory._instances.values() if worker._spec.sources)
    assert source_owner._assignment is not None
    assert source_owner._assignment.source_group_assignments == (
        PendingDispositionSourceGroupAssignment(
            source='admin', group='remote', orphan_generation=1, reserved_generation=2
        ),
    )


def test_supervisor_reconcile_processes_each_group_disposition_independently(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _two_dependent_config(tmp_path, monkeypatch)
    with (
        _lifecycle_source(
            tmp_path / 'source.db',
        ) as source,
        _lifecycle_group(tmp_path / 'source.db') as remote,
        _lifecycle_group(tmp_path / 'source.db', group='second') as second,
        Supervisor(config=config, config_path=config_path) as supervisor,
    ):
        source.initialize(100, incarnation='incarnation-1')
        _begin_readd_disposition(source, second, 'reset-to-latest')
        remote_before = remote.snapshot()
        second_before = second.snapshot()

        supervisor._reconcile(
            generations={
                ('test', 'remote'): 1,
                ('test', 'second'): 2,
            },
            source_states={('test', 'admin'): source},
            group_states={
                ('test', 'remote'): remote,
                ('test', 'second'): second,
            },
        )
        assert remote.snapshot() == remote_before
        assert second.snapshot() == second_before
        assert second.terminal_disposition_identity(2) is not None


def test_supervisor_reconciliation_repeats_only_missing_source_registration(
    tmp_path: Path,
) -> None:
    with _SupervisorState._open(tmp_path / 'state') as state:
        state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={},
        )

        assert state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 1},
        ) == {
            ('test', 'remote'): ActiveSourceGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='resume',
            )
        }
        with pytest.raises(RuntimeError, match='resume group state is missing'):
            state._reconcile_groups(
                configured={('test', 'remote'): 'admin'},
                generations={},
            )


def test_supervisor_rejects_a_second_instance_with_pid_and_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    first = Supervisor(config=config, config_path=config_path)
    second = Supervisor(config=config, config_path=config_path)
    lock_path = config.daemon.state_dir / 'supervisor.lock'
    try:
        first._open()

        with pytest.raises(
            AlreadyLockedError,
            match=rf'{lock_path}.*{os.getpid()}',
        ):
            second._open()
    finally:
        second._close()
        first._close()


def test_supervisor_repeated_failure_preserves_first_containment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=0.0))
        supervisor._tick()
        owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        evidence = _park_evidence(failed_worker_id=owner._worker_id)
        vars(supervisor)['_park_evidence_by_group'][('test', 'remote')] = evidence
        kill_calls: list[None] = []
        owner._on_kill = lambda: kill_calls.append(None)
        fail = supervisor._fail_current_worker

        fail(owner, 'lane-phase-overdue')
        first_health = _spawn_health(supervisor, owner._worker_id)
        first_evidence = supervisor._park_evidence_by_group
        fail(
            owner,
            'worker-control-send-failed',
            watchdog_failure=True,
        )

        assert supervisor._spawns[owner._worker_id]._termination_reason == ('lane-phase-overdue')
        assert _spawn_health(supervisor, owner._worker_id) == first_health
        assert supervisor._park_evidence_by_group == first_evidence
        assert kill_calls == [None]
    finally:
        supervisor._close()


def test_supervisor_replacement_rebuilds_evidence_after_old_lock_and_boot_change(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, loaded = _source_only_config(tmp_path, monkeypatch)
    config = replace(
        loaded,
        daemon=replace(
            loaded.daemon,
            supervisor_poll_interval_secs=0.01,
        ),
    )
    evidence = _park_evidence(observed_at_wall_clock=1000.0)
    old_clock = _FakeClock()
    old_clock.advance(1000.0)
    dependent_spec = next(
        spec for spec in build_worker_specs(config.sites['test']) if not spec.sources
    )
    policy = config.sites['test'].workers['pool']
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
        breaker = state._restore_worker_breaker(
            worker_id=dependent_spec.worker_id,
            restart_limit=policy.restart_limit,
            restart_window_secs=policy.restart_window_secs,
            healthy_reset_secs=policy.healthy_reset_secs,
            clock=old_clock,
            boot_id='boot-before-supervisor-crash',
        )
        assert breaker._record_failure('replacement interrupted by supervisor death') == 1.0
        state._persist_worker_breaker(breaker)
    _create_recovery_group_database(config)
    group_lock_path = group_lock(source_database(config.daemon.state_dir), 'remote')
    ready_read, ready_write = os.pipe()
    old_worker: ChildProcess | None = None
    old_status: int | None = None
    try:
        old_worker = spawn_child(
            [
                sys.executable,
                '-c',
                """
import os
import sys
import time
from pathlib import Path
from sg_event_daemon._platform import acquire_lock

with acquire_lock(Path(sys.argv[1])):
    os.write(int(sys.argv[2]), b'L')
    time.sleep(0.05)
""",
                os.fspath(group_lock_path),
                str(ready_write),
            ],
            pass_fds=(ready_write,),
            env=_child_environment(),
        )
        os.close(ready_write)
        ready_write = -1
        factory = _ScriptedWorkerFactory()
        new_clock = _FakeClock()
        new_clock.advance(5.0)
        token_index = 0

        def token() -> str:
            nonlocal token_index
            token_index += 1
            return f'new-boot-{token_index}'

        monkeypatch.setattr(
            supervisor_module,
            'boot_identity',
            lambda: 'boot-after-supervisor-crash',
        )
        assert _read_pipe(ready_read, 1) == b'L'
        with Supervisor(
            config=config,
            config_path=config_path,
            clock=new_clock,
            worker_factory=factory,
            token_factory=token,
        ) as supervisor:
            supervisor._reconcile(
                generations={('test', 'remote'): 1},
            )
            supervisor.start()
            owner = next(
                worker for worker in factory._instances.values() if not worker._spec.sources
            )
            assert owner._assignment is not None
            delivery = owner._assignment.group_assignments[0]
            assert isinstance(delivery, DeliveryGroupAssignment)
            assert delivery.conditional_park == _park_directive(evidence)
    finally:
        os.close(ready_read)
        if ready_write >= 0:
            os.close(ready_write)
        if old_worker is not None:
            old_status = reap_child(old_worker, timeout=5.0)
            if old_status is None:
                terminate_child(old_worker, force=True)
                old_status = reap_child(old_worker, timeout=5.0)
            assert old_status is not None, 'fallback child reap timed out'
    assert old_status == 0


def test_supervisor_replacement_worker_retains_independent_failure_fence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'replacement-fence-{token_index}'

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        old = next(worker for worker in factory._instances.values() if worker._spec.groups)
        old_kills: list[None] = []
        old._on_kill = lambda: old_kills.append(None)
        fail = cast(
            'Callable[[_ScriptedWorker, str], None]',
            vars(Supervisor)['_fail_current_worker'].__get__(supervisor),
        )
        fail(old, 'lane-phase-overdue')
        supervisor._tick()
        clock.advance(1.0)
        for _ in range(4):
            supervisor._tick()
            replacement = factory._instances[old._worker_id]
            if replacement is not old and replacement._assignment is not None:
                break
        else:
            pytest.fail('replacement did not become ready')
        replacement_kills: list[None] = []
        replacement._on_kill = lambda: replacement_kills.append(None)

        fail(old, 'stale-old-handle')
        fail(replacement, 'operator-phase-overdue')

        assert old_kills == [None]
        assert replacement_kills == [None]
        assert supervisor._spawns[old._worker_id]._termination_reason == ('operator-phase-overdue')
    finally:
        supervisor._close()


def test_supervisor_replaces_spawn_bound_grant_before_dependent_restart_assignment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    tokens = iter(
        (
            'source-generation-1',
            'source-spawn-1',
            'dependent-spawn-1',
            'dependent-capability-1',
            'dependent-spawn-2',
            'dependent-capability-2',
        )
    )
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=lambda: next(tokens),
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        source, dependent = build_worker_specs(config.sites['test'])
        source_worker = factory._instances[source.worker_id]
        old_dependent = factory._instances[dependent.worker_id]
        old_assignment = old_dependent._assignment
        assert old_assignment is not None

        with pytest.raises(ValueError, match='source-owning worker restart'):
            supervisor._replace_worker(source.worker_id)
        assert factory._instances[source.worker_id] is source_worker

        supervisor._replace_worker(dependent.worker_id)

        new_dependent = factory._instances[dependent.worker_id]
        new_assignment = new_dependent._assignment
        assert new_assignment is not None
        assert new_dependent is not old_dependent
        assert old_assignment.spawn_instance == 'dependent-spawn-1'
        assert new_assignment.spawn_instance == 'dependent-spawn-2'
        assert new_assignment.group_assignments == (
            DeliveryGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='initialize',
            ),
        )
        assert source_worker._sent_grants == [
            EventPeerGrant(
                source='admin',
                group='remote',
                dependent_spawn_instance='dependent-spawn-1',
                capability='dependent-capability-1',
            ),
            EventPeerGrant(
                source='admin',
                group='remote',
                dependent_spawn_instance='dependent-spawn-2',
                capability='dependent-capability-2',
            ),
        ]
        assert factory._operations[-6:] == [
            'shutdown:named:worker-restart',
            'close:named',
            'spawn:named',
            'grant:remote',
            'receive:grant-ready:remote',
            'assign:named',
        ]
    finally:
        supervisor._close()


def test_supervisor_reserves_and_reuses_configured_group_disposition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    group_path = source_database(config.daemon.state_dir)
    with _lifecycle_group(group_path):
        pass
    factory = _ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='reset-configured-1',
        command='reset-group-to-latest',
        arguments={
            'site': 'test',
            'group': 'remote',
            'reason': 'accept current frontier',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        owner = next(
            worker for worker in factory._instances.values() if 'remote' in worker._spec.groups
        )
        responses = iter(
            (
                ('probe', {'disposition_required': True}),
                ('accepted', {'group': 'remote', 'group_generation': 2}),
                ('recorded', {'group': 'remote', 'group_generation': 2}),
            )
        )
        owner._operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message=(response := next(responses))[0],
            request_id=sent.request_id,
            result=response[1],
        )

        first = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        repeated = supervisor.dispatch_group_disposition_command(command, timeout=1.0)

    assert first.result == {'group': 'remote', 'group_generation': 2}
    assert repeated.result == first.result
    assert [
        (
            sent.arguments['orphan_generation'],
            sent.arguments['reserved_generation'],
        )
        for sent in owner._sent_commands
    ] == [(1, 1), (1, 2), (2, 2)]


def test_supervisor_resets_worker_breaker_with_peer_audit_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[1]
    breaker = _WorkerBreaker(
        worker_id=spec.worker_id,
        restart_limit=0,
        restart_window_secs=100.0,
        healthy_reset_secs=100.0,
    )
    breaker._record_failure('ambiguous-worker-failure')
    identity = AuditIdentity(local_uid=501, username='operator', pid=456)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        state = cast('_SupervisorState', vars(supervisor)['_state'])
        state._persist_worker_breaker(breaker)
        vars(supervisor)['_specs'][spec.worker_id] = spec
        vars(supervisor)['_breakers'][spec.worker_id] = breaker

        result = supervisor.reset_worker_breaker_command(
            {
                'site': 'test',
                'worker': spec.worker_id,
                'reason': 'operator reviewed shared worker',
            },
            identity=identity,
            wall_clock_time=20.0,
        )

        assert result['state'] == 'closed'
        assert result['worker_id'] == spec.worker_id
        assert _breaker_audit_rows(state, spec.worker_id) == [(1,)] * 1
        database = cast('DatabaseHandle', vars(state)['_database'])
        audit_rows = database.connection.execute(
            'SELECT local_uid, username, pid FROM breaker_audit WHERE worker_id = ?',
            (spec.worker_id,),
        ).fetchall()
        assert [tuple(row) for row in audit_rows] == [(501, 'operator', 456)]


def test_supervisor_restart_discards_complete_old_worker_lifecycle_before_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'lifecycle-retirement-{token_index}'

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    )
    try:
        supervisor._open()
        source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        worker_id = dependent._worker_id
        stale_health = _health_update(dependent, heartbeat_at=clock())
        stale_health_deadline = 123.0
        stale_restart_deadline = 456.0
        supervisor._spawns[worker_id]._health = stale_health
        supervisor._spawns[worker_id]._health_deadline = stale_health_deadline
        supervisor._spawns[worker_id]._termination_reason = 'stale-termination'
        vars(supervisor)['_restart_deadlines'][worker_id] = stale_restart_deadline
        spawn_observations: list[str] = []

        def spawn_replacement(
            **options: Unpack[_ScriptedWorkerOptions],
        ) -> _ScriptedWorker:
            spec = options['spec']
            if spec.worker_id == worker_id:
                spawn_observations.append(spec.worker_id)
                assert worker_id not in supervisor._spawns
                assert worker_id not in vars(supervisor)['_restart_deadlines']
                assert dependent._close_calls == 1
            return factory(**options)

        vars(supervisor)['_worker_factory'] = spawn_replacement
        factory._authentication_ready = True

        supervisor._replace_worker(worker_id)

        replacement = factory._instances[worker_id]
        assert replacement is not dependent
        assert spawn_observations == [worker_id]
        assert dependent._close_calls == 1
        assert supervisor._spawns[worker_id]._health is not stale_health
        assert supervisor._spawns[worker_id]._health_deadline != stale_health_deadline
        assert supervisor._spawns[worker_id]._termination_reason is None
        assert worker_id not in vars(supervisor)['_restart_deadlines']
        stale_ready = EventPeerReady(
            source='admin',
            group='remote',
            dependent_spawn_instance=dependent._spawn_instance,
        )
        assert stale_ready in vars(supervisor)['_stale_grant_readies']
        assert source._sent_grants[-1].dependent_spawn_instance == replacement._spawn_instance
    finally:
        supervisor._close()


def test_supervisor_restart_fails_closed_while_old_child_is_unreaped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'dependent-capability-1',
            )
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        dependent._shutdown_status = None

        with pytest.raises(RuntimeError, match='unreaped'):
            supervisor._replace_worker(dependent._worker_id)

        assert len(factory._history[dependent._worker_id]) == 1
        dependent._shutdown_status = 0
    finally:
        supervisor._close()


def test_supervisor_restart_rebuilds_directive_for_fresh_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState._open(config.daemon.state_dir) as state:
        assert state._record_park_evidence(evidence)
    _create_recovery_group_database(config)
    assignments: list[tuple[str, ConditionalParkDirective]] = []

    for supervisor_number in (1, 2):
        factory = _ScriptedWorkerFactory()
        token_index = 0

        def token(*, number: int = supervisor_number) -> str:
            nonlocal token_index
            token_index += 1
            return f'supervisor-{number}-identity-{token_index}'

        with Supervisor(
            config=config,
            config_path=config_path,
            worker_factory=factory,
            token_factory=token,
        ) as supervisor:
            supervisor._reconcile(
                generations={('test', 'remote'): 1},
            )
            supervisor.start()
            owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
            assert owner._assignment is not None
            delivery = next(
                item for item in owner._assignment.group_assignments if item.group == 'remote'
            )
            assert isinstance(delivery, DeliveryGroupAssignment)
            assert delivery.conditional_park is not None
            assignments.append(
                (
                    owner._spawn_instance,
                    delivery.conditional_park,
                )
            )

    assert assignments[0][0] != assignments[1][0]
    assert assignments[0][1] == assignments[1][1] == _park_directive(evidence)


def test_supervisor_restart_resumes_after_group_full_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'dependent-capability-1',
                'dependent-spawn-2',
                'dependent-capability-2',
            )
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        group_path = source_database(config.daemon.state_dir)

        with _lifecycle_group(group_path):
            supervisor._replace_worker(dependent._worker_id)

        restarted = factory._instances[dependent._worker_id]
        assert restarted._assignment is not None
        assert restarted._assignment.group_assignments == (
            DeliveryGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='resume',
            ),
        )
    finally:
        supervisor._close()


def test_supervisor_restarts_workers_on_independent_deadlines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _two_dependent_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'token-{token_index}'

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        token_factory=token,
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependents = sorted(
            (worker for worker in factory._instances.values() if not worker._spec.sources),
            key=lambda worker: worker._worker_id,
        )
        first_id, second_id = (worker._worker_id for worker in dependents)
        dependents[0]._exit_status = 1
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=clock()))

        supervisor._tick()
        assert len(factory._history[first_id]) == 1
        clock.advance(1.0)
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        assert len(factory._history[first_id]) == 2

        factory._instances[first_id]._exit_status = 1
        factory._instances[second_id]._exit_status = 1
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=clock()))
        supervisor._tick()

        clock.advance(1.0)
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        assert len(factory._history[first_id]) == 2
        assert len(factory._history[second_id]) == 2

        clock.advance(1.0)
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        assert len(factory._history[first_id]) == 3
    finally:
        supervisor._close()


@pytest.mark.parametrize('source_mode', ['live', 'stopped'])
def test_supervisor_routes_absent_orphan_to_ready_source_or_direct(
    source_mode: Literal['live', 'stopped'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, group_path = _prepare_absent_orphan(config)
    factory = _ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id=f'reset-absent-{source_mode}',
        command='reset-group-to-latest',
        arguments={
            'site': 'test',
            'group': 'remote',
            'reason': 'retire absent group history',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    internal_commands: list[str] = []
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        if source_mode == 'live':
            supervisor.start()
            source_owner = next(
                worker for worker in factory._instances.values() if 'admin' in worker._spec.sources
            )

            def execute_source_command(message: OperatorCommand) -> CommandResult:
                internal_commands.append(message.command)
                assert message.command == 'group-disposition'
                with _lifecycle_source(source_path):
                    return _execute_source_group_command(group_path, message)

            source_owner._operator_handler = execute_source_command

        result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)

    with (
        _lifecycle_group(group_path, generation=2) as group,
        _lifecycle_source(
            source_path,
        ) as source,
    ):
        identity = group.terminal_disposition_identity(2)
        assert identity is not None
        assert source.has_terminal_disposition('remote', *identity)
        assert source.snapshot().replay_markers == ()
    assert result.ok
    assert result.result['group_generation'] == 2
    assert internal_commands == (['group-disposition'] if source_mode == 'live' else [])


@pytest.mark.parametrize('probe', ['liveness', 'readiness'])
def test_maximum_status_probe_returns_valid_control_frame(
    probe: str,
    tmp_path: Path,
) -> None:
    status = _project_status(_maximum_status_snapshot())
    with ControlServer(tmp_path / 'state', lambda _request, _identity: status) as server:
        results: list[Mapping[str, object]] = []
        client = threading.Thread(
            target=lambda: results.append(
                ControlClient(server._path).request(
                    'status',
                    request_id=probe[0] * 32,
                )
            )
        )
        client.start()
        server.serve_once(1.0)
        client.join(2)
        assert not client.is_alive()
        result = results[0]

    response_body = encode_json_body(
        {
            'version': _PROTOCOL_VERSION,
            'request_id': probe[0] * 32,
            'ok': True,
            'result': dict(result),
            'error': None,
        }
    )
    expected = probe == 'liveness'
    assert result[probe] is expected
    assert len(response_body) <= _MAX_MESSAGE_BYTES
    assert _MAX_MESSAGE_BYTES - supervisor_module._STATUS_RESULT_MAX_BYTES >= 262_144


@pytest.mark.parametrize(
    ('grant_operation', 'error_factory', 'expected_reason'),
    [
        (
            'send',
            lambda: BrokenPipeError('simulated source grant send failure'),
            'worker-control-send-failed',
        ),
        (
            'send',
            lambda: TimeoutError('worker control send timed out'),
            'worker-control-send-timeout',
        ),
        (
            'send',
            lambda: ValueError('simulated poisoned selector failure'),
            'worker-control-send-failed',
        ),
        (
            'receive',
            lambda: ConnectionResetError('simulated source grant receive failure'),
            'worker-control-eof',
        ),
        (
            'receive',
            lambda: EOFError('worker control peer closed'),
            'worker-control-eof',
        ),
    ],
)
def test_supervisor_grant_control_failure_restarts_source_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    grant_operation: Literal['send', 'receive'],
    error_factory: Callable[[], BaseException],
    expected_reason: str,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'grant-control-failure-{token_index}'

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        source._authentication_ready = True
        supervisor._tick()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        dependent._authentication_ready = True
        source._wait_results.append(None)
        failure_calls = _install_source_grant_control_failure(
            monkeypatch,
            source,
            grant_operation=grant_operation,
            error_factory=error_factory,
        )

        assert supervisor._tick()

        assert source._killed
        assert not dependent._killed
        assert supervisor._spawns[source._worker_id]._termination_reason == expected_reason
        assert supervisor._spawns[dependent._worker_id]._termination_reason is None
        assert len(failure_calls) == 1

        assert supervisor._tick()

        restart_deadlines = vars(supervisor)['_restart_deadlines']
        assert len(failure_calls) == 1
        assert source._worker_id not in supervisor._spawns
        assert restart_deadlines[source._worker_id] == 1.0

        factory._authentication_ready = True
        clock.advance(1.0)
        assert supervisor._tick()
        assert factory._instances[source._worker_id] is not source
    finally:
        supervisor._close()


@pytest.mark.parametrize('phase', ['internal', 'write-lock-wait', 'write-hold'])
def test_supervisor_internal_lane_overdue_never_creates_park_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    phase: Literal['internal', 'write-lock-wait', 'write-hold'],
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        group = _in_flight_group_health(dependent, heartbeat_at=0.0, phase=phase)
        lane_watchdog = WatchdogHealth(
            kind='lane',
            site='test',
            source='admin',
            group='remote',
            worker_instance=dependent._spawn_instance,
            phase=phase,
            heartbeat_at=0.0,
            deadline_at=10_000.0,
        )
        operator = WatchdogHealth(
            kind='operator',
            site='test',
            source=None,
            group=None,
            worker_instance=dependent._spawn_instance,
            phase='idle',
            heartbeat_at=0.0,
            deadline_at=None,
        )
        dependent._messages.append(
            _health_update(
                dependent,
                heartbeat_at=0.0,
                groups=(group,),
                watchdogs=(lane_watchdog, operator),
            )
        )
        source._messages.append(_health_update(source, heartbeat_at=0.0))
        supervisor._tick()
        credential = config.sites['test'].credentials['admin']
        clock.advance(
            internal_stall_deadline_secs(
                config.daemon.health_interval_secs,
                credential,
            )
        )
        dependent._messages.append(
            replace(
                _health_update(
                    dependent,
                    heartbeat_at=0.0,
                    groups=(group,),
                    watchdogs=(lane_watchdog, operator),
                ),
                worker=replace(
                    _health_update(dependent, heartbeat_at=clock()).worker,
                    heartbeat_at=clock(),
                ),
            )
        )
        source._messages.append(_health_update(source, heartbeat_at=clock()))
        monkeypatch.setattr(dependent, '_wait', _never_reap)

        supervisor._tick()

        assert dependent._killed
        assert supervisor._spawns[dependent._worker_id]._termination_reason == (
            'lane-phase-overdue'
        )
        assert supervisor._park_evidence_by_group == {}
    finally:
        supervisor._close()


@pytest.mark.parametrize('targeted', [False, True], ids=['idle-owner', 'active-owner'])
def test_supervisor_operator_shutdown_inventory_accepts_current_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    targeted: bool,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(
            item for item in factory._instances.values() if bool(item._spec.groups) == targeted
        )
        update = _health_update(worker, heartbeat_at=0.0)
        operator = WatchdogHealth(
            kind='operator',
            site='test',
            source='admin' if targeted else None,
            group='remote' if targeted else None,
            worker_instance=worker._spawn_instance,
            phase='shutdown',
            heartbeat_at=0.0,
            deadline_at=config.daemon.shutdown_deadline_secs,
            operator_command='park' if targeted else None,
        )
        worker._messages.append(
            replace(
                update,
                watchdogs=(*update.watchdogs[:-1], operator),
            )
        )

        supervisor._tick()

        assert not worker._killed
        published = next(
            item
            for item in _spawn_health(supervisor, worker._worker_id).watchdogs
            if item.kind == 'operator'
        )
        assert published.phase == 'shutdown'
        assert (published.source, published.group) == (
            ('admin', 'remote') if targeted else (None, None)
        )
    finally:
        supervisor._close()


@pytest.mark.parametrize('worker_mode', ['pool', 'dedicated'])
def test_supervisor_quiesces_groups_before_source_stream_adoption(
    worker_mode: Literal['pool', 'dedicated'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    if worker_mode == 'dedicated':
        config_path.write_text(
            _replace_config_text(
                config_path.read_text(encoding='utf-8'), 'worker = "pool"', 'worker = "dedicated"'
            ),
            encoding='utf-8',
        )
        config = load_config(config_path)
    factory = _ScriptedWorkerFactory()
    command = _source_adoption_command(f'adopt-{worker_mode}')
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_owner = next(
            worker for worker in factory._instances.values() if worker._spec.sources
        )
        group_owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        source_assignment = source_owner._assignment
        group_assignment = group_owner._assignment
        assert source_assignment is not None
        assert group_assignment is not None
        source_owner._operator_handler = _source_adoption_result
        group_owner._operator_handler = _source_adoption_result
        operations_before_adoption = len(factory._operations)

        result = supervisor.adopt_source_stream_command(command, timeout=1.0)

        source_replacement = factory._instances[source_owner._worker_id]
        group_replacement = factory._instances[group_owner._worker_id]
        replacement_source_assignment = source_replacement._assignment
        replacement_group_assignment = group_replacement._assignment

    assert source_replacement is not source_owner
    assert group_replacement is not group_owner
    assert replacement_source_assignment is not None
    assert replacement_group_assignment is not None
    replacement_operations = factory._operations[operations_before_adoption:]
    assert replacement_operations.index('spawn:site') < replacement_operations.index(
        f'spawn:{group_owner._spec.kind.value}'
    )
    replacement_generation = dict(replacement_source_assignment.source_generations)['admin']
    assert replacement_generation != dict(source_assignment.source_generations)['admin']
    assert replacement_group_assignment.source_endpoints == ()
    assert replacement_group_assignment.event_grants == ()
    assert replacement_group_assignment.group_assignments == ()
    assert group_replacement._spec.groups == ()
    assert source_replacement._sent_grants == []
    assert isinstance(
        replacement_source_assignment.source_group_assignments[0],
        PendingDispositionSourceGroupAssignment,
    )

    assert group_owner._sent_commands[0].command == 'park'
    assert group_owner._sent_commands[0].arguments['group'] == 'remote'
    assert [item.command for item in source_owner._sent_commands] == [
        'preflight-source-adoption',
        'adopt-source-stream',
    ]
    assert source_owner._sent_commands[1].arguments['groups_parked'] is True
    assert result.result == {
        'adoption_count': 1,
        'initialized': True,
        'source': 'admin',
        'status': 'running',
        'stream_incarnation': 'adoption-reservation-1',
    }


@pytest.mark.parametrize(
    ('command_name', 'condition', 'replay_from'),
    [
        ('replay', 'unparked', 50),
        ('reset-group-to-latest', 'unparked', None),
        ('replay', 'active-event', 50),
        ('reset-group-to-latest', 'active-event', None),
        ('replay', 'parked', 0),
        ('replay', 'parked', 101),
    ],
)
def test_supervisor_rejects_invalid_disposition_before_reservation(
    command_name: str,
    condition: str,
    replay_from: int | None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    ensure_secure_state_dir(config.daemon.state_dir)
    source_path = config.daemon.state_dir / 'sites' / 'test' / 'sources' / 'admin.db'
    group_path = source_database(config.daemon.state_dir)
    with _lifecycle_source(source_path) as source:
        source.initialize(100, incarnation='incarnation-1')
    with _lifecycle_group(group_path) as group:
        if condition == 'active-event':
            group.start_event(
                101,
                CallbackManifest(
                    callbacks=(
                        CallbackIdentity(
                            ordinal=0,
                            plugin_id='noop',
                            callback_id='publish',
                            execution='sync',
                            filter_digest='filter-1',
                            config_digest='config-1',
                            secret_refs=(),
                            credential_name='admin',
                            credential_fingerprint='admin:test',
                        ),
                    )
                ),
                expected_revision=group.snapshot().revision,
            )
        if condition != 'unparked':
            group.park(
                reason='maintenance',
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
                expected_revision=group.snapshot().revision,
            )
    factory = _ScriptedWorkerFactory()
    with Supervisor(config=config, config_path=config_path, worker_factory=factory) as supervisor:
        supervisor.reconcile_durable_state()
        supervisor.start()
        owner = next(
            worker for worker in factory._instances.values() if 'remote' in worker._spec.groups
        )
        component = object.__new__(worker_module._LaneComponent)
        component._group = 'remote'
        component._spec = owner._spec
        client = cast('worker_module._LocalJoinClient', SimpleNamespace())
        with _lifecycle_group(group_path) as group:
            before = group.snapshot()

            def execute(sent: OperatorCommand) -> CommandResult:
                try:
                    result = component._execute_disposition_operator(group, client, sent)
                except InvalidTransitionError as failure:
                    return CommandResult(
                        command=sent.command,
                        ok=False,
                        message=str(failure),
                        request_id=sent.request_id,
                        result={},
                    )
                return CommandResult(
                    command=sent.command,
                    ok=True,
                    message='accepted',
                    request_id=sent.request_id,
                    result=result,
                )

            owner._operator_handler = execute
            arguments: dict[str, object] = {
                'site': 'test',
                'group': 'remote',
                'reason': 'recover group',
            }
            if replay_from is not None:
                arguments['from_event_id'] = replay_from
                arguments['acknowledge_truncation'] = False
            result = supervisor.dispatch_group_disposition_command(
                OperatorCommand(
                    request_id='invalid-disposition',
                    command=command_name,
                    arguments=arguments,
                    identity=AuditIdentity.current(),
                ),
                timeout=1.0,
            )
            assert not result.ok
            assert group.snapshot() == before
        assert supervisor._state is not None
        inventory = supervisor._state._inventory_records()[('test', 'remote')]
        assert inventory._lifecycle == 'active'
        assert inventory._generation == 1

    with Supervisor(config=config, config_path=config_path) as restarted:
        restarted.reconcile_durable_state()
        assignment = restarted._build_worker_assignment(owner._spec, 'replacement', (), (), ())
        assert assignment.group_assignments == (
            DeliveryGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='resume',
            ),
        )


@pytest.mark.parametrize('reason', [None, [], {}, 1, True, ''])
@pytest.mark.parametrize('command_name', ['replay', 'reset-group-to-latest'])
def test_supervisor_rejects_invalid_disposition_reason_before_durable_changes(
    reason: object,
    command_name: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, group_path = _prepare_absent_orphan(config)
    arguments = {'site': 'test', 'group': 'remote', 'reason': reason}
    if command_name == 'replay':
        arguments.update(from_event_id=50, acknowledge_truncation=False)
    command = OperatorCommand(
        request_id='invalid-reason',
        command=command_name,
        arguments=arguments,
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )

    def durable_rows() -> tuple[tuple[str, ...], ...]:
        snapshots: list[tuple[str, ...]] = []
        for path in (config.daemon.state_dir / 'supervisor.db', group_path, source_path):
            with closing(sqlite3.connect(path)) as connection, connection:
                snapshots.append(tuple(connection.iterdump()))
        return tuple(snapshots)

    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=_ScriptedWorkerFactory(),
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        before = durable_rows()
        try:
            with pytest.raises(ValueError, match='reason must be nonempty text'):
                supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        finally:
            assert durable_rows() == before


def test_supervisor_routes_and_correlates_operator_command_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='supervisor-operator-1',
        command='park',
        arguments={
            'site': 'test',
            'group': 'remote',
            'reason': 'maintenance',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        owner = next(
            worker for worker in factory._instances.values() if 'remote' in worker._spec.groups
        )
        owner._messages.append(
            CommandResult(
                command='park',
                ok=True,
                message='stale',
                request_id='stale-request',
                result={'revision': 7},
            )
        )
        owner._operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'revision': 8, 'status': 'parked'},
        )

        result = supervisor.dispatch_operator_command(command, timeout=1.0)

    assert len(owner._sent_commands) == 1
    sent = owner._sent_commands[0]
    assert sent.request_id != command.request_id
    assert sent.command == command.command
    assert sent.arguments == command.arguments
    assert sent.identity == command.identity
    assert result.request_id == command.request_id
    assert result.result == {'revision': 8, 'status': 'parked'}


@pytest.mark.parametrize('wrapped', [False, True])
@pytest.mark.parametrize('wait_fails', [False, True])
def test_supervisor_run_caps_wait_and_unbinds_progress(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    wrapped: bool,
    wait_fails: bool,
) -> None:
    config_path, loaded = _source_only_config(tmp_path, monkeypatch)
    config = replace(
        loaded,
        daemon=replace(loaded.daemon, supervisor_poll_interval_secs=0.025),
    )
    factory = _ScriptedWorkerFactory()
    observed = _ObservedForegroundWait(fails=wait_fails)
    wait: Callable[[float], None] = (
        (lambda timeout: observed.serve_once(timeout=timeout)) if wrapped else observed.serve_once
    )
    with Supervisor(config=config, config_path=config_path, worker_factory=factory) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()

        if wait_fails:
            with pytest.raises(RuntimeError, match='wait failed'):
                supervisor.run(
                    should_stop=lambda: bool(observed._calls),
                    wait=wait,
                    set_foreground_progress=observed.set_foreground_progress,
                )
        else:
            supervisor.run(
                should_stop=lambda: bool(observed._calls),
                wait=wait,
                set_foreground_progress=observed.set_foreground_progress,
            )

    assert observed._calls == [pytest.approx(0.025)]
    assert len(observed._bindings) == 2
    assert observed._bindings[0] is not None
    assert observed._bindings[1] is None


def test_supervisor_run_timeout_wake_does_not_advance_progress(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    waits: list[float] = []
    advances: list[float] = []
    progress_count_at_wait: int | None = None

    def wait(timeout: float) -> bool:
        nonlocal progress_count_at_wait
        waits.append(timeout)
        progress_count_at_wait = len(advances)
        clock.advance(timeout)
        return False

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        advance_progress = cast(
            'Callable[[], None]',
            vars(Supervisor)['_advance_progress'].__get__(supervisor),
        )

        def observe_progress() -> None:
            advances.append(clock())
            advance_progress()

        monkeypatch.setattr(supervisor, '_advance_progress', observe_progress)
        supervisor.run(
            should_stop=lambda: bool(waits),
            wait=wait,
            set_foreground_progress=lambda _callback: None,
        )
        clock._now = 10.05

        result = supervisor.probe_result()

        assert len(waits) == 1
        assert waits[0] <= config.daemon.supervisor_poll_interval_secs
        assert progress_count_at_wait == len(advances)
        assert not result.liveness


def test_supervisor_serializes_shared_source_grants_across_pending_dependents(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _two_dependent_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'grant-owner-{token_index}'

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        source._authentication_ready = True
        supervisor._tick()
        dependents = tuple(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        assert len(dependents) == 2
        source._messages_before_grant.append(_health_update(source, heartbeat_at=clock()))
        for dependent in dependents:
            dependent._authentication_ready = True

        supervisor._tick()

        assert len(source._sent_grants) == 2
        assert not any(dependent._killed for dependent in dependents)
        first, second = source._sent_grants
        assert source._operations.index(
            f'receive:grant-ready:{first.group}'
        ) < source._operations.index(f'grant:{second.group}')
        assert (
            next(
                worker
                for worker in dependents
                if worker._spawn_instance == first.dependent_spawn_instance
            )._assignment
            is not None
        )
        assert (
            next(
                worker
                for worker in dependents
                if worker._spawn_instance == second.dependent_spawn_instance
            )._assignment
            is None
        )

        supervisor._tick()

        assert len(source._sent_grants) == 2
        assert all(dependent._assignment is not None for dependent in dependents)


def test_supervisor_source_exit_tolerates_delayed_dependent_grant_reap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'delayed-reap-{token_index}'

    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        assert len(source._sent_grants) == 1
        assert dependent._assignment is None
        dependent._wait_results.extend((None, None, -9))
        source._exit_status = 1

        supervisor._tick()

        source_workers = cast(
            'dict[tuple[str, str], _ScriptedWorker]',
            vars(supervisor)['_source_workers'],
        )
        assert ('test', 'admin') not in source_workers
        assert dependent._killed
        assert (
            supervisor._spawns[dependent._worker_id]._termination_reason
            == 'source-owner-unavailable'
        )
        assert dependent._worker_id in supervisor._spawns
        assert supervisor._spawns[dependent._worker_id]._lifecycle is not None
        operations_after_source_exit = len(factory._operations)

        supervisor._tick()

        assert dependent._worker_id in supervisor._spawns
        assert supervisor._spawns[dependent._worker_id]._lifecycle is not None
        assert factory._operations[operations_after_source_exit:] == []
        assert len(source._sent_grants) == 1
        assert dependent._assignment is None

        supervisor._tick()

        restart_deadlines = cast(
            'dict[str, float]',
            vars(supervisor)['_restart_deadlines'],
        )
        assert dependent._worker_id not in supervisor._spawns
        assert restart_deadlines[dependent._worker_id] == clock()
        assert len(factory._history[dependent._worker_id]) == 1

        factory._authentication_ready = True
        clock.advance(1.0)
        supervisor._tick()
        supervisor._tick()
        supervisor._tick()

        replacement = factory._instances[dependent._worker_id]
        replacement_source = next(
            worker for worker in factory._instances.values() if worker._spec.sources
        )
        assert replacement is not dependent
        assert replacement_source is not source
        assert replacement._assignment is not None
        assert replacement._assignment.spawn_instance != dependent._spawn_instance
        assert replacement._assignment.event_grants == tuple(replacement_source._sent_grants)
        assert dependent._assignment is None


def test_supervisor_source_startup_health_failure_uses_watchdog_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    supervisor = Supervisor(config=config, config_path=config_path, worker_factory=factory)
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        source._auto_endpoint_ready = False
        source._authentication_ready = True
        supervisor._tick()
        assert not _spawn_lifecycle(supervisor, source._worker_id)._ready
        source._messages.append(
            replace(
                _health_update(source, heartbeat_at=0.0),
                worker=replace(
                    _health_update(source, heartbeat_at=0.0).worker,
                    site='wrong-site',
                ),
            )
        )
        monkeypatch.setattr(source, '_wait', _never_reap)

        supervisor._tick()

        assert source._killed
        assert supervisor._spawns[source._worker_id]._termination_reason == (
            'watchdog-sampling-failed'
        )
    finally:
        supervisor._close()


def test_supervisor_source_state_uses_configured_obligation_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    config = replace(config, daemon=replace(config.daemon, max_group_obligations=17))
    _prepare_absent_orphan(config)
    supervisor = Supervisor(config=config, config_path=config_path)
    with ExitStack() as stack:
        sources = supervisor._open_reconciliation_sources(stack, {('test', 'admin')})
        assert sources[('test', 'admin')]._settings.max_group_obligations == 17


def test_supervisor_source_watchdog_uses_trusted_deadline_at_equality(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        source_detail = replace(
            _idle_source_health(source, source='admin', heartbeat_at=0.0),
            phase='provider-query',
            deadline_at=10_000.0,
        )
        watchdogs = (
            _source_watchdog(
                source,
                deadline_at=10_000.0,
                heartbeat_at=0.0,
                phase='provider-query',
            ),
            WatchdogHealth(
                kind='operator',
                site='test',
                source=None,
                group=None,
                worker_instance=source._spawn_instance,
                phase='idle',
                heartbeat_at=0.0,
                deadline_at=None,
            ),
        )
        source._messages.append(
            _health_update(
                source,
                heartbeat_at=clock(),
                sources=(source_detail,),
                watchdogs=watchdogs,
            )
        )
        dependent._messages.append(_health_update(dependent, heartbeat_at=clock()))
        supervisor._tick()

        credential = config.sites['test'].credentials['admin']
        trusted_budget = internal_stall_deadline_secs(
            config.daemon.health_interval_secs,
            credential,
        )
        supervisor._spawns[source._worker_id]._health_deadline = 10_000.0
        clock.advance(trusted_budget)
        source._messages.append(
            _health_update(
                source, heartbeat_at=clock() - 0.001, sources=(source_detail,), watchdogs=watchdogs
            )
        )
        dependent._messages.append(_health_update(dependent, heartbeat_at=clock()))
        supervisor._tick()
        assert not source._killed
        assert _spawn_health(supervisor, source._worker_id).sources[0].overdue_reason is None
        source._messages.append(
            _health_update(
                source, heartbeat_at=clock(), sources=(source_detail,), watchdogs=watchdogs
            )
        )
        dependent._messages.append(_health_update(dependent, heartbeat_at=clock()))
        monkeypatch.setattr(source, '_wait', _never_reap)

        supervisor._tick()

        assert source._killed
        assert not dependent._killed
        assert supervisor._spawns[source._worker_id]._termination_reason == (
            'source-phase-overdue'
        )
        accepted = _spawn_health(supervisor, source._worker_id)
        assert accepted.watchdogs[0].deadline_at == trusted_budget
        assert accepted.sources[0].deadline_at == trusted_budget
        assert accepted.sources[0].heartbeat_age == trusted_budget
        assert accepted.sources[0].overdue_reason == 'source-phase-overdue'
    finally:
        supervisor._close()


def test_supervisor_starts_source_then_installs_grant_before_dependent_assignment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    tokens = iter(
        (
            'source-generation-1',
            'source-spawn-1',
            'dependent-spawn-1',
            'dependent-capability-1',
        )
    )
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=lambda: next(tokens),
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()

        assert factory._operations == [
            'spawn:site',
            'assign:site',
            'receive:endpoint:admin',
            'spawn:named',
            'grant:remote',
            'receive:grant-ready:remote',
            'assign:named',
        ]
        source, dependent = build_worker_specs(config.sites['test'])
        source_assignment = factory._instances[source.worker_id]._assignment
        dependent_assignment = factory._instances[dependent.worker_id]._assignment
        assert source_assignment is not None
        assert source_assignment.group_assignments == ()
        assert source_assignment.source_group_assignments == (
            ActiveSourceGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='initialize',
            ),
        )
        assert dependent_assignment is not None
        assert dependent_assignment.group_assignments == (
            DeliveryGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='initialize',
            ),
        )
        assert dependent_assignment.event_grants == (
            EventPeerGrant(
                source='admin',
                group='remote',
                dependent_spawn_instance='dependent-spawn-1',
                capability='dependent-capability-1',
            ),
        )
        with pytest.raises(RuntimeError, match='cannot run after worker spawn'):
            supervisor._reconcile(
                generations={},
            )
    finally:
        supervisor._close()


def test_supervisor_startup_authentication_and_readiness_advance_nonblockingly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'dependent-capability-1',
            )
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )

        supervisor.start()

        assert factory._operations == ['spawn:site']
        source = next(iter(factory._instances.values()))
        assert source._assignment is None
        assert source._receive_timeouts == []

        factory._authentication_ready = True
        source._authentication_ready = True
        supervisor._tick()
        supervisor._tick()

        assert factory._operations == [
            'spawn:site',
            'assign:site',
            'receive:endpoint:admin',
            'spawn:named',
            'grant:remote',
            'receive:grant-ready:remote',
            'assign:named',
        ]
        assert all(
            timeout == 0.0
            for worker in factory._instances.values()
            for timeout in worker._receive_timeouts
        )
    finally:
        supervisor._close()


@pytest.mark.parametrize(
    ('control_stage', 'error_factory'),
    [
        ('authenticate', lambda: BrokenPipeError('simulated broken control pipe')),
        ('assign', lambda: ConnectionResetError('simulated reset control channel')),
        ('receive', lambda: OSError(errno.EIO, 'simulated control input failure')),
    ],
)
def test_supervisor_startup_control_oserror_enters_normal_restart_handling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    control_stage: Literal['authenticate', 'assign', 'receive'],
    error_factory: Callable[[], OSError],
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'control-disconnect-{token_index}'

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=token,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(iter(factory._instances.values()))
        source._wait_results.append(None)
        source._authentication_ready = True
        if control_stage == 'authenticate':

            def fail_authentication(*, timeout: float = 0.0) -> bool:
                assert timeout == 0.0
                raise error_factory()

            monkeypatch.setattr(source, '_authenticate', fail_authentication)
        elif control_stage == 'assign':

            def fail_assignment(_assignment: WorkerAssignment) -> None:
                raise error_factory()

            monkeypatch.setattr(source, '_assign', fail_assignment)
        else:

            def fail_receive(*, timeout: float | None = None) -> ControlMessage:
                assert timeout == 0.0
                raise error_factory()

            monkeypatch.setattr(source, '_receive', fail_receive)

        assert supervisor._tick()

        assert source._killed
        expected_reason = (
            'worker-control-send-failed' if control_stage == 'assign' else 'worker-control-eof'
        )
        assert supervisor._spawns[source._worker_id]._termination_reason == expected_reason
        assert source._worker_id in supervisor._spawns

        assert supervisor._tick()

        restart_deadlines = vars(supervisor)['_restart_deadlines']
        assert source._worker_id not in supervisor._spawns
        assert restart_deadlines[source._worker_id] == 1.0

        factory._authentication_ready = True
        clock.advance(1.0)
        assert supervisor._tick()
        assert factory._instances[source._worker_id] is not source
    finally:
        supervisor._close()


@pytest.mark.parametrize('deadline_offset', [0.0, 0.001])
def test_supervisor_startup_deadline_precedes_queued_authentication_and_health(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    deadline_offset: float,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=iter(('source-generation-1', 'source-spawn-1')).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        source = next(iter(factory._instances.values()))
        source._authentication_calls = 0
        source._authentication_ready = True
        health = _health_update(source, heartbeat_at=clock())
        source._messages.append(health)
        clock.advance(config.daemon.worker_startup_deadline_secs + deadline_offset)

        supervisor._tick()

        assert source._killed
        assert source._assignment is None
        assert source._authentication_calls == 0
        assert tuple(source._messages) == (health,)
    finally:
        supervisor._close()


def test_supervisor_startup_deadline_precedes_queued_grant_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    checkpoint_calls: list[object] = []

    def checkpoint(connection: object) -> bool:
        checkpoint_calls.append(connection)
        return True

    def checkpointer_factory(
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        clock: Callable[[], float],
    ) -> Checkpointer:
        def run_checkpoint() -> tuple[int, int, int]:
            checkpoint(connection)
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            interval_secs=interval_secs,
            clock=clock,
            run_checkpoint=run_checkpoint,
        )

    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        checkpointer_factory=checkpointer_factory,
        worker_factory=factory,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        source = next(iter(factory._instances.values()))
        source._authentication_ready = True
        supervisor._tick()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        source._auto_grant_ready = False
        dependent._authentication_ready = True
        supervisor._tick()
        grant = source._sent_grants[-1]
        ready = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=grant.dependent_spawn_instance,
        )
        source._messages.append(ready)
        kill_reasons: list[str] = []
        kill = dependent._kill

        def record_kill() -> None:
            reason = supervisor._spawns[dependent._worker_id]._termination_reason
            assert reason is not None
            kill_reasons.append(reason)
            kill()

        monkeypatch.setattr(dependent, '_kill', record_kill)
        clock.advance(config.daemon.worker_startup_deadline_secs)

        supervisor._tick()

        assert dependent._killed
        assert dependent._assignment is None
        assert not source._messages
        assert ready not in supervisor._stale_grant_readies
        assert kill_reasons == ['worker-startup-timeout']
        assert len(checkpoint_calls) == 1
    finally:
        supervisor._close()


def test_supervisor_startup_does_not_normalize_noncontrol_oserror(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=iter(('source-generation-1', 'source-spawn-1')).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(iter(factory._instances.values()))
        source._authentication_ready = True

        def fail_registration(_worker: _ScriptedWorker) -> None:
            raise OSError(errno.EACCES, 'simulated supervisor state failure')

        monkeypatch.setattr(supervisor, '_register_worker', fail_registration)

        with pytest.raises(OSError, match='supervisor state failure'):
            supervisor._tick()

        assert not source._killed
        assert supervisor._spawns[source._worker_id]._termination_reason is None
    finally:
        supervisor._close()


def test_supervisor_startup_health_does_not_extend_fixed_endpoint_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory(authentication_ready=False)
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=iter(('source-generation-1', 'source-spawn-1')).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        source = next(iter(factory._instances.values()))
        source._auto_endpoint_ready = False
        source._authentication_ready = True
        supervisor._tick()
        assert source._assignment is not None
        source_name, source_generation = source._assignment.source_generations[0]
        endpoint = SourceEndpointReady(
            source=source_name,
            source_generation=source_generation,
            path=os.fspath(Path.cwd() / f'{source_name}.sock'),
        )
        source._messages.append(_health_update(source, heartbeat_at=clock()))
        supervisor._tick()
        assert _spawn_health(supervisor, source._worker_id).worker.monitor_alive
        kills: list[str] = []
        kill = source._kill

        def assert_unhealthy_before_kill() -> None:
            health = _spawn_health(supervisor, source._worker_id)
            assert health.worker.spawn_instance == source._spawn_instance
            assert not health.worker.monitor_alive
            kills.append(source._spawn_instance)
            kill()

        monkeypatch.setattr(source, '_kill', assert_unhealthy_before_kill)
        source._messages.append(endpoint)
        clock.advance(config.daemon.worker_startup_deadline_secs)

        supervisor._tick()

        assert source._killed
        assert tuple(source._messages) == (endpoint,)
        assert kills == [source._spawn_instance]
        assert not supervisor._park_evidence_by_group
        assert not any(operation.startswith('spawn:named') for operation in factory._operations)
    finally:
        supervisor._close()


def test_supervisor_state_adopts_the_greatest_durable_group_generation(
    tmp_path: Path,
) -> None:
    with _SupervisorState._open(tmp_path / 'state') as state:
        assignments = state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 7},
        )

        assert assignments == {
            ('test', 'remote'): ActiveSourceGroupAssignment(
                source='admin',
                group='remote',
                group_generation=7,
                start_mode='resume',
            )
        }
        with transaction(state._database.connection):
            assert (
                _allocate_generation(
                    state._database.connection,
                    site='test',
                    group='remote',
                    observed_generations=(4, 7, 9),
                )
                == 10
            )


def test_supervisor_state_normalizes_breaker_timing_only_after_boot_change(
    tmp_path: Path,
) -> None:
    clock = _FakeClock()
    clock.advance(1_000.0)
    state_dir = tmp_path / 'state'
    with _SupervisorState._open(state_dir) as state:
        breaker = state._restore_worker_breaker(
            worker_id='dependent',
            restart_limit=3,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
            boot_id='boot-a',
        )
        assert breaker._record_failure('first failure') == 1.0
        state._persist_worker_breaker(breaker)

    clock.advance(0.25)
    with _SupervisorState._open(state_dir) as state:
        same_boot = state._restore_worker_breaker(
            worker_id='dependent',
            restart_limit=3,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
            boot_id='boot-a',
        )
        assert same_boot._projection._failure_times == (1_000.0,)
        assert same_boot._restart_deadline_at == 1_001.0

    clock._now = 5.0
    with _SupervisorState._open(state_dir) as state:
        rebooted = state._restore_worker_breaker(
            worker_id='dependent',
            restart_limit=3,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
            boot_id='boot-b',
        )
        assert rebooted._projection._failure_times == (5.0,)
        assert rebooted._restart_deadline_at == 5.0
        assert rebooted._record_failure('second failure') == 2.0
        assert rebooted._projection._failure_times == (5.0, 5.0)
        state._persist_worker_breaker(rebooted)

    clock.advance(0.5)
    with _SupervisorState._open(state_dir) as state:
        same_new_boot = state._restore_worker_breaker(
            worker_id='dependent',
            restart_limit=3,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
            boot_id='boot-b',
        )
        assert same_new_boot._projection._failure_times == (5.0, 5.0)
        assert same_new_boot._restart_deadline_at == 7.0


def test_supervisor_state_persists_initialize_then_reconciles_resume(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    with _SupervisorState._open(state_dir) as state:
        first = state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={},
        )

    with _SupervisorState._open(state_dir) as state:
        resumed = state._reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 1},
        )

    assert first == {
        ('test', 'remote'): ActiveSourceGroupAssignment(
            source='admin',
            group='remote',
            group_generation=1,
            start_mode='initialize',
        )
    }
    assert resumed == {
        ('test', 'remote'): ActiveSourceGroupAssignment(
            source='admin',
            group='remote',
            group_generation=1,
            start_mode='resume',
        )
    }


def test_supervisor_state_restores_complete_worker_breaker_authority(
    tmp_path: Path,
) -> None:
    clock = _FakeClock()
    state_dir = tmp_path / 'state'
    with _SupervisorState._open(state_dir) as state:
        breaker = _WorkerBreaker(
            worker_id='dependent',
            restart_limit=2,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert breaker._record_failure('first failure') == 1.0
        state._persist_worker_breaker(breaker)

    clock.advance(0.5)
    with _SupervisorState._open(state_dir) as state:
        restored = state._restore_worker_breaker(
            worker_id='dependent',
            restart_limit=2,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert len(restored._failure_times) == 1
        assert restored._restart_deadline_at == 1.0
        assert restored._failure_total == 1
        assert restored._revision == 0
        assert restored._history == ()
        assert restored._record_failure('second failure') == 2.0
        state._persist_worker_breaker(restored)

    clock.advance(2.0)
    with _SupervisorState._open(state_dir) as state:
        restored = state._restore_worker_breaker(
            worker_id='dependent',
            restart_limit=2,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert len(restored._failure_times) == 2
        assert restored._restart_deadline_at == 2.5
        assert restored._failure_total == 2
        assert restored._record_failure('third failure') is None
        state._persist_worker_breaker(restored)

    with _SupervisorState._open(state_dir) as state:
        restored = state._restore_worker_breaker(
            worker_id='dependent',
            restart_limit=2,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert restored._is_open
        assert restored._restart_deadline_at is None
        assert restored._failure_total == 3
        assert restored._revision == 1
        assert restored._transition_count == 1
        assert restored._history == (
            supervisor_module.BreakerTransition(
                sequence=1,
                worker_id='dependent',
                state='open',
                reason='third failure',
            ),
        )


def test_supervisor_state_retains_existing_unresolved_group_evidence(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    original = _park_evidence()
    repacked = _park_evidence(
        evidence_id='evidence-2',
        failed_worker_id='repacked-worker',
        failed_spawn_instance='repacked-spawn-1',
        event_id=43,
        group_revision=8,
        observed_at_wall_clock=20.0,
    )

    with _SupervisorState._open(state_dir) as state:
        assert state._record_park_evidence(original)
        assert not state._record_park_evidence(repacked)
        assert state._pending_park_evidence() == (original,)

    with _SupervisorState._open(state_dir) as state:
        assert state._pending_park_evidence() == (original,)
        state._retire_park_evidence(original._evidence_id)
        assert state._pending_park_evidence() == ()


def test_supervisor_state_retains_latest_100_durable_breaker_transitions(
    tmp_path: Path,
) -> None:
    clock = _FakeClock()
    state_dir = tmp_path / 'state'
    identity = AuditIdentity.current()
    with _SupervisorState._open(state_dir) as state:
        breaker = _WorkerBreaker(
            worker_id='dependent',
            restart_limit=0,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        for index in range(101):
            assert breaker._record_failure(f'failure {index}') is None
            state._persist_worker_breaker(breaker)
            state._reset_worker_breaker(
                breaker,
                expected_revision=breaker._revision,
                reason=f'reset {index}',
                identity=identity,
                wall_clock_time=float(index),
            )

    with _SupervisorState._open(state_dir) as state:
        restored = state._restore_worker_breaker(
            worker_id='dependent',
            restart_limit=0,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )

    assert not restored._is_open
    assert restored._failure_total == 101
    assert restored._revision == 202
    assert restored._transition_count == 202
    assert len(restored._history) == 100
    assert restored._history[0].sequence == 103
    assert restored._history[-1].sequence == 202


def test_supervisor_state_routes_pending_evidence_by_group_not_failed_worker(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    later_group = _park_evidence(
        evidence_id='evidence-later-group',
        site='zeta',
        group='lighting',
        failed_worker_id='old-default-worker',
    )
    earlier_group = _park_evidence(
        evidence_id='evidence-earlier-group',
        site='alpha',
        group='editorial',
        failed_worker_id='old-dedicated-worker',
    )
    with _SupervisorState._open(state_dir) as state:
        assert state._record_park_evidence(later_group)
        assert state._record_park_evidence(earlier_group)

    with _SupervisorState._open(state_dir) as state:
        assert state._pending_park_evidence() == (earlier_group, later_group)

    with closing(sqlite3.connect(state_dir / 'supervisor.db')) as inspection, inspection:
        assert tuple(
            str(row[1]) for row in inspection.execute('PRAGMA table_info(park_evidence)')
        ) == (
            'evidence_id',
            'site',
            'group_name',
            'group_generation',
            'failed_worker_id',
            'failed_spawn_instance',
            'event_id',
            'group_revision',
            'observed_at_wall_clock',
        )


def test_supervisor_status_aggregates_only_active_owner_telemetry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    state_dir = config.daemon.state_dir
    source_path = state_dir / 'sites' / 'test' / 'sources' / 'admin.db'
    state_dir.mkdir(mode=0o700)
    source_path.parent.mkdir(parents=True, mode=0o700)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        with (
            _lifecycle_source(source_path) as source,
            _lifecycle_group(source_path) as active,
            _lifecycle_group(source_path, group='inactive') as inactive,
        ):
            for group in (active, inactive):
                group.park(
                    reason='audit count coverage',
                    identity=AuditIdentity.current(),
                    wall_clock_time=10.0,
                    expected_revision=group.snapshot().revision,
                )
            source.initialize(100, incarnation='incarnation-1')
            active.dispose(
                DispositionRequest(
                    command='reset-to-latest',
                    group='remote',
                    disposition_id='status-reset',
                    reserved_generation=2,
                    expected_revision=active.snapshot().revision,
                    from_event_id=None,
                    acknowledge_truncation=False,
                    reason='audit count coverage',
                    identity=AuditIdentity.current(),
                    wall_clock_time=11.0,
                )
            )
            Supervisor._orphan_group(source, inactive)
            inactive.change_lifecycle(
                LifecycleRequest(
                    command='readd',
                    expected_lifecycle='orphaned',
                    expected_generation=1,
                    transition_generation=2,
                    identity=AuditIdentity.current(),
                    wall_clock_time=12.0,
                )
            )
            _adopt_lifecycle_stream(
                source,
                adoption_id='status-adoption',
                command_fingerprint='status-adoption',
                reason='audit count coverage',
            )
            # Source: reset receipt and adoption. Groups: bootstrap, park,
            # reset (reported), and bootstrap, park, orphan, re-add (unreported).
            assert source.audit_count() == 2
            assert active.audit_count() == 3
            assert inactive.audit_count() == 4
            source_audit_count = source.audit_count()
            group_audit_count = active.audit_count()
            source_size = source.database_size_bytes()
        state = cast('_SupervisorState', vars(supervisor)['_state'])
        database = cast('DatabaseHandle', vars(state)['_database'])
        database.connection.execute(
            """
            INSERT INTO breaker_audit (
                worker_id, prior_revision, new_revision, command, reason,
                wall_clock_time, local_uid, username, pid
            ) VALUES (
                'worker-1', 1, 2, 'reset-worker-breaker', 'reviewed',
                10.0, 1, 'operator', 2
            )
            """
        )
        source_owner = next(
            worker for worker in factory._instances.values() if worker._spec.sources
        )
        group_owner = next(worker for worker in factory._instances.values() if worker._spec.groups)
        source_owner._messages.append(
            _health_update(
                source_owner,
                heartbeat_at=clock(),
                sources=(
                    SourceHealth(
                        site='test',
                        source='admin',
                        source_generation='source-generation-1',
                        status='running',
                        frontier=100,
                        successful_clock=clock(),
                        retry_attempt=0,
                        next_retry_secs=None,
                        open_range_count=0,
                        late_id_count=0,
                        last_error=None,
                        worker_instance=source_owner._spawn_instance,
                        initialized=True,
                        stream_incarnation='incarnation-1',
                        audit_count=source_audit_count,
                        database_size_bytes=source_size,
                        phase='wait',
                        heartbeat_at=clock(),
                        deadline_at=clock() + 1.25,
                    ),
                ),
            )
        )
        group_owner._messages.append(
            _health_update(
                group_owner,
                heartbeat_at=clock(),
                groups=(
                    replace(
                        _in_flight_group_health(group_owner, heartbeat_at=clock()),
                        current_event_id=None,
                        current_plugin_id=None,
                        current_callback_id=None,
                        delivery_attempt=None,
                        phase='idle',
                        audit_count=group_audit_count,
                    ),
                ),
            )
        )
        supervisor._tick()
        supervisor_path = state_dir / 'supervisor.db'
        supervisor_size = sum(
            candidate.stat().st_size
            for candidate in (
                supervisor_path,
                Path(f'{supervisor_path}-wal'),
                Path(f'{supervisor_path}-shm'),
            )
            if candidate.exists()
        )

        status = supervisor.status_mapping({})

    assert status['audit_count'] == 6
    assert dict(cast('tuple[tuple[str, int], ...]', status['database_sizes'])) == {
        'supervisor.db': supervisor_size,
        'sites/test/sources/admin.db': source_size,
    }
    assert 'sites/test/groups/inactive.db' not in dict(
        cast('tuple[tuple[str, int], ...]', status['database_sizes'])
    )


def test_supervisor_status_detail_refresh_does_not_bypass_probe_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        clock.advance(1.24)
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        detail = _idle_source_health(source, source='admin', heartbeat_at=0.0)
        source._messages.append(
            _health_update(
                source,
                heartbeat_at=clock(),
                sources=(detail,),
                watchdogs=(
                    _source_watchdog(
                        source,
                        heartbeat_at=0.0,
                        deadline_at=1.25,
                        phase='wait',
                    ),
                    WatchdogHealth(
                        kind='operator',
                        site='test',
                        source=None,
                        group=None,
                        worker_instance=source._spawn_instance,
                        phase='idle',
                        heartbeat_at=clock(),
                        deadline_at=None,
                    ),
                ),
            )
        )
        dependent._messages.append(_health_update(dependent, heartbeat_at=clock()))
        supervisor._tick()
        clock.advance(0.01)

        status = supervisor.status_mapping({})

    assert status['readiness'] is True
    assert status['conditions'] == ('idle',)
    assert status['primary_state'] == 'idle'
    sources = cast('tuple[Mapping[str, object], ...]', status['sources'])
    assert sources[0]['deadline_at'] == 1.25
    assert sources[0]['overdue_reason'] is None
    assert sources[0]['heartbeat_age'] == 1.25


def test_supervisor_status_uses_published_probe_gates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        supervisor._tick()

        probe = supervisor.probe_result()
        status = supervisor.status_mapping({})

    assert status['liveness'] is probe.liveness
    assert status['readiness'] is probe.readiness
    assert status['conditions'] == probe.conditions


def test_supervisor_stays_out_of_worker_data_plane(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_data_plane(*_args: object, **_kwargs: object) -> None:
        pytest.fail('supervisor entered a worker-owned data-plane boundary')

    monkeypatch.setattr(GroupState, 'resolve_conditional_park', fail_data_plane)
    monkeypatch.setattr(providers_module.Provider, 'create_async', fail_data_plane)
    monkeypatch.setattr(providers_module.Provider, 'create_sync', fail_data_plane)
    monkeypatch.setattr(ipc_module, '_decode_event_delivery', fail_data_plane)
    monkeypatch.setattr(worker_module, '_DeliveryFloorChannel', fail_data_plane)
    monkeypatch.setattr(worker_module, '_SourceJoinSurface', fail_data_plane)
    monkeypatch.setattr(worker_module, 'build_worker_runtime', fail_data_plane)
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    tokens = iter(
        (
            'source-generation-1',
            'source-spawn-1',
            'dependent-spawn-1',
            'dependent-capability-1',
        )
    )

    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=lambda: next(tokens),
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()

    assert factory._operations == [
        'spawn:site',
        'assign:site',
        'receive:endpoint:admin',
        'spawn:named',
        'grant:remote',
        'receive:grant-ready:remote',
        'assign:named',
        'shutdown:named:supervisor-close',
        'close:named',
        'shutdown:site:supervisor-close',
        'close:site',
    ]


@pytest.mark.parametrize('lock_kind', ['source', 'group', 'retained-group', 'write-lock'])
def test_supervisor_takeover_is_bounded_while_a_worker_lock_remains(
    lock_kind: Literal['source', 'group', 'retained-group', 'write-lock'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, original = _source_only_config(tmp_path, monkeypatch)
    config = replace(
        original,
        daemon=replace(
            original.daemon,
            supervisor_poll_interval_secs=0.01,
            worker_startup_deadline_secs=0.05,
        ),
    )
    state_dir = ensure_secure_state_dir(config.daemon.state_dir)
    sites = ensure_secure_state_dir(state_dir / 'sites')
    site = ensure_secure_state_dir(sites / 'test')
    if lock_kind == 'source':
        component_dir = ensure_secure_state_dir(site / 'sources')
        held_path = component_dir / 'admin.db.lock'
    elif lock_kind == 'write-lock':
        component_dir = ensure_secure_state_dir(site / 'sources')
        held_path = component_dir / 'admin.db.write-lock'
    else:
        component_dir = ensure_secure_state_dir(site / 'groups')
        held_path = component_dir / (
            'removed.lock' if lock_kind == 'retained-group' else 'remote.lock'
        )
    ready_read, ready_write = os.pipe()
    process: ChildProcess | None = None
    supervisor: Supervisor | None = None
    try:
        process = spawn_child(
            [
                sys.executable,
                '-c',
                """
import os
import sys
import time
from pathlib import Path
from sg_event_daemon._platform import acquire_lock, secure_open_regular_file, try_flock_exclusive

if sys.argv[3] == 'write-lock':
    fd, _ = secure_open_regular_file(Path(sys.argv[1]))
    try:
        assert try_flock_exclusive(fd)
        os.write(int(sys.argv[2]), b'L')
        time.sleep(30)
    finally:
        os.close(fd)
else:
    with acquire_lock(Path(sys.argv[1])):
        os.write(int(sys.argv[2]), b'L')
        time.sleep(30)
""",
                os.fspath(held_path),
                str(ready_write),
                lock_kind,
            ],
            pass_fds=(ready_write,),
            env=_child_environment(),
        )
        os.close(ready_write)
        ready_write = -1
        supervisor = Supervisor(config=config, config_path=config_path)
        assert _read_pipe(ready_read, 1) == b'L'
        with pytest.raises(TimeoutError, match=rf'{held_path.name}'):
            supervisor._open()
    finally:
        if supervisor is not None:
            supervisor._close()
        os.close(ready_read)
        if ready_write >= 0:
            os.close(ready_write)
        if process is not None:
            terminate_child(process, force=True)
            assert reap_child(process, timeout=5.0) is not None

    with Supervisor(config=config, config_path=config_path):
        pass


@pytest.mark.parametrize(
    'missing',
    [
        pytest.param('worker', id='worker'),
        pytest.param('source', id='source'),
        pytest.param('group', id='group'),
        pytest.param('first-health', id='first-health'),
    ],
)
def test_probe_snapshot_marks_incomplete_inventory_stale(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    missing: str,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        source_update = _health_update(source, heartbeat_at=0.0)
        dependent_update = _health_update(dependent, heartbeat_at=0.0)
        updates = {
            source._worker_id: source_update,
            dependent._worker_id: dependent_update,
        }
        complete = {source._worker_id, dependent._worker_id}
        if missing == 'worker':
            updates = {
                source._worker_id: replace(source_update, groups=dependent_update.groups),
            }
        elif missing == 'source':
            updates[source._worker_id] = replace(source_update, sources=())
        elif missing == 'group':
            updates[dependent._worker_id] = replace(dependent_update, groups=())
        else:
            complete.remove(source._worker_id)
        for worker_id, spawn in supervisor._spawns.items():
            spawn._health = updates.get(worker_id)
            spawn._health_inventory_complete = worker_id in complete
        build_snapshot = cast(
            'Callable[[float], ProbeSnapshot]',
            vars(Supervisor)['_probe_snapshot'].__get__(supervisor),
        )

        snapshot = build_snapshot(0.0)

    assert not snapshot.complete
    assert not snapshot.ready
    assert snapshot.conditions[0] == 'stale'
    assert 'healthy' not in snapshot.conditions


def test_supervisor_tick_discards_health_from_an_old_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        stale = replace(
            _health_update(dependent, heartbeat_at=1.0).worker,
            spawn_instance='old-dependent-spawn',
        )
        dependent._messages.append(
            HealthUpdate(
                worker=stale,
                watchdogs=(),
                sources=(),
                groups=(),
                event_sockets=(),
            )
        )

        supervisor._tick()

        assert supervisor._spawns[dependent._worker_id]._health is None
        assert not dependent._killed
    finally:
        supervisor._close()


def test_supervisor_tick_discards_late_operator_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        owner = next(
            worker for worker in factory._instances.values() if 'remote' in worker._spec.groups
        )
        owner._messages.append(
            CommandResult(
                command='park',
                ok=True,
                message='late',
                request_id='unowned-transport-1',
                result={'revision': 8, 'status': 'parked'},
            )
        )

        supervisor._tick()

        assert supervisor._halt_reason_value is None
        assert not owner._killed

        owner._messages.append(
            SourceEndpointReady(
                source='admin',
                source_generation='source-generation-1',
                path=os.fspath(tmp_path / 'late-source.sock'),
            )
        )
        with pytest.raises(TypeError, match='SourceEndpointReady'):
            supervisor._tick()


def test_supervisor_tick_does_not_attribute_worker_staleness_to_fresh_lane(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        group_path = source_database(config.daemon.state_dir)
        clock.advance(1.0)
        group_health = _in_flight_group_health(dependent, heartbeat_at=clock())
        for worker in factory._instances.values():
            worker._messages.append(
                _health_update(
                    worker,
                    heartbeat_at=0.0 if worker is dependent else clock(),
                    groups=(group_health,) if worker is dependent else (),
                )
            )

        with _conditional_group(group_path):
            supervisor._tick()

        assert dependent._killed
        assert supervisor._park_evidence_by_group == {}
        assert len(factory._history[dependent._worker_id]) == 1
    finally:
        supervisor._close()


def test_supervisor_tick_nonblockingly_ingests_all_pending_worker_health(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        workers = tuple(factory._instances.values())
        updates = tuple(_health_update(worker, heartbeat_at=0.0) for worker in workers)
        for worker, update in zip(workers, updates, strict=True):
            worker._messages.extend((update, update))
            worker._receive_timeouts.clear()

        supervisor._tick()

        assert all(not worker._messages for worker in workers)
        assert all(worker._receive_timeouts == [0.0, 0.0] for worker in workers)
        assert {
            worker_id
            for worker_id, spawn in supervisor._spawns.items()
            if spawn._health is not None
        } == {update.worker.worker_id for update in updates}
        assert all(
            spawn._health.worker.spawn_instance == factory._instances[worker_id]._spawn_instance
            for worker_id, spawn in supervisor._spawns.items()
            if spawn._health is not None
        )
    finally:
        supervisor._close()


def test_supervisor_tick_persists_worker_breaker_restart_window(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
            )
        ).__next__,
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        dependent._exit_status = 1
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=clock()))

        supervisor._tick()

        policy = config.sites['test'].workers['pool']
        with _SupervisorState._open(config.daemon.state_dir) as state:
            restored = state._restore_worker_breaker(
                worker_id=dependent._worker_id,
                restart_limit=policy.restart_limit,
                restart_window_secs=policy.restart_window_secs,
                healthy_reset_secs=policy.healthy_reset_secs,
                clock=clock,
            )
        assert restored._failure_total == 1
        assert len(restored._failure_times) == 1
        assert restored._restart_deadline_at == 1.0
    finally:
        supervisor._close()


def test_supervisor_tick_publishes_snapshot_without_due_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()

        before = supervisor.probe_result()
        changed = supervisor._tick()
        after = supervisor.probe_result()

    assert not changed
    assert before.generated_at is None
    assert after.generated_at == 0.0
    assert after.liveness
    assert not after.complete
    assert not after.readiness


@pytest.mark.parametrize(
    ('phase', 'elapsed'),
    [
        ('callback', 120.001),
        ('internal', 192.001),
    ],
)
def test_supervisor_tick_uses_stale_lane_heartbeat_with_fresh_worker_monitor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    phase: Literal['callback', 'internal'],
    elapsed: float,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        wall_clock=clock,
        token_factory=iter(
            (
                'source-generation-1',
                'source-spawn-1',
                'dependent-spawn-1',
                'capability-1',
                'evidence-1',
                'dependent-spawn-2',
                'capability-2',
            )
        ).__next__,
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={},
        )
        supervisor.start()
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        group_path = source_database(config.daemon.state_dir)
        clock.advance(elapsed)
        group_health = _in_flight_group_health(
            dependent,
            heartbeat_at=0.0,
            phase=phase,
        )
        for worker in factory._instances.values():
            worker._messages.append(
                _health_update(
                    worker,
                    heartbeat_at=clock(),
                    groups=(group_health,) if worker is dependent else (),
                )
            )

        with _conditional_group(group_path):
            supervisor._tick()

        assert dependent._killed
        expected = _park_evidence(
            failed_worker_id=dependent._worker_id,
            observed_at_wall_clock=clock(),
        )
        expected_evidence = {('test', 'remote'): expected} if phase == 'callback' else {}
        assert supervisor._park_evidence_by_group == expected_evidence
        with _SupervisorState._open(config.daemon.state_dir) as state:
            assert state._pending_park_evidence() == ((expected,) if phase == 'callback' else ())
    finally:
        supervisor._close()


@pytest.mark.parametrize(
    ('operation', 'error_factory', 'expected_reason'),
    [
        ('send', lambda: TimeoutError('send timed out'), 'worker-control-send-timeout'),
        ('send', lambda: EOFError('send closed'), 'worker-control-send-failed'),
        ('send', lambda: OSError('send failed'), 'worker-control-send-failed'),
        ('send', lambda: ValueError('send poisoned'), 'worker-control-send-failed'),
        ('receive', lambda: TimeoutError('receive timed out'), 'worker-control-eof'),
        ('receive', lambda: EOFError('receive closed'), 'worker-control-eof'),
        ('receive', lambda: OSError('receive failed'), 'worker-control-eof'),
        ('receive', lambda: ValueError('receive poisoned'), 'worker-control-eof'),
    ],
)
def test_supervisor_operator_transport_failure_contains_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    operation: Literal['send', 'receive'],
    error_factory: Callable[[], BaseException],
    expected_reason: str,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='transport-failure-command',
        command='park',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=0.0))
        supervisor._tick()
        owner = next(worker for worker in factory._instances.values() if worker._spec.groups)

        if operation == 'send':

            def fail_send(_message: ControlMessage) -> None:
                raise error_factory()

            monkeypatch.setattr(owner, '_send', fail_send)
        else:

            def fail_receive(*, timeout: float | None = None) -> ControlMessage:
                assert timeout is not None
                assert timeout > 0
                raise error_factory()

            monkeypatch.setattr(owner, '_receive', fail_receive)

        with pytest.raises((TimeoutError, EOFError, OSError, ValueError)):
            supervisor.dispatch_operator_command(command, timeout=1.0)

        assert owner._killed
        assert supervisor._spawns[owner._worker_id]._termination_reason == expected_reason
        assert not _spawn_health(supervisor, owner._worker_id).worker.monitor_alive


def test_supervisor_trusted_wait_deadlines_clamp_retry_and_lane_idle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(config=config, config_path=config_path)
    worker = _ScriptedWorkerFactory()(
        config_path=config_path,
        spec=build_worker_specs(config.sites['test'])[0],
        spawn_instance='wait-spawn-1',
        max_frame_bytes=1_048_576,
        send_deadline_secs=0.1,
        startup_deadline_secs=2.0,
        shutdown_deadline_secs=2.0,
    )
    worker._assignment = _source_assignment(worker._spec, worker._spawn_instance)
    ordinary = _idle_source_health(worker, source='admin', heartbeat_at=10.0)
    retry = replace(
        ordinary,
        retry_attempt=3,
        next_retry_secs=10_000.0,
        last_error='provider outage',
    )
    source_watchdog = _source_watchdog(
        worker,
        deadline_at=10_000.0,
        heartbeat_at=10.0,
        phase='wait',
    )
    lane_watchdog = WatchdogHealth(
        kind='lane',
        site='test',
        source='admin',
        group='remote',
        worker_instance=worker._spawn_instance,
        phase='idle',
        heartbeat_at=10.0,
        deadline_at=10_000.0,
    )
    trusted = cast(
        'Callable[[Supervisor, WatchdogHealth, Mapping[str, SourceHealth]], float | None]',
        vars(Supervisor)['_trusted_watchdog_deadline'],
    )
    retry_cap = config.sites['test'].sources['admin'].poll_retry_max_secs * (
        1 + config.sites['test'].sources['admin'].poll_retry_jitter_fraction
    )
    tolerance = 5 * config.daemon.health_interval_secs

    assert trusted(supervisor, source_watchdog, {'admin': ordinary}) == (
        10.0 + config.sites['test'].sources['admin'].fetch_interval_secs + tolerance
    )
    assert trusted(supervisor, source_watchdog, {'admin': retry}) == (10.0 + retry_cap + tolerance)
    assert trusted(supervisor, lane_watchdog, {}) == 10.0 + retry_cap + tolerance
    with pytest.raises(ValueError, match='retry wait'):
        trusted(
            supervisor, source_watchdog, {'admin': replace(retry, next_retry_secs=float('inf'))}
        )


def test_supervisor_watchdog_sampling_failure_kills_complete_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        source._messages.append(_health_update(source, heartbeat_at=0.0))
        supervisor._tick()
        source._messages.append(
            replace(
                _health_update(
                    source,
                    heartbeat_at=0.0,
                    sources=(),
                    groups=(),
                    watchdogs=(),
                ),
                worker=replace(
                    _health_update(source, heartbeat_at=0.0).worker,
                    watchdog_failure='watchdog-sampling-failed',
                ),
            )
        )
        monkeypatch.setattr(source, '_wait', _never_reap)

        supervisor._tick()

        assert source._killed
        assert supervisor._spawns[source._worker_id]._termination_reason == (
            'watchdog-sampling-failed'
        )
        assert not _spawn_health(supervisor, source._worker_id).worker.monitor_alive
    finally:
        supervisor._close()


def test_supervisor_worker_control_eof_is_current_worker_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=_FakeClock(),
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        source._messages.append(_health_update(source, heartbeat_at=0.0))
        dependent._messages.append(_health_update(dependent, heartbeat_at=0.0))
        supervisor._tick()

        failures: list[str] = []

        def closed() -> Iterator[ControlMessage]:
            yield _health_update(dependent, heartbeat_at=0.0)
            failures.append('eof')
            raise EOFError('closed test control')

        monkeypatch.setattr(dependent, '_receive_available', closed)
        monkeypatch.setattr(dependent, '_wait', _never_reap)
        supervisor._tick()

        assert failures == ['eof']
        assert dependent._killed
        assert supervisor._spawns[dependent._worker_id]._termination_reason == (
            'worker-control-eof'
        )
    finally:
        supervisor._close()


def test_supervisor_worker_control_send_budget_tracks_health_interval(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    factory = _ScriptedWorkerFactory()

    with Supervisor(
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()

    assert factory._send_deadlines
    assert set(factory._send_deadlines) == {2 * config.daemon.health_interval_secs}


def test_takeover_write_lock_probe_leaves_file_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    state_dir = ensure_secure_state_dir(config.daemon.state_dir)
    sites = ensure_secure_state_dir(state_dir / 'sites')
    site = ensure_secure_state_dir(sites / 'test')
    sources = ensure_secure_state_dir(site / 'sources')
    path = sources / 'admin.db.write-lock'
    fd, _ = secure_open_regular_file(path)
    try:
        os.write(fd, b'keep these bytes')
    finally:
        os.close(fd)
    original_bytes = path.read_bytes()
    original_mtime_ns = path.stat().st_mtime_ns

    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._wait_for_worker_locks()

    assert path.read_bytes() == original_bytes
    assert path.stat().st_mtime_ns == original_mtime_ns


def test_trusted_interval_for_write_phases_ignores_small_internal_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, loaded = _source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    credential = site.credentials['admin']
    site = replace(
        site,
        credentials=MappingProxyType(
            {
                'admin': replace(
                    credential,
                    timeout_secs=0.1,
                    max_rpc_attempts=1,
                    rpc_attempt_interval_secs=0.0,
                )
            }
        ),
    )
    config = replace(
        loaded,
        daemon=replace(
            loaded.daemon,
            health_interval_secs=0.1,
            supervisor_poll_interval_secs=0.1,
        ),
        sites=MappingProxyType({'test': site}),
    )
    factory = _ScriptedWorkerFactory()
    clock = _FakeClock()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        dependent = next(
            worker for worker in factory._instances.values() if not worker._spec.sources
        )
        assert supervisor._internal_watchdog_budget('test', 'admin') == 1.0
        cases: tuple[tuple[WatchdogKind, str | None], ...] = (
            ('lane', 'remote'),
            ('source', None),
        )
        phase_budgets: tuple[tuple[WatchdogPhase, float], ...] = (
            ('write-hold', 5.0),
            ('write-lock-wait', write_lock_timeout_secs(0.1, 0.1) + 0.1),
        )
        for kind, group_name in cases:
            for phase, expected in phase_budgets:
                watchdog = WatchdogHealth(
                    kind=kind,
                    site='test',
                    source='admin',
                    group=group_name,
                    worker_instance=dependent._spawn_instance,
                    phase=phase,
                    heartbeat_at=0.0,
                    deadline_at=4.0,
                )
                assert supervisor._trusted_watchdog_interval(watchdog, {}) == expected
                assert supervisor._trusted_watchdog_deadline(watchdog, {}) == 4.0

        group = _in_flight_group_health(dependent, heartbeat_at=0.0, phase='write-hold')
        group = replace(group, deadline_at=4.0)
        lane_watchdog = WatchdogHealth(
            kind='lane',
            site='test',
            source='admin',
            group='remote',
            worker_instance=dependent._spawn_instance,
            phase='write-hold',
            heartbeat_at=0.0,
            deadline_at=4.0,
        )
        with monkeypatch.context() as patch:
            patch.setattr(write_lock_module, 'WRITE_HOLD_DEADLINE_SECS', 6.0)
            assert supervisor._trusted_watchdog_interval(lane_watchdog, {}) == 6.0
            assert (
                supervisor._trusted_watchdog_interval(
                    replace(lane_watchdog, phase='write-lock-wait'), {}
                )
                == write_lock_timeout_secs(0.1, 0.1) + 0.1
            )
        operator = WatchdogHealth(
            kind='operator',
            site='test',
            source=None,
            group=None,
            worker_instance=dependent._spawn_instance,
            phase='idle',
            heartbeat_at=0.0,
            deadline_at=None,
        )
        dependent._messages.append(
            _health_update(
                dependent,
                heartbeat_at=0.0,
                groups=(group,),
                watchdogs=(lane_watchdog, operator),
            )
        )
        source._messages.append(_health_update(source, heartbeat_at=0.0))
        supervisor._tick()
        clock.advance(2.0)
        dependent._messages.append(
            _health_update(
                dependent,
                heartbeat_at=clock(),
                groups=(group,),
                watchdogs=(lane_watchdog, operator),
            )
        )
        source._messages.append(_health_update(source, heartbeat_at=clock()))
        supervisor._tick()

        assert not dependent._killed
        assert supervisor._spawns[dependent._worker_id]._termination_reason is None
    finally:
        supervisor._close()


def test_worker_breaker_backoff_saturates_without_overflow() -> None:
    breaker = _WorkerBreaker(
        worker_id='dependent',
        restart_limit=1_100,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
        clock=lambda: 0.0,
    )

    delays = tuple(breaker._record_failure('ambiguous-worker-failure') for _ in range(1_100))

    assert delays[:7] == (1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0)
    assert all(delay == 60.0 for delay in delays[7:])


def test_worker_breaker_reason_limit_accepts_exact_utf8_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    reason = 'é' * 2_048
    breaker = _WorkerBreaker(
        worker_id='dependent',
        restart_limit=0,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
    )
    breaker._record_failure('ambiguous-worker-failure')
    identity = AuditIdentity.current()
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'boot-1')

    with _SupervisorState._open(tmp_path / 'state') as state:
        state._persist_worker_breaker(breaker)
        transition = state._reset_worker_breaker(
            breaker,
            expected_revision=1,
            reason=reason,
            identity=identity,
            wall_clock_time=20.0,
        )
        database = cast('DatabaseHandle', vars(state)['_database'])
        row = database.connection.execute(
            "SELECT reason FROM breaker_audit WHERE worker_id = 'dependent'"
        ).fetchone()

    assert len(reason.encode()) == 4_096
    assert transition.reason == reason
    assert row == (reason,)


@pytest.mark.parametrize(
    'reason',
    [
        'x' * 4_097,
        'é' * 2_048 + 'x',
    ],
    ids=['ascii', 'multibyte'],
)
def test_worker_breaker_reason_limit_rejects_utf8_overflow_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
    tmp_path: Path,
) -> None:
    breaker = _WorkerBreaker(
        worker_id='dependent',
        restart_limit=0,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
    )
    breaker._record_failure('ambiguous-worker-failure')
    before = breaker._projection
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'boot-1')

    with _SupervisorState._open(tmp_path / 'state') as state:
        state._persist_worker_breaker(breaker)
        with pytest.raises(ValueError, match=r'4096.*UTF-8'):
            state._reset_worker_breaker(
                breaker,
                expected_revision=1,
                reason=reason,
                identity=AuditIdentity.current(),
                wall_clock_time=20.0,
            )
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 0

    assert len(reason.encode()) == 4_097
    assert breaker._projection == before


@pytest.mark.parametrize('unreaped', [False, True])
@pytest.mark.parametrize('control_built', [False, True])
def test_failed_spawn_reaps_despite_socket_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    unreaped: bool,
    control_built: bool,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    process = cast('ChildProcess', object())
    attempts: list[str] = []
    initiating = ValueError('construction failed')

    def control(*_args: object, **_kwargs: object) -> object:
        if not control_built:
            raise initiating
        return _FailedSpawnSocket('control', attempts)

    def fail_instance(*_args: object, **_kwargs: object) -> None:
        raise initiating

    def terminate(child: ChildProcess, *, force: bool) -> None:
        assert child is process
        assert force
        attempts.append('terminate')

    def reap(child: ChildProcess, **_kwargs: object) -> int | None:
        assert child is process
        attempts.append('reap')
        return {True: None, False: 0}[unreaped]

    monkeypatch.setattr(
        supervisor_module.socket,
        'socketpair',
        lambda: (_FailedSpawnSocket('parent', attempts), _FailedSpawnSocket('child', attempts)),
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', Mock(return_value=process))
    monkeypatch.setattr(supervisor_module, 'WorkerControl', control)
    monkeypatch.setattr(_WorkerInstance, '__init__', fail_instance)
    monkeypatch.setattr(supervisor_module, 'terminate_child', terminate)
    monkeypatch.setattr(supervisor_module, '_reap_child_until', reap)
    expected = {True: _UnreapedWorkerSpawnError, False: ValueError}[unreaped]
    with pytest.raises(expected) as caught:
        _WorkerInstance._spawn(
            config_path=config_path,
            spec=spec,
            spawn_instance='failed-spawn',
            max_frame_bytes=131_072,
            send_deadline_secs=1.0,
            startup_deadline_secs=1.0,
            shutdown_deadline_secs=1.0,
        )
    assert attempts == [
        'child',
        'child',
        'control' if control_built else 'parent',
        'terminate',
        'reap',
    ]
    if unreaped:
        assert isinstance(caught.value, _UnreapedWorkerSpawnError)
        assert caught.value._process is process
        assert caught.value._spec is spec
        assert caught.value._spawn_instance == 'failed-spawn'
        assert caught.value.__cause__ is initiating
    else:
        assert caught.value is initiating
    assert any('child cleanup failed' in note for note in initiating.__notes__)


@pytest.mark.parametrize('reap_fails', [False, True])
def test_failed_spawn_cleanup_retains_termination_and_reap_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reap_fails: bool,
) -> None:
    _config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    process = cast('ChildProcess', object())
    attempts: list[str] = []
    termination_error = OSError('termination failed')
    reap_error = OSError('reap failed')

    def terminate(_process: ChildProcess, *, force: bool) -> None:
        assert force
        attempts.append('terminate')
        raise termination_error

    def reap(_process: ChildProcess, **_options: object) -> int:
        attempts.append('reap')
        if reap_fails:
            raise reap_error
        return 0

    monkeypatch.setattr(supervisor_module, 'terminate_child', terminate)
    monkeypatch.setattr(supervisor_module, '_reap_child_until', reap)
    expected = _UnreapedWorkerSpawnError if reap_fails else OSError
    with pytest.raises(expected, match=r'unreaped|termination failed') as caught:
        supervisor_module._cleanup_failed_worker_spawn(
            process=process,
            spec=spec,
            spawn_instance='failed-spawn',
            shutdown_deadline_secs=1.0,
            wait_slice_secs=0.1,
            foreground_progress=None,
        )
    assert attempts == ['terminate', 'reap']
    if reap_fails:
        assert isinstance(caught.value, _UnreapedWorkerSpawnError)
        assert caught.value._process is process
        assert any('termination failed' in note for note in caught.value.__notes__)
        assert any('reap failed' in note for note in caught.value.__notes__)
    else:
        assert caught.value is termination_error


@pytest.mark.parametrize('is_open', [False, True])
def test_breaker_policy_change_preserves_durable_authority(
    tmp_path: Path, *, is_open: bool
) -> None:
    clock = _FakeClock()
    with _SupervisorState._open(tmp_path / 'state') as state:
        breaker = _WorkerBreaker(
            worker_id='worker',
            restart_limit=1,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        breaker._record_failure('first')
        if is_open:
            breaker._record_failure('second')
        state._persist_worker_breaker(breaker)
        before = breaker._projection
        restored = state._restore_worker_breaker(
            worker_id='worker',
            restart_limit=5,
            restart_window_secs=200.0,
            healthy_reset_secs=30.0,
            clock=clock,
        )
        expected = replace(
            before, _restart_limit=5, _restart_window_secs=200.0, _healthy_reset_secs=30.0
        )
        assert restored._projection == expected
        row = state._database.connection.execute(
            'SELECT restart_limit, restart_window_secs, healthy_reset_secs FROM worker_breaker'
        ).fetchone()
        assert tuple(row) == (5, 200.0, 30.0)


@pytest.mark.parametrize(
    'failure_stage',
    [
        'reservation',
        'source-committed',
        'inventory-completed',
    ],
)
def test_absent_orphan_disposition_recovers_after_restart(
    failure_stage: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, group_path = _prepare_absent_orphan(config)
    command = OperatorCommand(
        request_id='reset-restart',
        command='reset-group-to-latest',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'retire history'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    _inject_orphan_disposition_failure(monkeypatch, failure_stage)
    with Supervisor(
        config=config, config_path=config_path, worker_factory=_ScriptedWorkerFactory()
    ) as supervisor:
        supervisor.reconcile_durable_state()
        with pytest.raises(TimeoutError, match=f'injected {failure_stage} failure'):
            supervisor.dispatch_group_disposition_command(command, timeout=1.0)
    with Supervisor(
        config=config, config_path=config_path, worker_factory=_ScriptedWorkerFactory()
    ) as supervisor:
        supervisor.reconcile_durable_state()
        result = supervisor.dispatch_group_disposition_command(
            replace(command, identity=replace(command.identity, pid=789)), timeout=1.0
        )
        assert result.ok
        assert result.result['group_generation'] == 2
    with (
        _lifecycle_group(group_path, generation=2) as group,
        _lifecycle_source(
            source_path,
        ) as source,
    ):
        terminal = group.terminal_disposition_identity(2)
        assert terminal is not None
        assert source.has_terminal_disposition('remote', *terminal)


@pytest.mark.parametrize('worker_mode', ['default', 'pool', 'dedicated'])
@pytest.mark.parametrize('command_name', ['replay', 'reset-group-to-latest'])
def test_pending_disposition_restarts_source_and_delivery_owners(
    command_name: str,
    worker_mode: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, loaded = _source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    config = replace(
        loaded,
        sites={
            'test': replace(
                site, groups={'remote': replace(site.groups['remote'], worker=worker_mode)}
            )
        },
    )
    factory = _ScriptedWorkerFactory()
    group_path = source_database(config.daemon.state_dir)
    _prepare_absent_orphan(config)
    with _lifecycle_source(group_path) as durable_source:
        durable_source.commit_observations((110,), successful_clock=1.0)
    command = OperatorCommand(
        request_id='complete-pending',
        command=command_name,
        arguments={
            'site': 'test',
            'group': 'remote',
            'reason': 'recover pending group',
            **(
                {'from_event_id': 101, 'acknowledge_truncation': False}
                if command_name == 'replay'
                else {}
            ),
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )

    def accept(command: OperatorCommand) -> CommandResult:
        return CommandResult(
            request_id=command.request_id,
            command=command.command,
            ok=True,
            message='accepted',
            result={'group_generation': 2},
        )

    with Supervisor(config=config, config_path=config_path, worker_factory=factory) as supervisor:
        supervisor.reconcile_durable_state()
        supervisor.start()
        source = next(worker for worker in factory._instances.values() if worker._spec.sources)
        owner_id = next(
            spec.worker_id
            for spec in build_worker_specs(config.sites['test'])
            if 'remote' in spec.groups
        )
        owner = factory._instances[owner_id]
        assert all(not worker._spec.groups for worker in factory._instances.values())
        source._operator_handler = lambda command: _execute_source_group_command(
            group_path, command
        )
        result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert result.ok
        new_source = factory._instances[source._worker_id]
        new_owner = factory._instances[owner._worker_id]
        assert new_source is not source
        assert new_owner is not owner
        assert new_source._assignment is not None
        assert new_owner._assignment is not None
        assert isinstance(
            new_source._assignment.source_group_assignments[0], ActiveSourceGroupAssignment
        )
        assert isinstance(new_owner._assignment.group_assignments[0], DeliveryGroupAssignment)
        new_owner._operator_handler = accept
        assert supervisor.dispatch_operator_command(
            replace(command, command='unpark'), timeout=1.0
        ).ok
        assert new_owner._sent_commands[-1].command == 'unpark'


@pytest.mark.parametrize('mode', ['readd', 'absent'])
@pytest.mark.parametrize('stage', ['reservation', 'source-committed', 'inventory-completed'])
def test_lifecycle_survives_supervisor_death_at_each_store_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, stage: str
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, configured = _source_only_config(tmp_path, monkeypatch)
    source_path, _group_path = _prepare_absent_orphan(configured)
    config = (
        configured
        if mode == 'readd'
        else replace(configured, sites={'test': replace(configured.sites['test'], groups={})})
    )
    proof = tmp_path / 'kill-boundary.txt'
    killed = subprocess.run(
        [
            sys.executable,
            '-c',
            (
                'from pathlib import Path; import sys; '
                'from tests.test_supervisor import kill_supervisor_store_boundary; '
                'kill_supervisor_store_boundary(Path(sys.argv[1]), sys.argv[2], '
                'sys.argv[3], Path(sys.argv[4]))'
            ),
            str(config_path),
            mode,
            stage,
            str(proof),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=20.0,
    )
    assert killed.returncode == -signal.SIGKILL, killed.stderr
    assert proof.read_text() == stage
    with closing(sqlite3.connect(source_path)) as connection, connection:
        assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        assert connection.execute('SELECT group_generation FROM groups').fetchone() == (
            1 if stage == 'reservation' else 2,
        )
        assert connection.execute('SELECT COUNT(*) FROM disposition_audit').fetchone() == (
            int(stage != 'reservation'),
        )
    command = OperatorCommand(
        request_id='retry-cross-store',
        command='reset-group-to-latest',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'cross-store recovery'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor.reconcile_durable_state()
        state = supervisor._state
        assert state is not None
        inventory = state._group_inventory_record(site='test', group='remote')
        assert inventory is not None
        assert inventory._generation == 2
        assert inventory._orphan_generation == (1 if stage == 'reservation' else None)
        if mode == 'readd' and stage == 'reservation':
            assert inventory._lifecycle == 'readded-parked'
            assert all(
                not supervisor._runnable_worker_spec(spec).groups
                for spec in build_worker_specs(config.sites['test'])
            )
        result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert result.ok
        assert result.result['group_generation'] == 2
        assert result.message == ('accepted' if stage == 'reservation' else 'recorded')
        with closing(sqlite3.connect(source_path)) as connection, connection:
            before_retry = list(connection.iterdump())
        assert (
            supervisor.dispatch_group_disposition_command(command, timeout=1.0).result
            == result.result
        )
        with closing(sqlite3.connect(source_path)) as connection, connection:
            assert list(connection.iterdump()) == before_retry
            assert connection.execute('SELECT COUNT(*) FROM disposition_audit').fetchone() == (1,)
            assert connection.execute('SELECT COUNT(*) FROM replay_markers').fetchone() == (0,)


@pytest.mark.parametrize('held', ['source', 'group'])
def test_supervisor_direct_path_refuses_held_owner_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, held: str
) -> None:
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, _group_path = _prepare_absent_orphan(config)
    lock_path = (
        source_path.with_suffix('.db.lock')
        if held == 'source'
        else group_lock(source_path, 'remote')
    )
    command = OperatorCommand(
        request_id='busy',
        command='reset-group-to-latest',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'recover'},
        identity=AuditIdentity.current(),
    )
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor.reconcile_durable_state()
        with acquire_lock(lock_path):
            result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert not result.ok
        assert result.result == {'code': f'{held}-busy'}
    with closing(sqlite3.connect(source_path)) as connection, connection:
        assert connection.execute('SELECT group_generation FROM groups').fetchone() == (1,)
        assert connection.execute('SELECT COUNT(*) FROM disposition_audit').fetchone() == (0,)


@pytest.mark.parametrize('kill_after_orphan', [False, True])
def test_new_removal_after_disposition_keeps_inventory_fence_until_orphan_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kill_after_orphan: bool
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, configured = _source_only_config(tmp_path, monkeypatch)
    path = source_database(configured.daemon.state_dir)
    with _lifecycle_source(path) as source, _lifecycle_group(path) as group:
        source.initialize(100, incarnation='incarnation-1')
        parked = group.park(
            reason='maintenance',
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
            expected_revision=group.snapshot().revision,
        )
        group.dispose(
            DispositionRequest(
                command='reset-to-latest',
                group='remote',
                disposition_id='prior-configured-disposition',
                reserved_generation=2,
                expected_revision=parked.revision,
                from_event_id=None,
                acknowledge_truncation=False,
                reason='maintenance',
                identity=AuditIdentity.current(),
                wall_clock_time=2.0,
            )
        )
    with _SupervisorState._open(configured.daemon.state_dir) as state:
        state._reconcile_groups(
            configured={('test', 'remote'): 'admin'}, generations={('test', 'remote'): 2}
        )
    omitted = replace(configured, sites={'test': replace(configured.sites['test'], groups={})})
    if kill_after_orphan:
        proof = tmp_path / 'orphan-commit.txt'
        killed = subprocess.run(
            [
                sys.executable,
                '-c',
                (
                    'from pathlib import Path; import sys; '
                    'from tests.test_supervisor import kill_supervisor_store_boundary; '
                    'kill_supervisor_store_boundary(Path(sys.argv[1]), "absent", '
                    '"orphan-committed", Path(sys.argv[2]))'
                ),
                str(config_path),
                str(proof),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=20.0,
        )
        assert killed.returncode == -signal.SIGKILL, killed.stderr
        assert proof.read_text() == 'orphan-committed'
        with _SupervisorState._open(configured.daemon.state_dir) as state:
            record = state._group_inventory_record(site='test', group='remote')
            assert record is not None
            assert record._configured
    with Supervisor(config=omitted, config_path=config_path) as supervisor:
        supervisor.reconcile_durable_state()
    with closing(sqlite3.connect(path)) as connection, connection:
        assert connection.execute('SELECT group_generation, lifecycle FROM groups').fetchone() == (
            2,
            'orphaned',
        )
        assert connection.execute(
            'SELECT group_generation, from_event_id FROM replay_markers'
        ).fetchall() == [(2, 101)]
        assert connection.execute(
            "SELECT disposition_id FROM audit WHERE command = 'orphan'"
        ).fetchall() == [('2',)]
    with _SupervisorState._open(configured.daemon.state_dir) as state:
        record = state._group_inventory_record(site='test', group='remote')
        assert record is not None
        assert not record._configured


@contextmanager
def _pending_supervisor_health_owner(
    config: Config, spec: WorkerSpec, assignment: WorkerAssignment
) -> Generator[tuple[Supervisor, _ScriptedWorker]]:
    with _SupervisorState._open(config.daemon.state_dir) as ledger:
        ledger._reconcile_groups(
            configured={('main', 'local'): 'events'}, generations={('main', 'local'): 7}
        )
        ledger._reconcile_groups(configured={}, generations={('main', 'local'): 7})
        with transaction(ledger._database.connection):
            assert (
                _allocate_generation(
                    ledger._database.connection,
                    site='main',
                    group='local',
                    observed_generations=(9,),
                )
                == 10
            )
    factory = _ScriptedWorkerFactory()
    tokens = iter((dict(assignment.source_generations)['events'], assignment.spawn_instance))
    supervisor = Supervisor(
        config=config,
        config_path=config.daemon.state_dir / 'config.toml',
        worker_factory=factory,
        token_factory=lambda: next(tokens),
    )
    supervisor._open()
    supervisor._reconcile(generations={('main', 'local'): 7})
    supervisor.start()
    worker = factory._instances[spec.worker_id]
    try:
        yield supervisor, worker
    finally:
        supervisor._close()


@pytest.mark.parametrize('source_ready', [False, True])
def test_lane_less_invalid_reset_does_not_reserve_a_running_group(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, source_ready: bool
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    ensure_secure_state_dir(config.daemon.state_dir)
    path = source_database(config.daemon.state_dir)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with _lifecycle_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')
        with _lifecycle_group(path) as group:
            assert not group.snapshot().parked
    with Supervisor(
        config=config, config_path=config_path, worker_factory=_ScriptedWorkerFactory()
    ) as supervisor:
        supervisor.reconcile_durable_state()
        if source_ready:
            supervisor.start()
            for spawn in tuple(supervisor._spawns.values()):
                worker = spawn._worker
                if worker._spec.groups:
                    supervisor._discard_worker_lifecycle(worker)
            assert any(
                worker._spec.sources
                for spawn in supervisor._spawns.values()
                for worker in (spawn._worker,)
            )
        assert supervisor._state is not None
        before = supervisor._state._group_inventory_record(site='test', group='remote')
        command = OperatorCommand(
            request_id='invalid-running-reset',
            command='reset-group-to-latest',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'invalid while running'},
            identity=AuditIdentity.current(),
        )
        with pytest.raises(InvalidTransitionError, match='parked or orphaned'):
            supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert supervisor._state._group_inventory_record(site='test', group='remote') == before
        assert supervisor._state._generation_high_water('test', 'remote') == 1
        with _lifecycle_group(path) as group:
            group.park(
                reason='prepare reset',
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
                expected_revision=group.snapshot().revision,
            )
        for spawn in supervisor._spawns.values():
            worker = spawn._worker
            if worker._spec.sources:
                cast('_ScriptedWorker', worker)._operator_handler = lambda message: (
                    _execute_source_group_command(path, message)
                )
        applied = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert applied.ok
        with _lifecycle_group(path, generation=2) as group:
            group.unpark(
                reason='resume after reset',
                identity=AuditIdentity.current(),
                wall_clock_time=2.0,
                expected_revision=group.snapshot().revision,
            )
            assert not group.snapshot().parked
        with closing(sqlite3.connect(path)) as connection, connection:
            before_retry = tuple(connection.iterdump())
        inventory = supervisor._state._group_inventory_record(site='test', group='remote')
        retried = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert retried.ok
        assert retried.result == applied.result
        with closing(sqlite3.connect(path)) as connection, connection:
            assert tuple(connection.iterdump()) == before_retry
        assert supervisor._state._group_inventory_record(site='test', group='remote') == inventory
        assert supervisor._state._generation_high_water('test', 'remote') == 2


@pytest.mark.parametrize('drift', ['identity', 'ordering'])
@pytest.mark.parametrize('retry', ['exact', 'conflict', 'stale'])
def test_direct_disposition_setup_preserves_durable_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, drift: str, retry: str
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    path, _ = _prepare_absent_orphan(config)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor.reconcile_durable_state()
        command = OperatorCommand(
            command='reset-group-to-latest',
            request_id='direct-setup',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'retain receipt'},
            identity=AuditIdentity.current(),
        )
        applied = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert applied.ok
        before = worker_tests._drift_disposition_policy(path, 'remote', drift)
        assert supervisor._state is not None
        inventory = supervisor._state._group_inventory_record(site='test', group='remote')
        if retry == 'exact':
            result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
            assert result.result == applied.result
        else:
            metadata = supervisor._orphan_group_metadata(site='test', group='remote')
            identity = supervisor_module.stable_operator_id(command.command, command.arguments)
            request = DispositionRequest(
                command='reset-to-latest',
                group='remote',
                disposition_id=identity if retry == 'conflict' else 'new-stale',
                reserved_generation=2 if retry == 'conflict' else 3,
                expected_revision=0,
                from_event_id=None,
                acknowledge_truncation=False,
                reason='conflicting' if retry == 'conflict' else 'retain receipt',
                identity=command.identity,
                operator_label=supervisor_module.control_peer_label(command.identity),
                wall_clock_time=1.0,
            )
            if retry == 'conflict':
                with pytest.raises(InvalidTransitionError, match='conflicting content'):
                    supervisor._run_direct_disposition(command, metadata, request)
            else:
                with pytest.raises((InvalidTransitionError, RevisionConflictError)):
                    supervisor._run_direct_disposition(command, metadata, request)
        with closing(sqlite3.connect(path)) as connection, connection:
            assert tuple(connection.iterdump()) == before
            assert connection.execute('SELECT COUNT(*) FROM epochs').fetchone() == (1,)
        assert supervisor._state._group_inventory_record(site='test', group='remote') == inventory
        assert supervisor._state._generation_high_water('test', 'remote') == 2


def test_pending_group_publishes_valid_health_during_slow_provider_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, _snapshot = worker_tests._pending_group_runtime(tmp_path)
    site = config.sites['main']
    source_config = replace(
        site.sources['events'],
        fetch_interval_secs=0.01,
        poll_retry_base_secs=0.01,
        poll_retry_max_secs=0.01,
        poll_retry_jitter_fraction=0.0,
    )
    site = replace(site, sources=MappingProxyType({'events': source_config}))
    config = replace(config, sites=MappingProxyType({'main': site}))
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    original = worker_tests._RuntimeProviderHandle.find

    def find(
        handle: worker_tests._RuntimeProviderHandle,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[worker_tests._FindOptions],
    ) -> list[dict[str, object]]:
        if not entered.is_set() and filters:
            entered.set()
            assert release.wait(2.0)
            finished.set()
            return []
        if filters:
            return []
        return original(handle, entity_type, filters, fields, **options)

    monkeypatch.setattr(worker_tests._RuntimeProviderHandle, 'find', find)
    publications = _HealthPublications(monkeypatch)
    updates: list[HealthUpdate] = []
    with (
        _pending_supervisor_health_owner(config, spec, assignment) as (supervisor, worker),
        worker_tests._running_concrete_runtime(
            monkeypatch, config, spec, assignment, worker_tests._runtime_provider()
        ) as (runtime, control),
    ):
        try:
            assert entered.wait(1.0)
            update = worker_tests._receive_typed_health(
                control,
                lambda item: (
                    len(item.groups) == 1
                    and any(
                        watch.kind == 'source' and watch.phase == 'provider-query'
                        for watch in item.watchdogs
                    )
                ),
            )
            update = _latest_available_health(control, update)
            updates.append(update)
            _assert_pending_provider_publication(
                supervisor, worker, update, published_at=publications._published_at(update)
            )
            deadline = time.monotonic() + 0.20
            while time.monotonic() < deadline:
                update = _latest_available_health(
                    control, worker_tests._receive_type(control, HealthUpdate)
                )
                updates.append(update)
                _assert_pending_provider_publication(
                    supervisor, worker, update, published_at=publications._published_at(update)
                )
            assert (
                publications._published_at(updates[-1]) - publications._published_at(updates[0])
                > 0.06
            )
            assert all(item.groups[0].parked for item in updates)
            assert all(
                item.groups[0].conditions == ('parked', 'replay-required') for item in updates
            )
            assert all(watch.kind != 'lane' for item in updates for watch in item.watchdogs)
            source_watch = next(watch for watch in updates[-1].watchdogs if watch.kind == 'source')
            assert source_watch.deadline_at is not None
            assert publications._published_at(updates[-1]) < source_watch.deadline_at
            assert runtime.fatal_reason is None
            assert runtime._threads['source:events'].is_alive()
        finally:
            release.set()
        assert finished.wait(1.0)
        successful = worker_tests._receive_typed_health(
            control,
            lambda item: bool(item.sources) and item.sources[0].successful_clock > 0,
        )
        successful = _latest_available_health(control, successful)
        updates.append(successful)
        _assert_pending_provider_publication(
            supervisor, worker, successful, published_at=publications._published_at(successful)
        )
        assert successful.groups[0].parked
        assert runtime.fatal_reason is None
    (tmp_path / 'pending-provider-health.jsonl').write_bytes(
        b'\n'.join(ipc_module.encode_control_message(update) for update in updates) + b'\n'
    )


@pytest.mark.parametrize('latest_alive', [False, True])
def test_pending_provider_health_uses_latest_queued_frame_without_health_filter(
    monkeypatch: pytest.MonkeyPatch,
    latest_alive: bool,
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(time, 'monotonic', clock)
    publications = _HealthPublications(monkeypatch)
    first_socket, second_socket = socket.socketpair()
    sender = WorkerControl(first_socket, max_frame_bytes=65536, send_deadline_secs=1)
    receiver = WorkerControl(second_socket, max_frame_bytes=65536, send_deadline_secs=1)
    stale = HealthUpdate(
        worker=WorkerHealth(
            site='main',
            worker_id='owner',
            spawn_instance='spawn',
            heartbeat_at=0,
            monitor_alive=True,
            thread_names=(),
            stuck_threads=(),
            breaker_open=False,
            breaker_reason=None,
            socket_buffer_requested=1024,
            socket_send_buffer=1024,
            socket_receive_buffer=1024,
        ),
        watchdogs=(),
        sources=(),
        groups=(),
        event_sockets=(),
    )
    latest = replace(
        stale, worker=replace(stale.worker, heartbeat_at=10, monitor_alive=latest_alive)
    )
    try:
        sender.send(stale)
        clock.advance(10)
        sender.send(latest)
        clock.advance(10)
        initial = worker_tests._receive_type(receiver, HealthUpdate)
        assert initial == stale
        assert publications._published_at(initial) == 0
        observed = _latest_available_health(receiver, initial)
        assert observed == latest
        assert publications._published_at(observed) == 10
        with pytest.raises(TimeoutError):
            receiver.receive(timeout=0.0)
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize('phase', ['write-hold', 'provider-query'])
def test_supervisor_unconfirmed_source_phase_uses_heartbeat_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: Literal['write-hold', 'provider-query']
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    config = replace(
        config, daemon=replace(config.daemon, health_interval_secs=6, checkpoint_interval_secs=60)
    )
    clock = _FakeClock()
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(w for w in factory._instances.values() if w._spec.sources)
        for worker in factory._instances.values():
            detail = replace(
                _idle_source_health(source, source='admin', heartbeat_at=0),
                phase=phase,
                deadline_at=5,
            )
            worker._messages.append(
                _health_update(
                    worker, heartbeat_at=0, sources=(detail,) if worker is source else None
                )
            )
        supervisor._tick()
        waits: list[float] = []
        clock.advance(5.04)

        def wait(seconds: float) -> None:
            waits.append(seconds)
            clock.advance(seconds)
            for worker in factory._instances.values():
                if worker is not source:
                    worker._messages.append(_health_update(worker, heartbeat_at=clock()))

        supervisor.run(
            should_stop=lambda: len(waits) == 2 or source._killed,
            wait=wait,
            set_foreground_progress=lambda _callback: None,
        )
        assert not source._killed
        assert supervisor.probe_result().readiness
        assert waits == [config.daemon.supervisor_poll_interval_secs] * 2
        accepted = _spawn_health(supervisor, source._worker_id)
        projection = supervisor._refresh_component_health(accepted, 29.999).sources[0]
        assert projection.deadline_at == 5
        assert projection.heartbeat_age == 29.999
        assert projection.overdue_reason is None
        assert supervisor._worker_health_failure(source._worker_id, accepted, 29.999) is None
        assert (
            supervisor._worker_health_failure(source._worker_id, accepted, 30)
            == 'worker-heartbeat-overdue'
        )
        clock.advance(30 - clock())
        monkeypatch.setattr(source, '_wait', _never_reap)
        supervisor._tick()
        assert source._killed
        assert (
            supervisor._spawns[source._worker_id]._termination_reason == 'worker-heartbeat-overdue'
        )
        assert not supervisor._park_evidence_by_group


@pytest.mark.parametrize(
    'order', ['confirmed-healthy', 'unconfirmed-healthy', 'healthy-confirmed', 'confirmed-sliding']
)
def test_supervisor_phase_batch_uses_latest_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, order: str
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    clock = _FakeClock()
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory._instances.values():
            worker._messages.append(_health_update(worker, heartbeat_at=0))
        supervisor._tick()
        remote = next(w for w in factory._instances.values() if not w._spec.sources)
        clock.advance(10)
        group = replace(_in_flight_group_health(remote, heartbeat_at=0), deadline_at=10)
        expired = _health_update(
            remote, heartbeat_at=9.99 if order == 'unconfirmed-healthy' else 10, groups=(group,)
        )
        healthy = _health_update(remote, heartbeat_at=10)
        if order == 'confirmed-sliding':
            healthy = _health_update(
                remote, heartbeat_at=10, groups=(replace(group, heartbeat_at=10, deadline_at=130),)
            )
        remote._messages.extend(
            (healthy, expired) if order == 'healthy-confirmed' else (expired, healthy)
        )
        for worker in factory._instances.values():
            if worker is not remote:
                worker._messages.append(_health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        assert remote._killed == (order == 'healthy-confirmed')
        if not remote._killed:
            assert supervisor._breakers[remote._worker_id]._failure_total == 0
            assert _spawn_health(supervisor, remote._worker_id).groups[0].overdue_reason is None


@pytest.mark.parametrize('kind', ['source', 'lane', 'operator'])
def test_supervisor_component_confirmation_and_projection_share_equality(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: Literal['source', 'lane', 'operator']
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    clock = _FakeClock()
    factory = _ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(
            w for w in factory._instances.values() if bool(w._spec.sources) == (kind != 'lane')
        )
        clock.advance(10.1)
        health = _health_update(worker, heartbeat_at=9.999)
        health = replace(
            health,
            watchdogs=tuple(
                replace(item, phase='internal', heartbeat_at=0, deadline_at=10, source='admin')
                if item.kind == kind
                else item
                for item in health.watchdogs
            ),
        )
        assert supervisor._record_worker_health(worker, health, clock())
        update = _spawn_health(supervisor, worker._worker_id)
        assert supervisor._worker_health_failure(worker._worker_id, update, clock()) is None
        assert all(item.overdue_reason is None for item in (*update.sources, *update.groups))
        assert supervisor._record_worker_health(
            worker, replace(health, worker=replace(health.worker, heartbeat_at=10)), clock()
        )
        update = _spawn_health(supervisor, worker._worker_id)
        assert (
            supervisor._worker_health_failure(worker._worker_id, update, clock())
            == f'{kind}-phase-overdue'
        )
        projections = (
            update.sources if kind == 'source' else update.groups if kind == 'lane' else ()
        )
        assert all(item.overdue_reason == f'{kind}-phase-overdue' for item in projections)


@pytest.mark.parametrize(
    ('caller', 'expected_timeout', 'wrapped'),
    [
        ('disposition', 0.0, False),
        ('restart', 0.0, True),
        ('exists', 0.0, False),
        ('orphan', 0.0, False),
        ('source', 0.0, False),
    ],
)
def test_supervisor_read_observation_timeout_and_error_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caller: str,
    expected_timeout: float,
    wrapped: bool,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    _prepare_absent_orphan(config)
    supervisor = Supervisor(config=config, config_path=config_path)
    path = source_database(config.daemon.state_dir)
    failure = sqlite3.OperationalError('read observation unavailable')
    calls: list[tuple[str, float, bool]] = []

    def fail_connect(database: str, *, timeout: float = 5.0, uri: bool = False) -> None:
        calls.append((database, timeout, uri))
        raise failure

    monkeypatch.setattr(sqlite3, 'connect', fail_connect)

    def observe() -> None:
        if caller == 'disposition':
            supervisor._disposition_observation('test', 'remote', 'disposition-1')
        elif caller == 'restart':
            supervisor._durable_restart_start_mode(
                'test', ActiveSourceGroupAssignment('admin', 'remote', 1, 'resume')
            )
        elif caller == 'exists':
            supervisor._group_state_exists(path, 'remote')
        elif caller == 'orphan':
            supervisor._orphan_group_metadata(site='test', group='remote')
        else:
            supervisor._reconciliation_source_metadata(site='test', source='admin')

    with pytest.raises(RuntimeError if wrapped else sqlite3.OperationalError) as caught:
        observe()
    assert calls == [(f'{path.as_uri()}?mode=ro', expected_timeout, True)]
    assert (caught.value.__cause__ if wrapped else caught.value) is failure


def test_worker_breaker_reset_requires_revision_and_persists_audit(
    tmp_path: Path,
) -> None:
    clock = _FakeClock()
    breaker = _WorkerBreaker(
        worker_id='dependent',
        restart_limit=1,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
        clock=clock,
    )
    breaker._record_failure('ambiguous-worker-failure')
    breaker._record_failure('ambiguous-worker-failure')
    stale = _WorkerBreaker(
        worker_id='dependent',
        restart_limit=1,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
        clock=clock,
    )
    stale._record_failure('ambiguous-worker-failure')
    stale._record_failure('ambiguous-worker-failure')
    assert breaker._is_open
    assert breaker._revision == 1
    identity = AuditIdentity.current()
    state_dir = tmp_path / 'state'

    with _SupervisorState._open(state_dir) as state:
        state._persist_worker_breaker(breaker)
        with pytest.raises(ValueError, match='revision'):
            state._reset_worker_breaker(
                breaker,
                expected_revision=0,
                reason='operator reviewed shared worker',
                identity=identity,
                wall_clock_time=20.0,
            )
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 0

        transition = state._reset_worker_breaker(
            breaker,
            expected_revision=1,
            reason='operator reviewed shared worker',
            identity=identity,
            wall_clock_time=21.0,
        )
        assert transition.state == 'closed'
        assert transition.reason == 'operator reviewed shared worker'
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 1
        assert state._audit_count() == 1
        with pytest.raises(ValueError, match='revision'):
            state._reset_worker_breaker(
                stale,
                expected_revision=1,
                reason='stale competing reset',
                identity=identity,
                wall_clock_time=22.0,
            )
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 1

    with _SupervisorState._open(state_dir) as state:
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 1
        restored = state._restore_worker_breaker(
            worker_id='dependent',
            restart_limit=1,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert not restored._is_open
        assert restored._revision == 2
        assert restored._transition_count == 2
    assert not breaker._is_open
    assert breaker._revision == 2


def test_worker_breakers_use_independent_fake_clock_backoff_and_healthy_decay() -> None:
    clock = _FakeClock()
    first = _WorkerBreaker(
        worker_id='first',
        restart_limit=2,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
        clock=clock,
    )
    second = _WorkerBreaker(
        worker_id='second',
        restart_limit=2,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
        clock=clock,
    )

    assert first._record_failure('ambiguous-worker-failure') == 1.0
    clock.advance(1.0)
    assert first._record_failure('ambiguous-worker-failure') == 2.0
    assert second._record_failure('ambiguous-worker-failure') == 1.0
    clock.advance(1.0)
    assert first._record_failure('ambiguous-worker-failure') is None
    assert first._is_open
    assert not second._is_open

    clock.advance(10.0)
    assert second._record_healthy()
    assert len(second._failure_times) == 0
    assert first._is_open


def test_worker_instance_authenticates_assigns_and_exits_on_control_eof(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    (tmp_path / 'sg_event_daemon.py').write_text(
        "raise RuntimeError('worker imported the current directory')\n", encoding='utf-8'
    )
    monkeypatch.chdir(tmp_path)
    spec = build_worker_specs(config.sites['test'])[0]
    spawn_instance = 'source-spawn-1'
    spawn_calls: list[tuple[tuple[str, ...], tuple[int, ...]]] = []
    platform_spawn = supervisor_module.spawn_child

    def record_spawn(
        argv: list[str],
        *,
        pass_fds: tuple[int, ...] = (),
        env: Mapping[str, str] | None = None,
    ) -> ChildProcess:
        spawn_calls.append((tuple(argv), pass_fds))
        return platform_spawn(argv, pass_fds=pass_fds, env=env)

    monkeypatch.setattr(supervisor_module, 'spawn_child', record_spawn)
    worker = _WorkerInstance._spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance=spawn_instance,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=2 * config.daemon.health_interval_secs,
        startup_deadline_secs=config.daemon.worker_startup_deadline_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        env=_child_environment(),
    )
    try:
        assert worker._assigned_worker is None
        with pytest.raises(RuntimeError, match='authenticate before assignment'):
            worker._assign(_source_assignment(spec, spawn_instance))
        assert worker._authenticate(timeout=config.daemon.worker_startup_deadline_secs)
        assert worker._assigned_worker is None
        worker._assign(_source_assignment(spec, spawn_instance))
        message = _receive_source_endpoint_ready(worker, deadline=time.monotonic() + 5.0)
        assert message.source == 'admin'
        assert message.source_generation == 'source-generation-1'
        assert Path(message.path).is_socket()
        assert worker._worker_id == spec.worker_id
        assert worker._spawn_instance == spawn_instance
        assert len(spawn_calls) == 1
        argv, pass_fds = spawn_calls[0]
        assert len(pass_fds) == 1
        assert argv == (
            sys.executable,
            '-P',
            '-m',
            'sg_event_daemon',
            'run-worker',
            '--config',
            os.fspath(config_path),
            '--site',
            spec.site,
            '--worker',
            spec.worker_id,
            '--control-fd',
            str(pass_fds[0]),
            '--spawn-instance',
            spawn_instance,
        )

        worker._close()

        assert worker._wait(timeout=5.0) == 0
    finally:
        worker._kill()
        worker._wait(timeout=5.0)


def test_worker_instance_graceful_shutdown_reaps_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    spawn_instance = 'source-spawn-graceful'
    worker = _WorkerInstance._spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance=spawn_instance,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=2 * config.daemon.health_interval_secs,
        startup_deadline_secs=config.daemon.worker_startup_deadline_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        env=_child_environment(),
    )
    try:
        assert worker._authenticate(timeout=config.daemon.worker_startup_deadline_secs)
        worker._assign(_source_assignment(spec, spawn_instance))
        assert worker._shutdown('test-graceful-stop') == 0
        assert worker._wait(timeout=0.0) == 0
    finally:
        worker._kill()
        worker._wait(timeout=5.0)


def test_worker_instance_kills_and_reaps_a_child_that_ignores_clean_stop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    spawn_instance = 'source-spawn-stuck'
    spawner = _ControlChildSpawner(
        """
import os
import sys
import time
from sg_event_daemon._ipc import ControlHello, WorkerControl

control = WorkerControl.from_fd(
    int(sys.argv[1]),
    max_frame_bytes=8_388_608,
    send_deadline_secs=1.0,
)
control.send(
    ControlHello(
        spawn_instance=sys.argv[2],
        worker_id=sys.argv[3],
        pid=os.getpid(),
    )
)
time.sleep(30)
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)
    worker = _WorkerInstance._spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance=spawn_instance,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=2 * config.daemon.health_interval_secs,
        startup_deadline_secs=10.0,
        shutdown_deadline_secs=0.05,
        env=_child_environment(),
    )

    try:
        assert worker._authenticate(timeout=10.0)
        worker._assign(_source_assignment(spec, spawn_instance))
        return_code = worker._shutdown('test-clean-stop')
    finally:
        worker._kill()
        worker._wait(timeout=5.0)
        worker._close()

    assert return_code is not None
    assert return_code != 0
    assert len(spawner._processes) == 1
    assert reap_child(spawner._processes[0]) == return_code


def test_worker_instance_rejects_mismatched_hello_and_reaps_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    spawner = _ControlChildSpawner(
        """
import os
import sys
import time
from sg_event_daemon._ipc import ControlHello, WorkerControl

control = WorkerControl.from_fd(
    int(sys.argv[1]),
    max_frame_bytes=8_388_608,
    send_deadline_secs=1.0,
)
control.send(
    ControlHello(
        spawn_instance='wrong-spawn',
        worker_id=sys.argv[3],
        pid=os.getpid(),
    )
)
time.sleep(30)
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)

    worker = _WorkerInstance._spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance='source-spawn-expected',
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=2 * config.daemon.health_interval_secs,
        startup_deadline_secs=10.0,
        shutdown_deadline_secs=0.1,
        env=_child_environment(),
    )
    try:
        with pytest.raises(ValueError, match='hello identity'):
            worker._authenticate(timeout=10.0)
    finally:
        worker._kill()
        worker._wait(timeout=5.0)
        worker._close()

    assert len(spawner._processes) == 1
    assert reap_child(spawner._processes[0]) is not None


def test_worker_instance_rejects_mismatched_protocol_version_and_reaps_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    spawner = _ControlChildSpawner(
        """
import json
import os
import struct
import sys
import time

body = json.dumps(
    {
        'kind': 'hello',
        'payload': {
            'pid': os.getpid(),
            'spawn_instance': sys.argv[2],
            'worker_id': sys.argv[3],
        },
        'schema': 'sg-event-daemon-worker-control',
        'version': 2,
    },
    separators=(',', ':'),
).encode()
os.write(int(sys.argv[1]), struct.pack('!I', len(body)) + body)
time.sleep(30)
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)

    worker = _WorkerInstance._spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance='source-spawn-version',
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=2 * config.daemon.health_interval_secs,
        startup_deadline_secs=10.0,
        shutdown_deadline_secs=0.1,
        env=_child_environment(),
    )
    try:
        with pytest.raises(ValueError, match='version conflicts'):
            worker._authenticate(timeout=10.0)
    finally:
        worker._kill()
        worker._wait(timeout=5.0)
        worker._close()

    assert len(spawner._processes) == 1
    assert reap_child(spawner._processes[0]) is not None


def test_worker_instance_shutdown_closes_control_for_already_reaped_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    process = cast('ChildProcess', SimpleNamespace(returncode=17))
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(
        parent_socket,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=2 * config.daemon.health_interval_secs,
    )
    worker = _WorkerInstance(
        _spec=spec,
        _spawn_instance='source-spawn-already-reaped',
        _process=process,
        _control=control,
        _shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
    )

    def reap_already_exited(
        child: ChildProcess,
        *,
        timeout: float = 0.0,
    ) -> int:
        assert child is process
        assert timeout == 0.0
        return 17

    monkeypatch.setattr(supervisor_module, 'reap_child', reap_already_exited)
    try:
        assert worker._shutdown('test-already-reaped') == 17
        with pytest.raises(EOFError, match='control is closed'):
            control.receive(timeout=0.0)
    finally:
        control.close()
        child_socket.close()


def test_worker_instance_shutdown_reaps_after_control_peer_closes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    spawn_instance = 'source-spawn-closed-control'
    spawner = _ControlChildSpawner(
        """
import os
import sys
import time
from sg_event_daemon._ipc import ControlHello, StopCommand, WorkerControl

control = WorkerControl.from_fd(
    int(sys.argv[1]),
    max_frame_bytes=8_388_608,
    send_deadline_secs=1.0,
)
control.send(
    ControlHello(
        spawn_instance=sys.argv[2],
        worker_id=sys.argv[3],
        pid=os.getpid(),
    )
)
control.receive(timeout=5.0)
control.send(StopCommand(reason='closing-control-before-exit'))
control.close()
time.sleep(30)
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)
    worker = _WorkerInstance._spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance=spawn_instance,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=2 * config.daemon.health_interval_secs,
        startup_deadline_secs=10.0,
        shutdown_deadline_secs=0.05,
        env=_child_environment(),
    )
    try:
        assert worker._authenticate(timeout=10.0)
        worker._assign(_source_assignment(spec, spawn_instance))
        assert worker._receive(timeout=5.0) == StopCommand(reason='closing-control-before-exit')
        with pytest.raises(EOFError, match='peer closed'):
            worker._receive(timeout=5.0)

        return_code = worker._shutdown('test-control-already-closed')

        assert return_code is not None
        assert return_code != 0
        assert reap_child(spawner._processes[0]) == return_code
    finally:
        worker._kill()
        worker._wait(timeout=5.0)


def test_worker_instance_staged_authentication_times_out_without_losing_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    spawner = _ControlChildSpawner('import time; time.sleep(30)')
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)

    worker = _WorkerInstance._spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance='source-spawn-silent',
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=2 * config.daemon.health_interval_secs,
        startup_deadline_secs=0.05,
        shutdown_deadline_secs=0.1,
        env=_child_environment(),
    )
    try:
        assert not worker._authenticate(timeout=0.05)
        assert worker._wait() is None
    finally:
        worker._kill()
        worker._wait(timeout=5.0)
        worker._close()

    assert len(spawner._processes) == 1
    assert reap_child(spawner._processes[0]) is not None


def test_worker_shutdown_slices_reap_without_pulsing_on_slice_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _config_path, config = _source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    clock = _FakeClock()
    advances: list[None] = []
    timeouts: list[float] = []
    progress_counts_at_reap: list[int] = []

    def send_control(_message: ControlMessage) -> None:
        return None

    control = cast(
        'WorkerControl',
        SimpleNamespace(send=send_control, close=lambda: None),
    )
    process = cast('ChildProcess', SimpleNamespace(returncode=None))

    def sliced_reap(
        child: ChildProcess,
        *,
        timeout: float = 0.0,
    ) -> int | None:
        assert child is process
        timeouts.append(timeout)
        progress_counts_at_reap.append(len(advances))
        if timeout:
            clock.advance(timeout)
        return 0 if len(timeouts) == 4 else None

    monkeypatch.setattr(supervisor_module.time, 'monotonic', clock)
    monkeypatch.setattr(supervisor_module, 'reap_child', sliced_reap)
    worker = _WorkerInstance(
        _spec=spec,
        _spawn_instance='sliced-shutdown',
        _process=process,
        _control=control,
        _shutdown_deadline_secs=1.0,
        _wait_slice_secs=0.25,
        _foreground_progress=lambda: advances.append(None),
    )

    assert worker._shutdown('sliced-test') == 0
    assert timeouts == [0.0, 0.25, 0.25, 0.25]
    assert progress_counts_at_reap[0] == 0
    # The initial reap, stop send, and entry to the bounded lifecycle wait are
    # the only semantic advances visible at every timed reap attempt. The two
    # `None` slice returns add no progress of their own.
    assert progress_counts_at_reap[1:] == [3, 3, 3]
