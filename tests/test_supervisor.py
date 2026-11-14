from __future__ import annotations

import errno
import json
import logging
import multiprocessing
import os
import select
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, contextmanager, suppress
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack, cast
from unittest.mock import Mock

import pytest

import sg_event_daemon._cli as cli_module
import sg_event_daemon._control as control_module
import sg_event_daemon._ipc as ipc_module
import sg_event_daemon._platform as platform_module
import sg_event_daemon._probe as probe_module
import sg_event_daemon._providers as providers_module
import sg_event_daemon._supervisor as supervisor_module
import sg_event_daemon._worker as worker_module
import sg_event_daemon._write_lock as write_lock_module
import tests.test_worker as worker_tests
from sg_event_daemon._checkpoint import Checkpointer
from sg_event_daemon._config import ConfigError, load_config
from sg_event_daemon._control import (
    _MAX_MESSAGE_BYTES,
    _PROTOCOL_VERSION,
    ControlClient,
    ControlCommandError,
    ControlServer,
    encode_json_body,
)
from sg_event_daemon._group_errors import InvalidTransitionError, RevisionConflictError
from sg_event_daemon._group_recovery import DispositionRequest, LifecycleRequest
from sg_event_daemon._group_state import GroupOpenOptions, GroupState
from sg_event_daemon._group_types import (
    AuditIdentity,
    BeginCallbackStep,
    ConditionalParkDirective,
    GroupSnapshot,
    StartEventStep,
)
from sg_event_daemon._health import (
    BreakerTransition,
    GroupHealth,
    WatchdogHealth,
    WorkerHealth,
    _StatusSnapshot,
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
from sg_event_daemon._source_store import SourceIdentityError
from sg_event_daemon._sqlite import (
    DatabaseError,
    transaction,
)
from sg_event_daemon._supervisor import (
    OperatorOutcomeUnknownError,
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
    watchdog_grace_secs,
)
from sg_event_daemon._worker import WorkerSpec, build_worker_specs
from sg_event_daemon._write_lock import write_lock_timeout_secs
from tests._ipc_support import (
    partial_startup_child_code as _partial_startup_child_code,
)
from tests._ipc_support import receive_control, send_control
from tests._ipc_support import (
    saturate_worker_control_sender as _saturate_worker_control_sender,
)
from tests._signals import read_complete_lines
from tests._stall_policy import (
    STALL_DEADLINE_SECS,
    activate_stall_policy,
    active_stall_detectors,
    stall_deadline,
)
from tests._storage_support import (
    group_exists,
    group_lock,
    open_group_on_source,
    source_database,
    write_context_for,
)
from tests._worker_processes import (
    ChildPatches,
    SupervisorProbe,
    Topology,
    running_supervisor,
    write_daemon_config,
)
from tests.group._support import audit_records, commit_step
from tests.health_helpers import status_from_health
from tests.source.test_state import open_source_range

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator, Mapping
    from multiprocessing.connection import Connection
    from multiprocessing.process import BaseProcess

    from sg_event_daemon._config import Config
    from sg_event_daemon._health import _AggregateStatusOptions
    from sg_event_daemon._ipc import ControlMessage
    from sg_event_daemon._sqlite import DatabaseHandle
    from sg_event_daemon._supervisor import _WorkerHandle
    from tests._stall_policy import StallDetectors


class FakeClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _CheckpointOptions(TypedDict):
    interval_secs: float
    clock: Callable[[], float]
    logger: logging.Logger
    log_extra: Mapping[str, str]


class _ControlChildSpawner:
    def __init__(self, code: str) -> None:
        self._code = code
        self.processes: list[ChildProcess] = []

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
        code = (
            'import pytest\n'
            'from tests._stall_policy import activate_stall_policy\n'
            '_stall_policy_patch = pytest.MonkeyPatch()\n'
            f'activate_stall_policy(_stall_policy_patch, enabled={active_stall_detectors()!r})\n'
            + self._code
        )
        process = spawn_child(
            [
                argv[0],
                '-c',
                code,
                str(control_fd),
                spawn_instance,
                worker_id,
            ],
            pass_fds=pass_fds,
            env=env,
        )
        self.processes.append(process)
        return process


class _FailedSpawnSocket:
    def __init__(self, name: str, attempts: list[str]) -> None:
        self._name = name
        self._attempts = attempts

    def close(self) -> None:
        self._attempts.append(self._name)
        if self._name != 'child' or self._attempts.count('child') > 1:
            raise OSError(self._name + ' cleanup failed')

    def fileno(self) -> int:
        return 42


class _HealthPublications:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        publications: dict[bytes, float] = {}
        self._received: dict[int, tuple[HealthUpdate, float]] = {}
        original_encode = ipc_module.encode_control_message
        original_receive = WorkerControl.receive
        original_batch = WorkerControl.receive_available

        def encode(message: ControlMessage) -> bytes:
            frame = original_encode(message)
            if isinstance(message, HealthUpdate):
                # Capture actual frame construction, shared by bounded sends
                # and the outbox, before any of its bytes can be received.
                publications[frame] = time.monotonic()
            return frame

        def observe(message: ControlMessage) -> ControlMessage:
            if isinstance(message, HealthUpdate):
                published_at = publications[original_encode(message)]
                self._received[id(message)] = (message, published_at)
            return message

        def receive(control: WorkerControl, *, timeout: float) -> ControlMessage:
            return observe(original_receive(control, timeout=timeout))

        def receive_available(control: WorkerControl) -> Iterator[ControlMessage]:
            batch = original_batch(control)
            return (observe(message) for message in batch)

        monkeypatch.setattr(ipc_module, 'encode_control_message', encode)
        monkeypatch.setattr(WorkerControl, 'receive', receive)
        monkeypatch.setattr(WorkerControl, 'receive_available', receive_available)

    def published_at(self, update: HealthUpdate) -> float:
        received, published_at = self._received[id(update)]
        assert received is update
        return published_at


class _LaneRecoveryControl:
    def __init__(self, *, drop_ready: bool) -> None:
        self._drop_ready = drop_ready
        self.messages: list[GroupRecoveryReady] = []

    def enqueue(self, message: ControlMessage) -> None:
        if not isinstance(message, GroupRecoveryReady):
            raise TypeError(type(message).__name__)
        self.messages.append(message)
        if self._drop_ready:
            raise EOFError('simulated lost group recovery ready')


class _ObservedForegroundWait:
    def __init__(self, *, fails: bool = False) -> None:
        self.bindings: list[Callable[[], None] | None] = []
        self.calls: list[float] = []
        self._fails = fails

    def serve_once(self, timeout: float) -> None:
        callback = self.bindings[-1]
        assert callback is not None
        callback()
        self.calls.append(timeout)
        if self._fails:
            raise RuntimeError('wait failed')

    def set_foreground_progress(self, callback: Callable[[], None] | None) -> None:
        self.bindings.append(callback)


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


class ScriptedWorker:
    def __init__(
        self,
        spec: WorkerSpec,
        spawn_instance: str,
        operations: list[str],
        assignment: WorkerAssignment | None,
        *,
        authentication_ready: bool,
    ) -> None:
        self.authentication_calls = 0
        self.batch_acquisitions = 0
        self.authentication_ready = authentication_ready
        self.assignment: WorkerAssignment | None = None
        self.auto_endpoint_ready = True
        self.auto_grant_ready = True
        self.close_calls = 0
        self.control: WorkerControl | None = None
        self.exit_status: int | None = None
        self.killed = False
        self.on_kill: Callable[[], None] | None = None
        self.operator_handler: Callable[[OperatorCommand], CommandResult] | None = None
        self.operations = operations
        self.receive_timeouts: list[float | None] = []
        self.messages_before_grant: deque[ControlMessage] = deque()
        self.sent_commands: list[OperatorCommand] = []
        self.sent_grants: list[EventPeerGrant] = []
        self.shutdown_status: int | None = 0
        self.spawn_instance = spawn_instance
        self.spec = spec
        self.wait_results: deque[int | None] = deque()
        self.messages: deque[ControlMessage] = deque()
        if assignment is not None:
            self.assign(assignment)

    def assign(self, assignment: WorkerAssignment) -> None:
        if self.assignment is not None:
            raise ValueError('duplicate assignment')
        self.assignment = assignment
        self.operations.append(f'assign:{self.spec.kind.value}')
        if self.auto_endpoint_ready:
            for source, generation in assignment.source_generations:
                self.messages.append(
                    SourceEndpointReady(
                        source=source,
                        source_generation=generation,
                        path=os.fspath(Path.cwd() / f'{source}.sock'),
                    )
                )

    def authenticate(self, *, timeout: float = 0.0) -> bool:
        if timeout != 0.0:
            raise AssertionError('monitor tick waited while authenticating a worker')
        self.authentication_calls += 1
        return self.authentication_ready

    def close(self) -> None:
        self.close_calls += 1
        self.operations.append(f'close:{self.spec.kind.value}')

    def has_pending_input(self) -> bool:
        if self.control is not None:
            return self.control.has_pending_input()
        return bool(self.messages)

    def kill(self) -> None:
        if self.on_kill is not None:
            self.on_kill()
        self.killed = True
        self.exit_status = -9
        self.operations.append(f'kill:{self.spec.kind.value}')

    @property
    def last_arrival_at(self) -> float | None:
        return self.control.last_arrival_at if self.control is not None else None

    def receive(self, *, timeout: float | None = None) -> ControlMessage:
        self.receive_timeouts.append(timeout)
        if self.control is not None:
            return self.control.receive(timeout=timeout or 0.0)
        if not self.messages:
            raise TimeoutError('scripted worker has no message')
        message = self.messages.popleft()
        if isinstance(message, SourceEndpointReady):
            self.operations.append(f'receive:endpoint:{message.source}')
        elif isinstance(message, EventPeerReady):
            self.operations.append(f'receive:grant-ready:{message.group}')
        return message

    def receive_available(self) -> Iterator[ControlMessage]:
        self.batch_acquisitions += 1
        if self.control is not None:
            return self.control.receive_available()
        count = len(self.messages)
        return (self.receive(timeout=0.0) for _ in range(count))

    def send(self, message: ControlMessage) -> None:
        if isinstance(message, OperatorCommand):
            self.sent_commands.append(message)
            self.operations.append(f'operator:{message.command}')
            if self.operator_handler is not None:
                self.messages.append(self.operator_handler(message))
            return
        if not isinstance(message, EventPeerGrant):
            raise TypeError(type(message).__name__)
        self.sent_grants.append(message)
        self.operations.append(f'grant:{message.group}')
        self.messages.extend(self.messages_before_grant)
        self.messages_before_grant.clear()
        if self.auto_grant_ready:
            self.messages.append(
                EventPeerReady(
                    source=message.source,
                    group=message.group,
                    dependent_spawn_instance=message.dependent_spawn_instance,
                )
            )

    def shutdown(self, reason: str) -> int | None:
        self.operations.append(f'shutdown:{self.spec.kind.value}:{reason}')
        self.exit_status = self.shutdown_status
        return self.shutdown_status

    def wait(self, *, timeout: float = 0.0) -> int | None:
        if timeout != 0.0:
            raise AssertionError('monitor tick waited while reaping a worker')
        if self.wait_results:
            return self.wait_results.popleft()
        return self.exit_status

    @property
    def worker_id(self) -> str:
        return self.spec.worker_id


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


class ScriptedWorkerFactory:
    def __init__(self, *, authentication_ready: bool = True) -> None:
        self.authentication_ready = authentication_ready
        self.history: dict[str, list[ScriptedWorker]] = {}
        self.instances: dict[str, ScriptedWorker] = {}
        self.operations: list[str] = []
        self.send_deadlines: list[float] = []

    def __call__(self, **options: Unpack[_ScriptedWorkerOptions]) -> ScriptedWorker:
        spec = options['spec']
        spawn_instance = options['spawn_instance']
        self.send_deadlines.append(options['send_deadline_secs'])
        self.operations.append(f'spawn:{spec.kind.value}')
        instance = ScriptedWorker(
            spec,
            spawn_instance,
            self.operations,
            None,
            authentication_ready=self.authentication_ready,
        )
        self.history.setdefault(spec.worker_id, []).append(instance)
        self.instances[spec.worker_id] = instance
        return instance


def _absent_orphan_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    path, config = source_only_config(tmp_path, monkeypatch)
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


def _assert_abandoned_health_prefix(frame: bytes) -> None:
    assert len(frame) > 4
    declared = int.from_bytes(frame[:4], 'big')
    assert declared > 60_000
    assert len(frame) < declared + 4
    padding_start = frame.find(b'"' + b'x' * 64)
    assert padding_start >= 4
    padding = frame[padding_start + 1 :]
    assert 64 <= len(padding) < 60_000
    assert padding == b'x' * len(padding), 'unfinished health frame contains appended bytes'


def _assert_no_lifecycle_charge(probe: SupervisorProbe) -> None:
    assert not probe.terminations, probe.failure_details()
    assert all(not breaker._failure_times for breaker in probe.breakers.values())


def _assert_pending_provider_publication(
    supervisor: Supervisor,
    worker: ScriptedWorker,
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
        assert all(spawn.termination_reason is None for spawn in supervisor._spawns.values())
        assert supervisor._breakers[worker.worker_id]._failure_total == 0
        assert not worker.killed
        assert supervisor.status_mapping({})['groups']
    finally:
        supervisor._clock = original_clock


def _assert_write_wait_policy(probe: SupervisorProbe) -> None:
    probe.wait_for(
        lambda: any(
            watchdog.phase == 'write-lock-wait'
            for health in probe.latest_health.values()
            for watchdog in health.watchdogs
        )
    )
    observed: list[WatchdogHealth] = []
    daemon = probe.supervisor._config.daemon
    # Acquisition retry keeps the production five-second hold component even
    # while the terminal hold detector and its wait-phase grace are lifted.
    acquisition_budget = (
        5.0 + 2 * max(daemon.health_interval_secs, daemon.supervisor_poll_interval_secs) + 1.0
    )
    expected = acquisition_budget + max(5 * daemon.health_interval_secs, STALL_DEADLINE_SECS)
    for health in probe.latest_health.values():
        sources = {source.source: source for source in health.sources}
        for watchdog in health.watchdogs:
            if watchdog.phase == 'write-lock-wait':
                observed.append(watchdog)
                assert probe.supervisor._trusted_watchdog_interval(watchdog, sources) == expected
    assert observed, 'the held writer did not publish a write-lock watchdog'


def _authenticate_worker(worker: _WorkerInstance) -> bool:
    while not worker.authenticate(timeout=0):
        select.select((worker._control.open_socket(),), (), ())
    return True


def _assign_lifecycle_source(probe: SupervisorProbe, worker: _WorkerInstance) -> None:
    lifecycle = probe.supervisor._spawns[worker.worker_id].lifecycle
    assert lifecycle is not None
    assert _authenticate_worker(worker)
    lifecycle.authenticated = True
    probe.supervisor._advance_source_lifecycle(lifecycle)
    assert lifecycle.assigned, probe.failure_details()


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


def _callback_hold_config(tmp_path: Path) -> Path:
    return write_daemon_config(tmp_path, Topology('dedicated', ('remote',)))


def _child_environment() -> dict[str, str]:
    environment = dict(os.environ)
    root = Path(__file__).parents[1]
    environment['PYTHONPATH'] = os.pathsep.join((os.fspath(root / 'src'), os.fspath(root)))
    environment['SG_EVENT_ADMIN_KEY'] = 'test-key'
    return environment


def _child_probe_address(server: ProbeServer) -> tuple[str, int]:
    listener = cast('socket.socket', vars(server)['_listener'])
    host, port = listener.getsockname()[:2]
    return str(host), int(port)


def _child_probe_response(address: tuple[str, int]) -> bytes:
    with socket.create_connection(address) as client:
        client.sendall(b'GET /livez HTTP/1.1\r\n\r\n')
        chunks: list[bytes] = []
        while chunk := client.recv(4_096):
            chunks.append(chunk)
    return b''.join(chunks)


def _complete_readd_disposition(source: SourceState, group: GroupState) -> None:
    source.initialize(100, incarnation='incarnation-1')
    _begin_readd_disposition(source, group, 'reset-to-latest')


def _control_events(root: Path, worker: _WorkerInstance) -> list[dict[str, object]]:
    path = root / f'{worker.spawn_instance}.control.jsonl'
    return [json.loads(line) for line in read_complete_lines(path) if line]


def _in_flight_group_health(
    worker: ScriptedWorker,
    *,
    heartbeat_at: float,
    group: str = 'remote',
    phase: Literal['callback', 'idle', 'internal', 'write-lock-wait', 'write-hold'] = 'callback',
) -> GroupHealth:
    return GroupHealth(
        site=worker.spec.site,
        source='admin',
        group=group,
        worker_instance=worker.spawn_instance,
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
        conditions=(),
        deadline_at=heartbeat_at + (1.25 if phase == 'idle' else 10_000.0),
    )


def _delay_worker_batch(
    monkeypatch: pytest.MonkeyPatch,
    worker: ScriptedWorker,
    inject: Callable[[], None],
    *,
    timing: str,
) -> None:
    acquire = worker.receive_available
    injected = False

    def delayed() -> Iterator[ControlMessage]:
        nonlocal injected
        batch = acquire()

        def messages() -> Iterator[ControlMessage]:
            nonlocal injected
            for message in batch:
                yield message
                if not injected and timing == 'iteration':
                    injected = True
                    inject()
            if not injected:
                injected = True
                inject()

        return messages()

    monkeypatch.setattr(worker, 'receive_available', delayed)


def _expire_shutdown_grace(monkeypatch: pytest.MonkeyPatch, worker: _WorkerInstance) -> None:
    worker._shutdown_deadline_secs = 10.0
    clock = FakeClock()
    production_reap = supervisor_module.reap_child

    def reap(process: ChildProcess, *, timeout: float = 0) -> int | None:
        if timeout == 0:
            return production_reap(process)
        if clock() >= worker._shutdown_deadline_secs:
            return process.wait()
        clock.advance(timeout)
        return None

    monkeypatch.setattr(supervisor_module, 'time', SimpleNamespace(monotonic=clock))
    monkeypatch.setattr(supervisor_module, 'reap_child', reap)


def _first_lifecycle_worker(probe: SupervisorProbe) -> _WorkerInstance:
    return cast('_WorkerInstance', next(iter(probe.supervisor._spawns.values())).worker)


def _force_reap_test_process(process: BaseProcess) -> None:
    process.kill()
    process.join()
    assert not process.is_alive()
    assert process.exitcode is not None


def _idle_source_health(
    worker: ScriptedWorker,
    *,
    source: str,
    heartbeat_at: float,
) -> SourceHealth:
    assignment = worker.assignment
    generations = {} if assignment is None else dict(assignment.source_generations)
    return SourceHealth(
        site=worker.spec.site,
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
        worker_instance=worker.spawn_instance,
        initialized=True,
        phase='wait',
        heartbeat_at=heartbeat_at,
        deadline_at=heartbeat_at + 1.25,
    )


def health_update(
    worker: ScriptedWorker,
    *,
    heartbeat_at: float,
    groups: tuple[GroupHealth, ...] | None = None,
    sources: tuple[SourceHealth, ...] | None = None,
    watchdogs: tuple[WatchdogHealth, ...] | None = None,
) -> HealthUpdate:
    current_sources = (
        tuple(
            _idle_source_health(worker, source=source, heartbeat_at=heartbeat_at)
            for source in worker.spec.sources
        )
        if sources is None
        else sources
    )
    current_groups = (
        tuple(
            _in_flight_group_health(worker, group=group, heartbeat_at=heartbeat_at, phase='idle')
            for group in worker.spec.groups
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
                    worker_instance=worker.spawn_instance,
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
                    worker_instance=worker.spawn_instance,
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
                site=worker.spec.site,
                source=None,
                group=None,
                worker_instance=worker.spawn_instance,
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
            site=worker.spec.site,
            worker_id=worker.worker_id,
            spawn_instance=worker.spawn_instance,
            heartbeat_at=heartbeat_at,
            monitor_alive=True,
            thread_names=(),
            stuck_threads=(),
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
    complete = _SupervisorState.complete_orphan_disposition

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
    monkeypatch.setattr(_SupervisorState, 'complete_orphan_disposition', complete_with_failure)


def _install_source_grant_control_failure(
    monkeypatch: pytest.MonkeyPatch,
    source: ScriptedWorker,
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

        monkeypatch.setattr(source, 'send', fail_send)
        monkeypatch.setattr(source, 'receive', reject_post_failure_receive)
    else:

        def fail_receive(*, timeout: float | None = None) -> ControlMessage:
            assert timeout == 0.0
            failure_calls.append(None)
            raise error_factory()

        monkeypatch.setattr(source, 'receive', fail_receive)
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
        assignment=delivery,
        credential=site.credentials[source.credentials],
        group=group,
        holdback_secs=0.0,
        site=site,
        source_config=source,
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
    for message in control.receive_available():
        assert isinstance(message, HealthUpdate), message
        latest = message
    return latest


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


def _limited_startup(config_path: Path, *, hard: int) -> tuple[dict[str, object], str]:
    code = (
        'from pathlib import Path\n'
        'from tests._worker_processes import run_open_file_startup\n'
        f'run_open_file_startup(Path({str(config_path)!r}))\n'
    )
    launcher = (
        'import os, resource, sys\n'
        f'resource.setrlimit(resource.RLIMIT_NOFILE, (256, {hard}))\n'
        f'os.execv(sys.executable, [sys.executable, "-c", {code!r}])\n'
    )
    result = subprocess.run(
        [sys.executable, '-c', launcher], capture_output=True, text=True, check=True
    )
    print(f'child stdout:\n{result.stdout}\nchild stderr:\n{result.stderr}')
    log, _, outcome = result.stdout.rstrip().rpartition('\n')
    return json.loads(outcome), log + result.stderr


def _local_and_remote_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    path, _config = source_only_config(tmp_path, monkeypatch)
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


def _maximum_status_snapshot(*, reverse: bool = False) -> _StatusSnapshot:
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
    return status_from_health(
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


def _park_evidence(
    **options: Unpack[_ParkEvidenceOptions],
) -> _ParkEvidence:
    return _ParkEvidence(
        directive=ConditionalParkDirective(
            evidence_id=options.get('evidence_id', 'evidence-1'),
            site=options.get('site', 'test'),
            group=options.get('group', 'remote'),
            group_generation=options.get('group_generation', 1),
            failed_spawn_instance=options.get(
                'failed_spawn_instance',
                'dependent-spawn-1',
            ),
            event_id=options.get('event_id', 42),
            group_revision=options.get('group_revision', 7),
            observed_at_wall_clock=options.get('observed_at_wall_clock', 10.0),
        ),
        failed_worker_id=options.get('failed_worker_id', 'dependent'),
    )


def _parked_group_rows(state: _SupervisorState) -> list[tuple[str, str, int]]:
    rows = state._database.connection.execute(
        'SELECT site, group_name, group_generation FROM group_inventory '
        "WHERE configured = 1 AND lifecycle = 'readded-parked' "
        'ORDER BY site, group_name'
    ).fetchall()
    return [(str(row[0]), str(row[1]), int(row[2])) for row in rows]


def _never_reap(*, timeout: float = 0.0) -> None:
    assert timeout == 0.0


def _pending_test_breaker() -> _WorkerBreaker:
    return _WorkerBreaker(
        worker_id='pending-worker',
        restart_limit=2,
        restart_window_secs=100,
        healthy_reset_secs=0.1,
    )


def _pending_restart_child(state_dir: str, connection: Connection) -> None:
    try:
        with _SupervisorState.open(Path(state_dir)) as state:
            breaker = _pending_test_breaker()
            state.persist_worker_breaker(breaker)
            connection.send('ready')
            assert connection.recv() == 'fail'
            for _ in range(3):
                breaker.record_failure('pending failure')
                state.persist_worker_breaker(breaker)
            connection.send(('pending-open', breaker.is_open))
            os.kill(os.getpid(), signal.SIGKILL)
    except (AssertionError, OSError, RuntimeError, ValueError) as error:
        connection.send(repr(error))


def _prepare_readd_inventory(path: Path) -> None:
    with _SupervisorState.open(path) as state:
        state.reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 1},
        )
        state.reconcile_groups(
            configured={},
            generations={('test', 'remote'): 1},
        )
        state.reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 1},
        )


def _read_pipe(fd: int, size: int) -> bytes:
    select.select((fd,), (), ())
    result = os.read(fd, size)
    assert result, 'child closed readiness pipe'
    return result


def _ready_control_probe(probe: SupervisorProbe) -> list[_WorkerInstance]:
    probe.wait_for(
        lambda: (
            bool(probe.supervisor._spawns)
            and all(
                spawn.lifecycle is None and spawn.health_inventory_complete
                for spawn in probe.supervisor._spawns.values()
            )
            and bool(probe.supervisor.probe_mapping()['complete'])
        ),
    )
    return [cast('_WorkerInstance', spawn.worker) for spawn in probe.supervisor._spawns.values()]


def _grant_during_source_page(
    probe: SupervisorProbe, root: Path
) -> tuple[_WorkerInstance, _WorkerInstance]:
    workers = _ready_control_probe(probe)
    source = next(worker for worker in workers if worker.spec.sources)
    dependent = next(worker for worker in workers if not worker.spec.sources)
    (root / 'hold-page').touch()
    probe.wait_for((root / 'page-entered').exists)
    probe.supervisor._replace_worker(dependent.worker_id)
    dependent = cast('_WorkerInstance', probe.supervisor._spawns[dependent.worker_id].worker)
    probe.wait_for(
        lambda: bool(
            (life := probe.supervisor._spawns[dependent.worker_id].lifecycle) and life.grant_sent
        ),
    )
    return source, dependent


def _receive_source_endpoint_ready(worker: _WorkerInstance) -> SourceEndpointReady:
    while True:
        message = receive_control(worker._control)
        if isinstance(message, SourceEndpointReady):
            return message


def _release_observed_worker_lock(
    monkeypatch: pytest.MonkeyPatch, path: Path, release_fd: int, process: ChildProcess
) -> list[Path]:
    observed: list[Path] = []

    def acquire(current: Path) -> platform_module.FileLock:
        try:
            return acquire_lock(current)
        except AlreadyLockedError:
            if current != path:
                raise
            observed.append(current)
            os.write(release_fd, b'R')
            assert process.wait() == 0
            return acquire_lock(current)

    monkeypatch.setattr(supervisor_module, 'acquire_lock', acquire)
    return observed


def _replace_config_text(text: str, old: str, new: str) -> str:
    assert text.count(old) == 1, f'expected exactly one configuration fragment: {old!r}'
    return text.replace(old, new)


def _default_packed_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    config_path, _config = source_only_config(tmp_path, monkeypatch)
    config_path.write_text(
        _replace_config_text(config_path.read_text(encoding='utf-8'), 'worker = "pool"\n', ''),
        encoding='utf-8',
    )
    return config_path, load_config(config_path)


def _reveal_callback_events(root: Path, last_id: int) -> None:
    # The child provider reads this frontier concurrently.
    pending = root / 'visible-events.pending'
    pending.write_text(str(last_id))
    pending.replace(root / 'visible-events')


def _socket_preflight_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, root: Path, state_bytes: int
) -> tuple[Path, Config]:
    config_path, _ = source_only_config(tmp_path, monkeypatch)
    state = root / ('s' * max(1, state_bytes - len(os.fsencode(root)) - 1))
    config_path.write_text(config_path.read_text().replace(str(tmp_path / 'state'), str(state)))
    (tmp_path / 'state-location.json').write_text(
        json.dumps(
            {
                'state_dir': str(state),
                'source_candidate_bytes': len(
                    os.fsencode(state / 'sites/test/sources/admin-events.sock')
                ),
            }
        )
    )
    return config_path, load_config(config_path)


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


def _silence_pass(supervisor: Supervisor, worker: ScriptedWorker, owner: str) -> None:
    if owner == 'operator':
        supervisor._accept_operator_batch(
            worker, _source_adoption_command('silence'), worker.receive_available()
        )
    else:
        supervisor._advance_worker_messages()


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


def _source_watchdog(
    worker: ScriptedWorker,
    *,
    deadline_at: float,
    heartbeat_at: float,
    phase: Literal['internal', 'provider-query', 'shutdown', 'startup', 'wait'],
) -> WatchdogHealth:
    source = worker.spec.sources[0]
    return WatchdogHealth(
        kind='source',
        site=worker.spec.site,
        source=source,
        group=None,
        worker_instance=worker.spawn_instance,
        phase=phase,
        heartbeat_at=heartbeat_at,
        deadline_at=deadline_at,
    )


def _spawn_health(supervisor: Supervisor, worker_id: str) -> HealthUpdate:
    health = supervisor._spawns[worker_id].health
    assert health is not None
    return health


def _spawn_lifecycle(supervisor: Supervisor, worker_id: str) -> supervisor_module._WorkerLifecycle:
    lifecycle = supervisor._spawns[worker_id].lifecycle
    assert lifecycle is not None
    return lifecycle


def _stage_post_startup_grant(
    supervisor: Supervisor, source: ScriptedWorker, dependent: ScriptedWorker
) -> None:
    supervisor._complete_worker_lifecycle(_spawn_lifecycle(supervisor, source.worker_id))
    supervisor._config = replace(
        supervisor._config, daemon=replace(supervisor._config.daemon, health_interval_secs=2.0)
    )
    assert dependent.assignment is not None
    supervisor._spawns[dependent.worker_id].lifecycle = supervisor_module._WorkerLifecycle(
        step_started_at=100,
        assignment=dependent.assignment,
        deadline_at=500,
        worker=dependent,
        authenticated=True,
        grant_sent=True,
    )


def _stage_source_recovery(
    supervisor: Supervisor, source: ScriptedWorker, evidence: _ParkEvidence
) -> _SupervisorState:
    assert source.assignment is not None
    state = supervisor._state
    assert state is not None
    assert state.record_park_evidence(evidence)
    supervisor._park_evidence_by_group[('test', 'local')] = evidence
    supervisor._spawns[source.worker_id].lifecycle = supervisor_module._WorkerLifecycle(
        step_started_at=0.0,
        assignment=source.assignment,
        deadline_at=10,
        worker=source,
        authenticated=True,
        assigned=True,
        ready=True,
    )
    supervisor._spawns[source.worker_id].pending_recovery = {
        evidence.directive.evidence_id: evidence.directive
    }
    return state


def _start_blocked_dependent_grant(
    supervisor: Supervisor,
    factory: ScriptedWorkerFactory,
) -> tuple[ScriptedWorker, ScriptedWorker]:
    supervisor._reconcile(generations={})
    supervisor.start()
    source = next(worker for worker in factory.instances.values() if worker.spec.sources)
    source.authentication_ready = True
    supervisor._tick()
    dependent = next(worker for worker in factory.instances.values() if not worker.spec.sources)
    source.auto_grant_ready = False
    dependent.authentication_ready = True
    supervisor._tick()
    return source, dependent


def _startup_hold_child(config: str, connection: Connection) -> None:
    root = logging.getLogger('sg_event_daemon.supervisor')
    handler = logging.FileHandler(Path(config).parent / 'startup.log')
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    try:
        with Supervisor(config=load_config(Path(config)), config_path=Path(config)) as supervisor:
            connection.send('opened')
            state = supervisor._state
            assert state is not None
            clock = FakeClock()
            state._clock = clock
            acquire = state._write_transaction
            attempts = 0

            @contextmanager
            def observed(timeout_millis: int) -> Generator[sqlite3.Connection]:
                nonlocal attempts
                assert timeout_millis == 1000
                attempts += 1
                clock.advance(1)
                if attempts == 8:
                    connection.send('waiting')
                    assert connection.recv() == 'released'
                with acquire(0) as acquired:
                    yield acquired

            state._write_transaction = observed
            supervisor.reconcile_durable_state()
            state._write_transaction = acquire
            connection.send('reconciled')
    except (AssertionError, OSError, RuntimeError, ValueError) as error:
        connection.send(repr(error))
    finally:
        handler.close()
        connection.close()


def _supervisor_park_crash_child(config: str, busy: bool, connection: Connection) -> None:
    with pytest.MonkeyPatch.context() as policy:
        activate_stall_policy(policy)
        path = Path(config)
        _reveal_callback_events(path.parent, 1)
        try:
            with running_supervisor(
                path, ChildPatches(hold_callback=True, gate_events=True)
            ) as probe:
                probe.wait_for(
                    lambda: (
                        len(probe.latest_health) == 2
                        and any(
                            group.group == 'remote' and group.join_phase == 'attached'
                            for health in probe.latest_health.values()
                            for group in health.groups
                        )
                    )
                )
                connection.send(
                    ('ready', [(item.process.pid, item.worker_id) for item in probe.spawns])
                )
                assert connection.recv() == 'arm'
                original = _WorkerInstance.kill

                def crash_at_kill(worker: _WorkerInstance) -> None:
                    if busy:
                        original(worker)
                        worker._process.wait()
                    connection.send(('boundary', busy))
                    os.kill(os.getpid(), signal.SIGKILL)

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(_WorkerInstance, 'kill', crash_at_kill)
                    (path.parent / 'callback-arm').touch()
                    _reveal_callback_events(path.parent, 2)
                    probe.wait_for((path.parent / 'callback-entered').exists)
                    (path.parent / 'callback-expire').touch()
                    probe.wait_for(lambda: False)
        except (AssertionError, OSError, RuntimeError, ValueError) as error:
            connection.send(('error', repr(error)))


def _two_dependent_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Config]:
    path, _config = source_only_config(tmp_path, monkeypatch)
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
    path, _config = source_only_config(tmp_path, monkeypatch)
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


def _unbounded_control_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    read_frame = control_module._read_frame

    def read(sock: socket.socket, *, deadline_at: float | None = None) -> bytes:
        result = read_frame(sock, deadline_at=deadline_at)
        sock.settimeout(None)
        return result

    def remaining(_deadline: float) -> None:
        return None

    monkeypatch.setattr(control_module, '_remaining_timeout', remaining)
    monkeypatch.setattr(control_module, '_read_frame', read)


def _spawn_worker_with_stall_policy(
    argv: list[str],
    *,
    pass_fds: tuple[int, ...] = (),
    env: Mapping[str, str] | None = None,
) -> ChildProcess:
    assert argv[1:4] == ['-P', '-m', 'sg_event_daemon']
    bootstrap = (
        'import runpy, sys\n'
        'import pytest\n'
        'from tests._stall_policy import activate_stall_policy\n'
        'with pytest.MonkeyPatch.context() as patch:\n'
        f'    activate_stall_policy(patch, enabled={active_stall_detectors()!r})\n'
        f'    sys.argv = {argv[3:]!r}\n'
        '    runpy.run_module("sg_event_daemon", run_name="__main__", alter_sys=True)\n'
    )
    return spawn_child([argv[0], '-P', '-c', bootstrap], pass_fds=pass_fds, env=env)


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
    commit_step(state, StartEventStep(42, manifest, None), expected_revision=5)
    started = commit_step(state, BeginCallbackStep(0), expected_revision=6)
    assert started.revision == 7
    return state


def _wait_lifecycle_file(path: Path, *, worker: _WorkerInstance) -> None:
    while True:
        exit_status = worker._process.poll()
        if path.exists():
            return
        assert exit_status is None, f'{path.name}: child exited'
        time.sleep(0.002)


def _killed_startup_monitor(
    tmp_path: Path, stall_detectors: StallDetectors, *, partial: bool, health: float
) -> None:
    config = _lifecycle_probe_config(tmp_path, health=health)
    (tmp_path / 'hold-server').touch()
    with running_supervisor(
        config, ChildPatches(control_probe=True, lifecycle_probe=True, last_event_id=4)
    ) as probe:
        worker = _first_lifecycle_worker(probe)
        if partial:
            (tmp_path / f'{worker.spawn_instance}.large').touch()
        (tmp_path / f'{worker.spawn_instance}.write-once').touch()
        _assign_lifecycle_source(probe, worker)
        _wait_lifecycle_file(tmp_path / f'{worker.spawn_instance}.writer-frozen', worker=worker)
        lifecycle = probe.supervisor._spawns[worker.worker_id].lifecycle
        assert lifecycle is not None
        probe.supervisor._advance_worker_messages()
        arrival = worker.last_arrival_at
        assert arrival is not None
        if partial:
            assert probe.supervisor._spawns[worker.worker_id].health is None
        else:
            assert probe.supervisor._spawns[worker.worker_id].health is not None
        lifecycle.deadline_at = arrival + 30.0
        expected = max(lifecycle.deadline_at, arrival + 5 * health)
        assert (5 * health < lifecycle.deadline_at - arrival) is (health == 2.0)
        with stall_detectors.enabled('startup', 'heartbeat'):
            clock = FakeClock()
            clock.now = expected - 0.001
            original_clock = probe.supervisor._clock
            probe.supervisor._clock = clock
            try:
                probe.supervisor._advance_worker_messages()
                assert not probe.terminations, probe.failure_details()
                clock.now = expected
                probe.supervisor._advance_worker_messages()
                assert len(probe.terminations) == 1, probe.failure_details()
                assert probe.terminations[0].reason == 'worker-startup-timeout'
            finally:
                probe.supervisor._clock = original_clock


def _create_recovery_group_database(
    config: Config,
    *,
    group: str = 'remote',
) -> None:
    path = source_database(config.daemon.state_dir)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with _conditional_group(path, group=group):
        pass


def _project_status(
    snapshot: _StatusSnapshot,
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


def _delay_reserved_operator_batches(
    supervisor: Supervisor,
    owner: ScriptedWorker,
    sender: socket.socket,
    monkeypatch: pytest.MonkeyPatch,
    *,
    outcome: str,
) -> list[bool]:
    clock = cast('FakeClock', supervisor._clock)
    original_grants = tuple(owner.sent_grants)
    acquire = owner.receive_available
    acquisitions = owner.batch_acquisitions
    reserved_reads: list[bool] = []
    receive = owner.receive
    send = owner.send

    def receive_reserved(*, timeout: float) -> ControlMessage:
        reserved_reads.append(supervisor._dispatch_reserved is owner)
        assert not owner.killed
        return receive(timeout=timeout)

    def acquire_delayed() -> Iterator[ControlMessage]:
        batch = acquire()

        def messages() -> Iterator[ControlMessage]:
            yield from batch
            assert supervisor._dispatch_reserved is owner
            assert tuple(owner.sent_grants) == original_grants
            clock.now = 112
            if outcome == 'eof':
                sender.close()
            else:
                sender.sendall(
                    ipc_module.encode_control_message(
                        health_update(
                            owner,
                            heartbeat_at=111.99
                            if owner.batch_acquisitions == acquisitions + 1
                            else 112,
                        )
                    )
                )
                if outcome == 'result' and owner.batch_acquisitions == acquisitions + 1:
                    sent = owner.sent_commands[-1]
                    sender.sendall(
                        ipc_module.encode_control_message(
                            CommandResult(
                                command=sent.command,
                                ok=True,
                                message='done',
                                request_id=sent.request_id,
                                result={},
                            )
                        )
                    )

        return messages()

    def send_grant(message: ControlMessage) -> None:
        send(message)
        while owner.messages:
            sender.sendall(ipc_module.encode_control_message(owner.messages.popleft()))

    monkeypatch.setattr(owner, 'receive', receive_reserved)
    monkeypatch.setattr(owner, 'receive_available', acquire_delayed)
    monkeypatch.setattr(owner, 'send', send_grant)
    return reserved_reads


def _recovery_ready(
    worker: ScriptedWorker,
    evidence: _ParkEvidence,
    *,
    outcome: Literal['parked', 'stale'] = 'parked',
    resulting_revision: int | None = None,
    spawn_instance: str | None = None,
) -> ControlMessage:
    ready_type = vars(ipc_module).get('GroupRecoveryReady')
    assert ready_type is not None
    revision = (
        evidence.directive.group_revision + 1 if resulting_revision is None else resulting_revision
    )
    return cast(
        'ControlMessage',
        ready_type(
            spawn_instance=(worker.spawn_instance if spawn_instance is None else spawn_instance),
            evidence_id=evidence.directive.evidence_id,
            site=evidence.directive.site,
            group=evidence.directive.group,
            group_generation=evidence.directive.group_generation,
            outcome=outcome,
            resulting_revision=revision,
        ),
    )


@pytest.mark.parametrize('interval', [0.1, 0.01])
def test_trusted_interval_for_write_phases_ignores_small_internal_budget(
    stall_detectors: StallDetectors,
    interval: float,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('phase', 'coordination')
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
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
            health_interval_secs=interval,
            supervisor_poll_interval_secs=interval,
        ),
        sites=MappingProxyType({'test': site}),
    )
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        assert supervisor._internal_watchdog_budget('test', 'admin') == max(10 * interval, 0.2)
        cases: tuple[tuple[WatchdogKind, str | None], ...] = (
            ('lane', 'remote'),
            ('source', None),
        )
        phase_budgets: tuple[tuple[WatchdogPhase, float], ...] = (
            ('write-hold', 5.0),
            (
                'write-lock-wait',
                write_lock_timeout_secs(interval, interval) + watchdog_grace_secs(interval),
            ),
        )
        for kind, group_name in cases:
            for phase, expected in phase_budgets:
                watchdog = WatchdogHealth(
                    kind=kind,
                    site='test',
                    source='admin',
                    group=group_name,
                    worker_instance=dependent.spawn_instance,
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
            worker_instance=dependent.spawn_instance,
            phase='write-hold',
            heartbeat_at=0.0,
            deadline_at=4.0,
        )
        with monkeypatch.context() as patch:
            patch.setattr(write_lock_module, 'WRITE_HOLD_DEADLINE_SECS', 6.0)
            assert supervisor._trusted_watchdog_interval(lane_watchdog, {}) == 6.0
            assert supervisor._trusted_watchdog_interval(
                replace(lane_watchdog, phase='write-lock-wait'), {}
            ) == write_lock_timeout_secs(interval, interval) + watchdog_grace_secs(interval)
        operator = WatchdogHealth(
            kind='operator',
            site='test',
            source=None,
            group=None,
            worker_instance=dependent.spawn_instance,
            phase='idle',
            heartbeat_at=0.0,
            deadline_at=None,
        )
        dependent.messages.append(
            health_update(
                dependent,
                heartbeat_at=0.0,
                groups=(group,),
                watchdogs=(lane_watchdog, operator),
            )
        )
        source.messages.append(health_update(source, heartbeat_at=0.0))
        supervisor._tick()
        clock.advance(2.0)
        dependent.messages.append(
            health_update(
                dependent,
                heartbeat_at=clock(),
                groups=(group,),
                watchdogs=(lane_watchdog, operator),
            )
        )
        source.messages.append(health_update(source, heartbeat_at=clock()))
        supervisor._tick()

        assert not dependent.killed
        assert supervisor._spawns[dependent.worker_id].termination_reason is None
    finally:
        supervisor._close()


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


@pytest.mark.parametrize('message_kind', ['fatal', 'recovery'])
def test_waiting_grant_owner_preserves_consumed_source_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, message_kind: str
) -> None:
    config_path, config = _local_and_remote_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        assert dependent.assignment is not None
        assert not supervisor._accept_worker_health(source, health_update(source, heartbeat_at=0))
        source.auto_grant_ready = False
        supervisor._spawns[dependent.worker_id].lifecycle = supervisor_module._WorkerLifecycle(
            step_started_at=0.0,
            assignment=dependent.assignment,
            deadline_at=config.daemon.worker_startup_deadline_secs,
            worker=dependent,
            authenticated=True,
        )
        # Establish the grant owner before queuing source recovery traffic.
        supervisor._advance_dependent_lifecycle(_spawn_lifecycle(supervisor, dependent.worker_id))
        _spawn_lifecycle(supervisor, dependent.worker_id).deadline_at = 0.1
        evidence = _park_evidence(group='local')
        assert supervisor._state is not None
        if message_kind == 'fatal':
            healthy = health_update(source, heartbeat_at=0)
            message = replace(healthy, worker=replace(healthy.worker, monitor_alive=False))
        else:
            _stage_source_recovery(supervisor, source, evidence)
            message = _recovery_ready(source, evidence)
        tail = health_update(source, heartbeat_at=0.1)
        late_ready = EventPeerReady(
            source=dependent.assignment.event_grants[0].source,
            group=dependent.assignment.event_grants[0].group,
            dependent_spawn_instance=dependent.spawn_instance,
        )
        with _socket_worker_channel(source) as (control, sender):
            monkeypatch.setattr(ipc_module, 'time', SimpleNamespace(monotonic=clock))
            for current in (
                (message, tail) if message_kind == 'fatal' else (message, late_ready, tail)
            ):
                sender.sendall(ipc_module.encode_control_message(current))
            acquire = source.receive_available
            observed = _observe_delayed_grant_batch(monkeypatch, source, clock, sender)
            dependent.assignment = None
            acquisitions = source.batch_acquisitions
            supervisor._advance_worker_messages()
            assert not dependent.killed
            assert source.batch_acquisitions == acquisitions + 1
            if message_kind == 'fatal':
                assert source.killed
                assert (
                    supervisor._spawns[source.worker_id].termination_reason
                    == 'worker-heartbeat-overdue'
                )
                assert observed == [message]
                # The later frame stays in the transport decoder; no retired-owner receive.
                assert bytes(
                    vars(control._reader)['_buffer']
                ) == ipc_module.encode_control_message(tail)
            else:
                assert not source.killed
                assert observed == [message, late_ready, tail]
                assert late_ready not in supervisor._stale_grant_readies
                assert supervisor._state.pending_park_evidence() == ()
                assert control.has_pending_input()
                monkeypatch.setattr(source, 'receive_available', acquire)
                supervisor._advance_worker_messages()
                assert source.batch_acquisitions == acquisitions + 2
                assert not control.has_pending_input()
                assert _spawn_health(supervisor, source.worker_id).worker.heartbeat_at == 0.1


@pytest.mark.parametrize('owner', ['steady', 'startup', 'ready', 'grant', 'operator'])
@pytest.mark.parametrize('known', [False, True])
def test_channel_owners_accept_only_exact_fenced_grant_replies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, owner: str, known: bool
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=FakeClock(), worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        assert source.assignment is not None
        assert dependent.assignment is not None
        grant = dependent.assignment.event_grants[0]
        fenced = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=dependent.spawn_instance,
        )
        supervisor._stale_grant_readies.add(fenced)
        message = fenced if known else replace(fenced, dependent_spawn_instance='unknown-spawn')
        lifecycle = supervisor_module._WorkerLifecycle(
            step_started_at=0.0,
            assignment=dependent.assignment if owner == 'grant' else source.assignment,
            deadline_at=10,
            worker=dependent if owner == 'grant' else source,
            authenticated=True,
            assigned=True,
            ready=owner == 'ready',
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
            source.messages.append(message)
            if owner == 'ready':
                return supervisor._advance_ready_lifecycle(lifecycle)
            return supervisor._accept_operator_batch(
                source,
                _source_adoption_command('fenced-reply'),
                source.receive_available(),
            )

        if known:
            dispatch()
            assert fenced not in supervisor._stale_grant_readies
        else:
            with pytest.raises((TypeError, ValueError)):
                dispatch()
            assert fenced in supervisor._stale_grant_readies
        assert not source.killed
        assert not lifecycle.endpoint_messages
        assert lifecycle.grant_index == 0


def _observe_delayed_grant_batch(
    monkeypatch: pytest.MonkeyPatch,
    source: ScriptedWorker,
    clock: FakeClock,
    sender: socket.socket,
) -> list[ControlMessage]:
    observed: list[ControlMessage] = []
    acquire = source.receive_available
    later = health_update(source, heartbeat_at=0.1)

    def cross_deadline() -> Iterator[ControlMessage]:
        batch = acquire()

        def messages() -> Iterator[ControlMessage]:
            for current in batch:
                observed.append(current)
                clock.now = 0.1
                yield current
            sender.sendall(ipc_module.encode_control_message(later))

        return messages()

    monkeypatch.setattr(source, 'receive_available', cross_deadline)
    return observed


@pytest.mark.parametrize('command_name', ['replay', 'reset-group-to-latest'])
def test_group_disposition_forwards_validated_arguments(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, command_name: str
) -> None:
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    prepare_absent_orphan(config)
    factory = ScriptedWorkerFactory()
    arguments: dict[str, object] = {'site': 'test', 'group': 'remote', 'reason': 'recover'}
    if command_name == 'replay':
        arguments.update(from_event_id=50, acknowledge_truncation=True)
    command = OperatorCommand(
        request_id='validated-values',
        command=command_name,
        arguments=arguments,
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    ) as supervisor:
        supervisor.reconcile_durable_state()
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        source.operator_handler = lambda sent: CommandResult(
            command=sent.command,
            request_id=sent.request_id,
            ok=False,
            message='captured',
            result={},
        )
        supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert len(source.sent_commands) == 1
        forwarded = source.sent_commands[0]
        assert forwarded.command == 'group-disposition'
        assert forwarded.arguments['disposition_id'] == supervisor_module.stable_operator_id(
            command.command, command.arguments
        )
        assert forwarded.arguments['from_event_id'] == (50 if command_name == 'replay' else None)
        assert forwarded.arguments['acknowledge_truncation'] is (command_name == 'replay')
        assert forwarded.arguments['reason'] == 'recover'
        assert forwarded.arguments['operator_label'] == 'operator uid=501'


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
    assert len(control.messages) == 1
    return control.messages[0]


@pytest.mark.parametrize(
    'running_lane', [False, True], ids=['before_reservation', 'before_lane_dispatch']
)
def test_group_disposition_rejects_invalid_position(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, running_lane: bool
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
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
            'from_event_id': 0,
            'acknowledge_truncation': False,
        },
        identity=AuditIdentity.current(),
    )
    with Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    ) as supervisor:
        supervisor.reconcile_durable_state()
        if running_lane:
            supervisor.start()
            assert any('remote' in worker.spec.groups for worker in factory.instances.values())
        assert supervisor._state is not None
        before = supervisor._state.inventory_records()[('test', 'remote')]
        with pytest.raises(ValueError, match='replay position must be a positive event ID'):
            supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert supervisor._state.inventory_records()[('test', 'remote')] == before
        assert all(not worker.sent_commands for worker in factory.instances.values())


@pytest.mark.parametrize('accepted_result', [False, True])
def test_operator_batch_exhaustion_does_not_extend_absolute_deadline(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    accepted_result: bool,
) -> None:
    stall_detectors.enable('control')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    monkeypatch.setattr(time, 'monotonic', clock)
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory.instances.values() if item.spec.groups)
        worker.operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={},
        )

        if not accepted_result:
            worker.operator_handler = None
            worker.messages.append(health_update(worker, heartbeat_at=0))

        def slow_empty_batch() -> Iterator[ControlMessage]:
            clock.advance(2)
            yield health_update(worker, heartbeat_at=clock())

        monkeypatch.setattr(worker, 'receive_available', slow_empty_batch)
        command = OperatorCommand(
            request_id='batch-timeout',
            command='park',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=456),
        )
        if accepted_result:
            assert supervisor.dispatch_operator_command(command, timeout=1).ok
        else:
            with pytest.raises(
                OperatorOutcomeUnknownError, match='worker did not answer within 1 second'
            ):
                supervisor.dispatch_operator_command(command, timeout=1)
        assert clock() == 2
        assert not worker.killed


@pytest.fixture
def short_root() -> Iterator[Path]:
    # Exact-length state paths need a root shorter than pytest's base temporary directory, which
    # can already be longer than a Unix socket address.
    with tempfile.TemporaryDirectory(prefix='sged-', dir=Path(os.sep) / 'tmp') as directory:
        yield Path(directory).resolve()


@pytest.mark.parametrize('heartbeat', [None, 0.0, 0.5])
def test_source_readiness_preserves_current_spawn_health_and_startup_deadline(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    heartbeat: float | None,
) -> None:
    stall_detectors.enable('heartbeat', 'startup')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(iter(factory.instances.values()))
        original_deadline = supervisor._spawns[source.worker_id].health_deadline
        source.authentication_ready = True
        assign = source.assign
        accepted: list[HealthUpdate] = []

        def assign_with_health(assignment: WorkerAssignment) -> None:
            assign(assignment)
            if heartbeat is not None:
                update = health_update(source, heartbeat_at=heartbeat)
                source.messages.appendleft(update)
                accepted.append(update)

        monkeypatch.setattr(source, 'assign', assign_with_health)
        clock.now = 0.5
        supervisor._advance_worker_messages()
        assert source.assignment is not None
        assert not source.messages
        if heartbeat is None:
            assert not source.killed
            assert supervisor._spawns[source.worker_id].health_deadline == original_deadline + 0.5
        else:
            assert len(accepted) == 1
            assert _spawn_health(supervisor, source.worker_id).worker.heartbeat_at == heartbeat
            assert supervisor._spawns[source.worker_id].health_deadline == heartbeat + 0.25
            assert source.killed is (heartbeat == 0)


@pytest.mark.parametrize('expire', [False, True])
def test_source_recovery_restart_uses_finite_health_acquisition(
    stall_detectors: StallDetectors, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, expire: bool
) -> None:
    stall_detectors.enable('startup')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    monkeypatch.setattr(time, 'sleep', clock.advance)
    arrivals: dict[str, float] = {}
    receive = ScriptedWorker.receive

    def receive_with_arrival(
        worker: ScriptedWorker, *, timeout: float | None = None
    ) -> ControlMessage:
        message = receive(worker, timeout=timeout)
        arrivals[worker.spawn_instance] = clock()
        return message

    def last_arrival(worker: ScriptedWorker) -> float | None:
        return arrivals.get(worker.spawn_instance)

    monkeypatch.setattr(ScriptedWorker, 'receive', receive_with_arrival)
    monkeypatch.setattr(ScriptedWorker, 'last_arrival_at', property(last_arrival))
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        originals = tuple(factory.instances.values())
        counts = {worker.worker_id: worker.batch_acquisitions for worker in originals}
        assign = ScriptedWorker.assign
        injections: list[ScriptedWorker] = []

        def assign_with_history(worker: ScriptedWorker, assignment: WorkerAssignment) -> None:
            assign(worker, assignment)
            if worker.spec.sources:
                injections.append(worker)
                old = health_update(worker, heartbeat_at=0)
                fresh = health_update(worker, heartbeat_at=0.5)
                worker.messages.extendleft((fresh, old))
                clock.now = config.daemon.worker_startup_deadline_secs if expire else 0.5

        monkeypatch.setattr(ScriptedWorker, 'assign', assign_with_history)
        result = _source_adoption_result(_source_adoption_command('review-restart'))
        actual = supervisor._restart_source_owners('test', 'admin', ('remote',), result)
        generated_at = supervisor.probe_result().generated_at
        assert generated_at is not None
        assert generated_at >= (config.daemon.worker_startup_deadline_secs if expire else 0.5)
        assert len(injections) == 1
        replacement = injections[0]
        assert replacement.batch_acquisitions > 0
        assert all(worker.batch_acquisitions == counts[worker.worker_id] for worker in originals)
        assert all(not worker.killed for worker in originals)
        assert all(breaker._failure_total == 0 for breaker in supervisor._breakers.values())
        assert actual == result
        assert not replacement.killed
        assert _spawn_health(supervisor, replacement.worker_id).worker.heartbeat_at == 0.5
        assert not replacement.messages
        assert arrivals[replacement.spawn_instance] == (
            config.daemon.worker_startup_deadline_secs if expire else 0.5
        )


def _wait_for_supervisor_probe_ready(
    supervisor: Supervisor,
    factory: ScriptedWorkerFactory,
) -> None:
    while True:
        heartbeat_at = supervisor._clock()
        for worker in tuple(factory.instances.values()):
            worker.messages.append(health_update(worker, heartbeat_at=heartbeat_at))
        supervisor._tick()
        if supervisor.probe_result().readiness:
            return


def _run_frozen_supervisor_probe_child(
    config_path: str,
    connection: Connection,
) -> None:
    probe: ProbeServer | None = None
    try:
        config = load_config(Path(config_path))
        config = replace(
            config,
            daemon=replace(
                config.daemon,
                health_interval_secs=0.05,
                supervisor_poll_interval_secs=0.05,
            ),
        )
        factory = ScriptedWorkerFactory()
        clock = FakeClock()
        with Supervisor(
            config=config,
            config_path=Path(config_path),
            worker_factory=factory,
            clock=clock,
        ) as supervisor:
            supervisor._reconcile(generations={})
            supervisor.start()
            _wait_for_supervisor_probe_ready(supervisor, factory)

            probe_module.monotonic = clock
            probe = ProbeServer('127.0.0.1', 0, supervisor.probe_result)
            probe.start()
            progress = vars(supervisor)['_progress']
            stale_after = cast('float', vars(progress)['_supervisor_stale_after'])
            connection.send(('ready', _child_probe_address(probe), stale_after))
            while True:
                if connection.poll(0.005) and connection.recv() == 'freeze':
                    clock.advance(stale_after)
                    connection.send(('frozen', clock()))
                    threading.Event().wait()
                heartbeat_at = clock()
                for worker in tuple(factory.instances.values()):
                    worker.messages.append(health_update(worker, heartbeat_at=heartbeat_at))
                supervisor._tick()
    except (AssertionError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError) as error:
        connection.send(('error', f'{type(error).__name__}: {error}'))
    finally:
        if probe is not None:
            probe.close()
            while not probe.join(timeout=0):
                time.sleep(0.002)
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


def prepare_absent_orphan(config: Config) -> tuple[Path, Path]:
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
    with _SupervisorState.open(config.daemon.state_dir) as state:
        state.reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={('test', 'remote'): 1},
        )
        state.reconcile_groups(
            configured={},
            generations={('test', 'remote'): 1},
        )
    return source_path, group_path


def source_only_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    production_deadlines: bool = False,
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
    if production_deadlines:
        path.write_text(
            path.read_text()
            .replace('health_interval_secs = 0.05', 'health_interval_secs = 2.0')
            .replace(
                'shutdown_deadline_secs = 2.0',
                f'shutdown_deadline_secs = {stall_deadline("shutdown", 10.0)}',
            )
            .replace(
                'worker_startup_deadline_secs = 10.0',
                f'worker_startup_deadline_secs = {stall_deadline("startup", 30.0)}',
            )
        )
    return path, load_config(path)


@pytest.mark.parametrize('worker_mode', ['default', 'pool', 'dedicated'])
def test_automatic_live_replacement_reconciles_completed_readd(
    worker_mode: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    group_config = replace(site.groups['remote'], worker=worker_mode)
    config = replace(
        loaded,
        sites={'test': replace(site, groups={'remote': group_config})},
    )
    _prepare_readd_inventory(config.daemon.state_dir)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
            worker.assignment
            for workers in factory.history.values()
            for worker in workers
            if worker.assignment is not None
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
        assert state.promote_readded_group(
            site='test',
            group='remote',
            source_state=source,
            group_state=group,
        )
        source_spec = build_worker_specs(config.sites['test'])[0]
        owner_spec = next(
            spec for spec in build_worker_specs(config.sites['test']) if 'remote' in spec.groups
        )
        source_worker = factory.instances[source_spec.worker_id]
        source_history_count = len(factory.history[source_spec.worker_id])
        owner_history_count = len(factory.history[owner_spec.worker_id])
        operations_before_exit = len(factory.operations)
        source_worker.exit_status = 1
        for _ in range(10):
            clock.advance(1.0)
            supervisor._tick()
            if (
                len(factory.history[source_spec.worker_id]) > source_history_count
                and len(factory.history[owner_spec.worker_id]) > owner_history_count
                and factory.history[owner_spec.worker_id][-1].assignment is not None
            ):
                break
        else:
            pytest.fail('automatic replacement did not become ready')

        replacement = factory.history[owner_spec.worker_id][-1]
        assert replacement is not factory.history[owner_spec.worker_id][0]
        assert replacement.assignment is not None
        assert replacement.assignment.group_assignments == (
            DeliveryGroupAssignment(
                source='admin',
                group='remote',
                group_generation=2,
                start_mode='resume',
            ),
        )
        source_replacement = factory.history[source_spec.worker_id][-1]
        assert source_replacement.assignment is not None
        assert source_replacement.assignment.source_group_assignments == (
            ActiveSourceGroupAssignment(
                source='admin',
                group='remote',
                group_generation=2,
                start_mode='resume',
            ),
        )
        replacement_operations = factory.operations[operations_before_exit:]
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
            worker.assignment is not None
            for workers in factory.history.values()
            for worker in workers
        )


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_completed_readd_restarts_as_delivery_resume_without_a_second_live_assignment(
    command: Literal['replay', 'reset-to-latest'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    _prepare_readd_inventory(config.daemon.state_dir)
    factory = ScriptedWorkerFactory()
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
            clock=FakeClock(),
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
        worker.assignment
        for workers in factory.history.values()
        for worker in workers
        if worker.assignment is not None
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
    assert len(factory.history[owner_assignment.worker_id]) == 1
    assert (
        sum(
            source_assignment.group == 'remote'
            for worker_assignment in assignments
            for source_assignment in worker_assignment.source_group_assignments
        )
        == 1
    )


def test_absent_orphan_replay_commits_once_and_retry_returns_recorded_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, _group_path = prepare_absent_orphan(config)
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=ScriptedWorkerFactory(),
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
        record = state.group_inventory_record(site='test', group='remote')
        assert record is not None
        assert (record.orphan_generation, record.generation) == (None, 2)
    with _lifecycle_group(source_path, generation=2) as group:
        assert group.snapshot().cursor == 100
        assert group.snapshot().replay_required_from is None
        terminal = group.terminal_disposition_identity(2)
        assert terminal is not None
        assert terminal[2] == 'applied'
        assert sum(item.command == 'replay' for item in audit_records(group)) == 1


def test_adopt_source_stream_completes_on_default_packing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _default_packed_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    command = _source_adoption_command('adopt-default-packed')
    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_spec = build_worker_specs(config.sites['test'])[0]
        source_owner = factory.instances[source_spec.worker_id]
        source_owner.operator_handler = _source_adoption_result

        result = supervisor.adopt_source_stream_command(command, timeout=1.0)

        replacement = factory.instances[source_spec.worker_id]
        assert result.ok, result.message
        assert replacement is not source_owner
        assert replacement.assignment is not None
        assert replacement.assignment.source_generations


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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
    group_path = source_database(config.daemon.state_dir)
    with _conditional_group(group_path) as group:
        if advance_before_result:
            completed = group.callback_done(0, expected_revision=7)
            assert completed.revision == 8

    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        assert owner.assignment is not None
        first_ready = _resolve_lane_recovery(
            config,
            owner.assignment,
            group_path,
            drop_ready=True,
        )
        assert first_ready.outcome == expected_outcome
        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == (evidence,)

        owner.exit_status = 1
        supervisor._tick()
        clock.advance(1.0)
        supervisor._tick()
        supervisor._tick()

        replacement = factory.instances[owner.worker_id]
        assert replacement is not owner
        assert replacement.assignment is not None
        replayed_ready = _resolve_lane_recovery(
            config,
            replacement.assignment,
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

        replacement.messages.append(replayed_ready)
        supervisor._tick()

        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == ()


@pytest.mark.parametrize('delay_before_observation', [False, True])
def test_pending_provider_publication_survives_consumer_scheduling_delay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, delay_before_observation: bool
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory.instances.values() if item.spec.groups)
        update = health_update(worker, heartbeat_at=clock())
        published_at = clock()
        accept = supervisor._accept_worker_health

        def delayed_accept(current: ScriptedWorker, message: HealthUpdate) -> bool:
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
    stall_detectors: StallDetectors, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory.instances.values() if item.spec.sources)
        clock.advance(1)
        update = health_update(worker, heartbeat_at=clock())
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
        assert worker.killed
        assert supervisor._spawns[worker.worker_id].termination_reason == failure


def test_channel_recovery_cannot_borrow_another_spawns_lifecycle(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
    _create_recovery_group_database(config)
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=FakeClock(), worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={('test', 'remote'): 1})
        supervisor.start()
        current = next(worker for worker in factory.instances.values() if not worker.spec.sources)
        old = ScriptedWorker(current.spec, 'old-spawn', [], None, authentication_ready=True)
        assert supervisor._spawns[current.worker_id].worker is current
        assert supervisor._spawns[current.worker_id].lifecycle is None
        with pytest.raises(ValueError, match='authenticated current authority'):
            supervisor._accept_worker_monitor_message(old, _recovery_ready(current, evidence))
        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == (evidence,)
        assert not current.killed


def test_checkpoint_error_is_not_replaced_by_shutdown_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    clock = FakeClock()
    factory = ScriptedWorkerFactory()

    def checkpointer_factory(
        connection: sqlite3.Connection,
        path: Path,
        **options: Unpack[_CheckpointOptions],
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            **options,
            run_checkpoint=lambda: (
                (_ for _ in ()).throw(DatabaseError('checkpoint corruption'))
                if options['clock']() >= 5 * options['interval_secs']
                else (0, 0, 0)
            ),
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        dependent.shutdown_status = None
        for _ in range(4):
            clock.advance(config.daemon.checkpoint_interval_secs)
            assert supervisor._tick()

        clock.advance(config.daemon.checkpoint_interval_secs)
        with pytest.raises(DatabaseError, match='checkpoint corruption') as caught:
            supervisor._tick()

        checkpoint_error = str(caught.value)
        assert caught.value.__notes__ == [f'worker child remained unreaped: {dependent.worker_id}']
        assert supervisor._halt_reason_value == checkpoint_error
    finally:
        supervisor._close()


def test_child_process_stale_supervisor_changes_livez_to_503(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, _config = source_only_config(tmp_path, monkeypatch)
    context = multiprocessing.get_context('spawn')
    parent, child = context.Pipe()
    process = context.Process(
        target=_run_frozen_supervisor_probe_child,
        args=(os.fspath(config_path), child),
    )
    process.start()
    child.close()
    try:
        assert parent.poll(None), 'supervisor child did not report probe startup'
        message = parent.recv()
        assert message[0] == 'ready', message
        address = cast('tuple[str, int]', message[1])
        stale_after = cast('float', message[2])
        assert stale_after == 10.0
        assert _child_probe_response(address).startswith(b'HTTP/1.1 200 OK\r\n')

        parent.send('freeze')
        assert parent.poll(None), 'supervisor child did not acknowledge the foreground freeze'
        frozen = parent.recv()
        assert frozen == ('frozen', stale_after)
        response = _child_probe_response(address)

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


def test_child_without_a_hello_is_contained_after_a_pass_reads_nothing(
    stall_detectors: StallDetectors, tmp_path: Path
) -> None:
    config = _lifecycle_probe_config(tmp_path)
    (tmp_path / 'hold-hello').touch()
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        worker = _first_lifecycle_worker(probe)
        _wait_lifecycle_file(tmp_path / f'{worker.spawn_instance}.hello-entered', worker=worker)
        lifecycle = probe.supervisor._spawns[worker.worker_id].lifecycle
        assert lifecycle is not None
        with stall_detectors.enabled('startup'):
            lifecycle.deadline_at = probe.supervisor._clock()
            probe.supervisor._advance_worker_messages()
            assert [item.reason for item in probe.terminations] == ['worker-startup-timeout']
            assert worker.last_arrival_at is None


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_readd_source_commit_reconciles_inventory_idempotently(
    command: Literal['replay', 'reset-to-latest'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
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
        _SupervisorState.open(state_dir) as state,
    ):
        source.initialize(100, incarnation='incarnation-1')
        if command == 'replay':
            open_source_range(source, 101, 101, successful_clock=1.0)
        Supervisor._orphan_group(source, group)
        assert not state.promote_readded_group(
            site='test',
            group='remote',
            source_state=source,
            group_state=group,
        )
        _begin_readd_disposition(source, group, command)
        assert state.promote_readded_group(
            site='test',
            group='remote',
            source_state=source,
            group_state=group,
        )
        assert state.reconcile_groups(
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
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    group = replace(site.groups['remote'], worker=worker_mode)
    config = replace(
        loaded,
        sites={'test': replace(site, groups={'remote': group})},
    )
    with _SupervisorState.open(config.daemon.state_dir) as state:
        state.reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={},
        )
        state.reconcile_groups(
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
    source_path, group_path = prepare_absent_orphan(config)
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=ScriptedWorkerFactory(),
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        with pytest.raises(TimeoutError, match=f'injected {failure_stage} failure'):
            supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        state = cast('_SupervisorState', vars(supervisor)['_state'])
        record = state.group_inventory_record(site='test', group='remote')
        assert record is not None
        assert record.generation == 2
        assert record.orphan_generation == (None if failure_stage == 'inventory-completed' else 1)
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
            replace(record, source='different-source'),
            replace(record, generation=99, orphan_generation=98),
        ):
            original_record = state.group_inventory_record
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
                conflict.setattr(state, 'group_inventory_record', read_record)
                with pytest.raises(RuntimeError, match='inventory conflicts'):
                    supervisor.dispatch_group_disposition_command(command, timeout=1.0)
            assert state.group_inventory_record == original_record
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
        completed = state.group_inventory_record(site='test', group='remote')
        assert completed is not None
        assert completed.generation == 2
        assert completed.orphan_generation is None
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


@pytest.mark.parametrize('current_spawn', [True, False])
def test_supervisor_accepts_ready_only_from_authenticated_current_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    current_spawn: bool,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
    _create_recovery_group_database(config)
    factory = ScriptedWorkerFactory()
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'identity-{token_index}'

    with Supervisor(
        clock=FakeClock(),
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        dependent.messages.append(
            _recovery_ready(
                dependent,
                evidence,
                spawn_instance=(dependent.spawn_instance if current_spawn else 'stale-spawn'),
            )
        )

        supervisor._tick()

        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == (() if current_spawn else (evidence,))
        assert dependent.killed is not current_spawn


def test_command_results_arrive_in_order_under_backpressure(tmp_path: Path) -> None:
    config = _control_probe_config(tmp_path, ())
    with running_supervisor(config, ChildPatches(control_probe=True)) as probe:
        worker = _ready_control_probe(probe)[0]
        command = OperatorCommand(
            command='restart-event',
            request_id='first',
            arguments={'site': 'test', 'source': 'admin'},
            identity=AuditIdentity(local_uid=os.getuid(), username='operator', pid=os.getpid()),
        )
        (tmp_path / 'operator-hold').touch()
        worker.send(command)
        started = _wait_control_event(tmp_path, worker, 'command-started')
        (tmp_path / f'{worker.spawn_instance}.large').touch()
        _wait_control_event(tmp_path, worker, 'frame-started')
        worker.send(replace(command, request_id='second'))
        while not any(
            event.get('request_id') == 'second' for event in _control_events(tmp_path, worker)
        ):
            assert worker._process.poll() is None, probe.failure_details()
            time.sleep(0.002)
        (tmp_path / 'operator-release').touch()
        _wait_control_event(tmp_path, worker, 'command-completed')
        _wait_control_event(tmp_path, worker, 'result-enqueued', request_id='second')
        _wait_control_event(tmp_path, worker, 'result-enqueued', request_id='first')
        results: list[CommandResult] = []
        while len(results) < 2:
            message = receive_control(worker._control)
            if isinstance(message, CommandResult):
                results.append(message)
        worker.send(replace(command, request_id='third'))
        while len(results) < 3:
            message = receive_control(worker._control)
            if isinstance(message, CommandResult):
                results.append(message)
        assert [item.request_id for item in results] == ['second', started['request_id'], 'third']
        assert not results[0].ok
        assert 'busy' in results[0].message
        assert results[1].ok
        assert results[2].ok
        assert worker._process.poll() is None
        assert not probe.terminations


def test_committed_pending_completion_cannot_resume_a_newer_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._reconcile(generations={})
        state = supervisor._state
        assert state is not None
        reservation = state.reserve_group_disposition(
            site='test', group='remote', observed_generation=1
        )
        assert supervisor._reconciled is not None
        supervisor._reconciled[('test', 'remote')] = reservation
        command = OperatorCommand(
            request_id='first',
            command='reset-group-to-latest',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'first'},
            identity=AuditIdentity(local_uid=1, username='operator', pid=1),
        )
        result = CommandResult(
            request_id='first',
            command=command.command,
            ok=True,
            message='completed',
            result={'group_generation': reservation.reserved_generation},
        )
        with _outside_supervisor_writer(state):
            assert (
                supervisor._complete_disposition_inventory('test', reservation, result) == result
            )
        # An operator lands the pending completion and then reserves a newer
        # disposition in the same transaction, before any periodic tick.
        newer = state.reserve_group_disposition(
            site='test', group='remote', observed_generation=reservation.reserved_generation
        )
        supervisor._reconciled[('test', 'remote')] = newer
        supervisor._resume_stored_dispositions()
        assert supervisor._reconciled[('test', 'remote')] == newer


def test_completed_adoption_keeps_unpark_fenced_by_durable_group_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        group_owner = next(worker for worker in factory.instances.values() if worker.spec.groups)

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

        source_owner.operator_handler = _adopt_source
        group_owner.operator_handler = lambda message: CommandResult(
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
        for history in factory.history.values()
        for worker in history
        for sent in worker.sent_commands
    )


@pytest.mark.parametrize(('synchronous', 'expected'), [('normal', 1), ('full', 2)])
def test_supervisor_database_opens_use_configured_synchronous_policy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    synchronous: Literal['normal', 'full'],
    expected: int,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    site = config.sites['test']
    source = site.sources['admin']
    strict_holdback = source.late_event_window_secs + 10.0
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
    prepare_absent_orphan(config)
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
            assert group._settings.strict_holdback_secs == (strict_holdback)


def test_containment_kills_overdue_worker_when_park_evidence_read_fails(
    stall_detectors: StallDetectors, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = _two_group_shared_worker_config(tmp_path, monkeypatch)
    clock = FakeClock()
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        wall_clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        remote = next(w for w in factory.instances.values() if not w.spec.sources)
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=0))
        supervisor._tick()
        clock.advance(10.1)
        confirmed = replace(_in_flight_group_health(remote, heartbeat_at=0), deadline_at=9.9)
        for worker in factory.instances.values():
            worker.messages.append(
                health_update(
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
        assert remote.killed
        assert supervisor._park_evidence_by_group == {}


@pytest.mark.parametrize('owner', ['steady', 'startup', 'ready', 'grant'])
@pytest.mark.parametrize('fresh', [False, True])
def test_supervisor_exhausts_owner_health_batch_before_temporal_verdict(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    owner: str,
    fresh: bool,
) -> None:
    stall_detectors.enable('heartbeat')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for _ in range(3):
            supervisor._tick()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(worker for worker in factory.instances.values() if worker.spec.groups)
        assert source.assignment is not None
        assert dependent.assignment is not None
        if owner in {'startup', 'ready'}:
            supervisor._spawns[source.worker_id].lifecycle = supervisor_module._WorkerLifecycle(
                step_started_at=0.0,
                assignment=source.assignment,
                deadline_at=10,
                worker=source,
                assigned=True,
                authenticated=True,
                ready=owner == 'ready',
            )
        elif owner == 'grant':
            source.auto_grant_ready = False
            supervisor._spawns[dependent.worker_id].lifecycle = supervisor_module._WorkerLifecycle(
                step_started_at=0.0,
                assignment=dependent.assignment,
                deadline_at=10,
                worker=dependent,
                authenticated=True,
            )
        source.messages.append(health_update(source, heartbeat_at=0))
        clock.advance(0.5)
        latest = health_update(source, heartbeat_at=clock())
        if fresh:
            source.messages.append(latest)
        supervisor._tick()
        temporal_grace = owner in {'startup', 'ready'}
        assert source.killed is (not fresh and not temporal_grace)
        if fresh:
            assert not source.messages
            assert (
                _spawn_health(supervisor, source.worker_id).worker.heartbeat_at
                == latest.worker.heartbeat_at
            )
            assert supervisor._breakers[source.worker_id]._failure_total == 0
        else:
            assert supervisor._breakers[source.worker_id]._failure_total == (
                0 if temporal_grace else 1
            )


@pytest.mark.parametrize('tail', ['fresh', 'stale', 'fatal'])
def test_operator_health_batch_checks_tail_after_correlated_result(
    stall_detectors: StallDetectors, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, tail: str
) -> None:
    stall_detectors.enable('heartbeat')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for _ in range(3):
            supervisor._tick()
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        owner.messages.append(health_update(owner, heartbeat_at=0))
        clock.advance(0.5)
        latest = health_update(owner, heartbeat_at=clock() if tail != 'stale' else 0)
        if tail == 'fatal':
            latest = replace(latest, worker=replace(latest.worker, monitor_alive=False))
        owner.operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'revision': 3},
        )
        send = owner.send

        def send_with_tail(message: ControlMessage) -> None:
            send(message)
            owner.messages.append(latest)

        monkeypatch.setattr(owner, 'send', send_with_tail)
        command = OperatorCommand(
            request_id='batch-command',
            command='park',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=456),
        )
        result = supervisor.dispatch_operator_command(command, timeout=1)
        assert result.request_id == command.request_id
        assert result.result == {'revision': 3}
        if tail == 'fresh':
            assert not owner.killed
            assert not owner.messages
            assert _spawn_health(supervisor, owner.worker_id).worker.heartbeat_at == clock()
        else:
            assert owner.killed


def test_dependent_assigned_before_its_source_initializes_waits(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path, remote=True)
    hold = tmp_path / 'hold-initial-query'
    hold.touch()
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        probe.wait_for((tmp_path / 'initial-query-entered').exists)
        probe.wait_for((tmp_path / 'remote-bootstrap-pending').exists)
        dependent = next(
            spawn for spawn in probe.supervisor._spawns.values() if not spawn.worker.spec.sources
        )
        worker = cast('_WorkerInstance', dependent.worker)
        assert worker._assigned_worker is not None
        probe.wait_for(lambda: dependent.health_inventory_complete)
        assert not any(
            source.initialized
            for health in probe.latest_health.values()
            for source in health.sources
        )
        assert probe.supervisor._startup_clock(dependent) is None
        _assert_no_lifecycle_charge(probe)
        assert not probe.supervisor.probe_mapping()['readiness']
        hold.unlink()
        probe.wait_for(lambda: bool(probe.supervisor.probe_mapping()['readiness']))
        assert probe.supervisor._spawns[dependent.worker.worker_id] is dependent
        _assert_no_lifecycle_charge(probe)


def test_dependent_grant_discards_late_operator_result_before_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        dependent.wait_results.append(None)
        grant = source.sent_grants[-1]
        ready = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=grant.dependent_spawn_instance,
        )
        source.messages.extend(
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

        assert supervisor._spawns[dependent.worker_id].termination_reason is None
        assert not dependent.killed
        assert tuple(source.messages) == ()
        assert not dependent.killed
        assert dependent.assignment is not None
        assert supervisor._spawns[dependent.worker_id].lifecycle is None


def test_direct_disposition_preserves_control_response_field_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    prepare_absent_orphan(config)
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


@pytest.mark.parametrize('failure', ['dead-monitor', 'sampling', 'inventory', 'future-watchdog'])
@pytest.mark.parametrize('owner', ['steady', 'startup', 'ready', 'grant', 'operator', 'direct'])
def test_supervisor_health_batch_cannot_erase_explicit_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str, owner: str
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=FakeClock(), worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory.instances.values() if item.spec.sources)
        assert worker.assignment is not None
        if owner in {'startup', 'ready'}:
            supervisor._spawns[worker.worker_id].lifecycle = supervisor_module._WorkerLifecycle(
                step_started_at=0.0,
                assignment=worker.assignment,
                deadline_at=10,
                worker=worker,
                authenticated=True,
                assigned=True,
                ready=owner == 'ready',
            )
        elif owner == 'grant':
            dependent = next(item for item in factory.instances.values() if not item.spec.sources)
            assert dependent.assignment is not None
            worker.auto_grant_ready = False
            supervisor._spawns[dependent.worker_id].lifecycle = supervisor_module._WorkerLifecycle(
                step_started_at=0.0,
                assignment=dependent.assignment,
                deadline_at=10,
                worker=dependent,
                authenticated=True,
            )
        healthy = health_update(worker, heartbeat_at=0)
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
        worker.messages.extend((bad, healthy))
        probe = Mock(side_effect=AssertionError('explicit failure probed'))
        monkeypatch.setattr(worker, 'has_pending_input', probe)
        if owner == 'direct':
            message = worker.receive(timeout=0)
            assert isinstance(message, HealthUpdate)
            assert supervisor._accept_worker_health(worker, message)
        elif owner == 'operator':
            assert (
                supervisor._accept_operator_batch(
                    worker,
                    _source_adoption_command('fatal-batch'),
                    worker.receive_available(),
                )
                is None
            )
        else:
            supervisor._advance_worker_messages()
        assert worker.killed
        probe.assert_not_called()
        assert tuple(worker.messages) == (healthy,)
        assert (
            supervisor._spawns[worker.worker_id].termination_reason
            == {
                'dead-monitor': 'worker-heartbeat-overdue',
                'sampling': 'watchdog-sampling-failed',
                'inventory': 'source-phase-overdue',
                'future-watchdog': 'source-phase-overdue',
            }[failure]
        )


def test_discard_worker_lifecycle_clears_attributed_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    with Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(worker for worker in factory.instances.values() if worker.spec.groups)
        supervisor._spawns[worker.worker_id].attributed_park_failure = True
        supervisor._replace_worker(worker.worker_id)
        replacement = factory.instances[worker.worker_id]
        replacement.exit_status = 1
        supervisor._tick()
        breaker = supervisor._breakers[worker.worker_id]
        assert breaker._failure_total == 1
        assert breaker.restart_deadline_at is not None


def test_dispatch_interrupted_result_has_unknown_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _dispatch_wait_harness(tmp_path, monkeypatch) as (supervisor, factory, _clock, command):
        owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        owner.operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=False,
            message='operator command interrupted by worker shutdown',
            request_id=sent.request_id,
            result={ipc_module.OUTCOME_KEY: ipc_module.OUTCOME_UNKNOWN},
        )
        with pytest.raises(OperatorOutcomeUnknownError) as caught:
            supervisor.dispatch_operator_command(command, timeout=600)
        assert str(caught.value) == (
            'restart-event did not finish: the worker stopped while running it. '
            'It may still complete; run status to see the current state.'
        )
        assert not isinstance(caught.value, (ValueError, TimeoutError))
        assert supervisor._dispatch_reserved is None
        assert not owner.killed


@pytest.mark.parametrize('source_mode', ['live', 'stopped'])
def test_supervisor_routes_absent_orphan_to_ready_source_or_direct(
    source_mode: Literal['live', 'stopped'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, group_path = prepare_absent_orphan(config)
    factory = ScriptedWorkerFactory()
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
        clock=FakeClock(),
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
                worker for worker in factory.instances.values() if 'admin' in worker.spec.sources
            )

            def execute_source_command(message: OperatorCommand) -> CommandResult:
                internal_commands.append(message.command)
                assert message.command == 'group-disposition'
                with _lifecycle_source(source_path):
                    return _execute_source_group_command(group_path, message)

            source_owner.operator_handler = execute_source_command

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


def test_dispatch_wait_ends_when_owner_is_reaped_by_tick(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _dispatch_wait_harness(tmp_path, monkeypatch) as (supervisor, factory, clock, command):
        owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        reads: list[float] = []

        def receive(*, timeout: float) -> ControlMessage:
            reads.append(timeout)
            assert not owner.close_calls
            clock.advance(timeout)
            owner.exit_status = 1
            raise TimeoutError('empty slice')

        monkeypatch.setattr(owner, 'receive', receive)
        with pytest.raises(OperatorOutcomeUnknownError, match='worker failed while running it'):
            supervisor.dispatch_operator_command(command, timeout=600)
        assert reads == [0.05]
        assert owner.close_calls == 1
        assert owner.worker_id in supervisor._restart_deadlines
        assert supervisor._dispatch_reserved is None


@pytest.mark.parametrize('probe', ['liveness', 'readiness'])
def test_maximum_status_probe_returns_valid_control_frame(
    probe: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _unbounded_control_transport(monkeypatch)
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
        assert server._listener is not None
        select.select((server._listener,), (), ())
        server.serve_once(0)
        client.join()
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


def test_dispatch_wait_keeps_supervising_other_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _dispatch_wait_harness(tmp_path, monkeypatch) as (supervisor, factory, clock, command):
        owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        other = next(
            worker for worker in factory.instances.values() if 'remote' in worker.spec.groups
        )
        failed = next(
            worker for worker in factory.instances.values() if 'second' in worker.spec.groups
        )
        original_grants = tuple(owner.sent_grants)
        before = supervisor.probe_result().generated_at
        original_receive = owner.receive

        def receive(*, timeout: float) -> ControlMessage:
            assert 0 < timeout <= 0.05
            assert not owner.close_calls
            clock.advance(timeout)
            other.messages.append(health_update(other, heartbeat_at=clock()))
            failed.exit_status = 1
            if clock() < 2:
                return health_update(owner, heartbeat_at=clock())
            assert _spawn_health(supervisor, other.worker_id).worker.heartbeat_at > 1.9
            assert failed.close_calls == 1
            replacement = factory.instances[failed.worker_id]
            assert replacement is not failed
            assert not replacement.killed
            assert supervisor._spawns[replacement.worker_id].lifecycle is not None
            assert tuple(owner.sent_grants) == original_grants
            generated_at = supervisor.probe_result().generated_at
            assert generated_at is not None
            assert before is not None
            assert generated_at > before
            sent = owner.sent_commands[-1]
            return CommandResult(
                command=sent.command,
                ok=True,
                message='done',
                request_id=sent.request_id,
                result={},
            )

        monkeypatch.setattr(owner, 'receive', receive)
        result = supervisor.dispatch_operator_command(command, timeout=5)
        assert result.ok
        assert result.request_id == command.request_id
        monkeypatch.setattr(owner, 'receive', original_receive)
        owner.messages.append(health_update(owner, heartbeat_at=clock()))
        supervisor._tick()
        assert len(owner.sent_grants) == len(original_grants) + 1
        assert factory.instances[failed.worker_id].assignment is not None


def test_failed_lane_disposition_keeps_spawn_health_inventory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        source = next(w for w in factory.instances.values() if w.spec.sources)
        lane = next(w for w in factory.instances.values() if w.spec.groups)
        for worker in (source, lane):
            assert not supervisor._accept_worker_health(
                worker, health_update(worker, heartbeat_at=clock())
            )
        results = iter(({'disposition_required': True}, {'code': 'source-database-busy'}))
        lane.operator_handler = lambda command: CommandResult(
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
            update = health_update(worker, heartbeat_at=clock())
            assert not supervisor._accept_worker_health(worker, update)
        assert not supervisor._monitor_worker_health(clock())
        assert all(spawn.termination_reason is None for spawn in supervisor._spawns.values())
        assert all(b._failure_total == 0 for b in supervisor._breakers.values())
        assert supervisor._pending_worker_groups(source) == set()
        supervisor._shutdown_workers('replace after retained reservation')
        supervisor.start()
        replacement = factory.instances[source.worker_id]
        assert replacement.spawn_instance != source.spawn_instance
        assert supervisor._pending_worker_groups(source) == set()
        assert supervisor._pending_worker_groups(replacement) == {'remote'}
        assert all(not w.spec.groups for w in factory.instances.values())
        group_health = replace(
            _in_flight_group_health(
                replacement, group='remote', heartbeat_at=clock(), phase='idle'
            ),
            parked=True,
            conditions=('parked',),
        )
        update = health_update(replacement, heartbeat_at=clock())
        assert not supervisor._accept_worker_health(
            replacement, replace(update, groups=(group_health,))
        )
        assert all(spawn.termination_reason is None for spawn in supervisor._spawns.values())

        replacement.operator_handler = lambda message: _execute_source_group_command(path, message)
        result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert result.ok
        current_source = factory.instances[source.worker_id]
        assert current_source is not replacement
        assert supervisor._pending_worker_groups(current_source) == set()
        assert all(b._failure_total == 0 for b in supervisor._breakers.values())


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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        source.authentication_ready = True
        supervisor._tick()
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        dependent.authentication_ready = True
        source.wait_results.append(None)
        failure_calls = _install_source_grant_control_failure(
            monkeypatch,
            source,
            grant_operation=grant_operation,
            error_factory=error_factory,
        )

        assert supervisor._tick()

        assert source.killed
        assert not dependent.killed
        assert supervisor._spawns[source.worker_id].termination_reason == expected_reason
        assert supervisor._spawns[dependent.worker_id].termination_reason is None
        assert len(failure_calls) == 1

        assert supervisor._tick()

        restart_deadlines = vars(supervisor)['_restart_deadlines']
        assert len(failure_calls) == 1
        assert source.worker_id not in supervisor._spawns
        assert restart_deadlines[source.worker_id] == 1.0

        factory.authentication_ready = True
        clock.advance(1.0)
        assert supervisor._tick()
        assert factory.instances[source.worker_id] is not source
    finally:
        supervisor._close()


def test_failed_spawn_cleanup_slices_forced_reap_under_one_deadline(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stall_detectors.enable('shutdown')
    clock = FakeClock()
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
        shutdown_deadline_secs=10.0,
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


@pytest.mark.parametrize('phase', ['internal', 'write-lock-wait', 'write-hold'])
def test_supervisor_internal_lane_overdue_never_creates_park_evidence(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    phase: Literal['internal', 'write-lock-wait', 'write-hold'],
) -> None:
    stall_detectors.enable('phase', 'coordination')
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        group = _in_flight_group_health(dependent, heartbeat_at=0.0, phase=phase)
        lane_watchdog = WatchdogHealth(
            kind='lane',
            site='test',
            source='admin',
            group='remote',
            worker_instance=dependent.spawn_instance,
            phase=phase,
            heartbeat_at=0.0,
            deadline_at=10_000.0,
        )
        operator = WatchdogHealth(
            kind='operator',
            site='test',
            source=None,
            group=None,
            worker_instance=dependent.spawn_instance,
            phase='idle',
            heartbeat_at=0.0,
            deadline_at=None,
        )
        dependent.messages.append(
            health_update(
                dependent,
                heartbeat_at=0.0,
                groups=(group,),
                watchdogs=(lane_watchdog, operator),
            )
        )
        source.messages.append(health_update(source, heartbeat_at=0.0))
        supervisor._tick()
        credential = config.sites['test'].credentials['admin']
        clock.advance(
            internal_stall_deadline_secs(
                config.daemon.health_interval_secs,
                credential,
            )
        )
        dependent.messages.append(
            replace(
                health_update(
                    dependent,
                    heartbeat_at=0.0,
                    groups=(group,),
                    watchdogs=(lane_watchdog, operator),
                ),
                worker=replace(
                    health_update(dependent, heartbeat_at=clock()).worker,
                    heartbeat_at=clock(),
                ),
            )
        )
        source.messages.append(health_update(source, heartbeat_at=clock()))
        monkeypatch.setattr(dependent, 'wait', _never_reap)

        supervisor._tick()

        assert dependent.killed
        assert supervisor._spawns[dependent.worker_id].termination_reason == ('lane-phase-overdue')
        assert supervisor._park_evidence_by_group == {}
    finally:
        supervisor._close()


@pytest.mark.parametrize('targeted', [False, True], ids=['idle-owner', 'active-owner'])
def test_supervisor_operator_shutdown_inventory_accepts_current_target(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    targeted: bool,
) -> None:
    stall_detectors.enable('phase', 'coordination')
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(
            item for item in factory.instances.values() if bool(item.spec.groups) == targeted
        )
        update = health_update(worker, heartbeat_at=0.0)
        operator = WatchdogHealth(
            kind='operator',
            site='test',
            source='admin' if targeted else None,
            group='remote' if targeted else None,
            worker_instance=worker.spawn_instance,
            phase='shutdown',
            heartbeat_at=0.0,
            deadline_at=config.daemon.shutdown_deadline_secs,
            operator_command='park' if targeted else None,
        )
        worker.messages.append(
            replace(
                update,
                watchdogs=(*update.watchdogs[:-1], operator),
            )
        )

        supervisor._tick()

        assert not worker.killed
        published = next(
            item
            for item in _spawn_health(supervisor, worker.worker_id).watchdogs
            if item.kind == 'operator'
        )
        assert published.phase == 'shutdown'
        assert (published.source, published.group) == (
            ('admin', 'remote') if targeted else (None, None)
        )
    finally:
        supervisor._close()


def test_frame_dropping_a_published_group_record_is_contained(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path)
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        probe.wait_for(lambda: bool(probe.supervisor.probe_mapping()['readiness']))
        assert sum(len(health.groups) for health in probe.latest_health.values()) == 1
        (tmp_path / 'omit-group-health').touch()
        probe.wait_for(lambda: bool(probe.terminations))
        assert [item.reason for item in probe.terminations] == ['lane-phase-overdue']
        probe.wait_for(
            lambda: any(breaker._failure_times for breaker in probe.breakers.values()),
            expected_exits=(probe.terminations[0].spawn_instance,),
        )
        assert any(breaker._failure_times for breaker in probe.breakers.values())


def test_gap_between_hello_and_assignment_restarts_the_clock_at_assignment(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path)
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        worker = _first_lifecycle_worker(probe)
        lifecycle = probe.supervisor._spawns[worker.worker_id].lifecycle
        assert lifecycle is not None
        assert _authenticate_worker(worker)
        lifecycle.authenticated = True
        lifecycle.deadline_at = probe.supervisor._clock() - 1
        assigned_at = probe.supervisor._clock()
        original_clock = probe.supervisor._clock
        probe.supervisor._clock = lambda: assigned_at
        try:
            probe.supervisor._tick()
        finally:
            probe.supervisor._clock = original_clock
        _assert_no_lifecycle_charge(probe)
        assert lifecycle.deadline_at == (
            assigned_at + probe.supervisor._config.daemon.worker_startup_deadline_secs
        )
        _ready_control_probe(probe)


def test_grant_in_flight_through_a_long_source_page_charges_nothing(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path, remote=True)
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        _source, dependent = _grant_during_source_page(probe, tmp_path)
        before = len(probe.terminations)
        lifecycle = probe.supervisor._spawns[dependent.worker_id].lifecycle
        assert lifecycle is not None
        assert probe.supervisor._lifecycle_unclocked(lifecycle)
        assert (
            probe.supervisor._startup_clock(probe.supervisor._spawns[dependent.worker_id]) is None
        )
        probe.supervisor._tick()
        assert len(probe.terminations) == before, probe.failure_details()
        assert not probe.breakers[dependent.worker_id]._failure_times
        (tmp_path / 'hold-page').unlink()
        _ready_control_probe(probe)
        assert probe.supervisor._spawns[dependent.worker_id].worker is dependent


@pytest.mark.parametrize('worker_mode', ['pool', 'dedicated'])
def test_supervisor_quiesces_groups_before_source_stream_adoption(
    worker_mode: Literal['pool', 'dedicated'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    if worker_mode == 'dedicated':
        config_path.write_text(
            _replace_config_text(
                config_path.read_text(encoding='utf-8'), 'worker = "pool"', 'worker = "dedicated"'
            ),
            encoding='utf-8',
        )
        config = load_config(config_path)
    factory = ScriptedWorkerFactory()
    command = _source_adoption_command(f'adopt-{worker_mode}')
    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        group_owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        source_assignment = source_owner.assignment
        group_assignment = group_owner.assignment
        assert source_assignment is not None
        assert group_assignment is not None
        source_owner.operator_handler = _source_adoption_result
        group_owner.operator_handler = _source_adoption_result
        operations_before_adoption = len(factory.operations)

        result = supervisor.adopt_source_stream_command(command, timeout=1.0)

        source_replacement = factory.instances[source_owner.worker_id]
        group_replacement = factory.instances[group_owner.worker_id]
        replacement_source_assignment = source_replacement.assignment
        replacement_group_assignment = group_replacement.assignment

    assert source_replacement is not source_owner
    assert group_replacement is not group_owner
    assert replacement_source_assignment is not None
    assert replacement_group_assignment is not None
    replacement_operations = factory.operations[operations_before_adoption:]
    assert replacement_operations.index('spawn:site') < replacement_operations.index(
        f'spawn:{group_owner.spec.kind.value}'
    )
    replacement_generation = dict(replacement_source_assignment.source_generations)['admin']
    assert replacement_generation != dict(source_assignment.source_generations)['admin']
    assert replacement_group_assignment.source_endpoints == ()
    assert replacement_group_assignment.event_grants == ()
    assert replacement_group_assignment.group_assignments == ()
    assert group_replacement.spec.groups == ()
    assert source_replacement.sent_grants == []
    assert isinstance(
        replacement_source_assignment.source_group_assignments[0],
        PendingDispositionSourceGroupAssignment,
    )

    assert group_owner.sent_commands[0].command == 'park'
    assert group_owner.sent_commands[0].arguments['group'] == 'remote'
    assert [item.command for item in source_owner.sent_commands] == [
        'preflight-source-adoption',
        'adopt-source-stream',
    ]
    assert source_owner.sent_commands[1].arguments['groups_parked'] is True
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    ensure_secure_state_dir(config.daemon.state_dir)
    source_path = config.daemon.state_dir / 'sites' / 'test' / 'sources' / 'admin.db'
    group_path = source_database(config.daemon.state_dir)
    with _lifecycle_source(source_path) as source:
        source.initialize(100, incarnation='incarnation-1')
    with _lifecycle_group(group_path) as group:
        if condition == 'active-event':
            commit_step(
                group,
                StartEventStep(
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
                    None,
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
    factory = ScriptedWorkerFactory()
    with Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    ) as supervisor:
        supervisor.reconcile_durable_state()
        supervisor.start()
        owner = next(
            worker for worker in factory.instances.values() if 'remote' in worker.spec.groups
        )
        component = object.__new__(worker_module._LaneComponent)
        component._group = 'remote'
        component._spec = owner.spec
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

            owner.operator_handler = execute
            arguments: dict[str, object] = {
                'site': 'test',
                'group': 'remote',
                'reason': 'recover group',
            }
            if replay_from is not None:
                arguments['from_event_id'] = replay_from
                arguments['acknowledge_truncation'] = False
            command = OperatorCommand(
                request_id='invalid-disposition',
                command=command_name,
                arguments=arguments,
                identity=AuditIdentity.current(),
            )
            if replay_from == 0:
                with pytest.raises(
                    ValueError, match='replay position must be a positive event ID'
                ):
                    supervisor.dispatch_group_disposition_command(command, timeout=1.0)
            else:
                result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
                assert not result.ok
            assert group.snapshot() == before
        assert supervisor._state is not None
        inventory = supervisor._state.inventory_records()[('test', 'remote')]
        assert inventory.lifecycle == 'active'
        assert inventory.generation == 1

    with Supervisor(config=config, config_path=config_path) as restarted:
        restarted.reconcile_durable_state()
        assignment = restarted._build_worker_assignment(owner.spec, 'replacement', (), (), ())
        assert assignment.group_assignments == (
            DeliveryGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='resume',
            ),
        )


def test_group_omission_and_readd_cannot_route_old_generation_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._reconcile(
            generations={},
        )
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(_park_evidence(group_revision=0))

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

    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.pending_park_evidence() == ()
        assert _parked_group_rows(state) == [('test', 'remote', 2)]


def test_healthy_breaker_decay_waits_for_empty_pending_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, _config = source_only_config(tmp_path, monkeypatch)
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
    clock = FakeClock()
    dependent_spec = next(
        spec for spec in build_worker_specs(config.sites['test']) if not spec.sources
    )
    policy = config.sites['test'].workers['pool']
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
        breaker = state.restore_worker_breaker(
            worker_id=dependent_spec.worker_id,
            restart_limit=policy.restart_limit,
            restart_window_secs=policy.restart_window_secs,
            healthy_reset_secs=policy.healthy_reset_secs,
            clock=clock,
        )
        assert breaker.record_failure('previous-unattributed-failure') == 1.0
        assert breaker.mark_restart_started()
        state.persist_worker_breaker(breaker)
    _create_recovery_group_database(config)
    factory = ScriptedWorkerFactory()
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        clock.advance(0.2)
        dependent.messages.append(health_update(dependent, heartbeat_at=clock()))

        supervisor._tick()

        current_breaker = cast(
            'dict[str, _WorkerBreaker]',
            vars(supervisor)['_breakers'],
        )[dependent.worker_id]
        assert len(current_breaker._failure_times) == 1
        dependent.messages.append(_recovery_ready(dependent, evidence))

        supervisor._tick()

        assert len(current_breaker._failure_times) == 0


def test_healthy_reset_waits_for_every_group_record(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path)
    hold = tmp_path / 'hold-page'
    hold.touch()
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        probe.wait_for((tmp_path / 'page-entered').exists)
        probe.wait_for(lambda: any(health.sources for health in probe.latest_health.values()))
        breaker = next(iter(probe.breakers.values()))
        clock = FakeClock()
        breaker._clock = clock
        breaker.record_failure('prior failure')
        clock.advance(breaker._healthy_reset_secs)
        previous = next(iter(probe.latest_health.values())).worker.heartbeat_at
        probe.wait_for(
            lambda: any(
                health.worker.heartbeat_at > previous for health in probe.latest_health.values()
            )
        )
        assert not any(health.groups for health in probe.latest_health.values())
        assert breaker._failure_times
        assert breaker._last_failure_at is not None
        assert not probe.terminations, probe.failure_details()
        hold.unlink()
        probe.wait_for(lambda: bool(probe.supervisor.probe_mapping()['readiness']))
        probe.wait_for(lambda: not breaker._failure_times)
        assert breaker._last_failure_at is None
        assert sum(len(health.groups) for health in probe.latest_health.values()) == 1
        assert not probe.terminations, probe.failure_details()


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
    source_path, group_path = prepare_absent_orphan(config)
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=ScriptedWorkerFactory(),
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


def test_large_frame_spans_passes_while_others_progress(tmp_path: Path) -> None:
    config = _control_probe_config(tmp_path)
    with running_supervisor(config, ChildPatches(control_probe=True)) as probe:
        workers = _ready_control_probe(probe)
        worker = next(item for item in workers if item.spec.sources)
        other = next(item for item in workers if not item.spec.sources)
        previous = probe.latest_health[worker.worker_id].worker.heartbeat_at
        other_previous = probe.latest_health[other.worker_id].worker.heartbeat_at
        (tmp_path / f'{worker.spawn_instance}.large').touch()
        _wait_control_event(tmp_path, worker, 'frame-started')
        due_checkpoint = probe.supervisor._clock() - 1
        probe.supervisor._next_checkpoint_at = due_checkpoint
        passes = 0
        while not any(
            len(name) > 10_000
            for name in probe.latest_health[worker.worker_id].worker.thread_names
        ):
            probe.supervisor._tick()
            assert not probe.terminations, probe.failure_details()
            passes += 1
            assert probe.supervisor._next_checkpoint_at is not None
            assert probe.supervisor._next_checkpoint_at > due_checkpoint
        assert passes >= 2
        probe.wait_for(
            lambda: probe.latest_health[other.worker_id].worker.heartbeat_at > other_previous
        )
        assert not probe.terminations, probe.failure_details()
        assert probe.latest_health[worker.worker_id].worker.heartbeat_at > previous
        # The same large writer must still receive a stop without a parent drain.
        worker.send(StopCommand(reason='partial-stop'))
        stopped = _wait_control_event(tmp_path, worker, 'stop')
        assert stopped['reason'] == 'partial-stop'
        assert worker._process.wait() == 0


@pytest.mark.parametrize('wrapped', [False, True])
@pytest.mark.parametrize('wait_fails', [False, True])
def test_supervisor_run_caps_wait_and_unbinds_progress(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    wrapped: bool,
    wait_fails: bool,
) -> None:
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
    config = replace(
        loaded,
        daemon=replace(loaded.daemon, supervisor_poll_interval_secs=0.025),
    )
    factory = ScriptedWorkerFactory()
    observed = _ObservedForegroundWait(fails=wait_fails)
    wait: Callable[[float], None] = (
        (lambda timeout: observed.serve_once(timeout=timeout)) if wrapped else observed.serve_once
    )
    with Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()

        if wait_fails:
            with pytest.raises(RuntimeError, match='wait failed'):
                supervisor.run(
                    should_stop=lambda: bool(observed.calls),
                    wait=wait,
                    set_foreground_progress=observed.set_foreground_progress,
                )
        else:
            supervisor.run(
                should_stop=lambda: bool(observed.calls),
                wait=wait,
                set_foreground_progress=observed.set_foreground_progress,
            )

    assert observed.calls == [pytest.approx(0.025)]
    assert len(observed.bindings) == 2
    assert observed.bindings[0] is not None
    assert observed.bindings[1] is None


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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
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
        source = next(iter(factory.instances.values()))
        source.wait_results.append(None)
        source.authentication_ready = True
        if control_stage == 'authenticate':

            def fail_authentication(*, timeout: float = 0.0) -> bool:
                assert timeout == 0.0
                raise error_factory()

            monkeypatch.setattr(source, 'authenticate', fail_authentication)
        elif control_stage == 'assign':

            def fail_assignment(_assignment: WorkerAssignment) -> None:
                raise error_factory()

            monkeypatch.setattr(source, 'assign', fail_assignment)
        else:

            def fail_receive(*, timeout: float | None = None) -> ControlMessage:
                assert timeout == 0.0
                raise error_factory()

            monkeypatch.setattr(source, 'receive', fail_receive)

        assert supervisor._tick()

        assert source.killed
        expected_reason = (
            'worker-control-send-failed' if control_stage == 'assign' else 'worker-control-eof'
        )
        assert supervisor._spawns[source.worker_id].termination_reason == expected_reason
        assert source.worker_id in supervisor._spawns

        assert supervisor._tick()

        restart_deadlines = vars(supervisor)['_restart_deadlines']
        assert source.worker_id not in supervisor._spawns
        assert restart_deadlines[source.worker_id] == 1.0

        factory.authentication_ready = True
        clock.advance(1.0)
        assert supervisor._tick()
        assert factory.instances[source.worker_id] is not source
    finally:
        supervisor._close()


@pytest.mark.parametrize('queued_first', [False, True])
def test_queued_dependents_do_not_expire_behind_other_grants(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    queued_first: bool,
) -> None:
    stall_detectors.enable('startup')
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    config = replace(
        loaded,
        daemon=replace(loaded.daemon, worker_startup_deadline_secs=30.0),
        sites={
            'test': replace(
                site,
                groups={
                    f'group-{index}': replace(
                        site.groups['remote'], name=f'group-{index}', worker='dedicated'
                    )
                    for index in range(5)
                },
            )
        },
    )
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(iter(factory.instances.values()))
        source.auto_grant_ready = False
        source.authentication_ready = True
        supervisor._tick()
        dependents = [worker for worker in factory.instances.values() if not worker.spec.sources]
        assert len(dependents) == 5
        for worker in dependents:
            worker.authentication_ready = True
        supervisor._tick()
        queued_ids = [worker.worker_id for worker in dependents[:4]]
        stuck_id = dependents[-1].worker_id

        for index, worker in enumerate(dependents[:4]):
            grant = source.sent_grants[-1]
            assert grant.dependent_spawn_instance == worker.spawn_instance
            if queued_first:
                supervisor._spawns[worker.worker_id] = supervisor._spawns.pop(worker.worker_id)
            clock.advance(10.0)
            for current in factory.instances.values():
                current.wait_results.append(None)
                if current.assignment is not None:
                    current.messages.append(health_update(current, heartbeat_at=clock()))
            source.messages.append(
                EventPeerReady(
                    source=grant.source,
                    group=grant.group,
                    dependent_spawn_instance=grant.dependent_spawn_instance,
                )
            )
            supervisor._tick()
            for current in dependents:
                assert supervisor._spawns[current.worker_id].termination_reason is None
            assert (
                source.sent_grants[-1].dependent_spawn_instance
                == dependents[index + 1].spawn_instance
            )
            assert supervisor._spawns[worker.worker_id].lifecycle is None

            waits: list[float] = []
            supervisor.run(
                should_stop=lambda waits=waits: bool(waits),
                wait=waits.append,
                set_foreground_progress=lambda _callback: None,
            )
            assert len(waits) == 1
            assert 0 < waits[0] <= config.daemon.supervisor_poll_interval_secs

        assert all(supervisor._spawns[worker_id].lifecycle is None for worker_id in queued_ids)
        clock.advance(30.0)
        for current in factory.instances.values():
            current.wait_results.append(None)
            if current.assignment is not None:
                current.messages.append(health_update(current, heartbeat_at=clock()))
        supervisor._tick()
        assert supervisor._spawns[stuck_id].termination_reason is None
        assert not source.killed
        assert all(not worker.killed for worker in dependents[:4])


@pytest.mark.parametrize('queued_first', [False, True])
def test_queued_dependent_survives_a_late_maintenance_turn(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    queued_first: bool,
) -> None:
    stall_detectors.enable('startup')
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    config = replace(
        loaded,
        daemon=replace(loaded.daemon, worker_startup_deadline_secs=30.0),
        sites={
            'test': replace(
                site,
                groups={
                    f'group-{index}': replace(
                        site.groups['remote'], name=f'group-{index}', worker='dedicated'
                    )
                    for index in range(3)
                },
            )
        },
    )
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(iter(factory.instances.values()))
        source.auto_grant_ready = False
        source.authentication_ready = True
        supervisor._tick()
        dependents = [worker for worker in factory.instances.values() if not worker.spec.sources]
        assert len(dependents) == 3
        for worker in dependents:
            worker.authentication_ready = True
        supervisor._tick()
        expired, first, second = dependents
        if queued_first:
            supervisor._spawns[expired.worker_id] = supervisor._spawns.pop(expired.worker_id)
        clock.advance(60.0)
        source.messages.append(health_update(source, heartbeat_at=clock()))
        for worker in dependents:
            worker.wait_results.append(None)
        supervisor._tick()
        assert supervisor._spawns[first.worker_id].termination_reason is None
        assert supervisor._spawns[second.worker_id].termination_reason is None
        assert supervisor._spawns[expired.worker_id].termination_reason is None
        assert source.sent_grants[-1].dependent_spawn_instance == expired.spawn_instance

        for worker in (expired, first, second):
            grant = source.sent_grants[-1]
            if queued_first:
                supervisor._spawns[worker.worker_id] = supervisor._spawns.pop(worker.worker_id)
            clock.advance(10.0)
            for current in factory.instances.values():
                current.wait_results.append(None)
                if current.assignment is not None:
                    current.messages.append(health_update(current, heartbeat_at=clock()))
            source.messages.append(
                EventPeerReady(
                    source=grant.source,
                    group=grant.group,
                    dependent_spawn_instance=grant.dependent_spawn_instance,
                )
            )
            supervisor._tick()
            assert supervisor._spawns[worker.worker_id].lifecycle is None
            if worker is first:
                assert source.sent_grants[-1].dependent_spawn_instance == second.spawn_instance
        assert all(
            supervisor._spawns[worker.worker_id].lifecycle is None for worker in (first, second)
        )
        assert not source.killed
        assert not first.killed
        assert not second.killed


def test_monitor_killed_during_a_gap_is_contained_after_its_bytes(
    stall_detectors: StallDetectors, tmp_path: Path
) -> None:
    config = _control_probe_config(tmp_path, ())
    with running_supervisor(config, ChildPatches(control_probe=True)) as probe:
        worker = _ready_control_probe(probe)[0]
        previous = worker.last_arrival_at
        probe.wait_for(lambda: worker.last_arrival_at != previous)
        (tmp_path / f'{worker.spawn_instance}.freeze').touch()
        _wait_control_event(tmp_path, worker, 'frozen')
        probe.supervisor._tick()
        assert not probe.terminations, probe.failure_details()
        arrival = worker.last_arrival_at
        assert arrival is not None
        with stall_detectors.enabled('heartbeat'):
            deadline = arrival + 5 * probe.supervisor._config.daemon.health_interval_secs
            assert (
                probe.supervisor._worker_health_failure(
                    worker.worker_id, probe.latest_health[worker.worker_id], deadline - 0.001
                )
                is None
            )
            probe.supervisor._monitor_worker_health(deadline)
            assert [item.reason for item in probe.terminations] == ['worker-heartbeat-overdue']


@pytest.mark.parametrize('lock_kind', ['source', 'group', 'retained-group', 'write-lock'])
def test_supervisor_takeover_is_bounded_while_a_worker_lock_remains(
    lock_kind: Literal['source', 'group', 'retained-group', 'write-lock'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, original = source_only_config(tmp_path, monkeypatch)
    config = original
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
import threading
from pathlib import Path
from sg_event_daemon._platform import acquire_lock, secure_open_regular_file, try_flock_exclusive

if sys.argv[3] == 'write-lock':
    fd, _ = secure_open_regular_file(Path(sys.argv[1]))
    try:
        assert try_flock_exclusive(fd)
        os.write(int(sys.argv[2]), b'L')
        threading.Event().wait()
    finally:
        os.close(fd)
else:
    with acquire_lock(Path(sys.argv[1])):
        os.write(int(sys.argv[2]), b'L')
        threading.Event().wait()
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
        clock = FakeClock()
        with monkeypatch.context() as patch:
            patch.setattr(
                supervisor_module,
                'time',
                SimpleNamespace(monotonic=clock, sleep=clock.advance),
            )
            with pytest.raises(TimeoutError, match=rf'{held_path.name}'):
                supervisor._open()
            assert clock() == config.daemon.worker_startup_deadline_secs
    finally:
        if supervisor is not None:
            supervisor._close()
        os.close(ready_read)
        if ready_write >= 0:
            os.close(ready_write)
        if process is not None:
            terminate_child(process, force=True)
            assert process.wait() is not None

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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        source_update = health_update(source, heartbeat_at=0.0)
        dependent_update = health_update(dependent, heartbeat_at=0.0)
        updates = {
            source.worker_id: source_update,
            dependent.worker_id: dependent_update,
        }
        complete = {source.worker_id, dependent.worker_id}
        if missing == 'worker':
            updates = {
                source.worker_id: replace(source_update, groups=dependent_update.groups),
            }
        elif missing == 'source':
            updates[source.worker_id] = replace(source_update, sources=())
        elif missing == 'group':
            updates[dependent.worker_id] = replace(dependent_update, groups=())
        else:
            complete.remove(source.worker_id)
        for worker_id, spawn in supervisor._spawns.items():
            spawn.health = updates.get(worker_id)
            spawn.health_inventory_complete = worker_id in complete
        build_snapshot = cast(
            'Callable[[float], ProbeSnapshot]',
            vars(Supervisor)['_probe_snapshot'].__get__(supervisor),
        )

        snapshot = build_snapshot(0.0)

    assert not snapshot.complete
    assert not snapshot.ready
    assert snapshot.conditions[0] == 'stale'
    assert 'healthy' not in snapshot.conditions


@pytest.mark.parametrize(
    ('phase', 'elapsed'),
    [
        ('callback', 120.001),
        ('internal', 192.001),
    ],
)
def test_supervisor_tick_uses_stale_lane_heartbeat_with_fresh_worker_monitor(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    phase: Literal['callback', 'internal'],
    elapsed: float,
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        group_path = source_database(config.daemon.state_dir)
        clock.advance(elapsed)
        group_health = _in_flight_group_health(
            dependent,
            heartbeat_at=0.0,
            phase=phase,
        )
        for worker in factory.instances.values():
            worker.messages.append(
                health_update(
                    worker,
                    heartbeat_at=clock(),
                    groups=(group_health,) if worker is dependent else (),
                )
            )

        with _conditional_group(group_path):
            supervisor._tick()

        assert dependent.killed
        expected = _park_evidence(
            failed_worker_id=dependent.worker_id,
            observed_at_wall_clock=clock(),
        )
        expected_evidence = {('test', 'remote'): expected} if phase == 'callback' else {}
        assert supervisor._park_evidence_by_group == expected_evidence
        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == ((expected,) if phase == 'callback' else ())
    finally:
        supervisor._close()


@pytest.mark.parametrize(
    ('operation', 'error_factory', 'expected_reason'),
    [
        ('send', lambda: TimeoutError('send timed out'), 'worker-control-send-timeout'),
        ('send', lambda: EOFError('send closed'), 'worker-control-send-failed'),
        ('send', lambda: OSError('send failed'), 'worker-control-send-failed'),
        ('send', lambda: ValueError('send poisoned'), 'worker-control-send-failed'),
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='transport-failure-command',
        command='park',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=0.0))
        supervisor._tick()
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)

        if operation == 'send':

            def fail_send(_message: ControlMessage) -> None:
                raise error_factory()

            monkeypatch.setattr(owner, 'send', fail_send)
        else:

            def fail_receive(*, timeout: float | None = None) -> ControlMessage:
                assert timeout is not None
                assert timeout > 0
                raise error_factory()

            monkeypatch.setattr(owner, 'receive', fail_receive)

        with pytest.raises(OperatorOutcomeUnknownError, match='park did not finish:'):
            supervisor.dispatch_operator_command(command, timeout=1.0)

        assert owner.killed
        assert supervisor._spawns[owner.worker_id].termination_reason == expected_reason
        assert not _spawn_health(supervisor, owner.worker_id).worker.monitor_alive


def test_operator_retry_ignores_result_from_prior_transport_attempt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        owner = next(
            worker for worker in factory.instances.values() if 'remote' in worker.spec.groups
        )
        owner.operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'revision': next(revisions), 'status': 'parked'},
        )

        first = supervisor.dispatch_operator_command(command, timeout=1.0)
        owner.messages.append(
            CommandResult(
                command=command.command,
                ok=True,
                message='late',
                request_id=owner.sent_commands[0].request_id,
                result={'revision': 8, 'status': 'parked'},
            )
        )
        second = supervisor.dispatch_operator_command(command, timeout=1.0)

    assert owner.sent_commands[0].request_id != owner.sent_commands[1].request_id
    assert first.request_id == command.request_id
    assert second.request_id == command.request_id
    assert second.result == {'revision': 9, 'status': 'parked'}


@pytest.mark.parametrize('tail_failure', ['eof', 'frame', 'closed'])
@pytest.mark.parametrize('accepted', [False, True])
def test_supervisor_preserves_result_before_transport_tail_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    tail_failure: str,
    accepted: bool,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='result-before-tail-failure',
        command='park',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with (
        Supervisor(
            config=config, config_path=config_path, clock=FakeClock(), worker_factory=factory
        ) as supervisor,
        ExitStack() as cleanup,
    ):
        supervisor._reconcile(generations={})
        supervisor.start()
        for _ in range(3):
            supervisor._tick()
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        left, right = socket.socketpair()
        receiver = WorkerControl(left, max_frame_bytes=1_048_576)
        sender = WorkerControl(right, max_frame_bytes=1_048_576)
        cleanup.callback(receiver.close)
        cleanup.callback(sender.close)

        def send_result(sent: OperatorCommand) -> None:
            send_control(
                sender,
                CommandResult(
                    command=sent.command,
                    ok=accepted,
                    message='committed' if accepted else 'refused',
                    request_id=sent.request_id,
                    result={'revision': 3},
                ),
            )

        def receive(*, timeout: float) -> ControlMessage:
            message = receiver.receive(timeout=timeout)
            if tail_failure == 'eof':
                sender.close()
            elif tail_failure == 'frame':
                right.sendall(b'\x00\x00\x00\x01!')
            else:
                receiver.close()
            return message

        monkeypatch.setattr(owner, 'send', send_result)
        monkeypatch.setattr(owner, 'receive', receive)
        monkeypatch.setattr(owner, 'receive_available', receiver.receive_available)
        result = supervisor.dispatch_operator_command(command, timeout=1)
        assert result == CommandResult(
            command='park',
            ok=accepted,
            message='committed' if accepted else 'refused',
            request_id=command.request_id,
            result={'revision': 3},
        )
        assert owner.killed
        assert supervisor._spawns[owner.worker_id].termination_reason == 'worker-control-eof'
        assert supervisor._dispatch_reserved is None


def test_operator_waits_for_dependent_grant_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        grant = source.sent_grants[-1]
        ready = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=grant.dependent_spawn_instance,
        )
        source.messages.append(ready)
        queued = tuple(source.messages)

        with pytest.raises(
            ValueError,
            match='has no running worker',
        ):
            supervisor.dispatch_operator_command(command, timeout=1.0)

        assert tuple(source.messages) == queued
        assert source.sent_commands == []
        for _ in range(2):
            supervisor._tick()
            if supervisor._spawns[dependent.worker_id].lifecycle is None:
                break
        else:
            pytest.fail('dependent grant lifecycle did not complete')

        source.operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'source': 'admin', 'status': 'running'},
        )
        result = supervisor.dispatch_operator_command(command, timeout=1.0)

    assert result.request_id == command.request_id
    assert result.result == {'source': 'admin', 'status': 'running'}
    assert len(source.sent_commands) == 1


def test_outside_hold_past_startup_deadline_preserves_workers(tmp_path: Path) -> None:
    config = write_daemon_config(
        tmp_path, Topology('dedicated', ('local', 'remote'), supervisor_poll_interval_secs=0.02)
    )
    config.write_text(
        config.read_text().replace(
            '[sites.test.groups.local]\nworker = "dedicated"',
            '[sites.test.groups.local]\nworker = "default"',
        )
    )
    with running_supervisor(
        config,
        ChildPatches(control_probe=True, lifecycle_probe=True, last_event_id=4, gate_events=True),
    ) as probe:
        workers = _ready_control_probe(probe)
        assert not probe.terminations, probe.failure_details()
        dependent = next(worker for worker in workers if not worker.spec.sources)
        database = source_database(probe.supervisor._config.daemon.state_dir)
        with closing(sqlite3.connect(database, isolation_level=None)) as writer:
            writer.execute('BEGIN IMMEDIATE')
            try:
                (tmp_path / 'events-ready').touch()
                probe.wait_for(
                    lambda: any(
                        source.phase == 'write-lock-wait'
                        for health in probe.latest_health.values()
                        for source in health.sources
                    ),
                )
                (tmp_path / 'local-call-arm').touch()
                probe.wait_for((tmp_path / 'local-call-entered').exists)
                probe.supervisor._replace_worker(dependent.worker_id)
                replacement = probe.supervisor._spawns[dependent.worker_id].worker
                before = len(probe.terminations)
                probe.wait_for(
                    lambda: any(
                        group.phase == 'write-lock-wait'
                        for health in probe.latest_health.values()
                        for group in health.groups
                    )
                )
                _assert_write_wait_policy(probe)
                assert len(probe.terminations) == before, probe.failure_details()
                assert all(
                    cast('_WorkerInstance', spawn.worker)._process.poll() is None
                    for spawn in probe.supervisor._spawns.values()
                )
            finally:
                writer.rollback()
        probe.wait_for((tmp_path / 'local-call-returned').exists)
        assert (tmp_path / 'local-call-entered').exists()
        _ready_control_probe(probe)
        assert probe.supervisor._spawns[dependent.worker_id].worker is replacement
        assert all(not breaker._failure_times for breaker in probe.breakers.values())


def test_outside_writer_on_supervisor_db_keeps_supervision_running(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    config = write_daemon_config(tmp_path, Topology('dedicated', ('remote',)))
    root = logging.getLogger('sg_event_daemon')
    monkeypatch.setattr(root, 'handlers', [caplog.handler])
    monkeypatch.setattr(root, 'propagate', False)
    caplog.set_level(logging.INFO, logger='sg_event_daemon')
    with running_supervisor(config, ChildPatches()) as probe:
        probe.wait_for(lambda: len(probe.latest_health) == 2)
        supervisor = probe.supervisor
        state = supervisor._state
        assert state is not None
        breaker = next(iter(probe.breakers.values()))
        clock = FakeClock()
        breaker._clock = clock
        breaker.record_failure('prior failure')
        clock.advance(breaker._healthy_reset_secs)
        state.persist_worker_breaker(breaker)
        heartbeat = probe.latest_health[breaker.worker_id].worker.heartbeat_at
        with _outside_supervisor_writer(state):
            probe.wait_for(
                lambda: probe.latest_health[breaker.worker_id].worker.heartbeat_at > heartbeat,
            )
            assert breaker._last_failure_at is None
            assert 'write-lock-stalled' in cast(
                'tuple[str, ...]', supervisor.status_mapping({})['conditions']
            )
            assert not supervisor.status_mapping({})['readiness']
            assert not probe.terminations
            row = state._database.connection.execute(
                'SELECT last_failure_at FROM worker_breaker WHERE worker_id = ?',
                (breaker.worker_id,),
            ).fetchone()
            assert row[0] is not None
        supervisor._tick()
        row = state._database.connection.execute(
            'SELECT last_failure_at FROM worker_breaker WHERE worker_id = ?',
            (breaker.worker_id,),
        ).fetchone()
        assert row[0] is None
        assert 'write-lock-stalled' not in cast(
            'tuple[str, ...]', supervisor.status_mapping({})['conditions']
        )
        assert sum('writes are pending:' in record.message for record in caplog.records) == 1
        assert (
            sum('pending writes stored after' in record.message for record in caplog.records) == 1
        )


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
    breaker.record_failure('ambiguous-worker-failure')
    before = breaker.projection
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'boot-1')

    with _SupervisorState.open(tmp_path / 'state') as state:
        state.persist_worker_breaker(breaker)
        with pytest.raises(ValueError, match=r'4096.*UTF-8'):
            state.reset_worker_breaker(
                breaker,
                expected_revision=1,
                reason=reason,
                identity=AuditIdentity.current(),
                wall_clock_time=20.0,
            )
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 0

    assert len(reason.encode()) == 4_097
    assert breaker.projection == before


def test_passed_clock_with_a_frame_in_flight_waits_at_most_the_poll_interval(
    stall_detectors: StallDetectors,
    tmp_path: Path,
) -> None:
    config = _lifecycle_probe_config(tmp_path, remote=True)
    with running_supervisor(
        config, ChildPatches(control_probe=True, lifecycle_probe=True, last_event_id=4)
    ) as probe:
        workers = _ready_control_probe(probe)
        dependent = next(worker for worker in workers if not worker.spec.sources)
        probe.supervisor._replace_worker(dependent.worker_id)
        dependent = cast('_WorkerInstance', probe.supervisor._spawns[dependent.worker_id].worker)
        (tmp_path / f'{dependent.spawn_instance}.large').touch()
        (tmp_path / f'{dependent.spawn_instance}.write-once').touch()
        probe.wait_for(lambda: probe.supervisor._spawns[dependent.worker_id].lifecycle is None)
        _wait_lifecycle_file(
            tmp_path / f'{dependent.spawn_instance}.writer-frozen', worker=dependent
        )
        spawn = probe.supervisor._spawns[dependent.worker_id]
        spawn.health_deadline = probe.supervisor._clock() - 1
        probe.supervisor._tick()
        assert spawn.termination_reason is None, probe.failure_details()
        assert not spawn.health_inventory_complete
        arrival = dependent.last_arrival_at
        assert arrival is not None
        with stall_detectors.enabled('startup', 'heartbeat'):
            silence = arrival + 5 * probe.supervisor._config.daemon.health_interval_secs
            assert probe.supervisor._worker_monitor_deadline(spawn, silence) == (
                silence + probe.supervisor._config.daemon.supervisor_poll_interval_secs
            )
            assert (
                probe.supervisor._worker_health_failure(
                    dependent.worker_id, spawn.health, silence - 0.001
                )
                is None
            )
            assert (
                probe.supervisor._worker_health_failure(dependent.worker_id, spawn.health, silence)
                == 'worker-startup-timeout'
            )


def test_pending_disposition_commits_before_single_group_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    config = write_daemon_config(tmp_path, Topology('dedicated', ('remote',)))
    with running_supervisor(config, ChildPatches()) as probe:
        probe.wait_for(lambda: len(probe.latest_health) == 2)
        supervisor = probe.supervisor
        state = supervisor._state
        assert state is not None
        identity = AuditIdentity(local_uid=1, username='operator', pid=1)
        parked = supervisor.dispatch_operator_command(
            OperatorCommand(
                request_id='park',
                command='park',
                arguments={'site': 'test', 'group': 'remote', 'reason': 'test'},
                identity=identity,
            ),
            timeout=float('inf'),
        )
        assert parked.ok
        command = OperatorCommand(
            request_id='reset',
            command='reset-group-to-latest',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'test'},
            identity=identity,
        )
        # Recreate the reserved re-add startup path: workers receive the pending
        # assignment and the source owner completes the disposition for the fenced lane.
        reservation = state.reserve_group_disposition(
            site='test',
            group='remote',
            observed_generation=1,
        )
        assert supervisor._reconciled is not None
        supervisor._reconciled[('test', 'remote')] = reservation
        restarted = supervisor._restart_source_owners('test', 'admin', ('remote',), parked)
        assert restarted.ok
        probe.wait_for(
            lambda: (
                bool(probe.latest_health)
                and all(spawn.lifecycle is None for spawn in supervisor._spawns.values())
            ),
        )
        # Hold only after the genuine worker has committed its disposition.
        writer = sqlite3.connect(state._database.path, isolation_level=None)
        complete = Supervisor._complete_disposition_inventory
        calls: list[PendingDispositionSourceGroupAssignment] = []

        def held_complete(
            owner: Supervisor,
            site: str,
            reservation: PendingDispositionSourceGroupAssignment,
            result: CommandResult,
        ) -> CommandResult:
            writer.execute('BEGIN IMMEDIATE')
            calls.append(reservation)
            return complete(owner, site, reservation, result)

        try:
            with monkeypatch.context() as patch:
                patch.setattr(Supervisor, '_complete_disposition_inventory', held_complete)
                result = supervisor.dispatch_group_disposition_command(
                    command, timeout=float('inf')
                )
            assert result.ok
            assert len(calls) == 1
            reservation = calls[0]
            assert isinstance(
                supervisor._reconciled[('test', 'remote')], PendingDispositionSourceGroupAssignment
            )
            spawns = len(probe.spawns)
            assert complete(supervisor, 'test', reservation, result) == result
            assert len(probe.spawns) == spawns
            record = state.group_inventory_record(site='test', group='remote')
            assert record is not None
            assert record.lifecycle == 'readded-parked'
            writer.rollback()
            probe.wait_for(
                lambda: (
                    supervisor._reconciled is not None
                    and isinstance(
                        supervisor._reconciled[('test', 'remote')], ActiveSourceGroupAssignment
                    )
                ),
            )
            probe.wait_for(lambda: len(probe.spawns) > spawns)
            resumed = len(probe.spawns)
            assert complete(supervisor, 'test', reservation, result) == result
            supervisor._tick()
            assert len(probe.spawns) == resumed
        finally:
            writer.close()


@pytest.mark.parametrize('unreaped', [False, True])
@pytest.mark.parametrize('control_built', [False, True])
def test_failed_spawn_reaps_despite_socket_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    unreaped: bool,
    control_built: bool,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
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
        _WorkerInstance.spawn(
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


def test_pending_evidence_follows_group_across_default_named_and_dedicated_packing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    original_config = config_path.read_text(encoding='utf-8')
    evidence = _park_evidence()
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
    _create_recovery_group_database(config)

    for packing in ('default', 'pool', 'dedicated'):
        config_path.write_text(
            _replace_config_text(original_config, 'worker = "pool"', f'worker = "{packing}"'),
            encoding='utf-8',
        )
        packed_config = load_config(config_path)
        factory = ScriptedWorkerFactory()
        token_index = 0

        def token(*, prefix: str = packing) -> str:
            nonlocal token_index
            token_index += 1
            return f'{prefix}-identity-{token_index}'

        with Supervisor(
            clock=FakeClock(),
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
                for worker in factory.instances.values()
                if worker.assignment is not None
                and any(item.group == 'remote' for item in worker.assignment.group_assignments)
            )
            assert owner.assignment is not None
            delivery = next(
                item for item in owner.assignment.group_assignments if item.group == 'remote'
            )
            assert isinstance(delivery, DeliveryGroupAssignment)
            assert delivery.conditional_park == evidence.directive


def test_pending_flush_preserves_timeout_and_nonbusy_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _SupervisorState.open(tmp_path) as state:
        connection = state._database.connection
        connection.execute('PRAGMA busy_timeout = 137')
        with state._write_transaction(0) as transaction_connection:
            assert transaction_connection is connection
            assert connection.execute('PRAGMA busy_timeout').fetchone()[0] == 137
        with _outside_supervisor_writer(state):
            attempts: list[int] = []
            write_transaction = state._write_transaction

            @contextmanager
            def observe(timeout_millis: int) -> Generator[sqlite3.Connection]:
                attempts.append(timeout_millis)
                with write_transaction(timeout_millis) as acquired:
                    yield acquired

            monkeypatch.setattr(state, '_write_transaction', observe)
            state.persist_worker_breaker(_pending_test_breaker())
            assert attempts == [0]
            assert not state.flush_pending()
            assert connection.execute('PRAGMA busy_timeout').fetchone()[0] == 137
        connection.execute('PRAGMA query_only = ON')
        with pytest.raises(DatabaseError, match='readonly'):
            state.flush_pending()
        assert connection.execute('PRAGMA busy_timeout').fetchone()[0] == 137
        connection.execute('PRAGMA query_only = OFF')
        assert state.flush_pending()


def test_pending_group_publishes_valid_health_during_slow_provider_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config, spec, assignment, _snapshot = worker_tests.pending_group_runtime(tmp_path)
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
    original = worker_tests.RuntimeProviderHandle.find

    def find(
        handle: worker_tests.RuntimeProviderHandle,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[worker_tests._FindOptions],
    ) -> list[dict[str, object]]:
        if not entered.is_set() and filters:
            entered.set()
            assert release.wait()
            finished.set()
            return []
        if filters:
            return []
        return original(handle, entity_type, filters, fields, **options)

    monkeypatch.setattr(worker_tests.RuntimeProviderHandle, 'find', find)
    publications = _HealthPublications(monkeypatch)
    updates: list[HealthUpdate] = []
    with (
        _pending_supervisor_health_owner(config, spec, assignment) as (supervisor, worker),
        worker_tests.running_concrete_runtime(
            monkeypatch, config, spec, assignment, worker_tests.runtime_provider()
        ) as (runtime, control),
    ):
        try:
            assert entered.wait()
            update = worker_tests.receive_typed_health(
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
                supervisor, worker, update, published_at=publications.published_at(update)
            )
            for _ in range(3):
                update = _latest_available_health(
                    control, worker_tests.receive_type(control, HealthUpdate)
                )
                updates.append(update)
                _assert_pending_provider_publication(
                    supervisor, worker, update, published_at=publications.published_at(update)
                )
            assert len(updates) == 4
            assert all(item.groups[0].parked for item in updates)
            assert all(
                item.groups[0].conditions == ('parked', 'replay-required') for item in updates
            )
            assert all(watch.kind != 'lane' for item in updates for watch in item.watchdogs)
            source_watch = next(watch for watch in updates[-1].watchdogs if watch.kind == 'source')
            assert source_watch.deadline_at is not None
            assert source_watch.deadline_at > source_watch.heartbeat_at
            assert runtime.fatal_reason is None
            assert runtime._threads['source:events'].is_alive()
        finally:
            release.set()
        assert finished.wait()
        successful = worker_tests.receive_typed_health(
            control,
            lambda item: bool(item.sources) and item.sources[0].successful_clock > 0,
        )
        successful = _latest_available_health(control, successful)
        updates.append(successful)
        _assert_pending_provider_publication(
            supervisor, worker, successful, published_at=publications.published_at(successful)
        )
        assert successful.groups[0].parked
        assert runtime.fatal_reason is None
    (tmp_path / 'pending-provider-health.jsonl').write_bytes(
        b'\n'.join(ipc_module.encode_control_message(update) for update in updates) + b'\n'
    )


def test_pending_open_projection_is_lost_on_supervisor_process_restart(tmp_path: Path) -> None:
    context = multiprocessing.get_context('spawn')
    parent, child = context.Pipe()
    process = context.Process(target=_pending_restart_child, args=(str(tmp_path), child))
    process.start()
    child.close()
    try:
        assert parent.poll(None)
        assert parent.recv() == 'ready'
        with closing(sqlite3.connect(tmp_path / 'supervisor.db', isolation_level=None)) as writer:
            writer.execute('BEGIN IMMEDIATE')
            parent.send('fail')
            assert parent.poll(None)
            assert parent.recv() == ('pending-open', True)
            process.join()
            assert process.exitcode == -signal.SIGKILL
            writer.rollback()
        with _SupervisorState.open(tmp_path) as state:
            breaker = state.restore_worker_breaker(
                worker_id='pending-worker',
                restart_limit=2,
                restart_window_secs=100,
                healthy_reset_secs=0.1,
            )
            assert not breaker.is_open
            assert breaker._failure_total == 0
            assert breaker.history == ()
    finally:
        if process.is_alive():
            process.kill()
        process.join()
        parent.close()


@pytest.mark.parametrize('reap_fails', [False, True])
def test_failed_spawn_cleanup_retains_termination_and_reap_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reap_fails: bool,
) -> None:
    _config_path, config = source_only_config(tmp_path, monkeypatch)
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
    clock = FakeClock()
    with _SupervisorState.open(tmp_path / 'state') as state:
        breaker = _WorkerBreaker(
            worker_id='worker',
            restart_limit=1,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        breaker.record_failure('first')
        if is_open:
            breaker.record_failure('second')
        state.persist_worker_breaker(breaker)
        before = breaker.projection
        restored = state.restore_worker_breaker(
            worker_id='worker',
            restart_limit=5,
            restart_window_secs=200.0,
            healthy_reset_secs=30.0,
            clock=clock,
        )
        expected = replace(
            before, restart_limit=5, restart_window_secs=200.0, healthy_reset_secs=30.0
        )
        assert restored.projection == expected
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
    source_path, group_path = prepare_absent_orphan(config)
    command = OperatorCommand(
        request_id='reset-restart',
        command='reset-group-to-latest',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'retire history'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    _inject_orphan_disposition_failure(monkeypatch, failure_stage)
    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=ScriptedWorkerFactory(),
    ) as supervisor:
        supervisor.reconcile_durable_state()
        with pytest.raises(TimeoutError, match=f'injected {failure_stage} failure'):
            supervisor.dispatch_group_disposition_command(command, timeout=1.0)
    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=ScriptedWorkerFactory(),
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


def test_pending_provider_publication_accepts_source_transition_after_worker_sample(
    stall_detectors: StallDetectors, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    clock = FakeClock()
    clock.advance(100)
    monkeypatch.setattr(time, 'monotonic', clock)
    publications = _HealthPublications(monkeypatch)
    factory = ScriptedWorkerFactory()
    first_socket, second_socket = socket.socketpair()
    sender = WorkerControl(first_socket, max_frame_bytes=131072)
    receiver = WorkerControl(second_socket, max_frame_bytes=131072)
    with (
        closing(sender),
        closing(receiver),
        Supervisor(
            config=config, config_path=config_path, clock=clock, worker_factory=factory
        ) as supervisor,
    ):
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory.instances.values() if item.spec.sources)
        dependent = next(item for item in factory.instances.values() if item.spec.groups)
        assert not supervisor._accept_worker_health(
            dependent, health_update(dependent, heartbeat_at=clock())
        )
        assert worker.assignment is not None
        runtime = worker_module.build_worker_runtime(
            config, worker.spec, worker.assignment, sender
        )
        runtime._outbox = ipc_module.WorkerControlOutbox(sender)
        runtime._construct_components()
        source = runtime._components['source:admin']
        assert isinstance(source, worker_module._SourceComponent)
        cache = runtime._health_cache
        assert cache is not None
        cache.publish_source(
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
        runtime._outbox.write_available()
        update = worker_tests.receive_type(receiver, HealthUpdate)
        watch = next(item for item in update.watchdogs if item.kind == 'source')
        published_at = publications.published_at(update)
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
        assert worker.killed
        assert supervisor._spawns[worker.worker_id].termination_reason == 'source-phase-overdue'


@pytest.mark.parametrize('worker_mode', ['default', 'pool', 'dedicated'])
@pytest.mark.parametrize('command_name', ['replay', 'reset-group-to-latest'])
def test_pending_disposition_restarts_source_and_delivery_owners(
    command_name: str,
    worker_mode: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    config = replace(
        loaded,
        sites={
            'test': replace(
                site, groups={'remote': replace(site.groups['remote'], worker=worker_mode)}
            )
        },
    )
    factory = ScriptedWorkerFactory()
    group_path = source_database(config.daemon.state_dir)
    prepare_absent_orphan(config)
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

    with Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    ) as supervisor:
        supervisor.reconcile_durable_state()
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        owner_id = next(
            spec.worker_id
            for spec in build_worker_specs(config.sites['test'])
            if 'remote' in spec.groups
        )
        owner = factory.instances[owner_id]
        assert all(not worker.spec.groups for worker in factory.instances.values())
        source.operator_handler = lambda command: _execute_source_group_command(
            group_path, command
        )
        result = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert result.ok
        new_source = factory.instances[source.worker_id]
        new_owner = factory.instances[owner.worker_id]
        assert new_source is not source
        assert new_owner is not owner
        assert new_source.assignment is not None
        assert new_owner.assignment is not None
        assert isinstance(
            new_source.assignment.source_group_assignments[0], ActiveSourceGroupAssignment
        )
        assert isinstance(new_owner.assignment.group_assignments[0], DeliveryGroupAssignment)
        new_owner.operator_handler = accept
        assert supervisor.dispatch_operator_command(
            replace(command, command='unpark'), timeout=1.0
        ).ok
        assert new_owner.sent_commands[-1].command == 'unpark'


@pytest.mark.parametrize('mode', ['readd', 'absent'])
@pytest.mark.parametrize('stage', ['reservation', 'source-committed', 'inventory-completed'])
def test_lifecycle_survives_supervisor_death_at_each_store_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, stage: str
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, configured = source_only_config(tmp_path, monkeypatch)
    source_path, _group_path = prepare_absent_orphan(configured)
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
        inventory = state.group_inventory_record(site='test', group='remote')
        assert inventory is not None
        assert inventory.generation == 2
        assert inventory.orphan_generation == (1 if stage == 'reservation' else None)
        if mode == 'readd' and stage == 'reservation':
            assert inventory.lifecycle == 'readded-parked'
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


def test_pending_stored_park_retirement_allows_new_real_callback_containment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _callback_hold_config(tmp_path)
    # Initialization chooses the latest visible event as its delivery floor.
    _reveal_callback_events(tmp_path, 1)
    with running_supervisor(config, ChildPatches(gate_events=True)) as initial:
        initial.wait_for(
            lambda: any(
                group.group == 'remote' and group.join_phase == 'attached'
                for health in initial.latest_health.values()
                for group in health.groups
            )
        )
        _reveal_callback_events(tmp_path, 4)
        initial.wait_for(lambda: len(initial.records('remote')) >= 3)
        previous_spawn = next(item for item in initial.spawns if 'remote' in item.worker_id)
        stale = _park_evidence(
            failed_worker_id=previous_spawn.worker_id,
            failed_spawn_instance=previous_spawn.spawn_instance,
            event_id=initial.records('remote')[0],
            group_revision=0,
        )
    with _SupervisorState.open(tmp_path / 'state') as seeded:
        assert seeded.record_park_evidence(stale)
    with running_supervisor(config, ChildPatches(hold_callback=True, gate_events=True)) as probe:
        supervisor = probe.supervisor
        state = supervisor._state
        assert state is not None
        observed: list[tuple[bool, tuple[_ParkEvidence, ...]]] = []
        original_kill = _WorkerInstance.kill

        def capture_attribution(worker: _WorkerInstance) -> None:
            spawn = supervisor._spawns[worker.worker_id]
            if spawn.termination_reason == 'lane-phase-overdue':
                observed.append(
                    (spawn.attributed_park_failure, tuple(probe.park_evidence.values()))
                )
            original_kill(worker)

        try:
            with _outside_supervisor_writer(state):
                assert probe.park_evidence == {('test', 'remote'): stale}
                # The real replacement worker acknowledges stale stored evidence;
                # its retirement cannot reach supervisor.db while this writer holds it.
                probe.wait_for(lambda: not probe.park_evidence)
                assert state.pending_park_evidence() == (stale,)
                with monkeypatch.context() as patch:
                    patch.setattr(_WorkerInstance, 'kill', capture_attribution)
                    (tmp_path / 'callback-arm').touch()
                    # Reserve a fresh callback until retirement and rearming finish.
                    _reveal_callback_events(tmp_path, 5)
                    probe.wait_for((tmp_path / 'callback-entered').exists)
                    (tmp_path / 'callback-expire').touch()
                    probe.wait_for(lambda: bool(observed))
                assert len(observed) == 1
                attributed, evidence = observed[0]
                assert attributed
                assert len(evidence) == 1
                replacement = evidence[0]
                assert replacement.directive.evidence_id != stale.directive.evidence_id
                assert (
                    replacement.directive.failed_spawn_instance
                    != stale.directive.failed_spawn_instance
                )
                assert replacement.directive.event_id > stale.directive.event_id
                assert replacement.directive.group_revision > stale.directive.group_revision
                assert state.pending_park_evidence() == (stale,)
                assert not state.record_park_evidence(_park_evidence(evidence_id='third'))
            assert state.flush_pending()
            assert state.pending_park_evidence() == (replacement,)
        finally:
            (tmp_path / 'callback-release').touch()


@pytest.mark.parametrize('held', ['source', 'group'])
def test_supervisor_direct_path_refuses_held_owner_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, held: str
) -> None:
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, _group_path = prepare_absent_orphan(config)
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


@pytest.mark.parametrize('kill_after_orphan', [False, True])
def test_new_removal_after_disposition_keeps_inventory_fence_until_orphan_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kill_after_orphan: bool
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, configured = source_only_config(tmp_path, monkeypatch)
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
    with _SupervisorState.open(configured.daemon.state_dir) as state:
        state.reconcile_groups(
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
        )
        assert killed.returncode == -signal.SIGKILL, killed.stderr
        assert proof.read_text() == 'orphan-committed'
        with _SupervisorState.open(configured.daemon.state_dir) as state:
            record = state.group_inventory_record(site='test', group='remote')
            assert record is not None
            assert record.configured
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
    with _SupervisorState.open(configured.daemon.state_dir) as state:
        record = state.group_inventory_record(site='test', group='remote')
        assert record is not None
        assert not record.configured


def test_probe_snapshot_parked_group_failure_is_visible_without_blocking_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        source_update = health_update(source, heartbeat_at=0.0)
        dependent_update = health_update(dependent, heartbeat_at=0.0)
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
            source.worker_id: source_update,
            dependent.worker_id: replace(dependent_update, groups=(parked_failure,)),
        }
        for worker_id, update in updates.items():
            supervisor._spawns[worker_id].health = update
            supervisor._spawns[worker_id].health_inventory_complete = True
        build_snapshot = cast(
            'Callable[[float], ProbeSnapshot]',
            vars(Supervisor)['_probe_snapshot'].__get__(supervisor),
        )

        snapshot = build_snapshot(0.0)

    assert snapshot.complete
    assert snapshot.ready
    assert 'parked' in snapshot.conditions
    assert 'callback-failed' in snapshot.conditions


@contextmanager
def _pending_supervisor_health_owner(
    config: Config, spec: WorkerSpec, assignment: WorkerAssignment
) -> Generator[tuple[Supervisor, ScriptedWorker]]:
    with _SupervisorState.open(config.daemon.state_dir) as ledger:
        ledger.reconcile_groups(
            configured={('main', 'local'): 'events'}, generations={('main', 'local'): 7}
        )
        ledger.reconcile_groups(configured={}, generations={('main', 'local'): 7})
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
    factory = ScriptedWorkerFactory()
    tokens = iter((dict(assignment.source_generations)['events'], assignment.spawn_instance))
    supervisor = Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config.daemon.state_dir / 'config.toml',
        worker_factory=factory,
        token_factory=lambda: next(tokens),
    )
    supervisor._open()
    supervisor._reconcile(generations={('main', 'local'): 7})
    supervisor.start()
    worker = factory.instances[spec.worker_id]
    try:
        yield supervisor, worker
    finally:
        supervisor._close()


@pytest.mark.parametrize('source_ready', [False, True])
def test_lane_less_invalid_reset_does_not_reserve_a_running_group(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, source_ready: bool
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    ensure_secure_state_dir(config.daemon.state_dir)
    path = source_database(config.daemon.state_dir)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with _lifecycle_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')
        with _lifecycle_group(path) as group:
            assert not group.snapshot().parked
    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=ScriptedWorkerFactory(),
    ) as supervisor:
        supervisor.reconcile_durable_state()
        if source_ready:
            supervisor.start()
            for spawn in tuple(supervisor._spawns.values()):
                worker = spawn.worker
                if worker.spec.groups:
                    supervisor._discard_worker_lifecycle(worker)
            assert any(
                worker.spec.sources
                for spawn in supervisor._spawns.values()
                for worker in (spawn.worker,)
            )
        assert supervisor._state is not None
        before = supervisor._state.group_inventory_record(site='test', group='remote')
        command = OperatorCommand(
            request_id='invalid-running-reset',
            command='reset-group-to-latest',
            arguments={'site': 'test', 'group': 'remote', 'reason': 'invalid while running'},
            identity=AuditIdentity.current(),
        )
        with pytest.raises(InvalidTransitionError, match='parked or orphaned'):
            supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert supervisor._state.group_inventory_record(site='test', group='remote') == before
        assert supervisor._state.generation_high_water('test', 'remote') == 1
        with _lifecycle_group(path) as group:
            group.park(
                reason='prepare reset',
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
                expected_revision=group.snapshot().revision,
            )
        for spawn in supervisor._spawns.values():
            worker = spawn.worker
            if worker.spec.sources:
                cast('ScriptedWorker', worker).operator_handler = lambda message: (
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
        inventory = supervisor._state.group_inventory_record(site='test', group='remote')
        retried = supervisor.dispatch_group_disposition_command(command, timeout=1.0)
        assert retried.ok
        assert retried.result == applied.result
        with closing(sqlite3.connect(path)) as connection, connection:
            assert tuple(connection.iterdump()) == before_retry
        assert supervisor._state.group_inventory_record(site='test', group='remote') == inventory
        assert supervisor._state.generation_high_water('test', 'remote') == 2


@pytest.mark.parametrize('drift', ['identity', 'ordering'])
@pytest.mark.parametrize('retry', ['exact', 'conflict', 'stale'])
def test_direct_disposition_setup_preserves_durable_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, drift: str, retry: str
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    path, _ = prepare_absent_orphan(config)
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
        before = worker_tests.drift_disposition_policy(path, 'remote', drift)
        assert supervisor._state is not None
        inventory = supervisor._state.group_inventory_record(site='test', group='remote')
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
        assert supervisor._state.group_inventory_record(site='test', group='remote') == inventory
        assert supervisor._state.generation_high_water('test', 'remote') == 2


@pytest.mark.parametrize('latest_alive', [False, True])
def test_pending_provider_health_uses_latest_queued_frame_without_health_filter(
    monkeypatch: pytest.MonkeyPatch,
    latest_alive: bool,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, 'monotonic', clock)
    publications = _HealthPublications(monkeypatch)
    first_socket, second_socket = socket.socketpair()
    sender = WorkerControl(first_socket, max_frame_bytes=65536)
    receiver = WorkerControl(second_socket, max_frame_bytes=65536)
    stale = HealthUpdate(
        worker=WorkerHealth(
            site='main',
            worker_id='owner',
            spawn_instance='spawn',
            heartbeat_at=0,
            monitor_alive=True,
            thread_names=(),
            stuck_threads=(),
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
        sender.send(stale, deadline_secs=1)
        clock.advance(10)
        sender.send(latest, deadline_secs=1)
        clock.advance(10)
        initial = worker_tests.receive_type(receiver, HealthUpdate)
        assert initial == stale
        assert publications.published_at(initial) == 0
        observed = _latest_available_health(receiver, initial)
        assert observed == latest
        assert publications.published_at(observed) == 10
        with pytest.raises(TimeoutError):
            receiver.receive(timeout=0.0)
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize('phase', ['write-hold', 'provider-query'])
def test_supervisor_unconfirmed_source_phase_uses_heartbeat_fallback(
    stall_detectors: StallDetectors,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: Literal['write-hold', 'provider-query'],
) -> None:
    stall_detectors.enable('heartbeat', 'phase', 'coordination')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    config = replace(
        config, daemon=replace(config.daemon, health_interval_secs=6, checkpoint_interval_secs=60)
    )
    clock = FakeClock()
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(w for w in factory.instances.values() if w.spec.sources)
        for worker in factory.instances.values():
            detail = replace(
                _idle_source_health(source, source='admin', heartbeat_at=0),
                phase=phase,
                deadline_at=5,
            )
            worker.messages.append(
                health_update(
                    worker, heartbeat_at=0, sources=(detail,) if worker is source else None
                )
            )
        supervisor._tick()
        waits: list[float] = []
        clock.advance(5.04)

        def wait(seconds: float) -> None:
            waits.append(seconds)
            clock.advance(seconds)
            for worker in factory.instances.values():
                if worker is not source:
                    worker.messages.append(health_update(worker, heartbeat_at=clock()))

        supervisor.run(
            should_stop=lambda: len(waits) == 2 or source.killed,
            wait=wait,
            set_foreground_progress=lambda _callback: None,
        )
        assert not source.killed
        assert supervisor.probe_result().readiness
        assert waits == [config.daemon.supervisor_poll_interval_secs] * 2
        accepted = _spawn_health(supervisor, source.worker_id)
        projection = supervisor._refresh_component_health(accepted, 29.999).sources[0]
        assert projection.deadline_at == 5
        assert projection.heartbeat_age == 29.999
        assert projection.overdue_reason is None
        assert supervisor._worker_health_failure(source.worker_id, accepted, 29.999) is None
        assert (
            supervisor._worker_health_failure(source.worker_id, accepted, 30)
            == 'worker-heartbeat-overdue'
        )
        clock.advance(30 - clock())
        monkeypatch.setattr(source, 'wait', _never_reap)
        supervisor._tick()
        assert source.killed
        assert (
            supervisor._spawns[source.worker_id].termination_reason == 'worker-heartbeat-overdue'
        )
        assert not supervisor._park_evidence_by_group


def test_ready_behind_a_late_finished_frame_completes_startup(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path)
    (tmp_path / 'hold-server').touch()
    with running_supervisor(
        config, ChildPatches(control_probe=True, lifecycle_probe=True, last_event_id=4)
    ) as probe:
        worker = _first_lifecycle_worker(probe)
        (tmp_path / f'{worker.spawn_instance}.large').touch()
        pause = tmp_path / f'{worker.spawn_instance}.pause-after-frame'
        pause.touch()
        _assign_lifecycle_source(probe, worker)
        _wait_lifecycle_file(tmp_path / f'{worker.spawn_instance}.frame-started', worker=worker)
        (tmp_path / 'hold-server').unlink()
        lifecycle = probe.supervisor._spawns[worker.worker_id].lifecycle
        assert lifecycle is not None
        sample = next(
            event
            for event in reversed(_control_events(tmp_path, worker))
            if event['kind'] == 'sample' and event['large']
        )
        startup_deadline = lifecycle.deadline_at
        lifecycle.deadline_at = cast('float', sample['sample']) + 0.001
        real_clock = probe.supervisor._clock
        after_deadline = lifecycle.deadline_at + 0.001
        probe.supervisor._clock = lambda: max(real_clock(), after_deadline)
        try:
            while not (tmp_path / f'{worker.spawn_instance}.frame-finished').exists():
                probe.supervisor._advance_worker_messages()
                _assert_no_lifecycle_charge(probe)
                time.sleep(0.01)
            _wait_lifecycle_file(
                tmp_path / f'{worker.spawn_instance}.frame-finished', worker=worker
            )
            probe.supervisor._advance_worker_messages()
            health = probe.latest_health[worker.worker_id]
            probe.supervisor._advance_worker_messages()  # Empty read while ready remains queued.
            assert (
                probe.latest_health[worker.worker_id].worker.heartbeat_at
                == health.worker.heartbeat_at
            )
            _assert_no_lifecycle_charge(probe)
        finally:
            probe.supervisor._clock = real_clock
            # The overdue marker belongs only to the paused-frame observation.
            lifecycle.deadline_at = startup_deadline
        pause.unlink()
        _ready_control_probe(probe)
        _assert_no_lifecycle_charge(probe)


def test_reconciliation_open_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    source_path, _group_path = prepare_absent_orphan(config)
    supervisor = Supervisor(config=config, config_path=config_path)
    with (
        closing(sqlite3.connect(source_path, isolation_level=None)) as holder,
        ExitStack() as stack,
    ):
        holder.execute('BEGIN IMMEDIATE')
        original = supervisor._source_writes

        def one_attempt(site: str, source: str) -> write_lock_module.WriteContext:
            return replace(original(site, source), policy='one-attempt')

        monkeypatch.setattr(supervisor, '_source_writes', one_attempt)
        sources = supervisor._open_reconciliation_sources(stack, {('test', 'admin')})
        assert not sources[('test', 'admin')].stamped
        holder.rollback()


def test_reconciliation_refuses_an_uninitialized_source_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    source_path, _group_path = prepare_absent_orphan(config)
    with closing(sqlite3.connect(source_path)) as connection, connection:
        connection.execute('DELETE FROM source_meta')
    supervisor = Supervisor(config=config, config_path=config_path)
    with ExitStack() as stack, pytest.raises(SourceIdentityError) as raised:
        supervisor._open_reconciliation_sources(stack, {('test', 'admin')})
    assert str(source_path) in str(raised.value)
    assert 'source-database-uninitialized' in str(raised.value)


def test_recovery_timeout_retains_evidence_and_counts_an_unattributed_failure(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('startup')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
    _create_recovery_group_database(config)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        clock.advance(config.daemon.worker_startup_deadline_secs)
        source.messages.append(health_update(source, heartbeat_at=clock()))

        supervisor._tick()

        assert dependent.killed
        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == (evidence,)
            policy = config.sites['test'].workers['pool']
            breaker = state.restore_worker_breaker(
                worker_id=dependent.worker_id,
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    spawned = False
    failed = False

    def clock() -> float:
        nonlocal failed
        if spawned and not failed:
            failed = True
            raise RuntimeError('registration clock failed')
        return 0.0

    def spawn(**options: Unpack[_ScriptedWorkerOptions]) -> ScriptedWorker:
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

        worker = next(iter(factory.instances.values()))
        assert worker.close_calls == 1
        assert sum(operation.startswith('shutdown:') for operation in factory.operations) == 1
        assert not supervisor._spawns


def test_removed_group_retires_active_event_into_replay_evidence(tmp_path: Path) -> None:
    with (
        _lifecycle_source(tmp_path / 'source.db') as source,
        _lifecycle_group(tmp_path / 'source.db') as group,
    ):
        source.initialize(500, incarnation='incarnation-1')
        before = group.snapshot()
        active = commit_step(
            group,
            StartEventStep(
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
                None,
            ),
            expected_revision=before.revision,
        )
        connection = group._handoff.connection
        connection.execute(
            'CREATE TRIGGER fail_orphan_audit BEFORE INSERT ON audit '
            "WHEN NEW.command = 'orphan' BEGIN SELECT RAISE(ABORT, 'orphan audit failed'); END"
        )
        with pytest.raises(DatabaseError, match='orphan audit failed'):
            Supervisor._orphan_group(source, group)
        assert group.snapshot() == active
        assert source.snapshot().replay_markers == ()
        connection.execute('DROP TRIGGER fail_orphan_audit')
        audit_before = group.audit_count()
        marker = Supervisor._orphan_group(source, group)
        orphaned = group.snapshot()
        assert marker.from_event_id == 101
        assert orphaned.cursor == before.cursor
        assert orphaned.in_flight_event_id is None
        assert orphaned.in_flight_obligation_id is None
        assert orphaned.manifest is None
        assert orphaned.replay_required_from == 101
        assert orphaned.revision == active.revision + 1
        assert Supervisor._orphan_group(source, group) == marker
        assert group.snapshot() == orphaned
        assert group.audit_count() == audit_before + 1


def test_repeated_startup_readd_uses_inventory_high_water_for_each_transition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    path, _ = prepare_absent_orphan(config)
    omitted = replace(config, sites={'test': replace(config.sites['test'], groups={})})
    for expected, current_config in ((2, config), (2, omitted), (3, config)):
        with Supervisor(config=current_config, config_path=config_path) as supervisor:
            supervisor.reconcile_durable_state()
            assert supervisor._state is not None
            assert supervisor._state.generation_high_water('test', 'remote') == expected
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


def test_replacement_wait_for_an_exempt_lifecycle_does_not_spin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _dispatch_wait_harness(tmp_path, monkeypatch) as (supervisor, factory, clock, _command):
        dependent = next(
            worker for worker in factory.instances.values() if 'remote' in worker.spec.groups
        )
        factory.authentication_ready = False
        supervisor._replace_worker(dependent.worker_id)
        dependent = factory.instances[dependent.worker_id]
        lifecycle = _spawn_lifecycle(supervisor, dependent.worker_id)
        lifecycle.authenticated = True
        lifecycle.grant_sent = False
        lifecycle.assigned = False
        lifecycle.deadline_at = -1
        sleeps: list[float] = []

        def ids(*_args: object) -> tuple[tuple[str, ...], tuple[str, ...]]:
            return (), (dependent.worker_id,)

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            lifecycle.ready = True
            clock.advance(seconds)

        def skip(*_args: object) -> None:
            return None

        monkeypatch.setattr(supervisor, '_source_adoption_restart_ids', ids)
        monkeypatch.setattr(supervisor, '_shutdown_worker', skip)
        monkeypatch.setattr(supervisor, '_discard_worker_lifecycle', skip)
        monkeypatch.setattr(supervisor, '_spawn_dependent', skip)
        monkeypatch.setattr(supervisor, '_advance_worker_messages', lambda: None)
        monkeypatch.setattr(time, 'sleep', sleep)
        result = CommandResult(
            command='restart-event', ok=True, message='done', request_id='test', result={}
        )
        assert supervisor._restart_source_owners('test', 'admin', (), result).ok
        assert sleeps
        assert all(0 < seconds <= 0.05 for seconds in sleeps)


def test_reserved_generation_is_not_active_before_group_full_commit(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    _prepare_readd_inventory(state_dir)
    with _SupervisorState.open(state_dir) as state:
        assignments = state.reconcile_groups(
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
            state.reconcile_groups(
                configured={('test', 'remote'): 'admin'}, generations={('test', 'remote'): 3}
            )


def test_restart_with_pending_park_under_an_outside_hold_becomes_ready(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path, remote=True)
    with running_supervisor(
        config, ChildPatches(lifecycle_probe=True, last_event_id=4, gate_events=True)
    ) as probe:
        workers = _ready_control_probe(probe)
        dependent = next(worker for worker in workers if not worker.spec.sources)
        supervisor = probe.supervisor
        probe.wait_for(
            lambda: any(
                group.join_phase == 'attached'
                for health in probe.latest_health.values()
                for group in health.groups
            ),
        )
        records = probe.records('remote')
        state = supervisor._state
        assert state is not None
        database = source_database(supervisor._config.daemon.state_dir)
        with closing(sqlite3.connect(database, isolation_level=None)) as writer:
            row = writer.execute(
                'SELECT group_generation, revision, cursor, parked FROM groups '
                'JOIN group_state USING (group_name) WHERE group_name = ?',
                ('remote',),
            ).fetchone()
            assert row[1] > 0
            evidence = _park_evidence(
                failed_worker_id=dependent.worker_id,
                failed_spawn_instance=dependent.spawn_instance,
                group_generation=row[0],
                group_revision=row[1] - 1,
                event_id=1,
            )
            assert state.record_park_evidence(evidence)
            supervisor._park_evidence_by_group[('test', 'remote')] = evidence
            (tmp_path / 'hold-recovery').touch()
            supervisor._replace_worker(dependent.worker_id)
            probe.wait_for((tmp_path / 'recovery-entered').exists)
            writer.execute('BEGIN IMMEDIATE')
            (tmp_path / 'hold-recovery').unlink()
            try:
                replacement = supervisor._spawns[dependent.worker_id]
                before = len(probe.terminations)
                probe.wait_for(
                    lambda: any(
                        'write-lock-stalled' in group.conditions
                        for health in probe.latest_health.values()
                        for group in health.groups
                    )
                )
                _assert_write_wait_policy(probe)
                assert len(probe.terminations) == before, probe.failure_details()
                assert replacement.lifecycle is None
                assert probe.park_evidence
                group = next(
                    group
                    for health in probe.latest_health.values()
                    for group in health.groups
                    if group.group == 'remote'
                )
                assert group.cursor == row[2]
                assert group.parked is bool(row[3])
                assert group.join_phase == ('parked' if row[3] else 'joining')
                assert probe.records('remote') == records
                assert writer.execute(
                    'SELECT COUNT(*) FROM conditional_park_results WHERE evidence_id = ?',
                    (evidence.directive.evidence_id,),
                ).fetchone() == (0,)
                assert any(
                    'write-lock-stalled' in group.conditions
                    for health in probe.latest_health.values()
                    for group in health.groups
                )
            finally:
                writer.rollback()
        probe.wait_for(lambda: not probe.park_evidence)
        assert state.pending_park_evidence() == ()
        with closing(sqlite3.connect(database)) as reader:
            assert reader.execute(
                'SELECT outcome FROM conditional_park_results WHERE evidence_id = ?',
                (evidence.directive.evidence_id,),
            ).fetchone() == ('stale',)
        assert not probe.breakers[dependent.worker_id]._failure_times


def test_resumed_group_under_an_outside_hold_waits_across_a_restart(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path)
    patches = ChildPatches(lifecycle_probe=True, last_event_id=4)
    with running_supervisor(config, patches) as first:
        first.wait_for(lambda: bool(first.supervisor.probe_mapping()['readiness']))
        database = source_database(first.supervisor._config.daemon.state_dir)
    with closing(sqlite3.connect(database, isolation_level=None)) as writer:
        writer.execute('BEGIN IMMEDIATE')
        with running_supervisor(config, patches) as probe:
            probe.wait_for(lambda: bool(probe.latest_health))
            probe.wait_for(
                lambda: (
                    'write-lock-stalled'
                    in cast('tuple[str, ...]', probe.supervisor.probe_mapping()['conditions'])
                )
            )
            _assert_write_wait_policy(probe)
            _assert_no_lifecycle_charge(probe)
            status = probe.supervisor.probe_mapping()
            assert not status['readiness']
            assert not status['complete']
            assert {'stale', 'write-lock-stalled'} <= set(
                cast('tuple[str, ...]', status['conditions'])
            )
            writer.rollback()
            probe.wait_for(lambda: bool(probe.supervisor.probe_mapping()['readiness']))
            assert probe.supervisor.probe_mapping()['complete']
            _assert_no_lifecycle_charge(probe)


def test_retired_dependent_ignores_a_late_grant_reply(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path, remote=True)
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        source, dependent = _grant_during_source_page(probe, tmp_path)
        probe.supervisor._shutdown_worker(dependent, 'retired-test')
        probe.supervisor._discard_worker_lifecycle(dependent)
        assert probe.supervisor._stale_grant_readies
        before = len(probe.terminations)
        (tmp_path / 'hold-page').unlink()
        probe.wait_for(
            lambda: not probe.supervisor._stale_grant_readies,
            expected_exits=(dependent.spawn_instance,),
        )
        assert len(probe.terminations) == before
        assert source._process.poll() is None
        assert not probe.breakers[dependent.worker_id]._failure_times


def test_shutdown_clears_every_per_worker_collection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    with Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(iter(factory.instances.values()))
        supervisor._spawns[worker.worker_id].attributed_park_failure = True
        close = worker.close

        def fail_close() -> None:
            close()
            raise OSError('worker close failed')

        monkeypatch.setattr(worker, 'close', fail_close)

        with pytest.raises(OSError, match='worker close failed'):
            supervisor._shutdown_workers('test shutdown')

        assert worker.close_calls == 1
        assert not supervisor._spawns


def test_slow_replacement_fails_no_send(tmp_path: Path) -> None:
    config = _control_probe_config(tmp_path, ('first', 'second'))
    with running_supervisor(config, ChildPatches(control_probe=True)) as probe:
        workers = _ready_control_probe(probe)
        unrelated = next(item for item in workers if item.spec.sources)
        (tmp_path / f'{unrelated.spawn_instance}.large').touch()
        _wait_control_event(tmp_path, unrelated, 'frame-started')
        for worker in workers:
            if worker is not unrelated:
                assert worker.shutdown('slow-replacement') == 0
        assert unrelated._process.poll() is None, probe.failure_details()
        resumed = time.monotonic()
        probe.wait_for(
            lambda: probe.latest_health[unrelated.worker_id].worker.heartbeat_at >= resumed,
            expected_exits=[
                worker.spawn_instance for worker in workers if worker is not unrelated
            ],
        )
        assert not any(item.worker_id == unrelated.worker_id for item in probe.terminations)
        assert probe.breakers[unrelated.worker_id]._last_failure_at is None


def test_source_adoption_unknown_target_names_option(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(config=config, config_path=config_path)
    command = _source_adoption_command('unknown-source')
    command = replace(command, arguments={**command.arguments, 'source': 'missing'})

    with pytest.raises(ValueError, match=r'source adoption source test/missing.*check --source'):
        supervisor._source_adoption_target(command)


def test_source_assignment_restarts_its_clock(tmp_path: Path) -> None:
    config = _lifecycle_probe_config(tmp_path)
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        worker = _first_lifecycle_worker(probe)
        lifecycle = probe.supervisor._spawns[worker.worker_id].lifecycle
        assert lifecycle is not None
        original = lifecycle.deadline_at
        _wait_lifecycle_file(tmp_path / f'{worker.spawn_instance}.hello-sent', worker=worker)
        before = max(probe.supervisor._clock(), original + 1)
        original_clock = probe.supervisor._clock
        probe.supervisor._clock = lambda: before
        try:
            probe.supervisor._advance_worker_messages()
        finally:
            probe.supervisor._clock = original_clock
        assert lifecycle.assigned
        assert lifecycle.deadline_at == (
            before + probe.supervisor._config.daemon.worker_startup_deadline_secs
        )
        assert lifecycle.deadline_at > original


def test_source_endpoint_readiness_admits_remote_dependent_during_local_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = _local_and_remote_config(tmp_path, monkeypatch)
    evidence = _park_evidence(group='local')
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
    _create_recovery_group_database(config, group='local')
    _create_recovery_group_database(config, group='remote')
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        source.authentication_ready = True
        supervisor._tick()
        remote = next(worker for worker in factory.instances.values() if not worker.spec.sources)
        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == (evidence,)
        assert source.assignment is not None
        local = next(item for item in source.assignment.group_assignments if item.group == 'local')
        assert isinstance(local, DeliveryGroupAssignment)
        assert local.conditional_park == evidence.directive
        source.messages_before_grant.append(
            health_update(
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
        remote.authentication_ready = True

        supervisor._tick()
        supervisor._tick()

        assert not source.killed
        assert not remote.killed
        assert remote.assignment is not None
        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == (evidence,)

        source.messages.append(_recovery_ready(source, evidence))
        supervisor._tick()

        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == ()


def test_source_held_before_its_server_is_contained_on_the_first_late_frame(
    stall_detectors: StallDetectors,
    tmp_path: Path,
) -> None:
    config = _lifecycle_probe_config(tmp_path)
    (tmp_path / 'hold-server').touch()
    with running_supervisor(
        config, ChildPatches(control_probe=True, lifecycle_probe=True, last_event_id=4)
    ) as probe:
        worker = _first_lifecycle_worker(probe)
        _assign_lifecycle_source(probe, worker)
        lifecycle = probe.supervisor._spawns[worker.worker_id].lifecycle
        assert lifecycle is not None
        probe.wait_for(lambda: worker.worker_id in probe.latest_health)
        accepted = probe.latest_health[worker.worker_id]
        with stall_detectors.enabled('startup'):
            lifecycle.deadline_at = accepted.worker.heartbeat_at
            probe.supervisor._advance_worker_messages()
            assert probe.terminations[0].reason == 'worker-startup-timeout'
            health = probe.terminations[0].health_before_containment
            assert health is not None
            assert health.worker.heartbeat_at >= lifecycle.deadline_at


def test_source_killed_with_a_grant_in_flight_restarts_the_dependent_uncounted(
    tmp_path: Path,
) -> None:
    config = _lifecycle_probe_config(tmp_path, remote=True)
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        source, dependent = _grant_during_source_page(probe, tmp_path)
        source.kill()
        (tmp_path / 'hold-page').unlink()
        probe.wait_for(
            lambda: any(
                item.spawn_instance == dependent.spawn_instance for item in probe.terminations
            ),
            expected_exits=(source.spawn_instance, dependent.spawn_instance),
        )
        reason = next(
            item.reason
            for item in probe.terminations
            if item.spawn_instance == dependent.spawn_instance
        )
        assert reason == 'source-owner-unavailable'
        assert not probe.breakers[dependent.worker_id]._failure_times
        probe.wait_for(
            lambda: (
                dependent.worker_id in probe.supervisor._spawns
                and probe.supervisor._spawns[dependent.worker_id].worker is not dependent
            ),
            expected_exits=(source.spawn_instance, dependent.spawn_instance),
        )
        _ready_control_probe(probe)
        assert not probe.breakers[dependent.worker_id]._failure_times


@pytest.mark.parametrize(
    'order', ['confirmed-healthy', 'unconfirmed-healthy', 'healthy-confirmed', 'confirmed-sliding']
)
def test_supervisor_phase_batch_uses_latest_observation(
    stall_detectors: StallDetectors, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, order: str
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    clock = FakeClock()
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=0))
        supervisor._tick()
        remote = next(w for w in factory.instances.values() if not w.spec.sources)
        clock.advance(10)
        group = replace(_in_flight_group_health(remote, heartbeat_at=0), deadline_at=10)
        expired = health_update(
            remote, heartbeat_at=9.99 if order == 'unconfirmed-healthy' else 10, groups=(group,)
        )
        healthy = health_update(remote, heartbeat_at=10)
        if order == 'confirmed-sliding':
            healthy = health_update(
                remote, heartbeat_at=10, groups=(replace(group, heartbeat_at=10, deadline_at=130),)
            )
        remote.messages.extend(
            (healthy, expired) if order == 'healthy-confirmed' else (expired, healthy)
        )
        for worker in factory.instances.values():
            if worker is not remote:
                worker.messages.append(health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        assert remote.killed == (order == 'healthy-confirmed')
        if not remote.killed:
            assert supervisor._breakers[remote.worker_id]._failure_total == 0
            assert _spawn_health(supervisor, remote.worker_id).groups[0].overdue_reason is None


@pytest.mark.parametrize('kind', ['source', 'lane', 'operator'])
def test_supervisor_component_confirmation_and_projection_share_equality(
    stall_detectors: StallDetectors,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: Literal['source', 'lane', 'operator'],
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    clock = FakeClock()
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(
            w for w in factory.instances.values() if bool(w.spec.sources) == (kind != 'lane')
        )
        clock.advance(10.1)
        health = health_update(worker, heartbeat_at=9.999)
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
        update = _spawn_health(supervisor, worker.worker_id)
        assert supervisor._worker_health_failure(worker.worker_id, update, clock()) is None
        assert all(item.overdue_reason is None for item in (*update.sources, *update.groups))
        assert supervisor._record_worker_health(
            worker, replace(health, worker=replace(health.worker, heartbeat_at=10)), clock()
        )
        update = _spawn_health(supervisor, worker.worker_id)
        assert (
            supervisor._worker_health_failure(worker.worker_id, update, clock())
            == f'{kind}-phase-overdue'
        )
        projections = (
            update.sources if kind == 'source' else update.groups if kind == 'lane' else ()
        )
        assert all(item.overdue_reason == f'{kind}-phase-overdue' for item in projections)


def test_start_accepts_long_source_with_private_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, short_root: Path
) -> None:
    config_path, config = _socket_preflight_config(tmp_path, monkeypatch, short_root, 71)
    monkeypatch.setattr(platform_module, '_unix_socket_path_limit', lambda: 107)
    state = config.daemon.state_dir
    assert len(os.fsencode(state / 'sites/test/sources/admin-events.sock')) >= 108
    runtime = Path(os.environ['XDG_RUNTIME_DIR'])
    assert runtime.stat().st_uid == os.geteuid()
    assert runtime.stat().st_mode & 0o777 == 0o700
    before_runtime = set(runtime.rglob('*'))
    candidates = supervisor_module._preflight_socket_paths(config)
    assert len(candidates) == 2
    assert candidates[1].parent == runtime / f'sg-event-daemon-{os.geteuid()}'
    factory = ScriptedWorkerFactory()
    with Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        assert factory.instances
        assert not supervisor._restart_deadlines
        assert all(breaker._failure_total == 0 for breaker in supervisor._breakers.values())
        assert set(runtime.rglob('*')) == before_runtime


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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    prepare_absent_orphan(config)
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


def test_startup_error_is_not_replaced_by_shutdown_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    startup_error = ValueError('startup lifecycle failed')
    supervisor = Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=iter(('source-generation-1', 'source-spawn-1')).__next__,
    )

    def fail_lifecycle_advance() -> bool:
        source = next(iter(factory.instances.values()))
        source.shutdown_status = None
        raise startup_error

    monkeypatch.setattr(supervisor, '_advance_lifecycles', fail_lifecycle_advance)
    try:
        supervisor._open()
        supervisor._reconcile(generations={})

        with pytest.raises(ValueError, match='startup lifecycle failed') as caught:
            supervisor.start()

        source = next(iter(factory.instances.values()))
        assert caught.value is startup_error
        assert caught.value.__notes__ == [f'worker child remained unreaped: {source.worker_id}']
    finally:
        supervisor._close()


def test_startup_raises_the_limit_for_a_large_configuration(tmp_path: Path) -> None:
    groups = tuple(f'group{i}' for i in range(35))
    config_path = write_daemon_config(tmp_path, Topology('default', groups))
    result, log = _limited_startup(config_path, hard=4096)
    assert 'error' not in result, result
    delivered = result['delivered']
    assert isinstance(delivered, dict)
    assert set(cast('dict[str, object]', delivered)) == set(groups)
    assert log.count('open-file limit 4096 (hard limit 4096)') == 1
    assert [json.loads(path.read_text()) for path in tmp_path.glob('*.limits')] == [[4096, 4096]]


def test_startup_readd_checks_incarnation_before_lifecycle_write(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    source_path, _ = prepare_absent_orphan(config)
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


def test_startup_refuses_a_configuration_over_the_hard_limit(tmp_path: Path) -> None:
    config_path = write_daemon_config(
        tmp_path, Topology('default', tuple(f'group{i}' for i in range(35)))
    )
    result, log = _limited_startup(config_path, hard=256)
    assert str(result.get('error', '')).startswith('ConfigError:'), result
    assert '["site","test"]' in str(result['error'])
    assert '296' in str(result['error'])
    assert '256' in str(result['error'])
    assert result['spawns'] == []
    assert result['authority_opens'] == []
    assert log.count('open-file limit 256 (hard limit 256)') == 1


def test_startup_refuses_when_only_the_supervisor_cannot_fit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
    config_path = write_daemon_config(tmp_path, Topology('default', ('small',)))
    site_text = config_path.read_text().split('[sites.test]', 1)[1]
    with config_path.open('a') as stream:
        for index in range(1, 12):
            stream.write(
                f'\n[sites.site{index}]' + site_text.replace('sites.test.', f'sites.site{index}.')
            )
    config = load_config(config_path)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        assert supervisor._state is not None
        supervisor._state.reconcile_groups(
            configured={('test', f'old{i}'): 'admin' for i in range(20)},
            generations={},
        )
        supervisor._state.reconcile_groups(configured={}, generations={})
    path = source_database(config.daemon.state_dir)
    for group in ('small', *(f'old{index}' for index in range(20))):
        with open_group_on_source(
            path,
            writes=write_context_for(path),
            site='test',
            site_server_url='https://example.invalid',
            source='admin',
            group=group,
            group_generation=1,
            ordering='eager',
            stream_fingerprint=worker_module.source_stream_fingerprint(
                config.sites['test'], config.sites['test'].sources['admin']
            ),
            source_generation='seed',
            verified_incarnation='incarnation',
            start_mode='initialize',
            initial_cursor=0,
            initial_delivery_floor=0,
            initial_last_delivered_id=0,
        ):
            pass
    result, log = _limited_startup(config_path, hard=256)
    assert str(result.get('error', '')).startswith('ConfigError:'), result
    assert 'supervisor' in str(result['error'])
    assert '283' in str(result['error'])
    assert '256' in str(result['error'])
    assert result['spawns'] == []
    assert result['authority_opens'] == []
    assert log.count('open-file limit 256 (hard limit 256)') == 1


def test_status_before_first_snapshot_is_stale_and_unready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        group_owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        source_owner.messages.append(
            health_update(
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
                        worker_instance=source_owner.spawn_instance,
                        initialized=True,
                        stream_incarnation='incarnation-1',
                        phase='wait',
                        heartbeat_at=clock(),
                        deadline_at=clock() + 1.25,
                    ),
                ),
            )
        )
        group_owner.messages.append(
            health_update(
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


def test_status_names_stopped_breaker_worker_and_restores_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    warning = Mock()
    monkeypatch.setattr(supervisor_module._LOGGER, 'warning', warning)
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    spec = build_worker_specs(config.sites['test'])[1]
    for restored in (False, True):
        with Supervisor(
            clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
        ) as supervisor:
            supervisor._reconcile(generations={})
            supervisor.start()
            if not restored:
                breaker = supervisor._breakers[spec.worker_id]
                for _ in range(config.sites['test'].workers['pool'].restart_limit):
                    breaker.record_failure('prior failure')
                dependent = factory.instances[spec.worker_id]
                dependent.exit_status = 1
                supervisor._tick()
                assert any('breaker open' in call.args[0] for call in warning.call_args_list)
            assert spec.worker_id not in supervisor._spawns
            status = supervisor.status_mapping({'site': 'test'})
            assert {
                'site': 'test',
                'worker_id': spec.worker_id,
                'breaker_open': True,
                'breaker_reason': 'worker-exit-1',
            } in cast('tuple[object, ...]', status['workers'])
            assert (
                supervisor_module._budget_status_payload(
                    status, site_filter='other', group_filter=None
                )['workers']
                == ()
            )
            assert (
                cast('dict[str, dict[str, int]]', status['projection_counts'])['workers']['total']
                >= 1
            )


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
    config_path, config = source_only_config(tmp_path, monkeypatch)
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
            worker = ScriptedWorker(
                template_spec,
                update.worker.spawn_instance,
                [],
                None,
                authentication_ready=True,
            )
            supervisor._spawns[update.worker.worker_id] = supervisor_module._WorkerSpawn(
                worker=worker,
                health_deadline=101.0,
                pending_groups=frozenset(),
                lifecycle=None,
                health=update,
            )

    with _SupervisorState.open(tmp_path / 'status-state') as state:
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


def test_stop_during_a_partly_written_frame_ends_without_a_final_frame(tmp_path: Path) -> None:
    config = _control_probe_config(tmp_path, ())
    with running_supervisor(config, ChildPatches(control_probe=True)) as probe:
        worker = _ready_control_probe(probe)[0]
        (tmp_path / f'{worker.spawn_instance}.large').touch()
        _wait_control_event(tmp_path, worker, 'frame-started')
        queued = time.monotonic()
        worker.send(StopCommand(reason='partial-stop'))
        assert worker._process.wait() == 0, probe.failure_details()
        stopped = _wait_control_event(tmp_path, worker, 'stop')
        assert stopped['reason'] == 'partial-stop'
        # EOF with an unfinished frame is valid. The retained wire prefix
        # must still end inside its padding, without an embedded final frame.
        messages: list[ControlMessage] = []
        with suppress(EOFError):
            messages.extend(worker.receive_available())
        assert not any(
            isinstance(message, HealthUpdate) and message.worker.heartbeat_at >= queued
            for message in messages
        )
        _assert_abandoned_health_prefix(bytes(worker._control._reader._buffer))
        log = probe.spawns[0].log_path.read_text()
        assert 'worker-control-send-timeout' not in log


@pytest.mark.parametrize('failure', ['first-source', 'second-site', 'control'])
def test_start_preflights_all_socket_candidates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, short_root: Path, failure: str
) -> None:
    config_path, config = _socket_preflight_config(
        tmp_path, monkeypatch, short_root, 108 if failure == 'control' else 71
    )
    source_bytes = len(
        os.fsencode(config.daemon.state_dir / 'sites/test/sources/admin-events.sock')
    )
    limit = 107 if failure == 'control' else source_bytes - 1
    monkeypatch.setattr(platform_module, '_unix_socket_path_limit', lambda: limit)
    monkeypatch.delenv('XDG_RUNTIME_DIR')
    unsafe_root = tmp_path / 'unsafe-runtime'
    unsafe_root.mkdir(mode=0o755)
    monkeypatch.setattr(platform_module.tempfile, 'gettempdir', lambda: str(unsafe_root))
    if failure == 'second-site':
        first = replace(config.sites['test'], name='a')
        second = replace(config.sites['test'], name='later')
        config = replace(config, sites={'a': first, 'later': second})
    factory = ScriptedWorkerFactory()
    with Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        before_state_paths = set(config.daemon.state_dir.rglob('*'))
        before_paths = set(tmp_path.rglob('*'))
        before_breakers = dict(supervisor._breakers)
        before_deadlines = dict(supervisor._restart_deadlines)
        assert supervisor._state is not None
        connection = supervisor._state._database.connection
        before_rows = connection.execute('SELECT * FROM worker_breaker').fetchall()
        error = None
        try:
            supervisor.start()
        except ConfigError as caught:
            error = caught
        assert isinstance(error, ConfigError), factory.operations
        assert 'shorter state path' in str(error)
        assert 'XDG_RUNTIME_DIR' in str(error)
        assert (
            'control' if failure == 'control' else 'later' if failure == 'second-site' else 'test'
        ) in str(error)
        assert not factory.operations
        assert supervisor._breakers == before_breakers
        assert supervisor._restart_deadlines == before_deadlines
        assert connection.execute('SELECT * FROM worker_breaker').fetchall() == before_rows
        assert set(tmp_path.rglob('*')) == before_paths
        assert set(config.daemon.state_dir.rglob('*')) == before_state_paths
        assert unsafe_root.stat().st_mode & 0o777 == 0o755


def test_supervisor_absent_orphan_terminal_retry_preserves_newer_reservation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _absent_orphan_config(tmp_path, monkeypatch)
    source_path, group_path = prepare_absent_orphan(config)
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=ScriptedWorkerFactory(),
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
        reserved = state.group_inventory_record(site='test', group='remote')
        assert reserved is not None
        assert (reserved.orphan_generation, reserved.generation) == (2, 3)

        recorded = supervisor.dispatch_group_disposition_command(
            replace(command, identity=replace(command.identity, pid=789)), timeout=1.0
        )

        assert recorded.ok
        assert recorded.message == 'recorded'
        assert recorded.result['group_generation'] == 2
        assert state.group_inventory_record(site='test', group='remote') == reserved
        completed = supervisor.dispatch_group_disposition_command(replacement, timeout=1.0)
        assert completed.ok
        assert completed.result['group_generation'] == 3
        with pytest.raises((InvalidTransitionError, ValueError), match='disposition'):
            supervisor.dispatch_group_disposition_command(
                replace(command, identity=replace(command.identity, pid=789)), timeout=1.0
            )
        inventory = state.group_inventory_record(site='test', group='remote')
        assert inventory is not None
        assert (inventory.orphan_generation, inventory.generation) == (None, 3)
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


@contextmanager
def _dispatch_wait_harness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[tuple[Supervisor, ScriptedWorkerFactory, FakeClock, OperatorCommand]]:
    config_path, config = _two_dependent_config(tmp_path, monkeypatch)
    config = replace(
        config,
        daemon=replace(
            config.daemon,
            supervisor_poll_interval_secs=0.05,
            worker_startup_deadline_secs=0.2,
        ),
    )
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    monkeypatch.setattr(time, 'monotonic', clock)
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        command = OperatorCommand(
            request_id='dispatch-wait',
            command='restart-event',
            arguments={'site': 'test', 'source': 'admin'},
            identity=AuditIdentity(local_uid=501, username='operator', pid=456),
        )
        yield supervisor, factory, clock, command


def test_supervisor_active_dependent_grant_requires_current_source_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    token_index = 0

    def token() -> str:
        nonlocal token_index
        token_index += 1
        return f'active-owner-{token_index}'

    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=token,
    ) as supervisor:
        _source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        lifecycle = _spawn_lifecycle(supervisor, dependent.worker_id)
        assert vars(lifecycle)['grant_sent']
        assert dependent.assignment is None
        source_workers = cast(
            'dict[tuple[str, str], ScriptedWorker]',
            vars(supervisor)['_source_workers'],
        )
        source_workers.pop(('test', 'admin'))

        with pytest.raises(
            RuntimeError,
            match='active dependent grant source owner is unavailable',
        ):
            supervisor._tick()

        assert not dependent.killed
        assert dependent.assignment is None


def test_supervisor_advances_startups_and_checkpoints_independently(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('startup')
    config_path, config = _two_dependent_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
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
        **options: Unpack[_CheckpointOptions],
    ) -> Checkpointer:
        def run_checkpoint() -> tuple[int, int, int]:
            checkpoint(connection)
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            **options,
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        source.authentication_ready = True
        supervisor._tick()
        dependents = sorted(
            (worker for worker in factory.instances.values() if not worker.spec.sources),
            key=lambda worker: worker.worker_id,
        )
        assert len(dependents) == 2
        dependents[0].authentication_ready = True
        clock.advance(config.daemon.checkpoint_interval_secs)

        supervisor._tick()

        assert dependents[0].assignment is not None
        assert dependents[1].assignment is None
        assert len(checkpoint_calls) == 1
        assert all(
            timeout == 0.0
            for worker in factory.instances.values()
            for timeout in worker.receive_timeouts
        )

        clock.advance(config.daemon.worker_startup_deadline_secs)
        source.messages.append(health_update(source, heartbeat_at=clock()))
        dependents[0].messages.append(health_update(dependents[0], heartbeat_at=clock()))
        supervisor._tick()

        assert not dependents[0].killed
        assert dependents[1].killed
    finally:
        supervisor._close()


def test_supervisor_ambiguous_overdue_worker_creates_no_evidence(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = _two_group_shared_worker_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()

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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        supervisor._tick()
        assert dependent.assignment is not None
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
        for worker in factory.instances.values():
            worker.messages.append(
                health_update(
                    worker,
                    heartbeat_at=clock(),
                    groups=(remote, second) if worker is dependent else None,
                )
            )
        supervisor._tick()

        clock.advance(config.sites['test'].groups['remote'].callback_deadline_secs + 0.001)
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        source.messages.append(health_update(source, heartbeat_at=clock()))
        dependent.messages.append(
            health_update(dependent, heartbeat_at=clock(), groups=(remote, second))
        )
        supervisor._tick()

        assert dependent.killed
        assert observed_containment == [('lane-phase-overdue', {'remote', 'second'})]
        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == ()
            policy = config.sites['test'].workers['pool']
            restored = state.restore_worker_breaker(
                worker_id=dependent.worker_id,
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
        source.messages.append(health_update(source, heartbeat_at=clock()))
        for _ in range(4):
            supervisor._tick()
            replacement = factory.instances[dependent.worker_id]
            if replacement is not dependent and replacement.assignment is not None:
                break
        else:
            pytest.fail('ambiguous worker replacement did not become ready')
        assert all(
            isinstance(assignment, DeliveryGroupAssignment) and assignment.conditional_park is None
            for assignment in replacement.assignment.group_assignments
        )


def test_supervisor_batch_verdict_uses_time_after_consumption(
    stall_detectors: StallDetectors, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    stall_detectors.enable('heartbeat')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(item for item in factory.instances.values() if item.spec.sources)
        worker.messages.append(health_update(worker, heartbeat_at=0))
        acquire = worker.receive_available

        def delayed_batch() -> Iterator[ControlMessage]:
            batch = acquire()

            def messages() -> Iterator[ControlMessage]:
                yield from batch
                clock.advance(0.5)

            return messages()

        monkeypatch.setattr(worker, 'receive_available', delayed_batch)
        supervisor._tick()
        assert clock() == 0.5
        assert worker.killed
        assert supervisor._breakers[worker.worker_id]._failure_total == 1


@pytest.mark.parametrize('outcome', ['result', 'timeout', 'eof'])
def test_dispatch_reserved_channel_does_not_expire_queued_dependent(
    stall_detectors: StallDetectors, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    stall_detectors.enable('heartbeat', 'control')
    with _dispatch_wait_harness(tmp_path, monkeypatch) as (supervisor, factory, clock, command):
        supervisor._config = replace(
            supervisor._config, daemon=replace(supervisor._config.daemon, health_interval_secs=2.0)
        )
        owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if 'remote' in worker.spec.groups
        )
        factory.authentication_ready = False
        supervisor._replace_worker(dependent.worker_id)
        replacement = factory.instances[dependent.worker_id]
        lifecycle = _spawn_lifecycle(supervisor, dependent.worker_id)
        lifecycle.authenticated = True
        original_grants = tuple(owner.sent_grants)
        clock.now = 100
        for worker in factory.instances.values():
            if worker is not owner and worker is not replacement:
                worker.messages.append(health_update(worker, heartbeat_at=112))
        with _socket_worker_channel(owner) as (control, sender):
            sender.sendall(
                ipc_module.encode_control_message(health_update(owner, heartbeat_at=100))
            )
            acquire = owner.receive_available
            receive = owner.receive
            acquisitions = owner.batch_acquisitions
            reserved_reads = _delay_reserved_operator_batches(
                supervisor, owner, sender, monkeypatch, outcome=outcome
            )
            if outcome == 'result':
                assert supervisor.dispatch_operator_command(command, timeout=20).ok
                assert owner.batch_acquisitions == acquisitions + 2
                assert reserved_reads == [True, True]
            else:
                with pytest.raises(OperatorOutcomeUnknownError):
                    supervisor.dispatch_operator_command(
                        command, timeout=12 if outcome == 'timeout' else 20
                    )
                assert owner.batch_acquisitions == acquisitions + 1
                assert reserved_reads == [True]
            assert supervisor._dispatch_reserved is None
            if outcome == 'eof':
                assert owner.killed
                assert (
                    supervisor._spawns[owner.worker_id].termination_reason == 'worker-control-eof'
                )
            else:
                assert not owner.killed
                assert control.has_pending_input()
                monkeypatch.setattr(owner, 'receive_available', acquire)
                monkeypatch.setattr(owner, 'receive', receive)
                supervisor._advance_worker_messages()
                assert len(owner.sent_grants) == len(original_grants) + 1
                assert replacement.assignment is not None
                assert not replacement.killed
                assert not control.has_pending_input()
                assert control.last_arrival_at == 112


def test_supervisor_capture_persists_wall_clock_but_not_monotonic_identity(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('phase', 'coordination', 'heartbeat')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    monotonic_clock = FakeClock()
    wall_clock = FakeClock()
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        group_health = _in_flight_group_health(
            dependent,
            heartbeat_at=monotonic_clock(),
        )
        for worker in factory.instances.values():
            worker.messages.append(
                health_update(
                    worker,
                    heartbeat_at=monotonic_clock(),
                    groups=(group_health,) if worker is dependent else (),
                )
            )
        supervisor._tick()
        group_path = source_database(config.daemon.state_dir)
        observed_before_kill: list[_ParkEvidence] = []
        dependent.on_kill = lambda: observed_before_kill.extend(
            supervisor._park_evidence_by_group.values()
        )

        with _conditional_group(group_path):
            monotonic_clock.advance(
                config.sites['test'].groups['remote'].callback_deadline_secs + 0.001
            )
            source = next(worker for worker in factory.instances.values() if worker.spec.sources)
            source.messages.append(health_update(source, heartbeat_at=monotonic_clock()))
            dependent.messages.append(
                health_update(dependent, heartbeat_at=monotonic_clock(), groups=(group_health,))
            )
            supervisor._tick()

        assert dependent.killed
        original = _park_evidence(
            failed_worker_id=dependent.worker_id,
            observed_at_wall_clock=1000.0,
        )
        assert observed_before_kill == [original]
        assert original.directive.observed_at_wall_clock != monotonic_clock()
        assert len(factory.history[dependent.worker_id]) == 2
        assert factory.operations.index('kill:named') < max(
            index
            for index, operation in enumerate(factory.operations)
            if operation == 'spawn:named'
        )

        repacked = factory.instances[dependent.worker_id]
        repacked_health = _in_flight_group_health(
            repacked,
            heartbeat_at=monotonic_clock(),
        )
        for worker in factory.instances.values():
            worker.messages.append(
                health_update(
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
        source.messages.append(health_update(source, heartbeat_at=monotonic_clock()))
        supervisor._tick()

        assert repacked.killed
        assert supervisor._park_evidence_by_group == {('test', 'remote'): original}
        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == (original,)
    finally:
        supervisor._close()


def test_supervisor_checkpoint_database_error_halts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    clock = FakeClock()
    factory = ScriptedWorkerFactory()

    def checkpointer_factory(
        connection: sqlite3.Connection,
        path: Path,
        **options: Unpack[_CheckpointOptions],
    ) -> Checkpointer:
        return Checkpointer(
            connection,
            path,
            **options,
            run_checkpoint=lambda: (
                (_ for _ in ()).throw(DatabaseError('checkpoint corruption'))
                if options['clock']() >= 5 * options['interval_secs']
                else (0, 0, 0)
            ),
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
            assert not any(operation.startswith('shutdown:') for operation in factory.operations)

        clock.advance(config.daemon.checkpoint_interval_secs)
        with pytest.raises(DatabaseError, match='checkpoint corruption'):
            supervisor._tick()

        assert supervisor._halt_reason_value is not None
        assert factory.operations[-4:] == [
            'shutdown:named:supervisor-checkpoint-failure',
            'close:named',
            'shutdown:site:supervisor-checkpoint-failure',
            'close:site',
        ]
        with pytest.raises(RuntimeError, match='halted'):
            supervisor._tick()


def test_supervisor_checkpoint_outside_reader_warns_without_stopping_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    snapshots: list[_StatusSnapshot] = []

    def capture_aggregate(**options: Unpack[_AggregateStatusOptions]) -> _StatusSnapshot:
        snapshot = aggregate_status(**options)
        snapshots.append(snapshot)
        return snapshot

    monkeypatch.setattr(supervisor_module, 'aggregate_status', capture_aggregate)
    config_path, config = source_only_config(tmp_path, monkeypatch)
    clock = FakeClock()
    factory = ScriptedWorkerFactory()
    root = logging.getLogger('sg_event_daemon')
    monkeypatch.setattr(root, 'handlers', [*root.handlers, caplog.handler])
    monkeypatch.setattr(root, 'propagate', False)
    caplog.set_level(logging.INFO, logger='sg_event_daemon')
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        state = supervisor._state
        assert state is not None
        with closing(sqlite3.connect(state._database.path, isolation_level=None)) as reader:
            reader.execute('BEGIN')
            reader.execute('SELECT COUNT(*) FROM breaker_audit').fetchone()
            state._database.connection.execute('CREATE TABLE checkpoint_probe (value INTEGER)')
            for _ in range(6):
                clock.advance(config.daemon.checkpoint_interval_secs)
                assert supervisor._tick()
            status = supervisor.status_mapping({})
            assert 'checkpoint-stalled' in cast('tuple[str, ...]', status['conditions'])
            assert not status['readiness']
            assert 'checkpoint-stalled' in snapshots[-1].conditions
            assert not snapshots[-1].readiness
            assert not any(operation.startswith('shutdown:') for operation in factory.operations)
            assert (
                len([record for record in caplog.records if 'bound exceeded' in record.message])
                == 1
            )
            with _outside_supervisor_writer(state):
                state.persist_worker_breaker(_pending_test_breaker())
                supervisor._tick()
                conditions = cast('tuple[str, ...]', supervisor.status_mapping({})['conditions'])
                assert {'checkpoint-stalled', 'write-lock-stalled'} <= set(conditions)
                assert {'checkpoint-stalled', 'write-lock-stalled'} <= set(
                    cast('tuple[str, ...]', supervisor.probe_mapping()['conditions'])
                )
            reader.execute('COMMIT')
            clock.advance(config.daemon.checkpoint_interval_secs)
            assert supervisor._tick()
            assert 'checkpoint-stalled' not in cast(
                'tuple[str, ...]', supervisor.status_mapping({})['conditions']
            )
            assert 'checkpoint-stalled' not in snapshots[-1].conditions
            assert any('bound cleared' in record.message for record in caplog.records)


def test_supervisor_checkpoints_state_on_configured_monotonic_cadence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    clock = FakeClock()
    checkpoint_calls: list[object] = []

    def checkpoint(connection: object) -> bool:
        checkpoint_calls.append(connection)
        return True

    def checkpointer_factory(
        connection: sqlite3.Connection,
        path: Path,
        **options: Unpack[_CheckpointOptions],
    ) -> Checkpointer:
        def run_checkpoint() -> tuple[int, int, int]:
            checkpoint(connection)
            return 0, 0, 0

        return Checkpointer(
            connection,
            path,
            **options,
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
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
    source = next(worker for worker in factory.instances.values() if worker.spec.sources)
    dependent = next(worker for worker in factory.instances.values() if not worker.spec.sources)
    source.messages.append(health_update(source, heartbeat_at=0.0))
    dependent.messages.append(health_update(dependent, heartbeat_at=0.0))
    for _ in range(4):
        supervisor._tick()
        if {
            worker_id
            for worker_id, spawn in supervisor._spawns.items()
            if spawn.health_inventory_complete
        } == {
            source.worker_id,
            dependent.worker_id,
        }:
            break
    assert {
        worker_id
        for worker_id, spawn in supervisor._spawns.items()
        if spawn.health_inventory_complete
    } == {
        source.worker_id,
        dependent.worker_id,
    }
    dependent.shutdown_status = None
    later_discard_error = OSError('later worker discard failed')
    source_close = source.close

    def fail_source_close() -> None:
        source_close()
        raise later_discard_error

    monkeypatch.setattr(source, 'close', fail_source_close)
    supervisor._spawns[dependent.worker_id].health_deadline = 1.0
    vars(supervisor)['_restart_deadlines'][source.worker_id] = 2.0
    supervisor._spawns[source.worker_id].termination_reason = 'test-termination'
    supervisor._spawns[source.worker_id].health_failure = 'test-health-failure'
    supervisor._spawns[dependent.worker_id].health = health_update(
        dependent,
        heartbeat_at=0.0,
    )

    with pytest.raises(RuntimeError) as caught:
        supervisor._close()

    assert str(caught.value) == f'worker child remained unreaped: {dependent.worker_id}'
    assert sum(operation.startswith('shutdown:') for operation in factory.operations) == 2
    assert dependent.close_calls == 1
    assert source.close_calls == 1
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        clock=FakeClock(),
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
        cast('ScriptedWorker', spawn.worker)
        for spawn in reversed(tuple(supervisor._spawns.values()))
    )
    first_worker, later_worker = shutdown_order
    first_worker.shutdown_status = None
    later_worker.shutdown_status = None

    with pytest.raises(RuntimeError) as caught:
        supervisor._close()

    expected = f'worker child remained unreaped: {first_worker.worker_id}'
    assert str(caught.value) == expected
    assert supervisor._halt_reason_value == expected


def test_supervisor_close_continues_with_fresh_reconciliation_after_reopen(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(
        clock=FakeClock(),
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
        worker_factory=ScriptedWorkerFactory(),
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config, config_path=config_path, clock=FakeClock(), worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        workers = tuple(factory.instances.values())
        for worker in workers:
            worker.messages.append(health_update(worker, heartbeat_at=0))
        first = workers[0]
        later = health_update(first, heartbeat_at=0)
        receive = first.receive

        def replenish(*, timeout: float | None = None) -> ControlMessage:
            message = receive(timeout=timeout)
            first.messages.append(later)
            return message

        monkeypatch.setattr(first, 'receive', replenish)
        acquisitions = {worker.worker_id: worker.batch_acquisitions for worker in workers}
        supervisor._tick()
        assert tuple(first.messages) == (later,)
        assert not workers[1].messages
        assert all(
            worker.batch_acquisitions == acquisitions[worker.worker_id] + 1 for worker in workers
        )
        assert sum(spawn.health is not None for spawn in supervisor._spawns.values()) == len(
            workers
        )
        assert not any(worker.killed for worker in workers)


def test_supervisor_command_saturation_enters_worker_failure_lifecycle(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    monkeypatch.setenv('PYTHONPATH', os.fspath(Path(__file__).parents[1] / 'src'))
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
    spawner = _ControlChildSpawner(
        """
import os
import sys
import time
import threading
from tests._ipc_support import send_control
from sg_event_daemon._ipc import ControlHello, WorkerControl

control = WorkerControl.from_fd(
    int(sys.argv[1]),
    max_frame_bytes=8_388_608,
)
send_control(control,
    ControlHello(
        spawn_instance=sys.argv[2],
        worker_id=sys.argv[3],
        pid=os.getpid(),
    ),
)
threading.Event().wait()
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)
    with Supervisor(config=config, config_path=config_path) as supervisor, ExitStack() as cleanup:
        supervisor._reconcile(generations={})
        supervisor.start()
        spawn = next(iter(supervisor._spawns.values()))
        worker = cast('_WorkerInstance', spawn.worker)
        kill = worker.kill

        def kill_and_wait() -> None:
            kill()
            # Make exit observable before this same tick reaches its reap
            # phase, without relying on how promptly the OS schedules death.
            worker._process.wait()

        cleanup.callback(kill_and_wait)
        monkeypatch.setattr(worker, 'kill', kill_and_wait)
        assert _authenticate_worker(worker)
        control = cast('WorkerControl', vars(worker)['_control'])
        _saturate_worker_control_sender(control)

        clock = FakeClock()
        clock.now = time.monotonic()
        wait_ready = WorkerControl._wait_ready

        def expire(
            sock: socket.socket,
            event: int,
            deadline: float,
            *,
            allow_expired_ready: bool = False,
            timeout_message: str,
        ) -> None:
            clock.now = deadline
            wait_ready(
                sock,
                event,
                deadline,
                allow_expired_ready=allow_expired_ready,
                timeout_message=timeout_message,
            )

        with stall_detectors.enabled('control'), monkeypatch.context() as patch:
            patch.setattr(ipc_module, 'time', SimpleNamespace(monotonic=clock))
            patch.setattr(WorkerControl, '_wait_ready', staticmethod(expire))
            assert supervisor._tick()

        # Reaping removes the mapping entry, but not this exact spawn record.
        assert spawn.termination_reason == 'worker-control-send-timeout'
        assert worker.worker_id not in supervisor._spawns
        assert worker._process.wait() not in (None, 0)


@pytest.mark.parametrize('stop_raises', [False, True])
def test_dispatch_wait_ends_when_supervisor_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stop_raises: bool
) -> None:
    with _dispatch_wait_harness(tmp_path, monkeypatch) as (supervisor, factory, clock, command):
        owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        failure = RuntimeError('stop callback failed')
        dispatched = False

        def should_stop() -> bool:
            if clock() == 0:
                return False
            if stop_raises:
                raise failure
            return True

        def receive(*, timeout: float) -> ControlMessage:
            clock.advance(timeout)
            raise TimeoutError('empty slice')

        def wait(_timeout: float) -> None:
            nonlocal dispatched
            with pytest.raises(OperatorOutcomeUnknownError, match='supervisor is stopping'):
                supervisor.dispatch_operator_command(command, timeout=600)
            dispatched = True
            assert not owner.killed
            assert supervisor._dispatch_reserved is None

        monkeypatch.setattr(owner, 'receive', receive)
        if stop_raises:
            with pytest.raises(RuntimeError) as caught:
                supervisor.run(
                    should_stop=should_stop,
                    wait=wait,
                    set_foreground_progress=lambda _callback: None,
                )
            assert caught.value is failure
        else:
            supervisor.run(
                should_stop=should_stop, wait=wait, set_foreground_progress=lambda _callback: None
            )
        assert dispatched
        assert clock() == 0.05
        assert supervisor._maintenance_failure is None


@pytest.mark.parametrize('failure_site', ['checkpoint', 'restart'])
@pytest.mark.parametrize('response_send_fails', [False, True])
def test_tick_failure_during_dispatch_is_held_then_raised(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_site: str, response_send_fails: bool
) -> None:
    with _dispatch_wait_harness(tmp_path, monkeypatch) as (supervisor, factory, clock, command):
        owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        failure = (
            DatabaseError('checkpoint broken')
            if failure_site == 'checkpoint'
            else OSError('spawn broken')
        )
        dispatched = False

        def receive(*, timeout: float) -> ControlMessage:
            clock.advance(timeout)
            raise TimeoutError('empty slice')

        def fail(*_args: object) -> bool:
            raise failure

        def wait(_timeout: float) -> None:
            nonlocal dispatched
            if failure_site == 'checkpoint':
                assert supervisor._state is not None
                monkeypatch.setattr(supervisor._state, 'checkpoint', fail)
                supervisor._next_checkpoint_at = clock()
            else:
                monkeypatch.setattr(supervisor, '_restart_due_workers', fail)
            with pytest.raises(OperatorOutcomeUnknownError, match=f'supervisor failed: {failure}'):
                supervisor.dispatch_operator_command(command, timeout=600)
            dispatched = True
            assert supervisor._dispatch_reserved is None
            if response_send_fails:
                raise BrokenPipeError('client left')

        monkeypatch.setattr(owner, 'receive', receive)
        with pytest.raises(type(failure)) as caught:
            supervisor.run(
                should_stop=lambda: dispatched,
                wait=wait,
                set_foreground_progress=lambda _callback: None,
            )
        assert caught.value is failure
        assert dispatched
        assert supervisor._maintenance_failure is None


def test_supervisor_derives_probe_freshness_from_existing_budgets(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('control')
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
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


def _run_recovery_cli(
    configuration: tuple[Path, Config],
    workers: tuple[ScriptedWorkerFactory, FakeClock],
    setup: Callable[[Supervisor], None],
    argv: list[str],
    *,
    generations: dict[tuple[str, str], int] | None = None,
) -> tuple[int, list[Exception]]:
    config_path, config = configuration
    factory, clock = workers
    ready = threading.Event()
    stop = threading.Event()
    failures: list[Exception] = []

    def serve() -> None:
        try:
            with Supervisor(
                config=config, config_path=config_path, clock=clock, worker_factory=factory
            ) as supervisor:
                supervisor._reconcile(generations={} if generations is None else generations)
                supervisor.start()
                setup(supervisor)
                with ControlServer(
                    config.daemon.state_dir,
                    lambda request, peer: cli_module._handle_supervisor_request(
                        supervisor, request, peer
                    ),
                ) as server:
                    ready.set()
                    supervisor.run(
                        should_stop=stop.is_set,
                        wait=server.serve_once,
                        set_foreground_progress=lambda _callback: None,
                    )
        except (
            AssertionError,
            OSError,
            RuntimeError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as error:
            failures.append(error)
        finally:
            ready.set()

    thread = threading.Thread(target=serve)
    with pytest.MonkeyPatch.context() as transport_patch:
        _unbounded_control_transport(transport_patch)
        thread.start()
        try:
            assert ready.wait()
            assert not failures
            return cli_module.main([*argv, '--config', os.fspath(config_path)]), failures
        finally:
            stop.set()
            thread.join()
            assert not thread.is_alive()


def test_supervisor_evidence_retirement_restarts_from_the_durable_group_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
    _create_recovery_group_database(config)
    group_path = source_database(config.daemon.state_dir)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        owner = next(worker for worker in factory.instances.values() if not worker.spec.sources)
        assert owner.assignment is not None

        ready = _resolve_lane_recovery(
            config,
            owner.assignment,
            group_path,
            drop_ready=False,
        )
        owner.messages.append(ready)
        supervisor._tick()

        with _SupervisorState.open(config.daemon.state_dir) as state:
            assert state.pending_park_evidence() == ()
        with _open_conditional_group(group_path) as group:
            parked = group.snapshot()
            assert parked.parked
            assert parked.revision == evidence.directive.group_revision + 1
            recorded = group.resolve_conditional_park(
                evidence.directive,
                identity=AuditIdentity.current(),
                decided_at_wall_clock=999.0,
            )
            assert recorded.outcome == 'parked'
            assert recorded.resulting_revision == parked.revision

        owner.exit_status = 1
        supervisor._tick()
        clock.advance(1.0)
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        source.messages.append(health_update(source, heartbeat_at=clock()))
        supervisor._tick()
        supervisor._tick()

        replacement = factory.instances[owner.worker_id]
        assert replacement is not owner
        assert replacement.assignment is not None
        assert replacement.assignment.group_assignments == (
            DeliveryGroupAssignment(
                source='admin',
                group='remote',
                group_generation=1,
                start_mode='resume',
                conditional_park=None,
            ),
        )


@pytest.mark.parametrize('packing', ['default', 'dependent'])
def test_cli_adoption_committed_before_replacement_startup_failure(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    packing: str,
) -> None:
    stall_detectors.enable('startup')
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = (
        _default_packed_config(tmp_path, monkeypatch)
        if packing == 'default'
        else source_only_config(tmp_path, monkeypatch)
    )
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    monkeypatch.setattr(supervisor_module.time, 'sleep', clock.advance)
    originals: list[ScriptedWorker] = []

    def setup(supervisor: Supervisor) -> None:
        originals.extend(factory.instances.values())
        for worker in originals:
            worker.operator_handler = _source_adoption_result
        if packing == 'default':
            factory.authentication_ready = False
        else:

            def spawn_with_dependent_failure(
                **options: Unpack[_ScriptedWorkerOptions],
            ) -> ScriptedWorker:
                factory.authentication_ready = bool(options['spec'].sources)
                return factory(**options)

            supervisor._worker_factory = spawn_with_dependent_failure

    result, failures = _run_recovery_cli(
        (config_path, config),
        (factory, clock),
        setup,
        ['adopt-source-stream', '--site', 'test', '--source', 'admin', '--reason', 'role changed'],
    )
    captured = capsys.readouterr()
    failed = next(worker for worker in originals if worker.spec.groups)
    replacement = factory.instances[failed.worker_id]
    assert result == 1
    assert captured.out == ''
    assert 'adopt-source-stream committed;' in captured.err
    assert failed.worker_id in captured.err
    assert 'worker-startup-timeout' in captured.err
    assert 'Run status to see its state.' in captured.err
    assert replacement is not failed
    assert replacement.assignment is None
    assert not failures
    if packing == 'dependent':
        source = next(worker for worker in originals if worker.spec.sources)
        assert factory.instances[source.worker_id] is not source
        assert factory.instances[source.worker_id].assignment is not None


def test_supervisor_failed_open_attempts_lock_after_state_close_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
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

    state = SimpleNamespace(pending_park_evidence=pending, close=close_state)
    monkeypatch.setattr(
        supervisor_module, 'acquire_lock', Mock(return_value=SimpleNamespace(close=close_lock))
    )
    monkeypatch.setattr(supervisor_module._SupervisorState, 'open', Mock(return_value=state))
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        future_detail = replace(
            _idle_source_health(source, source='admin', heartbeat_at=100.0),
            phase='provider-query',
            deadline_at=10_000.0,
        )
        future = health_update(
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
                    worker_instance=source.spawn_instance,
                    phase='idle',
                    heartbeat_at=0.0,
                    deadline_at=None,
                ),
            ),
        )
        source.messages.append(future)
        dependent.messages.append(health_update(dependent, heartbeat_at=0.0))
        monkeypatch.setattr(source, 'wait', _never_reap)

        supervisor._tick()

        assert source.killed
        assert supervisor._spawns[source.worker_id].termination_reason == ('source-phase-overdue')
        first = next(
            item
            for item in _spawn_health(supervisor, source.worker_id).watchdogs
            if item.kind == 'source'
        )
        assert first.heartbeat_at == 0.0
        assert first.deadline_at == 0.0

        clock.advance(100.0)
        source.messages.append(future)
        dependent.messages.append(health_update(dependent, heartbeat_at=clock()))
        supervisor._tick()
        repeated = next(
            item
            for item in _spawn_health(supervisor, source.worker_id).watchdogs
            if item.kind == 'source'
        )
        assert repeated.deadline_at == 0.0
    finally:
        supervisor._close()


@pytest.mark.parametrize('failure_site', ['adoption', 'source-spawn', 'dependent-spawn'])
def test_cli_adoption_failure_preserves_established_recovery_facts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    failure_site: str,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = _two_group_shared_worker_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    failure = OSError('replacement spawn failed')
    results: list[CommandResult] = []
    failed_workers: list[str] = []

    def setup(supervisor: Supervisor) -> None:
        def respond(command: OperatorCommand) -> CommandResult:
            if failure_site == 'adoption' and command.command == 'adopt-source-stream':
                return CommandResult(
                    command=command.command,
                    ok=False,
                    message='provider initialization failed',
                    request_id=command.request_id,
                    result={'initialization_attempt': 1},
                )
            return _source_adoption_result(command)

        for worker in factory.instances.values():
            worker.operator_handler = respond

        def spawn(**options: Unpack[_ScriptedWorkerOptions]) -> ScriptedWorker:
            spec = options['spec']
            if bool(spec.sources) == (failure_site == 'source-spawn'):
                failed_workers.append(spec.worker_id)
                raise failure
            return factory(**options)

        if failure_site != 'adoption':
            supervisor._worker_factory = spawn
        adopt = supervisor.adopt_source_stream_command

        def record_result(command: OperatorCommand, *, timeout: float) -> CommandResult:
            result = adopt(command, timeout=timeout)
            results.append(result)
            return result

        monkeypatch.setattr(supervisor, 'adopt_source_stream_command', record_result)

    result, failures = _run_recovery_cli(
        (config_path, config),
        (factory, clock),
        setup,
        ['adopt-source-stream', '--site', 'test', '--source', 'admin', '--reason', 'role changed'],
    )
    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ''
    with _SupervisorState.open(config.daemon.state_dir) as state:
        records = [
            state.group_inventory_record(site='test', group=group)
            for group in ('remote', 'second')
        ]
        assert all(record is not None and record.orphan_generation == 1 for record in records)
    if failure_site == 'adoption':
        assert 'provider initialization failed' in captured.err
        assert 'already parked and reserved: remote, second' in captured.err
        assert 'committed' not in captured.err
        assert len(results) == 1
        assert results[0].result == {'initialization_attempt': 1}
        assert not failures
    else:
        assert 'adopt-source-stream committed;' in captured.err
        assert len(failed_workers) == 1
        assert failed_workers[0] in captured.err
        assert 'replacement spawn failed' in captured.err
        assert 'supervisor is stopping' in captured.err
        assert 'Do not repeat the recovery' in captured.err
        assert len(results) == 1
        assert results[0].result == {
            'adoption_count': 1,
            'initialized': True,
            'source': 'admin',
            'status': 'running',
            'stream_incarnation': 'adoption-reservation-1',
        }
        assert failures == [failure]


@pytest.mark.parametrize('park_outcome', ['unanswered', 'refused'])
def test_cli_adoption_reports_park_outcome(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    park_outcome: str,
) -> None:
    config_path, config = _two_group_shared_worker_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()

    def setup(_supervisor: Supervisor) -> None:
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        source.operator_handler = _source_adoption_result
        if park_outcome == 'unanswered':

            def disconnected(*, timeout: float | None = None) -> ControlMessage:
                del timeout
                raise EOFError('park reply lost')

            monkeypatch.setattr(owner, 'receive', disconnected)
        else:

            def park(message: OperatorCommand) -> CommandResult:
                accepted = message.arguments['group'] == 'remote'
                return CommandResult(
                    command=message.command,
                    ok=accepted,
                    message='group is busy',
                    request_id=message.request_id,
                    result={'parked': True, 'group_generation': 1} if accepted else {},
                )

            owner.operator_handler = park

    result, failures = _run_recovery_cli(
        (config_path, config),
        (factory, clock),
        setup,
        ['adopt-source-stream', '--site', 'test', '--source', 'admin', '--reason', 'role changed'],
    )
    captured = capsys.readouterr()
    assert captured.out == ''
    assert not failures
    if park_outcome == 'unanswered':
        assert 'park did not finish:' in captured.err
        assert result == 3
        assert 'It may still complete; run status to see the current state.' in captured.err
    else:
        assert result == 1
        assert "source adoption could not park group 'second': group is busy" in captured.err
        assert 'already parked and reserved: remote' in captured.err
        with _SupervisorState.open(config.daemon.state_dir) as state:
            record = state.group_inventory_record(site='test', group='remote')
            assert record is not None
            assert record.orphan_generation == 1


@pytest.mark.parametrize('failure_site', ['checkpoint', 'restart'])
def test_cli_pending_command_reports_supervisor_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    failure_site: str,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    failure = (
        DatabaseError('checkpoint broken')
        if failure_site == 'checkpoint'
        else OSError('spawn broken')
    )

    def setup(supervisor: Supervisor) -> None:
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)

        def fail(*_args: object) -> bool:
            raise failure

        def receive(*, timeout: float | None = None) -> ControlMessage:
            del timeout
            if failure_site == 'checkpoint':
                assert supervisor._state is not None
                monkeypatch.setattr(supervisor._state, 'checkpoint', fail)
                supervisor._next_checkpoint_at = clock()
            else:
                monkeypatch.setattr(supervisor, '_restart_due_workers', fail)
            raise TimeoutError('park pending')

        monkeypatch.setattr(owner, 'receive', receive)

    result, failures = _run_recovery_cli(
        (config_path, config),
        (factory, clock),
        setup,
        ['park', '--site', 'test', '--group', 'remote', '--reason', 'maintenance'],
    )
    captured = capsys.readouterr()
    assert f'park did not finish: the supervisor failed: {failure}' in captured.err
    assert result == 3
    assert captured.out == ''
    assert failures == [failure]


def test_supervisor_grant_health_failure_kills_source_author(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
    )
    try:
        supervisor._open()
        source, dependent = _start_blocked_dependent_grant(supervisor, factory)
        source.messages.append(
            replace(
                health_update(source, heartbeat_at=0.0),
                worker=replace(
                    health_update(source, heartbeat_at=0.0).worker,
                    site='wrong-site',
                ),
            )
        )
        monkeypatch.setattr(source, 'wait', _never_reap)

        supervisor._tick()

        assert source.killed
        assert not dependent.killed
        assert supervisor._spawns[source.worker_id].termination_reason == (
            'watchdog-sampling-failed'
        )
        assert supervisor._spawns[dependent.worker_id].termination_reason is None
    finally:
        supervisor._close()


@pytest.mark.parametrize('refused', [False, True])
def test_cli_replay_reports_committed_or_reserved_state(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    refused: bool,
) -> None:
    stall_detectors.enable('startup')
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    source_path, group_path = prepare_absent_orphan(config)
    with _lifecycle_source(source_path) as source:
        source.commit_observations((110,), successful_clock=1.0)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    monkeypatch.setattr(supervisor_module.time, 'sleep', clock.advance)
    worker_ids: list[str] = []

    def setup(_supervisor: Supervisor) -> None:
        owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        worker_ids.append(owner.worker_id)

        def replay(message: OperatorCommand) -> CommandResult:
            if refused:
                return CommandResult(
                    command=message.command,
                    ok=False,
                    message='replay refused',
                    request_id=message.request_id,
                    result={},
                )
            return _execute_source_group_command(group_path, message)

        owner.operator_handler = replay
        factory.authentication_ready = False

    result, failures = _run_recovery_cli(
        (config_path, config),
        (factory, clock),
        setup,
        [
            'replay',
            '--site',
            'test',
            '--group',
            'remote',
            '--reason',
            'recover history',
            '--from-event-id',
            '101',
        ],
        generations={('test', 'remote'): 1},
    )
    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ''
    assert not failures
    if refused:
        assert 'replay refused; its durable disposition reservation was kept' in captured.err
    else:
        assert 'replay committed;' in captured.err
        assert worker_ids[0] in captured.err
        assert 'worker-startup-timeout' in captured.err
        with _lifecycle_group(group_path, generation=2) as group:
            assert group.snapshot().group_generation == 2
            assert group.terminal_disposition_identity(2) is not None


def test_supervisor_health_receipt_time_accepts_heartbeat_after_tick_entry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, _config = source_only_config(tmp_path, monkeypatch)
    config_path.write_text(
        _replace_config_text(
            config_path.read_text(encoding='utf-8'),
            'health_interval_secs = 0.05',
            'health_interval_secs = 1.0',
        ),
        encoding='utf-8',
    )
    config = load_config(config_path)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    clock.now = 10.0
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        source.messages.append(health_update(source, heartbeat_at=10.5))
        dependent.messages.append(health_update(dependent, heartbeat_at=10.5))
        original_receive = source.receive

        def receive_after_clock_advance(
            *,
            timeout: float | None = None,
        ) -> ControlMessage:
            message = original_receive(timeout=timeout)
            if isinstance(message, HealthUpdate):
                clock.advance(1.0)
            return message

        monkeypatch.setattr(source, 'receive', receive_after_clock_advance)

        supervisor._tick()

        assert clock() == 11.0
        assert not source.killed
        assert supervisor._spawns[source.worker_id].termination_reason is None
        assert _spawn_health(supervisor, source.worker_id).worker.heartbeat_at == 10.5


def test_supervisor_ingest_advances_between_pending_worker_messages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(iter(factory.instances.values()))
        update = health_update(worker, heartbeat_at=0.0)
        worker.messages.extend((update, update))
        advances: list[None] = []
        receive = worker.receive
        receive_count = 0

        def observe_receive(*, timeout: float | None = None) -> ControlMessage:
            nonlocal receive_count
            if receive_count == 1:
                assert advances == [None]
            receive_count += 1
            return receive(timeout=timeout)

        monkeypatch.setattr(worker, 'receive', observe_receive)
        monkeypatch.setattr(supervisor, '_advance_progress', lambda: advances.append(None))
        ingest = cast(
            'Callable[[], bool]',
            vars(Supervisor)['_ingest_worker_ipc'].__get__(supervisor),
        )

        assert ingest()
    finally:
        supervisor._close()


@contextmanager
def _outside_supervisor_writer(state: _SupervisorState) -> Generator[sqlite3.Connection]:
    with closing(sqlite3.connect(state._database.path, isolation_level=None)) as writer:
        writer.execute('BEGIN IMMEDIATE')
        try:
            yield writer
        finally:
            writer.rollback()


def test_supervisor_inventory_orphans_removal_and_parks_readded_generation(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    with _SupervisorState.open(state_dir) as state:
        initial = state.reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={},
        )
        initial_assignment = initial[('test', 'remote')]
        assert isinstance(initial_assignment, ActiveSourceGroupAssignment)
        assert initial_assignment.group_generation == 1
        assert (
            state.reconcile_groups(
                configured={},
                generations={('test', 'remote'): 1},
            )
            == {}
        )
        assert state.orphaned_groups() == {('test', 'remote'): 1}

    with _SupervisorState.open(state_dir) as state:
        assignments = state.reconcile_groups(
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

        stale_assignments = state.reconcile_groups(
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

        reconciled = state.reconcile_groups(
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        dependent.messages.append(health_update(dependent, heartbeat_at=0.0))
        supervisor._tick()
        dependent.messages.append(
            health_update(
                dependent,
                heartbeat_at=0.0,
                groups=(),
                watchdogs=(
                    WatchdogHealth(
                        kind='operator',
                        site='test',
                        source=None,
                        group=None,
                        worker_instance=dependent.spawn_instance,
                        phase='idle',
                        heartbeat_at=0.0,
                        deadline_at=None,
                    ),
                ),
            )
        )
        monkeypatch.setattr(dependent, 'wait', _never_reap)

        supervisor._tick()

        assert dependent.killed
        assert supervisor._spawns[dependent.worker_id].termination_reason == ('lane-phase-overdue')
    finally:
        supervisor._close()


def test_supervisor_open_file_need_includes_fixed_sources_and_groups() -> None:
    assert supervisor_module._supervisor_open_file_need(0, 0) == 7
    assert supervisor_module._supervisor_open_file_need(1, 0) == 14
    assert supervisor_module._supervisor_open_file_need(12, 32) == 251


def test_supervisor_open_restores_pending_conditional_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)

    with Supervisor(config=config, config_path=config_path) as supervisor:
        assert supervisor._park_evidence_by_group == {('test', 'remote'): evidence}


def test_supervisor_operator_dispatch_rejects_terminating_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
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
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=0.0))
        supervisor._tick()
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        owner.messages.append(
            health_update(
                owner,
                heartbeat_at=0.0,
                groups=(),
                watchdogs=(
                    WatchdogHealth(
                        kind='operator',
                        site='test',
                        source=None,
                        group=None,
                        worker_instance=owner.spawn_instance,
                        phase='idle',
                        heartbeat_at=0.0,
                        deadline_at=None,
                    ),
                ),
            )
        )
        monkeypatch.setattr(owner, 'wait', _never_reap)
        owner.operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted by dying worker',
            request_id=sent.request_id,
            result={'revision': 99},
        )
        supervisor._tick()
        assert owner.killed
        assert supervisor._spawns[owner.worker_id].termination_reason == ('lane-phase-overdue')
        sent_before = tuple(owner.sent_commands)

        with pytest.raises(
            ValueError,
            match='has no running worker',
        ):
            supervisor.dispatch_operator_command(command, timeout=1.0)

        assert tuple(owner.sent_commands) == sent_before
        assert supervisor._spawns[owner.worker_id].termination_reason == ('lane-phase-overdue')
    finally:
        supervisor._close()


@pytest.mark.parametrize('case', ['reset', 'newer', 'retire', 'restart'])
def test_pending_writes_keep_order_and_identity(tmp_path: Path, case: str) -> None:
    with _SupervisorState.open(tmp_path) as state:
        breaker = _pending_test_breaker()
        state.persist_worker_breaker(breaker)
        stored = breaker.projection
        with _outside_supervisor_writer(state):
            for _ in range(3):
                breaker.record_failure('pending failure')
                state.persist_worker_breaker(breaker)
            if case == 'retire':
                evidence = _park_evidence()
                assert state.record_park_evidence(evidence)
                state.retire_park_evidence(evidence.directive.evidence_id)
        if case == 'reset':
            state.reset_worker_breaker(
                breaker,
                expected_revision=breaker.revision,
                reason='reviewed',
                identity=AuditIdentity(local_uid=1, username='operator', pid=1),
                wall_clock_time=1,
            )
            assert not breaker.is_open
            assert state.audit_count() == 1
        elif case == 'newer':
            # A second projection joins before the periodic tick has a chance to flush.
            breaker.apply_audited_reset(expected_revision=breaker.revision, reason='newer')
            state.persist_worker_breaker(breaker)
        elif case == 'retire':
            assert state.flush_pending()
            assert state.pending_park_evidence() == ()
        if case != 'restart':
            restored = state.restore_worker_breaker(
                worker_id=breaker.worker_id,
                restart_limit=2,
                restart_window_secs=100,
                healthy_reset_secs=0.1,
            )
            assert restored.projection == breaker.projection
    if case == 'restart':
        with _SupervisorState.open(tmp_path) as reopened:
            restored = reopened.restore_worker_breaker(
                worker_id=breaker.worker_id,
                restart_limit=2,
                restart_window_secs=100,
                healthy_reset_secs=0.1,
            )
            assert restored.projection == stored


@pytest.mark.parametrize('acknowledge', [False, True])
def test_pending_park_evidence_does_not_delay_real_lane_containment(
    tmp_path: Path,
    acknowledge: bool,
) -> None:

    config = _callback_hold_config(tmp_path)
    _reveal_callback_events(tmp_path, 1)
    with running_supervisor(config, ChildPatches(hold_callback=True, gate_events=True)) as probe:
        probe.wait_for(
            lambda: (
                len(probe.latest_health) == 2
                and any(
                    group.group == 'remote' and group.join_phase == 'attached'
                    for health in probe.latest_health.values()
                    for group in health.groups
                )
            )
        )
        state = probe.supervisor._state
        assert state is not None
        with _outside_supervisor_writer(state):
            (tmp_path / 'callback-arm').touch()
            _reveal_callback_events(tmp_path, 2)
            probe.wait_for((tmp_path / 'callback-entered').exists)
            (tmp_path / 'callback-expire').touch()
            probe.wait_for(lambda: bool(probe.park_evidence))
            evidence = next(iter(probe.park_evidence.values()))
            assert any(item.reason == 'lane-phase-overdue' for item in probe.terminations)
            assert state.pending_park_evidence() == ()
            assert 'write-lock-stalled' in cast(
                'tuple[str, ...]', probe.supervisor.status_mapping({})['conditions']
            )
            assert all(
                spawn.process.poll() is None
                for spawn in probe.spawns
                if spawn.worker_id != evidence.failed_worker_id
            )
            if acknowledge:
                (tmp_path / 'callback-release').touch()
                probe.wait_for(
                    lambda: not probe.park_evidence,
                    expected_exits=(evidence.directive.failed_spawn_instance,),
                )
                assert state.pending_park_evidence() == ()
        # The acknowledgement must cancel an unstored record, while an
        # unacknowledged record must land on the first successful flush.
        assert state.flush_pending()
        assert state.pending_park_evidence() == (() if acknowledge else (evidence,))
        (tmp_path / 'callback-release').touch()


def test_supervisor_operator_wait_contains_partial_health_before_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    command = OperatorCommand(
        request_id='partial-health-command',
        command='park',
        arguments={'site': 'test', 'group': 'remote', 'reason': 'maintenance'},
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=0.0))
        supervisor._tick()
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        owner.messages.append(
            health_update(
                owner,
                heartbeat_at=0.0,
                groups=(),
                watchdogs=(
                    WatchdogHealth(
                        kind='operator',
                        site='test',
                        source=None,
                        group=None,
                        worker_instance=owner.spawn_instance,
                        phase='idle',
                        heartbeat_at=0.0,
                        deadline_at=None,
                    ),
                ),
            )
        )
        owner.operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'revision': 3},
        )

        with pytest.raises(OperatorOutcomeUnknownError, match='worker failed while running it'):
            supervisor.dispatch_operator_command(command, timeout=1.0)

        assert owner.killed
        assert supervisor._spawns[owner.worker_id].termination_reason == ('lane-phase-overdue')
        assert not _spawn_health(supervisor, owner.worker_id).worker.monitor_alive


def test_supervisor_operator_watchdog_overdue_uses_exact_reason_without_park(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('phase', 'coordination')
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )

        def _operator_update() -> HealthUpdate:
            group = _in_flight_group_health(
                dependent,
                heartbeat_at=clock(),
                phase='idle',
            )
            return health_update(
                dependent,
                heartbeat_at=clock(),
                groups=(group,),
                watchdogs=(
                    WatchdogHealth(
                        kind='lane',
                        site='test',
                        source='admin',
                        group='remote',
                        worker_instance=dependent.spawn_instance,
                        phase='idle',
                        heartbeat_at=clock(),
                        deadline_at=clock() + 1.25,
                    ),
                    WatchdogHealth(
                        kind='operator',
                        site='test',
                        source='admin',
                        group='remote',
                        worker_instance=dependent.spawn_instance,
                        phase='internal',
                        heartbeat_at=0.0,
                        deadline_at=10_000.0,
                        operator_command='park',
                    ),
                ),
            )

        source.messages.append(health_update(source, heartbeat_at=clock()))
        dependent.messages.append(_operator_update())
        supervisor._tick()
        credential = config.sites['test'].credentials['admin']
        clock.advance(
            internal_stall_deadline_secs(
                config.daemon.health_interval_secs,
                credential,
            )
        )
        source.messages.append(health_update(source, heartbeat_at=clock()))
        dependent.messages.append(_operator_update())
        monkeypatch.setattr(dependent, 'wait', _never_reap)

        supervisor._tick()

        assert dependent.killed
        assert supervisor._spawns[dependent.worker_id].termination_reason == (
            'operator-phase-overdue'
        )
        assert supervisor._park_evidence_by_group == {}
    finally:
        supervisor._close()


def test_supervisor_orphans_before_retiring_same_generation_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        supervisor._reconcile(
            generations={},
        )
    evidence = _park_evidence(group_revision=0)
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)

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
    original_retire = _SupervisorState.retire_park_evidence
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
            'retire_park_evidence',
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

    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.pending_park_evidence() == ()


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


def test_supervisor_outer_loop_does_not_spin_for_a_retired_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        dependent.wait_results.extend((None, None))
        health = health_update(dependent, heartbeat_at=clock())
        dependent.messages.append(
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

        assert all(0 < seconds <= config.daemon.supervisor_poll_interval_secs for seconds in waits)
    finally:
        supervisor._close()


def test_supervisor_outer_loop_ignores_consumed_restart_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        dependent.exit_status = 1
        supervisor._tick()
        factory.authentication_ready = False
        clock.advance(1.0)
        supervisor._tick()
        replacement = factory.instances[dependent.worker_id]

        supervisor.run(
            should_stop=lambda: bool(waits),
            wait=waits.append,
            set_foreground_progress=lambda _callback: None,
        )

        assert replacement is not dependent
        assert replacement.authentication_calls > 0
        assert waits == [pytest.approx(config.daemon.checkpoint_interval_secs)]
    finally:
        supervisor._close()


def test_supervisor_outer_loop_waits_for_confirming_group_phase_frame(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('phase', 'coordination')
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
    factory = ScriptedWorkerFactory()
    monotonic_clock = FakeClock()
    wall_clock = FakeClock()
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
            worker for worker in factory.instances.values() if 'remote' in worker.spec.groups
        )
        other = next(
            worker
            for worker in factory.instances.values()
            if not worker.spec.sources and worker is not remote
        )
        remote_health = _in_flight_group_health(
            remote,
            heartbeat_at=monotonic_clock(),
        )
        for worker in factory.instances.values():
            worker.messages.append(
                health_update(
                    worker,
                    heartbeat_at=monotonic_clock(),
                    groups=(remote_health,) if worker is remote else (),
                )
            )
        supervisor._tick()
        other.exit_status = 1
        supervisor._tick()
        for worker in factory.instances.values():
            worker.receive_timeouts.clear()

        group_path = source_database(config.daemon.state_dir)

        def wait(seconds: float) -> None:
            waits.append(seconds)
            monotonic_clock.advance(seconds)
            assert not remote.killed
            assert not supervisor._park_evidence_by_group
            if len(waits) == 2:
                remote.messages.append(
                    health_update(remote, heartbeat_at=monotonic_clock(), groups=(remote_health,))
                )

        with _conditional_group(group_path):
            supervisor.run(
                should_stop=lambda: remote.killed or len(waits) >= 3,
                wait=wait,
                set_foreground_progress=lambda _callback: None,
            )

        assert remote.killed
        assert waits == [config.daemon.supervisor_poll_interval_secs] * 2
        assert monotonic_clock() == sum(waits)
        evidence = supervisor._park_evidence_by_group[('test', 'remote')]
        assert evidence.failed_worker_id == remote.worker_id
        assert evidence.directive.failed_spawn_instance == remote.spawn_instance
        assert evidence.directive.event_id == 42
        assert evidence.directive.group_revision == 7
        assert evidence.directive.observed_at_wall_clock == 1000.0
        assert all(
            timeout == 0.0
            for worker in factory.instances.values()
            for timeout in worker.receive_timeouts
        )
    finally:
        supervisor._close()


def test_supervisor_outer_loop_waits_until_the_earliest_monitor_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, _config = source_only_config(tmp_path, monkeypatch)
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
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        dependent.exit_status = 1
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        for worker in factory.instances.values():
            worker.receive_timeouts.clear()

        supervisor.run(
            should_stop=lambda: bool(waits),
            wait=waits.append,
            set_foreground_progress=lambda _callback: None,
        )

        assert waits == [1.0]
    finally:
        supervisor._close()


def test_supervisor_park_evidence_selects_only_frame_confirmed_lane(
    stall_detectors: StallDetectors, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = _two_group_shared_worker_config(tmp_path, monkeypatch)
    clock = FakeClock()
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        wall_clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        remote = next(w for w in factory.instances.values() if not w.spec.sources)
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=0))
        supervisor._tick()
        clock.advance(10.1)
        confirmed = replace(_in_flight_group_health(remote, heartbeat_at=0), deadline_at=9.9)
        unconfirmed = replace(
            _in_flight_group_health(remote, heartbeat_at=0, group='second'), deadline_at=10.05
        )
        for worker in factory.instances.values():
            worker.messages.append(
                health_update(
                    worker,
                    heartbeat_at=10,
                    groups=(confirmed, unconfirmed) if worker is remote else None,
                )
            )
        with _conditional_group(source_database(config.daemon.state_dir)):
            supervisor._tick()
        assert remote.killed
        assert set(supervisor._park_evidence_by_group) == {('test', 'remote')}
        evidence = supervisor._park_evidence_by_group[('test', 'remote')]
        assert evidence.directive.failed_spawn_instance == remote.spawn_instance
        assert evidence.directive.event_id == 42
        assert evidence.directive.group_revision == 7


def test_supervisor_partial_startup_health_does_not_extend_startup_deadline(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('startup')
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        clock.advance(config.daemon.worker_startup_deadline_secs - 0.01)
        source.messages.append(
            health_update(
                source,
                heartbeat_at=clock(),
                sources=(),
                groups=(),
                watchdogs=(),
            )
        )
        supervisor._tick()
        assert not source.killed

        clock.advance(0.01)
        monkeypatch.setattr(source, 'wait', _never_reap)
        supervisor._tick()

        assert source.killed
        assert supervisor._spawns[source.worker_id].termination_reason == (
            'worker-startup-timeout'
        )
    finally:
        supervisor._close()


@pytest.mark.parametrize('busy', [False, True])
def test_park_evidence_survives_only_a_stored_flush(tmp_path: Path, busy: bool) -> None:
    config = _callback_hold_config(tmp_path)
    context = multiprocessing.get_context('spawn')
    parent, child = context.Pipe()
    process = context.Process(target=_supervisor_park_crash_child, args=(str(config), busy, child))
    process.start()
    child.close()
    workers: list[tuple[int, str]] = []
    try:
        assert parent.poll(None)
        kind, payload = parent.recv()
        assert kind == 'ready', payload
        workers = payload
        with closing(
            sqlite3.connect(tmp_path / 'state' / 'supervisor.db', isolation_level=None)
        ) as writer:
            if busy:
                writer.execute('BEGIN IMMEDIATE')
            parent.send('arm')
            assert parent.poll(None)
            kind, payload = parent.recv()
            assert kind == 'boundary', payload
            process.join()
            assert process.exitcode == -signal.SIGKILL
            writer.rollback()
        with _SupervisorState.open(tmp_path / 'state') as state:
            assert bool(state.pending_park_evidence()) is (not busy)
    finally:
        if process.is_alive():
            process.kill()
        process.join()
        parent.close()
        for pid, _worker_id in workers:
            with suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)


def test_supervisor_preflights_adoption_before_parking_healthy_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source_owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        group_owner = next(worker for worker in factory.instances.values() if worker.spec.groups)

        def _reject_adoption(message: OperatorCommand) -> CommandResult:
            return CommandResult(
                command=message.command,
                ok=False,
                message='source adoption requires stream-identity-changed halt',
                request_id=message.request_id,
                result={},
            )

        source_owner.operator_handler = _reject_adoption
        group_owner.operator_handler = lambda message: CommandResult(
            command=message.command,
            ok=True,
            message='accepted',
            request_id=message.request_id,
            result={'group': 'remote', 'parked': True},
        )

        result = supervisor.adopt_source_stream_command(command, timeout=1.0)

    assert not result.ok
    assert [item.command for item in source_owner.sent_commands] == ['preflight-source-adoption']
    assert group_owner.sent_commands == []


@pytest.mark.parametrize('command_name', ['reset-worker-breaker', 'reset-group-to-latest'])
def test_supervisor_busy_control_response_preserves_failure_code(
    tmp_path: Path,
    command_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    _unbounded_control_transport(monkeypatch)
    config = write_daemon_config(tmp_path, Topology('dedicated', ('remote',)))
    with running_supervisor(config, ChildPatches()) as probe:
        probe.wait_for(lambda: len(probe.latest_health) == 2)
        supervisor = probe.supervisor
        state = supervisor._state
        assert state is not None
        breaker = next(iter(probe.breakers.values()))
        arguments = {'site': 'test', 'reason': 'reviewed'}
        if command_name == 'reset-worker-breaker':
            for _ in range(6):
                breaker.record_failure('failed')
            state.persist_worker_breaker(breaker)
            assert breaker.is_open
            arguments['worker'] = breaker.worker_id
        else:
            arguments['group'] = 'remote'
            result = supervisor.dispatch_operator_command(
                OperatorCommand(
                    request_id='park-control',
                    command='park',
                    arguments=arguments,
                    identity=AuditIdentity(local_uid=1, username='operator', pid=1),
                ),
                timeout=float('inf'),
            )
            assert result.ok
        with (
            _outside_supervisor_writer(state),
            ControlServer(
                tmp_path / 'state',
                lambda request, peer: cli_module._handle_supervisor_request(
                    supervisor, request, peer
                ),
            ) as server,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            attempts: list[int] = []
            acquire = state._write_transaction

            @contextmanager
            def observed(timeout_millis: int) -> Generator[sqlite3.Connection]:
                attempts.append(timeout_millis)
                with acquire(0) as acquired:
                    yield acquired

            monkeypatch.setattr(state, '_write_transaction', observed)
            client = ControlClient.for_state_dir(tmp_path / 'state')
            future = executor.submit(client.request, command_name, arguments)
            assert server._listener is not None
            select.select((server._listener,), (), ())
            server.serve_once(0)
            error = future.exception()
            assert attempts == [2000]
            assert isinstance(error, ControlCommandError)
            assert error.result == {'code': 'supervisor-database-busy'}
            # Rejection leaves the supervisor's next control request serviceable.
            future = executor.submit(client.request, 'status')
            assert server._listener is not None
            select.select((server._listener,), (), ())
            server.serve_once(0)
            assert 'conditions' in future.result()


def test_supervisor_probe_mapping_uses_one_exact_observation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(config=config, config_path=config_path, clock=lambda: 0.0)
    progress = vars(supervisor)['_progress']
    halt_writes: list[str] = []
    vars(supervisor)['_halt_reason_value'] = 'already halted'
    monkeypatch.setattr(progress, 'set_halted', lambda: halt_writes.append('halted'))

    assert supervisor._halt_reason_value == 'already halted'
    supervisor.probe_result()
    supervisor.probe_result()

    assert halt_writes == []


@pytest.mark.parametrize('current_handle', [False, True], ids=['replaced', 'current'])
def test_supervisor_probe_termination_keeps_exact_spawn_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, current_handle: bool
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    previous = ScriptedWorker(spec, 'previous-spawn', [], None, authentication_ready=True)
    current = ScriptedWorker(spec, 'current-spawn', [], None, authentication_ready=True)
    previous_health = health_update(previous, heartbeat_at=1.0)
    current_health = health_update(current, heartbeat_at=2.0)
    supervisor = Supervisor(config=config, config_path=config_path, clock=FakeClock())
    supervisor._spawns[current.worker_id] = supervisor_module._WorkerSpawn(
        worker=current,
        health_deadline=10.0,
        pending_groups=frozenset(),
        lifecycle=None,
        health=current_health,
        termination_reason='current-containment',
    )
    probe = SupervisorProbe(supervisor, config_path, ChildPatches())
    probe.health_before_containment[previous.spawn_instance] = previous_health
    probe.health_before_containment[current.spawn_instance] = current_health
    observed = current if current_handle else previous

    probe.record_termination(cast('_WorkerInstance', observed), 'observed-exit')

    assert len(probe.terminations) == 1
    termination = probe.terminations[0]
    assert termination.spawn_instance == observed.spawn_instance
    assert (termination.reason, termination.health) == (
        ('current-containment', current_health) if current_handle else ('observed-exit', None)
    )
    assert termination.health_before_containment is (
        current_health if current_handle else previous_health
    )


@pytest.mark.parametrize('retired_group', ['remote', 'second'])
def test_pending_park_retirement_preserves_replacement_order(
    tmp_path: Path,
    retired_group: str,
) -> None:
    first = _park_evidence()
    other = _park_evidence(evidence_id='other', group='second')
    replacement = _park_evidence(
        evidence_id='replacement', failed_spawn_instance='replacement-spawn'
    )
    with _SupervisorState.open(tmp_path) as state:
        assert state.record_park_evidence(first)
        assert state.record_park_evidence(other)
        with _outside_supervisor_writer(state):
            state.retire_park_evidence(
                first.directive.evidence_id
                if retired_group == 'remote'
                else other.directive.evidence_id
            )
            assert state.record_park_evidence(replacement) is (retired_group == 'remote')
            # A second candidate must not overwrite either the unresolved stored
            # observation or the newly queued observation for this group.
            assert not state.record_park_evidence(_park_evidence(evidence_id='duplicate'))
            assert first in state.pending_park_evidence()
        assert state.flush_pending()
        expected = replacement if retired_group == 'remote' else first
        assert tuple(
            item for item in state.pending_park_evidence() if item.directive.group == 'remote'
        ) == (expected,)


def _control_probe_config(tmp_path: Path, groups: tuple[str, ...] = ('remote',)) -> Path:
    return write_daemon_config(
        tmp_path,
        Topology('dedicated' if groups else 'default', groups or ('local',)),
    )


def test_supervisor_ready_lifecycle_health_failure_uses_watchdog_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
    _create_recovery_group_database(config)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    )
    try:
        supervisor._open()
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        assert supervisor._spawns[owner.worker_id].lifecycle is None
        assert supervisor._spawns[owner.worker_id].pending_recovery
        owner.messages.append(
            replace(
                health_update(owner, heartbeat_at=0.0),
                worker=replace(
                    health_update(owner, heartbeat_at=0.0).worker,
                    site='wrong-site',
                ),
            )
        )
        monkeypatch.setattr(owner, 'wait', _never_reap)

        supervisor._tick()

        assert owner.killed
        assert supervisor._spawns[owner.worker_id].termination_reason == (
            'watchdog-sampling-failed'
        )
    finally:
        supervisor._close()


def test_supervisor_real_watchdog_phase_anchor_and_callback_slide(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(config=config, config_path=config_path)
    clock = FakeClock()
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
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
    monkeypatch.setenv('PYTHONPATH', os.fspath(Path(__file__).parents[1] / 'src'))
    report_path = tmp_path / 'startup-report.json'
    spawner = _ControlChildSpawner(_partial_startup_child_code(config_path, report_path))
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)
    supervisor = Supervisor(config=config, config_path=config_path)
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        while not vars(supervisor)['_restart_deadlines']:
            supervisor._tick()
            time.sleep(0.005)
        assert len(spawner.processes) == 1
        assert reap_child(spawner.processes[0]) is not None
        assert json.loads(report_path.read_text()) == {
            'stuck': ['source:admin'],
            'closed': [],
            'resources_open': True,
            'monitor_started': False,
            'forced': [],
        }
        spec = build_worker_specs(config.sites['test'])[0]
        assert spec.worker_id in vars(supervisor)['_restart_deadlines']
        assert spec.worker_id not in supervisor._spawns
    finally:
        supervisor._close()
        for process in spawner.processes:
            terminate_child(process, force=True)
            assert process.wait() is not None


def _wait_control_event(
    root: Path, worker: _WorkerInstance, kind: str, *, request_id: str | None = None
) -> dict[str, object]:
    while True:
        exit_status = worker._process.poll()
        for event in reversed(_control_events(root, worker)):
            if event['kind'] == kind and (
                request_id is None or event.get('request_id') == request_id
            ):
                return event
        assert exit_status is None, (
            f'{kind} missing; exit={worker._process.poll()}; '
            f'events={_control_events(root, worker)}'
        )
        time.sleep(0.002)


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
    with _SupervisorState.open(tmp_path / 'state') as state:
        state.reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={},
        )

        assert state.reconcile_groups(
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
            state.reconcile_groups(
                configured={('test', 'remote'): 'admin'},
                generations={},
            )


def test_supervisor_rejects_a_second_instance_with_pid_and_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
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


@pytest.mark.parametrize('active_event', [False, True])
def test_supervisor_reconcile_orphans_once_and_starts_parked_readd(
    active_event: bool,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
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
        source.initialize(500, incarnation='incarnation-1')
        if active_event:
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
            current = commit_step(
                group,
                StartEventStep(101, manifest, None),
                expected_revision=group.snapshot().revision,
            )
            current = commit_step(group, BeginCallbackStep(0), expected_revision=current.revision)
            group.callback_failed(0, 'callback failed', expected_revision=current.revision)
        before = group.snapshot()
        audit_before = group.audit_count()
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
        assert orphaned.revision == before.revision + 1
        assert orphaned.cursor == before.cursor
        assert orphaned.in_flight_event_id is None
        assert group.audit_count() == audit_before + 1
        if active_event:
            assert marker[0].from_event_id == 101

    factory = ScriptedWorkerFactory()
    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(
            generations={('test', 'remote'): 1},
        )
        supervisor.start()

    assert all(not worker.spec.groups for worker in factory.instances.values())
    source_owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
    assert source_owner.assignment is not None
    assert source_owner.assignment.source_group_assignments == (
        PendingDispositionSourceGroupAssignment(
            source='admin', group='remote', orphan_generation=1, reserved_generation=2
        ),
    )


@pytest.mark.parametrize('stop_during_gap', [False, True])
def test_supervisor_gap_contains_no_worker(tmp_path: Path, stop_during_gap: bool) -> None:
    config = _control_probe_config(tmp_path)
    with running_supervisor(config, ChildPatches(control_probe=True)) as probe:
        workers = _ready_control_probe(probe)
        worker = next(item for item in workers if item.spec.sources)
        (tmp_path / f'{worker.spawn_instance}.large').touch()
        _wait_control_event(tmp_path, worker, 'frame-started')
        while sum(event['kind'] == 'sample' for event in _control_events(tmp_path, worker)) <= 5:
            assert worker._process.poll() is None, probe.failure_details()
            time.sleep(0.002)
        resumed = time.monotonic()
        if stop_during_gap:
            worker.send(StopCommand(reason='gap-stop'))
            stopped = _wait_control_event(tmp_path, worker, 'stop')
            assert stopped['reason'] == 'gap-stop'
            assert worker._process.wait() == 0
            return
        assert worker._process.poll() is None, probe.failure_details()
        probe.wait_for(
            lambda: probe.latest_health[worker.worker_id].worker.heartbeat_at >= resumed
        )
        assert not probe.terminations, probe.failure_details()
        assert all(breaker._last_failure_at is None for breaker in probe.breakers.values())
        assert sum(event['kind'] == 'sample' for event in _control_events(tmp_path, worker)) > 5


def test_supervisor_repeated_failure_preserves_first_containment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=0.0))
        supervisor._tick()
        owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        evidence = _park_evidence(failed_worker_id=owner.worker_id)
        vars(supervisor)['_park_evidence_by_group'][('test', 'remote')] = evidence
        kill_calls: list[None] = []
        owner.on_kill = lambda: kill_calls.append(None)
        fail = supervisor._fail_current_worker

        fail(owner, 'lane-phase-overdue')
        first_health = _spawn_health(supervisor, owner.worker_id)
        first_evidence = {('test', 'remote'): evidence}
        fail(
            owner,
            'worker-control-send-failed',
            watchdog_failure=True,
        )

        assert supervisor._spawns[owner.worker_id].termination_reason == ('lane-phase-overdue')
        assert _spawn_health(supervisor, owner.worker_id) == first_health
        assert supervisor._park_evidence_by_group == first_evidence
        assert kill_calls == [None]
    finally:
        supervisor._close()


def test_supervisor_replacement_rebuilds_evidence_after_old_lock_and_boot_change(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
    config = replace(
        loaded,
        daemon=replace(
            loaded.daemon,
            supervisor_poll_interval_secs=0.01,
        ),
    )
    evidence = _park_evidence(observed_at_wall_clock=1000.0)
    old_clock = FakeClock(1000.0)
    dependent_spec = next(
        spec for spec in build_worker_specs(config.sites['test']) if not spec.sources
    )
    policy = config.sites['test'].workers['pool']
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
        breaker = state.restore_worker_breaker(
            worker_id=dependent_spec.worker_id,
            restart_limit=policy.restart_limit,
            restart_window_secs=policy.restart_window_secs,
            healthy_reset_secs=policy.healthy_reset_secs,
            clock=old_clock,
            boot_id='boot-before-supervisor-crash',
        )
        assert breaker.record_failure('replacement interrupted by supervisor death') == 1.0
        state.persist_worker_breaker(breaker)
    _create_recovery_group_database(config)
    group_lock_path = group_lock(source_database(config.daemon.state_dir), 'remote')
    ready_read, ready_write = os.pipe()
    release_read, release_write = os.pipe()
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
import threading
from pathlib import Path
from sg_event_daemon._platform import acquire_lock

with acquire_lock(Path(sys.argv[1])):
    os.write(int(sys.argv[2]), b'L')
    os.read(int(sys.argv[3]), 1)
""",
                os.fspath(group_lock_path),
                str(ready_write),
                str(release_read),
            ],
            pass_fds=(ready_write, release_read),
            env=_child_environment(),
        )
        os.close(ready_write)
        ready_write = -1
        factory = ScriptedWorkerFactory()
        new_clock = FakeClock(5.0)
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
        observed = _release_observed_worker_lock(
            monkeypatch, group_lock_path, release_write, old_worker
        )
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
                worker for worker in factory.instances.values() if not worker.spec.sources
            )
            assert owner.assignment is not None
            delivery = owner.assignment.group_assignments[0]
            assert isinstance(delivery, DeliveryGroupAssignment)
            assert delivery.conditional_park == evidence.directive
            assert observed == [group_lock_path]
    finally:
        os.write(release_write, b'R')
        os.close(ready_read)
        os.close(release_read)
        os.close(release_write)
        if ready_write >= 0:
            os.close(ready_write)
        if old_worker is not None:
            old_status = old_worker.wait()
    assert old_status == 0


def test_supervisor_replacement_worker_retains_independent_failure_fence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        old = next(worker for worker in factory.instances.values() if worker.spec.groups)
        old_kills: list[None] = []
        old.on_kill = lambda: old_kills.append(None)
        fail = cast(
            'Callable[[ScriptedWorker, str], None]',
            vars(Supervisor)['_fail_current_worker'].__get__(supervisor),
        )
        fail(old, 'lane-phase-overdue')
        supervisor._tick()
        clock.advance(1.0)
        for _ in range(4):
            supervisor._tick()
            replacement = factory.instances[old.worker_id]
            if replacement is not old and replacement.assignment is not None:
                break
        else:
            pytest.fail('replacement did not become ready')
        replacement_kills: list[None] = []
        replacement.on_kill = lambda: replacement_kills.append(None)

        fail(old, 'stale-old-handle')
        fail(replacement, 'operator-phase-overdue')

        assert old_kills == [None]
        assert replacement_kills == [None]
        assert supervisor._spawns[old.worker_id].termination_reason == ('operator-phase-overdue')
    finally:
        supervisor._close()


def test_supervisor_replaces_spawn_bound_grant_before_dependent_restart_assignment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
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
        clock=FakeClock(),
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
        source_worker = factory.instances[source.worker_id]
        old_dependent = factory.instances[dependent.worker_id]
        old_assignment = old_dependent.assignment
        assert old_assignment is not None

        with pytest.raises(ValueError, match='source-owning worker restart'):
            supervisor._replace_worker(source.worker_id)
        assert factory.instances[source.worker_id] is source_worker

        supervisor._replace_worker(dependent.worker_id)

        new_dependent = factory.instances[dependent.worker_id]
        new_assignment = new_dependent.assignment
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
        assert source_worker.sent_grants == [
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
        assert factory.operations[-6:] == [
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    group_path = source_database(config.daemon.state_dir)
    with _lifecycle_group(group_path):
        pass
    factory = ScriptedWorkerFactory()
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        owner = next(
            worker for worker in factory.instances.values() if 'remote' in worker.spec.groups
        )
        responses = iter(
            (
                ('probe', {'disposition_required': True}),
                ('accepted', {'group': 'remote', 'group_generation': 2}),
                ('recorded', {'group': 'remote', 'group_generation': 2}),
            )
        )
        owner.operator_handler = lambda sent: CommandResult(
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
        for sent in owner.sent_commands
    ] == [(1, 1), (1, 2), (2, 2)]


def test_supervisor_resets_worker_breaker_with_peer_audit_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[1]
    breaker = _WorkerBreaker(
        worker_id=spec.worker_id,
        restart_limit=0,
        restart_window_secs=100.0,
        healthy_reset_secs=100.0,
    )
    breaker.record_failure('ambiguous-worker-failure')
    identity = AuditIdentity(local_uid=501, username='operator', pid=456)
    with Supervisor(config=config, config_path=config_path) as supervisor:
        state = cast('_SupervisorState', vars(supervisor)['_state'])
        state.persist_worker_breaker(breaker)
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
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
        worker_id = dependent.worker_id
        stale_health = health_update(dependent, heartbeat_at=clock())
        stale_health_deadline = 123.0
        stale_restart_deadline = 456.0
        supervisor._spawns[worker_id].health = stale_health
        supervisor._spawns[worker_id].health_deadline = stale_health_deadline
        supervisor._spawns[worker_id].termination_reason = 'stale-termination'
        vars(supervisor)['_restart_deadlines'][worker_id] = stale_restart_deadline
        spawn_observations: list[str] = []

        def spawn_replacement(
            **options: Unpack[_ScriptedWorkerOptions],
        ) -> ScriptedWorker:
            spec = options['spec']
            if spec.worker_id == worker_id:
                spawn_observations.append(spec.worker_id)
                assert worker_id not in supervisor._spawns
                assert worker_id not in vars(supervisor)['_restart_deadlines']
                assert dependent.close_calls == 1
            return factory(**options)

        vars(supervisor)['_worker_factory'] = spawn_replacement
        factory.authentication_ready = True

        supervisor._replace_worker(worker_id)

        replacement = factory.instances[worker_id]
        assert replacement is not dependent
        assert spawn_observations == [worker_id]
        assert dependent.close_calls == 1
        assert supervisor._spawns[worker_id].health is not stale_health
        assert supervisor._spawns[worker_id].health_deadline != stale_health_deadline
        assert supervisor._spawns[worker_id].termination_reason is None
        assert worker_id not in vars(supervisor)['_restart_deadlines']
        stale_ready = EventPeerReady(
            source='admin',
            group='remote',
            dependent_spawn_instance=dependent.spawn_instance,
        )
        assert stale_ready in vars(supervisor)['_stale_grant_readies']
        assert source.sent_grants[-1].dependent_spawn_instance == replacement.spawn_instance
    finally:
        supervisor._close()


def test_supervisor_restart_fails_closed_while_old_child_is_unreaped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        clock=FakeClock(),
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        dependent.shutdown_status = None

        with pytest.raises(RuntimeError, match='unreaped'):
            supervisor._replace_worker(dependent.worker_id)

        assert len(factory.history[dependent.worker_id]) == 1
        dependent.shutdown_status = 0
    finally:
        supervisor._close()


def test_supervisor_restart_rebuilds_directive_for_fresh_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    evidence = _park_evidence()
    with _SupervisorState.open(config.daemon.state_dir) as state:
        assert state.record_park_evidence(evidence)
    _create_recovery_group_database(config)
    assignments: list[tuple[str, ConditionalParkDirective]] = []

    for supervisor_number in (1, 2):
        factory = ScriptedWorkerFactory()
        token_index = 0

        def token(*, number: int = supervisor_number) -> str:
            nonlocal token_index
            token_index += 1
            return f'supervisor-{number}-identity-{token_index}'

        with Supervisor(
            clock=FakeClock(),
            config=config,
            config_path=config_path,
            worker_factory=factory,
            token_factory=token,
        ) as supervisor:
            supervisor._reconcile(
                generations={('test', 'remote'): 1},
            )
            supervisor.start()
            owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
            assert owner.assignment is not None
            delivery = next(
                item for item in owner.assignment.group_assignments if item.group == 'remote'
            )
            assert isinstance(delivery, DeliveryGroupAssignment)
            assert delivery.conditional_park is not None
            assignments.append(
                (
                    owner.spawn_instance,
                    delivery.conditional_park,
                )
            )

    assert assignments[0][0] != assignments[1][0]
    assert assignments[0][1] == assignments[1][1] == evidence.directive


def test_supervisor_restart_resumes_after_group_full_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        clock=FakeClock(),
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        group_path = source_database(config.daemon.state_dir)

        with _lifecycle_group(group_path):
            supervisor._replace_worker(dependent.worker_id)

        restarted = factory.instances[dependent.worker_id]
        assert restarted.assignment is not None
        assert restarted.assignment.group_assignments == (
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
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
            (worker for worker in factory.instances.values() if not worker.spec.sources),
            key=lambda worker: worker.worker_id,
        )
        first_id, second_id = (worker.worker_id for worker in dependents)
        dependents[0].exit_status = 1
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=clock()))

        supervisor._tick()
        assert len(factory.history[first_id]) == 1
        clock.advance(1.0)
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        assert len(factory.history[first_id]) == 2

        factory.instances[first_id].exit_status = 1
        factory.instances[second_id].exit_status = 1
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=clock()))
        supervisor._tick()

        clock.advance(1.0)
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        assert len(factory.history[first_id]) == 2
        assert len(factory.history[second_id]) == 2

        clock.advance(1.0)
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=clock()))
        supervisor._tick()
        assert len(factory.history[first_id]) == 3
    finally:
        supervisor._close()


def _lifecycle_probe_config(tmp_path: Path, *, health: float = 2.0, remote: bool = False) -> Path:
    return write_daemon_config(
        tmp_path,
        Topology('dedicated' if remote else 'default', ('remote',), health_interval_secs=health),
    )


def test_supervisor_routes_and_correlates_operator_command_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
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
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        owner = next(
            worker for worker in factory.instances.values() if 'remote' in worker.spec.groups
        )
        owner.messages.append(
            CommandResult(
                command='park',
                ok=True,
                message='stale',
                request_id='stale-request',
                result={'revision': 7},
            )
        )
        owner.operator_handler = lambda sent: CommandResult(
            command=sent.command,
            ok=True,
            message='accepted',
            request_id=sent.request_id,
            result={'revision': 8, 'status': 'parked'},
        )

        result = supervisor.dispatch_operator_command(command, timeout=1.0)

    assert len(owner.sent_commands) == 1
    sent = owner.sent_commands[0]
    assert sent.request_id != command.request_id
    assert sent.command == command.command
    assert sent.arguments == command.arguments
    assert sent.identity == command.identity
    assert result.request_id == command.request_id
    assert result.result == {'revision': 8, 'status': 'parked'}


@pytest.mark.parametrize('remote', [False, True], ids=['default', 'dedicated'])
def test_first_ever_group_waits_for_its_sources_first_query(tmp_path: Path, remote: bool) -> None:
    config = _lifecycle_probe_config(tmp_path, remote=remote)
    hold = tmp_path / 'hold-page'
    hold.touch()
    with running_supervisor(config, ChildPatches(lifecycle_probe=True, last_event_id=4)) as probe:
        probe.wait_for((tmp_path / 'page-entered').exists)
        probe.wait_for(
            lambda: (
                any(health.sources for health in probe.latest_health.values())
                and (
                    not remote
                    or any(
                        spawn.lifecycle is not None and spawn.lifecycle.authenticated
                        for spawn in probe.supervisor._spawns.values()
                        if not spawn.worker.spec.sources
                    )
                )
            )
        )
        _assert_no_lifecycle_charge(probe)
        status = probe.supervisor.probe_mapping()
        assert not status['readiness']
        assert not status['complete']
        assert {'stale', 'source-degraded'} <= set(cast('tuple[str, ...]', status['conditions']))
        assert not any(health.groups for health in probe.latest_health.values())
        source = next(
            spawn for spawn in probe.supervisor._spawns.values() if spawn.worker.spec.sources
        )
        assert source.health_inventory_complete
        assert probe.supervisor._startup_clock(source) is None
        if remote:
            dependent = next(
                spawn
                for spawn in probe.supervisor._spawns.values()
                if not spawn.worker.spec.sources
            )
            assert dependent.lifecycle is not None
            assert dependent.lifecycle.authenticated
            assert not dependent.lifecycle.assigned
            assert probe.supervisor._startup_clock(dependent) is None
        hold.unlink()
        probe.wait_for(lambda: bool(probe.supervisor.probe_mapping()['readiness']))
        assert probe.supervisor.probe_mapping()['complete']
        assert sum(len(health.groups) for health in probe.latest_health.values()) == 1
        _assert_no_lifecycle_charge(probe)


def test_supervisor_run_timeout_wake_does_not_advance_progress(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('control')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        clock.now = 10.05

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
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        source.authentication_ready = True
        supervisor._tick()
        dependents = tuple(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        assert len(dependents) == 2
        source.messages_before_grant.append(health_update(source, heartbeat_at=clock()))
        for dependent in dependents:
            dependent.authentication_ready = True

        supervisor._tick()

        assert len(source.sent_grants) == 2
        assert not any(dependent.killed for dependent in dependents)
        first, second = source.sent_grants
        assert source.operations.index(
            f'receive:grant-ready:{first.group}'
        ) < source.operations.index(f'grant:{second.group}')
        assert (
            next(
                worker
                for worker in dependents
                if worker.spawn_instance == first.dependent_spawn_instance
            ).assignment
            is not None
        )
        assert (
            next(
                worker
                for worker in dependents
                if worker.spawn_instance == second.dependent_spawn_instance
            ).assignment
            is None
        )

        supervisor._tick()

        assert len(source.sent_grants) == 2
        assert all(dependent.assignment is not None for dependent in dependents)


def test_supervisor_source_exit_tolerates_delayed_dependent_grant_reap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
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
        assert len(source.sent_grants) == 1
        assert dependent.assignment is None
        dependent.wait_results.extend((None, None, -9))
        source.exit_status = 1

        supervisor._tick()

        source_workers = cast(
            'dict[tuple[str, str], ScriptedWorker]',
            vars(supervisor)['_source_workers'],
        )
        assert ('test', 'admin') not in source_workers
        assert dependent.killed
        assert (
            supervisor._spawns[dependent.worker_id].termination_reason
            == 'source-owner-unavailable'
        )
        assert dependent.worker_id in supervisor._spawns
        assert supervisor._spawns[dependent.worker_id].lifecycle is not None
        operations_after_source_exit = len(factory.operations)

        supervisor._tick()

        assert dependent.worker_id in supervisor._spawns
        assert supervisor._spawns[dependent.worker_id].lifecycle is not None
        assert factory.operations[operations_after_source_exit:] == []
        assert len(source.sent_grants) == 1
        assert dependent.assignment is None

        supervisor._tick()

        restart_deadlines = cast(
            'dict[str, float]',
            vars(supervisor)['_restart_deadlines'],
        )
        assert dependent.worker_id not in supervisor._spawns
        assert restart_deadlines[dependent.worker_id] == clock()
        assert len(factory.history[dependent.worker_id]) == 1

        factory.authentication_ready = True
        clock.advance(1.0)
        supervisor._tick()
        supervisor._tick()
        supervisor._tick()

        replacement = factory.instances[dependent.worker_id]
        replacement_source = next(
            worker for worker in factory.instances.values() if worker.spec.sources
        )
        assert replacement is not dependent
        assert replacement_source is not source
        assert replacement.assignment is not None
        assert replacement.assignment.spawn_instance != dependent.spawn_instance
        assert replacement.assignment.event_grants == tuple(replacement_source.sent_grants)
        assert dependent.assignment is None


def test_supervisor_source_startup_health_failure_uses_watchdog_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    supervisor = Supervisor(
        clock=FakeClock(), config=config, config_path=config_path, worker_factory=factory
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        source.auto_endpoint_ready = False
        source.authentication_ready = True
        supervisor._tick()
        assert not _spawn_lifecycle(supervisor, source.worker_id).ready
        source.messages.append(
            replace(
                health_update(source, heartbeat_at=0.0),
                worker=replace(
                    health_update(source, heartbeat_at=0.0).worker,
                    site='wrong-site',
                ),
            )
        )
        monkeypatch.setattr(source, 'wait', _never_reap)

        supervisor._tick()

        assert source.killed
        assert supervisor._spawns[source.worker_id].termination_reason == (
            'watchdog-sampling-failed'
        )
    finally:
        supervisor._close()


def test_supervisor_source_state_uses_configured_obligation_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    config = replace(config, daemon=replace(config.daemon, max_group_obligations=17))
    prepare_absent_orphan(config)
    supervisor = Supervisor(config=config, config_path=config_path)
    with ExitStack() as stack:
        sources = supervisor._open_reconciliation_sources(stack, {('test', 'admin')})
        assert sources[('test', 'admin')]._settings.max_group_obligations == 17


def test_supervisor_source_watchdog_uses_trusted_deadline_at_equality(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('phase', 'coordination')
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
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
                worker_instance=source.spawn_instance,
                phase='idle',
                heartbeat_at=0.0,
                deadline_at=None,
            ),
        )
        source.messages.append(
            health_update(
                source,
                heartbeat_at=clock(),
                sources=(source_detail,),
                watchdogs=watchdogs,
            )
        )
        dependent.messages.append(health_update(dependent, heartbeat_at=clock()))
        supervisor._tick()

        credential = config.sites['test'].credentials['admin']
        trusted_budget = internal_stall_deadline_secs(
            config.daemon.health_interval_secs,
            credential,
        )
        supervisor._spawns[source.worker_id].health_deadline = 10_000.0
        clock.advance(trusted_budget)
        source.messages.append(
            health_update(
                source, heartbeat_at=clock() - 0.001, sources=(source_detail,), watchdogs=watchdogs
            )
        )
        dependent.messages.append(health_update(dependent, heartbeat_at=clock()))
        supervisor._tick()
        assert not source.killed
        assert _spawn_health(supervisor, source.worker_id).sources[0].overdue_reason is None
        source.messages.append(
            health_update(
                source, heartbeat_at=clock(), sources=(source_detail,), watchdogs=watchdogs
            )
        )
        dependent.messages.append(health_update(dependent, heartbeat_at=clock()))
        monkeypatch.setattr(source, 'wait', _never_reap)

        supervisor._tick()

        assert source.killed
        assert not dependent.killed
        assert supervisor._spawns[source.worker_id].termination_reason == ('source-phase-overdue')
        accepted = _spawn_health(supervisor, source.worker_id)
        assert accepted.watchdogs[0].deadline_at == trusted_budget
        assert accepted.sources[0].deadline_at == trusted_budget
        assert accepted.sources[0].heartbeat_age == trusted_budget
        assert accepted.sources[0].overdue_reason == 'source-phase-overdue'
    finally:
        supervisor._close()


def test_supervisor_started_during_a_hold_waits_and_warns_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = source_only_config(tmp_path, monkeypatch)
    with _SupervisorState.open(tmp_path / 'state') as state:
        with _outside_supervisor_writer(state):
            context = multiprocessing.get_context('spawn')
            parent, child = context.Pipe()
            process = context.Process(target=_startup_hold_child, args=(str(config), child))
            process.start()
            child.close()
            try:
                assert parent.poll(None)
                assert parent.recv() == 'opened'
                assert parent.recv() == 'waiting'
                log = (tmp_path / 'startup.log').read_text()
                assert log.count('startup is waiting') == 1
            except BaseException:
                process.kill()
                process.join()
                parent.close()
                raise
        try:
            parent.send('released')
            assert parent.recv() == 'reconciled'
            process.join()
            assert process.exitcode == 0
            log = (tmp_path / 'startup.log').read_text()
            assert log.count('startup is waiting') == 1
            assert log.count('write began after waiting') == 1
        finally:
            if process.is_alive():
                process.kill()
            process.join()
            parent.close()


def test_supervisor_starts_source_then_installs_grant_before_dependent_assignment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    tokens = iter(
        (
            'source-generation-1',
            'source-spawn-1',
            'dependent-spawn-1',
            'dependent-capability-1',
        )
    )
    supervisor = Supervisor(
        clock=FakeClock(),
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

        assert factory.operations == [
            'spawn:site',
            'assign:site',
            'receive:endpoint:admin',
            'spawn:named',
            'grant:remote',
            'receive:grant-ready:remote',
            'assign:named',
        ]
        source, dependent = build_worker_specs(config.sites['test'])
        source_assignment = factory.instances[source.worker_id].assignment
        dependent_assignment = factory.instances[dependent.worker_id].assignment
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    clock = FakeClock()
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

        assert factory.operations == ['spawn:site']
        source = next(iter(factory.instances.values()))
        assert source.assignment is None
        assert source.receive_timeouts == []

        factory.authentication_ready = True
        source.authentication_ready = True
        supervisor._tick()
        supervisor._tick()

        assert factory.operations == [
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
            for worker in factory.instances.values()
            for timeout in worker.receive_timeouts
        )
    finally:
        supervisor._close()


@pytest.mark.parametrize('buffered', ['hello', 'ready-and-health', 'partial-health'])
def test_supervisor_gap_before_its_first_pass_contains_nothing(
    tmp_path: Path, buffered: str
) -> None:
    config = _lifecycle_probe_config(tmp_path)
    with running_supervisor(
        config, ChildPatches(control_probe=True, lifecycle_probe=True, last_event_id=4)
    ) as probe:
        worker = _first_lifecycle_worker(probe)
        _wait_lifecycle_file(tmp_path / f'{worker.spawn_instance}.hello-sent', worker=worker)
        if buffered != 'hello':
            if buffered == 'partial-health':
                (tmp_path / f'{worker.spawn_instance}.large').touch()
            _assign_lifecycle_source(probe, worker)
            _wait_lifecycle_file(tmp_path / f'{worker.spawn_instance}.wrote', worker=worker)
            _wait_lifecycle_file(
                tmp_path / f'{worker.spawn_instance}.endpoint-enqueued', worker=worker
            )
            if buffered == 'ready-and-health':
                _wait_lifecycle_file(
                    tmp_path / f'{worker.spawn_instance}.complete-health-offered', worker=worker
                )
        lifecycle = probe.supervisor._spawns[worker.worker_id].lifecycle
        assert lifecycle is not None
        startup_deadline = lifecycle.deadline_at
        if buffered == 'partial-health':
            _wait_control_event(tmp_path, worker, 'frame-started')
            sample = next(
                event
                for event in _control_events(tmp_path, worker)
                if event['kind'] == 'sample' and event['large']
            )
            lifecycle.deadline_at = cast('float', sample['sample']) + 0.001
        else:
            lifecycle.deadline_at = probe.supervisor._clock() - 1
        real_clock = probe.supervisor._clock
        after_deadline = lifecycle.deadline_at + 0.001
        probe.supervisor._clock = lambda: max(real_clock(), after_deadline)
        try:
            probe.supervisor._tick()
        finally:
            probe.supervisor._clock = real_clock
            # Preserve any fresh assignment budget, but retire the past marker.
            lifecycle.deadline_at = max(lifecycle.deadline_at, startup_deadline)
        _assert_no_lifecycle_charge(probe)
        _ready_control_probe(probe)
        _assert_no_lifecycle_charge(probe)


def test_supervisor_startup_does_not_normalize_noncontrol_oserror(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory(authentication_ready=False)
    supervisor = Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=iter(('source-generation-1', 'source-spawn-1')).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(iter(factory.instances.values()))
        source.authentication_ready = True

        def fail_registration(_worker: ScriptedWorker) -> None:
            raise OSError(errno.EACCES, 'simulated supervisor state failure')

        monkeypatch.setattr(supervisor, '_register_worker', fail_registration)

        with pytest.raises(OSError, match='supervisor state failure'):
            supervisor._tick()

        assert not source.killed
        assert supervisor._spawns[source.worker_id].termination_reason is None
    finally:
        supervisor._close()


def test_supervisor_startup_transaction_stops_between_one_second_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _SupervisorState.open(tmp_path) as state:
        attempts: list[int] = []
        stopped: list[bool] = []

        @contextmanager
        def busy(timeout_millis: int) -> Generator[sqlite3.Connection]:
            attempts.append(timeout_millis)
            cause = sqlite3.OperationalError('database is locked')
            cause.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise DatabaseError('database is locked') from cause
            yield state._database.connection

        def should_stop() -> bool:
            stopped.append(bool(attempts))
            return bool(attempts)

        monkeypatch.setattr(state, '_write_transaction', busy)
        with (
            pytest.raises(write_lock_module.WriteLockStoppedError, match='startup'),
            state._startup_transaction(should_stop),
        ):
            pytest.fail('startup acquired an outside-held database')
        assert attempts == [1000]
        assert stopped == [False, True]
        assert state._database.connection.execute('PRAGMA busy_timeout').fetchone()[0] == 5000


def test_supervisor_state_adopts_the_greatest_durable_group_generation(
    tmp_path: Path,
) -> None:
    with _SupervisorState.open(tmp_path / 'state') as state:
        assignments = state.reconcile_groups(
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
    clock = FakeClock()
    clock.advance(1_000.0)
    state_dir = tmp_path / 'state'
    with _SupervisorState.open(state_dir) as state:
        breaker = state.restore_worker_breaker(
            worker_id='dependent',
            restart_limit=3,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
            boot_id='boot-a',
        )
        assert breaker.record_failure('first failure') == 1.0
        state.persist_worker_breaker(breaker)

    clock.advance(0.25)
    with _SupervisorState.open(state_dir) as state:
        same_boot = state.restore_worker_breaker(
            worker_id='dependent',
            restart_limit=3,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
            boot_id='boot-a',
        )
        assert same_boot.projection.failure_times == (1_000.0,)
        assert same_boot.restart_deadline_at == 1_001.0

    clock.now = 5.0
    with _SupervisorState.open(state_dir) as state:
        rebooted = state.restore_worker_breaker(
            worker_id='dependent',
            restart_limit=3,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
            boot_id='boot-b',
        )
        assert rebooted.projection.failure_times == (5.0,)
        assert rebooted.restart_deadline_at == 5.0
        assert rebooted.record_failure('second failure') == 2.0
        assert rebooted.projection.failure_times == (5.0, 5.0)
        state.persist_worker_breaker(rebooted)

    clock.advance(0.5)
    with _SupervisorState.open(state_dir) as state:
        same_new_boot = state.restore_worker_breaker(
            worker_id='dependent',
            restart_limit=3,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
            boot_id='boot-b',
        )
        assert same_new_boot.projection.failure_times == (5.0, 5.0)
        assert same_new_boot.restart_deadline_at == 7.0


def test_supervisor_state_persists_initialize_then_reconciles_resume(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    with _SupervisorState.open(state_dir) as state:
        first = state.reconcile_groups(
            configured={('test', 'remote'): 'admin'},
            generations={},
        )

    with _SupervisorState.open(state_dir) as state:
        resumed = state.reconcile_groups(
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
    clock = FakeClock()
    state_dir = tmp_path / 'state'
    with _SupervisorState.open(state_dir) as state:
        breaker = _WorkerBreaker(
            worker_id='dependent',
            restart_limit=2,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert breaker.record_failure('first failure') == 1.0
        state.persist_worker_breaker(breaker)

    clock.advance(0.5)
    with _SupervisorState.open(state_dir) as state:
        restored = state.restore_worker_breaker(
            worker_id='dependent',
            restart_limit=2,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert len(restored._failure_times) == 1
        assert restored.restart_deadline_at == 1.0
        assert restored._failure_total == 1
        assert restored.revision == 0
        assert restored.history == ()
        assert restored.record_failure('second failure') == 2.0
        state.persist_worker_breaker(restored)

    clock.advance(2.0)
    with _SupervisorState.open(state_dir) as state:
        restored = state.restore_worker_breaker(
            worker_id='dependent',
            restart_limit=2,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert len(restored._failure_times) == 2
        assert restored.restart_deadline_at == 2.5
        assert restored._failure_total == 2
        assert restored.record_failure('third failure') is None
        state.persist_worker_breaker(restored)

    with _SupervisorState.open(state_dir) as state:
        restored = state.restore_worker_breaker(
            worker_id='dependent',
            restart_limit=2,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert restored.is_open
        assert restored.restart_deadline_at is None
        assert restored._failure_total == 3
        assert restored.revision == 1
        assert restored.transition_count == 1
        assert restored.history == (
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

    with _SupervisorState.open(state_dir) as state:
        assert state.record_park_evidence(original)
        assert not state.record_park_evidence(repacked)
        assert state.pending_park_evidence() == (original,)

    with _SupervisorState.open(state_dir) as state:
        assert state.pending_park_evidence() == (original,)
        state.retire_park_evidence(original.directive.evidence_id)
        assert state.pending_park_evidence() == ()


def test_supervisor_state_retains_latest_100_durable_breaker_transitions(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    state_dir = tmp_path / 'state'
    identity = AuditIdentity.current()
    with _SupervisorState.open(state_dir) as state:
        breaker = _WorkerBreaker(
            worker_id='dependent',
            restart_limit=0,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        for index in range(101):
            assert breaker.record_failure(f'failure {index}') is None
            state.persist_worker_breaker(breaker)
            state.reset_worker_breaker(
                breaker,
                expected_revision=breaker.revision,
                reason=f'reset {index}',
                identity=identity,
                wall_clock_time=float(index),
            )

    with _SupervisorState.open(state_dir) as state:
        restored = state.restore_worker_breaker(
            worker_id='dependent',
            restart_limit=0,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )

    assert not restored.is_open
    assert restored._failure_total == 101
    assert restored.revision == 202
    assert restored.transition_count == 202
    assert len(restored.history) == 100
    assert restored.history[0].sequence == 103
    assert restored.history[-1].sequence == 202


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
    with _SupervisorState.open(state_dir) as state:
        assert state.record_park_evidence(later_group)
        assert state.record_park_evidence(earlier_group)

    with _SupervisorState.open(state_dir) as state:
        assert state.pending_park_evidence() == (earlier_group, later_group)

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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    state_dir = config.daemon.state_dir
    source_path = state_dir / 'sites' / 'test' / 'sources' / 'admin.db'
    state_dir.mkdir(mode=0o700)
    source_path.parent.mkdir(parents=True, mode=0o700)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
        source_owner = next(worker for worker in factory.instances.values() if worker.spec.sources)
        group_owner = next(worker for worker in factory.instances.values() if worker.spec.groups)
        source_owner.messages.append(
            health_update(
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
                        worker_instance=source_owner.spawn_instance,
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
        group_owner.messages.append(
            health_update(
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        clock.advance(1.24)
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        detail = _idle_source_health(source, source='admin', heartbeat_at=0.0)
        source.messages.append(
            health_update(
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
                        worker_instance=source.spawn_instance,
                        phase='idle',
                        heartbeat_at=clock(),
                        deadline_at=None,
                    ),
                ),
            )
        )
        dependent.messages.append(health_update(dependent, heartbeat_at=clock()))
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    tokens = iter(
        (
            'source-generation-1',
            'source-spawn-1',
            'dependent-spawn-1',
            'dependent-capability-1',
        )
    )

    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
        token_factory=lambda: next(tokens),
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()

    assert factory.operations == [
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


@pytest.mark.parametrize('health', [2.0, 10.0])
def test_monitor_killed_mid_first_frame_is_contained_at_the_later_of_deadline_and_silence(
    tmp_path: Path, health: float, stall_detectors: StallDetectors
) -> None:
    _killed_startup_monitor(tmp_path, stall_detectors, partial=True, health=health)


def test_supervisor_tick_discards_health_from_an_old_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        stale = replace(
            health_update(dependent, heartbeat_at=1.0).worker,
            spawn_instance='old-dependent-spawn',
        )
        dependent.messages.append(
            HealthUpdate(
                worker=stale,
                watchdogs=(),
                sources=(),
                groups=(),
                event_sockets=(),
            )
        )

        supervisor._tick()

        assert supervisor._spawns[dependent.worker_id].health is None
        assert not dependent.killed
    finally:
        supervisor._close()


def test_supervisor_tick_discards_late_operator_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    with Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        owner = next(
            worker for worker in factory.instances.values() if 'remote' in worker.spec.groups
        )
        owner.messages.append(
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
        assert not owner.killed

        owner.messages.append(
            SourceEndpointReady(
                source='admin',
                source_generation='source-generation-1',
                path=os.fspath(tmp_path / 'late-source.sock'),
            )
        )
        with pytest.raises(TypeError, match='SourceEndpointReady'):
            supervisor._tick()


def test_supervisor_tick_does_not_attribute_worker_staleness_to_fresh_lane(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('heartbeat')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        group_path = source_database(config.daemon.state_dir)
        clock.advance(1.0)
        group_health = _in_flight_group_health(dependent, heartbeat_at=clock())
        for worker in factory.instances.values():
            worker.messages.append(
                health_update(
                    worker,
                    heartbeat_at=0.0 if worker is dependent else clock(),
                    groups=(group_health,) if worker is dependent else (),
                )
            )

        with _conditional_group(group_path):
            supervisor._tick()

        assert dependent.killed
        assert supervisor._park_evidence_by_group == {}
        assert len(factory.history[dependent.worker_id]) == 1
    finally:
        supervisor._close()


def test_supervisor_tick_nonblockingly_ingests_all_pending_worker_health(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
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
        workers = tuple(factory.instances.values())
        updates = tuple(health_update(worker, heartbeat_at=0.0) for worker in workers)
        for worker, update in zip(workers, updates, strict=True):
            worker.messages.extend((update, update))
            worker.receive_timeouts.clear()

        supervisor._tick()

        assert all(not worker.messages for worker in workers)
        assert all(worker.receive_timeouts == [0.0, 0.0] for worker in workers)
        assert {
            worker_id
            for worker_id, spawn in supervisor._spawns.items()
            if spawn.health is not None
        } == {update.worker.worker_id for update in updates}
        assert all(
            spawn.health.worker.spawn_instance == factory.instances[worker_id].spawn_instance
            for worker_id, spawn in supervisor._spawns.items()
            if spawn.health is not None
        )
    finally:
        supervisor._close()


def test_supervisor_tick_persists_worker_breaker_restart_window(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        dependent.exit_status = 1
        for worker in factory.instances.values():
            worker.messages.append(health_update(worker, heartbeat_at=clock()))

        supervisor._tick()

        policy = config.sites['test'].workers['pool']
        with _SupervisorState.open(config.daemon.state_dir) as state:
            restored = state.restore_worker_breaker(
                worker_id=dependent.worker_id,
                restart_limit=policy.restart_limit,
                restart_window_secs=policy.restart_window_secs,
                healthy_reset_secs=policy.healthy_reset_secs,
                clock=clock,
            )
        assert restored._failure_total == 1
        assert len(restored._failure_times) == 1
        assert restored.restart_deadline_at == 1.0
    finally:
        supervisor._close()


def test_supervisor_tick_publishes_snapshot_without_due_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
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


@pytest.mark.parametrize('health', [2.0, 10.0])
def test_monitor_killed_between_frames_is_contained_at_the_later_of_deadline_and_silence(
    tmp_path: Path, health: float, stall_detectors: StallDetectors
) -> None:
    _killed_startup_monitor(tmp_path, stall_detectors, partial=False, health=health)


def test_supervisor_trusted_wait_deadlines_clamp_retry_and_lane_idle(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('phase', 'coordination')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    supervisor = Supervisor(config=config, config_path=config_path)
    worker = ScriptedWorkerFactory()(
        config_path=config_path,
        spec=build_worker_specs(config.sites['test'])[0],
        spawn_instance='wait-spawn-1',
        max_frame_bytes=1_048_576,
        send_deadline_secs=0.1,
        startup_deadline_secs=2.0,
        shutdown_deadline_secs=2.0,
    )
    worker.assignment = _source_assignment(worker.spec, worker.spawn_instance)
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
        worker_instance=worker.spawn_instance,
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
    tolerance = watchdog_grace_secs(config.daemon.health_interval_secs)

    assert trusted(supervisor, source_watchdog, {'admin': ordinary}) == (
        10.0 + config.sites['test'].sources['admin'].fetch_interval_secs + tolerance
    )
    assert trusted(supervisor, source_watchdog, {'admin': retry}) == (10.0 + retry_cap + tolerance)
    assert trusted(supervisor, lane_watchdog, {}) == 10.0 + retry_cap + tolerance
    with pytest.raises(ValueError, match='retry wait'):
        trusted(
            supervisor, source_watchdog, {'admin': replace(retry, next_retry_secs=float('inf'))}
        )


@pytest.mark.parametrize(
    'case',
    [(10.51, 10.15, False), (10.10, 10.10, True), (10.51, 10.51, True)],
    ids=['10.51-10.15-False', '10.1-10.1-True', '10.51-10.51-True'],
)
def test_operator_wait_keeps_the_grace_floor(
    stall_detectors: StallDetectors,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: tuple[float, float, bool],
) -> None:
    child_deadline, observed_at, overdue = case
    stall_detectors.enable('phase', 'coordination')
    config_path, loaded = source_only_config(tmp_path, monkeypatch)
    site = loaded.sites['test']
    site = replace(
        site,
        credentials=MappingProxyType(
            {
                'admin': replace(
                    site.credentials['admin'],
                    timeout_secs=0.05,
                    max_rpc_attempts=1,
                    rpc_attempt_interval_secs=0.0,
                ),
            }
        ),
    )
    config = replace(
        loaded,
        daemon=replace(loaded.daemon, health_interval_secs=0.01),
        sites=MappingProxyType({'test': site}),
    )
    supervisor = Supervisor(config=config, config_path=config_path)
    watchdog = WatchdogHealth(
        kind='operator',
        site='test',
        source='admin',
        group=None,
        worker_instance='spawn',
        phase='internal',
        heartbeat_at=10.0,
        deadline_at=child_deadline,
    )
    deadline = supervisor._trusted_watchdog_deadline(watchdog, {})
    assert supervisor._phase_confirmed_overdue(observed_at, deadline) is overdue


def test_supervisor_watchdog_sampling_failure_kills_complete_spawn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        source.messages.append(health_update(source, heartbeat_at=0.0))
        supervisor._tick()
        source.messages.append(
            replace(
                health_update(
                    source,
                    heartbeat_at=0.0,
                    sources=(),
                    groups=(),
                    watchdogs=(),
                ),
                worker=replace(
                    health_update(source, heartbeat_at=0.0).worker,
                    watchdog_failure='watchdog-sampling-failed',
                ),
            )
        )
        monkeypatch.setattr(source, 'wait', _never_reap)

        supervisor._tick()

        assert source.killed
        assert supervisor._spawns[source.worker_id].termination_reason == (
            'watchdog-sampling-failed'
        )
        assert not _spawn_health(supervisor, source.worker_id).worker.monitor_alive
    finally:
        supervisor._close()


def test_supervisor_worker_control_eof_is_current_worker_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    supervisor = Supervisor(
        config=config,
        config_path=config_path,
        clock=FakeClock(),
        worker_factory=factory,
        token_factory=iter(
            ('source-generation-1', 'source-spawn-1', 'dependent-spawn-1', 'capability-1')
        ).__next__,
    )
    try:
        supervisor._open()
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        source.messages.append(health_update(source, heartbeat_at=0.0))
        dependent.messages.append(health_update(dependent, heartbeat_at=0.0))
        supervisor._tick()

        failures: list[str] = []

        def closed() -> Iterator[ControlMessage]:
            yield health_update(dependent, heartbeat_at=0.0)
            failures.append('eof')
            raise EOFError('closed test control')

        monkeypatch.setattr(dependent, 'receive_available', closed)
        monkeypatch.setattr(dependent, 'wait', _never_reap)
        supervisor._tick()

        assert failures == ['eof']
        assert dependent.killed
        assert supervisor._spawns[dependent.worker_id].termination_reason == ('worker-control-eof')
    finally:
        supervisor._close()


def test_supervisor_worker_control_send_budget_tracks_health_interval(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('control')
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'test-boot')
    config_path, config = source_only_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()

    with Supervisor(
        clock=FakeClock(),
        config=config,
        config_path=config_path,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()

    assert factory.send_deadlines
    assert set(factory.send_deadlines) == {2 * config.daemon.health_interval_secs}


def test_takeover_write_lock_probe_leaves_file_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch)
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


@contextmanager
def _socket_worker_channel(
    worker: ScriptedWorker,
) -> Generator[tuple[WorkerControl, socket.socket]]:
    receiver, sender = socket.socketpair()
    control = WorkerControl(receiver, max_frame_bytes=1_048_576)
    assert not worker.messages
    worker.control = control
    try:
        yield control, sender
    finally:
        worker.control = None
        control.close()
        sender.close()


@contextmanager
def _worker_silence_harness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[tuple[Supervisor, ScriptedWorker, FakeClock, WorkerControl, socket.socket]]:
    config_path, config = _default_packed_config(tmp_path, monkeypatch)
    config = replace(config, daemon=replace(config.daemon, health_interval_secs=2.0))
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    monkeypatch.setattr(ipc_module, 'time', SimpleNamespace(monotonic=clock))
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        for _ in range(3):
            supervisor._advance_worker_messages()
        (worker,) = factory.instances.values()
        clock.now = 100.0
        with _socket_worker_channel(worker) as (control, sender):
            yield supervisor, worker, clock, control, sender


def test_waiting_grant_accepts_source_batch_before_recovery(
    stall_detectors: StallDetectors, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    stall_detectors.enable('heartbeat')
    config_path, config = _local_and_remote_config(tmp_path, monkeypatch)
    factory = ScriptedWorkerFactory()
    clock = FakeClock()
    with Supervisor(
        config=config, config_path=config_path, clock=clock, worker_factory=factory
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        source = next(worker for worker in factory.instances.values() if worker.spec.sources)
        dependent = next(
            worker for worker in factory.instances.values() if not worker.spec.sources
        )
        assert dependent.assignment is not None
        assert not supervisor._accept_worker_health(source, health_update(source, heartbeat_at=0))
        evidence = _park_evidence(group='local')
        state = _stage_source_recovery(supervisor, source, evidence)
        supervisor._spawns[dependent.worker_id].lifecycle = supervisor_module._WorkerLifecycle(
            step_started_at=0.0,
            assignment=dependent.assignment,
            deadline_at=0.1,
            worker=dependent,
            authenticated=True,
            grant_sent=True,
        )
        grant = dependent.assignment.event_grants[0]
        late_ready = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=dependent.spawn_instance,
        )
        with _socket_worker_channel(source) as (control, sender):
            monkeypatch.setattr(ipc_module, 'time', SimpleNamespace(monotonic=clock))
            for message in (
                health_update(source, heartbeat_at=0.5),
                late_ready,
                _recovery_ready(source, evidence),
            ):
                sender.sendall(ipc_module.encode_control_message(message))
            dependent.assignment = None
            acquisitions = source.batch_acquisitions
            clock.now = 0.5
            supervisor._advance_worker_messages()
            assert not dependent.killed
            assert not source.killed
            assert source.batch_acquisitions == acquisitions + 1
            assert ipc_module.pending_socket_bytes(control.open_socket()) == 0
            assert _spawn_health(supervisor, source.worker_id).worker.heartbeat_at == 0.5
            assert late_ready not in supervisor._stale_grant_readies
            assert state.pending_park_evidence() == ()

            # A fresh waiting grant owns the source through a delayed finite batch.
            _stage_post_startup_grant(supervisor, source, dependent)
            clock.now = 100
            sender.sendall(
                ipc_module.encode_control_message(health_update(source, heartbeat_at=100))
            )

            def inject() -> None:
                clock.now = 112
                sender.sendall(
                    ipc_module.encode_control_message(health_update(source, heartbeat_at=111.99))
                )

            _delay_worker_batch(monkeypatch, source, inject, timing='iteration')
            acquisitions = source.batch_acquisitions
            supervisor._advance_worker_messages()
            assert not source.killed
            assert control.last_arrival_at == 100
            assert control.has_pending_input()
            assert source.batch_acquisitions == acquisitions + 1
            supervisor._advance_worker_messages()
            assert source.batch_acquisitions == acquisitions + 2
            assert control.last_arrival_at == 112
            assert ipc_module.pending_socket_bytes(control.open_socket()) == 0
            assert not source.killed
            assert _spawn_health(supervisor, source.worker_id).worker.heartbeat_at == 111.99


@pytest.mark.parametrize('owner', ['ordinary', 'operator'])
@pytest.mark.parametrize('timing', ['iteration', 'after-eof-peek', 'empty-entry'])
def test_delayed_worker_silence_defers_only_waiting_input(
    stall_detectors: StallDetectors,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner: str,
    timing: str,
) -> None:
    stall_detectors.enable('heartbeat')
    with _worker_silence_harness(tmp_path, monkeypatch) as (
        supervisor,
        worker,
        clock,
        control,
        sender,
    ):
        sender.sendall(ipc_module.encode_control_message(health_update(worker, heartbeat_at=100)))
        if timing == 'empty-entry':
            _silence_pass(supervisor, worker, owner)
            clock.now = 111.0
        spawn = supervisor._spawns[worker.worker_id]
        breaker = supervisor._breakers[worker.worker_id]
        before = breaker.projection
        acquisitions = worker.batch_acquisitions
        observations: list[tuple[HealthUpdate | None, HealthUpdate | None]] = []
        contain = supervisor._contain_current_worker
        probe = Mock(wraps=worker.has_pending_input)
        monkeypatch.setattr(worker, 'has_pending_input', probe)

        def observe(
            current: supervisor_module._WorkerHandle,
            reason: str,
            *,
            watchdog_failure: bool = False,
        ) -> bool:
            health = spawn.health
            result = contain(current, reason, watchdog_failure=watchdog_failure)
            if current is worker:
                observations.append((health, spawn.health))
            return result

        monkeypatch.setattr(supervisor, '_contain_current_worker', observe)

        def inject() -> None:
            clock.now = 112.0
            sender.sendall(
                ipc_module.encode_control_message(health_update(worker, heartbeat_at=111.99))
            )

        _delay_worker_batch(monkeypatch, worker, inject, timing=timing)
        _silence_pass(supervisor, worker, owner)
        assert not worker.killed
        assert worker.batch_acquisitions == acquisitions + 1
        assert probe.call_count == 1
        assert control.last_arrival_at == 100
        assert spawn.health_deadline == 110
        assert spawn.health_failure is None
        assert spawn.health is not None
        assert spawn.health.worker.heartbeat_at == 100
        assert observations
        assert observations[-1][0] is observations[-1][1]
        assert breaker.projection == before
        _silence_pass(supervisor, worker, owner)
        assert worker.batch_acquisitions == acquisitions + 2
        assert control.last_arrival_at == 112
        assert spawn.health_deadline == 122
        assert spawn.health is not None
        assert spawn.health.worker.heartbeat_at == 111.99
        assert not worker.killed
        clock.now = 121.999
        _silence_pass(supervisor, worker, owner)
        assert not worker.killed
        clock.now = 122.0
        _silence_pass(supervisor, worker, owner)
        assert worker.killed
        assert spawn.termination_reason == 'worker-heartbeat-overdue'


def test_worker_breaker_backoff_saturates_without_overflow() -> None:
    breaker = _WorkerBreaker(
        worker_id='dependent',
        restart_limit=1_100,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
        clock=lambda: 0.0,
    )

    delays = tuple(breaker.record_failure('ambiguous-worker-failure') for _ in range(1_100))

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
    breaker.record_failure('ambiguous-worker-failure')
    identity = AuditIdentity.current()
    monkeypatch.setattr(supervisor_module, 'boot_identity', lambda: 'boot-1')

    with _SupervisorState.open(tmp_path / 'state') as state:
        state.persist_worker_breaker(breaker)
        transition = state.reset_worker_breaker(
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


@pytest.mark.parametrize('owner', ['ordinary', 'operator'])
@pytest.mark.parametrize('judgment', [110.0, 112.0])
def test_delayed_worker_silence_contains_empty_channel(
    stall_detectors: StallDetectors,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner: str,
    judgment: float,
) -> None:
    stall_detectors.enable('heartbeat')
    with _worker_silence_harness(tmp_path, monkeypatch) as (
        supervisor,
        worker,
        clock,
        control,
        sender,
    ):
        sender.sendall(ipc_module.encode_control_message(health_update(worker, heartbeat_at=100)))
        _delay_worker_batch(
            monkeypatch, worker, lambda: setattr(clock, 'now', judgment), timing='iteration'
        )
        _silence_pass(supervisor, worker, owner)
        assert worker.killed
        assert control.last_arrival_at == 100
        assert (
            supervisor._spawns[worker.worker_id].termination_reason == 'worker-heartbeat-overdue'
        )


def test_worker_breaker_reset_requires_revision_and_persists_audit(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    breaker = _WorkerBreaker(
        worker_id='dependent',
        restart_limit=1,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
        clock=clock,
    )
    breaker.record_failure('ambiguous-worker-failure')
    breaker.record_failure('ambiguous-worker-failure')
    stale = _WorkerBreaker(
        worker_id='dependent',
        restart_limit=1,
        restart_window_secs=100.0,
        healthy_reset_secs=10.0,
        clock=clock,
    )
    stale.record_failure('ambiguous-worker-failure')
    stale.record_failure('ambiguous-worker-failure')
    assert breaker.is_open
    assert breaker.revision == 1
    identity = AuditIdentity.current()
    state_dir = tmp_path / 'state'

    with _SupervisorState.open(state_dir) as state:
        state.persist_worker_breaker(breaker)
        with pytest.raises(ValueError, match='revision'):
            state.reset_worker_breaker(
                breaker,
                expected_revision=0,
                reason='operator reviewed shared worker',
                identity=identity,
                wall_clock_time=20.0,
            )
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 0

        transition = state.reset_worker_breaker(
            breaker,
            expected_revision=1,
            reason='operator reviewed shared worker',
            identity=identity,
            wall_clock_time=21.0,
        )
        assert transition.state == 'closed'
        assert transition.reason == 'operator reviewed shared worker'
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 1
        assert state.audit_count() == 1
        with pytest.raises(ValueError, match='revision'):
            state.reset_worker_breaker(
                stale,
                expected_revision=1,
                reason='stale competing reset',
                identity=identity,
                wall_clock_time=22.0,
            )
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 1

    with _SupervisorState.open(state_dir) as state:
        assert _breaker_audit_rows(state, 'dependent') == [(1,)] * 1
        restored = state.restore_worker_breaker(
            worker_id='dependent',
            restart_limit=1,
            restart_window_secs=100.0,
            healthy_reset_secs=10.0,
            clock=clock,
        )
        assert not restored.is_open
        assert restored.revision == 2
        assert restored.transition_count == 2
    assert not breaker.is_open
    assert breaker.revision == 2


def test_worker_breakers_use_independent_fake_clock_backoff_and_healthy_decay() -> None:
    clock = FakeClock()
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

    assert first.record_failure('ambiguous-worker-failure') == 1.0
    clock.advance(1.0)
    assert first.record_failure('ambiguous-worker-failure') == 2.0
    assert second.record_failure('ambiguous-worker-failure') == 1.0
    clock.advance(1.0)
    assert first.record_failure('ambiguous-worker-failure') is None
    assert first.is_open
    assert not second.is_open

    clock.advance(10.0)
    assert second.record_healthy()
    assert len(second._failure_times) == 0
    assert first.is_open


def test_worker_instance_authenticates_assigns_and_exits_on_control_eof(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
    (tmp_path / 'sg_event_daemon.py').write_text(
        "raise RuntimeError('worker imported the current directory')\n", encoding='utf-8'
    )
    monkeypatch.chdir(tmp_path)
    spec = build_worker_specs(config.sites['test'])[0]
    spawn_instance = 'source-spawn-1'
    spawn_calls: list[tuple[tuple[str, ...], tuple[int, ...]]] = []

    def record_spawn(
        argv: list[str],
        *,
        pass_fds: tuple[int, ...] = (),
        env: Mapping[str, str] | None = None,
    ) -> ChildProcess:
        spawn_calls.append((tuple(argv), pass_fds))
        return _spawn_worker_with_stall_policy(argv, pass_fds=pass_fds, env=env)

    monkeypatch.setattr(supervisor_module, 'spawn_child', record_spawn)
    worker = _WorkerInstance.spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance=spawn_instance,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=stall_deadline('control', 2 * config.daemon.health_interval_secs),
        startup_deadline_secs=config.daemon.worker_startup_deadline_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        env=_child_environment(),
    )
    try:
        assert worker._assigned_worker is None
        with pytest.raises(RuntimeError, match='authenticate before assignment'):
            worker.assign(_source_assignment(spec, spawn_instance))
        assert _authenticate_worker(worker)
        assert worker._assigned_worker is None
        worker.assign(_source_assignment(spec, spawn_instance))
        message = _receive_source_endpoint_ready(worker)
        assert message.source == 'admin'
        assert message.source_generation == 'source-generation-1'
        assert Path(message.path).is_socket()
        assert worker.worker_id == spec.worker_id
        assert worker.spawn_instance == spawn_instance
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

        worker.close()

        assert worker._process.wait() == 0
    finally:
        worker.kill()
        worker._process.wait()


def test_worker_instance_graceful_shutdown_reaps_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
    spec = build_worker_specs(config.sites['test'])[0]
    spawn_instance = 'source-spawn-graceful'
    monkeypatch.setattr(supervisor_module, 'spawn_child', _spawn_worker_with_stall_policy)
    worker = _WorkerInstance.spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance=spawn_instance,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=stall_deadline('control', 2 * config.daemon.health_interval_secs),
        startup_deadline_secs=config.daemon.worker_startup_deadline_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        env=_child_environment(),
    )
    try:
        assert _authenticate_worker(worker)
        worker.assign(_source_assignment(spec, spawn_instance))
        assert worker.shutdown('test-graceful-stop') == 0
        assert worker.wait(timeout=0.0) == 0
    finally:
        worker.kill()
        worker._process.wait()


def test_worker_instance_kills_and_reaps_a_child_that_ignores_clean_stop(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
    spec = build_worker_specs(config.sites['test'])[0]
    spawn_instance = 'source-spawn-stuck'
    spawner = _ControlChildSpawner(
        """
import os
import sys
import time
import threading
from tests._ipc_support import send_control
from sg_event_daemon._ipc import ControlHello, WorkerControl

control = WorkerControl.from_fd(
    int(sys.argv[1]),
    max_frame_bytes=8_388_608,
)
send_control(control,
    ControlHello(
        spawn_instance=sys.argv[2],
        worker_id=sys.argv[3],
        pid=os.getpid(),
    ),
)
threading.Event().wait()
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)
    worker = _WorkerInstance.spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance=spawn_instance,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=stall_deadline('control', 2 * config.daemon.health_interval_secs),
        startup_deadline_secs=config.daemon.worker_startup_deadline_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        env=_child_environment(),
    )

    try:
        assert _authenticate_worker(worker)
        worker.assign(_source_assignment(spec, spawn_instance))
        with stall_detectors.enabled('shutdown'):
            _expire_shutdown_grace(monkeypatch, worker)
            return_code = worker.shutdown('test-clean-stop')
    finally:
        worker.kill()
        worker._process.wait()
        worker.close()

    assert return_code is not None
    assert return_code != 0
    assert len(spawner.processes) == 1
    assert reap_child(spawner.processes[0]) == return_code


def test_worker_instance_rejects_mismatched_hello_and_reaps_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
    spec = build_worker_specs(config.sites['test'])[0]
    spawner = _ControlChildSpawner(
        """
import os
import sys
import time
import threading
from tests._ipc_support import send_control
from sg_event_daemon._ipc import ControlHello, WorkerControl

control = WorkerControl.from_fd(
    int(sys.argv[1]),
    max_frame_bytes=8_388_608,
)
send_control(control,
    ControlHello(
        spawn_instance='wrong-spawn',
        worker_id=sys.argv[3],
        pid=os.getpid(),
    ),
)
threading.Event().wait()
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)

    worker = _WorkerInstance.spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance='source-spawn-expected',
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=stall_deadline('control', 2 * config.daemon.health_interval_secs),
        startup_deadline_secs=config.daemon.worker_startup_deadline_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        env=_child_environment(),
    )
    try:
        with pytest.raises(ValueError, match='hello identity'):
            _authenticate_worker(worker)
    finally:
        worker.kill()
        worker._process.wait()
        worker.close()

    assert len(spawner.processes) == 1
    assert reap_child(spawner.processes[0]) is not None


def test_worker_instance_rejects_mismatched_protocol_version_and_reaps_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
    spec = build_worker_specs(config.sites['test'])[0]
    spawner = _ControlChildSpawner(
        """
import json
import os
import struct
import sys
import time
import threading

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
threading.Event().wait()
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)

    worker = _WorkerInstance.spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance='source-spawn-version',
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=stall_deadline('control', 2 * config.daemon.health_interval_secs),
        startup_deadline_secs=config.daemon.worker_startup_deadline_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        env=_child_environment(),
    )
    try:
        with pytest.raises(ValueError, match='version conflicts'):
            _authenticate_worker(worker)
    finally:
        worker.kill()
        worker._process.wait()
        worker.close()

    assert len(spawner.processes) == 1
    assert reap_child(spawner.processes[0]) is not None


def test_worker_instance_shutdown_closes_control_for_already_reaped_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _config_path, config = source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    process = cast('ChildProcess', SimpleNamespace(returncode=17))
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(
        parent_socket,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
    )
    worker = _WorkerInstance(
        spec=spec,
        spawn_instance='source-spawn-already-reaped',
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
        assert worker.shutdown('test-already-reaped') == 17
        with pytest.raises(EOFError, match='control is closed'):
            control.receive(timeout=0.0)
    finally:
        control.close()
        child_socket.close()


def test_worker_instance_shutdown_reaps_after_control_peer_closes(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
    spec = build_worker_specs(config.sites['test'])[0]
    spawn_instance = 'source-spawn-closed-control'
    spawner = _ControlChildSpawner(
        """
import os
import sys
import time
import threading
from tests._ipc_support import receive_control, send_control
from sg_event_daemon._ipc import ControlHello, StopCommand, WorkerControl

control = WorkerControl.from_fd(
    int(sys.argv[1]),
    max_frame_bytes=8_388_608,
)
send_control(control,
    ControlHello(
        spawn_instance=sys.argv[2],
        worker_id=sys.argv[3],
        pid=os.getpid(),
    ),
)
receive_control(control)
send_control(control, StopCommand(reason='closing-control-before-exit'))
control.close()
threading.Event().wait()
"""
    )
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)
    worker = _WorkerInstance.spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance=spawn_instance,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=stall_deadline('control', 2 * config.daemon.health_interval_secs),
        startup_deadline_secs=config.daemon.worker_startup_deadline_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        env=_child_environment(),
    )
    try:
        assert _authenticate_worker(worker)
        worker.assign(_source_assignment(spec, spawn_instance))
        assert receive_control(worker._control) == StopCommand(
            reason='closing-control-before-exit'
        )
        with pytest.raises(EOFError, match='peer closed'):
            receive_control(worker._control)

        with stall_detectors.enabled('shutdown'):
            _expire_shutdown_grace(monkeypatch, worker)
            return_code = worker.shutdown('test-control-already-closed')

        assert return_code is not None
        assert return_code != 0
        assert reap_child(spawner.processes[0]) == return_code
    finally:
        worker.kill()
        worker._process.wait()


def test_worker_instance_staged_authentication_times_out_without_losing_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path, config = source_only_config(tmp_path, monkeypatch, production_deadlines=True)
    spec = build_worker_specs(config.sites['test'])[0]
    spawner = _ControlChildSpawner('import threading; threading.Event().wait()')
    monkeypatch.setattr(supervisor_module, 'spawn_child', spawner)

    worker = _WorkerInstance.spawn(
        config_path=config_path,
        spec=spec,
        spawn_instance='source-spawn-silent',
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=stall_deadline('control', 2 * config.daemon.health_interval_secs),
        startup_deadline_secs=config.daemon.worker_startup_deadline_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        env=_child_environment(),
    )
    try:
        assert not worker.authenticate(timeout=0)
        assert worker.wait() is None
    finally:
        worker.kill()
        worker._process.wait()
        worker.close()

    assert len(spawner.processes) == 1
    assert reap_child(spawner.processes[0]) is not None


def test_worker_shutdown_slices_reap_without_pulsing_on_slice_timeout(
    stall_detectors: StallDetectors,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    stall_detectors.enable('shutdown')
    _config_path, config = source_only_config(tmp_path, monkeypatch)
    spec = build_worker_specs(config.sites['test'])[0]
    clock = FakeClock()
    advances: list[None] = []
    timeouts: list[float] = []
    progress_counts_at_reap: list[int] = []

    def send_control(_message: ControlMessage, *, deadline_secs: float) -> None:
        assert deadline_secs > 0

    control = cast(
        'WorkerControl',
        SimpleNamespace(send=send_control, close=lambda: None),
    )
    process = cast('ChildProcess', SimpleNamespace(returncode=None))

    def unexpected_termination(_process: ChildProcess, *, force: bool) -> None:
        pytest.fail(f'graceful sliced reap must not terminate the child (force={force})')

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
    monkeypatch.setattr(supervisor_module, 'terminate_child', unexpected_termination)
    worker = _WorkerInstance(
        spec=spec,
        spawn_instance='sliced-shutdown',
        _process=process,
        _control=control,
        _shutdown_deadline_secs=10.0,
        _wait_slice_secs=0.25,
        _foreground_progress=lambda: advances.append(None),
    )

    assert worker.shutdown('sliced-test') == 0
    assert timeouts == [0.0, 0.25, 0.25, 0.25]
    assert progress_counts_at_reap[0] == 0
    # The initial reap, stop send, and entry to the bounded lifecycle wait are
    # the only semantic advances visible at every timed reap attempt. The two
    # `None` slice returns add no progress of their own.
    assert progress_counts_at_reap[1:] == [3, 3, 3]


@pytest.mark.parametrize('owner', ['ordinary', 'operator'])
def test_worker_silence_deferral_never_resets_breaker(
    stall_detectors: StallDetectors, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, owner: str
) -> None:
    stall_detectors.enable('heartbeat')
    with _worker_silence_harness(tmp_path, monkeypatch) as (
        supervisor,
        worker,
        clock,
        _control,
        sender,
    ):
        breaker = supervisor._breakers[worker.worker_id]
        breaker.record_failure('prior-failure')
        before = breaker.projection
        healthy = Mock(wraps=breaker.record_healthy)
        monkeypatch.setattr(breaker, 'record_healthy', healthy)
        assert supervisor._state is not None
        supervisor._state.persist_worker_breaker(breaker)
        database = supervisor._state._database.connection
        persisted_before = tuple(database.iterdump())
        persist = Mock(wraps=supervisor._state.persist_worker_breaker)
        monkeypatch.setattr(supervisor._state, 'persist_worker_breaker', persist)
        sender.sendall(ipc_module.encode_control_message(health_update(worker, heartbeat_at=100)))

        def inject() -> None:
            clock.now = 412.0
            sender.sendall(
                ipc_module.encode_control_message(health_update(worker, heartbeat_at=411.99))
            )

        _delay_worker_batch(monkeypatch, worker, inject, timing='iteration')
        _silence_pass(supervisor, worker, owner)
        assert not worker.killed
        assert breaker.projection == before
        healthy.assert_not_called()
        persist.assert_not_called()
        assert tuple(database.iterdump()) == persisted_before
        _silence_pass(supervisor, worker, owner)
        assert not worker.killed
        assert supervisor._spawns[worker.worker_id].health_deadline == 422
        clock.now = 422
        _silence_pass(supervisor, worker, owner)
        assert worker.killed


@pytest.mark.parametrize('owner', ['ordinary', 'operator'])
def test_worker_silence_partial_frame_consumes_then_expires(
    stall_detectors: StallDetectors, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, owner: str
) -> None:
    stall_detectors.enable('heartbeat')
    with _worker_silence_harness(tmp_path, monkeypatch) as (
        supervisor,
        worker,
        clock,
        control,
        sender,
    ):
        sender.sendall(ipc_module.encode_control_message(health_update(worker, heartbeat_at=100)))

        def inject() -> None:
            clock.now = 112
            sender.sendall(b'\x00')

        _delay_worker_batch(monkeypatch, worker, inject, timing='iteration')
        _silence_pass(supervisor, worker, owner)
        assert not worker.killed
        assert control.last_arrival_at == 100
        assert control.has_pending_input()
        assert control.has_pending_input()
        assert control.last_arrival_at == 100
        _silence_pass(supervisor, worker, owner)
        assert control.last_arrival_at == 112
        assert not control.has_pending_input()
        spawn = supervisor._spawns[worker.worker_id]
        assert spawn.health is not None
        assert spawn.health.worker.heartbeat_at == 100
        assert spawn.health_deadline == 122
        clock.now = 122
        _silence_pass(supervisor, worker, owner)
        assert worker.killed


@pytest.mark.parametrize('owner', ['ordinary', 'operator'])
@pytest.mark.parametrize('failure', ['malformed', 'eof', 'oserror', 'valueerror'])
def test_worker_silence_malformed_frame_and_eof_keep_failure(
    stall_detectors: StallDetectors,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner: str,
    failure: str,
) -> None:
    stall_detectors.enable('heartbeat')
    with _worker_silence_harness(tmp_path, monkeypatch) as (
        supervisor,
        worker,
        clock,
        control,
        sender,
    ):
        sender.sendall(ipc_module.encode_control_message(health_update(worker, heartbeat_at=100)))

        def inject() -> None:
            clock.now = 112
            if failure == 'malformed':
                sender.sendall(b'\xff\xff\xff\xff')
            elif failure == 'eof':
                sender.close()
            else:
                transport = Mock(wraps=control._socket)
                transport.recv.side_effect = (
                    OSError('peek failed') if failure == 'oserror' else ValueError('peek failed')
                )
                vars(control)['_socket'] = transport

        _delay_worker_batch(monkeypatch, worker, inject, timing='after-eof-peek')
        _silence_pass(supervisor, worker, owner)
        if failure == 'malformed':
            assert not worker.killed
            _silence_pass(supervisor, worker, owner)
        assert worker.killed
        assert supervisor._spawns[worker.worker_id].termination_reason == 'worker-control-eof'


@pytest.mark.parametrize('fence', ['missing', 'replacement', 'retiring'])
def test_worker_silence_stale_spawn_is_never_probed(
    stall_detectors: StallDetectors, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fence: str
) -> None:
    stall_detectors.enable('heartbeat')
    with _worker_silence_harness(tmp_path, monkeypatch) as (
        supervisor,
        worker,
        clock,
        _control,
        sender,
    ):
        sender.sendall(ipc_module.encode_control_message(health_update(worker, heartbeat_at=100)))
        supervisor._advance_worker_messages()
        spawn = supervisor._spawns[worker.worker_id]
        health = spawn.health
        breaker = supervisor._breakers[worker.worker_id].projection
        probe = Mock(side_effect=AssertionError('stale owner probed'))
        monkeypatch.setattr(worker, 'has_pending_input', probe)
        clock.now = 112
        replacement = None
        if fence == 'missing':
            supervisor._spawns.pop(worker.worker_id)
        elif fence == 'replacement':
            replacement = ScriptedWorker(
                worker.spec, 'replacement', [], None, authentication_ready=True
            )
            spawn.worker = replacement
        else:
            spawn.termination_reason = 'already-retiring'
        try:
            assert not supervisor._contain_current_worker(worker, 'worker-heartbeat-overdue')
            probe.assert_not_called()
            assert not worker.killed
            assert spawn.health is health
            assert supervisor._breakers[worker.worker_id].projection == breaker
            if replacement is not None:
                assert not replacement.killed
        finally:
            supervisor._spawns[worker.worker_id] = spawn
            spawn.worker = worker


@pytest.mark.parametrize('owner', ['ordinary', 'operator', 'direct'])
@pytest.mark.parametrize('failure', ['startup', 'source', 'lane', 'operator'])
def test_waiting_input_cannot_defer_explicit_worker_failure(
    stall_detectors: StallDetectors,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner: str,
    failure: str,
) -> None:
    stall_detectors.enable('heartbeat', 'phase', 'coordination', 'startup')
    with _worker_silence_harness(tmp_path, monkeypatch) as (
        supervisor,
        worker,
        clock,
        control,
        sender,
    ):
        healthy = health_update(worker, heartbeat_at=100)
        sender.sendall(ipc_module.encode_control_message(healthy))
        _silence_pass(supervisor, worker, 'ordinary')
        spawn = supervisor._spawns[worker.worker_id]
        if failure == 'startup':
            assert worker.assignment is not None
            spawn.lifecycle = supervisor_module._WorkerLifecycle(
                step_started_at=90,
                assignment=worker.assignment,
                deadline_at=110,
                worker=worker,
                authenticated=True,
                assigned=True,
            )
            clock.now = 112
            bad = health_update(worker, heartbeat_at=112)
        else:
            watchdogs = tuple(
                replace(
                    watchdog, heartbeat_at=99, deadline_at=99.5, phase='internal', source='admin'
                )
                if watchdog.kind == failure
                else watchdog
                for watchdog in healthy.watchdogs
            )
            bad = replace(healthy, watchdogs=watchdogs)
        probe = Mock(side_effect=AssertionError('explicit failure probed'))
        monkeypatch.setattr(worker, 'has_pending_input', probe)
        sender.sendall(ipc_module.encode_control_message(bad))
        batch = worker.receive_available()
        later = health_update(worker, heartbeat_at=clock())
        sender.sendall(ipc_module.encode_control_message(later))
        if owner == 'direct':
            assert supervisor._accept_worker_health(worker, cast('HealthUpdate', next(batch)))
        elif owner == 'operator':
            supervisor._accept_operator_batch(worker, _source_adoption_command('explicit'), batch)
        else:
            # The already acquired finite batch is the ordinary owner's entry snapshot.
            monkeypatch.setattr(worker, 'receive_available', lambda: batch)
            supervisor._advance_worker_messages()
        probe.assert_not_called()
        assert worker.killed
        assert spawn.termination_reason == (
            'worker-startup-timeout' if failure == 'startup' else f'{failure}-phase-overdue'
        )
        assert supervisor._state is not None
        assert supervisor._state.pending_park_evidence() == ()
        assert control.open_socket().recv(1, socket.MSG_PEEK)
        assert spawn.health is not None
        assert spawn.health.worker.monitor_alive is False
        assert spawn.health.worker.heartbeat_at == bad.worker.heartbeat_at
