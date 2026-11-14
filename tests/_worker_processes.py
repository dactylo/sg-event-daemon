from __future__ import annotations

import json
import logging
import math
import os
import resource
import socket
import sqlite3
import sys
import threading
import time
from contextlib import ExitStack, closing, contextmanager, nullcontext, suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict, Unpack, cast

import pytest

from sg_event_daemon import (
    _checkpoint,
    _checkpoint_host,
    _cli,
    _fanout,
    _ipc,
    _lane,
    _source,
    _sqlite,
    _supervisor,
    _worker,
    _write_lock,
)
from sg_event_daemon._checkpoint import CheckpointAttempt, Checkpointer, _CheckpointResult
from sg_event_daemon._config import DEFAULT_CHECKPOINT_INTERVAL_SECS, ConfigError, load_config
from sg_event_daemon._events import EVENT_FIELDS, Event
from sg_event_daemon._group_state import GroupState
from sg_event_daemon._group_types import AuditIdentity, CallbackDoneStep
from sg_event_daemon._logging import configure_logging
from sg_event_daemon._platform import ChildProcess, spawn_child
from sg_event_daemon._providers import Provider, ProviderCapabilities, _FindOptions
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._supervisor import Supervisor, _ParkEvidence, _WorkerBreaker, _WorkerInstance
from sg_event_daemon._watchdog import (
    ComponentWatchdog,
    WatchdogSample,
    internal_stall_deadline_secs,
    watchdog_grace_secs,
)
from sg_event_daemon._write_lock import (
    OPERATOR_WRITE_BUDGET_SECS,
    SourceWriteLock,
    WritePhase,
    WriteTiming,
    _Attempt,
    _FileLease,
    _TransactionOptions,
)
from tests._runtime_diagnostics import install_runtime_diagnostics
from tests._signals import publish_text, read_complete_lines
from tests._stall_policy import (
    STALL_DEADLINE_SECS,
    StallDetectors,
    activate_stall_policy,
    active_stall_detectors,
    lift_config,
    stall_deadline,
)
from tests._storage_support import group_lock, open_test_group, source_database, write_context_for
from tests.source._support import EventLogSG

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Mapping, Sequence
    from contextlib import AbstractContextManager

    from sg_event_daemon._fanout import (
        FanoutMessage,
        GroupChannelInterface,
        StrictOrderingControl,
    )
    from sg_event_daemon._gaps import IdRun
    from sg_event_daemon._group_types import (
        GroupSnapshot,
        LiveJoinIdentity,
        PreInvocationResult,
        PreInvocationStep,
    )
    from sg_event_daemon._ipc import HealthUpdate
    from sg_event_daemon._plugins import CallbackSpec, EventContext
    from sg_event_daemon._source import JoinRequest, JoinRetry, JoinTarget, SourcePoller
    from sg_event_daemon._source_state import _SourceOpenOptions
    from sg_event_daemon._source_store import SourcePollState
    from sg_event_daemon._write_lock import WriteLockLevel

_MISSING_IDS = frozenset({7, 19, 31})
_ROOT = Path(__file__).resolve().parents[1]


class DeterministicHandle:
    def __init__(self, seed: float, last_event_id: int, event_gate: Path | None) -> None:
        self._event_gate = event_gate
        self.config = _ProviderConfig()
        self.seed = seed
        self.last_event_id = last_event_id

    def close(self) -> None:
        pass

    def event(self, event_id: int) -> dict[str, object]:
        event = cast('dict[str, object]', dict.fromkeys(EVENT_FIELDS))
        event.update(
            id=event_id,
            type='EventLogEntry',
            event_type='Shotgun_Asset_Change',
            created_at=datetime.fromtimestamp(self.seed + event_id * 0.05, UTC),
            meta={},
        )
        return event

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        assert entity_type == 'EventLogEntry'
        assert fields is not None
        assert isinstance(filters, list)
        assert options.get('page', 0) == 0
        assert options.get('order') in (
            None,
            [{'field_name': 'id', 'direction': 'asc'}],
            [{'field_name': 'id', 'direction': 'desc'}],
        )
        seed = self.seed
        if self._event_gate is not None:
            seed = self._event_gate.stat().st_mtime if self._event_gate.exists() else time.time()
        visible = max(1, min(self.last_event_id, int((time.time() - seed) / 0.05) + 1))
        if self._event_gate is not None:
            frontier = self._event_gate.parent / 'visible-events'
            if frontier.exists():
                visible = min(self.last_event_id, int(frontier.read_text()))
        event_ids = [
            event_id for event_id in range(1, visible + 1) if event_id not in _MISSING_IDS
        ]
        event_ids = [
            event_id
            for event_id in event_ids
            if all(
                EventLogSG.matches(event_id, condition)
                for condition in cast('list[object]', filters)
            )
        ]
        if options.get('order') == [{'field_name': 'id', 'direction': 'desc'}]:
            event_ids.reverse()
        limit = options.get('limit', 0)
        if limit:
            event_ids = event_ids[:limit]
        return [self.event(event_id) for event_id in event_ids]

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        pass


class OutsideReaderProvider:
    def __init__(self, root: Path, *, frontier_cap: int | None = None) -> None:
        self._root = root
        self._frontier_cap = frontier_cap
        self.frontier = 100
        self._mutex = threading.Lock()

    def find(
        self,
        handle: DeterministicHandle,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        assert entity_type == 'EventLogEntry'
        assert fields is not None
        clauses = cast('list[object]', filters)
        with self._mutex:
            freeze = self._root / 'retained-frontier'
            forward = (
                len(clauses) == 1
                and isinstance(clauses[0], list)
                and cast('list[object]', clauses[0])[:2] == ['id', 'greater_than']
            )
            source_forward = forward and ':source:' in threading.current_thread().name
            advancing = (
                source_forward and (self._root / 'events-ready').exists() and not freeze.exists()
            )
            page_high = self.frontier
            if advancing:
                low = cast('int', cast('list[object]', clauses[0])[2])
                page_high = low + 100
                if self._frontier_cap is not None:
                    page_high = min(page_high, self._frontier_cap)
                self.frontier = max(self.frontier, page_high)
            frontier = int(freeze.read_text()) if freeze.exists() else self.frontier
            lower, upper = 1, frontier
            for clause in clauses:
                first, last = _outside_query_bounds(clause, frontier)
                lower, upper = max(lower, first), min(upper, last)
            if advancing:
                upper = min(upper, page_high)
            descending = options.get('order') == [{'field_name': 'id', 'direction': 'desc'}]
            candidates = range(upper, lower - 1, -1) if descending else range(lower, upper + 1)
            matching = (
                value
                for value in candidates
                if all(EventLogSG.matches(value, clause) for clause in clauses)
                and not (advancing and page_high - 10 <= value < page_high)
            )
            limit = options.get('limit', 0)
            values = list(islice(matching, limit)) if limit else list(matching)
            rows = [handle.event(value) for value in values]
            for row in rows:
                row['created_at'] = datetime(2026, 1, 1, tzinfo=UTC)
            return rows


class _DeterministicAsyncHandle:
    def __init__(self, seed: float, last_event_id: int, event_gate: Path | None) -> None:
        self._sync = DeterministicHandle(seed, last_event_id, event_gate)
        self.config = self._sync.config

    async def close(self) -> None:
        pass

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


class _OutsideReaderOptions(TypedDict, total=False):
    frontier_cap: int | None
    observe_pause: bool


@dataclass(frozen=True)
class ChildPatches:
    hold_deadline_secs: float = _write_lock.WRITE_HOLD_DEADLINE_SECS
    stall_group: str | None = None
    stall_poller: bool = False
    signal_dir: Path | None = None
    mask_write_hold: str | None = None
    last_event_id: int = 1000
    remote_rendezvous: bool = False
    track_revisions: bool = False
    timing: bool = False
    gate_events: bool = False
    checkpoint_clock: bool = False
    checkpoint_wal_limit_bytes: int | None = None
    hold_provider: bool = False
    gate_lane_write: bool = False
    prioritize_competitor: bool = False
    probe_coordination: bool = False
    writer_wal_limit_bytes: int | None = None
    probe_startup: bool = False
    hold_callback: bool = False
    lane_fault: str | None = None
    lane_fault_count: int = 1
    lane_detach_reason: str = 'strict-buffer-capacity'
    lane_abort_failure: str | None = None
    lane_outcome_failure: bool = False
    promotion_detach_reason: str | None = None
    lane_outcome_write_fault: Literal['success', 'request-retry'] | None = None
    source_halt: str | None = None
    async_provider: bool = False
    probe_open_file_limit: bool = False
    busy_source_case: str | None = None
    wait_case: str | None = None
    control_probe: bool = False
    lifecycle_probe: bool = False
    initial_source_query_failures: int = 0
    initial_group_query_failures: int = 0
    outside_reader: bool = False
    outside_pause_observation: bool = False
    probe_wal_restart: bool = False
    combined_delivery_gate: Literal['before-done', 'after-combined'] | None = None
    outside_frontier_cap: int | None = None


@dataclass(frozen=True)
class Topology:
    packing: Literal['default', 'named', 'dedicated', 'mixed']
    groups: tuple[str, ...]
    health_interval_secs: float = 2.0
    supervisor_poll_interval_secs: float = 0.05
    checkpoint_interval_secs: float = DEFAULT_CHECKPOINT_INTERVAL_SECS
    fetch_interval_secs: float = 0.05


@dataclass(frozen=True)
class WorkerSpawn:
    worker_id: str
    spawn_instance: str
    process: ChildProcess
    log_path: Path
    started_at: float


@dataclass(frozen=True)
class WorkerTermination:
    worker_id: str
    spawn_instance: str
    reason: str
    at: float
    health: HealthUpdate | None
    health_before_containment: HealthUpdate | None = None


class SupervisorProbe:
    def __init__(self, supervisor: Supervisor, config_path: Path, patches: ChildPatches) -> None:
        self.supervisor = supervisor
        self._config_path = config_path
        self._code = child_code(config_path, patches)
        self.spawns: list[WorkerSpawn] = []
        self.terminations: list[WorkerTermination] = []
        self.cap_heartbeats: list[tuple[str, float, float]] = []
        self.health_before_containment: dict[str, HealthUpdate | None] = {}

    @property
    def breakers(self) -> Mapping[str, _WorkerBreaker]:
        return self.supervisor._breakers

    @property
    def latest_health(self) -> Mapping[str, HealthUpdate]:
        return {
            worker_id: spawn.health
            for worker_id, spawn in self.supervisor._spawns.items()
            if spawn.health is not None
        }

    def failure_details(self) -> str:
        details = ['worker process evidence:']
        for spawn in self.spawns:
            details.append(
                f'worker={spawn.worker_id} spawn={spawn.spawn_instance} '
                f'exit={spawn.process.poll()} log={spawn.log_path}'
            )
            for suffix in ('.log', '.checkpoint-tries'):
                path = spawn.log_path.with_suffix(suffix)
                try:
                    content = path.read_text(encoding='utf-8', errors='backslashreplace')
                except OSError as error:
                    content = f'cannot read {path.name}: {error}'
                if suffix == '.checkpoint-tries':
                    content = '\n'.join(content.splitlines()[-20:])
                details.append(f'{path.name}:\n{content}')
        terminations = [
            (item.worker_id, item.spawn_instance, item.reason, item.at)
            for item in self.terminations
        ]
        details.append(f'terminations={terminations!r}')
        details.append(f'health={self.latest_health!r}')
        return '\n'.join(details)

    @property
    def park_evidence(self) -> Mapping[tuple[str, str], _ParkEvidence]:
        return self.supervisor._park_evidence_by_group

    def record_termination(self, worker: _WorkerInstance, fallback_reason: str) -> None:
        spawn = self.supervisor._spawns.get(worker.worker_id)
        if spawn is not None and spawn.worker is not worker:
            spawn = None
        reason = fallback_reason if spawn is None else spawn.termination_reason or fallback_reason
        if any(item.spawn_instance == worker.spawn_instance for item in self.terminations):
            return
        self.terminations.append(
            WorkerTermination(
                worker.worker_id,
                worker.spawn_instance,
                reason,
                time.monotonic(),
                None if spawn is None else spawn.health,
                self.health_before_containment.get(worker.spawn_instance),
            )
        )
        with (self._config_path.parent / 'terminations.log').open('a') as stream:
            stream.write(f'{self.terminations[-1]!r}\n')

    def records(self, group: str) -> tuple[int, ...]:
        path = self._config_path.parent / f'{group}.records'
        return tuple(int(line.split()[1]) for line in read_complete_lines(path))

    def spawn(
        self,
        argv: list[str],
        *,
        pass_fds: tuple[int, ...] = (),
        env: Mapping[str, str] | None = None,
    ) -> ChildProcess:
        worker_id = argv[argv.index('--worker') + 1]
        spawn_instance = argv[argv.index('--spawn-instance') + 1]
        started_at = time.monotonic()
        process = spawn_child(
            [sys.executable, '-c', self._code, *argv[argv.index('run-worker') :]],
            pass_fds=pass_fds,
            env=env,
        )
        self.spawns.append(
            WorkerSpawn(
                worker_id,
                spawn_instance,
                process,
                self._config_path.parent / f'{spawn_instance}.log',
                started_at,
            )
        )
        return process

    def startup_complete(self, worker_id: str, spawn_instance: str, *, now: float) -> bool:
        spawn = self.supervisor._spawns.get(worker_id)
        if spawn is None or spawn.worker.spawn_instance != spawn_instance:
            return False
        health = spawn.health
        return (
            spawn.lifecycle is None
            and spawn.health_inventory_complete
            and spawn.termination_reason is None
            and health is not None
            and health.worker.worker_id == worker_id
            and health.worker.spawn_instance == spawn_instance
            and health.worker.monitor_alive
            and health.worker.watchdog_failure is None
            and self.supervisor._worker_health_failure(worker_id, health, now) is None
            and health.worker.heartbeat_at
            <= now
            < health.worker.heartbeat_at
            + self.supervisor._config.daemon.health_interval_secs
            * _supervisor._WORKER_STALE_INTERVALS
        )

    def wait_for(
        self,
        predicate: Callable[[], bool],
        timeout: float | None = None,
        *,
        expected_exits: Collection[str] = (),
    ) -> None:
        # Retain the benchmark harness call shape without imposing a clock limit.
        # Benchmarks observe their own failures and can intentionally kill a worker.
        if timeout is not None:
            assert timeout > 0
        previous_terminations = len(self.terminations)
        while not predicate():
            self.supervisor._tick()
            if predicate():
                return
            if timeout is None and any(
                item.spawn_instance not in expected_exits
                for item in self.terminations[previous_terminations:]
            ):
                if predicate():
                    return
                raise AssertionError(self.failure_details())
            active = {spawn.worker.spawn_instance for spawn in self.supervisor._spawns.values()}
            if timeout is None and any(
                spawn.spawn_instance in active
                and spawn.spawn_instance not in expected_exits
                and spawn.process.poll() is not None
                for spawn in self.spawns
            ):
                if predicate():
                    return
                raise AssertionError(self.failure_details())
            time.sleep(self.supervisor._config.daemon.supervisor_poll_interval_secs)


class _ProviderConfig:
    max_rpc_attempts = 0
    rpc_attempt_interval = 0
    timeout_secs = 1.0


def _between_events(connection: sqlite3.Connection, name: str) -> bool:
    if not name.startswith('lane:'):
        return True
    row = connection.execute(
        'SELECT in_flight_event_id FROM group_state WHERE group_name = ?',
        (name.removeprefix('lane:'),),
    ).fetchone()
    return row is not None and row[0] is None


def _controlled_hold_sample(result: WatchdogSample, patches: ChildPatches) -> WatchdogSample:
    if patches.hold_callback and result.phase == 'callback':
        root = Path(sys.argv[sys.argv.index('--config') + 1]).parent
        if (root / 'callback-expire').exists():
            return replace(result, deadline_at=result.heartbeat_at - 1)
        if result.deadline_at is not None:
            now = time.monotonic()
            budget = result.deadline_at - result.heartbeat_at
            return replace(result, heartbeat_at=now, deadline_at=now + budget)
    signals = patches.signal_dir
    if signals is None or result.phase != 'write-hold' or result.identity != patches.stall_group:
        return result
    if (signals / 'expire-hold').exists():
        return replace(result, deadline_at=result.heartbeat_at - 1)
    if patches.remote_rendezvous:
        # These cases choose the containment step with an explicit marker or kill.
        # Genuine deadline arithmetic is exercised separately with a controlled clock.
        now = time.monotonic()
        return replace(result, heartbeat_at=now, deadline_at=now + patches.hold_deadline_secs)
    return result


def _coordination_gate(root: Path, name: str) -> None:
    (root / f'{name}.ready').touch()
    while not (root / 'requests-release').exists():
        time.sleep(0.001)
    publish_text(root / f'{name}.queued', str(time.monotonic()))


def _gate_lane_write(
    patches: ChildPatches, signals: Path | None, name: str, *, record: bool
) -> None:
    if (
        patches.gate_lane_write
        and signals is not None
        and record
        and name == 'lane:local'
        and not (signals / f'{name}.queued').exists()
    ):
        (signals / f'{name}.queued').touch()
        while not (signals / 'write-release').exists():
            time.sleep(0.001)


def _install_bounded_overflow_provider(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches, root: Path
) -> None:
    if patches.lane_fault not in {'overflow', 'remote-queue'}:
        return
    find = DeterministicHandle.find

    def bounded_find(
        handle: DeterministicHandle,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        gate = root / 'visible-events'
        handle.last_event_id = int(gate.read_text()) if gate.exists() else 1
        return find(handle, entity_type, filters, fields, **options)

    monkeypatch.setattr(DeterministicHandle, 'find', bounded_find)


def _install_callback_hold(
    monkeypatch: pytest.MonkeyPatch, config_path: Path, *, enabled: bool
) -> None:
    if not enabled:
        return
    invoke = _lane._invoke_sync

    def held_callback(callback: CallbackSpec, context: EventContext) -> None:
        root = config_path.parent
        if (root / 'callback-arm').exists():
            (root / 'callback-entered').touch()
            while not (root / 'callback-release').exists():
                time.sleep(0.005)
        invoke(callback, context)

    monkeypatch.setattr(_lane, '_invoke_sync', held_callback)


def _install_competitor_schedule(monkeypatch: pytest.MonkeyPatch, patches: ChildPatches) -> None:
    try_flock = _write_lock.try_flock_exclusive
    acquire_flock = _write_lock.acquire_flock_exclusive

    def scheduled_flock(fd: int) -> bool:
        # Choose the valid starvation schedule: the healthy process wins every
        # handoff until it pauses, even though each short transaction releases.
        signals = patches.signal_dir
        if (
            patches.prioritize_competitor
            and signals is not None
            and (signals / 'competitor-active').exists()
        ):
            return False
        return try_flock(fd)

    def scheduled_acquire(fd: int) -> None:
        # Apply the same chosen schedule to the kernel helper's acquisition.
        signals = patches.signal_dir
        while (
            patches.prioritize_competitor
            and signals is not None
            and (signals / 'competitor-active').exists()
        ):
            time.sleep(0.01)
        acquire_flock(fd)

    monkeypatch.setattr(_write_lock, 'try_flock_exclusive', scheduled_flock)
    monkeypatch.setattr(_write_lock, 'acquire_flock_exclusive', scheduled_acquire)


def _install_coordination_operator_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    execute = _worker._LaneComponent._execute_reasoned_operator
    transaction = SourceWriteLock.transaction
    remaining = SourceWriteLock._remaining
    active = threading.local()

    def command(
        lane: _worker._LaneComponent,
        state: GroupState,
        snapshot: GroupSnapshot,
        message: _ipc.OperatorCommand,
    ) -> GroupSnapshot:
        selected = lane._group == 'strict' and message.command == 'park'
        active.selected = selected
        try:
            return execute(lane, state, snapshot, message)
        finally:
            active.selected = False

    @contextmanager
    def controlled_transaction(
        lock: SourceWriteLock,
        connection: sqlite3.Connection,
        **options: Unpack[_TransactionOptions],
    ) -> Generator[sqlite3.Connection]:
        if not getattr(active, 'selected', False):
            with transaction(lock, connection, **options) as opened:
                yield opened
            return
        assert 'budget_secs' in options
        options['budget_secs'] = OPERATOR_WRITE_BUDGET_SECS
        owner = threading.get_ident()
        original_clock = lock.clock
        active.now = original_clock()
        active.sqlite_checks = 0
        active.file_checks = 0
        with monkeypatch.context() as patch:
            patch.setattr(
                lock,
                'clock',
                lambda: active.now if threading.get_ident() == owner else original_clock(),
            )
            with transaction(lock, connection, **options) as opened:
                yield opened

    def checked_remaining(
        lock: SourceWriteLock, level: WriteLockLevel, attempt: _Attempt
    ) -> float:
        if getattr(active, 'selected', False):
            blocked = (
                level == 'process-local'
                and attempt.entry.lock.locked()
                and attempt.entry.holder != threading.get_ident()
            )
            if level == 'SQLite':
                active.sqlite_checks += 1
                # The third check follows a real BEGIN IMMEDIATE busy result.
                blocked = active.sqlite_checks >= 3
            elif level == 'file':
                active.file_checks += 1
                # A second check is reached only after the nonblocking flock lost.
                blocked = active.file_checks >= 2
            if blocked:
                active.now = attempt.deadline_at
        return remaining(lock, level, attempt)

    monkeypatch.setattr(_worker._LaneComponent, '_execute_reasoned_operator', command)
    monkeypatch.setattr(SourceWriteLock, 'transaction', controlled_transaction)
    monkeypatch.setattr(SourceWriteLock, '_remaining', checked_remaining)


def _install_coordination_probes(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    _install_coordination_operator_expiry(monkeypatch)
    local_request = _worker._LocalJoinClient.request
    remote_request = _ipc.EventClient.request
    acknowledge = _worker._LocalJoinClient.acknowledge_strict_control
    service = _worker._SourceComponent._service_internal
    offered_control = False

    def local(
        client: _worker._LocalJoinClient, request: JoinRequest, channel: GroupChannelInterface
    ) -> JoinTarget | JoinRetry:
        if client._group != 'local_probe':
            return local_request(client, request, channel)
        _coordination_gate(root, 'local-join')
        result = local_request(client, request, channel)
        publish_text(root / 'local-join.answered', str(time.monotonic()))
        return result

    def remote(
        client: _ipc.EventClient, request: JoinRequest, channel: GroupChannelInterface
    ) -> JoinTarget | JoinRetry:
        _coordination_gate(root, 'remote-join')
        result = remote_request(client, request, channel)
        publish_text(root / 'remote-join.answered', str(time.monotonic()))
        return result

    def ack(client: _worker._LocalJoinClient, control: StrictOrderingControl) -> None:
        # This boundary is reached only after the lane committed the retained
        # source-offered control; the acknowledgment itself is not fabricated.
        _coordination_gate(root, 'strict-ack')
        acknowledge(client, control)
        publish_text(root / 'strict-ack.answered', str(time.monotonic()))

    def pump(
        component: _worker._SourceComponent,
        server: _ipc.EventServer,
        surface: _worker._SourceJoinSurface,
        poller: SourcePoller,
    ) -> None:
        nonlocal offered_control
        buffer = surface._strict_buffer
        if (
            not offered_control
            and (root / 'events-ready').exists()
            and buffer is not None
            and 'strict' in buffer._groups
        ):
            state = buffer._groups['strict']
            comparison = max(state.last_delivered_id, state.released_through)
            if (
                not state.provisional
                and state.identity is not None
                and comparison > state.delivery_floor + 1
            ):
                # Ordering controls require durable receipt before acknowledgment.
                buffer._queue_control(
                    state,
                    _fanout.StrictOrderingControl(
                        identity=state.identity,
                        event_id=comparison - 1,
                        last_delivered_id=comparison,
                        delivery_floor=state.delivery_floor,
                        action='halt',
                    ),
                )
                offered_control = True
        service(component, server, surface, poller)

    monkeypatch.setattr(_worker._LocalJoinClient, 'request', local)
    monkeypatch.setattr(_ipc.EventClient, 'request', remote)
    monkeypatch.setattr(_worker._LocalJoinClient, 'acknowledge_strict_control', ack)
    monkeypatch.setattr(_worker._SourceComponent, '_service_internal', pump)


def _install_growth_checkpoints(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    record: Callable[[str, dict[str, object]], None],
) -> None:
    checkpoint = Checkpointer.run_once
    passive = Checkpointer._passive_checkpoint
    reuse = Checkpointer._reuse_checkpoint

    def delayed(checkpointer: Checkpointer) -> CheckpointAttempt:
        if (root / 'checkpoint-hold').exists():
            (root / 'checkpoint-held').touch()
            while not (root / 'checkpoint-release').exists():
                time.sleep(0.005)
        return checkpoint(checkpointer)

    def contended(checkpointer: Checkpointer) -> _CheckpointResult:
        if (root / 'checkpoint-contended').exists():
            return 1, -1, -1
        return passive(checkpointer)

    def traced_reuse(checkpointer: Checkpointer) -> _CheckpointResult:
        result = (1, -1, -1) if (root / 'checkpoint-contended').exists() else reuse(checkpointer)
        record(
            '.reuse',
            {
                'at': time.monotonic(),
                'result': result,
                'bytes': _sqlite.wal_file_bytes(checkpointer._path),
            },
        )
        return result

    monkeypatch.setattr(Checkpointer, 'run_once', delayed)
    monkeypatch.setattr(Checkpointer, '_passive_checkpoint', contended)
    monkeypatch.setattr(Checkpointer, '_reuse_checkpoint', traced_reuse)


def _install_lifecycle_bootstrap(monkeypatch: pytest.MonkeyPatch, root: Path, spawn: str) -> None:
    send = _ipc.WorkerControl.send
    run_source = _worker._SourceComponent._run_body

    def hello(
        control: _ipc.WorkerControl, message: _ipc.ControlMessage, *, deadline_secs: float
    ) -> None:
        if isinstance(message, _ipc.ControlHello):
            (root / f'{spawn}.hello-entered').touch()
            while (root / 'hold-hello').exists():
                time.sleep(0.002)
        send(control, message, deadline_secs=deadline_secs)
        if isinstance(message, _ipc.ControlHello):
            (root / f'{spawn}.hello-sent').touch()

    def source(component: _worker._SourceComponent) -> None:
        (root / f'{spawn}.source-entered').touch()
        while (root / 'hold-server').exists():
            time.sleep(0.002)
        run_source(component)

    monkeypatch.setattr(_ipc.WorkerControl, 'send', hello)
    monkeypatch.setattr(_worker._SourceComponent, '_run_body', source)


def _install_lifecycle_output(monkeypatch: pytest.MonkeyPatch, root: Path, spawn: str) -> None:
    write = _ipc.WorkerControlOutbox.write_available
    next_frame = _ipc.WorkerControlOutbox._next_frame

    def output(outbox: _ipc.WorkerControlOutbox) -> None:
        write(outbox)
        (root / f'{spawn}.wrote').touch()
        if (root / f'{spawn}.write-once').exists():
            (root / f'{spawn}.writer-frozen').touch()
            while (root / f'{spawn}.write-once').exists():
                time.sleep(0.002)

    def frame(outbox: _ipc.WorkerControlOutbox) -> bool:
        if (
            not outbox._unsent
            and (root / f'{spawn}.frame-started').exists()
            and (root / f'{spawn}.pause-after-frame').exists()
        ):
            (root / f'{spawn}.frame-finished').touch()
            while (root / f'{spawn}.pause-after-frame').exists():
                time.sleep(0.002)
        selected = next_frame(outbox)
        if selected:
            (root / f'{spawn}.frame-started').touch()
        return selected

    monkeypatch.setattr(_ipc.WorkerControlOutbox, 'write_available', output)
    monkeypatch.setattr(_ipc.WorkerControlOutbox, '_next_frame', frame)


def _install_lifecycle_publications(
    monkeypatch: pytest.MonkeyPatch, root: Path, spawn: str
) -> None:
    enqueue = _ipc.WorkerControlOutbox.enqueue
    offer = _ipc.WorkerControlOutbox.offer_health

    def message(outbox: _ipc.WorkerControlOutbox, value: _ipc.ControlMessage) -> None:
        enqueue(outbox, value)
        if isinstance(value, _ipc.SourceEndpointReady):
            (root / f'{spawn}.endpoint-enqueued').touch()

    def health(outbox: _ipc.WorkerControlOutbox, value: _ipc.HealthUpdate) -> None:
        if (root / 'omit-group-health').exists():
            value = replace(value, groups=())
        offer(outbox, value)
        expected = sum(item.kind != 'operator' for item in value.watchdogs)
        if expected and len(value.sources) + len(value.groups) == expected:
            (root / f'{spawn}.complete-health-offered').touch()

    monkeypatch.setattr(_ipc.WorkerControlOutbox, 'enqueue', message)
    monkeypatch.setattr(_ipc.WorkerControlOutbox, 'offer_health', health)


def _install_permit_wait_probe(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    acquire = _worker._CatchupLimiter.acquire

    def blocked_acquire(limiter: _worker._CatchupLimiter, *, timeout: float) -> bool:
        # Exhaust the actual semaphore so its real timed acquire owns the wait.
        if not (root / 'wait-entered').exists():
            while acquire(limiter, timeout=0):
                pass
            publish_text(root / 'wait-entered', str(timeout))
        return acquire(limiter, timeout=timeout)

    monkeypatch.setattr(_worker._CatchupLimiter, 'acquire', blocked_acquire)


def _install_provider(
    monkeypatch: pytest.MonkeyPatch,
    seed: float,
    last_event_id: int,
    event_gate: Path | None,
    *,
    async_provider: bool = False,
) -> None:
    def factory(
        site: str, *, script_name: str, api_key: str, connect: bool
    ) -> DeterministicHandle:
        assert site == 'https://example.invalid'
        assert script_name in {'test', 'changed'}
        assert api_key == 'test-key'
        assert not connect
        return DeterministicHandle(seed, last_event_id, event_gate)

    def async_factory(
        site: str, *, script_name: str, api_key: str, connect: bool
    ) -> _DeterministicAsyncHandle:
        handle = factory(site, script_name=script_name, api_key=api_key, connect=connect)
        return _DeterministicAsyncHandle(handle.seed, last_event_id, event_gate)

    provider = Provider(
        'sg-python-api' if async_provider else 'shotgun-api3',
        ProviderCapabilities(sync=True, async_=async_provider),
        factory,
        async_factory if async_provider else None,
    )

    def load(*_args: object) -> Provider:
        return provider

    monkeypatch.setattr(_worker, 'load_provider', load)


def _install_queued_delivery_gate(
    monkeypatch: pytest.MonkeyPatch, root: Path, last_event_id: int
) -> None:
    if (root / 'combined-gate').exists():
        return
    queued = threading.Event()
    offer = _fanout.GroupChannel.offer
    offer_paced = _fanout.GroupChannel.offer_paced
    drive = _worker._LaneComponent._drive_live
    drain = _lane.GroupLane.drain_live
    expected = last_event_id - 1

    def offered(channel: _fanout.GroupChannel) -> None:
        if channel.status.queued_count == expected:
            queued.set()

    def queued_offer(channel: _fanout.GroupChannel, message: FanoutMessage) -> bool:
        accepted = offer(channel, message)
        offered(channel)
        return accepted

    def queued_paced(channel: _fanout.GroupChannel, message: FanoutMessage) -> bool:
        accepted = offer_paced(channel, message)
        offered(channel)
        return accepted

    def wait_for_batch(
        component: _worker._LaneComponent,
        resources: _worker._LaneResources,
        lifecycle: _worker._LaneLifecycle,
        *,
        idle_snapshot: GroupSnapshot | None = None,
    ) -> None:
        if not (root / 'combined-batch-wait').exists():
            # Block before normal coordination chooses this pass's deadline.
            publish_text(root / 'combined-batch-wait', str(os.getpid()))
            queued.wait()
            assert lifecycle.channel.status.queued_count == expected
        drive(component, resources, lifecycle, idle_snapshot=idle_snapshot)

    def observed_drain(
        lane: _lane.GroupLane,
        channel: GroupChannelInterface,
        *,
        idle_snapshot: GroupSnapshot | None = None,
        deadline_at: float = float('inf'),
    ) -> GroupSnapshot:
        marker = root / 'combined-batch-pass'
        if not marker.exists():
            started = time.monotonic()
            assert channel.status.queued_count == expected
            publish_text(
                marker,
                json.dumps(
                    {
                        'pid': os.getpid(),
                        'queued': channel.status.queued_count,
                        'started': started,
                        'deadline': deadline_at,
                    }
                ),
            )
        # This fixture tests crash atomicity after a complete batch is queued.
        # Deadline selection has separate controlled-clock coverage.
        return drain(lane, channel, idle_snapshot=idle_snapshot, deadline_at=float('inf'))

    monkeypatch.setattr(_fanout.GroupChannel, 'offer', queued_offer)
    monkeypatch.setattr(_fanout.GroupChannel, 'offer_paced', queued_paced)
    monkeypatch.setattr(_worker._LaneComponent, '_drive_live', wait_for_batch)
    monkeypatch.setattr(_lane.GroupLane, 'drain_live', observed_drain)


def _install_combined_delivery_gate(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    gate: str | None,
    last_event_id: int,
) -> None:
    if gate is None:
        return
    _install_queued_delivery_gate(monkeypatch, root, last_event_id)
    invoke = _lane._invoke_sync
    commit = GroupState.commit_pre_invocation
    marker = root / 'combined-gate'

    def acknowledge() -> None:
        if marker.exists():
            return
        publish_text(marker, str(os.getpid()))
        while not (root / 'combined-release').exists():
            time.sleep(0.005)

    def callback(callback: CallbackSpec, context: EventContext) -> None:
        invoke(callback, context)
        with (root / 'combined-callbacks').open('a') as stream:
            stream.write(f'{context.event["id"]} {callback.id} {context.delivery_attempt}\n')
        if gate == 'before-done':
            acknowledge()

    def commit_next(
        state: GroupState,
        steps: Sequence[PreInvocationStep],
        *,
        expected_revision: int,
    ) -> PreInvocationResult:
        result = commit(state, steps, expected_revision=expected_revision)
        if (
            gate == 'after-combined'
            and result.rejection is None
            and any(type(step).__name__ == 'CallbackDoneStep' for step in steps)
            and any(type(step).__name__ == 'BeginCallbackStep' for step in steps)
        ):
            acknowledge()
        return result

    monkeypatch.setattr(_lane, '_invoke_sync', callback)
    monkeypatch.setattr(GroupState, 'commit_pre_invocation', commit_next)


def _install_revision_probe(monkeypatch: pytest.MonkeyPatch, log_path: Path) -> None:
    begin_cycle = _worker.SourceState.begin_cycle
    complete_scan = _worker.GroupState.complete_obligation_scan

    def observed_cycle(state: _worker.SourceState) -> SourcePollState:
        before = state._observed_revisions
        result = begin_cycle(state)
        after = state._observed_revisions
        if after != before and after is not None:
            with log_path.with_suffix('.revisions').open('a') as output:
                output.write(f'{-1 if before is None else before[0]} {after[0]} {after[1]}\n')
        return result

    def observed_scan(
        state: _worker.GroupState, obligation_id: str, *, excused: Sequence[IdRun] = ()
    ) -> GroupSnapshot:
        result = complete_scan(state, obligation_id, excused=excused)
        revision = state._database.connection.execute(
            'SELECT index_revision FROM source_meta'
        ).fetchone()[0]
        with log_path.with_suffix('.scans').open('a') as output:
            output.write(f'{revision}\n')
        return result

    monkeypatch.setattr(_worker.SourceState, 'begin_cycle', observed_cycle)
    monkeypatch.setattr(_worker.GroupState, 'complete_obligation_scan', observed_scan)


def _install_source_halt_fault(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches, root: Path
) -> None:
    encode = _fanout.EventEncoder._encode_event
    make_event = DeterministicHandle.event

    def faulted_event(handle: DeterministicHandle, event_id: int) -> dict[str, object]:
        result = make_event(handle, event_id)
        if event_id == 5 and patches.source_halt in {
            'event-too-large',
            'frame-too-large',
            'commit-batch-capacity-exhausted',
        }:
            size = 9_000_000 if patches.source_halt == 'frame-too-large' else 6_500_000
            result['meta'] = {'payload': 'x' * size}
        return result

    def faulted_encode(
        encoder: _fanout.EventEncoder, event: Event
    ) -> _fanout.EncodedEvent | _fanout._EncodeFailure:
        if event['id'] == 5:
            if patches.source_halt == 'encoder-failed' and not (root / 'encoder-fixed').exists():
                raise ValueError('injected encoder fault')
            if patches.source_halt in {'frame-too-large', 'commit-batch-capacity-exhausted'}:
                # Exercise the frame/commit guards with a frame above the
                # configuration's conservative maximum-event relationship.
                encoder._max_event_bytes = 12_000_000
            if patches.source_halt == 'commit-batch-capacity-exhausted':
                encoder._max_ipc_frame_bytes = 16_000_000
        return encode(encoder, event)

    monkeypatch.setattr(DeterministicHandle, 'event', faulted_event)
    monkeypatch.setattr(_fanout.EventEncoder, '_encode_event', faulted_encode)


def _install_source_halt_probe(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches, root: Path
) -> None:
    if patches.source_halt is None:
        return
    find = DeterministicHandle.find
    run = _source.SourcePoller.run

    def traced_find(
        handle: DeterministicHandle,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        if ':source:' in threading.current_thread().name:
            with (root / 'source-queries').open('a') as stream:
                stream.write(f'{time.monotonic()}\n')
        return find(handle, entity_type, filters, fields, **options)

    def traced_run(poller: SourcePoller, *, max_cycles: int | None = None) -> None:
        try:
            run(poller, max_cycles=max_cycles)
        except _source.SourceHaltedError as error:
            with (root / 'source-halts').open('a') as stream:
                stream.write(f'{error.code} {time.monotonic()}\n')
            raise

    monkeypatch.setattr(DeterministicHandle, 'find', traced_find)
    monkeypatch.setattr(_source.SourcePoller, 'run', traced_run)
    _install_source_halt_fault(monkeypatch, patches, root)


def _install_startup_probe(
    monkeypatch: pytest.MonkeyPatch,
    log_path: Path,
    *,
    enabled: bool,
) -> None:
    if not enabled:
        return
    observe = _worker._SourceJoinSurface.observe_group

    def traced_observe(
        surface: _worker._SourceJoinSurface,
        request: _ipc.GroupBootstrapRequest,
    ) -> _ipc.GroupBootstrapObservation | _ipc.GroupBootstrapPending:
        result = observe(surface, request)
        row: dict[str, object] = (
            {'pending': result.retry_after_secs}
            if isinstance(result, _ipc.GroupBootstrapPending)
            else {'generation': result.source_generation}
        )
        row['group'] = request.group
        with log_path.with_suffix('.bootstrap').open('a') as stream:
            stream.write(json.dumps(row) + '\n')
        return result

    monkeypatch.setattr(_worker._SourceJoinSurface, 'observe_group', traced_observe)


def _install_success_outcome_failure(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches
) -> None:
    if patches.lane_outcome_write_fault is None:
        return
    record = GroupState.record_strict_join_outcome

    def failed_success(
        state: GroupState,
        *,
        attempt_token: str,
        capacity_caused: bool,
        abort_limit: int,
        expected_revision: int,
    ) -> GroupSnapshot:
        if capacity_caused == (patches.lane_outcome_write_fault == 'request-retry'):
            raise sqlite3.OperationalError('join outcome write failed')
        return record(
            state,
            attempt_token=attempt_token,
            capacity_caused=capacity_caused,
            abort_limit=abort_limit,
            expected_revision=expected_revision,
        )

    monkeypatch.setattr(GroupState, 'record_strict_join_outcome', failed_success)
    if patches.lane_outcome_write_fault == 'request-retry':

        def refused_request(
            _coordinator: _source.JoinCoordinator,
            request: _source.JoinRequest,
            _channel: GroupChannelInterface,
        ) -> _source.JoinRetry:
            return _source.JoinRetry(request.attempt, 0.0, 0, 0)

        monkeypatch.setattr(_source.JoinCoordinator, 'request', refused_request)


def _install_wal_restart_probe(
    monkeypatch: pytest.MonkeyPatch, log_path: Path, *, enabled: bool
) -> None:
    if not enabled:
        return
    original = SourceState.open

    def opened(
        _cls: type[SourceState], path: Path, **options: Unpack[_SourceOpenOptions]
    ) -> SourceState:
        started = time.monotonic()
        before = _sqlite.wal_file_bytes(path)
        result = original(path, **options)
        finished = time.monotonic()
        with log_path.with_suffix('.wal-recovery').open('a') as stream:
            stream.write(
                json.dumps(
                    {
                        'started': started,
                        'finished': finished,
                        'before': before,
                        'after': _sqlite.wal_file_bytes(path),
                    }
                )
                + '\n'
            )
        return result

    monkeypatch.setattr(SourceState, 'open', classmethod(opened))


def _outside_descriptor_sample(record: Callable[..., None], log_path: Path, path: Path) -> None:
    # Read the process descriptor directory without entering the storage transaction.
    directory = Path('/proc/self/fd') if Path('/proc/self/fd').exists() else Path('/dev/fd')
    count = 0
    for entry in list(directory.iterdir()):
        if entry.name.isdecimal():
            try:
                os.fstat(int(entry.name))
            except OSError:
                continue
            count += 1
    record(
        'samples',
        fds=count,
        pid=os.getpid(),
        spawn_instance=log_path.stem,
        wal_file=_sqlite.wal_file_bytes(path),
        wal_in_use=None,
    )


def _outside_query_bounds(expression: object, frontier: int) -> tuple[int, int]:
    if isinstance(expression, dict):
        group = cast('dict[str, object]', expression)
        assert group['filter_operator'] == 'any'
        bounds = [
            _outside_query_bounds(item, frontier)
            for item in cast('list[object]', group['filters'])
        ]
        return min(low for low, _high in bounds), max(high for _low, high in bounds)
    field, operator, *bounds = cast('list[object]', expression)
    assert field == 'id'
    low = cast('int', bounds[0])
    if operator == 'is':
        return low, low
    if operator == 'greater_than':
        return low + 1, frontier
    if operator == 'less_than':
        return 1, low - 1
    assert operator == 'between'
    return low, cast('int', bounds[1])


def _outside_writer_role() -> str:
    name = threading.current_thread().name
    return name.rsplit(':lane:', 1)[-1] if ':lane:' in name else 'site'


def _install_outside_delivery(
    monkeypatch: pytest.MonkeyPatch, record: Callable[..., None], delivery: threading.local
) -> None:
    callback_done = GroupState.callback_done
    pre_invocation = GroupState.commit_pre_invocation

    @contextmanager
    def labeled(kind: str) -> Generator[None]:
        previous = getattr(delivery, 'kind', 'other')
        delivery.kind = kind
        delivery.committed_at = None
        try:
            yield
        finally:
            delivery.kind = previous

    def completed(snapshot: GroupSnapshot) -> None:
        at = getattr(delivery, 'committed_at', None)
        if at is not None:
            record('progress', _outside_writer_role(), at=at, count=1, cursor=snapshot.cursor)

    def done(state: GroupState, ordinal: int, *, expected_revision: int) -> GroupSnapshot:
        with labeled('done'):
            snapshot = callback_done(state, ordinal, expected_revision=expected_revision)
        completed(snapshot)
        return snapshot

    def pre(
        state: GroupState, steps: Sequence[PreInvocationStep], *, expected_revision: int
    ) -> PreInvocationResult:
        with labeled('pre-invocation'):
            result = pre_invocation(state, steps, expected_revision=expected_revision)
        if (
            steps
            and isinstance(steps[0], CallbackDoneStep)
            and result.snapshot.revision > expected_revision
        ):
            completed(result.snapshot)
        return result

    monkeypatch.setattr(GroupState, 'callback_done', done)
    monkeypatch.setattr(GroupState, 'commit_pre_invocation', pre)


def _install_outside_writes(
    monkeypatch: pytest.MonkeyPatch,
    record: Callable[..., None],
    observe_pause: Callable[[str], None] | None = None,
) -> None:
    transaction = SourceWriteLock.transaction
    settle = SourceWriteLock._settle
    delivery = threading.local()

    def measured_settle(
        lock: SourceWriteLock,
        connection: sqlite3.Connection,
        attempt: _Attempt,
        lease: _FileLease,
        primary: BaseException | None,
    ) -> None:
        try:
            # Successful COMMIT and its finished_at stamp precede this call.
            # Sample before release so a later writer cannot inflate its WAL.
            delivery.wal_file = _sqlite.wal_file_bytes(lock.database_path)
        finally:
            settle(lock, connection, attempt, lease, primary)

    @contextmanager
    def measured(
        lock: SourceWriteLock,
        connection: sqlite3.Connection,
        **options: Unpack[_TransactionOptions],
    ) -> Generator[sqlite3.Connection]:
        timing = options.setdefault('timing', WriteTiming())
        path = Path(connection.execute('PRAGMA database_list').fetchone()[2])
        committed = False
        try:
            with transaction(lock, connection, **options) as active:
                if observe_pause is not None:
                    observe_pause('write')
                yield active
            committed = True
        except _write_lock.WriteLockTimeoutError as error:
            record('expired', _outside_writer_role(), level=error.level)
            raise
        finally:
            if math.isfinite(timing.finished_at):
                delivery.committed_at = timing.finished_at if committed else None
                record(
                    'transactions',
                    _outside_writer_role(),
                    at=timing.finished_at,
                    kind_label=getattr(delivery, 'kind', 'other'),
                    acquisition=timing.begun_at - timing.started_at,
                    protected=timing.finished_at - timing.begun_at,
                    committed=committed,
                    wal_file=delivery.wal_file if committed else _sqlite.wal_file_bytes(path),
                    wal_in_use=None,
                )

    if observe_pause is not None:
        expired = _write_lock.WriteContext._expired

        def published(
            context: _write_lock.WriteContext,
            error: _write_lock.WriteLockTimeoutError,
            timing: WriteTiming,
        ) -> None:
            expired(context, error, timing)
            if error.level == 'pause':
                observe_pause('pause')

        monkeypatch.setattr(_write_lock.WriteContext, '_expired', published)
    monkeypatch.setattr(SourceWriteLock, 'transaction', measured)
    monkeypatch.setattr(SourceWriteLock, '_settle', measured_settle)
    _install_outside_delivery(monkeypatch, record, delivery)


def _record_control_event(
    root: Path, runtime: _worker._WorkerRuntime, kind: str, **values: object
) -> None:
    with (root / f'{runtime._spawn_instance}.control.jsonl').open('a') as stream:
        stream.write(json.dumps({'kind': kind, 'at': time.monotonic(), **values}) + '\n')


def _install_control_command_probe(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    operator_response = _worker._WorkerRuntime._operator_response
    apply_control = _worker._WorkerRuntime._apply_control_message

    def command(
        runtime: _worker._WorkerRuntime, message: _ipc.OperatorCommand
    ) -> _ipc.CommandResult:
        if not (root / 'operator-hold').exists():
            return operator_response(runtime, message)
        _record_control_event(root, runtime, 'command-started', request_id=message.request_id)
        while not (root / 'operator-release').exists():
            if runtime._stop_event.wait(0.005):
                return runtime._operator_failure(message, 'stopped')
        _record_control_event(root, runtime, 'command-completed', request_id=message.request_id)
        return _ipc.CommandResult(
            command=message.command,
            request_id=message.request_id,
            ok=True,
            message='fixture completed',
            result={},
        )

    def apply(runtime: _worker._WorkerRuntime, message: object) -> None:
        apply_control(runtime, message)
        if isinstance(message, _ipc.OperatorCommand):
            _record_control_event(root, runtime, 'command-read', request_id=message.request_id)

    monkeypatch.setattr(_worker._WorkerRuntime, '_operator_response', command)
    monkeypatch.setattr(_worker._WorkerRuntime, '_apply_control_message', apply)


def _record_open_file_limit(config_path: Path, spawn_instance: str, *, enabled: bool) -> None:
    if enabled:
        publish_text(
            config_path.parent / f'{spawn_instance}.limits',
            json.dumps(resource.getrlimit(resource.RLIMIT_NOFILE)),
        )


def _record_queued_control_result(root: Path, spawn: str, message: _ipc.ControlMessage) -> None:
    if isinstance(message, _ipc.CommandResult):
        with (root / f'{spawn}.control.jsonl').open('a') as stream:
            stream.write(
                json.dumps(
                    {
                        'kind': 'result-enqueued',
                        'request_id': message.request_id,
                    }
                )
                + '\n'
            )


def _record_write(signals: Path, name: str, phase: str, deadline: float = 0) -> None:
    with (signals / f'{name}.writes').open('a', encoding='utf-8') as stream:
        stream.write(f'{phase} {time.monotonic()} {deadline}\n')


@contextmanager
def _record_attempt(signals: Path | None, name: str, *, record: bool) -> Generator[None]:
    try:
        yield
    except (_write_lock.WriteLockStoppedError, _write_lock.WriteLockTimeoutError) as error:
        if record and signals is not None:
            phase = (
                'stopped' if isinstance(error, _write_lock.WriteLockStoppedError) else 'expired'
            )
            _record_write(signals, name, phase)
        raise


@dataclass
class _LaneDetachProbe:
    patches: ChildPatches
    root: Path
    remaining: int

    def record(self, kind: str, **values: object) -> None:
        with (self.root / 'lane-probe.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(json.dumps({'kind': kind, **values}) + '\n')

    def inject(self, lane: _lane.GroupLane) -> None:
        self.remaining -= 1
        owned = lane._join_state
        assert isinstance(owned, (_lane._Joining, _lane._FinalizedJoin))
        self.record(
            'injected',
            fault=self.patches.lane_fault,
            token=owned.identity.attempt_token if owned.identity else '',
        )
        owned.channel.detach(self.patches.lane_detach_reason)


def _install_conflicting_finalize_probe(
    monkeypatch: pytest.MonkeyPatch, probe: _LaneDetachProbe
) -> None:
    fault = probe.patches.lane_fault
    if fault == 'boundary-conflict':
        for client_type in (_ipc.EventClient, _worker._LocalJoinClient):
            client_finalize = client_type.finalize

            def conflicting_finalize(
                client: _ipc.EventClient | _worker._LocalJoinClient,
                request: _source.JoinFinalize,
                original: Callable[..., _source.JoinFinalized] = client_finalize,
            ) -> _source.JoinFinalized:
                result = original(client, request)
                if probe.remaining:
                    probe.remaining -= 1
                    probe.record('injected', fault=fault, token=request.identity.attempt_token)
                    return replace(
                        result,
                        identity=replace(result.identity, boundary=result.identity.boundary + 1),
                    )
                return result

            monkeypatch.setattr(client_type, 'finalize', conflicting_finalize)


def _install_lane_join_detach_probe(
    monkeypatch: pytest.MonkeyPatch, probe: _LaneDetachProbe
) -> None:
    fault = probe.patches.lane_fault
    confirm = _lane.GroupLane._confirm_join
    drain = _lane.GroupLane._drain_channel
    promote = _lane.GroupLane._promote_join

    def traced_confirm(
        lane: _lane.GroupLane,
        source: _lane._JoinSource,
        identity: LiveJoinIdentity,
        attempt: _source.JoinAttemptIdentity,
    ) -> _source.Attached | _source.JoinRetry:
        result = confirm(lane, source, identity, attempt)
        if probe.remaining and fault == 'request-confirm' and isinstance(result, _source.Attached):
            probe.inject(lane)
        return result

    def traced_drain(
        lane: _lane.GroupLane,
        owned: _lane._Joining | _lane._FinalizedJoin,
    ) -> tuple[_fanout.FanoutMessage, ...]:
        if probe.remaining and fault == 'confirm-finalize' and isinstance(owned, _lane._Joining):
            channel_drain = owned.channel.drain

            def detached_drain() -> tuple[_fanout.FanoutMessage, ...]:
                messages = channel_drain()
                probe.inject(lane)
                return messages

            monkeypatch.setattr(owned.channel, 'drain', detached_drain)
        return drain(lane, owned)

    def traced_promote(
        lane: _lane.GroupLane,
        source: _lane._JoinSource,
        joining: _lane._Joining,
        identity: LiveJoinIdentity,
    ) -> None:
        if probe.remaining and fault == 'finalize-promote':
            probe.inject(lane)
        elif probe.remaining and fault == 'ownership-conflict':
            probe.remaining -= 1
            probe.record('injected', fault=fault, token=identity.attempt_token)
            lane._join_state = _lane._DetachedJoin()
        promote(lane, source, joining, identity)
        probe.record(
            'promoted',
            token=identity.attempt_token,
            anomalies=int(lane._state.snapshot().ordering_anomaly_id is not None),
        )

    monkeypatch.setattr(_lane.GroupLane, '_confirm_join', traced_confirm)
    monkeypatch.setattr(_lane.GroupLane, '_drain_channel', traced_drain)
    monkeypatch.setattr(_lane.GroupLane, '_promote_join', traced_promote)


def _install_lane_live_detach_probe(
    monkeypatch: pytest.MonkeyPatch, probe: _LaneDetachProbe
) -> None:
    fault = probe.patches.lane_fault
    root = probe.root
    patches = probe.patches
    prepare = _lane.GroupLane._prepare_live_drain
    snapshot = _lane.GroupLane._snapshot

    def traced_prepare(
        lane: _lane.GroupLane,
        channel: GroupChannelInterface,
        idle: GroupSnapshot | None,
    ) -> tuple[_lane._FinalizedJoin, GroupSnapshot | None]:
        if probe.remaining and idle is not None and fault in {'idle-snapshot', 'channel-closed'}:
            probe.record('idle-snapshot', reused=True)
            probe.inject(lane)
        if probe.remaining and idle is not None and fault in {'overflow', 'remote-queue'}:
            probe.remaining -= 1
            probe.record('injected', fault=fault)
            (root / 'overflow-active').touch()
            gate = root / 'events-ready'
            visible = (patches.lane_fault_count - probe.remaining) * 6
            pending = root / f'visible-events.{os.getpid()}.{threading.get_ident()}.pending'
            pending.write_text(str(visible))
            pending.replace(root / 'visible-events')
            gate.touch()
            os.utime(gate, (time.time() - 10, time.time() - 10))
            owned = lane._join_state
            assert isinstance(owned, _lane._FinalizedJoin)
            while channel.status.attached:
                if isinstance(owned.source, _ipc.EventClient):
                    owned.source.poll(0.01)
                time.sleep(0.005)
            probe.record('overflow', reason=channel.status.reason)
            (root / 'overflow-active').unlink(missing_ok=True)
        return prepare(lane, channel, idle)

    def traced_snapshot(lane: _lane.GroupLane) -> GroupSnapshot:
        result = snapshot(lane)
        if (
            probe.remaining
            and fault == 'live-loop'
            and isinstance(lane._join_state, _lane._FinalizedJoin)
        ):
            probe.inject(lane)
        return result

    monkeypatch.setattr(_lane.GroupLane, '_prepare_live_drain', traced_prepare)
    monkeypatch.setattr(_lane.GroupLane, '_snapshot', traced_snapshot)


def _install_lane_settlement_probe(
    monkeypatch: pytest.MonkeyPatch, probe: _LaneDetachProbe
) -> None:
    catch_up = _worker._LaneComponent._catch_up
    abort = _lane.GroupLane._abort_join
    outcome = _lane.GroupLane._record_strict_join_outcome
    finalize = _lane.GroupLane._finalize_join
    fault = probe.patches.lane_fault

    def traced_catchup(
        component: _worker._LaneComponent, resources: _worker._LaneResources, frontier: int
    ) -> None:
        target = resources.client.last_join_target
        assert target is not None
        probe.record('catchup', frontier=frontier, token=target.identity.attempt_token)
        catch_up(component, resources, frontier)

    def traced_abort(
        lane: _lane.GroupLane,
        source: _lane._JoinSource,
        joining: _lane._Joining,
        *,
        capacity_caused: bool,
        primary_error: BaseException | None = None,
    ) -> None:
        probe.record('abort', token=joining.attempt.attempt_token, capacity=capacity_caused)
        abort(lane, source, joining, capacity_caused=capacity_caused, primary_error=primary_error)

    def traced_outcome(
        lane: _lane.GroupLane,
        attempt: _source.JoinAttemptIdentity,
        *,
        capacity_caused: bool,
    ) -> GroupSnapshot:
        probe.record('outcome-attempt', token=attempt.attempt_token, capacity=capacity_caused)
        if probe.patches.lane_outcome_failure and capacity_caused:
            raise sqlite3.OperationalError('outcome write failed')
        result = outcome(lane, attempt, capacity_caused=capacity_caused)
        probe.record('outcome', token=attempt.attempt_token, capacity=capacity_caused)
        return result

    def traced_finalize(
        lane: _lane.GroupLane,
        source: _lane._JoinSource,
        identity: LiveJoinIdentity,
    ) -> None:
        if probe.remaining and fault == 'finalize-capacity':
            probe.remaining -= 1
            probe.record('injected', fault=fault, token=identity.attempt_token)
            raise _source.JoinCapacityError('strict finalize capacity probe')
        finalize(lane, source, identity)

    monkeypatch.setattr(_lane.GroupLane, '_finalize_join', traced_finalize)
    monkeypatch.setattr(_worker._LaneComponent, '_catch_up', traced_catchup)
    monkeypatch.setattr(_lane.GroupLane, '_abort_join', traced_abort)
    monkeypatch.setattr(_lane.GroupLane, '_record_strict_join_outcome', traced_outcome)


def _install_promotion_transport_probe(
    monkeypatch: pytest.MonkeyPatch, probe: _LaneDetachProbe
) -> None:
    encode = _ipc._encode_join_reply
    rpc = _ipc.EventClient._rpc
    close = _ipc.EventServer.close_connection

    def held_close(
        server: _ipc.EventServer, group: str, connection_id: str, *, reason: str
    ) -> None:
        # Keep the real sockets open so recovery must use the finalize reply.
        if reason == probe.patches.promotion_detach_reason:
            assert server.connection_status(group, connection_id).connected
            probe.record('transport-held', reason=reason)
            return
        close(server, group, connection_id, reason=reason)

    def traced_encode(reply: _ipc._JoinReply) -> _ipc._OutboundFrame:
        frame = encode(reply)
        if reply.error is not None or getattr(reply, 'detach_reason', None) is not None:
            decoded = _ipc._decode_event_wire(b''.join(frame.parts))
            assert isinstance(decoded, _ipc._JoinReply)
            probe.record(
                'wire-reply',
                reason=getattr(decoded, 'detach_reason', None),
                error=decoded.error,
            )
        return frame

    def traced_rpc(client: _ipc.EventClient, value: _ipc._JoinValue) -> _ipc._JoinResult:
        try:
            return rpc(client, value)
        except _source.LiveChannelDetachedError as error:
            if isinstance(value, _source.JoinFinalize):
                probe.record(
                    'remote-detach',
                    reason=error.reason,
                    connected=client._coordination_socket is not None
                    and client._socket is not None,
                )
            raise

    monkeypatch.setattr(_ipc.EventServer, 'close_connection', held_close)
    monkeypatch.setattr(_ipc, '_encode_join_reply', traced_encode)
    monkeypatch.setattr(_ipc.EventClient, '_rpc', traced_rpc)


def _install_remote_detach_probe(monkeypatch: pytest.MonkeyPatch, probe: _LaneDetachProbe) -> None:
    fault = probe.patches.lane_fault
    root = probe.root
    _install_conflicting_finalize_probe(monkeypatch, probe)

    if fault in {'overflow', 'remote-queue', 'peer-disconnect'}:
        flush = _ipc._PartialWriter.flush
        close_pair = _ipc.EventServer._close_pair
        publish = _ipc.EventServer.publish_connection

        def blocked_flush(writer: _ipc._PartialWriter, sock: _ipc._SendSocket) -> None:
            if writer._max_entries == 1 and (root / 'overflow-active').exists():
                return
            flush(writer, sock)

        def traced_close(server: _ipc.EventServer, peer: _ipc._ServerPeer, *, reason: str) -> None:
            if reason == 'queue-count-exhausted':
                probe.record('server-overflow', reason=reason)
            close_pair(server, peer, reason=reason)

        def traced_publish(
            server: _ipc.EventServer,
            group: str,
            connection_id: str,
            message: _fanout.FanoutMessage,
        ) -> _ipc._EventConnectionStatus:
            if fault == 'peer-disconnect' and not (root / 'peer-disconnected').exists():
                peer = server._paired_event_peer(group, connection_id)
                assert peer is not None
                (root / 'peer-disconnected').touch()
                probe.record('injected', fault=fault)
                close_pair(server, peer, reason='peer-eof')
            return publish(server, group, connection_id, message)

        monkeypatch.setattr(_ipc._PartialWriter, 'flush', blocked_flush)
        monkeypatch.setattr(_ipc.EventServer, '_close_pair', traced_close)
        monkeypatch.setattr(_ipc.EventServer, 'publish_connection', traced_publish)


def _install_source_abort_failure(
    monkeypatch: pytest.MonkeyPatch, probe: _LaneDetachProbe
) -> None:
    if probe.patches.lane_abort_failure is None:
        return
    for client_type in (_ipc.EventClient, _worker._LocalJoinClient):

        def failed_abort(
            _client: _ipc.EventClient | _worker._LocalJoinClient,
            request: _source.JoinAbort,
        ) -> None:
            probe.record('source-abort', token=request.identity.attempt_token)
            if probe.patches.lane_abort_failure == 'transport':
                raise ConnectionError('abort transport failed')
            raise _source.JoinProtocolError('abort protocol failed')

        monkeypatch.setattr(client_type, 'abort', failed_abort)


def _install_source_promotion_probe(
    monkeypatch: pytest.MonkeyPatch, probe: _LaneDetachProbe
) -> None:
    reason = probe.patches.promotion_detach_reason
    if reason is None:
        return
    finalize = _source.JoinCoordinator.finalize
    abort = _source.JoinCoordinator._abort_record

    def traced_finalize(
        coordinator: _source.JoinCoordinator, request: _source.JoinFinalize
    ) -> _source.JoinFinalized:
        if probe.remaining:
            probe.remaining -= 1
            identity = request.identity
            committed = coordinator._state._database.connection.execute(
                'SELECT attempt_token FROM join_commits WHERE group_name = ?',
                (identity.group,),
            ).fetchone()
            assert committed == (identity.attempt_token,)
            record = coordinator._records[identity.attempt_token]
            assert record.phase == 'prepared'
            probe.record('injected', fault='source-promotion', token=identity.attempt_token)
            record.channel.detach(reason)
            (probe.root / 'events-ready').touch()
        try:
            return finalize(coordinator, request)
        except _source.LiveChannelDetachedError as error:
            probe.record(
                'source-detach', reason=error.reason, token=request.identity.attempt_token
            )
            raise

    def traced_abort(
        coordinator: _source.JoinCoordinator, record: _source._JoinRecord, reason: str
    ) -> None:
        probe.record('source-settlement', token=record.identity.attempt_token, reason=reason)
        abort(coordinator, record, reason)

    monkeypatch.setattr(_source.JoinCoordinator, 'finalize', traced_finalize)
    monkeypatch.setattr(_source.JoinCoordinator, '_abort_record', traced_abort)
    _install_promotion_transport_probe(monkeypatch, probe)


def _install_lane_detach_probe(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches, root: Path
) -> None:
    if patches.lane_fault is None:
        return
    monkeypatch.setattr(sys.modules[__name__], '_MISSING_IDS', frozenset[int]())
    _install_bounded_overflow_provider(monkeypatch, patches, root)
    probe = _LaneDetachProbe(patches, root, patches.lane_fault_count)
    _install_success_outcome_failure(monkeypatch, patches)
    _install_source_promotion_probe(monkeypatch, probe)
    _install_source_abort_failure(monkeypatch, probe)
    _install_lane_settlement_probe(monkeypatch, probe)
    _install_lane_join_detach_probe(monkeypatch, probe)
    _install_lane_live_detach_probe(monkeypatch, probe)
    _install_remote_detach_probe(monkeypatch, probe)


def _record_commit(signals: Path | None, name: str, *, record: bool) -> None:
    if record and signals is not None:
        _record_write(signals, name, 'commit')
        marker = signals / f'{name}.committed'
        if not marker.exists():
            pending = marker.with_suffix('.pending')
            pending.write_text(str(time.monotonic()), encoding='utf-8')
            pending.replace(marker)


def _release_supervisor_gates(probe: SupervisorProbe, root: Path) -> None:
    for name in ('hold-hello', 'hold-server', 'hold-page', 'hold-recovery', 'hold-initial-query'):
        (root / name).unlink(missing_ok=True)
    for spawn in probe.spawns:
        for suffix in ('write-once', 'pause-after-frame', 'freeze'):
            (root / f'{spawn.spawn_instance}.{suffix}').unlink(missing_ok=True)
    if (root / 'callback-arm').exists() or (root / 'callback-entered').exists():
        (root / 'callback-release').touch()
    (root / 'growth-callback-release').touch()


def _remote_rendezvous(
    patches: ChildPatches, groups: tuple[str, ...], connection: sqlite3.Connection
) -> None:
    signals = patches.signal_dir
    if signals is None or not (signals / 'arm').exists():
        return
    stalled = signals / f'{patches.stall_group}.stalled'
    name = threading.current_thread().name.removeprefix('sg-event-daemon:')
    threads = ('source:admin', *(f'lane:{group}' for group in groups))
    if name not in threads or stalled.exists():
        return
    if not _between_events(connection, name):
        return
    if name == 'source:admin' and not all(
        (signals / f'lane:{group}.ready').exists() for group in groups
    ):
        return
    (signals / f'{name}.ready').touch()
    if name == f'lane:{patches.stall_group}':
        while not all((signals / f'{peer}.ready').exists() for peer in threads):
            time.sleep(0.001)
    else:
        while not stalled.exists():
            time.sleep(0.001)
        primary = 'source:admin' if name == f'lane:{groups[0]}' else f'lane:{groups[1]}'
        if name not in {'source:admin', f'lane:{groups[1]}'}:
            while not (signals / f'{primary}.local').exists():
                time.sleep(0.001)


def _rendezvous_writers(patches: ChildPatches, groups: tuple[str, ...]) -> None:
    signals = patches.signal_dir
    if signals is None or patches.stall_group not in groups or not (signals / 'arm').exists():
        return
    stalled = signals / f'{patches.stall_group}.stalled'
    if stalled.exists():
        return
    name = threading.current_thread().name
    group = next((group for group in groups if name == f'sg-event-daemon:lane:{group}'), None)
    if group is None:
        return
    # Queue the existing lanes at their next genuine write, before taking any lock.
    # This prevents the holder from blocking source coordination before its peers
    # reach their own writes. No writer thread or synthetic phase is introduced.
    (signals / f'{group}.ready').touch()
    if group == patches.stall_group:
        while not all((signals / f'{peer}.ready').exists() for peer in groups):
            time.sleep(0.01)
    else:
        while not stalled.exists():
            time.sleep(0.01)


def _stall_in_transaction(connection: sqlite3.Connection, patches: ChildPatches) -> None:
    signals = patches.signal_dir
    if signals is None or not (signals / 'arm').exists():
        return
    name = threading.current_thread().name
    who = patches.stall_group if name == f'sg-event-daemon:lane:{patches.stall_group}' else None
    if patches.stall_poller and name == 'sg-event-daemon:source:admin':
        who = 'poller'
    if who is None or (signals / f'{who}.stalled').exists():
        return
    if patches.remote_rendezvous or who == patches.stall_group:
        ready = f'lane:{who}' if patches.remote_rendezvous else who
        # Arming during acquisition must not bypass this holder's rendezvous.
        if not (signals / f'{ready}.ready').exists():
            return
    # This row must disappear after SIGKILL, even though BEGIN IMMEDIATE succeeded.
    connection.execute(
        'INSERT INTO callback_marks VALUES (?, ?, ?, ?, ?, ?, ?)',
        (patches.stall_group or 'poller', 999999, 0, 'uncommitted-stall', 'stall', 'success', 1),
    )
    publish_text(signals / f'{who}.stalled', str(time.monotonic()))
    while not (signals / 'release').exists():
        time.sleep(0.01)


def _timing_episode(patches: ChildPatches) -> Path | None:
    signals = patches.signal_dir
    if not patches.timing or signals is None or not (signals / 'current').exists():
        return None
    episode = signals / (signals / 'current').read_text().strip()
    return None if (episode / 'done').exists() else episode


def _install_timing_failures(monkeypatch: pytest.MonkeyPatch, patches: ChildPatches) -> None:
    component_failed = _worker._WorkerRuntime._component_failed

    def failed(worker: _worker._WorkerRuntime, failure: str) -> None:
        episode = _timing_episode(patches)
        if episode is not None:
            name = threading.current_thread().name.removeprefix('sg-event-daemon:')
            _record_write(episode, name, 'component-failed')
            publish_text(episode / f'{name}.fatal', failure)
        component_failed(worker, failure)

    monkeypatch.setattr(_worker._WorkerRuntime, '_component_failed', failed)


def _install_timing_join(monkeypatch: pytest.MonkeyPatch, patches: ChildPatches) -> None:
    commit = _lane.GroupLane._commit_join

    def commit_join(
        lane: _lane.GroupLane, identity: LiveJoinIdentity, snapshot: GroupSnapshot
    ) -> GroupSnapshot:
        episode = _timing_episode(patches)
        if (
            episode is not None
            and identity.group == 'named'
            and (episode / 'require-joining').exists()
        ):
            publish_text(episode / 'lane:named.joining', str(time.monotonic()))
        return commit(lane, identity, snapshot)

    monkeypatch.setattr(_lane.GroupLane, '_commit_join', commit_join)


def _install_timing_locks(monkeypatch: pytest.MonkeyPatch, patches: ChildPatches) -> None:
    begin = SourceWriteLock._begin
    try_flock = _write_lock.try_flock_exclusive

    def flock(fd: int) -> bool:
        acquired = try_flock(fd)
        episode = _timing_episode(patches)
        if (
            episode is not None
            and threading.current_thread().name.endswith(':lane:named')
            and (episode / 'lane:named.entered').exists()
        ):
            if not acquired:
                (episode / 'earlier-contention').touch()
            elif (episode / 'priority-holder').exists() and not (
                episode / 'lane:holder.stalled'
            ).exists():
                _write_lock.release_flock(fd)
                return False
        return acquired

    def observed_begin(
        lock: SourceWriteLock, attempt: _Attempt, connection: sqlite3.Connection
    ) -> None:
        episode = _timing_episode(patches)
        name = threading.current_thread().name.removeprefix('sg-event-daemon:')
        if episode is None or not (episode / f'{name}.entered').exists():
            begin(lock, attempt, connection)
            return
        _record_write(episode, name, 'SQLite', attempt.deadline_at)
        begin(lock, attempt, connection)

    monkeypatch.setattr(SourceWriteLock, '_begin', observed_begin)
    monkeypatch.setattr(_write_lock, 'try_flock_exclusive', flock)


def _install_timing_poll_waits(monkeypatch: pytest.MonkeyPatch, patches: ChildPatches) -> None:
    wait = _worker._ComponentWaiter.wait
    pump = _worker._ComponentWaiter.pump
    pending: dict[_worker._ComponentWaiter, float] = {}

    def observed_wait(waiter: _worker._ComponentWaiter, seconds: float) -> bool:
        pending[waiter] = time.monotonic() + seconds
        try:
            return wait(waiter, seconds)
        finally:
            pending.pop(waiter, None)

    def observed_pump(waiter: _worker._ComponentWaiter) -> None:
        deadline = pending.pop(waiter, None)
        episode = _timing_episode(patches)
        if (
            deadline is not None
            and episode is not None
            and (episode / 'source:admin.error').exists()
        ):
            # wait() has already fixed its deadline before invoking its first service pass.
            _record_write(episode, 'source:admin', 'poll-wait', deadline)
        pump(waiter)

    monkeypatch.setattr(_worker._ComponentWaiter, 'wait', observed_wait)
    monkeypatch.setattr(_worker._ComponentWaiter, 'pump', observed_pump)


def _timing_gate(patches: ChildPatches, connection: sqlite3.Connection) -> Path | None:
    episode = _timing_episode(patches)
    name = threading.current_thread().name.removeprefix('sg-event-daemon:')
    if episode is None or name not in {'source:admin', 'lane:named', 'lane:holder'}:
        return None
    if (
        name == 'lane:named'
        and (episode / 'require-joining').exists()
        and not (episode / 'lane:named.joining').exists()
    ):
        return None
    if (episode / f'{name}.entered').exists():
        return episode
    if not _between_events(connection, name) or (
        name == 'source:admin'
        and not all((episode / f'lane:{group}.ready').exists() for group in ('named', 'holder'))
    ):
        return None
    (episode / f'{name}.ready').touch()
    while not (episode / f'{name}.go').exists() and not (episode / 'done').exists():
        time.sleep(0.001)
    if (episode / 'done').exists():
        return None
    (episode / f'{name}.entered').touch()
    return episode


def _timing_signal(episode: Path, name: str) -> None:
    pending = episode / f'{name}.pending'
    pending.write_text(str(time.monotonic()))
    pending.replace(episode / name)


def _install_timing_health(
    monkeypatch: pytest.MonkeyPatch,
    patches: ChildPatches,
    runtime: list[_worker._WorkerRuntime],
) -> None:
    publish = _worker._WorkerRuntime._publish_monitor_health

    def health(worker: _worker._WorkerRuntime, now: float) -> float | None:
        if not runtime:
            runtime.append(worker)
        episode = _timing_episode(patches)
        holder = episode / 'lane:holder.stalled' if episode is not None else None
        if (
            episode is not None
            and 'holder' in worker._spec.groups
            and (episode / 'block-monitor').exists()
        ):
            _timing_signal(episode, 'monitor-blocked')
            while not (episode / 'done').exists():
                time.sleep(0.01)
        if (
            episode is not None
            and 'holder' in worker._spec.groups
            and (episode / 'align-health').exists()
            and (episode / 'lane:holder.entered').exists()
            and holder is not None
            and not holder.exists()
        ):
            # Align the real publication, regardless of the monitor's wait transport.
            (episode / 'health-ready').touch()
            while not (episode / 'publish-health').exists() and not (episode / 'done').exists():
                time.sleep(0.001)
            now = time.monotonic()
        pending = publish(worker, now)
        if episode is not None and 'holder' in worker._spec.groups:
            _record_write(episode, 'monitor', 'health', now)
            if (
                (episode / 'align-health').exists()
                and (episode / 'lane:holder.entered').exists()
                and holder is not None
                and not holder.exists()
            ):
                _timing_signal(episode, 'health-sent')
            if (episode / 'sample-hold').exists() and holder is not None and holder.exists():
                _timing_signal(episode, 'hold-health-sent')
        return pending

    monkeypatch.setattr(_worker._WorkerRuntime, '_publish_monitor_health', health)


def _stall_timing_holder(episode: Path, name: str, runtime: list[_worker._WorkerRuntime]) -> None:
    _timing_signal(episode, f'{name}.stalled')
    if (episode / 'sample-hold').exists():
        assert runtime
        runtime[0]._monitor_wake.set()
    while not (episode / 'release').exists() and not (episode / 'done').exists():
        time.sleep(0.005)


def _install_timing_transactions(
    monkeypatch: pytest.MonkeyPatch,
    patches: ChildPatches,
    runtime: list[_worker._WorkerRuntime],
) -> None:
    transaction = SourceWriteLock.transaction
    assert patches.signal_dir is not None
    config = load_config(patches.signal_dir.parent / 'daemon.toml')
    with StallDetectors(monkeypatch).enabled('phase'):
        budget = _write_lock.write_lock_timeout_secs(
            config.daemon.health_interval_secs, config.daemon.supervisor_poll_interval_secs
        )
        grace = watchdog_grace_secs(config.daemon.health_interval_secs)

    @contextmanager
    def observed_transaction(
        lock: SourceWriteLock,
        connection: sqlite3.Connection,
        **options: Unpack[_TransactionOptions],
    ) -> Generator[sqlite3.Connection]:
        episode = _timing_gate(patches, connection)
        if episode is None:
            with transaction(lock, connection, **options) as active:
                yield active
            return
        name = threading.current_thread().name.removeprefix('sg-event-daemon:')
        holder = (episode / 'holder').read_text().strip()
        if name == holder and (episode / 'align-health').exists():
            while not (episode / 'health-sent').exists():
                time.sleep(0.001)
        assert patches.signal_dir is not None
        assert (patches.signal_dir / 'clock').exists(), 'timing episode requires a frozen clock'
        options['budget_secs'] = budget
        original_watch = options.get('watch', _write_lock._no_watch)

        @contextmanager
        def observed_watch(phase: WritePhase, deadline: float) -> Generator[None]:
            # Only this deliberately gated transaction uses production detector arithmetic.
            # Setup, recovery and unrelated components retain the ordinary lifted budgets.
            if phase == 'write-hold':
                deadline += patches.hold_deadline_secs - _write_lock.WRITE_HOLD_DEADLINE_SECS
            else:
                deadline += grace - lock.wait_phase_grace_secs
            with original_watch(phase, deadline):
                _record_write(episode, name, phase, deadline)
                yield

        options['watch'] = observed_watch
        try:
            with transaction(lock, connection, **options) as active:
                if name == holder:
                    _stall_timing_holder(episode, name, runtime)
                yield active
        except _write_lock.WriteLockTimeoutError as error:
            _record_write(episode, name, f'timeout-{error.level}')
            publish_text(episode / f'{name}.error', str(error))
            raise
        else:
            _record_write(episode, name, 'commit')

    monkeypatch.setattr(SourceWriteLock, 'transaction', observed_transaction)


def _wait_for_initial_group_retry(wait: Callable[[float | None], bool], release: Path) -> bool:
    while not release.exists() and not wait(0.005):
        pass
    return wait(0)


def _install_initial_group_query_failure(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches, root: Path
) -> None:
    if not patches.initial_group_query_failures:
        return
    latest_visible_id = _source.CatchupReader.latest_visible_id
    run_lane = _worker._LaneComponent._run_body
    active = threading.local()
    remaining = patches.initial_group_query_failures

    def first_group_query(reader: _source.CatchupReader) -> int | None:
        nonlocal remaining
        if ':lane:new' not in threading.current_thread().name:
            return latest_visible_id(reader)
        records = root / 'existing.records'
        while not read_complete_lines(records):
            time.sleep(0.005)
        deliveries = len(read_complete_lines(records))
        with (root / 'first-group-queries').open('a') as stream:
            stream.write(f'{deliveries}\n')
        if remaining:
            remaining -= 1
            active.pending_retry = True
            raise RuntimeError('injected first-query outage')
        return latest_visible_id(reader)

    def run_body(component: _worker._LaneComponent) -> None:
        if component._group != 'new':
            run_lane(component)
            return
        wait = component._stop.wait

        def waited(timeout: float | None = None) -> bool:
            # Only the failing query's lane thread owns this pending retry.
            # Other users of the same event keep their original wait behavior.
            if getattr(active, 'pending_retry', False):
                active.pending_retry = False
                with (root / 'first-group-retry-waits').open('a') as stream:
                    stream.write(f'{timeout}\n')
                # The parent advances this retry only after the existing lane
                # delivers its newly released event. Short retry values are
                # contract data here, not real-time delivery windows.
                attempt = patches.initial_group_query_failures - remaining
                release = root / f'first-group-retry-{attempt}-release'
                return _wait_for_initial_group_retry(wait, release)
            return wait(timeout)

        with monkeypatch.context() as patch:
            patch.setattr(component._stop, 'wait', waited)
            run_lane(component)

    monkeypatch.setattr(_source.CatchupReader, 'latest_visible_id', first_group_query)
    monkeypatch.setattr(_worker._LaneComponent, '_run_body', run_body)


def _wait_for_monitor_release(root: Path, spawn: str) -> None:
    while (root / f'{spawn}.freeze').exists():
        time.sleep(0.002)


def _install_control_probe(monkeypatch: pytest.MonkeyPatch, root: Path, *, enabled: bool) -> None:
    if not enabled:
        return
    send = _ipc.WorkerControl.send
    enqueue = _ipc.WorkerControlOutbox.enqueue
    current_health = _worker._WorkerRuntime._current_health
    request_stop = _worker._WorkerRuntime.request_stop
    next_frame = _ipc.WorkerControlOutbox._next_frame
    spawn = sys.argv[sys.argv.index('--spawn-instance') + 1]

    def reject_bounded_health_send(
        control: _ipc.WorkerControl, message: _ipc.ControlMessage, *, deadline_secs: float
    ) -> None:
        assert not isinstance(message, _ipc.HealthUpdate), 'health used a bounded control send'
        send(control, message, deadline_secs=deadline_secs)

    def queued(outbox: _ipc.WorkerControlOutbox, message: _ipc.ControlMessage) -> None:
        enqueue(outbox, message)
        _record_queued_control_result(root, spawn, message)

    def frame(outbox: _ipc.WorkerControlOutbox) -> bool:
        pending = bool(outbox._unsent)
        selected = next_frame(outbox)
        if (
            selected
            and not pending
            and outbox._selected_health is not None
            and len(outbox._selected_health.worker.thread_names[-1]) > 10_000
        ):
            with (root / f'{spawn}.control.jsonl').open('a') as stream:
                stream.write(json.dumps({'kind': 'frame-started', 'at': time.monotonic()}) + '\n')
        return selected

    def health(runtime: _worker._WorkerRuntime, now: float) -> _worker.WorkerHealth:
        control = runtime._control
        assert control is not None
        control.open_socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        if (root / f'{runtime._spawn_instance}.freeze').exists():
            _record_control_event(root, runtime, 'frozen')
            # The monitor stays blocked until containment or fixture cleanup.
            _wait_for_monitor_release(root, runtime._spawn_instance)
        result = current_health(runtime, now)
        if (root / f'{runtime._spawn_instance}.large').exists():
            result = replace(result, thread_names=(*result.thread_names, 'x' * 60_000))
        _record_control_event(
            root, runtime, 'sample', sample=now, large=len(result.thread_names[-1]) > 10_000
        )
        return result

    def stop(runtime: _worker._WorkerRuntime, reason: str) -> None:
        _record_control_event(root, runtime, 'stop', reason=reason)
        request_stop(runtime, reason)

    monkeypatch.setattr(_ipc.WorkerControl, 'send', reject_bounded_health_send)
    monkeypatch.setattr(_ipc.WorkerControlOutbox, 'enqueue', queued)
    monkeypatch.setattr(_ipc.WorkerControlOutbox, '_next_frame', frame)
    monkeypatch.setattr(_worker._WorkerRuntime, '_current_health', health)
    monkeypatch.setattr(_worker._WorkerRuntime, 'request_stop', stop)
    _install_control_command_probe(monkeypatch, root)


def _wait_initial_query_gate(root: Path) -> None:
    if ':source:' in threading.current_thread().name and (root / 'hold-initial-query').exists():
        (root / 'initial-query-entered').touch()
        raise RuntimeError('initial source query held by test')


def _install_lifecycle_calls(
    monkeypatch: pytest.MonkeyPatch, root: Path, initial_source_query_failures: int
) -> None:
    find = DeterministicHandle.find
    local_observe = _worker._LocalJoinClient.observe_group
    remote_observe = _ipc.EventClient.observe_group
    poll_lane = _worker._LaneComponent._poll_lane

    def page(
        handle: DeterministicHandle,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        nonlocal initial_source_query_failures
        _wait_initial_query_gate(root)
        if initial_source_query_failures and ':source:' in threading.current_thread().name:
            initial_source_query_failures -= 1
            raise RuntimeError('first source query temporarily unavailable')
        if (root / 'hold-page').exists() and ':source:' in threading.current_thread().name:
            (root / 'page-entered').touch()
            while (root / 'hold-page').exists():
                time.sleep(0.002)
        return find(handle, entity_type, filters, fields, **options)

    def observe(
        client: _worker._LocalJoinClient, assignment: _ipc.GroupBootstrapRequest
    ) -> _ipc.GroupBootstrapObservation | _ipc.GroupBootstrapPending:
        (root / 'local-observe-entered').touch()
        result = local_observe(client, assignment)
        (root / 'local-observe-returned').touch()
        return result

    def observe_remote(
        client: _ipc.EventClient, assignment: _ipc.GroupBootstrapRequest
    ) -> _ipc.GroupBootstrapObservation | _ipc.GroupBootstrapPending:
        result = remote_observe(client, assignment)
        if isinstance(result, _ipc.GroupBootstrapPending):
            (root / 'remote-bootstrap-pending').touch()
        return result

    def lane(
        component: _worker._LaneComponent, client: _ipc.EventClient | _worker._LocalJoinClient
    ) -> None:
        if isinstance(client, _worker._LocalJoinClient) and (root / 'local-call-arm').exists():
            (root / 'local-call-arm').unlink(missing_ok=True)
            publish_text(root / 'local-call-entered', str(time.monotonic()))
            client._source.call(
                lambda surface: surface.state.snapshot(),
                wait_phase=client._wait_phase,
                stop=client._stop,
            )
            publish_text(root / 'local-call-returned', str(time.monotonic()))
        poll_lane(component, client)

    monkeypatch.setattr(DeterministicHandle, 'find', page)
    monkeypatch.setattr(_worker._LocalJoinClient, 'observe_group', observe)
    monkeypatch.setattr(_ipc.EventClient, 'observe_group', observe_remote)
    monkeypatch.setattr(_worker._LaneComponent, '_poll_lane', lane)


def _install_lifecycle_probe(
    monkeypatch: pytest.MonkeyPatch, root: Path, *, patches: ChildPatches
) -> None:
    if not patches.lifecycle_probe:
        return
    spawn = sys.argv[sys.argv.index('--spawn-instance') + 1]
    _install_lifecycle_bootstrap(monkeypatch, root, spawn)
    _install_lifecycle_output(monkeypatch, root, spawn)
    _install_lifecycle_calls(monkeypatch, root, patches.initial_source_query_failures)
    _install_lifecycle_publications(monkeypatch, root, spawn)
    resolve = _worker._LaneComponent._resolve_conditional_park

    def recovery(
        component: _worker._LaneComponent, context: _worker._LaneConfiguration, state: GroupState
    ) -> _worker._LaneConfiguration:
        if context.assignment.conditional_park is not None:
            (root / 'recovery-entered').touch()
            while (root / 'hold-recovery').exists():
                time.sleep(0.002)
        return resolve(component, context, state)

    monkeypatch.setattr(_worker._LaneComponent, '_resolve_conditional_park', recovery)


def child_code(config_path: Path, patches: ChildPatches) -> str:
    signals = 'None' if patches.signal_dir is None else f'Path({str(patches.signal_dir)!r})'
    return (
        f'import sys; sys.path.insert(0, {str(_ROOT)!r})\n'
        'from pathlib import Path\n'
        'from tests._worker_processes import ChildPatches, run_worker\n'
        f'run_worker(Path({str(config_path)!r}), ChildPatches('
        f'hold_deadline_secs={patches.hold_deadline_secs!r}, '
        f'probe_wal_restart={patches.probe_wal_restart!r}, '
        f'outside_reader={patches.outside_reader!r}, '
        f'outside_frontier_cap={patches.outside_frontier_cap!r}, '
        f'outside_pause_observation={patches.outside_pause_observation!r}, '
        f'combined_delivery_gate={patches.combined_delivery_gate!r}, '
        f'stall_group={patches.stall_group!r}, stall_poller={patches.stall_poller!r}, '
        f'signal_dir={signals}, '
        f'mask_write_hold={patches.mask_write_hold!r}, '
        f'last_event_id={patches.last_event_id!r}, '
        f'remote_rendezvous={patches.remote_rendezvous!r}, '
        f'track_revisions={patches.track_revisions!r}, timing={patches.timing!r}, '
        f'gate_events={patches.gate_events!r}, '
        f'checkpoint_clock={patches.checkpoint_clock!r}, '
        f'checkpoint_wal_limit_bytes={patches.checkpoint_wal_limit_bytes!r}, '
        f'hold_provider={patches.hold_provider!r}, gate_lane_write={patches.gate_lane_write!r}, '
        f'prioritize_competitor={patches.prioritize_competitor!r}, '
        f'writer_wal_limit_bytes={patches.writer_wal_limit_bytes!r}, '
        f'probe_startup={patches.probe_startup!r}, '
        f'hold_callback={patches.hold_callback!r}, '
        f'probe_open_file_limit={patches.probe_open_file_limit!r}, '
        f'lane_fault={patches.lane_fault!r}, lane_fault_count={patches.lane_fault_count!r}, '
        f'lane_detach_reason={patches.lane_detach_reason!r}, '
        f'lane_abort_failure={patches.lane_abort_failure!r}, '
        f'lane_outcome_failure={patches.lane_outcome_failure!r}, '
        f'promotion_detach_reason={patches.promotion_detach_reason!r}, '
        f'lane_outcome_write_fault={patches.lane_outcome_write_fault!r}, '
        f'probe_coordination={patches.probe_coordination!r}, '
        f'source_halt={patches.source_halt!r}, '
        f'busy_source_case={patches.busy_source_case!r}, '
        f'wait_case={patches.wait_case!r}, '
        f'control_probe={patches.control_probe!r}, '
        f'lifecycle_probe={patches.lifecycle_probe!r}, '
        f'initial_source_query_failures={patches.initial_source_query_failures!r}, '
        f'initial_group_query_failures={patches.initial_group_query_failures!r}, '
        f'async_provider={patches.async_provider!r}), '
        f'stall_detectors={active_stall_detectors()!r})\n'
    )


def install_checkpoints(
    monkeypatch: pytest.MonkeyPatch, log_path: Path, *, advance_clock: bool = False
) -> None:
    original = Checkpointer.run_once
    passive = Checkpointer._passive_checkpoint
    sync = Checkpointer._sync
    clocks: dict[Checkpointer, float] = {}
    sync_counts: dict[Checkpointer, int] = {}

    def checkpoint_try(checkpointer: Checkpointer) -> _CheckpointResult:
        started = time.monotonic()
        result: _CheckpointResult | None = None
        error: BaseException | None = None
        try:
            result = passive(checkpointer)
        except BaseException as caught:
            error = caught
            raise
        else:
            return result
        finally:
            finished = time.monotonic()
            # Measure the SQLite call, excluding the diagnostic file write.
            # Preserve the original failure if the diagnostic path is unavailable.
            with (
                suppress(OSError),
                log_path.with_suffix('.checkpoint-tries').open('a', encoding='utf-8') as stream,
            ):
                stream.write(
                    json.dumps(
                        {
                            'pid': os.getpid(),
                            'path': str(checkpointer._path),
                            'started': started,
                            'finished': finished,
                            'result': result,
                            'error': repr(error) if error is not None else None,
                        }
                    )
                    + '\n'
                )

    def checkpoint(checkpointer: Checkpointer) -> CheckpointAttempt:
        if advance_clock:
            if checkpointer not in clocks:
                clocks[checkpointer] = 0.0
                checkpointer._cleared_at = 0.0
                checkpointer._clock = lambda: clocks[checkpointer]
            clocks[checkpointer] += checkpointer._interval_secs
        before_syncs = sync_counts.get(checkpointer, 0)
        attempt = original(checkpointer)
        with log_path.with_suffix('.checkpoints').open('a', encoding='utf-8') as stream:
            stream.write(
                json.dumps(
                    {
                        'outcome': attempt.outcome,
                        'reuse': attempt.reuse,
                        'at': time.monotonic(),
                        'synced': sync_counts.get(checkpointer, 0) > before_syncs,
                        'crossed': sorted(checkpointer.crossed),
                    }
                )
                + '\n'
            )
        return attempt

    def sync_wal(checkpointer: Checkpointer) -> None:
        sync(checkpointer)
        sync_counts[checkpointer] = sync_counts.get(checkpointer, 0) + 1
        with log_path.with_suffix('.syncs').open('a') as stream:
            stream.write(f'{time.monotonic()}\n')

    monkeypatch.setattr(Checkpointer, '_sync', sync_wal)
    monkeypatch.setattr(Checkpointer, 'run_once', checkpoint)
    monkeypatch.setattr(Checkpointer, '_passive_checkpoint', checkpoint_try)


def install_faults(
    monkeypatch: pytest.MonkeyPatch,
    patches: ChildPatches,
    groups: tuple[str, ...],
    idle_interval_secs: float,
) -> None:
    _install_competitor_schedule(monkeypatch, patches)
    original_transaction = SourceWriteLock.transaction
    original_sample = ComponentWatchdog.sample
    original_acquire_file = SourceWriteLock._acquire_file

    def acquire_file(lock: SourceWriteLock, attempt: _Attempt) -> _FileLease:
        signals = patches.signal_dir
        if (
            patches.remote_rendezvous
            and signals is not None
            and (signals / f'{patches.stall_group}.stalled').exists()
        ):
            name = threading.current_thread().name.removeprefix('sg-event-daemon:')
            (signals / f'{name}.local').touch()
        return original_acquire_file(lock, attempt)

    @contextmanager
    def transaction(
        lock: SourceWriteLock,
        connection: sqlite3.Connection,
        **options: Unpack[_TransactionOptions],
    ) -> Generator[sqlite3.Connection]:
        if patches.remote_rendezvous:
            _remote_rendezvous(patches, groups, connection)
        else:
            _rendezvous_writers(patches, groups)
        signals = patches.signal_dir
        name = threading.current_thread().name.removeprefix('sg-event-daemon:')
        record = (
            signals is not None
            and (
                signals
                / (f'{patches.stall_group}.stalled' if patches.remote_rendezvous else 'arm')
            ).exists()
        )
        _gate_lane_write(patches, signals, name, record=record)
        original_watch = options.get('watch', _write_lock._no_watch)

        @contextmanager
        def watch(phase: WritePhase, deadline: float) -> Generator[None]:
            with original_watch(phase, deadline):
                if record and signals is not None:
                    _record_write(signals, name, phase, deadline)
                yield

        options['watch'] = watch
        with (
            _record_attempt(signals, name, record=record),
            original_transaction(lock, connection, **options) as active,
        ):
            _stall_in_transaction(active, patches)
            yield active
        _record_commit(signals, name, record=record)

    def sample(watchdog: ComponentWatchdog) -> WatchdogSample:
        result = _controlled_hold_sample(original_sample(watchdog), patches)
        if (
            result.kind == 'lane'
            and result.identity == patches.mask_write_hold
            and result.phase == 'write-hold'
            and not (
                patches.signal_dir is not None and (patches.signal_dir / 'expire-hold').exists()
            )
        ):
            now = time.monotonic()
            return replace(
                result, phase='idle', heartbeat_at=now, deadline_at=now + idle_interval_secs
            )
        return result

    monkeypatch.setattr(
        _write_lock,
        'WRITE_HOLD_DEADLINE_SECS',
        stall_deadline('phase', patches.hold_deadline_secs),
    )
    monkeypatch.setattr(SourceWriteLock, 'transaction', transaction)
    monkeypatch.setattr(SourceWriteLock, '_acquire_file', acquire_file)
    monkeypatch.setattr(ComponentWatchdog, 'sample', sample)


def install_timing_clock(monkeypatch: pytest.MonkeyPatch, signals: Path) -> None:
    raw_clock = time.monotonic

    def clock() -> float:
        frozen = signals / 'clock'
        try:
            return float(frozen.read_text())
        except FileNotFoundError:
            offset = signals / 'clock-offset'
            return raw_clock() + (float(offset.read_text()) if offset.exists() else 0.0)

    # threading captures its native wait clock at import, so conditions still block normally.
    # All daemon-owned clocks constructed after this point share the controlled timeline.
    monkeypatch.setattr(time, 'monotonic', clock)


def install_timing_samples(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches, idle_interval_secs: float
) -> None:
    sample = ComponentWatchdog.sample

    def watch_sample(watchdog: ComponentWatchdog) -> WatchdogSample:
        result = sample(watchdog)
        episode = _timing_episode(patches)
        if episode is None:
            return result
        name = f'{result.kind}:{result.identity}'
        gated = (episode / f'{name}.ready').exists() and not (episode / f'{name}.entered').exists()
        masked = (
            result.kind == 'lane'
            and result.identity == 'holder'
            and result.phase == 'write-hold'
            and (episode / 'mask-holder').exists()
        )
        if gated or masked:
            now = time.monotonic()
            return replace(
                result,
                phase='idle' if masked else result.phase,
                heartbeat_at=now,
                deadline_at=now + idle_interval_secs,
            )
        return result

    monkeypatch.setattr(ComponentWatchdog, 'sample', watch_sample)


def _install_timing(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches, idle_interval_secs: float
) -> None:
    runtime: list[_worker._WorkerRuntime] = []
    _install_timing_poll_waits(monkeypatch, patches)
    _install_timing_health(monkeypatch, patches, runtime)
    _install_timing_failures(monkeypatch, patches)
    _install_timing_join(monkeypatch, patches)
    _install_timing_locks(monkeypatch, patches)
    install_timing_samples(monkeypatch, patches, idle_interval_secs)
    _install_timing_transactions(monkeypatch, patches, runtime)


def run_competing_writer(path: Path, signals: Path) -> None:
    lock = SourceWriteLock(
        path.with_suffix('.db.write-lock'),
        database_path=path,
        timeout_secs=STALL_DEADLINE_SECS,
        wait_phase_grace_secs=STALL_DEADLINE_SECS,
    )
    try:
        with closing(sqlite3.connect(path, isolation_level=None)) as connection, connection:
            while not (signals / 'competitor-pause').exists():
                with lock.transaction(connection):
                    connection.execute('UPDATE source_meta SET index_revision = index_revision')
                    (signals / 'competitor-active').touch()
                    time.sleep(0.02)
                with (signals / 'competitor-commits').open('a') as stream:
                    stream.write('commit\n')
        (signals / 'competitor-active').unlink(missing_ok=True)
    finally:
        lock.close()


@contextmanager
def running_supervisor(config_path: Path, patches: ChildPatches) -> Generator[SupervisorProbe]:
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
        monkeypatch.setattr(
            _write_lock,
            'WRITE_HOLD_DEADLINE_SECS',
            stall_deadline('phase', patches.hold_deadline_secs),
        )
        supervisor = Supervisor(
            config=lift_config(load_config(config_path)), config_path=config_path
        )
        probe = SupervisorProbe(supervisor, config_path, patches)
        original_kill = _WorkerInstance.kill
        original_wait = _WorkerInstance.wait
        original_mark = Supervisor._mark_current_worker_unready
        original_health = Supervisor._record_worker_health

        def record_cap_health(
            owner: Supervisor, worker: _supervisor._WorkerHandle, message: HealthUpdate, now: float
        ) -> bool:
            accepted = original_health(owner, worker, message, now)
            if accepted:
                probe.cap_heartbeats.append(
                    (message.worker.spawn_instance, message.worker.heartbeat_at, time.monotonic())
                )
            return accepted

        if (
            patches.wait_case in {'connect-cap', 'connect-auth-cap'}
            or patches.busy_source_case == 'remote-cap'
        ):
            monkeypatch.setattr(Supervisor, '_record_worker_health', record_cap_health)

        def kill(worker: _WorkerInstance) -> None:
            probe.record_termination(worker, 'shutdown')
            original_kill(worker)

        def wait(worker: _WorkerInstance, *, timeout: float = 0.0) -> int | None:
            result = original_wait(worker, timeout=timeout)
            if result is not None:
                probe.record_termination(worker, f'worker-exit-{result}')
            return result

        def mark_unready(
            owner: Supervisor,
            worker: _supervisor._WorkerHandle,
            reason: str,
            watchdog_failure: bool,
        ) -> bool:
            if (
                (spawn := owner._spawns.get(worker.worker_id)) is not None
                and spawn.worker is worker
                and spawn.termination_reason is None
            ):
                probe.health_before_containment.setdefault(
                    worker.spawn_instance, probe.latest_health.get(worker.worker_id)
                )
            return original_mark(owner, worker, reason, watchdog_failure)

        monkeypatch.setattr(Supervisor, '_mark_current_worker_unready', mark_unready)
        monkeypatch.setattr(_supervisor, 'spawn_child', probe.spawn)
        monkeypatch.setattr(_WorkerInstance, 'kill', kill)
        monkeypatch.setattr(_WorkerInstance, 'wait', wait)
        try:
            with supervisor:
                try:
                    supervisor.reconcile_durable_state()
                    supervisor.start()
                    yield probe
                finally:
                    _release_supervisor_gates(probe, config_path.parent)
        except BaseException as error:
            error.add_note(probe.failure_details())
            raise


class _OperationClock:
    def __init__(self) -> None:
        self._local = threading.local()

    def clear(self) -> None:
        if hasattr(self._local, 'now'):
            del self._local.now

    def monotonic(self) -> float:
        return getattr(self._local, 'now', time.monotonic())

    def advance(self, seconds: float) -> None:
        self._local.now = self.monotonic() + seconds

    def start(self) -> None:
        self._local.now = time.monotonic()

    @staticmethod
    def time() -> float:
        return time.time()

    @staticmethod
    def sleep(seconds: float) -> None:
        time.sleep(seconds)


@dataclass
class _BusySourceProbe:
    case: str
    root: Path
    active: bool = False
    finished: bool = False
    detached: bool = False
    page: int = 0
    clock: _OperationClock = field(default_factory=_OperationClock)
    calls: threading.local = field(default_factory=threading.local)

    def record(self, kind: str, **values: object) -> None:
        with (self.root / 'busy-source.jsonl').open('a') as stream:
            stream.write(
                json.dumps(
                    {
                        'kind': kind,
                        'at': time.monotonic(),
                        'logical_at': self.clock.monotonic(),
                        **values,
                    }
                )
                + '\n'
            )


class _OutsideWarnings(logging.Handler):
    def __init__(self, observe: Callable[..., None]) -> None:
        super().__init__()
        self._observe = observe

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if record.levelno < logging.WARNING:
            return
        if message.startswith('checkpoint '):
            category = 'checkpoint:' + message.split(' bound exceeded', 1)[0].removeprefix(
                'checkpoint '
            )
        elif message.startswith('writes to '):
            category = 'writer:' + str(getattr(record, 'group', 'source'))
        else:
            return
        self._observe('warnings', str(getattr(record, 'group', 'site')), category=category)


def _busy_coordination_budget(probe: _BusySourceProbe) -> float:
    config = load_config(probe.root / 'daemon.toml')
    site = config.sites['test']
    return internal_stall_deadline_secs(
        config.daemon.health_interval_secs, site.credentials[site.sources['admin'].credentials]
    )


def _finish_cap_wait(probe: _BusySourceProbe, active: threading.local) -> None:
    if active.cap_entered:
        probe.record('cap-exit')
        (probe.root / 'cap-finished').touch()
    client = getattr(active, 'client', None)
    if client is not None and hasattr(active, 'previous_cap'):
        client._coordination_deadline_secs = active.previous_cap
        client._wait_slice_secs = active.previous_slice
    active.client = None
    probe.clock.clear()


def _install_bootstrap_clock(monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe) -> None:
    observe = _ipc.EventClient.observe_group
    poll = _ipc.EventClient._poll_coordination
    active = threading.local()
    monkeypatch.setattr(_ipc, 'time', probe.clock)

    def aged_observe(client: _ipc.EventClient, request: _ipc.GroupBootstrapRequest) -> object:
        if (probe.root / 'bootstrap-expired').exists():
            return observe(client, request)
        previous_cap = client._coordination_deadline_secs
        previous_slice = client._wait_slice_secs
        client._coordination_deadline_secs = _busy_coordination_budget(probe)
        active.client = client
        probe.clock.start()
        probe.record('observe-enter', deadline_secs=client._coordination_deadline_secs)
        probe.clock.advance(0.2)
        probe.record('observe-send', deadline_secs=client._coordination_deadline_secs)
        try:
            return observe(client, request)
        finally:
            client._coordination_deadline_secs = previous_cap
            client._wait_slice_secs = previous_slice
            active.client = None
            probe.clock.clear()

    def controlled_poll(client: _ipc.EventClient, timeout: float) -> None:
        if getattr(active, 'client', None) is client and timeout > 0:
            probe.clock.advance(timeout)
            return
        poll(client, timeout)

    monkeypatch.setattr(_ipc.EventClient, 'observe_group', aged_observe)
    monkeypatch.setattr(_ipc.EventClient, '_poll_coordination', controlled_poll)


def _install_busy_source_lane(monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe) -> None:
    prepare = _lane.GroupLane._prepare_live_drain
    catch_up = _worker._LaneComponent._catch_up

    def detach_for_call(
        lane: _lane.GroupLane, channel: GroupChannelInterface, idle: GroupSnapshot | None
    ) -> tuple[_lane._FinalizedJoin, GroupSnapshot | None]:
        if (
            not probe.detached
            and (probe.root / 'busy-arm').exists()
            and (not probe.case.startswith('operator'))
        ):
            probe.detached = True
            channel.detach('strict-buffer-capacity')
            probe.record('detach')
        return prepare(lane, channel, idle)

    def traced_catch_up(
        component: _worker._LaneComponent, resources: _worker._LaneResources, frontier: int
    ) -> None:
        if (probe.root / 'busy-entered').exists():
            probe.record('catchup')
        catch_up(component, resources, frontier)

    monkeypatch.setattr(_lane.GroupLane, '_prepare_live_drain', detach_for_call)
    monkeypatch.setattr(_worker._LaneComponent, '_catch_up', traced_catch_up)


def _install_busy_source_queries(monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe) -> None:
    find = DeterministicHandle.find

    def busy_find(
        handle: DeterministicHandle,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        if probe.active and ':source:' in threading.current_thread().name:
            probe.page += 1
            probe.record('page-start', page=probe.page)
            (probe.root / 'busy-query-entered').touch()
            if probe.case == 'long-recheck' and probe.page == 1:
                # Admit the join only after the first pre-query service pass,
                # then keep this page open until its exact mailbox submission.
                while not (probe.root / 'source-join-submitted').exists():
                    time.sleep(0.001)
            sg = EventLogSG(handle.event(i) for i in range(2, 82))
            rows = sg.find(entity_type, filters, fields, **options)
            probe.record('page-end', page=probe.page)
            return rows
        if (probe.root / 'busy-entered').exists() and ':lane:' in threading.current_thread().name:
            return EventLogSG(handle.event(i) for i in range(1, 83)).find(
                entity_type, filters, fields, **options
            )
        return find(handle, entity_type, filters, fields, **options)

    monkeypatch.setattr(DeterministicHandle, 'find', busy_find)


def _install_busy_source_recheck(monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe) -> None:
    recheck = _source.SourcePoller._recheck_missing

    def busy_recheck(poller: _source.SourcePoller) -> None:
        if (
            probe.finished
            or not (probe.root / 'busy-arm').exists()
            or (
                not probe.case.startswith('operator')
                and (not (probe.root / 'join-ready').exists())
            )
        ):
            recheck(poller)
            return
        probe.finished = True
        if probe.case == 'operator-started':
            recheck(poller)
            return
        state = poller._state
        if probe.case == 'long-recheck':
            state.commit_observations((82,), successful_clock=state.successful_clock())
            poller._config = replace(poller._config, batch_size=2)
        elif probe.case in {'slow-page', 'remote-cap', 'operator-expired', 'operator-retry'}:
            state.commit_observations(
                tuple(range(3, 42, 2)), successful_clock=state.successful_clock()
            )
            poller._config = replace(poller._config, batch_size=100)
        probe.active = True
        probe.record('cycle-start')
        (probe.root / 'busy-entered').touch()
        if probe.case in {'blocked-resume', 'cooperative-stop'}:
            with poller._watchdog.watch(
                'provider-query', deadline_at=time.monotonic() + poller._internal_deadline_secs
            ):
                while not (probe.root / 'busy-release').exists() and (not poller._poller_stopped):
                    time.sleep(0.005)
        else:
            if probe.case in {'operator-expired', 'operator-retry'}:
                while not (probe.root / 'operator-call-finished').exists():
                    time.sleep(0.001)
            recheck(poller)
        probe.record('cycle-end', pages=probe.page)
        probe.active = False
        (probe.root / 'busy-finished').touch()

    monkeypatch.setattr(_source.SourcePoller, '_recheck_missing', busy_recheck)


def _install_busy_source_remote(monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe) -> None:
    detach = _ipc.EventClient._detach
    dispatch = _ipc.EventServer.dispatch_join_requests

    def traced_detach(client: _ipc.EventClient, reason: str) -> None:
        if (probe.root / 'busy-entered').exists() or probe.case == 'remote-bootstrap':
            probe.record('remote-detach', reason=reason)
        if probe.case == 'remote-bootstrap' and reason == 'coordination-reply-timeout':
            (probe.root / 'bootstrap-expired').touch()
        detach(client, reason)

    def hold_bootstrap(server: _ipc.EventServer, source: _ipc._JoinSource) -> None:
        if probe.case == 'remote-bootstrap' and not (probe.root / 'bootstrap-expired').exists():
            return
        dispatch(server, source)

    monkeypatch.setattr(_ipc.EventClient, '_detach', traced_detach)
    monkeypatch.setattr(_ipc.EventServer, 'dispatch_join_requests', hold_bootstrap)
    if probe.case == 'remote-bootstrap':
        _install_bootstrap_clock(monkeypatch, probe)


def _install_busy_source_writes(monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe) -> None:
    record_missing = _source.SourceState.record_missing_rows

    def gated_write(
        state: _source.SourceState,
        obligation_id: str,
        event_ids: Sequence[int],
        *,
        successful_clock: float,
    ) -> _source.SourceCommitResult:
        active = probe.active and probe.case in {'slow-page', 'remote-cap'}
        if active:
            gate = 'source-join-submitted' if probe.case == 'slow-page' else 'busy-call-queued'
            while not (probe.root / gate).exists():
                time.sleep(0.001)
            if probe.case == 'remote-cap':
                while not (probe.root / 'cap-finished').exists():
                    time.sleep(0.001)
            probe.record('write-start', obligation=obligation_id)
        try:
            return record_missing(
                state, obligation_id, event_ids, successful_clock=successful_clock
            )
        finally:
            if active:
                probe.record('write-end', obligation=obligation_id)

    monkeypatch.setattr(_source.SourceState, 'record_missing_rows', gated_write)


def _install_connection_socket_faults(
    monkeypatch: pytest.MonkeyPatch,
    probe: _BusySourceProbe,
    active: threading.local,
) -> None:
    case, root = probe.case, probe.root
    socket_connect = socket.socket.connect
    socket_send = socket.socket.send

    def block_socket(sock: socket.socket) -> None:
        client = getattr(active, 'client', None)
        if client is None or (case.endswith('cap') and probe.finished):
            return
        if case.endswith('cap') and not active.cap_entered:
            active.cap_entered = True
            probe.record('cap-enter', spawn_instance=client._spawn_instance)
        timeout = sock.gettimeout()
        assert timeout is not None
        assert timeout > 0
        probe.record('socket-wait', timeout=timeout)
        (root / 'wait-entered').touch()
        if case.endswith('cap'):
            while not (root / 'cap-heartbeat.jsonl').exists():
                assert not client._stop.is_set(), 'client stopped before heartbeat receipt'
                time.sleep(0.001)
            probe.clock.advance(timeout)
        else:
            client._stop.wait(timeout)
        raise TimeoutError('injected full event socket')

    def blocked_connect(sock: socket.socket, address: str) -> None:
        if 'auth' not in case:
            block_socket(sock)
        socket_connect(sock, address)

    def blocked_send(sock: socket.socket, data: bytes | memoryview, flags: int = 0) -> int:
        if 'auth' in case:
            block_socket(sock)
        return socket_send(sock, data, flags)

    monkeypatch.setattr(socket.socket, 'connect', blocked_connect)
    monkeypatch.setattr(socket.socket, 'send', blocked_send)


def _install_connection_wait_probe(
    monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe
) -> None:
    case, root = probe.case, probe.root
    connect = _ipc.EventClient.connect
    poll = _ipc.EventClient.poll
    active = threading.local()
    monkeypatch.setattr(_ipc, 'time', probe.clock)

    def connect_client(client: _ipc.EventClient) -> None:
        if case.startswith('reconnect') and not (root / 'wait-arm').exists():
            connect(client)
            return
        active.client = client
        active.cap_entered = False
        active.previous_cap = client._coordination_deadline_secs
        active.previous_slice = client._wait_slice_secs
        if case.endswith('cap') and not probe.finished:
            client._coordination_deadline_secs = _busy_coordination_budget(probe)
            probe.clock.start()
            probe.record('connect-queued')
            probe.clock.advance(0.2)
        probe.record('connect-attempt', deadline_secs=client._coordination_deadline_secs)
        try:
            connect(client)
        except ConnectionError:
            probe.record('connect-retry')
            probe.finished = True
            raise
        finally:
            _finish_cap_wait(probe, active)

    def detach_for_reconnect(client: _ipc.EventClient, timeout: float = 0.0) -> int:
        if case.startswith('reconnect') and (root / 'wait-arm').exists() and not probe.detached:
            probe.detached = True
            client._detach('test-reconnect')
        return poll(client, timeout)

    monkeypatch.setattr(_ipc.EventClient, 'connect', connect_client)
    monkeypatch.setattr(_ipc.EventClient, 'poll', detach_for_reconnect)
    _install_connection_socket_faults(monkeypatch, probe, active)


def _install_lane_call_wait_probe(
    monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe
) -> None:
    original_call = _worker._LaneComponent._call
    execute = _worker._LaneComponent.execute_operator_command
    service = _worker._LaneComponent._service_calls
    mailbox_submit = _worker._OwnerMailbox[object].submit
    active = threading.local()
    held: set[_worker._LaneComponent] = set()

    def command(
        component: _worker._LaneComponent,
        message: _ipc.OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        previous = getattr(active, 'component', None)
        active.component = (
            component if component._group == 'waiting' and message.command == 'retry' else None
        )
        try:
            return execute(component, message, wait_phase=wait_phase)
        finally:
            active.component = previous

    def submit(
        mailbox: _worker._OwnerMailbox[object], operation: Callable[[object], object]
    ) -> _worker._OwnerCall[object]:
        call = mailbox_submit(mailbox, operation)
        if mailbox is getattr(active, 'mailbox', None):
            wait = call.complete.wait

            def waited(timeout: float | None = None) -> bool:
                probe.record('lane-result-wait', timeout=timeout)
                (probe.root / 'wait-entered').touch()
                return wait(timeout)

            monkeypatch.setattr(call.complete, 'wait', waited)
        return call

    def queued_call(
        component: _worker._LaneComponent,
        invoke: Callable[[GroupState, _ipc.EventClient | _worker._LocalJoinClient], object],
    ) -> object:
        if component is not getattr(active, 'component', None):
            return original_call(component, invoke)
        previous = getattr(active, 'mailbox', None)
        active.mailbox = component._mailbox
        held.add(component)
        try:
            return original_call(component, invoke)
        finally:
            held.remove(component)
            active.mailbox = previous

    def defer_selected_call(
        component: _worker._LaneComponent, resources: _worker._LaneResources
    ) -> None:
        if component not in held:
            service(component, resources)

    monkeypatch.setattr(_worker._LaneComponent, 'execute_operator_command', command)
    monkeypatch.setattr(_worker._OwnerMailbox, 'submit', submit)
    monkeypatch.setattr(_worker._LaneComponent, '_call', queued_call)
    monkeypatch.setattr(_worker._LaneComponent, '_service_calls', defer_selected_call)


def _install_remote_cap_poll(
    monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe, active: threading.local
) -> None:
    poll = _ipc.EventClient._poll_coordination
    monkeypatch.setattr(_ipc, 'time', probe.clock)

    def traced_poll(client: _ipc.EventClient, timeout: float) -> None:
        if getattr(active, 'client', None) is client and timeout > 0:
            if not active.cap_entered:
                active.cap_entered = True
                probe.record('cap-enter', spawn_instance=client._spawn_instance)
            probe.record('request-wait', timeout=timeout)
            while not (probe.root / 'cap-heartbeat.jsonl').exists():
                assert not client._stop.is_set(), 'client stopped before heartbeat receipt'
                time.sleep(0.001)
            probe.clock.advance(timeout)
            return
        poll(client, timeout)

    monkeypatch.setattr(_ipc.EventClient, '_poll_coordination', traced_poll)


def _install_retry_service_probe(monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe) -> None:
    root = probe.root
    poll_once = _source.SourcePoller._poll_once
    service = _worker._SourceComponent._service_internal
    wait = _worker._ComponentWaiter.wait
    active_poller: list[_source.SourcePoller] = []

    def failing_poll(poller: _source.SourcePoller) -> int:
        if (root / 'wait-arm').exists() and not probe.active:
            probe.active = True
            active_poller.append(poller)
            raise OSError('injected retry')
        return poll_once(poller)

    def observed_service(
        owner: _worker._SourceComponent,
        server: _ipc.EventServer,
        surface: _worker._SourceJoinSurface,
        poller: _source.SourcePoller,
    ) -> None:
        first_service = bool(active_poller) and poller is active_poller[0] and not probe.finished
        if first_service:
            probe.finished = True
            probe.record('service-start')
        service(owner, server, surface, poller)
        if first_service:
            probe.record('service-end')

    def observed_wait(waiter: _worker._ComponentWaiter, seconds: float) -> bool:
        result = wait(waiter, seconds)
        if (
            probe.finished
            and waiter is active_poller[0]._waiter
            and not (root / 'wait-finished').exists()
        ):
            sample = active_poller[0]._watchdog.sample()
            assert sample.phase == 'wait'
            probe.record('restored-wait')
            (root / 'wait-finished').touch()
        return result

    monkeypatch.setattr(_source.SourcePoller, '_poll_once', failing_poll)
    monkeypatch.setattr(_worker._SourceComponent, '_service_internal', observed_service)
    monkeypatch.setattr(_worker._ComponentWaiter, 'wait', observed_wait)


def _install_socket_probe(monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe) -> None:
    paths = _worker.local_socket_path
    remove = _ipc.remove_stale_socket

    def source_path(
        state: Path, kind: str, identity: tuple[str, ...], *, timeout_secs: float = 1.0
    ) -> Path:
        probe.record('source-path', timeout=timeout_secs)
        return paths(state, kind, identity)

    def server_path(path: Path, *, timeout_secs: float = 1.0) -> None:
        probe.record('server-path', timeout=timeout_secs)
        remove(path)

    monkeypatch.setattr(_worker, 'local_socket_path', source_path)
    monkeypatch.setattr(_ipc, 'remove_stale_socket', server_path)


def _install_wait_probe(monkeypatch: pytest.MonkeyPatch, case: str | None, root: Path) -> None:
    if case is None:
        return
    probe = _BusySourceProbe(case, root)
    if case == 'socket-probe':
        _install_socket_probe(monkeypatch, probe)
    elif case == 'permit':
        _install_permit_wait_probe(monkeypatch, root)
    elif case == 'retry-service':
        _install_retry_service_probe(monkeypatch, probe)
    elif case == 'lane-result':
        _install_lane_call_wait_probe(monkeypatch, probe)
    else:
        _install_connection_wait_probe(monkeypatch, probe)


def _observe_operator_source_wait(
    monkeypatch: pytest.MonkeyPatch,
    probe: _BusySourceProbe,
    call: _worker._OwnerCall[_worker._SourceJoinSurface],
    stop: threading.Event,
    queue_deadline_secs: float,
) -> None:
    wait = call.complete.wait
    advanced = False

    def controlled_wait(_timeout: float | None = None) -> bool:
        nonlocal advanced
        if not advanced:
            advanced = True
            if probe.case == 'operator-started':
                while not (probe.root / 'operator-started').exists():
                    assert not stop.is_set()
                    time.sleep(0.001)
            probe.clock.advance(queue_deadline_secs + 1)
            probe.record('operator-deadline-crossed')
            (probe.root / 'operator-release').touch()
            return False
        assert probe.case == 'operator-started', 'queued call ignored its admission deadline'
        return wait()

    monkeypatch.setattr(call.complete, 'wait', controlled_wait)


def _observe_unbounded_source_wait(
    monkeypatch: pytest.MonkeyPatch,
    probe: _BusySourceProbe,
    component: _worker._SourceComponent,
    call: _worker._OwnerCall[_worker._SourceJoinSurface],
) -> None:
    if probe.case not in {'blocked-resume', 'cooperative-stop'} or not getattr(
        probe.calls, 'join_request', False
    ):
        return
    probe.clock.start()
    wait = call.complete.wait
    advanced = False

    def controlled_wait(timeout: float | None = None) -> bool:
        nonlocal advanced
        probe.record('source-result-wait', timeout=timeout)
        if not advanced:
            advanced = True
            probe.clock.advance(component._coordination_deadline_secs + 1)
            return False
        (probe.root / 'source-wait-past-deadline').touch()
        return wait(timeout)

    monkeypatch.setattr(call.complete, 'wait', controlled_wait)


def _observe_source_join_submission(
    monkeypatch: pytest.MonkeyPatch,
    probe: _BusySourceProbe,
    component: _worker._SourceComponent,
    call: _worker._OwnerCall[_worker._SourceJoinSurface],
) -> None:
    if (
        not getattr(probe.calls, 'join_request', False)
        or not (probe.root / 'busy-entered').exists()
    ):
        return
    probe.record('join-submitted', page=probe.page, token=probe.calls.join_token)
    (probe.root / 'source-join-submitted').touch()
    _observe_unbounded_source_wait(monkeypatch, probe, component, call)


def _observed_source_join(
    probe: _BusySourceProbe, operation: Callable[[object], object], token: str
) -> Callable[[object], object]:
    def invoke(surface: object) -> object:
        probe.record('join-start', page=probe.page, token=token)
        result = operation(surface)
        probe.record('join-served', page=probe.page, token=token)
        return result

    return invoke


def _install_busy_source_calls(monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe) -> None:
    source_call = _worker._SourceComponent.call
    execute_operator = _worker._SourceComponent.execute_operator_command
    mailbox_submit = _worker._OwnerMailbox[object].submit
    active = probe.calls
    monkeypatch.setattr(_worker, 'time', probe.clock)

    def operator_command(
        component: _worker._SourceComponent,
        command: _ipc.OperatorCommand,
        *,
        wait_phase: Callable[[], AbstractContextManager[None]] = nullcontext,
    ) -> Mapping[str, object]:
        active.operator = command.command == 'preflight-source-adoption'
        try:
            return execute_operator(component, command, wait_phase=wait_phase)
        finally:
            active.operator = False

    def submit(
        mailbox: _worker._OwnerMailbox[object], operation: Callable[[object], object]
    ) -> _worker._OwnerCall[object]:
        # One permanent hook avoids concurrent callers replacing each other's hooks.
        # Select the exact call before wrapping its source-owner execution.
        observed = operation is getattr(active, 'invoke', None)
        if observed and getattr(active, 'join_request', False):
            operation = _observed_source_join(probe, operation, active.join_token)
        call = mailbox_submit(mailbox, operation)
        if observed:
            active.observe(call)
        return call

    def controlled_call(
        component: _worker._SourceComponent,
        invoke: Callable[[_worker._SourceJoinSurface], object],
        *,
        stop: threading.Event,
        wait_phase: Callable[[], AbstractContextManager[None]],
        queue_deadline_secs: float | None = None,
    ) -> object:
        operator = (
            probe.case.startswith('operator')
            and getattr(active, 'operator', False)
            and queue_deadline_secs is not None
            and not (probe.root / 'operator-call-finished').exists()
        )
        if not operator:

            def observe(call: _worker._OwnerCall[_worker._SourceJoinSurface]) -> None:
                _observe_source_join_submission(monkeypatch, probe, component, call)

        else:
            assert queue_deadline_secs is not None
            probe.clock.start()

            def observe(call: _worker._OwnerCall[_worker._SourceJoinSurface]) -> None:
                assert queue_deadline_secs is not None
                _observe_operator_source_wait(monkeypatch, probe, call, stop, queue_deadline_secs)

        active.invoke = invoke
        active.observe = observe
        try:
            return source_call(
                component,
                invoke,
                stop=stop,
                wait_phase=wait_phase,
                queue_deadline_secs=queue_deadline_secs,
            )
        finally:
            active.invoke = None
            active.observe = None
            probe.clock.clear()
            if operator:
                (probe.root / 'operator-call-finished').touch()

    monkeypatch.setattr(_worker._OwnerMailbox, 'submit', submit)
    monkeypatch.setattr(_worker._SourceComponent, 'call', controlled_call)
    monkeypatch.setattr(_worker._SourceComponent, 'execute_operator_command', operator_command)


def _install_busy_source_operator(
    monkeypatch: pytest.MonkeyPatch, probe: _BusySourceProbe
) -> None:
    _install_busy_source_calls(monkeypatch, probe)
    reserve = _worker._SourceJoinSurface.reserve_stream_adoption

    def controlled_reserve(
        surface: _worker._SourceJoinSurface, *, command_fingerprint: str, reason: str
    ) -> object:
        if probe.case == 'operator-started' and (probe.root / 'busy-arm').exists():
            probe.record('operator-start')
            (probe.root / 'operator-started').touch()
            while not (probe.root / 'operator-release').exists():
                time.sleep(0.001)
        with surface.state._writes.transaction(surface.state._database.connection):
            surface.state._database.connection.execute(
                "UPDATE source_meta SET condition = 'stream-identity-changed' WHERE singleton = 1"
            )
        result = reserve(surface, command_fingerprint=command_fingerprint, reason=reason)
        probe.record('operator-result')
        return result

    monkeypatch.setattr(_worker._SourceJoinSurface, 'reserve_stream_adoption', controlled_reserve)


def _start_cap_request(
    probe: _BusySourceProbe,
    active: threading.local,
    client: _worker._LocalJoinClient | _ipc.EventClient,
    *,
    armed: bool,
) -> None:
    active.client = (
        client
        if armed and probe.case == 'remote-cap' and not (probe.root / 'cap-finished').exists()
        else None
    )
    active.cap_entered = False
    if active.client is not None:
        assert isinstance(client, _ipc.EventClient)
        active.previous_cap = client._coordination_deadline_secs
        active.previous_slice = client._wait_slice_secs
        client._coordination_deadline_secs = _busy_coordination_budget(probe)
        probe.clock.start()
        probe.clock.advance(0.2)
        probe.record('request-attempt', deadline_secs=client._coordination_deadline_secs)


def _install_busy_source_request(
    monkeypatch: pytest.MonkeyPatch,
    probe: _BusySourceProbe,
    client_type: type[_worker._LocalJoinClient | _ipc.EventClient],
) -> None:
    request = client_type.request
    active = threading.local()
    if probe.case == 'remote-cap' and client_type is _ipc.EventClient:
        _install_remote_cap_poll(monkeypatch, probe, active)

    def traced_request(
        client: _worker._LocalJoinClient | _ipc.EventClient,
        value: _source.JoinRequest,
        channel: GroupChannelInterface,
        *,
        original: Callable[..., object] = request,
    ) -> object:
        armed = not probe.case.startswith('operator') and (probe.root / 'busy-arm').exists()
        if armed:
            (probe.root / 'join-ready').touch()
            gate = (
                'busy-query-entered'
                if probe.case in {'long-recheck', 'slow-page', 'remote-cap'}
                else 'busy-entered'
            )
            while not (probe.root / gate).exists():
                time.sleep(0.001)
            probe.record('call-queued', token=value.attempt.attempt_token)
            (probe.root / 'busy-call-queued').touch()
        _start_cap_request(probe, active, client, armed=armed)
        probe.calls.join_request = armed and isinstance(client, _worker._LocalJoinClient)
        probe.calls.join_token = value.attempt.attempt_token
        try:
            result = original(client, value, channel)
        except BaseException as error:
            if armed:
                probe.record('call-error', error=str(error))
            raise
        finally:
            probe.calls.join_request = False
            probe.calls.join_token = None
            _finish_cap_wait(probe, active)
        if armed:
            probe.record('call-result', token=value.attempt.attempt_token)
        return result

    monkeypatch.setattr(client_type, 'request', traced_request)


def _install_busy_source_probe(
    monkeypatch: pytest.MonkeyPatch,
    patches: ChildPatches,
    root: Path,
) -> None:
    if patches.busy_source_case is None:
        return
    probe = _BusySourceProbe(patches.busy_source_case, root)

    _install_busy_source_recheck(monkeypatch, probe)
    _install_busy_source_queries(monkeypatch, probe)
    _install_busy_source_writes(monkeypatch, probe)
    _install_busy_source_lane(monkeypatch, probe)
    for client_type in (_worker._LocalJoinClient, _ipc.EventClient):
        _install_busy_source_request(monkeypatch, probe, client_type)
    _install_busy_source_operator(monkeypatch, probe)
    _install_busy_source_remote(monkeypatch, probe)


def install_outside_reader(
    monkeypatch: pytest.MonkeyPatch,
    config_path: Path,
    log_path: Path,
    *,
    enabled: bool,
    **options: Unpack[_OutsideReaderOptions],
) -> None:
    observe_pause = options.get('observe_pause', False)
    if not enabled:
        return
    mutex = threading.Lock()

    def record(kind: str, role: str = 'site', **values: object) -> None:
        with mutex, log_path.with_suffix('.storage').open('a') as stream:
            stream.write(
                json.dumps({'kind': kind, 'role': role, 'at': time.monotonic(), **values}) + '\n'
            )

    path = source_database(load_config(config_path).daemon.state_dir, site='test', source='admin')

    def pause_observation(kind: str) -> None:
        # The existing frontier release is published after the read pin. Do
        # not apply the pinned-WAL invariant to startup before that boundary.
        if not (config_path.parent / 'events-ready').exists():
            return
        # Flush before an action, so SIGKILL cannot hide activity after a
        # refusal. The per-spawn stream preserves each owner's own order.
        with mutex, log_path.with_suffix('.pause').open('a') as stream:
            stream.write(
                json.dumps({'owner': threading.current_thread().name, 'kind': kind}) + '\n'
            )

    _install_outside_writes(monkeypatch, record, pause_observation if observe_pause else None)
    _outside_descriptor_sample(record, log_path, path)

    checkpoint = Checkpointer.run_once

    def checked(owner: Checkpointer) -> CheckpointAttempt:
        attempt = checkpoint(owner)
        _outside_descriptor_sample(record, log_path, path)
        record(
            'checkpoints',
            outcome=attempt.outcome,
            reuse=attempt.reuse,
            result=attempt.result,
            at=attempt.finished_at,
            wal_file=attempt.wal_bytes,
            crossed=sorted(owner.crossed),
        )
        return attempt

    invoke = _lane._invoke_sync

    def callback(spec: CallbackSpec, context: EventContext) -> None:
        if observe_pause:
            pause_observation('callback')
        invoke(spec, context)
        record('callbacks', context.group)

    handler = _OutsideWarnings(record)
    original_handle = logging.Logger.handle

    def logged(logger: logging.Logger, item: logging.LogRecord) -> None:
        handler.emit(item)
        original_handle(logger, item)

    monkeypatch.setattr(logging.Logger, 'handle', logged)
    monkeypatch.setattr(
        _checkpoint,
        'WAL_REUSE_WAIT_SECS',
        0.1 * load_config(config_path).daemon.checkpoint_interval_secs,
    )
    monkeypatch.setattr(Checkpointer, 'run_once', checked)
    monkeypatch.setattr(_lane, '_invoke_sync', callback)
    provider = OutsideReaderProvider(config_path.parent, frontier_cap=options.get('frontier_cap'))

    def find(
        handle: DeterministicHandle,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        return provider.find(handle, entity_type, filters, fields, **options)

    monkeypatch.setattr(DeterministicHandle, 'find', find)


def run_open_file_startup(config_path: Path) -> None:
    # Runs after exec in the limited child, including every supervisor probe.
    configure_logging()
    authority_opens: list[str] = []
    spawns: list[str] = []
    original_sources = Supervisor._open_reconciliation_sources
    original_groups = Supervisor._open_reconciliation_groups
    original_spawn = SupervisorProbe.spawn

    def sources(
        owner: Supervisor, stack: ExitStack, identities: Collection[tuple[str, str]]
    ) -> dict[tuple[str, str], SourceState]:
        authority_opens.append('sources')
        return original_sources(owner, stack, identities)

    def groups(
        owner: Supervisor,
        stack: ExitStack,
        identities: Collection[tuple[str, str]],
        configured: Mapping[tuple[str, str], str],
        source_states: Mapping[tuple[str, str], SourceState],
    ) -> tuple[dict[tuple[str, str], int], dict[tuple[str, str], GroupState]]:
        authority_opens.append('groups')
        return original_groups(owner, stack, identities, configured, source_states)

    def spawn(
        owner: SupervisorProbe,
        argv: list[str],
        *,
        pass_fds: tuple[int, ...] = (),
        env: Mapping[str, str] | None = None,
    ) -> ChildProcess:
        spawns.append('worker')
        return original_spawn(owner, argv, pass_fds=pass_fds, env=env)

    outcome: dict[str, object] = {'authority_opens': authority_opens, 'spawns': spawns}
    with pytest.MonkeyPatch.context() as monkeypatch:
        activate_stall_policy(monkeypatch)
        monkeypatch.setattr(Supervisor, '_open_reconciliation_sources', sources)
        monkeypatch.setattr(Supervisor, '_open_reconciliation_groups', groups)
        monkeypatch.setattr(SupervisorProbe, 'spawn', spawn)
        try:
            with running_supervisor(
                config_path,
                ChildPatches(last_event_id=4, probe_open_file_limit=True, gate_events=True),
            ) as probe:
                config = load_config(config_path)
                if len(config.sites) > 1:
                    outcome['started'] = True
                    return
                names = tuple(config.sites['test'].groups)
                probe.wait_for(
                    lambda: (
                        sum(len(health.groups) for health in probe.latest_health.values())
                        == len(names)
                        and all(
                            group.join_phase == 'attached'
                            for health in probe.latest_health.values()
                            for group in health.groups
                        )
                    ),
                )
                (config_path.parent / 'events-ready').touch()
                probe.wait_for(lambda: all(probe.records(name) for name in names))
                outcome['delivered'] = {name: probe.records(name) for name in names}
        except (ConfigError, RuntimeError, AssertionError, OSError) as error:
            outcome['error'] = f'{type(error).__name__}: {error}'
        finally:
            print(json.dumps(outcome), flush=True)


def wal_salt(path: Path) -> str:
    try:
        with Path(f'{path}-wal').open('rb') as stream:
            return stream.read(24)[16:24].hex()
    except FileNotFoundError:
        return ''


def _install_growth_transactions(
    monkeypatch: pytest.MonkeyPatch,
    patches: ChildPatches,
    record: Callable[[str, dict[str, object]], None],
) -> None:
    transaction = SourceWriteLock.transaction
    first_write_complete = threading.Event()

    @contextmanager
    def traced_transaction(
        lock: SourceWriteLock,
        connection: sqlite3.Connection,
        **options: Unpack[_TransactionOptions],
    ) -> Generator[sqlite3.Connection]:
        if patches.outside_reader and first_write_complete.is_set():
            with transaction(lock, connection, **options) as active:
                yield active
            return
        path = Path(connection.execute('PRAGMA database_list').fetchone()[2])
        name = threading.current_thread().name.removeprefix('sg-event-daemon:')
        started = time.monotonic()
        size = _sqlite.wal_file_bytes(path)
        try:
            with transaction(lock, connection, **options) as active:
                record(
                    '.wal-writes',
                    {
                        'thread': name,
                        'phase': 'begin',
                        'started': started,
                        'at': time.monotonic(),
                        'bytes': _sqlite.wal_file_bytes(path),
                    },
                )
                yield active
        except _write_lock.WriteLockTimeoutError as error:
            record(
                '.wal-writes',
                {
                    'thread': name,
                    'phase': error.level,
                    'at': time.monotonic(),
                    'started': started,
                    'bytes': size,
                },
            )
            raise
        except BaseException:
            if patches.outside_reader:
                record(
                    '.wal-writes',
                    {
                        'thread': name,
                        'phase': 'failed',
                        'at': time.monotonic(),
                        'started': started,
                    },
                )
            raise
        else:
            record(
                '.wal-writes',
                {
                    'thread': name,
                    'phase': 'commit',
                    'at': time.monotonic(),
                    'started': started,
                    'bytes': _sqlite.wal_file_bytes(path),
                    'salt': wal_salt(path),
                },
            )

            first_write_complete.set()

    monkeypatch.setattr(SourceWriteLock, 'transaction', traced_transaction)


def install_growth_probes(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches, log_path: Path
) -> None:
    root = log_path.parent
    limit = patches.writer_wal_limit_bytes
    if limit is None:
        return
    # Apply the artificial WAL bound at each enforcement boundary.
    monkeypatch.setattr(_write_lock, 'WAL_IN_USE_LIMIT_BYTES', limit)
    monkeypatch.setattr(_sqlite, 'WAL_IN_USE_LIMIT_BYTES', limit)
    monkeypatch.setattr(_checkpoint, 'WAL_IN_USE_LIMIT_BYTES', limit)
    # Keep the size-trigger margin proportional to the artificial growth bound.
    monkeypatch.setattr(_checkpoint_host, 'WAL_TRIGGER_BYTES', limit // 16)
    find = DeterministicHandle.find
    invoke = _lane._invoke_sync

    def gated_callback(callback: CallbackSpec, context: EventContext) -> None:
        invoke(callback, context)
        if (root / 'growth-callback-hold').exists() and context.group in {'local', 'remote'}:
            (root / f'growth-callback-{context.group}-held').touch()
            while not (root / 'growth-callback-release').exists():
                time.sleep(0.005)

    def record(suffix: str, row: dict[str, object]) -> None:
        with log_path.with_suffix(suffix).open('a') as stream:
            stream.write(json.dumps(row) + '\n')

    _install_growth_transactions(monkeypatch, patches, record)

    def traced_find(
        handle: DeterministicHandle,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        record(
            '.queries',
            {
                'thread': threading.current_thread().name.removeprefix('sg-event-daemon:'),
                'at': time.monotonic(),
            },
        )
        return find(handle, entity_type, filters, fields, **options)

    monkeypatch.setattr(DeterministicHandle, 'find', traced_find)
    monkeypatch.setattr(_lane, '_invoke_sync', gated_callback)
    _install_growth_checkpoints(monkeypatch, root, record)
    install_outside_reader(
        monkeypatch,
        root / 'daemon.toml',
        log_path,
        enabled=patches.outside_reader,
        frontier_cap=patches.outside_frontier_cap,
        observe_pause=patches.outside_pause_observation,
    )


def run_crashed_disposition_seed(path: Path) -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        activate_stall_policy(monkeypatch)
        state = open_test_group(path)
        connection = state._database.connection
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        connection.execute('BEGIN IMMEDIATE')
        connection.execute(
            'INSERT INTO callback_marks VALUES (?, ?, 0, ?, ?, ?, 1)',
            ('checkpoint-probe', 1, 'probe', 'x' * 500_000, 'success'),
        )
        connection.commit()
        print(wal_salt(path), flush=True)
        # Deliberately bypass connection.close(), preserving the crash WAL.
        os._exit(0)


def run_direct_disposition_probe(path: Path, limit: int) -> None:
    with pytest.MonkeyPatch.context() as monkeypatch:
        activate_stall_policy(monkeypatch)
        monkeypatch.setattr(_sqlite, 'WAL_IN_USE_LIMIT_BYTES', limit)
        monkeypatch.setattr(_write_lock, 'WAL_IN_USE_LIMIT_BYTES', limit)
        writes = replace(write_context_for(path), policy='operator')
        with GroupState.open_for_disposition(
            path,
            group_lock(path, 'editorial'),
            writes=writes,
            site='studio',
            site_server_url='https://studio.example.invalid',
            source='events',
            group='editorial',
            group_generation=7,
            ordering='eager',
            stream_fingerprint='stream-v1',
            source_generation='source-spawn-1',
            verified_incarnation='incarnation-1',
            start_mode='resume',
        ) as state:
            before = _sqlite.wal_file_bytes(path)
            snapshot = state.park(
                expected_revision=state.snapshot().revision,
                reason='probe',
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
            )
            print(
                json.dumps(
                    {
                        'before': before,
                        'after': _sqlite.wal_file_bytes(path),
                        'salt': wal_salt(path),
                        'parked': snapshot.parked,
                    }
                ),
                flush=True,
            )


def run_worker(
    config_path: Path,
    patches: ChildPatches,
    *,
    stall_detectors: tuple[str, ...] | None = None,
) -> None:
    spawn_instance = sys.argv[sys.argv.index('--spawn-instance') + 1]
    _record_open_file_limit(config_path, spawn_instance, enabled=patches.probe_open_file_limit)
    log_path = config_path.parent / f'{spawn_instance}.log'
    with log_path.open('ab', buffering=0) as log:
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
    with pytest.MonkeyPatch.context() as monkeypatch:
        activate_stall_policy(monkeypatch, enabled=stall_detectors)
        install_runtime_diagnostics(monkeypatch, log_path.with_suffix('.runtime-diagnostics'), 2)
        if patches.signal_dir is not None:
            install_timing_clock(monkeypatch, patches.signal_dir)
        _install_provider(
            monkeypatch,
            config_path.stat().st_mtime,
            patches.last_event_id,
            config_path.parent / 'events-ready' if patches.gate_events else None,
            async_provider=patches.async_provider,
        )
        _install_initial_group_query_failure(monkeypatch, patches, config_path.parent)
        _install_control_probe(monkeypatch, config_path.parent, enabled=patches.control_probe)
        _install_lifecycle_probe(monkeypatch, config_path.parent, patches=patches)
        _install_wait_probe(monkeypatch, patches.wait_case, config_path.parent)
        _install_busy_source_probe(monkeypatch, patches, config_path.parent)
        _install_source_halt_probe(monkeypatch, patches, config_path.parent)
        _install_lane_detach_probe(monkeypatch, patches, config_path.parent)
        _install_callback_hold(monkeypatch, config_path, enabled=patches.hold_callback)
        _install_combined_delivery_gate(
            monkeypatch, config_path.parent, patches.combined_delivery_gate, patches.last_event_id
        )
        if patches.checkpoint_wal_limit_bytes is not None:
            monkeypatch.setattr(
                _checkpoint, 'WAL_IN_USE_LIMIT_BYTES', patches.checkpoint_wal_limit_bytes
            )
        if patches.hold_provider:
            find = DeterministicHandle.find

            def held_find(
                handle: DeterministicHandle,
                entity_type: str,
                filters: object,
                fields: list[str] | None = None,
                **options: Unpack[_FindOptions],
            ) -> list[dict[str, object]]:
                root = config_path.parent
                if (
                    ':source:' in threading.current_thread().name
                    and (root / 'provider-arm').exists()
                ):
                    (root / 'provider-entered').touch()
                    while not (root / 'provider-release').exists():
                        time.sleep(0.005)
                return find(handle, entity_type, filters, fields, **options)

            monkeypatch.setattr(DeterministicHandle, 'find', held_find)
        if patches.probe_coordination:
            _install_coordination_probes(monkeypatch, config_path.parent)
        _install_startup_probe(monkeypatch, log_path, enabled=patches.probe_startup)
        _install_wal_restart_probe(monkeypatch, log_path, enabled=patches.probe_wal_restart)
        install_checkpoints(monkeypatch, log_path, advance_clock=patches.checkpoint_clock)
        if patches.track_revisions:
            _install_revision_probe(monkeypatch, log_path)
        config = load_config(config_path)
        site = sys.argv[sys.argv.index('--site') + 1]
        worker_id = sys.argv[sys.argv.index('--worker') + 1]
        spec = next(
            spec
            for spec in _worker.build_worker_specs(config.sites[site])
            if spec.worker_id == worker_id
        )
        idle_interval = (
            config.sites[site].sources['admin'].fetch_interval_secs
            + 5 * config.daemon.health_interval_secs
        )
        groups = tuple(config.sites[site].groups) if patches.remote_rendezvous else spec.groups
        install_faults(monkeypatch, patches, groups, idle_interval)
        install_growth_probes(monkeypatch, patches, log_path)
        if patches.timing:
            _install_timing(monkeypatch, patches, idle_interval)
        raise SystemExit(_cli.main(sys.argv[1:]))


def write_daemon_config(tmp_path: Path, topology: Topology) -> Path:
    plugin = _ROOT / 'tests' / 'fixtures' / 'recording_plugin.py'
    lines = [
        '[daemon]',
        f'state_dir = "{tmp_path / "state"}"',
        'sg_api_provider = "shotgun-api3"',
        f'health_interval_secs = {topology.health_interval_secs}',
        f'supervisor_poll_interval_secs = {topology.supervisor_poll_interval_secs}',
        f'checkpoint_interval_secs = {topology.checkpoint_interval_secs}',
        f'worker_startup_deadline_secs = {stall_deadline("startup", 30.0)}',
        f'shutdown_deadline_secs = {stall_deadline("shutdown", 10.0)}',
        f'async_cancel_grace_secs = {stall_deadline("shutdown", 5.0)}',
        '[sites.test]',
        'server = "https://example.invalid"',
        'default_source = "admin"',
        '[sites.test.credentials.admin]',
        'script_name = "test"',
        'api_key = { env = "SG_EVENT_HARNESS_KEY" }',
        '[sites.test.sources.admin]',
        'credentials = "admin"',
        f'fetch_interval_secs = {topology.fetch_interval_secs}',
    ]
    if topology.packing in {'named', 'mixed'}:
        lines.append('[sites.test.workers.pool]')
    if topology.packing == 'mixed':
        assert len(topology.groups) == 5
    for index, group in enumerate(topology.groups):
        packing = topology.packing
        worker = {
            'named': 'pool',
            'dedicated': 'dedicated',
            'default': 'default',
            'mixed': 'default',
        }[packing]
        if packing == 'mixed':
            worker = 'default' if index == 0 else 'dedicated' if index == 4 else 'pool'
        lines.extend(
            [
                f'[sites.test.groups.{group}]',
                f'worker = "{worker}"',
                f'callback_deadline_secs = {stall_deadline("phase", 120.0)}',
                f'[[sites.test.groups.{group}.plugins]]',
                'id = "recording"',
                f'path = "{plugin}"',
                f'[sites.test.groups.{group}.plugins.config]',
                f'record_file = "{tmp_path / f"{group}.records"}"',
            ]
        )
    config_path = tmp_path / 'daemon.toml'
    config_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return config_path
