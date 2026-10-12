from __future__ import annotations

import json
import os
import sys
import threading
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Unpack, cast

import pytest

from sg_event_daemon import _cli, _lane, _supervisor, _worker, _write_lock
from sg_event_daemon._checkpoint import CheckpointAttempt, Checkpointer, CheckpointResult
from sg_event_daemon._config import DEFAULT_CHECKPOINT_INTERVAL_SECS, load_config
from sg_event_daemon._events import EVENT_FIELDS
from sg_event_daemon._platform import ChildProcess, spawn_child
from sg_event_daemon._providers import Provider, ProviderCapabilities, _FindOptions
from sg_event_daemon._supervisor import Supervisor, _ParkEvidence, _WorkerBreaker, _WorkerInstance
from sg_event_daemon._watchdog import ComponentWatchdog, WatchdogSample
from sg_event_daemon._write_lock import SourceWriteLock, WritePhase, _Attempt, _TransactionOptions
from tests._runtime_diagnostics import install_runtime_diagnostics

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Generator, Mapping

    from sg_event_daemon._group_types import GroupSnapshot, LiveJoinIdentity
    from sg_event_daemon._ipc import HealthUpdate
    from sg_event_daemon._source_store import SourcePollState

_MISSING_IDS = frozenset({7, 19, 31})
_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ChildPatches:
    hold_deadline_secs: float = 0.5
    stall_group: str | None = None
    stall_poller: bool = False
    signal_dir: Path | None = None
    mask_write_hold: str | None = None
    last_event_id: int = 1000
    remote_rendezvous: bool = False
    track_revisions: bool = False
    timing: bool = False
    gate_events: bool = False


@dataclass(frozen=True)
class Topology:
    packing: Literal['default', 'named', 'dedicated', 'mixed']
    groups: tuple[str, ...]
    health_interval_secs: float = 0.05
    supervisor_poll_interval_secs: float = 0.05
    checkpoint_interval_secs: float = DEFAULT_CHECKPOINT_INTERVAL_SECS
    fetch_interval_secs: float = 0.05


@dataclass(frozen=True)
class WorkerSpawn:
    worker_id: str
    spawn_instance: str
    process: ChildProcess
    log_path: Path


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
        self._health_before_containment: dict[str, HealthUpdate | None] = {}

    @property
    def breakers(self) -> Mapping[str, _WorkerBreaker]:
        return self.supervisor._breakers

    @property
    def latest_health(self) -> Mapping[str, HealthUpdate]:
        return {
            worker_id: spawn._health
            for worker_id, spawn in self.supervisor._spawns.items()
            if spawn._health is not None
        }

    def _record_termination(self, worker: _WorkerInstance, fallback_reason: str) -> None:
        spawn = self.supervisor._spawns.get(worker._worker_id)
        reason = fallback_reason if spawn is None else spawn._termination_reason or fallback_reason
        if any(item.spawn_instance == worker._spawn_instance for item in self.terminations):
            return
        self.terminations.append(
            WorkerTermination(
                worker._worker_id,
                worker._spawn_instance,
                reason,
                time.monotonic(),
                self.latest_health.get(worker._worker_id),
                self._health_before_containment.get(worker._spawn_instance),
            )
        )
        with (self._config_path.parent / 'terminations.log').open('a') as stream:
            stream.write(f'{self.terminations[-1]!r}\n')

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

    def _spawn(
        self,
        argv: list[str],
        *,
        pass_fds: tuple[int, ...] = (),
        env: Mapping[str, str] | None = None,
    ) -> ChildProcess:
        worker_id = argv[argv.index('--worker') + 1]
        spawn_instance = argv[argv.index('--spawn-instance') + 1]
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
            )
        )
        return process

    def records(self, group: str) -> tuple[int, ...]:
        path = self._config_path.parent / f'{group}.records'
        if not path.exists():
            return ()
        return tuple(
            int(line.split()[1]) for line in path.read_text(encoding='utf-8').splitlines()
        )

    def wait_for(self, predicate: Callable[[], bool], timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            self.supervisor._tick()
            if time.monotonic() >= deadline:
                raise AssertionError(f'timed out; {self.failure_details()}')
            time.sleep(self.supervisor._config.daemon.supervisor_poll_interval_secs)


class _DeterministicHandle:
    def __init__(self, seed: float, last_event_id: int, event_gate: Path | None) -> None:
        self._event_gate = event_gate
        self.config = _ProviderConfig()
        self._seed = seed
        self._last_event_id = last_event_id

    def _event(self, event_id: int) -> dict[str, object]:
        event = cast('dict[str, object]', dict.fromkeys(EVENT_FIELDS))
        event.update(
            id=event_id,
            type='EventLogEntry',
            event_type='Shotgun_Asset_Change',
            created_at=datetime.fromtimestamp(self._seed + event_id * 0.05, UTC),
            meta={},
        )
        return event

    def close(self) -> None:
        pass

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
        seed = self._seed
        if self._event_gate is not None:
            seed = self._event_gate.stat().st_mtime if self._event_gate.exists() else time.time()
        visible = max(1, min(self._last_event_id, int((time.time() - seed) / 0.05) + 1))
        event_ids = [
            event_id for event_id in range(1, visible + 1) if event_id not in _MISSING_IDS
        ]
        for condition in cast('list[object]', filters):
            assert isinstance(condition, list)
            parts = cast('list[object]', condition)
            assert len(parts) == 3
            field, operator, value = parts
            assert field == 'id'
            assert isinstance(value, int)
            if operator == 'is':
                event_ids = [event_id for event_id in event_ids if event_id == value]
            elif operator == 'greater_than':
                event_ids = [event_id for event_id in event_ids if event_id > value]
            elif operator == 'less_than':
                event_ids = [event_id for event_id in event_ids if event_id < value]
            else:
                raise AssertionError(f'unsupported provider filter: {condition!r}')
        if options.get('order') == [{'field_name': 'id', 'direction': 'desc'}]:
            event_ids.reverse()
        limit = options.get('limit', 0)
        if limit:
            event_ids = event_ids[:limit]
        return [self._event(event_id) for event_id in event_ids]

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        pass


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


def _install_checkpoints(monkeypatch: pytest.MonkeyPatch, log_path: Path) -> None:
    original = Checkpointer.run_once
    passive = Checkpointer._passive_checkpoint

    def checkpoint_try(checkpointer: Checkpointer) -> CheckpointResult:
        started = time.monotonic()
        result: CheckpointResult | None = None
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
        attempt = original(checkpointer)
        with log_path.with_suffix('.checkpoints').open('a', encoding='utf-8') as stream:
            stream.write(f'{attempt.outcome} {attempt.retried} {attempt.result}\n')
        return attempt

    monkeypatch.setattr(Checkpointer, 'run_once', checkpoint)
    monkeypatch.setattr(Checkpointer, '_passive_checkpoint', checkpoint_try)


def _install_provider(
    monkeypatch: pytest.MonkeyPatch, seed: float, last_event_id: int, event_gate: Path | None
) -> None:
    def factory(
        site: str, *, script_name: str, api_key: str, connect: bool
    ) -> _DeterministicHandle:
        assert site == 'https://example.invalid'
        assert script_name == 'test'
        assert api_key == 'test-key'
        assert not connect
        return _DeterministicHandle(seed, last_event_id, event_gate)

    provider = Provider(
        'shotgun-api3', ProviderCapabilities(sync=True, async_=False), factory, None
    )

    def load(*_args: object) -> Provider:
        return provider

    monkeypatch.setattr(_worker, 'load_provider', load)


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

    def observed_scan(state: _worker.GroupState, obligation_id: str) -> GroupSnapshot:
        result = complete_scan(state, obligation_id)
        revision = state._database.connection.execute(
            'SELECT index_revision FROM source_meta'
        ).fetchone()[0]
        with log_path.with_suffix('.scans').open('a') as output:
            output.write(f'{revision}\n')
        return result

    monkeypatch.setattr(_worker.SourceState, 'begin_cycle', observed_cycle)
    monkeypatch.setattr(_worker.GroupState, 'complete_obligation_scan', observed_scan)


def _record_write(signals: Path, name: str, phase: str, deadline: float = 0) -> None:
    with (signals / f'{name}.writes').open('a', encoding='utf-8') as stream:
        stream.write(f'{phase} {time.monotonic()} {deadline}\n')


def _record_commit(signals: Path | None, name: str, *, record: bool) -> None:
    if record and signals is not None:
        _record_write(signals, name, 'commit')
        marker = signals / f'{name}.committed'
        if not marker.exists():
            pending = marker.with_suffix('.pending')
            pending.write_text(str(time.monotonic()), encoding='utf-8')
            pending.replace(marker)


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
    (signals / f'{who}.stalled').write_text(str(time.monotonic()), encoding='utf-8')
    while not (signals / 'release').exists():
        time.sleep(0.01)


def _install_faults(
    monkeypatch: pytest.MonkeyPatch,
    patches: ChildPatches,
    groups: tuple[str, ...],
    idle_interval_secs: float,
) -> None:
    original_transaction = SourceWriteLock.transaction
    original_sample = ComponentWatchdog.sample
    original_acquire_file = SourceWriteLock._acquire_file

    def acquire_file(lock: SourceWriteLock, attempt: _Attempt) -> None:
        signals = patches.signal_dir
        if (
            patches.remote_rendezvous
            and signals is not None
            and (signals / f'{patches.stall_group}.stalled').exists()
        ):
            name = threading.current_thread().name.removeprefix('sg-event-daemon:')
            (signals / f'{name}.local').touch()
        original_acquire_file(lock, attempt)

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
        original_watch = options.get('watch', _write_lock._no_watch)

        @contextmanager
        def watch(phase: WritePhase, deadline: float) -> Generator[None]:
            with original_watch(phase, deadline):
                if record and signals is not None:
                    _record_write(signals, name, phase, deadline)
                yield

        options['watch'] = watch
        with original_transaction(lock, connection, **options) as active:
            _stall_in_transaction(active, patches)
            yield active
        _record_commit(signals, name, record=record)

    def sample(watchdog: ComponentWatchdog) -> WatchdogSample:
        result = original_sample(watchdog)
        if (
            result.kind == 'lane'
            and result.identity == patches.mask_write_hold
            and result.phase == 'write-hold'
        ):
            now = time.monotonic()
            return replace(
                result, phase='idle', heartbeat_at=now, deadline_at=now + idle_interval_secs
            )
        return result

    monkeypatch.setattr(_write_lock, 'WRITE_HOLD_DEADLINE_SECS', patches.hold_deadline_secs)
    monkeypatch.setattr(SourceWriteLock, 'transaction', transaction)
    monkeypatch.setattr(SourceWriteLock, '_acquire_file', acquire_file)
    monkeypatch.setattr(ComponentWatchdog, 'sample', sample)


def _stall_timing_holder(episode: Path, name: str, runtime: list[_worker.WorkerRuntime]) -> None:
    (episode / f'{name}.stalled').write_text(str(time.monotonic()))
    if (episode / 'sample-hold').exists():
        assert runtime
        runtime[0]._monitor_wake.set()
    deadline = time.monotonic() + 45
    while not (episode / 'release').exists() and not (episode / 'done').exists():
        assert time.monotonic() < deadline, 'test holder was not released'
        time.sleep(0.005)


def _timing_episode(patches: ChildPatches) -> Path | None:
    signals = patches.signal_dir
    if not patches.timing or signals is None or not (signals / 'current').exists():
        return None
    episode = signals / (signals / 'current').read_text().strip()
    return None if (episode / 'done').exists() else episode


def _install_timing_failures(monkeypatch: pytest.MonkeyPatch, patches: ChildPatches) -> None:
    component_failed = _worker.WorkerRuntime._component_failed

    def failed(worker: _worker.WorkerRuntime, failure: str) -> None:
        episode = _timing_episode(patches)
        if episode is not None:
            name = threading.current_thread().name.removeprefix('sg-event-daemon:')
            _record_write(episode, name, 'component-failed')
            (episode / f'{name}.fatal').write_text(failure)
        component_failed(worker, failure)

    monkeypatch.setattr(_worker.WorkerRuntime, '_component_failed', failed)


def _install_timing_health(
    monkeypatch: pytest.MonkeyPatch,
    patches: ChildPatches,
    runtime: list[_worker.WorkerRuntime],
) -> None:
    publish = _worker.WorkerRuntime._publish_monitor_health

    def health(worker: _worker.WorkerRuntime, now: float) -> float | None:
        if not runtime:
            runtime.append(worker)
        episode = _timing_episode(patches)
        holder = episode / 'lane:holder.stalled' if episode is not None else None
        if (
            episode is not None
            and 'holder' in worker._spec.groups
            and (episode / 'block-monitor').exists()
        ):
            (episode / 'monitor-blocked').write_text(str(time.monotonic()))
            while not (episode / 'done').exists():
                time.sleep(0.01)
        pending = publish(worker, now)
        if episode is not None and 'holder' in worker._spec.groups:
            _record_write(episode, 'monitor', 'health', now)
            if (
                (episode / 'align-health').exists()
                and (episode / 'lane:holder.entered').exists()
                and holder is not None
                and not holder.exists()
            ):
                (episode / 'health-sent').write_text(str(time.monotonic()))
            if (episode / 'sample-hold').exists() and holder is not None and holder.exists():
                (episode / 'hold-health-sent').write_text(str(time.monotonic()))
        return pending

    monkeypatch.setattr(_worker.WorkerRuntime, '_publish_monitor_health', health)


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
            (episode / 'lane:named.joining').write_text(str(time.monotonic()))
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


def _install_timing_samples(
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
    if (episode / f'{name}.entered').exists() or not _between_events(connection, name):
        return None
    if name == 'source:admin' and not all(
        (episode / f'lane:{group}.ready').exists() for group in ('named', 'holder')
    ):
        return None
    (episode / f'{name}.ready').touch()
    while not (episode / f'{name}.go').exists() and not (episode / 'done').exists():
        time.sleep(0.001)
    if (episode / 'done').exists():
        return None
    (episode / f'{name}.entered').touch()
    return episode


def _install_timing_transactions(
    monkeypatch: pytest.MonkeyPatch,
    patches: ChildPatches,
    runtime: list[_worker.WorkerRuntime],
) -> None:
    transaction = SourceWriteLock.transaction

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
            assert runtime
            runtime[0]._monitor_wake.set()
            while not (episode / 'health-sent').exists():
                time.sleep(0.001)
        original_watch = options.get('watch', _write_lock._no_watch)

        @contextmanager
        def observed_watch(phase: WritePhase, deadline: float) -> Generator[None]:
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
            (episode / f'{name}.error').write_text(str(error))
            raise
        else:
            _record_write(episode, name, 'commit')

    monkeypatch.setattr(SourceWriteLock, 'transaction', observed_transaction)


def _install_timing(
    monkeypatch: pytest.MonkeyPatch, patches: ChildPatches, idle_interval_secs: float
) -> None:
    runtime: list[_worker.WorkerRuntime] = []
    _install_timing_health(monkeypatch, patches, runtime)
    _install_timing_failures(monkeypatch, patches)
    _install_timing_join(monkeypatch, patches)
    _install_timing_locks(monkeypatch, patches)
    _install_timing_samples(monkeypatch, patches, idle_interval_secs)
    _install_timing_transactions(monkeypatch, patches, runtime)


def child_code(config_path: Path, patches: ChildPatches) -> str:
    signals = 'None' if patches.signal_dir is None else f'Path({str(patches.signal_dir)!r})'
    return (
        f'import sys; sys.path.insert(0, {str(_ROOT)!r})\n'
        'from pathlib import Path\n'
        'from tests._worker_processes import ChildPatches, run_worker\n'
        f'run_worker(Path({str(config_path)!r}), ChildPatches('
        f'hold_deadline_secs={patches.hold_deadline_secs!r}, '
        f'stall_group={patches.stall_group!r}, stall_poller={patches.stall_poller!r}, '
        f'signal_dir={signals}, '
        f'mask_write_hold={patches.mask_write_hold!r}, '
        f'last_event_id={patches.last_event_id!r}, '
        f'remote_rendezvous={patches.remote_rendezvous!r}, '
        f'track_revisions={patches.track_revisions!r}, timing={patches.timing!r}, '
        f'gate_events={patches.gate_events!r}))\n'
    )


def run_worker(config_path: Path, patches: ChildPatches) -> None:
    spawn_instance = sys.argv[sys.argv.index('--spawn-instance') + 1]
    log_path = config_path.parent / f'{spawn_instance}.log'
    with log_path.open('ab', buffering=0) as log:
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
    with pytest.MonkeyPatch.context() as monkeypatch:
        install_runtime_diagnostics(monkeypatch, log_path.with_suffix('.runtime-diagnostics'), 2)
        _install_provider(
            monkeypatch,
            config_path.stat().st_mtime,
            patches.last_event_id,
            config_path.parent / 'events-ready' if patches.gate_events else None,
        )
        _install_checkpoints(monkeypatch, log_path)
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
        _install_faults(monkeypatch, patches, groups, idle_interval)
        if patches.timing:
            _install_timing(monkeypatch, patches, idle_interval)
        raise SystemExit(_cli.main(sys.argv[1:]))


@contextmanager
def running_supervisor(config_path: Path, patches: ChildPatches) -> Generator[SupervisorProbe]:
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
        monkeypatch.setattr(_write_lock, 'WRITE_HOLD_DEADLINE_SECS', patches.hold_deadline_secs)
        supervisor = Supervisor(config=load_config(config_path), config_path=config_path)
        probe = SupervisorProbe(supervisor, config_path, patches)
        original_kill = _WorkerInstance._kill
        original_wait = _WorkerInstance._wait
        original_mark = Supervisor._mark_current_worker_unready

        def kill(worker: _WorkerInstance) -> None:
            probe._record_termination(worker, 'shutdown')
            original_kill(worker)

        def wait(worker: _WorkerInstance, *, timeout: float = 0.0) -> int | None:
            result = original_wait(worker, timeout=timeout)
            if result is not None:
                probe._record_termination(worker, f'worker-exit-{result}')
            return result

        def mark_unready(
            owner: Supervisor,
            worker: _supervisor._WorkerHandle,
            reason: str,
            watchdog_failure: bool,
        ) -> bool:
            if (
                (spawn := owner._spawns.get(worker._worker_id)) is not None
                and spawn._worker is worker
                and spawn._termination_reason is None
            ):
                probe._health_before_containment.setdefault(
                    worker._spawn_instance, probe.latest_health.get(worker._worker_id)
                )
            return original_mark(owner, worker, reason, watchdog_failure)

        monkeypatch.setattr(Supervisor, '_mark_current_worker_unready', mark_unready)
        monkeypatch.setattr(_supervisor, 'spawn_child', probe._spawn)
        monkeypatch.setattr(_WorkerInstance, '_kill', kill)
        monkeypatch.setattr(_WorkerInstance, '_wait', wait)
        try:
            with supervisor:
                supervisor.reconcile_durable_state()
                supervisor.start()
                yield probe
        except BaseException as error:
            error.add_note(probe.failure_details())
            raise


def write_daemon_config(tmp_path: Path, topology: Topology) -> Path:
    plugin = _ROOT / 'tests' / 'fixtures' / 'recording_plugin.py'
    lines = [
        '[daemon]',
        f'state_dir = "{tmp_path / "state"}"',
        'sg_api_provider = "shotgun-api3"',
        f'health_interval_secs = {topology.health_interval_secs}',
        f'supervisor_poll_interval_secs = {topology.supervisor_poll_interval_secs}',
        f'checkpoint_interval_secs = {topology.checkpoint_interval_secs}',
        'worker_startup_deadline_secs = 10.0',
        'shutdown_deadline_secs = 2.0',
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
