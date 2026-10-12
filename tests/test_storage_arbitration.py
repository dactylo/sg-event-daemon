from __future__ import annotations

import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon import _group_state, _source_state, _sqlite, _write_lock
from sg_event_daemon._group_recovery import DispositionRequest
from sg_event_daemon._group_types import AuditIdentity
from sg_event_daemon._ipc import OperatorCommand
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._state_paths import source_database_path
from sg_event_daemon._supervisor import Supervisor
from sg_event_daemon._watchdog import ComponentWatchdog
from sg_event_daemon._worker import _LaneComponent, build_worker_specs
from tests._storage_support import (
    open_test_group,
    write_context_for,
)
from tests.source._support import open_group
from tests.test_supervisor import _prepare_absent_orphan, _source_only_config
from tests.test_worker import (
    _group as _group_config,
)
from tests.test_worker import (
    _InterruptibleComponent,
    _local_assignment,
    _prepare_runtime_state,
    _receive_typed_health,
    _run_cooperative_stop_component,
    _running_concrete_runtime,
    _runtime_config,
    _runtime_provider,
    _runtime_site,
    _start_control_drain,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from sg_event_daemon._group_state import GroupState


def _group(path: Path, name: str, writes: _write_lock.WriteContext) -> GroupState:
    return open_test_group(
        path,
        group=name,
        initial_position=0,
        resume_direct=True,
        source_generation='generation',
        stream_fingerprint='fingerprint',
        verified_incarnation='incarnation',
        writes=writes,
    )


def _watchdog(name: str) -> ComponentWatchdog:
    return ComponentWatchdog(
        kind='lane',
        identity=name,
        worker_instance='worker',
        registration_token=name,
        startup_deadline_secs=10,
    )


@pytest.mark.parametrize('stopping', [False, True])
def test_delivery_write_wait_is_fatal_unless_runtime_is_stopping(
    tmp_path: Path,
    stopping: bool,
) -> None:
    path = tmp_path / 'source.db'
    with _group(path, 'local', write_context_for(path)):
        pass
    holder = write_context_for(path)
    stop = threading.Event()
    entered = threading.Event()

    @contextmanager
    def watch(phase: _write_lock.WritePhase, _deadline: float, /) -> Generator[None]:
        if phase == 'write-lock-wait':
            entered.set()
        yield

    def run() -> None:
        writes = replace(write_context_for(path), budget_secs=0.2, stop=stop, watch=watch)
        with _group(path, 'local', writes) as state:
            state.advance_irrelevant(1, expected_revision=state.snapshot().revision)

    component = _InterruptibleComponent(run, stop.set)
    try:
        with (
            sqlite3.connect(path, isolation_level=None) as connection,
            holder.transaction(connection),
        ):
            reason, forced = _run_cooperative_stop_component(
                component, entered, request_stop=stopping
            )
        assert forced == []
        if stopping:
            assert reason is None
        else:
            assert reason is not None
            assert 'WriteLockTimeoutError' in reason
            assert 'process-local' in reason
            assert str(path.with_suffix('.db.write-lock')) in reason
    finally:
        holder.lock.close()


def test_failed_open_releases_write_lock_without_masking_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / 'source.db'
    writes = write_context_for(path)
    original = writes.lock.close

    def close() -> None:
        original()
        raise RuntimeError('close failure')

    monkeypatch.setattr(writes.lock, 'close', close)
    with pytest.raises(ValueError, match='source limits') as raised:
        SourceState.open(
            path,
            writes=writes,
            create=True,
            site='studio',
            source='events',
            stream_fingerprint='fingerprint',
            source_generation='generation',
            max_open_ranges=0,
        )
    assert any('close failure' in note for note in raised.value.__notes__)
    assert writes.lock._entry is None
    assert not path.exists()


@pytest.mark.parametrize('command_name', ['park', 'replay', 'reset-group-to-latest'])
def test_operator_command_returns_source_database_busy_and_keeps_the_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command_name: str,
) -> None:
    group = _group_config('local', worker='default', ordering='eager')
    site = _runtime_site(group)
    _prepare_runtime_state(tmp_path, site, group, 'resume', park_reason='existing maintenance')
    config = _runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    with _running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        _local_assignment(spec, 'local', 'resume'),
        _runtime_provider(),
    ) as (runtime, control):
        _receive_typed_health(control, lambda item: any(group.parked for group in item.groups))
        drain_stop, drain_thread = _start_control_drain(control)
        path = source_database_path(config, 'main', 'events')
        holder = write_context_for(path)
        arguments: dict[str, object] = {
            'site': 'main',
            'group': 'local',
            'reason': 'maintenance',
        }
        if command_name != 'park':
            arguments.update(orphan_generation=7, reserved_generation=8)
        if command_name == 'replay':
            arguments.update(from_event_id=41, acknowledge_truncation=False)
        command = OperatorCommand(
            request_id='busy-command',
            command=command_name,
            arguments=arguments,
            identity=AuditIdentity.current(),
        )
        try:
            with (
                sqlite3.connect(path, isolation_level=None) as connection,
                holder.transaction(connection),
            ):
                before = time.monotonic()
                result = runtime._operator_response(command)
                elapsed = time.monotonic() - before
                assert not result.ok
                assert result.result.get('code') == 'source-database-busy', (
                    result,
                    elapsed,
                    runtime.fatal_reason,
                )
                assert 1.9 <= elapsed < 2.5
                assert not runtime._stop_event.is_set()
            if command_name == 'park':
                unpark = runtime._operator_response(replace(command, command='unpark'))
                assert unpark.ok
            result = runtime._operator_response(command)
            assert result.ok
            assert result.result['parked'] is True
            if command_name != 'park':
                assert result.result['group_generation'] == 8
            assert runtime.fatal_reason is None
        finally:
            holder.lock.close()
            drain_stop.set()
            drain_thread.join(timeout=5)
            assert not drain_thread.is_alive()


def test_operator_scope_restores_policy_after_failure(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with (
        SourceState.open(
            path,
            writes=write_context_for(path),
            create=True,
            site='studio',
            source='events',
            stream_fingerprint='fingerprint',
            source_generation='generation',
        ) as source,
        _group(path, 'local', write_context_for(path)) as group,
    ):
        for state in (source, group):
            original = state._writes

            def reject(
                state: SourceState | GroupState, original: _write_lock.WriteContext
            ) -> None:
                with state.operator_writes():
                    assert state._writes.lock is original.lock
                    assert state._writes.budget_secs == 2
                    raise ValueError('rejected')

            with pytest.raises(ValueError, match='rejected'):
                reject(state, original)
            assert state._writes is original


def test_source_and_group_writers_share_one_lock_per_file(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    held = threading.Event()
    release = threading.Event()
    waiting = threading.Event()
    holder_watchdog = _watchdog('holder')
    waiter_watchdog = _watchdog('waiter')
    holder_component = _LaneComponent.__new__(_LaneComponent)
    holder_component._lane = None
    holder_component._watchdog = holder_watchdog
    waiter_component = _LaneComponent.__new__(_LaneComponent)
    waiter_component._lane = None
    waiter_component._watchdog = waiter_watchdog

    @contextmanager
    def holder_watch(phase: _write_lock.WritePhase, deadline: float, /) -> Generator[None]:
        with holder_component._write_watch(phase, deadline):
            if phase == 'write-hold':
                held.set()
                assert release.wait(5)
            yield

    @contextmanager
    def waiter_watch(phase: _write_lock.WritePhase, deadline: float, /) -> Generator[None]:
        with waiter_component._write_watch(phase, deadline):
            if phase == 'write-lock-wait':
                waiting.set()
            yield

    with SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site='studio',
        source='events',
        stream_fingerprint='fingerprint',
        source_generation='generation',
    ) as source:
        source.initialize(0, incarnation='incarnation')
        for name in ('holder', 'waiter'):
            with _group(path, name, write_context_for(path)):
                pass

        def advance(name: str, watch: _write_lock._PhaseWatcher) -> None:
            writes = replace(write_context_for(path), watch=watch)
            with _group(path, name, writes) as state:
                state.advance_irrelevant(1, expected_revision=state.snapshot().revision)

        with ThreadPoolExecutor(max_workers=2) as pool:
            holder = pool.submit(advance, 'holder', holder_watch)
            assert held.wait(5)
            waiter = pool.submit(advance, 'waiter', waiter_watch)
            try:
                assert waiting.wait(5)
                assert holder_watchdog.sample().phase == 'write-hold'
                assert waiter_watchdog.sample().phase == 'write-lock-wait'
                assert not waiter.done()
            finally:
                release.set()
                holder.result(timeout=5)
                waiter.result(timeout=5)
    for name in ('holder', 'waiter'):
        with _group(path, name, write_context_for(path)) as state:
            assert state.snapshot().cursor == 1


def test_source_file_writes_never_use_the_unarbitrated_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    phases: list[str] = []

    @contextmanager
    def watch(phase: _write_lock.WritePhase, _deadline: float, /) -> Generator[None]:
        phases.append(phase)
        yield

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail('source file used an unarbitrated transaction')

    for module in (_sqlite, _source_state, _group_state):
        monkeypatch.setattr(module, 'transaction', forbidden, raising=False)
    path = tmp_path / 'source.db'
    lock = _write_lock.SourceWriteLock(
        path.with_suffix('.db.write-lock'),
        timeout_secs=1,
        wait_phase_grace_secs=1,
    )
    try:
        writes = _write_lock.WriteContext(lock, watch=watch)
        with SourceState.open(
            path,
            create=True,
            writes=writes,
            site='studio',
            source='events',
            stream_fingerprint='fingerprint',
            source_generation='generation',
        ) as state:
            assert phases[:2] == ['write-lock-wait', 'write-hold']
            state.initialize(100, incarnation='incarnation-1')
            with open_group(path) as group:
                group.advance_irrelevant(101, expected_revision=group.snapshot().revision)
                assert group.snapshot().cursor == 101
    finally:
        lock.close()


def test_supervisor_source_writes_use_the_operator_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    path, _ = _prepare_absent_orphan(config)
    supervisor = Supervisor(config=config, config_path=config_path)
    request = DispositionRequest(
        command='reset-to-latest',
        group='remote',
        disposition_id='budget-test',
        reserved_generation=2,
        expected_revision=supervisor._disposition_observation('test', 'remote', 'budget-test')[1],
        from_event_id=None,
        acknowledge_truncation=False,
        reason='operator budget',
        identity=AuditIdentity.current(),
        wall_clock_time=1.0,
    )

    def execute() -> object:
        metadata = supervisor._orphan_group_metadata(site='test', group='remote')
        with supervisor._open_lifecycle_group(metadata, group='remote', operator=True) as state:
            assert state._writes.stop is None
            assert state._writes.watch is _write_lock._no_watch
            return state.dispose(request)

    holder = write_context_for(path)
    try:
        with (
            sqlite3.connect(path, isolation_level=None) as connection,
            holder.transaction(connection),
            ThreadPoolExecutor(max_workers=1) as pool,
        ):
            before = time.monotonic()
            future = pool.submit(execute)
            with pytest.raises(_write_lock.SourceDatabaseBusyError, match='process-local'):
                future.result(timeout=3)
            assert 1.9 <= time.monotonic() - before < 2.5
        assert execute() == execute()
    finally:
        holder.lock.close()
