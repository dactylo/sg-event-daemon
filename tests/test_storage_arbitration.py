from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
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
from sg_event_daemon._write_lock import OPERATOR_WRITE_BUDGET_SECS
from tests._storage_support import (
    open_test_group,
    wait_for_future_signal,
    write_context_for,
)
from tests.source._support import open_group
from tests.test_supervisor import prepare_absent_orphan, source_only_config
from tests.test_worker import (
    InterruptibleComponent,
    local_assignment,
    prepare_runtime_state,
    receive_typed_health,
    run_cooperative_stop_component,
    running_concrete_runtime,
    runtime_config,
    runtime_provider,
    runtime_site,
    start_control_drain,
)
from tests.test_worker import (
    group_config as _group_config,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path
    from typing import Unpack

    from sg_event_daemon._group_state import GroupState


def _expire_blocked_operator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_write_lock, 'OPERATOR_WRITE_BUDGET_SECS', OPERATOR_WRITE_BUDGET_SECS)
    active = threading.local()
    acquire = _write_lock.SourceWriteLock._acquire_local
    transaction = _write_lock.SourceWriteLock.transaction

    @contextmanager
    def controlled(
        lock: _write_lock.SourceWriteLock,
        connection: sqlite3.Connection,
        **options: Unpack[_write_lock._TransactionOptions],
    ) -> Generator[sqlite3.Connection]:
        if 'budget_secs' not in options:
            with transaction(lock, connection, **options) as transaction_connection:
                yield transaction_connection
            return
        assert options['budget_secs'] == 2.0
        owner = threading.get_ident()
        original_clock = lock.clock
        started_at = original_clock()
        previous = getattr(active, 'operator', False)
        active.operator = True
        try:
            with monkeypatch.context() as patch:
                patch.setattr(
                    lock,
                    'clock',
                    lambda: started_at if threading.get_ident() == owner else original_clock(),
                )
                with transaction(lock, connection, **options) as transaction_connection:
                    yield transaction_connection
        finally:
            active.operator = previous

    def expire(lock: _write_lock.SourceWriteLock, attempt: _write_lock._Attempt) -> None:
        if not getattr(active, 'operator', False) or not attempt.entry.lock.locked():
            acquire(lock, attempt)
            return
        assert attempt.deadline_at == pytest.approx(attempt.timing.started_at + 2)
        owner = threading.get_ident()
        original_clock = lock.clock
        with monkeypatch.context() as patch:
            patch.setattr(
                lock,
                'clock',
                lambda: (
                    attempt.deadline_at if threading.get_ident() == owner else original_clock()
                ),
            )
            acquire(lock, attempt)

    monkeypatch.setattr(_write_lock.SourceWriteLock, '_acquire_local', expire)
    monkeypatch.setattr(_write_lock.SourceWriteLock, 'transaction', controlled)


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


def test_delivery_write_wait_is_never_fatal(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _group(path, 'local', write_context_for(path)):
        pass
    holder = write_context_for(path)
    stop = threading.Event()
    entered = threading.Event()
    attempts: list[str] = []
    clock = [0.0]

    @contextmanager
    def watch(phase: _write_lock.WritePhase, _deadline: float, /) -> Generator[None]:
        if phase == 'write-lock-wait':
            attempts.append(phase)
            clock[0] += 0.05
            if len(attempts) == 3:
                entered.set()
                stop.wait()
        yield

    def run() -> None:
        lock = _write_lock.SourceWriteLock(
            path.with_suffix('.db.write-lock'),
            database_path=path,
            timeout_secs=0.05,
            wait_phase_grace_secs=1,
            clock=lambda: clock[0],
        )
        writes = _write_lock.WriteContext(lock, stop=stop, watch=watch)
        with _group(path, 'local', writes) as state:
            state.advance_irrelevant(1, expected_revision=state.snapshot().revision)

    component = InterruptibleComponent(run, stop.set)
    try:
        with (
            closing(sqlite3.connect(path, isolation_level=None)) as connection,
            connection,
            holder.transaction(connection),
        ):
            reason, forced = run_cooperative_stop_component(component, entered, request_stop=True)
        assert forced == []
        assert reason is None
        assert len(attempts) == 3
    finally:
        holder.lock.close()


@pytest.mark.parametrize('command_name', ['park', 'replay', 'reset-group-to-latest'])
def test_operator_command_returns_source_database_busy_and_keeps_the_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command_name: str,
) -> None:
    group = _group_config('local', worker='default', ordering='eager')
    site = runtime_site(group)
    prepare_runtime_state(tmp_path, site, group, 'resume', park_reason='existing maintenance')
    config = runtime_config(tmp_path, site)
    spec = build_worker_specs(site)[0]
    with running_concrete_runtime(
        monkeypatch,
        config,
        spec,
        local_assignment(spec, 'local', 'resume'),
        runtime_provider(),
    ) as (runtime, control):
        receive_typed_health(control, lambda item: any(group.parked for group in item.groups))
        drain_stop, drain_thread = start_control_drain(control)
        path = source_database_path(config, 'main', 'events')
        holder = write_context_for(path)
        arguments: dict[str, object] = {
            'site': 'main',
            'group': 'local',
            'reason': 'maintenance',
        }
        if command_name != 'park':
            arguments.update(
                disposition_id='busy-disposition', orphan_generation=7, reserved_generation=8
            )
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
                closing(sqlite3.connect(path, isolation_level=None)) as connection,
                connection,
                holder.transaction(connection),
            ):
                _expire_blocked_operator(monkeypatch)
                result = runtime._operator_response(command)
                assert not result.ok
                assert result.result.get('code') == 'source-database-busy', (
                    result,
                    runtime.fatal_reason,
                )
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
            drain_thread.join()
            assert not drain_thread.is_alive()


@pytest.mark.slow
def test_fourteen_writers_in_one_process_all_commit(tmp_path: Path) -> None:
    path = tmp_path / 'queue.db'
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute('CREATE TABLE commits (writer INTEGER, turn INTEGER)')
    barrier = threading.Barrier(14)

    def write(writer: int) -> int:
        lock = _write_lock.SourceWriteLock(
            path.with_suffix('.write-lock'),
            database_path=path,
            timeout_secs=0.3,
            wait_phase_grace_secs=1,
            clock=lambda: 0.0,
        )
        writes = _write_lock.WriteContext(lock)
        try:
            with closing(sqlite3.connect(path, isolation_level=None)) as connection, connection:
                barrier.wait()
                for turn in range(2):
                    with writes.transaction(connection):
                        connection.execute('INSERT INTO commits VALUES (?, ?)', (writer, turn))
                return 2
        finally:
            lock.close()

    with ThreadPoolExecutor(max_workers=14) as pool:
        totals = list(pool.map(write, range(14)))
    assert all(count > 0 for count in totals)
    with closing(sqlite3.connect(path)) as connection, connection:
        assert connection.execute('SELECT COUNT(*) FROM commits').fetchone() == (sum(totals),)


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
                    assert state._writes.policy == 'operator'
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
                assert release.wait()
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
            wait_for_future_signal(held, holder)
            waiter = pool.submit(advance, 'waiter', waiter_watch)
            try:
                wait_for_future_signal(waiting, waiter)
                assert holder_watchdog.sample().phase == 'write-hold'
                assert waiter_watchdog.sample().phase == 'write-lock-wait'
                assert not waiter.done()
            finally:
                release.set()
                holder.result()
                waiter.result()
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
        database_path=path,
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
    config_path, config = source_only_config(tmp_path, monkeypatch)
    path, _ = prepare_absent_orphan(config)
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
        _expire_blocked_operator(monkeypatch)
        metadata = supervisor._orphan_group_metadata(site='test', group='remote')
        with supervisor._open_lifecycle_group(metadata, group='remote', operator=True) as state:
            assert state._writes.stop is None
            assert state._writes.watch is _write_lock._no_watch
            return state.dispose(request)

    holder = write_context_for(path)
    try:
        with (
            closing(sqlite3.connect(path, isolation_level=None)) as connection,
            connection,
            holder.transaction(connection),
            ThreadPoolExecutor(max_workers=1) as pool,
        ):
            future = pool.submit(execute)
            with pytest.raises(_write_lock.SourceDatabaseBusyError, match='process-local'):
                future.result()
        assert execute() == execute()
    finally:
        holder.lock.close()
