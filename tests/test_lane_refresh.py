from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import ExitStack, closing
from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, cast

import pytest

from sg_event_daemon import _worker as worker_module
from sg_event_daemon._fanout import EventFanout
from sg_event_daemon._group_types import AuditIdentity
from sg_event_daemon._ipc import (
    ActiveSourceGroupAssignment,
    DeliveryGroupAssignment,
    EventPeerGrant,
    OperatorCommand,
    WorkerAssignment,
)
from sg_event_daemon._source import CatchupReader
from sg_event_daemon._worker import _CatchupLimiter, _LaneRefresh, build_worker_specs
from tests import test_worker as worker_tests
from tests._storage_support import (
    bind_group,
    insert_range,
    insert_source_late_ids,
)
from tests.source.test_state import record_source_late_ids
from tests.test_lane import _callback, _concrete_strict_states, _event, _lane

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path

    from sg_event_daemon._config import Config
    from sg_event_daemon._events import Event
    from sg_event_daemon._group_state import SourceProgress
    from sg_event_daemon._ipc import WorkerControl
    from sg_event_daemon._plugins import EventContext
    from sg_event_daemon._providers import Provider
    from sg_event_daemon._worker import WorkerRuntime


class _RetryReader:
    def __init__(self, failure: str) -> None:
        self.failure = failure
        self.attempts = 0

    def retention_floor(self) -> int:
        if self.failure == 'floor':
            self.failure = ''
            raise ConnectionError('provider unavailable')
        return 1

    def scan_range(self, _low: int, _high: int) -> Generator[Event]:
        self.attempts += 1
        if self.failure == 'first':
            self.failure = ''
            raise ConnectionError('provider unavailable')
        yield _event(11)
        if self.failure == 'later':
            self.failure = ''
            raise ConnectionError('provider unavailable')
        yield _event(12)


def _observe_idle_scan(
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    durations: list[float],
    first_refresh: list[bool],
) -> None:
    scans_started = 0
    scan = CatchupReader.scan_range
    scan_obligations = _LaneRefresh._scan_obligations

    def observed_refresh(
        refresh: _LaneRefresh,
        synchronization: SourceProgress,
        scan_fence: tuple[str, int, int] | None = None,
    ) -> None:
        applicable = scan_fence is None and any(
            pending.obligation_id == 'range-42-42'
            and pending.closed
            and (
                ordering != 'strict'
                or synchronization.source_successful_clock >= pending.eligibility_point
            )
            for pending in refresh._pending
        )
        before = scans_started
        scan_obligations(refresh, synchronization, scan_fence)
        if applicable and not first_refresh:
            first_refresh.append(scans_started > before)

    def measured_scan(reader: CatchupReader, low: int, high: int) -> Generator[Event]:
        nonlocal scans_started
        if (low, high) == (42, 42):
            scans_started += 1
        started = time.monotonic()
        try:
            yield from scan(reader, low, high)
        finally:
            durations.append(time.monotonic() - started)

    monkeypatch.setattr(CatchupReader, 'scan_range', measured_scan)
    monkeypatch.setattr(_LaneRefresh, '_scan_obligations', observed_refresh)


def _scan_failure_evidence(
    connection: sqlite3.Connection, runtimes: tuple[WorkerRuntime, WorkerRuntime]
) -> str:
    queries = (
        'SELECT * FROM source_meta',
        'SELECT * FROM group_state',
        'SELECT * FROM groups',
        'SELECT * FROM epochs',
        'SELECT * FROM range_groups',
        'SELECT * FROM discovery_ranges',
        'SELECT * FROM late_ids',
    )
    rows = {query: connection.execute(query).fetchall() for query in queries}
    health = [
        (
            runtime.fatal_reason,
            None if runtime._health_cache is None else runtime._health_cache._snapshot(),
        )
        for runtime in runtimes
    ]
    return f'rows={rows} health={health}'


def _start_idle_runtime(
    stack: ExitStack,
    monkeypatch: pytest.MonkeyPatch,
    config: Config,
    provider: Provider,
    *,
    remote: bool,
) -> tuple[WorkerRuntime, WorkerRuntime, WorkerControl]:
    specs = build_worker_specs(config.sites['main'])
    if remote:
        grant = EventPeerGrant(
            source='events',
            group='remote',
            dependent_spawn_instance='dependent',
            capability='capability',
        )
        source_assignment = WorkerAssignment(
            worker_id=specs[0].worker_id,
            spawn_instance='site-spawn-1',
            source_generations=(('events', 'source-generation-1'),),
            group_assignments=(),
            source_group_assignments=(
                ActiveSourceGroupAssignment(
                    source='events',
                    group='remote',
                    group_generation=7,
                    start_mode='initialize',
                ),
            ),
            source_endpoints=(),
            event_grants=(),
        )
        source_session = worker_tests._start_source_runtime(
            config, specs[0], source_assignment, (grant,)
        )
        stack.callback(source_session._close)
        source_runtime = source_session._runtime
        assignment = WorkerAssignment(
            worker_id=specs[1].worker_id,
            spawn_instance='dependent',
            source_generations=(),
            group_assignments=(
                DeliveryGroupAssignment(
                    source='events',
                    group='remote',
                    group_generation=7,
                    start_mode='initialize',
                ),
            ),
            source_group_assignments=(),
            source_endpoints=(source_session._endpoint,),
            event_grants=(grant,),
        )
        runtime, control = stack.enter_context(
            worker_tests._running_concrete_runtime(
                monkeypatch, config, specs[1], assignment, provider
            )
        )
    else:
        assignment = worker_tests._local_assignment(specs[0], 'remote', 'initialize')
        runtime, control = stack.enter_context(
            worker_tests._running_concrete_runtime(
                monkeypatch, config, specs[0], assignment, provider
            )
        )
        source_runtime = runtime
    return runtime, source_runtime, control


def _wait_for(predicate: Callable[[], bool], seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        assert time.monotonic() < deadline, 'durable scan did not complete within its bound'
        time.sleep(0.002)


def _wait_for_scan(
    connection: sqlite3.Connection,
    invoked: list[int],
    *,
    durations: list[float],
    first_refresh: list[bool],
    runtimes: tuple[WorkerRuntime, WorkerRuntime],
) -> None:
    try:
        _wait_for(
            lambda: (
                bool(first_refresh)
                and 42 in invoked
                and connection.execute(
                    'SELECT 1 FROM range_groups WHERE '
                    "obligation_id='range-42-42' AND "
                    "obligation_state='pending'"
                ).fetchone()
                is None
            ),
            # This is a hang guard, not a limit on provider scan duration.
            10.0,
        )
    except AssertionError as error:
        error.add_note(_scan_failure_evidence(connection, runtimes))
        error.add_note(f'invoked={invoked} scans={durations} first_refresh={first_refresh}')
        raise


@pytest.mark.parametrize('failure', ['floor', 'first', 'later'])
def test_failed_scan_is_retried_without_a_revision_change(tmp_path: Path, failure: str) -> None:
    delivered: list[int] = []

    def callback(context: EventContext) -> None:
        delivered.append(cast('int', context.event['id']))

    with _concrete_strict_states(
        tmp_path / 'source.db', initial_position=10, source_initial=20
    ) as (source, state):
        connection = vars(source)['_database'].connection
        insert_range(connection, 'range-a', 11, 12, state='closed')
        bind_group(connection, 'range-a', 'serial', 1, floor=10, eligibility_point=0)
        insert_source_late_ids(connection, 'range-a', (11, 12))
        lane, _factory = _lane(state, (_callback(0, 'callback', callback),), ordering='strict')
        assert lane.setup() is None
        reader = _RetryReader(failure)
        refresh = _LaneRefresh(
            lane=lane,
            state=state,
            reader=cast('CatchupReader', reader),
            limiter=_CatchupLimiter(1),
        )
        revision = state.refresh_ranges()[0].range_revision
        refresh._service()
        assert state.pending_ranges()
        assert state.refresh_ranges()[0].range_revision == revision
        refresh._service()
        assert state.pending_ranges() == ()
        assert delivered == [11, 12]
        lane.close()


@pytest.fixture
def suppress_range_changed(monkeypatch: pytest.MonkeyPatch) -> None:
    def suppress_hint(_fanout: EventFanout) -> None:
        pass

    monkeypatch.setattr(EventFanout, 'publish_range_changed', suppress_hint)


def _idle_scan_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    ordering: Literal['eager', 'strict'],
    remote: bool,
    parked: bool = False,
) -> None:
    interval = 0.05
    group = replace(
        worker_tests._group('remote', worker='pool' if remote else 'default', ordering=ordering),
        strict_holdback_secs=0.01 if ordering == 'strict' else None,
    )
    site = worker_tests._runtime_site(group)
    site = replace(
        site,
        sources=MappingProxyType(
            {'events': replace(site.sources['events'], fetch_interval_secs=interval)}
        ),
    )
    config = worker_tests._runtime_config(tmp_path, site)
    provider, provider_state, invoked = worker_tests._concrete_delivery_provider(
        monkeypatch, 'resume'
    )
    provider_state._publish(41)
    monkeypatch.setattr(worker_module, 'load_provider', worker_tests._provider_loader(provider))
    path = worker_tests._source_database_path(tmp_path)
    durations: list[float] = []
    first_refresh: list[bool] = []
    _observe_idle_scan(monkeypatch, ordering, durations, first_refresh)
    with ExitStack() as stack:
        runtime, source_runtime, control = _start_idle_runtime(
            stack,
            monkeypatch,
            config,
            provider,
            remote=remote,
        )
        worker_tests._receive_typed_health(
            control, lambda update: any(item.join_phase == 'attached' for item in update.groups)
        )
        drain = worker_tests._start_control_drain(control)
        stack.callback(worker_tests._stop_control_drain, *drain)
        source_component = cast(
            'worker_module._SourceComponent', source_runtime._components['source:events']
        )
        lane_component = cast('worker_module._LaneComponent', runtime._components['lane:remote'])
        provider_state._publish(43)
        with closing(sqlite3.connect(path, isolation_level=None)) as connection:
            _wait_for(
                lambda: (
                    connection.execute(
                        "SELECT 1 FROM discovery_ranges WHERE obligation_id='range-42-42'"
                    ).fetchone()
                    is not None
                ),
                2,
            )
            if parked:
                lane_component._execute_operator_command(
                    OperatorCommand(
                        request_id='park',
                        command='park',
                        arguments={'site': 'main', 'group': 'remote', 'reason': 'test'},
                        identity=AuditIdentity.current(),
                    )
                )

            def close_range(surface: worker_module._SourceJoinSurface) -> None:
                state = surface._state
                provider_state._publish(42)
                record_source_late_ids(
                    state, 'range-42-42', (42,), successful_clock=state.successful_clock()
                )
                state._close_range('range-42-42', successful_clock=state.successful_clock())

            source_component._call(close_range, deadline_secs=2, stop=threading.Event())
            if parked:
                time.sleep(interval * 2)
                assert 42 not in invoked
                started = time.monotonic()
                lane_component._execute_operator_command(
                    OperatorCommand(
                        request_id='unpark',
                        command='unpark',
                        arguments={'site': 'main', 'group': 'remote', 'reason': 'test'},
                        identity=AuditIdentity.current(),
                    )
                )
            else:
                started = time.monotonic()
            _wait_for_scan(
                connection,
                invoked,
                durations=durations,
                first_refresh=first_refresh,
                runtimes=(runtime, source_runtime),
            )
            delay = time.monotonic() - started
            assert durations
            # Cadence is checked with a controlled clock in the worker tests;
            # this integration checks scanning within the first eligible tick.
            assert first_refresh == [True]
            print(
                f'ordering={ordering} remote={remote} parked={parked} '
                f'delay={delay:.6f} scan={sum(durations):.6f}'
            )
            assert runtime.fatal_reason is None


@pytest.mark.usefixtures('suppress_range_changed')
def test_dropped_final_close_allows_provider_scan_duration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scan = CatchupReader.scan_range
    delayed = threading.Event()

    def slow_scan(reader: CatchupReader, low: int, high: int) -> Generator[Event]:
        if (low, high) == (42, 42):
            delayed.set()
            time.sleep(1.0)
        yield from scan(reader, low, high)

    monkeypatch.setattr(CatchupReader, 'scan_range', slow_scan)
    _idle_scan_check(tmp_path, monkeypatch, ordering='strict', remote=False)
    assert delayed.is_set()


@pytest.mark.usefixtures('suppress_range_changed')
@pytest.mark.parametrize(
    ('ordering', 'remote'), [('eager', False), ('strict', False), ('eager', True)]
)
def test_dropped_final_close_is_scanned_by_an_idle_lane(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    remote: bool,
) -> None:
    _idle_scan_check(tmp_path, monkeypatch, ordering=ordering, remote=remote)


@pytest.mark.usefixtures('suppress_range_changed')
def test_parked_lane_scans_after_unpark_without_a_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _idle_scan_check(tmp_path, monkeypatch, ordering='eager', remote=False, parked=True)
