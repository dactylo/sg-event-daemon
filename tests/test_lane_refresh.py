from __future__ import annotations

import logging
import sqlite3
import threading
import time
from contextlib import ExitStack, closing, contextmanager, nullcontext
from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, TypedDict, Unpack, cast

import pytest

from sg_event_daemon import _worker as worker_module
from sg_event_daemon._fanout import EventFanout, GroupChannel
from sg_event_daemon._group_types import (
    AuditIdentity,
    CompleteLateStep,
    GroupSnapshot,
    ReserveLateStep,
)
from sg_event_daemon._ipc import (
    ActiveSourceGroupAssignment,
    DeliveryGroupAssignment,
    EventPeerGrant,
    OperatorCommand,
    WorkerAssignment,
)
from sg_event_daemon._source import CatchupReader, SourcePoller, _IdRunPage
from sg_event_daemon._worker import _LaneRefresh, build_worker_specs
from tests import test_worker as worker_tests
from tests._storage_support import (
    bind_group,
    close_test_range,
    insert_range,
    insert_source_late_ids,
)
from tests.group._support import commit_step
from tests.source.test_state import record_source_late_ids
from tests.test_lane import (
    Coordinator,
    build_lane,
    concrete_strict_states,
    make_event,
    offer_event,
    open_group_state,
    record_callback,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence
    from pathlib import Path

    from sg_event_daemon._config import Config
    from sg_event_daemon._gaps import IdRun
    from sg_event_daemon._group_state import GroupState, SourceProgress
    from sg_event_daemon._ipc import WorkerControl
    from sg_event_daemon._lane import GroupLane
    from sg_event_daemon._plugins import EventContext
    from sg_event_daemon._providers import Provider
    from sg_event_daemon._worker import _WorkerRuntime


class _MissingReader:
    def __init__(self) -> None:
        self.calls: list[object] = []
        self.pages = 0
        self.omitted: set[int] = set()
        self.failure = ''

    def fetch_id_runs(self, runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
        self.calls.append(tuple(runs))
        if self.failure == 'transport':
            self.failure = ''
            raise ConnectionError('fetch unavailable')
        ids = [
            event_id
            for low, high in runs
            for event_id in range(low, high + 1)
            if event_id not in self.omitted
        ]
        for offset in range(0, len(ids), 2):
            page = ids[offset : offset + 2]
            self.pages += 1
            yield _IdRunPage(
                events=tuple(make_event(event_id) for event_id in page), covered_through=page[-1]
            )

        if self.failure == 'interrupted':
            self.failure = ''
            raise ConnectionError('fetch interrupted after pages')

    def retention_floor(self) -> int:
        if self.failure == 'retention' or (
            self.failure == 'closing-retention'
            and self.calls
            and isinstance(self.calls[-1], tuple)
        ):
            self.failure = ''
            self.calls.append('failed retention')
            raise ConnectionError('retention unavailable')
        self.calls.append('retention')
        return 1


class _RetryReader:
    def __init__(self, failure: str) -> None:
        self._failure = failure
        self._attempts = 0

    def fetch_id_runs(self, runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
        self._attempts += 1
        if self._failure == 'first':
            self._failure = ''
            raise ConnectionError('provider unavailable')
        for event_id in (11, 12):
            if any(low <= event_id <= high for low, high in runs):
                yield _IdRunPage((make_event(event_id),), event_id)
                if self._failure == 'later':
                    self._failure = ''
                    raise ConnectionError('provider unavailable')

    def retention_floor(self) -> int:
        if self._failure == 'floor':
            self._failure = ''
            raise ConnectionError('provider unavailable')
        return 1


def _clock(state: GroupState, clock: float) -> None:
    state._database.connection.execute('UPDATE source_meta SET successful_clock=?', (clock,))


def _observe_idle_scan(
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    durations: list[float],
    first_refresh: list[bool],
) -> None:
    scans_started = 0
    scan = CatchupReader.fetch_id_runs
    scan_obligations = _LaneRefresh._scan_obligations

    def observed_refresh(
        refresh: _LaneRefresh,
        synchronization: SourceProgress,
        scan_fence: tuple[str, int, int] | None = None,
        *,
        slot_held: bool = False,
    ) -> None:
        applicable = scan_fence is None and any(
            pending.obligation_id == 'range-42-42'
            and pending.closed
            and (
                ordering != 'strict'
                or synchronization.source_successful_clock >= pending.eligibility_point
            )
            and (
                ordering != 'eager'
                or (
                    refresh._state.snapshot().cursor >= pending.high_id
                    and synchronization.source_successful_clock >= pending.visibility_deadline
                )
            )
            for pending in refresh._pending
        )
        before = scans_started
        scan_obligations(refresh, synchronization, scan_fence, slot_held=slot_held)
        if applicable and not first_refresh:
            first_refresh.append(scans_started > before)

    def measured_scan(reader: CatchupReader, runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
        nonlocal scans_started
        if tuple(runs) == ((42, 42),):
            scans_started += 1
        started = time.monotonic()
        try:
            yield from scan(reader, runs)
        finally:
            durations.append(time.monotonic() - started)

    monkeypatch.setattr(CatchupReader, 'fetch_id_runs', measured_scan)
    monkeypatch.setattr(_LaneRefresh, '_scan_obligations', observed_refresh)


def _observe_parked_service(
    monkeypatch: pytest.MonkeyPatch, closed: threading.Event
) -> threading.Event:
    parked_serviced = threading.Event()
    drive_halted = worker_module._LaneComponent._drive_halted

    def observe_parked(
        component: worker_module._LaneComponent,
        resources: worker_module._LaneResources,
        lifecycle: worker_module._LaneLifecycle,
    ) -> None:
        drive_halted(component, resources, lifecycle)
        if closed.is_set() and resources.state.snapshot().parked:
            parked_serviced.set()

    monkeypatch.setattr(worker_module._LaneComponent, '_drive_halted', observe_parked)
    return parked_serviced


def _scan_failure_evidence(
    connection: sqlite3.Connection, runtimes: tuple[_WorkerRuntime, _WorkerRuntime]
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
            runtime._health_cache.snapshot(),
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
) -> tuple[_WorkerRuntime, _WorkerRuntime, WorkerControl]:
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
        source_session = worker_tests.start_source_runtime(
            config, specs[0], source_assignment, (grant,)
        )
        stack.callback(source_session.close)
        source_runtime = source_session.runtime
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
            source_endpoints=(source_session.endpoint,),
            event_grants=(grant,),
        )
        runtime, control = stack.enter_context(
            worker_tests.running_concrete_runtime(
                monkeypatch, config, specs[1], assignment, provider
            )
        )
    else:
        assignment = worker_tests.local_assignment(specs[0], 'remote', 'initialize')
        runtime, control = stack.enter_context(
            worker_tests.running_concrete_runtime(
                monkeypatch, config, specs[0], assignment, provider
            )
        )
        source_runtime = runtime
    return runtime, source_runtime, control


def _wait_for(
    predicate: Callable[[], bool], runtimes: tuple[_WorkerRuntime, _WorkerRuntime]
) -> None:
    while not predicate():
        for runtime in runtimes:
            assert runtime.fatal_reason is None, runtime.fatal_reason
        time.sleep(0.002)


def _wait_for_scan(
    connection: sqlite3.Connection,
    invoked: list[int],
    *,
    durations: list[float],
    first_refresh: list[bool],
    runtimes: tuple[_WorkerRuntime, _WorkerRuntime],
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
            runtimes,
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

    with concrete_strict_states(
        tmp_path / 'source.db', initial_position=10, source_initial=20
    ) as (source, state):
        connection = vars(source)['_database'].connection
        insert_range(connection, 'range-a', 11, 12, state='closed')
        bind_group(connection, 'range-a', 'serial', 1, floor=10, eligibility_point=0)
        insert_source_late_ids(connection, 'range-a', (11, 12))
        lane, _factory = build_lane(
            state, (record_callback(0, 'callback', callback),), ordering='strict'
        )
        assert lane.setup() is None
        reader = _RetryReader(failure)
        refresh = _LaneRefresh(
            lane=lane,
            state=state,
            reader=cast('CatchupReader', reader),
            slot=nullcontext,
            joined=lambda: False,
            fetch_interval_secs=1,
            late_event_window_secs=300,
            log_extra={},
        )
        revision = state.refresh_ranges()[0].range_revision
        refresh.service()
        assert state.pending_ranges()
        assert state.refresh_ranges()[0].range_revision == revision
        _clock(state, 1)
        refresh.service()
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
    # Keep this lane-scan probe independent of the source's background gap recheck.
    def skip_source_recheck(_poller: SourcePoller) -> None:
        return None

    monkeypatch.setattr(SourcePoller, '_recheck_missing_if_due', skip_source_recheck)
    closed = threading.Event()
    parked_serviced = _observe_parked_service(monkeypatch, closed)
    interval = 0.05
    group = replace(
        worker_tests.group_config(
            'remote', worker='pool' if remote else 'default', ordering=ordering
        ),
        strict_holdback_secs=0.01 if ordering == 'strict' else None,
    )
    site = worker_tests.runtime_site(group)
    site = replace(
        site,
        sources=MappingProxyType(
            {
                'events': replace(
                    site.sources['events'],
                    fetch_interval_secs=interval,
                    late_event_window_secs=0.2,
                )
            }
        ),
    )
    config = worker_tests.runtime_config(tmp_path, site)
    provider, provider_state, invoked = worker_tests.concrete_delivery_provider(
        monkeypatch, 'resume'
    )
    provider_state.publish(41)
    monkeypatch.setattr(worker_module, 'load_provider', worker_tests.provider_loader(provider))
    path = worker_tests.source_database_path(tmp_path)
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
        worker_tests.receive_typed_health(
            control, lambda update: any(item.join_phase == 'attached' for item in update.groups)
        )
        drain = worker_tests.start_control_drain(control)
        stack.callback(worker_tests.stop_control_drain, *drain)
        source_component = cast(
            'worker_module._SourceComponent', source_runtime._components['source:events']
        )
        lane_component = cast('worker_module._LaneComponent', runtime._components['lane:remote'])
        provider_state.publish(43)
        with closing(sqlite3.connect(path, isolation_level=None)) as connection:
            _wait_for(
                lambda: (
                    connection.execute(
                        "SELECT 1 FROM discovery_ranges WHERE obligation_id='range-42-42'"
                    ).fetchone()
                    is not None
                ),
                (runtime, source_runtime),
            )
            if parked:
                lane_component.execute_operator_command(
                    OperatorCommand(
                        request_id='park',
                        command='park',
                        arguments={'site': 'main', 'group': 'remote', 'reason': 'test'},
                        identity=AuditIdentity.current(),
                    )
                )

            def close_range(surface: worker_module._SourceJoinSurface) -> None:
                state = surface.state
                provider_state.publish(42)
                record_source_late_ids(
                    state, 'range-42-42', (42,), successful_clock=state.successful_clock()
                )
                state._close_range('range-42-42', successful_clock=state.successful_clock())

            source_component.call(close_range, stop=threading.Event(), wait_phase=nullcontext)
            closed.set()
            if parked:
                _wait_for(parked_serviced.is_set, (runtime, source_runtime))
                assert 42 not in invoked
                started = time.monotonic()
                lane_component.execute_operator_command(
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
    scan = CatchupReader.fetch_id_runs
    delayed = threading.Event()
    release = threading.Event()

    def acknowledge_scan() -> None:
        delayed.wait()
        release.set()

    observer = threading.Thread(target=acknowledge_scan)
    observer.start()

    def slow_scan(reader: CatchupReader, runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
        if tuple(runs) == ((42, 42),):
            delayed.set()
            release.wait()
        yield from scan(reader, runs)

    monkeypatch.setattr(CatchupReader, 'fetch_id_runs', slow_scan)
    try:
        _idle_scan_check(tmp_path, monkeypatch, ordering='strict', remote=False)
        assert delayed.is_set()
    finally:
        delayed.set()
        release.set()
        observer.join()


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


class _CloseoutOptions(TypedDict, total=False):
    ordering: Literal['eager', 'strict']
    joined: bool
    clock: float
    floor: int
    done: tuple[int, ...]
    callback: Callable[[EventContext], None]
    low: int
    high: int
    cursor: int


@contextmanager
def _closeout_fixture(
    tmp_path: Path,
    **options: Unpack[_CloseoutOptions],
) -> Generator[tuple[_LaneRefresh, GroupState, GroupLane, _MissingReader, list[int]]]:
    delivered: list[int] = []

    def record(context: EventContext) -> None:
        delivered.append(cast('int', context.event['id']))

    callback = options.get('callback', record)
    ordering = options.get('ordering', 'eager')
    clock = options.get('clock', 0)
    floor = options.get('floor', 100)
    done = options.get('done', ())
    joined = options.get('joined', False)
    low = options.get('low', 101)
    high = options.get('high', 110)
    cursor = options.get('cursor', 110 if ordering == 'eager' else 100)
    with open_group_state(
        tmp_path / 'closeout.db', initial_position=100, ordering=ordering
    ) as state:
        connection = state._database.connection
        connection.execute(
            'UPDATE source_meta SET initialized=1, frontier=120, successful_clock=?', (clock,)
        )
        connection.execute(
            'UPDATE group_state SET cursor=?, delivery_floor=?',
            (cursor, floor),
        )
        insert_range(connection, 'range-a', low, high, state='closed')
        connection.execute('UPDATE discovery_ranges SET visibility_deadline=300')
        bind_group(connection, 'range-a', 'serial', 1, floor=floor, eligibility_point=30)
        insert_source_late_ids(connection, 'range-a', range(low, high + 1))
        for event_id in done:
            snapshot = commit_step(
                state,
                ReserveLateStep('range-a', event_id),
                expected_revision=state.snapshot().revision,
            )
            commit_step(
                state, CompleteLateStep('range-a', event_id), expected_revision=snapshot.revision
            )
        lane, _factory = build_lane(
            state, (record_callback(0, 'callback', callback),), ordering=ordering
        )
        assert lane.setup() is None
        reader = _MissingReader()
        refresh = _LaneRefresh(
            lane=lane,
            state=state,
            reader=cast('CatchupReader', reader),
            slot=nullcontext,
            joined=lambda: joined,
            fetch_interval_secs=1,
            late_event_window_secs=300,
            log_extra={'site': 'test', 'source': 'events', 'group': 'serial'},
        )
        try:
            yield refresh, state, lane, reader, delivered
        finally:
            lane.close()


@pytest.fixture
def closeout_logs(caplog: pytest.LogCaptureFixture) -> Generator[pytest.LogCaptureFixture]:
    logger = logging.getLogger('sg_event_daemon')
    level = logger.level
    propagate = logger.propagate
    logger.propagate = False
    logger.addHandler(caplog.handler)
    logger.setLevel(logging.INFO)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)
        logger.setLevel(level)
        logger.propagate = propagate


def _restart_refresh(refresh: _LaneRefresh, window: float = 300) -> _LaneRefresh:
    return _LaneRefresh(
        lane=refresh._lane,
        state=refresh._state,
        reader=refresh._reader,
        slot=nullcontext,
        joined=refresh._joined,
        fetch_interval_secs=1,
        late_event_window_secs=window,
        log_extra=refresh._log_extra,
    )


def test_closeout_fetches_only_ids_above_the_floor(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, floor=105) as (refresh, state, _lane, reader, delivered):
        refresh.service()
        assert state.pending_ranges() == ()
        assert reader.calls == ['retention', ((106, 110),), 'retention']
        assert delivered == list(range(106, 111))


def test_closeout_stops_when_delivery_is_interrupted(tmp_path: Path) -> None:
    delivered: list[int] = []
    lane: GroupLane

    def callback(context: EventContext) -> None:
        delivered.append(cast('int', context.event['id']))
        lane.request_shutdown()

    with _closeout_fixture(tmp_path, callback=callback) as (
        refresh,
        state,
        lane,
        reader,
        _delivered,
    ):
        refresh.service()
        assert state.snapshot().status == 'running'
        assert state.pending_ranges()
        assert delivered == [101]
        assert reader.calls == ['retention', ((101, 110),)]
        assert reader.pages == 1
        lane.close()
        next_lane, _factory = build_lane(
            state,
            (
                record_callback(
                    0,
                    'callback',
                    lambda context: delivered.append(cast('int', context.event['id'])),
                ),
            ),
        )
        assert next_lane.setup() is None
        refresh._lane = next_lane
        try:
            _clock(state, 1)
            refresh.service()
            assert reader.calls[3] == ((102, 110),)
            assert delivered == list(range(101, 111))
            assert state.pending_ranges() == ()
        finally:
            next_lane.close()


def test_closeout_warnings_truncate_long_id_lists(
    tmp_path: Path,
    closeout_logs: pytest.LogCaptureFixture,
) -> None:
    with _closeout_fixture(tmp_path, high=124, cursor=124) as (
        refresh,
        state,
        _lane,
        reader,
        _delivered,
    ):
        reader.omitted = set(range(101, 125, 2))
        refresh.service()
        _clock(state, 300)
        refresh.service()
        assert len(closeout_logs.records) == 2
        assert all(' and 2 more' in message for message in closeout_logs.messages)
        assert all(record.name == 'sg_event_daemon.group' for record in closeout_logs.records)


def test_detached_eager_closeout_fetches_missing_at_close(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, done=(102, 108)) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        refresh.service()
        assert state.pending_ranges() == ()
        assert reader.calls == ['retention', ((101, 101), (103, 107), (109, 110)), 'retention']
        assert delivered == [101, 103, 104, 105, 106, 107, 109, 110]


def test_detached_strict_exhaustion_beyond_cursor_finishes_obligation(
    tmp_path: Path,
) -> None:
    with _closeout_fixture(
        tmp_path, ordering='strict', clock=30, low=106, high=108, cursor=104
    ) as (refresh, state, _lane, reader, delivered):
        # The caller's matching scan fence represents exhaustion through 105.
        progress = refresh.synchronize()
        assert state.snapshot().cursor == 104
        refresh._scan_obligations(progress, ('range-a', 106, 108))
        assert state.pending_ranges() == ()
        assert reader.calls == ['retention', ((106, 108),), 'retention']
        assert delivered == [106, 107, 108]


def test_eager_closeout_with_every_mark_makes_no_provider_call(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, done=tuple(range(101, 111))) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        refresh.service()
        assert state.pending_ranges() == ()
        assert reader.calls == []
        assert delivered == []


def test_eager_failing_gap_does_not_stop_second_due_gap(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, cursor=120) as (refresh, state, _lane, reader, _delivered):
        connection = state._database.connection
        insert_range(connection, 'range-b', 111, 115, state='closed')
        bind_group(connection, 'range-b', 'serial', 1, floor=100, eligibility_point=0)
        insert_source_late_ids(connection, 'range-b', (111,))
        reader.omitted = {101, 111}
        refresh.service()
        reader.failure = 'transport'
        _clock(state, 1)
        refresh.service(force=True)
        assert reader.calls[-1] == ((101, 101),)
        _clock(state, 2)
        refresh.service(force=True)
        assert reader.calls[-2] == ((111, 111),)
        assert len(state.pending_ranges()) == 2


def test_eager_gives_up_after_grace_with_exact_excused_set(
    tmp_path: Path,
    closeout_logs: pytest.LogCaptureFixture,
) -> None:
    with _closeout_fixture(tmp_path) as (refresh, state, _lane, reader, delivered):
        reader.omitted = {101, 103}
        refresh.service()
        reader.omitted = {103}
        _clock(state, 300)
        refresh.service()
        assert state.pending_ranges() == ()
        assert sorted(delivered) == [101, 102, *range(104, 111)]
        assert 'IDs 101-110: 103; replay the group from 103' in closeout_logs.messages[-1]


def test_eager_never_gives_up_queued_row(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, joined=True, clock=300) as (
        refresh,
        state,
        lane,
        reader,
        delivered,
    ):
        channel = GroupChannel(
            group='serial', ordering='eager', max_events=20, max_bytes=1_000_000
        )
        lane.join(Coordinator(boundary=110), channel, catch_up=lambda _boundary: None)
        reader.omitted = {101}
        refresh.service()
        offer_event(channel, make_event(101))
        _clock(state, 600)
        refresh.service()
        assert state.pending_ranges()
        assert 101 not in delivered
        lane.drain_live(channel)
        refresh.service()
        assert state.pending_ranges() == ()
        assert sorted(delivered) == list(range(101, 111))


def test_eager_omission_stays_pending_warns_once_and_completes_when_returned(
    tmp_path: Path,
    closeout_logs: pytest.LogCaptureFixture,
) -> None:
    with _closeout_fixture(tmp_path) as (refresh, state, _lane, reader, delivered):
        reader.omitted = {102}
        refresh.service()
        assert state.pending_ranges()
        _clock(state, 1)
        refresh.service(force=True)
        assert len(closeout_logs.records) == 1
        assert 'retrying' in closeout_logs.messages[0]
        reader.omitted.clear()
        _clock(state, 3)
        refresh.service()
        assert state.pending_ranges() == ()
        assert sorted(delivered) == list(range(101, 111))


def test_eager_restart_before_completion_delivers_returned_row_once(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path) as (refresh, state, _lane, reader, delivered):
        reader.omitted = {101}
        refresh.service()
        restarted = _restart_refresh(refresh)
        reader.omitted.clear()
        restarted.service()
        assert state.pending_ranges() == ()
        assert sorted(delivered) == list(range(101, 111))


def test_generation_change_drops_retry_state(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path) as (refresh, state, _lane, reader, _delivered):
        reader.omitted = {101}
        refresh.service()
        connection = state._database.connection
        connection.execute('UPDATE groups SET group_generation=2')
        connection.execute('UPDATE range_groups SET group_generation=2')
        _clock(state, 299)
        refresh.service(force=True)
        assert not refresh._retries.retrying((1, 'range-a'))
        _clock(state, 300)
        refresh.service()
        assert state.pending_ranges()
        _clock(state, 599)
        refresh.service()
        assert state.pending_ranges() == ()


def test_grace_does_not_elapse_while_source_clock_is_frozen(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path) as (refresh, state, _lane, reader, _delivered):
        reader.omitted = {101}
        refresh.service()
        _clock(state, 1)
        refresh.service()
        calls = list(reader.calls)
        for _ in range(10):
            refresh.service(force=True)
        assert state.pending_ranges()
        assert reader.calls == calls


def test_hints_after_initial_retention_failure_make_no_extra_fetch(
    tmp_path: Path,
    closeout_logs: pytest.LogCaptureFixture,
) -> None:
    with _closeout_fixture(tmp_path) as (refresh, state, _lane, reader, _delivered):
        reader.failure = 'retention'
        reader.omitted = {101}
        refresh.service()
        assert not closeout_logs.records
        for clock in (0, 0.5, 0.99):
            _clock(state, clock)
            refresh.service(force=True)
        assert reader.calls == ['failed retention']
        _clock(state, 100)
        refresh.service(force=True)
        assert 'retrying' in closeout_logs.messages[0]
        _clock(state, 399)
        refresh.service(force=True)
        assert state.pending_ranges()
        _clock(state, 400)
        refresh.service(force=True)
        assert state.pending_ranges() == ()


def test_hints_between_retries_make_no_extra_fetch(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path) as (refresh, state, _lane, reader, _delivered):
        reader.omitted = {101}
        refresh.service()
        calls = list(reader.calls)
        for clock in (0, 0.25, 0.99):
            _clock(state, clock)
            refresh.service(force=True)
        assert reader.calls == calls
        _clock(state, 1)
        refresh.service(force=True)
        assert len(reader.calls) == len(calls) + 3


def test_joined_eager_closeout_live_rows_complete_without_provider_call(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, joined=True) as (refresh, state, lane, reader, delivered):
        channel = GroupChannel(
            group='serial', ordering='eager', max_events=20, max_bytes=1_000_000
        )
        lane.join(Coordinator(boundary=110), channel, catch_up=lambda _boundary: None)
        for event_id in range(101, 111):
            offer_event(channel, make_event(event_id))
        refresh.service()
        assert state.pending_ranges()
        assert reader.calls == []
        assert channel.status.queued_count == 10
        lane.drain_live(channel)
        refresh.service()
        assert state.pending_ranges() == ()
        assert reader.calls == []
        assert delivered == list(range(101, 111))


def test_joined_eager_closeout_waits_for_deadline_then_fetches_only_missing(
    tmp_path: Path,
) -> None:
    with _closeout_fixture(tmp_path, joined=True, done=(101, 103, 105, 107, 109)) as (
        refresh,
        state,
        lane,
        reader,
        delivered,
    ):
        refresh.service()
        assert state.pending_ranges()
        assert reader.calls == []
        pending = state.pending_ranges()[0]
        _snapshot, consumed = lane.deliver_obligation_rows(
            pending, (make_event(102), make_event(104)), source_successful_clock=0
        )
        assert consumed
        state._database.connection.execute('UPDATE source_meta SET successful_clock=300')
        refresh.service()
        assert state.pending_ranges() == ()
        assert reader.calls == ['retention', ((106, 106), (108, 108), (110, 110)), 'retention']
        assert delivered == [102, 104, 106, 108, 110]


def test_late_final_close_gets_full_window_and_restart_restarts_grace(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, clock=1000) as (refresh, state, _lane, reader, _delivered):
        reader.omitted = {101}
        refresh.service()
        _clock(state, 1299)
        refresh.service()
        assert state.pending_ranges()
        restarted = _restart_refresh(refresh)
        restarted.service()
        _clock(state, 1300)
        restarted.service()
        assert state.pending_ranges()
        _clock(state, 1599)
        restarted.service()
        assert state.pending_ranges() == ()


def test_restart_with_new_window_restarts_grace(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path) as (refresh, state, _lane, reader, _delivered):
        reader.omitted = {101}
        refresh.service()
        _clock(state, 299)
        restarted = _restart_refresh(refresh, 20)
        restarted.service()
        _clock(state, 318)
        restarted.service()
        assert state.pending_ranges()
        _clock(state, 319)
        restarted.service()
        assert state.pending_ranges() == ()


def test_retry_selection_refreshes_after_strict_fence_completion(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, ordering='strict', clock=30, high=102) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        connection = state._database.connection
        insert_range(connection, 'range-b', 103, 104, state='closed')
        bind_group(connection, 'range-b', 'serial', 1, floor=100, eligibility_point=30)
        insert_source_late_ids(connection, 'range-b', (103, 104))
        refresh._retries.failed((1, 'range-b'), 29)
        refresh.service()
        assert delivered == [101, 102, 103, 104]
        assert state.pending_ranges() == ()
        assert reader.calls == [
            'retention',
            ((101, 102),),
            'retention',
            'retention',
            ((103, 104),),
            'retention',
        ]


def test_strict_closeout_fetches_only_missing_ids(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, ordering='strict', clock=30, done=(101, 102)) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        refresh.service()
        assert state.pending_ranges() == ()
        assert reader.calls == ['retention', ((103, 110),), 'retention']
        assert delivered == list(range(103, 111))


def test_strict_closeout_not_eligible_makes_no_provider_call(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, ordering='strict', clock=29) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        refresh.service()
        assert state.pending_ranges()
        assert reader.calls == []
        assert delivered == []


def test_strict_closeout_waits_for_open_lower_range(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, ordering='strict', clock=30, low=106) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        connection = state._database.connection
        insert_range(connection, 'range-lower', 101, 105)
        bind_group(connection, 'range-lower', 'serial', 1, floor=100, eligibility_point=30)
        refresh.service()
        assert reader.calls == []
        assert state.snapshot().status == 'running'
        assert len(state.pending_ranges()) == 2
        insert_source_late_ids(connection, 'range-lower', range(101, 106))
        close_test_range(connection, 'range-lower')
        refresh.service(force=True)
        assert delivered == list(range(101, 111))
        assert state.pending_ranges() == ()


def test_strict_closeout_waits_for_queued_lower_row(tmp_path: Path) -> None:
    with _closeout_fixture(tmp_path, ordering='strict', clock=30, joined=True, low=102) as (
        refresh,
        state,
        lane,
        reader,
        delivered,
    ):
        channel = GroupChannel(
            group='serial', ordering='strict', max_events=20, max_bytes=1_000_000
        )
        lane.join(Coordinator(boundary=100), channel, catch_up=lambda _boundary: None)
        offer_event(channel, make_event(101))
        refresh.service()
        assert reader.calls == []
        assert state.pending_ranges()
        lane.drain_live(channel)
        refresh.service()
        assert delivered == list(range(101, 111))
        assert state.snapshot().status == 'running'
        assert state.pending_ranges() == ()


def test_strict_floor_progress_preserves_closeout_and_queued_suffix(tmp_path: Path) -> None:
    with _closeout_fixture(
        tmp_path,
        ordering='strict',
        clock=30,
        low=106,
        high=112,
        cursor=100,
        floor=110,
        joined=True,
    ) as (refresh, state, lane, reader, delivered):
        channel = GroupChannel(
            group='serial', ordering='strict', max_events=20, max_bytes=1_000_000
        )
        lane.join(Coordinator(boundary=100), channel, catch_up=lambda _boundary: None)
        for event_id in (*range(101, 106), 113):
            offer_event(channel, make_event(event_id))
        lane.drain_live(channel)
        assert state.snapshot().cursor == 100
        assert delivered == []
        refresh.service()
        lane.drain_live(channel)
        assert reader.calls == ['retention', ((111, 112),), 'retention']
        assert delivered == [111, 112, 113]
        assert state.snapshot().cursor == 113


@pytest.mark.parametrize('explicit_fence', [False, True])
def test_strict_scan_rechecks_fence_after_waiting_for_slot(
    tmp_path: Path, explicit_fence: bool
) -> None:
    with _closeout_fixture(tmp_path, ordering='strict', clock=30) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):

        @contextmanager
        def competing_completion() -> Generator[None]:
            state.complete_obligation_scan('range-a', excused=((101, 110),))
            yield

        refresh._slot = competing_completion
        progress = refresh.synchronize()
        fence = ('range-a', 101, 110) if explicit_fence else None
        refresh._scan_obligations(progress, fence)
        assert reader.calls == []
        assert delivered == []
        assert state.pending_ranges() == ()


@pytest.mark.parametrize('failure', ['transport', 'interrupted', 'retention', 'closing-retention'])
def test_failed_reads_neither_start_nor_end_grace(
    tmp_path: Path,
    failure: str,
    closeout_logs: pytest.LogCaptureFixture,
) -> None:
    with _closeout_fixture(tmp_path) as (refresh, state, _lane, reader, _delivered):
        reader.omitted = {101}
        reader.failure = failure
        refresh.service()
        assert not closeout_logs.records
        _clock(state, 100)
        refresh.service()
        assert len(closeout_logs.records) == 1
        _clock(state, 400)
        reader.failure = failure
        refresh.service()
        assert state.pending_ranges()
        assert len(closeout_logs.records) == 1
        _clock(state, 401)
        refresh.service()
        assert state.pending_ranges() == ()
        assert 'gave up on 1 events' in closeout_logs.messages[-1]


@pytest.mark.parametrize('clock', [0.0, 1.0])
def test_idle_retry_selection_reads_one_snapshot_per_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    clock: float,
) -> None:
    with _closeout_fixture(tmp_path, cursor=1200) as (refresh, state, _lane, reader, _delivered):
        pending = state.pending_ranges()[0]
        count = 100
        refresh._pending = tuple(
            replace(pending, obligation_id=f'gap-{index}', low_id=101 + index, high_id=101 + index)
            for index in range(count)
        )
        for revision in refresh._pending:
            refresh._retries.missed((1, revision.obligation_id), 0)
        if clock:
            # All retries are due, but another pass already used this interval.
            refresh._retries.take_turn(clock)
        snapshot_reads = 0
        original_snapshot = state.snapshot

        def counted_snapshot() -> GroupSnapshot:
            nonlocal snapshot_reads
            snapshot_reads += 1
            return original_snapshot()

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)

        def missing(_obligation_id: str) -> tuple[IdRun, ...]:
            return ((101, 101),)

        monkeypatch.setattr(state, 'missing_late_runs', missing)
        progress = replace(state.refresh_ranges()[0], source_successful_clock=clock)
        refresh._scan_obligations(progress)
        assert reader.calls == []
        assert snapshot_reads == 1
        print(f'pending={count} clock={clock} snapshot_reads={snapshot_reads}')


class _StrictReader(_MissingReader):
    def __init__(self) -> None:
        super().__init__()
        self.requests: list[tuple[IdRun, ...]] = []
        self.retention_calls = 0
        self.fail_retention_at: int | None = None
        self.fail_page_at: int | None = None

    def fetch_id_runs(self, runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
        remaining = tuple(runs)
        while remaining:
            requested = remaining[:100]
            self.requests.append(requested)
            self.calls.append(requested)
            if len(self.requests) == self.fail_page_at:
                raise ConnectionError('later page unavailable')
            ids = [
                event_id
                for low, high in requested
                for event_id in range(low, high + 1)
                if event_id not in self.omitted
            ][:2]
            covered = ids[-1] if len(ids) == 2 else requested[-1][1]
            self.pages += 1
            yield _IdRunPage(tuple(make_event(event_id) for event_id in ids), covered)
            remaining = tuple(
                (max(low, covered + 1), high) for low, high in remaining if high > covered
            )

    def retention_floor(self) -> int:
        self.retention_calls += 1
        self.calls.append('retention')
        if self.retention_calls == self.fail_retention_at:
            raise ConnectionError('confirmation unavailable')
        return 1


@contextmanager
def _strict_closeout_fixture(
    tmp_path: Path, **options: Unpack[_CloseoutOptions]
) -> Generator[tuple[_LaneRefresh, GroupState, GroupLane, _StrictReader, list[int]]]:
    options.setdefault('ordering', 'strict')
    options.setdefault('clock', 30)
    with _closeout_fixture(tmp_path, **options) as (refresh, state, lane, _reader, delivered):
        reader = _StrictReader()
        refresh._reader = cast('CatchupReader', reader)
        yield refresh, state, lane, reader, delivered


def test_strict_confirmed_page_is_delivered_without_refetch(tmp_path: Path) -> None:
    observed: list[tuple[int, int]] = []
    reader: _StrictReader

    def callback(context: EventContext) -> None:
        observed.append((cast('int', context.event['id']), reader.retention_calls))

    with _strict_closeout_fixture(tmp_path, high=105, callback=callback) as (
        refresh,
        state,
        _lane,
        reader,
        _delivered,
    ):
        reader.omitted = {101}
        refresh.service()
        assert observed == []
        _clock(state, 330)
        reader.requests.clear()
        refresh.service()
        assert observed == [(102, 4), (103, 4), (104, 4), (105, 4)]
        assert reader.requests == [((101, 105),), ((104, 105),)]
        assert state.pending_ranges() == ()


def test_strict_failed_confirmation_starts_no_callback_above_omission(
    tmp_path: Path, closeout_logs: pytest.LogCaptureFixture
) -> None:
    with _strict_closeout_fixture(tmp_path, high=103) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        reader.omitted = {102}
        refresh.service()
        _clock(state, 330)
        reader.fail_retention_at = reader.retention_calls + 2
        refresh.service()
        assert delivered == [101]
        assert state.pending_ranges()
        assert not any('gave up on' in message for message in closeout_logs.messages)
        assert reader.requests == [((101, 103),), ((102, 103),)]


def test_strict_gives_up_id_read_earlier_and_id_first_requested_after_grace(
    tmp_path: Path, closeout_logs: pytest.LogCaptureFixture
) -> None:
    with _strict_closeout_fixture(tmp_path, high=303, done=tuple(range(102, 303, 2))) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        reader.omitted = {101}
        refresh.service()
        assert reader.requests == [tuple((event_id, event_id) for event_id in range(101, 300, 2))]
        assert delivered == []
        # 103 was returned but discarded; 301 exceeded the first request's 100 runs.
        reader.omitted = {101, 103, 301}
        _clock(state, 330)
        refresh.service()
        assert delivered == [*range(105, 300, 2), 303]
        assert state.pending_ranges() == ()
        warnings = [message for message in closeout_logs.messages if 'gave up on' in message]
        assert len(warnings) == 2
        assert ': 101, 103; replay' in warnings[0]
        assert ': 301; replay' in warnings[1]


@pytest.mark.parametrize(('omitted', 'prefix'), [(101, []), (102, [101])])
def test_strict_omitted_first_or_middle_id_delivers_only_prefix(
    tmp_path: Path, omitted: int, prefix: list[int]
) -> None:
    with _strict_closeout_fixture(tmp_path) as (refresh, state, _lane, reader, delivered):
        reader.omitted = {omitted}
        refresh.service()
        assert delivered == prefix
        assert reader.requests == [((101, 110),)]
        assert reader.retention_calls == 2
        assert state.pending_ranges()
        assert state.snapshot().status == 'running'


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('failure', ['transport', 'confirmation'])
def test_strict_later_page_failure_keeps_earlier_deliveries(
    tmp_path: Path, restart: bool, failure: str
) -> None:
    with _strict_closeout_fixture(tmp_path, high=106) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        reader.omitted = {101, 104}
        refresh.service()
        _clock(state, 330)
        if failure == 'transport':
            reader.fail_page_at = len(reader.requests) + 2
        else:
            reader.fail_retention_at = reader.retention_calls + 3
        refresh.service()
        assert delivered == [102, 103]
        assert state.snapshot().cursor == 103
        assert state.pending_ranges()
        following = _restart_refresh(refresh) if restart else refresh
        reader.omitted.clear()
        reader.requests.clear()
        _clock(state, 331)
        following.service()
        assert reader.requests == [((104, 106),), ((106, 106),)]
        assert delivered == [102, 103, 104, 105, 106]
        assert state.pending_ranges() == ()
        assert state.snapshot().status == 'running'


def test_strict_prefix_interrupted_before_omission_ends_pass(tmp_path: Path) -> None:
    delivered: list[int] = []
    lane: GroupLane

    def callback(context: EventContext) -> None:
        delivered.append(cast('int', context.event['id']))
        lane.request_shutdown()

    with _strict_closeout_fixture(tmp_path, high=103, callback=callback) as (
        refresh,
        state,
        lane,
        reader,
        _delivered,
    ):
        reader.omitted = {102}
        refresh.service()
        assert delivered == [101]
        assert state.snapshot().status == 'running'
        assert state.pending_ranges()
        assert reader.retention_calls == 1
        assert not refresh._retries.grace_ended((1, 'range-a'), 1000)


@pytest.mark.parametrize('omitted', [{102, 103}, {101, 102, 103}])
def test_strict_short_and_empty_last_page_confirm_remaining_ids(
    tmp_path: Path, omitted: set[int], closeout_logs: pytest.LogCaptureFixture
) -> None:
    with _strict_closeout_fixture(tmp_path, high=103) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        reader.omitted = omitted
        refresh.service()
        _clock(state, 330)
        refresh.service()
        assert state.pending_ranges() == ()
        assert delivered == ([101] if 101 not in omitted else [])
        assert reader.retention_calls == 5
        assert len(closeout_logs.records) == 2
        assert f'gave up on {len(omitted)} events' in closeout_logs.messages[-1]


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('advance_during', ['fetch', 'confirmation'])
def test_closeout_grace_starts_when_miss_is_confirmed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    advance_during: str,
) -> None:
    with _closeout_fixture(tmp_path, ordering=ordering, clock=30) as (
        refresh,
        state,
        _lane,
        reader,
        _delivered,
    ):
        reader.omitted = {101}
        retention = reader.retention_floor
        fetch = reader.fetch_id_runs

        def advancing_retention() -> int:
            floor = retention()
            if len(reader.calls) > 1:
                _clock(state, 330)
            return floor

        def advancing_fetch(runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
            _clock(state, 330)
            yield from fetch(runs)

        with monkeypatch.context() as patch:
            if advance_during == 'confirmation':
                patch.setattr(reader, 'retention_floor', advancing_retention)
            else:
                patch.setattr(reader, 'fetch_id_runs', advancing_fetch)
            refresh.service()
        calls = list(reader.calls)
        refresh.service(force=True)
        assert reader.calls == calls, 'the first retry must wait after confirmation'
        _clock(state, 629)
        refresh.service()
        assert state.pending_ranges(), 'the confirmed miss receives a full window'
        _clock(state, 630)
        refresh.service()
        assert state.pending_ranges() == ()


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('failure', [False, True])
def test_closeout_retry_wait_starts_at_pass_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    failure: bool,
) -> None:
    with _closeout_fixture(tmp_path, ordering=ordering, clock=30) as (
        refresh,
        state,
        _lane,
        reader,
        _delivered,
    ):
        reader.omitted = {101}
        refresh.service()
        _clock(state, 31)
        fetch = reader.fetch_id_runs

        def slow_fetch(runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
            _clock(state, 40)
            yield from fetch(runs)

        reader.failure = 'transport' if failure else ''
        with monkeypatch.context() as patch:
            patch.setattr(reader, 'fetch_id_runs', slow_fetch)
            refresh.service()
        calls = list(reader.calls)
        refresh.service(force=True)
        assert reader.calls == calls
        _clock(state, 41 if failure else 42)
        refresh.service()
        assert reader.calls != calls


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('identity_field', ['source_generation', 'stream_incarnation'])
def test_closeout_confirmation_rejects_changed_source_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    identity_field: str,
) -> None:
    with _closeout_fixture(tmp_path, ordering=ordering, clock=30) as (
        refresh,
        state,
        _lane,
        reader,
        _delivered,
    ):
        reader.omitted = {101}
        retention = reader.retention_floor

        def changed_identity() -> int:
            floor = retention()
            if len(reader.calls) > 1:
                query = (
                    'UPDATE source_meta SET source_generation=?, successful_clock=330'
                    if identity_field == 'source_generation'
                    else 'UPDATE source_meta SET stream_incarnation=?, successful_clock=330'
                )
                state._database.connection.execute(
                    query,
                    ('replacement',),
                )
            return floor

        monkeypatch.setattr(reader, 'retention_floor', changed_identity)
        refresh.service()
        assert state.pending_ranges()
        assert not refresh._retries.retrying((1, 'range-a'))


def test_strict_two_skipping_pages_log_one_warning_each(
    tmp_path: Path, closeout_logs: pytest.LogCaptureFixture
) -> None:
    with _strict_closeout_fixture(tmp_path, high=106) as (
        refresh,
        state,
        _lane,
        reader,
        delivered,
    ):
        reader.omitted = {101, 104}
        refresh.service()
        _clock(state, 330)
        refresh.service()
        assert delivered == [102, 103, 105, 106]
        assert state.pending_ranges() == ()
        warnings = [message for message in closeout_logs.messages if 'gave up on' in message]
        assert len(warnings) == 2
        assert ': 101; replay' in warnings[0]
        assert ': 104; replay' in warnings[1]
        assert reader.retention_calls == 6
