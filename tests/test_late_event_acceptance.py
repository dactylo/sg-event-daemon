from __future__ import annotations

import json
import logging
import os
import resource
import sys
import time
from contextlib import ExitStack, closing, contextmanager, nullcontext
from dataclasses import replace
from functools import wraps
from types import SimpleNamespace
from typing import TYPE_CHECKING, Unpack, cast

import pytest

from sg_event_daemon import _source as source_module
from sg_event_daemon._checkpoint_host import CheckpointHost
from sg_event_daemon._config import CredentialConfig, SecretValue
from sg_event_daemon._events import Event
from sg_event_daemon._fanout import EventFanout, GroupChannel
from sg_event_daemon._group_state import GroupState
from sg_event_daemon._lane import Delivery, GroupLane
from sg_event_daemon._providers import Provider, ProviderCapabilities, _FindOptions
from sg_event_daemon._source import (
    CatchupReader,
    JoinCoordinator,
    SourcePoller,
    _SuccessfulQueryClock,
)
from sg_event_daemon._source_schema import SOURCE_SCHEMA, source_identity
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._strict import StrictBuffer
from sg_event_daemon._watchdog import ComponentWatchdog, internal_stall_deadline_secs
from sg_event_daemon._worker import _LaneRefresh
from tests._stall_policy import stall_deadline
from tests._storage_support import bind_group, insert_range, open_test_group, write_context_for
from tests.source._support import EventLogSG
from tests.source.test_source import Monotonic
from tests.test_worker import (
    RuntimeProviderHandle,
    SourceHealthWaiter,
    group_config,
    health_failure_plugin,
    runtime_event,
    runtime_site,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence
    from pathlib import Path

    from sg_event_daemon._gaps import IdRun
    from sg_event_daemon._group_types import GroupSnapshot
    from sg_event_daemon._plugins import EventContext
    from sg_event_daemon._watchdog import WatchdogPhase, _WatchIdentityOptions


class _AcceptanceProvider(RuntimeProviderHandle):
    def __init__(self) -> None:
        super().__init__(lambda: 100)
        self.visible: set[int] | None = set()
        self.omitted: set[int] = set()
        self.frontier = 100
        self.requests = 0

    def _candidate_runs(self, clauses: list[object]) -> list[tuple[int, int]]:
        low, high = 1, self.frontier
        candidates: list[tuple[int, int]] = []
        for clause in clauses:
            if isinstance(clause, dict):
                expression = cast('dict[str, object]', clause)
                assert expression['filter_operator'] == 'any'
                for run in cast('list[list[object]]', expression['filters']):
                    assert run[:2] == ['id', 'between']
                    candidates.append((cast('int', run[2]), cast('int', run[3])))
            else:
                field, operator, *bounds = cast('list[object]', clause)
                assert field == 'id'
                value = cast('int', bounds[0])
                if operator == 'greater_than':
                    low = max(low, value + 1)
                elif operator == 'less_than':
                    high = min(high, value - 1)
                elif operator == 'between':
                    low, high = max(low, value), min(high, cast('int', bounds[1]))
                else:
                    assert operator == 'is'
                    low, high = max(low, value), min(high, value)
        return [(max(start, low), min(end, high)) for start, end in candidates or [(low, high)]]

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        del fields
        assert entity_type == 'EventLogEntry'
        self.requests += 1
        if not filters:
            return [{'id': 1}]
        clauses = cast('list[object]', filters)
        candidates = self._candidate_runs(clauses)
        rows: list[dict[str, object]] = []
        for start, end in candidates:
            for event_id in range(start, end + 1):
                if event_id in self.omitted or (
                    self.visible is not None and event_id not in self.visible
                ):
                    continue
                assert all(EventLogSG.matches(event_id, item) for item in clauses)
                rows.append(runtime_event(event_id))
                if len(rows) == options.get('limit', 0):
                    return rows
        return rows


def _assert_metadata_projections(source: SourceState, states: list[GroupState]) -> None:
    connection = source._database.connection
    source.invalidate_strict_ranges()
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    for state in states:
        state._database.connection.set_trace_callback(statements.append)
    try:
        snapshot = source.snapshot()
        ranges = source.strict_ranges_if_changed()
        assert ranges is not None
        strict = StrictBuffer(max_events=100, max_bytes=10_000_000)
        strict.synchronize_ranges(ranges)
        for state in states:
            _progress, pending = state.refresh_ranges()
            assert pending
    finally:
        connection.set_trace_callback(None)
        for state in states:
            state._database.connection.set_trace_callback(None)
    assert sum(item.late_id_count for item in snapshot.ranges) == 500_000
    assert len(snapshot.ranges) == 1_501
    assert len(states) == 20
    assert sum(state.snapshot().ordering == 'strict' for state in states) == 10
    assert not any(
        'event_id' in sql.lower() and 'source_late_ids' in sql.lower() for sql in statements
    )
    assert connection.execute('SELECT COUNT(*) FROM source_late_ids').fetchone() == (500_000,)


def _assert_retry_window(
    source: SourceState,
    states: list[GroupState],
    active: list[_LaneRefresh],
    closeout_pass: Callable[[_LaneRefresh], int],
) -> None:
    connection = source._database.connection
    for index, refresh in enumerate(active):
        assert closeout_pass(refresh) > 0
        assert len(refresh._retries._records) == (13 if index == 0 else 12)
    marks = connection.execute('SELECT COUNT(*) FROM late_ids').fetchone()[0]
    assert marks > 100_000
    for refresh in active:
        assert closeout_pass(refresh) == 0
    assert connection.execute('SELECT COUNT(*) FROM late_ids').fetchone()[0] == marks

    # At each interval a group can retry only one gap, with two retention
    # checks and at most two pages (the wide gap has 101 fragmented misses).
    for clock in range(2, 45):
        source.record_successful_query(float(clock))
        for refresh in active:
            requests = closeout_pass(refresh)
            assert requests <= 4
            assert closeout_pass(refresh) == 0
        if clock < 31:
            assert connection.execute('SELECT COUNT(*) FROM late_ids').fetchone()[0] == marks
    for state in states:
        assert state.pending_ranges() == ()
    assert connection.execute(
        "SELECT COUNT(*) FROM late_ids WHERE group_name BETWEEN 'group-10' AND 'group-12'"
    ).fetchone() == (0,)
    assert all(not refresh._retries._records for refresh in active)


def _provider(handle: _AcceptanceProvider) -> Provider:
    def factory(
        site: str, *, script_name: str, api_key: str, connect: bool
    ) -> _AcceptanceProvider:
        assert (site, script_name, api_key, connect) == (
            'https://example.test',
            'daemon',
            'key',
            False,
        )
        return handle

    return Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        sync_factory=factory,
        async_factory=None,
    )


def _retention_checkpoints(stack: ExitStack, path: Path) -> list[str]:
    checkpoint_errors: list[str] = []
    checkpoints = CheckpointHost(
        interval_secs=1.0,
        synchronous='NORMAL',
        on_failure=checkpoint_errors.append,
        logger=logging.getLogger('sg_event_daemon.source'),
    )
    stack.callback(checkpoints.close)
    checkpoints.register(path, source_identity('main', 'events'), SOURCE_SCHEMA, log_extra={})
    return checkpoint_errors


def _retention_memory_measurements(internal_operations: dict[str, float]) -> dict[str, object]:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak = int(peak if sys.platform == 'darwin' else peak * 1024)
    longest = max(internal_operations.values())
    deadline = internal_stall_deadline_secs(
        2.0,
        CredentialConfig(name='default', script_name='offline', api_key=SecretValue('offline')),
    )
    deadline = stall_deadline('coordination', deadline)
    # Durations are diagnostics; runner speed cannot determine correctness.
    return {
        'peak_resident_memory_bytes': peak,
        'longest_internal_operation_seconds': longest,
        'internal_operation_maxima_seconds': internal_operations,
        'internal_stall_deadline_seconds': deadline,
    }


def _sample_before_completion(
    lane: GroupLane, sample: Callable[[], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    complete = lane.complete_obligation

    def sampled_complete(obligation_id: str, *, excused: Sequence[IdRun] = ()) -> GroupSnapshot:
        sample()
        return complete(obligation_id, excused=excused)

    monkeypatch.setattr(lane, 'complete_obligation', sampled_complete)


def _seed_retention(
    source: SourceState, states: list[GroupState]
) -> tuple[list[tuple[str, int, int]], int]:
    connection = source._database.connection
    bounds: list[tuple[str, int, int]] = []
    low = 101
    # A 100k gap plus 1499 smaller gaps totals exactly 500k retained IDs.
    with source._writes.transaction(connection):
        for index in range(1_500):
            width = 100_000 if index == 0 else 400_000 // 1_499 + (index <= 400_000 % 1_499)
            high = low + width - 1
            key = f'{index:032x}'
            bounds.append((key, low, high))
            insert_range(connection, key, low, high, state='closed')
            connection.executemany(
                'INSERT INTO source_late_ids VALUES (?, ?)',
                ((key, event_id) for event_id in range(low, high + 1)),
            )
            for state in states[:10]:
                bind_group(
                    connection, key, state.snapshot().group, 7, floor=100, eligibility_point=1
                )
            # Every eager group retains fragmented gaps; three exercise close-out misses.
            for state in states[10:] if 1 <= index <= 12 else states[10:11] if index == 0 else []:
                bind_group(
                    connection, key, state.snapshot().group, 7, floor=100, eligibility_point=0
                )
            low = high + 2
        insert_range(connection, 'catchup-before-source', low, low + 9)
        bind_group(
            connection, 'catchup-before-source', 'group-13', 7, floor=100, eligibility_point=0
        )
        connection.execute('UPDATE source_meta SET frontier=?, successful_clock=1', (low + 10,))
        connection.execute(
            "UPDATE group_state SET cursor=? WHERE group_name >= 'group-10' "
            "AND group_name != 'group-13'",
            (low + 10,),
        )
    return bounds, low


def _source(path: Path, *, limit: int = 500_000) -> SourceState:
    source = SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site='main',
        source='events',
        stream_fingerprint='stream',
        source_generation='source-1',
        late_event_window_secs=30,
        max_source_late_ids=limit,
        max_group_obligations=2_000,
        max_pending_group_obligations=40_000,
    )
    source.initialize(100, incarnation='incarnation-1')
    return source


def _state(stack: ExitStack, path: Path, name: str, *, strict: bool = False) -> GroupState:
    return stack.enter_context(
        open_test_group(
            path,
            site='main',
            site_server_url='https://example.test',
            group=name,
            group_generation=7,
            ordering='strict' if strict else 'eager',
            initial_position=100,
            stream_fingerprint='stream',
            source_generation='source-1',
            strict_holdback_secs=1.0,
            max_group_obligations=2_000,
            max_late_ids_per_group=500_000,
        )
    )


def _lane(
    stack: ExitStack,
    state: GroupState,
    provider: Provider,
    *,
    delivered: list[int] | None = None,
) -> tuple[GroupLane, _LaneRefresh]:
    snapshot = state.snapshot()
    group = replace(
        group_config(snapshot.group, worker='default', ordering=snapshot.ordering),
        strict_holdback_secs=1.0,
    )
    group = replace(
        group, callback_deadline_secs=stall_deadline('phase', group.callback_deadline_secs)
    )
    site = runtime_site(group)
    plugins = ()
    if delivered is not None:

        def callback(context: EventContext) -> None:
            delivered.append(cast('int', context.event['id']))

        plugin = health_failure_plugin('callback')
        plugins = (replace(plugin, callbacks=(replace(plugin.callbacks[0], callback=callback),)),)
    lane = stack.enter_context(
        closing(
            GroupLane(
                state=state,
                site=site,
                group=group,
                provider=provider,
                plugins=plugins,
                worker_instance='worker-1',
                health_interval_secs=1,
                watchdog=ComponentWatchdog(
                    kind='lane',
                    identity=snapshot.group,
                    worker_instance='worker-1',
                    registration_token=f'watchdog-{snapshot.group}',
                    startup_deadline_secs=stall_deadline('startup', 30),
                ),
            )
        )
    )
    assert lane.setup() is None
    reader = stack.enter_context(
        CatchupReader(
            provider=provider,
            site_url=site.server,
            credential=site.credentials['source'],
            batch_size=1_000,
            fetch_interval_secs=1,
            waiter=SourceHealthWaiter(),
        )
    )
    return lane, _LaneRefresh(
        lane=lane,
        state=state,
        reader=reader,
        slot=nullcontext,
        joined=lambda: False,
        fetch_interval_secs=1,
        late_event_window_secs=30,
        log_extra={},
    )


def _assert_unlisted_catchup_marks(
    stack: ExitStack, state: GroupState, provider: Provider, source: SourceState
) -> None:
    connection = source._database.connection
    retained = source.range_for('catchup-before-source')
    assert retained is not None
    low = retained.low_id
    catchup, _ = _lane(stack, state, provider)
    _, consumed = catchup.deliver_many_counted(
        Delivery(Event(runtime_event(event_id))) for event_id in range(low, low + 10)
    )
    assert consumed == 10
    assert connection.execute(
        "SELECT COUNT(*) FROM late_ids WHERE group_name='group-13'"
    ).fetchone() == (10,)
    retained = source.range_for('catchup-before-source')
    assert retained is not None
    assert retained.late_id_count == 0


def _poller(
    stack: ExitStack,
    source: SourceState,
    provider: Provider,
    wall: Monotonic,
    *,
    publication: tuple[StrictBuffer, EventFanout] | None = None,
) -> SourcePoller:
    site = runtime_site(group_config('local', worker='default'))
    return stack.enter_context(
        SourcePoller(
            state=source,
            fanout=EventFanout() if publication is None else publication[1],
            provider=provider,
            site_url=site.server,
            credential=site.credentials['source'],
            config=replace(site.sources['events'], batch_size=1_000, late_event_window_secs=30),
            max_ipc_frame_bytes=8_388_608,
            source_generation='source-1',
            strict_buffer=None if publication is None else publication[0],
            successful_clock=_SuccessfulQueryClock(
                clock=wall, initial_value=source.successful_clock()
            ),
            waiter=SourceHealthWaiter(),
            random_value=lambda: 0.5,
            internal_deadline_secs=stall_deadline('coordination', 30),
            wait_tolerance_secs=5,
            watchdog=ComponentWatchdog(
                kind='source',
                identity='events',
                worker_instance='worker-1',
                registration_token=os.urandom(16).hex(),
                startup_deadline_secs=stall_deadline('startup', 30),
            ),
        )
    )


def _track_internal_phases(monkeypatch: pytest.MonkeyPatch, maxima: dict[str, float]) -> None:
    transition = ComponentWatchdog.transition
    watch = ComponentWatchdog.watch
    started: dict[ComponentWatchdog, float] = {}

    def finish(owner: ComponentWatchdog) -> None:
        now = time.perf_counter()
        sample = owner.sample()
        previous = started.get(owner)
        if previous is not None and sample.phase == 'internal':
            key = f'{sample.kind}.internal-phase-segment'
            maxima[key] = max(maxima.get(key, 0.0), now - previous)
        started[owner] = now

    def measured_transition(
        owner: ComponentWatchdog,
        phase: WatchdogPhase,
        *,
        deadline_at: float | None,
        **identities: Unpack[_WatchIdentityOptions],
    ) -> None:
        finish(owner)
        transition(owner, phase, deadline_at=deadline_at, **identities)
        started[owner] = time.perf_counter()

    @contextmanager
    def measured_watch(
        owner: ComponentWatchdog,
        phase: WatchdogPhase,
        *,
        deadline_at: float | None,
        **identities: Unpack[_WatchIdentityOptions],
    ) -> Generator[None]:
        finish(owner)
        with watch(owner, phase, deadline_at=deadline_at, **identities):
            started[owner] = time.perf_counter()
            try:
                yield
            finally:
                finish(owner)
        started[owner] = time.perf_counter()

    monkeypatch.setattr(ComponentWatchdog, 'transition', measured_transition)
    monkeypatch.setattr(ComponentWatchdog, 'watch', measured_watch)


def _track_internal_operations(monkeypatch: pytest.MonkeyPatch) -> dict[str, float]:
    internal_operations: dict[str, float] = {}
    _track_internal_phases(monkeypatch, internal_operations)

    def observe(owner: type[object], name: str) -> None:
        original = getattr(owner, name)

        @wraps(original)
        def measured(*args: object, **kwargs: object) -> object:
            started = time.perf_counter()
            try:
                return original(*args, **kwargs)
            finally:
                key = f'{owner.__name__}.{name}'
                internal_operations[key] = max(
                    internal_operations.get(key, 0.0), time.perf_counter() - started
                )

        monkeypatch.setattr(owner, name, measured)

    for owner, name in (
        (SourceState, 'snapshot'),
        (SourceState, 'strict_ranges_if_changed'),
        (StrictBuffer, 'synchronize_ranges'),
        (GroupState, 'refresh_ranges'),
        (SourceState, 'record_missing_rows'),
        (SourceState, 'record_range_scan_page'),
        (GroupLane, 'deliver_obligation_rows'),
        (GroupLane, 'complete_obligation'),
    ):
        observe(owner, name)

    return internal_operations


def _exercise_full_retention(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / 'source.db'
    handle = _AcceptanceProvider()
    handle.visible = None
    provider = _provider(handle)
    source_durations: list[float] = []
    closeout_durations: list[float] = []
    wal_samples: list[int] = []
    internal_operations = _track_internal_operations(monkeypatch)

    def sample_wal() -> None:
        wal = path.with_name(f'{path.name}-wal')
        wal_samples.append(wal.stat().st_size if wal.exists() else 0)

    with ExitStack() as stack:
        source = stack.enter_context(_source(path))
        checkpoint_errors = _retention_checkpoints(stack, path)
        states = [
            _state(stack, path, f'group-{index:02}', strict=index < 10) for index in range(20)
        ]
        connection = source._database.connection
        bounds, low = _seed_retention(source, states)
        sample_wal()
        _assert_metadata_projections(source, states)

        _assert_unlisted_catchup_marks(stack, states[13], provider, source)
        sample_wal()

        # The source lists the catch-up rows in an actual polling/recheck cycle.
        wall = Monotonic()
        handle.frontier = low + 10
        poller = _poller(stack, source, provider, wall)
        started = time.perf_counter()
        poller.run(max_cycles=1)
        source_durations.append(time.perf_counter() - started)
        sample_wal()
        retained = source.range_for('catchup-before-source')
        assert retained is not None
        assert retained.late_id_count == 10
        assert connection.execute(
            "SELECT COUNT(*) FROM late_ids WHERE group_name='group-13'"
        ).fetchone() == (10,)

        active: list[_LaneRefresh] = []
        for state in states[10:13]:
            lane, refresh = _lane(stack, state, provider)
            _sample_before_completion(lane, sample_wal, monkeypatch)
            active.append(refresh)
        for _, start, end in bounds[:13]:
            handle.omitted.update(range(start + 1, end + 1, 997 if end - start > 10_000 else 7))

        def closeout_pass(refresh: _LaneRefresh) -> int:
            before = handle.requests
            started = time.perf_counter()
            refresh.service()
            closeout_durations.append(time.perf_counter() - started)
            sample_wal()
            return handle.requests - before

        _assert_retry_window(source, states[10:13], active, closeout_pass)
        assert not checkpoint_errors
        sample_wal()
        memory_measurements = _retention_memory_measurements(internal_operations)
        connection.execute('PRAGMA wal_checkpoint(PASSIVE)')
        print(
            '\nMEASUREMENTS '
            + json.dumps(
                {
                    **memory_measurements,
                    'database_bytes': path.stat().st_size,
                    'peak_sampled_wal_bytes': max(wal_samples),
                    'wal_samples': len(wal_samples),
                    'request_count': handle.requests,
                    'source_cycle_count': len(source_durations),
                    'closeout_cycle_count': len(closeout_durations),
                    'source_cycle_seconds': source_durations,
                    'closeout_cycle_seconds': closeout_durations,
                }
            )
        )


@pytest.mark.slow
def test_resources_at_full_retention(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _exercise_full_retention(tmp_path, monkeypatch)


def test_sustained_arrival_behind_unfilled_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle = _AcceptanceProvider()
    provider = _provider(handle)
    wall = Monotonic()
    monkeypatch.setattr(source_module, 'time', SimpleNamespace(monotonic=wall))
    delivered: list[int] = []
    with ExitStack() as stack:
        source = stack.enter_context(_source(tmp_path / 'source.db', limit=3))
        state = _state(stack, source._database.path, 'local', strict=True)
        lane, refresh = _lane(stack, state, provider, delivered=delivered)
        strict = StrictBuffer(max_events=100, max_bytes=10_000_000, group_holdbacks={'local': 1})
        fanout = EventFanout()
        channel = GroupChannel(
            group='local', ordering='strict', max_events=100, max_bytes=10_000_000
        )
        lane.join(
            JoinCoordinator(
                state=source,
                fanout=fanout,
                source_generation='source-1',
                strict_retention=strict,
            ),
            channel,
            catch_up=lambda _boundary: None,
        )
        poller = _poller(stack, source, provider, wall, publication=(strict, fanout))
        for event_id in (102, 104, 106, 108):
            assert handle.visible is not None
            handle.visible.add(event_id)
            handle.frontier = event_id
            wall.advance(1)
            poller.run(max_cycles=1)
            if event_id > 102:
                handle.visible.add(event_id - 1)
                wall.advance(1)
                poller.run(max_cycles=1)
            refresh.service()
            lane.drain_live(channel)
        retained = source.snapshot().ranges
        assert [
            (item.low_id, item.state == 'closed', item.late_id_count) for item in retained
        ] == [
            (101, False, 0),
            (103, True, 1),
            (105, True, 1),
            (107, True, 1),
        ]
        assert delivered == []
        handle.visible.add(110)
        handle.frontier = 110
        wall.advance(1)
        poller.run(max_cycles=1)
        assert source.snapshot().status == 'late-capacity-exhausted'
        assert poller._paused_since is not None
        assert sum(item.late_id_count for item in source.snapshot().ranges) == 3
        wall.advance(31)
        poller.run(max_cycles=1)
        for _ in range(5):
            refresh.service()
            lane.drain_live(channel)
        assert delivered == list(range(102, 109))
        handle.visible.add(109)
        for _ in range(3):
            wall.advance(1)
            poller.run(max_cycles=1)
            refresh.service()
            lane.drain_live(channel)
        assert source.snapshot().status == 'running'
        assert poller._paused_since is None
        assert state.pending_ranges() == ()
        assert delivered == list(range(102, 111))
        assert len(delivered) == len(set(delivered))
