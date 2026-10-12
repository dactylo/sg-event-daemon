from __future__ import annotations

import os
import select
import sqlite3
import subprocess
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import datetime
from math import isfinite, nextafter
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, TypedDict, Unpack, cast
from unittest.mock import Mock

import pytest

import sg_event_daemon._source_store as source_store_module
from sg_event_daemon._config import (
    CredentialConfig,
    GroupConfig,
    SecretValue,
    SiteConfig,
    SourceConfig,
)
from sg_event_daemon._events import EVENT_FIELDS, Event, encode_event
from sg_event_daemon._fanout import (
    EncodedEvent,
    EncoderHaltedError,
    EncoderWorker,
    EventDelivery,
    EventFanout,
    FanoutStatus,
    GroupChannel,
    GroupChannelInterface,
    StrictCapacityControl,
    StrictOrderingControl,
)
from sg_event_daemon._group_state import SourceProgress
from sg_event_daemon._group_types import (
    AuditIdentity,
    LiveJoinIdentity,
)
from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._ipc import EventClient, EventPeerGrant, EventServer, _JoinSource
from sg_event_daemon._lane import GroupLane
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
    SourceHaltedError,
    SourceHealth,
    SourcePoller,
    StrictCatchupResult,
    StrictDeliveryOutcome,
    _saturating_retry_delay,
    _source_halt_code,
    _StrictJoinRetention,
    _SuccessfulQueryClock,
    history_truncation,
)
from sg_event_daemon._source_state import (
    SourceCapacityError,
    SourceSnapshot,
    SourceState,
)
from sg_event_daemon._source_store import DiscoveryRange
from sg_event_daemon._strict import StrictBuffer
from sg_event_daemon._watchdog import ComponentWatchdog
from sg_event_daemon._write_lock import WriteLockStoppedError, WriteLockTimeoutError
from tests._storage_support import (
    bind_group,
    insert_range,
    open_test_group,
    orphan_group,
    write_context_for,
)
from tests.source._support import open_group as _open_group
from tests.source._support import open_source as _open_source_fixture
from tests.source.test_state import open_source_range, record_source_late_ids
from tests.test_strict import attach_strict_group

if TYPE_CHECKING:
    import socket
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from sg_event_daemon._group_state import GroupState


_SITE = 'studio'
_SITE_URL = 'https://studio.example.invalid'
_SOURCE = 'events'
_SOURCE_GENERATION = 'source-spawn-1'
_STREAM = stream_fingerprint(_SITE_URL, 'admin', 'event-reader')


class _FakeConfig:
    def __init__(self) -> None:
        self.max_rpc_attempts = 0
        self.rpc_attempt_interval = 0
        self.timeout_secs = 0.0


class _FakeRetention(_StrictJoinRetention):
    def __init__(
        self,
        retry: bool = False,
        response: JoinRetry | None = None,
    ) -> None:
        self._calls: list[tuple[str, object]] = []
        self._response = response
        self._retry = retry

    def abort(self, identity: LiveJoinIdentity) -> None:
        self._calls.append(('abort', identity))

    def acknowledge_control(
        self,
        control: StrictCapacityControl | StrictOrderingControl,
    ) -> None:
        self._calls.append(('acknowledge-control', control))

    def finalize(self, identity: LiveJoinIdentity) -> JoinFinalized:
        self._calls.append(('finalize', identity))
        return JoinFinalized(identity)

    def prepare(
        self,
        identity: LiveJoinIdentity,
        channel: GroupChannelInterface,
    ) -> Attached:
        self._calls.append(('prepare', (identity, channel)))
        return Attached(identity)

    def reserve(self, identity: LiveJoinIdentity) -> JoinTarget | JoinRetry:
        self._calls.append(('reserve', identity))
        if self._response is not None:
            return self._response
        if self._retry:
            attempt = JoinAttemptIdentity(
                site=identity.site,
                source=identity.source,
                group=identity.group,
                join_kind=identity.join_kind,
                group_generation=identity.group_generation,
                attempt_token=identity.attempt_token,
            )
            return JoinRetry(
                attempt=attempt,
                retry_after_secs=1.0,
                headroom_events=0,
                headroom_bytes=0,
            )
        return JoinTarget(identity)


class _Monotonic:
    def __init__(self) -> None:
        self._now = 0.0

    def __call__(self) -> float:
        return self._now

    def _advance(self, seconds: float) -> None:
        self._now += seconds


class _RandomSequence:
    def __init__(self, *values: float) -> None:
        self._values = iter(values)

    def __call__(self) -> float:
        return next(self._values)


class _RecordingFanout(EventFanout):
    def __init__(
        self,
        state: SourceState | None = None,
        *,
        encoded_ids: list[int] | None = None,
    ) -> None:
        super().__init__()
        self._encode_counts: list[int] = []
        self._encoded_ids = encoded_ids
        self._published: list[tuple[EncodedEvent, ...]] = []
        self._snapshots: list[SourceSnapshot] = []
        self._hint_calls = 0
        self._state = state

    def publish_events(self, events: Iterable[EncodedEvent]) -> tuple[FanoutStatus, ...]:
        if self._encoded_ids is not None:
            self._encode_counts.append(len(self._encoded_ids))
        forwarded_events = tuple(events)
        self._published.append(forwarded_events)
        if self._state is not None:
            self._snapshots.append(self._state.snapshot())
        return super().publish_events(forwarded_events)

    def publish_range_changed(self) -> None:
        self._hint_calls += 1
        super().publish_range_changed()


class _ScriptedSG:
    def __init__(self, responses: Sequence[list[dict[str, object]] | BaseException]) -> None:
        self._calls: list[
            tuple[
                str,
                object,
                list[str] | None,
                int,
                list[dict[str, str]] | None,
                int,
            ]
        ] = []
        self._closed = False
        self.config = _FakeConfig()
        self._responses = deque(responses)
        self._session_uuid: str | None = None

    def close(self) -> None:
        self._closed = True

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        limit = options.get('limit', 0)
        order = options.get('order')
        page = options.get('page', 0)
        self._calls.append((entity_type, filters, fields, limit, order, page))
        response = self._responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response

    def set_session_uuid(self, session_uuid: str | None) -> None:
        self._session_uuid = session_uuid


class _SourceConfigOptions(TypedDict, total=False):
    batch_size: int
    commit_batch_max_bytes: int
    max_event_bytes: int


class _SourceOpenOptions(TypedDict, total=False):
    commit_batch_max_bytes: int
    delivery_floors: Mapping[str, int] | None
    group_holdbacks: Mapping[str, float] | None
    groups: Mapping[str, int] | None
    max_ipc_frame_bytes: int
    max_open_ranges: int
    max_group_obligations: int
    max_range_query_pages: int
    max_source_late_ids: int
    token_factory: Callable[[], str] | None


class _SyncFactory:
    def __init__(self, handle: _ScriptedSG) -> None:
        self._calls: list[tuple[str, str, str, bool]] = []
        self._handle = handle

    def __call__(
        self,
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> object:
        self._calls.append((site, script_name, api_key, connect))
        return self._handle


class _Waiter:
    def __init__(
        self,
        stop_on_wait: int | None = None,
        *,
        clock: _Monotonic | None = None,
    ) -> None:
        self.clock = clock
        self._set_calls = 0
        self._stop_on_wait = stop_on_wait
        self._waits: list[float] = []

    def set(self) -> None:
        self._set_calls += 1

    def wait(self, seconds: float) -> bool:
        self._waits.append(seconds)
        if self.clock is not None:
            self.clock._advance(seconds)
        return self._stop_on_wait == len(self._waits)


class _PollerOptions(TypedDict, total=False):
    config: SourceConfig | None
    fanout: EventFanout | None
    random_value: Callable[[], float] | None
    successful_clock: _SuccessfulQueryClock | None
    strict_buffer: StrictBuffer | None
    waiter: _Waiter | None
    watchdog: ComponentWatchdog | None


def _accept_strict_event(
    state: GroupState,
    event: Event,
    *,
    continue_epoch: bool = True,
) -> StrictDeliveryOutcome:
    event_id = cast('int', event['id'])
    snapshot = state.snapshot()
    state.advance_irrelevant(event_id, expected_revision=snapshot.revision)
    return StrictDeliveryOutcome(
        accepted=True,
        continue_epoch=continue_epoch,
        consumed=1,
    )


def _attempt(
    token: str,
    *,
    group: str = 'editorial',
    join_kind: Literal['eager', 'strict'] = 'eager',
) -> JoinAttemptIdentity:
    return JoinAttemptIdentity(
        site=_SITE,
        source=_SOURCE,
        group=group,
        join_kind=join_kind,
        group_generation=7,
        attempt_token=token,
    )


def _channel(
    *,
    group: str = 'editorial',
    ordering: Literal['eager', 'strict'] = 'eager',
    max_events: int = 100,
) -> GroupChannel:
    return GroupChannel(
        group=group,
        ordering=ordering,
        max_events=max_events,
        max_bytes=67_108_864,
    )


def _credential() -> CredentialConfig:
    return CredentialConfig(
        name='admin',
        script_name='event-reader',
        api_key=SecretValue('secret'),
        timeout_secs=17.0,
        max_rpc_attempts=4,
        rpc_attempt_interval_secs=2.5,
    )


def _deliver_strict_events(
    deliver: Callable[[Event, float], StrictDeliveryOutcome],
) -> Callable[[Sequence[Event], float], StrictDeliveryOutcome]:
    def deliver_page(events: Sequence[Event], ordering_time: float) -> StrictDeliveryOutcome:
        consumed = 0
        continue_epoch = True
        for event in events:
            outcome = deliver(event, ordering_time)
            if outcome.accepted:
                consumed += 1
            continue_epoch = outcome.continue_epoch
            if not outcome.accepted or not continue_epoch:
                break
        return StrictDeliveryOutcome(
            accepted=consumed == len(events),
            continue_epoch=continue_epoch,
            consumed=consumed,
        )

    return deliver_page


def _event(event_id: int, **changes: object) -> dict[str, object]:
    event: dict[str, object] = {
        'id': event_id,
        'event_type': 'Shotgun_Task_Change',
        'attribute_name': 'sg_status_list',
        'meta': {},
        'entity': None,
        'user': None,
        'project': None,
        'session_uuid': None,
        'created_at': datetime.fromisoformat('2026-07-25T01:02:03'),
        'type': 'EventLogEntry',
    }
    event.update(changes)
    return event


def _open_source(
    path: Path,
    **options: Unpack[_SourceOpenOptions],
) -> SourceState:
    groups = options.get('groups')
    return _open_source_fixture(
        path,
        groups={'editorial': 7} if groups is None else groups,
        delivery_floors=options.get('delivery_floors'),
        group_holdbacks=options.get('group_holdbacks'),
        visibility_overlap_secs=10.0,
        max_open_ranges=options.get('max_open_ranges', 100),
        max_group_obligations=options.get('max_group_obligations', 1_000),
        max_range_query_pages=options.get('max_range_query_pages', 2),
        max_pending_group_obligations=100,
        max_source_late_ids=options.get('max_source_late_ids', 100),
        max_ipc_frame_bytes=options.get('max_ipc_frame_bytes', 8_388_608),
        commit_batch_max_bytes=options.get(
            'commit_batch_max_bytes',
            67_108_864,
        ),
        token_factory=options.get('token_factory'),
    )


def _provider(
    *responses: list[dict[str, object]] | BaseException,
) -> tuple[Provider, _ScriptedSG, _SyncFactory]:
    handle = _ScriptedSG(responses)
    factory = _SyncFactory(handle)
    provider = Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        _sync_factory=factory,
        _async_factory=None,
    )
    return provider, handle, factory


def _read_child_stdout(process: subprocess.Popen[str]) -> str:
    stdout = process.stdout
    assert stdout is not None
    deadline = time.monotonic() + 2.0
    readable, _writable, _exceptional = select.select(
        (stdout,),
        (),
        (),
        max(0.0, deadline - time.monotonic()),
    )
    assert readable, 'child readiness timed out'
    return os.read(stdout.fileno(), 4_096).decode().strip()


def _revision_id(label: str) -> str:
    return label


def _seed_delivering_epoch(state: GroupState) -> None:
    snapshot = state.capture_epoch(
        low_id=101,
        high_id=103,
        captured_at=0.0,
        eligibility_point=1.0,
        reader_clock_checkpoint=0.0,
        source_generation=_SOURCE_GENERATION,
        expected_revision=state.snapshot().revision,
    )
    state.mark_epoch_eligible(
        snapshot.epochs[0].epoch_id,
        reader_clock_checkpoint=1.0,
        expected_revision=snapshot.revision,
    )


def _source_config(**options: Unpack[_SourceConfigOptions]) -> SourceConfig:
    return SourceConfig(
        name=_SOURCE,
        credentials='admin',
        fetch_interval_secs=1.0,
        batch_size=options.get('batch_size', 2),
        visibility_overlap_secs=10.0,
        poll_retry_base_secs=1.0,
        poll_retry_max_secs=3.0,
        poll_retry_multiplier=2.0,
        poll_retry_jitter_fraction=0.2,
        max_event_bytes=options.get('max_event_bytes', 4_194_304),
        commit_batch_max_bytes=options.get('commit_batch_max_bytes', 67_108_864),
        queue_max_events=10,
        queue_max_bytes=67_108_864,
        strict_max_events=20_000,
        strict_max_bytes=268_435_456,
        max_open_ranges=100,
        max_range_query_pages=2,
        max_pending_group_obligations=100,
        max_source_late_ids=100,
        max_late_ids_per_group=100,
    )


def _source_progress(
    frontier: int,
    *,
    source_clock: float = 0.0,
) -> SourceProgress:
    return SourceProgress(
        frontier=frontier,
        source_generation=_SOURCE_GENERATION,
        stream_incarnation='incarnation-1',
        source_successful_clock=source_clock,
        range_revision=0,
    )


def _reader(
    provider: Provider,
    *,
    successful_clock: _SuccessfulQueryClock | None = None,
    waiter: _Waiter | None = None,
) -> CatchupReader:
    return CatchupReader(
        provider=provider,
        site_url=_SITE_URL,
        credential=_credential(),
        batch_size=2,
        fetch_interval_secs=1.0,
        successful_clock=successful_clock,
        waiter=_Waiter() if waiter is None else waiter,
    )


def _strict_group(path: Path, *, holdback_secs: float = 300.0) -> GroupState:
    return open_test_group(
        path,
        initial_position=100,
        max_pending_epochs=8,
        ordering='strict',
        site=_SITE,
        site_server_url=_SITE_URL,
        source=_SOURCE,
        source_generation=_SOURCE_GENERATION,
        stream_fingerprint=_STREAM,
        strict_holdback_secs=holdback_secs,
    )


def _strict_reader(
    provider: Provider,
    successful_clock: _SuccessfulQueryClock,
) -> CatchupReader:
    return CatchupReader(
        provider=provider,
        site_url=_SITE_URL,
        credential=_credential(),
        batch_size=20,
        fetch_interval_secs=1.0,
        successful_clock=successful_clock,
        waiter=_Waiter(),
    )


def _watchdog_token() -> str:
    return os.urandom(16).hex()


def _poller(
    state: SourceState,
    provider: Provider,
    **options: Unpack[_PollerOptions],
) -> SourcePoller:
    fanout = options.get('fanout')
    config = options.get('config')
    successful_clock = options.get('successful_clock')
    strict_buffer = options.get('strict_buffer')
    waiter = options.get('waiter')
    random_value = options.get('random_value')
    watchdog = options.get('watchdog')
    return SourcePoller(
        state=state,
        fanout=EventFanout() if fanout is None else fanout,
        provider=provider,
        site_url=_SITE_URL,
        credential=_credential(),
        config=_source_config() if config is None else config,
        max_ipc_frame_bytes=8_388_608,
        source_generation=_SOURCE_GENERATION,
        successful_clock=successful_clock,
        strict_buffer=strict_buffer,
        waiter=_Waiter() if waiter is None else waiter,
        random_value=(lambda: 0.5) if random_value is None else random_value,
        watchdog=(
            ComponentWatchdog(
                kind='source',
                identity=_SOURCE,
                worker_instance=_SOURCE_GENERATION,
                registration_token=_watchdog_token(),
                startup_deadline_secs=10.0,
            )
            if watchdog is None
            else watchdog
        ),
        internal_deadline_secs=10.0,
        wait_tolerance_secs=5.0,
    )


def test_catchup_batch_flushes_prefix_before_gap_on_failure() -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}],
        [_event(101), _event(103)],
        [_event(102)],
    )
    pages: list[list[int]] = []

    def deliver_page(events: Sequence[Event]) -> None:
        pages.append([cast('int', event['id']) for event in events])
        if len(pages) == 2:
            raise RuntimeError('lane halted')

    with (
        _reader(provider) as reader,
        pytest.raises(RuntimeError, match='lane halted'),
    ):
        reader.catch_up(
            cursor=100,
            frontier=103,
            delivery_floor=100,
            source_progress=lambda: _source_progress(103),
            deliver_page=deliver_page,
        )
    assert pages == [[101], [102]]
    assert not sg._responses


def test_catchup_reader_clock_pauses_during_outage_and_resumes_durably(
    tmp_path: Path,
) -> None:
    wall = _Monotonic()
    waiter = _Waiter(clock=wall)
    clock = _SuccessfulQueryClock(clock=wall)
    provider, _sg, _factory = _provider(
        [{'id': 100}],
        RuntimeError('reader outage'),
        [{'id': 100}],
        [{'id': 100}],
        [{'id': 1}],
        [{'id': 101}],
    )
    state_path = tmp_path / 'group.db'
    with (
        _strict_group(state_path, holdback_secs=70.0) as state,
        _reader(provider, successful_clock=clock, waiter=waiter) as reader,
    ):
        assert reader.latest_visible_id() == 100
        assert clock._value == 0.0
        with pytest.raises(RuntimeError, match='reader outage'):
            reader.latest_visible_id()
        waiter.wait(60.0)
        assert reader.latest_visible_id() == 100
        assert clock._value == 0.0
        waiter.wait(10.0)
        assert reader.latest_visible_id() == 100
        assert clock._value == 10.0

        result = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(101),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=70.0,
            max_pending_epochs=8,
        )
        assert result._reader_clock == 10.0
        assert state.snapshot().reader_clock_checkpoint == 10.0

    with _strict_group(state_path, holdback_secs=70.0) as reopened:
        restart_provider, _restart_sg, _restart_factory = _provider(
            [{'id': 1}],
            [{'id': 101}],
        )
        restart_clock = _SuccessfulQueryClock(clock=_Monotonic())
        with _strict_reader(restart_provider, restart_clock) as reader:
            resumed = reader.strict_step(
                state=reopened,
                source_progress=lambda: _source_progress(101),
                deliver=_deliver_strict_events(
                    lambda _event, _ordering_time: StrictDeliveryOutcome(
                        accepted=True,
                        continue_epoch=True,
                    )
                ),
                holdback_secs=70.0,
                max_pending_epochs=8,
            )

        assert resumed._reader_clock == 10.0
        assert reopened.snapshot().reader_clock_checkpoint == 10.0


def test_catchup_reads_every_bounded_page_before_advancing_cursor() -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}],
        [_event(101), _event(102)],
        [_event(103), _event(104)],
        [_event(105)],
    )
    delivered: list[int] = []

    with _reader(provider) as reader:
        result = reader.catch_up(
            cursor=100,
            frontier=105,
            delivery_floor=100,
            source_progress=lambda: _source_progress(105),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )

    assert result._cursor == 105
    assert delivered == [101, 102, 103, 104, 105]
    assert all(
        clause[1] in {'greater_than', 'less_than'}
        for call in sg._calls[1:]
        for clause in cast('list[list[object]]', call[1])
    )
    assert [call[1] for call in sg._calls[1:]] == [
        [['id', 'greater_than', 100], ['id', 'less_than', 106]],
        [['id', 'greater_than', 102], ['id', 'less_than', 106]],
        [['id', 'greater_than', 104], ['id', 'less_than', 106]],
    ]


def test_catchup_retention_floor_ignores_the_discarded_delivery_prefix() -> None:
    provider, _sg, _factory = _provider(
        [{'id': 151}],
        [_event(151), _event(152)],
    )
    delivered: list[int] = []

    with _reader(provider) as reader:
        result = reader.catch_up(
            cursor=100,
            frontier=152,
            delivery_floor=150,
            source_progress=lambda: _source_progress(152),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )

    assert result._cursor == 152
    assert delivered == [151, 152]


def test_clean_strict_publications_do_not_rescan_ranges(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([], [])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        state.begin_cycle()
        assert state.strict_ranges_if_changed() == ()
        range_scans = 0
        original = source_store_module.ranges

        def counted_ranges(connection: sqlite3.Connection) -> tuple[DiscoveryRange, ...]:
            nonlocal range_scans
            range_scans += 1
            return original(connection)

        monkeypatch.setattr(source_store_module, 'ranges', counted_ranges)
        with _poller(state, provider, strict_buffer=strict) as poller:
            assert poller._poll_once() == 0
            assert poller._poll_once() == 0

    assert range_scans == 0


def test_delayed_old_token_messages_never_touch_new_attempt(tmp_path: Path) -> None:
    fanout = EventFanout()
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=fanout,
            source_generation=_SOURCE_GENERATION,
        )
        old = coordinator.request(JoinRequest(_attempt('old')), _channel())
        assert isinstance(old, JoinTarget)
        coordinator.abort(JoinAbort(old.identity))
        current_channel = _channel()
        current = coordinator.request(JoinRequest(_attempt('new')), current_channel)
        assert isinstance(current, JoinTarget)

        with pytest.raises(JoinProtocolError, match='stale'):
            coordinator.confirm(JoinConfirm(old.identity))
        with pytest.raises(JoinProtocolError, match='stale'):
            coordinator.finalize(JoinFinalize(old.identity))
        coordinator.abort(JoinAbort(old.identity))

        assert current_channel.status.provisional
        assert coordinator.confirm(JoinConfirm(current.identity)) == Attached(current.identity)


def test_delivery_floor_discards_queued_ordinary_frames_at_or_below_floor() -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [_event(103)],
    )
    delivered: list[int] = []
    with _reader(provider) as reader:
        result = reader.catch_up(
            cursor=100,
            frontier=103,
            delivery_floor=102,
            source_progress=lambda: _source_progress(103),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )

    assert delivered == [103]
    assert result._cursor == 103


def test_eager_join_captures_frontier_and_queues_subsequent_events_provisionally(
    tmp_path: Path,
) -> None:
    fanout = EventFanout()
    channel = _channel()
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=fanout,
            source_generation=_SOURCE_GENERATION,
        )
        request = JoinRequest(_attempt('attempt-1'))

        target = coordinator.request(request, channel)
        assert isinstance(target, JoinTarget)
        assert target == JoinTarget(
            LiveJoinIdentity(
                site=_SITE,
                source=_SOURCE,
                group='editorial',
                join_kind='eager',
                source_generation=_SOURCE_GENERATION,
                group_generation=7,
                boundary=200,
                attempt_token=request.attempt.attempt_token,
            )
        )
        assert coordinator.request(request, channel) == target

        state.commit_observations(tuple(range(201, 231)), successful_clock=1.0)
        fanout.publish_events(
            tuple(EncodedEvent.from_event_frame(i, b'x') for i in range(201, 231)),
        )
        attached = coordinator.confirm(JoinConfirm(target.identity))
        assert attached == Attached(target.identity)
        finalized = coordinator.finalize(JoinFinalize(target.identity))
        assert finalized == JoinFinalized(target.identity)
        assert channel.status.attached
        assert not channel.status.provisional


def test_eager_join_queue_overflow_requires_fresh_token_and_later_boundary(
    tmp_path: Path,
) -> None:
    fanout = EventFanout()
    first_channel = _channel(max_events=1)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=fanout,
            source_generation=_SOURCE_GENERATION,
        )
        first = coordinator.request(JoinRequest(_attempt('old')), first_channel)
        assert isinstance(first, JoinTarget)
        state.commit_observations((201, 202), successful_clock=1.0)
        fanout.publish_events(
            (
                EncodedEvent.from_event_frame(201, b'x'),
                EncodedEvent.from_event_frame(202, b'x'),
            ),
        )

        retry = coordinator.confirm(JoinConfirm(first.identity))
        assert isinstance(retry, JoinRetry)
        with pytest.raises(JoinProtocolError, match='fresh'):
            coordinator.request(JoinRequest(_attempt('old')), _channel())

        fresh_attempt = _attempt('fresh')
        second = coordinator.request(JoinRequest(fresh_attempt), _channel())
        assert isinstance(second, JoinTarget)
        assert second.identity.boundary == 202
        assert second.identity.attempt_token == fresh_attempt.attempt_token


def test_empty_initialized_poll_uses_no_complete_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([])
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        snapshots = 0
        original = state.snapshot

        def counted_snapshot() -> SourceSnapshot:
            nonlocal snapshots
            snapshots += 1
            return original()

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        with _poller(state, provider) as poller:
            assert poller._poll_once() == 0

    assert snapshots == 0


def test_event_oversize_halts_before_affected_frontier_commit(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([_event(101)])
    config = _source_config(max_event_bytes=10)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(state, provider, config=config) as poller:
            with pytest.raises(SourceHaltedError, match='event-too-large'):
                poller._poll_once()
            assert poller.health.status == 'event-too-large'
        assert state.snapshot().frontier == 100


def test_finalize_and_abort_retries_are_idempotent(tmp_path: Path) -> None:
    fanout = EventFanout()
    channel = _channel()
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=fanout,
            source_generation=_SOURCE_GENERATION,
        )
        target = coordinator.request(JoinRequest(_attempt('token')), channel)
        assert isinstance(target, JoinTarget)
        coordinator.confirm(JoinConfirm(target.identity))

        first = coordinator.finalize(JoinFinalize(target.identity))
        second = coordinator.finalize(JoinFinalize(target.identity))
        assert first == second == JoinFinalized(target.identity)

        coordinator.abort(JoinAbort(target.identity))
        coordinator.abort(JoinAbort(target.identity))
        assert not channel.status.attached


def test_full_advancing_batch_refetches_immediately_but_partial_waits(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [_event(101), _event(102)],
        [],
    )
    waiter = _Waiter()
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(state, provider, waiter=waiter) as poller:
            poller.run(max_cycles=2)

    assert waiter._waits == [1.0]


def test_health_from_mutation_snapshot_avoids_another_snapshot_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([])
    with _open_source(tmp_path / 'source.db') as state:
        snapshot_calls = 0
        snapshot = state.initialize(100, incarnation='incarnation-1')
        real_snapshot = state.snapshot

        def counted_snapshot() -> SourceSnapshot:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return real_snapshot()

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        with _poller(
            state,
            provider,
            successful_clock=_SuccessfulQueryClock(clock=lambda: 0.0),
        ) as poller:
            mutation_health = poller.health_from_snapshot(snapshot)
            property_health = poller.health

    assert snapshot_calls == 1
    assert mutation_health == property_health


def test_health_is_complete_immutable_source_snapshot(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([])
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(
            state,
            provider,
            successful_clock=_SuccessfulQueryClock(clock=lambda: 0.0),
        ) as poller:
            poller._poll_once()
            path = tmp_path / 'source.db'
            database_size_bytes = sum(
                candidate.stat().st_size
                for candidate in (
                    path,
                    Path(f'{path}-wal'),
                    Path(f'{path}-shm'),
                )
                if candidate.exists()
            )
            assert poller.health == SourceHealth(
                site=_SITE,
                source=_SOURCE,
                source_generation=_SOURCE_GENERATION,
                status='running',
                frontier=100,
                successful_clock=0.0,
                retry_attempt=0,
                next_retry_secs=None,
                open_range_count=0,
                late_id_count=0,
                last_error=None,
                stream_incarnation='incarnation-1',
                audit_count=0,
                database_size_bytes=database_size_bytes,
            )


@pytest.mark.parametrize(
    ('floor', 'required', 'frontier', 'expected_floor', 'no_visible_rows'),
    [
        (None, None, 20, None, False),
        (12, None, 20, None, False),
        (None, 11, 20, 21, True),
        (None, 11, 0, None, False),
        (12, 11, 20, 12, False),
        (11, 11, 20, None, False),
        (1, 11, 20, None, False),
    ],
)
def test_history_truncation_requires_source_evidence(
    floor: int | None,
    required: int | None,
    frontier: int,
    expected_floor: int | None,
    no_visible_rows: bool,
) -> None:
    error = history_truncation(floor, required, frontier)
    if expected_floor is None:
        assert error is None
    else:
        assert isinstance(error, HistoryTruncatedError)
        assert error.retention_floor == expected_floor
        assert error._required_event_id == required
        assert error._no_visible_rows is no_visible_rows


def test_join_abort_requires_a_fresh_attempt_token(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation=_SOURCE_GENERATION,
        )
        request = JoinRequest(_attempt('aborted'))
        target = coordinator.request(request, _channel())
        assert isinstance(target, JoinTarget)

        coordinator.abort(JoinAbort(target.identity))

        with pytest.raises(JoinProtocolError, match='fresh'):
            coordinator.request(request, _channel())


def test_join_rejects_an_attempt_for_another_source(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation=_SOURCE_GENERATION,
        )
        attempt = JoinAttemptIdentity(
            site=_SITE,
            source='other-source',
            group='editorial',
            join_kind='eager',
            group_generation=7,
            attempt_token=_revision_id('foreign'),
        )

        with pytest.raises(JoinProtocolError, match='source identity'):
            coordinator.request(JoinRequest(attempt), _channel())


def test_last_error_projection_tracks_retry_and_recovery_without_health_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, _sg, _factory = _provider(RuntimeError('provider outage'), [])
    with _open_source(tmp_path / 'last-error.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(state, provider) as poller:
            assert poller.last_error is None
            poller.run(max_cycles=1)
            with monkeypatch.context() as patch:
                patch.setattr(state, 'snapshot', Mock(side_effect=AssertionError('full snapshot')))
                assert poller.last_error == 'provider outage'
            poller.run(max_cycles=1)
            with monkeypatch.context() as patch:
                patch.setattr(state, 'snapshot', Mock(side_effect=AssertionError('full snapshot')))
                assert poller.last_error is None


def test_live_strict_epoch_spacing_prevents_early_capture(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 110}],
        [{'id': 1}],
        [{'id': 120}],
        [{'id': 1}],
        [{'id': 120}],
    )
    wall = _Monotonic()
    synchronizations = iter((110, 120, 120))
    with (
        _strict_group(tmp_path / 'group.db') as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(next(synchronizations)),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=300.0,
            max_pending_epochs=8,
        )
        wall._advance(42.0)

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(next(synchronizations)),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=300.0,
            max_pending_epochs=8,
        )

        assert [(epoch.low_id, epoch.high_id) for epoch in state.snapshot().epochs] == [(101, 110)]

        wall._advance(1.0)
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(next(synchronizations)),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=300.0,
            max_pending_epochs=8,
        )

        assert [(epoch.low_id, epoch.high_id) for epoch in state.snapshot().epochs] == [
            (101, 110),
            (111, 120),
        ]


def test_live_strict_epoch_uses_persisted_eligibility_after_config_change(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 110}],
        [{'id': 1}],
        [{'id': 110}],
        [{'id': 1}],
        [{'id': 110}],
        [_event(event_id) for event_id in range(101, 111)],
    )
    wall = _Monotonic()
    delivered: list[tuple[int, float]] = []
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=300.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=300.0,
            max_pending_epochs=8,
        )
        wall._advance(299.0)

        def deliver_waiting(event: Event, ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event)
            delivered.append((event_id, ordering_time))
            return outcome

        waiting = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=_deliver_strict_events(deliver_waiting),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert waiting._cursor == 100
        assert delivered == []
        assert state.snapshot().epochs[0].eligibility_point == 300.0

        wall._advance(1.0)
        completed = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=_deliver_strict_events(deliver_waiting),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert completed._cursor == 110
        assert delivered == [(event_id, 0.0) for event_id in range(101, 111)]
        assert state.snapshot().epochs == ()


def test_poll_once_joined_peer_write_failure_preserves_frontier(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([_event(101)], [_event(102)])
    fanout = EventFanout()
    remotes: list[GroupChannel] = []
    attempt_id = 'attempt-1'
    attempt = JoinAttemptIdentity(
        site='main',
        source='events',
        group='editorial',
        join_kind='eager',
        group_generation=1,
        attempt_token=attempt_id,
    )
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='editorial',
        join_kind='eager',
        source_generation='source-1',
        group_generation=1,
        boundary=100,
        attempt_token=attempt_id,
    )

    def request_join(request: JoinRequest, channel: GroupChannel) -> JoinTarget:
        assert request == JoinRequest(attempt)
        fanout.attach(channel)
        remotes.append(channel)
        return JoinTarget(identity)

    class BrokenSender:
        def __init__(self, sock: socket.socket) -> None:
            self.sock = sock

        def close(self) -> None:
            self.sock.close()

        def fileno(self) -> int:
            return self.sock.fileno()

        def send(self, _data: memoryview) -> int:
            raise BrokenPipeError('remote group disconnected')

    # A short path stays within the Unix endpoint limit on macOS.
    path = Path(os.sep, 'tmp') / f'sed-{os.getpid()}-{tmp_path.name}.sock'
    client = EventClient(
        path=path,
        source='events',
        group='editorial',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=131_072,
        queue_max_entries=2,
        queue_max_bytes=262_144,
        coordination_deadline_secs=1.0,
        stop=threading.Event(),
    )
    sink = GroupChannel(group='editorial', ordering='eager', max_events=2, max_bytes=262_144)
    source = cast('_JoinSource', Mock(request=request_join))
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=131_072,
        queue_max_entries=2,
        queue_max_bytes=262_144,
    ) as server:
        server.install_grant(
            EventPeerGrant(
                source='events',
                group='editorial',
                dependent_spawn_instance='spawn-1',
                capability='capability-1',
            )
        )
        try:
            client.connect()
            with ThreadPoolExecutor(max_workers=1) as executor:
                joined = executor.submit(client.request, JoinRequest(attempt), sink)
                deadline = time.monotonic() + 1.0
                while not joined.done() and time.monotonic() < deadline:
                    server.poll(0.001)
                    server.dispatch_join_requests(source)
                assert joined.result(timeout=1.0) == JoinTarget(identity)
            (channel,) = remotes
            peers = cast('dict[int, object]', vars(server)['_peers'])
            peer = next(peer for peer in peers.values() if vars(peer)['_role'] == 'event')
            vars(peer)['_socket'] = BrokenSender(cast('socket.socket', vars(peer)['_socket']))
            with _open_source(tmp_path / 'source.db') as state:
                state.initialize(100, incarnation='incarnation-1')
                with _poller(state, provider, fanout=fanout) as poller:
                    assert poller._poll_once() == 1
                    assert channel.status.queued_count == 1
                    assert channel.status.attached
                    server.poll(0.001)
                    assert not channel.status.attached
                    assert channel.status.reason == 'connection-replaced'
                    assert server._group_status('editorial')._reason == 'peer-write-failed'
                    assert channel.status.queued_count == 0
                    assert channel.status.queued_bytes == 0
                    assert state.snapshot().frontier == 101
                    assert poller._poll_once() == 1
                assert state.snapshot().frontier == 102
        finally:
            client.close()


def test_poller_adoption_reinitializes_and_resumes_queries(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')

    changed_stream = stream_fingerprint(
        _SITE_URL,
        'restricted',
        'event-reader',
    )
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            """
            INSERT INTO pending_adoptions (
                adoption_id, command_fingerprint, prior_incarnation,
                prior_stream_fingerprint, target_stream_fingerprint, reason
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                'adoption-1',
                'command-1',
                'incarnation-1',
                _STREAM,
                changed_stream,
                'approved replacement',
            ),
        )
    wall = _Monotonic()
    waiter = _Waiter(clock=wall)
    clock = _SuccessfulQueryClock(clock=wall)
    provider, sg, _factory = _provider(
        RuntimeError('adoption outage'),
        [{'id': 41}],
        [],
    )
    with (
        SourceState.open(
            path,
            writes=write_context_for(path),
            create=True,
            site=_SITE,
            source=_SOURCE,
            stream_fingerprint=changed_stream,
            source_generation=_SOURCE_GENERATION,
        ) as state,
        _poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller,
    ):
        with pytest.raises(RuntimeError, match='adoption outage'):
            poller.adopt_stream(
                adoption_id='adoption-1',
                command_fingerprint='command-1',
                new_stream_fingerprint=changed_stream,
                reason='approved replacement',
                groups_parked=True,
            )
        assert clock._value == 0.0
        waiter.wait(60.0)
        snapshot = poller.adopt_stream(
            adoption_id='adoption-1',
            command_fingerprint='command-1',
            new_stream_fingerprint=changed_stream,
            reason='approved replacement',
            groups_parked=True,
        )

        assert snapshot.initialized
        assert snapshot.frontier == 41
        assert snapshot.stream_incarnation == 'adoption-1'
        assert not poller._poller_stopped
        assert clock._value == 0.0
        waiter.wait(10.0)
        assert poller._poll_once() == 0
        assert state.snapshot().successful_clock == 10.0

    assert [call[1] for call in sg._calls] == [
        [],
        [],
        [['id', 'greater_than', 41]],
    ]


def test_poller_blocked_encoder_receive_publishes_internal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([_event(101)])
    entered = threading.Event()
    release = threading.Event()
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=10.0,
    )
    original_encode = EncoderWorker.encode

    def blocked_encode(worker: EncoderWorker, event: Event) -> EncodedEvent:
        entered.set()
        assert release.wait(1.0)
        return original_encode(worker, event)

    monkeypatch.setattr(EncoderWorker, 'encode', blocked_encode)

    def run() -> None:
        with _open_source(tmp_path / 'source.db') as state:
            state.initialize(100, incarnation='incarnation-1')
            with _poller(state, provider, watchdog=watchdog) as poller:
                poller._poll_once()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert entered.wait(1.0)
            sample = watchdog.sample()
            assert sample.phase == 'internal'
            assert sample.deadline_at is not None
        finally:
            release.set()
        future.result(timeout=1.0)


def test_poller_blocked_fanout_publishes_internal(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([_event(101)])
    entered = threading.Event()
    release = threading.Event()
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=10.0,
    )

    class _BlockingFanout(EventFanout):
        def publish_events(self, events: Iterable[EncodedEvent]) -> tuple[FanoutStatus, ...]:
            entered.set()
            assert release.wait(1.0)
            return super().publish_events(events)

    def run() -> None:
        with _open_source(tmp_path / 'source.db') as state:
            state.initialize(100, incarnation='incarnation-1')
            with _poller(
                state,
                provider,
                fanout=_BlockingFanout(),
                watchdog=watchdog,
            ) as poller:
                poller._poll_once()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert entered.wait(1.0)
            sample = watchdog.sample()
            assert sample.phase == 'internal'
            assert sample.deadline_at is not None
        finally:
            release.set()
        future.result(timeout=1.0)


@pytest.mark.parametrize('initialized', [False, True])
def test_poller_blocked_initial_and_main_queries_publish_provider_query(
    initialized: bool,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider([])
    entered = threading.Event()
    release = threading.Event()
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=f'query-{initialized}',
        startup_deadline_secs=10.0,
    )
    original_find = sg.find

    def blocked_find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        entered.set()
        assert release.wait(1.0)
        return original_find(entity_type, filters, fields, **options)

    monkeypatch.setattr(sg, 'find', blocked_find)

    def run() -> None:
        with _open_source(tmp_path / 'source.db') as state:
            if initialized:
                state.initialize(100, incarnation='incarnation-1')
            with _poller(state, provider, watchdog=watchdog) as poller:
                poller._poll_once()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert entered.wait(1.0)
            sample = watchdog.sample()
            assert sample.phase == 'provider-query'
            assert sample.deadline_at is not None
            assert sample.heartbeat_at < sample.deadline_at
        finally:
            release.set()
        future.result(timeout=1.0)


def test_poller_blocked_overlap_recheck_query_publishes_provider_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider([])
    entered = threading.Event()
    release = threading.Event()
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=10.0,
    )
    original_find = sg.find

    def blocked_find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        entered.set()
        assert release.wait(1.0)
        return original_find(entity_type, filters, fields, **options)

    monkeypatch.setattr(sg, 'find', blocked_find)

    def run() -> None:
        with _open_source(
            tmp_path / 'source.db',
            token_factory=lambda: 'range',
        ) as state:
            state.initialize(100, incarnation='incarnation-1')
            notice = open_source_range(state, 101, 101, successful_clock=1.0)

            state.record_successful_query(11.0)
            with _poller(state, provider, watchdog=watchdog) as poller:
                poller._recheck_once(notice.obligation_id)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert entered.wait(1.0)
            sample = watchdog.sample()
            assert sample.phase == 'provider-query'
            assert sample.deadline_at is not None
        finally:
            release.set()
        future.result(timeout=1.0)


def test_poller_blocked_provider_creation_publishes_provider_query(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    handle = _ScriptedSG(())

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> _ScriptedSG:
        assert site == _SITE_URL
        assert script_name == 'event-reader'
        assert api_key == 'secret'
        assert not connect
        entered.set()
        assert release.wait(1.0)
        return handle

    provider = Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        _sync_factory=factory,
        _async_factory=None,
    )
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=10.0,
    )
    with _open_source(tmp_path / 'source.db') as state:
        poller = _poller(state, provider, watchdog=watchdog)
        thread = threading.Thread(target=poller.__enter__)
        thread.start()
        try:
            assert entered.wait(1.0)
            sample = watchdog.sample()
            assert sample.phase == 'provider-query'
            assert sample.deadline_at is not None
            assert sample.heartbeat_at < sample.deadline_at
        finally:
            release.set()
            thread.join(1.0)
            assert not thread.is_alive()
            poller.close()


def test_poller_commits_range_membership_before_event_fanout(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([_event(102)])
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        fanout = _RecordingFanout(state)
        channel = _channel()
        fanout.attach(channel)
        with _poller(state, provider, fanout=fanout) as poller:
            poller._poll_once()
        published = fanout._snapshots[0]
        assert published.frontier == 102
        assert published.ranges[0].low_id == 101
        assert published.pending_obligations == 1
        assert published.ranges[0].group_eligibility == (
            ('editorial', published.successful_clock),
        )
        assert [
            message.encoded.event_id
            for message in channel.drain()
            if isinstance(message, EventDelivery)
        ] == [102]


def test_poller_halts_changed_stream_before_initialized_query(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')

    changed_stream = stream_fingerprint(
        _SITE_URL,
        'restricted',
        'event-reader',
    )
    provider, sg, _factory = _provider([_event(101)])
    with (
        SourceState.open(
            path,
            writes=write_context_for(path),
            create=True,
            site=_SITE,
            source=_SOURCE,
            stream_fingerprint=changed_stream,
            source_generation=_SOURCE_GENERATION,
        ) as state,
        _poller(state, provider) as poller,
    ):
        with pytest.raises(SourceHaltedError, match='stream-identity-changed'):
            poller._poll_once()

        assert poller.health.status == 'stream-identity-changed'
        assert state.snapshot().frontier == 100

    assert sg._calls == []


def test_poller_halts_strict_release_in_the_collapse_cycle(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([_event(104)])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    channel = _channel(ordering='strict')
    attach_strict_group(
        strict, 'editorial', 7, holdback_secs=1, delivery_floor=100, channel=channel
    )
    with _open_source(tmp_path / 'source.db', max_group_obligations=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=0)
        fanout = EventFanout()
        fanout.attach(channel)
        with _poller(state, provider, fanout=fanout, strict_buffer=strict) as poller:
            poller._poll_once()
        assert state.snapshot().replay_markers
        strict.synchronize_ranges(())
        strict.observe_many((EncodedEvent.from_event_frame(105, b'event'),), successful_time=1)
        assert strict.pump_many(('editorial',), 10) == ()


def test_poller_hints_after_disjoint_range_opens_and_close_only(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([_event(102)], [_event(104)], [_event(105)], [], [])
    tokens = iter(('open', 'merge', 'close'))
    with _open_source(
        tmp_path / 'source.db',
        token_factory=lambda: next(tokens),
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        fanout = _RecordingFanout(state)
        with _poller(state, provider, fanout=fanout) as poller:
            poller._poll_once()
            assert fanout._hint_calls == 1
            poller._poll_once()
            assert fanout._hint_calls == 2
            poller._poll_once()
            poller._poll_once()
            assert fanout._hint_calls == 2
            retained = state.snapshot().ranges[0]
            state.record_successful_query(312.0)
            state.begin_range_scan(retained.obligation_id, successful_clock=312.0)
            poller._recheck_once(retained.obligation_id)
            assert fanout._hint_calls == 3
            poller._recheck_once(retained.obligation_id)
            assert fanout._hint_calls == 3


def test_poller_initializes_from_latest_visible_id_through_provider_boundary(
    tmp_path: Path,
) -> None:
    wall = _Monotonic()
    waiter = _Waiter(clock=wall)
    clock = _SuccessfulQueryClock(clock=wall)
    provider, sg, factory = _provider(
        RuntimeError('initialization outage'),
        [{'id': 41}],
        [],
    )
    config = replace(
        _source_config(),
        fetch_interval_secs=10.0,
        poll_retry_base_secs=60.0,
        poll_retry_jitter_fraction=0.0,
        poll_retry_max_secs=60.0,
    )
    with (
        _open_source(tmp_path / 'source.db') as state,
        _poller(
            state,
            provider,
            config=config,
            successful_clock=clock,
            waiter=waiter,
        ) as poller,
    ):
        poller.run(max_cycles=2)
        assert waiter._waits == [60.0, 10.0]
        assert state.snapshot().frontier == 41
        assert state.snapshot().initialized
        assert poller._poll_once() == 0
        assert state.snapshot().successful_clock == 10.0

    assert factory._calls == [(_SITE_URL, 'event-reader', 'secret', False)]
    assert sg._calls == [
        (
            'EventLogEntry',
            [],
            ['id'],
            1,
            [{'field_name': 'id', 'direction': 'desc'}],
            0,
        ),
        (
            'EventLogEntry',
            [],
            ['id'],
            1,
            [{'field_name': 'id', 'direction': 'desc'}],
            0,
        ),
        (
            'EventLogEntry',
            [['id', 'greater_than', 41]],
            list(EVENT_FIELDS),
            2,
            [{'field_name': 'id', 'direction': 'asc'}],
            0,
        ),
    ]
    assert sg._closed


def test_poller_publishes_count_bounded_chunk_before_encoding_next_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([_event(101), _event(102), _event(103)])
    encoded_ids: list[int] = []
    original_encode = EncoderWorker.encode

    def record_encode(worker: EncoderWorker, event: Event) -> EncodedEvent:
        encoded_ids.append(cast('int', event['id']))
        return original_encode(worker, event)

    monkeypatch.setattr(EncoderWorker, 'encode', record_encode)
    fanout = _RecordingFanout(encoded_ids=encoded_ids)
    config = _source_config(batch_size=2)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(state, provider, fanout=fanout, config=config) as poller:
            poller._poll_once()

    assert fanout._encode_counts == [2, 3]


def test_poller_queries_main_stream_ascending_with_exact_event_fields(
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider([_event(101), _event(102)])
    fanout = _RecordingFanout()
    channel = GroupChannel(
        group='editorial',
        ordering='eager',
        max_events=10,
        max_bytes=67_108_864,
    )
    fanout.attach(channel)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(state, provider, fanout=fanout) as poller:
            assert poller._poll_once() == 2

        assert state.snapshot().frontier == 102
        messages = channel.drain()
        assert all(isinstance(message, EventDelivery) for message in messages)
        assert [cast('EventDelivery', message).encoded.event_id for message in messages] == [
            101,
            102,
        ]

    assert sg._calls == [
        (
            'EventLogEntry',
            [['id', 'greater_than', 100]],
            list(EVENT_FIELDS),
            2,
            [{'field_name': 'id', 'direction': 'asc'}],
            0,
        )
    ]


def test_poller_routes_strict_frames_only_through_the_holdback_buffer(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([_event(101)])
    channel = GroupChannel(
        group='editorial',
        ordering='strict',
        max_events=10,
        max_bytes=10_000_000,
    )
    fanout = EventFanout()
    fanout.attach(channel)
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(
        strict,
        'editorial',
        7,
        holdback_secs=10.0,
        delivery_floor=100,
        channel=channel,
    )
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        clock = _SuccessfulQueryClock(clock=lambda: 0.0)
        with _poller(
            state,
            provider,
            fanout=fanout,
            successful_clock=clock,
            strict_buffer=strict,
        ) as poller:
            assert poller._poll_once() == 1

    assert channel.drain() == ()
    assert strict.event_count == 1
    assert strict.pump_many(('editorial',), 9.0) == ()
    assert [release._event_id for release in strict.pump_many(('editorial',), 10.0)] == [101]
    queued = channel.drain()
    assert len(queued) == 1
    assert isinstance(queued[0], EventDelivery)


def test_poller_splits_provider_result_by_complete_frame_bytes(
    tmp_path: Path,
) -> None:
    events = [_event(101), _event(102), _event(103)]
    provider, _sg, _factory = _provider(events)
    fanout = _RecordingFanout()
    encoded = [
        EncodedEvent.from_event_frame(event_id, encode_event(Event(event)))
        for event_id, event in zip((101, 102, 103), events, strict=True)
    ]
    config = _source_config(
        batch_size=3,
        commit_batch_max_bytes=sum(event.byte_count for event in encoded[:2]),
    )
    assert sum(event.byte_count for event in encoded) > config.commit_batch_max_bytes
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(state, provider, fanout=fanout, config=config) as poller:
            assert poller._poll_once() == 3

        assert [len(events) for events in fanout._published] == [2, 1]
        assert all(
            sum(event.byte_count for event in events) <= config.commit_batch_max_bytes
            for events in fanout._published
        )
        assert state.snapshot().frontier == 103


def test_post_commit_process_exit_recovers_body_through_catchup(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / 'source.db'
    with _open_source(source_path) as source:
        source.initialize(100, incarnation='incarnation-1')

    code = """
import sys
from pathlib import Path
from sg_event_daemon._source_state import SourceState
from tests._storage_support import write_context_for

source = SourceState.open(
    Path(sys.argv[1]), writes=write_context_for(Path(sys.argv[1])), site='studio', source='events',
    create=True,
    stream_fingerprint=sys.argv[2], source_generation='source-spawn-1',
)
source.commit_observations((102,), successful_clock=1.0)
print('READY', flush=True)
sys.stdin.read()
"""
    environment = dict(os.environ)
    environment['PYTHONPATH'] = os.fspath(Path(__file__).parents[2] / 'src')
    process = subprocess.Popen(
        [sys.executable, '-c', code, str(source_path), _STREAM],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=environment,
    )
    return_code: int | None = None
    try:
        assert _read_child_stdout(process) == 'READY'
        process.kill()
        return_code = process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if process.stdin is not None:
            process.stdin.close()
        if process.stdout is not None:
            process.stdout.close()
    assert return_code is not None
    assert return_code != 0

    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [_event(101), _event(102)],
    )
    delivered: list[int] = []
    with _open_source(source_path) as source, _reader(provider) as reader:
        snapshot = source.snapshot()
        result = reader.catch_up(
            cursor=100,
            frontier=snapshot.frontier,
            delivery_floor=100,
            unresolved_bounds=((101, 101),),
            source_progress=lambda: _source_progress(
                snapshot.frontier,
            ),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )
    assert result._cursor == 102
    assert delivered == [101, 102]


def test_production_strict_publication_fills_and_refills_after_lane_drain(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [_event(event_id) for event_id in range(101, 126)],
        [],
        [],
        [],
    )
    source_config = _source_config(batch_size=25)
    credential = _credential()
    group_config = GroupConfig(
        name='editorial',
        source=_SOURCE,
        plugins=(),
        callback_credentials='admin',
        ordering='strict',
        strict_holdback_secs=1.0,
    )
    site_config = SiteConfig(
        name=_SITE,
        server=_SITE_URL,
        default_source=_SOURCE,
        credentials=MappingProxyType({'admin': credential}),
        sources=MappingProxyType({_SOURCE: source_config}),
        groups=MappingProxyType({'editorial': group_config}),
        workers=MappingProxyType({}),
    )
    strict = StrictBuffer(
        max_events=30,
        max_bytes=10_000_000,
        group_holdbacks={'editorial': 1.0},
        group_delivery_floors={'editorial': 100},
    )
    fanout = EventFanout()
    channel = GroupChannel(
        group='editorial',
        ordering='strict',
        max_events=10,
        max_bytes=10_000_000,
    )
    clock = _Monotonic()
    with (
        _open_source(
            tmp_path / 'source.db',
            delivery_floors={'editorial': 100},
            group_holdbacks={'editorial': 1.0},
        ) as source_state,
        _strict_group(tmp_path / 'group.db', holdback_secs=1.0) as group_state,
    ):
        source_state.initialize(100, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=source_state,
            fanout=fanout,
            source_generation=_SOURCE_GENERATION,
            strict_retention=strict,
        )
        lane = GroupLane(
            state=group_state,
            site=site_config,
            group=group_config,
            provider=provider,
            plugins=(),
            worker_instance='worker-1',
            health_interval_secs=1.0,
            watchdog=ComponentWatchdog(
                kind='lane',
                identity='editorial',
                worker_instance='worker-1',
                registration_token=os.urandom(16).hex(),
                startup_deadline_secs=10.0,
            ),
            token_factory=lambda: 'production-strict-join',
        )
        assert lane.setup() is None
        joined = lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        assert isinstance(joined, LiveJoinIdentity)

        with _poller(
            source_state,
            provider,
            fanout=fanout,
            config=source_config,
            successful_clock=_SuccessfulQueryClock(clock=clock),
            strict_buffer=strict,
        ) as poller:
            assert poller._poll_once() == 25
            assert channel.status.queued_count == 0
            clock._advance(1.0)
            assert poller._poll_once() == 0
            assert channel.status.queued_count == 10
            first = lane.drain_live(channel)
            assert first.cursor == 110
            assert poller._poll_once() == 0
            assert channel.status.queued_count == 10
            second = lane.drain_live(channel)
            assert second.cursor == 120
            assert poller._poll_once() == 0
            assert channel.status.queued_count == 5
            completed = lane.drain_live(channel)
        lane.close()

    assert completed.cursor == 125
    assert strict.event_count == 0


def test_publish_replay_after_retired_range_synchronizes_before_classification(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([_event(101)])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(strict, 'editorial', 7, holdback_secs=10.0, delivery_floor=100)
    strict.synchronize_ranges(
        (
            DiscoveryRange(
                obligation_id='range-101',
                low_id=101,
                high_id=101,
                opened_successful_at=0.0,
                _visibility_deadline=10.0,
                _revision=1,
                _close_generation=0,
                state='open',
                late_ids=(101,),
                group_eligibility=(('editorial', 10.0),),
                _scan_started_at=None,
                scan_next_id=101,
                _scan_pages=0,
                _degraded=False,
            ),
        )
    )
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(state, provider, strict_buffer=strict) as poller:
            assert poller._poll_once() == 1

    assert strict._observations[0]._range_ids == ()


def test_range_cap_projects_degraded_health_without_unrelated_detach(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([_event(104)])
    fanout = EventFanout()
    channel = GroupChannel(
        group='editorial',
        ordering='eager',
        max_events=10,
        max_bytes=67_108_864,
    )
    fanout.attach(channel)
    token = iter(('existing', 'affected'))
    with _open_source(
        tmp_path / 'source.db',
        max_open_ranges=1,
        token_factory=lambda: next(token),
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        with _poller(state, provider, fanout=fanout) as poller:
            with pytest.raises(SourceHaltedError, match='range-capacity-exhausted'):
                poller._poll_once()
            assert poller.health.status == 'range-capacity-exhausted'
        assert channel.status.attached
        assert state.snapshot().frontier == 102


def test_range_recheck_clock_pauses_during_query_outage(tmp_path: Path) -> None:
    wall = _Monotonic()
    waiter = _Waiter(clock=wall)
    clock = _SuccessfulQueryClock(
        clock=wall,
        initial_value=11.0,
    )
    provider, _sg, _factory = _provider(
        RuntimeError('range outage'),
        [_event(101)],
        [_event(102)],
    )
    config = replace(_source_config(), max_range_query_pages=1)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        state.record_successful_query(11.0)
        notice = open_source_range(state, 101, 103, successful_clock=11.0)

        with _poller(
            state,
            provider,
            config=config,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            with pytest.raises(RuntimeError, match='range outage'):
                poller._recheck_once(notice.obligation_id)
            assert clock._value == 11.0
            waiter.wait(60.0)
            poller._recheck_once(notice.obligation_id)
            assert state.snapshot().successful_clock == 11.0
            waiter.wait(10.0)
            poller._recheck_once(notice.obligation_id)

        assert state.snapshot().successful_clock == 21.0


def test_range_recheck_continues_after_short_provider_page(
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider(
        [_event(101)],
        [_event(102), _event(103)],
    )
    config = _source_config(batch_size=2)
    token = iter(('range', 'close'))
    with _open_source(
        tmp_path / 'source.db',
        token_factory=lambda: next(token),
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 103, successful_clock=1.0)

        state.record_successful_query(11.0)
        fanout = _RecordingFanout(state)
        with _poller(state, provider, config=config, fanout=fanout) as poller:
            assert poller._recheck_once(notice.obligation_id) == 3

        assert state.snapshot().ranges[0].late_ids == (101, 102, 103)
        assert state.snapshot().ranges[0].state == 'closed'
        assert fanout._snapshots[0].ranges[0].late_ids == (101,)
        assert fanout._snapshots[0].ranges[0].state == 'open'
        assert fanout._snapshots[-1].ranges[0].late_ids == (101, 102, 103)
        assert fanout._snapshots[-1].ranges[0].state == 'closed'

    assert sg._calls == [
        (
            'EventLogEntry',
            [
                ['id', 'greater_than', 100],
                ['id', 'less_than', 104],
            ],
            list(EVENT_FIELDS),
            2,
            [{'field_name': 'id', 'direction': 'asc'}],
            1,
        ),
        (
            'EventLogEntry',
            [
                ['id', 'greater_than', 101],
                ['id', 'less_than', 104],
            ],
            list(EVENT_FIELDS),
            2,
            [{'field_name': 'id', 'direction': 'asc'}],
            1,
        ),
    ]


def test_range_recheck_halts_changed_stream_before_query(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, token_factory=lambda: 'range') as source:
        source.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(source, 101, 101, successful_clock=1.0)

    changed_stream = stream_fingerprint(
        _SITE_URL,
        'restricted',
        'event-reader',
    )
    provider, sg, _factory = _provider([_event(101)])
    with (
        SourceState.open(
            path,
            writes=write_context_for(path),
            create=True,
            site=_SITE,
            source=_SOURCE,
            stream_fingerprint=changed_stream,
            source_generation=_SOURCE_GENERATION,
        ) as state,
        _poller(state, provider) as poller,
    ):
        before = state.snapshot()

        with pytest.raises(SourceHaltedError, match='stream-identity-changed'):
            poller._recheck_once(notice.obligation_id)

        assert state.snapshot() == before

    assert sg._calls == []


def test_range_recheck_persists_keyset_budget_across_reopens(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    calls: list[tuple[str, object, list[str] | None, int, list[dict[str, str]] | None, int]] = []
    provider, sg, _factory = _provider(
        [_event(101), _event(102)],
        RuntimeError('interrupted range scan'),
    )
    with _open_source(
        path,
        max_range_query_pages=2,
        token_factory=lambda: 'range',
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 105, successful_clock=1.0)

        state.record_successful_query(11.0)
        with (
            _poller(state, provider) as poller,
            pytest.raises(RuntimeError, match='interrupted range scan'),
        ):
            poller._recheck_once(notice.obligation_id)
        calls.extend(sg._calls)
        first_page = state.snapshot().ranges[0]
        assert (
            first_page.scan_next_id,
            first_page._scan_pages,
            first_page.scan_cycle_pages,
            first_page._degraded,
        ) == (103, 1, 1, False)

    provider, sg, _factory = _provider([_event(103), _event(104)])
    with _open_source(path, max_range_query_pages=2) as state:
        reopened = state.snapshot().ranges[0]
        assert (
            reopened.scan_next_id,
            reopened._scan_pages,
            reopened.scan_cycle_pages,
            reopened._degraded,
        ) == (103, 1, 1, False)
        with _poller(state, provider) as poller:
            assert poller._recheck_once(notice.obligation_id) == 2
        calls.extend(sg._calls)
        capped = state.snapshot().ranges[0]
        assert (
            capped.state,
            capped.scan_next_id,
            capped._scan_pages,
            capped.scan_cycle_pages,
            capped._degraded,
        ) == ('open', 105, 2, 2, True)
        assert state.snapshot().conditions == ('range-query-page-cap',)

    provider, sg, _factory = _provider([_event(105)])
    with _open_source(path, max_range_query_pages=2) as state:
        reopened = state.snapshot().ranges[0]
        assert (
            reopened.scan_next_id,
            reopened._scan_pages,
            reopened.scan_cycle_pages,
            reopened._degraded,
        ) == (105, 2, 2, True)
        assert state.snapshot().conditions == ('range-query-page-cap',)
        with _poller(state, provider) as poller:
            assert poller._recheck_once(notice.obligation_id) == 1
        calls.extend(sg._calls)
        completed = state.snapshot().ranges[0]
        assert (
            completed.state,
            completed.late_ids,
            completed._degraded,
        ) == ('closed', (101, 102, 103, 104, 105), False)
        assert state.snapshot().conditions == ()

    assert [call[1] for call in calls] == [
        [
            ['id', 'greater_than', 100],
            ['id', 'less_than', 106],
        ],
        [
            ['id', 'greater_than', 102],
            ['id', 'less_than', 106],
        ],
        [
            ['id', 'greater_than', 102],
            ['id', 'less_than', 106],
        ],
        [
            ['id', 'greater_than', 104],
            ['id', 'less_than', 106],
        ],
    ]
    assert [call[5] for call in calls] == [1, 1, 1, 1]


def test_range_recheck_splits_each_commit_by_complete_frame_bytes(
    tmp_path: Path,
) -> None:
    events = (_event(101), _event(102))
    provider, _sg, _factory = _provider(list(events))
    encoded = tuple(
        EncodedEvent.from_event_frame(event_id, encode_event(Event(event)))
        for event_id, event in zip((101, 102), events, strict=True)
    )
    config = _source_config(
        batch_size=2,
        commit_batch_max_bytes=sum(event.byte_count for event in encoded) - 1,
        max_event_bytes=464,
    )
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 102, successful_clock=1.0)

        fanout = _RecordingFanout(state)

        with _poller(state, provider, fanout=fanout, config=config) as poller:
            assert poller._recheck_once(notice.obligation_id) == 2

        assert [len(events) for events in fanout._published] == [1, 1]
        assert all(
            sum(event.byte_count for event in events) <= config.commit_batch_max_bytes
            for events in fanout._published
        )
        assert sum(event.byte_count for event in encoded) > config.commit_batch_max_bytes
        assert state.snapshot().ranges[0].late_ids == (101, 102)
        assert state.snapshot().ranges[0]._scan_pages == 1


def test_reader_rechecks_floor_before_committing_across_gap() -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [_event(103)],
        [],
        [{'id': 102}],
    )
    with (
        _reader(provider) as reader,
        pytest.raises(HistoryTruncatedError, match='history-truncated'),
    ):
        reader.catch_up(
            cursor=100,
            frontier=103,
            delivery_floor=100,
            source_progress=lambda: _source_progress(103),
            deliver_page=lambda _events: None,
        )


def test_reader_rechecks_stale_replica_gap_before_advancing() -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}],
        [_event(103)],
        [_event(102)],
        [],
        [{'id': 1}],
        [_event(102), _event(103)],
    )
    waiter = _Waiter()
    delivered: list[int] = []
    with _reader(provider, waiter=waiter) as reader:
        result = reader.catch_up(
            cursor=100,
            frontier=103,
            delivery_floor=100,
            source_progress=lambda: _source_progress(103),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )

    assert result._cursor == 103
    assert delivered == [102, 103]
    assert waiter._waits == [1.0, 1.0]
    assert not sg._responses


def test_recording_fanout_forwards_one_shot_events_in_order() -> None:
    fanout = _RecordingFanout()
    channel = _channel()
    fanout.attach(channel)
    events = (
        EncodedEvent.from_event_frame(101, b'event-101'),
        EncodedEvent.from_event_frame(102, b'event-102'),
    )

    fanout.publish_events(
        (event for event in events),
    )

    assert fanout._published == [events]
    assert channel.drain() == tuple(EventDelivery(event) for event in events)


def test_replay_is_inclusive_rejects_future_and_creates_no_source_range() -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [_event(50), _event(52)],
        [],
    )
    delivered: list[int] = []
    with _reader(provider) as reader:
        result = reader.catch_up(
            cursor=100,
            frontier=100,
            delivery_floor=0,
            replay_from=50,
            source_progress=lambda: _source_progress(100),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )
        with pytest.raises(ValueError, match=r'above.*frontier'):
            reader.catch_up(
                cursor=100,
                frontier=100,
                delivery_floor=0,
                replay_from=101,
                source_progress=lambda: _source_progress(100),
                deliver_page=lambda _events: None,
            )

    assert result._cursor == 100
    assert delivered == [50, 52]


def test_retry_backoff_jitters_capped_delay_after_large_attempt(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider(
        *(RuntimeError('long outage') for _ in range(1025)),
    )
    waiter = _Waiter()
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(
            state,
            provider,
            random_value=lambda: 1.0,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=1025)

            assert poller.health.retry_attempt == 1025
            assert poller.health.next_retry_secs == pytest.approx(3.6)

    assert len(waiter._waits) == 1025
    assert waiter._waits[-1] == pytest.approx(3.6)


def test_retry_backoff_uses_exact_cap_multiplier_jitter_and_resets(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        RuntimeError('outage-1'),
        RuntimeError('outage-2'),
        [],
        RuntimeError('outage-after-success'),
    )
    waiter = _Waiter()
    random_value = _RandomSequence(0.0, 0.5, 1.0, 0.0)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(
            state,
            provider,
            random_value=random_value,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=4)
            assert poller.health.retry_attempt == 1

    assert waiter._waits == [0.8, 2.0, 1.0, 0.8]


@pytest.mark.parametrize(
    ('base', 'maximum', 'multiplier', 'attempts'),
    [
        (1.0, 4.0, 2.0, (1, 2, 3, 4096, 10**1000)),
        (5e-324, 1.0, 1.1, (1, 2, 100, 4096, 8000, 10**1000)),
        (5e-324, 1.0, nextafter(1.0, 2.0), (1, 10**18, 2**64)),
        (5e-324, 1.0, 1.0, (1, 4096, 10**1000)),
        (8.0, 4.0, 2.0, (1, 2, 10**1000)),
        (
            0.5,
            float.fromhex('0x1.fffffffffffffp+1023'),
            11375.830160299263,
            (1, 76, 77, 78, 10**1000),
        ),
    ],
)
def test_retry_delay_is_positive_finite_capped_and_monotone(
    base: float,
    maximum: float,
    multiplier: float,
    attempts: tuple[int, ...],
) -> None:
    delays = [
        _saturating_retry_delay(attempt, base_secs=base, max_secs=maximum, multiplier=multiplier)
        for attempt in attempts
    ]

    assert all(isfinite(delay) and 0 < delay <= maximum for delay in delays)
    assert delays == sorted(delays)
    assert delays[0] == min(base, maximum)
    assert delays[-1] == (min(base, maximum) if multiplier == 1.0 else maximum)


@pytest.mark.parametrize(
    ('attempt', 'base', 'maximum', 'multiplier', 'expected'),
    [
        (4096, 5e-324, 1.0, 1.1, float.fromhex('0x1.0e0578384a299p-511')),
        (
            1773,
            float.fromhex('0x1.dc6f5bbbceb86p-948'),
            float.fromhex('0x1.55d77fe6eeddbp+532'),
            float.fromhex('0x1.c8a65478e1679p+0'),
            float.fromhex('0x1.55d77fe6eeddap+532'),
        ),
    ],
)
def test_retry_delay_scales_powers_beyond_float_range(
    attempt: int, base: float, maximum: float, multiplier: float, expected: float
) -> None:
    delay = _saturating_retry_delay(
        attempt, base_secs=base, max_secs=maximum, multiplier=multiplier
    )

    assert 0 < delay <= maximum
    assert delay == pytest.approx(expected, rel=1e-12, abs=0.0)


def test_retry_wait_publishes_bounded_delay_and_wait_deadline(tmp_path: Path) -> None:
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=10.0,
    )

    class _SamplingWaiter(_Waiter):
        def wait(self, seconds: float) -> bool:
            sample = watchdog.sample()
            assert sample.phase == 'wait'
            assert sample.deadline_at is not None
            assert sample.deadline_at >= sample.heartbeat_at + seconds
            return super().wait(seconds)

    provider, _sg, _factory = _provider(RuntimeError('provider outage'))
    waiter = _SamplingWaiter(stop_on_wait=1)
    config = _source_config()
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(
            state,
            provider,
            config=config,
            random_value=lambda: 1.0,
            waiter=waiter,
            watchdog=watchdog,
        ) as poller:
            poller.run(max_cycles=1)
            health = poller.health

    cap = config.poll_retry_max_secs * (1 + config.poll_retry_jitter_fraction)
    assert health.retry_attempt == 1
    assert health.next_retry_secs is not None
    assert 0 <= health.next_retry_secs <= cap
    assert health.status == 'running'
    assert health.last_error == 'provider outage'


def test_run_does_not_recheck_after_failed_poll(tmp_path: Path) -> None:
    provider, sg, _factory = _provider(RuntimeError('main outage'))
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        state.record_successful_query(11.0)

        with _poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=1)
            assert poller.health.retry_attempt == 1

        due_range = state.snapshot().ranges[0]
        assert due_range.state == 'open'
        assert due_range._scan_started_at is None
        assert due_range.scan_next_id == 101

    assert len(sg._calls) == 1
    assert waiter._waits == [1.0]


@pytest.mark.parametrize('stopped', [False, True])
def test_run_propagates_owner_write_failure_without_provider_retry(
    tmp_path: Path, stopped: bool
) -> None:
    provider, sg, _factory = _provider([])
    waiter = _Waiter()
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(state, provider, waiter=waiter) as poller:
            original = state._writes
            stop = threading.Event()
            if stopped:
                stop.set()
            state._writes = replace(original, budget_secs=0.01, stop=stop)
            entry = original.lock._entry
            assert entry is not None
            assert entry.lock.acquire(blocking=False)
            try:
                expected = WriteLockStoppedError if stopped else WriteLockTimeoutError
                with pytest.raises(expected, match='write lock wait') as failure:
                    poller.run(max_cycles=1)
                if not stopped:
                    assert isinstance(failure.value, WriteLockTimeoutError)
                    assert failure.value.level == 'process-local'
                assert waiter._waits == []
                assert poller._retry_attempt == 0
                assert poller.last_error is None
            finally:
                entry.lock.release()
                state._writes = original
        assert len(sg._calls) == 0
        assert state.snapshot().frontier == 100


def test_run_propagates_recheck_capacity_halt(tmp_path: Path) -> None:
    provider, sg, _factory = _provider(
        [],
        [_event(102)],
        [_event(104)],
        [_event(102)],
    )
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with _open_source(tmp_path / 'source.db', max_source_late_ids=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        revision = open_source_range(state, 101, 103, successful_clock=1.0)

        state.record_successful_query(11.0)
        record_source_late_ids(
            state,
            revision.obligation_id,
            (101,),
            successful_clock=11.0,
        )

        with _poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            with pytest.raises(SourceHaltedError) as halted:
                poller.run(max_cycles=2)

            retained = state.snapshot()
            assert retained.frontier == 104
            assert retained.status == 'late-capacity-exhausted'
            assert len(retained.ranges) == 1
            assert retained.ranges[0].obligation_id == revision.obligation_id
            assert retained.ranges[0].late_ids == (101,)
            assert poller.health.status == 'late-capacity-exhausted'
            assert poller.health.retry_attempt == 0
            assert str(halted.value).startswith('late-capacity-exhausted:')

    assert len(sg._calls) == 2
    assert waiter._waits == []


def test_run_propagates_recheck_encoder_halt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider(
        [],
        [_event(102)],
        [_event(104)],
        [_event(102)],
    )
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    encode = EncoderWorker.encode

    def encode_late_event(encoder: EncoderWorker, event: Event) -> EncodedEvent:
        if event['id'] == 102:
            raise EncoderHaltedError(
                'event-too-large: late event exceeds encoder limit', code='event-too-large'
            )
        return encode(encoder, event)

    monkeypatch.setattr(EncoderWorker, 'encode', encode_late_event)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        revision = open_source_range(state, 101, 103, successful_clock=1.0)

        state.record_successful_query(11.0)

        with _poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            with pytest.raises(SourceHaltedError) as halted:
                poller.run(max_cycles=2)

            retained = state.snapshot()
            assert retained.frontier == 104
            assert retained.status == 'running'
            assert len(retained.ranges) == 1
            assert retained.ranges[0].obligation_id == revision.obligation_id
            assert retained.ranges[0].late_ids == ()
            assert poller.health.status == 'event-too-large'
            assert poller.health.retry_attempt == 0
            assert str(halted.value).startswith('event-too-large:')

    assert len(sg._calls) == 2
    assert waiter._waits == []


def test_run_rechecks_at_most_one_range_per_poll_in_due_order(
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider([], [], [], [], [], [])
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=12.0)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        later_low = open_source_range(state, 101, 101, successful_clock=1.0)
        later_high = open_source_range(state, 201, 201, successful_clock=1.0)
        earliest = open_source_range(state, 301, 301, successful_clock=1.0)
        state._database.connection.execute(
            'UPDATE discovery_ranges SET visibility_deadline = 12 WHERE low_id < 301'
        )

        state.record_successful_query(12.0)

        with _poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=1)
            states = {item.obligation_id: item.state for item in state.snapshot().ranges}
            assert states == {
                earliest.obligation_id: 'closed',
                later_high.obligation_id: 'open',
                later_low.obligation_id: 'open',
            }

            poller.run(max_cycles=1)
            states = {item.obligation_id: item.state for item in state.snapshot().ranges}
            assert states == {
                earliest.obligation_id: 'closed',
                later_high.obligation_id: 'open',
                later_low.obligation_id: 'closed',
            }

            poller.run(max_cycles=1)

        assert all(item.state == 'closed' for item in state.snapshot().ranges)

    assert [call[1] for call in sg._calls[1::2]] == [
        [
            ['id', 'greater_than', 300],
            ['id', 'less_than', 302],
        ],
        [
            ['id', 'greater_than', 100],
            ['id', 'less_than', 102],
        ],
        [
            ['id', 'greater_than', 200],
            ['id', 'less_than', 202],
        ],
    ]
    assert waiter._waits == [1.0, 1.0, 1.0]


def test_run_rechecks_due_range_after_successful_poll(tmp_path: Path) -> None:
    provider, sg, _factory = _provider([], [_event(101)])
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 101, successful_clock=1.0)

        state.record_successful_query(11.0)

        with _poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=1)

        completed = state.snapshot().ranges[0]
        assert completed.obligation_id == notice.obligation_id
        assert completed.state == 'closed'

    assert [call[1] for call in sg._calls] == [
        [['id', 'greater_than', 102]],
        [
            ['id', 'greater_than', 100],
            ['id', 'less_than', 102],
        ],
    ]
    assert waiter._waits == [1.0]


def test_run_reselects_due_range_from_durable_progress_until_closed(
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider(
        [],
        [_event(101), _event(102)],
        [],
        [_event(103)],
    )
    config = replace(_source_config(), max_range_query_pages=1)
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with _open_source(
        tmp_path / 'source.db',
        max_range_query_pages=1,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 103, successful_clock=1.0)

        state.record_successful_query(11.0)

        with _poller(
            state,
            provider,
            config=config,
            successful_clock=clock,
        ) as poller:
            poller.run(max_cycles=1)
            first_cycle = state.snapshot().ranges[0]
            assert (
                first_cycle.state,
                first_cycle.scan_next_id,
                first_cycle.scan_cycle_pages,
            ) == ('open', 103, 1)

            poller.run(max_cycles=1)

        completed = state.snapshot().ranges[0]
        assert completed.state == 'closed'
        assert completed.late_ids == (101, 102, 103)

    assert [call[1] for call in sg._calls[1::2]] == [
        [
            ['id', 'greater_than', 100],
            ['id', 'less_than', 104],
        ],
        [
            ['id', 'greater_than', 102],
            ['id', 'less_than', 104],
        ],
    ]


def test_run_retries_consecutive_recheck_failures_exponentially(tmp_path: Path) -> None:
    provider, sg, _factory = _provider(
        [],
        RuntimeError('recheck outage 1'),
        [],
        RuntimeError('recheck outage 2'),
    )
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        state.record_successful_query(11.0)

        with _poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=2)
            assert poller.health.retry_attempt == 2
            assert poller.health.next_retry_secs == 2.0

        due_range = state.snapshot().ranges[0]
        assert due_range.state == 'open'
        assert due_range.scan_next_id == 101

    assert len(sg._calls) == 4
    assert waiter._waits == [1.0, 2.0]


def test_run_retries_failed_recheck_and_resumes_on_next_poll(tmp_path: Path) -> None:
    provider, sg, _factory = _provider(
        [],
        RuntimeError('recheck outage'),
        [],
        [_event(101)],
    )
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        state.record_successful_query(11.0)

        with _poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=1)
            interrupted = state.snapshot().ranges[0]
            assert interrupted.state == 'open'
            assert interrupted._scan_started_at == 11.0
            assert interrupted.scan_next_id == 101
            assert poller.health.retry_attempt == 1

            poller.run(max_cycles=1)
            assert poller.health.retry_attempt == 0

        assert state.snapshot().ranges[0].state == 'closed'

    assert len(sg._calls) == 4
    assert waiter._waits == [1.0, 1.0]


def test_run_skips_due_range_without_an_active_generation(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, groups={'editorial': 7}) as state:
        state.initialize(100, incarnation='incarnation-1')
        retired = open_source_range(state, 101, 101, successful_clock=1.0)

        with _open_group(path) as group:
            orphan_group(group)

    provider, sg, _factory = _provider([], [])
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with _open_source(path, groups={'editorial': 8}) as state:
        state._database.connection.execute(
            "UPDATE groups SET group_generation = 8, active = 1, lifecycle = 'active' "
            "WHERE group_name = 'editorial'"
        )
        active = open_source_range(state, 201, 201, successful_clock=1.0)

        state.record_successful_query(11.0)

        with _poller(state, provider, successful_clock=clock) as poller:
            poller.run(max_cycles=1)

        ranges = {item.obligation_id: item for item in state.snapshot().ranges}
        assert retired.obligation_id not in ranges
        assert ranges[active.obligation_id].state == 'closed'

    assert [call[1] for call in sg._calls] == [
        [['id', 'greater_than', 202]],
        [
            ['id', 'greater_than', 200],
            ['id', 'less_than', 202],
        ],
    ]


def test_scan_range_streams_all_pages_in_order() -> None:
    provider, sg, _factory = _provider(
        [_event(101), _event(102)],
        [_event(103)],
    )

    with _reader(provider) as reader:
        assert [event['id'] for event in reader.scan_range(101, 103)] == [101, 102, 103]
    assert not sg._responses


def test_source_generation_change_invalidates_cached_finalize(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation=_SOURCE_GENERATION,
        )
        target = coordinator.request(JoinRequest(_attempt('finalized')), _channel())
        assert isinstance(target, JoinTarget)
        coordinator.confirm(JoinConfirm(target.identity))
        coordinator.finalize(JoinFinalize(target.identity))

        coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation='source-spawn-2',
        )

        with pytest.raises(JoinProtocolError, match='stale'):
            coordinator.finalize(JoinFinalize(target.identity))


@pytest.mark.parametrize(
    ('error', 'expected'),
    [
        (
            SourceHaltedError('original diagnostic', code='commit-batch-capacity-exhausted'),
            'frame-too-large',
        ),
        (EncoderHaltedError('original diagnostic', code='encoder-died'), 'encoder-died'),
        (EncoderHaltedError('original diagnostic', code='encoder-failed'), 'encoder-failed'),
        (EncoderHaltedError('original diagnostic', code='encoder-shutdown'), 'encoder-shutdown'),
        (EncoderHaltedError('original diagnostic', code='event-too-large'), 'event-too-large'),
        (EncoderHaltedError('original diagnostic', code='frame-too-large'), 'frame-too-large'),
        (SourceCapacityError('original diagnostic', code='frame-too-large'), 'frame-too-large'),
        (
            SourceCapacityError('original diagnostic', code='late-capacity-exhausted'),
            'late-capacity-exhausted',
        ),
        (
            SourceCapacityError('original diagnostic', code='obligation-capacity-exhausted'),
            'obligation-capacity-exhausted',
        ),
        (
            SourceCapacityError('original diagnostic', code='range-capacity-exhausted'),
            'range-capacity-exhausted',
        ),
        (
            SourceHaltedError('original diagnostic', code='range-query-page-cap'),
            'range-query-page-cap',
        ),
        (
            SourceHaltedError('original diagnostic', code='stream-identity-changed'),
            'stream-identity-changed',
        ),
    ],
)
def test_source_halt_code_is_independent_of_diagnostic(
    error: BaseException,
    expected: str,
) -> None:
    for message in ('operator-facing explanation', 'encoder-failed: misleading prefix'):
        error.args = (message,)
        assert isinstance(error, RuntimeError)
        assert str(error) == message
        assert _source_halt_code(error) == expected


def test_source_halt_code_rejects_unknown_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    error = SourceHaltedError('event-too-large', code='event-too-large')
    monkeypatch.setattr(error, 'code', 'future-halt')
    with pytest.raises(RuntimeError, match='unknown source halt code: future-halt') as rejected:
        _source_halt_code(error)
    assert rejected.value.__cause__ is error


@pytest.mark.parametrize(
    'error', [RuntimeError('event-too-large'), SourceHaltedError('event-too-large')]
)
def test_source_halt_code_rejects_untyped_reasons(error: BaseException) -> None:
    with pytest.raises(RuntimeError, match='unknown source halt code') as rejected:
        _source_halt_code(error)
    assert rejected.value.__cause__ is error


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        ('x' * 4097, 'x' * 4096),
        ('é' * 2049, 'é' * 2048),
        ('x' * 4096 + ' token=abc', '<redacted>'),
    ],
)
def test_source_health_text_is_utf8_bounded(value: str, expected: str) -> None:
    identity = 'identity-' + 'i' * 4097
    health = SourceHealth(
        site=identity,
        source=identity,
        source_generation=identity,
        status='running',
        frontier=1,
        successful_clock=2.0,
        retry_attempt=0,
        next_retry_secs=None,
        open_range_count=0,
        late_id_count=0,
        last_error=value,
        worker_instance=identity,
    )

    assert health.last_error == expected
    assert health.last_error is not None
    assert len(health.last_error.encode()) <= 4096
    assert health.site == health.source == health.source_generation == identity
    assert health.worker_instance == identity
    assert health.status == 'running'


@pytest.mark.parametrize(
    ('cursor', 'frontier', 'bounds', 'floor', 'halts'),
    [
        (100, 120, (), 102, True),
        (110, 120, ((90, 95),), 100, True),
        (120, 120, (), 121, False),
    ],
)
def test_catchup_retention_floor_checks_oldest_required_position(
    bounds: tuple[tuple[int, int], ...],
    cursor: int,
    floor: int,
    frontier: int,
    halts: bool,
) -> None:
    provider, _sg, _factory = _provider(
        [{'id': floor}] if floor <= frontier else [],
        [],
    )
    with _reader(provider) as reader:
        if halts:
            with pytest.raises(HistoryTruncatedError, match='history-truncated'):
                reader.catch_up(
                    cursor=cursor,
                    frontier=frontier,
                    delivery_floor=0,
                    unresolved_bounds=bounds,
                    source_progress=lambda: _source_progress(frontier),
                    deliver_page=lambda _events: None,
                )
        else:
            result = reader.catch_up(
                cursor=cursor,
                frontier=frontier,
                delivery_floor=0,
                unresolved_bounds=bounds,
                source_progress=lambda: _source_progress(frontier),
                deliver_page=lambda _events: None,
            )
            assert result._cursor == frontier


@pytest.mark.parametrize('loss', ['channel', 'generation', 'group'])
def test_join_abort_is_token_specific_on_channel_group_or_generation_loss(
    loss: str,
    tmp_path: Path,
) -> None:
    retention = _FakeRetention()
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation=_SOURCE_GENERATION,
            strict_retention=retention,
        )
        target = coordinator.request(
            JoinRequest(_attempt('strict-loss', join_kind='strict')),
            _channel(ordering='strict'),
        )
        assert isinstance(target, JoinTarget)
        if loss == 'channel':
            coordinator.abort(JoinAbort(target.identity))
        elif loss == 'group':
            coordinator.group_died('editorial', 7)
        else:
            coordinator.group_died('editorial', 7)
            replacement = JoinCoordinator(
                state=state,
                fanout=EventFanout(),
                source_generation='source-spawn-2',
                strict_retention=retention,
            )
            with pytest.raises(JoinProtocolError, match='stale'):
                replacement.confirm(JoinConfirm(target.identity))

    assert retention._calls[-1] == ('abort', target.identity)


@pytest.mark.parametrize(
    ('attempt', 'expected'),
    [
        pytest.param(2, 2.0, id='below-saturation'),
        pytest.param(3, 4.0, id='at-saturation'),
        pytest.param(4096, 4.0, id='far-beyond-overflow'),
    ],
)
def test_retry_delay_saturates_before_overflow(attempt: int, expected: float) -> None:
    delay = _saturating_retry_delay(
        attempt,
        base_secs=1.0,
        max_secs=4.0,
        multiplier=2.0,
    )

    assert isfinite(delay)
    assert delay == expected


def test_source_range_state_supplies_durable_ordering_time_after_buffer_rebuild(
    tmp_path: Path,
) -> None:
    token = iter(('range-101', 'close-101'))
    provider, _sg, _factory = _provider([_event(101)], [])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(strict, 'editorial', 7, holdback_secs=10.0, delivery_floor=100)
    with _open_source(
        tmp_path / 'source.db',
        token_factory=lambda: next(token),
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 101, successful_clock=1.0)

        with _poller(
            state,
            provider,
            strict_buffer=strict,
            successful_clock=_SuccessfulQueryClock(
                clock=lambda: 0.0,
                initial_value=11.0,
            ),
        ) as poller:
            poller._recheck_once(notice.obligation_id)
            poller._recheck_once(notice.obligation_id)

    assert strict._observations[0]._ordering_time == 1.0
    assert [release._event_id for release in strict.pump_many(('editorial',), 11.0)] == [101]


def test_stop_interrupts_fetch_interval_and_retry_waits(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([], RuntimeError('outage'))
    waiter = _Waiter(stop_on_wait=1)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(state, provider, waiter=waiter) as poller:
            poller.run(max_cycles=10)
            assert poller._poller_stopped
            poller.stop()

    assert waiter._set_calls == 1
    assert len(waiter._waits) == 1


def test_strict_epoch_completes_an_exhausted_sparse_suffix(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 110}],
        [{'id': 1}],
        [{'id': 110}],
        [_event(102), _event(107)],
    )
    wall = _Monotonic()
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=60.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):

        def step() -> StrictCatchupResult:
            return reader.strict_step(
                state=state,
                source_progress=lambda: _source_progress(110),
                deliver=_deliver_strict_events(
                    lambda event, _at: _accept_strict_event(state, event)
                ),
                holdback_secs=60.0,
                max_pending_epochs=8,
                join_boundary=110,
            )

        step()
        wall._advance(60.0)
        result = step()

        assert state.snapshot().cursor == 110
        assert state.snapshot().epochs == ()
        assert result.join_ready is True


def test_strict_epoch_delivers_only_the_prefix_below_an_unresolved_range(
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 110}],
        [{'id': 1}],
        [{'id': 110}],
        [_event(event_id) for event_id in range(101, 106)],
    )
    wall = _Monotonic()
    delivered: list[int] = []
    pages: list[tuple[int, ...]] = []
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=60.0) as state,
        _strict_reader(
            provider,
            _SuccessfulQueryClock(clock=wall),
        ) as reader,
    ):
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )
        wall._advance(60.0)

        def deliver(events: Sequence[Event], _ordering_time: float) -> StrictDeliveryOutcome:
            pages.append(tuple(cast('int', event['id']) for event in events))
            for event in events:
                _accept_strict_event(state, event)
                delivered.append(cast('int', event['id']))
            return StrictDeliveryOutcome(
                accepted=True,
                continue_epoch=True,
                consumed=len(events),
            )

        result = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=deliver,
            holdback_secs=60.0,
            max_pending_epochs=8,
            unresolved_bounds=((106, 108),),
        )

        assert result._cursor == 105
        assert pages == [(101, 102, 103, 104, 105)]
        assert delivered == list(range(101, 106))
        assert state.snapshot().epochs[0].state == 'delivering'

    assert sg._calls[-1][1] == [
        ['id', 'greater_than', 100],
        ['id', 'less_than', 106],
    ]


def test_strict_epoch_respects_unresolved_obligation_and_final_join_boundary(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 10_000_000}],
    )
    wall = _Monotonic()
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=60.0) as state,
        _strict_reader(
            provider,
            _SuccessfulQueryClock(clock=wall),
        ) as reader,
    ):
        result = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(10_000_000, source_clock=60.0),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=60.0,
            max_pending_epochs=8,
            unresolved_bounds=((106, 108),),
            join_boundary=105,
        )

        assert result._cursor == 100
        epoch = state.snapshot().epochs[0]
        assert (epoch.low_id, epoch.high_id) == (101, 105)
        assert result._source_frontier == 10_000_000


def test_strict_epoch_restart_resumes_above_the_durable_cursor(
    tmp_path: Path,
) -> None:
    first_provider, _first_sg, _first_factory = _provider(
        [{'id': 1}],
        [{'id': 103}],
        [{'id': 1}],
        [{'id': 103}],
        [_event(event_id) for event_id in range(101, 104)],
    )
    wall = _Monotonic()
    state_path = tmp_path / 'group.db'
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(
            first_provider,
            _SuccessfulQueryClock(clock=wall),
        ) as reader,
    ):
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(103),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )
        wall._advance(60.0)

        def interrupt_after_first(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            if event_id > 101:
                raise RuntimeError('simulated group process crash')
            return _accept_strict_event(state, event)

        with pytest.raises(RuntimeError, match='simulated group process crash'):
            reader.strict_step(
                state=state,
                source_progress=lambda: _source_progress(103),
                deliver=_deliver_strict_events(interrupt_after_first),
                holdback_secs=60.0,
                max_pending_epochs=8,
            )
        assert state.snapshot().cursor == 101
        assert state.snapshot().epochs[0].state == 'delivering'

    restart_provider, restart_sg, _restart_factory = _provider(
        [{'id': 1}],
        [{'id': 103}],
        [_event(102), _event(103)],
    )
    delivered: list[int] = []
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(
            restart_provider,
            _SuccessfulQueryClock(clock=_Monotonic()),
        ) as reader,
    ):

        def deliver_after_restart(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event)
            delivered.append(event_id)
            return outcome

        completed = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(103),
            deliver=_deliver_strict_events(deliver_after_restart),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert completed._cursor == 103
        assert state.snapshot().epochs == ()

    assert delivered == [102, 103]
    assert restart_sg._calls[-1][1] == [
        ['id', 'greater_than', 101],
        ['id', 'less_than', 104],
    ]


def test_strict_epoch_shutdown_after_final_acceptance_does_not_redeliver(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / 'group.db'
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 101}],
        [{'id': 1}],
        [{'id': 101}],
        [_event(101)],
    )
    wall = _Monotonic()
    offered: list[int] = []
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(101),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )
        wall._advance(60.0)

        def accept_then_stop(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event, continue_epoch=False)
            offered.append(event_id)
            return outcome

        stopped = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(101),
            deliver=_deliver_strict_events(accept_then_stop),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert stopped._cursor == 101
        assert offered == [101]
        assert state.snapshot().epochs[0].state == 'delivering'

    restart_provider, _restart_sg, _restart_factory = _provider(
        [{'id': 1}],
        [{'id': 101}],
    )
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(
            restart_provider,
            _SuccessfulQueryClock(clock=_Monotonic()),
        ) as reader,
    ):

        def record_unexpected(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            offered.append(cast('int', event['id']))
            return StrictDeliveryOutcome(accepted=True, continue_epoch=True)

        completed = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(101),
            deliver=_deliver_strict_events(record_unexpected),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert completed._cursor == 101
        assert state.snapshot().epochs == ()

    assert offered == [101]


def test_strict_epoch_shutdown_before_first_event_reoffers_every_event(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / 'group.db'
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 102}],
        [{'id': 1}],
        [{'id': 102}],
        [_event(101), _event(102)],
    )
    wall = _Monotonic()
    offered: list[int] = []
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )
        wall._advance(60.0)

        def reject_delivery(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            offered.append(cast('int', event['id']))
            return StrictDeliveryOutcome(accepted=False, continue_epoch=False)

        stopped = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(reject_delivery),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert stopped._cursor == 100
        assert offered == [101]
        assert state.snapshot().epochs[0].state == 'delivering'

    restart_provider, _restart_sg, _restart_factory = _provider(
        [{'id': 1}],
        [{'id': 102}],
        [_event(101), _event(102)],
    )
    replayed: list[int] = []
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(
            restart_provider,
            _SuccessfulQueryClock(clock=_Monotonic()),
        ) as reader,
    ):

        def accept_replayed(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event)
            replayed.append(event_id)
            return outcome

        completed = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(accept_replayed),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert completed._cursor == 102
        assert state.snapshot().epochs == ()

    assert replayed == [101, 102]


def test_strict_epoch_shutdown_between_events_reoffers_unaccepted_event(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / 'group.db'
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 102}],
        [{'id': 1}],
        [{'id': 102}],
        [_event(101), _event(102)],
    )
    wall = _Monotonic()
    offered: list[int] = []
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )
        wall._advance(60.0)

        def stop_after_first(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            offered.append(event_id)
            if event_id != 101:
                return StrictDeliveryOutcome(accepted=False, continue_epoch=False)
            return _accept_strict_event(state, event, continue_epoch=False)

        stopped = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(stop_after_first),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert stopped._cursor == 101
        assert offered == [101]
        assert state.snapshot().epochs[0].state == 'delivering'

    restart_provider, _restart_sg, _restart_factory = _provider(
        [{'id': 1}],
        [{'id': 102}],
        [_event(102)],
    )
    replayed: list[int] = []
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(
            restart_provider,
            _SuccessfulQueryClock(clock=_Monotonic()),
        ) as reader,
    ):

        def accept_replayed(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event)
            replayed.append(event_id)
            return outcome

        completed = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(accept_replayed),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert completed._cursor == 102
        assert state.snapshot().epochs == ()

    assert replayed == [102]


def test_strict_epoch_step_preserves_capture_time_and_delivers_events(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 110}],
        [{'id': 1}],
        [{'id': 110}],
        [_event(event_id) for event_id in range(101, 111)],
    )
    wall = _Monotonic()
    delivered: list[tuple[int, float]] = []
    with (
        _strict_group(tmp_path / 'group.db') as state,
        _strict_reader(
            provider,
            _SuccessfulQueryClock(clock=wall),
        ) as reader,
    ):
        captured = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=_deliver_strict_events(
                lambda _event, _ordering_time: StrictDeliveryOutcome(
                    accepted=True,
                    continue_epoch=True,
                )
            ),
            holdback_secs=300.0,
            max_pending_epochs=8,
        )
        assert not captured.join_ready
        epoch = state.snapshot().epochs[0]
        assert (epoch.low_id, epoch.high_id) == (101, 110)
        assert epoch.captured_at == 0.0
        assert epoch.eligibility_point == 300.0

        wall._advance(300.0)

        def deliver_completed(event: Event, ordering_time: float) -> StrictDeliveryOutcome:
            outcome = _accept_strict_event(state, event)
            delivered.append((cast('int', event['id']), ordering_time))
            return outcome

        completed = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=_deliver_strict_events(deliver_completed),
            holdback_secs=300.0,
            max_pending_epochs=8,
        )

        assert completed.join_ready
        assert completed._cursor == 110
        assert state.snapshot().epochs == ()

    assert delivered == [(event_id, 0.0) for event_id in range(101, 111)]


@pytest.mark.parametrize('continue_epoch', [False, True])
def test_strict_exhaustion_evidence_tracks_the_new_durable_fence(
    continue_epoch: bool,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 110}],
        [{'id': 1}],
        [{'id': 110}],
        [_event(102), _event(104)],
    )
    wall = _Monotonic()
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=60.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=_deliver_strict_events(lambda event, _at: _accept_strict_event(state, event)),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )
        epoch_id = state.snapshot().epochs[0].epoch_id
        wall._advance(60.0)

        def progress_with_fence() -> SourceProgress:
            connection = vars(state)['_database'].connection
            insert_range(connection, 'range-106', 106, 108)
            bind_group(connection, 'range-106', 'editorial', 7, floor=100, eligibility_point=1)
            return _source_progress(110)

        def deliver(event: Event, _at: float) -> StrictDeliveryOutcome:
            accepted = _accept_strict_event(state, event)
            return StrictDeliveryOutcome(accepted=accepted.accepted, continue_epoch=continue_epoch)

        result = reader.strict_step(
            state=state,
            source_progress=progress_with_fence,
            deliver=_deliver_strict_events(deliver),
            holdback_secs=60.0,
            max_pending_epochs=8,
            unresolved_bounds=(),
        )
        assert state.strict_obligation_fence() == ('range-106', 106, 108)
        assert result.exhausted_through == (105 if continue_epoch else None)
        assert result.exhausted_fence == (('range-106', 106, 108) if continue_epoch else None)
        assert result.exhausted_epoch_id == (epoch_id if continue_epoch else None)
        assert result._cursor == (104 if continue_epoch else 102)
        assert not result.join_ready


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('attempt_token', 'foreign-token'),
        ('group', 'render'),
        ('group_generation', 8),
        ('join_kind', 'eager'),
        ('site', 'other-site'),
        ('source', 'other-source'),
    ],
)
def test_strict_join_rejects_foreign_retry_without_recording_attempt(
    field: str,
    value: object,
    tmp_path: Path,
) -> None:
    attempt = _attempt('retry-identity', join_kind='strict')
    foreign_attempt = replace(attempt, **{field: value})
    retention = _FakeRetention(
        response=JoinRetry(
            attempt=foreign_attempt,
            retry_after_secs=1.0,
            headroom_events=0,
            headroom_bytes=0,
        )
    )
    channel = _channel(ordering='strict')
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation=_SOURCE_GENERATION,
            strict_retention=retention,
        )
        request = JoinRequest(attempt)

        with pytest.raises(JoinProtocolError, match='retry'):
            coordinator.request(request, channel)

        retention._response = None
        assert isinstance(coordinator.request(request, channel), JoinTarget)


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('headroom_bytes', -1),
        ('headroom_bytes', True),
        ('headroom_bytes', 1.5),
        ('headroom_events', -1),
        ('headroom_events', True),
        ('headroom_events', 1.5),
        ('retry_after_secs', -1.0),
        ('retry_after_secs', float('inf')),
        ('retry_after_secs', float('nan')),
        pytest.param('retry_after_secs', 10**1000, id='oversized-retry-delay'),
        ('retry_after_secs', True),
        ('retry_after_secs', 'soon'),
    ],
)
def test_join_retry_rejects_invalid_bounds_at_construction(
    field: str,
    value: object,
) -> None:
    valid = JoinRetry(
        attempt=_attempt('retry-bounds', join_kind='strict'),
        retry_after_secs=1.0,
        headroom_events=0,
        headroom_bytes=0,
    )
    with pytest.raises(ValueError, match='retry'):
        replace(valid, **{field: value})


@pytest.mark.parametrize('replay', [False, True])
def test_catchup_batches_only_eligible_page_prefixes(replay: bool) -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}],
        [_event(101), _event(102)],
        [_event(103), _event(104)],
    )
    pages: list[list[int]] = []

    def deliver_page(events: Sequence[Event]) -> None:
        pages.append([cast('int', event['id']) for event in events])

    with _reader(provider) as reader:
        result = reader.catch_up(
            cursor=104 if replay else 100,
            frontier=104,
            delivery_floor=101 if replay else 100,
            source_progress=lambda: _source_progress(104),
            deliver_page=deliver_page,
            replay_from=101 if replay else None,
        )
    assert result._cursor == 104
    assert pages == ([[102], [103, 104]] if replay else [[101, 102], [103, 104]])
    assert not sg._responses


@pytest.mark.parametrize('replay', [False, True])
def test_catchup_services_provider_returns_and_empty_pages(replay: bool) -> None:
    responses: tuple[list[dict[str, object]], ...] = (
        ([{'id': 1}], []) if replay else ([{'id': 1}], [], [], [{'id': 1}])
    )
    provider, sg, _factory = _provider(*responses)
    waiter = _Waiter()
    serviced: list[tuple[float, int]] = []
    with _reader(provider, waiter=waiter) as reader:
        result = reader.catch_up(
            cursor=100,
            frontier=103,
            delivery_floor=100,
            replay_from=101 if replay else None,
            source_progress=lambda: _source_progress(103),
            deliver_page=lambda _events: pytest.fail('empty page delivered'),
            service=lambda seconds: serviced.append((seconds, len(sg._calls))),
        )
    assert result.status == 'caught-up'
    assert {count for seconds, count in serviced if seconds == 0.0} >= set(
        range(1, len(responses) + 1)
    )
    assert [seconds for seconds, _count in serviced if seconds > 0] == ([] if replay else [1.0])
    assert not waiter._waits


def test_strict_join_terminal_histories_remain_bounded_and_exact(
    tmp_path: Path,
) -> None:
    strict = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'editorial': 1.0},
    )
    fanout = EventFanout()
    first: JoinTarget | None = None
    current: JoinTarget | None = None
    current_channel: GroupChannel | None = None
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=fanout,
            source_generation=_SOURCE_GENERATION,
            strict_retention=strict,
        )
        for attempt_number in range(32):
            channel = _channel(ordering='strict')
            target = coordinator.request(
                JoinRequest(_attempt(f'bounded-{attempt_number}', join_kind='strict')),
                channel,
            )
            assert isinstance(target, JoinTarget)
            assert coordinator.confirm(JoinConfirm(target.identity)) == Attached(target.identity)
            assert coordinator.finalize(JoinFinalize(target.identity)) == JoinFinalized(
                target.identity
            )
            if first is None:
                first = target
            current = target
            current_channel = channel
            if attempt_number < 31:
                coordinator.abort(JoinAbort(target.identity))

        assert first is not None
        assert current is not None
        assert current_channel is not None
        assert len(coordinator._records) == 1
        assert len(strict._join_records) == 1
        assert coordinator.finalize(JoinFinalize(current.identity)) == JoinFinalized(
            current.identity
        )
        with pytest.raises(JoinProtocolError, match='stale'):
            coordinator.finalize(JoinFinalize(first.identity))
        coordinator.abort(JoinAbort(first.identity))
        assert current_channel.status.attached


def test_strict_join_uses_fixed_boundary_and_retention_call_order(
    tmp_path: Path,
) -> None:
    retention = _FakeRetention()
    channel = _channel(ordering='strict')
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation=_SOURCE_GENERATION,
            strict_retention=retention,
        )
        request = JoinRequest(_attempt('strict-1', join_kind='strict'))

        target = coordinator.request(request, channel)
        assert isinstance(target, JoinTarget)
        assert target.identity.boundary == 200
        state.commit_observations((201, 202), successful_clock=1.0)
        assert coordinator.request(request, channel) == target
        attached = coordinator.confirm(JoinConfirm(target.identity))
        finalized = coordinator.finalize(JoinFinalize(target.identity))

    assert attached == Attached(target.identity)
    assert finalized == JoinFinalized(target.identity)
    assert [name for name, _value in retention._calls] == ['reserve', 'prepare', 'finalize']


@pytest.mark.parametrize('slow_call', [1, 2, 3])
@pytest.mark.parametrize('continuation', ['resume', 'stop', 'halt'])
def test_strict_prepared_page_services_slow_queries_and_retains_suffix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, slow_call: int, continuation: str
) -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}], [{'id': 103}], [_event(event_id) for event_id in range(101, 104)]
    )
    wall = _Monotonic()
    deadline = [0.5]
    serviced: list[int] = []
    delivered: list[int] = []
    find = sg.find

    def slow_find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        result = find(entity_type, filters, fields, **options)
        if len(sg._calls) == slow_call:
            wall._advance(1.0)
        return result

    monkeypatch.setattr(sg, 'find', slow_find)
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=1.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):
        _seed_delivering_epoch(state)

        def service(seconds: float) -> None:
            assert seconds == 0.0
            serviced.append(len(sg._calls))
            if wall() >= deadline[0]:
                deadline[0] = wall() + 0.5
            if continuation == 'halt' and delivered and not state.snapshot().parked:
                state.park(
                    reason='maintenance',
                    identity=AuditIdentity.current(),
                    wall_clock_time=1.0,
                    expected_revision=state.snapshot().revision,
                )

        def deliver(events: Sequence[Event], _at: float) -> StrictDeliveryOutcome:
            assert wall() < deadline[0], 'provider return did not service the due tick'
            _accept_strict_event(state, events[0])
            delivered.append(cast('int', events[0]['id']))
            wall._advance(0.6)
            return StrictDeliveryOutcome(
                accepted=True,
                continue_epoch=continuation != 'stop',
                consumed=1,
            )

        result = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(103),
            deliver=deliver,
            holdback_secs=1.0,
            max_pending_epochs=8,
            service=service,
        )
        assert delivered == ([101, 102, 103] if continuation == 'resume' else [101])
        assert result._cursor == delivered[-1]
        assert bool(state.snapshot().epochs) == (continuation != 'resume')
        if continuation != 'resume':
            assert state.snapshot().reader_clock_checkpoint == result._reader_clock
        assert set(serviced) >= {1, 2, 3}
    assert len(sg._calls) == 3


@pytest.mark.parametrize(
    ('change', 'return_call'),
    [
        ('fence', 3),
        ('epoch', 3),
        *(
            (identity, call)
            for identity in ('group_generation', 'source_generation', 'verified_incarnation')
            for call in (1, 2, 3)
        ),
    ],
)
@pytest.mark.parametrize('empty', [False, True])
def test_strict_prepared_page_invalidates_changed_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str, return_call: int, empty: bool
) -> None:
    provider, sg, _factory = _provider([{'id': 1}], [{'id': 103}], [] if empty else [_event(101)])
    changed = False
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=1.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=_Monotonic())) as reader,
    ):
        _seed_delivering_epoch(state)

        def service(_seconds: float) -> None:
            nonlocal changed
            if len(sg._calls) != return_call or changed:
                return
            changed = True
            snapshot = state.snapshot()
            if change == 'epoch':
                state.complete_epoch(
                    snapshot.epochs[0].epoch_id, 103, expected_revision=snapshot.revision
                )
            elif change != 'fence':
                original_snapshot = state.snapshot
                replacement = 8 if change == 'group_generation' else 'replacement-source'
                monkeypatch.setattr(
                    state,
                    'snapshot',
                    lambda: replace(original_snapshot(), **{change: replacement}),
                )
            else:
                connection = vars(state)['_database'].connection
                insert_range(connection, 'range-102', 102, 103)
                bind_group(connection, 'range-102', 'editorial', 7, floor=100, eligibility_point=1)

        result = reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(103, source_clock=99.0),
            deliver=lambda _events, _at: pytest.fail('stale prepared page delivered'),
            holdback_secs=1.0,
            max_pending_epochs=8,
            service=service,
        )
        assert changed
        assert result.exhausted_through is None
        assert result.exhausted_epoch_id is None
        if change in {'source_generation', 'verified_incarnation'}:
            assert result.source_generation == state.snapshot().source_generation
            assert result.stream_incarnation == state.snapshot().verified_incarnation
        assert state.snapshot().cursor == (103 if change == 'epoch' else 100)
    assert len(sg._calls) == return_call


def test_strict_retry_allows_later_boundary_only_for_fresh_token(
    tmp_path: Path,
) -> None:
    retention = _FakeRetention(retry=True)
    with _open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation=_SOURCE_GENERATION,
            strict_retention=retention,
        )
        retry = coordinator.request(
            JoinRequest(_attempt('retry-1', join_kind='strict')),
            _channel(ordering='strict'),
        )
        assert isinstance(retry, JoinRetry)
        state.commit_observations((201,), successful_clock=1.0)
        retention._retry = False

        target = coordinator.request(
            JoinRequest(_attempt('retry-2', join_kind='strict')),
            _channel(ordering='strict'),
        )

    assert isinstance(target, JoinTarget)
    assert target.identity.boundary == 201


def test_successful_query_clock_pauses_during_outage_and_resets_anchor(
    tmp_path: Path,
) -> None:
    wall = _Monotonic()
    waiter = _Waiter(clock=wall)
    clock = _SuccessfulQueryClock(clock=wall)
    provider, _sg, _factory = _provider(
        [],
        RuntimeError('outage'),
        [],
        [],
    )
    state_path = tmp_path / 'source.db'
    config = replace(
        _source_config(),
        fetch_interval_secs=10.0,
        poll_retry_base_secs=60.0,
        poll_retry_jitter_fraction=0.0,
        poll_retry_max_secs=60.0,
    )
    with _open_source(state_path) as state:
        state.initialize(100, incarnation='incarnation-1')
        with _poller(
            state,
            provider,
            config=config,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller._poll_once()
            assert state.snapshot().successful_clock == 0.0
            poller.run(max_cycles=1)
            assert waiter._waits == [60.0]
            assert state.snapshot().successful_clock == 0.0
            poller.run(max_cycles=1)
            assert waiter._waits == [60.0, 10.0]
            assert state.snapshot().successful_clock == 0.0
            poller._poll_once()

        assert state.snapshot().successful_clock == 10.0

    with _open_source(state_path) as reopened:
        assert reopened.snapshot().successful_clock == 10.0
