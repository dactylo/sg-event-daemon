from __future__ import annotations

import json
import logging
import os
import select
import sqlite3
import subprocess
import sys
import threading
import time
import weakref
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import ExitStack, closing, nullcontext
from dataclasses import replace
from datetime import datetime
from functools import partial
from math import isfinite, nextafter
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import TYPE_CHECKING, Literal, TypedDict, Unpack, cast
from unittest.mock import Mock

import pytest

import sg_event_daemon._ipc as ipc_module
import sg_event_daemon._source as source_module
import sg_event_daemon._source_store as source_store_module
import sg_event_daemon._worker as worker_module
from sg_event_daemon._config import (
    DEFAULT_STRICT_HOLDBACK_SECS,
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
    EventDelivery,
    EventEncoder,
    EventFanout,
    FanoutStatus,
    GroupChannel,
    GroupChannelInterface,
    StrictOrderingControl,
)
from sg_event_daemon._group_recovery import DispositionRequest
from sg_event_daemon._group_state import SourceProgress
from sg_event_daemon._group_types import (
    AuditIdentity,
    LiveJoinIdentity,
)
from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._ipc import EventClient, EventPeerGrant, EventServer, _JoinSource
from sg_event_daemon._lane import GroupLane
from sg_event_daemon._providers import Provider, ProviderCapabilities, SyncSG, _FindOptions
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
    LiveChannelDetachedError,
    SourceHaltedError,
    SourceHaltStatus,
    SourceHealth,
    SourcePoller,
    StrictDeliveryOutcome,
    _source_halt_code,
    _StrictCatchupResult,
    _StrictJoinRetention,
    _SuccessfulQueryClock,
    history_truncation,
    saturating_retry_delay,
)
from sg_event_daemon._source_state import (
    SourceSnapshot,
    SourceState,
)
from sg_event_daemon._source_store import DiscoveryRange
from sg_event_daemon._sqlite import DatabaseError
from sg_event_daemon._strict import StrictBuffer
from sg_event_daemon._watchdog import ComponentWatchdog, WatchdogSample
from sg_event_daemon._worker import _ComponentWaiter, _LaneRefresh
from sg_event_daemon._write_lock import WriteLockStoppedError, WriteLockTimeoutError
from tests import test_worker as worker_tests
from tests._stall_policy import stall_deadline
from tests._storage_support import (
    bind_group,
    insert_range,
    open_test_group,
    orphan_group,
    write_context_for,
)
from tests._strict_state import strict_observations
from tests.source._support import EventLogSG, stored_range_details
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


class Monotonic:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ScriptedSG:
    def __init__(self, responses: Sequence[list[dict[str, object]] | BaseException]) -> None:
        self.calls: list[
            tuple[
                str,
                object,
                list[str] | None,
                int,
                list[dict[str, str]] | None,
                int,
            ]
        ] = []
        self.closed = False
        self.config = _FakeConfig()
        self.responses = deque(responses)
        self._session_uuid: str | None = None

    def close(self) -> None:
        self.closed = True

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
        self.calls.append((entity_type, filters, fields, limit, order, page))
        response = self.responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response

    def set_session_uuid(self, session_uuid: str | None) -> None:
        self._session_uuid = session_uuid


class SyncFactory:
    def __init__(self, handle: ScriptedSG) -> None:
        self.calls: list[tuple[str, str, str, bool]] = []
        self._handle = handle

    def __call__(
        self,
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> object:
        self.calls.append((site, script_name, api_key, connect))
        return self._handle


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
        self.calls: list[tuple[str, object]] = []
        self.response = response
        self.retry = retry

    def abort(self, identity: LiveJoinIdentity) -> None:
        self.calls.append(('abort', identity))

    def acknowledge_control(
        self,
        control: StrictOrderingControl,
    ) -> None:
        self.calls.append(('acknowledge-control', control))

    def finalize(self, identity: LiveJoinIdentity) -> JoinFinalized:
        self.calls.append(('finalize', identity))
        return JoinFinalized(identity)

    def prepare(
        self,
        identity: LiveJoinIdentity,
        channel: GroupChannelInterface,
    ) -> Attached:
        self.calls.append(('prepare', (identity, channel)))
        return Attached(identity)

    def reserve(self, identity: LiveJoinIdentity) -> JoinTarget | JoinRetry:
        self.calls.append(('reserve', identity))
        if self.response is not None:
            return self.response
        if self.retry:
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
        self.encode_counts: list[int] = []
        self._encoded_ids = encoded_ids
        self.published: list[tuple[EncodedEvent, ...]] = []
        self.snapshots: list[SourceSnapshot] = []
        self.hint_calls = 0
        self._state = state

    def publish_events(self, events: Iterable[EncodedEvent]) -> tuple[FanoutStatus, ...]:
        if self._encoded_ids is not None:
            self.encode_counts.append(len(self._encoded_ids))
        forwarded_events = tuple(events)
        self.published.append(forwarded_events)
        if self._state is not None:
            self.snapshots.append(self._state.snapshot())
        return super().publish_events(forwarded_events)

    def publish_range_changed(self) -> None:
        self.hint_calls += 1
        super().publish_range_changed()


class _ReaderServiceOptions(TypedDict, total=False):
    service: Callable[[float], None]


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
    max_pending_group_obligations: int
    max_group_obligations: int
    max_range_query_pages: int
    max_source_late_ids: int


class _Waiter:
    def __init__(
        self,
        stop_on_wait: int | None = None,
        *,
        clock: Monotonic | None = None,
    ) -> None:
        self._clock = clock
        self.set_calls = 0
        self._stop_on_wait = stop_on_wait
        self.waits: list[float] = []

    def set(self) -> None:
        self.set_calls += 1

    def wait(self, seconds: float) -> bool:
        self.waits.append(seconds)
        if self._clock is not None:
            self._clock.advance(seconds)
        return self._stop_on_wait == len(self.waits)


class _PollerOptions(TypedDict, total=False):
    config: SourceConfig | None
    fanout: EventFanout | None
    random_value: Callable[[], float] | None
    successful_clock: _SuccessfulQueryClock | None
    strict_buffer: StrictBuffer | None
    waiter: _Waiter | None
    watchdog: ComponentWatchdog | None
    service: Callable[[], None]
    query_service: Callable[[], None] | None


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


def _provider(
    *responses: list[dict[str, object]] | BaseException,
) -> tuple[Provider, ScriptedSG, SyncFactory]:
    handle = ScriptedSG(responses)
    factory = SyncFactory(handle)
    provider = Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        sync_factory=factory,
        async_factory=None,
    )
    return provider, handle, factory


def _read_child_stdout(process: subprocess.Popen[str]) -> str:
    stdout = process.stdout
    assert stdout is not None
    readable, _writable, _exceptional = select.select(
        (stdout,),
        (),
        (),
        None,
    )
    assert readable, 'child stdout closed without readiness'
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
        late_event_window_secs=10.0,
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


def _wait_signal(event: threading.Event, owner: Future[None] | threading.Thread) -> bool:
    while not event.is_set():
        if isinstance(owner, Future):
            if owner.done() and not event.is_set():
                owner.result()
                pytest.fail('source operation completed before its signal')
        else:
            assert owner.is_alive() or event.is_set(), 'source thread exited before its signal'
        time.sleep(0.001)
    return True


def _watchdog_token() -> str:
    return os.urandom(16).hex()


def make_event(event_id: int, **changes: object) -> dict[str, object]:
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


def make_poller(
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
                startup_deadline_secs=stall_deadline('startup', 10.0),
            )
            if watchdog is None
            else watchdog
        ),
        internal_deadline_secs=stall_deadline('coordination', 10.0),
        wait_tolerance_secs=stall_deadline('phase', 5.0),
        service=options.get('service', lambda: None),
        query_service=options.get('query_service'),
    )


def open_source(
    path: Path,
    **options: Unpack[_SourceOpenOptions],
) -> SourceState:
    groups = options.get('groups')
    return _open_source_fixture(
        path,
        groups={'editorial': 7} if groups is None else groups,
        delivery_floors=options.get('delivery_floors'),
        group_holdbacks=options.get('group_holdbacks'),
        late_event_window_secs=10.0,
        max_open_ranges=options.get('max_open_ranges', 100),
        max_group_obligations=options.get('max_group_obligations', 1_000),
        max_range_query_pages=options.get('max_range_query_pages', 2),
        max_pending_group_obligations=options.get('max_pending_group_obligations', 100),
        max_source_late_ids=options.get('max_source_late_ids', 100),
        max_ipc_frame_bytes=options.get('max_ipc_frame_bytes', 8_388_608),
        commit_batch_max_bytes=options.get(
            'commit_batch_max_bytes',
            67_108_864,
        ),
    )


def _exercise_blocked_servicing_poller(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    stop: bool,
) -> tuple[list[WatchdogSample], list[threading.Thread], list[str]]:
    provider, sg, _factory = _provider([])
    entered, release, serviced = threading.Event(), threading.Event(), threading.Event()
    samples: list[WatchdogSample] = []
    threads: list[threading.Thread] = []
    calls: list[str] = []
    pollers: list[SourcePoller] = []
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=10,
    )
    original = sg.find

    def find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        calls.append('find')
        threads.append(threading.current_thread())
        entered.set()
        assert release.wait()
        calls.append('return')
        return original(entity_type, filters, fields, **options)

    def close() -> None:
        calls.append('close')
        assert release.is_set()

    def service() -> None:
        samples.append(watchdog.sample())
        if len(samples) >= 2:
            serviced.set()

    monkeypatch.setattr(sg, 'find', find)
    monkeypatch.setattr(sg, 'close', close)

    def run() -> None:
        with open_source(tmp_path / 'source.db') as state:
            state.initialize(100, incarnation='incarnation-1')
            with make_poller(state, provider, watchdog=watchdog, query_service=service) as poller:
                pollers.append(poller)
                poller.run(max_cycles=1)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert _wait_signal(entered, future)
            observed = _wait_signal(serviced, future)
            if stop:
                pollers[0].stop()
            assert not future.done()
            assert calls == ['find']
        finally:
            release.set()
        future.result()
    assert observed, 'source owner did not service sockets while provider was blocked'
    return samples, threads, calls


def test_catchup_batch_flushes_prefix_before_gap_on_failure() -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}],
        [make_event(101), make_event(103)],
        [make_event(102)],
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
    assert not sg.responses


def test_catchup_reader_clock_pauses_during_outage_and_resumes_durably(
    tmp_path: Path,
) -> None:
    wall = Monotonic()
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
        assert clock.value == 0.0
        with pytest.raises(RuntimeError, match='reader outage'):
            reader.latest_visible_id()
        waiter.wait(60.0)
        assert reader.latest_visible_id() == 100
        assert clock.value == 0.0
        waiter.wait(10.0)
        assert reader.latest_visible_id() == 100
        assert clock.value == 10.0

        reader.strict_step(
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
        assert state.snapshot().reader_clock_checkpoint == 10.0

    with _strict_group(state_path, holdback_secs=70.0) as reopened:
        restart_provider, _restart_sg, _restart_factory = _provider(
            [{'id': 1}],
            [{'id': 101}],
        )
        restart_clock = _SuccessfulQueryClock(clock=Monotonic())
        with _strict_reader(restart_provider, restart_clock) as reader:
            reader.strict_step(
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

        assert reopened.snapshot().reader_clock_checkpoint == 10.0


def test_catchup_reads_every_bounded_page_before_advancing_cursor() -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}],
        [make_event(101), make_event(102)],
        [make_event(103), make_event(104)],
        [make_event(105)],
    )
    delivered: list[int] = []

    with _reader(provider) as reader:
        reader.catch_up(
            cursor=100,
            frontier=105,
            delivery_floor=100,
            source_progress=lambda: _source_progress(105),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )

    assert delivered == [101, 102, 103, 104, 105]
    assert all(
        clause[1] in {'greater_than', 'less_than'}
        for call in sg.calls[1:]
        for clause in cast('list[list[object]]', call[1])
    )
    assert [call[1] for call in sg.calls[1:]] == [
        [['id', 'greater_than', 100], ['id', 'less_than', 106]],
        [['id', 'greater_than', 102], ['id', 'less_than', 106]],
        [['id', 'greater_than', 104], ['id', 'less_than', 106]],
    ]


def test_catchup_retention_floor_ignores_the_discarded_delivery_prefix() -> None:
    provider, _sg, _factory = _provider(
        [{'id': 151}],
        [make_event(151), make_event(152)],
    )
    delivered: list[int] = []

    with _reader(provider) as reader:
        reader.catch_up(
            cursor=100,
            frontier=152,
            delivery_floor=150,
            source_progress=lambda: _source_progress(152),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )

    assert delivered == [151, 152]


def test_clean_strict_publications_do_not_rescan_ranges(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([], [])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    with open_source(tmp_path / 'source.db') as state:
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
        with make_poller(state, provider, strict_buffer=strict) as poller:
            assert poller._poll_once() == 0
            assert poller._poll_once() == 0

    assert range_scans == 0


def test_deadline_page_chunk_that_fills_range_ends_scan(tmp_path: Path) -> None:
    rows = [make_event(101), make_event(102), make_event(103)]
    provider, sg, _factory = _provider(rows)
    encoded = [
        EncodedEvent.from_event_frame(cast('int', row['id']), encode_event(Event(row)))
        for row in rows
    ]
    config = _source_config(
        batch_size=3, commit_batch_max_bytes=max(item.byte_count for item in encoded)
    )
    fanout = _RecordingFanout()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 103, successful_clock=1.0)
        state.record_missing_rows(gap.obligation_id, (102, 103), successful_clock=2.0)
        clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
        state.record_successful_query(11.0)
        with make_poller(
            state, provider, fanout=fanout, config=config, successful_clock=clock
        ) as poller:
            assert poller._recheck_once(gap.obligation_id) == 3
            assert poller.health.status == 'running'
            assert poller.health.retry_attempt == 0
        retained = state.range_for(gap.obligation_id)
        assert retained is not None
        assert (retained.state, retained.late_id_count, retained.scan_next_id) == (
            'closed',
            3,
            102,
        )
        assert [[item.event_id for item in chunk] for chunk in fanout.published] == [[101]]
        assert len(sg.calls) == 1


def test_deadline_scan_records_over_the_late_id_limit(tmp_path: Path) -> None:
    provider, sg, _factory = _provider([], [make_event(102)], [])
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with open_source(tmp_path / 'source.db', max_source_late_ids=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        revision = open_source_range(state, 101, 103, successful_clock=1.0)
        record_source_late_ids(state, revision.obligation_id, (101,), successful_clock=11)
        with make_poller(state, provider, successful_clock=clock, waiter=waiter) as poller:
            poller.run(max_cycles=1)
            retained = state.snapshot()
            assert retained.frontier == 104
            assert retained.status == 'running'
            assert retained.ranges[0].late_id_count == 2
            assert retained.ranges[0].state == 'closed'
            assert poller.health.retry_attempt == 0
            assert poller._paused_since is None
    assert len(sg.calls) == 3
    assert waiter.waits == [1.0]


def test_delayed_old_token_messages_never_touch_new_attempt(tmp_path: Path) -> None:
    fanout = EventFanout()
    with open_source(tmp_path / 'source.db') as state:
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
        [make_event(103)],
    )
    delivered: list[int] = []
    with _reader(provider) as reader:
        reader.catch_up(
            cursor=100,
            frontier=103,
            delivery_floor=102,
            source_progress=lambda: _source_progress(103),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )

    assert delivered == [103]


def test_eager_join_captures_frontier_and_queues_subsequent_events_provisionally(
    tmp_path: Path,
) -> None:
    fanout = EventFanout()
    channel = _channel()
    with open_source(tmp_path / 'source.db') as state:
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
    with open_source(tmp_path / 'source.db') as state:
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
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        snapshots = 0
        original = state.snapshot

        def counted_snapshot() -> SourceSnapshot:
            nonlocal snapshots
            snapshots += 1
            return original()

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        with make_poller(state, provider) as poller:
            assert poller._poll_once() == 0

    assert snapshots == 0


def test_event_oversize_halts_before_affected_frontier_commit(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([make_event(101)])
    config = _source_config(max_event_bytes=10)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, config=config) as poller:
            with pytest.raises(SourceHaltedError, match='event-too-large'):
                poller._poll_once()
            assert poller.health.status == 'event-too-large'
        assert state.snapshot().frontier == 100


def test_expired_stamp_is_a_poll_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, sg, _factory = _provider([])
    waiter = _Waiter()
    attempts: list[bool] = []

    def expired(_state: SourceState) -> None:
        attempts.append(True)
        raise WriteLockTimeoutError('stamp expired', 'SQLite')

    monkeypatch.setattr(SourceState, 'stamp_generation', expired, raising=False)
    with (
        open_source(tmp_path / 'expired-stamp.db') as state,
        make_poller(state, provider, waiter=waiter) as poller,
    ):
        poller.run(max_cycles=2)
        assert len(attempts) == 2
        assert not state.stamped
        assert poller._last_error == 'stamp expired'
        assert poller._retry_attempt == 0
        assert poller._next_retry_secs == poller._config.poll_retry_base_secs
        assert waiter.waits == [1.0, 1.0]
        assert not sg.calls


def test_failed_missing_recheck_does_not_retry_inside_fetch_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider, _, _ = _provider()
    now = [0.0]
    attempted: list[float] = []
    monkeypatch.setattr(source_module, 'time', SimpleNamespace(monotonic=lambda: now[0]))
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider) as poller:

            def poll_page() -> int:
                now[0] = (0.0, 0.1, 1.1)[len(pages)]
                pages.append(now[0])
                return poller._config.batch_size

            def recheck() -> None:
                attempted.append(now[0])
                if len(attempted) == 1:
                    raise RuntimeError('provider recheck failed')

            pages: list[float] = []
            monkeypatch.setattr(poller, '_poll_once', poll_page)
            monkeypatch.setattr(poller, '_recheck_missing_once', recheck)
            poller.run(max_cycles=3)
    assert attempted == [0.0, 1.1]


def test_fetch_id_runs_bounds_request_size() -> None:
    sg = EventLogSG(())
    provider, _unused, _factory = _provider()
    reader = _reader(provider)
    reader._handle = cast('SyncSG', sg)
    runs = tuple((event_id, event_id) for event_id in range(1, 301, 2))
    pages = list(reader.fetch_id_runs(runs))
    assert [page.covered_through for page in pages] == [199, 299]
    assert sg.calls[0][0] == [
        {
            'filter_operator': 'any',
            'filters': [['id', 'between', low, high] for low, high in runs[:100]],
        }
    ]
    assert sg.calls[1][0] == [
        {
            'filter_operator': 'any',
            'filters': [['id', 'between', low, high] for low, high in runs[100:]],
        }
    ]


def test_fetch_id_runs_pages_by_coverage() -> None:
    sg = EventLogSG(make_event(event_id) for event_id in (10, 11, 12, 20, 30, 31))
    sg.visible -= {11, 31}
    provider, _unused, _factory = _provider()
    reader = _reader(provider)
    reader._handle = cast('SyncSG', sg)
    pages = list(reader.fetch_id_runs(((10, 12), (20, 20), (30, 31))))
    assert [([event['id'] for event in page.events], page.covered_through) for page in pages] == [
        ([10, 12], 12),
        ([20, 30], 30),
        ([], 31),
    ]
    assert sg.calls[-1][0] == [{'filter_operator': 'any', 'filters': [['id', 'between', 31, 31]]}]
    assert all(
        limit == 2 and order == [{'field_name': 'id', 'direction': 'asc'}]
        for _, limit, order in sg.calls
    )


def test_finalize_and_abort_retries_are_idempotent(tmp_path: Path) -> None:
    fanout = EventFanout()
    channel = _channel()
    with open_source(tmp_path / 'source.db') as state:
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


def test_first_rejection_after_restart_warns(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with open_source(path, max_open_ranges=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1)
        assert state.commit_observations((120,), successful_clock=2).pause is not None
    wall = Monotonic()
    monkeypatch.setattr(source_module, 'time', SimpleNamespace(monotonic=wall))
    logger = logging.getLogger('sg_event_daemon')
    monkeypatch.setattr(logger, 'handlers', [caplog.handler])
    monkeypatch.setattr(logger, 'propagate', False)
    caplog.set_level(logging.INFO, logger='sg_event_daemon')
    provider, _sg, _factory = _provider([make_event(120)], [make_event(120)])
    with open_source(path, max_open_ranges=1) as state, make_poller(state, provider) as poller:
        wall.advance(2.0)
        assert poller._poll_once() == 0
        state._settings = replace(state._settings, max_open_ranges=2)
        wall.advance(3.0)
        assert poller._poll_once() == 1
    records = [record for record in caplog.records if record.name == 'sg_event_daemon.source']
    expected_warning = (
        'source paused: max_open_ranges limit 1 reached (2); open gaps continue and new gaps wait'
    )
    assert [(record.levelname, record.getMessage()) for record in records] == [
        ('WARNING', expected_warning),
        ('INFO', 'source resumed after 5.0s paused'),
    ]


def test_full_advancing_batch_refetches_immediately_but_partial_waits(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [make_event(101), make_event(102)],
        [],
    )
    waiter = _Waiter()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, waiter=waiter) as poller:
            poller.run(max_cycles=2)

    assert waiter.waits == [1.0]


def test_full_rejected_page_still_waits_fetch_interval(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([make_event(104), make_event(106)], [])
    waiter = _Waiter()
    with open_source(tmp_path / 'source.db', max_open_ranges=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1)
        with make_poller(state, provider, waiter=waiter) as poller:
            poller.run(max_cycles=1)
            assert poller.health.retry_attempt == 0
    assert waiter.waits == [1.0]


def test_health_from_mutation_snapshot_avoids_another_snapshot_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([])
    with open_source(tmp_path / 'source.db') as state:
        snapshot_calls = 0
        snapshot = state.initialize(100, incarnation='incarnation-1')
        real_snapshot = state.snapshot

        def counted_snapshot() -> SourceSnapshot:
            nonlocal snapshot_calls
            snapshot_calls += 1
            return real_snapshot()

        monkeypatch.setattr(state, 'snapshot', counted_snapshot)
        with make_poller(
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
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(
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


def test_health_sums_range_late_id_counts(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([])
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        state.commit_observations((103, 106), successful_clock=1.0)
        record_source_late_ids(state, 'range-101-102', (101, 102), successful_clock=2.0)
        record_source_late_ids(state, 'range-104-105', (105,), successful_clock=3.0)
        with make_poller(state, provider) as poller:
            assert poller.health_from_snapshot(state.snapshot()).late_id_count == 3


def test_higher_main_row_releases_when_late_only_publication_closes_range(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([make_event(105)], [make_event(101), make_event(104)])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    channel = _channel(ordering='strict')
    attach_strict_group(
        strict, 'editorial', 7, holdback_secs=1.0, delivery_floor=100, channel=channel
    )
    fanout = EventFanout()
    fanout.attach(channel)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, fanout=fanout, strict_buffer=strict) as poller:
            assert poller._poll_once() == 1
            assert not any(isinstance(message, EventDelivery) for message in channel.drain())
            retained = state.snapshot().ranges[0]
            assert (retained.low_id, retained.high_id) == (101, 104)
            state.record_successful_query(
                stored_range_details(state, retained.obligation_id).visibility_deadline
            )
            assert poller._recheck_once(retained.obligation_id) == 2
            closed = state.range_for(retained.obligation_id)
            assert closed is not None
            assert closed.state == 'closed'
            assert [
                message.encoded.event_id
                for message in channel.drain()
                if isinstance(message, EventDelivery)
            ] == [105]


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
        assert str(error) == (
            f'history-truncated: no visible rows for required id {required}'
            if no_visible_rows
            else f'history-truncated: retention floor {expected_floor} exceeds {required}'
        )


def test_join_abort_requires_a_fresh_attempt_token(tmp_path: Path) -> None:
    with open_source(tmp_path / 'source.db') as state:
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
    with open_source(tmp_path / 'source.db') as state:
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
    with open_source(tmp_path / 'last-error.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider) as poller:
            assert poller._last_error is None
            poller.run(max_cycles=1)
            with monkeypatch.context() as patch:
                patch.setattr(state, 'snapshot', Mock(side_effect=AssertionError('full snapshot')))
                assert poller._last_error == 'provider outage'
            poller.run(max_cycles=1)
            with monkeypatch.context() as patch:
                patch.setattr(state, 'snapshot', Mock(side_effect=AssertionError('full snapshot')))
                assert poller._last_error is None


def test_late_row_recorded_then_published_before_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sg = EventLogSG((make_event(101), make_event(103), make_event(105)))
    provider, _, _ = _provider()
    fanout = EventFanout()
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    channel = _channel()
    fanout.attach(channel)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        first = open_source_range(state, 101, 102, successful_clock=1.0)
        second = open_source_range(state, 104, 105, successful_clock=1.0)
        publish = fanout.publish_events

        def publish_committed(events: Sequence[EncodedEvent]) -> tuple[FanoutStatus, ...]:
            connection = state._database.connection
            assert not connection.in_transaction
            for event in events:
                assert (
                    connection.execute(
                        'SELECT 1 FROM source_late_ids WHERE event_id = ?', (event.event_id,)
                    ).fetchone()
                    is not None
                )
            return publish(events)

        monkeypatch.setattr(fanout, 'publish_events', publish_committed)
        with make_poller(state, provider, fanout=fanout, strict_buffer=strict) as poller:
            poller._handle = cast('SyncSG', sg)
            poller.run(max_cycles=1)
            retained_range = state.range_for(first.obligation_id)
            assert retained_range is not None
            assert retained_range.late_id_count == 1
            retained_range = state.range_for(second.obligation_id)
            assert retained_range is not None
            assert retained_range.late_id_count == 1
            assert [
                m.encoded.event_id for m in channel.drain() if isinstance(m, EventDelivery)
            ] == [101, 105]
            assert all(
                stored_range_details(state, r.obligation_id).scan_started_at is None
                for r in state.snapshot().ranges
            )
            assert all(r.state == 'open' for r in state.snapshot().ranges)
            assert strict.event_count == 0


def test_live_strict_epoch_spacing_prevents_early_capture(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 110}],
        [{'id': 1}],
        [{'id': 120}],
        [{'id': 1}],
        [{'id': 120}],
    )
    wall = Monotonic()
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
        wall.advance(42.0)

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

        wall.advance(1.0)
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
        [make_event(event_id) for event_id in range(101, 111)],
    )
    wall = Monotonic()
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
        wall.advance(299.0)

        def deliver_waiting(event: Event, ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event)
            delivered.append((event_id, ordering_time))
            return outcome

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=_deliver_strict_events(deliver_waiting),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert state.snapshot().cursor == 100
        assert delivered == []
        assert state.snapshot().epochs[0].eligibility_point == 300.0

        wall.advance(1.0)
        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=_deliver_strict_events(deliver_waiting),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert state.snapshot().cursor == 110
        assert delivered == [(event_id, 0.0) for event_id in range(101, 111)]
        assert state.snapshot().epochs == ()


def test_lock_expiry_is_a_poll_failure_without_backoff_growth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, _sg, _factory = _provider([])
    waiter = _Waiter()
    stop = threading.Event()
    pumped: list[float] = []
    slices: list[float] = []
    wall = Monotonic()
    monkeypatch.setattr(worker_module, 'time', SimpleNamespace(monotonic=wall))

    def observe_wait(seconds: float | None = None) -> bool:
        assert seconds is not None
        slices.append(seconds)
        wall.advance(seconds)
        return stop.is_set()

    monkeypatch.setattr(stop, 'wait', observe_wait)
    pump = _ComponentWaiter(stop)
    pump.configure_service(lambda: pumped.append(wall()))
    config = replace(_source_config(), poll_retry_base_secs=0.03)

    errors = iter(
        [
            WriteLockTimeoutError('expired', 'SQLite'),
            WriteLockTimeoutError('expired', 'file'),
            RuntimeError('provider failed'),
        ]
    )

    def fail_poll(_poller: SourcePoller) -> int:
        raise next(errors)

    monkeypatch.setattr(SourcePoller, '_poll_once', fail_poll)
    with (
        open_source(tmp_path / 'source.db') as state,
        make_poller(state, provider, waiter=waiter) as poller,
    ):
        poller._config = config
        poller._waiter = pump
        poller._retry_attempt = 1
        poller.run(max_cycles=2)
        assert poller._retry_attempt == 1
        assert poller._last_error == 'expired'
        assert poller._next_retry_secs == 0.03
        assert len(pumped) >= 6
        assert all(0 < seconds <= 0.01 for seconds in slices)
        poller.run(max_cycles=1)
        assert poller._retry_attempt == 2
        assert poller._next_retry_secs == pytest.approx(0.06)


def test_lowered_late_id_limit_pauses_instead_of_halting(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with open_source(path) as state:
        state.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(state, 101, 103, successful_clock=1)
        record_source_late_ids(state, opened.obligation_id, (101, 102), successful_clock=2)
    provider, _sg, _factory = _provider([make_event(106)])
    with open_source(path, max_source_late_ids=1) as state, make_poller(state, provider) as poller:
        assert poller._poll_once() == 0
        assert state.snapshot().status == 'late-capacity-exhausted'
        assert state.snapshot().frontier == 104
        assert state.snapshot().ranges[0].late_id_count == 2


def test_main_publication_synchronizes_retired_range(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([make_event(101)])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(strict, 'editorial', 7, holdback_secs=10.0, delivery_floor=100)
    strict.synchronize_ranges(
        (
            DiscoveryRange(
                obligation_id='range-101',
                low_id=101,
                high_id=101,
                opened_successful_at=0.0,
                state='open',
                late_id_count=1,
                scan_next_id=101,
            ),
        )
    )
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, strict_buffer=strict) as poller:
            assert poller._poll_once() == 1

    assert strict._ranges == {}
    assert [item.event_id for item in strict_observations(strict)] == [101]


def test_no_join_request_is_dispatched_during_a_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A full-service dispatch from inside find would run this owner callback.
    provider, sg, _factory = _provider([])
    serviced: list[str] = []
    socket_serviced = threading.Event()
    original = sg.find

    def find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        assert socket_serviced.wait()
        return original(entity_type, filters, fields, **options)

    def service_socket() -> None:
        serviced.append('socket')
        socket_serviced.set()

    monkeypatch.setattr(sg, 'find', find)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(
            state,
            provider,
            service=lambda: serviced.append('dispatch'),
            query_service=service_socket,
        ) as poller:
            poller.run(max_cycles=1)
    assert serviced
    assert set(serviced) == {'socket'}


def test_obligation_pause_clears_when_open_ranges_finish(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([make_event(104)], [], [], [make_event(104)], [])
    wall = Monotonic()
    waiter = _Waiter(clock=wall)
    clock = _SuccessfulQueryClock(clock=wall, initial_value=11)
    with open_source(tmp_path / 'source.db', max_pending_group_obligations=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1)
        with make_poller(state, provider, successful_clock=clock, waiter=waiter) as poller:
            poller.run(max_cycles=2)
            assert state.snapshot().frontier == 104
            assert state.snapshot().status == 'running'
            assert state.snapshot().pending_obligations == 2
            assert poller._paused_since is None


def test_poll_once_joined_peer_write_failure_preserves_frontier(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([make_event(101)], [make_event(102)])
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
            self._sock = sock

        def close(self) -> None:
            self._sock.close()

        def fileno(self) -> int:
            return self._sock.fileno()

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
        coordination_deadline_secs=stall_deadline('coordination', 1.0),
        stop=threading.Event(),
        wait_phase=nullcontext,
        wait_slice_secs=0.1,
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
                while not joined.done():
                    server.poll(0.001)
                    server.dispatch_join_requests(source)
                assert joined.result() == JoinTarget(identity)
            (channel,) = remotes
            peers = cast('dict[int, object]', vars(server)['_peers'])
            peer = next(peer for peer in peers.values() if vars(peer)['role'] == 'event')
            vars(peer)['socket'] = BrokenSender(cast('socket.socket', vars(peer)['socket']))
            with open_source(tmp_path / 'source.db') as state:
                state.initialize(100, incarnation='incarnation-1')
                with make_poller(state, provider, fanout=fanout) as poller:
                    assert poller._poll_once() == 1
                    assert channel.status.queued_count == 1
                    assert channel.status.attached
                    server.poll(0.001)
                    assert not channel.status.attached
                    assert channel.status.reason == 'connection-replaced'
                    assert server._group_status('editorial').reason == 'peer-write-failed'
                    assert channel.status.queued_count == 0
                    assert channel.status.queued_bytes == 0
                    assert state.snapshot().frontier == 101
                    assert poller._poll_once() == 1
                assert state.snapshot().frontier == 102
        finally:
            client.close()


def test_poller_adoption_reinitializes_and_resumes_queries(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with open_source(path) as source:
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
    wall = Monotonic()
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
        make_poller(
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
        assert clock.value == 0.0
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
        assert clock.value == 0.0
        waiter.wait(10.0)
        assert poller._poll_once() == 0
        assert state.snapshot().successful_clock == 10.0

    assert [call[1] for call in sg.calls] == [
        [],
        [],
        [['id', 'greater_than', 41]],
    ]


def test_poller_blocked_encoder_receive_publishes_internal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([make_event(101)])
    entered = threading.Event()
    release = threading.Event()
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=stall_deadline('startup', 10.0),
    )
    original_encode = EventEncoder.encode

    def blocked_encode(worker: EventEncoder, event: Event) -> EncodedEvent:
        entered.set()
        assert release.wait()
        return original_encode(worker, event)

    monkeypatch.setattr(EventEncoder, 'encode', blocked_encode)

    def run() -> None:
        with open_source(tmp_path / 'source.db') as state:
            state.initialize(100, incarnation='incarnation-1')
            with make_poller(state, provider, watchdog=watchdog) as poller:
                poller._poll_once()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert _wait_signal(entered, future)
            sample = watchdog.sample()
            assert sample.phase == 'internal'
            assert sample.deadline_at is not None
        finally:
            release.set()
        future.result()


def test_poller_blocked_fanout_publishes_internal(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([make_event(101)])
    entered = threading.Event()
    release = threading.Event()
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=stall_deadline('startup', 10.0),
    )

    class _BlockingFanout(EventFanout):
        def publish_events(self, events: Iterable[EncodedEvent]) -> tuple[FanoutStatus, ...]:
            entered.set()
            assert release.wait()
            return super().publish_events(events)

    def run() -> None:
        with open_source(tmp_path / 'source.db') as state:
            state.initialize(100, incarnation='incarnation-1')
            with make_poller(
                state,
                provider,
                fanout=_BlockingFanout(),
                watchdog=watchdog,
            ) as poller:
                poller._poll_once()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert _wait_signal(entered, future)
            sample = watchdog.sample()
            assert sample.phase == 'internal'
            assert sample.deadline_at is not None
        finally:
            release.set()
        future.result()


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
        startup_deadline_secs=stall_deadline('startup', 10.0),
    )
    original_find = sg.find

    def blocked_find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        entered.set()
        assert release.wait()
        return original_find(entity_type, filters, fields, **options)

    monkeypatch.setattr(sg, 'find', blocked_find)

    def run() -> None:
        with open_source(
            tmp_path / 'source.db',
        ) as state:
            state.initialize(100, incarnation='incarnation-1')
            notice = open_source_range(state, 101, 101, successful_clock=1.0)

            state.record_successful_query(11.0)
            with make_poller(state, provider, watchdog=watchdog) as poller:
                poller._recheck_once(notice.obligation_id)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert _wait_signal(entered, future)
            sample = watchdog.sample()
            assert sample.phase == 'provider-query'
            assert sample.deadline_at is not None
        finally:
            release.set()
        future.result()


def test_poller_blocked_provider_creation_publishes_provider_query(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    handle = ScriptedSG(())

    def factory(
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> ScriptedSG:
        assert site == _SITE_URL
        assert script_name == 'event-reader'
        assert api_key == 'secret'
        assert not connect
        entered.set()
        assert release.wait()
        return handle

    provider = Provider(
        name='shotgun-api3',
        capabilities=ProviderCapabilities(sync=True, async_=False),
        sync_factory=factory,
        async_factory=None,
    )
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=stall_deadline('startup', 10.0),
    )
    with open_source(tmp_path / 'source.db') as state:
        poller = make_poller(state, provider, watchdog=watchdog)
        thread = threading.Thread(target=poller.__enter__)
        thread.start()
        try:
            assert _wait_signal(entered, thread)
            sample = watchdog.sample()
            assert sample.phase == 'provider-query'
            assert sample.deadline_at is not None
            assert sample.heartbeat_at < sample.deadline_at
        finally:
            release.set()
            thread.join()
            assert not thread.is_alive()
            poller.close()


def test_poller_commits_range_membership_before_event_fanout(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([make_event(102)])
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        fanout = _RecordingFanout(state)
        channel = _channel()
        fanout.attach(channel)
        with make_poller(state, provider, fanout=fanout) as poller:
            poller._poll_once()
        published = fanout.snapshots[0]
        assert published.frontier == 102
        assert published.ranges[0].low_id == 101
        assert published.pending_obligations == 1
        assert [
            message.encoded.event_id
            for message in channel.drain()
            if isinstance(message, EventDelivery)
        ] == [102]


def test_poller_halts_changed_stream_before_initialized_query(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with open_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')

    changed_stream = stream_fingerprint(
        _SITE_URL,
        'restricted',
        'event-reader',
    )
    provider, sg, _factory = _provider([make_event(101)])
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
        make_poller(state, provider) as poller,
    ):
        with pytest.raises(SourceHaltedError, match='stream-identity-changed'):
            poller._poll_once()

        assert poller.health.status == 'stream-identity-changed'
        assert state.snapshot().frontier == 100

    assert sg.calls == []


def test_poller_halts_strict_release_in_the_collapse_cycle(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([make_event(104)])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    channel = _channel(ordering='strict')
    attach_strict_group(
        strict, 'editorial', 7, holdback_secs=1, delivery_floor=100, channel=channel
    )
    with open_source(tmp_path / 'source.db', max_group_obligations=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=0)
        fanout = EventFanout()
        fanout.attach(channel)
        with make_poller(state, provider, fanout=fanout, strict_buffer=strict) as poller:
            poller._poll_once()
        assert state.snapshot().replay_markers
        strict.synchronize_ranges(())
        strict.observe_many((EncodedEvent.from_event_frame(105, b'event'),), successful_time=1)
        assert strict.pump_many(('editorial',), 10) == ()


def test_poller_hints_after_disjoint_range_opens_and_close_only(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider(
        [make_event(102)], [make_event(104)], [make_event(105)], [], []
    )
    with open_source(
        tmp_path / 'source.db',
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        fanout = _RecordingFanout(state)
        with make_poller(state, provider, fanout=fanout) as poller:
            poller._poll_once()
            assert fanout.hint_calls == 1
            poller._poll_once()
            assert fanout.hint_calls == 2
            poller._poll_once()
            poller._poll_once()
            assert fanout.hint_calls == 2
            retained = state.snapshot().ranges[0]
            state.record_successful_query(312.0)
            state.begin_range_scan(retained.obligation_id, successful_clock=312.0)
            poller._recheck_once(retained.obligation_id)
            assert fanout.hint_calls == 3
            poller._recheck_once(retained.obligation_id)
            assert fanout.hint_calls == 3


def test_poller_initializes_from_latest_visible_id_through_provider_boundary(
    tmp_path: Path,
) -> None:
    wall = Monotonic()
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
        open_source(tmp_path / 'source.db') as state,
        make_poller(
            state,
            provider,
            config=config,
            successful_clock=clock,
            waiter=waiter,
        ) as poller,
    ):
        poller.run(max_cycles=2)
        assert waiter.waits == [60.0, 10.0]
        assert state.snapshot().frontier == 41
        assert state.snapshot().initialized
        assert poller._poll_once() == 0
        assert state.snapshot().successful_clock == 10.0

    assert factory.calls == [(_SITE_URL, 'event-reader', 'secret', False)]
    assert sg.calls == [
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
    assert sg.closed


def test_poller_publishes_count_bounded_chunk_before_encoding_next_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([make_event(101), make_event(102), make_event(103)])
    encoded_ids: list[int] = []
    original_encode = EventEncoder.encode

    def record_encode(worker: EventEncoder, event: Event) -> EncodedEvent:
        encoded_ids.append(cast('int', event['id']))
        return original_encode(worker, event)

    monkeypatch.setattr(EventEncoder, 'encode', record_encode)
    fanout = _RecordingFanout(encoded_ids=encoded_ids)
    config = _source_config(batch_size=2)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, fanout=fanout, config=config) as poller:
            poller._poll_once()

    assert fanout.encode_counts == [2, 3]


def test_poller_queries_main_stream_ascending_with_exact_event_fields(
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider([make_event(101), make_event(102)])
    fanout = _RecordingFanout()
    channel = GroupChannel(
        group='editorial',
        ordering='eager',
        max_events=10,
        max_bytes=67_108_864,
    )
    fanout.attach(channel)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, fanout=fanout) as poller:
            assert poller._poll_once() == 2

        assert state.snapshot().frontier == 102
        messages = channel.drain()
        assert all(isinstance(message, EventDelivery) for message in messages)
        assert [cast('EventDelivery', message).encoded.event_id for message in messages] == [
            101,
            102,
        ]

    assert sg.calls == [
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
    provider, _sg, _factory = _provider([make_event(101)])
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
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        clock = _SuccessfulQueryClock(clock=lambda: 0.0)
        with make_poller(
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
    assert [release.event_id for release in strict.pump_many(('editorial',), 10.0)] == [101]
    queued = channel.drain()
    assert len(queued) == 1
    assert isinstance(queued[0], EventDelivery)


def test_poller_splits_provider_result_by_complete_frame_bytes(
    tmp_path: Path,
) -> None:
    events = [make_event(101), make_event(102), make_event(103)]
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
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, fanout=fanout, config=config) as poller:
            assert poller._poll_once() == 3

        assert [len(events) for events in fanout.published] == [2, 1]
        assert all(
            sum(event.byte_count for event in events) <= config.commit_batch_max_bytes
            for events in fanout.published
        )
        assert state.snapshot().frontier == 103


def test_post_commit_process_exit_recovers_body_through_catchup(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / 'source.db'
    with open_source(source_path) as source:
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
        return_code = process.wait()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        if process.stdin is not None:
            process.stdin.close()
        if process.stdout is not None:
            process.stdout.close()
    assert return_code is not None
    assert return_code != 0

    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [make_event(101), make_event(102)],
    )
    delivered: list[int] = []
    with open_source(source_path) as source, _reader(provider) as reader:
        snapshot = source.snapshot()
        reader.catch_up(
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
    assert delivered == [101, 102]


def test_production_strict_publication_fills_and_refills_after_lane_drain(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [make_event(event_id) for event_id in range(101, 126)],
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
    clock = Monotonic()
    with (
        open_source(
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
                startup_deadline_secs=stall_deadline('startup', 10.0),
            ),
            token_factory=lambda: 'production-strict-join',
        )
        assert lane.setup() is None
        joined = lane.join(coordinator, channel, catch_up=lambda _boundary: None)
        assert isinstance(joined, LiveJoinIdentity)

        with make_poller(
            source_state,
            provider,
            fanout=fanout,
            config=source_config,
            successful_clock=_SuccessfulQueryClock(clock=clock),
            strict_buffer=strict,
        ) as poller:
            assert poller._poll_once() == 25
            assert channel.status.queued_count == 0
            clock.advance(1.0)
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


@pytest.mark.parametrize('remote', [False, True])
@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('reason', ['queue-count-exhausted', 'channel-closed'])
def test_promotion_detach_aborts_the_record_and_raises_its_reason(
    tmp_path: Path,
    remote: bool,
    ordering: Literal['eager', 'strict'],
    reason: str,
) -> None:
    server = Mock(spec=EventServer)
    server.connection_status.return_value = SimpleNamespace(
        connected=True, queued_count=0, queued_bytes=0, reason=None
    )
    channel = (
        ipc_module._RemoteGroupChannel(
            server, connection_id='connection', group='editorial', ordering=ordering
        )
        if remote
        else _channel(ordering=ordering)
    )
    retention = _FakeRetention()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=EventFanout(),
            source_generation=_SOURCE_GENERATION,
            strict_retention=retention,
        )
        request = JoinRequest(_attempt('promotion-detach', join_kind=ordering))
        target = coordinator.request(request, channel)
        assert isinstance(target, JoinTarget)
        coordinator.confirm(JoinConfirm(target.identity))
        channel.detach(reason)
        channel.close()
        with pytest.raises(LiveChannelDetachedError, match=reason) as caught:
            coordinator.finalize(JoinFinalize(target.identity))
        assert caught.value.reason == reason
        record = coordinator._records[target.identity.attempt_token]
        assert record.phase == 'aborted'
        assert record.requires_fresh
        assert record.finalized is None
        assert target.identity.group not in coordinator._active
        coordinator.abort(JoinAbort(target.identity))
        assert [call for call in retention.calls if call[0] == 'abort'] == (
            [('abort', target.identity)] if ordering == 'strict' else []
        )
        assert channel.status.reason == reason
        with pytest.raises(JoinProtocolError, match='fresh'):
            coordinator.request(request, channel)


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('error_type', [ConnectionError, EncoderHaltedError, ValueError])
def test_promotion_failure_without_a_detach_preserves_the_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    error_type: type[Exception],
) -> None:
    retention = _FakeRetention()
    fanout = EventFanout()
    error = error_type('promotion defect')

    def failed_promote(_group: str, _channel: GroupChannelInterface) -> None:
        raise error

    with open_source(tmp_path / 'source.db') as state:
        state.initialize(200, incarnation='incarnation-1')
        coordinator = JoinCoordinator(
            state=state,
            fanout=fanout,
            source_generation=_SOURCE_GENERATION,
            strict_retention=retention,
        )
        channel = _channel(ordering=ordering)
        target = coordinator.request(JoinRequest(_attempt('defect', join_kind=ordering)), channel)
        assert isinstance(target, JoinTarget)
        coordinator.confirm(JoinConfirm(target.identity))
        monkeypatch.setattr(fanout, 'promote', failed_promote)
        with pytest.raises(error_type, match='promotion defect') as caught:
            coordinator.finalize(JoinFinalize(target.identity))
        assert caught.value is error
        assert [call for call in retention.calls if call[0] == 'abort'] == (
            [('abort', target.identity)] if ordering == 'strict' else []
        )


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
        startup_deadline_secs=stall_deadline('startup', 10.0),
    )
    original_find = sg.find

    def blocked_find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        entered.set()
        assert release.wait()
        return original_find(entity_type, filters, fields, **options)

    monkeypatch.setattr(sg, 'find', blocked_find)

    def run() -> None:
        with open_source(tmp_path / 'source.db') as state:
            if initialized:
                state.initialize(100, incarnation='incarnation-1')
            with make_poller(state, provider, watchdog=watchdog) as poller:
                poller._poll_once()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert _wait_signal(entered, future)
            sample = watchdog.sample()
            assert sample.phase == 'provider-query'
            assert sample.deadline_at is not None
            assert sample.heartbeat_at < sample.deadline_at
        finally:
            release.set()
        future.result()


@pytest.mark.parametrize('ids', [(9,), (13,), (11,), (12, 10)])
def test_fetch_id_runs_rejects_invalid_provider_rows(ids: tuple[int, ...]) -> None:
    provider, _sg, _factory = _provider([make_event(event_id) for event_id in ids])
    with _reader(provider) as reader, pytest.raises(ValueError, match=r'outside|ascending'):
        list(reader.fetch_id_runs(((10, 10), (12, 12))))


@pytest.mark.parametrize('counts', [(2, 2, 2, 2), (2, 1, 2, 2)])
def test_catchup_rechecks_at_most_once_per_fetch_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, counts: tuple[int, ...]
) -> None:
    provider, _, _ = _provider()
    now = [0.0]
    rechecks: list[float] = []
    monkeypatch.setattr(source_module, 'time', SimpleNamespace(monotonic=lambda: now[0]))
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider) as poller:

            def poll_page() -> int:
                now[0] = (0.0, 0.1, 0.2, 1.1)[len(pages)]
                pages.append(now[0])
                return counts[len(pages) - 1]

            pages: list[float] = []
            monkeypatch.setattr(poller, '_poll_once', poll_page)
            monkeypatch.setattr(poller, '_recheck_missing_once', lambda: rechecks.append(now[0]))
            poller.run(max_cycles=4)
    assert rechecks == [0.0, 1.1]


def test_range_cap_projects_degraded_health_without_unrelated_detach(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider([make_event(104)])
    fanout = EventFanout()
    channel = GroupChannel(
        group='editorial',
        ordering='eager',
        max_events=10,
        max_bytes=67_108_864,
    )
    fanout.attach(channel)
    with open_source(
        tmp_path / 'source.db',
        max_open_ranges=1,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        with make_poller(state, provider, fanout=fanout) as poller:
            assert poller._poll_once() == 0
            assert poller.health.status == 'range-capacity-exhausted'
        assert channel.status.attached
        assert state.snapshot().frontier == 102


def test_range_closes_on_commit_that_fills_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger('sg_event_daemon')
    monkeypatch.setattr(logger, 'handlers', [caplog.handler])
    monkeypatch.setattr(logger, 'propagate', False)
    caplog.set_level(logging.INFO, logger='sg_event_daemon')
    provider, sg, _factory = _provider([make_event(101), make_event(102)])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    fanout = _RecordingFanout()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 102, successful_clock=1.0)
        clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=3.0)
        with make_poller(
            state, provider, fanout=fanout, strict_buffer=strict, successful_clock=clock
        ) as poller:
            poller._recheck_missing_once()
            retained = state.range_for(gap.obligation_id)
            assert retained is not None
            assert (retained.state, retained.late_id_count) == ('closed', 2)
            assert strict._ranges[gap.obligation_id].closed
            assert fanout.hint_calls == 1
            poller._recheck_missing_once()
            assert len(sg.calls) == 1
        assert state.successful_clock() == 3.0
    records = [record for record in caplog.records if record.name == 'sg_event_daemon.source']
    assert [(record.levelname, record.getMessage()) for record in records] == [
        ('INFO', 'late events in IDs 101-102 all appeared; gap closed after 2.0s'),
    ]
    assert (vars(records[0])['site'], vars(records[0])['source']) == (_SITE, _SOURCE)


def test_range_recheck_clock_pauses_during_query_outage(tmp_path: Path) -> None:
    wall = Monotonic()
    waiter = _Waiter(clock=wall)
    clock = _SuccessfulQueryClock(
        clock=wall,
        initial_value=11.0,
    )
    provider, _sg, _factory = _provider(
        RuntimeError('range outage'),
        [make_event(101)],
        [make_event(102)],
    )
    config = replace(_source_config(), max_range_query_pages=1)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        state.record_successful_query(11.0)
        notice = open_source_range(state, 101, 103, successful_clock=11.0)

        with make_poller(
            state,
            provider,
            config=config,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            with pytest.raises(RuntimeError, match='range outage'):
                poller._recheck_once(notice.obligation_id)
            assert clock.value == 11.0
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
        [make_event(101)],
        [make_event(102), make_event(103)],
    )
    config = _source_config(batch_size=2)
    with open_source(
        tmp_path / 'source.db',
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 103, successful_clock=1.0)

        state.record_successful_query(11.0)
        fanout = _RecordingFanout(state)
        with make_poller(state, provider, config=config, fanout=fanout) as poller:
            assert poller._recheck_once(notice.obligation_id) == 3

        assert state.snapshot().ranges[0].late_id_count == 3
        assert state.snapshot().ranges[0].state == 'closed'
        assert fanout.snapshots[0].ranges[0].late_id_count == 1
        assert fanout.snapshots[0].ranges[0].state == 'open'
        assert fanout.snapshots[-1].ranges[0].late_id_count == 3
        assert fanout.snapshots[-1].ranges[0].state == 'closed'

    assert sg.calls == [
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
    with open_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(source, 101, 101, successful_clock=1.0)

    changed_stream = stream_fingerprint(
        _SITE_URL,
        'restricted',
        'event-reader',
    )
    provider, sg, _factory = _provider([make_event(101)])
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
        make_poller(state, provider) as poller,
    ):
        before = state.snapshot()

        with pytest.raises(SourceHaltedError, match='stream-identity-changed'):
            poller._recheck_once(notice.obligation_id)

        assert state.snapshot() == before

    assert sg.calls == []


def test_range_recheck_persists_keyset_budget_across_reopens(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    calls: list[tuple[str, object, list[str] | None, int, list[dict[str, str]] | None, int]] = []
    provider, sg, _factory = _provider(
        [make_event(101), make_event(102)],
        RuntimeError('interrupted range scan'),
    )
    with open_source(
        path,
        max_range_query_pages=2,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 105, successful_clock=1.0)

        state.record_successful_query(11.0)
        with (
            make_poller(state, provider) as poller,
            pytest.raises(RuntimeError, match='interrupted range scan'),
        ):
            poller._recheck_once(notice.obligation_id)
        calls.extend(sg.calls)
        first_page = state.snapshot().ranges[0]
        first_details = stored_range_details(state, first_page.obligation_id)
        assert (
            first_page.scan_next_id,
            first_details.scan_pages,
            first_page.scan_cycle_pages,
            first_details.degraded,
        ) == (103, 1, 1, False)

    provider, sg, _factory = _provider([make_event(103), make_event(104)])
    with open_source(path, max_range_query_pages=2) as state:
        reopened = state.snapshot().ranges[0]
        reopened_details = stored_range_details(state, reopened.obligation_id)
        assert (
            reopened.scan_next_id,
            reopened_details.scan_pages,
            reopened.scan_cycle_pages,
            reopened_details.degraded,
        ) == (103, 1, 1, False)
        with make_poller(state, provider) as poller:
            assert poller._recheck_once(notice.obligation_id) == 2
        calls.extend(sg.calls)
        capped = state.snapshot().ranges[0]
        capped_details = stored_range_details(state, capped.obligation_id)
        assert (
            capped.state,
            capped.scan_next_id,
            capped_details.scan_pages,
            capped.scan_cycle_pages,
            capped_details.degraded,
        ) == ('open', 105, 2, 2, True)
        assert state.snapshot().conditions == ('range-query-page-cap',)

    provider, sg, _factory = _provider([make_event(105)])
    with open_source(path, max_range_query_pages=2) as state:
        reopened = state.snapshot().ranges[0]
        reopened_details = stored_range_details(state, reopened.obligation_id)
        assert (
            reopened.scan_next_id,
            reopened_details.scan_pages,
            reopened.scan_cycle_pages,
            reopened_details.degraded,
        ) == (105, 2, 2, True)
        assert state.snapshot().conditions == ('range-query-page-cap',)
        with make_poller(state, provider) as poller:
            assert poller._recheck_once(notice.obligation_id) == 1
        calls.extend(sg.calls)
        completed = state.snapshot().ranges[0]
        completed_details = stored_range_details(state, completed.obligation_id)
        assert (
            completed.state,
            completed.late_id_count,
            completed_details.degraded,
        ) == ('closed', 5, False)
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
    events = (make_event(101), make_event(102))
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
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 102, successful_clock=1.0)

        fanout = _RecordingFanout(state)

        with make_poller(state, provider, fanout=fanout, config=config) as poller:
            assert poller._recheck_once(notice.obligation_id) == 2

        assert [len(events) for events in fanout.published] == [1, 1]
        assert all(
            sum(event.byte_count for event in events) <= config.commit_batch_max_bytes
            for events in fanout.published
        )
        assert sum(event.byte_count for event in encoded) > config.commit_batch_max_bytes
        assert state.snapshot().ranges[0].late_id_count == 2
        retained = state.snapshot().ranges[0]
        assert stored_range_details(state, retained.obligation_id).scan_pages == 1


def test_reader_rechecks_floor_before_committing_across_gap() -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [make_event(103)],
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
        [make_event(103)],
        [make_event(102)],
        [],
        [{'id': 1}],
        [make_event(102), make_event(103)],
    )
    waiter = _Waiter()
    delivered: list[int] = []
    with _reader(provider, waiter=waiter) as reader:
        reader.catch_up(
            cursor=100,
            frontier=103,
            delivery_floor=100,
            source_progress=lambda: _source_progress(103),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )

    assert delivered == [102, 103]
    assert waiter.waits == [1.0, 1.0]
    assert not sg.responses


def test_recheck_encoder_failure_halts_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sg = EventLogSG((make_event(101),))
    provider, _, _ = _provider()

    def fail_encode(_encoder: EventEncoder, _event: Event) -> EncodedEvent:
        raise EncoderHaltedError('event-too-large: missing event', code='event-too-large')

    monkeypatch.setattr(EventEncoder, 'encode', fail_encode)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 102, successful_clock=1.0)
        with make_poller(state, provider) as poller:
            poller._handle = cast('SyncSG', sg)
            with pytest.raises(SourceHaltedError, match='event-too-large'):
                poller.run(max_cycles=1)
            assert poller.health.status == 'event-too-large'
            assert poller.health.retry_attempt == 0
            retained_range = state.range_for(gap.obligation_id)
            assert retained_range is not None
            assert retained_range.late_id_count == 0


def test_recheck_provider_failure_retries_without_halt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider, _, _ = _provider(
        [], RuntimeError('missing outage'), [], RuntimeError('missing outage')
    )
    waiter = _Waiter()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 102, successful_clock=1.0)
        with make_poller(state, provider, waiter=waiter) as poller:
            now = [0.0]
            monkeypatch.setattr(source_module, 'time', SimpleNamespace(monotonic=lambda: now[0]))
            poller.run(max_cycles=1)
            now[0] = 1.0
            poller.run(max_cycles=1)
            assert poller.health.status == 'running'
            assert poller.health.retry_attempt == 2
            assert poller._recheck_position == 0
        assert waiter.waits == [1.0, 2.0]


def test_recheck_requests_only_missing_runs(tmp_path: Path) -> None:
    sg = EventLogSG(make_event(i) for i in range(101, 108))
    provider, _, _ = _provider()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 107, successful_clock=1.0)
        record_source_late_ids(state, gap.obligation_id, (102, 104, 106), successful_clock=1.0)
        original = state.range_for(gap.obligation_id)
        assert original is not None
        original_details = stored_range_details(state, original.obligation_id)
        with make_poller(state, provider) as poller:
            poller._handle = cast('SyncSG', sg)
            poller._recheck_missing_once()
            calls = len(sg.calls)
            poller._recheck_missing_once()
        assert len(sg.calls) == calls
        retained_range = state.range_for(gap.obligation_id)
        assert retained_range is not None
        assert retained_range.late_id_count == 7
        current = state.range_for(gap.obligation_id)
        assert current is not None
        assert (
            stored_range_details(state, current.obligation_id).scan_started_at,
            current.scan_next_id,
            current.scan_cycle_pages,
        ) == (
            original_details.scan_started_at,
            original.scan_next_id,
            original.scan_cycle_pages,
        )
        for filters, limit, _ in sg.calls:
            clauses = cast('list[dict[str, list[list[object]]]]', filters)[0]['filters']
            assert len(clauses) <= 100
            assert limit == 2
            assert not any(
                cast('int', low) <= i <= cast('int', high)
                for _, _, low, high in clauses
                for i in (102, 104, 106)
            )


def test_recheck_runs_while_paused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sg = EventLogSG((make_event(101), make_event(102), make_event(105)))
    sg.visible.remove(102)
    provider, _, _ = _provider()
    fanout = EventFanout()
    channel = _channel()
    fanout.attach(channel)
    with open_source(tmp_path / 'source.db', max_open_ranges=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 102, successful_clock=1.0)
        with make_poller(state, provider, fanout=fanout) as poller:
            now = [0.0]
            monkeypatch.setattr(source_module, 'time', SimpleNamespace(monotonic=lambda: now[0]))
            poller._handle = cast('SyncSG', sg)
            poller.run(max_cycles=1)
            assert poller._paused_since is not None
            retained_range = state.range_for(gap.obligation_id)
            assert retained_range is not None
            assert retained_range.late_id_count == 1
            assert [
                m.encoded.event_id for m in channel.drain() if isinstance(m, EventDelivery)
            ] == [101]
            sg.visible.add(102)
            now[0] = 1.0
            poller.run(max_cycles=1)
            assert poller._paused_since is not None
            retained_range = state.range_for(gap.obligation_id)
            assert retained_range is not None
            assert retained_range.late_id_count == 2
            assert [
                m.encoded.event_id for m in channel.drain() if isinstance(m, EventDelivery)
            ] == [102]


def test_recheck_walk_wraps_and_never_sweeps_twice(tmp_path: Path) -> None:
    sg = EventLogSG((make_event(501), make_event(101)))
    sg.visible = {501}
    provider, _, _ = _provider()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 501, successful_clock=1.0)
        record_source_late_ids(
            state, gap.obligation_id, tuple(range(102, 501, 2)), successful_clock=1.0
        )
        with make_poller(
            state, provider, config=replace(_source_config(), max_range_query_pages=1)
        ) as poller:
            poller._handle = cast('SyncSG', sg)
            for position in (299, 499, 501):
                poller._recheck_missing_once()
                assert poller._recheck_position == position
            retained_range = state.range_for(gap.obligation_id)
            assert retained_range is not None
            assert retained_range.late_id_count == 201
            sg.visible.add(101)
            poller._recheck_missing_once()
            retained_range = state.range_for(gap.obligation_id)
            assert retained_range is not None
            assert retained_range.late_id_count == 202
            assert poller._recheck_position == 299
            poller._config = replace(_source_config(), max_range_query_pages=100)
            start = len(sg.calls)
            poller._recheck_missing_once()
            assert len(sg.calls) - start == 2


def test_recheck_wide_fragmented_range_records_cost(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sg = EventLogSG((make_event(1101),))
    durations: list[float] = []
    find = sg.find

    def timed_find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        start = time.perf_counter()
        result = find(entity_type, filters, fields, **options)
        durations.append(time.perf_counter() - start)
        return result

    monkeypatch.setattr(sg, 'find', timed_find)
    provider, _, _ = _provider()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 1101, successful_clock=1.0)
        record_source_late_ids(
            state, gap.obligation_id, tuple(range(102, 1101, 2)), successful_clock=1.0
        )
        with make_poller(
            state, provider, config=replace(_source_config(), max_range_query_pages=100)
        ) as poller:
            poller._handle = cast('SyncSG', sg)
            start = time.perf_counter()
            poller._recheck_missing_once()
            elapsed = time.perf_counter() - start
            retained_range = state.range_for(gap.obligation_id)
            assert retained_range is not None
            retained = retained_range.late_id_count
            assert retained == 501
            assert len(sg.calls) == 6
            assert poller._recheck_position == 1101
            print(
                'MEASUREMENTS '
                + json.dumps(
                    {
                        'query_count': len(sg.calls),
                        'retained_id_count': retained,
                        'completion_progress': poller._recheck_position,
                        'request_durations_secs': durations,
                        'cycle_duration_secs': elapsed,
                    }
                )
            )


def test_rechecks_record_over_the_late_id_limit(tmp_path: Path) -> None:
    sg = EventLogSG((make_event(101), make_event(102), make_event(103)))
    provider, _, _ = _provider()
    with open_source(tmp_path / 'source.db', max_source_late_ids=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 103, successful_clock=1.0)
        with make_poller(state, provider) as poller:
            poller._handle = cast('SyncSG', sg)
            poller._recheck_missing_once()
            assert poller.health.status == 'running'
        retained_range = state.range_for(gap.obligation_id)
        assert retained_range is not None
        assert retained_range.late_id_count == 3


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

    assert fanout.published == [events]
    assert channel.drain() == tuple(EventDelivery(event) for event in events)


def test_refused_suffix_reaches_no_group(tmp_path: Path) -> None:
    provider, sg, _factory = _provider([make_event(102), make_event(104)], [make_event(104)])
    fanout = _RecordingFanout()
    eager = _channel()
    fanout.attach(eager)
    strict = StrictBuffer(max_events=10, max_bytes=100000)
    strict_channel = _channel(group='strict', ordering='strict')
    fanout.attach(strict_channel)
    attach_strict_group(
        strict, 'strict', 1, holdback_secs=100, delivery_floor=100, channel=strict_channel
    )
    with open_source(tmp_path / 'source.db', max_open_ranges=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, fanout=fanout, strict_buffer=strict) as poller:
            assert poller._poll_once() == 0
            assert [event.event_id for page in fanout.published for event in page] == [102]
            assert strict.event_count == 1
            assert [m.encoded.event_id for m in eager.drain() if isinstance(m, EventDelivery)] == [
                102
            ]
            state._close_range('range-101-101', successful_clock=state.successful_clock())
            assert poller._poll_once() == 1
            assert [event.event_id for page in fanout.published for event in page] == [102, 104]
            assert strict.event_count == 2
            assert state.snapshot().frontier == 104
            assert poller._paused_since is None
    assert sg.calls[1][1] == [['id', 'greater_than', 102]]


def test_rejected_polls_keep_durable_clock_advancing(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider(*([make_event(104)] for _ in range(4)))
    reader_provider, reader_sg, _reader_factory = _provider(
        [{'id': 1}], [make_event(101)], [{'id': 1}]
    )
    clock = Monotonic()
    config = _source_config()
    group = GroupConfig(
        name='editorial',
        source=_SOURCE,
        plugins=(),
        callback_credentials='admin',
        ordering='strict',
        strict_holdback_secs=5,
    )
    site = SiteConfig(
        name=_SITE,
        server=_SITE_URL,
        default_source=_SOURCE,
        credentials=MappingProxyType({'admin': _credential()}),
        sources=MappingProxyType({_SOURCE: config}),
        groups=MappingProxyType({'editorial': group}),
        workers=MappingProxyType({}),
    )
    path = tmp_path / 'source.db'
    fanout = _RecordingFanout()
    with open_source(
        path,
        max_source_late_ids=1,
        delivery_floors={'editorial': 100},
        group_holdbacks={'editorial': 5},
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(source, 101, 101, successful_clock=1)
        record_source_late_ids(source, opened.obligation_id, (101,), successful_clock=1)
        source._close_range(opened.obligation_id, successful_clock=1)
        with (
            _strict_group(path, holdback_secs=5) as state,
            make_poller(
                source,
                provider,
                fanout=fanout,
                successful_clock=_SuccessfulQueryClock(clock=clock, initial_value=1),
            ) as poller,
            _reader(reader_provider) as reader,
            closing(
                GroupLane(
                    state=state,
                    site=site,
                    group=group,
                    provider=reader_provider,
                    plugins=(),
                    worker_instance='worker-1',
                    health_interval_secs=1,
                    watchdog=ComponentWatchdog(
                        kind='lane',
                        identity='editorial',
                        worker_instance='worker-1',
                        registration_token=_watchdog_token(),
                        startup_deadline_secs=10,
                    ),
                )
            ) as lane,
        ):
            assert lane.setup() is None
            refresh = _LaneRefresh(
                lane=lane,
                state=state,
                reader=reader,
                slot=nullcontext,
                joined=lambda: False,
                fetch_interval_secs=1,
                late_event_window_secs=10,
                log_extra={},
            )
            for expected_clock in (1, 3, 6):
                clock.now = expected_clock - 1
                assert poller._poll_once() == 0
                assert source.successful_clock() == expected_clock
                assert source.snapshot().frontier == 102
                assert poller._paused_since is not None
                refresh.service()
                if expected_clock < 6:
                    assert state.pending_ranges()
                    assert reader_sg.calls == []
            assert state.pending_ranges() == ()
            assert len(reader_sg.calls) == 3
            assert len(fanout.published) == 3
            assert all(not page for page in fanout.published)
            assert poller._poll_once() == 1
            assert source.snapshot().frontier == 104
            assert source.snapshot().status == 'running'
            assert poller._paused_since is None


@pytest.mark.parametrize('replay_from', [100, 101])
def test_replay_ignores_missing_ids_at_or_below_delivery_floor(replay_from: int) -> None:
    sg = EventLogSG(make_event(event_id) for event_id in (102, 103))
    provider, _unused, _factory = _provider()
    waiter = _Waiter()
    delivered: list[int] = []
    with _reader(provider, waiter=waiter) as reader:
        reader._handle = cast('SyncSG', sg)
        reader.catch_up(
            cursor=103,
            frontier=103,
            delivery_floor=101,
            replay_from=replay_from,
            source_progress=lambda: _source_progress(103),
            deliver_page=lambda events: delivered.extend(
                cast('int', event['id']) for event in events
            ),
        )
    assert delivered == [102, 103]
    assert waiter.waits == []
    assert len(sg.calls) == 2


def test_replay_is_inclusive_rejects_future_and_creates_no_source_range() -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [make_event(50), make_event(52)],
        [],
        [{'id': 1}],
        [make_event(52)],
        [],
        [{'id': 1}],
    )
    delivered: list[int] = []
    with _reader(provider) as reader:
        reader.catch_up(
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

    assert delivered == [50, 52]


@pytest.mark.parametrize(
    'status',
    ['encoder-failed', 'event-too-large', 'frame-too-large'],
)
def test_retained_data_halt_blocks_queries_after_internal_phase(
    tmp_path: Path, status: SourceHaltStatus, monkeypatch: pytest.MonkeyPatch
) -> None:
    warning = Mock()
    monkeypatch.setattr(source_module._LOGGER, 'warning', warning)
    provider, sg, _factory = _provider([])
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider) as poller:
            expected = poller._halt(status, RuntimeError('original halt'))
            with pytest.raises(SourceHaltedError) as caught:
                poller._poll_once()
            assert str(caught.value) == str(expected)
            assert caught.value.code == status
            assert poller._watchdog.sample().phase == 'internal'
    assert sg.calls == []

    assert warning.call_count == 1
    assert 'halted' in warning.call_args.args[0]
    assert warning.call_args.args[3] == status


def test_retry_backoff_jitters_capped_delay_after_large_attempt(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider(
        RuntimeError('long outage'),
    )
    waiter = _Waiter()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(
            state,
            provider,
            random_value=lambda: 1.0,
            waiter=waiter,
        ) as poller:
            poller._retry_attempt = 1024
            poller.run(max_cycles=1)

            assert poller.health.retry_attempt == 1025
            assert poller.health.next_retry_secs == pytest.approx(3.6)

    assert len(waiter.waits) == 1
    assert waiter.waits[-1] == pytest.approx(3.6)


def test_retry_backoff_uses_exact_cap_multiplier_jitter_and_resets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warning = Mock()
    info = Mock()
    monkeypatch.setattr(source_module._LOGGER, 'warning', warning)
    monkeypatch.setattr(source_module._LOGGER, 'info', info)
    provider, _sg, _factory = _provider(
        RuntimeError('outage-1'),
        RuntimeError('outage-2'),
        [],
        RuntimeError('outage-after-success'),
    )
    waiter = _Waiter()
    random_value = _RandomSequence(0.0, 0.5, 1.0, 0.0)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(
            state,
            provider,
            random_value=random_value,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=4)
            assert poller.health.retry_attempt == 1

    assert waiter.waits == [0.8, 2.0, 1.0, 0.8]

    assert warning.call_count == 2
    assert info.call_count == 1
    assert 'poll failed' in warning.call_args_list[0].args[0]
    assert 'recovered' in info.call_args.args[0]


def test_retry_wait_publishes_bounded_delay_and_wait_deadline(tmp_path: Path) -> None:
    watchdog = ComponentWatchdog(
        kind='source',
        identity=_SOURCE,
        worker_instance=_SOURCE_GENERATION,
        registration_token=_watchdog_token(),
        startup_deadline_secs=stall_deadline('startup', 10.0),
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
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(
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
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        state.record_successful_query(11.0)

        with make_poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=1)
            assert poller.health.retry_attempt == 1

        due_range = state.snapshot().ranges[0]
        assert due_range.state == 'open'
        assert stored_range_details(state, due_range.obligation_id).scan_started_at is None
        assert due_range.scan_next_id == 101

    assert len(sg.calls) == 1
    assert waiter.waits == [1.0]


def test_run_propagates_owner_write_stop_without_provider_retry(tmp_path: Path) -> None:
    provider, sg, _factory = _provider([])
    waiter = _Waiter()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, waiter=waiter) as poller:
            original = state._writes
            stop = threading.Event()
            stop.set()
            state._writes = replace(original, policy='one-attempt', stop=stop)
            entry = original.lock._entry
            assert entry is not None
            assert entry.lock.acquire(blocking=False)
            try:
                with pytest.raises(WriteLockStoppedError, match='write lock wait'):
                    poller.run(max_cycles=1)
                assert waiter.waits == []
                assert poller._retry_attempt == 0
                assert poller._last_error is None
            finally:
                entry.lock.release()
                state._writes = original
        assert len(sg.calls) == 0
        assert state.snapshot().frontier == 100


def test_run_propagates_recheck_encoder_halt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider(
        [],
        [make_event(102)],
        [make_event(104)],
        [make_event(102)],
    )
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    encode = EventEncoder.encode

    def encode_late_event(encoder: EventEncoder, event: Event) -> EncodedEvent:
        if event['id'] == 102:
            raise EncoderHaltedError(
                'event-too-large: late event exceeds encoder limit', code='event-too-large'
            )
        return encode(encoder, event)

    monkeypatch.setattr(EventEncoder, 'encode', encode_late_event)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        revision = open_source_range(state, 101, 103, successful_clock=1.0)

        state.record_successful_query(11.0)

        with make_poller(
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
            assert retained.ranges[0].late_id_count == 0
            assert poller.health.status == 'event-too-large'
            assert poller.health.retry_attempt == 0
            assert str(halted.value).startswith('event-too-large:')

    assert len(sg.calls) == 2
    assert waiter.waits == []


def test_run_propagates_stamp_database_failure_without_provider_retry(tmp_path: Path) -> None:
    provider, sg, _factory = _provider([])
    waiter = _Waiter()
    with (
        open_source(tmp_path / 'source.db') as state,
        make_poller(state, provider, waiter=waiter) as poller,
    ):
        state._database.connection.execute('PRAGMA query_only=ON')

        with pytest.raises(DatabaseError, match='readonly database') as caught:
            poller.run(max_cycles=2)

        assert isinstance(caught.value.__cause__, sqlite3.OperationalError)
        assert not state.stamped
        assert waiter.waits == []
        assert poller._retry_attempt == 0
        assert poller._last_error is None
        assert not sg.calls


def test_run_rechecks_at_most_one_range_per_poll_in_due_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, sg, _factory = _provider([], [], [], [], [], [], [], [], [])
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=12.0)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        later_low = open_source_range(state, 101, 101, successful_clock=1.0)
        later_high = open_source_range(state, 201, 201, successful_clock=1.0)
        earliest = open_source_range(state, 301, 301, successful_clock=1.0)
        state._database.connection.execute(
            'UPDATE discovery_ranges SET visibility_deadline = 12 WHERE low_id < 301'
        )

        state.record_successful_query(12.0)

        with make_poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            now = [0.0]
            monkeypatch.setattr(source_module, 'time', SimpleNamespace(monotonic=lambda: now[0]))
            poller.run(max_cycles=1)
            states = {item.obligation_id: item.state for item in state.snapshot().ranges}
            assert states == {
                earliest.obligation_id: 'closed',
                later_high.obligation_id: 'open',
                later_low.obligation_id: 'open',
            }

            now[0] = 1.0
            poller.run(max_cycles=1)
            states = {item.obligation_id: item.state for item in state.snapshot().ranges}
            assert states == {
                earliest.obligation_id: 'closed',
                later_high.obligation_id: 'open',
                later_low.obligation_id: 'closed',
            }

            now[0] = 2.0
            poller.run(max_cycles=1)

        assert all(item.state == 'closed' for item in state.snapshot().ranges)

    assert [call[1] for call in sg.calls[2::3]] == [
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
    assert waiter.waits == [1.0, 1.0, 1.0]


def test_run_rechecks_due_range_after_successful_poll(tmp_path: Path) -> None:
    provider, sg, _factory = _provider([], [], [make_event(101)])
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 101, successful_clock=1.0)

        state.record_successful_query(11.0)

        with make_poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=1)

        completed = state.snapshot().ranges[0]
        assert completed.obligation_id == notice.obligation_id
        assert completed.state == 'closed'

    assert [call[1] for call in sg.calls] == [
        [['id', 'greater_than', 102]],
        [{'filter_operator': 'any', 'filters': [['id', 'between', 101, 101]]}],
        [
            ['id', 'greater_than', 100],
            ['id', 'less_than', 102],
        ],
    ]
    assert waiter.waits == [1.0]


def test_run_reselects_due_range_from_durable_progress_until_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger('sg_event_daemon')
    monkeypatch.setattr(logger, 'handlers', [caplog.handler])
    monkeypatch.setattr(logger, 'propagate', False)
    caplog.set_level(logging.INFO, logger='sg_event_daemon')
    provider, sg, _factory = _provider(
        [],
        [],
        [make_event(101)],
        [],
        [],
        [make_event(103)],
    )
    config = replace(_source_config(), max_range_query_pages=1)
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with open_source(
        tmp_path / 'source.db',
        max_range_query_pages=1,
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 103, successful_clock=1.0)

        state.record_successful_query(11.0)

        with make_poller(
            state,
            provider,
            config=config,
            successful_clock=clock,
        ) as poller:
            now = [0.0]
            monkeypatch.setattr(source_module, 'time', SimpleNamespace(monotonic=lambda: now[0]))
            poller.run(max_cycles=1)
            first_cycle = state.snapshot().ranges[0]
            assert (
                first_cycle.state,
                first_cycle.scan_next_id,
                first_cycle.scan_cycle_pages,
            ) == ('open', 102, 1)

            now[0] = 1.0
            poller.run(max_cycles=1)

        completed = state.snapshot().ranges[0]
        assert completed.state == 'closed'
        assert completed.late_id_count == 2

    assert [call[1] for call in sg.calls[2::3]] == [
        [
            ['id', 'greater_than', 100],
            ['id', 'less_than', 104],
        ],
        [
            ['id', 'greater_than', 101],
            ['id', 'less_than', 104],
        ],
    ]

    records = [record for record in caplog.records if record.name == 'sg_event_daemon.source']
    assert [(record.levelname, record.getMessage()) for record in records] == [
        ('WARNING', 'gave up on 1 event IDs after 10.0s: 102'),
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
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        state.record_successful_query(11.0)

        with make_poller(
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

    assert len(sg.calls) == 4
    assert waiter.waits == [1.0, 2.0]


def test_run_retries_failed_recheck_and_resumes_on_next_poll(tmp_path: Path) -> None:
    provider, sg, _factory = _provider(
        [],
        RuntimeError('recheck outage'),
        [],
        [make_event(101)],
    )
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1.0)
        state.record_successful_query(11.0)

        with make_poller(
            state,
            provider,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller.run(max_cycles=1)
            interrupted = state.snapshot().ranges[0]
            assert interrupted.state == 'open'
            assert stored_range_details(state, interrupted.obligation_id).scan_started_at is None
            assert interrupted.scan_next_id == 101
            assert poller.health.retry_attempt == 1

            poller.run(max_cycles=1)
            assert poller.health.retry_attempt == 0

        assert state.snapshot().ranges[0].state == 'closed'

    assert len(sg.calls) == 4
    assert waiter.waits == [1.0, 1.0]


def test_run_skips_due_range_without_an_active_generation(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with open_source(path, groups={'editorial': 7}) as state:
        state.initialize(100, incarnation='incarnation-1')
        retired = open_source_range(state, 101, 101, successful_clock=1.0)

        with _open_group(path) as group:
            orphan_group(group)

    provider, sg, _factory = _provider([], [], [])
    clock = _SuccessfulQueryClock(clock=lambda: 0.0, initial_value=11.0)
    with open_source(path, groups={'editorial': 8}) as state:
        state._database.connection.execute(
            "UPDATE groups SET group_generation = 8, active = 1, lifecycle = 'active' "
            "WHERE group_name = 'editorial'"
        )
        active = open_source_range(state, 201, 201, successful_clock=1.0)

        state.record_successful_query(11.0)

        with make_poller(state, provider, successful_clock=clock) as poller:
            poller.run(max_cycles=1)

        ranges = {item.obligation_id: item for item in state.snapshot().ranges}
        assert retired.obligation_id not in ranges
        assert ranges[active.obligation_id].state == 'closed'

    assert [call[1] for call in sg.calls] == [
        [['id', 'greater_than', 202]],
        [{'filter_operator': 'any', 'filters': [['id', 'between', 201, 201]]}],
        [
            ['id', 'greater_than', 200],
            ['id', 'less_than', 202],
        ],
    ]


def test_servicing_failure_leaves_polling_as_a_source_owner_failure_after_the_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, sg, _factory = _provider([])
    entered, release, serviced = threading.Event(), threading.Event(), threading.Event()
    error = OSError('socket service failed')
    original = sg.find

    def find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        entered.set()
        assert release.wait()
        return original(entity_type, filters, fields, **options)

    def service() -> None:
        serviced.set()
        raise error

    monkeypatch.setattr(sg, 'find', find)
    waiter = _Waiter()

    def run() -> None:
        with open_source(tmp_path / 'source.db') as state:
            state.initialize(100, incarnation='incarnation-1')
            with make_poller(state, provider, waiter=waiter, query_service=service) as poller:
                poller.run(max_cycles=2)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert _wait_signal(entered, future)
            observed = _wait_signal(serviced, future)
            assert not future.done()
            assert not sg.closed
        finally:
            release.set()
        if observed:
            with pytest.raises(OSError, match='socket service failed') as caught:
                future.result()
            assert caught.value is error
        else:
            future.result()
    assert observed, 'socket failure was never observed during the blocked call'
    assert len(sg.calls) == 1
    assert waiter.waits == []
    assert sg.closed


def test_servicing_handle_never_overlaps_calls_or_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _samples, _threads, calls = _exercise_blocked_servicing_poller(
        tmp_path, monkeypatch, stop=True
    )
    assert calls == ['find', 'return', 'close']


def test_servicing_keeps_the_query_deadline_fixed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    samples, threads, _calls = _exercise_blocked_servicing_poller(
        tmp_path, monkeypatch, stop=False
    )
    assert len(samples) >= 2
    assert {sample.phase for sample in samples} == {'provider-query'}
    assert len({sample.deadline_at for sample in samples}) == 1
    assert len(threads) == 1
    assert not threads[0].is_alive()


def test_source_generation_change_invalidates_cached_finalize(tmp_path: Path) -> None:
    with open_source(tmp_path / 'source.db') as state:
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


def test_source_halt_code_rejects_unknown_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    error = SourceHaltedError('event-too-large', code='event-too-large')
    monkeypatch.setattr(error, 'code', 'future-halt')
    with pytest.raises(RuntimeError, match='unknown source halt code: future-halt') as rejected:
        _source_halt_code(error)
    assert rejected.value.__cause__ is error


def test_sparse_stale_page_that_rejects_keeps_higher_blocked_id(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([make_event(120)], [make_event(110)], [make_event(110)])
    with open_source(tmp_path / 'source.db', max_open_ranges=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(state, 101, 101, successful_clock=1)
        with make_poller(state, provider) as poller:
            assert poller._poll_once() == 0
            assert poller._poll_once() == 0
            assert poller._blocked_id == 120
            state._close_range(opened.obligation_id, successful_clock=state.successful_clock())
            assert poller._poll_once() == 1
            assert state.snapshot().frontier == 110
            assert poller._blocked_id == 120
            assert poller._paused_since is not None
            assert state.snapshot().status == 'range-capacity-exhausted'


def test_stale_pages_below_blocked_id_keep_pause_and_commit_reaching_it_ends_it(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [make_event(110)], [], [make_event(103)], [make_event(104)], [make_event(110)]
    )
    with open_source(tmp_path / 'source.db', max_open_ranges=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(state, 101, 101, successful_clock=1)
        with make_poller(state, provider) as poller:
            assert poller._poll_once() == 0
            for _ in range(3):
                poller._poll_once()
                assert poller._blocked_id == 110
                assert poller._paused_since is not None
                assert state.snapshot().status == 'range-capacity-exhausted'
            state._close_range(opened.obligation_id, successful_clock=state.successful_clock())
            assert poller._poll_once() == 1
            assert state.snapshot().status == 'running'
            assert poller._blocked_id is None
            assert poller._paused_since is None


def test_stamp_is_the_first_write_and_precedes_the_queryable_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, _sg, _factory = _provider([])
    with open_source(tmp_path / 'stamp.db') as state, make_poller(state, provider) as poller:
        trace: list[str] = []
        state._database.connection.set_trace_callback(trace.append)
        original = poller._require_queryable

        def require_queryable() -> None:
            assert state.stamped
            assert poller._watchdog.sample().phase == 'internal'
            writes = [sql for sql in trace if sql.lstrip().startswith('UPDATE')]
            assert writes
            assert 'source_generation' in writes[0]
            original()

        monkeypatch.setattr(poller, '_require_queryable', require_queryable)
        poller._poll_once()
        assert state.stamped


def test_stamp_records_and_clears_a_stream_identity_change(tmp_path: Path) -> None:
    path = tmp_path / 'stamp-identity.db'
    with open_source(path) as state:
        state.initialize(100, incarnation='original')
    provider, _sg, _factory = _provider([])
    with (
        _open_source_fixture(path, configured_stream='changed') as state,
        make_poller(state, provider) as poller,
    ):
        assert state._database.connection.execute(
            'SELECT condition FROM source_meta'
        ).fetchone() == (None,)
        with pytest.raises(SourceHaltedError):
            poller._poll_once()
        assert state.stamped
        assert 'stream-identity-changed' in state.snapshot().conditions
    provider, _sg, _factory = _provider([])
    with open_source(path) as state, make_poller(state, provider) as poller:
        assert 'stream-identity-changed' in state.snapshot().conditions
        poller._poll_once()
        assert state.stamped
        assert 'stream-identity-changed' not in state.snapshot().conditions


def test_stop_interrupts_fetch_interval_and_retry_waits(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([], RuntimeError('outage'))
    waiter = _Waiter(stop_on_wait=1)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(state, provider, waiter=waiter) as poller:
            poller.run(max_cycles=10)
            assert poller._poller_stopped
            poller.stop()

    assert waiter.set_calls == 1
    assert len(waiter.waits) == 1


@pytest.mark.parametrize('chunked', [False, True])
def test_strict_buffer_never_observes_late_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, chunked: bool
) -> None:
    provider, _sg, _factory = _provider([make_event(101), make_event(103)], [])
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(strict, 'editorial', 7, holdback_secs=100.0, delivery_floor=100)
    fanout = EventFanout()
    eager = _channel()
    fanout.attach(eager)
    observe = Mock(wraps=strict.observe_many)
    monkeypatch.setattr(strict, 'observe_many', observe)
    synchronize = Mock(wraps=strict.synchronize_ranges)
    pump = Mock(wraps=strict.pump_many)
    monkeypatch.setattr(strict, 'synchronize_ranges', synchronize)
    monkeypatch.setattr(strict, 'pump_many', pump)
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        notice = open_source_range(state, 101, 104, successful_clock=0.0)
        state.record_successful_query(10.0)
        with make_poller(
            state,
            provider,
            strict_buffer=strict,
            fanout=fanout,
            successful_clock=_SuccessfulQueryClock(clock=lambda: 0.0, initial_value=10.0),
        ) as poller:
            if chunked:
                encode = poller._encoded_chunks

                def single_event_chunks(
                    encoder: EventEncoder,
                    rows: Sequence[Mapping[str, object]],
                ) -> Iterable[tuple[EncodedEvent, ...]]:
                    return ((event,) for chunk in encode(encoder, rows) for event in chunk)

                monkeypatch.setattr(
                    poller,
                    '_encoded_chunks',
                    single_event_chunks,
                )
            assert poller._recheck_once(notice.obligation_id) == 2
            retained = state.range_for(notice.obligation_id)
            assert retained is not None
            assert retained.late_id_count == 2
    assert synchronize.call_count >= 2
    assert pump.call_count == (3 if chunked else 2)
    observe.assert_not_called()
    assert strict_observations(strict) == ()
    assert [
        message.encoded.event_id for message in eager.drain() if isinstance(message, EventDelivery)
    ] == [101, 103]


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
        saturating_retry_delay(attempt, base_secs=base, max_secs=maximum, multiplier=multiplier)
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
    delay = saturating_retry_delay(
        attempt, base_secs=base, max_secs=maximum, multiplier=multiplier
    )

    assert 0 < delay <= maximum
    assert delay == pytest.approx(expected, rel=1e-12, abs=0.0)


@pytest.mark.parametrize(
    ('error', 'expected'),
    [
        (
            SourceHaltedError('original diagnostic', code='commit-batch-capacity-exhausted'),
            'frame-too-large',
        ),
        (EncoderHaltedError('original diagnostic', code='encoder-failed'), 'encoder-failed'),
        (EncoderHaltedError('original diagnostic', code='event-too-large'), 'event-too-large'),
        (EncoderHaltedError('original diagnostic', code='frame-too-large'), 'frame-too-large'),
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
            reader.catch_up(
                cursor=cursor,
                frontier=frontier,
                delivery_floor=0,
                unresolved_bounds=bounds,
                source_progress=lambda: _source_progress(frontier),
                deliver_page=lambda _events: None,
            )


@pytest.mark.parametrize('loss', ['channel', 'generation', 'group'])
def test_join_abort_is_token_specific_on_channel_group_or_generation_loss(
    loss: str,
    tmp_path: Path,
) -> None:
    retention = _FakeRetention()
    with open_source(tmp_path / 'source.db') as state:
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

    assert retention.calls[-1] == ('abort', target.identity)


@pytest.mark.parametrize(
    ('attempt', 'expected'),
    [
        pytest.param(2, 2.0, id='below-saturation'),
        pytest.param(3, 4.0, id='at-saturation'),
        pytest.param(4096, 4.0, id='far-beyond-overflow'),
    ],
)
def test_retry_delay_saturates_before_overflow(attempt: int, expected: float) -> None:
    delay = saturating_retry_delay(
        attempt,
        base_secs=1.0,
        max_secs=4.0,
        multiplier=2.0,
    )

    assert isfinite(delay)
    assert delay == expected


def test_strict_epoch_delivers_only_the_prefix_below_an_unresolved_range(
    tmp_path: Path,
) -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 110}],
        [{'id': 1}],
        [{'id': 110}],
        [make_event(event_id) for event_id in range(101, 106)],
    )
    wall = Monotonic()
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
        wall.advance(60.0)

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

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(110),
            deliver=deliver,
            holdback_secs=60.0,
            max_pending_epochs=8,
            unresolved_bounds=((106, 108),),
        )

        assert state.snapshot().cursor == 105
        assert pages == [(101, 102, 103, 104, 105)]
        assert delivered == list(range(101, 106))
        assert state.snapshot().epochs[0].state == 'delivering'

    assert sg.calls[-1][1] == [
        ['id', 'greater_than', 100],
        ['id', 'less_than', 106],
    ]


@pytest.mark.parametrize('ordering', ['strict', 'eager', 'replay'])
@pytest.mark.parametrize('use_service', [False, True])
@pytest.mark.parametrize(
    'pages',
    [
        [[102, 103], [101], [102, 103]],
        [[101, 103], [102], [103]],
        [[101], [102, 103]],
        [[], [101, 102, 103]],
    ],
    ids=['leading', 'middle', 'trailing', 'empty'],
)
def test_strict_epoch_requeries_gap_before_advancing(
    tmp_path: Path,
    pages: list[list[int]],
    ordering: str,
    use_service: bool,
) -> None:
    probes: list[list[dict[str, object]]] = (
        [[{'id': 1}], [{'id': 103}]] * 2 if ordering == 'strict' else [[{'id': 1}]]
    )
    provider, sg, _factory = _provider(
        *probes,
        *[[make_event(event_id) for event_id in page] for page in pages],
    )
    wall = Monotonic()
    waiter = _Waiter()
    service_waits: list[float] = []
    service_options: _ReaderServiceOptions = (
        {'service': service_waits.append} if use_service else {}
    )
    delivered: list[int] = []
    with (
        ExitStack() as groups,
        CatchupReader(
            provider=provider,
            site_url=_SITE_URL,
            credential=_credential(),
            batch_size=20,
            fetch_interval_secs=1.0,
            waiter=waiter,
            successful_clock=_SuccessfulQueryClock(clock=wall),
        ) as reader,
    ):
        state = (
            groups.enter_context(_strict_group(tmp_path / 'group.db', holdback_secs=60.0))
            if ordering == 'strict'
            else None
        )

        def accept(event: Event, _at: float) -> StrictDeliveryOutcome:
            assert state is not None
            assert state.snapshot().epochs
            delivered.append(cast('int', event['id']))
            return _accept_strict_event(state, event)

        if ordering == 'strict':
            assert state is not None

            def step() -> _StrictCatchupResult:
                assert state is not None
                return reader.strict_step(
                    state=state,
                    source_progress=lambda: _source_progress(103),
                    deliver=_deliver_strict_events(accept),
                    holdback_secs=60.0,
                    max_pending_epochs=8,
                    join_boundary=103,
                    **service_options,
                )

            step()
            wall.advance(60.0)
            result = step()
            assert state.snapshot().epochs == ()
            assert result.join_ready
        else:
            reader.catch_up(
                cursor=103 if ordering == 'replay' else 100,
                frontier=103,
                delivery_floor=100,
                replay_from=101 if ordering == 'replay' else None,
                source_progress=lambda: _source_progress(103),
                deliver_page=lambda events: delivered.extend(cast('int', e['id']) for e in events),
                **service_options,
            )
    assert delivered == [101, 102, 103]
    assert waiter.waits == ([] if use_service else [1.0])
    assert [seconds for seconds in service_waits if seconds] == ([1.0] if use_service else [])
    assert not sg.responses


@pytest.mark.parametrize('ordering', ['strict', 'eager', 'replay'])
@pytest.mark.parametrize('floor', [1, 102])
def test_catchup_skip_logs_once_per_gap(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    ordering: str,
    floor: int,
) -> None:
    probes: list[list[dict[str, object]]] = (
        [[{'id': 1}], [{'id': 103}]] * 2 if ordering == 'strict' else [[{'id': 1}]]
    )
    provider, sg, _factory = _provider(
        *probes,
        [make_event(103)],
        [],
        [{'id': floor}],
        [make_event(103)],
    )
    wall = Monotonic()
    logger = logging.getLogger('sg_event_daemon')
    old_level = logger.level
    old_propagate = logger.propagate
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger.addHandler(caplog.handler)
    extras = {'site': _SITE, 'source': _SOURCE, 'group': 'editorial'}
    try:
        with (
            ExitStack() as groups,
            CatchupReader(
                provider=provider,
                site_url=_SITE_URL,
                credential=_credential(),
                batch_size=20,
                fetch_interval_secs=1.0,
                waiter=_Waiter(),
                successful_clock=_SuccessfulQueryClock(clock=wall),
                log_extra=extras,
            ) as reader,
        ):
            state = (
                groups.enter_context(_strict_group(tmp_path / 'group.db', holdback_secs=60.0))
                if ordering == 'strict'
                else None
            )

            def read() -> None:
                if ordering == 'strict':
                    assert state is not None
                    strict_state = state
                    reader.strict_step(
                        state=strict_state,
                        source_progress=lambda: _source_progress(103),
                        deliver=_deliver_strict_events(
                            lambda event, _at: _accept_strict_event(strict_state, event)
                        ),
                        holdback_secs=60.0,
                        max_pending_epochs=8,
                        join_boundary=103,
                    )
                else:
                    reader.catch_up(
                        cursor=103 if ordering == 'replay' else 100,
                        frontier=103,
                        delivery_floor=100,
                        replay_from=101 if ordering == 'replay' else None,
                        source_progress=lambda: _source_progress(103),
                        deliver_page=lambda _events: None,
                    )

            if ordering == 'strict':
                read()
                wall.advance(60.0)
            if floor == 102:
                with pytest.raises(HistoryTruncatedError, match='history-truncated'):
                    read()
            else:
                read()
                assert not sg.responses
    finally:
        logger.removeHandler(caplog.handler)
        logger.setLevel(old_level)
        logger.propagate = old_propagate
    records = [record for record in caplog.records if record.name == 'sg_event_daemon.group']
    if floor == 102:
        assert records == []
    else:
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert records[0].getMessage() == (
            'no events in IDs 101-102 on two reads 1.0s apart; skipped them; '
            'replay the group from 101 if they appear'
        )
        assert {key: getattr(records[0], key) for key in extras} == extras


def test_strict_epoch_respects_unresolved_obligation_and_final_join_boundary(
    tmp_path: Path,
) -> None:
    provider, _sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 10_000_000}],
    )
    wall = Monotonic()
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=60.0) as state,
        _strict_reader(
            provider,
            _SuccessfulQueryClock(clock=wall),
        ) as reader,
    ):
        reader.strict_step(
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

        assert state.snapshot().cursor == 100
        epoch = state.snapshot().epochs[0]
        assert (epoch.low_id, epoch.high_id) == (101, 105)


def test_strict_epoch_restart_resumes_above_the_durable_cursor(
    tmp_path: Path,
) -> None:
    first_provider, _first_sg, _first_factory = _provider(
        [{'id': 1}],
        [{'id': 103}],
        [{'id': 1}],
        [{'id': 103}],
        [make_event(event_id) for event_id in range(101, 104)],
    )
    wall = Monotonic()
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
        wall.advance(60.0)

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
        [make_event(102), make_event(103)],
    )
    delivered: list[int] = []
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(
            restart_provider,
            _SuccessfulQueryClock(clock=Monotonic()),
        ) as reader,
    ):

        def deliver_after_restart(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event)
            delivered.append(event_id)
            return outcome

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(103),
            deliver=_deliver_strict_events(deliver_after_restart),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert state.snapshot().cursor == 103
        assert state.snapshot().epochs == ()

    assert delivered == [102, 103]
    assert restart_sg.calls[-1][1] == [
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
        [make_event(101)],
    )
    wall = Monotonic()
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
        wall.advance(60.0)

        def accept_then_stop(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event, continue_epoch=False)
            offered.append(event_id)
            return outcome

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(101),
            deliver=_deliver_strict_events(accept_then_stop),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert state.snapshot().cursor == 101
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
            _SuccessfulQueryClock(clock=Monotonic()),
        ) as reader,
    ):

        def record_unexpected(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            offered.append(cast('int', event['id']))
            return StrictDeliveryOutcome(accepted=True, continue_epoch=True)

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(101),
            deliver=_deliver_strict_events(record_unexpected),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert state.snapshot().cursor == 101
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
        [make_event(101), make_event(102)],
    )
    wall = Monotonic()
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
        wall.advance(60.0)

        def reject_delivery(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            offered.append(cast('int', event['id']))
            return StrictDeliveryOutcome(accepted=False, continue_epoch=False)

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(reject_delivery),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert state.snapshot().cursor == 100
        assert offered == [101]
        assert state.snapshot().epochs[0].state == 'delivering'

    restart_provider, _restart_sg, _restart_factory = _provider(
        [{'id': 1}],
        [{'id': 102}],
        [make_event(101), make_event(102)],
    )
    replayed: list[int] = []
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(
            restart_provider,
            _SuccessfulQueryClock(clock=Monotonic()),
        ) as reader,
    ):

        def accept_replayed(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event)
            replayed.append(event_id)
            return outcome

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(accept_replayed),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert state.snapshot().cursor == 102
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
        [make_event(101), make_event(102)],
    )
    wall = Monotonic()
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
        wall.advance(60.0)

        def stop_after_first(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            offered.append(event_id)
            if event_id != 101:
                return StrictDeliveryOutcome(accepted=False, continue_epoch=False)
            return _accept_strict_event(state, event, continue_epoch=False)

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(stop_after_first),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert state.snapshot().cursor == 101
        assert offered == [101]
        assert state.snapshot().epochs[0].state == 'delivering'

    restart_provider, _restart_sg, _restart_factory = _provider(
        [{'id': 1}],
        [{'id': 102}],
        [make_event(102)],
    )
    replayed: list[int] = []
    with (
        _strict_group(state_path, holdback_secs=60.0) as state,
        _strict_reader(
            restart_provider,
            _SuccessfulQueryClock(clock=Monotonic()),
        ) as reader,
    ):

        def accept_replayed(event: Event, _ordering_time: float) -> StrictDeliveryOutcome:
            event_id = cast('int', event['id'])
            outcome = _accept_strict_event(state, event)
            replayed.append(event_id)
            return outcome

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(102),
            deliver=_deliver_strict_events(accept_replayed),
            holdback_secs=60.0,
            max_pending_epochs=8,
        )

        assert state.snapshot().cursor == 102
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
        [make_event(event_id) for event_id in range(101, 111)],
    )
    wall = Monotonic()
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

        wall.advance(300.0)

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
        assert state.snapshot().cursor == 110
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
        [make_event(102), make_event(104)],
        [],
        [{'id': 1}],
        [make_event(102), make_event(104)],
        [],
        [{'id': 1}],
        [make_event(104)],
        [],
        [{'id': 1}],
    )
    wall = Monotonic()
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
        wall.advance(60.0)

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
        assert state.snapshot().cursor == (104 if continue_epoch else 102)
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
    with open_source(tmp_path / 'source.db') as state:
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

        retention.response = None
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
        [make_event(102)] if replay else [make_event(101), make_event(102)],
        [make_event(103), make_event(104)],
    )
    pages: list[list[int]] = []

    def deliver_page(events: Sequence[Event]) -> None:
        pages.append([cast('int', event['id']) for event in events])

    with _reader(provider) as reader:
        reader.catch_up(
            cursor=104 if replay else 100,
            frontier=104,
            delivery_floor=101 if replay else 100,
            source_progress=lambda: _source_progress(104),
            deliver_page=deliver_page,
            replay_from=101 if replay else None,
        )
    assert pages == ([[102], [103, 104]] if replay else [[101, 102], [103, 104]])
    assert not sg.responses


@pytest.mark.parametrize('replay', [False, True])
def test_catchup_services_provider_returns_and_empty_pages(replay: bool) -> None:
    responses: tuple[list[dict[str, object]], ...] = ([{'id': 1}], [], [], [{'id': 1}])
    provider, sg, _factory = _provider(*responses)
    waiter = _Waiter()
    serviced: list[tuple[float, int]] = []
    with _reader(provider, waiter=waiter) as reader:
        reader.catch_up(
            cursor=100,
            frontier=103,
            delivery_floor=100,
            replay_from=101 if replay else None,
            source_progress=lambda: _source_progress(103),
            deliver_page=lambda _events: pytest.fail('empty page delivered'),
            service=lambda seconds: serviced.append((seconds, len(sg.calls))),
        )
    assert {count for seconds, count in serviced if seconds == 0.0} >= set(
        range(1, len(responses) + 1)
    )
    assert [seconds for seconds, _count in serviced if seconds > 0] == [1.0]
    assert not waiter.waits


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
    with open_source(tmp_path / 'source.db') as state:
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
    with open_source(tmp_path / 'source.db') as state:
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
    assert [name for name, _value in retention.calls] == ['reserve', 'prepare', 'finalize']


@pytest.mark.parametrize('slow_call', [1, 2, 3])
@pytest.mark.parametrize('continuation', ['resume', 'stop', 'halt'])
def test_strict_prepared_page_services_slow_queries_and_retains_suffix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, slow_call: int, continuation: str
) -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}], [{'id': 103}], [make_event(event_id) for event_id in range(101, 104)]
    )
    wall = Monotonic()
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
        if len(sg.calls) == slow_call:
            wall.advance(1.0)
        return result

    monkeypatch.setattr(sg, 'find', slow_find)
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=1.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):
        _seed_delivering_epoch(state)

        def service(seconds: float) -> None:
            assert seconds == 0.0
            serviced.append(len(sg.calls))
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
            wall.advance(0.6)
            return StrictDeliveryOutcome(
                accepted=True,
                continue_epoch=continuation != 'stop',
                consumed=1,
            )

        reader.strict_step(
            state=state,
            source_progress=lambda: _source_progress(103),
            deliver=deliver,
            holdback_secs=1.0,
            max_pending_epochs=8,
            service=service,
        )
        assert delivered == ([101, 102, 103] if continuation == 'resume' else [101])
        assert state.snapshot().cursor == delivered[-1]
        assert bool(state.snapshot().epochs) == (continuation != 'resume')
        assert set(serviced) >= {1, 2, 3}
    assert len(sg.calls) == 3


@pytest.mark.parametrize(
    ('change', 'return_call', 'empty'),
    [
        *((change, 3, empty) for change in ('fence', 'epoch') for empty in (False, True)),
        *(
            (identity, call, empty)
            for identity in ('group_generation', 'source_generation', 'verified_incarnation')
            for call in (1, 2, 3)
            for empty in ((False, True) if call == 3 else (False,))
        ),
    ],
)
def test_strict_prepared_page_invalidates_changed_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str, return_call: int, empty: bool
) -> None:
    provider, sg, _factory = _provider(
        [{'id': 1}], [{'id': 103}], [] if empty else [make_event(101)]
    )
    changed = False
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=1.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=Monotonic())) as reader,
    ):
        _seed_delivering_epoch(state)

        def service(_seconds: float) -> None:
            nonlocal changed
            if len(sg.calls) != return_call or changed:
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
    assert len(sg.calls) == return_call


@pytest.mark.parametrize('replay', [False, True])
def test_between_pages_runs_with_no_page_held(
    replay: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _TrackedEvent(Event):
        pass

    provider, sg, _factory = _provider(
        [{'id': 1}],
        [make_event(101), make_event(102)],
        [make_event(103), make_event(104)],
        [make_event(105), make_event(106)],
    )
    sequence: list[tuple[str, int]] = []
    delivered_refs: list[weakref.ReferenceType[Event]] = []

    def deliver_page(events: Sequence[Event]) -> None:
        sequence.append(('delivered', cast('int', events[-1]['id'])))
        delivered_refs.extend(weakref.ref(event) for event in events)

    def between_pages() -> None:
        assert all(event() is None for event in delivered_refs), 'previous page is still held'
        sequence.append(('between', len(sg.calls)))

    def tracked_events(rows: Sequence[Mapping[str, object]]) -> tuple[Event, ...]:
        return tuple(_TrackedEvent(row) for row in rows)

    with _reader(provider) as reader:
        monkeypatch.setattr(reader, '_events', tracked_events)
        reader.catch_up(
            cursor=106 if replay else 100,
            frontier=106,
            delivery_floor=100,
            source_progress=lambda: _source_progress(106),
            deliver_page=deliver_page,
            replay_from=101 if replay else None,
            between_pages=between_pages,
        )
    assert sequence == [
        ('between', 1),
        ('delivered', 102),
        ('between', 2),
        ('delivered', 104),
        ('between', 3),
        ('delivered', 106),
    ]


@pytest.mark.parametrize(
    ('change', 'gap_fills'), [('stop', False), ('fence', False), ('fence', True)]
)
def test_strict_gap_wait_preserves_stop_and_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    gap_fills: bool,
    change: str,
) -> None:
    logger = logging.getLogger('sg_event_daemon')
    monkeypatch.setattr(logger, 'handlers', [caplog.handler])
    monkeypatch.setattr(logger, 'propagate', False)
    caplog.set_level(logging.INFO, logger='sg_event_daemon')
    provider, sg, _factory = _provider(
        [{'id': 1}],
        [{'id': 103}],
        [make_event(103)],
        [make_event(101), make_event(102)] if gap_fills else [],
        [{'id': 1}],
    )
    with (
        _strict_group(tmp_path / 'group.db', holdback_secs=1.0) as state,
        _strict_reader(provider, _SuccessfulQueryClock(clock=Monotonic())) as reader,
    ):
        _seed_delivering_epoch(state)

        def service(seconds: float) -> None:
            if not seconds:
                return
            assert seconds == 1.0
            if change == 'stop':
                raise InterruptedError('gap wait stopped')
            connection = vars(state)['_database'].connection
            insert_range(connection, 'range-102', 102, 103)
            bind_group(connection, 'range-102', 'editorial', 7, floor=100, eligibility_point=1)

        def step() -> _StrictCatchupResult:
            return reader.strict_step(
                state=state,
                source_progress=lambda: _source_progress(103),
                deliver=lambda _events, _at: pytest.fail('invalidated gap page delivered'),
                holdback_secs=1.0,
                max_pending_epochs=8,
                service=service,
            )

        if change == 'stop':
            with pytest.raises(InterruptedError, match='gap wait stopped'):
                step()
            assert len(sg.calls) == 3
        else:
            result = step()
            assert result.exhausted_through is None
            assert not result.join_ready
        assert state.snapshot().cursor == 100
        assert state.snapshot().epochs
    assert 'skipped them' not in caplog.text


@pytest.mark.parametrize(
    'setting', ['max_open_ranges', 'max_pending_group_obligations', 'max_source_late_ids']
)
def test_pause_keeps_worker_up_and_deadline_scans_running(tmp_path: Path, setting: str) -> None:
    provider, sg, _factory = _provider([make_event(106), make_event(108)], [], [])
    waiter = _Waiter()
    clock = _SuccessfulQueryClock(clock=lambda: 0, initial_value=11)
    with open_source(tmp_path / 'source.db') as state:
        state._settings = replace(state._settings, **{setting: 1})
        state.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(state, 101, 103, successful_clock=1)
        record_source_late_ids(state, opened.obligation_id, (101,), successful_clock=11)
        with make_poller(state, provider, successful_clock=clock, waiter=waiter) as poller:
            poller.run(max_cycles=1)
            assert poller.health.retry_attempt == 0
            assert poller._paused_since is not None
            assert state.snapshot().ranges[0].state == 'closed'
            assert state.snapshot().frontier == 104
    assert len(sg.calls) == 3
    assert waiter.waits == [1.0]


@pytest.mark.parametrize(
    'setting', ['max_open_ranges', 'max_pending_group_obligations', 'max_source_late_ids']
)
def test_pause_warns_once_and_resume_reports_duration(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
    setting: str,
) -> None:
    wall = Monotonic()
    monkeypatch.setattr(source_module, 'time', SimpleNamespace(monotonic=wall))
    logger = logging.getLogger('sg_event_daemon')
    monkeypatch.setattr(logger, 'handlers', [caplog.handler])
    monkeypatch.setattr(logger, 'propagate', False)
    caplog.set_level(logging.INFO, logger='sg_event_daemon')
    provider, _sg, _factory = _provider([make_event(106)], [make_event(106)], [make_event(106)])
    with open_source(tmp_path / 'source.db') as state:
        state._settings = replace(state._settings, **{setting: 1})
        state.initialize(100, incarnation='incarnation-1')
        opened = open_source_range(state, 101, 103, successful_clock=1)
        record_source_late_ids(state, opened.obligation_id, (101,), successful_clock=2)
        with make_poller(state, provider) as poller:
            assert poller._poll_once() == 0
            wall.advance(2.5)
            assert poller._poll_once() == 0
            state._settings = replace(state._settings, **{setting: 2})
            wall.advance(1.0)
            assert poller._poll_once() == 1
    records = [record for record in caplog.records if record.name == 'sg_event_daemon.source']
    expected_warning = (
        f'source paused: {setting} limit 1 reached '
        f'({1 if setting == "max_source_late_ids" else 2}); '
        'open gaps continue and new gaps wait'
    )
    assert [(record.levelname, record.getMessage()) for record in records] == [
        ('WARNING', expected_warning),
        ('INFO', 'source resumed after 3.5s paused'),
    ]
    assert [(record.__dict__['site'], record.__dict__['source']) for record in records] == [
        (_SITE, _SOURCE),
        (_SITE, _SOURCE),
    ]


@pytest.mark.parametrize('rejected_suffix', [False, True])
def test_restart_while_paused_ends_on_first_clean_commit(
    tmp_path: Path, rejected_suffix: bool
) -> None:
    path = tmp_path / 'source.db'
    with open_source(path, max_open_ranges=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 101, successful_clock=1)
        assert state.commit_observations((120,), successful_clock=2).pause is not None
    rows = [make_event(103), make_event(110)] if rejected_suffix else [make_event(103)]
    provider, _sg, _factory = _provider(rows)
    with open_source(path, max_open_ranges=1) as state, make_poller(state, provider) as poller:
        assert poller._paused_since is not None
        assert poller._blocked_id is None
        poller._poll_once()
        assert state.snapshot().frontier == 103
        if rejected_suffix:
            assert poller._blocked_id == 110
            assert poller._paused_since is not None
            assert state.snapshot().status == 'range-capacity-exhausted'
        else:
            assert poller._paused_since is None
            assert state.snapshot().status == 'running'


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('wake', ['before', 'during', 'stop'])
def test_catchup_gap_wait_preserves_deadline_across_worker_wakes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: str,
    wake: str,
) -> None:
    component, resource_probe = worker_tests.lifecycle_probe(
        'sync', durable_status='running', setup_failure=None
    )
    resources = cast('worker_module._LaneResources', resource_probe)
    component._lifecycle = worker_module._LaneLifecycle(channel=resources.channel)
    resource_probe.context.source_config.fetch_interval_secs = 1.0
    wall = Monotonic()
    monkeypatch.setattr(worker_module.time, 'monotonic', wall)
    waits: list[float] = []

    def wait(timeout: float | None = None) -> bool:
        assert timeout is not None
        waits.append(timeout)
        if len(waits) == 1:
            if wake != 'before':
                wall.advance(0.25)
            if wake == 'stop':
                component._stop.set()
            component._wake.set()
        else:
            wall.advance(timeout)
        return component._wake.is_set()

    monkeypatch.setattr(component._wake, 'wait', wait)
    if wake == 'before':
        component._wake.set()
    probes: list[list[dict[str, object]]] = (
        [[{'id': 1}], [{'id': 101}]] * 2 if ordering == 'strict' else [[{'id': 1}]]
    )
    provider, sg, _factory = _provider(*probes, [], [], [{'id': 1}])
    service = partial(
        component._service_catchup,
        resources,
        phase='strict-catchup' if ordering == 'strict' else 'eager-catchup',
    )
    with (
        ExitStack() as groups,
        _strict_reader(provider, _SuccessfulQueryClock(clock=wall)) as reader,
    ):
        if ordering == 'strict':
            state = groups.enter_context(_strict_group(tmp_path / 'group.db', holdback_secs=60))
            step = partial(
                reader.strict_step,
                state=state,
                source_progress=lambda: _source_progress(101),
                deliver=_deliver_strict_events(
                    lambda event, _at: _accept_strict_event(state, event)
                ),
                holdback_secs=60,
                max_pending_epochs=8,
                join_boundary=101,
                service=service,
            )
            step()
            wall.advance(60)
        else:
            step = partial(
                reader.catch_up,
                cursor=100,
                frontier=101,
                delivery_floor=100,
                source_progress=lambda: _source_progress(101),
                deliver_page=lambda _events: None,
                service=service,
            )
        started = wall()
        if wake == 'stop':
            with pytest.raises(worker_module._CooperativeComponentStopError):
                step()
            assert wall() - started == 0.25
            assert len(sg.responses) == 2, 'stop must precede the second gap read'
        else:
            result = step()
            if ordering == 'strict':
                assert result is not None
                assert result.join_ready
            else:
                assert result is None
            assert wall() - started == 1.0
            assert waits == ([1.0, 1.0] if wake == 'before' else [1.0, 0.75])
            assert not sg.responses


@pytest.mark.parametrize('deadline_scan', [False, True])
def test_recheck_serves_before_every_provider_call(
    tmp_path: Path,
    deadline_scan: bool,
) -> None:
    provider, _, _ = _provider()
    sg = EventLogSG(make_event(i) for i in range(101, 109))
    served: list[int] = []
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 108, successful_clock=1)
        with make_poller(
            state,
            provider,
            config=replace(_source_config(), max_range_query_pages=10),
            service=lambda: served.append(len(sg.calls)),
        ) as poller:
            poller._handle = cast('SyncSG', sg)
            if deadline_scan:
                poller._recheck_once(gap.obligation_id)
            else:
                poller._recheck_missing_once()
        assert len(sg.calls) >= 4
        assert served == list(range(len(sg.calls)))
        assert (retained := state.range_for(gap.obligation_id)) is not None
        assert retained.late_id_count == 8


@pytest.mark.parametrize('deadline_scan', [False, True])
def test_stop_during_recheck_ends_the_cycle_at_the_next_page(
    tmp_path: Path,
    deadline_scan: bool,
) -> None:
    provider, _, _ = _provider()
    sg = EventLogSG(make_event(i) for i in range(101, 109))
    services: list[int] = []
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 108, successful_clock=1)

        def service() -> None:
            services.append(len(sg.calls))
            if sg.calls:
                poller.stop()

        with make_poller(
            state,
            provider,
            config=replace(_source_config(), max_range_query_pages=10),
            service=service,
        ) as poller:
            poller._handle = cast('SyncSG', sg)
            if deadline_scan:
                poller._recheck_once(gap.obligation_id)
            else:
                poller._recheck_missing_once()
        assert services == [0, 1]
        assert len(sg.calls) == 1
        assert (retained := state.range_for(gap.obligation_id)) is not None
        assert retained.late_id_count == 2


@pytest.mark.parametrize('deadline_scan', [False, True])
def test_service_pass_error_propagates_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    deadline_scan: bool,
) -> None:
    provider, _, _ = _provider()
    sg = EventLogSG(make_event(i) for i in range(101, 109))
    error = RuntimeError('source service invariant failed')
    waiter = _Waiter()
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        open_source_range(state, 101, 108, successful_clock=1)
        state.record_successful_query(20)

        def service() -> None:
            if sg.calls:
                raise error

        with make_poller(
            state,
            provider,
            config=replace(_source_config(), max_range_query_pages=10),
            waiter=waiter,
            service=service,
        ) as poller:
            poller._handle = cast('SyncSG', sg)
            monkeypatch.setattr(poller, '_poll_once', lambda: 0)
            if deadline_scan:
                monkeypatch.setattr(poller, '_recheck_missing_once', lambda: None)
            with pytest.raises(RuntimeError, match='source service invariant failed') as caught:
                poller.run(max_cycles=2)
            assert caught.value is error
            assert poller.health.retry_attempt == 0
        assert len(sg.calls) == 1
        assert waiter.waits == []


@pytest.mark.parametrize('deadline_scan', [False, True])
def test_service_between_pages_keeps_ranges_consistent(
    tmp_path: Path,
    deadline_scan: bool,
) -> None:
    provider, _, _ = _provider()
    sg = EventLogSG(make_event(i) for i in range(101, 109))
    fanout = _RecordingFanout()
    path = tmp_path / 'source.db'
    socket_path = Path(os.sep, 'tmp') / f'sed-service-{os.getpid()}-{deadline_scan}.sock'
    trace: list[str] = []
    with (
        open_source(path) as state,
        EventServer(
            path=socket_path,
            source=_SOURCE,
            max_frame_bytes=131_072,
            queue_max_entries=16,
            queue_max_bytes=262_144,
        ) as server,
    ):
        state.initialize(100, incarnation='incarnation-1')
        gap = open_source_range(state, 101, 108, successful_clock=1)
        coordinator = JoinCoordinator(
            state=state, fanout=fanout, source_generation=_SOURCE_GENERATION
        )
        server.install_grant(
            EventPeerGrant(
                source=_SOURCE,
                group='render',
                dependent_spawn_instance='remote-service',
                capability='service-capability',
            )
        )
        client = EventClient(
            path=socket_path,
            source=_SOURCE,
            group='render',
            spawn_instance='remote-service',
            capability='service-capability',
            max_frame_bytes=131_072,
            queue_max_entries=16,
            queue_max_bytes=262_144,
            coordination_deadline_secs=stall_deadline('coordination', 2),
            stop=threading.Event(),
            wait_phase=nullcontext,
            wait_slice_secs=0.1,
        )
        try:
            client.connect()
            with ThreadPoolExecutor(max_workers=1) as executor:
                remote_channel = GroupChannel(
                    group='render', ordering='eager', max_events=16, max_bytes=262_144
                )
                remote = executor.submit(
                    client.request,
                    JoinRequest(
                        replace(
                            _attempt('remote-between-pages'), group='render', group_generation=3
                        )
                    ),
                    remote_channel,
                )

                def service() -> None:
                    if len(sg.calls) != 1 or trace:
                        return
                    joined = coordinator.request(
                        JoinRequest(_attempt('local-between-pages')), _channel()
                    )
                    assert isinstance(joined, JoinTarget)
                    coordinator.confirm(JoinConfirm(joined.identity))
                    coordinator.finalize(JoinFinalize(joined.identity))
                    trace.append('joined')
                    with _open_group(path) as group:
                        group.park(
                            expected_revision=group.snapshot().revision,
                            reason='allow disposition',
                            identity=AuditIdentity.current(),
                            wall_clock_time=19,
                        )
                        result = group.dispose(
                            DispositionRequest(
                                command='reset-to-latest',
                                group='editorial',
                                disposition_id='between-pages',
                                reserved_generation=8,
                                expected_revision=group.snapshot().revision,
                                from_event_id=None,
                                acknowledge_truncation=False,
                                reason='retire old obligations',
                                identity=AuditIdentity.current(),
                                wall_clock_time=20,
                            )
                        )
                        assert result.obligation_count == 0
                    trace.append('disposed')
                    while not remote.done():
                        server.poll(0.001)
                        server.dispatch_join_requests(cast('_JoinSource', coordinator))
                    assert isinstance(remote.result(), JoinTarget)
                    trace.append('remote')

                with make_poller(
                    state,
                    provider,
                    config=replace(_source_config(), max_range_query_pages=10),
                    fanout=fanout,
                    service=service,
                ) as poller:
                    poller._handle = cast('SyncSG', sg)
                    if deadline_scan:
                        poller._recheck_once(gap.obligation_id)
                    else:
                        poller._recheck_missing_once()
                # Release the pending client request before the executor waits for its thread.
                if not trace:
                    client.close()
        finally:
            client.close()
        assert trace == ['joined', 'disposed', 'remote']
        assert (retained := state.range_for(gap.obligation_id)) is not None
        assert {event.event_id for chunk in fanout.published for event in chunk} == set(
            range(101, 109)
        ), (sg.calls, retained, fanout.published)
        assert retained.state == 'closed'


@pytest.mark.parametrize('provider_failure', [False, True])
def test_servicing_handle_keeps_provider_retry_for_provider_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider_failure: bool,
) -> None:
    error = RuntimeError('provider failure')
    provider, sg, _factory = _provider(error if provider_failure else [])
    serviced = threading.Event()
    entered = threading.Event()
    release = threading.Event()
    original = sg.find
    provider_threads: list[threading.Thread] = []

    def find(
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        provider_threads.append(threading.current_thread())
        entered.set()
        assert release.wait()
        return original(entity_type, filters, fields, **options)

    monkeypatch.setattr(sg, 'find', find)
    waiter = _Waiter()

    def run() -> None:
        with open_source(tmp_path / 'source.db') as state:
            state.initialize(100, incarnation='incarnation-1')
            with make_poller(state, provider, waiter=waiter, query_service=serviced.set) as poller:
                poller.run(max_cycles=1)
                assert poller.health.last_error == (str(error) if provider_failure else None)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(run)
        try:
            assert _wait_signal(entered, future)
            observed = _wait_signal(serviced, future)
        finally:
            release.set()
        future.result()
    assert observed, 'event service did not run during the provider call'
    assert len(waiter.waits) == 1
    assert not provider_threads[0].is_alive()


def test_strict_releases_after_default_hold_with_no_lower_gap(tmp_path: Path) -> None:
    provider, _sg, _factory = _provider([make_event(101)], [], [])
    wall = Monotonic()
    strict = StrictBuffer(max_events=20, max_bytes=10_000_000)
    channel = _channel(ordering='strict')
    fanout = EventFanout()
    fanout.attach(channel)
    attach_strict_group(
        strict,
        'editorial',
        7,
        holdback_secs=DEFAULT_STRICT_HOLDBACK_SECS,
        delivery_floor=100,
        channel=channel,
    )
    with open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(
            state,
            provider,
            strict_buffer=strict,
            fanout=fanout,
            successful_clock=_SuccessfulQueryClock(clock=wall),
        ) as poller:
            poller._poll_once()
            wall.advance(1.0)
            poller._poll_once()
            assert not channel.drain()
            wall.advance(1.0)
            poller._poll_once()
            assert [
                m.encoded.event_id for m in channel.drain() if isinstance(m, EventDelivery)
            ] == [101]


def test_strict_retry_allows_later_boundary_only_for_fresh_token(
    tmp_path: Path,
) -> None:
    retention = _FakeRetention(retry=True)
    with open_source(tmp_path / 'source.db') as state:
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
        retention.retry = False

        target = coordinator.request(
            JoinRequest(_attempt('retry-2', join_kind='strict')),
            _channel(ordering='strict'),
        )

    assert isinstance(target, JoinTarget)
    assert target.identity.boundary == 201


def test_successful_query_clock_pauses_during_outage_and_resets_anchor(
    tmp_path: Path,
) -> None:
    wall = Monotonic()
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
    with open_source(state_path) as state:
        state.initialize(100, incarnation='incarnation-1')
        with make_poller(
            state,
            provider,
            config=config,
            successful_clock=clock,
            waiter=waiter,
        ) as poller:
            poller._poll_once()
            assert state.snapshot().successful_clock == 0.0
            poller.run(max_cycles=1)
            assert waiter.waits == [60.0]
            assert state.snapshot().successful_clock == 0.0
            poller.run(max_cycles=1)
            assert waiter.waits == [60.0, 10.0]
            assert state.snapshot().successful_clock == 0.0
            poller._poll_once()

        assert state.snapshot().successful_clock == 10.0

    with open_source(state_path) as reopened:
        assert reopened.snapshot().successful_clock == 10.0


def test_zero_row_admission_publishes_committed_range_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider, sg, _factory = _provider([make_event(106)])
    fanout = _RecordingFanout()
    eager = _channel()
    fanout.attach(eager)
    with open_source(tmp_path / 'source.db', max_source_late_ids=1) as state:
        state.initialize(100, incarnation='incarnation-1')
        state.commit_observations((102, 104), successful_clock=1)
        record_source_late_ids(state, 'range-103-103', (103,), successful_clock=2)
        find = sg.find

        def orphan_during_query(
            entity_type: str,
            filters: object,
            fields: list[str] | None = None,
            **options: Unpack[_FindOptions],
        ) -> list[dict[str, object]]:
            state._database.connection.execute(
                'DELETE FROM range_groups WHERE obligation_id = ?', ('range-101-101',)
            )
            return find(entity_type, filters, fields, **options)

        monkeypatch.setattr(sg, 'find', orphan_during_query)
        with make_poller(state, provider, fanout=fanout) as poller:
            assert poller._poll_once() == 0
        assert state.range_for('range-101-101') is None
        assert state.snapshot().frontier == 104
        assert fanout.hint_calls == 1
        assert not [event for page in fanout.published for event in page]
        assert not [message for message in eager.drain() if isinstance(message, EventDelivery)]
