from __future__ import annotations

import threading
from datetime import datetime
from typing import TYPE_CHECKING, Literal

import pytest

import sg_event_daemon._fanout as fanout_module
from sg_event_daemon._event_frame import encode_event_header, event_frame_bytes
from sg_event_daemon._events import Event, _EventEncodingError, decode_event, encode_event
from sg_event_daemon._fanout import (
    EncodedEvent,
    EncoderHaltedError,
    EncoderWorker,
    EventDelivery,
    EventFanout,
    FanoutStatus,
    GroupChannel,
    StrictCapacityControl,
    StrictOrderingControl,
    _FrameTooLargeError,
)
from sg_event_daemon._group_types import LiveJoinIdentity
from sg_event_daemon._ipc import _encode_fanout_message

if TYPE_CHECKING:
    from collections.abc import Callable


def _encoded(event_id: int, size: int = 8) -> EncodedEvent:
    return EncodedEvent.from_event_frame(event_id, bytes([event_id % 251]) * size)


def _event(event_id: int = 1, **changes: object) -> Event:
    payload: dict[str, object] = {
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
    payload.update(changes)
    return Event(payload)


def test_channel_close_detaches_only_that_group() -> None:
    fanout = EventFanout()
    closed = GroupChannel(
        group='closed',
        ordering='eager',
        max_events=10,
        max_bytes=10_000_000,
    )
    healthy = GroupChannel(
        group='healthy',
        ordering='eager',
        max_events=10,
        max_bytes=10_000_000,
    )
    fanout.attach(closed)
    fanout.attach(healthy)
    closed.close()

    fanout.publish_events((_encoded(20),))
    fanout.detach('closed', reason='channel-closed')
    fanout.detach('closed', reason='channel-closed')

    assert closed.status.reason == 'channel-closed'
    assert healthy.drain() == (EventDelivery(_encoded(20)),)


def test_encoder_counts_complete_event_frame_at_exact_minimum_limit() -> None:
    payload_bytes = 100
    with EncoderWorker(
        max_event_bytes=payload_bytes,
        max_ipc_frame_bytes=199,
        event_encoder=lambda _event: b'x' * payload_bytes,
    ) as worker:
        encoded = worker.encode(_event())

    assert len(encoded.event_frame) == payload_bytes
    assert encoded.byte_count == 203


def test_encoder_detects_worker_death() -> None:
    def die(_event: Event) -> bytes:
        raise SystemExit(9)

    with EncoderWorker(
        max_event_bytes=100,
        max_ipc_frame_bytes=100_000,
        event_encoder=die,
    ) as worker:
        with pytest.raises(EncoderHaltedError, match='encoder-died') as raised:
            worker.encode(_event())
        assert raised.value.code == 'encoder-died'
        assert not worker._thread.is_alive()


@pytest.mark.parametrize(
    'factory',
    [
        lambda: _EventEncodingError('unsupported value'),
        lambda: RuntimeError('encoder exploded'),
    ],
)
def test_encoder_propagates_encoding_exceptions_as_terminal_halts(
    factory: Callable[[], Exception],
) -> None:
    def fail(_event: Event) -> bytes:
        raise factory()

    with EncoderWorker(
        max_event_bytes=100,
        max_ipc_frame_bytes=100_000,
        event_encoder=fail,
    ) as worker:
        with pytest.raises(EncoderHaltedError, match='encoder-failed') as raised:
            worker.encode(_event())
        assert raised.value.code == 'encoder-failed'
        with pytest.raises(EncoderHaltedError, match='encoder-failed') as raised:
            worker.encode(_event(2))
        assert raised.value.code == 'encoder-failed'


def test_encoder_rejects_event_and_complete_frame_oversize_before_return() -> None:
    with (
        EncoderWorker(
            max_event_bytes=9,
            max_ipc_frame_bytes=1_000,
            event_encoder=lambda _event: b'x' * 10,
        ) as worker,
        pytest.raises(EncoderHaltedError, match='event-too-large') as raised,
    ):
        worker.encode(_event())

    assert raised.value.code == 'event-too-large'

    with (
        EncoderWorker(
            max_event_bytes=10,
            max_ipc_frame_bytes=108,
            event_encoder=lambda _event: b'x' * 10,
        ) as worker,
        pytest.raises(_FrameTooLargeError, match='frame-too-large') as raised,
    ):
        worker.encode(_event())
    assert raised.value.code == 'frame-too-large'


def test_encoder_shutdown_interrupts_a_waiting_request() -> None:
    started = threading.Event()
    release = threading.Event()
    outcome: list[BaseException | EncodedEvent] = []

    def block(_event: Event) -> bytes:
        started.set()
        release.wait()
        return b'event'

    worker = EncoderWorker(
        max_event_bytes=100,
        max_ipc_frame_bytes=100_000,
        event_encoder=block,
    )

    def encode() -> None:
        try:
            outcome.append(worker.encode(_event()))
        except BaseException as exc:
            if not isinstance(exc, Exception):
                raise
            outcome.append(exc)

    caller = threading.Thread(target=encode)
    caller.start()
    try:
        assert started.wait(timeout=1)
        worker.close()
        caller.join(timeout=1)
    finally:
        release.set()
        worker.close()
        caller.join(timeout=1)

    assert not caller.is_alive()
    assert len(outcome) == 1
    assert isinstance(outcome[0], EncoderHaltedError)
    assert 'shutdown' in str(outcome[0])
    assert outcome[0].code == 'encoder-shutdown'


def test_encoder_uses_real_dedicated_thread_and_one_event_handoff() -> None:
    encoding_threads: list[int] = []

    def encode(event: Event) -> bytes:
        encoding_threads.append(threading.get_ident())
        return encode_event(event)

    caller_thread = threading.get_ident()
    with EncoderWorker(
        max_event_bytes=10_000,
        max_ipc_frame_bytes=100_000,
        event_encoder=encode,
    ) as worker:
        internals = vars(worker)
        assert internals['_requests'].maxsize == 1
        assert internals['_results'].maxsize == 1
        assert internals['_thread'].daemon
        first = worker.encode(_event(1))
        second = worker.encode(_event(2))

    assert first.event_id == 1
    assert second.event_id == 2
    assert len(set(encoding_threads)) == 1
    assert encoding_threads[0] != caller_thread
    assert decode_event(first.event_frame) == _event(1)


def test_event_frame_encoded_event_count_matches_wire_bytes() -> None:
    encoded = EncodedEvent.from_event_frame(21, b'event')
    frame = _encode_fanout_message(EventDelivery(encoded))

    assert encoded.byte_count == frame._wire_bytes


def test_event_frame_header_and_complete_size_match_wire_encoding() -> None:
    metadata: dict[str, object] = {'event_id': 21}
    encoded = EncodedEvent.from_event_frame(21, b'event')
    frame = _encode_fanout_message(EventDelivery(encoded))

    assert encode_event_header('event', metadata) == frame._header
    assert (
        event_frame_bytes(
            'event',
            metadata,
            payload_byte_count=len(encoded.event_frame),
        )
        == frame._wire_bytes
    )


def test_fanout_shares_one_immutable_encoded_event_across_eager_groups() -> None:
    fanout = EventFanout()
    first = GroupChannel(
        group='first',
        ordering='eager',
        max_events=10,
        max_bytes=10_000_000,
    )
    second = GroupChannel(
        group='second',
        ordering='eager',
        max_events=10,
        max_bytes=10_000_000,
    )
    fanout.attach(first)
    fanout.attach(second)
    encoded = _encoded(20)

    fanout.publish_events((encoded,))

    first_delivery = first.drain()[0]
    second_delivery = second.drain()[0]
    assert isinstance(first_delivery, EventDelivery)
    assert isinstance(second_delivery, EventDelivery)
    assert first_delivery.encoded is encoded
    assert second_delivery.encoded is encoded


def test_group_channel_detaches_at_byte_cap_and_close_is_idempotent() -> None:
    event = EventDelivery(_encoded(1))
    channel = GroupChannel(
        group='slow',
        ordering='eager',
        max_events=2,
        max_bytes=event.byte_count - 1,
    )

    assert not channel.offer(event)
    channel.close()
    channel.close()

    assert channel.status.reason == 'queue-byte-exhausted'
    assert channel.drain() == ()


def test_group_channel_detaches_at_event_count_cap_and_releases_bodies() -> None:
    channel = GroupChannel(
        group='slow',
        ordering='eager',
        max_events=1,
        max_bytes=1_000_000,
    )
    first = EventDelivery(_encoded(1))
    second = EventDelivery(_encoded(2))

    assert channel.offer(first)
    assert not channel.offer(second)
    assert channel.status == FanoutStatus(
        group='slow',
        ordering='eager',
        attached=False,
        provisional=False,
        queued_count=0,
        queued_bytes=0,
        reason='queue-count-exhausted',
    )
    assert channel.drain() == ()


def test_range_changed_does_not_consume_event_capacity() -> None:
    channel = GroupChannel(group='editorial', ordering='eager', max_events=1, max_bytes=1_000_000)
    assert channel.offer_hint(fanout_module.RangeChanged())
    event = EventDelivery(_encoded(20))
    assert channel.offer(event)
    assert channel.status.queued_count == 1
    assert channel.status.queued_bytes == event.byte_count


def test_range_changed_is_coalesced_per_channel() -> None:
    fanout = EventFanout()
    eager = GroupChannel(group='editorial', ordering='eager', max_events=2, max_bytes=1_000_000)
    strict = GroupChannel(group='render', ordering='strict', max_events=2, max_bytes=1_000_000)
    fanout.attach(eager)
    fanout.attach(strict)
    fanout.publish_range_changed()
    fanout.publish_range_changed()
    assert eager.drain() == (fanout_module.RangeChanged(),)
    assert strict.drain() == (fanout_module.RangeChanged(),)


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_range_changed_offer_never_detaches_a_full_channel(
    ordering: Literal['eager', 'strict'],
) -> None:
    channel = GroupChannel(
        group='editorial',
        ordering=ordering,
        max_events=1,
        max_bytes=1_000_000,
    )
    event = EventDelivery(_encoded(20))
    assert channel.offer(event)
    assert channel.offer_hint(fanout_module.RangeChanged())
    assert channel.status.attached
    assert channel.drain() == (event, fanout_module.RangeChanged())


def test_slow_group_detaches_without_delaying_healthy_group() -> None:
    fanout = EventFanout()
    slow = GroupChannel(
        group='slow',
        ordering='eager',
        max_events=1,
        max_bytes=1_000_000,
    )
    healthy = GroupChannel(
        group='healthy',
        ordering='eager',
        max_events=10,
        max_bytes=10_000_000,
    )
    fanout.attach(slow)
    fanout.attach(healthy)
    events = (_encoded(1), _encoded(2))

    statuses = fanout.publish_events(events)

    assert {status.group for status in statuses} == {'healthy', 'slow'}
    assert not slow.status.attached
    assert slow.drain() == ()
    assert healthy.drain() == tuple(EventDelivery(event) for event in events)


def test_strict_capacity_and_ordering_maximum_identities_match_ipc_bytes() -> None:
    maximum = 2**63 - 1
    identity = LiveJoinIdentity(
        site='s' * 64,
        source='o' * 64,
        group='g' * 64,
        join_kind='strict',
        source_generation='f' * 32,
        group_generation=maximum,
        boundary=maximum,
        attempt_token='a' * 32,
    )
    controls = (
        StrictCapacityControl(identity=identity, abort_limit=maximum),
        StrictOrderingControl(
            identity=identity,
            event_id=maximum,
            last_delivered_id=maximum,
            delivery_floor=maximum,
            action='halt',
        ),
    )

    for control in controls:
        assert control.byte_count == len(b''.join(_encode_fanout_message(control)._parts))


def test_strict_group_never_receives_direct_event_frame() -> None:
    fanout = EventFanout()
    strict = GroupChannel(
        group='editorial',
        ordering='strict',
        max_events=10,
        max_bytes=10_000_000,
    )
    fanout.attach(strict)

    fanout.publish_events((_encoded(20),))

    assert strict.drain() == ()


def test_strict_paced_offer_reports_full_without_detaching_channel() -> None:
    channel = GroupChannel(
        group='editorial',
        ordering='strict',
        max_events=1,
        max_bytes=10_000_000,
    )
    first = EventDelivery(_encoded(20))
    second = EventDelivery(_encoded(21))

    assert channel.offer_paced(first)
    assert not channel.offer_paced(second)

    assert channel.status.attached
    assert channel.status.queued_count == 1
    assert channel.drain() == (first,)
