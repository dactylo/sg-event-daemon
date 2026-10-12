from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Unpack

import pytest

import sg_event_daemon._strict as strict_module
from sg_event_daemon._fanout import (
    EncodedEvent,
    EventDelivery,
    FanoutMessage,
    GroupChannel,
    GroupChannelInterface,
    StrictCapacityControl,
    StrictOrderingControl,
)
from sg_event_daemon._group_types import LiveJoinIdentity
from sg_event_daemon._source import (
    Attached,
    JoinFinalized,
    JoinProtocolError,
    JoinRetry,
    JoinTarget,
)
from sg_event_daemon._source_store import DiscoveryRange
from sg_event_daemon._strict import (
    JOIN_ABORT_LIMIT,
    OrderingAnomaly,
    StrictBuffer,
    _AttachGroupOptions,
    _StrictRelease,
    _StrictReleaseWatermark,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from typing import Any

_ATTEMPT_ID = 'attempt-1'
_FRESH_ATTEMPT_ID = 'attempt-2'
_JOINING_ATTEMPT_ID = 'newest'
_NO_HEADROOM_ATTEMPT_ID = 'no-headroom'
_SPARSE_ATTEMPT_ID = 'sparse-join'


class _PacedTestChannel(GroupChannel):
    def __init__(self, *, accept_count: int) -> None:
        super().__init__(
            group='editorial',
            ordering='strict',
            max_events=20_000,
            max_bytes=10_000_000,
        )
        self._accept_count = accept_count
        self._accepted_event_ids: list[int] = []
        self._offer_count = 0

    def offer_paced(self, message: FanoutMessage) -> bool:
        assert isinstance(message, EventDelivery)
        self._offer_count += 1
        if len(self._accepted_event_ids) >= self._accept_count:
            return False
        self._accepted_event_ids.append(message.encoded.event_id)
        return True


class _SortCounter:
    def __init__(self) -> None:
        self._count = 0

    def __call__(
        self,
        values: Iterable[Any],
        *,
        key: Callable[[object], Any] | None = None,
        reverse: bool = False,
    ) -> list[Any]:
        if key is None:
            self._count += 1
            return sorted(values, reverse=reverse)
        return sorted(values, key=key, reverse=reverse)


def _channel(
    group: str,
    *,
    max_events: int = 20,
    max_bytes: int = 10_000_000,
) -> GroupChannel:
    return GroupChannel(
        group=group,
        ordering='strict',
        max_events=max_events,
        max_bytes=max_bytes,
    )


def _encoded(event_id: int, payload_bytes: int = 1) -> EncodedEvent:
    return EncodedEvent.from_event_frame(event_id, b'x' * payload_bytes)


def _identity(
    group: str = 'editorial',
    *,
    boundary: int = 100,
    generation: int = 1,
    token: str | None = None,
) -> LiveJoinIdentity:
    return LiveJoinIdentity(
        site='studio',
        source='events',
        group=group,
        join_kind='strict',
        source_generation='source-1',
        group_generation=generation,
        boundary=boundary,
        attempt_token=_ATTEMPT_ID if token is None else token,
    )


def _range(
    obligation_id: str,
    low_id: int,
    high_id: int,
    *,
    opened_successful_at: float,
    eligibility_points: dict[str, float],
) -> DiscoveryRange:
    return DiscoveryRange(
        obligation_id=obligation_id,
        low_id=low_id,
        high_id=high_id,
        opened_successful_at=opened_successful_at,
        _visibility_deadline=opened_successful_at,
        _revision=1,
        _close_generation=0,
        state='open',
        late_ids=(),
        group_eligibility=tuple(eligibility_points.items()),
        _scan_started_at=None,
        scan_next_id=low_id,
        _scan_pages=0,
        _degraded=False,
    )


def _release_ids(buffer: StrictBuffer, group: str, successful_time: float) -> list[int]:
    return [release._event_id for release in buffer.pump_many((group,), successful_time)]


def attach_strict_group(
    buffer: StrictBuffer,
    group: str,
    generation: int,
    *,
    channel: GroupChannelInterface | None = None,
    **options: Unpack[_AttachGroupOptions],
) -> _StrictReleaseWatermark:
    if channel is None:
        channel = _channel(group, max_events=20_000)
    watermark = buffer.restore_group_policy(group, generation, **options)
    identity = _identity(
        group,
        generation=generation,
        boundary=options['delivery_floor'],
        token=f'{group}-{generation}',
    )
    assert buffer.reserve(identity) == JoinTarget(identity)
    assert buffer.prepare(identity, channel) == Attached(identity)
    assert buffer.finalize(identity) == JoinFinalized(identity)
    return watermark


def test_capacity_abort_limit_is_fixed_and_surfaces_a_halt() -> None:
    assert JOIN_ABORT_LIMIT == 8
    frame_bytes = _encoded(1).byte_count
    buffer = StrictBuffer(
        max_events=2,
        max_bytes=2 * frame_bytes,
        group_holdbacks={'retainer': 300.0, 'joining': 300.0},
    )
    attach_strict_group(buffer, 'retainer', 1, holdback_secs=300.0, delivery_floor=0)
    buffer.observe_many(
        (_encoded(event_id) for event_id in (1, 2)),
        successful_time=0.0,
    )

    responses: list[JoinRetry] = []
    for attempt in range(JOIN_ABORT_LIMIT):
        identity = _identity('joining', boundary=2, token=f'attempt-{attempt}')
        response = buffer.reserve(identity)
        assert isinstance(response, JoinRetry)
        responses.append(response)

    assert len(responses) == JOIN_ABORT_LIMIT
    assert buffer._capacity_aborts.get('joining', 0) == JOIN_ABORT_LIMIT
    assert buffer._halt_reasons.get('joining', None) == 'strict-capacity-exhausted'


def test_capacity_relief_history_keeps_latest_hundred_without_changing_halt() -> None:
    buffer = StrictBuffer(max_events=1, max_bytes=_encoded(1).byte_count)
    for group_number in range(101):
        attach_strict_group(
            buffer,
            f'group-{group_number:03}',
            1,
            holdback_secs=300.0,
            delivery_floor=0,
        )
    buffer._observe(_encoded(1), successful_time=0.0)

    buffer._observe(_encoded(2), successful_time=0.0)

    expected = tuple(
        [f'detach-group:group-{group_number:03}' for group_number in range(1, 100)]
        + ['halt-group:group-100']
    )
    assert buffer._relief_actions == expected
    assert buffer._watermark('group-100')._released_through == 0
    assert buffer._halt_reasons.get('group-100', None) == 'strict-capacity-exhausted'


def test_default_window_sustains_twenty_five_events_per_second_within_caps() -> None:
    events = tuple(_encoded(event_id) for event_id in range(1, 7_501))
    max_bytes = sum(event.byte_count for event in events)
    buffer = StrictBuffer(max_events=20_000, max_bytes=max_bytes)
    attach_strict_group(buffer, 'editorial', 1, holdback_secs=300.0, delivery_floor=0)
    for index, event in enumerate(events):
        buffer._observe(event, successful_time=index / 25)

    assert buffer.event_count == 7_500
    assert buffer._halt_reasons.get('editorial', None) is None
    releases = buffer.pump_many(('editorial',), 600.0)
    assert len(releases) == 7_500
    assert releases[0]._event_id == 1
    assert releases[-1]._event_id == 7_500


def test_detached_group_does_not_pin_shared_payloads() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'healthy', 1, holdback_secs=60.0, delivery_floor=99)
    attach_strict_group(
        buffer,
        'halted',
        1,
        holdback_secs=300.0,
        delivery_floor=99,
    )
    buffer._observe(_encoded(100), successful_time=0.0)
    assert _release_ids(buffer, 'healthy', 60.0) == [100]
    assert buffer.event_count == 1

    buffer.detach_group('halted', reason='channel-lost')
    assert buffer.event_count == 0
    assert not buffer._detached_halts.get('halted', False)


def test_free_unused_projects_leases_once(monkeypatch: pytest.MonkeyPatch) -> None:
    encoded_events = tuple(_encoded(event_id) for event_id in range(101, 105))
    buffer = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'joining': 1.0},
    )
    attach_strict_group(buffer, 'retainer', 1, holdback_secs=1.0, delivery_floor=100)
    buffer.observe_many(encoded_events, successful_time=0.0)
    identity = _identity('joining', boundary=102)
    assert buffer.reserve(identity) == JoinTarget(identity)

    lease_reads = 0
    original_leases = StrictBuffer._leases

    def counting_leases(instance: StrictBuffer) -> tuple[LiveJoinIdentity, ...]:
        nonlocal lease_reads
        lease_reads += 1
        return original_leases.__get__(instance, StrictBuffer)

    with monkeypatch.context() as patch:
        patch.setattr(StrictBuffer, '_leases', property(counting_leases))
        assert _release_ids(buffer, 'retainer', 1.0) == [101, 102, 103, 104]

    assert lease_reads == 1
    assert buffer.event_count == 2
    assert buffer.prepare(identity, _channel('joining')) == Attached(identity)
    assert buffer.finalize(identity) == JoinFinalized(identity)
    assert _release_ids(buffer, 'joining', 1.0) == [103, 104]


def test_groups_share_one_payload_but_keep_independent_release_progress() -> None:
    encoded = _encoded(100, payload_bytes=32)
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'fast', 1, holdback_secs=60.0, delivery_floor=99)
    attach_strict_group(buffer, 'slow', 1, holdback_secs=300.0, delivery_floor=99)
    buffer._observe(encoded, successful_time=0.0)

    assert buffer.event_count == 1
    assert buffer.byte_count == encoded.byte_count
    assert _release_ids(buffer, 'fast', 60.0) == [100]
    assert buffer.event_count == 1
    assert buffer._watermark('fast')._released_through == 100
    assert buffer._watermark('slow')._released_through == 99
    assert _release_ids(buffer, 'slow', 299.0) == []
    assert _release_ids(buffer, 'slow', 300.0) == [100]
    assert buffer.event_count == 0


@pytest.mark.parametrize('accept_count', [0, 7])
def test_incremental_release_constructs_only_the_synchronously_offered_prefix(
    monkeypatch: pytest.MonkeyPatch,
    accept_count: int,
) -> None:
    events = tuple(_encoded(event_id) for event_id in range(1, 20_001))
    inserted = _encoded(20_001)
    channel = _PacedTestChannel(accept_count=accept_count)
    buffer = StrictBuffer(
        max_events=len(events) + 1,
        max_bytes=sum(event.byte_count for event in events) + inserted.byte_count,
    )
    attach_strict_group(
        buffer,
        'editorial',
        1,
        holdback_secs=1.0,
        delivery_floor=0,
        channel=channel,
    )
    buffer.observe_many(events, successful_time=0.0)
    construction_count = 0
    sort_counter = _SortCounter()

    def counting_release(
        *,
        _group: str,
        _event_id: int,
        _encoded: EncodedEvent,
        _ordering_time: float,
        _is_operator_replay: bool,
    ) -> _StrictRelease:
        nonlocal construction_count
        construction_count += 1
        return _StrictRelease(
            _group=_group,
            _event_id=_event_id,
            _encoded=_encoded,
            _ordering_time=_ordering_time,
            _is_operator_replay=_is_operator_replay,
        )

    monkeypatch.setattr(strict_module, '_StrictRelease', counting_release)

    with monkeypatch.context() as patch:
        patch.setattr(strict_module, 'sorted', sort_counter, raising=False)
        accepted = buffer.pump_many(('editorial',), 1.0)

        expected_ids = list(range(1, accept_count + 1))
        assert [release._event_id for release in accepted] == expected_ids
        assert channel._accepted_event_ids == expected_ids
        assert channel._offer_count == accept_count + 1
        assert construction_count == accept_count + 1
        assert buffer._watermark('editorial')._released_through == accept_count
        assert buffer.event_count == len(events) - accept_count
        assert sort_counter._count == 1

        for _ in range(3):
            assert buffer.pump_many(('editorial',), 1.0) == ()

        expected_sorts = 1 if accept_count == 0 else 2
        assert sort_counter._count == expected_sorts
        assert construction_count == accept_count + 4
        assert buffer._watermark('editorial')._released_through == accept_count
        assert buffer.event_count == len(events) - accept_count

    buffer._observe(inserted, successful_time=0.0)
    with monkeypatch.context() as patch:
        patch.setattr(strict_module, 'sorted', sort_counter, raising=False)
        assert buffer.pump_many(('editorial',), 1.0) == ()

    assert sort_counter._count == expected_sorts + 1
    assert construction_count == accept_count + 5
    assert buffer._watermark('editorial')._released_through == accept_count
    assert buffer.event_count == len(events) - accept_count + 1


def test_invalid_or_foreign_strict_join_identity_is_rejected() -> None:
    buffer = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'editorial': 300.0},
    )
    eager = replace(_identity(), join_kind='eager')
    with pytest.raises(JoinProtocolError, match='strict'):
        buffer.reserve(eager)

    invalid = replace(_identity(), boundary=-1)
    with pytest.raises(JoinProtocolError, match='boundary'):
        buffer.reserve(invalid)


def test_join_lease_exists_before_release_above_boundary_and_abort_frees_payload() -> None:
    buffer = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'editorial': 1.0},
    )
    identity = _identity(boundary=100)
    assert buffer.reserve(identity) == JoinTarget(identity)
    assert buffer._leases == (identity,)

    buffer._observe(_encoded(101), successful_time=0.0)
    assert buffer.event_count == 1
    buffer.abort(identity)

    assert buffer._leases == ()
    assert buffer.event_count == 0


def test_join_prepare_finalize_abort_are_exact_idempotent_and_token_fenced() -> None:
    buffer = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'editorial': 1.0, 'retainer': 300.0},
    )
    attach_strict_group(buffer, 'retainer', 1, holdback_secs=300.0, delivery_floor=10)
    buffer.observe_many((_encoded(11), _encoded(12)), successful_time=0.0)
    identity = _identity(boundary=10)
    channel = _channel('editorial')

    assert buffer.reserve(identity) == JoinTarget(identity)
    assert buffer.prepare(identity, channel) == Attached(identity)
    assert buffer.prepare(identity, channel) == Attached(identity)
    assert buffer.finalize(identity) == JoinFinalized(identity)
    assert buffer.finalize(identity) == JoinFinalized(identity)
    assert 'editorial' in buffer._groups
    assert buffer._watermark('editorial')._released_through == 10

    buffer.abort(identity)
    buffer.abort(identity)
    assert 'editorial' not in buffer._groups
    fresh = replace(identity, attempt_token=_FRESH_ATTEMPT_ID)
    fresh_channel = _channel('editorial')
    assert buffer.reserve(fresh) == JoinTarget(fresh)
    assert buffer.prepare(fresh, fresh_channel) == Attached(fresh)
    buffer.abort(identity)
    assert buffer.finalize(fresh) == JoinFinalized(fresh)
    assert 'editorial' in buffer._groups

    with pytest.raises(JoinProtocolError, match='stale'):
        buffer.finalize(identity)


def test_join_reservation_rejects_insufficient_byte_headroom_with_event_capacity() -> None:
    initial = (_encoded(1, payload_bytes=17), _encoded(2, payload_bytes=31))
    retained_bytes = sum(event.byte_count for event in initial)
    max_events = 20
    max_bytes = 2 * retained_bytes - 1
    buffer = StrictBuffer(
        max_events=max_events,
        max_bytes=max_bytes,
        group_holdbacks={'existing': 300.0, 'joining': 300.0},
    )
    attach_strict_group(buffer, 'existing', 1, holdback_secs=300.0, delivery_floor=0)
    buffer.observe_many(initial, successful_time=0.0)
    assert buffer.event_count == 2
    assert buffer.byte_count == retained_bytes
    assert (max_events - buffer.event_count) * 2 >= max_events
    assert (max_bytes - buffer.byte_count) * 2 < max_bytes
    identity = _identity('joining', boundary=2)

    retry = buffer.reserve(identity)

    assert isinstance(retry, JoinRetry)
    assert retry.headroom_events == max_events - len(initial)
    assert retry.headroom_bytes == retained_bytes - 1
    assert buffer._leases == ()
    assert tuple(item._encoded for item in buffer._observations) == initial
    assert buffer.byte_count == retained_bytes
    assert buffer._relief_actions == ()


def test_join_reservation_uses_measured_half_headroom_not_id_distance() -> None:
    initial = (_encoded(1), _encoded(1_000_000))
    buffer = StrictBuffer(
        max_events=4,
        max_bytes=2 * sum(event.byte_count for event in initial),
        group_holdbacks={'existing': 300.0, 'sparse': 300.0},
    )
    attach_strict_group(buffer, 'existing', 1, holdback_secs=300.0, delivery_floor=0)
    buffer.observe_many(initial, successful_time=0.0)

    sparse = _identity(
        'sparse',
        boundary=1_000_000,
        token=_SPARSE_ATTEMPT_ID,
    )
    assert buffer.reserve(sparse) == JoinTarget(sparse)
    buffer.abort(sparse)
    buffer._observe(_encoded(2_000_000), successful_time=0.0)
    retry_identity = replace(
        sparse,
        attempt_token=_NO_HEADROOM_ATTEMPT_ID,
    )
    retry = buffer.reserve(retry_identity)

    assert isinstance(retry, JoinRetry)
    assert retry.headroom_events == 1
    assert retry.headroom_bytes == _encoded(1).byte_count


def test_late_discovery_halts_above_floor_and_skips_body_free_below_floor() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(
        buffer,
        'anomalous',
        4,
        holdback_secs=60.0,
        delivery_floor=50,
        last_delivered_id=102,
    )
    attach_strict_group(
        buffer,
        'floored',
        4,
        holdback_secs=60.0,
        delivery_floor=102,
        last_delivered_id=102,
    )

    decisions = buffer._observe(_encoded(101), successful_time=100.0)

    assert decisions == (
        OrderingAnomaly(
            group='anomalous',
            group_generation=4,
            event_id=101,
            last_delivered_id=102,
            delivery_floor=50,
            action='halt',
        ),
        OrderingAnomaly(
            group='floored',
            group_generation=4,
            event_id=101,
            last_delivered_id=102,
            delivery_floor=102,
            action='skip',
        ),
    )
    assert buffer._halt_reasons.get('anomalous', None) == 'ordering-anomaly'
    assert buffer._halt_reasons.get('floored', None) is None
    assert buffer.event_count == 0


def test_lower_held_observation_and_overlapping_ranges_block_higher_rows() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'editorial', 1, holdback_secs=60.0, delivery_floor=99)
    buffer._observe(_encoded(100), successful_time=10.0)
    buffer._observe(_encoded(102), successful_time=0.0)
    buffer_ranges: dict[str, DiscoveryRange] = {}
    buffer_ranges['lower-a'] = _range(
        'lower-a', 101, 101, opened_successful_at=0.0, eligibility_points={'editorial': 60.0}
    )
    buffer.synchronize_ranges(buffer_ranges.values())
    buffer_ranges['lower-b'] = _range(
        'lower-b', 100, 101, opened_successful_at=5.0, eligibility_points={'editorial': 65.0}
    )
    buffer.synchronize_ranges(buffer_ranges.values())

    assert _release_ids(buffer, 'editorial', 60.0) == []
    buffer_ranges['lower-a'] = replace(buffer_ranges['lower-a'], state='closed')
    buffer.synchronize_ranges(buffer_ranges.values())
    assert _release_ids(buffer, 'editorial', 69.0) == []
    buffer_ranges['lower-b'] = replace(buffer_ranges['lower-b'], state='closed')
    buffer.synchronize_ranges(buffer_ranges.values())
    assert _release_ids(buffer, 'editorial', 70.0) == [100, 102]


def test_merged_range_uses_earliest_ordering_time_and_latest_eligibility() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'editorial', 1, holdback_secs=10.0, delivery_floor=99)
    buffer_ranges: dict[str, DiscoveryRange] = {}
    buffer_ranges['early'] = _range(
        'early', 100, 101, opened_successful_at=0.0, eligibility_points={'editorial': 50.0}
    )
    buffer.synchronize_ranges(buffer_ranges.values())
    buffer_ranges['late'] = _range(
        'late', 101, 102, opened_successful_at=20.0, eligibility_points={'editorial': 100.0}
    )
    buffer.synchronize_ranges(buffer_ranges.values())
    buffer._observe(
        _encoded(101),
        successful_time=90.0,
        range_ids=('early', 'late'),
    )
    buffer_ranges['early'] = replace(buffer_ranges['early'], state='closed')
    buffer.synchronize_ranges(buffer_ranges.values())
    buffer_ranges['late'] = replace(buffer_ranges['late'], state='closed')
    buffer.synchronize_ranges(buffer_ranges.values())

    observation = buffer._observations[0]
    assert observation._ordering_time == 0.0
    assert _release_ids(buffer, 'editorial', 99.0) == []
    assert _release_ids(buffer, 'editorial', 100.0) == [101]


def test_million_id_gap_uses_one_range_and_one_observation() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'restricted', 1, holdback_secs=60.0, delivery_floor=0)
    buffer_ranges: dict[str, DiscoveryRange] = {}
    buffer_ranges['sparse-gap'] = _range(
        'sparse-gap', 1, 999999, opened_successful_at=0.0, eligibility_points={'restricted': 60.0}
    )
    buffer.synchronize_ranges(buffer_ranges.values())
    buffer._observe(_encoded(1_000_000), successful_time=0.0)

    assert len(buffer._ranges) == 1
    assert buffer.event_count == 1


def test_multi_group_pump_reclaims_once(monkeypatch: pytest.MonkeyPatch) -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    for group in ('alpha', 'beta', 'gamma'):
        attach_strict_group(
            buffer,
            group,
            1,
            holdback_secs=1.0,
            delivery_floor=100,
            channel=_channel(group),
        )
    buffer.observe_many((_encoded(101), _encoded(102)), successful_time=0.0)
    floor_calls = 0
    reclamation_calls = 0
    original_floor = buffer._retention_floor
    original_reclamation = buffer._free_unused

    def counted_floor() -> int | None:
        nonlocal floor_calls
        floor_calls += 1
        return original_floor()

    def counted_reclamation() -> None:
        nonlocal reclamation_calls
        reclamation_calls += 1
        original_reclamation()

    monkeypatch.setattr(buffer, '_retention_floor', counted_floor)
    monkeypatch.setattr(buffer, '_free_unused', counted_reclamation)

    releases = buffer.pump_many((group for group in ('alpha', 'beta', 'gamma')), 1.0)

    assert [(item._group, item._event_id) for item in releases] == [
        ('alpha', 101),
        ('alpha', 102),
        ('beta', 101),
        ('beta', 102),
        ('gamma', 101),
        ('gamma', 102),
    ]
    assert floor_calls == 1
    assert reclamation_calls == 1
    assert buffer.event_count == 0


def test_observe_projects_leases_only_without_group_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lease_reads = 0
    original_leases = StrictBuffer._leases

    def counting_leases(instance: StrictBuffer) -> tuple[LiveJoinIdentity, ...]:
        nonlocal lease_reads
        lease_reads += 1
        return original_leases.__get__(instance, StrictBuffer)

    with monkeypatch.context() as patch:
        patch.setattr(StrictBuffer, '_leases', property(counting_leases))
        group_buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
        attach_strict_group(
            group_buffer,
            'retainer',
            1,
            holdback_secs=1.0,
            delivery_floor=100,
        )
        group_buffer._observe(_encoded(101), successful_time=0.0)

        assert group_buffer.event_count == 1
        assert lease_reads == 0

        lease_buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
        identity = _identity('joining', boundary=100)
        assert lease_buffer.reserve(identity) == JoinTarget(identity)
        lease_buffer._observe(_encoded(101), successful_time=0.0)

        assert lease_reads == 2
        assert lease_buffer.event_count == 1


def test_open_range_overlapping_the_release_watermark_still_blocks_higher_rows() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'editorial', 1, holdback_secs=1.0, delivery_floor=99)
    buffer._observe(_encoded(100), successful_time=0.0)
    assert _release_ids(buffer, 'editorial', 1.0) == [100]
    buffer_ranges: dict[str, DiscoveryRange] = {}
    buffer_ranges['overlapping'] = _range(
        'overlapping', 50, 101, opened_successful_at=0.0, eligibility_points={'editorial': 1.0}
    )
    buffer.synchronize_ranges(buffer_ranges.values())
    buffer._observe(_encoded(102), successful_time=0.0)

    assert _release_ids(buffer, 'editorial', 1.0) == []
    buffer_ranges['overlapping'] = replace(buffer_ranges['overlapping'], state='closed')
    buffer.synchronize_ranges(buffer_ranges.values())
    assert _release_ids(buffer, 'editorial', 1.0) == [102]


def test_ordered_observation_view_sorts_once_per_key_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(
        buffer,
        'editorial',
        1,
        holdback_secs=1.0,
        delivery_floor=0,
        channel=_channel('editorial', max_events=1),
    )
    buffer_ranges: dict[str, DiscoveryRange] = {}
    for range_id in ('range-a', 'range-b'):
        buffer_ranges[range_id] = _range(
            range_id, 1, 1, opened_successful_at=0.0, eligibility_points={'editorial': 0.0}
        )
        buffer.synchronize_ranges(buffer_ranges.values())
        buffer_ranges[range_id] = replace(buffer_ranges[range_id], state='closed')
        buffer.synchronize_ranges(buffer_ranges.values())
    first = _encoded(1)
    buffer._observe(first, successful_time=0.0, range_ids=('range-a',))

    sort_counter = _SortCounter()

    with monkeypatch.context() as patch:
        patch.setattr(strict_module, 'sorted', sort_counter, raising=False)
        assert [item._event_id for item in buffer._observations] == [1]
        assert [item._event_id for item in buffer._observations] == [1]
        assert buffer.pump_many(('editorial',), 0.5) == ()
    assert sort_counter._count == 1

    buffer._observe(first, successful_time=1.0, range_ids=('range-b',))
    sort_counter = _SortCounter()
    with monkeypatch.context() as patch:
        patch.setattr(strict_module, 'sorted', sort_counter, raising=False)
        assert buffer._observations[0]._range_ids == ('range-a', 'range-b')
    assert sort_counter._count == 0

    buffer._observe(_encoded(2), successful_time=0.0)
    sort_counter = _SortCounter()
    with monkeypatch.context() as patch:
        patch.setattr(strict_module, 'sorted', sort_counter, raising=False)
        assert [item._event_id for item in buffer._observations] == [1, 2]
        assert [item._event_id for item in buffer._observations] == [1, 2]
    assert sort_counter._count == 1

    assert [release._event_id for release in buffer.pump_many(('editorial',), 1.0)] == [1]
    sort_counter = _SortCounter()
    with monkeypatch.context() as patch:
        patch.setattr(strict_module, 'sorted', sort_counter, raising=False)
        assert [item._event_id for item in buffer._observations] == [2]
        assert [item._event_id for item in buffer._observations] == [2]
    assert sort_counter._count == 1


def test_ordering_decisions_preserve_input_order_without_changing_delivery() -> None:
    buffer = StrictBuffer(max_events=1, max_bytes=_encoded(1).byte_count)
    initial = attach_strict_group(
        buffer,
        'editorial',
        7,
        holdback_secs=60.0,
        delivery_floor=200,
    )

    decisions = buffer.observe_many(
        (_encoded(event_id) for event_id in range(1, 102)),
        successful_time=0.0,
    )

    assert tuple(item.event_id for item in decisions) == tuple(range(1, 102))
    assert all(item.action == 'skip' for item in decisions)
    assert buffer._watermark('editorial') == initial
    assert buffer._halt_reasons.get('editorial', None) is None


def test_queue_pacing_fills_all_current_strict_channel_space() -> None:
    channel = _channel('editorial', max_events=3)
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(
        buffer,
        'editorial',
        1,
        holdback_secs=1.0,
        delivery_floor=99,
        channel=channel,
    )
    buffer.observe_many(
        tuple(_encoded(event_id) for event_id in range(100, 104)),
        successful_time=0.0,
    )

    accepted = buffer.pump_many(('editorial',), 1.0)

    assert [item._event_id for item in accepted] == [100, 101, 102]
    assert channel.status.attached
    assert channel.status.queued_count == 3
    channel.drain()
    assert [item._event_id for item in buffer.pump_many(('editorial',), 1.0)] == [103]


def test_queue_pacing_waits_for_space_without_detaching_strict_channel() -> None:
    channel = _channel('editorial', max_events=1)
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(
        buffer,
        'editorial',
        1,
        holdback_secs=1.0,
        delivery_floor=99,
        channel=channel,
    )
    buffer.observe_many((_encoded(100), _encoded(101)), successful_time=0.0)

    assert [item._event_id for item in buffer.pump_many(('editorial',), 1.0)] == [100]
    assert buffer.pump_many(('editorial',), 1.0) == ()
    assert channel.status.attached
    first = channel.drain()
    assert len(first) == 1
    assert isinstance(first[0], EventDelivery)
    assert [item._event_id for item in buffer.pump_many(('editorial',), 1.0)] == [101]
    assert channel.status.attached


def test_range_expiry_releases_sparse_rows_without_allocating_missing_ids() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'editorial', 1, holdback_secs=300.0, delivery_floor=99)
    buffer.observe_many((_encoded(100), _encoded(102)), successful_time=0.0)
    buffer_ranges: dict[str, DiscoveryRange] = {}
    buffer_ranges['range-101'] = _range(
        'range-101', 101, 101, opened_successful_at=0.0, eligibility_points={'editorial': 300.0}
    )
    buffer.synchronize_ranges(buffer_ranges.values())

    assert len(buffer._ranges) == 1
    assert buffer.event_count == 2
    assert _release_ids(buffer, 'editorial', 300.0) == [100]
    buffer_ranges['range-101'] = replace(buffer_ranges['range-101'], state='closed')
    buffer.synchronize_ranges(buffer_ranges.values())
    assert _release_ids(buffer, 'editorial', 300.0) == [102]


def test_rebuild_restarts_ordinary_holdback_but_preserves_range_timing() -> None:
    before_crash = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(before_crash, 'ordinary', 1, holdback_secs=300.0, delivery_floor=99)
    before_crash._observe(_encoded(100), successful_time=0.0)
    assert _release_ids(before_crash, 'ordinary', 299.0) == []

    rebuilt = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(rebuilt, 'ordinary', 1, holdback_secs=300.0, delivery_floor=99)
    rebuilt._observe(_encoded(100), successful_time=300.0)
    assert _release_ids(rebuilt, 'ordinary', 599.0) == []
    assert _release_ids(rebuilt, 'ordinary', 600.0) == [100]

    range_rebuilt = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(range_rebuilt, 'ranged', 1, holdback_secs=300.0, delivery_floor=99)
    range_rebuilt_ranges: dict[str, DiscoveryRange] = {}
    range_rebuilt_ranges['durable-range'] = _range(
        'durable-range', 100, 100, opened_successful_at=0.0, eligibility_points={'ranged': 300.0}
    )
    range_rebuilt.synchronize_ranges(range_rebuilt_ranges.values())
    range_rebuilt_ranges['durable-range'] = replace(
        range_rebuilt_ranges['durable-range'], state='closed'
    )
    range_rebuilt.synchronize_ranges(range_rebuilt_ranges.values())
    range_rebuilt._observe(
        _encoded(100),
        successful_time=300.0,
        range_ids=('durable-range',),
    )
    assert _release_ids(range_rebuilt, 'ranged', 300.0) == [100]


def test_reclamation_floor_matches_payload_retention_truth_table() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'alpha', 1, holdback_secs=10.0, delivery_floor=101)
    attach_strict_group(buffer, 'beta', 1, holdback_secs=10.0, delivery_floor=103)
    first_lease = _identity('joining-a', boundary=100, token=_ATTEMPT_ID)
    second_lease = _identity('joining-b', boundary=104, token=_FRESH_ATTEMPT_ID)
    assert buffer.reserve(first_lease) == JoinTarget(first_lease)
    assert buffer.reserve(second_lease) == JoinTarget(second_lease)
    buffer.observe_many(
        tuple(_encoded(event_id) for event_id in range(99, 106)),
        successful_time=0.0,
    )

    assert buffer._retention_floor() == 100
    assert [item._event_id for item in buffer._observations] == [101, 102, 103, 104, 105]

    buffer.abort(first_lease)
    assert buffer._retention_floor() == 101
    assert [item._event_id for item in buffer._observations] == [102, 103, 104, 105]

    buffer._groups['alpha']._capacity_pending = True
    buffer._free_unused()
    assert buffer._retention_floor() == 103
    assert [item._event_id for item in buffer._observations] == [104, 105]

    buffer._groups['beta']._capacity_pending = True
    buffer.abort(second_lease)
    assert buffer._retention_floor() is None
    assert buffer._observations == ()


def test_release_waits_for_late_range_and_emits_the_complete_ascending_prefix() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'editorial', 1, holdback_secs=300.0, delivery_floor=99)
    buffer.observe_many((_encoded(100), _encoded(102)), successful_time=0.0)
    buffer_ranges: dict[str, DiscoveryRange] = {}
    buffer_ranges['range-101'] = _range(
        'range-101', 101, 101, opened_successful_at=0.0, eligibility_points={'editorial': 300.0}
    )
    buffer.synchronize_ranges(buffer_ranges.values())
    buffer._observe(
        _encoded(101),
        successful_time=299.0,
        range_ids=('range-101',),
    )

    assert _release_ids(buffer, 'editorial', 299.0) == []
    buffer_ranges['range-101'] = replace(buffer_ranges['range-101'], state='closed')
    buffer.synchronize_ranges(buffer_ranges.values())
    assert _release_ids(buffer, 'editorial', 300.0) == [100, 101, 102]


def test_relief_aborts_newest_join_then_detaches_lowest_watermark_stably() -> None:
    frame_bytes = _encoded(1).byte_count
    buffer = StrictBuffer(
        max_events=2,
        max_bytes=2 * frame_bytes,
        group_holdbacks={'alpha': 300.0, 'beta': 300.0, 'joining': 300.0},
    )
    attach_strict_group(buffer, 'alpha', 1, holdback_secs=300.0, delivery_floor=0)
    attach_strict_group(buffer, 'beta', 1, holdback_secs=300.0, delivery_floor=1)
    joining = _identity('joining', boundary=1, token=_JOINING_ATTEMPT_ID)
    assert buffer.reserve(joining) == JoinTarget(joining)
    buffer.observe_many((_encoded(1), _encoded(2)), successful_time=0.0)

    buffer._observe(_encoded(3), successful_time=0.0)

    assert buffer._relief_actions[:2] == (
        'abort-join:joining:newest',
        'detach-group:alpha',
    )
    assert 'alpha' not in buffer._groups
    assert 'beta' in buffer._groups
    assert tuple(item._event_id for item in buffer._observations) == (2, 3)


def test_relief_detaches_lowest_watermark_for_bytes_with_event_capacity() -> None:
    first = _encoded(1, payload_bytes=17)
    second = _encoded(2, payload_bytes=31)
    third = _encoded(3, payload_bytes=59)
    max_events = 20
    max_bytes = second.byte_count + third.byte_count
    buffer = StrictBuffer(max_events=max_events, max_bytes=max_bytes)
    attach_strict_group(buffer, 'alpha', 1, holdback_secs=300.0, delivery_floor=0)
    attach_strict_group(buffer, 'beta', 1, holdback_secs=300.0, delivery_floor=1)
    buffer.observe_many((first, second), successful_time=0.0)
    assert buffer.event_count == 2
    assert buffer.byte_count == first.byte_count + second.byte_count
    assert buffer.byte_count <= max_bytes
    assert buffer.event_count + 1 < max_events
    assert buffer.byte_count + third.byte_count > max_bytes
    assert buffer._relief_actions == ()

    buffer._observe(third, successful_time=0.0)

    assert buffer._relief_actions == ('detach-group:alpha',)
    assert 'alpha' not in buffer._groups
    assert 'beta' in buffer._groups
    assert buffer._halt_reasons.get('beta', None) is None
    assert buffer._watermark('beta')._released_through == 1
    assert tuple(item._encoded for item in buffer._observations) == (second, third)
    assert buffer.event_count == 2
    assert buffer.byte_count == max_bytes


def test_repeated_retained_observation_does_not_create_a_false_anomaly() -> None:
    encoded = _encoded(100)
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'fast', 1, holdback_secs=1.0, delivery_floor=99)
    attach_strict_group(buffer, 'slow', 1, holdback_secs=300.0, delivery_floor=99)
    buffer.observe_many((encoded, _encoded(101)), successful_time=0.0)
    assert _release_ids(buffer, 'fast', 1.0) == [100, 101]

    decisions = buffer._observe(encoded, successful_time=2.0)

    assert decisions == ()
    assert buffer._halt_reasons.get('fast', None) is None
    assert buffer.event_count == 2


def test_replay_generation_resets_ordering_comparison_and_marks_release() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(
        buffer,
        'editorial',
        8,
        holdback_secs=1.0,
        delivery_floor=99,
        last_delivered_id=99,
        is_operator_replay=True,
    )
    decisions = buffer.observe_many((_encoded(100), _encoded(101)), successful_time=0.0)

    releases = buffer.pump_many(('editorial',), 1.0)

    assert [release._event_id for release in releases] == [100, 101]
    assert all(release._is_operator_replay for release in releases)
    assert decisions == ()


def test_restart_restores_only_body_free_policy_and_durable_range_timing() -> None:
    rebuilt = StrictBuffer(max_events=20, max_bytes=10_000_000)
    rebuilt.synchronize_ranges(
        iter(
            (
                DiscoveryRange(
                    obligation_id='durable-range',
                    low_id=104,
                    high_id=106,
                    opened_successful_at=5.0,
                    _visibility_deadline=10.0,
                    _revision=2,
                    _close_generation=0,
                    state='open',
                    late_ids=(104,),
                    group_eligibility=(('editorial', 15.0),),
                    _scan_started_at=None,
                    scan_next_id=104,
                    _scan_pages=0,
                    _degraded=False,
                ),
            )
        )
    )

    watermark = rebuilt.restore_group_policy(
        'editorial',
        7,
        holdback_secs=10.0,
        delivery_floor=100,
        last_delivered_id=103,
        halted=False,
        is_operator_replay=True,
    )

    assert 'editorial' not in rebuilt._groups
    assert rebuilt.event_count == 0
    assert rebuilt.byte_count == 0
    assert len(rebuilt._ranges) == 1
    assert rebuilt.range_ids_for(104) == ('durable-range',)
    assert watermark._delivery_floor == 100
    assert watermark._last_delivered_id == 103
    assert watermark._is_operator_replay


def test_restored_floor_above_join_boundary_remains_the_comparison_point() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    buffer.restore_group_policy(
        'editorial',
        7,
        holdback_secs=10.0,
        delivery_floor=57,
        last_delivered_id=57,
    )
    identity = _identity(boundary=41, generation=7)
    channel = _channel('editorial')

    assert buffer.reserve(identity) == JoinTarget(identity)
    assert buffer.prepare(identity, channel) == Attached(identity)
    watermark = buffer._watermark('editorial')

    assert watermark._delivery_floor == 57
    assert watermark._last_delivered_id == 57
    assert watermark._released_through == 57


def test_retention_health_counts_each_groups_remaining_payloads() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    assert buffer.retention_health() == ()
    attach_strict_group(buffer, 'beta', 1, holdback_secs=10.0, delivery_floor=102)
    attach_strict_group(
        buffer,
        'alpha',
        1,
        holdback_secs=10.0,
        delivery_floor=100,
        channel=_channel('alpha', max_events=1),
    )
    first, second, third = (
        _encoded(101, payload_bytes=17),
        _encoded(102, payload_bytes=31),
        _encoded(103, payload_bytes=59),
    )
    buffer.observe_many((first, second, third), successful_time=0.0)

    snapshot = buffer.retention_health()
    assert snapshot == (
        ('alpha', 100, 3, first.byte_count + second.byte_count + third.byte_count),
        ('beta', 102, 1, third.byte_count),
    )
    assert [item._event_id for item in buffer.pump_many(('alpha',), 10.0)] == [101]
    assert buffer.retention_health() == (
        ('alpha', 101, 2, second.byte_count + third.byte_count),
        ('beta', 102, 1, third.byte_count),
    )
    assert snapshot[0][1:3] == (100, 3)

    buffer.detach_group('alpha', reason='channel-lost')
    assert buffer.retention_health() == (('beta', 102, 1, third.byte_count),)
    beta_channel = buffer._groups['beta']._channel
    assert beta_channel is not None
    for event_id in (101, 102):
        assert buffer.pump_many(('beta',), 10.0) == ()
        (control,) = beta_channel.drain()
        assert isinstance(control, StrictOrderingControl)
        assert control.action == 'skip'
        assert control.event_id == event_id
        buffer.acknowledge_control(control)
    assert _release_ids(buffer, 'beta', 10.0) == [103]
    assert buffer.retention_health() == (('beta', 103, 0, 0),)
    buffer.detach_group('beta', reason='channel-lost')
    assert buffer.retention_health() == ()


def test_retention_health_includes_leases_until_finalize_or_abort() -> None:
    buffer = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'alpha': 10.0, 'beta': 10.0},
    )
    alpha = _identity('alpha', boundary=100)
    beta = _identity('beta', boundary=102, token=_FRESH_ATTEMPT_ID)
    assert buffer.reserve(alpha) == JoinTarget(alpha)
    assert buffer.reserve(beta) == JoinTarget(beta)
    first, second = _encoded(101, payload_bytes=17), _encoded(103, payload_bytes=31)
    buffer.observe_many((first, second), successful_time=0.0)
    expected = (
        ('alpha', 100, 2, first.byte_count + second.byte_count),
        ('beta', 102, 1, second.byte_count),
    )
    assert buffer.retention_health() == expected

    assert buffer.prepare(alpha, _channel('alpha')) == Attached(alpha)
    assert _release_ids(buffer, 'alpha', 10.0) == [101, 103]
    assert buffer.retention_health() == expected
    assert buffer.finalize(alpha) == JoinFinalized(alpha)
    assert buffer.retention_health() == (
        ('alpha', 103, 0, 0),
        ('beta', 102, 1, second.byte_count),
    )
    buffer.abort(beta)
    assert buffer.retention_health() == (('alpha', 103, 0, 0),)
    buffer.abort(alpha)
    assert buffer.retention_health() == ()


def test_retention_health_matches_restored_and_capacity_pending_ownership() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    buffer.restore_group_policy('restored', 1, holdback_secs=10.0, delivery_floor=0)
    assert buffer.retention_health() == ()
    attach_strict_group(buffer, 'halted', 1, holdback_secs=10.0, delivery_floor=100)
    encoded = _encoded(101)
    buffer._observe(encoded, successful_time=0.0)
    assert buffer.retention_health() == (('halted', 100, 1, encoded.byte_count),)

    buffer._groups['halted']._capacity_pending = True
    buffer._free_unused()
    assert buffer.retention_health() == ()


def test_retired_range_releases_retained_observation_but_is_unknown_for_new_events() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'editorial', 1, holdback_secs=10.0, delivery_floor=100)
    buffer.synchronize_ranges(
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
    buffer._observe(
        _encoded(101),
        successful_time=10.0,
        range_ids=('range-101',),
    )

    assert _release_ids(buffer, 'editorial', 10.0) == []

    buffer.synchronize_ranges(())

    assert _release_ids(buffer, 'editorial', 10.0) == [101]
    with pytest.raises(ValueError, match='unknown ranges'):
        buffer._observe(
            _encoded(102),
            successful_time=10.0,
            range_ids=('range-101',),
        )


def test_single_unfittable_live_window_halts_without_advancing() -> None:
    first = _encoded(1)
    second = _encoded(2)
    buffer = StrictBuffer(max_events=1, max_bytes=max(first.byte_count, second.byte_count))
    attach_strict_group(buffer, 'only', 1, holdback_secs=300.0, delivery_floor=0)
    buffer._observe(first, successful_time=0.0)

    buffer._observe(second, successful_time=0.0)

    assert buffer._halt_reasons.get('only', None) == 'strict-capacity-exhausted'
    assert buffer._watermark('only')._released_through == 0
    assert buffer.event_count == 0


def test_strict_buffer_counts_each_complete_frame_once() -> None:
    first = _encoded(100, payload_bytes=17)
    second = _encoded(101, payload_bytes=31)
    buffer = StrictBuffer(
        max_events=2,
        max_bytes=first.byte_count + second.byte_count,
    )
    attach_strict_group(buffer, 'editorial', 1, holdback_secs=300.0, delivery_floor=99)

    buffer._observe(first, successful_time=0.0)
    buffer._observe(first, successful_time=1.0)
    buffer._observe(second, successful_time=0.0)

    assert buffer.event_count == 2
    assert buffer.byte_count == first.byte_count + second.byte_count
    assert tuple(item._event_id for item in buffer._observations) == (100, 101)


def test_strict_control_generation_replacement_discards_old_control() -> None:
    first = _encoded(1)
    second = _encoded(2)
    buffer = StrictBuffer(
        max_events=1,
        max_bytes=max(first.byte_count, second.byte_count),
        group_holdbacks={'editorial': 300.0},
        group_delivery_floors={'editorial': 0},
    )
    identity = _identity(boundary=0)
    channel = _channel('editorial')
    assert buffer.reserve(identity) == JoinTarget(identity)
    assert buffer.prepare(identity, channel) == Attached(identity)
    assert buffer.finalize(identity) == JoinFinalized(identity)
    buffer._observe(first, successful_time=0.0)
    buffer._observe(second, successful_time=0.0)
    assert buffer.pump_many(('editorial',), 0.0) == ()
    (old_control,) = channel.drain()
    assert isinstance(old_control, StrictCapacityControl)
    channel.detach('channel-lost-before-control-commit')
    assert buffer.pump_many(('editorial',), 0.0) == ()
    buffer.abort(identity)

    buffer.restore_group_policy(
        'editorial',
        2,
        holdback_secs=300.0,
        delivery_floor=0,
        halted=False,
    )
    replacement = _identity(boundary=0, generation=2, token=_FRESH_ATTEMPT_ID)
    replacement_channel = _channel('editorial')
    assert buffer.reserve(replacement) == JoinTarget(replacement)
    assert buffer.prepare(replacement, replacement_channel) == Attached(replacement)
    assert buffer.finalize(replacement) == JoinFinalized(replacement)

    assert buffer.pump_many(('editorial',), 0.0) == ()
    assert replacement_channel.drain() == ()
    assert buffer._halt_reasons.get('editorial', None) is None
    buffer._observe(first, successful_time=0.0)
    assert _release_ids(buffer, 'editorial', 300.0) == [1]
    with pytest.raises(JoinProtocolError, match='stale'):
        buffer.acknowledge_control(old_control)


def test_strict_control_rebind_rejects_future_generation() -> None:
    first = _encoded(1)
    second = _encoded(2)
    buffer = StrictBuffer(
        max_events=1,
        max_bytes=max(first.byte_count, second.byte_count),
        group_holdbacks={'editorial': 300.0},
        group_delivery_floors={'editorial': 0},
    )
    future = _identity(boundary=0, generation=2)
    channel = _channel('editorial')
    assert buffer.reserve(future) == JoinTarget(future)
    assert buffer.prepare(future, channel) == Attached(future)
    assert buffer.finalize(future) == JoinFinalized(future)
    buffer._observe(first, successful_time=0.0)
    buffer._observe(second, successful_time=0.0)
    assert buffer.pump_many(('editorial',), 0.0) == ()
    channel.drain()
    channel.detach('channel-lost-before-control-commit')
    assert buffer.pump_many(('editorial',), 0.0) == ()
    buffer.abort(future)

    buffer.restore_group_policy(
        'editorial',
        1,
        holdback_secs=300.0,
        delivery_floor=0,
        halted=False,
    )
    stale_registration = _identity(boundary=0, generation=1, token=_FRESH_ATTEMPT_ID)
    assert buffer.reserve(stale_registration) == JoinTarget(stale_registration)
    with pytest.raises(JoinProtocolError, match='generation'):
        buffer.prepare(stale_registration, _channel('editorial'))


def test_strict_halt_reason_transition_table_stays_synchronized() -> None:
    frame_bytes = _encoded(1).byte_count
    buffer = StrictBuffer(
        max_events=2,
        max_bytes=2 * frame_bytes,
        group_holdbacks={'blocker': 300.0, 'editorial': 300.0},
        group_delivery_floors={'editorial': 0},
    )
    attach_strict_group(buffer, 'blocker', 1, holdback_secs=300.0, delivery_floor=0)
    buffer.observe_many((_encoded(1), _encoded(2)), successful_time=0.0)
    for attempt_number in range(JOIN_ABORT_LIMIT):
        response = buffer.reserve(
            _identity('editorial', boundary=2, token=f'capacity-{attempt_number}')
        )
        assert isinstance(response, JoinRetry)

    assert buffer._detached_halts.get('editorial', False)
    assert buffer._halt_reasons.get('editorial', None) == 'strict-capacity-exhausted'

    buffer.detach_group('blocker', reason='test-relief')
    buffer.restore_group_policy(
        'editorial',
        1,
        holdback_secs=300.0,
        delivery_floor=0,
        halted=True,
    )
    assert buffer._detached_halts.get('editorial', False)
    assert buffer._halt_reasons.get('editorial', None) == 'strict-capacity-exhausted'

    recovered = _identity('editorial', boundary=0, token=_FRESH_ATTEMPT_ID)
    assert buffer.reserve(recovered) == JoinTarget(recovered)
    assert buffer.prepare(recovered, _channel('editorial')) == Attached(recovered)
    assert buffer.finalize(recovered) == JoinFinalized(recovered)
    assert buffer._halt_reasons.get('editorial', None) is None
    buffer._observe(_encoded(1), successful_time=0.0)
    assert _release_ids(buffer, 'editorial', 300.0) == [1]

    buffer.detach_group('editorial', reason='test-healthy-detach')
    assert not buffer._detached_halts.get('editorial', False)
    assert buffer._halt_reasons.get('editorial', None) is None

    buffer.restore_group_policy(
        'editorial',
        2,
        holdback_secs=300.0,
        delivery_floor=0,
        halted=False,
    )
    assert not buffer._detached_halts.get('editorial', False)
    assert buffer._halt_reasons.get('editorial', None) is None


def test_strict_join_terminal_history_is_bounded_to_current_group_token() -> None:
    buffer = StrictBuffer(
        max_events=20,
        max_bytes=10_000_000,
        group_holdbacks={'editorial': 1.0},
    )
    first_identity: LiveJoinIdentity | None = None
    current_identity: LiveJoinIdentity | None = None
    for attempt_number in range(32):
        identity = _identity(token=f'attempt-{attempt_number}')
        channel = _channel('editorial')
        assert buffer.reserve(identity) == JoinTarget(identity)
        assert buffer.prepare(identity, channel) == Attached(identity)
        assert buffer.finalize(identity) == JoinFinalized(identity)
        if first_identity is None:
            first_identity = identity
        current_identity = identity
        if attempt_number < 31:
            buffer.abort(identity)

    assert first_identity is not None
    assert current_identity is not None
    assert len(buffer._join_records) == 1
    assert buffer.finalize(current_identity) == JoinFinalized(current_identity)
    with pytest.raises(JoinProtocolError, match='stale'):
        buffer.finalize(first_identity)
    buffer.abort(first_identity)
    assert 'editorial' in buffer._groups


def test_successful_query_time_controls_holdback_eligibility() -> None:
    buffer = StrictBuffer(max_events=20, max_bytes=10_000_000)
    attach_strict_group(buffer, 'editorial', 1, holdback_secs=60.0, delivery_floor=99)
    buffer._observe(_encoded(100), successful_time=10.0)

    assert _release_ids(buffer, 'editorial', 10.0) == []
    assert _release_ids(buffer, 'editorial', 69.0) == []
    assert _release_ids(buffer, 'editorial', 70.0) == [100]


def test_terminal_capacity_control_survives_offer_and_rebinds_after_loss() -> None:
    first = _encoded(1)
    second = _encoded(2)
    buffer = StrictBuffer(
        max_events=1,
        max_bytes=max(first.byte_count, second.byte_count),
        group_holdbacks={'editorial': 300.0},
        group_delivery_floors={'editorial': 0},
    )
    identity = _identity(boundary=0)
    channel = _channel('editorial')
    assert buffer.reserve(identity) == JoinTarget(identity)
    assert buffer.prepare(identity, channel) == Attached(identity)
    assert buffer.finalize(identity) == JoinFinalized(identity)

    buffer._observe(first, successful_time=0.0)
    buffer._observe(second, successful_time=0.0)
    assert buffer.pump_many(('editorial',), 0.0) == ()
    (old_control,) = channel.drain()
    assert isinstance(old_control, StrictCapacityControl)
    channel.detach('channel-lost-before-control-commit')
    assert buffer.pump_many(('editorial',), 0.0) == ()
    buffer.abort(identity)

    fresh = replace(identity, attempt_token=_FRESH_ATTEMPT_ID)
    fresh_channel = _channel('editorial')
    assert buffer.reserve(fresh) == JoinTarget(fresh)
    assert buffer.prepare(fresh, fresh_channel) == Attached(fresh)
    assert buffer.finalize(fresh) == JoinFinalized(fresh)
    assert buffer.pump_many(('editorial',), 0.0) == ()
    (current_control,) = fresh_channel.drain()
    assert isinstance(current_control, StrictCapacityControl)
    assert current_control.identity == fresh
    assert current_control == replace(old_control, identity=fresh)

    with pytest.raises(JoinProtocolError, match='stale'):
        buffer.acknowledge_control(old_control)
    buffer.acknowledge_control(current_control)
    buffer.acknowledge_control(current_control)
    assert 'editorial' not in buffer._groups
