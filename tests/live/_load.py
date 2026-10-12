from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass
from itertools import pairwise
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

from tests.live._daemon import _LIVE_TIMEOUT_SECS, _LiveReport, _ProbeRecord, _ProducerProgress

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Mapping

    from tests.live._site import LiveSite


@dataclass(frozen=True)
class _GroupDeliveryMetrics:
    _delivered_unique_ids: tuple[int, ...]
    _duplicate_ids: tuple[int, ...]
    _inversions: tuple[tuple[int, int], ...]
    _missing_ids: tuple[int, ...]


@dataclass(frozen=True)
class _LoadPhases:
    _live_count: int
    _stopped_count: int


def _analyze_group_deliveries(
    expected_ids: Iterable[int],
    records: Iterable[_ProbeRecord],
) -> _GroupDeliveryMetrics:
    expected = frozenset(expected_ids)
    delivered = tuple(record.event_id for record in records if record.event_id in expected)
    counts = Counter(delivered)
    return _GroupDeliveryMetrics(
        _delivered_unique_ids=tuple(sorted(counts)),
        _duplicate_ids=tuple(
            sorted(identifier for identifier, count in counts.items() if count > 1)
        ),
        _inversions=tuple(
            (previous, current) for previous, current in pairwise(delivered) if previous > current
        ),
        _missing_ids=tuple(sorted(expected.difference(counts))),
    )


def assert_load_deliveries(
    expected_ids: Collection[int],
    records_by_group: Mapping[str, Iterable[_ProbeRecord]],
) -> Mapping[str, _GroupDeliveryMetrics]:
    if not records_by_group:
        raise AssertionError('load delivery has no probe groups')
    metrics = {
        group: _analyze_group_deliveries(expected_ids, records)
        for group, records in records_by_group.items()
    }
    delivered_sets = {value._delivered_unique_ids for value in metrics.values()}
    if len(delivered_sets) != 1:
        raise AssertionError('probe groups observed different event sets')
    for group, value in metrics.items():
        if value._missing_ids:
            raise AssertionError(f'probe group {group} has missing event IDs')
        if value._duplicate_ids:
            raise AssertionError(f'probe group {group} has duplicate event IDs')
    return MappingProxyType(metrics)


def load_timeout(count: int) -> float:
    if not 2 <= count <= 1_000:
        raise ValueError('live load event count must be between 2 and 1000')
    return _LIVE_TIMEOUT_SECS + float(count)


def partition_load(count: int) -> _LoadPhases:
    if not 2 <= count <= 1_000:
        raise ValueError('live load event count must be between 2 and 1000')
    return _LoadPhases(_live_count=(count + 1) // 2, _stopped_count=count // 2)


def perform_updates(
    site: LiveSite,
    start: int,
    count: int,
    phase: Literal['live', 'stopped'],
    report: _LiveReport,
) -> _ProducerProgress:
    progress = _ProducerProgress(phase=phase, requested_updates=count)
    report.producer_progress[phase] = progress
    report.requested_updates += count
    started = time.monotonic()
    try:
        for index in range(start, start + count):
            value = f'{site._namespace}-load-{index:06d}'
            site._update_description(value)
            progress.phase_values.append(value)
            progress.successful_updates += 1
            report.expected_phases.append(value)
            report.successful_updates += 1
    finally:
        progress.elapsed_secs = time.monotonic() - started
    return progress
