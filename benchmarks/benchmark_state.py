"""Exercise deterministic daemon state workloads without contacting ShotGrid.

Run the supported script as `PYTHONPATH=src python
benchmarks/benchmark_state.py [options]`. Completed workloads write one JSON
summary to standard output. The accepted `--json` option does not change the
output.

The workload measures event encoding and SQLite transaction, checkpoint, and
WAL work. It models group delivery and strict rejoin instead of running daemon
workers.
When `--state-dir PATH` is supplied, `PATH` is the parent for a per-run
temporary database directory. That directory is removed before the command
returns, so the reported `database_path` identifies a former path rather than a
retained artifact.

Exit status is 0 when all acceptance checks pass, 1 when the JSON summary
reports failed acceptance checks, and 2 when argument parsing fails or the
workload raises a caught `ValueError` or `OSError`.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import argparse
import json
import math
import socket
import sqlite3
import sys
import tempfile
import time
from collections import deque
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from itertools import pairwise
from pathlib import Path
from typing import Literal, TypedDict, cast

from sg_event_daemon._config import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_CHECKPOINT_INTERVAL_SECS,
    DEFAULT_MAX_IPC_FRAME_BYTES,
    DEFAULT_QUEUE_MAX_BYTES,
    DEFAULT_QUEUE_MAX_EVENTS,
    DEFAULT_SQLITE_SYNCHRONOUS,
    DEFAULT_STRICT_MAX_BYTES,
    DEFAULT_STRICT_MAX_EVENTS,
)
from sg_event_daemon._events import Event, decode_event, encode_event

_DEFAULT_CALLBACKS_PER_MATCH = 1
_DEFAULT_DURATION_SECS = 60.0
_DEFAULT_GROUPS = 13
_DEFAULT_HOLDBACK_SECS = 1.0
_DEFAULT_MATCH_RATIO = 0.1
_DEFAULT_NUMERIC_GAP_RATE = 0.01
_DEFAULT_PAYLOAD_BYTES = 1_024
_DEFAULT_RATE = 25.0
_IRRELEVANT_BATCH_EVENTS = 500
_STRICT_BASE_SERVICE_SECS = 0.005
_WORKLOAD_BASE_SERVICE_SECS = 0.0001


class _CountBytes(TypedDict):
    """Record a count and byte totals for committed chunks."""

    count: int
    total: int
    maximum: int


class _EventsBytes(TypedDict):
    """Record event and byte occupancy or bounds."""

    events: int
    bytes: int


class _GroupLag(TypedDict):
    """Record maximum and per-group lag."""

    maximum: int
    by_group: list[int]


class _StrictRejoinCommon(TypedDict):
    """Record fields common to converged and nonconverged strict runs."""

    offered_rate: float
    states: list[str]
    epoch_count: int
    epoch_page_sizes: list[int]
    catchup_delivery_rate: float
    delivery_rate_margin: float
    measured_backlog_events: int
    fixed_boundary_event_id: int
    retention_peak_events: int
    retention_peak_bytes: int
    holdback_windows_paid: int
    catchup_transitions: int


class _StrictRejoinResult(_StrictRejoinCommon):
    """Record a strict run including nullable nonconverged measurements."""

    offered_during_catchup: int
    offered_event_ids: list[int]
    delivered_event_ids: list[int]
    final_cursor: int
    final_offered_event_id: int
    delivery_gap_count: int
    decreasing_id_count: int
    lag_events: int
    derived_rejoin_bound_secs: float | None
    measured_rejoin_secs: float | None
    event_time_elapsed_secs: float
    event_time_trace: list[dict[str, object]]
    converged: bool


class _WorkloadSummary(TypedDict):
    """Record measured workload and queue observations."""

    event_count: int
    offered_event_groups: int
    accepted_event_groups: int
    delivered_event_groups: int
    dropped_events: int
    queued_event_groups: int
    delivery_gap_event_groups: int
    offered_rate: float
    group_count: int
    payload_bytes: int
    matching_callbacks: int
    sqlite_transactions: int
    irrelevant_transactions: int
    irrelevant_event_groups: int
    checkpoints: int
    commit_latency_ms: dict[str, float]
    queue_peak: _EventsBytes
    queue_bounds: _EventsBytes
    strict_buffer_peak: _EventsBytes
    strict_buffer_bounds: _EventsBytes
    queue_overflow_events: int
    strict_buffer_overflow_events: int
    catchup_transitions: int
    numeric_gap_count: int
    encoder_latency_ms: dict[str, float]
    decoded_object_memory_amplification: float
    commit_chunk_bytes: _CountBytes
    group_lag: _GroupLag
    group_cursors: list[int]
    wal_bytes: int
    sqlite_mode: str


class _BenchmarkSummary(_WorkloadSummary):
    """Record the complete JSON-compatible benchmark result."""

    database_path: str
    elapsed_secs: float
    elapsed_throughput_events_per_sec: float
    state_dir: str
    strict_rejoin: _StrictRejoinResult
    socket_buffers: dict[str, object]
    failures: list[str]
    config: dict[str, object]


@dataclass(frozen=True)
class _BenchmarkConfig:
    """Define one deterministic workload and its storage location."""

    rate: float = _DEFAULT_RATE
    duration_secs: float = _DEFAULT_DURATION_SECS
    groups: int = _DEFAULT_GROUPS
    match_ratio: float = _DEFAULT_MATCH_RATIO
    callbacks_per_match: int = _DEFAULT_CALLBACKS_PER_MATCH
    payload_bytes: int = _DEFAULT_PAYLOAD_BYTES
    numeric_gap_rate: float = _DEFAULT_NUMERIC_GAP_RATE
    ordering: Literal['eager', 'strict'] = 'eager'
    holdback_secs: float = _DEFAULT_HOLDBACK_SECS
    sqlite_synchronous: Literal['normal', 'full'] = DEFAULT_SQLITE_SYNCHRONOUS
    simulated_callback_latency_ms: float = 0.0
    simulated_storage_latency_ms: float = 0.0
    state_dir: Path | None = None


@dataclass(frozen=True)
class _CommitRequest:
    """Describe one benchmark cursor transaction."""

    _group_id: int
    _cursor: int
    _callback_count: int
    _chunk_bytes: int


@dataclass(frozen=True)
class _Delivery:
    """Describe one source event offered to one modeled group."""

    _event_id: int
    _frame: bytes
    _matching: bool
    _offered_at: float
    _ordinal: int
    _release_at: float


@dataclass
class _ProgressState:
    """Track deterministic event and irrelevant-range progress."""

    _gap_accumulator: float
    _irrelevant_bytes: list[int]
    _irrelevant_counts: list[int]
    _irrelevant_high: list[int | None]
    _last_event_id: int
    _match_accumulator: float
    _next_checkpoint_at: float
    _offered_event_ids: list[int]


@dataclass(frozen=True)
class _ScheduledDelivery:
    """Bind one accepted delivery to its modeled completion time."""

    _completes_at: float
    _delivery: _Delivery


@dataclass
class _GroupSimulation:
    """Track one group's independent buffer, queue, and delivery state."""

    _cursor: int
    _delivered_event_ids: list[int]
    _detached: bool
    _group_id: int
    _next_available_at: float
    _queue: deque[_ScheduledDelivery]
    _queue_bytes: int
    _strict_buffer: deque[_Delivery]
    _strict_buffer_bytes: int


@dataclass(frozen=True)
class _SimulationFaults:
    """Inject deterministic faults and reduced bounds into benchmark models."""

    _block_strict_rejoin: bool = False
    _drop_event_ordinal: int | None = None
    _queue_max_bytes: int = DEFAULT_QUEUE_MAX_BYTES
    _queue_max_events: int = DEFAULT_QUEUE_MAX_EVENTS
    _stall_group: int | None = None
    _strict_max_bytes: int = DEFAULT_STRICT_MAX_BYTES
    _strict_max_events: int = DEFAULT_STRICT_MAX_EVENTS


@dataclass
class _StrictSimulation:
    """Track strict source offers, retention, delivery, and event time."""

    _current_time: float
    _delivered_event_ids: list[int]
    _next_event_id: int
    _next_offer_at: float
    _offered_event_ids: list[int]
    _retained: deque[int]
    _retention_peak_events: int
    _trace: list[dict[str, object]]


@dataclass(frozen=True)
class _SummaryState:
    """Carry final cursor facts into summary construction."""

    _cursors: list[int]
    _delivered_event_ids: list[list[int]]
    _event_count: int
    _first_frame: bytes
    _groups: list[_GroupSimulation]
    _offered_event_ids: list[int]


@dataclass
class _WorkloadMeasurements:
    """Accumulate measured state-workload observations."""

    _accepted_event_groups: int
    _catchup_transitions: int
    _checkpoint_count: int
    _commit_chunk_bytes: list[int]
    _commit_latencies_ms: list[float]
    _delivered_event_groups: int
    _dropped_events: int
    _encoder_latencies_ms: list[float]
    _irrelevant_event_groups: int
    _irrelevant_transactions: int
    _matching_callbacks: int
    _matching_event_groups: int
    _numeric_gap_count: int
    _offered_event_groups: int
    _payload_bytes: int
    _queue_overflow_events: int
    _queue_peak_bytes: int
    _queue_peak_events: int
    _strict_buffer_overflow_events: int
    _strict_buffer_peak_bytes: int
    _strict_buffer_peak_events: int
    _wal_bytes: int


@dataclass(frozen=True)
class _WorkloadContext:
    """Bind workload configuration, storage, and measurements."""

    _config: _BenchmarkConfig
    _connection: sqlite3.Connection
    _database_path: Path
    _faults: _SimulationFaults
    _measurements: _WorkloadMeasurements
    _progress: _ProgressState


def _acceptance_failures(
    config: _BenchmarkConfig,
    summary: _WorkloadSummary,
    strict: _StrictRejoinResult,
    strict_failures: Sequence[str],
) -> list[str]:
    """Evaluate measured workload and strict-rejoin acceptance criteria."""
    queue_peak = summary['queue_peak']
    queue_bounds = summary['queue_bounds']
    strict_peak = summary['strict_buffer_peak']
    strict_bounds = summary['strict_buffer_bounds']
    lag = summary['group_lag']
    failures: list[str] = []
    dropped_events = summary['dropped_events']
    if dropped_events:
        noun = 'delivery' if dropped_events == 1 else 'deliveries'
        failures.append(f'workload dropped {dropped_events} event {noun}')
    offered_groups = summary['offered_event_groups']
    accepted_groups = summary['accepted_event_groups']
    delivered_groups = summary['delivered_event_groups']
    queued_groups = summary['queued_event_groups']
    if accepted_groups + dropped_events != offered_groups:
        failures.append('workload offer accounting is inconsistent')
    if delivered_groups + queued_groups != accepted_groups:
        failures.append('workload delivery accounting is inconsistent')
    if (
        queue_peak['events'] > queue_bounds['events']
        or queue_peak['bytes'] > queue_bounds['bytes']
    ):
        failures.append('workload exceeded configured queue bounds')
    if (
        strict_peak['events'] > strict_bounds['events']
        or strict_peak['bytes'] > strict_bounds['bytes']
    ):
        failures.append('workload exceeded configured strict-buffer bounds')
    if lag['maximum'] != 0:
        failures.append('workload left group lag after drain')
    irrelevant_event_groups = summary['irrelevant_event_groups']
    irrelevant_transactions = summary['irrelevant_transactions']
    if irrelevant_event_groups > 1 and irrelevant_transactions >= irrelevant_event_groups:
        failures.append('workload committed one cursor transaction per irrelevant event')
    failures.extend(strict_failures)
    if (
        config.simulated_callback_latency_ms == 0
        and strict['catchup_delivery_rate'] <= strict['offered_rate']
        and 'strict catch-up delivery-rate margin is not positive' not in failures
    ):
        failures.append('strict catch-up is slower than the offered rate')
    return failures


def _argument_parser() -> argparse.ArgumentParser:
    """Build the benchmark command-line parser."""
    parser = argparse.ArgumentParser(
        description='Run the deterministic sg-event-daemon state workload benchmark.',
    )
    parser.add_argument('--rate', type=float, default=_DEFAULT_RATE)
    parser.add_argument('--duration-secs', type=float, default=_DEFAULT_DURATION_SECS)
    parser.add_argument('--groups', type=int, default=_DEFAULT_GROUPS)
    parser.add_argument('--match-ratio', type=float, default=_DEFAULT_MATCH_RATIO)
    parser.add_argument(
        '--callbacks-per-match',
        type=int,
        default=_DEFAULT_CALLBACKS_PER_MATCH,
    )
    parser.add_argument('--payload-bytes', type=int, default=_DEFAULT_PAYLOAD_BYTES)
    parser.add_argument('--numeric-gap-rate', type=float, default=_DEFAULT_NUMERIC_GAP_RATE)
    parser.add_argument('--ordering', choices=('eager', 'strict'), default='eager')
    parser.add_argument('--holdback-secs', type=float, default=_DEFAULT_HOLDBACK_SECS)
    parser.add_argument(
        '--sqlite-sync-mode',
        '--sqlite-synchronous',
        dest='sqlite_synchronous',
        choices=('normal', 'full'),
        default=DEFAULT_SQLITE_SYNCHRONOUS,
    )
    parser.add_argument('--simulated-callback-latency-ms', type=float, default=0.0)
    parser.add_argument('--simulated-storage-latency-ms', type=float, default=0.0)
    parser.add_argument('--state-dir', type=Path)
    parser.add_argument('--json', action='store_true', dest='json_output')
    return parser


def _checkpoint(connection: sqlite3.Connection, measurements: _WorkloadMeasurements) -> None:
    """Run and count one passive WAL checkpoint."""
    connection.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
    measurements._checkpoint_count += 1


def _commit(
    connection: sqlite3.Connection,
    database_path: Path,
    measurements: _WorkloadMeasurements,
    request: _CommitRequest,
    storage_latency_ms: float,
) -> None:
    """Measure one cursor transaction and its current WAL footprint."""
    started = time.perf_counter()
    connection.execute('BEGIN IMMEDIATE')
    connection.execute(
        """
        UPDATE group_progress
        SET cursor = ?, callback_count = callback_count + ?
        WHERE group_id = ?
        """,
        (request._cursor, request._callback_count, request._group_id),
    )
    connection.commit()
    elapsed_ms = (time.perf_counter() - started) * 1_000 + storage_latency_ms
    measurements._commit_latencies_ms.append(elapsed_ms)
    measurements._commit_chunk_bytes.append(request._chunk_bytes)
    wal_path = database_path.with_name(f'{database_path.name}-wal')
    with suppress(FileNotFoundError):
        measurements._wal_bytes = max(measurements._wal_bytes, wal_path.stat().st_size)


def _deep_size(value: object, seen: set[int] | None = None) -> int:
    """Return recursive resident size without double-counting identities."""
    identities: set[int] = set() if seen is None else seen
    identity = id(value)
    if identity in identities:
        return 0
    identities.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, Mapping):
        mapping = cast('Mapping[object, object]', value)
        size += sum(
            _deep_size(key, identities) + _deep_size(item, identities)
            for key, item in mapping.items()
        )
    elif isinstance(value, tuple):
        size += sum(_deep_size(item, identities) for item in cast('tuple[object, ...]', value))
    elif isinstance(value, list):
        size += sum(_deep_size(item, identities) for item in cast('list[object]', value))
    elif isinstance(value, set | frozenset):
        size += sum(
            _deep_size(item, identities) for item in cast('set[object] | frozenset[object]', value)
        )
    return size


def _deliver_scheduled(
    context: _WorkloadContext,
    group: _GroupSimulation,
    delivery: _Delivery,
) -> None:
    """Apply one completed delivery to modeled and SQLite group state."""
    measurements = context._measurements
    progress = context._progress
    measurements._delivered_event_groups += 1
    group._cursor = delivery._event_id
    group._delivered_event_ids.append(delivery._event_id)
    if delivery._matching:
        _commit(
            context._connection,
            context._database_path,
            measurements,
            _CommitRequest(
                _group_id=group._group_id,
                _cursor=delivery._event_id,
                _callback_count=context._config.callbacks_per_match,
                _chunk_bytes=len(delivery._frame)
                * (progress._irrelevant_counts[group._group_id] + 1),
            ),
            context._config.simulated_storage_latency_ms,
        )
        measurements._matching_event_groups += 1
        measurements._matching_callbacks += context._config.callbacks_per_match
        progress._irrelevant_bytes[group._group_id] = 0
        progress._irrelevant_counts[group._group_id] = 0
        progress._irrelevant_high[group._group_id] = None
        return

    measurements._irrelevant_event_groups += 1
    progress._irrelevant_bytes[group._group_id] += len(delivery._frame)
    progress._irrelevant_counts[group._group_id] += 1
    progress._irrelevant_high[group._group_id] = delivery._event_id
    if progress._irrelevant_counts[group._group_id] < _IRRELEVANT_BATCH_EVENTS:
        return
    _commit(
        context._connection,
        context._database_path,
        measurements,
        _CommitRequest(
            _group_id=group._group_id,
            _cursor=delivery._event_id,
            _callback_count=0,
            _chunk_bytes=len(delivery._frame) * progress._irrelevant_counts[group._group_id],
        ),
        context._config.simulated_storage_latency_ms,
    )
    measurements._irrelevant_transactions += 1
    progress._irrelevant_bytes[group._group_id] = 0
    progress._irrelevant_counts[group._group_id] = 0
    progress._irrelevant_high[group._group_id] = None


def _delivery_service_secs(
    config: _BenchmarkConfig,
    delivery: _Delivery,
) -> float:
    """Return modeled service time for one accepted group delivery."""
    callback_secs = (
        config.callbacks_per_match * config.simulated_callback_latency_ms / 1_000
        if delivery._matching
        else 0.0
    )
    storage_secs = config.simulated_storage_latency_ms / 1_000 if delivery._matching else 0.0
    return _WORKLOAD_BASE_SERVICE_SECS + callback_secs + storage_secs


def _event(event_id: int, payload_bytes: int) -> Event:
    """Build one deterministic source-visible event."""
    return Event(
        {
            'attribute_name': 'sg_status_list',
            'created_at': '2026-07-29T12:00:00Z',
            'entity': {'id': 7, 'type': 'Shot'},
            'event_type': 'Shotgun_Shot_Change',
            'id': event_id,
            'meta': {'benchmark_payload': 'x' * payload_bytes},
            'project': None,
            'session_uuid': None,
            'type': 'EventLogEntry',
            'user': None,
        }
    )


def _flush_irrelevant(
    context: _WorkloadContext,
) -> None:
    """Commit every final batched irrelevant range."""
    progress = context._progress
    for group_id, high_id in enumerate(progress._irrelevant_high):
        if high_id is None:
            continue
        _commit(
            context._connection,
            context._database_path,
            context._measurements,
            _CommitRequest(
                _group_id=group_id,
                _cursor=high_id,
                _callback_count=0,
                _chunk_bytes=progress._irrelevant_bytes[group_id],
            ),
            context._config.simulated_storage_latency_ms,
        )
        context._measurements._irrelevant_transactions += 1


def _initialize_database(
    config: _BenchmarkConfig,
    database_path: Path,
) -> sqlite3.Connection:
    """Create one WAL database containing modeled cursor state."""
    connection = sqlite3.connect(database_path)
    connection.execute('PRAGMA journal_mode=WAL').fetchone()
    connection.execute(f'PRAGMA synchronous={config.sqlite_synchronous.upper()}')
    connection.execute(
        """
        CREATE TABLE group_progress (
            group_id INTEGER PRIMARY KEY,
            cursor INTEGER NOT NULL,
            callback_count INTEGER NOT NULL
        )
        """
    )
    connection.executemany(
        'INSERT INTO group_progress(group_id, cursor, callback_count) VALUES (?, 0, 0)',
        ((group_id,) for group_id in range(config.groups)),
    )
    connection.commit()
    return connection


def _measurements() -> _WorkloadMeasurements:
    """Create one empty workload measurement set."""
    return _WorkloadMeasurements(
        _accepted_event_groups=0,
        _catchup_transitions=0,
        _checkpoint_count=0,
        _commit_chunk_bytes=[],
        _commit_latencies_ms=[],
        _delivered_event_groups=0,
        _dropped_events=0,
        _encoder_latencies_ms=[],
        _irrelevant_event_groups=0,
        _irrelevant_transactions=0,
        _matching_callbacks=0,
        _matching_event_groups=0,
        _numeric_gap_count=0,
        _offered_event_groups=0,
        _payload_bytes=0,
        _queue_overflow_events=0,
        _queue_peak_bytes=0,
        _queue_peak_events=0,
        _strict_buffer_overflow_events=0,
        _strict_buffer_peak_bytes=0,
        _strict_buffer_peak_events=0,
        _wal_bytes=0,
    )


def _next_frame(
    config: _BenchmarkConfig,
    measurements: _WorkloadMeasurements,
    progress: _ProgressState,
) -> tuple[bytes, bool]:
    """Encode one deterministic event and decide whether it matches."""
    progress._last_event_id += 1
    progress._gap_accumulator += config.numeric_gap_rate
    if progress._gap_accumulator >= 1.0:
        extra = math.floor(progress._gap_accumulator)
        progress._gap_accumulator -= extra
        progress._last_event_id += extra
        measurements._numeric_gap_count += extra
    encode_started = time.perf_counter()
    frame = encode_event(_event(progress._last_event_id, config.payload_bytes))
    measurements._encoder_latencies_ms.append((time.perf_counter() - encode_started) * 1_000)
    measurements._payload_bytes += len(frame)
    progress._match_accumulator += config.match_ratio
    matching = progress._match_accumulator >= 1.0
    if matching:
        progress._match_accumulator -= math.floor(progress._match_accumulator)
    return frame, matching


def _percentiles(values: Sequence[float]) -> dict[str, float]:
    """Return nearest-rank p50, p95, and p99 values."""
    if not values:
        return {'p50': 0.0, 'p95': 0.0, 'p99': 0.0}
    ordered = sorted(values)

    def _percentile(fraction: float) -> float:
        index = max(0, math.ceil(len(ordered) * fraction) - 1)
        return ordered[index]

    return {
        'p50': _percentile(0.50),
        'p95': _percentile(0.95),
        'p99': _percentile(0.99),
    }


def _record_queue_state(
    context: _WorkloadContext,
    group: _GroupSimulation,
) -> None:
    """Record actual queue peaks and capacity crossings."""
    measurements = context._measurements
    events = len(group._queue)
    measurements._queue_peak_events = max(measurements._queue_peak_events, events)
    measurements._queue_peak_bytes = max(measurements._queue_peak_bytes, group._queue_bytes)
    if (
        events <= context._faults._queue_max_events
        and group._queue_bytes <= context._faults._queue_max_bytes
    ):
        return
    measurements._queue_overflow_events += 1
    if not group._detached:
        group._detached = True
        measurements._catchup_transitions += 1


def _schedule_delivery(
    context: _WorkloadContext,
    group: _GroupSimulation,
    delivery: _Delivery,
    ready_at: float,
) -> None:
    """Schedule one accepted delivery on a group's serial service lane."""
    starts_at = max(group._next_available_at, ready_at)
    completes_at = starts_at + _delivery_service_secs(context._config, delivery)
    group._next_available_at = completes_at
    group._queue.append(_ScheduledDelivery(completes_at, delivery))
    group._queue_bytes += len(delivery._frame)
    _record_queue_state(context, group)


def _offer_group(
    context: _WorkloadContext,
    group: _GroupSimulation,
    delivery: _Delivery,
) -> None:
    """Offer one source event to one group with independent accounting."""
    measurements = context._measurements
    measurements._offered_event_groups += 1
    if context._faults._drop_event_ordinal == delivery._ordinal and group._group_id == 0:
        measurements._dropped_events += 1
        return
    measurements._accepted_event_groups += 1
    if context._config.ordering == 'strict':
        group._strict_buffer.append(delivery)
        group._strict_buffer_bytes += len(delivery._frame)
        events = len(group._strict_buffer)
        measurements._strict_buffer_peak_events = max(
            measurements._strict_buffer_peak_events,
            events,
        )
        measurements._strict_buffer_peak_bytes = max(
            measurements._strict_buffer_peak_bytes,
            group._strict_buffer_bytes,
        )
        if (
            events > context._faults._strict_max_events
            or group._strict_buffer_bytes > context._faults._strict_max_bytes
        ):
            measurements._strict_buffer_overflow_events += 1
            if not group._detached:
                group._detached = True
                measurements._catchup_transitions += 1
        return
    _schedule_delivery(context, group, delivery, delivery._offered_at)


def _release_strict_buffer(
    context: _WorkloadContext,
    group: _GroupSimulation,
    until: float,
) -> None:
    """Move every holdback-complete strict event into the lane queue."""
    while group._strict_buffer and group._strict_buffer[0]._release_at <= until + 1e-12:
        delivery = group._strict_buffer.popleft()
        group._strict_buffer_bytes -= len(delivery._frame)
        _schedule_delivery(context, group, delivery, delivery._release_at)


def _advance_group(
    context: _WorkloadContext,
    group: _GroupSimulation,
    until: float,
) -> None:
    """Advance one group through releases and completed serial deliveries."""
    _release_strict_buffer(context, group, until)
    if context._faults._stall_group == group._group_id:
        return
    while group._queue and group._queue[0]._completes_at <= until + 1e-12:
        scheduled = group._queue.popleft()
        group._queue_bytes -= len(scheduled._delivery._frame)
        _deliver_scheduled(context, group, scheduled._delivery)


def _run_events(
    context: _WorkloadContext,
    groups: list[_GroupSimulation],
    event_count: int,
) -> bytes:
    """Encode and apply every modeled source event."""
    progress = context._progress
    first_frame: bytes | None = None
    for index in range(event_count):
        modeled_time = (index + 1) / context._config.rate
        for group in groups:
            _advance_group(context, group, modeled_time)
        frame, matching = _next_frame(context._config, context._measurements, progress)
        if first_frame is None:
            first_frame = frame
        progress._offered_event_ids.append(progress._last_event_id)
        delivery = _Delivery(
            _event_id=progress._last_event_id,
            _frame=frame,
            _matching=matching,
            _offered_at=modeled_time,
            _ordinal=index + 1,
            _release_at=modeled_time + context._config.holdback_secs,
        )
        for group in groups:
            _offer_group(context, group, delivery)
        while modeled_time >= progress._next_checkpoint_at:
            _checkpoint(context._connection, context._measurements)
            progress._next_checkpoint_at += DEFAULT_CHECKPOINT_INTERVAL_SECS
    if first_frame is None:
        raise RuntimeError('benchmark produced no events')
    drain_deadline = event_count / context._config.rate + context._config.holdback_secs + 1.0
    for group in groups:
        _advance_group(context, group, drain_deadline)
    return first_frame


def _socket_buffers() -> dict[str, object]:
    """Record effective socket buffers without imposing a portable minimum."""
    sender, receiver = socket.socketpair()
    try:
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, DEFAULT_MAX_IPC_FRAME_BYTES)
        receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, DEFAULT_MAX_IPC_FRAME_BYTES)
        return {
            'requested_send_bytes': DEFAULT_MAX_IPC_FRAME_BYTES,
            'sender_send_bytes': sender.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF),
            'sender_receive_bytes': sender.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
            'receiver_send_bytes': receiver.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF),
            'receiver_receive_bytes': receiver.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
            'minimum_asserted': False,
        }
    except OSError as error:
        return {
            'requested_send_bytes': DEFAULT_MAX_IPC_FRAME_BYTES,
            'error': f'{type(error).__name__}: {error}',
            'minimum_asserted': False,
        }
    finally:
        sender.close()
        receiver.close()


def _strict_initial_state(
    backlog_events: int,
    offered_rate: float,
) -> _StrictSimulation:
    """Create one detached strict simulation after backlog accumulation."""
    offered_event_ids = list(range(1, backlog_events + 1))
    current_time = backlog_events / offered_rate
    return _StrictSimulation(
        _current_time=current_time,
        _delivered_event_ids=[],
        _next_event_id=backlog_events + 1,
        _next_offer_at=(backlog_events + 1) / offered_rate,
        _offered_event_ids=offered_event_ids,
        _retained=deque(offered_event_ids),
        _retention_peak_events=backlog_events,
        _trace=[
            {
                'state': 'detached',
                'at_secs': 0.0,
                'cursor': 0,
                'offered': 0,
                'retained': 0,
            },
            {
                'state': 'catching-up',
                'at_secs': current_time,
                'cursor': 0,
                'offered': backlog_events,
                'retained': backlog_events,
            },
        ],
    )


def _strict_offer_until(
    simulation: _StrictSimulation,
    target_time: float,
    offered_rate: float,
) -> None:
    """Offer every strict source event due by one simulated time."""
    while simulation._next_offer_at <= target_time + 1e-12:
        simulation._offered_event_ids.append(simulation._next_event_id)
        simulation._retained.append(simulation._next_event_id)
        simulation._retention_peak_events = max(
            simulation._retention_peak_events,
            len(simulation._retained),
        )
        simulation._next_event_id += 1
        simulation._next_offer_at = simulation._next_event_id / offered_rate


def _strict_order_counts(delivered_event_ids: Sequence[int]) -> tuple[int, int]:
    """Count gaps and non-increasing transitions in delivered event ids."""
    pairs = tuple(pairwise(delivered_event_ids))
    return (
        sum(current != previous + 1 for previous, current in pairs),
        sum(current <= previous for previous, current in pairs),
    )


def _strict_process_boundary(
    simulation: _StrictSimulation,
    boundary_events: int,
    offered_rate: float,
    service_secs: float,
    storage_secs: float,
) -> tuple[list[int], float]:
    """Deliver one fixed boundary through bounded paginated epochs."""
    started = simulation._current_time
    page_sizes: list[int] = []
    boundary_ids = simulation._offered_event_ids[:boundary_events]
    for page_start in range(0, boundary_events, DEFAULT_BATCH_SIZE):
        page = boundary_ids[page_start : page_start + DEFAULT_BATCH_SIZE]
        page_sizes.append(len(page))
        simulation._current_time += storage_secs
        _strict_offer_until(simulation, simulation._current_time, offered_rate)
        for event_id in page:
            if not simulation._retained or simulation._retained[0] != event_id:
                raise RuntimeError('strict benchmark retention order is inconsistent')
            simulation._retained.popleft()
            simulation._current_time += service_secs
            _strict_offer_until(simulation, simulation._current_time, offered_rate)
            simulation._delivered_event_ids.append(event_id)
    return page_sizes, max(simulation._current_time - started, 1e-12)


def _strict_record_state(
    simulation: _StrictSimulation,
    state: str,
) -> None:
    """Record one observed strict state transition or terminal observation."""
    simulation._trace.append(
        {
            'state': state,
            'at_secs': simulation._current_time,
            'cursor': (
                simulation._delivered_event_ids[-1] if simulation._delivered_event_ids else 0
            ),
            'offered': len(simulation._offered_event_ids),
            'retained': len(simulation._retained),
        }
    )


def _strict_drain_rejoin(
    simulation: _StrictSimulation,
    offered_rate: float,
    service_secs: float,
    derived_bound_secs: float,
    *,
    blocked: bool,
) -> None:
    """Drain retained events or advance a deliberately blocked rejoin."""
    if blocked:
        simulation._current_time = max(
            simulation._current_time,
            derived_bound_secs + max(service_secs, 1 / offered_rate),
        )
        _strict_offer_until(simulation, simulation._current_time, offered_rate)
        _strict_record_state(simulation, 'fixed-boundary-rejoin')
        return
    while simulation._retained:
        event_id = simulation._retained.popleft()
        simulation._current_time += service_secs
        _strict_offer_until(simulation, simulation._current_time, offered_rate)
        simulation._delivered_event_ids.append(event_id)
    _strict_record_state(simulation, 'running')


def _strict_rejoin(
    config: _BenchmarkConfig,
    serialized_event_bytes: int,
    faults: _SimulationFaults,
) -> tuple[_StrictRejoinResult, list[str]]:
    """Execute one strict detach, catch-up epoch, and fixed-boundary rejoin."""
    offered_rate = _DEFAULT_RATE
    holdback_window = max(config.holdback_secs, 1 / offered_rate)
    backlog_events = max(1, math.floor(offered_rate * holdback_window + 1e-9))
    service_secs = _STRICT_BASE_SERVICE_SECS + (
        config.callbacks_per_match * config.simulated_callback_latency_ms / 1_000
    )
    storage_secs = config.simulated_storage_latency_ms / 1_000
    simulation = _strict_initial_state(backlog_events, offered_rate)
    fixed_boundary = simulation._offered_event_ids[-1]
    states = ['detached', 'catching-up']
    epoch_page_sizes, catchup_busy_secs = _strict_process_boundary(
        simulation,
        backlog_events,
        offered_rate,
        service_secs,
        storage_secs,
    )
    delivery_rate = backlog_events / catchup_busy_secs
    margin = delivery_rate - offered_rate
    failures: list[str] = []
    common: _StrictRejoinCommon = {
        'offered_rate': offered_rate,
        'states': states,
        'epoch_count': len(epoch_page_sizes),
        'epoch_page_sizes': epoch_page_sizes,
        'catchup_delivery_rate': delivery_rate,
        'delivery_rate_margin': margin,
        'measured_backlog_events': backlog_events,
        'fixed_boundary_event_id': fixed_boundary,
        'retention_peak_events': simulation._retention_peak_events,
        'retention_peak_bytes': simulation._retention_peak_events * serialized_event_bytes,
        'holdback_windows_paid': 1 if config.holdback_secs > 0 else 0,
        'catchup_transitions': 1,
    }
    if margin <= 0:
        failures.append('strict catch-up delivery-rate margin is not positive')
        lag_events = len(simulation._offered_event_ids) - len(simulation._delivered_event_ids)
        result: _StrictRejoinResult = {
            **common,
            'offered_during_catchup': len(simulation._offered_event_ids) - backlog_events,
            'offered_event_ids': simulation._offered_event_ids,
            'delivered_event_ids': simulation._delivered_event_ids,
            'final_cursor': (
                simulation._delivered_event_ids[-1] if simulation._delivered_event_ids else 0
            ),
            'final_offered_event_id': simulation._offered_event_ids[-1],
            'delivery_gap_count': 0,
            'decreasing_id_count': 0,
            'lag_events': lag_events,
            'derived_rejoin_bound_secs': None,
            'measured_rejoin_secs': None,
            'event_time_elapsed_secs': simulation._current_time,
            'event_time_trace': simulation._trace,
            'converged': False,
        }
        return result, failures

    derived_bound_secs = 2 * backlog_events / margin + config.holdback_secs
    states.append('fixed-boundary-rejoin')
    _strict_record_state(simulation, 'fixed-boundary-rejoin')
    _strict_drain_rejoin(
        simulation,
        offered_rate,
        service_secs,
        derived_bound_secs,
        blocked=faults._block_strict_rejoin,
    )
    if not faults._block_strict_rejoin:
        states.append('running')
    gap_count, decreasing_count = _strict_order_counts(simulation._delivered_event_ids)
    lag_events = len(simulation._offered_event_ids) - len(simulation._delivered_event_ids)
    measured_rejoin_secs = simulation._current_time
    converged = (
        not faults._block_strict_rejoin
        and simulation._delivered_event_ids == simulation._offered_event_ids
        and gap_count == 0
        and decreasing_count == 0
        and lag_events == 0
        and measured_rejoin_secs <= derived_bound_secs
    )
    result: _StrictRejoinResult = {
        **common,
        'states': states,
        'offered_during_catchup': len(simulation._offered_event_ids) - backlog_events,
        'offered_event_ids': simulation._offered_event_ids,
        'delivered_event_ids': simulation._delivered_event_ids,
        'final_cursor': (
            simulation._delivered_event_ids[-1] if simulation._delivered_event_ids else 0
        ),
        'final_offered_event_id': simulation._offered_event_ids[-1],
        'delivery_gap_count': gap_count,
        'decreasing_id_count': decreasing_count,
        'lag_events': lag_events,
        'derived_rejoin_bound_secs': derived_bound_secs,
        'measured_rejoin_secs': measured_rejoin_secs,
        'event_time_elapsed_secs': simulation._current_time,
        'event_time_trace': simulation._trace,
        'converged': converged,
    }
    if not converged:
        failures.append('strict group did not converge within its measured bound')
    return result, failures


def _summary(
    config: _BenchmarkConfig,
    faults: _SimulationFaults,
    measurements: _WorkloadMeasurements,
    state: _SummaryState,
) -> tuple[_WorkloadSummary, int]:
    """Build the complete workload summary."""
    serialized_event_bytes = max(len(state._first_frame), config.payload_bytes)
    lag_by_group = [
        state._event_count - len(delivered) for delivered in state._delivered_event_ids
    ]
    gap_by_group = [
        len(set(state._offered_event_ids).difference(delivered))
        for delivered in state._delivered_event_ids
    ]
    queued_event_groups = sum(
        len(group._queue) + len(group._strict_buffer) for group in state._groups
    )
    decoded_amplification = _deep_size(decode_event(state._first_frame)) / len(state._first_frame)
    summary: _WorkloadSummary = {
        'event_count': state._event_count,
        'offered_event_groups': measurements._offered_event_groups,
        'accepted_event_groups': measurements._accepted_event_groups,
        'delivered_event_groups': measurements._delivered_event_groups,
        'dropped_events': measurements._dropped_events,
        'queued_event_groups': queued_event_groups,
        'delivery_gap_event_groups': sum(gap_by_group),
        'offered_rate': config.rate,
        'group_count': config.groups,
        'payload_bytes': measurements._payload_bytes,
        'matching_callbacks': measurements._matching_callbacks,
        'sqlite_transactions': len(measurements._commit_latencies_ms),
        'irrelevant_transactions': measurements._irrelevant_transactions,
        'irrelevant_event_groups': measurements._irrelevant_event_groups,
        'checkpoints': measurements._checkpoint_count,
        'commit_latency_ms': _percentiles(measurements._commit_latencies_ms),
        'queue_peak': {
            'events': measurements._queue_peak_events,
            'bytes': measurements._queue_peak_bytes,
        },
        'queue_bounds': {
            'events': faults._queue_max_events,
            'bytes': faults._queue_max_bytes,
        },
        'strict_buffer_peak': {
            'events': measurements._strict_buffer_peak_events,
            'bytes': measurements._strict_buffer_peak_bytes,
        },
        'strict_buffer_bounds': {
            'events': faults._strict_max_events,
            'bytes': faults._strict_max_bytes,
        },
        'queue_overflow_events': measurements._queue_overflow_events,
        'strict_buffer_overflow_events': measurements._strict_buffer_overflow_events,
        'catchup_transitions': measurements._catchup_transitions,
        'numeric_gap_count': measurements._numeric_gap_count,
        'encoder_latency_ms': _percentiles(measurements._encoder_latencies_ms),
        'decoded_object_memory_amplification': decoded_amplification,
        'commit_chunk_bytes': {
            'count': len(measurements._commit_chunk_bytes),
            'total': sum(measurements._commit_chunk_bytes),
            'maximum': max(measurements._commit_chunk_bytes, default=0),
        },
        'group_lag': {'maximum': max(lag_by_group, default=0), 'by_group': lag_by_group},
        'group_cursors': state._cursors,
        'wal_bytes': measurements._wal_bytes,
        'sqlite_mode': f'WAL/{config.sqlite_synchronous.upper()}',
    }
    return summary, serialized_event_bytes


def _validate(config: _BenchmarkConfig) -> None:
    """Reject non-finite, negative, or empty benchmark dimensions."""
    finite_values = {
        'duration_secs': config.duration_secs,
        'holdback_secs': config.holdback_secs,
        'match_ratio': config.match_ratio,
        'numeric_gap_rate': config.numeric_gap_rate,
        'rate': config.rate,
        'simulated_callback_latency_ms': config.simulated_callback_latency_ms,
        'simulated_storage_latency_ms': config.simulated_storage_latency_ms,
    }
    for name, value in finite_values.items():
        if not math.isfinite(value):
            raise ValueError(f'{name} must be finite')
    if config.rate <= 0 or config.duration_secs <= 0:
        raise ValueError('rate and duration_secs must be positive')
    if config.groups <= 0 or config.callbacks_per_match <= 0:
        raise ValueError('groups and callbacks_per_match must be positive')
    if config.payload_bytes < 0:
        raise ValueError('payload_bytes must not be negative')
    if not 0 <= config.match_ratio <= 1 or not 0 <= config.numeric_gap_rate <= 1:
        raise ValueError('match_ratio and numeric_gap_rate must be between zero and one')
    if (
        config.holdback_secs < 0
        or config.simulated_callback_latency_ms < 0
        or config.simulated_storage_latency_ms < 0
    ):
        raise ValueError('holdback and simulated latencies must not be negative')


def _workload(
    config: _BenchmarkConfig,
    database_path: Path,
    faults: _SimulationFaults,
) -> tuple[_WorkloadSummary, int]:
    """Run the deterministic SQLite workload and return its summary."""
    event_count = max(1, round(config.rate * config.duration_secs))
    measurements = _measurements()
    connection = _initialize_database(config, database_path)
    progress = _ProgressState(
        _gap_accumulator=0.0,
        _irrelevant_bytes=[0] * config.groups,
        _irrelevant_counts=[0] * config.groups,
        _irrelevant_high=[None] * config.groups,
        _last_event_id=0,
        _match_accumulator=0.0,
        _next_checkpoint_at=DEFAULT_CHECKPOINT_INTERVAL_SECS,
        _offered_event_ids=[],
    )
    groups = [
        _GroupSimulation(
            _cursor=0,
            _delivered_event_ids=[],
            _detached=False,
            _group_id=group_id,
            _next_available_at=0.0,
            _queue=deque(),
            _queue_bytes=0,
            _strict_buffer=deque(),
            _strict_buffer_bytes=0,
        )
        for group_id in range(config.groups)
    ]
    context = _WorkloadContext(
        config,
        connection,
        database_path,
        faults,
        measurements,
        progress,
    )
    try:
        first_frame = _run_events(context, groups, event_count)
        _flush_irrelevant(context)
        if measurements._checkpoint_count == 0:
            _checkpoint(connection, measurements)
        cursors = [
            cast('int', row[0])
            for row in connection.execute(
                'SELECT cursor FROM group_progress ORDER BY group_id'
            ).fetchall()
        ]
    finally:
        connection.close()
    return _summary(
        config,
        faults,
        measurements,
        _SummaryState(
            _cursors=cursors,
            _delivered_event_ids=[group._delivered_event_ids for group in groups],
            _event_count=event_count,
            _first_frame=first_frame,
            _groups=groups,
            _offered_event_ids=progress._offered_event_ids,
        ),
    )


def _run_benchmark_scenario(
    config: _BenchmarkConfig,
    faults: _SimulationFaults,
) -> _BenchmarkSummary:
    """Run one validated deterministic benchmark and optional fault scenario."""
    _validate(config)
    if config.state_dir is not None:
        config.state_dir.mkdir(parents=True, exist_ok=True)
    state_dir_text = (
        str(config.state_dir) if config.state_dir is not None else '<temporary-directory>'
    )
    temporary_directory = (
        tempfile.TemporaryDirectory(prefix='sg-event-daemon-benchmark-')
        if config.state_dir is None
        else tempfile.TemporaryDirectory(
            prefix='sg-event-daemon-benchmark-',
            dir=config.state_dir,
        )
    )
    started = time.perf_counter()
    with temporary_directory as temporary:
        database_path = Path(temporary) / 'benchmark.sqlite3'
        summary, serialized_event_bytes = _workload(config, database_path, faults)
        strict, strict_failures = _strict_rejoin(
            config,
            serialized_event_bytes,
            faults,
        )
    elapsed_secs = time.perf_counter() - started
    result: _BenchmarkSummary = {
        **summary,
        'database_path': str(database_path),
        'elapsed_secs': elapsed_secs,
        'elapsed_throughput_events_per_sec': summary['event_count'] / max(elapsed_secs, 1e-9),
        'state_dir': state_dir_text,
        'strict_rejoin': strict,
        'socket_buffers': _socket_buffers(),
        'catchup_transitions': summary['catchup_transitions'] + strict['catchup_transitions'],
        'failures': _acceptance_failures(config, summary, strict, strict_failures),
        'config': {
            key: str(value) if isinstance(value, Path) else value
            for key, value in asdict(config).items()
        },
    }
    return result


def _run_benchmark(config: _BenchmarkConfig | None = None) -> _BenchmarkSummary:
    """Run one deterministic workload and return its JSON-compatible summary."""
    resolved = _BenchmarkConfig() if config is None else config
    return _run_benchmark_scenario(resolved, _SimulationFaults())


def _main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark command-line interface."""
    arguments = _argument_parser().parse_args(argv)
    config = _BenchmarkConfig(
        rate=arguments.rate,
        duration_secs=arguments.duration_secs,
        groups=arguments.groups,
        match_ratio=arguments.match_ratio,
        callbacks_per_match=arguments.callbacks_per_match,
        payload_bytes=arguments.payload_bytes,
        numeric_gap_rate=arguments.numeric_gap_rate,
        ordering=arguments.ordering,
        holdback_secs=arguments.holdback_secs,
        sqlite_synchronous=arguments.sqlite_synchronous,
        simulated_callback_latency_ms=arguments.simulated_callback_latency_ms,
        simulated_storage_latency_ms=arguments.simulated_storage_latency_ms,
        state_dir=arguments.state_dir,
    )
    try:
        summary = _run_benchmark(config)
    except (OSError, ValueError) as error:
        print(f'benchmark error: {error}', file=sys.stderr)
        return 2
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 1 if summary['failures'] else 0


if __name__ == '__main__':
    raise SystemExit(_main())
