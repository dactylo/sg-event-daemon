from __future__ import annotations

import json
import math
import re
import sqlite3
import statistics
import subprocess
import threading
import time
from collections import Counter, deque
from collections.abc import Mapping
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from itertools import pairwise
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, cast

from sg_event_daemon._events import EVENT_FIELDS
from tests.live._daemon import LIVE_TIMEOUT_SECS, LiveReport, ProbeRecord, ProducerProgress

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Iterable, Sequence
    from pathlib import Path

    from tests.live._site import LiveSite


@dataclass(frozen=True)
class GroupDeliveryMetrics:
    delivered_unique_ids: tuple[int, ...]
    duplicate_ids: tuple[int, ...]
    inversions: tuple[tuple[int, int], ...]
    missing_ids: tuple[int, ...]


@dataclass(frozen=True)
class LoadPhases:
    live_count: int
    stopped_count: int


@dataclass
class BulkProgress:
    successful_updates: int = 0
    successful_batches: int = 0
    elapsed_secs: float = 0.0
    setup_secs: float = 0.0
    drain_secs: float = 0.0
    failures: list[str] = field(default_factory=list[str])
    production_ended_at: float | None = None
    production_daemon_cpu: dict[int, float] = field(default_factory=dict[int, float])
    production_harness_cpu: float | None = None


class ResidentSampler:
    def __init__(self, root_pid: int, *, interval_secs: float = 1.0) -> None:
        self._root_pid = root_pid
        self._interval_secs = interval_secs
        self._peaks: dict[int, int] = {}
        self._lock = threading.Lock()
        self._error: Exception | None = None
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name='bulk-resident-sampler')

    def _run(self) -> None:
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                output = subprocess.run(
                    ['ps', '-A', '-o', 'pid=,ppid=,rss='],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=10.0,
                ).stdout
                rows = [tuple(map(int, line.split())) for line in output.splitlines()]
                descendants = {self._root_pid}
                while True:
                    children = {pid for pid, parent, _rss in rows if parent in descendants}
                    if children <= descendants:
                        break
                    descendants.update(children)
                descendants.discard(self._root_pid)
                with self._lock:
                    for pid, _parent, rss in rows:
                        if pid in descendants:
                            self._peaks[pid] = max(rss, self._peaks.get(pid, 0))
            except (OSError, subprocess.SubprocessError, ValueError) as error:
                self._error = error
                return
            self.stop.wait(max(0.0, self._interval_secs - (time.monotonic() - started)))

    def finish(self) -> None:
        self.stop.set()
        self.thread.join()
        if self._error is not None:
            raise RuntimeError('resident sampler failed') from self._error

    def peaks(self) -> dict[int, int]:
        with self._lock:
            return dict(self._peaks)

    def start(self) -> None:
        self.thread.start()


class VisibilityObserver:
    def __init__(self, site: LiveSite, *, after_id: int) -> None:
        self._site = site
        self.floor = after_id
        self.events: dict[int, dict[str, object]] = {}
        self.first_seen: dict[int, float] = {}
        self._high_water: deque[tuple[float, int]] = deque()
        self._highest_seen_id = after_id
        self.skipped_ranges: list[tuple[int, int]] = []
        self.cadence: list[float] = []
        self.completion_intervals: list[float] = []
        self.failures: list[str] = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, name='bulk-visibility-observer')

    def _advance_floor(self, completed: float) -> None:
        while self.floor + 1 in self.first_seen:
            self.floor += 1
        aged_high = self.floor
        while self._high_water and completed - self._high_water[0][0] >= 30.0:
            aged_high = max(aged_high, self._high_water.popleft()[1])
        if aged_high > self.floor:
            next_id = self.floor + 1
            for identifier in sorted(
                identifier
                for identifier in self.first_seen
                if self.floor < identifier <= aged_high
            ):
                if identifier > next_id:
                    self.skipped_ranges.append((next_id, identifier - 1))
                next_id = identifier + 1
            self.floor = aged_high
            while self.floor + 1 in self.first_seen:
                self.floor += 1

    def _record_ids(
        self, rows: list[dict[str, object]], *, completed: float, visible_at: float
    ) -> None:
        for row in rows:
            identifier = cast('int', row['id'])
            self.first_seen.setdefault(identifier, visible_at)
            self._highest_seen_id = max(self._highest_seen_id, identifier)
        if self._highest_seen_id > (self._high_water[-1][1] if self._high_water else self.floor):
            self._high_water.append((completed, self._highest_seen_id))

    def expected_ids(self, shot_ids: Collection[int]) -> set[int]:
        return {
            identifier
            for identifier, row in self.events.items()
            if isinstance(entity := row.get('entity'), Mapping)
            and cast('Mapping[str, object]', entity).get('type') == 'Shot'
            and cast('Mapping[str, object]', entity).get('id') in shot_ids
        }

    def finish(self) -> None:
        self.stop.set()
        self.thread.join()

    def late_ids(self) -> set[int]:
        earliest_higher = float('inf')
        late: set[int] = set()
        for identifier in sorted(self.first_seen, reverse=True):
            first = self.first_seen[identifier]
            if first > earliest_higher:
                late.add(identifier)
            earliest_higher = min(earliest_higher, first)
        return late

    def run(self) -> None:
        previous: float | None = None
        previous_completion: float | None = None
        while not self.stop.is_set():
            started = time.monotonic()
            if previous is not None:
                self.cadence.append(started - previous)
            previous = started
            try:
                rows = self._site.source_client.find(
                    'EventLogEntry',
                    [['id', 'greater_than', self.floor]],
                    ['id'],
                    order=[{'field_name': 'id', 'direction': 'asc'}],
                )
                completed = time.monotonic()
                if previous_completion is not None:
                    self.completion_intervals.append(completed - previous_completion)
                previous_completion = completed
                self._record_ids(rows, completed=completed, visible_at=time.time())
                self._advance_floor(completed)
            except Exception as error:  # noqa: BLE001
                self.failures.append(type(error).__name__)
            self.stop.wait(max(0.0, 0.1 - (time.monotonic() - started)))

    def select_events(self, shot_ids: Collection[int]) -> None:
        identifiers = sorted(self.first_seen)
        classified: dict[int, dict[str, object]] = {}
        self.events = {}
        for offset in range(0, len(identifiers), 500):
            rows = self._site.source_client.find(
                'EventLogEntry',
                [['id', 'in', identifiers[offset : offset + 500]]],
                list(EVENT_FIELDS),
                order=[{'field_name': 'id', 'direction': 'asc'}],
            )
            for row in rows:
                identifier = cast('int', row['id'])
                if identifier in self.first_seen and 'entity' in row and 'project' in row:
                    classified[identifier] = row
        missing = sorted(self.first_seen.keys() - classified.keys())
        if missing:
            raise AssertionError(
                f'visibility classification incomplete for {len(missing)} observed IDs: '
                f'{missing[:10]}'
            )
        self.events = {
            identifier: row
            for identifier, row in classified.items()
            if isinstance(entity := row['entity'], Mapping)
            and cast('Mapping[str, object]', entity).get('type') == 'Shot'
            and cast('Mapping[str, object]', entity).get('id') in shot_ids
            and isinstance(project := row['project'], Mapping)
            and cast('Mapping[str, object]', project).get('id') == self._site.project['id']
        }

    def start(self) -> None:
        self.thread.start()


@dataclass(frozen=True)
class BulkCriteriaEvidence:
    progress: BulkProgress
    observer: VisibilityObserver
    expected_ids: Collection[int]
    records_by_group: Mapping[str, Collection[ProbeRecord]]
    late_ids: Collection[int]
    failures: Mapping[str, int]
    log_paths: Iterable[Path]
    duration_secs: float
    sanitize_text: Callable[[str], str]


def _assert_eager_daemon_latency(summary: Mapping[str, object] | None, late_ids: set[int]) -> None:
    assert late_ids, 'bulk load requires at least one late event'
    assert summary is not None, 'eager first-return evidence unavailable'
    assert summary['expected_count'] == len(late_ids), 'eager late population mismatch'
    assert summary['missing_evidence_count'] == 0, 'eager first-return evidence incomplete'
    assert summary['over_limit_count'] == 0, 'eager late event exceeded 10 seconds'


def _assert_eager_latency(
    late_ids: set[int],
    records_by_group: Mapping[str, Collection[ProbeRecord]],
    first_seen: Mapping[int, float],
) -> None:
    for row in records_by_group.get('bulk-eager', ()):
        if row.event_id in late_ids:
            assert row.delivered_at is not None, 'eager late event has no delivery time'
            assert row.event_id in first_seen, 'eager late event has no observer time'
            assert row.delivered_at - first_seen[row.event_id] <= 10.0, (
                'eager late event exceeded 10 seconds'
            )


def _assert_late_event_present(late_ids: set[int]) -> None:
    assert late_ids, 'bulk load requires at least one late event'


def _assert_no_worker_failures(failures: Mapping[str, int]) -> None:
    assert not any(failures.values()), f'worker failures occurred: {dict(failures)}'


def _assert_observer_healthy(observer: VisibilityObserver) -> None:
    assert not observer.failures, 'visibility observer failed'


def _bulk_producer(
    task: tuple[LiveSite, int | None],
    stop: threading.Event,
    lock: threading.Lock,
    progress: BulkProgress,
    deadline: float,
) -> None:
    site, shot_id = task
    try:
        client = site.open_thread_producer()
        index = 0
        while not stop.is_set() and time.monotonic() < deadline:
            if shot_id is None:
                site.create_shot_batch(100)
                with lock:
                    progress.successful_batches += 1
            else:
                client.update('Shot', shot_id, {'description': f'{site.namespace}-update-{index}'})
                with lock:
                    progress.successful_updates += 1
            index += 1
    except Exception as error:  # noqa: BLE001
        with lock:
            progress.failures.append(type(error).__name__)
        stop.set()


def _daemon_latency_summary(
    expected_late: set[int],
    rows: Collection[ProbeRecord],
    timeline: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    counts = Counter(row.event_id for row in rows if row.event_id in expected_late)
    values = [
        cast('float', timeline[str(identifier)]['daemon_latency_secs'])
        for identifier in sorted(expected_late)
        if counts[identifier] == 1 and timeline[str(identifier)]['daemon_latency_secs'] is not None
    ]
    return {
        'expected_count': len(expected_late),
        'count': len(values),
        'median_secs': statistics.median(values) if values else None,
        'mean_secs': statistics.mean(values) if values else None,
        'max_secs': max(values) if values else None,
        'missing_evidence_count': len(expected_late) - len(values),
        'over_limit_count': sum(value > 10.0 for value in values),
    }


def _event_timeline(
    row: ProbeRecord | None,
    first_seen: float | None,
    source: tuple[float, int] | None,
    lane_requests: list[int],
) -> dict[str, object]:
    completed = None if source is None else source[0]
    delivered = None if row is None else row.delivered_at
    elapsed = (
        delivered - completed
        if completed is not None
        and delivered is not None
        and math.isfinite(completed)
        and math.isfinite(delivered)
        else None
    )
    if elapsed is not None and not math.isfinite(elapsed):
        elapsed = None
    return {
        'observer_first_seen': first_seen,
        'delivered_at': delivered,
        'source_read_completed_at': completed,
        'source_request': None if source is None else source[1],
        'source_to_delivery_secs': elapsed,
        'daemon_latency_secs': None if elapsed is None else max(0.0, elapsed),
        'lane_requests': lane_requests,
    }


def _latency_summary(values: list[float]) -> dict[str, object]:
    if not values:
        return {'count': 0, 'median_secs': None, 'p95_secs': None, 'max_secs': None}
    ordered = sorted(values)
    return {
        'count': len(values),
        'median_secs': statistics.median(ordered),
        'p95_secs': ordered[int(len(ordered) * 0.95)],
        'max_secs': ordered[-1],
    }


def _record_bulk_progress(site: LiveSite, progress: BulkProgress, report: LiveReport) -> None:
    report.load_metrics['bulk_producer'] = {
        'successful_updates': progress.successful_updates,
        'successful_batches': progress.successful_batches,
        'elapsed_secs': progress.elapsed_secs,
        'setup_secs': progress.setup_secs,
        'drain_secs': progress.drain_secs,
        'failures': progress.failures,
        'tracked_shots': len(site.bulk_shot_ids),
    }


def _returned_delivery_debt(
    expected: set[int],
    rows: Collection[ProbeRecord],
    source: Mapping[int, tuple[float, int]],
) -> dict[str, int]:
    first_callback: dict[int, float] = {}
    for row in rows:
        if (
            row.event_id in expected
            and row.delivered_at is not None
            and math.isfinite(row.delivered_at)
        ):
            first_callback[row.event_id] = min(
                first_callback.get(row.event_id, row.delivered_at), row.delivered_at
            )
    points: list[tuple[float, int]] = []
    outstanding = 0
    for identifier in expected.intersection(source):
        completed = source[identifier][0]
        callback = first_callback.get(identifier)
        if callback is not None and callback <= completed:
            continue
        points.append((completed, 1))
        if callback is None:
            outstanding += 1
        else:
            points.append((callback, -1))
    debt = peak = 0
    for _timestamp, change in sorted(points):
        debt += change
        peak = max(peak, debt)
    return {'peak_count': peak, 'outstanding_count': outstanding}


def analyze_group_deliveries(
    expected_ids: Iterable[int],
    records: Iterable[ProbeRecord],
) -> GroupDeliveryMetrics:
    expected = frozenset(expected_ids)
    delivered = tuple(record.event_id for record in records if record.event_id in expected)
    counts = Counter(delivered)
    return GroupDeliveryMetrics(
        delivered_unique_ids=tuple(sorted(counts)),
        duplicate_ids=tuple(
            sorted(identifier for identifier, count in counts.items() if count > 1)
        ),
        inversions=tuple(
            (previous, current) for previous, current in pairwise(delivered) if previous > current
        ),
        missing_ids=tuple(sorted(expected.difference(counts))),
    )


def _assert_strict_order(
    expected_ids: Collection[int], records_by_group: Mapping[str, Collection[ProbeRecord]]
) -> None:
    for group, records in records_by_group.items():
        if group == 'bulk-strict' and analyze_group_deliveries(expected_ids, records).inversions:
            raise AssertionError(f'strict group {group} has an inversion')


def assert_bulk_progress(progress: BulkProgress, *, duration_secs: float) -> None:
    assert not progress.failures, 'bulk producers failed'
    assert progress.elapsed_secs >= duration_secs, 'bulk phase did not span two windows'
    assert progress.successful_updates > 0, 'both bulk producer categories must perform work'
    assert progress.successful_batches > 0, 'both bulk producer categories must perform work'


def assert_load_deliveries(
    expected_ids: Collection[int],
    records_by_group: Mapping[str, Iterable[ProbeRecord]],
) -> Mapping[str, GroupDeliveryMetrics]:
    if not records_by_group:
        raise AssertionError('load delivery has no probe groups')
    metrics = {
        group: analyze_group_deliveries(expected_ids, records)
        for group, records in records_by_group.items()
    }
    delivered_sets = {value.delivered_unique_ids for value in metrics.values()}
    if len(delivered_sets) != 1:
        raise AssertionError('probe groups observed different event sets')
    for group, value in metrics.items():
        if value.missing_ids:
            raise AssertionError(f'probe group {group} has missing event IDs')
        if value.duplicate_ids:
            raise AssertionError(f'probe group {group} has duplicate event IDs')
    return MappingProxyType(metrics)


def index_response_reads(
    requests: Sequence[Mapping[str, object]],
) -> tuple[dict[int, tuple[float, int]], dict[str, dict[int, list[int]]]]:
    source: dict[int, tuple[float, int]] = {}
    ambiguous: set[int] = set()
    lanes: dict[str, dict[int, list[int]]] = {}
    for index, request in enumerate(requests):
        calls = cast('list[str]', request.get('source_calls', []))
        if not request.get('succeeded') or not calls:
            continue
        thread = cast('str', request['thread'])
        completed = request.get('completed_at')
        identifiers = cast('list[int]', request['event_ids'])
        if thread.startswith('sg-event-daemon:source:') and calls[-1] in {
            '_query_main',
            '_recheck_missing',
            '_perform_recheck',
        }:
            if not isinstance(completed, (int, float)) or not math.isfinite(completed):
                ambiguous.update(identifiers)
                continue
            for identifier in identifiers:
                candidate = (completed, index)
                source[identifier] = min(source.get(identifier, candidate), candidate)
        elif thread.startswith('sg-event-daemon:lane:') and calls[-1] in {
            '_query_range',
            'fetch_id_runs',
        }:
            group = thread.removeprefix('sg-event-daemon:lane:')
            events = lanes.setdefault(group, {})
            for identifier in identifiers:
                events.setdefault(identifier, []).append(index)
    # A finite repeat cannot establish the first completion of an ambiguous return.
    for identifier in ambiguous:
        source.pop(identifier, None)
    return source, lanes


def load_timeout(count: int) -> float:
    if not 2 <= count <= 1_000:
        raise ValueError('live load event count must be between 2 and 1000')
    return LIVE_TIMEOUT_SECS + float(count)


def partition_load(count: int) -> LoadPhases:
    if not 2 <= count <= 1_000:
        raise ValueError('live load event count must be between 2 and 1000')
    return LoadPhases(live_count=(count + 1) // 2, stopped_count=count // 2)


def perform_updates(
    site: LiveSite,
    start: int,
    count: int,
    phase: Literal['live', 'stopped'],
    report: LiveReport,
) -> ProducerProgress:
    progress = ProducerProgress(phase=phase, requested_updates=count)
    report.producer_progress[phase] = progress
    report.requested_updates += count
    started = time.monotonic()
    try:
        for index in range(start, start + count):
            value = f'{site.namespace}-load-{index:06d}'
            site.update_description(value)
            progress.phase_values.append(value)
            progress.successful_updates += 1
            report.expected_phases.append(value)
            report.successful_updates += 1
    finally:
        progress.elapsed_secs = time.monotonic() - started
    return progress


def read_closeout_requests(
    path: Path,
    provider: str,
    strict_groups: Iterable[str],
) -> dict[str, dict[str, object]]:
    groups = tuple(strict_groups)
    if provider != 'shotgun-api3':
        return {group: {'measured': False} for group in groups}
    if not path.exists():
        raise AssertionError('request recorder produced no log')
    durations: dict[str, list[float]] = {group: [] for group in groups}
    for line in path.read_text(encoding='utf-8').splitlines():
        row = json.loads(line)
        for group in groups:
            if row['thread'] == f'sg-event-daemon:lane:{group}' and row['between']:
                durations[group].append(row['duration_secs'])
    return {
        group: {'measured': True, 'count': len(values), 'durations_secs': values}
        for group, values in durations.items()
    }


def read_http_attempts(path: Path, provider: str) -> list[dict[str, object]]:
    if provider != 'shotgun-api3':
        return []
    http_path = path.with_suffix('.http.jsonl')
    return [
        cast('dict[str, object]', json.loads(line))
        for line in http_path.read_text(encoding='utf-8').splitlines()
    ]


def read_process_cpu_secs(root_pid: int) -> dict[int, float]:
    output = subprocess.run(
        ['ps', '-A', '-o', 'pid=,ppid=,time='],
        capture_output=True,
        text=True,
        check=True,
        timeout=10.0,
    ).stdout
    rows: list[tuple[int, int, float]] = []
    for line in output.splitlines():
        pid_text, parent_text, cpu_text = line.split()
        days_text, separator, clock = cpu_text.partition('-')
        days = int(days_text) if separator else 0
        if not separator:
            clock = cpu_text
        fields = clock.split(':')
        seconds = float(fields[-1]) + int(fields[-2]) * 60 + days * 86_400
        if len(fields) == 3:
            seconds += int(fields[0]) * 3_600
        rows.append((int(pid_text), int(parent_text), seconds))
    descendants = {root_pid}
    while True:
        children = {pid for pid, parent, _cpu in rows if parent in descendants}
        if children <= descendants:
            break
        descendants.update(children)
    return {pid: cpu for pid, _parent, cpu in rows if pid in descendants}


def _record_bulk_cpu(
    report: LiveReport,
    daemon_pid: int,
    initial_cpu: dict[int, float],
    initial_harness_cpu: float,
    final_harness_cpu: float,
) -> dict[int, float]:
    final_cpu = read_process_cpu_secs(daemon_pid)
    daemon_cpu = {str(pid): value - initial_cpu.get(pid, 0.0) for pid, value in final_cpu.items()}
    daemon_cpu['total'] = sum(daemon_cpu.values())
    report.load_metrics['daemon_cpu_secs'] = cast('dict[str, object]', daemon_cpu)
    cast('dict[str, object]', report.load_metrics)['harness_cpu_secs'] = (
        final_harness_cpu - initial_harness_cpu
    )
    return final_cpu


def perform_bulk_load(
    site: LiveSite, *, duration_secs: float, report: LiveReport, daemon_pid: int | None = None
) -> BulkProgress:
    progress = BulkProgress()
    stop = threading.Event()
    lock = threading.Lock()
    threads: list[threading.Thread] = []
    setup_started = time.monotonic()
    started: float | None = None
    deadline = 0.0
    initial_cpu: dict[int, float] = {}
    initial_harness_cpu = 0.0

    try:
        shots = site.create_shot_batch(24)
        if daemon_pid is not None:
            initial_cpu = read_process_cpu_secs(daemon_pid)
            initial_harness_cpu = time.process_time()
        started = time.monotonic()
        progress.setup_secs = started - setup_started
        deadline = started + duration_secs
        for shot_id in (*shots, *(None for _ in range(6))):
            thread = threading.Thread(
                target=_bulk_producer,
                args=((site, shot_id), stop, lock, progress, deadline),
                name='bulk-producer',
            )
            thread.start()
            threads.append(thread)
        stop.wait(max(0.0, deadline - time.monotonic()))
    finally:
        finished = time.monotonic()
        stop.set()
        try:
            if started is None:
                progress.setup_secs = finished - setup_started
            else:
                progress.elapsed_secs = (
                    duration_secs if finished >= deadline else finished - started
                )
                if daemon_pid is not None:
                    progress.production_ended_at = finished
                    progress.production_harness_cpu = time.process_time()
                    progress.production_daemon_cpu = _record_bulk_cpu(
                        report,
                        daemon_pid,
                        initial_cpu,
                        initial_harness_cpu,
                        progress.production_harness_cpu,
                    )
        finally:
            drain_started = time.monotonic()
            # Cleanup must never race a batch still committing on the server.
            for thread in threads:
                thread.join()
            progress.drain_secs = time.monotonic() - drain_started
            _record_bulk_progress(site, progress, report)
    return progress


def read_request_timeline(path: Path, provider: str) -> list[dict[str, object]]:
    if provider != 'shotgun-api3':
        return []
    return [
        cast('dict[str, object]', json.loads(line))
        for line in path.read_text(encoding='utf-8').splitlines()
    ]


def read_worker_failures(state_dir: Path) -> dict[str, int]:
    with closing(
        sqlite3.connect(f'{(state_dir / "supervisor.db").resolve().as_uri()}?mode=ro', uri=True)
    ) as connection:
        return dict(connection.execute('SELECT worker_id, failure_count FROM worker_breaker'))


def record_bulk_catch_up_cpu(report: LiveReport, progress: BulkProgress, daemon_pid: int) -> None:
    assert progress.production_ended_at is not None
    assert progress.production_harness_cpu is not None
    final_cpu = read_process_cpu_secs(daemon_pid)
    daemon_cpu = {
        str(pid): value - progress.production_daemon_cpu.get(pid, 0.0)
        for pid, value in final_cpu.items()
    }
    daemon_cpu['total'] = sum(daemon_cpu.values())
    report.load_metrics['daemon_cpu_secs_catch_up'] = {
        **daemon_cpu,
        'window_secs': max(0.0, time.monotonic() - progress.production_ended_at),
    }
    cast('dict[str, object]', report.load_metrics)['harness_cpu_secs_catch_up'] = (
        time.process_time() - progress.production_harness_cpu
    )


def record_bulk_health(
    report: LiveReport, log_paths: Iterable[Path], sanitize_text: Callable[[str], str]
) -> dict[str, str]:
    patterns = {
        'source_pause': re.compile(r'source paused:'),
        'give_up': re.compile(r'gave up on'),
        'warning': re.compile(r'^\S+ WARNING sg_event_daemon(?:\.[\w.]+)?:'),
        'fatal_line': re.compile(r'^\d{4}-\d{2}-\d{2}T\S+ error: '),
    }
    matches: dict[str, list[str]] = {name: [] for name in patterns}
    detach_pattern = re.compile(
        r'^(\S+) INFO sg_event_daemon\.source: detached group (\S+) '
        r'from its live channel: (queue-count-exhausted|queue-byte-exhausted|'
        r'strict-buffer-capacity)(?=\s|$)'
    )
    detaches: list[dict[str, object]] = []
    unavailable = False
    for path in log_paths:
        try:
            lines = path.read_text(encoding='utf-8').splitlines()
        except OSError:
            unavailable = True
            continue
        for line_number, line in enumerate(lines, 1):
            if match := detach_pattern.match(line):
                detaches.append(
                    {
                        'timestamp': match[1],
                        'group': match[2],
                        'reason': match[3],
                        'source_file': str(path),
                        'source_line': line_number,
                        'line': sanitize_text(line),
                    }
                )
            for name, pattern in patterns.items():
                if pattern.search(line):
                    matches[name].append(line)
    report.load_metrics['health'] = {
        name: {
            'available': not unavailable,
            'count': None if unavailable else len(lines),
            'lines': [sanitize_text(line) for line in lines[:5]],
        }
        for name, lines in matches.items()
    }
    report.load_metrics['health']['capacity_detaches'] = {
        'available': not unavailable,
        'count': None if unavailable else len(detaches),
        'entries': detaches,
    }
    criteria: dict[str, str] = {}
    for name in patterns:
        if unavailable:
            criteria[name] = 'failed: daemon log unavailable'
        elif matches[name]:
            criteria[name] = f'failed: daemon {name.replace("_", " ")} occurred'
        else:
            criteria[name] = 'passed'

    return criteria


def evaluate_bulk_criteria(
    report: LiveReport, evidence: BulkCriteriaEvidence
) -> dict[str, object]:
    health_criteria = record_bulk_health(report, evidence.log_paths, evidence.sanitize_text)
    criteria: dict[str, str] = {}

    def judge(name: str, check: Callable[[], object]) -> None:
        try:
            check()
        except AssertionError as error:
            criteria[name] = f'failed: {error}'
        else:
            criteria[name] = 'passed'

    judge(
        'producer',
        lambda: assert_bulk_progress(evidence.progress, duration_secs=evidence.duration_secs),
    )
    judge('observer', lambda: _assert_observer_healthy(evidence.observer))
    judge(
        'delivery',
        lambda: assert_load_deliveries(evidence.expected_ids, evidence.records_by_group),
    )
    late = set(evidence.late_ids).intersection(evidence.expected_ids)
    judge('late_event_present', lambda: _assert_late_event_present(late))
    judge(
        'strict_order',
        lambda: _assert_strict_order(evidence.expected_ids, evidence.records_by_group),
    )
    judge(
        'eager_latency',
        lambda: _assert_eager_latency(
            late, evidence.records_by_group, evidence.observer.first_seen
        ),
    )
    eager_metrics = cast('Mapping[str, object]', report.load_metrics.get('bulk-eager', {}))
    judge(
        'eager_daemon_latency',
        lambda: _assert_eager_daemon_latency(
            cast('Mapping[str, object] | None', eager_metrics.get('daemon_latency_summary')),
            late,
        ),
    )
    judge('worker_failures', lambda: _assert_no_worker_failures(evidence.failures))
    criteria.update(health_criteria)

    visibility = report.load_metrics.get('visibility', {})
    intervals = cast('list[float]', visibility.get('completion_intervals_secs', []))
    maximum = max(intervals, default=0.0)
    return {
        'criteria': criteria,
        'descriptive_criteria': ['eager_latency'],
        'observer_max_completion_interval_secs': maximum,
        'site_slow': maximum > 5.0,
        'outcome': 'failed'
        if any(value != 'passed' for name, value in criteria.items() if name != 'eager_latency')
        else 'passed',
    }


def record_bulk_visibility(
    observer: VisibilityObserver, shot_ids: Collection[int], report: LiveReport
) -> set[int]:
    expected = observer.expected_ids(shot_ids)
    report.load_metrics['visibility'] = {
        'expected_count': len(expected),
        'late_ids': sorted(observer.late_ids().intersection(expected)),
        'first_seen': {str(key): observer.first_seen[key] for key in sorted(expected)},
        'poll_cadence_secs': observer.cadence,
        'completion_intervals_secs': observer.completion_intervals,
        'failures': observer.failures,
        'whole_site_observed_count': len(observer.first_seen),
        'contiguous_floor': observer.floor,
        'skipped_id_ranges': observer.skipped_ranges,
    }
    return expected


@contextmanager
def sample_resident_memory(root_pid: int, report: LiveReport) -> Generator[None, None, None]:
    sampler = ResidentSampler(root_pid)
    sampler.start()
    try:
        yield
    finally:
        try:
            sampler.finish()
        finally:
            report.load_metrics['worker_peak_rss_kib'] = {
                str(pid): rss for pid, rss in sampler.peaks().items()
            }


def record_bulk_deliveries(
    observer: VisibilityObserver,
    expected: set[int],
    records: Mapping[str, Collection[ProbeRecord]],
    report: LiveReport,
    *,
    requests: Sequence[Mapping[str, object]] = (),
) -> set[int]:
    late_ids = observer.late_ids()
    expected_late = expected.intersection(late_ids)
    expected_on_time = expected - expected_late
    source, lanes = index_response_reads(requests)
    expected_ids = set(expected)
    for group, rows in records.items():
        metrics = analyze_group_deliveries(expected, rows)
        late_latency = [
            row.delivered_at - observer.first_seen[row.event_id]
            for row in rows
            if row.event_id in expected_late and row.delivered_at is not None
        ]
        on_time_latency = [
            row.delivered_at - observer.first_seen[row.event_id]
            for row in rows
            if row.event_id in expected_on_time and row.delivered_at is not None
        ]
        first_records: dict[int, ProbeRecord] = {}
        for row in rows:
            if row.event_id in expected:
                first_records.setdefault(row.event_id, row)
        timeline = {
            str(identifier): _event_timeline(
                first_records.get(identifier),
                observer.first_seen.get(identifier),
                source.get(identifier),
                lanes.get(group, {}).get(identifier, []),
            )
            for identifier in sorted(expected)
        }
        report.load_metrics[group] = {
            'delivered_count': len(metrics.delivered_unique_ids),
            'duplicate_ids': metrics.duplicate_ids,
            'missing_ids': metrics.missing_ids,
            'inversions': metrics.inversions,
            'late_latency_secs': late_latency,
            'on_time_latency_secs': on_time_latency,
            'latency_summary': {
                'late': _latency_summary(late_latency),
                'on_time': _latency_summary(on_time_latency),
            },
            'event_timeline': timeline,
            'source_read_missing_count': len(expected - source.keys()),
            'returned_delivery_debt': _returned_delivery_debt(expected_ids, rows, source),
        }
        if group == 'bulk-eager':
            report.load_metrics[group]['daemon_latency_summary'] = _daemon_latency_summary(
                expected_late, rows, timeline
            )
    return late_ids
