from __future__ import annotations

import io
import json
import math
import os
import runpy
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import AbstractContextManager, closing, nullcontext
from dataclasses import asdict, dataclass, replace
from traceback import FrameSummary
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Unpack, cast

import pytest

import sg_event_daemon._source as source_module
import tests.live._load as load_module
import tests.live.test_load as bulk_test
from sg_event_daemon._cli import _report_worker_failure
from sg_event_daemon._config import (
    CredentialConfig,
    GroupConfig,
    PluginConfig,
    SecretValue,
    SiteConfig,
    SourceConfig,
)
from sg_event_daemon._events import EVENT_FIELDS
from sg_event_daemon._gaps import log_catchup_skipped, log_ids_given_up, log_paused
from sg_event_daemon._logging import configure_logging
from sg_event_daemon._source import _id_runs_filter
from tests.live._daemon import LiveReport, ProbeRecord, ProducerProgress, read_probe_records
from tests.live._load import (
    GroupDeliveryMetrics,
    LoadPhases,
    VisibilityObserver,
    analyze_group_deliveries,
    assert_load_deliveries,
    load_timeout,
    partition_load,
    perform_bulk_load,
    perform_updates,
    read_closeout_requests,
    record_bulk_deliveries,
    record_bulk_visibility,
)
from tests.live._site import LiveSite
from tests.live.fixtures import bulk_probe
from tests.live.fixtures.bulk_probe import EventContext, callback
from tests.live.fixtures.request_recorder import sitecustomize as recorder
from tests.source import test_source as source_tests

if TYPE_CHECKING:
    from collections.abc import Collection
    from pathlib import Path

    from tests._live_mode import LiveMode, LiveProvider
    from tests.live._daemon import LiveCaseFactory


class _ProbeResidentSampler(load_module.ResidentSampler):
    def finish(self) -> None:
        self._peaks = {20: 400}

    def start(self) -> None:
        assert self._root_pid == 10


class _FailingUpdateSite(LiveSite):
    fail_on_call: int
    namespace: str
    _update_calls: int
    values: list[str]

    def __init__(self) -> None:
        self.fail_on_call = 1_001
        self.namespace = 'acceptance-run'
        self._update_calls = 0
        self.values = []

    def update_description(self, value: str) -> None:
        self._update_calls += 1
        if self._update_calls == self.fail_on_call:
            raise RuntimeError('update failed')
        self.values.append(value)


def _bulk_observer_events() -> dict[int, dict[str, object]]:
    return {
        identifier: {
            'id': identifier,
            'meta': None,
            'entity': {'id': 41, 'type': 'Shot'},
            'attribute_name': 'description',
            'created_at': '2026-10-28T00:00:00Z',
            'event_type': 'Shotgun_Shot_Change',
            'project': {'id': 7, 'type': 'Project'},
            'session_uuid': None,
            'type': 'EventLogEntry',
            'user': None,
        }
        for identifier in (101, 102)
    }


def _record(event_id: int, phase_value: str | None = None) -> ProbeRecord:
    return ProbeRecord(
        event_id=event_id,
        event_type='Shotgun_Shot_Change',
        attribute_name='description',
        entity_type='Shot',
        entity_id=41,
        delivery_attempt=1,
        is_operator_replay=False,
        group='sync',
        phase_value=phase_value,
    )


class _BulkAcceptanceObserver(VisibilityObserver):
    def finish(self) -> None:
        pass

    def select_events(self, shot_ids: Collection[int]) -> None:
        del shot_ids

    def start(self) -> None:
        self.first_seen = {101: 2.0, 102: 1.0}
        self.events = _bulk_observer_events()


def _skip_sleep(_seconds: float) -> None:
    pass


def _write_bulk_probe_rows(paths: dict[str, Path]) -> None:
    for name, path in paths.items():
        path.write_text(
            ''.join(
                json.dumps(asdict(replace(_record(identifier), group=name, delivered_at=3.0)))
                + '\n'
                for identifier in (101, 102)
            ),
            encoding='utf-8',
        )


def _write_duplicate_after_shutdown(tmp_path: Path, path: Path, group: str) -> None:
    state_dir = tmp_path / 'state'
    state_dir.mkdir()
    with closing(sqlite3.connect(state_dir / 'supervisor.db')) as connection, connection:
        connection.execute('CREATE TABLE worker_breaker (worker_id TEXT, failure_count INTEGER)')
        connection.execute("INSERT INTO worker_breaker VALUES ('worker', 0)")
    with path.open('a', encoding='utf-8') as stream:
        stream.write(
            json.dumps(asdict(replace(_record(101), group=group, delivered_at=3.0))) + '\n'
        )


@pytest.fixture
def failing_site() -> _FailingUpdateSite:
    return _FailingUpdateSite()


@pytest.fixture
def report() -> LiveReport:
    return LiveReport(
        site_hostname='example.invalid',
        project_name='Live Acceptance',
        provider='sg-python-api',
        mode='sync+async',
        namespace='acceptance-run',
    )


def _daemon_summary(report: LiveReport) -> dict[str, object]:
    return cast('dict[str, object]', report.load_metrics['bulk-eager']['daemon_latency_summary'])


def _event_timeline(report: LiveReport, identifier: int) -> dict[str, object]:
    events = cast(
        'dict[str, dict[str, object]]', report.load_metrics['bulk-eager']['event_timeline']
    )
    return events[str(identifier)]


def _source_request(
    identifier: int,
    completed_at: float,
    *,
    call: str = '_query_main',
    thread: str = 'sg-event-daemon:source:events',
    succeeded: bool = True,
) -> dict[str, object]:
    return {
        'thread': thread,
        'source_calls': [call],
        'succeeded': succeeded,
        'completed_at': completed_at,
        'event_ids': [identifier],
    }


@dataclass
class _DaemonCase:
    expected: set[int] | None = None
    late: set[int] | None = None
    eager: list[ProbeRecord] | None = None
    strict: list[ProbeRecord] | None = None
    first_seen: dict[int, float] | None = None
    requests: list[dict[str, object]] | None = None
    intervals: list[float] | None = None
    failures: dict[str, int] | None = None
    log_text: str = ''


def _criteria_case(
    tmp_path: Path, report: LiveReport, *, intervals: list[float], warning: bool
) -> dict[str, object]:
    path = tmp_path / 'daemon.log'
    path.write_text(
        '2026-11-03T00:00:00+00:00 WARNING sg_event_daemon.source: delayed\n' if warning else '',
        encoding='utf-8',
    )
    observer = VisibilityObserver(cast('LiveSite', None), after_id=100)
    observer.first_seen = {101: 2.0, 102: 1.0}
    report.load_metrics['visibility'] = {'completion_intervals_secs': intervals}
    record_bulk_deliveries(
        observer,
        {101, 102},
        {
            'bulk-eager': [
                replace(_record(101), delivered_at=13.0),
                replace(_record(102), delivered_at=3.0),
            ],
            'bulk-strict': [
                replace(_record(101), delivered_at=13.0),
                replace(_record(102), delivered_at=3.0),
            ],
        },
        report,
        requests=[_source_request(101, 2.0), _source_request(102, 2.0)],
    )
    return load_module.evaluate_bulk_criteria(
        report,
        load_module.BulkCriteriaEvidence(
            progress=load_module.BulkProgress(
                successful_updates=1, successful_batches=1, elapsed_secs=60.0
            ),
            observer=observer,
            expected_ids={101, 102},
            records_by_group={
                'bulk-eager': [
                    replace(_record(101), delivered_at=13.0),
                    replace(_record(102), delivered_at=3.0),
                ],
                'bulk-strict': [
                    replace(_record(101), delivered_at=13.0),
                    replace(_record(102), delivered_at=3.0),
                ],
            },
            late_ids={101},
            failures={},
            log_paths=(path,),
            duration_secs=60.0,
            sanitize_text=lambda value: value,
        ),
    )


def _daemon_case(tmp_path: Path, report: LiveReport, case: _DaemonCase) -> dict[str, object]:
    expected = {101} if case.expected is None else case.expected
    late = {101} if case.late is None else case.late
    eager = [replace(_record(101), delivered_at=31.0)] if case.eager is None else case.eager
    strict = [replace(_record(101), delivered_at=131.0)] if case.strict is None else case.strict
    observer = VisibilityObserver(cast('LiveSite', None), after_id=100)
    observer.first_seen = {101: 1.0, 102: 0.0} if case.first_seen is None else case.first_seen
    records = {'bulk-eager': eager, 'bulk-strict': strict}
    report.load_metrics['visibility'] = {
        'completion_intervals_secs': [1.0] if case.intervals is None else case.intervals
    }
    record_bulk_deliveries(
        observer,
        expected,
        records,
        report,
        requests=[_source_request(101, 21.0)] if case.requests is None else case.requests,
    )
    path = tmp_path / 'daemon.log'
    path.write_text(case.log_text, encoding='utf-8')
    return load_module.evaluate_bulk_criteria(
        report,
        load_module.BulkCriteriaEvidence(
            progress=load_module.BulkProgress(
                successful_updates=1, successful_batches=1, elapsed_secs=60.0
            ),
            observer=observer,
            expected_ids=expected,
            records_by_group=records,
            late_ids=late,
            failures={} if case.failures is None else case.failures,
            log_paths=(path,),
            duration_secs=60.0,
            sanitize_text=lambda value: value,
        ),
    )


def test_bulk_catch_up_cpu_is_measured_from_production_end(
    monkeypatch: pytest.MonkeyPatch, report: LiveReport
) -> None:
    readings = iter(({10: 5.0}, {10: 7.0}, {10: 9.5, 11: 1.0}))
    process_times = iter((1.0, 1.5, 2.25))

    def read_cpu(_pid: int) -> dict[int, float]:
        return next(readings)

    monkeypatch.setattr(load_module, 'read_process_cpu_secs', read_cpu)
    monkeypatch.setattr(load_module.time, 'process_time', lambda: next(process_times))

    class Site:
        namespace = 'test'

        def __init__(self) -> None:
            self.bulk_shot_ids: list[int] = []

        def create_shot_batch(self, count: int) -> tuple[int, ...]:
            return (41,) if count == 24 else ()

        def open_thread_producer(self) -> object:
            return self

        def update(self, *_args: object, **_kwargs: object) -> None:
            pass

    progress = perform_bulk_load(
        cast('LiveSite', Site()), duration_secs=0.01, report=report, daemon_pid=10
    )
    load_module.record_bulk_catch_up_cpu(report, progress, 10)
    assert report.load_metrics['daemon_cpu_secs'] == {'10': 2.0, 'total': 2.0}
    assert report.load_metrics['harness_cpu_secs'] == 0.5
    catch_up = cast('dict[str, float]', report.load_metrics['daemon_cpu_secs_catch_up'])
    assert {key: value for key, value in catch_up.items() if key != 'window_secs'} == {
        '10': 2.5,
        '11': 1.0,
        'total': 3.5,
    }
    assert catch_up['window_secs'] >= 0.0
    assert report.load_metrics['harness_cpu_secs_catch_up'] == 0.75


def test_bulk_cpu_covers_the_production_phase(
    monkeypatch: pytest.MonkeyPatch,
    report: LiveReport,
) -> None:
    readings = iter(({10: 5.0, 11: 1.0}, {10: 7.0, 11: 4.5, 12: 0.5}))
    calls: list[int] = []

    class Site:
        namespace = 'test'

        def __init__(self) -> None:
            self.bulk_shot_ids: list[int] = []
            self.setup_complete = False

        def create_shot_batch(self, count: int) -> tuple[int, ...]:
            if count == 24:
                self.setup_complete = True
                return tuple(range(24))
            time.sleep(0.001)
            return ()

        def open_thread_producer(self) -> object:
            return self

        def update(self, *_args: object, **_kwargs: object) -> None:
            time.sleep(0.001)

    site = Site()

    def read_process_cpu_secs(root_pid: int) -> dict[int, float]:
        assert site.setup_complete
        assert not joined
        calls.append(root_pid)
        return next(readings)

    monkeypatch.setattr(load_module, 'read_process_cpu_secs', read_process_cpu_secs, raising=False)
    original_join = threading.Thread.join
    joined = False

    def join(thread: threading.Thread, timeout: float | None = None) -> None:
        nonlocal joined
        if thread.name == 'bulk-producer':
            joined = True
        original_join(thread, timeout)

    monkeypatch.setattr(threading.Thread, 'join', join)
    perform_bulk_load(cast('LiveSite', site), duration_secs=0.05, report=report, daemon_pid=10)
    assert calls == [10, 10]
    assert report.load_metrics['daemon_cpu_secs'] == {
        '10': 2.0,
        '11': 3.5,
        '12': 0.5,
        'total': 6.0,
    }
    assert cast('float', report.load_metrics['harness_cpu_secs']) >= 0.0
    assert joined


def test_bulk_cpu_not_recorded_if_setup_fails(
    monkeypatch: pytest.MonkeyPatch,
    report: LiveReport,
) -> None:
    class FailingSetupSite:
        def __init__(self) -> None:
            self.bulk_shot_ids: list[int] = []

        def create_shot_batch(self, count: int) -> tuple[int, ...]:
            assert count == 24
            raise RuntimeError('setup failed')

    def read_process_cpu_secs(_root_pid: int) -> dict[int, float]:
        raise AssertionError('setup failure must not sample CPU')

    monkeypatch.setattr(load_module, 'read_process_cpu_secs', read_process_cpu_secs)
    with pytest.raises(RuntimeError, match='setup failed'):
        perform_bulk_load(
            cast('LiveSite', FailingSetupSite()), duration_secs=0.05, report=report, daemon_pid=10
        )
    assert 'daemon_cpu_secs' not in report.load_metrics
    assert 'harness_cpu_secs' not in report.load_metrics


def test_bulk_criteria_are_all_judged_before_failing(tmp_path: Path, report: LiveReport) -> None:
    result = _criteria_case(tmp_path, report, intervals=[0.1], warning=True)
    criteria = cast('dict[str, str]', result['criteria'])
    assert criteria['eager_latency'].startswith('failed: ')
    assert criteria['warning'].startswith('failed: ')
    assert all(
        value == 'passed'
        for name, value in criteria.items()
        if name not in {'eager_latency', 'eager_daemon_latency', 'warning'}
    )
    assert criteria['eager_daemon_latency'].startswith('failed: ')
    assert result['outcome'] == 'failed'


def test_bulk_events_are_expected_and_matched(tmp_path: Path) -> None:

    path = tmp_path / 'bulk-eager.jsonl'
    event: dict[str, object] = {
        'id': 101,
        'event_type': 'Shotgun_Shot_New',
        'attribute_name': None,
        'entity': {'type': 'Shot', 'id': 41},
        'project': {'type': 'Project', 'id': 7},
    }
    callback('bulk-eager', path, 7)(
        cast(
            'EventContext',
            SimpleNamespace(
                event=event,
                delivery_attempt=1,
                is_operator_replay=False,
            ),
        )
    )
    records = read_probe_records(path)
    assert records[0].attribute_name is None
    assert records[0].delivered_at is not None
    observer = VisibilityObserver(cast('LiveSite', None), after_id=100)
    observer.events = {101: event}
    observer.first_seen = {101: records[0].delivered_at - 1, 102: records[0].delivered_at - 2}
    expected = observer.expected_ids({41})
    assert expected == {101}


def test_bulk_health_keeps_counts_and_redacted_lines(tmp_path: Path, report: LiveReport) -> None:
    sensitive_value = 'test-secret-sentinel'
    path = tmp_path / 'daemon.log'
    path.write_text(
        ''.join(
            '2026-11-03T00:00:00+00:00 WARNING sg_event_daemon.source: '
            f'{sensitive_value if index == 0 else index}\n'
            for index in range(7)
        ),
        encoding='utf-8',
    )
    load_module.evaluate_bulk_criteria(
        report,
        load_module.BulkCriteriaEvidence(
            progress=load_module.BulkProgress(
                successful_updates=1, successful_batches=1, elapsed_secs=60
            ),
            observer=VisibilityObserver(cast('LiveSite', None), after_id=100),
            expected_ids={101},
            records_by_group={'bulk-eager': [replace(_record(101), delivered_at=2.0)]},
            late_ids={101},
            failures={},
            log_paths=(path,),
            duration_secs=60.0,
            sanitize_text=lambda value: value.replace(sensitive_value, '<redacted>'),
        ),
    )
    warning = cast('dict[str, object]', report.load_metrics['health']['warning'])
    assert warning['count'] == 7
    assert len(cast('list[str]', warning['lines'])) == 5
    assert sensitive_value not in str(warning)
    result = load_module.evaluate_bulk_criteria(
        report,
        load_module.BulkCriteriaEvidence(
            progress=load_module.BulkProgress(
                successful_updates=1, successful_batches=1, elapsed_secs=60
            ),
            observer=VisibilityObserver(cast('LiveSite', None), after_id=100),
            expected_ids={101},
            records_by_group={'bulk-eager': [replace(_record(101), delivered_at=2.0)]},
            late_ids={101},
            failures={},
            log_paths=(tmp_path / 'missing.log',),
            duration_secs=60.0,
            sanitize_text=lambda value: value,
        ),
    )
    assert all(
        cast('dict[str, str]', result['criteria'])[key] == 'failed: daemon log unavailable'
        for key in ('source_pause', 'give_up', 'warning', 'fatal_line')
    )


def test_bulk_probe_flushes_without_syncing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(_fd: int) -> None:
        raise AssertionError('probe must flush without fsync')

    monkeypatch.setattr(os, 'fsync', forbidden)
    path = tmp_path / 'probe.jsonl'
    for identifier in (1, 2):
        bulk_probe.append(path, {'id': identifier})
        assert [json.loads(line)['id'] for line in path.read_text().splitlines()] == list(
            range(1, identifier + 1)
        )


def test_bulk_producer_joins_every_thread_before_return(
    report: LiveReport, monkeypatch: pytest.MonkeyPatch
) -> None:
    joined: list[threading.Thread] = []

    class Stop(threading.Event):
        def wait(self, timeout: float | None = None) -> bool:
            del timeout
            return super().wait()

    class ProducerThread(threading.Thread):
        def join(self, timeout: float | None = None) -> None:
            joined.append(self)
            super().join(timeout)

    class Site:
        namespace = 'test'

        def __init__(self) -> None:
            self.bulk_shot_ids: list[int] = []

        def create_shot_batch(self, count: int) -> tuple[int, ...]:
            if count == 24:
                return tuple(range(24))
            raise RuntimeError('batch failed')

        def open_thread_producer(self) -> object:
            return self

        def update(self, *_args: object, **_kwargs: object) -> None:
            pass

    monkeypatch.setattr(
        load_module,
        'threading',
        SimpleNamespace(Event=Stop, Lock=threading.Lock, Thread=ProducerThread),
    )
    monkeypatch.setattr(load_module, 'time', SimpleNamespace(monotonic=lambda: 0.0))
    progress = perform_bulk_load(cast('LiveSite', Site()), duration_secs=60.0, report=report)
    assert progress.failures
    assert set(progress.failures) == {'RuntimeError'}
    assert len(joined) == 30
    assert all(not thread.is_alive() for thread in joined)
    assert not any(thread.name == 'bulk-producer' for thread in threading.enumerate())
    assert report.load_metrics['bulk_producer']['failures'] == progress.failures


def test_bulk_report_correlates_exact_source_and_lane_reads(report: LiveReport) -> None:
    observer = VisibilityObserver(cast('LiveSite', None), after_id=100)
    observer.first_seen = {101: 2.0, 102: 1.0, 103: 1.0}
    records = [replace(_record(identifier), delivered_at=8.0) for identifier in (101, 102, 103)]
    requests: list[dict[str, object]] = [
        {
            'thread': 'sg-event-daemon:source:events',
            'source_calls': ['_query_main'],
            'succeeded': True,
            'completed_at': 3.0,
            'event_ids': [101, 103],
            'min_id': 101,
            'max_id': 103,
        },
        {
            'thread': 'sg-event-daemon:source:events',
            'source_calls': ['_query_latest'],
            'succeeded': True,
            'completed_at': 2.0,
            'event_ids': [102],
            'min_id': 102,
            'max_id': 102,
        },
        {
            'thread': 'sg-event-daemon:lane:bulk-eager',
            'source_calls': ['catch_up', '_query_range'],
            'succeeded': True,
            'completed_at': 5.0,
            'event_ids': [101, 102, 103],
            'min_id': 101,
            'max_id': 103,
        },
        {
            'thread': 'sg-event-daemon:lane:bulk-eager',
            'source_calls': ['_lowest_visible_id'],
            'succeeded': True,
            'completed_at': 4.0,
            'event_ids': [101],
            'min_id': 101,
            'max_id': 101,
        },
    ]
    record_bulk_deliveries(
        observer, {101, 102, 103}, {'bulk-eager': records}, report, requests=requests
    )
    metric = report.load_metrics['bulk-eager']
    assert metric['late_latency_secs'] == [6.0]
    assert metric['on_time_latency_secs'] == [7.0, 7.0]
    assert metric['source_read_missing_count'] == 1
    assert metric['latency_summary'] == {
        'late': {'count': 1, 'median_secs': 6.0, 'p95_secs': 6.0, 'max_secs': 6.0},
        'on_time': {'count': 2, 'median_secs': 7.0, 'p95_secs': 7.0, 'max_secs': 7.0},
    }
    timeline = cast('dict[str, dict[str, object]]', metric['event_timeline'])
    assert timeline['101']['source_to_delivery_secs'] == 5.0
    assert timeline['101']['source_request'] == 0
    assert timeline['101']['lane_requests'] == [2]
    assert timeline['102']['source_request'] is None
    assert timeline['102']['source_to_delivery_secs'] is None
    assert timeline['102']['lane_requests'] == [2]
    assert timeline['103']['source_request'] == 0


def test_bulk_report_scans_late_ids_once_for_all_deliveries(report: LiveReport) -> None:
    class Observer(VisibilityObserver):
        def __init__(self) -> None:
            super().__init__(cast('LiveSite', None), after_id=100)
            self.scans = 0
            self.first_seen = {101: 2.0, 102: 1.0}

        def late_ids(self) -> set[int]:
            self.scans += 1
            return super().late_ids()

    observer = Observer()
    records = [replace(_record(identifier), delivered_at=3.0) for identifier in (101, 102)]
    late = record_bulk_deliveries(
        observer, {101, 102}, {'bulk-eager': records, 'bulk-strict': records}, report
    )
    assert late == {101}
    assert report.load_metrics['bulk-eager']['late_latency_secs'] == [1.0]
    assert report.load_metrics['bulk-strict']['late_latency_secs'] == [1.0]
    assert observer.scans == 1


def test_bulk_workload_window_excludes_slow_initial_setup(
    monkeypatch: pytest.MonkeyPatch,
    report: LiveReport,
) -> None:
    clock = SimpleNamespace(now=0.0)
    batch_started = threading.Event()
    update_started = threading.Event()
    release = threading.Event()

    class Stop(threading.Event):
        def set(self) -> None:
            super().set()
            release.set()

        def wait(self, timeout: float | None = None) -> bool:
            if timeout is None or timeout <= 0:
                return False
            batch_started.wait()
            update_started.wait()
            clock.now += timeout
            return False

    class Site:
        namespace = 'test'

        def __init__(self) -> None:
            self.bulk_shot_ids: list[int] = []

        def create_shot_batch(self, count: int) -> tuple[int, ...]:
            if count == 24:
                clock.now += 61.0
                return tuple(range(24))
            batch_started.set()
            release.wait()
            return ()

        def open_thread_producer(self) -> object:
            return self

        def update(self, *_args: object, **_kwargs: object) -> None:
            update_started.set()
            release.wait()

    monkeypatch.setattr(
        load_module,
        'threading',
        SimpleNamespace(Event=Stop, Lock=threading.Lock, Thread=threading.Thread),
    )
    monkeypatch.setattr(load_module, 'time', SimpleNamespace(monotonic=lambda: clock.now))
    progress = perform_bulk_load(cast('LiveSite', Site()), duration_secs=0.05, report=report)
    assert progress.successful_updates > 0
    assert progress.successful_batches > 0
    assert progress.elapsed_secs == pytest.approx(0.05)
    assert progress.setup_secs >= 61.0
    assert progress.drain_secs >= 0.0


def test_delivery_assertion_allows_inversion_but_rejects_loss_or_duplicate() -> None:
    assert_load_deliveries(
        dict.fromkeys((101, 102)).keys(),
        {'sync': (record for record in (_record(102), _record(101)))},
    )
    with pytest.raises(AssertionError, match='missing'):
        assert_load_deliveries((101, 102), {'sync': (_record(101),)})
    with pytest.raises(AssertionError, match='duplicate'):
        assert_load_deliveries((101,), {'sync': (_record(101), _record(101))})


def test_delivery_assertion_rejects_divergent_group_sets() -> None:
    with pytest.raises(AssertionError, match='different event sets'):
        assert_load_deliveries(
            (101, 102),
            {'async': (_record(102),), 'sync': (_record(101),)},
        )


def test_delivery_metrics_exclude_unrelated_records() -> None:
    metrics = analyze_group_deliveries(
        expected_ids=(event_id for event_id in (101,)),
        records=(record for record in (_record(999), _record(101), _record(999))),
    )
    assert metrics == GroupDeliveryMetrics(
        delivered_unique_ids=(101,),
        duplicate_ids=(),
        inversions=(),
        missing_ids=(),
    )


def test_delivery_metrics_report_missing_duplicates_and_inversions() -> None:
    metrics = analyze_group_deliveries(
        expected_ids=(101, 102, 103),
        records=(_record(102), _record(101), _record(102)),
    )
    assert metrics.missing_ids == (103,)
    assert metrics.duplicate_ids == (102,)
    assert metrics.inversions == ((102, 101),)
    assert metrics.delivered_unique_ids == (101, 102)


def test_eager_daemon_latency_keeps_early_callback_signed(
    tmp_path: Path, report: LiveReport
) -> None:
    result = _daemon_case(
        tmp_path,
        report,
        _DaemonCase(eager=[replace(_record(101), delivered_at=20.0)]),
    )
    timeline = _event_timeline(report, 101)
    assert timeline['source_to_delivery_secs'] == -1.0
    assert timeline['daemon_latency_secs'] == 0.0
    assert cast('dict[str, str]', result['criteria'])['eager_daemon_latency'] == 'passed'
    assert result['outcome'] == 'passed'


@pytest.mark.parametrize('interval', [1.0, 6.0])
@pytest.mark.parametrize('delivered_at', [31.0, 31.001])
def test_eager_daemon_latency_keeps_end_to_end_descriptive(
    tmp_path: Path, report: LiveReport, interval: float, delivered_at: float
) -> None:
    result = _daemon_case(
        tmp_path,
        report,
        _DaemonCase(
            eager=[replace(_record(101), delivered_at=delivered_at)], intervals=[interval]
        ),
    )
    criteria = cast('dict[str, str]', result['criteria'])
    summary = _daemon_summary(report)
    timeline = _event_timeline(report, 101)
    assert timeline['source_to_delivery_secs'] == pytest.approx(delivered_at - 21.0)
    assert timeline['daemon_latency_secs'] == pytest.approx(delivered_at - 21.0)
    assert report.load_metrics['bulk-eager']['late_latency_secs'] == [
        pytest.approx(delivered_at - 1.0)
    ]
    assert summary == {
        'expected_count': 1,
        'count': 1,
        'median_secs': pytest.approx(delivered_at - 21.0),
        'mean_secs': pytest.approx(delivered_at - 21.0),
        'max_secs': pytest.approx(delivered_at - 21.0),
        'missing_evidence_count': 0,
        'over_limit_count': int(delivered_at > 31.0),
    }
    assert criteria['eager_latency'].startswith('failed: ')
    assert criteria['eager_daemon_latency'] == (
        'passed' if delivered_at == 31.0 else 'failed: eager late event exceeded 10 seconds'
    )
    assert result['outcome'] == ('passed' if delivered_at == 31.0 else 'failed')
    assert result['site_slow'] is (interval > 5.0)
    assert result['descriptive_criteria'] == ['eager_latency']


def test_eager_daemon_latency_maximum_cannot_hide_behind_mean(
    tmp_path: Path, report: LiveReport
) -> None:
    result = _daemon_case(
        tmp_path,
        report,
        _DaemonCase(
            expected={101, 102},
            late={101, 102},
            first_seen={101: 2.0, 102: 1.0, 103: 0.0},
            eager=[
                replace(_record(101), delivered_at=31.001),
                replace(_record(102), delivered_at=21.0),
            ],
            strict=[replace(_record(identifier), delivered_at=131.0) for identifier in (101, 102)],
            requests=[_source_request(101, 21.0), _source_request(102, 20.0)],
        ),
    )
    summary = _daemon_summary(report)
    assert cast('float', summary['mean_secs']) < 10.0
    assert cast('float', summary['median_secs']) < 10.0
    assert summary['over_limit_count'] == 1
    assert cast('dict[str, str]', result['criteria'])['eager_daemon_latency'].startswith(
        'failed: '
    )


@pytest.mark.parametrize(
    'variant',
    [
        'missing_source',
        'missing_callback',
        'missing_time',
        'nonfinite_time',
        'nonfinite_source',
        'overflow_share',
        'duplicate',
    ],
)
def test_eager_daemon_latency_requires_every_late_id(
    tmp_path: Path, report: LiveReport, variant: str
) -> None:
    eager = [replace(_record(101), delivered_at=31.0)]
    requests = [_source_request(101, 21.0)]
    if variant == 'missing_source':
        requests = []
    elif variant == 'missing_callback':
        eager = []
    elif variant == 'missing_time':
        eager = [replace(_record(101), delivered_at=None)]
    elif variant == 'nonfinite_time':
        eager = [replace(_record(101), delivered_at=math.inf)]
    elif variant == 'nonfinite_source':
        requests = [_source_request(101, math.nan)]
    elif variant == 'overflow_share':
        requests = [_source_request(101, -1e308)]
        eager = [replace(_record(101), delivered_at=1e308)]
    else:
        eager *= 2
    result = _daemon_case(tmp_path, report, _DaemonCase(eager=eager, requests=requests))
    criteria = cast('dict[str, str]', result['criteria'])
    summary = _daemon_summary(report)
    assert summary['expected_count'] == 1
    assert summary['missing_evidence_count'] == 1
    assert summary['count'] == 0
    assert summary['median_secs'] is None
    assert summary['mean_secs'] is None
    assert summary['max_secs'] is None
    assert criteria['eager_daemon_latency'].startswith('failed: ')
    assert 'worker_failures' in criteria
    assert 'warning' in criteria
    assert result['outcome'] == 'failed'


@pytest.mark.parametrize('call', ['_query_main', '_recheck_missing', '_perform_recheck'])
@pytest.mark.parametrize('invalid_first', [True, False])
@pytest.mark.parametrize(
    'invalid_time', ['missing', None, 'invalid', math.nan, math.inf, -math.inf]
)
def test_eager_daemon_latency_repeated_return_cannot_clear_ambiguity(
    tmp_path: Path,
    report: LiveReport,
    call: str,
    invalid_first: bool,
    invalid_time: object,
) -> None:
    invalid = _source_request(101, 21.0, call=call)
    if invalid_time == 'missing':
        invalid.pop('completed_at')
    else:
        invalid['completed_at'] = invalid_time
    valid = _source_request(101, 29.0, call=call)
    valid['event_ids'] = [101, 102]
    requests = [invalid, valid] if invalid_first else [valid, invalid]
    original_requests = json.dumps(requests)
    result = _daemon_case(
        tmp_path,
        report,
        _DaemonCase(
            expected={101, 102},
            late={101, 102},
            first_seen={101: 2.0, 102: 1.0, 103: 0.0},
            eager=[replace(_record(identifier), delivered_at=31.001) for identifier in (101, 102)],
            strict=[replace(_record(identifier), delivered_at=131.0) for identifier in (101, 102)],
            requests=requests,
        ),
    )
    assert _daemon_summary(report) == {
        'expected_count': 2,
        'count': 1,
        'median_secs': pytest.approx(2.001),
        'mean_secs': pytest.approx(2.001),
        'max_secs': pytest.approx(2.001),
        'missing_evidence_count': 1,
        'over_limit_count': 0,
    }
    ambiguous = _event_timeline(report, 101)
    assert ambiguous['source_read_completed_at'] is None
    assert ambiguous['source_request'] is None
    assert ambiguous['source_to_delivery_secs'] is None
    assert ambiguous['daemon_latency_secs'] is None
    assert ambiguous['delivered_at'] == 31.001
    assert _event_timeline(report, 102)['source_request'] == int(invalid_first)
    assert report.load_metrics['bulk-eager']['source_read_missing_count'] == 1
    assert json.dumps(requests) == original_requests
    criteria = cast('dict[str, str]', result['criteria'])
    assert criteria['eager_daemon_latency'].startswith('failed: ')
    assert all(
        value == 'passed'
        for name, value in criteria.items()
        if name not in {'eager_latency', 'eager_daemon_latency'}
    )
    assert result['outcome'] == 'failed'


@pytest.mark.parametrize(
    'excluded',
    [
        {'succeeded': False},
        {'thread': 'visibility-observer'},
        {'thread': 'sg-event-daemon:lane:bulk-eager', 'source_calls': ['fetch_id_runs']},
        {'source_calls': ['_query_latest']},
        {'source_calls': ['_query_retention']},
        {'source_calls': []},
    ],
)
@pytest.mark.parametrize('completed_at', [None, math.nan])
def test_eager_daemon_latency_excluded_reads_cannot_make_source_ambiguous(
    tmp_path: Path,
    report: LiveReport,
    excluded: dict[str, object],
    completed_at: object,
) -> None:
    ignored = _source_request(101, 1.0)
    ignored.update(excluded)
    ignored['completed_at'] = completed_at
    requests = [ignored, _source_request(101, 29.0)]
    original_requests = json.dumps(requests)
    result = _daemon_case(tmp_path, report, _DaemonCase(requests=requests))
    assert _daemon_summary(report)['missing_evidence_count'] == 0
    assert _event_timeline(report, 101)['source_request'] == 1
    assert json.dumps(requests) == original_requests
    assert cast('dict[str, str]', result['criteria'])['eager_daemon_latency'] == 'passed'
    assert result['outcome'] == 'passed'


def test_eager_daemon_latency_requires_nonempty_late_population(
    tmp_path: Path, report: LiveReport
) -> None:
    result = _daemon_case(
        tmp_path, report, _DaemonCase(expected=set(), late=set(), eager=[], strict=[])
    )
    assert _daemon_summary(report)['expected_count'] == 0
    assert cast('dict[str, str]', result['criteria'])['eager_daemon_latency'].startswith(
        'failed: '
    )
    assert result['outcome'] == 'failed'


@pytest.mark.parametrize('call', ['_query_main', '_recheck_missing', '_perform_recheck'])
@pytest.mark.parametrize('detached_catch_up', [False, True])
def test_eager_daemon_latency_uses_first_source_body_return(
    tmp_path: Path, report: LiveReport, call: str, detached_catch_up: bool
) -> None:
    lane_thread = 'sg-event-daemon:lane:bulk-eager'
    lane_reads = [_source_request(101, 30.0, call='fetch_id_runs', thread=lane_thread)]
    ignored_group_reads: list[dict[str, object]] = []
    if not detached_catch_up:
        lane_reads.insert(0, _source_request(101, 3.0, call='_query_range', thread=lane_thread))
        ignored_group_reads.append(_source_request(101, 1.0, thread=lane_thread))
    requests = [
        _source_request(101, 29.0, call=call),
        _source_request(101, 21.0, call=call),
        _source_request(101, 1.0, call='_query_latest'),
        _source_request(101, 2.0, call='_query_main', succeeded=False),
        *lane_reads,
        _source_request(101, 1.0, call='_query_retention'),
        _source_request(101, 1.0, thread='visibility-observer'),
        *ignored_group_reads,
    ]
    log_text = (
        '2026-11-05T00:00:00+00:00 INFO sg_event_daemon.source: '
        'detached group bulk-eager from its live channel: queue-count-exhausted\n'
        if detached_catch_up
        else ''
    )
    result = _daemon_case(
        tmp_path,
        report,
        _DaemonCase(
            eager=[replace(_record(101), delivered_at=31.001)],
            requests=requests,
            log_text=log_text,
        ),
    )
    timeline = _event_timeline(report, 101)
    assert timeline['source_request'] == 1
    assert timeline['source_read_completed_at'] == 21.0
    assert timeline['delivered_at'] == 31.001
    assert timeline['source_to_delivery_secs'] == pytest.approx(10.001)
    assert timeline['daemon_latency_secs'] == pytest.approx(10.001)
    assert timeline['lane_requests'] == ([4] if detached_catch_up else [4, 5])
    if detached_catch_up:
        # The source returned 101 before detach, while its lane catch-up returned it at 30.
        assert requests[1]['event_ids'] == [101]
        assert requests[4]['completed_at'] == 30.0
        assert report.load_metrics['bulk-eager']['returned_delivery_debt'] == {
            'peak_count': 1,
            'outstanding_count': 0,
        }
        detaches = cast('dict[str, object]', report.load_metrics['health']['capacity_detaches'])
        assert detaches['count'] == 1
        assert 'measurement_exclusion' not in report.load_metrics
    assert cast('dict[str, str]', result['criteria'])['eager_daemon_latency'].startswith(
        'failed: '
    )


@pytest.mark.parametrize(
    ('variant', 'criterion'),
    [
        ('source_pause', 'source_pause'),
        ('warning', 'warning'),
        ('duplicate', 'delivery'),
        ('strict_inversion', 'strict_order'),
        ('worker_failure', 'worker_failures'),
    ],
)
def test_daemon_latency_pass_does_not_waive_other_live_failures(
    tmp_path: Path, report: LiveReport, variant: str, criterion: str
) -> None:
    log_text = ''
    strict = None
    expected = None
    eager = None
    requests = None
    first_seen = None
    failures = None
    if variant == 'source_pause':
        log_text = '2026-11-05T00:00:00+00:00 INFO sg_event_daemon.source: source paused:\n'
    elif variant == 'warning':
        log_text = '2026-11-05T00:00:00+00:00 WARNING sg_event_daemon.source: delayed\n'
    elif variant == 'duplicate':
        strict = [replace(_record(101), delivered_at=131.0)] * 2
    elif variant == 'strict_inversion':
        expected = {101, 102}
        first_seen = {101: 2.0, 102: 1.0}
        eager = [replace(_record(identifier), delivered_at=31.0) for identifier in (101, 102)]
        strict = [replace(_record(identifier), delivered_at=131.0) for identifier in (102, 101)]
        requests = [_source_request(101, 21.0)]
    else:
        failures = {'worker': 1}
    result = _daemon_case(
        tmp_path,
        report,
        _DaemonCase(
            expected=expected,
            eager=eager,
            strict=strict,
            first_seen=first_seen,
            requests=requests,
            failures=failures,
            log_text=log_text,
        ),
    )
    criteria = cast('dict[str, str]', result['criteria'])
    assert criteria['eager_daemon_latency'] == 'passed'
    assert criteria[criterion].startswith('failed: ')
    assert result['outcome'] == 'failed'
    assert all('eager_latency' not in line for line in bulk_test.binding_bulk_failures(result))


def test_health_patterns_match_daemon_log_lines(
    tmp_path: Path,
    report: LiveReport,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = SiteConfig(
        name='live',
        server='https://example.invalid',
        default_source='events',
        credentials={'source': CredentialConfig('source', 'poller', SecretValue('test-api-key'))},
        sources={'events': SourceConfig('events', 'source')},
        groups={
            'sync': GroupConfig(
                'sync',
                'events',
                (PluginConfig('sync-probe', tmp_path / 'probe.py', 'source', {}, {}),),
                'source',
            )
        },
        workers={},
    )
    stream = io.StringIO()
    configure_logging(redact_from=(config,), stream=stream)
    extra = {'site': 'live', 'source': 'events'}
    log_paused(extra, 'max_open_ranges', 500, 501)
    log_ids_given_up(extra, ((101, 101),), 300.0)
    log_catchup_skipped({**extra, 'group': 'sync'}, 102, 102, 1.0)
    _report_worker_failure('worker failed with test-api-key')
    stderr = capsys.readouterr().err
    assert 'test-api-key' not in stderr
    assert 'worker failed with <redacted>' in stderr
    stdout_path = tmp_path / 'daemon.stdout.log'
    stderr_path = tmp_path / 'daemon.stderr.log'
    stdout_path.write_text(stream.getvalue(), encoding='utf-8')
    stderr_path.write_text(stderr, encoding='utf-8')
    criteria = load_module.record_bulk_health(
        report,
        (stdout_path, stderr_path),
        lambda text: text,
    )
    health = cast('dict[str, dict[str, object]]', report.load_metrics['health'])
    assert health['fatal_line']['count'] == 1
    assert criteria['fatal_line'] == 'failed: daemon fatal line occurred'
    assert health['give_up']['count'] == 1
    assert health['warning']['count'] == 3
    assert health['source_pause']['count'] == 1


def test_live_capacity_detaches_keep_returned_delivery_debt(
    tmp_path: Path, report: LiveReport
) -> None:
    result = _daemon_case(
        tmp_path,
        report,
        _DaemonCase(
            expected={101, 102},
            late={101},
            first_seen={101: 2.0, 102: 1.0},
            eager=[
                replace(_record(101), delivered_at=3.0),
                replace(_record(102), delivered_at=4.0),
            ],
            strict=[
                replace(_record(101), delivered_at=3.0),
                replace(_record(102), delivered_at=4.0),
            ],
            requests=[
                {**_source_request(101, 1.0), 'event_ids': [101, 102]},
                {**_source_request(101, 2.0), 'event_ids': [101, 102]},
            ],
            log_text=(
                '2026-11-05T00:00:00+00:00 INFO sg_event_daemon.source: '
                'detached group bulk-eager from its live channel: queue-count-exhausted\n'
            ),
        ),
    )
    metric = report.load_metrics['bulk-eager']
    assert metric['returned_delivery_debt'] == {'peak_count': 2, 'outstanding_count': 0}
    assert _event_timeline(report, 101)['source_to_delivery_secs'] == 2.0
    assert _event_timeline(report, 102)['source_to_delivery_secs'] == 3.0
    detaches = cast('dict[str, object]', report.load_metrics['health']['capacity_detaches'])
    assert detaches['count'] == 1
    assert detaches['entries'] == [
        {
            'timestamp': '2026-11-05T00:00:00+00:00',
            'group': 'bulk-eager',
            'reason': 'queue-count-exhausted',
            'source_file': str(tmp_path / 'daemon.log'),
            'source_line': 1,
            'line': (
                '2026-11-05T00:00:00+00:00 INFO sg_event_daemon.source: '
                'detached group bulk-eager from its live channel: queue-count-exhausted'
            ),
        }
    ]
    assert 'measurement_exclusion' not in report.load_metrics
    assert cast('dict[str, str]', result['criteria'])['eager_daemon_latency'] == 'passed'


@pytest.mark.parametrize('count', [1, 1_001])
def test_load_helpers_reject_counts_outside_finite_bound(count: int) -> None:
    with pytest.raises(ValueError, match='between 2 and 1000'):
        partition_load(count)
    with pytest.raises(ValueError, match='between 2 and 1000'):
        load_timeout(count)


def test_load_timeout_is_bounded_by_base_timeout_and_count() -> None:
    assert load_timeout(2) == 122.0
    assert load_timeout(1_000) == 1_120.0


def test_observer_records_final_completion_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    returns = iter((1.0, 1.1, 7.1))
    clock = SimpleNamespace(now=0.0)

    class Client:
        def find(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            if len(calls) == 2:
                observer.stop.set()
            clock.now = next(returns)
            calls.append(clock.now)
            return []

    calls: list[float] = []
    observer = VisibilityObserver(
        cast('LiveSite', SimpleNamespace(source_client=Client())), after_id=100
    )
    monkeypatch.setattr(
        load_module,
        'time',
        SimpleNamespace(monotonic=lambda: clock.now, time=lambda: clock.now),
    )
    observer.run()
    assert observer.completion_intervals == pytest.approx([0.1, 6.0])


@pytest.mark.parametrize(
    ('count', 'expected'),
    [(2, (1, 1)), (3, (2, 1)), (1_000, (500, 500))],
)
def test_partition_load_keeps_both_phases_nonempty(
    count: int,
    expected: tuple[int, int],
) -> None:
    phases = partition_load(count)
    assert (phases.live_count, phases.stopped_count) == expected


def test_perform_updates_uses_sequential_zero_padded_phase_values(
    failing_site: _FailingUpdateSite,
    report: LiveReport,
) -> None:
    progress = perform_updates(failing_site, 7, 2, 'stopped', report)

    assert progress.phase == 'stopped'
    assert progress.requested_updates == 2
    assert progress.successful_updates == 2
    assert progress.phase_values == [
        'acceptance-run-load-000007',
        'acceptance-run-load-000008',
    ]
    assert failing_site.values == progress.phase_values
    assert report.expected_phases == progress.phase_values


def test_process_cpu_secs_reads_root_and_descendants(monkeypatch: pytest.MonkeyPatch) -> None:
    output = '30 20 1-00:00:01\n10 1 1:02.50\n20 10 00:01:02\n40 1 10:00\n'

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert command == ['ps', '-A', '-o', 'pid=,ppid=,time=']
        assert kwargs['check'] is True
        return subprocess.CompletedProcess(command, 0, stdout=output)

    monkeypatch.setattr(load_module.subprocess, 'run', run)
    assert load_module.read_process_cpu_secs(10) == {10: 62.5, 20: 62.0, 30: 86_401.0}


def test_producer_progress_survives_update_failure(
    failing_site: _FailingUpdateSite,
    report: LiveReport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failing_site.fail_on_call = 417
    clock = SimpleNamespace(now=0.0)
    original_update = failing_site.update_description

    def update(value: str) -> None:
        clock.now += 0.25
        original_update(value)

    monkeypatch.setattr(failing_site, 'update_description', update)
    monkeypatch.setattr(load_module, 'time', SimpleNamespace(monotonic=lambda: clock.now))

    with pytest.raises(RuntimeError, match='update failed'):
        perform_updates(failing_site, 0, 1_000, 'live', report)

    progress = report.producer_progress['live']
    assert progress.requested_updates == 1_000
    assert progress.successful_updates == 416
    assert len(progress.phase_values) == 416
    assert progress.elapsed_secs == 104.25
    assert report.requested_updates == 1_000
    assert report.successful_updates == 416


@pytest.mark.parametrize(
    ('provider', 'mode'),
    [('sg-python-api', 'sync+async'), ('shotgun-api3', 'sync')],
)
def test_record_load_derives_rate_without_a_pass_threshold(
    provider: LiveProvider,
    mode: LiveMode,
) -> None:
    report = LiveReport(
        site_hostname='example.invalid',
        project_name='Live Acceptance',
        provider=provider,
        mode=mode,
        namespace='acceptance-run',
        successful_updates=3,
        producer_progress={
            'live': ProducerProgress('live', 2, 2, elapsed_secs=4.0),
            'stopped': ProducerProgress('stopped', 1, 1, elapsed_secs=2.0),
        },
    )
    metrics = {
        'sync': GroupDeliveryMetrics((101, 102, 103), (), ((102, 101),), ()),
    }
    final_status = {
        'groups': ({'cursor': 103, 'group': 'sync', 'status': 'running'},),
        'liveness': True,
        'readiness': True,
        'summary': '<redacted>',
    }

    report.record_load(
        LoadPhases(2, 1),
        metrics,
        final_status=final_status,
        live_convergence_secs=1.25,
        restart_convergence_secs=2.5,
    )

    assert report.achieved_rate == pytest.approx(0.5)
    assert report.timings == {
        'live_producer_secs': 4.0,
        'live_convergence_secs': 1.25,
        'producer_secs': 6.0,
        'restart_convergence_secs': 2.5,
        'stopped_producer_secs': 2.0,
    }
    assert report.status_summary == final_status
    assert report.load_metrics == {
        'phases': {'live_count': 2, 'stopped_count': 1},
        'sync': {
            'delivered_unique_count': 3,
            'delivered_unique_ids': (101, 102, 103),
            'duplicate_count': 0,
            'duplicate_ids': (),
            'inversion_count': 1,
            'inversions': ((102, 101),),
            'missing_count': 0,
            'missing_ids': (),
        },
    }


@pytest.mark.parametrize('group', ['bulk-eager', 'bulk-strict'])
def test_bulk_acceptance_checks_records_written_during_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    report: LiveReport,
    group: str,
) -> None:
    paths = {name: tmp_path / f'{name}.jsonl' for name in ('bulk-eager', 'bulk-strict')}
    _write_bulk_probe_rows(paths)

    class Process:
        log_paths: tuple[Path, ...] = ()
        pid = 10
        _status_calls = 0
        sanitize_text = staticmethod(str)

        def start(self) -> None:
            pass

        def wait_ready(self) -> None:
            pass

        def status(self) -> dict[str, object]:
            self._status_calls += 1
            return {
                'groups': [
                    {'group': 'bulk-eager', 'status': 'running', 'conditions': []},
                    {
                        'group': 'bulk-strict',
                        'status': 'ordering-anomaly',
                        'conditions': ['capacity-exhausted'],
                    },
                ],
                'workers': [
                    {
                        'worker_id': 'worker',
                        'socket_buffer_requested': 100,
                        'socket_send_buffer': 200,
                        'socket_receive_buffer': 300,
                    }
                ],
            }

        def stop(self) -> tuple[BaseException, ...]:
            assert self._status_calls == 1
            _write_duplicate_after_shutdown(tmp_path, paths[group], group)
            return ()

    (tmp_path / 'requests.jsonl').write_text('', encoding='utf-8')
    (tmp_path / 'requests.http.jsonl').write_text('', encoding='utf-8')
    site = SimpleNamespace(bulk_owned_ids=lambda: {41})
    case = SimpleNamespace(
        site=site,
        report=report,
        baseline_event=SimpleNamespace(id=100),
        process=Process(),
        output_paths=paths,
        probe_readers={},
        config_path=tmp_path / 'daemon.toml',
        settings=SimpleNamespace(options=SimpleNamespace(provider='shotgun-api3')),
    )

    def factory(*, bulk: bool = False) -> AbstractContextManager[object]:
        assert bulk
        return nullcontext(case)

    def load(*_args: object, **_kwargs: object) -> load_module.BulkProgress:
        return load_module.BulkProgress(
            successful_updates=1,
            successful_batches=1,
            elapsed_secs=60.0,
            production_ended_at=time.monotonic(),
            production_daemon_cpu={10: 0.0},
            production_harness_cpu=time.process_time(),
        )

    monkeypatch.setattr(bulk_test, 'VisibilityObserver', _BulkAcceptanceObserver)
    monkeypatch.setattr(load_module, 'ResidentSampler', _ProbeResidentSampler)

    def read_cpu(_pid: int) -> dict[int, float]:
        return {10: 0.0}

    monkeypatch.setattr(load_module, 'read_process_cpu_secs', read_cpu)
    monkeypatch.setattr(bulk_test, 'perform_bulk_load', load)
    monkeypatch.setattr(
        bulk_test, 'time', SimpleNamespace(monotonic=time.monotonic, sleep=_skip_sleep)
    )
    with pytest.raises(pytest.fail.Exception, match='duplicate'):
        bulk_test.test_live_bulk_load_delivers_late_events(cast('LiveCaseFactory', factory))
    assert len(report.group_records[group]) == 3
    assert report.load_metrics[group]['duplicate_ids'] == (101,)
    assert report.load_metrics[group]['inversions'] == ((102, 101),)
    assert report.load_metrics['worker_peak_rss_kib'] == {'20': 400}
    assert report.load_metrics['worker_failures'] == {'worker': 0}
    criteria = cast('dict[str, str]', report.load_metrics['acceptance']['criteria'])
    assert criteria['delivery'].startswith('failed: ')
    assert report.load_metrics['control_socket_buffers'] == {
        'worker': {
            'socket_buffer_requested': 100,
            'socket_send_buffer': 200,
            'socket_receive_buffer': 300,
        },
    }
    assert report.status_summary['groups'] == [
        {'group': 'bulk-eager', 'status': 'running', 'conditions': []},
        {
            'group': 'bulk-strict',
            'status': 'ordering-anomaly',
            'conditions': ['capacity-exhausted'],
        },
    ]


@pytest.mark.parametrize(('updates', 'batches'), [(0, 1), (1, 0)])
def test_bulk_acceptance_requires_both_producer_categories(updates: int, batches: int) -> None:
    progress = load_module.BulkProgress(
        successful_updates=updates, successful_batches=batches, elapsed_secs=60.0
    )
    with pytest.raises(AssertionError, match='producer categories'):
        load_module.assert_bulk_progress(progress, duration_secs=60.0)


@pytest.mark.parametrize(
    'logger',
    [
        'sg_event_daemon',
        'sg_event_daemon.source',
        'sg_event_daemon.group',
        'sg_event_daemon.supervisor',
    ],
)
def test_any_daemon_warning_fails_the_live_load(
    tmp_path: Path, report: LiveReport, logger: str
) -> None:
    path = tmp_path / 'daemon.log'
    path.write_text(f'2026-10-31T00:00:00+00:00 WARNING {logger}: warning\n', encoding='utf-8')
    criteria = load_module.record_bulk_health(report, (path,), str)
    assert criteria['warning'] == 'failed: daemon warning occurred'
    path.write_text(f'2026-10-31T00:00:00+00:00 INFO {logger}: normal\n', encoding='utf-8')
    criteria = load_module.record_bulk_health(report, (path,), str)
    assert criteria['warning'] == 'passed'


@pytest.mark.parametrize('fail_early', [False, True])
def test_bulk_elapsed_is_the_duration_at_the_deadline(
    report: LiveReport,
    monkeypatch: pytest.MonkeyPatch,
    fail_early: bool,
) -> None:
    clock = SimpleNamespace(now=2**40 - 10.1)

    class Stop:
        def set(self) -> None:
            pass

        def wait(self, seconds: float) -> None:
            clock.now += 5.0 if fail_early else seconds

    class Thread:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def start(self) -> None:
            pass

        def join(self) -> None:
            pass

    def create(_count: int) -> tuple[int, ...]:
        return ()

    site = SimpleNamespace(create_shot_batch=create, namespace='test', bulk_shot_ids=[])
    monkeypatch.setattr(
        load_module, 'threading', SimpleNamespace(Event=Stop, Lock=threading.Lock, Thread=Thread)
    )
    monkeypatch.setattr(load_module, 'time', SimpleNamespace(monotonic=lambda: clock.now))
    progress = perform_bulk_load(cast('LiveSite', site), duration_secs=60.0, report=report)
    if fail_early:
        assert progress.elapsed_secs == pytest.approx(5.0)
    else:
        assert progress.elapsed_secs == 60.0


@pytest.mark.parametrize('busy_percent', [20.0, 95.0])
def test_host_busy_does_not_excuse_bulk_latency_miss(
    tmp_path: Path, report: LiveReport, busy_percent: float
) -> None:
    cast('dict[str, object]', report.load_metrics)['host_samples'] = [
        {
            'monotonic': 1.0,
            'wall_time': 1.0,
            'busy_percent': busy_percent,
            'load_average_1m': 1.0,
            'outside_processes': [],
            'error': None,
        }
    ]
    result = _criteria_case(tmp_path, report, intervals=[0.1, 6.0], warning=False)
    assert cast('dict[str, str]', result['criteria'])['eager_latency'].startswith('failed: ')
    assert cast('dict[str, str]', result['criteria'])['eager_daemon_latency'].startswith(
        'failed: '
    )
    assert result['outcome'] == 'failed'


def test_recorder_attributes_source_queries_across_the_helper_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serviced: list[bool] = []
    service_seen = threading.Event()

    def service() -> None:
        serviced.append(True)
        service_seen.set()

    class Shotgun(source_tests.ScriptedSG):
        def __init__(self) -> None:
            super().__init__([[source_tests.make_event(102)], [source_tests.make_event(101)]])
            self.attempts = 0

        def _http_request(self) -> tuple[tuple[int, str], dict[str, str], str]:
            self.attempts += 1
            return ((502 if self.attempts % 2 else 200, 'status'), {}, '')

        def find(
            self,
            entity_type: str,
            filters: object,
            fields: list[str] | None = None,
            **options: Unpack[source_tests._FindOptions],
        ) -> list[dict[str, object]]:
            service_seen.wait()
            self._http_request()
            self._http_request()
            return super().find(entity_type, filters, fields, **options)

    path = tmp_path / 'requests.jsonl'
    monkeypatch.setenv('SG_EVENT_TEST_REQUEST_LOG', str(path))
    real_import = recorder.import_module

    def module(name: str) -> SimpleNamespace | ModuleType:
        return SimpleNamespace(Shotgun=Shotgun) if name == 'shotgun_api3' else real_import(name)

    monkeypatch.setattr(recorder, 'import_module', module)
    original_stack = recorder.extract_stack

    def stack() -> list[FrameSummary]:
        return [
            *original_stack(),
            FrameSummary('/unrelated/_source.py', 1, '_query_latest'),
        ]

    monkeypatch.setattr(recorder, 'extract_stack', stack)
    wall = iter(float(value) for value in range(10, 30))
    monotonic = iter(float(value) for value in range(100, 130))
    monkeypatch.setattr(
        recorder,
        'time',
        SimpleNamespace(time=lambda: next(wall), monotonic=lambda: next(monotonic)),
    )
    monkeypatch.setattr(
        source_module._ServicingSyncSG, 'find', source_module._ServicingSyncSG.find
    )
    recorder.install()
    inner = Shotgun()
    provider = source_tests.Provider(
        name='shotgun-api3',
        capabilities=source_tests.ProviderCapabilities(sync=True, async_=False),
        sync_factory=source_tests.SyncFactory(inner),
        async_factory=None,
    )
    caller = threading.current_thread()
    monkeypatch.setattr(caller, 'name', 'sg-event-daemon:source:events')
    with source_tests.open_source(tmp_path / 'source.db') as state:
        state.initialize(100, incarnation='incarnation-1')
        with source_tests.make_poller(state, provider, query_service=service) as poller:
            assert poller._poll_once() == 1
            poller._recheck_missing_once()
    requests = load_module.read_request_timeline(path, 'shotgun-api3')
    attempts = load_module.read_http_attempts(path, 'shotgun-api3')
    source_reads, _lanes = load_module.index_response_reads(requests)
    assert set(source_reads) == {101, 102}
    assert [cast('list[str]', row['source_calls'])[-1] for row in requests] == [
        '_query_main',
        '_recheck_missing',
    ]
    assert [row['status'] for row in attempts] == [502, 200, 502, 200]
    for index, query in enumerate(requests):
        assert query['thread'] == caller.name
        for attempt in attempts[index * 2 : index * 2 + 2]:
            assert attempt['thread'] == caller.name
            assert (
                cast('float', query['started_at'])
                <= cast('float', attempt['started_at'])
                <= cast('float', query['completed_at'])
            )
    assert inner.attempts == 4
    assert serviced, 'queries did not cross the servicing helper boundary'


def test_request_log_attributes_closeout_to_groups(tmp_path: Path) -> None:

    path = tmp_path / 'requests.jsonl'
    path.write_text(
        '\n'.join(
            json.dumps(row)
            for row in (
                {
                    'thread': 'sg-event-daemon:lane:bulk-strict',
                    'between': True,
                    'started_at': 1.0,
                    'duration_secs': 0.2,
                },
                {
                    'thread': 'sg-event-daemon:source:events',
                    'between': True,
                    'started_at': 2.0,
                    'duration_secs': 0.3,
                },
                {
                    'thread': 'sg-event-daemon:lane:bulk-strict',
                    'between': False,
                    'started_at': 3.0,
                    'duration_secs': 0.4,
                },
            )
        ),
        encoding='utf-8',
    )
    assert read_closeout_requests(path, 'shotgun-api3', ['bulk-strict']) == {
        'bulk-strict': {'measured': True, 'count': 1, 'durations_secs': [0.2]},
    }
    assert read_closeout_requests(path, 'sg-python-api', ['bulk-strict']) == {
        'bulk-strict': {'measured': False},
    }


def test_request_recorder_auto_installs_only_as_startup_module(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Shotgun:
        def find(self, *_args: object, **_kwargs: object) -> list[object]:
            return []

    fake_provider = ModuleType('shotgun_api3')
    fake_provider.__dict__['Shotgun'] = Shotgun
    monkeypatch.setitem(sys.modules, 'shotgun_api3', fake_provider)
    monkeypatch.setenv('SG_EVENT_TEST_REQUEST_LOG', str(tmp_path / 'requests.jsonl'))
    monkeypatch.setattr(
        source_module._ServicingSyncSG, 'find', source_module._ServicingSyncSG.find
    )
    filename = recorder.__file__
    assert filename is not None
    original_find = Shotgun.find

    runpy.run_path(filename, run_name='tests.live.fixtures.request_recorder.sitecustomize')
    assert Shotgun.find is original_find

    runpy.run_path(filename, run_name='sitecustomize')
    assert Shotgun.find is not original_find


def test_request_recorder_preserves_sparse_response_ids_and_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Shotgun:
        def find(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            return [{'id': 101}, {'id': 103}]

    real_import = recorder.import_module

    def module(name: str) -> SimpleNamespace | ModuleType:
        return SimpleNamespace(Shotgun=Shotgun) if name == 'shotgun_api3' else real_import(name)

    def stack() -> list[FrameSummary]:
        source_file = source_module.__file__
        assert source_file is not None
        return [
            FrameSummary('/unrelated/_source.py', 1, '_query_main'),
            FrameSummary(source_file, 2, 'catch_up'),
            FrameSummary(source_file, 3, '_query_range'),
            FrameSummary('/unrelated/_source.py', 4, '_query_latest'),
        ]

    wall = iter((10.0, 12.0))
    monotonic = iter((100.0, 102.0))
    path = tmp_path / 'requests.jsonl'
    monkeypatch.setenv('SG_EVENT_TEST_REQUEST_LOG', str(path))
    monkeypatch.setattr(recorder, 'import_module', module)
    monkeypatch.setattr(recorder, 'extract_stack', stack, raising=False)
    monkeypatch.setattr(
        source_module._ServicingSyncSG, 'find', source_module._ServicingSyncSG.find
    )
    monkeypatch.setattr(
        recorder,
        'time',
        SimpleNamespace(time=lambda: next(wall), monotonic=lambda: next(monotonic)),
    )
    recorder.install()
    assert Shotgun().find('EventLogEntry', []) == [{'id': 101}, {'id': 103}]
    row = json.loads(path.read_text(encoding='utf-8'))
    assert row['row_count'] == 2
    assert row['min_id'] == 101
    assert row['max_id'] == 103
    assert row['event_ids'] == [101, 103]
    assert row['completed_at'] == 12.0
    assert row['succeeded'] is True
    assert row['source_calls'] == ['catch_up', '_query_range']


def test_request_recorder_records_each_http_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Shotgun:
        def _http_request(self) -> tuple[tuple[int, str], dict[str, str], str]:
            self._attempts += 1
            if self._attempts == 1:
                return ((502, 'bad gateway'), {'Retry-After': '2', 'Other': 'omit'}, '')
            return ((200, 'ok'), {}, '')

        def find(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            self._http_request()
            self._http_request()
            return [{'id': 101}]

        def __init__(self) -> None:
            self._attempts = 0

    path = tmp_path / 'requests.jsonl'
    monkeypatch.setenv('SG_EVENT_TEST_REQUEST_LOG', str(path))

    def module(name: str) -> SimpleNamespace:
        if name == 'shotgun_api3':
            return SimpleNamespace(Shotgun=Shotgun)
        return SimpleNamespace(__file__=source_module.__file__)

    monkeypatch.setattr(recorder, 'import_module', module)
    recorder.install()
    assert Shotgun().find('EventLogEntry', []) == [{'id': 101}]
    attempts = load_module.read_http_attempts(path, 'shotgun-api3')
    assert [row['status'] for row in attempts] == [502, 200]
    assert attempts[0]['headers'] == {'retry-after': '2'}
    assert len(path.read_text(encoding='utf-8').splitlines()) == 1


def test_request_recorder_records_failed_between_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Shotgun:
        def find(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError('provider failed')

    path = tmp_path / 'requests.jsonl'
    monkeypatch.setenv('SG_EVENT_TEST_REQUEST_LOG', str(path))

    def module(name: str) -> SimpleNamespace:
        if name == 'shotgun_api3':
            return SimpleNamespace(Shotgun=Shotgun)
        return SimpleNamespace(__file__=source_module.__file__)

    monkeypatch.setattr(recorder, 'import_module', module)
    wall = iter((10.0, 11.0))
    monotonic = iter((100.0, 101.0))
    monkeypatch.setattr(
        recorder,
        'time',
        SimpleNamespace(time=lambda: next(wall), monotonic=lambda: next(monotonic)),
    )
    recorder.install()
    with pytest.raises(RuntimeError, match='provider failed'):
        Shotgun().find('EventLogEntry', _id_runs_filter(((1, 2),)))
    row = json.loads(path.read_text(encoding='utf-8'))
    assert row['between'] is True
    assert row['thread'] == threading.current_thread().name
    assert row['duration_secs'] >= 0
    assert row['started_at'] > 0
    assert row['completed_at'] >= row['started_at']
    assert row['row_count'] is None
    assert row['event_ids'] == []
    assert row['succeeded'] is False


def test_resident_sampler_keeps_descendant_peaks(monkeypatch: pytest.MonkeyPatch) -> None:
    outputs = iter(
        (
            '30 20 300\n10 1 900\n20 10 200\n40 1 800\n',
            '20 10 150\n30 20 350\n50 10 100\n',
            '50 10 90\n',
        )
    )
    calls = 0
    sampler = load_module.ResidentSampler(10, interval_secs=0.001)

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        assert command == ['ps', '-A', '-o', 'pid=,ppid=,rss=']
        assert kwargs['check'] is True
        calls += 1
        output = next(outputs)
        if calls == 3:
            sampler.stop.set()
        return subprocess.CompletedProcess(command, 0, stdout=output)

    monkeypatch.setattr(load_module.subprocess, 'run', run)
    sampler.start()
    sampler.thread.join()
    sampler.finish()
    assert not sampler.thread.is_alive()
    assert calls == 3
    assert sampler.peaks() == {20: 200, 30: 350, 50: 100}
    copied = sampler.peaks()
    copied.clear()
    assert sampler.peaks() == {20: 200, 30: 350, 50: 100}


def test_returned_delivery_debt_counts_early_equal_and_missing_callbacks(
    report: LiveReport,
) -> None:
    observer = VisibilityObserver(cast('LiveSite', None), after_id=100)
    observer.first_seen = {101: 3.0, 102: 2.0, 103: 1.0, 104: 0.0}
    record_bulk_deliveries(
        observer,
        {101, 102, 103},
        {
            'bulk-eager': [
                replace(_record(101), delivered_at=0.0),
                replace(_record(102), delivered_at=1.0),
            ]
        },
        report,
        requests=[
            {**_source_request(101, 1.0), 'event_ids': [101, 102, 103]},
            {**_source_request(101, 2.0), 'event_ids': [101, 102, 103]},
        ],
    )
    metric = report.load_metrics['bulk-eager']
    assert metric['returned_delivery_debt'] == {'peak_count': 1, 'outstanding_count': 1}
    assert len(cast('dict[str, object]', metric['event_timeline'])) == 3
    assert _event_timeline(report, 103)['delivered_at'] is None


def test_slow_site_flag_excuses_no_latency_miss(tmp_path: Path, report: LiveReport) -> None:
    slow = _criteria_case(tmp_path, report, intervals=[0.1, 6.0], warning=False)
    assert slow['site_slow'] is True
    assert slow['observer_max_completion_interval_secs'] == 6.0
    assert cast('dict[str, str]', slow['criteria'])['eager_latency'].startswith('failed: ')
    assert cast('dict[str, str]', slow['criteria'])['eager_daemon_latency'].startswith('failed: ')
    assert slow['outcome'] == 'failed'
    fast = _criteria_case(tmp_path, report, intervals=[0.1, 1.7], warning=False)
    assert fast['site_slow'] is False


def test_visibility_classification_queries_bounded_observed_id_sets() -> None:
    batches: list[list[int]] = []

    class Client:
        def find(
            self, _entity: str, filters: object, _fields: object, **_kwargs: object
        ) -> list[dict[str, object]]:
            conditions = cast('list[list[object]]', filters)
            assert len(conditions) == 1
            assert conditions[0][:2] == ['id', 'in']
            identifiers = cast('list[int]', conditions[0][2])
            batches.append(identifiers)
            return [
                {'id': identifier, 'entity': None, 'project': None} for identifier in identifiers
            ]

    site = cast('LiveSite', SimpleNamespace(source_client=Client(), project={'id': 7}))
    observer = VisibilityObserver(site, after_id=100)
    observer.first_seen = dict.fromkeys(range(101, 602), 1.0)
    observer.select_events({41})
    assert [len(batch) for batch in batches] == [500, 1]
    assert {identifier for batch in batches for identifier in batch} == set(observer.first_seen)
    assert observer.events == {}


def test_visibility_observer_advances_only_over_whole_site_contiguous_ids() -> None:
    calls: list[tuple[object, object]] = []
    pages = iter(([101, 103], [103, 104], [102, 103, 104], [105]))

    class Client:
        def find(
            self, _entity: str, filters: object, fields: object, **_kwargs: object
        ) -> list[dict[str, object]]:
            calls.append((filters, fields))
            identifiers = next(pages)
            if len(calls) == 4:
                observer.stop.set()
            return [{'id': identifier} for identifier in identifiers]

    site = cast('LiveSite', SimpleNamespace(source_client=Client(), project={'id': 7}))
    observer = VisibilityObserver(site, after_id=100)
    observer.run()
    assert calls == [([['id', 'greater_than', floor]], ['id']) for floor in (100, 101, 101, 104)]
    assert observer.floor == 105
    assert observer.late_ids() == {102}


def test_visibility_observer_retains_late_ids_and_joins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Client:
        def __init__(self) -> None:
            self._calls = 0

        def find(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            self._calls += 1
            if self._calls == 1:
                return [{'id': 102}]
            observer.stop.set()
            return [{'id': 101}, {'id': 102}]

    client = Client()
    site = cast('LiveSite', SimpleNamespace(source_client=client, project={'id': 7}))
    observer = VisibilityObserver(site, after_id=100)
    waits: list[float] = []
    wall = iter((100.0, 101.0))

    def wait(seconds: float) -> bool:
        waits.append(seconds)
        return observer.stop.is_set()

    monkeypatch.setattr(
        load_module,
        'time',
        SimpleNamespace(monotonic=lambda: 100.0, time=lambda: next(wall)),
    )
    monkeypatch.setattr(observer.stop, 'wait', wait)
    observer.start()
    observer.thread.join()
    observer.finish()
    assert not observer.thread.is_alive()
    assert observer.late_ids() == {101}
    assert observer.cadence == [0.0]
    assert waits == [0.1, 0.1]
    assert observer.failures == []


def test_visibility_observer_skips_permanent_gap_after_bulk_window(
    monkeypatch: pytest.MonkeyPatch,
    report: LiveReport,
) -> None:
    calls: list[int] = []
    clock = SimpleNamespace(now=0.0)
    pages = iter(([101, 103], [103, 104], [103, 104], [104, 105]))
    times = iter((0.0, 29.9, 30.1, 30.2))

    class Client:
        def find(
            self, _entity: str, filters: object, _fields: object, **_kwargs: object
        ) -> list[dict[str, object]]:
            calls.append(cast('int', cast('list[list[object]]', filters)[0][2]))
            clock.now = next(times)
            if len(calls) == 4:
                observer.stop.set()
            return [{'id': identifier} for identifier in next(pages)]

    site = cast('LiveSite', SimpleNamespace(source_client=Client(), project={'id': 7}))
    observer = VisibilityObserver(site, after_id=100)
    monkeypatch.setattr(
        load_module,
        'time',
        SimpleNamespace(monotonic=lambda: clock.now, time=lambda: clock.now),
    )
    observer.run()
    assert calls == [100, 101, 101, 104]
    assert observer.floor == 105
    assert observer.skipped_ranges == [(102, 102)]
    assert 102 not in observer.first_seen
    record_bulk_visibility(observer, (), report)
    visibility = report.load_metrics['visibility']
    assert visibility['skipped_id_ranges'] == [(102, 102)]
    assert visibility['whole_site_observed_count'] == 4


def test_visibility_selection_rejects_unclassified_observed_ids() -> None:
    class Client:
        def find(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            return [
                {'id': identifier, 'entity': {'type': 'Shot', 'id': 41}, 'project': {'id': 7}}
                for identifier in (101, 103)
            ]

    site = cast('LiveSite', SimpleNamespace(source_client=Client(), project={'id': 7}))
    observer = VisibilityObserver(site, after_id=100)
    observer.first_seen = {101: 2.0, 102: 1.0, 103: 1.0}
    with pytest.raises(AssertionError, match=r'classification.*102'):
        observer.select_events({41})
    assert observer.events == {}


def test_visibility_selection_uses_run_entities_without_changing_first_seen() -> None:
    calls: list[tuple[object, object]] = []

    class Client:
        def find(
            self, _entity: str, filters: object, fields: object, **_kwargs: object
        ) -> list[dict[str, object]]:
            calls.append((filters, fields))
            return [
                {'id': 101, 'entity': {'type': 'Shot', 'id': 41}, 'project': {'id': 7}},
                {'id': 102, 'entity': {'type': 'Shot', 'id': 41}, 'project': {'id': 8}},
                {'id': 103, 'entity': {'type': 'Asset', 'id': 41}, 'project': {'id': 7}},
                {'id': 104, 'entity': {'type': 'Shot', 'id': 41}, 'project': {'id': 7}},
            ]

    site = cast('LiveSite', SimpleNamespace(source_client=Client(), project={'id': 7}))
    observer = VisibilityObserver(site, after_id=100)
    observer.first_seen = {101: 1.0, 102: 1.0, 103: 2.0}
    observer.select_events({41})
    assert observer.expected_ids({41}) == {101}
    assert observer.first_seen == {101: 1.0, 102: 1.0, 103: 2.0}
    assert len(calls) == 1
    assert calls[0][1] == list(EVENT_FIELDS)


def test_worker_failures_fail_the_live_load(tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / 'supervisor.db')) as connection, connection:
        connection.execute('CREATE TABLE worker_breaker (worker_id TEXT, failure_count INTEGER)')
        connection.executemany(
            'INSERT INTO worker_breaker VALUES (?, ?)', [('healthy', 0), ('failed', 2)]
        )
    failures = load_module.read_worker_failures(tmp_path)
    assert failures == {'healthy': 0, 'failed': 2}
