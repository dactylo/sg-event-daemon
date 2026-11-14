from __future__ import annotations

import copy
import importlib
import json
import logging
import math
import os
import runpy
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import threading
from contextlib import AbstractContextManager, closing, contextmanager, nullcontext
from dataclasses import dataclass, replace
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Any, Unpack, cast

import pytest

from sg_event_daemon._group_errors import InvalidTransitionError, RevisionConflictError
from sg_event_daemon._group_state import GroupState
from sg_event_daemon._group_types import BeginCallbackStep, CallbackDoneStep, StartEventStep
from sg_event_daemon._health import ConditionFlags
from sg_event_daemon._platform import ensure_secure_state_dir
from sg_event_daemon._plugins import CallbackIdentity, CallbackManifest
from sg_event_daemon._sqlite import DatabaseError
from tests import _worker_processes as support
from tests import test_supervisor as supervisor_tests
from tests._storage_support import open_test_group

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from sg_event_daemon._group_types import PreInvocationStep
    from tests._stall_policy import StallDetectors

_CELLS = (
    'catchup-2-empty',
    'catchup-13-empty',
    'second-process',
    'matching-1',
    'matching-2',
    'matching-13',
    'offered-1',
    'offered-2',
    'offered-13',
    'profile-13',
    'large-transactions',
    'outside-reader-13',
)
_METRICS = {
    'source_events_per_second',
    'matched_callbacks_per_second',
    'callbacks',
    'commits',
    'delivery_commits_per_callback',
    'reuse',
    'reset_seconds',
    'peak_wal_in_use_bytes',
    'peak_wal_file_bytes',
    'acquisition_seconds',
    'protected_write_seconds',
    'peak_open_descriptors',
    'worker_failures',
    'bound_crossings',
    'expired_attempts',
}


def _assert_offered_load_window(
    result: dict[str, Any], results_dir: Path, window: dict[str, float]
) -> None:
    assert not result['metrics']['worker_failures'], result
    assert json.loads((results_dir / 'offered-1.json').read_text()) == result
    assert result['load_start'] == window['start']
    assert result['load_end'] == window['end']
    assert result['load_end'] - result['load_start'] == pytest.approx(60 * 0.02)
    raw = json.loads((results_dir / 'offered-1.raw.json').read_text())
    group = next(record for record in raw if record['role'] == 'group0')
    commits = sorted(row['at'] for row in group['progress'])
    assert len(commits) == 86
    assert sum(row['count'] for row in group['progress']) == 86
    assert len(group['callbacks']) == 86
    assert result['metrics']['commits']['pre-invocation'] == 86
    assert result['metrics']['commits']['done'] == 9
    assert result['metrics']['commits']['obligation-completion'] == 1
    # The fake-clock admission test covers the load-end schedule. This run
    # checks the complete durable delivery after its bounded admission.
    assert result['groups']['group0']['delivered_including_drain'] == 86
    assert group['progress'][-1]['cursor'] == 186
    completion_rows = [
        row
        for row in group['transactions']
        if row['committed'] and row['kind'] in {'pre-invocation', 'done'}
    ]
    assert all(at in {row['at'] for row in completion_rows} for at in commits)


def _assert_outside_reader_warnings(result: dict[str, Any], results_dir: Path) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    result = copy.deepcopy(result)
    assert not result['metrics']['worker_failures']
    recovery = result['recovery']
    assert recovery['original_spawn_instance'] != recovery['replacement_spawn_instance']
    assert recovery['ready_spawn_instance'] == recovery['replacement_spawn_instance']
    assert recovery['ready_worker_id'] == recovery['replacement_worker_id']
    assert recovery['replacement_started_at'] <= recovery['ready_at']
    assert recovery['replacement_started_at'] <= recovery['startup_complete_at']
    assert recovery['startup_spawn_instance'] == recovery['replacement_spawn_instance']
    assert recovery['startup_worker_id'] == recovery['replacement_worker_id']
    assert (
        recovery['catchup_started_at'] <= recovery['ready_at'] <= recovery['catchup_finished_at']
    )
    assert recovery['catchup_started_at'] <= recovery['catchup_finished_at']
    assert recovery['wal_bytes_before_owner_open'] >= result['peak_pinned_wal_bytes']
    assert recovery['wal_bytes_after_recovery'] == recovery['wal_bytes_before_first_write'] == 0
    assert recovery['recovery_finished_at'] <= recovery['first_write_entered_at']
    assert recovery['first_write_entered_at'] <= recovery['first_write_completed_at']
    assert set(recovery['groups']) == {f'group{index}' for index in range(13)}
    assert {row['group'] for row in recovery['catchup_health']['groups']} == set(
        recovery['groups']
    )
    assert all(
        row['join_phase'] == 'attached' and row['pending_catchup_epoch_count'] == 0
        for row in recovery['catchup_health']['groups']
    )
    assert all(
        row['cursor'] == row['frontier'] and row['pending_ranges'] == 0
        for row in recovery['groups'].values()
    )
    assert recovery['all_groups_caught_up']
    assert result['criteria']['delivery-paused']
    assert result['criteria']['one-transaction-per-writer-overshoot']
    raw = json.loads((results_dir / 'outside-reader-13.raw.json').read_text())
    crossed = {
        bound
        for record in raw
        for attempt in record['checkpoints']
        for bound in attempt['crossed']
    }
    assert crossed == {'wal-growth', 'incomplete-duration'}
    actual = dict(result['warnings'])
    assert {'checkpoint:WAL growth', 'checkpoint:incomplete duration'} <= actual.keys()
    assert any(category.startswith('writer:') for category in actual)
    assert result['criteria']['warning-per-transition']
    for category in actual:
        result['warnings'] = dict(actual)
        del result['warnings'][category]
        harness._assess(result)
        assert not result['criteria']['warning-per-transition'], category
        result['warnings'] = dict(actual)
        result['warnings'][category] += 1
        harness._assess(result)
        assert not result['criteria']['warning-per-transition'], category
    result['warnings'] = actual
    harness._assess(result)
    assert result['criteria']['warning-per-transition']


def _assert_replacement_success(recovery: dict[str, Any], frozen_catchup_at: float) -> None:
    assert recovery['ready_at'] == 40.0
    assert recovery['catchup_finished_at'] == max(40.0, frozen_catchup_at)
    assert recovery['all_groups_caught_up']
    assert json.loads(json.dumps(recovery)) == recovery


def _delay_probe_startup(
    monkeypatch: pytest.MonkeyPatch, probe: support.SupervisorProbe, minimum_at: float
) -> None:
    original = probe.startup_complete

    def delayed(worker_id: str, spawn_instance: str, *, now: float) -> bool:
        return now >= minimum_at and original(worker_id, spawn_instance, now=now)

    monkeypatch.setattr(probe, 'startup_complete', delayed)


def _install_bounded_cell_workload(monkeypatch: pytest.MonkeyPatch, harness: ModuleType) -> None:
    original_admit = harness._admit_load
    original_workload = harness._run_workload
    original_outside = harness._outside_reader

    def bounded_admission(source: object, options: dict[str, Any], runtime: dict[str, Any]) -> int:
        rate = options.get('offered_rate')
        if rate is None:
            return original_admit(source, options, runtime)
        end = runtime['end']
        runtime['end'] = math.inf
        try:
            return original_admit(
                source,
                {
                    **options,
                    'offered_rate': None,
                    'event_limit': math.floor(rate * options['duration']),
                },
                runtime,
            )
        finally:
            runtime['end'] = end

    def bounded_workload(path: Path, **options: object) -> dict[str, Any]:
        if options.get('event_limit') == 1 and options['duration'] == 0.1:
            options['duration'] = math.inf
        return original_workload(path, **{**options, 'stall_timeout_secs': 3600.0})

    def bounded_outside(
        path: Path, duration: float, scale: float, *, observations: object | None = None
    ) -> dict[str, Any]:
        return original_outside(
            path,
            duration,
            scale,
            observations=observations,
            stall_timeout_secs=3600.0,
            wait_for_pause=True,
        )

    monkeypatch.setattr(harness, '_admit_load', bounded_admission)
    monkeypatch.setattr(harness, '_run_workload', bounded_workload)
    monkeypatch.setattr(harness, '_outside_reader', bounded_outside)


def _matching_result(rate: float = 95.0) -> dict[str, Any]:
    return {
        'cell': 'matching-13',
        'qualification': True,
        'duration_scale': 1.0,
        'device_class': 'test',
        'filesystem': 'test',
        'commit': 'test',
        'metrics': {
            'source_events_per_second': rate,
            'worker_failures': [],
            'protected_write_seconds': {'max': 0.01},
            'acquisition_seconds': {'max': 0.01},
            'bound_crossings': 0,
            'expired_attempts': 0,
            'peak_wal_file_bytes': 1024,
            'peak_wal_in_use_bytes': 1024,
            'reuse': {'reused': 10, 'busy': 0},
            'callbacks': 100,
            'delivery_commits_per_callback': 2.0,
        },
    }


def _floor_missed_matching_result(floors: str) -> dict[str, Any]:
    harness = importlib.import_module('benchmarks.storage_load')
    runs: list[dict[str, Any]] = []
    for rate in (70.0, 80.0, 85.0):
        run = _matching_result(rate)
        run['floors'] = floors
        harness._assess(run)
        runs.append(run)
    matched = dict(runs[0])
    matched.update(
        attempt_count=3,
        runs=runs,
        run_rates=[70.0, 80.0, 85.0],
        median_source_events_per_second=80.0,
    )
    harness._assess(matched)
    return matched


def _noop_manifest() -> CallbackManifest:
    return CallbackManifest(
        (
            CallbackIdentity(
                ordinal=0,
                plugin_id='noop',
                callback_id='noop',
                execution='sync',
                filter_digest='all',
                config_digest='empty',
                secret_refs=(),
                credential_name='source',
                credential_fingerprint='offline',
            ),
        )
    )


def _observe_load_window(monkeypatch: pytest.MonkeyPatch) -> dict[str, float]:
    harness = importlib.import_module('benchmarks.storage_load')
    start_load = harness._start_load
    window: dict[str, float] = {}

    def observe_start(path: Path, options: dict[str, Any], runtime: dict[str, Any]) -> None:
        start_load(path, options, runtime)
        window.update(start=runtime['start'], end=runtime['end'])

    monkeypatch.setattr(harness, '_start_load', observe_start)
    return window


@contextmanager
def _observed_group(
    path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[tuple[GroupState, dict[str, Any]]]:
    harness = importlib.import_module('benchmarks.storage_load')
    metrics = harness._metrics('group0')
    commit = harness._Connection.commit
    timestamps = iter((10.0, 20.0))

    def observed_commit(connection: sqlite3.Connection) -> None:
        commit(connection)
        if harness._LOCAL.metrics is not None:
            harness._LOCAL.committed_wal['at'] = next(timestamps)

    monkeypatch.setattr(harness._Connection, 'commit', observed_commit)
    harness._LOCAL.path, harness._LOCAL.metrics = path, None
    writes = harness._writes(
        path,
        threading.Event(),
        harness.ConditionFlags(),
        logging.getLogger('test'),
        'group0',
        limit=harness.WAL_IN_USE_LIMIT_BYTES,
    )
    with (
        harness._measurement_environment(0.02),
        harness._source(path, writes, create=True) as source,
    ):
        source.initialize(100, incarnation='incarnation-1')
        with harness._group(path, 'group0', writes) as state:
            harness._LOCAL.metrics = metrics
            yield state, metrics


@dataclass
class _OutsideReaderSampleStubs:
    root: Path
    probe: SimpleNamespace
    reader: SimpleNamespace
    clock: SimpleNamespace
    complete: Callable[..., set[str]]
    replacement: Callable[..., None]


def _outside_result() -> dict[str, Any]:
    recovery: dict[str, Any] = {
        'original_spawn_instance': 'original',
        'replacement_spawn_instance': 'replacement',
        'replacement_worker_id': 'site',
        'startup_spawn_instance': 'replacement',
        'startup_worker_id': 'site',
        'replacement_started_at': 100.0,
        'startup_complete_at': 129.999,
        'errors': [],
        'wal_bytes_before_first_write': 0,
        'wal_bytes_before_owner_open': 300000000,
        'wal_bytes_after_recovery': 0,
        'recovery_started_at': 100.1,
        'recovery_finished_at': 100.2,
        'first_actual_write_entered_at': 100.25,
        'first_write_entered_at': 100.3,
        'first_write_completed_at': 100.4,
        'first_write_seconds': 100.4 - 100.0,
        'all_groups_caught_up': True,
        'ready_at': 140.0,
        'ready_spawn_instance': 'replacement',
        'ready_worker_id': 'site',
        'catchup_started_at': 129.999,
        'catchup_finished_at': 140.0,
    }
    names = [f'group{index}' for index in range(13)]
    recovery.update(
        expected_groups=names,
        retained_frontier=100,
        groups={name: {'cursor': 100, 'frontier': 100, 'pending_ranges': 0} for name in names},
        catchup_health={
            'worker': {
                'worker_id': 'site',
                'spawn_instance': 'replacement',
                'monitor_alive': True,
                'watchdog_failure': None,
                'heartbeat_at': 139.9,
            },
            'groups': [
                {
                    'group': name,
                    'worker_instance': 'replacement',
                    'join_phase': 'attached',
                    'pending_catchup_epoch_count': 0,
                    'outstanding_late_obligation_count': 0,
                    'outstanding_late_id_count': 0,
                }
                for name in names
            ],
        },
    )
    result: dict[str, Any] = {
        'cell': 'outside-reader-13',
        'duration_seconds': 60,
        'reader_pin_seconds': 60,
        'metrics': {
            'worker_failures': [],
            'protected_write_seconds': {'max': 0.1},
            'acquisition_seconds': {'max': 0.1},
        },
        'crossing_wal_bytes': 300000000,
        'peak_pinned_wal_bytes': 300000000,
        'stall_conditions': ['checkpoint-stalled', 'write-lock-stalled'],
        'delivery_paused': True,
        'commits_after_crossing_by_writer': {'site': 1},
        'warnings': {'writer:source': 1},
        'warning_expectations': {'writer:source': 1},
        'recovery': recovery,
    }
    return result


def _stub_outside_reader_sample_dependencies(
    monkeypatch: pytest.MonkeyPatch, harness: ModuleType, stubs: _OutsideReaderSampleStubs
) -> None:
    def config(*_args: object) -> Path:
        return stubs.root / 'config.toml'

    def supervisor(*_args: object) -> AbstractContextManager[SimpleNamespace]:
        return nullcontext(stubs.probe)

    def connect(*_args: object, **_kwargs: object) -> SimpleNamespace:
        return stubs.reader

    def pin(*_args: object) -> float:
        return 0.0

    def zero(*_args: object) -> int:
        return 0

    def records(*_args: object) -> list[dict[str, Any]]:
        return []

    def retained(_path: Path) -> dict[str, dict[str, int]]:
        return {'group0': {'frontier': 100}}

    monkeypatch.setattr(harness, 'time', SimpleNamespace(monotonic=lambda: stubs.clock.now))
    monkeypatch.setattr(harness, '_outside_config', config)
    monkeypatch.setattr(harness, 'running_supervisor', supervisor)
    monkeypatch.setattr(harness, '_CONNECT', connect)
    monkeypatch.setattr(harness, '_pin_outside_reader', pin)
    monkeypatch.setattr(harness, 'wal_file_bytes', zero)
    monkeypatch.setattr(harness, '_open_descriptor_count', zero)
    monkeypatch.setattr(harness, '_retain_paused_peak', stubs.complete)
    monkeypatch.setattr(harness, '_production_records', records)
    monkeypatch.setattr(harness, '_caught_up_groups', retained)
    monkeypatch.setattr(harness, '_replacement_observations', stubs.replacement)


@pytest.mark.parametrize('stall_timeout_secs', [None, 3600.0])
@pytest.mark.parametrize('wait_for_pause', [False, True])
def test_outside_reader_samples_until_pin_duration_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stall_timeout_secs: float | None,
    *,
    wait_for_pause: bool,
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    clock = SimpleNamespace(now=0.0)
    sampled: list[bool] = []
    timeouts: list[float | None] = []
    barrier_reads: list[float] = []
    signals: list[int] = []
    reap_budgets: list[float] = []
    replacement_budgets: list[float | None] = []
    schedules = iter(((), ((0.0, False), (0.99, False), (1.0, True))))

    def execute(_statement: str) -> None:
        pass

    reader = SimpleNamespace(close=lambda: None, execute=execute)

    def send_signal(signum: int) -> None:
        signals.append(signum)

    def reap(*, timeout: float) -> int:
        reap_budgets.append(timeout)
        return -9

    original = SimpleNamespace(
        process=SimpleNamespace(poll=lambda: None, send_signal=send_signal, wait=reap),
        spawn_instance='original',
    )

    class SamplingCompleteError(Exception):
        pass

    def wait_for(predicate: Callable[[], bool], timeout: float | None) -> None:
        timeouts.append(timeout)
        for at, expected in next(schedules):
            clock.now = at
            observed = predicate()
            sampled.append(observed)
            assert observed is expected

    probe = SimpleNamespace(wait_for=wait_for, spawns=[original], terminations=[], supervisor=None)

    def complete(*_args: object) -> set[str]:
        return set()

    def replacement(
        _probe: object,
        _original: object,
        _path: Path,
        _recovery: dict[str, Any],
        *,
        stall_timeout_secs: float | None,
    ) -> None:
        replacement_budgets.append(stall_timeout_secs)
        raise SamplingCompleteError

    _stub_outside_reader_sample_dependencies(
        monkeypatch,
        harness,
        _OutsideReaderSampleStubs(tmp_path, probe, reader, clock, complete, replacement),
    )

    def pause_ready(*_args: object) -> bool:
        return clock.now == 1.0

    def retain_pause(*_args: object) -> None:
        barrier_reads.append(clock.now)

    monkeypatch.setattr(harness, '_outside_pause_ready', pause_ready)
    monkeypatch.setattr(harness, '_retain_outside_pause', retain_pause)
    with pytest.raises(SamplingCompleteError):
        harness._outside_reader(
            tmp_path / 'source.db',
            {False: 1.0, True: 0.01}[wait_for_pause],
            0.02,
            stall_timeout_secs=stall_timeout_secs,
            wait_for_pause=wait_for_pause,
        )
    # In CI, even the sample at 99 times the configured duration must wait for
    # evidence. Qualification still stops at its fixed duration.
    timeouts_by_options: dict[tuple[float | None, bool], list[float | None]] = {
        (None, False): [30, 6.0],
        (None, True): [30, None],
        (3600.0, False): [3600.0, 3600.0],
        (3600.0, True): [3600.0, None],
    }
    assert timeouts == timeouts_by_options[stall_timeout_secs, wait_for_pause]
    assert barrier_reads == ([1.0, 1.0] if wait_for_pause else [])
    assert sampled == [False, False, True]
    assert signals == [signal.SIGKILL]
    assert reap_budgets == [5 if stall_timeout_secs is None else stall_timeout_secs]
    assert replacement_budgets == [stall_timeout_secs]


def test_cell_execution_failure_preserves_a_failed_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')

    def fail(_path: Path, **_options: object) -> object:
        raise RuntimeError('injected component setup failure')

    monkeypatch.setattr(harness, '_run_workload', fail)
    result = harness._run_cell(
        'matching-1',
        work_dir=tmp_path / 'work',
        results_dir=tmp_path / 'results',
        duration_scale=0.02,
        device_class='test',
        filesystem='test',
        commit='test',
    )
    assert not result['passed']
    assert 'no-worker-failure' in result['failed_criteria']
    assert 'injected component setup failure' in result['metrics']['worker_failures'][0]
    assert json.loads((tmp_path / 'results' / 'matching-1.json').read_text()) == result
    raw = json.loads((tmp_path / 'results' / 'matching-1.raw.json').read_text())
    assert 'injected component setup failure' in raw[0]['errors'][0]


def test_completion_wrappers_allow_inactive_measurement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    manifest = _noop_manifest()
    with _observed_group(tmp_path / 'source.db', monkeypatch) as (state, metrics):
        harness._LOCAL.metrics = None
        started = state.commit_pre_invocation(
            (StartEventStep(101, manifest, None), BeginCallbackStep(0)), expected_revision=0
        )
        combined = state.commit_pre_invocation(
            (CallbackDoneStep(0), StartEventStep(102, manifest, None), BeginCallbackStep(0)),
            expected_revision=started.snapshot.revision,
        )
        done = state.callback_done(0, expected_revision=combined.snapshot.revision)
        assert done.cursor == 102
        assert metrics['transactions'] == metrics['progress'] == []


def test_documented_benchmark_load_command_imports_project_helpers(tmp_path: Path) -> None:
    make = shutil.which('make')
    assert make is not None
    work_dir = tmp_path / 'work'
    results_dir = tmp_path / 'results'
    work_dir.mkdir()
    results_dir.mkdir()
    environment = {
        key: value
        for key, value in os.environ.items()
        if key != 'PYTHONPATH' and not key.startswith(('SG_', 'SHOTGUN_', 'AWS_'))
    }
    # `-o env-check` skips the `make setup` stamp check: CI builds its environment without it.
    command = [
        make,
        '-o',
        'env-check',
        'benchmark-load',
        f'DEV_PYTHON={sys.executable}',
        'DEVICE_CLASS=offline-test',
        'FILESYSTEM=test',
        f'WORK_DIR={work_dir}',
        f'RESULTS_DIR={results_dir}',
        'BENCHMARK_FLAGS=--help',
    ]
    result = subprocess.run(
        command,
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'usage:' in result.stdout
    assert '--cell' in result.stdout
    assert not any(work_dir.iterdir())
    assert not any(results_dir.iterdir())


def test_every_cell_runs_short_and_reports_its_criteria(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    _install_bounded_cell_workload(monkeypatch, harness)
    window = _observe_load_window(monkeypatch)
    results = tmp_path / 'results'
    for cell in _CELLS:
        cell_results = results
        if cell.startswith('offered-'):
            # Short matching runs still report real criteria; their reuse sample
            # is too small to qualify an offered dependency. Exercise offered
            # execution with a separately identified synthetic passing input.
            cell_results = tmp_path / 'offered-results'
            cell_results.mkdir(exist_ok=True)
            dependency = _matching_result(90.0)
            dependency.update(
                cell=f'matching-{cell.split("-")[-1]}',
                qualification=False,
                duration_scale=0.02,
                median_source_events_per_second=90.0,
                passed=True,
            )
            (cell_results / f'{dependency["cell"]}.json').write_text(json.dumps(dependency))
        result = harness._run_cell(
            cell,
            work_dir=tmp_path / 'work',
            results_dir=cell_results,
            duration_scale=1 / 16 if cell == 'outside-reader-13' else 0.02,
            device_class='test',
            filesystem='test',
            commit='test',
        )
        saved = json.loads((cell_results / f'{cell}.json').read_text())
        assert saved == result
        assert result['cell'] == cell
        assert result['qualification'] is False
        assert result['metrics'].keys() >= _METRICS
        assert not result['metrics']['worker_failures'], result
        assert result['passed'] == (not result['failed_criteria'])
        assert result['criteria']
        assert all(value >= 1 for value in result['metrics']['peak_open_descriptors'].values())
        assert {'p99', 'max'} <= result['metrics']['acquisition_seconds'].keys()
        assert {'p99', 'max'} <= result['metrics']['protected_write_seconds'].keys()
        if cell.startswith('offered-'):
            matching = json.loads(
                (cell_results / f'matching-{cell.split("-")[-1]}.json').read_text()
            )
            assert result['offered_source_events_per_second'] == pytest.approx(
                matching['median_source_events_per_second'] * 0.8
            )
            assert result['matching_dependency'] == matching['cell']
            assert len(result['groups']) == int(cell.split('-')[-1])
        if cell == 'profile-13':
            assert sum(result['profile_shares'].values()) == pytest.approx(1.0)
            assert result['profile_shares']['sqlite'] > 0
        if cell == 'large-transactions':
            assert result['populations'] == {'retired_ids': 10_000, 'deleted_late_ids': 1_000}
            assert len(result['large_transactions']) == 2
            assert result['metrics']['commits']['source'] == 1
            assert result['metrics']['commits']['obligation-completion'] == 1
        if cell == 'outside-reader-13':
            assert result['metrics']['peak_open_descriptors'].keys() >= {
                'site',
                'restarted-site',
                'outside-reader',
            }
            assert result['crossing_wal_bytes'] > result['growth_bound_bytes']
            assert result['delivery_paused']
            assert set(result['stall_conditions']) == {'write-lock-stalled', 'checkpoint-stalled'}
            assert result['recovery']['wal_bytes_before_first_write'] == 0
            assert result['recovery']['all_groups_caught_up']
            assert result['warnings']['checkpoint:WAL growth'] == 1
            assert all(count == 1 for count in result['warnings'].values())

        if cell == 'offered-1':
            _assert_offered_load_window(result, cell_results, window)
        if cell == 'outside-reader-13':
            _assert_outside_reader_warnings(result, cell_results)


def test_large_owner_failure_restores_prior_observation_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    logger = harness.logging.getLogger('storage-load.large')
    previous_handlers = list(logger.handlers)

    def prior_connect(*_args: object, **_options: object) -> None:
        return

    def skip_initial(*_args: object, **_options: object) -> dict[str, list[str]]:
        return {'worker_failures': []}

    def unused_writes(*_args: object, **_options: object) -> object:
        return object()

    monkeypatch.setattr(harness.sqlite3, 'connect', prior_connect)
    monkeypatch.setattr(harness, '_run_workload', skip_initial)
    monkeypatch.setattr(harness, '_writes', unused_writes)

    def fail_source(*_args: object, **_options: object) -> None:
        raise RuntimeError('injected large owner-open failure')

    monkeypatch.setattr(harness, '_source', fail_source)
    try:
        with pytest.raises(RuntimeError, match='injected large owner-open failure'):
            harness._large_transactions(tmp_path / 'source.db', 0.02)
        assert harness.sqlite3.connect is prior_connect
        assert logger.handlers == previous_handlers
    finally:
        logger.handlers[:] = previous_handlers


def test_load_creates_private_root_under_group_writable_umask(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    work_dir = tmp_path / 'work'
    validated = False

    def stop_after_validation(path: Path, **_options: object) -> None:
        nonlocal validated
        ensure_secure_state_dir(path.parent)
        validated = True
        raise RuntimeError('validated before workload')

    monkeypatch.setattr(harness, '_run_workload', stop_after_validation)
    old_umask = os.umask(0o002)
    try:
        result = harness._run_attempt(
            'matching-1',
            work_dir=work_dir,
            results_dir=tmp_path / 'results',
            duration_scale=0.02,
            device_class='test',
            filesystem='test',
            commit='test',
        )
    finally:
        os.umask(old_umask)
    assert validated
    assert result['execution_failed']
    assert 'validated before workload' in result['metrics']['worker_failures'][0]
    assert stat.S_IMODE(work_dir.stat().st_mode) == 0o700


def test_load_rejects_existing_unsafe_root_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    work_dir = tmp_path / 'work'
    work_dir.mkdir()
    work_dir.chmod(0o775)
    sentinel = work_dir / 'sentinel'
    contents = b'leave existing root untouched\x00'
    sentinel.write_bytes(contents)
    validated = False

    def stop_after_validation(path: Path, **_options: object) -> None:
        nonlocal validated
        ensure_secure_state_dir(path.parent)
        validated = True
        raise AssertionError('database workload reached')

    monkeypatch.setattr(harness, '_run_workload', stop_after_validation)
    old_umask = os.umask(0o002)
    try:
        result = harness._run_attempt(
            'matching-1',
            work_dir=work_dir,
            results_dir=tmp_path / 'results',
            duration_scale=0.02,
            device_class='test',
            filesystem='test',
            commit='test',
        )
    finally:
        os.umask(old_umask)
    assert result['execution_failed']
    assert 'parent directory is writable' in result['metrics']['worker_failures'][0]
    assert not validated
    assert stat.S_IMODE(work_dir.stat().st_mode) == 0o775
    assert sentinel.read_bytes() == contents
    assert not (work_dir / 'matching-1' / 'source.db').exists()


def test_matching_nonfloor_miss_stops_repeat_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    calls: list[str] = []

    def attempt(_cell: str, **_options: object) -> dict[str, Any]:
        calls.append(_cell)
        result = _matching_result(80.0)
        result['metrics']['peak_wal_file_bytes'] = 70 * 1024 * 1024
        harness._assess(result)
        return result

    monkeypatch.setattr(harness, '_run_attempt', attempt)
    result = harness._run_cell(
        'matching-13',
        work_dir=tmp_path / 'work',
        results_dir=tmp_path / 'results',
        duration_scale=1.0,
        device_class='test',
        filesystem='test',
        commit='test',
    )
    assert calls == ['matching-13']
    assert result['attempt_count'] == 1
    assert 'peak-wal-file' in result['failed_criteria']


def test_offered_summary_counts_only_delivery_within_one_page_of_drain() -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    record = harness._metrics('group0')
    end = 60.0
    drain_end = end + 100 / 72
    committed_at = [end - 1, end, drain_end - 0.001, drain_end, drain_end + 0.001]
    record['callbacks'] = [at - 0.1 for at in committed_at]
    record['transactions'] = [
        {
            'at': at,
            'kind': 'pre-invocation' if index % 2 else 'done',
            'committed': True,
            'acquisition': index + 0.1,
            'protected': index + 0.2,
            'wal_file': 1024,
            'wal_in_use': 1024,
        }
        for index, at in enumerate(committed_at)
    ]
    record['progress'] = [
        {'at': at, 'count': 1, 'cursor': index + 101} for index, at in enumerate(committed_at)
    ]
    result = harness._summarize([record], 0.0, end, 1, drain_end=drain_end)
    assert result['groups']['group0'] == {
        'delivered_at_load_end': 2,
        'delivered_at_drain_end': 4,
        'delivered_including_drain': 5,
    }
    assert result['commits']['done'] == 3
    assert result['commits']['pre-invocation'] == 2
    assert result['acquisition_seconds'] == harness._percentiles(
        [index + 0.1 for index in range(5)]
    )
    assert result['protected_write_seconds'] == harness._percentiles(
        [index + 0.2 for index in range(5)]
    )


@pytest.mark.parametrize('sampling_error', [False, True])
def test_outside_commit_samples_wal_before_releasing_writer_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    *,
    sampling_error: bool,
) -> None:
    path = tmp_path / 'source.db'
    records: list[dict[str, object]] = []
    sampled_while_locked: list[bool] = []
    activity: list[str] = []
    conditions = ConditionFlags()

    def observe(kind: str) -> None:
        if kind == 'pause':
            assert conditions.names() == ('write-lock-stalled',)
            assert any('paused: the WAL file' in message for message in caplog.messages)
        activity.append(kind)

    def record(_kind: str, _role: str, **values: object) -> None:
        records.append(values)

    with (
        closing(sqlite3.connect(path, isolation_level=None)) as connection,
        closing(
            support.SourceWriteLock(
                tmp_path / 'source.db.write-lock',
                database_path=path,
                timeout_secs=10,
                wait_phase_grace_secs=10,
                clock=lambda: 12.0,
                wal_bytes=lambda: 0,
            )
        ) as lock,
    ):
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('CREATE TABLE events (id INTEGER)')
        entry = lock._entry
        assert entry is not None

        def wal_bytes(observed: Path) -> int:
            assert observed == path
            assert not connection.in_transaction
            held = entry.lock.locked() and entry.holder == threading.get_ident()
            sampled_while_locked.append(held)
            if sampling_error:
                raise OSError('WAL sample failed')
            # Model another writer growing the WAL as soon as the lock is
            # released; no elapsed time or competing-thread schedule is needed.
            return 80 if held else 160

        monkeypatch.setattr(support._sqlite, 'wal_file_bytes', wal_bytes)
        support._install_outside_writes(monkeypatch, record, observe)
        expected = (
            pytest.raises(OSError, match='WAL sample failed') if sampling_error else nullcontext()
        )
        with expected, lock.transaction(connection):
            assert activity == ['write']
            connection.execute('INSERT INTO events VALUES (1)')
        assert not entry.lock.locked()
        assert entry.holder is None
        if not sampling_error:
            monkeypatch.setattr(lock, '_wal_bytes', lambda: lock.wal_limit_bytes + 1)
            context = support._write_lock.WriteContext(
                lock,
                conditions=conditions,
                logger=logging.getLogger('outside-pause-order'),
                policy='one-attempt',
            )
            with (
                pytest.raises(support._write_lock.WriteLockTimeoutError, match='growth'),
                context.transaction(connection),
            ):
                pytest.fail('growth-paused transaction entered its body')
            assert activity == ['write', 'pause']
    if not sampling_error:
        assert len(records) == 2
        assert records[1]['level'] == 'pause'
        assert records[0]['committed'] is True
        assert records[0]['at'] == 12.0
        assert records[0]['wal_file'] == 80
    assert sampled_while_locked[0] is True


def test_outside_ci_pause_verdict_does_not_accept_quiet_records() -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    summary = harness._outside_summary(
        [harness._metrics('site')],
        [{'at': 20.0, 'bytes': 101, 'fds': 3}],
        {'errors': []},
        harness._metrics('restarted-site'),
        {
            'pinned': 0.0,
            'duration': 20.0,
            'pin_seconds': 20.0,
            'limit': 100,
            'killed': -9,
            'conditions': {'checkpoint-stalled', 'write-lock-stalled'},
            'pause_evidence': [],
        },
    )
    assert summary['delivery_paused'] is False


@pytest.mark.parametrize('second_commit', [False, True])
def test_outside_overshoot_rejects_second_post_crossing_commit(*, second_commit: bool) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    site = harness._metrics('site')
    writer = harness._metrics('group0')
    observations = [
        (site, 10.0, 120, True),
        (writer, 9.0, 80, True),
        (writer, 11.0, 140, True),
    ]
    if second_commit:
        observations.append((writer, 12.0, 160, True))
    for record, at, wal_file, committed in observations:
        record['transactions'].append(
            {
                'at': at,
                'wal_file': wal_file,
                'wal_in_use': None,
                'committed': committed,
                'kind': 'other',
                'acquisition': 0.0,
                'protected': 0.0,
            }
        )
    summary = harness._outside_summary(
        [site, writer],
        [{'at': 20.0, 'bytes': 160 if second_commit else 140, 'fds': 3}],
        {'errors': []},
        harness._metrics('restarted-site'),
        {
            'pinned': 0.0,
            'duration': 20.0,
            'pin_seconds': 20.0,
            'limit': 100,
            'killed': -9,
            'conditions': set(),
        },
    )
    counts = summary['commits_after_crossing_by_writer']
    assert counts == {'site': 0, 'group0': 2 if second_commit else 1}
    result = _outside_result()
    result['commits_after_crossing_by_writer'] = counts
    harness._assess(result)
    assert result['criteria']['one-transaction-per-writer-overshoot'] is not second_commit


def test_outside_reader_provider_never_advances_after_freeze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = support.OutsideReaderProvider(tmp_path, frontier_cap=500)
    handles = [support.DeterministicHandle(0, 1000, None) for _ in range(2)]
    (tmp_path / 'events-ready').touch()
    monkeypatch.setattr(threading.current_thread(), 'name', 'sg-event-daemon:source:admin')
    provider.find(handles[0], 'EventLogEntry', [['id', 'greater_than', 100]], ['id'])
    (tmp_path / 'retained-frontier').write_text('200')
    for role, handle in zip(('source:admin', 'lane:group0'), handles, strict=True):
        monkeypatch.setattr(threading.current_thread(), 'name', f'sg-event-daemon:{role}')
        for low in (200, 500, 900):
            assert (
                provider.find(handle, 'EventLogEntry', [['id', 'greater_than', low]], ['id']) == []
            )
        latest = provider.find(
            handle,
            'EventLogEntry',
            [],
            ['id'],
            limit=1,
            order=[{'field_name': 'id', 'direction': 'desc'}],
        )
        assert latest[0]['id'] == 200
    assert provider.frontier == 200


@pytest.mark.parametrize(('pin_seconds', 'expected'), [(59.9, False), (60.0, True)])
def test_reader_pin_criterion_uses_full_configured_window(
    pin_seconds: float, expected: bool
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    result = _outside_result()
    result['reader_pin_seconds'] = pin_seconds

    harness._assess(result)

    assert result['criteria']['reader-pinned-for-duration'] is expected


@pytest.mark.parametrize('existing', ['work-database', 'result', 'raw-result'])
def test_load_preserves_existing_result_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, existing: str
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    work_dir = tmp_path / 'work'
    results_dir = tmp_path / 'results'
    work_dir.mkdir()
    results_dir.mkdir()
    if existing == 'work-database':
        cell_dir = work_dir / 'matching-1'
        cell_dir.mkdir()
        target = cell_dir / 'source.db'
    else:
        suffix = '.raw.json' if existing == 'raw-result' else '.json'
        target = results_dir / f'matching-1{suffix}'
    contents = b'existing observations\x00'
    target.write_bytes(contents)

    def fail_if_workload_runs(_path: Path, **_options: object) -> None:
        raise AssertionError('workload reached')

    monkeypatch.setattr(harness, '_run_workload', fail_if_workload_runs)
    old_umask = os.umask(0o002)
    try:
        with pytest.raises(FileExistsError):
            harness._run_attempt(
                'matching-1',
                work_dir=work_dir,
                results_dir=results_dir,
                duration_scale=0.02,
                device_class='test',
                filesystem='test',
                commit='test',
            )
    finally:
        os.umask(old_umask)
    assert target.read_bytes() == contents


@pytest.mark.parametrize(
    'operation',
    ['standalone', 'combined', 'successor-rejected', 'invalid-done', 'stale', 'rolled-back'],
)
def test_durable_completion_observation_uses_only_its_committed_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    manifest = _noop_manifest()
    with _observed_group(tmp_path / 'source.db', monkeypatch) as (state, metrics):
        started = state.commit_pre_invocation(
            (StartEventStep(101, manifest, None), BeginCallbackStep(0)), expected_revision=0
        )
        assert started.rejection is None
        assert metrics['progress'] == []
        metrics['callbacks'].append(11.0)
        before = state.snapshot()
        steps = (
            CallbackDoneStep(1 if operation == 'invalid-done' else 0),
            StartEventStep(102, manifest, None),
            BeginCallbackStep(1 if operation == 'successor-rejected' else 0),
        )
        original = GroupState._pre_invocation_step

        def reject_start(owner: GroupState, step: PreInvocationStep, revision: int) -> int:
            if isinstance(step, StartEventStep):
                raise sqlite3.OperationalError('rollback after completion')
            return original(owner, step, revision)

        if operation == 'rolled-back':
            monkeypatch.setattr(GroupState, '_pre_invocation_step', reject_start)
        if operation == 'standalone':
            state.callback_done(0, expected_revision=before.revision)
        elif operation in {'stale', 'rolled-back'}:
            error_type = RevisionConflictError if operation == 'stale' else DatabaseError
            with pytest.raises(error_type):
                state.commit_pre_invocation(
                    steps, expected_revision=before.revision - int(operation == 'stale')
                )
        else:
            result = state.commit_pre_invocation(steps, expected_revision=before.revision)
            assert isinstance(result.rejection, InvalidTransitionError) is (
                operation in {'successor-rejected', 'invalid-done'}
            )
        completed = operation in {'standalone', 'combined', 'successor-rejected'}
        assert metrics['progress'] == (
            [{'at': 20.0, 'count': 1, 'cursor': 101}] if completed else []
        )
        assert len(metrics['transactions']) == 2
        assert [row['kind'] for row in metrics['transactions']] == [
            'pre-invocation',
            'done' if operation == 'standalone' else 'pre-invocation',
        ]
        assert [row['committed'] for row in metrics['transactions']] == [True, completed]
        assert metrics['transactions'][0]['at'] == 10.0
        if completed:
            assert metrics['progress'][0]['at'] == metrics['transactions'][1]['at']
            assert state.snapshot().cursor == 101
        else:
            assert state.snapshot() == before


def test_outside_restart_evidence_caches_completed_first_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    replacement = SimpleNamespace(started_at=100.0, log_path=tmp_path / 'replacement.log')
    reads: dict[str, int] = {}

    def rows(path: Path) -> list[dict[str, Any]]:
        reads[path.suffix] = reads.get(path.suffix, 0) + 1
        if path.suffix == '.wal-recovery':
            return [{'started': 100.1, 'finished': 100.2, 'before': 2_000_000, 'after': 0}]
        assert path.suffix == '.wal-writes'
        return [
            {'phase': 'begin', 'at': 100.3, 'started': 100.25, 'bytes': 0},
            {'phase': 'commit', 'at': 100.4, 'started': 100.25},
        ]

    monkeypatch.setattr(harness, '_json_rows', rows)
    recovery: dict[str, Any] = {}
    for _ in range(3):
        harness._retain_restart_evidence(replacement, recovery)
    assert recovery['first_write_completed_at'] == 100.4
    assert reads == {'.wal-recovery': 1, '.wal-writes': 1}


def test_peak_wal_criterion_reads_the_file_size() -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    result = _matching_result()
    result['metrics']['peak_wal_file_bytes'] = 64 * 1024 * 1024 + 1
    harness._assess(result)
    assert result['failed_criteria'] == ['peak-wal-file']
    assert 'peak-wal-in-use' not in result['criteria']
    result['metrics']['peak_wal_file_bytes'] = 1024
    result['metrics']['peak_wal_in_use_bytes'] = 64 * 1024 * 1024 + 1
    harness._assess(result)
    assert result['passed']


@pytest.mark.parametrize('rate', [72.0, 230.0, 250.0])
def test_offered_cells_admit_full_pages_on_schedule(
    monkeypatch: pytest.MonkeyPatch, rate: float
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    clock = [10.0]
    pages: list[tuple[float, int, int]] = []
    admissions: list[float] = []

    class Stop:
        def is_set(self) -> bool:
            return False

        def wait(self, seconds: float) -> None:
            clock[0] += seconds

    def publish(_inbox: object, page: tuple[int, ...], _stop: object) -> None:
        pages.append((clock[0], page[0], page[1]))

    def admit(*_args: object) -> None:
        admissions.append(clock[0])

    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(harness, '_admit_page', admit)
    monkeypatch.setattr(harness, '_put_page', publish)
    duration = 60 * 0.02
    count = harness._admit_load(
        object(),
        {'groups': 1, 'matching': True, 'duration': duration, 'offered_rate': rate},
        {'start': 10.0, 'end': 10.0 + duration, 'stop': Stop(), 'inboxes': [object()]},
    )
    due = int(rate * duration)
    assert count == due
    assert [high - low + 1 for _, low, high in pages] == [100] * (due // 100) + (
        [due % 100] if due % 100 else []
    )
    for at, (_, low, high) in zip(admissions, pages, strict=True):
        assert at >= 10.0 + (high - 100) / rate
        if high - low + 1 < 100:
            assert at == pytest.approx(10.0 + duration)


@pytest.mark.parametrize(
    ('rate', 'delivered', 'drained', 'expected'),
    [
        (1301.44, 77980, 78086, (78086, 78000, True, True)),
        (1301.44, 77800, 78086, (78086, 78000, True, True)),
        (1301.44, 77799, 78086, (78086, 78000, False, True)),
        (72.0, 4225, 4320, (4320, 4300, True, True)),
        (72.0, 4225, 4270, (4320, 4300, True, False)),
        (50.0, 2700, 3000, (3000, 2900, True, True)),
        (50.0, 2699, 3000, (3000, 2900, False, True)),
        (50.01, 2800, 3000, (3000, 3000, True, True)),
        (50.01, 2799, 3000, (3000, 3000, False, True)),
        (1.67, 0, 100, (100, 100, True, True)),
        (1.4, 0, 84, (84, 0, True, True)),
        (1.4, 42, 84, (84, 0, True, True)),
        (1.4, 84, 84, (84, 0, True, True)),
        (72.0, 0, 0, (4320, 4300, False, False)),
    ],
)
def test_offered_backlog_counts_released_pages(
    rate: float,
    delivered: int,
    drained: int,
    expected: tuple[int, int, bool, bool],
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    due, released, backlog_passes, delivery_passes = expected
    result = _matching_result()
    result.update(
        cell='offered-1',
        duration_seconds=60.0,
        offered_source_events_per_second=rate,
        admitted=drained,
        groups={'group0': {'delivered_at_load_end': delivered, 'delivered_at_drain_end': drained}},
    )
    harness._assess(result)
    assert result['offered'] == due
    row = result['groups']['group0']
    assert row['backlog_at_load_end'] == max(0, released - delivered)
    assert row['released_at_load_end'] == released
    assert row['undelivered_at_load_end'] == due - delivered
    assert 'group0-one-page-backlog' not in result['criteria']
    assert result['criteria']['group0-two-page-backlog'] is backlog_passes
    assert result['criteria']['group0-99-percent-delivery'] is delivery_passes


@pytest.mark.parametrize('rate', [250.0, 200.0, 84.0])
def test_offered_delivery_lag_uses_each_groups_durable_progress(rate: float) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    start, end = 10.0, 11.0
    records: list[dict[str, Any]] = []
    for index, delay in enumerate([0.01, 0.02]):
        record = harness._metrics(f'group{index}')
        for event in range(int(rate)):
            release = min(start + (event // 100 + 1) * 100 / rate, end)
            record['callbacks'].append(release)
            record['transactions'].append(
                {
                    'at': release + delay,
                    'kind': 'pre-invocation' if event % 2 else 'done',
                    'committed': True,
                    'acquisition': 0.0,
                    'protected': 0.0,
                    'wal_file': 1024,
                    'wal_in_use': 1024,
                }
            )
            record['progress'].append({'at': release + delay, 'count': 1, 'cursor': event + 101})
        # These operations have no durable event completion to report.
        first = record['transactions'][0]
        record['transactions'].extend(
            {**first, 'kind': kind, 'committed': committed, 'at': start}
            for kind, committed in [
                ('obligation-completion', True),
                ('pre-invocation', True),
                ('done', False),
            ]
        )
        record['transactions'].reverse()
        record['progress'].reverse()
        records.append(record)
    result = harness._summarize(records, start, end, 2, offered_rate=rate)
    for index, delay in enumerate([0.01, 0.02]):
        lag = result['groups'][f'group{index}']['delivery_lag_seconds']
        assert lag == pytest.approx({'median': delay, 'p99': delay, 'max': delay})


@pytest.mark.parametrize('floors', ['gate', 'report'])
def test_offered_rate_reads_the_matching_median_whatever_its_floor(
    tmp_path: Path, floors: str
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    matched = _floor_missed_matching_result(floors)
    assert matched['criteria']['source-events-rate'] is False
    assert matched['floor_missed'] is True
    assert matched['passed'] is (floors == 'report')
    assert matched['failed_criteria'] == (['source-events-rate'] if floors == 'gate' else [])
    path = tmp_path / 'matching-13.json'
    path.write_text(json.dumps(matched))
    saved = path.read_bytes()
    result = _matching_result()
    assert harness._offered_rate(result, tmp_path, 13) == 64.0
    assert result['matching_dependency'] == 'matching-13'
    assert result['offered_source_events_per_second'] == 64.0
    assert path.read_bytes() == saved
    assert json.loads(saved) == matched


@pytest.mark.parametrize('floors', ['gate', 'report'])
@pytest.mark.parametrize(
    'unusable',
    [
        'nonfloor_failure',
        'nonfloor_criterion',
        'execution_failed',
        'incomplete',
        'incomplete_run',
        'missing_run',
        'missing_rates',
        'missing_criteria',
        'missing_run_rate',
        'missing_median',
        'nonfinite_median',
        'zero_median',
        'negative_median',
        'wrong_cell',
        'wrong_provenance',
    ],
)
def test_offered_rate_rejects_unusable_matching_dependencies(
    tmp_path: Path, floors: str, unusable: str
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    matched = _floor_missed_matching_result(floors)
    if unusable == 'nonfloor_failure':
        matched['runs'][1]['metrics']['worker_failures'] = ['injected failure']
        harness._assess(matched['runs'][1])
        harness._assess(matched)
    elif unusable == 'nonfloor_criterion':
        matched['criteria']['no-worker-failure'] = False
    elif unusable in {'execution_failed', 'incomplete'}:
        matched[unusable] = True
    elif unusable == 'incomplete_run':
        matched['runs'][1]['incomplete'] = True
    elif unusable == 'missing_run':
        matched['runs'].pop()
    elif unusable in {'missing_rates', 'missing_criteria', 'missing_median'}:
        matched.pop(
            {
                'missing_rates': 'run_rates',
                'missing_criteria': 'criteria',
                'missing_median': 'median_source_events_per_second',
            }[unusable]
        )
    elif unusable == 'missing_run_rate':
        matched['runs'][1]['metrics'].pop('source_events_per_second')
    elif unusable in {'nonfinite_median', 'zero_median', 'negative_median'}:
        matched['median_source_events_per_second'] = {
            'nonfinite_median': float('nan'),
            'zero_median': 0.0,
            'negative_median': -1.0,
        }[unusable]
    else:
        field, value = {
            'wrong_cell': ('cell', 'matching-2'),
            'wrong_provenance': ('commit', 'other'),
        }[unusable]
        matched[field] = value
    path = tmp_path / 'matching-13.json'
    path.write_text(json.dumps(matched))
    saved = path.read_bytes()
    result = _matching_result()
    with pytest.raises(RuntimeError, match='matching dependency'):
        harness._offered_rate(result, tmp_path, 13)
    assert 'matching_dependency' not in result
    assert 'offered_source_events_per_second' not in result
    assert path.read_bytes() == saved


@pytest.mark.parametrize('floors', ['gate', 'report'])
@pytest.mark.parametrize(
    'missing',
    ['aggregate_criterion', 'run_criterion', 'run_criteria', 'empty_run_criteria'],
)
def test_offered_rate_rejects_missing_matching_criterion_evidence(
    tmp_path: Path, floors: str, missing: str
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    matched = _floor_missed_matching_result(floors)
    if missing == 'aggregate_criterion':
        matched['criteria'].pop('no-worker-failure')
    elif missing == 'run_criterion':
        matched['runs'][1]['criteria'].pop('no-worker-failure')
    elif missing == 'run_criteria':
        matched['runs'][1].pop('criteria')
    else:
        matched['runs'][1]['criteria'] = {}
    path = tmp_path / 'matching-13.json'
    path.write_text(json.dumps(matched))
    saved = path.read_bytes()
    result = _matching_result()
    with pytest.raises(RuntimeError, match='matching dependency'):
        harness._offered_rate(result, tmp_path, 13)
    assert 'matching_dependency' not in result
    assert 'offered_source_events_per_second' not in result
    assert path.read_bytes() == saved


@pytest.mark.parametrize('rates', [(108.0,), (80.0, 95.0, 100.0), (70.0, 80.0, 85.0)])
@pytest.mark.parametrize('floors', ['gate', 'report'])
@pytest.mark.parametrize('worker_failure', [False, True])
def test_rate_floor_uses_the_median_of_three_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rates: tuple[float, ...],
    floors: str,
    worker_failure: bool,
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    runs: list[dict[str, Any]] = []

    def attempt(_cell: str, **_options: object) -> dict[str, Any]:
        result = _matching_result(rates[len(runs)])
        result['floors'] = floors
        if worker_failure and len(runs) == len(rates) - 1:
            result['metrics']['worker_failures'] = ['injected failure']
        harness._assess(result)
        runs.append(result)
        return result

    monkeypatch.setattr(harness, '_run_attempt', attempt)
    result = harness._run_cell(
        'matching-13',
        work_dir=tmp_path / 'work',
        results_dir=tmp_path / 'results',
        duration_scale=1.0,
        device_class='test',
        filesystem='test',
        commit='test',
        floors=floors,
    )
    assert result['attempt_count'] == len(rates)
    assert result['run_rates'] == list(rates)
    expected_median = rates[len(rates) // 2]
    assert result['median_source_events_per_second'] == expected_median
    assert result['criteria']['source-events-rate'] is (expected_median >= 90)
    assert result['floor_missed'] is (expected_median < 90)
    assert result['passed'] is (
        not worker_failure and (expected_median >= 90 or floors == 'report')
    )
    assert json.loads((tmp_path / 'results' / 'matching-13.json').read_text()) == result


@pytest.mark.parametrize('floors', ['gate', 'report'])
def test_floor_report_mode_keeps_a_floor_miss_out_of_passed(floors: str) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    result = _matching_result(100.0)
    result.update(floors=floors, median_source_events_per_second=80.0)
    harness._assess(result)
    assert result['floor_missed']
    assert not result['criteria']['source-events-rate']
    assert result['passed'] is (floors == 'report')
    result['metrics']['worker_failures'] = ['injected failure']
    harness._assess(result)
    assert not result['passed']


@pytest.mark.parametrize('second_process', [False, True])
def test_delivery_commits_are_counted_apart_from_other_commits(
    tmp_path: Path, second_process: bool
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    result = harness._run_workload(
        tmp_path / 'source.db',
        groups=2,
        matching=True,
        duration=math.inf,
        event_limit=1,
        checkpoint_interval=0.02,
        second_process=second_process,
        stall_timeout_secs=3600.0,
    )
    assert result['callbacks'] == 2
    assert result['commits']['pre-invocation'] == 2
    assert result['commits']['done'] == 2
    assert result['commits']['failure'] == 0
    assert result['commits']['obligation-completion'] == 2
    assert result['delivery_commits_per_callback'] == 2.0
    assert sorted(record['role'] for record in result['raw']) == ['group0', 'group1', 'site']
    assert not result['worker_failures']


@pytest.mark.skipif(sys.platform == 'win32', reason='storage qualification uses POSIX WAL locking')
def test_wal_sampling_preserves_an_active_sqlite_read_lock(tmp_path: Path) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    path = tmp_path / 'source.db'
    code = """import fcntl, json, os, sys
fd = os.open(sys.argv[1], os.O_RDWR)
blocked = []
for offset in range(120, 128):
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 1, offset)
    except BlockingIOError:
        blocked.append(offset)
    else:
        fcntl.lockf(fd, fcntl.LOCK_UN, 1, offset)
print(json.dumps(blocked))
os.close(fd)
"""

    def blocked() -> list[int]:
        return json.loads(
            subprocess.check_output(
                [sys.executable, '-c', code, str(path) + '-shm'],
                text=True,
            )
        )

    try:
        with closing(sqlite3.connect(path, isolation_level=None)) as connection:
            connection.execute('PRAGMA journal_mode=WAL')
            connection.execute('PRAGMA wal_autocheckpoint=0')
            connection.execute('CREATE TABLE events(id INTEGER)')
            connection.execute('INSERT INTO events VALUES (1)')
            connection.execute('BEGIN')
            connection.execute('SELECT * FROM events').fetchall()
            before = blocked()
            assert before
            assert harness._position(path) > 0
            assert harness._position(path) > 0
            assert connection.in_transaction
            assert blocked() == before
    finally:
        # The harness must retain its descriptor until the last SQLite owner closes.
        harness._close_positions()


@pytest.mark.parametrize('delay_owner', ['admission', 'queue'])
@pytest.mark.parametrize('rate', [200.0, 300.0, 400.0])
def test_offered_demand_survives_producer_backpressure(
    monkeypatch: pytest.MonkeyPatch, delay_owner: str, rate: float
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    clock = [0.0]
    admitted = [0]

    class Stop:
        def is_set(self) -> bool:
            return False

        def wait(self, seconds: float) -> None:
            clock[0] += seconds

    def admit(*_args: object) -> None:
        admitted[0] += 1
        clock[0] += 1.3 if delay_owner == 'admission' else 0.001

    def publish(*_args: object) -> None:
        if delay_owner == 'queue':
            clock[0] += 1.3

    runtime: dict[str, Any] = {'stop': Stop(), 'start': 0.0, 'end': 1.0, 'inboxes': [object()]}
    monkeypatch.setattr(harness.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(harness, '_admit_page', admit)
    monkeypatch.setattr(harness, '_put_page', publish)
    count = harness._admit_load(
        object(), {'groups': 1, 'matching': True, 'duration': 1.0, 'offered_rate': rate}, runtime
    )
    result: dict[str, Any] = {
        'cell': 'offered-1',
        'offered_source_events_per_second': rate,
        'duration_seconds': 1.0,
        'offered': count,
        'groups': {'group0': {'delivered_at_load_end': 0, 'delivered_at_drain_end': 100}},
        'metrics': {
            'worker_failures': [],
            'protected_write_seconds': {'max': 0.01},
            'acquisition_seconds': {'max': 0.01},
            'bound_crossings': 0,
            'peak_wal_file_bytes': 0,
            'expired_attempts': 0,
        },
    }
    harness._assess(result)
    assert clock[0] > runtime['end']
    assert runtime['end'] == 1.0
    assert count == 100
    assert admitted[0] == 1
    assert not result['criteria']['group0-99-percent-delivery']
    assert result['offered'] == int(rate)
    assert result['groups']['group0']['released_at_load_end'] == int(rate) - 100
    assert result['groups']['group0']['undelivered_at_load_end'] == int(rate)
    assert result['criteria']['group0-two-page-backlog'] is (rate <= 300)
    assert runtime['admitted_at_load_end'] == (0 if delay_owner == 'admission' else 100)


@pytest.mark.parametrize('failure_stage', ['drain', 'cleanup', 'summary'])
def test_partial_observations_survive_late_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_stage: str
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    original_drain = harness._drain_load
    original_summary = harness._summarize
    original_close = harness._close_positions
    original_workload = harness._run_workload
    observed: dict[str, Any] = {}

    def one_event(path: Path, **options: object) -> dict[str, Any]:
        return original_workload(
            path,
            **{
                **options,
                'duration': math.inf,
                'event_limit': 1,
                'stall_timeout_secs': 3600.0,
            },
        )

    def drain(runtime: dict[str, Any]) -> None:
        original_drain(runtime)
        observed['runtime'] = runtime
        records = runtime['records']
        observed['transactions'] = sum(len(row['transactions']) for row in records)
        observed['samples'] = sum(len(row['samples']) for row in records)
        records[0]['errors'].append('earlier component failure')
        if failure_stage == 'drain':
            raise RuntimeError('injected failure after drain')

    def summarize(*args: object, **kwargs: object) -> object:
        if observed and failure_stage == 'summary':
            raise RuntimeError('injected failure during summary')
        return original_summary(*args, **kwargs)

    def close() -> None:
        original_close()
        if observed and failure_stage == 'cleanup':
            raise RuntimeError('injected failure after cleanup')

    monkeypatch.setattr(harness, '_drain_load', drain)
    monkeypatch.setattr(harness, '_summarize', summarize)
    monkeypatch.setattr(harness, '_close_positions', close)
    monkeypatch.setattr(harness, '_run_workload', one_event)
    try:
        result = harness._run_cell(
            'matching-1',
            work_dir=tmp_path / 'work',
            results_dir=tmp_path / 'results',
            duration_scale=0.002,
            device_class='test',
            filesystem='test',
            commit='test',
        )
        raw = json.loads((tmp_path / 'results' / 'matching-1.raw.json').read_text())
        assert observed['transactions'] > 0
        assert observed['samples'] > 0
        assert sum(len(row['transactions']) for row in raw) == observed['transactions']
        assert sum(len(row['samples']) for row in raw) == observed['samples']
        assert 'earlier component failure' in result['metrics']['worker_failures']
        assert any('injected failure' in error for error in result['metrics']['worker_failures'])
        assert result['incomplete']
        assert result['error_stage'] == failure_stage
        assert not result['passed']
        assert result['metrics'].get('source_events_per_second') is None
        runtime = observed['runtime']
        assert all(not worker.is_alive() for worker in runtime['workers'])
        assert not runtime['sampler'].is_alive()
    finally:
        original_close()


@pytest.mark.parametrize('mutation', ['missing-checkpoint', 'missing-writer', 'duplicate', 'none'])
def test_warning_gate_requires_every_independent_transition(mutation: str) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    site = harness._metrics('site')
    site['checkpoints'] = [
        {'at': 1, 'crossed': ['wal-growth']},
        {'at': 2, 'crossed': ['wal-growth', 'incomplete-duration']},
    ]
    group = harness._metrics('group0')
    group['expired'] = [{'at': 1}, {'at': 2}]
    expected = {
        'checkpoint:WAL growth': 1,
        'checkpoint:incomplete duration': 1,
        'writer:group0': 1,
    }
    warnings = dict(expected)
    if mutation == 'missing-checkpoint':
        del warnings['checkpoint:WAL growth']
    elif mutation == 'missing-writer':
        del warnings['writer:group0']
    elif mutation == 'duplicate':
        warnings['writer:group0'] += 1
    result: dict[str, Any] = {
        'cell': 'outside-reader-13',
        'duration_seconds': 60.0,
        'reader_pin_seconds': 60.1,
        'crossing_wal_bytes': 300_000_000,
        'stall_conditions': ['checkpoint-stalled', 'write-lock-stalled'],
        'delivery_paused': True,
        'commits_after_crossing_by_writer': {'source': 0, 'group0': 0},
        'warnings': warnings,
        'warning_expectations': expected,
        'recovery': {
            'wal_bytes_before_first_write': 0,
            'first_write_seconds': 1,
            'all_groups_caught_up': True,
        },
        'metrics': {
            'worker_failures': [],
            'protected_write_seconds': {'max': 0.01},
            'acquisition_seconds': {'max': 0.01},
        },
    }
    harness._assess(result)
    assert result['criteria']['warning-per-transition'] == (mutation == 'none')
    assert harness._warning_expectations([site, group]) == expected
    site['checkpoints'].extend([{'at': 3, 'crossed': []}, {'at': 4, 'crossed': ['wal-growth']}])
    group['transactions'] = [{'at': 3, 'committed': True}]
    group['expired'].append({'at': 4})
    assert harness._warning_expectations([site, group]) == {
        'checkpoint:WAL growth': 2,
        'checkpoint:incomplete duration': 1,
        'writer:group0': 2,
    }


@pytest.mark.parametrize(
    'defect',
    [
        None,
        'missing',
        'stale',
        'nan',
        'infinite',
        'negative',
        'exact',
        'late',
        'failed',
        'ordering',
        'wal',
        'write',
        'catchup',
    ],
)
def test_outside_reader_requires_current_replacement_startup(defect: str | None) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    result = _outside_result()
    recovery = result['recovery']
    if defect == 'missing':
        del recovery['startup_complete_at']
    elif defect == 'stale':
        recovery['startup_spawn_instance'] = 'original'
    elif defect in {'nan', 'infinite', 'negative', 'exact', 'late'}:
        recovery['startup_complete_at'] = {
            'nan': float('nan'),
            'infinite': float('inf'),
            'negative': 99.9,
            'exact': 130.0,
            'late': 130.1,
        }[defect]
    elif defect == 'failed':
        recovery['errors'] = ['replacement failed']
    elif defect == 'ordering':
        recovery['recovery_finished_at'] = 100.35
    elif defect == 'wal':
        recovery['wal_bytes_before_first_write'] = 1
    elif defect == 'write':
        recovery['first_write_seconds'] = 30
        recovery['first_write_completed_at'] = 130
    elif defect == 'catchup':
        recovery['all_groups_caught_up'] = False
    harness._assess(result)
    assert result['passed'] is (defect is None)
    if defect is None:
        # Startup and first write have independent deadlines, with either ordering valid.
        recovery['startup_complete_at'] = 100.15
        harness._assess(result)
        assert result['passed']


@pytest.mark.parametrize('elapsed', [-0.1, 30.0, 30.1])
def test_outside_reader_startup_deadline_rejects_invalid_elapsed(elapsed: float) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    recovery = _outside_result()['recovery']
    assert harness._replacement_startup(recovery)
    recovery['startup_complete_at'] = recovery['replacement_started_at'] + elapsed
    assert not harness._replacement_startup(recovery)


@pytest.mark.parametrize(('deadline_secs', 'expected'), [(30.0, False), (3600.0, True)])
def test_replacement_startup_observation_uses_optional_stall_budget(
    deadline_secs: float, expected: bool
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    recovery = _outside_result()['recovery']
    recovery['startup_complete_at'] = recovery['replacement_started_at'] + 31.0

    assert harness._within_startup(recovery, 'startup_complete_at', deadline_secs) is expected


@pytest.mark.parametrize('query', ['latest', 'late', 'bounded'])
def test_outside_reader_provider_shares_frontier_and_filters_before_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    query: str,
) -> None:
    monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
    config = support.write_daemon_config(tmp_path, support.Topology('default', ('group0',)))
    monkeypatch.setattr(sys, 'argv', ['probe', '--config', str(config)])
    support.install_outside_reader(monkeypatch, config, tmp_path / 'probe.log', enabled=True)
    first = support.DeterministicHandle(0, 1000, None)
    second = support.DeterministicHandle(0, 1000, None)
    (tmp_path / 'events-ready').touch()
    monkeypatch.setattr(threading.current_thread(), 'name', 'sg-event-daemon:source:admin')
    for low in range(100, 600, 100):
        rows = first.find('EventLogEntry', [['id', 'greater_than', low]], ['id'], limit=500)
        assert len(rows) == 90
        assert rows[-1]['id'] == low + 100
    monkeypatch.setattr(threading.current_thread(), 'name', 'sg-event-daemon:lane:group0')
    if query == 'latest':
        rows = second.find(
            'EventLogEntry', [], ['id'], limit=1, order=[{'field_name': 'id', 'direction': 'desc'}]
        )
        assert [row['id'] for row in rows] == [600]
    elif query == 'late':
        rows = second.find(
            'EventLogEntry',
            [{'filter_operator': 'any', 'filters': [['id', 'between', 590, 599]]}],
            ['id'],
            limit=3,
        )
        assert [row['id'] for row in rows] == [590, 591, 592]
    else:
        rows = second.find('EventLogEntry', [['id', 'greater_than', 595]], ['id'], limit=500)
        assert [row['id'] for row in rows] == [596, 597, 598, 599, 600]


@pytest.mark.parametrize('scale', [1 / 16, 1, 1 / 8])
def test_outside_reader_config_and_provider_follow_duration_scale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stall_detectors: StallDetectors,
    scale: float,
) -> None:
    stall_detectors.enable('startup')
    harness = importlib.import_module('benchmarks.storage_load')
    monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
    installed: list[support.ChildPatches] = []

    def worker(
        config: Path,
        patches: support.ChildPatches,
        *,
        stall_detectors: tuple[str, ...] | None = None,
    ) -> None:
        assert stall_detectors == ('startup',)
        installed.append(patches)
        monkeypatch.setattr(sys, 'argv', ['probe', '--config', str(config)])
        support.install_growth_probes(monkeypatch, patches, config.parent / 'probe.log')

    def supervisor(config: Path, patches: support.ChildPatches) -> None:
        generated = tmp_path / 'child.py'
        generated.write_text(support.child_code(config, patches))
        runpy.run_path(str(generated))
        raise RuntimeError('fixture installed')

    monkeypatch.setattr(support, 'run_worker', worker)
    monkeypatch.setattr(harness, 'running_supervisor', supervisor)
    with pytest.raises(RuntimeError, match='fixture installed'):
        harness._outside_reader(tmp_path / 'source.db', 1.0, scale)
    assert installed[0].outside_frontier_cap == (500 if scale == 1 / 16 else None)
    config = support.load_config(tmp_path / 'daemon.toml')
    assert len(config.sites['test'].groups) == 13
    assert config.sites['test'].sources['admin'].fetch_interval_secs == (
        1.0 if scale == 1 else 0.001
    )
    assert config.daemon.health_interval_secs == max(0.02, scale)
    assert config.daemon.checkpoint_interval_secs == max(0.02, scale)
    assert config.daemon.supervisor_poll_interval_secs == 0.01
    assert config.daemon.worker_startup_deadline_secs == 30.0
    assert {group.worker for group in config.sites['test'].groups.values()} == {'default'}
    handle = support.DeterministicHandle(0, 1000, None)
    (tmp_path / 'events-ready').touch()
    monkeypatch.setattr(threading.current_thread(), 'name', 'sg-event-daemon:source:admin')
    for low in (100, 200, 300, 400, 500):
        rows = handle.find('EventLogEntry', [['id', 'greater_than', low]], ['id'])
    assert [row['id'] for row in rows] == ([] if scale == 1 / 16 else [*range(501, 590), 600])


@pytest.mark.parametrize('frontier_cap', [500, None])
def test_outside_reader_provider_caps_scaled_frontier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, frontier_cap: int | None
) -> None:
    provider = support.OutsideReaderProvider(tmp_path, frontier_cap=frontier_cap)
    source = support.DeterministicHandle(0, 1000, None)
    lane = support.DeterministicHandle(0, 1000, None)
    assert len(provider.find(source, 'EventLogEntry', [], ['id'])) == 100
    (tmp_path / 'events-ready').touch()
    monkeypatch.setattr(threading.current_thread(), 'name', 'sg-event-daemon:source:admin')
    for low in (100, 200, 300, 400):
        rows = provider.find(source, 'EventLogEntry', [['id', 'greater_than', low]], ['id'])
        assert [row['id'] for row in rows] == [*range(low + 1, low + 90), low + 100]
        late = provider.find(
            lane, 'EventLogEntry', [['id', 'between', low + 90, low + 99]], ['id']
        )
        assert [row['id'] for row in late] == list(range(low + 90, low + 100))
    for low in (500, 600, 700):
        rows = provider.find(source, 'EventLogEntry', [['id', 'greater_than', low]], ['id'])
        assert len(rows) == (0 if frontier_cap == 500 else 90)
    monkeypatch.setattr(threading.current_thread(), 'name', 'sg-event-daemon:lane:group0')
    latest = provider.find(
        lane,
        'EventLogEntry',
        [],
        ['id'],
        limit=1,
        order=[{'field_name': 'id', 'direction': 'desc'}],
    )
    assert latest[0]['id'] == (500 if frontier_cap == 500 else 800)
    bounded = provider.find(lane, 'EventLogEntry', [['id', 'between', 490, 499]], ['id'], limit=3)
    assert [row['id'] for row in bounded] == [490, 491, 492]


@pytest.mark.parametrize('failure', ['before', 'after'])
@pytest.mark.parametrize('outside', [True, False])
def test_outside_restart_trace_stops_after_first_successful_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    *,
    outside: bool,
) -> None:
    monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
    config = support.write_daemon_config(tmp_path, support.Topology('default', ('group0',)))
    monkeypatch.setattr(sys, 'argv', ['probe', '--config', str(config)])
    actual = support.SourceWriteLock.transaction
    first = True

    @contextmanager
    def fail_first(
        lock: support.SourceWriteLock,
        connection: sqlite3.Connection,
        **options: Unpack[support._TransactionOptions],
    ) -> Generator[sqlite3.Connection]:
        nonlocal first
        failing, first = first, False
        if failing and failure == 'before':
            raise support._write_lock.WriteLockTimeoutError('injected', 'pause')
        with actual(lock, connection, **options) as active:
            yield active
            if failing:
                raise ValueError('injected')

    monkeypatch.setattr(support.SourceWriteLock, 'transaction', fail_first)
    support.install_growth_probes(
        monkeypatch,
        support.ChildPatches(writer_wal_limit_bytes=10_000_000, outside_reader=outside),
        tmp_path / 'worker.log',
    )
    path = tmp_path / 'test.db'
    with (
        closing(sqlite3.connect(path, isolation_level=None)) as connection,
        closing(
            support.SourceWriteLock(
                tmp_path / 'test.lock', database_path=path, timeout_secs=1, wait_phase_grace_secs=1
            )
        ) as lock,
    ):
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('CREATE TABLE records(value INTEGER)')
        with (
            pytest.raises(
                (ValueError, support._write_lock.WriteLockTimeoutError), match='injected'
            ),
            lock.transaction(connection),
        ):
            connection.execute('INSERT INTO records VALUES (0)')
        for value in range(1, 6):
            with lock.transaction(connection):
                connection.execute('INSERT INTO records VALUES (?)', (value,))
        assert connection.execute('SELECT COUNT(*) FROM records').fetchone()[0] == 5
    rows = [json.loads(line) for line in (tmp_path / 'worker.wal-writes').read_text().splitlines()]
    if outside:
        assert [row['phase'] for row in rows] == (
            ['pause', 'begin', 'commit']
            if failure == 'before'
            else ['begin', 'failed', 'begin', 'commit']
        )
        assert rows[-2]['started'] == rows[-1]['started']
    else:
        assert sum(row['phase'] == 'commit' for row in rows) == 5


@contextmanager
def _outside_supervisor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[
    tuple[support.SupervisorProbe, supervisor_tests.ScriptedWorker, supervisor_tests.FakeClock]
]:
    harness = importlib.import_module('benchmarks.storage_load')
    monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
    config_path = harness._outside_config(
        tmp_path, tuple(f'group{index}' for index in range(13)), 1
    )
    factory = supervisor_tests.ScriptedWorkerFactory()
    clock = supervisor_tests.FakeClock()
    with support.Supervisor(
        config=support.load_config(config_path),
        config_path=config_path,
        clock=clock,
        worker_factory=factory,
    ) as supervisor:
        supervisor._reconcile(generations={})
        supervisor.start()
        worker = next(iter(factory.instances.values()))
        supervisor._advance_worker_messages()
        yield (
            support.SupervisorProbe(supervisor, config_path, support.ChildPatches()),
            worker,
            clock,
        )


@pytest.mark.parametrize(
    ('outcome', 'stall_case'),
    [
        ('attached', (None, True, 0.0, 0.0)),
        ('joining', (None, False, 0.0, 0.0)),
        ('failed', (None, False, 0.0, 0.0)),
        ('stalled', (None, False, 121.0, 0.0)),
        ('stalled', (3600.0, True, 121.0, 0.0)),
        ('startup-stalled', (3600.0, True, 0.0, 31.0)),
    ],
)
def test_outside_reader_startup_precedes_service_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    stall_case: tuple[float | None, bool, float, float],
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    stall_timeout_secs, succeeds, frozen_catchup_at, startup_min_at = stall_case
    groups = tuple(f'group{index}' for index in range(13))
    with _outside_supervisor(tmp_path, monkeypatch) as (probe, worker, clock):
        supervisor = probe.supervisor
        original = SimpleNamespace(spawn_instance='original')
        replacement = SimpleNamespace(
            worker_id=worker.worker_id,
            spawn_instance=worker.spawn_instance,
            started_at=0.0,
            process=SimpleNamespace(poll=lambda: None),
            log_path=tmp_path / 'replacement.log',
        )
        probe.spawns.extend(
            [cast('support.WorkerSpawn', original), cast('support.WorkerSpawn', replacement)]
        )
        recovery: dict[str, Any] = {
            'errors': [],
            'retained_frontier': 100,
            'expected_groups': list(groups),
        }
        for suffix, rows in {
            '.wal-recovery': [{'started': 0.1, 'finished': 0.2, 'before': 2000000, 'after': 0}],
            '.wal-writes': [
                {'phase': 'begin', 'at': 0.3, 'started': 0.25, 'bytes': 0},
                {'phase': 'commit', 'at': 0.4, 'started': 0.25},
            ],
        }.items():
            replacement.log_path.with_suffix(suffix).write_text(
                ''.join(json.dumps(row) + '\n' for row in rows)
            )

        def publish(at: float) -> None:
            clock.now = at
            health = supervisor_tests.health_update(worker, heartbeat_at=at)
            health = replace(
                health,
                groups=tuple(
                    replace(
                        group,
                        cursor=100,
                        join_phase=(
                            'joining'
                            if index >= 8 and (at < 40 or outcome == 'joining')
                            else 'attached'
                        ),
                    )
                    for index, group in enumerate(health.groups)
                ),
            )
            worker.messages.append(health)
            supervisor._tick()
            assert supervisor._spawns[worker.worker_id].lifecycle is None
            assert supervisor._spawns[worker.worker_id].health_inventory_complete
            assert supervisor.probe_result().readiness is (at >= 40 and outcome != 'joining')

        publish(1.0)
        _delay_probe_startup(monkeypatch, probe, startup_min_at)
        windows: list[tuple[float, float]] = []
        budgets: list[float] = []

        def wait_for(predicate: Callable[[], bool], timeout: float) -> None:
            budgets.append(timeout)
            if predicate():
                return
            windows.append((clock(), timeout))
            deadline = clock() + timeout
            while not predicate():
                if clock() >= deadline:
                    raise AssertionError('observation deadline')
                next_at = next((at for at in (31.0, 40.0, 121.0) if at > clock()), deadline)
                publish(min(next_at, deadline))
                if outcome == 'failed' and clock() >= 31:
                    probe.terminations.append(
                        support.WorkerTermination(
                            worker.worker_id,
                            worker.spawn_instance,
                            'test failure',
                            clock(),
                            None,
                        )
                    )
            clock.now = max(clock.now, frozen_catchup_at)

        monkeypatch.setattr(probe, 'wait_for', wait_for)
        monkeypatch.setattr(harness.time, 'monotonic', clock)

        def durable_groups(_path: Path) -> dict[str, dict[str, int]]:
            return {
                group: {'cursor': 100, 'frontier': 100, 'pending_ranges': 0} for group in groups
            }

        monkeypatch.setattr(harness, '_caught_up_groups', durable_groups)
        expected = (
            nullcontext()
            if succeeds
            else pytest.raises(
                (AssertionError, RuntimeError), match=r'deadline|replacement worker failed'
            )
        )
        with expected:
            harness._replacement_observations(
                probe,
                original,
                tmp_path / 'state.db',
                recovery,
                stall_timeout_secs=stall_timeout_secs,
            )
        if succeeds:
            _assert_replacement_success(recovery, frozen_catchup_at)
        else:
            assert recovery.get('all_groups_caught_up', False) is (outcome == 'stalled')
            assert ('ready_at' in recovery) is (outcome == 'stalled')
        assert (
            recovery['startup_complete_at'],
            recovery['first_write_completed_at'],
            recovery['startup_spawn_instance'],
        ) == (max(1.0, startup_min_at), 0.4, worker.spawn_instance)
        assert (windows, budgets) == (
            cast(
                'dict[tuple[float | None, float], list[tuple[float, float]]]',
                {
                    (None, 0.0): [(1.0, 120.0)],
                    (3600.0, 0.0): [(1.0, 3600.0)],
                    (3600.0, 31.0): [(1.0, 3599.0), (31.0, 3600.0)],
                },
            )[(stall_timeout_secs, startup_min_at)],
            {
                None: [15.0, 29.0, 120.0],
                3600.0: [3600.0, 3599.0, 3600.0],
            }[stall_timeout_secs],
        )


@pytest.mark.parametrize('combined', [False, True])
def test_outside_reader_projects_observed_progress_and_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, combined: bool
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
    config = support.write_daemon_config(tmp_path, support.Topology('default', ('group0',)))
    log_path = tmp_path / 'original.log'
    monkeypatch.setattr(sys, 'argv', ['probe', '--config', str(config)])
    support.install_outside_reader(monkeypatch, config, log_path, enabled=True)
    monkeypatch.setattr(threading.current_thread(), 'name', 'sg-event-daemon:lane:group0')
    path = tmp_path / 'events.db'
    with open_test_group(path, group='group0', initial_position=100) as state:
        snapshot = state.snapshot()
        manifest = _noop_manifest()
        started = state.commit_pre_invocation(
            [StartEventStep(101, manifest, None), BeginCallbackStep(0)],
            expected_revision=snapshot.revision,
        )
        if combined:
            started = state.commit_pre_invocation(
                [CallbackDoneStep(0), StartEventStep(102, manifest, None), BeginCallbackStep(0)],
                expected_revision=started.snapshot.revision,
            )
        state.callback_done(0, expected_revision=started.snapshot.revision)
    records = harness._production_records(SimpleNamespace(log_path=log_path))
    group = next(record for record in records if record['role'] == 'group0')
    assert sum(row['count'] for row in group['progress']) == (2 if combined else 1)
    assert group['progress'][0]['at'] in {
        row['at'] for row in group['transactions'] if row['committed']
    }
    assert any(record['samples'] for record in records)
    summary = harness._summarize(records, 0, harness.time.monotonic() + 1, 1)
    assert summary['source_events_per_second'] > 0
    assert summary['groups']['group0']['delivered_including_drain'] == (2 if combined else 1)
    replacement = harness._metrics('restarted-site')
    replacement['production_records'] = records
    recovery: dict[str, Any] = {'errors': [], 'raw': replacement}
    harness._attach_outside_processes(summary, recovery, [{'at': 1, 'fds': 7, 'bytes': 0}])
    assert summary['peak_open_descriptors']['restarted-site'] > 0
    assert summary['peak_open_descriptors']['outside-reader'] == 7
    missing = harness._summarize([], 0, 1, 1)
    harness._attach_outside_processes(
        missing,
        {'errors': [], 'raw': harness._metrics('restarted-site')},
        [{'at': 1, 'fds': 7, 'bytes': 0}],
    )
    assert 'restarted-site' not in missing['peak_open_descriptors']


@pytest.mark.parametrize(
    'defect',
    [
        'no-group-records',
        'endpoint-only',
        'source',
        'source-watchdog',
        'lane',
        'operator',
        'stale-health',
        'stale-spawn',
        'dead-monitor',
        'current-failure',
    ],
)
def test_outside_startup_requires_current_production_liveness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stall_detectors: StallDetectors,
    defect: str,
) -> None:
    if defect == 'stale-health':
        stall_detectors.enable('heartbeat')
    with _outside_supervisor(tmp_path, monkeypatch) as (probe, worker, clock):
        supervisor = probe.supervisor
        clock.now = 1.0
        health = supervisor_tests.health_update(worker, heartbeat_at=1.0)
        if defect == 'no-group-records':
            health = replace(health, groups=())
        elif defect == 'source':
            health = replace(health, sources=())
        elif defect in {'source-watchdog', 'lane', 'operator'}:
            kind = 'source' if defect == 'source-watchdog' else defect
            removed = next(row for row in health.watchdogs if row.kind == kind)
            health = replace(
                health, watchdogs=tuple(row for row in health.watchdogs if row != removed)
            )
        elif defect == 'stale-spawn':
            health = replace(health, worker=replace(health.worker, spawn_instance='old'))
        elif defect == 'dead-monitor':
            health = replace(health, worker=replace(health.worker, monitor_alive=False))
        elif defect == 'current-failure':
            health = replace(
                health, worker=replace(health.worker, watchdog_failure='test failure')
            )
        if defect != 'endpoint-only':
            worker.messages.append(health)
            supervisor._tick()
        assert probe.startup_complete(
            worker.worker_id,
            worker.spawn_instance,
            now=7.0 if defect == 'stale-health' else clock(),
        ) is (defect == 'no-group-records')


@pytest.mark.parametrize(
    'artifact',
    [
        'source.db',
        'state/supervisor.db',
        'state/sites/test/sources/admin.db',
        'events-ready',
        'retained-frontier',
        'daemon.toml',
    ],
)
def test_outside_reader_refuses_existing_work_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, artifact: str
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    root = tmp_path / 'work' / 'outside-reader-13'
    prior = root / artifact
    prior.parent.mkdir(parents=True)
    prior.write_bytes(b'prior episode evidence\x00')
    before = {
        path.relative_to(root): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in root.rglob('*')
        if path.is_file()
    }
    entered = False

    def outside(*_args: object, **_kwargs: object) -> None:
        nonlocal entered
        entered = True
        raise AssertionError('prior work reached production configuration/spawn')

    monkeypatch.setattr(harness, '_outside_reader', outside)
    with pytest.raises(FileExistsError, match='already exists'):
        harness._run_attempt(
            'outside-reader-13',
            work_dir=root.parent,
            results_dir=tmp_path / 'fresh-results',
            duration_scale=1 / 16,
            device_class='test',
            filesystem='test',
            commit='test',
        )
    assert not entered
    assert not (tmp_path / 'fresh-results').exists()
    assert before == {
        path.relative_to(root): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in root.rglob('*')
        if path.is_file()
    }


@pytest.mark.parametrize(
    'field',
    [
        'original_spawn_instance',
        'wal_bytes_after_recovery',
        'wal_bytes_before_first_write',
        'first_write_seconds',
        'wal_bytes_before_owner_open',
        'first_actual_write_entered_at',
        'expected_groups',
        'retained_frontier',
        'groups',
        'catchup_health',
        'replacement_started_at',
        'startup_complete_at',
        'recovery_started_at',
        'recovery_finished_at',
        'first_write_entered_at',
        'first_write_completed_at',
        'catchup_started_at',
        'ready_at',
        'catchup_finished_at',
    ],
)
def test_outside_reader_rejects_missing_recovery_proof(field: str) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    result = _outside_result()
    del result['recovery'][field]
    harness._assess(result)
    assert not result['passed']


@pytest.mark.parametrize(
    ('path', 'value'),
    [
        (('original_spawn_instance',), ''),
        (('first_write_seconds',), -1),
        (('first_write_seconds',), 0.2),
        (('wal_bytes_after_recovery',), False),
        (('wal_bytes_before_first_write',), False),
        (('wal_bytes_before_owner_open',), 299999999),
        (('wal_bytes_before_owner_open',), float('inf')),
        (('first_actual_write_entered_at',), 100.15),
        (('first_actual_write_entered_at',), 100.31),
        (('recovery_started_at',), 99.9),
        (('catchup_started_at',), 100.3),
        (('catchup_finished_at',), float('inf')),
        (('catchup_finished_at',), 249.999),
        (('expected_groups',), ['other'] * 13),
        (('groups', 'group0', 'cursor'), 99),
        (('groups', 'group0', 'frontier'), 99),
        (('groups', 'group0', 'pending_ranges'), 1),
        (('catchup_health', 'worker', 'spawn_instance'), 'original'),
        (('catchup_health', 'worker', 'worker_id'), 'other'),
        (('catchup_health', 'worker', 'monitor_alive'), False),
        (('catchup_health', 'worker', 'watchdog_failure'), 'failed'),
        (('catchup_health', 'worker', 'heartbeat_at'), 141),
        (('catchup_health', 'groups', 0, 'group'), 'other'),
        (('catchup_health', 'groups', 0, 'worker_instance'), 'original'),
        (('catchup_health', 'groups', 0, 'join_phase'), 'joining'),
        (('catchup_health', 'groups', 0, 'pending_catchup_epoch_count'), 1),
        (('catchup_health', 'groups', 0, 'outstanding_late_obligation_count'), 1),
        (('catchup_health', 'groups', 0, 'outstanding_late_id_count'), 1),
    ],
)
def test_outside_reader_rejects_contradictory_recovery_proof(
    path: tuple[str | int, ...], value: object
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    result = _outside_result()
    target = result['recovery']
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    harness._assess(result)
    assert not result['passed']


@pytest.mark.parametrize('defect', [None, 'missing-owner', 'wrong-owner', 'write', 'callback'])
def test_outside_pause_requires_each_owner_and_rejects_later_activity(defect: str | None) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    owners = ['sg-event-daemon:source:admin'] + [
        f'sg-event-daemon:lane:group{index}' for index in range(13)
    ]
    rows = [{'owner': owner, 'kind': 'pause'} for owner in owners]
    if defect == 'missing-owner':
        rows.pop()
    elif defect == 'wrong-owner':
        rows[-1]['owner'] = 'unrelated-coordinator'
    elif defect in {'write', 'callback'}:
        rows.append({'owner': owners[0], 'kind': defect})
    # Activity before a refusal is allowed. No timestamps or quiet suffix
    # establish the barrier: every owner must have attempted a real write.
    rows.insert(0, {'owner': owners[0], 'kind': 'write'})
    if defect in {'write', 'callback'}:
        with pytest.raises(RuntimeError, match='activity after growth pause'):
            harness._outside_pause_complete(rows)
    else:
        assert harness._outside_pause_complete(rows) is (defect is None)


@pytest.mark.parametrize(
    'missing',
    [
        None,
        'growth',
        'checkpoint-stalled',
        'write-lock-stalled',
        'pause',
        'wal-growth',
        'incomplete-duration',
        'checkpoint:WAL growth',
        'checkpoint:incomplete duration',
        'writer:source',
        'writer:group12',
        'warning-count',
    ],
)
def test_outside_ci_episode_requires_all_observations(tmp_path: Path, missing: str | None) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    conditions = {'checkpoint-stalled', 'write-lock-stalled'} - {missing}
    health = SimpleNamespace(sources=[SimpleNamespace(conditions=conditions)], groups=[])
    original = SimpleNamespace(worker_id='original', log_path=tmp_path / 'original.log')
    probe = SimpleNamespace(latest_health={'original': health})
    owners = ['sg-event-daemon:source:admin'] + [
        f'sg-event-daemon:lane:group{index}' for index in range(13)
    ]
    rows = [{'owner': owner, 'kind': 'pause'} for owner in owners]
    if missing == 'pause':
        rows.pop()
    original.log_path.with_suffix('.pause').write_text(
        ''.join(json.dumps(row) + '\n' for row in rows)
    )
    _write_outside_warning_records(original.log_path.with_suffix('.storage'), missing)
    # An arbitrarily late sample still cannot substitute for a missing signal.
    sample = {'at': 1_000_000.0, 'bytes': 100 if missing == 'growth' else 101}
    assert harness._outside_pause_ready(probe, original, sample, 100) is (missing is None)


@pytest.mark.parametrize('observe_pause', [False, True])
def test_outside_pause_observer_flushes_before_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    observe_pause: bool,
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    monkeypatch.setenv('SG_EVENT_HARNESS_KEY', 'test-key')
    config = support.write_daemon_config(tmp_path, support.Topology('default', ('group0',)))
    log_path = tmp_path / 'original.log'
    owner = 'sg-event-daemon:lane:group0'
    monkeypatch.setattr(threading.current_thread(), 'name', owner)
    entered: list[bool] = []
    expected: list[dict[str, str]] = []

    def invoke(_spec: object, _context: object) -> None:
        rows = harness._json_rows(log_path.with_suffix('.pause'))
        assert rows == expected
        entered.append(True)

    monkeypatch.setattr(support._lane, '_invoke_sync', invoke)
    support.install_outside_reader(
        monkeypatch,
        config,
        log_path,
        enabled=True,
        observe_pause=observe_pause,
    )
    callback = cast('Any', support._lane._invoke_sync)
    callback(None, SimpleNamespace(group='group0'))
    assert entered == [True]
    (tmp_path / 'events-ready').touch()
    expected = [{'owner': owner, 'kind': 'callback'}] if observe_pause else []
    callback(None, SimpleNamespace(group='group0'))
    assert entered == [True, True]


def _write_outside_warning_records(path: Path, missing: str | None = None) -> None:
    roles = ['site', *(f'group{index}' for index in range(13))]
    rows: list[dict[str, Any]] = [
        {
            'kind': 'checkpoints',
            'role': 'site',
            'at': 1.0,
            'crossed': sorted({'wal-growth', 'incomplete-duration'} - {missing}),
        }
    ]
    categories = ['checkpoint:WAL growth', 'checkpoint:incomplete duration']
    for role in roles:
        rows.append({'kind': 'expired', 'role': role, 'at': 1.0, 'level': 'pause'})
        categories.append('writer:' + ('source' if role == 'site' else role))
    rows.extend(
        {'kind': 'warnings', 'role': 'site', 'at': 1.0, 'category': category}
        for category in categories
        if category != missing
    )
    if missing == 'warning-count':
        # Another independently recorded transition needs its own warning.
        rows.extend(
            [
                {
                    'kind': 'transactions',
                    'role': 'group0',
                    'at': 2.0,
                    'committed': True,
                    'kind_label': 'other',
                },
                {'kind': 'expired', 'role': 'group0', 'at': 3.0, 'level': 'pause'},
            ]
        )
    path.write_text(''.join(json.dumps(row) + '\n' for row in rows))


@pytest.mark.parametrize('duplicate', ['checkpoint:incomplete duration', 'writer:source'])
def test_outside_ci_completion_leaves_duplicate_warnings_to_verdict(
    tmp_path: Path, duplicate: str
) -> None:
    harness = importlib.import_module('benchmarks.storage_load')
    original = SimpleNamespace(log_path=tmp_path / 'original.log')
    path = original.log_path.with_suffix('.storage')
    _write_outside_warning_records(path)
    with path.open('a') as stream:
        stream.write(
            json.dumps(
                {
                    'kind': 'warnings',
                    'role': 'site',
                    'at': 2.0,
                    'category': duplicate,
                }
            )
            + '\n'
        )
    records = harness._production_records(original)
    assert harness._outside_warnings_observed(records)
    result = _outside_result()
    expected = harness._warning_expectations(records)
    result['warning_expectations'] = expected
    result['warnings'] = {**expected, duplicate: expected[duplicate] + 1}
    harness._assess(result)
    assert not result['criteria']['warning-per-transition']
