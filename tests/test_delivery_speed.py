from __future__ import annotations

import copy
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import tomllib
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any, cast

import pytest

from benchmarks import delivery_speed as benchmark
from benchmarks._delivery_speed_hooks import _instrumentation as hook
from tests._stall_policy import STALL_DEADLINE_SECS


def _change_lifetime_samples(samples: list[dict[str, Any]], change: str) -> None:
    if change in ('stress', 'loaded'):
        samples[1]['busy_percent'] = 70 if change == 'stress' else 81
    elif change == 'empty':
        samples.clear()
    elif change == 'timestamp':
        samples[1]['monotonic'] = None
    elif change == 'unordered':
        samples[0], samples[1] = samples[1], samples[0]
    elif change == 'error':
        samples[1]['error'] = 'capture failed'
    elif change == 'nan':
        samples[1]['busy_percent'] = float('nan')
    elif change == 'missing-before':
        samples.pop(0)
    elif change == 'missing-final':
        samples[:] = samples[:1]


def _detach_line(stamp: str, group: str, reason: str) -> str:
    return (
        f'{stamp} INFO sg_event_daemon.source: detached group {group} '
        f'from its live channel: {reason} group={group}\n'
    )


def _assert_recovered_analyzer(directory: Path) -> None:
    detached = directory / 'child-3.stdout.log'
    detached.write_text(
        _detach_line('2026-11-04T00:00:01+00:00', 'bulk-eager', 'queue-count-exhausted')
    )
    recovered = benchmark.analyze_run(directory)
    assert recovered['status'] == 'failed'
    assert recovered['recovery_run'] is True
    validation = cast('dict[str, bool]', recovered['validation'])
    groups = recovered['groups']
    detaches = cast('list[dict[str, object]]', recovered['capacity_detaches'])
    assert isinstance(groups, dict)
    assert validation['complete_delivery'] is True
    assert all(validation.values())
    assert groups['bulk-eager']['rate'] == 1400
    assert len(detaches) == 1
    detached.unlink()


def _healthy_status() -> dict[str, Any]:
    return {
        'readiness': True,
        'liveness': True,
        'projection_truncated': False,
        'sources': [
            {
                'status': 'running',
                'conditions': [],
                'frontier': 1_024_000,
                'late_id_count': 0,
                'open_range_count': 0,
                'pending_obligation_count': 0,
            }
        ],
        'groups': [
            {
                'group': 'bulk-eager',
                'status': 'running',
                'conditions': [],
                'cursor': 1_024_000,
                'join_phase': 'attached',
                'parked': False,
                'current_event_id': None,
                'outstanding_late_id_count': 0,
                'outstanding_late_obligation_count': 0,
                'pending_catchup_epoch_count': 0,
            }
        ],
        'workers': [
            {
                'monitor_alive': True,
                'stuck_threads': [],
                'watchdog_failure': None,
            }
            for _ in range(2)
        ],
    }


def _observations(
    *, paired: bool = False, counts: tuple[int, ...] = (7000,), duration: float = 5.0
) -> dict[str, Any]:
    start = 110.45
    end = start + duration
    names = ['bulk-eager', 'bulk-strict'] if paired else ['bulk-eager']
    records: dict[str, list[dict[str, object]]] = {}
    for name, count in zip(names, counts, strict=True):
        # Warm-up and one burst callback before publication never enter the numerator.
        times = [100.0, 110.1, start]
        times.extend(start + duration * index / count for index in range(1, count))
        times.extend(end + index / 1000 for index in range(1000))
        records[name] = [
            {
                'event_id': 1_000_001 + index,
                'delivered_at': stamp,
                'delivery_attempt': 1,
                'is_operator_replay': False,
            }
            for index, stamp in enumerate(times)
        ]
    return {
        'paired': paired,
        'warmup': 1,
        'records': records,
        'publications': [
            {'group': name, 'event_id': 1_024_000, 'end': finish}
            for name, finish in zip(names, [10.2] if not paired else [10.0, 10.2], strict=True)
        ],
        'transactions': [],
        'wall_offset': 100.0,
        'host_samples': [
            {'monotonic': 0.0, 'busy_percent': 60.0, 'error': None},
            {'monotonic': 12.0, 'busy_percent': 60.0, 'error': None},
            {'monotonic': 19.0, 'busy_percent': 60.0, 'error': None},
        ],
        'launch_monotonic': 1.0,
        'finish_monotonic': 20.0,
        'capacity_detaches': [],
    }


def _stop_test_children(process: subprocess.Popen[bytes], directory: Path) -> dict[str, bool]:
    marker = directory / f'session-{process.pid}.json'
    owns_session = marker.exists() and json.loads(marker.read_text()) == {
        'pid': process.pid,
        'sid': process.pid,
    }
    if not owns_session:
        if process.poll() is None:
            process.terminate()
        process.wait()
        return {'process_group_gone': False, 'cleanup_clean': False}
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    process.wait()
    while True:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return {'process_group_gone': True, 'cleanup_clean': process.returncode == 0}
        time.sleep(0.01)


def _wait_test_children_ready(directory: Path, process: subprocess.Popen[bytes]) -> None:
    index = 0
    while True:
        # A bounded control request may time out under load; _status returns
        # an empty result then, and only the attached state ends this wait.
        status = benchmark._status(directory, index)
        index += 1
        if (
            status.get('readiness')
            and len(status.get('groups', [])) == 2
            and all(row.get('join_phase') == 'attached' for row in status['groups'])
        ):
            return
        assert process.poll() is None, f'startup exited {process.returncode}'
        time.sleep(0.01)


def test_delivery_benchmark_uses_diagnosis_payload_and_probe(tmp_path: Path) -> None:
    event = hook._event(1_000_004)
    assert event == {
        'type': 'EventLogEntry',
        'id': 1_000_004,
        'event_type': 'Shotgun_Shot_New',
        'attribute_name': None,
        'meta': {
            'type': 'new_entity',
            'entity_type': 'Shot',
            'entity_id': 2,
            'new_value': 'offline-burst',
            'padding': 'x' * 128,
        },
        'entity': {'type': 'Shot', 'id': 2, 'name': 'offline-shot'},
        'user': None,
        'project': {'type': 'Project', 'id': 1, 'name': 'offline-project'},
        'session_uuid': None,
        'created_at': hook.datetime(2026, 11, 4, tzinfo=hook.UTC),
    }
    config = tomllib.loads(benchmark._write_config(tmp_path, paired=True).read_text())
    assert config['daemon']['supervisor_poll_interval_secs'] == 0.05
    source = config['sites']['live']['sources']
    assert source == {
        'events': {
            'credentials': 'source',
            'fetch_interval_secs': 0.05,
            'late_event_window_secs': 30.0,
        }
    }
    groups = config['sites']['live']['groups']
    assert set(groups) == {'bulk-eager', 'bulk-strict'}
    for name, group in groups.items():
        assert group['worker'] == 'dedicated'
        assert group['plugins'][0]['path'] == str(benchmark._PROBE)
        assert (
            Path(group['plugins'][0]['path']).read_bytes()
            == Path('tests/live/fixtures/bulk_probe.py').read_bytes()
        )
        assert group['ordering'] == name.removeprefix('bulk-')
    with pytest.raises(ValueError, match='10000'):
        benchmark.run_benchmark(work_dir=tmp_path / 'invalid', paired=False, burst=9999)


def test_delivery_capacity_detach_without_publication_has_no_interval(tmp_path: Path) -> None:
    log = tmp_path / 'child-2.stderr.log'
    log.write_text(
        _detach_line('2026-11-04T00:00:01+00:00', 'bulk-eager', 'queue-count-exhausted')
    )
    stdout = tmp_path / 'child-3.stdout.log'
    stdout.write_text(
        _detach_line('2026-11-04T00:00:02+00:00', 'bulk-eager', 'queue-byte-exhausted')
    )
    inputs = _observations()
    inputs['publications'].clear()
    inputs['capacity_detaches'] = benchmark._capacity_detaches(tmp_path)
    result = benchmark._measure(**inputs)
    assert result['status'] == 'failed'
    assert result['recovery_run'] is True
    assert {row['source_file'] for row in result['capacity_detaches']} == {log.name, stdout.name}
    assert 'interval' not in result
    log.write_text(
        _detach_line('2026-11-04T00:00:01+00:00', 'bulk-eager', 'transport-lost')
        + _detach_line('2026-11-04T00:00:02+00:00', 'bulk-eager', 'channel-closed')
    )
    stdout.unlink()
    assert benchmark._capacity_detaches(tmp_path) == []


def test_delivery_child_install_failure_is_fail_closed_without_group_signal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Missing run metadata makes hook installation fail before any user code.
    process = benchmark.spawn_child(
        [sys.executable, '-c', 'raise AssertionError("must not execute")', 'supervise'],
        env=benchmark._child_environment(tmp_path),
    )

    def no_group_signal(*_args: object) -> None:
        pytest.fail('unacknowledged process group signaled')

    try:
        assert process.wait() == 70
    finally:
        monkeypatch.setattr(
            benchmark.os,
            'killpg',
            no_group_signal,
        )
        assert not benchmark._stop_owned(process, tmp_path)['cleanup_clean']
    error = (tmp_path / f'child-{process.pid}.stderr.log').read_text()
    assert 'FileNotFoundError' in error
    assert 'must not execute' not in error
    assert not list(tmp_path.glob('session-*.json'))


def test_delivery_child_session_is_owned_and_workers_inherit_it(tmp_path: Path) -> None:
    (tmp_path / 'run.json').write_text(json.dumps({'warmup': 4000, 'burst': 20000}))
    script = """
import json, os, sys
from pathlib import Path
from sg_event_daemon._platform import spawn_child
child = spawn_child(
    [sys.executable, '-c', 'import os; print(os.getsid(0))', 'run-worker'], env=os.environ
)
assert child.wait() == 0
print(os.getpid(), os.getsid(0))
"""
    process = benchmark.spawn_child(
        [sys.executable, '-c', script, 'supervise'], env=benchmark._child_environment(tmp_path)
    )
    try:
        assert process.wait() == 0
    finally:
        assert _stop_test_children(process, tmp_path) == {
            'process_group_gone': True,
            'cleanup_clean': True,
        }
        # Exercise the benchmark's clean exited-session result after the real
        # process group has disappeared, so its retry window cannot decide it.
        assert benchmark._stop_owned(process, tmp_path)['cleanup_clean']
    markers = [json.loads(path.read_text()) for path in tmp_path.glob('instrumented-*.json')]
    assert len(markers) == 2
    worker = next(row for row in markers if row['pid'] != process.pid)
    assert worker['parent_pid'] == process.pid
    assert (tmp_path / f'child-{worker["pid"]}.stdout.log').read_text().strip() == str(process.pid)
    assert (
        tmp_path / f'child-{process.pid}.stdout.log'
    ).read_text().strip() == f'{process.pid} {process.pid}'
    assert len(list(tmp_path.glob('session-*.json'))) == 1
    assert len(list(tmp_path.glob('timings-*.json'))) == 2


def test_delivery_failure_keeps_evidence_and_never_overwrites(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Sampler:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def start(self) -> None:
            raise RuntimeError('sample startup failed')

        def stop(self) -> None:
            (tmp_path / 'stopped').touch()

        def samples(self) -> tuple[object, ...]:
            return ()

    monkeypatch.setattr(benchmark, 'HostSampler', Sampler)
    directory = tmp_path / 'run'
    report = benchmark.run_benchmark(work_dir=directory, paired=False)
    assert report['status'] == 'failed'
    assert (tmp_path / 'stopped').exists()
    assert (directory / 'run.json').exists()
    assert json.loads((directory / 'report.json').read_text())['status'] == 'failed'
    original = (directory / 'run.json').read_bytes()
    with pytest.raises(FileExistsError):
        benchmark.run_benchmark(work_dir=directory, paired=False)
    assert (directory / 'run.json').read_bytes() == original


def test_delivery_hook_is_offline_and_child_scoped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('SG_FAKE_TEST_SECRET', 'must-not-inherit')
    (tmp_path / 'run.json').write_text(json.dumps({'warmup': 4000, 'burst': 20000}))
    environment = benchmark._child_environment(tmp_path)
    assert not any(name.startswith(('SG_', 'SHOTGUN_')) for name in environment)
    script = """
import json, socket, sys
from sg_event_daemon._providers import Provider
assert Provider.create_sync.__module__ == '_instrumentation'
for action in (lambda: socket.socket().connect(('192.0.2.1', 80)),
               lambda: socket.getaddrinfo('offline.invalid', 443)):
    try:
        action()
    except RuntimeError:
        pass
    else:
        raise AssertionError('network permitted')
a, b = socket.socketpair()
a.send(b'ok')
assert b.recv(2) == b'ok'
a.close(); b.close()
print('offline')
"""
    child = subprocess.run(
        [sys.executable, '-c', script],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 0, child.stderr
    assert next(tmp_path.glob('child-*.stdout.log')).read_text().strip() == 'offline'
    installed = list(tmp_path.glob('instrumented-*.json'))
    assert len(installed) == 1
    assert json.loads(installed[0].read_text())['network_guard'] is True
    normal = subprocess.run(
        [
            sys.executable,
            '-c',
            (
                'from sg_event_daemon._providers import Provider; '
                'print(Provider.create_sync.__module__)'
            ),
        ],
        env={
            key: value
            for key, value in os.environ.items()
            if key != 'PYTHONPATH' and not key.startswith(('SG_', 'SHOTGUN_'))
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert normal.returncode == 0, normal.stderr
    assert normal.stdout.strip() == 'sg_event_daemon._providers'


def test_delivery_integrity_check_preserves_nonempty_wal_evidence(tmp_path: Path) -> None:
    state = tmp_path / 'state'
    state.mkdir()
    connection = sqlite3.connect(state / 'events.db')
    connection.execute('CREATE TABLE epochs (id INTEGER)')
    connection.close()
    wal = state / 'events.db-wal'
    wal.write_bytes(b'nonempty retained WAL evidence')
    checks = benchmark._database_checks(tmp_path)
    assert checks[0]['wal_empty'] is False
    assert wal.read_bytes() == b'nonempty retained WAL evidence'


@pytest.mark.parametrize(
    ('duration', 'status'), [(2.999, 'too-short'), (3.0, 'passed'), (6.0, 'passed')]
)
def test_delivery_interval_requires_three_seconds(duration: float, status: str) -> None:
    result = benchmark._measure(**_observations(counts=(8400,), duration=duration))
    assert result['status'] == status


@pytest.mark.parametrize(
    ('paired', 'counts', 'status'),
    [
        (False, (4200,), 'passed'),
        (False, (4199,), 'failed'),
        (True, (3000, 3000), 'passed'),
        (True, (2999, 4000), 'failed'),
        (True, (4000, 2999), 'failed'),
    ],
)
def test_delivery_floors_apply_to_every_group(
    paired: bool, counts: tuple[int, ...], status: str
) -> None:
    assert (
        benchmark._measure(**_observations(paired=paired, counts=counts, duration=3.0))['status']
        == status
    )


@pytest.mark.parametrize(
    'change',
    [
        'before',
        'inside',
        'after',
        'missing-interval',
        'missing-before',
        'error',
        'nan',
        'stress',
        'loaded',
        'timestamp',
        'unordered',
    ],
)
def test_delivery_floor_requires_all_host_samples(change: str) -> None:
    inputs = _observations()
    samples = inputs['host_samples']
    if change in ('before', 'inside', 'after'):
        samples[('before', 'inside', 'after').index(change)]['busy_percent'] = 60.1
    elif change == 'missing-interval':
        samples.pop(1)
    elif change == 'missing-before':
        samples.pop(0)
    elif change == 'timestamp':
        samples.append({'monotonic': float('nan'), 'busy_percent': 20, 'error': None})
    elif change == 'unordered':
        samples[1], samples[2] = samples[2], samples[1]
    elif change == 'nan':
        samples[1]['busy_percent'] = float('nan')
    elif change in ('stress', 'loaded'):
        samples[1]['busy_percent'] = 80.0 if change == 'stress' else 80.1
    else:
        samples[1]['error'] = 'sampling failed'
    result = benchmark._measure(**inputs)
    assert result['status'] == 'ineligible-host'
    assert result['host']['eligible'] is False
    assert result['host']['reason']
    assert result['groups']['bulk-eager']['rate'] == 1400


@pytest.mark.parametrize('group', ['bulk-eager', 'bulk-strict'])
@pytest.mark.parametrize(
    'reason', ['queue-count-exhausted', 'queue-byte-exhausted', 'strict-buffer-capacity']
)
@pytest.mark.parametrize('position', ['before', 'inside', 'after'])
def test_delivery_capacity_detach_excludes_recovered_run(
    tmp_path: Path, group: str, reason: str, position: str
) -> None:
    inputs = _observations(paired=True, counts=(5000, 5000))
    stamp = datetime.fromtimestamp(
        {'before': 109, 'inside': 112, 'after': 118}[position], UTC
    ).isoformat()
    log = tmp_path / 'child-2.stdout.log'
    log.write_text(
        _detach_line(stamp, group, reason) + _detach_line(stamp, group, 'transport-lost')
    )
    inputs['capacity_detaches'] = benchmark._capacity_detaches(tmp_path)
    result = benchmark._measure(**inputs)
    assert result['status'] == 'failed'
    assert result['recovery_run'] is True
    assert result['measurement_exclusion'] == 'capacity detach recovery'
    assert result['capacity_detaches'] == [
        {
            'group': group,
            'reason': reason,
            'source_file': log.name,
            'source_line': 1,
            'timestamp': stamp,
        }
    ]
    assert all(result['validation'].values())
    assert result['groups'][group]['rate'] == 1000


def test_delivery_interval_waits_for_publication_to_every_group() -> None:
    inputs = _observations(paired=True, counts=(5000, 5000))
    result = benchmark._measure(**inputs)
    assert result['interval'] == {
        'start_wall': 110.45,
        'end_wall': 115.45,
        'duration_secs': 5.0,
        'wall_offset': 100.0,
    }
    assert result['status'] == 'passed'
    for group, row in result['groups'].items():
        assert row['callbacks'] == 5000
        assert row['outstanding_at_start'] == 6000
        assert row['outstanding_at_end'] == 1000
        assert row['rate'] == 1000
        assert inputs['records'][group][-1000]['delivered_at'] == result['interval']['end_wall']
    inputs['publications'].pop()
    missing = benchmark._measure(**inputs)
    assert missing['status'] == 'failed'
    assert 'interval' not in missing


def test_delivery_provider_has_no_page_delay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_sleep(_seconds: float) -> None:
        pytest.fail('synthetic provider introduced a page delay')

    monkeypatch.setattr(hook.time, 'sleep', no_sleep)
    benchmark._release(tmp_path, 4)
    handle = hook._FakeHandle(tmp_path)
    assert [
        row['id']
        for row in handle.find('EventLogEntry', [['id', 'greater_than', 1000000]], limit=2)
    ] == [1000001, 1000002]
    assert handle.find('EventLogEntry', [['id', 'between', 1000002, 1000003]], ['id']) == [
        {'type': 'EventLogEntry', 'id': 1000002},
        {'type': 'EventLogEntry', 'id': 1000003},
    ]


def test_delivery_real_children_attach_and_exit_without_releasing_events(tmp_path: Path) -> None:
    (tmp_path / 'run.json').write_text(json.dumps({'warmup': 4000, 'burst': 20000}))
    config = benchmark._write_config(tmp_path, paired=True)
    config_text = config.read_text()
    assert config_text.count('worker = "dedicated"\n') == 2
    config_text = config_text.replace(
        'supervisor_poll_interval_secs = 0.05\n',
        'supervisor_poll_interval_secs = 0.05\n'
        f'worker_startup_deadline_secs = {STALL_DEADLINE_SECS}\n'
        f'shutdown_deadline_secs = {STALL_DEADLINE_SECS}\n'
        f'async_cancel_grace_secs = {STALL_DEADLINE_SECS}\n',
    ).replace(
        'worker = "dedicated"\n',
        f'worker = "dedicated"\ncallback_deadline_secs = {STALL_DEADLINE_SECS}\n',
    )
    config.write_text(config_text)
    (tmp_path / 'sitecustomize.py').write_text(
        'import logging\n'
        'import os\n'
        'try:\n'
        '    from _instrumentation import install\n'
        '    install()\n'
        '    import pytest\n'
        '    from tests._stall_policy import activate_stall_policy\n'
        '    patch = pytest.MonkeyPatch()\n'
        '    activate_stall_policy(patch)\n'
        'except BaseException:\n'
        '    logging.getLogger(__name__).exception("offline benchmark hook installation failed")\n'
        '    os._exit(70)\n'
    )
    environment = benchmark._child_environment(tmp_path)
    environment['PYTHONPATH'] = os.pathsep.join(
        (str(tmp_path), str(Path(__file__).resolve().parents[1]), environment['PYTHONPATH'])
    )
    benchmark._release(tmp_path, 0)
    process = benchmark.spawn_child(
        [sys.executable, '-m', 'sg_event_daemon', 'supervise', '--config', str(config)],
        env=environment,
    )
    try:
        _wait_test_children_ready(tmp_path, process)
    finally:
        cleanup = _stop_test_children(process, tmp_path)
    assert cleanup == {'process_group_gone': True, 'cleanup_clean': True}
    assert process.returncode == 0
    markers = [json.loads(path.read_text()) for path in tmp_path.glob('instrumented-*.json')]
    assert len(markers) == 4
    assert all(row['network_guard'] for row in markers)
    assert len(list(tmp_path.glob('timings-*.json'))) == 4
    assert not list(tmp_path.glob('bulk-*.jsonl'))


def test_delivery_releases_whole_burst_only_after_warmup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    actions: list[tuple[str, int]] = []
    process = object()

    def wait(_counter: object, target: int, actual: object) -> None:
        assert actual is process
        actions.append(('wait', target))

    def ready(*_args: object) -> int:
        return 0

    def release(_directory: Path, count: int) -> None:
        actions.append(('release', count))

    def sleep(_seconds: float) -> None:
        pass

    def status(*_args: object) -> dict[str, Any]:
        current = _healthy_status()
        current['sources'][0]['frontier'] = 1_014_000
        current['groups'][0]['cursor'] = 1_014_000
        return current

    monkeypatch.setattr(benchmark, '_wait_ready', ready)
    monkeypatch.setattr(benchmark._ProbeCounter, 'wait', wait)
    monkeypatch.setattr(benchmark, '_release', release)
    monkeypatch.setattr(benchmark.time, 'sleep', sleep)
    monkeypatch.setattr(benchmark, '_status', status)
    benchmark._deliver(
        tmp_path,
        cast('subprocess.Popen[bytes]', process),
        ['bulk-eager'],
        {'warmup': 4000, 'burst': 10000},
    )
    assert actions == [('release', 4000), ('wait', 4000), ('release', 14000), ('wait', 14000)]


def test_delivery_timing_uses_complete_transactions_and_callback_denominator() -> None:
    inputs = _observations()
    inputs['transactions'] = [
        {
            'thread': 'sg-event-daemon:lane:bulk-eager',
            'started_at': start,
            'begun_at': begun,
            'finished_at': finish,
            'committed': True,
        }
        for start, begun, finish in [
            (10.45, 10.5, 10.6),
            (15.1, 15.2, 15.4),
            (10.4, 10.5, 10.6),
            (15.3, 15.4, 15.5),
        ]
    ]
    result = benchmark._measure(**inputs)['groups']['bulk-eager']
    assert result['transactions'] == 2
    assert result['acquire_ms_per_event'] == pytest.approx(150 / 7000)
    assert result['hold_ms_per_event'] == pytest.approx(300 / 7000)


@pytest.mark.parametrize('change', ['duplicate', 'omit', 'reorder', 'attempt', 'replay'])
def test_delivery_validation_rejects_identity_order_attempt_or_health_error(change: str) -> None:
    inputs = _observations()
    rows = inputs['records']['bulk-eager']
    if change == 'duplicate':
        rows.insert(3, dict(rows[2]))
    elif change == 'omit':
        rows.pop(3)
    elif change == 'reorder':
        rows[2], rows[3] = rows[3], rows[2]
    elif change == 'attempt':
        rows[3]['delivery_attempt'] = 2
    else:
        rows[3]['is_operator_replay'] = True
    assert benchmark._measure(**inputs)['status'] == 'failed'


@pytest.mark.parametrize(
    'change', ['readiness', 'liveness', 'group', 'source', 'worker', 'missing']
)
def test_delivery_final_health_requires_every_component(change: str) -> None:
    status = _healthy_status()
    assert benchmark._healthy(status, ['bulk-eager'], 1_024_000)
    if change in ('readiness', 'liveness'):
        status[change] = False
    elif change == 'group':
        status['groups'][0]['conditions'] = ['write-lock-stalled']
    elif change == 'source':
        status['sources'][0]['conditions'] = ['checkpoint-stalled']
    elif change == 'worker':
        status['workers'][0] = {
            'site': 'main',
            'worker_id': 'stopped',
            'breaker_open': True,
            'breaker_reason': 'worker-exit-1',
        }
    else:
        status['groups'].clear()
    assert not benchmark._healthy(status, ['bulk-eager'], 1_024_000)


@pytest.mark.parametrize('status', ['failed', 'too-short', 'ineligible-host', 'passed'])
def test_delivery_cli_exit_requires_pass(
    status: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        sys, 'argv', ['delivery_speed', '--groups', 'alone', '--work-dir', str(tmp_path)]
    )

    def run(**kwargs: object) -> dict[str, object]:
        assert kwargs['burst'] == 10_000
        return {
            'status': status,
            'recovery_run': status == 'failed',
            'groups': {'bulk-eager': {'rate': 1400}},
        }

    monkeypatch.setattr(benchmark, 'run_benchmark', run)
    assert benchmark._main() == (0 if status == 'passed' else 1)


@cache
def _sequence_measure(
    paired: bool, rates: tuple[int, ...]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    inputs = _observations(paired=paired, counts=tuple(rate * 5 for rate in rates))
    inputs['transactions'] = [
        {
            'thread': f'sg-event-daemon:lane:{group}',
            'started_at': 11.0,
            'begun_at': 11.1,
            'finished_at': 11.3,
            'committed': True,
        }
        for group in inputs['records']
    ]
    report = benchmark._measure(**inputs)
    return report, inputs['host_samples']


def _sequence_attempt(
    slot: int,
    *,
    index: int | None = None,
    rates: tuple[int, ...] | None = None,
    burst: int = 10000,
) -> dict[str, Any]:
    paired = slot not in (1, 4, 7)
    rates = rates or ((1000, 1000) if paired else (1400,))
    offset = 100.0 * ((index or slot) - 1)
    report, host_samples = copy.deepcopy(_sequence_measure(paired, rates))
    for sample in host_samples:
        sample['monotonic'] += offset + 40
    report['interval']['wall_offset'] -= offset + 40
    report['metadata'] = {
        'paired': paired,
        'warmup': 4000,
        'burst': burst,
        'page_delay_secs': 0.0,
        'sampler_interval_secs': 1.0,
        'launch_monotonic': offset + 41,
        'finish_monotonic': offset + 60,
        'host_samples': host_samples,
        'exit_code': 0,
        'process_group_gone': True,
        'cleanup_clean': True,
    }
    report['validation'].update(
        dict.fromkeys(
            (
                'workload',
                'complete_delivery',
                'healthy_final_status',
                'clean_exit',
                'all_children_instrumented',
                'transaction_evidence',
                'database_integrity_cleanup',
            ),
            True,
        )
    )
    return {
        'slot': slot,
        'attempt': 1,
        'burst': burst,
        'raw_report': f'run-{index or slot}/report.json',
        'report': report,
        'preflight': {
            'grid_origin': offset,
            'interval_secs': 30.0,
            'errors': [],
            'observations': [
                {
                    'kind': kind,
                    'grid_index': tick,
                    'started_monotonic': offset + started,
                    'finished_monotonic': offset + finished,
                    'sample': {
                        'monotonic': offset + finished,
                        'busy_percent': 60.0,
                        'error': None,
                    },
                }
                for kind, tick, started, finished in (
                    ('scheduled', 0, 0, 2),
                    ('stop', 0, 2, 4),
                    ('scheduled', 1, 30, 34),
                    ('stop', 1, 34, 36),
                )
            ],
        },
        'started_monotonic': offset + 40,
        'finished_monotonic': offset + 61,
        'exit_code': 0 if report['status'] == 'passed' else 2,
        'identity': {
            'session': 'synthetic-session',
            'code': 'code-digest',
            'environment': 'environment-digest',
            'instrumentation': 'instrumentation-digest',
            'configuration': {
                'workload': 'fixed-workload-digest',
                'paired': paired,
                'burst': burst,
                'work_dir': f'run-{index or slot}',
            },
        },
    }


def _assess(
    attempts: list[dict[str, Any]], *, start: float = 0, finish: float = 800
) -> dict[str, Any]:
    return benchmark.assess_sequence(
        attempts=attempts, start_monotonic=start, finish_monotonic=finish
    )


def _assert_missing_publication_assessment(report: dict[str, Any]) -> None:
    attempt = _sequence_attempt(1, burst=20000)
    attempt['report'] = report
    attempt['started_monotonic'] = 0.0
    attempt['finished_monotonic'] = 20.0
    attempt['exit_code'] = 2
    preflight = attempt['preflight']
    preflight['grid_origin'] = -40.0
    for observation in preflight['observations']:
        observation['started_monotonic'] -= 40
        observation['finished_monotonic'] -= 40
        observation['sample']['monotonic'] -= 40
    result = _assess([attempt], start=-40, finish=20)
    axes = result['attempts'][0]['classification']
    assert axes['interval'] == 'missing'
    assert axes['correctness'] == 'passed'
    assert result['attempts'][0]['rates'] is None
    # Actual analyzer report: derivative transaction_evidence=False alone is not
    # corruption; absent independent transaction coverage must remain a failure.
    report['transaction_record_groups'] = []
    result = _assess([attempt], start=-40, finish=20)
    assert result['attempts'][0]['classification']['correctness'] == 'failed'


def _sequence() -> list[dict[str, Any]]:
    return [_sequence_attempt(slot) for slot in range(1, 9)]


def _short_sequence_attempt(duration: float) -> dict[str, Any]:
    attempt = _sequence_attempt(1)
    metadata = attempt['report']['metadata']
    validations = attempt['report']['validation']
    inputs = _observations(counts=(0,), duration=duration)
    inputs['transactions'] = [
        {
            'thread': 'sg-event-daemon:lane:bulk-eager',
            'started_at': 5,
            'begun_at': 5.1,
            'finished_at': 5.2,
            'committed': True,
        }
    ]
    report = benchmark._measure(**inputs)
    report['metadata'] = metadata
    report['interval']['wall_offset'] -= 40
    report['validation'].update(
        {name: value for name, value in validations.items() if name not in report['validation']}
    )
    report['validation']['transaction_evidence'] = False
    report['transaction_record_groups'] = ['bulk-eager']
    attempt['report'] = report
    attempt['exit_code'] = 2
    return attempt


def test_delivery_analyzer_rechecks_retained_health_counts_and_integrity(tmp_path: Path) -> None:
    inputs = _observations()
    measured = inputs['records']['bulk-eager'][2:]
    timestamps = [100.0] * 4000 + [110.1] * 12000 + [row['delivered_at'] for row in measured]
    assert len(timestamps) == 24000
    rows = [
        {
            'event_id': 1_000_001 + index,
            'delivered_at': stamp,
            'delivery_attempt': 1,
            'is_operator_replay': False,
        }
        for index, stamp in enumerate(timestamps)
    ]
    probe = tmp_path / 'bulk-eager.jsonl'
    probe.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    metadata = {
        'paired': False,
        'warmup': 4000,
        'burst': 20000,
        'burst_release_wall': 101.0,
        'burst_release_monotonic': 1.0,
        'host_samples': inputs['host_samples'],
        'launch_monotonic': 1.0,
        'finish_monotonic': 20.0,
        'exit_code': 0,
        'process_group_gone': True,
        'cleanup_clean': True,
    }
    (tmp_path / 'run.json').write_text(json.dumps(metadata))
    (tmp_path / 'status-final.json').write_text(json.dumps(_healthy_status()))
    for pid, command in [(1, 'supervise'), (2, 'run-worker'), (3, 'run-worker')]:
        (tmp_path / f'instrumented-{pid}.json').write_text(
            json.dumps({'pid': pid, 'argv': [command], 'network_guard': True})
        )
        (tmp_path / f'timings-{pid}.json').write_text(
            json.dumps(
                {
                    'pid': pid,
                    'publications': inputs['publications'] if pid == 2 else [],
                    'transactions': [
                        {
                            'thread': 'sg-event-daemon:lane:bulk-eager',
                            'started_at': 11,
                            'begun_at': 11.1,
                            'finished_at': 11.2,
                            'committed': True,
                        }
                    ]
                    if pid == 3
                    else [],
                }
            )
        )
    state = tmp_path / 'state'
    state.mkdir()
    for filename in ('supervisor.db', 'events.db'):
        connection = sqlite3.connect(state / filename)
        connection.execute('CREATE TABLE epochs (id INTEGER)')
        connection.close()
    report = benchmark.analyze_run(tmp_path)
    assert report['status'] == 'passed', report
    validation = report['validation']
    assert isinstance(validation, dict)
    assert validation['transaction_evidence'] is True
    _assert_recovered_analyzer(tmp_path)
    publication_path = tmp_path / 'timings-2.json'
    published = publication_path.read_text()
    missing_publication = json.loads(published)
    missing_publication['publications'] = []
    publication_path.write_text(json.dumps(missing_publication))
    report = benchmark.analyze_run(tmp_path)
    assert report['status'] == 'failed'
    assert report['groups'] == {}
    validation = report['validation']
    assert isinstance(validation, dict)
    assert validation['transaction_evidence'] is False
    _assert_missing_publication_assessment(report)
    publication_path.write_text(published)
    status = _healthy_status()
    status['readiness'] = False
    (tmp_path / 'status-final.json').write_text(json.dumps(status))
    assert benchmark.analyze_run(tmp_path)['status'] == 'failed'
    (tmp_path / 'status-final.json').write_text(json.dumps(_healthy_status()))
    probe.write_text(''.join(json.dumps(row) + '\n' for row in rows[:-1]))
    assert benchmark.analyze_run(tmp_path)['status'] == 'failed'
    probe.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    connection = sqlite3.connect(state / 'events.db')
    connection.execute('INSERT INTO epochs VALUES (1)')
    connection.commit()
    connection.close()
    assert benchmark.analyze_run(tmp_path)['status'] == 'failed'


def test_delivery_sequence_fallback_retains_original_budget_and_attempts() -> None:
    attempts = _sequence()
    first = attempts[0]['report']['interval']
    first['duration_secs'] = 2.0
    first['end_wall'] = first['start_wall'] + 2
    fallback = [_sequence_attempt(slot, index=slot + 8, burst=12000) for slot in range(1, 9)]
    result = _assess([*attempts, *fallback], finish=1600)
    assert result['delivery_gate'] == 'passed'
    assert result['selected_burst'] == 12000
    assert result['launch_count'] == 16
    assert len(result['attempts']) == 16
    assert result['deadline_monotonic'] == 1800
    attempts[0] = _sequence_attempt(1)
    assert _assess([*attempts, *fallback], finish=1600)['delivery_gate'] == 'unfulfilled'
    attempts[0] = _sequence_attempt(1, rates=(100,))
    attempts[3] = _sequence_attempt(4, rates=(100,))
    assert _assess([*attempts, *fallback], finish=1600)['delivery_gate'] == 'unfulfilled'


def test_delivery_sequence_replaced_stress_still_prohibits_fallback() -> None:
    first = _sequence_attempt(1)
    first['report']['metadata']['host_samples'][1]['busy_percent'] = 70
    attempts = [first, *[_sequence_attempt(slot, index=slot + 1) for slot in range(1, 9)]]
    attempts[1]['attempt'] = 2
    short = attempts[2]['report']['interval']
    short['duration_secs'] = 2
    short['end_wall'] = short['start_wall'] + 2
    attempts.extend(_sequence_attempt(slot, index=slot + 9, burst=12000) for slot in range(1, 9))
    result = _assess(attempts, finish=1700)
    assert result['outcome'] == 'invalid-evidence'
    assert 'unauthorized size fallback or restart' in result['reasons']
    assert result['launch_count'] == 17
    assert result['qualifying_aggregate'] is None


@pytest.mark.parametrize(
    'change',
    [
        'missing',
        'duplicate',
        'reorder',
        'overlap',
        'mode',
        'burst',
        'session',
        'code',
        'environment',
        'instrumentation',
        'configuration',
        'nan',
        'empty-validation',
        'correctness',
        'analysis',
        'cleanup',
        'metadata-time',
        'report-reuse',
    ],
)
def test_delivery_sequence_requires_order_and_complete_identity(change: str) -> None:
    attempts = _sequence()
    row = attempts[1]
    if change == 'missing':
        attempts.pop(1)
    elif change == 'duplicate':
        row['slot'] = 1
    elif change == 'reorder':
        attempts[1], attempts[2] = attempts[2], attempts[1]
    elif change == 'overlap':
        row['started_monotonic'] = 50
    else:
        mutations: dict[str, tuple[dict[str, Any], str, Any]] = {
            'mode': (row['report']['metadata'], 'paired', False),
            'burst': (row, 'burst', 12000),
            'session': (row['identity'], 'session', 'changed'),
            'code': (row['identity'], 'code', 'changed'),
            'environment': (row['identity'], 'environment', 'changed'),
            'instrumentation': (row['identity'], 'instrumentation', 'changed'),
            'configuration': (row['identity']['configuration'], 'workload', 'changed'),
            'nan': (row['report']['groups']['bulk-eager'], 'rate', float('nan')),
            'empty-validation': (row['report'], 'validation', {}),
            'correctness': (row['report']['validation'], 'bulk-eager:ids_order_once', False),
            'analysis': (row['report'], 'analysis_error', 'missing evidence'),
            'cleanup': (row['report']['metadata'], 'cleanup_clean', False),
            'metadata-time': (row['report']['metadata'], 'finish_monotonic', 180),
            'report-reuse': (row, 'raw_report', attempts[0]['raw_report']),
        }
        target, key, value = mutations[change]
        target[key] = value
    assert _assess(attempts)['delivery_gate'] == 'unfulfilled'


@pytest.mark.parametrize(
    'change',
    [
        'valid',
        'variable-duration',
        'one',
        'stale',
        'missed',
        'error',
        'nan',
        'loaded-stop',
        'stop-only',
        'loaded-between',
    ],
)
def test_delivery_sequence_requires_quiet_preflight(change: str) -> None:
    attempts = _sequence()
    preflight = attempts[0]['preflight']
    rows = preflight['observations']
    if change == 'variable-duration':
        # Schedule starts 0, 30, 60; completion gaps are 32 then 27, not exactly 30.
        rows.extend(
            [
                {
                    'kind': 'scheduled',
                    'grid_index': 2,
                    'started_monotonic': 60,
                    'finished_monotonic': 61,
                    'sample': {'monotonic': 61, 'busy_percent': 60, 'error': None},
                },
            ]
        )
        attempts[0]['started_monotonic'] = 62
        attempts[0]['finished_monotonic'] = 83
        metadata = attempts[0]['report']['metadata']
        metadata['launch_monotonic'] += 22
        metadata['finish_monotonic'] += 22
        for sample in metadata['host_samples']:
            sample['monotonic'] += 22
        attempts[0]['report']['interval']['wall_offset'] -= 22
    elif change == 'one':
        del rows[2:]
    elif change == 'stale':
        attempts[1]['preflight'] = preflight
    elif change == 'missed':
        rows[2]['grid_index'] = 2
    elif change == 'error':
        rows[2]['sample']['error'] = 'capture failed'
    elif change == 'nan':
        rows[2]['sample']['busy_percent'] = float('nan')
    elif change in ('loaded-stop', 'loaded-between'):
        rows[-1 if change == 'loaded-stop' else 1]['sample']['busy_percent'] = 60.1
    elif change == 'stop-only':
        rows[2]['kind'] = 'stop'
    result = _assess(attempts)
    assert result['delivery_gate'] == (
        'passed' if change in ('valid', 'variable-duration') else 'unfulfilled'
    )


@pytest.mark.parametrize(
    'exclusion', ['host', 'low', 'capacity', 'publication', 'short', 'correctness']
)
def test_delivery_sequence_replacement_and_exclusions(exclusion: str) -> None:
    attempts = [
        _sequence_attempt(1),
        *[_sequence_attempt(slot, index=slot + 1) for slot in range(1, 9)],
    ]
    attempts[1]['attempt'] = 2
    first = attempts[0]['report']
    if exclusion in ('host', 'correctness'):
        first['metadata']['host_samples'][1]['busy_percent'] = 70
    if exclusion == 'low':
        attempts[0] = _sequence_attempt(1, rates=(100,))
    elif exclusion == 'capacity':
        first['capacity_detaches'] = [{'reason': 'queue-count-exhausted'}]
    elif exclusion == 'publication':
        first.pop('interval')
        first['groups'] = {}
        first['validation']['all_group_publication'] = False
        first['validation']['transaction_evidence'] = False
        first['transaction_record_groups'] = ['bulk-eager']
    elif exclusion == 'short':
        first['interval']['duration_secs'] = 2
        first['interval']['end_wall'] = first['interval']['start_wall'] + 2
    elif exclusion == 'correctness':
        first['validation']['bulk-eager:ids_order_once'] = False
    result = _assess(attempts, finish=900)
    assert result['delivery_gate'] == ('passed' if exclusion == 'host' else 'unfulfilled')
    if exclusion in ('capacity', 'publication', 'short'):
        assert result['attempts'][0]['measured'] is False
        assert result['attempts'][0]['rates'] is None
    if exclusion == 'correctness':
        assert result['attempts'][0]['classification']['correctness'] == 'failed'
    if exclusion == 'host':
        assert result['attempts'][0]['classification']['host'] == 'stress'
        attempts.insert(2, _sequence_attempt(1, index=3))
        attempts[2]['attempt'] = 3
        assert _assess(attempts, finish=1000)['delivery_gate'] == 'unfulfilled'


@pytest.mark.parametrize(
    'change',
    [
        'empty',
        'partial',
        'too-many',
        'deadline-launch',
        'deadline-finish',
        'reset',
        'exact-finish',
    ],
)
def test_delivery_sequence_keeps_one_budget(change: str) -> None:
    attempts = _sequence()
    start, finish = 0.0, 800.0
    if change == 'empty':
        attempts = []
    elif change == 'partial':
        attempts.pop()
    elif change == 'too-many':
        attempts *= 3
    elif change == 'deadline-launch':
        attempts[-1]['started_monotonic'] = 1800
        attempts[-1]['finished_monotonic'] = 1801
        finish = 1801
    elif change == 'deadline-finish':
        attempts[-1]['finished_monotonic'] = 1800.1
        finish = 1800.1
    elif change == 'reset':
        start = 1
    else:
        attempts[-1]['finished_monotonic'] = 1800
        finish = 1800
    result = _assess(attempts, start=start, finish=finish)
    assert result['launch_count'] == len(attempts)
    assert len(result['attempts']) == len(attempts)
    assert result['delivery_gate'] == ('passed' if change == 'exact-finish' else 'unfulfilled')
    if change in ('empty', 'partial'):
        assert result['alone']['partial'] is True
        assert result['qualifying_aggregate'] is None


@pytest.mark.parametrize(
    'change',
    [
        'missing-identity',
        'empty-preflight',
        'missing-validation',
        'timing-validation',
        'preflight-error',
    ],
)
def test_delivery_sequence_rejects_missing_evidence_without_throwing(change: str) -> None:
    attempts = _sequence()
    row = attempts[1]
    if change == 'missing-identity':
        row.pop('identity')
    elif change == 'empty-preflight':
        row['preflight']['observations'] = []
    elif change == 'missing-validation':
        row['report']['validation'].pop('clean_exit')
    elif change == 'timing-validation':
        row['report']['validation'].pop('bulk-eager:timing_order')
    else:
        row['preflight']['errors'] = ['stop failed']
    assert _assess(attempts)['delivery_gate'] == 'unfulfilled'


@pytest.mark.parametrize('kind', ['missed', 'error', 'loaded'])
def test_delivery_sequence_preflight_pair_can_recover_after_reset(kind: str) -> None:
    attempts = _sequence()
    # Use the already valid 0/30 pair after an earlier failed observation;
    # both invalid and later quiet samples belong to the original budget.
    preflight = attempts[0]['preflight']
    preflight['grid_origin'] = -30
    for row in preflight['observations']:
        row['grid_index'] += 1
    earlier: dict[str, Any] = {
        'kind': 'scheduled',
        'grid_index': 0,
        'started_monotonic': -30,
        'finished_monotonic': -28,
        'sample': {'monotonic': -28, 'busy_percent': 70, 'error': None},
    }
    if kind == 'missed':
        earlier['kind'] = 'missed'
        earlier['sample'] = None
    elif kind == 'error':
        earlier['sample'] = {'monotonic': -28, 'busy_percent': None, 'error': 'capture failed'}
    preflight['observations'].insert(0, earlier)
    result = _assess(attempts, start=-30)
    assert result['delivery_gate'] == 'passed'
    assert result['deadline_monotonic'] == 1770
    assert result['attempts'][0]['preflight_check']['scheduled_pair'] == [1, 3]


@pytest.mark.parametrize('duration', [-1.0, 0.0, 2.0])
def test_delivery_sequence_short_analyzer_shape_is_an_absent_measurement(duration: float) -> None:
    attempt = _short_sequence_attempt(duration)
    report = attempt['report']
    result = _assess([attempt])
    assert result['outcome'] == 'measurement-limitation'
    assert result['attempts'][0]['classification']['interval'] == 'short'
    assert result['attempts'][0]['classification']['correctness'] == 'passed'
    assert result['attempts'][0]['rates'] is None
    report['transaction_record_groups'] = []
    result = _assess([attempt])
    assert result['outcome'] == 'correctness-failure'


@pytest.mark.parametrize('duration', [-1.0, 0.0, 2.0])
@pytest.mark.parametrize(
    'change',
    [
        'none',
        'stress',
        'loaded',
        'capacity',
        'publication',
        'correctness',
        'empty',
        'timestamp',
        'unordered',
        'error',
        'nan',
        'missing-before',
        'missing-final',
        'preflight-empty',
        'preflight-error',
        'preflight-loaded',
    ],
)
def test_delivery_sequence_fallback_requires_quiet_lifetime_evidence(
    duration: float, change: str
) -> None:
    attempts = _sequence()
    attempts[0] = _short_sequence_attempt(duration)
    first = attempts[0]
    report = first['report']
    samples = report['metadata']['host_samples']
    _change_lifetime_samples(samples, change)
    if change == 'capacity':
        report['capacity_detaches'] = [{'reason': 'queue-count-exhausted'}]
    elif change == 'publication':
        report.pop('interval')
        report['groups'] = {}
        report['validation']['all_group_publication'] = False
    elif change == 'correctness':
        report['validation']['bulk-eager:ids_order_once'] = False
    elif change == 'preflight-empty':
        first['preflight']['observations'] = []
    elif change == 'preflight-error':
        first['preflight']['errors'] = ['stop failed']
    elif change == 'preflight-loaded':
        first['preflight']['observations'][-1]['sample']['busy_percent'] = 70
    # Even a positive short interval need not contain a sample. This absence
    # grants neither a rate nor a host-only replacement, but is not host load.
    if change == 'none':
        samples.pop(1)
    fallback = [_sequence_attempt(slot, index=slot + 8, burst=12000) for slot in range(1, 9)]
    result = _assess([*attempts, *fallback], finish=1600)
    assert result['delivery_gate'] == ('passed' if change == 'none' else 'unfulfilled')
    assert result['launch_count'] == len(result['attempts']) == 16
    assert result['deadline_monotonic'] == 1800
    assert result['attempts'][0]['rates'] is None
    assert result['attempts'][0]['measured'] is False
    if change == 'none':
        assert result['attempts'][0]['host_check']['eligible'] is False
        assert result['attempts'][0]['host_check']['lifetime_eligible'] is True
        replacement = _sequence_attempt(1, index=2)
        replacement['attempt'] = 2
        assert _assess([first, replacement])['outcome'] == 'invalid-evidence'


@pytest.mark.parametrize('change', ['capacity', 'stress', 'publication', 'interval-sample'])
def test_delivery_sequence_fallback_rejects_other_initial_slot_exclusions(change: str) -> None:
    attempts = _sequence()
    attempts[0] = _short_sequence_attempt(2.0)
    report = attempts[1]['report']
    if change == 'capacity':
        report['capacity_detaches'] = [{'reason': 'queue-count-exhausted'}]
    elif change == 'stress':
        report['metadata']['host_samples'][1]['busy_percent'] = 70
    elif change == 'interval-sample':
        report['metadata']['host_samples'].pop(1)
    else:
        report.pop('interval')
        report['groups'] = {}
        report['validation']['all_group_publication'] = False
        report['validation']['transaction_evidence'] = False
        report['transaction_record_groups'] = ['bulk-eager', 'bulk-strict']
    attempts.extend(_sequence_attempt(slot, index=slot + 8, burst=12000) for slot in range(1, 9))
    result = _assess(attempts, finish=1600)
    assert result['delivery_gate'] == 'unfulfilled'
    assert result['attempts'][1]['classification']['correctness'] == 'passed'
    assert 'unauthorized size fallback or restart' in result['reasons']


def test_delivery_sequence_uses_median_of_paired_minima() -> None:
    rates = [
        (100,),
        (300, 1200),
        (1300, 400),
        (1400,),
        (1000, 1500),
        (1600, 1000),
        (1500,),
        (1000, 1000),
    ]
    attempts = [_sequence_attempt(slot, rates=pair) for slot, pair in enumerate(rates, 1)]
    result = _assess(attempts)
    assert result['delivery_gate'] == 'passed'
    assert result['alone'] == {
        'rates': [100, 1400, 1500],
        'median': 1400,
        'minimum': 100,
        'maximum': 1500,
        'partial': False,
    }
    assert result['paired']['rates'] == [300, 400, 1000, 1000, 1000]
    assert result['paired']['median'] == 1000
    assert result['paired']['minimum'] == 300
    assert result['paired']['maximum'] == 1000
    assert result['groups']['paired']['bulk-eager']['rates'] == [300, 1300, 1000, 1600, 1000]
    assert result['attempts'][0]['report']['groups']['bulk-eager']['callbacks'] == 500
    assert result['attempts'][0]['report']['groups']['bulk-eager'][
        'acquire_ms_per_event'
    ] == pytest.approx(0.2)
    assert result['attempts'][0]['report']['groups']['bulk-eager'][
        'hold_ms_per_event'
    ] == pytest.approx(0.4)
    assert result['attempts'][0]['report']['status'] == 'failed'
    assert len(result['selected']) == 8
    for slot, pair in zip(
        (2, 3, 5, 6, 8),
        ((800, 1500), (900, 1500), (1500, 900), (1500, 800), (1500, 1500)),
        strict=True,
    ):
        attempts[slot - 1] = _sequence_attempt(slot, rates=pair)
    result = _assess(attempts)
    assert result['outcome'] == 'floor-miss'
    assert result['paired']['median'] == 900
    assert result['groups']['paired']['bulk-eager']['median'] == 1500
    assert result['groups']['paired']['bulk-strict']['median'] == 1500
    attempts = _sequence()
    attempts[0] = _sequence_attempt(1, rates=(100,))
    attempts[3] = _sequence_attempt(4, rates=(1399,))
    assert _assess(attempts)['outcome'] == 'floor-miss'


@pytest.mark.parametrize(
    'change',
    ['none', 'timestamp', 'unordered', 'missing-before', 'missing-interval', 'missing-final'],
)
def test_delivery_stress_requires_structurally_valid_host_samples(change: str) -> None:
    inputs = _observations()
    samples = inputs['host_samples']
    for sample in samples:
        sample['busy_percent'] = 70
    if change == 'timestamp':
        samples[1]['monotonic'] = None
    elif change == 'unordered':
        samples[0], samples[1] = samples[1], samples[0]
    elif change == 'missing-before':
        samples.pop(0)
    elif change == 'missing-interval':
        samples.pop(1)
    elif change == 'missing-final':
        samples.pop()
    result = benchmark._measure(**inputs)
    assert result['host']['eligible'] is False
    assert result['host']['classification'] == ('stress' if change == 'none' else 'ineligible')
