from __future__ import annotations

import subprocess
import threading
import time
from dataclasses import asdict
from typing import TYPE_CHECKING

import pytest

from tests import _host_sampling as host_sampling

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


@pytest.mark.parametrize(
    ('durations', 'expected'),
    [((1.0, 1.0, 1.0), (1.0, 6.0, 11.0)), ((6.0, 6.0, 1.0), (6.0, 16.0, 21.0))],
)
def test_host_sampler_anchors_cadence_before_initial_capture(
    monkeypatch: pytest.MonkeyPatch,
    durations: tuple[float, ...],
    expected: tuple[float, ...],
) -> None:
    now = 0.0
    captures = iter(durations)
    sampler = host_sampling.HostSampler(10)

    def capture() -> host_sampling.HostSample:
        nonlocal now
        now += next(captures)
        return host_sampling.HostSample(now, now, 20.0, 1.5, (), None)

    def wait(timeout: float) -> bool:
        nonlocal now
        if len(sampler.samples()) == 3:
            return True
        now += timeout
        return False

    monkeypatch.setattr(host_sampling.time, 'monotonic', lambda: now)
    monkeypatch.setattr(sampler, 'capture', capture)
    monkeypatch.setattr(sampler.stop_event, 'wait', wait)
    monkeypatch.setattr(sampler.thread, 'start', sampler.run)
    sampler.start()
    assert tuple(sample.monotonic for sample in sampler.samples()) == expected


def test_host_sampler_keeps_five_second_grid_and_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert host_sampling.HostSampler(10).interval_secs == 5.0
    calls = 0
    periodic = threading.Event()

    def capture(_self: host_sampling.HostSampler) -> host_sampling.HostSample:
        nonlocal calls
        calls += 1
        if calls == 3:
            periodic.set()
        now = time.monotonic()
        if calls == 2:
            return host_sampling.HostSample(now, now, None, None, (), 'read failed')
        return host_sampling.HostSample(now, now, 20.0, 1.5, (), None)

    monkeypatch.setattr(host_sampling.HostSampler, 'capture', capture)
    sampler = host_sampling.HostSampler(10, interval_secs=0.02)
    sampler.start()
    assert periodic.wait()
    sampler.stop()
    samples: Sequence[host_sampling.HostSample] = sampler.samples()
    assert len(samples) >= 4
    assert samples[1].error == 'read failed'
    assert samples[1].busy_percent is None
    assert samples[1].load_average_1m is None
    assert not sampler.thread.is_alive()
    sampler.stop()
    assert sampler.samples() == tuple(samples)


def test_host_sampler_records_only_outside_processes_over_half_a_core() -> None:
    rows = (
        '10 1 99.0 /private/harness --secret\n'
        '20 10 70.0 /private/daemon\n'
        '21 20 80.0 /private/worker\n'
        '22 10 60.0 /usr/bin/ps\n'
        '30 1 50.0 /private/equal\n'
        '31 1 50.1 /private/other\n'
        '32 1 150.0 /private/busy\n'
    )
    processes = host_sampling.outside_processes(10, rows)
    assert [asdict(process) for process in processes] == [
        {'pid': 31, 'executable': 'other', 'cpu_percent': 50.1},
        {'pid': 32, 'executable': 'busy', 'cpu_percent': 150.0},
    ]


def test_host_sampler_reports_macos_and_linux_cpu_intervals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    top_output = (
        'CPU usage: 28.0% user, 32.0% sys, 40.0% idle\n'
        'CPU usage: 9.0% user, 11.0% sys, 80.0% idle\n'
    )

    def run(_command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 0, stdout=top_output)

    monkeypatch.setattr(host_sampling.subprocess, 'run', run)
    assert host_sampling.read_macos_busy_percent() == 20.0
    readings = iter(('cpu 100 100 0 800\n', 'cpu 200 200 0 1600\n'))

    def read(_path: host_sampling.Path, **_kwargs: object) -> str:
        return next(readings)

    monkeypatch.setattr(host_sampling.Path, 'read_text', read)
    before = host_sampling.read_linux_counters()
    after = host_sampling.read_linux_counters()
    assert host_sampling.busy_from_counters(before, after) == 20.0
    assert host_sampling.busy_from_counters(after, after) is None

    def ps(_command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess([], 0, stdout='10 1 0.0 /private/harness\n')

    monkeypatch.setattr(host_sampling, 'read_busy_percent', lambda: 20.0)
    monkeypatch.setattr(host_sampling.os, 'getloadavg', lambda: (3.0, 2.0, 1.0))
    monkeypatch.setattr(host_sampling.subprocess, 'run', ps)
    with monkeypatch.context() as patch:
        patch.setattr(host_sampling.sys, 'platform', 'darwin')
        sample = host_sampling.HostSampler(10).capture()
    assert sample.busy_percent == 20.0
    assert sample.load_average_1m == 3.0

    def fail() -> float:
        raise PermissionError('unavailable')

    monkeypatch.setattr(host_sampling, 'read_busy_percent', fail)
    with monkeypatch.context() as patch:
        patch.setattr(host_sampling.sys, 'platform', 'darwin')
        unavailable = host_sampling.HostSampler(10).capture()
    assert unavailable.busy_percent is None
    assert unavailable.load_average_1m is None
    assert unavailable.error == 'PermissionError'


def test_linux_outside_cpu_uses_current_interval_instead_of_lifetime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    processes = {
        10: ('harness', 1, 100),
        20: ('worker', 10, 200),
        30: ('old busy', 1, 300),
        31: ('new busy', 1, 400),
        32: ('half core', 1, 500),
        33: ('reused pid', 1, 600),
        34: ('below half', 1, 700),
        35: ('above half', 1, 800),
    }

    def write_snapshot(cpu_ticks: dict[int, int], *, reused_start: int = 600) -> None:
        for pid, (name, parent, start) in processes.items():
            directory = tmp_path / str(pid)
            directory.mkdir(exist_ok=True)
            fields = [
                'S',
                str(parent),
                *(['0'] * 9),
                str(cpu_ticks[pid]),
                '0',
                *(['0'] * 6),
                str(reused_start if pid == 33 else start),
            ]
            (directory / 'stat').write_text(
                f'{pid} ({name}) ' + ' '.join(fields) + '\n', encoding='ascii'
            )

    write_snapshot(dict.fromkeys(processes, 1000))
    ticks = iter((10.0, 10.1, 10.1))
    ticks_ns = iter((10_000_000_000, 10_100_000_000))

    def advance() -> float:
        write_snapshot(
            {
                10: 1010,
                20: 1010,
                30: 1000,
                31: 1010,
                32: 1005,
                33: 1010,
                34: 1004,
                35: 1006,
            },
            reused_start=601,
        )
        return 20.0

    def reject_ps(_command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise AssertionError('Linux process sampling must read native counters')

    def ticks_per_second(_name: str) -> int:
        return 100

    with monkeypatch.context() as patch:
        patch.setattr(host_sampling.sys, 'platform', 'linux')
        patch.setattr(host_sampling, 'PROC_ROOT', tmp_path, raising=False)
        patch.setattr(host_sampling, 'read_busy_percent', advance)
        patch.setattr(host_sampling.os, 'getloadavg', lambda: (3.0, 2.0, 1.0))
        patch.setattr(host_sampling.os, 'sysconf', ticks_per_second)
        patch.setattr(host_sampling.subprocess, 'run', reject_ps)
        patch.setattr(host_sampling.time, 'monotonic', lambda: next(ticks))
        patch.setattr(host_sampling.time, 'monotonic_ns', lambda: next(ticks_ns))
        sample = host_sampling.HostSampler(10).capture()

    assert sample.busy_percent == 20.0
    assert sample.load_average_1m == 3.0
    assert set(sample.outside_processes) == {
        host_sampling.HostProcess(31, 'new busy', 100.0),
        host_sampling.HostProcess(35, 'above half', 60.0),
    }
