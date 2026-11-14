from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

PROC_ROOT = Path('/proc')


@dataclass(frozen=True)
class HostProcess:
    pid: int
    executable: str
    cpu_percent: float


@dataclass(frozen=True)
class HostSample:
    monotonic: float
    wall_time: float
    busy_percent: float | None
    load_average_1m: float | None
    outside_processes: tuple[HostProcess, ...]
    error: str | None


class HostSampler:
    def __init__(self, root_pid: int, *, interval_secs: float = 5.0) -> None:
        self._root_pid = root_pid
        self.interval_secs = interval_secs
        self._samples: list[HostSample] = []
        self._lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self._started = False
        self._thread_started = False
        self._stopped = False
        self._first_capture_at = 0.0

    def _append(self, sample: HostSample) -> None:
        with self._lock:
            self._samples.append(sample)

    def capture(self) -> HostSample:
        try:
            if sys.platform.startswith('linux'):
                before = _read_linux_processes()
                started_ns = time.monotonic_ns()
                busy = read_busy_percent()
                load_average = os.getloadavg()[0]
                after = _read_linux_processes()
                outside = _outside_linux_processes(
                    self._root_pid, before, after, time.monotonic_ns() - started_ns
                )
            else:
                busy = read_busy_percent()
                load_average = os.getloadavg()[0]
                outside = outside_processes(self._root_pid, _read_processes())
        except (OSError, ValueError, IndexError, subprocess.SubprocessError) as error:
            return HostSample(time.monotonic(), time.time(), None, None, (), type(error).__name__)
        return HostSample(time.monotonic(), time.time(), busy, load_average, outside, None)

    def run(self) -> None:
        deadline = self._first_capture_at + self.interval_secs
        while deadline <= time.monotonic():
            deadline += self.interval_secs
        while not self.stop_event.wait(max(0.0, deadline - time.monotonic())):
            self._append(self.capture())
            deadline += self.interval_secs
            while deadline <= time.monotonic():
                deadline += self.interval_secs

    def samples(self) -> tuple[HostSample, ...]:
        with self._lock:
            return tuple(self._samples)

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._first_capture_at = time.monotonic()
        self._append(self.capture())
        self.thread.start()
        self._thread_started = True

    def stop(self) -> None:
        if not self._started or self._stopped:
            return
        self._stopped = True
        self.stop_event.set()
        if self._thread_started:
            self.thread.join()
        self._append(self.capture())


@dataclass(frozen=True)
class _LinuxProcess:
    pid: int
    parent_pid: int
    cpu_ticks: int
    start_ticks: int
    executable: str


def _outside_linux_processes(
    root_pid: int,
    before: dict[int, _LinuxProcess],
    after: dict[int, _LinuxProcess],
    elapsed_ns: int,
) -> tuple[HostProcess, ...]:
    if elapsed_ns <= 0:
        raise ValueError('process CPU interval did not advance')
    ticks_per_second = os.sysconf('SC_CLK_TCK')
    if ticks_per_second <= 0:
        raise ValueError('invalid Linux CPU tick rate')
    descendants = {root_pid}
    while True:
        children = {pid for pid, process in after.items() if process.parent_pid in descendants}
        if children <= descendants:
            break
        descendants.update(children)
    outside: list[HostProcess] = []
    for pid, process in after.items():
        earlier = before.get(pid)
        if pid in descendants or earlier is None or earlier.start_ticks != process.start_ticks:
            continue
        used_ticks = process.cpu_ticks - earlier.cpu_ticks
        if 2 * used_ticks * 1_000_000_000 > ticks_per_second * elapsed_ns:
            cpu_percent = 100.0 * used_ticks * 1_000_000_000 / (ticks_per_second * elapsed_ns)
            outside.append(HostProcess(pid, process.executable, cpu_percent))
    return tuple(outside)


def _parse_linux_process(stat: str) -> _LinuxProcess:
    name_start = stat.index('(')
    name_end = stat.rindex(')')
    fields = stat[name_end + 1 :].split()
    if len(fields) <= 19:
        raise ValueError('incomplete Linux process counters')
    return _LinuxProcess(
        pid=int(stat[:name_start].strip()),
        parent_pid=int(fields[1]),
        cpu_ticks=int(fields[11]) + int(fields[12]),
        start_ticks=int(fields[19]),
        executable=Path(stat[name_start + 1 : name_end]).name,
    )


def _read_linux_processes() -> dict[int, _LinuxProcess]:
    processes: dict[int, _LinuxProcess] = {}
    for directory in PROC_ROOT.iterdir():
        if not directory.name.isdecimal():
            continue
        try:
            stat = (directory / 'stat').read_text(encoding='ascii')
        except FileNotFoundError:
            continue
        process = _parse_linux_process(stat)
        processes[process.pid] = process
    if not processes:
        raise OSError('Linux process counters unavailable')
    return processes


def _read_processes() -> str:
    return subprocess.run(
        ['ps', '-A', '-o', 'pid=,ppid=,%cpu=,comm='],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def busy_from_counters(before: tuple[int, ...], after: tuple[int, ...]) -> float | None:
    # Linux guest time is already counted in user and nice time.
    total = sum(after[:8]) - sum(before[:8])
    if total <= 0:
        return None
    idle = after[3] - before[3]
    if len(before) > 4:
        idle += after[4] - before[4]
    return round(100.0 * (total - idle) / total, 1)


def outside_processes(root_pid: int, output: str) -> tuple[HostProcess, ...]:
    rows: list[tuple[int, int, float, str]] = []
    for line in output.splitlines():
        pid_text, parent_text, cpu_text, executable = line.split(maxsplit=3)
        rows.append((int(pid_text), int(parent_text), float(cpu_text), executable))
    descendants = {root_pid}
    while True:
        children = {pid for pid, parent, _cpu, _name in rows if parent in descendants}
        if children <= descendants:
            break
        descendants.update(children)
    return tuple(
        HostProcess(pid, Path(executable).name, cpu)
        for pid, _parent, cpu, executable in rows
        if pid not in descendants and cpu > 50.0
    )


def read_linux_counters() -> tuple[int, ...]:
    first = Path('/proc/stat').read_text(encoding='ascii').splitlines()[0].split()
    if first[0] != 'cpu':
        raise ValueError('missing aggregate CPU counters')
    return tuple(int(value) for value in first[1:])


def read_macos_busy_percent() -> float:
    output = subprocess.run(
        ['top', '-l', '2', '-s', '1', '-n', '0'],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    intervals = [line for line in output.splitlines() if line.startswith('CPU usage:')]
    if len(intervals) < 2:
        raise ValueError('top did not return an interval CPU reading')
    idle_text = intervals[-1].split('idle', 1)[0].rsplit(',', 1)[-1].strip()
    return round(100.0 - float(idle_text.removesuffix('%')), 1)


def read_busy_percent() -> float:
    if sys.platform == 'darwin':
        return read_macos_busy_percent()
    if sys.platform.startswith('linux'):
        before = read_linux_counters()
        time.sleep(0.1)
        busy = busy_from_counters(before, read_linux_counters())
        if busy is None:
            raise ValueError('CPU counter interval did not advance')
        return busy
    raise OSError('unsupported host platform')
