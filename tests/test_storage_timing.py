from __future__ import annotations

import os
import signal
import sqlite3
import time
from contextlib import closing, contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Literal

import pytest

from sg_event_daemon._health import WatchdogHealth
from sg_event_daemon._platform import release_flock, try_flock_exclusive
from sg_event_daemon._supervisor import Supervisor
from sg_event_daemon._watchdog import ComponentWatchdog
from tests import _worker_processes
from tests._storage_support import source_database
from tests._worker_processes import (
    ChildPatches,
    SupervisorProbe,
    Topology,
    running_supervisor,
    write_daemon_config,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path

    from sg_event_daemon._supervisor import _WorkerHandle

_HOLD = 5.0
_NAMES = ('source:admin', 'lane:named', 'lane:holder')
_REGISTRATION = 'owned'


def _arm_episode(signals: Path, number: str, holder: str, *, joining: bool = False) -> Path:
    episode = signals / number
    episode.mkdir()
    (episode / 'holder').write_text(holder)
    if joining:
        (episode / 'require-joining').touch()
    pending = signals / 'current.pending'
    pending.write_text(number)
    pending.replace(signals / 'current')
    return episode


@contextmanager
def _file_lock(path: Path) -> Generator[None]:
    fd = os.open(f'{path}.write-lock', os.O_RDWR)
    try:
        assert try_flock_exclusive(fd)
        yield
    finally:
        release_flock(fd)
        os.close(fd)


def _go(episode: Path, name: str) -> None:
    (episode / f'{name}.go').touch()


def _rows(episode: Path, name: str) -> list[tuple[str, float, float]]:
    path = episode / f'{name}.writes'
    if not path.exists():
        return []
    return [
        (a, float(b), float(c))
        for a, b, c in (line.split() for line in path.read_text().splitlines())
    ]


def _has(episode: Path, name: str, phase: str) -> bool:
    return any(row[0] == phase for row in _rows(episode, name))


def _phase(episode: Path, name: str, phase: str) -> tuple[str, float, float]:
    return next(row for row in _rows(episode, name) if row[0] == phase)


def _acquisition_deadline(episode: Path, name: str, *, health: float = 1) -> float:
    # The callback observes the phase after acquisition timing has started.
    # Its supplied watch deadline includes exactly one configured health interval.
    return _phase(episode, name, 'write-lock-wait')[2] - health


def _wait(probe: SupervisorProbe, predicate: Callable[[], bool], timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    next_poll = time.monotonic()
    while not predicate():
        now = time.monotonic()
        if now >= deadline:
            details = probe.failure_details()
            (probe.spawns[0].log_path.parent / 'timing-failure.txt').write_text(details)
            raise AssertionError(details)
        if now >= next_poll:
            probe.supervisor._tick()
            next_poll = (
                time.monotonic() + probe.supervisor._config.daemon.supervisor_poll_interval_secs
            )
        time.sleep(0.005)


def _assert_dependent_terminations(
    probe: SupervisorProbe, before: int, expected: dict[str, str], required: tuple[str, ...]
) -> None:
    for spawn in required:
        assert expected[spawn] == 'source-owner-unavailable'
    _wait(
        probe,
        lambda: set(expected) <= {item.spawn_instance for item in probe.terminations[before:]},
    )
    observed = {item.spawn_instance: item.reason for item in probe.terminations[before:]}
    assert {spawn: observed[spawn] for spawn in expected} == expected


def _episode(probe: SupervisorProbe, signals: Path, number: str, holder: str) -> Path:
    episode = _arm_episode(signals, number, holder)
    _wait(probe, lambda: all((episode / f'{name}.ready').exists() for name in _NAMES))
    return episode


@contextmanager
def _fixture(
    tmp_path: Path, *, health: float = 1, poll: float = 0.2, fetch: float = 0.05
) -> Generator[tuple[SupervisorProbe, Path]]:
    config = write_daemon_config(
        tmp_path,
        Topology(
            'named',
            ('named', 'holder'),
            health_interval_secs=health,
            supervisor_poll_interval_secs=poll,
            fetch_interval_secs=fetch,
        ),
    )
    text = config.read_text().replace(
        '[sites.test.groups.holder]\nworker = "pool"',
        '[sites.test.groups.holder]\nworker = "dedicated"',
    )
    text = text.replace(
        '[sites.test.workers.pool]', '[sites.test.workers.pool]\nrestart_limit = 1'
    )
    text = text.replace('worker_startup_deadline_secs = 10.0\n', '')
    config.write_text(text)
    signals = tmp_path / 'signals'
    signals.mkdir()
    with running_supervisor(
        config, ChildPatches(hold_deadline_secs=_HOLD, signal_dir=signals, timing=True)
    ) as probe:
        _wait(
            probe,
            lambda: (
                len(probe.latest_health) == 3
                and all(probe.records(group) for group in ('named', 'holder'))
            ),
            timeout=45,
        )
        assert not probe.terminations
        try:
            yield probe, signals
        finally:
            for episode in signals.iterdir():
                if episode.is_dir():
                    (episode / 'done').touch()
                    (episode / 'release').touch()


def _recover(probe: SupervisorProbe, signals: Path, episode: Path) -> None:
    (episode / 'done').touch()
    _wait(
        probe,
        lambda: (
            len(probe.latest_health) == 3
            and all(
                spawn._termination_reason is None for spawn in probe.supervisor._spawns.values()
            )
        ),
    )
    recovery = _episode(probe, signals, f'{episode.name}-recovery', '')
    for name in _NAMES:
        _go(recovery, name)
    _wait(probe, lambda: all(_has(recovery, name, 'commit') for name in _NAMES))
    (recovery / 'done').touch()
    _wait(
        probe,
        lambda: all(
            any(
                group.group == name and group.join_phase == 'attached'
                for health in probe.latest_health.values()
                for group in health.groups
            )
            for name in ('named', 'holder')
        ),
    )


def _worker(probe: SupervisorProbe, name: str) -> str:
    return next(
        worker_id
        for worker_id, health in probe.latest_health.items()
        if name in health.worker.thread_names
    )


def _block_monitor(probe: SupervisorProbe, episode: Path) -> tuple[float, float]:
    start = _phase(episode, 'lane:named', 'write-lock-wait')[1]
    _wait(probe, lambda: time.monotonic() >= start + 5.5)
    (episode / 'block-monitor').touch()
    _wait(probe, (episode / 'monitor-blocked').exists)
    holder_id = _worker(probe, 'lane:holder')
    health_deadline = probe.supervisor._spawns[holder_id]._health_deadline
    idle = next(
        item
        for item in probe.latest_health[holder_id].watchdogs
        if item.kind == 'lane' and item.group == 'holder'
    )
    idle_deadline = idle.deadline_at
    assert idle.phase == 'idle'
    assert idle_deadline is not None
    assert idle_deadline - idle.heartbeat_at == pytest.approx(6)
    assert idle_deadline - health_deadline > 0.2
    assert (
        _acquisition_deadline(episode, 'lane:named')
        < health_deadline
        < _acquisition_deadline(episode, 'source:admin')
    )
    print(f'blocked deadlines: heartbeat={health_deadline}, idle={idle_deadline}')
    return health_deadline, idle_deadline


def _fallback_episode(
    probe: SupervisorProbe,
    signals: Path,
    order: str,
    number: int,
    dependents: dict[str, dict[str, str]],
) -> None:
    episode = _episode(probe, signals, f'{order}-{number}', 'lane:holder')
    before = len(probe.terminations)
    (episode / 'mask-holder').touch()
    ids = {name: _worker(probe, name) for name in _NAMES}
    original_spawns = {
        name: next(
            spawn.spawn_instance
            for spawn in reversed(probe.spawns)
            if spawn.worker_id == worker_id
        )
        for name, worker_id in ids.items()
    }
    _go(episode, 'lane:holder')
    _wait(probe, (episode / 'lane:holder.stalled').exists)
    first, second = (
        ('source:admin', 'lane:named') if order == 'site-first' else ('lane:named', 'source:admin')
    )
    _go(episode, first)
    _wait(probe, lambda: _has(episode, first, 'write-lock-wait'))
    start = _phase(episode, first, 'write-lock-wait')[1]
    _wait(probe, lambda: time.monotonic() >= start + 3.0)
    _go(episode, second)
    _wait(probe, lambda: _has(episode, second, 'write-lock-wait'))
    assert _phase(episode, second, 'write-lock-wait')[1] - start > 0.2
    blocked_deadlines = _block_monitor(probe, episode) if order == 'blocked-monitor' else None
    _wait(probe, lambda: len(probe.terminations) > before)
    assert probe.terminations[before].worker_id == ids[first], probe.terminations
    assert probe.terminations[before].spawn_instance == original_spawns[first]
    assert _has(episode, first, 'timeout-file')
    _wait(probe, lambda: probe.breakers[ids[first]]._failure_total == number)
    if order != 'site-first':
        assert len(probe.terminations) == before + 1
        assert (
            next(
                spawn for spawn in reversed(probe.spawns) if spawn.worker_id == ids['source:admin']
            ).process.poll()
            is None
        )
    if order == 'blocked-monitor':
        _wait(probe, lambda: len(probe.terminations) == before + 2)
        assert probe.terminations[before + 1].worker_id == ids['lane:holder']
        assert probe.terminations[before + 1].reason == 'worker-heartbeat-overdue'
        assert blocked_deadlines is not None
        assert blocked_deadlines[0] <= probe.terminations[before + 1].at < blocked_deadlines[1]
        assert probe.terminations[before + 1].at < _acquisition_deadline(episode, second)
        _wait(probe, lambda: _has(episode, 'source:admin', 'commit'))
        assert not _has(episode, 'source:admin', 'timeout-file')
        assert probe.breakers[ids['source:admin']]._failure_total == 0
    else:
        _wait(
            probe,
            lambda: any(
                item.spawn_instance == original_spawns['source:admin']
                for item in probe.terminations[before:]
            ),
        )
        assert probe.breakers[ids['lane:holder']]._failure_total == 0
        _wait(probe, lambda: probe.breakers[ids['source:admin']]._failure_total == number)
        _assert_dependent_terminations(
            probe,
            before,
            dependents[original_spawns['source:admin']],
            tuple(
                original_spawns[name]
                for name in (
                    ('lane:holder', 'lane:named') if order == 'site-first' else ('lane:holder',)
                )
            ),
        )
        if order == 'site-first':
            assert not (episode / 'lane:named.error').exists()
    assert len({item.spawn_instance for item in probe.terminations}) == len(probe.terminations)
    print(
        f'{order}: episode={number}, '
        f'writes={ {name: _rows(episode, name) for name in _NAMES} }, '
        f'terminations={probe.terminations[before:]}'
    )
    if number == 1:
        _recover(probe, signals, episode)
    else:
        (episode / 'done').touch()


@pytest.mark.slow
@pytest.mark.parametrize('health', [2.0, 6.0])
def test_adverse_health_and_poll_alignment_contains_holder_inside_waiter_budgets(
    tmp_path: Path, health: float
) -> None:
    poll = 1.0
    budget = _HOLD + 2 * max(health, poll) + 1
    with _fixture(tmp_path, health=health, poll=poll) as (probe, signals):
        episode = _episode(probe, signals, 'alignment', 'lane:holder')
        (episode / 'priority-holder').touch()
        (episode / 'align-health').touch()
        path = source_database(tmp_path / 'state')
        with closing(sqlite3.connect(path, isolation_level=None)) as earlier:
            with _file_lock(path):
                earlier.execute('BEGIN IMMEDIATE')
                _go(episode, 'lane:named')
                _wait(probe, (episode / 'earlier-contention').exists)
                prior = _phase(episode, 'lane:named', 'write-lock-wait')[1]
                _wait(probe, lambda: time.monotonic() >= prior + 1)
                earlier.commit()
            _go(episode, 'lane:holder')
            _wait(probe, (episode / 'lane:holder.stalled').exists)
        held = _phase(episode, 'lane:holder', 'write-hold')[1]
        deadline = _phase(episode, 'lane:holder', 'write-hold')[2]
        sent = float((episode / 'health-sent').read_text())
        assert 0 <= held - sent < 0.25
        assert 1 <= held - prior < 1.3
        _go(episode, 'source:admin')
        _wait(probe, lambda: _has(episode, 'source:admin', 'write-lock-wait'))
        immediate = _phase(episode, 'source:admin', 'write-lock-wait')[1]
        assert 0 <= immediate - held < 0.25
        _wait(probe, lambda: time.monotonic() >= deadline - 0.15)
        before_poll = time.monotonic()
        probe.supervisor._tick()
        assert deadline - 0.2 < before_poll < deadline
        assert not probe.terminations
        time.sleep(poll)
        probe.supervisor._tick()
        _wait(probe, lambda: bool(probe.terminations))
        termination = probe.terminations[0]
        assert termination.worker_id == _worker(probe, 'lane:holder')
        assert termination.reason == 'lane-phase-overdue'
        if health == 2:
            assert any(
                deadline <= row[2] < deadline + 0.25
                for row in _rows(episode, 'monitor')
                if row[0] == 'health'
            )
        assert termination.at < min(
            _acquisition_deadline(episode, name, health=health) for name in _NAMES[:2]
        )
        _wait(probe, lambda: all(_has(episode, name, 'commit') for name in _NAMES[:2]))
        for name in _NAMES[:2]:
            assert _phase(episode, name, 'commit')[1] < _acquisition_deadline(
                episode, name, health=health
            )
            assert not any(row[0].startswith('timeout-') for row in _rows(episode, name))
            assert probe.breakers[_worker(probe, name)]._failure_total == 0
        assert len(probe.terminations) == 1
        prior_remaining = (
            _acquisition_deadline(episode, 'lane:named', health=health) - termination.at
        )
        print(
            f'alignment: health={health}, poll={poll}, budget={budget}, '
            f'health_sent={sent}, prior={prior}, held={held}, immediate={immediate}, '
            f'deadline={deadline}, before_poll={before_poll}, detected={termination.at}, '
            f'detection_estimate={held + max(_HOLD, health) + poll}, '
            f'prior_remaining={prior_remaining}'
        )
        (episode / 'done').touch()


@pytest.mark.slow
@pytest.mark.parametrize('order', ['site-first', 'remote-first', 'blocked-monitor'])
def test_fallback_order_counts_only_the_first_reason_for_each_spawn(
    tmp_path: Path, order: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    dependents: dict[str, dict[str, str]] = {}
    original_release = Supervisor._release_source_ownership

    def release_source(owner: Supervisor, worker: _WorkerHandle) -> None:
        if worker._spec.sources:
            dependents[worker._spawn_instance] = {
                spawn._worker._spawn_instance: spawn._termination_reason
                or 'source-owner-unavailable'
                for spawn in owner._spawns.values()
                if spawn._worker._spec.site == worker._spec.site
                and not spawn._worker._spec.sources
            }
        original_release(owner, worker)

    monkeypatch.setattr(Supervisor, '_release_source_ownership', release_source)
    with _fixture(tmp_path, fetch=1 if order == 'blocked-monitor' else 0.05) as (probe, signals):
        ids = {name: _worker(probe, name) for name in _NAMES}
        # Exercise the actual breaker transition after two faults, without
        # changing acquisition, health, hold, or restart-backoff timing.
        for breaker in probe.breakers.values():
            breaker._restart_limit = 1
        for number in (1, 2):
            _fallback_episode(probe, signals, order, number, dependents)
        charged = {
            'site-first': {'source:admin'},
            'remote-first': {'source:admin', 'lane:named'},
            'blocked-monitor': {'lane:holder', 'lane:named'},
        }[order]
        _wait(probe, lambda: all(probe.breakers[ids[name]]._is_open for name in charged))
        for name, worker_id in ids.items():
            breaker = probe.breakers[worker_id]
            assert breaker._failure_total == (2 if name in charged else 0)
            assert breaker._is_open == (name in charged)
        assert not probe.park_evidence


@pytest.mark.parametrize(
    ('kind', 'identity'), [('lane', 'holder'), ('lane', 'named'), ('source', 'admin')]
)
def test_health_mask_uses_configured_idle_policy_only_for_holder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: Literal['lane', 'source'], identity: str
) -> None:
    episode = _arm_episode(tmp_path, 'mask', 'lane:holder')
    (episode / 'mask-holder').touch()
    watchdog = ComponentWatchdog(
        kind=kind,
        identity=identity,
        worker_instance='spawn',
        registration_token=_REGISTRATION,
        startup_deadline_secs=30,
    )
    watchdog.transition('write-hold', deadline_at=time.monotonic() + 5)
    original = watchdog.sample()
    _worker_processes._install_timing_samples(
        monkeypatch, ChildPatches(signal_dir=tmp_path, timing=True), 6
    )
    actual = watchdog.sample()
    if (kind, identity) == ('lane', 'holder'):
        assert actual.heartbeat_at >= original.heartbeat_at
        assert actual == replace(
            original,
            phase='idle',
            heartbeat_at=actual.heartbeat_at,
            deadline_at=actual.heartbeat_at + 6,
        )
    else:
        assert actual == original
    WatchdogHealth(
        kind=actual.kind,
        site='test',
        source='admin',
        group=identity if kind == 'lane' else None,
        worker_instance=actual.worker_instance,
        phase=actual.phase,
        heartbeat_at=actual.heartbeat_at,
        deadline_at=actual.deadline_at,
    )


@pytest.mark.slow
def test_joining_remote_timeout_exits_before_later_source_timeout(tmp_path: Path) -> None:
    with _fixture(tmp_path) as (probe, signals):
        ids = {name: _worker(probe, name) for name in _NAMES}
        episode = _arm_episode(signals, 'joining', 'lane:holder', joining=True)
        (episode / 'mask-holder').touch()
        named = next(spawn for spawn in probe.spawns if spawn.worker_id == ids['lane:named'])
        named.process.send_signal(signal.SIGKILL)
        _wait(probe, lambda: all((episode / f'{name}.ready').exists() for name in _NAMES))
        assert (episode / 'lane:named.joining').exists()
        replacement = next(
            spawn for spawn in reversed(probe.spawns) if spawn.worker_id == ids['lane:named']
        )
        assert replacement.spawn_instance != named.spawn_instance
        assert probe.breakers[ids['lane:named']]._failure_total == 1
        before = len(probe.terminations)
        _go(episode, 'lane:holder')
        _wait(probe, (episode / 'lane:holder.stalled').exists)
        _go(episode, 'lane:named')
        _wait(probe, lambda: _has(episode, 'lane:named', 'write-lock-wait'))
        start = _phase(episode, 'lane:named', 'write-lock-wait')[1]
        _wait(probe, lambda: time.monotonic() >= start + 3)
        _go(episode, 'source:admin')
        _wait(probe, lambda: _has(episode, 'source:admin', 'write-lock-wait'))
        _wait(probe, lambda: len(probe.terminations) > before)
        ended = probe.terminations[before]
        assert ended.worker_id == ids['lane:named'], probe.terminations
        assert ended.spawn_instance == replacement.spawn_instance
        assert _has(episode, 'lane:named', 'component-failed')
        assert 'WriteLockTimeoutError' in (episode / 'lane:named.fatal').read_text()
        timeout = _phase(episode, 'lane:named', 'timeout-file')[1]
        deadline = _acquisition_deadline(episode, 'lane:named')
        source_deadline = _acquisition_deadline(episode, 'source:admin')
        fatal = _phase(episode, 'lane:named', 'component-failed')[1]
        assert deadline <= timeout <= fatal < ended.at < source_deadline
        assert not _has(episode, 'source:admin', 'timeout-file')
        assert len(probe.terminations) == before + 1
        _wait(probe, lambda: probe.breakers[ids['lane:named']]._failure_total == 2)
        assert probe.breakers[ids['source:admin']]._failure_total == 0
        assert probe.breakers[ids['lane:holder']]._failure_total == 0
        print(
            f'joining: start={start}, deadline={deadline}, timeout={timeout}, '
            f'source_deadline={source_deadline}, exit={ended}'
        )
        (episode / 'release').touch()
        _wait(probe, lambda: _has(episode, 'source:admin', 'commit'))
        (episode / 'done').touch()
        assert not probe.park_evidence


@pytest.mark.slow
def test_source_holder_stops_remote_waiters_without_charging_their_breakers(
    tmp_path: Path,
) -> None:
    with _fixture(tmp_path) as (probe, signals):
        episode = _episode(probe, signals, 'source-holder', 'source:admin')
        owner = _worker(probe, 'source:admin')
        _go(episode, 'source:admin')
        _wait(probe, (episode / 'source:admin.stalled').exists)
        for name in _NAMES[1:]:
            _go(episode, name)
        _wait(probe, lambda: all(_has(episode, name, 'write-lock-wait') for name in _NAMES[1:]))
        _wait(probe, lambda: len(probe.terminations) == 3)
        assert probe.terminations[0].worker_id == owner
        assert probe.terminations[0].reason == 'source-phase-overdue'
        for termination in probe.terminations[1:]:
            assert termination.reason == 'source-owner-unavailable'
            assert probe.breakers[termination.worker_id]._failure_total == 0
        for name in _NAMES[1:]:
            assert probe.terminations[-1].at < _acquisition_deadline(episode, name)
            assert not any(row[0].startswith('timeout-') for row in _rows(episode, name))
        _recover(probe, signals, episode)
        assert probe.breakers[owner]._failure_total == 1
        assert not probe.park_evidence
        print(f'source-holder: terminations={probe.terminations}')


@pytest.mark.slow
def test_completed_sampled_hold_survives_deadline_with_long_health_interval(
    tmp_path: Path,
) -> None:
    with _fixture(tmp_path, health=6) as (probe, signals):
        episode = _episode(probe, signals, 'completed-hold', 'lane:holder')
        (episode / 'sample-hold').touch()
        holder_id = _worker(probe, 'lane:holder')
        spawn = next(item for item in probe.spawns if item.worker_id == holder_id)
        _go(episode, 'lane:holder')
        _wait(probe, (episode / 'hold-health-sent').exists)
        sampled_at = float((episode / 'hold-health-sent').read_text())
        deadline = _phase(episode, 'lane:holder', 'write-hold')[2]
        assert sampled_at < deadline
        _wait(
            probe,
            lambda: any(
                item.phase == 'write-hold'
                for item in probe.latest_health[holder_id].watchdogs
                if item.group == 'holder'
            ),
        )
        sampled = probe.latest_health[holder_id]
        assert sampled.worker.heartbeat_at < deadline
        (episode / 'release').touch()
        _wait(probe, lambda: _has(episode, 'lane:holder', 'commit'))
        assert _phase(episode, 'lane:holder', 'commit')[1] < deadline
        _wait(probe, lambda: time.monotonic() >= deadline + 0.1)
        assert not probe.terminations
        _wait(probe, lambda: probe.latest_health[holder_id].worker.heartbeat_at >= sampled_at + 6)
        assert not probe.terminations
        assert spawn.process.poll() is None
        assert probe.breakers[holder_id]._failure_total == 0
        assert not probe.park_evidence
        (episode / 'done').touch()
