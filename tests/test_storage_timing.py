from __future__ import annotations

import os
import sqlite3
import time
from contextlib import closing, contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Literal

import pytest

from sg_event_daemon._health import WatchdogHealth
from sg_event_daemon._platform import release_flock, try_flock_exclusive
from sg_event_daemon._watchdog import ComponentWatchdog, watchdog_grace_secs
from tests import _worker_processes
from tests._signals import publish_text, read_complete_lines
from tests._storage_support import source_database
from tests._worker_processes import (
    ChildPatches,
    SupervisorProbe,
    Topology,
    install_timing_clock,
    running_supervisor,
    write_daemon_config,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path

    from tests._stall_policy import StallDetectors


_HOLD = 5.0
_NAMES = ('source:admin', 'lane:named', 'lane:holder')
_RAW_MONOTONIC = time.monotonic
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


def _resume_clock(signals: Path) -> None:
    frozen = signals / 'clock'
    if frozen.exists():
        publish_text(signals / 'clock-offset', str(float(frozen.read_text()) - _RAW_MONOTONIC()))
        frozen.unlink()


def _rows(episode: Path, name: str) -> list[tuple[str, float, float]]:
    path = episode / f'{name}.writes'
    return [
        (a, float(b), float(c)) for a, b, c in (line.split() for line in read_complete_lines(path))
    ]


def _has(episode: Path, name: str, phase: str) -> bool:
    return any(row[0] == phase for row in _rows(episode, name))


def _phase(episode: Path, name: str, phase: str) -> tuple[str, float, float]:
    return next(row for row in _rows(episode, name) if row[0] == phase)


def _acquisition_deadline(episode: Path, name: str, *, grace: float) -> float:
    # The callback observes the phase after acquisition timing has started.
    # Its supplied watch deadline also includes the configured watchdog grace.
    return _phase(episode, name, 'write-lock-wait')[2] - grace


def _set_clock(signals: Path, now: float) -> None:
    pending = signals / 'clock.pending'
    pending.write_text(str(now))
    pending.replace(signals / 'clock')


def _wait(
    probe: SupervisorProbe,
    predicate: Callable[[], bool],
    *,
    allow_terminations: bool = False,
) -> None:
    failures = len(probe.terminations)
    while not predicate():
        probe.supervisor._tick()
        if not allow_terminations and len(probe.terminations) != failures:
            raise AssertionError(probe.failure_details())
        for spawn in probe.spawns:
            if spawn.process.poll() is not None and not any(
                ended.spawn_instance == spawn.spawn_instance for ended in probe.terminations
            ):
                raise AssertionError(probe.failure_details())
        time.sleep(0.005)


def _episode(probe: SupervisorProbe, signals: Path, number: str, holder: str) -> Path:
    episode = _arm_episode(signals, number, holder)
    _wait(probe, lambda: all((episode / f'{name}.ready').exists() for name in _NAMES))
    _set_clock(signals, time.monotonic())
    return episode


def _recover(probe: SupervisorProbe, signals: Path, episode: Path) -> None:
    (episode / 'done').touch()
    _resume_clock(signals)
    _wait(
        probe,
        lambda: (
            len(probe.latest_health) == 3
            and all(
                spawn.termination_reason is None for spawn in probe.supervisor._spawns.values()
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


def _sample_at(
    probe: SupervisorProbe, signals: Path, now: float, *, blocked: str | None = None
) -> None:
    _set_clock(signals, now)
    workers = set(probe.latest_health)
    _wait(
        probe,
        lambda: all(
            worker == blocked
            or any(ended.worker_id == worker for ended in probe.terminations)
            or probe.latest_health[worker].worker.heartbeat_at >= now
            for worker in workers
        ),
        allow_terminations=True,
    )


@contextmanager
def _fixture(
    tmp_path: Path, *, health: float = 2, poll: float = 1, fetch: float = 0.05
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
    config.write_text(text)
    signals = tmp_path / 'signals'
    signals.mkdir()
    with pytest.MonkeyPatch.context() as patch:
        install_timing_clock(patch, signals)
        with running_supervisor(
            config, ChildPatches(hold_deadline_secs=_HOLD, signal_dir=signals, timing=True)
        ) as probe:
            _wait(
                probe,
                lambda: (
                    len(probe.latest_health) == 3
                    and all(probe.records(group) for group in ('named', 'holder'))
                ),
            )
            assert not probe.terminations
            try:
                yield probe, signals
            finally:
                for episode in signals.iterdir():
                    if episode.is_dir():
                        (episode / 'done').touch()
                        (episode / 'release').touch()

                _resume_clock(signals)


def _worker(probe: SupervisorProbe, name: str) -> str:
    return next(
        worker_id
        for worker_id, health in probe.latest_health.items()
        if name in health.worker.thread_names
    )


@pytest.mark.slow
@pytest.mark.parametrize('health', [2.0, 6.0])
def test_adverse_health_and_poll_alignment_contains_holder_inside_waiter_budgets(
    tmp_path: Path, health: float, stall_detectors: StallDetectors
) -> None:
    poll = 1.0
    budget = _HOLD + 2 * max(health, poll) + 1
    with stall_detectors.enabled('phase'):
        grace = watchdog_grace_secs(health)
    with _fixture(tmp_path, health=health, poll=poll) as (probe, signals):
        episode = _episode(probe, signals, 'alignment', 'lane:holder')
        (episode / 'priority-holder').touch()
        (episode / 'align-health').touch()
        path = source_database(tmp_path / 'state')
        with closing(sqlite3.connect(path, isolation_level=None)) as earlier:
            with _file_lock(path):
                earlier.execute('BEGIN IMMEDIATE')
                _go(episode, 'lane:holder')
                _wait(probe, (episode / 'health-ready').exists)
                _go(episode, 'lane:named')
                _wait(probe, (episode / 'earlier-contention').exists)
                prior = _phase(episode, 'lane:named', 'write-lock-wait')[1]
                _set_clock(signals, prior + 1)
                earlier.commit()
            (episode / 'publish-health').touch()
            _wait(probe, (episode / 'lane:holder.stalled').exists)
        held = _phase(episode, 'lane:holder', 'write-hold')[1]
        deadline = _phase(episode, 'lane:holder', 'write-hold')[2]
        sent = float((episode / 'health-sent').read_text())
        assert held == sent
        assert held - prior == 1
        _go(episode, 'source:admin')
        _wait(probe, lambda: _has(episode, 'source:admin', 'write-lock-wait'))
        immediate = _phase(episode, 'source:admin', 'write-lock-wait')[1]
        assert immediate == held
        _set_clock(signals, deadline - 0.15)
        before_poll = time.monotonic()
        probe.supervisor._tick()
        assert deadline - 0.2 < before_poll < deadline
        assert not probe.terminations
        holder = _worker(probe, 'lane:holder')
        _sample_at(probe, signals, before_poll + poll)
        assert probe.terminations, probe.failure_details()
        termination = probe.terminations[0]
        assert termination.worker_id == holder
        assert termination.reason == 'lane-phase-overdue'
        assert any(
            row[2] == before_poll + poll for row in _rows(episode, 'monitor') if row[0] == 'health'
        )
        assert termination.at < min(
            _acquisition_deadline(episode, name, grace=grace) for name in _NAMES[:2]
        )
        _wait(probe, lambda: all(_has(episode, name, 'commit') for name in _NAMES[:2]))
        for name in _NAMES[:2]:
            assert _phase(episode, name, 'commit')[1] < _acquisition_deadline(
                episode, name, grace=grace
            )
            assert not any(row[0].startswith('timeout-') for row in _rows(episode, name))
            assert probe.breakers[_worker(probe, name)]._failure_total == 0
        assert len(probe.terminations) == 1
        print(
            f'alignment: health={health}, poll={poll}, budget={budget}, '
            f'health_sent={sent}, prior={prior}, held={held}, immediate={immediate}, '
            f'deadline={deadline}, before_poll={before_poll}, detected={termination.at}, '
            f'detection_estimate={held + max(_HOLD, health) + poll}, '
            f'prior_remaining='
            f'{_acquisition_deadline(episode, "lane:named", grace=grace) - termination.at}'
        )
        (episode / 'done').touch()


@pytest.mark.slow
@pytest.mark.parametrize('blocked_monitor', [False, True])
def test_undetected_holder_leaves_waiters_waiting(
    tmp_path: Path, blocked_monitor: bool, stall_detectors: StallDetectors
) -> None:
    health = 2.0
    poll = 1.0
    acquisition_budget = _HOLD + 2 * max(health, poll) + 1
    with stall_detectors.enabled('phase'):
        grace = watchdog_grace_secs(health)
    with _fixture(tmp_path, health=health, poll=poll) as (probe, signals):
        episode = _episode(probe, signals, 'undetected', 'lane:holder')
        (episode / 'mask-holder').touch()
        _go(episode, 'lane:holder')
        _wait(probe, (episode / 'lane:holder.stalled').exists)
        holder = _worker(probe, 'lane:holder')
        if blocked_monitor:
            (episode / 'block-monitor').touch()
            _wait(probe, (episode / 'monitor-blocked').exists)
            # Start monitor silence before acquisition so only its heartbeat expires.
            with stall_detectors.enabled('heartbeat'):
                _sample_at(probe, signals, time.monotonic() + health * 2.5, blocked=holder)
        for name in _NAMES[:2]:
            _go(episode, name)
        _wait(probe, lambda: all(_has(episode, name, 'write-lock-wait') for name in _NAMES[:2]))
        if blocked_monitor:
            with stall_detectors.enabled('heartbeat'):
                _sample_at(probe, signals, time.monotonic() + health * 2.5, blocked=holder)
            assert probe.terminations, probe.failure_details()
            assert len(probe.terminations) == 1
            ended = probe.terminations[0]
            assert ended.worker_id == holder
            assert ended.reason == 'worker-heartbeat-overdue'
            _wait(probe, lambda: probe.breakers[ended.worker_id]._failure_total == 1)
        else:
            for expected in (2, 3):
                # Refresh monitor health midway so only acquisition, not liveness, expires.
                _sample_at(probe, signals, time.monotonic() + acquisition_budget / 2)
                _sample_at(probe, signals, time.monotonic() + acquisition_budget / 2)
                assert not probe.terminations, probe.failure_details()
                _wait(
                    probe,
                    lambda expected=expected: (
                        sum(row[0] == 'poll-wait' for row in _rows(episode, 'source:admin'))
                        >= expected - 1
                    ),
                )
                retry_deadline = next(
                    row[2]
                    for row in reversed(_rows(episode, 'source:admin'))
                    if row[0] == 'poll-wait'
                )
                _sample_at(probe, signals, retry_deadline)
                assert not probe.terminations, probe.failure_details()
                _wait(
                    probe,
                    lambda expected=expected: all(
                        sum(row[0] == 'write-lock-wait' for row in _rows(episode, name))
                        >= expected
                        for name in _NAMES[:2]
                    ),
                )
            assert not probe.terminations
            _wait(
                probe,
                lambda: all(
                    any(
                        'write-lock-stalled' in item.conditions
                        for health in probe.latest_health.values()
                        for item in (*health.sources, *health.groups)
                        if (
                            getattr(item, 'group', None) == 'named'
                            if name == 'lane:named'
                            else getattr(item, 'source', None) == 'admin'
                            and not hasattr(item, 'group')
                        )
                    )
                    for name in _NAMES[:2]
                ),
            )
            for name in _NAMES[:2]:
                assert probe.breakers[_worker(probe, name)]._failure_total == 0
                phases = [row for row in _rows(episode, name) if row[0] == 'write-lock-wait']
                assert all(
                    row[2] - row[1] == pytest.approx(acquisition_budget + grace) for row in phases
                )
            (episode / 'release').touch()
        _wait(probe, lambda: all(_has(episode, name, 'commit') for name in _NAMES[:2]))
        _wait(
            probe,
            lambda: all(
                'write-lock-stalled' not in item.conditions
                for health in probe.latest_health.values()
                for item in (*health.sources, *health.groups)
            ),
        )
        if not blocked_monitor:
            for name in _NAMES[:2]:
                spawn = next(
                    spawn for spawn in probe.spawns if spawn.worker_id == _worker(probe, name)
                )
                logs = spawn.log_path.read_text()
                assert logs.count('at the file level:') == 1
                assert logs.count('resumed after waiting') == 1
        (episode / 'done').touch()


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
    _worker_processes.install_timing_samples(
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
def test_source_holder_stops_remote_waiters_without_charging_their_breakers(
    tmp_path: Path, stall_detectors: StallDetectors
) -> None:
    with stall_detectors.enabled('phase'):
        grace = watchdog_grace_secs(2)
    with _fixture(tmp_path) as (probe, signals):
        episode = _episode(probe, signals, 'source-holder', 'source:admin')
        owner = _worker(probe, 'source:admin')
        _go(episode, 'source:admin')
        _wait(probe, (episode / 'source:admin.stalled').exists)
        for name in _NAMES[1:]:
            _go(episode, name)
        _wait(probe, lambda: all(_has(episode, name, 'write-lock-wait') for name in _NAMES[1:]))
        deadline = _phase(episode, 'source:admin', 'write-hold')[2]
        _sample_at(probe, signals, deadline)
        assert probe.terminations, probe.failure_details()
        assert probe.terminations[0].worker_id == owner
        assert probe.terminations[0].reason == 'source-phase-overdue'
        # Dependent containment follows the owner's OS exit and reap, not its kill request.
        _wait(probe, lambda: len(probe.terminations) >= 3, allow_terminations=True)
        assert len(probe.terminations) == 3, probe.failure_details()
        for termination in probe.terminations[1:]:
            assert termination.reason == 'source-owner-unavailable'
            assert probe.breakers[termination.worker_id]._failure_total == 0
        for name in _NAMES[1:]:
            assert probe.terminations[-1].at < _acquisition_deadline(episode, name, grace=grace)
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
        (episode / 'block-monitor').touch()
        _wait(probe, (episode / 'monitor-blocked').exists)
        (episode / 'release').touch()
        _wait(probe, lambda: _has(episode, 'lane:holder', 'commit'))
        assert _phase(episode, 'lane:holder', 'commit')[1] < deadline
        _set_clock(signals, deadline + 0.1)
        probe.supervisor._tick()
        assert not probe.terminations
        (episode / 'done').touch()
        _sample_at(probe, signals, sampled_at + 6)
        assert not probe.terminations
        assert spawn.process.poll() is None
        assert probe.breakers[holder_id]._failure_total == 0
        assert not probe.park_evidence
        (episode / 'done').touch()
