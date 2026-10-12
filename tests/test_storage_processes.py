from __future__ import annotations

import signal
import sqlite3
import time
from collections import Counter
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon._group_state import GroupState
from tests import _worker_processes
from tests._storage_support import bind_group, insert_range, source_database, write_context_for
from tests._worker_processes import (
    ChildPatches,
    SupervisorProbe,
    Topology,
    running_supervisor,
    write_daemon_config,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from sg_event_daemon._group_types import GroupSnapshot
    from sg_event_daemon._plugins import CallbackManifest

_GROUPS = ('local', 'named_a', 'named_b', 'named_c', 'remote')
_LAST_ID = 100
_THREADS = ('source:admin', *(f'lane:{group}' for group in _GROUPS))
# Process contention tests need scheduler headroom; timing budgets have dedicated coverage.
_TOPOLOGY = Topology('mixed', _GROUPS, health_interval_secs=1)


def _assert_waiter_commits(signals: Path, killed_at: float) -> None:
    for thread in _THREADS[:-1]:
        rows = [line.split() for line in (signals / f'{thread}.writes').read_text().splitlines()]
        wait = next(row for row in rows if row[0] == 'write-lock-wait')
        hold = next(row for row in rows if row[0] == 'write-hold')
        commit = next(row for row in rows if row[0] == 'commit')
        assert float(wait[1]) < killed_at <= float(hold[1]) <= float(commit[1])
        assert float(hold[1]) < float(wait[2])


def _expected(floor: int) -> tuple[int, ...]:
    return tuple(i for i in range(floor + 1, _LAST_ID + 1) if i not in {7, 19, 31})


def _install_inflight_pause(root: Path) -> None:
    original_start = GroupState.start_event
    marker = root / 'inflight-event'

    def start(
        state: GroupState,
        event_id: int,
        manifest: CallbackManifest,
        *,
        expected_revision: int,
        obligation_id: str | None = None,
    ) -> GroupSnapshot:
        snapshot = original_start(
            state,
            event_id,
            manifest,
            expected_revision=expected_revision,
            obligation_id=obligation_id,
        )
        if not marker.exists():
            marker.write_text(str(event_id))
            # The parent kills this process after inspecting the committed row.
            while not (root / 'release-inflight').exists():
                time.sleep(0.005)
        return snapshot

    pytest.MonkeyPatch().setattr(GroupState, 'start_event', start)


def _poller_revisions(probe: SupervisorProbe) -> list[tuple[int, ...]]:
    return [
        tuple(map(int, line.split()))
        for spawn in probe.spawns
        if spawn.log_path.with_suffix('.revisions').exists()
        for line in spawn.log_path.with_suffix('.revisions').read_text().splitlines()
    ]


def _scan_revisions(probe: SupervisorProbe) -> list[int]:
    return [
        int(line)
        for spawn in probe.spawns
        if spawn.log_path.with_suffix('.scans').exists()
        for line in spawn.log_path.with_suffix('.scans').read_text().splitlines()
    ]


def _start_delivery(probe: SupervisorProbe, root: Path) -> None:
    # Keep the finite stream at its initial floor until every lane has joined.
    probe.wait_for(
        lambda: (
            {
                group.group
                for health in probe.latest_health.values()
                for group in health.groups
                if group.join_phase == 'attached'
            }
            == set(_GROUPS)
        ),
        15,
    )
    (root / 'events-ready').touch()
    probe.wait_for(lambda: all(probe.records(group) for group in _GROUPS), 15)


def _state(path: Path) -> dict[str, tuple[int, int, int | None, int]]:
    with closing(sqlite3.connect(path)) as connection:
        return {
            row[0]: (row[1], row[2], row[3], row[4])
            for row in connection.execute(
                'SELECT group_name, cursor, delivery_floor, in_flight_event_id, '
                'last_delivered_id FROM group_state'
            )
        }


def _assert_deliveries(probe: SupervisorProbe, path: Path, floors: Mapping[str, int]) -> None:
    for group, (cursor, floor, in_flight, delivered) in _state(path).items():
        # Every fixture event matches. Initialize excludes the persisted floor,
        # independently of anything the callback recorded.
        assert floor == floors[group]
        expected = _expected(floors[group])
        assert len(expected) >= 20
        assert probe.records(group) == expected, group
        assert cursor == delivered == expected[-1]
        assert in_flight is None
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('SELECT COUNT(*) FROM callback_marks').fetchone() == (0,)
        assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)


def _quiescent(path: Path, last_id: int) -> bool:
    rows = _state(path)
    return set(rows) == set(_GROUPS) and all(
        cursor == last_id and in_flight is None
        for cursor, _floor, in_flight, _delivered in rows.values()
    )


def _waiters_blocked(probe: SupervisorProbe) -> bool:
    samples = [
        sample
        for health in probe.latest_health.values()
        for sample in health.watchdogs
        if sample.kind == 'source' or sample.group in _GROUPS[:-1]
    ]
    return len(samples) == 5 and all(sample.phase == 'write-lock-wait' for sample in samples)


def _worker_for(probe: SupervisorProbe, group: str) -> str:
    return next(
        worker_id
        for worker_id, health in probe.latest_health.items()
        if any(item.group == group for item in health.groups)
    )


@pytest.mark.slow
def test_killed_owner_refetches_and_delivers_committed_inflight_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    original_code = _worker_processes.child_code

    def child_code(config_path: Path, patches: ChildPatches) -> str:
        preamble, invocation = original_code(config_path, patches).rsplit('run_worker(', 1)
        return (
            preamble
            + f'from tests.test_storage_processes import {_install_inflight_pause.__name__}\n'
            + f'{_install_inflight_pause.__name__}(Path({str(tmp_path)!r}))\n'
            + 'run_worker('
            + invocation
        )

    monkeypatch.setattr(_worker_processes, 'child_code', child_code)
    config = write_daemon_config(tmp_path, Topology('default', ('local',)))
    path = source_database(tmp_path / 'state')
    marker = tmp_path / 'inflight-event'
    with running_supervisor(config, ChildPatches(last_event_id=_LAST_ID)) as probe:
        try:
            probe.wait_for(lambda: marker.exists() and bool(marker.read_text()), 15)
            event_id = int(marker.read_text())
            cursor, _floor, in_flight, delivered = _state(path)['local']
            assert in_flight == event_id
            assert cursor < event_id
            assert delivered < event_id
            assert event_id not in probe.records('local')
            assert len(probe.spawns) == 1
            original = probe.spawns[0]
            original.process.send_signal(signal.SIGKILL)
            probe.wait_for(lambda: len(probe.spawns) >= 2, 15)
            replacement = probe.spawns[1]
            assert replacement.worker_id == original.worker_id
            assert replacement.spawn_instance != original.spawn_instance
            probe.wait_for(lambda: _state(path)['local'][0] == _LAST_ID, 15)
            assert _state(path)['local'][2:] == (None, _LAST_ID)
            assert probe.records('local').count(event_id) == 1
            assert probe.records('local')[0] == event_id
            assert len(probe.spawns) == 2
            assert replacement.process.poll() is None
            assert [item.spawn_instance for item in probe.terminations] == [
                original.spawn_instance
            ]
            assert 'thread failed' not in replacement.log_path.read_text()
        finally:
            (tmp_path / 'release-inflight').touch()


@pytest.mark.slow
def test_site_named_and_dedicated_workers_share_one_source_file(tmp_path: Path) -> None:
    config = write_daemon_config(tmp_path, _TOPOLOGY)
    path = source_database(tmp_path / 'state')
    with running_supervisor(
        config,
        ChildPatches(
            hold_deadline_secs=5, last_event_id=_LAST_ID, track_revisions=True, gate_events=True
        ),
    ) as probe:
        _start_delivery(probe, tmp_path)
        floors = {group: row[1] for group, row in _state(path).items()}
        probe.wait_for(lambda: _quiescent(path, _LAST_ID), 15)
        probe.wait_for(
            lambda: all(
                any(
                    item.checkpoint_state == 'complete'
                    for item in (*health.groups, *health.sources)
                )
                for health in probe.latest_health.values()
            ),
            5,
        )
        assert len(probe.spawns) == 3, probe.terminations
        assert not probe.terminations
        assert all(
            spawn._termination_reason is None for spawn in probe.supervisor._spawns.values()
        )
        _assert_deliveries(probe, path, floors)
        writes = write_context_for(path)
        try:
            with (
                closing(sqlite3.connect(path, isolation_level=None)) as connection,
                writes.transaction(connection),
            ):
                insert_range(
                    connection, 'post-close-probe', _LAST_ID + 1, _LAST_ID + 1, state='closed'
                )
                for group, generation in connection.execute(
                    'SELECT group_name, group_generation FROM groups'
                ).fetchall():
                    bind_group(
                        connection,
                        'post-close-probe',
                        group,
                        generation,
                        floor=floors[group],
                        eligibility_point=0,
                    )
        finally:
            writes.lock.close()
        probe.wait_for(lambda: len(_scan_revisions(probe)) == len(_GROUPS), 10)
        completed_revision = max(_scan_revisions(probe))
        probe.wait_for(
            lambda: any(row[1] >= completed_revision for row in _poller_revisions(probe)), 5
        )
        for spawn in probe.spawns:
            lines = spawn.log_path.with_suffix('.checkpoints').read_text().splitlines()
            outcomes = Counter(line.split()[0] for line in lines)
            contended = sum(line.split()[1] == 'True' for line in lines)
            print(
                f'{spawn.worker_id}: checkpoints={dict(outcomes)}, contended_attempts={contended}'
            )
            assert outcomes['complete'] > 0


@pytest.mark.slow
def test_remote_stuck_holder_is_terminated_while_waiters_keep_their_workers(
    tmp_path: Path,
) -> None:
    config = write_daemon_config(tmp_path, _TOPOLOGY)
    signals = tmp_path / 'signals'
    signals.mkdir()
    patches = ChildPatches(
        hold_deadline_secs=5,
        stall_group='remote',
        signal_dir=signals,
        remote_rendezvous=True,
        gate_events=True,
    )
    with running_supervisor(config, patches) as probe:
        _start_delivery(probe, tmp_path)
        assert len(probe.spawns) == 3, probe.terminations
        remote = _worker_for(probe, 'remote')
        failures = {name: breaker._failure_total for name, breaker in probe.breakers.items()}
        (signals / 'arm').touch()
        probe.wait_for(lambda: _waiters_blocked(probe), 10)
        assert (signals / 'source:admin.local').exists()
        assert (signals / 'lane:named_a.local').exists()
        probe.wait_for(lambda: bool(probe.terminations), 10)
        termination = probe.terminations[0]
        assert termination.worker_id == remote
        assert termination.reason == 'lane-phase-overdue'
        for worker_id, health in probe.latest_health.items():
            if worker_id != remote:
                for sample in health.watchdogs:
                    if sample.kind in {'source', 'lane'}:
                        assert sample.phase == 'write-lock-wait'
                        assert sample.deadline_at is not None
                        assert termination.at < sample.deadline_at
        probe.wait_for(
            lambda: all((signals / f'{thread}.committed').exists() for thread in _THREADS[:-1]),
            10,
        )
        _assert_waiter_commits(signals, termination.at)
        probe.wait_for(lambda: len(probe.spawns) == 4, 15)
        assert [item.worker_id for item in probe.terminations] == [remote]
        assert probe.breakers[remote]._failure_total == failures[remote] + 1
        for name, before in failures.items():
            if name != remote:
                assert probe.breakers[name]._failure_total == before
                assert sum(spawn.worker_id == name for spawn in probe.spawns) == 1
        assert not probe.park_evidence


@pytest.mark.slow
def test_killed_remote_lane_releases_locks_and_resumes_from_the_committed_prefix(
    tmp_path: Path,
) -> None:
    config = write_daemon_config(tmp_path, _TOPOLOGY)
    signals = tmp_path / 'signals'
    signals.mkdir()
    patches = ChildPatches(
        hold_deadline_secs=5,
        stall_group='remote',
        signal_dir=signals,
        remote_rendezvous=True,
        gate_events=True,
        last_event_id=_LAST_ID,
    )
    path = source_database(tmp_path / 'state')
    with running_supervisor(config, patches) as probe:
        _start_delivery(probe, tmp_path)
        remote = _worker_for(probe, 'remote')
        assert len(probe.spawns) == 3, probe.terminations
        original = next(spawn for spawn in probe.spawns if spawn.worker_id == remote)
        (signals / 'arm').touch()
        probe.wait_for(lambda: _waiters_blocked(probe), 10)
        cursor = _state(path)['remote'][0]
        assert _state(path)['remote'][2] is None
        before = probe.records('remote')
        killed_at = time.monotonic()
        original.process.send_signal(signal.SIGKILL)
        probe.wait_for((signals / 'source:admin.committed').exists, 10)
        committed_at = float((signals / 'source:admin.committed').read_text())
        print(f'poller release-to-commit seconds={committed_at - killed_at:.6f}')
        assert committed_at >= killed_at
        probe.wait_for(lambda: len(probe.spawns) == 4, 15)
        probe.wait_for(lambda: _quiescent(path, _LAST_ID), 15)
        after = probe.records('remote')[len(before) :]
        assert after == _expected(cursor)
        assert len(probe.spawns) == 4
        assert {item.worker_id for item in probe.terminations} == {remote}
        assert all(
            'write lock wait expired' not in spawn.log_path.read_text() for spawn in probe.spawns
        )
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute('SELECT COUNT(*) FROM callback_marks').fetchone() == (0,)


@pytest.mark.slow
def test_killed_source_worker_stops_dependents_and_lanes_resume(tmp_path: Path) -> None:
    config = write_daemon_config(tmp_path, _TOPOLOGY)
    path = source_database(tmp_path / 'state')
    signals = tmp_path / 'signals'
    signals.mkdir()
    patches = ChildPatches(
        hold_deadline_secs=5,
        stall_group='remote',
        signal_dir=signals,
        remote_rendezvous=True,
        gate_events=True,
        last_event_id=_LAST_ID,
        track_revisions=True,
    )
    with running_supervisor(config, patches) as probe:
        _start_delivery(probe, tmp_path)
        assert len(probe.spawns) == 3, probe.terminations
        owner = _worker_for(probe, 'local')
        with closing(sqlite3.connect(path)) as connection:
            prior_index = connection.execute('SELECT index_revision FROM source_meta').fetchone()[
                0
            ]
        originals = tuple(probe.spawns)
        (signals / 'arm').touch()
        probe.wait_for(lambda: _waiters_blocked(probe), 10)
        cursors = {group: row[0] for group, row in _state(path).items()}
        floors = {group: row[1] for group, row in _state(path).items()}
        assert all(row[2] is None for row in _state(path).values())
        before = {group: probe.records(group) for group in _GROUPS}
        failures = {name: breaker._failure_total for name, breaker in probe.breakers.items()}
        next(spawn for spawn in originals if spawn.worker_id == owner).process.send_signal(
            signal.SIGKILL
        )
        probe.wait_for(lambda: len(probe.spawns) == 6, 15)
        probe.wait_for(lambda: _quiescent(path, _LAST_ID), 15)
        restarted = next(spawn for spawn in probe.spawns[3:] if spawn.worker_id == owner)
        probe.wait_for(restarted.log_path.with_suffix('.revisions').exists, 5)
        first = tuple(
            map(
                int,
                restarted.log_path.with_suffix('.revisions').read_text().splitlines()[0].split(),
            )
        )
        assert first[0] == -1
        assert first[1] >= prior_index
        assert len(probe.terminations) == 3
        for termination in probe.terminations:
            if termination.worker_id != owner:
                assert termination.reason == 'source-owner-unavailable'
                assert (
                    probe.breakers[termination.worker_id]._failure_total
                    == failures[termination.worker_id]
                )
        assert probe.breakers[owner]._failure_total == failures[owner] + 1
        for group, cursor in cursors.items():
            assert probe.records(group)[len(before[group]) :] == _expected(cursor), group
        _assert_deliveries(probe, path, floors)
        assert all(spawn.process.poll() is not None for spawn in originals)
        assert not probe.park_evidence
