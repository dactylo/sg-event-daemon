from __future__ import annotations

import json
import os
import signal
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pytest

from sg_event_daemon._group_state import GroupState
from sg_event_daemon._group_types import AuditIdentity, StartEventStep
from sg_event_daemon._ipc import OperatorCommand, StopCommand
from sg_event_daemon._platform import spawn_child
from sg_event_daemon._sqlite import wal_file_bytes
from tests import _worker_processes
from tests._signals import publish_text, read_complete_lines
from tests._storage_support import bind_group, insert_range, source_database, write_context_for
from tests._worker_processes import (
    ChildPatches,
    SupervisorProbe,
    Topology,
    running_supervisor,
    write_daemon_config,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from sg_event_daemon._group_types import PreInvocationResult, PreInvocationStep

_GROUPS = ('local', 'named_a', 'named_b', 'named_c', 'remote')
_LAST_ID = 100
# A permanent fixture gap: below every cursor, above every floor, and with no event to redeliver.
_PROBE_ID = 31
_THREADS = ('source:admin', *(f'lane:{group}' for group in _GROUPS))
# Process contention tests need scheduler headroom; timing budgets have dedicated coverage.
_TOPOLOGY = Topology('mixed', _GROUPS)


def _assert_waiter_commits(signals: Path, killed_at: float) -> None:
    for thread in _THREADS[:-1]:
        rows = [line.split() for line in read_complete_lines(signals / f'{thread}.writes')]
        wait = next(row for row in rows if row[0] == 'write-lock-wait')
        hold = next(row for row in rows if row[0] == 'write-hold')
        commit = next(row for row in rows if row[0] == 'commit')
        assert float(wait[1]) < killed_at <= float(hold[1]) <= float(commit[1])
        assert not any(row[0] == 'expired' and float(row[1]) < float(commit[1]) for row in rows)


def _bootstrap_rows(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in read_complete_lines(path)]


def _expected(floor: int) -> tuple[int, ...]:
    return tuple(i for i in range(floor + 1, _LAST_ID + 1) if i not in {7, 19, 31})


def _install_inflight_pause(root: Path) -> None:
    original_commit = GroupState.commit_pre_invocation
    marker = root / 'inflight-event'

    def commit(
        state: GroupState,
        steps: Sequence[PreInvocationStep],
        *,
        expected_revision: int,
    ) -> PreInvocationResult:
        result = original_commit(state, steps, expected_revision=expected_revision)
        if (
            result.rejection is None
            and any(isinstance(step, StartEventStep) for step in steps)
            and result.snapshot.in_flight_event_id is not None
            and not marker.exists()
        ):
            publish_text(marker, str(result.snapshot.in_flight_event_id))
            # The parent kills this process after inspecting the committed row.
            while not (root / 'release-inflight').exists():
                time.sleep(0.005)
        return result

    pytest.MonkeyPatch().setattr(GroupState, 'commit_pre_invocation', commit)


def _outside_waiters(probe: SupervisorProbe) -> bool:
    health = next(iter(probe.latest_health.values()), None)
    return (
        health is not None
        and len(health.sources) == len(health.groups) == 1
        and all(item.phase == 'write-lock-wait' for item in (*health.sources, *health.groups))
    )


def _poller_revisions(probe: SupervisorProbe) -> list[tuple[int, ...]]:
    return [
        tuple(map(int, line.split()))
        for spawn in probe.spawns
        for line in read_complete_lines(spawn.log_path.with_suffix('.revisions'))
    ]


def _scan_revisions(probe: SupervisorProbe) -> list[int]:
    return [
        int(line)
        for spawn in probe.spawns
        for line in read_complete_lines(spawn.log_path.with_suffix('.scans'))
    ]


def _set_event_frontier(root: Path, frontier: int) -> None:
    pending = root / 'visible-events.pending'
    pending.write_text(str(frontier))
    pending.replace(root / 'visible-events')


def _source_metadata(path: Path) -> tuple[str, str | None]:
    with closing(sqlite3.connect(path)) as connection:
        return connection.execute(
            'SELECT source_generation, condition FROM source_meta'
        ).fetchone()


def _start_combined_delivery(probe: SupervisorProbe, root: Path) -> None:
    batch_wait = root / 'combined-batch-wait'
    probe.wait_for(lambda: batch_wait.exists() and bool(batch_wait.read_text()))
    assert int(batch_wait.read_text()) == probe.spawns[0].process.pid
    (root / 'events-ready').touch()
    ready_at = time.time() - 1
    os.utime(root / 'events-ready', (ready_at, ready_at))


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
    )
    (root / 'events-ready').touch()
    probe.wait_for(lambda: all(probe.records(group) for group in _GROUPS))


def _start_gated_local_delivery(probe: SupervisorProbe, root: Path) -> None:
    probe.wait_for(
        lambda: any(
            group.join_phase == 'attached'
            for health in probe.latest_health.values()
            for group in health.groups
        )
    )
    (root / 'events-ready').touch()
    _set_event_frontier(root, 2)
    probe.wait_for(lambda: bool(probe.records('local')))


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


def _write_records(signals: Path, name: str, phase: str) -> list[float]:
    path = signals / f'{name}.writes'
    return [
        float(row[1]) for line in read_complete_lines(path) if (row := line.split())[0] == phase
    ]


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
    _set_event_frontier(tmp_path, 1)
    with running_supervisor(
        config, ChildPatches(last_event_id=_LAST_ID, gate_events=True)
    ) as probe:
        try:
            probe.wait_for(
                lambda: any(
                    group.group == 'local' and group.join_phase == 'attached'
                    for health in probe.latest_health.values()
                    for group in health.groups
                )
            )
            (tmp_path / 'events-ready').touch()
            _set_event_frontier(tmp_path, _LAST_ID)
            probe.wait_for(lambda: marker.exists() and bool(marker.read_text()))
            event_id = int(marker.read_text())
            cursor, _floor, in_flight, delivered = _state(path)['local']
            assert in_flight == event_id
            assert cursor < event_id
            assert delivered < event_id
            assert event_id not in probe.records('local')
            assert len(probe.spawns) == 1
            original = probe.spawns[0]
            original.process.send_signal(signal.SIGKILL)
            probe.wait_for(
                lambda: len(probe.spawns) >= 2, expected_exits=(original.spawn_instance,)
            )
            replacement = probe.spawns[1]
            assert replacement.worker_id == original.worker_id
            assert replacement.spawn_instance != original.spawn_instance
            probe.wait_for(lambda: _state(path)['local'][0] == _LAST_ID)
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
        ChildPatches(last_event_id=_LAST_ID, track_revisions=True, gate_events=True),
    ) as probe:
        _start_delivery(probe, tmp_path)
        floors = {group: row[1] for group, row in _state(path).items()}
        probe.wait_for(lambda: _quiescent(path, _LAST_ID))
        probe.wait_for(lambda: len(probe.latest_health) == 3)
        for worker_id, health in probe.latest_health.items():
            checkpoint_threads = [
                name for name in health.worker.thread_names if name.startswith('checkpoint:')
            ]
            assert bool(checkpoint_threads) == (json.loads(worker_id)[0] == 'site')
        assert len(probe.spawns) == 3, probe.terminations
        assert not probe.terminations
        assert all(spawn.termination_reason is None for spawn in probe.supervisor._spawns.values())
        _assert_deliveries(probe, path, floors)
        writes = write_context_for(path)
        try:
            with (
                closing(sqlite3.connect(path, isolation_level=None)) as connection,
                writes.transaction(connection),
            ):
                insert_range(connection, 'post-close-probe', _PROBE_ID, _PROBE_ID, state='closed')
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
        probe.wait_for(lambda: len(_scan_revisions(probe)) == len(_GROUPS))
        completed_revision = max(_scan_revisions(probe))
        probe.wait_for(
            lambda: any(row[1] >= completed_revision for row in _poller_revisions(probe))
        )


@pytest.mark.slow
def test_remote_stuck_holder_is_terminated_while_waiters_keep_their_workers(
    tmp_path: Path,
) -> None:
    config = write_daemon_config(tmp_path, _TOPOLOGY)
    signals = tmp_path / 'signals'
    signals.mkdir()
    patches = ChildPatches(
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
        probe.wait_for(lambda: _waiters_blocked(probe))
        assert (signals / 'source:admin.local').exists()
        assert (signals / 'lane:named_a.local').exists()
        for worker_id, health in probe.latest_health.items():
            if worker_id != remote:
                for sample in health.watchdogs:
                    if sample.kind in {'source', 'lane'}:
                        assert sample.phase == 'write-lock-wait'
                        assert sample.deadline_at is not None
        (signals / 'expire-hold').touch()
        probe.wait_for(
            lambda: (
                bool(probe.terminations)
                or any(
                    sample.kind == 'lane'
                    and sample.group == 'remote'
                    and sample.phase == 'write-hold'
                    and sample.deadline_at is not None
                    and health.worker.heartbeat_at >= sample.deadline_at
                    for worker_id, health in probe.latest_health.items()
                    if worker_id == remote
                    for sample in health.watchdogs
                )
            )
        )
        assert probe.terminations, probe.failure_details()
        termination = probe.terminations[0]
        assert termination.worker_id == remote
        assert termination.reason == 'lane-phase-overdue'
        probe.wait_for(
            lambda: all((signals / f'{thread}.committed').exists() for thread in _THREADS[:-1]),
            expected_exits=(termination.spawn_instance,),
        )
        _assert_waiter_commits(signals, termination.at)
        probe.wait_for(
            lambda: len(probe.spawns) == 4,
            expected_exits=(termination.spawn_instance,),
        )
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
        probe.wait_for(lambda: _waiters_blocked(probe))
        cursor = _state(path)['remote'][0]
        assert _state(path)['remote'][2] is None
        before = probe.records('remote')
        killed_at = time.monotonic()
        original.process.send_signal(signal.SIGKILL)
        probe.wait_for(
            (signals / 'source:admin.committed').exists, expected_exits=(original.spawn_instance,)
        )
        committed_at = float((signals / 'source:admin.committed').read_text())
        print(f'poller release-to-commit seconds={committed_at - killed_at:.6f}')
        assert committed_at >= killed_at
        probe.wait_for(lambda: len(probe.spawns) == 4, expected_exits=(original.spawn_instance,))
        probe.wait_for(lambda: _quiescent(path, _LAST_ID))
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
        probe.wait_for(lambda: _waiters_blocked(probe))
        cursors = {group: row[0] for group, row in _state(path).items()}
        floors = {group: row[1] for group, row in _state(path).items()}
        assert all(row[2] is None for row in _state(path).values())
        before = {group: probe.records(group) for group in _GROUPS}
        failures = {name: breaker._failure_total for name, breaker in probe.breakers.items()}
        next(spawn for spawn in originals if spawn.worker_id == owner).process.send_signal(
            signal.SIGKILL
        )
        probe.wait_for(
            lambda: len(probe.spawns) == 6,
            expected_exits=tuple(spawn.spawn_instance for spawn in originals),
        )
        probe.wait_for(lambda: _quiescent(path, _LAST_ID))
        restarted = next(spawn for spawn in probe.spawns[3:] if spawn.worker_id == owner)
        revisions = restarted.log_path.with_suffix('.revisions')
        probe.wait_for(lambda: bool(read_complete_lines(revisions)))
        first = tuple(map(int, read_complete_lines(revisions)[0].split()))
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


@pytest.mark.slow
def test_outside_writer_stalls_writes_without_failing(tmp_path: Path) -> None:
    config = write_daemon_config(tmp_path, Topology('default', ('local',)))
    signals = tmp_path / 'signals'
    signals.mkdir()
    _set_event_frontier(tmp_path, 1)
    with running_supervisor(
        config, ChildPatches(signal_dir=signals, gate_lane_write=True, gate_events=True)
    ) as probe:
        _start_gated_local_delivery(probe, tmp_path)
        (signals / 'arm').touch()
        _set_event_frontier(tmp_path, 3)
        probe.wait_for((signals / 'lane:local.queued').exists)
        with closing(
            sqlite3.connect(source_database(tmp_path / 'state'), isolation_level=None)
        ) as holder:
            holder.execute('BEGIN IMMEDIATE')
            (signals / 'write-release').touch()
            probe.wait_for(lambda: _outside_waiters(probe))
            probe.wait_for(
                lambda: all(
                    'write-lock-stalled' in item.conditions
                    for health in probe.latest_health.values()
                    for item in (*health.sources, *health.groups)
                ),
            )
            probe.wait_for(
                lambda: (
                    len(_write_records(signals, 'source:admin', 'expired')) >= 2
                    and len(_write_records(signals, 'lane:local', 'write-lock-wait')) >= 3
                )
            )
            assert not probe.terminations
            assert len(probe.spawns) == 1
            status = probe.supervisor.status_mapping({})
            assert status['primary_state'] == 'write-lock-stalled'
            assert not status['readiness']
            assert status['liveness']
            assert all(
                source.retry_attempt == 0
                for health in probe.latest_health.values()
                for source in health.sources
            )
            assert len(_write_records(signals, 'source:admin', 'expired')) >= 2
            assert len(_write_records(signals, 'lane:local', 'write-lock-wait')) >= 3
            holder.commit()
        for name in ('source:admin', 'lane:local'):
            probe.wait_for(lambda name=name: bool(_write_records(signals, name, 'commit')))
        probe.wait_for(
            lambda: all(
                'write-lock-stalled' not in item.conditions
                for health in probe.latest_health.values()
                for item in (*health.sources, *health.groups)
            ),
        )
        logs = probe.spawns[0].log_path.read_text()
        assert logs.count(' level:') == 2
        assert logs.count('resumed after waiting') == 2
        assert not probe.terminations


@pytest.mark.slow
def test_stop_while_waiting_is_cooperative(tmp_path: Path) -> None:
    config = write_daemon_config(tmp_path, Topology('default', ('local',)))
    signals = tmp_path / 'signals'
    signals.mkdir()
    _set_event_frontier(tmp_path, 1)
    with running_supervisor(
        config, ChildPatches(signal_dir=signals, gate_lane_write=True, gate_events=True)
    ) as probe:
        _start_gated_local_delivery(probe, tmp_path)
        (signals / 'arm').touch()
        _set_event_frontier(tmp_path, 3)
        probe.wait_for((signals / 'lane:local.queued').exists)
        with closing(
            sqlite3.connect(source_database(tmp_path / 'state'), isolation_level=None)
        ) as holder:
            holder.execute('BEGIN IMMEDIATE')
            (signals / 'write-release').touch()
            probe.wait_for(lambda: _outside_waiters(probe))
            spawn = probe.spawns[0]
            worker = next(iter(probe.supervisor._spawns.values())).worker
            worker.send(StopCommand(reason='test-stop'))
            assert spawn.process.wait() == 0
            assert all(
                _write_records(signals, name, 'stopped') for name in ('source:admin', 'lane:local')
            )
            logs = spawn.log_path.read_text()
            assert 'stuck' not in logs
            assert 'thread failed' not in logs
            assert not probe.terminations


@pytest.mark.slow
def test_lane_waits_through_successive_healthy_holders(tmp_path: Path) -> None:
    config = write_daemon_config(tmp_path, Topology('default', ('local',)))
    signals = tmp_path / 'signals'
    signals.mkdir()
    _set_event_frontier(tmp_path, 1)
    patches = ChildPatches(
        signal_dir=signals, gate_lane_write=True, prioritize_competitor=True, gate_events=True
    )
    with running_supervisor(config, patches) as probe:
        _start_gated_local_delivery(probe, tmp_path)
        (signals / 'arm').touch()
        _set_event_frontier(tmp_path, 3)
        probe.wait_for((signals / 'lane:local.queued').exists)
        code = (
            'from pathlib import Path; from tests._worker_processes import run_competing_writer; '
            f'run_competing_writer(Path({str(source_database(tmp_path / "state"))!r}), '
            f'Path({str(signals)!r}))'
        )
        competitor = spawn_child([sys.executable, '-c', code])
        try:
            probe.wait_for((signals / 'competitor-active').exists)
            (signals / 'write-release').touch()
            probe.wait_for(lambda: len(_write_records(signals, 'lane:local', 'expired')) >= 2)
            probe.wait_for(lambda: len(read_complete_lines(signals / 'competitor-commits')) > 20)
            assert not probe.terminations
            assert any(
                'write-lock-stalled' in group.conditions
                for health in probe.latest_health.values()
                for group in health.groups
            )
        finally:
            (signals / 'competitor-pause').touch()
            (signals / 'write-release').touch()
            assert competitor.wait() == 0
        probe.wait_for(lambda: bool(_write_records(signals, 'lane:local', 'commit')))
        probe.wait_for(
            lambda: all(
                'write-lock-stalled' not in group.conditions
                for health in probe.latest_health.values()
                for group in health.groups
            ),
        )
        logs = probe.spawns[0].log_path.read_text().splitlines()
        group_logs = [line for line in logs if 'sg_event_daemon.group:' in line]
        assert sum('waited' in line for line in group_logs) == 1
        assert sum('resumed after waiting' in line for line in group_logs) == 1
        assert not probe.terminations


@pytest.mark.slow
def test_source_serves_coordination_during_lock_retry(tmp_path: Path) -> None:
    groups = ('local', 'strict', 'local_probe', 'remote_probe')
    config = write_daemon_config(tmp_path, Topology('default', groups))
    text = config.read_text().replace(
        '[sites.test.groups.remote_probe]\nworker = "default"',
        '[sites.test.groups.remote_probe]\nworker = "dedicated"',
    )
    text = text.replace(
        '[sites.test.groups.strict]\n',
        '[sites.test.groups.strict]\nordering = "strict"\nstrict_holdback_secs = 0.2\n',
    )
    config.write_text(text)
    signals = tmp_path / 'signals'
    signals.mkdir()
    _set_event_frontier(tmp_path, 1)
    patches = ChildPatches(
        signal_dir=signals, gate_lane_write=True, probe_coordination=True, gate_events=True
    )
    with running_supervisor(config, patches) as probe:
        try:
            probe.wait_for(
                lambda: (
                    {'local', 'strict'}
                    <= {
                        group.group
                        for health in probe.latest_health.values()
                        for group in health.groups
                        if group.join_phase == 'attached'
                    }
                ),
            )
            (tmp_path / 'events-ready').touch()
            _set_event_frontier(tmp_path, 3)
            probe.wait_for(
                lambda: all(
                    (tmp_path / f'{name}.ready').exists()
                    for name in ('local-join', 'remote-join', 'strict-ack')
                ),
            )
            (signals / 'arm').touch()
            _set_event_frontier(tmp_path, 4)
            probe.wait_for((signals / 'lane:local.queued').exists)
            with closing(
                sqlite3.connect(source_database(tmp_path / 'state'), isolation_level=None)
            ) as holder:
                holder.execute('BEGIN IMMEDIATE')
                (signals / 'write-release').touch()
                probe.wait_for(lambda: bool(_write_records(signals, 'source:admin', 'expired')))
                (tmp_path / 'requests-release').touch()
                probe.wait_for(
                    lambda: all(
                        (tmp_path / f'{name}.answered').exists()
                        for name in ('local-join', 'remote-join', 'strict-ack')
                    ),
                )
                for name in ('local-join', 'remote-join', 'strict-ack'):
                    assert float((tmp_path / f'{name}.answered').read_text()) >= float(
                        (tmp_path / f'{name}.queued').read_text()
                    )
                # A halted strict lane can serve a bounded operator write.
                result = probe.supervisor.dispatch_operator_command(
                    OperatorCommand(
                        request_id='busy-park',
                        command='park',
                        arguments={'site': 'test', 'group': 'strict', 'reason': 'test'},
                        identity=AuditIdentity.current(),
                    ),
                    timeout=float('inf'),
                )
                assert not result.ok
                assert result.result['code'] == 'source-database-busy'
                assert not probe.terminations
        finally:
            (tmp_path / 'requests-release').touch()
            (signals / 'write-release').touch()


@pytest.mark.slow
@pytest.mark.parametrize('identity', ['unchanged', 'changed', 'reverted'])
def test_site_worker_restarts_under_an_outside_hold(tmp_path: Path, identity: str) -> None:
    config = write_daemon_config(
        tmp_path,
        Topology('default', ('local',)),
    )
    original_config = config.read_text()
    changed_config = original_config.replace('script_name = "test"', 'script_name = "changed"')
    path = source_database(tmp_path / 'state')
    _set_event_frontier(tmp_path, 1)
    with running_supervisor(config, ChildPatches(probe_startup=True, gate_events=True)) as probe:
        _start_gated_local_delivery(probe, tmp_path)
        if identity == 'reverted':
            config.write_text(changed_config)
            probe.spawns[0].process.send_signal(signal.SIGKILL)
            probe.wait_for(
                lambda: len(probe.spawns) == 2, expected_exits=(probe.spawns[0].spawn_instance,)
            )
            probe.wait_for(lambda: _source_metadata(path)[1] == 'stream-identity-changed')
            config.write_text(original_config)
        elif identity == 'changed':
            config.write_text(changed_config)
        prior = probe.spawns[-1]
        old_generation = _source_metadata(path)[0]
        count = len(probe.spawns)
        with closing(sqlite3.connect(path, isolation_level=None)) as holder:
            holder.execute('BEGIN IMMEDIATE')
            prior.process.send_signal(signal.SIGKILL)
            probe.wait_for(
                lambda: len(probe.spawns) == count + 1, expected_exits=(prior.spawn_instance,)
            )
            replacement = probe.spawns[-1]
            probe.wait_for(
                lambda: bool(_bootstrap_rows(replacement.log_path.with_suffix('.bootstrap'))),
            )
            pending = _bootstrap_rows(replacement.log_path.with_suffix('.bootstrap'))
            assert all(row.get('pending') == 1.0 for row in pending)
            assert _source_metadata(path)[0] == old_generation
            probe.wait_for(
                lambda: probe.supervisor._spawns[replacement.worker_id].lifecycle is None,
            )
            for command, arguments in (
                ('preflight-source-adoption', {'reason': 'test'}),
                (
                    'group-disposition',
                    {
                        'group': 'local',
                        'command': 'replay',
                        'reason': 'test',
                        'disposition_id': 'pending-replay',
                        'reserved_generation': 2,
                        'expected_revision': 0,
                        'from_event_id': 1,
                        'acknowledge_truncation': False,
                    },
                ),
            ):
                result = probe.supervisor.dispatch_operator_command(
                    OperatorCommand(
                        request_id=command,
                        command=command,
                        arguments={'site': 'test', 'source': 'admin', **arguments},
                        identity=AuditIdentity.current(),
                    ),
                    timeout=float('inf'),
                )
                assert not result.ok
                assert result.result.get('code') == 'source-database-busy', result
            assert replacement.process.poll() is None
            holder.commit()
        probe.wait_for(lambda: _source_metadata(path)[0] != old_generation)
        new_generation = _source_metadata(path)[0]
        probe.wait_for(
            lambda: any(
                row.get('generation') == new_generation
                for row in _bootstrap_rows(replacement.log_path.with_suffix('.bootstrap'))
            ),
        )
        assert replacement.process.poll() is None
        assert len(probe.spawns) == count + 1
        assert all(
            item.spawn_instance != replacement.spawn_instance for item in probe.terminations
        )
        if identity == 'changed':
            assert _source_metadata(path)[1] == 'stream-identity-changed'
            result = probe.supervisor.adopt_source_stream_command(
                OperatorCommand(
                    request_id='adopt-after-stamp',
                    command='adopt-source-stream',
                    arguments={'site': 'test', 'source': 'admin', 'reason': 'test'},
                    identity=AuditIdentity.current(),
                ),
                timeout=float('inf'),
            )
            assert result.ok, result
        else:
            assert _source_metadata(path)[1] is None
            before = len(probe.records('local'))
            _set_event_frontier(tmp_path, 3)
            probe.wait_for(lambda: len(probe.records('local')) > before)
            assert len(probe.spawns) == count + 1


@pytest.mark.slow
def test_site_worker_restart_over_a_large_wal_is_ready(tmp_path: Path) -> None:
    groups = tuple(f'group{index}' for index in range(13))
    config = write_daemon_config(
        tmp_path,
        Topology('default', groups, checkpoint_interval_secs=0.2),
    )
    path = source_database(tmp_path / 'state')
    limit = 1_000_000
    _set_event_frontier(tmp_path, 1)
    patches = ChildPatches(
        writer_wal_limit_bytes=limit,
        probe_wal_restart=True,
        gate_events=True,
    )
    with running_supervisor(config, patches) as probe:
        probe.wait_for(
            lambda: (
                set(groups)
                <= {
                    group.group
                    for health in probe.latest_health.values()
                    for group in health.groups
                    if group.join_phase == 'attached'
                }
            )
        )
        (tmp_path / 'events-ready').touch()
        _set_event_frontier(tmp_path, 2)
        probe.wait_for(lambda: all(probe.records(group) for group in groups))
        original = probe.spawns[0]
        with closing(sqlite3.connect(path, isolation_level=None)) as reader:
            reader.execute('BEGIN')
            reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
            _set_event_frontier(tmp_path, patches.last_event_id)
            probe.wait_for(lambda: wal_file_bytes(path) > limit)
            probe.wait_for(
                lambda: any(
                    'checkpoint-stalled' in source.conditions
                    for health in probe.latest_health.values()
                    for source in health.sources
                ),
            )
            assert not probe.terminations
            peak = wal_file_bytes(path)
            original.process.send_signal(signal.SIGKILL)
            assert original.process.wait() == -signal.SIGKILL
            # Keep the connection open to preserve the WAL; release its read pin
            # before the replacement's owner-open TRUNCATE.
            reader.execute('COMMIT')
            probe.wait_for(
                lambda: len(probe.spawns) == 2, expected_exits=(original.spawn_instance,)
            )
            replacement = probe.spawns[1]
            probe.wait_for(
                lambda: any(
                    row['phase'] == 'commit'
                    for row in _bootstrap_rows(replacement.log_path.with_suffix('.wal-writes'))
                ),
            )
            probe.wait_for(lambda: probe.supervisor.probe_result().readiness)
            ready_at = time.monotonic()
            recovered = _bootstrap_rows(replacement.log_path.with_suffix('.wal-recovery'))[0]
            writes = _bootstrap_rows(replacement.log_path.with_suffix('.wal-writes'))
            first = next(row for row in writes if row['phase'] == 'commit')
            assert isinstance(recovered['before'], int)
            recovery_started = recovered['started']
            recovery_finished = recovered['finished']
            first_write_at = first['at']
            assert isinstance(recovery_started, float)
            assert isinstance(recovery_finished, float)
            assert isinstance(first_write_at, float)
            assert recovered['before'] >= peak
            assert recovered['after'] == writes[0]['bytes'] == 0
            assert recovery_finished <= first_write_at <= ready_at
            assert len(probe.terminations) == 1
            assert probe.terminations[0].spawn_instance == original.spawn_instance
            # The control EOF can be observed before waitpid reaps the kill.
            assert probe.terminations[0].reason in {
                'worker-control-eof',
                f'worker-exit-{-signal.SIGKILL}',
            }
            assert sum(breaker._failure_total for breaker in probe.breakers.values()) == 1
            print(
                'MEASUREMENTS '
                + json.dumps(
                    {
                        'test': 'site_worker_restart_over_a_large_wal_is_ready',
                        'growth_bound_bytes': limit,
                        'peak_wal_bytes': peak,
                        'wal_recovery_secs': recovery_finished - recovery_started,
                        'recovery_from_restart_secs': recovery_finished - replacement.started_at,
                        'first_write_from_restart_secs': first_write_at - replacement.started_at,
                        'ready_from_restart_secs': ready_at - replacement.started_at,
                        'startup_deadline_secs': (
                            probe.supervisor._config.daemon.worker_startup_deadline_secs
                        ),
                    }
                )
            )


@pytest.mark.slow
def test_site_worker_refuses_schema_without_metadata(tmp_path: Path) -> None:
    config = write_daemon_config(
        tmp_path,
        Topology('default', ('local',)),
    )
    path = source_database(tmp_path / 'state')
    _set_event_frontier(tmp_path, 1)
    with running_supervisor(config, ChildPatches(gate_events=True)) as probe:
        _start_gated_local_delivery(probe, tmp_path)
        original = probe.spawns[0]
        original.process.send_signal(signal.SIGKILL)
        assert original.process.wait() == -signal.SIGKILL
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute('DELETE FROM source_meta')
        probe.wait_for(lambda: len(probe.spawns) >= 2, expected_exits=(original.spawn_instance,))
        replacement = probe.spawns[1]
        probe.wait_for(
            lambda: (
                replacement.log_path.exists()
                and 'SourceIdentityError' in replacement.log_path.read_text()
            ),
        )
        assert 'source database metadata is missing' in replacement.log_path.read_text()
        with closing(sqlite3.connect(path)) as connection, connection:
            assert connection.execute('SELECT COUNT(*) FROM source_meta').fetchone() == (0,)


@pytest.mark.slow
@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('same_event', [False, True])
@pytest.mark.parametrize('gate', ['before-done', 'after-combined'])
def test_worker_death_around_combined_delivery_commit(
    tmp_path: Path,
    ordering: str,
    same_event: bool,
    gate: Literal['before-done', 'after-combined'],
) -> None:
    config = write_daemon_config(tmp_path, Topology('default', ('local',)))
    plugin = tmp_path / 'combined_plugin.py'
    plugin.write_text(
        'def callback(context):\n    pass\n\n'
        'def register(reg):\n'
        '    reg.add_callback(id="a", callback=callback)\n'
        + ('    reg.add_callback(id="b", callback=callback)\n' if same_event else '')
    )
    config.write_text(
        config.read_text()
        .replace(
            '[sites.test.groups.local]',
            f'[sites.test.groups.local]\nordering = "{ordering}"'
            + ('\nstrict_holdback_secs = 0.01' if ordering == 'strict' else ''),
        )
        .replace(
            str(Path(_worker_processes.__file__).parent / 'fixtures' / 'recording_plugin.py'),
            str(plugin),
        )
    )
    path = source_database(tmp_path / 'state')
    marker = tmp_path / 'combined-gate'
    last_id = 2 if same_event else 3
    with running_supervisor(
        config,
        ChildPatches(
            last_event_id=last_id,
            gate_events=True,
            combined_delivery_gate=gate,
        ),
    ) as probe:
        try:
            _start_combined_delivery(probe, tmp_path)
            probe.wait_for(lambda: marker.exists() and bool(marker.read_text()))
            batch = json.loads((tmp_path / 'combined-batch-pass').read_text())
            assert batch['pid'] == probe.spawns[0].process.pid
            assert batch['queued'] == last_id - 1
            rows = (tmp_path / 'combined-callbacks').read_text().splitlines()
            assert rows == ['2 a 1']
            with closing(sqlite3.connect(path)) as reader:
                cursor, in_flight = reader.execute(
                    'SELECT cursor, in_flight_event_id FROM group_state'
                ).fetchone()
                marks = reader.execute(
                    'SELECT event_id, ordinal, outcome, delivery_attempt '
                    'FROM callback_marks ORDER BY ordinal'
                ).fetchall()
                if gate == 'before-done':
                    assert (cursor, in_flight) == (1, 2)
                    assert marks == (
                        [(2, 0, 'pending', 1), (2, 1, 'pending', 0)]
                        if same_event
                        else [(2, 0, 'pending', 1)]
                    )
                elif same_event:
                    assert (cursor, in_flight) == (1, 2)
                    assert marks == [(2, 0, 'done', 1), (2, 1, 'pending', 1)]
                else:
                    assert (cursor, in_flight) == (2, 3)
                    assert marks == [(3, 0, 'pending', 1)]
            original = probe.spawns[0]
            assert int(marker.read_text()) == original.process.pid
            original.process.send_signal(signal.SIGKILL)
            probe.wait_for(
                lambda: len(probe.spawns) == 2, expected_exits=(original.spawn_instance,)
            )
            probe.wait_for(
                lambda: _state(path)['local'][0] == last_id and _state(path)['local'][2] is None,
            )
            expected = ['2 a 1']
            if gate == 'before-done':
                expected.append('2 a 2')
            expected.append(
                f'{2 if same_event else 3} {"b" if same_event else "a"} '
                f'{1 if gate == "before-done" else 2}'
            )
            assert (tmp_path / 'combined-callbacks').read_text().splitlines() == expected
            assert probe.spawns[1].worker_id == original.worker_id
            assert probe.spawns[1].spawn_instance != original.spawn_instance
            with closing(sqlite3.connect(path)) as reader:
                assert reader.execute('SELECT COUNT(*) FROM callback_marks').fetchone() == (0,)
                assert reader.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        finally:
            (tmp_path / 'combined-release').touch()
