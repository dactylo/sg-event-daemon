from __future__ import annotations

import sqlite3
import struct
import threading
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon._watchdog import ComponentWatchdog
from sg_event_daemon._write_lock import (
    WRITE_HOLD_DEADLINE_SECS,
    SourceWriteLock,
    write_lock_timeout_secs,
)
from tests import _worker_processes
from tests._storage_support import source_database
from tests._worker_processes import (
    ChildPatches,
    SupervisorProbe,
    Topology,
    running_supervisor,
    write_daemon_config,
)
from tests.source._support import open_group, open_source
from tests.test_storage_timing import _wait

if TYPE_CHECKING:
    from sg_event_daemon._health import WatchdogHealth
    from sg_event_daemon._write_lock import _Attempt


_REGISTRATION = 'holder'


def _assert_prefix_state(path: Path, *, cursor: int) -> None:
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        assert connection.execute('SELECT low_id, high_id FROM discovery_ranges').fetchall() == [
            (101, 102)
        ]
        assert connection.execute(
            'SELECT group_name, group_generation FROM range_groups'
        ).fetchall() == [('editorial', 7)]
        assert connection.execute(
            'SELECT cursor FROM group_state WHERE group_name = ?', ('editorial',)
        ).fetchone() == (cursor,)


def _commit_ends(wal: bytes) -> tuple[int, ...]:
    page_size = struct.unpack_from('>I', wal, 8)[0]
    frame_size = 24 + page_size
    assert (len(wal) - 32) % frame_size == 0
    return tuple(
        offset + frame_size
        for offset in range(32, len(wal), frame_size)
        if struct.unpack_from('>I', wal, offset + 4)[0]
    )


def _expire_packed_holder(
    probe: SupervisorProbe, signals: Path, owner: str
) -> tuple[WatchdogHealth, ...]:
    observed: dict[str, WatchdogHealth] = {}

    def waiting() -> bool:
        for item in probe.latest_health[owner].watchdogs:
            identity = f'{item.kind}:{item.source if item.kind == "source" else item.group}'
            if item.phase == 'write-lock-wait' and identity in {
                'source:admin',
                'lane:waiter_a',
                'lane:waiter_b',
            }:
                observed[identity] = item
        return len(observed) == 3

    _wait(probe, waiting)
    waiters = tuple(observed.values())
    (signals / 'expire-hold').touch()

    def expiry_observed() -> bool:
        if probe.terminations:
            return True
        health = probe.latest_health[owner]
        return any(
            item.group == 'holder'
            and item.phase == 'write-hold'
            and item.deadline_at is not None
            and health.worker.heartbeat_at >= item.deadline_at
            for item in health.watchdogs
        )

    _wait(probe, expiry_observed, allow_terminations=True)
    assert probe.terminations, probe.failure_details()
    return waiters


def test_explicit_hold_expiry_overrides_the_holder_mask(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    watchdog = ComponentWatchdog(
        kind='lane',
        identity='holder',
        worker_instance='spawn',
        registration_token=_REGISTRATION,
        startup_deadline_secs=30,
        clock=lambda: 10.0,
    )
    watchdog.transition('write-hold', deadline_at=15.0)
    patches = ChildPatches(stall_group='holder', mask_write_hold='holder', signal_dir=tmp_path)
    _worker_processes.install_faults(monkeypatch, patches, ('holder',), 10.0)
    assert watchdog.sample().phase == 'idle'
    (tmp_path / 'expire-hold').touch()
    expired = watchdog.sample()
    assert expired.phase == 'write-hold'
    assert expired.heartbeat_at == 10.0
    assert expired.deadline_at == 9.0


def test_holder_armed_during_acquisition_waits_for_next_rendezvous(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    signals = tmp_path / 'signals'
    signals.mkdir()
    # Release any injected stall immediately so a regression cannot hang the test.
    (signals / 'release').touch()
    patches = ChildPatches(stall_group='holder', signal_dir=signals)
    path = tmp_path / 'source.db'
    with open_source(path, groups={}) as source:
        source.initialize(100, incarnation='incarnation-1')
        with open_group(path, group='holder') as group:
            _worker_processes.install_faults(monkeypatch, patches, ('holder', 'waiter'), 1.0)
            monkeypatch.setattr(threading.current_thread(), 'name', 'sg-event-daemon:lane:holder')
            original_begin = SourceWriteLock._begin

            def arm_during_acquisition(
                lock: SourceWriteLock, attempt: _Attempt, connection: sqlite3.Connection
            ) -> None:
                original_begin(lock, attempt, connection)
                (signals / 'arm').touch()

            monkeypatch.setattr(SourceWriteLock, '_begin', arm_during_acquisition)
            connection = group._database.connection
            with group._writes.transaction(connection):
                assert (signals / 'arm').exists()
                assert not (signals / 'holder.ready').exists()
                assert not (signals / 'holder.stalled').exists()
                assert connection.execute('SELECT COUNT(*) FROM callback_marks').fetchone() == (0,)

            # The next attempt completes its rendezvous and must still inject the fault.
            (signals / 'waiter.ready').touch()
            with group._writes.transaction(connection):
                assert (signals / 'holder.ready').exists()
                assert (signals / 'holder.stalled').exists()
                assert connection.execute('SELECT plugin_id FROM callback_marks').fetchall() == [
                    ('uncommitted-stall',)
                ]


@pytest.mark.slow
def test_same_process_stuck_holder_is_contained_with_its_waiters(
    tmp_path: Path,
) -> None:
    # Keep unrelated clocks live; mark only the held phase overdue after all waiters arrive.
    topology = Topology(packing='default', groups=('holder', 'waiter_a', 'waiter_b'))
    config_path = write_daemon_config(tmp_path, topology)
    signals = tmp_path / 'signals'
    signals.mkdir()
    patches = ChildPatches(
        hold_deadline_secs=WRITE_HOLD_DEADLINE_SECS,
        stall_group='holder',
        mask_write_hold='holder',
        signal_dir=signals,
    )
    # Acquisition must cover a protected hold plus health publication and supervisor polling.
    acquisition_budget = write_lock_timeout_secs(
        topology.health_interval_secs, topology.supervisor_poll_interval_secs
    )
    assert acquisition_budget > (
        WRITE_HOLD_DEADLINE_SECS
        + topology.health_interval_secs
        + topology.supervisor_poll_interval_secs
    )
    with running_supervisor(config_path, patches) as probe:
        try:
            _wait(probe, lambda: all(probe.records(group) for group in topology.groups))
            assert len(probe.spawns) == 1, probe.terminations
            original = probe.spawns[0]
            (signals / 'arm').touch()
            _wait(probe, (signals / 'holder.stalled').exists)
            waiters = _expire_packed_holder(probe, signals, original.worker_id)
            termination = probe.terminations[0]
            assert termination.spawn_instance == original.spawn_instance
            assert termination.reason == 'lane-phase-overdue'
            assert termination.health is not None
            watchdogs = termination.health.watchdogs
            holder = next(item for item in watchdogs if item.group == 'holder')
            assert holder.phase == 'write-hold'
            assert holder.deadline_at is not None
            assert len(waiters) == 3
            for waiter in waiters:
                assert waiter.phase == 'write-lock-wait', (waiter, termination.health)
                assert waiter.deadline_at is not None
            assert not probe.park_evidence
            _wait(
                probe,
                lambda: len(probe.spawns) == 2 and probe.spawns[0].process.poll() is not None,
            )
            replacement = probe.spawns[1]
            _wait(
                probe,
                lambda: any(
                    health.worker.spawn_instance == replacement.spawn_instance
                    and set(health.worker.thread_names)
                    >= {
                        'source:admin',
                        'lane:holder',
                        'lane:waiter_a',
                        'lane:waiter_b',
                    }
                    for health in probe.latest_health.values()
                ),
            )
            assert {item.worker_id for item in probe.terminations} == {original.worker_id}
            assert probe.breakers[original.worker_id]._failure_total == 1
            assert (
                original.process.poll() is not None
            )  # Reaping proves every thread in the exec child stopped.
            assert not probe.park_evidence
            with closing(sqlite3.connect(source_database(tmp_path / 'state'))) as connection:
                assert connection.execute(
                    "SELECT COUNT(*) FROM callback_marks WHERE plugin_id = 'uncommitted-stall'"
                ).fetchone() == (0,)
        finally:
            (signals / 'release').touch()


def test_wal_prefix_loss_keeps_the_range_without_later_consumption(tmp_path: Path) -> None:
    # Simulation of prefix loss, not a physical power-loss or device-durability test.
    path = tmp_path / 'source.db'
    with open_source(path, groups={}) as source:
        source.initialize(100, incarnation='incarnation-1')
        with open_group(path) as group:
            connection = source._database.connection
            connection.execute('PRAGMA wal_autocheckpoint=0')
            group._database.connection.execute('PRAGMA wal_autocheckpoint=0')
            connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            source.commit_observations(
                (103,), successful_clock=1.0
            )  # Commit A: range plus group rows.
            wal_path = Path(f'{path}-wal')
            after_a = wal_path.read_bytes()
            assert _commit_ends(after_a) == (len(after_a),)
            group.advance_irrelevant(103, expected_revision=group.snapshot().revision)  # Commit B.
            wal = wal_path.read_bytes()
            commits = _commit_ends(wal)
            assert commits == (len(after_a), len(wal))
            database = path.read_bytes()
            _assert_prefix_state(path, cursor=103)
            frame_size = 24 + struct.unpack_from('>I', wal, 8)[0]
            damaged = bytearray(wal)
            damaged[commits[1] - frame_size + 24] ^= 1
            variants = {
                'after-a': after_a,
                'inside-b': wal[: commits[0] + 24 + 17],
                'corrupt-b-commit': bytes(damaged),
            }
            for name, prefix in variants.items():
                copy = tmp_path / f'{name}.db'
                copy.write_bytes(database)
                Path(f'{copy}-wal').write_bytes(prefix)
                _assert_prefix_state(copy, cursor=100)
