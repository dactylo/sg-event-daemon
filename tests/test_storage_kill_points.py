from __future__ import annotations

import json
import select
import signal
import sqlite3
import subprocess
import sys
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon._group_state import GroupState
from sg_event_daemon._group_types import AuditIdentity
from tests._kill_point_child import disposition, group_options, open_source
from tests._storage_support import group_lock, write_context_for

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

_GROUPS = ('eager-a', 'eager-b', 'strict')
# The parent records commands, acknowledgements, and recovered in-flight commits.
_SCRIPT = (
    ('open', ''),
    ('publish', ''),
    ('advance', 'eager-a'),
    ('advance', 'eager-b'),
    ('fence', 'strict'),
    ('discover', ''),
    ('reserve', 'eager-a'),
    ('late', 'eager-a'),
    ('reserve', 'strict'),
    ('late', 'strict'),
    ('park', 'eager-a'),
    ('replay', 'eager-a'),
    ('unpark', 'eager-a'),
    ('park', 'eager-b'),
    ('reset', 'eager-b'),
    ('unpark', 'eager-b'),
    ('close', ''),
    ('reserve', 'eager-a'),
    ('late', 'eager-a'),
    ('scan', 'eager-a'),
    ('scan', 'strict'),
    ('advance', 'eager-a'),
    ('advance', 'strict'),
    ('retire', ''),
)
_TARGETS = (0, 1, 2, 7, 11, 14, 19, 22, 23)
pytestmark = pytest.mark.slow


@dataclass
class _GroupOracle:
    cursor: int = 100
    floor: int = 100
    generation: int = 1
    parked: bool = False
    scan_complete: bool = False
    late: str | None = None
    disposition: str | None = None
    terminal: set[int] = field(default_factory=set[int])


@dataclass
class _Oracle:
    frontier: int = 100
    opened: bool = False
    closed: bool = False
    retired: bool = False
    discovered: bool = False
    observed: set[int] = field(default_factory=set[int])
    groups: dict[str, _GroupOracle] = field(
        default_factory=lambda: {
            'eager-a': _GroupOracle(),
            'eager-b': _GroupOracle(cursor=102, floor=102),
            'strict': _GroupOracle(),
        }
    )

    def _lane(self, action: str, group: str) -> None:
        lane = self.groups[group]
        if action == 'advance':
            lane.cursor = 106
            lane.terminal.update(self.observed)
        elif action == 'fence':
            assert lane.cursor == 100
        elif action == 'reserve':
            lane.late = 'pending'
        elif action == 'late':
            lane.late = 'done'
            lane.terminal.add(102)
        elif action == 'scan':
            lane.scan_complete = True
            lane.late = None
        elif action in {'park', 'unpark'}:
            lane.parked = action == 'park'
        else:
            assert action in {'replay', 'reset'}
            lane.disposition = action
            lane.generation += 1
            lane.cursor = lane.floor = 100 if action == 'replay' else self.frontier
            lane.late = None
            # An explicit disposition discharges the prior generation's work.
            lane.terminal.update(self.observed)

    def apply(self, operation: tuple[str, str]) -> None:
        action, group = operation
        if group:
            self._lane(action, group)
        elif action == 'open':
            self.opened = True
            self.frontier = 104
            self.observed.add(104)
        elif action == 'publish':
            self.frontier = 106
            self.observed.update((105, 106))
        elif action == 'discover':
            self.discovered = True
            self.observed.add(102)
        elif action == 'close':
            self.closed = True
        else:
            assert action == 'retire'
            self.retired = True

    def projection(self) -> dict[str, object]:
        retained = self.opened and not self.retired
        return {
            'frontier': self.frontier,
            'groups': [
                (
                    name,
                    lane.generation,
                    'strict' if name == 'strict' else 'eager',
                    lane.cursor,
                    lane.floor,
                    int(lane.parked),
                    lane.cursor,
                    int(lane.disposition == 'replay'),
                    int(lane.late == 'pending'),
                )
                for name, lane in self.groups.items()
            ],
            'ranges': [('range-101-103', 101, 103, 'closed' if self.closed else 'open')]
            if retained
            else [],
            'bindings': [
                (
                    'range-101-103',
                    name,
                    lane.generation,
                    lane.floor,
                    'satisfied-by-floor'
                    if lane.floor >= 103
                    else 'scan-complete'
                    if lane.scan_complete
                    else 'pending',
                )
                for name, lane in self.groups.items()
            ]
            if retained
            else [],
            'source_late': [('range-101-103', 102)] if retained and self.discovered else [],
            'late': [
                (name, 'range-101-103', 102, lane.late)
                for name, lane in self.groups.items()
                if retained and lane.late is not None
            ],
            'markers': [],
        }

    def terminal(self, group: str) -> set[int]:
        lane = self.groups[group]
        return lane.terminal | {event_id for event_id in self.observed if event_id <= lane.floor}


def _assert_unresolved(connection: sqlite3.Connection, oracle: _Oracle) -> None:
    for group, lane in oracle.groups.items():
        unresolved = oracle.observed - oracle.terminal(group)
        if oracle.opened and not oracle.retired and not lane.scan_complete:
            unresolved |= {event_id for event_id in (101, 103) if event_id > lane.floor}
        for event_id in unresolved:
            cursor = connection.execute(
                'SELECT cursor FROM group_state WHERE group_name = ?', (group,)
            ).fetchone()[0]
            pending = connection.execute(
                'SELECT 1 FROM discovery_ranges JOIN range_groups USING (obligation_id) '
                "WHERE group_name = ? AND obligation_state = 'pending' "
                'AND low_id <= ? AND high_id >= ?',
                (group, event_id, event_id),
            ).fetchone()
            marker = connection.execute(
                'SELECT 1 FROM replay_markers WHERE group_name = ? AND from_event_id <= ?',
                (group, event_id),
            ).fetchone()
            assert event_id > cursor or pending or marker, (group, event_id)


def _notice(process: subprocess.Popen[str]) -> str:
    assert process.stdout is not None
    readable, _, _ = select.select([process.stdout], [], [], 15)
    assert readable, 'child protocol timed out'
    result = process.stdout.readline().strip()
    if not result:
        assert process.stderr is not None
        pytest.fail(f'child exited: {process.stderr.read()}')
    return result


@contextmanager
def _child(path: Path, *, poller_only: bool = False) -> Generator[subprocess.Popen[str]]:
    arguments = [sys.executable, '-m', 'tests._kill_point_child', str(path)]
    if poller_only:
        arguments.append('poller')
    with subprocess.Popen(
        arguments, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    ) as process:
        try:
            assert _notice(process) == 'ready'
            yield process
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=15)


def _oracle_from_journal(path: Path, *, include_inflight: bool) -> _Oracle:
    oracle = _Oracle()
    inflight = None
    for line in path.read_text(encoding='utf-8').splitlines():
        phase, action, group = json.loads(line)
        if phase == 'sent':
            inflight = action, group
        else:
            assert phase in {'ack', 'recovered-commit'}
            assert inflight == (action, group)
            oracle.apply((action, group))
            inflight = None
    if include_inflight:
        assert inflight is not None
        oracle.apply(inflight)
    return oracle


def _projection(connection: sqlite3.Connection) -> dict[str, object]:
    queries = {
        'groups': 'SELECT group_name, group_generation, ordering_mode, cursor, delivery_floor, '
        'parked, last_delivered_id, is_operator_replay, operational_late_id_count '
        'FROM groups JOIN group_state USING (group_name) ORDER BY group_name',
        'ranges': 'SELECT obligation_id, low_id, high_id, range_state FROM discovery_ranges '
        'ORDER BY obligation_id',
        'bindings': 'SELECT obligation_id, group_name, group_generation, delivery_floor, '
        'obligation_state FROM range_groups ORDER BY obligation_id, group_name',
        'source_late': 'SELECT obligation_id, event_id FROM source_late_ids ORDER BY event_id',
        'late': 'SELECT group_name, obligation_id, event_id, terminal_state FROM late_ids '
        'ORDER BY group_name, event_id',
        'markers': 'SELECT group_name, from_event_id FROM replay_markers ORDER BY group_name',
    }
    return {
        'frontier': connection.execute('SELECT frontier FROM source_meta').fetchone()[0],
        **{name: connection.execute(query).fetchall() for name, query in queries.items()},
    }


def _record(path: Path, phase: str, operation: tuple[str, str]) -> None:
    with path.open('a', encoding='utf-8') as stream:
        stream.write(json.dumps((phase, *operation)) + '\n')
        stream.flush()


def _send(process: subprocess.Popen[str], operation: tuple[str, str], point: str = '') -> None:
    assert process.stdin is not None
    process.stdin.write(json.dumps((*operation, point)) + '\n')
    process.stdin.flush()


@pytest.mark.parametrize('command', ['replay', 'reset'])
@pytest.mark.parametrize('order', ['lifecycle-first', 'range-first'])
def test_lifecycle_index_change_cannot_leave_stale_range_bindings(
    tmp_path: Path,
    command: str,
    order: str,
) -> None:
    path = tmp_path / 'events.db'
    # Bootstrap once through the same actual owners; killing an idle child leaves
    # the parent's next lifecycle owner free to acquire the group lock.
    with _child(path):
        pass
    writes = write_context_for(path)
    try:
        with GroupState.open(
            path, group_lock(path, 'eager-b'), **group_options(path, 'eager-b', writes)
        ) as group:
            group.park(
                expected_revision=group.snapshot().revision,
                reason='index-race',
                identity=AuditIdentity.current(),
                wall_clock_time=1,
            )
            with _child(path, poller_only=True) as process:
                _send(process, ('open', ''), 'stale' if order == 'lifecycle-first' else 'after')
                expected_notice = 'preflight:1' if order == 'lifecycle-first' else 'kill:after'
                assert _notice(process) == expected_notice
                with closing(sqlite3.connect(path)) as inspection:
                    prior_revision = inspection.execute(
                        'SELECT index_revision FROM source_meta'
                    ).fetchone()[0]
                if order == 'range-first':
                    process.kill()
                    assert process.wait(timeout=15) == -signal.SIGKILL
                disposition(group, command, from_event_id=100)
                with closing(sqlite3.connect(path)) as inspection:
                    assert (
                        inspection.execute('SELECT index_revision FROM source_meta').fetchone()[0]
                        > prior_revision
                    )
                if order == 'lifecycle-first':
                    assert process.stdin is not None
                    process.stdin.write('continue\n')
                    process.stdin.flush()
                    # The first BEGIN used the stale preflight and must roll back.
                    # Reaching attempt 2 proves the in-transaction recheck ran.
                    assert _notice(process) == 'preflight:2'
                    with closing(sqlite3.connect(path)) as inspection:
                        assert inspection.execute(
                            'SELECT COUNT(*) FROM discovery_ranges'
                        ).fetchone() == (0,)
                    process.kill()
                    assert process.wait(timeout=15) == -signal.SIGKILL
        with open_source(path, write_context_for(path)) as source:
            if order == 'lifecycle-first':
                source.commit_observations((104,), successful_clock=0)
            with closing(sqlite3.connect(path)) as inspection:
                floor = 99 if command == 'replay' else 104 if order == 'range-first' else 100
                assert inspection.execute(
                    'SELECT group_generation, delivery_floor, obligation_state FROM range_groups '
                    "WHERE group_name = 'eager-b'"
                ).fetchall() == [(2, floor, 'satisfied-by-floor' if floor >= 103 else 'pending')]
                assert inspection.execute(
                    'SELECT COUNT(*) FROM range_groups AS binding JOIN groups USING (group_name) '
                    'JOIN group_state USING (group_name) '
                    'WHERE binding.group_generation != groups.group_generation '
                    'OR binding.delivery_floor != group_state.delivery_floor'
                ).fetchone() == (0,)
                assert inspection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
    finally:
        writes.lock.close()


@pytest.mark.parametrize('point', ['before', 'after', 'checkpoint'])
@pytest.mark.parametrize(
    'target', _TARGETS, ids=[f'{_SCRIPT[i][0]}-{_SCRIPT[i][1]}' for i in _TARGETS]
)
def test_process_death_preserves_group_work_and_resumes_to_quiescence(
    tmp_path: Path,
    target: int,
    point: str,
) -> None:
    path = tmp_path / 'events.db'
    journal = tmp_path / 'parent-journal.jsonl'
    with _child(path) as process:
        for index, operation in enumerate(_SCRIPT[: target + 1]):
            _record(journal, 'sent', operation)
            _send(process, operation, point if index == target else '')
            if index == target:
                assert _notice(process) == f'kill:{point}'
                break
            assert _notice(process) == 'ack'
            _record(journal, 'ack', operation)
        process.kill()
        assert process.wait(timeout=15) == -signal.SIGKILL
    before = _oracle_from_journal(journal, include_inflight=False)
    after = _oracle_from_journal(journal, include_inflight=True)
    with closing(sqlite3.connect(path)) as connection:
        actual = _projection(connection)
        assert actual in (before.projection(), after.projection())
        expected = before if point == 'before' else after
        assert actual == expected.projection()
        _assert_unresolved(connection, expected)
        assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
    if point != 'before':
        _record(journal, 'recovered-commit', _SCRIPT[target])
    start = target if point == 'before' else target + 1
    with _child(path) as process:
        for operation in _SCRIPT[start:]:
            _record(journal, 'sent', operation)
            _send(process, operation)
            assert _notice(process) == 'ack'
            _record(journal, 'ack', operation)
    final = _oracle_from_journal(journal, include_inflight=False)
    with closing(sqlite3.connect(path)) as connection:
        assert _projection(connection) == final.projection()
        assert final.observed == {102, 104, 105, 106}
        for group in _GROUPS:
            assert final.terminal(group) == final.observed
        assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
