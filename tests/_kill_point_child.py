from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from sg_event_daemon._checkpoint import Checkpointer
from sg_event_daemon._group_recovery import DispositionRequest
from sg_event_daemon._group_state import GroupOpenOptions, GroupState
from sg_event_daemon._group_types import AuditIdentity
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._write_lock import WriteContext
from tests._storage_support import group_lock, write_context_for
from tests.source._support import SOURCE_GENERATION, STREAM

if TYPE_CHECKING:
    from collections.abc import Generator


class _KillWrites(WriteContext):
    armed: str = ''
    commits: int = 0
    attempts: int = 0

    @contextmanager
    def transaction(self, connection: sqlite3.Connection) -> Generator[sqlite3.Connection]:
        if self.armed == 'stale':
            object.__setattr__(self, 'attempts', self.attempts + 1)
            _pause(f'preflight:{self.attempts}')
        with super().transaction(connection) as active:
            yield active
            if self.armed == 'before':
                assert connection.in_transaction
                _pause('kill:before')
        object.__setattr__(self, 'commits', self.commits + 1)
        assert not connection.in_transaction
        if self.armed == 'after':
            _pause('kill:after')
        if self.armed == 'checkpoint':
            checkpointer = Checkpointer(connection, Path('events.db'), interval_secs=1)

            def checkpoint_progress() -> int:
                _pause('kill:checkpoint')
                return 0

            connection.set_progress_handler(checkpoint_progress, 1)
            checkpointer.run_once()
            raise AssertionError('checkpoint did not enter its progress callback')


def _pause(notice: str) -> None:
    print(notice, flush=True)
    assert sys.stdin.readline() == 'continue\n'


def _source_operation(source: SourceState, action: str) -> None:
    if action == 'open':
        source.commit_observations((104,), successful_clock=0)
    elif action == 'publish':
        source.commit_observations((105, 106), successful_clock=0)
    elif action == 'discover':
        source.record_range_scan_page(
            'range-101-103', scanned_through=103, event_ids=(102,), successful_clock=0
        )
    elif action == 'close':
        source.begin_range_scan('range-101-103', successful_clock=2)
        source.record_range_scan_page(
            'range-101-103', scanned_through=103, event_ids=(102,), successful_clock=2
        )
    else:
        assert action == 'retire'
        source.begin_cycle()


def disposition(state: GroupState, command: str, *, from_event_id: int = 101) -> None:
    current = state.snapshot()
    state.dispose(
        DispositionRequest(
            command='replay' if command == 'replay' else 'reset-to-latest',
            group=current.group,
            disposition_id=f'{current.group}-disposition',
            reserved_generation=current.group_generation + 1,
            expected_revision=current.revision,
            from_event_id=from_event_id if command == 'replay' else None,
            acknowledge_truncation=False,
            reason='kill-point acceptance',
            identity=AuditIdentity.current(),
            wall_clock_time=2,
        )
    )


def _lane_operation(state: GroupState, action: str) -> None:
    current = state.snapshot()
    if action in {'advance', 'fence'}:
        result = state.advance_irrelevant_prefix(106, expected_revision=current.revision)
        assert result.cursor == (100 if action == 'fence' else 106)
    elif action == 'reserve':
        state.reserve_late_event('range-101-103', 102, expected_revision=current.revision)
    elif action == 'late':
        state.complete_late_id('range-101-103', 102, expected_revision=current.revision)
    elif action == 'scan':
        state.complete_obligation_scan('range-101-103')
    elif action in {'park', 'unpark'}:
        transition = state.park if action == 'park' else state.unpark
        transition(
            expected_revision=current.revision,
            reason='kill-point acceptance',
            identity=AuditIdentity.current(),
            wall_clock_time=3,
        )
    else:
        assert action in {'replay', 'reset'}
        disposition(state, action)


def group_options(path: Path, group: str, writes: WriteContext) -> GroupOpenOptions:
    with closing(sqlite3.connect(path)) as connection:
        row = connection.execute(
            'SELECT group_generation, ordering_mode FROM groups WHERE group_name = ?', (group,)
        ).fetchone()
    options: GroupOpenOptions = {
        'writes': writes,
        'site': 'studio',
        'source': 'events',
        'group': group,
        'group_generation': 1 if row is None else int(row[0]),
        'ordering': 'strict' if group == 'strict' else 'eager',
        'strict_holdback_secs': 1,
        'site_server_url': 'https://studio.example.invalid',
        'stream_fingerprint': STREAM,
        'source_generation': SOURCE_GENERATION,
        'verified_incarnation': 'incarnation-1',
        'start_mode': 'initialize' if row is None else 'resume',
    }
    if row is None:
        floor = 102 if group == 'eager-b' else 100
        options.update(
            initial_cursor=floor, initial_delivery_floor=floor, initial_last_delivered_id=floor
        )
    return options


def open_source(path: Path, writes: WriteContext) -> SourceState:
    return SourceState.open(
        path,
        writes=writes,
        create=True,
        site='studio',
        source='events',
        stream_fingerprint=STREAM,
        source_generation=SOURCE_GENERATION,
        token_factory=lambda: 'range-101-103',
        visibility_overlap_secs=1,
    )


def main() -> None:
    path = Path(sys.argv[1])
    writes = _KillWrites(write_context_for(path).lock)
    with ExitStack() as stack:
        source = stack.enter_context(open_source(path, writes))
        if not source.snapshot().initialized:
            source.initialize(100, incarnation='incarnation-1')
        groups = (
            {
                name: stack.enter_context(
                    GroupState.open(
                        path, group_lock(path, name), **group_options(path, name, writes)
                    )
                )
                for name in ('eager-a', 'eager-b', 'strict')
            }
            if len(sys.argv) == 2
            else {}
        )
        print('ready', flush=True)
        for line in sys.stdin:
            command = json.loads(line)
            action, group, point = command
            object.__setattr__(writes, 'armed', point)
            prior = writes.commits
            if group:
                _lane_operation(groups[group], action)
            else:
                _source_operation(source, action)
            assert writes.commits > prior
            object.__setattr__(writes, 'armed', '')
            print('ack', flush=True)
    writes.lock.close()


if __name__ == '__main__':
    main()
