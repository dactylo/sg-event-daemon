from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
from contextlib import closing, contextmanager
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Literal

import pytest

from sg_event_daemon._fanout import StrictOrderingControl
from sg_event_daemon._group_errors import InvalidTransitionError, RevisionConflictError
from sg_event_daemon._group_recovery import DispositionRequest, LifecycleRequest
from sg_event_daemon._group_state import GroupOpenOptions, GroupState
from sg_event_daemon._group_types import (
    AuditIdentity,
    LiveJoinIdentity,
)
from sg_event_daemon._source_state import SourceIdentityError, SourceState
from sg_event_daemon._write_lock import WriteContext
from tests._storage_support import (
    group_lock,
    insert_range,
    insert_replay_marker,
    write_context_for,
)
from tests.source._support import SOURCE_GENERATION, STREAM, open_source

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from sg_event_daemon._group_types import GroupSnapshot
    from sg_event_daemon._source_state import SourceSnapshot


@dataclass(frozen=True)
class _RecordingWrites(WriteContext):
    connections: list[sqlite3.Connection] = field(default_factory=list[sqlite3.Connection])

    @contextmanager
    def transaction(self, connection: sqlite3.Connection) -> Generator[sqlite3.Connection]:
        self.connections.append(connection)
        with super().transaction(connection) as active:
            yield active


def _adoption_source(path: Path, writes: WriteContext) -> SourceState:
    return SourceState.open(
        path,
        writes=writes,
        create=False,
        site='studio',
        source='events',
        stream_fingerprint='replacement-stream',
        source_generation=SOURCE_GENERATION,
        token_factory=lambda: 'adoption-1',
    )


@contextmanager
def _group(tmp_path: Path) -> Generator[tuple[GroupState, _RecordingWrites]]:
    path = tmp_path / 'events.db'
    with open_source(path, groups={}) as source:
        source.initialize(100, incarnation='incarnation-1')
        writes = _RecordingWrites(write_context_for(path).lock)
        with GroupState.open(
            path,
            group_lock(path, 'editorial'),
            writes=writes,
            site='studio',
            source='events',
            group='editorial',
            group_generation=7,
            ordering='eager',
            site_server_url='https://studio.example.invalid',
            stream_fingerprint=STREAM,
            source_generation=SOURCE_GENERATION,
            verified_incarnation='incarnation-1',
            start_mode='initialize',
            initial_cursor=41,
            initial_delivery_floor=41,
            initial_last_delivered_id=41,
        ) as state:
            current = state.snapshot()
            state.park(
                expected_revision=current.revision,
                reason='recover',
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
            )
            writes.connections.clear()
            yield state, writes


def _adopt(source: SourceState, *, fingerprint: str = 'adopt-command') -> SourceSnapshot:
    return source.adopt_stream(
        adoption_id='adoption-1',
        command_fingerprint=fingerprint,
        new_stream_fingerprint='replacement-stream',
        reason='approved replacement',
        groups_parked=True,
    )


def _options(writes: WriteContext, *, initialize: bool) -> GroupOpenOptions:
    options: GroupOpenOptions = {
        'writes': writes,
        'site': 'studio',
        'source': 'events',
        'group': 'editorial',
        'group_generation': 7,
        'ordering': 'eager',
        'site_server_url': 'https://studio.example.invalid',
        'stream_fingerprint': STREAM,
        'source_generation': SOURCE_GENERATION,
        'verified_incarnation': 'incarnation-1',
        'start_mode': 'initialize' if initialize else 'resume',
    }
    if initialize:
        options.update(initial_cursor=41, initial_delivery_floor=41, initial_last_delivered_id=41)
    return options


def _request(
    state: GroupState, command: Literal['replay', 'reset-to-latest']
) -> DispositionRequest:
    assert command in {'replay', 'reset-to-latest'}
    return DispositionRequest(
        command=command,
        group='editorial',
        disposition_id='7',
        reserved_generation=8,
        expected_revision=state.snapshot().revision,
        from_event_id=30 if command == 'replay' else None,
        acknowledge_truncation=False,
        reason='recover',
        identity=AuditIdentity.current(),
        wall_clock_time=10.0,
    )


def _reserve_adoption(source: SourceState) -> None:
    result = source.reserve_stream_adoption(
        command_fingerprint='adopt-command', reason='approved replacement'
    )
    assert result.adoption_id == 'adoption-1'


def _seed_adoption_evidence(connection: sqlite3.Connection, generation: int = 7) -> None:
    connection.execute("INSERT INTO source_late_ids VALUES ('open', 25)")
    connection.execute("INSERT INTO late_ids VALUES ('editorial', 'open', 25, 'pending', NULL)")
    insert_replay_marker(connection, 'editorial', generation, 20)


@contextmanager
def _adoption(tmp_path: Path) -> Generator[tuple[SourceState, _RecordingWrites]]:
    with _group(tmp_path) as (group, _writes):
        group.dispose(_request(group, 'reset-to-latest'))
        connection = vars(group)['_database'].connection
        insert_range(connection, 'open', 20, 120)
        connection.execute(
            "INSERT INTO range_groups VALUES ('open', 'editorial', 8, 100, 0, 'pending')"
        )
        _seed_adoption_evidence(connection, 8)
    path = tmp_path / 'events.db'
    writes = _RecordingWrites(write_context_for(path).lock)
    with _adoption_source(path, writes) as source:
        writes.connections.clear()
        _reserve_adoption(source)
        assert len(writes.connections) == 1
        assert writes.connections[0].execute('PRAGMA synchronous').fetchone() == (2,)
        writes.connections.clear()
        yield source, writes


class _KillWrites(WriteContext):
    point: str
    oracle: Path
    armed: bool

    @contextmanager
    def transaction(self, connection: sqlite3.Connection) -> Generator[sqlite3.Connection]:
        with super().transaction(connection) as active:
            yield active
            if self.armed:
                with self.oracle.open('w') as stream:
                    json.dump(list(connection.iterdump()), stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                if self.point == 'before':
                    os.kill(os.getpid(), signal.SIGKILL)
        if self.armed and self.point == 'after':
            os.kill(os.getpid(), signal.SIGKILL)


def kill_lifecycle_test_process(path: Path, operation: str, point: str, oracle: Path) -> None:
    writes = _KillWrites(write_context_for(path).lock)
    object.__setattr__(writes, 'point', point)
    object.__setattr__(writes, 'oracle', oracle)
    object.__setattr__(writes, 'armed', operation == 'bootstrap')
    if operation == 'adoption':
        with _adoption_source(path, writes) as source:
            object.__setattr__(writes, 'armed', True)
            _adopt(source)
        return
    with GroupState.open(
        path,
        group_lock(path, 'editorial'),
        **_options(writes, initialize=operation == 'bootstrap'),
    ) as state:
        object.__setattr__(writes, 'armed', True)
        if operation in {'orphan', 'readd'}:
            state.change_lifecycle(
                LifecycleRequest(
                    'orphan' if operation == 'orphan' else 'readd',
                    'active' if operation == 'orphan' else 'orphaned',
                    7,
                    7 if operation == 'orphan' else 8,
                    AuditIdentity.current(),
                    10.0,
                )
            )
            return
        command = 'replay' if operation == 'replay' else 'reset-to-latest'
        state.dispose(_request(state, command))


def test_adoption_rechecks_reservation_after_write_lock_acquisition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _adoption(tmp_path) as (source, writes):
        original = _RecordingWrites.transaction
        changed_state: list[str] = []

        @contextmanager
        def invalidate_reservation(
            context: _RecordingWrites, connection: sqlite3.Connection
        ) -> Generator[sqlite3.Connection]:
            connection.execute("UPDATE pending_adoptions SET prior_incarnation = 'superseded'")
            changed_state.extend(connection.iterdump())
            with original(context, connection) as active:
                yield active

        monkeypatch.setattr(_RecordingWrites, 'transaction', invalidate_reservation)
        with pytest.raises(SourceIdentityError, match='reservation is invalid'):
            _adopt(source)
        assert len(writes.connections) == 1
        connection = vars(source)['_database'].connection
        assert changed_state
        assert list(connection.iterdump()) == changed_state


def test_adoption_retires_evidence_in_one_full_transaction_without_group_mutation(
    tmp_path: Path,
) -> None:
    with _adoption(tmp_path) as (source, writes):
        connection = vars(source)['_database'].connection
        protected = (
            'SELECT * FROM groups',
            'SELECT * FROM group_state',
            'SELECT * FROM audit',
            'SELECT * FROM disposition_audit',
        )
        retained = {query: connection.execute(query).fetchall() for query in protected}
        revisions = connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone()
        adopted = _adopt(source)
        assert adopted == source.snapshot()
        assert not source.snapshot().initialized
        assert len(writes.connections) == 1
        assert writes.connections[0].execute('PRAGMA synchronous').fetchone() == (2,)
        assert connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone() == (
            revisions[0] + 1,
            revisions[1] + 1,
        )
        for query in (
            'SELECT COUNT(*) FROM discovery_ranges',
            'SELECT COUNT(*) FROM range_groups',
            'SELECT COUNT(*) FROM source_late_ids',
            'SELECT COUNT(*) FROM late_ids',
            'SELECT COUNT(*) FROM replay_markers',
            'SELECT COUNT(*) FROM pending_adoptions',
        ):
            assert connection.execute(query).fetchone() == (0,)
        assert connection.execute(
            'SELECT adoption_id, command_fingerprint, stream_fingerprint, reason, '
            'retired_range_count, retired_late_id_count, retired_marker_count '
            'FROM stream_adoptions'
        ).fetchall() == [
            ('adoption-1', 'adopt-command', 'replacement-stream', 'approved replacement', 1, 1, 1)
        ]
        for query, rows in retained.items():
            assert connection.execute(query).fetchall() == rows
        writes.connections.clear()
        assert _adopt(source) == adopted
        assert writes.connections == []
        source.initialize(200)
        assert len(writes.connections) == 1
        assert writes.connections[0].execute('PRAGMA synchronous').fetchone() == (2,)
        initialized = source.snapshot()
        before = list(connection.iterdump())
        writes.connections.clear()
        assert _adopt(source) == initialized
        assert writes.connections == []
        with pytest.raises(SourceIdentityError, match='reused'):
            _adopt(source, fingerprint='conflicting-command')
        assert writes.connections == []
        assert list(connection.iterdump()) == before


@pytest.mark.parametrize(
    'mutation',
    [
        "UPDATE pending_adoptions SET command_fingerprint = 'stale'",
        "UPDATE pending_adoptions SET prior_incarnation = 'stale'",
        "UPDATE pending_adoptions SET prior_stream_fingerprint = 'stale'",
        "UPDATE pending_adoptions SET target_stream_fingerprint = 'stale'",
        "UPDATE pending_adoptions SET reason = 'stale'",
        'DELETE FROM pending_adoptions',
    ],
)
def test_adoption_stale_reservation_rejects_without_write(tmp_path: Path, mutation: str) -> None:
    with _adoption(tmp_path) as (source, writes):
        connection = vars(source)['_database'].connection
        connection.execute(mutation)
        before = list(connection.iterdump())
        with pytest.raises(SourceIdentityError, match='reservation is invalid'):
            _adopt(source)
        assert list(connection.iterdump()) == before
        assert writes.connections == []


def test_bootstrap_binds_only_open_ranges_in_one_full_transaction(tmp_path: Path) -> None:
    path = tmp_path / 'events.db'
    with open_source(path, groups={}) as source:
        source.initialize(100, incarnation='incarnation-1')
        connection = vars(source)['_database'].connection
        insert_range(connection, 'open', 30, 50, opened_successful_at=20.0)
        insert_range(connection, 'closed', 30, 50, state='closed')
        before = connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone()
        writes = _RecordingWrites(write_context_for(path).lock)
        with GroupState.open(
            path,
            group_lock(path, 'editorial'),
            writes=writes,
            site='studio',
            source='events',
            group='editorial',
            group_generation=7,
            ordering='strict',
            site_server_url='https://studio.example.invalid',
            stream_fingerprint=STREAM,
            source_generation=SOURCE_GENERATION,
            verified_incarnation=source.snapshot().stream_incarnation or '',
            start_mode='initialize',
            initial_cursor=41,
            initial_delivery_floor=41,
            initial_last_delivered_id=41,
            strict_holdback_secs=30.0,
        ) as state:
            assert len(writes.connections) == 1
            assert writes.connections[0].execute('PRAGMA synchronous').fetchone() == (2,)
            assert connection.execute(
                'SELECT obligation_id, group_generation, delivery_floor, eligibility_point, '
                'obligation_state FROM range_groups WHERE group_name = ?',
                ('editorial',),
            ).fetchall() == [('open', 7, 41, 50.0, 'pending')]
            assert connection.execute(
                'SELECT index_revision, range_revision FROM source_meta'
            ).fetchone() == (before[0] + 1, before[1] + 1)
            assert state.snapshot().cursor == 41


def test_bootstrap_rejects_old_generation_after_disposition(tmp_path: Path) -> None:
    with _group(tmp_path) as (state, _writes):
        state.dispose(_request(state, 'reset-to-latest'))
    path = tmp_path / 'events.db'
    writes = _RecordingWrites(write_context_for(path).lock)
    with pytest.raises(InvalidTransitionError, match='initialize group generation conflicts'):
        GroupState.open(path, group_lock(path, 'editorial'), **_options(writes, initialize=True))
    assert writes.connections == []


def test_bootstrap_retry_after_progress_returns_without_writes(tmp_path: Path) -> None:
    path = tmp_path / 'events.db'
    with open_source(path, groups={}) as source:
        source.initialize(100, incarnation='incarnation-1')
        with GroupState.open(
            path,
            group_lock(path, 'editorial'),
            **_options(write_context_for(path), initialize=True),
        ) as state:
            current = state.snapshot()
            state.advance_irrelevant(60, expected_revision=current.revision)
            progressed = state.snapshot()
        writes = _RecordingWrites(write_context_for(path).lock)
        with GroupState.open(
            path,
            group_lock(path, 'editorial'),
            **_options(writes, initialize=True),
        ) as state:
            assert state.snapshot() == progressed
            assert writes.connections == []
        options = _options(write_context_for(path), initialize=True)
        options['initial_cursor'] = 40
        with pytest.raises(InvalidTransitionError, match='initialize conflicts'):
            GroupState.open(path, group_lock(path, 'editorial'), **options)


@pytest.mark.parametrize('invalid', ['missing-group', 'generation', 'initialize'])
def test_disposition_acquisition_rejects_without_writes(tmp_path: Path, invalid: str) -> None:
    with _group(tmp_path):
        pass
    path = tmp_path / 'events.db'
    writes = _RecordingWrites(write_context_for(path).lock)
    options = _options(writes, initialize=invalid == 'initialize')
    if invalid == 'missing-group':
        options['group'] = 'missing'
    elif invalid == 'generation':
        options['group_generation'] = 8
    with closing(sqlite3.connect(path)) as connection, connection:
        before = tuple(connection.iterdump())
    with pytest.raises(InvalidTransitionError):
        GroupState.open_for_disposition(path, group_lock(path, 'editorial'), **options)
    assert writes.connections == []
    with closing(sqlite3.connect(path)) as connection, connection:
        assert tuple(connection.iterdump()) == before
    with GroupState.open_for_disposition(
        path, group_lock(path, 'editorial'), **_options(write_context_for(path), initialize=False)
    ) as reopened:
        assert reopened.snapshot().group_generation == 7


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_disposition_commits_once_and_retries_after_frontier_change(
    tmp_path: Path,
    command: Literal['replay', 'reset-to-latest'],
) -> None:
    with _group(tmp_path) as (state, writes):
        request = _request(state, command)
        connection = vars(state)['_database'].connection
        insert_range(connection, 'open', 20, 120)
        insert_range(connection, 'closed', 20, 120, state='closed')
        before = connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone()
        result = state.dispose(request)
        assert len(writes.connections) == 1
        assert writes.connections[0].execute('PRAGMA synchronous').fetchone() == (2,)
        assert result.group_generation == 8
        assert result.cursor == (29 if command == 'replay' else 100)
        assert connection.execute(
            'SELECT obligation_id, group_generation FROM range_groups ORDER BY obligation_id'
        ).fetchall() == [('open', 8)]
        assert connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone() == (before[0] + 1, before[1] + 1)
        assert connection.execute(
            "SELECT frontier, terminal_result FROM disposition_audit WHERE disposition_id = '7'"
        ).fetchone() == (100, 'applied')
        assert connection.execute(
            "SELECT command FROM audit WHERE disposition_id = '7' ORDER BY audit_id"
        ).fetchall() == [('bootstrap',), (command,)]
        connection.execute('UPDATE source_meta SET frontier = 150')
        writes.connections.clear()
        assert state.dispose(replace(request, wall_clock_time=20.0)) == result
        assert writes.connections == []
        with pytest.raises(InvalidTransitionError, match='conflicting content'):
            state.dispose(replace(request, reason='different'))
        assert writes.connections == []


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_disposition_stale_fence_rejects_before_any_write(
    tmp_path: Path, command: Literal['replay', 'reset-to-latest']
) -> None:
    with _group(tmp_path) as (state, writes):
        before = state.snapshot()
        request = replace(_request(state, command), expected_revision=before.revision - 1)
        with pytest.raises(RevisionConflictError):
            state.dispose(request)
        assert state.snapshot() == before
        assert writes.connections == []


def test_initialize_rejects_retired_current_generation_without_writes(tmp_path: Path) -> None:
    with _group(tmp_path) as (state, _writes):
        state.change_lifecycle(
            LifecycleRequest('orphan', 'active', 7, 7, AuditIdentity.current(), 2.0)
        )
    path = tmp_path / 'events.db'
    writes = _RecordingWrites(write_context_for(path).lock)
    with pytest.raises(InvalidTransitionError, match='initialize group lifecycle conflicts'):
        GroupState.open(path, group_lock(path, 'editorial'), **_options(writes, initialize=True))
    assert writes.connections == []


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_new_disposition_rejects_changed_source_authority_without_mutation(
    tmp_path: Path,
    command: Literal['replay', 'reset-to-latest'],
) -> None:
    with _group(tmp_path) as (state, writes):
        connection = vars(state)['_database'].connection
        connection.execute("UPDATE source_meta SET source_generation = 'replacement-source'")
        before = list(connection.iterdump())
        with pytest.raises(InvalidTransitionError, match='source authority changed'):
            state.dispose(_request(state, command))
        assert list(connection.iterdump()) == before
        assert len(writes.connections) == 1


def test_old_adoption_receipt_cannot_replace_a_later_stream(tmp_path: Path) -> None:
    with _adoption(tmp_path) as (source, writes):
        _adopt(source)
        source.initialize(200)
        connection = vars(source)['_database'].connection
        connection.execute("UPDATE source_meta SET condition = 'stream-identity-changed'")
        vars(source)['_token_factory'] = lambda: 'adoption-2'
        reservation = source.reserve_stream_adoption(
            command_fingerprint='later-command', reason='later replacement'
        )
        source.adopt_stream(
            adoption_id=reservation.adoption_id,
            command_fingerprint=reservation.command_fingerprint,
            new_stream_fingerprint='replacement-stream',
            reason='later replacement',
            groups_parked=True,
        )
        before = list(connection.iterdump())
        writes.connections.clear()
        with pytest.raises(SourceIdentityError, match='not current'):
            _adopt(source)
        assert writes.connections == []
        assert list(connection.iterdump()) == before


@pytest.mark.parametrize('operation', ['orphan', 'readd'])
def test_orphan_and_readd_are_atomic_audited_generation_fenced(
    tmp_path: Path, operation: str
) -> None:
    with _group(tmp_path) as (state, writes):
        connection = vars(state)['_database'].connection
        insert_range(connection, 'pending', 20, 90)
        connection.execute(
            "INSERT INTO range_groups VALUES ('pending', 'editorial', 7, 41, 0, 'pending')"
        )
        identity = AuditIdentity.current()
        if operation == 'readd':
            state.change_lifecycle(LifecycleRequest('orphan', 'active', 7, 7, identity, 2.0))
        writes.connections.clear()
        before = connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone()
        request = LifecycleRequest(
            command='readd' if operation == 'readd' else 'orphan',
            expected_lifecycle='orphaned' if operation == 'readd' else 'active',
            expected_generation=7,
            transition_generation=8 if operation == 'readd' else 7,
            identity=identity,
            wall_clock_time=3.0,
        )
        old_state = list(connection.iterdump())
        with pytest.raises(InvalidTransitionError, match='generation'):
            state.change_lifecycle(replace(request, expected_generation=6))
        with pytest.raises(InvalidTransitionError, match='lifecycle fence'):
            state.change_lifecycle(replace(request, expected_lifecycle='readded-parked'))
        assert list(connection.iterdump()) == old_state
        assert writes.connections == []
        prior_revision = state.snapshot().revision
        result = state.change_lifecycle(request)
        assert len(writes.connections) == 1
        assert writes.connections[0].execute('PRAGMA synchronous').fetchone() == (2,)
        assert result.parked
        assert connection.execute('SELECT orphaned FROM group_state').fetchone() == (
            int(operation == 'orphan'),
        )
        assert result.revision == prior_revision + 1
        assert result.replay_required_from == 20
        assert connection.execute(
            "SELECT group_generation, active, lifecycle FROM groups WHERE group_name = 'editorial'"
        ).fetchone() == (7, 0, 'orphaned' if operation == 'orphan' else 'readded-parked')
        assert connection.execute(
            'SELECT index_revision, range_revision FROM source_meta'
        ).fetchone() == (before[0] + 1, before[1] + 1)
        assert connection.execute(
            "SELECT from_event_id FROM replay_markers WHERE group_name = 'editorial'"
        ).fetchone() == (20,)
        assert connection.execute('SELECT COUNT(*) FROM range_groups').fetchone() == (0,)
        assert connection.execute(
            'SELECT command, terminal_result, local_uid, username, pid '
            'FROM audit ORDER BY audit_id DESC LIMIT 1'
        ).fetchone() == (operation, 'applied', identity.local_uid, identity.username, identity.pid)
        applied_state = list(connection.iterdump())
        writes.connections.clear()
        assert state.change_lifecycle(replace(request, wall_clock_time=4.0)) == result
        assert writes.connections == []
        with pytest.raises(InvalidTransitionError, match='conflict'):
            state.change_lifecycle(replace(request, expected_lifecycle='readded-parked'))
        assert writes.connections == []
        assert list(connection.iterdump()) == applied_state
        state.dispose(_request(state, 'reset-to-latest'))
        newer_state = list(connection.iterdump())
        writes.connections.clear()
        assert state.change_lifecycle(request) == result
        assert writes.connections == []
        with pytest.raises(InvalidTransitionError, match='generation'):
            state.change_lifecycle(replace(request, transition_generation=9))
        assert writes.connections == []
        assert list(connection.iterdump()) == newer_state


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_repeated_readd_intents_keep_distinct_receipts_until_disposition(
    tmp_path: Path, command: Literal['replay', 'reset-to-latest']
) -> None:
    with _group(tmp_path) as (state, writes):
        connection = vars(state)['_database'].connection
        insert_range(connection, 'pending', 20, 90)
        connection.execute(
            "INSERT INTO range_groups VALUES ('pending', 'editorial', 7, 41, 0, 'pending')"
        )
        identity = AuditIdentity.current()
        requests = (
            LifecycleRequest('orphan', 'active', 7, 7, identity, 2.0),
            LifecycleRequest('readd', 'orphaned', 7, 8, identity, 3.0),
            LifecycleRequest('orphan', 'readded-parked', 7, 8, identity, 4.0),
            LifecycleRequest('readd', 'orphaned', 7, 9, identity, 5.0),
        )
        receipts: list[GroupSnapshot] = []
        for request in requests:
            prior_revision = state.snapshot().revision
            writes.connections.clear()
            result = state.change_lifecycle(request)
            receipts.append(result)
            assert result.revision == prior_revision + 1
            assert result.group_generation == 7
            assert result.parked
            assert connection.execute('SELECT orphaned FROM group_state').fetchone() == (
                int(request.command == 'orphan'),
            )
            assert result.replay_required_from == 20
            assert len(writes.connections) == 1
            assert writes.connections[0].execute('PRAGMA synchronous').fetchone() == (2,)
            current_state = list(connection.iterdump())
            writes.connections.clear()
            for historical_request, receipt in zip(requests, receipts, strict=False):
                assert state.change_lifecycle(historical_request) == receipt
            assert writes.connections == []
            assert list(connection.iterdump()) == current_state
        assert connection.execute(
            "SELECT command, disposition_id FROM audit WHERE command IN ('orphan', 'readd') "
            'ORDER BY audit_id'
        ).fetchall() == [('orphan', '7'), ('readd', '8'), ('orphan', '8'), ('readd', '9')]
        disposition = replace(
            _request(state, command),
            disposition_id='9',
            reserved_generation=9,
            from_event_id=20 if command == 'replay' else None,
        )
        writes.connections.clear()
        installed = state.dispose(disposition)
        assert installed.group_generation == 9
        assert installed.cursor == (19 if command == 'replay' else 100)
        assert connection.execute('SELECT orphaned FROM group_state').fetchone() == (0,)
        assert installed.replay_required_from is None
        assert connection.execute(
            "SELECT group_generation, active, lifecycle FROM groups WHERE group_name = 'editorial'"
        ).fetchone() == (9, 1, 'active')
        assert connection.execute('SELECT COUNT(*) FROM replay_markers').fetchone() == (0,)
        assert len(writes.connections) == 1
        assert writes.connections[0].execute('PRAGMA synchronous').fetchone() == (2,)
        state.change_lifecycle(LifecycleRequest('orphan', 'active', 9, 9, identity, 11.0))
        state.change_lifecycle(LifecycleRequest('readd', 'orphaned', 9, 10, identity, 12.0))
        newer_state = list(connection.iterdump())
        writes.connections.clear()
        assert state.dispose(replace(disposition, wall_clock_time=20.0)) == installed
        for request, receipt in zip(requests, receipts, strict=True):
            assert state.change_lifecycle(request) == receipt
        with pytest.raises(InvalidTransitionError, match='generation'):
            state.change_lifecycle(replace(requests[-1], transition_generation=11))
        with pytest.raises(RevisionConflictError):
            state.dispose(replace(disposition, disposition_id='10', reserved_generation=10))
        with pytest.raises(InvalidTransitionError, match='conflicting content'):
            state.dispose(replace(disposition, reason='different'))
        assert writes.connections == []
        assert list(connection.iterdump()) == newer_state


@pytest.mark.parametrize(
    'operation', ['bootstrap', 'replay', 'reset-to-latest', 'orphan', 'readd', 'adoption']
)
@pytest.mark.parametrize('point', ['before', 'after'])
def test_lifecycle_kill_at_commit_preserves_exact_old_or_new_state(
    tmp_path: Path,
    operation: str,
    point: str,
) -> None:
    path = tmp_path / 'events.db'
    with open_source(path, groups={}) as source:
        source.initialize(100, incarnation='incarnation-1')
        connection = vars(source)['_database'].connection
        insert_range(connection, 'open', 20, 120)
        if operation != 'bootstrap':
            with GroupState.open(
                path,
                group_lock(path, 'editorial'),
                **_options(write_context_for(path), initialize=True),
            ) as state:
                state.park(
                    expected_revision=state.snapshot().revision,
                    reason='recover',
                    identity=AuditIdentity.current(),
                    wall_clock_time=1.0,
                )
                if operation == 'readd':
                    state.change_lifecycle(
                        LifecycleRequest('orphan', 'active', 7, 7, AuditIdentity.current(), 2.0)
                    )
        if operation == 'adoption':
            _seed_adoption_evidence(connection)
        before = list(connection.iterdump())
    if operation == 'adoption':
        with _adoption_source(path, write_context_for(path)) as source:
            _reserve_adoption(source)
            before = list(vars(source)['_database'].connection.iterdump())
    oracle = tmp_path / 'transaction-oracle.json'
    result = subprocess.run(
        [
            sys.executable,
            '-c',
            (
                'from pathlib import Path; import sys; '
                'from tests.test_lifecycle_transactions import kill_lifecycle_test_process; '
                'kill_lifecycle_test_process(Path(sys.argv[1]), '
                'sys.argv[2], sys.argv[3], Path(sys.argv[4]))'
            ),
            str(path),
            operation,
            point,
            str(oracle),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    assert oracle.exists()
    with closing(sqlite3.connect(path)) as inspection, inspection:
        expected = before if point == 'before' else json.loads(oracle.read_text())
        assert list(inspection.iterdump()) == expected
        assert inspection.execute('PRAGMA integrity_check').fetchone() == ('ok',)
        if operation == 'adoption':
            assert inspection.execute('SELECT COUNT(*) FROM pending_adoptions').fetchone() == (
                int(point == 'before'),
            )
            assert inspection.execute('SELECT COUNT(*) FROM stream_adoptions').fetchone() == (
                int(point == 'after'),
            )
            for query in (
                'SELECT COUNT(*) FROM discovery_ranges',
                'SELECT COUNT(*) FROM range_groups',
                'SELECT COUNT(*) FROM source_late_ids',
                'SELECT COUNT(*) FROM late_ids',
                'SELECT COUNT(*) FROM replay_markers',
            ):
                assert inspection.execute(query).fetchone() == (int(point == 'before'),)
            assert inspection.execute(
                'SELECT parked, stream_identity_changed FROM group_state'
            ).fetchone() == (1, 0)
        elif point == 'after':
            lifecycle_change = operation in {'orphan', 'readd'}
            assert inspection.execute('SELECT COUNT(*) FROM range_groups').fetchone() == (
                0 if lifecycle_change else 1,
            )
            assert inspection.execute('SELECT COUNT(*) FROM replay_markers').fetchone() == (
                int(lifecycle_change),
            )
            assert inspection.execute('SELECT COUNT(*) FROM disposition_audit').fetchone() == (
                int(operation in {'replay', 'reset-to-latest'}),
            )
            assert inspection.execute(
                'SELECT command, terminal_result FROM audit ORDER BY audit_id DESC LIMIT 1'
            ).fetchone() == (operation, 'applied')
            if lifecycle_change:
                assert inspection.execute(
                    'SELECT group_generation, active, lifecycle FROM groups'
                ).fetchone() == (7, 0, 'orphaned' if operation == 'orphan' else 'readded-parked')
                assert inspection.execute(
                    'SELECT orphaned, parked, replay_required_from FROM group_state'
                ).fetchone() == (int(operation == 'orphan'), 1, 20)
                assert inspection.execute(
                    'SELECT from_event_id FROM replay_markers'
                ).fetchone() == (20,)


@pytest.mark.parametrize(
    'operation',
    ['history', 'floor', 'strict-floor', 'strict-outcome', 'strict-ordering', 'strict-capacity'],
)
def test_only_lifecycle_transactions_use_the_full_connection(
    tmp_path: Path,
    operation: str,
) -> None:
    path = tmp_path / 'events.db'
    with open_source(path, groups={}) as source:
        source.initialize(100, incarnation='incarnation-1')
        writes = _RecordingWrites(write_context_for(path).lock)
        options = _options(writes, initialize=True)
        options['ordering'] = 'strict'
        with GroupState.open(path, group_lock(path, 'editorial'), **options) as state:
            assert [
                connection.execute('PRAGMA synchronous').fetchone()[0]
                for connection in writes.connections
            ] == [2]
            current_join = 'current-join'
            capacity_join = 'capacity-join'
            identity = LiveJoinIdentity(
                site='studio',
                source='events',
                group='editorial',
                join_kind='strict',
                source_generation=SOURCE_GENERATION,
                group_generation=7,
                boundary=100,
                attempt_token=current_join,
            )
            if operation in {'strict-ordering', 'strict-capacity'}:
                state.advance_irrelevant(100, expected_revision=state.snapshot().revision)
                state.commit_live_join(identity, expected_revision=state.snapshot().revision)
            writes.connections.clear()
            revision = state.snapshot().revision
            if operation == 'history':
                state.mark_history_truncated(45, expected_revision=revision)
            elif operation == 'floor':
                state.record_delivery_floor_skip(40, expected_revision=revision)
            elif operation == 'strict-floor':
                state.record_strict_floor_skip(40, expected_revision=revision)
            elif operation == 'strict-outcome':
                state.record_strict_join_outcome(
                    attempt_token=capacity_join,
                    capacity_caused=True,
                    abort_limit=3,
                    expected_revision=revision,
                )
            elif operation == 'strict-ordering':
                state.record_strict_ordering_decision(
                    StrictOrderingControl(identity, 99, 100, 41, 'halt'),
                    expected_revision=revision,
                )
            else:
                state.mark_strict_capacity_exhausted(
                    identity, abort_limit=3, expected_revision=revision
                )
            assert len(writes.connections) == 1
            assert writes.connections[0].execute('PRAGMA synchronous').fetchone() == (1,)


def test_source_initialization_is_full_but_restart_is_configured(tmp_path: Path) -> None:
    path = tmp_path / 'events.db'
    for create in (True, False):
        writes = _RecordingWrites(write_context_for(path).lock)
        with SourceState.open(
            path,
            writes=writes,
            create=create,
            site='studio',
            source='events',
            stream_fingerprint=STREAM,
            source_generation=SOURCE_GENERATION,
        ) as source:
            if create:
                source.initialize(100, incarnation='incarnation-1')
            assert [
                connection.execute('PRAGMA synchronous').fetchone()[0]
                for connection in writes.connections
            ] == ([2, 2, 2] if create else [1])
