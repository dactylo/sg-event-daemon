from __future__ import annotations

import sqlite3
import sys
import tracemalloc
from contextlib import closing
from types import SimpleNamespace
from typing import TYPE_CHECKING, Literal, TypedDict, cast
from unittest.mock import Mock

import pytest

import sg_event_daemon._group_delivery as group_delivery
import sg_event_daemon._group_types as group_types
from sg_event_daemon._group_errors import InvalidTransitionError, RevisionConflictError
from sg_event_daemon._group_recovery import DispositionRequest
from sg_event_daemon._group_state import GroupOpenOptions, GroupState
from sg_event_daemon._group_types import (
    AuditIdentity,
    BeginCallbackStep,
    CompleteLateStep,
    LiveJoinIdentity,
    ReserveLateStep,
    StartEventStep,
)
from sg_event_daemon._health import GroupHealth
from sg_event_daemon._lane import LaneHealth
from sg_event_daemon._platform import AlreadyLockedError, FileLock
from sg_event_daemon._plugins import CallbackIdentity, CallbackManifest
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._sqlite import DatabaseHandle
from sg_event_daemon._supervisor import Supervisor
from tests._storage_support import (
    bind_group,
    create_source_database,
    group_lock,
    insert_range,
    insert_source_late_ids,
    open_group_on_source,
    open_test_group,
    write_context_for,
)
from tests.group._support import commit_step
from tests.source._support import open_source

if TYPE_CHECKING:
    from pathlib import Path


class _BootstrapIdentity(TypedDict):
    site: str
    site_server_url: str
    source: str
    group: str
    group_generation: int
    ordering: Literal['eager', 'strict']
    stream_fingerprint: str
    source_generation: str
    verified_incarnation: str


class _CloseProbe:
    def __init__(
        self,
        name: str,
        operations: list[str],
        error: KeyboardInterrupt | None,
    ) -> None:
        self.error = error
        self._name = name
        self._operations = operations
        self.traceback: object | None = None

    def close(self) -> None:
        self._operations.append(self._name)
        if self.error is not None:
            try:
                raise self.error
            except KeyboardInterrupt as error:
                self.traceback = error.__traceback__
                raise


def _capacity_pending_count(state: GroupState) -> int:
    connection = vars(state)['_database'].connection
    return int(
        connection.execute(
            'SELECT COUNT(*) FROM late_ids WHERE group_name = ? '
            "AND terminal_state = 'capacity-pending'",
            ('editorial',),
        ).fetchone()[0]
    )


def _group_state_with_close_probes(
    failures: frozenset[str],
) -> tuple[GroupState, dict[str, _CloseProbe], list[str]]:
    operations: list[str] = []
    probes = {
        name: _CloseProbe(
            name,
            operations,
            KeyboardInterrupt(f'{name} close failed') if name in failures else None,
        )
        for name in ('database', 'handoff', 'lock', 'write-lock')
    }
    state = GroupState.__new__(GroupState)
    vars(state).update(
        {
            '_database': probes['database'],
            '_handoff': probes['handoff'],
            '_lock': probes['lock'],
            '_writes': SimpleNamespace(lock=probes['write-lock']),
        }
    )
    return state, probes, operations


def _late_capacity_resume_options(path: Path, limit: int) -> GroupOpenOptions:
    return {
        'writes': write_context_for(path),
        'site': 'studio',
        'site_server_url': 'https://studio.example.invalid',
        'source': 'events',
        'group': 'editorial',
        'group_generation': 7,
        'ordering': 'eager',
        'stream_fingerprint': 'stream-v1',
        'source_generation': 'source-spawn-1',
        'verified_incarnation': 'incarnation-1',
        'start_mode': 'resume',
        'max_late_ids_per_group': limit,
    }


def _obligation(state: GroupState, *, closed: bool = False) -> None:
    connection = vars(state)['_database'].connection
    insert_range(connection, 'range-1', 42, 44, state='closed' if closed else 'open')
    bind_group(connection, 'range-1', 'editorial', 7, floor=41, eligibility_point=0)
    if closed:
        insert_source_late_ids(connection, 'range-1', (42, 43, 44))


def _open_group(
    path: Path,
    *,
    generation: int = 7,
    max_group_obligations: int = 1_000,
    max_late_ids_per_group: int = 50_000,
    ordering: Literal['eager', 'strict'] = 'eager',
) -> GroupState:
    return open_test_group(
        path,
        group_generation=generation,
        max_group_obligations=max_group_obligations,
        max_late_ids_per_group=max_late_ids_per_group,
        ordering=ordering,
    )


def _create_halted_late_capacity_group(path: Path) -> int:
    with _open_group(path, max_late_ids_per_group=1) as state:
        _obligation(state)
        reserved = commit_step(state, ReserveLateStep('range-1', 42), expected_revision=0)
        connection = vars(state)['_database'].connection
        connection.execute(
            "INSERT INTO late_ids VALUES ('editorial', 'range-1', 43, 'capacity-pending', 1)"
        )
        connection.execute(
            "INSERT INTO late_ids VALUES ('editorial', 'range-1', 44, 'capacity-pending', 2)"
        )
        halted = state.snapshot()
        assert reserved.late_id_count == 1
        assert _capacity_pending_count(state) == 2
        assert halted.status == 'late-capacity-exhausted'
        return halted.revision


def manifest(*callback_ids: str) -> CallbackManifest:
    return CallbackManifest(
        callbacks=tuple(
            CallbackIdentity(
                ordinal=ordinal,
                plugin_id='publish',
                callback_id=callback_id,
                execution='sync',
                filter_digest=f'filter-{ordinal}',
                config_digest=f'config-{ordinal}',
                secret_refs=(),
                credential_name='publisher',
                credential_fingerprint='publisher:publish-script',
            )
            for ordinal, callback_id in enumerate(callback_ids)
        )
    )


def test_bootstrap_initializes_split_cursor_floor_and_comparison_atomically(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    options: _BootstrapIdentity = {
        'site': 'studio',
        'site_server_url': 'https://studio.example.invalid',
        'source': 'events',
        'group': 'editorial',
        'group_generation': 7,
        'ordering': 'strict',
        'stream_fingerprint': 'stream-v1',
        'source_generation': 'source-spawn-1',
        'verified_incarnation': 'incarnation-1',
    }

    with open_group_on_source(
        path,
        writes=write_context_for(path),
        **options,
        start_mode='initialize',
        initial_cursor=41,
        initial_delivery_floor=57,
        initial_last_delivered_id=57,
    ) as initialized:
        snapshot = initialized.snapshot()
        bootstrap = initialized._bootstrap_state()
        assert snapshot.cursor == 41
        assert snapshot.delivery_floor == 57
        assert bootstrap.last_delivered_id == 57

    with open_group_on_source(
        path, writes=write_context_for(path), **options, start_mode='resume'
    ) as resumed:
        snapshot = resumed.snapshot()
        bootstrap = resumed._bootstrap_state()
        assert snapshot.cursor == 41
        assert snapshot.delivery_floor == 57
        assert bootstrap.last_delivered_id == 57


def test_bootstrap_start_mode_must_match_reconciled_durable_state(
    tmp_path: Path,
) -> None:
    missing = tmp_path / 'missing.db'
    options: _BootstrapIdentity = {
        'site': 'studio',
        'site_server_url': 'https://studio.example.invalid',
        'source': 'events',
        'group': 'editorial',
        'group_generation': 7,
        'ordering': 'eager',
        'stream_fingerprint': 'stream-v1',
        'source_generation': 'source-spawn-1',
        'verified_incarnation': 'incarnation-1',
    }

    with pytest.raises(InvalidTransitionError, match=r'resume.*missing'):
        open_group_on_source(
            missing, writes=write_context_for(missing), **options, start_mode='resume'
        )

    existing = tmp_path / 'existing.db'
    with open_group_on_source(
        existing,
        writes=write_context_for(existing),
        **options,
        start_mode='initialize',
        initial_cursor=41,
        initial_delivery_floor=57,
        initial_last_delivered_id=57,
    ):
        pass
    with pytest.raises(InvalidTransitionError, match=r'initialize.*existing'):
        open_group_on_source(
            existing,
            writes=write_context_for(existing),
            **options,
            start_mode='initialize',
            initial_cursor=99,
            initial_delivery_floor=100,
            initial_last_delivered_id=100,
        )


def test_close_exhausts_owned_resources_when_every_close_succeeds() -> None:
    state, _probes, operations = _group_state_with_close_probes(frozenset())

    state.close()

    assert operations == ['database', 'handoff', 'lock', 'write-lock']


def test_combined_commit_advances_the_revision_per_step_and_conflicts_whole(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _open_group(tmp_path / 'group.db') as state:
        _obligation(state, closed=True)
        steps = (
            group_types.ReserveLateStep('range-1', 42),
            group_types.StartEventStep(42, manifest('one'), 'range-1'),
            group_types.BeginCallbackStep(0),
        )
        with closing(sqlite3.connect(tmp_path / 'group.db')) as operator, operator:
            operator.execute(
                "UPDATE group_state SET revision = revision + 1 WHERE group_name = 'editorial'"
            )
        with pytest.raises(RevisionConflictError):
            state.commit_pre_invocation(steps, expected_revision=0)
        assert state.snapshot().late_id_count == 0
        constructor = group_types.GroupSnapshot
        counted = Mock(wraps=constructor)
        for module in tuple(sys.modules.values()):
            for name, value in tuple(vars(module).items()):
                if value is constructor:
                    monkeypatch.setattr(module, name, counted)
        result = state.commit_pre_invocation(steps, expected_revision=1)
        assert counted.call_count == 1
        assert result.rejection is None
        assert result.snapshot.revision == 4
        assert result.snapshot.callback_marks[0][4] == 1
        assert result.snapshot == state.snapshot()


def test_disposition_open_does_not_promote_late_capacity(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    revision_before = _create_halted_late_capacity_group(path)

    with GroupState.open_for_disposition(
        path, group_lock(path, 'editorial'), **_late_capacity_resume_options(path, 3)
    ) as state:
        assert _capacity_pending_count(state) == 2
        assert state.snapshot().revision == revision_before


def test_first_start_full_commits_identity_and_position(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        snapshot = state.snapshot()
        assert snapshot.site == 'studio'
        assert snapshot.source == 'events'
        assert snapshot.group == 'editorial'
        assert snapshot.group_generation == 7
        assert snapshot.revision == 0
        assert snapshot.status == 'running'
        assert snapshot.conditions == ()
        assert snapshot.cursor == 41
        assert snapshot.delivery_floor == 41
        assert snapshot.verified_incarnation == 'incarnation-1'

    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute(
            """
            SELECT site, site_server_url, source_name, group_name, group_generation,
                   ordering_mode, stream_fingerprint
            FROM groups CROSS JOIN source_meta
            WHERE group_name = 'editorial'
            """
        ).fetchone() == (
            'studio',
            'https://studio.example.invalid',
            'events',
            'editorial',
            7,
            'eager',
            'stream-v1',
        )


def test_group_audit_count_returns_exact_owner_row_count(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        commit_step(state, StartEventStep(42, manifest('publish'), None), expected_revision=0)
        commit_step(state, BeginCallbackStep(0), expected_revision=1)
        state.resolve_conditional_park(
            group_types.ConditionalParkDirective(
                evidence_id='evidence-audit-count',
                site='studio',
                group='editorial',
                group_generation=7,
                failed_spawn_instance='worker-spawn-1',
                event_id=42,
                group_revision=2,
                observed_at_wall_clock=1000.5,
            ),
            identity=AuditIdentity.current(),
            decided_at_wall_clock=1001.0,
        )

        assert state.audit_count() == 2


def test_group_open_requires_explicit_start_mode(tmp_path: Path) -> None:
    options: dict[str, object] = {
        'writes': write_context_for(tmp_path / 'group.db'),
        'site': 'studio',
        'site_server_url': 'https://studio.example.invalid',
        'source': 'events',
        'group': 'editorial',
        'group_generation': 1,
        'ordering': 'eager',
        'stream_fingerprint': 'stream-v1',
        'source_generation': 'source-1',
        'verified_incarnation': 'incarnation-1',
    }
    with pytest.raises(KeyError, match='start_mode'):
        GroupState.open(
            tmp_path / 'group.db',
            group_lock(tmp_path / 'group.db', 'editorial'),
            **cast('GroupOpenOptions', options),
        )


def test_lane_lock_fails_before_a_second_owner_can_mutate_state(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as first:
        before = first.snapshot()
        with pytest.raises(AlreadyLockedError):
            _open_group(path)
        assert first.snapshot() == before


@pytest.mark.parametrize(
    ('limit', 'pending_after', 'status'),
    [(3, 0, 'running'), (2, 1, 'halted'), (1, 2, 'halted')],
)
def test_lane_open_promotes_late_capacity_under_current_limit(
    tmp_path: Path, limit: int, pending_after: int, status: str
) -> None:
    path = tmp_path / 'editorial.db'
    revision_before = _create_halted_late_capacity_group(path)

    with GroupState.open(
        path, group_lock(path, 'editorial'), **_late_capacity_resume_options(path, limit)
    ) as state:
        current = state.snapshot()
        assert _capacity_pending_count(state) == pending_after
        assert (current.status == 'running') is (status == 'running')
        if pending_after == 2:
            assert current.revision == revision_before
        else:
            assert current.revision == revision_before + 1
        assert current.late_id_count == 3 - pending_after


@pytest.mark.parametrize('invalid_mode', ['legacy', 'other'])
def test_group_open_rejects_invalid_start_mode(
    invalid_mode: str,
    tmp_path: Path,
) -> None:
    options: dict[str, object] = {
        'writes': write_context_for(tmp_path / 'group.db'),
        'site': 'studio',
        'site_server_url': 'https://studio.example.invalid',
        'source': 'events',
        'group': 'editorial',
        'group_generation': 1,
        'ordering': 'eager',
        'stream_fingerprint': 'stream-v1',
        'source_generation': 'source-1',
        'verified_incarnation': 'incarnation-1',
        'start_mode': invalid_mode,
        'initial_cursor': 0,
        'initial_delivery_floor': 0,
        'initial_last_delivered_id': 0,
    }
    with (
        pytest.raises(ValueError, match='group start mode must be initialize or resume'),
        open_group_on_source(tmp_path / 'group.db', **cast('GroupOpenOptions', options)),
    ):
        pass


@pytest.mark.parametrize(
    ('failures', 'first_failure'),
    [
        (frozenset({'database'}), 'database'),
        (frozenset({'handoff'}), 'handoff'),
        (frozenset({'database', 'handoff'}), 'database'),
    ],
)
def test_close_exhausts_owned_resources_after_database_failures(
    failures: frozenset[str],
    first_failure: str,
) -> None:
    state, probes, operations = _group_state_with_close_probes(failures)
    first_error = probes[first_failure].error
    assert first_error is not None

    with pytest.raises(KeyboardInterrupt) as raised:
        state.close()

    traceback = raised.value.__traceback__
    while traceback is not None and traceback.tb_next is not None:
        traceback = traceback.tb_next
    assert operations == ['database', 'handoff', 'lock', 'write-lock']
    assert raised.value is first_error
    assert traceback is probes[first_failure].traceback
    if len(failures) > 1:
        assert any('handoff close failed' in note for note in first_error.__notes__)


@pytest.mark.parametrize('database_failures', [frozenset({0}), frozenset({1}), frozenset({0, 1})])
def test_failed_open_attempts_all_owned_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    database_failures: frozenset[int],
) -> None:
    attempts: list[str] = []
    initiating = ValueError('construction failed')
    cleanup = OSError('database cleanup failure')
    original_database_close = DatabaseHandle.close
    original_lock_close = FileLock.close

    def close_database(handle: DatabaseHandle) -> None:
        original_database_close(handle)
        attempts.append('database')
        if len(attempts) - 1 in database_failures:
            raise cleanup

    def close_lock(lock: FileLock) -> None:
        original_lock_close(lock)
        attempts.append('lock')
        if len(database_failures) > 1:
            raise KeyboardInterrupt('lock cleanup failure')

    def fail_construction(*_args: object, **_kwargs: object) -> None:
        raise initiating

    create_source_database(tmp_path / 'state.db', site='studio', source='events')
    monkeypatch.setattr(DatabaseHandle, 'close', close_database)
    monkeypatch.setattr(FileLock, 'close', close_lock)
    monkeypatch.setattr(GroupState, '__init__', fail_construction)
    with pytest.raises(ValueError, match='failed') as caught:
        _open_group(tmp_path / 'state.db')
    assert caught.value is initiating
    assert attempts == ['database', 'database', 'lock']
    assert len(caught.value.__notes__) == len(database_failures) + (len(database_failures) > 1)
    assert all(
        'database cleanup failure' in note
        for note in caught.value.__notes__[: len(database_failures)]
    )
    if len(database_failures) > 1:
        assert 'lock cleanup failure' in caught.value.__notes__[-1]


def test_live_join_validation_does_not_wait_for_a_writer(tmp_path: Path) -> None:
    path = tmp_path / 'group.db'
    with _open_group(path) as state:
        before = state.snapshot()
        identity = LiveJoinIdentity(
            site='studio',
            source='events',
            group='editorial',
            join_kind='eager',
            source_generation='source-spawn-1',
            group_generation=7,
            boundary=41,
            attempt_token=path.name,
        )
        writer = sqlite3.connect(path, isolation_level=None)
        try:
            writer.execute('BEGIN IMMEDIATE')
            state._database.connection.execute('PRAGMA busy_timeout=0')
            assert state.validate_live_join(identity, before.revision) == before
        finally:
            writer.rollback()
            writer.close()


@pytest.mark.parametrize(
    ('changed_ordering', 'changed_fingerprint', 'primary_condition'),
    [
        ('eager', 'stream-v2', 'stream-identity-changed'),
        ('strict', 'stream-v1', 'ordering-changed'),
    ],
)
def test_obligation_cap_collapse_preserves_independent_lifecycle_condition(
    changed_fingerprint: str,
    changed_ordering: Literal['eager', 'strict'],
    primary_condition: str,
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path, max_group_obligations=1) as state:
        _obligation(state)
    with open_group_on_source(
        path,
        writes=write_context_for(path),
        site='studio',
        site_server_url='https://studio.example.invalid',
        source='events',
        group='editorial',
        group_generation=7,
        ordering=changed_ordering,
        stream_fingerprint=changed_fingerprint,
        source_generation='source-spawn-1',
        verified_incarnation='incarnation-1',
        start_mode='resume',
        max_group_obligations=1,
    ) as changed:
        with SourceState.open(
            path,
            writes=write_context_for(path),
            create=False,
            site='studio',
            source='events',
            stream_fingerprint='stream-v1',
            source_generation='source-spawn-1',
            max_group_obligations=1,
        ) as source:
            source.initialize(41, incarnation='incarnation-1')
            source.commit_observations((49,), successful_clock=1)
        collapsed = changed.snapshot()
        assert collapsed.replay_required_from == 42
        assert collapsed.obligation_count == 0
        assert primary_condition in collapsed.conditions
        assert 'replay-required' in collapsed.conditions
        assert collapsed.status == primary_condition
    with _open_group(path, max_group_obligations=1) as restored:
        current = restored.snapshot()
        assert current.status == 'replay-required'
        assert current.conditions == ('replay-required',)
        with pytest.raises(InvalidTransitionError):
            restored.advance_irrelevant(42, expected_revision=current.revision)


def test_old_terminal_receipt_cannot_clear_stream_mismatch_after_adoption(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    fingerprint = 'stream-v1'

    def open_group(generation: int, incarnation: str) -> GroupState:
        options: GroupOpenOptions = {
            'writes': write_context_for(path),
            'site': 'studio',
            'site_server_url': 'https://studio.example.invalid',
            'source': 'events',
            'group': 'editorial',
            'group_generation': generation,
            'ordering': 'eager',
            'stream_fingerprint': fingerprint,
            'source_generation': 'source-1',
            'verified_incarnation': incarnation,
            'start_mode': 'initialize' if generation == 1 else 'resume',
        }
        if generation == 1:
            options['initial_cursor'] = 100
            options['initial_delivery_floor'] = 100
            options['initial_last_delivered_id'] = 100
        return open_group_on_source(path, **options)

    with SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site='studio',
        source='events',
        stream_fingerprint=fingerprint,
        source_generation='source-1',
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        with open_group(1, 'incarnation-1') as group:
            Supervisor._orphan_group(source, group)
            group.dispose(
                DispositionRequest(
                    command='reset-to-latest',
                    group='editorial',
                    disposition_id='reset-to-latest-readd',
                    reserved_generation=2,
                    expected_revision=group.snapshot().revision,
                    from_event_id=None,
                    acknowledge_truncation=False,
                    reason='configured group re-added',
                    identity=AuditIdentity.current(),
                    wall_clock_time=10.0,
                )
            )
            terminal = group.terminal_disposition_identity(2)
            assert terminal is not None

        source._database.connection.execute(
            "UPDATE source_meta SET condition = 'stream-identity-changed' WHERE singleton = 1"
        )
        vars(source)['_token_factory'] = lambda: 'adoption-1'
        reservation = source.reserve_stream_adoption(
            command_fingerprint='adoption-fingerprint-1',
            reason='test adoption',
        )
        assert reservation.adoption_id == 'adoption-1'
        source.adopt_stream(
            adoption_id=reservation.adoption_id,
            command_fingerprint=reservation.command_fingerprint,
            new_stream_fingerprint=fingerprint,
            reason='test adoption',
            groups_parked=True,
        )
        source.initialize(200)
        with open_group(2, 'adoption-1') as changed:
            snapshot = changed.snapshot()
            assert snapshot.status == 'stream-identity-changed'
            assert changed.terminal_disposition_identity(2) == terminal
            assert snapshot.verified_incarnation == 'incarnation-1'
        assert source.snapshot().adoption_count == 1


@pytest.mark.parametrize(('synchronous', 'expected'), [(None, 1), ('NORMAL', 1), ('FULL', 2)])
def test_primary_synchronous_policy_survives_reopen_with_full_handoff(
    tmp_path: Path,
    synchronous: str | None,
    expected: int,
) -> None:
    path = tmp_path / 'group.db'
    options: GroupOpenOptions = {
        'writes': write_context_for(path),
        'site': 'studio',
        'site_server_url': 'https://studio.example.invalid',
        'source': 'events',
        'group': 'editorial',
        'group_generation': 7,
        'ordering': 'eager',
        'stream_fingerprint': 'stream-v1',
        'source_generation': 'source-spawn-1',
        'verified_incarnation': 'incarnation-1',
        'start_mode': 'initialize',
    }
    if synchronous is not None:
        options['synchronous'] = synchronous
    for start_mode in ('initialize', 'resume'):
        options['start_mode'] = start_mode
        if start_mode == 'initialize':
            options['initial_cursor'] = 41
            options['initial_delivery_floor'] = 41
            options['initial_last_delivered_id'] = 41
        else:
            del options['initial_cursor']
            del options['initial_delivery_floor']
            del options['initial_last_delivered_id']
        with open_group_on_source(path, **options) as state:
            primary = cast('DatabaseHandle', vars(state)['_database']).connection
            handoff = cast('DatabaseHandle', vars(state)['_handoff']).connection
            assert primary.execute('PRAGMA synchronous').fetchone() == (expected,)
            assert handoff.execute('PRAGMA synchronous').fetchone() == (2,)


def test_refresh_source_facts_and_rows_share_one_read_transaction(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with open_source(path, configured_stream='stream-v1') as source, _open_group(path) as state:
        source.initialize(50, incarnation='incarnation-1')
        connection = state._handoff.connection
        connection.execute("""
            UPDATE source_meta SET initialized=1, frontier=50, successful_clock=1,
            source_generation='source-spawn-1', stream_incarnation='incarnation-1'
            """)
        insert_range(connection, 'range-a', 42, 50)
        bind_group(connection, 'range-a', 'editorial', 7, floor=41, eligibility_point=0)
        writer = sqlite3.connect(path, isolation_level=None)
        changed = False

        def after_meta(statement: str) -> None:
            nonlocal changed
            if changed or 'FROM range_groups AS bindings' not in statement:
                return
            changed = True
            writer.execute("""
            UPDATE discovery_ranges SET range_state='closed', close_generation=1,
            source_revision=2 WHERE obligation_id='range-a'
            """)
            insert_source_late_ids(writer, 'range-a', (42,))
            writer.execute('UPDATE source_meta SET frontier=60, successful_clock=2')

        primary = state._database.connection
        primary.set_trace_callback(after_meta)
        try:
            before, old_ranges = state.refresh_ranges()
            assert old_ranges is not None
            (old_range,) = old_ranges
        finally:
            primary.set_trace_callback(None)
            writer.close()
        assert changed
        assert (before.frontier, before.source_successful_clock) == (50, 1)
        assert not old_range.closed
        assert old_range.visibility_deadline == 300.0
        after, pending = state.refresh_ranges(before.range_revision)
        assert pending is not None
        assert (after.frontier, after.source_successful_clock) == (60, 2)
        assert pending[0].closed
        assert pending[0].visibility_deadline == old_range.visibility_deadline


def test_restart_preserves_in_flight_manifest_marks_and_attempts(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        commit_step(
            state, StartEventStep(42, manifest('prepare', 'publish'), None), expected_revision=0
        )
        commit_step(state, BeginCallbackStep(0), expected_revision=1)
        before_restart = state.callback_done(0, expected_revision=2)
    with _open_group(path) as reopened:
        assert reopened.snapshot() == before_restart
        retried = commit_step(reopened, BeginCallbackStep(1), expected_revision=3)
        assert retried.callback_marks[1][-1] == 1


def test_stream_and_ordering_mismatches_project_independently_and_restore(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path):
        pass
    with open_group_on_source(
        path,
        writes=write_context_for(path),
        site='studio',
        site_server_url='https://changed.example.invalid',
        source='events',
        group='editorial',
        group_generation=7,
        ordering='strict',
        stream_fingerprint='stream-v2',
        source_generation='source-spawn-2',
        verified_incarnation='incarnation-2',
        start_mode='resume',
    ) as changed:
        current = changed.snapshot()
        assert current.status == 'stream-identity-changed'
        assert current.conditions == (
            'stream-identity-changed',
            'ordering-changed',
        )
        with pytest.raises(InvalidTransitionError):
            changed.advance_irrelevant(42, expected_revision=current.revision)
    with _open_group(path) as restored:
        assert restored.snapshot().conditions == ()
        assert restored.snapshot().cursor == 41


@pytest.mark.parametrize(
    ('case', 'limit', 'expected'),
    [
        ('duplicate', 10, (1, (11,))),
        ('open', 10, (1, (11,))),
        ('stale', 10, (1, (11,))),
        ('scan-complete', 10, (1, (11,))),
        ('done', 10, (0, ())),
        ('zero-limit', 0, (1, ())),
    ],
)
def test_unresolved_late_id_projection_cases(
    tmp_path: Path, case: str, limit: int, expected: tuple[int, tuple[int, ...]]
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        connection = vars(state)['_database'].connection
        insert_range(connection, 'a', 10, 12, state='closed')
        bind_group(connection, 'a', 'editorial', 7, floor=0, eligibility_point=0)
        insert_source_late_ids(connection, 'a', (11,))
        if case in {'duplicate', 'open', 'stale', 'scan-complete'}:
            insert_range(connection, 'b', 11, 13, state='open' if case == 'open' else 'closed')
            bind_group(
                connection,
                'b',
                'editorial',
                6 if case == 'stale' else 7,
                floor=0,
                eligibility_point=0,
                state='scan-complete' if case == 'scan-complete' else 'pending',
            )
            insert_source_late_ids(connection, 'b', (11 if case == 'duplicate' else 12,))
        if case == 'done':
            connection.execute("INSERT INTO late_ids VALUES ('editorial', 'a', 11, 'done', NULL)")

        assert state.unresolved_late_id_projection(limit=limit) == expected


@pytest.mark.parametrize('after_write', [False, True])
def test_combined_commit_rolls_back_rejected_step_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    after_write: bool,
) -> None:
    original = group_delivery.begin_callback_step

    def reject_after_write(
        connection: sqlite3.Connection,
        group: str,
        ordinal: int,
        revision: int,
    ) -> int:
        original(connection, group, ordinal, revision)
        raise InvalidTransitionError('rejected after writing attempt')

    if after_write:
        monkeypatch.setattr(group_delivery, 'begin_callback_step', reject_after_write)
    with _open_group(tmp_path / 'group.db') as state:
        _obligation(state, closed=True)
        result = state.commit_pre_invocation(
            (
                group_types.ReserveLateStep('range-1', 42),
                group_types.StartEventStep(42, manifest('one'), 'range-1'),
                group_types.BeginCallbackStep(0 if after_write else 1),
            ),
            expected_revision=0,
        )
        assert isinstance(result.rejection, InvalidTransitionError)
        assert result.snapshot.revision == 2
        assert result.snapshot.in_flight_event_id == 42
        assert result.snapshot.callback_marks[0][4] == 0
        assert result.snapshot == state.snapshot()


@pytest.mark.parametrize('final', [False, True])
@pytest.mark.parametrize('rejection', ['capacity', 'cas', 'fence'])
def test_done_step_savepoint_preserves_only_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    final: bool,
    rejection: str,
) -> None:
    path = tmp_path / 'group.db'
    with _open_group(path) as state:
        started = commit_step(
            state,
            StartEventStep(42, manifest('a') if final else manifest('a', 'b'), None),
            expected_revision=0,
        )
        attempted = commit_step(state, BeginCallbackStep(0), expected_revision=started.revision)
        original = group_delivery.begin_callback_step
        error_type = (
            RevisionConflictError
            if rejection == 'cas'
            else (
                group_delivery.StrictObligationBlockedError
                if rejection == 'fence'
                else InvalidTransitionError
            )
        )

        def reject(connection: sqlite3.Connection, group: str, ordinal: int, revision: int) -> int:
            original(connection, group, ordinal, revision)
            raise error_type(f'{rejection} rejected next attempt')

        monkeypatch.setattr(group_delivery, 'begin_callback_step', reject)
        steps = (group_types.CallbackDoneStep(0),)
        if final:
            steps += (group_types.StartEventStep(43, manifest('b'), None),)
        result = state.commit_pre_invocation(
            (*steps, group_types.BeginCallbackStep(0 if final else 1)),
            expected_revision=attempted.revision,
        )
        assert type(result.rejection) is error_type
        assert str(result.rejection) == f'{rejection} rejected next attempt'
        assert result.snapshot.revision == attempted.revision + 1
        assert result.snapshot.cursor == (42 if final else 41)
        assert result.snapshot.in_flight_event_id == (None if final else 42)
        assert [mark[3:] for mark in result.snapshot.callback_marks] == (
            [] if final else [('done', 1), ('pending', 0)]
        )
        assert result.snapshot == state.snapshot()
        with closing(sqlite3.connect(path)) as reader, reader:
            assert reader.execute(
                'SELECT event_id, ordinal, outcome, delivery_attempt '
                'FROM callback_marks ORDER BY ordinal'
            ).fetchall() == ([] if final else [(42, 0, 'done', 1), (42, 1, 'pending', 0)])
        monkeypatch.setattr(group_delivery, 'begin_callback_step', original)
        retried = state.commit_pre_invocation(
            ((group_types.StartEventStep(43, manifest('b'), None),) if final else ())
            + (group_types.BeginCallbackStep(0 if final else 1),),
            expected_revision=result.snapshot.revision,
        )
        assert retried.rejection is None
        assert retried.snapshot.callback_marks[-1][4] == 1


@pytest.mark.parametrize('case', ['stale', 'invalid', 'fence'])
def test_rejected_done_step_commits_no_success(tmp_path: Path, case: str) -> None:
    path = tmp_path / 'group.db'
    with _open_group(path, ordering='strict') as state:
        started = commit_step(
            state,
            StartEventStep(42, manifest('a') if case == 'fence' else manifest('a', 'b'), None),
            expected_revision=0,
        )
        attempted = commit_step(state, BeginCallbackStep(0), expected_revision=started.revision)
        if case == 'fence':
            # Publish a fence that rejects completion itself.
            _obligation(state)
        before = state.snapshot()
        steps = (
            group_types.CallbackDoneStep(1 if case == 'invalid' else 0),
            group_types.BeginCallbackStep(1),
        )
        if case == 'stale':
            with pytest.raises(RevisionConflictError):
                state.commit_pre_invocation(steps, expected_revision=attempted.revision - 1)
        else:
            result = state.commit_pre_invocation(steps, expected_revision=attempted.revision)
            assert result.snapshot == before
            assert isinstance(result.rejection, InvalidTransitionError)
            assert str(result.rejection) == (
                'callback was not invoked and pending'
                if case == 'invalid'
                else 'strict delivery is blocked by an unresolved obligation'
            )
        assert state.snapshot() == before
        with closing(sqlite3.connect(path)) as reader, reader:
            assert reader.execute(
                "SELECT COUNT(*) FROM callback_marks WHERE outcome = 'done'"
            ).fetchone() == (0,)
            assert reader.execute('SELECT cursor FROM group_state').fetchone() == (41,)


def test_unresolved_late_id_projection_reports_only_pending_callback_work(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'editorial.db', max_late_ids_per_group=2) as state:
        _obligation(state, closed=True)
        commit_step(
            state, ReserveLateStep('range-1', 42), expected_revision=state.snapshot().revision
        )
        commit_step(
            state, ReserveLateStep('range-1', 43), expected_revision=state.snapshot().revision
        )
        commit_step(
            state, CompleteLateStep('range-1', 43), expected_revision=state.snapshot().revision
        )
        commit_step(
            state, ReserveLateStep('range-1', 44), expected_revision=state.snapshot().revision
        )

        expected_late_ids = (42, 44)
        unresolved_count, unresolved_late_ids = state.unresolved_late_id_projection(
            limit=len(expected_late_ids)
        )
        health = GroupHealth.from_lane(
            LaneHealth(
                site='studio',
                source='events',
                group='editorial',
                worker_instance='worker-1',
                execution='sync',
                ordering='eager',
                phase='idle',
                status='running',
                cursor=41,
                event_id=None,
                plugin_id=None,
                callback_id=None,
                delivery_attempt=None,
                heartbeat_at=10.0,
                heartbeat_age=0.0,
                deadline_at=None,
                internal_stall_deadline_secs=5.0,
                overdue_reason=None,
            ),
            queue_event_count=0,
            queue_byte_count=0,
            catchup_frontier=None,
            pending_catchup_epoch_count=0,
            oldest_catchup_epoch_age=None,
            join_phase='attached',
            outstanding_late_obligation_count=1,
            outstanding_late_id_count=unresolved_count,
            outstanding_late_ids=unresolved_late_ids,
            outstanding_late_ids_truncated=False,
            parked=False,
            park_reason=None,
            failure_kind=None,
            failure_text=None,
            conditions=(),
        )

        assert unresolved_count == len(expected_late_ids)
        assert unresolved_late_ids == expected_late_ids
        assert health.outstanding_late_id_count == len(expected_late_ids)
        assert health.outstanding_late_ids == expected_late_ids


def test_unresolved_late_id_projection_uses_scalar_and_bounded_queries(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        connection = vars(state)['_database'].connection
        insert_range(connection, 'range-1', 1, 50_000, state='closed')
        bind_group(connection, 'range-1', 'editorial', 7, floor=41, eligibility_point=0)
        connection.execute('BEGIN')
        insert_source_late_ids(connection, 'range-1', range(1, 50_001))
        connection.commit()
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        tracing_was_active = tracemalloc.is_tracing()
        if not tracing_was_active:
            tracemalloc.start()
        starting_bytes, _peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.reset_peak()
        try:
            projection = state.unresolved_late_id_projection(limit=100)
            _current_bytes, peak_bytes = tracemalloc.get_traced_memory()
        finally:
            if not tracing_was_active:
                tracemalloc.stop()
            connection.set_trace_callback(None)

        selects = tuple(
            ' '.join(statement.split())
            for statement in statements
            if statement.lstrip().upper().startswith('SELECT')
        )
        assert projection == (50_000, tuple(range(1, 101)))
        assert len(selects) == 2
        assert selects[0].startswith('SELECT COUNT(*) FROM (')
        assert selects[1].endswith('LIMIT 100')
        assert peak_bytes - starting_bytes < 512 * 1024
        with pytest.raises(ValueError, match='late-id projection limit must be nonnegative'):
            state.unresolved_late_id_projection(limit=-1)
