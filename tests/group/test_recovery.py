from __future__ import annotations

import os
import select
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypedDict, Unpack

import pytest

import sg_event_daemon._group_recovery as recovery
import sg_event_daemon._group_types as group_types
from sg_event_daemon._fanout import StrictOrderingControl
from sg_event_daemon._group_errors import InvalidTransitionError, RevisionConflictError
from sg_event_daemon._group_recovery import LifecycleRequest
from sg_event_daemon._group_state import PendingRange
from sg_event_daemon._group_types import (
    AuditIdentity,
    LiveJoinIdentity,
    canonical_bytes,
    digest,
    snapshot_from_json,
)
from sg_event_daemon._plugins import CallbackIdentity, CallbackManifest
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._sqlite import DatabaseError
from sg_event_daemon._strict import JOIN_ABORT_LIMIT
from tests._storage_support import (
    close_test_range,
    insert_replay_marker,
    open_group_on_source,
    open_test_group,
    write_context_for,
)
from tests.group._support import assert_operational_count as _assert_operational_count
from tests.group._support import dispose_at_frontier, operator_records, seed_pending_range

if TYPE_CHECKING:
    from sg_event_daemon._group_state import GroupState


class _RecoveryArguments(TypedDict):
    reason: str
    identity: AuditIdentity
    wall_clock_time: float
    operator_label: str


class _GroupOpenOptions(TypedDict, total=False):
    group_generation: int
    max_late_ids_per_group: int
    max_pending_epochs: int
    ordering: Literal['eager', 'strict']
    source_generation: str
    stream_fingerprint: str
    strict_holdback_secs: float
    verified_incarnation: str


def _attempt_id(label: str) -> str:
    return f'attempt-{label}'


def _conditional_park_directive(
    evidence_id: str,
) -> group_types.ConditionalParkDirective:
    return group_types.ConditionalParkDirective(
        evidence_id=evidence_id,
        site='studio',
        group='editorial',
        group_generation=7,
        failed_spawn_instance='worker-spawn-1',
        event_id=42,
        group_revision=2,
        observed_at_wall_clock=1000.5,
    )


def _conditional_park_rows(path: Path) -> tuple[tuple[object, ...], ...]:
    with closing(sqlite3.connect(path)) as inspection, inspection:
        rows = inspection.execute(
            """
            SELECT evidence_id, group_generation, evidence_fingerprint,
                   outcome, resulting_revision, decided_at_wall_clock
            FROM conditional_park_results
            WHERE group_name = 'editorial'
            ORDER BY evidence_id
            """
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _join_commit_rows(path: Path) -> tuple[tuple[object, ...], ...]:
    with closing(sqlite3.connect(path)) as inspection, inspection:
        rows = inspection.execute(
            """
            SELECT attempt_token, identity_json, result_revision, snapshot_json
            FROM join_commits
            WHERE group_name = 'editorial'
            ORDER BY attempt_token
            """
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _join_identity(
    ordering: Literal['eager', 'strict'],
    token: str,
    *,
    group_generation: int = 7,
) -> LiveJoinIdentity:
    return LiveJoinIdentity(
        site='studio',
        source='events',
        group='editorial',
        join_kind=ordering,
        source_generation='source-spawn-1',
        group_generation=group_generation,
        boundary=41,
        attempt_token=token,
    )


def _live_join_replay_evidence(path: Path) -> tuple[object, ...]:
    with closing(sqlite3.connect(path)) as inspection, inspection:
        row = inspection.execute(
            """
            SELECT revision, is_operator_replay, replay_required_from,
                   history_truncated_floor
            FROM group_state
            WHERE group_name = 'editorial'
            """
        ).fetchone()
    assert row is not None
    return tuple(row)


def _assert_live_join_preflight_rejected_without_mutation(
    state: GroupState,
    path: Path,
    identity: LiveJoinIdentity,
    *,
    match: str,
) -> None:
    before = state.snapshot()
    before_revision = before.revision
    before_join_rows = _join_commit_rows(path)
    before_replay_evidence = _live_join_replay_evidence(path)

    with pytest.raises(InvalidTransitionError, match=match):
        state.validate_live_join(identity, expected_revision=before_revision)

    after = state.snapshot()
    assert after == before
    assert after.revision == before_revision
    assert _join_commit_rows(path) == before_join_rows
    assert _live_join_replay_evidence(path) == before_replay_evidence


def _assert_live_join_preflight_and_commit_rejected_without_mutation(
    state: GroupState,
    path: Path,
    identity: LiveJoinIdentity,
    *,
    match: str,
) -> None:
    before = state.snapshot()
    _assert_live_join_preflight_rejected_without_mutation(
        state,
        path,
        identity,
        match=match,
    )
    with pytest.raises(InvalidTransitionError):
        state.commit_live_join(identity, expected_revision=before.revision)
    assert state.snapshot() == before


def _manifest() -> CallbackManifest:
    return CallbackManifest(
        callbacks=(
            CallbackIdentity(
                ordinal=0,
                plugin_id='publish',
                callback_id='publish',
                execution='sync',
                filter_digest='filter-0',
                config_digest='config-0',
                secret_refs=(),
                credential_name='publisher',
                credential_fingerprint='publisher:publish-script',
            ),
        )
    )


def _manifest_pair() -> CallbackManifest:
    return CallbackManifest(
        callbacks=(
            *_manifest().callbacks,
            CallbackIdentity(
                ordinal=1,
                plugin_id='publish',
                callback_id='archive',
                execution='sync',
                filter_digest='filter-1',
                config_digest='config-1',
                secret_refs=(),
                credential_name='publisher',
                credential_fingerprint='publisher:publish-script',
            ),
        )
    )


def _open_disposition_group(
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
        ordering=ordering,
        max_group_obligations=max_group_obligations,
        max_late_ids_per_group=max_late_ids_per_group,
    )


def _open_group(
    path: Path,
    **options: Unpack[_GroupOpenOptions],
) -> GroupState:
    return open_test_group(
        path,
        group_generation=options.get('group_generation', 7),
        ordering=options.get('ordering', 'eager'),
        stream_fingerprint=options.get('stream_fingerprint', 'stream-v1'),
        source_generation=options.get('source_generation', 'source-spawn-1'),
        verified_incarnation=options.get('verified_incarnation', 'incarnation-1'),
        max_late_ids_per_group=options.get('max_late_ids_per_group', 50_000),
        max_pending_epochs=options.get('max_pending_epochs', 8),
        strict_holdback_secs=options.get('strict_holdback_secs', 300.0),
    )


def _park_manifest(*callback_ids: str) -> CallbackManifest:
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


def _recovery_arguments() -> _RecoveryArguments:
    return {
        'reason': 'operator recovery',
        'identity': AuditIdentity.current(),
        'wall_clock_time': 1000.0,
        'operator_label': 'ticket-7',
    }


def _seed_replay_marker(state: GroupState, from_event_id: int) -> None:
    connection = state._database.connection
    insert_replay_marker(connection, 'editorial', state.snapshot().group_generation, from_event_id)
    connection.execute(
        'UPDATE group_state SET replay_required_from = ?, revision = revision + 1 '
        "WHERE group_name = 'editorial'",
        (from_event_id,),
    )


def _strict_obligation(
    *,
    obligation_id: str = 'range-1',
    low_id: int = 42,
    high_id: int = 44,
) -> PendingRange:
    return PendingRange(
        obligation_id=obligation_id,
        low_id=low_id,
        high_id=high_id,
        closed=False,
        close_generation=0,
        eligibility_point=130.0,
        late_ids=(),
    )


def _seed_obligation(state: GroupState, obligation: PendingRange | None = None) -> None:
    selected = _strict_obligation() if obligation is None else obligation
    seed_pending_range(
        state._database.connection,
        selected.obligation_id,
        selected.low_id,
        selected.high_id,
    )


def _truncated_retirement_request(state: GroupState) -> recovery.DispositionRequest:
    state._database.connection.execute('UPDATE source_meta SET frontier = 900')
    state.start_event(100, _manifest(), expected_revision=state.snapshot().revision)
    state.begin_callback(0, expected_revision=state.snapshot().revision)
    state.park(expected_revision=state.snapshot().revision, **_recovery_arguments())
    before = state.mark_history_truncated(500, expected_revision=state.snapshot().revision)
    assert before.replay_required_from is None
    return recovery.DispositionRequest(
        command='replay',
        group='editorial',
        disposition_id='retire-truncated',
        reserved_generation=8,
        expected_revision=before.revision,
        from_event_id=500,
        acknowledge_truncation=True,
        **_recovery_arguments(),
    )


def test_callback_recovery_commands_are_exact_audited_and_idempotent(
    tmp_path: Path,
) -> None:
    arguments = _recovery_arguments()
    changed_manifest = _manifest_pair()
    with _open_group(tmp_path / 'editorial.db') as state:
        state.start_event(42, changed_manifest, expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        state.callback_failed(0, 'failed', expected_revision=2)
        retried = state.retry(
            event_id=42,
            callback_ordinal=0,
            expected_revision=3,
            **arguments,
        )
        assert retried.failure_code is None
        assert (
            state.retry(
                event_id=42,
                callback_ordinal=0,
                expected_revision=4,
                **arguments,
            )
            == retried
        )
        attempted = state.begin_callback(0, expected_revision=4)
        assert attempted.callback_marks[0][-1] == 2
        skipped = state.skip_callback(
            event_id=42,
            callback_ordinal=0,
            expected_revision=5,
            **arguments,
        )
        assert skipped.callback_marks[0][3] == 'skipped'
        state.begin_callback(1, expected_revision=6)
        state.callback_failed(1, 'failed later', expected_revision=7)
        restarted = state.restart_event(
            _manifest(),
            expected_revision=8,
            **arguments,
        )
        assert restarted.callback_marks == ((0, 'publish', 'publish', 'pending', 0),)
        assert (
            state.restart_event(
                _manifest(),
                expected_revision=9,
                **arguments,
            )
            == restarted
        )
        state.park(expected_revision=9, **arguments)
        unparked = state.unpark(expected_revision=10, **arguments)
        assert state.unpark(expected_revision=11, **arguments) == unparked

        records = operator_records(state)
        assert tuple(record.command for record in records) == (
            'retry',
            'skip-callback',
            'restart-event',
            'park',
            'unpark',
        )
        retry_record = records[0]
        assert (
            retry_record.site,
            retry_record.group,
            retry_record.event_id,
            retry_record.plugin_id,
            retry_record.callback_id,
            retry_record.prior_revision,
            retry_record.new_revision,
            retry_record.reason,
            retry_record.operator_label,
            retry_record.local_uid,
            retry_record.username,
            retry_record.pid,
        ) == (
            'studio',
            'editorial',
            42,
            'publish',
            'publish',
            3,
            4,
            'operator recovery',
            'ticket-7',
            arguments['identity'].local_uid,
            arguments['identity'].username,
            arguments['identity'].pid,
        )


def test_changed_stream_and_ordering_are_accepted_only_by_new_generation_disposition(
    tmp_path: Path,
) -> None:
    arguments = _recovery_arguments()
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        state.park(expected_revision=0, **arguments)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "UPDATE source_meta SET source_generation = 'source-spawn-2', "
            "stream_fingerprint = 'stream-v2', stream_incarnation = 'incarnation-2'"
        )
    with _open_group(
        path,
        ordering='strict',
        source_generation='source-spawn-2',
        stream_fingerprint='stream-v2',
        verified_incarnation='incarnation-2',
    ) as changed:
        before = changed.snapshot()
        assert {'stream-identity-changed', 'ordering-changed', 'parked'} <= set(before.conditions)
        reset = dispose_at_frontier(
            changed,
            'reset-to-latest',
            disposition_id='reset-changed',
            new_generation=8,
            source_frontier=90,
            expected_revision=before.revision,
            **arguments,
        )
        assert reset.ordering == 'strict'
        assert reset.stream_fingerprint == 'stream-v2'
        assert reset.verified_incarnation == 'incarnation-2'
    with _open_group(
        path,
        group_generation=8,
        ordering='strict',
        source_generation='source-spawn-2',
        stream_fingerprint='stream-v2',
        verified_incarnation='incarnation-2',
    ) as reopened:
        assert reopened.snapshot().conditions == ('parked',)


def test_conditional_park_exact_match_parks_and_increments_once(tmp_path: Path) -> None:
    with _open_disposition_group(tmp_path / 'editorial.db') as state:
        state.start_event(42, _park_manifest('publish'), expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        directive = group_types.ConditionalParkDirective(
            evidence_id='evidence-exact',
            site='studio',
            group='editorial',
            group_generation=7,
            failed_spawn_instance='worker-spawn-1',
            event_id=42,
            group_revision=2,
            observed_at_wall_clock=1000.5,
        )

        result = state.resolve_conditional_park(
            directive,
            identity=AuditIdentity.current(),
            decided_at_wall_clock=1001.0,
        )

        assert result == group_types.ConditionalParkResult(
            evidence_id='evidence-exact',
            outcome='parked',
            resulting_revision=3,
        )
        snapshot = state.snapshot()
        assert snapshot.revision == 3
        assert snapshot.parked
        assert snapshot.in_flight_event_id == 42
        assert tuple(record.command for record in operator_records(state)) == ('conditional-park',)


def test_conditional_park_reason_replaces_prior_operator_reason(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    arguments = _recovery_arguments()
    with _open_group(path) as state:
        assert state.park_reason() is None
        state.park(expected_revision=0, **arguments)
        state.unpark(expected_revision=1, **arguments)
        assert state.park_reason() == arguments['reason']
        state.start_event(42, _park_manifest('publish'), expected_revision=2)
        state.begin_callback(0, expected_revision=3)
        directive = replace(_conditional_park_directive('later-park'), group_revision=4)
        state.resolve_conditional_park(
            directive,
            identity=AuditIdentity.current(),
            decided_at_wall_clock=1001.0,
        )
        assert state.park_reason() == 'supervisor-overdue-lane'

    with _open_group(path) as reopened:
        assert reopened.park_reason() == 'supervisor-overdue-lane'
        reopened.unpark(expected_revision=5, **arguments)
        assert reopened.park_reason() == 'supervisor-overdue-lane'
        reopened.park(expected_revision=6, **arguments)
        assert reopened.park_reason() == arguments['reason']


def test_conditional_park_rejects_id_reuse_revision_regression_and_ambiguous_state(
    tmp_path: Path,
) -> None:
    identity = AuditIdentity.current()
    base = _conditional_park_directive('evidence-rejected')
    conflicts = (
        ('wrong-site', replace(base, site='review'), 'identity'),
        ('wrong-group', replace(base, group='lighting'), 'identity'),
        ('wrong-generation', replace(base, group_generation=8), 'generation'),
        ('revision-regression', replace(base, group_revision=3), 'regression'),
        ('conflicting-event', replace(base, event_id=43), 'ambiguous'),
    )
    for label, directive, message in conflicts:
        path = tmp_path / f'{label}.db'
        with _open_group(path) as state:
            state.start_event(42, _manifest(), expected_revision=0)
            state.begin_callback(0, expected_revision=1)
            before = state.snapshot()

            with pytest.raises(InvalidTransitionError, match=message):
                state.resolve_conditional_park(
                    directive,
                    identity=identity,
                    decided_at_wall_clock=1001.0,
                )

            assert state.snapshot() == before
        assert _conditional_park_rows(path) == ()

    parked_path = tmp_path / 'already-parked.db'
    with _open_group(parked_path) as state:
        state.start_event(42, _manifest(), expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        state.park(
            reason='operator maintenance',
            identity=identity,
            wall_clock_time=1000.0,
            expected_revision=2,
        )
        before = state.snapshot()

        with pytest.raises(InvalidTransitionError, match='ambiguous'):
            state.resolve_conditional_park(
                replace(base, group_revision=3),
                identity=identity,
                decided_at_wall_clock=1001.0,
            )

        assert state.snapshot() == before
    assert _conditional_park_rows(parked_path) == ()

    reuse_path = tmp_path / 'id-reuse.db'
    with _open_group(reuse_path) as state:
        state.start_event(42, _manifest(), expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        recorded = state.resolve_conditional_park(
            base,
            identity=identity,
            decided_at_wall_clock=1001.0,
        )
        before = state.snapshot()

        with pytest.raises(InvalidTransitionError, match='reuse'):
            state.resolve_conditional_park(
                replace(base, observed_at_wall_clock=1000.75),
                identity=identity,
                decided_at_wall_clock=1002.0,
            )

        assert recorded.outcome == 'parked'
        assert state.snapshot() == before
    assert len(_conditional_park_rows(reuse_path)) == 1


def test_conditional_park_result_is_idempotent_by_fingerprint(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    directive = _conditional_park_directive('evidence-idempotent')
    with _open_group(path) as state:
        state.start_event(42, _manifest(), expected_revision=0)
        state.begin_callback(0, expected_revision=1)

        first = state.resolve_conditional_park(
            directive,
            identity=AuditIdentity.current(),
            decided_at_wall_clock=1001.0,
        )
        repeated = state.resolve_conditional_park(
            directive,
            identity=AuditIdentity.current(),
            decided_at_wall_clock=2002.0,
        )

        assert (
            first
            == repeated
            == group_types.ConditionalParkResult(
                evidence_id='evidence-idempotent',
                outcome='parked',
                resulting_revision=3,
            )
        )
        assert state.snapshot().revision == 3
        assert tuple(record.command for record in operator_records(state)) == ('conditional-park',)

    assert _conditional_park_rows(path) == (
        (
            'evidence-idempotent',
            7,
            directive.fingerprint(),
            'parked',
            3,
            1001.0,
        ),
    )


def test_conditional_park_stale_result_does_not_change_revision(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    directive = _conditional_park_directive('evidence-stale')
    with _open_group(path) as state:
        state.start_event(42, _manifest(), expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        state.callback_done(0, expected_revision=2)
        before = state.snapshot()

        result = state.resolve_conditional_park(
            directive,
            identity=AuditIdentity.current(),
            decided_at_wall_clock=1001.0,
        )

        assert result == group_types.ConditionalParkResult(
            evidence_id='evidence-stale',
            outcome='stale',
            resulting_revision=3,
        )
        assert state.snapshot() == before
        assert operator_records(state) == ()

    assert _conditional_park_rows(path) == (
        (
            'evidence-stale',
            7,
            directive.fingerprint(),
            'stale',
            3,
            1001.0,
        ),
    )


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_delivery_floor_skip_audits_ordering_neutral_command(
    tmp_path: Path,
    ordering: Literal['eager', 'strict'],
) -> None:
    path = tmp_path / f'{ordering}.db'
    with _open_group(path, ordering=ordering) as state:
        audited = state.record_delivery_floor_skip(40, expected_revision=0)

        assert audited.revision == 1
        record = operator_records(state)[-1]
        assert (
            record.command,
            record.event_id,
            record.prior_revision,
            record.new_revision,
            record.reason,
            record.command_fingerprint,
        ) == (
            'delivery-floor-skip',
            40,
            0,
            1,
            'source delivery-floor decision',
            digest(
                canonical_bytes(
                    {
                        'command': 'delivery-floor-skip',
                        'delivery_floor': 41,
                        'event_id': 40,
                        'group_generation': 7,
                    }
                )
            ),
        )

    with closing(sqlite3.connect(path)) as inspection:
        result_snapshot_json = inspection.execute(
            """
            SELECT result_snapshot_json
            FROM audit
            WHERE group_name = 'editorial' AND command = 'delivery-floor-skip'
            """
        ).fetchone()
    assert result_snapshot_json is not None
    assert snapshot_from_json(str(result_snapshot_json[0])) == audited
    with _open_group(path, ordering=ordering) as reopened:
        assert reopened.snapshot() == audited
        assert operator_records(reopened)[-1].command == 'delivery-floor-skip'


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_delivery_floor_skip_is_idempotent_with_stale_revision(
    tmp_path: Path,
    ordering: Literal['eager', 'strict'],
) -> None:
    with _open_group(tmp_path / f'{ordering}.db', ordering=ordering) as state:
        audited = state.record_delivery_floor_skip(40, expected_revision=0)

        assert state.record_delivery_floor_skip(40, expected_revision=0) == audited
        assert state.snapshot() == audited
        assert tuple(record.command for record in operator_records(state)) == (
            'delivery-floor-skip',
        )


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_delivery_floor_skip_rejects_above_floor_without_mutation(
    tmp_path: Path,
    ordering: Literal['eager', 'strict'],
) -> None:
    with _open_group(tmp_path / f'{ordering}.db', ordering=ordering) as state:
        before = state.snapshot()

        with pytest.raises(
            InvalidTransitionError,
            match='delivery-floor skip event identity conflicts',
        ):
            state.record_delivery_floor_skip(42, expected_revision=0)

        assert state.snapshot() == before
        assert operator_records(state) == ()


@pytest.mark.parametrize(
    ('command', 'arguments'),
    [
        ('replay', {'from_event_id': value, 'reason': 'recover'})
        for value in ('1', True, 1.5, -1, None)
    ]
    + [
        ('replay', {'reason': 'recover'}),
        ('reset-to-latest', {'from_event_id': 5, 'reason': 'recover'}),
        *[
            ('replay', {'from_event_id': 1, 'reason': 'recover', 'acknowledge_truncation': value})
            for value in (1, 'true', None)
        ],
        ('replay', {'from_event_id': 1, 'reason': ''}),
        ('park', {'reason': 'recover'}),
    ],
)
def test_disposition_command_rejects_invalid_arguments(
    command: str, arguments: dict[str, object]
) -> None:
    with pytest.raises(ValueError, match=r'replay|reset-to-latest|group disposition'):
        recovery.disposition_command(
            command, arguments, disposition_id='recovery', wall_clock_time=1.0, operator_label=None
        )


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest', 'reset-group-to-latest'])
def test_disposition_command_request_carries_validated_values(command: str) -> None:
    position = 0 if command == 'replay' else None
    validated = recovery.disposition_command(
        command,
        {'from_event_id': position, 'acknowledge_truncation': True, 'reason': 'recover'},
        disposition_id='recovery',
        wall_clock_time=1.5,
        operator_label='operator',
    )
    assert validated.request('group', 2, 3) == recovery.DispositionRequest(
        command='replay' if command == 'replay' else 'reset-to-latest',
        group='group',
        disposition_id='recovery',
        reserved_generation=2,
        expected_revision=3,
        from_event_id=position,
        acknowledge_truncation=True,
        reason='recover',
        identity=AuditIdentity.current(),
        wall_clock_time=1.5,
        operator_label='operator',
    )


def test_disposition_reused_after_intervening_reset_keeps_shared_store_recoverable(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / 'source.db'
    group_path = source_path
    with (
        SourceState.open(
            source_path,
            writes=write_context_for(source_path),
            create=True,
            site='main',
            source='events',
            stream_fingerprint='stream',
            source_generation='source-1',
            visibility_overlap_secs=1.0,
        ) as source,
        open_group_on_source(
            group_path,
            writes=write_context_for(group_path),
            site='main',
            site_server_url='https://example.test',
            source='events',
            group='local',
            group_generation=7,
            ordering='eager',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='initialize',
            initial_cursor=41,
            initial_delivery_floor=41,
            initial_last_delivered_id=41,
        ) as group,
    ):
        source.initialize(41, incarnation='incarnation-1')
        group.park(
            reason='operator maintenance',
            identity=AuditIdentity.current(),
            wall_clock_time=1.0,
            expected_revision=0,
        )

        def operation(
            command: Literal['replay', 'reset-to-latest'],
            disposition_id: str,
            old_generation: int,
        ) -> recovery.DispositionRequest:
            return recovery.DispositionRequest(
                command=command,
                group='local',
                disposition_id=disposition_id,
                reserved_generation=old_generation + 1,
                expected_revision=group.snapshot().revision,
                from_event_id=41 if command == 'replay' else None,
                acknowledge_truncation=False,
                reason='same operator reason',
                identity=AuditIdentity.current(),
                wall_clock_time=1.0,
            )

        group.dispose(operation('replay', 'replay-id', 7))
        group.dispose(operation('reset-to-latest', 'reset-id', 8))
        before = group.snapshot()
        with pytest.raises(InvalidTransitionError, match='disposition identity'):
            group.dispose(operation('replay', 'replay-id', 9))
        assert group.snapshot() == before
        terminal = group.terminal_disposition_identity(9)
        assert terminal is not None
        assert source._database.connection.execute(
            "SELECT group_generation FROM groups WHERE group_name = 'local'"
        ).fetchone() == (9,)

    with (
        SourceState.open(
            source_path,
            writes=write_context_for(source_path),
            create=True,
            site='main',
            source='events',
            stream_fingerprint='stream',
            source_generation='source-1',
            visibility_overlap_secs=1.0,
        ) as reopened_source,
        open_group_on_source(
            group_path,
            writes=write_context_for(group_path),
            site='main',
            site_server_url='https://example.test',
            source='events',
            group='local',
            group_generation=9,
            ordering='eager',
            stream_fingerprint='stream',
            source_generation='source-1',
            verified_incarnation='incarnation-1',
            start_mode='resume',
        ) as reopened_group,
    ):
        assert reopened_group.snapshot().group_generation == 9
        assert reopened_source._database.connection.execute(
            "SELECT group_generation FROM groups WHERE group_name = 'local'"
        ).fetchone() == (9,)


def test_eager_live_join_allows_open_pending_range(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        state.advance_irrelevant(45, expected_revision=state.snapshot().revision)
        _seed_obligation(state)
        accepted = state.snapshot()
        assert accepted.obligation_count == 1
        joined = state.commit_live_join(
            LiveJoinIdentity(
                site='studio',
                source='events',
                group='editorial',
                join_kind='eager',
                source_generation='source-spawn-1',
                group_generation=7,
                boundary=45,
                attempt_token=_attempt_id('eager-open-obligation'),
            ),
            expected_revision=state.snapshot().revision,
        )
        assert joined.latest_join_boundary == 45
        assert joined.obligation_count == 1


def test_epoch_capture_spacing_and_idle_clock_checkpoints_use_bounded_interval(
    tmp_path: Path,
) -> None:
    with _open_group(
        tmp_path / 'editorial.db',
        ordering='strict',
        max_pending_epochs=3,
        strict_holdback_secs=30.0,
    ) as state:
        with pytest.raises(InvalidTransitionError):
            state.capture_epoch(
                low_id=42,
                high_id=45,
                captured_at=100.0,
                eligibility_point=129.0,
                reader_clock_checkpoint=100.0,
                source_generation='source-spawn-1',
                expected_revision=0,
            )
        first = state.capture_epoch(
            low_id=42,
            high_id=45,
            captured_at=100.0,
            eligibility_point=130.0,
            reader_clock_checkpoint=100.0,
            source_generation='source-spawn-1',
            expected_revision=0,
        )
        with pytest.raises(InvalidTransitionError):
            state.capture_epoch(
                low_id=46,
                high_id=50,
                captured_at=114.0,
                eligibility_point=144.0,
                reader_clock_checkpoint=114.0,
                source_generation='source-spawn-1',
                expected_revision=1,
            )
        second = state.capture_epoch(
            low_id=46,
            high_id=50,
            captured_at=115.0,
            eligibility_point=145.0,
            reader_clock_checkpoint=115.0,
            source_generation='source-spawn-1',
            expected_revision=1,
        )
        with pytest.raises(InvalidTransitionError):
            state.checkpoint_reader_clock(
                129.0,
                source_generation='source-spawn-1',
                expected_revision=2,
            )
        checkpointed = state.checkpoint_reader_clock(
            130.0,
            source_generation='source-spawn-1',
            expected_revision=2,
        )
        assert checkpointed.reader_clock_checkpoint == 130.0
        assert checkpointed.epochs == second.epochs
        assert first.epochs[0].captured_at == checkpointed.epochs[0].captured_at


def test_epoch_commit_and_eligibility_survive_real_process_death(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    environment = dict(os.environ)
    environment['PYTHONPATH'] = os.fspath(Path(__file__).parents[2] / 'src')
    code = """
import sys
from pathlib import Path
from tests._storage_support import open_group_on_source

path = Path(sys.argv[1])
phase = sys.argv[2]
start_options = (
    {'start_mode': 'resume'}
    if path.exists()
    else {
        'start_mode': 'initialize',
        'initial_cursor': 41,
        'initial_delivery_floor': 41,
        'initial_last_delivered_id': 41,
    }
)
with open_group_on_source(
    path,
    site='studio',
    site_server_url='https://studio.example.invalid',
    source='events',
    group='editorial',
    group_generation=7,
    ordering='strict',
    stream_fingerprint='stream-v1',
    source_generation='source-spawn-1',
    verified_incarnation='incarnation-1',
    max_pending_epochs=2,
    strict_holdback_secs=30.0,
    **start_options,
) as state:
    if phase == 'capture':
        state.capture_epoch(
            low_id=42,
            high_id=45,
            captured_at=100.0,
            eligibility_point=130.0,
            reader_clock_checkpoint=100.0,
            source_generation='source-spawn-1',
            expected_revision=0,
        )
    else:
        epoch = state.snapshot().epochs[0]
        state.mark_epoch_eligible(
            epoch.epoch_id,
            reader_clock_checkpoint=130.0,
            expected_revision=1,
        )
    print('READY', flush=True)
    sys.stdin.read()
"""
    for phase in ('capture', 'eligible'):
        process = subprocess.Popen(
            [sys.executable, '-c', code, str(path), phase],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            env=environment,
        )
        assert process.stdout is not None
        return_code: int | None = None
        try:
            deadline = time.monotonic() + 2.0
            readable, _writable, _exceptional = select.select(
                (process.stdout,),
                (),
                (),
                max(0.0, deadline - time.monotonic()),
            )
            assert readable, 'child readiness timed out'
            assert os.read(process.stdout.fileno(), 4_096).decode().strip() == 'READY'
            process.kill()
            return_code = process.wait(timeout=5)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            if process.stdin is not None:
                process.stdin.close()
            process.stdout.close()
        assert return_code is not None
        assert return_code != 0

        with _open_group(
            path,
            ordering='strict',
            max_pending_epochs=2,
            strict_holdback_secs=30.0,
        ) as reopened:
            epoch = reopened.snapshot().epochs[0]
            assert epoch.captured_at == 100.0
            assert reopened.snapshot().reader_clock_checkpoint == (
                100.0 if phase == 'capture' else 130.0
            )
            assert epoch.state == ('waiting' if phase == 'capture' else 'delivering')


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_exact_retry_returns_receipt_without_second_retirement_audit(
    tmp_path: Path, command: Literal['replay', 'reset-to-latest']
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        request = _truncated_retirement_request(state)
        request = replace(
            request, command=command, from_event_id=500 if command == 'replay' else None
        )
        result = state.dispose(request)
    with _open_group(path, group_generation=8) as state:
        before = tuple(state._database.connection.iterdump())
        assert state.dispose(request) == result
        assert tuple(state._database.connection.iterdump()) == before


def test_final_callback_skip_waits_for_new_lower_obligation(tmp_path: Path) -> None:
    higher = _strict_obligation(
        obligation_id='range-higher',
        low_id=46,
        high_id=48,
    )
    lower = _strict_obligation(obligation_id='range-lower')
    with _open_group(tmp_path / 'editorial.db', ordering='strict') as state:
        _seed_obligation(state, higher)
        state.reserve_late_event(
            higher.obligation_id, 46, expected_revision=state.snapshot().revision
        )
        state.start_event(
            46,
            _manifest(),
            expected_revision=state.snapshot().revision,
            obligation_id=higher.obligation_id,
        )
        state.begin_callback(0, expected_revision=state.snapshot().revision)
        _seed_obligation(state, lower)
        blocked = state.snapshot()

        with pytest.raises(InvalidTransitionError):
            state.skip_callback(
                event_id=46,
                callback_ordinal=0,
                expected_revision=blocked.revision,
                **_recovery_arguments(),
            )

        assert state.snapshot() == blocked
        assert operator_records(state) == ()


def test_final_late_callback_skip_maintains_operational_late_id_count(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        _seed_obligation(state)
        state.reserve_late_event('range-1', 42, expected_revision=state.snapshot().revision)
        connection = state._database.connection
        _assert_operational_count(connection)
        state.start_event(
            42,
            _manifest(),
            expected_revision=state.snapshot().revision,
            obligation_id='range-1',
        )
        state.begin_callback(0, expected_revision=state.snapshot().revision)

        connection.execute(
            "INSERT INTO late_ids VALUES ('editorial', 'range-1', 43, 'capacity-pending', 1)"
        )
        skipped = state.skip_callback(
            event_id=42,
            callback_ordinal=0,
            expected_revision=state.snapshot().revision,
            **_recovery_arguments(),
        )

        assert connection.execute(
            "SELECT terminal_state FROM late_ids WHERE group_name = 'editorial' AND event_id = 43"
        ).fetchone() == ('capacity-pending',)
        assert skipped.late_id_count == 0
        _assert_operational_count(connection)


@pytest.mark.parametrize('late', [False, True])
def test_final_success_and_skip_share_terminal_state_but_keep_distinct_audit(
    tmp_path: Path,
    late: bool,
) -> None:
    snapshots: list[group_types.GroupSnapshot] = []
    for outcome in ('done', 'skipped'):
        path = tmp_path / f'{outcome}.db'
        with _open_group(path) as state:
            if late:
                _seed_obligation(state)
                state.reserve_late_event(
                    'range-1', 42, expected_revision=state.snapshot().revision
                )
            started = state.start_event(
                42,
                _manifest(),
                expected_revision=state.snapshot().revision,
                obligation_id='range-1' if late else None,
            )
            attempted = state.begin_callback(0, expected_revision=started.revision)
            if outcome == 'done':
                completed = state.callback_done(0, expected_revision=attempted.revision)
                assert operator_records(state) == ()
            else:
                failed = state.callback_failed(
                    0, 'callback failure', expected_revision=attempted.revision
                )
                completed = state.skip_callback(
                    event_id=42,
                    callback_ordinal=0,
                    expected_revision=failed.revision,
                    **_recovery_arguments(),
                )
                records = operator_records(state)
                assert len(records) == 1
                assert records[0].command == 'skip-callback'
                assert records[0].prior_revision == failed.revision
                assert records[0].new_revision == completed.revision
                assert (
                    state.skip_callback(
                        event_id=42,
                        callback_ordinal=0,
                        expected_revision=failed.revision,
                        **_recovery_arguments(),
                    )
                    == completed
                )
            assert completed.revision == state.snapshot().revision
            assert completed.revision == attempted.revision + (1 if outcome == 'done' else 2)
            assert completed.cursor == 42
            assert completed.in_flight_event_id is None
            assert completed.in_flight_obligation_id is None
            assert completed.manifest is None
            assert completed.callback_marks == ()
            assert completed.failure_code is None
            assert completed.failure_text is None
            assert completed.late_id_count == 0
            connection = state._database.connection
            assert connection.execute(
                "SELECT last_delivered_id FROM group_state WHERE group_name = 'editorial' "
            ).fetchone() == (42,)
            _assert_operational_count(connection)
            assert connection.execute(
                'SELECT obligation_id, event_id, terminal_state FROM late_ids'
                " WHERE group_name = 'editorial' "
            ).fetchall() == ([('range-1', 42, 'done')] if late else [])
            snapshots.append(replace(completed, revision=0))
        with _open_group(path) as reopened:
            assert reopened.snapshot() == completed
    assert snapshots[0] == snapshots[1]


def test_history_truncation_override_retires_only_unavailable_replay_prefix(
    tmp_path: Path,
) -> None:
    arguments = _recovery_arguments()
    with _open_group(tmp_path / 'editorial.db') as state:
        _seed_replay_marker(state, 40)
        state.mark_history_truncated(45, expected_revision=state.snapshot().revision)
        state.park(expected_revision=state.snapshot().revision, **arguments)
        before = state.snapshot()
        with pytest.raises(InvalidTransitionError):
            dispose_at_frontier(
                state,
                'replay',
                disposition_id='truncated-replay',
                new_generation=8,
                from_event_id=45,
                source_frontier=50,
                expected_revision=state.snapshot().revision,
                **arguments,
            )
        assert state.snapshot() == before
        replaying = dispose_at_frontier(
            state,
            'replay',
            disposition_id='truncated-replay',
            new_generation=8,
            from_event_id=45,
            source_frontier=50,
            acknowledge_truncation=True,
            expected_revision=state.snapshot().revision,
            **arguments,
        )
        assert replaying.cursor == 44
        assert replaying.replay_required_from is None
        assert replaying.history_truncated_floor is None


def test_live_join_operational_history_retains_only_current_token(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    first_identity: LiveJoinIdentity | None = None
    first_expected_revision: int | None = None
    current_identity: LiveJoinIdentity | None = None
    current_snapshot = None
    with _open_group(path, ordering='strict') as state:
        for attempt_number in range(32):
            identity = _join_identity(
                'strict',
                _attempt_id(f'bounded-{attempt_number}'),
            )
            expected_revision = state.snapshot().revision
            joined = state.commit_live_join(
                identity,
                expected_revision=expected_revision,
            )
            if first_identity is None:
                first_identity = identity
                first_expected_revision = expected_revision
            current_identity = identity
            current_snapshot = joined
            assert len(_join_commit_rows(path)) == 1

        assert first_identity is not None
        assert first_expected_revision is not None
        assert current_identity is not None
        assert current_snapshot is not None
        assert (
            state.commit_live_join(
                current_identity,
                expected_revision=current_snapshot.revision,
            )
            == current_snapshot
        )
        with pytest.raises(RevisionConflictError, match='revision'):
            state.commit_live_join(
                first_identity,
                expected_revision=first_expected_revision,
            )
        assert _join_commit_rows(path)[0][0] == current_identity.attempt_token


def test_live_join_preflight_is_exact_read_only_and_preserves_replay(
    tmp_path: Path,
) -> None:
    arguments = _recovery_arguments()
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        state.park(expected_revision=0, **arguments)
        dispose_at_frontier(
            state,
            'replay',
            disposition_id='disposition-preflight',
            new_generation=8,
            from_event_id=40,
            source_frontier=50,
            expected_revision=1,
            **arguments,
        )
        state.unpark(expected_revision=state.snapshot().revision, **arguments)
        state.advance_irrelevant(50, expected_revision=state.snapshot().revision)
        identity = LiveJoinIdentity(
            site='studio',
            source='events',
            group='editorial',
            join_kind='eager',
            source_generation='source-spawn-1',
            group_generation=8,
            boundary=50,
            attempt_token=_attempt_id('preflight'),
        )
        before = state.snapshot()
        before_join_rows = _join_commit_rows(path)
        before_replay_evidence = _live_join_replay_evidence(path)

        accepted = state.validate_live_join(
            identity,
            expected_revision=before.revision,
        )
        assert accepted == before
        assert accepted.revision == before.revision
        assert accepted.is_operator_replay
        assert _join_commit_rows(path) == before_join_rows
        assert _live_join_replay_evidence(path) == before_replay_evidence
        _assert_live_join_preflight_rejected_without_mutation(
            state,
            path,
            replace(identity, join_kind='strict'),
            match='kind',
        )

        joined = state.commit_live_join(
            identity,
            expected_revision=accepted.revision,
        )
        assert joined.revision == before.revision + 1
        assert not joined.is_operator_replay
        with closing(sqlite3.connect(path)) as inspection, inspection:
            assert inspection.execute(
                "SELECT COUNT(*) FROM join_commits WHERE group_name = 'editorial' "
            ).fetchone() == (1,)


def test_live_join_preflight_rejects_cursor_below_boundary_without_mutation(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        _assert_live_join_preflight_rejected_without_mutation(
            state,
            path,
            replace(
                _join_identity('eager', 'cursor'),
                boundary=42,
            ),
            match='cursor',
        )


def test_live_join_preflight_rejects_strict_pending_epoch_without_mutation(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(
        path,
        ordering='strict',
        max_pending_epochs=2,
        strict_holdback_secs=30.0,
    ) as state:
        captured = state.capture_epoch(
            low_id=42,
            high_id=45,
            captured_at=100.0,
            eligibility_point=130.0,
            reader_clock_checkpoint=100.0,
            source_generation='source-spawn-1',
            expected_revision=0,
        )
        advanced = state.advance_irrelevant(45, expected_revision=1)
        assert advanced.epochs == captured.epochs
        _assert_live_join_preflight_rejected_without_mutation(
            state,
            path,
            replace(
                _join_identity('strict', 'pending-epoch'),
                boundary=45,
            ),
            match='strict catch-up',
        )


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_live_join_rejects_each_independent_group_local_blocker(
    ordering: Literal['eager', 'strict'],
    tmp_path: Path,
) -> None:
    arguments = _recovery_arguments()

    parked_path = tmp_path / 'parked.db'
    with _open_group(parked_path, ordering=ordering) as state:
        state.park(expected_revision=state.snapshot().revision, **arguments)
        identity = _join_identity(ordering, 'parked')
        _assert_live_join_preflight_and_commit_rejected_without_mutation(
            state,
            parked_path,
            identity,
            match='recovery evidence',
        )

    replay_path = tmp_path / 'replay.db'
    with _open_group(replay_path, ordering=ordering) as state:
        _seed_replay_marker(state, 40)
        identity = _join_identity(ordering, 'replay')
        _assert_live_join_preflight_and_commit_rejected_without_mutation(
            state,
            replay_path,
            identity,
            match='recovery evidence',
        )

    history_path = tmp_path / 'history.db'
    with _open_group(history_path, ordering=ordering) as state:
        state.mark_history_truncated(42, expected_revision=state.snapshot().revision)
        identity = _join_identity(ordering, 'history')
        _assert_live_join_preflight_and_commit_rejected_without_mutation(
            state,
            history_path,
            identity,
            match='recovery evidence',
        )

    capacity_path = tmp_path / 'capacity.db'
    with _open_group(
        capacity_path,
        ordering=ordering,
        max_late_ids_per_group=1,
    ) as state:
        _seed_obligation(state)
        state.reserve_late_event('range-1', 42, expected_revision=state.snapshot().revision)
        state.reserve_late_event('range-1', 43, expected_revision=state.snapshot().revision)
        identity = _join_identity(ordering, 'capacity')
        _assert_live_join_preflight_and_commit_rejected_without_mutation(
            state,
            capacity_path,
            identity,
            match='recovery evidence',
        )

    disposition_path = tmp_path / 'disposition.db'
    with _open_group(disposition_path, ordering=ordering) as state:
        state.park(expected_revision=state.snapshot().revision, **arguments)
        dispose_at_frontier(
            state,
            'reset-to-latest',
            disposition_id='reset-1',
            new_generation=8,
            source_frontier=41,
            expected_revision=state.snapshot().revision,
            **arguments,
        )
        identity = _join_identity(ordering, 'disposition', group_generation=8)
        _assert_live_join_preflight_and_commit_rejected_without_mutation(
            state,
            disposition_path,
            identity,
            match='recovery evidence',
        )

    changed_path = tmp_path / 'configuration.db'
    with _open_group(changed_path, ordering=ordering):
        pass
    changed_ordering: Literal['eager', 'strict'] = 'strict' if ordering == 'eager' else 'eager'
    with _open_group(
        changed_path,
        ordering=changed_ordering,
        source_generation='source-spawn-2',
        stream_fingerprint='stream-v2',
        verified_incarnation='incarnation-2',
    ) as state:
        blocked = state.snapshot()
        assert {'stream-identity-changed', 'ordering-changed'} <= set(blocked.conditions)
        identity = replace(
            _join_identity(ordering, 'configuration'),
            source_generation=state.snapshot().source_generation,
        )
        _assert_live_join_preflight_and_commit_rejected_without_mutation(
            state,
            changed_path,
            identity,
            match='recovery evidence',
        )


def test_live_join_rejects_event_unavailable_callback_failure_and_wrong_kind(
    tmp_path: Path,
) -> None:
    arguments = _recovery_arguments()
    strict_identity = LiveJoinIdentity(
        site='studio',
        source='events',
        group='editorial',
        join_kind='strict',
        source_generation='source-spawn-1',
        group_generation=7,
        boundary=41,
        attempt_token=_attempt_id('strict-join'),
    )
    strict_path = tmp_path / 'strict.db'
    with _open_group(strict_path, ordering='strict') as state:
        unavailable = state.require_event(42, expected_revision=0)
        _assert_live_join_preflight_rejected_without_mutation(
            state,
            strict_path,
            strict_identity,
            match='recovery evidence',
        )
        with pytest.raises(InvalidTransitionError):
            state.commit_live_join(strict_identity, expected_revision=1)
        assert state.snapshot() == unavailable
        state.retry(
            event_id=42,
            callback_ordinal=None,
            expected_revision=1,
            **arguments,
        )
        state.restore_required_event(42, expected_revision=2)
        with pytest.raises(InvalidTransitionError):
            state.commit_live_join(
                replace(
                    strict_identity,
                    join_kind='eager',
                    attempt_token=_attempt_id('wrong-kind'),
                ),
                expected_revision=3,
            )
        assert state.commit_live_join(strict_identity, expected_revision=3).revision == 4

    failed_path = tmp_path / 'failed.db'
    with _open_group(failed_path) as state:
        state.start_event(42, _manifest(), expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        failed = state.callback_failed(0, 'failed', expected_revision=2)
        identity = replace(
            strict_identity,
            join_kind='eager',
            attempt_token=_attempt_id('failed-join'),
        )
        _assert_live_join_preflight_rejected_without_mutation(
            state,
            failed_path,
            identity,
            match='recovery evidence',
        )
        with pytest.raises(InvalidTransitionError):
            state.commit_live_join(identity, expected_revision=3)
        assert state.snapshot() == failed


def test_manual_and_conditional_parks_require_exact_durable_evidence(tmp_path: Path) -> None:
    identity = AuditIdentity.current()
    path = tmp_path / 'editorial.db'
    stale_directive = replace(
        _conditional_park_directive('evidence-stale'),
        group_revision=3,
        observed_at_wall_clock=1002.0,
    )
    exact_directive = replace(stale_directive, evidence_id='evidence-exact', group_revision=4)
    with _open_group(path) as state:
        parked = state.park(
            reason='maintenance',
            identity=identity,
            wall_clock_time=1000.0,
            expected_revision=0,
            operator_label='ticket-7',
        )
        assert parked.status == 'parked'
        unparked = state.unpark(
            reason='maintenance complete',
            identity=identity,
            wall_clock_time=1001.0,
            expected_revision=1,
        )
        assert unparked.status == 'running'
        state.start_event(42, _manifest(), expected_revision=2)
        state.begin_callback(0, expected_revision=3)
        before_stale = state.snapshot()
        stale = state.resolve_conditional_park(
            stale_directive,
            identity=identity,
            decided_at_wall_clock=1003.0,
        )
        assert stale == group_types.ConditionalParkResult(
            evidence_id='evidence-stale', outcome='stale', resulting_revision=4
        )
        assert state.snapshot() == before_stale
        conditional = state.resolve_conditional_park(
            exact_directive,
            identity=identity,
            decided_at_wall_clock=1003.0,
        )
        assert conditional == group_types.ConditionalParkResult(
            evidence_id='evidence-exact', outcome='parked', resulting_revision=5
        )
        parked_snapshot = state.snapshot()
        assert parked_snapshot.status == 'parked'
        rows = operator_records(state)
        assert tuple(record.command for record in rows) == ('park', 'unpark', 'conditional-park')
        assert rows[0].operator_label == 'ticket-7'

    expected_results = (
        ('evidence-exact', 7, exact_directive.fingerprint(), 'parked', 5, 1003.0),
        ('evidence-stale', 7, stale_directive.fingerprint(), 'stale', 4, 1003.0),
    )
    assert _conditional_park_rows(path) == expected_results
    with _open_group(path) as reopened:
        for directive, expected in ((stale_directive, stale), (exact_directive, conditional)):
            assert (
                reopened.resolve_conditional_park(
                    directive,
                    identity=identity,
                    decided_at_wall_clock=2003.0,
                )
                == expected
            )
        assert reopened.snapshot() == parked_snapshot
        assert operator_records(reopened) == rows
    assert _conditional_park_rows(path) == expected_results


def test_ordering_anomaly_acknowledgement_is_exact_audited_and_idempotent(
    tmp_path: Path,
) -> None:
    arguments = _recovery_arguments()
    with _open_group(tmp_path / 'editorial.db', ordering='strict') as state:
        state.advance_irrelevant(45, expected_revision=0)
        anomaly = state.mark_ordering_anomaly(43, expected_revision=1)
        assert anomaly.status == 'ordering-anomaly'
        acknowledged = state.acknowledge_ordering_anomaly(
            event_id=43,
            expected_revision=2,
            **arguments,
        )
        assert acknowledged.ordering_anomaly_id is None
        assert (
            state.acknowledge_ordering_anomaly(
                event_id=43,
                expected_revision=3,
                **arguments,
            )
            == acknowledged
        )
        record = operator_records(state)[-1]
        assert record.command == 'acknowledge-ordering-anomaly'
        assert record.event_id == 43


def test_orphan_clears_obligations_but_retains_marks_until_reset(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        connection = state._handoff.connection
        _seed_obligation(state)
        state.reserve_late_event('range-1', 42, expected_revision=state.snapshot().revision)
        _assert_operational_count(connection)

        # Residual marks are retained by orphaning and retired by a generation reset.
        connection.execute(
            "INSERT INTO callback_marks VALUES ('editorial', 41, 0, "
            "'publish', 'publish', 'done', 1)"
        )
        orphaned = state.change_lifecycle(
            LifecycleRequest(
                command='orphan',
                expected_lifecycle='active',
                expected_generation=7,
                transition_generation=7,
                identity=AuditIdentity.current(),
                wall_clock_time=1000.0,
            )
        )
        assert orphaned.callback_marks == ((0, 'publish', 'publish', 'done', 1),)
        assert orphaned.late_id_count == 0
        _assert_operational_count(connection)
        assert connection.execute(
            "SELECT COUNT(*) FROM range_groups WHERE group_name = 'editorial' "
        ).fetchone() == (0,)
        reset = dispose_at_frontier(
            state,
            'reset-to-latest',
            disposition_id='reset-residual-marks',
            new_generation=8,
            source_frontier=100,
            expected_revision=orphaned.revision,
            **_recovery_arguments(),
        )
        assert reset.callback_marks == ()


def test_orphan_requires_an_audited_reset_before_new_generation_progress(tmp_path: Path) -> None:
    arguments = _recovery_arguments()
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        orphaned = state.change_lifecycle(
            LifecycleRequest(
                command='orphan',
                expected_lifecycle='active',
                expected_generation=7,
                transition_generation=7,
                identity=AuditIdentity.current(),
                wall_clock_time=1000.0,
            )
        )
        assert orphaned.status == 'orphaned'
        assert {'orphaned', 'parked', 'replay-required'} <= set(orphaned.conditions)
    with _open_group(path) as readded:
        assert readded.snapshot().status == 'orphaned'
        reset = dispose_at_frontier(
            readded,
            'reset-to-latest',
            disposition_id='reset-1',
            new_generation=8,
            source_frontier=100,
            expected_revision=1,
            **arguments,
        )
        assert reset.group_generation == 8
        assert reset.cursor == 100
        assert reset.delivery_floor == 100
        terminal = readded.snapshot()
        assert terminal.conditions == ('parked',)
    with _open_group(path, group_generation=8) as reopened:
        assert reopened.snapshot() == terminal


def test_replay_receipt_survives_live_join_progress(tmp_path: Path) -> None:
    arguments = _recovery_arguments()
    with _open_group(tmp_path / 'editorial.db') as state:
        state.park(expected_revision=0, **arguments)
        before = state.snapshot()
        with pytest.raises(InvalidTransitionError):
            dispose_at_frontier(
                state,
                'replay',
                disposition_id='replay',
                new_generation=8,
                from_event_id=51,
                source_frontier=50,
                expected_revision=before.revision,
                **arguments,
            )
        assert state.snapshot() == before
        replaying = dispose_at_frontier(
            state,
            'replay',
            disposition_id='replay',
            new_generation=8,
            from_event_id=40,
            source_frontier=50,
            expected_revision=before.revision,
            **arguments,
        )
        assert replaying.cursor == replaying.delivery_floor == 39
        assert replaying.group_generation == 8
        assert replaying.is_operator_replay
        state.unpark(expected_revision=state.snapshot().revision, **arguments)
        state.advance_irrelevant(50, expected_revision=state.snapshot().revision)
        first = replace(_join_identity('eager', 'first', group_generation=8), boundary=50)
        joined = state.commit_live_join(first, expected_revision=state.snapshot().revision)
        assert not joined.is_operator_replay
        second = replace(first, attempt_token=_attempt_id('second'))
        rejoined = state.commit_live_join(second, expected_revision=joined.revision)
        with pytest.raises(RevisionConflictError):
            state.commit_live_join(first, expected_revision=joined.revision - 1)
        with pytest.raises(InvalidTransitionError):
            state.commit_live_join(
                replace(first, boundary=49), expected_revision=rejoined.revision
            )
        assert (
            dispose_at_frontier(
                state,
                'replay',
                disposition_id='replay',
                new_generation=8,
                from_event_id=40,
                source_frontier=100,
                expected_revision=before.revision,
                **arguments,
            )
            == replaying
        )
        assert state.snapshot() == rejoined
        receipt = next(row for row in operator_records(state) if row.command == 'replay')
        assert receipt.disposition_id == 'replay'
        assert receipt.terminal_result == 'applied'


def test_required_event_blocks_epoch_completion_without_a_manifest(tmp_path: Path) -> None:
    with _open_group(
        tmp_path / 'editorial.db',
        ordering='strict',
        max_pending_epochs=2,
        strict_holdback_secs=30.0,
    ) as state:
        captured = state.capture_epoch(
            low_id=42,
            high_id=45,
            captured_at=100.0,
            eligibility_point=130.0,
            reader_clock_checkpoint=100.0,
            source_generation='source-spawn-1',
            expected_revision=0,
        )
        delivering = state.mark_epoch_eligible(
            captured.epochs[0].epoch_id,
            reader_clock_checkpoint=130.0,
            expected_revision=1,
        )
        blocked = state.require_event(42, expected_revision=2)
        with pytest.raises(InvalidTransitionError):
            state.complete_epoch(
                delivering.epochs[0].epoch_id,
                cursor=45,
                expected_revision=blocked.revision,
            )
        assert state.snapshot() == blocked


def test_reset_disposition_clears_operational_late_id_count_and_old_generation_work(
    tmp_path: Path,
) -> None:
    arguments = _recovery_arguments()
    path = tmp_path / 'editorial.db'
    with _open_group(
        path,
        ordering='strict',
        max_late_ids_per_group=1,
        max_pending_epochs=2,
        strict_holdback_secs=30.0,
    ) as state:
        state.capture_epoch(
            low_id=42,
            high_id=45,
            captured_at=100.0,
            eligibility_point=130.0,
            reader_clock_checkpoint=100.0,
            source_generation='source-spawn-1',
            expected_revision=state.snapshot().revision,
        )
        _seed_obligation(state)
        state.reserve_late_event('range-1', 42, expected_revision=state.snapshot().revision)
        capacity = state.reserve_late_event(
            'range-1', 43, expected_revision=state.snapshot().revision
        )
        assert 'late-capacity-exhausted' in capacity.conditions
        _assert_operational_count(state._handoff.connection)
        state.park(expected_revision=state.snapshot().revision, **arguments)
        reset = dispose_at_frontier(
            state,
            'reset-to-latest',
            disposition_id='reset-capacity',
            new_generation=8,
            source_frontier=100,
            expected_revision=state.snapshot().revision,
            **arguments,
        )
        assert reset.group_generation == 8
        assert reset.epochs == ()
        assert reset.obligation_count == 0
        assert reset.late_id_count == 0
        _assert_operational_count(state._handoff.connection)
        assert state._handoff.connection.execute(
            'SELECT group_generation, obligation_state FROM range_groups '
            "WHERE group_name = 'editorial' "
        ).fetchall() == [(8, 'satisfied-by-floor')]
        assert reset.conditions == ('parked',)
    with _open_group(path, group_generation=8, ordering='strict') as reopened:
        assert reopened.snapshot() == reset


@pytest.mark.parametrize(
    'manifest',
    [
        CallbackManifest(callbacks=()),
        CallbackManifest(callbacks=(replace(_manifest().callbacks[0], ordinal=1),)),
    ],
)
def test_restart_rejects_invalid_manifest_without_audit_or_state_change(
    manifest: CallbackManifest,
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        started = state.start_event(42, _manifest(), expected_revision=0)
        before_audit = operator_records(state)
        with pytest.raises(InvalidTransitionError):
            state.restart_event(
                manifest,
                expected_revision=started.revision,
                **_recovery_arguments(),
            )
        assert state.snapshot() == started
        assert operator_records(state) == before_audit


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_retire_truncated_in_flight_event(
    tmp_path: Path, command: Literal['replay', 'reset-to-latest']
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        request = _truncated_retirement_request(state)
        request = replace(
            request,
            command=command,
            from_event_id=500 if command == 'replay' else None,
            acknowledge_truncation=command == 'replay',
        )
        before = state.snapshot()
        result = state.dispose(request)

        assert result.group_generation == 8
        assert result.parked
        assert result.cursor == result.delivery_floor == (499 if command == 'replay' else 900)
        assert result.in_flight_event_id is None
        assert result.history_truncated_floor is None
        connection = state._database.connection
        assert connection.execute('SELECT COUNT(*) FROM callback_marks').fetchone() == (0,)
        retired = connection.execute(
            'SELECT event_id, result_snapshot_json, operator_label, reason FROM audit '
            "WHERE command = 'retire-truncated-event'"
        ).fetchall()
        assert len(retired) == 1
        assert retired[0][0] == 100
        assert snapshot_from_json(retired[0][1]) == before
        assert retired[0][2:] == ('ticket-7', 'operator recovery')


@pytest.mark.parametrize(
    'statement',
    [
        'UPDATE group_state SET orphaned = 1',
        'UPDATE group_state SET stream_identity_changed = 1',
        'UPDATE group_state SET parked = 0',
        'UPDATE groups SET active = 0',
        "UPDATE groups SET verified_incarnation = 'other'",
        "UPDATE source_meta SET source_generation = 'other'",
        "UPDATE source_meta SET stream_fingerprint = 'other'",
        "UPDATE source_meta SET stream_incarnation = 'other'",
    ],
)
@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_retirement_rejects_inactive_or_changed_identity(
    tmp_path: Path, statement: str, command: Literal['replay', 'reset-to-latest']
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        request = _truncated_retirement_request(state)
        request = replace(
            request, command=command, from_event_id=500 if command == 'replay' else None
        )
        connection = state._database.connection
        connection.execute(statement)
        before = tuple(connection.iterdump())
        with pytest.raises(InvalidTransitionError):
            state.dispose(request)
        assert tuple(connection.iterdump()) == before


@pytest.mark.parametrize(('from_event_id', 'acknowledge_truncation'), [(500, False), (400, True)])
def test_retirement_replay_requires_acknowledgement_and_floor(
    tmp_path: Path, from_event_id: int, acknowledge_truncation: bool
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        request = replace(
            _truncated_retirement_request(state),
            from_event_id=from_event_id,
            acknowledge_truncation=acknowledge_truncation,
        )
        connection = state._database.connection
        before = tuple(connection.iterdump())
        with pytest.raises(
            InvalidTransitionError, match='position changes require no active event'
        ):
            state.dispose(request)
        assert tuple(connection.iterdump()) == before


@pytest.mark.parametrize(
    ('from_event_id', 'marker', 'message'),
    [(950, None, 'source frontier'), (700, 600, 'retained replay evidence')],
)
def test_retirement_replay_validates_bounds_without_mutation(
    tmp_path: Path, from_event_id: int, marker: int | None, message: str
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        request = _truncated_retirement_request(state)
        if marker is not None:
            state._database.connection.execute(
                'UPDATE group_state SET replay_required_from = ?', (marker,)
            )
        request = replace(
            request, from_event_id=from_event_id, expected_revision=state.snapshot().revision
        )
        connection = state._database.connection
        before = tuple(connection.iterdump())
        with pytest.raises(InvalidTransitionError, match=message):
            state.dispose(request)
        assert tuple(connection.iterdump()) == before


@pytest.mark.parametrize('floor', [None, 100])
@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_retirement_requires_floor_above_in_flight_event(
    tmp_path: Path, floor: int | None, command: Literal['replay', 'reset-to-latest']
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        request = _truncated_retirement_request(state)
        request = replace(
            request, command=command, from_event_id=500 if command == 'replay' else None
        )
        connection = state._database.connection
        connection.execute('UPDATE group_state SET history_truncated_floor = ?', (floor,))
        before = tuple(connection.iterdump())
        with pytest.raises(
            InvalidTransitionError, match='position changes require no active event'
        ):
            state.dispose(request)
        assert tuple(connection.iterdump()) == before


@pytest.mark.parametrize('command', ['replay', 'reset-to-latest'])
def test_retirement_rolls_back_on_failure(
    tmp_path: Path, command: Literal['replay', 'reset-to-latest']
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        request = _truncated_retirement_request(state)
        request = replace(
            request, command=command, from_event_id=500 if command == 'replay' else None
        )
        connection = state._database.connection
        connection.execute(
            """CREATE TRIGGER fail_after_retirement AFTER DELETE ON callback_marks
            WHEN EXISTS (SELECT 1 FROM audit WHERE command = 'retire-truncated-event')
            BEGIN SELECT RAISE(ABORT, 'retirement rollback probe'); END"""
        )
        before = tuple(connection.iterdump())
        with pytest.raises(DatabaseError, match='retirement rollback probe'):
            state.dispose(request)
        assert tuple(connection.iterdump()) == before


def test_shape_mismatch_blocks_skip_without_retiring_event(tmp_path: Path) -> None:
    recorded_manifest = _manifest()
    with _open_group(tmp_path / 'editorial.db') as state:
        state.start_event(42, recorded_manifest, expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        mismatch = state.verify_manifest(_manifest_pair(), expected_revision=2)
        before_snapshot = state.snapshot()
        before_marks = before_snapshot.callback_marks
        before_audit_count = len(operator_records(state))

        assert mismatch.status == 'shape-mismatch'
        with pytest.raises(InvalidTransitionError):
            state.skip_callback(
                event_id=42,
                callback_ordinal=0,
                expected_revision=mismatch.revision,
                **_recovery_arguments(),
            )

        after_snapshot = state.snapshot()
        assert after_snapshot == before_snapshot
        assert after_snapshot.callback_marks == before_marks
        assert len(operator_records(state)) == before_audit_count
        assert after_snapshot.cursor == 41
        assert after_snapshot.in_flight_event_id == 42
        assert after_snapshot.manifest == recorded_manifest


def test_shape_mismatch_restart_event_restarts_same_event_after_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    recorded_manifest = _manifest_pair()
    current_manifest = _manifest()
    with _open_group(path) as state:
        state.start_event(42, recorded_manifest, expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        state.callback_done(0, expected_revision=2)
        mismatch = state.verify_manifest(current_manifest, expected_revision=3)
        assert mismatch.status == 'shape-mismatch'

    with _open_group(path) as reopened:
        before = reopened.snapshot()
        assert before.callback_marks == (
            (0, 'publish', 'publish', 'done', 1),
            (1, 'publish', 'archive', 'pending', 0),
        )
        restarted = reopened.restart_event(
            current_manifest,
            expected_revision=before.revision,
            **_recovery_arguments(),
        )

        assert before.status == 'shape-mismatch'
        assert before.in_flight_event_id == 42
        assert restarted.status == 'running'
        assert restarted.cursor == 41
        assert restarted.in_flight_event_id == 42
        assert restarted.manifest == current_manifest
        assert restarted.callback_marks == ((0, 'publish', 'publish', 'pending', 0),)
        assert tuple(record.command for record in operator_records(reopened)) == ('restart-event',)


def test_shape_mismatch_restored_manifest_resumes_same_event_after_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    recorded_manifest = _manifest_pair()
    with _open_group(path) as state:
        state.start_event(42, recorded_manifest, expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        state.callback_done(0, expected_revision=2)
        mismatch = state.verify_manifest(_manifest(), expected_revision=3)
        assert mismatch.status == 'shape-mismatch'

    with _open_group(path) as reopened:
        before = reopened.snapshot()
        restored = reopened.verify_manifest(
            recorded_manifest,
            expected_revision=before.revision,
        )
        resumed = reopened.begin_callback(1, expected_revision=restored.revision)

        assert before.status == 'shape-mismatch'
        assert before.in_flight_event_id == 42
        assert restored.status == 'running'
        assert restored.cursor == 41
        assert restored.in_flight_event_id == 42
        assert restored.manifest == recorded_manifest
        assert restored.callback_marks == (
            (0, 'publish', 'publish', 'done', 1),
            (1, 'publish', 'archive', 'pending', 0),
        )
        assert resumed.in_flight_event_id == 42
        assert resumed.callback_marks[1][-1] == 1


def test_strict_capacity_abort_streak_is_token_fenced_durable_and_reset(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path, ordering='strict') as state:
        first = state.record_strict_join_outcome(
            attempt_token=_attempt_id('capacity-1'),
            capacity_caused=True,
            abort_limit=JOIN_ABORT_LIMIT,
            expected_revision=0,
        )
        assert first.strict_capacity_abort_streak == 1
        assert first.strict_capacity_attempt_token == _attempt_id('capacity-1')
        assert (
            state.record_strict_join_outcome(
                attempt_token=_attempt_id('capacity-1'),
                capacity_caused=True,
                abort_limit=JOIN_ABORT_LIMIT,
                expected_revision=0,
            )
            == first
        )
        with pytest.raises(InvalidTransitionError, match='outcome conflicts'):
            state.record_strict_join_outcome(
                attempt_token=_attempt_id('capacity-1'),
                capacity_caused=False,
                abort_limit=JOIN_ABORT_LIMIT,
                expected_revision=0,
            )

    with _open_group(path, ordering='strict') as reopened:
        assert reopened.snapshot().strict_capacity_abort_streak == 1
        reset = reopened.record_strict_join_outcome(
            attempt_token=_attempt_id('non-capacity'),
            capacity_caused=False,
            abort_limit=JOIN_ABORT_LIMIT,
            expected_revision=1,
        )
        assert reset.strict_capacity_abort_streak == 0
        retained = reopened.record_strict_join_outcome(
            attempt_token=_attempt_id('capacity-2'),
            capacity_caused=True,
            abort_limit=JOIN_ABORT_LIMIT,
            expected_revision=reset.revision,
        )
        assert retained.strict_capacity_abort_streak == 1
        pending = reopened.commit_live_join(
            _join_identity('strict', _attempt_id('joined')),
            expected_revision=retained.revision,
        )
        assert pending.strict_capacity_abort_streak == 1
        assert pending.strict_capacity_attempt_token == _attempt_id('joined')
        joined = reopened.record_strict_join_outcome(
            attempt_token=_attempt_id('joined'),
            capacity_caused=False,
            abort_limit=JOIN_ABORT_LIMIT,
            expected_revision=pending.revision,
        )
        assert joined.strict_capacity_abort_streak == 0
        assert (
            reopened.record_strict_join_outcome(
                attempt_token=_attempt_id('joined'),
                capacity_caused=False,
                abort_limit=JOIN_ABORT_LIMIT,
                expected_revision=pending.revision,
            )
            == joined
        )
        with pytest.raises(InvalidTransitionError, match='outcome conflicts'):
            reopened.record_strict_join_outcome(
                attempt_token=_attempt_id('joined'),
                capacity_caused=True,
                abort_limit=JOIN_ABORT_LIMIT,
                expected_revision=pending.revision,
            )
        current = joined
        for attempt in range(JOIN_ABORT_LIMIT):
            current = reopened.record_strict_join_outcome(
                attempt_token=_attempt_id(f'terminal-{attempt}'),
                capacity_caused=True,
                abort_limit=JOIN_ABORT_LIMIT,
                expected_revision=current.revision,
            )
        assert current.strict_capacity_abort_streak == JOIN_ABORT_LIMIT
        assert current.status == 'strict-capacity-exhausted'

    with _open_group(path, ordering='strict') as restarted:
        assert restarted.snapshot().status == 'strict-capacity-exhausted'
        assert restarted.snapshot().strict_capacity_abort_streak == JOIN_ABORT_LIMIT


def test_strict_epoch_completion_waits_for_obligation_within_its_range(
    tmp_path: Path,
) -> None:
    higher = _strict_obligation(
        obligation_id='range-higher',
        low_id=46,
        high_id=48,
    )
    lower = _strict_obligation(obligation_id='range-lower')
    with _open_group(
        tmp_path / 'editorial.db',
        ordering='strict',
        max_pending_epochs=2,
        strict_holdback_secs=30.0,
    ) as state:
        captured = state.capture_epoch(
            low_id=42,
            high_id=45,
            captured_at=100.0,
            eligibility_point=130.0,
            reader_clock_checkpoint=100.0,
            source_generation='source-spawn-1',
            expected_revision=state.snapshot().revision,
        )
        delivering = state.mark_epoch_eligible(
            captured.epochs[0].epoch_id,
            reader_clock_checkpoint=130.0,
            expected_revision=state.snapshot().revision,
        )
        _seed_obligation(state, higher)
        _seed_obligation(state, lower)
        blocked = state.snapshot()

        with pytest.raises(InvalidTransitionError):
            state.complete_epoch(
                delivering.epochs[0].epoch_id,
                cursor=45,
                expected_revision=blocked.revision,
            )
        assert state.snapshot() == blocked


def test_strict_epochs_are_disjoint_bounded_and_checkpointed(tmp_path: Path) -> None:
    with _open_group(
        tmp_path / 'editorial.db',
        ordering='strict',
        max_pending_epochs=2,
        strict_holdback_secs=30.0,
    ) as state:
        first = state.capture_epoch(
            low_id=42,
            high_id=45,
            captured_at=100.0,
            eligibility_point=130.0,
            reader_clock_checkpoint=100.0,
            source_generation='source-spawn-1',
            expected_revision=0,
        )
        assert first.reader_clock_checkpoint == 100.0
        assert first.epochs[0].low_id == 42
        assert first.epochs[0].state == 'waiting'

        with pytest.raises(InvalidTransitionError):
            state.capture_epoch(
                low_id=47,
                high_id=50,
                captured_at=130.0,
                eligibility_point=160.0,
                reader_clock_checkpoint=130.0,
                source_generation='source-spawn-1',
                expected_revision=1,
            )
        assert state.snapshot() == first

        second = state.capture_epoch(
            low_id=46,
            high_id=50,
            captured_at=130.0,
            eligibility_point=160.0,
            reader_clock_checkpoint=130.0,
            source_generation='source-spawn-1',
            expected_revision=1,
        )
        assert tuple((epoch.low_id, epoch.high_id) for epoch in second.epochs) == (
            (42, 45),
            (46, 50),
        )
        with pytest.raises(InvalidTransitionError):
            state.capture_epoch(
                low_id=51,
                high_id=55,
                captured_at=160.0,
                eligibility_point=190.0,
                reader_clock_checkpoint=160.0,
                source_generation='source-spawn-1',
                expected_revision=2,
            )
        assert state.snapshot() == second

        with pytest.raises(InvalidTransitionError):
            state.mark_epoch_eligible(
                second.epochs[1].epoch_id,
                reader_clock_checkpoint=160.0,
                expected_revision=2,
            )
        assert state.snapshot() == second
        delivering = state.mark_epoch_eligible(
            second.epochs[0].epoch_id,
            reader_clock_checkpoint=130.0,
            expected_revision=2,
        )
        assert delivering.epochs[0].state == 'delivering'
        assert delivering.reader_clock_checkpoint == 130.0
        with pytest.raises(InvalidTransitionError):
            state.complete_epoch(
                delivering.epochs[0].epoch_id,
                cursor=44,
                expected_revision=3,
            )
        with pytest.raises(InvalidTransitionError):
            state.complete_epoch(
                delivering.epochs[0].epoch_id,
                cursor=50,
                expected_revision=3,
            )
        completed = state.complete_epoch(
            delivering.epochs[0].epoch_id,
            cursor=45,
            expected_revision=3,
        )
        assert completed.cursor == 45
        assert tuple(epoch.epoch_id for epoch in completed.epochs) == (
            delivering.epochs[1].epoch_id,
        )
        joined = state.commit_live_join(
            LiveJoinIdentity(
                site='studio',
                source='events',
                group='editorial',
                join_kind='strict',
                source_generation='source-spawn-1',
                group_generation=7,
                boundary=45,
                attempt_token=_attempt_id('strict-epoch-join'),
            ),
            expected_revision=4,
        )
        assert joined.latest_join_boundary == 45


def test_strict_floor_skip_preserves_strict_audit_command(tmp_path: Path) -> None:
    with _open_group(tmp_path / 'strict.db', ordering='strict') as state:
        state.record_strict_floor_skip(40, expected_revision=0)

        assert operator_records(state)[-1].command == 'strict-delivery-floor-skip'


def test_strict_live_join_waits_for_obligations_through_fixed_boundary(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(
        path,
        ordering='strict',
        max_pending_epochs=2,
        strict_holdback_secs=30.0,
    ) as state:
        captured = state.capture_epoch(
            low_id=42,
            high_id=45,
            captured_at=100.0,
            eligibility_point=130.0,
            reader_clock_checkpoint=100.0,
            source_generation='source-spawn-1',
            expected_revision=state.snapshot().revision,
        )
        delivering = state.mark_epoch_eligible(
            captured.epochs[0].epoch_id,
            reader_clock_checkpoint=130.0,
            expected_revision=state.snapshot().revision,
        )
        state.complete_epoch(
            delivering.epochs[0].epoch_id,
            cursor=45,
            expected_revision=state.snapshot().revision,
        )
        _seed_obligation(state)
        identity = LiveJoinIdentity(
            site='studio',
            source='events',
            group='editorial',
            join_kind='strict',
            source_generation='source-spawn-1',
            group_generation=7,
            boundary=45,
            attempt_token=_attempt_id('strict-obligation-join'),
        )
        _assert_live_join_preflight_rejected_without_mutation(
            state,
            path,
            identity,
            match='strict catch-up',
        )
        with pytest.raises(InvalidTransitionError):
            state.commit_live_join(identity, expected_revision=state.snapshot().revision)
        connection = state._database.connection
        close_test_range(connection, 'range-1')
        scanned = state.complete_obligation_scan('range-1')
        assert (
            state.commit_live_join(identity, expected_revision=scanned.revision).revision
            == scanned.revision + 1
        )


def test_strict_ordering_decision_rejects_stale_identity_without_mutation(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path, ordering='strict') as state:
        advanced = state.advance_irrelevant(102, expected_revision=0)
        identity = replace(
            _join_identity('strict', _attempt_id('current')),
            boundary=102,
        )
        joined = state.commit_live_join(identity, expected_revision=advanced.revision)
        cases = (
            (replace(identity, source_generation='source-stale'), 101, 102, 41),
            (replace(identity, group_generation=8), 101, 102, 41),
            (replace(identity, attempt_token=_attempt_id('stale')), 101, 102, 41),
            (identity, 101, 103, 41),
            (identity, 101, 102, 42),
            (identity, 103, 102, 41),
        )
        for candidate, event_id, last_delivered_id, delivery_floor in cases:
            with pytest.raises(InvalidTransitionError):
                state.record_strict_ordering_decision(
                    StrictOrderingControl(
                        identity=candidate,
                        event_id=event_id,
                        last_delivered_id=last_delivered_id,
                        delivery_floor=delivery_floor,
                        action='halt',
                    ),
                    expected_revision=joined.revision,
                )
            assert state.snapshot() == joined
            assert operator_records(state) == ()


def test_terminal_disposition_identity_is_exact_and_fails_closed_on_conflict(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    identity = AuditIdentity.current()
    with _open_disposition_group(path) as state:
        state.change_lifecycle(
            LifecycleRequest(
                command='orphan',
                expected_lifecycle='active',
                expected_generation=7,
                transition_generation=7,
                identity=AuditIdentity.current(),
                wall_clock_time=1000.0,
            )
        )
        dispose_at_frontier(
            state,
            'reset-to-latest',
            disposition_id='reset-1',
            new_generation=8,
            source_frontier=100,
            expected_revision=1,
            reason='configured group re-added',
            identity=identity,
            wall_clock_time=1.0,
        )

        assert state.terminal_disposition_identity(7) is None
        assert state.terminal_disposition_identity(8) == (
            'reset-1',
            next(
                row.command_fingerprint
                for row in operator_records(state)
                if row.command == 'reset-to-latest'
            ),
            'applied',
        )

        database = state._database
        database.connection.execute(
            """
            INSERT INTO audit (
                site, group_name, command, wall_clock_time,
                prior_revision, new_revision, reason,
                local_uid, username, pid, disposition_id,
                command_fingerprint, terminal_result, result_snapshot_json
            )
            SELECT site, group_name, command, wall_clock_time,
                   prior_revision, new_revision, reason,
                   local_uid, username, pid, 'reset-conflict',
                   'conflicting-fingerprint', terminal_result,
                   result_snapshot_json
            FROM audit
            WHERE group_name = 'editorial' AND disposition_id = 'reset-1'
              AND terminal_result IS NOT NULL
            """
        )
        with pytest.raises(InvalidTransitionError, match='terminal disposition rows conflict'):
            state.terminal_disposition_identity(8)


@pytest.mark.parametrize('reused_command', ['replay', 'reset-to-latest'])
def test_terminal_disposition_identity_rejects_reuse_after_reset(
    tmp_path: Path,
    reused_command: Literal['replay', 'reset-to-latest'],
) -> None:
    path = tmp_path / 'editorial.db'
    arguments = _recovery_arguments()
    with _open_group(path) as state:
        state.park(expected_revision=0, **arguments)
        terminal = dispose_at_frontier(
            state,
            'replay',
            disposition_id='replay-original',
            new_generation=8,
            from_event_id=40,
            source_frontier=50,
            expected_revision=1,
            **arguments,
        )
        before = dispose_at_frontier(
            state,
            'reset-to-latest',
            disposition_id='reset-later',
            new_generation=9,
            source_frontier=50,
            expected_revision=terminal.revision,
            **arguments,
        )
    with _open_group(path, group_generation=9) as state:
        audit_before = operator_records(state)
        with pytest.raises(InvalidTransitionError, match='intervening disposition'):
            dispose_at_frontier(
                state,
                'replay',
                disposition_id='replay-original',
                new_generation=8,
                from_event_id=40,
                source_frontier=70,
                expected_revision=1,
                **arguments,
            )
        with pytest.raises(InvalidTransitionError, match='conflicting content'):
            dispose_at_frontier(
                state,
                reused_command,
                disposition_id='replay-original',
                new_generation=10,
                from_event_id=40 if reused_command == 'replay' else None,
                source_frontier=70,
                expected_revision=before.revision,
                **arguments,
            )
        assert state.snapshot() == before
        assert operator_records(state) == audit_before


@pytest.mark.parametrize('outcome', ['done', 'skipped'])
def test_terminal_late_delta_failure_rolls_back_callback_and_audit(
    tmp_path: Path,
    outcome: str,
) -> None:
    with _open_group(tmp_path / 'terminal-rollback.db') as state:
        _seed_obligation(state)
        state.reserve_late_event('range-1', 42, expected_revision=state.snapshot().revision)
        state.start_event(
            42, _manifest(), expected_revision=state.snapshot().revision, obligation_id='range-1'
        )
        before = state.begin_callback(0, expected_revision=state.snapshot().revision)
        connection = state._database.connection

        def deny_count_update(
            action: int,
            _table: str | None,
            column: str | None,
            _database: str | None,
            _trigger: str | None,
        ) -> int:
            if action == sqlite3.SQLITE_UPDATE and column == 'operational_late_id_count':
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(deny_count_update)
        try:
            if outcome == 'done':
                with pytest.raises(DatabaseError):
                    state.callback_done(0, expected_revision=before.revision)
            else:
                with pytest.raises(DatabaseError):
                    state.skip_callback(
                        event_id=42,
                        callback_ordinal=0,
                        expected_revision=before.revision,
                        **_recovery_arguments(),
                    )
        finally:
            connection.set_authorizer(None)
        assert state.snapshot() == before
        assert operator_records(state) == ()
        assert connection.execute(
            "SELECT terminal_state FROM late_ids WHERE group_name = 'editorial' "
        ).fetchone() == ('pending',)
        _assert_operational_count(connection)
        if outcome == 'done':
            retried = state.callback_done(0, expected_revision=before.revision)
        else:
            retried = state.skip_callback(
                event_id=42,
                callback_ordinal=0,
                expected_revision=before.revision,
                **_recovery_arguments(),
            )
        assert retried.late_id_count == 0
        _assert_operational_count(connection)
