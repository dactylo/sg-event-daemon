from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast
from unittest.mock import Mock

import pytest

from sg_event_daemon import _group_delivery as delivery
from sg_event_daemon._group_delivery import finish_terminal_event
from sg_event_daemon._group_errors import (
    InvalidTransitionError,
    StrictObligationBlockedError,
    TerminalLateEventError,
)
from sg_event_daemon._group_state import PendingRange
from sg_event_daemon._group_types import (
    AuditIdentity,
    BeginCallbackStep,
    CompleteLateStep,
    GroupSnapshot,
    ReserveLateStep,
    StartEventStep,
)
from sg_event_daemon._plugins import CallbackIdentity, CallbackManifest
from tests._storage_support import bind_group, close_test_range, insert_range, open_test_group
from tests.group._support import assert_operational_count as _assert_operational_count
from tests.group._support import (
    audit_records,
    commit_step,
    dispose_at_frontier,
    seed_pending_range,
)

if TYPE_CHECKING:
    from sqlite3 import Connection

    from sg_event_daemon._group_state import GroupState


_COMPLETION_CASES = (
    'success',
    'stale_revision',
    'missing_inflight',
    'failure',
    'required_event',
    'shape_mismatch',
    'malformed_manifest',
    'missing_mark',
    'uninvoked_mark',
    'terminal_mark',
    'nonfinal_callback',
    'strict_fence',
    'late_pending',
    'late_done',
    'late_other_fence',
    'parked',
)


_COMPLETION_TABLE_QUERIES = (
    'SELECT * FROM _daemon_metadata',
    'SELECT * FROM audit',
    'SELECT * FROM callback_marks',
    'SELECT * FROM conditional_park_results',
    'SELECT * FROM discovery_ranges',
    'SELECT * FROM disposition_audit',
    'SELECT * FROM epochs',
    'SELECT * FROM group_state',
    'SELECT * FROM groups',
    'SELECT * FROM join_commits',
    'SELECT * FROM late_ids',
    'SELECT * FROM pending_adoptions',
    'SELECT * FROM range_groups',
    'SELECT * FROM replay_markers',
    'SELECT * FROM source_late_ids',
    'SELECT * FROM source_meta',
    'SELECT * FROM sqlite_sequence',
    'SELECT * FROM stream_adoptions',
)


def _completion_observation(connection: Connection, revision: int) -> dict[str, object]:
    connection.execute('BEGIN IMMEDIATE')
    connection.execute('SAVEPOINT completion_observation')
    try:
        try:
            result = delivery.callback_done(connection, 'editorial', 0, revision)
        except (RuntimeError, ValueError) as error:
            connection.execute('ROLLBACK TO completion_observation')
            outcome = {'error': type(error).__name__, 'message': str(error)}
        else:
            outcome = {'snapshot': asdict(result)}
        connection.execute('RELEASE completion_observation')
        tables = {
            query.rsplit(' ', 1)[1]: sorted(connection.execute(query).fetchall(), key=repr)
            for query in _COMPLETION_TABLE_QUERIES
        }
        assert set(tables) == {
            name
            for (name,) in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type = 'table'"
            )
        }
        # Normalize tuple-valued snapshot fields and SQL rows to JSON arrays.
        return json.loads(json.dumps({'outcome': outcome, 'tables': tables}))
    finally:
        connection.rollback()


def _database_connection(state: GroupState) -> Connection:
    handle = object.__getattribute__(state, '_database')
    return cast('Connection', object.__getattribute__(handle, 'connection'))


def _manifest(*callback_ids: str) -> CallbackManifest:
    return CallbackManifest(
        callbacks=tuple(
            CallbackIdentity(
                ordinal=ordinal,
                plugin_id='publish',
                callback_id=callback_id,
                execution='sync',
                filter_digest=f'filter-{ordinal}',
                config_digest=f'config-{ordinal}',
                secret_refs=(('token', 'PUBLISH_TOKEN'),),
                credential_name='publisher',
                credential_fingerprint='publisher:publish-script',
            )
            for ordinal, callback_id in enumerate(callback_ids)
        )
    )


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('case', _COMPLETION_CASES)
def test_callback_done_characterization(
    tmp_path: Path, ordering: Literal['eager', 'strict'], case: str
) -> None:
    baseline = json.loads(
        (Path(__file__).parent / 'fixtures' / 'completion_characterization.json').read_text()
    )
    with _open_group(tmp_path / 'editorial.db', ordering=ordering) as state:
        revision = _seed_completion_case(state, case)
        assert (
            _completion_observation(_database_connection(state), revision)
            == baseline[f'{ordering}/{case}']
        )


def _open_group(
    path: Path,
    *,
    max_failure_text_bytes: int = 262_144,
    max_manifest_bytes: int = 1_048_576,
    ordering: Literal['eager', 'strict'] = 'eager',
) -> GroupState:
    return open_test_group(
        path,
        max_failure_text_bytes=max_failure_text_bytes,
        max_manifest_bytes=max_manifest_bytes,
        ordering=ordering,
    )


def _resolve_obligation(
    state: GroupState,
    obligation: PendingRange,
) -> int:
    connection = _database_connection(state)
    close_test_range(connection, obligation.obligation_id)
    resolved = state.complete_obligation_scan(obligation.obligation_id)
    return resolved.revision


def _seed_completion_case(state: GroupState, case: str) -> int:
    commit_step(state, StartEventStep(42, _manifest('publish'), None), expected_revision=0)
    commit_step(state, BeginCallbackStep(0), expected_revision=1)
    connection = _database_connection(state)
    # Seed deterministic audit metadata before observing completion; retain every row.
    connection.execute(
        "UPDATE audit SET wall_clock_time = 1, local_uid = 1000, username = 'test-user', pid = 1"
    )
    updates = {
        'missing_inflight': 'UPDATE group_state SET in_flight_event_id = NULL',
        'failure': "UPDATE group_state SET failure_code = 'callback-failed'",
        'required_event': 'UPDATE group_state SET required_event_id = 42',
        'shape_mismatch': 'UPDATE group_state SET shape_mismatch = 1',
        'malformed_manifest': "UPDATE group_state SET manifest_json = 'invalid'",
        'parked': 'UPDATE group_state SET parked = 1',
    }
    if case in updates:
        connection.execute(updates[case])
    elif case == 'missing_mark':
        connection.execute('DELETE FROM callback_marks')
    elif case == 'uninvoked_mark':
        connection.execute('UPDATE callback_marks SET delivery_attempt = 0')
    elif case == 'terminal_mark':
        connection.execute("UPDATE callback_marks SET outcome = 'done'")
    elif case == 'nonfinal_callback':
        connection.execute(
            "INSERT INTO callback_marks VALUES ('editorial', 42, 1, 'publish', "
            "'next', 'pending', 0)"
        )
    elif case == 'strict_fence':
        seed_pending_range(connection, 'fence', 41, 51)
    elif case.startswith('late_'):
        seed_pending_range(connection, 'own', 42, 52)
        connection.execute(
            "UPDATE group_state SET in_flight_obligation_id = 'own', "
            'operational_late_id_count = ?',
            (0 if case == 'late_done' else 1,),
        )
        connection.execute(
            "INSERT INTO late_ids VALUES ('editorial', 'own', 42, ?, NULL)",
            ('done' if case == 'late_done' else 'pending',),
        )
        if case == 'late_other_fence':
            seed_pending_range(connection, 'earlier', 1, 11)
    return 1 if case == 'stale_revision' else 2


def _obligation(
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
        eligibility_point=30.0,
        visibility_deadline=0.0,
    )


def test_callback_failure_stops_later_callbacks_and_preserves_failure_evidence(
    tmp_path: Path,
) -> None:
    failure = 'ab\u00e9\U0001f642tail'
    failure_bytes = failure.encode()
    with _open_group(tmp_path / 'editorial.db', max_failure_text_bytes=5) as state:
        commit_step(
            state, StartEventStep(42, _manifest('prepare', 'publish'), None), expected_revision=0
        )
        commit_step(state, BeginCallbackStep(0), expected_revision=1)
        failed = state.callback_failed(0, failure, expected_revision=2)
        assert failed.status == 'callback-failed'
        assert failed.failure_text == 'ab\u00e9'
        assert failed.failure_byte_count == len(failure_bytes)
        assert failed.failure_digest == hashlib.sha256(failure_bytes).hexdigest()
        assert failed.failure_ordinal == 0
        with pytest.raises(InvalidTransitionError):
            commit_step(state, BeginCallbackStep(1), expected_revision=3)
        assert state.snapshot() == failed


def test_completion_rereads_source_fence_without_group_revision_change(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path, ordering='strict') as state:
        revision = _seed_completion_case(state, 'success')
        connection = _database_connection(state)
        # Exercise completion's reads in an earlier transaction, then discard its writes.
        _completion_observation(connection, revision)
        before = state.snapshot()
        with closing(sqlite3.connect(path)) as source, source:
            seed_pending_range(source, 'new-fence', 41, 44)
        assert state.snapshot().revision == before.revision
        with pytest.raises(StrictObligationBlockedError) as caught:
            state.callback_done(0, expected_revision=revision)
        assert str(caught.value) == 'strict delivery is blocked by an unresolved obligation'
        assert state.snapshot().callback_marks == before.callback_marks
        assert state.snapshot().cursor == before.cursor


def test_eager_late_delivery_requires_reservation_and_never_rewinds_cursor(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        state.advance_irrelevant(45, expected_revision=state.snapshot().revision)
        seed_pending_range(_database_connection(state))
        before = state.snapshot()
        with pytest.raises(InvalidTransitionError):
            commit_step(
                state,
                StartEventStep(43, _manifest('publish'), 'range-1'),
                expected_revision=state.snapshot().revision,
            )
        assert state.snapshot() == before
        commit_step(
            state, ReserveLateStep('range-1', 43), expected_revision=state.snapshot().revision
        )
        commit_step(
            state,
            StartEventStep(43, _manifest('publish'), 'range-1'),
            expected_revision=state.snapshot().revision,
        )
        commit_step(state, BeginCallbackStep(0), expected_revision=state.snapshot().revision)
        finished = state.callback_done(0, expected_revision=state.snapshot().revision)
        assert finished.cursor == 45
        assert _database_connection(state).execute(
            """
            SELECT terminal_state FROM late_ids
            WHERE group_name = 'editorial' AND obligation_id = 'range-1' AND event_id = 43
            """
        ).fetchone() == ('done',)


def test_equal_low_obligations_use_deterministic_fence_order(tmp_path: Path) -> None:
    wider = _obligation(obligation_id='range-z-wide', low_id=42, high_id=50)
    tied_later = _obligation(obligation_id='range-z-tied', low_id=42, high_id=44)
    earliest = _obligation(obligation_id='range-a-tied', low_id=42, high_id=44)
    with _open_group(tmp_path / 'editorial.db', ordering='strict') as state:
        for obligation in (wider, tied_later, earliest):
            seed_pending_range(
                _database_connection(state),
                obligation.obligation_id,
                obligation.low_id,
                obligation.high_id,
            )
        commit_step(
            state,
            ReserveLateStep(wider.obligation_id, 43),
            expected_revision=state.snapshot().revision,
        )
        commit_step(
            state,
            ReserveLateStep(tied_later.obligation_id, 43),
            expected_revision=state.snapshot().revision,
        )
        reserved = commit_step(
            state,
            ReserveLateStep(earliest.obligation_id, 43),
            expected_revision=state.snapshot().revision,
        )

        for obligation_id in (wider.obligation_id, tied_later.obligation_id):
            with pytest.raises(InvalidTransitionError):
                commit_step(
                    state,
                    StartEventStep(43, _manifest('publish'), obligation_id),
                    expected_revision=reserved.revision,
                )
            assert state.snapshot() == reserved

        started = commit_step(
            state,
            StartEventStep(43, _manifest('publish'), earliest.obligation_id),
            expected_revision=reserved.revision,
        )
        assert started.in_flight_obligation_id == earliest.obligation_id


def test_event_starts_before_callbacks_and_final_done_advances_atomically(
    tmp_path: Path,
) -> None:
    manifest = _manifest('prepare', 'publish')
    with _open_group(tmp_path / 'editorial.db') as state:
        started = commit_step(state, StartEventStep(42, manifest, None), expected_revision=0)
        assert started.revision == 1
        assert started.cursor == 41
        assert started.in_flight_event_id == 42
        assert started.manifest == manifest
        assert started.callback_marks == (
            (0, 'publish', 'prepare', 'pending', 0),
            (1, 'publish', 'publish', 'pending', 0),
        )
        first_attempt = commit_step(state, BeginCallbackStep(0), expected_revision=1)
        assert first_attempt.callback_marks[0][-1] == 1
        first_done = state.callback_done(0, expected_revision=2)
        assert first_done.cursor == 41
        assert first_done.callback_marks[0][3] == 'done'
        commit_step(state, BeginCallbackStep(1), expected_revision=3)
        finished = state.callback_done(1, expected_revision=4)
        assert finished.revision == 5
        assert finished.cursor == 42
        assert finished.in_flight_event_id is None
        assert finished.manifest is None
        assert finished.callback_marks == ()
        assert _database_connection(state).execute(
            'SELECT revision, cursor, in_flight_event_id FROM group_state'
            " WHERE group_name = 'editorial' "
        ).fetchone() == (5, 42, None)


def test_final_callback_waits_for_new_lower_obligation(tmp_path: Path) -> None:
    higher = _obligation(obligation_id='range-higher', low_id=46, high_id=48)
    lower = _obligation(obligation_id='range-lower')
    with _open_group(tmp_path / 'editorial.db', ordering='strict') as state:
        seed_pending_range(
            _database_connection(state), higher.obligation_id, higher.low_id, higher.high_id
        )
        commit_step(
            state, ReserveLateStep('range-higher', 46), expected_revision=state.snapshot().revision
        )
        commit_step(
            state,
            StartEventStep(46, _manifest('publish'), 'range-higher'),
            expected_revision=state.snapshot().revision,
        )
        commit_step(state, BeginCallbackStep(0), expected_revision=state.snapshot().revision)
        seed_pending_range(
            _database_connection(state), lower.obligation_id, lower.low_id, lower.high_id
        )
        blocked = state.snapshot()

        with pytest.raises(InvalidTransitionError):
            state.callback_done(0, expected_revision=blocked.revision)

        assert state.snapshot() == blocked
        assert blocked.cursor == 41
        assert blocked.callback_marks == ((0, 'publish', 'publish', 'pending', 1),)
        assert _database_connection(state).execute(
            """
            SELECT terminal_state FROM late_ids
            WHERE group_name = 'editorial' AND obligation_id = 'range-higher' AND event_id = 46
            """
        ).fetchone() == ('pending',)


def test_higher_late_obligation_waits_for_lower_unresolved_obligation(
    tmp_path: Path,
) -> None:
    lower = _obligation(obligation_id='range-lower')
    higher = _obligation(obligation_id='range-higher', low_id=46, high_id=48)
    with _open_group(tmp_path / 'editorial.db', ordering='strict') as state:
        for obligation in (lower, higher):
            seed_pending_range(
                _database_connection(state),
                obligation.obligation_id,
                obligation.low_id,
                obligation.high_id,
            )
        reserved = commit_step(
            state, ReserveLateStep('range-higher', 46), expected_revision=state.snapshot().revision
        )

        with pytest.raises(InvalidTransitionError):
            commit_step(
                state,
                StartEventStep(46, _manifest('publish'), 'range-higher'),
                expected_revision=reserved.revision,
            )

        assert state.snapshot() == reserved
        resolved_revision = _resolve_obligation(state, lower)
        started = commit_step(
            state,
            StartEventStep(46, _manifest('publish'), 'range-higher'),
            expected_revision=resolved_revision,
        )
        assert started.in_flight_event_id == 46
        assert started.in_flight_obligation_id == 'range-higher'


def test_late_final_callback_refreshes_operational_late_id_count(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        seed_pending_range(_database_connection(state))
        reserved = commit_step(
            state, ReserveLateStep('range-1', 42), expected_revision=state.snapshot().revision
        )
        connection = _database_connection(state)
        assert reserved.late_id_count == 1
        _assert_operational_count(connection)
        commit_step(
            state,
            StartEventStep(42, _manifest('publish'), 'range-1'),
            expected_revision=state.snapshot().revision,
        )
        commit_step(state, BeginCallbackStep(0), expected_revision=state.snapshot().revision)

        connection.execute(
            "INSERT INTO late_ids VALUES ('editorial', 'range-1', 43, 'capacity-pending', 1)"
        )
        finished = state.callback_done(0, expected_revision=state.snapshot().revision)

        assert connection.execute(
            "SELECT terminal_state FROM late_ids WHERE group_name = 'editorial' AND event_id = 43"
        ).fetchone() == ('capacity-pending',)
        assert finished.late_id_count == 0
        _assert_operational_count(connection)


@pytest.mark.parametrize('low_id', [42, 45])
def test_live_irrelevant_prefix_is_atomic_and_keeps_direct_rejection(
    tmp_path: Path, low_id: int
) -> None:
    with _open_group(tmp_path / 'source.db') as state:
        connection = _database_connection(state)
        connection.execute("UPDATE groups SET ordering_mode='strict'")
        insert_range(connection, 'fence', low_id, low_id, state='closed')
        bind_group(connection, 'fence', 'editorial', 7, floor=41, eligibility_point=0)
        before = state.snapshot()
        advanced = state.advance_irrelevant_prefix(50, expected_revision=before.revision)
        assert advanced.cursor == low_id - 1
        assert advanced.revision == before.revision + (low_id > 42)
        with pytest.raises(InvalidTransitionError):
            state.advance_irrelevant(50, expected_revision=advanced.revision)
        assert state.snapshot() == advanced


def test_manifest_shape_and_size_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    original = _manifest('publish')
    changed = _manifest('prepare')
    with _open_group(path, max_manifest_bytes=len(original.to_json().encode())) as state:
        assert state.verify_manifest(changed, expected_revision=0).revision == 0
        commit_step(state, StartEventStep(42, original, None), expected_revision=0)
        mismatch = state.verify_manifest(changed, expected_revision=1)
        assert mismatch.status == 'shape-mismatch'
        restored = state.verify_manifest(original, expected_revision=2)
        assert restored.status == 'running'
        with pytest.raises(InvalidTransitionError):
            state.restart_event(
                _manifest('publish-expanded'),
                reason='restart changed manifest',
                identity=AuditIdentity.current(),
                wall_clock_time=1000.0,
                expected_revision=3,
            )
        assert state.snapshot() == restored
    database_bytes = path.read_bytes()
    assert original.to_json().encode() in database_bytes


def test_next_callback_waits_for_new_lower_obligation(tmp_path: Path) -> None:
    higher = _obligation(obligation_id='range-higher', low_id=46, high_id=48)
    lower = _obligation(obligation_id='range-lower')
    with _open_group(tmp_path / 'editorial.db', ordering='strict') as state:
        seed_pending_range(
            _database_connection(state), higher.obligation_id, higher.low_id, higher.high_id
        )
        commit_step(
            state, ReserveLateStep('range-higher', 46), expected_revision=state.snapshot().revision
        )
        commit_step(
            state,
            StartEventStep(46, _manifest('prepare', 'publish'), 'range-higher'),
            expected_revision=state.snapshot().revision,
        )
        commit_step(state, BeginCallbackStep(0), expected_revision=state.snapshot().revision)
        state.callback_done(0, expected_revision=state.snapshot().revision)
        seed_pending_range(
            _database_connection(state), lower.obligation_id, lower.low_id, lower.high_id
        )
        blocked = state.snapshot()

        with pytest.raises(InvalidTransitionError):
            commit_step(state, BeginCallbackStep(1), expected_revision=blocked.revision)

        assert state.snapshot() == blocked
        assert blocked.callback_marks == (
            (0, 'publish', 'prepare', 'done', 1),
            (1, 'publish', 'publish', 'pending', 0),
        )


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
@pytest.mark.parametrize('boundary', ['snapshot', 'step'])
def test_ordinary_callback_done_builds_one_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordering: Literal['eager', 'strict'],
    boundary: str,
) -> None:
    with _open_group(tmp_path / 'editorial.db', ordering=ordering) as state:
        revision = _seed_completion_case(state, 'success')
        constructor = GroupSnapshot
        counted = Mock(wraps=constructor)
        for module in tuple(sys.modules.values()):
            for name, value in tuple(vars(module).items()):
                if value is constructor:
                    monkeypatch.setattr(module, name, counted)
        if boundary == 'snapshot':
            completed = state.callback_done(0, expected_revision=revision)
            assert completed.cursor == 42
            assert completed.revision == revision + 1
            assert counted.call_count == 1
        else:
            connection = _database_connection(state)
            connection.execute('BEGIN IMMEDIATE')
            try:
                assert delivery.callback_done_step(connection, 'editorial', 0, revision) == 3
                assert counted.call_count == 0
                assert connection.execute(
                    'SELECT revision, cursor, last_delivered_id, in_flight_event_id '
                    'FROM group_state'
                ).fetchone() == (3, 42, 42, None)
            finally:
                connection.rollback()


@pytest.mark.parametrize('change', ['manifest', 'recovery'])
@pytest.mark.parametrize('boundary', ['rollback', 'savepoint', 'commit', 'control', 'generation'])
def test_completion_state_does_not_survive_transaction_boundaries(
    tmp_path: Path, change: str, boundary: str
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        commit_step(
            state, StartEventStep(42, _manifest('prepare', 'publish'), None), expected_revision=0
        )
        commit_step(state, BeginCallbackStep(0), expected_revision=1)
        connection = _database_connection(state)
        connection.execute('BEGIN IMMEDIATE')
        connection.execute('SAVEPOINT completion')
        delivery.callback_done(connection, 'editorial', 0, 2)
        if boundary == 'rollback':
            connection.rollback()
            ordinal, revision = 0, 2
        elif boundary == 'savepoint':
            connection.execute('ROLLBACK TO completion')
            ordinal, revision = 0, 2
        else:
            connection.commit()
            ordinal = 1
            revision = commit_step(state, BeginCallbackStep(ordinal), expected_revision=3).revision
            if boundary == 'control':
                revision = state.park(
                    expected_revision=revision,
                    reason='test completion after control',
                    identity=AuditIdentity.current(),
                    wall_clock_time=1.0,
                ).revision
            elif boundary == 'generation':
                revision = state.callback_done(ordinal, expected_revision=revision).revision
                revision = state.park(
                    expected_revision=revision,
                    reason='park for generation replacement',
                    identity=AuditIdentity.current(),
                    wall_clock_time=1.0,
                ).revision
                reset = dispose_at_frontier(
                    state,
                    'replay',
                    disposition_id='completion-replay',
                    new_generation=8,
                    source_frontier=43,
                    from_event_id=42,
                    expected_revision=revision,
                    reason='replace completion generation',
                    identity=AuditIdentity.current(),
                    wall_clock_time=1.0,
                )
                reset = state.unpark(
                    expected_revision=reset.revision,
                    reason='resume replacement generation',
                    identity=AuditIdentity.current(),
                    wall_clock_time=2.0,
                )
                started = commit_step(
                    state,
                    StartEventStep(42, _manifest('publish'), None),
                    expected_revision=reset.revision,
                )
                ordinal = 0
                revision = commit_step(
                    state, BeginCallbackStep(ordinal), expected_revision=started.revision
                ).revision

        def change_current_state() -> None:
            if change == 'manifest':
                connection.execute("UPDATE group_state SET manifest_json = 'invalid'")
            else:
                connection.execute('UPDATE group_state SET shape_mismatch = 1')

        change_current_state()
        if not connection.in_transaction:
            connection.execute('BEGIN IMMEDIATE')
        error_type = ValueError if change == 'manifest' else InvalidTransitionError
        message = (
            'invalid callback manifest JSON'
            if change == 'manifest'
            else 'callback completion is blocked by recovery evidence'
        )
        try:
            with pytest.raises(error_type) as caught:
                delivery.callback_done(connection, 'editorial', ordinal, revision)
            assert str(caught.value) == message
        finally:
            connection.rollback()


def test_required_event_blocks_all_ordinary_progress_until_exact_retry(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        required = state.require_event(42, expected_revision=0)
        assert required.in_flight_event_id is None
        assert required.manifest is None
        assert required.required_event_id == 42
        assert required.status == 'event-unavailable'
        assert {'event-unavailable', 'required-event'} <= set(required.conditions)
        with pytest.raises(InvalidTransitionError):
            state.advance_irrelevant(43, expected_revision=1)
        with pytest.raises(InvalidTransitionError):
            commit_step(state, StartEventStep(43, _manifest('publish'), None), expected_revision=1)
        with pytest.raises(InvalidTransitionError):
            state.retry(
                event_id=43,
                callback_ordinal=None,
                reason='required event refetch',
                identity=AuditIdentity.current(),
                wall_clock_time=1000.0,
                expected_revision=1,
            )
        assert state.snapshot() == required
        retrying = state.retry(
            event_id=42,
            callback_ordinal=None,
            reason='required event refetch',
            identity=AuditIdentity.current(),
            wall_clock_time=1000.0,
            expected_revision=1,
        )
        assert retrying.retry_event_id == 42
        with pytest.raises(InvalidTransitionError):
            state.advance_irrelevant(43, expected_revision=2)
        restored = state.restore_required_event(42, expected_revision=2)
        assert restored.status == 'running'
        assert restored.required_event_id is None
        assert (
            commit_step(
                state, StartEventStep(42, _manifest('publish'), None), expected_revision=3
            ).revision
            == 4
        )


def test_required_event_independently_blocks_in_flight_callback_completion(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        commit_step(state, StartEventStep(42, _manifest('publish'), None), expected_revision=0)
        commit_step(state, BeginCallbackStep(0), expected_revision=1)
        required = state.require_event(42, expected_revision=2)
        assert required.in_flight_event_id == 42
        assert required.required_event_id == 42
        with pytest.raises(InvalidTransitionError):
            state.callback_done(0, expected_revision=3)
        assert state.snapshot() == required
        state.retry(
            event_id=42,
            callback_ordinal=None,
            reason='required event refetch',
            identity=AuditIdentity.current(),
            wall_clock_time=1000.0,
            expected_revision=3,
        )
        state.restore_required_event(42, expected_revision=4)
        finished = state.callback_done(0, expected_revision=5)
        assert finished.cursor == 42


def test_strict_forward_progress_stops_at_unresolved_obligation_bound(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db', ordering='strict') as state:
        seed_pending_range(_database_connection(state))
        accepted = state.snapshot()
        with pytest.raises(InvalidTransitionError):
            state.advance_irrelevant(45, expected_revision=state.snapshot().revision)
        assert state.snapshot() == accepted


def test_strict_ordinary_event_stops_at_unresolved_obligation_bound(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db', ordering='strict') as state:
        seed_pending_range(_database_connection(state))
        accepted = state.snapshot()

        with pytest.raises(InvalidTransitionError):
            commit_step(
                state,
                StartEventStep(42, _manifest('publish'), None),
                expected_revision=state.snapshot().revision,
            )

        assert state.snapshot() == accepted


def test_terminal_bookkeeping_leaves_commit_revision_and_audit_to_caller(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        seed_pending_range(_database_connection(state))
        commit_step(
            state, ReserveLateStep('range-1', 42), expected_revision=state.snapshot().revision
        )
        commit_step(
            state,
            StartEventStep(42, _manifest('publish'), 'range-1'),
            expected_revision=state.snapshot().revision,
        )
        attempted = commit_step(
            state, BeginCallbackStep(0), expected_revision=state.snapshot().revision
        )
        connection = _database_connection(state)
        connection.execute('BEGIN IMMEDIATE')
        try:
            connection.execute(
                "UPDATE callback_marks SET outcome = 'done' WHERE group_name = 'editorial' "
            )
            finish_terminal_event(connection, 'editorial')
            assert connection.in_transaction
            assert state.snapshot().revision == attempted.revision
            assert state.snapshot().in_flight_event_id is None
            assert state.snapshot().late_id_count == 0
            assert tuple(row.command for row in audit_records(state)) == ('bootstrap',)
        finally:
            connection.rollback()
        assert state.snapshot() == attempted
        assert connection.execute(
            "SELECT terminal_state FROM late_ids WHERE group_name = 'editorial' "
        ).fetchone() == ('pending',)
        _assert_operational_count(connection)
    with _open_group(path) as reopened:
        assert reopened.snapshot() == attempted


def test_terminal_late_start_has_a_distinct_duplicate_outcome_after_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        seed_pending_range(_database_connection(state))
        commit_step(
            state, ReserveLateStep('range-1', 42), expected_revision=state.snapshot().revision
        )
        completed = commit_step(
            state, CompleteLateStep('range-1', 42), expected_revision=state.snapshot().revision
        )

    with _open_group(path) as state:
        with pytest.raises(TerminalLateEventError):
            commit_step(
                state,
                StartEventStep(42, _manifest('publish'), 'range-1'),
                expected_revision=completed.revision,
            )
        assert state.snapshot() == completed
