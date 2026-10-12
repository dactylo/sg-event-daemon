from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Literal, cast

import pytest

from sg_event_daemon._group_delivery import finish_terminal_event
from sg_event_daemon._group_errors import InvalidTransitionError, TerminalLateEventError
from sg_event_daemon._group_state import PendingRange
from sg_event_daemon._group_types import (
    AuditIdentity,
)
from sg_event_daemon._plugins import CallbackIdentity, CallbackManifest
from tests._storage_support import close_test_range, open_test_group
from tests.group._support import assert_operational_count as _assert_operational_count
from tests.group._support import seed_pending_range

if TYPE_CHECKING:
    from pathlib import Path
    from sqlite3 import Connection

    from sg_event_daemon._group_state import GroupState


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
        late_ids=(),
    )


def _resolve_obligation(
    state: GroupState,
    obligation: PendingRange,
) -> int:
    connection = _database_connection(state)
    close_test_range(connection, obligation.obligation_id)
    resolved = state.complete_obligation_scan(obligation.obligation_id)
    return resolved.revision


def test_callback_failure_stops_later_callbacks_and_preserves_failure_evidence(
    tmp_path: Path,
) -> None:
    failure = 'ab\u00e9\U0001f642tail'
    failure_bytes = failure.encode()
    with _open_group(tmp_path / 'editorial.db', max_failure_text_bytes=5) as state:
        state.start_event(42, _manifest('prepare', 'publish'), expected_revision=0)
        state.begin_callback(0, expected_revision=1)
        failed = state.callback_failed(0, failure, expected_revision=2)
        assert failed.status == 'callback-failed'
        assert failed.failure_text == 'ab\u00e9'
        assert failed.failure_byte_count == len(failure_bytes)
        assert failed.failure_digest == hashlib.sha256(failure_bytes).hexdigest()
        assert failed.failure_ordinal == 0
        with pytest.raises(InvalidTransitionError):
            state.begin_callback(1, expected_revision=3)
        assert state.snapshot() == failed


def test_eager_late_delivery_requires_reservation_and_never_rewinds_cursor(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        state.advance_irrelevant(45, expected_revision=state.snapshot().revision)
        seed_pending_range(_database_connection(state))
        before = state.snapshot()
        with pytest.raises(InvalidTransitionError):
            state.start_event(
                43,
                _manifest('publish'),
                expected_revision=state.snapshot().revision,
                obligation_id='range-1',
            )
        assert state.snapshot() == before
        state.reserve_late_event('range-1', 43, expected_revision=state.snapshot().revision)
        state.start_event(
            43,
            _manifest('publish'),
            expected_revision=state.snapshot().revision,
            obligation_id='range-1',
        )
        state.begin_callback(0, expected_revision=state.snapshot().revision)
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
        state.reserve_late_event(
            wider.obligation_id, 43, expected_revision=state.snapshot().revision
        )
        state.reserve_late_event(
            tied_later.obligation_id, 43, expected_revision=state.snapshot().revision
        )
        reserved = state.reserve_late_event(
            earliest.obligation_id, 43, expected_revision=state.snapshot().revision
        )

        for obligation_id in (wider.obligation_id, tied_later.obligation_id):
            with pytest.raises(InvalidTransitionError):
                state.start_event(
                    43,
                    _manifest('publish'),
                    expected_revision=reserved.revision,
                    obligation_id=obligation_id,
                )
            assert state.snapshot() == reserved

        started = state.start_event(
            43,
            _manifest('publish'),
            expected_revision=reserved.revision,
            obligation_id=earliest.obligation_id,
        )
        assert started.in_flight_obligation_id == earliest.obligation_id


def test_event_starts_before_callbacks_and_final_done_advances_atomically(
    tmp_path: Path,
) -> None:
    manifest = _manifest('prepare', 'publish')
    with _open_group(tmp_path / 'editorial.db') as state:
        started = state.start_event(42, manifest, expected_revision=0)
        assert started.revision == 1
        assert started.cursor == 41
        assert started.in_flight_event_id == 42
        assert started.manifest == manifest
        assert started.callback_marks == (
            (0, 'publish', 'prepare', 'pending', 0),
            (1, 'publish', 'publish', 'pending', 0),
        )
        first_attempt = state.begin_callback(0, expected_revision=1)
        assert first_attempt.callback_marks[0][-1] == 1
        first_done = state.callback_done(0, expected_revision=2)
        assert first_done.cursor == 41
        assert first_done.callback_marks[0][3] == 'done'
        state.begin_callback(1, expected_revision=3)
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
        state.reserve_late_event('range-higher', 46, expected_revision=state.snapshot().revision)
        state.start_event(
            46,
            _manifest('publish'),
            expected_revision=state.snapshot().revision,
            obligation_id='range-higher',
        )
        state.begin_callback(0, expected_revision=state.snapshot().revision)
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
        reserved = state.reserve_late_event(
            'range-higher', 46, expected_revision=state.snapshot().revision
        )

        with pytest.raises(InvalidTransitionError):
            state.start_event(
                46,
                _manifest('publish'),
                expected_revision=reserved.revision,
                obligation_id='range-higher',
            )

        assert state.snapshot() == reserved
        resolved_revision = _resolve_obligation(state, lower)
        started = state.start_event(
            46,
            _manifest('publish'),
            expected_revision=resolved_revision,
            obligation_id='range-higher',
        )
        assert started.in_flight_event_id == 46
        assert started.in_flight_obligation_id == 'range-higher'


def test_late_final_callback_refreshes_operational_late_id_count(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        seed_pending_range(_database_connection(state))
        reserved = state.reserve_late_event(
            'range-1', 42, expected_revision=state.snapshot().revision
        )
        connection = _database_connection(state)
        assert reserved.late_id_count == 1
        _assert_operational_count(connection)
        state.start_event(
            42,
            _manifest('publish'),
            expected_revision=state.snapshot().revision,
            obligation_id='range-1',
        )
        state.begin_callback(0, expected_revision=state.snapshot().revision)

        connection.execute(
            "INSERT INTO late_ids VALUES ('editorial', 'range-1', 43, 'capacity-pending', 1)"
        )
        finished = state.callback_done(0, expected_revision=state.snapshot().revision)

        assert connection.execute(
            "SELECT terminal_state FROM late_ids WHERE group_name = 'editorial' AND event_id = 43"
        ).fetchone() == ('capacity-pending',)
        assert finished.late_id_count == 0
        _assert_operational_count(connection)


def test_manifest_shape_and_size_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / 'editorial.db'
    original = _manifest('publish')
    changed = _manifest('prepare')
    with _open_group(path, max_manifest_bytes=len(original.to_json().encode())) as state:
        assert state.verify_manifest(changed, expected_revision=0).revision == 0
        state.start_event(42, original, expected_revision=0)
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
        state.reserve_late_event('range-higher', 46, expected_revision=state.snapshot().revision)
        state.start_event(
            46,
            _manifest('prepare', 'publish'),
            expected_revision=state.snapshot().revision,
            obligation_id='range-higher',
        )
        state.begin_callback(0, expected_revision=state.snapshot().revision)
        state.callback_done(0, expected_revision=state.snapshot().revision)
        seed_pending_range(
            _database_connection(state), lower.obligation_id, lower.low_id, lower.high_id
        )
        blocked = state.snapshot()

        with pytest.raises(InvalidTransitionError):
            state.begin_callback(1, expected_revision=blocked.revision)

        assert state.snapshot() == blocked
        assert blocked.callback_marks == (
            (0, 'publish', 'prepare', 'done', 1),
            (1, 'publish', 'publish', 'pending', 0),
        )


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
            state.start_event(43, _manifest('publish'), expected_revision=1)
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
        assert state.start_event(42, _manifest('publish'), expected_revision=3).revision == 4


def test_required_event_independently_blocks_in_flight_callback_completion(
    tmp_path: Path,
) -> None:
    with _open_group(tmp_path / 'editorial.db') as state:
        state.start_event(42, _manifest('publish'), expected_revision=0)
        state.begin_callback(0, expected_revision=1)
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
            state.start_event(
                42, _manifest('publish'), expected_revision=state.snapshot().revision
            )

        assert state.snapshot() == accepted


def test_terminal_bookkeeping_leaves_commit_revision_and_audit_to_caller(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'editorial.db'
    with _open_group(path) as state:
        seed_pending_range(_database_connection(state))
        state.reserve_late_event('range-1', 42, expected_revision=state.snapshot().revision)
        state.start_event(
            42,
            _manifest('publish'),
            expected_revision=state.snapshot().revision,
            obligation_id='range-1',
        )
        attempted = state.begin_callback(0, expected_revision=state.snapshot().revision)
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
            assert tuple(row.command for row in state._audit_records()) == ('bootstrap',)
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
        state.reserve_late_event('range-1', 42, expected_revision=state.snapshot().revision)
        completed = state.complete_late_id(
            'range-1', 42, expected_revision=state.snapshot().revision
        )

    with _open_group(path) as state:
        with pytest.raises(TerminalLateEventError):
            state.start_event(
                42,
                _manifest('publish'),
                obligation_id='range-1',
                expected_revision=completed.revision,
            )
        assert state.snapshot() == completed
