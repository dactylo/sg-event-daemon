from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon import _source_obligations as source_obligations
from sg_event_daemon import _source_store as source_store
from sg_event_daemon._group_errors import InvalidTransitionError
from sg_event_daemon._group_recovery import DispositionRequest, LifecycleRequest
from sg_event_daemon._group_types import (
    AuditIdentity,
    snapshot_from_json,
)
from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._source_state import (
    SourceCapacityError,
    SourceIdentityError,
    SourceState,
    StreamAdoptionReservation,
)
from sg_event_daemon._sqlite import DatabaseError
from tests._storage_support import bind_group, insert_range, insert_source_late_ids, orphan_group
from tests.group.test_state import _manifest
from tests.source._support import (
    STREAM as _STREAM,
)
from tests.source._support import (
    open_group as _open_group,
)
from tests.source._support import (
    open_source as _open_source,
)
from tests.source.test_state import open_source_range

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from sg_event_daemon._group_state import _OperatorOptions


def _qualify_same_stream_halt(source: SourceState) -> None:
    database = vars(source)['_database']
    database.connection.execute(
        """
        UPDATE source_meta
        SET condition = 'stream-identity-changed'
        WHERE singleton = 1
        """
    )


def _reserve_adoption(
    source: SourceState,
    *,
    adoption_id: str,
    command_fingerprint: str,
    reason: str,
) -> StreamAdoptionReservation:
    vars(source)['_token_factory'] = lambda: adoption_id
    reservation = source.reserve_stream_adoption(
        command_fingerprint=command_fingerprint,
        reason=reason,
    )
    assert reservation.adoption_id == adoption_id
    return reservation


def _token_sequence(*tokens: str) -> Callable[[], str]:
    iterator: Iterator[str] = iter(tokens)
    return lambda: next(iterator)


def test_aggregate_capacity_counts_marker_replacing_pending_rows(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'editorial': 7},
        max_group_obligations=1,
        max_pending_group_obligations=1,
        token_factory=_token_sequence('r1', 'r2'),
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 101, successful_clock=1.0)

        open_source_range(source, 103, 103, successful_clock=2.0)

        snapshot = source.snapshot()
        assert snapshot.pending_obligations == 1
        assert snapshot.replay_markers[0].from_event_id == 101
        assert snapshot.frontier == 104


def test_aggregate_obligation_cap_halts_when_one_row_per_group_cannot_fit(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        groups={'alpha': 1, 'beta': 1},
        max_pending_group_obligations=2,
        token_factory=_token_sequence('r1', 'r2'),
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 101, successful_clock=1.0)
        before = source.snapshot()

        with pytest.raises(SourceCapacityError, match='obligation-capacity-exhausted'):
            open_source_range(source, 103, 103, successful_clock=2.0)

        after = source.snapshot()
        assert after.frontier == before.frontier
        assert after.ranges == before.ranges
        assert after.status == 'obligation-capacity-exhausted'


def test_aggregate_obligation_cap_preserves_existing_marker_coverage(
    tmp_path: Path,
) -> None:
    with _open_source(
        tmp_path / 'source.db',
        groups={'alpha': 1, 'beta': 1},
        max_pending_group_obligations=4,
        token_factory=_token_sequence('r1', 'r2'),
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 101, successful_clock=1.0)
        # A retained marker already covers beta's next range without a new row.
        connection = vars(source)['_database'].connection
        connection.execute(
            """
            INSERT INTO replay_markers (
                group_name, group_generation, source_generation,
                marker_id, from_event_id, command_digest
            ) VALUES ('beta', 1, 'source-spawn-1', 'marker', 101, 'digest')
            """
        )
        before = source.snapshot().replay_markers

        next_range = open_source_range(source, 103, 103, successful_clock=2.0)

        assert next_range.group_eligibility == (('alpha', 2.0),)
        assert source.snapshot().replay_markers == before
        assert source.snapshot().pending_obligations == 4


def test_capacity_condition_clears_on_the_cycle_after_relief(tmp_path: Path) -> None:
    with _open_source(
        tmp_path / 'source.db', groups={'editorial': 7}, max_open_ranges=1
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        first = open_source_range(source, 101, 101, successful_clock=1)
        with pytest.raises(SourceCapacityError, match='range-capacity'):
            open_source_range(source, 103, 103, successful_clock=2)
        source._close_range(first.obligation_id, successful_clock=2)
        assert source.snapshot().status == 'range-capacity-exhausted'
        source.begin_cycle()
        assert source.snapshot().status == 'running'


def test_changed_stream_halts_before_query_and_adoption_retires_old_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    changed = stream_fingerprint(
        'https://studio.example.invalid',
        'restricted',
        'event-reader',
    )
    with _open_source(
        path,
        token_factory=_token_sequence('range', 'marker'),
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 101, successful_clock=1.0)
        with _open_group(path, group='render', generation=3) as group:
            orphan_group(group)

    with _open_source(path, configured_stream=changed) as mismatched:
        assert mismatched.snapshot().status == 'stream-identity-changed'
        assert not mismatched.can_query
        reservation = _reserve_adoption(
            mismatched,
            adoption_id='adoption-1',
            command_fingerprint='adopt-command-1',
            reason='approved visibility change',
        )

        adopted = mismatched.adopt_stream(
            adoption_id=reservation.adoption_id,
            command_fingerprint=reservation.command_fingerprint,
            new_stream_fingerprint=changed,
            reason='approved visibility change',
            groups_parked=True,
        )
        assert adopted.status == 'uninitialized'
        assert adopted.stream_fingerprint == changed
        assert not adopted.initialized
        assert adopted.frontier == 0
        assert adopted.stream_incarnation == 'adoption-1'
        assert adopted.ranges == ()
        assert adopted.pending_obligations == 0
        assert adopted.replay_markers == ()
        assert adopted.adoption_count == 1
        assert vars(mismatched)['_database'].connection.execute(
            'SELECT retired_range_count, retired_late_id_count, retired_marker_count '
            'FROM stream_adoptions '
            "WHERE adoption_id = 'adoption-1'"
        ).fetchone() == (1, 0, 1)

        repeated = mismatched.adopt_stream(
            adoption_id=reservation.adoption_id,
            command_fingerprint=reservation.command_fingerprint,
            new_stream_fingerprint=changed,
            reason='approved visibility change',
            groups_parked=True,
        )
        assert repeated == adopted
        initialized = mismatched.initialize(200)
        assert initialized.frontier == 200
        assert initialized.stream_incarnation == 'adoption-1'

    with _open_source(path, configured_stream=changed) as restarted:
        assert restarted.snapshot().frontier == 200
        assert restarted.snapshot().ranges == ()


def test_collapse_publishes_halt_and_revision_in_one_transaction(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with (
        _open_source(path, max_group_obligations=1) as source,
        _open_group(path) as group,
        _open_group(path, group='render', generation=3) as other,
    ):
        source.initialize(100, incarnation='incarnation-1')
        first = open_source_range(source, 101, 103, successful_clock=1)
        connection = vars(source)['_database'].connection
        connection.executemany(
            """
            INSERT INTO late_ids (group_name, obligation_id, event_id,
            terminal_state) VALUES (?, ?, ?, ?)
            """,
            [
                ('editorial', first.obligation_id, 101, 'pending'),
                ('editorial', first.obligation_id, 102, 'done'),
                ('editorial', first.obligation_id, 103, 'capacity-pending'),
                ('render', first.obligation_id, 101, 'pending'),
            ],
        )
        connection.execute('UPDATE group_state SET operational_late_id_count = 1')
        before = group.snapshot()
        other_before = other.snapshot()
        # Keep the second group below the cap by satisfying its first binding.
        connection.execute(
            'UPDATE range_groups SET obligation_state = '
            "'scan-complete' WHERE group_name = 'render'"
        )
        connection.execute("""
            CREATE TRIGGER reject_partial_collapse BEFORE UPDATE ON group_state WHEN
            NEW.group_name = 'editorial' AND NEW.replay_required_from IS NOT NULL AND
            (NEW.revision != OLD.revision + 1 OR NEW.operational_late_id_count != 0
            OR EXISTS (SELECT 1 FROM late_ids WHERE group_name = 'editorial') OR NOT
            EXISTS (SELECT 1 FROM replay_markers WHERE group_name = 'editorial' AND
            from_event_id = 101)) BEGIN SELECT RAISE(ABORT, 'partial collapse'); END
            """)
        source._close_range(first.obligation_id, successful_clock=2)
        open_source_range(source, 105, 105, successful_clock=3)
        after = group.snapshot()
        assert after.status == 'replay-required'
        assert after.replay_required_from == 101
        assert after.revision == before.revision + 1
        assert after.late_id_count == 0
        assert other.snapshot().revision == other_before.revision
        assert connection.execute(
            "SELECT event_id, terminal_state FROM late_ids WHERE group_name = 'render'"
        ).fetchall() == [(101, 'pending')]
        assert source.snapshot().pending_obligations == 2


@pytest.mark.parametrize(
    ('case', 'allowed'),
    [
        ('covered', True),
        ('late-start', False),
        ('missing-marker', False),
        ('wrong-generation', False),
        ('wrong-low', False),
        ('inactive', False),
        ('unparked', False),
        ('orphaned', False),
        ('changed-identity', False),
        ('reset', False),
        ('truncated-no-ack', False),
        ('unproved-ack', False),
        ('truncated-ack', True),
    ],
)
def test_collapsed_active_event_replay_has_narrow_audited_exception(
    tmp_path: Path,
    case: str,
    allowed: bool,
) -> None:
    path = tmp_path / 'source.db'
    with (
        _open_source(path, groups={'editorial': 7}, max_group_obligations=1) as source,
        _open_group(path) as group,
    ):
        source.initialize(100, incarnation='incarnation-1')
        group.start_event(101, _manifest('callback'), expected_revision=group.snapshot().revision)
        open_source_range(source, 105, 105, successful_clock=1)
        open_source_range(source, 110, 110, successful_clock=2)
        operator: _OperatorOptions = {
            'reason': 'recover collapse',
            'identity': AuditIdentity.current(),
            'wall_clock_time': 1,
        }
        if case != 'unparked':
            group.park(expected_revision=group.snapshot().revision, **operator)
        connection = source._database.connection
        statements = {
            'missing-marker': 'DELETE FROM replay_markers',
            'wrong-generation': 'UPDATE replay_markers SET group_generation=6',
            'wrong-low': 'UPDATE replay_markers SET from_event_id=104',
            'inactive': "UPDATE groups SET active=0 WHERE group_name='editorial'",
            'orphaned': "UPDATE group_state SET orphaned=1 WHERE group_name='editorial'",
            'changed-identity': (
                "UPDATE source_meta SET stream_incarnation='other' WHERE singleton=1"
            ),
        }
        if case in statements:
            connection.execute(statements[case])
        if case in {'truncated-no-ack', 'truncated-ack'}:
            group.mark_history_truncated(108, expected_revision=group.snapshot().revision)
        from_id = (
            108
            if case in {'truncated-no-ack', 'truncated-ack', 'unproved-ack'}
            else 105
            if case == 'late-start'
            else 101
        )
        before = group.snapshot()
        request = DispositionRequest(
            command='reset-to-latest' if case == 'reset' else 'replay',
            group='editorial',
            disposition_id='recover',
            reserved_generation=8,
            expected_revision=before.revision,
            from_event_id=None if case == 'reset' else from_id,
            acknowledge_truncation=case in {'truncated-ack', 'unproved-ack'},
            **operator,
        )
        if not allowed:
            with pytest.raises(InvalidTransitionError):
                group.dispose(request)
            assert group.snapshot() == before
            return
        result = group.dispose(request)
        assert result.in_flight_event_id is None
        assert result.cursor == from_id - 1
        audit = connection.execute(
            'SELECT event_id, result_snapshot_json FROM audit WHERE '
            "command='replay-collapsed-event'"
        ).fetchall()
        assert len(audit) == 1
        assert audit[0][0] == 101
        assert snapshot_from_json(audit[0][1]) == before
        assert group.dispose(request) == result
        assert connection.execute(
            "SELECT COUNT(*) FROM audit WHERE command='replay-collapsed-event'"
        ).fetchone() == (1,)


@pytest.mark.parametrize('continuous', [False, True])
def test_commit_observations_retries_after_an_index_revision_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    continuous: bool,
) -> None:
    with _open_source(tmp_path / 'source.db', groups={'editorial': 7}) as source:
        source.initialize(100, incarnation='incarnation-1')
        preflight = source._preflight_range
        connection = vars(source)['_handoff'].connection
        attempts = 0

        def changing_preflight(high_id: int) -> set[tuple[str, int]] | None:
            nonlocal attempts
            result = preflight(high_id)
            attempts += 1
            if continuous or attempts == 1:
                connection.execute('UPDATE source_meta SET index_revision = index_revision + 1')
                if not continuous:
                    connection.execute(
                        'UPDATE group_state SET delivery_floor = 200 '
                        "WHERE group_name = 'editorial'"
                    )
            return result

        monkeypatch.setattr(source, '_preflight_range', changing_preflight)
        if continuous:
            with pytest.raises(DatabaseError, match='eight consecutive'):
                source.commit_observations((102,), successful_clock=1)
            assert attempts == 8
            assert source.snapshot().frontier == 100
        else:
            source.commit_observations((102,), successful_clock=1)
            assert attempts == 2
            assert source.snapshot().frontier == 102
        assert source.snapshot().ranges == ()


def test_floor_satisfied_groups_keep_satisfied_rows(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    with _open_source(
        path,
        delivery_floors={'editorial': 200, 'render': 100},
        token_factory=_token_sequence('range'),
    ) as state:
        state.initialize(100, incarnation='incarnation-1')
        obligations = open_source_range(state, 101, 150, successful_clock=1.0)

        assert obligations.group_eligibility == (('render', 1.0),)
        assert vars(state)['_database'].connection.execute(
            'SELECT group_name, obligation_state FROM range_groups ORDER BY group_name'
        ).fetchall() == [('editorial', 'satisfied-by-floor'), ('render', 'pending')]


def test_open_merge_excludes_closed_range_with_lane_marks(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        connection = source._database.connection
        insert_range(connection, 'closed', 101, 107, state='closed')
        bind_group(connection, 'closed', 'editorial', 7, floor=100, eligibility_point=20)
        connection.execute("""
            INSERT INTO late_ids (group_name, obligation_id, event_id,
            terminal_state) VALUES ('editorial', 'closed', 101, 'done')
            """)
        with source._writes.transaction(connection):
            mutation = source_store.stage_open_range(
                connection,
                source._settings,
                low_id=103,
                high_id=105,
                successful_clock=1,
                new_token=lambda: 'unused',
            )
            source_obligations.replace_range_revision(connection, source._settings, mutation, ())
        assert source.range_for('closed') is not None
        assert connection.execute(
            'SELECT obligation_id, terminal_state FROM late_ids'
        ).fetchall() == [('closed', 'done')]
        assert len(source.snapshot().ranges) == 2


def test_open_merge_preserves_maximum_eligibility_and_source_ids(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        connection = source._database.connection
        insert_range(connection, 'first', 101, 103)
        insert_range(connection, 'second', 105, 107)
        bind_group(connection, 'first', 'editorial', 7, floor=100, eligibility_point=20)
        bind_group(connection, 'second', 'editorial', 7, floor=100, eligibility_point=40)
        insert_source_late_ids(connection, 'first', (101,))
        insert_source_late_ids(connection, 'second', (107,))
        before = connection.execute('SELECT range_revision FROM source_meta').fetchone()[0]
        with source._writes.transaction(connection):
            mutation = source_store.stage_open_range(
                connection,
                source._settings,
                low_id=103,
                high_id=105,
                successful_clock=1,
                new_token=lambda: 'unused',
            )
            source_obligations.replace_range_revision(connection, source._settings, mutation, ())
        retained = source.range_for('first')
        assert retained is not None
        assert (retained.low_id, retained.high_id, retained.late_ids) == (101, 107, (101, 107))
        assert dict(retained.group_eligibility)['editorial'] == 40
        assert source.range_for('second') is None
        assert connection.execute('SELECT range_revision FROM source_meta').fetchone()[0] > before


def test_orphan_preserves_pending_range_low_in_replay_marker(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / 'source.db'
    with (
        _open_source(
            source_path,
            groups={'editorial': 7},
            token_factory=_token_sequence('range', 'marker'),
        ) as source,
    ):
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 110, successful_clock=1.0)

        with _open_group(source_path) as group:
            orphaned = orphan_group(group)
        (marker,) = source.snapshot().replay_markers

        assert marker.from_event_id == 101
        assert orphaned.replay_required_from == 101
        assert source.snapshot().replay_markers == (marker,)


@pytest.mark.parametrize('floor', [0, 102])
def test_range_binds_every_active_group_with_its_floor_state(tmp_path: Path, floor: int) -> None:
    with _open_source(tmp_path / 'source.db', delivery_floors={'render': floor}) as source:
        source.initialize(100, incarnation='incarnation-1')
        source.commit_observations([103], successful_clock=10.0)
        rows = (
            vars(source)['_database']
            .connection.execute(
                'SELECT group_name, obligation_state FROM range_groups ORDER BY group_name'
            )
            .fetchall()
        )
        assert rows == [
            ('editorial', 'pending'),
            ('render', 'pending' if floor < 102 else 'satisfied-by-floor'),
        ]
        assert source.snapshot().pending_obligations == (2 if floor < 102 else 1)


def test_reorphan_after_new_source_generation_stores_consistent_marker(tmp_path: Path) -> None:
    source_path = tmp_path / 'source.db'
    with _open_source(source_path, groups={'editorial': 7}) as source:
        source.initialize(100, incarnation='incarnation-1')
        open_source_range(source, 101, 110, successful_clock=1.0)
        with _open_group(source_path) as group:
            orphan_group(group)
        (original,) = source.snapshot().replay_markers

    with _open_source(
        source_path, groups={'editorial': 7}, source_generation='source-spawn-2'
    ) as source:
        with _open_group(source_path) as group:
            group.change_lifecycle(
                LifecycleRequest('readd', 'orphaned', 7, 8, AuditIdentity.current(), 2.0)
            )
            orphaned = group.change_lifecycle(
                LifecycleRequest('orphan', 'readded-parked', 7, 8, AuditIdentity.current(), 3.0)
            )
        (marker,) = source.snapshot().replay_markers

        assert marker.source_generation == 'source-spawn-2'
        assert marker.digest == hashlib.sha256(marker.canonical_bytes()).hexdigest()
        assert marker.marker_id == original.marker_id
        assert marker.from_event_id == orphaned.replay_required_from == 101


def test_reset_retires_ranges_from_durable_source_frontier(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path, groups={'editorial': 7}) as source, _open_group(path) as group:
        source.initialize(100, incarnation='incarnation-1')
        source.commit_observations((111,), successful_clock=1)
        operator: _OperatorOptions = {
            'reason': 'reset test',
            'identity': AuditIdentity.current(),
            'wall_clock_time': 1,
        }
        group.park(expected_revision=group.snapshot().revision, **operator)
        reset = group.dispose(
            DispositionRequest(
                command='reset-to-latest',
                group='editorial',
                disposition_id='reset',
                reserved_generation=8,
                expected_revision=group.snapshot().revision,
                from_event_id=None,
                acknowledge_truncation=False,
                **operator,
            )
        )
        source.begin_cycle()
        assert reset.delivery_floor == 111
        assert source.range_for('range-101-110') is None
        assert group.pending_ranges() == ()


@pytest.mark.parametrize('closed', [False, True])
def test_retirement_cascades_ranges_without_pending_groups(tmp_path: Path, closed: bool) -> None:

    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        connection = vars(source)['_database'].connection
        insert_range(connection, 'range-a', 101, 102, state='closed' if closed else 'open')
        bind_group(
            connection,
            'range-a',
            'editorial',
            7,
            floor=100,
            eligibility_point=0,
            state='scan-complete',
        )
        insert_source_late_ids(connection, 'range-a', [101])
        before = connection.execute('SELECT range_revision FROM source_meta').fetchone()[0]
        source.begin_cycle()
        assert source.range_for('range-a') is None
        assert connection.execute('SELECT COUNT(*) FROM source_late_ids').fetchone()[0] == 0
        assert connection.execute('SELECT range_revision FROM source_meta').fetchone()[0] > before


def test_retirement_preserves_range_during_retained_replay_handoff(tmp_path: Path) -> None:

    path = tmp_path / 'source.db'
    with _open_source(path, groups={'editorial': 7}) as source, _open_group(path) as group:
        source.initialize(100, incarnation='incarnation-1')
        source.commit_observations([111], successful_clock=1.0)
        arguments: _OperatorOptions = {
            'reason': 'replay test',
            'identity': AuditIdentity.current(),
            'wall_clock_time': 1.0,
        }
        parked = group.park(expected_revision=group.snapshot().revision, **arguments)
        result = group.dispose(
            DispositionRequest(
                command='replay',
                group='editorial',
                disposition_id='replay-retirement',
                reserved_generation=8,
                from_event_id=101,
                expected_revision=parked.revision,
                acknowledge_truncation=False,
                **arguments,
            )
        )
        assert result.cursor == 100
        source.begin_cycle()
        assert source.range_for('range-101-110') is not None
        assert group.pending_ranges()[0].obligation_id == 'range-101-110'


def test_same_stream_adoption_requires_qualifying_halt_and_parked_groups(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'source.db'
    with _open_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')
        before = source.snapshot()

        with pytest.raises(SourceIdentityError, match='stream-identity-changed'):
            source.reserve_stream_adoption(
                command_fingerprint='same-command',
                reason='server-side permission role changed',
            )
        assert source.snapshot() == before
        _qualify_same_stream_halt(source)
        reservation = _reserve_adoption(
            source,
            adoption_id='same-1',
            command_fingerprint='same-command',
            reason='server-side permission role changed',
        )
        with pytest.raises(SourceIdentityError, match='parked groups'):
            source.adopt_stream(
                adoption_id=reservation.adoption_id,
                command_fingerprint=reservation.command_fingerprint,
                new_stream_fingerprint=_STREAM,
                reason='server-side permission role changed',
                groups_parked=False,
            )
        assert source.snapshot().status == 'stream-identity-changed'

        adopted = source.adopt_stream(
            adoption_id=reservation.adoption_id,
            command_fingerprint=reservation.command_fingerprint,
            new_stream_fingerprint=_STREAM,
            reason='server-side permission role changed',
            groups_parked=True,
        )
        assert adopted.status == 'uninitialized'
        repeated = source.adopt_stream(
            adoption_id=reservation.adoption_id,
            command_fingerprint=reservation.command_fingerprint,
            new_stream_fingerprint=_STREAM,
            reason='server-side permission role changed',
            groups_parked=False,
        )
        assert repeated == adopted


def test_scan_progress_without_new_ids_preserves_range_revision(tmp_path: Path) -> None:
    with _open_source(tmp_path / 'source.db') as source:
        source.initialize(100, incarnation='incarnation-1')
        source.commit_observations([110], successful_clock=10.0)
        connection = vars(source)['_database'].connection
        revision = connection.execute('SELECT range_revision FROM source_meta').fetchone()[0]
        source.begin_range_scan('range-101-109', successful_clock=10.0)
        source.record_range_scan_page(
            'range-101-109', scanned_through=102, event_ids=(), successful_clock=10.0
        )
        assert (
            connection.execute('SELECT range_revision FROM source_meta').fetchone()[0] == revision
        )
        source.record_range_scan_page(
            'range-101-109', scanned_through=103, event_ids=[103], successful_clock=10.0
        )
        assert (
            connection.execute('SELECT range_revision FROM source_meta').fetchone()[0] > revision
        )


def test_unrecorded_adoption_rejects_reused_id_with_changed_command(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    changed = stream_fingerprint(
        'https://new.example.invalid',
        'admin',
        'event-reader',
    )
    with _open_source(path) as source:
        source.initialize(100, incarnation='incarnation-1')
    with _open_source(path, configured_stream=changed) as source:
        reservation = _reserve_adoption(
            source,
            adoption_id='adoption-1',
            command_fingerprint='command-1',
            reason='move',
        )
        source.adopt_stream(
            adoption_id=reservation.adoption_id,
            command_fingerprint=reservation.command_fingerprint,
            new_stream_fingerprint=changed,
            reason='move',
            groups_parked=True,
        )
        with pytest.raises(SourceIdentityError, match='reused'):
            source.adopt_stream(
                adoption_id='adoption-1',
                command_fingerprint='command-2',
                new_stream_fingerprint=changed,
                reason='different',
                groups_parked=True,
            )
