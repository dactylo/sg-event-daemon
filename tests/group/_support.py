from __future__ import annotations

from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack

from sg_event_daemon._group_recovery import DispositionRequest
from tests._storage_support import bind_group, insert_range

if TYPE_CHECKING:
    import sqlite3

    from sg_event_daemon._group_state import GroupState
    from sg_event_daemon._group_types import AuditIdentity, AuditRecord, GroupSnapshot


def _expected_operational_late_id_keys(connection: sqlite3.Connection) -> set[tuple[str, int]]:
    return {
        (obligation_id, event_id)
        for obligation_id, event_id, terminal_state in connection.execute(
            'SELECT obligation_id, event_id, terminal_state FROM late_ids'
            " WHERE group_name = 'editorial' "
        )
        if terminal_state == 'pending'
    }


def assert_operational_count(connection: sqlite3.Connection) -> set[tuple[str, int]]:
    stored = connection.execute(
        "SELECT operational_late_id_count FROM group_state WHERE group_name = 'editorial' "
    ).fetchone()[0]
    expected = _expected_operational_late_id_keys(connection)
    assert stored == len(expected)
    return expected


def operator_records(state: GroupState) -> tuple[AuditRecord, ...]:
    return tuple(record for record in state._audit_records() if record.command != 'bootstrap')


def seed_pending_range(
    connection: sqlite3.Connection,
    obligation_id: str = 'range-1',
    low_id: int = 42,
    high_id: int = 44,
    *,
    group: str = 'editorial',
) -> None:
    insert_range(connection, obligation_id, low_id, high_id)
    bind_group(
        connection,
        obligation_id,
        group,
        7,
        floor=41,
        eligibility_point=30.0,
    )


class _DispositionOptions(TypedDict):
    disposition_id: str
    new_generation: int
    source_frontier: int
    expected_revision: int
    reason: str
    identity: AuditIdentity
    wall_clock_time: float
    from_event_id: NotRequired[int | None]
    acknowledge_truncation: NotRequired[bool]
    operator_label: NotRequired[str | None]


def dispose_at_frontier(
    state: GroupState,
    command: Literal['replay', 'reset-to-latest'],
    **options: Unpack[_DispositionOptions],
) -> GroupSnapshot:
    vars(state)['_database'].connection.execute(
        'UPDATE source_meta SET frontier = ?',
        (options['source_frontier'],),
    )
    return state.dispose(
        DispositionRequest(
            command=command,
            group=state.snapshot().group,
            disposition_id=options['disposition_id'],
            reserved_generation=options['new_generation'],
            expected_revision=options['expected_revision'],
            from_event_id=options.get('from_event_id'),
            acknowledge_truncation=options.get('acknowledge_truncation', False),
            reason=options['reason'],
            identity=options['identity'],
            wall_clock_time=options['wall_clock_time'],
            operator_label=options.get('operator_label'),
        )
    )
