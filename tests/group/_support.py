from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack

from sg_event_daemon._group_recovery import DispositionRequest
from tests._storage_support import bind_group, insert_range

if TYPE_CHECKING:
    import sqlite3

    from sg_event_daemon._group_state import GroupState
    from sg_event_daemon._group_types import AuditIdentity, GroupSnapshot, PreInvocationStep


@dataclass(frozen=True)
class _AuditRecord:
    site: str
    group: str
    event_id: int | None
    plugin_id: str | None
    callback_id: str | None
    command: str
    wall_clock_time: float
    prior_revision: int
    new_revision: int
    reason: str
    operator_label: str | None
    local_uid: int
    username: str
    pid: int
    disposition_id: str | None
    command_fingerprint: str | None
    terminal_result: str | None


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


def audit_records(state: GroupState) -> tuple[_AuditRecord, ...]:
    connection = vars(state)['_database'].connection
    return tuple(
        _AuditRecord(
            site=str(row[0]),
            group=str(row[1]),
            event_id=None if row[2] is None else int(row[2]),
            plugin_id=None if row[3] is None else str(row[3]),
            callback_id=None if row[4] is None else str(row[4]),
            command=str(row[5]),
            wall_clock_time=float(row[6]),
            prior_revision=int(row[7]),
            new_revision=int(row[8]),
            reason=str(row[9]),
            operator_label=None if row[10] is None else str(row[10]),
            local_uid=int(row[11]),
            username=str(row[12]),
            pid=int(row[13]),
            disposition_id=None if row[14] is None else str(row[14]),
            command_fingerprint=None if row[15] is None else str(row[15]),
            terminal_result=None if row[16] is None else str(row[16]),
        )
        for row in connection.execute(
            """
            SELECT site, group_name, event_id, plugin_id, callback_id,
                   command, wall_clock_time, prior_revision, new_revision,
                   reason, operator_label, local_uid, username, pid,
                   disposition_id, command_fingerprint, terminal_result
            FROM audit
            WHERE group_name = ?
            ORDER BY audit_id
            """,
            (state.snapshot().group,),
        ).fetchall()
    )


def commit_step(
    state: GroupState, step: PreInvocationStep, *, expected_revision: int
) -> GroupSnapshot:
    result = state.commit_pre_invocation((step,), expected_revision=expected_revision)
    if result.rejection is not None:
        raise result.rejection
    return result.snapshot


def operator_records(state: GroupState) -> tuple[_AuditRecord, ...]:
    return tuple(record for record in audit_records(state) if record.command != 'bootstrap')


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
