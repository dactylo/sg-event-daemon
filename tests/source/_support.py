from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict, Unpack

from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._source_state import SourceState
from tests._storage_support import open_test_group, write_context_for

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from sg_event_daemon._group_state import GroupState


SITE = 'studio'
SOURCE = 'events'
SOURCE_GENERATION = 'source-spawn-1'
STREAM = stream_fingerprint(
    'https://studio.example.invalid',
    'admin',
    'event-reader',
)


class SourceOpenOptions(TypedDict, total=False):
    commit_batch_max_bytes: int
    configured_stream: str
    delivery_floors: Mapping[str, int] | None
    group_holdbacks: Mapping[str, float] | None
    groups: Mapping[str, int] | None
    max_ipc_frame_bytes: int
    max_group_obligations: int
    max_open_ranges: int
    max_pending_group_obligations: int
    max_range_query_pages: int
    max_source_late_ids: int
    source_generation: str
    synchronous: str
    token_factory: Callable[[], str] | None
    visibility_overlap_secs: float


def adopt_halted_stream(
    source: SourceState,
    *,
    command_fingerprint: str,
    reason: str,
) -> None:
    connection = vars(source)['_database'].connection
    connection.execute(
        """
        UPDATE source_meta
        SET condition = 'stream-identity-changed'
        WHERE singleton = 1
        """
    )
    reservation = source.reserve_stream_adoption(
        command_fingerprint=command_fingerprint,
        reason=reason,
    )
    source.adopt_stream(
        adoption_id=reservation.adoption_id,
        command_fingerprint=reservation.command_fingerprint,
        new_stream_fingerprint=STREAM,
        reason=reason,
        groups_parked=True,
    )


def open_group(
    path: Path,
    *,
    group: str = 'editorial',
    generation: int = 7,
    max_late_ids_per_group: int = 50_000,
) -> GroupState:
    return open_test_group(
        path,
        group=group,
        group_generation=generation,
        initial_position=100,
        max_late_ids_per_group=max_late_ids_per_group,
        site=SITE,
        source=SOURCE,
        stream_fingerprint=STREAM,
        source_generation=SOURCE_GENERATION,
    )


def open_source(
    path: Path,
    **options: Unpack[SourceOpenOptions],
) -> SourceState:
    groups = options.get('groups')
    delivery_floors = options.get('delivery_floors')
    group_holdbacks = options.get('group_holdbacks')
    state = SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site=SITE,
        source=SOURCE,
        stream_fingerprint=options.get('configured_stream', STREAM),
        source_generation=options.get('source_generation', SOURCE_GENERATION),
        visibility_overlap_secs=options.get('visibility_overlap_secs', 300.0),
        max_open_ranges=options.get('max_open_ranges', 100),
        max_group_obligations=options.get('max_group_obligations', 1_000),
        max_range_query_pages=options.get('max_range_query_pages', 100),
        max_pending_group_obligations=options.get('max_pending_group_obligations', 1_300),
        max_source_late_ids=options.get('max_source_late_ids', 50_000),
        max_ipc_frame_bytes=options.get('max_ipc_frame_bytes', 8_388_608),
        commit_batch_max_bytes=options.get('commit_batch_max_bytes', 67_108_864),
        token_factory=options.get('token_factory'),
        synchronous=options.get('synchronous', 'NORMAL'),
    )

    connection = vars(state)['_database'].connection
    configured = {'editorial': 7, 'render': 3} if groups is None else groups
    for group, generation in configured.items():
        floor = 0 if delivery_floors is None else delivery_floors.get(group, 0)
        holdback = 0.0 if group_holdbacks is None else group_holdbacks.get(group, 0.0)
        connection.execute(
            """INSERT OR IGNORE INTO groups VALUES (?, ?, ?, ?, ?, ?, 1, 'active')""",
            (
                group,
                generation,
                'strict' if holdback else 'eager',
                'https://studio.example.invalid',
                'incarnation-1',
                holdback,
            ),
        )
        connection.execute(
            'INSERT OR IGNORE INTO group_state '
            '(group_name, revision, cursor, delivery_floor, last_delivered_id) '
            'VALUES (?, 0, ?, ?, ?)',
            (group, floor, floor, floor),
        )
    return state
