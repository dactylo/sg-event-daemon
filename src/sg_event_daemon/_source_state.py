"""Sole lock, connection, and transaction authority for one shared source."""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from itertools import pairwise
from math import isfinite
from typing import TYPE_CHECKING, NotRequired, TypedDict, Unpack

from . import _source_obligations as obligations
from . import _source_store as store
from ._config import (
    DEFAULT_COMMIT_BATCH_MAX_BYTES,
    DEFAULT_MAX_GROUP_OBLIGATIONS,
    DEFAULT_MAX_IPC_FRAME_BYTES,
    DEFAULT_MAX_OPEN_RANGES,
    DEFAULT_MAX_PENDING_GROUP_OBLIGATIONS,
    DEFAULT_MAX_RANGE_QUERY_PAGES,
    DEFAULT_MAX_SOURCE_LATE_IDS,
    DEFAULT_SQLITE_SYNCHRONOUS,
    DEFAULT_VISIBILITY_OVERLAP_SECS,
)
from ._group_store import snapshot as group_snapshot
from ._platform import FileLock, acquire_lock
from ._resources import close_resources
from ._source_schema import (
    SOURCE_SCHEMA,
    UNRESOLVED_LATE_ID_COUNT_SQL,
    UNRESOLVED_LATE_ID_PREFIX_SQL,
    source_identity,
)
from ._source_store import (
    DiscoveryRange,
    SourceCapacityError,
    SourceIdentityError,
    SourcePollState,
    SourceSnapshot,
)
from ._sqlite import (
    DatabaseError,
    DatabaseHandle,
    database_family_size,
    open_database,
    read_transaction,
)
from ._state_paths import source_lock_path

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Generator, Iterable, Sequence
    from pathlib import Path
    from types import TracebackType
    from typing import Self

    from ._group_types import GroupSnapshot
    from ._write_lock import WriteContext


@dataclass(frozen=True)
class PendingGroupRecord:
    """Capture durable lane-less group detail on the source owner thread."""

    snapshot: GroupSnapshot
    audit_count: int
    park_reason: str | None
    late_id_count: int
    late_ids: tuple[int, ...]


@dataclass(frozen=True)
class DurableGroupPolicy:
    """Project file-owned identity and strict restart policy for one group."""

    group: str
    generation: int
    delivery_floor: int
    holdback_secs: float
    last_delivered_id: int
    halted: bool
    is_operator_replay: bool
    active: bool


class _IndexChangedError(Exception):
    """Abort an admission transaction whose preflight authority changed."""


@dataclass(frozen=True)
class SourceCommitResult:
    """Report whether one committed transaction changed a range.

    Attributes:
        range_changed: Whether a range opened, merged, or first closed.
    """

    range_changed: bool


@dataclass(frozen=True)
class StreamAdoptionReservation:
    """Describe one durable source-stream adoption reservation.

    `recorded` is true only when the matching adoption is the currently
    initialized, running stream. Retained audit history alone does not set it.
    """

    adoption_id: str
    command_fingerprint: str
    recorded: bool


class _AdoptStreamOptions(TypedDict):
    """Type one audited stream-adoption request."""

    adoption_id: str
    command_fingerprint: str
    new_stream_fingerprint: str
    reason: str
    groups_parked: bool


class _SourceOpenOptions(TypedDict):
    """Type one complete source-state open request."""

    writes: WriteContext
    site: str
    source: str
    stream_fingerprint: str
    source_generation: str
    synchronous: NotRequired[str]
    create: NotRequired[bool]
    visibility_overlap_secs: NotRequired[float]
    max_group_obligations: NotRequired[int]
    max_open_ranges: NotRequired[int]
    max_range_query_pages: NotRequired[int]
    max_pending_group_obligations: NotRequired[int]
    max_source_late_ids: NotRequired[int]
    max_ipc_frame_bytes: NotRequired[int]
    commit_batch_max_bytes: NotRequired[int]
    token_factory: NotRequired[Callable[[], str] | None]


class SourceState:
    """Own one locked SQLite authority for a shared source stream.

    The instance owns both database handles and the source-owner lock until
    `close()` or context exit. All other operations require that open lifetime.
    """

    def __init__(
        self,
        database: DatabaseHandle,
        handoff: DatabaseHandle,
        lock: FileLock,
        settings: store.SourceSettings,
        options: _SourceOpenOptions,
    ) -> None:
        """Bind open source database handles, the owner lock, settings, and token source.

        Args:
            database: Primary source-state database handle.
            handoff: FULL-synchronous handoff database handle.
            lock: Held source-owner lock released by `close()`.
            settings: Validated immutable source identity and policy settings.
            options: Opening options supplying the owned write context and
                optional non-reusable command token factory.
        """
        self._database = database
        self._handoff = handoff
        self._lock = lock
        self._writes = options['writes']
        self._settings = settings
        self._observed_revisions: tuple[int, int] | None = None
        self._strict_range_generation = 1
        self._strict_range_projection_generation = 0
        token_factory = options.get('token_factory')
        self._token_factory = _default_token if token_factory is None else token_factory

    def __enter__(self) -> Self:
        """Return this open source state.

        Returns:
            This authority while its source-owner lock remains held.
        """
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close connections and release the source-owner lock."""
        self.close()

    def _check_index_revision(self, expected: int) -> None:
        """Reject preflight authority changed by another writer before BEGIN IMMEDIATE."""
        row = self._database.connection.execute(
            'SELECT index_revision FROM source_meta'
        ).fetchone()
        if int(row[0]) != expected:
            raise _IndexChangedError

    def _clear_capacity_condition(self, connection: sqlite3.Connection) -> None:
        """Clear a capacity halt only after the next cycle establishes relief."""
        row = connection.execute('SELECT condition FROM source_meta').fetchone()
        if row is not None and row[0] in store.CAPACITY_CONDITIONS:
            return
        store.clear_capacity_condition(connection)

    def _new_token(self) -> str:
        """Return one validated non-reusable source command token."""
        token = self._token_factory()
        if type(token) is not str or not token:
            raise ValueError('source token factory returned an invalid token')
        return token

    def _observation_gaps(
        self,
        event_ids: Sequence[int],
        current: int,
    ) -> tuple[tuple[int, int], ...]:
        """Return uncovered gaps introduced by one ascending observation."""
        gaps: list[tuple[int, int]] = []
        for event_id in event_ids:
            if event_id <= current:
                continue
            if event_id > current + 1:
                gaps.extend(
                    store.uncovered_intervals(
                        self._database.connection,
                        current + 1,
                        event_id - 1,
                    )
                )
            current = event_id
        return tuple(gaps)

    @classmethod
    def _open_owned(
        cls,
        path: Path,
        **options: Unpack[_SourceOpenOptions],
    ) -> Self:
        """Open validated resources under the facade ownership boundary."""
        writes = options['writes']
        settings = store.SourceSettings(
            site=options['site'],
            source=options['source'],
            configured_fingerprint=options['stream_fingerprint'],
            source_generation=options['source_generation'],
            visibility_overlap_secs=options.get(
                'visibility_overlap_secs', DEFAULT_VISIBILITY_OVERLAP_SECS
            ),
            max_open_ranges=options.get('max_open_ranges', DEFAULT_MAX_OPEN_RANGES),
            max_group_obligations=options.get(
                'max_group_obligations', DEFAULT_MAX_GROUP_OBLIGATIONS
            ),
            max_range_query_pages=options.get(
                'max_range_query_pages', DEFAULT_MAX_RANGE_QUERY_PAGES
            ),
            max_pending_group_obligations=options.get(
                'max_pending_group_obligations',
                DEFAULT_MAX_PENDING_GROUP_OBLIGATIONS,
            ),
            max_source_late_ids=options.get('max_source_late_ids', DEFAULT_MAX_SOURCE_LATE_IDS),
            max_ipc_frame_bytes=options.get('max_ipc_frame_bytes', DEFAULT_MAX_IPC_FRAME_BYTES),
            commit_batch_max_bytes=options.get(
                'commit_batch_max_bytes', DEFAULT_COMMIT_BATCH_MAX_BYTES
            ),
        )
        numeric_limits = (
            settings.max_open_ranges,
            settings.max_range_query_pages,
            settings.max_pending_group_obligations,
            settings.max_source_late_ids,
            settings.max_ipc_frame_bytes,
            settings.commit_batch_max_bytes,
        )
        if any(limit <= 0 for limit in numeric_limits):
            raise ValueError('source limits must be positive')
        if not isfinite(settings.visibility_overlap_secs) or settings.visibility_overlap_secs <= 0:
            raise ValueError('visibility_overlap_secs must be finite and positive')
        lock = acquire_lock(source_lock_path(path))
        handoff: DatabaseHandle | None = None
        database: DatabaseHandle | None = None
        identity = source_identity(settings.site, settings.source)
        try:
            handoff = open_database(
                path,
                identity,
                SOURCE_SCHEMA,
                create=options.get('create', False),
                write_transaction=writes.transaction,
                synchronous='FULL',
            )
            database = open_database(
                path,
                identity,
                SOURCE_SCHEMA,
                create=options.get('create', False),
                write_transaction=writes.transaction,
                synchronous=options.get('synchronous', DEFAULT_SQLITE_SYNCHRONOUS.upper()),
            )
            connection = (
                handoff.connection
                if handoff.connection.execute('SELECT 1 FROM source_meta').fetchone() is None
                else database.connection
            )
            with writes.transaction(connection):
                store.initialize_meta(
                    connection,
                    site=settings.site,
                    source=settings.source,
                    stream_fingerprint=settings.configured_fingerprint,
                    source_generation=settings.source_generation,
                )
                store.prune_unbound_ranges(connection)
            return cls(database, handoff, lock, settings, options)
        except BaseException as error:
            close_resources((database, handoff, lock), primary_error=error)
            raise

    def _poll_state(self) -> SourcePollState:
        """Read the source fields needed by a poller without acquiring writer authority."""
        return store.poll_state(self._database.connection)

    def _require_pending_capacity(self) -> None:
        """Reject a transaction whose aggregate obligations exceed policy."""
        if (
            obligations.pending_count(self._database.connection, open_only=True)
            > self._settings.max_pending_group_obligations
        ):
            raise SourceCapacityError(
                'obligation-capacity-exhausted', code='obligation-capacity-exhausted'
            )

    def _set_condition(self, condition: str) -> None:
        """Persist one halt independently of a rolled-back observation."""
        with self._writes.transaction(self._database.connection):
            store.set_condition(self._database.connection, condition)

    def _preflight_range(
        self,
        high_id: int,
    ) -> set[tuple[str, int]] | None:
        """Return marker coverage, or None when no group needs the range."""
        try:
            with read_transaction(self._database.connection):
                eligible = tuple(
                    (row[0], row[1])
                    for row in store.eligible_groups(
                        self._database.connection,
                        high_id,
                    )
                )
                covered = obligations.check_range_capacity(
                    self._database.connection,
                    self._settings,
                    eligible,
                )
                return covered if any(group not in covered for group in eligible) else None
        except SourceCapacityError:
            self._set_condition('obligation-capacity-exhausted')
            raise

    def _set_capacity_condition(self, error: SourceCapacityError) -> None:
        """Persist the typed reason without interpreting diagnostic text."""
        if error.code is None:
            raise error
        self._set_condition(error.code)

    def audit_count(self) -> int:
        """Return the cumulative source-owner audit count.

        Returns:
            Disposition and stream-adoption audit rows.
        """
        return store.disposition_audit_count(
            self._database.connection
        ) + obligations.adoption_count(self._database.connection)

    def begin_range_scan(
        self,
        obligation_id: str,
        *,
        successful_clock: float,
    ) -> DiscoveryRange:
        """Begin or resume one bounded inclusive recheck cycle.

        Args:
            obligation_id: Exact retained range.
            successful_clock: Current source successful-query clock.

        Returns:
            Updated range scan position.

        Raises:
            ValueError: The obligation does not identify a retained open range.
        """
        with self._writes.transaction(self._database.connection):
            store.begin_range_scan(
                self._database.connection,
                self._settings,
                obligation_id,
                successful_clock=successful_clock,
            )
        current = store.range_for(self._database.connection, obligation_id)
        if current is None:
            raise RuntimeError('range disappeared after scan progress committed')
        return current

    @property
    def can_query(self) -> bool:
        """Return whether stream identity permits SG queries.

        Returns:
            `True` unless a configured stream mismatch halts this source.
        """
        return store.can_query(
            self._database.connection,
            self._settings.configured_fingerprint,
        )

    def close(self) -> None:
        """Close both database connections and release the source-owner lock."""
        close_resources((self._database, self._handoff, self._lock, self._writes.lock))

    def database_size_bytes(self) -> int:
        """Return current bytes used by this source SQLite family.

        Returns:
            Main database, WAL, and SHM bytes currently present.
        """
        return database_family_size(self._database.path)

    def due_range_id(self) -> str | None:
        """Return the first due active discovery range identity.

        Returns:
            One due range in stable order, or `None`.
        """
        return store.due_range_id(self._database.connection)

    def group_snapshot(self, group: str) -> GroupSnapshot:
        """Read one group's durable state without acquiring its delivery owner lock."""
        with read_transaction(self._database.connection):
            return group_snapshot(self._database.connection, group)

    def has_terminal_disposition(
        self,
        group: str,
        disposition_id: str,
        command_fingerprint: str,
        terminal_result: str,
    ) -> bool:
        """Return whether one exact terminal source disposition is audited.

        Args:
            group: Stable group identity.
            disposition_id: Stable disposition identity.
            command_fingerprint: Exact command fingerprint.
            terminal_result: Exact audited terminal result.

        Returns:
            Whether the exact terminal proof exists.

        Raises:
            SourceIdentityError: The audit identity conflicts or its terminal
                proof is incomplete.
        """
        return obligations.has_terminal_disposition(
            self._database.connection,
            group=group,
            disposition_id=disposition_id,
            command_fingerprint=command_fingerprint,
            terminal_result=terminal_result,
        )

    def invalidate_strict_ranges(self) -> None:
        """Invalidate the process-local strict range projection."""
        self._strict_range_generation += 1

    def _close_range(self, obligation_id: str, *, successful_clock: float) -> SourceCommitResult:
        """Close a range and publish its revision in one configured-level transaction."""
        with self._writes.transaction(self._database.connection):
            changed = store.stage_close_range(
                self._database.connection,
                obligation_id,
                successful_clock=successful_clock,
            )
        if changed:
            self.invalidate_strict_ranges()
        return SourceCommitResult(range_changed=changed)

    def begin_cycle(self) -> SourcePollState:
        """Refresh durable authority and retire ranges once at a polling boundary."""
        connection = self._database.connection
        with self._writes.transaction(connection):
            store.prune_unbound_ranges(connection)
            rows = connection.execute(
                "SELECT COUNT(*) FROM discovery_ranges WHERE range_state = 'open'"
            ).fetchone()[0]
            late = connection.execute('SELECT COUNT(*) FROM source_late_ids').fetchone()[0]
            if (
                rows < self._settings.max_open_ranges
                and obligations.pending_count(connection, open_only=True)
                < self._settings.max_pending_group_obligations
                and late < self._settings.max_source_late_ids
            ):
                store.clear_capacity_condition(connection)
            revisions = tuple(
                connection.execute(
                    'SELECT index_revision, range_revision FROM source_meta'
                ).fetchone()
            )
            if revisions != self._observed_revisions:
                self._observed_revisions = revisions
                self.invalidate_strict_ranges()
            return store.poll_state(connection)

    def commit_observations(
        self,
        event_ids: Iterable[int],
        *,
        successful_clock: float,
    ) -> SourceCommitResult:
        """Commit observed IDs with every group's obligation rows atomically.

        Args:
            event_ids: Strictly ascending positive EventLogEntry IDs.
            successful_clock: Finite monotonic successful-query clock.

        Raises:
            DatabaseError: Eight consecutive external index changes prevent
                admission.
            SourceCapacityError: The observation cannot fit without losing
                evidence.
            ValueError: IDs or the successful clock are invalid.
        """
        ids = tuple(event_ids)
        if any(type(event_id) is not int or event_id <= 0 for event_id in ids):
            raise ValueError('observed event ids must be positive integers')
        if any(left >= right for left, right in pairwise(ids)):
            raise ValueError('observed event ids must be strictly ascending')
        if not isfinite(successful_clock) or successful_clock < 0:
            raise ValueError('successful clock must be finite and nonnegative')
        for _attempt in range(8):
            connection = self._database.connection
            index_revision = int(
                connection.execute('SELECT index_revision FROM source_meta').fetchone()[0]
            )
            current = int(store.meta(connection)[3])
            gaps = self._observation_gaps(ids, current)
            coverage = {
                bounds: covered
                for bounds in gaps
                if (covered := self._preflight_range(bounds[1])) is not None
            }
            try:
                with self._writes.transaction(connection):
                    self._check_index_revision(index_revision)
                    for (low_id, high_id), covered in coverage.items():
                        mutation = store.stage_open_range(
                            connection,
                            self._settings,
                            low_id=low_id,
                            high_id=high_id,
                            successful_clock=successful_clock,
                            new_token=self._new_token,
                        )
                        obligations.replace_range_revision(
                            connection, self._settings, mutation, covered
                        )
                    self._require_pending_capacity()
                    store.validate_successful_clock(connection, successful_clock)
                    connection.execute(
                        (
                            'UPDATE source_meta SET frontier = ?, successful_clock = '
                            '? WHERE singleton = 1'
                        ),
                        (max(current, ids[-1]) if ids else current, successful_clock),
                    )
            except _IndexChangedError:
                self.invalidate_strict_ranges()
                continue
            except SourceCapacityError as exc:
                self._set_capacity_condition(exc)
                raise
            if coverage:
                self.invalidate_strict_ranges()
            return SourceCommitResult(range_changed=bool(coverage))
        raise DatabaseError('source index changed during eight consecutive observation attempts')

    @contextmanager
    def operator_writes(self) -> Generator[None]:
        """Use the bounded retryable policy for writes on this owner thread."""
        previous = self._writes
        self._writes = previous.for_operator()
        try:
            yield
        finally:
            self._writes = previous

    @classmethod
    def open(
        cls,
        path: Path,
        **options: Unpack[_SourceOpenOptions],
    ) -> Self:
        """Open or initialize one locked body-free source database.

        Args:
            path: SQLite path beneath the secure daemon state directory.
            options: Source identity, bounded policies, and optional token
                source. `synchronous` selects `NORMAL` (default) or `FULL` for
                the primary connection; handoffs always use `FULL`. `create`
                defaults to false; only the source owner may enable it to
                initialize a missing database. The facade consumes
                `writes.lock` on entry, closing it on failure or with the
                state.

        Returns:
            An open source-state authority holding its owner lock.

        Raises:
            AlreadyLockedError: Another owner holds the source lock.
            SecurityError: A state or lock path cannot be securely opened or
                fails filesystem safety checks.
            DatabaseError: Database opening, policy, initialization, or
                validation fails, including an unsupported schema version.
            SourceIdentityError: Stored site or source identity conflicts with
                the configured source.
            ValueError: A source limit is not positive or the visibility
                overlap is not finite and positive.
        """
        try:
            return cls._open_owned(path, **options)
        except BaseException as error:
            close_resources((options['writes'].lock,), primary_error=error)
            raise

    def group_policies(
        self, *, after_revision: int | None = None
    ) -> tuple[int, tuple[DurableGroupPolicy, ...]]:
        """Read one consistent index revision and its durable group policies.

        Args:
            after_revision: Last observed index revision. `None` forces a full
                policy read; equality with the current revision suppresses the
                policy payload.

        Returns:
            The current index revision and policy tuple. An unchanged revision
            returns an empty tuple, so compare revisions to distinguish that
            case from a full read containing no policies.
        """
        connection = self._database.connection
        with read_transaction(connection):
            revision = int(
                connection.execute('SELECT index_revision FROM source_meta').fetchone()[0]
            )
            if revision == after_revision:
                return revision, ()
            rows = connection.execute(
                """SELECT group_name, group_generation, delivery_floor, holdback_secs,
                last_delivered_id, parked OR orphaned OR stream_identity_changed
                OR ordering_changed
                OR history_truncated_floor IS NOT NULL OR replay_required_from IS NOT NULL
                OR shape_mismatch OR required_event_id IS NOT NULL OR failure_code IS NOT NULL
                OR ordering_anomaly_id IS NOT NULL OR strict_capacity_exhausted
                OR EXISTS (SELECT 1 FROM late_ids WHERE late_ids.group_name = groups.group_name
                AND terminal_state = 'capacity-pending'),
                is_operator_replay, active FROM groups JOIN group_state USING (group_name)"""
            ).fetchall()
            return revision, tuple(
                DurableGroupPolicy(
                    str(row[0]),
                    int(row[1]),
                    int(row[2]),
                    float(row[3]),
                    int(row[4]),
                    bool(row[5]),
                    bool(row[6]),
                    bool(row[7]),
                )
                for row in rows
            )

    def pending_group_record(self, group: str, *, limit: int) -> PendingGroupRecord:
        """Read consistent lane-less status and bounded recovery detail.

        Args:
            group: Exact configured pending group name.
            limit: Maximum distinct unresolved late IDs to return.
        """
        connection = self._database.connection
        with read_transaction(connection):
            current = group_snapshot(connection, group)
            audit_count = int(
                connection.execute(
                    'SELECT COUNT(*) FROM audit WHERE group_name = ?', (group,)
                ).fetchone()[0]
            )
            reason = connection.execute(
                'SELECT reason FROM audit WHERE group_name = ? AND command IN '
                "('park', 'conditional-park', 'orphan', 'readd') ORDER BY audit_id DESC LIMIT 1",
                (group,),
            ).fetchone()
            count = int(
                connection.execute(
                    UNRESOLVED_LATE_ID_COUNT_SQL,
                    (group,),
                ).fetchone()[0]
            )
            ids = tuple(
                int(row[0])
                for row in connection.execute(UNRESOLVED_LATE_ID_PREFIX_SQL, (group, limit))
            )
            return PendingGroupRecord(
                current, audit_count, None if reason is None else str(reason[0]), count, ids
            )

    def range_for(self, obligation_id: str) -> DiscoveryRange | None:
        """Return one exact retained discovery range.

        Args:
            obligation_id: Exact retained range identity.

        Returns:
            The retained range, or `None` when it is absent.
        """
        return store.range_for(self._database.connection, obligation_id)

    def record_range_scan_page(
        self,
        obligation_id: str,
        *,
        scanned_through: int,
        event_ids: Iterable[int],
        successful_clock: float,
        query_page_complete: bool = True,
    ) -> SourceCommitResult:
        """Persist one inclusive recheck page and close after a final scan.

        Args:
            obligation_id: Exact retained open range.
            scanned_through: Inclusive greatest id covered by this page.
            event_ids: Rows this page found within the range.
            successful_clock: Current successful-query clock.
            query_page_complete: Whether this is the final commit chunk for one
                provider query page.

        Returns:
            Whether range evidence or the exact close predicate changed.

        Raises:
            ValueError: The range is not open, progress is outside its current
                inclusive bounds, or event ids or successful clock are invalid.
            TypeError: `query_page_complete` is not a Boolean.
            SourceCapacityError: Late-id capacity would lose required evidence.
        """
        ids = tuple(event_ids)
        try:
            with self._writes.transaction(self._database.connection):
                store.record_late_ids(
                    self._database.connection,
                    self._settings,
                    obligation_id,
                    ids,
                    successful_clock=successful_clock,
                )
                final_scan = store.record_range_scan_page(
                    self._database.connection,
                    self._settings,
                    obligation_id,
                    store.RangeScanProgress(
                        scanned_through=scanned_through,
                        query_page_complete=query_page_complete,
                    ),
                    successful_clock=successful_clock,
                )
                self._clear_capacity_condition(self._database.connection)
        except SourceCapacityError as exc:
            self._set_capacity_condition(exc)
            raise
        if ids:
            self.invalidate_strict_ranges()
        if final_scan:
            return self._close_range(
                obligation_id,
                successful_clock=successful_clock,
            )
        return SourceCommitResult(range_changed=bool(ids))

    def record_successful_query(self, successful_clock: float) -> None:
        """Persist a monotonic successful-query clock checkpoint.

        Args:
            successful_clock: Finite nonnegative process clock excluding
                failed-query outage time; it must not precede the durable
                checkpoint.

        Raises:
            ValueError: The clock is invalid or moves backward.
        """
        with self._writes.transaction(self._database.connection):
            store.record_successful_query(
                self._database.connection,
                successful_clock,
            )
            self._clear_capacity_condition(self._database.connection)

    def replay_required_groups(self) -> tuple[tuple[str, int], ...]:
        """Read configured generations whose durable marker prevents release."""
        return tuple(
            (str(row[0]), int(row[1]))
            for row in self._database.connection.execute(
                """SELECT rm.group_name, rm.group_generation
                FROM replay_markers AS rm
                JOIN groups AS sg USING (group_name, group_generation)
                WHERE sg.active = 1"""
            )
        )

    def snapshot(self) -> SourceSnapshot:
        """Return one immutable body-free source snapshot.

        Returns:
            Current frontier, ranges, obligations, lifecycle, and capacity
            state.
        """
        connection = self._database.connection
        return store.snapshot(
            connection,
            self._settings,
            pending_obligations=obligations.pending_count(connection),
            replay_markers=obligations.replay_markers(
                connection,
                self._settings,
            ),
            adoption_count=obligations.adoption_count(connection),
        )

    def _validate_adoption(self, options: _AdoptStreamOptions) -> bool:
        """Reject invalid authority or recognize the current receipt without writes."""
        adoption_id = options['adoption_id']
        fingerprint = options['command_fingerprint']
        target = options['new_stream_fingerprint']
        reason = options['reason']
        connection = self._handoff.connection
        existing = obligations.existing_adoption(connection, adoption_id)
        current = self.snapshot()
        if existing is not None:
            if existing != (fingerprint, target, reason):
                raise SourceIdentityError(
                    'stream adoption id was reused', code='adoption-id-reused'
                )
            if current.stream_incarnation != adoption_id or current.stream_fingerprint != target:
                raise SourceIdentityError(
                    'stream adoption id is not current', code='adoption-id-not-current'
                )
            return True
        if target != self._settings.configured_fingerprint:
            raise SourceIdentityError(
                'adoption target is not the configured stream', code='adoption-target-mismatch'
            )
        if current.status != 'stream-identity-changed':
            raise SourceIdentityError(
                'source adoption requires stream-identity-changed halt',
                code='adoption-requires-identity-halt',
            )
        if not options['groups_parked']:
            raise SourceIdentityError(
                'stream adoption requires parked groups', code='adoption-requires-parked-groups'
            )
        pending = obligations.pending_adoption(connection, adoption_id)
        if pending != (
            fingerprint,
            current.stream_incarnation,
            current.stream_fingerprint,
            target,
            reason,
        ):
            raise SourceIdentityError(
                'stream adoption reservation is invalid', code='adoption-reservation-invalid'
            )
        return False

    def adopt_stream(
        self,
        **options: Unpack[_AdoptStreamOptions],
    ) -> SourceSnapshot:
        """FULL-commit audited stream adoption and old evidence retirement.

        A fresh adoption requires the configured target, a current
        `stream-identity-changed` halt, parked groups, and its matching durable
        reservation; it returns an uninitialized replacement-stream snapshot.
        An exact retry of the current recorded adoption preserves and returns
        the current state, which may already be initialized. Receipt and
        authority checks reject stale requests before a write transaction;
        fresh work rechecks that authority after acquiring the write lock.

        Args:
            options: Audited adoption identity, target, reason, and supervisor
                quiescence evidence.

        Returns:
            Fresh uninitialized state or the preserved exact-retry state.

        Raises:
            SourceIdentityError: Quiescing or command identity is invalid.
        """
        adoption_id = options['adoption_id']
        command_fingerprint = options['command_fingerprint']
        new_stream_fingerprint = options['new_stream_fingerprint']
        reason = options['reason']
        if not adoption_id or not command_fingerprint or not reason:
            raise SourceIdentityError(
                'stream adoption identity and reason are required',
                code='adoption-identity-required',
            )
        if self._validate_adoption(options):
            return self.snapshot()
        with self._writes.transaction(self._handoff.connection):
            if not self._validate_adoption(options):
                counts = obligations.adoption_counts(self._handoff.connection)
                obligations.retire_stream_evidence(
                    self._handoff.connection,
                )
                store.reset_for_adoption(
                    self._handoff.connection,
                    stream_fingerprint=new_stream_fingerprint,
                    incarnation=adoption_id,
                )
                obligations.insert_adoption(
                    self._handoff.connection,
                    adoption_id=adoption_id,
                    command_fingerprint=command_fingerprint,
                    stream_fingerprint=new_stream_fingerprint,
                    reason=reason,
                    retired_counts=counts,
                )
                obligations.retire_adoption_reservation(
                    self._handoff.connection,
                    adoption_id,
                )
        self.invalidate_strict_ranges()
        return self.snapshot()

    def initialize(
        self,
        latest_id: int | None,
        *,
        incarnation: str | None = None,
    ) -> SourceSnapshot:
        """FULL-commit first-start frontier, marker, and incarnation.

        The first initialization requires an incarnation when none is already
        retained. On an initialized source, valid initialization values are a
        no-op and do not replace the persisted frontier or incarnation.

        Args:
            latest_id: Latest visible id, or `None` for an empty source.
            incarnation: Initial non-reusable stream incarnation.

        Returns:
            Initialized immutable snapshot.

        Raises:
            SourceIdentityError: Configured stream identity is mismatched.
            ValueError: The latest id is invalid or first initialization lacks
                an incarnation.
        """
        if not self.can_query:
            raise SourceIdentityError('stream-identity-changed', code='stream-identity-changed')
        with self._writes.transaction(self._handoff.connection):
            store.initialize(
                self._handoff.connection,
                latest_id,
                incarnation=incarnation,
            )
        return self.snapshot()

    def reserve_stream_adoption(
        self,
        *,
        command_fingerprint: str,
        reason: str,
    ) -> StreamAdoptionReservation:
        """FULL-commit or replay one qualifying halted-stream reservation.

        Args:
            command_fingerprint: Stable semantic operator command identity.
            reason: Required operator audit reason.

        Returns:
            Fresh reservation, exact pending retry, or recorded completion.

        Raises:
            SourceIdentityError: Command identity or reason is missing or
                conflicts with a pending reservation, no initialized prior
                incarnation exists, or new work lacks a stream-identity halt.
        """
        if not command_fingerprint or not reason:
            raise SourceIdentityError(
                'stream adoption identity and reason are required',
                code='adoption-identity-required',
            )
        snapshot = self.snapshot()
        incarnation = snapshot.stream_incarnation
        if incarnation is not None and snapshot.status != 'stream-identity-changed':
            completed = obligations.existing_adoption(
                self._database.connection,
                incarnation,
            )
            if completed == (
                command_fingerprint,
                self._settings.configured_fingerprint,
                reason,
            ):
                return StreamAdoptionReservation(
                    adoption_id=incarnation,
                    command_fingerprint=command_fingerprint,
                    recorded=snapshot.initialized and snapshot.status == 'running',
                )
        if snapshot.status != 'stream-identity-changed':
            raise SourceIdentityError(
                'source adoption requires stream-identity-changed halt',
                code='adoption-requires-identity-halt',
            )
        if not snapshot.initialized or incarnation is None:
            raise SourceIdentityError(
                'stream adoption requires an initialized prior incarnation',
                code='adoption-requires-prior-incarnation',
            )
        with self._writes.transaction(self._handoff.connection):
            pending = obligations.pending_adoption_for_halt(
                self._handoff.connection,
                command_fingerprint=command_fingerprint,
                prior_incarnation=incarnation,
                prior_stream_fingerprint=snapshot.stream_fingerprint,
                target_stream_fingerprint=self._settings.configured_fingerprint,
            )
            if pending is not None:
                if pending[1] != reason:
                    raise SourceIdentityError(
                        'stream adoption command identity conflicts with its reason',
                        code='adoption-command-conflict',
                    )
                adoption_id = pending[0]
            else:
                adoption_id = self._new_token()
                obligations.reserve_adoption(
                    self._handoff.connection,
                    adoption_id=adoption_id,
                    command_fingerprint=command_fingerprint,
                    prior_incarnation=incarnation,
                    prior_stream_fingerprint=snapshot.stream_fingerprint,
                    target_stream_fingerprint=self._settings.configured_fingerprint,
                    reason=reason,
                )
        return StreamAdoptionReservation(
            adoption_id=adoption_id,
            command_fingerprint=command_fingerprint,
            recorded=False,
        )

    def strict_ranges_if_changed(self) -> tuple[DiscoveryRange, ...] | None:
        """Return the strict range projection once per dirty generation.

        Returns:
            Complete ranges after a visible mutation, or `None` while clean.
        """
        generation = self._strict_range_generation
        if generation == self._strict_range_projection_generation:
            return None
        projection = store.ranges(self._database.connection)
        self._strict_range_projection_generation = generation
        return projection

    def successful_clock(self) -> float:
        """Return the durable successful-query clock.

        Returns:
            Current monotonic successful-query clock.
        """
        return store.poll_state(self._database.connection).successful_clock


def _default_token() -> str:
    """Return one random non-reusable source command identity."""
    return uuid.uuid4().hex
