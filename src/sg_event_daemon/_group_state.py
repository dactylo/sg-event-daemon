"""Sole lock, connection, and transaction authority for one group."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from math import isfinite
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack

from . import _group_delivery as delivery
from . import _group_obligations as obligations
from . import _group_recovery as recovery
from ._config import (
    DEFAULT_MAX_FAILURE_TEXT_BYTES,
    DEFAULT_MAX_GROUP_OBLIGATIONS,
    DEFAULT_MAX_LATE_IDS_PER_GROUP,
    DEFAULT_MAX_MANIFEST_BYTES,
    DEFAULT_MAX_PENDING_EPOCHS,
    DEFAULT_SQLITE_SYNCHRONOUS,
    DEFAULT_STRICT_HOLDBACK_SECS,
    MIN_PENDING_EPOCHS,
)
from ._group_errors import InvalidTransitionError
from ._group_store import check_revision, snapshot
from ._group_types import (
    AuditIdentity,
    AuditRecord,
    ConditionalParkDirective,
    ConditionalParkResult,
    GroupSnapshot,
    LiveJoinIdentity,
    StartMode,
    is_start_mode,
)
from ._platform import FileLock, acquire_lock
from ._resources import close_resources
from ._source_schema import (
    SOURCE_SCHEMA,
    UNRESOLVED_LATE_ID_COUNT_SQL,
    UNRESOLVED_LATE_ID_PREFIX_SQL,
    source_identity,
)
from ._sqlite import (
    DatabaseHandle,
    open_database,
    read_transaction,
)

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable, Generator
    from pathlib import Path
    from types import TracebackType
    from typing import Self

    from ._fanout import StrictOrderingControl
    from ._plugins import CallbackManifest
    from ._write_lock import WriteContext


class GroupOpenOptions(TypedDict):
    """Type one complete group-open request."""

    writes: WriteContext
    site: str
    site_server_url: str
    source: str
    group: str
    group_generation: int
    ordering: Literal['eager', 'strict']
    stream_fingerprint: str
    source_generation: str
    synchronous: NotRequired[str]
    verified_incarnation: str
    start_mode: StartMode
    initial_cursor: NotRequired[int]
    initial_delivery_floor: NotRequired[int]
    initial_last_delivered_id: NotRequired[int]
    max_failure_text_bytes: NotRequired[int]
    max_group_obligations: NotRequired[int]
    max_late_ids_per_group: NotRequired[int]
    max_manifest_bytes: NotRequired[int]
    max_pending_epochs: NotRequired[int]
    strict_holdback_secs: NotRequired[float]


@dataclass(frozen=True)
class PendingRange:
    """Describe one current-generation pending range in a consistent durable snapshot."""

    obligation_id: str
    low_id: int
    high_id: int
    closed: bool
    close_generation: int
    eligibility_point: float
    late_ids: tuple[int, ...]


@dataclass(frozen=True)
class SourceProgress:
    """Capture durable source progress and the revision governing pending memberships."""

    range_revision: int
    frontier: int
    source_successful_clock: float
    source_generation: str
    stream_incarnation: str


@dataclass(frozen=True)
class _GroupBootstrapState:
    """Project durable facts needed to register one group with its source."""

    cursor: int
    delivery_floor: int
    last_delivered_id: int
    is_operator_replay: bool
    halted: bool


class _EpochCaptureOptions(TypedDict):
    """Type one strict epoch capture."""

    low_id: int
    high_id: int
    captured_at: float
    eligibility_point: float
    reader_clock_checkpoint: float
    source_generation: str


@dataclass(frozen=True)
class _GroupSettings:
    """Hold validated group identity and state-policy settings."""

    _site: str
    _site_server_url: str
    _source: str
    _group: str
    _group_generation: int
    _ordering: Literal['eager', 'strict']
    _stream_fingerprint: str
    _source_generation: str
    _verified_incarnation: str
    _initial_cursor: int
    _initial_delivery_floor: int
    _initial_last_delivered_id: int
    _start_mode: StartMode
    _max_failure_text_bytes: int
    _max_group_obligations: int
    _max_late_ids_per_group: int
    _max_manifest_bytes: int
    _max_pending_epochs: int
    _strict_holdback_secs: float


class _OperatorOptions(TypedDict):
    """Type exact operator audit context."""

    reason: str
    identity: AuditIdentity
    wall_clock_time: float
    operator_label: NotRequired[str | None]


class _OrderingAnomalyOptions(_OperatorOptions):
    """Type one exact ordering-anomaly acknowledgement."""

    event_id: int


class _RetryOptions(_OperatorOptions):
    """Type one exact retry command."""

    event_id: int
    callback_ordinal: int | None


class _SkipOptions(_OperatorOptions):
    """Type one exact callback-skip command."""

    event_id: int
    callback_ordinal: int


class GroupState:
    """Own one locked SQLite authority for a configured group.

    The primary and FULL handoff connections and the lane lock remain owned
    until `close()` or context exit. State operations require those resources
    to remain open.
    """

    def __init__(
        self,
        database: DatabaseHandle,
        handoff: DatabaseHandle,
        lock: FileLock,
        settings: _GroupSettings,
        writes: WriteContext,
    ) -> None:
        """Bind open group database handles, the lane lock, and validated settings.

        Args:
            database: Primary group-state database handle.
            handoff: FULL-synchronous handoff database handle.
            lock: Held lane-owner lock released by `close()`.
            settings: Validated immutable group identity and policy settings.
            writes: Owned source-file write arbitration context.
        """
        self._database = database
        self._handoff = handoff
        self._lock = lock
        self._settings = settings
        self._writes = writes

    def __enter__(self) -> Self:
        """Return this open group state.

        Returns:
            This state while its database and lane lock remain open.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the state when leaving a context."""
        self.close()

    def _audit_records(self) -> tuple[AuditRecord, ...]:
        """Return cumulative immutable audit history."""
        return recovery.audit_records(self._database.connection, self._settings._group)

    @staticmethod
    def _initial_positions(
        options: GroupOpenOptions,
    ) -> tuple[StartMode, int, int, int]:
        """Validate one explicit group starting position."""
        raw_start_mode = options['start_mode']
        if not is_start_mode(raw_start_mode):
            raise ValueError('group start mode must be initialize or resume')
        start_mode = raw_start_mode
        if start_mode == 'resume':
            if any(
                name in options
                for name in (
                    'initial_cursor',
                    'initial_delivery_floor',
                    'initial_last_delivered_id',
                )
            ):
                raise ValueError('resume must not supply initial position')
            return 'resume', 0, 0, 0
        initial_cursor = options.get('initial_cursor')
        delivery_floor = options.get('initial_delivery_floor')
        last_delivered_id = options.get('initial_last_delivered_id')
        if initial_cursor is None or delivery_floor is None or last_delivered_id is None:
            raise ValueError('initialize requires cursor, delivery floor, and last-delivered id')
        return 'initialize', initial_cursor, delivery_floor, last_delivered_id

    @staticmethod
    def _build_settings(options: GroupOpenOptions) -> _GroupSettings:
        """Validate and build immutable group-open settings."""
        start_mode, initial_cursor, delivery_floor, last_delivered_id = (
            GroupState._initial_positions(options)
        )
        initial_values = (initial_cursor, delivery_floor, last_delivered_id)
        if any(type(value) is not int or value < 0 for value in initial_values):
            raise ValueError('initial group positions must be nonnegative integers')
        if not initial_cursor <= delivery_floor <= last_delivered_id:
            raise ValueError('initial group positions are inconsistent')
        settings = _GroupSettings(
            _site=options['site'],
            _site_server_url=options['site_server_url'],
            _source=options['source'],
            _group=options['group'],
            _group_generation=options['group_generation'],
            _ordering=options['ordering'],
            _stream_fingerprint=options['stream_fingerprint'],
            _source_generation=options['source_generation'],
            _verified_incarnation=options['verified_incarnation'],
            _initial_cursor=initial_cursor,
            _initial_delivery_floor=delivery_floor,
            _initial_last_delivered_id=last_delivered_id,
            _start_mode=start_mode,
            _max_failure_text_bytes=options.get(
                'max_failure_text_bytes', DEFAULT_MAX_FAILURE_TEXT_BYTES
            ),
            _max_group_obligations=options.get(
                'max_group_obligations', DEFAULT_MAX_GROUP_OBLIGATIONS
            ),
            _max_late_ids_per_group=options.get(
                'max_late_ids_per_group', DEFAULT_MAX_LATE_IDS_PER_GROUP
            ),
            _max_manifest_bytes=options.get('max_manifest_bytes', DEFAULT_MAX_MANIFEST_BYTES),
            _max_pending_epochs=options.get('max_pending_epochs', DEFAULT_MAX_PENDING_EPOCHS),
            _strict_holdback_secs=options.get(
                'strict_holdback_secs', DEFAULT_STRICT_HOLDBACK_SECS
            ),
        )
        if settings._max_pending_epochs < MIN_PENDING_EPOCHS:
            raise ValueError('max_pending_epochs must be at least two')
        if not isfinite(settings._strict_holdback_secs) or settings._strict_holdback_secs <= 0:
            raise ValueError('strict_holdback_secs must be finite and positive')
        return settings

    @staticmethod
    def _insert_initial_state(
        handoff: DatabaseHandle,
        settings: _GroupSettings,
        writes: WriteContext,
    ) -> None:
        """Commit one captured generation and all open-range memberships."""
        request = _bootstrap_request(settings)
        if recovery.recorded_bootstrap(handoff.connection, settings._group, request) is not None:
            return
        with writes.transaction(handoff.connection):
            recovery.bootstrap(handoff.connection, settings._group, request)

    def _pending_ranges(self, connection: sqlite3.Connection) -> tuple[PendingRange, ...]:
        """Project current memberships and source IDs within the caller's read transaction."""
        rows = connection.execute(
            """
            SELECT ranges.obligation_id, low_id, high_id, range_state,
            close_generation, eligibility_point FROM range_groups AS bindings JOIN
            discovery_ranges AS ranges USING (obligation_id) JOIN groups AS meta
            ON meta.group_name = bindings.group_name AND meta.group_generation =
            bindings.group_generation WHERE bindings.group_name = ? AND
            obligation_state = 'pending' ORDER BY low_id, high_id,
            ranges.obligation_id
            """,
            (self._settings._group,),
        ).fetchall()
        return tuple(
            PendingRange(
                obligation_id=str(row[0]),
                low_id=int(row[1]),
                high_id=int(row[2]),
                closed=row[3] == 'closed',
                close_generation=int(row[4]),
                eligibility_point=float(row[5]),
                late_ids=tuple(
                    int(item[0])
                    for item in connection.execute(
                        (
                            'SELECT event_id FROM source_late_ids WHERE '
                            'obligation_id = ? ORDER BY event_id'
                        ),
                        (row[0],),
                    )
                ),
            )
            for row in rows
        )

    def _prepare_disposition_open(self) -> None:
        """Check existing command authority without reconciling delivery policy."""
        if self._settings._start_mode != 'resume':
            raise InvalidTransitionError('disposition requires existing group state')
        row = self._database.connection.execute(
            'SELECT group_generation FROM groups JOIN group_state USING (group_name) '
            'WHERE group_name = ?',
            (self._settings._group,),
        ).fetchone()
        if row is None:
            raise InvalidTransitionError('disposition group state is missing')
        if int(row[0]) != self._settings._group_generation:
            raise InvalidTransitionError('disposition group generation conflicts')

    @staticmethod
    def _reconcile_open(
        database: DatabaseHandle,
        settings: _GroupSettings,
        writes: WriteContext,
    ) -> None:
        """Reconcile source and ordering changes on the configured connection."""
        connection = database.connection
        stored = connection.execute(
            """SELECT group_generation, ordering_mode, site_server_url, verified_incarnation,
            stream_fingerprint, stream_incarnation FROM groups CROSS JOIN source_meta
            WHERE group_name = ?""",
            (settings._group,),
        ).fetchone()
        if stored is None:
            raise InvalidTransitionError('resume group state is missing')
        if int(stored[0]) != settings._group_generation:
            raise InvalidTransitionError('group generation requires an audited disposition')
        stream_changed = (
            str(stored[2]) != settings._site_server_url
            or str(stored[3]) != str(stored[5])
            or str(stored[4]) != settings._stream_fingerprint
            or str(stored[5]) != settings._verified_incarnation
        )
        ordering_changed = str(stored[1]) != settings._ordering
        flags = connection.execute(
            'SELECT stream_identity_changed, ordering_changed FROM group_state '
            'WHERE group_name = ?',
            (settings._group,),
        ).fetchone()
        if tuple(map(bool, flags)) != (stream_changed, ordering_changed):
            with writes.transaction(connection):
                if stream_changed:
                    connection.execute(
                        'DELETE FROM epochs WHERE group_name = ?', (settings._group,)
                    )
                connection.execute(
                    'UPDATE group_state SET stream_identity_changed = ?, ordering_changed = ?, '
                    'revision = revision + 1 WHERE group_name = ?',
                    (int(stream_changed), int(ordering_changed), settings._group),
                )

    @classmethod
    def _open_owned(
        cls,
        database_path: Path,
        lock_path: Path,
        prepare: Callable[[Self], None],
        **options: Unpack[GroupOpenOptions],
    ) -> Self:
        """Open validated resources under the facade ownership boundary."""
        writes = options['writes']
        settings = cls._build_settings(options)
        lock = acquire_lock(lock_path)
        handoff: DatabaseHandle | None = None
        database: DatabaseHandle | None = None
        identity = source_identity(settings._site, settings._source)
        try:
            handoff = open_database(
                database_path, identity, SOURCE_SCHEMA, create=False, synchronous='FULL'
            )
            database = open_database(
                database_path,
                identity,
                SOURCE_SCHEMA,
                create=False,
                synchronous=options.get('synchronous', DEFAULT_SQLITE_SYNCHRONOUS.upper()),
            )
            state = cls(database, handoff, lock, settings, writes)
            prepare(state)
        except BaseException as error:
            close_resources((database, handoff, lock), primary_error=error)
            raise
        return state

    def _prepare_lane_open(self) -> None:
        """Initialize or reconcile normal delivery startup under its existing owner."""
        if self._settings._start_mode == 'initialize':
            self._insert_initial_state(self._handoff, self._settings, self._writes)
        else:
            self._reconcile_open(self._database, self._settings, self._writes)

    def acknowledge_ordering_anomaly(
        self,
        *,
        expected_revision: int,
        **options: Unpack[_OrderingAnomalyOptions],
    ) -> GroupSnapshot:
        """Audit one exact strict-ordering anomaly skip.

        Args:
            expected_revision: Required for a new transition. An exact recorded
                retry returns its historical snapshot before revision
                comparison.
            **options: Exact event and operator audit context.

        Returns:
            New or recorded recovery snapshot.

        Raises:
            InvalidTransitionError: The event does not match the active anomaly
                or the new audit has an empty reason or nonlocal identity.
            RevisionConflictError: The expected revision differs for new work.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.acknowledge_ordering_anomaly(
                self._database.connection,
                self._settings._group,
                recovery.OrderingAnomalyRequest(
                    event_id=options['event_id'],
                    operator=_operator_request(options),
                ),
                expected_revision,
            )

    def advance_irrelevant(
        self,
        high_id: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Advance one irrelevant range.

        Args:
            high_id: Inclusive greatest irrelevant event id.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed immutable snapshot.

        Raises:
            InvalidTransitionError: Recovery blocks advancement, `high_id` does
                not advance the cursor, or a strict fence would be crossed.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return delivery.advance_irrelevant(
                self._database.connection,
                self._settings._group,
                high_id,
                expected_revision,
            )

    def advance_irrelevant_prefix(
        self,
        high_id: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit only the safe prefix of a live irrelevant batch.

        Args:
            high_id: Inclusive greatest buffered irrelevant event id.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Group snapshot at the actual safe cursor after the atomic fence
            check. If no prefix is safe, the snapshot is unchanged without a
            revision bump.

        Raises:
            InvalidTransitionError: A recovery condition or in-flight event
                prevents delivery progress.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return delivery.advance_irrelevant_prefix(
                self._database.connection,
                self._settings._group,
                high_id,
                expected_revision,
            )

    def audit_count(self) -> int:
        """Return the cumulative group-owner audit count.

        Returns:
            Number of retained group audit rows.
        """
        row = self._database.connection.execute(
            'SELECT COUNT(*) FROM audit WHERE group_name = ?', (self._settings._group,)
        ).fetchone()
        if row is None:
            raise RuntimeError('group audit count is unavailable')
        return int(row[0])

    def begin_callback(
        self,
        ordinal: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Increment one callback delivery attempt.

        Args:
            ordinal: In-flight callback ordinal.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed immutable snapshot.

        Raises:
            InvalidTransitionError: Recovery blocks delivery, the ordinal is
                not the next pending callback, or a strict fence blocks
                invocation.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return delivery.begin_callback(
                self._database.connection,
                self._settings._group,
                ordinal,
                expected_revision,
            )

    def callback_done(
        self,
        ordinal: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit one callback success.

        Args:
            ordinal: In-flight callback ordinal.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed immutable snapshot. Completing the final callback also
            advances the cursor and last-delivered position and clears
            in-flight event and callback state.

        Raises:
            InvalidTransitionError: Recovery blocks completion, the callback is
                not an invoked pending callback, or a strict fence blocks
                completion.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return delivery.callback_done(
                self._database.connection,
                self._settings._group,
                ordinal,
                expected_revision,
            )

    def callback_failed(
        self,
        ordinal: int,
        failure_text: str,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit one bounded callback failure.

        Args:
            ordinal: In-flight callback ordinal.
            failure_text: Redacted trusted-plugin failure text.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed halted snapshot.

        Raises:
            InvalidTransitionError: There is no active event or the ordinal
                does not identify an invoked pending callback.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return delivery.callback_failed(
                self._database.connection,
                self._settings._group,
                delivery.CallbackFailureRequest(
                    ordinal=ordinal,
                    failure_text=failure_text,
                    max_failure_text_bytes=self._settings._max_failure_text_bytes,
                ),
                expected_revision,
            )

    def capture_epoch(
        self,
        *,
        expected_revision: int,
        **options: Unpack[_EpochCaptureOptions],
    ) -> GroupSnapshot:
        """Persist one disjoint strict catch-up epoch.

        Args:
            expected_revision: Required compare-and-swap revision.
            **options: Exact epoch bounds and source-clock evidence.

        Returns:
            Committed immutable snapshot.

        Raises:
            InvalidTransitionError: Strict epoch identity, bounds, timing,
                sequence, or capacity prevents capture.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.capture_epoch(
                self._database.connection,
                self._settings._group,
                recovery.EpochCaptureRequest(
                    low_id=options['low_id'],
                    high_id=options['high_id'],
                    captured_at=options['captured_at'],
                    eligibility_point=options['eligibility_point'],
                    reader_clock_checkpoint=options['reader_clock_checkpoint'],
                    source_generation=options['source_generation'],
                ),
                recovery.EpochPolicy(
                    max_pending_epochs=self._settings._max_pending_epochs,
                    capture_interval=strict_capture_interval(
                        self._settings._strict_holdback_secs,
                        self._settings._max_pending_epochs,
                    ),
                    holdback_secs=self._settings._strict_holdback_secs,
                ),
                expected_revision,
            )

    def change_lifecycle(self, request: recovery.LifecycleRequest) -> GroupSnapshot:
        """FULL-commit an orphan or re-add after validating its durable intent fence.

        Exact audit receipts return before acquiring a write transaction. New
        work validates lifecycle and generation again inside the transaction.
        Orphaning parks the group and collapses obligation memberships into
        replay evidence. Re-adding leaves it parked and inactive pending
        disposition.

        Args:
            request: Generation- and lifecycle-fenced transition with trusted
                local audit identity.

        Returns:
            The committed snapshot or the exact historical lifecycle receipt.

        Raises:
            InvalidTransitionError: Transition identity or lifecycle/generation
                fences conflict, an event is active, or the audit identity is
                not local.
            ValueError: A recorded snapshot JSON value is invalid.
        """
        recorded = recovery.recorded_lifecycle(
            self._handoff.connection, self._settings._group, request
        )
        if recorded is not None:
            return recorded
        with self._writes.transaction(self._handoff.connection):
            return recovery.change_lifecycle(
                self._handoff.connection, self._settings._group, request
            )

    def checkpoint_reader_clock(
        self,
        reader_clock_checkpoint: float,
        *,
        source_generation: str,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Persist an idle-frontier strict reader-clock checkpoint.

        Args:
            reader_clock_checkpoint: Current successful-query clock.
            source_generation: Exact source process generation.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed immutable snapshot.

        Raises:
            InvalidTransitionError: Ordering is not strict, no epochs are
                pending, the source generation conflicts, or the checkpoint
                interval has not elapsed.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.checkpoint_reader_clock(
                self._database.connection,
                self._settings._group,
                recovery.ReaderClockRequest(
                    reader_clock_checkpoint=reader_clock_checkpoint,
                    source_generation=source_generation,
                    checkpoint_interval=strict_capture_interval(
                        self._settings._strict_holdback_secs,
                        self._settings._max_pending_epochs,
                    ),
                ),
                expected_revision,
            )

    def close(self) -> None:
        """Close both connections and release the lane lock in ownership order.

        The operation attempts every owned resource, then raises the first
        cleanup failure with its original traceback.
        """
        close_resources((self._database, self._handoff, self._lock, self._writes.lock))

    def commit_live_join(
        self,
        identity: LiveJoinIdentity,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit one exact token-fenced live-join attempt.

        A new commit clears operator-replay mode and removes prior join
        receipts.

        Args:
            identity: Complete source and group join attempt identity.
            expected_revision: Required for a new join. An exact retry of the
                current retained token returns its historical snapshot before
                comparison.

        Returns:
            New or recorded committed join snapshot.

        Raises:
            InvalidTransitionError: The token or source/group identity
                conflicts, the cursor is behind the boundary, or recovery
                blocks the join.
            RevisionConflictError: The expected revision differs for new work.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.commit_live_join(
                self._database.connection,
                self._settings._group,
                identity,
                expected_revision,
            )

    def complete_epoch(
        self,
        epoch_id: int,
        cursor: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Advance through one fully delivered strict epoch.

        The cursor reaches the epoch upper bound even when that requires
        advancing through invisible event ids.

        Args:
            epoch_id: Durable delivering epoch identity.
            cursor: Exact `epoch.high_id` upper bound.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed immutable snapshot.

        Raises:
            InvalidTransitionError: Recovery evidence or an obligation blocks
                completion, the epoch is not delivering, or `cursor` differs
                from its upper bound.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.complete_epoch(
                self._database.connection,
                self._settings._group,
                epoch_id,
                cursor,
                expected_revision,
            )

    def complete_late_id(
        self,
        obligation_id: str,
        event_id: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit one terminal late-id mark.

        Args:
            obligation_id: Stable range identity.
            event_id: Exact late event id.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed immutable snapshot.

        Raises:
            TerminalLateEventError: The late id is not pending for this
                obligation.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return obligations.complete_late_id(
                self._database.connection,
                self._settings._group,
                obligations.LateEventRequest(obligation_id=obligation_id, event_id=event_id),
                expected_revision,
                self._settings._max_late_ids_per_group,
            )

    def dispose(self, request: recovery.DispositionRequest) -> GroupSnapshot:
        """Commit one complete generation replacement, or return its exact recorded result.

        Exact receipts return before acquiring a write transaction and before
        current revision or source-authority checks. New work uses a FULL
        transaction to replace positions, generation, and recovery evidence.

        Args:
            request: Revision-fenced replay or reset for this group, with a
                nonempty disposition identity, increasing reserved generation,
                and trusted local audit context. Replay supplies an inclusive
                starting event id; reset supplies no replay position.

        Returns:
            The committed replacement snapshot or the exact historical receipt.

        Raises:
            InvalidTransitionError: Command identity or source authority
                conflicts;
                generation, replay bounds, retained-history acknowledgement,
                parked/orphaned state, or in-flight recovery requirements are
                invalid; or the audit reason or local identity is invalid.
            RevisionConflictError: The expected revision differs for new work.
            ValueError: A recorded snapshot JSON value is invalid.
        """
        if request.group != self._settings._group:
            raise InvalidTransitionError('disposition group identity conflicts')
        recorded = recovery.recorded_disposition(
            self._database.connection, self._settings._group, request
        )
        if recorded is not None:
            return recorded
        check_revision(self._database.connection, self._settings._group, request.expected_revision)
        with self._writes.transaction(self._handoff.connection):
            return recovery.dispose(
                self._handoff.connection,
                self._settings._group,
                request,
                target=_disposition_target(self._settings),
            )

    def lifecycle(self) -> str:
        """Read the group's durable lifecycle fence."""
        row = self._database.connection.execute(
            'SELECT lifecycle FROM groups WHERE group_name = ?', (self._settings._group,)
        ).fetchone()
        if row is None:
            raise RuntimeError('group lifecycle is unavailable')
        return str(row[0])

    def mark_epoch_eligible(
        self,
        epoch_id: int,
        *,
        reader_clock_checkpoint: float,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Persist one strict epoch eligibility transition.

        Args:
            epoch_id: Durable waiting epoch identity.
            reader_clock_checkpoint: Reader clock at or after eligibility.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed immutable snapshot.

        Raises:
            InvalidTransitionError: This is not the next waiting epoch,
                eligibility has not been reached, or the reader clock
                regresses.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.mark_epoch_eligible(
                self._database.connection,
                self._settings._group,
                epoch_id,
                reader_clock_checkpoint,
                expected_revision,
            )

    def mark_history_truncated(
        self,
        retention_floor: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Persist one visible-history retention floor halt.

        Args:
            retention_floor: Lowest currently visible source event id.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Current snapshot when the retained floor is at least as high;
            otherwise a newly halted snapshot.

        Raises:
            InvalidTransitionError: The retention floor is not positive.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.mark_history_truncated(
                self._database.connection,
                self._settings._group,
                retention_floor,
                expected_revision,
            )

    def mark_ordering_anomaly(
        self,
        event_id: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Persist one strict late-discovery ordering anomaly.

        Args:
            event_id: Late event below the last delivered id.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed halted snapshot.

        Raises:
            InvalidTransitionError: Ordering is not strict, another anomaly is
                active, or the event is not above the floor and below the last
                delivered id.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.mark_ordering_anomaly(
                self._database.connection,
                self._settings._group,
                event_id,
                expected_revision,
            )

    def mark_strict_capacity_exhausted(
        self,
        identity: LiveJoinIdentity,
        *,
        abort_limit: int,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit one complete-identity live-window capacity halt.

        Args:
            identity: Current committed strict live-join identity.
            abort_limit: Plan-level terminal streak value.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Idempotent current or newly halted snapshot.

        Raises:
            InvalidTransitionError: The strict live identity conflicts or the
                abort limit is not positive.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.mark_strict_capacity_exhausted(
                self._database.connection,
                self._settings._group,
                identity,
                abort_limit=abort_limit,
                expected_revision=expected_revision,
            )

    @classmethod
    def open(
        cls,
        database_path: Path,
        lock_path: Path,
        **options: Unpack[GroupOpenOptions],
    ) -> Self:
        """Open group state in an existing source database under its owner lock.

        Args:
            database_path: Existing source SQLite database path.
            lock_path: Separate group owner lock beneath the site groups
                directory.
            options: Complete group identity plus optional bounded state
                policies. `synchronous` selects `NORMAL` (default) or `FULL`
                for the primary connection; handoffs always use `FULL`.
                `initialize` requires all three initial positions as exact
                nonnegative integers satisfying `cursor <= delivery_floor
                <= last_delivered_id`; `resume` prohibits initial positions.
                The facade consumes `writes.lock` on entry, closing it on
                failure or with the state.

        Returns:
            An open group-state authority holding the lane lock.

        Raises:
            AlreadyLockedError: Another owner holds the lane lock.
            SecurityError: A state or lock path cannot be securely opened or
                fails filesystem safety checks.
            DatabaseError: Database opening, policy, initialization, or
                validation fails, including an unsupported schema version.
            InvalidTransitionError: `initialize` targets existing state,
                `resume` targets missing state, or the resumed site, group, or
                generation conflicts with the stored state.
            ValueError: The start mode or initial positions are invalid,
                max_pending_epochs is below two, or strict holdback is not
                finite and positive.
        """
        try:
            return cls._open_owned(
                database_path, lock_path, prepare=cls._prepare_lane_open, **options
            )
        except BaseException as error:
            close_resources((options['writes'].lock,), primary_error=error)
            raise

    @classmethod
    def open_for_disposition(
        cls, database_path: Path, lock_path: Path, **options: Unpack[GroupOpenOptions]
    ) -> Self:
        """Acquire existing command resources without delivery startup mutation.

        The facade consumes `writes.lock` and holds the same group owner lock
        and validated source database resources as `open()`. Receipt and
        new-command policy validation remain in `dispose()`, after this
        read-only acquisition.

        Args:
            database_path: Existing source database; never created by this
                opener.
            lock_path: Separate nonblocking group owner lock.
            options: Complete identity and target policy, with
                `start_mode='resume'`.

        Returns:
            Locked authority with unchanged durable group state.

        Raises:
            InvalidTransitionError: The group is missing, its generation
                differs, or initialization was requested.
            AlreadyLockedError: Another owner holds the group lock.
            DatabaseError: The existing source database cannot be validated.
            SecurityError: State or lock resources cannot be acquired safely.
            ValueError: The start mode or initial positions are invalid,
                `max_pending_epochs` is below two, or `strict_holdback_secs` is
                not finite and positive.
        """
        try:
            return cls._open_owned(
                database_path, lock_path, prepare=cls._prepare_disposition_open, **options
            )
        except BaseException as error:
            close_resources((options['writes'].lock,), primary_error=error)
            raise

    @contextmanager
    def operator_writes(self) -> Generator[None]:
        """Use the bounded retryable policy for writes on this owner thread."""
        previous = self._writes
        self._writes = previous.for_operator()
        try:
            yield
        finally:
            self._writes = previous

    def park(
        self,
        *,
        expected_revision: int,
        **options: Unpack[_OperatorOptions],
    ) -> GroupSnapshot:
        """Park callback scheduling without dropping coordination.

        Args:
            expected_revision: Required for a new transition. An exact recorded
                retry returns its historical snapshot before revision
                comparison.
            **options: Required reason, local identity, time, and optional
                audit label.

        Returns:
            New or recorded parked snapshot.

        Raises:
            InvalidTransitionError: The group is already parked, or the new
                audit reason or local identity is invalid.
            RevisionConflictError: The expected revision differs for new work.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.park(
                self._database.connection,
                self._settings._group,
                _operator_request(options),
                expected_revision,
            )

    def park_reason(self) -> str | None:
        """Return the latest durable park reason.

        Returns:
            Latest explicit or conditional park audit reason, even after
            unpark, or `None` when no park audit exists.
        """
        row = self._database.connection.execute(
            """
            SELECT reason
            FROM audit
            WHERE group_name = ? AND command IN ('park', 'conditional-park')
            ORDER BY audit_id DESC
            LIMIT 1
            """,
            (self._settings._group,),
        ).fetchone()
        return None if row is None else str(row[0])

    def pending_ranges(self) -> tuple[PendingRange, ...]:
        """Read current-generation pending ranges and their source IDs in one transaction."""
        with read_transaction(self._database.connection):
            return self._pending_ranges(self._database.connection)

    def record_delivery_floor_skip(
        self,
        event_id: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit one generation-scoped delivery-floor audit.

        Args:
            event_id: Exact row at or below the current generation floor.
            expected_revision: Required for a new audit. An exact recorded
                retry returns the current snapshot before revision comparison.

        Returns:
            Idempotent current or newly audited snapshot.

        Raises:
            InvalidTransitionError: The event is above the current delivery
                floor.
            RevisionConflictError: The expected revision differs for new work.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.record_delivery_floor_skip(
                self._database.connection,
                self._settings._group,
                event_id,
                operator=recovery.OperatorRequest(
                    reason='source delivery-floor decision',
                    identity=AuditIdentity.current(),
                    wall_clock_time=time.time(),
                ),
                expected_revision=expected_revision,
            )

    def record_strict_floor_skip(
        self,
        event_id: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit one generation-scoped delivery-floor audit.

        Args:
            event_id: Exact row at or below the current generation floor.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Idempotent current or newly audited snapshot.

        Raises:
            InvalidTransitionError: Ordering is not strict or the event is
                above the current delivery floor.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.record_strict_floor_skip(
                self._database.connection,
                self._settings._group,
                event_id,
                operator=recovery.OperatorRequest(
                    reason='source strict delivery-floor decision',
                    identity=AuditIdentity.current(),
                    wall_clock_time=time.time(),
                ),
                expected_revision=expected_revision,
            )

    def record_strict_join_outcome(
        self,
        *,
        attempt_token: str,
        capacity_caused: bool,
        abort_limit: int,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit one bounded token-fenced strict join outcome.

        Args:
            attempt_token: Non-reusable live-join attempt identity.
            capacity_caused: Whether measured strict capacity caused the abort.
            abort_limit: Plan-level consecutive capacity-abort bound.
            expected_revision: Required for a new outcome. An exact retry of
                the retained token returns the current snapshot before
                comparison.

        Returns:
            Idempotent current or newly committed snapshot.

        Raises:
            InvalidTransitionError: Ordering is not strict, the token or
                positive abort limit is invalid, or the recorded outcome
                conflicts.
            RevisionConflictError: The expected revision differs for new work.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.record_strict_join_outcome(
                self._database.connection,
                self._settings._group,
                recovery.StrictJoinOutcomeRequest(
                    attempt_token=attempt_token,
                    capacity_caused=capacity_caused,
                    abort_limit=abort_limit,
                ),
                expected_revision,
            )

    def record_strict_ordering_decision(
        self,
        control: StrictOrderingControl,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Commit one strict ordering halt or body-free floor audit.

        Args:
            control: Complete current live identity and source-side decision.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Idempotent current or newly committed snapshot.

        Raises:
            InvalidTransitionError: The strict identity, comparison evidence,
                event, or requested ordering action conflicts.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.record_strict_ordering_decision(
                self._database.connection,
                self._settings._group,
                control,
                operator=recovery.OperatorRequest(
                    reason='source strict delivery-floor decision',
                    identity=AuditIdentity.current(),
                    wall_clock_time=time.time(),
                ),
                expected_revision=expected_revision,
            )

    def refresh_ranges(
        self,
        last_revision: int | None = None,
    ) -> tuple[SourceProgress, tuple[PendingRange, ...] | None]:
        """Read source facts and changed pending ranges in one SQLite snapshot.

        Args:
            last_revision: Last processed durable range revision; `None` forces
                a row read.

        Returns:
            Source progress and the current-generation pending ranges. The
            ranges are `None` when the range revision is unchanged; an empty
            tuple means a row read found no pending ranges.
        """
        connection = self._database.connection
        with read_transaction(connection):
            row = connection.execute(
                """
                SELECT range_revision, frontier, successful_clock, source_generation,
                stream_incarnation FROM source_meta WHERE singleton = 1
                """
            ).fetchone()
            if row is None or row[4] is None:
                raise RuntimeError('source metadata is not initialized')
            progress = SourceProgress(int(row[0]), int(row[1]), float(row[2]), str(row[3]), row[4])
            return (
                progress,
                None
                if progress.range_revision == last_revision
                else self._pending_ranges(connection),
            )

    def require_event(
        self,
        event_id: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Persist one exact required event identity.

        Args:
            event_id: Exact required event id.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed halted snapshot.

        Raises:
            InvalidTransitionError: Any required event is already active, or a
                different event is in flight.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return delivery.require_event(
                self._database.connection,
                self._settings._group,
                event_id,
                expected_revision,
            )

    def reserve_late_event(
        self,
        obligation_id: str,
        event_id: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Reserve one exact late event before delivery.

        Args:
            obligation_id: Stable owning range identity.
            event_id: Exact late event identity.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Current snapshot for an already pending or completed id, a
            committed admitted reservation, or a capacity-blocked snapshot
            retaining a queued `capacity-pending` reservation.

        Raises:
            InvalidTransitionError: The obligation does not own this event in
                the current generation.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return obligations.reserve_late_event(
                self._database.connection,
                self._settings._group,
                obligations.LateEventRequest(obligation_id=obligation_id, event_id=event_id),
                expected_revision,
                self._settings._max_late_ids_per_group,
            )

    def resolve_conditional_park(
        self,
        directive: ConditionalParkDirective,
        *,
        identity: AuditIdentity,
        decided_at_wall_clock: float,
    ) -> ConditionalParkResult:
        """Record or replay one group-owned conditional-park decision.

        Args:
            directive: Exact immutable supervisor evidence.
            identity: Trusted local process identity for a parked-result audit.
            decided_at_wall_clock: Wall-clock time of the group decision.

        Returns:
            The new or previously recorded typed result. Evidence matching the
            current revision parks the group; a later revision returns `stale`
            without changing its park state.

        Raises:
            InvalidTransitionError: Evidence conflicts with group identity or a
                retained decision, or the parked-result audit identity is not
                local.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.resolve_conditional_park(
                self._database.connection,
                self._settings._group,
                directive,
                identity=identity,
                decided_at_wall_clock=decided_at_wall_clock,
            )

    def restart_event(
        self,
        manifest: CallbackManifest,
        *,
        expected_revision: int,
        **options: Unpack[_OperatorOptions],
    ) -> GroupSnapshot:
        """Audit and restart one event under a complete current manifest.

        Args:
            manifest: Current ordered callback shape.
            expected_revision: Required for a new transition. An exact recorded
                retry returns its historical snapshot before revision
                comparison.
            **options: Exact operator audit context.

        Returns:
            New or recorded restarted snapshot.

        Raises:
            InvalidTransitionError: No event is active, the manifest is empty,
                oversized or has nonconsecutive ordinals, or audit context is
                invalid.
            RevisionConflictError: The expected revision differs for new work.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.restart_event(
                self._database.connection,
                self._settings._group,
                recovery.ManifestRestartRequest(
                    manifest=manifest, max_manifest_bytes=self._settings._max_manifest_bytes
                ),
                _operator_request(options),
                expected_revision,
            )

    def restore_required_event(
        self,
        event_id: int,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Confirm one exact authorized event refetch.

        Args:
            event_id: Exact restored event id.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed snapshot with matching required-event and retry evidence
            cleared; independent conditions remain.

        Raises:
            InvalidTransitionError: The event or required-event retry
                authorization does not match retained state.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return delivery.restore_required_event(
                self._database.connection,
                self._settings._group,
                event_id,
                expected_revision,
            )

    def retry(
        self,
        *,
        expected_revision: int,
        **options: Unpack[_RetryOptions],
    ) -> GroupSnapshot:
        """Audit an exact callback retry or required-event refetch.

        Args:
            expected_revision: Required for a new transition. An exact recorded
                retry returns its historical snapshot before revision
                comparison.
            **options: Exact retry target and operator audit context.

        Returns:
            New or recorded recovery snapshot.

        Raises:
            InvalidTransitionError: The target does not match the required
                event or failed callback, or operator audit context is invalid.
            RevisionConflictError: The expected revision differs for new work.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.retry(
                self._database.connection,
                self._settings._group,
                recovery.CallbackRecoveryRequest(
                    event_id=options['event_id'],
                    callback_ordinal=options['callback_ordinal'],
                    operator=_operator_request(options),
                ),
                expected_revision,
            )

    def skip_callback(
        self,
        *,
        expected_revision: int,
        **options: Unpack[_SkipOptions],
    ) -> GroupSnapshot:
        """Audit one exact callback skip.

        Args:
            expected_revision: Required for a new transition. An exact recorded
                retry returns its historical snapshot before revision
                comparison.
            **options: Exact callback target and operator audit context.

        Returns:
            New or recorded recovery snapshot. Skipping the final pending
            callback completes the event, advances delivery positions, and
            clears in-flight event, manifest, and callback state.

        Raises:
            InvalidTransitionError: Recovery blocks the skip, the target is not
                an attempted pending callback, or operator audit context is
                invalid.
            RevisionConflictError: The expected revision differs for new work.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.skip_callback(
                self._database.connection,
                self._settings._group,
                recovery.CallbackRecoveryRequest(
                    event_id=options['event_id'],
                    callback_ordinal=options['callback_ordinal'],
                    operator=_operator_request(options),
                ),
                expected_revision,
            )

    def snapshot(self) -> GroupSnapshot:
        """Read the latest immutable group snapshot.

        Returns:
            Latest committed state.
        """
        return snapshot(self._database.connection, self._settings._group)

    def _bootstrap_state(self) -> _GroupBootstrapState:
        """Return durable delivery positions and replay/halt policy."""
        row = self._database.connection.execute(
            """
            SELECT cursor, delivery_floor, last_delivered_id,
                   is_operator_replay
            FROM group_state
            WHERE group_name = ?
            """,
            (self._settings._group,),
        ).fetchone()
        if row is None:
            raise RuntimeError('group state is not initialized')
        current = self.snapshot()
        return _GroupBootstrapState(
            cursor=int(row[0]),
            delivery_floor=int(row[1]),
            last_delivered_id=int(row[2]),
            is_operator_replay=bool(row[3]),
            halted=current.parked or bool(current.conditions),
        )

    def complete_obligation_scan(self, obligation_id: str) -> GroupSnapshot:
        """Commit completion for one closed pending range at the configured durability level.

        Args:
            obligation_id: Range whose complete post-close scan finished.

        Returns:
            Completed snapshot, or the current halted snapshot without a write.

        Raises:
            InvalidTransitionError: The range is not closed and pending, or an
                above-floor late event remains unfinished.
            RevisionConflictError: The group revision changed before
                transactional completion.
        """
        current = self.snapshot()
        if current.status != 'running':
            return current
        with self._writes.transaction(self._database.connection):
            return obligations.complete_obligation_scan(
                self._database.connection,
                self._settings._group,
                obligation_id,
                current.revision,
            )

    def source_frontier(self) -> int:
        """Read the durable source frontier in one read transaction.

        Returns:
            Highest durably observed source event id, or zero before any event.

        Raises:
            RuntimeError: Source metadata is missing.
        """
        connection = self._database.connection
        with read_transaction(connection):
            row = connection.execute(
                'SELECT frontier FROM source_meta WHERE singleton = 1'
            ).fetchone()
            if row is None:
                raise RuntimeError('source metadata is not initialized')
            return int(row[0])

    def start_event(
        self,
        event_id: int,
        manifest: CallbackManifest,
        *,
        expected_revision: int,
        obligation_id: str | None = None,
    ) -> GroupSnapshot:
        """Persist one event and manifest before invocation.

        Args:
            event_id: Source-visible event id.
            manifest: Complete ordered callback manifest.
            expected_revision: Required compare-and-swap revision.
            obligation_id: Optional late-range owner.

        Returns:
            Committed in-flight snapshot.

        Raises:
            InvalidTransitionError: The manifest is empty, oversized or has
                invalid ordinals, recovery blocks delivery, or ordinary
                progress is invalid.
            TerminalLateEventError: The late event is not pending.
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return delivery.start_event(
                self._database.connection,
                self._settings._group,
                delivery.EventStartRequest(
                    event_id=event_id,
                    manifest=manifest,
                    max_manifest_bytes=self._settings._max_manifest_bytes,
                    obligation_id=obligation_id,
                ),
                expected_revision,
            )

    def strict_obligation_fence(self) -> tuple[str, int, int] | None:
        """Return the lowest unresolved strict obligation identity and bounds.

        Returns:
            Exact obligation id, inclusive low id and high id, or `None` when
            no strict obligation blocks delivery.
        """
        return delivery.strict_obligation_fence(self._database.connection, self._settings._group)

    def terminal_disposition_identity(
        self,
        group_generation: int,
    ) -> tuple[str, str, str] | None:
        """Return the exact terminal disposition proof for one generation.

        Args:
            group_generation: Reserved generation requiring terminal proof.

        Returns:
            Stable disposition id, fingerprint, and result, or `None`.

        Raises:
            ValueError: The generation is not a nonnegative integer.
            InvalidTransitionError: Multiple conflicting terminal identities
                exist.
        """
        return recovery.terminal_disposition_identity(
            self._database.connection,
            self._settings._group,
            group_generation,
        )

    def unpark(
        self,
        *,
        expected_revision: int,
        **options: Unpack[_OperatorOptions],
    ) -> GroupSnapshot:
        """Clear only group-local park evidence.

        Args:
            expected_revision: Required for a new transition. An exact recorded
                retry returns its historical snapshot before revision
                comparison.
            **options: Required reason, local identity, time, and optional
                audit label.

        Returns:
            New or recorded unparked snapshot.

        Raises:
            InvalidTransitionError: The group is not parked, or the new audit
                reason or local identity is invalid.
            RevisionConflictError: The expected revision differs for new work.
        """
        with self._writes.transaction(self._database.connection):
            return recovery.unpark(
                self._database.connection,
                self._settings._group,
                _operator_request(options),
                expected_revision,
            )

    def unresolved_late_id_projection(
        self,
        *,
        limit: int,
    ) -> tuple[int, tuple[int, ...]]:
        """Read distinct unresolved closed-range IDs and a bounded prefix.

        Args:
            limit: Maximum number of distinct IDs in the ascending prefix.

        Returns:
            Exact unresolved count and the requested prefix.

        Raises:
            ValueError: The limit is negative.
        """
        if limit < 0:
            raise ValueError('late-id projection limit must be nonnegative')
        connection = self._database.connection
        with read_transaction(connection):
            count = connection.execute(
                UNRESOLVED_LATE_ID_COUNT_SQL,
                (self._settings._group,),
            ).fetchone()[0]
            prefix = tuple(
                int(row[0])
                for row in connection.execute(
                    UNRESOLVED_LATE_ID_PREFIX_SQL,
                    (self._settings._group, limit),
                )
            )
        return int(count), prefix

    def unresolved_obligation_bounds(self) -> tuple[tuple[int, int], ...]:
        """Return unresolved inclusive bounds in deterministic order.

        Returns:
            Durable low and high ids for every current range obligation.
        """
        return tuple(
            (int(row[0]), int(row[1]))
            for row in self._database.connection.execute(
                """
                SELECT low_id, high_id FROM range_groups JOIN discovery_ranges USING
                (obligation_id) WHERE group_name = ? AND obligation_state = 'pending'
                AND group_generation = (SELECT group_generation FROM groups WHERE
                group_name = ?) ORDER BY low_id, high_id, obligation_id
                """,
                (self._settings._group, self._settings._group),
            )
        )

    def validate_disposition(self, request: recovery.DispositionRequest) -> None:
        """Check a prospective new disposition without changing durable state.

        Args:
            request: Replay or reset request for a higher group generation.

        Raises:
            InvalidTransitionError: Group recovery state, generation, pending
                work, or replay bounds prevent the transition.
        """
        recovery.validate_disposition(
            self._database.connection,
            self._settings._group,
            request,
            _disposition_target(self._settings),
        )

    def validate_live_join(
        self,
        identity: LiveJoinIdentity,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Validate one live join without mutating durable state.

        Args:
            identity: Complete source and group join attempt identity.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Exact current immutable snapshot when the join is valid.

        Raises:
            InvalidTransitionError: The identity conflicts, the cursor is
                behind the boundary, or recovery blocks the join.
            RevisionConflictError: The expected revision differs.
        """
        with read_transaction(self._database.connection):
            return recovery.validate_live_join(
                self._database.connection,
                self._settings._group,
                identity,
                expected_revision,
            )

    def verify_manifest(
        self,
        manifest: CallbackManifest,
        *,
        expected_revision: int,
    ) -> GroupSnapshot:
        """Validate current callback shape.

        Args:
            manifest: Current complete callback manifest.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Current snapshot, newly halted for a shape mismatch or restored
            when the original manifest returns.

        Raises:
            RevisionConflictError: The expected revision differs.
        """
        with self._writes.transaction(self._database.connection):
            return delivery.verify_manifest(
                self._database.connection,
                self._settings._group,
                manifest,
                expected_revision,
            )


def _bootstrap_request(settings: _GroupSettings) -> recovery.BootstrapRequest:
    """Build the exact bootstrap command without runtime audit observations."""
    return recovery.BootstrapRequest(
        generation=settings._group_generation,
        cursor=settings._initial_cursor,
        delivery_floor=settings._initial_delivery_floor,
        last_delivered_id=settings._initial_last_delivered_id,
        ordering=settings._ordering,
        site_server_url=settings._site_server_url,
        verified_incarnation=settings._verified_incarnation,
        holdback_secs=settings._strict_holdback_secs if settings._ordering == 'strict' else 0.0,
    )


def _disposition_target(settings: _GroupSettings) -> recovery.DispositionTarget:
    """Capture configured policy and authority expectations separately from command identity."""
    return recovery.DispositionTarget(
        site_server_url=settings._site_server_url,
        ordering=settings._ordering,
        holdback_secs=settings._strict_holdback_secs if settings._ordering == 'strict' else 0.0,
        stream_fingerprint=settings._stream_fingerprint,
        source_generation=settings._source_generation,
        verified_incarnation=settings._verified_incarnation,
    )


def _operator_request(options: _OperatorOptions) -> recovery.OperatorRequest:
    """Build trusted local operator audit context."""
    return recovery.OperatorRequest(
        reason=options['reason'],
        identity=options['identity'],
        wall_clock_time=options['wall_clock_time'],
        operator_label=options.get('operator_label'),
    )


def strict_capture_interval(holdback_secs: float, max_pending_epochs: int) -> float:
    """Return the interval shared by strict epoch captures and reader checkpoints.

    Args:
        holdback_secs: Validated positive strict holdback in seconds.
        max_pending_epochs: Validated maximum epoch count, at least two.
    """
    return holdback_secs / (max_pending_epochs - 1)
